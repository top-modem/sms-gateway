use chrono::{Timelike, Utc};
use log::{debug, error, info, warn};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_serial::{SerialPortBuilderExt, SerialStream};

use crate::api::SseManager;
use crate::config::SmsStorage;
use crate::db::{Contact, ModemSMS, SimCard, Sms};
use crate::decode::parse_pdu_sms;
use crate::webhook;

use super::ftp_uploader;
use super::pdu::{build_pdus, string_to_ucs2_pub};
use super::types::*;

/// All SMS timestamps (incoming and outgoing) are normalized to GMT+8 so they
/// don't depend on the host OS's configured timezone. See also
/// `decode::parse_timestamp`, which applies the same target offset when
/// decoding incoming SMS-DELIVER PDU timestamps.
fn now_gmt8() -> chrono::NaiveDateTime {
    (Utc::now() + chrono::Duration::hours(8)).naive_utc()
}

const TERMINATORS: &[&[u8]] = &[
    b"\r\nOK\r\n",
    b"\r\nERROR\r\n",
    b"\r\n> ",
    b"\r\n+CME ERROR",
    b"\r\n+CMS ERROR",
    b"\r\nESIM\r\n",
    // Multi-SIM `ath?` reply carries no trailing OK.
    b"\r\nMSIM",
    // AT+QFUPL "ready to receive binary data" prompt (used for MMS attachment upload).
    b"CONNECT\r\n",
];

/// Terminators that have a variable-length error code after the prefix.
/// When matched, we scan ahead for the trailing `\r\n` to include the full error message.
const ERROR_TERMINATORS: &[&[u8]] = &[b"\r\n+CME ERROR", b"\r\n+CMS ERROR"];

/// Terminators considered for raw-byte responses (e.g. `AT+QFDWL`).
///
/// Deliberately excludes `CONNECT\r\n` and `\r\n> `: those are interim prompts
/// that precede the actual binary payload, not completion markers. Since raw
/// downloads arrive over several serial reads, the `CONNECT\r\n` prompt often
/// lands in `line_buf` on its own before any payload bytes have arrived; if it
/// were accepted as a terminator here, the raw response would be dispatched
/// with just `"CONNECT\r\n"` and no payload, well before the download actually
/// completes.
const RAW_TERMINATORS: &[&[u8]] = &[
    b"\r\nOK\r\n",
    b"\r\nERROR\r\n",
    b"\r\n+CME ERROR",
    b"\r\n+CMS ERROR",
];

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_LINE_BUF: usize = 64 * 1024;

/// Shared routing state between the background reader task and the command processor.
/// When `response_tx` is `Some`, bytes from the serial port are buffered for the command.
/// When `raw_response_tx` is `Some`, raw bytes are buffered (for binary transfers like AT+QFDWL).
/// When both are `None`, complete lines are dispatched as URCs via `urc_tx`.
struct ReaderState {
    response_tx: Option<tokio::sync::oneshot::Sender<io::Result<String>>>,
    raw_response_tx: Option<tokio::sync::oneshot::Sender<io::Result<Vec<u8>>>>,
}

pub struct Modem {
    pub name: String,
    pub com_port: String,
    pub baud_rate: u32,
    /// The HashMap key used when this modem has no SIM (e.g. "fallback_sim_3")
    pub fallback_key: String,
    command_tx: mpsc::UnboundedSender<ATCommand>,
    pub sim_id: RwLock<Option<String>>,
    _connection_state: Arc<RwLock<ConnectionState>>,
    /// Write half of the serial stream. `None` while disconnected.
    write_half: Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
    /// Shared routing state for the background reader task.
    reader_state: Arc<Mutex<ReaderState>>,
    /// Serializes ownership of `reader_state.response_tx` across the queued
    /// command processor and the raw multi-step senders (`send_pdu_atomic`,
    /// `upload_file`, `send_http_url`, `fetch_mms_content_via_requestheader`).
    /// Without this, a queued command (e.g. the periodic AT+CCID health check)
    /// can release the write lock before its response arrives, letting another
    /// sender overwrite `response_tx` and steal that response -- observed as
    /// e.g. an MMS `AT+QFUPL` CONNECT-prompt wait receiving a `+CCID` reply.
    command_lock: Arc<Mutex<()>>,
    /// URC sender (background reader holds a clone; kept here for test injection).
    _urc_tx: mpsc::UnboundedSender<String>,
    /// URC receiver — subscribed to by the ModemManager URC handler task.
    pub urc_rx: Arc<Mutex<mpsc::UnboundedReceiver<String>>>,
    /// Call ID of the active outbound call, set by manager after ATD succeeds.
    pub outbound_call_id: Arc<tokio::sync::Mutex<Option<String>>>,
    /// Cancel sender for the CLCC polling task; send () to stop polling (e.g. on user hangup).
    pub outbound_poll_cancel_tx: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    /// Becomes true when the CLCC poller observes stat=0 (remote answered the call).
    pub outbound_call_answered: Arc<tokio::sync::Mutex<bool>>,
    /// Call ID of the current inbound call, set by the URC handler on RING and
    /// cleared when the call ends. Lets a manual `/calls/answer` request update
    /// the same DB/SSE/recording state that auto-answer would.
    pub inbound_call_id: Arc<tokio::sync::Mutex<Option<String>>>,
    /// Cancel sender for the pending auto-answer delay; fired when the call is
    /// answered manually before the timer elapses, so the URC handler does not
    /// also send ATA.
    pub inbound_auto_answer_cancel_tx: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    /// True once the current inbound call has been answered, by either path.
    pub inbound_call_answered: Arc<std::sync::atomic::AtomicBool>,
    /// Cancel sender for the inbound call's 30s recording timer, shared so a
    /// manually-started recording is still stopped/downloaded by the URC
    /// handler's NO CARRIER cleanup.
    pub inbound_recording_cancel_tx: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    pub voice_otp_call_id: tokio::sync::Mutex<Option<String>>,
    voice_otp_microphone_state: tokio::sync::Mutex<Option<(String, bool)>>,
    pub voice_otp_cancel_tx: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    pub voice_otp_finishing: std::sync::atomic::AtomicBool,
    pub voice_otp_dtmf_sent: std::sync::atomic::AtomicBool,
    /// Set by a +CME 10/13/50 CPIN reply, cleared by CPIN READY; stale SIM rows must not be shown meanwhile.
    pub sim_absence_suspected: std::sync::atomic::AtomicBool,
    /// Receives the `+CUSD:` URC, which arrives after the AT+CUSD command's OK.
    pub ussd_reply_tx: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<String>>>,
    /// Reader task handle so we can abort and release COM resources on drop.
    reader_task: Mutex<Option<JoinHandle<()>>>,
    /// Command processor task handle so it does not outlive modem shutdown.
    command_task: Mutex<Option<JoinHandle<()>>>,
    /// If true, force COPS 46001 when IMSI MCC is 234/235.
    force_uk_mcc_to_46001: bool,
    /// Oneshot sender for the in-flight MMS send, resolved by the URC handler when
    /// the modem emits the async `+QMMSEND: <err>,<httprsp>` completion notification.
    pending_mms: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<(i32, i32)>>>>,
    /// Oneshot sender for the in-flight FTP login completion, resolved by the URC
    /// handler when the modem emits `+QFTPOPEN: <err>,<protocol_error>`.
    pending_ftp_open: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<(i32, i32)>>>>,
    /// Oneshot sender for the in-flight FTP download completion, resolved by the URC
    /// handler when the modem emits `+QFTPGET: <err>,<transferlen>`.
    pending_ftp_get: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<(i32, i64)>>>>,
    /// Oneshot sender for the in-flight MMS content fetch, resolved by the URC handler
    /// when the modem emits the async `+QHTTPGET: <err>,<httprsp>,<content_length>`
    /// completion notification.
    pending_http_get:
        Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<(i32, i32, i64)>>>>,
    /// Oneshot sender for the in-flight `AT+QHTTPREADFILE`, resolved by the URC handler
    /// when the modem emits the async `+QHTTPREADFILE: <err>` completion notification.
    /// `AT+QHTTPREADFILE` returns `OK` as soon as the command is accepted, not once the
    /// file is actually written -- issuing `AT+QFDWL` right after that `OK` races the file
    /// write for larger downloads (observed on real hardware as a 0-byte file via QFDWL).
    pending_http_readfile: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<i32>>>>,
    /// Machine type from `ath?`; `None` until a probe gives a definite answer.
    pub machine_type: RwLock<Option<MachineType>>,
    /// True while this port is in eSIM-management mode (SMS/recheck suspended).
    pub esim_mode: RwLock<bool>,
    /// Number of commands currently queued or in-flight in `command_processor`.
    /// Used by `send_command_priority` to fail fast (instead of piling on more
    /// queued commands) when a modem's single-lane queue is already backlogged --
    /// e.g. by a slow/unresponsive modem or overlapping status-refresh requests.
    /// Without this, a caller's outer `tokio::time::timeout` giving up does NOT
    /// cancel the already-queued command; it still runs to completion (with its
    /// own internal retries), so repeated polling can pile up an ever-growing
    /// backlog that never drains.
    pending_commands: Arc<AtomicUsize>,
    ftp_uploader: ftp_uploader::FtpUploader,
    /// First time each still-incomplete multipart SMS (sender, reference, total)
    /// was seen in storage. Its fragments are kept until all parts arrive or
    /// `INCOMPLETE_MULTIPART_TIMEOUT` passes.
    incomplete_multipart_seen: std::sync::Mutex<std::collections::HashMap<(String, u16, u8), Instant>>,
}

/// How long to wait for the missing parts of a multipart SMS before storing
/// the parts received so far. Operators sometimes deliver parts minutes apart.
const INCOMPLETE_MULTIPART_TIMEOUT: Duration = Duration::from_secs(30 * 60);

impl Modem {
    pub async fn new(
        com_port: &str,
        baud_rate: u32,
        name: &str,
        index: usize,
        force_uk_mcc_to_46001: bool,
    ) -> io::Result<Self> {
        let serial_stream = Self::create_serial_connection(com_port, baud_rate).await?;
        let (read_half, write_half_stream) = tokio::io::split(serial_stream);

        let (command_tx, command_rx) = mpsc::unbounded_channel::<ATCommand>();
        let (urc_tx, urc_rx) = mpsc::unbounded_channel::<String>();

        let write_half = Arc::new(Mutex::new(Some(write_half_stream)));
        let connection_state = Arc::new(RwLock::new(ConnectionState::Connected));
        let reader_state = Arc::new(Mutex::new(ReaderState {
            response_tx: None,
            raw_response_tx: None,
        }));

        // Spawn background reader task — owns ReadHalf, routes bytes to commands or URC channel
        let reader_task = tokio::spawn({
            let reader_state = reader_state.clone();
            let urc_tx = urc_tx.clone();
            let write_half = write_half.clone();
            let connection_state = connection_state.clone();
            let name_c = name.to_string();
            let com_port_c = com_port.to_string();
            async move {
                Self::reader_task_main(
                    read_half,
                    reader_state,
                    urc_tx,
                    write_half,
                    name_c,
                    com_port_c,
                    baud_rate,
                    connection_state,
                )
                .await;
            }
        });

        // Spawn sequential command processor
        let pending_commands = Arc::new(AtomicUsize::new(0));
        let command_lock = Arc::new(Mutex::new(()));
        let command_task = tokio::spawn({
            let write_half = write_half.clone();
            let reader_state = reader_state.clone();
            let name_c = name.to_string();
            let pending_commands = pending_commands.clone();
            let command_lock = command_lock.clone();
            async move {
                Self::command_processor(command_rx, write_half, reader_state, name_c, pending_commands, command_lock).await;
            }
        });

        info!("device:{}, com:{} connected successfully", name, com_port);

        Ok(Modem {
            name: name.to_string(),
            com_port: com_port.to_string(),
            baud_rate,
            fallback_key: format!("fallback_sim_{}", index),
            command_tx,
            sim_id: RwLock::new(None),
            _connection_state: connection_state,
            write_half,
            reader_state,
            command_lock,
            _urc_tx: urc_tx,
            urc_rx: Arc::new(Mutex::new(urc_rx)),
            outbound_call_id: Arc::new(tokio::sync::Mutex::new(None)),
            outbound_poll_cancel_tx: Arc::new(tokio::sync::Mutex::new(None)),
            outbound_call_answered: Arc::new(tokio::sync::Mutex::new(false)),
            inbound_call_id: Arc::new(tokio::sync::Mutex::new(None)),
            inbound_auto_answer_cancel_tx: Arc::new(tokio::sync::Mutex::new(None)),
            inbound_call_answered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            inbound_recording_cancel_tx: Arc::new(tokio::sync::Mutex::new(None)),
            voice_otp_call_id: tokio::sync::Mutex::new(None),
            voice_otp_microphone_state: tokio::sync::Mutex::new(None),
            voice_otp_cancel_tx: tokio::sync::Mutex::new(None),
            voice_otp_finishing: std::sync::atomic::AtomicBool::new(false),
            voice_otp_dtmf_sent: std::sync::atomic::AtomicBool::new(false),
            sim_absence_suspected: std::sync::atomic::AtomicBool::new(false),
            ussd_reply_tx: tokio::sync::Mutex::new(None),
            reader_task: Mutex::new(Some(reader_task)),
            command_task: Mutex::new(Some(command_task)),
            force_uk_mcc_to_46001,
            pending_mms: Arc::new(tokio::sync::Mutex::new(None)),
            pending_ftp_open: Arc::new(tokio::sync::Mutex::new(None)),
            pending_ftp_get: Arc::new(tokio::sync::Mutex::new(None)),
            pending_http_get: Arc::new(tokio::sync::Mutex::new(None)),
            pending_http_readfile: Arc::new(tokio::sync::Mutex::new(None)),
            ftp_uploader: ftp_uploader::FtpUploader::new(ftp_uploader::FtpUploadConfig {
                host: "47.237.101.133".to_string(),
                port: 21,
                username: "ftpuser".to_string(),
                password: "ftpuser".to_string(),
                timeout: Duration::from_secs(60),
                passive: true,
            }),
            machine_type: RwLock::new(None),
            esim_mode: RwLock::new(false),
            pending_commands,
            incomplete_multipart_seen: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }
}

impl Drop for Modem {
    fn drop(&mut self) {
        if let Some(task) = self.reader_task.get_mut().take() { task.abort(); }
        if let Some(task) = self.command_task.get_mut().take() { task.abort(); }
    }
}

async fn stop_serial_task(task: &Mutex<Option<JoinHandle<()>>>) {
    let mut handle = task.lock().await;
    if let Some(task) = handle.take() {
        task.abort();
        let _ = task.await;
    }
}

impl Modem {
    /// Release the serial port immediately, without waiting for every `Arc<Modem>`
    /// clone to be dropped. The reader task auto-reconnects on I/O errors, so a
    /// lingering clone would otherwise keep re-opening the COM port and block any
    /// attempt to re-initialize the modem on it.
    pub async fn shutdown(&self) {
        stop_serial_task(&self.command_task).await;
        stop_serial_task(&self.reader_task).await;
        *self.write_half.lock().await = None;
        *self._connection_state.write().await = ConnectionState::Disconnected;
    }
}

/// eSIM machine support. On these devices each COM port has two modes: a normal
/// SMS mode and an eSIM-management mode. The mode is toggled with vendor AT
/// commands over the same serial port; the actual eUICC/profile work is done
/// out-of-band via lpac over PC/SC. See esim_feature_design.txt.
impl Modem {
    /// Whether this port has been detected as an eSIM machine.
    pub async fn is_esim_capable(&self) -> bool {
        *self.machine_type.read().await == Some(MachineType::Esim)
    }

    /// Whether this port is currently in eSIM-management mode.
    pub async fn is_esim_mode(&self) -> bool {
        *self.esim_mode.read().await
    }

    async fn set_esim_mode(&self, on: bool) {
        *self.esim_mode.write().await = on;
    }

    /// Detect the machine type with `ath?` (`ESIM`, `MSIM`, or `ERROR` for a
    /// normal machine). Live replies override cached types; an
    /// unanswered probe falls back to the last persisted type without caching.
    pub async fn detect_machine_type(&self) -> MachineType {
        let probed = self.probe_machine_type().await;
        if probed != MachineType::Unknown {
            return probed;
        }
        match crate::db::get_port_hardware(&self.com_port).await {
            Ok(Some(hw)) => MachineType::from_db(&hw.machine_type),
            _ => MachineType::Unknown,
        }
    }

    pub async fn probe_machine_type(&self) -> MachineType {
        if self.is_esim_mode().await {
            if let Some(cached) = *self.machine_type.read().await {
                return cached;
            }
        }
        // A port in eSIM mode does not answer AT commands.
        let probed = if self.is_esim_mode().await {
            MachineType::Unknown
        } else {
            match tokio::time::timeout(Duration::from_secs(5), self.send_command("ath?\r\n")).await {
                Ok(Ok(resp)) => {
                    let detected = MachineType::from_type_reply(&resp);
                    if detected != MachineType::Unknown {
                        let reply = Self::format_log(resp.trim());
                        if let Err(e) = crate::db::upsert_port_machine_type(
                            &self.com_port,
                            detected.as_str(),
                            Some(&reply),
                        )
                        .await
                        {
                            warn!("[{}] failed to persist machine type: {}", self.com_port, e);
                        }
                    }
                    detected
                }
                _ => MachineType::Unknown,
            }
        };
        if probed != MachineType::Unknown {
            *self.machine_type.write().await = Some(probed);
        }
        probed
    }

    pub async fn detect_esim_capability(&self) -> bool {
        self.probe_machine_type().await == MachineType::Esim
    }

    /// Send a single mode-switch command (e.g. `atl6`) and require an `OK` reply.
    async fn send_switch_command(&self, cmd: &str) -> io::Result<()> {
        const SWITCH_TIMEOUT_SECS: u64 = 10;
        let started = Instant::now();
        let full = format!("{}\r\n", cmd);
        let resp = tokio::time::timeout(Duration::from_secs(SWITCH_TIMEOUT_SECS), self.send_command(&full))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{} timed out after {} ms",
                        cmd,
                        started.elapsed().as_millis()
                    ),
                )
            })??;
        if resp.contains("OK") {
            debug!(
                "[{}] switch {} ok in {} ms: {}",
                self.com_port,
                cmd,
                started.elapsed().as_millis(),
                Self::format_log(&resp)
            );
            Ok(())
        } else {
            warn!(
                "[{}] switch {} non-ok in {} ms: {}",
                self.com_port,
                cmd,
                started.elapsed().as_millis(),
                Self::format_log(&resp)
            );
            Err(io::Error::other(format!(
                "{} did not return OK: {}",
                cmd,
                Self::format_log(&resp)
            )))
        }
    }

    /// Enter eSIM-management mode: `atl6` then `ath7`. A Windows smart-card
    /// device appears after `ath7`. Sets `esim_mode` on success.
    pub async fn esim_enter(&self) -> io::Result<()> {
        // Mark as eSIM-busy before switching so periodic SIM probes skip this port
        // while atl6/ath7 are in flight.
        self.set_esim_mode(true).await;

        if let Err(e) = self.send_switch_command("atl6").await {
            self.set_esim_mode(false).await;
            return Err(e);
        }

        if let Err(e) = self.send_switch_command("ath7").await {
            self.set_esim_mode(false).await;
            return Err(e);
        }

        Ok(())
    }

    /// Exit eSIM-management mode: `atl7` then `ath6`. Always clears `esim_mode`
    /// (even on partial failure) so the port is never left flagged as busy.
    pub async fn esim_exit(&self) -> io::Result<()> {
        let r1 = self.send_switch_command("atl7").await;
        let r2 = self.send_switch_command("ath6").await;
        self.set_esim_mode(false).await;
        match (r1, r2) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e1), Ok(_)) => {
                let msg = e1.to_string().to_ascii_lowercase();
                if msg.contains("atl7") && msg.contains("timed out") {
                    log::warn!(
                        "[{}] atl7 timed out but ath6 succeeded; treating exit as recovered",
                        self.com_port
                    );
                    Ok(())
                } else {
                    Err(e1)
                }
            }
            (Ok(_), Err(e2)) => Err(e2),
            (Err(e1), Err(e2)) => Err(io::Error::other(format!(
                "exit failed (atl7: {}; ath6: {})",
                e1, e2
            ))),
        }
    }

    /// Abnormal-state recovery: `ath0` restores factory defaults. Clears `esim_mode`.
    pub async fn esim_reset(&self) -> io::Result<()> {
        let r = self.send_switch_command("ath0").await;
        self.set_esim_mode(false).await;
        r
    }
}

#[cfg(test)]
mod serial_reopen_tests {
    use super::*;

    #[tokio::test]
    async fn msim_shutdown_waits_for_task_resources_to_drop() {
        struct Resource(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Resource {
            fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
        }
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let resource = Resource(released.clone());
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = Mutex::new(Some(tokio::spawn(async move {
            let _resource = resource;
            let _ = started.send(());
            futures_util::future::pending::<()>().await;
        })));
        ready.await.unwrap();
        stop_serial_task(&task).await;
        assert!(released.load(Ordering::SeqCst));
        stop_serial_task(&task).await;
    }

    #[tokio::test]
    async fn msim_cancelled_queued_selection_never_executes() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        drop(response_rx);
        sender.send(ATCommand { command: "ath4\r\n".into(), response_tx,
            _priority: 5, retries: 0, counted: true }).unwrap();
        drop(sender);
        let count = Arc::new(AtomicUsize::new(1));
        let result = tokio::time::timeout(Duration::from_millis(100), Modem::command_processor(
            receiver, Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(ReaderState { response_tx: None, raw_response_tx: None })),
            "cancelled-selection-test".into(), count.clone(), Arc::new(Mutex::new(())),
        )).await;
        assert!(result.is_ok(), "a cancelled command attempted execution/retry");
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[ignore = "requires MODEM_TEST_PORT and exclusive access to a port with a ready SIM"]
    async fn serial_port_reopens_with_ready_sim() {
        let port = std::env::var("MODEM_TEST_PORT").expect("set MODEM_TEST_PORT");
        for cycle in 0..2 {
            assert!(Modem::quick_probe_port(&port, 115200).await, "quick probe cycle {cycle}");
            tokio::time::sleep(Duration::from_millis(500)).await;
            let modem = Modem::new(&port, 115200, "serial_reopen_test", 0, false)
                .await.expect("open test modem");
            assert!(modem.is_responsive().await, "AT cycle {cycle}");
            assert_eq!(modem.get_sim_status().await.unwrap().as_deref(), Some("READY"));
            assert!(modem.get_sim_iccid().await.unwrap().is_some());
            modem.shutdown().await;
            drop(modem);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

impl Modem {
    const SERIAL_OPEN_TIMEOUT_SECS: u64 = 20;

    async fn create_serial_connection(com_port: &str, baud_rate: u32) -> io::Result<SerialStream> {
        let port_name = com_port.to_string();
        let port_name_for_err = port_name.clone();
        let open_task = tokio::task::spawn_blocking(move || {
            tokio_serial::new(&port_name, baud_rate)
                .timeout(Duration::from_secs(10))
                .flow_control(tokio_serial::FlowControl::None)
                .open_native_async()
                .map_err(|e| {
                    let kind = match e.kind() {
                        tokio_serial::ErrorKind::Io(kind) => kind,
                        _ => io::ErrorKind::Other,
                    };
                    io::Error::new(kind, format!("Failed to open {}: {}", port_name, e))
                })
        });

        match tokio::time::timeout(
            Duration::from_secs(Self::SERIAL_OPEN_TIMEOUT_SECS),
            open_task,
        )
        .await
        {
            Ok(joined) => match joined {
                Ok(result) => result,
                Err(e) => Err(io::Error::other(format!(
                    "Serial open worker failed for {}: {}",
                    port_name_for_err, e
                ))),
            },
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "Opening serial port {} timed out after {}s",
                    com_port,
                    Self::SERIAL_OPEN_TIMEOUT_SECS
                ),
            )),
        }
    }

    /// Quick probe used by the reconnection worker: open a port with a short
    /// timeout, send AT+CPIN?, and report whether a SIM (or at least a modem
    /// responding to CPIN) is present. This avoids the ~10-20 second cost of a
    /// full modem initialization on every empty port.
    pub async fn quick_probe_port(com_port: &str, baud_rate: u32) -> bool {
        const OPEN_TIMEOUT: Duration = Duration::from_secs(2);
        const READ_TIMEOUT: Duration = Duration::from_millis(300);
        const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);

        let open_result = tokio::time::timeout(
            OPEN_TIMEOUT,
            tokio::task::spawn_blocking({
                let port = com_port.to_string();
                move || {
                    tokio_serial::new(&port, baud_rate)
                        .timeout(READ_TIMEOUT)
                        .flow_control(tokio_serial::FlowControl::None)
                        .open_native_async()
                        .map_err(|e| io::Error::other(format!("Failed to open {}: {}", port, e)))
                }
            }),
        )
        .await;

        let mut stream = match open_result {
            Ok(Ok(Ok(stream))) => stream,
            _ => return false,
        };

        // Drain any stale bytes that may be sitting in the port buffer.
        let mut drain = [0u8; 256];
        while let Ok(Ok(n)) =
            tokio::time::timeout(Duration::from_millis(100), stream.read(&mut drain)).await
        {
            if n == 0 {
                break;
            }
        }

        if stream.write_all(b"AT+CPIN?\r\n").await.is_err() {
            return false;
        }
        if stream.flush().await.is_err() {
            return false;
        }

        let mut buf = [0u8; 512];
        let mut response = String::new();
        let deadline = tokio::time::Instant::now() + RESPONSE_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    response.push_str(&String::from_utf8_lossy(&buf[..n]));
                    if response.contains("\r\nOK\r\n") || response.contains("\r\nERROR") {
                        break;
                    }
                }
                _ => break,
            }
        }

        let upper = response.to_uppercase();
        if upper.contains("+CPIN:") && !upper.contains("REMOVED") && !upper.contains("NOT INSERTED")
        {
            return true;
        }
        // A plain OK with no CPIN info is unusual for a modem; still treat it as
        // present so the full init can decide.
        upper.contains("OK")
    }

    // ─── Background reader task ────────────────────────────────────────────────

    /// Long-lived task that owns the read half of the serial stream.
    /// Routes bytes: when `response_tx` is set → command-response buffer;
    ///               otherwise → URC channel, line by line.
    async fn reader_task_main(
        read_half: ReadHalf<SerialStream>,
        reader_state: Arc<Mutex<ReaderState>>,
        urc_tx: mpsc::UnboundedSender<String>,
        write_half: Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
        name: String,
        com_port: String,
        baud_rate: u32,
        connection_state: Arc<RwLock<ConnectionState>>,
    ) {
        let mut read_half = Some(read_half);
        let mut raw_buf = [0u8; 256];
        let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
        while let Some(reader) = read_half.as_mut() {
            match reader.read(&mut raw_buf).await {
                Ok(0) => {
                    error!("Reader [{}]: EOF on serial port", name);
                    let ok = Self::reader_do_reconnect(
                        &mut read_half,
                        &write_half,
                        &reader_state,
                        &com_port,
                        baud_rate,
                        &connection_state,
                        &name,
                    )
                    .await;
                    line_buf.clear();
                    if !ok {
                        break;
                    }
                }
                Ok(n) => {
                    line_buf.extend_from_slice(&raw_buf[..n]);
                    // Raw-byte transfers (AT+QFDWL: MMS content, call recordings) can
                    // legitimately exceed MAX_LINE_BUF -- they're bounded by their own
                    // timeout in download_file(), not by this guard. Clearing the buffer
                    // mid-transfer would silently truncate the download (this previously
                    // corrupted MMS content fetches larger than 64KB). Only apply the
                    // overflow guard to string/URC-mode buffers.
                    let is_raw_mode = reader_state.lock().await.raw_response_tx.is_some();
                    if !is_raw_mode && line_buf.len() > MAX_LINE_BUF {
                        error!("Reader [{}]: buffer overflow, clearing", name);
                        let mut state = reader_state.lock().await;
                        if let Some(tx) = state.response_tx.take() {
                            let _ = tx.send(Err(io::Error::other("Buffer overflow")));
                        }
                        line_buf.clear();
                        continue;
                    }
                    Self::drain_reader_buf(&mut line_buf, &reader_state, &urc_tx, &name).await;
                }
                Err(e) => {
                    error!("Reader [{}]: read error: {}", name, e);
                    let ok = Self::reader_do_reconnect(
                        &mut read_half,
                        &write_half,
                        &reader_state,
                        &com_port,
                        baud_rate,
                        &connection_state,
                        &name,
                    )
                    .await;
                    line_buf.clear();
                    if !ok {
                        break;
                    }
                }
            }
        }
        error!("Reader task [{}] exiting", name);
    }

    /// Reconnect: notify any pending command, reset state, attempt up to 3 reconnects.
    /// Returns `true` on success, `false` after all attempts fail.
    async fn reader_do_reconnect(
        read_half: &mut Option<ReadHalf<SerialStream>>,
        write_half: &Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
        reader_state: &Arc<Mutex<ReaderState>>,
        com_port: &str,
        baud_rate: u32,
        connection_state: &Arc<RwLock<ConnectionState>>,
        name: &str,
    ) -> bool {
        {
            let mut state = reader_state.lock().await;
            if let Some(tx) = state.response_tx.take() {
                let _ = tx.send(Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Serial port disconnected",
                )));
            }
        }
        *connection_state.write().await = ConnectionState::Disconnected;
        *write_half.lock().await = None;
        drop(read_half.take());

        info!("Attempting to reconnect {} on {}", name, com_port);
        for attempt in 1..=3u32 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match Self::create_serial_connection(com_port, baud_rate).await {
                Ok(new_stream) => {
                    let (new_read, new_write) = tokio::io::split(new_stream);
                    *write_half.lock().await = Some(new_write);
                    *read_half = Some(new_read);
                    *connection_state.write().await = ConnectionState::Connected;
                    info!("Reconnected {} on {}", name, com_port);
                    return true;
                }
                Err(e) => {
                    error!("Reconnect attempt {} failed for {}: {}", attempt, name, e);
                }
            }
        }
        error!("Failed to reconnect {} after 3 attempts", name);
        false
    }

    /// Drain `line_buf`, routing its content to command response or URC channel.
    async fn drain_reader_buf(
        line_buf: &mut Vec<u8>,
        reader_state: &Arc<Mutex<ReaderState>>,
        urc_tx: &mpsc::UnboundedSender<String>,
        name: &str,
    ) {
        loop {
            let (has_str_tx, has_raw_tx) = {
                let state = reader_state.lock().await;
                (state.response_tx.is_some(), state.raw_response_tx.is_some())
            };
            if has_str_tx || has_raw_tx {
                // Command-response mode: buffer until we see a terminator. Raw-byte
                // responses (AT+QFDWL) use a restricted terminator set that excludes
                // the interim CONNECT\r\n prompt -- see RAW_TERMINATORS.
                let found = if has_raw_tx {
                    Self::find_raw_terminator(line_buf)
                } else {
                    Self::find_terminator(line_buf)
                };
                if let Some((term, pos)) = found {
                    // For error terminators (e.g. +CMS ERROR), scan ahead to include
                    // the full error code (e.g. "+CMS ERROR: 500") instead of cutting
                    // off at the prefix.
                    let end = if ERROR_TERMINATORS.contains(&term) {
                        let after_prefix = pos + term.len();
                        // Look for the trailing \r\n to capture the complete error line
                        if let Some(rn_pos) = line_buf[after_prefix..]
                            .windows(2)
                            .position(|w| w == b"\r\n")
                        {
                            after_prefix + rn_pos + 2
                        } else {
                            // Trailing \r\n not yet received — wait for more data
                            break;
                        }
                    } else {
                        pos + term.len()
                    };
                    let mut state = reader_state.lock().await;
                    if let Some(tx) = state.raw_response_tx.take() {
                        // Raw bytes mode (e.g. AT+QFDWL binary download)
                        let data = line_buf[..end].to_vec();
                        drop(state);
                        line_buf.drain(..end);
                        let _ = tx.send(Ok(data));
                    } else if let Some(tx) = state.response_tx.take() {
                        let response = String::from_utf8_lossy(&line_buf[..end]).into_owned();
                        debug!("RX [{}]: {}", name, Self::format_log(&response));
                        drop(state);
                        line_buf.drain(..end);
                        let _ = tx.send(Ok(response.clone()));
                        // Some modems (e.g. EC20F) embed call-state URCs like NO CARRIER
                        // inside the command response (before OK). Re-forward them so the
                        // URC handler can update the call record.
                        Self::forward_embedded_call_urcs(&response, urc_tx, name);
                    } else {
                        drop(state);
                        line_buf.drain(..end);
                    }
                    // Loop continues: remaining bytes may be URCs
                } else {
                    break; // Need more bytes
                }
            } else {
                // URC mode: dispatch one complete \r\n-terminated line per iteration
                if let Some(pos) = line_buf.windows(2).position(|w| w == b"\r\n") {
                    let line_bytes = line_buf[..pos].to_vec();
                    line_buf.drain(..pos + 2);
                    if !line_bytes.is_empty() {
                        let line = String::from_utf8_lossy(&line_bytes).into_owned();
                        let trimmed = line.trim().to_string();
                        if !trimmed.is_empty() {
                            debug!("URC [{}]: {}", name, Self::format_log(&trimmed));
                            let _ = urc_tx.send(trimmed);
                        }
                    }
                    // Loop continues: more lines may be present
                } else {
                    break; // Need more bytes
                }
            }
        }
    }

    // ─── Command processor ─────────────────────────────────────────────────────

    /// Sequential command processor: one ATCommand at a time, with retry on transient errors.
    async fn command_processor(
        mut command_rx: mpsc::UnboundedReceiver<ATCommand>,
        write_half: Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
        reader_state: Arc<Mutex<ReaderState>>,
        name: String,
        pending_commands: Arc<AtomicUsize>,
        command_lock: Arc<Mutex<()>>,
    ) {
        while let Some(mut at_command) = command_rx.recv().await {
            loop {
                if at_command.response_tx.is_closed() {
                    if at_command.counted { pending_commands.fetch_sub(1, Ordering::SeqCst); }
                    break;
                }
                match Self::execute_command_impl(
                    &write_half,
                    &reader_state,
                    &at_command.command,
                    &name,
                    Duration::from_secs(8),
                    &command_lock,
                )
                .await
                {
                    Ok(response) => {
                        let _ = at_command.response_tx.send(Ok(response));
                        if at_command.counted {
                            pending_commands.fetch_sub(1, Ordering::SeqCst);
                        }
                        break;
                    }
                    Err(_e) if at_command.retries < MAX_RETRIES => {
                        at_command.retries += 1;
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    Err(e) => {
                        let _ = at_command.response_tx.send(Err(e));
                        if at_command.counted {
                            pending_commands.fetch_sub(1, Ordering::SeqCst);
                        }
                        break;
                    }
                }
            }
        }
    }

    /// Send one AT command and await its response.
    /// Write lock is released immediately after flushing, before waiting for the response,
    /// so reconnect logic is never blocked by a pending command timeout.
    async fn execute_command_impl(
        write_half: &Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
        reader_state: &Arc<Mutex<ReaderState>>,
        command: &str,
        name: &str,
        timeout_dur: Duration,
        command_lock: &Arc<Mutex<()>>,
    ) -> io::Result<String> {
        // Held for the whole write+response round trip so no other sender can
        // steal this command's response out of `reader_state.response_tx`.
        let _command_guard = command_lock.lock().await;
        Self::execute_command_locked(write_half, reader_state, command, name, timeout_dur, &_command_guard).await
    }

    async fn execute_command_locked(
        write_half: &Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
        reader_state: &Arc<Mutex<ReaderState>>,
        command: &str,
        name: &str,
        timeout_dur: Duration,
        _guard: &tokio::sync::MutexGuard<'_, ()>,
    ) -> io::Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<io::Result<String>>();
        {
            let mut write_guard = write_half.lock().await;
            let write = write_guard.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "Serial port not connected")
            })?;
            reader_state.lock().await.response_tx = Some(tx);
            debug!("TX [{}]: {}", name, Self::format_log(command));
            write.write_all(command.as_bytes()).await?;
            write.flush().await?;
        } // write_guard drops here, releasing the write lock before waiting for response
        match tokio::time::timeout(timeout_dur, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Reader channel closed",
            )),
            Err(_) => {
                reader_state.lock().await.response_tx = None;
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("Command timeout for device {}", name),
                ))
            }
        }
    }

    /// Forward any call-state URC lines embedded inside a command response buffer.
    /// EC20F modems can include e.g. `NO CARRIER` before `OK` in the ATH response.
    fn forward_embedded_call_urcs(
        response: &str,
        urc_tx: &mpsc::UnboundedSender<String>,
        name: &str,
    ) {
        for line in response.split("\r\n") {
            let trimmed = line.trim();
            if matches!(trimmed, "NO CARRIER" | "BUSY" | "RING" | "CONNECT")
                || trimmed.starts_with("VOICE CALL: END")
                || trimmed.starts_with("+CLIP:")
                || trimmed.starts_with("+CEND:")
                || trimmed.starts_with("+CUSD:")
            {
                debug!("Re-forwarding embedded URC [{}]: {}", name, trimmed);
                let _ = urc_tx.send(trimmed.to_string());
            }
        }
    }

    fn find_terminator(buffer: &[u8]) -> Option<(&'static [u8], usize)> {
        TERMINATORS
            .iter()
            .filter_map(|&t| {
                buffer
                    .windows(t.len())
                    .position(|w| w == t)
                    .map(|pos| (t, pos))
            })
            .max_by_key(|&(_, pos)| pos)
    }

    /// Like `find_terminator`, but for raw-byte responses -- see `RAW_TERMINATORS`.
    fn find_raw_terminator(buffer: &[u8]) -> Option<(&'static [u8], usize)> {
        RAW_TERMINATORS
            .iter()
            .filter_map(|&t| {
                buffer
                    .windows(t.len())
                    .position(|w| w == t)
                    .map(|pos| (t, pos))
            })
            .max_by_key(|&(_, pos)| pos)
    }

    pub async fn init_modem(&self, sms_storage: Option<SmsStorage>) -> io::Result<()> {
        let init_commands = vec![
            ("ATE0\r\n", "Disable echo"),
            ("AT+CMEE=1\r\n", "Enable error messages"),
            ("AT+CMGF=0\r\n", "Set PDU mode"),
            ("AT+CSCS=\"UCS2\"\r\n", "Set character encoding"),
            ("AT+CLIP=1\r\n", "Enable caller ID (CLIP)"),
        ];

        for (cmd, description) in init_commands {
            if let Err(e) = self.send_command_with_ok(cmd).await {
                error!("Failed to {}: {}", description, e);
                return Err(e);
            }
        }

        if let Some(storage) = sms_storage {
            if let Err(e) = self.configure_sms_storage(storage).await {
                log::warn!(
                    "Failed to configure SMS storage for device {} (no SIM inserted?): {}",
                    self.name,
                    e
                );
            }
        }

        if let Err(e) = self.init_sim_info().await {
            log::warn!(
                "Failed to initialize SIM info for device {}: {}",
                self.name,
                e
            );
        }

        Ok(())
    }

    async fn configure_sms_storage(&self, storage: SmsStorage) -> io::Result<()> {
        let storage_str = match storage {
            SmsStorage::SIM => "SM",
            SmsStorage::ME => "ME",
            SmsStorage::MT => "MT",
        };

        let cmd = format!("AT+CPMS=\"{0}\",\"{0}\",\"{0}\"\r\n", storage_str);

        match self.send_command_with_ok(&cmd).await {
            Ok(_) => {
                info!(
                    "SMS storage set to {} for device {}",
                    storage_str, self.name
                );
                Ok(())
            }
            Err(e) => {
                error!("Failed to set SMS storage: {}", e);
                Err(e)
            }
        }
    }

    /// Re-probe ICCID/IMSI/phone and refresh the cached `sim_id`. Exposed so callers
    /// (e.g. the eSIM module) can force a refresh after switching profiles, instead
    /// of waiting for the next hot-swap detection cycle.
    pub(crate) async fn init_sim_info(&self) -> anyhow::Result<()> {
        let (iccid_result, imsi_result, phone_result) = tokio::join!(
            self.get_sim_iccid(),
            self.get_sim_imsi(),
            self.get_phone_number()
        );

        let iccid = iccid_result.ok().flatten();
        let imsi = imsi_result.ok().flatten();
        // Normalize the modem-reported MSISDN before storing: digits only,
        // no `+` prefix, no leading zeros.
        let phone_number = phone_result
            .ok()
            .flatten()
            .map(|p| crate::phone_number::normalize_msisdn(&p))
            .filter(|p| !p.is_empty());
        let should_force_china_unicom = self.force_uk_mcc_to_46001
            && imsi
                .as_deref()
                .is_some_and(|v| v.starts_with("234") || v.starts_with("235"));

        if let Some(iccid) = iccid {
            match SimCard::find_or_create_with_phone(&iccid, imsi.clone(), phone_number).await {
                Ok(_) => {
                    *self.sim_id.write().await = Some(iccid.clone());
                    info!(
                        "SIM card initialized for device {}: ICCID={}, IMSI={}",
                        self.name,
                        iccid,
                        imsi.as_deref().unwrap_or("unknown")
                    );

                    if should_force_china_unicom {
                        info!(
                            "Forcing operator 46001 for UK MCC SIM on device {}: ICCID={}, IMSI={}",
                            self.name,
                            iccid,
                            imsi.as_deref().unwrap_or("unknown")
                        );
                        if let Err(e) = self.send_cops_command().await {
                            log::warn!(
                                "Failed to force operator 46001 for UK MCC SIM on device {}: {}",
                                self.name,
                                e
                            );
                        } else {
                            info!(
                                "Forced operator 46001 for UK MCC SIM on device {}",
                                self.name
                            );
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to store SIM card info: {}", e);
                    return Err(e);
                }
            }
        } else {
            log::warn!("Could not retrieve SIM card ICCID for device {}", self.name);
        }

        Ok(())
    }

    /// Send AT+COPS command with extended timeout (20s) since network registration
    /// can take longer when the device is searching for operators, especially for
    /// forced operator selection on UK MCC SIMs (234/235 -> 46001).
    async fn send_cops_command(&self) -> io::Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<io::Result<String>>();

        let at_command = ATCommand {
            command: "AT+COPS=1,2,\"46001\"\r\n".to_string(),
            response_tx: tx,
            _priority: 5,
            retries: 0,
            counted: false,
        };

        self.command_tx
            .send(at_command)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Command queue closed"))?;

        // Use 20-second timeout instead of default 8 seconds.
        match tokio::time::timeout(Duration::from_secs(20), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Response channel closed",
            )),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "AT+COPS command timed out after 20s",
            )),
        }
    }

    /// Send an AT command with a custom outer timeout. The command processor
    /// still caps each attempt at 8s with retries; this only bounds the total wait.
    async fn send_command_outer_timeout(&self, command: &str, timeout_secs: u64) -> io::Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<io::Result<String>>();

        let at_command = ATCommand {
            command: command.to_string(),
            response_tx: tx,
            _priority: 5,
            retries: 0,
            counted: false,
        };

        self.command_tx
            .send(at_command)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Command queue closed"))?;

        match tokio::time::timeout(Duration::from_secs(timeout_secs), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Response channel closed",
            )),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("AT command timed out after {}s", timeout_secs),
            )),
        }
    }

    /// Force network registration to China Unicom (46001) using only
    /// AT+COPS=1,2,"46001",7.
    pub async fn force_register(&self) -> Result<(), String> {
        self.register_cops_46001().await?;
        info!("[force_register] AT+COPS succeeded on {}", self.com_port);
        Ok(())
    }

    /// Force network registration to China Unicom 46000 using only
    /// AT+COPS=1,2,"46000",7.
    pub async fn register_cops_46000(&self) -> Result<(), String> {
        let response = self
            .send_command_outer_timeout("AT+COPS=1,2,\"46000\",7\r\n", 30)
            .await
            .map_err(|e| format!("AT+COPS failed: {}", e))?;
        if !response.contains("OK") {
            return Err(format!("AT+COPS failed: {}", Self::format_log(&response)));
        }
        Ok(())
    }

    /// Manually register to China Unicom 46001 (LTE) via AT+COPS=1,2,"46001",7.
    /// Uses a 30s outer timeout since network registration is slow.
    pub async fn register_cops_46001(&self) -> Result<(), String> {
        let response = self
            .send_command_outer_timeout("AT+COPS=1,2,\"46001\",7\r\n", 30)
            .await
            .map_err(|e| format!("AT+COPS failed: {}", e))?;
        if !response.contains("OK") {
            return Err(format!("AT+COPS failed: {}", Self::format_log(&response)));
        }
        Ok(())
    }

    /// Reboot the module via AT+CFUN=1,1. The module may reset before replying,
    /// so OK, timeout, or a dropped connection all count as "reboot initiated".
    pub async fn reboot(&self) {
        let _ = self
            .send_command_outer_timeout("AT+CFUN=1,1\r\n", 8)
            .await;
    }

    /// Manual module reset on a live connection; returns the reply (or the error) for logging.
    pub async fn reset_module(&self) -> String {
        match self.send_command_outer_timeout("AT+CFUN=1,1\r\n", 8).await {
            Ok(response) => Self::format_log(&response),
            Err(error) => format!("no reply ({})", error),
        }
    }

    /// Send AT+CFUN=1,1 on a port that has no live modem handle. Returns false if the
    /// port cannot be opened or nothing answers.
    pub async fn send_reset_on_port(com_port: &str, baud_rate: u32) -> bool {
        let open_result = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking({
                let port = com_port.to_string();
                move || {
                    tokio_serial::new(&port, baud_rate)
                        .timeout(Duration::from_millis(300))
                        .flow_control(tokio_serial::FlowControl::None)
                        .open_native_async()
                }
            }),
        )
        .await;
        let Ok(Ok(Ok(mut stream))) = open_result else {
            return false;
        };
        if stream.write_all(b"AT+CFUN=1,1\r\n").await.is_err() || stream.flush().await.is_err() {
            return false;
        }
        let mut buf = [0u8; 256];
        let mut response = String::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(300), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    response.push_str(&String::from_utf8_lossy(&buf[..n]));
                    if response.contains("OK") || response.contains("ERROR") {
                        break;
                    }
                }
                Ok(Err(_)) => break,
                _ => {}
            }
        }
        info!("[manual-restart {}] reset reply without live handle: {:?}", com_port, response.trim());
        response.contains("OK")
    }

    /// Quick liveness probe: send AT with a short timeout and expect OK.
    pub async fn probe_alive(&self) -> bool {
        matches!(
            self.send_command_outer_timeout("AT\r\n", 3).await,
            Ok(resp) if resp.contains("OK")
        )
    }

    async fn send_command_priority(&self, command: &str, priority: u8) -> io::Result<String> {
        // Bound concurrent callers; cancelled queued commands are discarded by
        // command_processor before execution or retry.
        //
        // NOTE: a single /api/sims/info request legitimately fires 8 concurrent AT
        // commands per modem (see get_all_sim_info's tokio::join! of signal/network/
        // operator/model/sms_center/sim_status/memory_status/imei futures), so the
        // threshold must comfortably exceed that to avoid self-rejecting a normal,
        // isolated request (which would otherwise also starve modem initialization).
        const MAX_QUEUED_COMMANDS: usize = 16;
        if self.pending_commands.load(Ordering::SeqCst) >= MAX_QUEUED_COMMANDS {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("Command queue backlogged for device {}", self.name),
            ));
        }

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let at_command = ATCommand {
            command: command.to_string(),
            response_tx,
            _priority: priority,
            retries: 0,
            counted: true,
        };

        self.pending_commands.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = self.command_tx.send(at_command) {
            self.pending_commands.fetch_sub(1, Ordering::SeqCst);
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, format!("Command queue closed: {}", e)));
        }

        response_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Response channel closed"))?
    }

    pub async fn send_command(&self, command: &str) -> io::Result<String> {
        self.send_command_priority(command, 5).await
    }

    async fn send_command_with_ok(&self, command: &str) -> io::Result<String> {
        let response = self.send_command(command).await?;

        if response.contains("OK\r\n") {
            Ok(response)
        } else {
            // Non-OK responses (e.g. +CME ERROR: 10/13 for absent SIMs) are
            // expected on empty ports; the error is propagated so callers log
            // it with context. Keep this low-level line at debug to avoid noise.
            debug!("Command failed: {}", Self::format_log(&response));
            Err(io::Error::other(format!(
                "Command failed: {}",
                Self::format_log(&response)
            )))
        }
    }

    /// Send a PDU SMS atomically: holds the write mutex across both the AT+CMGS prompt step
    /// and the PDU data step, preventing other AT commands from slipping in between and
    /// corrupting the modem's PDU input state.
    async fn send_pdu_atomic(&self, tpdu_len: usize, pdu_hex: &str) -> anyhow::Result<()> {
        let setup_cmd = format!("AT+CMGS={}\r", tpdu_len);
        let full_pdu = format!("{}\x1A", pdu_hex);

        // Hold command_lock for the whole exchange so a queued command can't
        // steal the CMGS prompt/response out of reader_state.response_tx.
        let _command_guard = self.command_lock.lock().await;
        // Hold write lock for the entire two-step PDU sequence
        let mut write_guard = self.write_half.lock().await;
        let write = write_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Serial port not connected"))?;

        // Step 1: Send AT+CMGS=<n>\r and wait for the '>' prompt.
        let (tx1, rx1) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx1);
        debug!("TX [{}]: {}", self.name, Self::format_log(&setup_cmd));
        write.write_all(setup_cmd.as_bytes()).await?;
        write.flush().await?;
        let prompt = match tokio::time::timeout(Duration::from_secs(8), rx1).await {
            Ok(Ok(Ok(r))) => r,
            Ok(Ok(Err(e))) => return Err(e.into()),
            Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
            Err(_) => return Err(anyhow::anyhow!("Timeout waiting for SMS prompt")),
        };
        if !prompt.contains("> ") {
            return Err(anyhow::anyhow!(
                "SMS prompt not received: {}",
                Self::format_log(&prompt)
            ));
        }

        // Step 2: Send PDU + Ctrl-Z and wait for +CMGS:/OK.
        // Use a 30-second timeout — network transmission can be slow.
        let (tx2, rx2) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx2);
        debug!(
            "TX [{}]: <PDU {} bytes + Ctrl-Z>",
            self.name,
            pdu_hex.len() / 2
        );
        write.write_all(full_pdu.as_bytes()).await?;
        write.flush().await?;
        let final_response = match tokio::time::timeout(Duration::from_secs(30), rx2).await {
            Ok(Ok(Ok(r))) => r,
            Ok(Ok(Err(e))) => return Err(e.into()),
            Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
            Err(_) => return Err(anyhow::anyhow!("Timeout waiting for SMS send confirmation")),
        };

        if final_response.contains("OK\r\n") && final_response.contains("+CMGS:") {
            Ok(())
        } else {
            error!(
                "Incomplete SMS response: {}",
                Self::format_log(&final_response)
            );
            Err(anyhow::anyhow!("Incomplete SMS response"))
        }
    }

    async fn send_sms_content<F>(
        &self,
        setup_cmd: &str,
        message: &str,
        transform_fn: F,
    ) -> anyhow::Result<String>
    where
        F: FnOnce(&str) -> anyhow::Result<String>,
    {
        let prompt_response = self.send_command_priority(setup_cmd, 1).await?;

        if !prompt_response.contains("> ") {
            return Err(anyhow::anyhow!("SMS prompt not received"));
        }

        let transformed_message = transform_fn(message)?;
        let full_message = format!("{}\x1A", transformed_message);

        let final_response = self.send_command_priority(&full_message, 1).await?;

        if final_response.contains("OK\r\n") && final_response.contains("+CMGS:") {
            Ok(final_response)
        } else {
            error!(
                "Incomplete SMS response: {}",
                Self::format_log(&final_response)
            );
            Err(anyhow::anyhow!("Incomplete SMS response"))
        }
    }

    pub async fn send_sms_pdu(
        &self,
        contact: &Contact,
        message: &str,
    ) -> anyhow::Result<(i64, String)> {
        // Use contact.id if it looks like a phone number (starts with + or is all digits),
        // otherwise fall back to contact.name (contacts auto-created from received SMS
        // store the phone number in the name field with a UUID as the id).
        let phone = if contact.id.starts_with('+') || contact.id.chars().all(|c| c.is_ascii_digit())
        {
            contact.id.clone()
        } else {
            contact.name.clone()
        };
        info!("Sending SMS via PDU to {}: {}", phone, message);

        let sim_id = self.sim_id.read().await.clone().unwrap_or_default();

        let sms = Sms {
            id: 0,
            contact_id: contact.id.clone(),
            timestamp: now_gmt8().with_nanosecond(0).unwrap(),
            message: message.to_string(),
            sim_id,
            send: true,
            status: crate::db::SmsStatus::Loading,
            uploaded_to_platform: false,
            platform_item_id: None,
            platform_uploaded_at: None,
            platform_response: None,
        };

        let sms_id = sms.insert().await?;

        match self.send_pdu_message(&phone, message).await {
            Ok(_) => {
                Sms::update_status_by_id(sms_id, crate::db::SmsStatus::Read).await?;
                Ok((sms_id, contact.id.clone()))
            }
            Err(e) => {
                Sms::update_status_by_id(sms_id, crate::db::SmsStatus::Failed).await?;
                Err(e)
            }
        }
    }

    async fn send_pdu_message(&self, phone: &str, message: &str) -> anyhow::Result<()> {
        let pdus = build_pdus(phone, message, false)?;

        for (pdu_data, tpdu_length) in pdus {
            self.send_pdu_atomic(tpdu_length, &pdu_data).await?;
        }

        Ok(())
    }

    pub async fn read_sms_async_insert(
        &self,
        _sms_type: SmsType,
        sse_manager: Arc<SseManager>,
        webhook_manager: Option<webhook::WebhookManager>,
    ) -> anyhow::Result<()> {
        // Always read ALL messages so previously-read messages aren't missed
        let (sms_list, consumed) = self.read_sms(SmsType::All).await?;

        if consumed.is_empty() {
            return Ok(());
        }

        // Delete handled entries from modem FIRST so storage doesn't fill up.
        // Only the consumed indices are deleted: incomplete multipart fragments
        // and SMS that arrived after the listing stay for the next poll.
        if let Err(e) = self.delete_sms_indices(&consumed).await {
            log::warn!("Failed to delete SMS from modem after reading: {}", e);
        }

        if sms_list.is_empty() {
            return Ok(());
        }

        let webhook_future = async {
            if let Some(webhook_mgr) = webhook_manager {
                for sms in &sms_list {
                    if let Err(e) = webhook_mgr.send(sms.clone()) {
                        log::error!("Failed to send webhook: {}", e);
                    }
                }
            }
        };

        let db_future = async {
            match ModemSMS::bulk_insert(&sms_list).await {
                Ok(contact_ids) => {
                    if let Ok(conversations) =
                        crate::db::Conversation::query_by_contact_ids(&contact_ids).await
                    {
                        sse_manager.send(conversations);
                    }
                }
                Err(e) => {
                    log::error!("Insert SMS error: {}", e);
                    return false;
                }
            }
            true
        };

        let (_, sms_saved) = tokio::join!(webhook_future, db_future);
        if sms_saved {
            if let Err(error) = self.sync_welcome_sms_identity(&sms_list).await {
                log::warn!("[{}] Welcome SMS SIM sync failed: {}", self.com_port, error);
            }
        }

        // ── Upload received SMS to 火狐狸 platform ────────────────
        let sim_id = self.sim_id.read().await.clone().unwrap_or_default();
        if sim_id.is_empty() {
            log::warn!(
                "Skipping 火狐狸 SMS upload: SIM ID is empty on device {}",
                self.name
            );
        } else {
            match SimCard::query_all().await {
                Ok(cards) => {
                    if let Some(mut card) = cards.into_iter().find(|c| c.id == sim_id) {
                        if let Some(phone_num) = card.phone_number.clone().filter(|p| !p.trim().is_empty()) {
                            let mut country_id = card.country_code.clone().filter(|c| !c.trim().is_empty());

                            if country_id.is_none() {
                                match crate::db::FirefoxPlatformItem::find_latest_country_for_phone(&phone_num).await {
                                    Ok(resolved_country_id) => {
                                        if let Some(resolved_country_id) = resolved_country_id.filter(|c| !c.trim().is_empty()) {
                                            log::info!(
                                                "[{}] Backfilled SIM country_code from latest platform item for phone {}: {}",
                                                sim_id,
                                                phone_num,
                                                resolved_country_id
                                            );
                                            if let Err(e) = card.update_country_code(Some(resolved_country_id.clone())).await {
                                                log::warn!(
                                                    "[{}] Failed to persist backfilled country_code for phone {}: {}",
                                                    sim_id,
                                                    phone_num,
                                                    e
                                                );
                                            }
                                            country_id = Some(resolved_country_id);
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "[{}] Failed to resolve latest country_id for phone {}: {}",
                                            sim_id,
                                            phone_num,
                                            e
                                        );
                                    }
                                }
                            }

                            if let Some(country_id) = country_id {
                            match crate::db::AppSetting::get(crate::firefox_api::API_KEY_SETTING).await {
                                Ok(Some(api_key)) => {
                                    match reqwest::Client::builder()
                                        .timeout(std::time::Duration::from_secs(10))
                                        .build()
                                    {
                                        Ok(client) => {
                                            let normalized_phone = phone_num
                                                .chars()
                                                .filter(|c| c.is_ascii_digit())
                                                .collect::<String>()
                                                .trim_start_matches('0')
                                                .to_string();

                                            let wait_list_item_id = match crate::firefox_api::get_wait_phone_list(&client, &api_key).await {
                                                Ok(wait_resp) => {
                                                    wait_resp
                                                        .data
                                                        .and_then(|d| d.as_array().cloned())
                                                        .and_then(|items| {
                                                            items.into_iter().find_map(|item| {
                                                                let phone_match = item
                                                                    .get("Phone_Num")
                                                                    .and_then(|v| v.as_str())
                                                                    .map(|p| {
                                                                        let np = p
                                                                            .chars()
                                                                            .filter(|c| c.is_ascii_digit())
                                                                            .collect::<String>()
                                                                            .trim_start_matches('0')
                                                                            .to_string();
                                                                        !np.is_empty() && np == normalized_phone
                                                                    })
                                                                    .unwrap_or(false);

                                                                if !phone_match {
                                                                    return None;
                                                                }

                                                                item.get("Item_ID").and_then(|v| {
                                                                    if let Some(s) = v.as_str() {
                                                                        let trimmed = s.trim();
                                                                        if trimmed.is_empty() {
                                                                            None
                                                                        } else {
                                                                            Some(trimmed.to_string())
                                                                        }
                                                                    } else if let Some(n) = v.as_i64() {
                                                                        Some(n.to_string())
                                                                    } else if let Some(n) = v.as_u64() {
                                                                        Some(n.to_string())
                                                                    } else {
                                                                        None
                                                                    }
                                                                })
                                                            })
                                                        })
                                                }
                                                Err(e) => {
                                                    log::warn!(
                                                        "[{}] Failed to read wait-list for phone {}: {}",
                                                        sim_id,
                                                        phone_num,
                                                        e
                                                    );
                                                    None
                                                }
                                            };

                                            let latest_item_id = match crate::db::FirefoxPlatformItem::find_latest_item_for_phone(&phone_num).await {
                                                Ok(v) => v,
                                                Err(e) => {
                                                    log::warn!(
                                                        "[{}] Failed to resolve latest platform item for phone {}: {}",
                                                        sim_id,
                                                        phone_num,
                                                        e
                                                    );
                                                    None
                                                }
                                            };

                                            let preferred_item_id = if wait_list_item_id.is_some() {
                                                log::info!(
                                                    "[{}] Upload item source=wait-list for phone {}: {:?}",
                                                    sim_id,
                                                    phone_num,
                                                    wait_list_item_id
                                                );
                                                wait_list_item_id
                                            } else {
                                                log::info!(
                                                    "[{}] Upload item source=db-latest for phone {}: {:?}",
                                                    sim_id,
                                                    phone_num,
                                                    latest_item_id
                                                );
                                                latest_item_id
                                            };

                                            for sms in &sms_list {
                                                if sms.send {
                                                    continue;
                                                }

                                                let upload_content = if let Some(item_id) = preferred_item_id.as_deref() {
                                                    crate::db::build_upload_sms_content(item_id, &sms.message)
                                                        .await
                                                        .unwrap_or_else(|e| {
                                                            log::warn!(
                                                                "[{}] Failed to build upload content for item {}: {}, falling back to raw message",
                                                                sim_id,
                                                                item_id,
                                                                e
                                                            );
                                                            sms.message.clone()
                                                        })
                                                } else {
                                                    sms.message.clone()
                                                };

                                                match crate::firefox_api::upload_sms(
                                                    &client,
                                                    &api_key,
                                                    &country_id,
                                                    &phone_num,
                                                    &upload_content,
                                                )
                                                .await
                                                {
                                                    Ok(resp) if resp.code == "1" => {
                                                        info!(
                                                            "[{}] Uploaded incoming SMS to 火狐狸: phone={}, code={}, data={:?}",
                                                            sim_id,
                                                            phone_num,
                                                            resp.code,
                                                            resp.data
                                                        );

                                                        let response_json =
                                                            serde_json::to_string(&resp).ok();
                                                        if let Err(e) = crate::db::Sms::mark_uploaded_by_phone_message(
                                                            &phone_num,
                                                            &sim_id,
                                                            &[sms.message.clone()],
                                                            response_json,
                                                        )
                                                        .await
                                                        {
                                                            log::error!(
                                                                "[{}] Failed to mark SMS as uploaded after successful platform response: {}",
                                                                sim_id,
                                                                e
                                                            );
                                                        }
                                                    }
                                                    Ok(resp) => {
                                                        // Enqueue for retry instead of just logging
                                                        log::warn!(
                                                            "[{}] Failed to upload incoming SMS to 火狐狸 (platform rejected): phone={}, code={}, data={:?}",
                                                            sim_id,
                                                            phone_num,
                                                            resp.code,
                                                            resp.data
                                                        );

                                                        let response_json =
                                                            serde_json::to_string(&resp).ok();
                                                        if let Err(e) = crate::db::Sms::mark_platform_attempt_by_phone_message(
                                                            &phone_num,
                                                            &sim_id,
                                                            &[sms.message.clone()],
                                                            None,
                                                            false,
                                                            response_json,
                                                        )
                                                        .await
                                                        {
                                                            log::error!(
                                                                "[{}] Failed to mark incoming SMS as failed after platform rejection: {}",
                                                                sim_id,
                                                                e
                                                            );
                                                        }

                                                        if crate::firefox_api::is_unretryable_platform_rejection(&resp) {
                                                            log::warn!(
                                                                "[{}] Skipping retry enqueue for non-retryable rejection: phone={}, code={}, data={:?}",
                                                                sim_id,
                                                                phone_num,
                                                                resp.code,
                                                                resp.data
                                                            );
                                                        } else {
                                                            let sms_id = match crate::db::Sms::find_latest_incoming_id_by_sim_message(
                                                                &sim_id,
                                                                &sms.message,
                                                            )
                                                            .await
                                                            {
                                                                Ok(Some(id)) => id,
                                                                Ok(None) => {
                                                                    log::error!(
                                                                        "[{}] Failed to enqueue retry for incoming SMS: sms row not found (sim_id={}, phone={})",
                                                                        sim_id,
                                                                        sim_id,
                                                                        phone_num
                                                                    );
                                                                    continue;
                                                                }
                                                                Err(e) => {
                                                                    log::error!(
                                                                        "[{}] Failed to resolve sms_id for retry enqueue: {}",
                                                                        sim_id,
                                                                        e
                                                                    );
                                                                    continue;
                                                                }
                                                            };

                                                            let retry_item = crate::firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                                sms_id,
                                                                phone_num.to_string(),
                                                                country_id.to_string(),
                                                                sms.message.clone(),
                                                                format!("Auto-upload failed with code: {}", resp.code),
                                                                Some(resp.code.clone()),
                                                            );

                                                            if let Ok(pool) = crate::db::get_pool() {
                                                                if let Err(e) = crate::firefox_upload_retry::FirefoxUploadRetryItem::insert(pool, &retry_item).await {
                                                                    log::error!(
                                                                        "[{}] Failed to enqueue retry for incoming SMS: {}",
                                                                        sim_id,
                                                                        e
                                                                    );
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        log::warn!(
                                                            "[{}] Failed to upload incoming SMS to 火狐狸: phone={}, error={}",
                                                            sim_id,
                                                            phone_num,
                                                            e
                                                        );

                                                        let error_message =
                                                            format!("Auto-upload failed: {}", e);
                                                        if let Err(mark_err) = crate::db::Sms::mark_platform_attempt_by_phone_message(
                                                            &phone_num,
                                                            &sim_id,
                                                            &[sms.message.clone()],
                                                            None,
                                                            false,
                                                            Some(error_message.clone()),
                                                        )
                                                        .await
                                                        {
                                                            log::error!(
                                                                "[{}] Failed to mark incoming SMS as failed after upload error: {}",
                                                                sim_id,
                                                                mark_err
                                                            );
                                                        }

                                                        let sms_id = match crate::db::Sms::find_latest_incoming_id_by_sim_message(
                                                            &sim_id,
                                                            &sms.message,
                                                        )
                                                        .await
                                                        {
                                                            Ok(Some(id)) => id,
                                                            Ok(None) => {
                                                                log::error!(
                                                                    "[{}] Failed to enqueue retry for incoming SMS: sms row not found (sim_id={}, phone={})",
                                                                    sim_id,
                                                                    sim_id,
                                                                    phone_num
                                                                );
                                                                continue;
                                                            }
                                                            Err(e) => {
                                                                log::error!(
                                                                    "[{}] Failed to resolve sms_id for retry enqueue: {}",
                                                                    sim_id,
                                                                    e
                                                                );
                                                                continue;
                                                            }
                                                        };

                                                        let retry_item = crate::firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                            sms_id,
                                                            phone_num.to_string(),
                                                            country_id.to_string(),
                                                            sms.message.clone(),
                                                            format!("Auto-upload failed: {}", e),
                                                            None,
                                                        );

                                                        if let Ok(pool) = crate::db::get_pool() {
                                                            if let Err(e) = crate::firefox_upload_retry::FirefoxUploadRetryItem::insert(pool, &retry_item).await {
                                                                log::error!(
                                                                    "[{}] Failed to enqueue retry for incoming SMS: {}",
                                                                    sim_id,
                                                                    e
                                                                );
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::error!(
                                                "[{}] Failed to build HTTP client for 火狐狸 SMS upload: {}",
                                                sim_id,
                                                e
                                            );

                                            // Enqueue all SMS for retry when HTTP client fails
                                            for sms in &sms_list {
                                                if sms.send {
                                                    continue;
                                                }

                                                let sms_id = match crate::db::Sms::find_latest_incoming_id_by_sim_message(
                                                    &sim_id,
                                                    &sms.message,
                                                )
                                                .await
                                                {
                                                    Ok(Some(id)) => id,
                                                    Ok(None) => {
                                                        log::error!(
                                                            "[{}] Failed to enqueue retry after HTTP client build failure: sms row not found (sim_id={}, phone={})",
                                                            sim_id,
                                                            sim_id,
                                                            phone_num
                                                        );
                                                        continue;
                                                    }
                                                    Err(resolve_err) => {
                                                        log::error!(
                                                            "[{}] Failed to resolve sms_id after HTTP client build failure: {}",
                                                            sim_id,
                                                            resolve_err
                                                        );
                                                        continue;
                                                    }
                                                };

                                                let retry_item = crate::firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                    sms_id,
                                                    phone_num.to_string(),
                                                    country_id.to_string(),
                                                    sms.message.clone(),
                                                    format!("HTTP client build failed: {}", e),
                                                    None,
                                                );

                                                if let Ok(pool) = crate::db::get_pool() {
                                                    if let Err(enqueue_err) = crate::firefox_upload_retry::FirefoxUploadRetryItem::insert(pool, &retry_item).await {
                                                        log::error!(
                                                            "[{}] Failed to enqueue retry after HTTP client build failure: {}",
                                                            sim_id,
                                                            enqueue_err
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {
                                    log::warn!(
                                        "[{}] Skipping WorldCode SMS upload: app_settings.worldcode_api_key is not set",
                                        sim_id
                                    );
                                }
                                Err(e) => {
                                    log::warn!(
                                        "[{}] Skipping WorldCode SMS upload: failed to read worldcode_api_key: {}",
                                        sim_id,
                                        e
                                    );
                                }
                            }
                            } else {
                                log::warn!(
                                    "[{}] Skipping 火狐狸 SMS upload: SIM card missing country_code and no latest platform country mapping found",
                                    sim_id
                                );
                            }
                        } else {
                            log::warn!(
                                "[{}] Skipping 火狐狸 SMS upload: SIM card missing country_code or phone_number",
                                sim_id
                            );
                        }
                    } else {
                        log::warn!(
                            "[{}] Skipping 火狐狸 SMS upload: SIM card record not found in database",
                            sim_id
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[{}] Skipping 火狐狸 SMS upload: failed to query sim_cards: {}",
                        sim_id,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Reads all SMS from modem storage. Returns the regular SMS list plus the
    /// storage indices that were fully handled (SMS, completed multipart SMS,
    /// MMS WAP-push notifications, unparseable PDUs). Callers must delete only
    /// those indices (`delete_sms_indices`): fragments of a multipart SMS whose
    /// other parts have not arrived yet are left in storage so the next poll can
    /// complete them, and SMS arriving after this listing are not wiped unread.
    pub async fn read_sms(&self, sms_type: SmsType) -> io::Result<(Vec<ModemSMS>, Vec<u32>)> {
        let command = format!("AT+CMGL={}\r\n", sms_type.to_at_command_pdu());
        let response = self.send_command_with_ok(&command).await?;

        let sim_id = self.sim_id.read().await.clone().unwrap_or_default();
        let trimmed = response.trim();
        if trimmed != "OK" && !trimmed.is_empty() {
            log::debug!("[{}] AT+CMGL raw response: {}", sim_id, trimmed);
        }
        let batch = parse_pdu_sms(&response, &sim_id);
        let mut sms_list = batch.messages;
        let mut consumed = batch.consumed_indices;
        let expired = self.take_expired_incomplete(batch.incomplete, Instant::now());
        for partial in expired {
            log::warn!(
                "[{}] Multipart SMS from {} (ref {}, {} parts) still incomplete after {} min; storing received parts",
                sim_id,
                partial.sender,
                partial.reference,
                partial.total,
                INCOMPLETE_MULTIPART_TIMEOUT.as_secs() / 60
            );
            consumed.extend(partial.indices.iter().copied());
            sms_list.push(partial.into_partial_sms(&sim_id));
        }
        // Capture MMS WAP-push notifications *before* the caller deletes SMS from
        // the modem (this is our only chance -- there is no "leave undeleted for
        // retry" option like a ModemManager-based stack would have).
        for candidate in batch.mms_candidates {
            if let Err(e) = crate::mms_wap::handle_notification_candidate(&sim_id, candidate).await
            {
                log::warn!(
                    "[{}] Failed to process MMS WAP-push notification: {}",
                    sim_id,
                    e
                );
            }
        }
        Ok((sms_list, consumed))
    }

    /// Track still-incomplete multipart SMS and return the ones that have
    /// waited longer than `INCOMPLETE_MULTIPART_TIMEOUT`.
    fn take_expired_incomplete(
        &self,
        incomplete: Vec<crate::decode::IncompleteMultipart>,
        now: Instant,
    ) -> Vec<crate::decode::IncompleteMultipart> {
        let mut seen = self
            .incomplete_multipart_seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut still_pending = std::collections::HashMap::new();
        let mut expired = Vec::new();
        for item in incomplete {
            let key = (item.sender.clone(), item.reference, item.total);
            let first_seen = seen.get(&key).copied().unwrap_or(now);
            if now.duration_since(first_seen) >= INCOMPLETE_MULTIPART_TIMEOUT {
                expired.push(item);
            } else {
                if first_seen == now {
                    log::info!(
                        "Multipart SMS from {} (ref {}) has {}/{} parts; keeping fragments until the rest arrive",
                        item.sender,
                        item.reference,
                        item.parts.iter().filter(|p| p.is_some()).count(),
                        item.total
                    );
                }
                still_pending.insert(key, first_seen);
            }
        }
        *seen = still_pending;
        expired
    }

    /// Delete the given storage indices (AT+CMGD=<index>).
    pub async fn delete_sms_indices(&self, indices: &[u32]) -> io::Result<()> {
        let mut first_error = None;
        for index in indices {
            if let Err(e) = self
                .send_command_with_ok(&format!("AT+CMGD={}\r\n", index))
                .await
            {
                log::warn!("[{}] Failed to delete SMS index {}: {}", self.com_port, index, e);
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn get_modem_info<T>(
        &self,
        command: &str,
        parser: fn(&str) -> Option<T>,
    ) -> io::Result<Option<T>> {
        let raw_response = self.send_command_with_ok(command).await?;
        let cleaned_response = raw_response.trim().replace("OK", "");
        Ok(parser(&cleaned_response))
    }

    /// Ping the modem with a harmless command and a short timeout to verify it
    /// is actually responsive.  Some virtual COM ports open successfully but
    /// never return data; this catches them before we waste time on longer
    /// commands.
    pub async fn is_responsive(&self) -> bool {
        const PROBE_ATTEMPTS: usize = 2;

        for attempt in 0..PROBE_ATTEMPTS {
            let (tx, rx) = tokio::sync::oneshot::channel::<io::Result<String>>();
            let command = ATCommand {
                command: "AT\r\n".to_string(),
                response_tx: tx,
                _priority: 5,
                // Start at MAX_RETRIES so command_processor performs a single
                // quick probe attempt instead of 4 long timeout retries.
                retries: MAX_RETRIES,
                counted: false,
            };
            if self.command_tx.send(command).is_err() {
                return false;
            }

            let ok = matches!(
                tokio::time::timeout(Duration::from_secs(5), rx).await,
                Ok(Ok(Ok(resp))) if resp.trim().contains("OK")
            );

            if ok {
                return true;
            }

            if attempt + 1 < PROBE_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(700)).await;
            }
        }

        false
    }

    pub async fn get_signal_quality(&self) -> io::Result<Option<SignalQuality>> {
        self.get_modem_info("AT+CSQ\r\n", SignalQuality::from_response)
            .await
    }

    pub async fn check_network_registration(
        &self,
    ) -> io::Result<Option<NetworkRegistrationStatus>> {
        // Try AT+CEREG? (EPS/LTE registration) first; fall back to AT+CREG? (CS)
        if let Ok(Some(status)) = self
            .get_modem_info(
                "AT+CEREG?\r\n",
                NetworkRegistrationStatus::from_cereg_response,
            )
            .await
        {
            return Ok(Some(status));
        }
        self.get_modem_info("AT+CREG?\r\n", NetworkRegistrationStatus::from_response)
            .await
    }

    pub async fn check_operator(&self) -> io::Result<Option<OperatorInfo>> {
        self.get_modem_info("AT+COPS?\r\n", OperatorInfo::from_response)
            .await
    }

    pub async fn get_modem_model(&self) -> io::Result<Option<ModemInfo>> {
        self.get_modem_info("ATI\r\n", ModemInfo::from_response)
            .await
    }

    pub async fn get_imei(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+GSN\r\n", |response| {
            response
                .lines()
                .map(|l| l.trim())
                .find(|l| l.len() == 15 && l.chars().all(|c| c.is_ascii_digit()))
                .map(|s| s.to_string())
        })
        .await
    }

    pub async fn get_sim_iccid(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+CCID\r\n", Self::parse_iccid).await
    }

    fn parse_iccid(response: &str) -> Option<String> {
        response
                .lines()
                .find(|line| {
                    line.contains("CCID:")
                        || (line.trim().len() >= 19
                            && line.trim().chars().all(|c| c.is_ascii_hexdigit()))
                })
                .map(|line| {
                    if line.contains("CCID:") {
                        line.split("CCID:")
                            .nth(1)
                            .map(|s| s.trim().to_string())
                            .unwrap_or_else(|| line.trim().to_string())
                    } else {
                        line.trim().to_string()
                    }
                })
    }

    pub async fn get_sim_imsi(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+CIMI\r\n", |response| {
            response
                .lines()
                .find(|line| {
                    let trimmed = line.trim();
                    trimmed.len() >= 15 && trimmed.chars().all(|c| c.is_ascii_digit())
                })
                .map(|line| line.trim().to_string())
        })
        .await
    }

    pub async fn get_phone_number(&self) -> io::Result<Option<String>> {
        // Set phonebook memory preference to SIM card before querying MSISDN.
        // Ignore errors — non-critical and modem may already be set correctly.
        let _ = self.send_command_with_ok("AT$QCPBMPREF=1\r\n").await;
        self.get_modem_info("AT+CNUM\r\n", Self::parse_phone_number)
            .await
    }

    /// Write the user's own phone number into the SIM "Own Number" phonebook.
    /// Sequence: AT+CPBS="ON" then AT+CPBW=1,"<phone_number>",145
    pub async fn write_phone_number(&self, phone_number: &str) -> io::Result<()> {
        self.send_command_with_ok("AT+CPBS=\"ON\"\r\n").await?;
        self.send_command_with_ok(&format!("AT+CPBW=1,\"{}\",145\r\n", phone_number))
            .await?;
        Ok(())
    }

    fn parse_phone_number(response: &str) -> Option<String> {
        response
            .lines()
            .find(|line| line.starts_with("+CNUM:"))
            .and_then(|line| {
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 2 {
                    let number = parts[1].trim().trim_matches('"').to_string();
                    if number.is_empty() {
                        None
                    } else {
                        Some(number)
                    }
                } else {
                    None
                }
            })
    }

    fn format_log(input: &str) -> String {
        input
            .replace("\r\n", "\\r\\n")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
    }

    fn cme_code(err: &io::Error) -> Option<String> {
        let upper = err.to_string().to_ascii_uppercase();
        let (_, tail) = upper.split_once("+CME")?;
        let tail = tail.trim_start_matches(|character: char| character.is_ascii_whitespace() || character == ':').strip_prefix("ERROR")?;
        Some(tail
            .trim_start_matches(|character: char| character.is_ascii_whitespace() || character == ':')
            .chars()
            .take_while(|character| character.is_ascii_digit())
            .collect())
    }

    /// +CME ERROR 13 (SIM failure) / 50: recovered by a module restart.
    pub(crate) fn is_recoverable_sim_error(err: &io::Error) -> bool {
        matches!(Self::cme_code(err).as_deref(), Some("13" | "50"))
    }

    /// +CME ERROR 14 (SIM busy): transient while the SIM initialises; retry without restarting.
    pub(crate) fn is_sim_busy_error(err: &io::Error) -> bool {
        Self::cme_code(err).as_deref() == Some("14")
    }

    /// Whether an `AT+CFUN=1,1` SIM recovery may run now. eSIM machines recover with
    /// `ath0` instead, and a port is rebooted at most once per cooldown so a chip
    /// with no SIM (e.g. an eSIM without an enabled profile) is not rebooted in a loop.
    pub(crate) async fn sim_recovery_allowed(&self) -> bool {
        const COOLDOWN: Duration = Duration::from_secs(600);
        static LAST_RECOVERY: std::sync::OnceLock<
            std::sync::Mutex<std::collections::HashMap<String, Instant>>,
        > = std::sync::OnceLock::new();

        if *self.machine_type.read().await == Some(MachineType::Esim) || self.is_esim_mode().await {
            return false;
        }
        let mut last = LAST_RECOVERY
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        match last.get(&self.com_port) {
            Some(at) if now.duration_since(*at) < COOLDOWN => false,
            _ => {
                last.insert(self.com_port.clone(), now);
                true
            }
        }
    }

    pub async fn send_sms_text(
        &self,
        contact: &Contact,
        message: &str,
    ) -> anyhow::Result<(i64, String)> {
        let phone = if contact.id.starts_with('+') || contact.id.chars().all(|c| c.is_ascii_digit())
        {
            contact.id.clone()
        } else {
            contact.name.clone()
        };
        info!("Sending SMS text to {}: {}", phone, message);

        let sim_id = self.sim_id.read().await.clone().unwrap_or_default();

        let sms = Sms {
            id: 0,
            contact_id: contact.id.clone(),
            timestamp: now_gmt8().with_nanosecond(0).unwrap(),
            message: message.to_string(),
            sim_id,
            send: true,
            status: crate::db::SmsStatus::Loading,
            uploaded_to_platform: false,
            platform_item_id: None,
            platform_uploaded_at: None,
            platform_response: None,
        };

        let sms_id = sms.insert().await?;

        match self.send_text_message(&phone, message).await {
            Ok(_) => {
                Sms::update_status_by_id(sms_id, crate::db::SmsStatus::Read).await?;
                Ok((sms_id, contact.id.clone()))
            }
            Err(e) => {
                Sms::update_status_by_id(sms_id, crate::db::SmsStatus::Failed).await?;
                Err(e)
            }
        }
    }

    async fn send_text_message(&self, phone: &str, message: &str) -> anyhow::Result<()> {
        self.send_sms_content(&format!("AT+CMGS=\"{}\"\r", phone), message, |msg| {
            string_to_ucs2_pub(msg)
        })
        .await?;
        Ok(())
    }

    pub(super) async fn sync_welcome_sms_identity(&self, sms_list: &[ModemSMS]) -> anyhow::Result<()> {
        let candidates: Vec<_> = sms_list.iter().filter(|sms| !sms.send)
            .filter_map(|sms| crate::phone_number::parse_welcome_sms(&sms.message).map(|identity| (sms, identity)))
            .collect();
        let Some((sms, identity)) = candidates.first() else { return Ok(()); };
        if crate::db::AppSetting::get("welcome_sms_phone_sync_enabled").await?.as_deref() != Some("true") {
            return Ok(());
        }
        if candidates.iter().any(|(_, candidate)| candidate.phone_number != identity.phone_number) {
            anyhow::bail!("Conflicting welcome SMS numbers; no SIM write performed");
        }
        let Some(sim_id) = self.sim_id.read().await.clone() else { return Ok(()); };
        let _phone_guard = SimCard::lock_phone_number(&sim_id).await;
        let current_sim = self.sim_id.read().await;
        let esim_mode = self.esim_mode.read().await;
        if sms.sim_id != sim_id || current_sim.as_deref() != Some(sim_id.as_str()) || *esim_mode {
            return Ok(());
        }
        let Some(card) = SimCard::find_by_conditions(Some(&sim_id), None, None, None).await?.into_iter().next() else {
            return Ok(());
        };
        if !card.accepts_welcome_sms_number(&identity.phone_number) {
            return Ok(());
        }
        let command_guard = self.command_lock.lock().await;
        let type_reply = self.send_command_exclusive("ath?\r\n", &command_guard).await?;
        let machine_type = MachineType::from_type_reply(&type_reply);
        let iccid_reply = self.send_command_exclusive("AT+CCID\r\n", &command_guard).await?;
        let current_iccid = Self::parse_iccid(&iccid_reply)
            .ok_or_else(|| anyhow::anyhow!("Cannot verify receiving SIM ICCID"))?;
        if crate::esim::normalize_iccid(&current_iccid) != crate::esim::normalize_iccid(&sim_id) {
            anyhow::bail!("Receiving SIM identity changed; no SIM write performed");
        }
        match machine_type {
            MachineType::Normal | MachineType::Msim => {
                for command in [
                    "AT$QCPBMPREF=1\r\n".to_string(),
                    "AT+CPBS=\"ON\"\r\n".to_string(),
                    format!("AT+CPBW=1,\"+{}\",145\r\n", identity.phone_number),
                ] {
                    let response = self.send_command_exclusive(&command, &command_guard).await?;
                    if !response.contains("OK\r\n") {
                        anyhow::bail!("Welcome SMS phonebook command rejected; identity not saved");
                    }
                }
            }
            MachineType::Esim => {}
            MachineType::Unknown => anyhow::bail!("Cannot verify machine type; no SIM write performed"),
        }
        drop(command_guard);
        if card.save_welcome_sms_identity(&identity.phone_number, identity.operator).await? {
            log::info!("[{}] Welcome SMS identity saved: operator={}", self.com_port, identity.operator);
        }
        Ok(())
    }

    async fn send_command_exclusive(
        &self,
        command: &str,
        guard: &tokio::sync::MutexGuard<'_, ()>,
    ) -> io::Result<String> {
        Self::execute_command_locked(
            &self.write_half, &self.reader_state, command, &self.name, Duration::from_secs(8), guard,
        ).await
    }

    pub async fn read_sms_sync_insert(&self, _sms_type: SmsType) -> anyhow::Result<()> {
        let (sms_list, consumed) = self.read_sms(SmsType::All).await?;
        if !consumed.is_empty() {
            if let Err(e) = self.delete_sms_indices(&consumed).await {
                log::warn!("Failed to delete SMS from modem after reading: {}", e);
            }
        }
        if !sms_list.is_empty() {
            ModemSMS::bulk_insert(&sms_list).await?;
            if let Err(error) = self.sync_welcome_sms_identity(&sms_list).await {
                log::warn!("[{}] Welcome SMS SIM sync failed: {}", self.com_port, error);
            }
        }
        Ok(())
    }

    pub async fn get_sms_center(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+CSCA?\r\n", |response| {
            response
                .lines()
                .find(|line| line.starts_with("+CSCA:"))
                .and_then(|line| {
                    line.split('"')
                        .nth(1)
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                })
        })
        .await
    }

    pub async fn get_network_info(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+CPSI?\r\n", |response| {
            response
                .lines()
                .find(|line| line.starts_with("+CPSI:"))
                .map(|line| line.to_string())
        })
        .await
    }

    pub async fn get_sim_status(&self) -> io::Result<Option<String>> {
        let result = self.read_sim_status().await;
        if result.as_ref().is_ok_and(|status| status.as_deref().is_some_and(|value| value.trim().eq_ignore_ascii_case("READY"))) {
            self.sim_absence_suspected.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        result
    }

    async fn read_sim_status(&self) -> io::Result<Option<String>> {
        const CPIN_RECOVERY_RETRY_ATTEMPTS: u8 = 3;
        const CPIN_RECOVERY_RETRY_DELAY_SECS: u64 = 3;

        let first_try = self
            .get_modem_info("AT+CPIN?\r\n", |response| {
                response
                    .lines()
                    .find(|line| line.starts_with("+CPIN:"))
                    .and_then(|line| line.split(':').nth(1).map(|s| s.trim().to_string()))
            })
            .await;

        let mut first_err = match first_try {
            Ok(status) => return Ok(status),
            Err(e) => e,
        };

        const SIM_BUSY_RETRIES: u8 = 5;
        for attempt in 1..=SIM_BUSY_RETRIES {
            if !Self::is_sim_busy_error(&first_err) {
                break;
            }
            info!("[CPIN] {} SIM busy (+CME ERROR: 14), retry {}/{} in 1s", self.com_port, attempt, SIM_BUSY_RETRIES);
            tokio::time::sleep(Duration::from_secs(1)).await;
            match self
                .get_modem_info("AT+CPIN?\r\n", |response| {
                    response
                        .lines()
                        .find(|line| line.starts_with("+CPIN:"))
                        .and_then(|line| line.split(':').nth(1).map(|s| s.trim().to_string()))
                })
                .await
            {
                Ok(status) => return Ok(status),
                Err(e) => first_err = e,
            }
        }
        if Self::is_sim_busy_error(&first_err) {
            warn!("[CPIN] {} SIM still busy after {} retries", self.com_port, SIM_BUSY_RETRIES);
            return Err(first_err);
        }
        if matches!(Self::cme_code(&first_err).as_deref(), Some("10" | "13" | "50")) {
            // Set before the slow restart recovery; callers often time out and drop this future.
            self.sim_absence_suspected.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        if !Self::is_recoverable_sim_error(&first_err) || !self.sim_recovery_allowed().await {
            return Err(first_err);
        }

        warn!(
            "[CPIN恢复] 端口 {} (设备 {}) 检测到可恢复错误 {}，触发 AT+CFUN=1,1 重启恢复",
            self.com_port,
            self.name,
            first_err
        );
        info!(
            "[CPIN恢复] 端口 {} 正在发送恢复命令 AT+CFUN=1,1",
            self.com_port
        );
        self.reboot().await;

        let mut last_err = first_err;
        for attempt in 1..=CPIN_RECOVERY_RETRY_ATTEMPTS {
            info!(
                "[CPIN恢复] 端口 {} 等待 {} 秒后进行第 {}/{} 次 AT+CPIN? 重试",
                self.com_port,
                CPIN_RECOVERY_RETRY_DELAY_SECS,
                attempt,
                CPIN_RECOVERY_RETRY_ATTEMPTS
            );
            tokio::time::sleep(Duration::from_secs(CPIN_RECOVERY_RETRY_DELAY_SECS)).await;

            match self
                .get_modem_info("AT+CPIN?\r\n", |response| {
                    response
                        .lines()
                        .find(|line| line.starts_with("+CPIN:"))
                        .and_then(|line| line.split(':').nth(1).map(|s| s.trim().to_string()))
                })
                .await
            {
                Ok(status) => {
                    info!(
                        "[CPIN恢复] 端口 {} AT+CPIN? 已恢复成功（第 {}/{} 次重试）",
                        self.com_port, attempt, CPIN_RECOVERY_RETRY_ATTEMPTS
                    );
                    return Ok(status);
                }
                Err(e) => {
                    warn!(
                        "[CPIN恢复] 端口 {} 第 {}/{} 次 AT+CPIN? 重试失败: {}",
                        self.com_port,
                        attempt,
                        CPIN_RECOVERY_RETRY_ATTEMPTS,
                        e
                    );
                    last_err = e;
                }
            }
        }

        warn!(
            "[CPIN恢复] 端口 {} 已完成 {}/{} 次重试，仍未恢复 AT+CPIN?，返回最后错误: {}",
            self.com_port,
            CPIN_RECOVERY_RETRY_ATTEMPTS,
            CPIN_RECOVERY_RETRY_ATTEMPTS,
            last_err
        );

        Err(last_err)
    }

    pub async fn get_memory_status(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+CPMS?\r\n", |response| {
            response
                .lines()
                .find(|line| line.starts_with("+CPMS:"))
                .map(|line| line.to_string())
        })
        .await
    }

    pub async fn get_temperature_info(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+QTEMP?\r\n", |response| {
            response
                .lines()
                .find(|line| line.starts_with("+QTEMP:"))
                .map(|line| line.to_string())
        })
        .await
    }

    pub async fn set_sms_storage(&self, sms_storage: SmsStorage) -> io::Result<()> {
        let storage_str = match sms_storage {
            SmsStorage::SIM => "SM",
            SmsStorage::ME => "ME",
            SmsStorage::MT => "MT",
        };

        let cmd = format!("AT+CPMS=\"{0}\",\"{0}\",\"{0}\"\r\n", storage_str);

        match self.send_command_with_ok(&cmd).await {
            Ok(_) => {
                info!(
                    "SMS storage changed to {} for device {}",
                    storage_str, self.name
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to change SMS storage to {} for device {}: {}",
                    storage_str, self.name, e
                );
                Err(e)
            }
        }
    }

    pub async fn get_sms_storage_status(&self) -> io::Result<Option<String>> {
        self.get_modem_info("AT+CPMS?\r\n", |response| {
            response
                .lines()
                .find(|line| line.starts_with("+CPMS:"))
                .map(|line| line.to_string())
        })
        .await
    }

    // ─── Voice call AT commands ────────────────────────────────────────────────

    /// Initiate an outbound voice call. The trailing `;` keeps AT command mode active.
    pub async fn make_call(&self, phone: &str) -> io::Result<()> {
        self.send_command_with_ok(&format!("ATD{};\r\n", phone))
            .await?;
        Ok(())
    }

    /// Query the current call list. Returns raw response (contains "+CLCC:" lines when calls active).
    pub async fn query_clcc(&self) -> io::Result<String> {
        self.send_command_with_ok("AT+CLCC\r\n").await
    }

    /// Answer an incoming call.
    pub async fn answer_call(&self) -> io::Result<()> {
        self.send_command_with_ok("ATA\r\n").await?;
        Ok(())
    }

    /// Hang up the active or incoming call.
    pub async fn hangup_call(&self) -> io::Result<()> {
        self.send_command_with_ok("ATH\r\n").await?;
        Ok(())
    }

    /// Send a USSD code and wait for the network's `+CUSD:` reply.
    /// Returns the decoded response text.
    pub async fn send_ussd(&self, code: &str) -> io::Result<String> {
        if code.is_empty() || code.len() > 32 || !code.chars().all(|c| c.is_ascii_digit() || matches!(c, '*' | '#' | '+')) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "USSD code may only contain digits, *, # and +"));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.ussd_reply_tx.lock().await = Some(tx);
        let result = async {
            // The modem runs with AT+CSCS="UCS2", so the USSD string is UCS2 hex.
            let encoded = string_to_ucs2_pub(code).map_err(io::Error::other)?;
            let response = self
                .send_command_outer_timeout(&format!("AT+CUSD=1,\"{}\",15\r\n", encoded), 30)
                .await?;
            if !response.contains("OK") {
                return Err(io::Error::other(format!("AT+CUSD failed: {}", Self::format_log(&response))));
            }
            let line = match response.lines().find(|line| line.trim_start().starts_with("+CUSD:")) {
                Some(line) => line.to_string(),
                None => tokio::time::timeout(Duration::from_secs(30), rx)
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "No USSD reply within 30s"))?
                    .map_err(|_| io::Error::other("USSD reply channel closed"))?,
            };
            parse_cusd(&line)
        }
        .await;
        *self.ussd_reply_tx.lock().await = None;
        match &result {
            Ok(text) => info!("[USSD {}] {} reply: {:?}", self.com_port, code, text),
            Err(error) => warn!("[USSD {}] {} failed: {}", self.com_port, code, error),
        }
        result
    }

    /// Deliver a `+CUSD:` URC to a pending `send_ussd`; returns false when nobody waits.
    pub async fn deliver_ussd_reply(&self, line: &str) -> bool {
        match self.ussd_reply_tx.lock().await.take() {
            Some(tx) => tx.send(line.to_string()).is_ok(),
            None => false,
        }
    }

    async fn nwscanmode(&self) -> io::Result<u8> {
        let response = self.send_command_with_ok("AT+QCFG=\"nwscanmode\"\r\n").await?;
        response
            .lines()
            .find_map(|line| line.trim().strip_prefix("+QCFG: \"nwscanmode\","))
            .and_then(|value| value.trim().parse().ok())
            .ok_or_else(|| io::Error::other("Module did not report nwscanmode"))
    }

    async fn set_nwscanmode(&self, mode: u8) -> io::Result<()> {
        self.send_command_with_ok(&format!("AT+QCFG=\"nwscanmode\",{},1\r\n", mode)).await?;
        Ok(())
    }

    async fn wait_registered(&self, eps: bool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let command = if eps { "AT+CEREG?\r\n" } else { "AT+CREG?\r\n" };
            if let Ok(response) = self.send_command(command).await {
                let status = if eps {
                    NetworkRegistrationStatus::from_cereg_response(&response)
                } else {
                    NetworkRegistrationStatus::from_response(&response)
                };
                if status.is_some_and(|status| matches!(status.status_code().trim(), "1" | "5")) {
                    return true;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        false
    }

    /// USSD only works on 2G/3G: switch to GSM (then WCDMA), send the code, then restore LTE.
    pub async fn send_ussd_on_legacy_network(&self, code: &str) -> Result<String, String> {
        let original = self.nwscanmode().await.map_err(|e| format!("读取网络模式失败: {}", e))?;
        info!("[USSD {}] original nwscanmode={}, switching to 2G/3G for {}", self.com_port, original, code);
        let mut result = Err("无法注册到 2G/3G 网络".to_string());
        for (mode, label) in [(1u8, "2G"), (2u8, "3G")] {
            if let Err(e) = self.set_nwscanmode(mode).await {
                result = Err(format!("切换到 {} 失败: {}", label, e));
                continue;
            }
            if !self.wait_registered(false, Duration::from_secs(45)).await {
                info!("[USSD {}] not registered on {} within 45s", self.com_port, label);
                continue;
            }
            info!("[USSD {}] registered on {}, sending {}", self.com_port, label, code);
            tokio::time::sleep(Duration::from_secs(2)).await;
            result = self.send_ussd(code).await.map_err(|e| format!("{} USSD 失败: {}", label, e));
            break;
        }
        // A port that was already 2G/3G-only still goes back to automatic (4G) selection.
        let restore = if matches!(original, 1 | 2) { 0 } else { original };
        let restored = match self.set_nwscanmode(restore).await {
            Ok(()) => self.wait_registered(true, Duration::from_secs(60)).await,
            Err(e) => {
                warn!("[USSD {}] failed to restore nwscanmode {}: {}", self.com_port, restore, e);
                false
            }
        };
        if !restored {
            warn!("[USSD {}] not yet registered on 4G after restoring nwscanmode {}", self.com_port, restore);
        } else {
            info!("[USSD {}] restored nwscanmode {} and registered on 4G", self.com_port, restore);
        }
        result.map(|text| if restored { text } else { format!("{} (已恢复网络模式，4G 尚未注册)", text) })
    }

    /// Service provider name from the SIM (AT+CSPN?), e.g. "giffgaff".
    pub async fn read_spn(&self) -> Option<String> {
        let response = self.send_command_with_ok("AT+CSPN?\r\n").await.ok()?;
        let line = response.lines().find(|line| line.trim_start().starts_with("+CSPN:"))?;
        let start = line.find('"')? + 1;
        let end = start + line[start..].find('"')?;
        let raw = line[start..end].trim();
        let name = if raw.len() >= 4 && raw.len() % 4 == 0 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
            ucs2_hex_to_string(raw)
        } else {
            raw.to_string()
        };
        (!name.trim().is_empty()).then(|| name.trim().to_string())
    }

    /// Query CLCC and extract the caller's phone number from an incoming call.
    /// Returns None if no incoming call with a number is found.
    pub async fn read_clcc_caller_number(&self) -> io::Result<Option<String>> {
        let response = self.query_clcc().await?;
        Ok(parse_clcc_incoming_number(&response))
    }

    /// Delete all files from modem UFS to free space before a new recording.
    pub async fn delete_files(&self) -> io::Result<()> {
        self.send_command_with_ok("AT+QFDEL=\"*\"\r\n").await?;
        Ok(())
    }

    /// Start downlink audio recording to UFS (AMR format).
    /// `filename` is stored in modem UFS, e.g. `"a.amr"`.
    pub async fn start_recording(&self, filename: &str) -> io::Result<()> {
        self.send_command_with_ok(&format!("AT+QAUDRD=1,\"{}\",3,1\r\n", filename))
            .await?;
        Ok(())
    }

    /// Stop the active audio recording.
    pub async fn stop_recording(&self) -> io::Result<()> {
        self.send_command_with_ok("AT+QAUDRD=0\r\n").await?;
        Ok(())
    }

    pub(super) async fn voice_call_command(
        &self,
        call_id: &str,
        sim_id: &str,
        command: &str,
        verify_voice: bool,
    ) -> io::Result<()> {
        let current_call = self.inbound_call_id.lock().await;
        let current_sim = self.sim_id.read().await;
        if current_call.as_deref() != Some(call_id)
            || current_sim.as_deref() != Some(sim_id)
            || !self.inbound_call_answered.load(std::sync::atomic::Ordering::SeqCst)
            || *self.esim_mode.read().await
        {
            return Err(io::Error::other("The owned incoming call is no longer active"));
        }
        let guard = self.command_lock.lock().await;
        if verify_voice {
            let response = self.send_command_exclusive("AT+CLCC\r\n", &guard).await?;
            let active_voice = response.lines().filter_map(|line| line.trim().strip_prefix("+CLCC:"))
                .any(|line| {
                    let fields: Vec<_> = line.split(',').map(str::trim).collect();
                    fields.get(1) == Some(&"1") && fields.get(2) == Some(&"0") && fields.get(3) == Some(&"0")
                });
            if !active_voice {
                return Err(io::Error::other("No active incoming voice call for DTMF"));
            }
        }
        if command == "ATH\r\n" {
            self.restore_voice_microphone_locked(call_id, &guard).await;
        }
        if command.starts_with("AT+QAUDRD=1") {
            // A recording left running by a previous call makes the next start fail.
            let _ = self.send_command_exclusive("AT+QAUDRD=0\r\n", &guard).await;
        }
        let response = self.send_command_exclusive(command, &guard).await?;
        if !response.contains("OK\r\n") {
            return Err(io::Error::other(format!(
                "Modem rejected {}: {}",
                Self::format_log(command).trim(),
                Self::format_log(&response).trim()
            )));
        }
        Ok(())
    }

    pub(super) async fn quiet_voice_microphone(&self, call_id: &str, sim_id: &str) -> io::Result<()> {
        let current_call = self.inbound_call_id.lock().await;
        let current_sim = self.sim_id.read().await;
        if current_call.as_deref() != Some(call_id)
            || current_sim.as_deref() != Some(sim_id)
            || !self.inbound_call_answered.load(std::sync::atomic::Ordering::SeqCst)
            || *self.esim_mode.read().await
        {
            return Err(io::Error::other("Cannot mute a changed or inactive voice call"));
        }
        let guard = self.command_lock.lock().await;
        let reply = self.send_command_exclusive("AT+CMUT?\r\n", &guard).await?;
        let previous = Self::parse_voice_microphone_state(&reply)?;
        *self.voice_otp_microphone_state.lock().await = Some((call_id.to_string(), previous));
        if !previous {
            let reply = self.send_command_exclusive("AT+CMUT=1\r\n", &guard).await?;
            if !reply.contains("OK\r\n") {
                return Err(io::Error::other("The modem rejected uplink microphone muting"));
            }
        }
        Ok(())
    }

    pub(super) fn parse_voice_microphone_state(reply: &str) -> io::Result<bool> {
        if !reply.contains("OK\r\n") {
            return Err(io::Error::other("Cannot read the previous microphone mute state"));
        }
        match reply.lines().find_map(|line| line.trim().strip_prefix("+CMUT:")).map(str::trim) {
            Some("0") => Ok(false),
            Some("1") => Ok(true),
            _ => Err(io::Error::other("Unknown microphone mute state")),
        }
    }

    async fn restore_voice_microphone_locked(&self, call_id: &str, guard: &tokio::sync::MutexGuard<'_, ()>) {
        let mut saved = self.voice_otp_microphone_state.lock().await;
        if let Some((owner, previous)) = saved.as_ref() {
            if owner != call_id { return; }
            if !previous {
                match self.send_command_exclusive("AT+CMUT=0\r\n", guard).await {
                    Ok(reply) if reply.contains("OK\r\n") => {}
                    _ => log::warn!("[{}] microphone restore was not acknowledged for the ended voice call", self.com_port),
                }
            }
            *saved = None;
        }
    }

    pub(super) async fn send_voice_dtmf(&self, call_id: &str, sim_id: &str, digit: char) -> io::Result<()> {
        let command = Self::voice_dtmf_command(digit)?;
        if self.voice_otp_dtmf_sent.compare_exchange(
            false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst,
        ).is_err() {
            return Err(io::Error::other("DTMF has already been attempted for this call"));
        }
        self.voice_call_command(call_id, sim_id, &command, true).await
    }

    pub(super) async fn stop_ended_voice_recording(&self, call_id: &str, sim_id: &str) -> io::Result<()> {
        let current_call = self.inbound_call_id.lock().await;
        let current_sim = self.sim_id.read().await;
        if current_call.is_some()
            || current_sim.as_deref() != Some(sim_id)
            || self.voice_otp_call_id.lock().await.as_deref() != Some(call_id)
            || self.outbound_call_id.lock().await.is_some()
            || *self.esim_mode.read().await
        {
            return Err(io::Error::other("Cannot finalize audio for a changed call or SIM"));
        }
        let guard = self.command_lock.lock().await;
        let calls = self.send_command_exclusive("AT+CLCC\r\n", &guard).await?;
        if !calls.contains("OK\r\n") || calls.lines().any(|line| line.trim().starts_with("+CLCC:")) {
            return Err(io::Error::other("A new call prevents finalizing the ended call recording"));
        }
        self.restore_voice_microphone_locked(call_id, &guard).await;
        let response = self.send_command_exclusive("AT+QAUDRD=0\r\n", &guard).await?;
        if !response.contains("OK\r\n") && !response.contains("ERROR") {
            return Err(io::Error::other("Cannot confirm the ended call recording is stopped"));
        }
        Ok(())
    }

    fn voice_dtmf_command(digit: char) -> io::Result<String> {
        if !digit.is_ascii_digit() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Only one DTMF digit 0-9 is allowed"));
        }
        Ok(format!("AT+VTS=\"{}\",2\r\n", digit))
    }

    /// Inject a synthetic URC line into the URC channel.
    /// Used by the 30s recording timer to guarantee the URC handler processes
    /// call-end cleanup even when the modem doesn't emit NO CARRIER after ATH.
    pub fn inject_urc(&self, line: &str) {
        let _ = self._urc_tx.send(line.to_string());
    }

    /// Download a file from the modem UFS using AT+QFDWL and return its raw bytes.
    ///
    /// Protocol: modem responds `CONNECT\r\n`, then streams binary data, then
    /// `\r\n+QFDWL: <size>,<checksum>\r\nOK\r\n` when done.
    pub async fn download_file(&self, filename: &str) -> io::Result<Vec<u8>> {
        let _command_guard = self.command_lock.lock().await;
        let (raw_tx, raw_rx) = tokio::sync::oneshot::channel::<io::Result<Vec<u8>>>();
        let command = format!("AT+QFDWL=\"{}\"\r\n", filename);

        {
            let mut write_guard = self.write_half.lock().await;
            let write = write_guard.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "Serial port not connected")
            })?;
            self.reader_state.lock().await.raw_response_tx = Some(raw_tx);
            debug!("TX [{}]: {}", self.name, Self::format_log(&command));
            write.write_all(command.as_bytes()).await?;
            write.flush().await?;
            // write_guard drops here, releasing the write lock
        }

        let raw = match tokio::time::timeout(Duration::from_secs(60), raw_rx).await {
            Ok(Ok(result)) => result?,
            Ok(Err(_)) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Reader channel closed",
                ))
            }
            Err(_) => {
                self.reader_state.lock().await.raw_response_tx = None;
                return Err(io::Error::new(io::ErrorKind::TimedOut, "AT+QFDWL timeout"));
            }
        };

        Self::parse_qfdwl_response(&raw)
    }

    /// Parse the raw bytes returned by `AT+QFDWL`.
    /// Strips the `CONNECT\r\n` header and `\r\n+QFDWL: <size>,<checksum>\r\nOK\r\n` trailer.
    fn parse_qfdwl_response(raw: &[u8]) -> io::Result<Vec<u8>> {
        const CONNECT_MARKER: &[u8] = b"CONNECT\r\n";
        const TRAILER_MARKER: &[u8] = b"\r\n+QFDWL:";

        let connect_end = raw
            .windows(CONNECT_MARKER.len())
            .position(|w| w == CONNECT_MARKER)
            .map(|p| p + CONNECT_MARKER.len())
            .ok_or_else(|| io::Error::other("AT+QFDWL: missing CONNECT response"))?;

        let after_connect = &raw[connect_end..];
        let trailer_pos = after_connect
            .windows(TRAILER_MARKER.len())
            .enumerate()
            .filter(|(_, w)| *w == TRAILER_MARKER)
            .last()
            .map(|(pos, _)| pos)
            .ok_or_else(|| io::Error::other("AT+QFDWL: missing +QFDWL trailer"))?;

        Ok(after_connect[..trailer_pos].to_vec())
    }

    // ─── MMS (AT+QMMSCFG / AT+QMMSEDIT / AT+QMMSEND / AT+QFUPL) ────────────────

    /// Upload a file to modem RAM storage using `AT+QFUPL`, used to attach images
    /// to an MMS draft before `AT+QMMSEND`. Mirrors `send_pdu_atomic`'s two-step
    /// pattern: hold the write lock across the setup command (wait for the
    /// `CONNECT` prompt) and the raw data write (wait for `+QFUPL: <size>,<crc>`/`OK`).
    pub async fn upload_file(&self, filename: &str, data: &[u8]) -> anyhow::Result<()> {
        let setup_cmd = format!("AT+QFUPL=\"{}\",{}\r\n", filename, data.len());

        // Hold command_lock for the whole exchange so a queued command (e.g. the
        // periodic AT+CCID health check) can't steal the CONNECT prompt/final
        // response out of reader_state.response_tx.
        let _command_guard = self.command_lock.lock().await;
        let mut write_guard = self.write_half.lock().await;
        let write = write_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Serial port not connected"))?;

        // Step 1: send AT+QFUPL=... and wait for the CONNECT prompt.
        let (tx1, rx1) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx1);
        debug!("TX [{}]: {}", self.name, Self::format_log(&setup_cmd));
        write.write_all(setup_cmd.as_bytes()).await?;
        write.flush().await?;
        let prompt = match tokio::time::timeout(Duration::from_secs(10), rx1).await {
            Ok(Ok(Ok(r))) => r,
            Ok(Ok(Err(e))) => return Err(e.into()),
            Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
            Err(_) => {
                self.reader_state.lock().await.response_tx = None;
                return Err(anyhow::anyhow!("Timeout waiting for QFUPL CONNECT prompt"));
            }
        };
        if !prompt.contains("CONNECT") {
            return Err(anyhow::anyhow!(
                "QFUPL CONNECT prompt not received: {}",
                Self::format_log(&prompt)
            ));
        }

        // Step 2: write the raw file bytes and wait for the +QFUPL/OK confirmation.
        let (tx2, rx2) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx2);
        debug!("TX [{}]: <QFUPL {} bytes>", self.name, data.len());
        write.write_all(data).await?;
        write.flush().await?;
        let final_response = match tokio::time::timeout(Duration::from_secs(60), rx2).await {
            Ok(Ok(Ok(r))) => r,
            Ok(Ok(Err(e))) => return Err(e.into()),
            Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
            Err(_) => {
                self.reader_state.lock().await.response_tx = None;
                return Err(anyhow::anyhow!("Timeout waiting for QFUPL completion"));
            }
        };

        if final_response.contains("OK\r\n") && final_response.contains("+QFUPL:") {
            Ok(())
        } else {
            error!(
                "Incomplete QFUPL response: {}",
                Self::format_log(&final_response)
            );
            Err(anyhow::anyhow!("Incomplete QFUPL response"))
        }
    }

    /// Apply an MMS profile (APN/MMSC/proxy) to the modem before sending.
    /// Mirrors the `AT+QICSGP` / `AT+QMMSCFG` sequence used by python-gsmmodem's `APNS` command.
    ///
    /// `character` is deliberately set to `"ASCII"`, not `"UTF8"`. Every independent
    /// reference implementation found (daddyfix/cellular_modem_communicator,
    /// cmmakerclub/TEE_UC20_Shield-lib, Darthux/remote_sensor) uses ASCII and passes
    /// TO/Title strings as plain text; UTF8 mode expects those fields hex-encoded
    /// instead, which caused a real-hardware `+CME ERROR: 756` ("Invalid parameter")
    /// on the subject field (the plain-digit phone number happened to still parse as
    /// valid hex, masking the bug on the TO field).
    pub async fn configure_mms_profile(
        &self,
        apn: &str,
        mmsc: &str,
        proxy_host: &str,
        proxy_port: u16,
    ) -> anyhow::Result<()> {
        // QICSGP changes the profile, but does not replace an already-active
        // bearer. Deactivate/reactivate so QMMSEND actually uses the MMS APN.
        let _ = self.send_command("AT+QIDEACT=1\r\n").await;
        self.send_command_with_ok(&format!("AT+QICSGP=1,1,\"{}\",\"\",\"\",0\r\n", apn))
            .await?;
        self.send_command_with_ok("AT+QIACT=1\r\n").await?;
        self.send_command_with_ok("AT+QMMSCFG=\"contextid\",1\r\n")
            .await?;
        self.send_command_with_ok(&format!("AT+QMMSCFG=\"mmsc\",\"{}\"\r\n", mmsc))
            .await?;
        self.send_command_with_ok(&format!(
            "AT+QMMSCFG=\"proxy\",\"{}\",{}\r\n",
            proxy_host, proxy_port
        ))
        .await?;
        self.send_command_with_ok("AT+QMMSCFG=\"character\",\"ASCII\"\r\n")
            .await?;
        // Request the carrier's MMS delivery response using the verified modem
        // setting: report parameters, delivery/read notification flags, and
        // the carrier-specific response options.
        self.send_command_with_ok("AT+QMMSCFG=\"sendparam\",6,3,1,1,2,4\r\n")
            .await?;
        Ok(())
    }

    /// Register a completion waiter for the next `+QMMSEND:` URC.
    /// Must be called immediately before issuing `AT+QMMSEND`.
    async fn take_mms_waiter(&self) -> tokio::sync::oneshot::Receiver<(i32, i32)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.pending_mms.lock().await = Some(tx);
        rx
    }

    /// Called by the URC handler when a `+QMMSEND: <err>,<httprsp>` notification arrives.
    pub async fn resolve_mms_completion(&self, err_code: i32, http_code: i32) {
        if let Some(tx) = self.pending_mms.lock().await.take() {
            let _ = tx.send((err_code, http_code));
        }
    }

    fn sanitize_mms_filename(filename: &str) -> String {
        filename
            .replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|', ' '], "_")
    }

    fn build_mms_ram_name(own_number: &str, idx: usize, filename: &str) -> String {
        let cleaned_phone = crate::phone_number::normalize_msisdn(own_number);
        let safe_name = Self::sanitize_mms_filename(filename);
        if cleaned_phone.is_empty() {
            format!("RAM:mms_{}_{}", idx, safe_name)
        } else {
            format!("RAM:{}_{}_{}", cleaned_phone, idx, safe_name)
        }
    }

    fn host_staged_remote_name(phone_number: &str, filename: &str) -> String {
        let cleaned_phone = crate::phone_number::normalize_msisdn(phone_number);
        let safe_name = Self::sanitize_mms_filename(filename);
        let extension = std::path::Path::new(&safe_name)
            .extension()
            .and_then(|value| value.to_str());

        match (cleaned_phone.is_empty(), extension) {
            (false, Some(extension)) => format!("{}.{}", cleaned_phone, extension),
            (false, None) => cleaned_phone,
            (true, _) => safe_name,
        }
    }

    fn build_ftp_apn_command(apn: &str) -> String {
        format!("AT+QICSGP=2,1,\"{}\",\"\",\"\",1\r\n", apn)
    }

    fn select_ftp_apn(ftp_apn: Option<String>) -> String {
        ftp_apn
            .filter(|apn| !apn.trim().is_empty())
            .unwrap_or_else(|| "cmnet".to_string())
    }

    fn is_qiact_context_active(response: &str, context_id: u8) -> bool {
        response.lines().any(|line| {
            let line = line.trim_start();
            let Some(data) = line.strip_prefix("+QIACT:") else {
                return false;
            };
            data.trim_start()
                .split(',')
                .next()
                .and_then(|value| value.trim().parse::<u8>().ok())
                == Some(context_id)
        })
    }

    async fn ftp_upload_to_server(&self, remote_name: &str, data: &[u8]) -> anyhow::Result<()> {
        self.ftp_uploader.upload(remote_name, data).await
    }

    fn upload_attempt_order(mode: &str) -> Vec<&str> {
        match mode {
            "host_staged_attachment_upload" => vec!["host_staged_attachment_upload"],
            _ => vec!["modem_direct_attachment_upload"],
        }
    }

    /// Compose and send an MMS: clears any existing draft, sets recipient/subject,
    /// uploads and attaches each file, then issues `AT+QMMSEND` and awaits the
    /// asynchronous `+QMMSEND: <err>,<httprsp>` completion URC.
    ///
    /// Returns `(quectel_err_code, http_response_code)`. `err_code == 0` means success.
    pub async fn send_mms(
        &self,
        to: &str,
        subject: Option<&str>,
        attachments: &[(String, Vec<u8>)],
        attachment_upload_mode: &str,
        send_timeout_secs: u64,
        mms_profile: Option<(&str, &str, &str, u16)>,
    ) -> anyhow::Result<(i32, i32)> {
        // Clear any stale draft first (ignore errors — there may be nothing to clear).
        let _ = self.send_command_with_ok("AT+QMMSEDIT=0\r\n").await;

        self.send_command_with_ok(&format!("AT+QMMSEDIT=1,1,\"{}\"\r\n", to))
            .await?;
        if let Some(subject) = subject {
            if !subject.is_empty() {
                self.send_command_with_ok(&format!("AT+QMMSEDIT=4,1,\"{}\"\r\n", subject))
                    .await?;
            }
        }

        let own_number = self.get_phone_number().await.ok().flatten().unwrap_or_default();
        let mut ram_names = Vec::with_capacity(attachments.len());
        for (idx, (filename, data)) in attachments.iter().enumerate() {
            let ram_name = Self::build_mms_ram_name(&own_number, idx, filename);
            let _ = self.send_command(&format!("AT+QFDEL=\"{}\"\r\n", ram_name)).await;

            let mut last_err = None;
            for strategy in Self::upload_attempt_order(attachment_upload_mode) {
                match strategy {
                    "host_staged_attachment_upload" => {
                        let remote_name = Self::host_staged_remote_name(&own_number, filename);
                        warn!(
                            "[{}] uploading local attachment {} to FTP host as {} before modem fetch",
                            self.name, filename, remote_name
                        );
                        match self.ftp_upload_to_server(&remote_name, data).await {
                            Ok(()) => {}
                            Err(err) => {
                                warn!(
                                    "[{}] app-side FTP upload failed for {} -> {}: {}",
                                    self.name, filename, remote_name, err
                                );
                                last_err = Some(err);
                                continue;
                            }
                        }
                        match self.ftp_download_to_ram(&remote_name, &ram_name, send_timeout_secs).await {
                            Ok(()) => break,
                            Err(err) => {
                                warn!(
                                    "[{}] host-staged attachment upload failed for {}: {}",
                                    self.name, filename, err
                                );
                                last_err = Some(err);
                            }
                        }
                    }
                    _ => {
                        warn!(
                            "[{}] trying direct serial MMS upload for {}",
                            self.name, filename
                        );
                        match self.upload_file(&ram_name, data).await {
                            Ok(()) => break,
                            Err(err) => {
                                warn!(
                                    "[{}] direct serial MMS upload failed for {}: {}",
                                    self.name, filename, err
                                );
                                last_err = Some(err.into());
                            }
                        }
                    }
                }
            }

            if last_err.is_some() {
                return Err(anyhow::anyhow!(
                    "All MMS attachment upload strategies failed for {} ({}). Last error: {}",
                    filename,
                    attachment_upload_mode,
                    last_err.unwrap()
                ));
            }

            self.send_command_with_ok(&format!("AT+QMMSEDIT=5,1,\"{}\"\r\n", ram_name))
                .await?;
            ram_names.push(ram_name);
        }

        // Host-staged FTP uses a separate PDP/APN. Restore the MMS profile before
        // QMMSEND so the modem submits through the carrier's MMS bearer.
        if attachment_upload_mode == "host_staged_attachment_upload" {
            if let Some((apn, mmsc, proxy_host, proxy_port)) = mms_profile {
                self.configure_mms_profile(apn, mmsc, proxy_host, proxy_port).await?;
            }
        }

        let waiter = self.take_mms_waiter().await;
        let send_result = self
            .send_command_with_ok(&format!("AT+QMMSEND={}\r\n", send_timeout_secs))
            .await;

        let outcome = if let Err(e) = send_result {
            *self.pending_mms.lock().await = None;
            Err(anyhow::anyhow!("AT+QMMSEND rejected: {}", e))
        } else {
            match tokio::time::timeout(Duration::from_secs(send_timeout_secs + 15), waiter).await {
                Ok(Ok((err, http))) => Ok((err, http)),
                Ok(Err(_)) => Err(anyhow::anyhow!("MMS completion channel closed")),
                Err(_) => {
                    *self.pending_mms.lock().await = None;
                    Err(anyhow::anyhow!("Timed out waiting for +QMMSEND completion"))
                }
            }
        };

        // Best-effort cleanup regardless of outcome — free RAM storage and clear the draft.
        for ram_name in &ram_names {
            let _ = self
                .send_command(&format!("AT+QFDEL=\"{}\"\r\n", ram_name))
                .await;
        }
        let _ = self.send_command("AT+QMMSEDIT=0\r\n").await;

        outcome
    }

    /// Parse a `+QMMSEND: <err>,<httprsp>` URC line.
    pub fn parse_qmmsend(line: &str) -> Option<(i32, i32)> {
        let data = line.split(':').nth(1)?;
        let parts: Vec<&str> = data.split(',').collect();
        if parts.len() >= 2 {
            let err = parts[0].trim().parse().ok()?;
            let http = parts[1].trim().parse().ok()?;
            Some((err, http))
        } else {
            None
        }
    }

    /// Parse a `+QFTPOPEN: <err>,<protocol_error>` URC line from the Quectel FTP API.
    pub fn parse_qftpopen(line: &str) -> Option<(i32, i32)> {
        let data = line.split(':').nth(1)?;
        let parts: Vec<&str> = data.split(',').collect();
        if parts.len() >= 2 {
            let err = parts[0].trim().parse().ok()?;
            let proto = parts[1].trim().parse().ok()?;
            Some((err, proto))
        } else {
            None
        }
    }

    /// Parse a `+QFTPGET: <err>,<transferlen>` URC line from the Quectel FTP API.
    pub fn parse_qftpget(line: &str) -> Option<(i32, i64)> {
        let data = line.split(':').nth(1)?;
        let parts: Vec<&str> = data.split(',').collect();
        if parts.len() >= 2 {
            let err = parts[0].trim().parse().ok()?;
            let len = parts[1].trim().parse().ok()?;
            Some((err, len))
        } else {
            None
        }
    }

    /// Register a completion waiter for the next `+QFTPOPEN:` URC.
    async fn take_ftp_open_waiter(&self) -> tokio::sync::oneshot::Receiver<(i32, i32)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.pending_ftp_open.lock().await = Some(tx);
        rx
    }

    /// Called by the URC handler when a `+QFTPOPEN: <err>,<protocol_error>` notification arrives.
    pub async fn resolve_ftp_open_completion(&self, err_code: i32, protocol_error: i32) {
        if let Some(tx) = self.pending_ftp_open.lock().await.take() {
            let _ = tx.send((err_code, protocol_error));
        }
    }

    /// Register a completion waiter for the next `+QFTPGET:` URC.
    async fn take_ftp_get_waiter(&self) -> tokio::sync::oneshot::Receiver<(i32, i64)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.pending_ftp_get.lock().await = Some(tx);
        rx
    }

    /// Called by the URC handler when a `+QFTPGET: <err>,<transferlen>` notification arrives.
    pub async fn resolve_ftp_get_completion(&self, err_code: i32, transfer_len: i64) {
        if let Some(tx) = self.pending_ftp_get.lock().await.take() {
            let _ = tx.send((err_code, transfer_len));
        }
    }

    /// Download an attachment from the configured FTP server into modem RAM, avoiding
    /// the fragile serial-byte upload path when the hardware rejects `AT+QFUPL`.
    /// This is intentionally a fallback: healthy modems keep using the direct serial
    /// upload path, while a failing modem can fetch the file over cellular data instead.
    pub async fn ftp_download_to_ram(
        &self,
        remote_name: &str,
        local_ram_name: &str,
        timeout_secs: u64,
    ) -> anyhow::Result<()> {
        const FTP_HOST: &str = "47.237.101.133";
        const FTP_PORT: u16 = 21;
        const FTP_USER: &str = "ftpuser";
        const FTP_PASS: &str = "ftpuser";
        const CONTEXT_ID: u8 = 2;

        let ftp_apn = if let Some(sim_id) = self.sim_id.read().await.clone() {
            crate::db::MmsProfile::get(&sim_id)
                .await
                .ok()
                .flatten()
                .and_then(|profile| profile.ftp_apn)
                .map(|apn| Self::select_ftp_apn(Some(apn)))
                .unwrap_or_else(|| "cmnet".to_string())
        } else {
            "cmnet".to_string()
        };

        // Match the known-good modem flow used on real hardware:
        // 1) ensure a cellular PDP context exists and is active;
        // 2) configure FTP context/account/filetype/transmode/timeout;
        // 3) then open the FTP session and fetch the remote attachment.
        let qiact_before = self.send_command_with_ok("AT+QIACT?\r\n").await;
        let qiact_active = qiact_before
            .as_ref()
            .map(|v| Self::is_qiact_context_active(v, CONTEXT_ID))
            .unwrap_or(false);
        info!(
            "[{}] FTP APN before open: '{}' ; QIACT before: {} ; active={}",
            self.name,
            ftp_apn,
            qiact_before
                .as_ref()
                .map(|v| Self::format_log(v))
                .unwrap_or_else(|e| format!("<error: {}>", e)),
            qiact_active
        );

        // FIZZ uses PDP 1 for MMS and PDP 2 for data with the same APN. Only
        // recycle PDP 2 so the active MMS bearer remains available.
        if qiact_active {
            info!(
                "[{}] deactivating FTP PDP context {} before reconfiguration",
                self.name, CONTEXT_ID
            );
            self.send_command_with_ok(&format!("AT+QIDEACT={}\r\n", CONTEXT_ID))
                .await
                .map_err(|e| anyhow::anyhow!("FTP PDP {} deactivation failed: {}", CONTEXT_ID, e))?;
        }
        info!(
            "[{}] configuring FTP PDP context {} with APN '{}'",
            self.name, CONTEXT_ID, ftp_apn
        );
        self.send_command_with_ok(&Self::build_ftp_apn_command(&ftp_apn))
            .await
            .map_err(|e| anyhow::anyhow!("FTP PDP {} APN configuration failed: {}", CONTEXT_ID, e))?;
        self.send_command_with_ok(&format!("AT+QIACT={}\r\n", CONTEXT_ID))
            .await
            .map_err(|e| anyhow::anyhow!("FTP PDP {} activation failed: {}", CONTEXT_ID, e))?;
        let qiact_after = self.send_command_with_ok("AT+QIACT?\r\n").await;
        info!(
            "[{}] QIACT after FTP PDP {} activation: {}",
            self.name,
            CONTEXT_ID,
            qiact_after
                .as_ref()
                .map(|v| Self::format_log(v))
                .unwrap_or_else(|e| format!("<error: {}>", e))
        );
        if !qiact_after
            .as_ref()
            .map(|response| Self::is_qiact_context_active(response, CONTEXT_ID))
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!(
                "FTP PDP {} activation was accepted but context is not active",
                CONTEXT_ID
            ));
        }

        info!(
            "[{}] configuring FTP session for PDP context {}",
            self.name, CONTEXT_ID
        );
        self.send_command_with_ok(&format!("AT+QFTPCFG=\"contextid\",{}\r\n", CONTEXT_ID))
            .await
            .map_err(|e| anyhow::anyhow!("FTP context selection failed for PDP {}: {}", CONTEXT_ID, e))?;
        self.send_command_with_ok(&format!(
            "AT+QFTPCFG=\"account\",\"{}\",\"{}\"\r\n",
            FTP_USER, FTP_PASS
        ))
        .await?;
        self.send_command_with_ok("AT+QFTPCFG=\"filetype\",0\r\n").await?;
        self.send_command_with_ok("AT+QFTPCFG=\"transmode\",0\r\n").await?;
        self.send_command_with_ok(&format!("AT+QFTPCFG=\"rsptimeout\",{}\r\n", timeout_secs.max(30)))
            .await?;

        let waiter = self.take_ftp_open_waiter().await;
        info!(
            "[{}] opening FTP session to {}:{} for remote file {} -> RAM {}",
            self.name,
            FTP_HOST,
            FTP_PORT,
            remote_name,
            local_ram_name
        );
        info!(
            "[{}] FTP remote target name: '{}' (local RAM name: '{}')",
            self.name,
            remote_name,
            local_ram_name
        );
        self.send_command_with_ok(&format!(
            "AT+QFTPOPEN=\"{}\",{}\r\n",
            FTP_HOST, FTP_PORT
        ))
        .await?;

        match tokio::time::timeout(Duration::from_secs(timeout_secs.max(15) + 10), waiter).await {
            Ok(Ok((err_code, protocol_error))) => {
                if err_code != 0 || protocol_error != 0 {
                    return Err(anyhow::anyhow!(
                        "AT+QFTPOPEN failed: err={}, protocol_error={}",
                        err_code,
                        protocol_error
                    ));
                }
            }
            Ok(Err(_)) => return Err(anyhow::anyhow!("FTP open completion channel closed")),
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "Timed out waiting for +QFTPOPEN completion"
                ));
            }
        }

        let get_waiter = self.take_ftp_get_waiter().await;
        let cmd = format!(
            "AT+QFTPGET=\"{}\",\"{}\",0\r\n",
            remote_name, local_ram_name
        );
        info!(
            "[{}] executing FTP download: {}",
            self.name,
            Self::format_log(&cmd)
        );
        self.send_command_with_ok(&cmd).await?;

        match tokio::time::timeout(Duration::from_secs(timeout_secs + 20), get_waiter).await {
            Ok(Ok((err_code, transfer_len))) => {
                if err_code != 0 {
                    return Err(anyhow::anyhow!(
                        "AT+QFTPGET failed for {} -> {}: err={}, transfer_len={}",
                        remote_name,
                        local_ram_name,
                        err_code,
                        transfer_len
                    ));
                }
                if transfer_len <= 0 {
                    return Err(anyhow::anyhow!(
                        "AT+QFTPGET returned zero-length transfer for {} -> {}",
                        remote_name,
                        local_ram_name
                    ));
                }
            }
            Ok(Err(_)) => return Err(anyhow::anyhow!("FTP download completion channel closed")),
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "Timed out waiting for +QFTPGET completion"
                ));
            }
        }

        let _ = self.send_command("AT+QFTPCLOSE\r\n").await;
        Ok(())
    }

    // ─── MMS content fetch (AT+QHTTPxxx) ───────────────────────────────────────
    //
    // The EC20/EC25 has no dedicated MMS-retrieve AT command (see `mms_wap.rs`), so
    // fetching the M-Retrieve.conf body from a WAP-push `content_location` URL is done
    // with Quectel's generic HTTP AT command set, mirroring how `AT+QMMSEND` already
    // reaches the MMSC over the same APN/context. The modem performs the HTTP GET
    // itself; the response body is saved to a RAM file and pulled to the host with the
    // existing `download_file()` (`AT+QFDWL`, already used for call recordings).

    /// Register a completion waiter for the next `+QHTTPGET:` URC.
    /// Must be called immediately before issuing `AT+QHTTPGET`.
    async fn take_http_waiter(&self) -> tokio::sync::oneshot::Receiver<(i32, i32, i64)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.pending_http_get.lock().await = Some(tx);
        rx
    }

    /// Called by the URC handler when a `+QHTTPGET: <err>,<httprsp>,<content_length>`
    /// notification arrives.
    pub async fn resolve_http_get_completion(
        &self,
        err_code: i32,
        http_code: i32,
        content_length: i64,
    ) {
        if let Some(tx) = self.pending_http_get.lock().await.take() {
            let _ = tx.send((err_code, http_code, content_length));
        }
    }

    /// Register a completion waiter for the next `+QHTTPREADFILE:` URC.
    /// Must be called immediately before issuing `AT+QHTTPREADFILE`.
    async fn take_http_readfile_waiter(&self) -> tokio::sync::oneshot::Receiver<i32> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.pending_http_readfile.lock().await = Some(tx);
        rx
    }

    /// Called by the URC handler when a `+QHTTPREADFILE: <err>` notification arrives.
    pub async fn resolve_http_readfile_completion(&self, err_code: i32) {
        if let Some(tx) = self.pending_http_readfile.lock().await.take() {
            let _ = tx.send(err_code);
        }
    }

    /// Parse a `+QHTTPREADFILE: <err>` URC line.
    pub fn parse_qhttpreadfile(line: &str) -> Option<i32> {
        line.split(':').nth(1)?.trim().parse().ok()
    }

    /// Issue `AT+QHTTPREADFILE`, wait for its async `+QHTTPREADFILE: <err>` completion
    /// URC (not just the immediate `OK`, which only confirms the command was accepted),
    /// then pull the file to the host with `download_file()`.
    ///
    /// Skipping the completion wait races the modem's internal file write for larger
    /// downloads: `AT+QFDWL` issued right after `OK` can see a still-empty (0-byte) file
    /// if the actual write hasn't finished yet -- observed on real hardware for MMS
    /// content in the 100KB+ range (small content_location bodies complete fast enough
    /// that the race is rarely hit).
    async fn readfile_then_download(
        &self,
        ram_file: &str,
        timeout_secs: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let waiter = self.take_http_readfile_waiter().await;
        if let Err(e) = self
            .send_command_with_ok(&format!(
                "AT+QHTTPREADFILE=\"{}\",{}\r\n",
                ram_file, timeout_secs
            ))
            .await
        {
            *self.pending_http_readfile.lock().await = None;
            return Err(anyhow::anyhow!("AT+QHTTPREADFILE failed: {}", e));
        }

        match tokio::time::timeout(Duration::from_secs(timeout_secs + 15), waiter).await {
            Ok(Ok(0)) => {}
            Ok(Ok(err_code)) => {
                return Err(anyhow::anyhow!(
                    "AT+QHTTPREADFILE completed with err={}",
                    err_code
                ));
            }
            Ok(Err(_)) => return Err(anyhow::anyhow!("QHTTPREADFILE completion channel closed")),
            Err(_) => {
                *self.pending_http_readfile.lock().await = None;
                return Err(anyhow::anyhow!(
                    "Timed out waiting for +QHTTPREADFILE completion"
                ));
            }
        }

        self.download_file(ram_file)
            .await
            .map_err(anyhow::Error::from)
    }

    /// Parse a `+QHTTPGET: <err>[,<httprsp>,<content_length>]` URC line. On
    /// real EC20 firmware, error completions arrive as a bare `<err>` with no
    /// httprsp/content_length (e.g. `+QHTTPGET: 702` = HTTP(S) timeout, per
    /// Quectel's extended HTTP error table); only successful GETs (`<err>=0`)
    /// include the extra fields.
    pub fn parse_qhttpget(line: &str) -> Option<(i32, i32, i64)> {
        let data = line.split(':').nth(1)?;
        let parts: Vec<&str> = data.split(',').collect();
        let err = parts.first()?.trim().parse().ok()?;
        let http = parts
            .get(1)
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(-1);
        let len = parts
            .get(2)
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(-1);
        Some((err, http, len))
    }

    /// `AT+QHTTPURL=<len>,<timeout>` two-step: wait for the `CONNECT` prompt, then write
    /// the raw URL bytes. Mirrors `upload_file`'s two-step pattern (setup command that
    /// waits for a prompt, then a raw payload write), holding the write lock across both
    /// steps so no other AT command can slip in and corrupt the modem's input state.
    async fn send_http_url(&self, url: &str, timeout_secs: u64) -> anyhow::Result<()> {
        let setup_cmd = format!("AT+QHTTPURL={},{}\r\n", url.len(), timeout_secs);

        // Hold command_lock for the whole exchange so a queued command can't
        // steal the CONNECT prompt/final response out of reader_state.response_tx.
        let _command_guard = self.command_lock.lock().await;
        let mut write_guard = self.write_half.lock().await;
        let write = write_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Serial port not connected"))?;

        let (tx1, rx1) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx1);
        debug!("TX [{}]: {}", self.name, Self::format_log(&setup_cmd));
        write.write_all(setup_cmd.as_bytes()).await?;
        write.flush().await?;
        let prompt = match tokio::time::timeout(Duration::from_secs(10), rx1).await {
            Ok(Ok(Ok(r))) => r,
            Ok(Ok(Err(e))) => return Err(e.into()),
            Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
            Err(_) => {
                self.reader_state.lock().await.response_tx = None;
                return Err(anyhow::anyhow!(
                    "Timeout waiting for QHTTPURL CONNECT prompt"
                ));
            }
        };
        if !prompt.contains("CONNECT") {
            return Err(anyhow::anyhow!(
                "QHTTPURL CONNECT prompt not received: {}",
                Self::format_log(&prompt)
            ));
        }

        let (tx2, rx2) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx2);
        debug!("TX [{}]: <QHTTPURL {} bytes>", self.name, url.len());
        write.write_all(url.as_bytes()).await?;
        write.flush().await?;
        let final_response =
            match tokio::time::timeout(Duration::from_secs(timeout_secs.max(10)), rx2).await {
                Ok(Ok(Ok(r))) => r,
                Ok(Ok(Err(e))) => return Err(e.into()),
                Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
                Err(_) => {
                    self.reader_state.lock().await.response_tx = None;
                    return Err(anyhow::anyhow!("Timeout waiting for QHTTPURL completion"));
                }
            };

        if final_response.contains("OK\r\n") {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "QHTTPURL rejected: {}",
                Self::format_log(&final_response)
            ))
        }
    }

    /// Parse an absolute HTTP URL into host, port, and path components.
    fn parse_http_url(url: &str) -> anyhow::Result<(String, u16, String)> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| anyhow::anyhow!("MMS content URL is missing a scheme: {}", url))?;
        anyhow::ensure!(
            scheme.eq_ignore_ascii_case("http"),
            "unsupported MMS URL scheme: {}",
            scheme
        );

        let (authority, path) = match rest.find('/') {
            Some(pos) => (&rest[..pos], &rest[pos..]),
            None => (rest, "/"),
        };
        anyhow::ensure!(
            !authority.is_empty(),
            "MMS content URL has an empty host: {}",
            url
        );

        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
                (host.to_string(), port.parse::<u16>()?)
            }
            _ => (authority.to_string(), 80),
        };

        Ok((host, port, path.to_string()))
    }

    /// Build a minimal GET request block for Quectel `requestheader` mode.
    ///
    /// This module's `AT+QHTTPCFG` has no `"proxy"` key (confirmed against the
    /// local HTTP(S) application guide: the only settable keys are contextid,
    /// requestheader, responseheader, sslctxid, contenttype, rspout/auto,
    /// closed/ind, windowsize, closewaittime, custom_header), so proxy routing
    /// has to happen the way a real HTTP forward proxy works: the request line
    /// carries the absolute content URL while the TCP connection itself (set via
    /// `AT+QHTTPURL`, see `fetch_mms_content_via_requestheader`) targets the proxy.
    fn build_requestheader_get(url: &str) -> anyhow::Result<String> {
        let (host, port, _path) = Self::parse_http_url(url)?;
        let authority = if port == 80 {
            host.clone()
        } else {
            format!("{}:{}", host, port)
        };

        Ok(format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nX-Online-Host: {}\r\nProxy-Connection: Keep-Alive\r\nConnection: Keep-Alive\r\n\r\n",
            url,
            authority,
            host,
        ))
    }

    /// Try fetching MMS content through Quectel `requestheader` mode.
    ///
    /// Since this firmware has no dedicated HTTP proxy config key, the modem's
    /// TCP connection is pointed at the proxy itself via `AT+QHTTPURL`, while the
    /// custom request block sent after `AT+QHTTPGET` carries the real absolute
    /// content URL as the request line -- mirroring how a standard HTTP forward
    /// proxy request is built.
    async fn fetch_mms_content_via_requestheader(
        &self,
        url: &str,
        proxy_host: &str,
        proxy_port: u16,
        timeout_secs: u64,
    ) -> anyhow::Result<Vec<u8>> {
        const CONTEXT_ID: u8 = 1;

        self.send_command_with_ok("AT+QHTTPCFG=\"requestheader\",1\r\n")
            .await?;
        self.send_command_with_ok(&format!("AT+QHTTPCFG=\"contextid\",{}\r\n", CONTEXT_ID))
            .await?;

        if let Err(e) = self
            .send_command_with_ok(&format!("AT+QIACT={}\r\n", CONTEXT_ID))
            .await
        {
            debug!(
                "AT+QIACT={} did not return OK in requestheader mode (likely already active): {}",
                CONTEXT_ID, e
            );
        }

        // Connect the modem's HTTP socket to the carrier's MMS proxy, not the
        // content host -- the content host is typically unreachable directly
        // over the MMS APN (this is what produced the `+QHTTPGET: 702` timeouts).
        let proxy_url = format!("http://{}:{}/", proxy_host, proxy_port);
        self.send_http_url(&proxy_url, timeout_secs).await?;

        let request_headers = Self::build_requestheader_get(url)?;
        let waiter = self.take_http_waiter().await;

        // In requestheader mode AT+QHTTPGET takes <rsptime>,<data_length>[,<input_time>]
        // -- omitting <data_length> is rejected with +CME ERROR: 730 (Invalid parameter).
        let setup_cmd = format!("AT+QHTTPGET={},{}\r\n", timeout_secs, request_headers.len());
        // Held only across the response_tx round trip below (steps 1+2); released
        // before awaiting the async +QHTTPGET completion URC and issuing the
        // follow-up queued commands, which acquire this same lock themselves.
        let command_guard = self.command_lock.lock().await;
        let mut write_guard = self.write_half.lock().await;
        let write = write_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Serial port not connected"))?;

        let (tx1, rx1) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx1);
        debug!("TX [{}]: {}", self.name, Self::format_log(&setup_cmd));
        write.write_all(setup_cmd.as_bytes()).await?;
        write.flush().await?;

        let prompt = match tokio::time::timeout(Duration::from_secs(10), rx1).await {
            Ok(Ok(Ok(r))) => r,
            Ok(Ok(Err(e))) => return Err(e.into()),
            Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
            Err(_) => {
                self.reader_state.lock().await.response_tx = None;
                return Err(anyhow::anyhow!(
                    "Timeout waiting for QHTTPGET requestheader prompt"
                ));
            }
        };
        if !prompt.contains("CONNECT") && !prompt.contains("> ") {
            return Err(anyhow::anyhow!(
                "QHTTPGET requestheader prompt not received: {}",
                Self::format_log(&prompt)
            ));
        }

        let (tx2, rx2) = tokio::sync::oneshot::channel::<io::Result<String>>();
        self.reader_state.lock().await.response_tx = Some(tx2);
        debug!(
            "TX [{}]: <QHTTPGET request headers {} bytes>",
            self.name,
            request_headers.len()
        );
        write.write_all(request_headers.as_bytes()).await?;
        write.flush().await?;
        // Release the write lock now -- everything after this point (awaiting the
        // completion URC, then AT+QHTTPREADFILE/AT+QFDEL via send_command_with_ok)
        // needs to acquire this same lock through the command processor task. Holding
        // it here would deadlock that task against ourselves.
        drop(write_guard);

        let final_response =
            match tokio::time::timeout(Duration::from_secs(timeout_secs.max(10)), rx2).await {
                Ok(Ok(Ok(r))) => r,
                Ok(Ok(Err(e))) => return Err(e.into()),
                Ok(Err(_)) => return Err(anyhow::anyhow!("Reader channel closed")),
                Err(_) => {
                    self.reader_state.lock().await.response_tx = None;
                    return Err(anyhow::anyhow!(
                        "Timeout waiting for QHTTPGET requestheader completion"
                    ));
                }
            };

        if !final_response.contains("OK\r\n") {
            return Err(anyhow::anyhow!(
                "QHTTPGET requestheader rejected: {}",
                Self::format_log(&final_response)
            ));
        }
        // response_tx round trip is done; release before the follow-up queued
        // commands below try to acquire the same command_lock.
        drop(command_guard);

        let (err_code, http_code, _content_length) =
            match tokio::time::timeout(Duration::from_secs(timeout_secs + 15), waiter).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => return Err(anyhow::anyhow!("HTTP GET completion channel closed")),
                Err(_) => {
                    *self.pending_http_get.lock().await = None;
                    return Err(anyhow::anyhow!(
                        "Timed out waiting for +QHTTPGET completion"
                    ));
                }
            };

        if err_code != 0 {
            return Err(anyhow::anyhow!(
                "AT+QHTTPGET failed: err={} ({})",
                err_code,
                Self::describe_qhttp_error(err_code)
            ));
        }
        if !(200..300).contains(&http_code) {
            return Err(anyhow::anyhow!(
                "MMS content fetch HTTP error: {}",
                http_code
            ));
        }

        let port_tag: String = self
            .com_port
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();
        let ram_file = format!("RAM:mmsdl_hdr_{}.bin", port_tag);
        let result = self.readfile_then_download(&ram_file, timeout_secs).await;

        let _ = self
            .send_command(&format!("AT+QFDEL=\"{}\"\r\n", ram_file))
            .await;
        result
    }

    /// Fetch an MMS content URL (a WAP-push `content_location`, e.g. the M-Retrieve.conf
    /// body) over the modem's configured MMS APN context and return the raw response body.
    ///
    /// Sequence: bind the HTTP module to context 1 (already configured for MMS by
    /// `configure_mms_profile`) → point it at the carrier's MMS proxy (same one used for
    /// sending, if configured — MMS APNs are typically walled off from the open internet
    /// and only reachable via this proxy) → ensure the context is active → set the URL →
    /// issue GET and await the async completion URC → save the body to a RAM file → pull
    /// it to the host with `download_file()` → clean up the RAM file.
    pub async fn fetch_mms_content(
        &self,
        url: &str,
        proxy_host: Option<&str>,
        proxy_port: Option<u16>,
        timeout_secs: u64,
    ) -> anyhow::Result<Vec<u8>> {
        const CONTEXT_ID: u8 = 1;
        // Include the COM port in the RAM filename so concurrent fetches on different
        // modems in the same process don't collide (PID alone is shared across all
        // modem instances in this process).
        let port_tag: String = self
            .com_port
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();
        let ram_file = format!("RAM:mmsdl_{}.bin", port_tag);

        // EC20/EC25 needs an explicit HTTP session start before QHTTPURL/QHTTPGET.
        // Best-effort: Quectel firmware returns ERROR (not OK) when the HTTP service
        // is already initialized (e.g. left open by a previous fetch attempt), so a
        // failure here does not necessarily mean the HTTP stack is unusable.
        if let Err(e) = self.send_command_with_ok("AT+QHTTPINIT\r\n").await {
            debug!(
                "AT+QHTTPINIT did not return OK on {} (likely already initialized): {}",
                self.com_port, e
            );
        }

        if let (Some(host), Some(port)) = (proxy_host, proxy_port) {
            match self
                .fetch_mms_content_via_requestheader(url, host, port, timeout_secs)
                .await
            {
                Ok(bytes) => {
                    let _ = self.send_command("AT+QHTTPTERM\r\n").await;
                    return Ok(bytes);
                }
                Err(e) => {
                    warn!(
                        "requestheader-based MMS fetch failed for {} on {}: {} -- falling back to direct QHTTP",
                        url,
                        self.com_port,
                        e
                    );
                }
            }
        } else {
            debug!("No MMS proxy configured for this SIM; fetching content_location directly");
        }

        let fetch_result = async {
            self.send_command_with_ok(&format!("AT+QHTTPCFG=\"contextid\",{}\r\n", CONTEXT_ID))
                .await?;

            // Note: this firmware's AT+QHTTPCFG has no "proxy" key (confirmed against the
            // local HTTP(S) application guide -- only contextid/requestheader/responseheader/
            // sslctxid/contenttype/rspout,auto/closed,ind/windowsize/closewaittime/custom_header
            // are supported), so there is no way to make this direct-fetch fallback route
            // through the MMS proxy. It only succeeds when content_location's host happens
            // to be reachable directly over the MMS APN; the requestheader-based attempt
            // above is what actually goes through the proxy.

            // Activate the PDP context if not already active. Quectel firmware returns an
            // error (rather than OK) when re-activating an already-active context, so this
            // is best-effort: MMS send already exercises this same context/APN successfully,
            // so a failure here most likely just means it's already up.
            if let Err(e) = self
                .send_command_with_ok(&format!("AT+QIACT={}\r\n", CONTEXT_ID))
                .await
            {
                debug!(
                    "AT+QIACT={} did not return OK (likely already active): {}",
                    CONTEXT_ID, e
                );
            }

            self.send_http_url(url, timeout_secs).await?;

            // AT+QHTTPGET only accepts the request; completion arrives asynchronously as a
            // `+QHTTPGET: <err>,<httprsp>,<content_length>` URC (mirrors +QMMSEND).
            let waiter = self.take_http_waiter().await;
            if let Err(e) = self
                .send_command_with_ok(&format!("AT+QHTTPGET={}\r\n", timeout_secs))
                .await
            {
                *self.pending_http_get.lock().await = None;
                return Err(anyhow::anyhow!("AT+QHTTPGET rejected: {}", e));
            }

            let (err_code, http_code, _content_length) =
                match tokio::time::timeout(Duration::from_secs(timeout_secs + 15), waiter).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => {
                        return Err(anyhow::anyhow!("HTTP GET completion channel closed"))
                    }
                    Err(_) => {
                        *self.pending_http_get.lock().await = None;
                        return Err(anyhow::anyhow!(
                            "Timed out waiting for +QHTTPGET completion"
                        ));
                    }
                };

            if err_code != 0 {
                return Err(anyhow::anyhow!(
                    "AT+QHTTPGET failed: err={} ({})",
                    err_code,
                    Self::describe_qhttp_error(err_code)
                ));
            }
            if !(200..300).contains(&http_code) {
                return Err(anyhow::anyhow!(
                    "MMS content fetch HTTP error: {}",
                    http_code
                ));
            }

            let result = self.readfile_then_download(&ram_file, timeout_secs).await;

            // Best-effort cleanup regardless of outcome — free RAM storage.
            let _ = self
                .send_command(&format!("AT+QFDEL=\"{}\"\r\n", ram_file))
                .await;

            result
        }
        .await;

        let _ = self.send_command("AT+QHTTPTERM\r\n").await;

        fetch_result
    }

    /// Human-readable description for Quectel's extended HTTP(S) `<err>` codes
    /// (701-723 range) returned directly in the `+QHTTPGET:`/`+QHTTPPOST:` URC on
    /// this firmware, for clearer logs. Falls back to a generic label for codes
    /// outside the documented table (e.g. the low 0-19 basic-error range).
    fn describe_qhttp_error(err: i32) -> &'static str {
        match err {
            0 => "success",
            701 => "HTTP(S) unknown error",
            702 => "HTTP(S) timeout",
            703 => "HTTP(S) busy",
            704 => "HTTP(S) UART busy",
            705 => "HTTP(S) no GET/POST/PUT requests",
            706 => "HTTP(S) network busy",
            707 => "HTTP(S) network open failed",
            708 => "HTTP(S) network no configuration",
            709 => "HTTP(S) network deactivated",
            710 => "HTTP(S) network error",
            711 => "HTTP(S) URL error",
            712 => "HTTP(S) empty URL",
            713 => "HTTP(S) IP address error",
            714 => "HTTP(S) DNS error",
            715 => "HTTP(S) socket create error",
            716 => "HTTP(S) socket connect error",
            717 => "HTTP(S) socket read error",
            718 => "HTTP(S) socket write error",
            719 => "HTTP(S) socket closed",
            720 => "HTTP(S) data encode error",
            721 => "HTTP(S) data decode error",
            722 => "HTTP(S) read timeout",
            723 => "HTTP(S) response failed",
            _ => "unknown error",
        }
    }

    /// Extract the caller number from a `+CLIP:` URC line.
    /// e.g. `+CLIP: "18612345678",161,,,,0` → `Some("18612345678")`
    pub fn parse_clip(line: &str) -> Option<String> {
        if !line.starts_with("+CLIP:") {
            return None;
        }
        let rest = &line[6..];
        let start = rest.find('"')? + 1;
        let end = rest[start..].find('"')?;
        let number = &rest[start..start + end];
        if number.is_empty() {
            None
        } else {
            Some(number.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Modem;

    #[test]
    fn sim_busy_is_transient_not_recoverable() {
        for response in ["+CME ERROR: 14", "Command failed: \\r\\n+CME ERROR: 14\\r\\n", "+cme error 14"] {
            assert!(Modem::is_sim_busy_error(&std::io::Error::other(response)), "{}", response);
        }
        for response in ["+CME ERROR: 13", "+CME ERROR: 140", "+CMS ERROR: 14", "timeout"] {
            assert!(!Modem::is_sim_busy_error(&std::io::Error::other(response)), "{}", response);
        }
    }

    #[test]
    fn cusd_reply_decodes_ucs2_and_keeps_commas() {
        let hex = super::string_to_ucs2_pub("Your number is 07848998171, thanks").unwrap();
        assert_eq!(super::parse_cusd(&format!("+CUSD: 0,\"{}\",72", hex)).unwrap(), "Your number is 07848998171, thanks");
        assert_eq!(super::parse_cusd("+CUSD: 0,\"Balance: 5, number 07848998171\",15").unwrap(), "Balance: 5, number 07848998171");
        assert!(super::parse_cusd("+CUSD: 4").is_err());
        assert!(super::parse_cusd("+CUSD: 2").is_err());
    }

    #[test]
    fn recoverable_sim_error_matches_supported_codes_and_formats() {
        for response in ["+CME ERROR: 13", "+CME ERROR:50", "+CME: ERROR 50", "+cme error 13", "Command failed: \\r\\n+CME ERROR: 50\\r\\n"] {
            assert!(Modem::is_recoverable_sim_error(&std::io::Error::other(response)), "{}", response);
        }
    }

    #[test]
    fn recoverable_sim_error_rejects_other_codes() {
        for response in ["+CME ERROR: 10", "+CME ERROR: 130", "+CME ERROR: 500", "+CMS ERROR: 50", "ERROR", "timeout", "+CME ERROR: 14"] {
            assert!(!Modem::is_recoverable_sim_error(&std::io::Error::other(response)), "{}", response);
        }
    }

    #[test]
    fn build_ftp_apn_command_uses_configured_apn() {
        let command = Modem::build_ftp_apn_command("customftp");
        assert_eq!(command, "AT+QICSGP=2,1,\"customftp\",\"\",\"\",1\r\n");
    }

    #[test]
    fn ftp_apn_defaults_to_cmnet_without_profile_value() {
        assert_eq!(Modem::select_ftp_apn(None), "cmnet");
        assert_eq!(Modem::select_ftp_apn(Some("  ".to_string())), "cmnet");
    }

    #[test]
    fn ftp_apn_preserves_explicit_profile_value() {
        assert_eq!(Modem::select_ftp_apn(Some("customftp".to_string())), "customftp");
    }

    #[test]
    fn qiact_context_active_only_matches_requested_context() {
        let response = "+QIACT: 1,1,1,\"10.129.219.75\"\r\n\r\nOK\r\n";
        assert!(!Modem::is_qiact_context_active(response, 2));
        assert!(Modem::is_qiact_context_active(response, 1));
    }

    #[test]
    fn build_mms_ram_name_uses_own_phone_number() {
        assert_eq!(
            Modem::build_mms_ram_name("+86 138 1234 5678", 0, "a.png"),
            "RAM:8613812345678_0_a.png"
        );
    }

    #[test]
    fn host_staged_remote_name_uses_own_sim_number_and_extension() {
        assert_eq!(
            Modem::host_staged_remote_name("18126101015", "image.png"),
            "18126101015.png"
        );
        assert_eq!(
            Modem::host_staged_remote_name("18126101015", "message.txt"),
            "18126101015.txt"
        );
    }

}

/// Decode a UCS2 hex string (e.g. "00410042" → "AB").
/// Parse `+CUSD: <m>[,"<str>"[,<dcs>]]`; the text may contain commas.
fn parse_cusd(line: &str) -> io::Result<String> {
    let body = line.trim().strip_prefix("+CUSD:").unwrap_or(line).trim();
    let status = body.split(',').next().unwrap_or("").trim();
    let text = match (body.find('"'), body.rfind('"')) {
        (Some(start), Some(end)) if end > start => &body[start + 1..end],
        _ => "",
    };
    if text.is_empty() {
        return Err(io::Error::other(match status {
            "2" => "USSD session terminated by network".to_string(),
            "4" => "USSD operation not supported".to_string(),
            other => format!("Empty USSD reply (status {})", other),
        }));
    }
    let looks_ucs2 = text.len() >= 4 && text.len() % 4 == 0 && text.chars().all(|c| c.is_ascii_hexdigit());
    Ok(if looks_ucs2 { ucs2_hex_to_string(text) } else { text.to_string() })
}

fn ucs2_hex_to_string(hex: &str) -> String {
    (0..hex.len())
        .step_by(4)
        .filter_map(|i| u32::from_str_radix(&hex[i..i + 4], 16).ok())
        .filter_map(char::from_u32)
        .collect()
}

/// Parse the raw AT+CLCC response and return the caller's phone number from an incoming call.
/// Looks for mobile-terminated calls (dir=1) regardless of status.
fn parse_clcc_incoming_number(response: &str) -> Option<String> {
    for line in response.lines() {
        let line = line.trim();
        if !line.starts_with("+CLCC:") {
            continue;
        }
        let parts: Vec<&str> = line[6..].split(',').map(|s| s.trim()).collect();
        if parts.len() >= 7 {
            let dir = parts[1].parse::<i32>().unwrap_or(-1);
            if dir == 1 {
                let number = parts[5].trim_matches('"').to_string();
                if !number.is_empty() {
                    return Some(number);
                }
            }
        }
    }
    None
}
