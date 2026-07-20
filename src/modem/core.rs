use chrono::{Timelike, Utc};
use log::{debug, error, info, warn};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_serial::{SerialPortBuilderExt, SerialStream};

use crate::api::SseManager;
use crate::config::SmsStorage;
use crate::db::{Contact, ModemSMS, SimCard, Sms};
use crate::decode::parse_pdu_sms;
use crate::webhook;

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
    /// Reader task handle so we can abort and release COM resources on drop.
    reader_task: JoinHandle<()>,
    /// Command processor task handle so it does not outlive modem shutdown.
    command_task: JoinHandle<()>,
    /// If true, force COPS 46001 when IMSI MCC is 234/235.
    force_uk_mcc_to_46001: bool,
    /// Oneshot sender for the in-flight MMS send, resolved by the URC handler when
    /// the modem emits the async `+QMMSEND: <err>,<httprsp>` completion notification.
    pending_mms: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<(i32, i32)>>>>,
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
}

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
        let command_task = tokio::spawn({
            let write_half = write_half.clone();
            let reader_state = reader_state.clone();
            let name_c = name.to_string();
            async move {
                Self::command_processor(command_rx, write_half, reader_state, name_c).await;
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
            _urc_tx: urc_tx,
            urc_rx: Arc::new(Mutex::new(urc_rx)),
            outbound_call_id: Arc::new(tokio::sync::Mutex::new(None)),
            outbound_poll_cancel_tx: Arc::new(tokio::sync::Mutex::new(None)),
            outbound_call_answered: Arc::new(tokio::sync::Mutex::new(false)),
            reader_task,
            command_task,
            force_uk_mcc_to_46001,
            pending_mms: Arc::new(tokio::sync::Mutex::new(None)),
            pending_http_get: Arc::new(tokio::sync::Mutex::new(None)),
            pending_http_readfile: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
}

impl Drop for Modem {
    fn drop(&mut self) {
        self.reader_task.abort();
        self.command_task.abort();
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
                .map_err(|e| io::Error::other(format!("Failed to open {}: {}", port_name, e)))
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
        mut read_half: ReadHalf<SerialStream>,
        reader_state: Arc<Mutex<ReaderState>>,
        urc_tx: mpsc::UnboundedSender<String>,
        write_half: Arc<Mutex<Option<WriteHalf<SerialStream>>>>,
        name: String,
        com_port: String,
        baud_rate: u32,
        connection_state: Arc<RwLock<ConnectionState>>,
    ) {
        let mut raw_buf = [0u8; 256];
        let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
        loop {
            match read_half.read(&mut raw_buf).await {
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
        read_half: &mut ReadHalf<SerialStream>,
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

        info!("Attempting to reconnect {} on {}", name, com_port);
        for attempt in 1..=3u32 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match Self::create_serial_connection(com_port, baud_rate).await {
                Ok(new_stream) => {
                    let (new_read, new_write) = tokio::io::split(new_stream);
                    *write_half.lock().await = Some(new_write);
                    *read_half = new_read;
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
    ) {
        while let Some(mut at_command) = command_rx.recv().await {
            loop {
                match Self::execute_command_impl(
                    &write_half,
                    &reader_state,
                    &at_command.command,
                    &name,
                    Duration::from_secs(8),
                )
                .await
                {
                    Ok(response) => {
                        let _ = at_command.response_tx.send(Ok(response));
                        break;
                    }
                    Err(_e) if at_command.retries < MAX_RETRIES => {
                        at_command.retries += 1;
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    Err(e) => {
                        let _ = at_command.response_tx.send(Err(e));
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

    async fn init_sim_info(&self) -> anyhow::Result<()> {
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

    /// Force network registration to China Unicom (46001) using the Quectel
    /// band/scan-control + COPS sequence:
    ///   1. AT+QCFG="band",0,80,0,1
    ///   2. AT+QOPSCFG="scancontrol",2,0,80,0
    ///   3. AT+QCFG="cops_control",1
    ///   4. AT+COPS=1,2,"46001",7
    /// Steps are strictly ordered; the sequence aborts on the first failure.
    pub async fn force_register(&self) -> Result<(), String> {
        for (command, name) in [
            ("AT+QCFG=\"band\",0,80,0,1\r\n", "AT+QCFG(band)"),
            ("AT+QOPSCFG=\"scancontrol\",2,0,80,0\r\n", "AT+QOPSCFG"),
            ("AT+QCFG=\"cops_control\",1\r\n", "AT+QCFG"),
        ] {
            // Brief gap so the modem can apply the previous setting.
            tokio::time::sleep(Duration::from_millis(300)).await;
            self.send_command_with_ok(command)
                .await
                .map_err(|e| format!("{} failed: {}", name, e))?;
            info!("[force_register] {} succeeded on {}", name, self.com_port);
        }

        // Manual operator selection may take a long time while the modem scans
        // and registers; give it a longer outer timeout like send_cops_command.
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.register_cops_46001().await?;
        info!("[force_register] AT+COPS succeeded on {}", self.com_port);
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

    /// Quick liveness probe: send AT with a short timeout and expect OK.
    pub async fn probe_alive(&self) -> bool {
        matches!(
            self.send_command_outer_timeout("AT\r\n", 3).await,
            Ok(resp) if resp.contains("OK")
        )
    }

    async fn send_command_priority(&self, command: &str, priority: u8) -> io::Result<String> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let at_command = ATCommand {
            command: command.to_string(),
            response_tx,
            _priority: priority,
            retries: 0,
        };

        self.command_tx
            .send(at_command)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Command queue closed"))?;

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
            error!("Command failed: {}", response);
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
        let pdus = build_pdus(phone, message)?;

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
        let (sms_list, mms_found) = self.read_sms(SmsType::All).await?;

        if sms_list.is_empty() && !mms_found {
            return Ok(());
        }

        // Delete from modem FIRST so storage doesn't fill up. This must run
        // even when sms_list is empty but an MMS WAP-push notification was
        // found, otherwise notifications never get cleared from SIM storage.
        if let Err(e) = self.delete_all_sms().await {
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
                Err(e) => log::error!("Insert SMS error: {}", e),
            }
        };

        tokio::join!(webhook_future, db_future);

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
                    if let Some(card) = cards.iter().find(|c| c.id == sim_id) {
                        if let (Some(country_id), Some(phone_num)) =
                            (&card.country_code, &card.phone_number)
                        {
                            match crate::db::AppSetting::get("firefox_api_key").await {
                                Ok(Some(api_key)) => {
                                    match reqwest::Client::builder()
                                        .timeout(std::time::Duration::from_secs(10))
                                        .build()
                                    {
                                        Ok(client) => {
                                            for sms in &sms_list {
                                                if sms.send {
                                                    continue;
                                                }
                                                match crate::firefox_api::upload_sms(
                                                    &client,
                                                    &api_key,
                                                    country_id,
                                                    phone_num,
                                                    &sms.message,
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
                                                            phone_num,
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
                                                            phone_num,
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
                                                            phone_num,
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
                                        "[{}] Skipping 火狐狸 SMS upload: app_settings.firefox_api_key is not set",
                                        sim_id
                                    );
                                }
                                Err(e) => {
                                    log::warn!(
                                        "[{}] Skipping 火狐狸 SMS upload: failed to read firefox_api_key: {}",
                                        sim_id,
                                        e
                                    );
                                }
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

    /// Reads all SMS from modem storage. Returns the regular SMS list plus
    /// whether at least one MMS WAP-push notification was also found in this
    /// batch -- callers MUST take `mms_found` into account when deciding
    /// whether to delete modem storage (`delete_all_sms`), since MMS
    /// notifications are stripped out of `sms_list` here and would otherwise
    /// never be flagged for deletion when a poll returns MMS notifications
    /// but no regular SMS, leaving them to accumulate in SIM storage forever
    /// and potentially causing new incoming SMS/MMS to be rejected once full.
    pub async fn read_sms(&self, sms_type: SmsType) -> io::Result<(Vec<ModemSMS>, bool)> {
        let command = format!("AT+CMGL={}\r\n", sms_type.to_at_command_pdu());
        let response = self.send_command_with_ok(&command).await?;

        let sim_id = self.sim_id.read().await.clone().unwrap_or_default();
        let trimmed = response.trim();
        if trimmed != "OK" && !trimmed.is_empty() {
            log::info!("[{}] AT+CMGL raw response: {}", sim_id, trimmed);
        }
        let (sms_list, mms_candidates) = parse_pdu_sms(&response, &sim_id);
        let mms_found = !mms_candidates.is_empty();
        // Capture MMS WAP-push notifications *before* the caller deletes SMS from
        // the modem (this is our only chance -- there is no "leave undeleted for
        // retry" option like a ModemManager-based stack would have).
        for candidate in mms_candidates {
            if let Err(e) = crate::mms_wap::handle_notification_candidate(&sim_id, candidate).await
            {
                log::warn!(
                    "[{}] Failed to process MMS WAP-push notification: {}",
                    sim_id,
                    e
                );
            }
        }
        Ok((sms_list, mms_found))
    }

    /// Delete all SMS messages from modem storage (AT+CMGD=1,4)
    pub async fn delete_all_sms(&self) -> io::Result<()> {
        self.send_command_with_ok("AT+CMGD=1,4\r\n").await?;
        Ok(())
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
        self.get_modem_info("AT+CGMM\r\n", ModemInfo::from_response)
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
        self.get_modem_info("AT+CCID\r\n", |response| {
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
        })
        .await
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

    pub async fn read_sms_sync_insert(&self, _sms_type: SmsType) -> anyhow::Result<()> {
        let (sms_list, mms_found) = self.read_sms(SmsType::All).await?;
        if !sms_list.is_empty() || mms_found {
            if let Err(e) = self.delete_all_sms().await {
                log::warn!("Failed to delete SMS from modem after reading: {}", e);
            }
        }
        if !sms_list.is_empty() {
            ModemSMS::bulk_insert(&sms_list).await?;
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
        self.get_modem_info("AT+CPIN?\r\n", |response| {
            response
                .lines()
                .find(|line| line.starts_with("+CPIN:"))
                .and_then(|line| line.split(':').nth(1).map(|s| s.trim().to_string()))
        })
        .await
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

    /// Send a USSD code and wait for the response.
    /// Returns the decoded response text.
    pub async fn send_ussd(&self, code: &str) -> io::Result<String> {
        let command = format!("AT+CUSD=1,\"{}\"\r\n", code);
        let response = self.send_command_with_ok(&command).await?;

        let ussd_response = response
            .lines()
            .find(|line| line.starts_with("+CUSD:"))
            .map(|line| {
                let after_prefix = line.strip_prefix("+CUSD:").unwrap_or(line);
                let parts: Vec<&str> = after_prefix.split(',').collect();
                if parts.len() >= 2 {
                    let raw = parts[1].trim().trim_matches('"');
                    if raw.chars().all(|c| c.is_ascii_hexdigit())
                        && raw.len() % 4 == 0
                        && raw.len() >= 4
                    {
                        ucs2_hex_to_string(raw)
                    } else {
                        raw.to_string()
                    }
                } else {
                    String::new()
                }
            })
            .unwrap_or_default();

        Ok(ussd_response)
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
        self.send_command_with_ok(&format!("AT+QICSGP=1,1,\"{}\",\"\",\"\",0\r\n", apn))
            .await?;
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
        // Disable optional MMS support fields (delivery/read reports etc.) -- matches
        // the reference implementations above; keeps behavior simple/predictable.
        self.send_command_with_ok("AT+QMMSCFG=\"supportfield\",0\r\n")
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
        send_timeout_secs: u64,
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

        let mut ram_names = Vec::with_capacity(attachments.len());
        for (idx, (filename, data)) in attachments.iter().enumerate() {
            let ram_name = format!("RAM:mms_{}_{}", idx, filename);
            self.upload_file(&ram_name, data).await?;
            self.send_command_with_ok(&format!("AT+QMMSEDIT=5,1,\"{}\"\r\n", ram_name))
                .await?;
            ram_names.push(ram_name);
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

/// Decode a UCS2 hex string (e.g. "00410042" → "AB").
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
