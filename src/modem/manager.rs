use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::api::sse_manager::CallEvent;
use crate::api::SseManager;
use crate::config::{Settings, SmsStorage};
use crate::db::{Call, Contact, ModemSMS, SimCard, Sms, SmsStatus};
use crate::webhook;

use super::core::Modem;
use super::types::*;

/// Configuration for local whisper.cpp speech-to-text transcription.
/// All three fields must be `Some` for transcription to be enabled.
#[derive(Debug, Clone)]
pub struct TranscribeConfig {
    pub ffmpeg_exe: String,
    pub whisper_exe: String,
    pub whisper_model: String,
    /// Language code: "en", "zh", "ja", etc., or "auto" for auto-detect.
    pub whisper_language: String,
    /// Delay before auto-answering incoming calls (seconds).
    pub auto_answer_delay_secs: u64,
    /// Whether to enable auto-answer for incoming calls.
    pub auto_answer_enabled: bool,
}

impl TranscribeConfig {
    pub fn from_settings(s: &Settings) -> Option<Arc<Self>> {
        match (&s.ffmpeg_exe, &s.whisper_exe, &s.whisper_model) {
            (Some(ffmpeg), Some(whisper), Some(model)) => Some(Arc::new(Self {
                ffmpeg_exe: ffmpeg.clone(),
                whisper_exe: whisper.clone(),
                whisper_model: model.clone(),
                whisper_language: s.whisper_language.clone().unwrap_or_else(|| "auto".to_string()),
                auto_answer_delay_secs: s.auto_answer_delay_secs.unwrap_or(2),
                auto_answer_enabled: s.auto_answer_enabled.unwrap_or(true),
            })),
            _ => None,
        }
    }
}

pub struct ModemManager {
    modems: Arc<RwLock<HashMap<String, Arc<Modem>>>>,
    sim_cards_cache: Arc<RwLock<HashMap<String, SimCard>>>,
    port_runtime: Arc<RwLock<HashMap<String, PortRuntimeState>>>,
    /// Last SIM ID observed on each COM port so unavailable-port rows can
    /// still display the prior SIM identity instead of going blank.
    last_known_sim_ids_by_port: Arc<RwLock<HashMap<String, String>>>,
    /// Semaphore shared by unavailable-port quick probes to limit concurrent
    /// serial port opens (especially important on large USB hubs).
    probe_semaphore: Arc<Semaphore>,
    /// Semaphore shared by EVERY full modem initialization (startup phase AND
    /// the per-port reconnect workers) so a burst of newly-inserted SIMs on a
    /// large fleet can never all negotiate AT commands over the shared serial
    /// bus at once; unbounded concurrency here caused inits to stall/timeout
    /// indefinitely and left freshly-inserted SIMs stuck as "SIM REMOVED".
    init_semaphore: Arc<Semaphore>,
    /// Background URC tasks keyed by COM port so duplicate ICCIDs stay independent.
    urc_tasks: RwLock<HashMap<String, JoinHandle<()>>>,
    /// COM ports that are not currently usable (com_port, baud_rate)
    pub unavailable_ports: RwLock<Vec<(String, u32)>>,
    /// Background tasks that periodically quick-probe unavailable ports.
    unavailable_port_tasks: RwLock<HashMap<String, JoinHandle<()>>>,
    /// Per-port in-process lease: only one internal probe/init path may open
    /// a given COM port at a time, preventing self-contention lock storms.
    port_open_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    /// Original device list so unavailable ports can be retried later.
    devices: Vec<crate::config::Device>,
    /// If true, UK MCC SIMs (234/235) will force COPS 46001 during SIM init.
    force_uk_mcc_to_46001: bool,
    /// Number of modems to initialize in parallel (from config).
    max_concurrent_modem_init: usize,
    /// Default SMS storage location if not overridden per-device.
    default_sms_storage: Option<SmsStorage>,
    /// Transient AT+CCID probe failures keyed by COM port.
    sim_probe_fail_counts: RwLock<HashMap<String, u8>>,
    /// Consecutive per-port ICCID mismatch samples (new_iccid, streak).
    sim_swap_confirm_streaks: RwLock<HashMap<String, (String, u8)>>,
    /// Last moment each port was explicitly observed as SIM READY.
    recent_ready_at: RwLock<HashMap<String, Instant>>,
    /// Cooldown window keyed by COM port to avoid immediate re-demotion loops.
    sim_probe_cooldown_until: RwLock<HashMap<String, Instant>>,
    /// True once the initial background modem initialization has completed.
    initialization_complete: RwLock<bool>,
    /// One delayed post-register recovery task per SIM (task_id, handle).
    post_register_recovery_tasks: RwLock<HashMap<String, (u64, JoinHandle<()>)>>,
    /// Monotonic sequence used to avoid stale task cleanup races.
    post_register_recovery_seq: AtomicU64,
    /// Monotonic sequence used to version per-port probe/init cycles.
    port_runtime_seq: AtomicU64,
    /// Ports explicitly restarted by the operator. This blocks stale automatic
    /// probes from repromoting the old modem while its replacement initializes.
    manual_restarting_ports: RwLock<HashSet<String>>,
    /// Ports where CPIN is READY but ICCID changed. Hide the old identity while
    /// repeated samples confirm the new card before teardown/reinitialization.
    identity_revalidating_ports: RwLock<HashSet<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortLifecycle {
    Unknown,
    Probing,
    Active,
    Unavailable,
}

#[derive(Debug, Clone)]
pub struct PortRuntimeState {
    pub generation: u64,
    pub lifecycle: PortLifecycle,
    pub active_sim_id: Option<String>,
    pub last_sim_id: Option<String>,
    pub module_connected: bool,
    pub last_imei: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PortRuntimeSnapshot {
    pub com_port: String,
    pub baud_rate: u32,
    pub generation: u64,
    pub lifecycle: PortLifecycle,
    pub active_sim_id: Option<String>,
    pub last_sim_id: Option<String>,
    pub module_connected: bool,
    pub last_imei: Option<String>,
}

const SIM_PROBE_FAILURE_THRESHOLD: u8 = 3;
const SIM_DEMOTION_COOLDOWN_SECS: u64 = 20;

impl ModemManager {
    fn extract_imei_from_text(text: &str) -> Option<String> {
        let tail = text.split("imei=").nth(1)?.trim();

        let candidate = if let Some(rest) = tail.strip_prefix("Some(") {
            // Expected form: Some("862607...")
            let first_quote = rest.find('"')?;
            let after_first = &rest[first_quote + 1..];
            let second_quote = after_first.find('"')?;
            &after_first[..second_quote]
        } else if let Some(rest) = tail.strip_prefix('"') {
            // Expected form: "862607..."
            let end = rest.find('"')?;
            &rest[..end]
        } else {
            // Fallback: token up to whitespace/comma/paren
            tail.split(|c: char| c.is_whitespace() || c == ',' || c == ')' || c == ']')
                .next()
                .unwrap_or("")
        };

        let digits: String = candidate.chars().filter(|c| c.is_ascii_digit()).collect();
        if (14..=20).contains(&digits.len()) {
            Some(digits)
        } else {
            None
        }
    }

    fn should_defer_sms_reads_from_snapshots(snapshots: &[PortRuntimeSnapshot]) -> bool {
        snapshots
            .iter()
            .any(|snapshot| {
                snapshot.last_sim_id.is_some() && snapshot.lifecycle != PortLifecycle::Active
            })
    }

    async fn port_open_lock(&self, com_port: &str) -> Arc<Mutex<()>> {
        if let Some(lock) = self.port_open_locks.read().await.get(com_port).cloned() {
            return lock;
        }

        let mut locks = self.port_open_locks.write().await;
        locks
            .entry(com_port.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn begin_port_probe_cycle(&self, com_port: &str) -> u64 {
        let generation = self.port_runtime_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let mut runtime = self.port_runtime.write().await;
        let entry = runtime.entry(com_port.to_string()).or_insert(PortRuntimeState {
            generation,
            lifecycle: PortLifecycle::Unknown,
            active_sim_id: None,
            last_sim_id: None,
            module_connected: false,
            last_imei: None,
        });
        entry.generation = generation;
        entry.lifecycle = PortLifecycle::Probing;
        entry.active_sim_id = None;
        generation
    }

    async fn mark_port_active(&self, com_port: &str, sim_id: &str, generation: u64) -> bool {
        let mut runtime = self.port_runtime.write().await;
        let entry = runtime.entry(com_port.to_string()).or_insert(PortRuntimeState {
            generation,
            lifecycle: PortLifecycle::Unknown,
            active_sim_id: None,
            last_sim_id: None,
            module_connected: false,
            last_imei: None,
        });
        if entry.generation > generation {
            return false;
        }
        entry.generation = generation;
        entry.lifecycle = PortLifecycle::Active;
        entry.active_sim_id = Some(sim_id.to_string());
        entry.last_sim_id = Some(sim_id.to_string());
        entry.module_connected = true;
        true
    }

    async fn mark_port_unavailable(&self, com_port: &str, sim_id: Option<&str>) {
        let mut runtime = self.port_runtime.write().await;
        let entry = runtime.entry(com_port.to_string()).or_insert(PortRuntimeState {
            generation: 0,
            lifecycle: PortLifecycle::Unknown,
            active_sim_id: None,
            last_sim_id: None,
            module_connected: false,
            last_imei: None,
        });
        entry.lifecycle = PortLifecycle::Unavailable;
        entry.active_sim_id = None;
        if let Some(sim_id) = sim_id {
            entry.last_sim_id = Some(sim_id.to_string());
        }
    }

    /// Force `port_runtime` back in sync with a modem that is live in `self.modems`.
    /// The API renders the dashboard from `port_runtime` alone, so a port whose
    /// runtime state drifted out of `Active` shows as "SIM REMOVED" forever even
    /// though the modem keeps answering AT+CPIN? normally.
    async fn reaffirm_port_active(&self, com_port: &str, sim_id: &str) -> bool {
        if self.manual_restarting_ports.read().await.contains(com_port) {
            return false;
        }
        let generation = self.port_runtime_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let mut runtime = self.port_runtime.write().await;
        let entry = runtime.entry(com_port.to_string()).or_insert(PortRuntimeState {
            generation,
            lifecycle: PortLifecycle::Unknown,
            active_sim_id: None,
            last_sim_id: None,
            module_connected: false,
            last_imei: None,
        });
        let was_stale = entry.lifecycle != PortLifecycle::Active
            || entry.active_sim_id.as_deref() != Some(sim_id);
        entry.generation = generation;
        entry.lifecycle = PortLifecycle::Active;
        entry.active_sim_id = Some(sim_id.to_string());
        entry.last_sim_id = Some(sim_id.to_string());
        entry.module_connected = true;
        was_stale
    }

    async fn set_port_module_connected(&self, com_port: &str, connected: bool) {
        let mut runtime = self.port_runtime.write().await;
        let entry = runtime.entry(com_port.to_string()).or_insert(PortRuntimeState {
            generation: 0,
            lifecycle: PortLifecycle::Unknown,
            active_sim_id: None,
            last_sim_id: None,
            module_connected: connected,
            last_imei: None,
        });
        entry.module_connected = connected;
        if !connected {
            entry.last_imei = None;
        }
    }

    async fn set_port_last_imei(&self, com_port: &str, imei: Option<String>) {
        let mut runtime = self.port_runtime.write().await;
        let entry = runtime.entry(com_port.to_string()).or_insert(PortRuntimeState {
            generation: 0,
            lifecycle: PortLifecycle::Unknown,
            active_sim_id: None,
            last_sim_id: None,
            module_connected: false,
            last_imei: None,
        });
        entry.last_imei = imei;
    }

    /// Whether a live modem is currently registered on this COM port.
    async fn has_active_modem_on_port(&self, com_port: &str) -> bool {
        self.modems.read().await.contains_key(com_port)
    }

    /// Whether this ICCID is currently attached to another active COM port.
    async fn is_iccid_active_on_other_port(&self, iccid: &str, com_port: &str) -> bool {
        let modems = self.modems.read().await;
        for (port, modem) in modems.iter() {
            if port == com_port {
                continue;
            }
            let sid = modem.sim_id.read().await.clone();
            if sid.as_deref() == Some(iccid) {
                return true;
            }
        }
        false
    }

    pub async fn get_port_runtime_snapshots(&self) -> Vec<PortRuntimeSnapshot> {
        let runtime = self.port_runtime.read().await;
        self.devices
            .iter()
            .map(|device| {
                let state = runtime.get(&device.com_port).cloned().unwrap_or(PortRuntimeState {
                    generation: 0,
                    lifecycle: PortLifecycle::Unknown,
                    active_sim_id: None,
                    last_sim_id: None,
                    module_connected: false,
                    last_imei: None,
                });
                PortRuntimeSnapshot {
                    com_port: device.com_port.clone(),
                    baud_rate: device.baud_rate,
                    generation: state.generation,
                    lifecycle: state.lifecycle,
                    active_sim_id: state.active_sim_id,
                    last_sim_id: state.last_sim_id,
                    module_connected: state.module_connected,
                    last_imei: state.last_imei,
                }
            })
            .collect()
    }

    pub async fn get_manual_restarting_ports(&self) -> HashSet<String> {
        self.manual_restarting_ports.read().await.clone()
    }

    pub async fn get_identity_revalidating_ports(&self) -> HashSet<String> {
        self.identity_revalidating_ports.read().await.clone()
    }

    async fn set_last_known_sim_port(&self, com_port: &str, sim_id: &str) {
        let mut last_known = self.last_known_sim_ids_by_port.write().await;
        last_known.insert(com_port.to_string(), sim_id.to_string());
    }

    pub async fn is_initialization_complete(&self) -> bool {
        *self.initialization_complete.read().await
    }

    /// Reconcile `port_runtime` with the current live modem map.
    ///
    /// This is a defensive self-heal for cases where runtime state drifted to
    /// `Unavailable` while a modem object on the same COM port is still alive.
    /// The dashboard derives availability from `port_runtime`, so restoring it
    /// here prevents stale "SIM REMOVED" rows for healthy ports.
    pub async fn sync_port_runtime_with_live_modems(&self) {
        let modems: Vec<Arc<Modem>> = self.modems.read().await.values().cloned().collect();

        for modem in modems {
            if self
                .manual_restarting_ports
                .read()
                .await
                .contains(&modem.com_port)
            {
                continue;
            }
            let sim_id = if let Some(sim_id) = modem.sim_id.read().await.clone() {
                sim_id
            } else {
                // A live modem can temporarily lose its in-memory SIM id during
                // churn. Probe ICCID directly so runtime can still be restored.
                let recovered = tokio::time::timeout(Duration::from_secs(2), modem.get_sim_iccid())
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .flatten()
                    .filter(|v| !v.trim().is_empty());
                let Some(recovered_id) = recovered else {
                    continue;
                };
                *modem.sim_id.write().await = Some(recovered_id.clone());
                log::warn!(
                    "[runtime-sync] Recovered missing SIM ID on {} via direct ICCID probe: {}",
                    modem.com_port,
                    recovered_id
                );
                recovered_id
            };

            if self.reaffirm_port_active(&modem.com_port, &sim_id).await {
                log::warn!(
                    "[runtime-sync] Restored stale runtime state for {} on {} from live modem map",
                    sim_id,
                    modem.com_port
                );
            }

            self
                .recent_ready_at
                .write()
                .await
                .insert(modem.com_port.clone(), Instant::now());
            self.set_last_known_sim_port(&modem.com_port, &sim_id).await;
            self.remove_unavailable_port(&modem.com_port).await;
        }
    }

    /// Mark a port as unavailable and start a background worker that quick-probes
    /// it periodically. This replaces the old sequential unavailable_ports list so
    /// that each port is checked independently instead of waiting for a full 1..N scan.
    async fn add_unavailable_port(
        self: Arc<Self>,
        com_port: String,
        baud_rate: u32,
        device_id: String,
        sms_storage: Option<SmsStorage>,
        index: usize,
        sse_manager: Arc<SseManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) {
        // Never mark a port that still hosts a live modem as unavailable: the
        // dashboard reads `port_runtime`, so it would report "SIM REMOVED" while
        // the modem keeps working, and the spawned worker would fight the live
        // modem for the serial port and never succeed.
        if self.has_active_modem_on_port(&com_port).await {
            log::debug!(
                "Skipping unavailable-port registration for {}: a live modem is still active on it.",
                com_port
            );
            return;
        }

        self.mark_port_unavailable(&com_port, None).await;

        let mut ports = self.unavailable_ports.write().await;
        if !ports.iter().any(|(p, _)| p == &com_port) {
            ports.push((com_port.clone(), baud_rate));
        }
        drop(ports);

        let mut tasks = self.unavailable_port_tasks.write().await;
        if !tasks.contains_key(&com_port) {
            let handle = tokio::spawn(Self::unavailable_port_worker(
                self.clone(),
                com_port.clone(),
                baud_rate,
                device_id,
                sms_storage,
                index,
                sse_manager,
                transcribe_cfg,
            ));
            tasks.insert(com_port.clone(), handle);
            log::debug!("Started unavailable-port worker for {}", com_port);
        }
    }

    /// Remove a port from the unavailable list and abort its background worker.
    async fn remove_unavailable_port(&self,
        com_port: &str,
    ) {
        let mut ports = self.unavailable_ports.write().await;
        ports.retain(|(p, _)| p != com_port);
        drop(ports);

        let mut tasks = self.unavailable_port_tasks.write().await;
        if let Some(handle) = tasks.remove(com_port) {
            handle.abort();
            log::debug!("Stopped unavailable-port worker for {}", com_port);
        }
    }

    async fn stop_unavailable_port_worker(&self, com_port: &str) {
        self.unavailable_ports
            .write()
            .await
            .retain(|(port, _)| port != com_port);

        let handle = self.unavailable_port_tasks.write().await.remove(com_port);
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
            log::info!("[manual-restart] Stopped recovery worker for {}", com_port);
        }
    }

    pub async fn restart_port(
        self: Arc<Self>,
        com_port: &str,
        sse_manager: Arc<SseManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) -> Result<(), String> {
        let normalized = com_port.trim().to_ascii_uppercase();
        let Some((index, device)) = self
            .devices
            .iter()
            .enumerate()
            .find(|(_, device)| device.com_port.eq_ignore_ascii_case(&normalized))
            .map(|(index, device)| (index, device.clone()))
        else {
            return Err(format!("Unknown configured COM port: {}", com_port));
        };

        log::warn!("[manual-restart] Restart requested for {}", device.com_port);

        self.manual_restarting_ports
            .write()
            .await
            .insert(device.com_port.clone());

        self.stop_unavailable_port_worker(&device.com_port).await;

        if let Some(task) = self.urc_tasks.write().await.remove(&device.com_port) {
            task.abort();
        }

        let modem = self.modems.write().await.remove(&device.com_port);
        let old_sim_id = if let Some(modem) = modem.as_ref() {
            modem.sim_id.read().await.clone()
        } else {
            None
        };

        if let Some(sim_id) = old_sim_id.as_deref() {
            if let Some((_task_id, task)) = self
                .post_register_recovery_tasks
                .write()
                .await
                .remove(sim_id)
            {
                task.abort();
            }
        }

        if let Some(modem) = modem {
            modem.shutdown().await;
        }

        self.last_known_sim_ids_by_port
            .write()
            .await
            .remove(&device.com_port);
        self.sim_probe_fail_counts.write().await.remove(&device.com_port);
        self.sim_swap_confirm_streaks.write().await.remove(&device.com_port);
        self.recent_ready_at.write().await.remove(&device.com_port);
        self.sim_probe_cooldown_until
            .write()
            .await
            .remove(&device.com_port);

        let generation = self.port_runtime_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.port_runtime.write().await.insert(
            device.com_port.clone(),
            PortRuntimeState {
                generation,
                lifecycle: PortLifecycle::Probing,
                active_sim_id: None,
                last_sim_id: None,
                module_connected: false,
                last_imei: None,
            },
        );

        self.clone()
            .add_unavailable_port(
                device.com_port.clone(),
                device.baud_rate,
                format!("device_{}", index),
                device.sms_storage.or(self.default_sms_storage),
                index,
                sse_manager,
                transcribe_cfg,
            )
            .await;

        log::warn!(
            "[manual-restart] {} released and queued for full initialization",
            device.com_port
        );
        Ok(())
    }

    /// Background task that probes one unavailable port at a fixed interval and
    /// performs full initialization when a SIM appears. Each port gets its own
    /// worker, so scan time no longer scales with the total number of ports.
    async fn unavailable_port_worker(
        self: Arc<Self>,
        com_port: String,
        baud_rate: u32,
        device_id: String,
        sms_storage: Option<SmsStorage>,
        index: usize,
        sse_manager: Arc<SseManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) {
        const PROBE_INTERVAL: Duration = Duration::from_secs(3);
        // Large fleets share bus bandwidth across many idle/empty ports; probing
        // all of them as aggressively as a small deployment adds background AT
        // traffic that can starve the demotion recheck on genuinely active ports.
        let device_count = self.devices.len();
        let probe_interval = PROBE_INTERVAL + Duration::from_secs((device_count as u64 / 48).min(7));
        let mut consecutive_init_failures: u8 = 0;
        let mut probe_miss_streak: u8 = 0;
        const MODULE_DISCONNECT_MISS_THRESHOLD: u8 = 10;
        const WINDOWS_PORT_REOPEN_SETTLE: Duration = Duration::from_millis(500);

        loop {
            // If the port is no longer in the unavailable list, this worker is done.
            if !self.unavailable_ports.read().await.iter().any(|(p, _)| p == &com_port) {
                break;
            }

            let generation = self.begin_port_probe_cycle(&com_port).await;

            // Quick probe: empty ports fail fast. Acquire the shared probe semaphore
            // so a large number of unavailable ports cannot overwhelm the USB hub.
            let present = {
                let port_lock = self.port_open_lock(&com_port).await;
                let _port_open_guard = port_lock.lock().await;
                let _permit = self.probe_semaphore.acquire().await;
                Modem::quick_probe_port(&com_port, baud_rate).await
            };
            if !present {
                probe_miss_streak = probe_miss_streak.saturating_add(1);
                if probe_miss_streak < 3 {
                    tokio::time::sleep(probe_interval).await;
                    continue;
                }
                // Do not downgrade to DISCONNECTED on short transient probe bursts.
                // Only mark disconnected after sustained misses and no remembered IMEI.
                if probe_miss_streak >= MODULE_DISCONNECT_MISS_THRESHOLD {
                    let keep_connected = self
                        .port_runtime
                        .read()
                        .await
                        .get(&com_port)
                        .and_then(|state| state.last_imei.clone())
                        .is_some();
                    self.set_port_module_connected(&com_port, keep_connected).await;
                }
                log::debug!(
                    "Port {} quick probe missed {} times; forcing a full init attempt to avoid false negatives.",
                    com_port,
                    probe_miss_streak
                );
            } else {
                self.set_port_module_connected(&com_port, true).await;
                probe_miss_streak = 0;
                // quick_probe_port owns a temporary Windows serial handle. Its
                // async I/O cancellation can complete shortly after Drop, so an
                // immediate full-init reopen intermittently fails with access
                // denied even though this process was the previous owner.
                tokio::time::sleep(WINDOWS_PORT_REOPEN_SETTLE).await;
            }

            // Race guard: this port may have already been promoted by another
            // path while this worker was waiting/sleeping. Never compete with a
            // live modem handle on the same COM port.
            if self.has_active_modem_on_port(&com_port).await {
                log::info!(
                    "Port {} already has an active modem; stopping unavailable worker to avoid self-contention.",
                    com_port
                );
                self.remove_unavailable_port(&com_port).await;
                break;
            }

            // Full init for ports that look like they have a modem/SIM. Gate this
            // behind the shared init semaphore: unbounded concurrent full inits
            // (each doing several AT round-trips) saturate a shared serial/USB
            // bus on large fleets, so every one of them times out and retries
            // forever, leaving freshly-inserted SIMs stuck as "SIM REMOVED".
            let port_lock = self.port_open_lock(&com_port).await;
            let _port_open_guard = port_lock.lock().await;
            let _init_permit = self.init_semaphore.acquire().await;
            match Self::initialize_single_modem_safe(
                com_port.clone(),
                baud_rate,
                device_id.clone(),
                sms_storage,
                index,
                self.force_uk_mcc_to_46001,
            )
            .await
            {
                Ok((sim_id, modem, is_new)) => {
                    let modem_arc = Arc::new(modem);

                    // Stabilization gate: confirm READY + same ICCID twice in a row
                    // before promotion to reduce rapid port flip oscillation.
                    let stable = {
                        let first_ok = tokio::time::timeout(Duration::from_secs(2), modem_arc.get_sim_status())
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten()
                            .map(|s| s.trim().eq_ignore_ascii_case("READY"))
                            .unwrap_or(false)
                            && tokio::time::timeout(Duration::from_secs(2), modem_arc.get_sim_iccid())
                                .await
                                .ok()
                                .and_then(|r| r.ok())
                                .flatten()
                                .map(|id| id == sim_id)
                                .unwrap_or(false);

                        if !first_ok {
                            false
                        } else {
                            tokio::time::sleep(Duration::from_millis(350)).await;
                            let second_ready = tokio::time::timeout(Duration::from_secs(2), modem_arc.get_sim_status())
                                .await
                                .ok()
                                .and_then(|r| r.ok())
                                .flatten()
                                .map(|s| s.trim().eq_ignore_ascii_case("READY"))
                                .unwrap_or(false);
                            let second_same_iccid = tokio::time::timeout(Duration::from_secs(2), modem_arc.get_sim_iccid())
                                .await
                                .ok()
                                .and_then(|r| r.ok())
                                .flatten()
                                .map(|id| id == sim_id)
                                .unwrap_or(false);
                            second_ready && second_same_iccid
                        }
                    };

                    if !stable {
                        log::info!(
                            "Port {} init result for SIM {} failed stability gate; retrying later",
                            com_port, sim_id
                        );
                        consecutive_init_failures = consecutive_init_failures.saturating_add(1);
                        continue;
                    }

                    self.manual_restarting_ports.write().await.remove(&com_port);
                    self.identity_revalidating_ports.write().await.remove(&com_port);

                    if !self.mark_port_active(&com_port, &sim_id, generation).await {
                        log::debug!(
                            "Stale generation for {} on {} (generation {}); reaffirming active state instead of dropping this healthy modem.",
                            sim_id,
                            com_port,
                            generation
                        );
                        self.reaffirm_port_active(&com_port, &sim_id).await;
                    }

                    info!(
                        "Port {} reconnected with SIM {}. Promoting to active.",
                        com_port, sim_id
                    );

                    // Keep each COM port independent even when multiple ports
                    // report the same ICCID in this deployment.
                    // Key the modem by its physical COM port, not by SIM id.
                    self
                        .modems
                        .write()
                        .await
                        .insert(com_port.clone(), modem_arc.clone());

                    if is_new {
                        self.init_new_sim_sms_data(vec![sim_id.clone()]).await;
                    }

                    // Refresh the SIM cache entry for this SIM.
                    match SimCard::find_by_conditions(Some(&sim_id), None, None, None).await {
                        Ok(cards) => {
                            if let Some(card) = cards.into_iter().next() {
                                self.sim_cards_cache.write().await.insert(sim_id.clone(), card);
                                self.set_last_known_sim_port(&com_port, &sim_id).await;
                                log::info!(
                                    "[recheck] Updated cache for reconnected SIM {} on port {}",
                                    sim_id, com_port
                                );
                            } else {
                                log::warn!(
                                    "[recheck] Reconnected SIM {} not found in DB, cache not updated",
                                    sim_id
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "[recheck] Failed to query DB for reconnected SIM {}: {}",
                                sim_id, e
                            );
                        }
                    }

                    // Start/replace the URC handler for this specific COM port.
                    let handle = tokio::spawn(Self::run_urc_handler(
                        sim_id.clone(),
                        modem_arc.clone(),
                        sse_manager.clone(),
                        transcribe_cfg.clone(),
                    ));
                    if let Some(old) = self
                        .urc_tasks
                        .write()
                        .await
                        .insert(com_port.clone(), handle)
                    {
                        old.abort();
                    }

                    // Remove the port from the unavailable list and stop this worker.
                    self.remove_unavailable_port(&com_port).await;
                    break;
                }
                Err(e) => {
                    let permission_denied = e.chain().any(|cause| {
                        cause
                            .downcast_ref::<std::io::Error>()
                            .map(|io_error| {
                                io_error.kind() == std::io::ErrorKind::PermissionDenied
                            })
                            .unwrap_or(false)
                    });
                    log::warn!(
                        "Port {} quick probe passed but full init failed: {}",
                        com_port, e
                    );
                    let e_raw = e.to_string();
                    let e_text = e_raw.to_ascii_lowercase();
                    let sim_not_ready = e_raw.contains("SIM_NOT_READY")
                        || e_raw.contains("No SIM detected")
                        || e_raw.contains("Failed to read SIM ICCID")
                        || e_raw.contains("SIM_REMOVED");
                    if sim_not_ready {
                        self.manual_restarting_ports.write().await.remove(&com_port);
                        self.set_port_module_connected(&com_port, true).await;
                        if let Some(imei) = Self::extract_imei_from_text(&e_raw) {
                            self.set_port_last_imei(&com_port, Some(imei)).await;
                        }
                        log::info!(
                            "Port {} modem is present but SIM is not ready/removed yet ({}); keeping fast retry loop.",
                            com_port,
                            e_raw
                        );
                        self.mark_port_unavailable(&com_port, None).await;
                        consecutive_init_failures = 0;
                        tokio::time::sleep(probe_interval).await;
                        continue;
                    }
                    // Another task may have reactivated this port while the init
                    // attempt was in flight; treat access-denied here as a race
                    // and stop this worker if a live modem now exists.
                    if (permission_denied
                        || e_text.contains("access is denied")
                        || e_text.contains("permission denied")
                        || e_raw.contains("拒绝访问"))
                        && self.has_active_modem_on_port(&com_port).await
                    {
                        log::warn!(
                            "Port {} init failed with access denied but modem is now active; stopping unavailable worker.",
                            com_port
                        );
                        self.manual_restarting_ports.write().await.remove(&com_port);
                        self.remove_unavailable_port(&com_port).await;
                        break;
                    }
                    if permission_denied
                        || e_text.contains("access is denied")
                        || e_text.contains("permission denied")
                        || e_raw.contains("拒绝访问")
                    {
                        self.set_port_module_connected(&com_port, true).await;
                        consecutive_init_failures = 0;
                        tokio::time::sleep(WINDOWS_PORT_REOPEN_SETTLE).await;
                        continue;
                    }
                    self.manual_restarting_ports.write().await.remove(&com_port);
                    self.mark_port_unavailable(&com_port, None).await;
                    consecutive_init_failures = consecutive_init_failures.saturating_add(1);
                }
            }

            let backoff_secs = if consecutive_init_failures == 0 {
                probe_interval.as_secs()
            } else {
                (probe_interval.as_secs())
                    .saturating_mul(1_u64 << consecutive_init_failures.min(4))
                    .min(12)
            };
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        }

        // Clean up our own task handle so the supervisor does not respawn us.
        self.unavailable_port_tasks.write().await.remove(&com_port);
    }

    async fn initialize_single_modem_safe(
        port: String,
        baud_rate: u32,
        device_id: String,
        sms_storage: Option<SmsStorage>,
        index: usize,
        force_uk_mcc_to_46001: bool,
    ) -> anyhow::Result<(String, Modem, bool)> {
        // Full modem initialization includes multiple AT probes and can exceed
        // 10s on busy ports (observed on COM17), which causes false
        // unavailable flapping despite quick probe success.
        const INIT_TIMEOUT_SECS: u64 = 20;

        // Some serial stack failures on Windows can panic internally while opening a COM port.
        // Catch unwind here so one bad port cannot terminate the whole process.
        let init_future = tokio::time::timeout(
            Duration::from_secs(INIT_TIMEOUT_SECS),
            Self::initialize_single_modem(
                port.clone(),
                baud_rate,
                device_id,
                sms_storage,
                index,
                force_uk_mcc_to_46001,
            ),
        );

        let result = std::panic::AssertUnwindSafe(init_future)
        .catch_unwind()
        .await;

        match result {
            Ok(Ok(v)) => v,
            Ok(Err(_elapsed)) => Err(anyhow::anyhow!(
                "Modem initialization timed out after {}s on port {}",
                INIT_TIMEOUT_SECS,
                port
            )),
            Err(_) => Err(anyhow::anyhow!(
                "Modem initialization panicked on port {} (likely serial driver/runtime issue)",
                port
            )),
        }
    }

    pub fn new(config: &crate::config::AppConfig) -> Self {
        let init_concurrency = config.settings.max_concurrent_modem_init.unwrap_or(3).max(1);
        let device_count = config.devices.len().max(1);
        // Quick probes are lightweight compared with full modem init. Keep probe
        // concurrency higher so hot-insert detection across many ports (e.g. 17-32)
        // is not serialized behind a tiny init semaphore. Large fleets (e.g. 96+
        // configured ports, often multiplexed over a single USB modem-pool board)
        // share serial/USB bus bandwidth, so raw concurrency stops helping past a
        // point and instead increases command collisions/timeouts on genuinely
        // active ports; halve it once the fleet is large enough for that to matter.
        let large_fleet_divisor: usize = if device_count > 48 { 2 } else { 1 };
        let probe_concurrency = (init_concurrency
            .saturating_mul(4)
            .clamp(8, 32)
            .min(device_count)
            / large_fleet_divisor)
            .max(4);
        let mut port_runtime = HashMap::new();
        for device in &config.devices {
            port_runtime.insert(
                device.com_port.clone(),
                PortRuntimeState {
                    generation: 0,
                    lifecycle: PortLifecycle::Unknown,
                    active_sim_id: None,
                    last_sim_id: None,
                    module_connected: false,
                    last_imei: None,
                },
            );
        }

        Self {
            modems: Arc::new(RwLock::new(HashMap::new())),
            sim_cards_cache: Arc::new(RwLock::new(HashMap::new())),
            port_runtime: Arc::new(RwLock::new(port_runtime)),
            last_known_sim_ids_by_port: Arc::new(RwLock::new(HashMap::new())),
            probe_semaphore: Arc::new(Semaphore::new(probe_concurrency)),
            init_semaphore: Arc::new(Semaphore::new(init_concurrency)),
            urc_tasks: RwLock::new(HashMap::new()),
            unavailable_ports: RwLock::new(Vec::new()),
            unavailable_port_tasks: RwLock::new(HashMap::new()),
            port_open_locks: RwLock::new(HashMap::new()),
            devices: config.devices.clone(),
            force_uk_mcc_to_46001: config.settings.force_uk_mcc_to_46001.unwrap_or(true),
            max_concurrent_modem_init: init_concurrency,
            default_sms_storage: config.settings.sms_storage,
            sim_probe_fail_counts: RwLock::new(HashMap::new()),
            sim_swap_confirm_streaks: RwLock::new(HashMap::new()),
            recent_ready_at: RwLock::new(HashMap::new()),
            sim_probe_cooldown_until: RwLock::new(HashMap::new()),
            initialization_complete: RwLock::new(false),
            post_register_recovery_tasks: RwLock::new(HashMap::new()),
            post_register_recovery_seq: AtomicU64::new(0),
            port_runtime_seq: AtomicU64::new(0),
            manual_restarting_ports: RwLock::new(HashSet::new()),
            identity_revalidating_ports: RwLock::new(HashSet::new()),
        }
    }

    async fn clear_post_register_recovery_task_if_current(&self, sim_id: &str, task_id: u64) {
        let mut tasks = self.post_register_recovery_tasks.write().await;
        if let Some((current_id, _)) = tasks.get(sim_id) {
            if *current_id == task_id {
                tasks.remove(sim_id);
            }
        }
    }

    async fn run_post_register_recovery(&self, modem: Arc<Modem>, sim_id: &str) {
        const NETWORK_POLL_INTERVAL: Duration = Duration::from_secs(15);
        const NETWORK_POLL_MAX_TIMES: u8 = 4;

        let mut last_status_code = "unknown".to_string();
        let mut last_operator_id = "unknown".to_string();

        for attempt in 1..=NETWORK_POLL_MAX_TIMES {
            tokio::time::sleep(NETWORK_POLL_INTERVAL).await;

            let network = match modem.check_network_registration().await {
                Ok(v) => v,
                Err(e) => {
                    log::warn!(
                        "[force_register][recovery] SIM {} failed to read network status: {}",
                        sim_id,
                        e
                    );
                    None
                }
            };

            let operator = match modem.check_operator().await {
                Ok(v) => v,
                Err(e) => {
                    log::warn!(
                        "[force_register][recovery] SIM {} failed to read operator: {}",
                        sim_id,
                        e
                    );
                    None
                }
            };

            let is_roaming = network.as_ref().is_some_and(|n| n.is_roaming());
            let is_unicom_46001 = operator.as_ref().is_some_and(|o| o.is_operator("46001"));
            let status_code = network
                .as_ref()
                .map(|n| n.status_code().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let operator_id = operator
                .as_ref()
                .map(|o| o.operator_id().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            last_status_code = status_code.clone();
            last_operator_id = operator_id.clone();

            if is_roaming && is_unicom_46001 {
                log::info!(
                    "[force_register][recovery] SIM {} reached target roaming registration on 46001 after {}s (status={}, operator_id={})",
                    sim_id,
                    attempt as u64 * NETWORK_POLL_INTERVAL.as_secs(),
                    status_code,
                    operator_id
                );
                return;
            }

            log::info!(
                "[force_register][recovery] SIM {} not yet at target state after {}s (status={}, operator_id={}); rechecking ({}/{})",
                sim_id,
                attempt as u64 * NETWORK_POLL_INTERVAL.as_secs(),
                status_code,
                operator_id,
                attempt,
                NETWORK_POLL_MAX_TIMES
            );
        }

        log::warn!(
            "[force_register][recovery] SIM {} did not reach roaming on 46001 within {}s (status={}, operator_id={}); rebooting",
            sim_id,
            NETWORK_POLL_INTERVAL.as_secs() * NETWORK_POLL_MAX_TIMES as u64,
            last_status_code,
            last_operator_id
        );

        log::warn!(
            "[force_register][recovery] SIM {} sending AT+CFUN=1,1 before retrying COPS",
            sim_id
        );
        modem.reboot().await;

        // Wait up to 90s for the module to become responsive again.
        let mut alive = false;
        for _ in 0..45 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if modem.probe_alive().await {
                alive = true;
                break;
            }
        }

        if !alive {
            log::warn!(
                "[force_register][recovery] SIM {} did not become responsive after reboot; attempting COPS anyway",
                sim_id
            );
        }

        tokio::time::sleep(Duration::from_secs(15)).await;

        log::warn!(
            "[force_register][recovery] SIM {} sending AT+COPS=1,2,\"46001\",7 after reboot",
            sim_id
        );
        match modem.register_cops_46001().await {
            Ok(()) => log::info!(
                "[force_register][recovery] SIM {} AT+COPS=1,2,\"46001\",7 succeeded",
                sim_id
            ),
            Err(e) => log::error!(
                "[force_register][recovery] SIM {} AT+COPS=1,2,\"46001\",7 failed: {}",
                sim_id,
                e
            ),
        }
    }

    /// Start post-register recovery for one SIM immediately in the background.
    /// Any existing recovery task for the same SIM is cancelled and replaced.
    pub async fn start_post_register_recovery(self: Arc<Self>, modem: Arc<Modem>, sim_id: String) {
        let task_id = self.post_register_recovery_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let manager = self.clone();
        let modem_for_task = modem.clone();
        let sim_id_for_task = sim_id.clone();

        let handle = tokio::spawn(async move {
            manager
                .run_post_register_recovery(modem_for_task, &sim_id_for_task)
                .await;
            manager
                .clear_post_register_recovery_task_if_current(&sim_id_for_task, task_id)
                .await;
        });

        let mut tasks = self.post_register_recovery_tasks.write().await;
        if let Some((_old_id, old_handle)) = tasks.insert(sim_id.clone(), (task_id, handle)) {
            old_handle.abort();
        }

        log::info!(
            "[force_register][recovery] Started immediate recovery task for SIM {} (task_id={})",
            sim_id,
            task_id
        );
    }

    /// Initialize all configured modems and start their URC handlers.
    /// This is intentionally run in the background so the HTTP API can start
    /// immediately without waiting for slow/disconnected serial ports to time out.
    ///
    /// Startup now happens in two phases so it scales with many ports:
    ///   1) Quick-probe every configured port in parallel (a few seconds per port).
    ///   2) Run full modem initialization only on ports that passed the probe.
    ///   3) Spawn dedicated background workers for the remaining unavailable ports.
    pub async fn initialize_modems(
        self: Arc<Self>,
        sse_manager: Arc<SseManager>,
        _webhook_manager: Option<webhook::WebhookManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) -> anyhow::Result<()> {
        let force_uk_mcc_to_46001 = self.force_uk_mcc_to_46001;
        let max_concurrent = self.max_concurrent_modem_init.max(1);
        let device_count = self.devices.len();

        info!(
            "Starting modem discovery for {} configured port(s) (max_concurrent_modem_init={})",
            device_count, max_concurrent
        );

        // ── Phase 1: quick-probe all configured ports in parallel ──────────────
        let mut probe_futs = FuturesUnordered::new();
        for (index, device) in self.devices.iter().enumerate() {
            let sem = self.probe_semaphore.clone();
            let mm = self.clone();
            let port = device.com_port.clone();
            let baud_rate = device.baud_rate;
            probe_futs.push(async move {
                let port_lock = mm.port_open_lock(&port).await;
                let _port_open_guard = port_lock.lock().await;
                let _permit = sem.acquire().await;
                let present = Modem::quick_probe_port(&port, baud_rate).await;
                (index, port, baud_rate, present)
            });
        }

        let mut probe_results = Vec::new();
        while let Some((index, port, baud_rate, present)) = probe_futs.next().await {
            probe_results.push((index, port, baud_rate, present));
        }

        let probed_present: Vec<usize> = probe_results
            .iter()
            .filter(|(_, _, _, present)| *present)
            .map(|(index, _, _, _)| *index)
            .collect();
        let probed_absent: Vec<usize> = probe_results
            .iter()
            .filter(|(_, _, _, present)| !present)
            .map(|(index, _, _, _)| *index)
            .collect();

        info!(
            "Quick probe complete: {} port(s) look active, {} port(s) empty/unresponsive",
            probed_present.len(),
            probed_absent.len()
        );

        // ── Phase 2: full initialization only on ports that passed the probe ───
        let mut new_sim_ids = Vec::new();
        // Share the same semaphore the reconnect workers use, so startup
        // inits and later hot-insert inits never together exceed the bus's
        // safe concurrency.
        let init_semaphore = self.init_semaphore.clone();
        let mut init_futs = FuturesUnordered::new();

        let init_candidates: Vec<usize> = if probed_present.is_empty() {
            log::warn!(
                "Quick probe found 0 active ports; running immediate full-init fallback across all configured ports"
            );
            (0..self.devices.len()).collect()
        } else {
            probed_present
        };

        for index in init_candidates {
            let device = &self.devices[index];
            let port = device.com_port.clone();
            let baud_rate = device.baud_rate;
            let sms_storage = device.sms_storage.or(self.default_sms_storage);
            let temp_device_id = format!("device_{}", index);
            let sem = init_semaphore.clone();
            let mm = self.clone();

            init_futs.push(async move {
                mm.begin_port_probe_cycle(&port).await;
                let port_lock = mm.port_open_lock(&port).await;
                let _port_open_guard = port_lock.lock().await;
                let _permit = sem.acquire().await;
                let result = Self::initialize_single_modem_safe(
                    port.clone(),
                    baud_rate,
                    temp_device_id,
                    sms_storage,
                    index,
                    force_uk_mcc_to_46001,
                )
                .await;
                (port, index, result)
            });
        }

        let mut unavailable_after_init: Vec<(String, u32, usize)> = Vec::new();
        while let Some((port, index, result)) = init_futs.next().await {
            match result {
                Ok((sim_id, modem, is_new)) => {
                    if is_new {
                        new_sim_ids.push(sim_id.clone());
                    }
                    let modem_arc = Arc::new(modem);
                    // initialize_single_modem already requires two matching
                    // post-init READY/ICCID samples. Publish this port now so the
                    // dashboard does not wait for the fleet-wide reconciliation.
                    self.reaffirm_port_active(&port, &sim_id).await;
                    self.set_last_known_sim_port(&port, &sim_id).await;
                    // Key the modem by its physical COM port, not by SIM id.
                    self.modems.write().await.insert(port.clone(), modem_arc);
                }
                Err(e) => {
                    error!("Failed to initialize modem on {}: {}", port, e);
                    self.mark_port_unavailable(&port, None).await;
                    unavailable_after_init.push((port, self.devices[index].baud_rate, index));
                }
            }
        }

        // ── Phase 3: spawn dedicated workers for all unavailable ports ──────────
        for index in probed_absent {
            let device = &self.devices[index];
            self.clone().add_unavailable_port(
                device.com_port.clone(),
                device.baud_rate,
                format!("device_{}", index),
                device.sms_storage.or(self.default_sms_storage),
                index,
                sse_manager.clone(),
                transcribe_cfg.clone(),
            )
            .await;
        }
        for (port, baud_rate, index) in unavailable_after_init {
            let device = &self.devices[index];
            self.clone().add_unavailable_port(
                port,
                baud_rate,
                format!("device_{}", index),
                device.sms_storage.or(self.default_sms_storage),
                index,
                sse_manager.clone(),
                transcribe_cfg.clone(),
            )
            .await;
        }

        let active_count = self.modems.read().await.len();
        let unavailable_count = self.unavailable_ports.read().await.len();
        if active_count == 0 {
            log::warn!(
                "No modems with valid SIM cards were initialized ({} ports unavailable). \
                 Starting with empty modem set.",
                unavailable_count
            );
        }

        info!(
            "Modem initialization pass finished: {} active, {} unavailable",
            active_count, unavailable_count
        );

        self.reconcile_startup_sim_identities().await;

        // Reconciliation can reject an initialized port whose ICCID is unstable
        // or also claimed by another port. Start its recovery immediately rather
        // than waiting for the next fleet-wide invariant sweep.
        let active_ports: std::collections::HashSet<String> = self
            .modems
            .read()
            .await
            .keys()
            .cloned()
            .collect();
        for (index, device) in self.devices.iter().enumerate() {
            if active_ports.contains(&device.com_port)
                || self
                    .unavailable_ports
                    .read()
                    .await
                    .iter()
                    .any(|(port, _)| port == &device.com_port)
            {
                continue;
            }
            self.clone()
                .add_unavailable_port(
                    device.com_port.clone(),
                    device.baud_rate,
                    format!("device_{}", index),
                    device.sms_storage.or(self.default_sms_storage),
                    index,
                    sse_manager.clone(),
                    transcribe_cfg.clone(),
                )
                .await;
        }

        self.init_sim_cache().await?;
        self.cleanup_stale_sim_cards().await?;

        if !new_sim_ids.is_empty() {
            self.init_new_sim_sms_data(new_sim_ids).await;
        }

        self.start_urc_handlers(sse_manager, transcribe_cfg).await;

        {
            let mut complete = self.initialization_complete.write().await;
            *complete = true;
        }
        log::info!("Modem initialization complete");

        Ok(())
    }

    async fn initialize_single_modem(
        port: String,
        baud_rate: u32,
        device_id: String,
        sms_storage: Option<SmsStorage>,
        index: usize,
        force_uk_mcc_to_46001: bool,
    ) -> anyhow::Result<(String, Modem, bool)> {
        info!("Initializing modem on port {}", port);

        let modem = Modem::new(
            &port,
            baud_rate,
            &device_id,
            index,
            force_uk_mcc_to_46001,
        )
        .await?;

        // Stage 1: verify the modem is actually responsive on this COM port.
        const MODEM_READY_ATTEMPTS: u8 = 4;
        const MODEM_READY_DELAY_MS: u64 = 300;
        let mut modem_alive = false;
        for attempt in 1..=MODEM_READY_ATTEMPTS {
            if modem.is_responsive().await {
                modem_alive = true;
                break;
            }
            if attempt < MODEM_READY_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(MODEM_READY_DELAY_MS)).await;
            }
        }
        if !modem_alive {
            return Err(anyhow::anyhow!(
                "MODEM_NO_RESPONSE: {} did not respond to AT probes",
                port
            ));
        }

        // Stage 2: probe IMEI early so COM<->modem identity is confirmed.
        // This is only for display/identity; SIM readiness is decided solely by
        // AT+CPIN? returning +CPIN: READY in the next stage.
        const IMEI_READ_ATTEMPTS: u8 = 4;
        const IMEI_RETRY_DELAY_MS: u64 = 300;
        let mut imei_seen = false;
        let mut last_imei: Option<String> = None;
        for attempt in 1..=IMEI_READ_ATTEMPTS {
            match modem.get_imei().await {
                Ok(Some(v)) if !v.trim().is_empty() => {
                    imei_seen = true;
                    last_imei = Some(v);
                    break;
                }
                _ => {
                    if attempt < IMEI_READ_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(IMEI_RETRY_DELAY_MS)).await;
                    }
                }
            }
        }
        if !imei_seen {
            log::warn!(
                "IMEI probe did not succeed immediately on {}; continuing to CPIN/SIM checks",
                port
            );
        }

        // Stage 3: CPIN READY is the only SIM-ready condition.
        const CPIN_READ_ATTEMPTS: u8 = 8;
        const CPIN_RETRY_DELAY_MS: u64 = 400;
        let mut sim_ready = false;
        let mut last_cpin: Option<String> = None;
        for attempt in 1..=CPIN_READ_ATTEMPTS {
            match modem.get_sim_status().await {
                Ok(Some(status)) => {
                    last_cpin = Some(status.clone());
                    if status.trim().eq_ignore_ascii_case("READY") {
                        sim_ready = true;
                        break;
                    }
                }
                Ok(None) => {
                    last_cpin = Some("<empty>".to_string());
                }
                Err(e) => {
                    last_cpin = Some(format!("error: {}", e));
                }
            }
            if attempt < CPIN_READ_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(CPIN_RETRY_DELAY_MS)).await;
            }
        }
        if !sim_ready {
            return Err(anyhow::anyhow!(
                "SIM_REMOVED: {} cpin_last={:?} imei={:?}",
                port,
                last_cpin,
                last_imei
            ));
        }

        // Newly inserted SIMs can take a short moment before ICCID becomes readable.
        // Retry quickly here so hot-inserted cards do not wait for the next worker cycle.
        const ICCID_READ_ATTEMPTS: u8 = 8;
        const ICCID_RETRY_DELAY_MS: u64 = 500;
        let mut pre_sim_id = None;
        for attempt in 1..=ICCID_READ_ATTEMPTS {
            match modem.get_sim_iccid().await {
                Ok(Some(id)) => {
                    pre_sim_id = Some(id);
                    break;
                }
                Ok(None) => {
                    if attempt < ICCID_READ_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(ICCID_RETRY_DELAY_MS)).await;
                        continue;
                    }
                    log::debug!("No SIM detected on port {}, treating as unavailable", port);
                    return Err(anyhow::anyhow!("No SIM detected"));
                }
                Err(e) => {
                    if attempt < ICCID_READ_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(ICCID_RETRY_DELAY_MS)).await;
                        continue;
                    }
                    log::debug!(
                        "Failed to read SIM ICCID on port {} (cannot communicate): {}. Treating as unavailable.",
                        port, e
                    );
                    return Err(anyhow::anyhow!("Failed to read SIM ICCID: {}", e));
                }
            }
        }

        let pre_sim_id = pre_sim_id.unwrap();
        let pre_sim_is_new = Self::is_new_sim_id(&pre_sim_id).await;

        if let Err(e) = modem.init_modem(sms_storage).await {
            log::warn!(
                "Modem AT init failed for port {}: {}. Treating as unavailable.",
                port, e
            );
            return Err(anyhow::anyhow!("Modem AT init failed: {}", e));
        }

        // Some multi-port modem banks briefly expose the previous slot's ICCID
        // after startup or a bulk SIM swap. init_modem() performs its own SIM
        // refresh, so never overwrite that result with the earlier probe. Require
        // two matching post-init READY/ICCID samples before exposing this port.
        const POST_INIT_STABLE_ATTEMPTS: u8 = 8;
        const POST_INIT_STABLE_DELAY_MS: u64 = 500;
        let mut previous_iccid: Option<String> = None;
        let mut stable_iccid: Option<String> = None;
        for attempt in 1..=POST_INIT_STABLE_ATTEMPTS {
            let ready = modem
                .get_sim_status()
                .await
                .ok()
                .flatten()
                .is_some_and(|status| status.trim().eq_ignore_ascii_case("READY"));
            let current_iccid = if ready {
                modem
                    .get_sim_iccid()
                    .await
                    .ok()
                    .flatten()
                    .filter(|id| !id.trim().is_empty())
            } else {
                None
            };

            if current_iccid.is_some() && current_iccid == previous_iccid {
                stable_iccid = current_iccid;
                break;
            }
            previous_iccid = current_iccid;

            if attempt < POST_INIT_STABLE_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(POST_INIT_STABLE_DELAY_MS)).await;
            }
        }

        let sim_id = stable_iccid.ok_or_else(|| {
            anyhow::anyhow!(
                "SIM_ID_UNSTABLE: {} pre_init_iccid={} post_init_last={:?}",
                port,
                pre_sim_id,
                previous_iccid
            )
        })?;

        if sim_id != pre_sim_id {
            log::warn!(
                "Port {} ICCID changed during initialization (pre-init {}, stable post-init {}); using stable identity",
                port,
                pre_sim_id,
                sim_id
            );
        }

        let is_new_sim = if sim_id == pre_sim_id {
            pre_sim_is_new
        } else {
            Self::is_new_sim_id(&sim_id).await
        };

        // Keep the modem and manager on the same verified post-init identity.
        *modem.sim_id.write().await = Some(sim_id.clone());

        info!(
            "Successfully initialized modem on {} with SIM ID: {}",
            port, sim_id
        );

        Ok((sim_id, modem, is_new_sim))
    }

    async fn reconcile_startup_sim_identities(&self) {
        let modems: Vec<Arc<Modem>> = self.modems.read().await.values().cloned().collect();
        let mut claimed_iccid_counts = std::collections::HashMap::new();
        for modem in &modems {
            if let Some(sim_id) = modem.sim_id.read().await.clone() {
                *claimed_iccid_counts.entry(sim_id).or_insert(0usize) += 1;
            }
        }
        for modem in modems {
            let startup_sim_id = modem.sim_id.read().await.clone();
            let duplicate_startup_claim = startup_sim_id
                .as_ref()
                .and_then(|sim_id| claimed_iccid_counts.get(sim_id))
                .is_some_and(|count| *count > 1);
            if duplicate_startup_claim {
                log::warn!(
                    "[startup-reconcile] {} shares startup ICCID {:?} with another port; keeping it hidden for automatic recovery",
                    modem.com_port,
                    startup_sim_id
                );
                self.modems.write().await.remove(&modem.com_port);
                modem.shutdown().await;
                self.mark_port_unavailable(&modem.com_port, startup_sim_id.as_deref())
                    .await;
                continue;
            }
        }
    }

    async fn is_new_sim_id(sim_id: &str) -> bool {
        match SimCard::find_by_conditions(Some(sim_id), None, None, None).await {
            Ok(existing) => existing.is_empty(),
            Err(e) => {
                log::warn!("Failed to check SIM ID existence: {}", e);
                true
            }
        }
    }

    async fn init_sim_cache(&self) -> anyhow::Result<()> {
        // The modems map is keyed by COM port, so build (sim_id, modem) pairs
        // from each modem's own runtime SIM id.
        let sim_modems: Vec<(String, Arc<Modem>)> = {
            let modems = self.modems.read().await;
            let mut v = Vec::with_capacity(modems.len());
            for modem in modems.values() {
                if let Some(sid) = modem.sim_id.read().await.clone() {
                    v.push((sid, modem.clone()));
                }
            }
            v
        };

        if sim_modems.is_empty() {
            info!("Initialized SIM cache with 0 cards (no modems available)");
            return Ok(());
        }

        let sim_id_refs: Vec<&str> = sim_modems.iter().map(|(id, _)| id.as_str()).collect();
        let sim_cards = SimCard::get_by_ids(&sim_id_refs).await?;
        let cached_ids: HashSet<String> = sim_cards.keys().cloned().collect();

        // Active modems without a sim_cards row can appear after a transient
        // demotion deleted the DB record while the modem stayed active. Recreate
        // the row from live SIM info so the dashboard always shows IMSI.
        let missing: Vec<(String, Arc<Modem>)> = sim_modems
            .iter()
            .filter(|(id, _)| !cached_ids.contains(id.as_str()))
            .map(|(id, modem)| (id.clone(), modem.clone()))
            .collect();

        let mut cache = self.sim_cards_cache.write().await;
        *cache = sim_cards;
        drop(cache);

        {
            let mut last_known = self.last_known_sim_ids_by_port.write().await;
            last_known.clear();
            for (sim_id, modem) in sim_modems.iter() {
                last_known.insert(modem.com_port.clone(), sim_id.clone());
            }
        }

        for (iccid, modem) in missing {
            log::info!(
                "[init_sim_cache] Active modem {} on {} has no sim_cards row; recreating from live SIM info",
                iccid, modem.com_port
            );
            let (imsi_res, phone_res) = tokio::join!(modem.get_sim_imsi(), modem.get_phone_number());
            let imsi = imsi_res.ok().flatten();
            let phone_number = phone_res.ok().flatten();
            match SimCard::find_or_create_with_phone(&iccid, imsi, phone_number).await {
                Ok(card) => {
                    let mut cache = self.sim_cards_cache.write().await;
                    cache.insert(card.id.clone(), card);
                }
                Err(e) => {
                    log::warn!(
                        "[init_sim_cache] Failed to recreate sim_cards row for {}: {}",
                        iccid, e
                    );
                }
            }
        }

        info!(
            "Initialized SIM cache with {} cards",
            self.sim_cards_cache.read().await.len()
        );
        Ok(())
    }

    /// Remove sim_cards rows for SIMs that are no longer physically present.
    /// This prevents the dashboard/API from showing stale IMSI/ICCID/phone info
    /// after a SIM has been pulled out (especially when the server was stopped
    /// at the time the SIM was removed).
    async fn cleanup_stale_sim_cards(&self) -> anyhow::Result<()> {
        log::info!(
            "Skipping destructive stale sim_cards cleanup during runtime churn; keeping last-known SIM identity rows intact"
        );
        Ok(())
    }

    async fn init_new_sim_sms_data(&self, new_sim_ids: Vec<String>) {
        let snapshots = self.get_port_runtime_snapshots().await;
        if Self::should_defer_sms_reads_from_snapshots(&snapshots) {
            log::info!(
                "Deferring initial SMS backfill for {} new SIM(s) while port recovery is still in progress",
                new_sim_ids.len()
            );
            return;
        }

        let mut futures = FuturesUnordered::new();

        for sim_id in new_sim_ids {
            // Clone the modem Arc and release the map lock BEFORE the slow
            // full-SIM SMS scan below; holding the read guard across it stalls
            // all other promotions/demotions.
            let modem = self.get_modem(&sim_id).await;
            futures.push(async move {
                if let Some(modem) = modem {
                    match modem.read_sms_sync_insert(SmsType::All).await {
                        Ok(()) => info!("Initialized SMS data for new SIM: {}", sim_id),
                        Err(e) => error!("Failed to initialize SMS data for {}: {}", sim_id, e),
                    }
                }
            });
        }

        while futures.next().await.is_some() {}
    }

    pub async fn get_sim_ids(&self) -> Vec<String> {
        // The modems map is keyed by COM port; each modem carries its own SIM id.
        let modems = self.modems.read().await;
        let mut ids = Vec::with_capacity(modems.len());
        for modem in modems.values() {
            if let Some(sid) = modem.sim_id.read().await.clone() {
                ids.push(sid);
            }
        }
        ids
    }

    /// All configured (com_port, baud_rate) pairs, in config order.
    pub fn configured_ports(&self) -> Vec<(String, u32)> {
        self.devices
            .iter()
            .map(|d| (d.com_port.clone(), d.baud_rate))
            .collect()
    }

    /// Resolve a modem by its current SIM id. The modems map is keyed by COM
    /// port, so this scans the (small, <=128) set matching each modem's own
    /// runtime SIM id. This keeps SIM identity a property of the physical port
    /// instead of a fragile map key that breaks on hot-swap.
    pub async fn get_modem(&self, sim_id: &str) -> Option<Arc<Modem>> {
        let modems = self.modems.read().await;
        for modem in modems.values() {
            if modem.sim_id.read().await.as_deref() == Some(sim_id) {
                return Some(modem.clone());
            }
        }
        None
    }

    pub async fn send_sms(
        &self,
        sim_id: &str,
        contact: &Contact,
        message: &str,
    ) -> anyhow::Result<(i64, String)> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.send_sms_pdu(contact, message).await
    }

    pub async fn read_sms(&self, sim_id: &str, sms_type: SmsType) -> anyhow::Result<Vec<ModemSMS>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.read_sms(sms_type).await.map(|(list, _)| list).map_err(Into::into)
    }

    pub async fn read_sms_async_insert(
        &self,
        sim_id: &str,
        sms_type: SmsType,
        sse_manager: Arc<SseManager>,
        webhook_manager: Option<webhook::WebhookManager>,
    ) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem
            .read_sms_async_insert(sms_type, sse_manager, webhook_manager)
            .await
    }

    pub async fn read_sms_sync_insert(
        &self,
        sim_id: &str,
        sms_type: SmsType,
    ) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.read_sms_sync_insert(sms_type).await
    }

    /// Read SMS from modem, sync to DB, and return the latest incoming message from modem read.
    pub async fn read_sms_and_get_latest(
        &self,
        sim_id: &str,
    ) -> anyhow::Result<Option<Sms>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        let (sms_list, mms_found) = modem.read_sms(crate::modem::SmsType::All).await?;

        if sms_list.is_empty() && !mms_found {
            return Ok(None);
        }

        // Delete even if sms_list is empty but an MMS WAP-push notification
        // was found, so notifications don't accumulate in SIM storage forever.
        if let Err(e) = modem.delete_all_sms().await {
            log::warn!("Failed to delete SMS from modem after reading: {}", e);
        }

        if sms_list.is_empty() {
            return Ok(None);
        }

        ModemSMS::bulk_insert(&sms_list).await?;

        let latest = sms_list
            .into_iter()
            .filter(|s| !s.send)
            .max_by_key(|s| s.timestamp);

        let Some(sms) = latest else {
            return Ok(None);
        };

        let sms_id = match Sms::find_latest_incoming_id_by_sim_message(sim_id, &sms.message).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                log::warn!(
                    "Failed to resolve inserted SMS id for SIM {} after modem read; falling back to id=0",
                    sim_id
                );
                0
            }
            Err(e) => {
                log::warn!(
                    "Failed to query inserted SMS id for SIM {} after modem read: {}; falling back to id=0",
                    sim_id,
                    e
                );
                0
            }
        };

        Ok(Some(Sms {
            id: sms_id,
            contact_id: sms.contact,
            timestamp: sms.timestamp,
            message: sms.message,
            sim_id: sms.sim_id,
            send: sms.send,
            status: SmsStatus::Read,
            uploaded_to_platform: false,
            platform_item_id: None,
            platform_uploaded_at: None,
            platform_response: None,
        }))
    }

    pub async fn read_all_sms_async(
        &self,
        sms_type: SmsType,
        sse_manager: Arc<SseManager>,
        webhook_manager: Option<webhook::WebhookManager>,
    ) {
        let snapshots = self.get_port_runtime_snapshots().await;
        if Self::should_defer_sms_reads_from_snapshots(&snapshots) {
            log::info!(
                "Deferring SMS read cycle while {} previously occupied port(s) are still recovering",
                snapshots
                    .iter()
                    .filter(|snapshot| snapshot.last_sim_id.is_some() && snapshot.lifecycle != PortLifecycle::Active)
                    .count()
            );
            return;
        }

        // Snapshot the active modems and release the map lock BEFORE any serial
        // I/O. Holding the read guard across the SMS reads below blocks all
        // writers (SIM promotions/demotions) for the duration of the slowest
        // modem, which stalled dashboard updates for minutes after a SIM move.
        let entries: Vec<(String, Arc<Modem>)> = {
            let modems = self.modems.read().await;
            let mut v = Vec::with_capacity(modems.len());
            for modem in modems.values() {
                if modem.is_esim_mode().await {
                    continue;
                }
                if let Some(sid) = modem.sim_id.read().await.clone() {
                    if !sid.starts_with("fallback_sim_") {
                        v.push((sid, modem.clone()));
                    }
                }
            }
            v
        };

        let mut futures = FuturesUnordered::new();
        for (sim_id, modem) in entries {
            let sse_manager = sse_manager.clone();
            let webhook_manager = webhook_manager.clone();
            futures.push(async move {
                if let Err(e) = modem
                    .read_sms_async_insert(sms_type, sse_manager, webhook_manager)
                    .await
                {
                    error!("Failed to read SMS for {}: {}", sim_id, e);
                }
            });
        }

        while futures.next().await.is_some() {}
    }

    /// Re-check all currently active modems for SIM insertion/removal.
    /// Demotes modems where AT+CPIN? reports the SIM is not ready, or where
    /// +CCID returns a different or missing ICCID. Reconnection is handled by
    /// per-port background workers, so this function only manages demotion and
    /// makes sure every unavailable port has a live worker.
    pub async fn recheck_fallback_modems(
        self: Arc<Self>,
        _sms_storage_map: &std::collections::HashMap<String, Option<crate::config::SmsStorage>>,
        sse_manager: Arc<SseManager>,
        _webhook_manager: Option<webhook::WebhookManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) {
        if !self.is_initialization_complete().await {
            log::debug!("Skipping SIM recheck while initial modem discovery is still running");
            return;
        }

        // ── Demotion: parallel AT+CPIN? / AT+CCID on all active modems ────────
        let active_entries: Vec<(String, Arc<Modem>)> = {
            let modems = self.modems.read().await;
            let mut v = Vec::with_capacity(modems.len());
            for modem in modems.values() {
                if modem.is_esim_mode().await {
                    continue;
                }
                if let Some(sid) = modem.sim_id.read().await.clone() {
                    v.push((sid, modem.clone()));
                }
            }
            v
        };
        let active_entries_len = active_entries.len();

        enum ProbeOutcome {
            Healthy { iccid: String, modem: Arc<Modem> },
            Swap {
                iccid: String,
                com_port: String,
                modem: Arc<Modem>,
                new_iccid: String,
            },
            ConfirmedAbsent {
                iccid: String,
                com_port: String,
                modem: Arc<Modem>,
            },
            TransientIssue {
                iccid: String,
                com_port: String,
                hide_identity: bool,
            },
        }

        let mut demotion_futs = FuturesUnordered::new();
        for (iccid, modem) in active_entries {
            let mgr = self.clone();
            demotion_futs.push(async move {
                // AT+CPIN? is the most reliable indicator of physical SIM presence.
                // If the SIM is pulled out, the modem typically reports
                // +CPIN: SIM REMOVED, +CME ERROR, or plain ERROR.
                // Use a generous explicit timeout so a dead modem cannot stall the
                // whole recheck cycle, while still tolerating shared-bus latency
                // spikes on large fleets (e.g. COM1-96 modem-pool boards) that
                // would otherwise be misread as SIM removal.
                let status_probe = tokio::time::timeout(Duration::from_secs(10), modem.get_sim_status()).await;
                let mut status: Option<String> = None;
                let mut status_error: Option<String> = None;
                match status_probe {
                    Ok(Ok(v)) => {
                        status = v;
                    }
                    Ok(Err(e)) => {
                        status_error = Some(e.to_string());
                    }
                    Err(_) => {
                        status_error = Some("timeout".to_string());
                    }
                }
                let status_ready = status
                    .as_deref()
                    .map(|s| s.trim().eq_ignore_ascii_case("READY"))
                    .unwrap_or(false);
                if !status_ready {
                    let active_on_other_port = mgr
                        .is_iccid_active_on_other_port(&iccid, &modem.com_port)
                        .await;
                    if active_on_other_port {
                        log::warn!(
                            "SIM {} on {} is not READY and already active on another port; demoting stale mapping immediately.",
                            iccid,
                            modem.com_port
                        );
                        return ProbeOutcome::ConfirmedAbsent {
                            iccid,
                            com_port: modem.com_port.clone(),
                            modem,
                        };
                    }

                    let explicit_absent = status
                        .as_deref()
                        .map(|s| {
                            let upper = s.trim().to_ascii_uppercase();
                            upper.contains("REMOVED")
                                || upper.contains("NOT INSERTED")
                                || upper.contains("SIM FAILURE")
                        })
                        .unwrap_or(false);

                    // Many modems do not emit an explicit "NOT INSERTED" after a
                    // hot pull; AT+CPIN? just returns an empty/non-READY response
                    // (e.g. +CME ERROR: 10). Corroborate with a fresh IMSI read:
                    // a physically absent SIM cannot return a valid IMSI, while a
                    // transient CPIN glitch on a present SIM still can.
                    let status_transient_error = status_error
                        .as_deref()
                        .map(|e| {
                            let lower = e.to_ascii_lowercase();
                            lower.contains("backlogged")
                                || lower.contains("wouldblock")
                                || lower.contains("timeout")
                                || lower.contains("timed out")
                        })
                        .unwrap_or(false);

                    let imsi_probe = tokio::time::timeout(Duration::from_secs(10), modem.get_sim_imsi()).await;
                    let mut imsi_value: Option<String> = None;
                    let mut imsi_error: Option<String> = None;
                    match imsi_probe {
                        Ok(Ok(v)) => {
                            imsi_value = v;
                        }
                        Ok(Err(e)) => {
                            imsi_error = Some(e.to_string());
                        }
                        Err(_) => {
                            imsi_error = Some("timeout".to_string());
                        }
                    }
                    let imsi_ok = imsi_value
                        .as_deref()
                        .map(|v| !v.trim().is_empty())
                        .unwrap_or(false);
                    let imsi_transient_error = imsi_error
                        .as_deref()
                        .map(|e| {
                            let lower = e.to_ascii_lowercase();
                            lower.contains("backlogged")
                                || lower.contains("wouldblock")
                                || lower.contains("timeout")
                                || lower.contains("timed out")
                        })
                        .unwrap_or(false);

                    if status_transient_error || imsi_transient_error {
                        log::info!(
                            "SIM probe on {} classified as transient queue/timeout pressure (was {}, status={:?}, status_err={:?}, imsi_err={:?}); keeping modem active.",
                            modem.com_port,
                            iccid,
                            status,
                            status_error,
                            imsi_error
                        );
                    }

                    let suspect_error_only_absence =
                        !explicit_absent && !imsi_ok && !status_transient_error && !imsi_transient_error;

                    if suspect_error_only_absence {
                        // A single CPIN/IMSI error burst (often +CME 10/3) can
                        // happen while the modem is still alive; confirm once
                        // more before counting this as true SIM removal.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let confirm_status = tokio::time::timeout(Duration::from_secs(5), modem.get_sim_status())
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten();
                        let confirm_ready = confirm_status
                            .as_deref()
                            .map(|s| s.trim().eq_ignore_ascii_case("READY"))
                            .unwrap_or(false);
                        let confirm_iccid = tokio::time::timeout(Duration::from_secs(5), modem.get_sim_iccid())
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten();
                        let confirm_same_iccid = confirm_iccid.as_deref() == Some(iccid.as_str());
                        if confirm_ready || confirm_same_iccid {
                            log::info!(
                                "Ignoring provisional SIM-absent evidence on {} (was {}): confirm probe indicates modem still stable (ready={}, same_iccid={}); keeping modem active.",
                                modem.com_port,
                                iccid,
                                confirm_ready,
                                confirm_same_iccid
                            );
                            return ProbeOutcome::TransientIssue {
                                iccid,
                                com_port: modem.com_port.clone(),
                                hide_identity: true,
                            };
                        }
                    }

                    if explicit_absent {
                        log::info!(
                            "SIM absence evidence on {} (was {}, status={:?}, status_err={:?}, imsi_readable={}, imsi_err={:?}).",
                            modem.com_port, iccid, status, status_error, imsi_ok, imsi_error
                        );
                        return ProbeOutcome::ConfirmedAbsent {
                            iccid,
                            com_port: modem.com_port.clone(),
                            modem,
                        };
                    }

                    if !imsi_ok && !status_transient_error && !imsi_transient_error {
                        let recently_ready = mgr
                            .recent_ready_at
                            .read()
                            .await
                            .get(&modem.com_port)
                            .copied()
                            .map(|t| t.elapsed() < Duration::from_secs(180))
                            .unwrap_or(false);
                        if recently_ready {
                            log::warn!(
                                "Suppressing non-explicit SIM-absent sample on {} (was {}): port was READY within the last 180s; keeping active.",
                                modem.com_port,
                                iccid
                            );
                        } else {
                            log::warn!(
                                "Ignoring non-explicit SIM-absent sample on {} (was {}): explicit SIM REMOVED evidence required for demotion.",
                                modem.com_port,
                                iccid
                            );
                        }
                        return ProbeOutcome::TransientIssue {
                            iccid,
                            com_port: modem.com_port.clone(),
                            hide_identity: true,
                        };
                    }

                    log::warn!(
                        "SIM probe on {} is inconclusive/transient (was {}, status={:?}, status_err={:?}, imsi_readable={}, imsi_err={:?}); keeping modem active.",
                        modem.com_port,
                        iccid,
                        status,
                        status_error,
                        imsi_ok,
                        imsi_error
                    );
                    return ProbeOutcome::TransientIssue {
                        iccid,
                        com_port: modem.com_port.clone(),
                        hide_identity: true,
                    };
                }

                log::info!(
                    "SIM status on {} is READY (was {}); checking ICCID for stale cached state.",
                    modem.com_port, iccid
                );

                // The modem just proved it is READY, independent of whatever the
                // subsequent ICCID re-check below returns. `port_runtime` (what the
                // API/dashboard reads) must reflect that now: if it had drifted
                // out of Active for any reason, a flaky/timed-out ICCID re-check
                // below would otherwise leave it stuck reporting "SIM REMOVED"
                // forever even though this modem is demonstrably alive.
                if mgr.reaffirm_port_active(&modem.com_port, &iccid).await {
                    log::warn!(
                        "[recheck] Port {} hosts READY SIM {} but runtime state had drifted; restored to Active.",
                        modem.com_port, iccid
                    );
                    mgr.remove_unavailable_port(&modem.com_port).await;
                }
                mgr
                    .recent_ready_at
                    .write()
                    .await
                    .insert(modem.com_port.clone(), Instant::now());

                match tokio::time::timeout(Duration::from_secs(5), modem.get_sim_iccid()).await {
                    Ok(Ok(Some(current_iccid))) if current_iccid == iccid => {
                        ProbeOutcome::Healthy { iccid, modem }
                    }
                    Ok(Ok(Some(current_iccid))) => {
                        // Cross-port response bleed can briefly return another
                        // slot's ICCID under load. Confirm the new identity several
                        // times on this same port now instead of waiting for several
                        // fleet-wide recheck cycles.
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        let confirm = tokio::time::timeout(Duration::from_secs(5), modem.get_sim_iccid())
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten();
                        if confirm.as_deref() == Some(current_iccid.as_str()) {
                            let seen_on_other_port = mgr
                                .is_iccid_active_on_other_port(&current_iccid, &modem.com_port)
                                .await;
                            let required_matches: u8 = if seen_on_other_port { 4 } else { 3 };
                            let mut matches: u8 = 2;
                            while matches < required_matches {
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                let sample = tokio::time::timeout(
                                    Duration::from_secs(5),
                                    modem.get_sim_iccid(),
                                )
                                .await
                                .ok()
                                .and_then(|result| result.ok())
                                .flatten();
                                if sample.as_deref() != Some(current_iccid.as_str()) {
                                    break;
                                }
                                matches = matches.saturating_add(1);
                            }

                            if matches >= required_matches {
                                info!(
                                    "SIM swap confirmed on {} (was {}, now {}, samples={}, active_on_other_port={}). Forcing re-init.",
                                    modem.com_port,
                                    iccid,
                                    current_iccid,
                                    matches,
                                    seen_on_other_port
                                );
                                ProbeOutcome::Swap {
                                    iccid,
                                    com_port: modem.com_port.clone(),
                                    modem,
                                    new_iccid: current_iccid,
                                }
                            } else {
                                log::warn!(
                                    "Ignoring unstable ICCID mismatch on {} (was {}, candidate={}, matching_samples={}/{}); keeping modem active.",
                                    modem.com_port,
                                    iccid,
                                    current_iccid,
                                    matches,
                                    required_matches
                                );
                                ProbeOutcome::TransientIssue {
                                    iccid,
                                    com_port: modem.com_port.clone(),
                                    hide_identity: true,
                                }
                            }
                        } else {
                            log::warn!(
                                "Ignoring unconfirmed ICCID mismatch on {} (was {}, first_read={}, confirm={:?}); keeping modem active.",
                                modem.com_port,
                                iccid,
                                current_iccid,
                                confirm
                            );
                            ProbeOutcome::TransientIssue {
                                iccid,
                                com_port: modem.com_port.clone(),
                                hide_identity: true,
                            }
                        }
                    }
                    Ok(Ok(None)) => {
                        log::debug!(
                            "ICCID read returned empty on {} (was {}). Keeping modem active for now.",
                            modem.com_port, iccid
                        );
                        ProbeOutcome::TransientIssue {
                            iccid,
                            com_port: modem.com_port.clone(),
                            hide_identity: false,
                        }
                    }
                    Ok(Err(e)) => {
                        log::debug!(
                            "ICCID read failed on {} (was {}): {}. Keeping modem active for now.",
                            modem.com_port, iccid, e
                        );
                        ProbeOutcome::TransientIssue {
                            iccid,
                            com_port: modem.com_port.clone(),
                            hide_identity: false,
                        }
                    }
                    Err(_) => {
                        log::debug!(
                            "ICCID read timed out on {} (was {}). Keeping modem active for now.",
                            modem.com_port, iccid
                        );
                        ProbeOutcome::TransientIssue {
                            iccid,
                            com_port: modem.com_port.clone(),
                            hide_identity: false,
                        }
                    }
                }
            });
        }
        let mut demotions: Vec<(String, String, Arc<Modem>)> = Vec::new();
        log::info!(
            "Rechecking {} active modem(s) for SIM presence",
            active_entries_len
        );
        while let Some(result) = demotion_futs.next().await {
            match result {
                ProbeOutcome::Healthy { iccid, modem } => {
                    let port_key = modem.com_port.clone();
                    self.identity_revalidating_ports.write().await.remove(&port_key);
                    self.sim_probe_fail_counts.write().await.remove(&port_key);
                    self.sim_swap_confirm_streaks.write().await.remove(&port_key);
                    self.sim_probe_cooldown_until.write().await.remove(&port_key);

                    // `port_runtime` was already reaffirmed to Active right after the
                    // READY probe above (before this ICCID re-check ran), so nothing
                    // further to do here for dashboard state.

                    // Ensure the sim_cards DB row exists and has the current IMSI.
                    // A transient demotion may have deleted the row while the modem
                    // stayed active, which leaves the dashboard IMSI column blank.
                    // Only query AT+CNUM/AT+CIMI when the cached row is actually missing
                    // that field, so healthy modems aren't hit with extra AT commands
                    // (contending with foreground requests on the same serial port)
                    // every 5s recheck cycle when nothing has changed.
                    let cached_card = self.sim_cards_cache.read().await.get(&iccid).cloned();
                    let cached_imsi = cached_card
                        .as_ref()
                        .and_then(|c| c.imsi.clone())
                        .filter(|v| !v.is_empty());
                    let needs_phone = cached_card
                        .as_ref()
                        .and_then(|c| c.phone_number.as_deref())
                        .map(|p| p.is_empty())
                        .unwrap_or(true);
                    let needs_imsi = cached_imsi.is_none();

                    let imsi = if needs_imsi {
                        tokio::time::timeout(Duration::from_secs(5), modem.get_sim_imsi())
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten()
                    } else {
                        cached_imsi
                    };
                    let phone_number = if needs_phone {
                        tokio::time::timeout(Duration::from_secs(5), modem.get_phone_number())
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .flatten()
                            .map(|p| crate::phone_number::normalize_msisdn(&p))
                            .filter(|p| !p.is_empty())
                    } else {
                        None
                    };

                    if !needs_imsi && !needs_phone {
                        // Nothing missing in the cache/DB -- skip the write entirely.
                    } else if let Some(imsi) = imsi {
                        match SimCard::find_or_create_with_phone(&iccid, Some(imsi), phone_number)
                            .await
                        {
                            Ok(card) => {
                                self.sim_cards_cache
                                    .write()
                                    .await
                                    .insert(card.id.clone(), card);
                            }
                            Err(e) => {
                                log::warn!(
                                    "[recheck] Failed to ensure sim_cards row for {}: {}",
                                    iccid, e
                                );
                            }
                        }
                    }
                }
                ProbeOutcome::Swap {
                    iccid,
                    com_port,
                    modem,
                    new_iccid,
                } => {
                    self.identity_revalidating_ports
                        .write()
                        .await
                        .insert(com_port.clone());
                    self.sim_swap_confirm_streaks.write().await.remove(&com_port);
                    self.sim_probe_fail_counts.write().await.remove(&com_port);
                    log::info!(
                        "SIM swap on {} passed rapid same-port validation ({} -> {}); restarting immediately.",
                        com_port,
                        iccid,
                        new_iccid
                    );
                    demotions.push((iccid, com_port, modem));
                }
                ProbeOutcome::ConfirmedAbsent {
                    iccid,
                    com_port,
                    modem,
                } => {
                    self.identity_revalidating_ports
                        .write()
                        .await
                        .insert(com_port.clone());
                    self.sim_swap_confirm_streaks.write().await.remove(&com_port);
                    let in_cooldown = self
                        .sim_probe_cooldown_until
                        .read()
                        .await
                        .get(&com_port)
                        .copied()
                        .map(|until| until > Instant::now())
                        .unwrap_or(false);
                    if in_cooldown {
                        log::debug!(
                            "SIM {} on {} is in demotion cooldown; skipping this failure sample",
                            iccid, com_port
                        );
                        continue;
                    }

                    let failures = {
                        let mut fail_counts = self.sim_probe_fail_counts.write().await;
                        let count = fail_counts.entry(com_port.clone()).or_insert(0);
                        *count = count.saturating_add(1);
                        *count
                    };

                    let required_failures = SIM_PROBE_FAILURE_THRESHOLD;

                    if failures >= required_failures {
                        info!(
                            "SIM probe explicitly failed {} times on {} (was {}). Marking port as unavailable.",
                            failures, com_port, iccid
                        );
                        self.sim_probe_fail_counts.write().await.remove(&com_port);
                        self
                            .sim_probe_cooldown_until
                            .write()
                            .await
                            .insert(
                                com_port.clone(),
                                Instant::now() + Duration::from_secs(SIM_DEMOTION_COOLDOWN_SECS),
                            );
                        demotions.push((iccid, com_port, modem));
                    } else {
                        log::debug!(
                            "Transient SIM probe failure {}/{} on {} (SIM {}). Keeping modem active.",
                            failures,
                            required_failures,
                            com_port,
                            iccid
                        );
                    }
                }
                ProbeOutcome::TransientIssue {
                    iccid,
                    com_port,
                    hide_identity,
                } => {
                    if hide_identity {
                        self.identity_revalidating_ports
                            .write()
                            .await
                            .insert(com_port.clone());
                    }
                    log::debug!(
                        "Transient probe issue on {} (SIM {}, identity_hidden={}); leaving modem active.",
                        com_port,
                        iccid,
                        hide_identity
                    );
                }
            }
        }
        if demotions.is_empty() {
            log::debug!("Recheck demotion phase found 0 modem(s) to demote");
        } else {
            log::info!(
                "Recheck demotion phase found {} modem(s) to demote",
                demotions.len()
            );
        }
        for (iccid, com_port, probed_modem) in demotions {
            log::info!("[recheck] Demoting modem {} on {}", iccid, com_port);
            let baud_rate = probed_modem.baud_rate;

            // This port is no longer hosting a valid SIM session; drop stale
            // fallback mapping immediately so unavailable rows won't mirror a
            // SIM that has moved to another COM port.
            self.last_known_sim_ids_by_port.write().await.remove(&com_port);

            // Remove the map entry only if it is still the exact modem we probed.
            // A concurrent reconnect worker may already have replaced it with the
            // same SIM on a different port; removing that fresh entry would make
            // the SIM vanish from the dashboard until the next restart.
            let removed = {
                let mut modems = self.modems.write().await;
                if modems
                    .get(&com_port)
                    .map(|current| Arc::ptr_eq(current, &probed_modem))
                    .unwrap_or(false)
                {
                    modems.remove(&com_port);
                    true
                } else {
                    false
                }
            };

            if removed {
                if let Some(task) = self.urc_tasks.write().await.remove(&com_port) {
                    task.abort();
                }
                self.mark_port_unavailable(&com_port, Some(&iccid)).await;
            } else {
                log::info!(
                    "[recheck] Skipped demotion of {} on {}: active entry was already replaced (SIM re-detected on another port).",
                    iccid, com_port
                );
            }

            // Release the serial port explicitly. The reader task auto-reconnects on
            // I/O errors, so any lingering Arc clone would keep re-opening this COM
            // port and permanently block the reconnect worker from re-initializing it.
            if removed {
                probed_modem.shutdown().await;
            }

            // Whether the entry was removed or replaced, the probed modem on this
            // port is gone, so make sure the port is watched for reconnection.
            let device_cfg = self.devices.iter().find(|d| d.com_port == com_port);
            let sms_storage = device_cfg
                .and_then(|d| d.sms_storage)
                .or(self.default_sms_storage);
            let index = device_cfg
                .map(|d| self.devices.iter().position(|x| x.com_port == d.com_port).unwrap_or(0))
                .unwrap_or_else(|| {
                    com_port
                        .trim_start_matches(|c: char| !c.is_ascii_digit())
                        .parse::<usize>()
                        .unwrap_or(0)
                        .saturating_sub(1)
                });
            self.clone().add_unavailable_port(
                com_port.clone(),
                baud_rate,
                format!("device_{}", index),
                sms_storage,
                index,
                sse_manager.clone(),
                transcribe_cfg.clone(),
            )
            .await;
        }

        // ── Invariant sweep: every configured port is active or watched ───────
        // A port can fall out of both sets — e.g. its SIM was promoted away on a
        // different port before this recheck's snapshot was taken, so no demotion
        // was ever queued for it. Such a port would vanish from the dashboard
        // entirely, so start watching it again here.
        // Only run after startup init: during discovery, ports legitimately sit
        // in neither set while they are being probed/initialized.
        if *self.initialization_complete.read().await {
            let active_ports: std::collections::HashSet<String> = self
                .modems
                .read()
                .await
                .values()
                .map(|m| m.com_port.clone())
                .collect();
            for (index, device) in self.devices.iter().enumerate() {
                if active_ports.contains(&device.com_port) {
                    continue;
                }
                if self
                    .unavailable_ports
                    .read()
                    .await
                    .iter()
                    .any(|(p, _)| p == &device.com_port)
                {
                    continue;
                }
                log::info!(
                    "[recheck] Port {} is neither active nor watched; marking it unavailable.",
                    device.com_port
                );
                self.clone()
                    .add_unavailable_port(
                        device.com_port.clone(),
                        device.baud_rate,
                        format!("device_{}", index),
                        device.sms_storage.or(self.default_sms_storage),
                        index,
                        sse_manager.clone(),
                        transcribe_cfg.clone(),
                    )
                    .await;
            }
        }

        // ── Supervisor: keep unavailable-port workers alive and consistent ──────
        // Each unavailable port should have its own worker. If a worker has been
        // aborted or crashed, respawn it. If a worker exists for a port that is no
        // longer unavailable, stop it.
        let unavailable_snapshot: Vec<(String, u32)> = self.unavailable_ports.read().await.clone();
        let mut expected_workers: HashMap<String, (u32, usize, Option<SmsStorage>)> =
            HashMap::new();
        for (com_port, baud_rate) in &unavailable_snapshot {
            let device_cfg = self.devices.iter().find(|d| d.com_port == *com_port);
            let sms_storage = device_cfg
                .and_then(|d| d.sms_storage)
                .or(self.default_sms_storage);
            let index = device_cfg
                .map(|d| self.devices.iter().position(|x| x.com_port == d.com_port).unwrap_or(0))
                .unwrap_or_else(|| {
                    com_port
                        .trim_start_matches(|c: char| !c.is_ascii_digit())
                        .parse::<usize>()
                        .unwrap_or(0)
                        .saturating_sub(1)
                });
            expected_workers.insert(com_port.clone(), (*baud_rate, index, sms_storage));
        }

        // Stop workers for ports that are no longer unavailable.
        let stale_ports: Vec<String> = {
            let tasks = self.unavailable_port_tasks.read().await;
            tasks
                .keys()
                .filter(|p| !expected_workers.contains_key(p.as_str()))
                .cloned()
                .collect()
        };
        if !stale_ports.is_empty() {
            let mut tasks = self.unavailable_port_tasks.write().await;
            for port in stale_ports {
                if let Some(handle) = tasks.remove(&port) {
                    handle.abort();
                }
            }
        }

        // Respawn workers for unavailable ports that don't have one.
        let missing_ports: Vec<(String, u32, usize, Option<SmsStorage>)> = {
            let tasks = self.unavailable_port_tasks.read().await;
            expected_workers
                .iter()
                .filter(|(p, _)| !tasks.contains_key(p.as_str()))
                .map(|(p, (baud, index, storage))| (p.clone(), *baud, *index, *storage))
                .collect()
        };
        for (com_port, baud_rate, index, sms_storage) in missing_ports {
            self.clone().add_unavailable_port(
                com_port,
                baud_rate,
                format!("device_{}", index),
                sms_storage,
                index,
                sse_manager.clone(),
                transcribe_cfg.clone(),
            )
            .await;
        }
    }

    pub async fn get_signal_quality(&self, sim_id: &str) -> anyhow::Result<Option<SignalQuality>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_signal_quality().await.map_err(Into::into)
    }

    pub async fn check_network_registration(
        &self,
        sim_id: &str,
    ) -> anyhow::Result<Option<NetworkRegistrationStatus>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.check_network_registration().await.map_err(Into::into)
    }

    pub async fn check_operator(&self, sim_id: &str) -> anyhow::Result<Option<OperatorInfo>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.check_operator().await.map_err(Into::into)
    }

    pub async fn get_modem_model(&self, sim_id: &str) -> anyhow::Result<Option<ModemInfo>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_modem_model().await.map_err(Into::into)
    }

    pub async fn get_imei(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_imei().await.map_err(Into::into)
    }

    pub async fn get_sms_center(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_sms_center().await.map_err(Into::into)
    }

    pub async fn get_network_info(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_network_info().await.map_err(Into::into)
    }

    pub async fn get_sim_status(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_sim_status().await.map_err(Into::into)
    }

    pub async fn get_memory_status(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_memory_status().await.map_err(Into::into)
    }

    pub async fn get_temperature_info(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_temperature_info().await.map_err(Into::into)
    }

    pub async fn get_modem_by_com_port(&self, com_port: &str) -> Option<Arc<Modem>> {
        self.modems.read().await.get(com_port).cloned()
    }

    /// Snapshot of all currently-tracked modems (used by the eSIM service to
    /// enumerate ports and probe eSIM capability).
    pub async fn all_modems(&self) -> Vec<Arc<Modem>> {
        self.modems.read().await.values().cloned().collect()
    }

    pub async fn send_at_command(&self, com_port: &str, command: &str) -> anyhow::Result<String> {
        let modem = self
            .get_modem_by_com_port(com_port)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found on port: {}", com_port))?;
        modem.send_command(command).await.map_err(Into::into)
    }

    /// Force network registration (Quectel scancontrol/cops sequence) on a SIM.
    pub async fn force_register(&self, sim_id: &str) -> Result<(), String> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| format!("Modem not found for SIM ID: {}", sim_id))?;
        modem.force_register().await
    }

    /// Re-register a SIM using the 46000 COPS command only.
    pub async fn re_register(&self, sim_id: &str) -> Result<(), String> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| format!("Modem not found for SIM ID: {}", sim_id))?;
        modem.register_cops_46000().await
    }

    pub async fn set_sms_storage(
        &self,
        sim_id: &str,
        sms_storage: SmsStorage,
    ) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.set_sms_storage(sms_storage).await.map_err(Into::into)
    }

    pub async fn get_sms_storage_status(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.get_sms_storage_status().await.map_err(Into::into)
    }

    /// Write the user's own phone number into the SIM card and update the DB record.
    pub async fn set_sim_phone_number(
        &self,
        sim_id: &str,
        phone_number: &str,
    ) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        log::info!("[SIM:{}] Writing phone number {} to SIM own-number phonebook", sim_id, phone_number);
        modem.write_phone_number(phone_number).await.map_err(anyhow::Error::from)?;

        // Update DB and in-memory cache so the dashboard shows the new number immediately.
        match SimCard::find_by_conditions(Some(sim_id), None, None, None).await {
            Ok(cards) => {
                if let Some(mut card) = cards.into_iter().next() {
                    if let Err(e) = card.update_phone_number(Some(phone_number.to_string())).await {
                        log::warn!("[SIM:{}] Wrote phone number to SIM but failed to update local DB: {}", sim_id, e);
                    } else {
                        self.update_sim_cache(card).await;
                        log::info!("[SIM:{}] Phone number {} saved to SIM and local DB/cache updated", sim_id, phone_number);
                    }
                } else {
                    log::warn!("[SIM:{}] Wrote phone number to SIM but no local DB record found to update", sim_id);
                }
            }
            Err(e) => {
                log::warn!("[SIM:{}] Wrote phone number to SIM but failed to query local DB: {}", sim_id, e);
            }
        }

        Ok(())
    }

    pub async fn send_ussd(&self, sim_id: &str, code: &str) -> anyhow::Result<String> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.send_ussd(code).await.map_err(Into::into)
    }

    pub async fn read_clcc_caller_number(&self, sim_id: &str) -> anyhow::Result<Option<String>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.read_clcc_caller_number().await.map_err(Into::into)
    }

    pub async fn answer_call_simple(&self, sim_id: &str) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.answer_call().await.map_err(Into::into)
    }

    pub async fn hangup_call_simple(&self, sim_id: &str) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        modem.hangup_call().await.map_err(Into::into)
    }

    pub async fn get_sim_card_cached(&self, sim_id: &str) -> Option<SimCard> {
        self.sim_cards_cache.read().await.get(sim_id).cloned()
    }

    pub async fn get_last_known_sim_card_for_port(&self, com_port: &str) -> Option<SimCard> {
        let sim_id = self.last_known_sim_ids_by_port.read().await.get(com_port).cloned()?;
        self.get_sim_card_cached(&sim_id).await
    }

    pub async fn update_sim_cache(&self, sim_card: SimCard) {
        let mut cache = self.sim_cards_cache.write().await;
        cache.insert(sim_card.id.clone(), sim_card);
    }

    /// Refresh the in-memory SIM cache for the given SIM IDs from the database.
    pub async fn refresh_sim_cache(&self, sim_ids: &[String]) {
        if sim_ids.is_empty() {
            return;
        }
        let refs: Vec<&str> = sim_ids.iter().map(|s| s.as_str()).collect();
        match SimCard::get_by_ids(&refs).await {
            Ok(cards) => {
                let mut cache = self.sim_cards_cache.write().await;
                for (sim_id, card) in cards {
                    cache.insert(sim_id, card);
                }
            }
            Err(e) => {
                log::warn!("[refresh_sim_cache] Failed to refresh SIM cache: {}", e);
            }
        }
    }

    /// Find the sim_id (ICCID) that matches a given SIM phone number.
    /// Searches the in-memory cache, so the SIM must have been seen at least once.
    /// Normalizes both numbers before comparing (removes leading '+' and any
    /// non-digits, then tries both with and without a leading 0).
    pub async fn find_sim_id_by_phone_number(&self, phone_number: &str) -> Option<String> {
        fn normalize(p: &str) -> Vec<String> {
            let digits: String = p.chars().filter(|c| c.is_ascii_digit()).collect();
            let mut variants = vec![];
            // Short form without country code / leading 0
            if !digits.is_empty() {
                variants.push(digits.clone());
            }
            // With leading 0
            if !digits.starts_with('0') {
                variants.push(format!("0{}", digits));
            }
            // Without leading 0
            if digits.starts_with('0') {
                variants.push(digits.trim_start_matches('0').to_string());
            }
            variants
        }

        let target_variants = normalize(phone_number);
        let cache = self.sim_cards_cache.read().await;
        cache.iter().find_map(|(sim_id, sim_card)| {
            sim_card.phone_number.as_deref().and_then(|p| {
                let stored_variants = normalize(p);
                if target_variants.iter().any(|t| stored_variants.contains(t)) {
                    Some(sim_id.clone())
                } else {
                    None
                }
            })
        })
    }
    // ─── Voice call delegation ────────────────────────────────────────────────

    /// Initiate an outbound call and record it in the DB.
    /// Returns the new Call UUID on success.
    pub async fn make_call(&self, sim_id: &str, phone: &str, sse: Arc<SseManager>) -> anyhow::Result<String> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        // Cancel any existing outbound poller and finalize the previous call as missed.
        // This prevents the old poller's poll_cancel_rx from firing when we replace
        // outbound_poll_cancel_tx, which would leave the previous call stuck as 'ringing'.
        if let Some(tx) = modem.outbound_poll_cancel_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(old_id) = modem.outbound_call_id.lock().await.take() {
            if let Err(e) = Call::update_status(&old_id, "missed").await {
                error!("[{}] failed to finalize previous outbound call {}: {}", sim_id, old_id, e);
            }
            sse.send_call_event(CallEvent {
                event_type: "call_ended".into(),
                sim_id: sim_id.to_string(),
                call_id: old_id,
                phone: None,
                direction: "outbound".into(),
            });
        }

        // Send the ATD command first — only record if the modem accepts it.
        modem.make_call(phone).await?;
        // Insert a DB record now that the modem confirmed OK.
        let call_id = Call::insert(sim_id, Some(phone), "outbound").await?;
        // Reset per-call state.
        *modem.outbound_call_answered.lock().await = false;
        *modem.outbound_call_id.lock().await = Some(call_id.clone());
        info!("[{}] outbound call {} to {} started", sim_id, call_id, phone);

        // Spawn AT+CLCC polling task — sole mechanism for detecting outbound call state.
        // EC20F does not reliably emit NO CARRIER for rejected calls; NO CARRIER is
        // also unreliable when inbound calls are active (it gets consumed by the inbound handler).
        let call_id_c = call_id.clone();
        let phone_c   = phone.to_string();
        let modem_c   = modem.clone();
        let sim_id_c  = sim_id.to_string();
        let sse_c     = sse;
        let (poll_cancel_tx, mut poll_cancel_rx) = tokio::sync::oneshot::channel::<()>();
        *modem.outbound_poll_cancel_tx.lock().await = Some(poll_cancel_tx);

        tokio::spawn(async move {
            let mut call_answered     = false;
            let mut consecutive_empty: u8 = 0;
            info!("[CLCC {}] polling started for call {}", sim_id_c, call_id_c);
            loop {
                tokio::select! {
                    result = &mut poll_cancel_rx => {
                        // Cancelled explicitly by hangup_call() — finalize properly.
                        // Ignore if sender was dropped (overlapping call scenario already handled).
                        if result.is_ok() {
                            info!("[CLCC {}] polling cancelled (user hung up)", sim_id_c);
                        }
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                match modem_c.query_clcc().await {
                    Ok(ref resp) => {
                        if let Some(stat) = Self::parse_clcc_mo_stat(resp) {
                            // MO call still in CLCC — reset empty counter.
                            consecutive_empty = 0;
                            match stat {
                                0 if !call_answered => {
                                    call_answered = true;
                                    *modem_c.outbound_call_answered.lock().await = true;
                                    if let Err(e) = Call::update_status(&call_id_c, "active").await {
                                        error!("[CLCC {}] failed to set active: {}", sim_id_c, e);
                                    }
                                    sse_c.send_call_event(CallEvent {
                                        event_type: "call_answered".into(),
                                        sim_id:     sim_id_c.clone(),
                                        call_id:    call_id_c.clone(),
                                        phone:      Some(phone_c.clone()),
                                        direction:  "outbound".into(),
                                    });
                                    info!("[CLCC {}] call {} answered by remote", sim_id_c, call_id_c);
                                }
                                3 => info!("[CLCC {}] remote ringing", sim_id_c),
                                2 => info!("[CLCC {}] dialing", sim_id_c),
                                _ => {}
                            }
                        } else {
                            // No MO (dir=0) entry — outbound call is gone.
                            consecutive_empty += 1;
                            info!("[CLCC {}] no MO call in CLCC ({}/2)", sim_id_c, consecutive_empty);
                            if consecutive_empty >= 2 {
                                let status = if call_answered { "ended" } else { "missed" };
                                // Always update using call_id_c (captured) — never the mutex,
                                // which may have been replaced by a newer call.
                                if let Err(e) = Call::update_status(&call_id_c, status).await {
                                    error!("[CLCC {}] failed to finalize call {}: {}", sim_id_c, call_id_c, e);
                                }
                                // Remove from mutex only if it still holds our call_id.
                                let mut lock = modem_c.outbound_call_id.lock().await;
                                if lock.as_deref() == Some(call_id_c.as_str()) {
                                    *lock = None;
                                }
                                drop(lock);
                                sse_c.send_call_event(CallEvent {
                                    event_type: "call_ended".into(),
                                    sim_id:     sim_id_c.clone(),
                                    call_id:    call_id_c.clone(),
                                    phone:      Some(phone_c.clone()),
                                    direction:  "outbound".into(),
                                });
                                info!("[CLCC {}] call_ended SSE sent (status={})", sim_id_c, status);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("[CLCC {}] AT+CLCC failed: {}", sim_id_c, e);
                    }
                }
            }
            info!("[CLCC {}] polling ended", sim_id_c);
        });

        Ok(call_id)
    }

    /// Parse the stat field from the first +CLCC line with dir=0 (MO = mobile-originated).
    /// stat: 0=active, 1=held, 2=dialing(MO), 3=alerting/ringing(MO), 4=incoming(MT), 5=waiting.
    /// Returns None if no MO call is present (CLCC empty or only MT entries).
    fn parse_clcc_mo_stat(response: &str) -> Option<u8> {
        for line in response.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("+CLCC:") {
                // Format: <idx>,<dir>,<stat>,<mode>,<mpty>[,"<num>",<type>]
                let parts: Vec<&str> = rest.split(',').collect();
                if parts.len() >= 3 {
                    let dir: u8 = parts[1].trim().parse().unwrap_or(255);
                    if dir == 0 {  // 0 = MO (mobile originated)
                        if let Ok(stat) = parts[2].trim().parse::<u8>() {
                            return Some(stat);
                        }
                    }
                }
            }
        }
        None
    }

    /// Answer the current inbound call. Mirrors the URC handler's auto-answer
    /// side effects (DB status, SSE event, recording) and coordinates with it
    /// via shared `Modem` state so the two paths never both send ATA.
    pub async fn answer_call(&self, sim_id: &str, sse: Arc<SseManager>) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;

        // Auto-answer may have already answered this call (e.g. the button was
        // pressed after the auto-answer delay elapsed); sending ATA again would
        // just return ERROR, so treat it as an idempotent success.
        if modem.inbound_call_answered.load(Ordering::SeqCst) {
            return Ok(());
        }

        if let Err(e) = modem.answer_call().await {
            // Lost the race with the URC handler's auto-answer — if it already
            // marked the call answered, this is a harmless no-op, not a failure.
            if modem.inbound_call_answered.load(Ordering::SeqCst) {
                return Ok(());
            }
            return Err(e.into());
        }

        // Prevent the URC handler's pending auto-answer timer from sending a
        // second ATA now that the call has been answered manually.
        if let Some(tx) = modem.inbound_auto_answer_cancel_tx.lock().await.take() {
            let _ = tx.send(());
        }
        modem.inbound_call_answered.store(true, Ordering::SeqCst);

        let call_id = modem.inbound_call_id.lock().await.clone();
        if let Some(call_id) = call_id {
            if let Err(e) = Call::update_status(&call_id, "active").await {
                error!("[{}] failed to set call {} active: {}", sim_id, call_id, e);
            }
            sse.send_call_event(CallEvent {
                event_type: "call_answered".into(),
                sim_id: sim_id.to_string(),
                call_id,
                phone: None,
                direction: "inbound".into(),
            });
            // ── Start downlink recording (mirrors auto-answer) ──────────────
            if let Err(e) = modem.delete_files().await {
                error!("[{}] failed to clear modem UFS: {}", sim_id, e);
            }
            match modem.start_recording("a.amr").await {
                Ok(()) => info!("[{}] recording started -> a.amr (manual answer)", sim_id),
                Err(e) => error!("[{}] failed to start recording: {}", sim_id, e),
            }
            let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
            *modem.inbound_recording_cancel_tx.lock().await = Some(cancel_tx);
            let modem_c = modem.clone();
            let sim_id_c = sim_id.to_string();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        info!("[{}] 30s recording limit, stopping and hanging up", sim_id_c);
                        modem_c.stop_recording().await.ok();
                        modem_c.hangup_call().await.ok();
                        modem_c.inject_urc("NO CARRIER");
                    }
                    _ = cancel_rx => {
                        info!("[{}] recording timer cancelled", sim_id_c);
                    }
                }
            });
        }

        Ok(())
    }

    /// Hang up the current call.  Returns the call_id and final status of the
    /// outbound call if one was in progress, so the caller can send an SSE event.
    pub async fn hangup_call(&self, sim_id: &str) -> anyhow::Result<Option<(String, String)>> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;
        modem.hangup_call().await?;
        // Cancel the CLCC polling task so it does not race with our finalization below.
        if let Some(tx) = modem.outbound_poll_cancel_tx.lock().await.take() {
            let _ = tx.send(());
        }
        // Take any pending outbound call and finalize it.
        if let Some(call_id) = modem.outbound_call_id.lock().await.take() {
            let answered = *modem.outbound_call_answered.lock().await;
            let status = if answered { "ended" } else { "missed" };
            if let Err(e) = Call::update_status(&call_id, status).await {
                error!("[{}] failed to update outbound call {} on hangup: {}", sim_id, call_id, e);
            } else {
                info!("[{}] outbound call {} marked {} on hangup", sim_id, call_id, status);
            }
            return Ok(Some((call_id, status.to_string())));
        }
        Ok(None)
    }

    // ─── URC handler tasks ────────────────────────────────────────────────────

    /// Spawn one URC handler task per modem. Must be called once after
    /// initialization.  Each task owns the modem's `urc_rx` and processes
    /// RING / +CLIP: / NO CARRIER / VOICE CALL: END messages forever.
    pub async fn start_urc_handlers(&self,
        sse_manager: Arc<SseManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) {
        let entries: Vec<(String, String, Arc<Modem>)> = {
            let modems = self.modems.read().await;
            let mut v = Vec::with_capacity(modems.len());
            for modem in modems.values() {
                if let Some(sid) = modem.sim_id.read().await.clone() {
                    v.push((modem.com_port.clone(), sid, modem.clone()));
                }
            }
            v
        };
        if entries.is_empty() {
            log::info!("No active modems; skipping URC handler startup");
            return;
        }
        for (com_port, sim_id, modem) in entries {
            let sse = sse_manager.clone();
            let cfg = transcribe_cfg.clone();
            let sim_id_for_task = sim_id.clone();
            let handle = tokio::spawn(async move {
                Self::run_urc_handler(sim_id_for_task, modem, sse, cfg).await;
            });
            if let Some(old) = self.urc_tasks.write().await.insert(com_port, handle) {
                old.abort();
            }
        }
    }

    async fn run_urc_handler(sim_id: String, modem: Arc<Modem>, sse: Arc<SseManager>, transcribe_cfg: Option<Arc<TranscribeConfig>>) {
        // Hold the receiver for the task's lifetime — one consumer per modem.
        let mut rx = modem.urc_rx.lock().await;
        // active_call_id tracks the current ringing/active call for this modem.
        let mut active_call_id: Option<String> = None;
        // Phone number arrives via +CLIP: *after* the first RING on EC20 modems.
        let mut current_phone: Option<String> = None;
        // Whether ATA was sent (answered); used to distinguish missed vs ended.
        let mut call_answered = false;

        info!("[URC {}] handler started", sim_id);

        loop {
            let line = match rx.recv().await {
                Some(l) => l,
                None => {
                    info!("[URC {}] channel closed, handler exiting", sim_id);
                    break;
                }
            };

            let line = line.trim().to_string();
            if line.is_empty() {
                continue;
            }

            if line.starts_with("+CLIP:") {
                if let Some(phone) = Modem::parse_clip(&line) {
                    // If a call record already exists (RING arrived before +CLIP:), update its phone.
                    if let Some(ref id) = active_call_id {
                        if let Err(e) = Call::update_phone(id, &phone).await {
                            error!("[URC {}] failed to update phone on call {}: {}", sim_id, id, e);
                        }
                        // Also push an updated SSE event so the frontend shows the number.
                        sse.send_call_event(CallEvent {
                            event_type: "incoming_call".into(),
                            sim_id: sim_id.clone(),
                            call_id: id.clone(),
                            phone: Some(phone.clone()),
                            direction: "inbound".into(),
                        });
                    }
                    current_phone = Some(phone);
                }
                continue;
            }

            if line == "RING" {
                if active_call_id.is_none() {
                    // First RING — insert new inbound call record.
                    // phone may be None here (EC20 sends RING before +CLIP:); we'll patch it when +CLIP: arrives.
                    match Call::insert(&sim_id, current_phone.as_deref(), "inbound").await {
                        Ok(id) => {
                            info!("[URC {}] incoming call {} from {:?}", sim_id, id, current_phone);
                            sse.send_call_event(CallEvent {
                                event_type: "incoming_call".into(),
                                sim_id: sim_id.clone(),
                                call_id: id.clone(),
                                phone: current_phone.clone(),
                                direction: "inbound".into(),
                            });
                            active_call_id = Some(id.clone());
                            call_answered = false;
                            *modem.inbound_call_id.lock().await = Some(id.clone());
                            modem.inbound_call_answered.store(false, Ordering::SeqCst);
                            // Check if auto-answer is enabled
                            let should_auto_answer = transcribe_cfg
                                .as_ref()
                                .map(|cfg| cfg.auto_answer_enabled)
                                .unwrap_or(true);
                            if should_auto_answer {
                                // Wait before auto-answering, but let a manual `/calls/answer`
                                // request cancel this wait so we never send a second ATA.
                                let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
                                *modem.inbound_auto_answer_cancel_tx.lock().await = Some(cancel_tx);
                                let delay_secs = transcribe_cfg
                                    .as_ref()
                                    .map(|cfg| cfg.auto_answer_delay_secs)
                                    .unwrap_or(0);
                                let mut answered_manually = false;
                                if delay_secs > 0 {
                                    info!(
                                        "[URC {}] waiting {}s before auto-answer",
                                        sim_id, delay_secs
                                    );
                                    tokio::select! {
                                        _ = tokio::time::sleep(Duration::from_secs(delay_secs)) => {}
                                        _ = &mut cancel_rx => { answered_manually = true; }
                                    }
                                } else if cancel_rx.try_recv().is_ok() {
                                    answered_manually = true;
                                }
                                *modem.inbound_auto_answer_cancel_tx.lock().await = None;

                                if answered_manually || modem.inbound_call_answered.load(Ordering::SeqCst) {
                                    info!("[URC {}] auto-answer skipped; call already answered manually", sim_id);
                                    call_answered = true;
                                } else {
                                // Auto-answer the incoming call.
                                // EC20 voice calls respond to ATA with OK only — no CONNECT URC.
                                // So we handle the answered state here rather than in the CONNECT branch.
                                match modem.answer_call().await {
                                Err(e) => error!("[URC {}] auto-answer failed: {}", sim_id, e),
                                Ok(()) => {
                                    info!("[URC {}] auto-answered incoming call", sim_id);
                                    call_answered = true;
                                    modem.inbound_call_answered.store(true, Ordering::SeqCst);
                                    if let Err(e) = Call::update_status(&id, "active").await {
                                        error!("[URC {}] failed to set call {} active: {}", sim_id, id, e);
                                    }
                                    sse.send_call_event(CallEvent {
                                        event_type: "call_answered".into(),
                                        sim_id: sim_id.clone(),
                                        call_id: id.clone(),
                                        phone: current_phone.clone(),
                                        direction: "inbound".into(),
                                    });
                                    // ── Start downlink recording ──────────────────────────
                                    if let Err(e) = modem.delete_files().await {
                                        error!("[URC {}] failed to clear modem UFS: {}", sim_id, e);
                                    }
                                    match modem.start_recording("a.amr").await {
                                        Ok(()) => info!("[URC {}] recording started -> a.amr", sim_id),
                                        Err(e) => error!("[URC {}] failed to start recording: {}", sim_id, e),
                                    }
                                    info!("[URC {}] call_answered={} after start_recording", sim_id, call_answered);
                                    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
                                    *modem.inbound_recording_cancel_tx.lock().await = Some(cancel_tx);
                                    let modem_c = modem.clone();
                                    let sim_id_c = sim_id.clone();
                                    tokio::spawn(async move {
                                        tokio::select! {
                                            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                                                info!("[URC {}] 30s recording limit, stopping and hanging up", sim_id_c);
                                                modem_c.stop_recording().await.ok();
                                                modem_c.hangup_call().await.ok();
                                                // EC20F may not emit NO CARRIER after ATH — inject
                                                // a synthetic one so the URC handler always cleans up.
                                                modem_c.inject_urc("NO CARRIER");
                                            }
                                            _ = cancel_rx => {
                                                info!("[URC {}] recording timer cancelled", sim_id_c);
                                            }
                                        }
                                    });
                                    // ─────────────────────────────────────────────────────
                                }
                                }
                                }
                            } else {
                                info!("[URC {}] auto-answer disabled, call waiting for manual answer", sim_id);
                            }
                        }
                        Err(e) => error!("[URC {}] failed to insert call: {}", sim_id, e),
                    }
                }
                // Subsequent RINGs for the same call are ignored (DB record already exists).
                continue;
            }

            // CONNECT URC — outbound call answered by remote.
            // (Inbound auto-answer is handled directly in the RING branch above.)
            if line == "ATA" || line == "CONNECT" {
                if let Some(outbound_id) = modem.outbound_call_id.lock().await.clone() {
                    call_answered = true;
                    if let Err(e) = Call::update_status(&outbound_id, "active").await {
                        error!("[URC {}] failed to set outbound call {} active: {}", sim_id, outbound_id, e);
                    }
                    sse.send_call_event(CallEvent {
                        event_type: "call_answered".into(),
                        sim_id: sim_id.clone(),
                        call_id: outbound_id,
                        phone: current_phone.clone(),
                        direction: "outbound".into(),
                    });
                }
                continue;
            }

            if line == "NO CARRIER" || line == "BUSY" || line.starts_with("VOICE CALL: END") || line.starts_with("+CEND:") {
                let has_recording = modem.inbound_recording_cancel_tx.lock().await.is_some();
                info!("[URC {}] {} received: call_answered={}, has_active={}, has_recording={}",
                    sim_id, line, call_answered, active_call_id.is_some(), has_recording);
                if let Some(call_id) = active_call_id.take() {
                    *modem.inbound_call_id.lock().await = None;
                    modem.inbound_call_answered.store(false, Ordering::SeqCst);
                    if let Some(tx) = modem.inbound_auto_answer_cancel_tx.lock().await.take() {
                        let _ = tx.send(());
                    }
                    // ── Stop recording if active ──────────────────────────────
                    if let Some(tx) = modem.inbound_recording_cancel_tx.lock().await.take() {
                        modem.stop_recording().await.ok();
                        tx.send(()).ok(); // cancel the 30s timer task
                        info!("[URC {}] recording stopped", sim_id);
                        // Download the recording from modem UFS and save to DB
                        match modem.download_file("a.amr").await {
                            Ok(data) => {
                                info!("[URC {}] recording downloaded: {} bytes", sim_id, data.len());
                                if let Err(e) = Call::save_recording(&call_id, &data).await {
                                    error!("[URC {}] failed to save recording to DB: {}", sim_id, e);
                                } else {
                                    info!("[URC {}] recording saved to DB", sim_id);
                                    // ── Spawn transcription (fire-and-forget) ──────────
                                    if let Some(cfg) = transcribe_cfg.as_ref() {
                                        let data_c = data.clone();
                                        let call_id_c = call_id.clone();
                                        let sim_id_c = sim_id.clone();
                                        let cfg_c = cfg.clone();
                                        tokio::spawn(async move {
                                            info!("[URC {}] transcribing recording for call {}", sim_id_c, call_id_c);
                                            let t = std::time::Instant::now();
                                            match crate::transcribe::transcribe(
                                                &data_c,
                                                &cfg_c.ffmpeg_exe,
                                                &cfg_c.whisper_exe,
                                                &cfg_c.whisper_model,
                                                &cfg_c.whisper_language,
                                            ).await {
                                                Ok(text) => {
                                                    info!("[URC {}] transcript ({:.1}s): {}", sim_id_c, t.elapsed().as_secs_f64(), text);
                                                    if let Err(e) = Call::save_transcript(&call_id_c, &text).await {
                                                        error!("[URC {}] failed to save transcript: {}", sim_id_c, e);
                                                    }
                                                }
                                                Err(e) => error!("[URC {}] transcription failed ({:.1}s): {}", sim_id_c, t.elapsed().as_secs_f64(), e),
                                            }
                                        });
                                    }
                                    // ───────────────────────────────────────────────────
                                }
                            }
                            Err(e) => error!("[URC {}] failed to download recording: {}", sim_id, e),
                        }
                    }
                    // ─────────────────────────────────────────────────────────
                    // Missed = inbound call that was never answered; Busy = remote busy
                    let new_status = if line == "BUSY" {
                        "missed"
                    } else if !call_answered {
                        "missed"
                    } else {
                        "ended"
                    };
                    call_answered = false;
                    if let Err(e) = Call::update_status(&call_id, new_status).await {
                        error!("[URC {}] failed to update call {}: {}", sim_id, call_id, e);
                    }
                    sse.send_call_event(CallEvent {
                        event_type: "call_ended".into(),
                        sim_id: sim_id.clone(),
                        call_id,
                        phone: current_phone.take(),
                        direction: "inbound".into(),
                    });
                } else {
                    // NO CARRIER with no tracked inbound call.
                    // Outbound call lifecycle is handled entirely by the CLCC poller —
                    // do not interfere here. Just reset local state.
                    call_answered = false;
                    current_phone.take();
                }
                continue;
            }

            // MMS send completion — resolve the waiter set by Modem::send_mms().
            if line.starts_with("+QMMSEND:") {
                if let Some((err_code, http_code)) = Modem::parse_qmmsend(&line) {
                    info!(
                        "[URC {}] MMS send completed: err={}, http={}",
                        sim_id, err_code, http_code
                    );
                    modem.resolve_mms_completion(err_code, http_code).await;
                } else {
                    error!("[URC {}] failed to parse +QMMSEND: line: {}", sim_id, line);
                }
                continue;
            }

            // FTP login completion — resolve the waiter set by Modem::ftp_download_to_ram().
            if line.starts_with("+QFTPOPEN:") {
                if let Some((err_code, protocol_error)) = Modem::parse_qftpopen(&line) {
                    info!(
                        "[URC {}] FTP login completed: err={}, protocol_error={}",
                        sim_id, err_code, protocol_error
                    );
                    modem.resolve_ftp_open_completion(err_code, protocol_error).await;
                } else {
                    error!("[URC {}] failed to parse +QFTPOPEN: line: {}", sim_id, line);
                }
                continue;
            }

            // FTP GET completion — resolve the waiter set by Modem::ftp_download_to_ram().
            if line.starts_with("+QFTPGET:") {
                if let Some((err_code, transfer_len)) = Modem::parse_qftpget(&line) {
                    info!(
                        "[URC {}] FTP GET completed: err={}, transfer_len={}",
                        sim_id, err_code, transfer_len
                    );
                    modem.resolve_ftp_get_completion(err_code, transfer_len).await;
                } else {
                    error!("[URC {}] failed to parse +QFTPGET: line: {}", sim_id, line);
                }
                continue;
            }

            // MMS content fetch (AT+QHTTPGET) completion — resolve the waiter set by
            // Modem::fetch_mms_content().
            if line.starts_with("+QHTTPGET:") {
                if let Some((err_code, http_code, content_length)) = Modem::parse_qhttpget(&line) {
                    info!(
                        "[URC {}] MMS content HTTP GET completed: err={}, http={}, len={}",
                        sim_id, err_code, http_code, content_length
                    );
                    modem
                        .resolve_http_get_completion(err_code, http_code, content_length)
                        .await;
                } else {
                    error!("[URC {}] failed to parse +QHTTPGET: line: {}", sim_id, line);
                }
                continue;
            }

            // AT+QHTTPREADFILE completion — resolve the waiter set by
            // Modem::readfile_then_download(). `OK` from the command itself only means
            // the request was accepted; the file isn't actually ready until this URC.
            if line.starts_with("+QHTTPREADFILE:") {
                if let Some(err_code) = Modem::parse_qhttpreadfile(&line) {
                    info!("[URC {}] QHTTPREADFILE completed: err={}", sim_id, err_code);
                    modem.resolve_http_readfile_completion(err_code).await;
                } else {
                    error!("[URC {}] failed to parse +QHTTPREADFILE: line: {}", sim_id, line);
                }
                continue;
            }

            // Any other URC (e.g. +CMTI:, +CSQ:) — log at trace level
            log::trace!("[URC {}] unhandled: {:?}", sim_id, line);
        }
    }
}
