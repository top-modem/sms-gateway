use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;

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
    _initialization_semaphore: Arc<Semaphore>,
    /// Background URC tasks keyed by SIM ID so they can be cancelled on demotion.
    urc_tasks: RwLock<HashMap<String, JoinHandle<()>>>,
    /// COM ports that are not currently usable (com_port, baud_rate)
    pub unavailable_ports: RwLock<Vec<(String, u32)>>,
    /// Original device list so unavailable ports can be retried later.
    devices: Vec<crate::config::Device>,
    /// If true, UK MCC SIMs (234/235) will force COPS 46001 during SIM init.
    force_uk_mcc_to_46001: bool,
    /// Number of modems to initialize in parallel (from config).
    max_concurrent_modem_init: usize,
    /// Default SMS storage location if not overridden per-device.
    default_sms_storage: Option<SmsStorage>,
    /// Transient AT+CCID probe failures before a modem is considered truly unavailable.
    sim_probe_fail_counts: RwLock<HashMap<String, u8>>,
}

const SIM_PROBE_FAILURE_THRESHOLD: u8 = 1;

impl ModemManager {
    async fn initialize_single_modem_safe(
        port: String,
        baud_rate: u32,
        device_id: String,
        sms_storage: Option<SmsStorage>,
        index: usize,
        force_uk_mcc_to_46001: bool,
    ) -> anyhow::Result<(String, Modem, bool)> {
        const INIT_TIMEOUT_SECS: u64 = 45;

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
        Self {
            modems: Arc::new(RwLock::new(HashMap::new())),
            sim_cards_cache: Arc::new(RwLock::new(HashMap::new())),
            _initialization_semaphore: Arc::new(Semaphore::new(3)),
            urc_tasks: RwLock::new(HashMap::new()),
            unavailable_ports: RwLock::new(Vec::new()),
            devices: config.devices.clone(),
            force_uk_mcc_to_46001: config.settings.force_uk_mcc_to_46001.unwrap_or(true),
            max_concurrent_modem_init: config.settings.max_concurrent_modem_init.unwrap_or(3),
            default_sms_storage: config.settings.sms_storage,
            sim_probe_fail_counts: RwLock::new(HashMap::new()),
        }
    }

    /// Initialize all configured modems and start their URC handlers.
    /// This is intentionally run in the background so the HTTP API can start
    /// immediately without waiting for slow/disconnected serial ports to time out.
    pub async fn initialize_modems(
        self: Arc<Self>,
        sse_manager: Arc<SseManager>,
        _webhook_manager: Option<webhook::WebhookManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) -> anyhow::Result<()> {
        let force_uk_mcc_to_46001 = self.force_uk_mcc_to_46001;
        let max_concurrent = self.max_concurrent_modem_init;

        // 0 or 1 = serial initialization (safest for USB hubs with shared resources)
        // 2+ = parallel initialization with semaphore to limit concurrent AT commands
        let is_serial = max_concurrent <= 1;

        let mut modems = HashMap::new();
        let mut new_sim_ids = Vec::new();
        let mut unavailable_ports: Vec<(String, u32)> = Vec::new();

        if is_serial {
            info!("Initializing modems serially (max_concurrent_modem_init={})", max_concurrent);
            // Initialize ports sequentially (not concurrently).  In this deployment
            // many COM ports share the same USB hub / serial concentrator; issuing
            // AT commands to several ports at the same time causes command timeouts
            // and makes the service hang during startup.
            for (index, device) in self.devices.iter().enumerate() {
                let port = device.com_port.clone();
                let baud_rate = device.baud_rate;
                let sms_storage = device.sms_storage.or(self.default_sms_storage);
                let temp_device_id = format!("device_{}", index);

                match Self::initialize_single_modem_safe(
                    port.clone(),
                    baud_rate,
                    temp_device_id,
                    sms_storage,
                    index,
                    force_uk_mcc_to_46001,
                )
                .await
                {
                    Ok((sim_id, modem, is_new)) => {
                        if is_new {
                            new_sim_ids.push(sim_id.clone());
                        }
                        modems.insert(sim_id, Arc::new(modem));
                    }
                    Err(e) => {
                        error!("Failed to initialize modem on {}: {}", port, e);
                        unavailable_ports.push((port, baud_rate));
                    }
                }
            }
        } else {
            info!("Initializing modems in parallel (max_concurrent_modem_init={})", max_concurrent);
            // Parallel initialization with semaphore to limit concurrent AT commands
            let semaphore = Arc::new(Semaphore::new(max_concurrent));
            let mut init_futures = FuturesUnordered::new();

            for (index, device) in self.devices.iter().enumerate() {
                let port = device.com_port.clone();
                let baud_rate = device.baud_rate;
                let sms_storage = device.sms_storage.or(self.default_sms_storage);
                let temp_device_id = format!("device_{}", index);
                let sem = semaphore.clone();

                init_futures.push(async move {
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
                    (port, result)
                });
            }

            while let Some((port, result)) = init_futures.next().await {
                match result {
                    Ok((sim_id, modem, is_new)) => {
                        if is_new {
                            new_sim_ids.push(sim_id.clone());
                        }
                        modems.insert(sim_id, Arc::new(modem));
                    }
                    Err(e) => {
                        error!("Failed to initialize modem on {}: {}", port, e);
                        if let Some(device) = self.devices.iter().find(|d| d.com_port == port) {
                            unavailable_ports.push((port, device.baud_rate));
                        }
                    }
                }
            }
        }

        // Even if no modems have a real SIM, we still start the manager so the
        // API can report all ports as unavailable instead of crashing.
        if modems.is_empty() {
            log::warn!(
                "No modems with valid SIM cards were initialized ({} ports unavailable). \
                 Starting with empty modem set.",
                unavailable_ports.len()
            );
        }

        info!(
            "Successfully initialized {} modem(s), {} unavailable",
            modems.len(),
            unavailable_ports.len()
        );

        {
            let mut self_modems = self.modems.write().await;
            *self_modems = modems;
        }
        {
            let mut self_unavailable = self.unavailable_ports.write().await;
            *self_unavailable = unavailable_ports;
        }

        self.init_sim_cache().await?;
        self.cleanup_stale_sim_cards().await?;

        if !new_sim_ids.is_empty() {
            self.init_new_sim_sms_data(new_sim_ids).await;
        }

        self.start_urc_handlers(sse_manager, transcribe_cfg).await;

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

        // Some virtual COM ports open but are not backed by a real modem.
        // Keep this probe as advisory only: certain healthy modems can miss the
        // first quick AT check right after opening and then respond normally.
        if !modem.is_responsive().await {
            log::warn!(
                "Modem on port {} did not pass initial quick AT probe; continuing with ICCID/init checks.",
                port
            );
        }

        let pre_sim_id = match modem.get_sim_iccid().await {
            Ok(Some(id)) => Some(id),
            Ok(None) => {
                log::warn!("No SIM detected on port {}, treating as unavailable", port);
                return Err(anyhow::anyhow!("No SIM detected"));
            }
            Err(e) => {
                log::warn!(
                    "Failed to read SIM ICCID on port {} (cannot communicate): {}. Treating as unavailable.",
                    port, e
                );
                return Err(anyhow::anyhow!("Failed to read SIM ICCID: {}", e));
            }
        };

        let is_new_sim = Self::is_new_sim_id(pre_sim_id.as_ref().unwrap()).await;

        if let Err(e) = modem.init_modem(sms_storage).await {
            log::warn!(
                "Modem AT init failed for port {}: {}. Treating as unavailable.",
                port, e
            );
            return Err(anyhow::anyhow!("Modem AT init failed: {}", e));
        }

        let sim_id = pre_sim_id.unwrap();

        info!(
            "Successfully initialized modem on {} with SIM ID: {}",
            port, sim_id
        );

        Ok((sim_id, modem, is_new_sim))
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
        let modems = self.modems.read().await;
        let sim_ids: Vec<&str> = modems.keys().map(|k| k.as_str()).collect();

        if sim_ids.is_empty() {
            info!("Initialized SIM cache with 0 cards (no modems available)");
            return Ok(());
        }

        let sim_cards = SimCard::get_by_ids(&sim_ids).await?;

        let mut cache = self.sim_cards_cache.write().await;
        *cache = sim_cards;

        info!("Initialized SIM cache with {} cards", cache.len());
        Ok(())
    }

    /// Remove sim_cards rows for SIMs that are no longer physically present.
    /// This prevents the dashboard/API from showing stale IMSI/ICCID/phone info
    /// after a SIM has been pulled out (especially when the server was stopped
    /// at the time the SIM was removed).
    async fn cleanup_stale_sim_cards(&self) -> anyhow::Result<()> {
        let modems = self.modems.read().await;
        let active_ids: std::collections::HashSet<&str> =
            modems.keys().map(|k| k.as_str()).collect();
        if active_ids.is_empty() {
            log::info!("No active modems; skipping stale sim_cards cleanup");
            return Ok(());
        }

        let all_cards = SimCard::query_all().await?;
        let mut deleted = 0;
        for card in all_cards {
            if !active_ids.contains(card.id.as_str()) {
                match SimCard::delete_by_id(&card.id).await {
                    Ok(true) => {
                        deleted += 1;
                        log::info!(
                            "[cleanup] Deleted stale sim_cards row for absent SIM {}",
                            card.id
                        );
                    }
                    Ok(false) => {}
                    Err(e) => log::warn!(
                        "[cleanup] Failed to delete stale sim_cards row {}: {}",
                        card.id,
                        e
                    ),
                }
            }
        }

        if deleted > 0 {
            info!("Cleaned up {} stale sim_cards row(s) for removed SIMs", deleted);
        }
        Ok(())
    }

    async fn init_new_sim_sms_data(&self, new_sim_ids: Vec<String>) {
        let mut futures = FuturesUnordered::new();

        for sim_id in new_sim_ids {
            let modems = self.modems.clone();
            futures.push(async move {
                let modems = modems.read().await;
                if let Some(modem) = modems.get(&sim_id) {
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
        self.modems.read().await.keys().cloned().collect()
    }

    pub async fn get_modem(&self, sim_id: &str) -> Option<Arc<Modem>> {
        self.modems.read().await.get(sim_id).cloned()
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
        let modems = self.modems.read().await;
        let mut futures = FuturesUnordered::new();

        for (sim_id, modem) in modems.iter() {
            // Skip modems without a SIM card — they cannot receive SMS
            if sim_id.starts_with("fallback_sim_") {
                continue;
            }

            let modem = modem.clone();
            let sse_manager = sse_manager.clone();
            let webhook_manager = webhook_manager.clone();
            let sim_id = sim_id.clone();

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
    /// Demotes real-ICCID modems where +CCID returns a different or missing ICCID.
    /// Periodically attempts to reopen ports listed in `unavailable_ports`.
    pub async fn recheck_fallback_modems(
        &self,
        sms_storage_map: &std::collections::HashMap<String, Option<crate::config::SmsStorage>>,
        sse_manager: Arc<SseManager>,
        _webhook_manager: Option<webhook::WebhookManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) {
        // ── Demotion: parallel AT+CCID on all real-ICCID modems ──────────────
        let active_entries: Vec<(String, Arc<Modem>)> = {
            let modems = self.modems.read().await;
            modems
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        #[derive(Debug)]
        enum ProbeOutcome {
            Healthy { iccid: String },
            Swap { iccid: String, com_port: String },
            MissingOrError { iccid: String, com_port: String },
        }

        let mut demotion_futs = FuturesUnordered::new();
        for (iccid, modem) in active_entries {
            demotion_futs.push(async move {
                match modem.get_sim_iccid().await {
                    Ok(Some(current_iccid)) if current_iccid == iccid => ProbeOutcome::Healthy { iccid },
                    Ok(Some(current_iccid)) => {
                        info!(
                            "SIM swap detected on {} (was {}, now {}). Forcing re-init.",
                            modem.com_port, iccid, current_iccid
                        );
                        ProbeOutcome::Swap { iccid, com_port: modem.com_port.clone() }
                    }
                    _ => {
                        ProbeOutcome::MissingOrError { iccid, com_port: modem.com_port.clone() }
                    }
                }
            });
        }
        let mut demotions: Vec<(String, String)> = Vec::new();
        while let Some(result) = demotion_futs.next().await {
            match result {
                ProbeOutcome::Healthy { iccid } => {
                    self.sim_probe_fail_counts.write().await.remove(&iccid);
                }
                ProbeOutcome::Swap { iccid, com_port } => {
                    self.sim_probe_fail_counts.write().await.remove(&iccid);
                    demotions.push((iccid, com_port));
                }
                ProbeOutcome::MissingOrError { iccid, com_port } => {
                    let failures = {
                        let mut fail_counts = self.sim_probe_fail_counts.write().await;
                        let count = fail_counts.entry(iccid.clone()).or_insert(0);
                        *count = count.saturating_add(1);
                        *count
                    };

                    if failures >= SIM_PROBE_FAILURE_THRESHOLD {
                        info!(
                            "SIM probe failed {} times on {} (was {}). Marking port as unavailable.",
                            failures, com_port, iccid
                        );
                        self.sim_probe_fail_counts.write().await.remove(&iccid);
                        demotions.push((iccid, com_port));
                    } else {
                        log::debug!(
                            "Transient SIM probe failure {}/{} on {} (SIM {}). Keeping modem active.",
                            failures,
                            SIM_PROBE_FAILURE_THRESHOLD,
                            com_port,
                            iccid
                        );
                    }
                }
            }
        }
        for (iccid, com_port) in demotions {
            let mut modems = self.modems.write().await;
            if let Some(modem) = modems.remove(&iccid) {
                if let Some(task) = self.urc_tasks.write().await.remove(&iccid) {
                    task.abort();
                }
                let baud_rate = modem.baud_rate;
                drop(modems);
                let mut unavailable = self.unavailable_ports.write().await;
                if !unavailable.iter().any(|(p, _)| p == &com_port) {
                    unavailable.push((com_port.clone(), baud_rate));
                }
                // Remove demoted SIM from cache so dashboard doesn't show stale data
                let mut cache = self.sim_cards_cache.write().await;
                if cache.remove(&iccid).is_some() {
                    log::info!("[recheck] Removed demoted SIM {} from cache (port {})", iccid, com_port);
                }
                // Also delete the DB record so the SIM card list does not retain
                // stale IMSI/ICCID/phone info after the physical SIM is removed.
                match SimCard::delete_by_id(&iccid).await {
                    Ok(true) => log::info!(
                        "[recheck] Deleted sim_cards DB row for removed SIM {} (port {})",
                        iccid, com_port
                    ),
                    Ok(false) => log::debug!(
                        "[recheck] No sim_cards DB row to delete for SIM {} (port {})",
                        iccid, com_port
                    ),
                    Err(e) => log::warn!(
                        "[recheck] Failed to delete sim_cards DB row for SIM {} (port {}): {}",
                        iccid, com_port, e
                    ),
                }
            }
        }

        // ── Reconnection: try to reopen ports that were unavailable at startup ──
        let unavailable_snapshot: Vec<(String, u32)> = self.unavailable_ports.read().await.clone();
        let mut reconnected = Vec::new();
        for (com_port, baud_rate) in unavailable_snapshot {
            // Look up the original device configuration for sms_storage and index.
            let device_cfg = self.devices.iter().find(|d| d.com_port == com_port);
            let sms_storage = device_cfg
                .and_then(|d| d.sms_storage)
                .or(*sms_storage_map.get(&com_port).unwrap_or(&None));
            let index = device_cfg
                .map(|d| self.devices.iter().position(|x| x.com_port == d.com_port).unwrap_or(0))
                .unwrap_or_else(|| {
                    com_port
                        .trim_start_matches(|c: char| !c.is_ascii_digit())
                        .parse::<usize>()
                        .unwrap_or(0)
                        .saturating_sub(1)
                });
            match Self::initialize_single_modem_safe(
                com_port.clone(),
                baud_rate,
                format!("device_{}", index),
                sms_storage,
                index,
                self.force_uk_mcc_to_46001,
            )
            .await
            {
                Ok((sim_id, modem, is_new)) => {
                    info!(
                        "Port {} reconnected with SIM {}. Promoting to active.",
                        com_port, sim_id
                    );
                    let mut modems = self.modems.write().await;
                    modems.insert(sim_id.clone(), Arc::new(modem));
                    drop(modems);
                    if is_new {
                        self.init_new_sim_sms_data(vec![sim_id.clone()]).await;
                    }
                    // Query DB for the latest SIM info and update cache
                    match SimCard::find_by_conditions(Some(&sim_id), None, None, None).await {
                        Ok(cards) => {
                            if let Some(card) = cards.into_iter().next() {
                                let mut cache = self.sim_cards_cache.write().await;
                                cache.insert(sim_id.clone(), card);
                                log::info!("[recheck] Updated cache for reconnected SIM {} on port {}", sim_id, com_port);
                            } else {
                                log::warn!("[recheck] Reconnected SIM {} not found in DB, cache not updated", sim_id);
                            }
                        }
                        Err(e) => {
                            log::warn!("[recheck] Failed to query DB for reconnected SIM {}: {}", sim_id, e);
                        }
                    }
                    if let Some(modem) = self.get_modem(&sim_id).await {
                        let handle = tokio::spawn(Self::run_urc_handler(
                            sim_id.clone(),
                            modem,
                            sse_manager.clone(),
                            transcribe_cfg.clone(),
                        ));
                        if let Some(old) = self.urc_tasks.write().await.insert(sim_id.clone(), handle) {
                            old.abort();
                        }
                    }
                    reconnected.push(com_port);
                }
                Err(e) => {
                    log::debug!("Port {} still unavailable: {}", com_port, e);
                }
            }
        }
        if !reconnected.is_empty() {
            let mut unavailable = self.unavailable_ports.write().await;
            unavailable.retain(|(p, _)| !reconnected.contains(p));
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
        let modems = self.modems.read().await;
        modems
            .values()
            .find(|m| m.com_port == com_port)
            .cloned()
    }

    pub async fn send_at_command(&self, com_port: &str, command: &str) -> anyhow::Result<String> {
        let modem = self
            .get_modem_by_com_port(com_port)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found on port: {}", com_port))?;
        modem.send_command(command).await.map_err(Into::into)
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

    pub async fn answer_call(&self, sim_id: &str) -> anyhow::Result<()> {
        let modem = self
            .get_modem(sim_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Modem not found for SIM ID: {}", sim_id))?;
        modem.answer_call().await.map_err(Into::into)
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

    /// Spawn one URC handler task per modem.  Must be called once after
    /// initialization.  Each task owns the modem's `urc_rx` and processes
    /// RING / +CLIP: / NO CARRIER / VOICE CALL: END messages forever.
    pub async fn start_urc_handlers(&self,
        sse_manager: Arc<SseManager>,
        transcribe_cfg: Option<Arc<TranscribeConfig>>,
    ) {
        let modems = self.modems.read().await;
        if modems.is_empty() {
            log::info!("No active modems; skipping URC handler startup");
            return;
        }
        for (sim_id, modem) in modems.iter() {
            let sim_id = sim_id.clone();
            let modem = modem.clone();
            let sse = sse_manager.clone();
            let cfg = transcribe_cfg.clone();
            let sim_id_for_task = sim_id.clone();
            let handle = tokio::spawn(async move {
                Self::run_urc_handler(sim_id_for_task, modem, sse, cfg).await;
            });
            if let Some(old) = self.urc_tasks.write().await.insert(sim_id.clone(), handle) {
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
        // Oneshot sender to cancel the 30-second recording timer. Some = recording active.
        let mut recording_cancel_tx: Option<tokio::sync::oneshot::Sender<()>> = None;

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
                            // Check if auto-answer is enabled
                            let should_auto_answer = transcribe_cfg
                                .as_ref()
                                .map(|cfg| cfg.auto_answer_enabled)
                                .unwrap_or(true);
                            if should_auto_answer {
                                // Wait before auto-answering to let the phone ring briefly.
                                if let Some(cfg) = transcribe_cfg.as_ref() {
                                    if cfg.auto_answer_delay_secs > 0 {
                                        info!(
                                            "[URC {}] waiting {}s before auto-answer",
                                            sim_id, cfg.auto_answer_delay_secs
                                        );
                                        tokio::time::sleep(Duration::from_secs(cfg.auto_answer_delay_secs))
                                            .await;
                                    }
                                }
                                // Auto-answer the incoming call.
                                // EC20 voice calls respond to ATA with OK only — no CONNECT URC.
                                // So we handle the answered state here rather than in the CONNECT branch.
                                match modem.answer_call().await {
                                Err(e) => error!("[URC {}] auto-answer failed: {}", sim_id, e),
                                Ok(()) => {
                                    info!("[URC {}] auto-answered incoming call", sim_id);
                                    call_answered = true;
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
                                    recording_cancel_tx = Some(cancel_tx);
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
                info!("[URC {}] {} received: call_answered={}, has_active={}, has_recording={}",
                    sim_id, line, call_answered, active_call_id.is_some(), recording_cancel_tx.is_some());
                if let Some(call_id) = active_call_id.take() {
                    // ── Stop recording if active ──────────────────────────────
                    if let Some(tx) = recording_cancel_tx.take() {
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
