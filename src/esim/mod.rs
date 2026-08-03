pub mod activation;
pub mod batch;
pub mod error;
pub mod lpac;

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::Settings;
use crate::ModemManagerRef;

pub use activation::ActivationCode;
pub use batch::{BatchManager, BatchOp, BatchReqItem, BatchRequest, JobSnapshot};
pub use error::EsimError;
use batch::BatchItemResult;
use lpac::Lpac;

/// A downloadable-profile request coming from the API layer.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DownloadRequest {
    pub smdp: Option<String>,
    pub matching_id: Option<String>,
    pub confirmation_code: Option<String>,
    pub imei: Option<String>,
    /// A single `LPA:1$smdp$matchingId` activation string (alternative to the fields above).
    pub activation_code: Option<String>,
}

/// One row in the eSIM ports overview.
#[derive(Debug, Clone, Serialize)]
pub struct PortInfo {
    pub com_port: String,
    pub esim_capable: bool,
    pub esim_mode: bool,
    /// True if this port currently holds the machine-wide eSIM session.
    pub session_active: bool,
    pub sim_id: Option<String>,
}

/// Machine-wide eSIM management service. Only one port may be in an eSIM session
/// at a time (a physical constraint of these machines), enforced by `active`.
pub struct EsimService {
    manager: ModemManagerRef,
    /// COM port that currently holds the eSIM session, if any.
    active: Arc<Mutex<Option<String>>>,
    lpac: Lpac,
    enabled: bool,
    default_smdp: Option<String>,
    reader_settle_ms: u64,
    /// Directory scanned for batch activation sources (QR images + text lists).
    source_dir: PathBuf,
    /// Machine-wide batch job state + progress broadcast.
    batch: Arc<BatchManager>,
    batch_auto_enable: bool,
    batch_replace_existing: bool,
    batch_stop_on_error: bool,
}

impl EsimService {
    pub fn new(manager: ModemManagerRef, settings: &Settings) -> Self {
        let lpac_exe = settings
            .lpac_exe
            .clone()
            .unwrap_or_else(|| {
                if Path::new("./lpac/lpac.exe").exists() {
                    "./lpac/lpac.exe".to_string()
                } else {
                    "lpac".to_string()
                }
            });

        let lpac = Lpac::new(
            lpac_exe,
            settings
                .lpac_apdu_backend
                .clone()
                .unwrap_or_else(|| "pcsc".to_string()),
            settings
                .lpac_http_backend
                .clone()
                .unwrap_or_else(|| "winhttp".to_string()),
            settings.lpac_pcsc_reader_name.clone(),
        );
        Self {
            manager,
            active: Arc::new(Mutex::new(None)),
            lpac,
            enabled: settings.esim_enabled.unwrap_or(false),
            default_smdp: settings.esim_default_smdp.clone(),
            reader_settle_ms: settings.esim_reader_settle_ms.unwrap_or(1500),
            source_dir: PathBuf::from(
                settings
                    .esim_source_dir
                    .clone()
                    .unwrap_or_else(|| "./esim/com_port".to_string()),
            ),
            batch: Arc::new(BatchManager::new()),
            batch_auto_enable: settings.esim_batch_auto_enable.unwrap_or(true),
            batch_replace_existing: settings.esim_batch_replace_existing.unwrap_or(true),
            batch_stop_on_error: settings.esim_batch_stop_on_error.unwrap_or(false),
        }
    }

    fn ensure_enabled(&self) -> Result<(), EsimError> {
        if self.enabled {
            Ok(())
        } else {
            Err(EsimError::Disabled)
        }
    }

    /// Public enablement check for endpoints that do not touch the modem gate.
    pub fn ready(&self) -> Result<(), EsimError> {
        self.ensure_enabled()
    }

    async fn get_modem(
        &self,
        com_port: &str,
    ) -> Result<Arc<crate::modem::core::Modem>, EsimError> {
        self.manager
            .get_modem_by_com_port(com_port)
            .await
            .ok_or_else(|| EsimError::PortNotFound(com_port.to_string()))
    }

    /// List every tracked port, probing eSIM capability (cached after first probe).
    pub async fn list_ports(&self) -> Result<Vec<PortInfo>, EsimError> {
        self.ensure_enabled()?;
        let active = self.active.lock().await.clone();
        let mut out = Vec::new();
        for modem in self.manager.all_modems().await {
            let capable = modem.detect_esim_capability().await;
            let sim_id = modem.sim_id.read().await.clone();
            out.push(PortInfo {
                com_port: modem.com_port.clone(),
                esim_capable: capable,
                esim_mode: modem.is_esim_mode().await,
                session_active: active.as_deref() == Some(modem.com_port.as_str()),
                sim_id,
            });
        }
        out.sort_by(|a, b| a.com_port.cmp(&b.com_port));
        Ok(out)
    }

    /// Acquire the machine-wide gate for `com_port` and switch it into eSIM mode.
    pub async fn enter(&self, com_port: &str) -> Result<(), EsimError> {
        self.ensure_enabled()?;
        let modem = self.get_modem(com_port).await?;
        if !modem.detect_esim_capability().await {
            return Err(EsimError::NotCapable(com_port.to_string()));
        }

        {
            let mut active = self.active.lock().await;
            match active.as_deref() {
                Some(p) if p == com_port => {} // already ours; idempotent
                Some(p) => return Err(EsimError::Busy(p.to_string())),
                None => *active = Some(com_port.to_string()),
            }
        }

        // Perform the serial mode switch. On failure, release the gate.
        if let Err(e) = modem.esim_enter().await {
            *self.active.lock().await = None;
            let _ = self.audit(com_port, "enter", None, Some(&e.to_string())).await;
            return Err(EsimError::At(e.to_string()));
        }

        // Let the Windows smart-card reader appear before lpac is used.
        tokio::time::sleep(Duration::from_millis(self.reader_settle_ms)).await;
        let _ = self.audit(com_port, "enter", Some(0), Some("ok")).await;
        Ok(())
    }

    /// Switch `com_port` back to Normal mode and release the gate. Always clears
    /// the gate even if the AT sequence partially fails.
    pub async fn exit(&self, com_port: &str) -> Result<(), EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let modem = self.get_modem(com_port).await?;
        let result = modem.esim_exit().await;
        *self.active.lock().await = None;

        match &result {
            Ok(_) => {
                let _ = self.audit(com_port, "exit", Some(0), Some("ok")).await;
            }
            Err(e) => {
                let _ = self.audit(com_port, "exit", None, Some(&e.to_string())).await;
            }
        }

        // The periodic recheck worker will re-read the (possibly new) ICCID/IMSI
        // on this port now that it is back in Normal mode.
        result.map_err(|e| EsimError::At(e.to_string()))
    }

    /// Abnormal-state recovery (`ath0`). Releases the gate if held by this port.
    pub async fn reset(&self, com_port: &str) -> Result<(), EsimError> {
        self.ensure_enabled()?;
        let modem = self.get_modem(com_port).await?;
        let result = modem.esim_reset().await;
        {
            let mut active = self.active.lock().await;
            if active.as_deref() == Some(com_port) {
                *active = None;
            }
        }
        match &result {
            Ok(_) => {
                let _ = self.audit(com_port, "reset", Some(0), Some("ok")).await;
            }
            Err(e) => {
                let _ = self.audit(com_port, "reset", None, Some(&e.to_string())).await;
            }
        }
        result.map_err(|e| EsimError::At(e.to_string()))
    }

    async fn ensure_session(&self, com_port: &str) -> Result<(), EsimError> {
        let active = self.active.lock().await;
        match active.as_deref() {
            Some(p) if p == com_port => Ok(()),
            _ => Err(EsimError::NotInSession(com_port.to_string())),
        }
    }

    pub async fn chip_info(&self, com_port: &str) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        self.lpac.chip_info().await
    }

    pub async fn profiles(&self, com_port: &str) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        self.lpac.profile_list().await
    }

    pub async fn download(
        &self,
        com_port: &str,
        req: &DownloadRequest,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let smdp = req.smdp.as_deref().or(self.default_smdp.as_deref());
        let result = self
            .lpac
            .profile_download(
                smdp,
                req.matching_id.as_deref(),
                req.confirmation_code.as_deref(),
                req.imei.as_deref(),
                req.activation_code.as_deref(),
            )
            .await;
        let params = serde_json::to_string(req).ok();
        self.audit_result(com_port, "download", params.as_deref(), &result)
            .await;
        result
    }

    pub async fn enable(
        &self,
        com_port: &str,
        iccid: &str,
        refresh: bool,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let result = self.lpac.profile_enable(iccid, refresh).await;
        self.audit_result(com_port, "enable", Some(iccid), &result)
            .await;
        result
    }

    pub async fn disable(
        &self,
        com_port: &str,
        iccid: &str,
        refresh: bool,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let result = self.lpac.profile_disable(iccid, refresh).await;
        self.audit_result(com_port, "disable", Some(iccid), &result)
            .await;
        result
    }

    pub async fn delete(&self, com_port: &str, iccid: &str) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let result = self.lpac.profile_delete(iccid).await;
        self.audit_result(com_port, "delete", Some(iccid), &result)
            .await;
        result
    }

    pub async fn nickname(
        &self,
        com_port: &str,
        iccid: &str,
        name: &str,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let result = self.lpac.profile_nickname(iccid, name).await;
        self.audit_result(com_port, "nickname", Some(iccid), &result)
            .await;
        result
    }

    pub async fn notification_list(&self, com_port: &str) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        self.lpac.notification_list().await
    }

    pub async fn notification_process(
        &self,
        com_port: &str,
        seq: &str,
        remove: bool,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        self.lpac.notification_process(seq, remove).await
    }

    pub async fn notification_remove(
        &self,
        com_port: &str,
        seq: &str,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        self.lpac.notification_remove(seq).await
    }

    /// One-shot convenience flow: enter -> download -> enable -> process
    /// notifications -> exit, all under a single gate acquisition. Always exits
    /// eSIM mode, even on error.
    pub async fn provision(
        &self,
        com_port: &str,
        req: &DownloadRequest,
    ) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.enter(com_port).await?;

        let outcome = self.provision_inner(com_port, req).await;

        // Always return the port to Normal mode regardless of the result.
        let _ = self.exit(com_port).await;
        outcome
    }

    async fn provision_inner(
        &self,
        com_port: &str,
        req: &DownloadRequest,
    ) -> Result<Value, EsimError> {
        let downloaded = self.download(com_port, req).await?;

        // Best-effort: enable the freshly-downloaded profile if we can find its ICCID.
        if let Some(iccid) = downloaded
            .get("iccid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        {
            let _ = self.enable(com_port, &iccid, true).await;
        }

        // Process any pending notifications (GSMA compliance), removing them.
        if let Ok(list) = self.notification_list(com_port).await {
            if let Some(items) = list.as_array() {
                for item in items {
                    if let Some(seq) = item
                        .get("seqNumber")
                        .and_then(|v| v.as_i64())
                        .map(|n| n.to_string())
                    {
                        let _ = self.notification_process(com_port, &seq, true).await;
                    }
                }
            }
        }

        Ok(downloaded)
    }

    async fn audit(
        &self,
        com_port: &str,
        op: &str,
        code: Option<i64>,
        message: Option<&str>,
    ) -> Result<(), EsimError> {
        if let Err(e) =
            crate::db::log_esim_operation(com_port, None, op, None, code, message).await
        {
            log::warn!("[eSIM] failed to write audit log for {}: {}", op, e);
        }
        Ok(())
    }

    async fn audit_result(
        &self,
        com_port: &str,
        op: &str,
        params: Option<&str>,
        result: &Result<Value, EsimError>,
    ) {
        let (code, message) = match result {
            Ok(_) => (Some(0i64), "ok".to_string()),
            Err(EsimError::Lpac { code, message }) => (Some(*code), message.clone()),
            Err(e) => (None, e.to_string()),
        };
        if let Err(e) =
            crate::db::log_esim_operation(com_port, None, op, params, code, Some(&message)).await
        {
            log::warn!("[eSIM] failed to write audit log for {}: {}", op, e);
        }
    }

    // ---- Activation sources ------------------------------------------------

    /// Scan the configured source directory for QR-image / text activation codes.
    pub fn scan_sources(&self) -> Vec<ActivationCode> {
        activation::scan_source_dir(&self.source_dir)
    }

    // ---- Single-profile helpers (one profile per eUICC) --------------------

    /// ICCIDs of every profile currently on the active eUICC.
    async fn list_profile_iccids(&self, com_port: &str) -> Result<Vec<String>, EsimError> {
        let list = self.profiles(com_port).await?;
        Ok(list
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| p.get("iccid").and_then(|v| v.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// ICCID of the single profile on the eUICC, if any.
    async fn current_iccid(&self, com_port: &str) -> Result<Option<String>, EsimError> {
        Ok(self.list_profile_iccids(com_port).await?.into_iter().next())
    }

    /// Enable the one profile present on the eUICC.
    async fn enable_current(&self, com_port: &str) -> Result<Option<String>, EsimError> {
        match self.current_iccid(com_port).await? {
            Some(iccid) => {
                self.enable(com_port, &iccid, true).await?;
                Ok(Some(iccid))
            }
            None => Err(EsimError::Lpac {
                code: -1,
                message: "no profile on eUICC".to_string(),
            }),
        }
    }

    /// Disable the one profile present on the eUICC.
    async fn disable_current(&self, com_port: &str) -> Result<Option<String>, EsimError> {
        match self.current_iccid(com_port).await? {
            Some(iccid) => {
                self.disable(com_port, &iccid, true).await?;
                Ok(Some(iccid))
            }
            None => Err(EsimError::Lpac {
                code: -1,
                message: "no profile on eUICC".to_string(),
            }),
        }
    }

    /// Delete the one profile present on the eUICC.
    async fn delete_current(&self, com_port: &str) -> Result<Option<String>, EsimError> {
        match self.current_iccid(com_port).await? {
            Some(iccid) => {
                // A profile must be disabled before it can be deleted.
                let _ = self.disable(com_port, &iccid, true).await;
                self.delete(com_port, &iccid).await?;
                Ok(Some(iccid))
            }
            None => Err(EsimError::Lpac {
                code: -1,
                message: "no profile to delete".to_string(),
            }),
        }
    }

    /// Remove every profile from the eUICC (used before a replace-download).
    async fn delete_all_profiles(&self, com_port: &str) {
        if let Ok(iccids) = self.list_profile_iccids(com_port).await {
            for iccid in iccids {
                let _ = self.disable(com_port, &iccid, true).await;
                let _ = self.delete(com_port, &iccid).await;
            }
        }
    }

    /// Send + remove all pending notifications (best-effort GSMA compliance).
    async fn process_all_notifications(&self, com_port: &str) {
        if let Ok(list) = self.notification_list(com_port).await {
            if let Some(items) = list.as_array() {
                for item in items {
                    if let Some(seq) = item
                        .get("seqNumber")
                        .and_then(|v| v.as_i64())
                        .map(|n| n.to_string())
                    {
                        let _ = self.notification_process(com_port, &seq, true).await;
                    }
                }
            }
        }
    }

    // ---- Batch orchestration ----------------------------------------------

    /// Current or last batch job snapshot.
    pub async fn batch_snapshot(&self) -> Option<JobSnapshot> {
        self.batch.snapshot().await
    }

    /// Subscribe to batch progress events (SSE).
    pub fn subscribe_batch(&self) -> tokio::sync::broadcast::Receiver<JobSnapshot> {
        self.batch.subscribe()
    }

    /// Request cancellation of the running batch job (checked between ports).
    pub fn cancel_batch(&self) {
        self.batch.request_cancel();
    }

    /// Start a sequential batch job. Returns the job id. Only one job may run
    /// machine-wide; a second request is rejected while one is in flight.
    pub async fn start_batch(self: Arc<Self>, req: BatchRequest) -> Result<String, EsimError> {
        self.ensure_enabled()?;
        {
            let active = self.active.lock().await;
            if let Some(p) = active.as_deref() {
                return Err(EsimError::Busy(format!("port {} in eSIM session", p)));
            }
        }
        if self
            .batch
            .running
            .swap(true, Ordering::SeqCst)
        {
            return Err(EsimError::Busy("batch job already running".to_string()));
        }
        self.batch.cancel.store(false, Ordering::SeqCst);

        let id = uuid::Uuid::new_v4().to_string();
        let items: Vec<BatchItemResult> = req
            .items
            .iter()
            .map(|it| BatchItemResult {
                com_port: it.com_port.clone(),
                status: "pending".to_string(),
                message: None,
                iccid: None,
            })
            .collect();
        let snap = JobSnapshot {
            id: id.clone(),
            op: req.op,
            status: "running".to_string(),
            items,
            started_at: chrono::Utc::now().to_rfc3339(),
            finished_at: None,
        };
        self.batch.publish(snap.clone()).await;

        let svc = self.clone();
        tokio::spawn(async move {
            svc.run_batch(req, snap).await;
        });
        Ok(id)
    }

    async fn run_batch(self: Arc<Self>, req: BatchRequest, mut snap: JobSnapshot) {
        let auto_enable = req.auto_enable.unwrap_or(self.batch_auto_enable);
        let replace = req.replace_existing.unwrap_or(self.batch_replace_existing);
        let stop_on_error = req.stop_on_error.unwrap_or(self.batch_stop_on_error);
        let mut aborted = false;

        for i in 0..req.items.len() {
            if aborted || self.batch.is_cancelled() {
                snap.items[i].status = "skipped".to_string();
                snap.items[i].message = Some("cancelled".to_string());
                self.batch.publish(snap.clone()).await;
                continue;
            }

            let item = req.items[i].clone();

            // A download needs an activation source; skip ports without one.
            if req.op == BatchOp::Download
                && item.activation_code.is_none()
                && item.matching_id.is_none()
            {
                snap.items[i].status = "skipped".to_string();
                snap.items[i].message = Some("no activation code".to_string());
                self.batch.publish(snap.clone()).await;
                continue;
            }

            snap.items[i].status = "running".to_string();
            self.batch.publish(snap.clone()).await;

            match self
                .run_batch_item(req.op, &item, auto_enable, replace)
                .await
            {
                Ok(iccid) => {
                    snap.items[i].status = "ok".to_string();
                    snap.items[i].iccid = iccid;
                    snap.items[i].message = Some("ok".to_string());
                }
                Err(e) => {
                    snap.items[i].status = "failed".to_string();
                    snap.items[i].message = Some(e.to_string());
                    if stop_on_error {
                        aborted = true;
                    }
                }
            }
            self.batch.publish(snap.clone()).await;
        }

        snap.status = if self.batch.is_cancelled() {
            "cancelled".to_string()
        } else {
            "done".to_string()
        };
        snap.finished_at = Some(chrono::Utc::now().to_rfc3339());
        self.batch.publish(snap).await;
        self.batch.running.store(false, Ordering::SeqCst);
    }

    /// Enter the port's eSIM session, run one operation, and always exit.
    async fn run_batch_item(
        &self,
        op: BatchOp,
        item: &BatchReqItem,
        auto_enable: bool,
        replace: bool,
    ) -> Result<Option<String>, EsimError> {
        self.enter(&item.com_port).await?;
        let result = self
            .batch_item_inner(op, item, auto_enable, replace)
            .await;
        let _ = self.exit(&item.com_port).await;
        result
    }

    async fn batch_item_inner(
        &self,
        op: BatchOp,
        item: &BatchReqItem,
        auto_enable: bool,
        replace: bool,
    ) -> Result<Option<String>, EsimError> {
        let com = &item.com_port;
        match op {
            BatchOp::Download => {
                if replace {
                    self.delete_all_profiles(com).await;
                }
                let req = DownloadRequest {
                    smdp: item.smdp.clone(),
                    matching_id: item.matching_id.clone(),
                    confirmation_code: item.confirmation_code.clone(),
                    imei: item.imei.clone(),
                    activation_code: item.activation_code.clone(),
                };
                let downloaded = self.download(com, &req).await?;
                let iccid = downloaded
                    .get("iccid")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or(self.current_iccid(com).await.ok().flatten());
                if auto_enable {
                    if let Some(ic) = &iccid {
                        let _ = self.enable(com, ic, true).await;
                    }
                }
                self.process_all_notifications(com).await;
                Ok(iccid)
            }
            BatchOp::Delete => self.delete_current(com).await,
            BatchOp::Activate => self.enable_current(com).await,
            BatchOp::Deactivate => self.disable_current(com).await,
        }
    }
}
