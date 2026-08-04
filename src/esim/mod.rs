pub mod activation;
pub mod batch;
pub mod error;
pub mod lpac;

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::config::Settings;
use crate::phone_number::normalize_msisdn;
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
    /// Optional MSISDN associated with the activation source.
    pub phone_number: Option<String>,
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
    pub last_op: Option<String>,
    /// `ok` when the last audited operation succeeded, otherwise `failed`.
    pub last_status: Option<String>,
    pub last_message: Option<String>,
    pub last_at: Option<String>,
}

/// Machine-wide eSIM management service. Only one port may be in an eSIM session
/// at a time (a physical constraint of these machines), enforced by `active`.
pub struct EsimService {
    manager: ModemManagerRef,
    /// COM port that currently holds the eSIM session, if any.
    active: Arc<Mutex<Option<String>>>,
    /// Correlation IDs for active eSIM sessions, keyed by COM port.
    session_traces: Arc<Mutex<HashMap<String, String>>>,
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
    fn is_timeout_error(err: &dyn std::fmt::Display, markers: &[&str]) -> bool {
        let msg = err.to_string().to_ascii_lowercase();
        msg.contains("timed out") && markers.iter().any(|m| msg.contains(m))
    }

    fn profile_is_enabled(profile: &Value) -> bool {
        let state = profile
            .get("profileState")
            .or_else(|| profile.get("state"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        matches!(state.as_str(), "enabled" | "active")
    }

    fn profile_is_disabled(profile: &Value) -> bool {
        let state = profile
            .get("profileState")
            .or_else(|| profile.get("state"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        matches!(state.as_str(), "disabled" | "inactive")
    }

    fn new_trace_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    async fn set_session_trace(&self, com_port: &str, trace_id: String) {
        self.session_traces
            .lock()
            .await
            .insert(com_port.to_string(), trace_id);
    }

    async fn session_trace(&self, com_port: &str) -> Option<String> {
        self.session_traces.lock().await.get(com_port).cloned()
    }

    async fn clear_session_trace(&self, com_port: &str) {
        self.session_traces.lock().await.remove(com_port);
    }
    fn profile_matches_iccid(profile: &Value, iccid: &str) -> bool {
        let lhs = profile
            .get("iccid")
            .or_else(|| profile.get("ICCID"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        lhs.eq_ignore_ascii_case(iccid)
    }

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
            session_traces: Arc::new(Mutex::new(HashMap::new())),
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

    async fn clear_active_if_matches(&self, com_port: &str) {
        let mut active = self.active.lock().await;
        if active.as_deref() == Some(com_port) {
            *active = None;
        }
    }

    /// If the remembered active session port no longer exists, clear the gate.
    /// This prevents stale "session busy" locks after abrupt hardware disappearance.
    async fn clear_stale_active_session(&self) {
        let active_port = { self.active.lock().await.clone() };
        if let Some(port) = active_port {
            let still_present = self.manager.get_modem_by_com_port(&port).await.is_some();
            if !still_present {
                log::warn!(
                    "[eSIM] clearing stale active session gate for missing port {}",
                    port
                );
                self.clear_active_if_matches(&port).await;
            }
        }
    }

    /// List every tracked port, probing eSIM capability (cached after first probe).
    pub async fn list_ports(&self) -> Result<Vec<PortInfo>, EsimError> {
        self.ensure_enabled()?;
        let active = self.active.lock().await.clone();
        let modems = self.manager.all_modems().await;
        let latest_by_port = crate::db::get_latest_esim_operations_by_ports(
            &modems
                .iter()
                .map(|m| m.com_port.clone())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap_or_default();
        let mut out = Vec::new();
        for modem in modems {
            let capable = modem.detect_esim_capability().await;
            let sim_id = modem.sim_id.read().await.clone();
            let latest = latest_by_port.get(&modem.com_port);
            let last_status = latest.map(|op| {
                if op.result_code == Some(0)
                    || op
                        .message
                        .as_deref()
                        .map(|m| m.eq_ignore_ascii_case("ok"))
                        .unwrap_or(false)
                {
                    "ok".to_string()
                } else {
                    "failed".to_string()
                }
            });
            out.push(PortInfo {
                com_port: modem.com_port.clone(),
                esim_capable: capable,
                esim_mode: modem.is_esim_mode().await,
                session_active: active.as_deref() == Some(modem.com_port.as_str()),
                sim_id,
                last_op: latest.map(|op| op.op_type.clone()),
                last_status,
                last_message: latest.and_then(|op| op.message.clone()),
                last_at: latest.map(|op| op.created_at.clone()),
            });
        }
        out.sort_by(|a, b| {
            fn com_num(s: &str) -> Option<u32> {
                let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
                digits.parse::<u32>().ok()
            }
            match (com_num(&a.com_port), com_num(&b.com_port)) {
                (Some(x), Some(y)) => x.cmp(&y).then_with(|| a.com_port.cmp(&b.com_port)),
                _ => a.com_port.cmp(&b.com_port),
            }
        });
        Ok(out)
    }

    /// Acquire the machine-wide gate for `com_port` and switch it into eSIM mode.
    pub async fn enter(&self, com_port: &str) -> Result<(), EsimError> {
        self.ensure_enabled()?;
        self.clear_stale_active_session().await;
        let modem = self.get_modem(com_port).await?;
        if !modem.detect_esim_capability().await {
            return Err(EsimError::NotCapable(com_port.to_string()));
        }

        let mut already_active = false;
        {
            let mut active = self.active.lock().await;
            match active.as_deref() {
                Some(p) if p == com_port => {
                    already_active = true;
                } // already ours; idempotent
                Some(p) => return Err(EsimError::Busy(p.to_string())),
                None => *active = Some(com_port.to_string()),
            }
        }

        let trace_id = if already_active {
            self.session_trace(com_port)
                .await
                .unwrap_or_else(Self::new_trace_id)
        } else {
            Self::new_trace_id()
        };
        log::info!(
            "[eSIM][trace={}] enter requested on {}{}",
            trace_id,
            com_port,
            if already_active { " (idempotent)" } else { "" }
        );

        // Perform the serial mode switch. If the transition times out, reset and retry once.
        let mut enter_result = modem.esim_enter().await;
        if let Err(e) = &enter_result {
            if Self::is_timeout_error(e, &["atl6", "ath7"]) {
                log::warn!(
                    "[eSIM][trace={}] enter timeout on {}; reset and retry once: {}",
                    trace_id,
                    com_port,
                    e
                );
                let _ = modem.esim_reset().await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                enter_result = modem.esim_enter().await;
            }
        }

        // On failure, reset back to normal mode and release the gate.
        if let Err(e) = enter_result {
            let reset_result = modem.esim_reset().await;
            *self.active.lock().await = None;
            self.clear_session_trace(com_port).await;
            if let Err(re) = reset_result {
                log::warn!(
                    "[eSIM][trace={}] enter failed on {}; fallback reset also failed: {}",
                    trace_id,
                    com_port,
                    re
                );
            }
            let _ = self.audit(com_port, "enter", None, Some(&e.to_string())).await;
            log::warn!("[eSIM][trace={}] enter failed on {}: {}", trace_id, com_port, e);
            return Err(EsimError::At(e.to_string()));
        }

        // Let the Windows smart-card reader appear before lpac is used.
        tokio::time::sleep(Duration::from_millis(self.reader_settle_ms)).await;
        self.set_session_trace(com_port, trace_id.clone()).await;
        let _ = self.audit(com_port, "enter", Some(0), Some("ok")).await;
        log::info!("[eSIM][trace={}] enter success on {}", trace_id, com_port);
        Ok(())
    }

    /// Switch `com_port` back to Normal mode and release the gate. Always clears
    /// the gate even if the AT sequence partially fails.
    pub async fn exit(&self, com_port: &str) -> Result<(), EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let trace_id = self
            .session_trace(com_port)
            .await
            .unwrap_or_else(Self::new_trace_id);
        log::info!("[eSIM][trace={}] exit requested on {}", trace_id, com_port);
        let modem = match self.get_modem(com_port).await {
            Ok(m) => m,
            Err(e) => {
                // If the modem vanished while holding the gate, unblock future sessions.
                self.clear_active_if_matches(com_port).await;
                self.clear_session_trace(com_port).await;
                let _ = self
                    .audit(com_port, "exit", None, Some("modem missing; gate released"))
                    .await;
                return Err(e);
            }
        };
        let mut result = modem.esim_exit().await;
        if let Err(e) = &result {
            if Self::is_timeout_error(e, &["atl7", "ath7"]) {
                log::warn!(
                    "[eSIM][trace={}] exit timeout on {}; retrying once: {}",
                    trace_id,
                    com_port,
                    e
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                result = modem.esim_exit().await;
            }
        }

        let mut recovered_by_reset = false;
        if result.is_err() {
            match modem.esim_reset().await {
                Ok(_) => {
                    if !modem.is_esim_mode().await {
                        log::warn!(
                            "[eSIM][trace={}] exit failed on {}, but reset restored normal mode; normalizing success",
                            trace_id,
                            com_port
                        );
                        result = Ok(());
                        recovered_by_reset = true;
                    } else {
                        log::warn!(
                            "[eSIM][trace={}] exit failed on {}; reset succeeded but modem still reports eSIM mode",
                            trace_id,
                            com_port
                        );
                    }
                }
                Err(re) => {
                    log::warn!(
                        "[eSIM][trace={}] exit failed on {}; reset failed: {}",
                        trace_id,
                        com_port,
                        re
                    );
                }
            }
        }
        self.clear_active_if_matches(com_port).await;
        self.clear_session_trace(com_port).await;

        match &result {
            Ok(_) => {
                let success_msg = if recovered_by_reset {
                    "ok (recovered by reset)"
                } else {
                    "ok"
                };
                let _ = self.audit(com_port, "exit", Some(0), Some(success_msg)).await;
                log::info!(
                    "[eSIM][trace={}] exit success on {}{}",
                    trace_id,
                    com_port,
                    if recovered_by_reset {
                        " (recovered by reset)"
                    } else {
                        ""
                    }
                );
            }
            Err(e) => {
                let _ = self.audit(com_port, "exit", None, Some(&e.to_string())).await;
                log::warn!(
                    "[eSIM][trace={}] exit failed on {}: {}",
                    trace_id,
                    com_port,
                    e
                );
            }
        }

        // Refresh the cached sim_id/ICCID now that the port is back in Normal
        // mode, instead of waiting for the next hot-swap detection cycle, so the
        // port list reflects a profile switch made during this eSIM session.
        if result.is_ok() {
            if let Some(modem) = self.manager.get_modem_by_com_port(com_port).await {
                if let Err(e) = modem.init_sim_info().await {
                    log::warn!(
                        "[eSIM][trace={}] failed to refresh SIM info on {} after exit: {}",
                        trace_id,
                        com_port,
                        e
                    );
                } else if let Some(new_sim_id) = modem.sim_id.read().await.clone() {
                    // init_sim_info() only writes to the DB; also refresh the
                    // ModemManager's in-memory sim_cards_cache (used by the SIM
                    // dashboard), otherwise it keeps serving the pre-switch
                    // ICCID/IMSI until the next full cache rebuild/restart.
                    self.manager.refresh_sim_cache(&[new_sim_id]).await;
                }
            }
        }

        result.map_err(|e| EsimError::At(e.to_string()))
    }

    /// Abnormal-state recovery (`ath0`). Releases the gate if held by this port.
    pub async fn reset(&self, com_port: &str) -> Result<(), EsimError> {
        self.ensure_enabled()?;
        let trace_id = self
            .session_trace(com_port)
            .await
            .unwrap_or_else(Self::new_trace_id);
        log::info!("[eSIM][trace={}] reset requested on {}", trace_id, com_port);
        let modem = self.get_modem(com_port).await?;
        let result = modem.esim_reset().await;
        {
            let mut active = self.active.lock().await;
            if active.as_deref() == Some(com_port) {
                *active = None;
            }
        }
        self.clear_session_trace(com_port).await;
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
        if let Ok(downloaded) = &result {
            if let Some(iccid) = self.resolve_downloaded_iccid(com_port, downloaded).await {
                let _ = self
                    .persist_download_phone_number(&iccid, req.phone_number.as_deref(), com_port)
                    .await;
            }
        }
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
        let trace_id = self
            .session_trace(com_port)
            .await
            .unwrap_or_else(Self::new_trace_id);
        log::info!(
            "[eSIM][trace={}] enable requested on {} for {}",
            trace_id,
            com_port,
            iccid
        );
        let mut result = self.lpac.profile_enable(iccid, refresh).await;
        if let Err(err) = &result {
            let orig_error = err.to_string();
            // Some modems/lpac builds return an error even when the profile has already switched.
            if let Ok(list) = self.lpac.profile_list().await {
                let enabled = list
                    .as_array()
                    .map(|arr| {
                        arr.iter().any(|p| {
                            Self::profile_matches_iccid(p, iccid) && Self::profile_is_enabled(p)
                        })
                    })
                    .unwrap_or(false);
                if enabled {
                    log::warn!(
                        "[eSIM][trace={}] enable on {} for {} normalized to success after readback: {}",
                        trace_id,
                        com_port,
                        iccid,
                        orig_error
                    );
                    result = Ok(json!({
                        "iccid": iccid,
                        "state": "enabled",
                        "normalized": true,
                        "normalized_reason": orig_error
                    }));
                } else {
                    log::warn!(
                        "[eSIM][trace={}] enable failed on {} for {} and readback did not confirm activation: {}",
                        trace_id,
                        com_port,
                        iccid,
                        orig_error
                    );
                }
            } else {
                log::warn!(
                    "[eSIM][trace={}] enable failed on {} for {} and profile readback failed: {}",
                    trace_id,
                    com_port,
                    iccid,
                    orig_error
                );
            }
        }
        self.audit_result(com_port, "enable", Some(iccid), &result).await;
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
        let trace_id = self
            .session_trace(com_port)
            .await
            .unwrap_or_else(Self::new_trace_id);
        log::info!(
            "[eSIM][trace={}] disable requested on {} for {}",
            trace_id,
            com_port,
            iccid
        );
        let mut result = self.lpac.profile_disable(iccid, refresh).await;
        if let Err(err) = &result {
            let orig_error = err.to_string();
            // Some modems/lpac builds return an error even when the profile state changed.
            if let Ok(list) = self.lpac.profile_list().await {
                let disabled = list
                    .as_array()
                    .map(|arr| {
                        arr.iter().any(|p| {
                            Self::profile_matches_iccid(p, iccid) && Self::profile_is_disabled(p)
                        })
                    })
                    .unwrap_or(false);
                if disabled {
                    log::warn!(
                        "[eSIM][trace={}] disable on {} for {} normalized to success after readback: {}",
                        trace_id,
                        com_port,
                        iccid,
                        orig_error
                    );
                    result = Ok(json!({
                        "iccid": iccid,
                        "state": "disabled",
                        "normalized": true,
                        "normalized_reason": orig_error
                    }));
                } else {
                    log::warn!(
                        "[eSIM][trace={}] disable failed on {} for {} and readback did not confirm deactivation: {}",
                        trace_id,
                        com_port,
                        iccid,
                        orig_error
                    );
                }
            } else {
                log::warn!(
                    "[eSIM][trace={}] disable failed on {} for {} and profile readback failed: {}",
                    trace_id,
                    com_port,
                    iccid,
                    orig_error
                );
            }
        }
        self.audit_result(com_port, "disable", Some(iccid), &result).await;
        result
    }

    pub async fn delete(&self, com_port: &str, iccid: &str) -> Result<Value, EsimError> {
        self.ensure_enabled()?;
        self.ensure_session(com_port).await?;
        let trace_id = self
            .session_trace(com_port)
            .await
            .unwrap_or_else(Self::new_trace_id);
        log::info!(
            "[eSIM][trace={}] delete requested on {} for {}",
            trace_id,
            com_port,
            iccid
        );
        let mut result = self.lpac.profile_delete(iccid).await;
        if let Err(err) = &result {
            let orig_error = err.to_string();
            // Some modems/lpac builds report a failure even though delete already took effect.
            if let Ok(list) = self.lpac.profile_list().await {
                let exists = list
                    .as_array()
                    .map(|arr| arr.iter().any(|p| Self::profile_matches_iccid(p, iccid)))
                    .unwrap_or(false);
                if !exists {
                    log::warn!(
                        "[eSIM][trace={}] delete on {} for {} normalized to success after readback: {}",
                        trace_id,
                        com_port,
                        iccid,
                        orig_error
                    );
                    result = Ok(json!({
                        "iccid": iccid,
                        "state": "deleted",
                        "normalized": true,
                        "normalized_reason": orig_error
                    }));
                } else {
                    log::warn!(
                        "[eSIM][trace={}] delete failed on {} for {} and readback still finds profile: {}",
                        trace_id,
                        com_port,
                        iccid,
                        orig_error
                    );
                }
            } else {
                log::warn!(
                    "[eSIM][trace={}] delete failed on {} for {} and profile readback failed: {}",
                    trace_id,
                    com_port,
                    iccid,
                    orig_error
                );
            }
        }
        self.audit_result(com_port, "delete", Some(iccid), &result).await;
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
        self.cleanup_session(com_port).await;
        outcome
    }

    async fn cleanup_session(&self, com_port: &str) {
        if let Err(e) = self.exit(com_port).await {
            log::warn!(
                "[eSIM] exit failed on {}: {}. Falling back to ath0 reset.",
                com_port,
                e
            );
            let _ = self.reset(com_port).await;
        }
    }

    async fn provision_inner(
        &self,
        com_port: &str,
        req: &DownloadRequest,
    ) -> Result<Value, EsimError> {
        let downloaded = self.download(com_port, req).await?;

        // Best-effort: enable the freshly-downloaded profile if we can find its ICCID.
        // Some lpac builds return success without an ICCID field, so fall back to
        // querying current profile list in-session.
        if let Some(iccid) = self
            .resolve_downloaded_iccid(com_port, &downloaded)
            .await
        {
            let _ = self
                .persist_download_phone_number(&iccid, req.phone_number.as_deref(), com_port)
                .await;
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

    async fn resolve_downloaded_iccid(&self, com_port: &str, downloaded: &Value) -> Option<String> {
        downloaded
            .get("iccid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or(self.current_iccid(com_port).await.ok().flatten())
    }

    fn normalized_phone_number(raw: Option<&str>) -> Option<String> {
        raw.map(normalize_msisdn).filter(|p| !p.is_empty())
    }

    async fn persist_download_phone_number(
        &self,
        iccid: &str,
        raw_phone_number: Option<&str>,
        com_port: &str,
    ) -> Result<(), EsimError> {
        let Some(phone_number) = Self::normalized_phone_number(raw_phone_number) else {
            return Ok(());
        };

        match crate::db::SimCard::find_by_conditions(Some(iccid), None, None, None).await {
            Ok(mut cards) if !cards.is_empty() => {
                let mut card = cards.remove(0);
                if card.phone_number.as_deref() != Some(phone_number.as_str()) {
                    card.update_phone_number(Some(phone_number.clone()))
                        .await
                        .map_err(|e| EsimError::Spawn(e.to_string()))?;
                    log::info!(
                        "[eSIM] persisted downloaded phone number for {} on {}: {}",
                        iccid,
                        com_port,
                        phone_number
                    );
                }
            }
            Ok(_) => {
                log::warn!(
                    "[eSIM] download on {} resolved ICCID {} but no sim_cards row was found for phone number persistence",
                    com_port,
                    iccid
                );
            }
            Err(e) => {
                log::warn!(
                    "[eSIM] failed to look up sim_cards row for {} on {}: {}",
                    iccid,
                    com_port,
                    e
                );
            }
        }

        Ok(())
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
        let trace_id = self
            .session_trace(com_port)
            .await
            .unwrap_or_else(Self::new_trace_id);
        match result {
            Ok(_) => {
                log::info!(
                    "[eSIM][trace={}] op={} port={} success (params_present={})",
                    trace_id,
                    op,
                    com_port,
                    params.is_some()
                );
            }
            Err(e) => {
                log::warn!(
                    "[eSIM][trace={}] op={} port={} failed: {} (params_present={})",
                    trace_id,
                    op,
                    com_port,
                    e,
                    params.is_some()
                );
            }
        }
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
        let mut result = self
            .batch_item_inner(op, item, auto_enable, replace)
            .await;
        if Self::is_transient_lpac_readiness_error(&result) {
            // Some readers appear a little late after mode switch; retry once before exiting.
            let retry_wait_ms = self.reader_settle_ms.saturating_mul(2).max(5000);
            tokio::time::sleep(Duration::from_millis(retry_wait_ms)).await;
            result = self
                .batch_item_inner(op, item, auto_enable, replace)
                .await;
        }
        self.cleanup_session(&item.com_port).await;
        result
    }

    fn is_transient_lpac_readiness_error(result: &Result<Option<String>, EsimError>) -> bool {
        let msg = match result {
            Err(EsimError::Spawn(m)) => m.to_ascii_lowercase(),
            Err(EsimError::Lpac { message, .. }) => message.to_ascii_lowercase(),
            _ => return false,
        };
        msg.contains("reader")
            || msg.contains("smart card")
            || msg.contains("pc/sc")
            || msg.contains("pcsc")
            || msg.contains("card not present")
            || msg.contains("no card")
            || msg.contains("euicc_init")
            || msg.contains("timeout")
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
                    phone_number: item.phone_number.clone(),
                    activation_code: item.activation_code.clone(),
                };
                let downloaded = self.download(com, &req).await?;
                let iccid = self.resolve_downloaded_iccid(com, &downloaded).await;
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
