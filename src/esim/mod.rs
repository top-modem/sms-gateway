pub mod error;
pub mod lpac;

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::Settings;
use crate::ModemManagerRef;

pub use error::EsimError;
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
}

impl EsimService {
    pub fn new(manager: ModemManagerRef, settings: &Settings) -> Self {
        let lpac = Lpac::new(
            settings
                .lpac_exe
                .clone()
                .unwrap_or_else(|| "lpac".to_string()),
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
        }
    }

    fn ensure_enabled(&self) -> Result<(), EsimError> {
        if self.enabled {
            Ok(())
        } else {
            Err(EsimError::Disabled)
        }
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
}
