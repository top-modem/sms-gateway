use serde_json::Value;
use tokio::process::Command;

use super::error::EsimError;

/// Thin wrapper around the external `lpac` CLI (SGP.22 LPA). All lpac output is
/// line-delimited JSON of the form
/// `{"type":"lpa","payload":{"code":0,"message":"success","data":...}}`.
/// We spawn lpac with the PC/SC APDU backend (the serial port is kept by our
/// own modem handle) and parse the final `type=="lpa"` envelope.
#[derive(Clone)]
pub struct Lpac {
    exe: String,
    apdu_backend: String,
    http_backend: String,
    reader_name: Option<String>,
}

impl Lpac {
    pub fn new(
        exe: String,
        apdu_backend: String,
        http_backend: String,
        reader_name: Option<String>,
    ) -> Self {
        Self {
            exe,
            apdu_backend,
            http_backend,
            reader_name,
        }
    }

    /// Run an lpac subcommand and return its `payload.data` on success.
    async fn run(&self, args: &[&str]) -> Result<Value, EsimError> {
        let primary = self.run_with_http_backend(args, &self.http_backend).await;
        if !Self::is_http_driver_missing(&primary) {
            return primary;
        }

        // Some lpac builds only include one HTTP backend. Retry with the common fallback.
        let fallback = if self.http_backend.eq_ignore_ascii_case("winhttp") {
            "curl"
        } else {
            "winhttp"
        };
        self.run_with_http_backend(args, fallback).await
    }

    async fn run_with_http_backend(
        &self,
        args: &[&str],
        http_backend: &str,
    ) -> Result<Value, EsimError> {
        let mut cmd = Command::new(&self.exe);
        cmd.args(args);
        cmd.env("LPAC_APDU", &self.apdu_backend);
        cmd.env("LPAC_HTTP", http_backend);
        if let Some(name) = &self.reader_name {
            if !name.is_empty() {
                cmd.env("LPAC_APDU_PCSC_DRV_NAME", name);
            }
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| EsimError::Spawn(e.to_string()))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut last_lpa: Option<Value> = None;
        for line in stdout.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<Value>(line) {
                if v.get("type").and_then(|t| t.as_str()) == Some("lpa") {
                    last_lpa = Some(v);
                }
            }
        }

        let env = last_lpa.ok_or_else(|| EsimError::Lpac {
            code: -1,
            message: format!(
                "no lpa output from lpac (stderr: {})",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        })?;

        let payload = env.get("payload").cloned().unwrap_or(Value::Null);
        let code = payload.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        if code != 0 {
            let message = payload
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown lpac error")
                .to_string();
            return Err(EsimError::Lpac { code, message });
        }
        Ok(payload.get("data").cloned().unwrap_or(Value::Null))
    }

    fn is_http_driver_missing(result: &Result<Value, EsimError>) -> bool {
        let msg = match result {
            Err(EsimError::Lpac { message, .. }) => message.to_ascii_lowercase(),
            _ => return false,
        };
        msg.contains("no http driver found")
            || msg.contains("unknown http driver")
            || msg.contains("http driver")
    }

    /// `lpac chip info` — EID and default SM-DP+/SM-DS.
    pub async fn chip_info(&self) -> Result<Value, EsimError> {
        self.run(&["chip", "info"]).await
    }

    /// `lpac profile list`.
    pub async fn profile_list(&self) -> Result<Value, EsimError> {
        self.run(&["profile", "list"]).await
    }

    /// `lpac profile download -s <smdp> -m <matchingId> [-c <conf>] [-i <imei>] [-a <activation>]`.
    #[allow(clippy::too_many_arguments)]
    pub async fn profile_download(
        &self,
        smdp: Option<&str>,
        matching_id: Option<&str>,
        confirmation_code: Option<&str>,
        imei: Option<&str>,
        activation_code: Option<&str>,
    ) -> Result<Value, EsimError> {
        let mut args: Vec<String> = vec!["profile".into(), "download".into()];
        if let Some(s) = smdp {
            args.push("-s".into());
            args.push(s.into());
        }
        if let Some(m) = matching_id {
            args.push("-m".into());
            args.push(m.into());
        }
        if let Some(c) = confirmation_code {
            args.push("-c".into());
            args.push(c.into());
        }
        if let Some(i) = imei {
            args.push("-i".into());
            args.push(i.into());
        }
        if let Some(a) = activation_code {
            args.push("-a".into());
            args.push(a.into());
        }
        let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run(&refs).await
    }

    /// `lpac profile enable <ICCID/AID> [1/0]`.
    pub async fn profile_enable(&self, id: &str, refresh: bool) -> Result<Value, EsimError> {
        self.run(&["profile", "enable", id, if refresh { "1" } else { "0" }])
            .await
    }

    /// `lpac profile disable <ICCID/AID> [1/0]`.
    pub async fn profile_disable(&self, id: &str, refresh: bool) -> Result<Value, EsimError> {
        self.run(&["profile", "disable", id, if refresh { "1" } else { "0" }])
            .await
    }

    /// `lpac profile delete <ICCID>`.
    pub async fn profile_delete(&self, iccid: &str) -> Result<Value, EsimError> {
        self.run(&["profile", "delete", iccid]).await
    }

    /// `lpac profile nickname <ICCID> <name>`.
    pub async fn profile_nickname(&self, iccid: &str, name: &str) -> Result<Value, EsimError> {
        self.run(&["profile", "nickname", iccid, name]).await
    }

    /// `lpac notification list`.
    pub async fn notification_list(&self) -> Result<Value, EsimError> {
        self.run(&["notification", "list"]).await
    }

    /// `lpac notification process [-r] <seq>` — send a notification, optionally
    /// removing it afterwards.
    pub async fn notification_process(&self, seq: &str, remove: bool) -> Result<Value, EsimError> {
        if remove {
            self.run(&["notification", "process", "-r", seq]).await
        } else {
            self.run(&["notification", "process", seq]).await
        }
    }

    /// `lpac notification remove <seq>`.
    pub async fn notification_remove(&self, seq: &str) -> Result<Value, EsimError> {
        self.run(&["notification", "remove", seq]).await
    }
}
