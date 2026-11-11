use anyhow::{bail, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use std::time::Duration;
use tokio::sync::Mutex;

const SETTING_KEY: &str = "aliyun_asr_settings";
const REGIONS: [&str; 4] = ["cn-shanghai", "cn-beijing", "cn-shenzhen", "ap-southeast-1"];

/// CreateToken endpoint, API version and RegionId: mainland regions share Shanghai; Singapore is separate.
fn token_service(region: &str) -> (&'static str, &'static str, &'static str) {
    if region == "ap-southeast-1" {
        ("https://nlsmeta.ap-southeast-1.aliyuncs.com/", "2019-07-17", "ap-southeast-1")
    } else {
        ("https://nls-meta.cn-shanghai.aliyuncs.com/", "2019-02-28", "cn-shanghai")
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct AliyunAsrSettings {
    pub app_key: String,
    pub token: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub region: String,
    pub sample_rate: u32,
}

/// Secret-free view returned to the browser.
#[derive(Debug, serde::Serialize)]
pub(crate) struct AliyunAsrPublic {
    pub app_key: String,
    pub region: String,
    pub sample_rate: u32,
    pub access_key_id: String,
    pub has_token: bool,
    pub has_access_key_secret: bool,
    pub ready: bool,
}

/// Browser update: `None` keeps a saved secret, an empty string clears it.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AliyunAsrUpdate {
    pub app_key: String,
    pub region: String,
    pub sample_rate: u32,
    pub access_key_id: String,
    pub token: Option<String>,
    pub access_key_secret: Option<String>,
}

static TOKEN_CACHE: Mutex<Option<(String, i64)>> = Mutex::const_new(None);

impl AliyunAsrSettings {
    pub async fn load() -> Result<Self> {
        let mut settings: Self = match crate::db::AppSetting::get(SETTING_KEY).await? {
            Some(value) => serde_json::from_str(&value).context("Invalid saved Aliyun ASR settings")?,
            None => Self::default(),
        };
        if settings.region.is_empty() { settings.region = REGIONS[0].into(); }
        if settings.sample_rate == 0 { settings.sample_rate = 16000; }
        Ok(settings)
    }

    pub fn ready(&self) -> bool {
        !self.app_key.is_empty()
            && (!self.token.is_empty() || (!self.access_key_id.is_empty() && !self.access_key_secret.is_empty()))
    }

    pub fn public(&self) -> AliyunAsrPublic {
        AliyunAsrPublic {
            app_key: self.app_key.clone(),
            region: self.region.clone(),
            sample_rate: self.sample_rate,
            access_key_id: self.access_key_id.clone(),
            has_token: !self.token.is_empty(),
            has_access_key_secret: !self.access_key_secret.is_empty(),
            ready: self.ready(),
        }
    }

    pub async fn apply(update: AliyunAsrUpdate) -> Result<Self> {
        let mut settings = Self::load().await?;
        let field = |value: &str| -> Result<String> {
            let value = value.trim();
            if value.len() > 128 || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
                bail!("Aliyun credentials may only contain letters, digits, '-' and '_'");
            }
            Ok(value.to_string())
        };
        if !REGIONS.contains(&update.region.as_str()) {
            bail!("Region must be cn-shanghai, cn-beijing, cn-shenzhen or ap-southeast-1");
        }
        if !matches!(update.sample_rate, 8000 | 16000) {
            bail!("Sample rate must be 8000 or 16000");
        }
        settings.app_key = field(&update.app_key)?;
        settings.access_key_id = field(&update.access_key_id)?;
        settings.region = update.region;
        settings.sample_rate = update.sample_rate;
        if let Some(token) = update.token { settings.token = field(&token)?; }
        if let Some(secret) = update.access_key_secret { settings.access_key_secret = field(&secret)?; }
        crate::db::AppSetting::set(SETTING_KEY, Some(&serde_json::to_string(&settings)?)).await?;
        *TOKEN_CACHE.lock().await = None;
        Ok(settings)
    }
}

fn client() -> Result<reqwest::Client> {
    // Shared so the TLS connection to the gateway stays warm between calls.
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client.clone());
    }
    // Drop idle pooled connections before the gateway closes them, so a reused socket is never stale.
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .pool_idle_timeout(Duration::from_secs(20))
        .build()?;
    Ok(CLIENT.get_or_init(|| client).clone())
}

fn pop_signature(secret: &str, canonical_query: &str) -> String {
    let string_to_sign = format!("GET&%2F&{}", urlencoding::encode(canonical_query));
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(format!("{}&", secret).as_bytes()).expect("HMAC accepts any key length");
    mac.update(string_to_sign.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn canonical_query(params: &std::collections::BTreeMap<&str, String>) -> String {
    params.iter().map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))).collect::<Vec<_>>().join("&")
}

async fn access_token(settings: &AliyunAsrSettings) -> Result<String> {
    if settings.access_key_id.is_empty() || settings.access_key_secret.is_empty() {
        return Ok(settings.token.clone());
    }
    let now = chrono::Utc::now().timestamp();
    let mut cache = TOKEN_CACHE.lock().await;
    if let Some((token, expires)) = cache.as_ref() {
        if *expires - 300 > now { return Ok(token.clone()); }
    }
    let (endpoint, version, region_id) = token_service(&settings.region);
    let params: std::collections::BTreeMap<&str, String> = [
        ("AccessKeyId", settings.access_key_id.clone()),
        ("Action", "CreateToken".into()),
        ("Format", "JSON".into()),
        ("RegionId", region_id.into()),
        ("SignatureMethod", "HMAC-SHA1".into()),
        ("SignatureNonce", uuid::Uuid::new_v4().to_string()),
        ("SignatureVersion", "1.0".into()),
        ("Timestamp", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        ("Version", version.into()),
    ].into_iter().collect();
    let query = canonical_query(&params);
    let url = format!("{}?{}&Signature={}",
        endpoint, query, urlencoding::encode(&pop_signature(&settings.access_key_secret, &query)));
    let response = client()?.get(url).send().await.map_err(reqwest::Error::without_url)
        .context("Failed to request Aliyun access token")?;
    let status = response.status();
    let body: serde_json::Value = response.json().await.map_err(reqwest::Error::without_url)
        .context("Invalid Aliyun token response")?;
    let token = body.pointer("/Token/Id").and_then(|v| v.as_str());
    let expires = body.pointer("/Token/ExpireTime").and_then(|v| v.as_i64());
    let (Some(token), Some(expires)) = (token, expires) else {
        bail!("Aliyun CreateToken failed (HTTP {}): {}", status,
            body.get("Code").and_then(|v| v.as_str()).unwrap_or("unknown error"));
    };
    *cache = Some((token.to_string(), expires));
    Ok(token.to_string())
}

/// Recognise a short (<60 s) recording with the Aliyun 一句话识别 RESTful API.
pub(crate) async fn transcribe(amr_bytes: &[u8], ffmpeg_exe: &str) -> Result<String> {
    let settings = AliyunAsrSettings::load().await?;
    if !settings.ready() {
        bail!("Aliyun ASR is not configured (AppKey and token or AccessKey required)");
    }
    let started = std::time::Instant::now();
    let (audio, format) = match crate::transcribe::convert_to_opus(amr_bytes, ffmpeg_exe, settings.sample_rate).await {
        Ok(audio) => (audio, "opus"),
        Err(error) => {
            log::warn!("Opus conversion failed, sending WAV instead: {:#}", error);
            (crate::transcribe::convert_to_wav(amr_bytes, ffmpeg_exe, settings.sample_rate).await?, "wav")
        }
    };
    let converted_ms = started.elapsed().as_millis();
    let size = audio.len();
    let text = recognize_format(&settings, audio, format).await?;
    log::info!("Aliyun ASR timing: ffmpeg {} ms, upload+recognition {} ms ({} {} bytes)",
        converted_ms, started.elapsed().as_millis() - converted_ms, format, size);
    Ok(text)
}

pub(crate) async fn recognize(settings: &AliyunAsrSettings, wav: Vec<u8>) -> Result<String> {
    recognize_format(settings, wav, "wav").await
}

async fn recognize_format(settings: &AliyunAsrSettings, audio: Vec<u8>, format: &str) -> Result<String> {
    let token = access_token(settings).await?;
    let url = format!(
        "https://nls-gateway-{}.aliyuncs.com/stream/v1/asr?appkey={}&format={}&sample_rate={}&enable_punctuation_prediction=true&enable_inverse_text_normalization=true",
        settings.region, urlencoding::encode(&settings.app_key), format, settings.sample_rate);
    let mut attempt = 0;
    let response = loop {
        attempt += 1;
        let sent = client()?.post(&url)
            .header("X-NLS-Token", &token)
            .header("Content-Type", "application/octet-stream")
            .body(audio.clone())
            .send().await.map_err(reqwest::Error::without_url);
        match sent {
            Ok(response) => break response,
            Err(error) if attempt < 2 && !error.is_timeout() => {
                log::warn!("Aliyun request failed ({}), retrying once", error);
            }
            Err(error) => return Err(anyhow::Error::new(error).context("Failed to reach Aliyun speech recognition")),
        }
    };
    let body: serde_json::Value = response.json().await.map_err(reqwest::Error::without_url)
        .context("Invalid Aliyun recognition response")?;
    let status = body.get("status").and_then(|v| v.as_i64()).unwrap_or_default();
    let task_id = body.get("task_id").and_then(|v| v.as_str()).unwrap_or("-");
    if status == 40000001 {
        *TOKEN_CACHE.lock().await = None;
        bail!("Aliyun token is invalid or expired (task_id {}): enter a new token or an AccessKey ID/Secret for automatic renewal", task_id);
    }
    if status != 20000000 {
        let message = body.get("message").and_then(|v| v.as_str()).unwrap_or("").replace(token.as_str(), "***");
        bail!("Aliyun ASR error {} (task_id {}): {}", status, task_id, message);
    }
    log::debug!("Aliyun ASR ok: region={} task_id={} attempts={}", settings.region, task_id, attempt);
    Ok(body.get("result").and_then(|v| v.as_str()).unwrap_or("").trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_signature_matches_aliyun_documented_example() {
        let params: std::collections::BTreeMap<&str, String> = [
            ("AccessKeyId", "my_access_key_id"), ("Action", "CreateToken"), ("Format", "JSON"),
            ("RegionId", "cn-shanghai"), ("SignatureMethod", "HMAC-SHA1"),
            ("SignatureNonce", "b924c8c3-6d03-4c5d-ad36-d984d3116788"), ("SignatureVersion", "1.0"),
            ("Timestamp", "2019-04-18T08:32:31Z"), ("Version", "2019-02-28"),
        ].into_iter().map(|(k, v)| (k, v.to_string())).collect();
        let query = canonical_query(&params);
        assert_eq!(query, "AccessKeyId=my_access_key_id&Action=CreateToken&Format=JSON&RegionId=cn-shanghai&SignatureMethod=HMAC-SHA1&SignatureNonce=b924c8c3-6d03-4c5d-ad36-d984d3116788&SignatureVersion=1.0&Timestamp=2019-04-18T08%3A32%3A31Z&Version=2019-02-28");
        assert_eq!(pop_signature("my_access_key_secret", &query), "hHq4yNsPitlfDJ2L0nQPdugdEzM=");
    }

    #[test]
    fn singapore_token_request_matches_international_documented_signature() {
        let (endpoint, version, region_id) = token_service("ap-southeast-1");
        assert_eq!(endpoint, "https://nlsmeta.ap-southeast-1.aliyuncs.com/");
        let params: std::collections::BTreeMap<&str, String> = [
            ("AccessKeyId", "my_access_key_id"), ("Action", "CreateToken"), ("Format", "JSON"),
            ("RegionId", region_id), ("SignatureMethod", "HMAC-SHA1"),
            ("SignatureNonce", "b924c8c3-6d03-4c5d-ad36-d984d3116788"), ("SignatureVersion", "1.0"),
            ("Timestamp", "2019-04-18T08:32:31Z"), ("Version", version),
        ].into_iter().map(|(k, v)| (k, v.to_string())).collect();
        assert_eq!(pop_signature("my_access_key_secret", &canonical_query(&params)), "4TdrOuLHXJU+5C6s927Wkq2h+wc=");
        assert_eq!(token_service("cn-beijing").2, "cn-shanghai");
    }

    #[test]
    fn readiness_requires_app_key_and_a_credential() {
        let mut settings = AliyunAsrSettings { app_key: "abc".into(), ..Default::default() };
        assert!(!settings.ready());
        settings.token = "t".into();
        assert!(settings.ready());
        settings.token.clear();
        settings.access_key_id = "id".into();
        assert!(!settings.ready());
        settings.access_key_secret = "s".into();
        assert!(settings.ready());
        assert!(!settings.public().has_token);
    }
}
