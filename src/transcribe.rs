use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use uuid::Uuid;

fn hidden_command(exe: &str) -> Command {
    let mut cmd = Command::new(exe);
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.as_std_mut().creation_flags(CREATE_NO_WINDOW);
    }
    cmd
}

/// Recognition is Aliyun-only; obsolete saved keys (language, provider) are ignored.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct VoiceOtpSettings {
    pub enabled: bool,
    pub com_port: String,
    pub prompt_seconds: u64,
    pub code_seconds: u64,
    pub otp_length: usize,
    pub deadline_seconds: u64,
}

impl Default for VoiceOtpSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            com_port: "COM2".into(),
            prompt_seconds: 10,
            code_seconds: 10,
            otp_length: 6,
            deadline_seconds: 60,
        }
    }
}

impl VoiceOtpSettings {
    pub(crate) const MAX_PORTS: usize = 32;

    /// `com_port` holds a comma-separated list such as `COM2,COM5`.
    pub(crate) fn ports(&self) -> impl Iterator<Item = &str> {
        self.com_port.split(',').map(str::trim).filter(|port| !port.is_empty())
    }

    pub(crate) fn handles(&self, com_port: &str) -> bool {
        self.enabled && self.ports().any(|port| port == com_port)
    }

    pub(crate) fn answer_delay_for(&self, com_port: &str, normal_delay: u64) -> u64 {
        if self.handles(com_port) { 0 } else { normal_delay }
    }

    pub fn validate(&self) -> Result<()> {
        let ports: Vec<&str> = self.ports().collect();
        if ports.is_empty() || ports.len() > Self::MAX_PORTS {
            bail!("Select between 1 and {} COM ports", Self::MAX_PORTS);
        }
        let mut seen = std::collections::HashSet::new();
        for port in &ports {
            let number = port.strip_prefix("COM").and_then(|value| value.parse::<u16>().ok());
            if !number.is_some_and(|number| (1..=128).contains(&number) && *port == format!("COM{}", number)) {
                bail!("Each port must be between COM1 and COM128");
            }
            if !seen.insert(*port) {
                bail!("{} is listed more than once", port);
            }
        }
        if !(3..=30).contains(&self.prompt_seconds) || !(3..=30).contains(&self.code_seconds) {
            bail!("Each recording window must be between 3 and 30 seconds");
        }
        if !(4..=8).contains(&self.otp_length) {
            bail!("OTP length must be between 4 and 8 digits");
        }
        if !(30..=240).contains(&self.deadline_seconds)
            || self.deadline_seconds < self.prompt_seconds + self.code_seconds + 10
        {
            bail!("Call deadline must allow both recordings and transcription (30-240 seconds)");
        }
        Ok(())
    }

    pub async fn load() -> Result<Self> {
        let settings = match crate::db::AppSetting::get("voice_otp_local_settings").await? {
            Some(value) => serde_json::from_str(&value).context("Invalid saved voice OTP settings")?,
            None => Self::default(),
        };
        settings.validate()?;
        Ok(settings)
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct VoiceOtpProgress {
    pub state: String,
    pub provider: String,
    pub prompt_transcript: Option<String>,
    #[serde(default)]
    pub prompt_readings: Vec<char>,
    pub prompt_comparison: Option<String>,
    pub code_transcript: Option<String>,
    pub dtmf: Option<char>,
    pub otp: Option<String>,
    pub error: Option<String>,
}

pub(crate) fn resolve_local_file(configured: Option<&str>, bundled: &[&str]) -> Option<String> {
    if let Some(path) = configured.filter(|path| !path.trim().is_empty()) {
        if std::path::Path::new(path).is_file() {
            return Some(path.to_string());
        }
        if let Some(path) = std::env::var_os("PATH").and_then(|paths| {
            std::env::split_paths(&paths).map(|directory| directory.join(path)).find(|candidate| candidate.is_file())
        }) {
            return Some(path.to_string_lossy().into_owned());
        }
    }
    let roots = [std::env::current_dir().ok(), std::env::current_exe().ok().and_then(|path| path.parent().map(std::path::Path::to_path_buf))];
    for root in roots.into_iter().flatten() {
        for directory in root.ancestors().take(4) {
            for relative in bundled {
                let path = directory.join(relative);
                if path.is_file() {
                    return Some(path.to_string_lossy().into_owned());
                }
            }
        }
    }
    None
}

struct TempAudioFiles(Vec<PathBuf>);

impl Drop for TempAudioFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum VoicePrompt {
    PressDigit(char),
    DirectOtp(String),
}

fn spoken_digit(token: &str) -> Option<char> {
    match token {
        "zero" | "oh" | "零" | "〇" => Some('0'),
        "one" | "一" => Some('1'),
        "two" | "二" | "两" => Some('2'),
        "three" | "三" => Some('3'),
        "four" | "四" => Some('4'),
        "five" | "五" => Some('5'),
        "six" | "六" => Some('6'),
        "seven" | "七" => Some('7'),
        "eight" | "八" => Some('8'),
        "nine" | "九" => Some('9'),
        _ if token.len() == 1 => token.chars().next().filter(char::is_ascii_digit),
        _ => None,
    }
}

pub(crate) fn voice_prompt_readings(text: &str) -> Result<Vec<char>> {
    static PRESS: std::sync::LazyLock<fancy_regex::Regex> = std::sync::LazyLock::new(|| {
        fancy_regex::Regex::new(r#"(?i)(?:\bpress(?:\s+(?:the|number|digit|key))*\s+["'“‘「『]?(zero|one|two|three|four|five|six|seven|eight|nine|[0-9])\b["'”’」』]?|(?:请)?按(?:下|键)?\s*["'“‘「『]?([0-9零〇一二两三四五六七八九])["'”’」』]?(?![0-9零〇一二两三四五六七八九]))"#)
            .expect("valid voice prompt pattern")
    });
    let lower = text.to_lowercase();
    let matches = PRESS.captures_iter(&lower).collect::<std::result::Result<Vec<_>, _>>()?;
    let mut readings = Vec::new();
    for matched in &matches {
        let remainder = lower[matched.get(0).unwrap().end()..]
            .trim_start_matches(|character: char| character.is_whitespace() || character == ',');
        if ["or ", "and ", "或", "和"].iter().any(|prefix| remainder.starts_with(prefix)) {
            bail!("Prompt contains alternative or multiple digit choices");
        }
        if remainder.split_whitespace().next().is_some_and(|token| {
            let token = token.trim_matches(|character: char| character.is_ascii_punctuation()
                || matches!(character, '，' | '。' | '“' | '”' | '‘' | '’' | '「' | '」' | '『' | '』'));
            spoken_digit(token).is_some()
        })
            || remainder.chars().next().is_some_and(|character| spoken_digit(&character.to_string()).is_some())
        {
            bail!("Press instruction contains more than one digit");
        }
        let token = matched.get(1).or_else(|| matched.get(2)).unwrap().as_str();
        readings.push(spoken_digit(token).context("invalid DTMF digit")?);
    }
    Ok(readings)
}

pub(crate) fn parse_voice_prompt(text: &str, otp_length: usize) -> Result<VoicePrompt> {
    let readings = voice_prompt_readings(text)?;
    let digits: std::collections::BTreeSet<_> = readings.iter().copied().collect();
    if digits.len() == 1 {
        return Ok(VoicePrompt::PressDigit(digits.into_iter().next().unwrap()));
    }
    if !readings.is_empty() || text.to_lowercase().contains("press") || text.contains('按') {
        bail!("Prompt must identify one unambiguous single-digit press instruction");
    }
    Ok(VoicePrompt::DirectOtp(extract_voice_otp(text, otp_length)?))
}

/// Aliyun hears "WhatsApp" in phone audio as "west up", "whats的", "wifi app" and the like;
/// a short w-word directly before 验证码 is restored to the brand name.
pub(crate) fn normalize_voice_brand(text: &str) -> String {
    static BRAND: std::sync::LazyLock<fancy_regex::Regex> = std::sync::LazyLock::new(|| {
        fancy_regex::Regex::new(r"(?i)(?<![a-z'])w[a-z']{0,7}(?:\s?[a-z']{1,4})?\s*的?\s*[，,]?\s*(?=验证码)")
            .expect("valid brand pattern")
    });
    BRAND.replace_all(text, "WhatsApp").into_owned()
}

pub(crate) fn extract_voice_otp(text: &str, expected_length: usize) -> Result<String> {
    if !(4..=8).contains(&expected_length) {
        bail!("OTP length must be between 4 and 8 digits");
    }
    static RUN: std::sync::LazyLock<fancy_regex::Regex> = std::sync::LazyLock::new(|| {
        fancy_regex::Regex::new(r"(?i)(?<![A-Za-z0-9])(?:[0-9]+|zero\b|oh\b|one\b|two\b|three\b|four\b|five\b|six\b|seven\b|eight\b|nine\b|[零〇一二两三四五六七八九]+)(?:[\s,，-]+(?:[0-9]+|zero\b|oh\b|one\b|two\b|three\b|four\b|five\b|six\b|seven\b|eight\b|nine\b|[零〇一二两三四五六七八九]+))*(?![A-Za-z0-9])")
            .expect("valid voice OTP pattern")
    });
    let lower = text.to_lowercase();
    let mut candidates = std::collections::BTreeSet::new();
    for matched in RUN.find_iter(&lower) {
        let matched = matched?;
        let mut digits = String::new();
        for token in matched.as_str().split(|character: char| character.is_whitespace() || matches!(character, ',' | '，' | '-')) {
            if token.chars().all(|character| character.is_ascii_digit()) {
                digits.push_str(token);
            } else if let Some(digit) = spoken_digit(token) {
                digits.push(digit);
            } else {
                for character in token.chars() {
                    digits.push(spoken_digit(&character.to_string()).context("invalid spoken digit")?);
                }
            }
        }
        if digits.len() == expected_length {
            candidates.insert(digits);
        } else if digits.len() > expected_length {
            // The IVR repeats the code; ASR may merge a full reading with a partial repeat.
            let code: Vec<char> = digits.chars().take(expected_length).collect();
            if digits.chars().enumerate().all(|(index, digit)| digit == code[index % expected_length]) {
                candidates.insert(code.into_iter().collect());
            }
        }
    }
    if candidates.len() != 1 {
        bail!("Transcript does not contain one unambiguous {}-digit code", expected_length);
    }
    Ok(candidates.into_iter().next().unwrap())
}

/// Convert an AMR recording to mono 16-bit WAV at `sample_rate` for cloud recognition.
pub(crate) async fn convert_to_wav(amr_bytes: &[u8], ffmpeg_exe: &str, sample_rate: u32) -> Result<Vec<u8>> {
    convert_audio(amr_bytes, ffmpeg_exe, sample_rate, false).await
}

/// Convert an AMR recording to mono OGG-Opus: about 16x smaller than WAV for slow uplinks.
pub(crate) async fn convert_to_opus(amr_bytes: &[u8], ffmpeg_exe: &str, sample_rate: u32) -> Result<Vec<u8>> {
    convert_audio(amr_bytes, ffmpeg_exe, sample_rate, true).await
}

async fn convert_audio(amr_bytes: &[u8], ffmpeg_exe: &str, sample_rate: u32, opus: bool) -> Result<Vec<u8>> {
    if amr_bytes.is_empty() || amr_bytes.len() > 5 * 1024 * 1024 {
        bail!("Recording must contain between 1 byte and 5 MB of audio");
    }
    let id = Uuid::new_v4().to_string();
    let amr_path = std::env::temp_dir().join(format!("{}.amr", id));
    let out_path = std::env::temp_dir().join(format!("{}.{}", id, if opus { "ogg" } else { "wav" }));
    let _temp_files = TempAudioFiles(vec![amr_path.clone(), out_path.clone()]);
    tokio::fs::write(&amr_path, amr_bytes).await.context("failed to write temp AMR file")?;
    let rate = sample_rate.to_string();
    let codec: &[&str] = if opus {
        &["-c:a", "libopus", "-b:a", "16k", "-application", "voip", "-f", "ogg"]
    } else {
        &["-c:a", "pcm_s16le"]
    };
    let child = hidden_command(ffmpeg_exe)
        .args(["-y", "-i", amr_path.to_str().unwrap(), "-ar", &rate, "-ac", "1"])
        .args(codec)
        .arg(out_path.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to launch ffmpeg")?;
    let output = match tokio::time::timeout(Duration::from_secs(60), child.wait_with_output()).await {
        Ok(result) => result.context("ffmpeg error")?,
        Err(_) => bail!("ffmpeg timed out after 60s"),
    };
    if !output.status.success() {
        bail!("ffmpeg failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    tokio::fs::read(&out_path).await.context("failed to read converted audio")
}

#[cfg(test)]
mod voice_otp_tests {
    use super::*;

    #[test]
    fn voice_otp_immediate_answer_is_limited_to_the_enabled_port() {
        let mut settings = VoiceOtpSettings::default();
        assert_eq!(settings.answer_delay_for("COM2", 2), 2);
        settings.enabled = true;
        assert_eq!(settings.answer_delay_for("COM2", 2), 0);
        assert_eq!(settings.answer_delay_for("COM1", 2), 2);
        settings.enabled = false;
        assert_eq!(settings.answer_delay_for("COM2", 2), 2);
    }

    #[test]
    fn voice_otp_defaults_are_opt_in_and_windows_are_independent() {
        let mut settings = VoiceOtpSettings::default();
        assert!(!settings.enabled);
        assert_eq!(settings.com_port, "COM2");
        settings.prompt_seconds = 3;
        settings.code_seconds = 20;
        assert!(settings.validate().is_ok());
        settings.code_seconds = 31;
        assert!(settings.validate().is_err());
        settings.code_seconds = 10;
        settings.com_port = "COM2\r\nATH".into();
        assert!(settings.validate().is_err());
    }

    #[test]
    fn voice_otp_accepts_up_to_32_distinct_ports() {
        let mut settings = VoiceOtpSettings { enabled: true, com_port: "COM2, COM5".into(), ..Default::default() };
        assert!(settings.validate().is_ok());
        assert!(settings.handles("COM5") && settings.handles("COM2") && !settings.handles("COM3"));
        assert_eq!(settings.answer_delay_for("COM5", 2), 0);
        settings.com_port = (1..=32).map(|n| format!("COM{n}")).collect::<Vec<_>>().join(",");
        assert!(settings.validate().is_ok());
        settings.com_port.push_str(",COM33");
        assert!(settings.validate().is_err());
        for invalid in ["COM2,COM2", "", "COM2,,ATH", "COM02"] {
            settings.com_port = invalid.into();
            assert!(settings.validate().is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn voice_otp_accepts_explicit_press_instructions() {
        for prompt in ["To hear your code, press five.", "Please press the number 5.", "请按五收听验证码"] {
            assert_eq!(parse_voice_prompt(prompt, 6).unwrap(), VoicePrompt::PressDigit('5'));
        }
        for prompt in ["要接听whatsapp 验证码请按9", "要接聽 WhatsApp 驗證碼，請按九。", "要接听What's that? 验证吗?请按9要接听"] {
            assert_eq!(parse_voice_prompt(prompt, 6).unwrap(), VoicePrompt::PressDigit('9'));
        }
    }

    #[test]
    fn voice_otp_accepts_quoted_and_repeated_identical_instructions() {
        for prompt in [
            "接听我次的验证码请按\"9\"接听我次的验证码请按\"9\"",
            "请按“9”。请按“9”。",
            "To hear your code, press 'nine'. Please press 'nine'.",
        ] {
            assert_eq!(parse_voice_prompt(prompt, 6).unwrap(), VoicePrompt::PressDigit('9'));
        }
        assert_eq!(parse_voice_prompt("Press five. Press five.", 6).unwrap(), VoicePrompt::PressDigit('5'));
    }

    #[test]
    fn voice_otp_restores_whatsapp_before_the_code_word() {
        for (heard, expected) in [
            ("听说west up验证码，请按2。", "听说WhatsApp验证码，请按2。"),
            ("听说whats up，验证码请按2。", "听说WhatsApp验证码请按2。"),
            ("就说whats的验证码请按2。", "就说WhatsApp验证码请按2。"),
            ("接收wh a验证码，请按2。", "接收WhatsApp验证码，请按2。"),
            ("支收wifi up验证码，按一主要支撑wifi f验证码按一。", "支收WhatsApp验证码，按一主要支撑WhatsApp验证码按一。"),
            ("输出word p验证码请按2。", "输出WhatsApp验证码请按2。"),
        ] {
            assert_eq!(normalize_voice_brand(heard), expected);
        }
        for unchanged in ["您的验证码为951782。", "Telegram验证码请按2", "微信验证码"] {
            assert_eq!(normalize_voice_brand(unchanged), unchanged);
        }
    }

    #[test]
    fn voice_otp_compares_both_prompt_repetitions() {
        let prompt = "要接听WhatsApp验证码请按九。要接听WhatsApp验证码请按9。";
        assert_eq!(voice_prompt_readings(prompt).unwrap(), vec!['9', '9']);
        assert_eq!(parse_voice_prompt(prompt, 6).unwrap(), VoicePrompt::PressDigit('9'));
        assert!(parse_voice_prompt("请按9。请按6。", 6).is_err());
        assert_eq!(parse_voice_prompt("请按9。要接听WhatsApp验证码请按", 6).unwrap(), VoicePrompt::PressDigit('9'));
        assert!(parse_voice_prompt("噪声，没有完整按键指令", 6).is_err());
        assert_eq!(parse_voice_prompt("Your code is 005861.", 6).unwrap(), VoicePrompt::DirectOtp("005861".into()));
    }

    #[test]
    fn voice_otp_recognises_the_instructions_heard_in_the_user_prompt_clip() {
        // Aliyun results for the first 4-5 s of a real WhatsApp prompt.
        for text in ["接收wifi验证码按一。", "接收whaup验证码，按一主客。", "接收whaup验证码，按一点要支收。"] {
            assert_eq!(parse_voice_prompt(text, 6).unwrap(), VoicePrompt::PressDigit('1'), "{text}");
        }
        assert!(parse_voice_prompt("就是wifi up验证码。", 6).is_err());
    }

    #[test]
    fn voice_otp_rejects_ambiguous_or_guessed_dtmf() {
        for prompt in ["Press five or press six.", "Press five or six.", "请按五或六", "Press 56.", "Press 5 6.", "Press five six.", "请按\"9\"。请按\"6\"。", "Press star.", "Please wait.", "六傑廷 摩托艾驗證嗎請按九六傑廷"] {
            assert!(parse_voice_prompt(prompt, 6).is_err(), "unexpected instruction: {prompt}");
        }
    }

    #[test]
    fn voice_otp_preserves_leading_zeroes_and_spoken_digits() {
        for transcript in ["Your code is 005861.", "Your code is zero zero five eight six one.", "验证码是零零五八六一"] {
            assert_eq!(extract_voice_otp(transcript, 6).unwrap(), "005861");
        }
        assert_eq!(parse_voice_prompt("Your code is 58861.", 5).unwrap(), VoicePrompt::DirectOtp("58861".into()));
    }

    #[test]
    fn voice_otp_accepts_a_full_code_followed_by_its_repeat() {
        for transcript in ["557 632557", "557632 557632", "557632557632", "Your code is 557632. 557632."] {
            assert_eq!(extract_voice_otp(transcript, 6).unwrap(), "557632", "transcript: {transcript}");
        }
        for transcript in ["557632558", "557 632 991", "557632 123456"] {
            assert!(extract_voice_otp(transcript, 6).is_err(), "unexpected code: {transcript}");
        }
    }

    #[test]
    fn voice_otp_does_not_join_unrelated_groups_or_truncate_codes() {
        for transcript in ["Your code is 123. Another number is 456.", "Codes 123456 and 654321.", "Your code is 1234567.", "Silence."] {
            assert!(extract_voice_otp(transcript, 6).is_err(), "unexpected code: {transcript}");
        }
    }
}
