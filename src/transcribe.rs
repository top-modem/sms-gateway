use anyhow::{bail, Context, Result};
use log::{debug, error};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct VoiceOtpSettings {
    pub enabled: bool,
    pub com_port: String,
    pub prompt_seconds: u64,
    pub code_seconds: u64,
    pub otp_length: usize,
    pub language: String,
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
            language: "auto".into(),
            deadline_seconds: 120,
        }
    }
}

impl VoiceOtpSettings {
    pub(crate) fn answer_delay_for(&self, com_port: &str, normal_delay: u64) -> u64 {
        if self.enabled && self.com_port == com_port { 0 } else { normal_delay }
    }

    pub fn validate(&self) -> Result<()> {
        let port_number = self.com_port.strip_prefix("COM").and_then(|value| value.parse::<u16>().ok());
        if !port_number.is_some_and(|number| (1..=128).contains(&number) && self.com_port == format!("COM{}", number)) {
            bail!("Select a COM port between COM1 and COM128");
        }
        if !(3..=30).contains(&self.prompt_seconds) || !(3..=30).contains(&self.code_seconds) {
            bail!("Each recording window must be between 3 and 30 seconds");
        }
        if !(4..=8).contains(&self.otp_length) {
            bail!("OTP length must be between 4 and 8 digits");
        }
        if !matches!(self.language.as_str(), "auto" | "en" | "zh") {
            bail!("Voice language must be auto, en or zh");
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

pub(crate) fn compare_repeated_voice_prompt(text: &str, otp_length: usize) -> Result<VoicePrompt> {
    let instruction = parse_voice_prompt(text, otp_length)?;
    if matches!(instruction, VoicePrompt::PressDigit(_)) && voice_prompt_readings(text)?.len() < 2 {
        bail!("Two clear matching prompt repetitions are required before sending DTMF");
    }
    Ok(instruction)
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

/// Convert an AMR audio blob to text using a local whisper.cpp installation.
///
/// Steps:
/// 1. Write `amr_bytes` to `<tmpdir>/<id>.amr`
/// 2. Run ffmpeg to convert to 16 kHz mono WAV: `<tmpdir>/<id>.wav`
/// 3. Run whisper-cli with `--output-txt` (language auto-detected or forced)
/// 4. Read the generated `<tmpdir>/<id>.wav.txt` transcript
/// 5. Delete all temp files
///
/// `language`: language code ("en", "zh", "ja", etc.) or "auto" for auto-detect.
/// Returns the trimmed transcript string on success.
pub async fn transcribe(
    amr_bytes: &[u8],
    ffmpeg_exe: &str,
    whisper_exe: &str,
    whisper_model: &str,
    language: &str,
) -> Result<String> {
    static WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
    let _worker = WORKERS.acquire().await.context("Local transcription worker unavailable")?;
    if amr_bytes.is_empty() || amr_bytes.len() > 5 * 1024 * 1024 {
        bail!("Recording must contain between 1 byte and 5 MB of audio");
    }
    let tmp = std::env::temp_dir();
    let id = Uuid::new_v4().to_string();

    let amr_path: PathBuf = tmp.join(format!("{}.amr", id));
    let wav_path: PathBuf = tmp.join(format!("{}.wav", id));
    // whisper --output-txt writes to <-of value>.txt
    let txt_path: PathBuf = tmp.join(format!("{}.txt", id));
    let _temp_files = TempAudioFiles(vec![amr_path.clone(), wav_path.clone(), txt_path.clone()]);

    // ── Step 1: Write AMR bytes ───────────────────────────────────────────────
    tokio::fs::write(&amr_path, amr_bytes)
        .await
        .context("failed to write temp AMR file")?;

    // ── Step 2: ffmpeg AMR → 16 kHz mono WAV ─────────────────────────────────
    let ffmpeg_child = Command::new(ffmpeg_exe)
        .args([
            "-y", // overwrite output without asking
            "-i",
            amr_path.to_str().unwrap(),
            "-ar",
            "16000", // 16 kHz sample rate (required by Whisper)
            "-ac",
            "1", // mono
            wav_path.to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to launch ffmpeg (is it installed and on PATH?)")?;

    let ffmpeg_out = match tokio::time::timeout(
        Duration::from_secs(60),
        ffmpeg_child.wait_with_output(),
    )
    .await
    {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            cleanup(&[&amr_path, &wav_path, &txt_path]).await;
            bail!("ffmpeg error: {}", e);
        }
        Err(_) => {
            cleanup(&[&amr_path, &wav_path, &txt_path]).await;
            bail!("ffmpeg timed out after 60s");
        }
    };

    if !ffmpeg_out.status.success() {
        let stderr = String::from_utf8_lossy(&ffmpeg_out.stderr);
        cleanup(&[&amr_path, &wav_path, &txt_path]).await;
        bail!("ffmpeg failed: {}", stderr.trim());
    }

    debug!("transcribe: ffmpeg ok, wav={}", wav_path.display());

    // ── Step 3: whisper-cli transcription ─────────────────────────────────────
    let output_prefix = txt_path.with_extension("");
    let output_prefix_str = output_prefix.to_str().unwrap();
    let wav_str = wav_path.to_str().unwrap();
    let mut whisper_args: Vec<&str> = vec![
        "-m",
        whisper_model,
        "-f",
        wav_str,
        "--output-txt", // write transcript to <-of>.txt
        "-nt",          // no timestamps in output
        "-of",
        output_prefix_str,
    ];
    whisper_args.push("-l");
    whisper_args.push(language);
    let whisper_child = Command::new(whisper_exe)
        .args(&whisper_args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to launch whisper-cli (is it installed?)")?;

    let whisper_out = match tokio::time::timeout(
        Duration::from_secs(120),
        whisper_child.wait_with_output(),
    )
    .await
    {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            cleanup(&[&amr_path, &wav_path, &txt_path]).await;
            bail!("whisper-cli error: {}", e);
        }
        Err(_) => {
            cleanup(&[&amr_path, &wav_path, &txt_path]).await;
            bail!("whisper-cli timed out after 120s");
        }
    };

    if !whisper_out.status.success() {
        let stderr = String::from_utf8_lossy(&whisper_out.stderr);
        cleanup(&[&amr_path, &wav_path, &txt_path]).await;
        bail!("whisper-cli failed: {}", stderr.trim());
    }

    debug!("transcribe: whisper ok, reading {}", txt_path.display());

    // ── Step 4: Read transcript ───────────────────────────────────────────────
    let transcript = tokio::fs::read_to_string(&txt_path)
        .await
        .context("failed to read whisper transcript file")?;

    // ── Step 5: Cleanup ───────────────────────────────────────────────────────
    cleanup(&[&amr_path, &wav_path, &txt_path]).await;

    Ok(transcript.trim().to_string())
}

async fn cleanup(paths: &[&PathBuf]) {
    for p in paths {
        if let Err(e) = tokio::fs::remove_file(p).await {
            // Non-fatal — just log at debug level
            error!(
                "transcribe: failed to delete temp file {}: {}",
                p.display(),
                e
            );
        }
    }
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
    fn voice_otp_compares_both_prompt_repetitions() {
        let prompt = "要接听WhatsApp验证码请按九。要接听WhatsApp验证码请按9。";
        assert_eq!(voice_prompt_readings(prompt).unwrap(), vec!['9', '9']);
        assert_eq!(compare_repeated_voice_prompt(prompt, 6).unwrap(), VoicePrompt::PressDigit('9'));
        assert!(compare_repeated_voice_prompt("请按9。请按6。", 6).is_err());
        assert!(compare_repeated_voice_prompt("请按9。要接听WhatsApp验证码请按", 6).is_err());
        assert!(compare_repeated_voice_prompt("噪声，没有完整按键指令", 6).is_err());
        assert_eq!(compare_repeated_voice_prompt("Your code is 005861.", 6).unwrap(), VoicePrompt::DirectOtp("005861".into()));
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

    #[tokio::test]
    #[ignore = "requires bundled local tools and generated VOICE_OTP_SMOKE_PROMPT/CODE WAV files"]
    async fn voice_otp_local_whisper_pipeline() {
        let ffmpeg = resolve_local_file(None, &["tools/ffmpeg/ffmpeg.exe"]).unwrap();
        let whisper = resolve_local_file(None, &["tools/whisper/whisper-cli.exe"]).unwrap();
        let model = resolve_local_file(None, &["tools/whisper/ggml-base.bin"]).unwrap();
        let prompt_audio = tokio::fs::read(std::env::var("VOICE_OTP_SMOKE_PROMPT").unwrap()).await.unwrap();
        let code_audio = tokio::fs::read(std::env::var("VOICE_OTP_SMOKE_CODE").unwrap()).await.unwrap();
        let prompt = transcribe(&prompt_audio, &ffmpeg, &whisper, &model, "en").await.unwrap();
        assert_eq!(parse_voice_prompt(&prompt, 6).unwrap(), VoicePrompt::PressDigit('5'), "prompt: {prompt}");
        let code = transcribe(&code_audio, &ffmpeg, &whisper, &model, "en").await.unwrap();
        assert_eq!(extract_voice_otp(&code, 6).unwrap(), "005861", "transcript: {code}");
    }
}
