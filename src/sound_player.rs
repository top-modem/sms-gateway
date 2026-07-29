use std::path::Path;

/// Best-effort success sound playback. Failures are logged and ignored.
pub fn play_sms_upload_success_sound() {
    #[cfg(target_os = "windows")]
    {
        let wav_path = Path::new("sound").join("sms.wav");
        if !wav_path.exists() {
            log::warn!("[Sound] SMS success sound file not found: {}", wav_path.display());
            return;
        }

        let escaped = wav_path.to_string_lossy().replace('"', "\"\"");
        let ps_script = format!(
            "try {{ $p='{}'; $player = New-Object System.Media.SoundPlayer($p); $player.Play(); }} catch {{ }}",
            escaped
        );

        let _ = std::process::Command::new("powershell")
            .args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &ps_script,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
}
