use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::core::Modem;
use super::manager::TranscribeConfig;
use crate::api::{sse_manager::CallEvent, SseManager};
use crate::db::Call;
use crate::transcribe::{VoiceOtpProgress, VoiceOtpSettings, VoicePrompt};

#[derive(Debug)]
struct VoiceCancelled;

impl std::fmt::Display for VoiceCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Incoming call ended or was cancelled")
    }
}

impl std::error::Error for VoiceCancelled {}

#[derive(Debug)]
struct RemoteCallEnded;

impl std::fmt::Display for RemoteCallEnded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Remote call ended")
    }
}

impl std::error::Error for RemoteCallEnded {}

struct VoiceCancellation {
    remote: Option<oneshot::Receiver<()>>,
    user: oneshot::Receiver<()>,
}

impl VoiceCancellation {
    #[cfg(test)]
    fn for_test(remote: oneshot::Receiver<()>) -> (oneshot::Sender<()>, Self) {
        let (sender, user) = oneshot::channel();
        (sender, Self { remote: Some(remote), user })
    }
}

trait CallIo: Sync {
    fn active(&self) -> impl Future<Output = Result<()>> + Send;
    fn prepare_audio(&self) -> impl Future<Output = Result<()>> + Send;
    fn record(&self, filename: &str) -> impl Future<Output = Result<()>> + Send;
    fn stop(&self) -> impl Future<Output = Result<()>> + Send;
    fn download(&self, filename: &str) -> impl Future<Output = Result<Vec<u8>>> + Send;
    fn delete(&self, filename: &str) -> impl Future<Output = ()> + Send;
    fn transcribe(&self, audio: &[u8]) -> impl Future<Output = Result<String>> + Send;
    fn save_audio(&self, audio: &[u8], prompt: bool) -> impl Future<Output = Result<()>> + Send;
    fn progress(&self, progress: &VoiceOtpProgress) -> impl Future<Output = Result<()>> + Send;
    fn dtmf(&self, digit: char) -> impl Future<Output = Result<()>> + Send;
    fn hangup(&self) -> impl Future<Output = Result<()>> + Send;
    fn finish_remote_recording(&self) -> impl Future<Output = Result<()>> + Send;
}

struct LocalCall {
    modem: Arc<Modem>,
    call_id: String,
    sim_id: String,
    config: Arc<TranscribeConfig>,
    sse: Arc<SseManager>,
    started: std::time::Instant,
}

impl CallIo for LocalCall {
    async fn prepare_audio(&self) -> Result<()> {
        if let Err(error) = self.modem.quiet_voice_microphone(&self.call_id, &self.sim_id).await {
            log::warn!("[Voice {}] could not quiet the uplink microphone: {}", self.modem.com_port, error);
        }
        Ok(())
    }

    async fn active(&self) -> Result<()> {
        if self.modem.inbound_call_id.lock().await.as_deref() != Some(self.call_id.as_str())
            || self.modem.sim_id.read().await.as_deref() != Some(self.sim_id.as_str())
            || !self.modem.inbound_call_answered.load(Ordering::SeqCst)
        {
            bail!("The incoming call ended or its SIM identity changed");
        }
        Ok(())
    }

    async fn record(&self, filename: &str) -> Result<()> {
        self.modem.voice_call_command(&self.call_id, &self.sim_id,
            &format!("AT+QAUDRD=1,\"{}\",3,1\r\n", filename), false).await?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.modem.voice_call_command(&self.call_id, &self.sim_id, "AT+QAUDRD=0\r\n", false).await?;
        Ok(())
    }

    async fn download(&self, filename: &str) -> Result<Vec<u8>> {
        let audio = self.modem.download_file(filename).await?;
        if !(audio.starts_with(b"#!AMR\n") || audio.starts_with(b"#!AMR-WB\n")) || audio.len() < 12 {
            bail!("The modem returned an empty or invalid AMR recording");
        }
        Ok(audio)
    }

    async fn delete(&self, filename: &str) {
        let _ = self.modem.send_command(&format!("AT+QFDEL=\"{}\"\r\n", filename)).await;
    }

    async fn transcribe(&self, audio: &[u8]) -> Result<String> {
        let started = std::time::Instant::now();
        let text = crate::aliyun_asr::transcribe(audio, &self.config.ffmpeg_exe).await?;
        log::info!("[Voice {}] Aliyun recognition took {} ms for {} bytes: {:?}",
            self.modem.com_port, started.elapsed().as_millis(), audio.len(), text);
        Ok(text)
    }

    async fn save_audio(&self, audio: &[u8], prompt: bool) -> Result<()> {
        if prompt { Call::save_prompt_recording(&self.call_id, audio).await }
        else { Call::save_recording(&self.call_id, audio).await }
    }

    async fn progress(&self, progress: &VoiceOtpProgress) -> Result<()> {
        Call::save_voice_otp(&self.call_id, progress).await?;
        if progress.state == "completed" {
            if let Some(text) = progress.code_transcript.as_deref().or(progress.prompt_transcript.as_deref()) {
                Call::save_transcript(&self.call_id, text).await?;
            }
        }
        log::info!(
            "[Voice {}] call={} t={}ms state={} readings={:?} comparison={:?} dtmf={:?} otp={:?} prompt={:?} code={:?} error={:?}",
            self.modem.com_port, self.call_id, self.started.elapsed().as_millis(), progress.state,
            progress.prompt_readings, progress.prompt_comparison, progress.dtmf, progress.otp,
            progress.prompt_transcript, progress.code_transcript, progress.error,
        );
        self.sse.send_call_event(CallEvent {
            event_type: "voice_otp_progress".into(), sim_id: self.sim_id.clone(),
            call_id: self.call_id.clone(), phone: None, direction: "inbound".into(),
        });
        Ok(())
    }

    async fn dtmf(&self, digit: char) -> Result<()> {
        self.modem.send_voice_dtmf(&self.call_id, &self.sim_id, digit).await?;
        Ok(())
    }

    async fn hangup(&self) -> Result<()> {
        self.modem.voice_otp_finishing.store(true, Ordering::SeqCst);
        if let Err(error) = self.modem.voice_call_command(&self.call_id, &self.sim_id, "ATH\r\n", false).await {
            self.modem.voice_otp_finishing.store(false, Ordering::SeqCst);
            return Err(error.into());
        }
        self.modem.inject_urc(&format!("VOICE_OTP_END:{}", self.call_id));
        Ok(())
    }

    async fn finish_remote_recording(&self) -> Result<()> {
        self.modem.stop_ended_voice_recording(&self.call_id, &self.sim_id).await?;
        self.modem.voice_otp_finishing.store(true, Ordering::SeqCst);
        Ok(())
    }
}

async fn cancellable<Output>(
    future: impl Future<Output = Result<Output>>,
    cancel: &mut VoiceCancellation,
    deadline: Instant,
) -> Result<Output> {
    if Instant::now() >= deadline { bail!("Voice-flow deadline exceeded"); }
    tokio::select! {
        biased;
        _ = &mut cancel.user => Err(VoiceCancelled.into()),
        _ = async {
            if let Some(receiver) = cancel.remote.as_mut() { let _ = receiver.await; }
            else { std::future::pending::<()>().await; }
        } => {
            cancel.remote = None;
            Err(RemoteCallEnded.into())
        },
        result = tokio::time::timeout_at(deadline, future) => result.context("Voice-flow deadline exceeded")?,
    }
}

async fn state(io: &impl CallIo, progress: &mut VoiceOtpProgress, value: &str) -> Result<()> {
    progress.state = value.into();
    io.progress(progress).await
}

/// Like `cancellable`, but the caller hanging up does not abort it: work on audio
/// that is already recorded must still finish after the call ends.
async fn survives_hangup<Output>(
    future: impl Future<Output = Result<Output>>,
    cancel: &mut VoiceCancellation,
    deadline: Instant,
) -> Result<Output> {
    if Instant::now() >= deadline { bail!("Voice-flow deadline exceeded"); }
    tokio::select! {
        biased;
        _ = &mut cancel.user => Err(VoiceCancelled.into()),
        result = tokio::time::timeout_at(deadline, future) => result.context("Voice-flow deadline exceeded")?,
    }
}

/// The IVR hangs up roughly 15 s after answering when no key is pressed, and recognition
/// takes about as long as the audio, so the prompt is checked in short slices.
const PROMPT_SEGMENT: Duration = Duration::from_secs(3);

fn prompt_segment_file(prompt_file: &str, index: usize) -> String {
    if index == 0 { return prompt_file.to_string(); }
    let stem = prompt_file.strip_suffix(".amr").unwrap_or(prompt_file);
    format!("{}_{}.amr", stem, index)
}

/// Joins AMR recordings by dropping the repeated file header of later slices.
fn append_amr(all: &mut Vec<u8>, slice: &[u8]) {
    let header: &[u8] = if slice.starts_with(b"#!AMR-WB\n") { b"#!AMR-WB\n" } else { b"#!AMR\n" };
    if all.is_empty() {
        all.extend_from_slice(slice);
    } else if all.starts_with(header) && slice.starts_with(header) {
        all.extend_from_slice(&slice[header.len()..]);
    } else {
        *all = slice.to_vec();
    }
}

fn record_prompt_readings(progress: &mut VoiceOtpProgress, text: &str) {
    progress.prompt_transcript = Some(text.to_string());
    progress.prompt_readings = crate::transcribe::voice_prompt_readings(text).unwrap_or_default();
    progress.prompt_comparison = Some(match progress.prompt_readings.len() {
        0 => "incomplete",
        _ if progress.prompt_readings.iter().any(|digit| *digit != progress.prompt_readings[0]) => "conflicting",
        1 => "single",
        _ => "matched",
    }.into());
}

async fn run(
    io: &impl CallIo,
    settings: &VoiceOtpSettings,
    progress: &mut VoiceOtpProgress,
    cancel: &mut VoiceCancellation,
    prompt_file: &str,
    code_file: &str,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(settings.deadline_seconds);
    cancellable(async { Ok(()) }, cancel, deadline).await?;
    io.active().await?;
    io.prepare_audio().await?;
    state(io, progress, "recording_prompt").await?;
    let listen_until = Instant::now() + Duration::from_secs(settings.prompt_seconds);
    let mut segment_index = 0;
    let mut segment = prompt_segment_file(prompt_file, 0);
    let mut segment_started = Instant::now();
    io.record(&segment).await?;
    let mut prompt_audio = Vec::new();
    let mut remote_ended = false;
    let mut early_digit = None;
    loop {
        let wait = (segment_started + PROMPT_SEGMENT).min(listen_until).saturating_duration_since(Instant::now());
        match cancellable(async { tokio::time::sleep(wait).await; Ok(()) }, cancel, deadline).await {
            Ok(()) => io.stop().await?,
            Err(error) if error.is::<RemoteCallEnded>() => {
                remote_ended = true;
                io.finish_remote_recording().await?;
            }
            Err(error) => return Err(error),
        }
        let finished = segment.clone();
        let mut last = remote_ended || Instant::now() >= listen_until;
        if !last {
            // Keep listening while the finished slice is recognised.
            let next = prompt_segment_file(prompt_file, segment_index + 1);
            match io.record(&next).await {
                Ok(()) => {
                    segment_index += 1;
                    segment = next;
                    segment_started = Instant::now();
                }
                Err(error) => {
                    if io.active().await.is_ok() { return Err(error); }
                    remote_ended = true;
                    last = true;
                }
            }
        }
        if last { state(io, progress, "downloading_prompt").await?; }
        match io.download(&finished).await {
            Ok(audio) => append_amr(&mut prompt_audio, &audio),
            Err(error) if last && prompt_audio.is_empty() => return Err(error),
            Err(error) => log::warn!("Voice prompt slice {} could not be downloaded: {:#}", finished, error),
        }
        if finished != prompt_file { io.delete(&finished).await; }
        if last { break; }
        if prompt_audio.is_empty() { continue; }
        state(io, progress, "transcribing_prompt").await?;
        match survives_hangup(io.transcribe(&prompt_audio), cancel, deadline).await {
            Ok(text) => {
                if let Some(digit) = crate::transcribe::early_press_digit(&text) {
                    record_prompt_readings(progress, &text);
                    early_digit = Some(digit);
                    break;
                }
            }
            Err(error) if error.is::<VoiceCancelled>() => return Err(error),
            Err(error) => log::warn!("Partial voice prompt recognition failed: {:#}", error),
        }
        state(io, progress, "recording_prompt").await?;
    }
    if early_digit.is_some() {
        // Starting the code recording also stops this slice, so a failure here is harmless.
        let _ = io.stop().await;
        io.delete(&segment).await;
    }
    io.save_audio(&prompt_audio, true).await?;
    let instruction = if let Some(digit) = early_digit {
        state(io, progress, "checking_prompt").await?;
        Ok(VoicePrompt::PressDigit(digit))
    } else {
        state(io, progress, "transcribing_prompt").await?;
        let prompt_text = survives_hangup(io.transcribe(&prompt_audio), cancel, deadline).await?;
        record_prompt_readings(progress, &prompt_text);
        state(io, progress, "checking_prompt").await?;
        crate::transcribe::parse_voice_prompt(&prompt_text, settings.otp_length)
    };
    let instruction = match instruction {
        Ok(instruction) => instruction,
        Err(error) => {
            let reason = format!("No clear press instruction: {}", error);
            progress.error = Some(reason.clone());
            state(io, progress, "awaiting_instruction").await?;
            return cancellable(std::future::pending::<Result<()>>(), cancel, deadline)
                .await.context(reason);
        }
    };
    if matches!(instruction, VoicePrompt::DirectOtp(_)) {
        progress.prompt_comparison = None;
    }
    match instruction {
        VoicePrompt::DirectOtp(code) => {
            if io.active().await.is_ok() {
                io.hangup().await?;
            }
            io.save_audio(&prompt_audio, false).await?;
            progress.otp = Some(code);
        }
        VoicePrompt::PressDigit(digit) => {
            if remote_ended { return Err(RemoteCallEnded.into()); }
            io.active().await?;
            state(io, progress, "recording_code").await?;
            io.record(code_file).await?;
            cancellable(async { Ok(()) }, cancel, deadline).await?;
            io.dtmf(digit).await?;
            progress.dtmf = Some(digit);
            io.progress(progress).await?;
            match cancellable(async { tokio::time::sleep(Duration::from_secs(settings.code_seconds)).await; Ok(()) }, cancel, deadline).await {
                Ok(()) => {
                    io.stop().await?;
                    state(io, progress, "hanging_up").await?;
                    io.hangup().await?;
                }
                Err(error) if error.is::<RemoteCallEnded>() => {
                    io.finish_remote_recording().await?;
                }
                Err(error) => return Err(error),
            }
            state(io, progress, "downloading_code").await?;
            let code_audio = io.download(code_file).await?;
            io.save_audio(&code_audio, false).await?;
            state(io, progress, "transcribing_code").await?;
            let code_text = cancellable(io.transcribe(&code_audio), cancel, deadline).await?;
            progress.code_transcript = Some(code_text.clone());
            progress.otp = Some(crate::transcribe::extract_voice_otp(&code_text, settings.otp_length)?);
        }
    }
    state(io, progress, "completed").await
}

pub(super) async fn maybe_start(
    modem: Arc<Modem>, call_id: String, sim_id: String,
    config: Option<Arc<TranscribeConfig>>, sse: Arc<SseManager>,
) -> Result<bool> {
    let settings = VoiceOtpSettings::load().await?;
    if !settings.handles(&modem.com_port) {
        return Ok(false);
    }
    let config = config.context("ffmpeg is unavailable for converting call audio")?;
    if !crate::aliyun_asr::AliyunAsrSettings::load().await?.ready() {
        bail!("Aliyun speech recognition is not configured");
    }
    static ADMISSION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(VoiceOtpSettings::MAX_PORTS);
    let permit = ADMISSION.try_acquire().context("Too many voice-flow calls are already running")?;
    let mut owned_call = modem.voice_otp_call_id.lock().await;
    if owned_call.is_some() { bail!("The previous voice-flow job is still finishing"); }
    *owned_call = Some(call_id.clone());
    modem.voice_otp_finishing.store(false, Ordering::SeqCst);
    modem.voice_otp_dtmf_sent.store(false, Ordering::SeqCst);
    let (cancel_tx, cancel_rx) = oneshot::channel();
    *modem.inbound_recording_cancel_tx.lock().await = Some(cancel_tx);
    let (user_cancel_tx, user_cancel_rx) = oneshot::channel();
    *modem.voice_otp_cancel_tx.lock().await = Some(user_cancel_tx);
    let mut cancellation = VoiceCancellation { remote: Some(cancel_rx), user: user_cancel_rx };
    drop(owned_call);
    let io = LocalCall { modem: modem.clone(), call_id: call_id.clone(), sim_id, config, sse, started: std::time::Instant::now() };
    let token: String = call_id.chars().filter(char::is_ascii_alphanumeric).take(12).collect();
    let prompt_file = format!("vp{}.amr", token);
    let code_file = format!("vc{}.amr", token);
    tokio::spawn(async move {
        let _permit = permit;
        let mut progress = VoiceOtpProgress { provider: "aliyun".into(), ..Default::default() };
        if let Err(error) = run(&io, &settings, &mut progress, &mut cancellation, &prompt_file, &code_file).await {
            progress.state = if error.is::<VoiceCancelled>() || error.is::<RemoteCallEnded>()
                || (!modem.voice_otp_finishing.load(Ordering::SeqCst) && io.active().await.is_err())
                { "cancelled" } else { "failed" }.into();
            progress.error = Some(format!("{:#}", error));
            if let Err(persist_error) = io.progress(&progress).await {
                log::error!("[Voice {}] cannot save failure: {}", modem.com_port, persist_error);
            }
            if io.active().await.is_ok() {
                let _ = io.stop().await;
                log::warn!("[Voice {}] call={} failed, keeping the call connected: {:#}", modem.com_port, call_id, error);
                let wait_until = Instant::now() + Duration::from_secs(settings.deadline_seconds);
                let ended = cancellable(std::future::pending::<Result<()>>(), &mut cancellation, wait_until).await;
                if ended.as_ref().is_err_and(|error| !error.is::<RemoteCallEnded>()) && io.active().await.is_ok() {
                    let _ = io.hangup().await;
                }
            }
        }
        if modem.inbound_call_id.lock().await.as_deref().is_none_or(|current| current == call_id) {
            if modem.stop_ended_voice_recording(&call_id, &io.sim_id).await.is_ok() {
                for filename in [&prompt_file, &code_file] {
                    let _ = modem.send_command(&format!("AT+QFDEL=\"{}\"\r\n", filename)).await;
                }
            }
        }
        let mut owned_call = modem.voice_otp_call_id.lock().await;
        if owned_call.as_deref() == Some(call_id.as_str()) {
            modem.inbound_recording_cancel_tx.lock().await.take();
            modem.voice_otp_cancel_tx.lock().await.take();
            *owned_call = None;
        }
    });
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct FakeCall {
        events: Mutex<Vec<String>>,
        transcripts: Mutex<VecDeque<String>>,
        ended: std::sync::atomic::AtomicBool,
        end_after_dtmf: Mutex<Option<oneshot::Sender<()>>>,
        dtmf_notify: Mutex<Option<oneshot::Sender<()>>>,
        cancel_on_code_download: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl FakeCall {
        fn new(transcripts: &[&str]) -> Self {
            Self { events: Mutex::new(Vec::new()), transcripts: Mutex::new(transcripts.iter().map(|text| text.to_string()).collect()),
                ended: std::sync::atomic::AtomicBool::new(false), end_after_dtmf: Mutex::new(None),
                dtmf_notify: Mutex::new(None), cancel_on_code_download: Mutex::new(None) }
        }
        fn note(&self, event: String) { self.events.lock().unwrap().push(event); }
    }

    impl CallIo for FakeCall {
        async fn prepare_audio(&self) -> Result<()> { self.note("prepare_microphone".into()); Ok(()) }
        async fn active(&self) -> Result<()> {
            if self.ended.load(Ordering::SeqCst) { bail!("Fake remote call ended"); }
            Ok(())
        }
        async fn record(&self, filename: &str) -> Result<()> { self.note(format!("record:{filename}")); Ok(()) }
        async fn stop(&self) -> Result<()> { self.note("stop".into()); Ok(()) }
        async fn download(&self, filename: &str) -> Result<Vec<u8>> {
            self.note(format!("download:{filename}"));
            if filename == "code.amr" {
                if let Some(sender) = self.cancel_on_code_download.lock().unwrap().take() {
                    let _ = sender.send(());
                }
            }
            Ok(vec![1])
        }
        async fn delete(&self, filename: &str) { self.note(format!("delete:{filename}")); }
        async fn transcribe(&self, _audio: &[u8]) -> Result<String> { self.note("transcribe".into()); self.transcripts.lock().unwrap().pop_front().context("missing fake transcript") }
        async fn save_audio(&self, _audio: &[u8], _prompt: bool) -> Result<()> { Ok(()) }
        async fn progress(&self, _progress: &VoiceOtpProgress) -> Result<()> { Ok(()) }
        async fn dtmf(&self, digit: char) -> Result<()> {
            self.note(format!("dtmf:{digit}"));
            if let Some(sender) = self.dtmf_notify.lock().unwrap().take() {
                let _ = sender.send(());
            }
            if let Some(sender) = self.end_after_dtmf.lock().unwrap().take() {
                self.ended.store(true, Ordering::SeqCst);
                let _ = sender.send(());
            }
            Ok(())
        }
        async fn hangup(&self) -> Result<()> { self.note("hangup".into()); Ok(()) }
        async fn finish_remote_recording(&self) -> Result<()> { self.note("stop_after_remote_end".into()); Ok(()) }
    }

    fn settings() -> VoiceOtpSettings {
        VoiceOtpSettings { prompt_seconds: 0, code_seconds: 0, ..Default::default() }
    }

    #[tokio::test]
    async fn voice_otp_records_before_dtmf_and_transcribes_after_hangup() {
        let io = FakeCall::new(&["To hear your code, press five. To hear your code, press five.", "Your code is zero zero five eight six one."]);
        let (_sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let mut progress = VoiceOtpProgress::default();
        run(&io, &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap();
        assert_eq!(progress.otp.as_deref(), Some("005861"));
        assert_eq!(progress.state, "completed");
        assert_eq!(progress.prompt_readings, vec!['5', '5']);
        assert_eq!(progress.prompt_comparison.as_deref(), Some("matched"));
        let events = io.events.lock().unwrap();
        assert!(events.iter().position(|event| event == "prepare_microphone").unwrap()
            < events.iter().position(|event| event == "download:prompt.amr").unwrap());
        assert_eq!(events.iter().filter(|event| event.starts_with("dtmf:")).count(), 1);
        assert!(events.iter().position(|event| event == "record:code.amr").unwrap() < events.iter().position(|event| event == "dtmf:5").unwrap());
        assert!(events.iter().position(|event| event == "hangup").unwrap() < events.iter().position(|event| event == "download:code.amr").unwrap());
    }

    #[tokio::test]
    async fn voice_otp_direct_code_skips_dtmf_and_second_recording() {
        let io = FakeCall::new(&["Your code is 005861."]);
        let (_sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let mut progress = VoiceOtpProgress::default();
        run(&io, &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap();
        assert_eq!(progress.otp.as_deref(), Some("005861"));
        assert!(io.events.lock().unwrap().iter().all(|event| !event.starts_with("dtmf:") && event != "record:code.amr"));
    }

    #[tokio::test]
    async fn voice_otp_direct_code_survives_remote_hangup_during_prompt() {
        let io = Arc::new(FakeCall::new(&["Your WhatsApp code is 256490. Your WhatsApp code is 256490."]));
        let (sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let ender = io.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            ender.ended.store(true, Ordering::SeqCst);
            let _ = sender.send(());
        });
        let mut config = settings();
        config.prompt_seconds = 5;
        let mut progress = VoiceOtpProgress::default();
        run(io.as_ref(), &config, &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap();
        assert_eq!(progress.otp.as_deref(), Some("256490"));
        assert_eq!(progress.state, "completed");
        let events = io.events.lock().unwrap();
        assert!(events.iter().any(|event| event == "stop_after_remote_end"));
        assert!(events.iter().any(|event| event == "transcribe"));
        assert!(!events.iter().any(|event| event == "hangup"));
    }

    #[tokio::test]
    async fn voice_otp_unclear_prompt_keeps_the_call_connected_without_guessing_dtmf() {
        let io = Arc::new(FakeCall::new(&["今天我在电这么忙请按"]));
        let (_sender, receiver) = oneshot::channel();
        let (user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let worker_io = io.clone();
        let mut worker = tokio::spawn(async move {
            let mut progress = VoiceOtpProgress::default();
            let result = run(worker_io.as_ref(), &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await;
            (result, progress)
        });
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut worker).await.is_err());
        assert!(io.events.lock().unwrap().iter().all(|event| !event.starts_with("dtmf:") && event != "hangup"));
        user_sender.send(()).unwrap();
        let (result, progress) = worker.await.unwrap();
        assert!(result.unwrap_err().is::<VoiceCancelled>());
        assert_eq!(progress.state, "awaiting_instruction");
        assert!(progress.error.as_deref().unwrap().contains("No clear press instruction"));
    }

    #[tokio::test]
    async fn voice_otp_conflicting_instruction_still_respects_call_deadline() {
        let io = FakeCall::new(&["Press five or press six."]);
        let (_sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let mut progress = VoiceOtpProgress::default();
        let mut config = settings();
        config.deadline_seconds = 1;
        assert!(run(&io, &config, &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.is_err());
        assert_eq!(progress.state, "awaiting_instruction");
        assert!(io.events.lock().unwrap().iter().all(|event| !event.starts_with("dtmf:")));
    }

    #[test]
    fn voice_otp_microphone_state_must_be_verified_before_muting() {
        assert!(!Modem::parse_voice_microphone_state("\r\n+CMUT: 0\r\nOK\r\n").unwrap());
        assert!(Modem::parse_voice_microphone_state("\r\n+CMUT: 1\r\nOK\r\n").unwrap());
        for reply in ["ERROR\r\n", "+CMUT: 2\r\nOK\r\n", "OK\r\n"] {
            assert!(Modem::parse_voice_microphone_state(reply).is_err());
        }
    }

    #[tokio::test]
    async fn voice_otp_sends_dtmf_after_the_first_prompt_slice_names_a_key() {
        let io = FakeCall::new(&["好，验证码请按一主要接收", "你的验证码是256490。"]);
        let (_sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let mut config = settings();
        config.prompt_seconds = 20;
        let started = std::time::Instant::now();
        let mut progress = VoiceOtpProgress::default();
        run(&io, &config, &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(progress.otp.as_deref(), Some("256490"));
        assert_eq!(progress.dtmf, Some('1'));
        assert_eq!(progress.prompt_comparison.as_deref(), Some("single"));
        let events = io.events.lock().unwrap();
        let position = |name: &str| events.iter().position(|event| event == name).unwrap();
        assert!(position("record:prompt_1.amr") < position("download:prompt.amr"));
        assert!(position("download:prompt.amr") < position("dtmf:1"));
        assert!(position("delete:prompt_1.amr") < position("record:code.amr"));
        assert!(position("record:code.amr") < position("dtmf:1"));
    }

    #[test]
    fn voice_otp_prompt_slices_join_without_repeating_the_amr_header() {
        let mut audio = Vec::new();
        append_amr(&mut audio, b"#!AMR\nAB");
        append_amr(&mut audio, b"#!AMR\nCD");
        assert_eq!(audio, b"#!AMR\nABCD");
        assert_eq!(prompt_segment_file("vpabc.amr", 0), "vpabc.amr");
        assert_eq!(prompt_segment_file("vpabc.amr", 2), "vpabc_2.amr");
    }

    #[tokio::test]
    async fn voice_otp_cancelled_call_never_sends_dtmf() {
        let io = FakeCall::new(&["Press five."]);
        let (sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        sender.send(()).unwrap();
        let mut progress = VoiceOtpProgress::default();
        assert!(run(&io, &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.is_err());
        assert!(io.events.lock().unwrap().iter().all(|event| !event.starts_with("dtmf:")));
    }

    #[tokio::test]
    async fn voice_otp_expired_deadline_never_starts_recording_or_dtmf() {
        let io = FakeCall::new(&["Press five."]);
        let (_sender, receiver) = oneshot::channel();
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let mut progress = VoiceOtpProgress::default();
        let mut config = settings();
        config.deadline_seconds = 0;
        assert!(run(&io, &config, &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.is_err());
        assert!(io.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn voice_otp_remote_end_after_dtmf_still_saves_and_transcribes_code() {
        let io = FakeCall::new(&["Press nine. Press nine.", "Your code is 005861."]);
        let (sender, receiver) = oneshot::channel();
        *io.end_after_dtmf.lock().unwrap() = Some(sender);
        let (_user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let mut progress = VoiceOtpProgress::default();
        run(&io, &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap();
        assert_eq!(progress.otp.as_deref(), Some("005861"));
        let events = io.events.lock().unwrap();
        assert!(events.iter().any(|event| event == "stop_after_remote_end"));
        assert!(!events.iter().any(|event| event == "hangup"));
        assert!(events.iter().any(|event| event == "download:code.amr"));
    }

    #[tokio::test]
    async fn voice_otp_explicit_user_cancel_is_not_remote_completion() {
        let io = FakeCall::new(&["Press nine. Press nine.", "Your code is 005861."]);
        let (_sender, receiver) = oneshot::channel();
        let (user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        user_sender.send(()).unwrap();
        let mut progress = VoiceOtpProgress::default();
        let error = run(&io, &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap_err();
        assert!(error.is::<VoiceCancelled>());
        assert!(io.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn voice_otp_keeps_call_connected_for_the_post_dtmf_window() {
        let io = Arc::new(FakeCall::new(&["Press nine. Press nine.", "Your code is 005861."]));
        let (notify, notified) = oneshot::channel();
        *io.dtmf_notify.lock().unwrap() = Some(notify);
        let (remote_sender, receiver) = oneshot::channel();
        let (user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        let worker_io = io.clone();
        let mut worker = tokio::spawn(async move {
            let _remote_sender = remote_sender;
            let _user_sender = user_sender;
            let mut config = settings();
            config.code_seconds = 1;
            let mut progress = VoiceOtpProgress::default();
            run(worker_io.as_ref(), &config, &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap();
            progress
        });
        notified.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut worker).await.is_err());
        assert!(!io.events.lock().unwrap().iter().any(|event| event == "hangup"));
        let progress = worker.await.unwrap();
        assert_eq!(progress.otp.as_deref(), Some("005861"));
        assert!(io.events.lock().unwrap().iter().any(|event| event == "hangup"));
    }

    #[tokio::test]
    async fn voice_otp_user_can_cancel_code_processing_after_remote_hangup() {
        let io = FakeCall::new(&["Press nine. Press nine.", "Your code is 005861."]);
        let (remote_sender, receiver) = oneshot::channel();
        *io.end_after_dtmf.lock().unwrap() = Some(remote_sender);
        let (user_sender, mut cancellation) = VoiceCancellation::for_test(receiver);
        *io.cancel_on_code_download.lock().unwrap() = Some(user_sender);
        let mut progress = VoiceOtpProgress::default();
        let error = run(&io, &settings(), &mut progress, &mut cancellation, "prompt.amr", "code.amr").await.unwrap_err();
        assert!(error.is::<VoiceCancelled>());
        assert!(progress.otp.is_none());
        assert!(!io.events.lock().unwrap().iter().any(|event| event == "hangup"));
    }
}