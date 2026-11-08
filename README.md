# 📱 SMS Gateway

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/214zzl995/sms-gateway)

<div align="center">
  <img src="https://raw.githubusercontent.com/214zzl995/sms-gateway/main/frontend/public/logo.svg" alt="SMS Gateway Logo" width="200">
  <h3>A modern SMS management and forwarding system</h3>
</div>

## 📖 Overview

SMS Gateway is a comprehensive solution for receiving, sending, and forwarding SMS messages through GSM modems. It provides a modern web-based interface for managing SMS conversations and offers powerful webhook functionality with extensive filtering capabilities.

### Key Features

- **Multi-device Support**: Connect to multiple GSM modems simultaneously
- **Real-time Messaging**: Send and receive SMS with live updates via SSE
- **Modern Web Interface**: Intuitive conversation-based UI with SIM card management
- **Powerful Webhooks**: Forward SMS to external services with advanced filtering
- **SIM-centric Architecture**: Manage multiple SIM cards with individual settings
- **Flexible Configuration**: TOML-based configuration with comprehensive options
- **PDU Support**: Full PDU encoding/decoding with UCS2 character support

## 🛠️ Building From Source

### Prerequisites

- Rust 1.60+ and Cargo
- Node.js 16+ and npm/pnpm
- SQLite

### Step 1: Build the Frontend

```bash
# Navigate to the frontend directory
cd frontend

# Install dependencies
pnpm install

# Build the frontend
pnpm run build
```

This will generate a `dist` directory within the frontend folder.

### Step 2: Build the Backend

```bash
# Return to the project root
cd ..

# Build the backend in release mode
cargo build --release
```

The compiled binary will be available in `target/release/sms-gateway`.

### eSIM QR Image Phone Recognition

On Windows 10/11, eSIM QR imports and source-folder scans use the local Windows
OCR engine to read a number labeled `Mobile Number`, `Phone Number`, or `MSISDN`.
English OCR is preferred; otherwise an installed user-language OCR engine is used.
No image is sent to an external OCR service. Windows must have an OCR language
installed; on other platforms QR decoding still works but phone OCR is unavailable.

For a single activation QR, a uniquely recognized labeled number fills the editable
phone field in the batch-download preview. Review it before starting a download.
SIM numbers, unlabeled digits, conflicting numbers, and images with multiple
activation QRs are not automatically associated with a phone number. Importing an
image alone does not download a profile or write a phone number to the database.
The download flow stores the supplied number against the newly downloaded ICCID;
existing profiles also have a separate phone-number edit action.

The opt-in Windows image regression uses the supplied test PNG without embedding
its activation code in the executable:

```powershell
$env:ESIM_OCR_TEST_IMAGE = (Resolve-Path .\doc\779347821.png).Path
cargo test --release esim::activation::tests -- --include-ignored
```

### WhatsApp Voice-Code Flow (Aliyun)

The Calls view includes an opt-in two-stage voice-code flow. Speech recognition
uses only the Aliyun 智能语音交互 short-sentence (一句话识别) RESTful API; there is
no local speech model. The default port is COM2, with independent 10-second
prompt/code recordings, a six-digit code, and a 120-second deadline. Save the
settings before calling; only the enabled ports use the interactive flow.

`tools/ffmpeg/ffmpeg.exe` converts modem AMR recordings to WAV before upload (a
configured `ffmpeg_exe` takes precedence); `tools/lpac/lpac.exe` is used by the
separate eSIM operations.

#### Ports and Aliyun configuration

The port setting accepts a comma-separated list of up to 32 ports (for example
`COM2,COM5`); each listed port runs its own independent voice flow.

In the Aliyun panel enter the project AppKey and either a console token
(expires, for testing) or a RAM AccessKey ID/Secret, from which the gateway
creates and renews tokens automatically. Choose the project region and the
model sample rate (16 kHz for general models, 8 kHz for telephone models); the
language comes from the project's model. Use "Test connection" to verify.
Credentials stay on the gateway (`GET/PUT /api/settings/aliyun-asr`,
`POST /api/settings/aliyun-asr/test`) and secrets are never returned to the
browser. The flow cannot be enabled until Aliyun is configured. Recordings are
sent to Aliyun for recognition; ordinary call recordings are also transcribed
by Aliyun when it is configured.

The enabled voice-flow port answers without the ordinary auto-answer delay;
other calls retain their configured delay. After answering, the flow
records/downloads/transcribes the prompt while the call remains connected.
Record enough of the repeated prompt to capture both complete instructions.
The full recording is transcribed in one pass, and each recognized
press instruction is compared. At least two clear readings of the same digit
are required before DTMF. Quoted digits are accepted, but conflicting choices
and multi-digit instructions are rejected. Each reading and the comparison
result are shown in the call log. This is transcript-level consistency checking,
not a claim of two independent ASR engines or guaranteed recognition accuracy.
The second recording starts before the digit is sent.
The call stays connected for the full configured second recording window after
DTMF, then the flow hangs up and transcribes the final audio. If the remote
party ends the call during this window, the flow safely finalizes the owned
recording and transcribes it instead of discarding it as a cancelled job.
Explicit user cancellation remains separate and stops processing. Finalizing
an ended recording requires the same SIM and no new active call.
A direct unambiguous code skips DTMF and the second recording. Incomplete or
conflicting prompt readings leave the call connected for review without sending
a digit; remote hangup, explicit cancellation, or the configured deadline ends
that wait. Deadline expiry or a changed call/SIM stops the flow safely.

The OTP flow reads and preserves the modem's microphone mute state, then mutes
uplink microphone speech before live file transfers. Downlink recording and
network-generated DTMF remain separate. The previous state is restored before
owned-call hangup or guarded ended-call cleanup. Unsupported mute control is
logged without inventing an audio setting. This is a scoped mitigation for
caller-side noise after an in-call download; its actual effect requires a real
call test. Ordinary calls do not receive these audio changes.

The call log shows stages, both transcripts, the sent digit, the resulting code,
recording downloads, and cancellation. Codes retain leading zeroes. A restart
marks unfinished jobs interrupted and never replays DTMF. Apart from Aliyun
recognition, no audio or code is sent to other services, WorldCode, or
webhooks by this flow. Other incoming calls keep
the existing ordinary recording behavior.

The prompt window and Aliyun recognition must finish before the remote IVR
hangs up; increasing the application's deadline cannot extend a call ended by
the remote party. Use a complete first prompt and an Aliyun model matching the
prompt language when tuning a short IVR. The flow never sends a delayed digit to
an ended or changed call.

The control API is authenticated: `GET/PUT /api/settings/voice-otp`,
`POST /api/calls/{id}/voice-cancel`, and
`GET /api/calls/{id}/recording?stage=prompt|code`.

Run `cargo test voice_otp` for hardware-free parsing, persistence, ordering,
cancellation, and restart checks. Real in-call AMR download, recording restart,
DTMF delivery, and timing require a real call test; the application
does not initiate that test call.

## � Logs for Bug Reports

Logs are written to `logs/` beside the executable (`<name>_rCURRENT.log` is the
running file; older files rotate daily and the last 7 are kept). Each line has a
local timestamp with UTC offset, level, module and source line; every start writes
a `===== ... starting` banner with the version. Panics are logged with a backtrace.

Useful prefixes: `[Voice COMx]` (each voice-OTP step with elapsed ms, transcripts,
readings, sent digit, OTP and error), `[USSD COMx]` (network-mode switch, raw reply,
4G restore), `[FreeSMS COMx]` (operator choice and replies), `[ussd]` / `[free_sms]`
(per-port result). Aliyun failures include the Aliyun `task_id`. For more detail,
start with `--log-level debug`. When reporting a problem, send the log files and the
time it happened.

## �🚀 Running the Application

```bash
# Run with default settings
./target/release/sms-gateway

# Specify a custom config file
./target/release/sms-gateway --config /path/to/config.toml

# Set custom log directory
./target/release/sms-gateway --log /path/to/logs

# Set log level
./target/release/sms-gateway --log-level debug

# Update to the latest release
./target/release/sms-gateway update

# Show version
./target/release/sms-gateway version
```

## ⚙️ Configuration

The application is configured using a TOML file. By default, it looks for the config file at:
- Debug mode: `./config.toml`
- Release mode: `/etc/sms-gateway/config.toml`

Copy `config.toml.example` to `config.toml` and modify according to your setup:

```bash
cp config.toml.example config.toml
```

### Basic Configuration

```toml
[settings]
server_host = "0.0.0.0"
server_port = 8080
username = "admin"
password = "your_secure_password"
read_sms_frequency = 30

# Multiple device support
[[devices]]
com_port = "/dev/ttyUSB0"
baud_rate = 115200

[[devices]]
com_port = "/dev/ttyUSB1"
baud_rate = 115200
```

### Webhook Configuration

```toml
[[settings.webhooks]]
url = "https://your-endpoint.com/webhook"
method = "POST"

[settings.webhooks.headers]
"Content-Type" = "application/json"

body = '''
{
    "from": "@contact@",
    "message": "@message@",
    "timestamp": "@timestamp@"
}
'''
```

For detailed configuration options including filtering, see `config.toml.example`.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
