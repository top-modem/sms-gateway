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

## 🚀 Running the Application

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
