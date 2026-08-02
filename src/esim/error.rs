use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Errors surfaced by the eSIM service. Each maps to an HTTP status so handlers
/// can simply `?` and return a consistent JSON error envelope.
#[derive(Debug)]
pub enum EsimError {
    /// The feature is turned off in config (`esim_enabled = false`).
    Disabled,
    /// Another port currently holds the machine-wide eSIM session.
    Busy(String),
    /// No modem is tracked for the given COM port.
    PortNotFound(String),
    /// The port exists but did not report eSIM capability (`ath?`).
    NotCapable(String),
    /// The port is not currently inside an eSIM session (enter first).
    NotInSession(String),
    /// A mode-switch AT command failed.
    At(String),
    /// Failed to spawn the lpac process.
    Spawn(String),
    /// lpac ran but returned a non-zero envelope code (or unparseable output).
    Lpac { code: i64, message: String },
}

impl std::fmt::Display for EsimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EsimError::Disabled => write!(f, "eSIM feature is disabled"),
            EsimError::Busy(p) => write!(f, "eSIM session busy on {}", p),
            EsimError::PortNotFound(p) => write!(f, "port {} not found", p),
            EsimError::NotCapable(p) => write!(f, "port {} is not eSIM-capable", p),
            EsimError::NotInSession(p) => write!(f, "port {} is not in an eSIM session", p),
            EsimError::At(m) => write!(f, "AT error: {}", m),
            EsimError::Spawn(m) => write!(f, "failed to run lpac: {}", m),
            EsimError::Lpac { code, message } => write!(f, "lpac error {}: {}", code, message),
        }
    }
}

impl std::error::Error for EsimError {}

impl IntoResponse for EsimError {
    fn into_response(self) -> Response {
        let status = match &self {
            EsimError::Disabled => StatusCode::FORBIDDEN,
            EsimError::Busy(_) | EsimError::NotInSession(_) => StatusCode::CONFLICT,
            EsimError::PortNotFound(_) => StatusCode::NOT_FOUND,
            EsimError::NotCapable(_) => StatusCode::BAD_REQUEST,
            EsimError::At(_) | EsimError::Spawn(_) => StatusCode::INTERNAL_SERVER_ERROR,
            EsimError::Lpac { .. } => StatusCode::BAD_GATEWAY,
        };
        let code = match &self {
            EsimError::Lpac { code, .. } => Some(*code),
            _ => None,
        };
        (
            status,
            Json(json!({ "success": false, "error": self.to_string(), "code": code })),
        )
            .into_response()
    }
}
