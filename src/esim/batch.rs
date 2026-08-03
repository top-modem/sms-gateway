//! Sequential batch orchestrator for eSIM operations.
//!
//! Because the machine allows only ONE eSIM session at a time (see
//! [`super::EsimService`]), batch jobs run strictly sequentially: for every
//! selected port we `enter` -> operate -> `exit` before moving to the next.
//! Only one batch job may run machine-wide; progress is broadcast over SSE.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

/// The batch operation to apply to every selected port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchOp {
    Download,
    Delete,
    Activate,
    Deactivate,
}

/// One requested port + its activation parameters (parameters used for `download` only).
#[derive(Debug, Clone, Deserialize)]
pub struct BatchReqItem {
    pub com_port: String,
    #[serde(default)]
    pub activation_code: Option<String>,
    #[serde(default)]
    pub smdp: Option<String>,
    #[serde(default)]
    pub matching_id: Option<String>,
    #[serde(default)]
    pub confirmation_code: Option<String>,
    #[serde(default)]
    pub imei: Option<String>,
}

/// Incoming batch request body.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchRequest {
    pub op: BatchOp,
    pub items: Vec<BatchReqItem>,
    #[serde(default)]
    pub auto_enable: Option<bool>,
    #[serde(default)]
    pub replace_existing: Option<bool>,
    #[serde(default)]
    pub stop_on_error: Option<bool>,
}

/// Live status of one port within a job.
#[derive(Debug, Clone, Serialize)]
pub struct BatchItemResult {
    pub com_port: String,
    /// pending | running | ok | failed | skipped
    pub status: String,
    pub message: Option<String>,
    pub iccid: Option<String>,
}

/// A full snapshot of a job, broadcast on every state change.
#[derive(Debug, Clone, Serialize)]
pub struct JobSnapshot {
    pub id: String,
    pub op: BatchOp,
    /// running | done | cancelled
    pub status: String,
    pub items: Vec<BatchItemResult>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

/// Holds the single current/last job and the progress broadcast channel.
pub struct BatchManager {
    inner: Mutex<Option<JobSnapshot>>,
    pub(crate) running: AtomicBool,
    pub(crate) cancel: AtomicBool,
    tx: broadcast::Sender<JobSnapshot>,
}

impl Default for BatchManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchManager {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(200);
        Self {
            inner: Mutex::new(None),
            running: AtomicBool::new(false),
            cancel: AtomicBool::new(false),
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<JobSnapshot> {
        self.tx.subscribe()
    }

    pub async fn snapshot(&self) -> Option<JobSnapshot> {
        self.inner.lock().await.clone()
    }

    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    /// Publish an updated snapshot: store it and broadcast to SSE subscribers.
    pub(crate) async fn publish(&self, snap: JobSnapshot) {
        *self.inner.lock().await = Some(snap.clone());
        let _ = self.tx.send(snap);
    }
}
