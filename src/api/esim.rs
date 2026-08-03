use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Multipart, Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::wrappers::BroadcastStream;

use crate::esim::{BatchRequest, DownloadRequest, EsimError, EsimService};

type Svc = Arc<EsimService>;

/// Build the `/esim/*` sub-router (mounted under `/api`, so paths become
/// `/api/esim/...`). Auth is applied by the parent router's layer.
pub fn routes(svc: Svc) -> Router {
    Router::new()
        .route("/esim/ports", get(list_ports).with_state(svc.clone()))
        .route(
            "/esim/{com}/session/enter",
            post(enter).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/session/exit",
            post(exit).with_state(svc.clone()),
        )
        .route("/esim/{com}/reset", post(reset).with_state(svc.clone()))
        .route("/esim/{com}/chip", get(chip).with_state(svc.clone()))
        .route("/esim/{com}/profiles", get(profiles).with_state(svc.clone()))
        .route(
            "/esim/{com}/profiles/download",
            post(download).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/profiles/enable",
            post(enable).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/profiles/disable",
            post(disable).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/profiles/delete",
            post(delete).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/profiles/nickname",
            post(nickname).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/notifications",
            get(notifications).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/notifications/process",
            post(notifications_process).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/notifications/remove",
            post(notifications_remove).with_state(svc.clone()),
        )
        .route(
            "/esim/{com}/provision",
            post(provision).with_state(svc.clone()),
        )
        // ---- Activation sources & batch operations ----
        .route("/esim/sources", get(sources).with_state(svc.clone()))
        .route(
            "/esim/sources/upload",
            post(sources_upload).with_state(svc.clone()),
        )
        .route("/esim/batch", post(batch_start).with_state(svc.clone()))
        .route("/esim/batch", get(batch_get).with_state(svc.clone()))
        .route(
            "/esim/batch/cancel",
            post(batch_cancel).with_state(svc.clone()),
        )
        .route(
            "/esim/batch/events",
            get(batch_events).with_state(svc.clone()),
        )
}

fn ok(data: Value) -> Json<Value> {
    Json(json!({ "success": true, "data": data }))
}

async fn list_ports(State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    let ports = svc.list_ports().await?;
    Ok(ok(json!(ports)))
}

async fn enter(
    Path(com): Path<String>,
    State(svc): State<Svc>,
) -> Result<Json<Value>, EsimError> {
    svc.enter(&com).await?;
    Ok(ok(json!({ "com_port": com, "esim_mode": true })))
}

async fn exit(Path(com): Path<String>, State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    svc.exit(&com).await?;
    Ok(ok(json!({ "com_port": com, "esim_mode": false })))
}

async fn reset(Path(com): Path<String>, State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    svc.reset(&com).await?;
    Ok(ok(json!({ "com_port": com, "esim_mode": false })))
}

async fn chip(Path(com): Path<String>, State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.chip_info(&com).await?))
}

async fn profiles(
    Path(com): Path<String>,
    State(svc): State<Svc>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.profiles(&com).await?))
}

async fn download(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<DownloadRequest>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.download(&com, &body).await?))
}

#[derive(Deserialize)]
struct ProfileAction {
    iccid: String,
    #[serde(default = "default_refresh")]
    refresh_flag: bool,
}

fn default_refresh() -> bool {
    true
}

async fn enable(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<ProfileAction>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.enable(&com, &body.iccid, body.refresh_flag).await?))
}

async fn disable(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<ProfileAction>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.disable(&com, &body.iccid, body.refresh_flag).await?))
}

#[derive(Deserialize)]
struct DeleteRequest {
    iccid: String,
}

async fn delete(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<DeleteRequest>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.delete(&com, &body.iccid).await?))
}

#[derive(Deserialize)]
struct NicknameRequest {
    iccid: String,
    nickname: String,
}

async fn nickname(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<NicknameRequest>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.nickname(&com, &body.iccid, &body.nickname).await?))
}

async fn notifications(
    Path(com): Path<String>,
    State(svc): State<Svc>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.notification_list(&com).await?))
}

#[derive(Deserialize)]
struct NotificationAction {
    seq: String,
    #[serde(default)]
    remove: bool,
}

async fn notifications_process(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<NotificationAction>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc
        .notification_process(&com, &body.seq, body.remove)
        .await?))
}

#[derive(Deserialize)]
struct SeqQuery {
    seq: String,
}

async fn notifications_remove(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Query(q): Query<SeqQuery>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.notification_remove(&com, &q.seq).await?))
}

async fn provision(
    Path(com): Path<String>,
    State(svc): State<Svc>,
    Json(body): Json<DownloadRequest>,
) -> Result<Json<Value>, EsimError> {
    Ok(ok(svc.provision(&com, &body).await?))
}

/// Scan the configured source directory for QR-image / text activation codes.
async fn sources(State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    svc.ready()?;
    Ok(ok(json!(svc.scan_sources())))
}

/// Parse uploaded QR images / text files into activation codes (no persistence).
async fn sources_upload(
    State(svc): State<Svc>,
    mut multipart: Multipart,
) -> Result<Json<Value>, EsimError> {
    svc.ready()?;
    let mut codes = Vec::new();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| EsimError::Spawn(e.to_string()))?
    {
        let name = field
            .file_name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "upload".to_string());
        let bytes = field
            .bytes()
            .await
            .map_err(|e| EsimError::Spawn(e.to_string()))?;
        codes.extend(crate::esim::activation::parse_file_bytes(&name, &bytes));
    }
    Ok(ok(json!(codes)))
}

async fn batch_start(
    State(svc): State<Svc>,
    Json(body): Json<BatchRequest>,
) -> Result<Json<Value>, EsimError> {
    let id = svc.start_batch(body).await?;
    Ok(ok(json!({ "job_id": id })))
}

async fn batch_get(State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    svc.ready()?;
    Ok(ok(json!(svc.batch_snapshot().await)))
}

async fn batch_cancel(State(svc): State<Svc>) -> Result<Json<Value>, EsimError> {
    svc.ready()?;
    svc.cancel_batch();
    Ok(ok(json!({ "cancelled": true })))
}

/// SSE stream of batch job progress snapshots.
async fn batch_events(
    State(svc): State<Svc>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(svc.subscribe_batch()).map(|res| match res {
        Ok(snap) => {
            let data = serde_json::to_string(&snap).unwrap_or_default();
            Ok(Event::default().event("batch").data(data))
        }
        Err(_) => Ok(Event::default().comment("lagged")),
    });
    Sse::new(stream).keep_alive(KeepAlive::new())
}
