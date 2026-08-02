use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::esim::{DownloadRequest, EsimError, EsimService};

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
