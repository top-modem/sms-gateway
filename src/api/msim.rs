use std::{convert::Infallible, sync::Arc};
use axum::{extract::{Path, State}, response::{IntoResponse, Response, Sse, sse::{Event, KeepAlive}}, routing::{get, post, put}, Json, Router};
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::wrappers::BroadcastStream;
use crate::msim::{MsimService, ScanRequest, SelectRequest};

type Service = Arc<MsimService>;

pub fn routes(service: Service) -> Router {
    Router::new()
        .route("/msim/ports", get(ports))
        .route("/msim/{com}/slots", get(slots))
        .route("/msim/{com}/scan", post(scan))
        .route("/msim/{com}/slots/{slot}/select", post(select))
        .route("/msim/{com}/slots/{slot}/phone", put(phone))
        .route("/msim/operations/{id}", get(operation))
        .route("/msim/operations/{id}/cancel", post(cancel))
        .route("/msim/operations/{id}/events", get(events))
        .with_state(service)
}

fn respond(result: anyhow::Result<Value>, accepted: bool) -> Response {
    match result {
        Ok(data) => (if accepted { StatusCode::ACCEPTED } else { StatusCode::OK }, Json(json!({"success": true, "data": data}))).into_response(),
        Err(error) => {
            let message = error.to_string();
            let status = if message.starts_with("not found:") { StatusCode::NOT_FOUND }
                else if message.starts_with("invalid:") { StatusCode::BAD_REQUEST }
                else if message.starts_with("busy:") || message.starts_with("conflict:") { StatusCode::CONFLICT }
                else { StatusCode::INTERNAL_SERVER_ERROR };
            (status, Json(json!({"error": message}))).into_response()
        }
    }
}

async fn ports(State(service): State<Service>) -> Response { respond(service.ports().await, false) }
async fn slots(State(service): State<Service>, Path(com): Path<String>) -> Response { respond(service.inventory(&com).await, false) }
async fn scan(State(service): State<Service>, Path(com): Path<String>, Json(request): Json<ScanRequest>) -> Response {
    respond(service.scan(&com, request).await.and_then(|operation| Ok(serde_json::to_value(operation)?)), true)
}
async fn select(State(service): State<Service>, Path((com, slot)): Path<(String, i64)>, Json(request): Json<SelectRequest>) -> Response {
    respond(service.select(&com, slot, request).await.and_then(|operation| Ok(serde_json::to_value(operation)?)), true)
}
#[derive(Deserialize)]
struct PhoneRequest { expected_iccid: String, phone_number: String }
async fn phone(State(service): State<Service>, Path((com, slot)): Path<(String, i64)>, Json(request): Json<PhoneRequest>) -> Response {
    respond(service.phone(&com, slot, &request.expected_iccid, &request.phone_number).await, false)
}
async fn operation(State(service): State<Service>, Path(id): Path<String>) -> Response {
    respond(service.operation(&id).await.and_then(|operation| Ok(serde_json::to_value(operation)?)), false)
}
async fn cancel(State(service): State<Service>, Path(id): Path<String>) -> Response {
    respond(service.cancel(&id).await.and_then(|operation| Ok(serde_json::to_value(operation)?)), false)
}
async fn events(State(service): State<Service>, Path(id): Path<String>) -> Response {
    let receiver = service.subscribe();
    let initial = match service.operation(&id).await {
        Ok(operation) => operation,
        Err(error) => return respond(Err(error), false),
    };
    let snapshot = futures_util::stream::once(async move {
        Ok::<Event, Infallible>(Event::default().event("operation")
            .data(serde_json::to_string(&initial).unwrap_or_default()))
    });
    let stream = BroadcastStream::new(receiver).filter_map(move |result| {
        let id = id.clone();
        async move {
            match result {
                Ok(operation) if operation.id == id => Some(Ok(Event::default().event("operation")
                    .data(serde_json::to_string(&operation).unwrap_or_default()))),
                _ => None,
            }
        }
    });
    Sse::new(snapshot.chain(stream)).keep_alive(KeepAlive::new()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msim_api_preserves_error_and_acceptance_statuses() {
        for (message, status) in [
            ("not found: operation", StatusCode::NOT_FOUND),
            ("invalid: slot", StatusCode::BAD_REQUEST),
            ("conflict: card changed", StatusCode::CONFLICT),
            ("busy: port operation", StatusCode::CONFLICT),
            ("database unavailable", StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            assert_eq!(respond(Err(anyhow::anyhow!(message)), false).status(), status);
        }
        assert_eq!(respond(Ok(json!({"slots": []})), false).status(), StatusCode::OK);
        assert_eq!(respond(Ok(json!({"id": "operation"})), true).status(), StatusCode::ACCEPTED);
    }
}