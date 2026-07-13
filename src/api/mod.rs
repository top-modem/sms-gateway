use fancy_regex::Regex;
use std::{convert::Infallible, sync::Arc, sync::OnceLock, time::Duration};

use axum::{
    extract::{Path, Query, State},
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::{delete, get, post, put},
    Json, Router,
};
use futures_util::StreamExt;
use log::debug;
use log::error;
use mime_guess::from_path;
use reqwest::{header, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx;
#[allow(unused_imports)]
pub use sse_manager::{CallEvent, SseManager};

use crate::{
    config::SmsStorage,
    db::{AppSetting, BarcodeScan, Call, Contact, Conversation, SimCard, Sms},
    firefox_api,
    modem::{ModemInfo as ModemModel, NetworkRegistrationStatus, OperatorInfo, SignalQuality, SmsType},
    phone_number::{import_phone_numbers, new_task_handle, call_exchange, sms_exchange, ussd_batch, PhoneNumberTask, TaskHandle},
    service_control,
    ModemManagerRef,
};

static PHONE_NUMBER_TASK: OnceLock<TaskHandle> = OnceLock::new();

/// Combined state for call routes that need both modem access and SSE broadcast.
#[derive(Clone)]
struct CallState {
    mm: ModemManagerRef,
    sse: Arc<SseManager>,
}

fn decode_sms_center(sms_center: &str) -> String {
    // Check if it's UCS2 encoded (contains sequences like 002B, 0030, etc.)
    if sms_center.contains("002B") || sms_center.contains("0030") {
        let mut decoded = String::new();
        let chars: Vec<char> = sms_center.chars().collect();

        for chunk in chars.chunks(4) {
            if chunk.len() == 4 {
                let hex_str: String = chunk.iter().collect();
                if let Ok(char_code) = u16::from_str_radix(&hex_str, 16) {
                    if char_code > 0 {
                        if let Some(ch) = char::from_u32(char_code as u32) {
                            decoded.push(ch);
                        }
                    }
                }
            }
        }

        if !decoded.is_empty() {
            return decoded;
        }
    }

    sms_center.to_string()
}

fn format_memory_status(memory_status: &str) -> String {
    // Parse +CPMS: "SM",5,10,"SM",5,10,"SM",5,10 format
    if let Ok(re) =
        Regex::new(r#"\+CPMS:\s*"([^"]+)",(\d+),(\d+),"([^"]+)",(\d+),(\d+),"([^"]+)",(\d+),(\d+)"#)
    {
        if let Ok(Some(captures)) = re.captures(memory_status) {
            if let (Some(used1), Some(max1), Some(used2), Some(max2), Some(used3), Some(max3)) = (
                captures.get(2),
                captures.get(3),
                captures.get(5),
                captures.get(6),
                captures.get(8),
                captures.get(9),
            ) {
                return format!(
                    "Read: {}/{}, Write: {}/{}, Receive: {}/{}",
                    used1.as_str(),
                    max1.as_str(),
                    used2.as_str(),
                    max2.as_str(),
                    used3.as_str(),
                    max3.as_str()
                );
            }
        }
    }

    memory_status.to_string()
}

mod auth;
pub(crate) mod sse_manager;

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend/dist"]
struct Asset;

pub async fn run_api(
    modem_manager: ModemManagerRef,
    server_host: &str,
    server_port: &u16,
    username: &str,
    password: &str,
    sse_manager: Arc<SseManager>,
) -> anyhow::Result<()> {
    let api = Router::new()
        .route("/check", get(check))
        .route("/service/status", get(get_service_status))
        .route("/service/start", post(start_service))
        .route("/service/stop", post(stop_service))
        .route("/sms", get(get_sms_paginated))
        .route("/sms", post(send_sms).with_state((modem_manager.clone(), sse_manager.clone())))
        .route("/sms/sse", get(sse_events).with_state(sse_manager.clone()))
        // 破坏性改造: 删除所有/api/device路径，改为/api/sims
        .route(
            "/sims/info",
            get(get_all_sim_info).with_state(modem_manager.clone()),
        )
        .route(
            "/sims/{sim_id}/refresh",
            get(refresh_sim_sms).with_state(modem_manager.clone()),
        )
        .route(
            "/at/{com_port}",
            post(send_at_command_handler).with_state(modem_manager.clone()),
        )
        .route("/contacts", get(get_contacts))
        .route("/contacts", post(create_contact))
        .route("/contacts/{id}", delete(delete_contact_by_id))
        .route("/conversation", get(get_conversation))
        .route("/conversations/{id}/unread", post(get_conversation_unread))
        .route("/sim-cards", get(get_all_sim_cards)) // 保留用于管理
        .route("/sims/stats", get(get_sim_sms_stats))
        .route(
            "/sims/{sim_id}/info",
            get(get_enhanced_sim_info).with_state(modem_manager.clone()),
        )
        .route(
            "/sim-cards/{sim_id}/alias",
            put(update_sim_alias).with_state(modem_manager.clone()),
        )
        .route(
            "/sim-cards/{sim_id}/phone",
            put(update_sim_phone).with_state(modem_manager.clone()),
        )
        .route(
            "/sims/{sim_id}/storage",
            get(get_sms_storage_status).with_state(modem_manager.clone()),
        )
        .route(
            "/sims/{sim_id}/storage",
            put(set_sms_storage).with_state(modem_manager.clone()),
        )
        .route(
            "/sims/{sim_id}/phone",
            post(set_sim_phone_number).with_state(modem_manager.clone()),
        )
        // ── Phone number management routes ──────────────────────────────────────
        .route(
            "/phone-numbers/import",
            post(phone_numbers_import).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/barcode-scan",
            post(phone_numbers_barcode_scan).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/barcode-scans",
            get(phone_numbers_barcode_scans).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/barcode-scans",
            delete(phone_numbers_barcode_scans_clear).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/barcode-scans/import",
            post(phone_numbers_barcode_scans_import).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/call-exchange",
            post(phone_numbers_call_exchange).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/sms-exchange",
            post(phone_numbers_sms_exchange).with_state(modem_manager.clone()),
        )
        .route(
            "/phone-numbers/ussd",
            post(phone_numbers_ussd).with_state(modem_manager.clone()),
        )
        .route("/phone-numbers/status", get(phone_numbers_status))
        // ── 火狐狸 platform integration routes ─────────────────────────────
        .route("/settings/firefox-api-key", get(get_firefox_api_key))
        .route("/settings/firefox-api-key", put(set_firefox_api_key))
        .route("/firefox/countries", get(get_firefox_countries))
        .route(
            "/firefox/upload",
            post(firefox_upload).with_state(modem_manager.clone()),
        )
        .route("/firefox/batch-status", post(firefox_batch_status))
        .route("/firefox/batch-uploads", get(firefox_batch_uploads))
        .route("/firefox/delete-batch", post(firefox_delete_batch))
        .route("/firefox/delete-batch-by-id", post(firefox_delete_batch_by_id))
        .route("/firefox/delete-country", post(firefox_delete_country))
        .route("/firefox/delete-all", post(firefox_delete_all))
        .route("/firefox/wait-list", get(firefox_wait_list))
        .route("/firefox/result-list", get(firefox_result_list))
        .route("/firefox/upload-sms", post(firefox_upload_sms))
        .route("/firefox/platform-items", get(firefox_platform_items))
        .route("/firefox/platform-items/{item_id}", get(firefox_platform_item_detail))
        .route("/firefox/platform-statistics", get(firefox_platform_statistics))
        .route("/firefox/platform-rejection-reasons", get(firefox_platform_rejection_reasons))
        // ── Firefox upload retry queue routes ────────────────────────────
        .route("/firefox/upload-retry/stats", get(firefox_upload_retry_stats))
        .route("/firefox/upload-retry/queue", get(firefox_upload_retry_queue))
        .route("/firefox/upload-retry/dead-letter", get(firefox_upload_retry_dead_letter))
        .route("/firefox/upload-retry/{id}/retry", post(firefox_upload_retry_manual))
        .route("/firefox/upload-retry/{id}", delete(firefox_upload_retry_delete))
        // ── Voice call routes ─────────────────────────────────────────────
        .route(
            "/calls",
            get(get_calls),
        )
        .route(
            "/calls/sse",
            get(calls_sse_events).with_state(sse_manager.clone()),
        )
        .route(
            "/calls/make",
            post(make_call).with_state(CallState { mm: modem_manager.clone(), sse: sse_manager.clone() }),
        )
        .route(
            "/calls/answer",
            post(answer_call).with_state(modem_manager.clone()),
        )
        .route(
            "/calls/hangup",
            post(hangup_call).with_state(CallState { mm: modem_manager.clone(), sse: sse_manager.clone() }),
        )
        .route(
            "/calls/{id}/recording",
            get(get_call_recording),
        )
        .route(
            "/calls/{id}/transcript",
            get(get_call_transcript),
        )
        // ── MMS routes (EC20/EC25 via AT+QMMSEDIT/AT+QMMSEND) ────────────
        .route(
            "/mms",
            post(create_mms).with_state(modem_manager.clone()),
        )
        .route("/mms", get(get_mms_paginated))
        .route("/mms/{id}", get(get_mms_detail))
        .route(
            "/sim-cards/{sim_id}/mms-profile",
            get(get_mms_profile),
        )
        .route(
            "/sim-cards/{sim_id}/mms-profile",
            put(set_mms_profile),
        )
        .route("/mms/inbox", get(get_mms_inbox_paginated))
        .route("/mms/inbox/{id}", get(get_mms_inbox_detail))
        .route("/mms/inbox/{id}/parts/{part_id}", get(get_mms_inbox_part))
        .layer(axum::middleware::from_fn_with_state(
            (username.to_string(), password.to_string()),
            auth::basic_auth,
        ));

    let app = Router::new()
        .nest_service("/api", api)
        .fallback(static_handler);

    let listener =
        tokio::net::TcpListener::bind(format!("{}:{}", server_host, server_port)).await?;
    debug!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Serialize)]
pub struct PaginatedSmsResponse {
    data: Vec<Sms>,
    total: i64,
    page: u32,
    per_page: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct SmsQuery {
    page: u32,
    per_page: u32,
    #[serde(default)]
    contact_id: Option<String>,
    /// "inbox" = received, "sent" = sent. Returns SmsRow with contact_name resolved.
    #[serde(default)]
    direction: Option<String>,
}

#[derive(Serialize)]
struct ServiceStatusResponse {
    running: bool,
}

async fn get_service_status() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(ServiceStatusResponse {
            running: service_control::is_running(),
        }),
    )
}

async fn start_service() -> impl IntoResponse {
    service_control::start();
    (
        StatusCode::OK,
        Json(ServiceStatusResponse {
            running: true,
        }),
    )
}

async fn stop_service() -> impl IntoResponse {
    service_control::stop();
    (
        StatusCode::OK,
        Json(ServiceStatusResponse {
            running: false,
        }),
    )
}

async fn get_sms_paginated(Query(query): Query<SmsQuery>) -> Response {
    // Direction-filtered view (inbox / sent) — returns SmsRow (includes contact_name)
    if let Some(ref dir) = query.direction {
        let send = dir.as_str() == "sent";
        return match crate::db::Sms::paginate_by_direction(send, query.page, query.per_page).await {
            Ok((rows, total)) => Json(json!({
                "data": rows,
                "total": total,
                "page": query.page,
                "per_page": query.per_page,
            })).into_response(),
            Err(e) => {
                error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to get SMS: {}", e)).into_response()
            }
        };
    }

    let result = match &query.contact_id {
        Some(contact_id) => {
            Sms::paginate_by_contact_id(contact_id, query.page, query.per_page).await
        }
        None => Sms::paginate(query.page, query.per_page).await,
    };

    let (sms_list, total) = match result {
        Ok(res) => res,
        Err(e) => {
            error!("{}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get SMS: {}", e),
            )
                .into_response();
        }
    };

    Json(PaginatedSmsResponse {
        data: sms_list,
        total,
        page: query.page,
        per_page: query.per_page,
    })
    .into_response()
}

async fn send_sms(
    State((modem_manager, sse_manager)): State<(ModemManagerRef, Arc<SseManager>)>,
    Json(mut payload): Json<SmsPayload>,
) -> impl IntoResponse {
    // Resolve the sim_id: use sim_id directly, or look it up from phone_number.
    let sim_id = match payload.sim_id.clone() {
        Some(id) => id,
        None => match payload.phone_number.as_deref() {
            Some(phone) => match modem_manager.find_sim_id_by_phone_number(phone).await {
                Some(id) => id,
                None => return (
                    StatusCode::NOT_FOUND,
                    format!("No SIM card found with phone number: {}", phone),
                ).into_response(),
            },
            None => return (
                StatusCode::BAD_REQUEST,
                "Either sim_id or phone_number must be provided".to_string(),
            ).into_response(),
        },
    };

    if payload.new {
        payload.contact.find_or_create().await.unwrap();
    }

    match modem_manager
        .send_sms(&sim_id, &payload.contact, &payload.message)
        .await
    {
        Ok((sms_id, contact_id)) => {
            if let Ok(convs) =
                Conversation::query_by_contact_ids(&[contact_id.clone()]).await
            {
                sse_manager.send(convs);
            }
            (
                StatusCode::OK,
                Json(json!({ "sms_id": sms_id, "contact_id": contact_id })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Send failed: {}", e),
        )
            .into_response(),
    }
}

async fn get_all_sim_info(State(modem_manager): State<ModemManagerRef>) -> Response {
    use futures::future::join_all;
    use tokio::time::{timeout, Duration};

    fn to_data_error<T, E: ToString>(result: Result<T, E>) -> (Option<T>, Option<String>) {
        match result {
            Ok(data) => (Some(data), None),
            Err(e) => (None, Some(e.to_string())),
        }
    }

    let sim_ids = modem_manager.get_sim_ids();

    // 并发获取所有调制解调器信息，带超时控制
    let sim_ids = sim_ids.await;
    let modem_futures: Vec<_> = sim_ids
        .iter()
        .map(|sim_id| {
            let sim_id = sim_id.clone();
            let modem_manager = modem_manager.clone();
            async move {
                log::debug!("Getting SIM details for: {}", sim_id);

                // 并发执行所有AT命令，每个都有超时保护
                let signal_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.get_signal_quality(&sim_id),
                );
                let network_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.check_network_registration(&sim_id),
                );
                let operator_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.check_operator(&sim_id),
                );
                let model_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.get_modem_model(&sim_id),
                );
                let sms_center_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.get_sms_center(&sim_id),
                );
                let sim_status_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.get_sim_status(&sim_id),
                );
                let memory_status_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.get_memory_status(&sim_id),
                );
                let imei_future = timeout(
                    Duration::from_secs(5),
                    modem_manager.get_imei(&sim_id),
                );

                let (
                    signal_result,
                    network_result,
                    operator_result,
                    model_result,
                    sms_center_result,
                    sim_status_result,
                    memory_status_result,
                    imei_result,
                ) = tokio::join!(
                    signal_future,
                    network_future,
                    operator_future,
                    model_future,
                    sms_center_future,
                    sim_status_future,
                    memory_status_future,
                    imei_future
                );

                let (signal_data, signal_error) = to_data_error(
                    signal_result
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("Signal quality timeout"))),
                );
                let (network_data, network_error) = to_data_error(
                    network_result
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("Network registration timeout"))),
                );
                let (operator_data, operator_error) = to_data_error(
                    operator_result
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("Operator info timeout"))),
                );
                let (model_data, model_error) = to_data_error(
                    model_result.unwrap_or_else(|_| Err(anyhow::anyhow!("Model info timeout"))),
                );
                let (sms_center_data, sms_center_error) = to_data_error(
                    sms_center_result
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("SMS center timeout"))),
                );
                let (sim_status_data, sim_status_error) = to_data_error(
                    sim_status_result
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("SIM status timeout"))),
                );
                let (memory_status_data, memory_status_error) = to_data_error(
                    memory_status_result
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("Memory status timeout"))),
                );
                let (imei_data, _imei_error) = to_data_error(
                    imei_result.unwrap_or_else(|_| Err(anyhow::anyhow!("IMEI timeout"))),
                );

                let sim_data = modem_manager.get_sim_card_cached(&sim_id).await;

                log::debug!("All AT commands completed for {}", sim_id);

                (
                    sim_id,
                    signal_data,
                    signal_error,
                    network_data,
                    network_error,
                    operator_data,
                    operator_error,
                    model_data,
                    model_error,
                    sim_data,
                    sms_center_data,
                    sms_center_error,
                    sim_status_data,
                    sim_status_error,
                    memory_status_data,
                    memory_status_error,
                    imei_data,
                )
            }
        })
        .collect();

    let modem_results = join_all(modem_futures).await;

    // 构建响应数据
    let mut details = Vec::new();
    for (
        sim_id,
        signal_data,
        _signal_error,
        network_data,
        _network_error,
        operator_data,
        _operator_error,
        model_data,
        _model_error,
        sim_data,
        sms_center_data,
        _sms_center_error,
        sim_status_data,
        _sim_status_error,
        memory_status_data,
        _memory_status_error,
        imei_data,
    ) in modem_results
    {
        let (sim_data, _sim_error): (Option<SimCard>, Option<String>) = (sim_data, None);

        // Determine if this modem has a SIM card inserted
        let has_sim = !sim_id.starts_with("fallback_sim_");
        let json_sim_id: Option<&str> = if has_sim { Some(&sim_id) } else { None };

        // Get the SIM card effective alias for display
        let _display_name = if let Some(ref sim) = sim_data {
            sim.get_effective_alias()
        } else {
            format!("SIM {}", sim_id)
        };

        // Get modem info for com_port and baud_rate
        let (com_port, baud_rate) = match modem_manager.get_modem(&sim_id).await {
            Some(modem) => (modem.com_port.clone(), modem.baud_rate),
            _ => ("N/A".to_string(), 0),
        };

        let phone_number = sim_data.as_ref().and_then(|s| s.phone_number.clone());

        details.push(json!({
            "available": true,
            "sim_id": json_sim_id,
            "has_sim": has_sim,
            "name": sim_id.clone(),
            "com_port": com_port,
            "baud_rate": baud_rate,
            "signal_quality": if has_sim { signal_data } else { None },
            "network_registration": if has_sim { network_data } else { None },
            "operator_info": if has_sim { operator_data } else { None },
            "model_info": model_data,
            "imei": imei_data,
            "sms_center": if has_sim { sms_center_data.as_ref().and_then(|s| s.as_ref()).map(|s| decode_sms_center(s)) } else { None },
            "sim_status": if has_sim { sim_status_data } else { None },
            "memory_status": if has_sim { memory_status_data.as_ref().and_then(|s| s.as_ref()).map(|s| format_memory_status(s)) } else { None },
            "phone_number": if has_sim { phone_number } else { None }
        }));
    }

    // Append stubs for ports that failed to open at startup
    let unavailable_ports = modem_manager.unavailable_ports.read().await.clone();
    for (com_port, baud_rate) in unavailable_ports {
        details.push(json!({
            "available": false,
            "sim_id": null,
            "has_sim": false,
            "name": null,
            "com_port": com_port,
            "baud_rate": baud_rate,
            "signal_quality": null,
            "network_registration": null,
            "operator_info": null,
            "model_info": null,
            "imei": null,
            "sms_center": null,
            "sim_status": null,
            "memory_status": null,
            "phone_number": null
        }));
    }

    // Sort by COM port number (COM1, COM2, ..., COM10, COM11, ...)
    details.sort_by_key(|entry| {
        entry["com_port"]
            .as_str()
            .unwrap_or("")
            .trim_start_matches(|c: char| !c.is_ascii_digit())
            .parse::<u32>()
            .unwrap_or(u32::MAX)
    });

    (StatusCode::OK, Json(details)).into_response()
}

#[derive(Deserialize)]
struct AtCommandRequest {
    command: String,
}

async fn send_at_command_handler(
    Path(com_port): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
    Json(body): Json<AtCommandRequest>,
) -> Response {
    match modem_manager.send_at_command(&com_port, &body.command).await {
        Ok(response) => (StatusCode::OK, Json(json!({"response": response}))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

async fn refresh_sim_sms(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
) -> Response {
    match modem_manager
        .read_sms_sync_insert(&sim_id, SmsType::RecUnread)
        .await
    {
        Ok(_) => (StatusCode::OK).into_response(),
        Err(err) => (StatusCode::BAD_GATEWAY, err.to_string()).into_response(),
    }
}

async fn check() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

#[derive(Deserialize)]
struct PhoneNumberImportEntry {
    iccid: String,
    msisdn: String,
}

#[derive(Deserialize)]
struct PhoneNumberImportRequest {
    entries: Vec<PhoneNumberImportEntry>,
}

#[derive(Deserialize)]
struct PhoneNumberUssdRequest {
    code: String,
}

fn get_phone_number_task() -> &'static TaskHandle {
    PHONE_NUMBER_TASK.get_or_init(new_task_handle)
}

#[derive(Serialize)]
struct BarcodeScanResponse {
    id: i64,
    iccid: String,
    msisdn: String,
    phone_number: Option<String>,
    created_at: Option<String>,
}

#[derive(Deserialize)]
struct BarcodeScanRequest {
    iccid: String,
    msisdn: String,
}

fn validate_barcode_iccid(iccid: &str) -> bool {
    let digits: String = iccid.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.starts_with("8944") && digits.len() == 20
}

fn validate_barcode_msisdn(msisdn: &str) -> bool {
    let digits: String = msisdn.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() != 11 {
        return false;
    }
    ["077", "071", "073", "074", "075", "078", "079"]
        .iter()
        .any(|prefix| digits.starts_with(prefix))
}

fn normalize_msisdn(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('+') {
        let digits: String = trimmed[1..].chars().filter(|c| c.is_ascii_digit()).collect();
        let normalized = digits.trim_start_matches('0');
        if normalized.is_empty() {
            "+".to_string()
        } else {
            format!("+{}", normalized)
        }
    } else {
        let digits: String = trimmed.chars().filter(|c| c.is_ascii_digit()).collect();
        digits.trim_start_matches('0').to_string()
    }
}

async fn phone_numbers_barcode_scan(
    State(mm): State<ModemManagerRef>,
    Json(request): Json<BarcodeScanRequest>,
) -> Response {
    let iccid: String = request.iccid.chars().filter(|c| c.is_ascii_digit()).collect();
    let msisdn: String = request.msisdn.chars().filter(|c| c.is_ascii_digit()).collect();

    if !validate_barcode_iccid(&iccid) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid ICCID: must be 20 digits starting with 8944"})),
        )
            .into_response();
    }

    if !validate_barcode_msisdn(&msisdn) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid MSISDN: must be 11 digits starting with 077/071/073/074/075/078/079"})),
        )
            .into_response();
    }

    if mm.get_modem(&iccid).await.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Scanned ICCID is not detected in any COM port"})),
        )
            .into_response();
    }

    match BarcodeScan::upsert(&iccid, &msisdn).await {
        Ok(_) => (StatusCode::OK, Json(json!({"iccid": iccid, "msisdn": msisdn}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to save scan: {}", e)})),
        )
            .into_response(),
    }
}

async fn phone_numbers_barcode_scans() -> Response {
    match BarcodeScan::list_unimported().await {
        Ok(rows) => {
            let response: Vec<BarcodeScanResponse> = rows
                .into_iter()
                .map(|r| BarcodeScanResponse {
                    id: r.id,
                    iccid: r.iccid,
                    msisdn: r.msisdn,
                    phone_number: r.phone_number,
                    created_at: r.created_at.map(|dt| dt.to_string()),
                })
                .collect();
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to list scans: {}", e)})),
        )
            .into_response(),
    }
}

async fn phone_numbers_barcode_scans_clear() -> Response {
    match BarcodeScan::delete_all().await {
        Ok(_) => (StatusCode::OK, Json(json!({"status": "cleared"}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to clear scans: {}", e)})),
        )
            .into_response(),
    }
}

async fn phone_numbers_barcode_scans_import(
    State(mm): State<ModemManagerRef>,
) -> Response {
    let scans = match BarcodeScan::list_unimported().await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to list scans: {}", e)})),
            )
                .into_response();
        }
    };

    let mut imported_ids = Vec::new();
    let mut failed = None;

    for scan in scans {
        let normalized = normalize_msisdn(&scan.msisdn);
        if let Err(e) = mm.set_sim_phone_number(&scan.iccid, &normalized).await {
            failed = Some(json!({
                "iccid": scan.iccid,
                "error": format!("Failed to write phone number to SIM: {}", e),
            }));
            break;
        }
        imported_ids.push(scan.id);
    }

    if let Some(err) = failed {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": err,
                "imported_count": imported_ids.len(),
            })),
        )
            .into_response();
    }

    if let Err(e) = BarcodeScan::mark_imported(&imported_ids).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to mark scans imported: {}", e)})),
        )
            .into_response();
    }

    (StatusCode::OK, Json(json!({"imported_count": imported_ids.len()}))).into_response()
}

async fn phone_numbers_import(
    State(mm): State<ModemManagerRef>,
    Json(payload): Json<PhoneNumberImportRequest>,
) -> Response {
    let entries: Vec<(String, String)> = payload
        .entries
        .into_iter()
        .map(|e| (e.iccid, normalize_msisdn(&e.msisdn)))
        .collect();

    let task = get_phone_number_task().clone();
    tokio::spawn(import_phone_numbers(mm, task, entries));

    (StatusCode::ACCEPTED, Json(json!({"status": "started"}))).into_response()
}

async fn phone_numbers_call_exchange(State(mm): State<ModemManagerRef>) -> Response {
    let task = get_phone_number_task().clone();
    tokio::spawn(call_exchange(mm, task));
    (StatusCode::ACCEPTED, Json(json!({"status": "started"}))).into_response()
}

async fn phone_numbers_sms_exchange(State(mm): State<ModemManagerRef>) -> Response {
    let task = get_phone_number_task().clone();
    tokio::spawn(sms_exchange(mm, task));
    (StatusCode::ACCEPTED, Json(json!({"status": "started"}))).into_response()
}

async fn phone_numbers_ussd(
    State(mm): State<ModemManagerRef>,
    Json(payload): Json<PhoneNumberUssdRequest>,
) -> Response {
    let task = get_phone_number_task().clone();
    let code = payload.code;
    tokio::spawn(ussd_batch(mm, task, code));
    (StatusCode::ACCEPTED, Json(json!({"status": "started"}))).into_response()
}

async fn phone_numbers_status() -> Response {
    let task = get_phone_number_task().read().await;
    let task_clone = PhoneNumberTask {
        running: task.running,
        task_type: task.task_type.clone(),
        total: task.total,
        done: task.done,
        current: task.current.clone(),
        errors: task.errors.clone(),
        results: task.results.clone(),
    };
    Json(task_clone).into_response()
}

async fn get_contacts() -> Json<Vec<Contact>> {
    let contacts = Contact::query_all().await.unwrap();
    Json(contacts)
}

async fn get_conversation() -> Json<Vec<Conversation>> {
    let conversation = Conversation::query_all().await.unwrap();
    Json(conversation)
}

async fn create_contact(Json(payload): Json<Contact>) -> Response {
    match Contact::insert(&payload).await {
        Ok(id) => (StatusCode::OK, Json(id)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_contact_by_id(Path(id): Path<String>) -> Response {
    match Contact::delete_by_id(&id).await {
        Ok(true) => (StatusCode::OK).into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "Contact not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ─── Voice call handlers ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct MakeCallRequest {
    sim_id: String,
    phone: String,
}

#[derive(Deserialize)]
struct SimIdRequest {
    sim_id: String,
}

#[derive(Deserialize)]
struct CallsQuery {
    sim_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 { 50 }

async fn get_calls(Query(q): Query<CallsQuery>) -> Response {
    let result = match q.sim_id {
        Some(sim_id) => Call::query_by_sim(&sim_id, q.limit).await.map(|v| {
            let total = v.len() as i64;
            json!({ "data": v, "total": total })
        }),
        None => Call::query_all(q.limit, q.offset).await.map(|(v, total)| {
            json!({ "data": v, "total": total })
        }),
    };
    match result {
        Ok(data) => Json(data).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_call_recording(Path(id): Path<String>) -> Response {
    match Call::get_recording(&id).await {
        Ok(Some(data)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "audio/amr"),
                (header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}.amr\"", id).leak()),
            ],
            data,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_call_transcript(Path(id): Path<String>) -> Response {
    match Call::get_transcript(&id).await {
        Ok(Some(text)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            text,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn make_call(State(cs): State<CallState>, Json(body): Json<MakeCallRequest>) -> Response {
    match cs.mm.make_call(&body.sim_id, &body.phone, cs.sse.clone()).await {
        Ok(call_id) => {
            cs.sse.send_call_event(CallEvent {
                event_type: "outbound_call_started".into(),
                sim_id: body.sim_id.clone(),
                call_id: call_id.clone(),
                phone: Some(body.phone.clone()),
                direction: "outbound".into(),
            });
            Json(json!({ "call_id": call_id })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn answer_call(State(mm): State<ModemManagerRef>, Json(body): Json<SimIdRequest>) -> Response {
    match mm.answer_call(&body.sim_id).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn hangup_call(State(cs): State<CallState>, Json(body): Json<SimIdRequest>) -> Response {
    match cs.mm.hangup_call(&body.sim_id).await {
        Ok(Some((call_id, _status))) => {
            cs.sse.send_call_event(CallEvent {
                event_type: "call_ended".into(),
                sim_id: body.sim_id.clone(),
                call_id,
                phone: None,
                direction: "outbound".into(),
            });
            StatusCode::OK.into_response()
        }
        Ok(None) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn calls_sse_events(State(sse_manager): State<Arc<SseManager>>) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = sse_manager.subscribe_calls();

    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Ok(data) = serde_json::to_string(&event) {
                        let sse_event = Event::default().event("call_event").data(data);
                        return Some((Ok::<_, Infallible>(sse_event), rx));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    error!("Calls SSE receiver lagged by {} messages", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    match Asset::get(path) {
        Some(content) => {
            let mime = from_path(path).first_or_octet_stream();
            Response::builder()
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(content.data.into())
                .unwrap()
        }
        None => {
            if let Some(index) = Asset::get("index.html") {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/html")
                    .body(index.data.into())
                    .unwrap()
            } else {
                (StatusCode::NOT_FOUND, "File not found").into_response()
            }
        }
    }
}

async fn sse_events(
    State(sse_manager): State<Arc<SseManager>>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx_stream = tokio_stream::wrappers::BroadcastStream::new(sse_manager.subscribe()).map(
        |msg| match msg {
            Ok(cnversations) => {
                let timestamp = chrono::Utc::now().timestamp_millis();
                Ok(Event::default()
                    .id(timestamp.to_string())
                    .event("conversations")
                    .json_data(&cnversations)
                    .unwrap())
            }
            Err(_) => Ok(Event::default()
                .event("error")
                .comment("Failed to receive broadcast message")),
        },
    );

    Sse::new(rx_stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .event(
                Event::default()
                    .event("keep-alive")
                    .id(chrono::Utc::now().timestamp_millis().to_string()),
            ),
    )
}

async fn get_conversation_unread(Path(id): Path<String>) -> Response {
    match Sms::query_unread_by_contact_id(&id).await {
        Ok(messages) => (StatusCode::OK, Json(messages)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ── MMS routes ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct MmsAttachmentPayload {
    filename: String,
    #[serde(default)]
    content_type: Option<String>,
    /// Standard base64-encoded file bytes (no data: URL prefix).
    base64: String,
}

#[derive(Deserialize)]
pub struct CreateMmsPayload {
    sim_id: String,
    to: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    attachments: Vec<MmsAttachmentPayload>,
}

#[derive(Deserialize, Debug)]
pub struct MmsQuery {
    #[serde(default = "default_page")]
    page: u32,
    #[serde(default = "default_per_page")]
    per_page: u32,
}

fn default_page() -> u32 {
    1
}
fn default_per_page() -> u32 {
    20
}

#[derive(Serialize)]
pub struct PaginatedMmsResponse {
    data: Vec<crate::db::MmsMessage>,
    total: i64,
    page: u32,
    per_page: u32,
}

/// Enqueue a new MMS send job. The actual AT command exchange happens asynchronously
/// in the background MMS worker — poll GET /api/mms/{id} (or watch SSE) for the result.
async fn create_mms(
    State(modem_manager): State<ModemManagerRef>,
    Json(payload): Json<CreateMmsPayload>,
) -> Response {
    if !modem_manager.get_sim_ids().await.contains(&payload.sim_id) {
        return (
            StatusCode::NOT_FOUND,
            format!("No active modem found for SIM {}", payload.sim_id),
        )
            .into_response();
    }
    if payload.to.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "`to` must not be empty".to_string()).into_response();
    }

    let mms_id = match crate::db::MmsMessage::insert_queued(
        &payload.sim_id,
        &payload.to,
        payload.subject.as_deref(),
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to enqueue MMS job: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to enqueue MMS: {}", e))
                .into_response();
        }
    };

    for attachment in &payload.attachments {
        use base64::prelude::*;
        let data = match BASE64_STANDARD.decode(&attachment.base64) {
            Ok(d) => d,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid base64 for attachment {}: {}", attachment.filename, e),
                )
                    .into_response();
            }
        };
        if let Err(e) = crate::db::MmsAttachment::insert(
            &mms_id,
            &attachment.filename,
            attachment.content_type.as_deref(),
            &data,
        )
        .await
        {
            error!("Failed to save MMS attachment: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to save attachment: {}", e),
            )
                .into_response();
        }
    }

    (StatusCode::ACCEPTED, Json(json!({ "id": mms_id, "status": "queued" }))).into_response()
}

async fn get_mms_paginated(Query(query): Query<MmsQuery>) -> Response {
    match crate::db::MmsMessage::query_paginated(query.per_page as i64, ((query.page - 1) * query.per_page) as i64)
        .await
    {
        Ok((data, total)) => Json(PaginatedMmsResponse {
            data,
            total,
            page: query.page,
            per_page: query.per_page,
        })
        .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to list MMS: {}", e)).into_response(),
    }
}

async fn get_mms_detail(Path(id): Path<String>) -> Response {
    let job = match crate::db::MmsMessage::find_by_id(&id).await {
        Ok(Some(job)) => job,
        Ok(None) => return (StatusCode::NOT_FOUND, "MMS job not found".to_string()).into_response(),
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load MMS: {}", e)).into_response()
        }
    };
    let attachments = crate::db::MmsAttachment::list_meta(&id).await.unwrap_or_default();
    Json(json!({ "job": job, "attachments": attachments })).into_response()
}

async fn get_mms_profile(Path(sim_id): Path<String>) -> Response {
    match crate::db::MmsProfile::get(&sim_id).await {
        Ok(Some(profile)) => Json(profile).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "SIM card not found".to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load MMS profile: {}", e))
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetMmsProfilePayload {
    apn: Option<String>,
    mmsc: Option<String>,
    proxy_host: Option<String>,
    proxy_port: Option<i32>,
}

async fn set_mms_profile(
    Path(sim_id): Path<String>,
    Json(payload): Json<SetMmsProfilePayload>,
) -> Response {
    match crate::db::MmsProfile::set(
        &sim_id,
        payload.apn.as_deref(),
        payload.mmsc.as_deref(),
        payload.proxy_host.as_deref(),
        payload.proxy_port,
    )
    .await
    {
        Ok(()) => (StatusCode::OK, Json(json!({ "ok": true }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to update MMS profile: {}", e))
            .into_response(),
    }
}

#[derive(Serialize)]
pub struct PaginatedMmsInboxResponse {
    data: Vec<crate::db::MmsInboxNotification>,
    total: i64,
    page: u32,
    per_page: u32,
}

/// Phase 1: lists detected MMS WAP-push notifications (status will read
/// "notified" until the fetch/decode phase is implemented).
async fn get_mms_inbox_paginated(Query(query): Query<MmsQuery>) -> Response {
    match crate::db::MmsInboxNotification::query_paginated(
        query.per_page as i64,
        ((query.page - 1) * query.per_page) as i64,
    )
    .await
    {
        Ok((data, total)) => Json(PaginatedMmsInboxResponse {
            data,
            total,
            page: query.page,
            per_page: query.per_page,
        })
        .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to list MMS inbox: {}", e))
            .into_response(),
    }
}

async fn get_mms_inbox_detail(Path(id): Path<String>) -> Response {
    match crate::db::MmsInboxNotification::find_by_id(&id).await {
        Ok(Some(item)) => {
            let parts = crate::db::MmsInboxPart::list_meta(&id).await.unwrap_or_default();
            Json(json!({ "notification": item, "parts": parts })).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "MMS notification not found".to_string()).into_response(),
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load MMS notification: {}", e))
                .into_response()
        }
    }
}

/// Download/view a single decoded MMS part's raw bytes (image, SMIL, text, ...).
async fn get_mms_inbox_part(Path((_id, part_id)): Path<(String, String)>) -> Response {
    match crate::db::MmsInboxPart::fetch_data(&part_id).await {
        Ok(Some((content_type, filename, data))) => {
            let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
            let filename = filename.unwrap_or_else(|| part_id.clone());
            // `.leak()` returns `&'static mut str`; explicitly type as `&'static str`
            // (a valid mut->immut reborrow) so the array elements match the `&str`
            // header-value type axum expects -- otherwise both being `&mut str`
            // leaves the array typed `[(HeaderName, &mut str); 2]`, which has no
            // `IntoResponse` impl.
            let content_type: &'static str = content_type.leak();
            let content_disposition: &'static str =
                format!("inline; filename=\"{}\"", filename).leak();
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, content_type),
                    (header::CONTENT_DISPOSITION, content_disposition),
                ],
                data,
            )
                .into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, "MMS part not found".to_string()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to load MMS part: {}", e)).into_response(),
    }
}

#[derive(serde::Deserialize)]
pub struct SmsPayload {
    /// ICCID-based SIM identifier. Either this or `phone_number` must be supplied.
    sim_id: Option<String>,
    /// SIM phone number (e.g. "+8618126101015"). Used as an alternative to `sim_id`.
    phone_number: Option<String>,
    contact: Contact,
    message: String,
    new: bool,
}

#[derive(Serialize)]
pub struct EnhancedModemInfo {
    pub name: String,
    pub com_port: String,
    pub baud_rate: u32,
    pub signal_quality: Option<SignalQuality>,
    pub network_registration: Option<NetworkRegistrationStatus>,
    pub operator_info: Option<OperatorInfo>,
    pub model_info: Option<ModemModel>,
    pub sms_center: Option<String>,
    pub sim_status: Option<String>,
    pub memory_status: Option<String>,
}

#[derive(Serialize)]
#[allow(dead_code)]
pub struct ModemInfo {
    pub name: String,
    pub com_port: String,
    pub baud_rate: u32,
}

async fn get_sim_sms_stats() -> Response {
    match Sms::count_by_sim_id().await {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(e) => {
            error!("Failed to get SIM SMS stats: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed: {}", e)).into_response()
        }
    }
}

async fn get_all_sim_cards() -> Response {
    match SimCard::query_all().await {
        Ok(sim_cards) => (StatusCode::OK, Json(sim_cards)).into_response(),
        Err(e) => {
            error!("Failed to get SIM cards: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get SIM cards: {}", e),
            )
                .into_response()
        }
    }
}

async fn get_enhanced_sim_info(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
) -> Response {
    match modem_manager.get_modem(&sim_id).await {
        Some(modem) => {
            let sms_center_raw = modem_manager.get_sms_center(&sim_id).await.ok().flatten();
            let memory_status_raw = modem_manager
                .get_memory_status(&sim_id)
                .await
                .ok()
                .flatten();

            let enhanced_info = EnhancedModemInfo {
                name: sim_id.clone(),
                com_port: modem.com_port.clone(),
                baud_rate: modem.baud_rate,
                signal_quality: modem_manager
                    .get_signal_quality(&sim_id)
                    .await
                    .ok()
                    .flatten(),
                network_registration: modem_manager
                    .check_network_registration(&sim_id)
                    .await
                    .ok()
                    .flatten(),
                operator_info: modem_manager.check_operator(&sim_id).await.ok().flatten(),
                model_info: modem_manager.get_modem_model(&sim_id).await.ok().flatten(),
                sms_center: sms_center_raw.as_ref().map(|s| decode_sms_center(s)),
                sim_status: modem_manager.get_sim_status(&sim_id).await.ok().flatten(),
                memory_status: memory_status_raw.as_ref().map(|s| format_memory_status(s)),
            };

            (StatusCode::OK, Json(enhanced_info)).into_response()
        }
        _ => (StatusCode::NOT_FOUND, "SIM not found").into_response(),
    }
}

#[derive(serde::Deserialize)]
pub struct UpdateAliasRequest {
    alias: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct UpdatePhoneRequest {
    phone_number: Option<String>,
}

async fn update_sim_alias(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
    Json(request): Json<UpdateAliasRequest>,
) -> Response {
    match SimCard::query_all().await {
        Ok(sim_cards) => {
            if let Some(mut sim_card) = sim_cards.into_iter().find(|s| s.id == sim_id) {
                match sim_card.update_alias(request.alias.clone()).await {
                    Ok(_) => {
                        // Update cache
                        modem_manager.update_sim_cache(sim_card.clone()).await;
                        (StatusCode::OK, Json(sim_card)).into_response()
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to update alias: {}", e),
                    )
                        .into_response(),
                }
            } else {
                (StatusCode::NOT_FOUND, "SIM card not found").into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to query SIM cards: {}", e),
        )
            .into_response(),
    }
}

async fn update_sim_phone(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
    Json(request): Json<UpdatePhoneRequest>,
) -> Response {
    match SimCard::query_all().await {
        Ok(sim_cards) => {
            if let Some(mut sim_card) = sim_cards.into_iter().find(|s| s.id == sim_id) {
                match sim_card
                    .update_phone_number(request.phone_number.clone())
                    .await
                {
                    Ok(_) => {
                        // Update cache
                        modem_manager.update_sim_cache(sim_card.clone()).await;
                        (StatusCode::OK, Json(sim_card)).into_response()
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to update phone number: {}", e),
                    )
                        .into_response(),
                }
            } else {
                (StatusCode::NOT_FOUND, "SIM card not found").into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to query SIM cards: {}", e),
        )
            .into_response(),
    }
}

// DELETE: refresh_alias_mapping_api - 不再需要，因为直接使用SIM ID作为键
// async fn refresh_alias_mapping_api() {...}

#[derive(Deserialize)]
struct SmsStorageRequest {
    storage: SmsStorage,
}

async fn get_sms_storage_status(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
) -> Response {
    match modem_manager.get_sms_storage_status(&sim_id).await {
        Ok(Some(status)) => (StatusCode::OK, Json(json!({"status": status}))).into_response(),
        Ok(None) => (StatusCode::OK, Json(json!({"status": "Unknown"}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to get SMS storage status: {}", e)})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct SetPhoneRequest {
    phone_number: String,
}

async fn set_sim_phone_number(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
    Json(request): Json<SetPhoneRequest>,
) -> Response {
    let phone_number = request.phone_number.trim();
    if phone_number.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Phone number is required"})),
        )
            .into_response();
    }

    // Basic sanity check: allow digits, plus, and common separators.
    let normalized: String = phone_number
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '+')
        .collect();
    if normalized.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid phone number"})),
        )
            .into_response();
    }

    // 1. Write the number into the SIM card via AT commands.
    if let Err(e) = modem_manager.set_sim_phone_number(&sim_id, &normalized).await {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": format!("Failed to write phone number to SIM: {}", e)})),
        )
            .into_response();
    }

    // 2. Persist the number in the database and refresh the in-memory cache.
    match SimCard::query_all().await {
        Ok(sim_cards) => {
            if let Some(mut sim_card) = sim_cards.into_iter().find(|s| s.id == sim_id) {
                match sim_card.update_phone_number(Some(normalized.to_string())).await {
                    Ok(_) => {
                        modem_manager.update_sim_cache(sim_card.clone()).await;
                        (StatusCode::OK, Json(sim_card)).into_response()
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": format!("Failed to update phone number: {}", e)})),
                    )
                        .into_response(),
                }
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "SIM card not found"})),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to query SIM cards: {}", e)})),
        )
            .into_response(),
    }
}

// ─── 火狐狸 platform integration handlers ─────────────────────────────────────

async fn get_firefox_api_key() -> Response {
    match AppSetting::get("firefox_api_key").await {
        Ok(value) => (StatusCode::OK, Json(json!({ "api_key": value }))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to read API key: {}", e)})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct SetFirefoxApiKeyRequest {
    api_key: String,
}

async fn set_firefox_api_key(Json(request): Json<SetFirefoxApiKeyRequest>) -> Response {
    let api_key = request.api_key.trim();
    if api_key.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "API key is required"})),
        )
            .into_response();
    }

    match AppSetting::set("firefox_api_key", Some(api_key)).await {
        Ok(()) => (StatusCode::OK, Json(json!({"message": "API key saved"}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to save API key: {}", e)})),
        )
            .into_response(),
    }
}

async fn get_firefox_countries() -> Response {
    (StatusCode::OK, Json(json!(firefox_api::countries()))).into_response()
}

#[derive(Deserialize)]
struct FirefoxUploadRequest {
    sim_ids: Vec<String>,
    country_id: String,
}

async fn firefox_upload(
    State(modem_manager): State<ModemManagerRef>,
    Json(request): Json<FirefoxUploadRequest>,
) -> Response {
    if request.sim_ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No SIM cards selected"})),
        )
            .into_response();
    }

    let country_id = request.country_id.trim();
    if country_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Country code is required"})),
        )
            .into_response();
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    // Resolve phone numbers for the selected SIMs.
    let sim_cards = match SimCard::query_all().await {
        Ok(cards) => cards,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to query SIM cards: {}", e)})),
            )
                .into_response();
        }
    };

    let mut phone_numbers = Vec::new();
    let mut sim_ids_to_update = Vec::new();
    for sim_id in &request.sim_ids {
        if let Some(card) = sim_cards.iter().find(|c| &c.id == sim_id) {
            if let Some(phone) = card.phone_number.as_deref().filter(|p| !p.is_empty()) {
                // Normalize: keep only digits and plus sign.
                let normalized: String = phone.chars().filter(|c| c.is_ascii_digit() || *c == '+').collect();
                if !normalized.is_empty() {
                    phone_numbers.push(normalized);
                    sim_ids_to_update.push(sim_id.clone());
                    continue;
                }
            }
        }
    }

    if phone_numbers.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "None of the selected SIM cards have a valid phone number"})),
        )
            .into_response();
    }

    // Persist the selected country code on each uploaded SIM.
    for sim_id in &sim_ids_to_update {
        if let Some(mut card) = sim_cards.iter().find(|c| &c.id == sim_id).cloned() {
            if let Err(e) = card.update_country_code(Some(country_id.to_string())).await {
                log::warn!("Failed to update country_code for {}: {}", sim_id, e);
            }
            modem_manager.update_sim_cache(card).await;
        }
    }

    match firefox_api::upload_phone_batch(&client, &api_key, country_id, &phone_numbers).await {
        Ok(results) => {
            let batch_ids: Vec<String> = results
                .iter()
                .map(|r| r.batch_id.clone())
                .collect();
            let api_responses: Vec<firefox_api::ApiResponse> = results
                .iter()
                .map(|r| r.response.clone())
                .collect();

            // Persist each batch upload record with its own phone numbers
            for result in &results {
                if let Err(e) = crate::db::FirefoxBatchUpload::insert(
                    &result.batch_id,
                    country_id,
                    &result.phone_numbers,
                )
                .await
                {
                    log::warn!(
                        "Failed to persist firefox batch upload record (batch_id={}): {}",
                        result.batch_id,
                        e
                    );
                }
            }

            (
                StatusCode::OK,
                Json(json!({
                    "message": "Upload completed",
                    "uploaded_count": phone_numbers.len(),
                    "batch_ids": batch_ids,
                    "results": api_responses,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": format!("Upload failed: {}", e)})),
        )
            .into_response(),
    }
}

// ─── Helper: read api key + build http client ────────────────────────────

async fn get_firefox_client() -> Result<(String, reqwest::Client), Response> {
    let api_key = AppSetting::get("firefox_api_key")
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to read API key: {}", e)}))).into_response()
        })?
        .ok_or_else(|| {
            (StatusCode::BAD_REQUEST, Json(json!({"error": "Firefox API key not configured"}))).into_response()
        })?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to build HTTP client: {}", e)}))).into_response()
        })?;

    Ok((api_key, client))
}

// ─── 5. PhoneBatchResult ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct FirefoxBatchStatusRequest {
    batch_id: String,
}

async fn firefox_batch_status(Json(request): Json<FirefoxBatchStatusRequest>) -> Response {
    let batch_id = request.batch_id.trim().to_string();
    if batch_id.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "Batch ID is required"}))).into_response();
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    match firefox_api::query_batch_status(&client, &api_key, &batch_id).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Query failed: {}", e)}))).into_response(),
    }
}

// ─── 5b. Firefox Batch Uploads History ──────────────────────────────────

async fn firefox_batch_uploads() -> Response {
    match crate::db::FirefoxBatchUpload::query_recent(100).await {
        Ok(uploads) => (StatusCode::OK, Json(json!(uploads))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to query batch uploads: {}", e)})),
        )
            .into_response(),
    }
}

// ─── 6. PhoneDeleteBatch ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct FirefoxDeleteBatchRequest {
    entries: Vec<DeleteBatchEntry>,
}

#[derive(Deserialize)]
struct DeleteBatchEntry {
    country_id: String,
    phone_num: String,
}

async fn firefox_delete_batch(Json(request): Json<FirefoxDeleteBatchRequest>) -> Response {
    if request.entries.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "No entries provided"}))).into_response();
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    let entries: Vec<(&str, &str)> = request.entries.iter().map(|e| (e.country_id.as_str(), e.phone_num.as_str())).collect();

    // Clear local country_code for deleted phone numbers
    if let Ok(cards) = SimCard::query_all().await {
        for entry in &request.entries {
            for card in &cards {
                if card.phone_number.as_deref() == Some(&entry.phone_num) {
                    let mut c = card.clone();
                    let _ = c.update_country_code(None).await;
                }
            }
        }
    }

    match firefox_api::delete_phone_batch(&client, &api_key, &entries).await {
        Ok(results) => (StatusCode::OK, Json(json!({"message": "Delete completed", "results": results}))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Delete failed: {}", e)}))).into_response(),
    }
}

// ─── 6b. PhoneDeleteBatchById ────────────────────────────────────────────

#[derive(Deserialize)]
struct FirefoxDeleteBatchByIdRequest {
    batch_id: String,
}

async fn firefox_delete_batch_by_id(Json(request): Json<FirefoxDeleteBatchByIdRequest>) -> Response {
    let batch_id = request.batch_id.trim().to_string();
    if batch_id.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "Batch ID is required"}))).into_response();
    }

    let upload = match crate::db::FirefoxBatchUpload::query_by_batch_id(&batch_id).await {
        Ok(Some(u)) => u,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, Json(json!({"error": "Batch not found"}))).into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to query batch: {}", e)}))).into_response();
        }
    };

    let phone_numbers: Vec<String> = upload
        .phone_numbers
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if phone_numbers.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "No phone numbers in batch"}))).into_response();
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    let entries: Vec<(&str, &str)> = phone_numbers
        .iter()
        .map(|p| (upload.country_id.as_str(), p.as_str()))
        .collect();

    // Clear local country_code for deleted phone numbers
    if let Ok(cards) = SimCard::query_all().await {
        for phone_num in &phone_numbers {
            for card in &cards {
                if card.phone_number.as_deref() == Some(phone_num) {
                    let mut c = card.clone();
                    let _ = c.update_country_code(None).await;
                }
            }
        }
    }

    match firefox_api::delete_phone_batch(&client, &api_key, &entries).await {
        Ok(results) => {
            // Optionally delete the local batch record after successful deletion
            let _ = crate::db::FirefoxBatchUpload::delete_by_id(upload.id).await;
            (StatusCode::OK, Json(json!({"message": "Delete by batch completed", "phone_numbers": phone_numbers, "results": results}))).into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Delete failed: {}", e)}))).into_response(),
    }
}

// ─── 7. PhoneDeleteCountry ───────────────────────────────────────────────

#[derive(Deserialize)]
struct FirefoxDeleteCountryRequest {
    country_id: String,
}

async fn firefox_delete_country(Json(request): Json<FirefoxDeleteCountryRequest>) -> Response {
    let country_id = request.country_id.trim().to_string();
    if country_id.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "Country ID is required"}))).into_response();
    }

    // Clear local country_code for all SIMs in this country
    if let Ok(cards) = SimCard::query_all().await {
        for card in &cards {
            if card.country_code.as_deref() == Some(&country_id) {
                let mut c = card.clone();
                let _ = c.update_country_code(None).await;
            }
        }
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    match firefox_api::delete_phone_country(&client, &api_key, &country_id).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Delete failed: {}", e)}))).into_response(),
    }
}

// ─── 8. PhoneDeleteAll ───────────────────────────────────────────────────

async fn firefox_delete_all() -> Response {
    // Clear local country_code for all SIMs
    if let Ok(cards) = SimCard::query_all().await {
        for card in &cards {
            if card.country_code.is_some() {
                let mut c = card.clone();
                let _ = c.update_country_code(None).await;
            }
        }
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    match firefox_api::delete_phone_all(&client, &api_key).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Delete failed: {}", e)}))).into_response(),
    }
}

// ─── 9. GetWaitPhoneList ─────────────────────────────────────────────────

async fn firefox_wait_list() -> Response {
    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    match firefox_api::get_wait_phone_list(&client, &api_key).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Query failed: {}", e)}))).into_response(),
    }
}

// ─── 10. GetResultPhoneList ──────────────────────────────────────────────

#[derive(Deserialize)]
struct FirefoxResultListRequest {
    country_id: String,
    phone_num: String,
    item_id: String,
}

async fn firefox_result_list(Query(request): Query<FirefoxResultListRequest>) -> Response {
    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    match firefox_api::get_result_phone_list(&client, &api_key, &request.country_id, &request.phone_num, &request.item_id).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Query failed: {}", e)}))).into_response(),
    }
}

// ─── 11. UploadSms ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct FirefoxUploadSmsRequest {
    country_id: String,
    phone_num: String,
    sms_content: String,
}

async fn firefox_upload_sms(Json(request): Json<FirefoxUploadSmsRequest>) -> Response {
    let country_id = request.country_id.trim().to_string();
    let phone_num = request.phone_num.trim().to_string();
    let sms_content = request.sms_content.trim().to_string();

    if country_id.is_empty() || phone_num.is_empty() || sms_content.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "country_id, phone_num, and sms_content are required"}))).into_response();
    }

    let (api_key, client) = match get_firefox_client().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    match firefox_api::upload_sms(&client, &api_key, &country_id, &phone_num, &sms_content).await {
        Ok(result) => (StatusCode::OK, Json(json!(result))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("Upload SMS failed: {}", e)}))).into_response(),
    }
}

// ─── 12. Platform Items & Statistics ─────────────────────────────────────

async fn firefox_platform_items() -> Response {
    match crate::db::FirefoxPlatformItem::query_all().await {
        Ok(items) => (StatusCode::OK, Json(json!(items))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to query platform items: {}", e)})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct FirefoxPlatformItemDetailQuery {
    sim_id: Option<String>,
}

async fn firefox_platform_item_detail(
    Path(item_id): Path<String>,
    Query(query): Query<FirefoxPlatformItemDetailQuery>,
) -> Response {
    let item_id = item_id.trim().to_string();
    if item_id.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "Item ID is required"}))).into_response();
    }

    match crate::db::FirefoxPlatformItem::query_by_item_id(&item_id).await {
        Ok(items) => match crate::db::Sms::query_by_platform_item_and_sim(&item_id, query.sim_id.as_deref()).await {
            Ok(sms_list) => {
                let items = if let Some(sim_id) = query.sim_id.as_deref() {
                    items.into_iter()
                        .filter(|item| item.iccid.as_deref() == Some(sim_id) || item.sim_id.as_deref() == Some(sim_id))
                        .collect::<Vec<_>>()
                } else {
                    items
                };

                if items.is_empty() && sms_list.is_empty() {
                    return (StatusCode::NOT_FOUND, Json(json!({"error": "Item not found"}))).into_response();
                }

                (StatusCode::OK, Json(json!({
                    "item_id": item_id,
                    "sim_id": query.sim_id,
                    "items": items,
                    "sms_list": sms_list,
                }))).into_response()
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to query SMS: {}", e)}))).into_response(),
        },
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to query item: {}", e)}))).into_response(),
    }
}

async fn firefox_platform_statistics() -> Response {
    match crate::db::FirefoxPlatformItem::query_statistics().await {
        Ok(stats) => (StatusCode::OK, Json(json!(stats))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to query statistics: {}", e)})),
        )
            .into_response(),
    }
}

async fn firefox_platform_rejection_reasons() -> Response {
    match crate::db::Sms::query_platform_rejection_reason_summary(8).await {
        Ok(stats) => (StatusCode::OK, Json(json!(stats))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to query rejection reason summary: {}", e)})),
        )
            .into_response(),
    }
}

async fn set_sms_storage(
    Path(sim_id): Path<String>,
    State(modem_manager): State<ModemManagerRef>,
    Json(request): Json<SmsStorageRequest>,
) -> Response {
    match modem_manager
        .set_sms_storage(&sim_id, request.storage)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"message": "SMS storage location updated successfully"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to set SMS storage: {}", e)})),
        )
            .into_response(),
    }
}

// ─── Firefox Upload Retry Queue Handlers ─────────────────────────────────

async fn firefox_upload_retry_stats() -> Response {
    match crate::firefox_upload_retry::FirefoxUploadRetryItem::get_stats(&crate::db::get_pool().unwrap_or_else(|_| panic!("No DB pool"))).await {
        Ok(stats) => (StatusCode::OK, Json(json!(stats))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to get queue statistics: {}", e)}))).into_response(),
    }
}

async fn firefox_upload_retry_queue() -> Response {
    match crate::firefox_upload_retry::FirefoxUploadRetryItem::get_ready_for_retry(&crate::db::get_pool().unwrap_or_else(|_| panic!("No DB pool")), 100).await {
        Ok(items) => (StatusCode::OK, Json(json!({"items": items, "count": items.len()}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to get queue items: {}", e)}))).into_response(),
    }
}

async fn firefox_upload_retry_dead_letter() -> Response {
    match crate::firefox_upload_retry::FirefoxUploadRetryItem::get_dead_letter_items(&crate::db::get_pool().unwrap_or_else(|_| panic!("No DB pool")), 100).await {
        Ok(items) => (StatusCode::OK, Json(json!({"items": items, "count": items.len()}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to get dead-letter items: {}", e)}))).into_response(),
    }
}

async fn firefox_upload_retry_manual(Path(id): Path<String>) -> Response {
    let pool = match crate::db::get_pool() {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Database error: {}", e)}))).into_response(),
    };
    
    // Update the item to retry immediately
    let now = chrono::Utc::now().naive_utc();
    match sqlx::query(
        "UPDATE firefox_upload_retry_queue SET next_retry_at = ? WHERE id = ?"
    )
    .bind(now)
    .bind(&id)
    .execute(pool)
    .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "Item scheduled for immediate retry"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to update item: {}", e)}))).into_response(),
    }
}

async fn firefox_upload_retry_delete(Path(id): Path<String>) -> Response {
    let pool = match crate::db::get_pool() {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Database error: {}", e)}))).into_response(),
    };
    
    match sqlx::query("DELETE FROM firefox_upload_retry_queue WHERE id = ?")
        .bind(&id)
        .execute(pool)
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({"message": "Item deleted from retry queue"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Failed to delete item: {}", e)}))).into_response(),
    }
}
