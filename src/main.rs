#![cfg_attr(all(target_os = "windows", not(debug_assertions)), windows_subsystem = "windows")]

use std::{path::PathBuf, sync::Arc};
use chrono::{Duration as ChronoDuration, NaiveDateTime, Utc};

use api::SseManager;
use db::db_init;
use flexi_logger::{
    colored_detailed_format, Age, Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming,
};
use log::LevelFilter;
use modem::{ModemManager, SmsType};
use modem::manager::TranscribeConfig;
use structopt::StructOpt;

mod api;
mod config;
mod db;
mod decode;
mod firefox_api;
mod firefox_upload_retry;
mod firefox_upload_retry_worker;
mod mms_wap;
mod mms_retrieve;
mod mms_worker;
mod modem;
mod phone_number;
mod service_control;
mod sound_player;
mod transcribe;
#[cfg(test)]
mod tests;
mod update;
mod webhook;

pub type ModemManagerRef = Arc<ModemManager>;

#[tokio::main]
async fn main() {
    let param = Param::from_args();
    if let Some(command) = param.command {
        match command {
            Command::Update => {
                if let Err(err) = update::run_update().await {
                    eprintln!("Update failed: {}", err);
                    std::process::exit(1);
                }
            }
            Command::Version => {
                println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            }
        }
        return;
    }

    #[cfg(target_os = "windows")]
    {
        if param.no_tray {
            if let Err(err) = run_gateway(&param).await {
                eprintln!("Error: {}", err);
                std::process::exit(1);
            }
            return;
        }

        // Start backend first; only show tray/browser after it survives startup.
        let run_param = param.clone();
        let mut gateway = tokio::spawn(async move { run_gateway(&run_param).await });

        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut gateway).await {
            Ok(joined) => {
                // Backend exited quickly: treat as startup failure and show clear error.
                let err_text = match joined {
                    Ok(Ok(())) => "SMS Gateway exited unexpectedly.".to_string(),
                    Ok(Err(err)) => format_startup_error_message(&err.to_string()),
                    Err(join_err) => format!("SMS Gateway startup task failed: {}", join_err),
                };
                show_windows_error_dialog(&err_text);
                eprintln!("Error: {}", err_text);
                std::process::exit(1);
            }
            Err(_) => {
                // Backend is running; now start tray and open browser.
                maybe_start_windows_tray();

                // Keep process alive until backend exits.
                match gateway.await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        let err_text = format_startup_error_message(&err.to_string());
                        show_windows_error_dialog(&err_text);
                        eprintln!("Error: {}", err_text);
                    }
                    Err(join_err) => {
                        let err_text = format!("SMS Gateway runtime task failed: {}", join_err);
                        show_windows_error_dialog(&err_text);
                        eprintln!("Error: {}", err_text);
                    }
                }
                std::process::exit(1);
            }
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        if let Err(err) = run_gateway(&param).await {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    }
}

async fn run_gateway(param: &Param) -> anyhow::Result<()> {
    if let Err(err) = log_init(&param.log_path, &param.log_level) {
        return Err(err);
    };
    if let Err(err) = db_init().await {
        return Err(err.into());
    }
    #[cfg(debug_assertions)]
    let config = match config::AppConfig::load(&PathBuf::from("./config.toml")) {
        Ok(config) => config,
        Err(err) => {
            return Err(err);
        }
    };
    #[cfg(not(debug_assertions))]
    let config = match config::AppConfig::load(&param.config_file) {
        Ok(config) => config,
        Err(err) => {
            return Err(err);
        }
    };

    let modem_manager = Arc::new(ModemManager::new(&config));

    let sse_manager = Arc::new(api::SseManager::new());

    let transcribe_cfg = TranscribeConfig::from_settings(&config.settings);

    let webhook_manager = match config.settings.webhooks.clone() {
        Some(cfgs) => Some(webhook::start_webhook_worker_with_concurrency(
            cfgs,
            config.settings.webhooks_max_concurrent.unwrap_or(1),
        )),
        _ => None,
    };

    // Start modem initialization in the background so the HTTP API can become
    // available immediately, even when many COM ports are slow/disconnected.
    {
        let mm = modem_manager.clone();
        let sse = sse_manager.clone();
        let webhook = webhook_manager.clone();
        let tc = transcribe_cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = mm.initialize_modems(sse, webhook, tc).await {
                log::error!("Failed to initialize modems: {}", e);
            }
        });
    }

    tokio::spawn(read_sms_worker(
        modem_manager.clone(),
        config.settings.read_sms_frequency,
        sse_manager.clone(),
        webhook_manager.clone(),
    ));

    // Build a map of com_port -> sms_storage for the recheck worker
    let sms_storage_map: std::collections::HashMap<String, Option<crate::config::SmsStorage>> =
        config
            .devices
            .iter()
            .map(|d| (d.com_port.clone(), d.sms_storage.or(config.settings.sms_storage)))
            .collect();

    tokio::spawn(recheck_fallback_worker(
        modem_manager.clone(),
        sms_storage_map,
        sse_manager.clone(),
        webhook_manager.clone(),
        transcribe_cfg,
    ));

    tokio::spawn(firefox_poll_worker(modem_manager.clone()));

    tokio::spawn(firefox_item_catalog_sync_worker());

    firefox_upload_retry_worker::start_retry_worker();

    mms_worker::start_mms_worker(
        modem_manager.clone(),
        config.settings.mms_send_timeout_secs.unwrap_or(60),
    );

    api::run_api(
        modem_manager.clone(),
        &config.settings.server_host,
        &config.settings.server_port,
        &config.settings.username.unwrap(),
        &config.settings.password.unwrap(),
        sse_manager.clone(),
        None,
        None,
    )
    .await?;

    Ok(())
}

async fn firefox_item_catalog_sync_worker() {
    let check_interval = tokio::time::Duration::from_secs(6 * 60 * 60);

    loop {
        if !crate::service_control::is_running() {
            tokio::time::sleep(check_interval).await;
            continue;
        }

        if let Err(e) = sync_firefox_item_catalog_if_due().await {
            log::warn!("[火狐狸价目] 定时同步失败: {}", e);
        }

        tokio::time::sleep(check_interval).await;
    }
}

async fn sync_firefox_item_catalog_if_due() -> anyhow::Result<()> {
    let now = Utc::now().naive_utc();
    let last_sync = db::AppSetting::get("firefox_item_catalog_last_sync_at")
        .await?
        .and_then(|v| NaiveDateTime::parse_from_str(v.trim(), "%Y-%m-%d %H:%M:%S").ok());

    if let Some(last) = last_sync {
        if now - last < ChronoDuration::days(3) {
            return Ok(());
        }
    }

    let token = db::AppSetting::get("firefox_user_token").await.ok().flatten();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let items = firefox_api::get_user_item_prices(&client, token.as_deref(), None).await?;
    if items.is_empty() {
        log::warn!("[火狐狸价目] 同步返回空列表，保留本地数据");
        return Ok(());
    }

    let upserts: Vec<db::FirefoxItemPriceUpsert> = items
        .into_iter()
        .map(|item| {
            let price = item.item_uprice.trim().parse::<f64>().unwrap_or(0.0);
            db::FirefoxItemPriceUpsert {
                item_id: item.item_id.trim().to_string(),
                country_id: item.country_id.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
                item_name: item.item_name.trim().to_string(),
                item_uprice: if price.is_finite() { price.max(0.0) } else { 0.0 },
                country_title: item.country_title.map(|v| v.trim().to_string()).filter(|v| !v.is_empty()),
            }
        })
        .filter(|item| !item.item_id.is_empty() && !item.item_name.is_empty())
        .collect();

    if upserts.is_empty() {
        log::warn!("[火狐狸价目] 同步后没有有效项目，跳过更新");
        return Ok(());
    }

    db::upsert_firefox_item_prices(&upserts).await?;

    let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
    db::AppSetting::set("firefox_item_catalog_last_sync_at", Some(&timestamp)).await?;

    log::info!("[火狐狸价目] 同步完成: {} 项", upserts.len());
    Ok(())
}

#[cfg(target_os = "windows")]
fn maybe_start_windows_tray() {
    let url = "http://localhost:8080".to_string();
    let tray_url = url.clone();
    std::thread::spawn(move || {
        if let Err(err) = run_windows_tray(tray_url) {
            show_windows_error_dialog(&format!("Failed to start system tray: {}", err));
        }
    });

    open_browser_with_fallback(&url);
}

#[cfg(target_os = "windows")]
fn format_startup_error_message(err: &str) -> String {
    if err.contains("10048") || err.contains("Only one usage") || err.contains("通常每个套接字地址") {
        return "小牛智卡 is already running (port is in use). Please check the tray icons, or stop the existing instance first.".to_string();
    }
    format!("小牛智卡 failed to start: {}", err)
}

#[cfg(target_os = "windows")]
fn show_windows_error_dialog(message: &str) {
    let _ = native_dialog::MessageDialog::new()
        .set_type(native_dialog::MessageType::Error)
        .set_title("小牛智卡")
        .set_text(message)
        .show_alert();
}

#[cfg(target_os = "windows")]
fn open_browser_with_fallback(url: &str) {
    if webbrowser::open(url).is_ok() {
        return;
    }

    let _ = std::process::Command::new("cmd")
        .args(["/C", "start", "", url])
        .spawn();
}

#[cfg(target_os = "windows")]
fn run_windows_tray(url: String) -> anyhow::Result<()> {
    let mut tray = create_windows_tray_item()?;

    let open_url = url.clone();
    tray.add_menu_item("Open Panel", move || {
        open_browser_with_fallback(&open_url);
    })?;

    tray.add_menu_item("Exit", || {
        std::process::exit(0);
    })?;

    loop {
        std::thread::park();
    }
}

#[cfg(target_os = "windows")]
fn create_windows_tray_item() -> anyhow::Result<tray_item::TrayItem> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        LoadIconW, LoadImageW, HICON, IDI_APPLICATION, IMAGE_ICON, LR_LOADFROMFILE,
    };

    fn try_load_icon_from_candidates() -> Option<HICON> {
        let exe = std::env::current_exe().ok()?;
        let exe_dir = exe.parent()?;
        let candidate_paths = [
            exe_dir.join("icon").join("tray.ico"),
            exe_dir.join("icon").join("xiaoniu-zhika.ico"),
            exe_dir.join("xiaoniu-zhika.ico"),
            exe_dir.join("sms-gateway.ico"),
        ];

        for icon_path in candidate_paths {
            if !icon_path.exists() {
                continue;
            }

            let wide: Vec<u16> = icon_path
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();

            let h = unsafe { LoadImageW(0, wide.as_ptr(), IMAGE_ICON, 64, 64, LR_LOADFROMFILE) };
            if h != 0 {
                return Some(h as HICON);
            }
        }

        None
    }

    let hicon = try_load_icon_from_candidates().unwrap_or_else(|| unsafe {
        // Guaranteed Windows fallback so tray creation never fails on icon loading.
        LoadIconW(0, IDI_APPLICATION as *const u16)
    });

    if hicon == 0 {
        anyhow::bail!("unable to load tray icon handle")
    }

    tray_item::TrayItem::new("小牛智卡", tray_item::IconSource::RawIcon(hicon))
        .map_err(|e| anyhow::anyhow!(e.to_string()))
}

async fn read_sms_worker(
    modem_manager: ModemManagerRef,
    read_sms_frequency: u64,
    sse_manager: Arc<SseManager>,
    webhook_manager: Option<webhook::WebhookManager>,
) {
    loop {
        if !crate::service_control::is_running() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        modem_manager
            .read_all_sms_async(
                SmsType::RecUnread,
                sse_manager.clone(),
                webhook_manager.clone(),
            )
            .await;

        tokio::time::sleep(tokio::time::Duration::from_secs(read_sms_frequency)).await;
    }
}

async fn recheck_fallback_worker(
    modem_manager: ModemManagerRef,
    sms_storage_map: std::collections::HashMap<String, Option<crate::config::SmsStorage>>,
    sse_manager: Arc<SseManager>,
    webhook_manager: Option<webhook::WebhookManager>,
    transcribe_cfg: Option<Arc<modem::manager::TranscribeConfig>>,
) {
    loop {
        if !crate::service_control::is_running() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;
        modem_manager
            .clone()
            .recheck_fallback_modems(
                &sms_storage_map,
                sse_manager.clone(),
                webhook_manager.clone(),
                transcribe_cfg.clone(),
            )
            .await;
    }
}

async fn firefox_poll_worker(modem_manager: ModemManagerRef) {
    use std::collections::{HashMap, HashSet};
    let poll_interval = tokio::time::Duration::from_secs(3);
    let mut processed_tasks: HashSet<String> = HashSet::new();
    let mut no_sms_backoff_until: HashMap<String, tokio::time::Instant> = HashMap::new();
    let mut no_sms_miss_count: HashMap<String, u8> = HashMap::new();
    let mut heartbeat_count: u32 = 0;
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("[火狐狸轮询] 创建 HTTP 客户端失败: {}", e);
            return;
        }
    };

    loop {
        tokio::time::sleep(poll_interval).await;
        heartbeat_count += 1;

        if !crate::service_control::is_running() {
            continue;
        }

        let api_key = match db::AppSetting::get("firefox_api_key").await {
            Ok(Some(key)) => key,
            _ => continue,
        };

        let wait_list_resp = {
            let mut last_error: Option<anyhow::Error> = None;
            let mut response = None;
            for attempt in 1..=3 {
                match firefox_api::get_wait_phone_list(&client, &api_key).await {
                    Ok(resp) => {
                        response = Some(resp);
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < 3 {
                            tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;
                        }
                    }
                }
            }
            match response {
                Some(resp) => resp,
                None => {
                    log::warn!(
                        "[火狐狸轮询] 获取等待列表失败（已重试3次）: {}",
                        last_error
                            .map(|e| e.to_string())
                            .unwrap_or_else(|| "unknown error".to_string())
                    );
                    continue;
                }
            }
        };

        {
            let resp = wait_list_resp;
                let count = resp.data.as_ref().and_then(|d| d.as_array()).map(|a| a.len()).unwrap_or(0);
                if count > 0 {
                    if let Some(items) = resp.data.as_ref().and_then(|d| d.as_array()) {
                        for item in items {
                            let phone_num = item.get("Phone_Num").and_then(|v| v.as_str()).unwrap_or("?");
                            let country_id = item.get("Country_ID").and_then(|v| v.as_str()).unwrap_or("?");
                            let item_id = item.get("Item_ID").and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))).map(|v| v.to_string()).unwrap_or_else(|| "?".to_string());

                            if phone_num == "?" || item_id == "?" || country_id == "?" {
                                continue;
                            }

                            if let Err(e) = db::FirefoxPlatformItem::upsert(&item_id, country_id, phone_num).await {
                                log::error!(
                                    "[火狐狸轮询] 刷新平台项目映射失败 - 号码: {}, Item_ID: {}, 错误: {}",
                                    phone_num,
                                    item_id,
                                    e
                                );
                            }

                            let task_key = format!("{}|{}|{}", country_id, phone_num, item_id);
                            let now = tokio::time::Instant::now();

                            if let Some(until) = no_sms_backoff_until.get(&task_key) {
                                if *until > now {
                                    continue;
                                }
                                no_sms_backoff_until.remove(&task_key);
                            }

                            // Skip if we already processed this item
                            if processed_tasks.contains(&task_key) {
                                // Still check result list for release status
                                if let Ok(result_resp) = firefox_api::get_result_phone_list(&client, &api_key, country_id, phone_num, &item_id).await {
                                    if let Some(data) = result_resp.data {
                                        if let Some(arr) = data.as_array() {
                                            for result_item in arr {
                                                let is_ret = result_item.get("Phone_IsRet").and_then(|v| v.as_str()).unwrap_or("false");
                                                let remark = result_item.get("Phone_Remark").and_then(|v| v.as_str()).unwrap_or("");
                                                if is_ret == "True" || is_ret == "true" || !remark.is_empty() {
                                                    processed_tasks.remove(&task_key);
                                                    no_sms_backoff_until.remove(&task_key);
                                                    no_sms_miss_count.remove(&task_key);
                                                    log::info!("[火狐狸轮询] 任务已释放 - 号码: {}, Item_ID: {}, 备注: {}", phone_num, item_id, remark);
                                                }
                                            }
                                        }
                                    }
                                }
                                continue;
                            }

                            log::info!("[火狐狸轮询] 等待短信 - 号码: {}, Country: {}, Item_ID: {}", phone_num, country_id, item_id);

                            // Find the SIM that owns this phone number
                            let sim_id = modem_manager.find_sim_id_by_phone_number(phone_num).await;
                            let sim_id = match sim_id {
                                Some(id) => id,
                                None => {
                                    log::warn!("[火狐狸轮询] 未找到号码 {} 对应的 SIM 卡", phone_num);
                                    processed_tasks.insert(task_key.clone());
                                    continue;
                                }
                            };

                            if let Err(e) = db::Sms::normalize_failed_item_mapping_for_phone(&sim_id, phone_num, &item_id).await {
                                log::error!(
                                    "[火狐狸轮询] 修正失败短信平台映射失败 - 号码: {}, SIM: {}, Item_ID: {}, 错误: {}",
                                    phone_num,
                                    sim_id,
                                    item_id,
                                    e
                                );
                            }

                            log::info!("[火狐狸轮询] 找到 SIM: {}, 强制读取短信", sim_id);

                            // Read SMS from modem, sync to DB, and get the latest incoming message
                            let mut mark_task_processed = true;
                            match modem_manager.read_sms_and_get_latest(&sim_id).await {
                                Ok(Some(sms)) => {
                                    let upload_content = db::build_upload_sms_content(&item_id, &sms.message)
                                        .await
                                        .unwrap_or_else(|e| {
                                            log::error!(
                                                "[火狐狸轮询] 构建上传短信内容失败 (SMS ID: {}, Item_ID: {}): {}，使用原始短信内容",
                                                sms.id,
                                                item_id,
                                                e
                                            );
                                            sms.message.clone()
                                        });
                                    log::info!("[火狐狸轮询] 获取到短信内容 ({}): {}, 上传中 - 号码: {}", sms.contact_id, sms.message, phone_num);
                                    match firefox_api::upload_sms(&client, &api_key, country_id, phone_num, &upload_content).await {
                                        Ok(upload_resp) if upload_resp.code == "1" => {
                                            let response_json = serde_json::to_string(&upload_resp).ok();
                                            if let Err(e) = db::Sms::mark_platform_attempt(
                                                sms.id,
                                                &item_id,
                                                true,
                                                response_json.as_deref(),
                                            )
                                            .await
                                            {
                                                log::error!(
                                                    "[火狐狸轮询] 标记短信上传成功失败 (SMS ID: {}, Item_ID: {}): {}",
                                                    sms.id,
                                                    item_id,
                                                    e
                                                );
                                            }
                                            log::info!("[火狐狸轮询] 短信上传成功 - 号码: {}, Item_ID: {}, 响应: {:?}", phone_num, item_id, upload_resp)
                                        }
                                        Ok(upload_resp) => {
                                            let response_json = serde_json::to_string(&upload_resp).ok();
                                            if let Err(e) = db::Sms::mark_platform_attempt(
                                                sms.id,
                                                &item_id,
                                                false,
                                                response_json.as_deref(),
                                            )
                                            .await
                                            {
                                                log::error!(
                                                    "[火狐狸轮询] 标记短信上传失败失败 (SMS ID: {}, Item_ID: {}): {}",
                                                    sms.id,
                                                    item_id,
                                                    e
                                                );
                                            }

                                            if firefox_api::is_unretryable_platform_rejection(&upload_resp) {
                                                log::warn!(
                                                    "[火狐狸轮询] 短信上传失败（不可重试），不进入重试队列 - 号码: {}, Item_ID: {}, code: {}, data: {:?}",
                                                    phone_num,
                                                    item_id,
                                                    upload_resp.code,
                                                    upload_resp.data
                                                );
                                            } else {
                                                // Enqueue retry only for retryable failures.
                                                let retry_item = firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                    sms.id,
                                                    phone_num.to_string(),
                                                    country_id.to_string(),
                                                    sms.message.clone(),
                                                    format!("Upload failed with code: {}", upload_resp.code),
                                                    Some(upload_resp.code.clone()),
                                                );

                                                match firefox_upload_retry::FirefoxUploadRetryItem::insert(
                                                    &db::get_pool().unwrap_or_else(|_| panic!("No DB pool")),
                                                    &retry_item
                                                ).await {
                                                    Ok(_) => log::info!("[火狐狸轮询] 短信上传失败，已加入重试队列 - 号码: {}, Item_ID: {}, 重试ID: {}, code: {}", phone_num, item_id, retry_item.id, upload_resp.code),
                                                    Err(e) => log::error!("[火狐狸轮询] 将失败的短信加入重试队列失败: {}", e),
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            let error_message = format!("Upload failed: {}", e);
                                            if let Err(mark_err) = db::Sms::mark_platform_attempt(
                                                sms.id,
                                                &item_id,
                                                false,
                                                Some(&error_message),
                                            )
                                            .await
                                            {
                                                log::error!(
                                                    "[火狐狸轮询] 标记短信上传错误失败 (SMS ID: {}, Item_ID: {}): {}",
                                                    sms.id,
                                                    item_id,
                                                    mark_err
                                                );
                                            }

                                            // Network error - also queue for retry
                                            let retry_item = firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                sms.id,
                                                phone_num.to_string(),
                                                country_id.to_string(),
                                                sms.message.clone(),
                                                format!("Upload failed: {}", e),
                                                None,
                                            );
                                            
                                            match firefox_upload_retry::FirefoxUploadRetryItem::insert(
                                                &db::get_pool().unwrap_or_else(|_| panic!("No DB pool")),
                                                &retry_item
                                            ).await {
                                                Ok(_) => log::info!("[火狐狸轮询] 短信上传失败，已加入重试队列 - 号码: {}, Item_ID: {}, 重试ID: {}, 错误: {}", phone_num, item_id, retry_item.id, e),
                                                Err(e) => log::error!("[火狐狸轮询] 将失败的短信加入重试队列失败: {}", e),
                                            }
                                        },
                                    }
                                }
                                Ok(None) => {
                                    mark_task_processed = false;
                                    log::warn!("[火狐狸轮询] 读取 SMS 后未找到短信 (SIM: {}, 号码: {})", sim_id, phone_num);
                                    // Check the result list in case platform already has content
                                    if let Ok(result_resp) = firefox_api::get_result_phone_list(&client, &api_key, country_id, phone_num, &item_id).await {
                                        if let Some(data) = result_resp.data {
                                            if let Some(arr) = data.as_array() {
                                                for result_item in arr {
                                                    if let Some(content) = result_item.get("Phone_SmsContent").and_then(|v| v.as_str()) {
                                                        if !content.is_empty() {
                                                            let upload_content = db::build_upload_sms_content(&item_id, content)
                                                                .await
                                                                .unwrap_or_else(|e| {
                                                                    log::error!(
                                                                        "[火狐狸轮询] 构建平台列表上传短信内容失败 (SIM: {}, Item_ID: {}): {}，使用原始短信内容",
                                                                        sim_id,
                                                                        item_id,
                                                                        e
                                                                    );
                                                                    content.to_string()
                                                                });
                                                            log::info!("[火狐狸轮询] 从平台获取到短信内容, 上传中 - 号码: {}, Item_ID: {}", phone_num, item_id);
                                                            match firefox_api::upload_sms(&client, &api_key, country_id, phone_num, &upload_content).await {
                                                                Ok(upload_resp) if upload_resp.code == "1" => {
                                                                    let response_json = serde_json::to_string(&upload_resp).ok();
                                                                    if let Err(e) = db::Sms::mark_platform_attempt_by_phone_message(
                                                                        phone_num,
                                                                        &sim_id,
                                                                        &[content.to_string()],
                                                                        Some(&item_id),
                                                                        true,
                                                                        response_json,
                                                                    )
                                                                    .await
                                                                    {
                                                                        log::error!(
                                                                            "[火狐狸轮询] 标记平台列表短信上传成功失败 (SIM: {}, Item_ID: {}): {}",
                                                                            sim_id,
                                                                            item_id,
                                                                            e
                                                                        );
                                                                    }
                                                                    log::info!("[火狐狸轮询] 短信上传成功 - 号码: {}, Item_ID: {}, 响应: {:?}", phone_num, item_id, upload_resp)
                                                                }
                                                                Ok(upload_resp) => {
                                                                    let response_json = serde_json::to_string(&upload_resp).ok();
                                                                    if let Err(e) = db::Sms::mark_platform_attempt_by_phone_message(
                                                                        phone_num,
                                                                        &sim_id,
                                                                        &[content.to_string()],
                                                                        Some(&item_id),
                                                                        false,
                                                                        response_json,
                                                                    )
                                                                    .await
                                                                    {
                                                                        log::error!(
                                                                            "[火狐狸轮询] 标记平台列表短信上传失败失败 (SIM: {}, Item_ID: {}): {}",
                                                                            sim_id,
                                                                            item_id,
                                                                            e
                                                                        );
                                                                    }

                                                                    if firefox_api::is_unretryable_platform_rejection(&upload_resp) {
                                                                        log::warn!(
                                                                            "[火狐狸轮询] 平台列表短信上传失败（不可重试），不进入重试队列 - 号码: {}, Item_ID: {}, code: {}, data: {:?}",
                                                                            phone_num,
                                                                            item_id,
                                                                            upload_resp.code,
                                                                            upload_resp.data
                                                                        );
                                                                    } else {
                                                                        let sms_id = match db::Sms::find_latest_incoming_id_by_sim_message(
                                                                            &sim_id,
                                                                            content,
                                                                        )
                                                                        .await
                                                                        {
                                                                            Ok(Some(id)) => id,
                                                                            Ok(None) => {
                                                                                log::error!(
                                                                                    "[火狐狸轮询] 平台列表短信重试入队失败：未找到SMS记录 (SIM: {}, Item_ID: {}, phone: {})",
                                                                                    sim_id,
                                                                                    item_id,
                                                                                    phone_num
                                                                                );
                                                                                continue;
                                                                            }
                                                                            Err(e) => {
                                                                                log::error!(
                                                                                    "[火狐狸轮询] 平台列表短信重试入队失败：查询SMS记录错误 (SIM: {}, Item_ID: {}): {}",
                                                                                    sim_id,
                                                                                    item_id,
                                                                                    e
                                                                                );
                                                                                continue;
                                                                            }
                                                                        };

                                                                        let retry_item = firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                                            sms_id,
                                                                            phone_num.to_string(),
                                                                            country_id.to_string(),
                                                                            content.to_string(),
                                                                            format!("Upload failed with code: {}", upload_resp.code),
                                                                            Some(upload_resp.code.clone()),
                                                                        );

                                                                        match firefox_upload_retry::FirefoxUploadRetryItem::insert(
                                                                            &db::get_pool().unwrap_or_else(|_| panic!("No DB pool")),
                                                                            &retry_item
                                                                        ).await {
                                                                            Ok(_) => log::info!("[火狐狸轮询] 短信上传失败（来自平台列表），已加入重试队列 - 号码: {}, Item_ID: {}, 重试ID: {}", phone_num, item_id, retry_item.id),
                                                                            Err(e) => log::error!("[火狐狸轮询] 将失败的短信加入重试队列失败: {}", e),
                                                                        }
                                                                    }
                                                                },
                                                                Err(e) => {
                                                                    let error_message = format!("Upload failed: {}", e);
                                                                    if let Err(mark_err) = db::Sms::mark_platform_attempt_by_phone_message(
                                                                        phone_num,
                                                                        &sim_id,
                                                                        &[content.to_string()],
                                                                        Some(&item_id),
                                                                        false,
                                                                        Some(error_message.clone()),
                                                                    )
                                                                    .await
                                                                    {
                                                                        log::error!(
                                                                            "[火狐狸轮询] 标记平台列表短信上传错误失败 (SIM: {}, Item_ID: {}): {}",
                                                                            sim_id,
                                                                            item_id,
                                                                            mark_err
                                                                        );
                                                                    }

                                                                    // Network error - also queue for retry
                                                                    let sms_id = match db::Sms::find_latest_incoming_id_by_sim_message(
                                                                        &sim_id,
                                                                        content,
                                                                    )
                                                                    .await
                                                                    {
                                                                        Ok(Some(id)) => id,
                                                                        Ok(None) => {
                                                                            log::error!(
                                                                                "[火狐狸轮询] 平台列表短信重试入队失败：未找到SMS记录 (SIM: {}, Item_ID: {}, phone: {})",
                                                                                sim_id,
                                                                                item_id,
                                                                                phone_num
                                                                            );
                                                                            continue;
                                                                        }
                                                                        Err(e) => {
                                                                            log::error!(
                                                                                "[火狐狸轮询] 平台列表短信重试入队失败：查询SMS记录错误 (SIM: {}, Item_ID: {}): {}",
                                                                                sim_id,
                                                                                item_id,
                                                                                e
                                                                            );
                                                                            continue;
                                                                        }
                                                                    };

                                                                    let retry_item = firefox_upload_retry::FirefoxUploadRetryItem::new(
                                                                        sms_id,
                                                                        phone_num.to_string(),
                                                                        country_id.to_string(),
                                                                        content.to_string(),
                                                                        format!("Upload failed: {}", e),
                                                                        None,
                                                                    );
                                                                     
                                                                    match firefox_upload_retry::FirefoxUploadRetryItem::insert(
                                                                        &db::get_pool().unwrap_or_else(|_| panic!("No DB pool")),
                                                                        &retry_item
                                                                    ).await {
                                                                        Ok(_) => log::info!("[火狐狸轮询] 短信上传失败（来自平台列表），已加入重试队列 - 号码: {}, Item_ID: {}, 重试ID: {}", phone_num, item_id, retry_item.id),
                                                                        Err(e) => log::error!("[火狐狸轮询] 将失败的短信加入重试队列失败: {}", e),
                                                                    }
                                                                },
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    mark_task_processed = false;
                                    log::warn!("[火狐狸轮询] 读取/查询短信失败 (SIM: {}): {}", sim_id, e)
                                },
                            }

                            if mark_task_processed {
                                no_sms_backoff_until.remove(&task_key);
                                no_sms_miss_count.remove(&task_key);
                                processed_tasks.insert(task_key);
                            } else {
                                let miss = no_sms_miss_count
                                    .entry(task_key.clone())
                                    .and_modify(|v| *v = (*v + 1).min(6))
                                    .or_insert(1);
                                let shift = (*miss as u32).saturating_sub(1);
                                let delay_secs = (3u64.saturating_mul(1u64 << shift)).min(60);
                                no_sms_backoff_until.insert(
                                    task_key.clone(),
                                    tokio::time::Instant::now()
                                        + tokio::time::Duration::from_secs(delay_secs),
                                );
                                log::info!(
                                    "[火狐狸轮询] 暂未读到短信，进入退避: 号码={}, Item_ID={}, miss_count={}, delay={}s",
                                    phone_num,
                                    item_id,
                                    miss,
                                    delay_secs
                                );
                            }
                        }
                    }
                }
            }

        if heartbeat_count % 10 == 0 {
            log::info!("[火狐狸轮询] 运行中 (心跳), 已轮询 {} 次", heartbeat_count);
        }
    }
}

#[derive(Debug, Clone, StructOpt)]
pub struct Param {
    #[structopt(subcommand)]
    pub command: Option<Command>,

    #[cfg(debug_assertions)]
    #[structopt(
        short = "l",
        long = "log",
        parse(from_os_str),
        default_value = "./logs"
    )]
    pub log_path: PathBuf,

    #[cfg(not(debug_assertions))]
    #[structopt(
        short = "l",
        long = "log",
        parse(from_os_str),
        default_value = "./logs"
    )]
    pub log_path: PathBuf,

    #[cfg(debug_assertions)]
    #[structopt(
        short = "v",
        long = "log-level",
        default_value = "debug",
        possible_values = &["off", "error", "warn", "info", "debug", "trace"]
    )]
    pub log_level: LevelFilter,

    #[cfg(not(debug_assertions))]
    #[structopt(
        short = "v",
        long = "log-level",
        default_value = "info",
        possible_values = &["off", "error", "warn", "info", "debug", "trace"]
    )]
    pub log_level: LevelFilter,

    #[structopt(
        short = "c",
        long = "config",
        parse(from_os_str),
        default_value = "./config.toml"
    )]
    pub config_file: PathBuf,

    #[cfg(target_os = "windows")]
    #[structopt(long = "no-tray")]
    pub no_tray: bool,
}

#[derive(Debug, Clone, StructOpt)]
pub enum Command {
    /// Update sms-gateway to the latest release
    Update,
    /// Show version information
    Version,
}

fn log_init(log_path: &PathBuf, log_level: &LevelFilter) -> anyhow::Result<()> {
    if !log_path.exists() {
        std::fs::create_dir_all(log_path)?;
    }
    let file_spec = FileSpec::default().directory(log_path);

    let _ = Logger::try_with_str(format!("{}", log_level))?
        .log_to_file(file_spec)
        .duplicate_to_stderr(Duplicate::All)
        .format_for_stderr(colored_detailed_format)
        .format_for_stdout(colored_detailed_format)
        //https://upload.wikimedia.org/wikipedia/commons/1/15/Xterm_256color_chart.svg
        .set_palette(String::from("b196;208;28;7;8"))
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7),
        )
        .start()?;
    Ok(())
}

// SIM检测逻辑已完全移除 - 设备映射在启动时建立，运行时不再检测
