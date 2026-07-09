use std::{path::PathBuf, sync::Arc};

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
mod modem;
mod phone_number;
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
    if let Err(err) = log_init(&param.log_path, &param.log_level) {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    };
    if let Err(err) = db_init().await {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }
    #[cfg(debug_assertions)]
    let config = match config::AppConfig::load(&PathBuf::from("./config.toml")) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    };
    #[cfg(not(debug_assertions))]
    let config = match config::AppConfig::load(&param.config_file) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    };

    let modem_manager = match ModemManager::initialize(&config).await {
        Ok(manager) => Arc::new(manager),
        Err(err) => {
            eprintln!("Failed to initialize ModemManager: {}", err);
            std::process::exit(1);
        }
    };

    let sse_manager = Arc::new(api::SseManager::new());

    let transcribe_cfg = TranscribeConfig::from_settings(&config.settings);
    modem_manager.start_urc_handlers(sse_manager.clone(), transcribe_cfg.clone()).await;

    let webhook_manager = match config.settings.webhooks.clone() {
        Some(cfgs) => Some(webhook::start_webhook_worker_with_concurrency(
            cfgs,
            config.settings.webhooks_max_concurrent.unwrap_or(1),
        )),
        _ => None,
    };

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

    if let Ok(_) = api::run_api(
        modem_manager.clone(),
        &config.settings.server_host,
        &config.settings.server_port,
        &config.settings.username.unwrap(),
        &config.settings.password.unwrap(),
        sse_manager.clone(),
        config.settings.barcode_output_file.clone(),
        config.settings.barcode_launcher_path.clone(),
    )
    .await
    {};
}

async fn read_sms_worker(
    modem_manager: ModemManagerRef,
    read_sms_frequency: u64,
    sse_manager: Arc<SseManager>,
    webhook_manager: Option<webhook::WebhookManager>,
) {
    loop {
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
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        modem_manager
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
    use std::collections::HashSet;
    let poll_interval = tokio::time::Duration::from_secs(3);
    let mut processed_tasks: HashSet<String> = HashSet::new();
    let mut heartbeat_count: u32 = 0;

    loop {
        tokio::time::sleep(poll_interval).await;
        heartbeat_count += 1;

        let api_key = match db::AppSetting::get("firefox_api_key").await {
            Ok(Some(key)) => key,
            _ => continue,
        };

        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                log::error!("[火狐狸轮询] 创建 HTTP 客户端失败: {}", e);
                continue;
            }
        };

        match firefox_api::get_wait_phone_list(&client, &api_key).await {
            Ok(resp) => {
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

                            // Persist/refresh platform item
                            if let Err(e) = crate::db::FirefoxPlatformItem::upsert(&item_id, country_id, phone_num).await {
                                log::warn!("[火狐狸轮询] 保存平台 item 失败: item_id={}, phone={}, error={}", item_id, phone_num, e);
                            }

                            let task_key = format!("{}|{}|{}", country_id, phone_num, item_id);

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

                            log::info!("[火狐狸轮询] 找到 SIM: {}, 强制读取短信", sim_id);

                            // Read SMS from modem, sync to DB, and get the latest incoming message
                            match modem_manager.read_sms_and_get_latest(&sim_id).await {
                                Ok(Some(sms)) => {
                                    log::info!("[火狐狸轮询] 获取到短信内容 ({}): {}, 上传中 - 号码: {}", sms.contact_id, sms.message, phone_num);
                                    match firefox_api::upload_sms(&client, &api_key, country_id, phone_num, &sms.message).await {
                                        Ok(upload_resp) if upload_resp.code == "1" => {
                                            log::info!("[火狐狸轮询] 短信上传成功 - 号码: {}, Item_ID: {}, 响应: {:?}", phone_num, item_id, upload_resp);
                                            let resp_data = upload_resp.data.as_deref();
                                            if let Err(e) = crate::db::Sms::mark_uploaded(sms.id, &item_id, resp_data).await {
                                                log::warn!("[火狐狸轮询] 标记 SMS 上传状态失败: id={}, error={}", sms.id, e);
                                            }
                                        }
                                        Ok(upload_resp) => log::warn!("[火狐狸轮询] 短信上传失败 (平台返回) - 号码: {}, Item_ID: {}, code: {}, data: {:?}", phone_num, item_id, upload_resp.code, upload_resp.data),
                                        Err(e) => log::warn!("[火狐狸轮询] 短信上传失败 - 号码: {}, Item_ID: {}, 错误: {}", phone_num, item_id, e),
                                    }
                                }
                                Ok(None) => {
                                    log::warn!("[火狐狸轮询] 读取 SMS 后未找到短信 (SIM: {}, 号码: {})", sim_id, phone_num);
                                    // Check the result list in case platform already has content
                                    if let Ok(result_resp) = firefox_api::get_result_phone_list(&client, &api_key, country_id, phone_num, &item_id).await {
                                        if let Some(data) = result_resp.data {
                                            if let Some(arr) = data.as_array() {
                                                for result_item in arr {
                                                    if let Some(content) = result_item.get("Phone_SmsContent").and_then(|v| v.as_str()) {
                                                        if !content.is_empty() {
                                                            log::info!("[火狐狸轮询] 从平台获取到短信内容, 上传中 - 号码: {}, Item_ID: {}", phone_num, item_id);
                                                            match firefox_api::upload_sms(&client, &api_key, country_id, phone_num, content).await {
                                                                Ok(upload_resp) if upload_resp.code == "1" => log::info!("[火狐狸轮询] 短信上传成功 - 号码: {}, Item_ID: {}, 响应: {:?}", phone_num, item_id, upload_resp),
                                                                Ok(upload_resp) => log::warn!("[火狐狸轮询] 短信上传失败 (平台返回) - 号码: {}, Item_ID: {}, code: {}, data: {:?}", phone_num, item_id, upload_resp.code, upload_resp.data),
                                                                Err(e) => log::warn!("[火狐狸轮询] 短信上传失败 - 号码: {}, Item_ID: {}, 错误: {}", phone_num, item_id, e),
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => log::warn!("[火狐狸轮询] 读取/查询短信失败 (SIM: {}): {}", sim_id, e),
                            }

                            processed_tasks.insert(task_key);
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("[火狐狸轮询] 获取等待列表失败: {}", e);
            }
        }

        if heartbeat_count % 10 == 0 {
            log::info!("[火狐狸轮询] 运行中 (心跳), 已轮询 {} 次", heartbeat_count);
        }
    }
}

#[derive(Debug, StructOpt)]
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
        default_value = "/var/lib/sms-gateway/log"
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
        default_value = "/etc/sms-gateway/config.toml"
    )]
    pub config_file: PathBuf,
}

#[derive(Debug, StructOpt)]
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
