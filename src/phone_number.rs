use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::db::{Contact, SimCard, Sms};
use crate::modem::SmsSendMode;
use crate::modem::manager::ModemManager;
use crate::modem::types::SmsType;
use log;

use fancy_regex::Regex;

/// Shared, in-memory state for a running phone-number management task.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PhoneNumberTask {
    pub running: bool,
    pub task_type: String,
    pub total: usize,
    pub done: usize,
    pub current: String,
    pub errors: Vec<String>,
    pub results: Vec<PhoneResult>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PhoneResult {
    pub com_port: String,
    pub sim_id: String,
    pub phone_number: Option<String>,
    pub status: PhoneResultStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum PhoneResultStatus {
    Success,
    Skipped,
    Failed,
}

pub type TaskHandle = Arc<RwLock<PhoneNumberTask>>;

pub fn new_task_handle() -> TaskHandle {
    Arc::new(RwLock::new(PhoneNumberTask::default()))
}

fn normalize_import_msisdn(raw: &str) -> String {
    let digits: String = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    digits.trim_start_matches('0').to_string()
}

/// Normalize a modem-reported MSISDN (AT+CNUM etc.) for storage/display:
/// digits only, no `+` prefix, leading zeros stripped.
/// e.g. "+7770065802" -> "7770065802", "07787905235" -> "7787905235".
pub fn normalize_msisdn(raw: &str) -> String {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.trim_start_matches('0').to_string()
}

/// Batch import phone numbers from `(iccid, msisdn)` pairs.
pub async fn import_phone_numbers(
    mm: Arc<ModemManager>,
    task: TaskHandle,
    entries: Vec<(String, String)>,
) {
    {
        let mut t = task.write().await;
        t.running = true;
        t.task_type = "import".to_string();
        t.total = entries.len();
        t.done = 0;
        t.current.clear();
        t.errors.clear();
        t.results.clear();
    }

    for (idx, (iccid, msisdn)) in entries.iter().enumerate() {
        let msisdn_clean = normalize_import_msisdn(msisdn);

        {
            let mut t = task.write().await;
            t.current = format!("正在导入 ICCID {} → {}", iccid, msisdn_clean);
        }

        if msisdn_clean.is_empty() {
            let result = PhoneResult {
                com_port: String::new(),
                sim_id: iccid.clone(),
                phone_number: None,
                status: PhoneResultStatus::Failed,
                message: "手机号格式无效".to_string(),
            };

            let mut t = task.write().await;
            t.errors.push(format!("{}: {}", iccid, result.message));
            t.results.push(result);
            t.done = idx + 1;
            continue;
        }

        let result = if let Some(sim_id) = find_sim_id_by_iccid(iccid).await {
            match write_phone_number(&mm, &sim_id, &msisdn_clean).await {
                Ok(_) => PhoneResult {
                    com_port: com_port_for_sim(&mm, &sim_id).await.unwrap_or_default(),
                    sim_id: sim_id.clone(),
                    phone_number: Some(msisdn_clean.clone()),
                    status: PhoneResultStatus::Success,
                    message: "导入成功".to_string(),
                },
                Err(e) => PhoneResult {
                    com_port: com_port_for_sim(&mm, &sim_id).await.unwrap_or_default(),
                    sim_id: sim_id.clone(),
                    phone_number: None,
                    status: PhoneResultStatus::Failed,
                    message: format!("写入 SIM 失败: {}", e),
                },
            }
        } else {
            PhoneResult {
                com_port: String::new(),
                sim_id: iccid.clone(),
                phone_number: None,
                status: PhoneResultStatus::Failed,
                message: "未找到匹配的 SIM".to_string(),
            }
        };

        {
            let mut t = task.write().await;
            if result.status == PhoneResultStatus::Failed {
                t.errors.push(format!("{}: {}", iccid, result.message));
            }
            t.results.push(result);
            t.done = idx + 1;
        }
    }

    {
        let mut t = task.write().await;
        t.running = false;
        t.current = "导入完成".to_string();
    }
}

/// Discover phone numbers by making calls between modems.
/// COM1 must already have a MSISDN.
pub async fn call_exchange(mm: Arc<ModemManager>, task: TaskHandle) {
    run_pairwise_exchange(
        mm,
        task,
        "call",
        |mm, caller_sim, receiver_sim, receiver_phone| async move {
            let caller_modem = mm
                .get_modem(&caller_sim)
                .await
                .ok_or("Caller modem not found")?;
            let receiver_modem = mm
                .get_modem(&receiver_sim)
                .await
                .ok_or("Receiver modem not found")?;

            // Ensure both sides are idle before making a call.
            let _ = receiver_modem.hangup_call().await;
            let _ = caller_modem.hangup_call().await;
            sleep(Duration::from_millis(500)).await;

            // Caller dials the receiver.
            caller_modem
                .make_call(&receiver_phone)
                .await
                .map_err(|e| format!("拨号失败: {}", e))?;
            log::debug!(
                "[号码交换] {} 呼出到 {} ({}) 成功",
                caller_sim,
                receiver_sim,
                receiver_phone
            );

            // Wait for the receiver to see the incoming call.
            let mut answered = false;
            for attempt in 0..15 {
                sleep(Duration::from_millis(1000)).await;
                let clcc = receiver_modem.query_clcc().await.unwrap_or_default();
                log::debug!(
                    "[号码交换] {} 第{}次 CLCC: {}",
                    receiver_sim,
                    attempt,
                    clcc.replace('\r', "").replace('\n', " ")
                );
                if clcc.contains("+CLCC:") {
                    // There is a call visible; try to answer.
                    match receiver_modem.answer_call().await {
                        Ok(_) => {
                            answered = true;
                            break;
                        }
                        Err(e) => {
                            log::warn!(
                                "[号码交换] {} 接听失败 (尝试{}): {}",
                                receiver_sim,
                                attempt,
                                e
                            );
                        }
                    }
                }
            }

            if !answered {
                let _ = caller_modem.hangup_call().await;
                let _ = receiver_modem.hangup_call().await;
                return Err("接听失败，未检测到来电".to_string());
            }

            // Give network time to set up the active call.
            sleep(Duration::from_millis(2000)).await;

            // Read CLCC on receiver to get the caller ID.
            let clcc_response = receiver_modem
                .query_clcc()
                .await
                .map_err(|e| format!("查询 CLCC 失败: {}", e))?;
            log::debug!(
                "[号码交换] {} 接通后 CLCC: {}",
                receiver_sim,
                clcc_response.replace('\r', "").replace('\n', " ")
            );
            let caller = mm
                .read_clcc_caller_number(&receiver_sim)
                .await
                .map_err(|e| format!("读取 CLCC 失败: {}", e))?
                .ok_or("CLCC 中未找到来电号码")?;

            // Hangup both sides.
            let _ = caller_modem.hangup_call().await;
            let _ = receiver_modem.hangup_call().await;

            Ok(caller)
        },
    )
    .await;
}

/// Discover phone numbers by sending SMS between modems.
/// COM1 must already have a MSISDN.
pub async fn sms_exchange(mm: Arc<ModemManager>, task: TaskHandle) {
    run_pairwise_exchange(
        mm,
        task,
        "sms",
        |mm, sender_sim, receiver_sim, receiver_phone| async move {
            let message = "Hi";

            // Send SMS from sender to receiver.
            let contact = Contact {
                id: receiver_sim.clone(),
                name: receiver_phone.clone(),
            };
            mm.send_sms(&sender_sim, &contact, message, SmsSendMode::Pdu)
                .await
                .map_err(|e| format!("发短信失败: {}", e))?;

            // Wait for delivery.
            sleep(Duration::from_secs(6)).await;

            // Force read on receiver.
            mm.read_sms_sync_insert(&receiver_sim, SmsType::RecUnread)
                .await
                .map_err(|e| format!("读取短信失败: {}", e))?;

            // Look up the latest incoming SMS on the receiver SIM.
            let latest = Sms::find_latest_incoming_by_sim_id(&receiver_sim)
                .await
                .map_err(|e| format!("查询短信失败: {}", e))?
                .ok_or("未收到新短信")?;

            // contact_id may be a UUID; resolve to phone number via contacts table.
            let phone = Contact::query_all()
                .await
                .ok()
                .and_then(|contacts| contacts.into_iter().find(|c| c.id == latest.contact_id))
                .map(|c| c.name)
                .filter(|n| !n.is_empty())
                .or(Some(latest.contact_id))
                .unwrap_or_default();

            if phone.is_empty() {
                return Err("无法解析发送方号码".to_string());
            }

            Ok(phone)
        },
    )
    .await;
}

/// Send USSD code to every SIM that does not yet have a phone number and try to extract it.
pub async fn ussd_batch(mm: Arc<ModemManager>, task: TaskHandle, code: String) {
    let sims = list_sims(&mm).await;
    let targets: Vec<_> = sims
        .into_iter()
        .filter(|s| s.phone_number.is_none())
        .collect();

    {
        let mut t = task.write().await;
        t.running = true;
        t.task_type = "ussd".to_string();
        t.total = targets.len();
        t.done = 0;
        t.current.clear();
        t.errors.clear();
        t.results.clear();
    }

    for (idx, info) in targets.iter().enumerate() {
        {
            let mut t = task.write().await;
            t.current = format!("正在对 {} 发送 USSD: {}", info.com_port, code);
        }

        let result = match mm.send_ussd(&info.sim_id, &code).await {
            Ok(response) => {
                let detected = extract_phone_from_ussd(&response);
                let message = if detected.is_some() {
                    "USSD 响应中检测到号码".to_string()
                } else {
                    format!("未检测到号码: {}", response)
                };

                if let Some(ref phone) = detected {
                    if let Err(e) = write_phone_number(&mm, &info.sim_id, phone).await {
                        PhoneResult {
                            com_port: info.com_port.clone(),
                            sim_id: info.sim_id.clone(),
                            phone_number: detected.clone(),
                            status: PhoneResultStatus::Failed,
                            message: format!("检测到号码但写入失败: {}", e),
                        }
                    } else {
                        PhoneResult {
                            com_port: info.com_port.clone(),
                            sim_id: info.sim_id.clone(),
                            phone_number: detected.clone(),
                            status: PhoneResultStatus::Success,
                            message,
                        }
                    }
                } else {
                    PhoneResult {
                        com_port: info.com_port.clone(),
                        sim_id: info.sim_id.clone(),
                        phone_number: None,
                        status: PhoneResultStatus::Skipped,
                        message,
                    }
                }
            }
            Err(e) => PhoneResult {
                com_port: info.com_port.clone(),
                sim_id: info.sim_id.clone(),
                phone_number: None,
                status: PhoneResultStatus::Failed,
                message: format!("USSD 失败: {}", e),
            },
        };

        {
            let mut t = task.write().await;
            if result.status == PhoneResultStatus::Failed {
                t.errors
                    .push(format!("{}: {}", info.com_port, result.message));
            }
            t.results.push(result);
            t.done = idx + 1;
        }
    }

    {
        let mut t = task.write().await;
        t.running = false;
        t.current = "USSD 批量操作完成".to_string();
    }
}

pub(crate) fn extract_phone_from_ussd(response: &str) -> Option<String> {
    // Try common UK mobile pattern first.
    let re = Regex::new(r"(?:(?:\+|00)44|0)7\d{9}").ok()?;
    re.find(response)
        .ok()
        .flatten()
        .map(|m| m.as_str().to_string())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Internal helpers
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
struct SimInfo {
    com_port: String,
    sim_id: String,
    phone_number: Option<String>,
}

async fn list_sims(mm: &Arc<ModemManager>) -> Vec<SimInfo> {
    let mut result = Vec::new();
    let sim_ids = mm.get_sim_ids().await;
    for sim_id in sim_ids {
        if let Some(card) = mm.get_sim_card_cached(&sim_id).await {
            if sim_id.starts_with("fallback_sim_") {
                continue;
            }
            let com_port = mm
                .get_modem(&sim_id)
                .await
                .map(|m| m.com_port.clone())
                .unwrap_or_default();
            result.push(SimInfo {
                com_port,
                sim_id,
                phone_number: card.phone_number.clone(),
            });
        }
    }
    // Stable order by com port number.
    result.sort_by(|a, b| extract_com_index(&a.com_port).cmp(&extract_com_index(&b.com_port)));
    result
}

fn extract_com_index(port: &str) -> u32 {
    port.trim_start_matches("COM").parse().unwrap_or(u32::MAX)
}

async fn find_sim_id_by_iccid(iccid: &str) -> Option<String> {
    SimCard::query_all()
        .await
        .ok()
        .and_then(|cards| cards.into_iter().find(|c| c.id.trim() == iccid.trim()))
        .map(|c| c.id)
}

async fn com_port_for_sim(mm: &Arc<ModemManager>, sim_id: &str) -> Option<String> {
    mm.get_modem(sim_id).await.map(|m| m.com_port.clone())
}

async fn write_phone_number(
    mm: &Arc<ModemManager>,
    sim_id: &str,
    phone: &str,
) -> anyhow::Result<()> {
    mm.set_sim_phone_number(sim_id, phone).await?;
    if let Some(mut sim_card) = SimCard::query_all()
        .await?
        .into_iter()
        .find(|s| s.id == sim_id)
    {
        sim_card
            .update_phone_number(Some(phone.to_string()))
            .await?;
        mm.update_sim_cache(sim_card).await;
    }
    Ok(())
}

/// Runs a pairwise exchange where `no_number` SIMs interact with `has_number` SIMs.
async fn run_pairwise_exchange<F, Fut>(
    mm: Arc<ModemManager>,
    task: TaskHandle,
    task_type: &str,
    interact: F,
) where
    F: Fn(Arc<ModemManager>, String, String, String) -> Fut + Send + Sync + Copy + 'static,
    Fut: std::future::Future<Output = Result<String, String>> + Send,
{
    let sims = list_sims(&mm).await;

    let mut has_number: Vec<SimInfo> = sims
        .clone()
        .into_iter()
        .filter(|s| s.phone_number.is_some())
        .collect();
    let mut no_number: Vec<SimInfo> = sims
        .into_iter()
        .filter(|s| s.phone_number.is_none())
        .collect();

    {
        let mut t = task.write().await;
        t.running = true;
        t.task_type = task_type.to_string();
        t.total = no_number.len();
        t.done = 0;
        t.current.clear();
        t.errors.clear();
        t.results.clear();
    }

    if has_number.is_empty() {
        let mut t = task.write().await;
        t.running = false;
        t.errors
            .push("至少需要一个端口已有 MSISDN（建议 COM1）".to_string());
        return;
    }

    // Track processed (successful) targets so we can use them as sources in later rounds.
    let mut newly_has: Vec<SimInfo> = Vec::new();
    let mut processed = 0usize;

    while !no_number.is_empty() {
        let mut next_round_no_number = Vec::new();
        let sources: Vec<SimInfo> = has_number.iter().chain(newly_has.iter()).cloned().collect();

        for (i, target) in no_number.iter().enumerate() {
            let source = match sources.get(i) {
                Some(s) => s,
                None => {
                    // Not enough sources yet; postpone to next round.
                    next_round_no_number.push(target.clone());
                    continue;
                }
            };

            let receiver_phone = source.phone_number.clone().unwrap_or_default();
            if receiver_phone.is_empty() {
                next_round_no_number.push(target.clone());
                continue;
            }

            {
                let mut t = task.write().await;
                t.current = format!(
                    "{}: {} ({}) → {} ({})",
                    match task_type {
                        "call" => "呼叫",
                        "sms" => "发短信",
                        _ => "交互",
                    },
                    target.com_port,
                    target.sim_id,
                    source.com_port,
                    receiver_phone
                );
            }

            let result = match interact(
                mm.clone(),
                target.sim_id.clone(),
                source.sim_id.clone(),
                receiver_phone,
            )
            .await
            {
                Ok(phone) => match write_phone_number(&mm, &target.sim_id, &phone).await {
                    Ok(_) => {
                        let mut info = target.clone();
                        info.phone_number = Some(phone.clone());
                        newly_has.push(info);
                        PhoneResult {
                            com_port: target.com_port.clone(),
                            sim_id: target.sim_id.clone(),
                            phone_number: Some(phone),
                            status: PhoneResultStatus::Success,
                            message: "获取号码成功".to_string(),
                        }
                    }
                    Err(e) => PhoneResult {
                        com_port: target.com_port.clone(),
                        sim_id: target.sim_id.clone(),
                        phone_number: Some(phone),
                        status: PhoneResultStatus::Failed,
                        message: format!("获取到号码但写入失败: {}", e),
                    },
                },
                Err(e) => PhoneResult {
                    com_port: target.com_port.clone(),
                    sim_id: target.sim_id.clone(),
                    phone_number: None,
                    status: PhoneResultStatus::Failed,
                    message: e,
                },
            };

            let is_failed = result.status == PhoneResultStatus::Failed;
            {
                let mut t = task.write().await;
                if is_failed {
                    t.errors
                        .push(format!("{}: {}", target.com_port, result.message));
                }
                t.results.push(result);
                processed += 1;
                t.done = processed;
            }

            // Brief pause between devices to keep the serial bus / network sane.
            sleep(Duration::from_millis(3000)).await;
        }

        has_number.extend(newly_has.drain(..));
        no_number = next_round_no_number;
    }

    {
        let mut t = task.write().await;
        t.running = false;
        t.current = format!(
            "{}完成",
            match task_type {
                "call" => "呼叫交换",
                "sms" => "短信交换",
                _ => "交互",
            }
        );
    }
}
