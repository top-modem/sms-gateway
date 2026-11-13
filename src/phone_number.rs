use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::db::{Contact, SimCard, Sms};
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

/// Some SIMs carry a dummy own-number such as "440000000000" instead of their MSISDN.
pub(crate) fn is_placeholder_msisdn(raw: &str) -> bool {
    let digits = normalize_msisdn(raw);
    let trailing_zeros = digits.len() - digits.trim_end_matches('0').len();
    digits.len() < 6 || trailing_zeros >= 6 || digits.chars().all(|digit| Some(digit) == digits.chars().next())
}

/// Three UK has no 2G and switched off 3G, so USSD over the legacy network cannot work.
fn ussd_unavailable_reason(imsi: Option<&str>) -> Option<&'static str> {
    imsi.filter(|imsi| imsi.starts_with("23420"))
        .map(|_| "Three UK 没有 2G/3G 网络，无法使用 USSD；请用欢迎短信或短信取号")
}

#[derive(Debug, PartialEq)]
pub(crate) struct WelcomeSmsIdentity {
    pub operator: &'static str,
    pub phone_number: String,
}

pub(crate) fn parse_welcome_sms(message: &str) -> Option<WelcomeSmsIdentity> {
    static THREE_WELCOME: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
        Regex::new(r"(?i)\A\s*Hi\s+there,\s*welcome\s+to\s+Three\.\s+Your\s+number\s+for\s+this\s+SIM\s+is\s+(0[0-9]{10})\.(?:\s|\z)")
            .expect("valid Three welcome SMS pattern")
    });
    let captures = THREE_WELCOME.captures(message).ok()??;
    Some(WelcomeSmsIdentity {
        operator: "Three",
        phone_number: normalize_msisdn(captures.get(1)?.as_str()),
    })
}

#[cfg(test)]
mod welcome_sms_tests {
    use super::*;

    #[test]
    fn welcome_sms_three_sample_extracts_operator_and_own_number() {
        let sms = "Hi there, welcome to Three.\n\nYour number for this SIM is 07848998171.\n\nTo start using your number, you'll need to top up. You'll be charged 35p per minute for calls, 15p per text, and 10p per MB of data.";
        assert_eq!(parse_welcome_sms(sms), Some(WelcomeSmsIdentity {
            operator: "Three",
            phone_number: "7848998171".to_string(),
        }));
    }

    #[test]
    fn welcome_sms_accepts_case_and_line_ending_variations() {
        assert_eq!(
            parse_welcome_sms("  HI THERE, WELCOME TO THREE.\r\n\r\nYour number for this SIM is 07848998171.\r\n"),
            parse_welcome_sms("Hi there, welcome to Three. Your number for this SIM is 07848998171."),
        );
        assert!(parse_welcome_sms("Hi there, welcome to Three. Your number for this SIM is 07848998171.").is_some());
    }

    #[test]
    fn welcome_sms_rejects_unrelated_or_malformed_numbers() {
        for sms in [
            "Your verification code is 07848998171.",
            "Hi there, welcome to Another. Your number for this SIM is 07848998171.",
            "Hi there, welcome to Three. Call support on 07848998171.",
            "Hi there, welcome to Three. Your number for this SIM is 0784899817.",
            "Hi there, welcome to Three. Your number for this SIM is 078489981711.",
            "Forwarded: Hi there, welcome to Three. Your number for this SIM is 07848998171.",
        ] {
            assert!(parse_welcome_sms(sms).is_none(), "unexpected match: {sms}");
        }
    }
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
            mm.send_sms(&sender_sim, &contact, message)
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

/// Ports switch radio mode independently, so several can run at once.
const NUMBER_LOOKUP_CONCURRENCY: usize = 8;

/// Run `lookup` for every SIM without a phone number, saving any number it returns.
async fn lookup_missing_numbers<F, Fut>(mm: Arc<ModemManager>, task: TaskHandle, task_type: &str, lookup: F)
where
    F: Fn(Arc<ModemManager>, SimInfo) -> Fut,
    Fut: std::future::Future<Output = Result<Option<String>, String>>,
{
    use futures::StreamExt;
    let targets: Vec<_> = list_sims(&mm).await.into_iter().filter(|s| s.phone_number.is_none()).collect();
    {
        let mut t = task.write().await;
        t.running = true;
        t.task_type = task_type.to_string();
        t.total = targets.len();
        t.done = 0;
        t.current = format!("正在处理 {} 个无号码端口", targets.len());
        t.errors.clear();
        t.results.clear();
    }
    let mut results = futures::stream::iter(targets.into_iter().map(|info| {
        let mm = mm.clone();
        let lookup = &lookup;
        async move {
            let outcome = lookup(mm.clone(), info.clone()).await;
            let (phone_number, status, message) = match outcome {
                Ok(Some(phone)) => match write_phone_number(&mm, &info.sim_id, &phone).await {
                    Ok(()) => (Some(phone), PhoneResultStatus::Success, "已获取并保存号码".to_string()),
                    Err(e) => (Some(phone), PhoneResultStatus::Failed, format!("获取到号码但保存失败: {}", e)),
                },
                Ok(None) => (None, PhoneResultStatus::Skipped, "未检测到号码".to_string()),
                Err(message) => (None, PhoneResultStatus::Failed, message),
            };
            PhoneResult { com_port: info.com_port, sim_id: info.sim_id, phone_number, status, message }
        }
    }))
    .buffer_unordered(NUMBER_LOOKUP_CONCURRENCY);
    while let Some(result) = results.next().await {
        log::info!("[{}] {} {:?}: number={:?} {}", task_type, result.com_port, result.status, result.phone_number, result.message);
        let mut t = task.write().await;
        if result.status == PhoneResultStatus::Failed {
            t.errors.push(format!("{}: {}", result.com_port, result.message));
        }
        t.current = format!("{} 完成", result.com_port);
        t.results.push(result);
        t.done += 1;
    }
    let mut t = task.write().await;
    t.running = false;
    t.current = "取号完成".to_string();
}

/// Send a USSD code on 2G/3G to every SIM without a phone number, then return it to 4G.
pub async fn ussd_batch(mm: Arc<ModemManager>, task: TaskHandle, code: String) {
    lookup_missing_numbers(mm, task, "ussd", |mm, info| {
        let code = code.clone();
        async move {
            if let Some(reason) = ussd_unavailable_reason(info.imsi.as_deref()) {
                return Err(reason.to_string());
            }
            let response = mm.send_ussd_on_legacy_network(&info.sim_id, &code).await?;
            match extract_phone_from_ussd(&response) {
                Some(phone) => Ok(Some(phone)),
                None => Err(format!("USSD 回复中未找到号码: {}", response)),
            }
        }
    })
    .await;
}

#[derive(Debug, PartialEq)]
pub(crate) struct FreeSmsRoute {
    pub operator: &'static str,
    pub to: &'static str,
    pub body: &'static str,
}

/// Free "what is my number" SMS for UK operators, chosen by SIM SPN, then IMSI network.
pub(crate) fn free_sms_route(spn: Option<&str>, imsi: Option<&str>) -> Option<FreeSmsRoute> {
    const EE: FreeSmsRoute = FreeSmsRoute { operator: "EE", to: "150", body: "NUMBER" };
    const O2: FreeSmsRoute = FreeSmsRoute { operator: "O2", to: "2020", body: "NUMBER" };
    const GIFFGAFF: FreeSmsRoute = FreeSmsRoute { operator: "giffgaff", to: "43430", body: "NUMBER" };
    const ASDA: FreeSmsRoute = FreeSmsRoute { operator: "Asda Mobile", to: "2732", body: "MY NUMBER" };
    let spn = spn.unwrap_or("").trim().to_lowercase();
    if spn.contains("giffgaff") { return Some(GIFFGAFF); }
    if spn.contains("asda") { return Some(ASDA); }
    if spn == "o2" || spn.starts_with("o2 ") || spn.starts_with("o2-") { return Some(O2); }
    if spn == "ee" { return Some(EE); }
    // giffgaff shares O2's 234-10 network code, so its SPN check must come first.
    match imsi.map(str::trim).filter(|imsi| imsi.len() >= 5).map(|imsi| &imsi[..5]) {
        Some("23410") => Some(O2),
        Some("23430" | "23431" | "23432" | "23433" | "23434") => Some(EE),
        _ => None,
    }
}

/// Text the operator's free number-lookup short code and read the number from its reply.
pub async fn free_sms_batch(mm: Arc<ModemManager>, task: TaskHandle) {
    lookup_missing_numbers(mm, task, "free_sms", |mm, info| async move {
        let spn = mm.read_spn(&info.sim_id).await;
        let imsi = mm.get_sim_card_cached(&info.sim_id).await.and_then(|card| card.imsi);
        let route = free_sms_route(spn.as_deref(), imsi.as_deref())
            .ok_or_else(|| format!("不支持的运营商 (SPN: {}, IMSI: {})", spn.as_deref().unwrap_or("-"),
                imsi.as_deref().map(|i| i.chars().take(5).collect::<String>()).unwrap_or_else(|| "-".into())))?;
        log::info!("[FreeSMS {}] SPN={:?} IMSI prefix={:?} -> {} '{}' to {}", info.com_port, spn,
            imsi.as_deref().map(|i| i.chars().take(5).collect::<String>()), route.operator, route.body, route.to);
        let before = Sms::max_id().await.map_err(|e| format!("读取短信记录失败: {}", e))?;
        let contact = Contact { id: route.to.to_string(), name: route.to.to_string() };
        mm.send_sms(&info.sim_id, &contact, route.body)
            .await
            .map_err(|e| format!("{}: 发送 {} 到 {} 失败: {}", route.operator, route.body, route.to, e))?;
        for _ in 0..30 {
            sleep(Duration::from_secs(4)).await;
            let _ = mm.read_sms_sync_insert(&info.sim_id, SmsType::RecUnread).await;
            let replies = Sms::incoming_after_id(&info.sim_id, before).await.unwrap_or_default();
            for (sender, message) in &replies {
                log::info!("[FreeSMS {}] reply from {}: {:?}", info.com_port, sender, message);
            }
            if let Some(phone) = replies.iter().find_map(|(_, message)| extract_phone_from_ussd(message)) {
                return Ok(Some(phone));
            }
        }
        Err(format!("{}: 已发送 {} 到 {}，2 分钟内未收到含号码的回复", route.operator, route.body, route.to))
    })
    .await;
}

/// Find one UK mobile number (07…, 447…, +447…, 00447…) and return it as national `7XXXXXXXXX`.
pub(crate) fn extract_phone_from_ussd(response: &str) -> Option<String> {
    static UK_MOBILE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
        Regex::new(r"(?<![\d+])(?:(?:\+|00)?44\s?|0)7(?:[ -]?\d){9}(?!\d)").expect("valid UK mobile pattern")
    });
    let matched = UK_MOBILE.find(response).ok()??;
    let digits: String = matched.as_str().chars().filter(char::is_ascii_digit).collect();
    let national = digits.trim_start_matches("00").strip_prefix("44").unwrap_or(&digits).trim_start_matches('0');
    (national.len() == 10).then(|| national.to_string())
}

#[cfg(test)]
mod number_lookup_tests {
    use super::*;

    #[test]
    fn uk_number_is_extracted_in_national_form() {
        for text in ["Your number is 07848998171.", "Your mobile number is +447848998171", "447848998171", "MSISDN: 0044 7848998171", "Number: 07848 998171"] {
            assert_eq!(extract_phone_from_ussd(text).as_deref(), Some("7848998171"), "{text}");
        }
        for text in ["Balance £5.00", "Call 0800123456", "078489981712", "Ref 1078489981"] {
            assert_eq!(extract_phone_from_ussd(text), None, "{text}");
        }
    }

    #[test]
    fn placeholder_sim_numbers_are_not_real_numbers() {
        for dummy in ["440000000000", "+440000000000", "0000000000", "1111111111", "12345"] {
            assert!(is_placeholder_msisdn(dummy), "{dummy}");
        }
        for real in ["7848960848", "+447557537442", "07345830360", "7700900100"] {
            assert!(!is_placeholder_msisdn(real), "{real}");
        }
        assert!(ussd_unavailable_reason(Some("234201234567890")).is_some());
        assert!(ussd_unavailable_reason(Some("234151234567890")).is_none());
    }

    #[test]
    fn free_sms_route_uses_spn_before_imsi() {
        assert_eq!(free_sms_route(Some("giffgaff"), Some("234100000000000")).unwrap().to, "43430");
        assert_eq!(free_sms_route(Some("ASDA Mobile"), None).unwrap().body, "MY NUMBER");
        assert_eq!(free_sms_route(None, Some("234100000000000")).unwrap().to, "2020");
        assert_eq!(free_sms_route(Some("EE"), None).unwrap().to, "150");
        assert_eq!(free_sms_route(Some(""), Some("234330000000000")).unwrap().to, "150");
        assert_eq!(free_sms_route(Some("Lyca"), Some("234260000000000")), None);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Internal helpers
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
struct SimInfo {
    com_port: String,
    sim_id: String,
    phone_number: Option<String>,
    imsi: Option<String>,
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
                phone_number: card.phone_number.clone()
                    .filter(|phone| !phone.trim().is_empty() && !is_placeholder_msisdn(phone)),
                imsi: card.imsi.clone(),
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
