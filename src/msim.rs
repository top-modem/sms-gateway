use std::{collections::HashMap, sync::{Arc, Weak, atomic::{AtomicBool, Ordering}}, time::Duration};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex, Semaphore};

use crate::{api::SseManager, db, modem::{core::Modem, types::MachineType}, modem::manager::TranscribeConfig, ModemManagerRef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemResult {
    pub slot: i64,
    pub status: String,
    pub iccid: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    pub com_port: String,
    pub kind: String,
    pub status: String,
    pub stage: String,
    pub current_slot: Option<i64>,
    pub return_slot: Option<i64>,
    pub restore_status: String,
    pub selection_verified: bool,
    pub selected_slot: Option<i64>,
    pub items: Vec<ItemResult>,
    pub error: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScanRequest {
    pub slots: Option<Vec<i64>>,
    pub return_slot: Option<i64>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SelectRequest {
    pub expected_iccid: Option<String>,
    pub request_id: Option<String>,
}

struct VerifiedSelection {
    slot: i64,
    iccid: Option<String>,
    modem: Option<Weak<Modem>>,
}

pub struct MsimService {
    manager: ModemManagerRef,
    sse: Arc<SseManager>,
    transcribe: Option<Arc<TranscribeConfig>>,
    gate: Arc<Semaphore>,
    control: Mutex<HashMap<String, Arc<AtomicBool>>>,
    verified: Mutex<HashMap<String, VerifiedSelection>>,
    events: broadcast::Sender<Operation>,
}

fn validate_slots(slots: &[i64]) -> Result<()> {
    if slots.is_empty() || slots.len() > 4 || slots.iter().any(|slot| !(1..=4).contains(slot)) {
        bail!("invalid: choose slots 1 through 4");
    }
    let mut unique = slots.to_vec();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() != slots.len() { bail!("invalid: duplicate slots"); }
    Ok(())
}

fn confirmed_empty(response: &str) -> bool {
    response.lines().any(|line| matches!(line.trim().to_ascii_uppercase().as_str(),
        "+CME ERROR: 10" | "+CME ERROR: SIM NOT INSERTED" | "NO SIM"))
}

fn sim_ready(response: &str) -> bool {
    response.lines().any(|line| line.trim().eq_ignore_ascii_case("+CPIN: READY"))
}

fn transport_failed(error: &anyhow::Error) -> bool {
    error.is::<tokio::time::error::Elapsed>() || error.downcast_ref::<std::io::Error>()
        .is_some_and(|error| !matches!(error.kind(), std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData))
}

impl MsimService {
    pub async fn new(manager: ModemManagerRef, sse: Arc<SseManager>, transcribe: Option<Arc<TranscribeConfig>>) -> Result<Self> {
        let pool = db::get_pool()?;
        let interrupted: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, snapshot FROM msim_operations WHERE status = 'running'",
        ).fetch_all(pool).await?;
        for (id, snapshot) in interrupted {
            let mut operation: Operation = serde_json::from_str(&snapshot)?;
            operation.status = "interrupted".into();
            operation.stage = "interrupted".into();
            operation.selection_verified = false;
            operation.restore_status = "unknown".into();
            operation.error = Some("Application restarted; select a slot to verify the card".into());
            operation.finished_at = Some(chrono::Utc::now().to_rfc3339());
            sqlx::query("UPDATE msim_operations SET status = 'interrupted', snapshot = ? WHERE id = ?")
                .bind(serde_json::to_string(&operation)?).bind(id).execute(pool).await?;
        }
        let (events, _) = broadcast::channel(64);
        Ok(Self { manager, sse, transcribe, gate: Arc::new(Semaphore::new(1)),
            control: Mutex::new(HashMap::new()), verified: Mutex::new(HashMap::new()), events })
    }

    fn configured(&self, port: &str) -> Result<()> {
        if !self.manager.configured_ports().iter().any(|(name, _)| name == port) {
            bail!("not found: configured port {}", port);
        }
        Ok(())
    }

    async fn publish(&self, operation: &Operation) -> Result<()> {
        sqlx::query("UPDATE msim_operations SET status = ?, snapshot = ?, updated_at = datetime('now') WHERE id = ?")
            .bind(&operation.status).bind(serde_json::to_string(operation)?).bind(&operation.id)
            .execute(db::get_pool()?).await?;
        let _ = self.events.send(operation.clone());
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Operation> { self.events.subscribe() }

    pub async fn operation(&self, id: &str) -> Result<Operation> {
        let snapshot: Option<String> = sqlx::query_scalar("SELECT snapshot FROM msim_operations WHERE id = ?")
            .bind(id).fetch_optional(db::get_pool()?).await?;
        Ok(serde_json::from_str(&snapshot.ok_or_else(|| anyhow!("not found: operation"))?)?)
    }

    async fn latest(&self, port: &str) -> Result<Option<Operation>> {
        let snapshot: Option<String> = sqlx::query_scalar(
            "SELECT snapshot FROM msim_operations WHERE com_port = ? ORDER BY rowid DESC LIMIT 1",
        ).bind(port).fetch_optional(db::get_pool()?).await?;
        snapshot.map(|snapshot| serde_json::from_str(&snapshot).map_err(Into::into)).transpose()
    }

    async fn selection(&self, port: &str) -> Result<Option<i64>> {
        let Some(modem) = self.manager.get_modem_by_com_port(port).await else { return Ok(None) };
        let live_iccid = modem.sim_id.read().await.clone();
        let mut verified = self.verified.lock().await;
        let Some(expected) = verified.get_mut(port) else { return Ok(None) };
        let same_connection = expected.modem.as_ref().is_none_or(|previous| {
            previous.upgrade().is_some_and(|previous| Arc::ptr_eq(&previous, &modem))
        });
        if !same_connection || (live_iccid.is_some() && live_iccid != expected.iccid) {
            verified.remove(port);
            return Ok(None);
        }
        if live_iccid.is_none() || expected.iccid.is_none() { return Ok(None); }
        expected.modem = Some(Arc::downgrade(&modem));
        Ok(Some(expected.slot))
    }

    pub async fn inventory(&self, port: &str) -> Result<Value> {
        self.configured(port)?;
        let hardware = db::get_port_hardware(port).await?;
        if hardware.as_ref().map(|hardware| hardware.machine_type.as_str()) != Some("msim") {
            bail!("invalid: port is not a confirmed Multi-SIM device");
        }
        let selected = self.selection(port).await?;
        Ok(json!({ "com_port": port, "slots": db::get_msim_slots(port).await?,
            "active_slot": hardware.and_then(|hardware| hardware.active_slot),
            "selection_verified": selected.is_some(), "verified_slot": selected,
            "operation": self.latest(port).await? }))
    }

    pub async fn ports(&self) -> Result<Value> {
        let hardware = db::get_port_hardware_map().await?;
        let mut ports = Vec::new();
        for (port, _) in self.manager.configured_ports() {
            if hardware.get(&port).is_some_and(|hardware| hardware.machine_type == "msim") {
                ports.push(self.inventory(&port).await?);
            }
        }
        Ok(json!(ports))
    }

    pub async fn cancel(&self, id: &str) -> Result<Operation> {
        let operation = self.operation(id).await?;
        if let Some(cancel) = self.control.lock().await.get(id) { cancel.store(true, Ordering::SeqCst); }
        Ok(operation)
    }

    pub async fn phone(&self, port: &str, slot: i64, iccid: &str, raw: &str) -> Result<Value> {
        validate_slots(&[slot])?;
        let _permit = self.gate.clone().try_acquire_owned().map_err(|_| anyhow!("busy: a slot operation is running"))?;
        self.inventory(port).await?;
        if self.latest(port).await?.is_some_and(|operation| operation.status == "running") {
            bail!("busy: port operation is running");
        }
        let phone = raw.trim();
        if !phone.chars().all(|character| character.is_ascii_digit() || character == '+')
            || phone.matches('+').count() > 1 || (phone.contains('+') && !phone.starts_with('+')) {
            bail!("invalid: phone number must contain digits and an optional leading +");
        }
        let normalized = crate::phone_number::normalize_msisdn(phone);
        if !phone.is_empty() && normalized.is_empty() { bail!("invalid: phone number"); }
        if !db::save_msim_phone(port, slot, iccid, if normalized.is_empty() { None } else { Some(&normalized) }).await? {
            bail!("conflict: card changed; refresh the slot");
        }
        if let Some(card) = db::SimCard::find_by_conditions(Some(iccid), None, None, None).await?.into_iter().next() {
            self.manager.update_sim_cache(card).await;
        }
        self.inventory(port).await
    }

    pub async fn scan(self: Arc<Self>, port: &str, request: ScanRequest) -> Result<Operation> {
        let slots = request.slots.unwrap_or_else(|| vec![1, 2, 3, 4]);
        validate_slots(&slots)?;
        let verified = self.selection(port).await?;
        let return_slot = request.return_slot.or(verified)
            .ok_or_else(|| anyhow!("invalid: choose the final slot because the current selection is unverified"))?;
        validate_slots(&[return_slot])?;
        self.start(port, "scan", slots, Some(return_slot), None, request.request_id).await
    }

    pub async fn select(self: Arc<Self>, port: &str, slot: i64, request: SelectRequest) -> Result<Operation> {
        validate_slots(&[slot])?;
        let previous = self.selection(port).await?;
        self.start(port, "select", vec![slot], previous, request.expected_iccid, request.request_id).await
    }

    async fn start(self: Arc<Self>, port: &str, kind: &str, slots: Vec<i64>, return_slot: Option<i64>, expected: Option<String>, request_id: Option<String>) -> Result<Operation> {
        self.inventory(port).await?;
        if let Some(request_id) = request_id.as_deref() {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT id FROM msim_operations WHERE com_port = ? AND request_id = ?",
            ).bind(port).bind(request_id).fetch_optional(db::get_pool()?).await?;
            if let Some(id) = existing { return self.operation(&id).await; }
        }
        let permit = self.gate.clone().try_acquire_owned().map_err(|_| anyhow!("busy: another slot operation is running"))?;
        if kind == "select" {
            let expected = expected.as_deref().filter(|iccid| !iccid.is_empty())
                .ok_or_else(|| anyhow!("invalid: expected_iccid is required; read the slot first"))?;
            if !db::get_msim_slots(port).await?.iter().any(|slot| {
                slot.slot == slots[0] && slot.state == "present" && slot.iccid.as_deref() == Some(expected)
            }) {
                bail!("conflict: card changed; refresh the slot");
            }
        }
        let operation = Operation { id: uuid::Uuid::new_v4().to_string(), com_port: port.into(), kind: kind.into(),
            status: "running".into(), stage: "queued".into(), current_slot: None, return_slot,
            restore_status: "pending".into(), selection_verified: false, selected_slot: None,
            items: slots.iter().map(|slot| ItemResult { slot: *slot, status: "pending".into(), iccid: None, message: None }).collect(),
            error: None, started_at: chrono::Utc::now().to_rfc3339(), finished_at: None };
        sqlx::query("INSERT INTO msim_operations (id, com_port, request_id, status, snapshot) VALUES (?, ?, ?, 'running', ?)")
            .bind(&operation.id).bind(port).bind(request_id).bind(serde_json::to_string(&operation)?)
            .execute(db::get_pool()?).await.map_err(|error| anyhow!("busy: {}", error))?;
        let cancel = Arc::new(AtomicBool::new(false));
        self.control.lock().await.insert(operation.id.clone(), cancel.clone());
        let task_operation = operation.clone();
        tokio::spawn(async move {
            let _permit = permit;
            self.run(task_operation, expected, cancel).await;
        });
        Ok(operation)
    }

    async fn read_identity(&self, modem: &Modem, operation: &mut Operation, slot: i64) -> Result<Option<(String, Option<String>)>> {
        let restoring = operation.stage == "restoring";
        if !restoring { operation.stage = "selecting".into(); }
        operation.current_slot = Some(slot);
        self.publish(operation).await?;
        let reply = tokio::time::timeout(Duration::from_secs(8), modem.send_command(&format!("ath{}\r\n", slot))).await??;
        if !reply.lines().any(|line| line.trim() == "OK") && !confirmed_empty(&reply) {
            bail!("Slot {} did not acknowledge selection: {}", slot, reply.trim());
        }
        db::set_port_msim_slot(&operation.com_port, slot).await?;
        if !restoring { operation.stage = "reading_identity".into(); }
        self.publish(operation).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let started = tokio::time::Instant::now();
        let mut previous = None;
        let mut empty_samples = 0;
        while started.elapsed() < Duration::from_secs(25) {
            let status = tokio::time::timeout(Duration::from_secs(4), modem.send_command("AT+CPIN?\r\n")).await??;
            if confirmed_empty(&status) {
                previous = None;
                empty_samples += 1;
            } else {
                empty_samples = 0;
                if status.contains("SIM PIN") || status.contains("SIM PUK") { bail!("SIM requires PIN or PUK"); }
                if sim_ready(&status) {
                    let iccid = tokio::time::timeout(Duration::from_secs(4), modem.get_sim_iccid()).await??;
                    if let Some(iccid) = iccid.filter(|iccid| !iccid.trim().is_empty()) {
                        if previous.as_deref() == Some(iccid.as_str()) {
                            let imsi = tokio::time::timeout(Duration::from_secs(4), modem.get_sim_imsi()).await.ok().and_then(Result::ok).flatten();
                            let card = db::SimCard::find_or_create_with_phone(&iccid, imsi.clone(), None).await?;
                            db::save_msim_slot(&operation.com_port, slot, "present", Some(&iccid), imsi.as_deref(), None).await?;
                            self.manager.update_sim_cache(card).await;
                            return Ok(Some((iccid, imsi)));
                        }
                        previous = Some(iccid);
                    } else { previous = None; }
                } else { previous = None; }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        if empty_samples >= 3 {
            db::save_msim_slot(&operation.com_port, slot, "empty", None, None, None).await?;
            return Ok(None);
        }
        bail!("Timed out verifying slot {}; cached identity was not replaced", slot)
    }

    async fn run(self: Arc<Self>, mut operation: Operation, expected: Option<String>, cancel: Arc<AtomicBool>) {
        let port = operation.com_port.clone();
        self.verified.lock().await.remove(&port);
        let opened = self.manager.open_msim_modem(&port).await;
        let mut error = None;
        let mut selection = None;
        let mut resume_without_selection = false;
        if let Ok((modem, guard)) = opened {
            let type_result = tokio::time::timeout(Duration::from_secs(6), modem.send_command("ath?\r\n")).await;
            let is_msim = matches!(type_result, Ok(Ok(ref reply)) if MachineType::from_type_reply(reply) == MachineType::Msim);
            if is_msim {
                let _ = db::upsert_port_machine_type(&port, "msim", Some("MSIM")).await;
                for index in 0..operation.items.len() {
                    if cancel.load(Ordering::SeqCst) { break; }
                    let slot = operation.items[index].slot;
                    operation.items[index].status = "running".into();
                    let result = self.read_identity(&modem, &mut operation, slot).await;
                    let mut lost_transport = result.as_ref().err().is_some_and(transport_failed);
                    let saved = async {
                        match result {
                            Ok(Some((iccid, imsi))) => {
                                operation.stage = "reading_number".into();
                                self.publish(&operation).await?;
                                let phone = match tokio::time::timeout(Duration::from_secs(5), modem.get_phone_number()).await {
                                    Ok(Ok(phone)) => phone,
                                    Ok(Err(failure)) => {
                                        let failure = anyhow!(failure);
                                        if transport_failed(&failure) { return Err(failure); }
                                        None
                                    }
                                    Err(_) => None,
                                };
                                let card = db::SimCard::find_or_create_with_phone(&iccid, imsi.clone(), phone.clone()).await?;
                                self.manager.update_sim_cache(card).await;
                                operation.items[index].iccid = Some(iccid.clone());
                                if operation.kind == "select" && expected.as_deref().is_some_and(|expected| expected != iccid) {
                                    bail!("Card changed; refresh before selecting it");
                                }
                                selection = Some((slot, Some(iccid)));
                                Ok(())
                            }
                            Ok(None) => {
                                if operation.kind == "select" { bail!("Selected slot is empty"); }
                                operation.items[index].message = Some("empty".into());
                                Ok(())
                            }
                            Err(failure) => {
                                db::save_msim_slot(&port, slot, "unreadable", None, None, Some(&failure.to_string())).await?;
                                Err(failure)
                            }
                        }
                    }.await;
                    match saved {
                        Ok(()) => operation.items[index].status = "ok".into(),
                        Err(failure) => {
                            lost_transport |= transport_failed(&failure);
                            operation.items[index].status = "failed".into();
                            operation.items[index].message = Some(failure.to_string());
                            error = Some(failure.to_string());
                        }
                    }
                    let _ = self.publish(&operation).await;
                    if lost_transport { break; }
                }
                let restore = operation.kind == "scan" || error.is_some() || cancel.load(Ordering::SeqCst);
                if restore {
                    selection = None;
                    if let Some(slot) = operation.return_slot {
                        operation.stage = "restoring".into();
                        let _ = self.publish(&operation).await;
                        match self.read_identity(&modem, &mut operation, slot).await {
                            Ok(identity) => {
                                operation.restore_status = "ok".into();
                                selection = Some((slot, identity.map(|(iccid, _)| iccid)));
                            }
                            Err(failure) => {
                                operation.restore_status = "failed".into();
                                error = Some(format!("Restore failed: {}", failure));
                            }
                        }
                    } else {
                        operation.restore_status = "unknown".into();
                    }
                } else { operation.restore_status = "not_needed".into(); }
            } else {
                resume_without_selection = true;
                error = Some("Fresh ath? reply did not confirm MSIM; no slot command was sent".into());
            }
            modem.shutdown().await;
            drop(modem);
            drop(guard);
        } else if let Err(failure) = opened { error = Some(failure.to_string()); }
        for item in &mut operation.items {
            if item.status == "pending" { item.status = "skipped".into(); }
        }
        let resume = selection.is_some() || resume_without_selection;
        if let Some((slot, iccid)) = selection {
            self.verified.lock().await.insert(port.clone(), VerifiedSelection { slot, iccid, modem: None });
            operation.selection_verified = true;
            operation.selected_slot = Some(slot);
        }
        if operation.restore_status == "pending" { operation.restore_status = "unknown".into(); }
        if !resume {
            let detail = error.get_or_insert_with(|| "Final selection could not be verified".into());
            detail.push_str("; messaging remains paused on this port. Read or select a slot to recover.");
        }
        operation.status = if operation.restore_status == "failed" || error.is_some() { "failed" }
            else if cancel.load(Ordering::SeqCst) { "cancelled" } else { "done" }.into();
        operation.stage = operation.status.clone();
        operation.error = error;
        operation.finished_at = Some(chrono::Utc::now().to_rfc3339());
        if let Err(failure) = self.publish(&operation).await { log::error!("Multi-SIM operation save failed: {}", failure); }
        self.control.lock().await.remove(&operation.id);
        if resume {
            if let Err(failure) = self.manager.clone().restart_port(&port, self.sse.clone(), self.transcribe.clone()).await {
                log::error!("Multi-SIM port resume failed: {}", failure);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msim_slot_validation() {
        assert!(validate_slots(&[1, 2, 3, 4]).is_ok());
        for slots in [vec![], vec![0], vec![5], vec![1, 1]] { assert!(validate_slots(&slots).is_err()); }
    }

    #[test]
    fn msim_empty_requires_explicit_absence() {
        assert!(confirmed_empty("\r\n+CME ERROR: 10\r\n"));
        for reply in ["+CME ERROR: 13", "+CME ERROR: 50", "+CME ERROR: 100", "ERROR", ""] {
            assert!(!confirmed_empty(reply));
        }
    }

    #[test]
    fn msim_readiness_does_not_match_not_ready_or_unrelated_text() {
        assert!(sim_ready("\r\n+CPIN: READY\r\nOK\r\n"));
        for reply in ["+CPIN: NOT READY", "READY", "+CPIN: SIM PIN", "NOT READY", ""] {
            assert!(!sim_ready(reply));
        }
    }

    #[test]
    fn msim_transport_loss_stops_further_slots() {
        assert!(transport_failed(&std::io::Error::from(std::io::ErrorKind::NotConnected).into()));
        assert!(transport_failed(&std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()));
        assert!(!transport_failed(&anyhow!("SIM requires PIN or PUK")));
        assert!(!transport_failed(&anyhow!("Timed out verifying identity")));
        assert!(!transport_failed(&std::io::Error::from(std::io::ErrorKind::InvalidData).into()));
    }
}