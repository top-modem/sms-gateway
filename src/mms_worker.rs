/// Background worker that processes queued MMS send jobs.
///
/// Sending an MMS involves uploading an image to modem RAM (AT+QFUPL, up to tens of
/// seconds) and waiting for the modem's async `+QMMSEND: <err>,<httprsp>` URC (up to
/// `mms_send_timeout_secs`), so jobs are processed one at a time per poll tick rather
/// than blocking the HTTP request that created them (see POST /api/mms).
use crate::db::{MmsAttachment, MmsInboxNotification, MmsInboxPart, MmsMessage, MmsProfile};
use crate::ModemManagerRef;
use log::{error, info, warn};
use std::time::Duration;

/// Start the MMS worker background task.
///
/// `send_timeout_secs` is reused as both the outgoing send timeout and the
/// inbox content-fetch timeout (AT+QHTTPGET/AT+QHTTPREADFILE) -- both talk to
/// the same MMSC/APN over similar-latency HTTP round-trips, so a single
/// configured value is sufficient for now.
pub fn start_mms_worker(modem_manager: ModemManagerRef, send_timeout_secs: u64) {
    tokio::spawn(async move {
        mms_worker_loop(modem_manager, send_timeout_secs).await;
    });
}

async fn mms_worker_loop(modem_manager: ModemManagerRef, send_timeout_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        if !crate::service_control::is_running() {
            continue;
        }

        if let Err(e) = process_queue(&modem_manager, send_timeout_secs).await {
            error!("[MMS Worker] Error processing queue: {}", e);
        }

        if let Err(e) = process_inbox_queue(&modem_manager, send_timeout_secs).await {
            error!("[MMS Worker] Error processing inbox fetch queue: {}", e);
        }
    }
}

async fn process_queue(
    modem_manager: &ModemManagerRef,
    send_timeout_secs: u64,
) -> anyhow::Result<()> {
    // Process a small batch per tick so one slow send doesn't starve the poll loop for long.
    let jobs = MmsMessage::get_queued(5).await?;
    if jobs.is_empty() {
        return Ok(());
    }

    for job in jobs {
        info!(
            "[MMS Worker] Processing job {} (sim={}, to={})",
            job.id, job.sim_id, job.to_number
        );

        if let Err(e) = MmsMessage::mark_sending(&job.id).await {
            error!(
                "[MMS Worker] Failed to mark job {} as sending: {}",
                job.id, e
            );
            continue;
        }

        let modem = match modem_manager.get_modem(&job.sim_id).await {
            Some(m) => m,
            None => {
                warn!(
                    "[MMS Worker] Job {}: modem for SIM {} not available",
                    job.id, job.sim_id
                );
                let _ = MmsMessage::mark_result(
                    &job.id,
                    "failed",
                    None,
                    None,
                    Some(&format!("Modem for SIM {} not available", job.sim_id)),
                )
                .await;
                continue;
            }
        };

        // Apply the per-SIM MMS profile (APN/MMSC/proxy) before every send — cheap AT
        // round-trips, and avoids stale config if the operator changed carriers.
        let attachment_upload_mode = match MmsProfile::get(&job.sim_id).await {
            Ok(Some(profile)) => profile
                .mms_send_mode
                .clone()
                .unwrap_or_else(|| "modem_direct_attachment_upload".to_string()),
            Ok(None) => "modem_direct_attachment_upload".to_string(),
            Err(e) => {
                error!(
                    "[MMS Worker] Job {}: failed to load MMS profile: {}",
                    job.id, e
                );
                "modem_direct_attachment_upload".to_string()
            }
        };

        let mut mms_profile_for_restore: Option<(String, String, String, u16)> = None;

        match MmsProfile::get(&job.sim_id).await {
            Ok(Some(profile))
                if profile.mms_apn.is_some()
                    && profile.mms_mmsc.is_some()
                    && profile.mms_proxy_host.is_some()
                    && profile.mms_proxy_port.is_some() =>
            {
                let apn = profile.mms_apn.unwrap();
                let mmsc = profile.mms_mmsc.unwrap();
                let proxy_host = profile.mms_proxy_host.unwrap();
                let proxy_port = profile.mms_proxy_port.unwrap() as u16;
                mms_profile_for_restore = Some((
                    apn.clone(),
                    mmsc.clone(),
                    proxy_host.clone(),
                    proxy_port,
                ));
                if let Err(e) = modem
                    .configure_mms_profile(&apn, &mmsc, &proxy_host, proxy_port)
                    .await
                {
                    error!(
                        "[MMS Worker] Job {}: failed to apply MMS profile: {}",
                        job.id, e
                    );
                    let _ = MmsMessage::mark_result(
                        &job.id,
                        "failed",
                        None,
                        None,
                        Some(&format!("Failed to apply MMS profile: {}", e)),
                    )
                    .await;
                    continue;
                }
            }
            Ok(_) => {
                warn!(
                    "[MMS Worker] Job {}: no MMS profile configured for SIM {} (set via PUT /api/sim-cards/{{id}}/mms-profile); attempting send with modem defaults",
                    job.id, job.sim_id
                );
            }
            Err(e) => {
                error!(
                    "[MMS Worker] Job {}: failed to load MMS profile: {}",
                    job.id, e
                );
            }
        }

        let attachments = match MmsAttachment::fetch_all_with_data(&job.id).await {
            Ok(a) => a,
            Err(e) => {
                error!(
                    "[MMS Worker] Job {}: failed to load attachments: {}",
                    job.id, e
                );
                let _ = MmsMessage::mark_result(
                    &job.id,
                    "failed",
                    None,
                    None,
                    Some(&format!("Failed to load attachments: {}", e)),
                )
                .await;
                continue;
            }
        };

        match modem
            .send_mms(
                &job.to_number,
                job.subject.as_deref(),
                &attachments,
                &attachment_upload_mode,
                send_timeout_secs,
                mms_profile_for_restore.as_ref().map(|(apn, mmsc, proxy, port)| {
                    (apn.as_str(), mmsc.as_str(), proxy.as_str(), *port)
                }),
            )
            .await
        {
            Ok((err_code, http_code)) => {
                if err_code == 0 {
                    info!(
                        "[MMS Worker] Job {} sent successfully (http={})",
                        job.id, http_code
                    );
                    let _ = MmsMessage::mark_result(
                        &job.id,
                        "sent",
                        Some(err_code),
                        Some(http_code),
                        None,
                    )
                    .await;
                } else {
                    warn!(
                        "[MMS Worker] Job {} rejected by modem/MMSC: err={}, http={}",
                        job.id, err_code, http_code
                    );
                    let _ = MmsMessage::mark_result(
                        &job.id,
                        "failed",
                        Some(err_code),
                        Some(http_code),
                        Some(&format!("Quectel err={}, http={}", err_code, http_code)),
                    )
                    .await;
                }
            }
            Err(e) => {
                let is_timeout = e.to_string().contains("Timed out");
                error!("[MMS Worker] Job {} send failed: {}", job.id, e);
                let _ = MmsMessage::mark_result(
                    &job.id,
                    if is_timeout { "timeout" } else { "failed" },
                    None,
                    None,
                    Some(&e.to_string()),
                )
                .await;
            }
        }
    }

    Ok(())
}

/// Retry backoff schedule (seconds) for MMS content-fetch failures, matching
/// asterisk-chan-modemmanager's `mms_txn` retry policy: a quick first retry,
/// then backing off. `retry_count` (0-based, before this attempt) indexes into
/// this array; once exhausted the row stays "failed" and is no longer retried.
const RETRY_BACKOFF_SECS: [i64; 3] = [30, 120, 600];

/// Fetch and decode the content for MMS notifications that are due (newly
/// notified, or failed and past their retry backoff). Reuses the AT-port-only
/// `AT+QHTTPxxx` fetch mechanism (`Modem::fetch_mms_content`) -- see
/// `mms_retrieve.rs` for the M-Retrieve.conf decoder.
async fn process_inbox_queue(
    modem_manager: &ModemManagerRef,
    fetch_timeout_secs: u64,
) -> anyhow::Result<()> {
    if let Ok(n) = MmsInboxNotification::expire_overdue().await {
        if n > 0 {
            info!("[MMS Worker] Expired {} overdue MMS notification(s)", n);
        }
    }

    let items = MmsInboxNotification::get_fetchable(5).await?;
    if items.is_empty() {
        return Ok(());
    }

    for item in items {
        let url = match item.content_location.as_deref() {
            Some(u) => u,
            None => continue, // excluded by the query already, but be defensive
        };

        info!(
            "[MMS Worker] Fetching MMS content {} (sim={}, txn={}, attempt={})",
            item.id,
            item.sim_id,
            item.transaction_id,
            item.retry_count + 1
        );

        if let Err(e) = MmsInboxNotification::mark_fetching(&item.id).await {
            error!("[MMS Worker] Failed to mark {} as fetching: {}", item.id, e);
            continue;
        }

        let modem = match modem_manager.get_modem(&item.sim_id).await {
            Some(m) => m,
            None => {
                warn!(
                    "[MMS Worker] {}: modem for SIM {} not available",
                    item.id, item.sim_id
                );
                fail_and_schedule_retry(&item, "Modem not available").await;
                continue;
            }
        };

        // The generic HTTP module needs the carrier's MMS proxy explicitly (it does not
        // share AT+QMMSCFG's proxy setting) since MMS APNs are typically only routable
        // through that proxy, not the open internet -- see Modem::fetch_mms_content.
        let (proxy_host, proxy_port) = match MmsProfile::get(&item.sim_id).await {
            Ok(Some(profile)) => (
                profile.mms_proxy_host,
                profile.mms_proxy_port.map(|p| p as u16),
            ),
            Ok(None) => (None, None),
            Err(e) => {
                warn!(
                    "[MMS Worker] {}: failed to load MMS profile: {}",
                    item.id, e
                );
                (None, None)
            }
        };

        let bytes = match modem
            .fetch_mms_content(url, proxy_host.as_deref(), proxy_port, fetch_timeout_secs)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                error!("[MMS Worker] {}: content fetch failed: {}", item.id, e);
                fail_and_schedule_retry(&item, &e.to_string()).await;
                continue;
            }
        };

        let conf = match crate::mms_retrieve::decode_retrieve_conf(&bytes) {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "[MMS Worker] {}: failed to decode M-Retrieve.conf: {}",
                    item.id, e
                );
                fail_and_schedule_retry(&item, &format!("Decode failed: {}", e)).await;
                continue;
            }
        };

        if let Err(e) = MmsInboxPart::delete_all_for_inbox(&item.id).await {
            warn!(
                "[MMS Worker] {}: failed to clear stale parts before insert: {}",
                item.id, e
            );
        }

        let mut store_err = None;
        for (idx, part) in conf.parts.iter().enumerate() {
            let filename = part.filename.clone().unwrap_or_else(|| {
                let ext = mime_guess::get_mime_extensions_str(&part.content_type)
                    .and_then(|exts| exts.first())
                    .copied()
                    .unwrap_or("bin");
                format!("part{}.{}", idx, ext)
            });
            if let Err(e) =
                MmsInboxPart::insert(&item.id, &part.content_type, Some(&filename), &part.data)
                    .await
            {
                store_err = Some(e);
                break;
            }
        }

        match store_err {
            None => match MmsInboxNotification::mark_fetched(
                &item.id,
                conf.subject.as_deref(),
                conf.from.as_deref(),
            )
            .await
            {
                Ok(()) => info!(
                    "[MMS Worker] {} fetched successfully: {} part(s), subject={:?}",
                    item.id,
                    conf.parts.len(),
                    conf.subject
                ),
                Err(e) => error!("[MMS Worker] {}: failed to mark fetched: {}", item.id, e),
            },
            Some(e) => {
                error!("[MMS Worker] {}: failed to store MMS parts: {}", item.id, e);
                fail_and_schedule_retry(&item, &format!("Failed to store parts: {}", e)).await;
            }
        }
    }

    Ok(())
}

/// Record a failed fetch attempt and schedule the next retry per
/// `RETRY_BACKOFF_SECS`, or give up (leave `next_retry_at` unset) once
/// attempts are exhausted.
async fn fail_and_schedule_retry(item: &MmsInboxNotification, error_message: &str) {
    let next_retry_at = RETRY_BACKOFF_SECS
        .get(item.retry_count as usize)
        .map(|&secs| chrono::Utc::now().naive_utc() + chrono::Duration::seconds(secs));
    if next_retry_at.is_none() {
        warn!(
            "[MMS Worker] {}: retry attempts exhausted ({} tries), giving up",
            item.id,
            item.retry_count + 1
        );
    }
    if let Err(e) =
        MmsInboxNotification::mark_fetch_failed(&item.id, error_message, next_retry_at).await
    {
        error!(
            "[MMS Worker] {}: failed to record fetch failure: {}",
            item.id, e
        );
    }
}
