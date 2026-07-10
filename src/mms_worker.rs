/// Background worker that processes queued MMS send jobs.
///
/// Sending an MMS involves uploading an image to modem RAM (AT+QFUPL, up to tens of
/// seconds) and waiting for the modem's async `+QMMSEND: <err>,<httprsp>` URC (up to
/// `mms_send_timeout_secs`), so jobs are processed one at a time per poll tick rather
/// than blocking the HTTP request that created them (see POST /api/mms).
use crate::db::{MmsAttachment, MmsMessage, MmsProfile};
use crate::ModemManagerRef;
use log::{error, info, warn};
use std::time::Duration;

/// Start the MMS worker background task.
pub fn start_mms_worker(
    modem_manager: ModemManagerRef,
    send_timeout_secs: u64,
) {
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
    }
}

async fn process_queue(modem_manager: &ModemManagerRef, send_timeout_secs: u64) -> anyhow::Result<()> {
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
            error!("[MMS Worker] Failed to mark job {} as sending: {}", job.id, e);
            continue;
        }

        let modem = match modem_manager.get_modem(&job.sim_id).await {
            Some(m) => m,
            None => {
                warn!("[MMS Worker] Job {}: modem for SIM {} not available", job.id, job.sim_id);
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
                if let Err(e) = modem
                    .configure_mms_profile(&apn, &mmsc, &proxy_host, proxy_port)
                    .await
                {
                    error!("[MMS Worker] Job {}: failed to apply MMS profile: {}", job.id, e);
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
                error!("[MMS Worker] Job {}: failed to load MMS profile: {}", job.id, e);
            }
        }

        let attachments = match MmsAttachment::fetch_all_with_data(&job.id).await {
            Ok(a) => a,
            Err(e) => {
                error!("[MMS Worker] Job {}: failed to load attachments: {}", job.id, e);
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
            .send_mms(&job.to_number, job.subject.as_deref(), &attachments, send_timeout_secs)
            .await
        {
            Ok((err_code, http_code)) => {
                if err_code == 0 {
                    info!("[MMS Worker] Job {} sent successfully (http={})", job.id, http_code);
                    let _ = MmsMessage::mark_result(&job.id, "sent", Some(err_code), Some(http_code), None).await;
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
