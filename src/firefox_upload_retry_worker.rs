/// Background worker that retries failed 火狐狸 SMS uploads
/// This worker:
/// 1. Polls the retry queue every 10 seconds
/// 2. Retries uploads that are ready (next_retry_at <= now)
/// 3. Uses exponential backoff (5s, 10s, 20s, 40s, 80s, 160s max)
/// 4. Logs dead-letter items (exceeded max retries)

use crate::firefox_api;
use crate::firefox_upload_retry::FirefoxUploadRetryItem;
use crate::db;
use log::{error, info, warn};
use std::time::Duration;

/// Start the retry worker background task
pub fn start_retry_worker() {
    tokio::spawn(async move {
        retry_worker_loop().await;
    });
}

async fn retry_worker_loop() {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        if let Err(e) = process_retry_queue().await {
            error!("[Firefox Upload Retry] Error processing queue: {}", e);
        }
    }
}

async fn process_retry_queue() -> anyhow::Result<()> {
    let pool = db::get_pool()?;
    // Get items ready for retry (limit 10 per batch)
    let items = FirefoxUploadRetryItem::get_ready_for_retry(pool, 10).await?;

    if items.is_empty() {
        return Ok(());
    }

    info!("[Firefox Upload Retry] Processing {} items from retry queue", items.len());

    // Get API key
    let api_key = match crate::db::AppSetting::get("firefox_api_key").await {
        Ok(Some(key)) => key,
        _ => {
            warn!("[Firefox Upload Retry] API key not configured, skipping retry batch");
            return Ok(());
        }
    };

    // Build HTTP client
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!("[Firefox Upload Retry] Failed to build HTTP client: {}", e);
            return Ok(());
        }
    };

    // Process each item
    for item in items {
        match firefox_api::upload_sms(
            &client,
            &api_key,
            &item.country_id,
            &item.phone_number,
            &item.message,
        )
        .await
        {
            Ok(resp) if resp.code == "1" => {
                // Success! Remove from queue
                info!(
                    "[Firefox Upload Retry] Upload succeeded (SMS ID: {}, retry attempt: {})",
                    item.sms_id, item.retry_count + 1
                );

                if let Err(e) = FirefoxUploadRetryItem::mark_success(pool, &item.id).await {
                    error!(
                        "[Firefox Upload Retry] Failed to mark item {} as success: {}",
                        item.id, e
                    );
                }

                // Also update the SMS record to mark as uploaded to platform
                let _ = sqlx::query(
                    r#"
                    UPDATE sms
                    SET uploaded_to_platform = 1,
                        platform_uploaded_at = CURRENT_TIMESTAMP,
                        platform_response = ?
                    WHERE id = ?
                    "#,
                )
                .bind(serde_json::to_string(&resp).unwrap_or_default())
                .bind(item.sms_id)
                .execute(pool)
                .await;
            }
            Ok(resp) => {
                // Platform rejection (code != "1")
                let error_msg = format!(
                    "Platform rejected: code={}, data={:?}",
                    resp.code, resp.data
                );

                if item.retry_count + 1 >= item.max_retries {
                    // Dead letter
                    warn!(
                        "[Firefox Upload Retry] Item {} exceeded max retries ({}): {}",
                        item.id, item.max_retries, error_msg
                    );
                } else {
                    // Retry
                    info!(
                        "[Firefox Upload Retry] Requeueing item {} (attempt {}/{}): {}",
                        item.id,
                        item.retry_count + 1,
                        item.max_retries,
                        error_msg
                    );
                }

                if let Err(e) = FirefoxUploadRetryItem::update_after_retry(
                    pool,
                    &item.id,
                    item.retry_count,
                    error_msg,
                    Some(resp.code),
                )
                .await
                {
                    error!(
                        "[Firefox Upload Retry] Failed to update item {}: {}",
                        item.id, e
                    );
                }
            }
            Err(e) => {
                // Network/HTTP error
                let error_msg = format!("HTTP request failed: {}", e);

                if item.retry_count + 1 >= item.max_retries {
                    // Dead letter
                    warn!(
                        "[Firefox Upload Retry] Item {} exceeded max retries after {} attempts: {}",
                        item.id, item.max_retries, error_msg
                    );
                } else {
                    // Retry
                    info!(
                        "[Firefox Upload Retry] Requeueing item {} after transient error (attempt {}/{})",
                        item.id, item.retry_count + 1, item.max_retries
                    );
                }

                if let Err(e) = FirefoxUploadRetryItem::update_after_retry(
                    pool,
                    &item.id,
                    item.retry_count,
                    error_msg,
                    None,
                )
                .await
                {
                    error!(
                        "[Firefox Upload Retry] Failed to update item {}: {}",
                        item.id, e
                    );
                }
            }
        }

        // Rate limiting: small delay between retries
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}
