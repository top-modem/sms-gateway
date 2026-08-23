//! WAP Push / MMS notification (`m-notification-ind`) decoding.
//!
//! Incoming MMS are announced by the network as a binary SMS carrying a WSP
//! "Push" PDU whose body is a WAP-209 (OMA MMS Encapsulation) encoded
//! `m-notification-ind`. The Quectel EC20/EC25 modules used in this project have
//! **no** dedicated AT command to detect or auto-fetch this (unlike sending,
//! where `AT+QMMSEND` lets the modem's own baseband do the HTTP work) — so we
//! decode the notification ourselves from the raw PDU bytes captured by
//! `decode::parse_pdu_sms`.
//!
//! This is a best-effort decoder written against the public WAP-230-WSP /
//! WAP-209-MMSENCAPSULATION specs, without a captured real-world sample to
//! validate against yet. It is intentionally defensive: any bounds/format
//! problem aborts decoding of that message (never panics) and the raw bytes are
//! always persisted to `mms_inbox.notification_raw`, so nothing is lost and the
//! decoder can be corrected later — mirroring how the send-side AT command
//! syntax was written from documentation first and adjusted after hardware
//! testing.
//!
//! Reference (architecture, not code): `koreapyj/asterisk-chan-modemmanager`'s
//! `mms_codec_decode_notification`, adapted here to our AT-command-only reality.

use anyhow::{Context, Result};
use chrono::NaiveDateTime;

use crate::db::MmsInboxNotification;
use crate::decode::MmsNotificationCandidate;

// WSP well-known header field codes (high bit set) relevant to m-notification-ind.
const HDR_FROM: u8 = 0x89;
const HDR_MESSAGE_CLASS: u8 = 0x8A;
const HDR_MESSAGE_ID: u8 = 0x8B;
const HDR_MESSAGE_TYPE: u8 = 0x8C;
const HDR_MESSAGE_SIZE: u8 = 0x8E;
const HDR_EXPIRY: u8 = 0x88;
const HDR_DATE: u8 = 0x85;
const HDR_STATUS: u8 = 0x95;
const HDR_TO: u8 = 0x97;
const HDR_TRANSACTION_ID: u8 = 0x98;
const HDR_CONTENT_LOCATION: u8 = 0x83;

const MMS_MSG_TYPE_NOTIFICATION_IND: u8 = 0x82;
const MMS_MSG_TYPE_DELIVERY_IND: u8 = 0x86;

// Additional well-known MMS header fields we don't need the *value* of, but
// which real-world notifications commonly include (e.g. every notification
// we've seen on real hardware carries X-Mms-MMS-Version). These must still be
// skipped correctly -- previously any unrecognized field aborted parsing of
// the remaining headers entirely, which silently dropped transaction-id /
// content-location / message-size whenever they happened to appear *after*
// one of these in the PDU.

#[derive(Debug, Default)]
struct MmsNotification {
    transaction_id: String,
    content_location: Option<String>,
    message_size: Option<i64>,
    message_class: Option<String>,
    expiry: Option<NaiveDateTime>,
}

#[derive(Debug)]
enum MmsPush {
    Notification(MmsNotification),
    DeliveryReport(MmsDeliveryReport),
    Unsupported(u8),
}

#[derive(Debug, Default)]
struct MmsDeliveryReport {
    message_id: String,
    recipient: Option<String>,
    status: Option<u8>,
    date: Option<NaiveDateTime>,
}

/// Entry point: called for every WAP-push candidate found while reading SMS.
/// Persists (or refreshes, if the carrier re-sent the same notification) a row
/// in `mms_inbox`. Errors are logged by the caller and never abort the SMS read
/// cycle.
pub async fn handle_notification_candidate(
    sim_id: &str,
    candidate: MmsNotificationCandidate,
) -> Result<()> {
    let raw = candidate.raw.as_slice();

    match decode_wap_push(raw) {
        Ok(MmsPush::Notification(notif)) => {
            log::info!(
                "[{}] MMS通知已解析: txn={} content_location={:?} size={:?}",
                sim_id,
                notif.transaction_id,
                notif.content_location,
                notif.message_size
            );
            MmsInboxNotification::insert_or_update(
                sim_id,
                &notif.transaction_id,
                &candidate.sender,
                notif.content_location.as_deref(),
                notif.message_size,
                notif.expiry,
                notif.message_class.as_deref(),
                raw,
                candidate.timestamp - chrono::Duration::hours(8),
            )
            .await
            .context("failed to persist MMS notification")?;
        }
        Ok(MmsPush::DeliveryReport(report)) => {
            log::info!(
                "[{}] MMS delivery report: message_id={} recipient={:?} status={:?}",
                sim_id,
                report.message_id,
                report.recipient,
                report.status
            );
            let received_at = candidate.timestamp - chrono::Duration::hours(8);
            MmsInboxNotification::insert_delivery_report(
                sim_id,
                &report.message_id,
                &candidate.sender,
                report.recipient.as_deref(),
                report.status,
                report.date.unwrap_or(received_at),
                raw,
            )
            .await
            .context("failed to persist MMS delivery report")?;
        }
        Ok(MmsPush::Unsupported(message_type)) => {
            log::info!(
                "[{}] Ignoring unsupported MMS WAP-push message type 0x{:02X} from {}",
                sim_id,
                message_type,
                candidate.sender
            );
        }
        Err(e) => {
            // Best-effort decode failed (protocol details unconfirmed against
            // real hardware yet) -- still persist the raw bytes under a
            // synthetic transaction id so nothing is silently dropped and this
            // can be reprocessed later once the decoder is fixed.
            log::warn!(
                "[{}] 无法解析MMS WAP-push通知，已保存原始字节以便后续修复解码: {}",
                sim_id,
                e
            );
            let fallback_txn = format!("undecoded-{:016x}", fnv1a(raw));
            MmsInboxNotification::insert_or_update(
                sim_id,
                &fallback_txn,
                &candidate.sender,
                None,
                None,
                None,
                None,
                raw,
                candidate.timestamp - chrono::Duration::hours(8),
            )
            .await
            .context("failed to persist raw MMS WAP-push payload")?;
        }
    }
    Ok(())
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Decodes a raw WSP "Push" PDU down to the `m-notification-ind` fields we need.
/// Layout (WAP-230-WSP §8): `[TID][PDU-Type=0x06][Headers-Len][Headers...][Body]`
fn decode_wap_push(data: &[u8]) -> Result<MmsPush> {
    anyhow::ensure!(
        data.len() > 2,
        "WAP push payload too short ({} bytes)",
        data.len()
    );

    let mut pos = 1usize; // byte 0 = WSP transaction id, unused for Push
    let pdu_type = *data.get(pos).context("missing PDU type")?;
    pos += 1;
    anyhow::ensure!(
        pdu_type == 0x06,
        "not a WSP Push PDU (type=0x{:02X})",
        pdu_type
    );

    // Headers-Len: Uintvar-integer. We don't need to individually parse the
    // WSP-level headers (mandatory Content-Type + optional charset) — the
    // length field alone tells us exactly where the body starts.
    let (headers_len, len_bytes) = read_uintvar(data.get(pos..).context("missing headers-len")?)?;
    pos += len_bytes;
    let body_start = pos + headers_len as usize;
    anyhow::ensure!(
        body_start <= data.len(),
        "WSP push header length out of bounds"
    );

    let body = &data[body_start..];
    anyhow::ensure!(
        body.len() >= 2 && body[0] == HDR_MESSAGE_TYPE,
        "MMS body does not start with X-Mms-Message-Type"
    );

    match body[1] {
        MMS_MSG_TYPE_NOTIFICATION_IND => {
            decode_mms_notification_ind(body).map(MmsPush::Notification)
        }
        MMS_MSG_TYPE_DELIVERY_IND => decode_mms_delivery_ind(body).map(MmsPush::DeliveryReport),
        message_type => Ok(MmsPush::Unsupported(message_type)),
    }
}

fn decode_mms_delivery_ind(data: &[u8]) -> Result<MmsDeliveryReport> {
    let mut report = MmsDeliveryReport::default();
    let mut pos = 0usize;

    while pos < data.len() {
        let field = data[pos];
        pos += 1;
        if pos >= data.len() {
            break;
        }

        let step = match field {
            HDR_MESSAGE_TYPE => Some(1),
            HDR_MESSAGE_ID => read_text_string(&data[pos..]).ok().map(|(value, n)| {
                report.message_id = value;
                n
            }),
            HDR_TO => read_text_string(&data[pos..]).ok().map(|(value, n)| {
                report.recipient = Some(value);
                n
            }),
            HDR_DATE => read_long_integer(&data[pos..]).ok().map(|(value, n)| {
                report.date =
                    chrono::DateTime::from_timestamp(value as i64, 0).map(|date| date.naive_utc());
                n
            }),
            HDR_STATUS => {
                report.status = Some(data[pos]);
                Some(1)
            }
            _ => skip_unknown_value(&data[pos..]),
        };

        match step {
            Some(n) if pos + n <= data.len() => pos += n,
            _ => break,
        }
    }

    anyhow::ensure!(
        !report.message_id.is_empty(),
        "delivery report missing Message-ID"
    );
    Ok(report)
}

/// Decodes WAP-209 encoded MMS headers with no overall length prefix (runs to
/// end of the WSP push body). Stops gracefully (keeping whatever was already
/// parsed) on the first unrecognized header, since header order/coverage isn't
/// fully guaranteed and we'd rather return a partial result than nothing.
fn decode_mms_notification_ind(data: &[u8]) -> Result<MmsNotification> {
    let mut notif = MmsNotification::default();
    let mut pos = 0usize;
    let mut saw_notification_ind = false;

    while pos < data.len() {
        let field = data[pos];
        if field & 0x80 == 0 {
            // Application-header: a NUL-terminated field name we don't
            // recognize, followed by a value of the same generic shapes as
            // well-known headers. Skip both rather than aborting, so a
            // trailing/unexpected header doesn't hide fields we already
            // parsed earlier in the PDU.
            let name_end = match data[pos..].iter().position(|&b| b == 0) {
                Some(idx) => pos + idx + 1,
                None => {
                    log::debug!(
                        "遇到非知名的MMS头字段名(0x{:02X})且未找到结尾，停止解析剩余头部",
                        field
                    );
                    break;
                }
            };
            match skip_unknown_value(&data[name_end..]) {
                Some(n) if name_end + n <= data.len() => {
                    pos = name_end + n;
                    continue;
                }
                _ => {
                    log::debug!(
                        "遇到非知名的MMS头字段(0x{:02X})，值解析失败，停止解析剩余头部",
                        field
                    );
                    break;
                }
            }
        }
        pos += 1;
        if pos >= data.len() {
            break;
        }

        let step = match field {
            HDR_MESSAGE_TYPE => {
                saw_notification_ind = data[pos] == MMS_MSG_TYPE_NOTIFICATION_IND;
                Some(1)
            }
            HDR_TRANSACTION_ID => match read_text_string(&data[pos..]) {
                Ok((s, n)) => {
                    notif.transaction_id = s;
                    Some(n)
                }
                Err(_) => None,
            },
            HDR_MESSAGE_SIZE => match read_long_integer(&data[pos..]) {
                Ok((v, n)) => {
                    notif.message_size = Some(v as i64);
                    Some(n)
                }
                Err(_) => None,
            },
            HDR_CONTENT_LOCATION => match read_text_string(&data[pos..]) {
                Ok((s, n)) => {
                    notif.content_location = Some(s);
                    Some(n)
                }
                Err(_) => None,
            },
            HDR_EXPIRY => decode_expiry(&data[pos..]).map(|(expiry, n)| {
                notif.expiry = expiry;
                n
            }),
            HDR_MESSAGE_CLASS => {
                if data[pos] & 0x80 != 0 {
                    notif.message_class = Some(format!("class-0x{:02X}", data[pos]));
                    Some(1)
                } else {
                    read_text_string(&data[pos..]).ok().map(|(s, n)| {
                        notif.message_class = Some(s);
                        n
                    })
                }
            }
            HDR_FROM => {
                // Skip -- we already have the true SMS sender from the outer PDU
                // and don't need the (often hidden) MMS-layer From header.
                read_value_length(&data[pos..])
                    .ok()
                    .map(|(len, n)| n + len as usize)
            }
            // Any other well-known header (X-Mms-MMS-Version, Priority,
            // Delivery-Report, Sender-Visibility, Reply-Charging, ...): we
            // don't need the value, but we must still skip over it correctly
            // so parsing can continue to find the headers we *do* need,
            // wherever they appear in the PDU.
            _ => skip_unknown_value(&data[pos..]),
        };

        match step {
            Some(n) if pos + n <= data.len() => pos += n,
            _ => {
                log::debug!(
                    "MMS头字段0x{:02X}解析失败或长度越界，停止解析剩余头部",
                    field
                );
                break;
            }
        }
    }

    anyhow::ensure!(
        saw_notification_ind,
        "did not find X-Mms-Message-Type = m-notification-ind"
    );
    anyhow::ensure!(
        !notif.transaction_id.is_empty(),
        "missing X-Mms-Transaction-Id"
    );
    Ok(notif)
}

/// Expiry-value = Value-length (Absolute-token(0x80)|Relative-token(0x81)) Long-integer
fn decode_expiry(data: &[u8]) -> Option<(Option<NaiveDateTime>, usize)> {
    let (len, n) = read_value_length(data).ok()?;
    let span = data.get(n..n + len as usize)?;
    let token = *span.first()?;
    let (secs, _) = read_long_integer(span.get(1..)?).ok()?;
    let ts = if token == 0x81 {
        chrono::Utc::now().naive_utc() + chrono::Duration::seconds(secs as i64)
    } else {
        chrono::DateTime::from_timestamp(secs as i64, 0)
            .map(|dt| dt.naive_utc())
            .unwrap_or_else(|| chrono::Utc::now().naive_utc())
    };
    Some((Some(ts), n + len as usize))
}

/// Best-effort skip over the value of a well-known header field we don't
/// otherwise care about, per the generic WSP `Value` grammar (WAP-230-WSP
/// §8.4.2.1):
///   Value = Short-integer | Long-integer | Uintvar-integer
///          | Date-value | Delta-seconds-value | Text-string | ...
/// In practice that collapses to three shapes we need to distinguish by
/// looking at the first byte of the value (not consumed by the caller yet):
///   - high bit set (0x80-0xFF): Short-integer -> exactly 1 byte.
///   - 0x00-0x1E: Short-length -> that many bytes follow (Long-integer /
///     other length-prefixed encodings all fit this, since Value-length's
///     short form is just the length byte itself).
///   - 0x1F: Length-quote -> a Uintvar-integer length follows, then that many
///     bytes of data.
///   - otherwise (printable ASCII, no high bit, > 0x1E): Text-string,
///     NUL-terminated.
/// Returns the number of bytes (including the value's own length prefix, if
/// any) to advance past, or `None` if the data doesn't fit any known shape
/// (caller aborts parsing, as before).
pub(crate) fn skip_unknown_value(data: &[u8]) -> Option<usize> {
    let first = *data.first()?;
    if first & 0x80 != 0 {
        Some(1)
    } else if first <= 0x1E {
        Some(1 + first as usize)
    } else if first == 0x1F {
        let (len, n) = read_uintvar(data.get(1..)?).ok()?;
        Some(n + 1 + len as usize)
    } else {
        let nul_at = data.iter().position(|&b| b == 0)?;
        Some(nul_at + 1)
    }
}

/// Uintvar-integer: continuation-bit octets, 7 value bits each, MSB first.
pub(crate) fn read_uintvar(data: &[u8]) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut i = 0usize;
    loop {
        let b = *data.get(i).context("uintvar out of bounds")?;
        value = (value << 7) | (b & 0x7F) as u64;
        i += 1;
        if b & 0x80 == 0 {
            return Ok((value, i));
        }
        anyhow::ensure!(i < 8, "uintvar too long");
    }
}

/// Value-length = Short-length(0..=30 direct) | (Length-quote(31) Uintvar-integer)
pub(crate) fn read_value_length(data: &[u8]) -> Result<(u64, usize)> {
    let first = *data.first().context("value-length out of bounds")?;
    if first < 31 {
        Ok((first as u64, 1))
    } else {
        let (len, n) = read_uintvar(data.get(1..).context("value-length out of bounds")?)?;
        Ok((len, 1 + n))
    }
}

/// Text-string: optional leading Quote(0x7F), then bytes up to and including NUL.
pub(crate) fn read_text_string(data: &[u8]) -> Result<(String, usize)> {
    let (body, prefix) = if data.first() == Some(&0x7F) {
        (&data[1..], 1)
    } else {
        (data, 0)
    };
    let nul_pos = body
        .iter()
        .position(|&b| b == 0)
        .context("text-string missing NUL terminator")?;
    let s = String::from_utf8_lossy(&body[..nul_pos]).into_owned();
    Ok((s, prefix + nul_pos + 1))
}

/// Long-integer: `[length][value bytes, big-endian]`, length in 1..=8.
pub(crate) fn read_long_integer(data: &[u8]) -> Result<(u64, usize)> {
    let len = *data.first().context("long-integer out of bounds")? as usize;
    anyhow::ensure!(
        (1..=8).contains(&len),
        "invalid long-integer length {}",
        len
    );
    let value_bytes = data
        .get(1..1 + len)
        .context("long-integer value out of bounds")?;
    let mut value: u64 = 0;
    for &b in value_bytes {
        value = (value << 8) | b as u64;
    }
    Ok((value, 1 + len))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTIFICATION_PUSH: &str = "1806246170706C69636174696F6E2F766E642E7761702E6D6D732D6D65737361676500B487AF848C82982323477242353630736C3133008D9089178031383132363130313031352F545950453D504C4D4E008A808E04000005018805810303F47F83687474703A2F2F3132302E3139382E3234372E34363A38302F5F5643784D793200";
    const DELIVERY_REPORT_PUSH: &str = "6106246170706C69636174696F6E2F766E642E7761702E6D6D732D6D65737361676500B487AF848C868D918B303832333134313233363932303031313236313938009731383132363130313031352F545950453D504C4D4E0085046A8A8F549581";

    #[test]
    fn decodes_real_notification_push() {
        let raw = hex::decode(NOTIFICATION_PUSH).unwrap();
        let MmsPush::Notification(notification) = decode_wap_push(&raw).unwrap() else {
            panic!("expected notification");
        };

        assert_eq!(notification.transaction_id, "##GrB560sl13");
        assert_eq!(
            notification.content_location.as_deref(),
            Some("http://120.198.247.46:80/_VCxMy2")
        );
        assert_eq!(notification.message_size, Some(1281));
    }

    #[test]
    fn classifies_real_delivery_report_push() {
        let raw = hex::decode(DELIVERY_REPORT_PUSH).unwrap();
        assert!(matches!(
            decode_wap_push(&raw).unwrap(),
            MmsPush::DeliveryReport(_)
        ));
    }

    #[test]
    fn decodes_real_delivery_report_fields() {
        let raw = hex::decode(DELIVERY_REPORT_PUSH).unwrap();
        let MmsPush::DeliveryReport(report) = decode_wap_push(&raw).unwrap() else {
            panic!("expected delivery report");
        };

        assert_eq!(report.message_id, "082314123692001126198");
        assert_eq!(report.recipient.as_deref(), Some("18126101015/TYPE=PLMN"));
        assert_eq!(report.status, Some(0x81));
        assert!(report.date.is_some());
    }
}
