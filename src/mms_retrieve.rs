//! M-Retrieve.conf (MMS content) decoding.
//!
//! Once `Modem::fetch_mms_content()` (see `modem/core.rs`, `AT+QHTTPxxx`) has
//! pulled the raw bytes from a WAP-push notification's `content_location` URL,
//! this module decodes them: unlike the WAP push envelope handled by
//! `mms_wap.rs`, this is the raw WAP-209 (OMA MMS Encapsulation) `m-retrieve-conf`
//! PDU body returned directly as the HTTP response — no WSP Push/SMS transport
//! envelope wraps it.
//!
//! Written against the public WAP-230-WSP / WAP-209-MMSENCAPSULATION specs and
//! cross-checked against `koreapyj/asterisk-chan-modemmanager`'s vendored
//! `mmsd-tng` codec (`src/mms/vendor/{wsputil,mmsutil}.c`) for the exact
//! well-known header codes and multipart entry framing, without a captured
//! real-world M-Retrieve.conf sample to validate against yet. As with
//! `mms_wap.rs`, this is intentionally defensive: bounds/format problems abort
//! with an error rather than panicking, and the caller always keeps the raw
//! bytes so decoding can be retried later once corrected against real hardware.

use anyhow::{Context, Result};

use crate::mms_wap::{read_text_string, read_uintvar, read_value_length, skip_unknown_value};

// X-Mms header field codes relevant to m-retrieve-conf (well-known field name
// byte = 0x80 | assigned-number, per OMA-TS-MMS_ENC Table 25).
const HDR_MESSAGE_TYPE: u8 = 0x8C;
const HDR_FROM: u8 = 0x89;
const HDR_SUBJECT: u8 = 0x96;
const HDR_CONTENT_TYPE: u8 = 0x84;

const MMS_MSG_TYPE_RETRIEVE_CONF: u8 = 0x84;

// MMS part-header field codes (distinct namespace from the top-level headers
// above; only meaningful while walking a multipart entry's own header block).
const PART_HDR_CONTENT_LOCATION: u8 = 0x8E;
const PART_HDR_CONTENT_ID: u8 = 0xC0;

/// One decoded part of an MMS multipart body (SMIL, text, image, ...).
#[derive(Debug, Clone)]
pub struct MmsRetrievePart {
    pub content_type: String,
    pub filename: Option<String>,
    pub data: Vec<u8>,
}

/// Decoded M-Retrieve.conf: the fields we surface plus every body part.
#[derive(Debug, Clone, Default)]
pub struct MmsRetrieveConf {
    pub subject: Option<String>,
    pub from: Option<String>,
    pub parts: Vec<MmsRetrievePart>,
}

/// Entry point: decode a raw M-Retrieve.conf byte buffer (the HTTP GET response
/// body fetched from a WAP-push `content_location` URL).
pub fn decode_retrieve_conf(data: &[u8]) -> Result<MmsRetrieveConf> {
    anyhow::ensure!(data.len() >= 2, "M-Retrieve.conf too short ({} bytes)", data.len());
    anyhow::ensure!(data[0] == HDR_MESSAGE_TYPE, "missing X-Mms-Message-Type header");
    anyhow::ensure!(
        data[1] == MMS_MSG_TYPE_RETRIEVE_CONF,
        "not a m-retrieve-conf PDU (type=0x{:02X})",
        data[1]
    );

    let mut pos = 2usize;
    let mut subject = None;
    let mut from = None;

    // Walk headers until Content-Type (0x84), which always marks the start of
    // the message body per WAP-209 -- mirrors how `mms_wap.rs` locates the WSP
    // push body, and how mmsd-tng's `is_content_type_header()` stops its own
    // header iterator at exactly this field.
    while pos < data.len() {
        let field = data[pos];
        if field == HDR_CONTENT_TYPE {
            break;
        }
        if field & 0x80 == 0 {
            // Application-header: NUL-terminated name, then a generic value.
            let name_end = match data[pos..].iter().position(|&b| b == 0) {
                Some(idx) => pos + idx + 1,
                None => break,
            };
            match skip_unknown_value(&data[name_end..]) {
                Some(n) if name_end + n <= data.len() => {
                    pos = name_end + n;
                    continue;
                }
                _ => break,
            }
        }
        pos += 1;
        if pos >= data.len() {
            break;
        }

        let step = match field {
            HDR_FROM => decode_from_header(&data[pos..]).map(|(v, n)| {
                from = v;
                n
            }),
            HDR_SUBJECT => decode_encoded_text_header(&data[pos..]).map(|(v, n)| {
                subject = v;
                n
            }),
            _ => skip_unknown_value(&data[pos..]),
        };

        match step {
            Some(n) if pos + n <= data.len() => pos += n,
            _ => {
                log::debug!("M-Retrieve.conf header 0x{:02X} failed to parse, stopping header scan", field);
                break;
            }
        }
    }

    anyhow::ensure!(
        pos < data.len() && data[pos] == HDR_CONTENT_TYPE,
        "M-Retrieve.conf missing Content-Type header (truncated?)"
    );

    let parts = decode_message_body(&data[pos..]).context("failed to decode MMS message body")?;
    anyhow::ensure!(!parts.is_empty(), "MMS message body contained no parts");

    Ok(MmsRetrieveConf { subject, from, parts })
}

/// Generic WSP field `Value` decode (WAP-230-WSP §8.4.2.1): distinguishes the
/// Short-integer / Long-integer / Text-string shapes by their leading byte.
/// Mirrors `mms_wap::skip_unknown_value`'s disambiguation but also returns the
/// value bytes (not just how many bytes to skip), since callers here need the
/// actual content, not just to skip past it.
enum WspFieldValue<'a> {
    Short(u8),
    Long(&'a [u8]),
    Text(&'a [u8]),
}

fn decode_field_value(data: &[u8]) -> Option<(WspFieldValue<'_>, usize)> {
    let first = *data.first()?;
    if first <= 30 {
        let len = first as usize;
        let value = data.get(1..1 + len)?;
        Some((WspFieldValue::Long(value), 1 + len))
    } else if first >= 128 {
        Some((WspFieldValue::Short(first & 0x7F), 1))
    } else if first == 31 {
        let (len, n) = read_uintvar(data.get(1..)?).ok()?;
        let value = data.get(1 + n..1 + n + len as usize)?;
        Some((WspFieldValue::Long(value), 1 + n + len as usize))
    } else {
        let nul = data.iter().position(|&b| b == 0)?;
        Some((WspFieldValue::Text(&data[..nul]), nul + 1))
    }
}

/// Reads a Short-integer (high bit set, single byte) or Long-integer
/// (`[length][big-endian bytes]`) -- used for the charset MIB-enum prefix in
/// `Encoded-string-value` and for well-known Content-Type codes.
fn read_short_or_long_integer(data: &[u8]) -> Option<(u64, usize)> {
    let first = *data.first()?;
    if first & 0x80 != 0 {
        Some(((first & 0x7F) as u64, 1))
    } else if first <= 30 {
        let len = first as usize;
        let bytes = data.get(1..1 + len)?;
        let mut v = 0u64;
        for &b in bytes {
            v = (v << 8) | b as u64;
        }
        Some((v, 1 + len))
    } else {
        None
    }
}

/// `Encoded-string-value = Text-string | (Value-length Char-set Text-string)`.
/// Only the `utf-8`/`us-ascii` charsets are handled precisely; anything else
/// falls back to a lossy UTF-8 decode rather than dropping the field, since a
/// best-effort subject/sender beats none.
fn decode_encoded_string(bytes: &[u8]) -> Option<String> {
    let first = *bytes.first()?;
    if first <= 30 || first == 31 {
        if let Ok((len, n)) = read_value_length(bytes) {
            if let Some(span) = bytes.get(n..n + len as usize) {
                if let Some((_charset, cn)) = read_short_or_long_integer(span) {
                    if let Some(text_bytes) = span.get(cn..) {
                        let nul = text_bytes.iter().position(|&b| b == 0).unwrap_or(text_bytes.len());
                        return Some(String::from_utf8_lossy(&text_bytes[..nul]).into_owned());
                    }
                }
            }
        }
    }
    read_text_string(bytes).ok().map(|(s, _)| s)
}

/// `X-Mms-Subject` value: either a plain Text-string or a Value-length
/// `[charset][text]` pair (mirrors mmsd-tng's `extract_encoded_text`).
fn decode_encoded_text_header(data: &[u8]) -> Option<(Option<String>, usize)> {
    let (val, consumed) = decode_field_value(data)?;
    let text = match val {
        WspFieldValue::Text(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        WspFieldValue::Long(bytes) => {
            let (_charset, n) = read_short_or_long_integer(bytes)?;
            let text_bytes = bytes.get(n..)?;
            let nul = text_bytes.iter().position(|&b| b == 0).unwrap_or(text_bytes.len());
            Some(String::from_utf8_lossy(&text_bytes[..nul]).into_owned())
        }
        WspFieldValue::Short(_) => None,
    };
    Some((text, consumed))
}

/// `From-value = Value-length (Address-present-token=128 Encoded-string-value
/// | Insert-address-token=129)` (mirrors mmsd-tng's `extract_from`).
fn decode_from_header(data: &[u8]) -> Option<(Option<String>, usize)> {
    let (val, consumed) = decode_field_value(data)?;
    let bytes = match val {
        WspFieldValue::Long(b) => b,
        _ => return None,
    };
    let token = *bytes.first()?;
    if token == 0x81 {
        return Some((None, consumed));
    }
    if token != 0x80 {
        return None;
    }
    Some((decode_encoded_string(&bytes[1..]), consumed))
}

/// Decode a Content-Type header/value: `Constrained-media | (Value-length
/// Media Parameters*)`. Returns the resolved MIME type string and how many
/// bytes of `data` the whole value (including any parameters) occupies --
/// parameters themselves (e.g. multipart `start`/`type`, or `charset`) aren't
/// needed for storing/serving the decoded parts, so they're skipped rather
/// than parsed.
fn decode_content_type_value(data: &[u8]) -> Option<(String, usize)> {
    let (val, consumed) = decode_field_value(data)?;
    let mime = match val {
        WspFieldValue::Short(code) => content_type_name(code)?.to_string(),
        WspFieldValue::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        WspFieldValue::Long(inner) => {
            let (media_val, _media_consumed) = decode_field_value(inner)?;
            match media_val {
                WspFieldValue::Short(code) => content_type_name(code)?.to_string(),
                WspFieldValue::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                WspFieldValue::Long(_) => return None,
            }
        }
    };
    Some((mime, consumed))
}

/// Decode the MMS message body starting at its Content-Type header field byte
/// (`0x84`). If the resolved media type is a multipart container, walks the
/// WSP multipart entries (WAP-230-WSP §8.5.2); otherwise the whole remainder
/// is treated as a single part (a valid, if rare, single-part MMS).
fn decode_message_body(data: &[u8]) -> Result<Vec<MmsRetrievePart>> {
    anyhow::ensure!(!data.is_empty() && data[0] == HDR_CONTENT_TYPE, "expected Content-Type header");
    let mut pos = 1usize;

    let (top_level_mime, ct_consumed) = decode_content_type_value(data.get(pos..).context("truncated Content-Type value")?)
        .context("failed to decode top-level Content-Type value")?;
    pos += ct_consumed;

    if !top_level_mime.contains("multipart") {
        return Ok(vec![MmsRetrievePart {
            content_type: top_level_mime,
            filename: None,
            data: data[pos..].to_vec(),
        }]);
    }

    // Number of parts (Uintvar). Later specs set this to 0 and expect the
    // reader to just iterate entries until the data runs out, so the value
    // itself is not relied upon -- only its encoded length matters here.
    let (_n_entries, n) = read_uintvar(data.get(pos..).context("truncated multipart entry count")?)?;
    pos += n;

    let mut parts = Vec::new();
    while pos < data.len() {
        let (headers_len, n1) = read_uintvar(&data[pos..])?;
        pos += n1;
        let (body_len, n2) = read_uintvar(data.get(pos..).context("truncated multipart entry")?)?;
        pos += n2;

        let entry_start = pos;
        let headers_len = headers_len as usize;
        let body_len = body_len as usize;
        anyhow::ensure!(
            entry_start + headers_len + body_len <= data.len(),
            "multipart entry length out of bounds"
        );

        let (content_type, ct_consumed) = decode_content_type_value(&data[entry_start..entry_start + headers_len])
            .with_context(|| format!("failed to decode multipart entry #{} content-type", parts.len()))?;
        let part_headers = &data[entry_start + ct_consumed..entry_start + headers_len];
        let filename = extract_part_filename(part_headers);

        let body_start = entry_start + headers_len;
        let body = data[body_start..body_start + body_len].to_vec();

        parts.push(MmsRetrievePart { content_type, filename, data: body });
        pos = body_start + body_len;
    }

    Ok(parts)
}

/// Best-effort filename hint for a part, from its `Content-Location` or
/// `Content-ID` header (part-level headers, distinct namespace from the
/// top-level X-Mms-* headers). Returns `None` if neither is present/decodable
/// -- the caller synthesizes a name from the part index + MIME type instead.
fn extract_part_filename(headers: &[u8]) -> Option<String> {
    let mut pos = 0usize;
    let mut filename = None;

    while pos < headers.len() {
        let field = headers[pos];
        if field & 0x80 == 0 {
            let name_end = match headers[pos..].iter().position(|&b| b == 0) {
                Some(idx) => pos + idx + 1,
                None => break,
            };
            match skip_unknown_value(&headers[name_end..]) {
                Some(n) => {
                    pos = name_end + n;
                    continue;
                }
                None => break,
            }
        }
        pos += 1;
        if pos >= headers.len() {
            break;
        }

        let mut consumed_here = None;
        if field == PART_HDR_CONTENT_LOCATION {
            if let Ok((s, n)) = read_text_string(&headers[pos..]) {
                filename.get_or_insert(s);
                consumed_here = Some(n);
            }
        } else if field == PART_HDR_CONTENT_ID {
            if let Ok((s, n)) = read_text_string(&headers[pos..]) {
                filename.get_or_insert(s.trim_matches(|c| c == '<' || c == '>').to_string());
                consumed_here = Some(n);
            }
        }

        let step = consumed_here.or_else(|| skip_unknown_value(&headers[pos..]));
        match step {
            Some(n) if pos + n <= headers.len() => pos += n,
            _ => break,
        }
    }

    filename
}

/// WSP well-known Content-Type assignments (http://www.wapforum.org/wina/wsp-content-type.htm),
/// as used by every WAP/MMS stack; index == well-known code. Cross-checked
/// against mmsd-tng's vendored `wsputil.c` `content_types[]` table.
const CONTENT_TYPES: &[&str] = &[
    "*/*",
    "text/*",
    "text/html",
    "text/plain",
    "text/x-hdml",
    "text/x-ttml",
    "text/x-vCalendar",
    "text/x-vCard",
    "text/vnd.wap.wml",
    "text/vnd.wap.wmlscript",
    "text/vnd.wap.wta-event",
    "multipart/*",
    "multipart/mixed",
    "multipart/form-data",
    "multipart/byteranges",
    "multipart/alternative",
    "application/*",
    "application/java-vm",
    "application/x-www-form-urlencoded",
    "application/x-hdmlc",
    "application/vnd.wap.wmlc",
    "application/vnd.wap.wmlscriptc",
    "application/vnd.wap.wta-eventc",
    "application/vnd.wap.uaprof",
    "application/vnd.wap.wtls-ca-certificate",
    "application/vnd.wap.wtls-user-certificate",
    "application/x-x509-ca-cert",
    "application/x-x509-user-cert",
    "image/*",
    "image/gif",
    "image/jpeg",
    "image/tiff",
    "image/png",
    "image/vnd.wap.wbmp",
    "application/vnd.wap.multipart.*",
    "application/vnd.wap.multipart.mixed",
    "application/vnd.wap.multipart.form-data",
    "application/vnd.wap.multipart.byteranges",
    "application/vnd.wap.multipart.alternative",
    "application/xml",
    "text/xml",
    "application/vnd.wap.wbxml",
    "application/x-x968-cross-cert",
    "application/x-x968-ca-cert",
    "application/x-x968-user-cert",
    "text/vnd.wap.si",
    "application/vnd.wap.sic",
    "text/vnd.wap.sl",
    "application/vnd.wap.slc",
    "text/vnd.wap.co",
    "application/vnd.wap.coc",
    "application/vnd.wap.multipart.related",
    "application/vnd.wap.sia",
    "text/vnd.wap.connectivity-xml",
    "application/vnd.wap.connectivity-wbxml",
    "application/pkcs7-mime",
    "application/vnd.wap.hashed-certificate",
    "application/vnd.wap.signed-certificate",
    "application/vnd.wap.cert-response",
    "application/xhtml+xml",
    "application/wml+xml",
    "text/css",
    "application/vnd.wap.mms-message",
    "application/vnd.wap.rollover-certificate",
    "application/vnd.wap.locc+wbxml",
    "application/vnd.wap.loc+xml",
    "application/vnd.syncml.dm+wbxml",
    "application/vnd.syncml.dm+xml",
    "application/vnd.syncml.notification",
    "application/vnd.wap.xhtml+xml",
    "application/vnd.wv.csp.cir",
    "application/vnd.oma.dd+xml",
    "application/vnd.oma.drm.message",
    "application/vnd.oma.drm.content",
    "application/vnd.oma.drm.rights+xml",
    "application/vnd.oma.drm.rights+wbxml",
];

fn content_type_name(code: u8) -> Option<&'static str> {
    CONTENT_TYPES.get(code as usize).copied()
}
