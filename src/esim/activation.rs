//! Parsing of eSIM activation sources: `LPA:1$smdp$matchingId[$conf]` strings,
//! multi-line text files, and QR-code images (JPG/PNG/…).
//!
//! A numeric filename stem is treated as a COM-port hint (e.g. `2.jpg` -> COM2)
//! so the batch UI can auto-pair a code with its port.

use std::path::Path;

use serde::Serialize;

use crate::phone_number::normalize_msisdn;

/// A normalized eSIM activation code from any source (text line, QR image, manual).
#[derive(Debug, Clone, Serialize)]
pub struct ActivationCode {
    /// The full `LPA:1$smdp$matchingId[$conf]` string.
    pub raw_lpa: String,
    pub smdp: String,
    pub matching_id: String,
    pub confirmation_code: Option<String>,
    /// Where this code came from, e.g. `qr:2.jpg`, `text:esim_list.txt#L2`, `manual`.
    pub source: String,
    /// COM-port number implied by a numeric filename stem, if any (`2.jpg` -> 2).
    pub port_hint: Option<u32>,
    /// Any extra non-LPA tokens found on the same line (e.g. a phone number).
    pub label: Option<String>,
}

/// Parse a single line that may contain one or more `LPA:` tokens plus free text.
/// Non-LPA tokens become the shared `label` of the codes found on that line.
pub fn parse_lpa_line(line: &str, source: &str, port_hint: Option<u32>) -> Vec<ActivationCode> {
    let mut out = Vec::new();
    let mut label_tokens: Vec<String> = Vec::new();
    for tok in line.split_whitespace() {
        match parse_lpa_token(tok, source, port_hint) {
            Some(code) => out.push(code),
            None => label_tokens.push(tok.to_string()),
        }
    }
    if !label_tokens.is_empty() {
        let label = label_tokens.join(" ");
        let label = normalize_label(&label);
        for c in out.iter_mut() {
            if c.label.is_none() {
                c.label = Some(label.clone());
            }
        }
    }
    out
}

fn normalize_label(label: &str) -> String {
    let cleaned = normalize_msisdn(label);
    if !cleaned.is_empty() {
        cleaned
    } else {
        label.trim().to_string()
    }
}

pub(super) fn phone_from_ocr_text(text: &str) -> Option<String> {
    let mut candidates = Vec::new();
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        let corrected_label = line.replace('0', "o").replace('I', "l").to_ascii_lowercase();
        let value = ["mobile number", "phone number", "msisdn"]
            .iter()
            .find_map(|label| lower.find(label).or_else(|| corrected_label.find(label))
                .map(|offset| &line[offset + label.len()..]));
        let Some(value) = value else { continue };
        let value = value.trim().trim_start_matches([':', '=', '.', '\u{ff1a}', '\u{ff0e}']).trim();
        if !value.chars().all(|ch| ch.is_ascii_digit() || ch.is_whitespace() || "+-()".contains(ch)) {
            continue;
        }
        let phone = normalize_msisdn(value);
        if (7..=15).contains(&phone.len()) && !candidates.contains(&phone) {
            candidates.push(phone);
        }
    }
    if candidates.len() == 1 { candidates.pop() } else { None }
}

#[cfg(windows)]
fn recognize_image_text(image: &image::GrayImage) -> anyhow::Result<String> {
    use windows::core::HSTRING;
    use windows::Globalization::Language;
    use windows::Graphics::Imaging::{BitmapPixelFormat, SoftwareBitmap};
    use windows::Media::Ocr::OcrEngine;
    use windows::Storage::Streams::DataWriter;
    use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_MULTITHREADED};

    unsafe { CoInitializeEx(None, COINIT_MULTITHREADED).ok()? };
    let result = (|| -> anyhow::Result<String> {
        let language = Language::CreateLanguage(&HSTRING::from("en-US"))?;
        let engine = if OcrEngine::IsLanguageSupported(&language)? {
            OcrEngine::TryCreateFromLanguage(&language)?
        } else {
            OcrEngine::TryCreateFromUserProfileLanguages()?
        };
        let limit = OcrEngine::MaxImageDimension()?;
        let resized;
        let image = if image.width() > limit || image.height() > limit {
            resized = image::imageops::thumbnail(image, limit, limit);
            &resized
        } else {
            image
        };
        let writer = DataWriter::new()?;
        writer.WriteBytes(image.as_raw())?;
        let bitmap = SoftwareBitmap::CreateCopyFromBuffer(
            &writer.DetachBuffer()?, BitmapPixelFormat::Gray8,
            image.width() as i32, image.height() as i32,
        )?;
        let result = engine.RecognizeAsync(&bitmap)?.get()?;
        let mut lines = Vec::new();
        for line in result.Lines()? {
            lines.push(line.Text()?.to_string());
        }
        Ok(lines.join("\n"))
    })();
    unsafe { CoUninitialize() };
    result
}

#[cfg(not(windows))]
fn recognize_image_text(_image: &image::GrayImage) -> anyhow::Result<String> {
    anyhow::bail!("Image phone OCR requires Windows with English OCR installed")
}

/// Parse one whitespace-delimited token into an `ActivationCode`, if it is an LPA string.
fn parse_lpa_token(tok: &str, source: &str, port_hint: Option<u32>) -> Option<ActivationCode> {
    let t = tok.trim();
    // Accept both "LPA:1$..." and a bare "1$..." token.
    let body = t
        .strip_prefix("LPA:")
        .or_else(|| if t.starts_with("1$") { Some(t) } else { None })?;

    let parts: Vec<&str> = body.split('$').collect();
    if parts.len() < 3 || parts[0] != "1" {
        return None;
    }
    let smdp = parts[1].trim().to_string();
    let matching_id = parts[2].trim().to_string();
    if smdp.is_empty() || matching_id.is_empty() {
        return None;
    }
    let confirmation_code = parts
        .get(3)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    Some(ActivationCode {
        raw_lpa: format!("LPA:{}", body),
        smdp,
        matching_id,
        confirmation_code,
        source: source.to_string(),
        port_hint,
        label: None,
    })
}

/// Decode every QR code in an image and return the activation codes they contain.
pub fn decode_qr_image(bytes: &[u8], source: &str, port_hint: Option<u32>) -> Vec<ActivationCode> {
    let img = match image::load_from_memory(bytes) {
        Ok(i) => i.to_luma8(),
        Err(_) => return Vec::new(),
    };
    let mut prepared = rqrr::PreparedImage::prepare(img.clone());
    let mut out = Vec::new();
    for grid in prepared.detect_grids() {
        if let Ok((_meta, content)) = grid.decode() {
            out.extend(parse_lpa_line(&content, source, port_hint));
        }
    }
    if out.len() == 1 && out[0].label.is_none() {
        match recognize_image_text(&img) {
            Ok(text) => out[0].label = phone_from_ocr_text(&text),
            Err(error) => log::warn!("[eSIM] image phone OCR unavailable: {}", error),
        }
    }
    out
}

/// Derive a COM-port number from a filename stem (`2` -> 2, `02` -> 2, `com3` -> None).
pub fn port_hint_from_stem(stem: &str) -> Option<u32> {
    let s = stem.trim();
    if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
        s.parse::<u32>().ok()
    } else {
        None
    }
}

/// Parse raw bytes of one uploaded file by its filename, dispatching to text or QR.
pub fn parse_file_bytes(name: &str, bytes: &[u8]) -> Vec<ActivationCode> {
    let path = Path::new(name);
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let port_hint = port_hint_from_stem(stem);

    match ext.as_str() {
        "txt" | "csv" | "" => {
            let content = String::from_utf8_lossy(bytes);
            let mut out = Vec::new();
            for (idx, line) in content.lines().enumerate() {
                let src = format!("text:{}#L{}", name, idx + 1);
                out.extend(parse_lpa_line(line, &src, None));
            }
            out
        }
        "jpg" | "jpeg" | "png" | "bmp" | "gif" | "webp" => {
            decode_qr_image(bytes, &format!("qr:{}", name), port_hint)
        }
        _ => Vec::new(),
    }
}

/// Scan a directory for activation sources: `*.txt` parsed as text, image files
/// decoded as QR. Results are sorted by filename for stable ordering.
pub fn scan_source_dir(dir: &Path) -> Vec<ActivationCode> {
    let mut out = Vec::new();
    let read_dir = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return out,
    };
    let mut paths: Vec<_> = read_dir
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    paths.sort();

    for path in paths {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        if let Ok(bytes) = std::fs::read(&path) {
            out.extend(parse_file_bytes(&name, &bytes));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_lpa() {
        let codes = parse_lpa_line("LPA:1$rsp.truphone.com$JQ-22FHBJ-164K53M", "manual", None);
        assert_eq!(codes.len(), 1);
        assert_eq!(codes[0].smdp, "rsp.truphone.com");
        assert_eq!(codes[0].matching_id, "JQ-22FHBJ-164K53M");
        assert!(codes[0].confirmation_code.is_none());
    }

    #[test]
    fn parses_line_with_trailing_label() {
        let codes = parse_lpa_line(
            "0493913181  LPA:1$sm.example.com$ABC123",
            "text:list.txt#L3",
            None,
        );
        assert_eq!(codes.len(), 1);
        assert_eq!(codes[0].matching_id, "ABC123");
        assert_eq!(codes[0].label.as_deref(), Some("493913181"));
    }

    #[test]
    fn parses_confirmation_code() {
        let codes = parse_lpa_line("LPA:1$smdp$mid$conf", "manual", None);
        assert_eq!(codes[0].confirmation_code.as_deref(), Some("conf"));
    }

    #[test]
    fn port_hint_from_numeric_stem() {
        assert_eq!(port_hint_from_stem("2"), Some(2));
        assert_eq!(port_hint_from_stem("02"), Some(2));
        assert_eq!(port_hint_from_stem("com3"), None);
    }

    #[test]
    fn ocr_phone_uses_mobile_label_not_sim_number() {
        assert_eq!(phone_from_ocr_text(
            "New Mobile Number : 779347821\nPackage : PRP_16\nSim Number : 9402980000044561\nCustomer : MR"
        ).as_deref(), Some("779347821"));
        assert_eq!(phone_from_ocr_text("PHONE NUMBER: +94 (77) 934-7821").as_deref(), Some("94779347821"));
        assert_eq!(phone_from_ocr_text("New M0biIe Number \u{ff0e} 779347821").as_deref(), Some("779347821"));
        assert_eq!(phone_from_ocr_text("MOBILE NUMBER\u{ff1a} 779347821").as_deref(), Some("779347821"));
    }

    #[test]
    fn ocr_phone_rejects_unlabelled_invalid_and_ambiguous_values() {
        for text in [
            "Sim Number: 9402980000044561\n779347821",
            "Mobile Number: 77934?821",
            "Mobile Number: 123",
            "Mobile Number: 9402980000044561",
            "Mobile Number: 779347821\nPhone Number: 779543395",
        ] {
            assert_eq!(phone_from_ocr_text(text), None, "{text}");
        }
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "requires Windows OCR and ESIM_OCR_TEST_IMAGE pointing to the supplied PNG"]
    fn ocr_image_reads_phone_independently_of_filename() {
        let path = std::env::var("ESIM_OCR_TEST_IMAGE").expect("set ESIM_OCR_TEST_IMAGE");
        let bytes = std::fs::read(path).expect("read OCR image fixture");
        let codes = parse_file_bytes("upload.png", &bytes);
        assert_eq!(codes.len(), 1);
        assert_eq!(codes[0].label.as_deref(), Some("779347821"));
        assert_eq!(codes[0].port_hint, None);
    }
}
