use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

const API_BASE: &str = "https://www.worldcode.win/ksapi.ashx";
const USER_API_BASE: &str = "https://www.worldcode.win/yhapi.ashx";
pub const API_KEY_SETTING: &str = "worldcode_api_key";

/// Country list for the WorldCode seller API.
/// Format: (country_id, dial_prefix, display_name)
pub const COUNTRIES: &[(&str, &str, &str)] = &[
    ("afg", "+93", "Afghanistan"),
    ("dza", "+213", "Algeria"),
    ("ago", "+244", "Angola"),
    ("arg", "+54", "Argentina"),
    ("aus", "+61", "Australia"),
    ("aut", "+43", "Austria"),
    ("aze", "+994", "Azerbaijan"),
    ("bgd", "+880", "Bangladesh"),
    ("bel", "+32", "Belgium"),
    ("btn", "+975", "Bhutan"),
    ("bol", "+591", "Bolivia"),
    ("bra", "+55", "Brazil"),
    ("brn", "+673", "Brunei"),
    ("bfa", "+226", "Burkina Faso"),
    ("bdi", "+257", "Burundi"),
    ("caf", "+236", "Central African Republic"),
    ("khm", "+855", "Cambodia"),
    ("cmr", "+237", "Cameroon"),
    ("can", "+1", "Canada"),
    ("cpv", "+238", "Cape Verde"),
    ("tcd", "+235", "Chad"),
    ("chl", "+56", "Chile"),
    ("chn", "+86", "China"),
    ("col", "+57", "Colombia"),
    ("cog", "+242", "Congo"),
    ("cub", "+53", "Cuba"),
    ("cze", "+420", "Czech"),
    ("dnk", "+45", "Denmark"),
    ("dom", "+1", "Dominican"),
    ("egy", "+20", "Egypt"),
    ("eng", "+44", "England"),
    ("eth", "+251", "Ethiopia"),
    ("fji", "+679", "Fiji"),
    ("fra", "+33", "France"),
    ("gab", "+241", "Gabon"),
    ("gmb", "+220", "Gambia"),
    ("geo", "+995", "Georgia"),
    ("deu", "+49", "Germany"),
    ("gha", "+233", "Ghana"),
    ("grc", "+30", "Greece"),
    ("gtm", "+502", "Guatemala"),
    ("gin", "+224", "Guinea"),
    ("gnb", "+245", "Guinea-Bissau"),
    ("hnd", "+504", "Honduras"),
    ("hkg", "+852", "Hong Kong"),
    ("ind", "+91", "India"),
    ("idn", "+62", "Indonesia"),
    ("irn", "+98", "Iran"),
    ("irq", "+964", "Iraq"),
    ("isr", "+972", "Israel"),
    ("ita", "+39", "Italy"),
    ("civ", "+225", "Ivory"),
    ("jpn", "+81", "Japan"),
    ("jor", "+962", "Jordan"),
    ("kaz", "+7", "Kazakhstan"),
    ("ken", "+254", "Kenya"),
    ("kwt", "+965", "Kuwait"),
    ("kgz", "+996", "Kyrgyzstan"),
    ("lao", "+856", "Laos"),
    ("lbn", "+961", "Lebanon"),
    ("lbr", "+231", "Liberia"),
    ("lby", "+218", "Libyan"),
    ("mac", "+853", "Macao"),
    ("mdg", "+261", "Madagascar"),
    ("mys", "+60", "Malaysia"),
    ("mdv", "+960", "Maldives"),
    ("mli", "+223", "Mali"),
    ("mrt", "+222", "Mauritania"),
    ("mus", "+230", "Mauritius"),
    ("mex", "+52", "Mexico"),
    ("mng", "+976", "Mongolia"),
    ("mar", "+212", "Morocco"),
    ("moz", "+258", "Mozambique"),
    ("mmr", "+95", "Myanmar"),
    ("npl", "+977", "Nepal"),
    ("nld", "+31", "Netherlands"),
    ("nzl", "+64", "New Zealand"),
    ("nic", "+505", "Nicaragua"),
    ("nga", "+234", "Nigeria"),
    ("pak", "+92", "Pakistan"),
    ("pan", "+507", "Panama"),
    ("png", "+675", "Papua"),
    ("per", "+51", "Peru"),
    ("phl", "+63", "Philippines"),
    ("pol", "+48", "Poland"),
    ("pri", "+1", "Puerto Rico"),
    ("rou", "+40", "Romania"),
    ("rus", "+7", "Russia"),
    ("wsm", "+685", "Samoa"),
    ("sau", "+966", "Saudi Arabia"),
    ("sen", "+221", "Senegal"),
    ("sle", "+232", "Sierra Leone"),
    ("sgp", "+65", "Singapore"),
    ("som", "+252", "Somalia"),
    ("zaf", "+27", "South Africa"),
    ("kor", "+82", "South Korea"),
    ("ssd", "+211", "South Sudan"),
    ("esp", "+34", "Spain"),
    ("lka", "+94", "Sri Lanka"),
    ("sdn", "+249", "Sudan"),
    ("swe", "+46", "Sweden"),
    ("che", "+41", "Switzerland"),
    ("syr", "+963", "Syrian"),
    ("twn", "+886", "Taiwan"),
    ("tjk", "+992", "Tajikistan"),
    ("tza", "+255", "Tanzania"),
    ("tha", "+66", "Thailand"),
    ("tls", "+670", "Timor Leste"),
    ("tgo", "+228", "Togo"),
    ("tun", "+216", "Tunisia"),
    ("tur", "+90", "Turkey"),
    ("are", "+971", "UAE"),
    ("uga", "+256", "Uganda"),
    ("ury", "+598", "Uruguay"),
    ("usa", "+1", "USA"),
    ("uzb", "+998", "Uzbekistan"),
    ("ven", "+58", "Venezuela"),
    ("vnm", "+84", "Vietnam"),
    ("yem", "+967", "Yemen"),
    ("zmb", "+260", "Zambia"),
    ("zwe", "+263", "Zimbabwe"),
    ("mtq", "+596", "Martinique"),
    ("niu", "+683", "Niue"),
    ("cod", "+243", "Zaire"),
    ("vgb", "+1284", "British Virgin Islands"),
];

#[derive(Debug, Serialize)]
pub struct CountryInfo {
    pub id: &'static str,
    pub prefix: &'static str,
    pub name: &'static str,
}

pub fn countries() -> Vec<CountryInfo> {
    COUNTRIES
        .iter()
        .map(|(id, prefix, name)| CountryInfo { id, prefix, name })
        .collect()
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UserItemPrice {
    #[serde(rename = "Item_ID", deserialize_with = "string_or_number")]
    pub item_id: String,
    #[serde(rename = "Item_Name")]
    pub item_name: String,
    #[serde(rename = "Item_UPrice", deserialize_with = "string_or_number")]
    pub item_uprice: String,
    #[serde(rename = "Country_ID")]
    pub country_id: Option<String>,
    #[serde(rename = "Country_Title")]
    pub country_title: Option<String>,
}

fn string_or_number<'de, Deserializer>(deserializer: Deserializer) -> std::result::Result<String, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        _ => Err(serde::de::Error::custom("expected a string or number")),
    }
}

fn seller_request(client: &reqwest::Client, method: reqwest::Method, api_key: &str) -> reqwest::RequestBuilder {
    client.request(method, API_BASE).query(&[("key", api_key)])
}

fn user_catalog_request(client: &reqwest::Client, keyword: Option<&str>) -> reqwest::RequestBuilder {
    client.get(USER_API_BASE).query(&[("act", "getItem"), ("key", keyword.unwrap_or("").trim())])
}

pub async fn get_user_item_prices(
    client: &reqwest::Client,
    keyword: Option<&str>,
) -> Result<Vec<UserItemPrice>> {
    let response = user_catalog_request(client, keyword)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to request WorldCode user item catalog")?;

    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to read WorldCode user item catalog response")?;

    if !status.is_success() {
        bail!("WorldCode user API returned HTTP {}", status);
    }

    let raw = text.trim();
    let payload = if let Some(rest) = raw.strip_prefix("1|") {
        rest
    } else if let Some(rest) = raw.strip_prefix("0|") {
        bail!("WorldCode user API getItem failed: {}", rest);
    } else {
        raw
    };

    let items: Vec<UserItemPrice> = serde_json::from_str(payload)
        .context("Failed to parse WorldCode user getItem payload")?;

    Ok(items)
}

// ─── Shared request helper ───────────────────────────────────────────────

async fn do_post<T: Serialize, R: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    api_key: &str,
    payload: &T,
) -> Result<R> {
    let response = seller_request(client, reqwest::Method::POST, api_key)
        .header("Content-Type", "application/json")
        .json(payload)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to send request to WorldCode platform")?;

    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to read WorldCode platform response")?;

    if !status.is_success() {
        anyhow::bail!("WorldCode platform returned HTTP {}", status);
    }

    serde_json::from_str(&text)
        .context("Failed to parse WorldCode response")
}

async fn do_get<R: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    api_key: &str,
    query: &[(&str, &str)],
) -> Result<R> {
    let response = seller_request(client, reqwest::Method::GET, api_key)
        .query(query)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to send GET request to WorldCode platform")?;

    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to read WorldCode platform response")?;

    if !status.is_success() {
        anyhow::bail!("WorldCode platform returned HTTP {}", status);
    }

    serde_json::from_str(&text)
        .context("Failed to parse WorldCode response")
}

// ─── Shared response type (code + data) ─────────────────────────────────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiResponse {
    pub code: String,
    pub data: Option<String>,
}

/// Some platform rejections are deterministic business-rule failures and should
/// not be retried (e.g. keyword mismatch / pure-digit content).
pub fn is_unretryable_platform_rejection(resp: &ApiResponse) -> bool {
    if resp.code != "0" {
        return false;
    }

    let Some(reason) = resp.data.as_deref() else {
        return false;
    };

    reason.contains("未匹配到关键字")
        || reason.contains("丢弃纯数字")
        || reason.to_ascii_lowercase().contains("keyword")
}

fn is_pure_digit_rejection(resp: &ApiResponse) -> bool {
    resp.code == "0"
        && resp
            .data
            .as_deref()
            .map(|r| r.contains("丢弃纯数字"))
            .unwrap_or(false)
}

fn is_digits_only_sms(s: &str) -> bool {
    let trimmed = s.trim();
    !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit())
}

// ─── 1. PhoneAddBatch ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct PhoneAddBatchRequest<'a> {
    act: &'static str,
    PhoneList: Vec<PhoneAddEntry<'a>>,
}

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct PhoneAddEntry<'a> {
    Country_ID: &'a str,
    Phone_Num: &'a str,
    Phone_ComName: &'a str,
    Provi_Name: &'a str,
    City_Name: &'a str,
}

/// Upload a batch of phone numbers to the 火狐狸 platform (max 50 per batch).
pub struct BatchUploadResult {
    pub batch_id: String,
    pub phone_numbers: Vec<String>,
    pub response: ApiResponse,
}

pub async fn upload_phone_batch(
    client: &reqwest::Client,
    api_key: &str,
    country_id: &str,
    phone_numbers: &[String],
) -> Result<Vec<BatchUploadResult>> {
    const BATCH_SIZE: usize = 50;
    let mut results = Vec::new();

    for chunk in phone_numbers.chunks(BATCH_SIZE) {
        let phone_list: Vec<PhoneAddEntry> = chunk
            .iter()
            .map(|num| PhoneAddEntry {
                Country_ID: country_id,
                Phone_Num: num,
                Phone_ComName: "",
                Provi_Name: "",
                City_Name: "",
            })
            .collect();

        let payload = PhoneAddBatchRequest {
            act: "PhoneAddBatch",
            PhoneList: phone_list,
        };
        let resp: ApiResponse = do_post(client, api_key, &payload).await?;
        let batch_id = resp.data.clone().unwrap_or_default();
        results.push(BatchUploadResult {
            batch_id,
            phone_numbers: chunk.iter().cloned().collect(),
            response: resp,
        });
    }
    Ok(results)
}

// ─── 2. PhoneDeleteBatch ────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct PhoneDeleteBatchRequest<'a> {
    act: &'static str,
    PhoneList: Vec<PhoneDeleteEntry<'a>>,
}

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct PhoneDeleteEntry<'a> {
    Country_ID: &'a str,
    Phone_Num: &'a str,
}

/// Delete uploaded phone numbers from the platform (max 100 per batch).
pub async fn delete_phone_batch(
    client: &reqwest::Client,
    api_key: &str,
    entries: &[(&str, &str)], // (country_id, phone_num) pairs
) -> Result<Vec<ApiResponse>> {
    const BATCH_SIZE: usize = 100;
    let mut results = Vec::new();

    for chunk in entries.chunks(BATCH_SIZE) {
        let phone_list: Vec<PhoneDeleteEntry> = chunk
            .iter()
            .map(|(country_id, phone_num)| PhoneDeleteEntry {
                Country_ID: country_id,
                Phone_Num: phone_num,
            })
            .collect();

        let payload = PhoneDeleteBatchRequest {
            act: "PhoneDeleteBatch",
            PhoneList: phone_list,
        };
        let resp: ApiResponse = do_post(client, api_key, &payload).await?;
        results.push(resp);
    }
    Ok(results)
}

// ─── 3. PhoneDeleteCountry ──────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct PhoneDeleteCountryRequest {
    act: &'static str,
    #[serde(rename = "Country_ID")]
    country_id: String,
}

/// Delete all uploaded phone numbers for a given country.
pub async fn delete_phone_country(
    client: &reqwest::Client,
    api_key: &str,
    country_id: &str,
) -> Result<ApiResponse> {
    let payload = PhoneDeleteCountryRequest {
        act: "PhoneDeleteCountry",
        country_id: country_id.to_string(),
    };
    do_post(client, api_key, &payload).await
}

// ─── 4. PhoneDeleteAll ──────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct PhoneDeleteAllRequest {
    act: &'static str,
}

/// Delete all uploaded phone numbers from the platform.
pub async fn delete_phone_all(client: &reqwest::Client, api_key: &str) -> Result<ApiResponse> {
    let payload = PhoneDeleteAllRequest {
        act: "PhoneDeleteAll",
    };
    do_post(client, api_key, &payload).await
}

// ─── 5. PhoneBatchResult ────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct BatchPhoneStatusItem {
    #[serde(rename = "Phone_Status")]
    pub phone_status: Option<String>,
    #[serde(rename = "Phone_Num")]
    pub phone_num: Option<String>,
    #[serde(rename = "Phone_Msg")]
    pub phone_msg: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BatchStatusResponse {
    pub code: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// Query batch upload status. Phone_Status: "1"=success, "0"=fail, "5"=uploading.
pub async fn query_batch_status(
    client: &reqwest::Client,
    api_key: &str,
    batch_id: &str,
) -> Result<BatchStatusResponse> {
    #[derive(Debug, Serialize)]
    struct Request {
        act: &'static str,
        #[serde(rename = "BatchID")]
        batch_id: String,
    }

    let payload = Request {
        act: "PhoneBatchResult",
        batch_id: batch_id.to_string(),
    };
    do_post(client, api_key, &payload).await
}

// ─── 6. GetWaitPhoneList ────────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct WaitPhoneItem {
    #[serde(rename = "Item_ID")]
    pub item_id: Option<String>,
    #[serde(rename = "Phone_GetTime")]
    pub phone_get_time: Option<String>,
    #[serde(rename = "Phone_Num")]
    pub phone_num: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WaitPhoneListResponse {
    pub code: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// Get the list of phone numbers waiting for SMS on the platform.
pub async fn get_wait_phone_list(
    client: &reqwest::Client,
    api_key: &str,
) -> Result<WaitPhoneListResponse> {
    do_get(client, api_key, &[("act", "GetWaitPhoneList")]).await
}

// ─── 7. GetResultPhoneList ──────────────────────────────────────────────

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub struct ResultPhoneItem {
    #[serde(rename = "Item_ID")]
    pub item_id: Option<String>,
    #[serde(rename = "Phone_GetTime")]
    pub phone_get_time: Option<String>,
    #[serde(rename = "Phone_Num")]
    pub phone_num: Option<String>,
    #[serde(rename = "Phone_IsRet")]
    pub phone_is_ret: Option<String>,
    #[serde(rename = "Phone_RetTime")]
    pub phone_ret_time: Option<String>,
    #[serde(rename = "Phone_Remark")]
    pub phone_remark: Option<String>,
    #[serde(rename = "Phone_RemarkTime")]
    pub phone_remark_time: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResultPhoneListResponse {
    pub code: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// Query SMS result for a specific phone number / item on the platform.
pub async fn get_result_phone_list(
    client: &reqwest::Client,
    api_key: &str,
    country_id: &str,
    phone_num: &str,
    item_id: &str,
) -> Result<ResultPhoneListResponse> {
    do_get(
        client,
        api_key,
        &[
            ("act", "GetResultPhoneList"),
            ("Country_ID", country_id),
            ("Phone_Num", phone_num),
            ("Item_ID", item_id),
        ],
    )
    .await
}

// ─── 8. UploadSms ───────────────────────────────────────────────────────

/// Upload a received SMS back to the 火狐狸 platform.
pub async fn upload_sms(
    client: &reqwest::Client,
    api_key: &str,
    country_id: &str,
    phone_num: &str,
    sms_content: &str,
) -> Result<ApiResponse> {
    #[derive(Debug, Serialize)]
    #[allow(non_snake_case)]
    struct UploadSmsRequest {
        act: &'static str,
        Country_ID: String,
        Phone_Num: String,
        Phone_SmsContent: String,
    }

    let payload = UploadSmsRequest {
        act: "UploadSms",
        Country_ID: country_id.to_string(),
        Phone_Num: phone_num.to_string(),
        Phone_SmsContent: sms_content.to_string(),
    };
    log::info!(
        "[火狐狸平台] UploadSms request: phone={}, country_id={}, content={:?}",
        phone_num,
        country_id,
        payload.Phone_SmsContent
    );
    let first_resp: anyhow::Result<ApiResponse> = do_post(client, api_key, &payload).await;
    match &first_resp {
        Ok(r) => log::info!(
            "[火狐狸平台] UploadSms response: phone={}, code={}, data={:?}",
            phone_num,
            r.code,
            r.data
        ),
        Err(e) => log::warn!(
            "[火狐狸平台] UploadSms failed: phone={}, error={}",
            phone_num,
            e
        ),
    }

    let Ok(first_ok) = first_resp else {
        return first_resp;
    };

    if is_pure_digit_rejection(&first_ok) && is_digits_only_sms(sms_content) {
        let fallback_content = format!("OTP {}", sms_content.trim());
        let fallback_payload = UploadSmsRequest {
            act: "UploadSms",
            Country_ID: country_id.to_string(),
            Phone_Num: phone_num.to_string(),
            Phone_SmsContent: fallback_content.clone(),
        };

        log::info!(
            "[火狐狸平台] UploadSms pure-digit fallback retry: phone={}, original={:?}, fallback={:?}",
            phone_num,
            sms_content,
            fallback_content
        );

        let retry_resp: anyhow::Result<ApiResponse> = do_post(client, api_key, &fallback_payload).await;
        match &retry_resp {
            Ok(r) => log::info!(
                "[火狐狸平台] UploadSms fallback response: phone={}, code={}, data={:?}",
                phone_num,
                r.code,
                r.data
            ),
            Err(e) => log::warn!(
                "[火狐狸平台] UploadSms fallback failed: phone={}, error={}",
                phone_num,
                e
            ),
        }

        if let Ok(r) = &retry_resp {
            if r.code == "1" {
                crate::sound_player::play_sms_upload_success_sound();
            }
        }

        return retry_resp;
    }

    if first_ok.code == "1" {
        crate::sound_player::play_sms_upload_success_sound();
    }

    Ok(first_ok)
}

#[cfg(test)]
mod worldcode_tests {
    use super::*;

    #[test]
    fn worldcode_seller_requests_encode_keys_and_keep_documented_payloads() {
        let client = reqwest::Client::new();
        let payload = PhoneAddBatchRequest {
            act: "PhoneAddBatch",
            PhoneList: vec![PhoneAddEntry {
                Country_ID: "vgb", Phone_Num: "1234567", Phone_ComName: "", Provi_Name: "", City_Name: "",
            }],
        };
        let request = seller_request(&client, reqwest::Method::POST, "test&key=+#value")
            .json(&payload).build().unwrap();
        assert_eq!(request.url().scheme(), "https");
        assert_eq!(request.url().host_str(), Some("www.worldcode.win"));
        assert_eq!(request.url().path(), "/ksapi.ashx");
        assert_eq!(request.url().query_pairs().collect::<Vec<_>>(), vec![("key".into(), "test&key=+#value".into())]);
        let body: serde_json::Value = serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"act":"PhoneAddBatch", "PhoneList":[{
            "Country_ID":"vgb", "Phone_Num":"1234567", "Phone_ComName":"", "Provi_Name":"", "City_Name":""
        }]}));
        let get = seller_request(&client, reqwest::Method::GET, "test-key")
            .query(&[("act", "GetWaitPhoneList")]).build().unwrap();
        assert!(get.url().query_pairs().any(|(name, value)| name == "act" && value == "GetWaitPhoneList"));
        assert_eq!(API_KEY_SETTING, "worldcode_api_key");
    }

    #[test]
    fn worldcode_price_catalog_is_public_and_key_is_a_keyword() {
        let request = user_catalog_request(&reqwest::Client::new(), Some(" WhatsApp & Telegram "))
            .build().unwrap();
        assert_eq!(request.url().host_str(), Some("www.worldcode.win"));
        assert_eq!(request.url().path(), "/yhapi.ashx");
        assert_eq!(request.url().query_pairs().collect::<Vec<_>>(), vec![
            ("act".into(), "getItem".into()), ("key".into(), "WhatsApp & Telegram".into()),
        ]);
    }

    #[test]
    fn worldcode_catalog_accepts_string_and_numeric_prices() {
        for payload in [
            r#"{"Item_ID":"1000","Item_Name":"WhatsApp","Item_UPrice":"0.25","Country_ID":"eng"}"#,
            r#"{"Item_ID":1000,"Item_Name":"WhatsApp","Item_UPrice":0.25,"Country_ID":"eng"}"#,
        ] {
            let price: UserItemPrice = serde_json::from_str(payload).unwrap();
            assert_eq!(price.item_id, "1000");
            assert_eq!(price.item_uprice, "0.25");
        }
    }

    #[test]
    fn worldcode_country_catalog_includes_british_virgin_islands() {
        let country = countries().into_iter().find(|country| country.id == "vgb").unwrap();
        assert_eq!(country.prefix, "+1284");
        assert_eq!(country.name, "British Virgin Islands");
        let unique: std::collections::HashSet<_> = COUNTRIES.iter().map(|(id, _, _)| id).collect();
        assert_eq!(unique.len(), COUNTRIES.len());
    }
}
