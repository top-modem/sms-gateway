use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const API_BASE: &str = "http://ks.firefox.fun/ksapi.ashx";

/// Country list parsed from 火狐狸-API上卡协议文档.pdf.
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

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct PhoneAddBatchRequest<'a> {
    act: &'static str,
    PhoneList: Vec<PhoneEntry<'a>>,
}

#[derive(Debug, Serialize)]
#[allow(non_snake_case)]
struct PhoneEntry<'a> {
    Country_ID: &'a str,
    Phone_Num: &'a str,
    Phone_ComName: &'a str,
    Provi_Name: &'a str,
    City_Name: &'a str,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse {
    pub code: String,
    pub data: Option<String>,
}

/// Upload a batch of phone numbers to the 火狐狸 platform.
/// The platform limits each batch to 50 entries.
pub async fn upload_phone_batch(
    client: &reqwest::Client,
    api_key: &str,
    country_id: &str,
    phone_numbers: &[String],
) -> Result<Vec<ApiResponse>> {
    const BATCH_SIZE: usize = 50;

    let mut results = Vec::new();

    for chunk in phone_numbers.chunks(BATCH_SIZE) {
        let phone_list: Vec<PhoneEntry> = chunk
            .iter()
            .map(|num| PhoneEntry {
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

        let url = format!("{}?key={}", API_BASE, api_key);
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("Failed to send request to 火狐狸 platform")?;

        let status = response.status();
        let text = response
            .text()
            .await
            .context("Failed to read 火狐狸 platform response")?;

        if !status.is_success() {
            anyhow::bail!("火狐狸 platform returned HTTP {}: {}", status, text);
        }

        let parsed: ApiResponse = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse 火狐狸 response: {}", text))?;
        results.push(parsed);
    }

    Ok(results)
}
