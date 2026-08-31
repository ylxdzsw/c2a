use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, de};
use sha2::{Digest, Sha256};

use crate::{
    claims,
    error::{Error, Result},
    provider::USER_AGENT,
    storage::CodexCredentials,
};

pub const AUTH_BASE: &str = "https://auth.openai.com";
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const UPSTREAM: &str = "https://chatgpt.com/backend-api/codex/responses";
pub const QUOTA_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
pub const ORIGINATOR: &str = "c2a";
pub const SESSION_ID_HEADER: &str = "session-id";
pub const ROUTING_HINT_HEADER: &str = "x-codex-routing-hint";

const RESPONSE_LIMIT: usize = 64 * 1024;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Deserialize)]
pub struct Quota {
    pub rate_limit: Option<RateLimit>,
    pub additional_rate_limits: Option<Vec<AdditionalRateLimit>>,
}

#[derive(Debug, Deserialize)]
pub struct AdditionalRateLimit {
    pub limit_name: String,
    pub rate_limit: Option<RateLimit>,
}

#[derive(Debug, Deserialize)]
pub struct RateLimit {
    pub primary_window: Option<RateLimitWindow>,
    pub secondary_window: Option<RateLimitWindow>,
}

#[derive(Debug, Deserialize)]
pub struct RateLimitWindow {
    pub used_percent: f64,
    pub limit_window_seconds: i64,
    pub reset_at: i64,
}

#[derive(Debug, Deserialize)]
struct DeviceCode {
    device_auth_id: String,
    #[serde(alias = "usercode")]
    user_code: String,
    #[serde(deserialize_with = "deserialize_interval")]
    interval: u64,
}

#[derive(Debug, Deserialize)]
struct AuthorizationCode {
    authorization_code: String,
    code_challenge: String,
    code_verifier: String,
}

#[derive(Serialize)]
struct DeviceCodeRequest<'a> {
    client_id: &'a str,
}

#[derive(Serialize)]
struct PollRequest<'a> {
    device_auth_id: &'a str,
    user_code: &'a str,
}

pub async fn login(client: &reqwest::Client) -> Result<CodexCredentials> {
    let response = client
        .post(format!("{AUTH_BASE}/api/accounts/deviceauth/usercode"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&DeviceCodeRequest {
            client_id: CLIENT_ID,
        })?)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "device code request failed with status {}",
            response.status().as_u16()
        )));
    }
    let code: DeviceCode = decode(response).await?;
    println!("Open {AUTH_BASE}/codex/device and enter {}", code.user_code);

    let deadline = tokio::time::Instant::now() + LOGIN_TIMEOUT;
    let authorization = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::message("device login timed out"));
        }
        let response = client
            .post(format!("{AUTH_BASE}/api/accounts/deviceauth/token"))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&PollRequest {
                device_auth_id: &code.device_auth_id,
                user_code: &code.user_code,
            })?)
            .send()
            .await?;
        if response.status().is_success() {
            break decode(response).await?;
        }
        if !matches!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::NOT_FOUND
        ) {
            return Err(Error::message(format!(
                "device login failed with status {}",
                response.status().as_u16()
            )));
        }
        tokio::time::sleep(
            Duration::from_secs(code.interval.max(1))
                .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        )
        .await;
    };
    verify_pkce(&authorization)?;

    let response = client
        .post(format!("{AUTH_BASE}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", CLIENT_ID),
            ("code", authorization.authorization_code.as_str()),
            (
                "redirect_uri",
                "https://auth.openai.com/deviceauth/callback",
            ),
            ("code_verifier", authorization.code_verifier.as_str()),
        ])
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "token exchange failed with status {}",
            response.status().as_u16()
        )));
    }
    let token: serde_json::Value = decode(response).await?;
    let access = token
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::message("login response did not contain an access token"))?;
    let refresh = token
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::message("login response did not contain a refresh token"))?;
    claims::decode(access)?;
    Ok(CodexCredentials {
        version: 1,
        access_token: access.to_owned(),
        refresh_token: refresh.to_owned(),
    })
}

pub async fn quota(
    client: &reqwest::Client,
    access_token: &str,
    account_id: &str,
) -> Result<Quota> {
    let response = client
        .get(QUOTA_URL)
        .bearer_auth(access_token)
        .header("ChatGPT-Account-Id", account_id)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "Codex quota request failed with status {}",
            response.status().as_u16()
        )));
    }
    decode(response).await
}

fn verify_pkce(code: &AuthorizationCode) -> Result<()> {
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code.code_verifier.as_bytes()));
    if challenge != code.code_challenge {
        return Err(Error::message("device login returned invalid PKCE state"));
    }
    Ok(())
}

fn deserialize_interval<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(value) => value.trim().parse().map_err(de::Error::custom),
        serde_json::Value::Number(value) => value
            .as_u64()
            .ok_or_else(|| de::Error::custom("invalid polling interval")),
        _ => Err(de::Error::custom("invalid polling interval")),
    }
}

pub(crate) async fn decode<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > RESPONSE_LIMIT {
            return Err(Error::message("upstream JSON response exceeds 64 KiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::Quota;

    #[test]
    fn quota_response_preserves_windows_and_additional_limits() {
        let quota: Quota = serde_json::from_value(serde_json::json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 20,
                    "limit_window_seconds": 18000,
                    "reset_after_seconds": 60,
                    "reset_at": 123
                },
                "secondary_window": null
            },
            "additional_rate_limits": [{
                "limit_name": "code review",
                "metered_feature": "code_review",
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 30,
                        "limit_window_seconds": 604800,
                        "reset_after_seconds": 120,
                        "reset_at": 456
                    },
                    "secondary_window": null
                }
            }]
        }))
        .unwrap();

        let primary = quota.rate_limit.unwrap().primary_window.unwrap();
        assert_eq!(primary.used_percent, 20.0);
        assert_eq!(primary.limit_window_seconds, 18_000);
        assert_eq!(primary.reset_at, 123);
        let additional = &quota.additional_rate_limits.unwrap()[0];
        assert_eq!(additional.limit_name, "code review");
        assert_eq!(
            additional
                .rate_limit
                .as_ref()
                .unwrap()
                .primary_window
                .as_ref()
                .unwrap()
                .reset_at,
            456
        );
    }
}
