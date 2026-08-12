use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, de};
use sha2::{Digest, Sha256};

use crate::{
    claims,
    error::{Error, Result},
    storage::Credentials,
};

pub const AUTH_BASE: &str = "https://auth.openai.com";
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const UPSTREAM: &str = "https://chatgpt.com/backend-api/codex/responses";
pub const ORIGINATOR: &str = "c2a";
pub const USER_AGENT: &str = concat!("c2a/", env!("CARGO_PKG_VERSION"));

const OAUTH_LIMIT: usize = 64 * 1024;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15 * 60);

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

pub async fn login(client: &reqwest::Client) -> Result<Credentials> {
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
    Ok(Credentials {
        version: 1,
        access_token: access.to_owned(),
        refresh_token: refresh.to_owned(),
    })
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
        if bytes.len() + chunk.len() > OAUTH_LIMIT {
            return Err(Error::message("OAuth response exceeds 64 KiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
