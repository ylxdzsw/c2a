use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, de};
use sha2::{Digest, Sha256};

use crate::{
    audit::RequestAudit,
    claims,
    error::{Error, Result},
    provider::USER_AGENT,
    storage::CodexCredentials,
};

pub const AUTH_BASE: &str = "https://auth.openai.com";
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const UPSTREAM: &str = "https://chatgpt.com/backend-api/codex/responses";
// Native JSON Images protocol: openai/codex 5c5308fc9a9e (2026-09-20),
// codex-api/src/{images.rs,endpoint/images.rs}. No multipart or SSE adapter.
pub const IMAGE_GENERATIONS: &str = "https://chatgpt.com/backend-api/codex/images/generations";
pub const IMAGE_EDITS: &str = "https://chatgpt.com/backend-api/codex/images/edits";
pub const QUOTA_URL: &str = "https://chatgpt.com/backend-api/wham/usage";
pub const SESSION_ID_HEADER: &str = "session-id";
pub const ROUTING_HINT_HEADER: &str = "x-codex-routing-hint";

const RESPONSE_LIMIT: usize = 64 * 1024;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Deserialize, Serialize)]
pub struct Quota {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_rate_limits: Option<Vec<AdditionalRateLimit>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AdditionalRateLimit {
    pub limit_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimit>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RateLimit {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_window: Option<RateLimitWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_window: Option<RateLimitWindow>,
}

#[derive(Debug, Deserialize, Serialize)]
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
    audit: &mut RequestAudit,
) -> Result<Quota> {
    let response = client
        .get(QUOTA_URL)
        .bearer_auth(access_token)
        .header("ChatGPT-Account-Id", account_id)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .send()
        .await?;
    decode_quota(response, audit).await
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
    response: reqwest::Response,
) -> Result<T> {
    decode_inner(response, None).await
}

pub(crate) async fn decode_quota<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    audit: &mut RequestAudit,
) -> Result<T> {
    crate::relay::record_upstream(&response, &mut audit.record);
    if !response.status().is_success() {
        audit.record.outcome = "upstream_error".into();
        return Err(Error::message(format!(
            "{} quota request failed with status {}",
            audit.record.provider,
            response.status().as_u16()
        )));
    }
    decode_inner(response, Some(audit)).await
}

async fn decode_inner<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
    mut audit: Option<&mut RequestAudit>,
) -> Result<T> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if let Some(audit) = audit.as_deref_mut() {
            audit.record.response_bytes += chunk.len() as u64;
            if !chunk.is_empty() {
                let elapsed = audit.elapsed_ms();
                audit.record.first_upstream_byte_ms.get_or_insert(elapsed);
            }
        }
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
            "account_id": "not-audited",
            "plan_type": "not-audited",
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

        let record = crate::audit::AuditRecord {
            operation: "quota".into(),
            quota: Some(serde_json::to_value(&quota).unwrap()),
            ..Default::default()
        };
        let value = serde_json::to_value(record).unwrap();
        assert_eq!(value["operation"], "quota");
        assert_eq!(
            value["quota"],
            serde_json::json!({
                "rate_limit": {"primary_window": {
                    "used_percent": 20.0, "limit_window_seconds": 18000, "reset_at": 123
                }},
                "additional_rate_limits": [{
                    "limit_name": "code review",
                    "rate_limit": {"primary_window": {
                        "used_percent": 30.0, "limit_window_seconds": 604800, "reset_at": 456
                    }}
                }]
            })
        );

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

    #[tokio::test]
    async fn quota_audit_observes_success_http_errors_and_invalid_json() {
        let mut random = [0; 16];
        getrandom::fill(&mut random).unwrap();
        let path =
            std::env::temp_dir().join(format!("c2a-quota-{:x}", u128::from_ne_bytes(random)));
        let file = crate::audit::open(&path).unwrap();
        for (status, body, outcome) in [
            (200, r#"{"rate_limit":null}"#, "completed"),
            (429, "not persisted", "upstream_error"),
            (200, "invalid JSON", "failed"),
        ] {
            let mut audit = crate::audit::RequestAudit::new(file.clone(), "codex", "query".into());
            audit.record.operation = "quota".into();
            let response = hyper::Response::builder()
                .status(status)
                .header("x-request-id", "upstream-query")
                .body(body)
                .unwrap();
            let result: crate::error::Result<Quota> =
                super::decode_quota(response.into(), &mut audit).await;
            if result.is_ok() {
                audit.record.outcome = "completed".into();
            }
            assert_eq!(audit.record.outcome, outcome);
            assert_eq!(audit.record.upstream_http_status, Some(status));
            assert_eq!(
                audit.record.upstream_request_id.as_deref(),
                Some("upstream-query")
            );
            assert_eq!(
                audit.record.response_bytes,
                if status == 200 { body.len() as u64 } else { 0 }
            );
            audit.finish().await;
        }
        let records = std::fs::read_to_string(&path).unwrap();
        assert_eq!(records.lines().count(), 3);
        assert!(!records.contains("not persisted"));
        assert!(!records.contains("invalid JSON"));
        std::fs::remove_file(path).unwrap();
    }
}
