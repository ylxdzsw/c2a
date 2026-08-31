use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;

use crate::{
    device,
    error::{Error, Result},
    paths::Paths,
    provider::{Provider, USER_AGENT},
    storage::{self, CopilotCredentials},
};

// GitHub currently gates Copilot model capability by OAuth application. Borrow
// OpenCode's public client ID only for authorization; every API request still
// identifies this integration with c2a's own User-Agent.
pub const CLIENT_ID: &str = "Ov23li8tweQw6odWQebz";
pub const API_VERSION: &str = "2026-06-01";

const DEVICE_CODE_URL: &str = "https://github.com/login/device/code";
const ACCESS_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const USER_URL: &str = "https://api.github.com/user";
const DISCOVERY_URL: &str = "https://api.github.com/copilot_internal/user";
const REFRESH_MARGIN: i64 = 300;
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Deserialize)]
struct DeviceCode {
    device_code: String,
    user_code: String,
    verification_uri: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Debug, Default, Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    expires_in: Option<u64>,
    refresh_token: Option<String>,
    refresh_token_expires_in: Option<u64>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubUser {
    login: String,
}

#[derive(Debug, Deserialize)]
struct DiscoveryResponse {
    endpoints: DiscoveryEndpoints,
    sku: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryEndpoints {
    api: String,
}

#[derive(Debug, Deserialize)]
struct QuotaResponse {
    quota_reset_date: Option<String>,
    quota_reset_date_utc: Option<String>,
    token_based_billing: Option<bool>,
    quota_snapshots: Option<BTreeMap<String, QuotaSnapshot>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct QuotaSnapshot {
    pub entitlement: Option<f64>,
    pub quota_remaining: Option<f64>,
    pub remaining: Option<f64>,
    pub percent_remaining: Option<f64>,
    #[serde(default)]
    pub unlimited: bool,
    pub token_based_billing: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct Discovery {
    pub endpoint: String,
    pub sku: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Status {
    pub login: String,
    pub endpoint: String,
    pub sku: Option<String>,
    pub expires_at: Option<i64>,
    pub refresh_expires_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Quota {
    pub reset_at: Option<String>,
    pub token_based_billing: bool,
    pub snapshots: BTreeMap<String, QuotaSnapshot>,
}

pub async fn login(client: &reqwest::Client) -> Result<CopilotCredentials> {
    let response = client
        .post(DEVICE_CODE_URL)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .form(&[("client_id", CLIENT_ID), ("scope", "read:user")])
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "GitHub device code request failed with status {}",
            response.status().as_u16()
        )));
    }
    let code: DeviceCode = device::decode(response).await?;
    if code.device_code.is_empty()
        || code.user_code.is_empty()
        || code.expires_in == 0
        || !code.verification_uri.starts_with("https://")
    {
        return Err(Error::message("GitHub returned an invalid device code"));
    }
    println!(
        "Open {} and enter {}",
        code.verification_uri, code.user_code
    );

    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(code.expires_in).min(LOGIN_TIMEOUT);
    let mut interval = Duration::from_secs(code.interval.max(1));
    let token = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::message("GitHub device login timed out"));
        }
        tokio::time::sleep(
            interval.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        )
        .await;
        let response = client
            .post(ACCESS_TOKEN_URL)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .form(&[
                ("client_id", CLIENT_ID),
                ("device_code", code.device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(Error::message(format!(
                "GitHub device login failed with status {}",
                response.status().as_u16()
            )));
        }
        let token: TokenResponse = device::decode(response).await?;
        if token
            .access_token
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        {
            break token;
        }
        match token.error.as_deref() {
            Some("authorization_pending") => {}
            Some("slow_down") => interval += Duration::from_secs(5),
            Some("expired_token") => {
                return Err(Error::message("GitHub device code expired"));
            }
            Some("access_denied") => {
                return Err(Error::message("GitHub device login was denied"));
            }
            Some(error) => {
                let detail = token
                    .error_description
                    .as_deref()
                    .map(|value| format!(": {value}"))
                    .unwrap_or_default();
                return Err(Error::message(format!(
                    "GitHub device login failed: {error}{detail}"
                )));
            }
            None => return Err(Error::message("GitHub returned an invalid token response")),
        }
    };
    let credentials = credentials_from_token(token, None)?;
    user(client, &credentials.access_token).await?;
    discover(client, &credentials.access_token).await?;
    Ok(credentials)
}

pub async fn status(client: &reqwest::Client, paths: &Paths) -> Result<Option<Status>> {
    if storage::load_copilot(paths)?.is_none() {
        return Ok(None);
    }
    let credentials = credentials(client, paths, None).await?;
    let user = user(client, &credentials.access_token).await?;
    let discovery = discover(client, &credentials.access_token).await?;
    Ok(Some(Status {
        login: user.login,
        endpoint: discovery.endpoint,
        sku: discovery.sku,
        expires_at: credentials.expires_at,
        refresh_expires_at: credentials.refresh_expires_at,
    }))
}

pub async fn credentials(
    client: &reqwest::Client,
    paths: &Paths,
    rejected_access_token: Option<&str>,
) -> Result<CopilotCredentials> {
    let current = storage::load_copilot(paths)?
        .ok_or_else(|| Error::message("not logged in; run `c2a copilot login`"))?;
    if crate::refresh::reusable_access_token(
        &current.access_token,
        current.expires_at,
        rejected_access_token,
        now(),
        REFRESH_MARGIN,
    ) {
        return Ok(current);
    }

    let mut lock = storage::lock(paths, Provider::Copilot)?;
    let latest = lock
        .load_copilot()?
        .ok_or_else(|| Error::message("credentials disappeared"))?;
    if latest.access_token != current.access_token
        && (rejected_access_token.is_none()
            || latest.expires_at.is_none_or(|expiry| expiry > now()))
    {
        return Ok(latest);
    }
    let refresh_token = latest
        .refresh_token
        .as_deref()
        .ok_or_else(|| Error::message("Copilot credentials expired; run `c2a copilot login`"))?;
    if latest
        .refresh_expires_at
        .is_some_and(|expiry| expiry <= now())
    {
        return Err(Error::message(
            "Copilot refresh token expired; run `c2a copilot login`",
        ));
    }

    let response = client
        .post(ACCESS_TOKEN_URL)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .form(&[
            ("client_id", CLIENT_ID),
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ])
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "GitHub token refresh failed with status {}; run `c2a copilot login`",
            response.status().as_u16()
        )));
    }
    let token: TokenResponse = device::decode(response).await?;
    if let Some(error) = token.error.as_deref() {
        return Err(Error::message(format!(
            "GitHub token refresh failed: {error}; run `c2a copilot login`"
        )));
    }
    let updated = credentials_from_token(token, Some(&latest))?;
    lock.store(&updated)?;
    Ok(updated)
}

pub async fn discover(client: &reqwest::Client, access_token: &str) -> Result<Discovery> {
    let response = github_get(client, DISCOVERY_URL, access_token)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "Copilot endpoint discovery failed with status {}",
            response.status().as_u16()
        )));
    }
    let value: DiscoveryResponse = device::decode(response).await?;
    let endpoint = validate_endpoint(&value.endpoints.api)?;
    Ok(Discovery {
        endpoint,
        sku: value.sku,
    })
}

pub async fn quota(client: &reqwest::Client, access_token: &str) -> Result<Quota> {
    let response = github_get(client, DISCOVERY_URL, access_token)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "Copilot quota request failed with status {}",
            response.status().as_u16()
        )));
    }
    let value: QuotaResponse = device::decode(response).await?;
    Ok(Quota {
        reset_at: value.quota_reset_date_utc.or(value.quota_reset_date),
        token_based_billing: value.token_based_billing.unwrap_or(false),
        snapshots: value.quota_snapshots.unwrap_or_default(),
    })
}

async fn user(client: &reqwest::Client, access_token: &str) -> Result<GitHubUser> {
    let response = github_get(client, USER_URL, access_token).send().await?;
    if !response.status().is_success() {
        return Err(Error::message(format!(
            "GitHub user request failed with status {}",
            response.status().as_u16()
        )));
    }
    let user: GitHubUser = device::decode(response).await?;
    if user.login.is_empty() {
        return Err(Error::message(
            "GitHub user response did not contain a login",
        ));
    }
    Ok(user)
}

fn github_get(client: &reqwest::Client, url: &str, access_token: &str) -> reqwest::RequestBuilder {
    client
        .get(url)
        .bearer_auth(access_token)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::USER_AGENT, USER_AGENT)
}

fn credentials_from_token(
    mut token: TokenResponse,
    previous: Option<&CopilotCredentials>,
) -> Result<CopilotCredentials> {
    let access_token = token
        .access_token
        .take()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::message("GitHub response did not contain an access token"))?;
    let issued_at = now();
    let rotated_refresh = token.refresh_token.take().filter(|value| !value.is_empty());
    let refresh_token = rotated_refresh
        .clone()
        .or_else(|| previous.and_then(|value| value.refresh_token.clone()));
    let refresh_expires_at = if rotated_refresh.is_some() {
        token
            .refresh_token_expires_in
            .and_then(|seconds| expiry(issued_at, seconds))
    } else {
        previous.and_then(|value| value.refresh_expires_at)
    };
    Ok(CopilotCredentials {
        version: 1,
        access_token,
        expires_at: token
            .expires_in
            .and_then(|seconds| expiry(issued_at, seconds)),
        refresh_token,
        refresh_expires_at,
    })
}

fn expiry(issued_at: i64, seconds: u64) -> Option<i64> {
    i64::try_from(seconds)
        .ok()
        .and_then(|seconds| issued_at.checked_add(seconds))
}

fn validate_endpoint(value: &str) -> Result<String> {
    let mut url =
        reqwest::Url::parse(value).map_err(|_| Error::message("invalid Copilot API endpoint"))?;
    let host = url
        .host_str()
        .ok_or_else(|| Error::message("invalid Copilot API endpoint"))?;
    if url.scheme() != "https"
        || !(host == "githubcopilot.com" || host.ends_with(".githubcopilot.com"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(Error::message("invalid Copilot API endpoint"));
    }
    url.set_path("");
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::{
        CopilotCredentials, QuotaResponse, TokenResponse, credentials_from_token, validate_endpoint,
    };

    #[test]
    fn quota_response_preserves_fractional_ai_credits() {
        let quota: QuotaResponse = serde_json::from_value(serde_json::json!({
            "quota_reset_date": "2026-09-01",
            "quota_reset_date_utc": "2026-09-01T00:00:00Z",
            "token_based_billing": true,
            "quota_snapshots": {
                "premium_interactions": {
                    "entitlement": 1500,
                    "quota_remaining": 1254.7,
                    "remaining": 1254,
                    "percent_remaining": 83.6,
                    "unlimited": false,
                    "token_based_billing": true
                }
            }
        }))
        .unwrap();

        assert_eq!(quota.token_based_billing, Some(true));
        assert_eq!(
            quota.quota_snapshots.unwrap()["premium_interactions"].quota_remaining,
            Some(1254.7)
        );
        assert_eq!(
            quota.quota_reset_date_utc.as_deref(),
            Some("2026-09-01T00:00:00Z")
        );
    }

    #[test]
    fn token_response_preserves_optional_refresh_credentials() {
        let credentials = credentials_from_token(
            TokenResponse {
                access_token: Some("access".into()),
                expires_in: Some(3600),
                refresh_token: Some("refresh".into()),
                refresh_token_expires_in: Some(7200),
                ..TokenResponse::default()
            },
            None,
        )
        .unwrap();
        assert_eq!(credentials.access_token, "access");
        assert_eq!(credentials.refresh_token.as_deref(), Some("refresh"));
        assert!(credentials.expires_at.is_some());
        assert!(credentials.refresh_expires_at.is_some());
    }

    #[test]
    fn refresh_response_can_retain_an_unrotated_refresh_token() {
        let previous = CopilotCredentials {
            version: 1,
            access_token: "old-access".into(),
            expires_at: Some(1),
            refresh_token: Some("old-refresh".into()),
            refresh_expires_at: Some(i64::MAX),
        };
        let credentials = credentials_from_token(
            TokenResponse {
                access_token: Some("new-access".into()),
                ..TokenResponse::default()
            },
            Some(&previous),
        )
        .unwrap();
        assert_eq!(credentials.access_token, "new-access");
        assert_eq!(credentials.refresh_token.as_deref(), Some("old-refresh"));
        assert_eq!(credentials.refresh_expires_at, Some(i64::MAX));
    }

    #[test]
    fn discovered_endpoint_must_be_an_https_copilot_origin() {
        assert_eq!(
            validate_endpoint("https://api.individual.githubcopilot.com/").unwrap(),
            "https://api.individual.githubcopilot.com"
        );
        for invalid in [
            "http://api.githubcopilot.com",
            "https://example.com",
            "https://api.githubcopilot.com/path",
            "https://user@api.githubcopilot.com",
        ] {
            assert!(validate_endpoint(invalid).is_err(), "{invalid}");
        }
    }
}
