use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    claims,
    error::{Error, Result},
    paths::Paths,
    storage::{self, CodexCredentials},
};

pub async fn credentials(
    client: &reqwest::Client,
    paths: &Paths,
    rejected_access_token: Option<&str>,
) -> Result<(CodexCredentials, claims::Claims)> {
    let current = storage::load_codex(paths)?
        .ok_or_else(|| Error::message("not logged in; run `c2a codex login`"))?;
    let parsed = claims::decode(&current.access_token)?;
    if reusable_access_token(
        &current.access_token,
        parsed.expiry,
        rejected_access_token,
        now(),
        300,
    ) {
        return Ok((current, parsed));
    }
    let mut lock = storage::lock(paths, crate::provider::Provider::Codex)?;
    let latest = lock
        .load_codex()?
        .ok_or_else(|| Error::message("credentials disappeared"))?;
    let latest_claims = claims::decode(&latest.access_token)?;
    if latest.access_token != current.access_token {
        return Ok((latest, latest_claims));
    }
    let body = serde_json::to_vec(
        &serde_json::json!({"client_id": crate::device::CLIENT_ID, "grant_type":"refresh_token", "refresh_token":latest.refresh_token}),
    )?;
    let response = client
        .post("https://auth.openai.com/oauth/token")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await?
        .error_for_status()?;
    let value: serde_json::Value = crate::device::decode(response).await?;
    let access = value
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::message("refresh response did not contain an access token"))?;
    let refresh = value
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(&latest.refresh_token);
    let updated = CodexCredentials {
        version: 1,
        access_token: access.to_owned(),
        refresh_token: refresh.to_owned(),
    };
    let parsed = claims::decode(access)?;
    if parsed.account_id != latest_claims.account_id {
        return Err(Error::message(
            "refreshed account does not match the logged-in account",
        ));
    }
    lock.store(&updated)?;
    Ok((updated, parsed))
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

pub(crate) fn reusable_access_token(
    access_token: &str,
    expiry: Option<i64>,
    rejected_access_token: Option<&str>,
    now: i64,
    refresh_margin: i64,
) -> bool {
    match rejected_access_token {
        Some(rejected) => rejected != access_token && expiry.is_none_or(|expiry| expiry > now),
        None => expiry.is_none_or(|expiry| expiry > now + refresh_margin),
    }
}

#[cfg(test)]
mod tests {
    use super::reusable_access_token;

    #[test]
    fn rejected_token_only_forces_refresh_while_it_is_current() {
        assert!(reusable_access_token(
            "replacement",
            Some(200),
            Some("rejected"),
            100,
            30
        ));
        assert!(!reusable_access_token(
            "rejected",
            Some(200),
            Some("rejected"),
            100,
            30
        ));
        assert!(!reusable_access_token(
            "replacement",
            Some(100),
            Some("rejected"),
            100,
            30
        ));
    }

    #[test]
    fn proactive_refresh_keeps_its_margin() {
        assert!(reusable_access_token("access", Some(131), None, 100, 30));
        assert!(!reusable_access_token("access", Some(130), None, 100, 30));
        assert!(reusable_access_token("access", None, None, 100, 30));
    }
}
