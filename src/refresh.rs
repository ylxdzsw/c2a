use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    claims,
    error::{Error, Result},
    paths::Paths,
    storage::{self, Credentials, Lock},
};

pub async fn credentials(
    client: &reqwest::Client,
    paths: &Paths,
    force: bool,
) -> Result<(Credentials, claims::Claims)> {
    let current =
        storage::load(paths)?.ok_or_else(|| Error::message("not logged in; run `c2a login`"))?;
    let parsed = claims::decode(&current.access_token)?;
    if !force && parsed.expiry.is_none_or(|exp| exp > now() + 300) {
        return Ok((current, parsed));
    }
    let _lock = Lock::acquire(paths)?;
    let latest = storage::load(paths)?.ok_or_else(|| Error::message("credentials disappeared"))?;
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
    let updated = Credentials {
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
    storage::store(paths, &updated)?;
    Ok((updated, parsed))
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}
