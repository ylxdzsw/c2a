use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::Value;

use crate::error::{Error, Result};

#[derive(Clone, Debug, Default)]
pub struct Claims {
    pub account_id: String,
    pub expiry: Option<i64>,
    pub email: Option<String>,
    pub plan: Option<String>,
}

pub fn decode(token: &str) -> Result<Claims> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| Error::message("access token has no claims"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| Error::message("access token claims are invalid"))?;
    let value: Value = serde_json::from_slice(&bytes)?;
    let auth = value
        .get("https://api.openai.com/auth")
        .and_then(Value::as_object);
    let account = auth
        .and_then(|v| v.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .or_else(|| value.get("chatgpt_account_id").and_then(Value::as_str))
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::message("access token has no account ID"))?;
    Ok(Claims {
        account_id: account.to_string(),
        expiry: value.get("exp").and_then(Value::as_i64),
        email: value
            .get("email")
            .and_then(Value::as_str)
            .map(str::to_owned),
        plan: auth
            .and_then(|v| v.get("chatgpt_plan_type"))
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}
