mod audit;
mod claims;
mod copilot;
mod device;
mod error;
mod paths;
mod provider;
mod refresh;
mod relay;
mod server;
mod sse;
mod storage;

use std::{ffi::OsString, process::ExitCode};

use chrono::{DateTime, SecondsFormat, Utc};

use error::{Error, Result};
use paths::Paths;
use provider::Provider;

const USAGE: &str = "usage: c2a <codex|copilot> <login|status|quota|logout|serve [SOCKET]>";

fn main() -> ExitCode {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    match runtime.block_on(run(std::env::args_os().collect())) {
        Ok(()) => ExitCode::SUCCESS,
        Err(MainError::Usage(message)) => {
            eprintln!("{message}\n{USAGE}");
            ExitCode::from(2)
        }
        Err(MainError::App(error)) => {
            eprintln!("c2a: {error}");
            ExitCode::FAILURE
        }
    }
}

enum MainError {
    Usage(String),
    App(Error),
}
impl From<Error> for MainError {
    fn from(value: Error) -> Self {
        Self::App(value)
    }
}

async fn run(args: Vec<OsString>) -> std::result::Result<(), MainError> {
    let tail = args.get(1..).unwrap_or(&[]);
    match tail {
        [value] if value == "--version" => {
            println!("c2a {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [value] if value == "help" || value == "--help" => {
            println!("{USAGE}");
            Ok(())
        }
        [help, provider] if help == "help" && Provider::parse(provider).is_some() => {
            println!("{USAGE}");
            Ok(())
        }
        [provider, value] if value == "--help" && Provider::parse(provider).is_some() => {
            println!("{USAGE}");
            Ok(())
        }
        [provider, command] if command == "login" => {
            login(parse_provider(provider)?).await.map_err(Into::into)
        }
        [provider, command] if command == "status" => {
            status(parse_provider(provider)?).await.map_err(Into::into)
        }
        [provider, command] if command == "quota" => {
            quota(parse_provider(provider)?).await.map_err(Into::into)
        }
        [provider, command] if command == "logout" => {
            logout(parse_provider(provider)?).map_err(Into::into)
        }
        [provider, command] if command == "serve" => serve(parse_provider(provider)?, None)
            .await
            .map_err(Into::into),
        [provider, command, socket] if command == "serve" => serve(
            parse_provider(provider)?,
            Some(server::socket_argument(socket.clone())),
        )
        .await
        .map_err(Into::into),
        _ => Err(MainError::Usage("invalid command".into())),
    }
}

fn parse_provider(value: &std::ffi::OsStr) -> std::result::Result<Provider, MainError> {
    Provider::parse(value).ok_or_else(|| MainError::Usage("invalid provider".into()))
}

fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(30))
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .referer(false)
        .build()?)
}
async fn login(provider: Provider) -> Result<()> {
    let paths = Paths::new()?;
    match provider {
        Provider::Codex => {
            let credentials = device::login(&client()?).await?;
            storage::store_codex(&paths, &credentials)?;
        }
        Provider::Copilot => {
            let credentials = copilot::login(&client()?).await?;
            storage::store_copilot(&paths, &credentials)?;
        }
    }
    println!("logged in");
    Ok(())
}
async fn status(provider: Provider) -> Result<()> {
    let paths = Paths::new()?;
    match provider {
        Provider::Codex => {
            let Some(credentials) = storage::load_codex(&paths)? else {
                println!("not logged in");
                return Ok(());
            };
            let claims = claims::decode(&credentials.access_token)?;
            println!(
                "logged in\naccount: {}\nemail: {}\nplan: {}\nexpires: {}",
                claims.account_id,
                claims.email.as_deref().unwrap_or("unknown"),
                claims.plan.as_deref().unwrap_or("unknown"),
                format_expiry(claims.expiry)
            );
        }
        Provider::Copilot => {
            let Some(status) = copilot::status(&client()?, &paths).await? else {
                println!("not logged in");
                return Ok(());
            };
            println!(
                "logged in\naccount: {}\nplan: {}\nendpoint: {}\nexpires: {}\nrefresh expires: {}",
                status.login,
                status.sku.as_deref().unwrap_or("unknown"),
                status.endpoint,
                format_expiry(status.expires_at),
                format_expiry(status.refresh_expires_at)
            );
        }
    }
    Ok(())
}

async fn quota(provider: Provider) -> Result<()> {
    let paths = Paths::new()?;
    let client = client()?;
    let output = match provider {
        Provider::Codex => {
            let (credentials, claims) = refresh::credentials(&client, &paths, None).await?;
            let quota =
                device::quota(&client, &credentials.access_token, &claims.account_id).await?;
            format_codex_quota(&quota)?
        }
        Provider::Copilot => {
            let credentials = copilot::credentials(&client, &paths, None).await?;
            let quota = copilot::quota(&client, &credentials.access_token).await?;
            format_copilot_quota(&quota)?
        }
    };
    println!("{output}");
    Ok(())
}

fn format_codex_quota(quota: &device::Quota) -> Result<String> {
    let mut lines = Vec::new();
    if let Some(limit) = &quota.rate_limit {
        append_codex_limit(&mut lines, None, limit);
    }
    for additional in quota.additional_rate_limits.as_deref().unwrap_or_default() {
        if let Some(limit) = &additional.rate_limit {
            append_codex_limit(&mut lines, Some(&additional.limit_name), limit);
        }
    }
    if lines.is_empty() {
        return Err(Error::message("Codex returned no quota data"));
    }
    Ok(lines.join("\n"))
}

fn append_codex_limit(lines: &mut Vec<String>, name: Option<&str>, limit: &device::RateLimit) {
    for (fallback, window) in [
        ("primary", limit.primary_window.as_ref()),
        ("secondary", limit.secondary_window.as_ref()),
    ] {
        let Some(window) = window else { continue };
        let label = window_label(window.limit_window_seconds, fallback);
        let label = name
            .filter(|value| !value.is_empty())
            .map(|name| format!("{name} {label}"))
            .unwrap_or(label);
        lines.push(format!(
            "{label}: {}% remaining",
            format_number((100.0 - window.used_percent).clamp(0.0, 100.0))
        ));
        lines.push(format!("resets: {}", format_expiry(Some(window.reset_at))));
    }
}

fn format_copilot_quota(quota: &copilot::Quota) -> Result<String> {
    let mut lines = Vec::new();
    for key in ["premium_interactions", "chat", "completions"] {
        let Some(snapshot) = quota.snapshots.get(key) else {
            continue;
        };
        let label = match key {
            "premium_interactions"
                if quota.token_based_billing || snapshot.token_based_billing.unwrap_or(false) =>
            {
                "AI credits"
            }
            "premium_interactions" => "premium requests",
            "chat" => "chat",
            "completions" => "completions",
            _ => unreachable!(),
        };
        if snapshot.unlimited {
            lines.push(format!("{label}: unlimited"));
            continue;
        }
        let remaining = snapshot.quota_remaining.or(snapshot.remaining);
        let value = match (remaining, snapshot.entitlement, snapshot.percent_remaining) {
            (Some(remaining), Some(entitlement), Some(percent)) => format!(
                "{} of {} remaining ({}%)",
                format_number(remaining),
                format_number(entitlement),
                format_number(percent.clamp(0.0, 100.0))
            ),
            (Some(remaining), Some(entitlement), None) => format!(
                "{} of {} remaining",
                format_number(remaining),
                format_number(entitlement)
            ),
            (_, _, Some(percent)) => {
                format!("{}% remaining", format_number(percent.clamp(0.0, 100.0)))
            }
            _ => continue,
        };
        lines.push(format!("{label}: {value}"));
    }
    if lines.is_empty() {
        return Err(Error::message("Copilot returned no quota data"));
    }
    if let Some(reset_at) = &quota.reset_at {
        lines.push(format!("resets: {reset_at}"));
    }
    Ok(lines.join("\n"))
}

fn window_label(seconds: i64, fallback: &str) -> String {
    match seconds {
        18_000 => "5h".into(),
        86_400 => "daily".into(),
        604_800 => "weekly".into(),
        2_592_000 => "monthly".into(),
        value if value > 0 && value % 86_400 == 0 => format!("{}d", value / 86_400),
        value if value > 0 && value % 3_600 == 0 => format!("{}h", value / 3_600),
        _ => fallback.into(),
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn format_expiry(expiry: Option<i64>) -> String {
    expiry
        .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_else(|| "unknown".into())
}
fn logout(provider: Provider) -> Result<()> {
    let paths = Paths::new()?;
    storage::clear(&paths, provider)?;
    println!("logged out");
    Ok(())
}
async fn serve(provider: Provider, socket: Option<std::path::PathBuf>) -> Result<()> {
    let paths = Paths::new()?;
    paths.ensure_dir()?;
    let audit = audit::open(&paths.audit)?;
    let relay = relay::Relay::new(provider, paths, audit)?;
    server::serve(relay, socket).await
}
