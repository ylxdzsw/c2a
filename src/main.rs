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

const USAGE: &str = "usage: c2a <codex|copilot> <login|status|logout|serve [SOCKET]>";

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
