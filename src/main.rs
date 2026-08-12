mod audit;
mod claims;
mod device;
mod error;
mod paths;
mod refresh;
mod relay;
mod server;
mod sse;
mod storage;

use std::{ffi::OsString, process::ExitCode};

use chrono::{DateTime, SecondsFormat, Utc};

use error::{Error, Result};
use paths::Paths;
use storage::Lock;

const USAGE: &str = "usage: c2a <login|status|logout|serve [SOCKET]>";

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
        [help, command]
            if help == "help"
                && matches!(
                    command.to_str(),
                    Some("login" | "status" | "logout" | "serve")
                ) =>
        {
            println!("{USAGE}");
            Ok(())
        }
        [command, value]
            if value == "--help"
                && matches!(
                    command.to_str(),
                    Some("login" | "status" | "logout" | "serve")
                ) =>
        {
            println!("{USAGE}");
            Ok(())
        }
        [command] if command == "login" => login().await.map_err(Into::into),
        [command] if command == "status" => status().map_err(Into::into),
        [command] if command == "logout" => logout().map_err(Into::into),
        [command] if command == "serve" => serve(None).await.map_err(Into::into),
        [command, socket] if command == "serve" => {
            serve(Some(server::socket_argument(socket.clone())))
                .await
                .map_err(Into::into)
        }
        _ => Err(MainError::Usage("invalid command".into())),
    }
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
async fn login() -> Result<()> {
    let paths = Paths::new()?;
    let _lock = Lock::acquire(&paths)?;
    let credentials = device::login(&client()?).await?;
    storage::store(&paths, &credentials)?;
    println!("logged in");
    Ok(())
}
fn status() -> Result<()> {
    let paths = Paths::new()?;
    let Some(credentials) = storage::load(&paths)? else {
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
    Ok(())
}

fn format_expiry(expiry: Option<i64>) -> String {
    expiry
        .and_then(|value| DateTime::<Utc>::from_timestamp(value, 0))
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_else(|| "unknown".into())
}
fn logout() -> Result<()> {
    let paths = Paths::new()?;
    let _lock = Lock::acquire(&paths)?;
    storage::remove(&paths)?;
    println!("logged out");
    Ok(())
}
async fn serve(socket: Option<std::path::PathBuf>) -> Result<()> {
    let paths = Paths::new()?;
    paths.ensure_dir()?;
    let audit = audit::open(&paths.audit)?;
    let relay = relay::Relay::new(paths, audit)?;
    server::serve(relay, socket).await
}
