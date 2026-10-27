use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, service::service_fn};
use hyper_util::rt::TokioIo;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};

use crate::{
    device,
    error::{Error, Result},
    paths::Paths,
    provider::{Provider, USER_AGENT},
    storage::{self, ChatgptSession, ChatgptState},
};

// SIWC OSS contract, checked 2026-10-23:
// https://developers.openai.com/siwc/token-sharing-open-source/{sign-in,token-reference}
// Manual full-URL handoff also used by CLIProxyAPI d946c4fba6fc,
// internal/api/handlers/management/oauth_siwc.go. Independently implemented here.
const ISSUER: &str = "https://auth.openai.com";
const AUTHORIZE: &str = "https://auth.openai.com/api/accounts/authorize";
const TOKEN: &str = "https://auth.openai.com/api/accounts/oauth/token";
const JWKS: &str = "https://auth.openai.com/.well-known/jwks.json";
const REVOKE: &str = "https://auth.openai.com/api/accounts/oauth/revoke";
const RESOURCE: &str = "https://api.openai.com/v1";
const DYNAMIC_CLIENT: &str = "dynamic_agent_client";
const SHARING_SCOPE: &str = "chatgpt.tokens.use.direct";
const SCOPES: &str =
    "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct";
pub const UPSTREAM: &str = "https://api.openai.com/v1/responses";
// SIWC cache affinity: CLIProxyAPI 1f917e77c456 (2026-10-27),
// internal/runtime/executor/codex_executor_request.go, applyCodexHeadersWithSIWC.
// Unlike the Codex backend's session-id, this header uses an underscore.
// Live gpt-6-luna probe 2026-10-27: three identical 6,981-token requests per
// variant cached 0/0/0 tokens without it versus 0/6,784/6,784 with it.
pub const SESSION_ID_HEADER: &str = "session_id";
pub const USAGE_URL: &str = "https://chatgpt.com/settings/usage";
const CALLBACK_LIMIT: usize = 16 * 1024;

struct Attempt {
    redirect: Url,
    state: String,
    nonce: String,
    verifier: String,
    client_id: Option<String>,
}

struct Callback {
    code: String,
    client_id: String,
}

impl Attempt {
    fn new(port: u16, client_id: Option<String>) -> Result<Self> {
        Ok(Self {
            redirect: Url::parse(&format!("http://127.0.0.1:{port}/auth/callback")).unwrap(),
            state: random()?,
            nonce: random()?,
            verifier: random()?,
            client_id,
        })
    }

    fn authorization_url(&self, host_id: &str) -> Url {
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(self.verifier.as_bytes()));
        let mut url = Url::parse(AUTHORIZE).unwrap();
        url.query_pairs_mut().extend_pairs([
            (
                "client_id",
                self.client_id.as_deref().unwrap_or(DYNAMIC_CLIENT),
            ),
            ("ext_agent_host_id", host_id),
            ("redirect_uri", self.redirect.as_str()),
            ("response_type", "code"),
            ("scope", SCOPES),
            ("resource", RESOURCE),
            ("state", &self.state),
            ("nonce", &self.nonce),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
        ]);
        if self.client_id.is_none() {
            url.query_pairs_mut().append_pair("agent_name_hint", "c2a");
        }
        // No id_token_hint: the printed authorization link must not contain tokens.
        url
    }

    fn callback(&self, raw: &str) -> Result<Option<Callback>> {
        let invalid =
            || Error::message("invalid ChatGPT callback URL; paste the complete final URL");
        if raw.len() > CALLBACK_LIMIT {
            return Err(invalid());
        }
        let raw = raw.trim();
        // iOS can omit the scheme when copying the loopback address. Restore
        // only this literal host prefix; all exact callback checks still apply.
        let normalized = raw
            .starts_with("127.0.0.1:")
            .then(|| format!("http://{raw}"));
        let raw = normalized.as_deref().unwrap_or(raw);
        let url = Url::parse(raw).map_err(|_| invalid())?;
        if raw.split('?').next() != Some(self.redirect.as_str())
            || url.origin() != self.redirect.origin()
            || url.path() != self.redirect.path()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid());
        }
        let mut params = HashMap::new();
        for (key, value) in url.query_pairs() {
            if params
                .insert(key.into_owned(), value.into_owned())
                .is_some()
            {
                return Err(invalid());
            }
        }
        if params.get("state") != Some(&self.state) {
            return Err(Error::message(
                "ChatGPT callback state does not match this login attempt",
            ));
        }
        if params.contains_key("error") {
            if params.contains_key("code") {
                return Err(invalid());
            }
            return Ok(None);
        }
        let code = params
            .remove("code")
            .filter(|v| !v.is_empty())
            .ok_or_else(invalid)?;
        let issued = params.remove("client_id");
        if let (Some(expected), Some(actual)) = (&self.client_id, &issued)
            && expected != actual
        {
            return Err(Error::message(
                "ChatGPT callback changed the registered client",
            ));
        }
        let client_id = issued
            .or_else(|| self.client_id.clone())
            .ok_or_else(invalid)?;
        if client_id == DYNAMIC_CLIENT
            || client_id.is_empty()
            || client_id.len() > 200
            || !client_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(invalid());
        }
        Ok(Some(Callback { code, client_id }))
    }
}

pub async fn login(client: &reqwest::Client, paths: &Paths) -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    // Install handlers before terminal echo is disabled, and keep cancellation
    // active through code exchange and identity verification as well as input.
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    tokio::select! {
        result = login_inner(client, paths) => result,
        _ = interrupt.recv() => Err(Error::message("ChatGPT login cancelled")),
        _ = terminate.recv() => Err(Error::message("ChatGPT login cancelled")),
        _ = hangup.recv() => Err(Error::message("ChatGPT login cancelled")),
    }
}

async fn login_inner(client: &reqwest::Client, paths: &Paths) -> Result<()> {
    let mut saved = {
        let mut lock = storage::lock(paths, Provider::Chatgpt)?;
        if let Some(saved) = lock.load_chatgpt()? {
            saved
        } else {
            let saved = ChatgptState {
                version: 1,
                host_id: host_id()?,
                client_id: None,
                subject: None,
                session: None,
                pending_refresh: None,
            };
            lock.store(&saved)?;
            saved
        }
    };
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let attempt = Attempt::new(listener.local_addr()?.port(), saved.client_id.clone())?;
    let mut authorization = attempt.authorization_url(&saved.host_id);
    if saved.client_id.is_some() && !saved.session.as_ref().is_some_and(sharing) {
        // This is an explicit user login, not automatic reconsent on requests.
        authorization
            .query_pairs_mut()
            .append_pair("prompt", "consent");
    }
    let input = CallbackInput::new()?;
    println!("Continue with ChatGPT: open this link on this or another device:\n{authorization}");
    println!(
        "If the final 127.0.0.1 page cannot load, paste its complete URL here, then press Enter.\nInput is hidden: pasted characters will not appear. Keep this process running; sign-in expires in 15 minutes."
    );
    std::io::stdout().flush()?;
    let callback = tokio::time::timeout(
        Duration::from_secs(900),
        wait_callback(listener, &attempt, input),
    )
    .await
    .map_err(|_| Error::message("ChatGPT login timed out"))??;
    let callback =
        callback.ok_or_else(|| Error::message("ChatGPT authorization was declined or failed"))?;
    {
        let mut lock = storage::lock(paths, Provider::Chatgpt)?;
        let latest = lock
            .load_chatgpt()?
            .ok_or_else(|| Error::message("ChatGPT registration disappeared"))?;
        if latest.client_id != saved.client_id
            || latest.host_id != saved.host_id
            || latest.subject != saved.subject
        {
            return Err(Error::message(
                "ChatGPT registration changed during sign-in; try again",
            ));
        }
        // Retain the issued registration even if the code expires or exchange fails.
        saved = latest;
        saved.client_id = Some(callback.client_id.clone());
        lock.store(&saved)?;
    }
    let reply = request_tokens(
        client,
        &[
            ("grant_type", "authorization_code"),
            ("client_id", &callback.client_id),
            ("code", &callback.code),
            ("code_verifier", &attempt.verifier),
            ("redirect_uri", attempt.redirect.as_str()),
            ("resource", RESOURCE),
        ],
    )
    .await?;
    let TokenReply::Tokens(reply) = reply else {
        return Err(sign_in_required());
    };
    let id_token = reply
        .id_token
        .as_deref()
        .ok_or_else(|| Error::message("ChatGPT did not return an ID token"))?;
    let subject = verify_identity(
        client,
        id_token,
        &callback.client_id,
        Some(&attempt.nonce),
        now(),
    )
    .await
    .map_err(VerificationError::error)?;
    if saved.subject.as_ref().is_some_and(|old| old != &subject) {
        return Err(Error::message(
            "ChatGPT identity does not match this registration",
        ));
    }
    let granted = reply
        .scope
        .as_deref()
        .ok_or_else(|| Error::message("ChatGPT did not confirm granted permissions"))?;
    let session = if granted
        .split_whitespace()
        .any(|scope| scope == SHARING_SCOPE)
    {
        let mut session = reply.session(None)?;
        session.id_token = None;
        Some(session)
    } else {
        None
    };
    let sharing = session.as_ref().is_some_and(sharing);
    let mut lock = storage::lock(paths, Provider::Chatgpt)?;
    let latest = lock
        .load_chatgpt()?
        .ok_or_else(|| Error::message("ChatGPT registration disappeared"))?;
    if latest != saved {
        return Err(Error::message(
            "ChatGPT credentials changed during sign-in; try again",
        ));
    }
    saved.subject = Some(subject);
    saved.session = session;
    saved.pending_refresh = None;
    lock.store(&saved)?;
    println!(
        "ChatGPT plan usage: {}\nManage usage: {USAGE_URL}",
        if sharing { "enabled" } else { "not authorized" }
    );
    Ok(())
}

async fn wait_callback(
    listener: TcpListener,
    attempt: &Attempt,
    mut input: CallbackInput,
) -> Result<Option<Callback>> {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut connections = JoinSet::new();
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            result = receiver.recv() => {
                let _ = tokio::time::timeout(Duration::from_secs(1), async {
                    while connections.join_next().await.is_some() {}
                }).await;
                return result.ok_or_else(|| Error::message("ChatGPT callback listener stopped"));
            }
            _ = poll.tick() => {
                if let Some(raw) = input.read_line()? {
                    if raw.trim().is_empty() { continue; }
                    match attempt.callback(&raw) {
                        Ok(callback) => {
                            eprintln!("Callback received; checking ChatGPT sign-in...");
                            return Ok(callback);
                        }
                        Err(error) => eprintln!("c2a: {error}; try pasting again"),
                    }
                }
            }
            accepted = listener.accept(), if connections.len() < 16 => {
                let (stream, _) = accepted?;
                let sender = sender.clone();
                let expected_host = format!("127.0.0.1:{}", attempt.redirect.port().unwrap());
                let redirect = attempt.redirect.clone();
                let expected_state = attempt.state.clone();
                let client_id = attempt.client_id.clone();
                connections.spawn(async move {
                    let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                        let attempt = Attempt { redirect: redirect.clone(), state: expected_state.clone(), client_id: client_id.clone(), nonce: String::new(), verifier: String::new() };
                        let sender = sender.clone();
                        let host_matches = request.headers().get(hyper::header::HOST).and_then(|v| v.to_str().ok()) == Some(expected_host.as_str());
                        async move {
                            let callback = (request.method() == hyper::Method::GET && host_matches && request.uri().scheme().is_none() && request.uri().path() == "/auth/callback")
                                .then(|| attempt.callback(&format!("{}{}", attempt.redirect.origin().ascii_serialization(), request.uri())))
                                .transpose();
                            let accepted = if let Ok(Some(callback)) = callback { sender.try_send(callback).is_ok() } else { false };
                            let response = Response::builder().status(if accepted { 200 } else { 400 })
                                .header("Content-Type", "text/plain; charset=utf-8")
                                .header("Cache-Control", "no-store")
                                .header("Referrer-Policy", "no-referrer")
                                .header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
                                .body(Full::new(Bytes::from_static(if accepted { b"Return to c2a to finish sign-in. You can close this tab." } else { b"Invalid callback. Return to the browser tab that started sign-in." }))).unwrap();
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                    let _ = tokio::time::timeout(Duration::from_secs(5), hyper::server::conn::http1::Builder::new().keep_alive(false).max_buf_size(CALLBACK_LIMIT).serve_connection(TokioIo::new(stream), service)).await;
                });
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

// Poll stdin without a blocking worker that would keep the runtime alive after
// an automatic callback. Restore terminal echo on success, errors and signals.
struct CallbackInput {
    file: File,
    terminal: Option<libc::termios>,
    bytes: Vec<u8>,
    eof: bool,
}

impl CallbackInput {
    fn new() -> Result<Self> {
        let fd = unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_DUPFD_CLOEXEC, 3) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        Self::from_file(file)
    }

    fn from_file(file: File) -> Result<Self> {
        let fd = file.as_raw_fd();
        let mut original = std::mem::MaybeUninit::uninit();
        let terminal = if unsafe { libc::isatty(fd) } == 1 {
            if unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let original = unsafe { original.assume_init() };
            let mut hidden = original;
            // Canonical mode silently truncates long callback URLs at the
            // kernel's line limit (~4 KiB). Buffer the line in c2a instead.
            hidden.c_lflag &= !(libc::ECHO | libc::ECHONL | libc::ICANON);
            hidden.c_cc[libc::VMIN] = 1;
            hidden.c_cc[libc::VTIME] = 0;
            if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &hidden) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Some(original)
        } else {
            None
        };
        Ok(Self {
            file,
            terminal,
            bytes: Vec::new(),
            eof: false,
        })
    }

    fn read_line(&mut self) -> Result<Option<String>> {
        if let Some(end) = self.bytes.iter().position(|b| *b == b'\n') {
            let line: Vec<_> = self.bytes.drain(..=end).collect();
            return String::from_utf8(line)
                .map(Some)
                .map_err(|_| Error::message("callback URL is not UTF-8"));
        }
        if self.eof {
            return Ok(None);
        }
        let mut poll = libc::pollfd {
            fd: self.file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, 0) };
        if ready < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if ready == 0 {
            return Ok(None);
        }
        let mut bytes = [0; 4096];
        let count = self.file.read(&mut bytes)?;
        if count == 0 {
            self.eof = true;
            if self.bytes.is_empty() {
                return Ok(None);
            }
            self.bytes.push(b'\n');
        } else {
            for &byte in &bytes[..count] {
                match (self.terminal.as_ref(), byte) {
                    (Some(terminal), byte) if byte == terminal.c_cc[libc::VERASE] => {
                        self.bytes.pop();
                    }
                    (Some(terminal), byte) if byte == terminal.c_cc[libc::VKILL] => {
                        self.bytes.clear()
                    }
                    (Some(_), b'\r') => self.bytes.push(b'\n'),
                    _ => self.bytes.push(byte),
                }
            }
        }
        if self.bytes.len() > CALLBACK_LIMIT {
            return Err(Error::message("callback URL exceeds 16 KiB"));
        }
        // Process on the next tick, including input terminated by EOF.
        Ok(None)
    }
}

impl Drop for CallbackInput {
    fn drop(&mut self) {
        if let Some(original) = self.terminal {
            unsafe {
                libc::tcsetattr(self.file.as_raw_fd(), libc::TCSANOW, &original);
            }
            println!();
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    id_token: Option<String>,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    expires_in: i64,
    scope: Option<String>,
    earliest_refresh_at: Option<serde_json::Value>,
}

enum TokenReply {
    Tokens(TokenResponse),
    Reauthenticate,
}

async fn request_tokens(client: &reqwest::Client, fields: &[(&str, &str)]) -> Result<TokenReply> {
    let response = client
        .post(TOKEN)
        .header("User-Agent", USER_AGENT)
        .form(fields)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;
    let status = response.status();
    let body: serde_json::Value = device::decode(response)
        .await
        .map_err(|_| Error::message("invalid ChatGPT OAuth response"))?;
    if !status.is_success() {
        return match body.get("error").and_then(|v| {
            v.as_str()
                .or_else(|| v.get("code").and_then(|v| v.as_str()))
        }) {
            Some(
                "invalid_grant"
                | "invalid_refresh_token"
                | "token_expired"
                | "refresh_token_expired"
                | "refresh_token_invalidated"
                | "refresh_token_reused",
            ) => Ok(TokenReply::Reauthenticate),
            Some("invalid_client") => Err(Error::message(
                "ChatGPT OAuth client registration is invalid",
            )),
            _ => Err(Error::message(format!(
                "ChatGPT OAuth request failed with status {}",
                status.as_u16()
            ))),
        };
    }
    serde_json::from_value(body)
        .map(TokenReply::Tokens)
        .map_err(|_| Error::message("invalid ChatGPT token response"))
}

impl TokenResponse {
    fn session(self, previous: Option<&ChatgptSession>) -> Result<ChatgptSession> {
        let invalid = || Error::message("incomplete ChatGPT token response");
        if self.access_token.is_empty()
            || self.refresh_token.is_empty()
            || !self.token_type.eq_ignore_ascii_case("bearer")
            || self.expires_in <= 0
            || self.id_token.as_deref() == Some("")
        {
            return Err(invalid());
        }
        let received_at = now();
        let expires_at = received_at
            .checked_add(self.expires_in)
            .ok_or_else(invalid)?;
        let scopes = match self.scope {
            Some(scope) => scope.split_whitespace().map(str::to_owned).collect(),
            None => previous.ok_or_else(invalid)?.scopes.clone(),
        };
        let earliest_refresh_at = self
            .earliest_refresh_at
            .map(|value| {
                value
                    .as_i64()
                    .or_else(|| {
                        value.as_str().and_then(|value| {
                            chrono::DateTime::parse_from_rfc3339(value)
                                .ok()
                                .map(|v| v.timestamp())
                        })
                    })
                    .ok_or_else(invalid)
            })
            .transpose()?;
        Ok(ChatgptSession {
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            id_token: self.id_token,
            scopes,
            expires_at,
            earliest_refresh_at,
            received_at,
        })
    }
}

#[derive(Deserialize)]
struct Identity {
    sub: String,
    exp: i64,
    iat: i64,
    nbf: Option<i64>,
    aud: serde_json::Value,
    azp: Option<String>,
    nonce: Option<String>,
}

enum VerificationError {
    Unavailable,
    Invalid,
}

impl VerificationError {
    fn error(self) -> Error {
        Error::message(match self {
            Self::Unavailable => {
                "ChatGPT identity keys are temporarily unavailable; credentials preserved, try again later"
            }
            Self::Invalid => {
                "ChatGPT ID token signature or identity claims are invalid; sign in again"
            }
        })
    }
}

async fn verify_identity(
    client: &reqwest::Client,
    token: &str,
    client_id: &str,
    nonce: Option<&str>,
    received_at: i64,
) -> std::result::Result<String, VerificationError> {
    let response = client
        .get(JWKS)
        .header("User-Agent", USER_AGENT)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| VerificationError::Unavailable)?;
    if !response.status().is_success() {
        return Err(VerificationError::Unavailable);
    }
    let keys: JwkSet = device::decode(response)
        .await
        .map_err(|_| VerificationError::Unavailable)?;
    identity_with_keys(token, client_id, nonce, received_at, &keys)
}

fn identity_with_keys(
    token: &str,
    client_id: &str,
    nonce: Option<&str>,
    received_at: i64,
    keys: &JwkSet,
) -> std::result::Result<String, VerificationError> {
    let header = decode_header(token).map_err(|_| VerificationError::Invalid)?;
    if header.alg != Algorithm::RS256 {
        return Err(VerificationError::Invalid);
    }
    let key = header
        .kid
        .as_deref()
        .and_then(|kid| keys.find(kid))
        .ok_or(VerificationError::Unavailable)?;
    if key
        .common
        .key_algorithm
        .is_some_and(|alg| alg != jsonwebtoken::jwk::KeyAlgorithm::RS256)
        || key
            .common
            .public_key_use
            .as_ref()
            .is_some_and(|usage| *usage != jsonwebtoken::jwk::PublicKeyUse::Signature)
        || key
            .common
            .key_operations
            .as_ref()
            .is_some_and(|ops| !ops.contains(&jsonwebtoken::jwk::KeyOperations::Verify))
    {
        return Err(VerificationError::Invalid);
    }
    let key = DecodingKey::from_jwk(key).map_err(|_| VerificationError::Invalid)?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[ISSUER]);
    validation.set_audience(&[client_id]);
    validation.set_required_spec_claims(&["sub", "exp", "iss", "aud"]);
    // Pending refreshes are checked at receipt time: a JWKS outage must not
    // discard the only replacement refresh token after its ID token expires.
    validation.validate_exp = false;
    let identity = decode::<Identity>(token, &key, &validation)
        .map_err(|_| VerificationError::Invalid)?
        .claims;
    if identity.sub.is_empty()
        || identity.exp < received_at.saturating_sub(5)
        || identity.iat > received_at.saturating_add(5)
        || identity
            .nbf
            .is_some_and(|nbf| nbf > received_at.saturating_add(5))
        || nonce.is_some_and(|expected| identity.nonce.as_deref() != Some(expected))
        || identity.azp.as_deref().is_some_and(|azp| azp != client_id)
        || identity
            .aud
            .as_array()
            .is_some_and(|aud| aud.len() > 1 && identity.azp.as_deref() != Some(client_id))
    {
        return Err(VerificationError::Invalid);
    }
    Ok(identity.sub)
}

pub async fn credentials(
    client: &reqwest::Client,
    paths: &Paths,
    rejected: Option<&str>,
) -> Result<ChatgptSession> {
    let client = client.clone();
    let paths = paths.clone();
    let rejected = rejected.map(str::to_owned);
    let runtime = tokio::runtime::Handle::current();
    // File locks do not block Tokio workers; once rotation starts it must finish
    // persisting even if the downstream request disappears.
    tokio::task::spawn_blocking(move || {
        runtime.block_on(credentials_locked(&client, &paths, rejected.as_deref()))
    })
    .await
    .map_err(|_| Error::message("ChatGPT credential task failed"))?
}

async fn credentials_locked(
    client: &reqwest::Client,
    paths: &Paths,
    rejected: Option<&str>,
) -> Result<ChatgptSession> {
    let mut lock = storage::lock(paths, Provider::Chatgpt)?;
    let mut saved = lock.load_chatgpt()?.ok_or_else(sign_in_required)?;
    let client_id = saved.client_id.clone().ok_or_else(sign_in_required)?;
    if saved.pending_refresh.is_none() {
        let session = saved.session.as_ref().ok_or_else(sign_in_required)?;
        if crate::refresh::reusable_access_token(
            &session.access_token,
            Some(session.expires_at),
            rejected,
            now(),
            60,
        ) {
            return permitted(session.clone());
        }
        if session
            .earliest_refresh_at
            .is_some_and(|earliest| earliest > now())
        {
            return if rejected.is_none() && session.expires_at > now() {
                permitted(session.clone())
            } else {
                Err(Error::message(
                    "ChatGPT token cannot refresh yet; try again later",
                ))
            };
        }
        let reply = request_tokens(
            client,
            &[
                ("grant_type", "refresh_token"),
                ("client_id", &client_id),
                ("refresh_token", &session.refresh_token),
                ("resource", RESOURCE),
            ],
        )
        .await?;
        match reply {
            TokenReply::Reauthenticate => {
                saved.session = None;
                lock.store(&saved)?;
                return Err(sign_in_required());
            }
            TokenReply::Tokens(reply) => {
                let session = reply.session(saved.session.as_ref())?;
                saved.session = None;
                saved.pending_refresh = Some(session);
                lock.store(&saved)?;
            }
        }
    }
    let pending = saved.pending_refresh.as_ref().unwrap();
    if let Some(token) = &pending.id_token {
        match verify_identity(client, token, &client_id, None, pending.received_at).await {
            Ok(subject) if saved.subject.as_deref() == Some(&subject) => {}
            Err(VerificationError::Unavailable) => {
                return Err(VerificationError::Unavailable.error());
            }
            _ => {
                saved.session = None;
                saved.pending_refresh = None;
                lock.store(&saved)?;
                return Err(VerificationError::Invalid.error());
            }
        }
    }
    let mut session = saved.pending_refresh.take().unwrap();
    session.id_token = None;
    saved.session = Some(session.clone());
    lock.store(&saved)?;
    if session.expires_at <= now() {
        return Err(Error::message(
            "ChatGPT session recovered; retry to renew access",
        ));
    }
    permitted(session)
}

pub async fn logout(client: &reqwest::Client, paths: &Paths) -> Result<()> {
    let mut lock = storage::lock(paths, Provider::Chatgpt)?;
    let Some(mut saved) = lock.load_chatgpt()? else {
        return Ok(());
    };
    let mut confirmed = true;
    if let Some(session) = saved.pending_refresh.as_ref().or(saved.session.as_ref()) {
        let response = client
            .post(REVOKE)
            .header("User-Agent", USER_AGENT)
            .form(&[
                ("token", session.refresh_token.as_str()),
                ("token_type_hint", "refresh_token"),
                (
                    "client_id",
                    saved.client_id.as_deref().ok_or_else(sign_in_required)?,
                ),
            ])
            .timeout(Duration::from_secs(30))
            .send()
            .await;
        confirmed = response.is_ok_and(|response| response.status() == reqwest::StatusCode::OK);
    }
    saved.session = None;
    saved.pending_refresh = None;
    lock.store(&saved)?;
    if !confirmed {
        eprintln!(
            "c2a: local tokens cleared, but remote revocation was not confirmed; disconnect c2a in ChatGPT settings"
        );
    }
    Ok(())
}

pub fn sharing(session: &ChatgptSession) -> bool {
    session.scopes.iter().any(|s| s == SHARING_SCOPE)
}

fn permitted(session: ChatgptSession) -> Result<ChatgptSession> {
    if !sharing(&session) {
        return Err(Error::message(
            "ChatGPT plan usage was not authorized; run `c2a chatgpt login` to approve it",
        ));
    }
    Ok(session)
}

fn sign_in_required() -> Error {
    Error::message("ChatGPT session is not usable; run `c2a chatgpt login`")
}

fn random() -> Result<String> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes)
        .map_err(|_| Error::message("could not generate OAuth randomness"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn host_id() -> Result<String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| Error::message("could not generate host ID"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(
        "urn:uuid:{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use std::{fs::File, time::Duration};

    fn query(url: &Url) -> HashMap<String, String> {
        url.query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect()
    }

    #[test]
    fn authorization_url_binds_pkce_and_client_registration() {
        for (client_id, expected_client, has_hint) in [
            (None, DYNAMIC_CLIENT, true),
            (
                Some("registered_client-42".into()),
                "registered_client-42",
                false,
            ),
        ] {
            let attempt = Attempt::new(43127, client_id).unwrap();
            let url = attempt.authorization_url("urn:uuid:test-host");
            let params = query(&url);
            let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(attempt.verifier.as_bytes()));

            assert_eq!(url.as_str().split('?').next(), Some(AUTHORIZE));
            assert_eq!(params["client_id"], expected_client);
            assert_eq!(params["ext_agent_host_id"], "urn:uuid:test-host");
            assert_eq!(params["redirect_uri"], attempt.redirect.as_str());
            assert_eq!(params["response_type"], "code");
            assert_eq!(params["scope"], SCOPES);
            assert_eq!(params["resource"], RESOURCE);
            assert_eq!(params["state"], attempt.state);
            assert_eq!(params["nonce"], attempt.nonce);
            assert_eq!(params["code_challenge"], challenge);
            assert_eq!(params["code_challenge_method"], "S256");
            assert_eq!(params.contains_key("agent_name_hint"), has_hint);
            assert!(!params.contains_key("id_token_hint"));
            if has_hint {
                assert_eq!(params["agent_name_hint"], "c2a");
            }
            assert_ne!(attempt.state, attempt.nonce);
        }
    }

    #[test]
    fn callback_requires_exact_url_state_and_registered_client() {
        let attempt = Attempt::new(43127, None).unwrap();
        let port = attempt.redirect.port().unwrap();
        let valid = format!(
            "http://127.0.0.1:{port}/auth/callback?code=auth-code&state={}&client_id=issued_client",
            attempt.state
        );
        let ios = valid.strip_prefix("http://").unwrap();
        for raw in [valid.as_str(), ios, &format!("  {ios}\r\n")] {
            let callback = attempt.callback(raw).unwrap().unwrap();
            assert_eq!(callback.code, "auth-code");
            assert_eq!(callback.client_id, "issued_client");
        }

        for invalid in [
            format!(
                "http://localhost:{port}/auth/callback?code=x&state={}&client_id=client",
                attempt.state
            ),
            format!(
                "http://127.0.0.1:{}/auth/callback?code=x&state={}&client_id=client",
                port + 1,
                attempt.state
            ),
            format!(
                "https://127.0.0.1:{port}/auth/callback?code=x&state={}&client_id=client",
                attempt.state
            ),
            format!(
                "http://127.0.0.1:{port}/other?code=x&state={}&client_id=client",
                attempt.state
            ),
            format!(
                "http://user@127.0.0.1:{port}/auth/callback?code=x&state={}&client_id=client",
                attempt.state
            ),
            format!(
                "http://127.0.0.1:{port}/auth/callback?code=x&state={}&client_id=client#fragment",
                attempt.state
            ),
            format!("http://127.0.0.1:{port}/auth/callback?code=x&state=wrong&client_id=client"),
            format!(
                "http://127.0.0.1.evil.test:{port}/auth/callback?code=x&state={}&client_id=client",
                attempt.state
            ),
            format!(
                "http://127.0.0.1:{port}@evil.test/auth/callback?code=x&state={}&client_id=client",
                attempt.state
            ),
        ] {
            for raw in [
                invalid.as_str(),
                invalid.strip_prefix("http://").unwrap_or(&invalid),
            ] {
                assert!(attempt.callback(raw).is_err(), "{raw}");
            }
        }

        let duplicate_code = format!("{valid}&code=second");
        let duplicate_state = format!("{valid}&state={}", attempt.state);
        assert!(attempt.callback(&duplicate_code).is_err());
        assert!(attempt.callback(&duplicate_state).is_err());
        assert!(
            attempt
                .callback(&format!("{ios}&state={}", attempt.state))
                .is_err()
        );

        let denied = format!(
            "http://127.0.0.1:{port}/auth/callback?error=access_denied&state={}",
            attempt.state
        );
        assert!(attempt.callback(&denied).unwrap().is_none());
        assert!(
            attempt
                .callback(&format!("{denied}&code=unexpected&client_id=issued_client"))
                .is_err()
        );

        let saved = Attempt::new(43127, Some("registered_client".into())).unwrap();
        let no_issued_client = format!(
            "http://127.0.0.1:{port}/auth/callback?code=auth-code&state={}",
            saved.state
        );
        assert_eq!(
            saved
                .callback(&no_issued_client)
                .unwrap()
                .unwrap()
                .client_id,
            "registered_client"
        );
        let changed_client = format!(
            "http://127.0.0.1:{port}/auth/callback?code=auth-code&state={}&client_id=other_client",
            saved.state
        );
        assert!(saved.callback(&changed_client).is_err());

        let unregistered = Attempt::new(43127, None).unwrap();
        let missing_client = format!(
            "http://127.0.0.1:{port}/auth/callback?code=auth-code&state={}",
            unregistered.state
        );
        assert!(unregistered.callback(&missing_client).is_err());
    }

    #[test]
    fn localhost_http_callback_completes_automatic_handoff() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                    .await
                    .unwrap();
                let port = listener.local_addr().unwrap().port();
                let attempt = Attempt::new(port, None).unwrap();
                let state = attempt.state.clone();
                let input = CallbackInput {
                    file: File::open("/dev/null").unwrap(),
                    terminal: None,
                    bytes: Vec::new(),
                    eof: false,
                };
                let waiter = tokio::spawn(async move { wait_callback(listener, &attempt, input).await });
                let response = reqwest::Client::new()
                    .get(format!(
                        "http://127.0.0.1:{port}/auth/callback?code=automatic-code&state={state}&client_id=automatic_client"
                    ))
                    .timeout(Duration::from_secs(3))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                assert!(response.text().await.unwrap().contains("Return to c2a"));
                let callback = tokio::time::timeout(Duration::from_secs(3), waiter)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(callback.code, "automatic-code");
                assert_eq!(callback.client_id, "automatic_client");
            });
    }

    #[test]
    fn terminal_paste_preserves_long_urls_and_restores_settings() {
        let (mut master_fd, mut slave_fd) = (-1, -1);
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master_fd,
                    &mut slave_fd,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    std::ptr::null(),
                )
            },
            0
        );
        let master = unsafe { File::from_raw_fd(master_fd) };
        let slave = unsafe { File::from_raw_fd(slave_fd) };
        let observer = slave.try_clone().unwrap();
        let mut input = CallbackInput::from_file(slave).unwrap();
        let original = input.terminal.unwrap();
        let attempt = Attempt::new(43127, None).unwrap();
        let url = format!(
            "{}?code={}&state={}&client_id=test-client\n",
            attempt.redirect,
            "x".repeat(8000),
            attempt.state
        );
        let expected_len = url.len();
        let mut writer = master.try_clone().unwrap();
        let write = std::thread::spawn(move || writer.write_all(url.as_bytes()).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let line = loop {
            if let Some(line) = input.read_line().unwrap() {
                break line;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "terminal paste did not complete"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        write.join().unwrap();
        assert_eq!(line.len(), expected_len);
        assert_eq!(attempt.callback(&line).unwrap().unwrap().code.len(), 8000);
        drop(input);
        let mut restored = std::mem::MaybeUninit::uninit();
        assert_eq!(
            unsafe { libc::tcgetattr(observer.as_raw_fd(), restored.as_mut_ptr()) },
            0
        );
        let restored = unsafe { restored.assume_init() };
        assert_eq!(restored.c_lflag, original.c_lflag);
        assert_eq!(restored.c_cc, original.c_cc);
    }

    #[test]
    fn denial_and_pasted_callback_complete_the_same_pending_attempt() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                for pasted in [false, true] {
                    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                        .await
                        .unwrap();
                    let port = listener.local_addr().unwrap().port();
                    let attempt = Attempt::new(port, None).unwrap();
                    let url = format!(
                        "http://127.0.0.1:{port}/auth/callback?state={}&{}",
                        attempt.state,
                        if pasted {
                            "code=manual-code&client_id=manual_client"
                        } else {
                            "error=access_denied"
                        }
                    );
                    let (reader, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
                    let input = CallbackInput {
                        file: File::from(std::os::fd::OwnedFd::from(reader)),
                        terminal: None,
                        bytes: Vec::new(),
                        eof: false,
                    };
                    let waiter =
                        tokio::spawn(async move { wait_callback(listener, &attempt, input).await });
                    if pasted {
                        writeln!(writer, "{url}").unwrap();
                    } else {
                        let response = reqwest::Client::builder()
                            .no_proxy()
                            .build()
                            .unwrap()
                            .get(url)
                            .timeout(Duration::from_secs(3))
                            .send()
                            .await
                            .unwrap();
                        assert_eq!(response.status(), reqwest::StatusCode::OK);
                        assert!(response.text().await.unwrap().contains("Return to c2a"));
                    }
                    let callback = tokio::time::timeout(Duration::from_secs(3), waiter)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    if pasted {
                        assert_eq!(callback.unwrap().code, "manual-code");
                    } else {
                        assert!(callback.is_none());
                    }
                }
            });
    }

    fn token_response() -> TokenResponse {
        TokenResponse {
            access_token: "new-access".into(),
            refresh_token: "new-refresh".into(),
            id_token: None,
            token_type: "Bearer".into(),
            expires_in: 120,
            scope: Some("openid chatgpt.tokens.use.direct".into()),
            earliest_refresh_at: None,
        }
    }

    #[test]
    fn refresh_token_response_retains_scopes_and_validates_token_times() {
        let previous = ChatgptSession {
            access_token: "old-access".into(),
            refresh_token: "old-refresh".into(),
            id_token: None,
            scopes: vec!["openid".into(), SHARING_SCOPE.into()],
            expires_at: 1,
            earliest_refresh_at: None,
            received_at: 1,
        };
        let earliest = now() + 30;
        let before = now();
        let session = TokenResponse {
            scope: None,
            earliest_refresh_at: Some(serde_json::json!(earliest)),
            ..token_response()
        }
        .session(Some(&previous))
        .unwrap();
        let after = now();
        assert_eq!(session.access_token, "new-access");
        assert_eq!(session.refresh_token, "new-refresh");
        assert_eq!(session.scopes, previous.scopes);
        assert_eq!(session.earliest_refresh_at, Some(earliest));
        assert!(session.received_at >= before && session.received_at <= after);
        assert_eq!(session.expires_at, session.received_at + 120);
        assert!(sharing(&session));

        let mut not_bearer = token_response();
        not_bearer.token_type = "MAC".into();
        assert!(not_bearer.session(None).is_err());
        let mut no_access = token_response();
        no_access.access_token.clear();
        assert!(no_access.session(None).is_err());
        let mut no_refresh = token_response();
        no_refresh.refresh_token.clear();
        assert!(no_refresh.session(None).is_err());
        let mut empty_id_token = token_response();
        empty_id_token.id_token = Some(String::new());
        assert!(empty_id_token.session(None).is_err());
        let mut no_lifetime = token_response();
        no_lifetime.expires_in = 0;
        assert!(no_lifetime.session(None).is_err());
        let mut overflow = token_response();
        overflow.expires_in = i64::MAX;
        assert!(overflow.session(None).is_err());
        let mut invalid_refresh_time = token_response();
        invalid_refresh_time.earliest_refresh_at = Some(serde_json::json!(false));
        assert!(invalid_refresh_time.session(None).is_err());
        let mut no_previous_scopes = token_response();
        no_previous_scopes.scope = None;
        assert!(no_previous_scopes.session(None).is_err());
    }

    fn signed_identity(claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("chatgpt-test-key".into());
        encode(
            &header,
            claims,
            &EncodingKey::from_rsa_der(include_bytes!("../tests/fixtures/chatgpt-test-key.der")),
        )
        .unwrap()
    }

    fn identity_claims(received_at: i64) -> serde_json::Value {
        serde_json::json!({
            "sub": "chatgpt-subject",
            "iss": ISSUER,
            "aud": "registered-client",
            "exp": received_at + 600,
            "iat": received_at,
            "nonce": "login-nonce"
        })
    }

    #[test]
    fn identity_verification_checks_signature_and_bound_claims() {
        let keys: JwkSet =
            serde_json::from_str(include_str!("../tests/fixtures/chatgpt-test-jwks.json")).unwrap();
        let received_at = now();
        let claims = identity_claims(received_at);
        let token = signed_identity(&claims);
        assert_eq!(
            identity_with_keys(
                &token,
                "registered-client",
                Some("login-nonce"),
                received_at,
                &keys
            )
            .ok()
            .as_deref(),
            Some("chatgpt-subject")
        );

        // Recovery checks the signed identity at the recorded receipt time,
        // even when a key-service outage outlasted this ID token's lifetime.
        let old_receipt = received_at - 7200;
        let old_token = signed_identity(&identity_claims(old_receipt));
        assert!(
            identity_with_keys(&old_token, "registered-client", None, old_receipt, &keys).is_ok()
        );
        assert!(
            identity_with_keys(&old_token, "registered-client", None, received_at, &keys).is_err()
        );

        for (claim, value) in [
            ("iss", serde_json::json!("https://wrong.example")),
            ("aud", serde_json::json!("another-client")),
            ("nonce", serde_json::json!("another-nonce")),
            ("exp", serde_json::json!(received_at - 6)),
            ("sub", serde_json::json!("")),
        ] {
            let mut invalid_claims = identity_claims(received_at);
            invalid_claims[claim] = value;
            let invalid = signed_identity(&invalid_claims);
            assert!(
                identity_with_keys(
                    &invalid,
                    "registered-client",
                    Some("login-nonce"),
                    received_at,
                    &keys
                )
                .is_err(),
                "accepted invalid {claim}"
            );
        }

        let mut parts: Vec<_> = token.split('.').map(str::to_owned).collect();
        let mut signature = URL_SAFE_NO_PAD.decode(&parts[2]).unwrap();
        signature[0] ^= 1;
        parts[2] = URL_SAFE_NO_PAD.encode(signature);
        let bad_signature = parts.join(".");
        assert!(
            identity_with_keys(
                &bad_signature,
                "registered-client",
                Some("login-nonce"),
                received_at,
                &keys
            )
            .is_err()
        );
    }
}
