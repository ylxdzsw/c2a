use std::{
    convert::Infallible,
    ffi::OsString,
    fs, io,
    os::fd::{FromRawFd, RawFd},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::{
    HeaderMap, Method, Request, Response, StatusCode,
    body::Incoming,
    header::{ALLOW, CONTENT_TYPE},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use tokio::{
    net::UnixListener,
    sync::{Semaphore, watch},
    task::JoinSet,
};

use crate::{
    audit::RequestAudit,
    error::{ApiError, ApiErrorBody, Error, Result},
    images,
    provider::Provider,
    relay::{self, Body, Operation, Relay, RequestMetadata},
};

const BODY_LIMIT: usize = 16 * 1024 * 1024;

pub async fn serve(relay: Relay, socket: Option<PathBuf>) -> Result<()> {
    let activated = activated_listener()?;
    if activated.is_some() && socket.is_some() {
        return Err(Error::message(
            "socket path cannot be supplied with systemd activation",
        ));
    }
    let (listener, cleanup) = if let Some(listener) = activated {
        (listener, None)
    } else {
        let path = socket
            .ok_or_else(|| Error::message("serve requires SOCKET without systemd activation"))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let listener = match UnixListener::bind(&path) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                return Err(Error::message(format!(
                    "socket already exists: {}",
                    path.display()
                )));
            }
            Err(error) => return Err(error.into()),
        };
        if let Err(error) =
            fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        {
            let _ = fs::remove_file(&path);
            return Err(error.into());
        }
        (listener, Some(path))
    };
    let semaphore = Arc::new(Semaphore::new(16));
    let relay = Arc::new(relay);
    let mut tasks = JoinSet::new();
    let stopping = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    install_signal_tasks(stopping.clone())?;
    while !stopping.load(Ordering::Acquire) {
        reap_finished(&mut tasks);
        match tokio::time::timeout(Duration::from_millis(250), listener.accept()).await {
            Ok(Ok((stream, _))) => {
                let relay = relay.clone();
                let semaphore = semaphore.clone();
                let shutdown = shutdown_rx.clone();
                tasks.spawn(async move {
                    let service = service_fn(move |request| {
                        handle(request, relay.clone(), semaphore.clone(), shutdown.clone())
                    });
                    if let Err(error) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        eprintln!("c2a: connection failed: {error}");
                    }
                });
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {}
        }
    }
    let wait = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(30), wait)
        .await
        .is_err()
    {
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            semaphore.clone().acquire_many_owned(16),
        )
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    if let Some(path) = cleanup {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

async fn handle(
    request: Request<Incoming>,
    relay: Arc<Relay>,
    semaphore: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
) -> std::result::Result<Response<Body>, Infallible> {
    let (request_id, id_error) = match random_id() {
        Ok(request_id) => (request_id, None),
        Err(error) => ("00000000000000000000000000000000".into(), Some(error)),
    };
    let mut audit = Some(relay.audit_request(request_id.clone()));
    if let Some(audit) = audit.as_mut() {
        audit.record.operation = "unknown".into();
    }
    let response = match id_error {
        Some(error) => json_error_with_id(StatusCode::BAD_GATEWAY, &error.to_string(), &request_id),
        None => {
            match handle_inner(request, relay, semaphore, shutdown, &request_id, &mut audit).await {
                Ok(response) => response,
                Err(error) => {
                    json_error_with_id(StatusCode::BAD_GATEWAY, &error.to_string(), &request_id)
                }
            }
        }
    };
    if let Some(mut audit) = audit.take() {
        let status = response.status();
        audit.record.local_http_status = Some(status.as_u16());
        audit.record.outcome =
            if status.is_client_error() || status == StatusCode::SERVICE_UNAVAILABLE {
                "rejected"
            } else {
                "failed"
            }
            .into();
        audit.finish().await;
    }
    Ok(response)
}

async fn handle_inner(
    request: Request<Incoming>,
    relay: Arc<Relay>,
    semaphore: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
    request_id: &str,
    audit: &mut Option<RequestAudit>,
) -> Result<Response<Body>> {
    let Some(operation) = relay.operation(request.uri().path()) else {
        return Ok(json_error_with_id(
            StatusCode::NOT_FOUND,
            "not found",
            request_id,
        ));
    };
    if let Some(audit) = audit.as_mut() {
        audit.record.operation = operation.as_str().into();
    }
    if request.method() != Method::POST {
        let mut response = json_error_with_id(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
            request_id,
        );
        response
            .headers_mut()
            .insert(ALLOW, hyper::header::HeaderValue::from_static("POST"));
        return Ok(response);
    }
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Ok(json_error_with_id(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content type must be application/json",
            request_id,
        ));
    }
    let mut permits = Vec::new();
    for semaphore in std::iter::once(semaphore)
        .chain((operation != Operation::Responses).then(|| relay.image_slots.clone()))
    {
        match semaphore.try_acquire_owned() {
            Ok(permit) => permits.push(permit),
            Err(_) => {
                return Ok(json_error_with_id(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "too many active relays",
                    request_id,
                ));
            }
        }
    }
    let limit = if operation == Operation::Responses {
        BODY_LIMIT
    } else {
        images::BODY_LIMIT
    };
    let too_large = || {
        json_error_with_id(
            StatusCode::PAYLOAD_TOO_LARGE,
            &format!("request body exceeds {} MiB", limit / (1024 * 1024)),
            request_id,
        )
    };
    if request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > limit as u64)
    {
        return Ok(too_large());
    }
    let identity_headers = forwarded_identity_headers(request.headers());
    let collected = match tokio::time::timeout(
        Duration::from_secs(60),
        Limited::new(request.into_body(), limit).collect(),
    )
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(error)) if error.downcast_ref::<LengthLimitError>().is_some() => {
            return Ok(too_large());
        }
        Err(_) => {
            return Ok(json_error_with_id(
                StatusCode::REQUEST_TIMEOUT,
                "request body timed out",
                request_id,
            ));
        }
        Ok(Err(_)) => {
            return Ok(json_error_with_id(
                StatusCode::BAD_REQUEST,
                "request body could not be read",
                request_id,
            ));
        }
    };
    let bytes = collected.to_bytes();
    if let Some(audit) = audit.as_mut() {
        // Record only the collected length; request payloads are never retained.
        audit.record.request_bytes = bytes.len() as u64;
    }
    let validation = if operation == Operation::Responses {
        validate_body(&bytes, relay.provider())
    } else {
        images::validate(&bytes, operation)
    };
    let mut metadata = match validation {
        Ok(metadata) => metadata,
        Err(message) => {
            return Ok(json_error_with_id(
                StatusCode::BAD_REQUEST,
                message,
                request_id,
            ));
        }
    };
    if let Some(audit) = audit.as_mut() {
        audit.record.model = metadata.model.clone();
    }
    metadata.identity_headers = identity_headers;
    let audit = audit
        .take()
        .expect("request audit must exist before relay handoff");
    match relay.start(bytes, metadata, audit, permits, shutdown).await {
        Ok(response) => Ok(response),
        Err(error) => Ok(json_error_with_id(
            StatusCode::BAD_GATEWAY,
            &error.to_string(),
            request_id,
        )),
    }
}

fn forwarded_identity_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::new();
    for name in ["user-agent", "originator"] {
        for value in headers.get_all(name) {
            forwarded.append(name, value.clone());
        }
    }
    forwarded
}

fn validate_body(
    bytes: &[u8],
    provider: Provider,
) -> std::result::Result<RequestMetadata, &'static str> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| "request body must be valid JSON")?;
    let object = value
        .as_object()
        .ok_or("request body must be a JSON object")?;
    let model = object
        .get("model")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("model must be a nonempty string")?;
    if object.get("stream") != Some(&serde_json::Value::Bool(true)) {
        return Err("stream must be true");
    }
    if provider == Provider::Chatgpt {
        if object.get("store") != Some(&serde_json::Value::Bool(false)) {
            return Err("ChatGPT plan requests require store: false");
        }
        if !object.get("input").is_some_and(serde_json::Value::is_array) {
            return Err("ChatGPT plan requests require input as an array");
        }
    }
    Ok(RequestMetadata {
        operation: Operation::Responses,
        model: model.to_owned(),
        identity_headers: HeaderMap::new(),
        prompt_cache_key: object
            .get("prompt_cache_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        service_tier: object
            .get("service_tier")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        reasoning_effort: value
            .pointer("/reasoning/effort")
            .and_then(serde_json::Value::as_str)
            .filter(|effort| !effort.is_empty())
            .map(str::to_owned),
    })
}

fn reap_finished(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        if let Err(error) = result {
            eprintln!("c2a: connection task failed: {error}");
        }
    }
}

fn json_error_with_id(status: StatusCode, message: &str, id: &str) -> Response<Body> {
    let bytes = serde_json::to_vec(&ApiError {
        error: ApiErrorBody {
            message,
            r#type: "proxy_error",
            request_id: id,
        },
    })
    .unwrap();
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(relay::full(bytes))
        .unwrap()
}
fn random_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| Error::message(error.to_string()))?;
    Ok(bytes.iter().map(|value| format!("{value:02x}")).collect())
}

fn activated_listener() -> Result<Option<UnixListener>> {
    let pid = std::env::var("LISTEN_PID").ok();
    let fds = std::env::var("LISTEN_FDS").ok();
    if pid.is_none() && fds.is_none() {
        return Ok(None);
    }
    let pid: u32 = pid
        .ok_or_else(|| Error::message("malformed socket activation"))?
        .parse()
        .map_err(|_| Error::message("malformed LISTEN_PID"))?;
    let fds: i32 = fds
        .ok_or_else(|| Error::message("malformed socket activation"))?
        .parse()
        .map_err(|_| Error::message("malformed LISTEN_FDS"))?;
    if pid != std::process::id() || fds != 1 {
        return Err(Error::message(
            "exactly one socket-activation descriptor is required",
        ));
    }
    validate_fd(3)?;
    let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(3) };
    std_listener.set_nonblocking(true)?;
    Ok(Some(UnixListener::from_std(std_listener)?))
}

fn validate_fd(fd: RawFd) -> Result<()> {
    let mut ty: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut ty as *mut libc::c_int).cast(),
            &mut len,
        )
    } != 0
        || ty != libc::SOCK_STREAM
    {
        return Err(Error::message("fd 3 is not a Unix stream listener"));
    }
    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addr_len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    if unsafe {
        libc::getsockname(
            fd,
            (&mut addr as *mut libc::sockaddr_storage).cast(),
            &mut addr_len,
        )
    } != 0
        || addr.ss_family != libc::AF_UNIX as u16
    {
        return Err(Error::message("fd 3 is not AF_UNIX"));
    }
    let mut accept = 0;
    let mut accept_len = size_of::<libc::c_int>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ACCEPTCONN,
            (&mut accept as *mut libc::c_int).cast(),
            &mut accept_len,
        )
    } != 0
        || accept != 1
    {
        return Err(Error::message("fd 3 is not listening"));
    }
    Ok(())
}

fn install_signal_tasks(stopping: Arc<AtomicBool>) -> io::Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let term_stopping = stopping.clone();
    tokio::spawn(async move {
        term.recv().await;
        term_stopping.store(true, Ordering::Release);
    });
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        stopping.store(true, Ordering::Release);
    });
    Ok(())
}

pub fn socket_argument(value: OsString) -> PathBuf {
    PathBuf::from(value)
}

#[cfg(test)]
mod tests {
    use super::{forwarded_identity_headers, validate_body};

    #[test]
    fn only_identity_headers_are_selected_for_forwarding() {
        let mut headers = hyper::HeaderMap::new();
        assert!(forwarded_identity_headers(&headers).is_empty());
        for (name, value) in [
            ("User-Agent", "mu/1.0"),
            ("Originator", "mu"),
            ("Authorization", "Bearer downstream"),
            ("Chatgpt-Account-Id", "downstream-account"),
            ("Content-Type", "text/plain"),
            ("X-Request-Id", "downstream-id"),
        ] {
            headers.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        let forwarded = forwarded_identity_headers(&headers);
        assert_eq!(forwarded.len(), 2);
        assert_eq!(forwarded["user-agent"], "mu/1.0");
        assert_eq!(forwarded["originator"], "mu");
    }

    #[test]
    fn validation_does_not_rewrite_request_bytes() {
        let bytes = br#"{ "stream": true, "model": "gpt-5", "prompt_cache_key": "session-1", "service_tier": "priority", "reasoning": {"effort": "high"}, "input": [1, 2] }"#.to_vec();
        let original = bytes.clone();
        let metadata = validate_body(&bytes, crate::provider::Provider::Codex).unwrap();
        assert_eq!(metadata.model, "gpt-5");
        assert_eq!(metadata.prompt_cache_key.as_deref(), Some("session-1"));
        assert_eq!(metadata.service_tier.as_deref(), Some("priority"));
        assert_eq!(metadata.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(bytes, original);
    }

    #[test]
    fn reasoning_effort_is_optional_metadata_not_validation() {
        for reasoning in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!({"effort": null}),
            serde_json::json!({"effort": ""}),
            serde_json::json!({"effort": 42}),
            serde_json::json!({"effort": "future-effort"}),
        ] {
            let request = serde_json::json!({"stream": true, "model": "m", "reasoning": reasoning});
            let metadata = validate_body(
                &serde_json::to_vec(&request).unwrap(),
                crate::provider::Provider::Codex,
            )
            .unwrap();
            assert_eq!(
                metadata.reasoning_effort.as_deref(),
                reasoning["effort"]
                    .as_str()
                    .filter(|effort| !effort.is_empty())
            );
        }
        let metadata = validate_body(
            br#"{"stream":true,"model":"m"}"#,
            crate::provider::Provider::Codex,
        )
        .unwrap();
        assert!(metadata.reasoning_effort.is_none());
    }
}
