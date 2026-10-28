use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use http_body_util::{BodyExt, Channel, Full, combinators::BoxBody};
use hyper::{
    HeaderMap, Response, StatusCode,
    body::Bytes,
    header::{CONTENT_TYPE, HeaderName, HeaderValue, RETRY_AFTER},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};

use crate::{
    audit::{Audit, AuditRecord, RequestAudit},
    chatgpt,
    claims::Claims,
    copilot, device,
    error::{ApiError, ApiErrorBody, Error, Result},
    images,
    paths::Paths,
    provider::Provider,
    refresh,
    storage::CodexCredentials,
};

pub(crate) const ERROR_LIMIT: usize = 64 * 1024;
const ERROR_DETAIL_LIMIT: usize = 1024;
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

pub type Body = BoxBody<Bytes, Infallible>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Responses,
    ImageGenerations,
    ImageEdits,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Responses => "responses",
            Self::ImageGenerations => "images.generations",
            Self::ImageEdits => "images.edits",
        }
    }

    fn upstream(self) -> &'static str {
        match self {
            Self::Responses => device::UPSTREAM,
            Self::ImageGenerations => device::IMAGE_GENERATIONS,
            Self::ImageEdits => device::IMAGE_EDITS,
        }
    }
}

#[derive(Debug)]
pub struct RequestMetadata {
    pub operation: Operation,
    pub model: String,
    pub prompt_cache_key: Option<String>,
    pub service_tier: Option<String>,
    pub reasoning_effort: Option<String>,
    pub identity_headers: HeaderMap,
}

pub struct Relay {
    client: reqwest::Client,
    provider: Provider,
    paths: Paths,
    audit: Audit,
    credentials: tokio::sync::Mutex<()>,
    copilot_endpoint: tokio::sync::Mutex<Option<CopilotEndpoint>>,
    pub image_slots: Arc<Semaphore>,
}

struct Sent {
    response: reqwest::Response,
    access_token: String,
}

struct CopilotEndpoint {
    access_token: String,
    endpoint: String,
}

impl Relay {
    pub fn new(provider: Provider, paths: Paths, audit: Audit) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .referer(false)
            .build()?;
        Ok(Self {
            client,
            provider,
            paths,
            audit,
            credentials: tokio::sync::Mutex::new(()),
            copilot_endpoint: tokio::sync::Mutex::new(None),
            image_slots: Arc::new(Semaphore::new(2)),
        })
    }

    pub fn operation(&self, path: &str) -> Option<Operation> {
        match (self.provider, path) {
            (_, "/v1/responses") => Some(Operation::Responses),
            (Provider::Codex, "/v1/images/generations") => Some(Operation::ImageGenerations),
            (Provider::Codex, "/v1/images/edits") => Some(Operation::ImageEdits),
            _ => None,
        }
    }

    pub fn provider(&self) -> Provider {
        self.provider
    }

    pub fn audit_request(&self, request_id: String) -> RequestAudit {
        RequestAudit::new(self.audit.clone(), self.provider.as_str(), request_id)
    }

    pub async fn start(
        self: &Arc<Self>,
        body: Bytes,
        metadata: RequestMetadata,
        mut audit: RequestAudit,
        permits: Vec<OwnedSemaphorePermit>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Response<Body>> {
        audit.begin_relay();
        audit.record.operation = metadata.operation.as_str().into();
        audit.record.model = metadata.model.clone();
        audit.record.request_bytes = body.len() as u64;
        audit.record.requested_service_tier = metadata.service_tier.clone();
        audit.record.requested_reasoning_effort = metadata.reasoning_effort.clone();
        let relay = self.clone();
        if metadata.operation != Operation::Responses {
            return images::start(audit, permits, shutdown, move |record| {
                Box::pin(async move { relay.send_authenticated(&body, &metadata, record).await })
            })
            .await;
        }
        let (mut reply, receive) = oneshot::channel();
        tokio::spawn(async move {
            let _permits = permits;
            let result = {
                let work = relay.responses(&body, &metadata, &mut audit);
                if *shutdown.borrow() {
                    Err("shutdown")
                } else {
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => Err("shutdown"),
                        _ = reply.closed() => Err("client_disconnect"),
                        result = work => Ok(result),
                    }
                }
            };
            match result {
                Ok(Ok((response, stream))) => {
                    audit.record.local_http_status = Some(response.status().as_u16());
                    if reply.send(Ok(response)).is_err() {
                        audit.record.local_http_status = None;
                        audit.record.outcome = "client_disconnect".into();
                    } else if let Some((mut upstream, mut sender)) = stream {
                        observe_stream(&mut upstream, &mut sender, &mut audit, &mut shutdown).await;
                    }
                }
                Ok(Err(error)) => {
                    audit.record.local_http_status = Some(502);
                    if reply.send(Err(error)).is_err() {
                        audit.record.local_http_status = None;
                        audit.record.outcome = "client_disconnect".into();
                    }
                }
                Err(outcome) => {
                    audit.record.outcome = outcome.into();
                    if reply
                        .send(Err(Error::message(format!("request ended: {outcome}"))))
                        .is_ok()
                    {
                        audit.record.local_http_status = Some(502);
                    }
                }
            }
            audit.finish().await;
        });
        receive
            .await
            .map_err(|_| Error::message("relay task stopped"))?
    }

    async fn responses(
        &self,
        body: &Bytes,
        metadata: &RequestMetadata,
        audit: &mut RequestAudit,
    ) -> Result<(Response<Body>, Option<UpstreamStream>)> {
        let mut response = self
            .send_authenticated(body, metadata, &mut audit.record)
            .await?;
        let status = response.status();
        let upstream_request_id = audit.record.upstream_request_id.clone();
        if !status.is_success() {
            audit.record.outcome = "upstream_error".into();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .cloned();
            let error_body = limited_observed(&mut response, ERROR_LIMIT, audit).await?;
            if self.provider == Provider::Chatgpt {
                let mut builder = Response::builder().status(status.as_u16());
                if let Some(value) = response.headers().get(reqwest::header::CONTENT_TYPE) {
                    builder = builder.header(CONTENT_TYPE, value.as_bytes());
                }
                if let Some(value) = retry_after {
                    builder = builder.header(RETRY_AFTER, value);
                }
                if let Some(value) = upstream_request_id {
                    builder = builder.header("x-request-id", value);
                }
                return builder
                    .body(full(error_body))
                    .map(|response| (response, None))
                    .map_err(|error| Error::message(error.to_string()));
            }
            let detail = upstream_error_detail(&error_body)
                .map(|detail| format!(": {detail}"))
                .unwrap_or_default();
            let message = format!("upstream returned {}{detail}", status.as_u16());
            return upstream_error_response(
                status.as_u16(),
                &message,
                &audit.record.request_id,
                retry_after.as_ref(),
                upstream_request_id.as_deref(),
            )
            .map(|response| (response, None));
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .cloned();
        if !accepts_content_type(content_type.as_ref()) {
            audit.record.outcome = "upstream_error".into();
            limited_observed(&mut response, ERROR_LIMIT, audit).await?;
            return Err(Error::message("upstream did not return text/event-stream"));
        }
        let content_type = content_type
            .unwrap_or_else(|| reqwest::header::HeaderValue::from_static("text/event-stream"));
        let mut builder = Response::builder()
            .status(status.as_u16())
            .header(CONTENT_TYPE, content_type);
        if let Some(value) = response.headers().get(reqwest::header::CACHE_CONTROL) {
            builder = builder.header(hyper::header::CACHE_CONTROL, value);
        }
        if let Some(value) = upstream_request_id {
            builder = builder.header("x-request-id", value);
        }
        let (sender, channel) = Channel::<Bytes, Infallible>::new(4);
        builder
            .body(channel.boxed())
            .map(|local| (local, Some((response, sender))))
            .map_err(|error| Error::message(error.to_string()))
    }

    async fn send_authenticated(
        &self,
        body: &Bytes,
        metadata: &RequestMetadata,
        record: &mut AuditRecord,
    ) -> Result<reqwest::Response> {
        let mut sent = self.send(body, metadata, None, record).await?;
        record_upstream(&sent.response, record);
        // SIWC admission 401s can indicate permissions, not an expired token.
        // Refresh proactively; preserve direct-route authorization failures.
        if self.provider != Provider::Chatgpt
            && sent.response.status() == reqwest::StatusCode::UNAUTHORIZED
        {
            record.auth_retries = Some(record.auth_retries.unwrap_or_default() + 1);
            let rejected_access_token = sent.access_token;
            drop(sent.response);
            sent = self
                .send(body, metadata, Some(&rejected_access_token), record)
                .await?;
            record_upstream(&sent.response, record);
        }
        Ok(sent.response)
    }

    async fn send(
        &self,
        body: &Bytes,
        metadata: &RequestMetadata,
        rejected_access_token: Option<&str>,
        record: &mut AuditRecord,
    ) -> Result<Sent> {
        match self.provider {
            Provider::Chatgpt => {
                let _guard = self.credentials.lock().await;
                let credentials =
                    chatgpt::credentials(&self.client, &self.paths, rejected_access_token).await?;
                let access_token = credentials.access_token;
                drop(_guard);
                let request =
                    chatgpt_request(&self.client, body.clone(), metadata, &access_token).build()?;
                record.upstream_attempts = Some(record.upstream_attempts.unwrap_or_default() + 1);
                Ok(Sent {
                    response: self.client.execute(request).await?,
                    access_token,
                })
            }
            Provider::Codex => {
                let _guard = self.credentials.lock().await;
                let (credentials, claims) =
                    refresh::credentials(&self.client, &self.paths, rejected_access_token).await?;
                let access_token = credentials.access_token.clone();
                let request =
                    codex_request(&self.client, body.clone(), metadata, &credentials, &claims)
                        .build()?;
                drop(_guard);
                record.upstream_attempts = Some(record.upstream_attempts.unwrap_or_default() + 1);
                Ok(Sent {
                    response: self.client.execute(request).await?,
                    access_token,
                })
            }
            Provider::Copilot => {
                let _guard = self.credentials.lock().await;
                let credentials =
                    copilot::credentials(&self.client, &self.paths, rejected_access_token).await?;
                let access_token = credentials.access_token.clone();
                drop(_guard);
                let endpoint = self.copilot_endpoint(&credentials.access_token).await?;
                let request = copilot_request(
                    &self.client,
                    body,
                    metadata,
                    &endpoint,
                    &credentials.access_token,
                )
                .build()?;
                record.upstream_attempts = Some(record.upstream_attempts.unwrap_or_default() + 1);
                Ok(Sent {
                    response: self.client.execute(request).await?,
                    access_token,
                })
            }
        }
    }

    async fn copilot_endpoint(&self, access_token: &str) -> Result<String> {
        let mut cache = self.copilot_endpoint.lock().await;
        if let Some(value) = cache
            .as_ref()
            .filter(|value| value.access_token == access_token)
        {
            return Ok(value.endpoint.clone());
        }
        let discovery = copilot::discover(&self.client, access_token).await?;
        let endpoint = discovery.endpoint;
        *cache = Some(CopilotEndpoint {
            access_token: access_token.to_owned(),
            endpoint: endpoint.clone(),
        });
        Ok(endpoint)
    }
}

type UpstreamStream = (reqwest::Response, http_body_util::channel::Sender<Bytes>);

fn record_upstream(response: &reqwest::Response, record: &mut AuditRecord) {
    record.upstream_http_status = Some(response.status().as_u16());
    record.upstream_request_id = sanitize_request_id(
        response
            .headers()
            .get("x-request-id")
            .or_else(|| response.headers().get("x-oai-request-id"))
            .and_then(|value| value.to_str().ok()),
    );
}

async fn observe_stream(
    response: &mut reqwest::Response,
    sender: &mut http_body_util::channel::Sender<Bytes>,
    audit: &mut RequestAudit,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut observer = crate::sse::Observer::new();
    let mut outcome = None;
    loop {
        match next_or_shutdown(response, shutdown).await {
            Next::Shutdown => {
                outcome = Some("shutdown".to_string());
                break;
            }
            Next::Chunk(Ok(Some(chunk))) => {
                let elapsed = audit.elapsed_ms();
                if !chunk.is_empty() {
                    audit.record.first_upstream_byte_ms.get_or_insert(elapsed);
                }
                audit.record.response_bytes += chunk.len() as u64;
                observer.feed_at(&chunk, elapsed);
                let sent = tokio::select! {
                    biased;
                    _ = shutdown.changed() => Err("shutdown"),
                    result = sender.send_data(chunk) => result.map_err(|_| "client_disconnect"),
                };
                if let Err(reason) = sent {
                    outcome = Some(reason.to_owned());
                    break;
                }
            }
            Next::Chunk(Err(_)) | Next::Idle => {
                outcome = Some("premature_eof".to_string());
                break;
            }
            Next::Chunk(Ok(None)) => {
                observer.finish();
                break;
            }
        }
    }
    let observation = &observer.observation;
    audit.record.outcome = if observer.failed() {
        "uninspected".into()
    } else {
        outcome
            .or_else(|| observation.outcome.clone())
            .unwrap_or_else(|| "premature_eof".into())
    };
    audit.record.response_id = observation.response_id.clone();
    audit.record.response_model = observation.response_model.clone();
    audit.record.response_service_tier = observation.response_service_tier.clone();
    audit.record.response_reasoning_effort = observation.response_reasoning_effort.clone();
    audit.record.input_tokens = observation.input_tokens;
    audit.record.output_tokens = observation.output_tokens;
    audit.record.cached_input_tokens = observation.cached_input_tokens;
    audit.record.reasoning_output_tokens = observation.reasoning_output_tokens;
    audit.record.total_tokens = observation.total_tokens;
    audit.record.first_output_ms = observation.first_output_ms;
    audit.record.first_text_ms = observation.first_text_ms;
    audit.record.last_output_ms = observation.last_output_ms;
    audit.record.terminal_ms = observation.terminal_ms;
    audit.record.first_output_kind = observation.first_output_kind.clone();
}

pub(crate) async fn limited_observed(
    response: &mut reqwest::Response,
    limit: usize,
    audit: &mut RequestAudit,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while bytes.len() < limit {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        if !chunk.is_empty() {
            let elapsed = audit.elapsed_ms();
            audit.record.first_upstream_byte_ms.get_or_insert(elapsed);
        }
        let kept = chunk.len().min(limit - bytes.len());
        // Retain the existing error-body count (bounded bytes, not full wire bytes).
        audit.record.response_bytes += kept as u64;
        bytes.extend_from_slice(&chunk[..kept]);
    }
    Ok(bytes)
}

enum Next {
    Shutdown,
    Chunk(std::result::Result<Option<Bytes>, reqwest::Error>),
    Idle,
}

async fn next_or_shutdown(
    response: &mut reqwest::Response,
    shutdown: &mut watch::Receiver<bool>,
) -> Next {
    if *shutdown.borrow() {
        return Next::Shutdown;
    }
    let mut changed = Box::pin(shutdown.changed());
    let mut chunk = Box::pin(response.chunk());
    let race = poll_fn(|context| {
        if changed.as_mut().poll(context).is_ready() {
            return Poll::Ready(Next::Shutdown);
        }
        if let Poll::Ready(result) = chunk.as_mut().poll(context) {
            return Poll::Ready(Next::Chunk(result));
        }
        Poll::Pending
    });
    tokio::time::timeout(STREAM_IDLE_TIMEOUT, race)
        .await
        .unwrap_or(Next::Idle)
}

fn chatgpt_request(
    client: &reqwest::Client,
    body: Bytes,
    metadata: &RequestMetadata,
    access_token: &str,
) -> reqwest::RequestBuilder {
    let mut request = client
        .post(chatgpt::UPSTREAM)
        .bearer_auth(access_token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .headers(metadata.identity_headers.clone())
        .body(body);
    if let Some(value) = metadata
        .prompt_cache_key
        .as_deref()
        .and_then(valid_header_value)
    {
        request = request.header(chatgpt::SESSION_ID_HEADER, value);
    }
    request
}

fn codex_request(
    client: &reqwest::Client,
    body: Bytes,
    metadata: &RequestMetadata,
    credentials: &CodexCredentials,
    claims: &Claims,
) -> reqwest::RequestBuilder {
    let mut request = client
        .post(metadata.operation.upstream())
        .bearer_auth(&credentials.access_token)
        .header("Chatgpt-Account-Id", &claims.account_id)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            reqwest::header::ACCEPT,
            if metadata.operation == Operation::Responses {
                "text/event-stream"
            } else {
                "application/json"
            },
        )
        .headers(metadata.identity_headers.clone())
        .body(body);
    if metadata.operation != Operation::Responses {
        return request;
    }
    if let Some(value) = metadata
        .prompt_cache_key
        .as_deref()
        .and_then(valid_header_value)
    {
        request = request.header(device::SESSION_ID_HEADER, value);
    }
    let routing_hint = match metadata.service_tier.as_deref() {
        Some(tier) => format!("model={};tier={tier}", metadata.model),
        None => format!("model={}", metadata.model),
    };
    if let Some(value) = valid_header_value(&routing_hint) {
        request = request.header(device::ROUTING_HINT_HEADER, value);
    }
    request
}

fn copilot_request(
    client: &reqwest::Client,
    body: &[u8],
    metadata: &RequestMetadata,
    endpoint: &str,
    access_token: &str,
) -> reqwest::RequestBuilder {
    client
        .post(format!("{endpoint}/responses"))
        .bearer_auth(access_token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .headers(metadata.identity_headers.clone())
        .header("X-GitHub-Api-Version", copilot::API_VERSION)
        .body(body.to_vec())
}

fn valid_header_value(value: &str) -> Option<reqwest::header::HeaderValue> {
    reqwest::header::HeaderValue::try_from(value).ok()
}

pub(crate) fn upstream_error_detail(bytes: &[u8]) -> Option<String> {
    let json = serde_json::from_slice::<serde_json::Value>(bytes).ok();
    let detail = json
        .as_ref()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .or_else(|| value.get("detail"))
        })
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            json.is_none()
                .then(|| std::str::from_utf8(bytes).ok())
                .flatten()
        })?;
    let detail = detail
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(ERROR_DETAIL_LIMIT)
        .collect::<String>();
    (!detail.is_empty()).then_some(detail)
}

fn accepts_content_type(content_type: Option<&reqwest::header::HeaderValue>) -> bool {
    content_type
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
        })
        || content_type.is_none()
}

pub(crate) fn sanitize_request_id(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 256
                && value.bytes().all(|byte| byte.is_ascii_graphic())
        })
        .map(str::to_owned)
}

pub(crate) fn upstream_error_response(
    upstream_status: u16,
    message: &str,
    request_id: &str,
    retry_after: Option<&reqwest::header::HeaderValue>,
    upstream_request_id: Option<&str>,
) -> Result<Response<Body>> {
    let status =
        StatusCode::from_u16(upstream_status).map_err(|error| Error::message(error.to_string()))?;
    let bytes = serde_json::to_vec(&ApiError {
        error: ApiErrorBody {
            message,
            r#type: "proxy_error",
            request_id,
        },
    })?;
    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json");
    if let Some(value) =
        retry_after.and_then(|value| HeaderValue::from_bytes(value.as_bytes()).ok())
    {
        builder = builder.header(RETRY_AFTER, value);
    }
    if let Some(value) = upstream_request_id {
        builder = builder.header(HeaderName::from_static("x-request-id"), value);
    }
    builder
        .body(full(bytes))
        .map_err(|error| Error::message(error.to_string()))
}

pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).boxed()
}

#[cfg(test)]
mod tests {
    use super::{
        Operation, RequestMetadata, accepts_content_type, chatgpt_request, codex_request,
        copilot_request, upstream_error_detail, upstream_error_response,
    };
    use crate::{claims::Claims, copilot, device, storage::CodexCredentials};

    fn metadata() -> RequestMetadata {
        RequestMetadata {
            operation: Operation::Responses,
            model: "m".into(),
            prompt_cache_key: None,
            service_tier: None,
            reasoning_effort: None,
            identity_headers: Default::default(),
        }
    }

    #[test]
    fn identity_headers_are_forwarded_only_when_supplied() {
        let client = reqwest::Client::new();
        let credentials = CodexCredentials {
            version: 1,
            access_token: "access".into(),
            refresh_token: "refresh".into(),
        };
        let claims = Claims {
            account_id: "account".into(),
            ..Claims::default()
        };
        for (user_agent, originator) in [
            (None, None),
            (Some("mu/1.0"), None),
            (None, Some("mu")),
            (Some("mu/1.0"), Some("mu")),
        ] {
            let mut metadata = metadata();
            for (name, value) in [("user-agent", user_agent), ("originator", originator)] {
                if let Some(value) = value {
                    metadata
                        .identity_headers
                        .insert(name, value.parse().unwrap());
                }
            }
            let mut requests = vec![
                chatgpt_request(&client, "{}".into(), &metadata, "access"),
                copilot_request(
                    &client,
                    b"{}",
                    &metadata,
                    "https://api.githubcopilot.com",
                    "access",
                ),
            ];
            for operation in [
                Operation::Responses,
                Operation::ImageGenerations,
                Operation::ImageEdits,
            ] {
                metadata.operation = operation;
                requests.push(codex_request(
                    &client,
                    "{}".into(),
                    &metadata,
                    &credentials,
                    &claims,
                ));
            }
            for request in requests {
                let request = request.build().unwrap();
                for (name, expected) in [("user-agent", user_agent), ("originator", originator)] {
                    assert_eq!(
                        request
                            .headers()
                            .get(name)
                            .map(|value| value.to_str().unwrap()),
                        expected
                    );
                }
                assert_eq!(request.headers()["authorization"], "Bearer access");
            }
        }
    }

    #[test]
    fn chatgpt_request_preserves_native_bytes_without_codex_headers() {
        let bytes = hyper::body::Bytes::from_static(
            br#"{ "model":"m", "stream":true, "store":false, "input":[] }"#,
        );
        let request = chatgpt_request(
            &reqwest::Client::new(),
            bytes.clone(),
            &metadata(),
            "access",
        )
        .build()
        .unwrap();
        assert_eq!(request.url().as_str(), crate::chatgpt::UPSTREAM);
        assert_eq!(request.body().unwrap().as_bytes().unwrap(), bytes);
        assert_eq!(request.headers()["authorization"], "Bearer access");
        assert!(request.headers().get("user-agent").is_none());
        for name in [
            "originator",
            "chatgpt-account-id",
            device::SESSION_ID_HEADER,
            device::ROUTING_HINT_HEADER,
        ] {
            assert!(request.headers().get(name).is_none());
        }
    }

    #[test]
    fn chatgpt_session_header_uses_only_a_header_safe_body_cache_key() {
        for key in [None, Some("mu:session-1:epoch:0"), Some("bad\nkey")] {
            let bytes = hyper::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "model": "m", "stream": true, "store": false,
                    "input": [], "prompt_cache_key": key,
                }))
                .unwrap(),
            );
            let request = chatgpt_request(
                &reqwest::Client::new(),
                bytes.clone(),
                &RequestMetadata {
                    prompt_cache_key: key.map(str::to_owned),
                    ..metadata()
                },
                "access",
            )
            .build()
            .unwrap();
            assert_eq!(request.body().unwrap().as_bytes().unwrap(), bytes);
            assert_eq!(
                request.headers().get(crate::chatgpt::SESSION_ID_HEADER),
                key.filter(|key| !key.contains('\n'))
                    .map(|key| key.parse::<hyper::header::HeaderValue>().unwrap())
                    .as_ref(),
            );
            for name in [
                device::SESSION_ID_HEADER,
                device::ROUTING_HINT_HEADER,
                "x-client-request-id",
            ] {
                assert!(request.headers().get(name).is_none());
            }
        }
    }

    #[test]
    fn image_requests_use_native_paths_and_preserve_bytes() {
        for operation in [Operation::ImageGenerations, Operation::ImageEdits] {
            let bytes = hyper::body::Bytes::from_static(
                br#"{ "model":"gpt-image-2.5-flare", "prompt":"test", "quality":"xhigh" }"#,
            );
            let request = codex_request(
                &reqwest::Client::new(),
                bytes.clone(),
                &RequestMetadata {
                    operation,
                    model: "gpt-image-2.5-flare".into(),
                    prompt_cache_key: Some("unused".into()),
                    service_tier: Some("unused".into()),
                    reasoning_effort: None,
                    identity_headers: Default::default(),
                },
                &CodexCredentials {
                    version: 1,
                    access_token: "access".into(),
                    refresh_token: "refresh".into(),
                },
                &Claims {
                    account_id: "account".into(),
                    ..Claims::default()
                },
            )
            .build()
            .unwrap();
            assert_eq!(request.url().as_str(), operation.upstream());
            assert_eq!(request.body().unwrap().as_bytes().unwrap(), bytes);
            assert_eq!(
                request.headers()[reqwest::header::ACCEPT],
                "application/json"
            );
            assert_eq!(request.headers()["authorization"], "Bearer access");
            assert_eq!(request.headers()["chatgpt-account-id"], "account");
            assert!(request.headers().get("originator").is_none());
            assert!(request.headers().get("user-agent").is_none());
            for name in [
                device::SESSION_ID_HEADER,
                device::ROUTING_HINT_HEADER,
                "version",
                "x-codex-image-turn-id",
            ] {
                assert!(request.headers().get(name).is_none());
            }
        }
    }

    #[test]
    fn upstream_request_uses_body_derived_routing_headers() {
        let request = codex_request(
            &reqwest::Client::new(),
            hyper::body::Bytes::from_static(br#"{"model":"gpt-5.6-luna","stream":true,"prompt_cache_key":"session-1","service_tier":"priority"}"#),
            &RequestMetadata {
                operation: Operation::Responses,
                model: "gpt-5.6-luna".into(),
                prompt_cache_key: Some("session-1".into()),
                service_tier: Some("priority".into()),
                reasoning_effort: None,
                identity_headers: Default::default(),
            },
            &CodexCredentials {
                version: 1,
                access_token: "access".into(),
                refresh_token: "refresh".into(),
            },
            &Claims {
                account_id: "account".into(),
                ..Claims::default()
            },
        )
        .build()
        .unwrap();

        assert!(request.headers().get("originator").is_none());
        assert!(request.headers().get("user-agent").is_none());
        assert_eq!(request.headers()[device::SESSION_ID_HEADER], "session-1");
        assert_eq!(
            request.headers()[device::ROUTING_HINT_HEADER],
            "model=gpt-5.6-luna;tier=priority"
        );
        assert!(request.headers().get("version").is_none());
        assert!(
            request
                .headers()
                .get("x-openai-internal-codex-responses-lite")
                .is_none()
        );
    }

    #[test]
    fn upstream_request_omits_invalid_session_header() {
        let request = codex_request(
            &reqwest::Client::new(),
            hyper::body::Bytes::from_static(
                br#"{"model":"gpt-5","stream":true,"prompt_cache_key":"bad\\nkey"}"#,
            ),
            &RequestMetadata {
                operation: Operation::Responses,
                model: "gpt-5".into(),
                prompt_cache_key: Some("bad\nkey".into()),
                service_tier: None,
                reasoning_effort: None,
                identity_headers: Default::default(),
            },
            &CodexCredentials {
                version: 1,
                access_token: "access".into(),
                refresh_token: "refresh".into(),
            },
            &Claims {
                account_id: "account".into(),
                ..Claims::default()
            },
        )
        .build()
        .unwrap();

        assert!(request.headers().get(device::SESSION_ID_HEADER).is_none());
        assert_eq!(
            request.headers()[device::ROUTING_HINT_HEADER],
            "model=gpt-5"
        );
    }

    #[test]
    fn copilot_request_uses_versioned_api_without_default_identity() {
        let request = copilot_request(
            &reqwest::Client::new(),
            br#"{"model":"gpt-5.6-luna","stream":true}"#,
            &metadata(),
            "https://api.individual.githubcopilot.com",
            "access",
        )
        .build()
        .unwrap();

        assert_eq!(
            request.url().as_str(),
            "https://api.individual.githubcopilot.com/responses"
        );
        assert!(request.headers().get("user-agent").is_none());
        assert_eq!(
            request.headers()["X-GitHub-Api-Version"],
            copilot::API_VERSION
        );
        for name in [
            "OpenAI-Intent",
            "X-Initiator",
            "X-Interaction-Type",
            "Copilot-Vision-Request",
            "Editor-Version",
            "Editor-Plugin-Version",
            "Copilot-Integration-Id",
            "X-Request-Id",
            "originator",
        ] {
            assert!(request.headers().get(name).is_none(), "{name}");
        }
    }

    #[test]
    fn upstream_error_detail_accepts_json_and_plain_text() {
        assert_eq!(
            upstream_error_detail(br#"{"error":{"message":"model unavailable"}}"#).as_deref(),
            Some("model unavailable")
        );
        assert_eq!(
            upstream_error_detail(b"requires\na newer\tclient").as_deref(),
            Some("requires a newer client")
        );
        assert_eq!(upstream_error_detail(br#"{"unknown":true}"#), None);
    }

    #[tokio::test]
    async fn upstream_error_preserves_status_and_retry_headers() {
        use http_body_util::BodyExt;

        let retry_after = reqwest::header::HeaderValue::from_static("120");
        let response = upstream_error_response(
            429,
            "upstream returned 429: usage limit reached",
            "local-request",
            Some(&retry_after),
            Some("upstream-request"),
        )
        .unwrap();

        assert_eq!(response.status(), hyper::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[hyper::header::RETRY_AFTER], "120");
        assert_eq!(response.headers()["x-request-id"], "upstream-request");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "proxy_error");
    }

    fn test_audit() -> (std::path::PathBuf, crate::audit::RequestAudit) {
        let mut random = [0u8; 8];
        getrandom::fill(&mut random).unwrap();
        let path =
            std::env::temp_dir().join(format!("c2a-stream-audit-{}", u64::from_ne_bytes(random)));
        let mut audit = crate::audit::RequestAudit::new(
            crate::audit::open(&path).unwrap(),
            "codex",
            "local".into(),
        );
        audit.record.model = "requested-alias".into();
        audit.begin_relay();
        (path, audit)
    }

    #[tokio::test]
    async fn stream_statistics_preserve_bytes_and_actual_response_metadata() {
        use http_body_util::BodyExt;

        let bytes = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"model\":\"served-model\",\"service_tier\":\"default\",\"reasoning\":{\"effort\":\"medium\"}}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"content-not-retained\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"usage\":{\"input_tokens\":100,\"output_tokens\":20,\"total_tokens\":120,\"input_tokens_details\":{\"cached_tokens\":80},\"output_tokens_details\":{\"reasoning_tokens\":5}}}}\n\n",
        ).as_bytes().to_vec();
        let mut upstream: reqwest::Response = hyper::Response::new(bytes.clone()).into();
        let (path, mut audit) = test_audit();
        let (mut sender, body) = http_body_util::Channel::new(4);
        audit.record.requested_reasoning_effort = Some("high".into());
        let (_shutdown, mut receiver) = tokio::sync::watch::channel(false);
        super::observe_stream(&mut upstream, &mut sender, &mut audit, &mut receiver).await;
        drop(sender);
        assert_eq!(body.collect().await.unwrap().to_bytes(), bytes);
        audit.finish().await;
        let line = std::fs::read_to_string(&path).unwrap();
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["model"], "requested-alias");
        assert_eq!(value["response_model"], "served-model");
        assert_eq!(value["response_service_tier"], "default");
        assert_eq!(value["requested_reasoning_effort"], "high");
        assert_eq!(value["response_reasoning_effort"], "medium");
        assert_eq!(value["cached_input_tokens"], 80);
        assert_eq!(value["reasoning_output_tokens"], 5);
        assert_eq!(value["total_tokens"], 120);
        assert_eq!(value["outcome"], "completed");
        assert_eq!(value["response_bytes"], bytes.len());
        let times = [
            "relay_start_ms",
            "first_upstream_byte_ms",
            "first_output_ms",
            "last_output_ms",
            "terminal_ms",
            "duration_ms",
        ]
        .map(|field| value[field].as_f64().unwrap());
        assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(!line.contains("content-not-retained"));
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn stream_disconnect_and_bounded_error_reads_retain_statistics() {
        let (path, mut audit) = test_audit();
        let mut response: reqwest::Response = hyper::Response::new(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n".to_vec(),
        )
        .into();
        let (mut sender, body) = http_body_util::Channel::new(4);
        drop(body);
        let (_shutdown, mut receiver) = tokio::sync::watch::channel(false);
        super::observe_stream(&mut response, &mut sender, &mut audit, &mut receiver).await;
        assert_eq!(audit.record.outcome, "client_disconnect");
        assert!(audit.record.first_output_ms.is_some());
        assert!(audit.record.terminal_ms.is_none());
        audit.finish().await;
        std::fs::remove_file(path).unwrap();

        let (path, mut audit) = test_audit();
        let mut response: reqwest::Response = hyper::Response::builder()
            .status(429)
            .header("x-request-id", "upstream-id")
            .body(b"error detail".to_vec())
            .unwrap()
            .into();
        super::record_upstream(&response, &mut audit.record);
        let bytes = super::limited_observed(&mut response, 5, &mut audit)
            .await
            .unwrap();
        assert_eq!(bytes, b"error");
        assert_eq!(audit.record.response_bytes, 5);
        assert_eq!(audit.record.upstream_http_status, Some(429));
        assert_eq!(
            audit.record.upstream_request_id.as_deref(),
            Some("upstream-id")
        );
        assert!(audit.record.first_upstream_byte_ms.is_some());
        assert!(audit.record.first_output_ms.is_none());
        audit.finish().await;
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn accepts_missing_but_not_wrong_content_type() {
        let event_stream = reqwest::header::HeaderValue::from_static("text/event-stream");
        let json = reqwest::header::HeaderValue::from_static("application/json");

        assert!(accepts_content_type(Some(&event_stream)));
        assert!(accepts_content_type(None));
        assert!(!accepts_content_type(Some(&json)));
    }
}
