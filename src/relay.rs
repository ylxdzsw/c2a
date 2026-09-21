use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use http_body_util::{BodyExt, Channel, Full, combinators::BoxBody};
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{CONTENT_TYPE, HeaderName, HeaderValue, RETRY_AFTER},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::{
    audit::{self, Audit, AuditRecord},
    claims::Claims,
    copilot, device,
    error::{ApiError, ApiErrorBody, Error, Result},
    images,
    paths::Paths,
    provider::{Provider, USER_AGENT},
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

    pub async fn start(
        self: &Arc<Self>,
        body: Bytes,
        metadata: RequestMetadata,
        request_id: String,
        permits: Vec<OwnedSemaphorePermit>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Response<Body>> {
        let started_at = audit::timestamp();
        let request_bytes = body.len() as u64;
        if metadata.operation != Operation::Responses {
            let record = AuditRecord {
                provider: self.provider.as_str().into(),
                operation: metadata.operation.as_str().into(),
                request_id,
                started_at,
                model: metadata.model.clone(),
                request_bytes,
                outcome: "failed".into(),
                ..AuditRecord::default()
            };
            let relay = self.clone();
            return images::start(self.audit.clone(), record, permits, shutdown, async move {
                relay.send_authenticated(&body, &metadata).await
            })
            .await;
        }
        let mut response = self.send_authenticated(&body, &metadata).await?;

        let RequestMetadata { model, .. } = metadata;

        let status = response.status();
        let upstream_status = status.as_u16();
        let upstream_request_id = sanitize_request_id(
            response
                .headers()
                .get("x-request-id")
                .or_else(|| response.headers().get("x-oai-request-id"))
                .and_then(|value| value.to_str().ok()),
        );
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .cloned();
            let error_body = limited(&mut response, ERROR_LIMIT).await?;
            let response_bytes = error_body.len() as u64;
            self.record(AuditRecord {
                provider: self.provider.as_str().into(),
                operation: Operation::Responses.as_str().into(),
                request_id: request_id.clone(),
                started_at,
                finished_at: audit::timestamp(),
                model,
                request_bytes,
                response_bytes,
                upstream_http_status: Some(upstream_status),
                upstream_request_id: upstream_request_id.clone(),
                upstream_imagegen_request_id: None,
                response_id: None,
                outcome: "upstream_error".into(),
                input_tokens: None,
                output_tokens: None,
            })
            .await;
            let detail = upstream_error_detail(&error_body)
                .map(|detail| format!(": {detail}"))
                .unwrap_or_default();
            let message = format!("upstream returned {upstream_status}{detail}");
            return upstream_error_response(
                upstream_status,
                &message,
                &request_id,
                retry_after.as_ref(),
                upstream_request_id.as_deref(),
            );
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .cloned();
        if !accepts_content_type(content_type.as_ref()) {
            let response_bytes = limited(&mut response, ERROR_LIMIT).await?.len() as u64;
            self.record(AuditRecord {
                provider: self.provider.as_str().into(),
                operation: Operation::Responses.as_str().into(),
                request_id,
                started_at,
                finished_at: audit::timestamp(),
                model,
                request_bytes,
                response_bytes,
                upstream_http_status: Some(upstream_status),
                upstream_request_id,
                upstream_imagegen_request_id: None,
                response_id: None,
                outcome: "upstream_error".into(),
                input_tokens: None,
                output_tokens: None,
            })
            .await;
            return Err(Error::message("upstream did not return text/event-stream"));
        }
        // The Codex backend can stream SSE without a Content-Type header.
        let content_type = content_type
            .unwrap_or_else(|| reqwest::header::HeaderValue::from_static("text/event-stream"));
        let cache_control = response
            .headers()
            .get(reqwest::header::CACHE_CONTROL)
            .cloned();
        let (mut sender, body_channel) = Channel::<Bytes, Infallible>::new(4);
        let audit = self.audit.clone();
        let provider = self.provider;
        let upstream_request_id_task = upstream_request_id.clone();
        tokio::spawn(async move {
            let _permits = permits;
            let mut observer = crate::sse::Observer::new();
            let mut response_bytes = 0u64;
            let mut outcome = None;
            loop {
                match next_or_shutdown(&mut response, &mut shutdown).await {
                    Next::Shutdown => {
                        outcome = Some("shutdown".to_string());
                        break;
                    }
                    Next::Chunk(Ok(Some(chunk))) => {
                        response_bytes += chunk.len() as u64;
                        observer.feed(&chunk);
                        if sender.send_data(chunk).await.is_err() {
                            outcome = Some("client_disconnect".to_string());
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
            let final_outcome = if observer.failed() {
                "uninspected".into()
            } else {
                outcome.unwrap_or_else(|| {
                    if let Some(value) = &observation.outcome {
                        value.clone()
                    } else {
                        "premature_eof".into()
                    }
                })
            };
            let record = AuditRecord {
                provider: provider.as_str().into(),
                operation: Operation::Responses.as_str().into(),
                request_id,
                started_at,
                finished_at: audit::timestamp(),
                model,
                request_bytes,
                response_bytes,
                upstream_http_status: Some(upstream_status),
                upstream_request_id: upstream_request_id_task,
                upstream_imagegen_request_id: None,
                response_id: observation.response_id.clone(),
                outcome: final_outcome,
                input_tokens: observation.input_tokens,
                output_tokens: observation.output_tokens,
            };
            if let Err(error) = audit::append(&audit, &record).await {
                eprintln!("c2a: audit append failed: {error}");
            }
        });

        let mut builder = Response::builder().status(
            StatusCode::from_u16(upstream_status)
                .map_err(|error| Error::message(error.to_string()))?,
        );
        if let Ok(value) = HeaderValue::from_bytes(content_type.as_bytes()) {
            builder = builder.header(hyper::header::CONTENT_TYPE, value);
        }
        if let Some(value) =
            cache_control.and_then(|value| HeaderValue::from_bytes(value.as_bytes()).ok())
        {
            builder = builder.header(hyper::header::CACHE_CONTROL, value);
        }
        if let Some(value) = upstream_request_id {
            builder = builder.header(HeaderName::from_static("x-request-id"), value);
        }
        builder
            .body(body_channel.boxed())
            .map_err(|error| Error::message(error.to_string()))
    }

    async fn send_authenticated(
        &self,
        body: &Bytes,
        metadata: &RequestMetadata,
    ) -> Result<reqwest::Response> {
        let mut sent = self.send(body, metadata, None).await?;
        if sent.response.status() == reqwest::StatusCode::UNAUTHORIZED {
            let rejected_access_token = sent.access_token;
            drop(sent.response);
            sent = self
                .send(body, metadata, Some(&rejected_access_token))
                .await?;
        }
        Ok(sent.response)
    }

    async fn send(
        &self,
        body: &Bytes,
        metadata: &RequestMetadata,
        rejected_access_token: Option<&str>,
    ) -> Result<Sent> {
        match self.provider {
            Provider::Codex => {
                let _guard = self.credentials.lock().await;
                let (credentials, claims) =
                    refresh::credentials(&self.client, &self.paths, rejected_access_token).await?;
                let access_token = credentials.access_token.clone();
                let request =
                    codex_request(&self.client, body.clone(), metadata, &credentials, &claims)
                        .build()?;
                drop(_guard);
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
                Ok(Sent {
                    response: copilot_request(
                        &self.client,
                        body,
                        &endpoint,
                        &credentials.access_token,
                    )
                    .send()
                    .await?,
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

    async fn record(&self, record: AuditRecord) {
        if let Err(error) = audit::append(&self.audit, &record).await {
            eprintln!("c2a: audit append failed: {error}");
        }
    }
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
        .header("originator", device::ORIGINATOR)
        .header(reqwest::header::USER_AGENT, USER_AGENT)
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
    endpoint: &str,
    access_token: &str,
) -> reqwest::RequestBuilder {
    client
        .post(format!("{endpoint}/responses"))
        .bearer_auth(access_token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .header(reqwest::header::USER_AGENT, USER_AGENT)
        .header("X-GitHub-Api-Version", copilot::API_VERSION)
        .body(body.to_vec())
}

fn valid_header_value(value: &str) -> Option<reqwest::header::HeaderValue> {
    reqwest::header::HeaderValue::try_from(value).ok()
}

pub(crate) async fn limited(response: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while bytes.len() < limit {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        bytes.extend_from_slice(&chunk[..chunk.len().min(limit - bytes.len())]);
    }
    Ok(bytes)
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
            r#type: "c2a_error",
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
        Operation, RequestMetadata, accepts_content_type, codex_request, copilot_request,
        upstream_error_detail, upstream_error_response,
    };
    use crate::{claims::Claims, copilot, device, provider::USER_AGENT, storage::CodexCredentials};

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
            assert_eq!(request.headers()["originator"], "c2a");
            assert_eq!(request.headers()["user-agent"], USER_AGENT);
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
    fn upstream_request_uses_honest_identity() {
        let request = codex_request(
            &reqwest::Client::new(),
            hyper::body::Bytes::from_static(br#"{"model":"gpt-5.6-luna","stream":true,"prompt_cache_key":"session-1","service_tier":"priority"}"#),
            &RequestMetadata {
                operation: Operation::Responses,
                model: "gpt-5.6-luna".into(),
                prompt_cache_key: Some("session-1".into()),
                service_tier: Some("priority".into()),
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

        assert_eq!(request.headers()["originator"], device::ORIGINATOR);
        assert_eq!(request.headers()[reqwest::header::USER_AGENT], USER_AGENT);
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
    fn copilot_request_uses_c2a_identity_and_versioned_api() {
        let request = copilot_request(
            &reqwest::Client::new(),
            br#"{"model":"gpt-5.6-luna","stream":true}"#,
            "https://api.individual.githubcopilot.com",
            "access",
        )
        .build()
        .unwrap();

        assert_eq!(
            request.url().as_str(),
            "https://api.individual.githubcopilot.com/responses"
        );
        assert_eq!(request.headers()[reqwest::header::USER_AGENT], USER_AGENT);
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

    #[test]
    fn upstream_error_preserves_status_and_retry_headers() {
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
