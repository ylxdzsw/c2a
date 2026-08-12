use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use http_body_util::{BodyExt, Channel, Full, combinators::BoxBody};
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{HeaderName, HeaderValue},
};
use tokio::sync::{OwnedSemaphorePermit, watch};

use crate::{
    audit::{self, Audit, AuditRecord},
    claims::Claims,
    device,
    error::{Error, Result},
    paths::Paths,
    refresh,
    storage::Credentials,
};

const ERROR_LIMIT: usize = 64 * 1024;
const ERROR_DETAIL_LIMIT: usize = 1024;
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

pub type Body = BoxBody<Bytes, Infallible>;

#[derive(Clone)]
pub struct Relay {
    client: reqwest::Client,
    paths: Paths,
    audit: Audit,
}

impl Relay {
    pub fn new(paths: Paths, audit: Audit) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .referer(false)
            .build()?;
        Ok(Self {
            client,
            paths,
            audit,
        })
    }

    pub async fn start(
        &self,
        body: Vec<u8>,
        model: String,
        request_id: String,
        permit: OwnedSemaphorePermit,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Response<Body>> {
        let started_at = audit::timestamp();
        let request_bytes = body.len() as u64;
        let (credentials, claims) = refresh::credentials(&self.client, &self.paths, false).await?;
        let mut response = self.send(&body, &credentials, &claims).await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            drop(response);
            let (credentials, claims) =
                refresh::credentials(&self.client, &self.paths, true).await?;
            response = self.send(&body, &credentials, &claims).await?;
        }

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
            let error_body = limited(&mut response, ERROR_LIMIT).await?;
            let response_bytes = error_body.len() as u64;
            self.record(AuditRecord {
                request_id,
                started_at,
                finished_at: audit::timestamp(),
                model,
                request_bytes,
                response_bytes,
                upstream_http_status: upstream_status,
                upstream_request_id,
                response_id: None,
                outcome: "upstream_error".into(),
                input_tokens: None,
                output_tokens: None,
            })
            .await;
            let detail = upstream_error_detail(&error_body)
                .map(|detail| format!(": {detail}"))
                .unwrap_or_default();
            return Err(Error::message(format!(
                "upstream returned {upstream_status}{detail}"
            )));
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .cloned();
        if !accepts_content_type(content_type.as_ref()) {
            let response_bytes = limited(&mut response, ERROR_LIMIT).await?.len() as u64;
            self.record(AuditRecord {
                request_id,
                started_at,
                finished_at: audit::timestamp(),
                model,
                request_bytes,
                response_bytes,
                upstream_http_status: upstream_status,
                upstream_request_id,
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
        let upstream_request_id_task = upstream_request_id.clone();
        tokio::spawn(async move {
            let _permit = permit;
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
                request_id,
                started_at,
                finished_at: audit::timestamp(),
                model,
                request_bytes,
                response_bytes,
                upstream_http_status: upstream_status,
                upstream_request_id: upstream_request_id_task,
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

    async fn send(
        &self,
        body: &[u8],
        credentials: &Credentials,
        claims: &Claims,
    ) -> Result<reqwest::Response> {
        Ok(upstream_request(&self.client, body, credentials, claims)
            .send()
            .await?)
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

fn upstream_request(
    client: &reqwest::Client,
    body: &[u8],
    credentials: &Credentials,
    claims: &Claims,
) -> reqwest::RequestBuilder {
    client
        .post(device::UPSTREAM)
        .bearer_auth(&credentials.access_token)
        .header("Chatgpt-Account-Id", &claims.account_id)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .header("originator", device::ORIGINATOR)
        .header(reqwest::header::USER_AGENT, device::USER_AGENT)
        .body(body.to_vec())
}

async fn limited(response: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while bytes.len() < limit {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        bytes.extend_from_slice(&chunk[..chunk.len().min(limit - bytes.len())]);
    }
    Ok(bytes)
}

fn upstream_error_detail(bytes: &[u8]) -> Option<String> {
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

fn sanitize_request_id(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 256
                && value.bytes().all(|byte| byte.is_ascii_graphic())
        })
        .map(str::to_owned)
}

pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).boxed()
}

#[cfg(test)]
mod tests {
    use super::{accepts_content_type, upstream_error_detail, upstream_request};
    use crate::{claims::Claims, device, storage::Credentials};

    #[test]
    fn upstream_request_uses_honest_identity() {
        let request = upstream_request(
            &reqwest::Client::new(),
            br#"{"model":"gpt-5.6-luna","stream":true}"#,
            &Credentials {
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
        assert_eq!(
            request.headers()[reqwest::header::USER_AGENT],
            device::USER_AGENT
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
    fn accepts_missing_but_not_wrong_content_type() {
        let event_stream = reqwest::header::HeaderValue::from_static("text/event-stream");
        let json = reqwest::header::HeaderValue::from_static("application/json");

        assert!(accepts_content_type(Some(&event_stream)));
        assert!(accepts_content_type(None));
        assert!(!accepts_content_type(Some(&json)));
    }
}
