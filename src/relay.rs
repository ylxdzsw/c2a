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
                .and_then(|value| value.to_str().ok()),
        );
        if !status.is_success() {
            let response_bytes = limited(&mut response, ERROR_LIMIT).await? as u64;
            self.record(AuditRecord {
                schema_version: 1,
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
            return Err(Error::message(format!(
                "upstream returned {upstream_status}"
            )));
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .cloned();
        if !content_type
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
            })
        {
            let response_bytes = limited(&mut response, ERROR_LIMIT).await? as u64;
            self.record(AuditRecord {
                schema_version: 1,
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
        let content_type = content_type.unwrap();
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
                schema_version: 1,
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
        Ok(self
            .client
            .post(device::UPSTREAM)
            .bearer_auth(&credentials.access_token)
            .header("Chatgpt-Account-Id", &claims.account_id)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .header("originator", device::ORIGINATOR)
            .header(
                reqwest::header::USER_AGENT,
                format!("codex_cli_rs/{}", device::CODEX_VERSION),
            )
            .body(body.to_vec())
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

async fn limited(response: &mut reqwest::Response, limit: usize) -> Result<usize> {
    let mut total = 0usize;
    while total < limit {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        total += chunk.len().min(limit - total);
    }
    Ok(total)
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
