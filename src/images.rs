use std::{
    borrow::Cow,
    future::{Future, pending, poll_fn},
    pin::pin,
    task::Poll,
    time::Duration,
};

use http_body_util::{BodyExt, Channel};
use hyper::{
    Response,
    body::Bytes,
    header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use tokio::sync::{OwnedSemaphorePermit, oneshot, watch};

use crate::{
    audit::{self, Audit, AuditRecord},
    error::{Error, Result},
    relay::{self, Body, Operation, RequestMetadata},
};

pub const BODY_LIMIT: usize = 64 * 1024 * 1024;
const RESPONSE_LIMIT: usize = 64 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(600);
const MAX_EDIT_IMAGES: usize = 5;

// Derived structs also accept JSON arrays. Require objects without building a
// serde_json::Value tree that would copy the image strings and unknown fields.
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = Object<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                T::deserialize(de::value::MapAccessDeserializer::new(map)).map(Object)
            }
        }
        deserializer.deserialize_map(ObjectVisitor(std::marker::PhantomData))
    }
}

struct NonemptyString;

impl<'de> Deserialize<'de> for NonemptyString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StringVisitor;
        impl Visitor<'_> for StringVisitor {
            type Value = NonemptyString;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a nonempty string")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                if value.is_empty() {
                    Err(E::custom("empty string"))
                } else {
                    Ok(NonemptyString)
                }
            }
        }
        deserializer.deserialize_str(StringVisitor)
    }
}

// Validate array entries one at a time rather than allocating a Vec for every
// small object in a potentially 64 MiB document.
struct Count<T, const MAX: usize>(usize, std::marker::PhantomData<T>);

impl<T, const MAX: usize> Default for Count<T, MAX> {
    fn default() -> Self {
        Self(0, std::marker::PhantomData)
    }
}

impl<'de, T: Deserialize<'de>, const MAX: usize> Deserialize<'de> for Count<T, MAX> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct CountVisitor<T, const MAX: usize>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>, const MAX: usize> Visitor<'de> for CountVisitor<T, MAX> {
            type Value = Count<T, MAX>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an image array")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut count = 0;
                while sequence.next_element::<T>()?.is_some() {
                    if count == MAX {
                        return Err(de::Error::custom("too many images"));
                    }
                    count += 1;
                }
                Ok(Count(count, std::marker::PhantomData))
            }
        }
        deserializer.deserialize_seq(CountVisitor(std::marker::PhantomData))
    }
}

#[derive(Deserialize)]
struct ImageRequest<'a> {
    #[serde(borrow)]
    model: Cow<'a, str>,
    #[serde(rename = "prompt")]
    _prompt: NonemptyString,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    images: Count<Object<ImageUrl>, MAX_EDIT_IMAGES>,
}

#[derive(Deserialize)]
struct ImageUrl {
    #[serde(rename = "image_url")]
    _image_url: NonemptyString,
}

pub fn validate(
    bytes: &[u8],
    operation: Operation,
) -> std::result::Result<RequestMetadata, &'static str> {
    let Object(request): Object<ImageRequest<'_>> = serde_json::from_slice(bytes)
        .map_err(|_| "image request requires a model, nonempty prompt, and at most five images with nonempty image_url strings")?;
    if request.model.is_empty() {
        return Err("model must be a nonempty string");
    }
    if request.stream {
        return Err("image streaming is not supported; omit stream or set it to false");
    }
    if operation == Operation::ImageEdits && request.images.0 == 0 {
        return Err("images must contain one to five objects with nonempty image_url strings");
    }
    Ok(RequestMetadata {
        operation,
        model: request.model.into_owned(),
        prompt_cache_key: None,
        service_tier: None,
    })
}

#[derive(Deserialize)]
struct ImageResponse {
    data: Count<Object<ImageData>, { usize::MAX }>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct ImageData {
    #[serde(rename = "b64_json")]
    _b64_json: NonemptyString,
}

#[derive(Default, Deserialize)]
struct Usage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

fn observe(bytes: &[u8], record: &mut AuditRecord) -> Result<()> {
    let Object(response): Object<ImageResponse> = serde_json::from_slice(bytes)
        .map_err(|_| Error::message("upstream returned invalid image JSON"))?;
    if response.data.0 == 0 {
        return Err(Error::message("upstream returned no image data"));
    }
    if let Some(usage) = response.usage {
        record.input_tokens = usage.input_tokens;
        record.output_tokens = usage.output_tokens;
    }
    Ok(())
}

// The task owns admission permits and records cancellation even if Hyper drops
// the service future while the non-streaming upstream request is still pending.
pub async fn start(
    audit: Audit,
    mut record: AuditRecord,
    permits: Vec<OwnedSemaphorePermit>,
    mut shutdown: watch::Receiver<bool>,
    send: impl Future<Output = Result<reqwest::Response>> + Send + 'static,
) -> Result<Response<Body>> {
    let (mut reply, receive) = oneshot::channel();
    tokio::spawn(async move {
        let _permits = permits;
        let work = async {
            let response = send.await?;
            read_response(response, &mut record, RESPONSE_LIMIT).await
        };
        let result = interrupt(work, &mut shutdown, reply.closed()).await;
        match result {
            Ok(Ok((mut response, bytes))) => {
                if let Some(bytes) = bytes {
                    let (mut sender, body) = Channel::new(1);
                    *response.body_mut() = body.boxed();
                    if reply.send(Ok(response)).is_err() {
                        record.outcome = "client_disconnect".into();
                    } else {
                        record.outcome = "completed".into();
                        for chunk in bytes.chunks(64 * 1024) {
                            // Copy small output chunks so queued data cannot retain the
                            // entire image allocation after admission is released.
                            match interrupt(
                                sender.send_data(Bytes::copy_from_slice(chunk)),
                                &mut shutdown,
                                pending(),
                            )
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(_)) => {
                                    record.outcome = "client_disconnect".into();
                                    break;
                                }
                                Err(outcome) => {
                                    record.outcome = outcome.into();
                                    break;
                                }
                            }
                        }
                    }
                } else if reply.send(Ok(response)).is_err() {
                    record.outcome = "client_disconnect".into();
                }
            }
            Ok(Err(error)) => {
                let _ = reply.send(Err(error));
            }
            Err(outcome) => {
                record.outcome = outcome.into();
                let _ = reply.send(Err(Error::message(format!(
                    "image request ended: {outcome}"
                ))));
            }
        }
        record.finished_at = audit::timestamp();
        if let Err(error) = audit::append(&audit, &record).await {
            eprintln!("c2a: audit append failed: {error}");
        }
    });
    receive
        .await
        .map_err(|_| Error::message("image relay task stopped"))?
}

async fn interrupt<F: Future>(
    work: F,
    shutdown: &mut watch::Receiver<bool>,
    cancelled: impl Future<Output = ()>,
) -> std::result::Result<F::Output, &'static str> {
    if *shutdown.borrow() {
        return Err("shutdown");
    }
    let mut changed = pin!(shutdown.changed());
    let mut cancelled = pin!(cancelled);
    let mut work = pin!(work);
    let race = poll_fn(|context| {
        if changed.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err("shutdown"));
        }
        if cancelled.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err("client_disconnect"));
        }
        work.as_mut().poll(context).map(Ok)
    });
    tokio::time::timeout(DEADLINE, race)
        .await
        .unwrap_or(Err("premature_eof"))
}

async fn read_response(
    mut response: reqwest::Response,
    record: &mut AuditRecord,
    limit: usize,
) -> Result<(Response<Body>, Option<Bytes>)> {
    let status = response.status();
    record.upstream_http_status = Some(status.as_u16());
    record.upstream_request_id = relay::sanitize_request_id(
        response
            .headers()
            .get("x-request-id")
            .or_else(|| response.headers().get("x-oai-request-id"))
            .and_then(|value| value.to_str().ok()),
    );
    record.upstream_imagegen_request_id = relay::sanitize_request_id(
        response
            .headers()
            .get("x-codex-imagegen-request-id")
            .and_then(|value| value.to_str().ok()),
    );
    record.outcome = "upstream_error".into();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .cloned();
        let bytes = relay::limited(&mut response, relay::ERROR_LIMIT).await?;
        record.response_bytes = bytes.len() as u64;
        let detail = relay::upstream_error_detail(&bytes)
            .map(|detail| format!(": {detail}"))
            .unwrap_or_default();
        let response = relay::upstream_error_response(
            status.as_u16(),
            &format!("upstream returned {}{detail}", status.as_u16()),
            &record.request_id,
            retry_after.as_ref(),
            record.upstream_request_id.as_deref(),
        )?;
        return Ok((response, None));
    }
    if let Some(content_type) = response.headers().get(CONTENT_TYPE)
        && !content_type.to_str().ok().is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return Err(Error::message("upstream did not return application/json"));
    }
    let cache_control = response.headers().get(CACHE_CONTROL).cloned();
    let mut bytes = Vec::new();
    record.outcome = "premature_eof".into();
    while let Some(chunk) = response.chunk().await? {
        record.response_bytes += chunk.len() as u64;
        if chunk.len() > limit - bytes.len() {
            record.outcome = "uninspected".into();
            return Err(Error::message("upstream image response exceeds size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    record.outcome = "uninspected".into();
    observe(&bytes, record)?;
    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_LENGTH, bytes.len());
    if let Some(value) = cache_control {
        builder = builder.header(CACHE_CONTROL, value);
    }
    if let Some(value) = &record.upstream_request_id {
        builder = builder.header("x-request-id", value);
    }
    if let Some(value) = &record.upstream_imagegen_request_id {
        builder = builder.header("x-codex-imagegen-request-id", value);
    }
    let response = builder
        .body(relay::full(Bytes::new()))
        .map_err(|error| Error::message(error.to_string()))?;
    Ok((response, Some(Bytes::from(bytes))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, sync::Arc};
    use tokio::sync::Semaphore;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn upstream(bytes: Vec<u8>) -> reqwest::Response {
        Response::builder()
            .header("x-request-id", "outer-id")
            .header("x-codex-imagegen-request-id", "image-id")
            .body(bytes)
            .unwrap()
            .into()
    }

    fn image_bytes() -> Vec<u8> {
        format!(r#"{{ "data": [{{"b64_json":"{}", "generation_id":"not-a-response-id"}}], "usage":{{"input_tokens":3,"output_tokens":7}}, "future_field":true }}"#, "A".repeat(300 * 1024)).into_bytes()
    }

    #[test]
    fn native_image_validation_preserves_options_and_rejects_wrong_shapes() {
        let bytes = br#" { "model":"gpt-image-2.5-flare", "prompt":"snowman \u2603", "quality":"xhigh", "future": [1,2] } "#;
        let metadata = validate(bytes, Operation::ImageGenerations).unwrap();
        assert_eq!(metadata.model, "gpt-image-2.5-flare");
        for invalid in [
            br#"["model","prompt",false,[]]"#.as_slice(),
            br#"{"model":"m","prompt":"p","stream":true}"#,
            br#"{"model":"m","prompt":"p","stream":null}"#,
            br#"{"model":"m","prompt":""}"#,
        ] {
            assert!(validate(invalid, Operation::ImageGenerations).is_err());
        }
        for count in [0, 1, 5, 6] {
            let body = serde_json::to_vec(&serde_json::json!({
                "model":"m", "prompt":"p", "stream":false,
                "images": vec![serde_json::json!({"image_url":"data:image/png;base64,AA=="}); count]
            }))
            .unwrap();
            assert_eq!(
                validate(&body, Operation::ImageEdits).is_ok(),
                (1..=5).contains(&count)
            );
        }
        assert!(
            validate(
                br#"{"model":"m","prompt":"p","images":[["url"]]}"#,
                Operation::ImageEdits
            )
            .is_err()
        );
    }

    #[test]
    fn image_response_is_byte_preserving_bounded_and_metadata_only() {
        runtime().block_on(async {
            let bytes = image_bytes();
            let mut record = AuditRecord::default();
            let (response, body) = read_response(upstream(bytes.clone()), &mut record, bytes.len())
                .await
                .unwrap();
            assert_eq!(body.unwrap(), bytes);
            assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
            assert_eq!(response.headers()["x-request-id"], "outer-id");
            assert_eq!(
                response.headers()["x-codex-imagegen-request-id"],
                "image-id"
            );
            assert_eq!(record.response_bytes, bytes.len() as u64);
            assert_eq!(record.input_tokens, Some(3));
            assert_eq!(record.output_tokens, Some(7));
            assert!(record.response_id.is_none());
            assert!(
                read_response(upstream(bytes.clone()), &mut record, bytes.len() - 1)
                    .await
                    .is_err()
            );
            for invalid in [
                br#"{"data":[]}"#.as_slice(),
                br#"{"data":[{"b64_json":""}]}"#,
                br#"{"data":[["abc"]]}"#,
                b"{truncated",
            ] {
                assert!(
                    read_response(upstream(invalid.to_vec()), &mut record, 1024)
                        .await
                        .is_err()
                );
                assert_eq!(record.outcome, "uninspected");
            }
            let wrong_type = Response::builder()
                .header(CONTENT_TYPE, "text/event-stream")
                .body(bytes)
                .unwrap()
                .into();
            assert!(
                read_response(wrong_type, &mut record, RESPONSE_LIMIT)
                    .await
                    .is_err()
            );
            let error = Response::builder()
                .status(429)
                .header("retry-after", "12")
                .body(br#"{"error":{"message":"quota exceeded"}}"#.to_vec())
                .unwrap()
                .into();
            let (response, body) = read_response(error, &mut record, RESPONSE_LIMIT)
                .await
                .unwrap();
            assert_eq!(response.status(), 429);
            assert_eq!(response.headers()["retry-after"], "12");
            assert!(body.is_none());
            assert_eq!(record.outcome, "upstream_error");
        });
    }

    #[test]
    fn image_tasks_release_permits_and_audit_delivery_cancellation_and_shutdown() {
        runtime().block_on(async {
            for scenario in [
                "completed",
                "client_disconnect",
                "shutdown",
                "cancel_before_headers",
            ] {
                let mut random = [0u8; 8];
                getrandom::fill(&mut random).unwrap();
                let path = std::env::temp_dir()
                    .join(format!("c2a-image-test-{}", u64::from_ne_bytes(random)));
                let audit = audit::open(&path).unwrap();
                let slots = Arc::new(Semaphore::new(1));
                let permits = vec![slots.clone().try_acquire_owned().unwrap()];
                let (shutdown, receiver) = watch::channel(false);
                let record = AuditRecord {
                    operation: "images.generations".into(),
                    ..AuditRecord::default()
                };
                if scenario == "cancel_before_headers" || scenario == "shutdown" {
                    let (entered, started) = oneshot::channel();
                    let task = tokio::spawn(start(audit, record, permits, receiver, async {
                        let _ = entered.send(());
                        pending().await
                    }));
                    started.await.unwrap();
                    if scenario == "shutdown" {
                        shutdown.send(true).unwrap();
                        assert!(task.await.unwrap().is_err());
                    } else {
                        task.abort();
                        let _ = task.await;
                    }
                } else {
                    let response = start(audit, record, permits, receiver, async {
                        Ok(upstream(image_bytes()))
                    })
                    .await
                    .unwrap();
                    if scenario == "completed" {
                        assert_eq!(
                            response.into_body().collect().await.unwrap().to_bytes(),
                            image_bytes()
                        );
                    } else {
                        drop(response);
                    }
                }
                let _permit = tokio::time::timeout(Duration::from_secs(2), slots.acquire_owned())
                    .await
                    .unwrap()
                    .unwrap();
                let lines = fs::read_to_string(&path).unwrap();
                assert_eq!(lines.lines().count(), 1);
                let value: serde_json::Value = serde_json::from_str(&lines).unwrap();
                let expected = if scenario == "cancel_before_headers" {
                    "client_disconnect"
                } else {
                    scenario
                };
                assert_eq!(value["outcome"], expected);
                fs::remove_file(path).unwrap();
            }
        });
    }
}
