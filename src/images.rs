use std::{
    borrow::Cow,
    future::{Future, pending, poll_fn},
    pin::{Pin, pin},
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
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use tokio::sync::{OwnedSemaphorePermit, oneshot, watch};

use crate::{
    audit::{AuditRecord, RequestAudit},
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
        identity_headers: Default::default(),
    })
}

#[derive(Deserialize)]
struct ImageResponse {
    data: Count<Object<ImageData>, { usize::MAX }>,
    usage: Option<Usage>,
    #[serde(default)]
    model: ObservedString,
    #[serde(default)]
    service_tier: ObservedString,
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
    #[serde(default)]
    input_tokens_details: UsageDetails,
    #[serde(default)]
    output_tokens_details: UsageDetails,
    #[serde(default)]
    total_tokens: ObservedU64,
}

#[derive(Default)]
struct ObservedString(Option<String>);

macro_rules! ignore_simple_values {
    ($value:expr) => {
        fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            while sequence.next_element::<IgnoredAny>()?.is_some() {}
            Ok($value)
        }
    };
}

macro_rules! ignore_integer_values {
    ($value:expr) => {
        fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
    };
}

macro_rules! ignore_string_values {
    ($value:expr) => {
        fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
        fn visit_string<E: de::Error>(self, _: String) -> std::result::Result<Self::Value, E> {
            Ok($value)
        }
    };
}

macro_rules! ignore_map_values {
    ($value:expr) => {
        fn visit_map<A: MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            while map.next_key::<IgnoredAny>()?.is_some() {
                map.next_value::<IgnoredAny>()?;
            }
            Ok($value)
        }
    };
}

impl<'de> Deserialize<'de> for ObservedString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StringVisitor;
        impl<'de> Visitor<'de> for StringVisitor {
            type Value = ObservedString;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an optional nonempty string")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(ObservedString(
                    (!value.is_empty()).then(|| value.to_owned()),
                ))
            }
            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(ObservedString((!value.is_empty()).then_some(value)))
            }
            ignore_integer_values!(ObservedString(None));
            ignore_simple_values!(ObservedString(None));
            ignore_map_values!(ObservedString(None));
        }
        deserializer.deserialize_any(StringVisitor)
    }
}

#[derive(Default)]
struct ObservedU64(Option<u64>);

impl<'de> Deserialize<'de> for ObservedU64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct U64Visitor;
        impl<'de> Visitor<'de> for U64Visitor {
            type Value = ObservedU64;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an optional unsigned integer")
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(ObservedU64(Some(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(ObservedU64(u64::try_from(value).ok()))
            }
            ignore_string_values!(ObservedU64(None));
            ignore_simple_values!(ObservedU64(None));
            ignore_map_values!(ObservedU64(None));
        }
        deserializer.deserialize_any(U64Visitor)
    }
}

#[derive(Default)]
struct UsageDetails {
    cached_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
}

impl<'de> Deserialize<'de> for UsageDetails {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct DetailsVisitor;
        impl<'de> Visitor<'de> for DetailsVisitor {
            type Value = UsageDetails;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("optional usage details")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut details = UsageDetails::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "cached_tokens" => {
                            details.cached_tokens = map.next_value::<ObservedU64>()?.0;
                        }
                        "reasoning_tokens" => {
                            details.reasoning_tokens = map.next_value::<ObservedU64>()?.0;
                        }
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(details)
            }
            ignore_integer_values!(UsageDetails::default());
            ignore_string_values!(UsageDetails::default());
            ignore_simple_values!(UsageDetails::default());
        }
        deserializer.deserialize_any(DetailsVisitor)
    }
}

fn observe(bytes: &[u8], record: &mut AuditRecord) -> Result<()> {
    let Object(response): Object<ImageResponse> = serde_json::from_slice(bytes)
        .map_err(|_| Error::message("upstream returned invalid image JSON"))?;
    if response.data.0 == 0 {
        return Err(Error::message("upstream returned no image data"));
    }
    record.response_model = response.model.0;
    record.response_service_tier = response.service_tier.0;
    if let Some(usage) = response.usage {
        record.input_tokens = usage.input_tokens;
        record.output_tokens = usage.output_tokens;
        record.cached_input_tokens = usage.input_tokens_details.cached_tokens;
        record.reasoning_output_tokens = usage.output_tokens_details.reasoning_tokens;
        record.total_tokens = usage.total_tokens.0;
    }
    Ok(())
}

// The task owns admission permits and records cancellation even if Hyper drops
// the service future while the non-streaming upstream request is still pending.
pub async fn start(
    mut audit: RequestAudit,
    permits: Vec<OwnedSemaphorePermit>,
    mut shutdown: watch::Receiver<bool>,
    send: impl for<'a> FnOnce(
        &'a mut AuditRecord,
    )
        -> Pin<Box<dyn Future<Output = Result<reqwest::Response>> + Send + 'a>>
    + Send
    + 'static,
) -> Result<Response<Body>> {
    let (mut reply, receive) = oneshot::channel();
    tokio::spawn(async move {
        let _permits = permits;
        let work = async {
            let response = send(&mut audit.record).await?;
            read_response(response, &mut audit, RESPONSE_LIMIT).await
        };
        let result = interrupt(work, &mut shutdown, reply.closed()).await;
        match result {
            Ok(Ok((mut response, bytes))) => {
                let status = response.status().as_u16();
                if let Some(bytes) = bytes {
                    let (mut sender, body) = Channel::new(1);
                    *response.body_mut() = body.boxed();
                    if reply.send(Ok(response)).is_err() {
                        audit.record.outcome = "client_disconnect".into();
                    } else {
                        audit.record.local_http_status = Some(status);
                        audit.record.outcome = "completed".into();
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
                                    audit.record.outcome = "client_disconnect".into();
                                    break;
                                }
                                Err(outcome) => {
                                    audit.record.outcome = outcome.into();
                                    break;
                                }
                            }
                        }
                    }
                } else if reply.send(Ok(response)).is_err() {
                    audit.record.outcome = "client_disconnect".into();
                } else {
                    audit.record.local_http_status = Some(status);
                }
            }
            Ok(Err(error)) => {
                if reply.send(Err(error)).is_ok() {
                    audit.record.local_http_status = Some(502);
                } else {
                    audit.record.outcome = "client_disconnect".into();
                }
            }
            Err(outcome) => {
                audit.record.outcome = outcome.into();
                if reply
                    .send(Err(Error::message(format!(
                        "image request ended: {outcome}"
                    ))))
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
    audit: &mut RequestAudit,
    limit: usize,
) -> Result<(Response<Body>, Option<Bytes>)> {
    let status = response.status();
    audit.record.upstream_http_status = Some(status.as_u16());
    audit.record.upstream_request_id = relay::sanitize_request_id(
        response
            .headers()
            .get("x-request-id")
            .or_else(|| response.headers().get("x-oai-request-id"))
            .and_then(|value| value.to_str().ok()),
    );
    audit.record.upstream_imagegen_request_id = relay::sanitize_request_id(
        response
            .headers()
            .get("x-codex-imagegen-request-id")
            .and_then(|value| value.to_str().ok()),
    );
    audit.record.outcome = "upstream_error".into();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .cloned();
        let bytes = relay::limited_observed(&mut response, relay::ERROR_LIMIT, audit).await?;
        let detail = relay::upstream_error_detail(&bytes)
            .map(|detail| format!(": {detail}"))
            .unwrap_or_default();
        let response = relay::upstream_error_response(
            status.as_u16(),
            &format!("upstream returned {}{detail}", status.as_u16()),
            &audit.record.request_id,
            retry_after.as_ref(),
            audit.record.upstream_request_id.as_deref(),
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
    audit.record.outcome = "premature_eof".into();
    while let Some(chunk) = response.chunk().await? {
        audit.record.response_bytes += chunk.len() as u64;
        if !chunk.is_empty() {
            let elapsed = audit.elapsed_ms();
            audit.record.first_upstream_byte_ms.get_or_insert(elapsed);
        }
        if chunk.len() > limit - bytes.len() {
            audit.record.outcome = "uninspected".into();
            return Err(Error::message("upstream image response exceeds size limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    audit.record.outcome = "uninspected".into();
    observe(&bytes, &mut audit.record)?;
    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_LENGTH, bytes.len());
    if let Some(value) = cache_control {
        builder = builder.header(CACHE_CONTROL, value);
    }
    if let Some(value) = &audit.record.upstream_request_id {
        builder = builder.header("x-request-id", value);
    }
    if let Some(value) = &audit.record.upstream_imagegen_request_id {
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

    fn test_request_audit() -> (RequestAudit, std::path::PathBuf) {
        let mut random = [0u8; 8];
        getrandom::fill(&mut random).unwrap();
        let path =
            std::env::temp_dir().join(format!("c2a-image-test-{}", u64::from_ne_bytes(random)));
        let audit = crate::audit::open(&path).unwrap();
        (RequestAudit::new(audit, "codex", "image-test".into()), path)
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
            let (mut audit, path) = test_request_audit();
            let (response, body) = read_response(upstream(bytes.clone()), &mut audit, bytes.len())
                .await
                .unwrap();
            assert_eq!(body.unwrap(), bytes);
            assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
            assert_eq!(response.headers()["x-request-id"], "outer-id");
            assert_eq!(
                response.headers()["x-codex-imagegen-request-id"],
                "image-id"
            );
            assert_eq!(audit.record.response_bytes, bytes.len() as u64);
            assert_eq!(audit.record.input_tokens, Some(3));
            assert_eq!(audit.record.output_tokens, Some(7));
            assert!(audit.record.first_upstream_byte_ms.is_some());
            assert!(audit.record.response_id.is_none());
            assert!(
                read_response(upstream(bytes.clone()), &mut audit, bytes.len() - 1)
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
                    read_response(upstream(invalid.to_vec()), &mut audit, 1024)
                        .await
                        .is_err()
                );
                assert_eq!(audit.record.outcome, "uninspected");
            }
            let wrong_type = Response::builder()
                .header(CONTENT_TYPE, "text/event-stream")
                .body(bytes)
                .unwrap()
                .into();
            assert!(
                read_response(wrong_type, &mut audit, RESPONSE_LIMIT)
                    .await
                    .is_err()
            );
            let error = Response::builder()
                .status(429)
                .header("retry-after", "12")
                .body(br#"{"error":{"message":"quota exceeded"}}"#.to_vec())
                .unwrap()
                .into();
            let (response, body) = read_response(error, &mut audit, RESPONSE_LIMIT)
                .await
                .unwrap();
            assert_eq!(response.status(), 429);
            assert_eq!(response.headers()["retry-after"], "12");
            assert!(body.is_none());
            assert_eq!(audit.record.outcome, "upstream_error");
            assert!(audit.record.first_upstream_byte_ms.is_some());
            audit.finish().await;
            fs::remove_file(path).unwrap();
        });
    }

    #[test]
    fn image_response_observes_optional_model_tier_and_token_usage() {
        let mut record = AuditRecord::default();
        observe(
            br#"{"data":[{"b64_json":"AA=="}],"model":"gpt-image-2.5","service_tier":"priority","usage":{"input_tokens":11,"output_tokens":13,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":5},"total_tokens":24}}"#,
            &mut record,
        )
        .unwrap();
        assert_eq!(record.response_model.as_deref(), Some("gpt-image-2.5"));
        assert_eq!(record.response_service_tier.as_deref(), Some("priority"));
        assert_eq!(record.input_tokens, Some(11));
        assert_eq!(record.output_tokens, Some(13));
        assert_eq!(record.cached_input_tokens, Some(3));
        assert_eq!(record.reasoning_output_tokens, Some(5));
        assert_eq!(record.total_tokens, Some(24));

        let mut missing = AuditRecord::default();
        observe(br#"{"data":[{"b64_json":"AA=="}]}"#, &mut missing).unwrap();
        assert!(missing.response_model.is_none());
        assert!(missing.response_service_tier.is_none());
        assert!(missing.input_tokens.is_none());
        assert!(missing.output_tokens.is_none());
        assert!(missing.cached_input_tokens.is_none());
        assert!(missing.reasoning_output_tokens.is_none());
        assert!(missing.total_tokens.is_none());

        let mut malformed_metadata = AuditRecord::default();
        observe(
            br#"{"data":[{"b64_json":"AA=="}],"model":{"unexpected":true},"service_tier":[],"usage":{"input_tokens":2,"output_tokens":4,"input_tokens_details":false,"output_tokens_details":[1],"total_tokens":{"unexpected":true}}}"#,
            &mut malformed_metadata,
        )
        .unwrap();
        assert!(malformed_metadata.response_model.is_none());
        assert!(malformed_metadata.response_service_tier.is_none());
        assert_eq!(malformed_metadata.input_tokens, Some(2));
        assert_eq!(malformed_metadata.output_tokens, Some(4));
        assert!(malformed_metadata.cached_input_tokens.is_none());
        assert!(malformed_metadata.reasoning_output_tokens.is_none());
        assert!(malformed_metadata.total_tokens.is_none());
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
                let audit_file = crate::audit::open(&path).unwrap();
                let slots = Arc::new(Semaphore::new(1));
                let permits = vec![slots.clone().try_acquire_owned().unwrap()];
                let (shutdown, receiver) = watch::channel(false);
                let mut audit = RequestAudit::new(audit_file, "codex", "image-test".into());
                audit.record.operation = "images.generations".into();
                if scenario == "cancel_before_headers" || scenario == "shutdown" {
                    let (entered, started) = oneshot::channel();
                    let task = tokio::spawn(start(audit, permits, receiver, move |record| {
                        record.upstream_attempts = Some(1);
                        Box::pin(async move {
                            let _ = entered.send(());
                            pending().await
                        })
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
                    let response = start(audit, permits, receiver, |record| {
                        record.upstream_attempts = Some(1);
                        Box::pin(async { Ok(upstream(image_bytes())) })
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
                assert_eq!(value["upstream_attempts"], 1);
                let expected_status = match scenario {
                    "completed" | "client_disconnect" => Some(200u16),
                    "shutdown" => Some(502),
                    _ => None,
                };
                assert_eq!(
                    value["local_http_status"],
                    expected_status.map_or(serde_json::Value::Null, serde_json::Value::from)
                );
                fs::remove_file(path).unwrap();
            }
        });
    }
}
