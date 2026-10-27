use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    os::unix::io::AsRawFd,
    path::Path,
    sync::Arc,
    time::Instant,
};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::paths::check_file;

const EVENT_LIMIT: usize = 256 * 1024;

#[derive(Debug, Default, Serialize)]
pub struct AuditRecord {
    pub provider: String,
    pub operation: String,
    pub request_id: String,
    pub started_at: String,
    pub finished_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay_start_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay_duration_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_upstream_byte_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_output_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_text_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_output_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_output_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_attempts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_retries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_http_status: Option<u16>,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_model: Option<String>,
    pub request_bytes: u64,
    pub response_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_imagegen_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_service_tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_service_tier: Option<String>,
}

pub type Audit = Arc<Mutex<File>>;

pub struct RequestAudit {
    pub record: AuditRecord,
    audit: Audit,
    receipt: Instant,
    finished: bool,
}

impl RequestAudit {
    pub fn new(audit: Audit, provider: &str, request_id: String) -> Self {
        let receipt = Instant::now();
        let received_at = timestamp();
        Self {
            record: AuditRecord {
                provider: provider.to_owned(),
                request_id,
                started_at: received_at.clone(),
                received_at: Some(received_at),
                outcome: "failed".into(),
                upstream_attempts: Some(0),
                auth_retries: Some(0),
                ..AuditRecord::default()
            },
            audit,
            receipt,
            finished: false,
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.receipt.elapsed().as_secs_f64() * 1000.0
    }

    pub fn begin_relay(&mut self) {
        let now = Instant::now();
        self.record.started_at = timestamp();
        self.record.relay_start_ms =
            Some(now.saturating_duration_since(self.receipt).as_secs_f64() * 1000.0);
    }

    fn finalize(&mut self) {
        let duration_ms = self.elapsed_ms();
        self.record.finished_at = timestamp();
        self.record.duration_ms = Some(duration_ms);
        self.record.relay_duration_ms = self
            .record
            .relay_start_ms
            .map(|relay_start_ms| (duration_ms - relay_start_ms).max(0.0));
    }

    pub async fn finish(mut self) {
        self.finalize();
        self.finished = true;
        let record = std::mem::take(&mut self.record);
        let audit = self.audit.clone();
        // Once finalization starts, cancellation of the caller must not lose the
        // record while it waits for the shared audit lock.
        let task = tokio::spawn(async move {
            if let Err(error) = append(&audit, &record).await {
                eprintln!("c2a: failed to append audit record: {error}");
            }
        });
        if let Err(error) = task.await {
            eprintln!("c2a: audit task failed: {error}");
        }
    }
}

impl Drop for RequestAudit {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if self.record.outcome != "shutdown" {
            self.record.outcome = "client_disconnect".into();
        }
        self.finalize();
        self.finished = true;
        let record = std::mem::take(&mut self.record);
        let audit = self.audit.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(error) = append(&audit, &record).await {
                        eprintln!("c2a: failed to append audit record: {error}");
                    }
                });
            }
            Err(error) => {
                eprintln!("c2a: failed to append audit record after drop: {error}");
            }
        }
    }
}

pub fn open(path: &Path) -> io::Result<Audit> {
    let created = OpenOptions::new()
        .create_new(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path);
    let file = match created {
        Ok(file) => {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .append(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?,
        Err(error) => return Err(error),
    };
    check_file(&file, path, 0o600).map_err(io::Error::other)?;
    Ok(Arc::new(Mutex::new(file)))
}

pub async fn append(audit: &Audit, record: &AuditRecord) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(record).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let mut file = audit.lock().await;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let result = file.write_all(&bytes);
    let unlock = if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    result.and(unlock)
}

pub fn timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    DateTime::<Utc>::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Debug, Default)]
pub struct Observation {
    pub response_id: Option<String>,
    pub response_model: Option<String>,
    pub response_service_tier: Option<String>,
    pub outcome: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub reasoning_output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub first_output_ms: Option<f64>,
    pub first_text_ms: Option<f64>,
    pub last_output_ms: Option<f64>,
    pub terminal_ms: Option<f64>,
    pub first_output_kind: Option<String>,
    pub terminal: bool,
}

pub struct SseObserver {
    buffer: Vec<u8>,
    failed: bool,
    pub observation: Observation,
}

impl SseObserver {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            failed: false,
            observation: Observation::default(),
        }
    }

    pub fn feed_at(&mut self, bytes: &[u8], elapsed_ms: f64) {
        if self.failed {
            return;
        }
        self.buffer.extend_from_slice(bytes);
        while let Some((position, separator)) = find_event(&self.buffer) {
            let event = self.buffer.drain(..position).collect::<Vec<_>>();
            self.buffer.drain(..separator);
            self.event(&event, elapsed_ms);
            if self.failed {
                self.buffer.clear();
                return;
            }
        }
        if self.buffer.len() > EVENT_LIMIT {
            self.failed = true;
            self.buffer.clear();
        }
    }

    pub fn finish(&mut self) {
        if !self.buffer.is_empty() {
            self.failed = true;
            self.buffer.clear();
        }
    }

    fn event(&mut self, bytes: &[u8], elapsed_ms: f64) {
        let Ok(text) = std::str::from_utf8(bytes) else {
            self.failed = true;
            return;
        };
        let data = text
            .split_terminator('\n')
            .filter_map(|line| {
                let line = line.strip_suffix('\r').unwrap_or(line);
                if line == "data" {
                    Some("")
                } else {
                    line.strip_prefix("data:")
                        .map(|value| value.strip_prefix(' ').unwrap_or(value))
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() || data == "[DONE]" {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(&data) else {
            self.failed = true;
            return;
        };
        self.observe(&value, elapsed_ms);
    }

    fn observe(&mut self, value: &Value, elapsed_ms: f64) {
        let response = value.get("response").unwrap_or(value);
        if let Some(model) = response
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
        {
            self.observation.response_model = Some(model.to_owned());
        }
        if let Some(service_tier) = response
            .get("service_tier")
            .and_then(Value::as_str)
            .filter(|service_tier| !service_tier.is_empty())
        {
            self.observation.response_service_tier = Some(service_tier.to_owned());
        }
        if let Some(id) = response
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| id.starts_with("resp_"))
        {
            self.observation.response_id = Some(id.to_owned());
        }
        let event_type = value.get("type").and_then(Value::as_str);
        let terminal = match event_type {
            Some("response.completed") => {
                self.observation.outcome = Some("completed".into());
                true
            }
            Some("response.incomplete") => {
                self.observation.outcome = Some("incomplete".into());
                true
            }
            Some("response.failed") | Some("error") => {
                self.observation.outcome = Some("failed".into());
                true
            }
            _ => false,
        };
        if terminal {
            if !self.observation.terminal {
                self.observation.terminal_ms = Some(elapsed_ms);
            }
            self.observation.terminal = true;
        }
        if let Some(usage) = response.get("usage").or_else(|| value.get("usage")) {
            set_u64(
                &mut self.observation.input_tokens,
                usage.get("input_tokens"),
            );
            set_u64(
                &mut self.observation.output_tokens,
                usage.get("output_tokens"),
            );
            set_u64(
                &mut self.observation.cached_input_tokens,
                usage
                    .get("input_tokens_details")
                    .and_then(|details| details.get("cached_tokens")),
            );
            set_u64(
                &mut self.observation.reasoning_output_tokens,
                usage
                    .get("output_tokens_details")
                    .and_then(|details| details.get("reasoning_tokens")),
            );
            set_u64(
                &mut self.observation.total_tokens,
                usage.get("total_tokens"),
            );
        }

        let (output_kind, is_text) = match event_type {
            Some("response.output_text.delta") => (Some("output_text"), true),
            Some("response.refusal.delta") => (Some("refusal"), true),
            Some("response.function_call_arguments.delta") => {
                (Some("function_call_arguments"), false)
            }
            Some("response.custom_tool_call_input.delta") => {
                (Some("custom_tool_call_input"), false)
            }
            Some("response.reasoning_text.delta") => (Some("reasoning_text"), false),
            Some("response.reasoning_summary_text.delta") => {
                (Some("reasoning_summary_text"), false)
            }
            _ => (None, false),
        };
        if let Some(output_kind) = output_kind.filter(|_| {
            value
                .get("delta")
                .and_then(Value::as_str)
                .is_some_and(|delta| !delta.is_empty())
        }) {
            if self.observation.first_output_ms.is_none() {
                self.observation.first_output_ms = Some(elapsed_ms);
                self.observation.first_output_kind = Some(output_kind.to_owned());
            }
            self.observation.last_output_ms = Some(elapsed_ms);
            if is_text && self.observation.first_text_ms.is_none() {
                self.observation.first_text_ms = Some(elapsed_ms);
            }
        }
    }

    pub fn failed(&self) -> bool {
        self.failed
    }
}

fn set_u64(slot: &mut Option<u64>, value: Option<&Value>) {
    if let Some(value) = value.and_then(Value::as_u64) {
        *slot = Some(value);
    }
}

fn find_event(bytes: &[u8]) -> Option<(usize, usize)> {
    let lf = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, 2));
    let crlf = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| (position, 4));
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(if left.0 < right.0 { left } else { right }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}
