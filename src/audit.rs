use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::Arc,
};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::paths::check_file;

const EVENT_LIMIT: usize = 256 * 1024;

#[derive(Debug, Default, Serialize)]
pub struct AuditRecord {
    pub request_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub model: String,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub upstream_http_status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
}

pub type Audit = Arc<Mutex<File>>;

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
    file.write_all(&bytes)
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
    pub outcome: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
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

    pub fn feed(&mut self, bytes: &[u8]) {
        if self.failed {
            return;
        }
        self.buffer.extend_from_slice(bytes);
        while let Some((position, separator)) = find_event(&self.buffer) {
            let event = self.buffer.drain(..position).collect::<Vec<_>>();
            self.buffer.drain(..separator);
            self.event(&event);
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

    fn event(&mut self, bytes: &[u8]) {
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
        self.observe(&value);
    }

    fn observe(&mut self, value: &Value) {
        let response = value.get("response").unwrap_or(value);
        if let Some(id) = response
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| id.starts_with("resp_"))
        {
            self.observation.response_id = Some(id.to_owned());
        }
        match value.get("type").and_then(Value::as_str) {
            Some("response.completed") => {
                self.observation.terminal = true;
                self.observation.outcome = Some("completed".into());
            }
            Some("response.incomplete") => {
                self.observation.terminal = true;
                self.observation.outcome = Some("incomplete".into());
            }
            Some("response.failed") | Some("error") => {
                self.observation.terminal = true;
                self.observation.outcome = Some("failed".into());
            }
            _ => {}
        }
        if let Some(usage) = response.get("usage").or_else(|| value.get("usage")) {
            self.observation.input_tokens = usage.get("input_tokens").and_then(Value::as_u64);
            self.observation.output_tokens = usage.get("output_tokens").and_then(Value::as_u64);
        }
    }

    pub fn failed(&self) -> bool {
        self.failed
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
