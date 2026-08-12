#![allow(dead_code)]

use std::{fs, path::PathBuf};

#[path = "../src/audit.rs"]
mod audit;
#[path = "../src/error.rs"]
mod error;
#[path = "../src/paths.rs"]
mod paths;

fn temp_dir() -> PathBuf {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).unwrap();
    let path = std::env::temp_dir().join(format!(
        "c2a-audit-test-{}-{}",
        std::process::id(),
        random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));
    fs::create_dir(&path).unwrap();
    path
}

fn completed_event() -> Vec<u8> {
    concat!(
        ": comment\r\n",
        "data: {\"type\":\"response.completed\",\r\n",
        "data: \"response\":{\"id\":\"resp_test\",\"status\":\"completed\",",
        "\"usage\":{\"input_tokens\":10,\"output_tokens\":4}},\"note\":\"snowman ",
        "\u{2603}\"}\r\n\r\n"
    )
    .as_bytes()
    .to_vec()
}

#[test]
fn sse_parses_across_every_byte_split() {
    let event = completed_event();
    for split in 0..=event.len() {
        let mut observer = audit::SseObserver::new();
        observer.feed(&event[..split]);
        observer.feed(&event[split..]);
        observer.finish();
        assert!(!observer.failed(), "split {split}");
        assert_eq!(
            observer.observation.response_id.as_deref(),
            Some("resp_test")
        );
        assert_eq!(observer.observation.outcome.as_deref(), Some("completed"));
        assert_eq!(observer.observation.input_tokens, Some(10));
        assert_eq!(observer.observation.output_tokens, Some(4));
    }
}

#[test]
fn malformed_or_oversized_events_become_uninspected() {
    let mut malformed = audit::SseObserver::new();
    malformed.feed(b"data: {bad}\n\n");
    assert!(malformed.failed());

    let mut oversized = audit::SseObserver::new();
    oversized.feed(&vec![b'x'; 256 * 1024 + 1]);
    assert!(oversized.failed());
}

#[test]
fn audit_serialization_omits_unavailable_and_secret_fields() {
    let record = audit::AuditRecord {
        request_id: "request".into(),
        started_at: "start".into(),
        finished_at: "finish".into(),
        model: "model".into(),
        request_bytes: 1,
        response_bytes: 2,
        upstream_http_status: 200,
        upstream_request_id: None,
        response_id: None,
        outcome: "completed".into(),
        input_tokens: None,
        output_tokens: None,
    };
    let value = serde_json::to_value(record).unwrap();
    let object = value.as_object().unwrap();
    assert!(!object.contains_key("schema_version"));
    assert!(!object.contains_key("response_id"));
    assert!(!object.contains_key("input_tokens"));
    for forbidden in [
        "access_token",
        "account_id",
        "headers",
        "error",
        "request_body",
        "response_body",
    ] {
        assert!(!object.contains_key(forbidden));
    }
}

#[test]
fn concurrent_audit_lines_do_not_interleave() {
    let root = temp_dir();
    let path = root.join("audit.jsonl");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let audit = audit::open(&path).unwrap();
        let mut tasks = Vec::new();
        for index in 0..32 {
            let audit = audit.clone();
            tasks.push(tokio::spawn(async move {
                let record = audit::AuditRecord {
                    request_id: format!("request-{index}"),
                    started_at: "start".into(),
                    finished_at: "finish".into(),
                    model: "model".into(),
                    request_bytes: 1,
                    response_bytes: 2,
                    upstream_http_status: 200,
                    upstream_request_id: None,
                    response_id: None,
                    outcome: "completed".into(),
                    input_tokens: None,
                    output_tokens: None,
                };
                audit::append(&audit, &record).await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });
    let lines = fs::read_to_string(&path).unwrap();
    assert_eq!(lines.lines().count(), 32);
    for line in lines.lines() {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}
