use std::{
    fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Server {
    child: Child,
    root: PathBuf,
    socket: PathBuf,
}

impl Server {
    fn start(provider: &str) -> Self {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).unwrap();
        let root = std::env::temp_dir().join(format!(
            "c2a-relay-test-{}-{}",
            std::process::id(),
            random
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ));
        fs::create_dir(&root).unwrap();
        let socket = root.join("c2a.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_c2a"))
            .args([provider, "serve", socket.to_str().unwrap()])
            .env("XDG_STATE_HOME", &root)
            .env_remove("LISTEN_PID")
            .env_remove("LISTEN_FDS")
            .env_remove("LISTEN_FDNAMES")
            .env_remove("LISTEN_PIDFDID")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() {
            assert!(Instant::now() < deadline, "server socket did not appear");
            thread::sleep(Duration::from_millis(10));
        }
        Self {
            child,
            root,
            socket,
        }
    }

    fn request(&self, request: &[u8]) -> Vec<u8> {
        let mut stream = UnixStream::connect(&self.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(request).unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        response
    }

    fn post(&self, path: &str, body: &[u8], content_type: &str) -> Vec<u8> {
        let mut request = format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).into_bytes();
        request.extend_from_slice(body);
        self.request(&request)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn status(response: &[u8]) -> u16 {
    let line = response.split(|byte| *byte == b'\n').next().unwrap();
    std::str::from_utf8(line)
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

fn post(body: &[u8], content_type: &str) -> Vec<u8> {
    let server = Server::start("codex");
    let mut request = format!(
        "POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ).into_bytes();
    request.extend_from_slice(body);
    server.request(&request)
}

#[test]
fn validates_path_method_and_content_type() {
    let server = Server::start("codex");
    let not_found =
        server.request(b"GET /other HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    assert_eq!(status(&not_found), 404);
    let method = server
        .request(b"GET /v1/responses HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    assert_eq!(status(&method), 405);
    assert!(
        String::from_utf8_lossy(&method)
            .to_ascii_lowercase()
            .contains("allow: post")
    );
    assert_eq!(status(&post(b"{}", "text/plain")), 415);
}

#[test]
fn corrupt_copilot_credentials_do_not_stop_the_service() {
    let server = Server::start("copilot");
    let state = server.root.join("c2a");
    fs::write(state.join("copilot.json"), "{bad").unwrap();
    fs::set_permissions(
        state.join("copilot.json"),
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )
    .unwrap();

    let valid = b"{\"model\":\"gpt-5.6-luna\",\"stream\":true}";
    let mut request = format!(
        "POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        valid.len()
    )
    .into_bytes();
    request.extend_from_slice(valid);
    assert_eq!(status(&server.request(&request)), 502);
    assert_eq!(
        status(
            &server.request(b"GET /other HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        ),
        404
    );
}

#[test]
fn validates_native_streaming_body() {
    assert_eq!(status(&post(b"{bad", "application/json")), 400);
    assert_eq!(status(&post(b"[]", "application/json")), 400);
    assert_eq!(status(&post(b"{\"stream\":true}", "application/json")), 400);
    assert_eq!(
        status(&post(
            b"{\"model\":\"gpt-5\",\"stream\":false}",
            "application/json"
        )),
        400
    );
}

#[test]
fn image_routes_are_codex_only_and_require_native_json() {
    let codex = Server::start("codex");
    let copilot = Server::start("copilot");
    for path in ["/v1/images/generations", "/v1/images/edits"] {
        assert_eq!(status(&copilot.post(path, b"{}", "application/json")), 404);
        assert_eq!(
            status(
                &codex.request(
                    format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                        .as_bytes()
                )
            ),
            405
        );
        assert_eq!(status(&codex.post(path, b"{}", "multipart/form-data")), 415);
        for body in [
            b"{}".as_slice(),
            br#"["m","p",false,[]]"#,
            br#"{"model":"m","prompt":"p","stream":true}"#,
            br#"{"model":"m","prompt":"p","stream":null}"#,
        ] {
            assert_eq!(status(&codex.post(path, body, "application/json")), 400);
        }
        assert_eq!(status(&codex.request(format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 67108865\r\nConnection: close\r\n\r\n").as_bytes())), 413);
    }
    assert_eq!(
        status(&codex.post(
            "/v1/images/edits",
            br#"{"model":"m","prompt":"p"}"#,
            "application/json"
        )),
        400
    );
    assert_eq!(
        status(&codex.post(
            "/v1/images/edits",
            br#"{"model":"m","prompt":"p","images":[["url"]]}"#,
            "application/json"
        )),
        400
    );
    // Valid requests reach credential acquisition, but never the network in this empty state directory.
    assert_eq!(
        status(&codex.post(
            "/v1/images/generations",
            br#"{"model":"m","prompt":"p"}"#,
            "application/json"
        )),
        502
    );
    assert_eq!(
        status(&codex.post(
            "/v1/images/edits",
            br#"{"model":"m","prompt":"p","images":[{"image_url":"data:image/png;base64,AA=="}]}"#,
            "application/json"
        )),
        502
    );
}

#[test]
fn image_admission_is_bounded_before_body_collection() {
    let server = Server::start("codex");
    let mut pending = Vec::new();
    for _ in 0..2 {
        let mut stream = UnixStream::connect(&server.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(b"POST /v1/images/generations HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\nExpect: 100-continue\r\n\r\n").unwrap();
        let mut interim = [0u8; 25];
        stream.read_exact(&mut interim).unwrap();
        assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
        pending.push(stream);
    }
    assert_eq!(
        status(&server.post("/v1/images/generations", b"{}", "application/json")),
        503
    );
    assert_eq!(
        status(&server.post("/v1/responses", b"{}", "application/json")),
        400
    );
    drop(pending);
}
