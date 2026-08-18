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
