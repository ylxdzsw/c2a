# c2a Project Guide

## Purpose

`c2a` is a Linux-only Rust service that exposes the OpenAI Responses API over a
local Unix-domain socket and relays native streaming requests to the ChatGPT
Codex subscription backend. It owns one ChatGPT login and accepts only:

```text
POST /v1/responses
```

The request must be JSON, contain a nonempty `model`, and set `stream` to the
boolean `true`. c2a does not translate models, rewrite request bodies, expose
TCP, provide API keys, or maintain an account pool.

## Repository Structure

All application modules are intentionally flat under `src/`.

- `main.rs`: direct `args_os()` CLI parser and command dispatch for `login`,
  `status`, `logout`, and `serve`.
- `paths.rs`: fixed XDG/HOME state paths and ownership, type, symlink, and mode
  checks.
- `device.rs`: Codex device login, PKCE verification, OAuth response bounds,
  and fixed upstream compatibility constants.
- `refresh.rs`: locked proactive refresh and the one forced refresh used after
  an upstream `401`.
- `storage.rs`: `auth.lock`, atomic credential writes, reloads, and logout
  removal.
- `claims.rs`: access-token JWT claim decoding, including the derived account ID.
- `server.rs`: Unix listener, systemd socket activation, HTTP/1.1 handling,
  request validation, relay limits, and shutdown coordination.
- `relay.rs`: upstream request construction, authentication retry, response
  header filtering, byte-preserving stream forwarding, and final audit record
  creation.
- `audit.rs`: JSONL audit records and the bounded observational SSE parser.
- `sse.rs`: small alias used by the relay to access the observer boundary.
- `error.rs`: application and JSON API error types.
- `tests/auth.rs`: CLI, credential, path, and permission tests.
- `tests/audit.rs`: SSE split-boundary, malformed-event, audit serialization, and
  concurrent append tests.
- `tests/relay.rs`: real Unix-socket request validation tests.
- `c2a.service`, `c2a.socket`: root-owned system-level systemd units.
- `README.md`: user-facing build, login, service, mu, and audit instructions.

## Key Design Decisions

### Native relay

The original local request bytes are retained only for the duration of the
relay and sent unchanged upstream. c2a validates a parsed JSON representation
but does not serialize it back into a new request. Local headers are not
forwarded. c2a always identifies itself with `originator: c2a` and
`User-Agent: c2a/<package-version>`; it does not send a Codex `version` header
or the Responses Lite header. Successful upstream responses may omit
`Content-Type`, in which case c2a synthesizes `text/event-stream`; an explicitly
wrong content type is rejected. c2a copies only content type, cache control,
and a sanitized upstream request ID. Both `x-request-id` and
`x-oai-request-id` are recognized upstream and exposed locally as
`x-request-id`.

### Fixed compatibility values

Private Codex compatibility details are compiled into `device.rs`:

- Auth base: `https://auth.openai.com`
- Responses upstream: `https://chatgpt.com/backend-api/codex/responses`
- Client ID used by the current Codex device flow
- Upstream identity: `originator: c2a` and `User-Agent: c2a/<package-version>`

Keep these values centralized. When Codex changes its private protocol, update
this module and add or adjust focused tests. Do not introduce a configuration
layer for them unless the product scope changes.

c2a always uses full Responses, including for models that official Codex may
select for Responses Lite. Full Responses is intentional because c2a preserves
the caller's request bytes and does not implement model-specific tool or body
transformations.

### Authentication and state

State is fixed at:

```text
$XDG_STATE_HOME/c2a/auth.json
$XDG_STATE_HOME/c2a/auth.lock
$XDG_STATE_HOME/c2a/audit.jsonl
```

When `XDG_STATE_HOME` is unset, use `~/.local/state/c2a`. XDG and HOME paths
must be absolute. The directory is mode `0700`; auth, lock, and audit files are
regular files owned by the effective user with mode `0600`.

Credential files contain only `version`, `access_token`, and `refresh_token`.
Never persist account IDs, email, plans, expiry, ID tokens, OAuth response
bodies, or refresh timestamps. The account ID is derived from the access-token
claims each time credentials are loaded.

Credential writes use a random same-directory temporary file opened with
`create_new` and `O_NOFOLLOW`, followed by `sync_all`, rename, and directory
sync. Credential mutations acquire `auth.lock` with `flock`. Login acquires the
lock before starting the device flow; refresh reloads after locking so another
process's rotation can be reused.

### Device login

The device flow uses SHA-256 only for PKCE. The current Codex protocol sends JSON
to `/api/accounts/deviceauth/usercode` and `/api/accounts/deviceauth/token`.
Polling responses with both `403` and `404` are treated as pending by the
current upstream Codex implementation. Do not change that behavior based only
on generic OAuth assumptions; verify against current Codex sources first.

OAuth and upstream error bodies are bounded to 64 KiB. Up to 1 KiB of sanitized
upstream error detail may be returned in the immediate local error response but
is not persisted. Tokens must be nonempty, and an access token must contain a
usable ChatGPT account ID before it is stored.

### Streaming and audit

Up to 16 relays may be active. Upstream connection setup is limited to 30
seconds. Streaming uses a 10-minute idle timeout and preserves normal
backpressure. A semaphore permit remains held until the stream task finishes.

The SSE observer receives a copy of each upstream chunk. It handles arbitrary
chunk boundaries, LF/CRLF events, multiline `data:` fields, split UTF-8, and
malformed JSON. An unfinished event may be at most 256 KiB. Observer failure
does not stop forwarding, but it forces audit outcome `uninspected`.

Audit records retain only request/response IDs, timestamps, model, byte counts,
upstream HTTP status, terminal outcome, and observable token counts. Payloads,
tokens, account data, headers, errors, and hashes are not stored. Each record is
serialized once and appended under an in-process mutex. Writes are not
synchronously forced to stable storage per request; this is intentional
best-effort audit durability.

Possible relay outcomes include `completed`, `incomplete`, `failed`,
`upstream_error`, `premature_eof`, `client_disconnect`, `shutdown`, and
`uninspected`.

### Shutdown and sockets

Manual serving requires a socket path and refuses an existing path. The socket
created manually is mode `0600` and is removed only during cleanup after the
service stops. With systemd activation, c2a accepts exactly one descriptor at
fd 3 and validates `LISTEN_PID`, `LISTEN_FDS`, `AF_UNIX`, `SOCK_STREAM`, and
listening state. A socket path cannot be supplied alongside activation.

SIGINT and SIGTERM stop acceptance, allow active connection tasks to finish for
up to 30 seconds, broadcast stream shutdown, and then abort remaining tasks
after a bounded drain period.

The checked-in systemd units are installed system-wide, run c2a as root from
`/usr/local/bin/c2a`, use `/root/.local/state/c2a`, and activate
`/run/c2a/c2a.sock` with mode `0600`.

## Development Workflow

Use the minimum toolchain declared in `Cargo.toml`: Rust `1.89` or newer, Rust
2024 edition, and Linux. Keep dependencies within the existing manifest and
avoid adding frameworks, provider traits, configuration layers, or logging
abstractions without a scope change.

Run the focused verification set before committing:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
git diff --check
```

Useful broader checks:

```bash
cargo build --release
cargo tree --edges features
cargo tree --duplicates
systemd-analyze verify c2a.service c2a.socket
```

The integration tests start c2a subprocesses. The surrounding environment may
have inherited `LISTEN_PID` or `LISTEN_FDS` values from another socket-activated
service; tests must remove those variables when exercising manual serving.

Do not run `c2a login` during automated tests. It is interactive, changes local
credential state, and contacts the real OAuth service. Use temporary
`XDG_STATE_HOME` directories and mock or pre-stream validation paths instead.

## Change Notes

- Read the relevant module and tests before editing; preserve existing user
  changes in a dirty worktree.
- Keep source modules flat and changes narrowly scoped.
- Do not log or print access tokens, refresh tokens, raw OAuth bodies, upstream
  payloads, or authorization headers.
- Do not hash request or response payloads. SHA-256 is for PKCE only.
- Any upstream compatibility change should be checked against current Codex
  source and recorded near the constants in `device.rs`.
- Any change to audit fields must preserve the minimal metadata-only schema and
  update serialization tests.
- Any change to request validation or stream lifecycle should include a focused
  test and a real Unix-socket test where practical.
