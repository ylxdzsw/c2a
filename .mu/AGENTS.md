# c2a Project Guide

## Purpose

`c2a` is a Linux-only Rust service that exposes the OpenAI Responses API over
local Unix-domain sockets and relays native streaming requests to either the
ChatGPT Codex or GitHub Copilot subscription backend. It owns one login per
provider and accepts only:

```text
POST /v1/responses
```

The request must be JSON, contain a nonempty `model`, and set `stream` to the
boolean `true`. c2a does not translate models, rewrite request bodies, expose
TCP, provide API keys, or maintain an account pool.

## Repository Structure

All application modules are intentionally flat under `src/`.

- `main.rs`: direct `args_os()` CLI parser and provider-scoped command dispatch
  for `login`, `status`, `logout`, and `serve`.
- `paths.rs`: fixed XDG/HOME state paths and ownership, type, symlink, and mode
  checks.
- `device.rs`: Codex device login, PKCE verification, OAuth response bounds,
  and fixed upstream compatibility constants.
- `copilot.rs`: GitHub device login, token refresh, account and endpoint
  discovery, and Copilot compatibility constants.
- `provider.rs`: the closed `codex | copilot` provider set and shared c2a
  identity.
- `refresh.rs`: locked proactive refresh and the one forced refresh used after
  an upstream `401`.
- `storage.rs`: provider credential-file locking, bounded in-place writes,
  reloads, and logout clearing.
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
- `c2a@.service`, `c2a@.socket`: root-owned provider-instance systemd units.
- `README.md`: user-facing build, login, service, mu, and audit instructions.

## Key Design Decisions

### Native relay

The original local request bytes are retained only for the duration of the
relay and sent unchanged upstream. c2a validates a parsed JSON representation
but does not serialize it back into a new request. Local headers are not
forwarded. Both providers use `User-Agent: c2a/<package-version>`. Codex also
uses `originator: c2a`; it does not send a Codex `version` header or the
Responses Lite header. Copilot sends the current GitHub API version but no
borrowed editor, plugin, integration, initiator, intent, vision, or request-ID
identity headers. Successful upstream responses may omit `Content-Type`, in
which case c2a synthesizes `text/event-stream`; an explicitly wrong content
type is rejected. c2a copies only content type, cache control, and a sanitized
upstream request ID. Both `x-request-id` and `x-oai-request-id` are recognized
upstream and exposed locally as `x-request-id`. Upstream HTTP errors retain
their status and may additionally copy `Retry-After`; c2a-generated relay
failures use `502 Bad Gateway`.

### Fixed compatibility values

Provider compatibility details are compiled into `device.rs` and `copilot.rs`:

- Auth base: `https://auth.openai.com`
- Responses upstream: `https://chatgpt.com/backend-api/codex/responses`
- Client ID used by the current Codex device flow
- Upstream identity: `originator: c2a` and `User-Agent: c2a/<package-version>`
- GitHub device OAuth endpoints and the OpenCode public OAuth client ID
- GitHub account and Copilot endpoint discovery endpoints
- Copilot API version and `User-Agent: c2a/<package-version>`

Keep these values centralized. When Codex changes its private protocol, update
`device.rs`; when GitHub or Copilot changes its protocol, update `copilot.rs`.
Add or adjust focused tests and do not introduce a configuration layer unless
the product scope changes.

As probed against the live Copilot backend on 2026-08-18,
`gpt-5.6-luna` accepts a native streaming Responses request with only bearer
authorization and JSON content type. It does not require
`Copilot-Integration-Id`, `Editor-Version`, `Editor-Plugin-Version`,
`X-Initiator`, `OpenAI-Intent`, or an explicit API-version header. Keep c2a's
honest User-Agent and current API-version header, but do not add borrowed
integration identity without fresh evidence.

c2a always uses full Responses, including for models that official Codex may
select for Responses Lite. Full Responses is intentional because c2a preserves
the caller's request bytes and does not implement model-specific tool or body
transformations.

### Authentication and state

State is fixed at:

```text
$XDG_STATE_HOME/c2a/codex.json
$XDG_STATE_HOME/c2a/copilot.json
$XDG_STATE_HOME/c2a/audit.jsonl
```

When `XDG_STATE_HOME` is unset, use `~/.local/state/c2a`. XDG and HOME paths
must be absolute. The directory is mode `0700`; credential and audit files are
regular files owned by the effective user with mode `0600`.

Codex credentials contain only `version`, `access_token`, and `refresh_token`.
Copilot credentials may additionally contain access- and refresh-token expiry
timestamps. Never persist account IDs, email, plans, discovered endpoints, ID
tokens, OAuth response bodies, or payload-derived values. The Codex account ID
is derived from access-token claims; Copilot status queries the GitHub account
and entitlement endpoints live.

Each credential file is also its own cross-process `flock` target. Reads take a
shared lock; writes and logout take an exclusive lock. Writes intentionally
truncate, rewrite, and `sync_all` the same inode instead of using a separate
lock file and atomic rename. An empty file means logged out. A nonempty corrupt
file is an error but a new login may overwrite it.

The device flow runs before the writer lock is acquired. Concurrent logins are
not serialized; the last completed login wins. This is an accepted trade-off.
Refresh reloads after locking so another process's rotation can be reused.
Within a relay process, credential acquisition is asynchronously serialized so
waiters do not block Tokio workers on `flock`. A `401` refresh is tied to the
specific rejected access token; if another request already replaced it, reuse
the replacement instead of refreshing again.

There is intentionally no migration, legacy filename lookup, old CLI alias, or
old systemd-unit compatibility in the program or package. Operational
migration of an existing host is a one-time administrator action.

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

Audit records retain only provider, request/response IDs, timestamps, model,
byte counts, upstream HTTP status, terminal outcome, and observable token
counts. Payloads, tokens, account data, headers, errors, and hashes are not
stored. Codex and Copilot services share one audit file. Each record is
serialized once and appended under both an in-process mutex and a cross-process
`flock`, so lines from the two service instances do not interleave. Writes are
not synchronously forced to stable storage per request; this is intentional
best-effort audit durability. Audit records have no schema-version field. Do
not change the meaning of an existing audit field after it is introduced. When
new information needs to be persisted, use a new field name or add a new field
without versioning the record.

The systemd journal is for c2a's own process and service diagnostics, mostly
errors. It is not request or response logging; actual request/response
information belongs in the metadata-only audit log and must not be added to the
journal by default.

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

The checked-in template units are installed system-wide, run c2a as root from
`/usr/bin/c2a`, use `/root/.local/state/c2a`, and instantiate only
`c2a@codex` and `c2a@copilot`. Their sockets are
`/run/c2a/codex.sock` and `/run/c2a/copilot.sock`, both mode `0600`.

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
systemd-analyze verify \
  c2a@codex.service c2a@codex.socket \
  c2a@copilot.service c2a@copilot.socket
```

The integration tests start c2a subprocesses. The surrounding environment may
have inherited `LISTEN_PID` or `LISTEN_FDS` values from another socket-activated
service; tests must remove those variables when exercising manual serving.

Do not run `c2a <provider> login` during automated tests. It is interactive,
changes local credential state, and contacts the real OAuth service. Use
temporary `XDG_STATE_HOME` directories and mock or pre-stream validation paths
instead.

## Change Notes

- Read the relevant module and tests before editing; preserve existing user
  changes in a dirty worktree.
- Keep source modules flat and changes narrowly scoped.
- Do not log or print access tokens, refresh tokens, raw OAuth bodies, upstream
  payloads, or authorization headers.
- Do not hash request or response payloads. SHA-256 is for PKCE only.
- Any upstream compatibility change should be checked against the current
  provider implementation or a minimal live probe and recorded near the
  constants in `device.rs` or `copilot.rs`.
- Any change to audit fields must preserve the minimal metadata-only schema and
  update serialization tests.
- Any change to request validation or stream lifecycle should include a focused
  test and a real Unix-socket test where practical.
