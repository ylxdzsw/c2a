# c2a

`c2a` exposes native OpenAI APIs on local Unix sockets. It relays streaming
Responses requests to a ChatGPT Codex subscription, a GitHub Copilot
subscription, or ChatGPT plan usage through Sign in with ChatGPT (SIWC), and
JSON Images requests to Codex. It is Linux-only and supports one login for each
provider.

| Endpoint | Codex | Copilot | ChatGPT (SIWC) |
| --- | --- | --- | --- |
| `POST /v1/responses` (`stream: true`) | Yes | Yes | Yes, `store: false` |
| `POST /v1/images/generations` (JSON) | Yes | No | No |
| `POST /v1/images/edits` (JSON) | Yes | No | No |

## Build and install

```bash
cargo build --release
install -Dm755 target/release/c2a /usr/bin/c2a
```

Codex and Copilot use device login. ChatGPT uses browser OAuth with PKCE and a
temporary loopback callback, with a pasted-URL fallback for remote browsers.
Relayed requests use downstream application identity headers when supplied;
authentication and discovery identify as `c2a`. Compatibility details may change.

## Login and status

```bash
c2a codex login
c2a codex status
c2a codex quota
c2a codex logout

c2a copilot login
c2a copilot status
c2a copilot quota
c2a copilot logout

c2a chatgpt login
c2a chatgpt status
c2a chatgpt quota
c2a chatgpt logout
```

`quota` fetches the provider's current remaining allowance. Reset timestamps are
shown in local time with the remaining days, hours, and minutes.
Codex reports percentage remaining for each usage window. Copilot reports AI
credits on current plans and premium requests on legacy plans.
For ChatGPT, `quota` prints the ChatGPT usage-settings link instead of querying
an undocumented quota endpoint.

Credentials and audit records are stored under `$XDG_STATE_HOME/c2a`, or
`~/.local/state/c2a` when `XDG_STATE_HOME` is unset. The state directory must be
owned by the current user with mode `0700`; `codex.json`, `copilot.json`,
`chatgpt.json`, and `audit.jsonl` must be regular user-owned files with mode `0600`. Credential
files are directly locked during reads and writes. If a nonempty credential
file is corrupt, Codex or Copilot login can replace it. ChatGPT login refuses
corrupt state rather than silently losing its retained client and host identity.

## Sign in with ChatGPT

The `chatgpt` provider uses OpenAI's documented local/open-source SIWC flow and
the public `https://api.openai.com/v1/responses` endpoint. It is separate from
the `codex` provider: their credentials and upstreams are not interchangeable.
Automated checks use offline fixtures. Sign-in has been reported working from
an iOS browser; live inference has not yet been smoke-tested. Account eligibility,
model availability, and preview restrictions remain subject to OpenAI's backend.

Run `c2a chatgpt login` and open the printed authorization link. You can use a
browser on the same machine, a phone, or another computer:

1. Sign in to ChatGPT and approve c2a's identity and plan-usage permissions.
2. On the same machine, the browser normally reaches c2a's temporary listener
   at `http://127.0.0.1:<port>/auth/callback` automatically.
3. On another device, that address refers to the browser's device and normally
   fails to load. Copy the **complete final URL from the address bar** and paste
   it into the waiting c2a command and **press Enter**. Input is hidden: no
   pasted characters will appear. c2a prints a confirmation when it accepts the
   callback. Do not paste it into a shell command, a log, or a support message.
   If iOS omits `http://` when copying, a URL beginning with
   `127.0.0.1:<port>/auth/callback?...` is also accepted.
4. Keep the login process running; it retains the secret PKCE verifier. The
   callback must match that attempt's address and state. c2a exchanges the code
   directly with OpenAI and verifies the signed ID token before using credentials.

Login has a 15-minute deadline; authorization codes may expire sooner. The
phone never needs to reach the server, and no public callback, SSH tunnel, or
credential-file transfer is needed for this fallback. Mobile browsers must
retain the failed callback URL for copying. Browser-specific behavior and the
live SIWC flow still need verification. The inference service remains Unix-only;
only the login command temporarily listens on TCP, bound to `127.0.0.1`.

`chatgpt.json` retains a stable random host ID, the issued client ID, and the
verified subject. Successful sessions also contain granted scopes, tokens, and
expiry/refresh timing. Logout attempts to revoke the renewable session, clears
local tokens, and retains the registration for later sign-in to the same account
and workspace. c2a does not maintain a profile/account pool. It omits ID-token
login hints so printed authorization links contain no tokens. ID tokens are
only retained temporarily in an unusable pending-refresh checkpoint when
signature verification needs to recover from an unavailable key service.

Refreshes are serialized across processes. A replacement refresh token is
checkpointed before key retrieval, and a started refresh finishes saving even
if its caller disconnects. Storage keeps c2a's existing locked, in-place writes
and `sync_all`, not atomic rename: an interrupted write can still require
credential recovery. SIWC refresh is proactive; a direct-route `401` is returned
unchanged rather than assuming every authorization failure needs a refresh.

Each request must include `stream: true`, `store: false`, a nonempty `model`,
and an `input` array containing the required context. c2a forwards the original
bytes; it does not remove unsupported options or transform tools. The preview
does not support HTTP `previous_response_id`, several ordinary Responses
parameters, or hosted image generation/file search/Code Interpreter/MCP tools.
Image inputs are distinct from image generation. Neither Images nor Decisions
routes are exposed on the ChatGPT socket. See the current
[SIWC limitations](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations)
before adapting an existing client. SIWC HTTP error bodies and machine-readable
codes pass through, bounded to 64 KiB; SSE errors pass through unchanged.

Plan usage must actually be granted; identity sign-in alone cannot power
inference. Usage draws on the existing allowance and app-specific limits, not a
new pool. Credits may be consumed if the user enables them in ChatGPT. c2a does
not silently fall back to Codex, Copilot, or API billing. Manage permissions and
limits at <https://chatgpt.com/settings/usage>.

Implementation references reviewed on 2026-10-23 (no third-party source copied):

- [OpenAI's SIWC contract](https://developers.openai.com/siwc/token-sharing-open-source/sign-in)
  and published OIDC discovery/JWKS/revocation endpoints.
- [CLIProxyAPI, `d946c4f`](https://github.com/router-for-me/CLIProxyAPI/blob/d946c4fba6fc9a5cfbca1f3d85184d62ad2fb683/internal/api/handlers/management/oauth_siwc.go)
  (MIT): remote full-URL callback validation and server-owned pending attempts.
- [Pi, `936e5f4`](https://github.com/earendil-works/pi/blob/936e5f40aede9327b9142b63506ed2b68bc6b4ac/packages/ai/src/auth/oauth/openai-chatgpt.ts)
  (MIT): separate SIWC provider and racing browser/manual callback paths.
- OpenCode `2a381f8` and Codex CLI `8531184` were reviewed as older Codex OAuth
  implementations, not evidence that their credentials work with SIWC.

The implementation uses a Rust JWT verifier for signed identity checks; it does
not embed OpenAI's noncommercial Node DevKit.

## Run manually

```bash
c2a codex serve /run/user/$(id -u)/c2a/codex.sock
c2a copilot serve /run/user/$(id -u)/c2a/copilot.sock
c2a chatgpt serve /run/user/$(id -u)/c2a/chatgpt.sock
```

The socket path must not already exist. The service removes only a socket it
created itself.

## Run with systemd

```bash
install -Dm644 c2a@.service /usr/lib/systemd/system/c2a@.service
install -Dm644 c2a@.socket /usr/lib/systemd/system/c2a@.socket
systemctl daemon-reload
systemctl enable --now c2a@codex.socket
systemctl enable --now c2a@copilot.socket
systemctl enable --now c2a@chatgpt.socket
```

On Arch Linux, build and install the package instead:

```bash
makepkg -si
```

The system service runs as root, stores state under `/root/.local/state/c2a`, and
activates `/run/c2a/codex.sock`, `/run/c2a/copilot.sock`, and
`/run/c2a/chatgpt.sock` with mode `0600` when their instances are enabled.

## Configure mu

Percent-encode the absolute socket path in an `http+unix` Responses endpoint.
For the system service:

```jsonc
{
  "providers": {
    "c2a-codex": {
      "endpoint": "http+unix://%2Frun%2Fc2a%2Fcodex.sock/v1/responses",
      "models": {
        "gpt-5.6": {
          "context_window": 353000
        }
      }
    },
    "c2a-copilot": {
      "endpoint": "http+unix://%2Frun%2Fc2a%2Fcopilot.sock/v1/responses",
      "models": {
        "gpt-5.6-luna": {
          "context_window": 1050000
        }
      }
    }
  }
}
```

`c2a` does not translate models or request bodies. The configured model name and
native Responses payload are forwarded unchanged. On all Responses and Images
routes, downstream `User-Agent` and `originator` headers are forwarded unchanged
when provided and omitted when absent; c2a adds no default application identity.
No other local request headers are forwarded. Authentication, account selection,
and provider-required headers remain controlled by c2a. This applies to relayed
inference requests only: login, refresh, discovery, and status requests retain
c2a's own identity, including ChatGPT's application registration.
For Codex backend compatibility, c2a derives `session-id` from the
body's `prompt_cache_key` and derives `x-codex-routing-hint` from `model` and an
optional `service_tier`.
For ChatGPT SIWC, c2a derives `session_id` (underscore) from the same body key;
it does not add `session-id`, `x-codex-routing-hint`, or `x-client-request-id`.
An absent or header-unsafe key omits the derived session header on either
provider without changing the request body.

## Generate and edit images

Images use the Codex-native JSON protocol, not a Responses tool call. They share
the Codex login and socket; there is no local API key. Example generation:

```bash
curl --fail-with-body --max-time 660 --unix-socket /run/c2a/codex.sock \
  http://localhost/v1/images/generations \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-image-2","prompt":"A watercolor fox","size":"1024x1024","n":1}' \
  -o response.json
jq -r '.data[0].b64_json' response.json | base64 -d > fox.png
```

For an edit, create a request file with the image embedded as a data URL:

```bash
python -c 'import base64,json; print(json.dumps({"model":"gpt-image-2","prompt":"Add a snowy background","images":[{"image_url":"data:image/png;base64,"+base64.b64encode(open("fox.png","rb").read()).decode()}]}))' > edit.json
curl --fail-with-body --max-time 660 --unix-socket /run/c2a/codex.sock \
  http://localhost/v1/images/edits \
  -H 'Content-Type: application/json' --data-binary @edit.json -o edited.json
```

Both routes require nonempty `model` and `prompt` strings. Omit `stream` or set
it to `false`. Edits require one to five `images` objects with nonempty
`image_url` strings. Multipart uploads and image streaming are not supported.
c2a does not read image paths, fetch URLs, decode images, or translate options.
Unknown options and model IDs pass through; their availability is determined by
the Codex backend and account. Current first-party Codex source defaults to
`gpt-image-2`; public API model availability need not match Codex availability.

Live checks on 2026-09-20 accepted `gpt-image-2`, `gpt-image-2.5-flare`, and
`gpt-image-2.5-sunburst` on both endpoints. Each returned valid PNG image data
and token usage. Edits worked with one, two (mixed JPEG/PNG), and five reference
images. Transparent generation produced real alpha. The backend does not echo
the served model, so acceptance alone does not identify the engine used.

Do not assume every requested option is honored: `n: 2` returned one image;
JPEG and WebP output requests returned PNG; a `1024x1024` request returned
`1254x1254`; medium, high, and xhigh quality requests returned low, medium, and
low respectively. Inspect returned metadata and image bytes. c2a preserves
these upstream results rather than rewriting or converting them.

Images requests and success responses are each limited to 64 MiB (including
base64 and JSON overhead). Two image operations may run at once, within the
shared limit of 16 active relays. Admission happens before request collection;
local uploads have a 60-second deadline. Upstream Images operations have a
10-minute deadline, including the wait for response headers. Responses requests
retain their 16 MiB body limit and SSE behavior.

Successful Images JSON is buffered and checked for nonempty image data before
being returned byte-for-byte. No generated files are stored by the service.
HTTP errors retain their status; invalid or oversized success bodies return
`502`. Only an upstream `401` gets one credential-refresh retry. Timeouts and
other ambiguous failures are not retried: the backend may already have consumed
quota. Callers should likewise avoid automatic retries.

## Audit records

Each handled local HTTP request produces one final JSON line in the shared
`audit.jsonl`, including validation/admission rejections, authentication or
connection failures, and relays cancelled before response headers. A retry is
part of the same local request, not a separate audit line. Requests rejected by
the HTTP parser before the handler runs are not audited. Forced termination or
a crash can still prevent a final record.

Records identify `provider`, `operation` (`responses`, `images.generations`,
`images.edits`, or `unknown` for an unsupported route), local `request_id`, byte
counts, timestamps, outcome, and available upstream IDs. Images may include a
distinct `upstream_imagegen_request_id`, never substituted for `response_id`.
Unknown optional values are omitted, not written as zero or inferred.

### Model, tier, usage, and outcomes

| Field | Meaning |
| --- | --- |
| `model` | Requested model; empty for requests rejected before successful validation |
| `response_model` | Last nonempty model ID reported by upstream |
| `requested_service_tier` | Requested Responses service tier, when supplied |
| `response_service_tier` | Last nonempty tier reported by upstream, independent of the requested tier |
| `input_tokens`, `output_tokens`, `total_tokens` | Upstream usage counts, when reported |
| `cached_input_tokens` | `usage.input_tokens_details.cached_tokens` |
| `reasoning_output_tokens` | `usage.output_tokens_details.reasoning_tokens` |
| `upstream_attempts` | Inference HTTP sends started, including failed sends; excludes OAuth, refresh, and discovery requests |
| `auth_retries` | Credential-recovery retries entered after an upstream 401; a retry may fail before another send, or reuse another request's refreshed credentials |
| `local_http_status` | Status handed back to the local HTTP handler; absent if cancelled before that handoff |
| `upstream_http_status` | Last upstream response status received; absent if no response headers were received |

Compare `model` and `response_model` for an exact-string sanity check. A missing
response model means **unknown**, not a match; different names can reflect alias
resolution rather than an incorrect model. Images also record model/tier when
reported, but the image backend often omits them. A reported model name is not
independent proof of which model executed the request. We do not reject or
rewrite responses based on model or tier differences.

Responses usage and metadata are observed from SSE; Images metadata is extracted
selectively from JSON without decoding image data. Partial usage events do not
erase earlier observed counts. A reported zero is retained distinctly from an
unavailable count. New optional image metadata with an unexpected type is ignored.
No total or detail count is synthesized from other fields.

Outcomes remain `completed`, `incomplete`, `failed`, `upstream_error`,
`premature_eof`, `client_disconnect`, `shutdown`, and `uninspected`; local
validation/admission failures use `rejected`. `uninspected` means observation or
validation failed, not necessarily that forwarding failed. An upstream success
status alone does not establish successful completion: inspect `outcome` too.

### Timing and throughput

All numeric timing fields use **monotonic milliseconds**, with fractional
precision. Offsets are measured from entry to the HTTP request handler, after
headers have arrived and before body collection. They measure what c2a observes,
not server-side inference timestamps or client receipt times.

| Field | Meaning |
| --- | --- |
| `received_at` | UTC wall-clock time at handler entry |
| `started_at`, `finished_at` | Existing UTC timestamps; for relays, `started_at` remains after body collection/validation; for local rejections it equals receipt time |
| `duration_ms` | Handler entry through final relay/handler outcome, before audit append |
| `relay_start_ms`, `relay_duration_ms` | Relay start offset and duration, excluding local body collection/validation but including authentication and retries |
| `first_upstream_byte_ms` | First nonempty body chunk read from the response being relayed/inspected, including error bodies; not response headers or a discarded retry response |
| `first_output_ms` | First complete, nonempty recognized output delta event |
| `first_text_ms` | First nonempty output-text or refusal delta, excluding reasoning and tool arguments |
| `last_output_ms` | Last recognized nonempty output delta |
| `terminal_ms` | First completed/incomplete/failed/error terminal SSE event |
| `first_output_kind` | Kind of the first recognized output delta, not its content |

Recognized delta kinds are `output_text`, `refusal`, `function_call_arguments`,
`custom_tool_call_input`, `reasoning_text`, and `reasoning_summary_text`.
Lifecycle events such as `response.created`, empty deltas, unknown event types,
and `[DONE]` are not counted as first output. Events split across chunks are
timed when their terminating chunk arrives. Images are non-streaming and omit
output/terminal SSE timings rather than pretending their first byte is a token.

For rows with the required fields and positive denominators:

| Metric | Calculation |
| --- | --- |
| Observed TTFT | `first_output_ms` (or `first_text_ms` for visible text only) |
| Relay-only TTFT | `first_output_ms - relay_start_ms` |
| Whole-duration output TPS | `1000 * output_tokens / duration_ms` |
| Relay-only average output TPS | `1000 * output_tokens / relay_duration_ms` |
| Decode-window output TPS estimate | `1000 * output_tokens / (terminal_ms - first_output_ms)` |
| Token-weighted cache hit rate | `cached_input_tokens / input_tokens` |
| Reasoning-token ratio | `reasoning_output_tokens / output_tokens` |

`last_output_ms - first_output_ms` is also available for a last-delta decode
window that excludes time between the last delta and the terminal event.
**Decode-window TPS is an estimate, not exact per-token decoding speed.** One
delta can contain multiple tokens, several events can arrive in one chunk, and
hidden reasoning tokens may be counted in usage without observable deltas.
Reasoning summaries need not correspond to the actual reasoning-token stream.
Tool execution, network buffering, and downstream backpressure can affect these
intervals. We do not invent a token timestamp from usage or a lifecycle event,
or assume the first delta contains exactly one token. A missing first-output
time or a zero-width window means decode TPS is unavailable.

Use completed, inspected Responses for comparable token-rate statistics; handle
failed/incomplete requests separately. Aggregate cache/reasoning ratios by
summing numerators and denominators **over the same rows where both are known**,
not by treating missing values as zero. Request-level cache hit rate can instead
be computed as the fraction of rows with known `cached_input_tokens` greater
than zero. Rates/ratios are derived at query time, not redundantly persisted.

`request_bytes` is the collected local body size (zero when collection was not
completed). `response_bytes` retains its existing meaning: upstream body bytes
read, with error bodies bounded to the inspection limit. It is not a count of
bytes delivered to the client. Completion means c2a finished reading/handing off
the stream or buffered image response; the client may still be draining queued
bytes. Existing audit records are left unchanged and lack new fields.

Field/event references: [Responses API](https://developers.openai.com/api/reference/resources/responses/methods/create)
and [streaming events](https://developers.openai.com/api/reference/resources/responses/streaming-events/).

### Persistence and privacy

The location remains `$XDG_STATE_HOME/c2a/audit.jsonl`, or
`~/.local/state/c2a/audit.jsonl` when XDG state is unset. The state directory is
`0700`; the regular, user-owned audit file is `0600`. Provider processes share
one append-only JSONL file, serialized under an in-process mutex and an exclusive
cross-process `flock`. Records are appended at completion, not per event. Writes
are not synchronously forced to stable storage per request. There is no built-in
rotation or retention policy. Audit append failures are reported on stderr;
under systemd these process diagnostics normally go to the journal.

Request/response bodies, prompts, generated content, authentication tokens,
account data, general headers, errors, and payload hashes are not retained.
Bounded upstream error details are returned only in the immediate local error
response. Upstream HTTP errors keep their status, `Retry-After`, and sanitized
request ID; failures within c2a are returned as `502 Bad Gateway`.
