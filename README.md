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
All providers identify upstream as `c2a`. Compatibility details may change.

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
native Responses payload are forwarded unchanged. Local request headers are not
forwarded. For Codex backend compatibility, c2a derives `session-id` from the
body's `prompt_cache_key` and derives `x-codex-routing-hint` from `model` and an
optional `service_tier`.

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

Each upstream request produces one JSON line in the shared `audit.jsonl`.
New records identify the provider and operation (`responses`,
`images.generations`, or `images.edits`) and contain timestamps, byte counts,
request/response IDs, model, terminal outcome, and input/output token counts
when observable from the SSE stream or image JSON. Images may also record a
distinct `upstream_imagegen_request_id`; it is never substituted for a Responses
ID. `model` is the requested model ID; Responses records also include
`response_model` when upstream reports a nonempty model name in the SSE stream.
Different names may reflect alias resolution, not necessarily an incorrect
model. Images do not currently report a served model name.
`upstream_http_status` is absent when an image operation fails before an
upstream response is received. Existing records are left unchanged.
Request bodies, response bodies, tokens, account data, headers,
errors, and payload hashes are not retained. Bounded upstream error details are
returned only in the immediate local error response. Upstream HTTP errors keep
their status, `Retry-After`, and sanitized request ID; failures within c2a are
returned as `502 Bad Gateway`.

Audit lines are directly locked and appended immediately, but are not
synchronously forced to stable storage for every request.
