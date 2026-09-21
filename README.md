# c2a

`c2a` exposes native OpenAI APIs on local Unix sockets. It relays streaming
Responses requests to either a ChatGPT Codex subscription or a GitHub Copilot
subscription, and JSON Images requests to Codex. It is Linux-only and supports
one login for each provider.

| Endpoint | Codex | Copilot |
| --- | --- | --- |
| `POST /v1/responses` (`stream: true`) | Yes | Yes |
| `POST /v1/images/generations` (JSON) | Yes | No |
| `POST /v1/images/edits` (JSON) | Yes | No |

## Build and install

```bash
cargo build --release
install -Dm755 target/release/c2a /usr/bin/c2a
```

The binary uses each provider's device login flow and identifies upstream as
`c2a`. Provider compatibility details may change without notice.

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
```

`quota` fetches the provider's current remaining allowance. Reset timestamps are
shown in local time with the remaining days, hours, and minutes.
Codex reports percentage remaining for each usage window. Copilot reports AI
credits on current plans and premium requests on legacy plans.

Credentials and audit records are stored under `$XDG_STATE_HOME/c2a`, or
`~/.local/state/c2a` when `XDG_STATE_HOME` is unset. The state directory must be
owned by the current user with mode `0700`; `codex.json`, `copilot.json`, and
`audit.jsonl` must be regular user-owned files with mode `0600`. Credential
files are directly locked during reads and writes. If a nonempty credential
file is corrupt, log in to that provider again to replace it.

## Run manually

```bash
c2a codex serve /run/user/$(id -u)/c2a/codex.sock
c2a copilot serve /run/user/$(id -u)/c2a/copilot.sock
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
```

On Arch Linux, build and install the package instead:

```bash
makepkg -si
```

The system service runs as root, stores state under `/root/.local/state/c2a`, and
activates `/run/c2a/codex.sock` and `/run/c2a/copilot.sock` with mode `0600`.

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
ID. `upstream_http_status` is absent when an image operation fails before an
upstream response is received. Existing records are left unchanged.
Request bodies, response bodies, tokens, account data, headers,
errors, and payload hashes are not retained. Bounded upstream error details are
returned only in the immediate local error response. Upstream HTTP errors keep
their status, `Retry-After`, and sanitized request ID; failures within c2a are
returned as `502 Bad Gateway`.

Audit lines are directly locked and appended immediately, but are not
synchronously forced to stable storage for every request.
