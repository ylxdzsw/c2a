# c2a

`c2a` exposes the native OpenAI Responses API on a local Unix socket and relays
streaming requests to either a ChatGPT Codex subscription or a GitHub Copilot
subscription. It is Linux-only, supports one login for each provider, and
accepts only `POST /v1/responses` with `stream: true`.

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
c2a codex logout

c2a copilot login
c2a copilot status
c2a copilot logout
```

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

## Audit records

Each upstream request produces one JSON line in the shared `audit.jsonl`.
New records identify the provider and contain timestamps, byte counts,
request/response IDs, model, terminal outcome, and input/output token counts
when observable from the SSE stream. Existing records without a provider are
left unchanged. Request bodies, response bodies, tokens, account data, headers,
errors, and payload hashes are not retained. Bounded upstream error details are
returned only in the immediate local error response.

Audit lines are directly locked and appended immediately, but are not
synchronously forced to stable storage for every request.
