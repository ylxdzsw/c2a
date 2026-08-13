# c2a

`c2a` exposes the native OpenAI Responses API on a local Unix socket and relays
streaming requests to the ChatGPT Codex subscription backend. It is Linux-only,
supports one c2a-owned ChatGPT login, and accepts only `POST /v1/responses` with
`stream: true`.

## Build and install

```bash
cargo build --release
install -Dm755 target/release/c2a /usr/bin/c2a
```

The binary uses the current Codex device login flow and identifies upstream as
`c2a`. These private upstream details may change without notice.

## Login and status

```bash
c2a login
c2a status
c2a logout
```

Credentials and audit records are stored under `$XDG_STATE_HOME/c2a`, or
`~/.local/state/c2a` when `XDG_STATE_HOME` is unset. The state directory must be
owned by the current user with mode `0700`; credential, lock, and audit files
must be regular user-owned files with mode `0600`.

## Run manually

```bash
c2a serve /run/user/$(id -u)/c2a/c2a.sock
```

The socket path must not already exist. The service removes only a socket it
created itself.

## Run with systemd

```bash
install -Dm644 c2a.service /usr/lib/systemd/system/c2a.service
install -Dm644 c2a.socket /usr/lib/systemd/system/c2a.socket
systemctl daemon-reload
systemctl enable --now c2a.socket
```

On Arch Linux, build and install the package instead:

```bash
makepkg -si
```

The system service runs as root, stores state under `/root/.local/state/c2a`, and
activates `/run/c2a/c2a.sock` with mode `0600`.

## Configure mu

Percent-encode the absolute socket path in an `http+unix` Responses endpoint.
For the system service:

```jsonc
{
  "providers": {
    "c2a": {
      "endpoint": "http+unix://%2Frun%2Fc2a%2Fc2a.sock/v1/responses",
      "models": {
        "gpt-5.6": {
          "context_window": 353000
        }
      }
    }
  }
}
```

`c2a` does not translate models or request bodies. The configured model name and
native Responses payload are forwarded unchanged. Local request headers are not
forwarded.

## Audit records

Each upstream request produces one JSON line in `audit.jsonl`. Records contain
timestamps, byte counts, request/response IDs, model, terminal outcome, and
input/output token counts when observable from the SSE stream. Request bodies,
response bodies, tokens, account data, headers, errors, and payload hashes are
not retained. Bounded upstream error details are returned only in the immediate
local error response.

Audit lines are appended immediately but are not synchronously forced to stable
storage for every request.
