# c2a

`c2a` exposes the native OpenAI Responses API on a local Unix socket and relays
streaming requests to the ChatGPT Codex subscription backend. It is Linux-only,
supports one c2a-owned ChatGPT login, and accepts only `POST /v1/responses` with
`stream: true`.

## Build and install

```bash
cargo build --release
install -Dm755 target/release/c2a ~/.local/bin/c2a
```

The binary uses the current Codex device login flow and compatibility identity
for Codex `0.146.1`. These private upstream details may change without notice.

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
install -Dm644 c2a.service ~/.config/systemd/user/c2a.service
install -Dm644 c2a.socket ~/.config/systemd/user/c2a.socket
systemctl --user daemon-reload
systemctl --user enable --now c2a.socket
```

The activated socket is `%t/c2a/c2a.sock`.

## Configure mu

Percent-encode the absolute socket path in an `http+unix` Responses endpoint.
For a user with UID `1000`:

```jsonc
{
  "providers": {
    "c2a": {
      "endpoint": "http+unix://%2Frun%2Fuser%2F1000%2Fc2a%2Fc2a.sock/v1/responses",
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
only timestamps, byte counts, request/response IDs, model, terminal outcome, and
input/output token counts when observable from the SSE stream. Request bodies,
response bodies, tokens, account data, headers, errors, and payload hashes are
not retained.
