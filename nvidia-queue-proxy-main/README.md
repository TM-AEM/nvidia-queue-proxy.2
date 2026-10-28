# nvidia-queue-proxy

Cloudflare Worker that proxies NVIDIA API requests through a single global FIFO
queue (implemented as a Durable Object) with rate limiting, cooldown, bounded
queueing, retry/backoff, and streaming.

## Configuration

All settings are read from the Worker environment. Parsing is strict and fails
closed: an invalid value causes the request to be rejected with HTTP 500 rather
than being silently coerced. Missing or explicit `null`/`"null"` values use the
documented default. Empty/whitespace-only strings, `NaN`, infinities, malformed
numbers, fractional values for integer-only settings, negative values where not
listed as allowed, and values outside the bounds below are all rejected.
Error messages never include secret values.

| Variable | Aliases | Default | Bounds | Zero allowed | Notes |
| --- | --- | --- | --- | --- | --- |
| `RPM` | — | `40` | `1`–`100000` | no | Requests per 60s window |
| `COOLDOWN_MS` | — | `1670` | `0`–`60000` | yes | Completion/send cooldown; `0` disables |
| `RESPONSE_HEADER_TIMEOUT_MS` | `UPSTREAM_TIMEOUT_MS` | `30000` | `1`–`86400000` | no | Upstream response-header timeout |
| `STREAM_IDLE_TIMEOUT_MS` | — | `30000` | `1`–`86400000` | no | Max upstream read inactivity while streaming |
| `MAX_QUEUE_WAIT_MS` | `QUEUE_TIMEOUT_MS` | `60000` | `1`–`86400000` | no | Max time a request waits for a queue slot |
| `MAX_QUEUE_DEPTH` | `MAX_QUEUE_SIZE` | `100` | `0`–`100000` | yes | Waiting backlog size; `0` allows only the active request |
| `MAX_BODY_BYTES` | — | `20971520` (20 MiB) | `0`–`1073741824` (1 GiB) | yes | Maximum accepted request body size |

When a canonical name and a legacy alias are both set, the canonical name wins.

### Request body limit (`MAX_BODY_BYTES`)

The declared `Content-Length` is checked first and rejected with HTTP 413 before
the body is read. The actual body is also counted while it is consumed, so a
missing or understated `Content-Length` cannot bypass the limit. Oversized
requests are rejected before queue admission and before any upstream dispatch, so
they never consume a FIFO ticket, dispatch attempt, or rate-limit slot. Valid
bodies are forwarded byte-for-byte (no JSON parsing or reserialization).
