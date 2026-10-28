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
| `MAX_BODY_BYTES` | — | `20971520` (20 MiB) | `0`–`20971520` (20 MiB) | yes | Maximum accepted request body size |
| `REQUEST_BODY_TIMEOUT_MS` | — | `30000` | `1000`–`120000` | no | Total deadline for reading the incoming request body |

When a canonical name and a legacy alias are both set, the canonical name wins.

### Request body handling (`MAX_BODY_BYTES`, `REQUEST_BODY_TIMEOUT_MS`)

The request body is fully buffered in memory before queue admission and before
any upstream dispatch. It is never decoded as text and never reserialized, so
valid bodies are forwarded byte-for-byte.

`MAX_BODY_BYTES` caps the accepted body size. Because the implementation buffers
the whole body, the configurable value may not exceed the safe cap of 20 MiB
(the default). The declared `Content-Length` is checked first and rejected with
HTTP `413` before the body is read; the actual body is also counted while it is
consumed, so a missing or understated `Content-Length` cannot bypass the limit.
A body exactly at the limit is accepted; one byte over is rejected. With
`MAX_BODY_BYTES = 0`, an empty body is accepted and any non-empty body is
rejected with `413`.

`REQUEST_BODY_TIMEOUT_MS` is a single total deadline covering the entire body
read; it does not restart after each chunk, so a slow client cannot keep a
request alive indefinitely. On expiry the read loop transitions to a terminal
state and stops retaining or requesting further data; best-effort cancellation is
initiated, and the request fails with HTTP `408`. Any read that settles after the
deadline is discarded and never re-enters the queue or dispatches upstream. All
timers are cleared on every terminal path.

Rejected bodies (too large, timed out, or unreadable) never acquire a FIFO
queue ticket, never update upstream attempt accounting, and never call the
upstream fetcher.

Exact HTTP behavior:

| Condition | Status | Body |
| --- | --- | --- |
| Body within the limit, read completes before the deadline | forwarded | — |
| Declared or actual size exceeds `MAX_BODY_BYTES` | `413` | `{"error":"Request body too large"}` |
| Body read exceeds `REQUEST_BODY_TIMEOUT_MS` | `408` | `{"error":"Request body read timed out"}` |
| Body read fails (source error) | `400` | `{"error":"Failed to read request body"}` |
| Client aborts before the body is read | `499` | `{"error":"Client cancelled"}` |

### Cancellation limitations

Body-reader cancellation is best-effort and bounded. When the reader's
`cancel()` is awaited, it is raced against a short bounded grace period, and any
rejection is swallowed so it cannot become an unhandled rejection. This bounds
how long the Worker waits before responding, but it cannot prove that an
uncooperative stream has actually stopped producing. If a stream ignores
cancellation, the client still receives the `413`/`408`/`400` response promptly,
and the discarded body can never re-enter the queue or be dispatched. The
upstream response-stream cancellation policy is separate and remains
fail-closed.
