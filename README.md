# nvidia-queue-proxy

Cloudflare Worker that proxies NVIDIA API requests through a single global FIFO
queue (implemented as a Durable Object) with rate limiting, cooldown, bounded
queueing, retry/backoff, and streaming.

## Purpose and architecture

The proxy is a server-to-server front door for the NVIDIA inference API
(`https://integrate.api.nvidia.com`, see `NVIDIA_API` in `src/index.js`). It does
not implement its own model logic: it authenticates the caller, serialises all
upstream traffic through one global queue, applies a rate limit and cooldown,
then forwards the request body byte-for-byte and streams the response back.

**What it does**

- Authenticates callers with a shared secret and rejects everything else.
- Serialises upstream dispatch through a single global FIFO queue so one tenant
  cannot exceed the configured rate.
- Enforces a sliding-window rate limit (`RPM`), a per-completion/send cooldown
  (`COOLDOWN_MS`), and a bounded waiting backlog (`MAX_QUEUE_DEPTH`).
- Transparently forwards `POST /v1/*` requests to the upstream and returns the
  upstream response, retrying upstream `429`s with backoff up to a limit.
- Sanitises a denylist of hop-by-hop, credential, topology, and
  infrastructure/tracing response headers.

**What it does not do**

- It does not expose browser CORS and does not implement `GET /v1/models` (see
  "Browser CORS and the models endpoint").
- It is not a general-purpose reverse proxy: only `POST /v1/*` is proxied.
- It does not validate, transform, or parse the JSON body; it never decodes the
  body as text.
- It does not itself call the upstream for `/health` or `/ready`.

**Request flow**

1. The Worker's top-level `fetch` (`export default` in `src/index.js`) handles
   `GET /health` and `GET /ready` first, without authentication or an upstream
   call.
2. All other paths require both Worker secrets to be configured; if `PROXY_KEY`
   or `NVIDIA_API_KEY` is missing the request is rejected with `500`.
3. The caller is authenticated by comparing the supplied key against
   `PROXY_KEY`; a mismatch returns `401`.
4. Non-`POST` methods and paths that do not start with `/v1/` return `404`.
5. The request is forwarded to the single `RateGate` Durable Object instance
   (`env.RATE_GATE.idFromName("nvidia-global-queue")`), which performs body
   admission, queueing, rate limiting, upstream dispatch, and response handling.

**Authentication boundary.** The only authenticated surface is `POST /v1/*`.
Authentication accepts either `Authorization: Bearer <PROXY_KEY>` or the
`x-proxy-key: <PROXY_KEY>` header. The upstream key (`NVIDIA_API_KEY`) is never
sent by the client; the proxy replaces the outbound `Authorization` header with
`Bearer ${NVIDIA_API_KEY}` and removes the client's `x-proxy-key`, `host`, and
`content-length` headers before dispatch.

**Durable Object responsibilities.** The `RateGate` Durable Object is the
single serialisation point. It owns: bounded request-body admission and memory
accounting; the FIFO waiting queue and per-request queue-wait deadline; the
sliding-window rate counter and cooldown timestamps persisted in storage under
`rate-state`; upstream `429` retry/backoff; and response header sanitisation.
Because a single Durable Object instance runs single-threaded, its synchronous
check-then-reserve steps cannot interleave across concurrent requests.

**Liveness vs readiness.** `GET /health` is liveness: it reports `{"ok":true}`
whenever the Worker can answer, independent of configuration and bindings, and
must not be used to infer readiness. `GET /ready` is readiness: it reports
whether the required configuration and bindings are present. See "Health and
readiness" for details.

## Prerequisites and reproducible setup

- **Runtime.** Node.js 22.x and npm 10.x are the versions observed in this
  repository. The `package.json` declares no `engines` field and there is no
  `.nvmrc`, so these are observed rather than enforced.
- **Tooling.** Wrangler is a dev dependency (`wrangler ^4.126.0`, resolved from
  `package-lock.json`) and is run via `npx`, not assumed to be on `PATH`.
- **Secrets.** `PROXY_KEY` and `NVIDIA_API_KEY` must be present for `POST /v1/*`
  to work; see "Configuration".

Reproducible install and local commands (run from repository root):

```bash
# Install exactly the locked dependency tree (npm; lockfile version 3)
npm ci

# Run the full local test suite (Node's built-in test runner)
npm test

# Validate the Worker build and bindings without deploying
npx wrangler deploy --dry-run
```

`npm test` runs `node --test test/batch1.test.js test/runtime.test.js`, which
uses Miniflare (`miniflare` in `package.json`) to exercise the Worker and the
Durable Object locally. The dry-run command performs no deployment and makes no
changes to Cloudflare resources.

## Configuration

### Secrets (required)

Two Worker **secrets** must be configured; they are not stored in
`wrangler.jsonc` and must never be committed or echoed. Set them with
`wrangler secret put` (they are then available as `env.PROXY_KEY` and
`env.NVIDIA_API_KEY` at runtime):

```bash
# Client-facing shared secret. Callers present this value; the proxy compares it.
npx wrangler secret put PROXY_KEY

# Upstream NVIDIA API key. The proxy attaches this to outbound requests; it is
# never sent to clients.
npx wrangler secret put NVIDIA_API_KEY
```

| Secret | Role | Notes |
| --- | --- | --- |
| `PROXY_KEY` | Client authentication | Required. Compared against the caller-supplied key. If missing, every `POST /v1/*` fails with `500` and `/ready` reports not ready. |
| `NVIDIA_API_KEY` | Upstream authentication | Required. Attached as `Authorization: Bearer <NVIDIA_API_KEY>` on outbound requests. If missing, every `POST /v1/*` fails with `500` and `/ready` reports not ready. |

These are the only secrets. All values in the tuning table below are non-secret
configuration and may be supplied as plain Worker variables.

### Durable Object binding

The Worker requires one Durable Object binding, declared in `wrangler.jsonc`
using the current `exports`-based form with SQLite storage — there are no legacy
`migrations` entries to manage:

```jsonc
"durable_objects": {
  "bindings": [{ "name": "RATE_GATE", "class_name": "RateGate" }]
},
"exports": {
  "RateGate": { "type": "durable-object", "storage": "sqlite" }
}
```

| Binding | Class | Purpose |
| --- | --- | --- |
| `RATE_GATE` | `RateGate` | The single global queue, rate limiter, and response handler. `env.RATE_GATE` must be present; otherwise `/ready` reports not ready. |

The proxy always addresses one instance
(`idFromName("nvidia-global-queue")`), so the queue is global rather than
per-caller.

### Tuning variables (non-secret)

All tuning settings are read from the Worker environment. Parsing is strict and
fails closed: an invalid value causes the request to be rejected with HTTP 500
rather than being silently coerced. Missing or explicit `null`/`"null"` values
use the documented default. Empty/whitespace-only strings, `NaN`, infinities,
malformed numbers, fractional values for integer-only settings, negative values
where not listed as allowed, and values outside the bounds below are all
rejected. Configuration is resolved inside the Durable Object request handler;
a `ConfigError` is reported to the client as `{"error":"Server configuration
error"}` with HTTP `500`. Error messages never include secret values.

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
| `MAX_CONCURRENT_BODY_READS` | — | `100` | `1`–`10000` | no | Max bodies read concurrently (see "Aggregate body memory") |
| `MAX_AGGREGATE_BODY_BYTES` | — | `33554432` (32 MiB) | `1024`–`67108864` (64 MiB) | no | Max retained body bytes across all requests, including queued (see "Aggregate body memory") |

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

### Aggregate body memory (`MAX_CONCURRENT_BODY_READS`, `MAX_AGGREGATE_BODY_BYTES`)

Because bodies are fully buffered before dispatch, two independent caps keep the
buffered-body memory bounded:

- `MAX_CONCURRENT_BODY_READS` (default `100`, max `10000`) bounds how many
  request bodies may be read from the network at once.
- `MAX_AGGREGATE_BODY_BYTES` (default `32 MiB`, max `64 MiB`) bounds the sum of
  retained body bytes across all requests.

`MAX_AGGREGATE_BODY_BYTES` must be `>= 1024`.

The two caps measure different lifetimes, and this matters:

- **Read concurrency** (`MAX_CONCURRENT_BODY_READS`) is released as soon as a
  body read settles.
- **Byte retention** (`MAX_AGGREGATE_BODY_BYTES`) is charged for the body's
  **entire retained lifetime** — from admission, through the read, while the
  request waits in the FIFO queue, and until the upstream dispatch completes
  (including any `429` retries, which reuse the same buffer). The reservation is
  released only when the buffered body is no longer needed, so a body sitting in
  the queue still counts against the budget.

The byte bound is enforced in three places:

1. **Admission.** A request whose declared `Content-Length` is present is
   checked against the aggregate budget before it is admitted: if
   `retained + declared > MAX_AGGREGATE_BODY_BYTES` the request is rejected with
   HTTP `503` and a `Retry-After` header. The check and the corresponding
   reservation together run synchronously (no `await` between them), so under
   the single-threaded Durable Object execution model concurrent requests
   cannot all pass the check against stale accounting.
2. **Per chunk.** A request with no `Content-Length` (a chunked stream) is
   admitted optimistically and accounted per chunk. Before any chunk is
   retained, `retained + chunkSize` is checked against the budget; a chunk that
   would exceed it is refused and the request fails with `503`. A chunk is
   therefore never retained past the budget.
3. **Queue wait.** Because the byte reservation is held while a request is
   queued, a new request is rejected with `503` when the retained bodies of the
   active request plus the queued backlog would exceed the budget. Queued bodies
   are therefore included in the aggregate bound, not only bodies currently
   being read.

Cleanup is exactly-once on every terminal path — success, rejection, `408`
timeout, `413`, client abort, queue-depth rejection, queue-wait timeout,
malformed input, and read failure — so a reservation can never leak or be
double-released; a subsequent valid request is admitted once capacity is
released.

**What is bounded.** The sum of committed (retained) body bytes across all
requests — actively reading, queued, and dispatching — never exceeds
`MAX_AGGREGATE_BODY_BYTES`, for both declared and chunked requests.

**What is not strictly bounded by the aggregate budget.** A streaming reader
must materialise a chunk before its size is known, so each admitted reader may
briefly hold one chunk that is not yet accounted. The transient, unaccounted
allowance is therefore `MAX_CONCURRENT_BODY_READS × (max chunk size delivered by
the runtime)`, in addition to the committed budget. The per-request limit for an
unknown-length stream is enforced when the whole chunk is available
(`MAX_BODY_BYTES`), so a single chunk cannot exceed that limit. When a body is
assembled from multiple chunks the source buffers are dereferenced as they are
copied into the final buffer, so the request does not retain both the chunk list
and the assembled copy at full size simultaneously. The aggregate budget bounds
the proxy's own retained buffers; it is not a claim of an absolute process-heap
bound, since the JavaScript runtime may transiently hold unaccounted copies.

Exact HTTP behavior:

| Condition | Status | Body |
| --- | --- | --- |
| Body within the limit, read completes before the deadline | forwarded | — |
| Declared or actual size exceeds `MAX_BODY_BYTES` | `413` | `{"error":"Request body too large"}` |
| Body read exceeds `REQUEST_BODY_TIMEOUT_MS` | `408` | `{"error":"Request body read timed out"}` |
| Body read fails (source error) | `400` | `{"error":"Failed to read request body"}` |
| Client aborts before the body is read | `499` | `{"error":"Client cancelled"}` |

## Queue and rate-limit behavior

All upstream dispatch is serialised through the single `RateGate` Durable
Object, so at most one upstream request is in flight. The relevant controls are
`RPM`, `COOLDOWN_MS`, `MAX_QUEUE_DEPTH`, and `MAX_QUEUE_WAIT_MS`.

- **Sliding-window rate limit (`RPM`).** Up to `RPM` upstream attempts are
  permitted per rolling 60-second window (`WINDOW_MS`). Timestamps of recent
  attempts are persisted in Durable Object storage (`rate-state`); an attempt is
  held until the window has room.
- **Cooldown (`COOLDOWN_MS`).** A minimum gap is enforced between attempts and
  after a completion/inactivity (tracked via `lastSendAt`/`lastCompletionAt`).
  `COOLDOWN_MS = 0` disables the cooldown, but the default `1670 ms` is
  deliberately chosen to match the upstream tolerance described in the source
  comment near `MAX_BACKOFF_MS`.
- **Queue depth (`MAX_QUEUE_DEPTH`).** When a request is already active and the
  waiting backlog is full, a new request is rejected immediately with
  `503 {"error":"Queue depth exceeded"}` and a `Retry-After` header.
  `MAX_QUEUE_DEPTH = 0` allows only the single active request.
- **Queue wait (`MAX_QUEUE_WAIT_MS`).** A queued request that does not get a
  slot within this deadline is rejected with
  `503 {"error":"Maximum queue wait exceeded"}` and a `Retry-After` header. No
  request waits indefinitely.

Queue admission is FIFO. After admission, a request waits for rate-limit room
before each upstream attempt. Tickets and slots are released exactly once on
every terminal path (success, rejection, timeout, client abort, or error), so
capacity cannot leak.

### Retry-After and backoff

On an upstream `429`, the proxy honours the upstream `Retry-After` when present
and re-attempts with bounded exponential backoff: one initial upstream attempt
plus up to `MAX_429_RETRIES` (5) retries, for at most 6 total upstream attempts
when every attempt is made. `Retry-After` parsing follows RFC 7231 / RFC 9110
§10.2.3 (see the note below for the exact rules).

- A valid upstream delay **greater than `MAX_BACKOFF_MS` (30s)** is honoured by
  returning the `429` to the client immediately (with the upstream `Retry-After`
  preserved) rather than waiting.
- A missing, malformed, or smaller delay uses exponential backoff with jitter,
  capped at `MAX_BACKOFF_MS`.
- After the initial attempt and `MAX_429_RETRIES` (5) retries, the `429` is
  returned to the client.

The proxy never retries after a request has finished, and a
cancellation/timeout cannot trigger a late retry (the queue slot is released
exactly once).

#### Retry-After value parsing

An upstream `429` may carry a `Retry-After` header. Parsing follows
RFC 7231 / RFC 9110 §10.2.3 exactly:

- `delay-seconds` must be a bare, non-empty run of digits (`1*DIGIT`).
  Fractional values (`1.5`), scientific notation (`1e2`), sign prefixes
  (`+5`), and non-finite tokens (`Infinity`, `NaN`) are rejected and follow
  the documented fallback (exponential backoff) rather than being coerced.
- An HTTP-date is accepted and interpreted relative to the current time.
- A missing or malformed value falls back to exponential backoff.

The original `Retry-After` value is preserved on the downstream `429` response
(it is not removed by response-header sanitization). The queue slot is released
exactly once on every terminal path, and cancellation/timeout cannot trigger a
retry after the request has finished.

## Request header policy

### Upstream request header sanitization

Before a request is forwarded to NVIDIA, the inbound client header set is
filtered with a **denylist** so content-negotiation and vendor payload headers
keep working. The following are removed:

- the HTTP/1.1 hop-by-hop set (RFC 9110 §7.6.1): `Connection`, `Keep-Alive`,
  `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`,
  `Transfer-Encoding`, `Upgrade`;
- every header **named by the client `Connection` header**;
- caller credentials and session state: the client `Authorization`,
  `Proxy-Authorization`, `Cookie`/`Cookie2`, `Set-Cookie`/`Set-Cookie2`, and the
  proxy secret `X-Proxy-Key`;
- forwarding/topology headers so the caller's IP and internal hosts are not
  disclosed upstream: `Forwarded`, `X-Forwarded-For`, `X-Forwarded-Host`,
  `X-Forwarded-Proto`, `X-Forwarded-Port`, `X-Real-IP`, `Via`, `True-Client-IP`,
  `CDN-Loop`;
- edge/platform headers injected by the runtime that reveal the caller's IP,
  country, or edge POP: the `cf-*` family (`CF-Connecting-IP`, `CF-IPCountry`,
  …);
- the reserved distributed-tracing/platform-internal families listed under the
  response policy (`x-amz-`, `x-b3-`, `grpc-`, `traceparent`, …);
- `Host` and `Content-Length`, which the runtime recomputes for the upstream
  request.

The outbound `Authorization` header is then **set from the `NVIDIA_API_KEY`
Worker secret**, overwriting any client-supplied value; a client can never
inject or override the upstream credential. Documented content negotiation
(`Content-Type`, `Accept`) and neutral vendor headers (`openai-*`,
`x-request-id`, …) are preserved.

## Response header policy

### Upstream response header sanitization

The proxy is a transparent passthrough, so upstream response headers are
forwarded with a **denylist** rather than a fixed allowlist: unknown or
vendor-specific headers are preserved so clients keep working when the upstream
adds new ones. Before any upstream header reaches the client it is stripped of:

- the HTTP/1.1 hop-by-hop set (RFC 9110 §7.6.1): `Connection`, `Keep-Alive`,
  `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`,
  `Transfer-Encoding`, `Upgrade`;
- every header **named by the upstream `Connection` header** (e.g.
  `Connection: x-internal` removes `x-internal`);
- credential/session headers that must not leak: `Authorization`,
  `Proxy-Authorization`, `Set-Cookie`, `Set-Cookie2`;
- network-topology and request-chain disclosure headers, even though the proxy
  otherwise preserves unknown vendor headers: `Forwarded`, `X-Forwarded-For`,
  `X-Forwarded-Host`, `X-Forwarded-Proto`, `X-Forwarded-Port`, `X-Real-IP`,
  `Via`, `Server`;
- reserved distributed-tracing and platform-internal header **families**, matched
  by prefix because they are minted by infrastructure and are never API payload:
  `x-amz-`, `x-amzn-`, `x-ms-`, `x-b3-`, `x-envoy-`, `x-cloud-trace-`, `grpc-`,
  `traceparent`, `tracestate`. Deliberately neutral vendor headers (for example
  `x-custom-upstream`, `x-request-id`, `openai-*`, `x-ratelimit-*`) are
  unaffected, preserving the documented passthrough contract;
- `Content-Length`, because the body may be buffered, transformed, or
  stream-replaced, making the upstream length invalid downstream.

Headers required for correct behavior — the authoritative `Content-Type`,
streaming/cache semantics such as `Cache-Control`, and any `Retry-After` on a
`429` — are preserved. Sanitization is applied on every upstream-derived path:
successful responses, streaming responses, and the `429` retry path. The
response stream is never buffered to enable header processing. The proxy's own
authentication (`401`), rate-limit (`503` + `Retry-After`), and error responses
are generated by the proxy and are never polluted by upstream headers.

## API contract

### Routes and methods

| Route | Method | Auth | Behavior |
| --- | --- | --- | --- |
| `/health` | `GET` | none | Liveness: `200 {"ok":true}`. No upstream call. |
| `/ready` | `GET` | none | Readiness: `200` when all required bindings are present, else `503`. No upstream call. |
| `/v1/*` (any path under `/v1/`) | `POST` | required | Body is buffered, queued, then forwarded to `https://integrate.api.nvidia.com` + the same path/query. |
| authenticated, but any other path or method | any | required | `404 Not found`. |

`/health` and `/ready` are matched by exact pathname and `GET`; other methods on
those paths fall through to the normal routing rules.

Authenticated requests are then method/path-checked; authentication precedes
routing, so an unauthenticated request to an unknown path is rejected with `401`
rather than `404`.

### Authentication

- Supply the shared secret either as `Authorization: Bearer <PROXY_KEY>` or as
  the `x-proxy-key: <PROXY_KEY>` header. A missing/incorrect key returns
  `401 Unauthorized` (plain-text body `Unauthorized`).
- If `PROXY_KEY` or `NVIDIA_API_KEY` is not configured at all, authenticated
  routes return `500 Server configuration error` before any auth check.
- The upstream credential is never accepted from the client: the proxy always
  overwrites the outbound `Authorization` header with
  `Bearer <NVIDIA_API_KEY>` and strips the client's `x-proxy-key`, `host`, and
  `content-length` headers.

### Request shape

The proxy does not parse, validate, or reshape the body. Any body accepted by
the upstream is forwarded byte-for-byte with its original method (`POST`) and
path. Clients must therefore send whatever the NVIDIA chat completions API
expects (for example a JSON object with `model` and `messages`). A
`content-type` header is forwarded as-is; the proxy does not require it and does
not inject one.

### Response and error behavior

Successful responses and their sanitized headers are streamed straight through
from the upstream (see "Upstream response header sanitization"). Errors
generated by the proxy use JSON bodies unless noted:

| Status | Body | When |
| --- | --- | --- |
| `200` | route-specific (see above) | `/health`, `/ready` when ready |
| `401` | `Unauthorized` (plain text) | Missing/incorrect `PROXY_KEY` |
| `404` | `Not found` (plain text) | Authenticated request with non-`POST` method or a path not under `/v1/` |
| `500` | `{"error":"Server configuration error"}` or plain `Server configuration error` | Missing secrets, or invalid tuning configuration |
| `400` | `{"error":"Invalid Content-Length header"}` / `{"error":"Failed to read request body"}` | Malformed `Content-Length`; request-body read failure |
| `413` | `{"error":"Request body too large"}` | Declared or actual body exceeds `MAX_BODY_BYTES` |
| `408` | `{"error":"Request body read timed out"}` | Body read exceeded `REQUEST_BODY_TIMEOUT_MS` |
| `499` | `{"error":"Client cancelled"}` | Client aborted before completion |
| `502` | `{"error":"Proxy upstream request failed"}` | Upstream fetch failed (non-timeout) |
| `503` | `{"error":"..."}` + `Retry-After` | Queue depth exceeded, max queue wait exceeded, or aggregate/concurrent body budget exceeded |
| `504` | `{"error":"Upstream response header timeout"}` + `Retry-After` | Upstream did not send response headers within `RESPONSE_HEADER_TIMEOUT_MS` |
| upstream `429` | upstream body (sanitized) | Upstream rate limit; see "Retry-After handling" |

The `502`, `429`, and streaming paths never expose proxy secrets; the proxy's
own error responses are not polluted by upstream headers.

### Browser CORS and the models endpoint (intentionally not supported)

- **CORS is not enabled.** The intended clients are server-side callers that
  authenticate with a shared secret (`Authorization: Bearer <PROXY_KEY>` or
  `x-proxy-key`). Enabling browser CORS would expose that shared credential to
  arbitrary web origins and is therefore deliberately omitted. Authentication
  runs **before** method/path routing, so an unauthenticated `OPTIONS` (or any
  other method) is rejected with `401`, not `404`; only an **authenticated**
  request with a non-`POST` method or a path outside `/v1/` returns `404` (see
  "Request handling order").
- **`GET /v1/models` is not implemented.** No documented client requires it, and
  the proxy does not model the upstream API surface; fabricating a model list
  would misrepresent the upstream. Only `POST /v1/*` is proxied.

## Health checks and smoke testing

### Health and readiness

Two unauthenticated `GET` endpoints separate liveness from readiness. Neither
contacts the upstream, so neither consumes inference quota, and neither exposes
secret values, environment contents, stack traces, or infrastructure details.

| Endpoint | Meaning | Response |
| --- | --- | --- |
| `GET /health` | Liveness: the Worker is running and can answer. Independent of configuration and bindings. | `200 {"ok":true}` |
| `GET /ready` | Readiness: required bindings are present and usable (`PROXY_KEY`, `NVIDIA_API_KEY`, the `RATE_GATE` Durable Object binding). | `200 {"ready":true,"checks":{...}}` when ready, `503 {"ready":false,"checks":{...}}` otherwise |

`/ready` reports only boolean field names (`proxy_key`, `upstream_key`,
`rate_gate`); it never echoes any value. Because it does not issue an upstream
request, it verifies configuration/binding presence rather than upstream
reachability, which cannot be checked without a billed inference call.

## Post-deployment smoke check

Run only after an authorized deployment, against the deployed URL. It performs a
liveness check, a readiness check, and (optionally) one authenticated request.
Never paste the proxy key on a command line where it would be recorded in shell
history; read it from an environment variable that is not echoed.

```bash
# Liveness (no credentials, no upstream call)
curl -fsS https://<deployed-host>/health

# Readiness (no credentials, no upstream call)
curl -fsS https://<deployed-host>/ready

# Authenticated round-trip (consumes one upstream inference request; run only if
# a real request is acceptable). PROXY_KEY is read from the environment and is
# never written to shell history or logs.
curl -fsS -X POST https://<deployed-host>/v1/chat/completions \
  -H "Authorization: Bearer ${PROXY_KEY}" \
  -H "content-type: application/json" \
  -d '{"model":"<model>","messages":[{"role":"user","content":"ping"}]}'
```

The authenticated request is the only step that reaches the upstream and
therefore the only step that can consume inference quota; it is optional and
should be skipped when a non-billable check is required. These commands were
**NOT RUN** in this batch: no deployment was performed.

The snippets above read `PROXY_KEY` from the environment so the value never
appears in shell history. Do not add the literal key to a `curl -H` argument.

There is also a tracked manual harness at `src/queue-test.html`: a standalone
browser page (Arabic UI) that fires three concurrent `POST /v1/chat/completions`
requests to test queueing. The endpoint URL, `PROXY_KEY`, and model are entered
at runtime, so no `workers.dev` host or credential is embedded in the page, and
the page reports only status codes and timings. It issues **real, billable**
inference requests and does not test `/health` or `/ready`. It is a convenience
tool, not part of the deployed Worker (`wrangler.jsonc` deploys only
`src/index.js`) and not part of `npm test`; treat it as a development aid and
run it only against a local Worker or an explicitly authorized test
environment.

## Cancellation limitations

Cancellation is best-effort and bounded in both directions:

- **Request body:** when the body reader's `cancel()` is awaited it is raced
  against a short bounded grace period, and any rejection is swallowed so it
  cannot become an unhandled rejection. If a stream ignores cancellation, the
  client still receives the `413`/`408`/`400` response promptly, and the
  discarded body can never re-enter the queue or be dispatched.
- **Upstream response stream:** when the client cancels or the stream idle
  timeout fires, the upstream reader's `cancel()` is also raced against a
  bounded grace period (`10s`). The downstream `cancel()` therefore always
  resolves within that grace period and cannot hang indefinitely.

  However, elapsed time is not proof that the upstream operation stopped. If
  the upstream reader's `cancel()` does not settle within the grace period, the
  proxy does **not** release the concurrency slot on the timer alone: the slot
  is preserved so that a subsequent queued request cannot be dispatched while
  the previous upstream connection may still be active. The slot is released as
  soon as the upstream confirms termination — its `cancel()` settles (even
  after the grace elapsed), its read errors, or it reports `done` while the
  response stream is still being drained. This fail-closed policy means that an
  upstream that ignores cancellation and never closes its connection can hold
  the slot indefinitely; unlucky clients are still released because each queued
  request is dropped with a `503` at its own `MAX_QUEUE_WAIT_MS`, so no client
  waits forever even though the affected instance cannot admit a new upstream
  request until the connection stops. This limitation is intentional: the proxy
  never claims both immediate slot release and safe upstream concurrency without
  evidence that the upstream stopped.

## Troubleshooting and operations

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| `500 Server configuration error` / `{"error":"Server configuration error"}` | `PROXY_KEY` or `NVIDIA_API_KEY` missing, or an invalid tuning value | Confirm both secrets are set; check `/ready` for which check is `false`; review tuning values against the Configuration bounds. |
| `401 Unauthorized` | Caller key mismatch | Caller must send `Authorization: Bearer <PROXY_KEY>` or `x-proxy-key: <PROXY_KEY>` with the exact configured value. |
| `404 Not found` | Wrong method or path | Only `POST /v1/*` (plus `GET /health`, `GET /ready`) is routed. |
| `429` from upstream repeatedly | Upstream rate limit | Inspect the upstream `Retry-After`; the proxy retries up to 5 times then surfaces the `429`. Consider lowering `RPM`/raising `COOLDOWN_MS`. |
| `503 Queue depth exceeded` | Backlog full (`MAX_QUEUE_DEPTH`) | Burst exceeds configured backlog; increase `MAX_QUEUE_DEPTH` or reduce offered load. |
| `503 Maximum queue wait exceeded` | Queue wait exceeded `MAX_QUEUE_WAIT_MS` | Sustained load beyond service rate; increase `MAX_QUEUE_WAIT_MS`, raise `RPM`, or reduce load. |
| `503 Too many concurrent uploads...` | `MAX_CONCURRENT_BODY_READS` or `MAX_AGGREGATE_BODY_BYTES` exceeded | Large or many concurrent uploads; raise the aggregate caps or reduce concurrency. |
| `504 Upstream response header timeout` | Upstream slow to respond | Raise `RESPONSE_HEADER_TIMEOUT_MS`. |
| `413` / `408` / `400` on the request body | Body too large, too slow, or unreadable | Compare against `MAX_BODY_BYTES` / `REQUEST_BODY_TIMEOUT_MS`; see "Request body handling". |
| `/ready` returns `503` | A required binding is missing | Inspect the boolean `checks` object (`proxy_key`, `upstream_key`, `rate_gate`); `/ready` does not test upstream reachability. |

Local diagnostics (from repository root):

```bash
# Reproduce configuration and queue behavior in the local test runtime
npm test

# Validate the Worker build and bindings without deploying or mutating anything
npx wrangler deploy --dry-run

# Inspect live Worker logs after an authorized deployment (no secrets are logged)
npx wrangler tail
```

`console.error` diagnostics in the Worker log only an error `name` and a
truncated `message`; they never log API keys, headers, or request bodies.

Operations notes:

- Failures tied to configuration, Durable Object binding presence, or queue
  behavior surface through the status codes above; the corresponding
  configuration lives in Worker variables/secrets and `wrangler.jsonc`.
- Failures tied to the upstream (timeouts, `429`, `502`) require inspecting
  Worker logs (via `wrangler tail`) and the upstream's own status. The Durable
  Object `rate-state` key holds only timestamps, not request contents.
- Rollback is an operational decision, not a destructive local command: redeploy
  the previous known-good Worker version, or adjust the offending tuning
  variable/secret and redeploy. No rollback command is issued here, and no
  destructive Git or Cloudflare operation should be run without explicit
  authorization.
- If the concurrency slot is held because the upstream ignored cancellation and
  never closed its connection (see "Cancellation limitations"), queued clients
  still receive `503` at their own `MAX_QUEUE_WAIT_MS`, but the instance will
  not admit a new upstream request until that connection stops. This may require
  inspecting Worker logs and, in the worst case, operator intervention.

## Security and limitations

- **Secret handling.** `PROXY_KEY` and `NVIDIA_API_KEY` are Worker secrets, not
  repository values. They are never returned to clients, never written to logs,
  and never embedded in error messages. The client's `x-proxy-key` header is
  stripped before the request reaches the upstream.
- **Trust model.** This is a server-to-server proxy. Callers are trusted once
  they present the shared secret; there is no per-user identity, and all callers
  share one global queue and one upstream credential. Do not expose the proxy or
  its secret to browsers or untrusted clients (hence no CORS).
- **Header sanitisation is a denylist, not a guarantee.** Known hop-by-hop,
  Connection-nominated, credential/session, topology, `Content-Length`, and
  reserved tracing/platform prefixes are removed, and unknown vendor headers are
  deliberately forwarded. The denylist therefore does **not** guarantee that
  every possible unknown internal upstream header is removed; it removes the
  documented families and preserves the passthrough contract.
- **Memory bounds are practical, not absolute.** The aggregate budget bounds
  retained request-body bytes for the body's full lifetime, including while the
  request is queued, but a streaming reader may briefly hold one unaccounted-for
  chunk (see "Aggregate body memory"); an absolute process-heap bound is not
  promised.
- **Cancellation is best-effort.** There is no guaranteed recovery if the
  upstream permanently ignores cancellation and never closes its connection
  (see "Cancellation limitations").
- **No CORS, no `/v1/models`.** Both are intentionally unsupported.
- **Verified scope.** All checks in this document were performed locally
  (`npm test`, `wrangler deploy --dry-run`). Live production behavior was **not**
  exercised in this task; no deployment occurred.

## Release verification checklist

Complete these in order for an authorized release:

1. **Local tests** (this task: done) — `npm test` from repository root.
2. **Wrangler dry-run** (this task: done) — `npm run deploy:dry-run`.
3. **Authorized deployment** (this task: **NOT performed**) — deploy via
   `npm run deploy` only with explicit authorization.
4. **Post-deployment smoke test** (this task: **NOT performed**) — run the
   "Post-deployment smoke check" commands against the deployed host. The
   `/health` and `/ready` checks are free and call no upstream; the optional
   authenticated `POST /v1/chat/completions` step consumes one upstream
   inference request and should be run only when acceptable.
5. **Confirm secrets and bindings** (this task: **NOT performed**) — verify
   `PROXY_KEY`, `NVIDIA_API_KEY`, and the `RATE_GATE` Durable Object binding are
   present via `/ready` and `wrangler` configuration.

## Continuous integration and deployment

The repository ships a GitHub Actions workflow
(`.github/workflows/deploy.yml`) that both tests and deploys the Worker:

- **Triggers.** A push to `main` that touches `src/**`, `test/**`, `scripts/**`, `wrangler.jsonc`, `package.json`, or the
  workflow file itself, plus manual `workflow_dispatch`. It does **not** deploy
  on pull requests or on pushes to other branches.
- **Install.** Dependencies are installed reproducibly with `npm ci`.
- **Checks.** `npm run check:canonical` (a guard that fails if a second
  Wrangler config or Worker entry point is present, or if the canonical
  `wrangler.jsonc` `main` does not resolve to `src/index.js`), `npm run lint`,
  `npm test`, and `npm run deploy:dry-run` all run before deploy.
- **Deploy.** On a qualifying push to `main`, and only if the checks pass, the
  workflow runs `npm run deploy` using the `CLOUDFLARE_API_TOKEN` and
  `CLOUDFLARE_ACCOUNT_ID` repository secrets, then records the deployed commit
  SHA and workflow run URL.
- **Smoke test.** An automated post-deploy smoke test verifies `/health` and `/ready` against the deployed endpoint.
