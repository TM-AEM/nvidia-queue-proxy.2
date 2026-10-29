import { DurableObject } from "cloudflare:workers";

const NVIDIA_API = "https://integrate.api.nvidia.com";

// Invoke the global fetch without a receiver. workerd host functions throw
// "Illegal invocation" when called with a non-undefined `this`, so the global
// fetch must never be invoked as an object property (e.g. `this.fetchImpl(...)`).
// Wrapping it in an arrow preserves the injectable `env.__fetch` test seam.
const boundFetch = (...args) => fetch(...args);

const WINDOW_MS = 60_000;
const DEFAULT_RPM = 40;
const DEFAULT_COOLDOWN_MS = 1_670;
const MAX_429_RETRIES = 5;
const MAX_BACKOFF_MS = 30_000;
// Fraction of the computed delay used as an additive jitter band. Jitter is
// additive only: it spreads retries upward to desynchronize clients without
// ever reducing a delay below the policy floor (max of exponential backoff and
// Retry-After), so a server-requested Retry-After minimum is always honored.
const BACKOFF_JITTER_FRACTION = 0.25;
const DEFAULT_RESPONSE_HEADER_TIMEOUT_MS = 30_000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_QUEUE_WAIT_MS = 60_000;
const DEFAULT_MAX_QUEUE_DEPTH = 100;
const DEFAULT_MAX_BODY_BYTES = 20 * 1024 * 1024;
const DEFAULT_REQUEST_BODY_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_CONCURRENT_BODY_READS = 100;
const MAX_CONCURRENT_BODY_READS_LIMIT = 10_000;
const DEFAULT_MAX_AGGREGATE_BODY_BYTES = 32 * 1024 * 1024;
const MAX_AGGREGATE_BODY_BYTES_LIMIT = 64 * 1024 * 1024;
const DEFAULT_STREAM_CANCEL_GRACE_MS = 10_000;

const MAX_TIMEOUT_MS = 86_400_000;
const MAX_RPM_LIMIT = 100_000;
const MAX_QUEUE_DEPTH_LIMIT = 100_000;
const MAX_BODY_BYTES_LIMIT = DEFAULT_MAX_BODY_BYTES;
const MIN_REQUEST_BODY_TIMEOUT_MS = 1_000;
const MAX_REQUEST_BODY_TIMEOUT_MS = 120_000;
const DEFAULT_BODY_CANCEL_GRACE_MS = 1_000;

// Maximum number of bytes of an upstream 429 (rate-limit) response body that
// the proxy will buffer. The error body is only passed through to the client
// and is never parsed, so a small fixed cap is sufficient; bounding the read
// prevents a large or hostile rate-limit response from exhausting Durable
// Object memory. A body that exceeds this cap is replaced with a short
// synthetic error body (see TRUNCATED_429_BODY).
const MAX_429_BODY_BYTES = 64 * 1024;

// Hard time cap for reading an upstream 429 error body. Bounds how long a
// stalled upstream read can hold the active queue slot before the body is
// abandoned and a synthetic error body is returned.
const DEFAULT_429_BODY_TIMEOUT_MS = 30_000;

// Deterministic, valid JSON returned in place of an upstream 429 body that was
// too large or could not be read. Truncating an arbitrary payload could yield
// invalid JSON, so a fixed synthetic body is returned instead.
const TRUNCATED_429_BODY = JSON.stringify({
  error: "Upstream rate-limit response body exceeded the proxy limit",
});

const CONFIG_SPECS = {
  RPM: {
    envNames: ["RPM"],
    default: DEFAULT_RPM,
    min: 1,
    max: MAX_RPM_LIMIT,
    integer: true,
    allowZero: false,
  },
  COOLDOWN_MS: {
    envNames: ["COOLDOWN_MS"],
    default: DEFAULT_COOLDOWN_MS,
    min: 0,
    max: 60_000,
    integer: true,
    allowZero: true,
  },
  RESPONSE_HEADER_TIMEOUT_MS: {
    envNames: ["RESPONSE_HEADER_TIMEOUT_MS", "UPSTREAM_TIMEOUT_MS"],
    default: DEFAULT_RESPONSE_HEADER_TIMEOUT_MS,
    min: 1,
    max: MAX_TIMEOUT_MS,
    integer: true,
    allowZero: false,
  },
  STREAM_IDLE_TIMEOUT_MS: {
    envNames: ["STREAM_IDLE_TIMEOUT_MS"],
    default: DEFAULT_STREAM_IDLE_TIMEOUT_MS,
    min: 1,
    max: MAX_TIMEOUT_MS,
    integer: true,
    allowZero: false,
  },
  MAX_QUEUE_WAIT_MS: {
    envNames: ["MAX_QUEUE_WAIT_MS", "QUEUE_TIMEOUT_MS"],
    default: DEFAULT_MAX_QUEUE_WAIT_MS,
    min: 1,
    max: MAX_TIMEOUT_MS,
    integer: true,
    allowZero: false,
  },
  MAX_QUEUE_DEPTH: {
    envNames: ["MAX_QUEUE_DEPTH", "MAX_QUEUE_SIZE"],
    default: DEFAULT_MAX_QUEUE_DEPTH,
    min: 0,
    max: MAX_QUEUE_DEPTH_LIMIT,
    integer: true,
    allowZero: true,
  },
  MAX_BODY_BYTES: {
    envNames: ["MAX_BODY_BYTES"],
    default: DEFAULT_MAX_BODY_BYTES,
    min: 0,
    max: MAX_BODY_BYTES_LIMIT,
    integer: true,
    allowZero: true,
  },
  REQUEST_BODY_TIMEOUT_MS: {
    envNames: ["REQUEST_BODY_TIMEOUT_MS"],
    default: DEFAULT_REQUEST_BODY_TIMEOUT_MS,
    min: MIN_REQUEST_BODY_TIMEOUT_MS,
    max: MAX_REQUEST_BODY_TIMEOUT_MS,
    integer: true,
    allowZero: false,
  },
  MAX_CONCURRENT_BODY_READS: {
    envNames: ["MAX_CONCURRENT_BODY_READS"],
    default: DEFAULT_MAX_CONCURRENT_BODY_READS,
    min: 1,
    max: MAX_CONCURRENT_BODY_READS_LIMIT,
    integer: true,
    allowZero: false,
  },
  MAX_AGGREGATE_BODY_BYTES: {
    envNames: ["MAX_AGGREGATE_BODY_BYTES"],
    default: DEFAULT_MAX_AGGREGATE_BODY_BYTES,
    min: 1024,
    max: MAX_AGGREGATE_BODY_BYTES_LIMIT,
    integer: true,
    allowZero: false,
  },
};

export class ConfigError extends Error {
  constructor(key, message) {
    super(message);
    this.name = "ConfigError";
    this.configKey = key;
  }
}

export function readConfigValue(rawValue, spec, key) {
  if (
    rawValue === undefined ||
    rawValue === null ||
    rawValue === "null"
  ) {
    return spec.default;
  }

  if (typeof rawValue === "number") {
    return validateConfigNumber(rawValue, spec, key);
  }

  if (typeof rawValue === "string") {
    const trimmed = rawValue.trim();

    if (trimmed === "") {
      throw new ConfigError(
        key,
        `Invalid configuration for ${key}: empty or whitespace-only value`
      );
    }

    const pattern = spec.integer
      ? /^-?\d+$/
      : /^-?\d+(\.\d+)?$/;

    if (!pattern.test(trimmed)) {
      throw new ConfigError(
        key,
        `Invalid configuration for ${key}: not a valid number`
      );
    }

    return validateConfigNumber(Number(trimmed), spec, key);
  }

  throw new ConfigError(
    key,
    `Invalid configuration for ${key}: unsupported value type`
  );
}

function validateConfigNumber(num, spec, key) {
  if (!Number.isFinite(num)) {
    throw new ConfigError(
      key,
      `Invalid configuration for ${key}: not a finite number`
    );
  }

  if (spec.integer && !Number.isInteger(num)) {
    throw new ConfigError(
      key,
      `Invalid configuration for ${key}: must be an integer`
    );
  }

  if (num < spec.min || num > spec.max) {
    throw new ConfigError(
      key,
      `Invalid configuration for ${key}: must be between ${spec.min} and ${spec.max}`
    );
  }

  if (num === 0 && !spec.allowZero) {
    throw new ConfigError(
      key,
      `Invalid configuration for ${key}: zero is not a supported value`
    );
  }

  return num;
}

export function resolveConfig(env) {
  const resolved = {};

  for (const [key, spec] of Object.entries(CONFIG_SPECS)) {
    let rawValue;

    for (const name of spec.envNames) {
      if (env?.[name] !== undefined) {
        rawValue = env[name];
        break;
      }
    }

    resolved[key] = readConfigValue(rawValue, spec, key);
  }

  return resolved;
}

export class BodyTooLargeError extends Error {
  constructor() {
    super("Request body exceeds the configured maximum size");
    this.name = "BodyTooLargeError";
  }
}

export class BodyReadTimeoutError extends Error {
  constructor() {
    super("Request body read timed out");
    this.name = "BodyReadTimeoutError";
  }
}

export class AggregateBodyLimitExceededError extends Error {
  constructor() {
    super("Aggregate request body memory limit exceeded");
    this.name = "AggregateBodyLimitExceededError";
  }
}

function cancelReaderBounded(
  reader,
  reason,
  setTimeoutImpl,
  clearTimeoutImpl,
  graceMs
) {
  let cancelResult;

  try {
    cancelResult = reader.cancel(reason);
  } catch {
    return Promise.resolve();
  }

  const settled = Promise.resolve(cancelResult).catch(() => {});

  return new Promise((resolve) => {
    let done = false;

    const finish = () => {
      if (done) return;
      done = true;
      resolve();
    };

    const timer = setTimeoutImpl(finish, graceMs);

    settled.then(() => {
      clearTimeoutImpl(timer);
      finish();
    });
  });
}

export async function readBoundedBody(
  request,
  maxBytes,
  options = {}
) {
  const {
    timeoutMs = DEFAULT_REQUEST_BODY_TIMEOUT_MS,
    setTimeoutImpl = setTimeout,
    clearTimeoutImpl = clearTimeout,
    cancelGraceMs = DEFAULT_BODY_CANCEL_GRACE_MS,
    onChunk = null,
  } = options;

  const declared = request.headers.get("content-length");

  if (declared !== null) {
    const declaredBytes = Number(declared);

    if (
      Number.isFinite(declaredBytes) &&
      declaredBytes > maxBytes
    ) {
      throw new BodyTooLargeError();
    }
  }

  const bodyStream = request.body;

  if (!bodyStream) {
    return new Uint8Array(0);
  }

  const reader = bodyStream.getReader();
  const chunks = [];
  let total = 0;
  let timer = null;

  const terminal = { stopped: false };

  const deadline = new Promise((resolve) => {
    timer = setTimeoutImpl(() => {
      terminal.stopped = true;
      resolve();
    }, timeoutMs);
  });

  const readDone = (async () => {
    for (;;) {
      const { done, value } = await reader.read();

      if (terminal.stopped) return;
      if (done) break;

      total += value.byteLength;

      if (total > maxBytes) {
        throw new BodyTooLargeError();
      }

      if (typeof onChunk === "function") {
        onChunk(value.byteLength);
      }

      chunks.push(value);
    }
  })();

  readDone.catch(() => {});

  const outcome = await Promise.race([
    readDone.then(() => ({ kind: "done" })),
    deadline.then(() => ({ kind: "timeout" })),
  ]).catch(async (readError) => {
    terminal.stopped = true;
    clearTimeoutImpl(timer);

    await cancelReaderBounded(
      reader,
      readError,
      setTimeoutImpl,
      clearTimeoutImpl,
      cancelGraceMs
    );

    throw readError;
  });

  clearTimeoutImpl(timer);

  if (outcome.kind === "timeout") {
    await cancelReaderBounded(
      reader,
      new BodyReadTimeoutError(),
      setTimeoutImpl,
      clearTimeoutImpl,
      cancelGraceMs
    );

    throw new BodyReadTimeoutError();
  }

  if (chunks.length === 0) {
    return new Uint8Array(0);
  }

  if (chunks.length === 1) {
    return chunks[0];
  }

  // Assemble the final body. The aggregate byte reservation is still held
  // while this copy runs, so the destination buffer is bounded by the budget.
  // Clear each source chunk reference as soon as it has been copied so the
  // original per-chunk buffers can be reclaimed immediately instead of the
  // whole `chunks` array and `combined` being retained simultaneously (which
  // would momentarily double the request's resident body memory).
  const combined = new Uint8Array(total);
  let offset = 0;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    combined.set(chunk, offset);
    offset += chunk.byteLength;
    chunks[i] = null;
  }

  return combined;
}

// Bounded read of an upstream *response* body. Unlike readBoundedBody (which
// owns the request-side semantics and has no truncation mode), this helper is
// used for the upstream 429 error body: it reads incrementally with a hard byte
// cap and a hard time cap, and reports whether the body was truncated so the
// caller can substitute a safe synthetic body instead of silently corrupting
// the payload.
//
// Returns { bytes, truncated }. On truncation the upstream stream is cancelled
// on a best-effort basis, bounded by the same cancel grace used elsewhere so a
// stalled upstream cancellation cannot hold the queue.
async function readBoundedResponseBody(
  response,
  maxBytes,
  options = {}
) {
  const {
    timeoutMs = DEFAULT_429_BODY_TIMEOUT_MS,
    setTimeoutImpl = setTimeout,
    clearTimeoutImpl = clearTimeout,
    cancelGraceMs = DEFAULT_BODY_CANCEL_GRACE_MS,
  } = options;

  const bodyStream = response?.body;

  if (!bodyStream) {
    return { bytes: new Uint8Array(0), truncated: false };
  }

  const reader = bodyStream.getReader();
  const chunks = [];
  let total = 0;
  let timer = null;

  const terminal = { stopped: false };

  const deadline = new Promise((resolve) => {
    timer = setTimeoutImpl(() => {
      terminal.stopped = true;
      resolve();
    }, timeoutMs);
  });

  const readDone = (async () => {
    for (;;) {
      const { done, value } = await reader.read();

      if (terminal.stopped) return;
      if (done) return;

      if (value === undefined || value === null) continue;

      if (total + value.byteLength > maxBytes) {
        terminal.stopped = true;
        throw new BodyTooLargeError();
      }

      total += value.byteLength;
      chunks.push(value);
    }
  })();

  readDone.catch(() => {});

  let outcome;

  try {
    outcome = await Promise.race([
      readDone.then(() => ({ kind: "done" })),
      deadline.then(() => ({ kind: "timeout" })),
    ]);
  } catch (readError) {
    // A read failure, an over-limit throw, or a cancellation rejection.
    terminal.stopped = true;
    clearTimeoutImpl(timer);

    await cancelReaderBounded(
      reader,
      readError,
      setTimeoutImpl,
      clearTimeoutImpl,
      cancelGraceMs
    );

    return { bytes: null, truncated: true };
  }

  clearTimeoutImpl(timer);

  if (outcome.kind === "timeout") {
    terminal.stopped = true;

    await cancelReaderBounded(
      reader,
      new BodyReadTimeoutError(),
      setTimeoutImpl,
      clearTimeoutImpl,
      cancelGraceMs
    );

    return { bytes: null, truncated: true };
  }

  if (chunks.length === 0) {
    return { bytes: new Uint8Array(0), truncated: false };
  }

  if (chunks.length === 1) {
    return { bytes: chunks[0], truncated: false };
  }

  const combined = new Uint8Array(total);
  let offset = 0;

  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return { bytes: combined, truncated: false };
}

export function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      return reject(signal.reason || new Error("Aborted"));
    }

    const timer = setTimeout(() => {
      if (signal) {
        signal.removeEventListener("abort", onAbort);
      }

      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason || new Error("Aborted"));
    };

    if (signal) {
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

export function parseRetryAfterMs(response, now = Date.now()) {
  const value = response?.headers?.get("Retry-After");

  if (!value) return null;

  const trimmed = value.trim();

  // RFC 7231 / RFC 9110 §10.2.3: Retry-After = HTTP-date / delay-seconds,
  // delay-seconds = 1*DIGIT. A bare delay-seconds value must therefore be a
  // non-empty, digits-only string; coercive numeric parsing (e.g. "+5",
  // "5abc", "1e2", "Infinity") is rejected so that malformed headers follow
  // the documented fallback rather than silently becoming a delay.
  const delayMatch = /^\d+$/.test(trimmed);

  if (delayMatch) {
    const seconds = Number(trimmed);

    if (seconds === 0) {
      return 0;
    }

    const ms = seconds * 1000;
    return Number.isFinite(ms) ? Math.round(ms) : Infinity;
  }

  // Reject anything that looks numeric (fractional seconds, scientific
  // notation, +/- prefix, Infinity/NaN) before attempting HTTP-date parsing:
  // Date.parse() is over-permissive and can turn "1.5" into a date, which is
  // not a valid Retry-After value per RFC 7231 / RFC 9110.
  if (/^[+-]?\d+([.,]\d+)?([eE][+-]?\d+)?\s*$/.test(trimmed)) {
    return null;
  }

  const date = Date.parse(trimmed);

  if (Number.isFinite(date)) {
    return Math.max(0, date - now);
  }

  return null;
}

export function retryAfterMs(
  response,
  now = Date.now(),
  maxMs = MAX_BACKOFF_MS
) {
  const parsed = parseRetryAfterMs(response, now);

  if (parsed === null) return 0;

  return Math.min(maxMs, parsed);
}

// Additive jitter: spreads retries upward to desynchronize clients without ever
// dropping below the policy floor (the pre-jitter delay). The band is limited
// both by BACKOFF_JITTER_FRACTION of the delay and by the remaining headroom to
// maxMs, so a delay is never pushed above the configured maximum and a
// server-requested Retry-After minimum is never reduced.
export function applyBackoffJitter(
  delay,
  random = Math.random,
  maxMs = MAX_BACKOFF_MS,
  fraction = BACKOFF_JITTER_FRACTION
) {
  if (!(delay > 0)) return delay;

  const span = Math.min(fraction * delay, Math.max(0, maxMs - delay));

  // The injected random source is sampled once and normalized: a non-finite
  // value (NaN, undefined, Infinity) or one outside [0, 1] falls back to the
  // full-jitter upper bound (1) so an invalid source can never yield a NaN
  // delay or silently disable backoff. Valid inputs are used as-is.
  const sample = random();
  const r = Number.isFinite(sample) && sample >= 0 && sample <= 1 ? sample : 1;

  const jittered = delay + Math.round(span * r);

  return Math.min(maxMs, Math.max(delay, jittered));
}

export function computeBackoffDelay(
  retryIndex,
  response,
  options = {}
) {
  const {
    now = Date.now(),
    maxBackoffMs = MAX_BACKOFF_MS,
    baseDelayMs = 1000,
    jitterFn = null,
  } = options;

  const expDelay = baseDelayMs * (2 ** retryIndex);
  const retryAfter = response
    ? retryAfterMs(response, now, maxBackoffMs)
    : 0;

  const delay = Math.min(
    maxBackoffMs,
    Math.max(expDelay, retryAfter)
  );

  if (typeof jitterFn === "function") {
    return jitterFn(delay);
  }

  return delay;
}

// Headers that must never be forwarded downstream from an upstream response.
//
// - The first group is the HTTP/1.1 hop-by-hop set (RFC 9110 §7.6.1): these
//   describe a single connection and are meaningless (and unsafe) to relay.
// - The second group carries credentials or session state that belong to the
//   upstream and must not leak to the client.
// - `content-length` is always removed because the proxy may buffer, transform,
//   or stream-replace the body, so the upstream length would be wrong.
const HOP_BY_HOP_RESPONSE_HEADERS = [
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
];

const SENSITIVE_RESPONSE_HEADERS = [
  "authorization",
  "proxy-authorization",
  "set-cookie",
  "set-cookie2",
];

// Headers that disclose the proxy's or the upstream's network topology,
// infrastructure, or request-chain metadata. They are hop-by-hop-adjacent
// intermediary metadata (RFC 9110 §7.6.3 `Via`/`Server`/`Forwarded`) that a
// transparent API gateway must not relay: doing so can reveal client IPs,
// internal hosts, or intermediate proxies to the downstream client. They are
// removed even though the proxy otherwise preserves unknown vendor headers, so
// no recognized vendor payload header is affected.
const TOPOLOGY_RESPONSE_HEADERS = [
  "forwarded",
  "x-forwarded-for",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
  "x-real-ip",
  "via",
  "server",
];

// Distributed-tracing and platform-internal diagnostic header *families*.
//
// These prefixes are reserved by the tracing/infrastructure platforms that mint
// them (W3C Trace Context, Zipkin `b3`, AWS `x-amz*`, Azure `x-ms-*`, Envoy
// `x-envoy-*`, gRPC `grpc-*`) and are never part of an API payload. A
// transparent gateway that forwards them leaks internal trace identifiers,
// infrastructure host/service names, and routing metadata to the client.
//
// Only these reserved families are matched by prefix; deliberately neutral
// vendor headers (e.g. `x-custom-upstream`, `x-request-id`, `openai-*`,
// `x-ratelimit-*`) are unaffected, preserving the documented passthrough
// contract.
const INFRASTRUCTURE_RESPONSE_HEADER_PREFIXES = [
  "x-amz-",
  "x-amzn-",
  "x-ms-",
  "x-b3-",
  "x-envoy-",
  "x-cloud-trace-",
  "grpc-",
  "traceparent",
  "tracestate",
];

// Sanitize an upstream response header set before it is forwarded downstream.
export function safeResponseHeaders(headers) {
  const result = new Headers(headers);

  for (const name of HOP_BY_HOP_RESPONSE_HEADERS) {
    result.delete(name);
  }

  for (const name of SENSITIVE_RESPONSE_HEADERS) {
    result.delete(name);
  }

  for (const name of TOPOLOGY_RESPONSE_HEADERS) {
    result.delete(name);
  }

  // Remove reserved distributed-tracing / platform-internal header families by
  // prefix. The set of present header names is snapshotted first so deletion
  // during iteration is safe.
  for (const name of [...result.keys()]) {
    for (const prefix of INFRASTRUCTURE_RESPONSE_HEADER_PREFIXES) {
      if (name.startsWith(prefix)) {
        result.delete(name);
        break;
      }
    }
  }

  // Headers nominated by the upstream `Connection` header are themselves
  // hop-by-hop and must be removed along with the `Connection` header
  // (RFC 9110 §7.6.1). The tokens are parsed from the pre-sanitized value so a
  // header named only via `Connection` cannot survive.
  const connectionValue = headers.get("connection");

  if (connectionValue) {
    for (const token of connectionValue.split(",")) {
      const name = token.trim().toLowerCase();

      if (name) {
        result.delete(name);
      }
    }
  }

  // The body may have been buffered, transformed, or replaced; the upstream
  // length is never valid downstream.
  result.delete("content-length");

  return result;
}

export class RateGate extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    this.waitingQueue = [];
    this.activeTicket = null;
    this.activeBodyReads = 0;
    this.activeBodyBytes = 0;

    this.now =
      typeof env?.__now === "function"
        ? env.__now
        : () => Date.now();

    this.getNow = () => this.now();

    this.sleepImpl =
      typeof env?.__sleep === "function"
        ? env.__sleep
        : sleep;

    this.fetchImpl =
      typeof env?.__fetch === "function"
        ? env.__fetch
        : boundFetch;

    this.setTimeoutImpl =
      typeof env?.__setTimeout === "function"
        ? env.__setTimeout
        : setTimeout;

    this.clearTimeoutImpl =
      typeof env?.__clearTimeout === "function"
        ? env.__clearTimeout
        : clearTimeout;

    this.randomImpl =
      typeof env?.__random === "function"
        ? env.__random
        : Math.random;
  }

  cleanTicket(ticket) {
    if (ticket.timer !== null) {
      clearTimeout(ticket.timer);
      ticket.timer = null;
    }

    if (ticket.abortHandler && ticket.request?.signal) {
      ticket.request.signal.removeEventListener(
        "abort",
        ticket.abortHandler
      );

      ticket.abortHandler = null;
    }
  }

  removeFromWaitingQueue(ticket) {
    const index = this.waitingQueue.indexOf(ticket);

    if (index !== -1) {
      this.waitingQueue.splice(index, 1);
    }
  }

  releaseSlot() {
    this.activeTicket = null;

    while (this.waitingQueue.length > 0) {
      const nextTicket = this.waitingQueue.shift();

      if (nextTicket.settled) {
        continue;
      }

      nextTicket.settled = true;
      this.cleanTicket(nextTicket);
      this.activeTicket = nextTicket;
      nextTicket.resolve(nextTicket);

      return;
    }
  }

  async acquireSlot(request, maxQueueWaitMs, cooldownMs) {
    if (!this.activeTicket) {
      const ticket = {
        request,
        settled: true,
        timer: null,
        abortHandler: null,
      };

      this.activeTicket = ticket;

      return ticket;
    }

    let ticketResolve;

    const ticketPromise = new Promise((resolve) => {
      ticketResolve = resolve;
    });

    const ticket = {
      request,
      resolve: ticketResolve,
      timer: null,
      abortHandler: null,
      settled: false,
    };

    ticket.timer = setTimeout(() => {
      if (ticket.settled) return;

      ticket.settled = true;
      this.removeFromWaitingQueue(ticket);
      this.cleanTicket(ticket);

      const retryAfterSec = Math.max(
        1,
        Math.ceil(cooldownMs / 1000)
      );

      ticket.resolve({
        rejectedResponse: Response.json(
          { error: "Maximum queue wait exceeded" },
          {
            status: 503,
            headers: {
              "Retry-After": String(retryAfterSec),
            },
          }
        ),
      });
    }, maxQueueWaitMs);

    if (request.signal) {
      if (request.signal.aborted) {
        this.cleanTicket(ticket);
        ticket.settled = true;

        return {
          rejectedResponse: Response.json(
            { error: "Client cancelled" },
            { status: 499 }
          ),
        };
      }

      ticket.abortHandler = () => {
        if (ticket.settled) return;

        ticket.settled = true;
        this.removeFromWaitingQueue(ticket);
        this.cleanTicket(ticket);

        ticket.resolve({
          rejectedResponse: Response.json(
            { error: "Client cancelled" },
            { status: 499 }
          ),
        });
      };

      request.signal.addEventListener(
        "abort",
        ticket.abortHandler,
        { once: true }
      );
    }

    this.waitingQueue.push(ticket);

    return await ticketPromise;
  }

  async waitForSlot(state, rpm, cooldownMs, signal) {
    while (true) {
      if (signal?.aborted) {
        throw signal.reason || new Error("Aborted");
      }

      const now = this.now();

      state.starts = state.starts.filter(
        (time) => now - time < WINDOW_MS
      );

      let wait = 0;

      if (state.starts.length >= rpm) {
        const oldestNeeded =
          state.starts[state.starts.length - rpm];

        wait = Math.max(
          wait,
          oldestNeeded + WINDOW_MS - now
        );
      }

      if (state.lastCompletionAt) {
        wait = Math.max(
          wait,
          state.lastCompletionAt + cooldownMs - now
        );
      }

      if (state.lastSendAt) {
        wait = Math.max(
          wait,
          state.lastSendAt + cooldownMs - now
        );
      }

      if (wait <= 0) return;

      await this.sleepImpl(wait, signal);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (
      request.method !== "POST" ||
      !url.pathname.startsWith("/v1/")
    ) {
      return new Response("Not found", { status: 404 });
    }

    let config;

    try {
      config = resolveConfig(this.env);
    } catch (configError) {
      if (!(configError instanceof ConfigError)) {
        throw configError;
      }

      return Response.json(
        { error: "Server configuration error" },
        { status: 500 }
      );
    }

    const {
      RPM: rpm,
      COOLDOWN_MS: cooldownMs,
      RESPONSE_HEADER_TIMEOUT_MS: responseHeaderTimeoutMs,
      STREAM_IDLE_TIMEOUT_MS: streamIdleTimeoutMs,
      MAX_QUEUE_WAIT_MS: maxQueueWaitMs,
      MAX_QUEUE_DEPTH: maxQueueDepth,
      MAX_BODY_BYTES: maxBodyBytes,
      REQUEST_BODY_TIMEOUT_MS: requestBodyTimeoutMs,
      MAX_CONCURRENT_BODY_READS: maxConcurrentBodyReads,
      MAX_AGGREGATE_BODY_BYTES: maxAggregateBodyBytes,
    } = config;

    const hasBodyStream = Boolean(request.body);

    let declaredBytes = 0;
    const clHeader = request.headers.get("content-length");
    if (clHeader !== null) {
      const trimmed = clHeader.trim();
      if (!/^\d+$/.test(trimmed)) {
        return Response.json(
          { error: "Invalid Content-Length header" },
          { status: 400 }
        );
      }
      const n = Number(trimmed);
      if (!Number.isSafeInteger(n)) {
        return Response.json(
          { error: "Invalid Content-Length header" },
          { status: 400 }
        );
      }
      declaredBytes = n;
    }

    if (declaredBytes > maxBodyBytes) {
      return Response.json(
        { error: "Request body too large" },
        { status: 413 }
      );
    }

    if (
      hasBodyStream &&
      (this.activeBodyReads >= maxConcurrentBodyReads ||
        (declaredBytes > 0 &&
          this.activeBodyBytes + declaredBytes > maxAggregateBodyBytes))
    ) {
      const retryAfterSec = Math.max(
        1,
        Math.ceil(cooldownMs / 1000)
      );

      return Response.json(
        { error: "Too many concurrent uploads, please retry later" },
        {
          status: 503,
          headers: {
            "Retry-After": String(retryAfterSec),
          },
        }
      );
    }

    let allocatedBytes = declaredBytes;
    let totalReadBytes = 0;
    if (hasBodyStream) {
      this.activeBodyReads++;
      this.activeBodyBytes += allocatedBytes;
    }

    // The aggregate byte budget must be charged for as long as the buffered
    // body is retained by the application, not merely while it is being read.
    // The buffer stays alive across the queue wait and upstream dispatch, so
    // the reservation is held until the request's terminal path. This flag
    // makes the release idempotent so every exit path releases exactly once.
    let bodyRetained = hasBodyStream;

    const releaseBodyRetention = () => {
      if (!bodyRetained) return;
      bodyRetained = false;
      this.activeBodyBytes = Math.max(
        0,
        this.activeBodyBytes - allocatedBytes
      );
    };

    let body;

    try {
      body = await readBoundedBody(
        request,
        maxBodyBytes,
        {
          timeoutMs: requestBodyTimeoutMs,
          setTimeoutImpl: this.setTimeoutImpl,
          clearTimeoutImpl: this.clearTimeoutImpl,
          onChunk: (chunkBytes) => {
            totalReadBytes += chunkBytes;
            if (totalReadBytes > allocatedBytes) {
              const delta = totalReadBytes - allocatedBytes;
              if (this.activeBodyBytes + delta > maxAggregateBodyBytes) {
                throw new AggregateBodyLimitExceededError();
              }
              this.activeBodyBytes += delta;
              allocatedBytes += delta;
            }
          },
        }
      );
    } catch (bodyError) {
      // The body was not (fully) retained: release its byte reservation now.
      // The read concurrency slot is released in the `finally` below.
      releaseBodyRetention();

      if (bodyError instanceof AggregateBodyLimitExceededError) {
        const retryAfterSec = Math.max(
          1,
          Math.ceil(cooldownMs / 1000)
        );

        return Response.json(
          { error: "Too many concurrent uploads, please retry later" },
          {
            status: 503,
            headers: {
              "Retry-After": String(retryAfterSec),
            },
          }
        );
      }

      if (bodyError instanceof BodyTooLargeError) {
        return Response.json(
          { error: "Request body too large" },
          { status: 413 }
        );
      }

      if (bodyError instanceof BodyReadTimeoutError) {
        return Response.json(
          { error: "Request body read timed out" },
          { status: 408 }
        );
      }

      if (request.signal?.aborted) {
        return Response.json(
          { error: "Client cancelled" },
          { status: 499 }
        );
      }

      return Response.json(
        { error: "Failed to read request body" },
        { status: 400 }
      );
    } finally {
      // Body-read concurrency is released as soon as the read settles; only
      // the retained-byte budget is held for the body's full lifetime.
      if (hasBodyStream) {
        this.activeBodyReads = Math.max(
          0,
          this.activeBodyReads - 1
        );
      }
    }

    if (request.signal?.aborted) {
      releaseBodyRetention();

      return Response.json(
        { error: "Client cancelled" },
        { status: 499 }
      );
    }

    if (
      this.activeTicket &&
      this.waitingQueue.length >= maxQueueDepth
    ) {
      const retryAfterSec = Math.max(
        1,
        Math.ceil(cooldownMs / 1000)
      );

      releaseBodyRetention();

      return Response.json(
        { error: "Queue depth exceeded" },
        {
          status: 503,
          headers: {
            "Retry-After": String(retryAfterSec),
          },
        }
      );
    }

    const ticket = await this.acquireSlot(
      request,
      maxQueueWaitMs,
      cooldownMs
    );

    if (ticket.rejectedResponse) {
      releaseBodyRetention();

      return ticket.rejectedResponse;
    }

    if (request.signal?.aborted) {
      this.releaseSlot();
      releaseBodyRetention();

      return Response.json(
        { error: "Client cancelled" },
        { status: 499 }
      );
    }

    const upstreamAbortController = new AbortController();

    const onClientAbort = () => {
      upstreamAbortController.abort(
        request.signal?.reason ||
        new Error("Client cancelled")
      );
    };

    if (request.signal) {
      request.signal.addEventListener(
        "abort",
        onClientAbort,
        { once: true }
      );
    }

    try {
      const state =
        (await this.ctx.storage.get("rate-state")) || {
          starts: [],
          lastSendAt: 0,
          lastCompletionAt: 0,
        };

      let upstreamResponse;

      for (let retry = 0; ; retry++) {
        if (request.signal?.aborted) {
          throw new Error("Client cancelled");
        }

        await this.waitForSlot(
          state,
          rpm,
          cooldownMs,
          upstreamAbortController.signal
        );

        const attemptTime = this.now();

        state.starts = state.starts.filter(
          (time) => attemptTime - time < WINDOW_MS
        );

        state.starts.push(attemptTime);
        state.lastSendAt = attemptTime;

        await this.ctx.storage.put("rate-state", state);

        const upstreamUrl = new URL(
          url.pathname + url.search,
          NVIDIA_API
        );

        const headers = new Headers(request.headers);

        headers.set(
          "Authorization",
          `Bearer ${this.env.NVIDIA_API_KEY}`
        );

        headers.delete("host");
        headers.delete("x-proxy-key");
        headers.delete("content-length");

        let headerTimeoutId = null;
        let headerTimedOut = false;

        headerTimeoutId = setTimeout(() => {
          headerTimedOut = true;

          upstreamAbortController.abort(
            new Error("Upstream response header timeout")
          );
        }, responseHeaderTimeoutMs);

        try {
          upstreamResponse = await this.fetchImpl(
            upstreamUrl,
            {
              method: "POST",
              headers,
              body,
              redirect: "manual",
              signal: upstreamAbortController.signal,
            }
          );
        } catch (fetchError) {
          if (headerTimeoutId !== null) {
            clearTimeout(headerTimeoutId);
            headerTimeoutId = null;
          }

          state.lastCompletionAt = this.now();

          await this.ctx.storage.put("rate-state", state);

          if (headerTimedOut) {
            if (request.signal) {
              request.signal.removeEventListener(
                "abort",
                onClientAbort
              );
            }

            this.releaseSlot();

            const retryAfterSec = Math.max(
              1,
              Math.ceil(cooldownMs / 1000)
            );

            return Response.json(
              { error: "Upstream response header timeout" },
              {
                status: 504,
                headers: {
                  "Retry-After": String(retryAfterSec),
                },
              }
            );
          }

          if (request.signal?.aborted) {
            if (request.signal) {
              request.signal.removeEventListener(
                "abort",
                onClientAbort
              );
            }

            this.releaseSlot();

            return Response.json(
              { error: "Client cancelled" },
              { status: 499 }
            );
          }

          throw fetchError;
        }

        if (headerTimeoutId !== null) {
          clearTimeout(headerTimeoutId);
          headerTimeoutId = null;
        }

        if (upstreamResponse.status !== 429) {
          break;
        }

        const { bytes: retryBodyBytes, truncated: retryBodyTruncated } =
          await readBoundedResponseBody(
            upstreamResponse,
            MAX_429_BODY_BYTES,
            {
              setTimeoutImpl: this.setTimeoutImpl,
              clearTimeoutImpl: this.clearTimeoutImpl,
            }
          );

        const retryHeaders = safeResponseHeaders(
          upstreamResponse.headers
        );

        // A body that exceeded the cap (or could not be read) is replaced with a
        // deterministic synthetic JSON body; the upstream payload is never
        // returned in a truncated, possibly-corrupt form.
        const retryBody = retryBodyTruncated
          ? TRUNCATED_429_BODY
          : retryBodyBytes;

        if (retryBodyTruncated) {
          retryHeaders.set("content-type", "application/json");
        }

        state.lastCompletionAt = this.now();

        await this.ctx.storage.put("rate-state", state);

        if (retry >= MAX_429_RETRIES) {
          this.releaseSlot();

          if (request.signal) {
            request.signal.removeEventListener(
              "abort",
              onClientAbort
            );
          }

          return new Response(retryBody, {
            status: 429,
            headers: retryHeaders,
          });
        }

        const requestedRetryAfterMs = parseRetryAfterMs(
          upstreamResponse,
          this.now()
        );

        if (
          requestedRetryAfterMs !== null &&
          requestedRetryAfterMs > MAX_BACKOFF_MS
        ) {
          this.releaseSlot();

          if (request.signal) {
            request.signal.removeEventListener(
              "abort",
              onClientAbort
            );
          }

          return new Response(retryBody, {
            status: 429,
            headers: retryHeaders,
          });
        }

        const retryDelay = computeBackoffDelay(
          retry,
          upstreamResponse,
          {
            now: this.now(),
            maxBackoffMs: MAX_BACKOFF_MS,
            jitterFn: (delay) =>
              applyBackoffJitter(
                delay,
                this.randomImpl,
                MAX_BACKOFF_MS
              ),
          }
        );

        await this.sleepImpl(
          retryDelay,
          upstreamAbortController.signal
        );
      }

      const responseHeaders = safeResponseHeaders(
        upstreamResponse.headers
      );

      if (!upstreamResponse.body) {
        state.lastCompletionAt = this.now();

        await this.ctx.storage.put("rate-state", state);

        if (request.signal) {
          request.signal.removeEventListener(
            "abort",
            onClientAbort
          );
        }

        this.releaseSlot();

        return new Response(null, {
          status: upstreamResponse.status,
          statusText: upstreamResponse.statusText,
          headers: responseHeaders,
        });
      }

      const reader = upstreamResponse.body.getReader();

      let slotReleased = false;
      let cleanupDone = false;
      let idleTimer = null;
      let idleTimedOut = false;
      let controllerRef = null;
      let cancelPromise = null;

      // Status recorded when the upstream cancellation promise settles. Set
      // true when the upstream confirmed termination (cancel settled or the
      // upstream connection closed); false when the grace period elapsed
      // first (reader.cancel() never settled).
      let cancelStatus = { resolved: false, settled: false };

      const cancelUpstream = (reason) => {
        if (cancelPromise) {
          return cancelPromise;
        }

        let resolveCancel;

        cancelPromise = new Promise((resolve) => {
          resolveCancel = resolve;
        });

        if (!upstreamAbortController.signal.aborted) {
          upstreamAbortController.abort(reason);
        }

        (async () => {
          // Race the upstream reader cancellation against a bounded grace
          // period. The downstream client never waits more than the grace
          // period for its cancel() to resolve, so a never-settling
          // reader.cancel() cannot hang required cleanup indefinitely.
          //
          // `graceElapsed` records whether the grace timer already resolved
          // the downstream cancelPromise with `settled: false`. Even after the
          // grace elapses, a later real settlement of reader.cancel() upgrades
          // `cancelStatus.settled` to true and re-enters finish() so the held
          // concurrency slot is released.
          let graceElapsed = false;

          const recordSettlement = (settled) => {
            cancelStatus.resolved = true;
            cancelStatus.settled = settled;
          };

          const cancelDone = Promise.resolve(
            reader.cancel(reason)
          ).catch(() => {
            // A rejected reader.cancel() still means the upstream reader is
            // terminal (it will not be read again), so it counts as settled.
          });

          const graceTimer = this.setTimeoutImpl(() => {
            if (graceElapsed) return;
            graceElapsed = true;
            // AUDIT-02: reader.cancel() never settled within the grace
            // period. Do NOT release the concurrency slot on a timer: the
            // upstream operation may still be active, and releasing the
            // slot here would allow queued requests to overlap the still-
            // running upstream — breaking the one-request-at-a-time
            // invariant. The slot is preserved until the upstream truly
            // terminates; queued requests remain blocked until then.
            recordSettlement(false);
            resolveCancel(cancelStatus);
          }, DEFAULT_STREAM_CANCEL_GRACE_MS);

          await cancelDone;

          if (!graceElapsed) {
            this.clearTimeoutImpl(graceTimer);
            graceElapsed = true;
            recordSettlement(true);
            resolveCancel(cancelStatus);
            return;
          }

          // Grace already elapsed and resolved the downstream cancelPromise
          // with settled:false. The upstream has now confirmed termination, so
          // upgrade the status and let finish() release the held slot.
          if (!cancelStatus.settled) {
            recordSettlement(true);
            await finish();
          }
        })();

        return cancelPromise;
      };

      let onAbortStream = null;

      // Complete shutdown of the response stream. Idempotent and re-entrant:
      // the one-time terminal cleanup runs under `cleanupDone`, while the
      // queue slot is released under `slotReleased` the moment upstream
      // termination is confirmed. This split matters for AUDIT-02: when the
      // grace period elapses first the slot is intentionally held, and it must
      // still be releasable if the upstream later terminates on its own.
      const finish = async () => {
        if (cancelPromise) {
          await cancelPromise;
        }

        // Cleanup that is safe regardless of upstream confirmation: detach the
        // downstream consumer, drain pending idle timer, and drop all abort
        // listeners so a later upstream abort cannot re-enter here.
        if (!cleanupDone) {
          cleanupDone = true;

          clearIdleTimer();

          if (onAbortStream) {
            upstreamAbortController.signal.removeEventListener(
              "abort",
              onAbortStream
            );
            onAbortStream = null;
          }

          if (request.signal) {
            request.signal.removeEventListener("abort", onClientAbort);
          }

          state.lastCompletionAt = this.getNow();
          await this.ctx.storage.put("rate-state", state);
        }

        // Release the concurrency slot only if upstream termination was
        // confirmed. If reader.cancel() never settled within the grace
        // period, the slot is held so queued requests cannot be dispatched
        // while the (possibly still active) upstream connection is owned by
        // the finished request. The downstream client is already unblocked
        // because cancelPromise resolved within the grace period, and any
        // later upstream termination re-enters finish() to release the slot.
        //
        // AUDIT-02 documented limitation: if the upstream ignores
        // cancellation and never terminates the connection, this slot remains
        // held indefinitely. Queue liveness for new clients is preserved
        // because each queued request is dropped with a 503 at its
        // MAX_QUEUE_WAIT_MS, so no client waits forever; the affected
        // instance simply cannot admit a new upstream request until the
        // upstream connection stops.
        if (cancelStatus.settled && !slotReleased) {
          slotReleased = true;
          this.releaseSlot();
        }
      };

      const clearIdleTimer = () => {
        if (idleTimer !== null) {
          this.clearTimeoutImpl(idleTimer);
          idleTimer = null;
        }
      };

      const armIdleTimer = () => {
        clearIdleTimer();

        if (cleanupDone || idleTimedOut) return;

        idleTimer = this.setTimeoutImpl(async () => {
          idleTimer = null;

          if (cleanupDone || idleTimedOut) return;

          idleTimedOut = true;

          const timeoutErr = new Error("Stream idle timeout");

          try {
            controllerRef?.error(timeoutErr);
          } catch {}

          await cancelUpstream(timeoutErr);
          await finish();
        }, streamIdleTimeoutMs);
      };

      onAbortStream = async () => {
        if (cleanupDone) return;

        clearIdleTimer();

        await cancelUpstream(
          upstreamAbortController.signal.reason
        );

        try {
          controllerRef?.error(
            upstreamAbortController.signal.reason
          );
        } catch {}

        await finish();
      };

      upstreamAbortController.signal.addEventListener(
        "abort",
        onAbortStream,
        { once: true }
      );

      const stream = new ReadableStream(
        {
          start(controller) {
            controllerRef = controller;
          },

          async pull(controller) {
            armIdleTimer();

            let result;

            try {
              result = await reader.read();
            } catch (readError) {
              clearIdleTimer();

              // Upstream connection terminated (read error): positive
              // confirmation that the upstream is no longer active, so the
              // slot may be released even if reader.cancel() never settled.
              cancelStatus.resolved = true;
              cancelStatus.settled = true;

              if (idleTimedOut || cleanupDone) {
                // Already in/about to be in terminal shutdown; just allow any
                // pending slot release to run.
                await finish();
                return;
              }

              if (upstreamAbortController.signal.aborted) {
                // We aborted the upstream ourselves (client cancel or idle
                // shutdown). The downstream controller was already errored by
                // the abort path, so only surface termination and let
                // finish() release the held slot.
                await cancelUpstream(readError);
                await finish();
                return;
              }

              try {
                controller.error(readError);
              } catch {}

              await cancelUpstream(readError);
              await finish();

              return;
            }

            clearIdleTimer();

            if (idleTimedOut) {
              await finish();
              return;
            }

            if (result.done) {
              if (cancelPromise) {
                await cancelPromise;
              }

              // Upstream connection terminated normally (done): confirmation
              // that the upstream is no longer active, so the slot may be
              // released even if reader.cancel() never settled.
              cancelStatus.resolved = true;
              cancelStatus.settled = true;

              if (!cleanupDone) {
                try {
                  controller.close();
                } catch {}
              }

              await finish();
            } else {
              controller.enqueue(result.value);
            }
          },

          async cancel(reason) {
            clearIdleTimer();

            await cancelUpstream(reason);
            await finish();
          },
        },
        { highWaterMark: 0 }
      );

      return new Response(stream, {
        status: upstreamResponse.status,
        statusText: upstreamResponse.statusText,
        headers: responseHeaders,
      });
    } catch (error) {
      if (request.signal) {
        request.signal.removeEventListener(
          "abort",
          onClientAbort
        );
      }

      this.releaseSlot();

      if (request.signal?.aborted) {
        return Response.json(
          { error: "Client cancelled" },
          { status: 499 }
        );
      }

      // Diagnostic logging: never log API keys, headers, or request bodies.
      console.error("Proxy upstream request failed", {
        name: String(error?.name || "Error").slice(0, 100),
        message: String(error?.message || error).slice(0, 500),
      });

      return Response.json(
        { error: "Proxy upstream request failed" },
        { status: 502 }
      );
    } finally {
      // The buffered request body is no longer needed once dispatch has been
      // attempted to completion (or the request terminated), so its aggregate
      // byte reservation is released here — exactly once for every terminal
      // path that reaches dispatch (success, streaming, timeout, cancellation,
      // upstream error).
      releaseBodyRetention();
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (
      url.pathname === "/health" &&
      request.method === "GET"
    ) {
      // Liveness only: the Worker is running and can answer. This is
      // deliberately independent of configuration and bindings so it cannot be
      // used to probe readiness, and it never touches the upstream or consumes
      // inference quota.
      return Response.json({ ok: true });
    }

    if (
      url.pathname === "/ready" &&
      request.method === "GET"
    ) {
      // Readiness: report whether the required bindings/configuration are
      // present and usable, without disclosing any values. No upstream request
      // is made, so no inference quota is consumed.
      const checks = {
        proxy_key: typeof env.PROXY_KEY === "string" && env.PROXY_KEY.length > 0,
        upstream_key:
          typeof env.NVIDIA_API_KEY === "string" &&
          env.NVIDIA_API_KEY.length > 0,
        rate_gate: Boolean(env.RATE_GATE),
      };

      const ready = Object.values(checks).every(Boolean);

      return Response.json(
        { ready, checks },
        { status: ready ? 200 : 503 }
      );
    }

    if (!env.PROXY_KEY || !env.NVIDIA_API_KEY) {
      return new Response("Server configuration error", {
        status: 500,
      });
    }

    const authorization =
      request.headers.get("Authorization") || "";

    const suppliedKey = authorization.startsWith("Bearer ")
      ? authorization.slice(7)
      : request.headers.get("x-proxy-key") || "";

    if (suppliedKey !== env.PROXY_KEY) {
      return new Response("Unauthorized", { status: 401 });
    }

    if (
      request.method !== "POST" ||
      !url.pathname.startsWith("/v1/")
    ) {
      return new Response("Not found", { status: 404 });
    }

    const id = env.RATE_GATE.idFromName(
      "nvidia-global-queue"
    );

    return env.RATE_GATE.get(id).fetch(request);
  },
};
