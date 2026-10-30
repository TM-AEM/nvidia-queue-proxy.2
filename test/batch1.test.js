import test from "node:test";
import assert from "node:assert/strict";
import { register } from "node:module";

// Mock cloudflare:workers for Node environment
register(
  "data:text/javascript," +
    encodeURIComponent(`
export async function resolve(specifier, context, nextResolve) {
  if (specifier === "cloudflare:workers") {
    return {
      url: "data:text/javascript," + encodeURIComponent("export class DurableObject { constructor(ctx, env) { this.ctx = ctx; this.env = env; } }"),
      shortCircuit: true
    };
  }
  return nextResolve(specifier, context);
}
`),
  import.meta.url
);

const {
  RateGate,
  retryAfterMs,
  parseRetryAfterMs,
  computeBackoffDelay,
  applyBackoffJitter,
  safeResponseHeaders,
  readConfigValue,
  resolveConfig,
  ConfigError,
  readBoundedBody,
  BodyTooLargeError,
  BodyReadTimeoutError,
  AggregateBodyLimitExceededError,
  timingSafeStringEqual,
  default: worker,
} = await import("../src/index.js");

// ==========================================
// Platform-function receiver regression (Illegal invocation)
// ==========================================
// The default fetch path must invoke the global fetch without a receiver. The
// spy below is a strict-mode function that records its `this`, so a regression
// to the old `this.fetchImpl(...)` pattern (receiver = RateGate) is detected as
// `this === the RateGate instance` instead of `undefined`.
let fetchReceiver = "not-called";
function strictFetchSpy(...args) {
  fetchReceiver =
    this === undefined ? "undefined" : this === globalThis ? "globalThis" : "object";
  return Promise.resolve(new Response("spy", { status: 200 }));
}

function makeDefaultFetchEnv(extra = {}) {
  return {
    PROXY_KEY: "test-proxy-key",
    NVIDIA_API_KEY: "test-nvidia-api-key",
    COOLDOWN_MS: "0",
    RPM: "1000",
    // No __fetch: exercise the default (global fetch) path.
    __now: () => Date.now(),
    __sleep: () => Promise.resolve(),
    __setTimeout: (fn, ms) => setTimeout(fn, ms),
    __clearTimeout: (id) => clearTimeout(id),
    ...extra,
  };
}

function createMockCtx() {
  const store = new Map();
  return {
    storage: {
      store,
      async get(key) {
        if (!store.has(key)) return undefined;
        return JSON.parse(JSON.stringify(store.get(key)));
      },
      async put(key, value) {
        store.set(key, JSON.parse(JSON.stringify(value)));
      },
    },
  };
}

class FakeClock {
  constructor(initialTime = 1_000_000) {
    this.currentTime = initialTime;
    this.sleepCalls = [];
  }

  now() {
    return this.currentTime;
  }

  advance(ms) {
    this.currentTime += ms;
  }

  sleep(ms, signal) {
    this.sleepCalls.push({ ms, time: this.currentTime });
    this.currentTime += ms;
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        return reject(signal.reason || new Error("Aborted"));
      }
      resolve();
    });
  }
}

// Deterministic virtual timer. Only timers created through these functions are
// tracked; they fire exclusively when advance() is called, so tests never rely
// on real wall-clock sleeps. This isolates the stream idle timer from the
// queue-wait and header timers, which continue to use the real event loop.
function createFakeTimers() {
  let now = 0;
  let seq = 0;
  const timers = new Map();
  return {
    setTimeout(fn, ms) {
      const id = ++seq;
      timers.set(id, { fn, at: now + ms });
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
    advance(ms) {
      now += ms;
      for (const [id, timer] of [...timers]) {
        if (timer.at <= now) {
          timers.delete(id);
          timer.fn();
        }
      }
    },
    pendingCount() {
      return timers.size;
    },
  };
}

function tick() {
  return new Promise((resolve) => setImmediate(resolve));
}

// A controllable upstream: chunks are enqueued only when pushed, so tests can
// choose between a ready upstream (no timeout) and a stalled upstream (timeout)
// with full determinism.
function createControllableUpstream() {
  const pending = [];
  const buffered = [];
  let cancelled = false;
  const state = {
    cancelled: false,
    push(chunk) {
      if (pending.length > 0) {
        pending.shift().enqueue(chunk);
      } else {
        buffered.push(chunk);
      }
    },
    get cancelled() {
      return cancelled;
    },
    get pendingCount() {
      return pending.length;
    },
  };
  const stream = new ReadableStream(
    {
      pull(controller) {
        if (buffered.length > 0) {
          controller.enqueue(buffered.shift());
        } else {
          pending.push(controller);
        }
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 }
  );
  return { stream, state };
}

// A controllable request-body source for timeout/cancellation tests. Chunks are
// enqueued only when pushed, and cancel() behavior is configurable so tests can
// exercise a cooperative or a never-settling cancellation.
function createControllableBody(options = {}) {
  const { cancelNeverSettles = false, onCancel = null } = options;
  const pending = [];
  const buffered = [];
  const state = {
    cancelCalled: false,
    push(chunk) {
      if (pending.length > 0) {
        pending.shift().enqueue(chunk);
      } else {
        buffered.push(chunk);
      }
    },
    close() {
      if (pending.length > 0) {
        pending.shift().close();
      } else {
        buffered.push(null);
      }
    },
    get pendingCount() {
      return pending.length;
    },
  };
  const stream = new ReadableStream(
    {
      pull(controller) {
        if (buffered.length > 0) {
          const next = buffered.shift();
          if (next === null) {
            controller.close();
          } else {
            controller.enqueue(next);
          }
        } else {
          pending.push(controller);
        }
      },
      cancel(reason) {
        state.cancelCalled = true;
        if (onCancel) onCancel(reason);
        if (cancelNeverSettles) {
          return new Promise(() => {});
        }
        return undefined;
      },
    },
    { highWaterMark: 0 }
  );
  return { stream, state };
}

// A manual body source whose reader is fully controlled by the test. Unlike a
// ReadableStream, cancel() does not close the reader, so a test can resolve a
// "late" read after a timeout to prove it cannot re-enter the queue.
function createManualBody() {
  const reads = [];
  const state = { cancelled: false };
  const reader = {
    read() {
      return new Promise((resolve) => {
        reads.push(resolve);
      });
    },
    cancel() {
      state.cancelled = true;
      return Promise.resolve();
    },
  };
  return {
    body: { getReader: () => reader },
    state,
    resolveRead(value) {
      const resolve = reads.shift();
      if (resolve) resolve({ value, done: false });
    },
    closeRead() {
      const resolve = reads.shift();
      if (resolve) resolve({ value: undefined, done: true });
    },
    get pendingReads() {
      return reads.length;
    },
  };
}

// A fully instrumented body source. Counts how many reader.read() calls were
// initiated, lets the test resolve/reject each pending read on demand, and can
// simulate a cancel() that never settles (a source that ignores cancellation).
function createTrackedBody(options = {}) {
  const { cancelNeverSettles = false } = options;
  const reads = [];
  const state = {
    cancelCalled: false,
    cancelNeverSettles,
    readsStarted: 0,
    readsResolved: 0,
  };
  const reader = {
    read() {
      state.readsStarted += 1;
      return new Promise((resolve, reject) => {
        reads.push({ resolve, reject });
      });
    },
    cancel() {
      state.cancelCalled = true;
      if (cancelNeverSettles) return new Promise(() => {});
      return Promise.resolve();
    },
  };
  return {
    body: { getReader: () => reader },
    state,
    resolveNext(value) {
      const entry = reads.shift();
      if (!entry) return false;
      state.readsResolved += 1;
      entry.resolve({ value, done: false });
      return true;
    },
    closeNext() {
      const entry = reads.shift();
      if (!entry) return false;
      state.readsResolved += 1;
      entry.resolve({ value: undefined, done: true });
      return true;
    },
    rejectNext(err) {
      const entry = reads.shift();
      if (!entry) return false;
      state.readsResolved += 1;
      entry.reject(err);
      return true;
    },
    get pendingReads() {
      return reads.length;
    },
  };
}

// ==========================================
// T2 TESTS — Retry-After & Exponential Backoff
// ==========================================

test("T2: retryAfterMs parses absent, numeric, date-form, and malformed values", () => {
  const baseTime = 1_000_000;

  // Absent header
  assert.equal(retryAfterMs(new Response("ok"), baseTime), 0);
  assert.equal(retryAfterMs(null, baseTime), 0);

  // Numeric values
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": "0" } }), baseTime),
    0
  );
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": "5" } }), baseTime),
    5000
  );
  // Fractional numeric value -> invalid per RFC 7231 -> 0 (fall back to backoff)
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": "1.5" } }), baseTime),
    0
  );
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": " 10  " } }), baseTime),
    10000
  );

  // Negative numeric value -> invalid -> 0
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": "-5" } }), baseTime),
    0
  );

  // Malformed strings
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": "invalid-str" } }), baseTime),
    0
  );
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": "NaN" } }), baseTime),
    0
  );

  // HTTP Date form
  const futureDate = new Date(baseTime + 12_000).toUTCString();
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": futureDate } }), baseTime),
    12000
  );

  const pastDate = new Date(baseTime - 5_000).toUTCString();
  assert.equal(
    retryAfterMs(new Response("ok", { headers: { "Retry-After": pastDate } }), baseTime),
    0
  );
});

test("T2: computeBackoffDelay respects larger of exponential delay and Retry-After, capped at 30s", () => {
  const now = 100_000;

  // retryIndex 0: exp = 1000ms. If Retry-After absent -> 1000ms
  assert.equal(computeBackoffDelay(0, null, { now }), 1000);

  // retryIndex 0: exp = 1000ms. If Retry-After is 5000ms -> 5000ms
  const resp5s = new Response("429", { headers: { "Retry-After": "5" } });
  assert.equal(computeBackoffDelay(0, resp5s, { now }), 5000);

  // retryIndex 2: exp = 4000ms. If Retry-After is 2000ms -> 4000ms (exp is larger)
  const resp2s = new Response("429", { headers: { "Retry-After": "2" } });
  assert.equal(computeBackoffDelay(2, resp2s, { now }), 4000);

  // retryIndex 3: exp = 8000ms. If Retry-After is 20s -> 20000ms
  const resp20s = new Response("429", { headers: { "Retry-After": "20" } });
  assert.equal(computeBackoffDelay(3, resp20s, { now }), 20000);

  // 30s cap test: Retry-After is 45s -> capped at 30000ms
  const resp45s = new Response("429", { headers: { "Retry-After": "45" } });
  assert.equal(computeBackoffDelay(0, resp45s, { now }), 30000);

  // 30s cap test: retryIndex 5 (exp = 32000ms) -> capped at 30000ms
  assert.equal(computeBackoffDelay(5, null, { now }), 30000);

  // Jitter support
  const withJitter = computeBackoffDelay(0, resp5s, {
    now,
    jitterFn: (d) => d + 42,
  });
  assert.equal(withJitter, 5042);
});

test("T2: RateGate retries at most 5 times on 429 and returns final 429 when exhausted", async () => {
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  let attemptCount = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      attemptCount++;
      return new Response(JSON.stringify({ error: "rate limit exceeded" }), {
        status: 429,
        headers: {
          "Retry-After": "2",
          "content-type": "application/json",
          "x-custom-upstream": "test-val",
        },
      });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: JSON.stringify({ message: "hi" }),
  });

  const res = await rateGate.fetch(req);

  // 1 initial attempt + 5 retries = 6 attempts total
  assert.equal(attemptCount, 6, "Must make initial attempt plus 5 retries (6 total)");
  assert.equal(res.status, 429);
  assert.equal(res.headers.get("x-custom-upstream"), "test-val");
  const body = await res.json();
  assert.equal(body.error, "rate limit exceeded");
});

test("T2: RateGate does not retry unrelated HTTP statuses", async () => {
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  let attemptCount = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      attemptCount++;
      return new Response("Bad request", { status: 400 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: "{}",
  });

  const res = await rateGate.fetch(req);
  assert.equal(attemptCount, 1, "Must not retry HTTP 400");
  assert.equal(res.status, 400);
  await res.text();
});

// ==========================================
// T1 TESTS — Attempt Accounting & Sliding Window
// ==========================================

test("T1: Counts every upstream attempt including 429 retries and network exceptions", async () => {
  const clock = new FakeClock(10_000);
  const ctx = createMockCtx();
  let attempt = 0;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      attempt++;
      if (attempt === 1) {
        // Attempt 1: 429
        return new Response("Too Many Requests", {
          status: 429,
          headers: { "Retry-After": "1" },
        });
      }
      if (attempt === 2) {
        // Attempt 2: Network failure
        throw new Error("Connection reset by peer");
      }
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: "{}",
  });

  const res = await rateGate.fetch(req);
  assert.equal(res.status, 502);

  // Inspect persisted state
  const state = await ctx.storage.get("rate-state");
  assert.ok(state, "State must be persisted in DO storage");
  assert.equal(
    state.starts.length,
    2,
    "Both the 429 attempt and the network failure attempt must be in starts"
  );
  assert.equal(attempt, 2);
});

test("T1: Records attempt timestamp immediately before dispatching upstream", async () => {
  const clock = new FakeClock(50_000);
  const ctx = createMockCtx();
  let recordedStartsBeforeHeaders = null;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      // In the middle of fetch, before returning headers, check what is in storage
      const currentState = await ctx.storage.get("rate-state");
      recordedStartsBeforeHeaders = [...currentState.starts];
      clock.advance(500); // simulate fetch duration
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: "{}",
  });

  const res = await rateGate.fetch(req);
  await res.text();
  assert.ok(recordedStartsBeforeHeaders, "Attempt must be saved before headers arrive");
  assert.equal(recordedStartsBeforeHeaders.length, 1);
  assert.equal(recordedStartsBeforeHeaders[0], 50_000);
});

test("T1: Sliding window 60-second exact boundary behavior", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  // Populate state with 2 requests at t = 1000 and t = 2000
  await ctx.storage.put("rate-state", {
    starts: [1000, 2000],
    lastSendAt: 2000,
    lastCompletionAt: 2500,
  });

  let dispatchedTime = 0;
  const env = {
    RPM: 2, // Limit is 2 requests per 60s
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatchedTime = clock.now();
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Set clock to t = 5000: both [1000, 2000] are within 60s.
  clock.currentTime = 5000;

  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: "{}",
  });

  const res = await rateGate.fetch(req);
  await res.text();

  // The oldest entry at 1000 expires at 1000 + 60,000 = 61,000.
  // So dispatch must have waited until 61,000!
  assert.equal(dispatchedTime, 61_000, "Must wait exactly until oldest attempt expires at 61,000");

  const finalState = await ctx.storage.get("rate-state");
  // At t = 61,000, attempt at 1000 expired (61000 - 1000 = 60000 >= 60000).
  // Remaining were [2000], plus new attempt at [61000]
  assert.deepEqual(finalState.starts, [2000, 61000]);
});

test("T1: Preserves completion-based cooldown behavior", async () => {
  const clock = new FakeClock(10_000);
  const ctx = createMockCtx();

  // Previous request completed at t = 10,000. Cooldown is 1670ms.
  await ctx.storage.put("rate-state", {
    starts: [8000],
    lastSendAt: 8000,
    lastCompletionAt: 10_000,
  });

  let dispatchedTime = 0;
  const env = {
    RPM: 40,
    COOLDOWN_MS: 1670,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatchedTime = clock.now();
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  // Current time is 10,500 (500ms after completion; 1170ms left in cooldown)
  clock.currentTime = 10_500;

  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: "{}",
  });

  const res = await rateGate.fetch(req);
  await res.text();
  assert.equal(dispatchedTime, 11_670, "Must wait for 1670ms completion cooldown (10000 + 1670)");
});

// ==========================================
// T3 TESTS — Timeouts, Queue Bounds, Cancellation
// ==========================================

test("T3: Maximum queue depth rejection with 503 and Retry-After", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  let unblockFirst;
  const firstRunning = new Promise((resolve) => {
    unblockFirst = resolve;
  });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 2000,
    MAX_QUEUE_DEPTH: 2,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      await firstRunning;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1: becomes active
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  // Wait a tick for request 1 to acquire slot
  await new Promise((r) => setTimeout(r, 10));

  // Request 2 & 3: queue depth is 2, so they enter the queue
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );
  const p3 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "3",
    })
  );

  // Request 4: queue depth is already 2 -> rejected immediately with 503!
  const res4 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "4",
    })
  );

  assert.equal(res4.status, 503);
  assert.equal(res4.headers.get("Retry-After"), "2");
  const data4 = await res4.json();
  assert.equal(data4.error, "Queue depth exceeded");

  // Clean up remaining by unblocking first and consuming streams sequentially
  unblockFirst();
  const r1 = await p1;
  await r1.text();
  const r2 = await p2;
  await r2.text();
  const r3 = await p3;
  await r3.text();
});

test("T3: Maximum queue wait timeout rejects with 503 and Retry-After without blocking subsequent requests", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  let unblockFirst;
  const firstBlock = new Promise((resolve) => {
    unblockFirst = resolve;
  });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 1500,
    MAX_QUEUE_WAIT_MS: 50, // Short wait timeout for testing
    MAX_QUEUE_DEPTH: 10,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      await firstBlock;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1 blocks slot
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  await new Promise((r) => setTimeout(r, 10));

  // Request 2 enters queue
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );

  // Wait so Request 2 hits MAX_QUEUE_WAIT_MS (50ms)
  const res2 = await p2;
  assert.equal(res2.status, 503);
  assert.equal(res2.headers.get("Retry-After"), "2");
  const data2 = await res2.json();
  assert.equal(data2.error, "Maximum queue wait exceeded");

  // Now unblock Request 1 and consume it
  unblockFirst();
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  // Request 3 arrives after Request 2 timed out — must not deadlock!
  const res3 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "3",
    })
  );
  assert.equal(res3.status, 200);
  await res3.text();
});

test("T3: Client cancellation in queue removes request without deadlocking queue", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  let unblockFirst;
  const firstBlock = new Promise((resolve) => {
    unblockFirst = resolve;
  });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      await firstBlock;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1 holds slot
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  await new Promise((r) => setTimeout(r, 10));

  // Request 2 waits in queue with an AbortController
  const abortCtrl2 = new AbortController();
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
      signal: abortCtrl2.signal,
    })
  );

  // Request 3 also waits in queue
  const p3 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "3",
    })
  );

  // Abort request 2 while in queue
  abortCtrl2.abort();
  const res2 = await p2;
  assert.equal(res2.status, 499);

  // Unblock request 1
  unblockFirst();
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  // Request 3 must proceed smoothly and not deadlock!
  const res3 = await p3;
  assert.equal(res3.status, 200);
  await res3.text();
});

test("T3: Upstream response header timeout aborts fetch and returns 504", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let upstreamSignalAborted = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: (url, opts) => {
      return new Promise((resolve, reject) => {
        opts.signal.addEventListener("abort", () => {
          upstreamSignalAborted = true;
          reject(new DOMException("The operation was aborted", "AbortError"));
        });
      });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );

  assert.equal(res.status, 504);
  assert.ok(upstreamSignalAborted, "Upstream fetch must have received abort signal");
});

test("T3: Slot is not released until previous upstream operation confirms stopped", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  let firstCalled = false;
  let op1Active = false;
  let op2StartedWhileOp1Active = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 50,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      if (!firstCalled) {
        firstCalled = true;
        op1Active = true;
        return new Promise((resolve, reject) => {
          opts.signal.addEventListener("abort", async () => {
            // Simulate delay for upstream connection shutdown
            await new Promise((r) => setTimeout(r, 20));
            op1Active = false;
            reject(new DOMException("Aborted", "AbortError"));
          });
        });
      } else {
        if (op1Active) {
          op2StartedWhileOp1Active = true;
        }
        return new Response("ok", { status: 200 });
      }
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1 will hit response header timeout (50ms)
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  // Request 2 waits in queue
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );

  const [res1, res2] = await Promise.all([p1, p2]);
  assert.equal(res1.status, 504);
  assert.equal(res2.status, 200);
  await res2.text();
  assert.equal(
    op2StartedWhileOp1Active,
    false,
    "Second request must never start while first request is active"
  );
});

test("T3: Stream idle timeout aborts upstream and unblocks queue", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let upstreamReaderCancelled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    STREAM_IDLE_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      const stream = new ReadableStream({
        async pull(controller) {
          await new Promise((resolve, reject) => {
            opts.signal.addEventListener("abort", () => {
              upstreamReaderCancelled = true;
              reject(opts.signal.reason);
            });
          });
        },
        cancel() {
          upstreamReaderCancelled = true;
        },
      });
      return new Response(stream, { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  assert.equal(res1.status, 200);
  const reader = res1.body.getReader();

  // Read should reject when idle timeout fires
  await assert.rejects(async () => {
    await reader.read();
  });

  assert.ok(upstreamReaderCancelled, "Upstream stream reader must be cancelled on idle timeout");

  // Verify next request can execute without deadlock
  let secondRan = false;
  rateGate.fetchImpl = async () => {
    secondRan = true;
    return new Response("ok", { status: 200 });
  };

  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );
  assert.equal(res2.status, 200);
  await res2.text();
  assert.ok(secondRan, "Queue must not deadlock after stream idle timeout");
});

test("T3: Client cancellation during streaming aborts upstream and releases queue", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let upstreamCancelled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      const stream = new ReadableStream({
        pull(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-1"));
        },
        cancel() {
          upstreamCancelled = true;
        },
      });
      return new Response(stream, { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  assert.equal(res1.status, 200);
  const reader = res1.body.getReader();
  const chunk1 = await reader.read();
  assert.equal(new TextDecoder().decode(chunk1.value), "chunk-1");

  // Client cancels reading stream
  await reader.cancel("Client navigated away");
  assert.ok(upstreamCancelled, "Cancellation must propagate to upstream");

  // Queue must be open for next request
  rateGate.fetchImpl = async () => new Response("ok-2", { status: 200 });
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );
  assert.equal(res2.status, 200);
  await res2.text();
});

test("T1/T3: Strict FIFO ordering under concurrent requests", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const executionOrder = [];

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      const id = opts.headers.get("x-req-id");
      executionOrder.push(id);
      await new Promise((r) => setTimeout(r, 5));
      return new Response(JSON.stringify({ id }), { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  const reqPromises = [1, 2, 3, 4, 5].map(async (id) => {
    const res = await rateGate.fetch(
      new Request("https://proxy.internal/v1/chat/completions", {
        method: "POST",
        headers: { "x-req-id": String(id) },
        body: JSON.stringify({ id }),
      })
    );
    await res.text();
  });

  await Promise.all(reqPromises);
  assert.deepEqual(
    executionOrder,
    ["1", "2", "3", "4", "5"],
    "Requests must execute in strict FIFO order"
  );
});


// ==========================================
// BATCH 1F REGRESSION TESTS (F1 - F5)
// ==========================================

test("F1: Stalled upstream read times out and follows slot-safe cancellation", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const upstream = createControllableUpstream();
  let upstreamSignalAborted = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    STREAM_IDLE_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async (url, opts) => {
      opts.signal.addEventListener("abort", () => {
        upstreamSignalAborted = true;
      });
      return new Response(upstream.stream, { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  assert.equal(res1.status, 200);

  const reader = res1.body.getReader();
  // First upstream chunk arrives.
  upstream.state.push(new TextEncoder().encode("chunk-1"));
  const first = await reader.read();
  assert.equal(new TextDecoder().decode(first.value), "chunk-1");

  // No read-ahead while the consumer is idle.
  await tick();
  assert.equal(upstream.state.pendingCount, 0, "Proxy must not read ahead while idle");
  assert.equal(timers.pendingCount(), 0, "No idle timer while no upstream read is pending");

  // The consumer asks for the next chunk; the upstream stalls forever.
  const pendingRead = reader.read();
  await tick();
  assert.equal(upstream.state.pendingCount, 1, "Upstream read must be pending");
  assert.equal(timers.pendingCount(), 1, "Idle timer must be armed while upstream read is pending");

  // Fire the idle timeout.
  timers.advance(40);

  await assert.rejects(
    () => pendingRead,
    (err) => err.message.includes("Stream idle timeout")
  );
  assert.equal(upstream.state.cancelled, true, "Upstream stream must be cancelled on idle timeout");
  assert.equal(upstreamSignalAborted, true, "Upstream fetch signal must be aborted on idle timeout");

  // Queue slot must be released so the next request can run without deadlock.
  rateGate.fetchImpl = async () => new Response("unblocked", { status: 200 });
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  assert.equal(res2.status, 200);
  assert.equal(await res2.text(), "unblocked");
});

test("F1: Paused downstream consumer does not trigger the idle timeout", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const upstream = createControllableUpstream();

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    STREAM_IDLE_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => new Response(upstream.stream, { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);
  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  assert.equal(res1.status, 200);

  const reader = res1.body.getReader();
  // Upstream delivers the first chunk and the downstream consumes it.
  upstream.state.push(new TextEncoder().encode("chunk-1"));
  const first = await reader.read();
  assert.equal(new TextDecoder().decode(first.value), "chunk-1");

  // The downstream now pauses. The upstream has nothing more ready and is
  // therefore stalled, but the proxy must not arm the idle timer until the
  // consumer actually requests the next chunk.
  await tick();
  assert.equal(timers.pendingCount(), 0, "No idle timer may run while downstream is paused");
  assert.equal(upstream.state.pendingCount, 0, "Proxy must not read ahead while downstream is paused");

  // Pausing longer than the idle timeout must not cancel the stream.
  timers.advance(1_000);
  await tick();
  assert.equal(upstream.state.cancelled, false, "Paused downstream must not cancel the stream");
  assert.equal(timers.pendingCount(), 0);

  // Only when the consumer resumes does the timer arm for the next upstream read.
  const pendingRead = reader.read();
  await tick();
  assert.equal(upstream.state.pendingCount, 1, "Resuming must issue the next upstream read");
  assert.equal(timers.pendingCount(), 1, "Timer must be armed while the resumed read is pending");

  // A genuinely stalled upstream still times out after the consumer resumes.
  timers.advance(40);
  await assert.rejects(
    () => pendingRead,
    (err) => err.message.includes("Stream idle timeout")
  );
  assert.equal(upstream.state.cancelled, true, "Stalled upstream must still time out after resume");
});

test("F2: Delayed incoming request body read does not hold exclusive queue slot", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const executionLog = [];

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      const tag = opts.headers.get("x-tag");
      executionLog.push(`dispatched-${tag}`);
      return new Response(JSON.stringify({ ok: true, tag }), { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1: Slow incoming body (takes 60ms to read)
  const slowStream = new ReadableStream({
    async start(controller) {
      await new Promise((r) => setTimeout(r, 60));
      controller.enqueue(new TextEncoder().encode("slow body chunk"));
      controller.close();
    },
  });

  const slowReq = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "x-tag": "slow", "content-type": "text/plain" },
    body: slowStream,
    duplex: "half",
  });

  const pSlow = rateGate.fetch(slowReq);

  // Request 2 arrives 10ms later with an instant body
  await new Promise((r) => setTimeout(r, 10));

  const fastReq = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "x-tag": "fast" },
    body: "instant",
  });

  const resFast = await rateGate.fetch(fastReq);
  assert.equal(resFast.status, 200);
  await resFast.text();

  const resSlow = await pSlow;
  assert.equal(resSlow.status, 200);
  await resSlow.text();

  // Fast request must have dispatched BEFORE slow request because slow request
  // did not acquire the slot while reading its incoming body!
  assert.deepEqual(
    executionLog,
    ["dispatched-fast", "dispatched-slow"],
    "Fast request must acquire slot and dispatch before slow request completes its body read"
  );
});

test("F3: Non-settling upstream cancellation fails closed and prevents overlapping requests", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let op2Dispatched = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 50, // Queue wait limit
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      const tag = opts.headers.get("x-tag");
      if (tag === "op1") {
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("chunk-1"));
          },
          cancel() {
            // Hanging cancellation promise that never settles!
            return new Promise(() => {});
          },
        });
        return new Response(stream, { status: 200 });
      }
      if (tag === "op2") {
        op2Dispatched = true;
        return new Response("ok-2", { status: 200 });
      }
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1 starts streaming
  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-tag": "op1" },
      body: "{}",
    })
  );

  const reader1 = res1.body.getReader();
  await reader1.read();

  // Client cancels stream -> triggers cancel() on upstream which never settles
  reader1.cancel("client abort");

  // Request 2 arrives and waits in queue
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-tag": "op2" },
      body: "{}",
    })
  );

  // Request 2 must NOT be dispatched while upstream cancellation is unsettled (fail-closed!)
  assert.equal(
    op2Dispatched,
    false,
    "Op 2 must NEVER be dispatched to upstream while Op 1 cancellation is unsettled"
  );
  assert.equal(
    res2.status,
    503,
    "Request 2 must time out in queue with 503 rather than sending overlapping request"
  );
});

test("F3: Upstream cancellation shutdown delay prevents overlapping dispatch to next request", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let op1Active = false;
  let overlapDetected = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      const tag = opts.headers.get("x-tag");
      if (tag === "op1") {
        op1Active = true;
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("chunk-1"));
          },
          async cancel() {
            // Upstream takes 30ms to cleanly terminate connection
            await new Promise((r) => setTimeout(r, 30));
            op1Active = false;
          },
        });
        return new Response(stream, { status: 200 });
      } else {
        if (op1Active) {
          overlapDetected = true;
        }
        return new Response("ok-2", { status: 200 });
      }
    },
  };

  const rateGate = new RateGate(ctx, env);

  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-tag": "op1" },
      body: "{}",
    })
  );

  const reader1 = res1.body.getReader();
  await reader1.read();

  // Trigger cancel on op1
  const cancelPromise = reader1.cancel();

  // Concurrently request op2
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-tag": "op2" },
      body: "{}",
    })
  );

  const [_, res2] = await Promise.all([cancelPromise, p2]);
  assert.equal(res2.status, 200);
  await res2.text();
  assert.equal(
    overlapDetected,
    false,
    "Op 2 must not dispatch while Op 1 cancellation shutdown is in progress"
  );
});

test("F4: MAX_QUEUE_DEPTH = 0 allows 1 active request and rejects all waiting requests with 503", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let unblockFirst;
  const firstBlock = new Promise((resolve) => {
    unblockFirst = resolve;
  });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 1500,
    MAX_QUEUE_DEPTH: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      await firstBlock;
      return new Response("ok-1", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1: active request
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
    })
  );

  // Yield to allow Request 1 to become active
  await new Promise((r) => setTimeout(r, 10));

  // Request 2 arrives while Request 1 is active: queue depth is 0 -> immediately 503
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );

  assert.equal(res2.status, 503);
  assert.equal(res2.headers.get("Retry-After"), "2");
  const data2 = await res2.json();
  assert.equal(data2.error, "Queue depth exceeded");

  // Unblock and complete request 1
  unblockFirst();
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  // Request 3 arrives when queue is idle: must succeed with 200
  rateGate.fetchImpl = async () => new Response("ok-3", { status: 200 });
  const res3 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "3",
    })
  );
  assert.equal(res3.status, 200);
  assert.equal(await res3.text(), "ok-3");
});

test("F4: MAX_QUEUE_DEPTH = 1 allows 1 active and 1 waiting, rejecting 3rd request with 503", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let unblockFirst;
  const firstBlock = new Promise((resolve) => {
    unblockFirst = resolve;
  });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 1500,
    MAX_QUEUE_DEPTH: 1,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      const id = opts.headers.get("x-id");
      if (id === "1") {
        await firstBlock;
      }
      return new Response(`ok-${id}`, { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1: becomes active
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-id": "1" },
      body: "1",
    })
  );
  await new Promise((r) => setTimeout(r, 10));

  // Request 2: admitted into backlog queue (depth 1)
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-id": "2" },
      body: "2",
    })
  );

  // Request 3: backlog full -> rejected immediately with 503
  const res3 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-id": "3" },
      body: "3",
    })
  );

  assert.equal(res3.status, 503);
  assert.equal(res3.headers.get("Retry-After"), "2");

  // Unblock request 1
  unblockFirst();
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  const res2 = await p2;
  assert.equal(res2.status, 200);
  await res2.text();
});

test("F5: Sliding window exact 60,999ms vs 61,000ms boundary behavior", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  // Populate state with 1 request at t = 1,000ms
  await ctx.storage.put("rate-state", {
    starts: [1000],
    lastSendAt: 1000,
    lastCompletionAt: 1500,
  });

  const env = {
    RPM: 1, // Limit: 1 request per 60,000ms
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  // Case A: At t = 60,999ms (59,999ms elapsed since t = 1,000ms)
  // Window has 1ms left! Must sleep for 1ms.
  clock.currentTime = 60_999;
  clock.sleepCalls = [];

  const resA = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  await resA.text();

  assert.equal(clock.sleepCalls.length, 1, "Must sleep when at 60,999ms (1ms before expiration)");
  assert.equal(clock.sleepCalls[0].ms, 1, "Must wait exactly 1ms to reach 61,000ms window boundary");

  // Reset state for Case B: 1 request at t = 61,000ms
  await ctx.storage.put("rate-state", {
    starts: [61_000],
    lastSendAt: 61_000,
    lastCompletionAt: 61_500,
  });

  // Case B: At t = 121,000ms (exactly 60,000ms elapsed since t = 61,000ms)
  // Window has expired (121,000 - 61,000 >= 60,000). Must not sleep!
  clock.currentTime = 121_000;
  clock.sleepCalls = [];

  const resB = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  await resB.text();

  assert.equal(clock.sleepCalls.length, 0, "Must not sleep when at exactly 61,000ms after start (expired)");
});

test("F5: Client request.signal abort during active streaming propagates upstream and releases slot", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let upstreamCancelled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      const stream = new ReadableStream({
        pull(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-data"));
        },
        cancel() {
          upstreamCancelled = true;
        },
      });
      return new Response(stream, { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const abortCtrl = new AbortController();

  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
      signal: abortCtrl.signal,
    })
  );

  assert.equal(res1.status, 200);
  const reader = res1.body.getReader();
  const chunk = await reader.read();
  assert.equal(new TextDecoder().decode(chunk.value), "chunk-data");

  // Abort client request signal
  abortCtrl.abort();

  // Allow abort handler to execute
  await new Promise((r) => setTimeout(r, 20));
  assert.ok(upstreamCancelled, "Client request abort must propagate to upstream stream");

  // Next request must execute cleanly
  rateGate.fetchImpl = async () => new Response("ok-2", { status: 200 });
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );
  assert.equal(res2.status, 200);
  assert.equal(await res2.text(), "ok-2");
});


// ==========================================
// F2 REGRESSION TESTS — Bounded request bodies
// ==========================================

function bytes(...values) {
  return new Uint8Array(values);
}

test("F2: Body below the limit is forwarded byte-for-byte", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let receivedBody = null;
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 16,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      receivedBody = opts.body;
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const payload = bytes(1, 2, 3, 4, 5, 6, 7, 8);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: payload,
    })
  );
  assert.equal(res.status, 200);
  await res.text();
  assert.deepEqual(
    Array.from(receivedBody),
    [1, 2, 3, 4, 5, 6, 7, 8],
    "Exact request bytes must be forwarded unchanged"
  );
});

test("F2: Body exactly at the limit is accepted, one byte over is rejected", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: "8",
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };
  const rateGate = new RateGate(ctx, env);

  const atLimit = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(1, 2, 3, 4, 5, 6, 7, 8),
    })
  );
  assert.equal(atLimit.status, 200, "Body exactly at the limit must be accepted");
  await atLimit.text();

  const overLimit = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(1, 2, 3, 4, 5, 6, 7, 8, 9),
    })
  );
  assert.equal(overLimit.status, 413, "Body one byte over the limit must be rejected");
});

test("F2: Declared Content-Length above the limit is rejected before body consumption", async () => {
  // Direct proof: readBoundedBody must short-circuit on the length header and
  // never touch the body stream.
  let bodyRead = false;
  const fakeRequest = {
    headers: { get: (name) => (name === "content-length" ? "1000" : null) },
    body: new ReadableStream(
      {
        pull(controller) {
          bodyRead = true;
          controller.enqueue(bytes(1));
          controller.close();
        },
      },
      { highWaterMark: 0 }
    ),
  };
  await assert.rejects(() => readBoundedBody(fakeRequest, 16), BodyTooLargeError);
  assert.equal(bodyRead, false, "Body stream must not be read when Content-Length exceeds the limit");

  // Integration: the endpoint returns 413 and never dispatches upstream.
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let upstreamCalled = false;
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 16,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      upstreamCalled = true;
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "1000" },
      body: bytes(1),
    })
  );
  assert.equal(res.status, 413);
  assert.equal(upstreamCalled, false, "Upstream must not be called for oversized requests");
});

test("F2: Actual body over the limit is rejected even without a Content-Length header", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let upstreamCalled = false;
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 16,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      upstreamCalled = true;
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const bigStream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(100));
      controller.close();
    },
  });
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bigStream,
      duplex: "half",
    })
  );
  assert.equal(res.status, 413);
  assert.equal(upstreamCalled, false);
});

test("F2: Actual body over the limit is rejected even with an understated Content-Length", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 16,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };
  const rateGate = new RateGate(ctx, env);
  const bigStream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(100));
      controller.close();
    },
  });
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "1" },
      body: bigStream,
      duplex: "half",
    })
  );
  assert.equal(res.status, 413, "Understated Content-Length must not bypass the limit");
});

test("F2: Oversized request does not acquire a queue slot or count as an attempt", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 16,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };
  const rateGate = new RateGate(ctx, env);

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(...new Array(50).fill(7)),
    })
  );
  assert.equal(res.status, 413);
  assert.equal(rateGate.activeTicket, null, "No queue slot may be held after a 413");
  assert.equal(
    await ctx.storage.get("rate-state"),
    undefined,
    "No attempt may be recorded for a rejected oversized request"
  );

  // A valid request afterwards must succeed (no leaked queue state).
  const ok = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(1),
    })
  );
  assert.equal(ok.status, 200);
  await ok.text();
});

test("F2: Request-body read failure returns 400 without leaking queue state", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 1024,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };
  const rateGate = new RateGate(ctx, env);
  const failingStream = new ReadableStream({
    pull() {
      throw new Error("socket reset");
    },
  });
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: failingStream,
      duplex: "half",
    })
  );
  assert.equal(res.status, 400);
  const data = await res.json();
  assert.equal(data.error, "Failed to read request body");
  assert.equal(rateGate.activeTicket, null, "No slot may be held after a body-read failure");

  const ok = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(1, 2, 3),
    })
  );
  assert.equal(ok.status, 200);
  await ok.text();
});

test("F2: Empty request body remains valid", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let receivedBody = null;
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 16,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      receivedBody = opts.body;
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", { method: "POST" })
  );
  assert.equal(res.status, 200);
  await res.text();
  assert.equal(receivedBody.byteLength, 0, "Empty body must be forwarded as zero bytes");
});

test("F2: MAX_BODY_BYTES = 0 rejects any non-empty body but allows empty bodies", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };
  const rateGate = new RateGate(ctx, env);

  const empty = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", { method: "POST" })
  );
  assert.equal(empty.status, 200, "Empty body must be accepted when the limit is zero");

  const nonEmpty = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(1),
    })
  );
  assert.equal(nonEmpty.status, 413, "Any non-empty body must be rejected when the limit is zero");
});

// ==========================================
// F3 REGRESSION TESTS — Strict configuration parsing
// ==========================================

test("F3: Missing values select the documented defaults", () => {
  const cfg = resolveConfig({});
  assert.equal(cfg.RPM, 40);
  assert.equal(cfg.COOLDOWN_MS, 1670);
  assert.equal(cfg.RESPONSE_HEADER_TIMEOUT_MS, 30_000);
  assert.equal(cfg.STREAM_IDLE_TIMEOUT_MS, 30_000);
  assert.equal(cfg.MAX_QUEUE_WAIT_MS, 60_000);
  assert.equal(cfg.MAX_QUEUE_DEPTH, 100);
  assert.equal(cfg.MAX_BODY_BYTES, 20 * 1024 * 1024);
});

test("F3: Explicit null and 'null' values select the documented defaults", () => {
  const cfg = resolveConfig({
    RPM: null,
    COOLDOWN_MS: "null",
    MAX_QUEUE_DEPTH: null,
    MAX_BODY_BYTES: "null",
  });
  assert.equal(cfg.RPM, 40);
  assert.equal(cfg.COOLDOWN_MS, 1670);
  assert.equal(cfg.MAX_QUEUE_DEPTH, 100);
  assert.equal(cfg.MAX_BODY_BYTES, 20 * 1024 * 1024);
});

test("F3: Empty and whitespace-only strings are rejected", () => {
  assert.throws(() => resolveConfig({ RPM: "" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "   " }), ConfigError);
  assert.throws(() => resolveConfig({ COOLDOWN_MS: "\t\n " }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_BODY_BYTES: "" }), ConfigError);
});

test("F3: Valid zero is preserved only where it has documented meaning", () => {
  assert.equal(resolveConfig({ COOLDOWN_MS: 0 }).COOLDOWN_MS, 0);
  assert.equal(resolveConfig({ COOLDOWN_MS: "0" }).COOLDOWN_MS, 0);
  assert.equal(resolveConfig({ MAX_QUEUE_DEPTH: 0 }).MAX_QUEUE_DEPTH, 0);
  assert.equal(resolveConfig({ MAX_QUEUE_DEPTH: "0" }).MAX_QUEUE_DEPTH, 0);
  assert.equal(resolveConfig({ MAX_BODY_BYTES: 0 }).MAX_BODY_BYTES, 0);
});

test("F3: Unsupported zero values are rejected", () => {
  assert.throws(() => resolveConfig({ RPM: 0 }), ConfigError);
  assert.throws(() => resolveConfig({ STREAM_IDLE_TIMEOUT_MS: "0" }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_QUEUE_WAIT_MS: 0 }), ConfigError);
  assert.throws(() => resolveConfig({ RESPONSE_HEADER_TIMEOUT_MS: "0" }), ConfigError);
});

test("F3: Fractional integer-only values are rejected", () => {
  assert.throws(() => resolveConfig({ RPM: 40.5 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_QUEUE_DEPTH: "3.5" }), ConfigError);
  assert.throws(() => resolveConfig({ STREAM_IDLE_TIMEOUT_MS: "10.1" }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_BODY_BYTES: 1.5 }), ConfigError);
});

test("F3: Negative values are rejected", () => {
  assert.throws(() => resolveConfig({ RPM: -1 }), ConfigError);
  assert.throws(() => resolveConfig({ COOLDOWN_MS: -1 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_BODY_BYTES: -5 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_QUEUE_DEPTH: "-2" }), ConfigError);
});

test("F3: NaN, infinities, malformed strings, and excessive values are rejected", () => {
  assert.throws(() => resolveConfig({ RPM: NaN }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: Infinity }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: -Infinity }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "NaN" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "Infinity" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "-Infinity" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "abc" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "12px" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "0x10" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "1e3" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: "1,000" }), ConfigError);
  assert.throws(() => resolveConfig({ RPM: 1_000_000 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_BODY_BYTES: 2 ** 40 }), ConfigError);
  assert.throws(() => resolveConfig({ STREAM_IDLE_TIMEOUT_MS: 1e12 }), ConfigError);
});

test("F3: Legacy aliases are honored and canonical names take priority", () => {
  assert.equal(
    resolveConfig({ UPSTREAM_TIMEOUT_MS: 5000 }).RESPONSE_HEADER_TIMEOUT_MS,
    5000
  );
  assert.equal(resolveConfig({ QUEUE_TIMEOUT_MS: 1234 }).MAX_QUEUE_WAIT_MS, 1234);
  assert.equal(resolveConfig({ MAX_QUEUE_SIZE: 5 }).MAX_QUEUE_DEPTH, 5);
  assert.equal(
    resolveConfig({ MAX_QUEUE_DEPTH: 7, MAX_QUEUE_SIZE: 9 }).MAX_QUEUE_DEPTH,
    7,
    "Canonical name must win over the legacy alias"
  );
});

test("F3: String numerics from environment bindings parse correctly", () => {
  const cfg = resolveConfig({ RPM: "20", COOLDOWN_MS: " 250 ", MAX_BODY_BYTES: "1024" });
  assert.equal(cfg.RPM, 20);
  assert.equal(cfg.COOLDOWN_MS, 250);
  assert.equal(cfg.MAX_BODY_BYTES, 1024);
});

test("F3: Configuration errors do not reveal configured secrets", () => {
  let error;
  try {
    resolveConfig({ RPM: "not-a-number", NVIDIA_API_KEY: "sk-super-secret-value" });
  } catch (err) {
    error = err;
  }
  assert.ok(error instanceof ConfigError, "Invalid configuration must throw ConfigError");
  assert.ok(
    !error.message.includes("sk-super-secret-value"),
    "Error message must not contain secret values"
  );
});

test("F3: readConfigValue honors spec bounds and rejects unsupported types", () => {
  const floatSpec = { default: 1, min: 0, max: 10, integer: false, allowZero: true };
  assert.equal(readConfigValue("2.5", floatSpec, "X"), 2.5);
  assert.equal(readConfigValue(undefined, floatSpec, "X"), 1);
  assert.throws(() => readConfigValue("11", floatSpec, "X"), ConfigError);
  assert.throws(() => readConfigValue({}, floatSpec, "X"), ConfigError);
});

test("F3: Invalid runtime configuration fails closed with 500 and no secret leak", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: "definitely-not-a-number",
    NVIDIA_API_KEY: "sk-do-not-leak",
    PROXY_KEY: "proxy-secret",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };
  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: bytes(1),
    })
  );
  assert.equal(res.status, 500);
  const text = await res.text();
  assert.ok(!text.includes("sk-do-not-leak"), "Response must not reveal secrets");
  assert.ok(!text.includes("proxy-secret"), "Response must not reveal secrets");
  assert.equal(rateGate.activeTicket, null, "No queue slot may be acquired on config failure");
});

// ==========================================
// BATCH 1G REGRESSION TESTS — Body read timeout, limits, cancellation
// ==========================================

test("F1: Body completing before the deadline is accepted", async () => {
  const timers = createFakeTimers();
  const body = createControllableBody();
  const fakeRequest = {
    headers: { get: () => null },
    body: body.stream,
  };
  const promise = readBoundedBody(fakeRequest, 1024, {
    timeoutMs: 5_000,
    setTimeoutImpl: (fn, ms) => timers.setTimeout(fn, ms),
    clearTimeoutImpl: (id) => timers.clearTimeout(id),
    cancelGraceMs: 1_000,
  });

  await tick();
  body.state.push(new Uint8Array([1, 2, 3]));
  body.state.close();

  const result = await promise;
  assert.deepEqual(Array.from(result), [1, 2, 3]);
  assert.equal(timers.pendingCount(), 0, "All timers must be cleared on the success path");
});

test("F1: A body that stalls until the deadline returns a 408 timeout", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const body = createControllableBody();
  let upstreamCalled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    REQUEST_BODY_TIMEOUT_MS: 2_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      upstreamCalled = true;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: body.stream,
    duplex: "half",
  });

  const resPromise = rateGate.fetch(req);
  await tick();
  // The client sends nothing and stalls -> fire the total deadline.
  timers.advance(2_000);

  const res = await resPromise;
  assert.equal(res.status, 408, "A stalled body must return 408 Request Timeout");
  const data = await res.json();
  assert.equal(data.error, "Request body read timed out");
  assert.equal(upstreamCalled, false, "No upstream dispatch may occur on a body timeout");
  assert.equal(rateGate.activeTicket, null, "No queue slot may be acquired on a body timeout");
  assert.equal(
    await ctx.storage.get("rate-state"),
    undefined,
    "No attempt accounting may occur on a body timeout"
  );
});

test("F1: A body that keeps producing chunks but exceeds the total deadline returns 408", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const body = createControllableBody();

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    REQUEST_BODY_TIMEOUT_MS: 1_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: body.stream,
    duplex: "half",
  });

  const resPromise = rateGate.fetch(req);
  await tick();
  // The client keeps dribbling bytes but never finishes before the deadline.
  body.state.push(new Uint8Array([1]));
  await tick();
  body.state.push(new Uint8Array([2]));
  await tick();
  timers.advance(1_000);

  const res = await resPromise;
  assert.equal(res.status, 408, "The deadline must cover the whole body, not restart per chunk");
});

test("F1: A timeout that loses the race to a completed body does not dispatch late", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const body = createControllableBody();
  let upstreamCalled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    REQUEST_BODY_TIMEOUT_MS: 5_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      upstreamCalled = true;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: body.stream,
    duplex: "half",
  });

  const resPromise = rateGate.fetch(req);
  await tick();
  // Body completes and closes BEFORE the deadline fires.
  body.state.push(new Uint8Array([9, 9]));
  body.state.close();
  const res = await resPromise;
  assert.equal(res.status, 200, "A body completing before the deadline must be accepted");
  await res.text();
  assert.equal(upstreamCalled, true, "The accepted request must dispatch upstream");
  assert.equal(timers.pendingCount(), 0, "No deadline timer may remain after completion");

  // Advancing past the deadline must not cause any late dispatch or error.
  timers.advance(10_000);
  assert.equal(upstreamCalled, true);
});

test("F3: A timeout does not wait indefinitely for a never-settling reader.cancel()", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const body = createControllableBody({ cancelNeverSettles: true });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    REQUEST_BODY_TIMEOUT_MS: 1_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: body.stream,
    duplex: "half",
  });

  const resPromise = rateGate.fetch(req);
  await tick();
  timers.advance(1_000); // total deadline fires

  let settled = false;
  resPromise.then(() => {
    settled = true;
  });
  await tick();
  // The 408 response must arrive once the bounded cancel grace elapses, even
  // though reader.cancel() never settles.
  assert.equal(settled, false, "Response must not be delivered before the bounded grace elapses");
  assert.equal(body.state.cancelCalled, true, "Cancellation must be attempted");
  timers.advance(1_000); // cancel grace fires -> bounded cleanup completes

  const res = await resPromise;
  assert.equal(res.status, 408, "The 408 must not be blocked by a never-settling cancel()");
  assert.equal(rateGate.activeTicket, null);
});

test("F3: A late body read after timeout cannot enter the queue", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const manual = createManualBody();
  let upstreamCalled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    REQUEST_BODY_TIMEOUT_MS: 1_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      upstreamCalled = true;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = {
    url: "https://proxy.internal/v1/chat/completions",
    method: "POST",
    headers: { get: () => null },
    signal: undefined,
    body: manual.body,
  };

  const resPromise = rateGate.fetch(req);
  await tick();
  timers.advance(1_000); // deadline fires -> 408
  timers.advance(1_000); // bounded cancel grace elapses
  const res = await resPromise;
  assert.equal(res.status, 408);
  assert.equal(manual.state.cancelled, true);

  // Now the stalled upstream read finally resolves with data; the late result
  // must be discarded and must never reach queue admission or dispatch.
  manual.resolveRead(new Uint8Array([1, 2, 3]));
  await tick();
  await tick();
  manual.closeRead();
  await tick();
  assert.equal(upstreamCalled, false, "A late body read must never enter the queue or dispatch");
  assert.equal(rateGate.activeTicket, null);
  assert.equal(await ctx.storage.get("rate-state"), undefined);
});

test("F2: MAX_BODY_BYTES cannot be configured above the 20 MiB safe cap", () => {
  assert.equal(resolveConfig({ MAX_BODY_BYTES: 20 * 1024 * 1024 }).MAX_BODY_BYTES, 20 * 1024 * 1024);
  assert.throws(
    () => resolveConfig({ MAX_BODY_BYTES: 20 * 1024 * 1024 + 1 }),
    ConfigError,
    "Values above the 20 MiB cap must be rejected"
  );
  assert.throws(() => resolveConfig({ MAX_BODY_BYTES: "1073741824" }), ConfigError);
});

test("F2: A body at the exact limit is forwarded byte-for-byte, including binary", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let receivedBody = null;
  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 8,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async (url, opts) => {
      receivedBody = opts.body;
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const payload = new Uint8Array([0, 255, 1, 254, 2, 253, 3, 252]);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: payload,
    })
  );
  assert.equal(res.status, 200);
  await res.text();
  assert.deepEqual(
    Array.from(receivedBody),
    Array.from(payload),
    "Binary body must be forwarded byte-identical"
  );
});

test("F2: Multi-chunk body is concatenated in order and preserved exactly", async () => {
  const body = createControllableBody();
  const fakeRequest = { headers: { get: () => null }, body: body.stream };
  const promise = readBoundedBody(fakeRequest, 1024);
  await tick();
  body.state.push(new Uint8Array([1, 2]));
  await tick();
  body.state.push(new Uint8Array([3]));
  await tick();
  body.state.push(new Uint8Array([4, 5]));
  await tick();
  body.state.close();
  const result = await promise;
  assert.deepEqual(Array.from(result), [1, 2, 3, 4, 5]);
});

test("F3: A rejecting reader.cancel() does not become an unhandled rejection", async () => {
  const body = createControllableBody({
    onCancel: () => {
      throw new Error("cancel rejected");
    },
  });
  const fakeRequest = { headers: { get: () => null }, body: body.stream };
  const timers = createFakeTimers();
  const promise = readBoundedBody(fakeRequest, 1024, {
    timeoutMs: 1_000,
    setTimeoutImpl: (fn, ms) => timers.setTimeout(fn, ms),
    clearTimeoutImpl: (id) => timers.clearTimeout(id),
    cancelGraceMs: 500,
  });
  await tick();
  timers.advance(1_000);
  timers.advance(500);
  await assert.rejects(() => promise, BodyReadTimeoutError);
  // Allow any stray microtasks to run; an unhandled rejection would fail the run.
  await tick();
  await tick();
});

test("F3: Oversized body with a never-settling cancel still returns 413 promptly", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const body = createControllableBody({ cancelNeverSettles: true });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 4,
    REQUEST_BODY_TIMEOUT_MS: 60_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: body.stream,
    duplex: "half",
  });

  const resPromise = rateGate.fetch(req);
  await tick();
  body.state.push(new Uint8Array(10)); // exceeds the limit -> BodyTooLargeError
  await tick();
  timers.advance(1_000); // bounded cancel grace

  const res = await resPromise;
  assert.equal(res.status, 413, "413 must not be blocked by a never-settling cancel()");
  assert.equal(rateGate.activeTicket, null);
});

test("F1: REQUEST_BODY_TIMEOUT_MS defaults and bounds are enforced", () => {
  assert.equal(resolveConfig({}).REQUEST_BODY_TIMEOUT_MS, 30_000);
  assert.equal(resolveConfig({ REQUEST_BODY_TIMEOUT_MS: "1000" }).REQUEST_BODY_TIMEOUT_MS, 1_000);
  assert.equal(resolveConfig({ REQUEST_BODY_TIMEOUT_MS: 120_000 }).REQUEST_BODY_TIMEOUT_MS, 120_000);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: 999 }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: 120_001 }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: 0 }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: -5 }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: "1500.5" }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: "NaN" }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: "" }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: "   " }), ConfigError);
  assert.throws(() => resolveConfig({ REQUEST_BODY_TIMEOUT_MS: "Infinity" }), ConfigError);
});

// ==========================================
// BATCH 1G.1 REGRESSION TESTS — Late body reads after timeout
// ==========================================

test("1G.1-A: A late read fulfillment after timeout is not retained and no further read is initiated", async () => {
  const timers = createFakeTimers();
  const tracked = createTrackedBody();
  const fakeRequest = { headers: { get: () => null }, body: tracked.body };

  const promise = readBoundedBody(fakeRequest, 1024, {
    timeoutMs: 1_000,
    setTimeoutImpl: (fn, ms) => timers.setTimeout(fn, ms),
    clearTimeoutImpl: (id) => timers.clearTimeout(id),
    cancelGraceMs: 500,
  });
  promise.catch(() => {});

  await tick();
  assert.equal(tracked.state.readsStarted, 1, "Exactly one read must be in flight");
  assert.equal(tracked.pendingReads, 1);

  // Deadline expires while the read is still pending.
  timers.advance(1_000);
  await tick();
  assert.equal(tracked.state.cancelCalled, true, "Cancellation must be attempted on timeout");
  timers.advance(500); // bounded cancel grace elapses -> handler settles with 408

  await assert.rejects(() => promise, BodyReadTimeoutError);

  const readsStartedAtTimeout = tracked.state.readsStarted;

  // The pending read finally fulfills with a non-empty chunk AFTER the 408.
  assert.equal(tracked.resolveNext(new Uint8Array([7, 7, 7])), true);
  await tick();
  await tick();

  assert.equal(
    tracked.state.readsStarted,
    readsStartedAtTimeout,
    "The loop must NOT initiate another read after timeout"
  );
  assert.equal(tracked.pendingReads, 0, "No new read may be left pending after timeout");
});

test("1G.1-B: A source that keeps producing after timeout is not consumed or retained, and never dispatches", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const tracked = createTrackedBody({ cancelNeverSettles: true });
  let upstreamCalled = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    REQUEST_BODY_TIMEOUT_MS: 1_000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      upstreamCalled = true;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const req = {
    url: "https://proxy.internal/v1/chat/completions",
    method: "POST",
    headers: { get: () => null },
    signal: undefined,
    body: tracked.body,
  };

  const resPromise = rateGate.fetch(req);
  await tick();
  timers.advance(1_000); // deadline fires -> terminal state set
  await tick(); // let the handler enter bounded cancellation (arms the grace timer)
  timers.advance(1_000); // bounded cancel grace elapses (cancel() never settles)
  const res = await resPromise;
  assert.equal(res.status, 408);

  const readsStartedAtTimeout = tracked.state.readsStarted;

  // The source keeps producing: several late chunks become available.
  for (let i = 0; i < 5; i++) {
    tracked.resolveNext(new Uint8Array([i, i, i, i]));
    await tick();
  }
  await tick();

  assert.equal(
    tracked.state.readsStarted,
    readsStartedAtTimeout,
    "The application-level loop must not consume successive late chunks"
  );
  assert.equal(upstreamCalled, false, "No upstream dispatch after a body timeout");
  assert.equal(rateGate.activeTicket, null, "No queue slot acquired after a body timeout");
  assert.equal(
    await ctx.storage.get("rate-state"),
    undefined,
    "No rate-limit attempt accounting after a body timeout"
  );
});

test("1G.1-C: A late read rejection after timeout causes no unhandled rejection or state corruption", async () => {
  const timers = createFakeTimers();
  const tracked = createTrackedBody();
  const fakeRequest = { headers: { get: () => null }, body: tracked.body };

  const promise = readBoundedBody(fakeRequest, 1024, {
    timeoutMs: 1_000,
    setTimeoutImpl: (fn, ms) => timers.setTimeout(fn, ms),
    clearTimeoutImpl: (id) => timers.clearTimeout(id),
    cancelGraceMs: 500,
  });
  promise.catch(() => {});

  await tick();
  timers.advance(1_000);
  timers.advance(500);
  await assert.rejects(() => promise, BodyReadTimeoutError);

  // The pending read rejects only after the timeout already settled.
  assert.equal(tracked.rejectNext(new Error("late source failure")), true);
  await tick();
  await tick();
  // Reaching here without the runner flagging an unhandled rejection is the assertion.
  assert.equal(tracked.state.readsStarted, 1, "No further read may be initiated");
});

test("1G.1-D1: Successful completion before the deadline is accepted and no terminal action is duplicated", async () => {
  const timers = createFakeTimers();
  const tracked = createTrackedBody();
  const fakeRequest = { headers: { get: () => null }, body: tracked.body };

  const promise = readBoundedBody(fakeRequest, 1024, {
    timeoutMs: 5_000,
    setTimeoutImpl: (fn, ms) => timers.setTimeout(fn, ms),
    clearTimeoutImpl: (id) => timers.clearTimeout(id),
    cancelGraceMs: 500,
  });

  await tick();
  tracked.resolveNext(new Uint8Array([1, 2, 3]));
  await tick();
  tracked.closeNext();
  const result = await promise;
  assert.deepEqual(Array.from(result), [1, 2, 3]);
  assert.equal(timers.pendingCount(), 0, "The deadline timer must be cleared on completion");
  assert.equal(tracked.state.cancelCalled, false, "No cancellation on the success path");

  // A late timer must not retroactively cancel a completed request.
  timers.advance(10_000);
  assert.equal(tracked.state.cancelCalled, false, "A late deadline must not cancel a completed read");
});

test("1G.1-D2: A timeout winning first cannot be reversed by a late read completion", async () => {
  const timers = createFakeTimers();
  const tracked = createTrackedBody();
  const fakeRequest = { headers: { get: () => null }, body: tracked.body };

  const promise = readBoundedBody(fakeRequest, 1024, {
    timeoutMs: 1_000,
    setTimeoutImpl: (fn, ms) => timers.setTimeout(fn, ms),
    clearTimeoutImpl: (id) => timers.clearTimeout(id),
    cancelGraceMs: 500,
  });
  promise.catch(() => {});

  await tick();
  timers.advance(1_000); // timeout wins the race
  await tick();
  // Late completion arrives before the bounded cancel grace has elapsed.
  tracked.closeNext();
  await tick();
  timers.advance(500);

  await assert.rejects(() => promise, BodyReadTimeoutError, "The timeout outcome must stand");
  assert.equal(timers.pendingCount(), 0, "No timer may remain after the terminal path");
});

// ==========================================
// NQP-02: Global-fetch receiver regression (unit)
// ==========================================
test("NQP-02: default fetch path invokes the global fetch with no receiver (no Illegal invocation)", async () => {
  const origFetch = globalThis.fetch;
  globalThis.fetch = strictFetchSpy;
  try {
    fetchReceiver = "not-called";
    const ctx = createMockCtx();
    const rateGate = new RateGate(ctx, makeDefaultFetchEnv());
    const req = new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: JSON.stringify({ message: "hi" }),
    });
    const res = await rateGate.fetch(req);
    assert.equal(res.status, 200, "The proxied response must be propagated, not a 502");
    assert.equal(fetchReceiver, "undefined", "The global fetch must be called without a receiver (this === undefined)");
  } finally {
    globalThis.fetch = origFetch;
  }
});

test("NQP-02: injected env.__fetch seam is still honored", async () => {
  const ctx = createMockCtx();
  let called = false;
  const env = makeDefaultFetchEnv({
    __fetch: async () => {
      called = true;
      return new Response("injected", { status: 200 });
    },
  });
  const rateGate = new RateGate(ctx, env);
  const req = new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body: JSON.stringify({ message: "hi" }),
  });
  const res = await rateGate.fetch(req);
  assert.equal(res.status, 200);
  assert.equal(called, true, "env.__fetch must be used when provided");
  assert.equal(await res.text(), "injected");
});

// ==========================================
// NQP-RL-02: Rate-limiter regression tests
// ==========================================
// Direct evidence that RateGate enforces: (1) a minimum interval between
// consecutive upstream starts equal to the cooldown, (2) the sliding-window
// request limit at its 60s boundary, (3) strict one-at-a-time upstream
// serialization, and (4) that queued requests and 429 retries cannot bypass the
// limiter. All timing is driven by the injectable FakeClock / deferred upstream
// (no real sleeps, no network, no NVIDIA credentials).

const RL_COOLDOWN_MS = 1_670;
const RL_WINDOW_MS = 60_000;

test("NQP-RL-02: consecutive upstream starts are spaced by at least the cooldown", async () => {
  const clock = new FakeClock(1_000_000);
  const ctx = createMockCtx();
  const starts = [];
  const env = {
    RPM: 40,
    COOLDOWN_MS: RL_COOLDOWN_MS,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      starts.push(clock.now());
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const count = 5;

  await Promise.all(
    Array.from({ length: count }, (_, i) =>
      rateGate
        .fetch(
          new Request("https://proxy.internal/v1/chat/completions", {
            method: "POST",
            headers: { "x-req-id": String(i) },
            body: "{}",
          })
        )
        .then((res) => res.text())
    )
  );

  assert.equal(starts.length, count, "every queued request must reach the upstream");
  const ordered = [...starts].sort((a, b) => a - b);
  const gaps = ordered.slice(1).map((t, i) => t - ordered[i]);

  for (const gap of gaps) {
    assert.ok(
      gap >= RL_COOLDOWN_MS,
      `consecutive upstream starts must be >= ${RL_COOLDOWN_MS}ms apart (got ${gap}ms)`
    );
  }
  assert.deepEqual(
    gaps,
    [RL_COOLDOWN_MS, RL_COOLDOWN_MS, RL_COOLDOWN_MS, RL_COOLDOWN_MS],
    "with the fake clock the spacing is exactly the cooldown (no extra padding)"
  );
});

test("NQP-RL-02: sliding-window limit is enforced at the 60s boundary", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const starts = [];
  const env = {
    RPM: 3,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      starts.push(clock.now());
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const count = 6;

  await Promise.all(
    Array.from({ length: count }, (_, i) =>
      rateGate
        .fetch(
          new Request("https://proxy.internal/v1/chat/completions", {
            method: "POST",
            headers: { "x-req-id": String(i) },
            body: "{}",
          })
        )
        .then((res) => res.text())
    )
  );

  const ordered = [...starts].sort((a, b) => a - b);
  assert.deepEqual(
    ordered,
    [0, 0, 0, RL_WINDOW_MS, RL_WINDOW_MS, RL_WINDOW_MS],
    "the 4th request must wait for the oldest start to leave the 60s window"
  );

  for (const t of ordered) {
    const inWindow = ordered.filter(
      (x) => x >= t && x < t + RL_WINDOW_MS
    ).length;
    assert.ok(
      inWindow <= 3,
      `no 60s window may contain more than RPM=3 starts (found ${inWindow} at t=${t})`
    );
  }
});

test("NQP-RL-02: at most one upstream request is in flight at a time", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let inFlight = 0;
  let maxInFlight = 0;
  const releaseUpstream = [];
  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    // Blocks until the test releases it, so overlap (if any) is directly observable.
    __fetch: async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((resolve) => releaseUpstream.push(resolve));
      inFlight -= 1;
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);
  const count = 5;

  const waitFor = async (predicate, label) => {
    for (let i = 0; i < 500; i++) {
      if (predicate()) return;
      await tick();
    }
    assert.fail(`Timed out waiting for ${label}`);
  };

  const settled = Array.from({ length: count }, (_, i) =>
    rateGate
      .fetch(
        new Request("https://proxy.internal/v1/chat/completions", {
          method: "POST",
          headers: { "x-req-id": String(i) },
          body: "{}",
        })
      )
      .then((res) => res.text())
  );

  await waitFor(
    () => releaseUpstream.length >= 1,
    "the first upstream call to start"
  );
  assert.equal(
    inFlight,
    1,
    "only one upstream call may be in flight while the rest wait"
  );

  for (let released = 0; released < count; released++) {
    releaseUpstream[released]();
    await waitFor(
      () => released === count - 1 || releaseUpstream.length >= released + 2,
      `upstream call ${released + 2} to start only after the previous resolves`
    );
  }

  await Promise.all(settled);
  assert.equal(maxInFlight, 1, "no two upstream calls may ever overlap");
});

test("NQP-RL-02: a 429 retry cannot bypass the cooldown between upstream attempts", async () => {
  const clock = new FakeClock(1_000_000);
  const ctx = createMockCtx();
  const attemptTimes = [];
  let attempt = 0;
  const env = {
    RPM: 100,
    COOLDOWN_MS: RL_COOLDOWN_MS,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      attemptTimes.push(clock.now());
      attempt += 1;
      if (attempt === 1) {
        // No Retry-After: the exponential backoff (1000ms) alone is below the
        // cooldown, so the limiter must supply the remaining gap.
        return new Response(JSON.stringify({ error: "rate limit" }), {
          status: 429,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response("ok", { status: 200 });
    },
  };
  const rateGate = new RateGate(ctx, env);

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  await res.text();

  assert.equal(attemptTimes.length, 2, "one initial attempt plus one retry");
  const gap = attemptTimes[1] - attemptTimes[0];
  assert.ok(
    gap >= RL_COOLDOWN_MS,
    `the retry must wait at least the ${RL_COOLDOWN_MS}ms cooldown after the previous attempt (got ${gap}ms)`
  );
});


// ==========================================
// NQP-429 REGRESSION TESTS — Bounded upstream 429 bodies
// ==========================================
//
// The upstream 429 (rate-limit) error body is read with a hard byte cap and a
// hard time cap instead of an unbounded response.arrayBuffer(). These tests pin
// the observable behavior of that bound: bodies up to the cap pass through
// byte-for-byte, over-limit or unreadable bodies are replaced with a
// deterministic synthetic JSON body, and every path (including stalled reads
// and never-settling cancellations) still releases the queue slot and preserves
// retry/cooldown/accounting semantics.
//
// MAX_429 mirrors MAX_429_BODY_BYTES in src/index.js (64 KiB). It is duplicated
// here rather than imported because that source constant is intentionally not
// exported: workerd's module format rejects non-function named exports, which
// the Miniflare runtime test would otherwise fail on.
const MAX_429 = 64 * 1024;
const SYNTHETIC_429 = {
  error: "Upstream rate-limit response body exceeded the proxy limit",
};

function make429Env(overrides = {}) {
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    ...overrides,
  };
  return { clock, ctx, env };
}

function chatRequest(body = "{}") {
  return new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    body,
  });
}

test("NQP-429-01: a 429 response with an empty body is passed through unchanged", async () => {
  const { ctx, env } = make429Env();
  let attempts = 0;
  env.__fetch = async () => {
    attempts++;
    return new Response("", { status: 429 });
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(await res.text(), "", "an empty upstream body must remain empty");
  assert.equal(attempts, 6, "the existing retry limit must be preserved");
  assert.equal(rateGate.activeTicket, null, "the queue slot must be released");
});

test("NQP-429-02: a 429 body smaller than the cap is forwarded byte-for-byte", async () => {
  const { ctx, env } = make429Env();
  const upstreamBody = "rate limit exceeded: please slow down";
  env.__fetch = async () =>
    new Response(upstreamBody, {
      status: 429,
      headers: { "content-type": "text/plain", "x-custom-upstream": "keep-me" },
    });

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(res.headers.get("x-custom-upstream"), "keep-me", "safe headers must be preserved");
  assert.equal(await res.text(), upstreamBody, "a sub-cap body must be forwarded exactly");
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-03: a 429 body exactly at the cap is forwarded byte-for-byte", async () => {
  const { ctx, env } = make429Env();
  const upstreamBody = "a".repeat(MAX_429);
  env.__fetch = async () => new Response(upstreamBody, { status: 429 });

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  const text = await res.text();
  assert.equal(text.length, MAX_429, "a body at the cap must be accepted in full");
  assert.equal(text, upstreamBody);
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-04: a 429 body one byte over the cap is replaced with a synthetic body", async () => {
  const { ctx, env } = make429Env();
  const upstreamBody = "a".repeat(MAX_429 + 1);
  env.__fetch = async () =>
    new Response(upstreamBody, {
      status: 429,
      headers: { "x-custom-upstream": "keep-me" },
    });

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  const text = await res.text();
  assert.notEqual(text, upstreamBody, "the oversized upstream body must not be returned");
  assert.deepEqual(
    JSON.parse(text),
    SYNTHETIC_429,
    "a deterministic synthetic JSON body must be returned instead of a corrupt truncation"
  );
  assert.equal(res.headers.get("x-custom-upstream"), "keep-me", "safe headers must be preserved");
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-05: an effectively unbounded streamed 429 body is bounded and does not hang", async () => {
  const { ctx, env } = make429Env();
  let attempts = 0;
  env.__fetch = async () => {
    attempts++;
    const stream = new ReadableStream({
      pull(controller) {
        controller.enqueue(new Uint8Array(MAX_429)); // never ends
      },
    });
    return new Response(stream, { status: 429 });
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.deepEqual(JSON.parse(await res.text()), SYNTHETIC_429);
  assert.equal(attempts, 6, "the retry limit must still be enforced");
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-06: a 429 body stream that stalls during reading is bounded and cancelled", async () => {
  const timers = createFakeTimers();
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  let attempts = 0;
  let stalledCancelled = false;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      attempts++;
      if (attempts === 1) {
        const stream = new ReadableStream({
          pull() {
            /* never enqueues: stalls forever */
          },
          cancel() {
            stalledCancelled = true;
          },
        });
        return new Response(stream, { status: 429 });
      }
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const resPromise = rateGate.fetch(chatRequest());
  await tick();
  timers.advance(30_000); // the 429-body read deadline fires
  await tick();

  const res = await resPromise;
  assert.equal(res.status, 200, "the proxy must not hang on a stalled upstream read");
  assert.equal(stalledCancelled, true, "the stalled upstream stream must be cancelled");
  assert.equal(attempts, 2, "the stalled attempt must be abandoned and retried");
  await res.text();
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-07: a 429 body whose cancellation never settles is cleaned up within the bounded grace", async () => {
  const timers = createFakeTimers();
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  let attempts = 0;
  let cancelCalled = false;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      attempts++;
      if (attempts === 1) {
        const stream = new ReadableStream({
          pull(controller) {
            controller.enqueue(new Uint8Array(MAX_429 + 1)); // immediately over cap
          },
          cancel() {
            cancelCalled = true;
            return new Promise(() => {}); // never settles
          },
        });
        return new Response(stream, { status: 429 });
      }
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const resPromise = rateGate.fetch(chatRequest());
  await tick();
  await tick();
  timers.advance(1_000); // DEFAULT_BODY_CANCEL_GRACE_MS
  await tick();

  const res = await resPromise;
  assert.equal(cancelCalled, true, "cancellation must be attempted");
  assert.equal(res.status, 200, "the proxy must not hang on a never-settling cancel");
  assert.equal(attempts, 2);
  await res.text();
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-08: a non-JSON 429 body below the cap is forwarded verbatim", async () => {
  const { ctx, env } = make429Env();
  const upstreamBody = "RATE LIMITED <<<not json>>> plain text";
  env.__fetch = async () => new Response(upstreamBody, { status: 429 });

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(await res.text(), upstreamBody, "a non-JSON sub-cap body must be forwarded unchanged");
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-09: repeated 429 responses with bounded bodies still stop at the retry limit", async () => {
  const { ctx, env } = make429Env();
  let attempts = 0;
  const upstreamBody = "slow down";
  env.__fetch = async () => {
    attempts++;
    return new Response(upstreamBody, { status: 429 });
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(attempts, 6, "one initial attempt plus five retries");
  assert.equal(res.status, 429);
  assert.equal(await res.text(), upstreamBody, "the final bounded body must be returned");
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-10: a subsequent queued request succeeds after the bounded-body failure path", async () => {
  const { ctx, env } = make429Env();
  let calls = 0;
  const oversized = "a".repeat(MAX_429 + 1);
  env.__fetch = async () => {
    calls++;
    if (calls <= 6) return new Response(oversized, { status: 429 });
    return new Response("ok", { status: 200 });
  };

  const rateGate = new RateGate(ctx, env);
  const p1 = rateGate.fetch(chatRequest("1"));
  const p2 = rateGate.fetch(chatRequest("2"));

  const [r1, r2] = await Promise.all([p1, p2]);
  assert.equal(r1.status, 429);
  assert.deepEqual(JSON.parse(await r1.text()), SYNTHETIC_429);
  assert.equal(r2.status, 200);
  assert.equal(await r2.text(), "ok", "the queued request must succeed after the failure path");
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-11: attempt accounting and cooldown are preserved for bounded 429 bodies", async () => {
  const RL_COOLDOWN_MS = 1_670;
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  const attemptTimes = [];
  const oversized = "a".repeat(MAX_429 + 1);

  const env = {
    RPM: 100,
    COOLDOWN_MS: RL_COOLDOWN_MS,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      attemptTimes.push(clock.now());
      return new Response(oversized, { status: 429 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());
  assert.equal(res.status, 429);
  await res.text();

  const state = await ctx.storage.get("rate-state");
  assert.equal(state.starts.length, 6, "every attempt including retries must be counted");
  for (let i = 1; i < attemptTimes.length; i++) {
    const gap = attemptTimes[i] - attemptTimes[i - 1];
    assert.ok(
      gap >= RL_COOLDOWN_MS,
      `attempt ${i} must be at least the ${RL_COOLDOWN_MS}ms cooldown after the previous one (got ${gap}ms)`
    );
  }
  assert.equal(rateGate.activeTicket, null);
});

test("NQP-429-12: no overlapping upstream requests while handling 429 bodies", async () => {
  const { ctx, env } = make429Env();
  let calls = 0;
  let inFlight = 0;
  let maxInFlight = 0;

  env.__fetch = async () => {
    calls++;
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    if (calls <= 6) {
      const stream = new ReadableStream({
        pull(controller) {
          controller.enqueue(new Uint8Array(MAX_429 + 1));
        },
        cancel() {
          inFlight--;
        },
      });
      return new Response(stream, { status: 429 });
    }
    inFlight--;
    return new Response("ok", { status: 200 });
  };

  const rateGate = new RateGate(ctx, env);
  const [r1, r2] = await Promise.all([
    rateGate.fetch(chatRequest("1")),
    rateGate.fetch(chatRequest("2")),
  ]);

  assert.equal(r1.status, 429);
  await r1.text();
  assert.equal(r2.status, 200);
  await r2.text();

  assert.equal(maxInFlight, 1, "at most one upstream request may be in flight at a time");
  assert.equal(calls, 7, "six attempts for the first request plus one for the second");
  assert.equal(rateGate.activeTicket, null);
});


// ==========================================
// NQP-P2 REGRESSION TESTS — Retry-policy hardening (jitter + bounded Retry-After)
// ==========================================
//
// NQP-P2-FINISH adds (a) an additive, bounded jitter band to the retry backoff
// and (b) an explicit upper bound on Retry-After parsing. These tests pin both
// behaviors with injected determinism (a fixed __random / random function) so
// they never depend on wall-clock timing or real randomness.

test("NQP-P2-01: applyBackoffJitter stays within [delay, maxBackoff] across the random domain", () => {
  const maxBackoff = 30_000;

  for (const delay of [1, 1000, 5000, 24_000, 30_000]) {
    for (const r of [0, 0.25, 0.5, 0.999999, 1]) {
      const out = applyBackoffJitter(delay, () => r, maxBackoff);

      assert.ok(
        out >= delay,
        `jitter must never reduce the delay (delay=${delay}, r=${r}, out=${out})`
      );
      assert.ok(
        out <= maxBackoff,
        `jitter must never exceed the maximum (delay=${delay}, r=${r}, out=${out})`
      );
      assert.ok(
        Number.isInteger(out),
        `jitter must yield an integer millisecond delay (delay=${delay}, r=${r})`
      );
    }
  }
});

test("NQP-P2-02: applyBackoffJitter is deterministic for injected randomness and preserves zero", () => {
  assert.equal(applyBackoffJitter(8000, () => 0, 30_000), 8000, "zero random adds nothing");
  assert.equal(applyBackoffJitter(8000, () => 1, 30_000), 10_000, "full random adds the 25% band");
  assert.equal(applyBackoffJitter(30_000, () => 1, 30_000), 30_000, "a delay at the cap stays at the cap");
  assert.equal(applyBackoffJitter(0, () => 1, 30_000), 0, "a zero delay must stay zero (no spurious delay)");
});

test("NQP-P2-03: jittered backoff never falls below a Retry-After minimum", () => {
  const now = 100_000;
  const resp = new Response("429", { headers: { "Retry-After": "10" } });

  const floor = computeBackoffDelay(0, resp, {
    now,
    maxBackoffMs: 30_000,
    jitterFn: (d) => applyBackoffJitter(d, () => 0, 30_000),
  });
  assert.equal(floor, 10_000, "with zero jitter the delay equals the Retry-After floor");

  const ceiling = computeBackoffDelay(0, resp, {
    now,
    maxBackoffMs: 30_000,
    jitterFn: (d) => applyBackoffJitter(d, () => 1, 30_000),
  });
  assert.equal(ceiling, 12_500, "jitter is applied on top of the Retry-After floor");
  assert.ok(ceiling >= 10_000, "jitter must never drop below the server-requested Retry-After");
});

test("NQP-P2-04: retryAfterMs caps oversized and extreme values at the backoff maximum", () => {
  const now = 1_000_000;
  const mk = (v) => new Response("429", { headers: { "Retry-After": v } });

  assert.equal(retryAfterMs(mk("999999999"), now), 30_000, "a huge seconds value is capped");
  assert.equal(retryAfterMs(mk("1000000"), now), 30_000, "a very large seconds value is capped");
  assert.equal(retryAfterMs(mk("5"), now), 5_000, "a valid small value is preserved exactly");
  assert.equal(retryAfterMs(mk("999"), now, 5_000), 5_000, "a custom cap is honored");
  // "1e400" is not a valid delay-seconds (1*DIGIT) and is not an HTTP-date, so
  // it is malformed and follows the documented fallback (0 => use exponential
  // backoff) rather than being coerced into an unbounded "Infinity" delay.
  assert.equal(retryAfterMs(mk("1e400"), now), 0, "a malformed non-digit value falls back instead of being coerced");

  const farFuture = new Date(now + 9_999_999_999).toUTCString();
  assert.equal(retryAfterMs(mk(farFuture), now), 30_000, "a far-future HTTP-date is capped");
});

test("NQP-P2-05: the retry path applies jitter to backoff delays and counts each attempt", async () => {
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();
  let attempts = 0;

  const env = {
    RPM: 1000,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 1,
    __fetch: async () => {
      attempts++;
      return new Response("", { status: 429 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  await res.text();

  assert.equal(attempts, 6, "six attempts (initial + five retries)");
  assert.deepEqual(
    clock.sleepCalls.map((c) => c.ms),
    [1250, 2500, 5000, 10_000, 20_000],
    "each retry delay must include the additive 25% jitter band (deterministic __random = 1)"
  );

  const state = await ctx.storage.get("rate-state");
  assert.equal(state.starts.length, 6, "every upstream attempt is counted exactly once");
  assert.equal(rateGate.activeTicket, null, "the queue slot is released after retries are exhausted");
});

test("NQP-P2-06: an oversized Retry-After ceases retries immediately without retrying early", async () => {
  const clock = new FakeClock(1_000);
  const ctx = createMockCtx();

  const env = {
    RPM: 1000,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 1,
    __fetch: async () =>
      new Response("", {
        status: 429,
        headers: { "Retry-After": "1000000000" },
      }),
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(res.headers.get("Retry-After"), "1000000000");
  await res.text();

  assert.equal(
    clock.sleepCalls.length,
    0,
    "must not retry early when Retry-After exceeds MAX_BACKOFF_MS"
  );
  assert.equal(rateGate.activeTicket, null, "the queue slot is released");
});

test("NQP-P3-01: applyBackoffJitter normalizes invalid random sources and stays within bounds", () => {
  const maxBackoff = 30_000;
  const delay = 8000;
  const lo = delay;
  const hi = maxBackoff;

  // Invalid random sources must never yield a NaN/non-finite delay and must
  // remain within [delay, maxBackoff]. The documented fallback is 1 (upper
  // bound), so each of these produces delay + full 25% band = 10000.
  for (const [label, value] of [
    ["NaN", NaN],
    ["undefined", undefined],
    ["Infinity", Infinity],
    ["-Infinity", -Infinity],
    ["null", null],
    ["negative", -0.5],
    ["above one", 1.5],
  ]) {
    const out = applyBackoffJitter(delay, () => value, maxBackoff);

    assert.ok(Number.isFinite(out), `random=${label} must produce a finite delay (got ${out})`);
    assert.ok(out >= lo && out <= hi, `random=${label} must stay within [${lo}, ${hi}] (got ${out})`);
    assert.equal(out, 10_000, `random=${label} must use the documented fallback of 1`);
  }

  // Valid lower and upper bounds must be used exactly.
  assert.equal(applyBackoffJitter(delay, () => 0, maxBackoff), lo, "random=0 yields the base delay");
  assert.equal(applyBackoffJitter(delay, () => 1, maxBackoff), 10_000, "random=1 yields the full band");

  // The random source is sampled exactly once per call.
  let calls = 0;
  applyBackoffJitter(delay, () => {
    calls++;
    return 0.5;
  }, maxBackoff);
  assert.equal(calls, 1, "the injected random source must be called exactly once");

  // Zero and cap semantics are preserved.
  assert.equal(applyBackoffJitter(0, () => NaN, maxBackoff), 0, "a zero delay stays zero even with an invalid source");
  assert.equal(applyBackoffJitter(maxBackoff, () => NaN, maxBackoff), maxBackoff, "a delay at the cap stays at the cap");
});

// ==========================================
// AUDIT-01: Bound Pre-Admission Request-Body Memory & Concurrency
// ==========================================

test("AUDIT-01: resolveConfig parses MAX_CONCURRENT_BODY_READS and MAX_AGGREGATE_BODY_BYTES", () => {
  const defaults = resolveConfig({});
  assert.equal(defaults.MAX_CONCURRENT_BODY_READS, 100);
  assert.equal(defaults.MAX_AGGREGATE_BODY_BYTES, 32 * 1024 * 1024);

  const custom = resolveConfig({
    MAX_CONCURRENT_BODY_READS: "5",
    MAX_AGGREGATE_BODY_BYTES: "16777216",
  });
  assert.equal(custom.MAX_CONCURRENT_BODY_READS, 5);
  assert.equal(custom.MAX_AGGREGATE_BODY_BYTES, 16_777_216);

  assert.throws(() => resolveConfig({ MAX_CONCURRENT_BODY_READS: 0 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_CONCURRENT_BODY_READS: -1 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_CONCURRENT_BODY_READS: "abc" }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_AGGREGATE_BODY_BYTES: 0 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_AGGREGATE_BODY_BYTES: 500 }), ConfigError);
  assert.throws(() => resolveConfig({ MAX_AGGREGATE_BODY_BYTES: 100 * 1024 * 1024 }), ConfigError);
});

test("AUDIT-01: saturated concurrency rejects excess upload with 503 and Retry-After", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 1670,
    MAX_CONCURRENT_BODY_READS: 1,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  let unblockFirst;
  const firstBlocker = new Promise((resolve) => {
    unblockFirst = resolve;
  });

  const slowStream = new ReadableStream({
    async start(controller) {
      await firstBlocker;
      controller.enqueue(new TextEncoder().encode("{}"));
      controller.close();
    },
  });

  // Request 1 starts reading and holds the single upload slot
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: slowStream,
      duplex: "half",
    })
  );

  await tick();

  // Request 2 arrives while Request 1 is still reading -> rejected immediately
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );

  assert.equal(res2.status, 503);
  assert.equal(res2.headers.get("Retry-After"), "2");
  const data2 = await res2.json();
  assert.equal(data2.error, "Too many concurrent uploads, please retry later");

  // Unblock request 1
  unblockFirst();
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  // Once request 1 finishes, request 3 is admitted cleanly
  const res3 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );
  assert.equal(res3.status, 200);
  await res3.text();
});

test("AUDIT-01: saturated aggregate memory rejects excess request with 503", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_AGGREGATE_BODY_BYTES: 1024, // 1 KiB aggregate limit for test
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  // Request with Content-Length larger than maxAggregateBodyBytes is rejected immediately
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "2048" },
      body: "x",
    })
  );

  assert.equal(res.status, 503);
  const data = await res.json();
  assert.equal(data.error, "Too many concurrent uploads, please retry later");
  assert.equal(rateGate.activeBodyReads, 0, "activeBodyReads must remain 0");
  assert.equal(rateGate.activeBodyBytes, 0, "activeBodyBytes must remain 0");
});

test("AUDIT-01: body read errors decrement active upload counters", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 100,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  // Oversized body triggers BodyTooLargeError (413)
  const res413 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "x".repeat(200),
    })
  );
  assert.equal(res413.status, 413);
  assert.equal(rateGate.activeBodyReads, 0);
  assert.equal(rateGate.activeBodyBytes, 0);
});

// ==========================================
// AUDIT-05: Respect Upstream Retry-After Without Retrying Early
// ==========================================

test("AUDIT-05: parseRetryAfterMs parses numeric and HTTP-date forms", () => {
  const baseTime = 1_000_000;
  const mk = (v) => new Response("", { headers: { "Retry-After": v } });

  assert.equal(parseRetryAfterMs(null, baseTime), null);
  assert.equal(parseRetryAfterMs(new Response(""), baseTime), null);
  assert.equal(parseRetryAfterMs(mk(""), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("   "), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("invalid"), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("NaN"), baseTime), null);

  assert.equal(parseRetryAfterMs(mk("0"), baseTime), 0);
  assert.equal(parseRetryAfterMs(mk("-5"), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("5"), baseTime), 5000);
  assert.equal(parseRetryAfterMs(mk("60"), baseTime), 60_000);
  assert.equal(parseRetryAfterMs(mk("1.5"), baseTime), null, "fractional seconds must return null per RFC 7231");
  // Non-digit characters are rejected strictly per RFC 7231 delay-seconds = 1*DIGIT.
  assert.equal(parseRetryAfterMs(mk("1e400"), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("+5"), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("5abc"), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("abc5"), baseTime), null);
  assert.equal(parseRetryAfterMs(mk("Infinity"), baseTime), null);

  const future = new Date(baseTime + 15_000).toUTCString();
  assert.equal(parseRetryAfterMs(mk(future), baseTime), 15_000);

  const past = new Date(baseTime - 5000).toUTCString();
  assert.equal(parseRetryAfterMs(mk(past), baseTime), 0);
});

test("AUDIT-05: 429 with Retry-After > MAX_BACKOFF_MS returns 429 immediately with no early retry", async () => {
  const clock = new FakeClock(10_000);
  const ctx = createMockCtx();
  let attempts = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0,
    __fetch: async () => {
      attempts++;
      return new Response(JSON.stringify({ error: "rate limit" }), {
        status: 429,
        headers: {
          "Retry-After": "60",
          "Content-Type": "application/json",
        },
      });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );

  assert.equal(res.status, 429);
  assert.equal(res.headers.get("Retry-After"), "60");
  assert.equal(attempts, 1, "must not attempt automatic retries when Retry-After exceeds 30s");
  assert.equal(clock.sleepCalls.length, 0, "must make zero sleep calls");
  assert.equal(rateGate.activeTicket, null, "queue slot must be released");
});

test("AUDIT-05: 429 with Retry-After <= MAX_BACKOFF_MS retries after requested delay", async () => {
  const clock = new FakeClock(10_000);
  const ctx = createMockCtx();
  let attempts = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0, // Zero jitter for exact floor assertion
    __fetch: async () => {
      attempts++;
      if (attempts === 1) {
        return new Response("429", {
          status: 429,
          headers: { "Retry-After": "5" },
        });
      }
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );

  assert.equal(res.status, 200);
  assert.equal(attempts, 2);
  assert.equal(clock.sleepCalls.length, 1);
  assert.equal(clock.sleepCalls[0].ms, 5000, "delay must equal the 5s Retry-After floor");
});

test("AUDIT-05: 429 with fractional Retry-After falls back to exponential backoff", async () => {
  const clock = new FakeClock(10_000);
  const ctx = createMockCtx();
  let attempts = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0, // Zero jitter
    __fetch: async () => {
      attempts++;
      if (attempts === 1) {
        return new Response("429", {
          status: 429,
          headers: { "Retry-After": "1.5" },
        });
      }
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );

  assert.equal(res.status, 200);
  assert.equal(attempts, 2);
  assert.equal(clock.sleepCalls.length, 1);
  // Retry 0 base delay is 1000ms (baseDelayMs * 2^0)
  assert.equal(clock.sleepCalls[0].ms, 1000, "fractional delay must fall back to exponential backoff");
});

test("AUDIT-01: invalid Content-Length header is rejected with 400 Bad Request", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  const invalidLengths = ["-5", "abc", "1.5", "", "   ", "10, 10"];
  for (const bad of invalidLengths) {
    const res = await rateGate.fetch(
      new Request("https://proxy.internal/v1/chat/completions", {
        method: "POST",
        headers: { "content-length": bad },
        body: "{}",
      })
    );
    assert.equal(res.status, 400, `Content-Length: ${bad} must be rejected with 400`);
    const data = await res.json();
    assert.equal(data.error, "Invalid Content-Length header");
  }

  assert.equal(rateGate.activeBodyReads, 0, "counters must remain 0");
  assert.equal(rateGate.activeBodyBytes, 0, "counters must remain 0");
});

test("AUDIT-01: declared Content-Length > MAX_BODY_BYTES returns 413 immediately without reserving memory", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 1024,
    MAX_AGGREGATE_BODY_BYTES: 4096,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "2048" },
      body: "x",
    })
  );

  assert.equal(res.status, 413);
  const data = await res.json();
  assert.equal(data.error, "Request body too large");
  assert.equal(rateGate.activeBodyReads, 0);
  assert.equal(rateGate.activeBodyBytes, 0);
});

test("AUDIT-01: understated Content-Length dynamically reserves memory and rejects when aggregate limit is exceeded", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_BODY_BYTES: 10 * 1024, // 10 KiB max per request
    MAX_AGGREGATE_BODY_BYTES: 1024, // 1 KiB aggregate limit
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  // Client claims Content-Length is only 10 bytes, but actually streams 2000 bytes
  const stream = new ReadableStream({
    start(controller) {
      // Chunk 1: 500 bytes (within aggregate limit of 1024)
      controller.enqueue(new Uint8Array(500));
      // Chunk 2: 600 bytes (exceeds aggregate limit: 500 + 600 = 1100 > 1024)
      controller.enqueue(new Uint8Array(600));
      controller.close();
    },
  });

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "10" },
      body: stream,
      duplex: "half",
    })
  );

  assert.equal(res.status, 503);
  const data = await res.json();
  assert.equal(data.error, "Too many concurrent uploads, please retry later");
  // Clean decrement on failure:
  assert.equal(rateGate.activeBodyReads, 0, "activeBodyReads must be restored to 0");
  assert.equal(rateGate.activeBodyBytes, 0, "activeBodyBytes must be restored to 0");
});

test("AUDIT-02: upstream reader.cancel rejection is caught cleanly and releases queue slot", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-1"));
        },
        cancel() {
          // Rejection during cancel
          return Promise.reject(new Error("socket destroyed"));
        },
      });
      return new Response(stream, { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
    })
  );

  const reader1 = res1.body.getReader();
  await reader1.read();

  // Cancel stream, triggering upstream reader.cancel() rejection
  await reader1.cancel("client abort");

  // Slot should be cleanly released without unhandled rejection
  assert.equal(rateGate.activeTicket, null, "slot must be released cleanly after cancel rejection");
});

// ==========================================
// BATCH 1 REVIEW — AUDIT-01 aggregate memory bound
// ==========================================
//
// These tests target the exact admission order: the declared Content-Length is
// validated, checked against MAX_BODY_BYTES, then reserved against the
// concurrency and aggregate-byte budgets in the same synchronous step before
// the body is read. They prove that concurrent large declared-length requests
// cannot be granted reservations whose total exceeds the aggregate budget, and
// that all reservations are released exactly once on every terminal path.

function makeAggregateEnv(overrides = {}) {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => new Response("ok", { status: 200 }),
    ...overrides,
  };
  return { clock, ctx, env };
}

// A body source that never yields a chunk until the test explicitly releases
// it, so a request stays admitted-but-not-yet-producing for as long as needed.
function createGatedStream() {
  let releaseGate;
  const gate = new Promise((resolve) => {
    releaseGate = resolve;
  });
  const stream = new ReadableStream(
    {
      async start(controller) {
        await gate;
        controller.enqueue(new TextEncoder().encode("{}"));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );
  return { stream, release: () => releaseGate() };
}

test("AUDIT-01: concurrent large declared-length requests cannot exceed the aggregate budget", async () => {
  // Aggregate budget 1024; each request declares 600. Only one may be admitted;
  // the second must be rejected before it can be read or dispatched.
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 1024,
    MAX_CONCURRENT_BODY_READS: 10,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const gated = createGatedStream();
  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "600" },
      body: gated.stream,
      duplex: "half",
    })
  );

  await tick();

  // While request 1 holds a 600-byte reservation (and has not produced any
  // chunk yet), request 2 declares another 600 -> 1200 > 1024 -> rejected.
  const res2 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "600" },
      body: "x".repeat(600),
    })
  );

  assert.equal(res2.status, 503, "the over-budget reservation must be rejected");
  assert.equal(
    (await res2.json()).error,
    "Too many concurrent uploads, please retry later"
  );

  // Only the single 600-byte reservation is held; the budget is never exceeded.
  assert.ok(
    rateGate.activeBodyBytes <= 1024,
    "aggregate byte reservation must never exceed the configured budget"
  );

  gated.release();
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  assert.equal(rateGate.activeBodyReads, 0, "counters restored after success");
  assert.equal(rateGate.activeBodyBytes, 0, "counters restored after success");
});

test("AUDIT-01: declared reservations summing exactly to the budget are both admitted", async () => {
  // Aggregate budget 1024; two requests of 512 each = exactly the budget.
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 1024,
    MAX_CONCURRENT_BODY_READS: 10,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const g1 = createGatedStream();
  const g2 = createGatedStream();

  const p1 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "512" },
      body: g1.stream,
      duplex: "half",
    })
  );
  await tick();

  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "512" },
      body: g2.stream,
      duplex: "half",
    })
  );
  await tick();

  assert.equal(rateGate.activeBodyReads, 2, "both requests admitted");
  assert.equal(
    rateGate.activeBodyBytes,
    1024,
    "reservations sum exactly to the budget, never above it"
  );

  g1.release();
  g2.release();

  // The queue slot is serialized, so drain request 1's response before
  // awaiting request 2 (whose upstream dispatch waits for that release).
  const res1 = await p1;
  assert.equal(res1.status, 200);
  await res1.text();

  const res2 = await p2;
  assert.equal(res2.status, 200);
  await res2.text();

  assert.equal(rateGate.activeBodyReads, 0);
  assert.equal(rateGate.activeBodyBytes, 0);
});

test("AUDIT-01: unknown-length chunked stream is accounted before retention and bounded", async () => {
  // No declared length. The aggregate budget is 1024; the stream produces
  // 600 + 600 = 1200 bytes. The second chunk must be refused before it is
  // retained, so the aggregate limit is enforced even without Content-Length.
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 1024,
    MAX_CONCURRENT_BODY_READS: 10,
    MAX_BODY_BYTES: 20 * 1024,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const stream = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array(600));
        controller.enqueue(new Uint8Array(600));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: stream,
      duplex: "half",
    })
  );

  assert.equal(res.status, 503, "chunked overflow is rejected");
  assert.equal(
    (await res.json()).error,
    "Too many concurrent uploads, please retry later"
  );
  assert.equal(rateGate.activeBodyReads, 0, "counters restored after rejection");
  assert.equal(rateGate.activeBodyBytes, 0, "counters restored after rejection");
});

test("AUDIT-01: a request rejected before admission never acquires a queue slot", async () => {
  const { clock, env } = makeAggregateEnv({
    MAX_BODY_BYTES: 1024,
    MAX_AGGREGATE_BODY_BYTES: 4096,
  });
  let fetchCalls = 0;
  env.__fetch = async () => {
    fetchCalls++;
    return new Response("ok", { status: 200 });
  };
  const rateGate = new RateGate(createMockCtx(), env);

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "2048" },
      body: "x".repeat(2048),
    })
  );

  assert.equal(res.status, 413);
  assert.equal(rateGate.activeTicket, null, "no queue slot acquired");
  assert.equal(rateGate.waitingQueue.length, 0, "nothing queued");
  assert.equal(fetchCalls, 0, "upstream was never contacted");
  assert.equal(rateGate.activeBodyReads, 0);
  assert.equal(rateGate.activeBodyBytes, 0);
});

test("AUDIT-01: reservations are released exactly once after a body-read timeout", async () => {
  const timers = createFakeTimers();
  const { clock, env } = makeAggregateEnv({
    REQUEST_BODY_TIMEOUT_MS: 1000,
    MAX_AGGREGATE_BODY_BYTES: 4096,
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
  });
  const rateGate = new RateGate(createMockCtx(), env);

  // A body that never completes: the deadline is the only thing that fires.
  const stalled = new ReadableStream(
    {
      pull() {
        /* never enqueues */
      },
    },
    { highWaterMark: 0 }
  );

  const p = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "10" },
      body: stalled,
      duplex: "half",
    })
  );

  await tick();
  assert.equal(rateGate.activeBodyReads, 1, "reservation held while reading");

  // Advance the virtual clock past the body deadline.
  timers.advance(1000);

  const res = await p;
  assert.equal(res.status, 408, "body read timeout surfaces as 408");
  assert.equal(rateGate.activeBodyReads, 0, "reservation released exactly once");
  assert.equal(rateGate.activeBodyBytes, 0, "byte reservation released");
});

test("AUDIT-01: reservations are released exactly once after a client abort", async () => {
  const { env } = makeAggregateEnv({ MAX_AGGREGATE_BODY_BYTES: 4096 });
  const rateGate = new RateGate(createMockCtx(), env);

  const controller = new AbortController();

  // Simulate the runtime tying the request signal to the body stream: on abort
  // the pending read rejects, exactly as an aborted upstream body would.
  const stalled = new ReadableStream(
    {
      start(streamController) {
        controller.signal.addEventListener(
          "abort",
          () => streamController.error(new Error("client aborted")),
          { once: true }
        );
      },
      pull() {
        /* never enqueues */
      },
      cancel() {
        /* cooperative */
      },
    },
    { highWaterMark: 0 }
  );

  const p = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      signal: controller.signal,
      headers: { "content-length": "10" },
      body: stalled,
      duplex: "half",
    })
  );

  await tick();
  assert.equal(rateGate.activeBodyReads, 1, "reservation held while reading");

  controller.abort("client gone");

  const res = await p;
  assert.equal(res.status, 499, "abort surfaces as 499");
  assert.equal(rateGate.activeBodyReads, 0, "reservation released exactly once");
  assert.equal(rateGate.activeBodyBytes, 0, "byte reservation released");
});

test("AUDIT-01: an absent Content-Length is treated as chunked and never reserves more than the budget", async () => {
  // Budget 1000, per-request limit 1000. A body delivered as a single 1500-byte
  // chunk exceeds the per-request limit; the read fails before retention.
  const { env } = makeAggregateEnv({
    MAX_BODY_BYTES: 1000,
    MAX_AGGREGATE_BODY_BYTES: 4096,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const stream = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array(1500));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: stream,
      duplex: "half",
    })
  );

  assert.equal(res.status, 413, "per-request limit enforced without Content-Length");
  assert.equal(rateGate.activeBodyReads, 0);
  assert.equal(rateGate.activeBodyBytes, 0);
});

// ==========================================
// BATCH 1 REVIEW — AUDIT-01 aggregate memory bound
// ==========================================
//
// These tests target the quantitative memory policy: admissions must reserve
// declared capacity atomically, committed retained bytes must never exceed the
// aggregate budget, chunked streams must be bounded per chunk, and capacity
// must be released so a valid request can succeed afterwards.

test("AUDIT-01: committed retained bytes never exceed the aggregate budget when many chunked streams are all admitted before any yields a chunk", async () => {
  // Every request is unknown-length, so all pass admission with a zero-byte
  // reservation. They are then released together so each has a chunk in flight
  // before accounting runs. Committed bytes must still never exceed the budget.
  const BUDGET = 1024;
  const CHUNK = 600;
  const N = 8;
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: BUDGET,
    MAX_CONCURRENT_BODY_READS: 1000,
    MAX_BODY_BYTES: 100 * 1024,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  // Observe the peak committed (accounted) retained bytes.
  let peakCommitted = 0;
  let stored = 0;
  Object.defineProperty(rateGate, "activeBodyBytes", {
    get() {
      return stored;
    },
    set(value) {
      stored = value;
      if (value > peakCommitted) peakCommitted = value;
    },
    configurable: true,
  });

  const pulls = [];
  const streams = [];
  for (let i = 0; i < N; i++) {
    streams.push(
      new ReadableStream(
        {
          pull(controller) {
            pulls.push(() => {
              controller.enqueue(new Uint8Array(CHUNK));
              controller.close();
            });
          },
        },
        { highWaterMark: 0 }
      )
    );
  }

  const promises = streams.map((body) =>
    rateGate.fetch(
      new Request("https://proxy.internal/v1/chat/completions", {
        method: "POST",
        body,
        duplex: "half",
      })
    )
  );

  // Let all N requests reach admission and issue their first read.
  await tick();
  await tick();
  await tick();
  assert.equal(pulls.length, N, "all requests admitted and awaiting a chunk");
  assert.equal(rateGate.activeBodyBytes, 0, "unknown-length admissions reserve nothing");

  // Release every chunk "simultaneously".
  pulls.forEach((p) => p());

  const results = await Promise.all(promises);
  const statuses = results.map((r) => r.status);

  assert.ok(
    statuses.includes(200),
    "at least one request fits within the budget"
  );
  assert.ok(
    peakCommitted <= BUDGET,
    `committed retained bytes (${peakCommitted}) must never exceed the budget (${BUDGET})`
  );
  assert.equal(rateGate.activeBodyBytes, 0, "all reservations released");
  assert.equal(rateGate.activeBodyReads, 0, "all reader reservations released");
});

test("AUDIT-01: a chunk exceeding the remaining aggregate capacity is rejected without retention", async () => {
  const BUDGET = 1200;
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: BUDGET,
    MAX_CONCURRENT_BODY_READS: 100,
    MAX_BODY_BYTES: 100 * 1024,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  // First request reserves 700 bytes of the 1000 budget (declared length).
  const holderGate = createGatedStreamWithSize(700);
  const holder = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "700" },
      body: holderGate.stream,
      duplex: "half",
    })
  );
  await tick();
  assert.equal(rateGate.activeBodyBytes, 700, "700 reserved by the holder");

  // A chunked request delivers a single 600-byte chunk: 700 + 600 = 1300 > 1200
  // so it must be rejected at accounting time and never retained.
  const overflow = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array(600));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: overflow,
      duplex: "half",
    })
  );

  assert.equal(res.status, 503, "the over-budget chunk is rejected");
  assert.equal(
    (await res.json()).error,
    "Too many concurrent uploads, please retry later"
  );
  assert.equal(
    rateGate.activeBodyBytes,
    700,
    "the rejected chunk's bytes were never retained"
  );

  holderGate.release();
  const holderRes = await holder;
  assert.equal(holderRes.status, 200);
});

test("AUDIT-01: a valid request succeeds after the previous reservation is released", async () => {
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 1024,
    MAX_CONCURRENT_BODY_READS: 1,
  });
  let fetchCount = 0;
  env.__fetch = async () => {
    fetchCount++;
    return new Response("ok", { status: 200 });
  };
  const rateGate = new RateGate(createMockCtx(), env);

  const first = await rateGate.fetch(chatRequest("x".repeat(100)));
  assert.equal(first.status, 200, "first request succeeds");
  assert.equal(fetchCount, 1, "first request reached upstream");
  assert.equal(rateGate.activeBodyReads, 0, "first reservation released");
  assert.equal(rateGate.activeBodyBytes, 0, "first bytes released");
  // Drain the response so the inference execution slot is released before the
  // next request is admitted.
  await first.text();

  // With the reservation released, a second request must be admitted.
  const second = await rateGate.fetch(chatRequest("y".repeat(100)));
  assert.equal(second.status, 200, "second request succeeds after release");
  assert.equal(fetchCount, 2, "second request reached upstream");
  assert.equal(rateGate.activeBodyReads, 0);
  assert.equal(rateGate.activeBodyBytes, 0);
  await second.text();
});

test("AUDIT-01: a duplicated Content-Length header is rejected and cannot bypass admission", async () => {
  // A duplicated Content-Length is combined by the runtime into "10, 10",
  // which is not a valid delay-seconds-style integer. The proxy must reject it
  // rather than trusting either value, so it cannot bypass the size gates.
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 1024,
    MAX_CONCURRENT_BODY_READS: 100,
    MAX_BODY_BYTES: 100 * 1024,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const headers = new Headers();
  headers.append("content-length", "10");
  headers.append("content-length", "10");

  const stream = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array(600));
        controller.enqueue(new Uint8Array(600));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers,
      body: stream,
      duplex: "half",
    })
  );

  assert.equal(res.status, 400, "a malformed duplicated Content-Length is rejected");
  assert.equal(
    (await res.json()).error,
    "Invalid Content-Length header"
  );
  assert.equal(rateGate.activeBodyReads, 0, "no reader reservation was taken");
  assert.equal(rateGate.activeBodyBytes, 0, "no byte reservation was taken");
});

test("AUDIT-01: multi-chunk bodies are assembled correctly and counted once", async () => {
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 4096,
    MAX_BODY_BYTES: 4096,
  });
  let receivedBody = null;
  env.__fetch = async (_url, opts) => {
    receivedBody = opts.body instanceof Uint8Array ? opts.body : null;
    return new Response("ok", { status: 200 });
  };
  const rateGate = new RateGate(createMockCtx(), env);

  const stream = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2, 3]));
        controller.enqueue(new Uint8Array([4, 5]));
        controller.enqueue(new Uint8Array([6]));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );

  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: stream,
      duplex: "half",
    })
  );

  assert.equal(res.status, 200, "multi-chunk body is accepted");
  assert.deepEqual(
    Array.from(receivedBody),
    [1, 2, 3, 4, 5, 6],
    "assembled body preserves chunk order and content"
  );
  assert.equal(rateGate.activeBodyReads, 0, "reservation released after success");
  assert.equal(rateGate.activeBodyBytes, 0, "bytes released after success");
});

// A gated stream that declares a size via a buffered chunk, used to hold a
// reservation without completing the body until explicitly released.
function createGatedStreamWithSize(size) {
  let releaseGate;
  const gate = new Promise((resolve) => {
    releaseGate = resolve;
  });
  const stream = new ReadableStream(
    {
      async start(controller) {
        await gate;
        controller.enqueue(new Uint8Array(size));
        controller.close();
      },
    },
    { highWaterMark: 0 }
  );
  return { stream, release: () => releaseGate() };
}

// ==========================================
// BATCH 1 REVIEW — AUDIT-02 upstream cancellation
// ==========================================
//
// The upstream reader.cancel() is raced against a bounded grace period. The
// downstream cancel() must always resolve. Crucially, the concurrency slot is
// NOT released merely because the grace timer elapsed: if cancellation never
// settles, the slot is preserved (fail-closed) until the upstream actually
// terminates, so a queued request cannot overlap a possibly-active upstream.

test("AUDIT-02: upstream reader.cancel() resolving releases the slot", async () => {
  const { env } = makeAggregateEnv();
  let upstreamCancelReason = null;

  env.__fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-1"));
        },
        cancel(reason) {
          upstreamCancelReason = reason;
          return Promise.resolve();
        },
      }),
      { status: 200 }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());
  const reader = res.body.getReader();
  await reader.read();

  await reader.cancel("client abort");

  assert.equal(
    upstreamCancelReason,
    "client abort",
    "the upstream reader.cancel() must receive the downstream reason"
  );
  assert.equal(rateGate.activeTicket, null, "slot released after cooperative cancel");
});

test("AUDIT-02: upstream reader.cancel() rejecting still releases the slot without unhandled rejection", async () => {
  const { env } = makeAggregateEnv();

  env.__fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-1"));
        },
        cancel() {
          return Promise.reject(new Error("socket destroyed"));
        },
      }),
      { status: 200 }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());
  const reader = res.body.getReader();
  await reader.read();

  await reader.cancel("client abort");

  assert.equal(
    rateGate.activeTicket,
    null,
    "a rejecting cancel() must not block slot release"
  );
});

test("AUDIT-02: a never-settling reader.cancel() still resolves the downstream cancel() within the bounded grace", async () => {
  const timers = createFakeTimers();
  const { env } = makeAggregateEnv({
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
  });

  env.__fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-1"));
        },
        cancel() {
          // Never settles: the upstream ignores cancellation.
          return new Promise(() => {});
        },
      }),
      { status: 200 }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());
  const reader = res.body.getReader();
  await reader.read();

  let cancelResolved = false;
  const cancelResult = reader
    .cancel("client abort")
    .then(() => {
      cancelResolved = true;
    });

  await tick();
  assert.equal(
    cancelResolved,
    false,
    "the downstream cancel() must not resolve before the grace period"
  );

  // Fire the bounded cancellation grace timer.
  timers.advance(10_000);

  await cancelResult;
  assert.equal(
    cancelResolved,
    true,
    "the downstream cancel() must resolve once the bounded grace elapses"
  );

  // Fail-closed: the slot must NOT be released on the grace timer alone.
  assert.notEqual(
    rateGate.activeTicket,
    null,
    "the slot must be held while the upstream may still be active"
  );
});

test("AUDIT-02: the slot is released once the upstream stream confirms termination, regardless of a never-settling cancel()", async () => {
  const timers = createFakeTimers();
  const { env } = makeAggregateEnv({
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
  });

  // The upstream stream yields one chunk and then ends (done). Its cancel()
  // never settles, so the queued request must rely on the upstream actually
  // reporting termination before the slot can be released.
  env.__fetch = async () =>
    new Response(
      new ReadableStream(
        {
          start(controller) {
            controller.enqueue(new TextEncoder().encode("chunk-1"));
            controller.close();
          },
          cancel() {
            return new Promise(() => {});
          },
        },
        { highWaterMark: 0 }
      ),
      { status: 200 }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());
  const reader = res.body.getReader();

  // Drain the stream. When the upstream reports done, the proxy confirms
  // termination and releases the slot even though cancel() never settles.
  let chunk = await reader.read();
  while (!chunk.done) {
    chunk = await reader.read();
  }

  for (let i = 0; i < 100 && rateGate.activeTicket !== null; i++) {
    await tick();
  }

  assert.equal(
    rateGate.activeTicket,
    null,
    "a confirmed upstream termination releases the slot"
  );
  assert.equal(rateGate.activeBodyReads, 0, "no body reservations leak");
});

test("AUDIT-02: a queued request waits behind a never-settling cancellation and is not dispatched while upstream may be active", async () => {
  const timers = createFakeTimers();
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let op2Dispatched = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    // The queue-wait timer uses the real event loop; keep it short so op2 is
    // released promptly once it is clear the slot will not free up.
    MAX_QUEUE_WAIT_MS: 50,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async (url, opts) => {
      const tag = opts.headers.get("x-tag");
      if (tag === "op1") {
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("chunk-1"));
            },
            cancel() {
              return new Promise(() => {});
            },
          }),
          { status: 200 }
        );
      }
      op2Dispatched = true;
      return new Response("ok-2", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-tag": "op1" },
      body: "{}",
    })
  );
  const reader1 = res1.body.getReader();
  await reader1.read();

  const cancelP = reader1.cancel("client abort");
  await tick();

  // op2 queues behind op1's held slot.
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "x-tag": "op2" },
      body: "{}",
    })
  );

  await tick();

  // Even after the cancellation grace elapses, op2 must NOT be dispatched
  // while the upstream may still be active (fail-closed).
  timers.advance(10_000);
  await cancelP;
  await tick();
  await tick();

  assert.equal(
    op2Dispatched,
    false,
    "a queued request must not overlap a possibly-active upstream"
  );
  assert.equal(rateGate.waitingQueue.length, 1, "op2 remains queued");

  // The real queue-wait timer releases op2 with a 503 without ever
  // dispatching it to the (possibly still active) upstream.
  const res2 = await p2;
  assert.equal(res2.status, 503, "op2 is released with 503 at its queue-wait limit");
});

test("AUDIT-02: repeated concurrent completion and abort events release the slot exactly once", async () => {
  const { env } = makeAggregateEnv();
  let releaseCount = 0;

  env.__fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("chunk-1"));
        },
        cancel() {
          return Promise.resolve();
        },
      }),
      { status: 200 }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const originalRelease = rateGate.releaseSlot.bind(rateGate);
  rateGate.releaseSlot = () => {
    releaseCount += 1;
    return originalRelease();
  };

  const res = await rateGate.fetch(chatRequest());
  const reader = res.body.getReader();
  await reader.read();

  // Fire downstream cancel and an independent completion attempt concurrently.
  await Promise.all([
    reader.cancel("client abort"),
    Promise.resolve().then(() => tick()),
  ]);

  assert.equal(releaseCount, 1, "releaseSlot must run exactly once");
  assert.equal(rateGate.activeTicket, null, "slot is free");
});

// ==========================================
// BATCH 1 REVIEW — AUDIT-02 late upstream termination after a held grace
// ==========================================

// Regression for the re-entrant finish() path: when cancellation does not
// settle within the bounded grace, the slot is intentionally held
// (fail-closed). If the upstream then *does* confirm the cancellation after
// the grace elapses, finish() must re-enter and release the held slot rather
// than leaving it stuck forever.
test("AUDIT-02: a held slot is released when cancellation settles after the grace elapses", async () => {
  const timers = createFakeTimers();
  const clock = new FakeClock(0);
  const ctx = createMockCtx();

  let releaseCancel;
  const cancelGate = new Promise((resolve) => {
    releaseCancel = resolve;
  });

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    // The upstream yields one chunk, then ignores cancellation until the test
    // explicitly releases it. This forces the proxy's bounded grace to elapse
    // first, after which the upstream finally confirms termination.
    __fetch: async () =>
      new Response(
        new ReadableStream(
          {
            start(controller) {
              controller.enqueue(new TextEncoder().encode("chunk-1"));
            },
            cancel() {
              return cancelGate;
            },
          },
          { highWaterMark: 0 }
        ),
        { status: 200 }
      ),
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());
  const reader = res.body.getReader();

  await reader.read(); // consume chunk-1

  const cancelP = reader.cancel("client abort");
  await tick();

  // The bounded grace has not elapsed yet: cancel() must still be pending and
  // the slot held.
  assert.notEqual(
    rateGate.activeTicket,
    null,
    "the slot is held while cancellation is unsettled"
  );

  // Elapse the cancellation grace: the downstream cancel() resolves, but the
  // slot stays held because cancellation has not settled.
  timers.advance(10_000);
  await cancelP;

  assert.notEqual(
    rateGate.activeTicket,
    null,
    "fail-closed: the slot must remain held after the grace elapses"
  );

  // Now the upstream finally confirms the cancellation. This is positive
  // evidence the upstream is no longer active, so the held slot must be
  // released.
  releaseCancel();

  for (let i = 0; i < 100 && rateGate.activeTicket !== null; i++) {
    await tick();
  }

  assert.equal(
    rateGate.activeTicket,
    null,
    "a late cancellation confirmation must release the previously held slot"
  );
});

// ==========================================
// BATCH 1 REVIEW — AUDIT-05 Retry-After semantics
// ==========================================

test("AUDIT-05: malformed Retry-After values follow the documented fallback, valid ones are capped", () => {
  const now = 1_000_000;
  const mk = (v) => new Response("429", { headers: { "Retry-After": v } });

  // Malformed per RFC 7231 delay-seconds = 1*DIGIT -> fallback (0).
  for (const bad of ["5abc", "abc5", "+5", "1e2", "1E2", "Infinity", "NaN", "1.5", "2.0", "-5", "5,5", ""]) {
    assert.equal(retryAfterMs(mk(bad), now), 0, `malformed "${bad}" must fall back`);
  }

  // Valid integer seconds -> honored, then capped at MAX_BACKOFF_MS.
  assert.equal(retryAfterMs(mk("5"), now), 5000);
  assert.equal(retryAfterMs(mk("999999999"), now), 30_000, "valid huge value is capped, not rejected");
});

test("AUDIT-05: parseRetryAfterMs rejects fractional and non-digit delay-seconds", () => {
  const now = 1_000_000;
  const mk = (v) => new Response("", { headers: { "Retry-After": v } });

  assert.equal(parseRetryAfterMs(mk("1.5"), now), null);
  assert.equal(parseRetryAfterMs(mk("2.0"), now), null);
  assert.equal(parseRetryAfterMs(mk("5abc"), now), null);
  assert.equal(parseRetryAfterMs(mk("1e2"), now), null);
  assert.equal(parseRetryAfterMs(mk("+5"), now), null);
  assert.equal(parseRetryAfterMs(mk("Infinity"), now), null);
  assert.equal(parseRetryAfterMs(mk("5"), now), 5000);
  assert.equal(parseRetryAfterMs(mk("0"), now), 0);
});

test("AUDIT-05: the original valid Retry-After survives downstream sanitization on the final 429", async () => {
  const { clock, env } = makeAggregateEnv({ __random: () => 0 });
  let attempts = 0;
  env.__fetch = async () => {
    attempts++;
    return new Response(JSON.stringify({ error: "rate limit" }), {
      status: 429,
      headers: {
        "Retry-After": "60",
        "Content-Type": "application/json",
        Connection: "keep-alive",
        "Transfer-Encoding": "chunked",
      },
    });
  };

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(res.headers.get("Retry-After"), "60", "valid Retry-After is preserved downstream");
  assert.equal(res.headers.get("connection"), null, "hop-by-hop headers are still stripped");
  assert.equal(res.headers.get("transfer-encoding"), null, "hop-by-hop headers are still stripped");
  assert.equal(attempts, 1, "no early retry when Retry-After exceeds MAX_BACKOFF_MS");
  assert.equal(rateGate.activeTicket, null, "slot released exactly once");
});

test("AUDIT-05: cancellation during the retry backoff does not trigger a retry after the request finished", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  let attempts = 0;
  const controller = new AbortController();

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    // Abort the request while sleeping in the retry backoff.
    __sleep: (ms, signal) =>
      new Promise((resolve, reject) => {
        controller.abort("client gone");
        if (signal?.aborted) {
          reject(signal.reason || new Error("Aborted"));
          return;
        }
        resolve();
      }),
    __random: () => 0,
    __fetch: async () => {
      attempts++;
      return new Response("429", { status: 429, headers: { "Retry-After": "1" } });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "{}",
      signal: controller.signal,
    })
  );

  assert.equal(res.status, 499, "cancellation during backoff returns 499");
  assert.equal(attempts, 1, "no further upstream attempt after cancellation");
  assert.equal(rateGate.activeTicket, null, "slot released exactly once");
});

test("AUDIT-05: a 429 exhausted retry path releases the slot exactly once", async () => {
  const { env } = makeAggregateEnv({ __random: () => 0 });
  let attempts = 0;
  env.__fetch = async () => {
    attempts++;
    return new Response("", { status: 429 }); // no Retry-After -> exponential backoff
  };

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(attempts, 6, "initial attempt plus 5 retries");
  assert.equal(rateGate.activeTicket, null, "slot released exactly once after exhaustion");
  assert.equal(rateGate.waitingQueue.length, 0, "no lingering waiters");
});

// ==========================================
// BATCH 2 — AUDIT-07 upstream response-header sanitization
// ==========================================

test("AUDIT-07: safeResponseHeaders removes hop-by-hop, sensitive, and Connection-nominated headers", () => {
  const upstream = new Headers({
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    "x-custom-upstream": "keep-me",
    connection: "keep-alive, x-internal-trace",
    "keep-alive": "timeout=5",
    "transfer-encoding": "chunked",
    "x-internal-trace": "secret-trace-id",
    "proxy-authenticate": "Basic realm=x",
    "set-cookie": "session=abc; HttpOnly",
    authorization: "Bearer upstream-secret",
    "content-length": "1234",
  });

  const safe = safeResponseHeaders(upstream);

  assert.equal(safe.get("content-type"), "text/event-stream", "content-type is preserved");
  assert.equal(safe.get("cache-control"), "no-cache", "cache-control is preserved");
  assert.equal(safe.get("x-custom-upstream"), "keep-me", "unknown upstream headers are preserved (transparent passthrough)");

  for (const removed of [
    "connection",
    "keep-alive",
    "transfer-encoding",
    "proxy-authenticate",
    "authorization",
    "set-cookie",
    "content-length",
    "x-internal-trace",
  ]) {
    assert.equal(safe.get(removed), null, `${removed} must not be forwarded`);
  }
});

test("AUDIT-07: safeResponseHeaders removes only headers named by Connection and keeps the rest", () => {
  const upstream = new Headers({
    "content-type": "application/json",
    connection: "X-Hop-One, x-hop-two",
    "x-hop-one": "1",
    "x-hop-two": "2",
    "x-keep": "3",
  });

  const safe = safeResponseHeaders(upstream);

  assert.equal(safe.get("x-hop-one"), null, "Connection-nominated header is removed");
  assert.equal(safe.get("x-hop-two"), null, "Connection-nominated header is removed (case-insensitive)");
  assert.equal(safe.get("x-keep"), "3", "unrelated header survives");
  assert.equal(safe.get("content-type"), "application/json");
});

test("AUDIT-07: a successful proxied response forwards safe headers and drops sensitive ones", async () => {
  const { env } = makeAggregateEnv();
  env.__fetch = async () =>
    new Response("hello", {
      status: 200,
      headers: {
        "content-type": "text/plain",
        "cache-control": "max-age=60",
        "x-request-id": "abc-123",
        "set-cookie": "sid=leak",
        connection: "x-secret-relay",
        "x-secret-relay": "should-not-appear",
        "content-length": "5",
      },
    });

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 200);
  assert.equal(res.headers.get("content-type"), "text/plain");
  assert.equal(res.headers.get("cache-control"), "max-age=60");
  assert.equal(res.headers.get("x-request-id"), "abc-123");
  assert.equal(res.headers.get("set-cookie"), null, "upstream cookie must not leak");
  assert.equal(res.headers.get("x-secret-relay"), null, "Connection-nominated header must not leak");
  assert.equal(res.headers.get("content-length"), null, "content-length is never forwarded");
  assert.equal(await res.text(), "hello", "body is forwarded unchanged");
  assert.equal(rateGate.activeTicket, null, "slot released after completion");
});

test("AUDIT-07: a streaming upstream response keeps streaming headers and drops sensitive ones", async () => {
  const { env } = makeAggregateEnv();
  env.__fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("data: a\n\n"));
          controller.close();
        },
      }),
      {
        status: 200,
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          connection: "keep-alive, x-internal",
          "x-internal": "internal-only",
          "transfer-encoding": "chunked",
          "content-length": "20",
        },
      }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 200);
  assert.equal(res.headers.get("content-type"), "text/event-stream");
  assert.equal(res.headers.get("cache-control"), "no-cache");
  assert.equal(res.headers.get("x-internal"), null, "Connection-nominated header is stripped");
  assert.equal(res.headers.get("transfer-encoding"), null, "hop-by-hop transfer-encoding is stripped");
  assert.equal(res.headers.get("content-length"), null, "content-length is never forwarded on a stream");
  assert.equal(await res.text(), "data: a\n\n", "streamed body is delivered intact");
});

test("AUDIT-07: the 429 retry path sanitizes upstream headers before returning the body", async () => {
  const { env } = makeAggregateEnv({ __random: () => 0 });
  let attempts = 0;
  env.__fetch = async () => {
    attempts++;
    return new Response(JSON.stringify({ error: "slow down" }), {
      status: 429,
      headers: {
        "content-type": "application/json",
        "retry-after": "1",
        "x-custom-upstream": "keep-me",
        "set-cookie": "sid=leak",
        connection: "x-listed",
        "x-listed": "nope",
        "content-length": "24",
      },
    });
  };

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 429);
  assert.equal(res.headers.get("content-type"), "application/json");
  assert.equal(res.headers.get("x-custom-upstream"), "keep-me", "safe custom header preserved on 429");
  assert.equal(res.headers.get("retry-after"), "1", "valid Retry-After is preserved");
  assert.equal(res.headers.get("set-cookie"), null, "upstream cookie must not leak on 429");
  assert.equal(res.headers.get("x-listed"), null, "Connection-nominated header must not leak on 429");
  assert.equal(res.headers.get("content-length"), null, "content-length is never forwarded");
  assert.equal(attempts, 6, "retry behavior is unchanged");
});

test("AUDIT-07: the proxy's own error responses are not polluted by upstream headers", async () => {
  const { env } = makeAggregateEnv();
  // Upstream returns 200 but with an Authorization echo and a cookie.
  env.__fetch = async () =>
    new Response("ok", {
      status: 200,
      headers: {
        "content-type": "text/plain",
        authorization: "Bearer upstream-secret",
        "set-cookie": "sid=leak",
      },
    });

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 200);
  assert.equal(res.headers.get("authorization"), null, "upstream Authorization echo must not leak");
  assert.equal(res.headers.get("set-cookie"), null, "upstream cookie must not leak");
});

test("AUDIT-07: network-topology and request-chain disclosure headers are not forwarded", () => {
  const upstream = new Headers({
    "content-type": "application/json",
    "x-request-id": "req-1",
    forwarded: "for=203.0.113.7;proto=https",
    "x-forwarded-for": "203.0.113.7, 10.0.0.1",
    "x-forwarded-host": "internal.upstream.local",
    "x-forwarded-proto": "https",
    "x-forwarded-port": "443",
    "x-real-ip": "203.0.113.7",
    via: "1.1 internal-proxy",
    server: "envoy/1.30",
  });

  const safe = safeResponseHeaders(upstream);

  assert.equal(safe.get("content-type"), "application/json", "content-type is preserved");
  assert.equal(safe.get("x-request-id"), "req-1", "unrelated vendor header is preserved");

  for (const removed of [
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
    "x-real-ip",
    "via",
    "server",
  ]) {
    assert.equal(safe.get(removed), null, `${removed} must not be forwarded`);
  }
});

test("AUDIT-07: topology disclosure headers are stripped end-to-end while vendor payload headers survive", async () => {
  const { env } = makeAggregateEnv();
  env.__fetch = async () =>
    new Response("ok", {
      status: 200,
      headers: {
        "content-type": "text/plain",
        "x-request-id": "req-9",
        "x-ratelimit-remaining-requests": "12",
        "x-forwarded-for": "203.0.113.7",
        "x-real-ip": "203.0.113.7",
        via: "1.1 edge",
        server: "nginx",
      },
    });

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-request-id"), "req-9", "vendor request id survives");
  assert.equal(
    res.headers.get("x-ratelimit-remaining-requests"),
    "12",
    "vendor rate-limit header survives"
  );
  assert.equal(res.headers.get("x-forwarded-for"), null, "client IP must not leak");
  assert.equal(res.headers.get("x-real-ip"), null, "client IP must not leak");
  assert.equal(res.headers.get("via"), null, "intermediary metadata must not leak");
  assert.equal(res.headers.get("server"), null, "server banner must not leak");
});

// ==========================================
// BATCH 3 — AUDIT-07 infrastructure/tracing header families
// ==========================================

test("AUDIT-07: reserved distributed-tracing and platform-internal header families are stripped", () => {
  const upstream = new Headers({
    "content-type": "application/json",
    "x-request-id": "req-1",
    "x-custom-upstream": "keep-me",
    "x-amzn-trace-id": "Root=1-abc",
    "x-amz-cf-id": "cf-123",
    "x-ms-request-id": "azure-id",
    "x-b3-traceid": "trace",
    "x-b3-spanid": "span",
    "x-envoy-upstream-service-time": "12",
    "x-cloud-trace-context": "ctx",
    "grpc-status": "0",
    traceparent: "00-abc-def-01",
    tracestate: "vendor=x",
  });

  const safe = safeResponseHeaders(upstream);

  assert.equal(safe.get("content-type"), "application/json", "content-type preserved");
  assert.equal(safe.get("x-request-id"), "req-1", "neutral correlation id preserved");
  assert.equal(safe.get("x-custom-upstream"), "keep-me", "neutral vendor header preserved");

  for (const removed of [
    "x-amzn-trace-id",
    "x-amz-cf-id",
    "x-ms-request-id",
    "x-b3-traceid",
    "x-b3-spanid",
    "x-envoy-upstream-service-time",
    "x-cloud-trace-context",
    "grpc-status",
    "traceparent",
    "tracestate",
  ]) {
    assert.equal(safe.get(removed), null, `${removed} must not be forwarded`);
  }
});

test("AUDIT-07: infrastructure header families are stripped end-to-end without affecting vendor payload headers", async () => {
  const { env } = makeAggregateEnv();
  env.__fetch = async () =>
    new Response("ok", {
      status: 200,
      headers: {
        "content-type": "text/plain",
        "x-custom-upstream": "keep-me",
        "openai-model": "model-x",
        "x-amzn-trace-id": "Root=1-abc",
        "x-envoy-upstream-service-time": "9",
      },
    });

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(chatRequest());

  assert.equal(res.status, 200);
  assert.equal(res.headers.get("x-custom-upstream"), "keep-me", "documented passthrough preserved");
  assert.equal(res.headers.get("openai-model"), "model-x", "api payload header preserved");
  assert.equal(res.headers.get("x-amzn-trace-id"), null, "trace id must not leak");
  assert.equal(res.headers.get("x-envoy-upstream-service-time"), null, "envoy timing must not leak");
});

// The pre-existing passthrough contract must remain intact after the Batch 3
// prefix rules were added.
test("AUDIT-07: the established x-custom-upstream passthrough contract still holds", async () => {
  const upstream = new Headers({
    "x-custom-upstream": "test-val",
    "content-type": "application/json",
  });
  const safe = safeResponseHeaders(upstream);
  assert.equal(safe.get("x-custom-upstream"), "test-val");
});

// ==========================================
// BATCH 3 — AUDIT-06 liveness and readiness
// ==========================================

test("AUDIT-06: /health is a liveness probe that works even with missing configuration", async () => {
  const res = await worker.fetch(
    new Request("https://proxy.local/health", { method: "GET" }),
    {}
  );

  assert.equal(res.status, 200, "liveness must succeed regardless of configuration");
  assert.deepEqual(await res.json(), { ok: true });
});

test("AUDIT-06: /ready reports ready when all required bindings are present", async () => {
  const env = {
    PROXY_KEY: "test-proxy-key",
    NVIDIA_API_KEY: "test-nvidia-key",
    RATE_GATE: { idFromName: () => "id", get: () => ({ fetch() {} }) },
  };

  const res = await worker.fetch(
    new Request("https://proxy.local/ready", { method: "GET" }),
    env
  );

  assert.equal(res.status, 200);
  const body = await res.json();
  assert.equal(body.ready, true);
  assert.deepEqual(body.checks, {
    proxy_key: true,
    upstream_key: true,
    rate_gate: true,
  });
  // No secret values may be present anywhere in the payload.
  const text = JSON.stringify(body);
  assert.ok(!text.includes("test-proxy-key"), "must not leak the proxy key");
  assert.ok(!text.includes("test-nvidia-key"), "must not leak the upstream key");
});

test("AUDIT-06: /ready reports not-ready with 503 when bindings are missing, without leaking values", async () => {
  const res = await worker.fetch(
    new Request("https://proxy.local/ready", { method: "GET" }),
    {}
  );

  assert.equal(res.status, 503);
  const body = await res.json();
  assert.equal(body.ready, false);
  assert.deepEqual(body.checks, {
    proxy_key: false,
    upstream_key: false,
    rate_gate: false,
  });
});

test("AUDIT-06: /ready reports not-ready when only the rate gate binding is missing", async () => {
  const res = await worker.fetch(
    new Request("https://proxy.local/ready", { method: "GET" }),
    { PROXY_KEY: "k", NVIDIA_API_KEY: "u" }
  );

  assert.equal(res.status, 503);
  const body = await res.json();
  assert.equal(body.ready, false);
  assert.equal(body.checks.rate_gate, false);
  assert.equal(body.checks.proxy_key, true);
  assert.equal(body.checks.upstream_key, true);
});

// ==========================================
// BATCH 1 REVIEW — AUDIT-10: buffered-body lifetime accounting
// ==========================================
//
// The aggregate byte budget must be charged for as long as a buffered request
// body is retained by the application, not merely while it is being read from
// the network. A body that has finished reading but is still waiting in the
// FIFO queue is retained and must count against `MAX_AGGREGATE_BODY_BYTES`.
//
// These tests drive the real request lifecycle: they keep the active upstream
// blocked so later requests finish reading and remain queued, and then assert
// (a) queued bodies count toward the budget, (b) an over-budget request is
// rejected with 503, and (c) reservations are released exactly once on
// dispatch completion, cancellation, queue timeout, and read failure.

// A blocking upstream: each dispatch is held until the test releases it, so the
// active slot stays owned and later requests deterministically queue behind it.
function createBlockingUpstream() {
  const releases = [];
  const fetchImpl = async () => {
    await new Promise((resolve) => releases.push(resolve));
    return new Response("ok", { status: 200 });
  };
  return {
    fetchImpl,
    get pending() {
      return releases.length;
    },
    releaseNext() {
      const next = releases.shift();
      assert.ok(next, "an upstream dispatch must be waiting to be released");
      next();
    },
  };
}

// Wait until a predicate holds, driving the microtask/macrotask queues, without
// relying on wall-clock sleeps.
async function waitFor(predicate, label) {
  for (let i = 0; i < 1000; i++) {
    if (predicate()) return;
    await tick();
  }
  assert.fail(`Timed out waiting for ${label}`);
}

function declaredRequest(id, bytes) {
  return new Request("https://proxy.internal/v1/chat/completions", {
    method: "POST",
    headers: { "content-length": String(bytes), "x-req-id": id },
    body: "x".repeat(bytes),
  });
}

test("AUDIT-10: bodies buffered while queued count toward the aggregate budget", async () => {
  const blocking = createBlockingUpstream();
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 2048,
    MAX_CONCURRENT_BODY_READS: 10,
    MAX_QUEUE_DEPTH: 100,
    __fetch: (url, opts) => blocking.fetchImpl(url, opts),
  });
  const rateGate = new RateGate(createMockCtx(), env);

  // Request 1 acquires the active slot and blocks upstream. It retains 600.
  const p1 = rateGate.fetch(declaredRequest("r1", 600));
  await waitFor(() => blocking.pending >= 1, "request 1 to reach upstream");

  // Requests 2 and 3 finish reading (600 bytes each) and queue behind r1.
  const p2 = rateGate.fetch(declaredRequest("r2", 600));
  const p3 = rateGate.fetch(declaredRequest("r3", 600));

  await waitFor(
    () => rateGate.waitingQueue.length >= 2,
    "requests 2 and 3 to queue"
  );

  // 600 (active r1) + 600 (queued r2) + 600 (queued r3) = 1800 <= 2048.
  assert.equal(
    rateGate.activeBodyBytes,
    1800,
    "queued bodies must be charged to the aggregate budget"
  );

  // Request 4 declares 600 -> 1800 + 600 = 2400 > 2048 -> rejected.
  const res4 = await rateGate.fetch(declaredRequest("r4", 600));
  assert.equal(res4.status, 503, "the over-budget queued reservation must be rejected");
  assert.equal(
    (await res4.json()).error,
    "Too many concurrent uploads, please retry later"
  );
  assert.ok(
    rateGate.activeBodyBytes <= 2048,
    "the aggregate budget must never be exceeded while bodies are queued"
  );

  // Drain: release each upstream dispatch in turn and consume each response so
  // the streaming path runs to completion and releases its slot for the next
  // queued request.
  blocking.releaseNext();
  const r1res = await p1;
  await r1res.text();
  await waitFor(() => blocking.pending >= 1, "request 2 to reach upstream");
  blocking.releaseNext();
  const r2res = await p2;
  await r2res.text();
  await waitFor(() => blocking.pending >= 1, "request 3 to reach upstream");
  blocking.releaseNext();
  const r3res = await p3;
  await r3res.text();

  assert.equal(
    rateGate.activeBodyBytes,
    0,
    "every reservation must be released once dispatch completes"
  );
});

test("AUDIT-10: cancelling a queued request releases its retained-body reservation", async () => {
  const blocking = createBlockingUpstream();
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 2048,
    MAX_CONCURRENT_BODY_READS: 10,
    MAX_QUEUE_DEPTH: 100,
    __fetch: (url, opts) => blocking.fetchImpl(url, opts),
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const p1 = rateGate.fetch(declaredRequest("r1", 600));
  await waitFor(() => blocking.pending >= 1, "request 1 to reach upstream");

  const abort = new AbortController();
  const p2 = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "600", "x-req-id": "r2" },
      body: "x".repeat(600),
      signal: abort.signal,
    })
  );

  await waitFor(
    () => rateGate.waitingQueue.length >= 1 && rateGate.activeBodyBytes === 1200,
    "request 2 to queue with its reservation charged"
  );

  // Cancel the queued request: its reservation must be released exactly once.
  abort.abort("client abort");
  const res2 = await p2;
  assert.equal(res2.status, 499, "a cancelled queued request returns 499");
  assert.equal(
    rateGate.activeBodyBytes,
    600,
    "only the active request's reservation may remain after the queued cancel"
  );
  assert.equal(
    rateGate.waitingQueue.length,
    0,
    "the cancelled request must be removed from the queue"
  );

  blocking.releaseNext();
  await p1;
  assert.equal(rateGate.activeBodyBytes, 0, "the active reservation must be released");
});

test("AUDIT-10: a queued request that exceeds the queue wait releases its reservation", async () => {
  const blocking = createBlockingUpstream();
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 2048,
    MAX_CONCURRENT_BODY_READS: 10,
    MAX_QUEUE_DEPTH: 100,
    // A 1 ms queue-wait deadline keeps the real queue timer short and the test
    // fast and reliable while still exercising the genuine timeout path.
    MAX_QUEUE_WAIT_MS: 1,
    __fetch: (url, opts) => blocking.fetchImpl(url, opts),
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const p1 = rateGate.fetch(declaredRequest("r1", 600));
  await waitFor(() => blocking.pending >= 1, "request 1 to reach upstream");

  const p2 = rateGate.fetch(declaredRequest("r2", 600));
  await waitFor(
    () => rateGate.activeBodyBytes === 1200,
    "request 2 to queue with its reservation charged"
  );

  // The queued request times out on its own deadline and is rejected with 503.
  const res2 = await p2;
  assert.equal(res2.status, 503, "a queue-wait timeout returns 503");
  assert.equal(
    (await res2.json()).error,
    "Maximum queue wait exceeded"
  );

  // Its reservation is released; only the active request's 600 bytes remain.
  assert.equal(
    rateGate.activeBodyBytes,
    600,
    "the timed-out request's reservation must be released"
  );

  blocking.releaseNext();
  await p1;
  await waitFor(
    () => rateGate.activeBodyBytes === 0,
    "the active reservation to be released after dispatch"
  );
});

test("AUDIT-10: a read failure releases the retained-body reservation exactly once", async () => {
  // A body stream whose read rejects: the reservation must be released even
  // though the body was never fully retained.
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 2048,
    MAX_CONCURRENT_BODY_READS: 10,
  });

  const failingStream = new ReadableStream({
    pull() {
      throw new Error("source failure");
    },
  });

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      headers: { "content-length": "600", "x-req-id": "rf" },
      body: failingStream,
      duplex: "half",
    })
  );

  assert.equal(res.status, 400, "a failed body read returns 400");
  assert.equal(
    rateGate.activeBodyBytes,
    0,
    "the byte reservation must be released on read failure"
  );
  assert.equal(
    rateGate.activeBodyReads,
    0,
    "the read-concurrency counter must be released on read failure"
  );

  // A subsequent valid request must still be admitted (no leaked reservation).
  const ok = await rateGate.fetch(declaredRequest("ok", 600));
  assert.equal(ok.status, 200, "a valid request must succeed after the failure path");
  await ok.text();
  assert.equal(rateGate.activeBodyBytes, 0);
});

test("AUDIT-10: a successful dispatch releases the retained-body reservation exactly once", async () => {
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 2048,
    MAX_CONCURRENT_BODY_READS: 10,
  });
  const rateGate = new RateGate(createMockCtx(), env);

  const res = await rateGate.fetch(declaredRequest("ok", 600));
  assert.equal(res.status, 200);
  await res.text();
  assert.equal(rateGate.activeBodyBytes, 0, "reservation released after success");
  assert.equal(rateGate.activeBodyReads, 0, "read counter released after success");
});

test("AUDIT-10: a streaming successful response releases the request-body reservation at dispatch", async () => {
  const { env } = makeAggregateEnv({
    MAX_AGGREGATE_BODY_BYTES: 2048,
    MAX_CONCURRENT_BODY_READS: 10,
  });
  env.__fetch = async () =>
    new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("data: a\n\n"));
          controller.close();
        },
      }),
      { status: 200, headers: { "content-type": "text/event-stream" } }
    );

  const rateGate = new RateGate(createMockCtx(), env);
  const res = await rateGate.fetch(declaredRequest("s1", 600));
  assert.equal(res.status, 200);

  // The request body is sent to the upstream before the response stream is
  // consumed, so its reservation is released once dispatch returns, even while
  // the downstream stream is still being read.
  assert.equal(
    rateGate.activeBodyBytes,
    0,
    "the request-body reservation is released once dispatch returns"
  );

  assert.equal(await res.text(), "data: a\n\n");
});

// ==========================================
// AUDIT-17 / F-01 — slot release must survive a storage-write failure in the
// streaming finish() cleanup
// ==========================================
//
// The normal streaming-completion path invokes finish() exactly once. finish()
// persists `rate-state` and only then releases the concurrency slot. If that
// persistence write rejects, the original implementation exited before the
// release, leaving the single slot held forever for the instance. This test
// injects a storage failure on the completion write only (the initial
// per-attempt write still succeeds) and asserts the slot is nevertheless
// released and the next request reaches the upstream.
function makeVaultCtx() {
  const store = new Map();
  let putCount = 0;
  let failAtPut = Infinity;

  return {
    storage: {
      async get(key) {
        if (!store.has(key)) return undefined;
        return JSON.parse(JSON.stringify(store.get(key)));
      },
      async put(key, value) {
        putCount += 1;
        if (putCount === failAtPut) {
          throw new Error("simulated storage write failure");
        }
        store.set(key, JSON.parse(JSON.stringify(value)));
      },
    },
    // Fails exactly the (1-based) nth put call, then succeeds for later puts.
    failPutAt(n) {
      failAtPut = n;
    },
    get putCount() {
      return putCount;
    },
  };
}

function streamingUpstream(counter) {
  return async () => {
    counter.calls += 1;
    return new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("data: a\n\n"));
          controller.close();
        },
      }),
      { status: 200, headers: { "content-type": "text/event-stream" } }
    );
  };
}

test("AUDIT-17/F-01: a storage write failure during streaming finish() still releases the queue slot", async () => {
  const clock = new FakeClock(0);
  const ctx = makeVaultCtx();
  const counter = { calls: 0 };
  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: streamingUpstream(counter),
  };

  const rateGate = new RateGate(ctx, env);

  let releaseCount = 0;
  const originalRelease = rateGate.releaseSlot.bind(rateGate);
  rateGate.releaseSlot = () => {
    releaseCount += 1;
    return originalRelease();
  };

  // Track unhandled rejections created by the injected failure.
  const unhandled = [];
  const onUnhandled = (reason) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);

  try {
    // Put #1 is the per-attempt rate-state write before dispatch; it must
    // succeed so the request proceeds to the upstream. Put #2 is the
    // completion write inside finish(); fail exactly that one to trigger F-01.
    ctx.failPutAt(2);

    const res1 = await rateGate.fetch(chatRequest());
    assert.equal(res1.status, 200, "the first (streaming) response must succeed");

    // Consume the stream to its normal terminal state. finish() runs here.
    assert.equal(await res1.text(), "data: a\n\n");
    // Let any queued microtasks (finish() is async) settle deterministically.
    await tick();

    assert.equal(
      releaseCount,
      1,
      "the slot must be released exactly once despite the storage failure"
    );
    assert.equal(
      rateGate.activeTicket,
      null,
      "the queue slot must be free after streaming completes"
    );

    // A second request must reach the upstream rather than waiting for the
    // queue timeout (the original defect returned 503 here).
    const res2 = await rateGate.fetch(chatRequest());
    assert.equal(
      res2.status,
      200,
      "the next request must be dispatched, not rejected with 503"
    );
    assert.equal(
      counter.calls,
      2,
      "the second request must actually reach the upstream"
    );
    assert.equal(await res2.text(), "data: a\n\n");

    assert.equal(
      releaseCount,
      2,
      "each completed request releases the slot exactly once"
    );

    await tick();
    assert.equal(
      unhandled.length,
      0,
      "the persistence failure must not create an unhandled rejection"
    );
  } finally {
    process.removeListener("unhandledRejection", onUnhandled);
  }
});

test("AUDIT-17/F-01 control: the normal streaming completion path still releases the slot", async () => {
  const clock = new FakeClock(0);
  const ctx = makeVaultCtx();
  const counter = { calls: 0 };
  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: streamingUpstream(counter),
  };

  const rateGate = new RateGate(ctx, env);

  const res1 = await rateGate.fetch(chatRequest());
  assert.equal(res1.status, 200);
  assert.equal(await res1.text(), "data: a\n\n");
  await tick();

  assert.equal(rateGate.activeTicket, null, "the slot is free without a failure");
  assert.ok(ctx.putCount >= 1, "the completion write was attempted");

  const res2 = await rateGate.fetch(chatRequest());
  assert.equal(res2.status, 200);
  assert.equal(counter.calls, 2, "the second request reaches the upstream");
  assert.equal(await res2.text(), "data: a\n\n");
});

// ==========================================
// TASK 20 — deterministic injected-timer regression (header timeout)
// ==========================================
//
// The upstream response-header timeout must be armed and cleared through the
// same injected timer implementation the rest of the RateGate uses
// (`__setTimeout` / `__clearTimeout`). Before TASK 19 this timer bypassed the
// injection and used the real event loop directly, which made the header
// timeout impossible to exercise deterministically. These tests drive the timer
// exclusively through `createFakeTimers()` so they never depend on wall-clock
// time.

test("TASK 20: header timeout is armed through the injected timer and fires deterministically", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  let upstreamSignalAborted = false;

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: (url, opts) =>
      new Promise((resolve, reject) => {
        opts.signal.addEventListener("abort", () => {
          upstreamSignalAborted = true;
          reject(new DOMException("The operation was aborted", "AbortError"));
        });
      }),
  };

  const rateGate = new RateGate(ctx, env);

  const pending = rateGate.fetch(chatRequest());

  // The header timer must be armed through the injected implementation.
  await tick();
  assert.equal(
    timers.pendingCount(),
    1,
    "the header timeout must be armed via the injected setTimeout"
  );

  // No wall-clock wait: advance the virtual clock exactly to the timeout.
  timers.advance(40);

  const res = await pending;
  assert.equal(res.status, 504, "the injected header timeout must yield 504");
  assert.ok(upstreamSignalAborted, "the upstream fetch must be aborted on timeout");
  assert.equal(timers.pendingCount(), 0, "the fired header timer must be cleared");
});

test("TASK 20: header timer is cleared through the injected timer on a fast success", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    // Upstream responds immediately with a complete (non-streamed) body.
    __fetch: async () => new Response("ok", { status: 200 }),
  };

  const rateGate = new RateGate(ctx, env);

  const res = await rateGate.fetch(chatRequest());
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "ok");

  // The header timer armed before dispatch must be cleared after the response
  // headers arrive; no stray injected timer may remain.
  assert.equal(
    timers.pendingCount(),
    0,
    "the header timer must be cleared via the injected clearTimeout on success"
  );
});

// ==========================================
// TASK 22 — isolated negative-path security tests
// ==========================================
//
// These tests exercise the header-timeout negative path in isolation and assert
// that the proxy's own 504 response never leaks the upstream credential, the
// upstream `Authorization`, or network-topology headers. The proxy builds its
// own JSON 504 body and must not relay anything from the upstream response.

test("TASK 22: the 504 header-timeout response never leaks NVIDIA_API_KEY", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();
  const SECRET = "nvapi-VERY-SECRET-KEY-0123456789";

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: SECRET,
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: (url, opts) =>
      new Promise((resolve, reject) => {
        opts.signal.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError"))
        );
      }),
  };

  const rateGate = new RateGate(ctx, env);
  const pending = rateGate.fetch(chatRequest());
  await tick();
  timers.advance(40);
  const res = await pending;

  assert.equal(res.status, 504);
  assert.equal(res.headers.get("authorization"), null, "no Authorization header may be set on the proxy 504");
  const body = await res.text();
  assert.ok(!body.includes(SECRET), "the upstream API key must never appear in the 504 body");
  assert.ok(!JSON.stringify([...res.headers]).includes(SECRET), "the upstream API key must never appear in 504 headers");
});

test("TASK 22: the 504 header-timeout path does not relay upstream topology headers", async () => {
  const clock = new FakeClock(0);
  const ctx = createMockCtx();
  const timers = createFakeTimers();

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    // The upstream handler arms headers but never resolves headers in time; the
    // header timeout must fire and the proxy must return its OWN 504 headers.
    __fetch: (url, opts) =>
      new Promise((resolve, reject) => {
        opts.signal.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError"))
        );
      }),
  };

  const rateGate = new RateGate(ctx, env);
  const pending = rateGate.fetch(chatRequest());
  await tick();
  timers.advance(40);
  const res = await pending;

  assert.equal(res.status, 504);
  for (const name of [
    "server",
    "via",
    "x-forwarded-for",
    "x-real-ip",
    "forwarded",
    "x-amz-request-id",
    "set-cookie",
    "proxy-authenticate",
  ]) {
    assert.equal(res.headers.get(name), null, `topology/credential header "${name}" must not be relayed on the 504`);
  }
  assert.ok(res.headers.get("retry-after"), "the proxy 504 must still carry its own Retry-After header");
  const body = await res.json();
  assert.equal(body.error, "Upstream response header timeout");
});

test("TASK 22: the header-timeout 504 releases the queue slot and persists the timed-out attempt", async () => {
  const clock = new FakeClock(0);
  const ctx = makeRecordingCtx([]);
  const timers = createFakeTimers();

  const env = {
    RPM: 10,
    COOLDOWN_MS: 0,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: (url, opts) =>
      new Promise((resolve, reject) => {
        opts.signal.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError"))
        );
      }),
  };

  const rateGate = new RateGate(ctx, env);
  const pending = rateGate.fetch(chatRequest());
  await tick();
  // Move wall-clock forward before the header timer fires so the timeout's
  // completion timestamp differs from the pre-dispatch start timestamp; this
  // makes the timeout-path rate-state write observably distinct.
  clock.advance(500);
  timers.advance(40);
  const res = await pending;

  assert.equal(res.status, 504);

  // The 504 path must release the single queue slot so the instance can serve
  // the next request (a leaked slot would stall the instance — F-01/F-03 class).
  assert.equal(rateGate.activeTicket, null, "the queue slot must be released on the 504 timeout path");

  // The timed-out attempt WAS dispatched, so its pre-dispatch rate-state write
  // committed: durable state must hold exactly one in-window start plus a
  // completion timestamp recorded when the timeout fired (t=500).
  const durable = ctx.durable();
  assert.ok(durable, "the dispatched-and-timed-out attempt must persist rate-state");
  assert.equal(durable.starts.length, 1, "durable rate-state records the one timed-out attempt");
  assert.equal(durable.starts[0], 0, "the timed-out attempt started at t=0");
  assert.equal(durable.lastCompletionAt, 500, "the timeout records a completion timestamp at the timeout instant");
});

// ==========================================
// TASK 25 / F-03 — pre-dispatch persistence-failure regression
// ==========================================
//
// F-03 is the pre-dispatch analogue of F-01: the per-attempt `rate-state`
// write that happens after a slot is acquired but before the upstream request
// is dispatched. If that write rejects, the request must still release the
// queue slot (the main handler catch does this) and must not leave the single
// slot held. These tests pin that invariant.

test("TASK 25/F-03: a pre-dispatch rate-state write failure releases the slot and returns 502", async () => {
  const clock = new FakeClock(0);
  const ctx = makeVaultCtx();
  let dispatchCount = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatchCount += 1;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  let releaseCount = 0;
  const originalRelease = rateGate.releaseSlot.bind(rateGate);
  rateGate.releaseSlot = () => {
    releaseCount += 1;
    return originalRelease();
  };

  // Put #1 is the pre-dispatch per-attempt rate-state write. Fail it so the
  // request never reaches the upstream, then assert the slot is released.
  ctx.failPutAt(1);

  const res = await rateGate.fetch(chatRequest());
  await tick();

  assert.equal(res.status, 502, "a pre-dispatch persistence failure surfaces as 502");
  assert.equal(dispatchCount, 0, "the upstream must not be dispatched when the pre-dispatch write fails");
  assert.equal(releaseCount, 1, "the slot must be released exactly once");
  assert.equal(rateGate.activeTicket, null, "the queue slot must be free after the failure");
});

test("TASK 25/F-03 control: a subsequent request is admitted after a pre-dispatch failure", async () => {
  const clock = new FakeClock(0);
  const ctx = makeVaultCtx();
  let dispatchCount = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatchCount += 1;
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // First request: fail exactly the pre-dispatch write.
  ctx.failPutAt(1);
  const res1 = await rateGate.fetch(chatRequest());
  assert.equal(res1.status, 502);
  assert.equal(rateGate.activeTicket, null, "the slot is free after the pre-dispatch failure");

  // Second request: no injected failure remains (failPutAt fires once), so it
  // must be admitted and reach the upstream rather than waiting for the queue
  // timeout.
  const res2 = await rateGate.fetch(chatRequest());
  assert.equal(await res2.text(), "ok");
  assert.equal(res2.status, 200, "the queue must admit a new request after a pre-dispatch failure");
  assert.equal(dispatchCount, 1, "the second request must reach the upstream");
});

// ==========================================
// TASK 27 — strengthened F-03 RPM regression (fail-closed dispatch ordering)
// ==========================================
//
// Invariant under test (approved, fail-closed): an upstream request must never
// be dispatched unless its own pre-dispatch `rate-state` persistence write has
// succeeded. These tests prove the ordering on a SINGLE RateGate instance with
// ONE shared storage, using an interleaved event log, so a subsequent request
// cannot leak past the RPM gate or reach the upstream without its own durable
// write.
//
// Deliberately NOT asserted: that a *failed, undispatched* attempt must consume
// an RPM slot. The implementation persists only after `waitForSlot` admits the
// attempt, and a rejected `put` leaves durable `rate-state` untouched — so an
// attempt that never dispatched correctly consumes no durable capacity. That is
// the documented behavior, not a violation.

// Storage wrapper that records durable put/get events and can reject specific
// puts, while preserving full fidelity of the underlying store for `get`.
function makeRecordingCtx(events) {
  const store = new Map();
  let putCount = 0;
  let failAtPut = Infinity;

  return {
    storage: {
      async get(key) {
        events.push({ type: "get", key });
        if (!store.has(key)) return undefined;
        return JSON.parse(JSON.stringify(store.get(key)));
      },
      async put(key, value) {
        putCount += 1;
        const call = putCount;
        events.push({ type: "put-attempt", key, call });
        if (call === failAtPut) {
          events.push({ type: "put-reject", key, call });
          throw new Error("simulated pre-dispatch persistence failure");
        }
        store.set(key, JSON.parse(JSON.stringify(value)));
        events.push({
          type: "put-commit",
          key,
          call,
          starts: JSON.parse(JSON.stringify(value?.starts ?? [])),
        });
      },
    },
    failPutAt(n) {
      failAtPut = n;
    },
    // Direct read of the durable store, bypassing the event log, for final
    // assertions about what actually survived.
    durable() {
      if (!store.has("rate-state")) return undefined;
      return JSON.parse(JSON.stringify(store.get("rate-state")));
    },
  };
}

test("TASK 27/F-03/RPM: a rejected pre-dispatch write never dispatches and consumes no durable slot", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  let dispatchCount = 0;

  const env = {
    RPM: 1, // A single admitted attempt fills the window.
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatchCount += 1;
      events.push({ type: "dispatch", at: clock.now() });
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Fail exactly the first pre-dispatch rate-state write.
  ctx.failPutAt(1);

  const res = await rateGate.fetch(chatRequest());
  await tick();

  assert.equal(res.status, 502, "a rejected pre-dispatch write surfaces as 502");
  assert.equal(dispatchCount, 0, "no upstream dispatch may occur without a successful persistence write");

  // Event ordering: the put was attempted and rejected BEFORE any dispatch.
  const firstDispatch = events.findIndex((e) => e.type === "dispatch");
  const firstCommit = events.findIndex((e) => e.type === "put-commit");
  const firstReject = events.findIndex((e) => e.type === "put-reject");
  assert.equal(firstDispatch, -1, "the event log must contain no dispatch at all");
  assert.ok(firstReject !== -1, "the pre-dispatch put must have been rejected");
  assert.equal(firstCommit, -1, "no put may commit when the first write is rejected");

  // The only write attempted in this request is the failed pre-dispatch write;
  // it rejected the rate-state key itself and nothing was recorded after it.
  const putAttempts = events.filter((e) => e.type === "put-attempt");
  assert.equal(putAttempts.length, 1, "exactly one write must be attempted");
  assert.equal(putAttempts[0].key, "rate-state", "the attempted write must be the rate-state write");
  assert.equal(putAttempts[0].call, 1, "the attempted write must be the first put call");
  const rejected = events[firstReject];
  assert.equal(rejected.key, "rate-state", "the rejection must be for the rate-state write");
  assert.equal(
    firstReject,
    events.length - 1,
    "the rejected write must be the terminal event (nothing committed or dispatched after it)"
  );

  // Durable state must be untouched: the rejected write throws before the store
  // is mutated, so rate-state was never created. The undispatched attempt
  // therefore consumed no slot.
  const durable = ctx.durable();
  assert.equal(
    durable,
    undefined,
    "a failed, undispatched attempt must not create durable rate-state or consume RPM capacity"
  );

  // The slot must be free so the instance can serve the next request.
  assert.equal(rateGate.activeTicket, null, "the queue slot must be released after the failure");
});

test("TASK 27/F-03/RPM: subsequent request on the same instance persists before dispatch and consumes exactly one slot", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  let dispatchCount = 0;

  const env = {
    RPM: 1,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatchCount += 1;
      events.push({ type: "dispatch", at: clock.now() });
      return new Response("ok", { status: 200 });
    },
  };

  // SAME RateGate + SAME storage across the whole sequence.
  const rateGate = new RateGate(ctx, env);

  // Request 1: fail its pre-dispatch write. It must not dispatch.
  ctx.failPutAt(1);
  const res1 = await rateGate.fetch(chatRequest());
  assert.equal(res1.status, 502);
  assert.equal(dispatchCount, 0, "request 1 must not dispatch after a rejected write");
  assert.equal(rateGate.activeTicket, null, "the slot is freed after request 1 fails");

  // Reset the event boundary so ordering of request 2 is unambiguous.
  events.length = 0;

  // Request 2: no failure is queued now (failPutAt fires once), so it must
  // commit its own pre-dispatch write BEFORE it is dispatched.
  const res2 = await rateGate.fetch(chatRequest());
  assert.equal(await res2.text(), "ok");
  assert.equal(res2.status, 200, "request 2 must be admitted on the same instance");
  assert.equal(dispatchCount, 1, "exactly one dispatch across the sequence (request 2 only)");

  // Strict ordering for request 2: commit(rate-state) must precede dispatch.
  const commitIdx = events.findIndex(
    (e) => e.type === "put-commit" && e.key === "rate-state"
  );
  const dispatchIdx = events.findIndex((e) => e.type === "dispatch");
  assert.ok(commitIdx !== -1, "request 2 must commit its pre-dispatch rate-state write");
  assert.ok(dispatchIdx !== -1, "request 2 must dispatch");
  assert.ok(
    commitIdx < dispatchIdx,
    "request 2 must persist rate-state BEFORE dispatching upstream"
  );

  // Request 2's durable write recorded exactly one attempt timestamp.
  const committed = events[commitIdx];
  assert.equal(
    committed.starts.length,
    1,
    "request 2's successful write records exactly one attempt start"
  );

  // Final durable state reflects only request 2 (request 1 never persisted).
  const durable = ctx.durable();
  assert.equal(
    durable.starts.length,
    1,
    "durable rate-state holds exactly the one dispatched attempt"
  );
  assert.equal(
    durable.starts[0],
    committed.starts[0],
    "the durable start matches the value persisted before dispatch"
  );
});

test("TASK 27/F-03/RPM: the RPM gate still blocks a third request on the same instance after a failure+success sequence", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  const dispatches = [];

  const env = {
    RPM: 1, // Only one dispatched attempt per rolling 60s window.
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __fetch: async () => {
      dispatches.push(clock.now());
      events.push({ type: "dispatch", at: clock.now() });
      return new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Request 1: pre-dispatch write fails -> no dispatch, no durable slot.
  ctx.failPutAt(1);
  const res1 = await rateGate.fetch(chatRequest());
  assert.equal(res1.status, 502);
  assert.equal(dispatches.length, 0, "request 1 must not dispatch");

  // Request 2: succeeds and dispatches at t=0, consuming the single RPM slot.
  const res2 = await rateGate.fetch(chatRequest());
  await res2.text();
  assert.equal(res2.status, 200);
  assert.equal(dispatches.length, 1, "request 2 dispatches once");
  assert.equal(dispatches[0], 0, "request 2 dispatches at t=0");

  // Request 3 at the same instant: the RPM window is full (RPM=1, one start at
  // t=0), so the gate must force a wait until that start ages out of the
  // 60s window. The FakeClock makes that wait deterministic and instant.
  const res3 = await rateGate.fetch(chatRequest());
  await res3.text();
  assert.equal(res3.status, 200);
  assert.equal(dispatches.length, 2, "request 3 eventually dispatches once admitted");
  assert.ok(
    dispatches[1] >= 60_000,
    "request 3 must wait until the prior RPM start ages out of the 60s window"
  );
  assert.equal(
    dispatches[1] - dispatches[0],
    60_000,
    "the RPM gate releases the third dispatch exactly one window after the second"
  );

  // Durable rolling window after request 3: the t=0 start has aged out of the
  // 60s window (60000 - 0 is not < 60000), so durable state holds only
  // request 3's start at t=60000. Request 1 never persisted.
  const durable = ctx.durable();
  assert.equal(
    durable.starts.length,
    1,
    "durable rolling window holds only the in-window dispatched attempt"
  );
  assert.equal(
    durable.starts[0],
    60_000,
    "the durable start is request 3's dispatch timestamp"
  );
});

// ==========================================
// TASK 35 — F-03/RPM retry & timeout interaction coverage
// ==========================================
//
// These tests close three coverage gaps identified in TASK 34:
//   A. Each dispatched upstream 429 retry consumes an additional RPM slot and
//      persists its rate-state before dispatching; a retry whose pre-dispatch
//      write fails does not dispatch and leaves no durable RPM entry for it.
//   B. The per-attempt header-timeout timer is owned by the active attempt and
//      cleared before a retry proceeds; a timeout on the retry attempt ends the
//      request with 504 (no further dispatch), releases the slot once, and
//      durable state reflects only the attempts actually dispatched.
//   C. Client cancellation during streaming (with injected timers) propagates
//      the abort upstream, runs streaming cleanup, releases the active slot
//      exactly once, promotes a waiting request only after release, leaves no
//      injected timer pending, and neither removes nor duplicates RPM entries.

test("TASK 35/RPM: each dispatched 429 retry consumes another RPM slot and persists before dispatch", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  let fetches = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0,
    __fetch: async () => {
      fetches += 1;
      events.push({ type: "dispatch", n: fetches, at: clock.now() });
      return fetches === 1
        ? new Response("", { status: 429 })
        : new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const res = await rateGate.fetch(chatRequest());
  assert.equal(await res.text(), "ok");
  assert.equal(res.status, 200);
  assert.equal(fetches, 2, "one 429 then one successful retry");

  const dispatches = events.filter((e) => e.type === "dispatch");
  assert.equal(dispatches.length, 2, "two dispatched attempts (initial + retry)");

  // Each dispatch must be preceded by its own rate-state commit.
  for (const dispatch of dispatches) {
    const dIdx = events.indexOf(dispatch);
    const priorCommit = events
      .slice(0, dIdx)
      .reverse()
      .find((e) => e.type === "put-commit" && e.key === "rate-state");
    assert.ok(
      priorCommit,
      `dispatch n=${dispatch.n} must have a rate-state commit before it`
    );
  }

  // Durable state records exactly the two dispatched attempts' starts.
  const durable = ctx.durable();
  assert.ok(durable, "durable rate-state must exist");
  assert.equal(durable.starts.length, 2, "both dispatched attempts consumed an RPM slot");
});

test("TASK 35/RPM: a retry whose pre-dispatch write fails does not dispatch and records no slot", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  let fetches = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0,
    __fetch: async () => {
      fetches += 1;
      events.push({ type: "dispatch", n: fetches, at: clock.now() });
      return fetches === 1
        ? new Response("", { status: 429 })
        : new Response("ok", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);

  // Put call order for a 429 retry: 1 = initial pre-dispatch, 2 = 429 body
  // completion, 3 = retry pre-dispatch. Fail call 3 so the retry cannot
  // dispatch.
  ctx.failPutAt(3);

  const res = await rateGate.fetch(chatRequest());
  assert.equal(res.status, 502, "a retry pre-dispatch persistence failure surfaces as 502");
  assert.equal(fetches, 1, "the retry must not dispatch when its pre-dispatch write fails");

  const reject = events.find((e) => e.type === "put-reject");
  assert.ok(reject, "the retry pre-dispatch write must be rejected");
  assert.equal(reject.key, "rate-state", "the rejected write is the rate-state write");

  // Only the first dispatched attempt is durable; the failed retry added none.
  const durable = ctx.durable();
  assert.ok(durable, "durable rate-state must exist from the first attempt");
  assert.equal(durable.starts.length, 1, "the failed retry consumed no durable RPM slot");

  assert.equal(rateGate.activeTicket, null, "the queue slot must be released after the failure");
});

test("TASK 35/RPM: header timeout on a 429 retry ends the request, clears both timers, releases the slot once", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  const timers = createFakeTimers();
  let fetches = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 1000,
    RESPONSE_HEADER_TIMEOUT_MS: 40,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0,
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async (url, opts) => {
      fetches += 1;
      const n = fetches;
      events.push({ type: "dispatch", n, at: clock.now(), pending: timers.pendingCount() });
      if (n === 1) {
        // First attempt returns 429 immediately; its header timer is cleared
        // before the retry proceeds.
        return new Response("", { status: 429 });
      }
      // Retry attempt never delivers a response header; the per-attempt header
      // timer must abort it.
      return new Promise((resolve, reject) => {
        opts.signal.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError"))
        );
      });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const pending = rateGate.fetch(chatRequest());
  await tick();

  // At this point attempt 1 (429) has been dispatched and the retry has begun
  // after the backoff sleep; each dispatch had exactly its own +1 timer armed
  // (the previous attempt's timer was cleared before the retry).
  const dispatchEvents = events.filter((e) => e.type === "dispatch");
  assert.equal(dispatchEvents.length, 2, "initial + retry dispatched before the timeout");
  assert.equal(dispatchEvents[0].pending, 1, "attempt 1 arms exactly one header timer");
  assert.equal(dispatchEvents[1].pending, 1, "retry arms its own timer after the prior is cleared");

  clock.advance(500);
  timers.advance(40);
  const res = await pending;
  assert.equal(res.status, 504, "a header timeout on the retry surfaces as 504");
  assert.equal(fetches, 2, "no further dispatch after the retry times out");
  assert.equal(timers.pendingCount(), 0, "no injected header/idle timer may remain pending");
  assert.equal(rateGate.activeTicket, null, "the slot is released on the timeout path");

  // Both dispatched attempts are durable; the timed-out retry records the
  // timeout instant as its completion (start t=1000, timeout at t=1500).
  const durable = ctx.durable();
  assert.ok(durable, "durable rate-state must exist");
  assert.equal(durable.starts.length, 2, "durable records both dispatched attempts");
  assert.deepEqual(durable.starts, [0, 1000], "attempts were dispatched at t=0 and t=1000");
  assert.equal(durable.lastCompletionAt, 1500, "the retry's timeout records the completion instant");
});

test("TASK 35/RPM: client cancel during streaming releases the slot once, promotes a waiter, and preserves RPM entries", async () => {
  const clock = new FakeClock(0);
  const events = [];
  const ctx = makeRecordingCtx(events);
  const timers = createFakeTimers();
  let fetches = 0;

  const env = {
    RPM: 100,
    COOLDOWN_MS: 0,
    MAX_QUEUE_WAIT_MS: 100000,
    RESPONSE_HEADER_TIMEOUT_MS: 1000,
    NVIDIA_API_KEY: "test-key",
    __now: () => clock.now(),
    __sleep: (ms, signal) => clock.sleep(ms, signal),
    __random: () => 0,
    __setTimeout: (fn, ms) => timers.setTimeout(fn, ms),
    __clearTimeout: (id) => timers.clearTimeout(id),
    __fetch: async () => {
      fetches += 1;
      const n = fetches;
      events.push({ type: "dispatch", n, at: clock.now() });
      if (n === 1) {
        return new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("chunk-1"));
            },
            cancel() {
              events.push({ type: "upstream-cancel", n: 1, at: clock.now() });
            },
          }),
          { status: 200 }
        );
      }
      return new Response("ok-2", { status: 200 });
    },
  };

  const rateGate = new RateGate(ctx, env);
  const abortCtrl = new AbortController();

  // Request 1 starts streaming and consumes the slot.
  const res1 = await rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "1",
      signal: abortCtrl.signal,
    })
  );
  assert.equal(res1.status, 200);
  const reader = res1.body.getReader();
  const chunk = await reader.read();
  assert.equal(new TextDecoder().decode(chunk.value), "chunk-1");
  assert.equal(timers.pendingCount(), 0, "no header timeout remains armed once headers arrived");

  // Request 2 queues behind the active slot.
  const res2p = rateGate.fetch(
    new Request("https://proxy.internal/v1/chat/completions", {
      method: "POST",
      body: "2",
    })
  );
  await tick();
  assert.equal(rateGate.waitingQueue.length, 1, "request 2 is waiting behind the active slot");
  assert.equal(fetches, 1, "request 2 has not dispatched while request 1 holds the slot");

  // Client cancels the streaming response.
  abortCtrl.abort();
  for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r));

  const res2 = await res2p;
  assert.equal(res2.status, 200);
  assert.equal(await res2.text(), "ok-2");

  // The upstream cancel was observed, and the waiter dispatched only after it.
  const cancelIdx = events.findIndex((e) => e.type === "upstream-cancel");
  const dispatch2Idx = events.findIndex((e) => e.type === "dispatch" && e.n === 2);
  assert.ok(cancelIdx !== -1, "client cancel must propagate to the upstream stream");
  assert.ok(dispatch2Idx !== -1, "the queued request must be promoted and dispatched");
  assert.ok(
    cancelIdx < dispatch2Idx,
    "the waiting request dispatches only after the active slot is released"
  );

  assert.equal(timers.pendingCount(), 0, "no injected timer may remain pending after cleanup");
  assert.equal(rateGate.activeTicket, null, "the slot is free once the promoted request finishes");

  // Exactly the two dispatched attempts are durable: none removed/duplicated.
  const durable = ctx.durable();
  assert.ok(durable, "durable rate-state must exist");
  assert.equal(durable.starts.length, 2, "exactly two dispatched attempts are recorded");
});

// ==========================================
// REM-02: Constant-time proxy authentication tests
// ==========================================

test("REM-02: timingSafeStringEqual returns true only for matching strings", async () => {
  assert.equal(await timingSafeStringEqual("super-secret-key-123", "super-secret-key-123"), true);
  assert.equal(await timingSafeStringEqual("", ""), true);
});

test("REM-02: timingSafeStringEqual rejects mismatched and unequal-length strings safely", async () => {
  // Mismatched with same length
  assert.equal(await timingSafeStringEqual("super-secret-key-123", "super-secret-key-124"), false);
  // Unequal length: shorter
  assert.equal(await timingSafeStringEqual("super-secret-key-12", "super-secret-key-123"), false);
  // Unequal length: longer
  assert.equal(await timingSafeStringEqual("super-secret-key-1234", "super-secret-key-123"), false);
  // Empty vs non-empty
  assert.equal(await timingSafeStringEqual("", "super-secret-key-123"), false);
  assert.equal(await timingSafeStringEqual("super-secret-key-123", ""), false);
  // Non-string arguments
  assert.equal(await timingSafeStringEqual(null, "super-secret-key-123"), false);
  assert.equal(await timingSafeStringEqual(undefined, "super-secret-key-123"), false);
  assert.equal(await timingSafeStringEqual(12345, 12345), false);
});

test("REM-02: worker rejects mismatched or unequal-length proxy keys with 401", async () => {
  const env = {
    PROXY_KEY: "expected-secret-key-456",
    NVIDIA_API_KEY: "nvidia-secret-789",
    RATE_GATE: { idFromName: () => "id", get: () => ({ fetch: () => new Response("ok") }) },
  };

  // Same length mismatch
  const res1 = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: "Bearer expected-secret-key-457" },
    }),
    env
  );
  assert.equal(res1.status, 401, "mismatched key of same length must return 401");

  // Shorter key
  const res2 = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: "Bearer expected-secret" },
    }),
    env
  );
  assert.equal(res2.status, 401, "shorter key must return 401");

  // Longer key
  const res3 = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: "Bearer expected-secret-key-456-longer" },
    }),
    env
  );
  assert.equal(res3.status, 401, "longer key must return 401");

  // Missing or empty key
  const res4 = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
    }),
    env
  );
  assert.equal(res4.status, 401, "missing authorization must return 401");

  // Valid key via Authorization: Bearer
  const res5 = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: "Bearer expected-secret-key-456" },
    }),
    env
  );
  assert.equal(res5.status, 200, "valid Bearer key must be accepted");

  // Valid key via x-proxy-key header
  const res6 = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { "x-proxy-key": "expected-secret-key-456" },
    }),
    env
  );
  assert.equal(res6.status, 200, "valid x-proxy-key must be accepted");
});

test("REM-02: worker preserves 500 when PROXY_KEY or NVIDIA_API_KEY is missing from environment", async () => {
  // Missing PROXY_KEY
  const resMissingProxy = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: "Bearer any-key" },
    }),
    { NVIDIA_API_KEY: "nvidia-secret-789" }
  );
  assert.equal(resMissingProxy.status, 500, "missing PROXY_KEY in env must return 500");
  assert.equal(await resMissingProxy.text(), "Server configuration error");

  // Missing NVIDIA_API_KEY
  const resMissingNvidia = await worker.fetch(
    new Request("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: "Bearer test-key" },
    }),
    { PROXY_KEY: "test-key" }
  );
  assert.equal(resMissingNvidia.status, 500, "missing NVIDIA_API_KEY in env must return 500");
  assert.equal(await resMissingNvidia.text(), "Server configuration error");
});
