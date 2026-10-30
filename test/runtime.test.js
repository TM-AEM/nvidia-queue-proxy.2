import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

// Runtime regression for the production "Illegal invocation" defect.
//
// The unit suite runs in Node, where calling the global fetch as an object
// property does not throw. Only the real workerd runtime rejects a host
// function invoked with a non-undefined `this`, so this test executes the
// canonical Worker source (src/index.js) inside workerd via Miniflare and
// exercises the real proxy path: outer handler -> RateGate -> upstream fetch.
//
// Miniflare is a direct dependency of the project's wrangler devDependency, so
// no new dependency is introduced.

const here = path.dirname(fileURLToPath(import.meta.url));
const scriptPath = path.resolve(here, "../src/index.js");

// Test-only bindings. These are obvious placeholders, never real credentials.
const TEST_BINDINGS = {
  PROXY_KEY: "test-proxy-key",
  NVIDIA_API_KEY: "test-nvidia-api-key",
  COOLDOWN_MS: "0",
  RPM: "1000",
};

// `bindingOverrides` lets a test deliberately omit or blank a required secret so
// the not-ready path can be exercised. Omitting a key that is present in
// TEST_BINDINGS (e.g. passing `{ PROXY_KEY: undefined }`) removes that binding
// from the runtime, which is exactly how a missing secret is observed by the
// Worker. Existing callers pass no overrides and keep the full binding set.
function createRuntime(upstreamCalls, bindingOverrides = {}) {
  const bindings = { ...TEST_BINDINGS, ...bindingOverrides };
  for (const key of Object.keys(bindings)) {
    if (bindings[key] === undefined) {
      delete bindings[key];
    }
  }

  return new Miniflare(
    convertV4MiniflareOptions({
      scriptPath,
      modules: true,
      compatibilityDate: "2026-10-27",
      durableObjects: { RATE_GATE: { className: "RateGate", useSQLite: true } },
      bindings,
      // Deterministic upstream stub: no real network is contacted.
      outboundService(request) {
        return request.text().then((body) => {
          upstreamCalls.push({ url: request.url, method: request.method, body });
          return new Response(JSON.stringify({ ok: true, from: "upstream-stub" }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        });
      },
    })
  );
}

test("NQP-02 runtime: proxy request reaches the upstream stub without Illegal invocation", async () => {
  const upstreamCalls = [];
  const mf = createRuntime(upstreamCalls);
  try {
    const res = await mf.dispatchFetch("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${TEST_BINDINGS.PROXY_KEY}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ model: "test-model", messages: [{ role: "user", content: "hi" }] }),
    });

    assert.equal(res.status, 200, "The proxied request must succeed, not fail with 502");
    assert.equal(upstreamCalls.length, 1, "The upstream stub must be reached exactly once");
    assert.equal(upstreamCalls[0].method, "POST");
    assert.match(upstreamCalls[0].url, /^https:\/\/integrate\.api\.nvidia\.com\/v1\/chat\/completions/);
    assert.equal(
      upstreamCalls[0].body,
      JSON.stringify({ model: "test-model", messages: [{ role: "user", content: "hi" }] }),
      "The request body must be forwarded byte-for-byte"
    );

    const body = await res.json();
    assert.deepEqual(body, { ok: true, from: "upstream-stub" });
  } finally {
    await mf.dispose();
  }
});

test("NQP-02 runtime: unauthorized request is rejected before reaching upstream", async () => {
  const upstreamCalls = [];
  const mf = createRuntime(upstreamCalls);
  try {
    const res = await mf.dispatchFetch("https://proxy.local/v1/chat/completions", {
      method: "POST",
      headers: { authorization: "Bearer wrong-key", "content-type": "application/json" },
      body: JSON.stringify({ messages: [] }),
    });
    assert.equal(res.status, 401);
    assert.equal(upstreamCalls.length, 0, "No upstream request may be issued for an unauthorized call");
  } finally {
    await mf.dispose();
  }
});

test("AUDIT-10 runtime: an unauthenticated OPTIONS request is rejected with 401 before routing", async () => {
  // Authentication runs before method/path dispatch, so an unauthenticated
  // OPTIONS request must be rejected with 401, not 404. No CORS handling is
  // expected; the point is the ordering of authentication relative to routing.
  const upstreamCalls = [];
  const mf = createRuntime(upstreamCalls);
  try {
    const res = await mf.dispatchFetch("https://proxy.local/v1/chat/completions", {
      method: "OPTIONS",
    });
    assert.equal(res.status, 401, "Authentication must precede route/method dispatch");
    assert.equal(upstreamCalls.length, 0, "No upstream request may be issued for an unauthenticated call");
  } finally {
    await mf.dispose();
  }
});

test("AUDIT-06 runtime: /health is liveness and /ready is readiness with real bindings", async () => {
  const upstreamCalls = [];
  const mf = createRuntime(upstreamCalls);
  try {
    const health = await mf.dispatchFetch("https://proxy.local/health");
    assert.equal(health.status, 200);
    assert.deepEqual(await health.json(), { ok: true });

    const ready = await mf.dispatchFetch("https://proxy.local/ready");
    assert.equal(ready.status, 200, "the runtime must report ready with real bindings");
    const body = await ready.json();
    assert.equal(body.ready, true);
    assert.equal(body.checks.rate_gate, true);
    assert.equal(body.checks.proxy_key, true);
    assert.equal(body.checks.upstream_key, true);

    assert.equal(upstreamCalls.length, 0, "health/readiness must not contact the upstream");
  } finally {
    await mf.dispose();
  }
});

test("AUDIT-12 runtime: /ready reports 503 with not-ready checks when required secrets are missing", async () => {
  // With a required secret absent, the Worker must report not-ready. The
  // documented contract is HTTP 503 with `{ ready: false, checks: {...} }`, and
  // the readiness probe must never contact the upstream.
  const upstreamCalls = [];
  const mf = createRuntime(upstreamCalls, {
    PROXY_KEY: undefined,
    NVIDIA_API_KEY: undefined,
  });
  try {
    const res = await mf.dispatchFetch("https://proxy.local/ready");
    assert.equal(res.status, 503, "readiness must fail closed when secrets are missing");

    const body = await res.json();
    assert.equal(body.ready, false, "the runtime must report not ready");
    assert.equal(body.checks.proxy_key, false, "the missing PROXY_KEY must fail its check");
    assert.equal(body.checks.upstream_key, false, "the missing NVIDIA_API_KEY must fail its check");
    assert.equal(body.checks.rate_gate, true, "the durable-object binding is still present");

    assert.equal(upstreamCalls.length, 0, "the readiness probe must not contact the upstream");
  } finally {
    await mf.dispose();
  }
});
