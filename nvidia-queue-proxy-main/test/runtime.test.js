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

function createRuntime(upstreamCalls) {
  return new Miniflare(
    convertV4MiniflareOptions({
      scriptPath,
      modules: true,
      compatibilityDate: "2026-10-27",
      durableObjects: { RATE_GATE: { className: "RateGate", useSQLite: true } },
      bindings: { ...TEST_BINDINGS },
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
