import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { validateWorkerUrl, AUTHORIZED_WORKER_HOSTNAME } from "../scripts/validate-worker-url.js";
import {
  validateHealthResponse,
  validateReadyResponse,
  validateUnauthenticatedModelsResponse,
  validateAuthenticatedModelsResponse,
} from "../scripts/smoke-http-validate.js";

const execFileAsync = promisify(execFile);

// ==========================================
// 1. WORKER_URL Validation Regression Suite (TASK 1)
// ==========================================

test("URL Validation: 1. Exact authorized URL", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev");
  assert.equal(res.valid, true);
  assert.equal(res.origin, "https://nvidia-queue-proxy-2.5s5k7kk.workers.dev");
});

test("URL Validation: 2. Exact URL with one trailing slash", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev/");
  assert.equal(res.valid, true);
  assert.equal(res.origin, "https://nvidia-queue-proxy-2.5s5k7kk.workers.dev");
});

test("URL Validation: 3. Different workers.dev hostname", () => {
  const res = validateWorkerUrl("https://other-worker.5s5k7kk.workers.dev");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL hostname is not authorized");
});

test("URL Validation: 4. Hostname lookalike", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev.attacker.com");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL hostname is not authorized");
});

test("URL Validation: 5. HTTP instead of HTTPS", () => {
  const res = validateWorkerUrl("http://nvidia-queue-proxy-2.5s5k7kk.workers.dev");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must use https protocol");
});

test("URL Validation: 6. Explicit port 443", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev:443");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not specify an explicit port");
});

test("URL Validation: 7. Explicit non-standard port 8080", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev:8080");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not specify an explicit port");
});

test("URL Validation: 8. Embedded username/password correctly classified as credentials", () => {
  const res = validateWorkerUrl("https://user:pass@nvidia-queue-proxy-2.5s5k7kk.workers.dev");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain credentials");
});

test("URL Validation: 9. Credentials with port correctly classified as credentials", () => {
  const res = validateWorkerUrl("https://user:pass@nvidia-queue-proxy-2.5s5k7kk.workers.dev:8080");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain credentials");
});

test("URL Validation: 10. Unexpected path", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev/v1/models");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a path component");
});

test("URL Validation: 11. Query string", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev?foo=bar");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a query string");
});

test("URL Validation: 12. Fragment", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev#top");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a fragment");
});

test("URL Validation: 13. Malformed URL string", () => {
  const res = validateWorkerUrl("invalid-url-string");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL is malformed");
});

test("URL Validation: 14. Missing or empty value", () => {
  const res1 = validateWorkerUrl("");
  assert.equal(res1.valid, false);
  assert.equal(res1.error, "Missing required secret WORKER_URL");

  const res2 = validateWorkerUrl(null);
  assert.equal(res2.valid, false);
  assert.equal(res2.error, "Missing required secret WORKER_URL");
});

test("URL Validation: 15. Multiple trailing slashes", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev//");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL contains multiple trailing slashes");
});

test("URL Validation: 16. Whitespace or control-character edge cases", () => {
  const res1 = validateWorkerUrl(" https://nvidia-queue-proxy-2.5s5k7kk.workers.dev ");
  assert.equal(res1.valid, false);
  assert.equal(res1.error, "WORKER_URL contains invalid whitespace or control characters");

  const res2 = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev\n");
  assert.equal(res2.valid, false);
  assert.equal(res2.error, "WORKER_URL contains invalid whitespace or control characters");
});

// ==========================================
// 2. Reusable HTTP Response Validator Tests (TASK 2)
// ==========================================

test("HTTP Response Validator: /health accepts only HTTP 200 with ok === true", () => {
  assert.deepEqual(validateHealthResponse(200, JSON.stringify({ ok: true })), { ok: true });
  assert.deepEqual(validateHealthResponse("200", JSON.stringify({ ok: true })), { ok: true });

  // Status code failures
  assert.equal(validateHealthResponse(500, JSON.stringify({ ok: true })).ok, false);
  assert.equal(validateHealthResponse(404, JSON.stringify({ ok: true })).ok, false);

  // Payload failures (failed closed, never exposing payload)
  assert.equal(validateHealthResponse(200, JSON.stringify({ ok: false })).ok, false);
  assert.equal(validateHealthResponse(200, JSON.stringify({ ok: "true" })).ok, false);
  assert.equal(validateHealthResponse(200, JSON.stringify({ ok: 1 })).ok, false);
  assert.equal(validateHealthResponse(200, "not valid json").ok, false);
  assert.equal(validateHealthResponse(200, "").ok, false);
});

test("HTTP Response Validator: /ready accepts only HTTP 200 with all readiness fields boolean true", () => {
  const validPayload = JSON.stringify({
    ready: true,
    checks: { proxy_key: true, upstream_key: true, rate_gate: true },
  });
  assert.deepEqual(validateReadyResponse(200, validPayload), { ok: true });

  // Status code failure
  assert.equal(validateReadyResponse(503, validPayload).ok, false);

  // Missing or false checks fail closed
  const falseCheck = JSON.stringify({ ready: true, checks: { proxy_key: true, upstream_key: false, rate_gate: true } });
  assert.equal(validateReadyResponse(200, falseCheck).ok, false);

  const missingCheck = JSON.stringify({ ready: true, checks: { proxy_key: true } });
  assert.equal(validateReadyResponse(200, missingCheck).ok, false);

  const nullCheck = JSON.stringify({ ready: true, checks: { proxy_key: true, upstream_key: null, rate_gate: true } });
  assert.equal(validateReadyResponse(200, nullCheck).ok, false);

  const stringCheck = JSON.stringify({ ready: true, checks: { proxy_key: "true", upstream_key: true, rate_gate: true } });
  assert.equal(validateReadyResponse(200, stringCheck).ok, false);

  assert.equal(validateReadyResponse(200, "invalid-json").ok, false);
});

test("HTTP Response Validator: unauthenticated /v1/models expects HTTP 401", () => {
  assert.deepEqual(validateUnauthenticatedModelsResponse(401), { ok: true });
  assert.deepEqual(validateUnauthenticatedModelsResponse("401"), { ok: true });
  assert.equal(validateUnauthenticatedModelsResponse(200).ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(404).ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(500).ok, false);
});

test("HTTP Response Validator: authenticated /v1/models expects HTTP 404", () => {
  assert.deepEqual(validateAuthenticatedModelsResponse(404), { ok: true });
  assert.deepEqual(validateAuthenticatedModelsResponse("404"), { ok: true });
  assert.equal(validateAuthenticatedModelsResponse(200).ok, false);
  assert.equal(validateAuthenticatedModelsResponse(401).ok, false);
  assert.equal(validateAuthenticatedModelsResponse(500).ok, false);
});

// ==========================================
// 3. Local Mock Server & Transport Failure Tests (TASK 2)
// ==========================================

test("Mock Server: Connection refusal (curl exit 7) is distinguishable from HTTP status error", async () => {
  const deadPort = 59888;
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "smoke-test-"));
  const statusFile = path.join(tmpDir, "status.txt");

  try {
    let curlExit = 0;
    try {
      await execFileAsync("bash", [
        "-c",
        `set -e; CURL_EXIT=0; curl -s -S --connect-timeout 1 --max-time 1 -o /dev/null -w "%{http_code}" "http://127.0.0.1:${deadPort}/health" > "${statusFile}" || CURL_EXIT=$?; exit $CURL_EXIT`,
      ]);
    } catch (err) {
      curlExit = err.code || err.exitCode;
    }

    assert.equal(curlExit, 7, "curl exit code 7 indicates connection refusal");
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("Mock Server: Timeout behavior fails closed with curl exit 28", async () => {
  const server = http.createServer((req, res) => {
    // Deliberately hang connection
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    let curlExit = 0;
    try {
      await execFileAsync("bash", [
        "-c",
        `set -e; CURL_EXIT=0; curl -s -S --connect-timeout 1 --max-time 1 -o /dev/null -w "%{http_code}" "http://127.0.0.1:${port}/health" || CURL_EXIT=$?; exit $CURL_EXIT`,
      ]);
    } catch (err) {
      curlExit = err.code || err.exitCode;
    }

    assert.equal(curlExit, 28, "curl exit code 28 indicates operation timeout");
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Mock Server: HTTP 500 status code fails closed via shared validator", async () => {
  const server = http.createServer((req, res) => {
    res.writeHead(500, { "Content-Type": "text/plain" });
    res.end("Internal Server Error");
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "smoke-test-"));
    const statusFile = path.join(tmpDir, "status.txt");
    const bodyFile = path.join(tmpDir, "body.json");

    try {
      await execFileAsync("bash", [
        "-c",
        `curl -s -S --connect-timeout 2 --max-time 3 -o "${bodyFile}" -w "%{http_code}" "http://127.0.0.1:${port}/health" > "${statusFile}"`,
      ]);

      const status = fs.readFileSync(statusFile, "utf8").trim();
      const body = fs.readFileSync(bodyFile, "utf8");
      const validation = validateHealthResponse(status, body);
      assert.equal(validation.ok, false);
      assert.match(validation.error, /Health probe failed: expected HTTP 200, received HTTP 500/);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

// ==========================================
// 4. Temporary Directory Lifecycle Tests (TASK 3)
// ==========================================

test("Temporary Directory Lifecycle: Cleans up successfully on exit 0", async () => {
  const script = `
    set -euo pipefail
    TMP_DIR=$(mktemp -d) || { echo "init failed"; exit 1; }
    trap 'rm -rf "\${TMP_DIR:-}"' EXIT
    echo "$TMP_DIR" > /tmp/test_smoke_cleanup_0.txt
    touch "$TMP_DIR/body.json" "$TMP_DIR/status.txt"
    exit 0
  `;
  await execFileAsync("bash", ["-c", script]);
  const dirPath = fs.readFileSync("/tmp/test_smoke_cleanup_0.txt", "utf8").trim();
  assert.equal(fs.existsSync(dirPath), false, "Directory must be removed after exit 0");
  fs.rmSync("/tmp/test_smoke_cleanup_0.txt", { force: true });
});

test("Temporary Directory Lifecycle: Cleans up successfully on failure exit and preserves exit code", async () => {
  const script = `
    set -euo pipefail
    TMP_DIR=$(mktemp -d) || { echo "init failed"; exit 1; }
    trap 'rm -rf "\${TMP_DIR:-}"' EXIT
    echo "$TMP_DIR" > /tmp/test_smoke_cleanup_fail.txt
    touch "$TMP_DIR/body.json" "$TMP_DIR/status.txt"
    exit 37
  `;
  try {
    await execFileAsync("bash", ["-c", script]);
    assert.fail("Should have exited with code 37");
  } catch (err) {
    assert.equal(err.code, 37, "Exit code 37 must be preserved");
  }
  const dirPath = fs.readFileSync("/tmp/test_smoke_cleanup_fail.txt", "utf8").trim();
  assert.equal(fs.existsSync(dirPath), false, "Directory must be removed after exit 37");
  fs.rmSync("/tmp/test_smoke_cleanup_fail.txt", { force: true });
});

test("Temporary Directory Lifecycle: Handles mktemp -d failure safely without using uninitialized paths", async () => {
  const script = `
    set -euo pipefail
    TMP_DIR=$(false || mktemp -d /nonexistent_dir/xxx 2>/dev/null) || { echo "init_handled"; exit 1; }
    trap 'rm -rf "\${TMP_DIR:-}"' EXIT
    exit 0
  `;
  let exitCode = 0;
  let stdout = "";
  try {
    const res = await execFileAsync("bash", ["-c", script]);
    stdout = res.stdout;
  } catch (err) {
    exitCode = err.code || err.exitCode;
    stdout = err.stdout;
  }
  assert.equal(exitCode, 1, "Must exit with code 1 on mktemp -d failure");
  assert.match(stdout, /init_handled/, "Must handle directory creation failure safely");
});
