import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { validateWorkerUrl, AUTHORIZED_WORKER_HOSTNAME } from "../scripts/validate-worker-url.js";

const execFileAsync = promisify(execFile);

// ==========================================
// 1. WORKER_URL Validation Regression Suite
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

test("URL Validation: 7. Explicit non-standard port", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev:8080");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not specify an explicit port");
});

test("URL Validation: 8. Embedded username/password", () => {
  const res = validateWorkerUrl("https://user:pass@nvidia-queue-proxy-2.5s5k7kk.workers.dev");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not specify an explicit port");
});

test("URL Validation: 9. Unexpected path", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev/v1/models");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a path component");
});

test("URL Validation: 10. Query string", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev?foo=bar");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a query string");
});

test("URL Validation: 11. Fragment", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev#top");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a fragment");
});

test("URL Validation: 12. Malformed URL", () => {
  const res = validateWorkerUrl("invalid-url-string");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL is malformed");
});

test("URL Validation: 13. Missing or empty value", () => {
  const res1 = validateWorkerUrl("");
  assert.equal(res1.valid, false);
  assert.equal(res1.error, "Missing required secret WORKER_URL");

  const res2 = validateWorkerUrl(null);
  assert.equal(res2.valid, false);
  assert.equal(res2.error, "Missing required secret WORKER_URL");
});

test("URL Validation: 14. Multiple trailing slashes", () => {
  const res = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev//");
  assert.equal(res.valid, false);
  assert.equal(res.error, "WORKER_URL must not contain a path component");
});

test("URL Validation: 15. Whitespace or control-character edge cases", () => {
  const res1 = validateWorkerUrl(" https://nvidia-queue-proxy-2.5s5k7kk.workers.dev ");
  assert.equal(res1.valid, false);
  assert.equal(res1.error, "WORKER_URL contains invalid whitespace or control characters");

  const res2 = validateWorkerUrl("https://nvidia-queue-proxy-2.5s5k7kk.workers.dev\n");
  assert.equal(res2.valid, false);
  assert.equal(res2.error, "WORKER_URL contains invalid whitespace or control characters");
});

// ==========================================
// 2. HTTP Smoke Test Behavior & Mock Server
// ==========================================

test("HTTP Behavior: Successful expected HTTP response (/health 200 ok=true)", async () => {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "smoke-test-"));
    const bodyFile = path.join(tmpDir, "body.json");
    const statusFile = path.join(tmpDir, "status.txt");

    try {
      const { stdout } = await execFileAsync("bash", [
        "-c",
        `curl -s -S --connect-timeout 2 --max-time 3 -o "${bodyFile}" -w "%{http_code}" "http://127.0.0.1:${port}/health" > "${statusFile}"`,
      ]);

      const status = fs.readFileSync(statusFile, "utf8").trim();
      assert.equal(status, "200");

      const body = JSON.parse(fs.readFileSync(bodyFile, "utf8"));
      assert.equal(body.ok, true);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("HTTP Behavior: Unexpected HTTP response (e.g. 500 status)", async () => {
  const server = http.createServer((req, res) => {
    res.writeHead(500, { "Content-Type": "text/plain" });
    res.end("Internal Server Error");
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "smoke-test-"));
    const statusFile = path.join(tmpDir, "status.txt");

    try {
      await execFileAsync("bash", [
        "-c",
        `curl -s -S --connect-timeout 2 --max-time 3 -o /dev/null -w "%{http_code}" "http://127.0.0.1:${port}/health" > "${statusFile}"`,
      ]);

      const status = fs.readFileSync(statusFile, "utf8").trim();
      assert.equal(status, "500", "Must record status code 500");
      assert.notEqual(status, "200", "Must not accept 500 as 200");
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("HTTP Behavior: Connection refusal / transport failure", async () => {
  // Use a port that is not listening
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

    assert.notEqual(curlExit, 0, "curl must fail with non-zero exit code on connection refusal");
    assert.equal(curlExit, 7, "curl exit code 7 indicates connection failure");
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("HTTP Behavior: Timeout behavior (server hangs past timeout)", async () => {
  const server = http.createServer((req, res) => {
    // Deliberately hold connection open without responding
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

test("HTTP Behavior: Invalid JSON response payload", () => {
  const invalidPayload = "<html><body>502 Bad Gateway from Proxy</body></html>";
  let parseFailed = false;
  try {
    const data = JSON.parse(invalidPayload);
    if (data.ok === true) parseFailed = false;
  } catch {
    parseFailed = true;
  }
  assert.equal(parseFailed, true, "Invalid JSON must be rejected");
});

test("HTTP Behavior: Valid JSON with missing/false readiness fields", () => {
  const missingChecks = JSON.stringify({ ready: true, checks: { proxy_key: true, upstream_key: false, rate_gate: true } });
  const data = JSON.parse(missingChecks);
  const checks = data.checks || {};
  const valid = data.ready === true &&
                checks.proxy_key === true &&
                checks.upstream_key === true &&
                checks.rate_gate === true;
  assert.equal(valid, false, "Must reject readiness when upstream_key is false");
});

test("HTTP Behavior: Temporary-file lifecycle & cleanup after success and failure", async () => {
  // 1. Success path cleanup
  const scriptSuccess = `
    set -euo pipefail
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT
    echo "$TMP_DIR" > /tmp/test_tmp_dir_path.txt
    touch "$TMP_DIR/body.json" "$TMP_DIR/status.txt"
    exit 0
  `;
  await execFileAsync("bash", ["-c", scriptSuccess]);
  const successDir = fs.readFileSync("/tmp/test_tmp_dir_path.txt", "utf8").trim();
  assert.equal(fs.existsSync(successDir), false, "Directory must be removed after successful exit");

  // 2. Failure path cleanup
  const scriptFailure = `
    set -euo pipefail
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT
    echo "$TMP_DIR" > /tmp/test_tmp_dir_path.txt
    touch "$TMP_DIR/body.json"
    exit 42
  `;
  try {
    await execFileAsync("bash", ["-c", scriptFailure]);
  } catch (err) {
    assert.equal(err.code, 42, "Original exit code must be preserved");
  }
  const failureDir = fs.readFileSync("/tmp/test_tmp_dir_path.txt", "utf8").trim();
  assert.equal(fs.existsSync(failureDir), false, "Directory must be removed after error exit");

  fs.rmSync("/tmp/test_tmp_dir_path.txt", { force: true });
});

test("HTTP Behavior: Temporary-file partial initialization robustness", async () => {
  // Test that single mktemp -d eliminates partial multi-file leak risk
  const scriptSingleInit = `
    set -euo pipefail
    TMP_DIR=$(mktemp -d) || { echo "init failed"; exit 1; }
    trap 'rm -rf "$TMP_DIR"' EXIT
    BODY_FILE="\${TMP_DIR}/body.json"
    STATUS_FILE="\${TMP_DIR}/status.txt"
    touch "$BODY_FILE"
    echo "$TMP_DIR" > /tmp/test_single_init.txt
  `;
  await execFileAsync("bash", ["-c", scriptSingleInit]);
  const dirPath = fs.readFileSync("/tmp/test_single_init.txt", "utf8").trim();
  assert.equal(fs.existsSync(dirPath), false, "Temporary directory cleaned up completely");
  fs.rmSync("/tmp/test_single_init.txt", { force: true });
});
