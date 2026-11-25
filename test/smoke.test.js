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
  validateInvalidCredentialsResponse,
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

  assert.equal(validateReadyResponse(200, JSON.stringify({ ready: false, checks: { proxy_key: true, upstream_key: true, rate_gate: true } })).ok, false);
  assert.equal(validateReadyResponse(200, JSON.stringify({ ready: "true", checks: { proxy_key: true, upstream_key: true, rate_gate: true } })).ok, false);
  assert.equal(validateReadyResponse(200, "invalid-json").ok, false);

  const secretBody = "secret-leak-canary-xyz";
  const healthErr = validateHealthResponse(200, secretBody);
  assert.equal(healthErr.ok, false);
  assert.equal(healthErr.error.includes(secretBody), false, "health error must never echo response body");
  const readyErr = validateReadyResponse(200, secretBody);
  assert.equal(readyErr.ok, false);
  assert.equal(readyErr.error.includes(secretBody), false, "ready error must never echo response body");
});

test("HTTP Response Validator: unauthenticated /v1/models expects HTTP 401 and exact 'Unauthorized' body", () => {
  assert.deepEqual(validateUnauthenticatedModelsResponse(401, "Unauthorized"), { ok: true });
  assert.deepEqual(validateUnauthenticatedModelsResponse("401", "Unauthorized"), { ok: true });
  assert.equal(validateUnauthenticatedModelsResponse(200, "Unauthorized").ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(404, "Unauthorized").ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(500, "Unauthorized").ok, false);

  // Missing, padded, or unexpected bodies fail closed without echoing the body
  assert.equal(validateUnauthenticatedModelsResponse(401).ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(401, "").ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(401, "Unauthorized\n").ok, false);
  assert.equal(validateUnauthenticatedModelsResponse(401, " Unauthorized").ok, false);
  const unexpected = validateUnauthenticatedModelsResponse(401, "canary-secret-token-body");
  assert.equal(unexpected.ok, false);
  assert.equal(unexpected.error.includes("canary-secret-token-body"), false);
});

test("HTTP Response Validator: invalid credentials /v1/models expects HTTP 401 and exact 'Unauthorized' body", () => {
  assert.deepEqual(validateInvalidCredentialsResponse(401, "Unauthorized"), { ok: true });
  assert.deepEqual(validateInvalidCredentialsResponse("401", "Unauthorized"), { ok: true });
  assert.equal(validateInvalidCredentialsResponse(200, "Unauthorized").ok, false);
  assert.equal(validateInvalidCredentialsResponse(404, "Unauthorized").ok, false);
  assert.equal(validateInvalidCredentialsResponse(500, "Unauthorized").ok, false);

  assert.equal(validateInvalidCredentialsResponse(401).ok, false);
  assert.equal(validateInvalidCredentialsResponse(401, "").ok, false);
  assert.equal(validateInvalidCredentialsResponse(401, "Not found").ok, false);
  const unexpected = validateInvalidCredentialsResponse(401, "invalid-body-canary-999");
  assert.equal(unexpected.ok, false);
  assert.equal(unexpected.error.includes("invalid-body-canary-999"), false);
});

test("HTTP Response Validator: authenticated /v1/models expects HTTP 404 and exact 'Not found' body", () => {
  assert.deepEqual(validateAuthenticatedModelsResponse(404, "Not found"), { ok: true });
  assert.deepEqual(validateAuthenticatedModelsResponse("404", "Not found"), { ok: true });
  assert.equal(validateAuthenticatedModelsResponse(200, "Not found").ok, false);
  assert.equal(validateAuthenticatedModelsResponse(401, "Unauthorized").ok, false);
  assert.equal(validateAuthenticatedModelsResponse(500, "Not found").ok, false);

  assert.equal(validateAuthenticatedModelsResponse(404).ok, false);
  assert.equal(validateAuthenticatedModelsResponse(404, "").ok, false);
  assert.equal(validateAuthenticatedModelsResponse(404, "Not Found").ok, false);
  assert.equal(validateAuthenticatedModelsResponse(404, "Not found\n").ok, false);
  const unexpected = validateAuthenticatedModelsResponse(404, "authed-body-canary-777");
  assert.equal(unexpected.ok, false);
  assert.equal(unexpected.error.includes("authed-body-canary-777"), false);
});

test("HTTP Validator CLI: enforces body file presence, exact bodies, and zero body disclosure", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "smoke-cli-"));
  const unauthFile = path.join(tmpDir, "unauth.txt");
  const authFile = path.join(tmpDir, "auth.txt");
  const badFile = path.join(tmpDir, "bad.txt");
  fs.writeFileSync(unauthFile, "Unauthorized");
  fs.writeFileSync(authFile, "Not found");
  fs.writeFileSync(badFile, "SUPER_SECRET_LEAK_CANARY");

  try {
    // Valid CLI calls exit 0
    await execFileAsync("node", ["scripts/smoke-http-validate.js", "unauth-models", "401", unauthFile]);
    await execFileAsync("node", ["scripts/smoke-http-validate.js", "invalid-auth-models", "401", unauthFile]);
    await execFileAsync("node", ["scripts/smoke-http-validate.js", "auth-models", "404", authFile]);

    // Missing file fails closed with exit 1
    await assert.rejects(
      () => execFileAsync("node", ["scripts/smoke-http-validate.js", "auth-models", "404"]),
      (err) => err.code === 1
    );
    await assert.rejects(
      () => execFileAsync("node", ["scripts/smoke-http-validate.js", "unauth-models", "401", path.join(tmpDir, "missing.txt")]),
      (err) => err.code === 1
    );

    // Unexpected body fails closed with exit 1 and never leaks body text
    await assert.rejects(
      () => execFileAsync("node", ["scripts/smoke-http-validate.js", "auth-models", "404", badFile]),
      (err) => err.code === 1 && !err.stderr.includes("SUPER_SECRET_LEAK_CANARY")
    );
    await assert.rejects(
      () => execFileAsync("node", ["scripts/smoke-http-validate.js", "unauth-models", "401", badFile]),
      (err) => err.code === 1 && !err.stderr.includes("SUPER_SECRET_LEAK_CANARY")
    );
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
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

// ==========================================
// 5. Workflow Step Behavioral Execution Tests (Checks C, D, E)
// ==========================================

const WORKFLOW_PATH = path.resolve(import.meta.dirname, "../.github/workflows/production-smoke-test.yml");

function extractWorkflowStepRun(stepNamePrefix) {
  const yml = fs.readFileSync(WORKFLOW_PATH, "utf8");
  const lines = yml.split(/\r?\n/);
  const nameIdx = lines.findIndex((line) => line.includes(`- name:`) && line.includes(stepNamePrefix));
  assert.ok(nameIdx !== -1, `Could not find workflow step matching: ${stepNamePrefix}`);

  let runIdx = -1;
  for (let i = nameIdx + 1; i < lines.length; i++) {
    if (/^\s*-\s*name:/.test(lines[i])) break;
    if (/^\s*run:\s*\|/.test(lines[i])) {
      runIdx = i;
      break;
    }
  }
  assert.ok(runIdx !== -1, `Could not find run block for step: ${stepNamePrefix}`);

  const scriptLines = [];
  for (let i = runIdx + 1; i < lines.length; i++) {
    const line = lines[i];
    if (line.trim() === "") {
      scriptLines.push("");
      continue;
    }
    if (!line.startsWith("          ")) break;
    scriptLines.push(line.slice(10));
  }
  return scriptLines.join("\n");
}

function createCurlSpyDir(realCurlPath) {
  const spyDir = fs.mkdtempSync(path.join(os.tmpdir(), "curl-spy-"));
  const spyBin = path.join(spyDir, "bin");
  fs.mkdirSync(spyBin, { mode: 0o755 });
  const argsLog = path.join(spyDir, "argv.log");
  const modeLog = path.join(spyDir, "cfg-mode.log");

  const wrapper = `#!/usr/bin/env bash
set -euo pipefail
: > "${argsLog}"
for arg in "$@"; do
  printf '%s\\n' "$arg" >> "${argsLog}"
done
prev=""
for arg in "$@"; do
  if [ "$prev" = "--config" ] && [ -f "$arg" ]; then
    stat -c '%a' "$arg" > "${modeLog}"
  fi
  prev="$arg"
done
exec "${realCurlPath}" "$@"
`;
  fs.writeFileSync(path.join(spyBin, "curl"), wrapper, { mode: 0o755 });
  return { spyDir, spyBin, argsLog, modeLog };
}

test("Check E workflow script: fails closed when PROXY_KEY is missing, empty, or whitespace-only without touching network", async () => {
  const checkEScript = extractWorkflowStepRun("Check E");
  let requests = 0;
  const server = http.createServer((req, res) => {
    requests += 1;
    res.writeHead(404);
    res.end("Not found");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    for (const badVal of [undefined, "", "   ", "\t\n"]) {
      const env = { ...process.env, TARGET_URL: `http://127.0.0.1:${port}` };
      if (badVal === undefined) {
        delete env.PROXY_KEY;
      } else {
        env.PROXY_KEY = badVal;
      }

      await assert.rejects(
        () => execFileAsync("bash", ["-euo", "pipefail", "-c", checkEScript], {
          cwd: path.resolve(import.meta.dirname, ".."),
          env,
        }),
        (err) => {
          assert.notEqual(err.code, 0, "Check E must exit non-zero when PROXY_KEY is missing/blank");
          const combined = `${err.stdout || ""}\n${err.stderr || ""}`;
          assert.match(combined, /PROXY_KEY/);
          return true;
        }
      );
    }
    assert.equal(requests, 0, "Check E must fail closed before making any HTTP request");
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Check E workflow script: rejects CR/LF/NUL control characters in PROXY_KEY without touching network or leaking key", async () => {
  const checkEScript = extractWorkflowStepRun("Check E");
  let requests = 0;
  const server = http.createServer((req, res) => {
    requests += 1;
    res.writeHead(404);
    res.end("Not found");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    for (const badSecret of ["secret-line1\nX-Injected: true", "secret-cr\rinjected"]) {
      const isolatedTmp = fs.mkdtempSync(path.join(os.tmpdir(), "checke-ctrl-tmp-"));
      try {
        await assert.rejects(
          () => execFileAsync("bash", ["-euo", "pipefail", "-c", checkEScript], {
            cwd: path.resolve(import.meta.dirname, ".."),
            env: {
              ...process.env,
              TMPDIR: isolatedTmp,
              TARGET_URL: `http://127.0.0.1:${port}`,
              PROXY_KEY: badSecret,
            },
          }),
          (err) => {
            assert.notEqual(err.code, 0, "Check E must fail on CR/LF in PROXY_KEY");
            const out = `${err.stdout || ""}\n${err.stderr || ""}`;
            assert.equal(out.includes("secret-line1"), false);
            assert.equal(out.includes("secret-cr"), false);
            return true;
          }
        );
        assert.deepEqual(fs.readdirSync(isolatedTmp), [], "TMP_DIR must be removed after invalid PROXY_KEY format");
      } finally {
        fs.rmSync(isolatedTmp, { recursive: true, force: true });
      }
    }
    assert.equal(requests, 0, "Malformed PROXY_KEY must fail before any HTTP request");
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Check E workflow script: passes key via 0600 curl config (not argv), validates 404 'Not found', and cleans up TMP_DIR", async () => {
  const checkEScript = extractWorkflowStepRun("Check E");
  const { stdout: whichCurl } = await execFileAsync("which", ["curl"]);
  const realCurl = whichCurl.trim();
  const { spyDir, spyBin, argsLog, modeLog } = createCurlSpyDir(realCurl);
  const isolatedTmp = fs.mkdtempSync(path.join(os.tmpdir(), "checke-tmp-"));
  const specialSecret = 'nqp-secret-"quoted"\\\\slash%pct$val';

  const seenHeaders = [];
  const server = http.createServer((req, res) => {
    seenHeaders.push({ url: req.url, auth: req.headers.authorization });
    if (req.url === "/v1/models" && req.headers.authorization === `Bearer ${specialSecret}`) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not found");
      return;
    }
    res.writeHead(401, { "Content-Type": "text/plain" });
    res.end("Unauthorized");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    const res = await execFileAsync("bash", ["-euo", "pipefail", "-c", checkEScript], {
      cwd: path.resolve(import.meta.dirname, ".."),
      env: {
        ...process.env,
        PATH: `${spyBin}:${process.env.PATH}`,
        TMPDIR: isolatedTmp,
        TARGET_URL: `http://127.0.0.1:${port}`,
        PROXY_KEY: specialSecret,
      },
    });

    assert.match(res.stdout, /Check E passed/);
    assert.equal(seenHeaders.length, 1);
    assert.equal(seenHeaders[0].url, "/v1/models");
    assert.equal(seenHeaders[0].auth, `Bearer ${specialSecret}`);

    const argvText = fs.readFileSync(argsLog, "utf8");
    assert.equal(argvText.includes(specialSecret), false, "PROXY_KEY must never appear in curl argv");
    assert.equal(argvText.includes("-L"), false, "curl must not follow redirects");
    assert.match(argvText, /--config/);
    assert.equal(fs.readFileSync(modeLog, "utf8").trim(), "600", "curl config file must have mode 0600");
    assert.deepEqual(fs.readdirSync(isolatedTmp), [], "TMP_DIR must be removed on Check E success");
    assert.equal(res.stdout.includes(specialSecret), false);
    assert.equal(res.stderr.includes(specialSecret), false);
  } finally {
    fs.rmSync(spyDir, { recursive: true, force: true });
    fs.rmSync(isolatedTmp, { recursive: true, force: true });
    await new Promise((resolve) => server.close(resolve));
  }
});

test("Check E workflow script: fails closed on wrong status, wrong body, or 302 redirect, without disclosing body or secret, and cleans up TMP_DIR", async () => {
  const checkEScript = extractWorkflowStepRun("Check E");
  const secret = "nqp-top-secret-key-999";
  const bodyCanary = "SENSITIVE_UPSTREAM_BODY_LEAK_CANARY";

  const cases = [
    { name: "401 Unauthorized instead of 404", status: 401, body: "Unauthorized" },
    { name: "404 wrong capitalization", status: 404, body: "Not Found" },
    { name: "500 error with canary body", status: 500, body: bodyCanary },
    { name: "302 redirect must not be followed", status: 302, body: "", extraHeaders: { Location: "/health" } },
  ];

  for (const tc of cases) {
    const isolatedTmp = fs.mkdtempSync(path.join(os.tmpdir(), "checke-fail-tmp-"));
    let redirectFollowed = false;
    const server = http.createServer((req, res) => {
      if (req.url === "/health") {
        redirectFollowed = true;
        res.writeHead(404);
        res.end("Not found");
        return;
      }
      res.writeHead(tc.status, { "Content-Type": "text/plain", ...(tc.extraHeaders || {}) });
      res.end(tc.body);
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = server.address().port;

    try {
      await assert.rejects(
        () => execFileAsync("bash", ["-euo", "pipefail", "-c", checkEScript], {
          cwd: path.resolve(import.meta.dirname, ".."),
          env: {
            ...process.env,
            TMPDIR: isolatedTmp,
            TARGET_URL: `http://127.0.0.1:${port}`,
            PROXY_KEY: secret,
          },
        }),
        (err) => {
          assert.notEqual(err.code, 0, `${tc.name} must fail closed`);
          const out = `${err.stdout || ""}\n${err.stderr || ""}`;
          assert.equal(out.includes(secret), false, `${tc.name} must never disclose PROXY_KEY`);
          assert.equal(out.includes(bodyCanary), false, `${tc.name} must never disclose response body`);
          return true;
        }
      );
      assert.equal(redirectFollowed, false, `${tc.name}: curl must not follow redirects`);
      assert.deepEqual(fs.readdirSync(isolatedTmp), [], `${tc.name}: TMP_DIR must be cleaned up on failure`);
    } finally {
      fs.rmSync(isolatedTmp, { recursive: true, force: true });
      await new Promise((resolve) => server.close(resolve));
    }
  }
});

test("Checks C and D workflow scripts: send expected credentials, enforce exact 'Unauthorized' body, and fail closed without leakage", async () => {
  const checkCScript = extractWorkflowStepRun("Check C");
  const checkDScript = extractWorkflowStepRun("Check D");

  const recorded = [];
  const validServer = http.createServer((req, res) => {
    recorded.push({ method: req.method, url: req.url, auth: req.headers.authorization });
    if (req.url === "/v1/models") {
      res.writeHead(401, { "Content-Type": "text/plain" });
      res.end("Unauthorized");
      return;
    }
    res.writeHead(404);
    res.end("Not found");
  });
  await new Promise((resolve) => validServer.listen(0, "127.0.0.1", resolve));
  const validPort = validServer.address().port;

  const badBodyCanary = "PRIVATE_UNAUTH_BODY_LEAK";
  const badBodyServer = http.createServer((req, res) => {
    res.writeHead(401, { "Content-Type": "text/plain" });
    res.end(badBodyCanary);
  });
  await new Promise((resolve) => badBodyServer.listen(0, "127.0.0.1", resolve));
  const badPort = badBodyServer.address().port;

  try {
    const resC = await execFileAsync("bash", ["-euo", "pipefail", "-c", checkCScript], {
      cwd: path.resolve(import.meta.dirname, ".."),
      env: { ...process.env, TARGET_URL: `http://127.0.0.1:${validPort}` },
    });
    assert.match(resC.stdout, /Check C passed/);

    const resD = await execFileAsync("bash", ["-euo", "pipefail", "-c", checkDScript], {
      cwd: path.resolve(import.meta.dirname, ".."),
      env: { ...process.env, TARGET_URL: `http://127.0.0.1:${validPort}` },
    });
    assert.match(resD.stdout, /Check D passed/);

    assert.equal(recorded.length, 2);
    assert.deepEqual(recorded[0], { method: "GET", url: "/v1/models", auth: undefined });
    assert.deepEqual(recorded[1], { method: "GET", url: "/v1/models", auth: "Bearer invalid-dummy-token" });

    for (const [label, script] of [["Check C", checkCScript], ["Check D", checkDScript]]) {
      await assert.rejects(
        () => execFileAsync("bash", ["-euo", "pipefail", "-c", script], {
          cwd: path.resolve(import.meta.dirname, ".."),
          env: { ...process.env, TARGET_URL: `http://127.0.0.1:${badPort}` },
        }),
        (err) => {
          assert.notEqual(err.code, 0, `${label} must fail on non-'Unauthorized' 401 body`);
          const out = `${err.stdout || ""}\n${err.stderr || ""}`;
          assert.equal(out.includes(badBodyCanary), false, `${label} must never leak response body`);
          return true;
        }
      );
    }
  } finally {
    await new Promise((resolve) => validServer.close(resolve));
    await new Promise((resolve) => badBodyServer.close(resolve));
  }
});

test("Checks A and B workflow scripts: enforce /health and /ready contracts via GET and fail closed on non-ready responses", async () => {
  const checkAScript = extractWorkflowStepRun("Check A");
  const checkBScript = extractWorkflowStepRun("Check B");

  let mode = "ok";
  const seen = [];
  const server = http.createServer((req, res) => {
    seen.push({ method: req.method, url: req.url });
    if (req.url === "/health") {
      if (mode === "ok") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } else {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false }));
      }
      return;
    }
    if (req.url === "/ready") {
      if (mode === "ok") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ready: true, checks: { proxy_key: true, upstream_key: true, rate_gate: true } }));
      } else {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ready: false, checks: { proxy_key: false, upstream_key: true, rate_gate: true } }));
      }
      return;
    }
    res.writeHead(404);
    res.end("Not found");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;

  try {
    const resA = await execFileAsync("bash", ["-euo", "pipefail", "-c", checkAScript], {
      cwd: path.resolve(import.meta.dirname, ".."),
      env: { ...process.env, TARGET_URL: `http://127.0.0.1:${port}` },
    });
    assert.match(resA.stdout, /Check A passed/);

    const resB = await execFileAsync("bash", ["-euo", "pipefail", "-c", checkBScript], {
      cwd: path.resolve(import.meta.dirname, ".."),
      env: { ...process.env, TARGET_URL: `http://127.0.0.1:${port}` },
    });
    assert.match(resB.stdout, /Check B passed/);
    assert.deepEqual(seen, [
      { method: "GET", url: "/health" },
      { method: "GET", url: "/ready" },
    ]);

    mode = "fail";
    await assert.rejects(
      () => execFileAsync("bash", ["-euo", "pipefail", "-c", checkAScript], {
        cwd: path.resolve(import.meta.dirname, ".."),
        env: { ...process.env, TARGET_URL: `http://127.0.0.1:${port}` },
      }),
      (err) => err.code !== 0
    );
    await assert.rejects(
      () => execFileAsync("bash", ["-euo", "pipefail", "-c", checkBScript], {
        cwd: path.resolve(import.meta.dirname, ".."),
        env: { ...process.env, TARGET_URL: `http://127.0.0.1:${port}` },
      }),
      (err) => err.code !== 0
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});


