import fs from "node:fs";
import { fileURLToPath } from "node:url";

/**
 * Validates /health response: HTTP 200 and JSON with ok === true.
 * Never exposes raw body on failure.
 *
 * @param {string|number} statusCode
 * @param {string} bodyString
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateHealthResponse(statusCode, bodyString) {
  const code = String(statusCode).trim();
  if (code !== "200") {
    return { ok: false, error: `Health probe failed: expected HTTP 200, received HTTP ${code}` };
  }
  try {
    const data = JSON.parse(bodyString);
    if (data && data.ok === true) {
      return { ok: true };
    }
    return { ok: false, error: "Health check failed: expected JSON payload {ok: true}" };
  } catch {
    return { ok: false, error: "Health check failed: response was not valid JSON" };
  }
}

/**
 * Validates /ready response: HTTP 200 and JSON with ready === true and
 * checks.proxy_key === true, checks.upstream_key === true, checks.rate_gate === true.
 * Must be strictly boolean true; non-boolean truthy values fail closed.
 *
 * @param {string|number} statusCode
 * @param {string} bodyString
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateReadyResponse(statusCode, bodyString) {
  const code = String(statusCode).trim();
  if (code !== "200") {
    return { ok: false, error: `Readiness probe failed: expected HTTP 200, received HTTP ${code}` };
  }
  try {
    const data = JSON.parse(bodyString);
    if (!data || typeof data !== "object") {
      return { ok: false, error: "Readiness check failed: invalid JSON structure" };
    }
    const checks = data.checks;
    if (!checks || typeof checks !== "object") {
      return { ok: false, error: "Readiness check failed: missing checks object" };
    }
    const isReady = data.ready === true &&
      checks.proxy_key === true &&
      checks.upstream_key === true &&
      checks.rate_gate === true;

    if (isReady) {
      return { ok: true };
    }
    return { ok: false, error: "Readiness check failed: one or more required bindings are not ready" };
  } catch {
    return { ok: false, error: "Readiness check failed: response was not valid JSON" };
  }
}

/**
 * Validates unauthenticated /v1/models response: must be HTTP 401.
 *
 * @param {string|number} statusCode
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateUnauthenticatedModelsResponse(statusCode) {
  const code = String(statusCode).trim();
  if (code === "401") {
    return { ok: true };
  }
  return { ok: false, error: `Security check failed: expected HTTP 401 Unauthorized, received HTTP ${code}` };
}

/**
 * Validates authenticated /v1/models response: must be HTTP 404.
 *
 * @param {string|number} statusCode
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateAuthenticatedModelsResponse(statusCode) {
  const code = String(statusCode).trim();
  if (code === "404") {
    return { ok: true };
  }
  return { ok: false, error: `Unsupported route check failed: expected HTTP 404 Not Found, received HTTP ${code}` };
}

// CLI entry point for workflow integration
if (process.argv[1] && fileURLToPath(import.meta.url) === fs.realpathSync(process.argv[1])) {
  const [,, checkType, statusCode, bodyFilePath] = process.argv;
  let result;

  let body = "";
  if (bodyFilePath && fs.existsSync(bodyFilePath)) {
    try {
      body = fs.readFileSync(bodyFilePath, "utf8");
    } catch (err) {
      console.error(`::error::Failed to read body file: ${err.message}`);
      process.exit(1);
    }
  }

  switch (checkType) {
    case "health":
      result = validateHealthResponse(statusCode, body);
      break;
    case "ready":
      result = validateReadyResponse(statusCode, body);
      break;
    case "unauth-models":
      result = validateUnauthenticatedModelsResponse(statusCode);
      break;
    case "auth-models":
      result = validateAuthenticatedModelsResponse(statusCode);
      break;
    default:
      console.error(`::error::Unknown check type: ${checkType}`);
      process.exit(1);
  }

  if (!result.ok) {
    console.error(`::error::${result.error}`);
    process.exit(1);
  }
  process.exit(0);
}
