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
 * Validates unauthenticated /v1/models response: must be HTTP 401 with exact body "Unauthorized".
 * Never exposes raw body on failure.
 *
 * @param {string|number} statusCode
 * @param {string} bodyString
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateUnauthenticatedModelsResponse(statusCode, bodyString) {
  const code = String(statusCode).trim();
  if (code !== "401") {
    return { ok: false, error: `Security check failed: expected HTTP 401 Unauthorized, received HTTP ${code}` };
  }
  if (typeof bodyString !== "string" || bodyString !== "Unauthorized") {
    return { ok: false, error: "Security check failed: expected exact 'Unauthorized' response body" };
  }
  return { ok: true };
}

/**
 * Validates invalid credentials response: must be HTTP 401 with exact body "Unauthorized".
 * Never exposes raw body on failure.
 *
 * @param {string|number} statusCode
 * @param {string} bodyString
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateInvalidCredentialsResponse(statusCode, bodyString) {
  const code = String(statusCode).trim();
  if (code !== "401") {
    return { ok: false, error: `Security check failed: expected HTTP 401 Unauthorized for invalid credentials, received HTTP ${code}` };
  }
  if (typeof bodyString !== "string" || bodyString !== "Unauthorized") {
    return { ok: false, error: "Security check failed: expected exact 'Unauthorized' response body for invalid credentials" };
  }
  return { ok: true };
}

/**
 * Validates authenticated /v1/models response: must be HTTP 404 with exact body "Not found".
 * Never exposes raw body on failure.
 *
 * @param {string|number} statusCode
 * @param {string} bodyString
 * @returns {{ ok: boolean, error?: string }}
 */
export function validateAuthenticatedModelsResponse(statusCode, bodyString) {
  const code = String(statusCode).trim();
  if (code !== "404") {
    return { ok: false, error: `Unsupported route check failed: expected HTTP 404 Not Found, received HTTP ${code}` };
  }
  if (typeof bodyString !== "string" || bodyString !== "Not found") {
    return { ok: false, error: "Unsupported route check failed: expected exact 'Not found' response body" };
  }
  return { ok: true };
}

// CLI entry point for workflow integration
if (process.argv[1] && fileURLToPath(import.meta.url) === fs.realpathSync(process.argv[1])) {
  const [,, checkType, statusCode, bodyFilePath] = process.argv;
  let result;

  if (!bodyFilePath || !fs.existsSync(bodyFilePath)) {
    console.error("::error::Response body file is missing");
    process.exit(1);
  }

  let body;
  try {
    body = fs.readFileSync(bodyFilePath, "utf8");
  } catch (err) {
    console.error(`::error::Failed to read body file: ${err.message}`);
    process.exit(1);
  }

  switch (checkType) {
    case "health":
      result = validateHealthResponse(statusCode, body);
      break;
    case "ready":
      result = validateReadyResponse(statusCode, body);
      break;
    case "unauth-models":
      result = validateUnauthenticatedModelsResponse(statusCode, body);
      break;
    case "invalid-auth-models":
      result = validateInvalidCredentialsResponse(statusCode, body);
      break;
    case "auth-models":
      result = validateAuthenticatedModelsResponse(statusCode, body);
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
