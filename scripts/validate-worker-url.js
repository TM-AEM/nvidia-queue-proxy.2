import { fileURLToPath } from "node:url";
import fs from "node:fs";

export const AUTHORIZED_WORKER_HOSTNAME = "nvidia-queue-proxy-2.5s5k7kk.workers.dev";

/**
 * Validates a WORKER_URL string against strict production constraints.
 * Never logs or prints the secret value.
 *
 * @param {string} raw - The raw input URL string from the environment.
 * @param {string} [expectedHostname] - The required exact hostname.
 * @returns {{ valid: boolean, origin?: string, error?: string }}
 */
export function validateWorkerUrl(raw, expectedHostname = AUTHORIZED_WORKER_HOSTNAME) {
  if (!raw || typeof raw !== "string") {
    return { valid: false, error: "Missing required secret WORKER_URL" };
  }

  // Reject strings with leading/trailing whitespace or control characters
  if (raw !== raw.trim() || /[\x00-\x1F\x7F]/.test(raw)) {
    return { valid: false, error: "WORKER_URL contains invalid whitespace or control characters" };
  }

  // Standards-compliant parsing via WHATWG URL
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    return { valid: false, error: "WORKER_URL is malformed" };
  }

  // 1. Enforce HTTPS protocol
  if (parsed.protocol !== "https:") {
    return { valid: false, error: "WORKER_URL must use https protocol" };
  }

  // 2. Enforce exact authorized hostname
  if (parsed.hostname !== expectedHostname) {
    return { valid: false, error: "WORKER_URL hostname is not authorized" };
  }

  // 3. Reject any explicit port (including default :443)
  // WHATWG URL normalizes :443 on https, so check the authority segment in raw input as well
  const authority = raw.slice(raw.indexOf("://") + 3).split("/")[0].split("?")[0].split("#")[0];
  if (authority.includes(":") || parsed.port !== "") {
    return { valid: false, error: "WORKER_URL must not specify an explicit port" };
  }

  // 4. Reject credentials (username / password)
  if (parsed.username !== "" || parsed.password !== "" || authority.includes("@")) {
    return { valid: false, error: "WORKER_URL must not contain credentials" };
  }

  // 5. Reject unexpected paths (allow only empty or single trailing slash)
  if (parsed.pathname !== "" && parsed.pathname !== "/") {
    return { valid: false, error: "WORKER_URL must not contain a path component" };
  }

  // Reject multiple trailing slashes (e.g. https://domain//)
  const afterAuthority = raw.slice(raw.indexOf(authority) + authority.length);
  if (afterAuthority.startsWith("//")) {
    return { valid: false, error: "WORKER_URL contains multiple trailing slashes" };
  }

  // 6. Reject query strings
  if (parsed.search !== "" || raw.includes("?")) {
    return { valid: false, error: "WORKER_URL must not contain a query string" };
  }

  // 7. Reject fragments
  if (parsed.hash !== "" || raw.includes("#")) {
    return { valid: false, error: "WORKER_URL must not contain a fragment" };
  }

  return { valid: true, origin: parsed.origin };
}

// CLI entry point when executed directly by GitHub Actions or scripts
if (process.argv[1] && fileURLToPath(import.meta.url) === fs.realpathSync(process.argv[1])) {
  const result = validateWorkerUrl(process.env.WORKER_URL);
  if (!result.valid) {
    console.error(`::error::${result.error}`);
    process.exit(1);
  }

  // Output sanitized origin
  console.log(result.origin);

  // If running in GitHub Actions, append to GITHUB_OUTPUT
  if (process.env.GITHUB_OUTPUT) {
    try {
      fs.appendFileSync(process.env.GITHUB_OUTPUT, `target_url=${result.origin}\n`);
    } catch (err) {
      console.error(`::error::Failed to write to GITHUB_OUTPUT: ${err.message}`);
      process.exit(1);
    }
  }
}
