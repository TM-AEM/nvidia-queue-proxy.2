import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const readWorkflow = (name) =>
  readFileSync(join(repoRoot, ".github", "workflows", name), "utf8");

// NQP-PR15 P1 — production deployment must be gated to the canonical main ref.
// A `workflow_dispatch` run can be started from any branch, so without an
// explicit ref guard a manual run could deploy a non-main branch's source to
// the production Worker.
test("deploy workflow: production deploy is gated to refs/heads/main", () => {
  const yml = readWorkflow("deploy.yml");

  assert.match(
    yml,
    /if:\s*github\.ref == 'refs\/heads\/main'/,
    "the deploy job must carry a job-level `if: github.ref == 'refs/heads/main'` guard"
  );

  assert.match(
    yml,
    /if\s+\[\s*"\$\{\{\s*github\.ref\s*\}\}"\s*!=\s*"refs\/heads\/main"\s*\]/,
    "a defense-in-depth step must assert the ref is refs/heads/main"
  );

  assert.match(
    yml,
    /ref:\s*main/,
    "checkout must pin to the canonical `main` ref, not the dispatch ref"
  );
});

test("deploy workflow: never deploys on pull_request events", () => {
  const yml = readWorkflow("deploy.yml");
  const triggerBlock = yml.split("permissions:")[0];

  assert.doesNotMatch(
    triggerBlock,
    /pull_request/,
    "the deploy workflow must not be triggered by pull_request"
  );
});

test("deploy workflow: only one wrangler deploy invocation, from the canonical script", () => {
  const yml = readWorkflow("deploy.yml");
  const deployCalls = yml.match(/npm run deploy\s*$/gm) ?? [];

  assert.equal(
    deployCalls.length,
    1,
    "there must be exactly one `npm run deploy` invocation (not `deploy:dry-run`)"
  );
  assert.doesNotMatch(
    yml,
    /wrangler deploy(?!\s*--dry-run)/,
    "deployment must go through `npm run deploy`, not a raw `wrangler deploy`"
  );
});

test("smoke-test workflow: dispatch is restricted to main", () => {
  const yml = readWorkflow("production-smoke-test.yml");

  assert.match(
    yml,
    /github\.event_name == 'workflow_dispatch' && github\.ref == 'refs\/heads\/main'/,
    "a manually dispatched smoke test must be restricted to refs/heads/main"
  );
});
