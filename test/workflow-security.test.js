import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const workflowsDir = join(repoRoot, ".github", "workflows");

const readWorkflow = (name) => readFileSync(join(workflowsDir, name), "utf8");
const listWorkflows = () =>
  readdirSync(workflowsDir).filter((f) => f.endsWith(".yml") || f.endsWith(".yaml"));

// The trigger block is everything from the top-level `on:` key up to the next
// top-level key (`permissions:`). Extracting it (rather than scanning the whole
// file) keeps a `pull_request` mention inside a step or comment from producing
// a false negative.
const triggerBlock = (yml) => {
  const start = yml.indexOf("\non:");
  const after = start === -1 ? yml : yml.slice(start + 1);
  const end = after.indexOf("\npermissions:");
  return end === -1 ? after : after.slice(0, end);
};

// ---------------------------------------------------------------------------
// Reusable safety assertions. Each throws on a violation. They run against the
// real workflows (must pass) and against mutated copies (must throw), so the
// mutation tests prove the assertions catch a regression instead of passing
// vacuously.
// ---------------------------------------------------------------------------

function assertNoAutomaticTrigger(yml, label) {
  const block = triggerBlock(yml);
  for (const event of ["push", "pull_request", "workflow_run", "repository_dispatch"]) {
    assert.doesNotMatch(
      block,
      new RegExp(`^\\s{2}${event}:`, "m"),
      `${label}: must not be triggered by \`${event}\``
    );
  }
}

function assertManualDispatchAvailable(yml, label) {
  assert.match(
    triggerBlock(yml),
    /^\s{2}workflow_dispatch:/m,
    `${label}: must remain manually dispatchable`
  );
}

function assertMainRefGuard(yml, label) {
  assert.match(
    yml,
    /if:\s*github\.ref == 'refs\/heads\/main'/,
    `${label}: job-level \`if: github.ref == 'refs/heads/main'\` guard is mandatory`
  );
  assert.match(
    yml,
    /if\s+\[\s*"\$\{\{\s*github\.ref\s*\}\}"\s*!=\s*"refs\/heads\/main"\s*\]/,
    `${label}: defense-in-depth ref assertion step is mandatory`
  );
}

function assertCanonicalCheckout(yml, label) {
  assert.match(
    yml,
    /ref:\s*main\b/,
    `${label}: checkout must pin to the canonical \`main\` ref`
  );
}

function assertEnvironmentGate(yml, label) {
  assert.match(
    yml,
    /^\s{4}environment:\s*$/m,
    `${label}: the deploy job must declare a GitHub environment`
  );
  assert.match(
    yml,
    /^\s{6}name:\s*production\s*$/m,
    `${label}: the environment must be named exactly \`production\``
  );
}

function assertSingleDeployInvocation(yml, label) {
  const deployCalls = yml.match(/npm run deploy\s*$/gm) ?? [];
  assert.equal(
    deployCalls.length,
    1,
    `${label}: exactly one \`npm run deploy\` invocation is required`
  );
  assert.doesNotMatch(
    yml,
    /wrangler deploy(?!\s*--dry-run)/,
    `${label}: deployment must go through \`npm run deploy\`, not a raw \`wrangler deploy\``
  );
}

function assertValidationPreserved(yml, label) {
  for (const step of [
    "npm ci",
    "npm run check:canonical",
    "npm run lint",
    "npm test",
    "npm run deploy:dry-run",
  ]) {
    assert.match(
      yml,
      new RegExp(step.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")),
      `${label}: validation step \`${step}\` must be preserved`
    );
  }
}

// ---------------------------------------------------------------------------
// Positive assertions against the real, committed workflows.
// ---------------------------------------------------------------------------

test("deploy workflow: no automatic trigger — a merge to main cannot deploy", () => {
  assertNoAutomaticTrigger(readWorkflow("deploy.yml"), "deploy.yml");
});

test("deploy workflow: manual dispatch remains available", () => {
  assertManualDispatchAvailable(readWorkflow("deploy.yml"), "deploy.yml");
});

test("deploy workflow: main-ref guard and canonical checkout are mandatory", () => {
  const yml = readWorkflow("deploy.yml");
  assertMainRefGuard(yml, "deploy.yml");
  assertCanonicalCheckout(yml, "deploy.yml");
});

test("deploy workflow: production environment gate is mandatory and named `production`", () => {
  assertEnvironmentGate(readWorkflow("deploy.yml"), "deploy.yml");
});

test("deploy workflow: exactly one deploy invocation, via the canonical script", () => {
  assertSingleDeployInvocation(readWorkflow("deploy.yml"), "deploy.yml");
});

test("deploy workflow: deployment validation steps are preserved", () => {
  assertValidationPreserved(readWorkflow("deploy.yml"), "deploy.yml");
});

test("CI workflow: no production secrets and no deploy path", () => {
  const ci = readWorkflow("ci.yml");
  assert.doesNotMatch(ci, /secrets\./, "CI must not reference repository secrets");
  assert.doesNotMatch(ci, /npm run deploy\s*$/m, "CI must not run the deploy script");
  assert.doesNotMatch(ci, /wrangler deploy(?!\s*--dry-run)/, "CI must not run a raw deploy");
});

test("CI workflow: validates pull requests and non-main pushes without deploying", () => {
  const ci = readWorkflow("ci.yml");
  const block = triggerBlock(ci);
  assert.match(block, /^\s{2}pull_request:/m, "CI must validate pull requests");
  assert.match(block, /branches-ignore:/, "CI must validate non-main branch pushes");
  assert.match(ci, /npm run deploy:dry-run/, "CI must still validate the bundle with a dry-run");
});

test("repository: no alternate workflow can automatically deploy to production", () => {
  const offenders = [];

  for (const name of listWorkflows()) {
    const yml = readWorkflow(name);
    const deploys =
      /npm run deploy\s*$/m.test(yml) || /wrangler deploy(?!\s*--dry-run)/.test(yml);
    if (!deploys) continue;

    const block = triggerBlock(yml);
    const automatic = ["push", "pull_request", "workflow_run", "repository_dispatch"].some((e) =>
      new RegExp(`^\\s{2}${e}:`, "m").test(block)
    );
    if (automatic) offenders.push(name);
  }

  assert.deepEqual(
    offenders,
    [],
    `workflows with an automatic trigger that also deploy: ${offenders.join(", ") || "(none)"}`
  );
});

// ---------------------------------------------------------------------------
// Mutation tests: prove each guard assertion fails when the guard is removed or
// weakened. This is what makes the suite a real safety net.
// ---------------------------------------------------------------------------

const DEPLOY = readWorkflow("deploy.yml");

const mutations = [
  {
    name: "removing the job-level main-ref guard is caught",
    yml: DEPLOY.replace(/^\s*if: github\.ref == 'refs\/heads\/main'\n/m, ""),
    check: (yml) => assertMainRefGuard(yml, "mutated"),
  },
  {
    name: "re-adding a push trigger is caught",
    yml: DEPLOY.replace(
      /^\s*workflow_dispatch:\n/m,
      "  push:\n    branches:\n      - main\n  workflow_dispatch:\n"
    ),
    check: (yml) => assertNoAutomaticTrigger(yml, "mutated"),
  },
  {
    name: "adding a pull_request trigger is caught",
    yml: DEPLOY.replace(/^\s*workflow_dispatch:\n/m, "  pull_request:\n  workflow_dispatch:\n"),
    check: (yml) => assertNoAutomaticTrigger(yml, "mutated"),
  },
  {
    name: "removing manual dispatch is caught",
    yml: DEPLOY.replace(/^\s*workflow_dispatch:\n/m, ""),
    check: (yml) => assertManualDispatchAvailable(yml, "mutated"),
  },
  {
    name: "unpinning the checkout ref is caught",
    yml: DEPLOY.replace(/^\s*ref: main\n/m, "          ref: ${{ github.ref }}\n"),
    check: (yml) => assertCanonicalCheckout(yml, "mutated"),
  },
  {
    name: "removing the environment gate is caught",
    yml: DEPLOY.replace(/^\s*environment:\n\s*name: production\n/m, ""),
    check: (yml) => assertEnvironmentGate(yml, "mutated"),
  },
  {
    name: "renaming the environment is caught",
    yml: DEPLOY.replace(/^\s*name: production\s*$/m, "      name: prod"),
    check: (yml) => assertEnvironmentGate(yml, "mutated"),
  },
  {
    name: "a second deploy invocation is caught",
    yml: `${DEPLOY}\n      - name: sneaky\n        run: npm run deploy\n`,
    check: (yml) => assertSingleDeployInvocation(yml, "mutated"),
  },
];

for (const { name, yml, check } of mutations) {
  test(`mutation: ${name}`, () => {
    assert.notEqual(yml, DEPLOY, "mutation must actually change the workflow text");
    assert.throws(() => check(yml), `the guard must reject: ${name}`);
  });
}
