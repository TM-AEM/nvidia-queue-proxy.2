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

// Robust trigger parser that extracts all declared events across inline scalars,
// inline arrays, YAML lists, and indented map keys.
export function parseWorkflowTriggers(yml) {
  const match = yml.match(/^on:\s*(.*)$/m);
  if (!match) return [];
  const inline = match[1].trim();
  const triggers = new Set();

  if (inline) {
    if (inline.startsWith("[") && inline.endsWith("]")) {
      const items = inline.slice(1, -1).split(",");
      for (const item of items) {
        const cleaned = item.trim().replace(/^['"]|['"]$/g, "");
        if (cleaned) triggers.add(cleaned);
      }
    } else {
      const cleaned = inline.split("#")[0].trim().replace(/^['"]|['"]$/g, "");
      if (cleaned) triggers.add(cleaned);
    }
  }

  // Scan subsequent indented lines until next top-level unindented key
  const afterOn = yml.slice(match.index + match[0].length);
  const lines = afterOn.split(/\r?\n/);
  for (const line of lines) {
    if (/^\s*$/.test(line) || /^\s*#/.test(line)) continue;
    if (/^[^\s]/.test(line)) break;

    const listMatch = line.match(/^\s{1,4}-\s*([a-zA-Z0-9_-]+)/);
    if (listMatch) {
      triggers.add(listMatch[1].trim());
      continue;
    }

    const mapMatch = line.match(/^\s{1,4}([a-zA-Z0-9_-]+)\s*:/);
    if (mapMatch) {
      triggers.add(mapMatch[1].trim());
    }
  }

  return Array.from(triggers);
}

// The trigger block extracts lines from top-level `on:` until the next top-level key.
const triggerBlock = (yml) => {
  const match = yml.match(/^on:\s*(.*)$/m);
  if (!match) return "";
  const after = yml.slice(match.index);
  const lines = after.split(/\r?\n/);
  const blockLines = [lines[0]];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (/^\s*$/.test(line) || /^\s*#/.test(line)) {
      blockLines.push(line);
      continue;
    }
    if (/^[^\s]/.test(line)) break;
    blockLines.push(line);
  }
  return blockLines.join("\n");
};

// ---------------------------------------------------------------------------
// Reusable safety assertions. Each throws on a violation. They run against the
// real workflows (must pass) and against mutated copies (must throw), so the
// mutation tests prove the assertions catch a regression instead of passing
// vacuously.
// ---------------------------------------------------------------------------

function assertNoAutomaticTrigger(yml, label) {
  const triggers = parseWorkflowTriggers(yml);
  const automatic = triggers.filter((t) => t !== "workflow_dispatch");
  assert.equal(
    automatic.length,
    0,
    `${label}: must not have automatic triggers, found: [${automatic.join(", ")}]`
  );
}

function assertManualDispatchAvailable(yml, label) {
  const triggers = parseWorkflowTriggers(yml);
  assert.ok(
    triggers.includes("workflow_dispatch"),
    `${label}: must remain manually dispatchable via \`workflow_dispatch\``
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

// Matches a real `npm run deploy` invocation while excluding `deploy:dry-run`.
// It deliberately does not require end-of-line so that a flagged invocation
// (`npm run deploy --env production`) is still detected.
const DEPLOY_INVOCATION = /npm run deploy(?!:)/g;

function assertSingleDeployInvocation(yml, label) {
  const deployCalls = yml.match(DEPLOY_INVOCATION) ?? [];
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
      new RegExp(DEPLOY_INVOCATION.source).test(yml) ||
      /wrangler deploy(?!\s*--dry-run)/.test(yml);
    if (!deploys) continue;

    const triggers = parseWorkflowTriggers(yml);
    const automatic = triggers.filter((t) => t !== "workflow_dispatch");
    if (automatic.length > 0) {
      offenders.push(`${name} (${automatic.join(", ")})`);
    }
  }

  assert.deepEqual(
    offenders,
    [],
    `workflows with an automatic trigger that also deploy: ${offenders.join(", ") || "(none)"}`
  );
});

// The smoke-test workflow chains off the deploy workflow by *name*
// (`workflow_run.workflows`). If the deploy workflow is renamed without
// updating the reference, the post-deploy smoke test silently stops running.
test("smoke-test workflow: workflow_run references the actual deploy workflow name", () => {
  const deployName = readWorkflow("deploy.yml").match(/^name:\s*(.+)$/m)?.[1].trim();
  assert.ok(deployName, "deploy.yml must declare a name");

  const smoke = readWorkflow("production-smoke-test.yml");
  assert.match(
    smoke,
    new RegExp(`workflows:\\s*\\["${deployName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"\\]`),
    `the smoke test must reference the deploy workflow by its exact name (${deployName})`
  );
});

// ---------------------------------------------------------------------------
// Supply-chain: every third-party action must be pinned to an immutable commit
// SHA. A mutable tag (`@v4`) can be repointed by the upstream owner and would
// then run unreviewed code inside a job that holds production credentials.
// ---------------------------------------------------------------------------

function assertActionsPinnedToSha(yml, label) {
  const usesLines = yml.match(/^\s*uses:\s*\S+/gm) ?? [];
  assert.ok(usesLines.length > 0, `${label}: expected at least one \`uses:\` reference`);
  for (const line of usesLines) {
    const ref = line.replace(/^\s*uses:\s*/, "").trim();
    assert.match(
      ref,
      /^[^@\s]+@[0-9a-f]{40}$/,
      `${label}: action \`${ref}\` must be pinned to a full 40-char commit SHA`
    );
  }
}

function assertNoProductionSecretsOutsideDeploy(yml, label) {
  assert.doesNotMatch(
    yml,
    /secrets\.(CLOUDFLARE_API_TOKEN|CLOUDFLARE_ACCOUNT_ID)/,
    `${label}: production Cloudflare credentials must not be referenced here`
  );
}

test("all workflows: every action is pinned to an immutable commit SHA", () => {
  for (const name of listWorkflows()) {
    assertActionsPinnedToSha(readWorkflow(name), name);
  }
});

test("all workflows: production Cloudflare secrets appear only in the gated deploy workflow", () => {
  for (const name of listWorkflows()) {
    if (name === "deploy.yml") continue;
    assertNoProductionSecretsOutsideDeploy(readWorkflow(name), name);
  }
});

test("deploy workflow: production Cloudflare secrets are consumed there", () => {
  const yml = readWorkflow("deploy.yml");
  assert.match(yml, /secrets\.CLOUDFLARE_API_TOKEN/, "deploy.yml must read CLOUDFLARE_API_TOKEN");
  assert.match(yml, /secrets\.CLOUDFLARE_ACCOUNT_ID/, "deploy.yml must read CLOUDFLARE_ACCOUNT_ID");
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
    name: "adding an inline array trigger is caught",
    yml: DEPLOY.replace(/^on:\n/m, "on: [push, workflow_dispatch]\n"),
    check: (yml) => assertNoAutomaticTrigger(yml, "mutated"),
  },
  {
    name: "adding a schedule trigger is caught",
    yml: DEPLOY.replace(/^\s*workflow_dispatch:\n/m, "  schedule:\n    - cron: '0 0 * * *'\n  workflow_dispatch:\n"),
    check: (yml) => assertNoAutomaticTrigger(yml, "mutated"),
  },
  {
    name: "adding a pull_request_target trigger is caught",
    yml: DEPLOY.replace(/^\s*workflow_dispatch:\n/m, "  pull_request_target:\n  workflow_dispatch:\n"),
    check: (yml) => assertNoAutomaticTrigger(yml, "mutated"),
  },
  {
    name: "adding a release trigger is caught",
    yml: DEPLOY.replace(/^\s*workflow_dispatch:\n/m, "  release:\n    types: [published]\n  workflow_dispatch:\n"),
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
  {
    name: "unpinning an action back to a mutable tag is caught",
    yml: DEPLOY.replace(
      /uses: actions\/checkout@[0-9a-f]{40}/,
      "uses: actions/checkout@v4"
    ),
    check: (yml) => assertActionsPinnedToSha(yml, "mutated"),
  },
  {
    name: "leaking a production secret into the CI workflow is caught",
    yml:
      readWorkflow("ci.yml") +
      '\n      - run: echo "$' +
      "{{ secrets.CLOUDFLARE_API_TOKEN }}\"\n",
    check: (yml) => assertNoProductionSecretsOutsideDeploy(yml, "mutated"),
  },
  {
    name: "a flagged second deploy invocation is caught",
    yml: `${DEPLOY}\n      - name: sneaky\n        run: npm run deploy --env production\n`,
    check: (yml) => assertSingleDeployInvocation(yml, "mutated"),
  },
  {
    name: "a renamed deploy workflow breaking the smoke-test reference is caught",
    yml: readWorkflow("production-smoke-test.yml").replace(
      /workflows:\s*\["Test and Deploy NVIDIA Queue Proxy"\]/,
      'workflows: ["Renamed Deploy Workflow"]'
    ),
    check: (yml) => {
      const deployName = "Test and Deploy NVIDIA Queue Proxy";
      assert.match(
        yml,
        new RegExp(`workflows:\\s*\\["${deployName}"\\]`),
        "mutated: smoke test must reference the exact deploy workflow name"
      );
    },
  },
];

for (const { name, yml, check } of mutations) {
  test(`mutation: ${name}`, () => {
    assert.notEqual(yml, DEPLOY, "mutation must actually change the workflow text");
    assert.throws(() => check(yml), `the guard must reject: ${name}`);
  });
}
