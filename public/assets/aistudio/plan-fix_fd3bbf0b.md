# NVIDIA Queue Proxy — Remediation & Completion Plan

**File:** `plan-fix.md`  
**Repository:** `TM-AEM/nvidia-queue-proxy.2`  
**Baseline branch:** `main`  
**Baseline commit reviewed:** `6bfb53a07afbdda923edc6e79319b628379450ba`  
**Plan status:** Proposed — audit findings must be revalidated against the current branch before implementation  
**Scope:** Repository integrity, Cloudflare Worker correctness, security, client compatibility, queue reliability, CI/CD, operational documentation, and final release verification.

---

## 1. Purpose

This plan defines the work required to make the NVIDIA Queue Proxy repository safe to maintain, predictable to deploy, compatible with its intended client, and verifiable in production.

The project is a Cloudflare Worker proxy for NVIDIA API traffic. Its intended responsibilities include:

- Protecting the upstream NVIDIA API credential.
- Authenticating callers with a separate proxy credential.
- Serializing or rate-limiting upstream requests.
- Handling NVIDIA `429` responses and `Retry-After`.
- Preserving streaming responses.
- Applying bounded request/response reads and timeouts.
- Preventing queue corruption, duplicate dispatch, and resource leaks.
- Providing operational health/readiness endpoints.
- Supporting reproducible tests and deployment.

This document is a work plan, not evidence that every listed item is currently broken. Each task must first verify the current implementation and then either fix a confirmed defect or record why the task is already satisfied.

## 2. Baseline and known evidence

The audit was performed against `main` at:

`6bfb53a07afbdda923edc6e79319b628379450ba`

The GitHub Actions run inspected for this commit reported:

- 167 tests passing (162 in the main test suite and 5 runtime/workerd tests).
- Wrangler deployment dry-run passing.
- Cloudflare deployment step completing successfully.

These results are useful evidence, but they do **not** by themselves prove that the deployed public endpoint is reachable, configured correctly, or serving the intended build. A post-deployment smoke test is still required.

### 2.1 Repository structure observed

The repository contains an apparent split between:

- The newer Worker project under `nvidia-queue-proxy-main/`.
- A separate root-level Worker entry point at `src/index.js` with a root `wrangler.jsonc`.
- Root-level frontend/AI-Studio scaffolding, including `src/App.tsx`, `src/main.tsx`, `vite.config.ts`, `index.html`, and `metadata.json`.

The inspected deployment workflow and the newer Worker configuration use the nested project. The root-level Worker appears older and materially different. This creates a risk that a developer running Wrangler from the repository root could deploy a different implementation from the one tested by CI.

The root-level `.env.example` also appears to contain frontend/Gemini-oriented settings rather than a complete Worker configuration example. The root `.gitignore` did not visibly exclude all common Wrangler local-secret paths at the time of review.

### 2.2 Existing implementation observed in the nested Worker

The nested Worker already contains significant functionality, including:

- Request authentication and upstream NVIDIA authorization.
- Queue/rate-limit logic.
- Retry handling for upstream `429` responses.
- Backoff and jitter logic.
- Request and response size limits.
- Request-body read timeout handling.
- Aggregate request-body memory limits.
- Streaming response handling.
- Health/readiness routes.
- Response-header sanitization.
- Tests for many of these behaviors.

Do not replace or simplify these mechanisms without first understanding their invariants and regression tests.

### 2.3 Open audit issues observed

The following repository issues were open during the audit:

| Issue | Subject | Initial interpretation |
|---|---|---|
| #5 / AUDIT-01 | Bound concurrent request-body reads before queue admission | Implementation appears substantially present; verify and reconcile issue |
| #6 / AUDIT-02 | Prevent queue stalls when upstream stream cancellation hangs | Bounded cancellation behavior exists, but a non-terminating upstream may intentionally retain a queue slot |
| #7 / AUDIT-03 | Browser CORS requirements and origin allowlist | Product/client architecture decision remains necessary |
| #8 / AUDIT-04 | `GET /v1/models` support | Conditional on actual client requirements |
| #9 / AUDIT-05 | Respect upstream `Retry-After` | Implementation appears substantially present; verify edge cases and reconcile issue |
| #10 / AUDIT-06 | Separate liveness/readiness and post-deploy smoke check | Endpoints exist; post-deploy workflow verification remains |
| #11 / AUDIT-07 | Reproducible dependencies and forwarded-header sanitization | Lockfile and response sanitization exist; request-header policy and CI install mode need verification |
| #12 / AUDIT-08 | Wrangler observability warning | Current config/dry-run suggest this may already be resolved |
| #13 / AUDIT-09 | Operational README | Documentation exists but contains stale deployment/test details |
| #14 / AUDIT-10 | Final release verification | Final gate; was blocked by AUDIT-05 at audit time |

Issue status must be checked again before implementation. Do not close an issue based only on this document.

---

## 3. Engineering rules and non-negotiable constraints

All implementation work must follow these rules:

1. **Audit first.** Inspect the current branch, working tree, relevant source, tests, workflow, and issue state before changing anything.
2. **Preserve working behavior.** Do not remove existing queue, retry, streaming, timeout, memory-bound, or security behavior merely to simplify the code.
3. **One canonical Worker.** There must be one clearly documented and enforced deployment entry point.
4. **No secret exposure.** Never put NVIDIA API keys or proxy keys in frontend bundles, browser JavaScript, committed files, logs, test snapshots, URLs, or error messages.
5. **No unsafe CORS shortcut.** CORS is not authentication. Never solve browser access by exposing a shared proxy secret to public client-side code.
6. **Bound resource use.** Every potentially unbounded read, wait, retry, queue, and stream operation must have an explicit policy and test.
7. **Preserve streaming.** Do not buffer successful streaming responses unless a documented requirement makes buffering unavoidable.
8. **Deterministic tests.** Use fake upstreams, controlled clocks/sleep functions, and deterministic concurrency barriers where possible. Avoid real NVIDIA calls in normal CI.
9. **No destructive Git operations.** Do not run `git reset --hard`, `git clean`, force-push, history rewriting, or broad deletion commands.
10. **Protect unrelated work.** Do not overwrite or remove unrelated files or changes. Do not modify `.openhands/`.
11. **No unapproved release action.** Do not commit, push, merge, deploy, rotate production secrets, or change production configuration without explicit authorization for that action.
12. **Evidence-based status.** Mark a task PASS only when its acceptance criteria are demonstrated by source inspection and tests or operational evidence.

---

# 4. Phased remediation plan

## PHASE 0 — Re-establish the baseline

**Priority:** P0  
**Type:** Read-only verification  
**Goal:** Ensure the audit is still based on the current repository state before any implementation.

### Tasks

- [ ] Fetch the latest repository metadata and default branch.
- [ ] Confirm the current branch and exact HEAD SHA.
- [ ] Inspect the working tree and identify uncommitted changes if using a checkout.
- [ ] Compare current `main` with the audited baseline commit.
- [ ] List all files and identify added, removed, renamed, or duplicated Worker/config files.
- [ ] Re-read all open AUDIT issues and their dependencies.
- [ ] Re-run the existing test suite without changing files.
- [ ] Capture the current CI run and deployment status.
- [ ] Confirm whether the production Worker name and public URL have changed.

### Deliverables

- Baseline report with branch, SHA, working-tree state, and test totals.
- Updated issue/task matrix.
- Explicit list of differences from the audit baseline.

### Acceptance criteria

- Exact current HEAD is recorded.
- No implementation files are modified in this phase.
- Existing test failures, if any, are recorded before remediation.
- The plan is adjusted if the repository has materially changed.

---

## PHASE 1 — Repository integrity and canonical deployment source

**Priority:** P0  
**Related paths:** root `src/index.js`, root `wrangler.jsonc`, `nvidia-queue-proxy-main/`, deployment workflows, package manifests.

**Goal:** Eliminate ambiguity about which Worker implementation is authoritative and prevent accidental deployment of an older Worker.

### 1.1 Choose the canonical project layout

- [ ] Inspect all package manifests, Wrangler configs, workflow working directories, and deployment commands.
- [ ] Confirm whether `nvidia-queue-proxy-main/` is the intended production Worker project.
- [ ] Confirm whether root-level frontend/AI-Studio files are still needed.
- [ ] Identify every command that can invoke Wrangler from the repository root.
- [ ] Document the canonical working directory and deployment command.

### 1.2 Resolve the duplicate Worker implementation safely

Preferred approach, subject to repository dependency inspection:

- Keep the nested Worker as the canonical production implementation if that is what CI currently tests and deploys.
- Ensure the root-level Wrangler configuration cannot silently deploy the stale root `src/index.js`.
- Either remove the obsolete root Worker/config after proving they are unused, or convert the root into an explicit non-deployable workspace/documentation entry point.
- If the root frontend is still needed, keep it separate from production Worker deployment and ensure its scripts cannot accidentally deploy the Worker.
- Do not blindly copy one implementation over another. Compare behavior and tests first.

### 1.3 Make deployment commands unambiguous

- [ ] Use explicit workflow `working-directory` settings.
- [ ] Use explicit package scripts for test, dry-run, and deploy.
- [ ] Make deployment fail if invoked from an unsupported directory.
- [ ] Add a repository check that detects an unexpected second production Wrangler config or duplicate Worker entry point.
- [ ] Ensure CI tests the exact directory and configuration used for deployment.
- [ ] Ensure the production deployment command and dry-run command use the same Wrangler config.

### 1.4 Protect local secrets and generated files

Review and update `.gitignore` in the relevant project roots to exclude, as applicable:

```gitignore
.env
.env.*
!.env.example
.dev.vars
.dev.vars.*
.wrangler/
node_modules/
dist/
coverage/
```

Requirements:

- [ ] Do not ignore the committed safe example file.
- [ ] Check whether any secret-bearing files are already tracked.
- [ ] If a secret was committed historically, treat it as compromised and recommend rotation; deleting it in a new commit does not erase Git history.
- [ ] Do not print secret values in logs or audit output.

### Deliverables

- One canonical Worker location.
- One documented production deployment command.
- Guard/check against accidental deployment of the obsolete entry point.
- Updated ignore rules and safe environment example.

### Acceptance criteria

- CI tests and deploys the same Worker source/config.
- Running the documented command from the documented location deploys the canonical Worker.
- A wrong-directory or stale-config deployment is blocked or clearly fails.
- No real secret is committed.
- Existing functionality and tests remain intact.

---

## PHASE 2 — Client contract and compatibility decisions

**Priority:** P1  
**Related paths:** Worker router, authentication middleware, `queue-test.html`, README, client integration documentation.

**Goal:** Define exactly how MonkeyCode or any other client reaches the proxy, then implement only the routes and browser behavior actually required.

### 2.1 Establish the real client topology

Before changing CORS or authentication, collect evidence for:

- [ ] Is the client a browser application, a server-side backend, or both?
- [ ] Does the browser call the proxy directly?
- [ ] Is the proxy key stored in a server-side secret store or exposed in browser code?
- [ ] What exact HTTP methods and paths does the client call?
- [ ] Does the client call `GET /v1/models` during startup/model discovery?
- [ ] Does the client require streaming responses?
- [ ] What `Origin` values are sent, if any?
- [ ] Does the client use OpenAI-compatible request/response formats or NVIDIA-specific formats?

Do not infer the answer from a frontend URL alone. Inspect actual network requests or client configuration when available.

### 2.2 CORS decision

If all requests originate from a trusted backend:

- [ ] Keep browser CORS disabled unless another documented browser use case exists.
- [ ] Document that the proxy key must remain server-side.

If direct browser access is a genuine requirement:

- [ ] Define an explicit origin allowlist.
- [ ] Reject unknown origins.
- [ ] Handle preflight `OPTIONS` only for allowed origins and supported methods/headers.
- [ ] Return the required `Access-Control-Allow-Origin`, `Vary: Origin`, and limited allow-header/method values.
- [ ] Never use wildcard origins with credentialed requests.
- [ ] Never embed a shared proxy key in public frontend assets.
- [ ] Prefer a same-origin backend/BFF that holds the secret if the browser cannot safely authenticate independently.
- [ ] Test allowed origin, denied origin, preflight, missing origin, and disallowed headers.

### 2.3 `GET /v1/models` decision

- [ ] Inspect the actual client behavior.
- [ ] If the client requires model discovery, implement a bounded, authenticated `GET /v1/models` route.
- [ ] Decide whether it should proxy NVIDIA's model list or return a controlled static/filtered list.
- [ ] Do not expose internal/private models unintentionally.
- [ ] Apply the same proxy authentication and safe response-header policy.
- [ ] Add method/path tests and ensure the route does not bypass rate-limit or authorization policy.
- [ ] If not required, document why it is intentionally unsupported and retain a clear 404/405 behavior.

### 2.4 Request contract

Document:

- Supported methods.
- Supported path prefix.
- Authentication header name.
- Streaming behavior.
- Request-size limits.
- Timeout behavior.
- Retry behavior.
- Supported content types.
- Error response format.
- Model discovery behavior.
- CORS policy.

### Deliverables

- Client topology decision record.
- CORS decision and tests, if applicable.
- `GET /v1/models` implementation or documented non-support decision.
- Client integration contract.

### Acceptance criteria

- Every supported client request has an explicit tested route.
- No public browser bundle contains a shared proxy credential.
- CORS behavior matches the documented topology.
- Unsupported methods and paths fail predictably.
- No compatibility feature bypasses authentication or queue controls.

---

## PHASE 3 — Authentication, request-header policy, and response sanitization

**Priority:** P1  
**Related paths:** authentication middleware, upstream request construction, response-header sanitizer, error handlers, tests.

**Goal:** Prevent credential leakage, unsafe header forwarding, and inconsistent response metadata.

### 3.1 Inbound authentication

- [ ] Confirm the proxy credential is read only from the intended server-side environment binding.
- [ ] Compare credentials without logging either value.
- [ ] Reject missing, empty, malformed, or invalid credentials.
- [ ] Ensure auth failures do not consume upstream queue capacity unnecessarily.
- [ ] Ensure health/readiness endpoints expose no secrets or sensitive configuration.
- [ ] Confirm upstream NVIDIA credentials never come from client input.
- [ ] Confirm client-supplied `Authorization` is overwritten, not trusted as the NVIDIA credential.

### 3.2 Outbound request-header allow/deny policy

Build an explicit policy for headers forwarded to NVIDIA.

At minimum, inspect and test:

- `Host`
- `Connection`
- Headers named by the `Connection` header
- `Content-Length`
- `Transfer-Encoding`
- `Proxy-Authorization`
- `Cookie`
- `Set-Cookie`
- `X-Proxy-Key`
- Client `Authorization`
- Forwarding and internal network topology headers
- Platform/runtime-specific headers

Requirements:

- [ ] Remove hop-by-hop headers and all headers nominated by `Connection`.
- [ ] Remove the proxy credential before upstream dispatch.
- [ ] Remove cookies and proxy authorization unless a documented upstream contract explicitly requires them.
- [ ] Set the NVIDIA `Authorization` header from the Worker secret.
- [ ] Preserve only documented content-negotiation and vendor headers needed by the NVIDIA API.
- [ ] Avoid an overly broad denylist that silently breaks required NVIDIA features; test allowed vendor headers too.
- [ ] Never forward a client-supplied NVIDIA credential.

### 3.3 Response-header policy

- [ ] Keep the existing response sanitizer and its regression tests.
- [ ] Strip `Set-Cookie`, authentication echoes, hop-by-hop headers, and connection-nominated headers.
- [ ] Strip internal tracing/topology headers where appropriate.
- [ ] Preserve required content type, cache policy, request IDs, and vendor headers where safe.
- [ ] Do not forward stale `Content-Length` after transforming a body.
- [ ] When replacing/truncating a body, remove or recompute body metadata such as `Content-Encoding`, `Content-Range`, `ETag`, and digest headers when they no longer describe the returned body.
- [ ] Verify that 429 retry responses receive the same sanitization as normal responses.
- [ ] Verify proxy-generated 4xx/5xx responses cannot inherit upstream credentials or unsafe headers.

### 3.4 Error and log hygiene

- [ ] Ensure API keys never appear in exception messages returned to clients.
- [ ] Ensure logs do not include authorization headers or full secret-bearing environment objects.
- [ ] Avoid logging full request bodies by default.
- [ ] Use a safe request correlation ID where helpful.
- [ ] Ensure error bodies do not disclose internal stack traces or environment values.

### Deliverables

- Documented request and response header policies.
- Security regression tests.
- Safe error/logging behavior.

### Acceptance criteria

- Tests prove proxy and NVIDIA secrets are not forwarded to the wrong destination or exposed in responses/logs.
- Connection-nominated headers are removed.
- Safe required vendor headers still work.
- Transformed response bodies do not retain invalid representation metadata.

---

## PHASE 4 — Queue, rate-limit, retry, and cancellation correctness

**Priority:** P1  
**Related paths:** Durable Object/queue implementation, retry loop, timeout helpers, stream lifecycle, concurrency tests.

**Goal:** Preserve strict upstream request scheduling while ensuring failures, cancellations, and retries cannot corrupt the queue or exceed resource limits.

### 4.1 Queue invariants

Write down and test these invariants:

- [ ] At most the configured number of upstream operations are active.
- [ ] Requests are admitted in the documented order (FIFO if FIFO is the intended contract).
- [ ] A request does not dispatch before its queue slot and rate-limit eligibility are granted.
- [ ] A failed request releases resources exactly once.
- [ ] A retry remains part of the same logical queued operation unless the design explicitly states otherwise.
- [ ] A caller disconnect cannot cause a second dispatch.
- [ ] A timed-out request cannot later be treated as a successful active request.
- [ ] Queue counters and RPM accounting cannot become negative or exceed configured bounds through duplicate cleanup.

### 4.2 RPM accounting

- [ ] Confirm whether the RPM limit counts logical requests or every upstream attempt.
- [ ] Use one consistent definition across implementation, tests, metrics, and README.
- [ ] Ensure 429 retries are accounted for according to that definition.
- [ ] Verify the sliding-window boundary at exactly the configured interval.
- [ ] Verify state survives Durable Object lifecycle/restart according to the persistence design.
- [ ] Test concurrent arrivals at the limit boundary.
- [ ] Test multiple Worker instances targeting the same Durable Object identity.
- [ ] Ensure invalid requests rejected before dispatch do not accidentally count as upstream attempts.

### 4.3 Retry-After and backoff

- [ ] Parse integer seconds and HTTP-date forms if supported by the implementation contract.
- [ ] Reject malformed, negative, non-finite, or unreasonable values safely.
- [ ] Never retry earlier than a valid upstream `Retry-After` instruction.
- [ ] Apply a documented maximum wait.
- [ ] Combine exponential backoff and jitter without reducing a valid required wait.
- [ ] Ensure retries are bounded by a maximum attempt count and an overall request deadline.
- [ ] Ensure non-429 upstream failures follow an explicit retry/no-retry policy.
- [ ] Ensure response bodies are consumed/cancelled safely before retrying.
- [ ] Ensure 429 bodies are read with a strict byte limit and timeout.
- [ ] Ensure a truncated synthetic error response has consistent headers.

### 4.4 Timeout and cancellation lifecycle

- [ ] Identify separate deadlines for request-body reading, queue waiting, upstream headers, upstream body/stream, and total request lifetime.
- [ ] Ensure timers are cleared on success, failure, cancellation, and timeout.
- [ ] Ensure AbortSignals are propagated where supported.
- [ ] Ensure cancellation cleanup is idempotent.
- [ ] Test upstream fetch that never resolves.
- [ ] Test upstream response whose body never completes.
- [ ] Test upstream body cancellation that never settles.
- [ ] Test client disconnect during queue wait.
- [ ] Test client disconnect after upstream dispatch.
- [ ] Test cancellation racing with a successful response.
- [ ] Verify no duplicate release or queue advancement occurs.

### 4.5 Non-terminating upstream cancellation policy

The current observed design appears to retain a queue slot if upstream cancellation does not settle within the configured grace period. This is a fail-closed choice that avoids dispatching another request while the previous upstream operation may still be active, but it can permanently reduce service capacity.

Do not change this to a forced release without proving that the old upstream operation can no longer consume capacity or overlap in a way that violates the RPM/serialization contract.

Choose and document one policy:

**Policy A — Fail-closed (safer serialization):**
- Retain the slot when upstream termination cannot be established.
- Expose a safe operational signal/metric for the stuck slot.
- Document operator recovery steps.
- Ensure the behavior is bounded at the Worker/platform lifecycle level where possible.

**Policy B — Bounded forced recovery (higher availability, more risk):**
- Release after a hard deadline only if the architecture can prove that late upstream work cannot violate the concurrency/RPM guarantee.
- Add tests for late completion and overlapping operations.
- Document the weaker guarantee explicitly.

### Deliverables

- Queue state-machine/invariant documentation.
- Deterministic concurrency and race tests.
- Explicit stuck-cancellation policy.
- Verified RPM and retry accounting.

### Acceptance criteria

- No duplicate upstream dispatch.
- No early retry against a valid `Retry-After`.
- No unbounded retry loop.
- No unbounded request-body or retry-body read.
- Queue release happens exactly once under all tested completion paths.
- Cancellation policy is explicit and supported by tests.

---

## PHASE 5 — Resource bounds and failure isolation

**Priority:** P1  
**Related paths:** body readers, streaming helpers, queue admission, configuration parser, response construction.

**Goal:** Ensure the Worker remains bounded under large inputs, slow clients, malformed configuration, and upstream failures.

### 5.1 Request-body limits

- [ ] Verify the maximum individual request-body size.
- [ ] Verify the aggregate concurrent request-body memory budget.
- [ ] Verify the maximum number of simultaneous body readers.
- [ ] Verify declared `Content-Length` is not blindly trusted.
- [ ] Verify chunked/unknown-length bodies are still bounded while reading.
- [ ] Verify timeout and cancellation release aggregate budget exactly once.
- [ ] Verify a request rejected by size limit is never dispatched upstream.

### 5.2 Response-body limits

- [ ] Verify bounded reads for upstream 429/error bodies.
- [ ] Verify successful streaming responses are not accidentally buffered.
- [ ] Verify synthetic error bodies are size-limited.
- [ ] Verify a stalled upstream body cannot hold resources forever without a defined deadline.
- [ ] Verify response stream cancellation propagates appropriately.

### 5.3 Configuration validation

- [ ] Enumerate every environment variable and binding.
- [ ] Define defaults and valid ranges.
- [ ] Reject malformed numeric values rather than silently accepting dangerous fallback behavior.
- [ ] Reject zero/negative values where invalid.
- [ ] Apply safe upper bounds to timeouts, body sizes, RPM, retry counts, and concurrency.
- [ ] Distinguish required secrets from optional settings.
- [ ] Fail readiness when required configuration is missing or invalid.
- [ ] Never print secret values in validation errors.
- [ ] Test boundary values, malformed strings, empty values, and missing bindings.

### Deliverables

- Configuration reference.
- Boundary tests.
- Verified memory/time limits.

### Acceptance criteria

- No unbounded body reads.
- No silent unsafe configuration fallback.
- Resource counters return to baseline after success, rejection, timeout, and cancellation.
- Readiness correctly reports invalid required configuration without exposing secrets.

---

## PHASE 6 — Health, readiness, observability, and safe operations

**Priority:** P1  
**Related paths:** health routes, Durable Object state, logging, Wrangler observability config, deployment workflow.

**Goal:** Make the service diagnosable without exposing sensitive data and make post-deployment verification meaningful.

### 6.1 Liveness vs readiness

- [ ] Define `/health` as a lightweight liveness signal.
- [ ] Define `/ready` as a readiness/configuration/dependency signal.
- [ ] Keep both endpoints free of secret values.
- [ ] Decide which checks are local and which require external dependencies.
- [ ] Avoid making liveness fail merely because NVIDIA is temporarily rate-limiting requests unless that is explicitly the intended semantic.
- [ ] Ensure readiness does not create expensive upstream traffic on every probe.
- [ ] Add tests for missing/invalid bindings and expected status codes.

### 6.2 Observability

- [ ] Review current Wrangler observability settings.
- [ ] Confirm logs and traces do not capture secrets or request bodies.
- [ ] Add structured, low-cardinality operational events where useful.
- [ ] Track queue wait, upstream latency, retry count, 429 count, timeout count, cancellation/stuck-slot count, and rejected body sizes where platform support permits.
- [ ] Avoid high-cardinality labels such as full prompt text, API key, or arbitrary user input.
- [ ] Define retention and cost implications for logs/traces.

### 6.3 Operational runbook

Document how to:

- Verify liveness and readiness.
- Identify invalid/missing configuration without printing secrets.
- Diagnose upstream 429 behavior.
- Diagnose a stuck queue slot.
- Check the latest deployed commit/version.
- Roll back to a known-good deployment using the approved Cloudflare workflow.
- Rotate NVIDIA and proxy secrets safely.
- Validate that no secret has been committed.

### Deliverables

- Liveness/readiness contract.
- Safe operational signals.
- Runbook.

### Acceptance criteria

- Health endpoints are tested.
- Logs are useful but secret-safe.
- An operator can diagnose common failures without inspecting secret values.

---

## PHASE 7 — Reproducible dependencies and CI hardening

**Priority:** P1  
**Related paths:** `package.json`, `package-lock.json`, GitHub Actions workflow files, test scripts.

**Goal:** Ensure CI installs exactly the locked dependency graph and validates the same artifact that is deployed.

### 7.1 Dependency reproducibility

- [ ] Confirm `package.json` and `package-lock.json` are in sync.
- [ ] Replace `npm install` with `npm ci` in CI where a lockfile is committed.
- [ ] Use the intended Node.js version explicitly.
- [ ] Review dependency audit output and transitive dependency changes.
- [ ] Avoid unnecessary new dependencies.
- [ ] Ensure the root and nested project do not install conflicting or unused dependency sets.

### 7.2 CI checks

Required checks should include:

- [ ] Formatting/linting if configured.
- [ ] Static syntax/type checks as applicable.
- [ ] Unit/regression tests.
- [ ] Runtime/workerd tests.
- [ ] Wrangler configuration validation.
- [ ] Wrangler dry-run using the exact production config.
- [ ] A check that prevents stale root Worker deployment.
- [ ] Secret-presence checks that reveal only whether a required secret exists, never its value.

### 7.3 Workflow hardening

- [ ] Pin actions to reviewed versions or immutable SHAs according to repository policy.
- [ ] Set minimal GitHub token permissions.
- [ ] Avoid exposing secrets to pull-request workflows from untrusted forks.
- [ ] Ensure deployment runs only from the intended branch/event and approved environment.
- [ ] Use a deployment concurrency group to prevent overlapping production deploys.
- [ ] Preserve build/test logs without printing credentials.
- [ ] Ensure the deployed commit SHA is recorded.

### Deliverables

- Reproducible CI install.
- Hardened workflow.
- CI checks for canonical source and deployment config.

### Acceptance criteria

- CI passes from a clean checkout using the lockfile.
- CI tests and dry-runs the exact production Worker.
- Untrusted PRs cannot access production secrets.
- Deployment is tied to a recorded commit SHA.

---

## PHASE 8 — Post-deployment smoke test and deployment safety

**Priority:** P1  
**Related paths:** deployment workflow, health/readiness routes, environment configuration, README.

**Goal:** Verify the deployed service after deployment rather than treating a successful deploy command as proof of runtime health.

### 8.1 Safe smoke-test design

- [ ] Obtain the actual deployed Worker URL from the authoritative configuration or deployment output.
- [ ] Do not guess or hardcode a stale `workers.dev` hostname.
- [ ] Call `/health` without credentials if the liveness contract allows it.
- [ ] Call `/ready` and assert the documented status.
- [ ] Use a short timeout and bounded retries for transient propagation delay.
- [ ] Do not send a real NVIDIA API key in a smoke-test request.
- [ ] Do not send a paid NVIDIA inference request as a default deployment check.
- [ ] If testing authentication, send a deliberately invalid test credential and assert a safe rejection.
- [ ] Ensure the smoke test cannot accidentally trigger expensive upstream inference.
- [ ] Fail the workflow if the service does not become healthy within the defined window.

### 8.2 Deployment result

- [ ] Record deployed environment, Worker name, commit SHA, workflow run, and smoke-test result.
- [ ] Do not print secret values.
- [ ] Distinguish deployment command success from post-deployment health success.
- [ ] Define rollback/stop conditions.

### Deliverables

- Automated post-deployment health/readiness smoke test.
- Deployment summary tied to commit SHA.

### Acceptance criteria

- A deployment is not marked fully successful until smoke tests pass.
- Smoke tests are safe, bounded, and do not incur unnecessary NVIDIA usage.
- The tested endpoint is derived from the authoritative deployment configuration.

---

## PHASE 9 — Documentation and manual test tooling

**Priority:** P2  
**Related paths:** `nvidia-queue-proxy-main/README.md`, `nvidia-queue-proxy-main/src/queue-test.html`, `.env.example`, deployment docs.

**Goal:** Make setup, testing, deployment, and incident handling accurate and repeatable.

### 9.1 README

Update the README to include:

- [ ] Project purpose and architecture.
- [ ] Canonical project directory.
- [ ] Required Node.js/npm versions.
- [ ] Local installation and test commands.
- [ ] Local development command.
- [ ] Wrangler configuration and Durable Object binding explanation.
- [ ] Required secrets and how to set them securely.
- [ ] Optional configuration variables, defaults, ranges, and units.
- [ ] Authentication header and request examples with placeholder values only.
- [ ] Supported methods/routes.
- [ ] CORS decision.
- [ ] `GET /v1/models` decision.
- [ ] Rate-limit and queue semantics.
- [ ] Retry-After and timeout semantics.
- [ ] Streaming behavior.
- [ ] Health/readiness behavior.
- [ ] Deployment procedure.
- [ ] Post-deploy verification.
- [ ] Rollback and secret rotation.
- [ ] Known limitations, including the chosen stuck-cancellation policy.

Remove stale claims that deployment has not occurred if the repository has successful deployment evidence. Do not claim the live endpoint is healthy unless a smoke test verifies it.

### 9.2 Manual queue test page

- [ ] Replace stale Worker URL with a configurable endpoint input or a safe documented default.
- [ ] Do not embed a real proxy key in the HTML source.
- [ ] Prefer a user-entered temporary test credential or a backend-mediated test.
- [ ] Warn that browser-side credentials are visible to the browser user and must not be shared.
- [ ] Add request timeout and clear in-progress state.
- [ ] Avoid displaying secrets in page output or console logs.
- [ ] Make test labels accurately distinguish simulated tests from real upstream calls.
- [ ] Test the page against a local Worker and an explicitly authorized test environment.

### 9.3 Environment example

- [ ] Provide a safe `.env.example` or `.dev.vars.example` appropriate to the canonical Worker.
- [ ] Use placeholders, never real credentials.
- [ ] Explain which values are Cloudflare secrets versus ordinary configuration.
- [ ] Ensure examples cannot be mistaken for production-ready credentials.

### Deliverables

- Accurate README.
- Safe manual test tool.
- Safe environment template.

### Acceptance criteria

- A new maintainer can run tests and deploy using only documented steps.
- No stale production URL remains in the manual test tool.
- No secret is embedded in browser assets or documentation.
- Operational claims match verifiable evidence.

---

## PHASE 10 — Full regression, release audit, and issue reconciliation

**Priority:** P1 release gate  
**Related paths:** all Worker source/tests/config/docs and AUDIT issues #5–#14.

**Goal:** Prove the final implementation is correct, secure, deployable, and documented.

### 10.1 Required regression matrix

Run and record:

- [ ] Authentication success/failure.
- [ ] Missing/invalid proxy key.
- [ ] NVIDIA key never sourced from client input.
- [ ] Supported/unsupported routes and methods.
- [ ] CORS allowed/denied/preflight cases if enabled.
- [ ] `GET /v1/models` behavior according to the final decision.
- [ ] Request body size boundary.
- [ ] Aggregate body memory boundary.
- [ ] Concurrent body-reader limit.
- [ ] Request-body timeout/cancellation.
- [ ] FIFO/concurrency behavior.
- [ ] RPM sliding-window boundary.
- [ ] Durable Object persistence/restart behavior.
- [ ] Upstream 429 with integer Retry-After.
- [ ] Upstream 429 with HTTP-date Retry-After if supported.
- [ ] Invalid Retry-After.
- [ ] Retry maximum and overall deadline.
- [ ] Bounded 429 body read.
- [ ] Streaming response preservation.
- [ ] Client disconnect during queue wait.
- [ ] Client disconnect during upstream stream.
- [ ] Upstream fetch timeout.
- [ ] Upstream body stall.
- [ ] Upstream cancellation stall.
- [ ] Exactly-once queue/resource cleanup.
- [ ] Request and response header sanitization.
- [ ] Synthetic error body header consistency.
- [ ] Health/readiness behavior.
- [ ] Invalid/missing configuration.
- [ ] Secret-safe logs and errors.
- [ ] Canonical deployment guard.

### 10.2 Clean CI and deployment verification

- [ ] Run `npm ci` from a clean checkout in the canonical project.
- [ ] Run the complete unit/regression suite.
- [ ] Run runtime/workerd tests.
- [ ] Run static checks.
- [ ] Run Wrangler dry-run with the production config.
- [ ] Deploy only after explicit release authorization.
- [ ] Run post-deployment smoke tests.
- [ ] Capture workflow URL, commit SHA, deployment target, and health result.

### 10.3 Issue reconciliation

For each issue #5–#14:

- [ ] Re-read its acceptance criteria.
- [ ] Link the exact test(s), source change, or operational evidence.
- [ ] Update its status only when its criteria are met.
- [ ] If already implemented, close with evidence rather than reimplementing.
- [ ] If obsolete, explain why and close or replace it through normal repository process.
- [ ] If blocked by a client/product decision, keep it open and document the decision owner/input required.
- [ ] Ensure AUDIT-10 is not marked PASS while any mandatory release gate remains unresolved.

### Final release acceptance criteria

The project can be declared release-ready only when all are true:

1. There is one canonical Worker implementation and deployment path.
2. CI tests and deploys that exact implementation.
3. No secrets are committed, logged, or exposed to browser clients.
4. Client compatibility decisions are explicit and tested.
5. Queue, RPM, retries, timeouts, and cancellation semantics are documented and regression-tested.
6. Resource use is bounded.
7. Health and readiness checks have clear semantics.
8. Post-deployment smoke tests pass against the actual deployed URL.
9. README and manual test tooling are accurate.
10. All mandatory AUDIT issues are closed or explicitly documented as conditional/non-applicable with evidence.
11. The final commit SHA and CI/deployment evidence are recorded.

---

# 5. Suggested task breakdown and execution order

The following order minimizes the risk of fixing the wrong code or deploying the wrong project.

| Order | Task ID | Work | Priority | Depends on |
|---:|---|---|---|---|
| 1 | FIX-000 | Re-establish current baseline | P0 | None |
| 2 | FIX-010 | Canonical Worker/deployment source | P0 | FIX-000 |
| 3 | FIX-020 | Protect local secrets and configuration | P1 | FIX-010 |
| 4 | FIX-030 | Establish client topology and API contract | P1 | FIX-000 |
| 5 | FIX-040 | Decide CORS and model discovery | P1/conditional | FIX-030 |
| 6 | FIX-050 | Request/response header security | P1 | FIX-000 |
| 7 | FIX-060 | Queue/RPM/retry/cancellation verification | P1 | FIX-000 |
| 8 | FIX-070 | Resource bounds and config validation | P1 | FIX-000 |
| 9 | FIX-080 | Health/readiness and observability | P1 | FIX-010 |
| 10 | FIX-090 | Reproducible CI and workflow hardening | P1 | FIX-010 |
| 11 | FIX-100 | Post-deploy smoke test | P1 | FIX-080, FIX-090 |
| 12 | FIX-110 | README/manual test/environment docs | P2 | FIX-030, FIX-040, FIX-080, FIX-100 |
| 13 | FIX-120 | Full regression and issue reconciliation | P1 gate | All mandatory tasks |

### Recommended implementation batches

**Batch A — Prevent wrong deployment**
- FIX-000
- FIX-010
- FIX-020

**Batch B — Lock down the public contract**
- FIX-030
- FIX-040
- FIX-050

**Batch C — Prove runtime correctness**
- FIX-060
- FIX-070
- FIX-080

**Batch D — Make release repeatable**
- FIX-090
- FIX-100
- FIX-110

**Batch E — Final verification**
- FIX-120

Do not begin Batch B by assuming browser CORS is required. First establish the actual client topology.

---

# 6. Required test and evidence standards

Every completed task must include:

- Task ID and status.
- Baseline commit SHA.
- Files inspected.
- Files changed.
- Root cause, if a defect was found.
- Implementation summary.
- Tests added or modified.
- Exact test commands.
- Test totals and pass/fail results.
- Relevant CI run URL.
- Security impact.
- Remaining limitations.
- Whether deployment was performed (default: no, unless separately authorized).

Use these status values consistently:

- `NOT_STARTED`
- `IN_PROGRESS`
- `BLOCKED`
- `PASS`
- `PASS_WITH_NOTES`
- `FAIL`
- `NOT_APPLICABLE`

A task is not `PASS` merely because code exists. It must satisfy its acceptance criteria.

---

# 7. Risks and decisions requiring explicit confirmation

| Decision | Why it matters | Safe default until confirmed |
|---|---|---|
| Is MonkeyCode browser-only or backend-mediated? | Determines safe authentication and CORS design | Keep shared proxy secret server-side; do not expose it in browser code |
| Is `GET /v1/models` required? | Avoids unnecessary route expansion or client breakage | Do not implement until client evidence confirms need |
| Which Worker directory is canonical? | Prevents deploying stale code | Treat the nested project as current CI/deploy source, then enforce it |
| What does RPM count: logical requests or upstream attempts? | Affects quota correctness and retry accounting | Document current semantics and test them before changing |
| What is the stuck-cancellation policy? | Fail-closed safety can reduce availability | Preserve current fail-closed behavior until safe recovery is proven |
| Is the public `workers.dev` endpoint production-facing? | Determines smoke-test and secret-rotation requirements | Do not send paid inference requests during routine smoke tests |

---

# 8. Explicit out-of-scope items

Unless separately requested, this plan does not authorize:

- Replacing Cloudflare Workers with another hosting provider.
- Introducing paid infrastructure or services requiring a bank card.
- Sending real paid NVIDIA inference requests as part of CI.
- Publishing API keys or proxy keys in frontend code.
- Rewriting Git history.
- Deleting unrelated frontend assets or project files without proving they are obsolete.
- Changing production secrets.
- Deploying to production.
- Committing, pushing, merging, or closing issues without the required authorization.

---

# 9. Final completion report template

Use this template when all phases are finished:

```text
PROJECT=nvidia-queue-proxy.2
FINAL_STATUS=PASS | PASS_WITH_NOTES | FAIL
BRANCH=main
FINAL_COMMIT=<sha>

CANONICAL_WORKER=<path>
DEPLOY_CONFIG=<path>
CI_RUN=<url>
DEPLOYMENT=<not-performed | successful | failed>
POST_DEPLOY_HEALTH=<pass | fail | not-run>
POST_DEPLOY_READINESS=<pass | fail | not-run>

UNIT_TESTS=<passed>/<total>
RUNTIME_TESTS=<passed>/<total>
STATIC_CHECKS=<pass/fail>
WRANGLER_DRY_RUN=<pass/fail>

SECURITY_REVIEW=<pass/fail>
QUEUE_REVIEW=<pass/fail>
CLIENT_COMPATIBILITY=<pass/fail/conditional>
DOCUMENTATION=<pass/fail>

AUDIT_01=<status>
AUDIT_02=<status>
AUDIT_03=<status>
AUDIT_04=<status>
AUDIT_05=<status>
AUDIT_06=<status>
AUDIT_07=<status>
AUDIT_08=<status>
AUDIT_09=<status>
AUDIT_10=<status>

OPEN_BLOCKERS:
- <none or exact list>

KNOWN_LIMITATIONS:
- <none or exact list>

DEPLOYMENT_AUTHORIZATION=<granted/not-granted>
```

---

## 10. Definition of done

The project is complete only when the proxy has one safe and reproducible deployment path, its client contract is known, secrets and headers are protected, queue and retry behavior is proven under concurrency and cancellation, resource use is bounded, CI validates the deployable artifact, post-deployment checks verify the actual service, and the repository documentation reflects the real system.

**Important:** This plan is intentionally audit-first. Existing functionality should be verified and retained, not reimplemented simply because an old GitHub issue remains open.
