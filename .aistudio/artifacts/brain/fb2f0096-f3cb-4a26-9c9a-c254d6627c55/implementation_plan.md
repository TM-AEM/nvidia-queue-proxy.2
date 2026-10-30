# NVIDIA Queue Proxy — Architecture & Remediation Plan

A production-grade Cloudflare Worker proxy that strictly queues, rate-limits, retries, and serializes inference requests to the NVIDIA NIM API, protecting credentials and preventing rate-limit violations.

---

## User Review & Critical Decisions

> [!IMPORTANT]
> The following decisions were established during phase 1 clarification and directly govern the remediation and deployment architecture:

- **Confirmed: Client Topology & Proxy Secret**: All client calls originate from a trusted backend server or API router. Browser CORS remains disabled by default, and the shared `PROXY_KEY` will never be distributed to client-side browser bundles.
- **Confirmed: Route Scope & Model Discovery**: Only authenticated `POST /v1/*` inference endpoints (such as `/v1/chat/completions`) are proxied. `GET /v1/models` will not be proxied, preserving a strict, minimal attack surface and returning deterministic 404 responses.
- **Confirmed: Project Layout & Consolidation**: The project will be consolidated to a single canonical Worker at the root. The duplicate and outdated root Worker entry point will be replaced with the fully hardened 1,900+ line implementation from `nvidia-queue-proxy-main/`, and the nested duplicate directory will be pruned.
- **Stuck Upstream Cancellation Policy (Recommended Default)**: Fail-closed slot retention. If an upstream HTTP streaming connection fails to settle its abort within the configured grace period, the queue slot is preserved until the Durable Object lifecycle reclaims it, ensuring upstream RPM quotas cannot be breached by overlapping ghost requests.

---

## 1. Overview & Core Concept

- **What It Does**: Acts as a resilient front door for NVIDIA NIM API calls. It ingests OpenAI-compatible completion requests, authenticates callers using a local proxy key, buffers requests within a bounded FIFO queue managed by a Cloudflare Durable Object, enforces strict requests-per-minute (RPM) and cooldown spacing, safely forwards authenticated payloads to NVIDIA, and streams responses back to the client while honoring upstream `429` `Retry-After` headers.
- **Target Audience / Persona**: Backend engineering teams, LLM application developers, and orchestration systems (such as MonkeyCode) requiring guaranteed SLA rate compliance and credential isolation for NVIDIA NIM models.
- **Key Value**: Completely isolates upstream NVIDIA API credentials from downstream callers, eliminates concurrency spikes that cause upstream 429 errors, provides deterministic backoff retries with additive jitter, and bounds memory/time resource consumption against slow or hostile clients.

---

## 2. User Experience & Visual Design

The proxy is primarily a high-performance HTTP gateway, paired with an integrated developer diagnostic dashboard and manual verification tool for queue health and latency observation.

### Key User Flows

1. **Inference Request Flow (Backend to Proxy)**:
   - Client backend issues `POST /v1/chat/completions` with `Authorization: Bearer <PROXY_KEY>`.
   - Proxy validates key in constant time (<1ms) and admits the payload to memory budget.
   - Request enters FIFO queue in Durable Object `RateGate`.
   - Rate limit and cooldown window evaluated.
   - Proxy strips internal headers, attaches `NVIDIA_API_KEY`, and dispatches upstream.
   - Streamed response chunks piped directly to the caller with sanitized response headers.

2. **Operational Health & Readiness Probing**:
   - Automated monitoring probes `GET /health` (liveness: always instant 200).
   - Automated monitoring probes `GET /ready` (readiness: verifies required secret bindings without leaking secrets or triggering upstream charges).

3. **Developer Verification & Interactive Playground**:
   - Web console interface for testing queue pacing, simulated concurrent bursts (e.g. 3-5 concurrent calls), and visualizing response timing, queue wait durations, and status codes.

### Visual Identity & Theme (Developer Verification Console)

- **Aesthetic Direction**: Utilitarian, dark-mode terminal inspired by NVIDIA developer tooling and Cloudflare dashboard aesthetics. High contrast, monospace metric counters, and clean telemetry badges.
- **Color Palette**:
  - Dominant Background: Deep slate (`#0B0F17`)
  - Accent / Brand: NVIDIA Green (`#76B900`) & Emerald (`#10B981`)
  - Neutral Borders & Cards: Subtle slate borders (`#1E293B`) and muted charcoal panels (`#111827`)
  - Warning / Retry State: Amber (`#F59E0B`)
  - Error State: Rose (`#F43F5E`)
- **Typography & Hierarchy**:
  - Headers: Clean sans-serif (`Inter`, system-ui)
  - Code & Telemetry: Monospace (`JetBrains Mono`, `ui-monospace`)
- **Interactive Feedback**:
  - Real-time status tags (Connected, Queued, Dispatched, Streaming, Complete).
  - High-precision latency badges (queue wait vs upstream TTFT).

---

## 3. Key Product Decisions & Trade-Offs

- **Decision 1: Consolidate to Root vs Maintain Nested Project**
  - *Chosen Approach*: Consolidate the production-grade Worker into the root directory (`src/index.js`, `wrangler.jsonc`, `package.json`), aligning CI workflows, tests, and deployment commands.
  - *Why*: Eliminates dual-source ambiguity where local commands or dry-runs might deploy outdated files while CI deploys nested files.
  - *Alternatives Considered*: Retaining the nested directory with a root redirect script was rejected because it adds friction and increases risk of developer error.

- **Decision 2: Memory-Bounded Request Admission Before Queue Entry**
  - *Chosen Approach*: Stream request bodies with a strict byte cap (`20 MB`), a concurrency limit on body readers (`100`), and an aggregate memory ceiling (`32 MB`).
  - *Why*: Prevents denial-of-service and Durable Object memory exhaustion from malicious or high-throughput clients attempting to upload oversized payloads.
  - *Alternatives Considered*: Streaming directly without prior buffering risks holding queue slots during slow client uploads.

- **Decision 3: Fail-Closed Queue Slot Retention on Upstream Cancellation Stall**
  - *Chosen Approach*: If an upstream fetch stream abort does not resolve within the cancellation grace period (`10,000 ms`), the active slot remains occupied until the runtime recycles the Durable Object.
  - *Why*: Prioritizes NVIDIA quota protection. Releasing the slot prematurely while an upstream request is still actively executing could cause double-dispatch and trigger upstream account penalties.
  - *Alternatives Considered*: Bounded forced recovery was considered but discarded as unsafe under strict upstream rate contracts.

- **Decision 4: Forwarded and Sanitized Header Whitelist**
  - *Chosen Approach*: Strip all hop-by-hop headers, proxy credentials (`x-proxy-key`, client `Authorization`), connection-nominated headers, cookies, and internal routing metadata before sending upstream; strip upstream response infrastructure headers before returning to client.
  - *Why*: Strict defense-in-depth against credential leaks and protocol smuggling.

---

## 4. Technical Architecture & Data Strategy

### System Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                          DOWNSTREAM CLIENTS                            │
│           (Backend Server / Orchestrator / Automated Agent)            │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ POST /v1/chat/completions
                                    │ (Bearer PROXY_KEY)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   CLOUDFLARE WORKER ROUTER (Root)                      │
│                                                                        │
│  ├─ GET /health ──────────► [200 OK: Process Alive]                    │
│  ├─ GET /ready ───────────► [200/500: Verify Env Secrets Configured]   │
│  ├─ Authenticator ────────► [Constant-Time Key Match or 401]           │
│  └─ Request Guard ────────► [Verify Method=POST, Path=/v1/*]           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Validated Request Body
                                    │ Memory Budget Checked
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│              DURABLE OBJECT: RateGate ("nvidia-global-queue")          │
│                                                                        │
│  ┌───────────────────────┐         ┌───────────────────────────────┐   │
│  │ Memory Admission Gate │ ──────► │ FIFO Queue & Wait Deadline    │   │
│  └───────────────────────┘         └───────────────┬───────────────┘   │
│                                                    │                   │
│  ┌───────────────────────┐                         ▼                   │
│  │ SQLite Storage State  │ ◄────── │ RPM Sliding Window & Cooldown │   │
│  └───────────────────────┘         └───────────────┬───────────────┘   │
└────────────────────────────────────────────────────┼───────────────────┘
                                                     │ Serialized Request
                                                     │ (Authorization: Bearer NVIDIA_KEY)
                                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   UPSTREAM NVIDIA NIM API GATEWAY                      │
│               (https://integrate.api.nvidia.com)                       │
│                                                                        │
│  ├─ Response 200 OK ──────► Stream chunks back through proxy to client │
│  └─ Response 429 ─────────► Parse Retry-After, exponential backoff,    │
│                             additive jitter, retry up to 5 times       │
└────────────────────────────────────────────────────────────────────────┘
```

### State Model & Invariants

- **RPM Window**: 60,000 ms sliding window tracked in memory and synchronized with Durable Object SQLite storage.
- **Cooldown Interval**: Minimum spacing (`1,670 ms` default, matching ~36-40 RPM safe ceiling) enforced between successive upstream dispatches.
- **Backoff & Jitter**:
  $$\text{Delay} = \min(\text{MAX\_BACKOFF}, \max(2^{\text{retry}} \times \text{BASE\_DELAY}, \text{Retry-After})) \times (1 + \text{jitter})$$
  Jitter is strictly additive ($[0, 0.25]$), guaranteeing the delay never drops below the upstream-mandated `Retry-After`.
- **Resource Bounds**:
  - Max Request Body: `20 MB`
  - Max Concurrent Body Readers: `100`
  - Max Aggregate Memory: `32 MB`
  - Max Queue Depth: `100` items
  - Max Queue Wait: `60,000 ms`
  - Max Upstream 429 Error Body Read: `64 KB` (timed out at `30,000 ms`)

### Component & Execution Strategy

1. **Workspace Consolidation**:
   - Migrate canonical `nvidia-queue-proxy-main/src/index.js` into root `src/index.js`.
   - Update root `wrangler.jsonc` with exact observability, Durable Object SQLite export, and compatibility rules.
   - Relocate the comprehensive test suites (`test/batch1.test.js`, `test/runtime.test.js`) to root `test/`.
   - Update root `package.json` test and deploy scripts.
   - Safely remove redundant nested directory.
2. **CI & Workflow Hardening**:
   - Update `.github/workflows/deploy.yml` to run `npm ci` from the repository root.
   - Include automated `npm test` and `wrangler deploy --dry-run` steps.
   - Add post-deployment smoke test verifying `/health` and `/ready` against the deployed endpoint.
3. **Developer Interface & Diagnostic Tool**:
   - Provide clean, modern queue inspection and verification UI in `src/App.tsx` allowing developers to inspect proxy health, monitor latency, and test concurrent requests safely.
