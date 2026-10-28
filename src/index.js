import { DurableObject } from "cloudflare:workers";

const NVIDIA_API = "https://integrate.api.nvidia.com";
const WINDOW_MS = 60_000;
const DEFAULT_RPM = 40;
const DEFAULT_COOLDOWN_MS = 1_670;
const MAX_429_RETRIES = 5;
const MAX_BACKOFF_MS = 30_000;
const DEFAULT_RESPONSE_HEADER_TIMEOUT_MS = 30_000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_QUEUE_WAIT_MS = 60_000;
const DEFAULT_MAX_QUEUE_DEPTH = 100;

export function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      return reject(signal.reason || new Error("Aborted"));
    }
    const timer = setTimeout(() => {
      if (signal) signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason || new Error("Aborted"));
    };
    if (signal) {
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

export function retryAfterMs(response, now = Date.now()) {
  const value = response?.headers?.get("Retry-After");
  if (!value) return 0;
  const trimmed = value.trim();
  const seconds = Number(trimmed);
  if (!Number.isNaN(seconds)) {
    return seconds > 0 ? Math.round(seconds * 1000) : 0;
  }
  const date = Date.parse(trimmed);
  return Number.isFinite(date) ? Math.max(0, date - now) : 0;
}

export function computeBackoffDelay(retryIndex, response, options = {}) {
  const {
    now = Date.now(),
    maxBackoffMs = MAX_BACKOFF_MS,
    baseDelayMs = 1000,
    jitterFn = null,
  } = options;
  const expDelay = baseDelayMs * (2 ** retryIndex);
  const retryAfter = response ? retryAfterMs(response, now) : 0;
  const delay = Math.min(maxBackoffMs, Math.max(expDelay, retryAfter));
  if (typeof jitterFn === "function") {
    return jitterFn(delay);
  }
  return delay;
}

export function safeResponseHeaders(headers) {
  const result = new Headers(headers);
  for (const name of [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
  ]) {
    result.delete(name);
  }
  return result;
}

export class RateGate extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.waitingQueue = [];
    this.activeTicket = null;
    this.now = typeof env?.__now === "function" ? env.__now : () => Date.now();
    this.getNow = () => this.now();
    this.sleepImpl =
      typeof env?.__sleep === "function" ? env.__sleep : sleep;
    this.getSleepImpl = () => this.sleepImpl || sleep;
    this.fetchImpl =
      typeof env?.__fetch === "function" ? env.__fetch : fetch;
    this.getFetchImpl = () => this.fetchImpl || (typeof this.env?.__fetch === "function" ? this.env.__fetch : fetch);
  }

  cleanTicket(ticket) {
    if (ticket.timer !== null) {
      clearTimeout(ticket.timer);
      ticket.timer = null;
    }
    if (ticket.abortHandler && ticket.request?.signal) {
      ticket.request.signal.removeEventListener("abort", ticket.abortHandler);
      ticket.abortHandler = null;
    }
  }

  removeFromWaitingQueue(ticket) {
    const index = this.waitingQueue.indexOf(ticket);
    if (index !== -1) {
      this.waitingQueue.splice(index, 1);
    }
  }

  releaseSlot() {
    this.activeTicket = null;
    while (this.waitingQueue.length > 0) {
      const nextTicket = this.waitingQueue.shift();
      if (nextTicket.settled) {
        continue;
      }
      nextTicket.settled = true;
      this.cleanTicket(nextTicket);
      this.activeTicket = nextTicket;
      nextTicket.resolve(nextTicket);
      return;
    }
  }

  async acquireSlot(request, maxQueueWaitMs, cooldownMs) {
    if (!this.activeTicket) {
      const ticket = {
        request,
        settled: true,
        timer: null,
        abortHandler: null,
      };
      this.activeTicket = ticket;
      return ticket;
    }

    let ticketResolve;
    const ticketPromise = new Promise((resolve) => {
      ticketResolve = resolve;
    });

    const ticket = {
      request,
      resolve: ticketResolve,
      timer: null,
      abortHandler: null,
      settled: false,
    };

    ticket.timer = setTimeout(() => {
      if (ticket.settled) return;
      ticket.settled = true;
      this.removeFromWaitingQueue(ticket);
      this.cleanTicket(ticket);
      const retryAfterSec = Math.max(1, Math.ceil(cooldownMs / 1000));
      ticket.resolve({
        rejectedResponse: Response.json(
          { error: "Maximum queue wait exceeded" },
          {
            status: 503,
            headers: {
              "Retry-After": String(retryAfterSec),
            },
          }
        ),
      });
    }, maxQueueWaitMs);

    if (request.signal) {
      if (request.signal.aborted) {
        this.cleanTicket(ticket);
        ticket.settled = true;
        return {
          rejectedResponse: Response.json(
            { error: "Client cancelled" },
            { status: 499 }
          ),
        };
      }
      ticket.abortHandler = () => {
        if (ticket.settled) return;
        ticket.settled = true;
        this.removeFromWaitingQueue(ticket);
        this.cleanTicket(ticket);
        ticket.resolve({
          rejectedResponse: Response.json(
            { error: "Client cancelled" },
            { status: 499 }
          ),
        });
      };
      request.signal.addEventListener("abort", ticket.abortHandler, {
        once: true,
      });
    }

    this.waitingQueue.push(ticket);
    return await ticketPromise;
  }

  async waitForSlot(state, rpm, cooldownMs, signal) {
    while (true) {
      if (signal?.aborted) {
        throw signal.reason || new Error("Aborted");
      }
      const now = this.now();
      state.starts = state.starts.filter((time) => now - time < WINDOW_MS);
      let wait = 0;
      if (state.starts.length >= rpm) {
        const oldestNeeded = state.starts[state.starts.length - rpm];
        wait = Math.max(wait, oldestNeeded + WINDOW_MS - now);
      }
      if (state.lastCompletionAt) {
        wait = Math.max(wait, state.lastCompletionAt + cooldownMs - now);
      }
      if (state.lastSendAt) {
        wait = Math.max(wait, state.lastSendAt + cooldownMs - now);
      }
      if (wait <= 0) return;
      await this.sleepImpl(wait, signal);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "POST" || !url.pathname.startsWith("/v1/")) {
      return new Response("Not found", { status: 404 });
    }

    const rpm = Math.max(
      1,
      Math.min(1000, Number(this.env.RPM) || DEFAULT_RPM)
    );
    const rawCooldown = this.env.COOLDOWN_MS;
    const cooldownMs = Math.max(
      0,
      Math.min(
        60_000,
        rawCooldown !== undefined && rawCooldown !== null && !Number.isNaN(Number(rawCooldown))
          ? Number(rawCooldown)
          : DEFAULT_COOLDOWN_MS
      )
    );
    const responseHeaderTimeoutMs = Math.max(
      1,
      Number(
        this.env.RESPONSE_HEADER_TIMEOUT_MS ?? this.env.UPSTREAM_TIMEOUT_MS
      ) || DEFAULT_RESPONSE_HEADER_TIMEOUT_MS
    );
    const streamIdleTimeoutMs = Math.max(
      1,
      Number(this.env.STREAM_IDLE_TIMEOUT_MS) || DEFAULT_STREAM_IDLE_TIMEOUT_MS
    );
    const maxQueueWaitMs = Math.max(
      1,
      Number(
        this.env.MAX_QUEUE_WAIT_MS ?? this.env.QUEUE_TIMEOUT_MS
      ) || DEFAULT_MAX_QUEUE_WAIT_MS
    );
    const rawQueueDepth = this.env.MAX_QUEUE_DEPTH ?? this.env.MAX_QUEUE_SIZE;
    const maxQueueDepth = Math.max(
      0,
      rawQueueDepth !== undefined && rawQueueDepth !== null && !Number.isNaN(Number(rawQueueDepth))
        ? Number(rawQueueDepth)
        : DEFAULT_MAX_QUEUE_DEPTH
    );

    // F2: Read request body BEFORE acquiring exclusive queue slot
    let body;
    try {
      body = await request.arrayBuffer();
    } catch (readErr) {
      if (request.signal?.aborted) {
        return Response.json({ error: "Client cancelled" }, { status: 499 });
      }
      return Response.json(
        { error: "Failed to read request body" },
        { status: 400 }
      );
    }

    if (request.signal?.aborted) {
      return Response.json({ error: "Client cancelled" }, { status: 499 });
    }

    // F4: Queue depth rejection
    // If an active request is running and waiting backlog has reached capacity:
    if (this.activeTicket && this.waitingQueue.length >= maxQueueDepth) {
      const retryAfterSec = Math.max(1, Math.ceil(cooldownMs / 1000));
      return Response.json(
        { error: "Queue depth exceeded" },
        {
          status: 503,
          headers: {
            "Retry-After": String(retryAfterSec),
          },
        }
      );
    }

    // Acquire queue slot
    const ticket = await this.acquireSlot(
      request,
      maxQueueWaitMs,
      cooldownMs
    );
    if (ticket.rejectedResponse) {
      return ticket.rejectedResponse;
    }

    // Client may have aborted while slot was being acquired
    if (request.signal?.aborted) {
      this.releaseSlot();
      return Response.json({ error: "Client cancelled" }, { status: 499 });
    }

    const upstreamAbortController = new AbortController();
    const onClientAbort = () => {
      upstreamAbortController.abort(
        request.signal?.reason || new Error("Client cancelled")
      );
    };
    if (request.signal) {
      request.signal.addEventListener("abort", onClientAbort, { once: true });
    }

    try {
      const state = (await this.ctx.storage.get("rate-state")) || {
        starts: [],
        lastSendAt: 0,
        lastCompletionAt: 0,
      };

      let upstreamResponse;
      for (let retry = 0; ; retry++) {
        if (request.signal?.aborted) {
          throw new Error("Client cancelled");
        }

        // Wait for RPM slot and cooldown
        await this.waitForSlot(
          state,
          rpm,
          cooldownMs,
          upstreamAbortController.signal
        );

        // T1: Count every attempt immediately before dispatching upstream
        const attemptTime = this.now();
        state.starts = state.starts.filter(
          (time) => attemptTime - time < WINDOW_MS
        );
        state.starts.push(attemptTime);
        state.lastSendAt = attemptTime;
        await this.ctx.storage.put("rate-state", state);

        const upstreamUrl = new URL(
          url.pathname + url.search,
          NVIDIA_API
        );
        const headers = new Headers(request.headers);
        headers.set(
          "Authorization",
          `Bearer ${this.env.NVIDIA_API_KEY}`
        );
        headers.delete("host");
        headers.delete("x-proxy-key");
        headers.delete("content-length");

        // Response header timeout
        let headerTimeoutId = null;
        let headerTimedOut = false;
        headerTimeoutId = setTimeout(() => {
          headerTimedOut = true;
          upstreamAbortController.abort(
            new Error("Upstream response header timeout")
          );
        }, responseHeaderTimeoutMs);

        try {
          upstreamResponse = await this.fetchImpl(upstreamUrl, {
            method: "POST",
            headers,
            body,
            redirect: "manual",
            signal: upstreamAbortController.signal,
          });
        } catch (fetchError) {
          if (headerTimeoutId !== null) {
            clearTimeout(headerTimeoutId);
            headerTimeoutId = null;
          }
          state.lastCompletionAt = this.now();
          await this.ctx.storage.put("rate-state", state);

          if (headerTimedOut) {
            if (request.signal) {
              request.signal.removeEventListener("abort", onClientAbort);
            }
            this.releaseSlot();
            const retryAfterSec = Math.max(1, Math.ceil(cooldownMs / 1000));
            return Response.json(
              { error: "Upstream response header timeout" },
              {
                status: 504,
                headers: { "Retry-After": String(retryAfterSec) },
              }
            );
          }
          if (request.signal?.aborted) {
            if (request.signal) {
              request.signal.removeEventListener("abort", onClientAbort);
            }
            this.releaseSlot();
            return Response.json(
              { error: "Client cancelled" },
              { status: 499 }
            );
          }
          throw fetchError;
        }

        if (headerTimeoutId !== null) {
          clearTimeout(headerTimeoutId);
          headerTimeoutId = null;
        }

        // If not 429, break out of retry loop
        if (upstreamResponse.status !== 429) {
          break;
        }

        // T2: Handle 429 retry
        const retryBody = await upstreamResponse.arrayBuffer();
        const retryHeaders = safeResponseHeaders(upstreamResponse.headers);
        state.lastCompletionAt = this.now();
        await this.ctx.storage.put("rate-state", state);

        if (retry >= MAX_429_RETRIES) {
          // Retries exhausted
          this.releaseSlot();
          if (request.signal) {
            request.signal.removeEventListener("abort", onClientAbort);
          }
          return new Response(retryBody, {
            status: 429,
            headers: retryHeaders,
          });
        }

        const retryDelay = computeBackoffDelay(retry, upstreamResponse, {
          now: this.now(),
          maxBackoffMs: MAX_BACKOFF_MS,
        });

        await this.sleepImpl(retryDelay, upstreamAbortController.signal);
      }

      // Safe response headers
      const responseHeaders = safeResponseHeaders(upstreamResponse.headers);

      if (!upstreamResponse.body) {
        state.lastCompletionAt = this.now();
        await this.ctx.storage.put("rate-state", state);
        if (request.signal) {
          request.signal.removeEventListener("abort", onClientAbort);
        }
        this.releaseSlot();
        return new Response(null, {
          status: upstreamResponse.status,
          statusText: upstreamResponse.statusText,
          headers: responseHeaders,
        });
      }

      // Streaming response with idle timeout and slot holding
      const reader = upstreamResponse.body.getReader();
      let released = false;
      let idleTimer = null;
      let idleTimedOut = false;
      let controllerRef = null;

      let cancelPromise = null;
      const cancelUpstream = (reason) => {
        if (!cancelPromise) {
          let resolveCancel;
          cancelPromise = new Promise((resolve) => {
            resolveCancel = resolve;
          });

          if (!upstreamAbortController.signal.aborted) {
            upstreamAbortController.abort(reason);
          }

          (async () => {
            try {
              await reader.cancel(reason);
            } catch {}
            resolveCancel();
          })();
        }
        return cancelPromise;
      };

      let onAbortStream = null;
      const finish = async () => {
        if (cancelPromise) {
          await cancelPromise;
        }
        if (released) return;
        released = true;
        clearIdleTimer();
        if (onAbortStream) {
          upstreamAbortController.signal.removeEventListener("abort", onAbortStream);
        }
        if (request.signal) {
          request.signal.removeEventListener("abort", onClientAbort);
        }
        try {
          state.lastCompletionAt = this.getNow();
          await this.ctx.storage.put("rate-state", state);
        } finally {
          this.upstreamInFlight = false;
          this.releaseSlot();
        }
      };

      const clearIdleTimer = () => {
        if (idleTimer !== null) {
          clearTimeout(idleTimer);
          idleTimer = null;
        }
      };

      const resetIdleTimer = () => {
        clearIdleTimer();
        if (released || idleTimedOut) return;
        idleTimer = setTimeout(async () => {
          if (released || idleTimedOut) return;
          idleTimedOut = true;
          const timeoutErr = new Error("Stream idle timeout");
          await cancelUpstream(timeoutErr);
          try {
            controllerRef?.error(timeoutErr);
          } catch {}
          await finish();
        }, streamIdleTimeoutMs);
      };

      onAbortStream = async () => {
        if (released) return;
        clearIdleTimer();
        await cancelUpstream(upstreamAbortController.signal.reason);
        try {
          controllerRef?.error(upstreamAbortController.signal.reason);
        } catch {}
        await finish();
      };
      upstreamAbortController.signal.addEventListener("abort", onAbortStream, { once: true });

      const stream = new ReadableStream({
        start(controller) {
          controllerRef = controller;
          resetIdleTimer();
        },
        async pull(controller) {
          resetIdleTimer();
          try {
            const result = await reader.read();
            if (idleTimedOut) {
              return;
            }
            if (result.done) {
              clearIdleTimer();
              if (cancelPromise) {
                await cancelPromise;
              }
              controller.close();
              await finish();
            } else {
              controller.enqueue(result.value);
              resetIdleTimer();
            }
          } catch (readError) {
            clearIdleTimer();
            await cancelUpstream(readError);
            try {
              controller.error(readError);
            } catch {}
            await finish();
          }
        },
        async cancel(reason) {
          clearIdleTimer();
          await cancelUpstream(reason);
          await finish();
        },
      });

      return new Response(stream, {
        status: upstreamResponse.status,
        statusText: upstreamResponse.statusText,
        headers: responseHeaders,
      });
    } catch (error) {
      if (request.signal) {
        request.signal.removeEventListener("abort", onClientAbort);
      }
      this.releaseSlot();
      if (request.signal?.aborted) {
        return Response.json({ error: "Client cancelled" }, { status: 499 });
      }
      return Response.json(
        { error: "Proxy upstream request failed" },
        { status: 502 }
      );
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health" && request.method === "GET") {
      return Response.json({ ok: true });
    }

    if (!env.PROXY_KEY || !env.NVIDIA_API_KEY) {
      return new Response("Server configuration error", {
        status: 500,
      });
    }

    const authorization = request.headers.get("Authorization") || "";
    const suppliedKey = authorization.startsWith("Bearer ")
      ? authorization.slice(7)
      : request.headers.get("x-proxy-key") || "";

    if (suppliedKey !== env.PROXY_KEY) {
      return new Response("Unauthorized", { status: 401 });
    }

    if (request.method !== "POST" || !url.pathname.startsWith("/v1/")) {
      return new Response("Not found", { status: 404 });
    }

    const id = env.RATE_GATE.idFromName("nvidia-global-queue");
    return env.RATE_GATE.get(id).fetch(request);
  },
};
