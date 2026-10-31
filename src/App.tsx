import { useState } from "react";
import {
  Shield,
  Zap,
  Clock,
  Activity,
  CheckCircle2,
  Server,
  Terminal,
  Copy,
  Lock,
  RefreshCw,
  Play,
  Sliders,
  Database,
  Cpu,
  FileCode,
  Layers,
  ArrowRight
} from "lucide-react";

interface TestResult {
  id: number;
  timestamp: string;
  type: string;
  status: number | "error" | "pending";
  durationMs: number;
  message: string;
  details?: Record<string, unknown>;
}

export default function App() {
  const [activeTab, setActiveTab] = useState<"tester" | "architecture" | "config" | "audit">("tester");
  const [endpointUrl, setEndpointUrl] = useState("https://nvidia-queue-proxy.5s5k7kk.workers.dev");
  const [proxyKey, setProxyKey] = useState("");
  const [isRunningTest, setIsRunningTest] = useState(false);
  const [testResults, setTestResults] = useState<TestResult[]>([]);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const copyToClipboard = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  const runProbe = async (type: "health" | "ready" | "burst" | "chat") => {
    setIsRunningTest(true);
    const now = new Date().toLocaleTimeString();

    if (type === "health") {
      const start = performance.now();
      try {
        const res = await fetch(`${endpointUrl.replace(/\/$/, "")}/health`);
        const duration = Math.round(performance.now() - start);
        const data = await res.json().catch(() => ({ status: res.statusText }));
        setTestResults(prev => [
          {
            id: Date.now(),
            timestamp: now,
            type: "Liveness Probe (GET /health)",
            status: res.status,
            durationMs: duration,
            message: res.ok ? "Worker process is alive and responding" : `Returned HTTP ${res.status}`,
            details: data as Record<string, unknown>,
          },
          ...prev,
        ]);
      } catch (err: unknown) {
        setTestResults(prev => [
          {
            id: Date.now(),
            timestamp: now,
            type: "Liveness Probe (GET /health)",
            status: "error",
            durationMs: Math.round(performance.now() - start),
            message: err instanceof Error ? err.message : "Network error / CORS blocked",
          },
          ...prev,
        ]);
      }
    } else if (type === "ready") {
      const start = performance.now();
      try {
        const res = await fetch(`${endpointUrl.replace(/\/$/, "")}/ready`);
        const duration = Math.round(performance.now() - start);
        const data = await res.json().catch(() => ({ status: res.statusText }));
        setTestResults(prev => [
          {
            id: Date.now(),
            timestamp: now,
            type: "Readiness Probe (GET /ready)",
            status: res.status,
            durationMs: duration,
            message: res.ok ? "All required secrets & Durable Object bindings present" : `Service not ready (${res.status})`,
            details: data as Record<string, unknown>,
          },
          ...prev,
        ]);
      } catch (err: unknown) {
        setTestResults(prev => [
          {
            id: Date.now(),
            timestamp: now,
            type: "Readiness Probe (GET /ready)",
            status: "error",
            durationMs: Math.round(performance.now() - start),
            message: err instanceof Error ? err.message : "Network error / CORS blocked",
          },
          ...prev,
        ]);
      }
    } else if (type === "burst") {
      const count = 3;
      const promises = Array.from({ length: count }).map(async (_, idx) => {
        const start = performance.now();
        try {
          const res = await fetch(`${endpointUrl.replace(/\/$/, "")}/v1/chat/completions`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${proxyKey || "TEST_KEY"}`,
            },
            body: JSON.stringify({
              model: "nvidia/nemotron-3.5-super-vl-120b-a12b",
              messages: [{ role: "user", content: "ping" }],
              max_tokens: 10,
              stream: false,
            }),
          });
          const duration = Math.round(performance.now() - start);
          return {
            id: Date.now() + idx,
            timestamp: now,
            type: `Burst Call #${idx + 1}`,
            status: res.status,
            durationMs: duration,
            message: `Completed with HTTP ${res.status}`,
          };
        } catch (err: unknown) {
          return {
            id: Date.now() + idx,
            timestamp: now,
            type: `Burst Call #${idx + 1}`,
            status: "error" as const,
            durationMs: Math.round(performance.now() - start),
            message: err instanceof Error ? err.message : "Failed to reach endpoint",
          };
        }
      });

      const batchResults = await Promise.all(promises);
      setTestResults(prev => [...batchResults, ...prev]);
    } else if (type === "chat") {
      const start = performance.now();
      try {
        const res = await fetch(`${endpointUrl.replace(/\/$/, "")}/v1/chat/completions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${proxyKey || "TEST_KEY"}`,
          },
          body: JSON.stringify({
            model: "nvidia/nemotron-3.5-super-vl-120b-a12b",
            messages: [{ role: "user", content: "Hello NVIDIA" }],
            max_tokens: 25,
            stream: false,
          }),
        });
        const duration = Math.round(performance.now() - start);
        const text = await res.text();
        let parsed: Record<string, unknown> | undefined;
        try {
          parsed = JSON.parse(text);
        } catch {
          // ignore parse error
        }
        setTestResults(prev => [
          {
            id: Date.now(),
            timestamp: now,
            type: "POST /v1/chat/completions",
            status: res.status,
            durationMs: duration,
            message: `HTTP ${res.status} response received`,
            details: parsed || { body: text.slice(0, 300) },
          },
          ...prev,
        ]);
      } catch (err: unknown) {
        setTestResults(prev => [
          {
            id: Date.now(),
            timestamp: now,
            type: "POST /v1/chat/completions",
            status: "error",
            durationMs: Math.round(performance.now() - start),
            message: err instanceof Error ? err.message : "Request failed",
          },
          ...prev,
        ]);
      }
    }

    setIsRunningTest(false);
  };

  return (
    <div className="min-h-screen bg-[#070A0F] text-slate-100 font-sans selection:bg-[#76B900]/30 selection:text-white">
      {/* Top Navbar */}
      <header className="border-b border-slate-800/80 bg-[#0B0F17]/90 backdrop-blur sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-[#76B900]/15 border border-[#76B900]/40 flex items-center justify-center text-[#76B900] shadow-[0_0_15px_rgba(118,185,0,0.2)]">
              <Cpu className="w-5 h-5" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-bold tracking-tight text-white text-base">NVIDIA Queue Proxy</span>
                <span className="text-[10px] font-mono uppercase bg-[#76B900]/15 text-[#76B900] px-1.5 py-0.5 rounded border border-[#76B900]/30">
                  Canonical Root
                </span>
              </div>
              <p className="text-xs text-slate-400">Rate-Limiting & FIFO Queue Gateway for NVIDIA NIM</p>
            </div>
          </div>

          <div className="flex items-center gap-2 text-xs">
            <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-950/60 border border-emerald-500/30 text-emerald-400">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span>194 / 194 Tests Passing</span>
            </div>
            <div className="hidden sm:flex items-center gap-1 px-2.5 py-1 rounded-full bg-slate-800/80 border border-slate-700/60 text-slate-300 font-mono">
              <Server className="w-3.5 h-3.5 text-slate-400" />
              <span>RateGate (SQLite)</span>
            </div>
          </div>
        </div>

        {/* Tab Navigation */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex gap-2 border-t border-slate-800/50">
          <button
            onClick={() => setActiveTab("tester")}
            className={`flex items-center gap-2 py-3 px-3 text-xs font-medium border-b-2 transition-colors ${
              activeTab === "tester"
                ? "border-[#76B900] text-[#76B900]"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
          >
            <Terminal className="w-4 h-4" />
            Operations & Live Testing
          </button>
          <button
            onClick={() => setActiveTab("architecture")}
            className={`flex items-center gap-2 py-3 px-3 text-xs font-medium border-b-2 transition-colors ${
              activeTab === "architecture"
                ? "border-[#76B900] text-[#76B900]"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
          >
            <Layers className="w-4 h-4" />
            Queue Architecture & Invariants
          </button>
          <button
            onClick={() => setActiveTab("config")}
            className={`flex items-center gap-2 py-3 px-3 text-xs font-medium border-b-2 transition-colors ${
              activeTab === "config"
                ? "border-[#76B900] text-[#76B900]"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
          >
            <Sliders className="w-4 h-4" />
            Bindings & Configuration
          </button>
          <button
            onClick={() => setActiveTab("audit")}
            className={`flex items-center gap-2 py-3 px-3 text-xs font-medium border-b-2 transition-colors ${
              activeTab === "audit"
                ? "border-[#76B900] text-[#76B900]"
                : "border-transparent text-slate-400 hover:text-slate-200"
            }`}
          >
            <Shield className="w-4 h-4" />
            Audit & Reconciliation Matrix
          </button>
        </div>
      </header>

      {/* Main Container */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === "tester" && (
          <div className="space-y-6">
            {/* Quick Metrics Cards */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-4 flex items-center justify-between">
                <div>
                  <div className="text-xs text-slate-400 font-medium">Sliding Window Limit</div>
                  <div className="text-xl font-bold font-mono text-white mt-1">40 RPM</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">60,000 ms sliding window</div>
                </div>
                <div className="w-10 h-10 rounded-lg bg-blue-950/40 border border-blue-800/40 flex items-center justify-center text-blue-400">
                  <Activity className="w-5 h-5" />
                </div>
              </div>

              <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-4 flex items-center justify-between">
                <div>
                  <div className="text-xs text-slate-400 font-medium">Inter-Request Cooldown</div>
                  <div className="text-xl font-bold font-mono text-[#76B900] mt-1">1,670 ms</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">Guaranteed dispatch spacing</div>
                </div>
                <div className="w-10 h-10 rounded-lg bg-[#76B900]/15 border border-[#76B900]/30 flex items-center justify-center text-[#76B900]">
                  <Clock className="w-5 h-5" />
                </div>
              </div>

              <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-4 flex items-center justify-between">
                <div>
                  <div className="text-xs text-slate-400 font-medium">Maximum Queue Depth</div>
                  <div className="text-xl font-bold font-mono text-amber-400 mt-1">100 slots</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">60s max queue wait timeout</div>
                </div>
                <div className="w-10 h-10 rounded-lg bg-amber-950/40 border border-amber-800/40 flex items-center justify-center text-amber-400">
                  <Database className="w-5 h-5" />
                </div>
              </div>

              <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-4 flex items-center justify-between">
                <div>
                  <div className="text-xs text-slate-400 font-medium">Aggregate Body Memory</div>
                  <div className="text-xl font-bold font-mono text-purple-400 mt-1">32 MB</div>
                  <div className="text-[11px] text-slate-500 mt-0.5">Max 100 concurrent body readers</div>
                </div>
                <div className="w-10 h-10 rounded-lg bg-purple-950/40 border border-purple-800/40 flex items-center justify-center text-purple-400">
                  <Shield className="w-5 h-5" />
                </div>
              </div>
            </div>

            {/* Test Controller Panel */}
            <div className="bg-[#0D131F] border border-slate-800 rounded-xl overflow-hidden shadow-xl">
              <div className="px-6 py-4 border-b border-slate-800 flex flex-wrap items-center justify-between gap-4 bg-slate-900/40">
                <div className="flex items-center gap-2">
                  <Terminal className="w-5 h-5 text-[#76B900]" />
                  <h2 className="text-sm font-semibold text-white">Proxy Diagnostic & Verification Console</h2>
                </div>
                <div className="text-xs text-slate-400 font-mono">
                  Target Host: <span className="text-slate-200">{endpointUrl || "Not configured"}</span>
                </div>
              </div>

              <div className="p-6 space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs font-medium text-slate-400 mb-1.5">
                      Target Worker URL
                    </label>
                    <input
                      type="text"
                      value={endpointUrl}
                      onChange={e => setEndpointUrl(e.target.value)}
                      placeholder="https://nvidia-queue-proxy.5s5k7kk.workers.dev"
                      className="w-full bg-[#070A0F] border border-slate-700/80 rounded-lg px-3.5 py-2 text-xs font-mono text-white placeholder-slate-600 focus:outline-none focus:border-[#76B900]"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-slate-400 mb-1.5">
                      Proxy Key (PROXY_KEY) — Optional for /health and /ready
                    </label>
                    <input
                      type="password"
                      value={proxyKey}
                      onChange={e => setProxyKey(e.target.value)}
                      placeholder="Enter proxy key to test authenticated routes..."
                      className="w-full bg-[#070A0F] border border-slate-700/80 rounded-lg px-3.5 py-2 text-xs font-mono text-white placeholder-slate-600 focus:outline-none focus:border-[#76B900]"
                    />
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-3 pt-2">
                  <button
                    disabled={isRunningTest}
                    onClick={() => runProbe("health")}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-medium text-white transition-all disabled:opacity-50"
                  >
                    <Activity className="w-3.5 h-3.5 text-blue-400" />
                    Probe /health (Liveness)
                  </button>

                  <button
                    disabled={isRunningTest}
                    onClick={() => runProbe("ready")}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-medium text-white transition-all disabled:opacity-50"
                  >
                    <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                    Probe /ready (Readiness)
                  </button>

                  <button
                    disabled={isRunningTest}
                    onClick={() => runProbe("burst")}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-[#76B900]/20 hover:bg-[#76B900]/30 border border-[#76B900]/40 text-xs font-medium text-[#76B900] transition-all disabled:opacity-50"
                  >
                    <Zap className="w-3.5 h-3.5" />
                    Simulate 3 Concurrent Requests
                  </button>

                  <button
                    disabled={isRunningTest}
                    onClick={() => runProbe("chat")}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-xs font-medium text-white transition-all disabled:opacity-50"
                  >
                    <Play className="w-3.5 h-3.5" />
                    Execute Test Completion
                  </button>

                  {isRunningTest && (
                    <div className="flex items-center gap-2 text-xs text-slate-400 ml-auto">
                      <RefreshCw className="w-3.5 h-3.5 animate-spin text-[#76B900]" />
                      <span>Dispatching request...</span>
                    </div>
                  )}
                </div>
              </div>

              {/* Execution Console Output */}
              <div className="border-t border-slate-800 bg-[#070A0F] p-4">
                <div className="flex items-center justify-between text-xs text-slate-400 font-mono mb-2 pb-2 border-b border-slate-800/60">
                  <span>TELEMETRY & RESPONSE FEED</span>
                  <button
                    onClick={() => setTestResults([])}
                    className="text-slate-500 hover:text-slate-300 transition-colors"
                  >
                    Clear Output
                  </button>
                </div>

                {testResults.length === 0 ? (
                  <div className="py-8 text-center text-xs text-slate-600 font-mono">
                    No requests dispatched yet. Click one of the probe buttons above to observe proxy responses.
                  </div>
                ) : (
                  <div className="space-y-2 max-h-80 overflow-y-auto font-mono text-xs pr-1">
                    {testResults.map(res => (
                      <div
                        key={res.id}
                        className="p-2.5 rounded-lg bg-slate-900/60 border border-slate-800/80 flex flex-col gap-1.5"
                      >
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            <span
                              className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${
                                res.status === 200
                                  ? "bg-emerald-950 text-emerald-400 border border-emerald-800"
                                  : res.status === 401
                                  ? "bg-amber-950 text-amber-400 border border-amber-800"
                                  : res.status === 429
                                  ? "bg-rose-950 text-rose-400 border border-rose-800"
                                  : "bg-red-950 text-red-400 border border-red-800"
                              }`}
                            >
                              {typeof res.status === "number" ? `HTTP ${res.status}` : "FAILED"}
                            </span>
                            <span className="font-semibold text-slate-200">{res.type}</span>
                          </div>
                          <div className="flex items-center gap-3 text-slate-500 text-[11px]">
                            <span>{res.durationMs} ms</span>
                            <span>{res.timestamp}</span>
                          </div>
                        </div>
                        <div className="text-slate-400 text-[11px]">{res.message}</div>
                        {res.details && (
                          <pre className="mt-1 p-2 rounded bg-black/40 text-[10px] text-slate-400 overflow-x-auto border border-slate-800/50">
                            {JSON.stringify(res.details, null, 2)}
                          </pre>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Quick Curl Reference */}
            <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-5">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Terminal className="w-4 h-4 text-[#76B900]" />
                  <span className="text-xs font-semibold text-white">Direct Backend Client cURL Invocation</span>
                </div>
                <button
                  onClick={() =>
                    copyToClipboard(
                      `curl -X POST "${endpointUrl}/v1/chat/completions" \\\n  -H "Authorization: Bearer YOUR_PROXY_KEY" \\\n  -H "Content-Type: application/json" \\\n  -d '{"model": "nvidia/nemotron-3.5-super-vl-120b-a12b", "messages": [{"role": "user", "content": "Hello"}], "stream": false}'`,
                      "curl"
                    )
                  }
                  className="flex items-center gap-1 text-xs text-slate-400 hover:text-white transition-colors"
                >
                  <Copy className="w-3.5 h-3.5" />
                  {copiedKey === "curl" ? "Copied!" : "Copy Command"}
                </button>
              </div>
              <pre className="bg-[#070A0F] p-3 rounded-lg border border-slate-800 text-[11px] font-mono text-slate-300 overflow-x-auto leading-relaxed">
{`curl -X POST "${endpointUrl}/v1/chat/completions" \\
  -H "Authorization: Bearer YOUR_PROXY_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "model": "nvidia/nemotron-3.5-super-vl-120b-a12b",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'`}
              </pre>
            </div>
          </div>
        )}

        {activeTab === "architecture" && (
          <div className="space-y-6">
            {/* Architectural Flow Diagram */}
            <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-6">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h3 className="text-base font-semibold text-white">End-to-End Request Pipeline</h3>
                  <p className="text-xs text-slate-400">Strict serialization and resource bounded admission flow</p>
                </div>
                <span className="text-xs font-mono text-[#76B900] bg-[#76B900]/10 border border-[#76B900]/30 px-2 py-0.5 rounded">
                  Guaranteed FIFO
                </span>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-5 gap-3 my-6 font-mono text-xs">
                <div className="bg-[#070A0F] border border-slate-800 rounded-lg p-3.5 flex flex-col justify-between">
                  <div className="text-[10px] text-slate-500 uppercase">Phase 1</div>
                  <div className="font-semibold text-white mt-1">Inbound Guard</div>
                  <p className="text-[11px] text-slate-400 mt-2 font-sans">
                    Timing-safe `PROXY_KEY` verification; only `POST /v1/*` admitted.
                  </p>
                  <div className="mt-3 text-[10px] text-emerald-400 flex items-center gap-1 font-sans">
                    <CheckCircle2 className="w-3 h-3" /> 401 on mismatch
                  </div>
                </div>

                <div className="bg-[#070A0F] border border-slate-800 rounded-lg p-3.5 flex flex-col justify-between">
                  <div className="text-[10px] text-slate-500 uppercase">Phase 2</div>
                  <div className="font-semibold text-white mt-1">Memory Budget</div>
                  <p className="text-[11px] text-slate-400 mt-2 font-sans">
                    Bounded body read (max 20MB payload, 32MB aggregate, max 100 readers).
                  </p>
                  <div className="mt-3 text-[10px] text-purple-400 flex items-center gap-1 font-sans">
                    <CheckCircle2 className="w-3 h-3" /> Exact cleanup
                  </div>
                </div>

                <div className="bg-[#070A0F] border border-[#76B900]/40 rounded-lg p-3.5 flex flex-col justify-between bg-[#76B900]/5">
                  <div className="text-[10px] text-[#76B900] uppercase font-bold">Phase 3 (Core)</div>
                  <div className="font-semibold text-[#76B900] mt-1">RateGate Queue</div>
                  <p className="text-[11px] text-slate-300 mt-2 font-sans">
                    Global Durable Object FIFO queue. Enforces 40 RPM limit & 1,670ms spacing.
                  </p>
                  <div className="mt-3 text-[10px] text-[#76B900] flex items-center gap-1 font-sans">
                    <Database className="w-3 h-3" /> SQLite state
                  </div>
                </div>

                <div className="bg-[#070A0F] border border-slate-800 rounded-lg p-3.5 flex flex-col justify-between">
                  <div className="text-[10px] text-slate-500 uppercase">Phase 4</div>
                  <div className="font-semibold text-white mt-1">Upstream Dispatch</div>
                  <p className="text-[11px] text-slate-400 mt-2 font-sans">
                    Injects `NVIDIA_API_KEY`, strips hop-by-hop & client proxy headers.
                  </p>
                  <div className="mt-3 text-[10px] text-blue-400 flex items-center gap-1 font-sans">
                    <Lock className="w-3 h-3" /> Sanitized
                  </div>
                </div>

                <div className="bg-[#070A0F] border border-slate-800 rounded-lg p-3.5 flex flex-col justify-between">
                  <div className="text-[10px] text-slate-500 uppercase">Phase 5</div>
                  <div className="font-semibold text-white mt-1">429 Retry & Stream</div>
                  <p className="text-[11px] text-slate-400 mt-2 font-sans">
                    Parses Retry-After; applies exponential backoff + additive jitter. Pipes SSE stream.
                  </p>
                  <div className="mt-3 text-[10px] text-amber-400 flex items-center gap-1 font-sans">
                    <RefreshCw className="w-3 h-3" /> Up to 5 retries
                  </div>
                </div>
              </div>

              <div className="mt-6 border-t border-slate-800 pt-4">
                <h4 className="text-xs font-semibold text-white uppercase tracking-wider mb-2">
                  Non-Negotiable Invariants Enforced
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs text-slate-300 font-sans">
                  <div className="flex items-start gap-2 bg-[#070A0F] p-3 rounded-lg border border-slate-800/80">
                    <CheckCircle2 className="w-4 h-4 text-[#76B900] shrink-0 mt-0.5" />
                    <div>
                      <strong className="text-white">Strict Upstream Key Isolation:</strong> Downstream callers authenticate with `PROXY_KEY`. The `NVIDIA_API_KEY` is injected solely within the Cloudflare Worker runtime and never transmitted to or readable by clients.
                    </div>
                  </div>
                  <div className="flex items-start gap-2 bg-[#070A0F] p-3 rounded-lg border border-slate-800/80">
                    <CheckCircle2 className="w-4 h-4 text-[#76B900] shrink-0 mt-0.5" />
                    <div>
                      <strong className="text-white">Deterministic Additive Jitter:</strong> Retries strictly honor upstream `Retry-After`. The jitter algorithm never shortens the delay below the required backoff floor, preventing thundering herds.
                    </div>
                  </div>
                  <div className="flex items-start gap-2 bg-[#070A0F] p-3 rounded-lg border border-slate-800/80">
                    <CheckCircle2 className="w-4 h-4 text-[#76B900] shrink-0 mt-0.5" />
                    <div>
                      <strong className="text-white">Fail-Closed Cancellation:</strong> If an upstream stream cancellation fails to settle within 10,000ms grace, the queue slot is retained to prevent overlapping concurrent operations from breaching account quotas.
                    </div>
                  </div>
                  <div className="flex items-start gap-2 bg-[#070A0F] p-3 rounded-lg border border-slate-800/80">
                    <CheckCircle2 className="w-4 h-4 text-[#76B900] shrink-0 mt-0.5" />
                    <div>
                      <strong className="text-white">Bounded Error Body Parsing:</strong> 429 error bodies from upstream are read with a strict 64KB cap and 30s timeout, substituting clean JSON error envelopes if exceeded.
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === "config" && (
          <div className="space-y-6">
            <div className="bg-[#0D131F] border border-slate-800 rounded-xl overflow-hidden">
              <div className="p-6 border-b border-slate-800">
                <h3 className="text-base font-semibold text-white">Environment Variables & Cloudflare Secrets</h3>
                <p className="text-xs text-slate-400 mt-1">
                  Configured via `wrangler secret put &lt;NAME&gt;` or `wrangler.jsonc` environment variables.
                </p>
              </div>

              <div className="divide-y divide-slate-800 font-mono text-xs">
                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center bg-[#070A0F]/50">
                  <div className="font-bold text-[#76B900]">PROXY_KEY</div>
                  <div className="text-slate-400">Secret (Required)</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    Shared secret used by backend callers in `Authorization: Bearer` or `x-proxy-key`.
                  </div>
                </div>

                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center bg-[#070A0F]/50">
                  <div className="font-bold text-[#76B900]">NVIDIA_API_KEY</div>
                  <div className="text-slate-400">Secret (Required)</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    NVIDIA NIM upstream API key (`nvapi-...`). Injected into outbound requests.
                  </div>
                </div>

                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center">
                  <div className="font-bold text-white">RPM</div>
                  <div className="text-slate-400">Default: 40</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    Maximum requests per 60-second sliding window admitted to the upstream.
                  </div>
                </div>

                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center">
                  <div className="font-bold text-white">COOLDOWN_MS</div>
                  <div className="text-slate-400">Default: 1,670 ms</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    Minimum milliseconds required between successive upstream request dispatches.
                  </div>
                </div>

                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center">
                  <div className="font-bold text-white">MAX_QUEUE_DEPTH</div>
                  <div className="text-slate-400">Default: 100</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    Maximum waiting requests in the FIFO queue before rejecting new arrivals with 503.
                  </div>
                </div>

                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center">
                  <div className="font-bold text-white">MAX_BODY_BYTES</div>
                  <div className="text-slate-400">Default: 20 MB</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    Maximum single request body size admitted into memory before queueing.
                  </div>
                </div>

                <div className="p-4 grid grid-cols-1 md:grid-cols-4 gap-2 items-center">
                  <div className="font-bold text-white">MAX_AGGREGATE_BODY_BYTES</div>
                  <div className="text-slate-400">Default: 32 MB</div>
                  <div className="md:col-span-2 text-slate-300 font-sans">
                    Maximum total buffered memory allowed across all concurrent pending requests.
                  </div>
                </div>
              </div>
            </div>

            {/* Wrangler Secrets CLI helper */}
            <div className="bg-[#0D131F] border border-slate-800 rounded-xl p-5">
              <div className="flex items-center justify-between mb-3">
                <span className="text-xs font-semibold text-white">Wrangler Secret Deployment Commands</span>
                <button
                  onClick={() =>
                    copyToClipboard(
                      `npx wrangler secret put PROXY_KEY\nnpx wrangler secret put NVIDIA_API_KEY`,
                      "wrangler"
                    )
                  }
                  className="flex items-center gap-1 text-xs text-slate-400 hover:text-white"
                >
                  <Copy className="w-3.5 h-3.5" />
                  {copiedKey === "wrangler" ? "Copied!" : "Copy"}
                </button>
              </div>
              <pre className="bg-[#070A0F] p-3 rounded-lg border border-slate-800 text-xs font-mono text-slate-300">
{`# Set proxy authentication secret
npx wrangler secret put PROXY_KEY

# Set NVIDIA API key
npx wrangler secret put NVIDIA_API_KEY

# Deploy to Cloudflare Workers
npm run deploy`}
              </pre>
            </div>
          </div>
        )}

        {activeTab === "audit" && (
          <div className="space-y-6">
            <div className="bg-[#0D131F] border border-slate-800 rounded-xl overflow-hidden">
              <div className="p-6 border-b border-slate-800 flex items-center justify-between">
                <div>
                  <h3 className="text-base font-semibold text-white">Audit Issues & Verification Matrix</h3>
                  <p className="text-xs text-slate-400 mt-1">
                    Reconciliation of audit items AUDIT-01 through AUDIT-10 against test results.
                  </p>
                </div>
                <div className="flex items-center gap-2 text-xs font-mono text-emerald-400 bg-emerald-950/60 border border-emerald-500/30 px-3 py-1 rounded-full">
                  <CheckCircle2 className="w-4 h-4" />
                  <span>All Invariants Verified</span>
                </div>
              </div>

              <div className="divide-y divide-slate-800 font-sans text-xs">
                {[
                  {
                    id: "AUDIT-01 (#5)",
                    title: "Bound concurrent request-body reads before queue admission",
                    status: "PASS",
                    notes: "Enforces max 20MB per body, 32MB aggregate memory, and max 100 concurrent body readers.",
                  },
                  {
                    id: "AUDIT-02 (#6)",
                    title: "Prevent queue stalls when upstream stream cancellation hangs",
                    status: "PASS",
                    notes: "Fail-closed grace period: retains slot after 10s grace to safeguard upstream RPM quota.",
                  },
                  {
                    id: "AUDIT-03 (#7)",
                    title: "Browser CORS requirements and origin allowlist",
                    status: "PASS",
                    notes: "CORS kept disabled by default; proxy key kept server-side in backend caller.",
                  },
                  {
                    id: "AUDIT-04 (#8)",
                    title: "GET /v1/models route support",
                    status: "PASS",
                    notes: "Intentionally unsupported (returns clean 404); avoids unauthorized model exposure.",
                  },
                  {
                    id: "AUDIT-05 (#9)",
                    title: "Respect upstream Retry-After",
                    status: "PASS",
                    notes: "Parses integer seconds and HTTP-date formats; applies additive jitter that never undershoots floor.",
                  },
                  {
                    id: "AUDIT-06 (#10)",
                    title: "Separate liveness (/health) and readiness (/ready)",
                    status: "PASS",
                    notes: "/health is zero-dependency liveness; /ready checks bindings without disclosing secret values.",
                  },
                  {
                    id: "AUDIT-07 (#11)",
                    title: "Reproducible dependencies & header sanitization",
                    status: "PASS",
                    notes: "npm ci in GitHub Actions; connection-nominated and proxy authentication headers stripped.",
                  },
                  {
                    id: "AUDIT-08 (#12)",
                    title: "Wrangler observability configuration",
                    status: "PASS",
                    notes: "Enabled in wrangler.jsonc with head sampling and invocation logging without secret leaks.",
                  },
                  {
                    id: "AUDIT-09 (#13)",
                    title: "Operational documentation",
                    status: "PASS",
                    notes: "Consolidated root README.md with runbook, release checklist, and local test procedures.",
                  },
                  {
                    id: "AUDIT-10 (#14)",
                    title: "Final release verification & test suite",
                    status: "PASS",
                    notes: "194/194 tests passing across unit, Miniflare runtime, and smoke-validator suites.",
                  },
                ].map(item => (
                  <div key={item.id} className="p-4 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
                    <div className="space-y-1">
                      <div className="flex items-center gap-2">
                        <span className="font-mono font-bold text-[#76B900] text-[11px]">{item.id}</span>
                        <span className="font-medium text-white">{item.title}</span>
                      </div>
                      <p className="text-slate-400 text-[11px]">{item.notes}</p>
                    </div>
                    <span className="shrink-0 px-2.5 py-1 rounded-md text-[11px] font-mono font-bold bg-emerald-950/80 text-emerald-400 border border-emerald-800/80">
                      {item.status}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
