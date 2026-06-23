// Typed client for the benchui server API. The bench SPA talks to its OWN server (the Go benchui
// server that embeds this build), which in turn drives the WaveSpan cluster. This is a pure module:
// no React, no DOM beyond `fetch`/`EventSource`.
//
// SSE notes:
//   - GET  /api/runs/{id}/stream  is a plain GET, so it can use the browser `EventSource`.
//   - POST /api/dataset/load      is an SSE response to a POST body; `EventSource` only does GET, so
//                                 we stream it by hand via `fetch` + `response.body.getReader()`.

// ---------------------------------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------------------------------

/** A parameter a workload accepts (`GET /api/workloads`). */
export interface WorkloadParam {
  name: string;
  type: string;
  default: unknown;
}

/** A workload kind and its tunable parameters (`GET /api/workloads`). */
export interface WorkloadInfo {
  kind: string;
  params: WorkloadParam[];
}

/** One cluster node, addressed by its admin endpoint. */
export interface NodeRef {
  name: string;
  adminAddr: string;
}

/** Per-node reachability from a target probe (`POST /api/target/probe`). */
export interface ProbeNode {
  name: string;
  reachable: boolean;
  profiling: boolean;
}

/** Result of probing a target (`POST /api/target/probe`). */
export interface ProbeResult {
  dataAddr: string;
  nodes: ProbeNode[];
}

/** A workload selection for a run: a kind plus concrete parameter values. */
export interface WorkloadSelection {
  kind: string;
  params: Record<string, unknown>;
}

/** Body for creating a run (`POST /api/runs`). */
export interface CreateRunBody {
  dataAddr: string;
  graph: string;
  workloads: WorkloadSelection[];
  concurrency: number;
  durationMs: number;
}

/** Body for loading a dataset (`POST /api/dataset/load`). */
export interface LoadDatasetBody {
  dataAddr: string;
  graph: string;
  users: number;
  follows: number;
  kv: number;
}

/** One workload's rolling stats inside a `Sample` window. */
export interface WindowStat {
  tput: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  errs: number;
  total: number;
}

/** A live sample emitted ~1/s on the run stream (`GET /api/runs/{id}/stream`). */
export interface Sample {
  timeMs: number;
  perWorkload: Record<string, WindowStat>;
}

/** Current state + summary of a run (`GET /api/runs/{id}`). */
export interface RunState {
  state: string;
  summary: {
    perWorkload: Record<string, WindowStat>;
  };
}

/** One analysed slice of a profiling report. */
export interface ReportSection {
  kind: string;
  title: string;
  explain: string;
  unit: string;
  total: number;
  agg: unknown;
  app: unknown;
  perNode: unknown;
  notes: string[];
}

/** A profiling report (`GET /api/profile/{pid}/report`). */
export interface Report {
  bench: string;
  nodes: string[];
  cpuSeconds: number;
  sections: ReportSection[];
}

/** Body for probing a target (`POST /api/target/probe`). */
export interface ProbeBody {
  dataAddr: string;
  nodes: NodeRef[];
}

/** Body for starting a profile capture (`POST /api/runs/{id}/profile`). */
export interface ProfileBody {
  cpuSeconds: number;
  nodes: NodeRef[];
}

// ---------------------------------------------------------------------------------------------------
// Low-level fetch helpers
// ---------------------------------------------------------------------------------------------------

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`GET ${path} failed: ${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

async function postJSON<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`POST ${path} failed: ${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

async function postNoBody(path: string, body?: unknown): Promise<void> {
  const res = await fetch(path, {
    method: "POST",
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`POST ${path} failed: ${res.status} ${res.statusText}`);
}

// ---------------------------------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------------------------------

/** `GET /api/workloads` — available workload kinds and their parameters. */
export function listWorkloads(): Promise<WorkloadInfo[]> {
  return getJSON<WorkloadInfo[]>("/api/workloads");
}

/** `POST /api/target/probe` — check reachability + profiling support of a target. */
export function probeTarget(body: ProbeBody): Promise<ProbeResult> {
  return postJSON<ProbeResult>("/api/target/probe", body);
}

/** `POST /api/runs` — create a run; returns its id. */
export function createRun(body: CreateRunBody): Promise<{ id: string }> {
  return postJSON<{ id: string }>("/api/runs", body);
}

/** `GET /api/runs/{id}` — current state + summary of a run. */
export function getRun(id: string): Promise<RunState> {
  return getJSON<RunState>(`/api/runs/${encodeURIComponent(id)}`);
}

/** `POST /api/runs/{id}/start` */
export function startRun(id: string): Promise<void> {
  return postNoBody(`/api/runs/${encodeURIComponent(id)}/start`);
}

/** `POST /api/runs/{id}/pause` */
export function pauseRun(id: string): Promise<void> {
  return postNoBody(`/api/runs/${encodeURIComponent(id)}/pause`);
}

/** `POST /api/runs/{id}/resume` */
export function resumeRun(id: string): Promise<void> {
  return postNoBody(`/api/runs/${encodeURIComponent(id)}/resume`);
}

/** `POST /api/runs/{id}/stop` */
export function stopRun(id: string): Promise<void> {
  return postNoBody(`/api/runs/${encodeURIComponent(id)}/stop`);
}

/** `POST /api/runs/{id}/profile` — start a CPU profile capture; returns the profile id. */
export function startProfile(id: string, body: ProfileBody): Promise<{ pid: string }> {
  return postJSON<{ pid: string }>(`/api/runs/${encodeURIComponent(id)}/profile`, body);
}

/** `GET /api/profile/{pid}/report` — analysed profiling report. */
export function getReport(pid: string): Promise<Report> {
  return getJSON<Report>(`/api/profile/${encodeURIComponent(pid)}/report`);
}

/** URL for a raw per-node profile artifact (`GET /api/profile/{pid}/raw/{node}.{kind}.pb.gz`). */
export function rawProfileURL(pid: string, node: string, kind: string): string {
  return `/api/profile/${encodeURIComponent(pid)}/raw/${encodeURIComponent(node)}.${encodeURIComponent(
    kind,
  )}.pb.gz`;
}

// ---------------------------------------------------------------------------------------------------
// SSE streams
// ---------------------------------------------------------------------------------------------------

/** Handle to close an open SSE stream. */
export interface StreamHandle {
  close(): void;
}

/**
 * Subscribe to a run's live sample stream (`GET /api/runs/{id}/stream`). This is a GET, so we use the
 * native `EventSource`. Each `data:` line is a `Sample` JSON object. Returns a handle to close it.
 */
export function openSampleStream(
  runId: string,
  onSample: (s: Sample) => void,
  onError?: (e: Event) => void,
): StreamHandle {
  const es = new EventSource(`/api/runs/${encodeURIComponent(runId)}/stream`);
  es.onmessage = (ev) => {
    if (!ev.data) return;
    try {
      onSample(JSON.parse(ev.data) as Sample);
    } catch {
      // Ignore malformed frames (e.g. keep-alive comments surface elsewhere, not as onmessage).
    }
  };
  es.onerror = (ev) => {
    onError?.(ev);
  };
  return { close: () => es.close() };
}

/**
 * Stream dataset-load progress (`POST /api/dataset/load`). Because the response is `text/event-stream`
 * over a POST body, the native `EventSource` (GET-only) cannot be used; we read the response body via
 * a `ReadableStream` reader and parse `data:` lines by hand. `onMsg` is called once per `data:` frame.
 *
 * Returns a handle: `close()` aborts the in-flight request. The returned promise resolves when the
 * stream ends, or rejects on a transport error (it does NOT reject on `close()` — that resolves).
 */
export function openLoadStream(
  body: LoadDatasetBody,
  onMsg: (data: string) => void,
): StreamHandle & { done: Promise<void> } {
  const ctrl = new AbortController();

  const done = (async () => {
    let res: Response;
    try {
      res = await fetch("/api/dataset/load", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });
    } catch (err) {
      if (ctrl.signal.aborted) return;
      throw err;
    }
    if (!res.ok) throw new Error(`POST /api/dataset/load failed: ${res.status} ${res.statusText}`);
    if (!res.body) throw new Error("POST /api/dataset/load: response has no body to stream");

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";

    const flushEvent = (raw: string) => {
      // An SSE event is one or more lines separated by \n; data is the joined `data:` lines.
      const dataLines: string[] = [];
      for (const line of raw.split("\n")) {
        if (line.startsWith("data:")) {
          // Strip "data:" and an optional single leading space, per the SSE spec.
          dataLines.push(line.slice(5).replace(/^ /, ""));
        }
      }
      if (dataLines.length > 0) onMsg(dataLines.join("\n"));
    };

    try {
      for (;;) {
        const { done: streamDone, value } = await reader.read();
        if (streamDone) break;
        buf += decoder.decode(value, { stream: true });
        // Events are delimited by a blank line (\n\n). Normalise CRLF first.
        buf = buf.replace(/\r\n/g, "\n");
        let sep: number;
        while ((sep = buf.indexOf("\n\n")) !== -1) {
          const raw = buf.slice(0, sep);
          buf = buf.slice(sep + 2);
          if (raw.trim() !== "") flushEvent(raw);
        }
      }
      // Flush any trailing event not terminated by a blank line.
      buf += decoder.decode();
      if (buf.trim() !== "") flushEvent(buf.replace(/\r\n/g, "\n"));
    } catch (err) {
      if (ctrl.signal.aborted) return;
      throw err;
    }
  })();

  return { close: () => ctrl.abort(), done };
}
