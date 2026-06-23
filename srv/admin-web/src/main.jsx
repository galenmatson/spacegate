import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const ADMIN_API_BASE = "/api/v2/admin";
const AUTH_API_BASE = "/api/v2/auth";

const emptyEndpointForm = {
  display_name: "",
  endpoint_key: "",
  provider: "openai_compatible",
  base_url: "",
  auth_mode: "none",
  api_key_env: "",
  api_key: "",
  default_model: "",
  timeout_s: 30,
  enabled: true,
  notes: "",
};

const terminalJobStatuses = new Set(["succeeded", "failed", "cancelled"]);

const actionGuidance = {
  build_database: {
    group: "build",
    purpose: "Runs the full deterministic pipeline: download, cook, ingest, promote, and verify.",
    prerequisites: "Use when you intentionally want a fresh served build from current source inputs.",
    writes: "$SPACEGATE_STATE_DIR/raw, cooked, out/<build_id>, reports/<build_id>, and served/current through scripts.",
    next: "Inspect the job log and verification report before publishing or running retention.",
    warning: "Long-running and high impact. Do not manually edit raw, cooked, out, reports, or served artifacts.",
    duration: "Long",
  },
  verify_build: {
    group: "build",
    purpose: "Checks the served build, or a specific build id, against schema, provenance, and runtime gates.",
    prerequisites: "Run after build/promotion and before publish, deployment, or cleanup recommendations.",
    writes: "Verification reports under the state report tree.",
    next: "If verification succeeds, publish or continue with deployment/retention decisions.",
    duration: "Short to medium",
  },
  publish_db: {
    group: "build",
    purpose: "Packages the promoted build and updates download metadata for public release artifacts.",
    prerequisites: "Requires a verified build. Leave build id empty to publish served/current.",
    writes: "Public download metadata and package/report files under the configured dl root.",
    next: "Confirm current metadata and keep a release metadata backup before risky changes.",
    warning: "This affects what download clients see; it does not change immutable build contents.",
    duration: "Medium",
  },
  score_coolness: {
    group: "presentation",
    purpose: "Generates deterministic disc coolness ranking and scoring reports for a build.",
    prerequisites: "Use after a valid build exists. Ephemeral scoring is useful for experiments.",
    writes: "Disc scoring artifacts and reports, unless ephemeral mode is selected.",
    next: "Save a profile if the result is worth preserving, then activate it deliberately.",
    duration: "Medium",
  },
  save_coolness_profile: {
    group: "presentation",
    purpose: "Persists an immutable coolness profile version without activating it.",
    prerequisites: "Use after evaluating weights through preview or scoring jobs.",
    writes: "Coolness profile metadata.",
    next: "Activate the saved profile when it should become the default presentation policy.",
    duration: "Short",
  },
  apply_coolness_profile: {
    group: "presentation",
    purpose: "Activates a saved immutable coolness profile version.",
    prerequisites: "The profile id and version should already exist and be reviewed.",
    writes: "Active coolness profile selection metadata.",
    next: "Regenerate scores or snapshots if the visible presentation should change.",
    duration: "Short",
  },
  generate_snapshots: {
    group: "presentation",
    purpose: "Renders snapshot images for filtered coolness-ranked systems.",
    prerequisites: "Run after scoring when the top targets or view parameters changed.",
    writes: "Snapshot assets and manifests in disc/build artifact paths.",
    next: "Reload public search/detail views to confirm new images are referenced correctly.",
    duration: "Medium to long",
  },
  backup_admin_db: {
    group: "recovery",
    purpose: "Creates a point-in-time backup of admin auth, sessions, jobs, audit, and registry state.",
    prerequisites: "Run before restore operations or risky auth/admin changes.",
    writes: "$SPACEGATE_STATE_DIR/admin/backups/admin_db.",
    next: "Use the backup filename if a restore is needed later.",
    duration: "Short",
  },
  restore_admin_db: {
    group: "recovery",
    purpose: "Restores admin auth/audit database tables from a named backup file.",
    prerequisites: "Create a fresh backup first unless the current DB is already known bad.",
    writes: "Admin DB auth, allowlist, sessions, audit, inference registry, and related admin tables.",
    next: "Verify login, allowlist, audit visibility, and endpoint registry after restore.",
    warning: "Can change who can log in and what audit/history is visible.",
    duration: "Short",
  },
  backup_release_metadata: {
    group: "recovery",
    purpose: "Backs up public release metadata and the current download symlink target.",
    prerequisites: "Run before publish or before manually repairing download metadata.",
    writes: "$SPACEGATE_STATE_DIR/admin/backups/release_metadata.",
    next: "Use the backup id if download metadata needs rollback.",
    duration: "Short",
  },
  restore_release_metadata: {
    group: "recovery",
    purpose: "Restores /dl/current.json and optionally the /dl/current symlink from a metadata backup.",
    prerequisites: "Use when publish/deploy left public download metadata pointing at the wrong release or missing fields.",
    writes: "Download metadata and, when selected, the current download symlink.",
    next: "Verify public download status and release metadata after restore.",
    warning: "This does not rebuild science artifacts or change served/current; it repairs what release/download clients see.",
    duration: "Short",
  },
  restart_services: {
    group: "service",
    purpose: "Restarts API/web processes tracked by the legacy service runner.",
    prerequisites: "Use for local process-runner mode, not Docker compose deployments unless the host is configured that way.",
    writes: "Runtime process state and logs.",
    next: "Verify API/web health after restart.",
    warning: "The Admin UI may briefly disconnect.",
    duration: "Short",
  },
  stop_services: {
    group: "service",
    purpose: "Stops API/web processes tracked by the legacy service runner.",
    prerequisites: "Use only when intentionally taking those services down.",
    writes: "Runtime process state.",
    next: "Start services from the host if Admin becomes unavailable.",
    warning: "The Admin UI may disconnect immediately.",
    duration: "Short",
  },
};

const actionGroups = [
  {
    key: "build",
    title: "Build Pipeline",
    description: "Build, verify, and publish deterministic science artifacts in order.",
    actions: ["build_database", "verify_build", "publish_db"],
    sequence: ["Build Database", "Verify Build", "Publish Database", "Retention after verified promotion"],
  },
  {
    key: "presentation",
    title: "Presentation Generation",
    description: "Generate ranking and snapshot artifacts without changing canonical science rows.",
    actions: ["score_coolness", "save_coolness_profile", "apply_coolness_profile", "generate_snapshots"],
    sequence: ["Score Coolness", "Save Profile", "Activate Profile", "Generate Snapshots"],
  },
  {
    key: "recovery",
    title: "Backups and Recovery",
    description: "Create rollback points and recover admin or release metadata state.",
    actions: ["backup_admin_db", "restore_admin_db", "backup_release_metadata", "restore_release_metadata"],
    sequence: ["Backup First", "Restore Only When Needed", "Verify Auth or Release Metadata"],
  },
  {
    key: "service",
    title: "Service Control",
    description: "Legacy process-runner controls for API/web service state.",
    actions: ["restart_services", "stop_services"],
    sequence: ["Confirm Runtime Mode", "Run Action", "Verify Health"],
  },
];

const auditPresets = [
  { key: "all", label: "All", params: {} },
  { key: "auth", label: "Auth", params: { event_prefix: "auth." } },
  { key: "actions", label: "Admin Actions", params: { event_prefix: "admin.action." } },
  { key: "inference", label: "Inference", params: { event_prefix: "admin.inference." } },
  { key: "queries", label: "Queries", params: { event_prefix: "api.search." } },
  { key: "errors", label: "Errors", params: { result: "error" } },
  { key: "denies", label: "Denies", params: { result: "deny" } },
];

function readCookie(name) {
  const prefix = `${name}=`;
  const item = document.cookie
    .split("; ")
    .find((value) => value.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : "";
}

function compactError(data, fallback) {
  return String(data?.detail?.message || data?.error?.message || data?.message || fallback || "Request failed");
}

async function fetchJson(path, options = {}) {
  const response = await fetch(path, {
    credentials: "include",
    ...options,
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_) {
    data = { raw: text };
  }
  return { response, data };
}

function formatInt(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "0";
  return Math.round(number).toLocaleString("en-US");
}

function formatLatency(value) {
  if (value === null || value === undefined || value === "") return "n/a";
  return `${formatInt(value)} ms`;
}

function formatBytes(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let size = number;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toLocaleString("en-US", { maximumFractionDigits: index <= 1 ? 0 : 1 })} ${units[index]}`;
}

function formatPct(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return `${number.toLocaleString("en-US", { maximumFractionDigits: 1 })}%`;
}

function formatDate(value) {
  if (!value) return "n/a";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function compactId(value, size = 18) {
  const text = String(value || "");
  if (text.length <= size) return text || "n/a";
  return `${text.slice(0, size - 1)}...`;
}

function normalizeOptional(value) {
  const trimmed = String(value || "").trim();
  return trimmed ? trimmed : null;
}

function buildCsrfHeaders(csrf) {
  if (!csrf?.cookie_name) return {};
  const token = readCookie(csrf.cookie_name);
  return token ? { [csrf.header_name || "X-CSRF-Token"]: token } : {};
}

function dateMs(value) {
  if (!value) return null;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function formatDurationMs(ms) {
  if (!Number.isFinite(ms) || ms < 0) return "n/a";
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours}h ${rest}m` : `${hours}h`;
}

function jobDuration(job) {
  const start = dateMs(job?.started_at || job?.created_at);
  if (start === null) return "n/a";
  const end = dateMs(job?.finished_at) ?? Date.now();
  return formatDurationMs(end - start);
}

function jobStatusTone(status) {
  const value = String(status || "");
  if (value === "succeeded") return "ok";
  if (value === "failed") return "danger";
  if (value === "cancelled") return "muted";
  if (value === "queued" || value === "running") return "warn";
  return "";
}

function riskTone(riskLevel) {
  const value = String(riskLevel || "low");
  if (value === "high") return "danger";
  if (value === "medium") return "warn";
  return "ok";
}

function parseMaybeJson(value) {
  if (value === null || value === undefined || value === "") return {};
  if (typeof value === "object") return value;
  try {
    return JSON.parse(String(value));
  } catch (_) {
    return { raw: String(value) };
  }
}

function jsonBlock(value) {
  return JSON.stringify(value ?? {}, null, 2);
}

function actionLabel(action) {
  if (!action) return "Action";
  return String(action)
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function App() {
  const [authState, setAuthState] = useState({ loading: true, data: null, error: "" });
  const [activeScreen, setActiveScreen] = useState("overview");

  useEffect(() => {
    let cancelled = false;
    async function loadAuth() {
      try {
        const { response, data } = await fetchJson(`${AUTH_API_BASE}/me`);
        if (cancelled) return;
        if (!response.ok) {
          setAuthState({ loading: false, data: null, error: compactError(data, response.status) });
          return;
        }
        setAuthState({ loading: false, data, error: "" });
      } catch (error) {
        if (!cancelled) {
          setAuthState({ loading: false, data: null, error: String(error) });
        }
      }
    }
    loadAuth();
    return () => {
      cancelled = true;
    };
  }, []);

  const auth = authState.data;
  const csrf = auth?.csrf || {};

  async function logout() {
    const token = csrf.cookie_name ? readCookie(csrf.cookie_name) : "";
    await fetch(`${AUTH_API_BASE}/logout`, {
      method: "POST",
      credentials: "include",
      headers: token ? { [csrf.header_name || "X-CSRF-Token"]: token } : {},
    });
    window.location.href = "/admin/";
  }

  if (authState.loading) {
    return <div className="boot">Loading admin session...</div>;
  }

  if (authState.error) {
    return (
      <div className="boot">
        <h1>Spacegate Admin</h1>
        <p>{authState.error}</p>
      </div>
    );
  }

  if (!auth?.auth_enabled) {
    return (
      <div className="boot">
        <h1>Spacegate Admin</h1>
        <p>Auth is disabled.</p>
      </div>
    );
  }

  if (!auth?.authenticated) {
    return (
      <div className="boot">
        <h1>Spacegate Admin</h1>
        <a className="button primary" href={`${AUTH_API_BASE}/login/google?next=${encodeURIComponent("/admin/")}`}>
          Sign in with Google
        </a>
      </div>
    );
  }

  return (
    <div className="admin-shell">
      <aside className="sidebar">
        <div>
          <div className="brand">Spacegate Admin</div>
          <div className="identity">{auth.user?.email}</div>
        </div>
        <nav>
          <button className={activeScreen === "overview" ? "active" : ""} onClick={() => setActiveScreen("overview")}>Overview</button>
          <button className={activeScreen === "builds" ? "active" : ""} onClick={() => setActiveScreen("builds")}>Builds</button>
          <button className={activeScreen === "dataset" ? "active" : ""} onClick={() => setActiveScreen("dataset")}>Dataset</button>
          <button className={activeScreen === "inference" ? "active" : ""} onClick={() => setActiveScreen("inference")}>Inference</button>
          <button className={activeScreen === "operations" ? "active" : ""} onClick={() => setActiveScreen("operations")}>Operations</button>
          <button className={activeScreen === "agency" ? "active" : ""} onClick={() => setActiveScreen("agency")}>Agency</button>
        </nav>
        <button className="button" onClick={logout}>Log out</button>
      </aside>
      <main className="workspace">
        {activeScreen === "overview" ? (
          <OverviewScreen auth={auth} />
        ) : activeScreen === "inference" ? (
          <InferenceScreen csrf={csrf} />
        ) : activeScreen === "operations" ? (
          <OperationsScreen csrf={csrf} />
        ) : (
          <PlaceholderScreen name={activeScreen} />
        )}
      </main>
    </div>
  );
}

function OverviewScreen({ auth }) {
  const [state, setState] = useState({
    loading: true,
    status: null,
    dataset: null,
    jobs: [],
    endpoints: [],
    errors: {},
  });

  async function loadOverview() {
    setState((current) => ({ ...current, loading: true }));
    const [statusResult, datasetResult, jobsResult, endpointsResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/status`),
      fetchJson(`${ADMIN_API_BASE}/status/dataset`),
      fetchJson(`${ADMIN_API_BASE}/actions/jobs?limit=8`),
      fetchJson(`${ADMIN_API_BASE}/inference/endpoints`),
    ]);

    const errors = {};
    if (!statusResult.response.ok) errors.status = compactError(statusResult.data, statusResult.response.status);
    if (!datasetResult.response.ok) errors.dataset = compactError(datasetResult.data, datasetResult.response.status);
    if (!jobsResult.response.ok) errors.jobs = compactError(jobsResult.data, jobsResult.response.status);
    if (!endpointsResult.response.ok) errors.inference = compactError(endpointsResult.data, endpointsResult.response.status);

    setState({
      loading: false,
      status: statusResult.response.ok ? statusResult.data : null,
      dataset: datasetResult.response.ok ? datasetResult.data : null,
      jobs: jobsResult.response.ok && Array.isArray(jobsResult.data.items) ? jobsResult.data.items : [],
      endpoints: endpointsResult.response.ok && Array.isArray(endpointsResult.data.items) ? endpointsResult.data.items : [],
      errors,
    });
  }

  useEffect(() => {
    loadOverview();
  }, []);

  const dataset = state.dataset || {};
  const sizes = dataset.sizes_bytes || {};
  const counts = dataset.dataset_counts || {};
  const disk = dataset.disk || {};
  const api = dataset.api_process_runtime || {};
  const duckdb = dataset.duckdb_runtime || {};
  const runningJobs = state.jobs.filter((job) => ["queued", "running"].includes(String(job.status || "")));
  const failedJobs = state.jobs.filter((job) => String(job.status || "") === "failed");
  const healthyEndpoints = state.endpoints.filter((endpoint) => endpoint.last_probe?.status === "ok");
  const modelCount = state.endpoints.reduce((total, endpoint) => total + (Array.isArray(endpoint.models) ? endpoint.models.length : 0), 0);

  const overviewKpis = [
    { label: "API", value: state.status?.status || "n/a", tone: state.status?.status === "ok" ? "ok" : "" },
    { label: "Build", value: compactId(state.status?.build_id, 20) },
    { label: "Jobs active", value: runningJobs.length, tone: runningJobs.length ? "warn" : "ok" },
    { label: "Inference healthy", value: `${healthyEndpoints.length}/${state.endpoints.length}` },
    { label: "Systems", value: formatInt(counts.systems) },
    { label: "Stars", value: formatInt(counts.stars) },
  ];

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Overview</h1>
          <p className="muted">Operational status for the current Spacegate runtime.</p>
        </div>
        <button className="button" onClick={loadOverview}>{state.loading ? "Refreshing..." : "Refresh"}</button>
      </header>

      {Object.keys(state.errors).length ? (
        <div className="status-line danger-line">
          {Object.entries(state.errors).map(([key, value]) => `${key}: ${value}`).join(" | ")}
        </div>
      ) : (
        <div className="status-line">Last checked {formatDate(state.status?.time_utc)} as {auth.user?.email}</div>
      )}

      <div className="overview-kpis">
        {overviewKpis.map((item) => (
          <div className={`kpi ${item.tone || ""}`} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>

      <section className="overview-grid">
        <OverviewCard title="Current Build">
          <OverviewFact label="Build ID" value={state.status?.build_id || "n/a"} />
          <OverviewFact label="Core DB" value={formatBytes(sizes.core_db)} />
          <OverviewFact label="Arm DB" value={formatBytes(sizes.arm_db)} />
          <OverviewFact label="Disc DB" value={formatBytes(sizes.disc_db)} />
          <OverviewFact label="DB path" value={state.status?.db_path || "n/a"} />
        </OverviewCard>

        <OverviewCard title="Runtime Capacity">
          <OverviewFact label="/data used" value={`${formatPct(disk.used_pct)} (${formatBytes(disk.used_bytes)})`} />
          <OverviewFact label="/data free" value={formatBytes(disk.free_bytes)} />
          <OverviewFact label="API RSS" value={formatBytes(api.rss_bytes)} />
          <OverviewFact label="API peak RSS" value={formatBytes(api.peak_rss_bytes)} />
          <OverviewFact label="DuckDB memory" value={`${formatBytes(duckdb.memory_usage_bytes)} / ${formatBytes(duckdb.memory_limit_bytes)}`} />
        </OverviewCard>

        <OverviewCard title="Recent Jobs">
          {state.jobs.length ? (
            <div className="compact-list">
              {state.jobs.map((job) => (
                <div className="compact-row" key={job.job_id}>
                  <span className={`dot ${job.status || ""}`} />
                  <div>
                    <strong>{job.action || "job"}</strong>
                    <span>{job.status || "unknown"} | {compactId(job.job_id, 14)}</span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="muted">No recent jobs.</p>
          )}
          {failedJobs.length ? <p className="notice">{failedJobs.length} recent failed job(s).</p> : null}
        </OverviewCard>

        <OverviewCard title="Inference">
          <OverviewFact label="Endpoints" value={formatInt(state.endpoints.length)} />
          <OverviewFact label="Healthy probes" value={`${healthyEndpoints.length}/${state.endpoints.length}`} />
          <OverviewFact label="Cached models" value={formatInt(modelCount)} />
          <div className="compact-list">
            {state.endpoints.slice(0, 5).map((endpoint) => (
              <div className="compact-row" key={endpoint.endpoint_id}>
                <span className={`dot ${endpoint.last_probe?.status === "ok" ? "succeeded" : "failed"}`} />
                <div>
                  <strong>{endpoint.display_name || endpoint.endpoint_key}</strong>
                  <span>{endpoint.last_probe?.status || "unprobed"} | {(endpoint.models || []).length} models</span>
                </div>
              </div>
            ))}
          </div>
        </OverviewCard>
      </section>
    </div>
  );
}

function OverviewCard({ title, children }) {
  return (
    <section className="panel overview-card">
      <h2>{title}</h2>
      {children}
    </section>
  );
}

function OverviewFact({ label, value }) {
  return (
    <div className="overview-fact">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function OperationsScreen({ csrf }) {
  const [activeTab, setActiveTab] = useState("runbook");
  const [actions, setActions] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [backups, setBackups] = useState({ admin_db: [], release_metadata: [] });
  const [auditItems, setAuditItems] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState("");
  const [selectedJob, setSelectedJob] = useState(null);
  const [logState, setLogState] = useState({ text: "", offset: 0, eof: true, status: "" });
  const [selectedAudit, setSelectedAudit] = useState(null);
  const [nextAuditBefore, setNextAuditBefore] = useState(null);
  const [auditPreset, setAuditPreset] = useState("all");
  const [auditFilters, setAuditFilters] = useState({ event_type: "", result: "", request_id: "", actor_user_id: "" });
  const [status, setStatus] = useState("Loading operations...");
  const [busyAction, setBusyAction] = useState("");

  const headers = useMemo(() => buildCsrfHeaders(csrf), [csrf]);
  const actionsByName = useMemo(() => new Map(actions.map((item) => [item.name, item])), [actions]);

  async function loadActions() {
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/catalog`);
    if (!response.ok) {
      setStatus(`Action catalog: ${compactError(data, response.status)}`);
      return [];
    }
    const items = Array.isArray(data.items) ? data.items : [];
    setActions(items);
    return items;
  }

  async function loadJobs() {
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs?limit=100`);
    if (!response.ok) {
      setStatus(`Jobs: ${compactError(data, response.status)}`);
      return [];
    }
    const items = Array.isArray(data.items) ? data.items : [];
    setJobs(items);
    if (!selectedJobId && items.length) {
      setSelectedJobId(items[0].job_id);
    } else if (selectedJobId) {
      const existing = items.find((job) => job.job_id === selectedJobId);
      if (existing) setSelectedJob(existing);
    }
    return items;
  }

  async function loadBackups() {
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/backups?limit=100`);
    if (!response.ok) {
      setStatus(`Backups: ${compactError(data, response.status)}`);
      return { admin_db: [], release_metadata: [] };
    }
    const payload = {
      admin_db: Array.isArray(data.admin_db) ? data.admin_db : [],
      release_metadata: Array.isArray(data.release_metadata) ? data.release_metadata : [],
    };
    setBackups(payload);
    return payload;
  }

  function auditQueryString({ append = false, preset = auditPreset, filters = auditFilters } = {}) {
    const params = new URLSearchParams();
    params.set("limit", "50");
    if (append && nextAuditBefore) {
      params.set("before_audit_id", String(nextAuditBefore));
    }
    const presetConfig = auditPresets.find((item) => item.key === preset) || auditPresets[0];
    Object.entries(presetConfig.params || {}).forEach(([key, value]) => params.set(key, value));
    Object.entries(filters || {}).forEach(([key, value]) => {
      const trimmed = String(value || "").trim();
      if (trimmed) params.set(key, trimmed);
    });
    return params.toString();
  }

  async function loadAudit({ append = false, preset = auditPreset, filters = auditFilters } = {}) {
    const query = auditQueryString({ append, preset, filters });
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/audit?${query}`);
    if (!response.ok) {
      setStatus(`Audit: ${compactError(data, response.status)}`);
      return [];
    }
    const items = Array.isArray(data.items) ? data.items : [];
    setAuditItems((current) => (append ? [...current, ...items] : items));
    setNextAuditBefore(data.next_before_audit_id || null);
    if (!append && items.length) setSelectedAudit(items[0]);
    if (!append && !items.length) setSelectedAudit(null);
    return items;
  }

  async function loadJobDetail(jobId) {
    if (!jobId) return null;
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}`);
    if (!response.ok) {
      setStatus(`Job ${compactId(jobId)}: ${compactError(data, response.status)}`);
      return null;
    }
    const job = data.job || null;
    setSelectedJob(job);
    return job;
  }

  async function loadLogChunk(jobId, offset, reset = false) {
    if (!jobId) return;
    const safeOffset = Number.isFinite(Number(offset)) ? Number(offset) : 0;
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}/log?offset=${safeOffset}&limit=65536`);
    if (!response.ok) {
      setLogState((current) => ({ ...current, eof: true, status: compactError(data, response.status) }));
      return;
    }
    const chunk = String(data.chunk || "");
    setLogState((current) => ({
      text: reset ? chunk : `${current.text}${chunk}`,
      offset: Number(data.next_offset ?? safeOffset),
      eof: Boolean(data.eof),
      status: data.status || "",
    }));
  }

  async function selectJob(jobId) {
    if (!jobId) return;
    setActiveTab("jobs");
    setSelectedJobId(jobId);
    setLogState({ text: "", offset: 0, eof: false, status: "" });
    await loadJobDetail(jobId);
    await loadLogChunk(jobId, 0, true);
  }

  async function loadOperations() {
    setStatus("Refreshing operations...");
    await Promise.all([loadActions(), loadJobs(), loadBackups(), loadAudit({ append: false })]);
    setStatus("Ready");
  }

  useEffect(() => {
    loadOperations();
  }, []);

  useEffect(() => {
    if (!selectedJobId) return;
    let cancelled = false;
    async function loadSelected() {
      const job = await loadJobDetail(selectedJobId);
      if (!cancelled && job) {
        await loadLogChunk(selectedJobId, 0, true);
      }
    }
    loadSelected();
    return () => {
      cancelled = true;
    };
  }, [selectedJobId]);

  useEffect(() => {
    const hasActiveJob = jobs.some((job) => !terminalJobStatuses.has(String(job.status || "")));
    const selectedActive = selectedJob && !terminalJobStatuses.has(String(selectedJob.status || ""));
    if (!hasActiveJob && !selectedActive) return undefined;
    const timer = window.setInterval(async () => {
      await loadJobs();
      if (selectedJobId) {
        await loadJobDetail(selectedJobId);
        await loadLogChunk(selectedJobId, logState.offset, false);
      }
    }, 3000);
    return () => window.clearInterval(timer);
  }, [jobs, selectedJob?.status, selectedJobId, logState.offset]);

  async function runAction(actionName, params, confirmation) {
    setBusyAction(actionName);
    setStatus(`Starting ${actionLabel(actionName)}...`);
    const payload = { action: actionName, params: params || {} };
    if (confirmation) payload.confirmation = confirmation;
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/run`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    setBusyAction("");
    if (!response.ok) {
      const message = compactError(data, response.status);
      setStatus(`${actionLabel(actionName)} failed to start: ${message}`);
      return { ok: false, message };
    }
    const job = data.job || {};
    setStatus(`Queued ${actionLabel(actionName)} as ${job.job_id || "job"}.`);
    await Promise.all([loadJobs(), loadAudit({ append: false })]);
    if (job.job_id) {
      await selectJob(job.job_id);
    }
    return { ok: true, job };
  }

  async function cancelJob(jobId) {
    if (!jobId) return;
    if (!window.confirm(`Cancel queued job ${jobId}?`)) return;
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: "POST",
      headers,
    });
    if (!response.ok) {
      setStatus(`Cancel failed: ${compactError(data, response.status)}`);
      return;
    }
    setStatus(`Cancelled ${jobId}.`);
    await Promise.all([loadJobs(), loadAudit({ append: false })]);
    await loadJobDetail(jobId);
  }

  function applyAuditPreset(nextPreset) {
    setAuditPreset(nextPreset);
    loadAudit({ append: false, preset: nextPreset, filters: auditFilters });
  }

  function updateAuditFilter(key, value) {
    setAuditFilters((current) => ({ ...current, [key]: value }));
  }

  const activeJobs = jobs.filter((job) => ["queued", "running"].includes(String(job.status || "")));
  const failedJobs = jobs.filter((job) => String(job.status || "") === "failed");
  const latestAdminBackup = backups.admin_db?.[0]?.mtime_utc || backups.admin_db?.[0]?.created_at;
  const latestReleaseBackup = backups.release_metadata?.[0]?.mtime_utc || backups.release_metadata?.[0]?.created_at;
  const kpis = [
    { label: "Active jobs", value: activeJobs.length, tone: activeJobs.length ? "warn" : "ok" },
    { label: "Recent failures", value: failedJobs.length, tone: failedJobs.length ? "danger" : "ok" },
    { label: "Admin DB backups", value: formatInt(backups.admin_db?.length || 0) },
    { label: "Release backups", value: formatInt(backups.release_metadata?.length || 0) },
  ];

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Operations</h1>
          <p className="muted">Runbook actions, execution history, recovery points, and audit forensics.</p>
        </div>
        <button className="button" onClick={loadOperations}>Refresh</button>
      </header>

      <div className="kpi-row">
        {kpis.map((item) => (
          <div className={`kpi ${item.tone || ""}`} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>

      <div className="status-line">
        {status} Admin DB backup: {formatDate(latestAdminBackup)} | Release metadata backup: {formatDate(latestReleaseBackup)}
      </div>

      <div className="tab-row">
        {[
          ["runbook", "Runbook"],
          ["jobs", "Jobs"],
          ["backups", "Backups"],
          ["audit", "Audit Trail"],
        ].map(([key, label]) => (
          <button className={activeTab === key ? "active" : ""} key={key} onClick={() => setActiveTab(key)}>
            {label}
          </button>
        ))}
      </div>

      {activeTab === "runbook" ? (
        <RunbookTab actionsByName={actionsByName} runAction={runAction} busyAction={busyAction} />
      ) : activeTab === "jobs" ? (
        <JobsTab
          jobs={jobs}
          selectedJob={selectedJob}
          logState={logState}
          selectJob={selectJob}
          cancelJob={cancelJob}
          refreshSelected={() => selectedJobId && loadLogChunk(selectedJobId, 0, true)}
        />
      ) : activeTab === "backups" ? (
        <BackupsTab
          backups={backups}
          actionsByName={actionsByName}
          runAction={runAction}
          busyAction={busyAction}
        />
      ) : (
        <AuditTab
          auditItems={auditItems}
          selectedAudit={selectedAudit}
          setSelectedAudit={setSelectedAudit}
          auditPreset={auditPreset}
          setAuditPreset={applyAuditPreset}
          auditFilters={auditFilters}
          updateAuditFilter={updateAuditFilter}
          loadAudit={loadAudit}
          nextAuditBefore={nextAuditBefore}
          selectJob={selectJob}
        />
      )}
    </div>
  );
}

function RunbookTab({ actionsByName, runAction, busyAction }) {
  return (
    <div className="runbook-grid">
      {actionGroups.map((group) => {
        const groupActions = group.actions.map((name) => actionsByName.get(name)).filter(Boolean);
        if (!groupActions.length) return null;
        return (
          <section className="panel runbook-group" key={group.key}>
            <div className="runbook-head">
              <div>
                <h2>{group.title}</h2>
                <p className="muted">{group.description}</p>
              </div>
              <div className="workflow-rail">
                {group.sequence.map((step, index) => (
                  <span key={`${group.key}-${step}`}>{index + 1}. {step}</span>
                ))}
              </div>
            </div>
            <div className="action-grid">
              {groupActions.map((action) => (
                <ActionCard
                  action={action}
                  key={action.name}
                  runAction={runAction}
                  busy={busyAction === action.name}
                />
              ))}
            </div>
          </section>
        );
      })}
    </div>
  );
}

function ActionCard({ action, runAction, busy }) {
  const [values, setValues] = useState(() => initialActionValues(action));
  const [status, setStatus] = useState("");
  const guidance = actionGuidance[action.name] || {};
  const schema = action.params_schema || {};

  useEffect(() => {
    setValues(initialActionValues(action));
    setStatus("");
  }, [action.name]);

  function updateValue(key, value) {
    setValues((current) => ({ ...current, [key]: value }));
  }

  async function submit(event) {
    event.preventDefault();
    const params = {};
    for (const [name, spec] of Object.entries(schema)) {
      const value = values[name];
      if (spec?.type === "boolean") {
        params[name] = Boolean(value);
      } else if (spec?.type === "integer") {
        if (value === "" || value === null || value === undefined) {
          if (spec.required) {
            setStatus(`Missing required parameter: ${name}`);
            return;
          }
        } else {
          params[name] = Number.parseInt(String(value), 10);
        }
      } else if (String(value || "").trim()) {
        params[name] = String(value).trim();
      } else if (spec?.required) {
        setStatus(`Missing required parameter: ${name}`);
        return;
      }
    }
    const confirmation = String(values.confirmation || "").trim();
    if (action.requires_confirmation && !confirmation) {
      setStatus("Confirmation phrase is required.");
      return;
    }
    setStatus("Submitting...");
    const result = await runAction(action.name, params, confirmation);
    setStatus(result.ok ? `Queued ${result.job?.job_id || "job"}.` : result.message);
  }

  return (
    <form className={`action-card ${riskTone(action.risk_level)}`} onSubmit={submit}>
      <div className="action-title-row">
        <div>
          <h3>{action.display_name || actionLabel(action.name)}</h3>
          <div className="action-meta">
            <span className={`badge ${riskTone(action.risk_level)}`}>{action.risk_level || "low"} risk</span>
            {action.requires_confirmation ? <span className="badge warn">confirmation</span> : null}
          </div>
        </div>
      </div>
      <p>{guidance.purpose || action.description}</p>
      <div className="hint-list">
        {guidance.prerequisites ? <div><strong>Before:</strong> {guidance.prerequisites}</div> : null}
        {guidance.writes ? <div><strong>Writes:</strong> {guidance.writes}</div> : null}
        {guidance.next ? <div><strong>Next:</strong> {guidance.next}</div> : null}
        {guidance.duration ? <div><strong>Expected:</strong> {guidance.duration}</div> : null}
        {guidance.warning ? <div className="warning-text"><strong>Warning:</strong> {guidance.warning}</div> : null}
      </div>
      <div className="action-fields">
        {Object.entries(schema).map(([name, spec]) => (
          <ActionParamField
            key={name}
            name={name}
            spec={spec || {}}
            value={values[name]}
            updateValue={updateValue}
          />
        ))}
        {action.requires_confirmation ? (
          <label>
            <span>Confirmation phrase</span>
            <input
              value={values.confirmation || ""}
              onChange={(event) => updateValue("confirmation", event.target.value)}
              placeholder={action.confirmation_phrase || `RUN ${action.name}`}
            />
          </label>
        ) : null}
      </div>
      <div className="card-actions">
        <button className={`button ${action.risk_level === "high" ? "danger" : "primary"}`} disabled={busy} type="submit">
          {busy ? "Starting..." : "Start Job"}
        </button>
        {status ? <span className="inline-status">{status}</span> : null}
      </div>
    </form>
  );
}

function ActionParamField({ name, spec, value, updateValue }) {
  const label = spec.label || actionLabel(name);
  if (spec.type === "boolean") {
    return (
      <label className="checkbox-row">
        <input type="checkbox" checked={Boolean(value)} onChange={(event) => updateValue(name, event.target.checked)} />
        <span>{label}</span>
      </label>
    );
  }
  if (name.includes("json") || name === "notes" || name === "reason") {
    return (
      <label>
        <span>{label}</span>
        <textarea
          value={value ?? ""}
          onChange={(event) => updateValue(name, event.target.value)}
          placeholder={spec.placeholder || ""}
          rows={name.includes("json") ? 4 : 3}
        />
      </label>
    );
  }
  return (
    <label>
      <span>{label}</span>
      <input
        type={spec.type === "integer" ? "number" : "text"}
        min={spec.min}
        max={spec.max}
        value={value ?? ""}
        onChange={(event) => updateValue(name, event.target.value)}
        placeholder={spec.placeholder || ""}
      />
    </label>
  );
}

function initialActionValues(action) {
  const values = {};
  for (const [name, spec] of Object.entries(action.params_schema || {})) {
    if (spec?.default !== undefined) {
      values[name] = spec.default;
    } else if (spec?.type === "boolean") {
      values[name] = false;
    } else {
      values[name] = "";
    }
  }
  if (action.requires_confirmation) values.confirmation = "";
  return values;
}

function JobsTab({ jobs, selectedJob, logState, selectJob, cancelJob, refreshSelected }) {
  const activeRows = jobs.filter((job) => ["queued", "running"].includes(String(job.status || "")));
  const selectedStatus = String(selectedJob?.status || "");
  const logDownloadHref = selectedJob ? `${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(selectedJob.job_id)}/log/download` : "";

  return (
    <section className="jobs-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Job Queue</h2>
          <span className="muted">{formatInt(activeRows.length)} active</span>
        </div>
        {jobs.length ? (
          <table className="select-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Action</th>
                <th>Created</th>
                <th>Duration</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr className={selectedJob?.job_id === job.job_id ? "selected" : ""} key={job.job_id}>
                  <td><button className={`link-button ${jobStatusTone(job.status)}`} onClick={() => selectJob(job.job_id)}>{job.status}</button></td>
                  <td>
                    <strong>{actionLabel(job.action)}</strong>
                    <span className="table-subtext">{compactId(job.job_id, 24)}</span>
                  </td>
                  <td>{formatDate(job.created_at)}</td>
                  <td>{jobDuration(job)}</td>
                  <td>{job.error_message ? compactId(job.error_message, 56) : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No admin jobs have been recorded yet. Start with the Runbook tab when you are ready to run an allowlisted action.</div>
        )}
      </div>

      <div className="panel job-detail">
        <div className="panel-head">
          <h2>Selected Job</h2>
          {selectedJob ? <span className={`badge ${jobStatusTone(selectedStatus)}`}>{selectedStatus}</span> : null}
        </div>
        {selectedJob ? (
          <>
            <div className="overview-fact">
              <span>Job ID</span>
              <strong>{selectedJob.job_id}</strong>
            </div>
            <div className="overview-fact">
              <span>Action</span>
              <strong>{actionLabel(selectedJob.action)}</strong>
            </div>
            <div className="overview-fact">
              <span>Timeline</span>
              <strong>{formatDate(selectedJob.created_at)}{" -> "}{formatDate(selectedJob.finished_at || selectedJob.started_at)}</strong>
            </div>
            <div className="overview-fact">
              <span>Exit</span>
              <strong>{selectedJob.exit_code ?? "n/a"} {selectedJob.error_message ? `| ${selectedJob.error_message}` : ""}</strong>
            </div>
            <JobTrap job={selectedJob} logState={logState} />
            <details open>
              <summary>Parameters</summary>
              <pre className="json-box">{jsonBlock(selectedJob.params)}</pre>
            </details>
            <details>
              <summary>Execution Plan</summary>
              <pre className="json-box">{jsonBlock(selectedJob.execution)}</pre>
            </details>
            <div className="log-toolbar">
              <button className="button" onClick={refreshSelected}>Reload Log</button>
              <a className="button" href={logDownloadHref}>Download Log</a>
              {selectedStatus === "queued" ? (
                <button className="button danger" onClick={() => cancelJob(selectedJob.job_id)}>Cancel Queued Job</button>
              ) : null}
            </div>
            <pre className="log-box">{logState.text || "No log output is available yet."}</pre>
          </>
        ) : (
          <div className="empty">Select a job to inspect parameters, execution plan, log output, and troubleshooting hints.</div>
        )}
      </div>
    </section>
  );
}

function JobTrap({ job, logState }) {
  const status = String(job.status || "");
  const created = dateMs(job.created_at);
  const started = dateMs(job.started_at);
  const now = Date.now();
  const messages = [];
  if (status === "queued" && created !== null && now - created > 5 * 60 * 1000) {
    messages.push("This job has been queued for more than five minutes. Check whether another long-running job is occupying the runner.");
  }
  if (status === "running" && started !== null && now - started > 60 * 60 * 1000) {
    messages.push("This job has been running for more than an hour. Inspect the live log before starting dependent actions.");
  }
  if (status === "failed") {
    messages.push("This job failed. Review the final log lines, exit code, and correlated audit entries before retrying.");
  }
  if (terminalJobStatuses.has(status) && !logState.text) {
    messages.push("The job row is terminal but no log text was returned. The log file may be missing or unreadable.");
  }
  if (!messages.length) return null;
  return (
    <div className="trap-list">
      {messages.map((message) => <div key={message}>{message}</div>)}
    </div>
  );
}

function BackupsTab({ backups, actionsByName, runAction, busyAction }) {
  const backupActions = ["backup_admin_db", "restore_admin_db", "backup_release_metadata", "restore_release_metadata"]
    .map((name) => actionsByName.get(name))
    .filter(Boolean);
  return (
    <div className="backups-layout">
      <section className="panel">
        <h2>Recovery Actions</h2>
        <div className="action-grid compact">
          {backupActions.map((action) => (
            <ActionCard
              action={action}
              key={action.name}
              runAction={runAction}
              busy={busyAction === action.name}
            />
          ))}
        </div>
      </section>
      <section className="panel">
        <h2>Admin DB Snapshots</h2>
        <BackupTable
          emptyText="No admin DB backups have been created yet. Run Backup Admin DB before risky admin/auth changes."
          items={backups.admin_db || []}
          type="admin_db"
        />
      </section>
      <section className="panel">
        <h2>Release Metadata Snapshots</h2>
        <p className="muted">Restore Release Metadata is used when public download metadata or the /dl/current symlink needs rollback after a publish/deploy mistake. It does not rebuild science artifacts.</p>
        <BackupTable
          emptyText="No release metadata backups have been created yet. Run Backup Release Metadata before publish or manual metadata repair."
          items={backups.release_metadata || []}
          type="release_metadata"
        />
      </section>
    </div>
  );
}

function BackupTable({ items, type, emptyText }) {
  if (!items.length) return <div className="empty">{emptyText}</div>;
  return (
    <table>
      <thead>
        <tr>
          <th>{type === "admin_db" ? "Filename" : "Backup ID"}</th>
          <th>Created</th>
          <th>Size / target</th>
        </tr>
      </thead>
      <tbody>
        {items.map((item) => (
          <tr key={item.name || item.backup_id}>
            <td>{item.name || item.backup_id}</td>
            <td>{formatDate(item.created_at || item.mtime_utc)}</td>
            <td>{type === "admin_db" ? formatBytes(item.bytes) : (item.current_symlink_target || "n/a")}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function AuditTab({
  auditItems,
  selectedAudit,
  setSelectedAudit,
  auditPreset,
  setAuditPreset,
  auditFilters,
  updateAuditFilter,
  loadAudit,
  nextAuditBefore,
  selectJob,
}) {
  return (
    <section className="audit-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Audit Trail</h2>
          <button className="button" onClick={() => loadAudit({ append: false })}>Refresh</button>
        </div>
        <div className="audit-presets">
          {auditPresets.map((preset) => (
            <button className={auditPreset === preset.key ? "active" : ""} key={preset.key} onClick={() => setAuditPreset(preset.key)}>
              {preset.label}
            </button>
          ))}
        </div>
        <div className="filter-grid">
          <label>
            <span>Event type</span>
            <input value={auditFilters.event_type} onChange={(event) => updateAuditFilter("event_type", event.target.value)} placeholder="auth.login.denied" />
          </label>
          <label>
            <span>Result</span>
            <select value={auditFilters.result} onChange={(event) => updateAuditFilter("result", event.target.value)}>
              <option value="">Any</option>
              <option value="success">success</option>
              <option value="deny">deny</option>
              <option value="error">error</option>
            </select>
          </label>
          <label>
            <span>Request ID</span>
            <input value={auditFilters.request_id} onChange={(event) => updateAuditFilter("request_id", event.target.value)} placeholder="req_..." />
          </label>
          <label>
            <span>Actor user ID</span>
            <input value={auditFilters.actor_user_id} onChange={(event) => updateAuditFilter("actor_user_id", event.target.value)} />
          </label>
        </div>
        <div className="card-actions">
          <button className="button primary" onClick={() => loadAudit({ append: false })}>Apply Filters</button>
          <button className="button" disabled={!nextAuditBefore} onClick={() => loadAudit({ append: true })}>Load Older</button>
        </div>
        <div className="audit-list">
          {auditItems.length ? auditItems.map((entry) => (
            <button className={selectedAudit?.audit_id === entry.audit_id ? "audit-row selected" : "audit-row"} key={entry.audit_id} onClick={() => setSelectedAudit(entry)}>
              <span className={`badge ${entry.result === "success" ? "ok" : entry.result === "error" ? "danger" : "warn"}`}>{entry.result}</span>
              <strong>#{entry.audit_id} {entry.event_type}</strong>
              <span>{formatDate(entry.created_at)} {entry.request_id || ""} {entry.correlation_id || ""}</span>
            </button>
          )) : <div className="empty">No audit entries match the current filters.</div>}
        </div>
      </div>
      <div className="panel audit-detail">
        <h2>Selected Event</h2>
        {selectedAudit ? (
          <>
            <div className="overview-fact">
              <span>Event</span>
              <strong>{selectedAudit.event_type}</strong>
            </div>
            <div className="overview-fact">
              <span>Route</span>
              <strong>{selectedAudit.method || ""} {selectedAudit.route || "n/a"}</strong>
            </div>
            <div className="overview-fact">
              <span>Correlation</span>
              <strong>{selectedAudit.correlation_id || "n/a"}</strong>
            </div>
            {selectedAudit.correlation_id && String(selectedAudit.correlation_id).startsWith("job_") ? (
              <button className="button" onClick={() => selectJob(selectedAudit.correlation_id)}>Open Correlated Job</button>
            ) : null}
            <pre className="json-box">{jsonBlock(selectedAudit)}</pre>
          </>
        ) : (
          <div className="empty">Select an audit row to inspect request, route, result, and details payload.</div>
        )}
      </div>
    </section>
  );
}

function PlaceholderScreen({ name }) {
  const title = name.charAt(0).toUpperCase() + name.slice(1);
  return (
    <section className="panel">
      <h1>{title}</h1>
      <p className="muted">This section will migrate from the embedded Admin UI after Inference reaches parity.</p>
    </section>
  );
}

function InferenceScreen({ csrf }) {
  const [endpoints, setEndpoints] = useState([]);
  const [stats, setStats] = useState([]);
  const [form, setForm] = useState(emptyEndpointForm);
  const [status, setStatus] = useState("Loading registry...");
  const [busyEndpoint, setBusyEndpoint] = useState(null);

  const csrfHeaders = useMemo(() => {
    if (!csrf?.cookie_name) return {};
    const token = readCookie(csrf.cookie_name);
    return token ? { [csrf.header_name || "X-CSRF-Token"]: token } : {};
  }, [csrf]);

  async function loadInference() {
    setStatus("Refreshing registry...");
    const [endpointResult, statsResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/inference/endpoints`),
      fetchJson(`${ADMIN_API_BASE}/inference/stats`),
    ]);
    if (!endpointResult.response.ok) {
      setStatus(compactError(endpointResult.data, endpointResult.response.status));
      return;
    }
    setEndpoints(Array.isArray(endpointResult.data.items) ? endpointResult.data.items : []);
    if (statsResult.response.ok) {
      setStats(Array.isArray(statsResult.data.items) ? statsResult.data.items : []);
    }
    setStatus("Ready");
  }

  useEffect(() => {
    loadInference();
  }, []);

  function updateForm(key, value) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  async function createEndpoint(event) {
    event.preventDefault();
    const payload = {
      display_name: form.display_name.trim(),
      endpoint_key: normalizeOptional(form.endpoint_key),
      provider: form.provider,
      base_url: form.base_url.trim(),
      auth_mode: form.auth_mode,
      api_key_env: normalizeOptional(form.api_key_env),
      api_key: normalizeOptional(form.api_key),
      default_model: normalizeOptional(form.default_model),
      timeout_s: Number.parseInt(String(form.timeout_s || "30"), 10),
      enabled: Boolean(form.enabled),
      notes: normalizeOptional(form.notes),
      role_defaults: {},
    };
    if (!payload.display_name || !payload.base_url) {
      setStatus("Display name and base URL are required.");
      return;
    }
    setStatus("Creating endpoint...");
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/inference/endpoints`, {
      method: "POST",
      headers: csrfHeaders,
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      setStatus(compactError(data, response.status));
      return;
    }
    setForm(emptyEndpointForm);
    setStatus("Endpoint added.");
    await loadInference();
  }

  async function pollEndpoint(endpointId) {
    setBusyEndpoint(endpointId);
    setStatus(`Polling endpoint ${endpointId}...`);
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/inference/endpoints/${endpointId}/poll-models`, {
      method: "POST",
      headers: csrfHeaders,
    });
    setBusyEndpoint(null);
    if (!response.ok) {
      setStatus(compactError(data, response.status));
      await loadInference();
      return;
    }
    setStatus(`Polled ${formatInt((data.models || []).length)} models.`);
    await loadInference();
  }

  async function deleteEndpoint(endpoint) {
    if (!window.confirm(`Remove endpoint "${endpoint.display_name || endpoint.endpoint_key}"?`)) return;
    setBusyEndpoint(endpoint.endpoint_id);
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/inference/endpoints/${endpoint.endpoint_id}`, {
      method: "DELETE",
      headers: csrfHeaders,
    });
    setBusyEndpoint(null);
    if (!response.ok) {
      setStatus(compactError(data, response.status));
      return;
    }
    setStatus("Endpoint removed.");
    await loadInference();
  }

  const kpis = useMemo(() => {
    const enabled = endpoints.filter((item) => item.enabled).length;
    const modelCount = endpoints.reduce((total, item) => total + (Array.isArray(item.models) ? item.models.length : 0), 0);
    const ready = endpoints.filter((item) => item.last_probe?.status === "ok").length;
    return [
      { label: "Endpoints", value: endpoints.length },
      { label: "Enabled", value: enabled },
      { label: "Healthy probes", value: ready },
      { label: "Cached models", value: modelCount },
    ];
  }, [endpoints]);

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Inference</h1>
          <p className="muted">Endpoint registry, model probes, and usage telemetry.</p>
        </div>
        <button className="button" onClick={loadInference}>Refresh</button>
      </header>

      <div className="kpi-row">
        {kpis.map((item) => (
          <div className="kpi" key={item.label}>
            <span>{item.label}</span>
            <strong>{formatInt(item.value)}</strong>
          </div>
        ))}
      </div>

      <div className="status-line">{status}</div>

      <section className="inference-grid">
        <EndpointForm form={form} updateForm={updateForm} createEndpoint={createEndpoint} />
        <EndpointList
          endpoints={endpoints}
          busyEndpoint={busyEndpoint}
          pollEndpoint={pollEndpoint}
          deleteEndpoint={deleteEndpoint}
        />
      </section>

      <UsageStats stats={stats} />
    </div>
  );
}

function EndpointForm({ form, updateForm, createEndpoint }) {
  return (
    <form className="panel endpoint-form" onSubmit={createEndpoint}>
      <h2>Add Endpoint</h2>
      <label>
        <span>Display name</span>
        <input value={form.display_name} onChange={(event) => updateForm("display_name", event.target.value)} placeholder="Photon vLLM" />
      </label>
      <label>
        <span>Endpoint key</span>
        <input value={form.endpoint_key} onChange={(event) => updateForm("endpoint_key", event.target.value)} placeholder="photon-vllm" />
      </label>
      <label>
        <span>Provider</span>
        <select value={form.provider} onChange={(event) => updateForm("provider", event.target.value)}>
          <option value="openai_compatible">OpenAI-compatible</option>
          <option value="openai">OpenAI</option>
          <option value="google">Google Gemini</option>
          <option value="custom">Custom</option>
        </select>
      </label>
      <label>
        <span>Base URL</span>
        <input value={form.base_url} onChange={(event) => updateForm("base_url", event.target.value)} placeholder="http://photon-vllm:8000/v1" />
      </label>
      <label>
        <span>Auth mode</span>
        <select value={form.auth_mode} onChange={(event) => updateForm("auth_mode", event.target.value)}>
          <option value="none">None</option>
          <option value="env">Environment variable</option>
          <option value="stored">Stored encrypted key</option>
        </select>
      </label>
      <label>
        <span>API key env var</span>
        <input value={form.api_key_env} onChange={(event) => updateForm("api_key_env", event.target.value)} placeholder="SPACEGATE_OPENAI_API_KEY" />
      </label>
      <label>
        <span>API key</span>
        <input type="password" value={form.api_key} onChange={(event) => updateForm("api_key", event.target.value)} autoComplete="new-password" />
      </label>
      <label>
        <span>Default model</span>
        <input value={form.default_model} onChange={(event) => updateForm("default_model", event.target.value)} />
      </label>
      <label>
        <span>Timeout seconds</span>
        <input type="number" min="1" max="600" value={form.timeout_s} onChange={(event) => updateForm("timeout_s", event.target.value)} />
      </label>
      <label>
        <span>Notes</span>
        <textarea value={form.notes} onChange={(event) => updateForm("notes", event.target.value)} rows={3} />
      </label>
      <label className="checkbox-row">
        <input type="checkbox" checked={form.enabled} onChange={(event) => updateForm("enabled", event.target.checked)} />
        <span>Enabled</span>
      </label>
      <button className="button primary" type="submit">Add Endpoint</button>
    </form>
  );
}

function EndpointList({ endpoints, busyEndpoint, pollEndpoint, deleteEndpoint }) {
  return (
    <section className="panel">
      <h2>Registered Endpoints</h2>
      <div className="endpoint-list">
        {endpoints.length === 0 ? (
          <div className="empty">No endpoints registered.</div>
        ) : endpoints.map((endpoint) => (
          <article className="endpoint-card" key={endpoint.endpoint_id}>
            <div className="endpoint-head">
              <div>
                <h3>{endpoint.display_name || endpoint.endpoint_key}</h3>
                <div className="muted">{endpoint.endpoint_key}</div>
              </div>
              <span className={endpoint.enabled ? "badge ok" : "badge"}>{endpoint.enabled ? "enabled" : "disabled"}</span>
            </div>
            <div className="endpoint-meta">
              <Meta label="Provider" value={endpoint.provider} />
              <Meta label="Base URL" value={endpoint.base_url} />
              <Meta label="Auth" value={`${endpoint.auth_mode || "none"}${endpoint.api_key_configured ? " / configured" : ""}`} />
              <Meta label="Default model" value={endpoint.default_model || ""} />
              <Meta label="Last probe" value={probeSummary(endpoint.last_probe)} />
            </div>
            {endpoint.notes ? <p className="note">{endpoint.notes}</p> : null}
            <ModelTable models={endpoint.models || []} />
            <div className="card-actions">
              <button className="button" disabled={busyEndpoint === endpoint.endpoint_id} onClick={() => pollEndpoint(endpoint.endpoint_id)}>
                {busyEndpoint === endpoint.endpoint_id ? "Polling..." : "Poll Models"}
              </button>
              <button className="button danger" disabled={busyEndpoint === endpoint.endpoint_id} onClick={() => deleteEndpoint(endpoint)}>
                Remove
              </button>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function Meta({ label, value }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value || "n/a"}</strong>
    </div>
  );
}

function probeSummary(probe) {
  if (!probe) return "never";
  const count = formatInt(probe.model_count || 0);
  const latency = formatLatency(probe.latency_ms);
  return `${probe.status} | ${count} models | ${latency}`;
}

function ModelTable({ models }) {
  return (
    <details className="models">
      <summary>Models ({formatInt(models.length)})</summary>
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th>Context</th>
            <th>Owner</th>
            <th>Last seen</th>
          </tr>
        </thead>
        <tbody>
          {models.length === 0 ? (
            <tr><td colSpan="4">No cached models.</td></tr>
          ) : models.map((model) => (
            <tr key={model.model_id}>
              <td>{model.model_id}</td>
              <td>{model.max_model_len || ""}</td>
              <td>{model.owned_by || ""}</td>
              <td>{model.last_seen_at || ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
}

function UsageStats({ stats }) {
  return (
    <section className="panel">
      <h2>Usage Stats</h2>
      <table>
        <thead>
          <tr>
            <th>Endpoint</th>
            <th>Model</th>
            <th>Requests</th>
            <th>Total tokens</th>
            <th>Avg latency</th>
            <th>Last used</th>
          </tr>
        </thead>
        <tbody>
          {stats.length === 0 ? (
            <tr><td colSpan="6">No usage events recorded yet.</td></tr>
          ) : stats.map((item) => (
            <tr key={`${item.endpoint_id}-${item.model_id}`}>
              <td>{item.display_name || item.endpoint_key || ""}</td>
              <td>{item.model_id || ""}</td>
              <td>{formatInt(item.request_count)}</td>
              <td>{formatInt(item.total_tokens)}</td>
              <td>{item.avg_latency_ms == null ? "" : `${Number(item.avg_latency_ms).toFixed(1)} ms`}</td>
              <td>{item.last_used_at || ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

createRoot(document.getElementById("root")).render(<App />);
