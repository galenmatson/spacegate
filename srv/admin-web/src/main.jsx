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

const fallbackActionGuidance = {
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

const fallbackActionGroups = [
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

function formatFloat(value, digits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  return number.toLocaleString("en-US", { maximumFractionDigits: digits });
}

function toNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function pctFromPart(part, total) {
  const numerator = Number(part);
  const denominator = Number(total);
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) return 0;
  return (numerator / denominator) * 100;
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
        ) : activeScreen === "builds" ? (
          <BuildsScreen csrf={csrf} />
        ) : activeScreen === "dataset" ? (
          <DatasetScreen />
        ) : activeScreen === "inference" ? (
          <InferenceScreen csrf={csrf} />
        ) : activeScreen === "operations" ? (
          <OperationsScreen csrf={csrf} />
        ) : activeScreen === "agency" ? (
          <AgencyScreen />
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

function BuildsScreen({ csrf }) {
  const [state, setState] = useState({
    loading: true,
    status: null,
    dataset: null,
    operations: null,
    actions: [],
    message: "Loading build state...",
  });
  const [busyAction, setBusyAction] = useState("");
  const headers = useMemo(() => buildCsrfHeaders(csrf), [csrf]);

  const actionsByName = useMemo(() => new Map(state.actions.map((item) => [item.name, item])), [state.actions]);

  async function loadBuilds() {
    setState((current) => ({ ...current, loading: true, message: "Refreshing build state..." }));
    const [statusResult, datasetResult, operationsResult, actionsResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/status`),
      fetchJson(`${ADMIN_API_BASE}/status/dataset`),
      fetchJson(`${ADMIN_API_BASE}/operations/status`),
      fetchJson(`${ADMIN_API_BASE}/actions/catalog`),
    ]);
    const errors = [];
    if (!statusResult.response.ok) errors.push(`status: ${compactError(statusResult.data, statusResult.response.status)}`);
    if (!datasetResult.response.ok) errors.push(`dataset: ${compactError(datasetResult.data, datasetResult.response.status)}`);
    if (!operationsResult.response.ok) errors.push(`operations: ${compactError(operationsResult.data, operationsResult.response.status)}`);
    if (!actionsResult.response.ok) errors.push(`actions: ${compactError(actionsResult.data, actionsResult.response.status)}`);
    setState({
      loading: false,
      status: statusResult.response.ok ? statusResult.data : null,
      dataset: datasetResult.response.ok ? datasetResult.data : null,
      operations: operationsResult.response.ok ? operationsResult.data : null,
      actions: actionsResult.response.ok && Array.isArray(actionsResult.data.items) ? actionsResult.data.items : [],
      message: errors.length ? errors.join(" | ") : "Ready",
    });
  }

  useEffect(() => {
    loadBuilds();
  }, []);

  async function runAction(actionName, params, confirmation) {
    setBusyAction(actionName);
    setState((current) => ({ ...current, message: `Starting ${actionLabel(actionName)}...` }));
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
      setState((current) => ({ ...current, message: `${actionLabel(actionName)} failed to start: ${message}` }));
      return { ok: false, message };
    }
    const job = data.job || {};
    setState((current) => ({ ...current, message: `Queued ${actionLabel(actionName)} as ${job.job_id || "job"}.` }));
    await loadBuilds();
    return { ok: true, job };
  }

  const dataset = state.dataset || {};
  const operations = state.operations || {};
  const sizes = dataset.sizes_bytes || {};
  const counts = dataset.dataset_counts || {};
  const disk = dataset.disk || {};
  const builds = operations.builds || {};
  const served = builds.served_current || {};
  const retention = operations.retention || {};
  const recentBuilds = Array.isArray(builds.recent) ? builds.recent : [];
  const tmpBuilds = Array.isArray(builds.tmp) ? builds.tmp : [];
  const currentBuild = recentBuilds.find((item) => item.build_id === served.build_id) || recentBuilds[0] || null;
  const buildActions = ["build_database", "verify_build", "publish_db"].map((name) => actionsByName.get(name)).filter(Boolean);
  const kpis = [
    { label: "Served build", value: compactId(state.status?.build_id || served.build_id, 20) },
    { label: "Build dirs", value: formatInt(builds.out_count || recentBuilds.length) },
    { label: "Temp outputs", value: formatInt(tmpBuilds.length), tone: tmpBuilds.length ? "warn" : "ok" },
    { label: "Retention", value: retention.can_run_now ? "ready" : "blocked", tone: retention.can_run_now ? "ok" : "warn" },
  ];

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Builds</h1>
          <p className="muted">Deterministic science artifacts, report state, and the build/verify/publish path.</p>
        </div>
        <button className="button" onClick={loadBuilds}>{state.loading ? "Refreshing..." : "Refresh"}</button>
      </header>

      <div className="kpi-row">
        {kpis.map((item) => (
          <div className={`kpi ${item.tone || ""}`} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>

      <div className={state.message === "Ready" ? "status-line" : "status-line danger-line"}>{state.message}</div>

      <section className="builds-grid">
        <div className="panel">
          <h2>Current Served Build</h2>
          <OverviewFact label="Build ID" value={state.status?.build_id || served.build_id || "n/a"} />
          <OverviewFact label="Served target" value={served.target || "n/a"} />
          <OverviewFact label="Core / Arm / Disc" value={`${formatBytes(sizes.core_db)} / ${formatBytes(sizes.arm_db)} / ${formatBytes(sizes.disc_db)}`} />
          <OverviewFact label="Systems / Stars / Planets" value={`${formatInt(counts.systems)} / ${formatInt(counts.stars)} / ${formatInt(counts.planets)}`} />
          <OverviewFact label="/data free" value={`${formatBytes(disk.free_bytes)} (${formatPct(100 - Number(disk.used_pct || 0))} free)`} />
        </div>

        <div className="panel">
          <h2>Retention Readiness</h2>
          <p className="muted">Retention should run only after successful promotion and verification, never during ingest or failed-build diagnosis.</p>
          <OverviewFact label="Default keep builds" value={formatInt(retention.default_keep_builds)} />
          <OverviewFact label="Default keep reports" value={formatInt(retention.default_keep_reports)} />
          <OverviewFact label="Script" value={retention.script || "scripts/prune_state_retention.sh"} />
          {Array.isArray(retention.blocked_reasons) && retention.blocked_reasons.length ? (
            <div className="trap-list">
              {retention.blocked_reasons.map((reason) => <div key={reason}>{reason}</div>)}
            </div>
          ) : (
            <div className="status-line">No active build blockers reported. Use dry-run before applying retention.</div>
          )}
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <div>
            <h2>Build Runbook</h2>
            <p className="muted">Sequential path: build database, verify the served or selected build, then publish download metadata only after verification is clean.</p>
          </div>
        </div>
        <div className="action-grid">
          {buildActions.map((action) => (
            <ActionCard action={action} key={action.name} runAction={runAction} busy={busyAction === action.name} />
          ))}
        </div>
      </section>

      <section className="builds-grid">
        <BuildArtifactPanel title="Served Build Artifacts" build={currentBuild} />
        <RecentBuildsPanel builds={recentBuilds} servedBuildId={served.build_id || state.status?.build_id} />
      </section>

      <section className="builds-grid">
        <TempBuildsPanel builds={tmpBuilds} />
        <BuildReportsPanel build={currentBuild} />
      </section>
    </div>
  );
}

function BuildArtifactPanel({ title, build }) {
  return (
    <div className="panel">
      <h2>{title}</h2>
      {build ? (
        <>
          <OverviewFact label="Build ID" value={build.build_id} />
          <OverviewFact label="Path" value={build.path} />
          <OverviewFact label="Size" value={formatBytes(build.size_bytes)} />
          <OverviewFact label="Promotable" value={build.promotable ? "yes" : `missing ${build.missing_required?.join(", ") || "required artifacts"}`} />
          <div className="artifact-flags">
            {Object.entries(build.artifacts || {}).map(([key, value]) => (
              <span className={`badge ${value ? "ok" : "danger"}`} key={key}>{key}: {value ? "yes" : "no"}</span>
            ))}
          </div>
        </>
      ) : (
        <div className="empty">No build artifact summary is available yet.</div>
      )}
    </div>
  );
}

function RecentBuildsPanel({ builds, servedBuildId }) {
  return (
    <div className="panel">
      <h2>Recent Build Directories</h2>
      {builds.length ? (
        <table>
          <thead>
            <tr>
              <th>Build</th>
              <th>Reports</th>
              <th>Artifacts</th>
              <th>Size</th>
            </tr>
          </thead>
          <tbody>
            {builds.map((build) => (
              <tr key={build.build_id}>
                <td>
                  <strong>{build.build_id}</strong>
                  <span className="table-subtext">{build.build_id === servedBuildId ? "served/current" : formatDate(build.mtime_utc)}</span>
                </td>
                <td>{formatInt(build.reports?.count || 0)}</td>
                <td>{build.promotable ? "core+arm" : `missing ${(build.missing_required || []).join(", ")}`}</td>
                <td>{formatBytes(build.size_bytes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty">No immutable build directories were found under the state out directory.</div>
      )}
    </div>
  );
}

function TempBuildsPanel({ builds }) {
  return (
    <div className="panel">
      <h2>Temporary Outputs</h2>
      <p className="muted">Temporary ingest outputs are useful for failure diagnosis. Do not prune them until the root cause is captured.</p>
      {builds.length ? (
        <table>
          <thead>
            <tr><th>Name</th><th>Modified</th><th>Size</th></tr>
          </thead>
          <tbody>
            {builds.map((build) => (
              <tr key={build.name}>
                <td>{build.name}</td>
                <td>{formatDate(build.mtime_utc)}</td>
                <td>{formatBytes(build.size_bytes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty">No temporary ingest output directories are currently present.</div>
      )}
    </div>
  );
}

function BuildReportsPanel({ build }) {
  const files = build?.reports?.files || [];
  return (
    <div className="panel">
      <h2>Report Files</h2>
      {build ? (
        <>
          <OverviewFact label="Reports dir" value={build.reports_dir || "n/a"} />
          <OverviewFact label="Report count" value={formatInt(build.reports?.count || 0)} />
          <OverviewFact label="Latest report mtime" value={formatDate(build.reports?.latest_mtime_utc)} />
          {files.length ? (
            <div className="report-chip-list">
              {files.map((name) => <span className="badge" key={name}>{name}</span>)}
            </div>
          ) : (
            <div className="empty">No JSON reports were found for this build.</div>
          )}
        </>
      ) : (
        <div className="empty">Select or create a build before report files are available.</div>
      )}
    </div>
  );
}

function DatasetScreen() {
  const [activeTab, setActiveTab] = useState("summary");
  const [state, setState] = useState({
    loading: true,
    data: null,
    message: "Loading dataset status...",
  });

  async function loadDataset(forceRefresh = false) {
    setState((current) => ({ ...current, loading: true, message: forceRefresh ? "Refreshing dataset cache..." : "Refreshing dataset status..." }));
    const query = forceRefresh ? "?refresh=1" : "";
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/status/dataset${query}`);
    if (!response.ok) {
      setState({ loading: false, data: null, message: `Dataset status: ${compactError(data, response.status)}` });
      return;
    }
    setState({ loading: false, data, message: "Ready" });
  }

  useEffect(() => {
    loadDataset(false);
  }, []);

  const data = state.data || {};
  const counts = data.dataset_counts || {};
  const sizes = data.sizes_bytes || {};
  const disk = data.disk || {};
  const slice = data.slice_metrics || {};
  const determinism = data.determinism || {};
  const breakdowns = data.breakdowns || {};
  const cache = data.cache || {};
  const catalogContribution = breakdowns.catalog_contribution_report || {};
  const contributionRows = sortedCatalogContributionRows(catalogContribution);
  const kpis = [
    { label: "Systems", value: formatInt(counts.systems) },
    { label: "Stars", value: formatInt(counts.stars) },
    { label: "Planets", value: formatInt(counts.planets) },
    { label: "Determinism", value: datasetDeterminismLabel(determinism), tone: determinism.status === "match" ? "ok" : "warn" },
  ];

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Dataset</h1>
          <p className="muted">Served science artifact, source contribution, schema gates, determinism, and runtime diagnostics.</p>
        </div>
        <div className="button-row">
          <button className="button" onClick={() => loadDataset(false)}>{state.loading ? "Refreshing..." : "Refresh"}</button>
          <button className="button" onClick={() => loadDataset(true)}>Force Refresh</button>
        </div>
      </header>

      <div className="kpi-row">
        {kpis.map((item) => (
          <div className={`kpi ${item.tone || ""}`} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>

      <div className={state.message === "Ready" ? "status-line" : "status-line danger-line"}>
        {state.message} Build: {data.build_id || "unknown"} | Generated: {formatDate(data.generated_at_utc)} | Cache: {cache.hit ? "hit" : "miss"} age {formatFloat(cache.age_s, 3)}s
      </div>

      <section className="dataset-grid">
        <div className="panel">
          <h2>Dataset Contract</h2>
          <OverviewFact label="Total rows" value={formatInt(counts.rows_total)} />
          <OverviewFact label="Layer DBs" value={`core ${formatBytes(sizes.core_db)} | arm ${formatBytes(sizes.arm_db)} | disc ${formatBytes(sizes.disc_db)}`} />
          <OverviewFact label="Arm graph" value={`${formatInt(counts.arm_component_entities)} components, ${formatInt(counts.arm_hierarchy_edges)} hierarchy edges, ${formatInt(counts.arm_orbit_edges)} orbit edges`} />
          <OverviewFact label="Slice profile" value={`${determinism.slice_profile_id || "n/a"} @ ${determinism.slice_profile_version || "n/a"}`} />
          <OverviewFact label="/data free" value={`${formatBytes(disk.free_bytes)} (${formatPct(100 - toNumber(disk.used_pct))} free)`} />
        </div>
        <div className="panel">
          <h2>Operator Readout</h2>
          <DatasetSummaryLines data={data} contributionRows={contributionRows} />
        </div>
      </section>

      <div className="tab-row">
        {[
          ["summary", "Summary"],
          ["science", "Science Shape"],
          ["sources", "Sources"],
          ["quality", "Quality"],
          ["runtime", "Runtime"],
          ["raw", "Raw JSON"],
        ].map(([key, label]) => (
          <button className={activeTab === key ? "active" : ""} key={key} onClick={() => setActiveTab(key)}>
            {label}
          </button>
        ))}
      </div>

      {activeTab === "summary" ? (
        <DatasetSummaryTab data={data} contributionRows={contributionRows} />
      ) : activeTab === "science" ? (
        <DatasetScienceTab data={data} />
      ) : activeTab === "sources" ? (
        <DatasetSourcesTab data={data} contributionRows={contributionRows} />
      ) : activeTab === "quality" ? (
        <DatasetQualityTab data={data} />
      ) : activeTab === "runtime" ? (
        <DatasetRuntimeTab data={data} />
      ) : (
        <section className="panel">
          <h2>Raw Dataset Status JSON</h2>
          <pre className="json-box tall">{jsonBlock(data)}</pre>
        </section>
      )}
    </div>
  );
}

function DatasetSummaryTab({ data, contributionRows }) {
  const counts = data.dataset_counts || {};
  const sizes = data.sizes_bytes || {};
  const slice = data.slice_metrics || {};
  const breakdowns = data.breakdowns || {};
  const exotic = breakdowns.exotic_star_counts || {};
  const compact = breakdowns.compact_object_counts || {};
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Inventory</h2>
        <MetricList
          rows={[
            ["Systems / stars / planets", `${formatInt(counts.systems)} / ${formatInt(counts.stars)} / ${formatInt(counts.planets)}`],
            ["Multi / single systems", `${formatInt(counts.multi_star_systems)} / ${formatInt(counts.single_star_systems)}`],
            ["Exoplanets", `${formatInt(counts.exoplanets_total)} total, ${formatInt(counts.exoplanets_temperate)} temperate, ${formatInt(counts.exoplanets_candidate_habitable)} candidate habitable`],
            ["Arm overlays", `${formatInt(counts.arm_vsx_variability)} VSX, ${formatInt(counts.arm_variability_high)} high variability, ${formatInt(counts.arm_ultracoolsheet_objects)} ultracool`],
            ["Compact objects", `${formatInt(compact.compact_total)} total, ${formatInt(compact.white_dwarf)} white dwarfs, ${formatInt(compact.neutron_star)} neutron stars, ${formatInt(compact.pulsar)} pulsars`],
            ["Exotic highlights", `${formatInt(exotic.brown_dwarf_like_lty)} L/T/Y, ${formatInt(exotic.white_dwarf_like_d_prefix)} WD-like, ${formatInt(exotic.high_proper_motion_ge_1000_mas_yr)} high proper motion`],
          ]}
        />
      </div>
      <div className="panel">
        <h2>Build Slice</h2>
        <MetricList
          rows={[
            ["Backbone input", formatInt(slice.input_backbone_rows)],
            ["Sliced-in stars", formatInt(slice.sliced_in_stars)],
            ["Sliced-out rows", `${formatInt(slice.sliced_out_rows)} (${formatPct(slice.sliced_out_pct)})`],
            ["Policy retained stars", formatInt(slice.policy_retained_stars)],
            ["Policy sliced-out stars", `${formatInt(slice.policy_sliced_out_stars)} (${formatPct(slice.policy_sliced_out_stars_pct)})`],
            ["Parquet footprint", formatBytes(sizes.parquet_total)],
          ]}
        />
      </div>
      <div className="panel">
        <h2>Top Source Catalogs</h2>
        <DatasetBarList rows={(breakdowns.stars_by_source_catalog || []).slice(0, 10).map((row) => ({
          label: row.source_catalog || "?",
          value: row.star_count,
          max: counts.stars,
          detail: `${formatInt(row.star_count)} stars`,
        }))} />
      </div>
      <div className="panel">
        <h2>Top Contribution Utility</h2>
        <DatasetBarList rows={contributionRows.slice(0, 10).map((row) => ({
          label: `${row.catalog || "?"} (${row.domain || "?"})`,
          value: row.utility_score,
          max: 100,
          detail: `${formatFloat(row.utility_score, 2)} utility`,
        }))} />
      </div>
    </section>
  );
}

function DatasetScienceTab({ data }) {
  const counts = data.dataset_counts || {};
  const breakdowns = data.breakdowns || {};
  const spectralRows = (breakdowns.stars_by_spectral_class || []).slice(0, 18);
  const standard = breakdowns.spectral_class_standard_counts || {};
  const sysMult = breakdowns.system_multiplicity_evidence || {};
  const starMult = breakdowns.star_multiplicity_evidence || {};
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Spectral Distribution</h2>
        <DatasetBarList rows={spectralRows.map((row) => ({
          label: row.spectral_class || "?",
          value: row.star_count,
          max: counts.stars,
          detail: `${formatInt(row.star_count)} stars, ${formatPct(row.pct_of_stars)}`,
          color: spectralColor(row.spectral_class),
        }))} />
      </div>
      <div className="panel">
        <h2>Standard Spectral Counts</h2>
        <KeyValueTable rows={["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D", "unknown"].map((key) => [key, formatInt(standard[key])])} />
      </div>
      <div className="panel">
        <h2>System Multiplicity Evidence</h2>
        <MultiplicityTable values={sysMult} total={counts.systems} />
      </div>
      <div className="panel">
        <h2>Star Multiplicity Evidence</h2>
        <MultiplicityTable values={starMult} total={counts.stars} />
      </div>
    </section>
  );
}

function DatasetSourcesTab({ data, contributionRows }) {
  const breakdowns = data.breakdowns || {};
  const catalogContribution = breakdowns.catalog_contribution_report || {};
  const overlaps = catalogOverlapRows(catalogContribution);
  const pipeline = breakdowns.catalog_pipeline_report || {};
  const stages = pipeline.stages || {};
  return (
    <div className="runbook-grid">
      <section className="panel">
        <h2>Catalog Contribution</h2>
        {contributionRows.length ? (
          <table>
            <thead>
              <tr>
                <th>Catalog</th>
                <th>Domain</th>
                <th>Input</th>
                <th>Direct</th>
                <th>Evidence</th>
                <th>Linked</th>
                <th>Utility</th>
              </tr>
            </thead>
            <tbody>
              {contributionRows.slice(0, 35).map((row) => (
                <tr key={`${row.catalog}-${row.domain}`}>
                  <td>{row.catalog || "?"}</td>
                  <td>{row.domain || "?"}</td>
                  <td>{row.input_rows === null || row.input_rows === undefined ? "n/a" : formatInt(row.input_rows)}</td>
                  <td>{formatInt(row.direct_rows)}</td>
                  <td>{formatInt(row.evidence_rows)}</td>
                  <td>{formatInt(row.linked_rows)}</td>
                  <td>{row.utility_tier || "n/a"} ({formatFloat(row.utility_score, 2)})</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No catalog contribution report was found for this build.</div>
        )}
      </section>
      <section className="dataset-grid">
        <div className="panel">
          <h2>Catalog Overlap</h2>
          {overlaps.length ? (
            <table>
              <thead>
                <tr><th>Scope</th><th>Pair</th><th>Intersection</th><th>Jaccard</th><th>Scope Share</th></tr>
              </thead>
              <tbody>
                {overlaps.map((row) => (
                  <tr key={`${row.scope}-${row.left_catalog}-${row.right_catalog}`}>
                    <td>{row.scope}</td>
                    <td>{row.left_catalog || "?"} / {row.right_catalog || "?"}</td>
                    <td>{formatInt(row.intersection_count)}</td>
                    <td>{formatPct(row.jaccard_pct)}</td>
                    <td>{formatPct(row.intersection_pct_of_scope)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty">No overlap metrics are available for this build.</div>
          )}
        </div>
        <div className="panel">
          <h2>Catalog Pipeline</h2>
          <KeyValueTable rows={["download", "cook", "ingest"].map((stage) => {
            const item = stages[stage] || {};
            return [stage, catalogStageSummary(stage, item), item.updated_at || "n/a"];
          })} columns={["Stage", "Summary", "Updated"]} />
        </div>
      </section>
    </div>
  );
}

function DatasetQualityTab({ data }) {
  const breakdowns = data.breakdowns || {};
  const determinism = data.determinism || {};
  const comparison = determinism.comparison || {};
  const tables = comparison.tables || {};
  const qc = breakdowns.qc_report || {};
  const grouping = breakdowns.system_grouping_report || {};
  const match = breakdowns.match_report || {};
  const coolness = breakdowns.coolness_report || {};
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Determinism</h2>
        <MetricList
          rows={[
            ["Status", datasetDeterminismLabel(determinism)],
            ["Current build", determinism.current_build_id || data.build_id || "unknown"],
            ["Baseline build", determinism.baseline_build_id || "n/a"],
            ["Comparable baselines", formatInt(determinism.comparable_baselines)],
            ["Source fingerprint", determinism.source_inputs_fingerprint || "n/a"],
            ["Transform / layer", `${determinism.transform_version || "n/a"} / ${determinism.build_layer || "n/a"}`],
          ]}
        />
        <KeyValueTable rows={["stars", "systems", "planets"].map((key) => [key, tables[key] ? (tables[key].match ? "match" : "mismatch") : "n/a"])} />
      </div>
      <div className="panel">
        <h2>QC and Reports</h2>
        <MetricList
          rows={[
            ["QC report status", qc.status || qc.result || "present"],
            ["Report count hints", `${Object.keys(breakdowns).filter((key) => key.endsWith("_report")).length} report payloads loaded`],
            ["Grouping multi-star systems", formatInt(grouping.multi_star_systems)],
            ["Grouping source", grouping.grouping_strategy || grouping.strategy || "n/a"],
            ["Match report keys", Object.keys(match).length ? Object.keys(match).slice(0, 8).join(", ") : "n/a"],
            ["Coolness profile", coolness.profile_id || coolness.profile || "n/a"],
          ]}
        />
      </div>
      <div className="panel">
        <h2>Loaded Report Payloads</h2>
        <div className="report-chip-list">
          {Object.entries(breakdowns)
            .filter(([key, value]) => key.endsWith("_report") && value && typeof value === "object" && Object.keys(value).length)
            .map(([key]) => <span className="badge" key={key}>{key}</span>)}
        </div>
      </div>
      <div className="panel">
        <h2>Quality Notes</h2>
        <div className="trap-list">
          <div>Canonical science remains immutable in `core`; supplemental evidence belongs in `arm` and presentation artifacts belong in `disc`.</div>
          <div>Verification failures should block promotion, public deployment, and retention recommendations.</div>
          <div>Contradictory values should stay source-native until an explicit adjudication proposal is accepted.</div>
        </div>
      </div>
    </section>
  );
}

function DatasetRuntimeTab({ data }) {
  const sizes = data.sizes_bytes || {};
  const disk = data.disk || {};
  const host = data.host_runtime || {};
  const api = data.api_process_runtime || {};
  const duckdb = data.duckdb_runtime || {};
  const bottlenecks = data.bottleneck_hints || {};
  const timings = Object.entries(data.timings_ms || {}).sort((a, b) => toNumber(b[1]) - toNumber(a[1])).slice(0, 10);
  const hostMemTotal = toNumber(host.mem_total_bytes);
  const hostMemAvailable = toNumber(host.mem_available_bytes);
  const hostMemUsed = Math.max(hostMemTotal - hostMemAvailable, 0);
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Storage</h2>
        <MetricList
          rows={[
            ["Project footprint", formatBytes(sizes.project_total)],
            ["State footprint", formatBytes(sizes.state_total)],
            ["Served build footprint", formatBytes(sizes.build_total), data.paths?.build_dir || ""],
            ["Raw / cooked / out", `${formatBytes(sizes.raw_total)} / ${formatBytes(sizes.cooked_total)} / ${formatBytes(sizes.out_total)}`],
            ["Reports / served / parquet", `${formatBytes(sizes.reports_total)} / ${formatBytes(sizes.served_total)} / ${formatBytes(sizes.parquet_total)}`],
            ["/data partition", `${formatBytes(disk.used_bytes)} used of ${formatBytes(disk.total_bytes)}`, `${formatBytes(disk.free_bytes)} free (${formatPct(disk.used_pct)} used)`],
          ]}
        />
        <DatasetBarList rows={[
          { label: "/data used", value: disk.used_pct, max: 100, detail: `${formatBytes(disk.used_bytes)} / ${formatBytes(disk.total_bytes)}` },
          { label: "Host RAM used", value: pctFromPart(hostMemUsed, hostMemTotal), max: 100, detail: `${formatBytes(hostMemUsed)} / ${formatBytes(hostMemTotal)}` },
          { label: "API RSS / host", value: pctFromPart(api.rss_bytes, hostMemTotal), max: 100, detail: formatBytes(api.rss_bytes) },
          { label: "DuckDB memory", value: pctFromPart(duckdb.memory_usage_bytes, duckdb.memory_limit_bytes), max: 100, detail: `${formatBytes(duckdb.memory_usage_bytes)} / ${formatBytes(duckdb.memory_limit_bytes)}` },
        ]} />
      </div>
      <div className="panel">
        <h2>Runtime</h2>
        <MetricList
          rows={[
            ["CPU", `${formatInt(host.cpu_count)} cores`, `load 1m=${formatFloat(host.loadavg_1m)} 5m=${formatFloat(host.loadavg_5m)} 15m=${formatFloat(host.loadavg_15m)}`],
            ["Host memory", `${formatBytes(hostMemUsed)} used of ${formatBytes(hostMemTotal)}`, `${formatBytes(hostMemAvailable)} available`],
            ["API process", `pid=${api.pid || "?"}, threads=${formatInt(api.threads)}`, `RSS=${formatBytes(api.rss_bytes)}, peak=${formatBytes(api.peak_rss_bytes)}, VM=${formatBytes(api.vm_size_bytes)}`],
            ["Process IO", `read=${formatBytes(api.io_read_bytes)}, write=${formatBytes(api.io_write_bytes)}`],
            ["DuckDB runtime", `db=${formatBytes(duckdb.database_size_bytes)}, wal=${formatBytes(duckdb.wal_size_bytes)}`, `memory=${formatBytes(duckdb.memory_usage_bytes)} / ${formatBytes(duckdb.memory_limit_bytes)}`],
            ["Bottleneck hints", `${bottlenecks.likely_memory_bound ? "memory-bound risk" : "memory headroom likely acceptable"} | ${bottlenecks.likely_io_bound ? "IO-bound risk" : "IO-bound risk low"}`],
          ]}
        />
      </div>
      <div className="panel">
        <h2>Status Query Timings</h2>
        <KeyValueTable rows={timings.map(([key, value]) => [key, `${formatFloat(value, 3)} ms`])} />
      </div>
      <div className="panel">
        <h2>Runtime Notes</h2>
        <div className="hint-list">
          {(bottlenecks.notes || []).map((note) => <div key={note}>{note}</div>)}
        </div>
      </div>
    </section>
  );
}

function DatasetSummaryLines({ data, contributionRows }) {
  const counts = data.dataset_counts || {};
  const sizes = data.sizes_bytes || {};
  const slice = data.slice_metrics || {};
  const breakdowns = data.breakdowns || {};
  const determinism = data.determinism || {};
  const exotic = breakdowns.exotic_star_counts || {};
  const lines = [
    `Total rows: ${formatInt(counts.rows_total)} (${formatInt(counts.systems)} systems, ${formatInt(counts.stars)} stars, ${formatInt(counts.planets)} planets).`,
    `Multiplicity: ${formatInt(counts.multi_star_systems)} multi-star systems and ${formatInt(counts.single_star_systems)} single-star systems.`,
    `Arm graph: ${formatInt(counts.arm_component_entities)} components, ${formatInt(counts.arm_hierarchy_edges)} hierarchy edges, ${formatInt(counts.arm_orbit_edges)} orbit edges.`,
    `Input slice: ${formatInt(slice.input_backbone_rows)} input rows, ${formatInt(slice.sliced_out_rows)} sliced out (${formatPct(slice.sliced_out_pct)}).`,
    `Storage: core ${formatBytes(sizes.core_db)}, arm ${formatBytes(sizes.arm_db)}, disc ${formatBytes(sizes.disc_db)}, state ${formatBytes(sizes.state_total)}.`,
    `Exoplanets: ${formatInt(counts.exoplanets_total)} total, ${formatInt(counts.exoplanets_temperate)} temperate, ${formatInt(counts.exoplanets_candidate_habitable)} candidate habitable.`,
    `Exotic highlights: ${formatInt(exotic.brown_dwarf_like_lty)} L/T/Y, ${formatInt(exotic.white_dwarf_like_d_prefix)} WD-like, ${formatInt(exotic.high_proper_motion_ge_1000_mas_yr)} high proper motion.`,
    `Determinism: ${datasetDeterminismLabel(determinism)} against ${determinism.baseline_build_id || "no baseline"}.`,
    `Catalog contribution rows: ${formatInt(contributionRows.length)}.`,
  ];
  return (
    <div className="narrative-list">
      {lines.map((line) => <div key={line}>{line}</div>)}
    </div>
  );
}

function MetricList({ rows }) {
  return (
    <div className="metric-list">
      {rows.map(([label, value, note]) => (
        <div className="metric-row" key={label}>
          <span>{label}</span>
          <strong>{value}</strong>
          {note ? <em>{note}</em> : null}
        </div>
      ))}
    </div>
  );
}

function KeyValueTable({ rows, columns = ["Key", "Value"] }) {
  return (
    <table>
      <thead>
        <tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.join("|")}>
            {columns.map((_, index) => <td key={`${row[0]}-${index}`}>{row[index] || ""}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DatasetBarList({ rows }) {
  if (!rows.length) return <div className="empty">No rows are available for this section.</div>;
  return (
    <div className="dataset-bars">
      {rows.map((row) => {
        const max = Math.max(toNumber(row.max), toNumber(row.value), 1);
        const width = Math.max(0, Math.min(100, pctFromPart(row.value, max)));
        return (
          <div className="dataset-bar-row" key={row.label}>
            <div>
              <strong>{row.label}</strong>
              <span>{row.detail || formatInt(row.value)}</span>
            </div>
            <div className="dataset-bar-track">
              <div className="dataset-bar-fill" style={{ width: `${width}%`, background: row.color || undefined }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function MultiplicityTable({ values, total }) {
  const keys = ["none", "nss_only", "wds_only", "msc_only", "sbx_only", "nss_wds", "nss_msc", "nss_sbx", "wds_msc", "wds_sbx", "msc_sbx", "nss_wds_msc", "nss_wds_msc_sbx"];
  return (
    <table>
      <thead>
        <tr><th>Evidence pattern</th><th>Rows</th><th>Share</th></tr>
      </thead>
      <tbody>
        {keys.map((key) => (
          <tr key={key}>
            <td>{key}</td>
            <td>{formatInt(values[key])}</td>
            <td>{formatPct(pctFromPart(values[key], total))}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function sortedCatalogContributionRows(report) {
  const rows = Array.isArray(report.catalog_contributions) ? [...report.catalog_contributions] : [];
  return rows.sort((a, b) => {
    const utilityDelta = toNumber(b.utility_score) - toNumber(a.utility_score);
    if (utilityDelta !== 0) return utilityDelta;
    const directDelta = toNumber(b.direct_rows) - toNumber(a.direct_rows);
    if (directDelta !== 0) return directDelta;
    return String(a.catalog || "").localeCompare(String(b.catalog || ""));
  });
}

function catalogOverlapRows(report) {
  const starRows = report?.overlaps?.star_evidence?.pairwise || [];
  const systemRows = report?.overlaps?.system_evidence?.pairwise || [];
  return [
    ...starRows.map((row) => ({ ...row, scope: "stars" })),
    ...systemRows.map((row) => ({ ...row, scope: "systems" })),
  ];
}

function catalogStageSummary(stage, item) {
  if (!item || !Object.keys(item).length) return "no stage report";
  if (stage === "download") return `${formatInt(item.source_count)} sources | ${formatInt(item.manifest_files_count)} manifests`;
  if (stage === "cook") return `${formatInt(item.existing_catalog_count)} / ${formatInt(item.catalog_count)} cooked files`;
  return `build=${item.build_id || "n/a"} | entries=${formatInt(item.catalog_contribution_entries)}`;
}

function datasetDeterminismLabel(determinism) {
  const status = String(determinism?.status || "");
  if (status === "match") return "match";
  if (status === "mismatch") return "mismatch";
  if (status === "no_baseline") return "no baseline";
  if (status === "missing_current_report") return "missing report";
  return status || "unknown";
}

function spectralColor(value) {
  const key = String(value || "").toUpperCase();
  const colors = {
    O: "#5b7cfa",
    B: "#5ca4ff",
    A: "#74b8ff",
    F: "#f7d774",
    G: "#f59e0b",
    K: "#e87524",
    M: "#c2410c",
    L: "#8b5cf6",
    T: "#14b8a6",
    Y: "#64748b",
    D: "#94a3b8",
  };
  return colors[key] || "#64748b";
}

function AgencyScreen() {
  const [activeTab, setActiveTab] = useState("portfolios");
  const [state, setState] = useState({ loading: true, data: null, portfolios: null, selectedDetail: null, message: "Loading agency status..." });
  const [selectedDossierId, setSelectedDossierId] = useState("");

  async function loadAgency() {
    setState((current) => ({ ...current, loading: true, message: "Refreshing agency status..." }));
    const [statusResult, portfolioResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/agency/status`),
      fetchJson(`${ADMIN_API_BASE}/agency/portfolios?limit=100`),
    ]);
    const errors = [];
    if (!statusResult.response.ok) errors.push(`agency: ${compactError(statusResult.data, statusResult.response.status)}`);
    if (!portfolioResult.response.ok) errors.push(`portfolios: ${compactError(portfolioResult.data, portfolioResult.response.status)}`);
    const items = portfolioResult.response.ok && Array.isArray(portfolioResult.data.items) ? portfolioResult.data.items : [];
    const nextSelected = selectedDossierId || items[0]?.dossier_id || "";
    let selectedDetail = null;
    if (nextSelected) {
      selectedDetail = await loadPortfolioDetail(nextSelected, { silent: true });
    }
    if (nextSelected && !selectedDossierId) setSelectedDossierId(nextSelected);
    setState({
      loading: false,
      data: statusResult.response.ok ? statusResult.data : null,
      portfolios: portfolioResult.response.ok ? portfolioResult.data : { items: [], counts_by_status: {} },
      selectedDetail,
      message: errors.length ? errors.join(" | ") : "Ready",
    });
  }

  async function loadPortfolioDetail(dossierId, { silent = false } = {}) {
    if (!dossierId) return null;
    if (!silent) setState((current) => ({ ...current, message: `Loading ${compactId(dossierId, 28)}...` }));
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/agency/portfolios/${encodeURIComponent(dossierId)}`);
    if (!response.ok) {
      if (!silent) setState((current) => ({ ...current, message: `Portfolio detail: ${compactError(data, response.status)}` }));
      return null;
    }
    if (!silent) {
      setState((current) => ({ ...current, selectedDetail: data, message: "Ready" }));
    }
    return data;
  }

  async function selectPortfolio(dossierId) {
    setSelectedDossierId(dossierId);
    await loadPortfolioDetail(dossierId);
  }

  useEffect(() => {
    loadAgency();
  }, []);

  const data = state.data || {};
  const portfolios = state.portfolios || { items: [], counts_by_status: {} };
  const portfolioItems = Array.isArray(portfolios.items) ? portfolios.items : [];
  const readiness = data.readiness || {};
  const evalReports = data.eval_reports || {};
  const liveCounts = readiness.live_counts || {};
  const anomalies = Array.isArray(evalReports.anomaly_inbox) ? evalReports.anomaly_inbox : [];
  const reports = Array.isArray(evalReports.reports) ? evalReports.reports : [];
  const kpis = [
    { label: "Portfolio rows", value: formatInt(liveCounts.object_dossiers), tone: liveCounts.object_dossiers ? "ok" : "warn" },
    { label: "Source files", value: formatInt(liveCounts.source_documents), tone: liveCounts.source_documents ? "ok" : "warn" },
    { label: "Findings", value: formatInt(liveCounts.extracted_claims), tone: liveCounts.extracted_claims ? "ok" : "warn" },
    { label: "Journal entries", value: formatInt(liveCounts.portfolio_journal_entries), tone: liveCounts.portfolio_journal_entries ? "ok" : "warn" },
  ];

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Agency</h1>
          <p className="muted">Evidence portfolios, agent workflow readiness, quarantined anomalies, and the future conversation workbench.</p>
        </div>
        <button className="button" onClick={loadAgency}>{state.loading ? "Refreshing..." : "Refresh"}</button>
      </header>

      <div className="kpi-row">
        {kpis.map((item) => (
          <div className={`kpi ${item.tone || ""}`} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>

      <div className={state.message === "Ready" ? "status-line" : "status-line danger-line"}>
        {state.message} Portfolio persistence: {readiness.portfolio_persistence_ready ? "ready" : "not live"} | Generated: {formatDate(data.generated_at_utc)}
      </div>

      <section className="agency-grid">
        <div className="panel">
          <h2>Evidence Portfolio Model</h2>
          <MetricList
            rows={[
              ["Hot rows", data.storage_model?.hot_layer || "admin operational dossier, source, extraction, finding, and journal rows"],
              ["Disc materialization", data.storage_model?.disc_materialization || "future public citation/factsheet/exposition surfaces"],
              ["Proposal layer", data.storage_model?.proposal_layer || "arm proposal and accepted overlay rows"],
              ["Cold archive", data.storage_model?.cold_archive || "/mnt/space/spacegate/agent_archive"],
              ["Core policy", data.storage_model?.core_policy || "agents never write directly to core"],
            ]}
          />
        </div>
        <div className="panel">
          <h2>Readiness</h2>
          <MetricList
            rows={[
              ["Persistence", readiness.portfolio_persistence_ready ? "ready" : "not live"],
              ["Implemented admin tables", (readiness.implemented_admin_tables || []).join(", ") || "none"],
              ["Missing admin tables", (readiness.missing_admin_tables || []).join(", ") || "none"],
              ["Missing disc materialization", (readiness.missing_disc_tables || []).join(", ") || "none"],
              ["Eval reports", `${formatInt(evalReports.report_count)} reports found`],
            ]}
          />
          {Array.isArray(readiness.notes) && readiness.notes.length ? (
            <div className="hint-list agency-notes">
              {readiness.notes.map((note) => <div key={note}>{note}</div>)}
            </div>
          ) : null}
        </div>
      </section>

      <div className="tab-row">
        {[
          ["portfolios", "Portfolios"],
          ["flow", "Portfolio Flow"],
          ["anomalies", "Anomaly Inbox"],
          ["reports", "Eval Reports"],
          ["storage", "Storage Readiness"],
          ["interaction", "Agent Interaction"],
        ].map(([key, label]) => (
          <button className={activeTab === key ? "active" : ""} key={key} onClick={() => setActiveTab(key)}>
            {label}
          </button>
        ))}
      </div>

      {activeTab === "portfolios" ? (
        <AgencyPortfoliosTab
          portfolios={portfolioItems}
          countsByStatus={portfolios.counts_by_status || {}}
          selectedDossierId={selectedDossierId}
          selectedDetail={state.selectedDetail}
          selectPortfolio={selectPortfolio}
        />
      ) : activeTab === "flow" ? (
        <AgencyPortfolioFlowTab stages={data.workflow_stages || []} />
      ) : activeTab === "anomalies" ? (
        <AgencyAnomalyTab anomalies={anomalies} />
      ) : activeTab === "reports" ? (
        <AgencyReportsTab reports={reports} searchedDirs={evalReports.searched_dirs || []} />
      ) : activeTab === "storage" ? (
        <AgencyStorageTab data={data} />
      ) : (
        <AgencyInteractionTab model={data.interaction_model || {}} />
      )}
    </div>
  );
}

function AgencyPortfoliosTab({ portfolios, countsByStatus, selectedDossierId, selectedDetail, selectPortfolio }) {
  return (
    <section className="agency-portfolio-layout">
      <div className="panel">
        <div className="panel-head">
          <div>
            <h2>Evidence Portfolios</h2>
            <p className="muted">Operational dossier rows in the admin database. Public disc materialization is a separate later step.</p>
          </div>
          <span className="badge">{formatInt(portfolios.length)} shown</span>
        </div>
        <div className="report-chip-list">
          {Object.entries(countsByStatus).map(([status, count]) => (
            <span className="badge muted" key={status}>{status}: {formatInt(count)}</span>
          ))}
        </div>
        {portfolios.length ? (
          <table className="select-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Target</th>
                <th>Sources</th>
                <th>Findings</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {portfolios.map((item) => (
                <tr className={selectedDossierId === item.dossier_id ? "selected" : ""} key={item.dossier_id}>
                  <td><button className="link-button" onClick={() => selectPortfolio(item.dossier_id)}>{item.dossier_status}</button></td>
                  <td>
                    <strong>{item.display_name || item.stable_object_key}</strong>
                    <span className="table-subtext">{item.object_type} | {item.stable_object_key}</span>
                  </td>
                  <td>{formatInt(item.source_count)}</td>
                  <td>{formatInt(item.claim_count)}</td>
                  <td>{formatDate(item.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No Evidence Portfolio rows exist yet. The schema is ready; the next implementation step is controlled creation from coolness/adjudication queues or an operator-selected target.</div>
        )}
      </div>
      <AgencyPortfolioDetail detail={selectedDetail} />
    </section>
  );
}

function AgencyPortfolioDetail({ detail }) {
  if (!detail?.dossier) {
    return (
      <div className="panel agency-detail">
        <h2>Selected Portfolio</h2>
        <div className="empty">Select a portfolio to inspect source files, extraction sets, findings, and journal entries.</div>
      </div>
    );
  }
  const dossier = detail.dossier;
  const sources = detail.source_documents || [];
  const bundles = detail.claim_bundles || [];
  const claims = detail.extracted_claims || [];
  const journal = detail.journal_entries || [];
  return (
    <div className="panel agency-detail">
      <div className="panel-head">
        <div>
          <h2>{dossier.display_name || dossier.stable_object_key}</h2>
          <p className="muted">{dossier.dossier_id}</p>
        </div>
        <span className="badge warn">{dossier.dossier_status}</span>
      </div>
      <MetricList
        rows={[
          ["Object", `${dossier.object_type} | ${dossier.stable_object_key}`],
          ["Queue", `${dossier.queue_priority || "n/a"} | ${dossier.queue_reason || "n/a"}`],
          ["Review / publication", `${dossier.review_state} / ${dossier.publication_state}`],
          ["Freshness", dossier.freshness_state || "n/a"],
          ["Updated", formatDate(dossier.updated_at)],
        ]}
      />
      <div className="agency-detail-sections">
        <details open>
          <summary>Source Files ({formatInt(sources.length)})</summary>
          <CompactAgencyTable
            emptyText="No source files are attached."
            rows={sources.map((item) => [item.source_domain, item.title || item.canonical_url, item.retrieval_status, formatDate(item.accessed_at)])}
            columns={["Domain", "Title / URL", "Status", "Accessed"]}
          />
        </details>
        <details>
          <summary>Extraction Sets ({formatInt(bundles.length)})</summary>
          <CompactAgencyTable
            emptyText="No extraction sets are attached."
            rows={bundles.map((item) => [item.bundle_kind, item.model_id || item.extraction_method || "n/a", item.status, formatDate(item.created_at)])}
            columns={["Kind", "Model / method", "Status", "Created"]}
          />
        </details>
        <details>
          <summary>Findings ({formatInt(claims.length)})</summary>
          <CompactAgencyTable
            emptyText="No findings are attached."
            rows={claims.map((item) => [item.review_status, item.predicate, item.subject_label || item.subject_stable_key || "n/a", item.confidence ?? "n/a"])}
            columns={["Review", "Predicate", "Subject", "Confidence"]}
          />
        </details>
        <details>
          <summary>Journal ({formatInt(journal.length)})</summary>
          <div className="timeline-list">
            {journal.length ? journal.map((item) => (
              <div className="timeline-row" key={item.journal_entry_id}>
                <strong>{item.title}</strong>
                <span>{formatDate(item.created_at)} | {item.actor_type} | {item.stage} | {item.outcome}</span>
                <p>{item.narrative}</p>
              </div>
            )) : <div className="empty">No journal entries are attached.</div>}
          </div>
        </details>
      </div>
    </div>
  );
}

function CompactAgencyTable({ rows, columns, emptyText }) {
  if (!rows.length) return <div className="empty">{emptyText}</div>;
  return <KeyValueTable rows={rows} columns={columns} />;
}

function AgencyPortfolioFlowTab({ stages }) {
  const fallback = [
    { key: "seeded", title: "Seeded", description: "Queued target object.", predecessor: null, successor: "gathering" },
    { key: "gathering", title: "Gathering", description: "Collect source files.", predecessor: "seeded", successor: "extracted" },
    { key: "extracted", title: "Extracted", description: "Findings exist.", predecessor: "gathering", successor: "review_ready" },
    { key: "review_ready", title: "Review Ready", description: "Proposals need verdicts.", predecessor: "extracted", successor: "published" },
  ];
  const rows = stages.length ? stages : fallback;
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>Portfolio Flow</h2>
          <p className="muted">Sequential states for object-level evidence cases. Blocked can happen from any state when source, schema, identity, or review prerequisites are missing.</p>
        </div>
      </div>
      <div className="agency-stage-grid">
        {rows.map((stage, index) => (
          <div className="agency-stage" key={stage.key || stage.title}>
            <div className="agency-stage-index">{index + 1}</div>
            <div>
              <h3>{stage.title || actionLabel(stage.key)}</h3>
              <p>{stage.description}</p>
              <div className="action-meta">
                <span className="badge muted">from {stage.predecessor || "start/any"}</span>
                <span className="badge muted">next {stage.successor || "operator decision"}</span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function AgencyAnomalyTab({ anomalies }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>Anomaly Inbox</h2>
          <p className="muted">Quarantined eval findings. These are review signals, not accepted science or public claims.</p>
        </div>
        <span className="badge warn">{formatInt(anomalies.length)} shown</span>
      </div>
      {anomalies.length ? (
        <table>
          <thead>
            <tr>
              <th>Severity</th>
              <th>Type</th>
              <th>Subject</th>
              <th>Case / model</th>
              <th>Summary</th>
            </tr>
          </thead>
          <tbody>
            {anomalies.map((item, index) => (
              <tr key={`${item.report_id}-${item.case_id}-${index}`}>
                <td><span className={`badge ${item.severity === "high" ? "danger" : item.severity === "medium" ? "warn" : "muted"}`}>{item.severity || "n/a"}</span></td>
                <td>{item.anomaly_type || "n/a"}</td>
                <td>{item.subject || "n/a"}</td>
                <td>
                  <strong>{item.case_id || "n/a"}</strong>
                  <span className="table-subtext">{item.provider || "?"} / {item.model_id || "?"}</span>
                </td>
                <td>
                  {item.summary || ""}
                  {item.recommended_next_action ? <span className="table-subtext">next: {item.recommended_next_action}</span> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty">No anomaly inbox items were found in the searched eval reports. That can mean no reports are mounted into the runtime, or no run emitted quarantined anomalies.</div>
      )}
    </section>
  );
}

function AgencyReportsTab({ reports, searchedDirs }) {
  return (
    <div className="runbook-grid">
      <section className="panel">
        <h2>Eval Report Locations</h2>
        <KeyValueTable rows={(searchedDirs || []).map((item) => [item.path, item.exists ? "found" : "missing"])} columns={["Path", "Status"]} />
      </section>
      <section className="panel">
        <h2>Latest Eval Reports</h2>
        {reports.length ? (
          <table>
            <thead>
              <tr>
                <th>Report</th>
                <th>Provider / model</th>
                <th>Score</th>
                <th>Cases</th>
                <th>Anomalies</th>
              </tr>
            </thead>
            <tbody>
              {reports.map((report) => (
                <tr key={report.report_id}>
                  <td>
                    <strong>{report.report_id}</strong>
                    <span className="table-subtext">{formatDate(report.created_at || report.mtime_utc)}</span>
                  </td>
                  <td>
                    {report.provider || "n/a"} / {report.model_id || "n/a"}
                    <span className="table-subtext">{(report.roles || []).join(", ") || "roles n/a"}</span>
                  </td>
                  <td>{formatFloat(toNumber(report.mean_score, NaN), 3)}</td>
                  <td>{formatInt(report.case_count)}</td>
                  <td>{formatInt(report.anomaly_count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No eval report JSON files were found in the runtime report locations.</div>
        )}
      </section>
    </div>
  );
}

function AgencyStorageTab({ data }) {
  const adminStore = data.admin_store || {};
  const disc = data.disc || {};
  const arm = data.arm || {};
  return (
    <section className="agency-grid">
      <div className="panel">
        <h2>Admin Portfolio Store</h2>
        <AgencyTableReadiness expected={adminStore.expected || {}} />
      </div>
      <div className="panel">
        <h2>Disc Materialization Tables</h2>
        <AgencyTableReadiness expected={disc.expected || {}} />
      </div>
      <div className="panel">
        <h2>Arm Evidence Signals</h2>
        <AgencyTableReadiness expected={arm.expected || {}} />
      </div>
      <div className="panel">
        <h2>Runtime Tables</h2>
        <p className="muted">Admin tables are mutable operator state. Disc tables are served build artifacts and should be materialized deliberately.</p>
        <div className="report-chip-list">
          {(adminStore.tables || []).filter((name) => name.startsWith("agent_")).map((name) => <span className="badge ok" key={name}>{name}</span>)}
          {(disc.tables || []).map((name) => <span className="badge" key={name}>{name}</span>)}
        </div>
        {disc.error ? <p className="notice">{disc.error}</p> : null}
      </div>
      <div className="panel">
        <h2>Paths</h2>
        <MetricList rows={Object.entries(data.paths || {}).map(([key, value]) => [key, String(value || "n/a")])} />
      </div>
    </section>
  );
}

function AgencyTableReadiness({ expected }) {
  const rows = Object.entries(expected || {});
  if (!rows.length) return <div className="empty">No table readiness data is available.</div>;
  return (
    <table>
      <thead>
        <tr><th>Table</th><th>Status</th><th>Rows</th></tr>
      </thead>
      <tbody>
        {rows.map(([name, item]) => (
          <tr key={name}>
            <td>{name}</td>
            <td><span className={`badge ${item.exists ? "ok" : "warn"}`}>{item.exists ? "present" : "missing"}</span></td>
            <td>{item.count === null || item.count === undefined ? "n/a" : formatInt(item.count)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function AgencyInteractionTab({ model }) {
  const features = Array.isArray(model.minimum_features) ? model.minimum_features : [];
  const sidecars = Array.isArray(model.possible_sidecars) ? model.possible_sidecars : [];
  return (
    <section className="agency-grid">
      <div className="panel">
        <h2>Recommendation</h2>
        <MetricList
          rows={[
            ["Approach", model.recommended || "Build a Spacegate-native portfolio conversation workbench."],
            ["Reason", model.why || "The agent conversation needs source, claim, proposal, review, journal, and layer context."],
            ["Default mode", "read-only Q&A against selected portfolio context"],
            ["Mutation path", "explicit proposal or review action, never freeform core edits"],
          ]}
        />
      </div>
      <div className="panel">
        <h2>Minimum Workbench Features</h2>
        <div className="hint-list">
          {features.map((feature) => <div key={feature}>{feature}</div>)}
        </div>
      </div>
      <div className="panel">
        <h2>Free Sidecar Candidates</h2>
        <p className="muted">Useful references or optional local chat clients, but not a replacement for Spacegate’s evidence and review state.</p>
        <div className="report-chip-list">
          {sidecars.map((item) => <span className="badge" key={item}>{item}</span>)}
        </div>
      </div>
      <div className="panel">
        <h2>Context Boundary</h2>
        <div className="trap-list">
          <div>Every chat turn should name the portfolio, selected sources, selected findings, model id, prompt version, and token budget.</div>
          <div>Agent answers should cite source/finding IDs or say that evidence is missing.</div>
          <div>Useful exchanges should become journal entries so a human or later LLM can follow the evidence trail.</div>
        </div>
      </div>
    </section>
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
  const [opsStatus, setOpsStatus] = useState(null);
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

  async function loadOpsStatus() {
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/operations/status`);
    if (!response.ok) {
      setStatus(`Operations status: ${compactError(data, response.status)}`);
      return null;
    }
    setOpsStatus(data);
    return data;
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
    await Promise.all([loadOpsStatus(), loadActions(), loadJobs(), loadBackups(), loadAudit({ append: false })]);
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

  const activeJobs = opsStatus?.jobs?.active || jobs.filter((job) => ["queued", "running"].includes(String(job.status || "")));
  const failedJobs = opsStatus?.jobs?.latest_failures || jobs.filter((job) => String(job.status || "") === "failed");
  const latestAdminBackup = opsStatus?.backups?.latest_admin_db?.mtime_utc || opsStatus?.backups?.latest_admin_db?.created_at || backups.admin_db?.[0]?.mtime_utc || backups.admin_db?.[0]?.created_at;
  const latestReleaseBackup = opsStatus?.backups?.latest_release_metadata?.mtime_utc || opsStatus?.backups?.latest_release_metadata?.created_at || backups.release_metadata?.[0]?.mtime_utc || backups.release_metadata?.[0]?.created_at;
  const kpis = [
    { label: "Active jobs", value: activeJobs.length, tone: activeJobs.length ? "warn" : "ok" },
    { label: "Recent failures", value: failedJobs.length, tone: failedJobs.length ? "danger" : "ok" },
    { label: "Admin DB backups", value: formatInt(opsStatus?.backups?.admin_db_count ?? backups.admin_db?.length ?? 0) },
    { label: "Release backups", value: formatInt(opsStatus?.backups?.release_metadata_count ?? backups.release_metadata?.length ?? 0) },
  ];
  const groups = opsStatus?.action_groups || fallbackActionGroups;

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
        <RunbookTab actionGroups={groups} actionsByName={actionsByName} runAction={runAction} busyAction={busyAction} />
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

function RunbookTab({ actionGroups, actionsByName, runAction, busyAction }) {
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
  const guidance = action.operator_guidance || fallbackActionGuidance[action.name] || {};
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
        {guidance.writes_to ? <div><strong>Writes:</strong> {guidance.writes_to}</div> : null}
        {guidance.writes ? <div><strong>Writes:</strong> {guidance.writes}</div> : null}
        {Array.isArray(guidance.outputs) && guidance.outputs.length ? <div><strong>Outputs:</strong> {guidance.outputs.join(", ")}</div> : null}
        {Array.isArray(guidance.success_next_actions) && guidance.success_next_actions.length ? <div><strong>Next:</strong> {guidance.success_next_actions.join(" ")}</div> : null}
        {guidance.next ? <div><strong>Next:</strong> {guidance.next}</div> : null}
        {guidance.expected_duration ? <div><strong>Expected:</strong> {String(guidance.expected_duration).replaceAll("_", " ")}</div> : null}
        {guidance.duration ? <div><strong>Expected:</strong> {guidance.duration}</div> : null}
        {Array.isArray(guidance.warnings) && guidance.warnings.length ? <div className="warning-text"><strong>Warning:</strong> {guidance.warnings.join(" ")}</div> : null}
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
