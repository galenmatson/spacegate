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
const inferenceRoles = ["discover", "prune", "compile", "identify", "extract", "criticize", "adjudicate", "narrate"];
const defaultSmokePrompt = "Spacegate inference smoke test. Reply with exactly: spacegate inference smoke ok";
const emptyAuditFilters = { event_type: "", result: "", request_id: "", actor_user_id: "", correlation_id: "" };
const coolnessWeightDefaults = {
  luminosity: 0.22,
  proper_motion: 0.10,
  multiplicity: 0.14,
  nice_planets: 0.12,
  weird_planets: 0.14,
  proximity: 0.08,
  system_complexity: 0.12,
  exotic_star: 0.08,
};
const coolnessWeightMeta = [
  ["luminosity", "Luminosity", "Bright, rare spectral classes and visually prominent stellar hosts."],
  ["proper_motion", "Proper Motion", "Nearby high-motion systems with strong apparent movement."],
  ["multiplicity", "Multiplicity", "Binary and multi-star architecture."],
  ["nice_planets", "Nice Planets", "Temperate or otherwise inviting planet candidates."],
  ["weird_planets", "Weird Planets", "Unusual, extreme, or dynamically interesting planets."],
  ["proximity", "Proximity", "Nearby systems that are easier to explain and map."],
  ["system_complexity", "System Complexity", "Richer systems with more objects and structure."],
  ["exotic_star", "Exotic Star", "Remnants, giants, peculiar stars, and other rare stellar traits."],
];
const defaultCoolnessSliders = Object.fromEntries(
  coolnessWeightMeta.map(([key]) => [
    key,
    Math.max(1, Math.min(10, Math.round((coolnessWeightDefaults[key] / Math.max(...Object.values(coolnessWeightDefaults))) * 10))),
  ])
);

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
  retention_dry_run: {
    group: "build",
    purpose: "Runs the retention script in dry-run mode and logs exactly which build, report, and tmp paths would be pruned.",
    prerequisites: "Use after verification and after temporary outputs have been reviewed.",
    writes: "Admin job log only; it never passes --apply.",
    next: "Review the log and candidate list before any manual cleanup.",
    warning: "Read-only. Apply requires a separate high-risk action.",
    duration: "Short",
  },
  retention_apply: {
    group: "build",
    purpose: "Deletes the exact retention candidate directories from a matching recent dry-run.",
    prerequisites: "Run Retention Dry Run first, then confirm the candidate hash and paths have not changed.",
    writes: "Only stale out/ and reports/ candidate directories. raw/, cooked/, and served/current are protected.",
    next: "Refresh Builds and Runtime storage after completion.",
    warning: "High-risk deletion action. Requires confirmation and a matching dry-run candidate hash.",
    duration: "Short",
  },
  score_coolness: {
    group: "presentation",
    purpose: "Generates deterministic disc coolness ranking and scoring reports for a build.",
    prerequisites: "Use after a valid build exists. Ephemeral scoring is useful for experiments.",
    writes: "Disc scoring artifacts and reports. Ephemeral mode avoids creating or mutating stored profile versions.",
    next: "Generate snapshots for the new ranking. Save and activate a profile if the result is worth preserving.",
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
    actions: ["build_database", "verify_build", "publish_db", "retention_dry_run", "retention_apply"],
    sequence: ["Build Database", "Verify Build", "Publish Database", "Retention after verified promotion"],
  },
  {
    key: "presentation",
    title: "Presentation Generation",
    description: "Generate ranking and snapshot artifacts without changing canonical science rows.",
    actions: ["score_coolness", "generate_snapshots", "save_coolness_profile", "apply_coolness_profile"],
    sequence: ["Score Coolness", "Generate Snapshots", "Save Profile", "Activate Profile"],
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

async function fetchText(path, options = {}) {
  const response = await fetch(path, {
    credentials: "include",
    ...options,
    headers: {
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  return { response, text };
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
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatDateCompact(value) {
  if (!value) return "n/a";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
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

async function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "readonly");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand("copy");
  document.body.removeChild(textarea);
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

function runtimeStatusTone(status) {
  const value = String(status || "");
  if (value === "configured" || value === "alias_satisfied" || value === "ok" || value === "loaded") return "ok";
  if (value === "optional_missing" || value === "missing") return "muted";
  if (value === "warning" || value === "unknown") return "warn";
  if (value === "unreadable" || value === "error") return "danger";
  return "";
}

function runtimeStatusLabel(status) {
  const value = String(status || "unknown");
  if (value === "alias_satisfied") return "alias satisfied";
  if (value === "optional_missing") return "optional";
  return value;
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
  const jobLogId = new URLSearchParams(window.location.search).get("job_log");

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
    const nextPath = `${window.location.pathname}${window.location.search || ""}`;
    return (
      <div className="boot">
        <h1>Spacegate Admin</h1>
        <a className="button primary" href={`${AUTH_API_BASE}/login/google?next=${encodeURIComponent(nextPath || "/admin/")}`}>
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
          <button className={activeScreen === "runtime" ? "active" : ""} onClick={() => setActiveScreen("runtime")}>Runtime</button>
        </nav>
        <button className="button" onClick={logout}>Log out</button>
      </aside>
      <main className="workspace">
        {jobLogId ? (
          <FullJobLogScreen jobId={jobLogId} />
        ) : activeScreen === "overview" ? (
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
          <AgencyScreen csrf={csrf} />
        ) : activeScreen === "runtime" ? (
          <RuntimeScreen />
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
    buildStatus: null,
    operations: null,
    actions: [],
    message: "Loading build state...",
  });
  const [busyAction, setBusyAction] = useState("");
  const headers = useMemo(() => buildCsrfHeaders(csrf), [csrf]);

  const actionsByName = useMemo(() => new Map(state.actions.map((item) => [item.name, item])), [state.actions]);

  async function loadBuilds() {
    setState((current) => ({ ...current, loading: true, message: "Refreshing build state..." }));
    const [statusResult, datasetResult, buildStatusResult, operationsResult, actionsResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/status`),
      fetchJson(`${ADMIN_API_BASE}/status/dataset`),
      fetchJson(`${ADMIN_API_BASE}/builds/status`),
      fetchJson(`${ADMIN_API_BASE}/operations/status`),
      fetchJson(`${ADMIN_API_BASE}/actions/catalog`),
    ]);
    const errors = [];
    if (!statusResult.response.ok) errors.push(`status: ${compactError(statusResult.data, statusResult.response.status)}`);
    if (!datasetResult.response.ok) errors.push(`dataset: ${compactError(datasetResult.data, datasetResult.response.status)}`);
    if (!buildStatusResult.response.ok) errors.push(`builds: ${compactError(buildStatusResult.data, buildStatusResult.response.status)}`);
    if (!operationsResult.response.ok) errors.push(`operations: ${compactError(operationsResult.data, operationsResult.response.status)}`);
    if (!actionsResult.response.ok) errors.push(`actions: ${compactError(actionsResult.data, actionsResult.response.status)}`);
    setState({
      loading: false,
      status: statusResult.response.ok ? statusResult.data : null,
      dataset: datasetResult.response.ok ? datasetResult.data : null,
      buildStatus: buildStatusResult.response.ok ? buildStatusResult.data : null,
      operations: operationsResult.response.ok ? operationsResult.data : null,
      actions: actionsResult.response.ok && Array.isArray(actionsResult.data.items) ? actionsResult.data.items : [],
      message: errors.length ? errors.join(" | ") : "Ready",
    });
  }

  useEffect(() => {
    loadBuilds();
  }, []);

  async function waitForJobTerminal(jobId, timeoutMs = 90000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}`);
      if (!response.ok) return null;
      const job = data.job || data;
      const status = String(job.status || "");
      if (terminalJobStatuses.has(status)) return job;
      await new Promise((resolve) => setTimeout(resolve, 1500));
    }
    return null;
  }

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
    if (job.job_id && ["retention_dry_run", "retention_apply"].includes(actionName)) {
      setState((current) => ({ ...current, message: `Queued ${actionLabel(actionName)} as ${job.job_id}. Waiting for completion...` }));
      const finished = await waitForJobTerminal(job.job_id);
      const finalStatus = finished?.status || "queued";
      setState((current) => ({ ...current, message: `${actionLabel(actionName)} ${finalStatus}. Refreshing build state...` }));
    }
    await loadBuilds();
    return { ok: true, job };
  }

  const dataset = state.dataset || {};
  const buildStatus = state.buildStatus || {};
  const operations = state.operations || {};
  const sizes = dataset.sizes_bytes || {};
  const counts = dataset.dataset_counts || {};
  const disk = dataset.disk || {};
  const legacyBuilds = operations.builds || {};
  const served = buildStatus.served_current || legacyBuilds.served_current || {};
  const retention = buildStatus.retention || operations.retention || {};
  const recentBuilds = Array.isArray(buildStatus.recent) ? buildStatus.recent : Array.isArray(legacyBuilds.recent) ? legacyBuilds.recent : [];
  const tmpBuilds = Array.isArray(buildStatus.tmp) ? buildStatus.tmp : Array.isArray(legacyBuilds.tmp) ? legacyBuilds.tmp : [];
  const currentBuild = buildStatus.current_build || recentBuilds.find((item) => item.build_id === served.build_id) || recentBuilds[0] || null;
  const verification = currentBuild?.verification || {};
  const snapshot = currentBuild?.snapshot || {};
  const pathHealth = buildStatus.path_health || {};
  const nextActions = Array.isArray(buildStatus.next_actions) ? buildStatus.next_actions : [];
  const retentionPlan = retention.dry_run || {};
  const retentionContext = {
    plan: retentionPlan,
    latestDryRun: retention.latest_matching_dry_run || null,
    applyReady: Boolean(retention.apply_ready),
  };
  const buildActions = ["build_database", "verify_build", "publish_db", "retention_dry_run", "retention_apply"]
    .map((name) => enrichBuildAction(actionsByName.get(name), retentionContext))
    .filter(Boolean);
  const kpis = [
    { label: "Served build", value: compactId(state.status?.build_id || served.build_id, 20) },
    { label: "Verification", value: readableStatus(verification.status || "unknown"), tone: statusTone(verification.status) },
    { label: "Snapshots", value: readableStatus(snapshot.status || "missing"), tone: statusTone(snapshot.status) },
    { label: "Temp outputs", value: formatInt(tmpBuilds.length), tone: tmpBuilds.length ? "warn" : "ok" },
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
          <OverviewFact label="Build dirs / report dirs" value={`${formatInt(buildStatus.out_count || recentBuilds.length)} / ${formatInt(buildStatus.report_build_count)}`} />
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

      <section className="builds-grid">
        <NextActionsPanel actions={nextActions} />
        <PathHealthPanel paths={pathHealth} />
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
        <BuildVerificationPanel build={currentBuild} />
      </section>

      <section className="builds-grid">
        <SnapshotReportPanel build={currentBuild} />
        <RecentBuildsPanel builds={recentBuilds} servedBuildId={served.build_id || state.status?.build_id} />
      </section>

      <section className="builds-grid">
        <TempBuildsPanel builds={tmpBuilds} />
        <RetentionPlanPanel retention={retention} />
      </section>

      <BuildReportsPanel build={currentBuild} />
    </div>
  );
}

function readableStatus(value) {
  return String(value || "unknown").replaceAll("_", " ");
}

function statusTone(value) {
  const status = String(value || "").toLowerCase();
  if (["ok", "passed", "passed_reports", "generated", "reused", "present", "ready"].includes(status)) return "ok";
  if (["failed", "error", "parse_error", "missing_reports"].includes(status)) return "danger";
  if (["unknown", "", "null_result"].includes(status)) return "muted";
  return "warn";
}

function priorityTone(value) {
  const priority = String(value || "").toLowerCase();
  if (priority === "high") return "danger";
  if (priority === "low") return "muted";
  return "warn";
}

function retentionCandidateSummary(plan) {
  const candidates = plan?.candidates || {};
  const builds = Array.isArray(candidates.builds) ? candidates.builds.length : 0;
  const tmp = Array.isArray(candidates.tmp) ? candidates.tmp.length : 0;
  const reports = Array.isArray(candidates.reports) ? candidates.reports.length : 0;
  return `${formatInt(builds)} build dirs, ${formatInt(tmp)} temp outputs, ${formatInt(reports)} report dirs; ${formatBytes(plan?.estimated_reclaimable_bytes)} reclaimable`;
}

function enrichBuildAction(action, retentionContext) {
  if (!action) return null;
  if (!["retention_dry_run", "retention_apply"].includes(action.name)) return action;
  const retentionPlan = retentionContext?.plan || {};
  const schema = { ...(action.params_schema || {}) };
  if (schema.keep_builds) schema.keep_builds = { ...schema.keep_builds, default: retentionPlan.keep_builds ?? schema.keep_builds.default };
  if (schema.keep_reports) schema.keep_reports = { ...schema.keep_reports, default: retentionPlan.keep_reports ?? schema.keep_reports.default };
  if (schema.skip_tmp) schema.skip_tmp = { ...schema.skip_tmp, default: retentionPlan.prune_tmp === false };
  if (schema.candidate_hash) schema.candidate_hash = { ...schema.candidate_hash, default: retentionPlan.candidate_hash || "" };
  const guidance = { ...(action.operator_guidance || fallbackActionGuidance[action.name] || {}) };
  if (action.name === "retention_dry_run") {
    guidance.outputs = [
      `Current plan: ${retentionCandidateSummary(retentionPlan)}`,
      `Candidate hash: ${compactId(retentionPlan.candidate_hash, 18)}`,
    ];
  }
  if (action.name === "retention_apply") {
    const dryRun = retentionContext?.latestDryRun;
    const ready = Boolean(retentionContext?.applyReady);
    guidance.prerequisites = dryRun
      ? `Satisfied by dry-run job ${dryRun.job_id} (${formatDate(dryRun.finished_at)}).`
      : "Run Retention Dry Run first. Apply is blocked until a matching successful dry-run exists.";
    guidance.outputs = [
      `Will delete: ${retentionCandidateSummary(retentionPlan)}`,
      `Candidate hash: ${compactId(retentionPlan.candidate_hash, 18)}`,
    ];
    guidance.warning = ready
      ? "High-risk deletion action. Type the confirmation phrase only after reviewing the candidate list and dry-run log."
      : "Apply will fail until the matching dry-run requirement is satisfied.";
  }
  const disabledReason = action.name === "retention_apply" && !retentionContext?.applyReady
    ? "Run Retention Dry Run and wait for it to succeed before applying retention."
    : "";
  return { ...action, params_schema: schema, operator_guidance: guidance, disabled_reason: disabledReason };
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
          <OverviewFact label="Verification" value={readableStatus(build.verification?.status)} />
          <OverviewFact label="Snapshots" value={readableStatus(build.snapshot?.status)} />
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

function NextActionsPanel({ actions }) {
  return (
    <div className="panel">
      <h2>Recommended Next Actions</h2>
      {actions.length ? (
        <div className="alert-list">
          {actions.map((item, index) => (
            <div className={`alert-row ${priorityTone(item.priority)}`} key={`${item.title}-${index}`}>
              <span className={`badge ${priorityTone(item.priority)}`}>{item.priority || "normal"}</span>
              <div>
                <strong>{item.title}</strong>
                <span className="table-subtext">{item.detail}</span>
                <span className="table-subtext">{item.action}</span>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty">No build next-action guidance is available yet.</div>
      )}
    </div>
  );
}

function PathHealthPanel({ paths }) {
  const rows = Object.entries(paths || {});
  return (
    <div className="panel">
      <h2>Pipeline Path Health</h2>
      <p className="muted">Container-visible state paths used by download, cook, ingest, reports, and served/current promotion.</p>
      {rows.length ? (
        <table>
          <thead>
            <tr><th>Target</th><th>Status</th><th>Access</th><th>Path</th></tr>
          </thead>
          <tbody>
            {rows.map(([key, item]) => (
              <tr key={key}>
                <td>{item.label || key}</td>
                <td><span className={`badge ${statusTone(item.status)}`}>{readableStatus(item.status)}</span></td>
                <td>
                  {item.exists ? `${item.readable ? "read" : "no read"} / ${item.writable ? "write" : "no write"}` : "missing"}
                  {(item.issues || []).map((issue) => <span className="table-subtext warning-text" key={issue}>{issue}</span>)}
                </td>
                <td>{item.path}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty">Path health has not been reported by the API.</div>
      )}
    </div>
  );
}

function BuildVerificationPanel({ build }) {
  const verification = build?.verification || {};
  const required = verification.required_reports || [];
  const supplemental = verification.supplemental_reports || [];
  const duplicate = verification.checks?.duplicate_near_pair_totals || {};
  const counts = verification.checks?.counts || {};
  return (
    <div className="panel">
      <h2>Verification Gates</h2>
      {build ? (
        <>
          <OverviewFact label="Status" value={<span className={`badge ${statusTone(verification.status)}`}>{readableStatus(verification.status)}</span>} />
          <OverviewFact label="QC counts" value={`${formatInt(counts.systems)} systems / ${formatInt(counts.stars)} stars / ${formatInt(counts.planets)} planets`} />
          <OverviewFact label="Distance invariant violations" value={String(verification.checks?.dist_invariant_violations ?? "n/a")} />
          <OverviewFact label="Duplicate near pairs" value={`${formatInt(duplicate.candidate_pairs)} candidate / ${formatInt(duplicate.likely_duplicate_pairs)} likely / ${formatInt(duplicate.high_confidence_pairs)} high-confidence`} />
          <div className="artifact-flags">
            {required.map((item) => <span className={`badge ${item.exists ? "ok" : "danger"}`} key={item.name}>{item.name}</span>)}
            {supplemental.map((item) => <span className={`badge ${item.exists ? "ok" : "muted"}`} key={item.name}>{item.name}</span>)}
          </div>
          {verification.issues?.length ? (
            <div className="trap-list">
              {verification.issues.map((issue) => <div key={issue}>{issue}</div>)}
            </div>
          ) : null}
          {verification.warnings?.length ? (
            <div className="status-line">
              {verification.warnings.map((warning) => <span className="table-subtext" key={warning}>{warning}</span>)}
            </div>
          ) : null}
        </>
      ) : (
        <div className="empty">No build is selected for verification summary.</div>
      )}
    </div>
  );
}

function SnapshotReportPanel({ build }) {
  const snapshot = build?.snapshot || {};
  return (
    <div className="panel">
      <h2>Snapshot Report</h2>
      {build ? (
        <>
          <OverviewFact label="Status" value={<span className={`badge ${statusTone(snapshot.status)}`}>{readableStatus(snapshot.status)}</span>} />
          <OverviewFact label="Requested / generated / reused" value={`${formatInt(snapshot.requested)} / ${formatInt(snapshot.generated)} / ${formatInt(snapshot.reused)}`} />
          <OverviewFact label="Manifest rows upserted" value={formatInt(snapshot.manifest_rows_upserted)} />
          <OverviewFact label="Generated at" value={formatDate(snapshot.generated_at)} />
          <OverviewFact label="Generator / view" value={`${snapshot.generator_version || "n/a"} / ${snapshot.view_type || "n/a"}`} />
          <OverviewFact label="Params hash" value={snapshot.params_hash || "n/a"} />
          <OverviewFact label="Manifest parquet" value={snapshot.manifest_parquet || "n/a"} />
          {snapshot.null_result ? (
            <div className="status-line">Null result recorded: zero requested, generated, reused, and manifest-upserted snapshot rows.</div>
          ) : null}
          {snapshot.parse_error ? <div className="trap-list"><div>{snapshot.parse_error}</div></div> : null}
        </>
      ) : (
        <div className="empty">No build is selected for snapshot report summary.</div>
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
              <th>Verify</th>
              <th>Snapshots</th>
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
                <td><span className={`badge ${statusTone(build.verification?.status)}`}>{readableStatus(build.verification?.status)}</span></td>
                <td><span className={`badge ${statusTone(build.snapshot?.status)}`}>{readableStatus(build.snapshot?.status)}</span></td>
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

function RetentionPlanPanel({ retention }) {
  const plan = retention?.dry_run || {};
  const candidates = plan.candidates || {};
  const buildCandidates = candidates.builds || [];
  const tmpCandidates = candidates.tmp || [];
  const reportCandidates = candidates.reports || [];
  const allCandidates = [
    ...buildCandidates.map((item) => ({ ...item, type: "build" })),
    ...tmpCandidates.map((item) => ({ ...item, type: "tmp" })),
    ...reportCandidates.map((item) => ({ ...item, type: "report" })),
  ];
  return (
    <div className="panel">
      <h2>Retention Dry-Run Plan</h2>
      <p className="muted">Parsed preview of what the retention script would report. The Admin action is dry-run only and does not delete artifacts.</p>
      <OverviewFact label="Policy" value={`keep ${formatInt(plan.keep_builds)} builds / ${formatInt(plan.keep_reports)} report dirs`} />
      <OverviewFact label="Candidates" value={`${formatInt(buildCandidates.length)} builds / ${formatInt(tmpCandidates.length)} tmp / ${formatInt(reportCandidates.length)} reports`} />
      <OverviewFact label="Estimated reclaimable" value={formatBytes(plan.estimated_reclaimable_bytes)} />
      <OverviewFact label="Served build protected" value={plan.served_build_id || "n/a"} />
      <OverviewFact label="Candidate hash" value={plan.candidate_hash ? compactId(plan.candidate_hash, 18) : "n/a"} />
      {allCandidates.length ? (
        <table>
          <thead>
            <tr><th>Type</th><th>Name</th><th>Reason</th><th>Size</th></tr>
          </thead>
          <tbody>
            {allCandidates.slice(0, 18).map((item) => (
              <tr key={`${item.type}-${item.path}`}>
                <td><span className={`badge ${item.type === "tmp" ? "warn" : "muted"}`}>{item.type}</span></td>
                <td>
                  {item.name}
                  <span className="table-subtext">{item.path}</span>
                </td>
                <td>{item.reason}</td>
                <td>{formatBytes(item.size_bytes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty">Retention dry-run has no build, report, or temporary-output candidates under the current policy.</div>
      )}
      {allCandidates.length > 18 ? <div className="status-line">{formatInt(allCandidates.length - 18)} additional candidates omitted from this compact view. Run Retention Dry Run for the full log.</div> : null}
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
        {rows.map((row, rowIndex) => (
          <tr key={`${row[0]}-${rowIndex}`}>
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

function AgencyScreen({ csrf }) {
  const [activeTab, setActiveTab] = useState("portfolios");
  const [state, setState] = useState({ loading: true, data: null, portfolios: null, seedCandidates: null, selectedDetail: null, message: "Loading agency status..." });
  const [selectedDossierId, setSelectedDossierId] = useState("");
  const [busySeedId, setBusySeedId] = useState("");
  const headers = useMemo(() => buildCsrfHeaders(csrf), [csrf]);

  async function loadAgency(preferredDossierId = selectedDossierId) {
    setState((current) => ({ ...current, loading: true, message: "Refreshing agency status..." }));
    const [statusResult, portfolioResult, seedResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/agency/status`),
      fetchJson(`${ADMIN_API_BASE}/agency/portfolios?limit=100`),
      fetchJson(`${ADMIN_API_BASE}/agency/seed-candidates?limit=50`),
    ]);
    const errors = [];
    if (!statusResult.response.ok) errors.push(`agency: ${compactError(statusResult.data, statusResult.response.status)}`);
    if (!portfolioResult.response.ok) errors.push(`portfolios: ${compactError(portfolioResult.data, portfolioResult.response.status)}`);
    if (!seedResult.response.ok) errors.push(`seed candidates: ${compactError(seedResult.data, seedResult.response.status)}`);
    const items = portfolioResult.response.ok && Array.isArray(portfolioResult.data.items) ? portfolioResult.data.items : [];
    const nextSelected = preferredDossierId || items[0]?.dossier_id || "";
    let selectedDetail = null;
    if (nextSelected) {
      selectedDetail = await loadPortfolioDetail(nextSelected, { silent: true });
    }
    if (nextSelected && nextSelected !== selectedDossierId) setSelectedDossierId(nextSelected);
    setState({
      loading: false,
      data: statusResult.response.ok ? statusResult.data : null,
      portfolios: portfolioResult.response.ok ? portfolioResult.data : { items: [], counts_by_status: {} },
      seedCandidates: seedResult.response.ok ? seedResult.data : { items: [], message: "Seed candidates unavailable" },
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

  async function seedPortfolio(candidate) {
    if (!candidate?.stable_object_key) return;
    setBusySeedId(candidate.id || candidate.stable_object_key);
    setState((current) => ({ ...current, message: `Seeding ${candidate.display_name || candidate.stable_object_key}...` }));
    const payload = {
      stable_object_key: candidate.stable_object_key,
      object_type: candidate.object_type || "system",
      display_name: candidate.display_name || candidate.stable_object_key,
      queue_reason: candidate.queue_reason || "coolness_rank",
      queue_priority: candidate.queue_priority || "normal",
      source_build_id: candidate.source_build_id || null,
      source: candidate.source || "coolness_scores",
      metadata: candidate.metadata || {},
    };
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/agency/portfolios`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    setBusySeedId("");
    if (!response.ok) {
      setState((current) => ({ ...current, message: `Seed failed: ${compactError(data, response.status)}` }));
      return;
    }
    const createdId = data?.dossier?.dossier_id || "";
    if (createdId) setSelectedDossierId(createdId);
    await loadAgency(createdId);
  }

  useEffect(() => {
    loadAgency();
  }, []);

  const data = state.data || {};
  const portfolios = state.portfolios || { items: [], counts_by_status: {} };
  const seedCandidates = state.seedCandidates || { items: [] };
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
          seedCandidates={seedCandidates}
          seedPortfolio={seedPortfolio}
          busySeedId={busySeedId}
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

function AgencyPortfoliosTab({ portfolios, countsByStatus, selectedDossierId, selectedDetail, selectPortfolio, seedCandidates, seedPortfolio, busySeedId }) {
  const seedItems = Array.isArray(seedCandidates?.items) ? seedCandidates.items : [];
  return (
    <section className="agency-portfolio-layout">
      <div className="agency-stack">
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
            <div className="empty">No Evidence Portfolio rows exist yet. Seed a ranked target below to create the first admin dossier and journal entry.</div>
          )}
        </div>
        <div className="panel">
          <div className="panel-head">
            <div>
              <h2>Seed Candidates</h2>
              <p className="muted">Ranked systems from current disc coolness scores. Seeding creates only workflow rows and a journal entry.</p>
            </div>
            <span className="badge">{formatInt(seedItems.length)} shown</span>
          </div>
          {seedCandidates?.message ? <div className="status-line">{seedCandidates.message}</div> : null}
          {seedItems.length ? (
            <table className="select-table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Target</th>
                  <th>Score</th>
                  <th>Priority</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {seedItems.map((item) => {
                  const busy = busySeedId === (item.id || item.stable_object_key);
                  const disabled = Boolean(item.existing_dossier_id) || busy;
                  return (
                    <tr key={item.id || item.stable_object_key}>
                      <td>{item.rank ?? "n/a"}</td>
                      <td>
                        <strong>{item.display_name || item.stable_object_key}</strong>
                        <span className="table-subtext">{item.stable_object_key}</span>
                        {item.existing_dossier_id ? <span className="table-subtext">existing: {compactId(item.existing_dossier_id, 28)} ({item.existing_dossier_status})</span> : null}
                      </td>
                      <td>{item.score_total == null ? "n/a" : formatFloat(item.score_total, 2)}</td>
                      <td><span className={`badge ${item.queue_priority === "high" ? "warn" : "muted"}`}>{item.queue_priority || "normal"}</span></td>
                      <td>
                        <button className="button" disabled={disabled} onClick={() => seedPortfolio(item)}>
                          {item.existing_dossier_id ? "Seeded" : busy ? "Seeding..." : "Seed"}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <div className="empty">No seed candidates are available. Run score coolness for the current build, then refresh this workspace.</div>
          )}
          <div className="hint-list agency-notes">
            <div>Seeded portfolios are not scientific claims. They are admin work items that make the next retrieval and review steps traceable.</div>
            <div>The first journal entry records the operator, source build, ranking metadata, and the fact that no model or extraction step has run.</div>
          </div>
        </div>
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
  const [selectedJobAudit, setSelectedJobAudit] = useState([]);
  const [selectedJobEvents, setSelectedJobEvents] = useState([]);
  const [logState, setLogState] = useState({ text: "", offset: 0, eof: true, status: "" });
  const [selectedAudit, setSelectedAudit] = useState(null);
  const [nextAuditBefore, setNextAuditBefore] = useState(null);
  const [auditPreset, setAuditPreset] = useState("all");
  const [auditFilters, setAuditFilters] = useState(emptyAuditFilters);
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

  async function loadJobAudit(jobId) {
    if (!jobId) {
      setSelectedJobAudit([]);
      return [];
    }
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}/audit?limit=25`);
    if (!response.ok) {
      setStatus(`Job audit ${compactId(jobId)}: ${compactError(data, response.status)}`);
      setSelectedJobAudit([]);
      return [];
    }
    const items = Array.isArray(data.items) ? data.items : [];
    setSelectedJobAudit(items);
    return items;
  }

  async function loadJobEvents(jobId) {
    if (!jobId) {
      setSelectedJobEvents([]);
      return [];
    }
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}/events?limit=100`);
    if (!response.ok) {
      setStatus(`Job events ${compactId(jobId)}: ${compactError(data, response.status)}`);
      setSelectedJobEvents([]);
      return [];
    }
    const items = Array.isArray(data.items) ? data.items : [];
    setSelectedJobEvents(items);
    return items;
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
    await loadJobEvents(jobId);
    await loadJobAudit(jobId);
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
        await loadJobEvents(selectedJobId);
        await loadJobAudit(selectedJobId);
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
        await loadJobEvents(selectedJobId);
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
    await loadJobEvents(jobId);
    await loadJobAudit(jobId);
  }

  function applyAuditPreset(nextPreset) {
    setAuditPreset(nextPreset);
    loadAudit({ append: false, preset: nextPreset, filters: auditFilters });
  }

  function clearAuditFilters() {
    setAuditPreset("all");
    setAuditFilters(emptyAuditFilters);
    loadAudit({ append: false, preset: "all", filters: emptyAuditFilters });
  }

  function updateAuditFilter(key, value) {
    setAuditFilters((current) => ({ ...current, [key]: value }));
  }

  function filterAuditForJob(jobId) {
    if (!jobId) return;
    const nextFilters = { ...emptyAuditFilters, correlation_id: jobId };
    setActiveTab("audit");
    setAuditPreset("all");
    setAuditFilters(nextFilters);
    loadAudit({ append: false, preset: "all", filters: nextFilters });
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
          selectedJobAudit={selectedJobAudit}
          selectedJobEvents={selectedJobEvents}
          logState={logState}
          actionsByName={actionsByName}
          selectJob={selectJob}
          filterAuditForJob={filterAuditForJob}
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
          applyAuditPreset={applyAuditPreset}
          setAuditPreset={setAuditPreset}
          auditFilters={auditFilters}
          updateAuditFilter={updateAuditFilter}
          clearAuditFilters={clearAuditFilters}
          loadAudit={loadAudit}
          nextAuditBefore={nextAuditBefore}
          selectJob={selectJob}
        />
      )}
    </div>
  );
}

function FullJobLogScreen({ jobId }) {
  const [state, setState] = useState({ loading: true, job: null, text: "", status: "", message: "Loading full log..." });
  const [query, setQuery] = useState("");
  const [levelFilter, setLevelFilter] = useState("all");
  const safeJobId = String(jobId || "").trim();
  const encodedJobId = encodeURIComponent(safeJobId);
  const downloadHref = safeJobId ? `${ADMIN_API_BASE}/actions/jobs/${encodedJobId}/log/download` : "";
  const rawTextHref = safeJobId ? `${ADMIN_API_BASE}/actions/jobs/${encodedJobId}/log/text` : "";

  async function loadFullLog() {
    if (!safeJobId) {
      setState({ loading: false, job: null, text: "", status: "", message: "No job id was provided." });
      return;
    }
    setState((current) => ({ ...current, loading: true, message: "Loading full log..." }));
    const [jobResult, logResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodedJobId}`),
      fetchText(`${ADMIN_API_BASE}/actions/jobs/${encodedJobId}/log/text`),
    ]);
    if (!jobResult.response.ok) {
      setState({
        loading: false,
        job: null,
        text: "",
        status: "",
        message: `Job metadata: ${compactError(jobResult.data, jobResult.response.status)}`,
      });
      return;
    }
    if (!logResult.response.ok) {
      setState({
        loading: false,
        job: jobResult.data.job || null,
        text: logResult.text || "",
        status: "",
        message: `Log: ${logResult.text || logResult.response.status}`,
      });
      return;
    }
    setState({
      loading: false,
      job: jobResult.data.job || null,
      text: logResult.text || "",
      status: logResult.response.headers.get("X-Job-Status") || jobResult.data.job?.status || "",
      message: "Ready",
    });
  }

  useEffect(() => {
    loadFullLog();
  }, [safeJobId]);

  const job = state.job || {};
  return (
    <div className="screen full-log-screen">
      <header className="page-header">
        <div>
          <h1>Job Log</h1>
          <p className="muted">{safeJobId || "No job selected"} | {actionLabel(job.action)} | {state.message}</p>
        </div>
        <div className="log-toolbar">
          <button className="button" onClick={loadFullLog}>Reload</button>
          {rawTextHref ? <a className="button" href={rawTextHref} target="_blank" rel="noreferrer">Raw Text</a> : null}
          {downloadHref ? <a className="button" href={downloadHref}>Download</a> : null}
          <a className="button" href="/admin/">Admin</a>
        </div>
      </header>
      {state.job ? (
        <div className="log-summary-grid">
          <div className="overview-fact">
            <span>Status</span>
            <strong>{job.status || state.status || "n/a"}</strong>
          </div>
          <div className="overview-fact">
            <span>Timeline</span>
            <strong>{formatDate(job.created_at)}{" -> "}{formatDate(job.finished_at || job.started_at)}</strong>
          </div>
          <div className="overview-fact">
            <span>Exit</span>
            <strong>{job.exit_code ?? "n/a"} {job.error_message ? `| ${job.error_message}` : ""}</strong>
          </div>
        </div>
      ) : null}
      <JobLogViewer
        text={state.text}
        query={query}
        setQuery={setQuery}
        levelFilter={levelFilter}
        setLevelFilter={setLevelFilter}
        eof={!state.loading}
        status={state.status || job.status || ""}
        fullHeight
      />
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
          <section className={`panel runbook-group ${group.key || ""}`} key={group.key}>
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
  const [values, setValues] = useState(() => ({ ...initialActionValues(action), ...loadPersistedActionDraft(action) }));
  const [status, setStatus] = useState("");
  const guidance = action.operator_guidance || fallbackActionGuidance[action.name] || {};
  const schema = action.params_schema || {};
  const disabledReason = action.disabled_reason || "";
  const confirmationPhrase = action.confirmation_phrase || `RUN ${action.name}`;

  useEffect(() => {
    setValues({ ...initialActionValues(action), ...loadPersistedActionDraft(action) });
    setStatus("");
  }, [
    action.name,
    schema.candidate_hash?.default,
    schema.keep_builds?.default,
    schema.keep_reports?.default,
    schema.skip_tmp?.default,
  ]);

  function updateValue(key, value) {
    setValues((current) => {
      const next = { ...current, [key]: value };
      savePersistedActionDraft(action, next);
      return next;
    });
  }

  async function submit(event) {
    event.preventDefault();
    if (disabledReason) {
      setStatus(disabledReason);
      return;
    }
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
        {disabledReason ? <div className="warning-text"><strong>Blocked:</strong> {disabledReason}</div> : null}
      </div>
      <div className="action-fields">
        {Object.entries(schema).map(([name, spec]) => (
          <ActionParamField
            action={action}
            key={name}
            name={name}
            spec={spec || {}}
            value={values[name]}
            values={values}
            updateValue={updateValue}
          />
        ))}
        {action.requires_confirmation ? (
          <label>
            <span>Confirmation phrase</span>
            <input
              value={values.confirmation || ""}
              onChange={(event) => updateValue("confirmation", event.target.value)}
              placeholder={confirmationPhrase}
            />
            <em className="confirmation-reminder">Required phrase: <code>{confirmationPhrase}</code></em>
          </label>
        ) : null}
      </div>
      <div className="card-actions">
        <button className={`button ${action.risk_level === "high" ? "danger" : "primary"}`} disabled={busy || Boolean(disabledReason)} type="submit">
          {busy ? "Starting..." : "Start Job"}
        </button>
        {status ? <span className="inline-status">{status}</span> : null}
      </div>
    </form>
  );
}

function ActionParamField({ action, name, spec, value, values, updateValue }) {
  if (spec.hidden) return null;
  const label = spec.label || actionLabel(name);
  if (name === "weights_json" && ["score_coolness", "save_coolness_profile"].includes(String(action?.name || ""))) {
    return (
      <CoolnessWeightsField
        collapsed={String(action?.name || "") === "save_coolness_profile"}
        label={label}
        name={name}
        sliderDraft={values?._coolness_sliders}
        value={value}
        updateValue={updateValue}
      />
    );
  }
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
      {action?.name === "generate_snapshots" && name === "top_coolness" && Number(value) > 10000 ? (
        <em className="confirmation-reminder warning-text">
          Above 10,000 systems can run for a long time and produce many files. This is allowed on Photon; monitor Jobs and storage.
        </em>
      ) : null}
    </label>
  );
}

function slidersFromWeightsJson(raw) {
  const text = String(raw || "").trim();
  if (!text) return { ...defaultCoolnessSliders };
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { ...defaultCoolnessSliders };
    const values = coolnessWeightMeta.map(([key]) => Number(parsed[key]));
    const maxValue = Math.max(...values.filter((item) => Number.isFinite(item) && item > 0), 0);
    if (maxValue <= 0) return { ...defaultCoolnessSliders };
    return Object.fromEntries(
      coolnessWeightMeta.map(([key]) => {
        const value = Number(parsed[key]);
        const sliderValue = Number.isFinite(value) && value > 0 ? Math.round((value / maxValue) * 10) : 1;
        return [key, Math.max(1, Math.min(10, sliderValue))];
      })
    );
  } catch (_) {
    return { ...defaultCoolnessSliders };
  }
}

function validCoolnessSliderDraft(raw) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const out = {};
  for (const [key] of coolnessWeightMeta) {
    const value = Number(raw[key]);
    if (!Number.isFinite(value)) return null;
    out[key] = Math.max(1, Math.min(10, Math.round(value)));
  }
  return out;
}

function weightsJsonFromSliders(sliders) {
  const total = coolnessWeightMeta.reduce((sum, [key]) => sum + Math.max(1, Math.min(10, Number(sliders[key]) || 1)), 0);
  const weights = Object.fromEntries(
    coolnessWeightMeta.map(([key]) => {
      const value = Math.max(1, Math.min(10, Number(sliders[key]) || 1));
      return [key, Number((value / total).toFixed(6))];
    })
  );
  return JSON.stringify(weights);
}

function CoolnessWeightsField({ collapsed = false, label, name, sliderDraft, value, updateValue }) {
  const [sliders, setSliders] = useState(() => validCoolnessSliderDraft(sliderDraft) || slidersFromWeightsJson(value));
  const usingOverride = Boolean(String(value || "").trim());
  const normalized = weightsJsonFromSliders(sliders);

  useEffect(() => {
    const draft = validCoolnessSliderDraft(sliderDraft);
    if (draft) {
      setSliders(draft);
    } else if (!String(value || "").trim()) {
      setSliders({ ...defaultCoolnessSliders });
    } else {
      setSliders(slidersFromWeightsJson(value));
    }
  }, [sliderDraft, value]);

  function updateSlider(key, rawValue) {
    const sliderValue = Math.max(1, Math.min(10, Number.parseInt(String(rawValue), 10) || 1));
    setSliders((current) => {
      const next = { ...current, [key]: sliderValue };
      updateValue("_coolness_sliders", next);
      updateValue(name, weightsJsonFromSliders(next));
      return next;
    });
  }

  function resetWeights() {
    setSliders({ ...defaultCoolnessSliders });
    updateValue("_coolness_sliders", null);
    updateValue(name, "");
  }

  const field = (
    <div className="coolness-weights-field">
      <div className="field-head">
        <div>
          <strong>{label.replace(" JSON", "")}</strong>
          <span className="table-subtext">Set relative contribution from 1 to 10. Slider values are normalized into the scorer's weight JSON.</span>
        </div>
        <button className="button" type="button" onClick={resetWeights}>Use Profile Defaults</button>
      </div>
      <div className="coolness-slider-list">
        {coolnessWeightMeta.map(([key, itemLabel, description]) => (
          <label className="coolness-slider-row" key={key}>
            <span>
              <strong>{itemLabel}</strong>
              <em>{description}</em>
            </span>
            <input
              type="range"
              min="1"
              max="10"
              step="1"
              value={sliders[key] || 1}
              onChange={(event) => updateSlider(key, event.target.value)}
            />
            <output>{sliders[key] || 1}</output>
          </label>
        ))}
      </div>
      <div className="status-line">
        {usingOverride ? "Using slider override for this job." : "No override: this job will use the selected profile/default weights."}
      </div>
      {usingOverride ? (
        <details>
          <summary>Generated weights JSON</summary>
          <pre className="json-box">{JSON.stringify(JSON.parse(normalized), null, 2)}</pre>
        </details>
      ) : null}
    </div>
  );
  if (!collapsed) return field;
  return (
    <details className="coolness-weights-disclosure">
      <summary>{label.replace(" JSON", "")}</summary>
      {field}
    </details>
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

function jobActorLabel(job) {
  const actor = job?.requested_by || {};
  if (actor.email && actor.display_name) return `${actor.display_name} <${actor.email}>`;
  if (actor.email) return actor.email;
  if (job?.requested_by_user_id) return `user #${job.requested_by_user_id}`;
  return "unknown";
}

function JobsTab({
  jobs,
  selectedJob,
  selectedJobAudit,
  selectedJobEvents,
  logState,
  actionsByName,
  selectJob,
  filterAuditForJob,
  cancelJob,
  refreshSelected,
}) {
  const [logQuery, setLogQuery] = useState("");
  const [logLevelFilter, setLogLevelFilter] = useState("all");
  const [copyStatus, setCopyStatus] = useState("");
  const activeRows = jobs.filter((job) => ["queued", "running"].includes(String(job.status || "")));
  const selectedStatus = String(selectedJob?.status || "");
  const auditItems = Array.isArray(selectedJobAudit) ? selectedJobAudit : [];
  const eventItems = Array.isArray(selectedJobEvents) ? selectedJobEvents : [];
  const selectedAction = selectedJob ? actionsByName.get(selectedJob.action) || null : null;

  async function copySelectedJobDiagnostics() {
    if (!selectedJob) return;
    const lines = String(logState.text || "").split(/\r?\n/).filter(Boolean);
    const lineCounts = lines.reduce((acc, line) => {
      const level = classifyLogLine(line);
      acc[level] = (acc[level] || 0) + 1;
      return acc;
    }, {});
    const bundle = {
      generated_at_utc: new Date().toISOString(),
      job: selectedJob,
      action: selectedAction ? {
        name: selectedAction.name,
        display_name: selectedAction.display_name,
        category: selectedAction.category,
        group_key: selectedAction.group_key,
        risk_level: selectedAction.risk_level,
        required_roles: selectedAction.required_roles,
        requires_confirmation: selectedAction.requires_confirmation,
        operator_guidance: selectedAction.operator_guidance,
      } : null,
      timeline: eventItems,
      audit_items: auditItems,
      output_hints: selectedJob.artifact_hints || [],
      log: {
        status: logState.status || selectedJob.status || "",
        eof: Boolean(logState.eof),
        loaded_offset: logState.offset,
        loaded_line_count: lines.length,
        line_counts: lineCounts,
        final_lines: lines.slice(-60),
      },
    };
    try {
      await copyTextToClipboard(JSON.stringify(bundle, null, 2));
      setCopyStatus("Copied job diagnostics.");
    } catch (error) {
      setCopyStatus(`Copy failed: ${String(error)}`);
    }
  }

  return (
    <section className="jobs-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Job Queue</h2>
          <span className="muted">{formatInt(activeRows.length)} active</span>
        </div>
        {jobs.length ? (
          <table className="select-table job-queue-table">
            <colgroup>
              <col className="job-col-status" />
              <col className="job-col-action" />
              <col className="job-col-actor" />
              <col className="job-col-created" />
              <col className="job-col-duration" />
              <col className="job-col-error" />
            </colgroup>
            <thead>
              <tr>
                <th>Status</th>
                <th>Action</th>
                <th>Actor</th>
                <th>Created</th>
                <th>Duration</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr
                  className={selectedJob?.job_id === job.job_id ? "selected clickable-row" : "clickable-row"}
                  key={job.job_id}
                  onClick={() => selectJob(job.job_id)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      selectJob(job.job_id);
                    }
                  }}
                  role="button"
                  tabIndex={0}
                  title={`Open job detail for ${job.job_id}`}
                >
                  <td><span className={`badge ${jobStatusTone(job.status)}`}>{job.status}</span></td>
                  <td>
                    <strong>{actionLabel(job.action)}</strong>
                    <span className="table-subtext">{compactId(job.job_id, 24)}</span>
                  </td>
                  <td className="job-actor-cell" title={jobActorLabel(job)}>{jobActorLabel(job)}</td>
                  <td title={formatDate(job.created_at)}>{formatDateCompact(job.created_at)}</td>
                  <td>{jobDuration(job)}</td>
                  <td className="job-error-cell" title={job.error_message || ""}>{job.error_message ? compactId(job.error_message, 72) : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No admin jobs have been recorded yet. Start with the Runbook tab when you are ready to run an allowlisted action.</div>
        )}
      </div>

      <JobDetailPanel
        job={selectedJob}
        action={selectedAction}
        selectedStatus={selectedStatus}
        auditItems={auditItems}
        eventItems={eventItems}
        logState={logState}
        logQuery={logQuery}
        setLogQuery={setLogQuery}
        logLevelFilter={logLevelFilter}
        setLogLevelFilter={setLogLevelFilter}
        refreshSelected={refreshSelected}
        filterAuditForJob={filterAuditForJob}
        cancelJob={cancelJob}
        copyDiagnostics={copySelectedJobDiagnostics}
        copyStatus={copyStatus}
      />
    </section>
  );
}

function jobGuidance(job, action) {
  const fromAction = action?.operator_guidance || {};
  const fallback = fallbackActionGuidance[job?.action] || {};
  return { ...fallback, ...fromAction };
}

function normalizeGuidanceList(value) {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean).map(String) : [String(value)];
}

function persistedActionDraftKey(action) {
  if (action?.name !== "score_coolness") return "";
  return "spacegate.admin.actionDraft.score_coolness";
}

function loadPersistedActionDraft(action) {
  const key = persistedActionDraftKey(action);
  if (!key) return {};
  try {
    const raw = window.localStorage?.getItem(key);
    const parsed = raw ? JSON.parse(raw) : {};
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch (_) {
    return {};
  }
}

function savePersistedActionDraft(action, values) {
  const key = persistedActionDraftKey(action);
  if (!key) return;
  try {
    window.localStorage?.setItem(key, JSON.stringify(values || {}));
  } catch (_) {
    // Draft persistence is a convenience; failed storage should not block jobs.
  }
}

function JobDetailPanel({
  job,
  action,
  selectedStatus,
  auditItems,
  eventItems,
  logState,
  logQuery,
  setLogQuery,
  logLevelFilter,
  setLogLevelFilter,
  refreshSelected,
  filterAuditForJob,
  cancelJob,
  copyDiagnostics,
  copyStatus,
}) {
  if (!job) {
    return (
      <div className="panel job-detail">
        <div className="panel-head">
          <h2>Job Detail</h2>
        </div>
        <div className="empty">Select a job to inspect parameters, execution plan, log output, and troubleshooting hints.</div>
      </div>
    );
  }
  const encodedJobId = encodeURIComponent(job.job_id);
  const logDownloadHref = `${ADMIN_API_BASE}/actions/jobs/${encodedJobId}/log/download`;
  const logTextHref = `${ADMIN_API_BASE}/actions/jobs/${encodedJobId}/log/text`;
  const logViewHref = `/admin/?job_log=${encodedJobId}`;
  const guidance = jobGuidance(job, action);
  const risk = action?.risk_level || guidance.risk_level || "low";
  const timelineEnd = job.finished_at || job.started_at || null;
  return (
    <div className="panel job-detail">
      <div className="panel-head">
        <div>
          <h2>Job Detail</h2>
          <p className="muted">{job.job_id}</p>
        </div>
        <div className="report-chip-list compact">
          <span className={`badge ${jobStatusTone(selectedStatus)}`}>{selectedStatus || "unknown"}</span>
          <span className={`badge ${riskTone(risk)}`}>{risk} risk</span>
        </div>
      </div>

      <JobFailureSummary job={job} logText={logState.text} />
      <JobTrap job={job} logState={logState} />

      <div className="job-detail-grid">
        <div className="overview-fact">
          <span>Action</span>
          <strong>{action?.display_name || actionLabel(job.action)}</strong>
        </div>
        <div className="overview-fact">
          <span>Actor</span>
          <strong>{jobActorLabel(job)}</strong>
        </div>
        <div className="overview-fact">
          <span>Created</span>
          <strong>{formatDate(job.created_at)}</strong>
        </div>
        <div className="overview-fact">
          <span>Started</span>
          <strong>{formatDate(job.started_at)}</strong>
        </div>
        <div className="overview-fact">
          <span>Finished</span>
          <strong>{formatDate(job.finished_at)}</strong>
        </div>
        <div className="overview-fact">
          <span>Duration</span>
          <strong>{jobDuration(job)}</strong>
        </div>
        <div className="overview-fact">
          <span>Exit Code</span>
          <strong>{job.exit_code ?? "n/a"}</strong>
        </div>
        <div className="overview-fact">
          <span>Log Path</span>
          <strong title={job.log_path || ""}>{compactId(job.log_path, 42)}</strong>
        </div>
      </div>

      <JobGuidancePanel action={action} guidance={guidance} job={job} />

      <div className="log-toolbar">
        <button className="button" onClick={refreshSelected}>Reload Log</button>
        <a className="button" href={logViewHref} target="_blank" rel="noreferrer">Readable Log</a>
        <a className="button" href={logTextHref} target="_blank" rel="noreferrer">Raw Text</a>
        <a className="button" href={logDownloadHref}>Download Log</a>
        <button className="button" onClick={() => filterAuditForJob(job.job_id)}>Open Job Audit</button>
        <button className="button" onClick={copyDiagnostics}>Copy Diagnostics</button>
        {selectedStatus === "queued" ? (
          <button className="button danger" onClick={() => cancelJob(job.job_id)}>Cancel Queued Job</button>
        ) : null}
      </div>
      {copyStatus ? <div className="status-line">{copyStatus}</div> : null}

      <JobEventsPanel events={eventItems} />
      <JobArtifactHintsPanel hints={job.artifact_hints || []} />
      <JobAuditPanel items={auditItems} filterAuditForJob={() => filterAuditForJob(job.job_id)} />

      <details open>
        <summary>Parameters</summary>
        <pre className="json-box">{jsonBlock(job.params)}</pre>
      </details>
      <details>
        <summary>Execution Plan</summary>
        <pre className="json-box">{jsonBlock(job.execution)}</pre>
      </details>
      <details>
        <summary>Lifecycle Summary</summary>
        <MetricList
          rows={[
            ["Job ID", job.job_id],
            ["Timeline", `${formatDate(job.created_at)} -> ${formatDate(timelineEnd)}`],
            ["Requested by", jobActorLabel(job), job.requested_by?.roles?.length ? `roles: ${job.requested_by.roles.join(", ")}` : ""],
            ["Correlation", job.job_id, "Use this ID to filter Audit Trail entries."],
          ]}
        />
      </details>

      <JobLogViewer
        text={logState.text}
        query={logQuery}
        setQuery={setLogQuery}
        levelFilter={logLevelFilter}
        setLevelFilter={setLogLevelFilter}
        eof={logState.eof}
        status={logState.status}
      />
    </div>
  );
}

function JobGuidancePanel({ action, guidance, job }) {
  const before = normalizeGuidanceList(guidance.prerequisites || guidance.before);
  const writes = normalizeGuidanceList(guidance.writes_to || guidance.writes);
  const outputs = normalizeGuidanceList(guidance.outputs);
  const next = normalizeGuidanceList(guidance.success_next_actions || guidance.next);
  const warnings = normalizeGuidanceList(guidance.warnings || guidance.warning);
  return (
    <details open className="job-guidance-panel">
      <summary>Operation Guidance</summary>
      <div className="hint-list">
        <div><strong>Purpose:</strong> {guidance.purpose || action?.description || `Run ${actionLabel(job.action)}.`}</div>
        {before.length ? <div><strong>Before:</strong> {before.join(" ")}</div> : null}
        {writes.length ? <div><strong>Writes:</strong> {writes.join(", ")}</div> : null}
        {outputs.length ? <div><strong>Expected outputs:</strong> {outputs.join(", ")}</div> : null}
        {guidance.expected_duration || guidance.duration ? <div><strong>Expected duration:</strong> {String(guidance.expected_duration || guidance.duration).replaceAll("_", " ")}</div> : null}
        {next.length ? <div><strong>Next action:</strong> {next.join(" ")}</div> : null}
        {warnings.length ? <div className="warning-text"><strong>Warning:</strong> {warnings.join(" ")}</div> : null}
        {action?.required_roles?.length ? <div><strong>Required roles:</strong> {action.required_roles.join(", ")}</div> : null}
      </div>
    </details>
  );
}

function JobFailureSummary({ job, logText }) {
  const status = String(job?.status || "");
  const failed = status === "failed" || job?.error_message || (job?.exit_code !== null && job?.exit_code !== undefined && Number(job.exit_code) !== 0);
  if (!failed) return null;
  const lines = String(logText || "").split(/\r?\n/).filter(Boolean);
  const important = lines.filter((line) => ["error", "warn"].includes(classifyLogLine(line))).slice(-5);
  return (
    <div className="failure-card">
      <div>
        <strong>Failure Summary</strong>
        <span className="table-subtext">Exit {job.exit_code ?? "n/a"} | {job.error_message || "No structured error message recorded."}</span>
      </div>
      {important.length ? (
        <ul>
          {important.map((line, index) => <li key={`${index}-${line}`}>{compactId(line, 180)}</li>)}
        </ul>
      ) : (
        <p className="muted">No error or warning lines are loaded yet. Open the readable log or reload the log chunk before retrying.</p>
      )}
    </div>
  );
}

function jobEventTone(event) {
  const status = String(event?.event_status || "");
  const type = String(event?.event_type || "");
  if (status === "succeeded" || type === "completed") return "ok";
  if (status === "failed" || type === "failed") return "danger";
  if (status === "cancelled" || type === "cancelled") return "muted";
  if (status === "running" || status === "queued" || type === "executing" || type === "started") return "warn";
  return "muted";
}

function JobEventsPanel({ events }) {
  const items = Array.isArray(events) ? events : [];
  return (
    <details open className="job-events-panel">
      <summary>Timeline ({formatInt(items.length)})</summary>
      {items.length ? (
        <div className="job-event-list">
          {items.map((event) => (
            <div className={`job-event-row ${jobEventTone(event)}`} key={`${event.event_id}-${event.event_type}-${event.created_at}`}>
              <span className={`badge ${jobEventTone(event)}`}>{event.event_status || event.event_type}</span>
              <div>
                <strong>{event.event_type}{event.synthetic ? " (derived)" : ""}</strong>
                <span className="table-subtext">{formatDate(event.created_at)} | {event.message || "No message"}</span>
                {event.details && Object.keys(event.details).length ? (
                  <span className="table-subtext">{compactId(JSON.stringify(event.details), 140)}</span>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty">No structured job events are available yet. New jobs should record queue, start, execution, and terminal milestones.</div>
      )}
    </details>
  );
}

function JobArtifactHintsPanel({ hints }) {
  const items = Array.isArray(hints) ? hints : [];
  return (
    <details open className="job-output-panel">
      <summary>Outputs ({formatInt(items.length)})</summary>
      {items.length ? (
        <div className="job-output-list">
          {items.map((item, index) => (
            <div className="job-output-row" key={`${item.kind || "output"}-${item.path || item.label || index}-${index}`}>
              <div>
                <span className={`badge ${item.exists ? "ok" : "warn"}`}>{item.exists ? "found" : "missing"}</span>
              </div>
              <div>
                <strong>{item.label || item.kind || "Output"}</strong>
                <span className="table-subtext">
                  {item.path || item.description || ""}
                </span>
                {item.description && item.path ? <span className="table-subtext">{item.description}</span> : null}
                {item.note ? <span className="table-subtext">note: {item.note}</span> : null}
                <div className="report-chip-list compact">
                  {item.kind ? <span className="badge muted">{item.kind}</span> : null}
                  {item.size_bytes ? <span className="badge muted">{formatBytes(item.size_bytes)}</span> : null}
                  {item.candidate_count !== undefined ? <span className="badge muted">candidates {formatInt(item.candidate_count)}</span> : null}
                  {item.estimated_reclaimable_bytes !== undefined ? <span className="badge muted">reclaim {formatBytes(item.estimated_reclaimable_bytes)}</span> : null}
                  {item.candidate_hash ? <span className="badge muted">{compactId(item.candidate_hash, 18)}</span> : null}
                  {item.mtime_utc ? <span className="badge muted">{formatDate(item.mtime_utc)}</span> : null}
                </div>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty">No structured output hints are available for this action yet. Use the log reader for detailed command output.</div>
      )}
    </details>
  );
}

function JobAuditPanel({ items, filterAuditForJob }) {
  return (
    <details open className="job-audit-panel">
      <summary>Correlated Audit ({formatInt(items.length)})</summary>
      {items.length ? (
        <div className="job-audit-list">
          {items.map((entry) => (
            <div className="job-audit-row" key={entry.audit_id}>
              <span className={`badge ${entry.result === "success" ? "ok" : entry.result === "error" ? "danger" : "warn"}`}>{entry.result}</span>
              <strong>#{entry.audit_id} {entry.event_type}</strong>
              <span>{formatDate(entry.created_at)} | {auditActorLabel(entry)}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty">No correlated audit entries were found for this job. Newer jobs should usually have at least a launch audit event.</div>
      )}
      <button className="button" onClick={filterAuditForJob}>Inspect In Audit Trail</button>
    </details>
  );
}

function classifyLogLine(text) {
  const line = String(text || "");
  const lower = line.toLowerCase();
  if (lower.includes("[error]") || lower.includes("error:") || lower.includes("traceback") || lower.includes("failed") || lower.includes("exception")) return "error";
  if (lower.includes("warning") || lower.includes("warn:") || lower.includes("blocked") || lower.includes("skipped")) return "warn";
  if (lower.includes("ok:") || lower.includes("succeeded") || lower.includes("complete") || lower.includes("healthy")) return "ok";
  if (line.startsWith("[") || lower.startsWith("action:") || lower.startsWith("params:") || lower.startsWith("execution:")) return "meta";
  return "plain";
}

function JobLogViewer({ text, query, setQuery, levelFilter, setLevelFilter, eof, status, fullHeight = false }) {
  const rows = String(text || "")
    .split(/\r?\n/)
    .map((line, index) => ({ index: index + 1, text: line, level: classifyLogLine(line) }))
    .filter((row, index, array) => row.text || index < array.length - 1);
  const counts = rows.reduce((acc, row) => {
    acc[row.level] = (acc[row.level] || 0) + 1;
    return acc;
  }, {});
  const needle = String(query || "").trim().toLowerCase();
  const filtered = rows.filter((row) => {
    const levelMatch = levelFilter === "all" || row.level === levelFilter;
    const queryMatch = !needle || row.text.toLowerCase().includes(needle) || String(row.index).includes(needle);
    return levelMatch && queryMatch;
  });
  const hasLog = rows.length > 0;
  return (
    <div className="log-reader">
      <div className="log-reader-head">
        <div>
          <strong>Log Reader</strong>
          <span className="table-subtext">{formatInt(filtered.length)} shown of {formatInt(rows.length)} lines | status {status || "n/a"} | {eof ? "complete" : "streaming"}</span>
        </div>
        <div className="report-chip-list compact">
          <span className="badge danger">errors {formatInt(counts.error || 0)}</span>
          <span className="badge warn">warnings {formatInt(counts.warn || 0)}</span>
          <span className="badge ok">ok {formatInt(counts.ok || 0)}</span>
        </div>
      </div>
      <div className="log-controls">
        <label>
          <span>Search log</span>
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="error, Deleted, job id, line number" />
        </label>
        <label>
          <span>Line filter</span>
          <select value={levelFilter} onChange={(event) => setLevelFilter(event.target.value)}>
            <option value="all">All lines</option>
            <option value="error">Errors</option>
            <option value="warn">Warnings/skips</option>
            <option value="ok">OK/complete</option>
            <option value="meta">Metadata</option>
            <option value="plain">Plain</option>
          </select>
        </label>
      </div>
      {hasLog ? (
        <div className={`log-lines ${fullHeight ? "full-height" : ""}`} role="log" aria-label="Job log output">
          {filtered.length ? filtered.map((row) => (
            <div className={`log-line ${row.level}`} key={`${row.index}-${row.text}`}>
              <span className="log-line-no">{row.index}</span>
              <span className="log-line-text">{row.text || " "}</span>
            </div>
          )) : (
            <div className="empty">No log lines match the current filter.</div>
          )}
        </div>
      ) : (
        <div className="empty">No log output is available yet.</div>
      )}
    </div>
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

function auditActorLabel(entry) {
  const actor = entry?.actor || {};
  if (actor.email && actor.display_name) return `${actor.display_name} <${actor.email}>`;
  if (actor.email) return actor.email;
  if (entry?.actor_user_id) return `user #${entry.actor_user_id}`;
  return "system / unauthenticated";
}

function auditActorDetail(entry) {
  const actor = entry?.actor || {};
  const parts = [];
  if (entry?.actor_user_id) parts.push(`id ${entry.actor_user_id}`);
  if (Array.isArray(actor.roles) && actor.roles.length) parts.push(`roles ${actor.roles.join(", ")}`);
  return parts.length ? parts.join(" | ") : "no actor user recorded";
}

function AuditTab({
  auditItems,
  selectedAudit,
  setSelectedAudit,
  auditPreset,
  applyAuditPreset,
  setAuditPreset,
  auditFilters,
  updateAuditFilter,
  clearAuditFilters,
  loadAudit,
  nextAuditBefore,
  selectJob,
}) {
  function filterSelectedActor() {
    if (!selectedAudit?.actor_user_id) return;
    const actorId = String(selectedAudit.actor_user_id);
    const nextFilters = { event_type: "", result: "", request_id: "", actor_user_id: actorId };
    setAuditPreset("all");
    Object.entries(nextFilters).forEach(([key, value]) => updateAuditFilter(key, value));
    loadAudit({ append: false, preset: "all", filters: nextFilters });
  }

  return (
    <section className="audit-layout">
      <div className="panel">
        <div className="panel-head">
          <h2>Audit Trail</h2>
          <button className="button" onClick={() => loadAudit({ append: false })}>Refresh</button>
        </div>
        <div className="audit-presets">
          {auditPresets.map((preset) => (
            <button className={auditPreset === preset.key ? "active" : ""} key={preset.key} onClick={() => applyAuditPreset(preset.key)}>
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
          <label>
            <span>Correlation ID</span>
            <input value={auditFilters.correlation_id} onChange={(event) => updateAuditFilter("correlation_id", event.target.value)} placeholder="job_..." />
          </label>
        </div>
        <div className="card-actions">
          <button className="button primary" onClick={() => loadAudit({ append: false })}>Apply Filters</button>
          <button className="button" onClick={clearAuditFilters}>Clear Filters</button>
          <button className="button" disabled={!nextAuditBefore} onClick={() => loadAudit({ append: true })}>Load Older</button>
        </div>
        <div className="audit-list">
          {auditItems.length ? auditItems.map((entry) => (
            <button className={selectedAudit?.audit_id === entry.audit_id ? "audit-row selected" : "audit-row"} key={entry.audit_id} onClick={() => setSelectedAudit(entry)}>
              <span className={`badge ${entry.result === "success" ? "ok" : entry.result === "error" ? "danger" : "warn"}`}>{entry.result}</span>
              <strong>#{entry.audit_id} {entry.event_type}</strong>
              <span className="audit-row-meta">{formatDate(entry.created_at)} | {auditActorLabel(entry)}</span>
              <span className="audit-row-meta audit-row-aux">{auditActorDetail(entry)} {entry.request_id || ""} {entry.correlation_id || ""}</span>
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
              <span>Actor</span>
              <strong>{auditActorLabel(selectedAudit)} | {auditActorDetail(selectedAudit)}</strong>
            </div>
            <div className="overview-fact">
              <span>Correlation</span>
              <strong>{selectedAudit.correlation_id || "n/a"}</strong>
            </div>
            {selectedAudit.actor_user_id ? (
              <button className="button" onClick={filterSelectedActor}>Filter To This Actor</button>
            ) : null}
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

function RuntimeScreen() {
  const [state, setState] = useState({ loading: true, data: null, message: "Loading runtime status..." });
  const [copyStatus, setCopyStatus] = useState("");

  async function loadRuntime() {
    setState((current) => ({ ...current, loading: true, message: "Refreshing runtime status..." }));
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/runtime/status`);
    if (!response.ok) {
      setState({ loading: false, data: null, message: `Runtime status: ${compactError(data, response.status)}` });
      return;
    }
    setState({ loading: false, data, message: "Ready" });
  }

  useEffect(() => {
    loadRuntime();
  }, []);

  const data = state.data || {};
  const authStatus = data.auth || {};
  const host = data.host_runtime || {};
  const api = data.api_process_runtime || {};
  const paths = data.paths || {};
  const filesystemAlerts = Array.isArray(data.filesystem_alerts) ? data.filesystem_alerts : [];
  const filesystemSummary = data.filesystem_summary || {};
  const endpoints = Array.isArray(data.inference_endpoints) ? data.inference_endpoints : [];
  const healthyEndpoints = endpoints.filter((endpoint) => endpoint.last_probe_status === "ok");
  const configuredEnv = data.environment?.configured || {};
  const sensitiveEnv = data.environment?.sensitive || {};
  const configSources = data.environment?.config_sources || {};
  const configSourceRows = (configSources.sources || []).map((item) => [
    item.precedence || "",
    <span className={`badge ${runtimeStatusTone(item.status)}`}>{runtimeStatusLabel(item.status)}</span>,
    item.role || "",
    item.path || "",
  ]);
  function pathAccessLabel(item) {
    if (!item.exists) {
      return item.configured || item.required ? "missing" : "optional not mounted";
    }
    const parts = [item.readable ? "read ok" : "read blocked"];
    if (item.require_writable) {
      parts.push(item.writable ? "write ok" : "write blocked");
    } else {
      parts.push(item.writable ? "write available" : "read-only ok");
    }
    if (item.mount_expected) {
      parts.push(item.mounted ? `mounted at ${item.mount_point || "target"}` : "mount not visible");
    } else if (item.mount_point) {
      parts.push(`fs ${item.mount_point}`);
    }
    return parts.join(" / ");
  }
  const pathRows = Object.entries(paths).map(([key, item]) => [
    key,
    <span className={`badge ${item.check_status === "error" ? "danger" : item.check_status === "warning" ? "warn" : "ok"}`}>
      {item.check_status || "unknown"}
    </span>,
    item.exists ? (item.is_dir ? "dir" : item.is_file ? "file" : "exists") : "missing",
    pathAccessLabel(item),
    item.disk ? `${formatBytes(item.disk.free_bytes)} free (${formatPct(item.disk.used_pct)} used)` : "n/a",
    <>
      {item.path || ""}
      {item.env_key ? <span className="table-subtext">{item.env_key}</span> : null}
      {item.description ? <span className="table-subtext">{item.description}</span> : null}
      {(item.issues || []).map((issue) => (
        <span className="table-subtext warning-text" key={`${key}-${issue.code}`}>
          {issue.message} {issue.next_action}
        </span>
      ))}
    </>,
  ]);
  const envRows = Object.entries(configuredEnv).map(([key, item]) => [
    key,
    <span className={`badge ${runtimeStatusTone(item.status)}`}>{runtimeStatusLabel(item.status)}</span>,
    item.configured ? String(item.value || "") : String(item.note || item.description || ""),
  ]);
  const secretRows = Object.entries(sensitiveEnv).map(([key, item]) => [
    key,
    <span className={`badge ${runtimeStatusTone(item.status)}`}>{runtimeStatusLabel(item.status)}</span>,
    String(item.note || item.description || ""),
  ]);
  const kpis = [
    { label: "Build", value: compactId(data.build_id, 18) },
    { label: "Git", value: data.git?.head_short || "n/a" },
    { label: "Auth", value: authStatus.enabled ? "enabled" : "disabled", tone: authStatus.enabled ? "ok" : "warn" },
    { label: "Inference probes", value: `${healthyEndpoints.length}/${endpoints.length}` },
    {
      label: "Filesystem",
      value: filesystemSummary.alert_count ? `${filesystemSummary.error_count || 0} error / ${filesystemSummary.warning_count || 0} warn` : "ok",
      tone: filesystemSummary.error_count ? "danger" : filesystemSummary.warning_count ? "warn" : "ok",
    },
    { label: "API RSS", value: formatBytes(api.rss_bytes) },
  ];

  async function copyDiagnostics() {
    const redactEnv = (rows) => Object.fromEntries(
      Object.entries(rows || {}).map(([key, item]) => [
        key,
        {
          configured: !!item.configured,
          required: !!item.required,
          status: item.status || "unknown",
          satisfied_by: item.satisfied_by || [],
          note: item.note || item.description || "",
        },
      ])
    );
    const bundle = {
      generated_at_utc: data.generated_at_utc || null,
      build_id: data.build_id || null,
      git: data.git || {},
      filesystem_summary: filesystemSummary,
      filesystem_alerts: filesystemAlerts,
      paths,
      environment: {
        configured: redactEnv(configuredEnv),
        sensitive: redactEnv(sensitiveEnv),
        config_sources: configSources,
        notes: data.environment?.notes || [],
      },
      auth: authStatus,
      container_runtime: data.container_runtime || {},
      host_runtime: host,
      api_process_runtime: api,
      inference_endpoints: endpoints.map((endpoint) => ({
        endpoint_key: endpoint.endpoint_key,
        display_name: endpoint.display_name,
        provider: endpoint.provider,
        enabled: endpoint.enabled,
        base_url: endpoint.base_url,
        auth_mode: endpoint.auth_mode,
        api_key_configured: !!endpoint.api_key_configured,
        default_model: endpoint.default_model,
        model_count: endpoint.model_count,
        last_probe_status: endpoint.last_probe_status,
        last_probe_at: endpoint.last_probe_at,
        last_probe_error: endpoint.last_probe_error,
      })),
    };
    const text = JSON.stringify(bundle, null, 2);
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const textarea = document.createElement("textarea");
        textarea.value = text;
        textarea.setAttribute("readonly", "readonly");
        textarea.style.position = "fixed";
        textarea.style.left = "-9999px";
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand("copy");
        document.body.removeChild(textarea);
      }
      setCopyStatus("Copied redacted diagnostics.");
    } catch (error) {
      setCopyStatus(`Copy failed: ${String(error)}`);
    }
  }

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Runtime</h1>
          <p className="muted">Read-only host, container, path, auth, and configuration diagnostics.</p>
        </div>
        <div className="button-row">
          <button className="button" onClick={copyDiagnostics} disabled={!state.data}>Copy Diagnostics</button>
          <button className="button" onClick={loadRuntime}>{state.loading ? "Refreshing..." : "Refresh"}</button>
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
        {state.message} Generated: {formatDate(data.generated_at_utc)}
        {copyStatus ? <span className="table-subtext">{copyStatus}</span> : null}
      </div>

      {filesystemAlerts.length ? (
        <section className="panel runtime-alert-panel">
          <div className="panel-head">
            <div>
              <h2>Filesystem Alerts</h2>
              <p className="muted">Configured runtime targets with missing, inaccessible, unwritable, or unmounted destinations.</p>
            </div>
            <span className={`badge ${filesystemSummary.error_count ? "danger" : "warn"}`}>
              {formatInt(filesystemAlerts.length)} alert{filesystemAlerts.length === 1 ? "" : "s"}
            </span>
          </div>
          <div className="alert-list">
            {filesystemAlerts.map((alert) => (
              <div className={`alert-row ${alert.severity === "error" ? "danger" : "warn"}`} key={`${alert.path_key}-${alert.code}`}>
                <span className={`badge ${alert.severity === "error" ? "danger" : "warn"}`}>{alert.severity}</span>
                <div>
                  <strong>{alert.path_key}</strong>
                  <span className="table-subtext">{alert.env_key || "derived path"} · {alert.path}</span>
                  <span className="table-subtext">{alert.message}</span>
                  <span className="table-subtext"><strong>Next:</strong> {alert.next_action}</span>
                </div>
              </div>
            ))}
          </div>
        </section>
      ) : (
        <div className="status-line">Filesystem targets ok: {formatInt(filesystemSummary.configured_target_count)} configured targets checked.</div>
      )}

      <section className="runtime-grid">
        <div className="panel">
          <h2>Auth and OIDC</h2>
          <MetricList
            rows={[
              ["Auth", authStatus.enabled ? "enabled" : "disabled"],
              ["Provider", authStatus.provider || "n/a"],
              ["Issuer", authStatus.issuer || "n/a"],
              ["Redirect URI", authStatus.redirect_uri || "n/a"],
              ["Admin DB", authStatus.admin_db_path || "n/a"],
            ]}
          />
        </div>
        <div className="panel">
          <h2>Container and API Process</h2>
          <MetricList
            rows={[
              ["Container", data.container_runtime?.in_container ? "inside container" : "host process", data.container_runtime?.hostname || ""],
              ["Docker socket", data.container_runtime?.docker_socket_visible ? "visible" : "not mounted", data.container_runtime?.docker_status_note || ""],
              ["CPU load", `cores=${formatInt(host.cpu_count)}`, `1m=${formatFloat(host.loadavg_1m)} 5m=${formatFloat(host.loadavg_5m)} 15m=${formatFloat(host.loadavg_15m)}`],
              ["Host memory", `${formatBytes(Math.max(toNumber(host.mem_total_bytes) - toNumber(host.mem_available_bytes), 0))} used`, `${formatBytes(host.mem_available_bytes)} available of ${formatBytes(host.mem_total_bytes)}`],
              ["API process", `pid=${api.pid || "?"}, threads=${formatInt(api.threads)}`, `RSS=${formatBytes(api.rss_bytes)}, peak=${formatBytes(api.peak_rss_bytes)}`],
              ["Process IO", `read=${formatBytes(api.io_read_bytes)}, write=${formatBytes(api.io_write_bytes)}`],
            ]}
          />
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <div>
            <h2>Paths and Storage</h2>
            <p className="muted">Container-visible path checks. Missing host-only paths may simply not be mounted into the API container.</p>
          </div>
        </div>
        <KeyValueTable rows={pathRows} columns={["Key", "Health", "Type", "Access", "Disk", "Path"]} />
      </section>

      <section className="panel">
        <div className="panel-head">
          <div>
            <h2>Config Sources</h2>
            <p className="muted">Launcher-observed env files. Later files have higher precedence; values are not shown.</p>
          </div>
          <span className={`badge ${configSources.unreadable_count ? "danger" : "muted"}`}>
            {formatInt(configSources.loaded_count)} loaded / {formatInt(configSources.unreadable_count)} unreadable
          </span>
        </div>
        {configSourceRows.length ? (
          <>
            <KeyValueTable rows={configSourceRows} columns={["Order", "Status", "Role", "Path"]} />
            <div className="hint-list">
              {(configSources.notes || []).map((note) => <div key={note}>{note}</div>)}
            </div>
          </>
        ) : (
          <div className="empty">No config source metadata was passed into the API container. Start through the Spacegate launcher to populate this card.</div>
        )}
      </section>

      <section className="panel">
        <h2>Configured Environment</h2>
        <KeyValueTable rows={envRows} columns={["Key", "Status", "Value"]} />
      </section>

      <section className="panel">
        <h2>Secret Status</h2>
        <KeyValueTable rows={secretRows} columns={["Key", "Status", "Note"]} />
        <div className="hint-list">
          {(data.environment?.notes || []).map((note) => <div key={note}>{note}</div>)}
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <div>
            <h2>Inference Reachability</h2>
            <p className="muted">Last recorded probe state from the dynamic endpoint registry.</p>
          </div>
          <span className="badge">{healthyEndpoints.length}/{endpoints.length} healthy</span>
        </div>
        {endpoints.length ? (
          <table>
            <thead>
              <tr>
                <th>Endpoint</th>
                <th>Provider</th>
                <th>Status</th>
                <th>Models</th>
                <th>Auth</th>
                <th>Last probe</th>
              </tr>
            </thead>
            <tbody>
              {endpoints.map((endpoint) => (
                <tr key={endpoint.endpoint_id}>
                  <td>
                    <strong>{endpoint.display_name || endpoint.endpoint_key}</strong>
                    <span className="table-subtext">{endpoint.base_url}</span>
                  </td>
                  <td>{endpoint.provider || "n/a"}</td>
                  <td><span className={`badge ${endpoint.last_probe_status === "ok" ? "ok" : endpoint.enabled ? "warn" : "muted"}`}>{endpoint.last_probe_status || (endpoint.enabled ? "unprobed" : "disabled")}</span></td>
                  <td>{formatInt(endpoint.model_count)}</td>
                  <td>{endpoint.auth_mode || "none"}{endpoint.api_key_configured ? " / configured" : ""}</td>
                  <td>
                    {formatDate(endpoint.last_probe_at)}
                    {endpoint.last_probe_error ? <span className="table-subtext">{endpoint.last_probe_error}</span> : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No inference endpoints are registered.</div>
        )}
      </section>
    </div>
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
  const [evalReports, setEvalReports] = useState(null);
  const [credentialEnvs, setCredentialEnvs] = useState({ items: [], notes: [] });
  const [form, setForm] = useState(emptyEndpointForm);
  const [status, setStatus] = useState("Loading registry...");
  const [busyEndpoint, setBusyEndpoint] = useState(null);
  const [roleDrafts, setRoleDrafts] = useState({});
  const [smokeDrafts, setSmokeDrafts] = useState({});
  const [smokeResults, setSmokeResults] = useState({});

  const csrfHeaders = useMemo(() => {
    if (!csrf?.cookie_name) return {};
    const token = readCookie(csrf.cookie_name);
    return token ? { [csrf.header_name || "X-CSRF-Token"]: token } : {};
  }, [csrf]);

  async function loadInference() {
    setStatus("Refreshing registry...");
    const [endpointResult, statsResult, evalResult, credentialResult] = await Promise.all([
      fetchJson(`${ADMIN_API_BASE}/inference/endpoints`),
      fetchJson(`${ADMIN_API_BASE}/inference/stats`),
      fetchJson(`${ADMIN_API_BASE}/inference/eval-reports?limit=24`),
      fetchJson(`${ADMIN_API_BASE}/inference/credential-envs`),
    ]);
    if (!endpointResult.response.ok) {
      setStatus(compactError(endpointResult.data, endpointResult.response.status));
      return;
    }
    const nextEndpoints = Array.isArray(endpointResult.data.items) ? endpointResult.data.items : [];
    setEndpoints(nextEndpoints);
    setRoleDrafts((current) => {
      const next = { ...current };
      nextEndpoints.forEach((endpoint) => {
        if (!next[endpoint.endpoint_id]) next[endpoint.endpoint_id] = { ...(endpoint.role_defaults || {}) };
      });
      return next;
    });
    setSmokeDrafts((current) => {
      const next = { ...current };
      nextEndpoints.forEach((endpoint) => {
        if (!next[endpoint.endpoint_id]) {
          next[endpoint.endpoint_id] = {
            role: "discover",
            model_id: "",
            prompt: defaultSmokePrompt,
            temperature: 0,
            max_tokens: 32,
          };
        }
      });
      return next;
    });
    if (statsResult.response.ok) {
      setStats(Array.isArray(statsResult.data.items) ? statsResult.data.items : []);
    }
    if (evalResult.response.ok) {
      setEvalReports(evalResult.data);
    } else {
      setEvalReports({ reports: [], role_summary: [], anomaly_inbox: [], searched_dirs: [], error: compactError(evalResult.data, evalResult.response.status) });
    }
    if (credentialResult.response.ok) {
      setCredentialEnvs({
        items: Array.isArray(credentialResult.data.items) ? credentialResult.data.items : [],
        notes: Array.isArray(credentialResult.data.notes) ? credentialResult.data.notes : [],
      });
    } else {
      setCredentialEnvs({ items: [], notes: [`Credential envs: ${compactError(credentialResult.data, credentialResult.response.status)}`] });
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

  function updateRoleDefault(endpointId, role, modelId) {
    setRoleDrafts((current) => ({
      ...current,
      [endpointId]: {
        ...(current[endpointId] || {}),
        [role]: modelId,
      },
    }));
  }

  async function saveRoleDefaults(endpoint) {
    const roleDefaults = {};
    const draft = roleDrafts[endpoint.endpoint_id] || {};
    inferenceRoles.forEach((role) => {
      const modelId = normalizeOptional(draft[role]);
      if (modelId) roleDefaults[role] = modelId;
    });
    setBusyEndpoint(endpoint.endpoint_id);
    setStatus(`Saving role defaults for ${endpoint.display_name || endpoint.endpoint_key}...`);
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/inference/endpoints/${endpoint.endpoint_id}`, {
      method: "PATCH",
      headers: csrfHeaders,
      body: JSON.stringify({ role_defaults: roleDefaults }),
    });
    setBusyEndpoint(null);
    if (!response.ok) {
      setStatus(compactError(data, response.status));
      return;
    }
    setStatus("Role defaults saved.");
    await loadInference();
  }

  function updateSmokeDraft(endpointId, key, value) {
    setSmokeDrafts((current) => ({
      ...current,
      [endpointId]: {
        ...(current[endpointId] || {}),
        [key]: value,
      },
    }));
  }

  async function smokeTestEndpoint(endpoint) {
    const draft = smokeDrafts[endpoint.endpoint_id] || {};
    const payload = {
      role: draft.role || "discover",
      model_id: normalizeOptional(draft.model_id),
      prompt: normalizeOptional(draft.prompt) || defaultSmokePrompt,
      temperature: Number(draft.temperature ?? 0),
      max_tokens: Number.parseInt(String(draft.max_tokens || "32"), 10),
    };
    setBusyEndpoint(endpoint.endpoint_id);
    setStatus(`Running smoke test for ${endpoint.display_name || endpoint.endpoint_key}...`);
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/inference/endpoints/${endpoint.endpoint_id}/smoke-test`, {
      method: "POST",
      headers: csrfHeaders,
      body: JSON.stringify(payload),
    });
    setBusyEndpoint(null);
    if (!response.ok) {
      setStatus(`Smoke test failed: ${compactError(data, response.status)}`);
      await loadInference();
      return;
    }
    setSmokeResults((current) => ({ ...current, [endpoint.endpoint_id]: data }));
    setStatus(`Smoke test ok: ${data.model_id || "model"} | ${formatLatency(data.latency_ms)} | ${formatInt(data.usage?.total_tokens)} tokens`);
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
    const reportCount = Number(evalReports?.report_count || 0);
    return [
      { label: "Endpoints", value: endpoints.length },
      { label: "Enabled", value: enabled },
      { label: "Healthy probes", value: ready },
      { label: "Cached models", value: modelCount },
      { label: "Eval reports", value: reportCount },
    ];
  }, [endpoints, evalReports]);

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
        <EndpointForm form={form} updateForm={updateForm} createEndpoint={createEndpoint} credentialEnvs={credentialEnvs} />
        <EndpointList
          endpoints={endpoints}
          busyEndpoint={busyEndpoint}
          roleDrafts={roleDrafts}
          smokeDrafts={smokeDrafts}
          smokeResults={smokeResults}
          pollEndpoint={pollEndpoint}
          updateRoleDefault={updateRoleDefault}
          saveRoleDefaults={saveRoleDefaults}
          updateSmokeDraft={updateSmokeDraft}
          smokeTestEndpoint={smokeTestEndpoint}
          deleteEndpoint={deleteEndpoint}
        />
      </section>

      <InferenceEvalReports evalReports={evalReports} />
      <UsageStats stats={stats} />
    </div>
  );
}

function EndpointForm({ form, updateForm, createEndpoint, credentialEnvs }) {
  const credentialItems = Array.isArray(credentialEnvs?.items) ? credentialEnvs.items : [];
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
        <input list="credential-env-options" value={form.api_key_env} onChange={(event) => updateForm("api_key_env", event.target.value)} placeholder="SPACEGATE_OPENAI_API_KEY" />
        <datalist id="credential-env-options">
          {credentialItems.map((item) => (
            <option value={item.env_key} key={item.env_key} label={`${item.label} (${item.configured ? "configured" : "missing"})`} />
          ))}
        </datalist>
      </label>
      {credentialItems.length ? (
        <div className="credential-list">
          {credentialItems.map((item) => (
            <div className="credential-row" key={item.env_key}>
              <span className={`badge ${item.configured ? "ok" : "muted"}`}>{item.configured ? "configured" : "missing"}</span>
              <span>{item.env_key}</span>
              <span className="muted">{item.provider}</span>
            </div>
          ))}
          {(credentialEnvs.notes || []).map((note) => <div className="table-subtext" key={note}>{note}</div>)}
        </div>
      ) : null}
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

function EndpointList({
  endpoints,
  busyEndpoint,
  roleDrafts,
  smokeDrafts,
  smokeResults,
  pollEndpoint,
  updateRoleDefault,
  saveRoleDefaults,
  updateSmokeDraft,
  smokeTestEndpoint,
  deleteEndpoint,
}) {
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
            <RoleDefaultsEditor
              endpoint={endpoint}
              draft={roleDrafts[endpoint.endpoint_id] || endpoint.role_defaults || {}}
              busy={busyEndpoint === endpoint.endpoint_id}
              updateRoleDefault={updateRoleDefault}
              saveRoleDefaults={saveRoleDefaults}
            />
            <SmokeTestPanel
              endpoint={endpoint}
              draft={smokeDrafts[endpoint.endpoint_id] || {}}
              result={smokeResults[endpoint.endpoint_id]}
              busy={busyEndpoint === endpoint.endpoint_id}
              updateSmokeDraft={updateSmokeDraft}
              smokeTestEndpoint={smokeTestEndpoint}
            />
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

function RoleDefaultsEditor({ endpoint, draft, busy, updateRoleDefault, saveRoleDefaults }) {
  const models = Array.isArray(endpoint.models) ? endpoint.models : [];
  return (
    <details className="models role-defaults">
      <summary>Role Defaults</summary>
      <div className="role-grid">
        {inferenceRoles.map((role) => (
          <label key={role}>
            <span>{role}</span>
            <select value={draft[role] || ""} onChange={(event) => updateRoleDefault(endpoint.endpoint_id, role, event.target.value)}>
              <option value="">Endpoint default ({endpoint.default_model || "none"})</option>
              {draft[role] && !models.some((model) => model.model_id === draft[role]) ? (
                <option value={draft[role]}>{draft[role]}</option>
              ) : null}
              {models.map((model) => (
                <option value={model.model_id} key={model.model_id}>{model.model_id}</option>
              ))}
            </select>
          </label>
        ))}
      </div>
      <div className="card-actions">
        <button className="button" disabled={busy} onClick={() => saveRoleDefaults(endpoint)}>
          {busy ? "Saving..." : "Save Role Defaults"}
        </button>
      </div>
    </details>
  );
}

function SmokeTestPanel({ endpoint, draft, result, busy, updateSmokeDraft, smokeTestEndpoint }) {
  const models = Array.isArray(endpoint.models) ? endpoint.models : [];
  const role = draft.role || "discover";
  const roleDefault = (endpoint.role_defaults || {})[role] || "";
  return (
    <details className="models smoke-test">
      <summary>Smoke Test</summary>
      <div className="smoke-grid">
        <label>
          <span>Role</span>
          <select value={role} onChange={(event) => updateSmokeDraft(endpoint.endpoint_id, "role", event.target.value)}>
            {inferenceRoles.map((item) => <option value={item} key={item}>{item}</option>)}
          </select>
        </label>
        <label>
          <span>Model</span>
          <select value={draft.model_id || ""} onChange={(event) => updateSmokeDraft(endpoint.endpoint_id, "model_id", event.target.value)}>
            <option value="">Role/default ({roleDefault || endpoint.default_model || "none"})</option>
            {draft.model_id && !models.some((model) => model.model_id === draft.model_id) ? (
              <option value={draft.model_id}>{draft.model_id}</option>
            ) : null}
            {models.map((model) => (
              <option value={model.model_id} key={model.model_id}>{model.model_id}</option>
            ))}
          </select>
        </label>
        <label>
          <span>Temperature</span>
          <input type="number" min="0" max="2" step="0.1" value={draft.temperature ?? 0} onChange={(event) => updateSmokeDraft(endpoint.endpoint_id, "temperature", event.target.value)} />
        </label>
        <label>
          <span>Max tokens</span>
          <input type="number" min="1" max="512" value={draft.max_tokens || 32} onChange={(event) => updateSmokeDraft(endpoint.endpoint_id, "max_tokens", event.target.value)} />
        </label>
      </div>
      <label className="stacked-field">
        <span>Prompt</span>
        <textarea rows={3} value={draft.prompt || defaultSmokePrompt} onChange={(event) => updateSmokeDraft(endpoint.endpoint_id, "prompt", event.target.value)} />
      </label>
      <div className="card-actions">
        <button className="button" disabled={busy || !endpoint.enabled} onClick={() => smokeTestEndpoint(endpoint)}>
          {busy ? "Running..." : "Run Smoke Test"}
        </button>
      </div>
      {result ? (
        <div className="smoke-result">
          <MetricList
            rows={[
              ["Status", result.status || "n/a"],
              ["Role / model", `${result.role || "n/a"} / ${result.model_id || "n/a"}`],
              ["Latency", formatLatency(result.latency_ms)],
              ["Tokens", formatInt(result.usage?.total_tokens)],
            ]}
          />
          <pre className="json-box">{result.output_excerpt || ""}</pre>
        </div>
      ) : null}
    </details>
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

function InferenceEvalReports({ evalReports }) {
  const reports = Array.isArray(evalReports?.reports) ? evalReports.reports : [];
  const roleSummary = Array.isArray(evalReports?.role_summary) ? evalReports.role_summary : [];
  const anomalies = Array.isArray(evalReports?.anomaly_inbox) ? evalReports.anomaly_inbox : [];
  const searchedDirs = Array.isArray(evalReports?.searched_dirs) ? evalReports.searched_dirs : [];
  const sortedRoles = [...roleSummary].sort((a, b) => {
    const ai = inferenceRoles.indexOf(a.role);
    const bi = inferenceRoles.indexOf(b.role);
    return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi) || String(a.role).localeCompare(String(b.role));
  });
  return (
    <section className="panel inference-evals">
      <div className="panel-head">
        <div>
          <h2>Eval Report History</h2>
          <p className="muted">Read-only model suitability signals from agent eval reports. Eval output is experimental and does not mutate science layers.</p>
        </div>
        <span className="badge">{formatInt(evalReports?.report_count || reports.length)} reports</span>
      </div>
      {evalReports?.error ? <div className="status-line danger-line">{evalReports.error}</div> : null}
      <div className="report-chip-list">
        {searchedDirs.map((item) => (
          <span className={`badge ${item.exists ? "ok" : "muted"}`} key={item.path}>{item.exists ? "found" : "missing"}: {item.path}</span>
        ))}
      </div>
      <div className="inference-eval-grid">
        <div>
          <h3>Role Suitability</h3>
          {sortedRoles.length ? (
            <table>
              <thead>
                <tr>
                  <th>Role</th>
                  <th>Best candidate</th>
                  <th>Score</th>
                  <th>Schema</th>
                  <th>Cases</th>
                  <th>Latest run</th>
                </tr>
              </thead>
              <tbody>
                {sortedRoles.map((item) => {
                  const best = item.best_candidate || {};
                  const latest = item.latest_candidate || {};
                  return (
                    <tr key={item.role}>
                      <td><strong>{item.role}</strong></td>
                      <td>
                        {best.provider || "n/a"} / {best.model_id || "n/a"}
                        <span className="table-subtext">{best.report_id || "no report"}</span>
                      </td>
                      <td>{best.mean_score == null ? "n/a" : formatFloat(toNumber(best.mean_score, NaN), 3)}</td>
                      <td>{best.schema_valid_rate == null ? "n/a" : `${formatFloat(toNumber(best.schema_valid_rate, NaN) * 100, 1)}%`}</td>
                      <td>{formatInt(best.case_count)}</td>
                      <td>
                        {formatDate(latest.created_at)}
                        <span className="table-subtext">{latest.provider || "n/a"} / {latest.model_id || "n/a"}</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <div className="empty">No role-scored eval reports were found. Run `scripts/agent_eval.py run --provider local --model MODEL_ID --role extract` to generate report history.</div>
          )}
        </div>
        <div>
          <h3>Latest Reports</h3>
          {reports.length ? (
            <table>
              <thead>
                <tr>
                  <th>Report</th>
                  <th>Provider / model</th>
                  <th>Roles</th>
                  <th>Score</th>
                  <th>Anomalies</th>
                </tr>
              </thead>
              <tbody>
                {reports.slice(0, 10).map((report) => (
                  <tr key={report.report_id}>
                    <td>
                      <strong>{report.report_id}</strong>
                      <span className="table-subtext">{formatDate(report.created_at || report.mtime_utc)}</span>
                    </td>
                    <td>
                      {report.provider || "n/a"} / {report.model_id || "n/a"}
                      <span className="table-subtext">{report.prompt_version || "prompt n/a"}</span>
                    </td>
                    <td>{(report.roles || []).join(", ") || "n/a"}</td>
                    <td>{report.mean_score == null ? "n/a" : formatFloat(toNumber(report.mean_score, NaN), 3)}</td>
                    <td>{formatInt(report.anomaly_count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty">No eval reports were found in the searched report directories.</div>
          )}
        </div>
      </div>
      <details className="models">
        <summary>Anomaly Inbox ({formatInt(anomalies.length)})</summary>
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
          <div className="empty">No quarantined anomalies were found in the latest eval reports.</div>
        )}
      </details>
    </section>
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
