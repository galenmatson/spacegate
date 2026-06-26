import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const ADMIN_API_BASE = "/api/v2/admin";
const AUTH_API_BASE = "/api/v2/auth";
const OBJECT_RECENTS_KEY = "spacegate.admin.objectDiagnostics.recents";
const OBJECT_RECENTS_LIMIT = 8;

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

function formatMode(stat) {
  if (!stat) return "mode n/a";
  const owner = stat.owner || stat.uid;
  const group = stat.group || stat.gid;
  const flags = [
    stat.setuid ? "setuid" : null,
    stat.setgid ? "setgid" : null,
    stat.sticky ? "sticky" : null,
  ].filter(Boolean);
  return `${owner}:${group} ${stat.mode_octal || "????"}${flags.length ? ` ${flags.join(", ")}` : ""}`;
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

function compactBuildId(value) {
  const text = String(value || "");
  if (!text) return "n/a";
  const match = text.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})\d{2}Z/);
  if (match) return `${match[2]}/${match[3]} ${match[4]}:${match[5]}`;
  return compactId(text, 15);
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
  const [operationsJobRequest, setOperationsJobRequest] = useState("");
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

  function openOperationsJob(jobId) {
    const id = String(jobId || "").trim();
    if (!id) return;
    setOperationsJobRequest(id);
    setActiveScreen("operations");
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
          <button className={activeScreen === "objects" ? "active" : ""} onClick={() => setActiveScreen("objects")}>Objects</button>
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
          <BuildsScreen csrf={csrf} openOperationsJob={openOperationsJob} />
        ) : activeScreen === "dataset" ? (
          <DatasetScreen />
        ) : activeScreen === "objects" ? (
          <ObjectDiagnosticsScreen />
        ) : activeScreen === "inference" ? (
          <InferenceScreen csrf={csrf} />
        ) : activeScreen === "operations" ? (
          <OperationsScreen csrf={csrf} requestedJobId={operationsJobRequest} />
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
    { label: "Build", value: compactBuildId(state.status?.build_id) },
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

function BuildsScreen({ csrf, openOperationsJob }) {
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

  async function cancelJob(jobId) {
    if (!jobId) return;
    if (!window.confirm(`Cancel queued job ${jobId}?`)) return;
    setState((current) => ({ ...current, message: `Cancelling ${compactId(jobId)}...` }));
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/actions/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: "POST",
      headers,
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      setState((current) => ({ ...current, message: `Cancel failed: ${compactError(data, response.status)}` }));
      return;
    }
    setState((current) => ({ ...current, message: `Cancelled ${jobId}. Refreshing build state...` }));
    await loadBuilds();
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
  const snapshotControl = buildStatus.snapshot_control || operations.snapshot_control || {};
  const recentBuilds = Array.isArray(buildStatus.recent) ? buildStatus.recent : Array.isArray(legacyBuilds.recent) ? legacyBuilds.recent : [];
  const tmpBuilds = Array.isArray(buildStatus.tmp) ? buildStatus.tmp : Array.isArray(legacyBuilds.tmp) ? legacyBuilds.tmp : [];
  const currentBuild = buildStatus.current_build || recentBuilds.find((item) => item.build_id === served.build_id) || recentBuilds[0] || null;
  const verification = currentBuild?.verification || {};
  const snapshot = currentBuild?.snapshot || {};
  const coolness = currentBuild?.coolness || {};
  const pathHealth = buildStatus.path_health || {};
  const nextActions = Array.isArray(buildStatus.next_actions) ? buildStatus.next_actions : [];
  const operationJobs = operationJobsFromStatus(operations);
  const currentBuildId = currentBuild?.build_id || served.build_id || state.status?.build_id || "";
  const relatedBuildJobs = relatedJobsForBuild(operationJobs, currentBuildId);
  const verifyJob = latestJobForBuild(operationJobs, ["verify_build"], currentBuildId);
  const publishJob = latestJobForBuild(operationJobs, ["publish_db"], currentBuildId);
  const scoreJob = latestJobForBuild(operationJobs, ["score_coolness"], currentBuildId);
  const snapshotJob = latestJobForBuild(operationJobs, ["generate_snapshots"], currentBuildId);
  const retentionPlan = retention.dry_run || {};
  const retentionDryRunJob = retention.latest_matching_dry_run || null;
  const retentionApplyJob = latestJobByParams(operationJobs, "retention_apply", { candidate_hash: retentionPlan.candidate_hash });
  const retentionContext = {
    plan: retentionPlan,
    latestDryRun: retention.latest_matching_dry_run || null,
    applyReady: Boolean(retention.apply_ready),
  };
  const buildActions = ["build_database", "verify_build", "publish_db", "retention_dry_run", "retention_apply"]
    .map((name) => enrichBuildAction(actionsByName.get(name), retentionContext))
    .filter(Boolean);
  const kpis = [
    { label: "Served build", value: compactBuildId(state.status?.build_id || served.build_id) },
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
        <BuildOperationsPanel jobs={relatedBuildJobs} openOperationsJob={openOperationsJob} />
      </section>

      <section className="builds-grid">
        <PathHealthPanel paths={pathHealth} />
        <BuildArtifactPanel
          title="Served Build Artifacts"
          build={currentBuild}
          relatedJobs={[verifyJob, publishJob].filter(Boolean)}
          openOperationsJob={openOperationsJob}
        />
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
            <ActionCard action={action} key={action.name} runAction={runAction} busy={busyAction === action.name} snapshotControl={snapshotControl} />
          ))}
        </div>
      </section>

      <section className="builds-grid">
        <BuildVerificationPanel build={currentBuild} verifyJob={verifyJob} openOperationsJob={openOperationsJob} />
        <CoolnessReportPanel build={currentBuild} coolness={coolness} scoreJob={scoreJob} openOperationsJob={openOperationsJob} />
      </section>

      <section className="builds-grid">
        <SnapshotOperationsPanel
          snapshotControl={snapshotControl}
          snapshotJob={snapshotJob}
          openOperationsJob={openOperationsJob}
          cancelJob={cancelJob}
        />
        <SnapshotReportPanel build={currentBuild} snapshotJob={snapshotJob} scoreJob={scoreJob} openOperationsJob={openOperationsJob} />
      </section>

      <section className="builds-grid">
        <RecentBuildsPanel builds={recentBuilds} servedBuildId={served.build_id || state.status?.build_id} jobs={operationJobs} openOperationsJob={openOperationsJob} />
        <RetentionPlanPanel
          retention={retention}
          dryRunJob={retentionDryRunJob}
          applyJob={retentionApplyJob}
          openOperationsJob={openOperationsJob}
        />
      </section>

      <section className="builds-grid">
        <TempBuildsPanel builds={tmpBuilds} openOperationsJob={openOperationsJob} />
        <BuildReportsPanel build={currentBuild} />
      </section>
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
  if (["unknown", "", "null_result", "not_applicable"].includes(status)) return "muted";
  return "warn";
}

function operationJobsFromStatus(operations) {
  const buckets = [operations?.jobs?.active, operations?.jobs?.recent, operations?.jobs?.latest_failures];
  const seen = new Set();
  const out = [];
  buckets.flatMap((items) => Array.isArray(items) ? items : []).forEach((job) => {
    if (!job?.job_id || seen.has(job.job_id)) return;
    seen.add(job.job_id);
    out.push(job);
  });
  return out.sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || "")));
}

function jobParams(job) {
  return job?.params && typeof job.params === "object" ? job.params : {};
}

function jobMatchesBuild(job, buildId, { allowImplicit = true } = {}) {
  const id = String(buildId || "").trim();
  if (!id) return false;
  const params = jobParams(job);
  const explicit = String(params.build_id || "").trim();
  if (explicit) return explicit === id;
  if (allowImplicit && ["verify_build", "publish_db", "score_coolness", "generate_snapshots"].includes(String(job?.action || ""))) {
    return true;
  }
  return false;
}

function latestJobForBuild(jobs, actions, buildId, options = {}) {
  const actionSet = new Set(actions);
  return (jobs || []).find((job) => actionSet.has(String(job.action || "")) && jobMatchesBuild(job, buildId, options)) || null;
}

function latestJobByParams(jobs, action, params) {
  const expected = Object.entries(params || {}).filter(([, value]) => String(value || "").trim());
  if (!expected.length) return null;
  return (jobs || []).find((job) => {
    if (String(job.action || "") !== action) return false;
    const actual = jobParams(job);
    return expected.every(([key, value]) => String(actual[key] || "").trim() === String(value || "").trim());
  }) || null;
}

function relatedJobsForBuild(jobs, buildId) {
  const actions = new Set(["build_database", "build_database_slice", "verify_build", "publish_db", "score_coolness", "generate_snapshots", "retention_dry_run", "retention_apply"]);
  return (jobs || [])
    .filter((job) => actions.has(String(job.action || "")) && (jobMatchesBuild(job, buildId) || ["retention_dry_run", "retention_apply"].includes(String(job.action || ""))))
    .slice(0, 8);
}

function OperationJobButton({ job, openOperationsJob, label = "Open Job" }) {
  if (!job?.job_id) return null;
  return (
    <button className="button" type="button" onClick={() => openOperationsJob?.(job.job_id)}>
      {label}: {compactId(job.job_id, 18)}
    </button>
  );
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

function BuildArtifactPanel({ title, build, relatedJobs = [], openOperationsJob }) {
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
          {relatedJobs.length ? (
            <div className="log-toolbar">
              {relatedJobs.map((job) => (
                <OperationJobButton job={job} key={job.job_id} label={actionLabel(job.action)} openOperationsJob={openOperationsJob} />
              ))}
            </div>
          ) : null}
        </>
      ) : (
        <div className="empty">No build artifact summary is available yet.</div>
      )}
    </div>
  );
}

function BuildOperationsPanel({ jobs, openOperationsJob }) {
  const items = Array.isArray(jobs) ? jobs : [];
  return (
    <div className="panel">
      <h2>Build Operation Trail</h2>
      <p className="muted">Recent jobs connected to build, verification, presentation, publish, or retention operations.</p>
      {items.length ? (
        <div className="job-audit-list">
          {items.map((job) => (
            <div className="job-audit-row" key={job.job_id}>
              <span className={`badge ${jobStatusTone(job.status)}`}>{job.status}</span>
              <strong>{actionLabel(job.action)}</strong>
              <span>{formatDate(job.created_at)} | {job.error_message || job.job_id}</span>
              <button className="button" type="button" onClick={() => openOperationsJob?.(job.job_id)}>Open Job</button>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty">No recent build-related admin jobs were found in the Operations status window.</div>
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

function BuildVerificationPanel({ build, verifyJob, openOperationsJob }) {
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
          <div className="log-toolbar">
            <OperationJobButton job={verifyJob} label="Open Verify Job" openOperationsJob={openOperationsJob} />
          </div>
        </>
      ) : (
        <div className="empty">No build is selected for verification summary.</div>
      )}
    </div>
  );
}

function CoolnessReportPanel({ build, coolness, scoreJob, openOperationsJob }) {
  const item = coolness || {};
  return (
    <div className="panel">
      <h2>Coolness Report</h2>
      {build ? (
        <>
          <OverviewFact label="Status" value={<span className={`badge ${statusTone(item.status)}`}>{readableStatus(item.status)}</span>} />
          <OverviewFact label="Profile" value={`${item.profile_id || "n/a"} @ ${item.profile_version || "n/a"}`} />
          <OverviewFact label="Scored rows" value={formatInt(item.scored_rows)} />
          <div className="log-toolbar">
            <OperationJobButton job={scoreJob} label="Open Score Job" openOperationsJob={openOperationsJob} />
          </div>
          {!scoreJob ? <div className="empty">No recent Score Coolness job was found for this build in the Operations status window.</div> : null}
        </>
      ) : (
        <div className="empty">No build is selected for coolness report summary.</div>
      )}
    </div>
  );
}

function SnapshotOperationsPanel({ snapshotControl, snapshotJob, openOperationsJob, cancelJob }) {
  const control = snapshotControl || {};
  const progress = control.progress || {};
  const outputs = control.outputs || {};
  const estimate = control.estimate || {};
  const storage = control.storage || {};
  const job = control.job || snapshotJob || null;
  const percent = Math.max(0, Math.min(100, Number(progress.percent || 0)));
  const jobStatus = job?.status || control.status || "unknown";
  return (
    <div className="panel">
      <div className="panel-head">
        <div>
          <h2>Snapshot Operations Control</h2>
          <p className="muted">Monitor long snapshot renders, filesystem footprint, warnings, and safe cancellation state.</p>
        </div>
        <span className={`badge ${jobStatusTone(jobStatus)}`}>{jobStatus}</span>
      </div>
      <div className="snapshot-progress">
        <div className="snapshot-progress-head">
          <strong>{readableStatus(progress.stage || "unknown")}</strong>
          <span>{formatPct(percent)}</span>
        </div>
        <div className="progress-track">
          <div className="progress-fill" style={{ width: `${percent}%` }} />
        </div>
      </div>
      <div className="overview-facts compact">
        <OverviewFact label="Requested / processed" value={`${formatInt(progress.requested)} / ${formatInt(progress.processed)}`} />
        <OverviewFact label="Generated / reused" value={`${formatInt(progress.generated)} / ${formatInt(progress.reused)}`} />
        <OverviewFact label="Failed / skipped" value={`${formatInt(progress.failed)} / ${formatInt(progress.skipped)}`} />
        <OverviewFact label="Elapsed" value={control.elapsed_seconds === null || control.elapsed_seconds === undefined ? "n/a" : formatDurationMs(Number(control.elapsed_seconds) * 1000)} />
        <OverviewFact label="Output root" value={outputs.snapshot_root || "n/a"} />
        <OverviewFact label="Selected artifact size" value={formatBytes(outputs.selected_artifact_size_bytes || outputs.snapshot_root_size_bytes || storage.output_size_bytes)} />
        <OverviewFact label="Estimated run size" value={estimate.estimated_bytes ? formatBytes(estimate.estimated_bytes) : "n/a"} />
        <OverviewFact label="Bulk root visible" value={storage.bulk_dir ? `${runtimeStatusLabel(storage.bulk_dir.status)} (${storage.bulk_dir.path})` : "n/a"} />
      </div>
      {Array.isArray(control.safety_warnings) && control.safety_warnings.length ? (
        <div className="trap-list">
          {control.safety_warnings.map((warning) => <div key={warning}>{warning}</div>)}
        </div>
      ) : (
        <div className="status-line">No snapshot safety warnings for the selected/latest snapshot job.</div>
      )}
      {control.latest_error ? <div className="status-line danger-line">Latest error: {control.latest_error}</div> : null}
      {control.latest_warning ? <div className="status-line">Latest warning: {control.latest_warning}</div> : null}
      <div className="hint-list">
        <div><strong>Cancellation:</strong> {control.cancellation?.can_cancel_selected_job ? "queued job can be cancelled safely" : control.cancellation?.manual_stop_note || "queued-only cancellation is supported"}</div>
        {storage.output_parent ? <div><strong>Output parent:</strong> {runtimeStatusLabel(storage.output_parent.status)} | {storage.output_parent.path}</div> : null}
      </div>
      <div className="card-actions">
        <OperationJobButton job={job} label="Open Snapshot Job" openOperationsJob={openOperationsJob} />
        {control.cancellation?.can_cancel_selected_job ? (
          <button className="button danger" type="button" onClick={() => cancelJob?.(job?.job_id)}>Cancel Queued Job</button>
        ) : null}
      </div>
    </div>
  );
}

function SnapshotReportPanel({ build, snapshotJob, scoreJob, openOperationsJob }) {
  const snapshot = build?.snapshot || {};
  return (
    <div className="panel">
      <h2>Snapshot Report</h2>
      {build ? (
        <>
          <OverviewFact label="Status" value={<span className={`badge ${statusTone(snapshot.status)}`}>{readableStatus(snapshot.status)}</span>} />
          <OverviewFact label="Requested / generated / reused" value={`${formatInt(snapshot.requested)} / ${formatInt(snapshot.generated)} / ${formatInt(snapshot.reused)}`} />
          <OverviewFact label="Failed / skipped" value={`${formatInt(snapshot.failed)} / ${formatInt(snapshot.skipped)}`} />
          <OverviewFact label="Manifest rows upserted" value={formatInt(snapshot.manifest_rows_upserted)} />
          <OverviewFact label="Generated at" value={formatDate(snapshot.generated_at)} />
          <OverviewFact label="Generator / view" value={`${snapshot.generator_version || "n/a"} / ${snapshot.view_type || "n/a"}`} />
          <OverviewFact label="Params hash" value={snapshot.params_hash || "n/a"} />
          <OverviewFact label="Snapshot root" value={snapshot.snapshot_root || "n/a"} />
          <OverviewFact label="Selected artifact size" value={formatBytes(snapshot.selected_artifact_size_bytes || snapshot.snapshot_root_size_bytes)} />
          <OverviewFact label="Manifest parquet" value={snapshot.manifest_parquet || "n/a"} />
          {snapshot.null_result ? (
            <div className="status-line">Null result recorded: zero requested, generated, reused, and manifest-upserted snapshot rows.</div>
          ) : null}
          {snapshot.parse_error ? <div className="trap-list"><div>{snapshot.parse_error}</div></div> : null}
          <div className="log-toolbar">
            <OperationJobButton job={snapshotJob} label="Open Snapshot Job" openOperationsJob={openOperationsJob} />
            <OperationJobButton job={scoreJob} label="Open Score Job" openOperationsJob={openOperationsJob} />
          </div>
        </>
      ) : (
        <div className="empty">No build is selected for snapshot report summary.</div>
      )}
    </div>
  );
}

function RecentBuildsPanel({ builds, servedBuildId, jobs, openOperationsJob }) {
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
              <th>Job</th>
              <th>Size</th>
            </tr>
          </thead>
          <tbody>
            {builds.map((build) => {
              const job = latestJobForBuild(jobs, ["verify_build", "publish_db", "score_coolness", "generate_snapshots"], build.build_id, { allowImplicit: build.build_id === servedBuildId });
              return (
                <tr key={build.build_id}>
                  <td>
                    <strong>{build.build_id}</strong>
                    <span className="table-subtext">{build.build_id === servedBuildId ? "served/current" : formatDate(build.mtime_utc)}</span>
                  </td>
                  <td><span className={`badge ${statusTone(build.verification?.status)}`}>{readableStatus(build.verification?.status)}</span></td>
                  <td><span className={`badge ${statusTone(build.snapshot?.status)}`}>{readableStatus(build.snapshot?.status)}</span></td>
                  <td>{formatInt(build.reports?.count || 0)}</td>
                  <td>{build.promotable ? "core+arm" : `missing ${(build.missing_required || []).join(", ")}`}</td>
                  <td><OperationJobButton job={job} label={actionLabel(job?.action)} openOperationsJob={openOperationsJob} /></td>
                  <td>{formatBytes(build.size_bytes)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      ) : (
        <div className="empty">No immutable build directories were found under the state out directory.</div>
      )}
    </div>
  );
}

function TempBuildsPanel({ builds, openOperationsJob }) {
  return (
    <div className="panel">
      <h2>Temporary Outputs</h2>
      <p className="muted">Temporary ingest outputs are useful for failure diagnosis. Do not prune them until the root cause is captured.</p>
      {builds.length ? (
        <table>
          <thead>
            <tr><th>Name</th><th>Modified</th><th>Size</th><th>Failure Job</th></tr>
          </thead>
          <tbody>
            {builds.map((build) => (
              <tr key={build.name}>
                <td>{build.name}</td>
                <td>{formatDate(build.mtime_utc)}</td>
                <td>{formatBytes(build.size_bytes)}</td>
                <td><OperationJobButton job={build.related_failed_job} label="Open" openOperationsJob={openOperationsJob} /></td>
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

function RetentionPlanPanel({ retention, dryRunJob, applyJob, openOperationsJob }) {
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
      <div className="log-toolbar">
        <OperationJobButton job={dryRunJob} label="Open Dry-Run Job" openOperationsJob={openOperationsJob} />
        <OperationJobButton job={applyJob} label="Open Apply Job" openOperationsJob={openOperationsJob} />
      </div>
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
  const planetEnvironment = breakdowns.planet_environment_coverage || {};
  const planetEnvironmentExamples = breakdowns.planet_environment_gap_examples || [];
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
        <h2>Planet Environment Coverage</h2>
        <MetricList
          rows={[
            ["Source or derivable", `${formatInt(planetEnvironment.source_or_derivable_count)} / ${formatInt(planetEnvironment.total_planets)}`, `${formatPct(planetEnvironment.source_or_derivable_pct)} coverage`],
            ["Source equilibrium temp", formatInt(planetEnvironment.source_eq_temp_count)],
            ["Source insolation only", formatInt(planetEnvironment.source_insolation_only_count)],
            ["Proxy derivable", formatInt(planetEnvironment.proxy_derivable_count), "stellar class + semi-major axis"],
            ["Missing environment", `${formatInt(planetEnvironment.missing_environment_count)} (${formatPct(planetEnvironment.missing_pct)})`],
            ["Broad HZ environment", formatInt(planetEnvironment.broad_hz_environment_count)],
            ["Nice-planet-like", formatInt(planetEnvironment.nice_planet_like_count), "broad HZ + mass/eccentricity filters"],
          ]}
        />
        <details className="object-details">
          <summary>Gap examples</summary>
          {planetEnvironmentExamples.length ? (
            <table>
              <thead><tr><th>Planet</th><th>Reason</th><th>Source</th><th>Host spectral</th></tr></thead>
              <tbody>
                {planetEnvironmentExamples.map((row) => (
                  <tr key={`${row.stable_object_key}-${row.gap_reason}`}>
                    <td>{row.planet_name || row.stable_object_key || "n/a"}</td>
                    <td>{readableStatus(row.gap_reason)}</td>
                    <td>{row.source_catalog || "n/a"}</td>
                    <td>{[row.spectral_class, row.luminosity_class, row.spectral_type_raw].filter(Boolean).join(" / ") || "n/a"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : <div className="empty">No planet environment coverage gaps were sampled.</div>}
        </details>
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

function ObjectDiagnosticsScreen() {
  const [query, setQuery] = useState("");
  const [activeTab, setActiveTab] = useState("overview");
  const [selectedObject, setSelectedObject] = useState(null);
  const [recentObjects, setRecentObjects] = useState(() => loadObjectRecents());
  const [state, setState] = useState({
    loadingSearch: false,
    loadingDetail: false,
    search: null,
    detail: null,
    message: "Search for a system, alias, catalog id, or stable object key.",
  });

  async function runSearch(event = null) {
    if (event) event.preventDefault();
    setState((current) => ({ ...current, loadingSearch: true, message: "Searching objects..." }));
    const params = new URLSearchParams();
    if (String(query || "").trim()) params.set("q", String(query).trim());
    params.set("limit", "25");
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/objects/search?${params.toString()}`);
    if (!response.ok) {
      setState((current) => ({ ...current, loadingSearch: false, search: null, message: `Object search: ${compactError(data, response.status)}` }));
      return;
    }
    setState((current) => ({ ...current, loadingSearch: false, search: data, message: `Found ${formatInt((data.items || []).length)} system candidate(s).` }));
    const first = Array.isArray(data.items) ? data.items[0] : null;
    if (first?.system_id) {
      await loadDetail(first.system_id, first.diagnostic_focus);
    }
  }

  async function loadDetail(systemId, focus = null) {
    if (!systemId) return;
    setState((current) => ({ ...current, loadingDetail: true, message: `Loading diagnostics for system ${systemId}...` }));
    const { response, data } = await fetchJson(`${ADMIN_API_BASE}/objects/systems/${encodeURIComponent(systemId)}`);
    if (!response.ok) {
      setState((current) => ({ ...current, loadingDetail: false, detail: null, message: `Object detail: ${compactError(data, response.status)}` }));
      return;
    }
    const loadedSystem = data?.public?.system || {};
    const nextObject = resolveObjectFocus(data, focus) || { type: "system", data: loadedSystem };
    setSelectedObject(nextObject);
    rememberObjectRecent(data, nextObject);
    setState((current) => ({ ...current, loadingDetail: false, detail: data, message: "Ready" }));
  }

  function rememberObjectRecent(detail, object) {
    const entry = objectRecentEntry(detail, object);
    if (!entry) return;
    setRecentObjects((current) => {
      const next = [
        entry,
        ...current.filter((item) => item.key !== entry.key),
      ].slice(0, OBJECT_RECENTS_LIMIT);
      saveObjectRecents(next);
      return next;
    });
  }

  function handleSelectObject(object) {
    setSelectedObject(object);
    rememberObjectRecent(state.detail, object);
  }

  function clearObjectRecents() {
    saveObjectRecents([]);
    setRecentObjects([]);
  }

  useEffect(() => {
    runSearch();
  }, []);

  const items = state.search?.items || [];
  const detail = state.detail || {};
  const publicPayload = detail.public || {};
  const system = publicPayload.system || {};
  const diagnostics = detail.diagnostics || {};
  const readiness = diagnostics.readiness || [];
  const stars = publicPayload.stars || [];
  const planets = publicPayload.planets || [];
  const disc = diagnostics.disc || {};
  const arm = diagnostics.arm || {};
  const simulationReadiness = diagnostics.simulation_readiness || {};
  const provenance = diagnostics.provenance || {};
  const kpis = [
    { label: "System", value: system.display_name || system.system_name || compactId(system.stable_object_key, 22) },
    { label: "Stars", value: formatInt(system.star_count ?? stars.length) },
    { label: "Planets", value: formatInt(system.planet_count ?? planets.length) },
    { label: "Orbit solutions", value: formatInt(arm.orbital_solutions?.count), tone: arm.orbital_solutions?.count ? "ok" : "warn" },
  ];

  return (
    <div className="screen">
      <header className="page-header">
        <div>
          <h1>Object Diagnostics</h1>
          <p className="muted">Layer-aware lookup for core identity, provenance, arm graph/orbits, disc presentation, and public artifact state.</p>
        </div>
      </header>

      <form className="object-search" onSubmit={runSearch}>
        <label>
          <span>System, component, alias, catalog id, or stable key</span>
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Sol, Mars, Ganymede, Gaia 4472832130942575872, system 1" />
        </label>
        <button className="button primary" type="submit">{state.loadingSearch ? "Searching..." : "Search"}</button>
      </form>

      <ObjectRecentList items={recentObjects} onOpen={(item) => loadDetail(item.system_id, item.focus)} onClear={clearObjectRecents} />

      <div className={state.message === "Ready" ? "status-line" : "status-line"}>{state.message}</div>

      <section className="object-layout">
        <div className="panel">
          <h2>Search Results</h2>
          {items.length ? (
            <div className="select-list">
              {items.map((item) => (
                <button
                  className={`select-row ${item.system_id === system.system_id ? "selected" : ""}`}
                  key={objectSearchResultKey(item)}
                  onClick={() => loadDetail(item.system_id, item.diagnostic_focus)}
                  type="button"
                >
                  <strong>{item.object_match?.label || item.display_name || item.system_name || `System ${item.system_id}`}</strong>
                  <span>
                    {item.object_match
                      ? `${item.object_match.component_type || item.object_match.type} in ${item.display_name || item.system_name || `System ${item.system_id}`}`
                      : compactId(item.stable_object_key, 34)}
                    {" | "}{formatFloat(item.dist_ly, 2)} ly | {formatInt(item.star_count)} stars | {formatInt(item.planet_count)} planets
                  </span>
                </button>
              ))}
            </div>
          ) : (
            <div className="empty">No search results yet. Try a common name, moon/planet name, Gaia/HD/HIP id, or `system 123`.</div>
          )}
        </div>

        <div className="panel">
          <h2>Readiness</h2>
          {readiness.length ? (
            <div className="readiness-list">
              {readiness.map((item) => (
                <div className="readiness-row" key={item.key}>
                  <span className={`badge ${statusTone(item.status)}`}>{readableStatus(item.status)}</span>
                  <div>
                    <div className="readiness-title">
                      <strong>{item.label}</strong>
                      {item.workspace ? <span className="badge muted">{item.workspace}</span> : null}
                    </div>
                    <span>{item.detail}</span>
                    {item.why ? <span><strong>Why:</strong> {item.why}</span> : null}
                    {item.next_action ? <span><strong>Next:</strong> {item.next_action}</span> : null}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="empty">Select an object to see diagnostics.</div>
          )}
        </div>
      </section>

      {state.detail ? (
        <>
          <div className="kpi-row">
            {kpis.map((item) => (
              <div className={`kpi ${item.tone || ""}`} key={item.label}>
                <span>{item.label}</span>
                <strong>{item.value}</strong>
              </div>
            ))}
          </div>

          <ObjectFocusPanel object={selectedObject || { type: "system", data: system }} arm={arm} simulation={simulationReadiness} onSelectObject={handleSelectObject} />

          <div className="tab-row">
            {[
              ["overview", "Overview"],
              ["layers", "Layers"],
              ["members", "Members"],
              ["graph", "Graph / Orbits"],
              ["simulation", "Simulation"],
              ["presentation", "Presentation"],
              ["raw", "Raw JSON"],
            ].map(([key, label]) => (
              <button className={activeTab === key ? "active" : ""} key={key} onClick={() => setActiveTab(key)}>{label}</button>
            ))}
          </div>

          {activeTab === "overview" ? (
            <ObjectOverviewTab detail={detail} />
          ) : activeTab === "layers" ? (
            <ObjectLayersTab detail={detail} />
          ) : activeTab === "members" ? (
            <ObjectMembersTab stars={stars} planets={planets} arm={arm} simulation={simulationReadiness} selectedObject={selectedObject} onSelectObject={handleSelectObject} />
          ) : activeTab === "graph" ? (
            <ObjectGraphTab arm={arm} hierarchy={publicPayload.hierarchy} system={system} selectedObject={selectedObject} onSelectObject={handleSelectObject} />
          ) : activeTab === "simulation" ? (
            <ObjectSimulationTab simulation={simulationReadiness} />
          ) : activeTab === "presentation" ? (
            <ObjectPresentationTab disc={disc} system={system} />
          ) : (
            <section className="panel">
              <h2>Raw Object Diagnostics JSON</h2>
              <pre className="json-box tall">{jsonBlock(detail)}</pre>
            </section>
          )}
        </>
      ) : null}
    </div>
  );
}

function loadObjectRecents() {
  try {
    const parsed = JSON.parse(window.localStorage?.getItem(OBJECT_RECENTS_KEY) || "[]");
    return Array.isArray(parsed) ? parsed.filter((item) => item?.system_id && item?.label).slice(0, OBJECT_RECENTS_LIMIT) : [];
  } catch (_) {
    return [];
  }
}

function objectSearchResultKey(item) {
  const focus = item?.diagnostic_focus || {};
  return `${item?.system_id || "system"}:${focus.type || "system"}:${focus.key || focus.id || ""}`;
}

function saveObjectRecents(items) {
  try {
    window.localStorage?.setItem(OBJECT_RECENTS_KEY, JSON.stringify(items || []));
  } catch (_) {
    // Recent-object persistence is only an operator convenience.
  }
}

function objectFocusToken(object) {
  const type = object?.type || "system";
  const data = object?.data || {};
  if (type === "star" && data.star_id) return { type, id: Number(data.star_id) };
  if (type === "planet" && data.planet_id) return { type, id: Number(data.planet_id) };
  if (type === "component" && data.stable_component_key) return { type, key: String(data.stable_component_key) };
  return { type: "system" };
}

function objectRecentEntry(detail, object) {
  const system = detail?.public?.system || {};
  if (!system.system_id) return null;
  const type = object?.type || "system";
  const data = object?.data || system;
  const focus = objectFocusToken(object);
  const label = objectDisplayName(type, data);
  const systemLabel = system.display_name || system.system_name || `System ${system.system_id}`;
  const sublabel = type === "system"
    ? `${compactId(system.stable_object_key, 34)}`
    : `${actionLabel(type)} in ${systemLabel}`;
  const focusKey = focus.key || focus.id || "system";
  return {
    key: `${system.system_id}:${focus.type}:${focusKey}`,
    system_id: Number(system.system_id),
    system_label: systemLabel,
    label,
    sublabel,
    focus,
    saved_at: new Date().toISOString(),
  };
}

function ObjectRecentList({ items, onOpen, onClear }) {
  const listRef = useRef(null);
  const [visibleLimit, setVisibleLimit] = useState(OBJECT_RECENTS_LIMIT);
  useEffect(() => {
    const node = listRef.current;
    if (!node) return undefined;
    const updateLimit = () => {
      const width = node.getBoundingClientRect().width || 0;
      const cardMinWidth = 180;
      const gapPx = 8;
      const count = Math.max(1, Math.floor((width + gapPx) / (cardMinWidth + gapPx)));
      setVisibleLimit(Math.min(OBJECT_RECENTS_LIMIT, count));
    };
    updateLimit();
    if (typeof ResizeObserver === "undefined") {
      window.addEventListener("resize", updateLimit);
      return () => window.removeEventListener("resize", updateLimit);
    }
    const observer = new ResizeObserver(updateLimit);
    observer.observe(node);
    return () => observer.disconnect();
  }, [items.length]);
  if (!items.length) return null;
  const visibleItems = items.slice(0, visibleLimit);
  return (
    <section className="panel object-recent-panel">
      <div className="panel-head">
        <div>
          <h2>Recent Objects</h2>
          <p className="muted">Quick return links stored in this browser.</p>
        </div>
        <button className="button" type="button" onClick={onClear}>Clear</button>
      </div>
      <div className="object-recent-list" ref={listRef}>
        {visibleItems.map((item) => (
          <button className="object-recent-row" key={item.key} type="button" onClick={() => onOpen?.(item)}>
            <strong>{item.label}</strong>
            <span>{item.sublabel}</span>
          </button>
        ))}
      </div>
    </section>
  );
}

function resolveObjectFocus(detail, focus) {
  const wantedType = String(focus?.type || "");
  const wantedId = Number(focus?.id);
  const wantedKey = String(focus?.key || "");
  if (!wantedType) return null;
  if (wantedType === "star") {
    if (!Number.isFinite(wantedId)) return null;
    const star = (detail?.public?.stars || []).find((row) => Number(row.star_id) === wantedId);
    return star ? { type: "star", data: { ...star, arm_component: componentForCoreObject(detail?.diagnostics?.arm || {}, "star", star.star_id) } } : null;
  }
  if (wantedType === "planet") {
    if (!Number.isFinite(wantedId)) return null;
    const planet = (detail?.public?.planets || []).find((row) => Number(row.planet_id) === wantedId);
    return planet ? { type: "planet", data: { ...planet, arm_component: componentForCoreObject(detail?.diagnostics?.arm || {}, "planet", planet.planet_id) } } : null;
  }
  if (wantedType === "component" && wantedKey) {
    const component = (detail?.diagnostics?.arm?.components?.items || []).find((row) => String(row.stable_component_key || "") === wantedKey);
    return component ? { type: "component", data: component } : null;
  }
  return null;
}

function ObjectFocusPanel({ object, arm, simulation, onSelectObject }) {
  const type = object?.type || "system";
  const data = object?.data || {};
  const name = objectDisplayName(type, data);
  const componentKey = focusComponentKey(object, arm);
  const relations = objectComponentRelations(componentKey, arm);
  const idValue = type === "star"
    ? data.star_id
    : type === "planet"
      ? data.planet_id
      : type === "component"
        ? compactId(data.stable_component_key, 24)
        : data.system_id;
  return (
    <section className="panel object-focus-panel">
      <div className="panel-head">
        <div>
          <h2>{actionLabel(type)} Diagnostics Focus</h2>
          <p className="muted">{name}</p>
        </div>
        <span className="badge muted">{type} {idValue || ""}</span>
      </div>
      <div className="object-focus-grid">
        <ObjectKeyValueTable payload={objectFocusSummary(type, data, simulation)} />
        <div>
          <h3>Provenance</h3>
          <ObjectKeyValueTable payload={data.provenance || {}} />
        </div>
      </div>
      {componentKey ? (
        <div className="object-focus-grid">
          <ObjectRelationList
            title="Contained Components"
            rows={relations.children}
            empty="No child components are linked to this object in arm.system_hierarchy_edges."
            onSelectObject={onSelectObject}
          />
          <ObjectRelationList
            title="Containing Components"
            rows={relations.parents}
            empty="No parent component is linked to this object in arm.system_hierarchy_edges."
            onSelectObject={onSelectObject}
          />
        </div>
      ) : null}
    </section>
  );
}

function objectFocusSummary(type, data, simulation = null) {
  if (type === "component") {
    return {
      display_name: data.display_name,
      stable_component_key: data.stable_component_key,
      component_type: data.component_type,
      core_object_type: data.core_object_type,
      core_object_id: data.core_object_id,
      catalog_component_label: data.catalog_component_label,
      source_catalog: data.source_catalog,
      source_version: data.source_version,
      source_pk: data.source_pk,
    };
  }
  if (type === "star") {
    return {
      display_name: data.display_name || data.star_name,
      star_id: data.star_id,
      stable_object_key: data.stable_object_key,
      system_id: data.system_id,
      component: data.component,
      spectral_class: data.spectral_class || data.spectral_type,
      teff_k: data.teff_k,
      mass_msun: data.mass_msun,
      radius_rsun: data.radius_rsun,
      luminosity_lsun: data.luminosity_lsun,
      arm_catalogs: data.arm_catalogs,
      arm_component_key: data.arm_component?.stable_component_key,
      source_catalog: data.source_catalog,
    };
  }
  if (type === "planet") {
    const environment = data.environment_evidence || {};
    const resolvedSma = resolvedSimulationField(simulation, "planet", data.planet_id, "semi_major_axis_au", data.semi_major_axis_au);
    const resolvedInsol = resolvedSimulationField(simulation, "planet", data.planet_id, "candidate_insol_earth", data.insol_earth);
    const resolvedEqTemp = resolvedSimulationField(simulation, "planet", data.planet_id, "candidate_eq_temp_k", data.eq_temp_k);
    return {
      planet_name: data.planet_name,
      planet_id: data.planet_id,
      stable_object_key: data.stable_object_key,
      system_id: data.system_id,
      orbital_period_days: data.orbital_period_days,
      semi_major_axis_au: data.semi_major_axis_au,
      resolved_semi_major_axis_au: resolvedFieldSummary(resolvedSma, 5),
      radius_earth: data.radius_earth,
      mass_earth: data.mass_earth,
      insol_earth: data.insol_earth,
      resolved_insol_earth: resolvedFieldSummary(resolvedInsol, 3),
      eq_temp_k: data.eq_temp_k,
      resolved_eq_temp_k: resolvedFieldSummary(resolvedEqTemp, 1),
      environment_evidence_basis: environment.evidence_basis,
      candidate_eq_temp_k: environment.candidate_eq_temp_k,
      candidate_insol_earth: environment.candidate_insol_earth,
      broad_hz_candidate: environment.broad_hz_candidate,
      nice_planet_candidate: environment.nice_planet_candidate,
      environment_missing_reason: environment.missing_reason,
      discovery_method: data.discovery_method,
      lifecycle_status: data.lifecycle_status,
      arm_component_key: data.arm_component?.stable_component_key,
      source_catalog: data.source_catalog,
    };
  }
  return {
    display_name: data.display_name || data.system_name,
    system_id: data.system_id,
    stable_object_key: data.stable_object_key,
    distance_ly: data.dist_ly,
    distance_pc: data.dist_pc,
    ra_deg: data.ra_deg,
    dec_deg: data.dec_deg,
    grouping_basis: data.grouping_basis,
    grouping_confidence: data.grouping_confidence,
    star_count: data.star_count,
    planet_count: data.planet_count,
    arm_component_key: data.arm_component?.stable_component_key,
  };
}

function hasObjectScalarValue(value) {
  if (value === null || value === undefined || value === "") return false;
  if (typeof value === "number") return Number.isFinite(value);
  return true;
}

function simulationRowsForType(simulation, objectType) {
  if (objectType === "planet") return Array.isArray(simulation?.planets) ? simulation.planets : [];
  if (objectType === "star") return Array.isArray(simulation?.stars) ? simulation.stars : [];
  return [];
}

function simulationRowForObject(simulation, objectType, objectId) {
  const numericId = Number(objectId);
  if (!Number.isFinite(numericId)) return null;
  return simulationRowsForType(simulation, objectType).find((row) => Number(row.object_id) === numericId) || null;
}

function simulationFieldForObject(simulation, objectType, objectId, fieldKey) {
  const row = simulationRowForObject(simulation, objectType, objectId);
  return (row?.fields || []).find((field) => String(field.key || "") === String(fieldKey || "")) || null;
}

function resolvedSimulationField(simulation, objectType, objectId, fieldKey, sourceValue) {
  if (hasObjectScalarValue(sourceValue)) return null;
  const field = simulationFieldForObject(simulation, objectType, objectId, fieldKey);
  if (!field || !hasObjectScalarValue(field.value)) return null;
  if (String(field.status || "") !== "derived" || String(field.layer || "") !== "arm") return null;
  return field;
}

function resolvedFieldSummary(field, digits = 3) {
  if (!field) return null;
  return {
    value: formatResolvedScienceNumber(field.value, digits, field.unit),
    status: field.status || "derived",
    layer: field.layer || "arm",
    confidence_tier: field.confidence_tier || "unknown",
    basis: field.basis || "n/a",
    replacement_target: field.replacement_target || "n/a",
  };
}

function formatResolvedScienceNumber(value, digits = 3, unit = "") {
  const number = Number(value);
  if (!Number.isFinite(number)) return "n/a";
  const text = formatFloat(number, digits);
  return unit ? `${text} ${unit}` : text;
}

function ResolvedScienceValue({ sourceValue, resolvedField, digits = 3, unit = "", details = true }) {
  if (hasObjectScalarValue(sourceValue)) {
    return <>{formatResolvedScienceNumber(sourceValue, digits, unit)}</>;
  }
  if (!resolvedField) return <>n/a</>;
  return (
    <span className="resolved-science-value">
      <strong>{formatResolvedScienceNumber(resolvedField.value, digits, resolvedField.unit || unit)}</strong>
      {details ? <span className="badge muted">derived</span> : null}
      {details ? (
        <span className="table-subtext">
          {resolvedField.layer || "arm"} | {resolvedField.confidence_tier || "unknown"} | {resolvedField.basis || "n/a"}
        </span>
      ) : null}
    </span>
  );
}

function objectDisplayName(type, data) {
  if (type === "component") return componentDisplayLabel(data);
  return data.display_name || data.system_name || data.star_name || data.planet_name || data.stable_object_key || "Selected object";
}

function componentDisplayLabel(component) {
  const displayName = String(component?.display_name || "").trim();
  if (displayName) return displayName;
  const label = String(component?.catalog_component_label || "").trim();
  if (label) return label;
  const key = String(component?.stable_component_key || "").trim();
  if (!key) return "Component";
  const tail = key.split(":").filter(Boolean).pop() || key;
  return actionLabel(tail.replaceAll("-", "_"));
}

function componentTypeRank(value) {
  const ranks = {
    system: 0,
    star: 1,
    main_sequence: 1,
    brown_dwarf: 2,
    compact: 2,
    planet: 3,
    subplanet: 4,
    moon: 5,
    minor_body: 6,
    artificial: 7,
    unresolved_component: 8,
    component: 9,
  };
  return ranks[String(value || "")] ?? 9;
}

function componentSortKey(component) {
  const distance = Number(component?.sort_distance_au ?? component?.semi_major_axis_au);
  return [
    componentTypeRank(component?.component_type || component?.core_object_type),
    Number.isFinite(distance) ? distance : Number.POSITIVE_INFINITY,
    String(componentDisplayLabel(component)).toLowerCase(),
    String(component?.stable_component_key || ""),
  ];
}

function compareComponent(a, b) {
  const left = componentSortKey(a);
  const right = componentSortKey(b);
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] < right[index]) return -1;
    if (left[index] > right[index]) return 1;
  }
  return 0;
}

function componentByKey(arm) {
  const out = new Map();
  (arm?.components?.items || []).forEach((component) => {
    if (component?.stable_component_key) out.set(String(component.stable_component_key), component);
  });
  return out;
}

function componentForCoreObject(arm, coreObjectType, coreObjectId) {
  const targetId = Number(coreObjectId);
  if (!coreObjectType || !Number.isFinite(targetId)) return null;
  return (arm?.components?.items || []).find((component) => (
    String(component.core_object_type || "") === String(coreObjectType)
    && Number(component.core_object_id) === targetId
  )) || null;
}

function focusComponentKey(object, arm) {
  const type = object?.type || "";
  const data = object?.data || {};
  if (type === "component") return data.stable_component_key || "";
  if (data.arm_component?.stable_component_key) return data.arm_component.stable_component_key;
  if (type === "star") return componentForCoreObject(arm, "star", data.star_id)?.stable_component_key || "";
  if (type === "planet") return componentForCoreObject(arm, "planet", data.planet_id)?.stable_component_key || "";
  if (type === "system") return componentForCoreObject(arm, "system", data.system_id)?.stable_component_key || "";
  return "";
}

function objectComponentRelations(componentKey, arm) {
  const key = String(componentKey || "");
  const byKey = componentByKey(arm);
  const toRelation = (edge, direction) => {
    const relatedKey = direction === "child" ? edge.child_component_key : edge.parent_component_key;
    return {
      edge,
      component: byKey.get(String(relatedKey || "")) || { stable_component_key: relatedKey },
    };
  };
  const edges = arm?.hierarchy_edges?.items || [];
  return {
    children: edges
      .filter((edge) => String(edge.parent_component_key || "") === key)
      .map((edge) => toRelation(edge, "child"))
      .sort((a, b) => compareComponent(a.component, b.component)),
    parents: edges
      .filter((edge) => String(edge.child_component_key || "") === key)
      .map((edge) => toRelation(edge, "parent"))
      .sort((a, b) => compareComponent(a.component, b.component)),
  };
}

function ObjectRelationList({ title, rows, empty, onSelectObject }) {
  const items = Array.isArray(rows) ? rows : [];
  return (
    <div>
      <h3>{title}</h3>
      {items.length ? (
        <div className="component-link-list">
          {items.map(({ component, edge }, index) => (
            <button
              className="component-link-row"
              key={`${component.stable_component_key}-${index}`}
              type="button"
              onClick={() => onSelectObject?.({ type: "component", data: component })}
            >
              <strong>{componentDisplayLabel(component)}</strong>
              <span>
                {component.component_type || "component"} | {edge.edge_kind || "edge"} | {edge.confidence_tier || "unknown"}
                {Number.isFinite(Number(component.sort_distance_au)) ? ` | ${formatFloat(component.sort_distance_au, 4)} au` : ""}
              </span>
            </button>
          ))}
        </div>
      ) : <div className="empty">{empty}</div>}
    </div>
  );
}

function ObjectOverviewTab({ detail }) {
  const publicPayload = detail.public || {};
  const system = publicPayload.system || {};
  const aliases = system.aliases || [];
  const urls = detail.diagnostics?.public_urls || {};
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Core Identity</h2>
        <OverviewFact label="Display name" value={system.display_name || system.system_name || "n/a"} />
        <OverviewFact label="System ID" value={system.system_id || "n/a"} />
        <OverviewFact label="Stable object key" value={system.stable_object_key || "n/a"} />
        <OverviewFact label="Distance" value={`${formatFloat(system.dist_ly, 3)} ly / ${formatFloat(system.dist_pc, 3)} pc`} />
        <OverviewFact label="Coordinates" value={`${formatFloat(system.ra_deg, 5)} / ${formatFloat(system.dec_deg, 5)} deg`} />
        <OverviewFact label="Grouping" value={`${system.grouping_basis || "n/a"} | ${system.grouping_confidence || "n/a"}`} />
      </div>
      <div className="panel">
        <h2>Aliases</h2>
        {aliases.length ? (
          <table>
            <thead><tr><th>Alias</th><th>Kind</th><th>Source</th><th>Primary</th></tr></thead>
            <tbody>
              {aliases.slice(0, 20).map((alias, index) => (
                <tr key={`${alias.alias_norm}-${index}`}>
                  <td>{alias.alias_raw}</td>
                  <td>{alias.alias_kind || "n/a"}</td>
                  <td>{alias.source_catalog || "n/a"}</td>
                  <td>{alias.is_primary ? "yes" : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <div className="empty">No system aliases returned for this object.</div>}
      </div>
      <div className="panel">
        <h2>Public Links</h2>
        <MetricList rows={[
          ["API detail", <ObjectLinkValue value={urls.api_detail} />],
          ["Public detail", <ObjectLinkValue value={urls.public_detail} />],
          ["Snapshot URL", <ObjectLinkValue value={system.snapshot?.url} />],
        ]} />
      </div>
      <div className="panel">
        <h2>System Provenance</h2>
        <ObjectKeyValueTable payload={system.provenance || {}} />
      </div>
    </section>
  );
}

function ObjectLayersTab({ detail }) {
  const build = detail.build || {};
  const diagnostics = detail.diagnostics || {};
  const provenance = diagnostics.provenance || {};
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Layer Artifacts</h2>
        <MetricList rows={[
          ["Core DB", build.core_db_path || "n/a"],
          ["Arm DB", build.arm_db_path || "missing"],
          ["Disc DB", build.disc_db_path || "missing"],
        ]} />
      </div>
      <div className="panel">
        <h2>Provenance Completeness</h2>
        <MetricList rows={[
          ["System rows", `${formatInt(provenance.system?.checked)} checked, ${formatInt(provenance.system?.incomplete_count)} incomplete`],
          ["Star rows", `${formatInt(provenance.stars?.checked)} checked, ${formatInt(provenance.stars?.incomplete_count)} incomplete`],
          ["Planet rows", `${formatInt(provenance.planets?.checked)} checked, ${formatInt(provenance.planets?.incomplete_count)} incomplete`],
        ]} />
      </div>
      <div className="panel">
        <h2>Provenance Gaps</h2>
        <pre className="json-box">{jsonBlock({
          system: provenance.system?.examples || [],
          stars: provenance.stars?.examples || [],
          planets: provenance.planets?.examples || [],
        })}</pre>
      </div>
      <div className="panel">
        <h2>Layer Errors</h2>
        <MetricList rows={[
          ["Arm errors", (diagnostics.arm?.errors || []).join(" | ") || "none"],
          ["Disc errors", (diagnostics.disc?.errors || []).join(" | ") || "none"],
        ]} />
      </div>
    </section>
  );
}

function ObjectMembersTab({ stars, planets, arm, simulation, selectedObject, onSelectObject }) {
  const selectedType = selectedObject?.type;
  const selectedId = selectedObject?.data?.star_id || selectedObject?.data?.planet_id || selectedObject?.data?.stable_component_key;
  const armOnlyComponents = orderedArmOnlyComponents(arm);
  const componentGroups = groupedArmOnlyComponents(armOnlyComponents);
  return (
    <section className="object-members-layout">
      <div className="panel">
        <h2>Stars</h2>
        {stars.length ? (
          <table>
            <thead><tr><th>Name</th><th>Component</th><th>Spectral</th><th>Temp</th><th>Catalogs</th></tr></thead>
            <tbody>
              {stars.slice(0, 40).map((star) => (
                <tr className={selectedType === "star" && selectedId === star.star_id ? "selected-row" : ""} key={star.star_id}>
                  <td>
                    <button className="link-button" type="button" onClick={() => onSelectObject?.({ type: "star", data: { ...star, arm_component: componentForCoreObject(arm, "star", star.star_id) } })}>
                      {star.display_name || star.star_name || star.stable_object_key}
                    </button>
                  </td>
                  <td>{star.component || "n/a"}</td>
                  <td>{star.spectral_class || star.spectral_type || "n/a"}</td>
                  <td>{formatFloat(star.teff_k, 0)} K</td>
                  <td>{(star.arm_catalogs || []).join(", ") || star.source_catalog || "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <div className="empty">No stars returned.</div>}
      </div>
      <div className="panel">
        <h2>Planets</h2>
        {planets.length ? (
          <table>
            <thead><tr><th>Name</th><th>Period</th><th>Semi-major axis</th><th>Radius</th><th>Env Evidence</th><th>Candidate Environment</th></tr></thead>
            <tbody>
              {planets.slice(0, 40).map((planet) => {
                const environment = planet.environment_evidence || {};
                const resolvedSma = resolvedSimulationField(simulation, "planet", planet.planet_id, "semi_major_axis_au", planet.semi_major_axis_au);
                const resolvedInsol = resolvedSimulationField(simulation, "planet", planet.planet_id, "candidate_insol_earth", planet.insol_earth);
                const resolvedEqTemp = resolvedSimulationField(simulation, "planet", planet.planet_id, "candidate_eq_temp_k", planet.eq_temp_k);
                const hasDerivedEnvironment = Boolean(resolvedInsol || resolvedEqTemp);
                return (
                  <tr className={selectedType === "planet" && selectedId === planet.planet_id ? "selected-row" : ""} key={planet.planet_id}>
                    <td>
                      <button className="link-button" type="button" onClick={() => onSelectObject?.({ type: "planet", data: { ...planet, arm_component: componentForCoreObject(arm, "planet", planet.planet_id) } })}>
                        {planet.planet_name || planet.stable_object_key}
                      </button>
                    </td>
                    <td>{formatFloat(planet.orbital_period_days, 3)} d</td>
                    <td><ResolvedScienceValue sourceValue={planet.semi_major_axis_au} resolvedField={resolvedSma} digits={5} unit="au" /></td>
                    <td>{formatFloat(planet.radius_earth, 2)} Earth</td>
                    <td>
                      <span className={`badge ${hasDerivedEnvironment ? "muted" : environment.evidence_basis === "missing" ? "warn" : environment.evidence_basis === "stellar_class_luminosity_proxy" ? "muted" : "ok"}`}>
                        {hasDerivedEnvironment ? "derived arm" : environmentEvidenceLabel(environment.evidence_basis)}
                      </span>
                      {environment.missing_reason && !hasDerivedEnvironment ? <span className="table-subtext">{environment.missing_reason}</span> : null}
                    </td>
                    <td>
                      <ResolvedScienceValue sourceValue={environment.candidate_eq_temp_k} resolvedField={resolvedEqTemp} digits={1} unit="K" />
                      <span className="table-subtext">
                        flux <ResolvedScienceValue sourceValue={environment.candidate_insol_earth} resolvedField={resolvedInsol} digits={3} unit="Earth=1" details={false} />
                        {environment.broad_hz_candidate ? " | broad HZ" : ""}
                        {environment.nice_planet_candidate ? " | nice candidate" : ""}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : <div className="empty">No planets returned.</div>}
      </div>
      <div className="panel">
        <div className="panel-head">
          <div>
            <h2>Arm Components</h2>
            <p className="muted">Supplemental components from arm, grouped for navigation and sorted by hierarchy and distance where available.</p>
          </div>
          <span className="badge muted">{formatInt(armOnlyComponents.length)} rows</span>
        </div>
        {componentGroups.length ? (
          <div className="component-group-list">
            {componentGroups.map((group) => (
              <details
                className="component-group"
                key={group.key}
                defaultOpen={group.rows.some((component) => selectedType === "component" && selectedId === component.stable_component_key) || group.defaultOpen}
              >
                <summary>
                  <span>{group.label}</span>
                  <span className="badge muted">{formatInt(group.rows.length)}</span>
                </summary>
                <ComponentGroupTable
                  rows={group.rows}
                  selectedType={selectedType}
                  selectedId={selectedId}
                  onSelectObject={onSelectObject}
                />
              </details>
            ))}
          </div>
        ) : <div className="empty">No non-core arm components returned. Moons, minor bodies, and artificial objects appear here when present.</div>}
      </div>
    </section>
  );
}

function environmentEvidenceLabel(value) {
  const key = String(value || "missing");
  if (key === "source_eq_temp") return "source temp";
  if (key === "source_insolation") return "source flux";
  if (key === "stellar_class_luminosity_proxy") return "proxy flux";
  return "missing";
}

function groupedArmOnlyComponents(components) {
  const labels = {
    moon: "Moons",
    subplanet: "Dwarf Planets / Subplanets",
    minor_body: "Minor Bodies",
    artificial: "Artificial Objects",
    region: "Regions",
    unresolved_component: "Unresolved Components",
  };
  const order = ["moon", "subplanet", "minor_body", "artificial", "region", "unresolved_component", "other"];
  const groups = new Map();
  (components || []).forEach((component) => {
    const type = String(component?.component_type || "other");
    const key = labels[type] ? type : "other";
    if (!groups.has(key)) {
      groups.set(key, {
        key,
        label: labels[key] || "Other Arm Components",
        rows: [],
        defaultOpen: ["moon", "subplanet"].includes(key),
      });
    }
    groups.get(key).rows.push(component);
  });
  return Array.from(groups.values()).sort((a, b) => order.indexOf(a.key) - order.indexOf(b.key));
}

function ComponentGroupTable({ rows, selectedType, selectedId, onSelectObject }) {
  return (
    <table>
      <thead><tr><th>Name</th><th>Parent</th><th>Type</th><th>Core Link</th><th>Distance</th><th>Source</th></tr></thead>
      <tbody>
        {rows.map((component) => (
          <tr className={selectedType === "component" && selectedId === component.stable_component_key ? "selected-row" : ""} key={component.stable_component_key}>
            <td>
              <button className="link-button" type="button" onClick={() => onSelectObject?.({ type: "component", data: component })}>
                {component.depth ? `${"\u00a0\u00a0".repeat(Math.min(component.depth, 4))}${componentDisplayLabel(component)}` : componentDisplayLabel(component)}
              </button>
            </td>
            <td>{component.parent_display_name || "n/a"}</td>
            <td>{component.component_type || "component"}</td>
            <td>{component.core_object_type ? `${component.core_object_type} ${component.core_object_id || ""}` : "arm only"}</td>
            <td>{Number.isFinite(Number(component.sort_distance_au)) ? `${formatFloat(component.sort_distance_au, 4)} au` : ""}</td>
            <td>{component.source_catalog || "n/a"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function orderedArmOnlyComponents(arm) {
  const components = (arm?.components?.items || [])
    .filter((component) => !["system", "star", "planet"].includes(String(component.core_object_type || "")));
  const byKey = componentByKey(arm);
  const childMap = new Map();
  const parentMap = new Map();
  (arm?.hierarchy_edges?.items || []).forEach((edge) => {
    const parent = String(edge.parent_component_key || "");
    const child = String(edge.child_component_key || "");
    if (!parent || !child) return;
    if (!childMap.has(parent)) childMap.set(parent, []);
    childMap.get(parent).push(child);
    if (!parentMap.has(child)) parentMap.set(child, parent);
  });
  childMap.forEach((children) => {
    children.sort((left, right) => compareComponent(byKey.get(left) || { stable_component_key: left }, byKey.get(right) || { stable_component_key: right }));
  });
  const componentKeys = new Set(components.map((component) => String(component.stable_component_key || "")));
  const roots = components
    .filter((component) => {
      const key = String(component.stable_component_key || "");
      return !(arm?.hierarchy_edges?.items || []).some((edge) => componentKeys.has(String(edge.parent_component_key || "")) && String(edge.child_component_key || "") === key);
    })
    .sort(compareComponent)
    .map((component) => String(component.stable_component_key || ""));
  const out = [];
  const seen = new Set();
  const visit = (key, depth) => {
    if (!key || seen.has(key)) return;
    const component = byKey.get(key);
    if (component && componentKeys.has(key)) {
      const parent = byKey.get(parentMap.get(key));
      out.push({ ...component, depth, parent_display_name: parent ? componentDisplayLabel(parent) : "" });
    }
    seen.add(key);
    (childMap.get(key) || []).forEach((childKey) => {
      if (componentKeys.has(childKey)) visit(childKey, depth + 1);
    });
  };
  roots.forEach((key) => visit(key, 0));
  components.sort(compareComponent).forEach((component) => {
    const key = String(component.stable_component_key || "");
    if (!seen.has(key)) {
      const parent = byKey.get(parentMap.get(key));
      out.push({ ...component, depth: 0, parent_display_name: parent ? componentDisplayLabel(parent) : "" });
    }
  });
  return out;
}

function ObjectGraphTab({ arm, hierarchy, system, selectedObject, onSelectObject }) {
  return (
    <section className="runbook-grid">
      <div className="panel">
        <h2>Relation Diagram</h2>
        <ObjectRelationGraph arm={arm} system={system} selectedObject={selectedObject} onSelectObject={onSelectObject} />
      </div>
      <div className="panel">
        <h2>Hierarchy Summary</h2>
        <ObjectKeyValueTable payload={hierarchy?.counts || {}} />
      </div>
      <div className="panel">
        <h2>Arm Components</h2>
        <ObjectRowsTable rows={arm.components?.items || []} preferredColumns={["display_name", "component_type", "core_object_type", "core_object_id", "stable_component_key", "source_catalog", "source_pk"]} />
      </div>
      <div className="panel">
        <h2>Hierarchy Edges</h2>
        <ObjectRowsTable rows={arm.hierarchy_edges?.items || []} preferredColumns={["parent_component_key", "child_component_key", "edge_kind", "member_role", "confidence_tier", "source_catalog"]} />
      </div>
      <div className="panel">
        <h2>Orbit Edges</h2>
        <ObjectRowsTable rows={arm.orbit_edges?.items || []} preferredColumns={["edge_label", "relation_kind", "primary_display_name", "secondary_display_name", "host_display_name", "confidence_tier", "source_catalog"]} />
      </div>
      <div className="panel">
        <h2>Orbital Solutions</h2>
        <ObjectRowsTable rows={arm.orbital_solutions?.items || []} preferredColumns={["edge_label", "solution_source_catalog", "period_days", "semi_major_axis_au", "semi_major_axis_arcsec", "eccentricity", "inclination_deg", "confidence_tier"]} />
      </div>
    </section>
  );
}

function ObjectRelationGraph({ arm, system, selectedObject, onSelectObject }) {
  const graph = buildObjectRelationGraph(arm, system);
  const selectedKey = focusComponentKey(selectedObject, arm);
  if (!graph.nodes.length) return <div className="empty">No arm component rows are available for a relation diagram.</div>;
  return (
    <div className="object-graph-wrap">
      <svg className="object-graph" viewBox="0 0 920 440" role="img" aria-label="System relation graph">
        <defs>
          <marker id="graphArrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
            <path d="M0,0 L8,4 L0,8 Z" fill="#64748b" />
          </marker>
        </defs>
        {graph.edges.map((edge) => (
          <g key={edge.id}>
            <line
              className={`object-graph-edge ${edge.kind}`}
              x1={edge.from.x}
              y1={edge.from.y}
              x2={edge.to.x}
              y2={edge.to.y}
              markerEnd="url(#graphArrow)"
            />
            <text className="object-graph-edge-label" x={(edge.from.x + edge.to.x) / 2} y={(edge.from.y + edge.to.y) / 2 - 6}>{edge.label}</text>
          </g>
        ))}
        {graph.nodes.map((node) => (
          <g
            className={`object-graph-node ${node.kind} ${selectedKey === node.key ? "selected" : ""}`}
            key={node.key}
            transform={`translate(${node.x} ${node.y})`}
            onClick={() => onSelectObject?.({ type: "component", data: node.component || { stable_component_key: node.key, component_type: node.kind, display_name: node.label } })}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelectObject?.({ type: "component", data: node.component || { stable_component_key: node.key, component_type: node.kind, display_name: node.label } });
              }
            }}
            role="button"
            tabIndex={0}
          >
            <title>{node.label}</title>
            <circle r="22" />
            <text className="object-graph-node-type" y="-2">{node.shortType}</text>
            <text className="object-graph-node-label" y="38">{compactId(node.label, 20)}</text>
          </g>
        ))}
      </svg>
      <div className="graph-legend">
        <span><i className="legend-swatch contains" /> containment</span>
        <span><i className="legend-swatch orbit" /> orbit/dynamic relation</span>
      </div>
    </div>
  );
}

function buildObjectRelationGraph(arm, system) {
  const components = Array.isArray(arm?.components?.items) ? arm.components.items : [];
  const nodesByKey = new Map();
  const addNode = (key, payload = {}) => {
    const cleanKey = String(key || "").trim();
    if (!cleanKey) return null;
    const label = componentDisplayLabel({ ...payload, stable_component_key: cleanKey });
    const kind = payload.component_type || payload.core_object_type || "component";
    if (!nodesByKey.has(cleanKey)) {
      nodesByKey.set(cleanKey, {
        key: cleanKey,
        label,
        kind,
        shortType: graphTypeLabel(kind),
        component: payload,
      });
    } else {
      const existing = nodesByKey.get(cleanKey);
      if (label && existing.label === componentDisplayLabel({ stable_component_key: cleanKey })) existing.label = label;
      if (payload.component_type || payload.core_object_type) existing.kind = kind;
      existing.shortType = graphTypeLabel(existing.kind);
      existing.component = { ...(existing.component || {}), ...payload, stable_component_key: cleanKey };
    }
    return nodesByKey.get(cleanKey);
  };
  components.forEach((component) => addNode(component.stable_component_key, component));
  if (!nodesByKey.size && system?.stable_object_key) {
    addNode(system.stable_object_key, { display_name: system.display_name || system.system_name, component_type: "system" });
  }
  const rawEdges = [];
  (arm?.hierarchy_edges?.items || []).forEach((edge, index) => {
    const from = addNode(edge.parent_component_key);
    const to = addNode(edge.child_component_key);
    if (from && to) rawEdges.push({ id: `h-${index}`, from: from.key, to: to.key, kind: "contains", label: edge.edge_kind || "contains" });
  });
  (arm?.orbit_edges?.items || []).forEach((edge, index) => {
    const fromKey = edge.primary_component_key || edge.host_component_key;
    const toKey = edge.secondary_component_key || edge.barycenter_key;
    const from = addNode(fromKey);
    const to = addNode(toKey);
    if (from && to) rawEdges.push({ id: `o-${index}`, from: from.key, to: to.key, kind: "orbit", label: edge.relation_kind || "orbits" });
  });
  const tiers = ["system", "star", "main_sequence", "brown_dwarf", "compact", "planet", "subplanet", "moon", "minor_body", "artificial", "unresolved_component", "component"];
  const tierIndexFor = (kind) => {
    const index = tiers.indexOf(String(kind || ""));
    return index >= 0 ? index : tiers.length - 1;
  };
  const nodes = Array.from(nodesByKey.values())
    .sort((a, b) => {
      const tierDelta = tierIndexFor(a.kind) - tierIndexFor(b.kind);
      if (tierDelta !== 0) return tierDelta;
      return String(a.label || "").localeCompare(String(b.label || ""));
    })
    .slice(0, 48);
  const visible = new Set(nodes.map((node) => node.key));
  const grouped = new Map();
  nodes.forEach((node) => {
    const tierIndex = tierIndexFor(node.kind);
    if (!grouped.has(tierIndex)) grouped.set(tierIndex, []);
    grouped.get(tierIndex).push(node);
  });
  const tierKeys = Array.from(grouped.keys()).sort((a, b) => a - b);
  tierKeys.forEach((tierKey, tierIndex) => {
    const row = grouped.get(tierKey);
    const y = 56 + tierIndex * Math.max(70, Math.min(95, 350 / Math.max(1, tierKeys.length - 1)));
    row.forEach((node, nodeIndex) => {
      const x = 70 + ((nodeIndex + 1) * (780 / (row.length + 1)));
      node.x = x;
      node.y = y;
    });
  });
  const positioned = new Map(nodes.map((node) => [node.key, node]));
  const edges = rawEdges
    .filter((edge) => visible.has(edge.from) && visible.has(edge.to))
    .slice(0, 60)
    .map((edge) => ({ ...edge, from: positioned.get(edge.from), to: positioned.get(edge.to) }))
    .filter((edge) => edge.from && edge.to);
  return { nodes, edges };
}

function graphTypeLabel(value) {
  const text = String(value || "node");
  if (text === "system") return "SYS";
  if (text === "star" || text === "main_sequence") return "STAR";
  if (text === "planet") return "PLN";
  if (text === "subplanet") return "SUB";
  if (text === "moon") return "MOON";
  if (text === "minor_body") return "MNR";
  if (text === "artificial") return "ASO";
  if (text === "unresolved_component") return "UNR";
  return text.slice(0, 3).toUpperCase();
}

function ObjectSimulationTab({ simulation }) {
  const counts = simulation?.counts || {};
  const stars = Array.isArray(simulation?.stars) ? simulation.stars : [];
  const planets = Array.isArray(simulation?.planets) ? simulation.planets : [];
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Simulation Readiness</h2>
        <MetricList rows={[
          ["Readiness score", formatPct((simulation?.score || 0) * 100)],
          ["Source fields", formatInt(counts.source)],
          ["Derived fields", formatInt(counts.derived)],
          ["Assumed fields", formatInt(counts.assumed)],
          ["Missing fields", formatInt(counts.missing)],
        ]} />
      </div>
      <div className="panel">
        <h2>Layer Guidance</h2>
        <div className="trap-list">
          {(simulation?.notes || []).map((note) => <div key={note}>{note}</div>)}
        </div>
      </div>
      <div className="panel wide-panel">
        <h2>Stars</h2>
        {stars.length ? <SimulationObjectTable rows={stars} /> : <div className="empty">No star simulation fields were returned.</div>}
      </div>
      <div className="panel wide-panel">
        <h2>Planets</h2>
        {planets.length ? <SimulationObjectTable rows={planets} /> : <div className="empty">No planet simulation fields were returned.</div>}
      </div>
    </section>
  );
}

function SimulationObjectTable({ rows }) {
  return (
    <div className="simulation-object-list">
      {rows.map((row) => (
        <div className="simulation-object-card" key={`${row.object_type}-${row.object_id || row.stable_object_key}`}>
          <div className="panel-head compact">
            <div>
              <h3>{row.display_name || row.stable_object_key || "Object"}</h3>
              <p className="muted">
                {row.object_type} {row.object_id || ""}
                {row.host_display_name ? ` | host ${row.host_display_name}` : ""}
              </p>
            </div>
            <span className="badge muted">{compactId(row.stable_object_key, 28)}</span>
          </div>
          <table>
            <thead><tr><th>Field</th><th>Value</th><th>Status</th><th>Basis</th><th>Replace With</th></tr></thead>
            <tbody>
              {(row.fields || []).map((field) => (
                <tr key={field.key}>
                  <td>{field.label || actionLabel(field.key)}</td>
                  <td>
                    <strong>{renderSimulationValue(field)}</strong>
                    <span className="table-subtext">{field.layer || "n/a"} | {field.confidence_tier || "unknown"}</span>
                  </td>
                  <td><span className={`badge ${simulationStatusTone(field.status)}`}>{readableStatus(field.status)}</span></td>
                  <td>{field.basis || "n/a"}</td>
                  <td>{field.replacement_target || "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

function renderSimulationValue(field) {
  if (field?.value === null || field?.value === undefined || field?.value === "") return "n/a";
  const value = typeof field.value === "number" ? formatFloat(field.value, Math.abs(field.value) >= 100 ? 2 : 5) : String(field.value);
  return field.unit ? `${value} ${field.unit}` : value;
}

function simulationStatusTone(status) {
  const key = String(status || "");
  if (key === "source") return "ok";
  if (key === "derived") return "muted";
  if (key === "assumed") return "warn";
  if (key === "missing") return "danger";
  return statusTone(key);
}

function ObjectPresentationTab({ disc, system }) {
  const coolness = disc.coolness || null;
  const snapshots = disc.snapshots || [];
  return (
    <section className="dataset-grid">
      <div className="panel">
        <h2>Coolness</h2>
        {coolness ? (
          <>
            <OverviewFact label="Rank" value={formatInt(coolness.rank)} />
            <OverviewFact label="Score" value={formatFloat(coolness.score_total, 4)} />
            <OverviewFact label="Profile" value={`${coolness.profile_id || "n/a"} @ ${coolness.profile_version || "n/a"}`} />
            <h3>Score Signals</h3>
            <MetricList rows={coolnessSignalRows(coolness.counts || {})} />
            <h3>Score Contributions</h3>
            <ObjectCoolnessExplanation rows={coolness.explanation || []} />
            <details className="object-details">
              <summary>Raw feature and score columns</summary>
              <ObjectFeatureTable features={coolness.features || {}} />
            </details>
          </>
        ) : <div className="empty">No coolness row found in disc.</div>}
      </div>
      <div className="panel">
        <h2>Snapshots</h2>
        {snapshots.length ? (
          <table>
            <thead><tr><th>View</th><th>Build</th><th>Params</th><th>Created</th><th>URL</th></tr></thead>
            <tbody>
              {snapshots.map((snapshot, index) => (
                <tr key={`${snapshot.artifact_path}-${index}`}>
                  <td>{snapshot.view_type}</td>
                  <td>{compactId(snapshot.build_id, 22)}</td>
                  <td>{snapshot.params_hash}</td>
                  <td>{formatDate(snapshot.created_at)}</td>
                  <td>{snapshot.url ? <a href={snapshot.url} target="_blank" rel="noreferrer">open</a> : "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <div className="empty">No snapshot manifest rows found.</div>}
      </div>
      <div className="panel">
        <h2>Primary Snapshot</h2>
        {system.snapshot ? <ObjectKeyValueTable payload={system.snapshot} /> : <div className="empty">No primary snapshot is attached to the public system payload.</div>}
      </div>
    </section>
  );
}

function coolnessSignalRows(counts) {
  return [
    ["Nice planet count", formatInt(counts.nice_planet_count)],
    ["Nice from source temp", formatInt(counts.nice_planet_source_eq_temp_count)],
    ["Nice from source insolation", formatInt(counts.nice_planet_source_insolation_count)],
    ["Nice from proxy insolation", formatInt(counts.nice_planet_proxy_insolation_count)],
    ["Weird planet count", formatInt(counts.weird_planet_count)],
    ["Ultra-short period", formatInt(counts.ultra_short_period_count)],
    ["High eccentricity", formatInt(counts.high_eccentricity_count)],
  ];
}

function ObjectCoolnessExplanation({ rows }) {
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) return <div className="empty">No coolness contribution rows returned.</div>;
  const total = items.reduce((sum, item) => sum + Math.max(0, toNumber(item.score_contribution)), 0);
  return (
    <table>
      <thead><tr><th>Element</th><th>Feature</th><th>Weight</th><th>Contribution</th><th>Share</th></tr></thead>
      <tbody>
        {items.map((item) => (
          <tr key={item.key}>
            <td>{actionLabel(item.key)}</td>
            <td>{formatFloat(item.feature_value, 4)}</td>
            <td>{formatFloat(item.effective_weight, 4)}</td>
            <td><strong>{formatFloat(item.score_contribution, 4)}</strong></td>
            <td>{formatPct(pctFromPart(item.score_contribution, total))}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function isHttpUrl(value) {
  return /^https?:\/\//i.test(String(value || ""));
}

function ObjectLinkValue({ value }) {
  const text = String(value || "").trim();
  if (!text) return "n/a";
  if (isHttpUrl(text) || text.startsWith("/")) {
    return <a href={text} target="_blank" rel="noreferrer">{text}</a>;
  }
  return text;
}

function ObjectKeyValueTable({ payload }) {
  const entries = Object.entries(payload || {}).filter(([, value]) => value !== undefined && value !== null && value !== "");
  if (!entries.length) return <div className="empty">No fields returned.</div>;
  return (
    <table className="kv-table">
      <tbody>
        {entries.map(([key, value]) => (
          <tr key={key}>
            <th>{actionLabel(key)}</th>
            <td>{renderObjectValue(value)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function renderObjectValue(value) {
  if (value === null || value === undefined || value === "") return "n/a";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number") return Number.isInteger(value) ? formatInt(value) : formatFloat(value, 6);
  if (Array.isArray(value)) return <ObjectNestedValue value={value} />;
  if (typeof value === "object") return <ObjectNestedValue value={value} />;
  return <ObjectLinkValue value={value} />;
}

function ObjectNestedValue({ value, depth = 0 }) {
  if (value === null || value === undefined || value === "") return "n/a";
  if (typeof value !== "object") return renderObjectScalar(value);
  if (Array.isArray(value)) {
    if (!value.length) return "none";
    const scalarOnly = value.every((item) => item === null || typeof item !== "object");
    if (scalarOnly) {
      return (
        <span className="object-inline-list">
          {value.map((item, index) => (
            <React.Fragment key={`${String(item)}-${index}`}>
              {index ? ", " : ""}
              {renderObjectScalar(item)}
            </React.Fragment>
          ))}
        </span>
      );
    }
    return (
      <div className="object-nested-list">
        {value.slice(0, 12).map((item, index) => (
          <div className="object-nested-row" key={index}>
            <strong>{index + 1}</strong>
            <ObjectNestedValue value={item} depth={depth + 1} />
          </div>
        ))}
        {value.length > 12 ? <span className="table-subtext">plus {formatInt(value.length - 12)} more</span> : null}
      </div>
    );
  }
  const entries = Object.entries(value).filter(([, item]) => item !== undefined && item !== null && item !== "");
  if (!entries.length) return "none";
  if (depth > 2) return <code>{JSON.stringify(value)}</code>;
  return (
    <div className="object-nested-list">
      {entries.map(([key, item]) => (
        <div className="object-nested-row" key={key}>
          <strong>{actionLabel(key)}</strong>
          <ObjectNestedValue value={item} depth={depth + 1} />
        </div>
      ))}
    </div>
  );
}

function renderObjectScalar(value) {
  if (value === null || value === undefined || value === "") return "n/a";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number") return Number.isInteger(value) ? formatInt(value) : formatFloat(value, 6);
  return <ObjectLinkValue value={value} />;
}

function ObjectFeatureTable({ features }) {
  const rows = Object.entries(features || {}).sort(([a], [b]) => a.localeCompare(b));
  if (!rows.length) return <div className="empty">No feature values returned.</div>;
  return (
    <table>
      <thead><tr><th>Feature</th><th>Value</th></tr></thead>
      <tbody>
        {rows.map(([key, value]) => (
          <tr key={key}>
            <td>{actionLabel(key)}</td>
            <td>{typeof value === "number" ? formatFloat(value, 6) : String(value ?? "n/a")}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ObjectRowsTable({ rows, preferredColumns = [] }) {
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) return <div className="empty">No rows returned for this diagnostic.</div>;
  const allColumns = Object.keys(items[0] || {});
  const columns = preferredColumns.length
    ? [
        ...preferredColumns.filter((column) => allColumns.includes(column)),
        ...allColumns.filter((column) => !preferredColumns.includes(column)),
      ].slice(0, 8)
    : allColumns.slice(0, 8);
  return (
    <table>
      <thead><tr>{columns.map((column) => <th key={column}>{actionLabel(column)}</th>)}</tr></thead>
      <tbody>
        {items.slice(0, 30).map((row, rowIndex) => (
          <tr key={rowIndex}>
            {columns.map((column) => <td key={column}>{renderObjectValue(row[column])}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
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

function OperationsScreen({ csrf, requestedJobId = "" }) {
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
    if (!requestedJobId) return;
    selectJob(requestedJobId);
  }, [requestedJobId]);

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

function ActionCard({ action, runAction, busy, snapshotControl = null }) {
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
        {action.name === "generate_snapshots" ? <SnapshotActionSafety values={values} snapshotControl={snapshotControl} /> : null}
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

function snapshotRunWarnings(topCount) {
  const count = Number(topCount);
  if (!Number.isFinite(count) || count <= 0) return [];
  if (count >= 1000000) {
    return ["1,000,000 or more snapshots is a major batch run. Expect long runtime, many filesystem entries, and monitor Operations until completion."];
  }
  if (count >= 100000) {
    return ["100,000 or more snapshots is a large batch run. Watch elapsed time, output footprint, and job logs."];
  }
  if (count > 10000) {
    return ["More than 10,000 snapshots is allowed on Photon, but should be treated as a monitored batch job."];
  }
  return [];
}

function SnapshotActionSafety({ values, snapshotControl }) {
  const topCount = Number(values?.top_coolness || 0);
  const warnings = snapshotRunWarnings(topCount);
  const average = Number(snapshotControl?.estimate?.average_bytes_per_requested);
  const estimatedBytes = Number.isFinite(average) && average > 0 && topCount > 0 ? average * topCount : null;
  const bulk = snapshotControl?.storage?.bulk_dir;
  return (
    <div className="snapshot-safety">
      <strong>Snapshot run safety</strong>
      <span>Requested count: {formatInt(topCount)}{estimatedBytes ? ` | estimated footprint ${formatBytes(estimatedBytes)}` : " | no footprint estimate yet"}</span>
      {bulk ? <span>Bulk root: {runtimeStatusLabel(bulk.status)} | {bulk.path}</span> : null}
      {warnings.length ? <span className="warning-text">{warnings.join(" ")}</span> : <span>No large-run threshold warning for this count.</span>}
    </div>
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
  if (Array.isArray(spec.options) || Array.isArray(spec.enum)) {
    const options = Array.isArray(spec.options) && spec.options.length
      ? spec.options
      : (spec.enum || []).map((item) => ({ value: item, label: actionLabel(item) }));
    return (
      <label>
        <span>{label}</span>
        <select value={value ?? ""} onChange={(event) => updateValue(name, event.target.value)}>
          {options.map((option) => {
            const optionValue = typeof option === "object" ? option.value : option;
            const optionLabel = typeof option === "object" ? option.label || option.value : actionLabel(option);
            return <option key={optionValue} value={optionValue}>{optionLabel}</option>;
          })}
        </select>
        {spec.help ? <em className="confirmation-reminder">{spec.help}</em> : null}
        {options.map((option) => (
          typeof option === "object" && option.description ? (
            <em className="confirmation-reminder" key={`${option.value}-description`}>{option.label || option.value}: {option.description}</em>
          ) : null
        ))}
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
          {snapshotRunWarnings(Number(value)).join(" ")}
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
  const security = data.runtime_security || {};
  const securitySummary = security.summary || {};
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
      {item.stat ? <span className="table-subtext">{formatMode(item.stat)}</span> : null}
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
    {
      label: "Security",
      value: securitySummary.total ? `${securitySummary.passed}/${securitySummary.total}` : "n/a",
      tone: securitySummary.status === "ok" ? "ok" : securitySummary.status === "warning" ? "warn" : "",
    },
    { label: "API RSS", value: formatBytes(api.rss_bytes) },
  ];

  async function copyDiagnostics() {
    try {
      const { response, text } = await fetchText(`${ADMIN_API_BASE}/runtime/diagnostics`);
      if (!response.ok) {
        throw new Error(text || `HTTP ${response.status}`);
      }
      await copyTextToClipboard(text);
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
          <a className="button" href={`${ADMIN_API_BASE}/runtime/diagnostics`} target="_blank" rel="noreferrer">Open Diagnostics</a>
          <a className="button" href={`${ADMIN_API_BASE}/runtime/diagnostics?download=1`}>Download Diagnostics</a>
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
        <div className="panel runtime-security-panel">
          <div className="panel-head">
            <div>
              <h2>Runtime Security</h2>
              <p className="muted">Observed inside the API process. No Docker socket required.</p>
            </div>
            <span className={`badge ${securitySummary.status === "ok" ? "ok" : "warn"}`}>
              {securitySummary.total ? `${securitySummary.passed}/${securitySummary.total}` : "unknown"}
            </span>
          </div>
          <MetricList
            rows={[
              ["Process user", `uid=${security.effective_uid ?? "?"} gid=${security.effective_gid ?? "?"}`, `expected ${security.expected_uid ?? "?"}:${security.expected_gid ?? "?"}`],
              ["No new privileges", security.no_new_privileges ? "active" : "not active"],
              ["Seccomp", security.seccomp_label || "unknown", `mode=${security.seccomp_mode ?? "?"}`],
              ["Capabilities", security.capabilities?.effective_empty && security.capabilities?.permitted_empty ? "dropped" : "present", `eff=${security.capabilities?.effective_hex || "n/a"} prm=${security.capabilities?.permitted_hex || "n/a"}`],
              ["Root filesystem", security.write_probes?.project_root?.status || "unknown", security.write_probes?.project_root?.error || ""],
              ["Scratch/state writes", `/tmp ${security.write_probes?.tmp?.status || "unknown"}`, `state ${security.write_probes?.state_dir?.status || "unknown"}`],
              ["Umask / bytecode", security.umask_configured || "n/a", security.python_bytecode_disabled ? "Python bytecode disabled" : "Python bytecode may be written"],
            ]}
          />
          {Array.isArray(security.hardening_checks) && security.hardening_checks.length ? (
            <div className="check-list">
              {security.hardening_checks.map((item) => (
                <div className="check-row" key={item.key}>
                  <span className={`badge ${item.ok ? "ok" : "warn"}`}>{item.ok ? "ok" : "check"}</span>
                  <span>{item.label}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="empty">Runtime security observations are unavailable.</div>
          )}
          <div className="hint-list">
            {(security.notes || []).map((note) => <div key={note}>{note}</div>)}
          </div>
        </div>
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
