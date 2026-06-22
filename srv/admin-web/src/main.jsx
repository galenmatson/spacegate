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

function normalizeOptional(value) {
  const trimmed = String(value || "").trim();
  return trimmed ? trimmed : null;
}

function App() {
  const [authState, setAuthState] = useState({ loading: true, data: null, error: "" });
  const [activeScreen, setActiveScreen] = useState("inference");

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
          <button className={activeScreen === "agency" ? "active" : ""} onClick={() => setActiveScreen("agency")}>Agency</button>
          <button className={activeScreen === "audit" ? "active" : ""} onClick={() => setActiveScreen("audit")}>Audit</button>
        </nav>
        <button className="button" onClick={logout}>Log out</button>
      </aside>
      <main className="workspace">
        {activeScreen === "inference" ? (
          <InferenceScreen csrf={csrf} />
        ) : (
          <PlaceholderScreen name={activeScreen} />
        )}
      </main>
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
