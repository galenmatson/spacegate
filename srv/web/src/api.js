const rawBase = import.meta.env.VITE_API_BASE || "";
const API_BASE = rawBase.endsWith("/") ? rawBase.slice(0, -1) : rawBase;

export function apiUrl(path) {
  const normalizedPath = String(path || "").startsWith("/") ? path : `/${path}`;
  return `${API_BASE}${normalizedPath}`;
}

export async function fetchSystems(params) {
  const query = new URLSearchParams(params);
  const url = apiUrl(`/api/v1/systems/search?${query.toString()}`);
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Search failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchSystemDetail(systemId) {
  const url = apiUrl(`/api/v1/systems/${systemId}`);
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Detail failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchSystemSimulationScene(systemId) {
  const url = apiUrl(`/api/v1/systems/${systemId}/simulation-scene`);
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Simulation scene failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchMapSystems(params = {}) {
  const query = new URLSearchParams(params);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  const url = apiUrl(`/api/v1/map/systems${suffix}`);
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Map systems failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchHealth() {
  const url = apiUrl("/api/v1/health");
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Health failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchSpectralMix() {
  const url = apiUrl("/api/v1/stats/spectral");
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Spectral stats failed: ${res.status} ${detail}`);
  }
  return res.json();
}
