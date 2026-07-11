const rawBase = import.meta.env.VITE_API_BASE || "";
const API_BASE = rawBase.endsWith("/") ? rawBase.slice(0, -1) : rawBase;
const SIMULATION_SCENE_CACHE_LIMIT = 128;
const simulationSceneCache = new Map();

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

export async function fetchSystemDetail(systemId, params = {}) {
  const query = new URLSearchParams(params);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  const url = apiUrl(`/api/v1/systems/${systemId}${suffix}`);
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Detail failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchSystemSimulationScene(systemId, params = {}) {
  const query = new URLSearchParams(params);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  const cacheKey = `${String(systemId || "")}:${String(params?.name_style || "public_full")}`;
  const cacheDisabled = Boolean(globalThis?.window?.__SPACEGATE_DISABLE_SIM_SCENE_CACHE);
  if (!cacheDisabled && simulationSceneCache.has(cacheKey)) {
    const cached = simulationSceneCache.get(cacheKey);
    simulationSceneCache.delete(cacheKey);
    simulationSceneCache.set(cacheKey, cached);
    return cached;
  }
  const url = apiUrl(`/api/v1/systems/${systemId}/simulation-scene${suffix}`);
  const request = fetch(url)
    .then(async (res) => {
      if (!res.ok) {
        const detail = await res.text();
        throw new Error(`Simulation scene failed: ${res.status} ${detail}`);
      }
      return res.json();
    })
    .catch((error) => {
      simulationSceneCache.delete(cacheKey);
      throw error;
    });
  if (!cacheDisabled) {
    simulationSceneCache.set(cacheKey, request);
    if (simulationSceneCache.size > SIMULATION_SCENE_CACHE_LIMIT) {
      const oldestKey = simulationSceneCache.keys().next().value;
      simulationSceneCache.delete(oldestKey);
    }
  }
  return request;
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

export async function fetchPublicConfig() {
  const url = apiUrl("/api/v1/public-config");
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Public config failed: ${res.status} ${detail}`);
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
