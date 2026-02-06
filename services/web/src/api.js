const rawBase = import.meta.env.VITE_API_BASE || "";
const API_BASE = rawBase.endsWith("/") ? rawBase.slice(0, -1) : rawBase;

export async function fetchSystems(params) {
  const query = new URLSearchParams(params);
  const url = `${API_BASE}/api/v1/systems/search?${query.toString()}`;
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Search failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function fetchSystemDetail(systemId) {
  const url = `${API_BASE}/api/v1/systems/${systemId}`;
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(`Detail failed: ${res.status} ${detail}`);
  }
  return res.json();
}
