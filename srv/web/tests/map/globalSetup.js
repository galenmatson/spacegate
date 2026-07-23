const baseUrl = () =>
  String(process.env.SPACEGATE_MAP_BASE_URL || "https://10.0.0.12").replace(/\/+$/, "");

async function fetchJson(path) {
  const response = await fetch(`${baseUrl()}${path}`);
  const contentType = response.headers.get("content-type") || "";
  if (!response.ok) {
    throw new Error(`Map test preflight ${path} returned HTTP ${response.status}`);
  }
  if (!contentType.toLowerCase().includes("json")) {
    throw new Error(
      `Map test preflight ${path} expected JSON but received ${contentType || "unknown content type"}`,
    );
  }
  return response.json();
}

export default async function globalSetup() {
  const [health, index] = await Promise.all([
    fetchJson("/api/v1/health"),
    fetchJson("/map-tiles/index.json"),
  ]);
  if (health.status !== "ok" || !health.build_id) {
    throw new Error("Map test preflight API health has no active build identity");
  }
  if (!index.build_id || index.build_id !== health.build_id) {
    throw new Error(
      `Map test preflight build mismatch: API=${health.build_id || "missing"} tiles=${index.build_id || "missing"}`,
    );
  }
  for (const radius of [100, 250, 500, 1000]) {
    if (!index.manifests?.[String(radius)]) {
      throw new Error(`Map test preflight index is missing the ${radius}-ly manifest`);
    }
  }
}
