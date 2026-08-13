export const PHYSICAL_SCALE_MODE = "physical";
export const PHYSICAL_SCENE_RADIUS = 5.2;
export const AU_IN_METRES = 149_597_870_700;
export const LIGHT_SECONDS_PER_AU = 499.004783836;

function finitePositive(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

export function focusGraphNodes(focusGraph) {
  return focusGraph?.schema_version === "simulation_focus_graph_v1" && focusGraph?.nodes
    ? focusGraph.nodes
    : {};
}

export function focusNode(focusGraph, focusKey) {
  const nodes = focusGraphNodes(focusGraph);
  return nodes[focusKey] || nodes[focusGraph?.root_focus_key] || null;
}

export function focusRadiusAu(focusGraph, focusKey) {
  return finitePositive(focusNode(focusGraph, focusKey)?.physical_bounds?.radius_au);
}

export function sceneUnitsPerAu(focusGraph, focusKey, sceneRadius = PHYSICAL_SCENE_RADIUS) {
  const radiusAu = focusRadiusAu(focusGraph, focusKey);
  return radiusAu ? sceneRadius / radiusAu : null;
}

export function focusKeyForPayload(focusGraph, payload) {
  const payloadKey = String(payload?.id || "");
  if (!payloadKey) return null;
  const nodes = focusGraphNodes(focusGraph);
  return Object.keys(nodes).find((key) => (
    String(nodes[key]?.object_key || "") === payloadKey
    || String(nodes[key]?.orbit_key || "") === payloadKey
    || String(nodes[key]?.tree_node_key || "") === payloadKey
  )) || null;
}

export function focusBreadcrumb(focusGraph, focusKey) {
  const nodes = focusGraphNodes(focusGraph);
  const path = [];
  const visited = new Set();
  let key = focusKey || focusGraph?.root_focus_key;
  while (key && nodes[key] && !visited.has(key)) {
    visited.add(key);
    path.unshift({ key, label: nodes[key].display_name || key });
    key = nodes[key].parent_focus_key;
  }
  return path;
}

export function siblingFocusKeys(focusGraph, focusKey) {
  const nodes = focusGraphNodes(focusGraph);
  const node = nodes[focusKey];
  const parent = node?.parent_focus_key ? nodes[node.parent_focus_key] : null;
  return parent?.child_focus_keys || [];
}

export function niceScaleLength(value) {
  const positive = finitePositive(value);
  if (!positive) return null;
  const exponent = Math.floor(Math.log10(positive));
  const fraction = positive / 10 ** exponent;
  const niceFraction = fraction >= 5 ? 5 : fraction >= 2 ? 2 : 1;
  return niceFraction * 10 ** exponent;
}

function formatValue(value, maximumFractionDigits = 2) {
  return Number(value).toLocaleString(undefined, { maximumFractionDigits });
}

export function formatAuDistance(au) {
  const value = finitePositive(au);
  if (!value) return "Scale unavailable";
  if (value >= 63_241.077) return `${formatValue(value / 63_241.077, 2)} ly`;
  if (value >= 1_000) return `${formatValue(value / 1_000, 2)} kAU`;
  if (value >= 0.01) return `${formatValue(value, value < 1 ? 3 : 2)} AU`;
  return `${formatValue(value * 149_597_870.7, 0)} km`;
}

export function formatMetricDistance(au) {
  const metres = finitePositive(au) * AU_IN_METRES;
  if (!metres) return "";
  const units = [
    [1e15, "Pm"],
    [1e12, "Tm"],
    [1e9, "Gm"],
    [1e6, "Mm"],
    [1e3, "km"],
  ];
  const [divisor, unit] = units.find(([threshold]) => metres >= threshold) || [1, "m"];
  return `${formatValue(metres / divisor, 2)} ${unit}`;
}

export function formatLightTravelTime(au) {
  const seconds = finitePositive(au) * LIGHT_SECONDS_PER_AU;
  if (!seconds) return "";
  if (seconds < 120) return `${formatValue(seconds, 1)} light-seconds`;
  const minutes = seconds / 60;
  if (minutes < 120) return `${formatValue(minutes, 1)} light-minutes`;
  const hours = minutes / 60;
  if (hours < 72) return `${formatValue(hours, 1)} light-hours`;
  const days = hours / 24;
  if (days < 730) return `${formatValue(days, 1)} light-days`;
  return `${formatValue(days / 365.25, 2)} light-years`;
}

export function physicalScaleReadout(au) {
  const value = finitePositive(au);
  if (!value) return null;
  return {
    au: value,
    primary: formatAuDistance(value),
    metric: formatMetricDistance(value),
    lightTime: formatLightTravelTime(value),
  };
}

export function physicalOrbitAxis(orbit) {
  const extent = orbit?.physical_extent;
  if (extent?.applicability !== "physical") return null;
  return finitePositive(extent?.semi_major_axis_au?.value);
}

