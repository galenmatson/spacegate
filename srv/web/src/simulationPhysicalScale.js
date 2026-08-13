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

function stableIndicatorDirection(value, salt = 0) {
  const text = `${String(value || "indicator")}:${salt}`;
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % 2 === 0 ? -1 : 1;
}

export function layoutBoundedIndicators(indicators, limit = 5) {
  const candidates = (indicators || []).filter((item) => item?.offscreen || item?.unresolved).slice(0, limit);
  const placed = [];
  for (const item of candidates) {
    const width = Math.max(160, Number(item.viewportWidth) || 640);
    const height = Math.max(120, Number(item.viewportHeight) || 360);
    const targetX = Math.min(width - 8, Math.max(8, Number(item.x) || width / 2));
    const targetY = Math.min(height - 8, Math.max(8, Number(item.y) || height / 2));
    let baseX = Math.min(width - 72, Math.max(72, targetX));
    let baseY = Math.min(height - 28, Math.max(28, targetY));
    if (item.unresolved && !item.offscreen) {
      const preferredDirection = stableIndicatorDirection(item.focusKey, 0);
      const preferredX = Math.min(width - 82, Math.max(82, targetX + preferredDirection * 118));
      const alternateX = Math.min(width - 82, Math.max(82, targetX - preferredDirection * 118));
      baseX = Math.abs(preferredX - targetX) >= Math.abs(alternateX - targetX) ? preferredX : alternateX;
      baseY = Math.min(height - 30, Math.max(30, targetY + stableIndicatorDirection(item.focusKey, 1) * 22));
    }
    const offsets = [0, -34, 34, -68, 68, -102, 102];
    const offset = offsets.find((value) => {
      const y = Math.min(height - 28, Math.max(28, baseY + value));
      return placed.every((other) => Math.abs(other.displayX - baseX) >= 136 || Math.abs(other.displayY - y) >= 30);
    });
    if (offset === undefined) continue;
    const displayY = Math.min(height - 28, Math.max(28, baseY + offset));
    const targetDeltaX = targetX - baseX;
    const targetDeltaY = targetY - displayY;
    const absoluteX = Math.abs(targetDeltaX);
    const absoluteY = Math.abs(targetDeltaY);
    const edgeScale = item.unresolved
      ? Math.min(
        absoluteX > 0 ? 72 / absoluteX : Number.POSITIVE_INFINITY,
        absoluteY > 0 ? 20 / absoluteY : Number.POSITIVE_INFINITY,
        1,
      )
      : 0;
    const leaderStartX = targetDeltaX * edgeScale;
    const leaderStartY = targetDeltaY * edgeScale;
    const leaderDeltaX = targetDeltaX - leaderStartX;
    const leaderDeltaY = targetDeltaY - leaderStartY;
    placed.push({
      ...item,
      displayX: baseX,
      displayY,
      targetX,
      targetY,
      leaderStartX,
      leaderStartY,
      leaderLength: Math.max(0, Math.hypot(leaderDeltaX, leaderDeltaY) - 3),
      leaderAngleDeg: Math.atan2(leaderDeltaY, leaderDeltaX) * 180 / Math.PI,
    });
  }
  return placed;
}
