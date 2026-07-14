export const MAP_DENSITY_MODES = {
  balanced: {
    id: "balanced",
    label: "Balanced",
    title: "Full nearby detail with a gradual transition to spatially stable background context.",
    backgroundProbability: 1 / 7,
    detailInnerLy: 45,
    detailOuterLy: 105,
    recenterLy: 18,
  },
  exact: {
    id: "exact",
    label: "Exact",
    title: "Render every catalog system in the selected radius.",
    backgroundProbability: 1,
    detailInnerLy: 0,
    detailOuterLy: 0,
    recenterLy: Infinity,
  },
  performance: {
    id: "performance",
    label: "Performance",
    title: "Use a smaller detail bubble and lighter stable background density.",
    backgroundProbability: 1 / 11,
    detailInnerLy: 32,
    detailOuterLy: 82,
    recenterLy: 16,
  },
};

export const MAP_DENSITY_MODE_OPTIONS = Object.values(MAP_DENSITY_MODES);

export function normalizeMapDensityMode(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return MAP_DENSITY_MODES[normalized]?.id || "balanced";
}

export function mapDensityProfile(mode) {
  return MAP_DENSITY_MODES[normalizeMapDensityMode(mode)];
}

export function stableMapSampleUnit(systemId) {
  const text = String(systemId ?? "");
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) / 0x1_0000_0000;
}

export function isPersistentMapSystem(system) {
  return Number(system?.planet_count || 0) > 0
    || Number(system?.star_count || 0) > 1
    || Number(system?.coolness_rank || Number.POSITIVE_INFINITY) <= 15000;
}

export function includeBackgroundMapPoint(system, mode) {
  const profile = mapDensityProfile(mode);
  return profile.backgroundProbability >= 1
    || isPersistentMapSystem(system)
    || stableMapSampleUnit(system?.system_id) < profile.backgroundProbability;
}

export function systemDistanceFrom(system, centerLy) {
  const center = Array.isArray(centerLy) && centerLy.length === 3 ? centerLy : [0, 0, 0];
  return Math.hypot(
    Number(system?.x_helio_ly || 0) - Number(center[0] || 0),
    Number(system?.y_helio_ly || 0) - Number(center[1] || 0),
    Number(system?.z_helio_ly || 0) - Number(center[2] || 0),
  );
}

export function mapPointInclusionProbability(system, centerLy, mode) {
  const profile = mapDensityProfile(mode);
  if (profile.backgroundProbability >= 1 || isPersistentMapSystem(system)) return 1;
  const distance = systemDistanceFrom(system, centerLy);
  if (distance <= profile.detailInnerLy) return 1;
  if (distance >= profile.detailOuterLy) return profile.backgroundProbability;
  const linear = (profile.detailOuterLy - distance) / (profile.detailOuterLy - profile.detailInnerLy);
  const smooth = linear * linear * (3 - 2 * linear);
  return profile.backgroundProbability + (1 - profile.backgroundProbability) * smooth;
}

export function includeDetailedMapPoint(system, centerLy, mode) {
  if (includeBackgroundMapPoint(system, mode)) return false;
  return stableMapSampleUnit(system?.system_id) < mapPointInclusionProbability(system, centerLy, mode);
}

export function cameraMovedBeyond(previousCenter, nextCenter, thresholdLy) {
  if (!Array.isArray(previousCenter) || previousCenter.length !== 3) return true;
  if (!Array.isArray(nextCenter) || nextCenter.length !== 3) return false;
  return Math.hypot(
    Number(nextCenter[0] || 0) - Number(previousCenter[0] || 0),
    Number(nextCenter[1] || 0) - Number(previousCenter[1] || 0),
    Number(nextCenter[2] || 0) - Number(previousCenter[2] || 0),
  ) >= Number(thresholdLy || 0);
}

export function radialDensitySeamRatio(systems, boundaryLy = 110, shellWidthLy = 20) {
  const innerMin = boundaryLy - shellWidthLy;
  const outerMax = boundaryLy + shellWidthLy;
  let innerCount = 0;
  let outerCount = 0;
  for (const system of systems || []) {
    const distance = Number(system?.dist_ly);
    if (!Number.isFinite(distance)) continue;
    if (distance >= innerMin && distance < boundaryLy) innerCount += 1;
    else if (distance >= boundaryLy && distance < outerMax) outerCount += 1;
  }
  const shellVolume = (inner, outer) => (4 / 3) * Math.PI * (outer ** 3 - inner ** 3);
  const innerDensity = innerCount / shellVolume(innerMin, boundaryLy);
  const outerDensity = outerCount / shellVolume(boundaryLy, outerMax);
  return outerDensity > 0 ? innerDensity / outerDensity : null;
}
