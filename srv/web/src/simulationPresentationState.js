const STORAGE_KEY = "spacegate.systemSimulation.presentation.v1";

const VALID_SCALE_MODES = new Set(["structure", "true_orbits", "true_bodies", "log"]);
const VALID_SPEEDS = new Set([0.25, 1, 5, 20, 100, 500, 1000, 5000, 10000]);

function booleanOr(value, fallback) {
  return typeof value === "boolean" ? value : Boolean(fallback);
}

export function normalizeSimulationPresentationState(value = {}, defaults = {}) {
  const scaleMode = String(value?.scaleMode || defaults?.scaleMode || "structure").trim().toLowerCase();
  const speed = Number(value?.speedMultiplier ?? defaults?.speedMultiplier ?? 1);
  const defaultFormationLines = defaults?.showFormationLines && typeof defaults.showFormationLines === "object"
    ? defaults.showFormationLines
    : {};
  const formationLines = value?.showFormationLines && typeof value.showFormationLines === "object"
    ? value.showFormationLines
    : defaultFormationLines;
  return {
    scaleMode: VALID_SCALE_MODES.has(scaleMode) ? scaleMode : "structure",
    speedMultiplier: VALID_SPEEDS.has(speed) ? speed : 1,
    showOrbits: booleanOr(value?.showOrbits, defaults?.showOrbits ?? true),
    showHabitableZones: booleanOr(value?.showHabitableZones, defaults?.showHabitableZones ?? true),
    showFormationLines: Object.fromEntries(
      Object.keys(defaultFormationLines).map((key) => [key, booleanOr(formationLines[key], defaultFormationLines[key])]),
    ),
    showLabels: booleanOr(value?.showLabels, defaults?.showLabels ?? true),
  };
}

export function readSimulationPresentationState(systemId, defaults = {}, storage = globalThis?.sessionStorage) {
  const fallback = normalizeSimulationPresentationState({}, defaults);
  if (!storage || systemId === null || systemId === undefined || systemId === "") {
    return fallback;
  }
  try {
    const stored = JSON.parse(storage.getItem(STORAGE_KEY) || "null");
    if (String(stored?.system_id || "") !== String(systemId)) {
      return fallback;
    }
    return normalizeSimulationPresentationState(stored?.presentation, defaults);
  } catch (_) {
    return fallback;
  }
}

export function writeSimulationPresentationState(systemId, presentation, defaults = {}, storage = globalThis?.sessionStorage) {
  if (!storage || systemId === null || systemId === undefined || systemId === "") {
    return false;
  }
  const payload = {
    schema_version: "simulation_presentation_session_v1",
    system_id: String(systemId),
    presentation: normalizeSimulationPresentationState(presentation, defaults),
  };
  try {
    storage.setItem(STORAGE_KEY, JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

export const SIMULATION_PRESENTATION_STORAGE_KEY = STORAGE_KEY;
