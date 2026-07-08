const MAP_RETURN_STATE_STORAGE_PREFIX = "spacegate.map.return.";
const LY_TO_SCENE = 0.55;

function mapReturnStorageKey(token) {
  const safeToken = String(token || "").replace(/[^a-zA-Z0-9_-]/g, "");
  return safeToken ? `${MAP_RETURN_STATE_STORAGE_PREFIX}${safeToken}` : "";
}

export function readStoredMapReturnState(token) {
  if (typeof window === "undefined") {
    return null;
  }
  const key = mapReturnStorageKey(token);
  if (!key) {
    return null;
  }
  try {
    const stored = window.sessionStorage.getItem(key);
    return stored ? JSON.parse(stored) : null;
  } catch {
    return null;
  }
}

export function writeStoredMapReturnState(state) {
  if (typeof window === "undefined") {
    return "";
  }
  const token = `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
  const key = mapReturnStorageKey(token);
  if (!key) {
    return "";
  }
  try {
    window.sessionStorage.setItem(key, JSON.stringify({
      ...state,
      savedAt: new Date().toISOString(),
    }));
    return token;
  } catch {
    return "";
  }
}

function systemScenePosition(system) {
  const x = Number(system?.x_helio_ly);
  const y = Number(system?.y_helio_ly);
  const z = Number(system?.z_helio_ly);
  if (![x, y, z].every(Number.isFinite)) {
    return null;
  }
  return [x * LY_TO_SCENE, z * LY_TO_SCENE, -y * LY_TO_SCENE];
}

export function mapExploreHrefForSystem(system) {
  const systemId = system?.system_id;
  if (!systemId) {
    return "/map";
  }
  const scenePosition = systemScenePosition(system);
  const camera = scenePosition
    ? {
        position: [scenePosition[0], scenePosition[1] + 3.5, scenePosition[2] + 17],
        yaw: 0,
        pitch: -0.08,
      }
    : undefined;
  const token = writeStoredMapReturnState({
    camera,
    selectedSystemId: systemId,
    selectedSystemName: system.display_name || system.system_name || "",
    selectedSystem: system,
    drillMode: "explore",
    mapFrame: "icrs",
    showDirectionLabels: false,
    selectionHistoryIds: [systemId],
  });
  return token ? `/map?restore=${encodeURIComponent(token)}` : "/map";
}
