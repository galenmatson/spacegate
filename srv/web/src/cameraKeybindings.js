export const CAMERA_KEYBIND_STORAGE_KEY = "spacegate.map.keybindScheme";
export const CAMERA_KEYBIND_CHANGE_EVENT = "spacegate:camera-keybind-change";

export const CAMERA_KEYBIND_SCHEMES = Object.freeze({
  wasd: Object.freeze({
    id: "wasd",
    label: "WASD",
    forward: "w",
    back: "s",
    left: "a",
    right: "d",
    up: "q",
    down: "z",
    hint: "WASD fly · Q/Z vertical",
  }),
  esdf: Object.freeze({
    id: "esdf",
    label: "ESDF",
    forward: "e",
    back: "d",
    left: "s",
    right: "f",
    up: "a",
    down: "z",
    hint: "ESDF fly · A/Z vertical",
  }),
  num8456: Object.freeze({
    id: "num8456",
    label: "8456",
    forward: "numpad8",
    back: "numpad5",
    left: "numpad4",
    right: "numpad6",
    up: "numpad7",
    down: "numpad1",
    hint: "8456 fly · 7/1 vertical",
  }),
});

export const CAMERA_KEYBIND_OPTIONS = Object.freeze(Object.values(CAMERA_KEYBIND_SCHEMES));

export function normalizeCameraKeybindScheme(value) {
  const key = String(value || "").trim().toLowerCase();
  return CAMERA_KEYBIND_SCHEMES[key] ? key : "wasd";
}

export function readStoredCameraKeybindScheme(storage = globalThis?.localStorage) {
  try {
    return normalizeCameraKeybindScheme(storage?.getItem(CAMERA_KEYBIND_STORAGE_KEY));
  } catch {
    return "wasd";
  }
}

export function writeStoredCameraKeybindScheme(value, storage = globalThis?.localStorage, eventTarget = globalThis?.window) {
  const scheme = normalizeCameraKeybindScheme(value);
  try {
    storage?.setItem(CAMERA_KEYBIND_STORAGE_KEY, scheme);
  } catch {
    // Persistence is optional; the live control still uses the selected scheme.
  }
  if (typeof CustomEvent !== "undefined") {
    eventTarget?.dispatchEvent?.(new CustomEvent(CAMERA_KEYBIND_CHANGE_EVENT, { detail: { scheme } }));
  }
  return scheme;
}

export function cameraMovementToken(event) {
  const code = String(event?.code || "").toLowerCase();
  if (/^numpad[0-9]$/.test(code)) return code;
  return String(event?.key || "").toLowerCase();
}

export function isCameraKeyboardInputTarget(target) {
  if (typeof Element === "undefined") return false;
  const element = target instanceof Element ? target : null;
  return Boolean(element?.closest?.("input, select, textarea, [contenteditable='true']"));
}
