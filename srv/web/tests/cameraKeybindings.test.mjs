import assert from "node:assert/strict";
import test from "node:test";

import {
  CAMERA_KEYBIND_SCHEMES,
  CAMERA_KEYBIND_STORAGE_KEY,
  cameraMovementToken,
  normalizeCameraKeybindScheme,
  readStoredCameraKeybindScheme,
  writeStoredCameraKeybindScheme,
} from "../src/cameraKeybindings.js";

function memoryStorage() {
  const values = new Map();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
  };
}

test("camera key schemes preserve the map movement vocabulary", () => {
  assert.deepEqual(Object.keys(CAMERA_KEYBIND_SCHEMES), ["wasd", "esdf", "num8456"]);
  assert.equal(CAMERA_KEYBIND_SCHEMES.esdf.forward, "e");
  assert.equal(CAMERA_KEYBIND_SCHEMES.num8456.up, "numpad7");
  assert.equal(normalizeCameraKeybindScheme("ESDF"), "esdf");
  assert.equal(normalizeCameraKeybindScheme("unknown"), "wasd");
});

test("camera preference uses one shared storage contract", () => {
  const storage = memoryStorage();
  assert.equal(writeStoredCameraKeybindScheme("num8456", storage, null), "num8456");
  assert.equal(storage.getItem(CAMERA_KEYBIND_STORAGE_KEY), "num8456");
  assert.equal(readStoredCameraKeybindScheme(storage), "num8456");
});

test("camera movement tokens retain numpad codes and normalize letter keys", () => {
  assert.equal(cameraMovementToken({ code: "Numpad8", key: "ArrowUp" }), "numpad8");
  assert.equal(cameraMovementToken({ code: "KeyE", key: "E" }), "e");
});
