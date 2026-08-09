import assert from "node:assert/strict";
import test from "node:test";

import {
  readSimulationPresentationState,
  writeSimulationPresentationState,
} from "../src/simulationPresentationState.js";

function memoryStorage() {
  const values = new Map();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
  };
}

const defaults = {
  scaleMode: "structure",
  speedMultiplier: 1,
  showOrbits: true,
  showHabitableZones: true,
  showFormationLines: { soot: true, snow: false },
  showLabels: true,
};

test("same-system presentation survives a surface remount without carrying an epoch", () => {
  const storage = memoryStorage();
  assert.equal(writeSimulationPresentationState(42, {
    scaleMode: "log",
    speedMultiplier: 5000,
    showOrbits: false,
    showHabitableZones: false,
    showFormationLines: { soot: false, snow: true },
    showLabels: false,
    simulationDays: 999,
  }, defaults, storage), true);

  assert.deepEqual(readSimulationPresentationState(42, defaults, storage), {
    scaleMode: "log",
    speedMultiplier: 5000,
    showOrbits: false,
    showHabitableZones: false,
    showFormationLines: { soot: false, snow: true },
    showLabels: false,
  });
});

test("a different system receives current global defaults", () => {
  const storage = memoryStorage();
  writeSimulationPresentationState(42, { scaleMode: "log", speedMultiplier: 10000 }, defaults, storage);
  assert.deepEqual(readSimulationPresentationState(43, { ...defaults, scaleMode: "true_orbits" }, storage), {
    ...defaults,
    scaleMode: "true_orbits",
  });
});
