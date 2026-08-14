import assert from "node:assert/strict";
import test from "node:test";

import {
  focusBreadcrumb,
  focusNavigationNeighbors,
  focusKeyForPayload,
  formatAuDistance,
  formatLightTravelTime,
  formatMetricDistance,
  layoutBoundedIndicators,
  niceScaleLength,
  physicalFocusLabelLayout,
  physicalScaleResolution,
  sceneUnitsPerAu,
} from "../src/simulationPhysicalScale.js";

const graph = {
  schema_version: "simulation_focus_graph_v1",
  root_focus_key: "focus:root",
  nodes: {
    "focus:root": { display_name: "System", parent_focus_key: null, child_focus_keys: ["focus:star"], physical_bounds: { radius_au: 100 } },
    "focus:star": { display_name: "A", parent_focus_key: "focus:root", child_focus_keys: [], object_key: "star:a", physical_bounds: { radius_au: 2 } },
  },
};

test("physical focus uses one linear AU transform", () => {
  assert.ok(Math.abs(sceneUnitsPerAu(graph, "focus:root") - 0.052) < 1e-12);
  assert.equal(sceneUnitsPerAu(graph, "focus:star"), 2.6);
});

test("focus lookup and breadcrumb retain hierarchy", () => {
  assert.equal(focusKeyForPayload(graph, { id: "star:a" }), "focus:star");
  assert.deepEqual(focusBreadcrumb(graph, "focus:star").map((item) => item.label), ["System", "A"]);
});

test("focus navigation cycles through the complete hierarchy without dead ends", () => {
  const nested = {
    schema_version: "simulation_focus_graph_v1",
    root_focus_key: "root",
    nodes: {
      root: { focus_key: "root", parent_focus_key: null, child_focus_keys: ["wrapper"] },
      wrapper: { focus_key: "wrapper", parent_focus_key: "root", child_focus_keys: ["a", "b"], orbit_key: "orbit:wrapper" },
      a: { focus_key: "a", parent_focus_key: "wrapper", child_focus_keys: [], object_key: "star:a" },
      b: { focus_key: "b", parent_focus_key: "wrapper", child_focus_keys: [], object_key: "star:b" },
    },
  };
  assert.deepEqual(focusNavigationNeighbors(nested, "root"), {
    previous: "b",
    next: "wrapper",
    mode: "system-cycle",
  });
  assert.deepEqual(focusNavigationNeighbors(nested, "a"), {
    previous: "wrapper",
    next: "b",
    mode: "system-cycle",
  });
  assert.equal(focusNavigationNeighbors(nested, "b").next, "root");
  const labels = physicalFocusLabelLayout(nested, "root");
  assert.deepEqual([...labels.keys()], ["orbit:wrapper", "star:a", "star:b"]);
  assert.notEqual(labels.get("star:a").side, labels.get("star:b").side);
});

test("a scale-less focus retains the nearest defensible parent ruler", () => {
  const nested = {
    schema_version: "simulation_focus_graph_v1",
    root_focus_key: "root",
    nodes: {
      root: { focus_key: "root", parent_focus_key: null, child_focus_keys: ["unknown"], physical_bounds: { radius_au: 120 } },
      unknown: { focus_key: "unknown", parent_focus_key: "root", child_focus_keys: [], physical_bounds: { radius_au: null } },
    },
  };
  assert.deepEqual(physicalScaleResolution(nested, "unknown"), {
    requestedFocusKey: "unknown",
    scaleFocusKey: "root",
    radiusAu: 120,
    inherited: true,
  });
  assert.ok(Math.abs(sceneUnitsPerAu(nested, "unknown") - (5.2 / 120)) < 1e-12);
});

test("scale labels use AU, standard metre prefixes, and light time", () => {
  assert.equal(formatAuDistance(1), "1 AU");
  assert.equal(formatMetricDistance(1), "149.6 Gm");
  assert.equal(formatLightTravelTime(1), "8.3 light-minutes");
  assert.equal(formatAuDistance(10_000), "10 kAU");
});

test("scale ruler uses 1-2-5 steps", () => {
  assert.equal(niceScaleLength(37), 20);
  assert.equal(niceScaleLength(7.4), 5);
  assert.equal(niceScaleLength(0.18), 0.1);
});

test("scale beacons are bounded and collision managed", () => {
  const laidOut = layoutBoundedIndicators([
    { focusKey: "a", unresolved: true, x: 200, y: 120, viewportWidth: 400, viewportHeight: 240 },
    { focusKey: "b", unresolved: true, x: 202, y: 121, viewportWidth: 400, viewportHeight: 240 },
    { focusKey: "c", offscreen: true, x: 390, y: 220, viewportWidth: 400, viewportHeight: 240 },
  ]);
  assert.equal(laidOut.length, 3);
  assert.ok(Math.abs(laidOut[0].displayX - laidOut[0].targetX) >= 90);
  assert.ok(laidOut[0].leaderLength >= 8);
  assert.ok(Math.abs(laidOut[0].displayY - laidOut[1].displayY) >= 30);
  assert.ok(laidOut[2].displayX <= 328);
  assert.ok(laidOut[2].displayY <= 212);
});
