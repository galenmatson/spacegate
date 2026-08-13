import assert from "node:assert/strict";
import test from "node:test";

import {
  focusBreadcrumb,
  focusKeyForPayload,
  formatAuDistance,
  formatLightTravelTime,
  formatMetricDistance,
  niceScaleLength,
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
