import assert from "node:assert/strict";
import test from "node:test";

import { mapDeviceDefaultsFor } from "../src/mapDeviceDefaults.js";

const accelerated = {
  webgl2: true,
  hardwareAccelerated: true,
  maxTextureSize: 16384,
};

test("enhanced desktop defaults to the full bright exact map", () => {
  assert.deepEqual(mapDeviceDefaultsFor({
    ...accelerated,
    width: 1600,
    touch: false,
    cores: 16,
    memoryGiB: 8,
  }), {
    tier: "enhanced_desktop",
    radiusLy: 1000,
    densityMode: "exact",
    starRenderMode: "bright",
  });
});

test("ordinary accelerated desktop defaults to 500 ly balanced bright", () => {
  assert.deepEqual(mapDeviceDefaultsFor({
    ...accelerated,
    width: 1180,
    touch: false,
    cores: 8,
    memoryGiB: 8,
  }), {
    tier: "standard_desktop",
    radiusLy: 500,
    densityMode: "balanced",
    starRenderMode: "bright",
  });
});

test("strong touch hardware starts conservatively below the full sphere", () => {
  assert.deepEqual(mapDeviceDefaultsFor({
    ...accelerated,
    width: 480,
    touch: true,
    cores: 8,
    memoryGiB: 8,
  }), {
    tier: "enhanced_touch",
    radiusLy: 500,
    densityMode: "balanced",
    starRenderMode: "bright",
  });
});

test("software-rendered or low-memory devices use constrained defaults", () => {
  assert.deepEqual(mapDeviceDefaultsFor({
    width: 1440,
    touch: false,
    cores: 8,
    memoryGiB: 8,
    webgl2: true,
    hardwareAccelerated: false,
    maxTextureSize: 16384,
  }), {
    tier: "constrained_desktop",
    radiusLy: 250,
    densityMode: "performance",
    starRenderMode: "discovery",
  });
  assert.equal(mapDeviceDefaultsFor({
    ...accelerated,
    width: 412,
    touch: true,
    cores: 4,
    memoryGiB: 4,
  }).radiusLy, 100);
});
