import assert from "node:assert/strict";
import {
  cameraMovedBeyond,
  includeBackgroundMapPoint,
  includeDetailedMapPoint,
  mapPointInclusionProbability,
  radialDensitySeamRatio,
  stableMapSampleUnit,
} from "../src/mapLod.js";

const ordinary = (systemId, x) => ({
  system_id: systemId,
  x_helio_ly: x,
  y_helio_ly: 0,
  z_helio_ly: 0,
  star_count: 1,
  planet_count: 0,
  coolness_rank: null,
});

assert.equal(stableMapSampleUnit("42"), stableMapSampleUnit("42"));
assert.equal(cameraMovedBeyond(null, [0, 0, 0], 8), true);
assert.equal(cameraMovedBeyond([0, 0, 0], [17.9, 0, 0], 18), false);
assert.equal(cameraMovedBeyond([0, 0, 0], [18, 0, 0], 18), true);

const inner = mapPointInclusionProbability(ordinary(1, 45), [0, 0, 0], "balanced");
const transition = mapPointInclusionProbability(ordinary(1, 75), [0, 0, 0], "balanced");
const outer = mapPointInclusionProbability(ordinary(1, 105), [0, 0, 0], "balanced");
assert.equal(inner, 1);
assert(inner > transition && transition > outer);
assert.equal(outer, 1 / 7);

const includedAt = (distance) => {
  let count = 0;
  for (let id = 1; id <= 20000; id += 1) {
    const system = ordinary(id, distance);
    if (includeBackgroundMapPoint(system, "balanced") || includeDetailedMapPoint(system, [0, 0, 0], "balanced")) count += 1;
  }
  return count;
};
const justInsideOldBoundary = includedAt(105);
const justOutsideOldBoundary = includedAt(115);
assert(Math.abs(justInsideOldBoundary - justOutsideOldBoundary) / justOutsideOldBoundary < 0.03);

let cameraRelativeCandidate = null;
for (let id = 1; id <= 1000; id += 1) {
  const system = ordinary(id, 150);
  if (!includeBackgroundMapPoint(system, "balanced") && includeDetailedMapPoint(system, [150, 0, 0], "balanced")) {
    cameraRelativeCandidate = system;
    break;
  }
}
assert(cameraRelativeCandidate);
assert.equal(includeDetailedMapPoint(cameraRelativeCandidate, [0, 0, 0], "balanced"), false);
assert.equal(includeDetailedMapPoint(cameraRelativeCandidate, [150, 0, 0], "balanced"), true);

assert.equal(includeBackgroundMapPoint(ordinary(999, 200), "exact"), true);
const syntheticShell = [];
for (let id = 1; id <= 10000; id += 1) syntheticShell.push({ ...ordinary(id, 100), dist_ly: 100 });
for (let id = 10001; id <= 10000 + Math.round(10000 * ((130 ** 3 - 110 ** 3) / (110 ** 3 - 90 ** 3))); id += 1) {
  syntheticShell.push({ ...ordinary(id, 120), dist_ly: 120 });
}
assert(Math.abs(radialDensitySeamRatio(syntheticShell) - 1) < 0.001);
process.stdout.write("map LOD policy ok\n");
