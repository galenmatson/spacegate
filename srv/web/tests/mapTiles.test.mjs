import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import {
  decodeMapTile,
  MapTileManager,
  mapTileRequestPriority,
  progressiveSampleStages,
  tileIntersectsSphere,
} from "../src/mapTiles.js";

const webRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = resolve(webRoot, "../..");
const encoded = execFileSync(resolve(repoRoot, ".venv/bin/python"), ["-c", `
import sys
sys.path.insert(0, ${JSON.stringify(repoRoot)})
from scripts.build_map_tiles import encode_tile
row=(17788193,'canon:system:test','Test Pulsar',0.0,0.0,0.0,0.0,30.0,'PULSAR',6,0,1,0,5772.0,['A','A','M','M','M','M'],'Test abbrev','Test catalog','Test source')
raw,_=encode_tile(depth=4,x=8,y=8,z=8,rows=[row],exact=True,represented_count=1)
sys.stdout.buffer.write(raw)
`], { cwd: webRoot });
const decoded = await decodeMapTile(encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength));
assert.equal(decoded.header.tile_id, "d4-e00");
assert.equal(decoded.systems[0].system_id, 17788193);
assert.equal(decoded.systems[0].display_name, "Test Pulsar");
assert.equal(decoded.systems[0].display_names.astronomer_abbrev, "Test abbrev");
assert.equal(decoded.systems[0].display_names.source_technical, "Test source");
assert.equal(decoded.systems[0].representative_stellar_class, "PULSAR");
assert.deepEqual(decoded.systems[0].stellar_class_badges, ["A", "A", "M", "M", "M", "M"]);
assert.deepEqual(
  decoded.systems[0].x_helio_ly.toFixed(4),
  "0.0000",
);
const tile = {
  exact: true,
  depth: 4,
  origin_ly: [100, 0, 0],
  bounds_min_ly: [64, -64, -64],
  bounds_max_ly: [128, 64, 64],
  interest: { top_k_mean: 1 },
};
const baselinePriority = mapTileRequestPriority(tile, { now: 1000, queuedAt: 1000 });
const directionalPriority = mapTileRequestPriority(tile, { direction: [1, 0, 0], now: 1000, queuedAt: 1000 });
const urgentPriority = mapTileRequestPriority(tile, { urgent: [100, 0, 0], now: 1000, queuedAt: 1000 });
assert(directionalPriority > baselinePriority);
assert(urgentPriority > directionalPriority);
const nearby = { ...tile, origin_ly: [20, 0, 0], interest: { top_k_mean: 0 } };
const distantCool = { ...tile, origin_ly: [200, 0, 0], interest: { top_k_mean: 1 } };
assert(mapTileRequestPriority(nearby, { now: 1000 }) > mapTileRequestPriority(distantCool, { now: 1000 }));
assert.equal(tileIntersectsSphere(tile, [0, 0, 0], 64), true);
assert.equal(tileIntersectsSphere(tile, [-10, 0, 0], 64), false);
let receiverWasUndefined = true;
const batches = [];
const mockFetch = function (url) {
  receiverWasUndefined &&= this === undefined;
  if (url === "/map-tiles/index.json") {
    return Promise.resolve(new Response(JSON.stringify({ build_id: "test", public_radii_ly: [100] })));
  }
  if (url === "/map-tiles/radius-100/manifest.json") {
    return Promise.resolve(new Response(JSON.stringify({
      build_id: "test",
      tiles: [{
        exact: true,
        depth: 4,
        tile_id: "d4-e00",
        sha256: "test",
        url: "/map-tiles/test",
        compressed_bytes: encoded.length,
        bounds_min_ly: [-64, -64, -64],
        bounds_max_ly: [64, 64, 64],
      }],
      counts: { eligible_systems: 1 },
      coolness_profile: {},
    })));
  }
  return Promise.resolve(new Response(encoded));
};
const manager = new MapTileManager({ fetchImpl: mockFetch, onBatch: (systems) => batches.push(...systems) });
await manager.loadRadius(100);
assert.equal(receiverWasUndefined, true);
assert.equal(batches.length, 1);
const detail = await manager.loadDetailBubble([0, 0, 0], 10);
assert.equal(detail.length, 1);
assert.equal(detail[0].display_name, "Test Pulsar");

const progressiveManifest = {
  build_id: "test-deep",
  tiles: [
    {
      exact: false,
      depth: 3,
      tile_id: "d3-fine",
      sha256: "fine",
      url: "/map-tiles/fine",
      compressed_bytes: encoded.length,
      bounds_min_ly: [-256, -256, -256],
      bounds_max_ly: [256, 256, 256],
    },
    {
      exact: false,
      depth: 2,
      tile_id: "d2-coarse",
      sha256: "coarse",
      url: "/map-tiles/coarse",
      compressed_bytes: encoded.length,
      bounds_min_ly: [-512, -512, -512],
      bounds_max_ly: [512, 512, 512],
    },
  ],
  counts: { eligible_systems: 1000 },
  coolness_profile: {},
};
assert.deepEqual(progressiveSampleStages(progressiveManifest).map((stage) => stage.depth), [2, 3]);
const progressiveBatches = [];
const replacements = [];
const progressiveFetch = (url) => {
  if (url === "/map-tiles/index.json") {
    return Promise.resolve(new Response(JSON.stringify({ build_id: "test-deep", public_radii_ly: [500] })));
  }
  if (url === "/map-tiles/radius-500/manifest.json") {
    return Promise.resolve(new Response(JSON.stringify(progressiveManifest)));
  }
  return Promise.resolve(new Response(encoded));
};
const progressiveManager = new MapTileManager({
  fetchImpl: progressiveFetch,
  onBatch: (_systems, tileMetadata) => progressiveBatches.push(tileMetadata.stage_depth),
  onReplace: (replacement) => replacements.push(replacement),
});
const progressiveResult = await progressiveManager.loadRadius(500);
assert.deepEqual(progressiveBatches, [2, 3]);
assert.deepEqual(replacements[0].remove_tile_ids, ["d2-coarse"]);
assert.equal(progressiveResult.stats.progressive, true);
assert.equal(progressiveResult.stats.exact_systems, 0);
assert.equal(progressiveResult.stats.complete, true);
process.stdout.write("map tile decoder ok\n");
