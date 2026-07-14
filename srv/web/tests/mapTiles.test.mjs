import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { decodeMapTile, mapTileRequestPriority } from "../src/mapTiles.js";

const webRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = resolve(webRoot, "../..");
const encoded = execFileSync(resolve(repoRoot, ".venv/bin/python"), ["-c", `
import sys
sys.path.insert(0, ${JSON.stringify(repoRoot)})
from scripts.build_map_tiles import encode_tile
row=(17788193,'canon:system:sol','Sol',0.0,0.0,0.0,0.0,30.0,'G',1,8,1,1,5772.0,'Sol abbrev','Sol catalog','Sol source')
raw,_=encode_tile(depth=4,x=8,y=8,z=8,rows=[row],exact=True,represented_count=1)
sys.stdout.buffer.write(raw)
`], { cwd: webRoot });
const decoded = await decodeMapTile(encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength));
assert.equal(decoded.header.tile_id, "d4-e00");
assert.equal(decoded.systems[0].system_id, 17788193);
assert.equal(decoded.systems[0].display_name, "Sol");
assert.equal(decoded.systems[0].display_names.astronomer_abbrev, "Sol abbrev");
assert.equal(decoded.systems[0].display_names.source_technical, "Sol source");
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
process.stdout.write("map tile decoder ok\n");
