import fs from "node:fs/promises";
import process from "node:process";
import { mapTileRequestPriority } from "../src/mapTiles.js";

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, "").split("=");
  return [key, rest.join("=") || true];
}));
if (!args.manifest) throw new Error("Pass --manifest=/path/to/radius manifest.json");
const manifest = JSON.parse(await fs.readFile(String(args.manifest), "utf8"));
const exact = manifest.tiles.filter((tile) => tile.exact);
const distance = (tile) => Math.hypot(...tile.origin_ly);
const baseline = [...exact].sort((left, right) => distance(left) - distance(right));
const interested = [...exact].sort((left, right) => (
  mapTileRequestPriority(right, { focus: [0, 0, 0], now: 0, queuedAt: 0 })
  - mapTileRequestPriority(left, { focus: [0, 0, 0], now: 0, queuedAt: 0 })
));
const baselineRank = new Map(baseline.map((tile, index) => [tile.tile_id, index + 1]));
const interestedRank = new Map(interested.map((tile, index) => [tile.tile_id, index + 1]));
const percentile = (values, fraction) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))] ?? null;
};
const distantThreshold = percentile(exact.map(distance), 0.5);
const distant = exact.filter((tile) => distance(tile) >= distantThreshold);
const highInterest = [...distant]
  .sort((left, right) => Number(right.interest?.top_k_mean || 0) - Number(left.interest?.top_k_mean || 0))
  .slice(0, Math.max(1, Math.ceil(exact.length * 0.1)));
const nearbyThreshold = percentile(exact.map(distance), 0.25);
const nearby = exact.filter((tile) => distance(tile) <= nearbyThreshold);
const result = {
  schema_version: "spacegate_map_interest_trace_v1",
  radius_ly: manifest.radius_ly,
  manifest_sha256: manifest.manifest_sha256,
  coolness_profile: manifest.coolness_profile,
  exact_tiles: exact.length,
  high_interest_tiles: highInterest.length,
  distant_tile_threshold_ly: distantThreshold,
  high_interest_median_rank: {
    distance_only: percentile(highInterest.map((tile) => baselineRank.get(tile.tile_id)), 0.5),
    bounded_interest: percentile(highInterest.map((tile) => interestedRank.get(tile.tile_id)), 0.5),
  },
  nearby_tile_p95_rank: {
    distance_only: percentile(nearby.map((tile) => baselineRank.get(tile.tile_id)), 0.95),
    bounded_interest: percentile(nearby.map((tile) => interestedRank.get(tile.tile_id)), 0.95),
  },
};
result.passed = (
  result.high_interest_median_rank.bounded_interest <= result.high_interest_median_rank.distance_only
  && result.nearby_tile_p95_rank.bounded_interest <= result.nearby_tile_p95_rank.distance_only + 5
);
const encoded = `${JSON.stringify(result, null, 2)}\n`;
if (args.output) await fs.writeFile(String(args.output), encoded);
process.stdout.write(encoded);
if (!result.passed) process.exitCode = 1;
