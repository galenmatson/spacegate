import fs from "node:fs/promises";
import process from "node:process";

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, "").split("=");
  return [key, rest.join("=") || true];
}));
for (const key of ["baseline", "tiled100", "tiled250", "warm250", "rapid250", "output"]) {
  if (!args[key]) throw new Error(`Missing --${key}=...`);
}
const read = async (key) => JSON.parse(await fs.readFile(String(args[key]), "utf8"));
const [baseline, tiled100, tiled250, warm250, rapid250] = await Promise.all([
  read("baseline"), read("tiled100"), read("tiled250"), read("warm250"), read("rapid250"),
]);
const baselineByProfile = new Map(baseline.results.map((row) => [row.profile, row]));
const checks = [];
const check = (scenario, profile, metric, actual, budget, predicate = (value, limit) => value <= limit) => {
  checks.push({ scenario, profile, metric, actual, budget, passed: Boolean(predicate(actual, budget)) });
};

for (const row of tiled100.results) {
  const before = baselineByProfile.get(row.profile);
  check("tiled_100_cold", row.profile, "usable_ms", row.usable_ms, Math.max(4000, before.usable_ms * 1.25));
  check("tiled_100_cold", row.profile, "settle_ms", row.visible_region_settle_ms, Math.max(5000, before.usable_ms * 1.5));
  check("tiled_100_cold", row.profile, "requests", row.network.requests, 36);
  check("tiled_100_cold", row.profile, "p95_frame_ms", row.frame_time_ms.p95, Math.max(60, before.frame_time_ms.p95 * 1.25));
  check("tiled_100_cold", row.profile, "heap_bytes", row.heap?.usedJSHeapSize || 0, Math.max(96_000_000, (before.heap?.usedJSHeapSize || 0) * 2));
  check("tiled_100_cold", row.profile, "selection_ms", row.selection_ms, Math.max(750, before.selection_ms * 2));
  check("tiled_100_cold", row.profile, "rendered_points", Number(row.renderer.mapStarCount), 10_239, (value, limit) => value === limit);
  check("tiled_100_cold", row.profile, "tile_failures", Number(row.renderer.mapTileFailures), 0, (value) => value === 0);
}

for (const [scenario, payload] of [
  ["tiled_250_cold", tiled250],
  ["tiled_250_warm", warm250],
  ["tiled_250_rapid_direction", rapid250],
]) {
  for (const row of payload.results) {
    const mobile = row.profile === "mobile";
    check(scenario, row.profile, "usable_ms", row.usable_ms, 4000);
    check(scenario, row.profile, "settle_ms", row.visible_region_settle_ms, 6000);
    check(scenario, row.profile, "requests", row.network.requests, 100);
    check(scenario, row.profile, "encoded_bytes", row.network.encodedBytes, 16_000_000);
    check(scenario, row.profile, "median_frame_ms", row.frame_time_ms.median, 50);
    check(scenario, row.profile, "p95_frame_ms", row.frame_time_ms.p95, 84);
    check(scenario, row.profile, "long_task_total_ms", row.long_tasks.total_ms, 1500);
    check(scenario, row.profile, "heap_bytes", row.heap?.usedJSHeapSize || 0, scenario === "tiled_250_warm" ? 320_000_000 : 180_000_000);
    check(scenario, row.profile, "selection_ms", row.selection_ms, 750);
    check(scenario, row.profile, "rendered_points_min", Number(row.renderer.mapStarCount), 20_000, (value, limit) => value >= limit);
    check(scenario, row.profile, "rendered_points_max", Number(row.renderer.mapStarCount), mobile ? 40_000 : 50_000);
    check(scenario, row.profile, "tile_completion", Number(row.renderer.mapTilesLoaded), Number(row.renderer.mapTilesQueued), (value, limit) => value === limit);
    check(scenario, row.profile, "tile_failures", Number(row.renderer.mapTileFailures), 0, (value) => value === 0);
    check(scenario, row.profile, "radial_seam_ratio_max", Number(row.renderer.mapRadialSeamRatio), 2);
    check(scenario, row.profile, "radial_seam_ratio_min", Number(row.renderer.mapRadialSeamRatio), 0.35, (value, limit) => value >= limit);
    check(scenario, row.profile, "camera_detail_systems", Number(row.renderer.mapDetailSystems), 1, (value, limit) => value >= limit);
    check(
      scenario,
      row.profile,
      "density_profile",
      row.renderer.mapDensityMode,
      mobile ? "performance" : "balanced",
      (value, limit) => value === limit,
    );
    const tileUrls = row.network.urls.filter((url) => url.endsWith(".sgtile.gz"));
    check(scenario, row.profile, "duplicate_tile_requests", tileUrls.length, new Set(tileUrls).size, (value, limit) => value === limit);
  }
}

const report = {
  schema_version: "spacegate_map_performance_acceptance_v1",
  generated_at: new Date().toISOString(),
  baseline_run_id: baseline.run_id,
  after_run_id: tiled250.run_id,
  passed: checks.every((item) => item.passed),
  failed_checks: checks.filter((item) => !item.passed),
  checks,
};
await fs.writeFile(String(args.output), `${JSON.stringify(report, null, 2)}\n`);
process.stdout.write(`${JSON.stringify({ passed: report.passed, checks: checks.length, failed_checks: report.failed_checks }, null, 2)}\n`);
if (!report.passed) process.exitCode = 1;
