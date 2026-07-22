import fs from "node:fs/promises";
import process from "node:process";

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, "").split("=");
  return [key, rest.join("=") || true];
}));
for (const key of ["deep500", "deep1000", "warm1000", "rapid1000", "output"]) {
  if (!args[key]) throw new Error(`Missing --${key}=...`);
}

const read = async (key) => JSON.parse(await fs.readFile(String(args[key]), "utf8"));
const [deep500, deep1000, warm1000, rapid1000] = await Promise.all([
  read("deep500"), read("deep1000"), read("warm1000"), read("rapid1000"),
]);
const checks = [];
const check = (scenario, profile, metric, actual, budget, predicate = (value, limit) => value <= limit) => {
  checks.push({ scenario, profile, metric, actual, budget, passed: Boolean(predicate(actual, budget)) });
};

for (const [scenario, payload, expectedRadius] of [
  ["deep_500_cold", deep500, 500],
  ["deep_1000_cold", deep1000, 1000],
  ["deep_1000_warm", warm1000, 1000],
  ["deep_1000_rapid_direction", rapid1000, 1000],
]) {
  const radius = Number(payload.radius_ly);
  const expectedEligible = expectedRadius === 500 ? 2_332_007 : 5_869_091;
  for (const row of payload.results) {
    const mobile = row.profile === "mobile";
    const renderer = row.renderer || {};
    const tileUrls = row.network.urls.filter((url) => url.endsWith(".sgtile.gz"));
    const maxRendered = radius === 500 ? 50_000 : 120_000;
    const minRendered = radius === 500 ? 15_000 : 80_000;

    check(scenario, row.profile, "radius_ly", radius, expectedRadius, (value, limit) => value === limit);
    check(scenario, row.profile, "forced_gc_available", row.forced_gc_available, true, (value, limit) => value === limit);
    check(scenario, row.profile, "usable_ms", row.usable_ms, 4_000);
    check(scenario, row.profile, "settle_ms", row.visible_region_settle_ms, radius === 500 ? 6_000 : 20_000);
    check(scenario, row.profile, "requests", row.network.requests, radius === 500 ? 170 : 600);
    check(
      scenario,
      row.profile,
      "encoded_bytes",
      row.network.encodedBytes,
      scenario === "deep_1000_warm" ? 6_000_000 : radius === 500 ? 20_000_000 : 32_000_000,
    );
    check(scenario, row.profile, "network_failures", row.network.failed, 0, (value) => value === 0);
    check(scenario, row.profile, "median_frame_ms", row.frame_time_ms.median, 50);
    check(scenario, row.profile, "p95_frame_ms", row.frame_time_ms.p95, radius === 500 ? 84 : 220);
    check(scenario, row.profile, "long_task_total_ms", row.long_tasks.total_ms, radius === 500 ? 3_000 : 12_000);
    check(scenario, row.profile, "long_task_max_ms", row.long_tasks.max_ms, radius === 500 ? 500 : 1_000);
    check(scenario, row.profile, "heap_bytes", row.heap?.usedJSHeapSize || 0, radius === 500 ? 320_000_000 : 650_000_000);
    check(scenario, row.profile, "search_result_ms", row.search_result_ms, radius === 500 ? 4_000 : 6_000);
    check(scenario, row.profile, "selection_ms", row.selection_ms, radius === 500 ? 1_000 : 2_500);
    check(scenario, row.profile, "eligible_systems", Number(renderer.mapTileEligibleSystems), expectedEligible, (value, limit) => value === limit);
    check(scenario, row.profile, "rendered_points_min", Number(renderer.mapStarCount), minRendered, (value, limit) => value >= limit);
    check(scenario, row.profile, "rendered_points_max", Number(renderer.mapStarCount), maxRendered);
    check(scenario, row.profile, "tile_completion", Number(renderer.mapTilesLoaded), Number(renderer.mapTilesQueued), (value, limit) => value === limit);
    check(scenario, row.profile, "tile_failures", Number(renderer.mapTileFailures), 0, (value) => value === 0);
    check(scenario, row.profile, "progressive_transport", renderer.mapTileProgressive, "true", (value, limit) => value === limit);
    check(scenario, row.profile, "progressive_completion", renderer.mapTileComplete, "true", (value, limit) => value === limit);
    check(scenario, row.profile, "sample_stage_depth", Number(renderer.mapTileCompletedStageDepth), 4, (value, limit) => value === limit);
    check(scenario, row.profile, "global_exact_systems", Number(renderer.mapTileExactSystems), 0, (value) => value === 0);
    check(scenario, row.profile, "camera_detail_systems", Number(renderer.mapDetailSystems), 1, (value, limit) => value >= limit);
    check(scenario, row.profile, "duplicate_tile_requests", tileUrls.length, new Set(tileUrls).size, (value, limit) => value === limit);
    check(
      scenario,
      row.profile,
      "density_profile",
      renderer.mapDensityMode,
      mobile ? "performance" : "balanced",
      (value, limit) => value === limit,
    );
  }
}

const report = {
  schema_version: "spacegate_deep_map_performance_acceptance_v1",
  generated_at: new Date().toISOString(),
  run_ids: {
    deep_500_cold: deep500.run_id,
    deep_1000_cold: deep1000.run_id,
    deep_1000_warm: warm1000.run_id,
    deep_1000_rapid_direction: rapid1000.run_id,
  },
  passed: checks.every((item) => item.passed),
  failed_checks: checks.filter((item) => !item.passed),
  checks,
};
await fs.writeFile(String(args.output), `${JSON.stringify(report, null, 2)}\n`);
process.stdout.write(`${JSON.stringify({ passed: report.passed, checks: checks.length, failed_checks: report.failed_checks }, null, 2)}\n`);
if (!report.passed) process.exitCode = 1;
