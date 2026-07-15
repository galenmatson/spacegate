import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, "").split("=");
  return [key, rest.join("=") || true];
}));
const baseURL = String(args.baseURL || process.env.SPACEGATE_MAP_BASE_URL || "https://10.0.0.12");
const radius = Number(args.radius || 1000);
const seconds = Number(args.seconds || 120);
const output = String(args.output || `/data/spacegate/state/reports/map_benchmarks/${new Date().toISOString().replace(/[:.]/g, "")}_idle_${radius}.json`);

const browser = await chromium.launch({ headless: true, args: ["--js-flags=--expose-gc"] });
try {
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    viewport: { width: 3840, height: 2160 },
    deviceScaleFactor: 1,
  });
  const page = await context.newPage();
  await page.addInitScript(() => {
    localStorage.setItem("spacegate.map.starRenderMode", "bright");
    localStorage.setItem("spacegate.map.densityMode", "exact");
  });
  let idleTileRequests = 0;
  let countingIdle = false;
  page.on("request", (request) => {
    if (countingIdle && request.url().endsWith(".sgtile.gz")) idleTileRequests += 1;
  });
  await page.goto(`${baseURL}/map?radius=${radius}`, { waitUntil: "domcontentloaded" });
  const canvas = page.locator(".map-canvas canvas");
  await canvas.waitFor({ state: "visible", timeout: 30_000 });
  await page.waitForFunction(() => {
    const node = document.querySelector(".map-canvas canvas");
    return node?.dataset.mapTileComplete === "true" && Number(node.dataset.mapStarCount || 0) > 0;
  }, null, { timeout: 90_000 });
  // Let late detail/label state settle before defining the parked interval.
  await page.waitForTimeout(5_000);

  const snapshot = async () => page.evaluate(() => {
    const forcedGcAvailable = typeof globalThis.gc === "function";
    globalThis.gc?.();
    const node = document.querySelector(".map-canvas canvas");
    return {
      captured_at: new Date().toISOString(),
      forced_gc_available: forcedGcAvailable,
      heap: performance.memory ? {
        used: performance.memory.usedJSHeapSize,
        total: performance.memory.totalJSHeapSize,
        limit: performance.memory.jsHeapSizeLimit,
      } : null,
      dataset: { ...(node?.dataset || {}) },
    };
  });
  const before = await snapshot();
  countingIdle = true;
  await page.waitForTimeout(seconds * 1_000);
  countingIdle = false;
  const after = await snapshot();

  const checks = [
    { metric: "forced_gc_available", actual: after.forced_gc_available, budget: true, passed: after.forced_gc_available === true },
    { metric: "idle_tile_requests", actual: idleTileRequests, budget: 0, passed: idleTileRequests === 0 },
    {
      metric: "forced_gc_heap_growth_bytes",
      actual: Number(after.heap?.used || 0) - Number(before.heap?.used || 0),
      budget: 16_000_000,
      passed: Number(after.heap?.used || 0) - Number(before.heap?.used || 0) <= 16_000_000,
    },
    ...["mapTelemetryEmits", "mapLabelRebuilds", "runtimeWebglTextures", "runtimeWebglGeometries", "runtimeWebglPrograms"].map((metric) => ({
      metric,
      actual: Number(after.dataset[metric] || 0),
      budget: Number(before.dataset[metric] || 0),
      passed: Number(after.dataset[metric] || 0) === Number(before.dataset[metric] || 0),
    })),
    {
      metric: "context_recoveries",
      actual: Number(after.dataset.runtimeContextRecoveries || 0),
      budget: 0,
      passed: Number(after.dataset.runtimeContextRecoveries || 0) === 0,
    },
    {
      metric: "tile_failures",
      actual: Number(after.dataset.mapTileFailures || 0),
      budget: 0,
      passed: Number(after.dataset.mapTileFailures || 0) === 0,
    },
    {
      metric: "rendered_systems",
      actual: Number(after.dataset.mapStarCount || 0),
      budget: 1,
      passed: Number(after.dataset.mapStarCount || 0) >= 1,
    },
  ];
  const report = {
    schema_version: "spacegate_map_idle_soak_v1",
    base_url: baseURL,
    radius_ly: radius,
    viewport: { width: 3840, height: 2160 },
    duration_seconds: seconds,
    generated_at: new Date().toISOString(),
    passed: checks.every((item) => item.passed),
    checks,
    before,
    after,
  };
  await fs.mkdir(path.dirname(output), { recursive: true });
  await fs.writeFile(output, `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify({ output, passed: report.passed, checks }, null, 2)}\n`);
  if (!report.passed) process.exitCode = 1;
  await context.close();
} finally {
  await browser.close();
}
