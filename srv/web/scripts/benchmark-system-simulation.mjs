#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "@playwright/test";

function option(name, fallback = "") {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))];
}

const baseUrl = option("base-url", "https://10.0.0.12").replace(/\/$/, "");
const output = option("output");
const label = option("label", "simulation-benchmark");
const mode = option("mode", "structure");
const systemId = Number(option("system-id", "17785920"));
const withLens = process.argv.includes("--lens");
if (!output || !Number.isInteger(systemId)) throw new Error("--output and a numeric --system-id are required");

const profiles = [
  { id: "desktop-1440", width: 1440, height: 900, deviceScaleFactor: 1, isMobile: false, hasTouch: false },
  { id: "desktop-4k", width: 3840, height: 2160, deviceScaleFactor: 1, isMobile: false, hasTouch: false },
  { id: "mobile-412", width: 412, height: 915, deviceScaleFactor: 2.625, isMobile: true, hasTouch: true },
];

const browser = await chromium.launch({ headless: true });
const results = [];
for (const profile of profiles) {
  const context = await browser.newContext({
    viewport: { width: profile.width, height: profile.height },
    deviceScaleFactor: profile.deviceScaleFactor,
    isMobile: profile.isMobile,
    hasTouch: profile.hasTouch,
    ignoreHTTPSErrors: true,
  });
  const page = await context.newPage();
  const client = await context.newCDPSession(page);
  await client.send("Performance.enable");
  const consoleErrors = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));
  const started = performance.now();
  await page.goto(`${baseUrl}/systems/${systemId}`, { waitUntil: "domcontentloaded" });
  const canvas = page.locator(".system-preview-canvas canvas");
  await canvas.waitFor({ state: "visible", timeout: 30_000 });
  const readyMs = performance.now() - started;
  const scaleSelect = page.locator("[data-testid='system-preview-scale-mode']");
  const availableModes = await scaleSelect.locator("option").evaluateAll((nodes) => nodes.map((node) => node.value));
  const selectedMode = availableModes.includes(mode) ? mode : "structure";
  await scaleSelect.selectOption(selectedMode);
  await page.waitForTimeout(1_000);
  const frameIntervals = await page.evaluate(async () => new Promise((resolve) => {
    const values = [];
    let previous = performance.now();
    function sample(now) {
      values.push(now - previous);
      previous = now;
      if (values.length >= 180) resolve(values.slice(5));
      else requestAnimationFrame(sample);
    }
    requestAnimationFrame(sample);
  }));
  let selectionLatencyMs = null;
  const firstObject = page.locator("[data-testid='system-preview-object-list'] .system-preview-object-select").first();
  if (await firstObject.count()) {
    const selectionStarted = performance.now();
    if (profile.hasTouch) await firstObject.tap();
    else await firstObject.click();
    await page.locator("[data-testid='system-preview-pinned']").waitFor({ state: "visible" });
    selectionLatencyMs = performance.now() - selectionStarted;
  }
  let lensMetrics = null;
  const lensControl = page.locator("[data-lens-control='true']");
  if (withLens && await lensControl.count()) {
    await lensControl.click();
    await page.locator("[data-testid='system-preview-scale-lens']").waitFor({ state: "visible" });
    const lensFrames = await page.evaluate(async () => new Promise((resolve) => {
      const values = [];
      let previous = performance.now();
      function sample(now) {
        values.push(now - previous);
        previous = now;
        if (values.length >= 120) resolve(values.slice(5));
        else requestAnimationFrame(sample);
      }
      requestAnimationFrame(sample);
    }));
    lensMetrics = {
      canvas_count: await page.locator(".system-preview-canvas canvas").count(),
      frame_ms_median: percentile(lensFrames, 0.5),
      frame_ms_p95: percentile(lensFrames, 0.95),
    };
  }
  const canvasMetrics = await canvas.evaluate((node) => ({
    css_width: node.getBoundingClientRect().width,
    css_height: node.getBoundingClientRect().height,
    buffer_width: node.width,
    buffer_height: node.height,
    png_data_url_bytes: node.toDataURL("image/png").length,
    scale_mode: node.dataset.scaleMode || "",
    scene_labels: Number(node.dataset.sceneLabelCount || 0),
    camera_position: node.dataset.cameraPosition || "",
    camera_target: node.dataset.cameraTargetPosition || "",
    webgl_context_count: Number(node.dataset.webglContextCount || 0),
    lens_uses_shared_context: node.dataset.lensUsesSharedContext || "false",
    renderer_geometries: Number(node.dataset.rendererGeometries || 0),
    renderer_textures: Number(node.dataset.rendererTextures || 0),
    renderer_programs: Number(node.dataset.rendererPrograms || 0),
    renderer_draw_calls: Number(node.dataset.rendererDrawCalls || 0),
    renderer_triangles: Number(node.dataset.rendererTriangles || 0),
    physical_focus_nodes: Number(node.dataset.physicalFocusNodeCount || 0),
    scene_units_per_au: Number(node.dataset.sceneUnitsPerAu || 0),
    compressed_physical_root: node.dataset.compressedPhysicalRoot || "false",
  }));
  const memory = await page.evaluate(() => ({
    used_js_heap_bytes: performance.memory?.usedJSHeapSize ?? null,
    total_js_heap_bytes: performance.memory?.totalJSHeapSize ?? null,
  }));
  const performanceMetrics = Object.fromEntries(
    (await client.send("Performance.getMetrics")).metrics.map((item) => [item.name, item.value]),
  );
  const screenshotPath = path.join(path.dirname(output), "screenshots", `${label}-${profile.id}.png`);
  await fs.mkdir(path.dirname(screenshotPath), { recursive: true });
  await page.locator("[data-testid='system-preview-panel']").screenshot({ path: screenshotPath });
  results.push({
    profile: profile.id,
    requested_mode: mode,
    selected_mode: selectedMode,
    mode_supported: availableModes.includes(mode),
    ready_ms: readyMs,
    frame_ms_median: percentile(frameIntervals, 0.5),
    frame_ms_p95: percentile(frameIntervals, 0.95),
    frame_ms_max: Math.max(...frameIntervals),
    selection_latency_ms: selectionLatencyMs,
    lens: lensMetrics,
    canvas: canvasMetrics,
    memory,
    browser_metrics: {
      task_duration_seconds: performanceMetrics.TaskDuration ?? null,
      script_duration_seconds: performanceMetrics.ScriptDuration ?? null,
      layout_duration_seconds: performanceMetrics.LayoutDuration ?? null,
      nodes: performanceMetrics.Nodes ?? null,
      js_event_listeners: performanceMetrics.JSEventListeners ?? null,
    },
    console_errors: consoleErrors,
    screenshot: screenshotPath,
  });
  await context.close();
}
await browser.close();

const report = {
  schema_version: "spacegate.system_simulation_browser_benchmark.v1",
  generated_at_utc: new Date().toISOString(),
  label,
  base_url: baseUrl,
  system_id: systemId,
  requested_mode: mode,
  lens_requested: withLens,
  status: results.every((row) => row.console_errors.length === 0 && row.canvas.png_data_url_bytes > 2_000) ? "pass" : "fail",
  results,
};
await fs.mkdir(path.dirname(output), { recursive: true });
await fs.writeFile(output, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ status: report.status, output, profiles: results.length }, null, 2));
process.exitCode = report.status === "pass" ? 0 : 1;
