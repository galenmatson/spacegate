import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, "").split("=");
  return [key, rest.join("=") || true];
}));
const phase = String(args.phase || "baseline");
const baseURL = String(args.baseURL || process.env.SPACEGATE_MAP_BASE_URL || "https://10.0.0.12");
const reportRoot = String(args.reportRoot || process.env.SPACEGATE_MAP_BENCHMARK_ROOT || "/data/spacegate/state/reports/map_benchmarks");
const runId = String(args.runId || new Date().toISOString().replace(/[:.]/g, "").replace("Z", "Z"));
const radiusLy = Number(args.radius || 100);
const searchQuery = String(args.query || (radiusLy >= 250 ? "DMPP-1" : "Tau Ceti"));
const scenario = String(args.scenario || "continuous-flight");
const cacheMode = String(args.cache || "cold");
const profiles = [
  { id: "desktop", viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 },
  { id: "mobile", viewport: { width: 412, height: 915 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true },
  { id: "photon_high", viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1 },
];

function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))];
}

async function benchmarkProfile(browser, profile) {
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    viewport: profile.viewport,
    deviceScaleFactor: profile.deviceScaleFactor,
    isMobile: Boolean(profile.isMobile),
    hasTouch: Boolean(profile.hasTouch),
  });
  const cdp = await context.newCDPSession(await context.newPage());
  const pages = context.pages();
  const page = pages[0];
  const network = { requests: 0, encodedBytes: 0, failed: 0, cacheResponses: 0, urls: [] };
  const loadingRequests = new Map();
  await cdp.send("Network.enable");
  cdp.on("Network.requestWillBeSent", (event) => {
    if (!event.request.url.startsWith(baseURL)) return;
    network.requests += 1;
    loadingRequests.set(event.requestId, event.request.url);
    network.urls.push(event.request.url.replace(baseURL, ""));
  });
  cdp.on("Network.loadingFinished", (event) => {
    if (!loadingRequests.has(event.requestId)) return;
    network.encodedBytes += Number(event.encodedDataLength || 0);
    loadingRequests.delete(event.requestId);
  });
  cdp.on("Network.loadingFailed", (event) => {
    if (!loadingRequests.has(event.requestId)) return;
    network.failed += 1;
    loadingRequests.delete(event.requestId);
  });
  cdp.on("Network.responseReceived", (event) => {
    if (!event.response.url.startsWith(baseURL)) return;
    if (event.response.fromDiskCache || event.response.fromPrefetchCache || event.response.fromServiceWorker) {
      network.cacheResponses += 1;
    }
  });
  await page.addInitScript(() => {
    window.__spacegateBenchmark = { frames: [], longTasks: [] };
    let previous = performance.now();
    const frame = (now) => {
      window.__spacegateBenchmark.frames.push(now - previous);
      previous = now;
      requestAnimationFrame(frame);
    };
    requestAnimationFrame(frame);
    try {
      new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          window.__spacegateBenchmark.longTasks.push({ start: entry.startTime, duration: entry.duration });
        }
      }).observe({ type: "longtask", buffered: true });
    } catch (_) {
      // Long Task API is not available in every browser configuration.
    }
  });

  if (cacheMode === "warm") {
    await page.goto(`${baseURL}/map?radius=${radiusLy}`, { waitUntil: "domcontentloaded" });
    await page.waitForFunction(() => {
      const node = document.querySelector(".map-canvas canvas");
      return node?.dataset.mapTileComplete === "true";
    }, null, { timeout: radiusLy >= 250 ? 60_000 : 30_000 });
    await page.goto("about:blank");
    network.requests = 0;
    network.encodedBytes = 0;
    network.failed = 0;
    network.cacheResponses = 0;
    network.urls = [];
    loadingRequests.clear();
  }

  const startedAt = Date.now();
  await page.goto(`${baseURL}/map?radius=${radiusLy}`, { waitUntil: "domcontentloaded" });
  const canvas = page.locator(".map-canvas canvas");
  await canvas.waitFor({ state: "visible", timeout: 30_000 });
  await page.waitForFunction(() => Number(document.querySelector(".map-canvas canvas")?.dataset.mapStarCount || 0) > 0, null, { timeout: 30_000 });
  const usableMs = Date.now() - startedAt;
  const settleStarted = Date.now();
  await page.waitForFunction(() => {
    const node = document.querySelector(".map-canvas canvas");
    const eligible = Number(node?.dataset.mapTileEligibleSystems || 0);
    const exact = Number(node?.dataset.mapTileExactSystems || 0);
    const rendered = Number(node?.dataset.mapStarCount || 0);
    const progressive = node?.dataset.mapTileProgressive === "true";
    return node?.dataset.mapTransport === "monolithic"
      || (eligible > 0
        && rendered > 0
        && node?.dataset.mapTileComplete === "true"
        && (progressive || exact >= eligible));
  }, null, { timeout: radiusLy >= 250 ? 60_000 : 30_000 });
  const visibleSettleMs = Date.now() - settleStarted + usableMs;
  await page.waitForTimeout(1500);

  const cameraBefore = await canvas.getAttribute("data-map-camera-position");
  if (profile.isMobile) {
    const forward = page.locator("[data-testid='map-mobile-flight-forward']");
    await forward.dispatchEvent("pointerdown", { pointerId: 1, pointerType: "touch" });
    await page.waitForTimeout(1200);
    await forward.dispatchEvent("pointerup", { pointerId: 1, pointerType: "touch" });
  } else {
    await page.keyboard.down("ArrowUp");
    await page.waitForTimeout(1200);
    await page.keyboard.up("ArrowUp");
    if (scenario === "rapid-direction-change") {
      for (const key of ["ArrowLeft", "ArrowDown", "ArrowRight", "ArrowUp"]) {
        await page.keyboard.down(key);
        await page.waitForTimeout(300);
        await page.keyboard.up(key);
      }
    }
  }
  await page.waitForTimeout(600);
  const cameraAfter = await canvas.getAttribute("data-map-camera-position");

  const searchStarted = Date.now();
  const searchToggle = page.locator("[data-testid='map-search-toggle']");
  if (await searchToggle.getAttribute("aria-pressed") !== "true") await searchToggle.click();
  await page.locator("[data-testid='map-star-search-input']").fill(searchQuery);
  await page.locator(".map-search-topbar").getByRole("button", { name: /^Search$/ }).click();
  const firstCard = page.locator(".map-search-card").first();
  await firstCard.waitFor({ state: "visible", timeout: 20_000 });
  const searchResultMs = Date.now() - searchStarted;
  const selectionStarted = Date.now();
  await firstCard.locator(".map-search-card-actions .map-command-button.primary").click();
  await page.locator("[data-testid='map-system-drill']").waitFor({ state: "visible", timeout: 20_000 });
  const selectionMs = Date.now() - selectionStarted;
  await page.waitForTimeout(1200);

  const heapBeforeGc = await page.evaluate(() => performance.memory ? {
    usedJSHeapSize: performance.memory.usedJSHeapSize,
    totalJSHeapSize: performance.memory.totalJSHeapSize,
    jsHeapSizeLimit: performance.memory.jsHeapSizeLimit,
  } : null);
  const forcedGcAvailable = await page.evaluate(() => {
    if (typeof globalThis.gc !== "function") return false;
    globalThis.gc();
    return true;
  });
  if (forcedGcAvailable) await page.waitForTimeout(250);

  const runtime = await page.evaluate(() => {
    const canvasNode = document.querySelector(".map-canvas canvas");
    const paint = performance.getEntriesByType("paint").map((entry) => ({ name: entry.name, startTime: entry.startTime }));
    return {
      navigation: performance.getEntriesByType("navigation")[0]?.toJSON?.() || null,
      paint,
      heap: performance.memory ? {
        usedJSHeapSize: performance.memory.usedJSHeapSize,
        totalJSHeapSize: performance.memory.totalJSHeapSize,
        jsHeapSizeLimit: performance.memory.jsHeapSizeLimit,
      } : null,
      frames: window.__spacegateBenchmark?.frames || [],
      longTasks: window.__spacegateBenchmark?.longTasks || [],
      canvasDataset: { ...(canvasNode?.dataset || {}) },
    };
  });
  const stableFrames = runtime.frames.slice(Math.floor(runtime.frames.length * 0.25)).filter((value) => value > 0 && value < 1000);
  const result = {
    profile: profile.id,
    viewport: profile.viewport,
    usable_ms: usableMs,
    visible_region_settle_ms: visibleSettleMs,
    search_result_ms: searchResultMs,
    selection_ms: selectionMs,
    camera_before: cameraBefore,
    camera_after: cameraAfter,
    network,
    first_contentful_paint_ms: runtime.paint.find((entry) => entry.name === "first-contentful-paint")?.startTime ?? null,
    frame_time_ms: {
      samples: stableFrames.length,
      median: percentile(stableFrames, 0.5),
      p95: percentile(stableFrames, 0.95),
      approximate_fps: stableFrames.length ? 1000 / (stableFrames.reduce((sum, value) => sum + value, 0) / stableFrames.length) : null,
    },
    long_tasks: {
      count: runtime.longTasks.length,
      total_ms: runtime.longTasks.reduce((sum, entry) => sum + entry.duration, 0),
      max_ms: runtime.longTasks.length ? Math.max(...runtime.longTasks.map((entry) => entry.duration)) : 0,
    },
    heap_before_gc: heapBeforeGc,
    forced_gc_available: forcedGcAvailable,
    heap: runtime.heap,
    renderer: runtime.canvasDataset,
    estimated_gpu_point_buffer_bytes: Number(runtime.canvasDataset.mapStarCount || 0) * 36,
  };
  await context.close();
  return result;
}

const browser = await chromium.launch({ headless: true, args: ["--js-flags=--expose-gc"] });
try {
  const results = [];
  for (const profile of profiles) results.push(await benchmarkProfile(browser, profile));
  const payload = {
    schema_version: "spacegate_map_benchmark_v1",
    phase,
    radius_ly: radiusLy,
    scenario,
    search_query: searchQuery,
    cache_mode: cacheMode,
    run_id: runId,
    base_url: baseURL,
    recorded_at: new Date().toISOString(),
    results,
  };
  const outputDir = path.join(reportRoot, runId);
  await fs.mkdir(outputDir, { recursive: true });
  const outputPath = path.join(outputDir, `${phase}.json`);
  await fs.writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`);
  process.stdout.write(`${outputPath}\n`);
} finally {
  await browser.close();
}
