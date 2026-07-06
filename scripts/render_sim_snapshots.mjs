#!/usr/bin/env node
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(new URL("../srv/web/package.json", import.meta.url));
const { chromium } = require("@playwright/test");

function usage() {
  console.error("Usage: node scripts/render_sim_snapshots.mjs <jobs.json>");
}

function joinUrl(baseUrl, route) {
  return `${String(baseUrl || "").replace(/\/+$/, "")}/${String(route || "").replace(/^\/+/, "")}`;
}

async function waitForPaintedCanvas(canvasLocator, timeoutMs = 15000) {
  await canvasLocator.waitFor({ timeout: timeoutMs });
  await canvasLocator.evaluate((canvas) => new Promise((resolve) => {
    const finish = () => resolve(true);
    requestAnimationFrame(() => requestAnimationFrame(finish));
  }));
  await canvasLocator.evaluate((canvas) => {
    const gl = canvas.getContext("webgl2") || canvas.getContext("webgl") || canvas.getContext("experimental-webgl");
    if (!gl) {
      return;
    }
    const width = gl.drawingBufferWidth || canvas.width || 0;
    const height = gl.drawingBufferHeight || canvas.height || 0;
    if (!width || !height) {
      return;
    }
    const sampleWidth = Math.min(width, 96);
    const sampleHeight = Math.min(height, 96);
    const pixels = new Uint8Array(sampleWidth * sampleHeight * 4);
    gl.readPixels(0, 0, sampleWidth, sampleHeight, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
  });
}

async function main() {
  const jobsPath = process.argv[2];
  if (!jobsPath) {
    usage();
    process.exit(2);
  }
  const payload = JSON.parse(await fs.readFile(jobsPath, "utf8"));
  const jobs = Array.isArray(payload.jobs) ? payload.jobs : [];
  const baseUrl = payload.base_url || process.env.SPACEGATE_SNAPSHOT_BASE_URL || process.env.SPACEGATE_MAP_BASE_URL || "https://10.0.0.12";
  const viewport = {
    width: Number(payload.width_px || 980),
    height: Number(payload.height_px || 552),
  };
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport,
    ignoreHTTPSErrors: true,
    deviceScaleFactor: 1,
  });
  await page.addInitScript((theme) => {
    try {
      window.localStorage.setItem("spacegate.theme", theme);
    } catch {
      // Ignore storage failures in unusual browser contexts.
    }
  }, String(payload.theme || "simple_dark"));
  const results = [];
  try {
    for (const job of jobs) {
      const started = Date.now();
      const outPath = String(job.output_path || "");
      try {
        if (!outPath) {
          throw new Error("missing output_path");
        }
        await fs.mkdir(path.dirname(outPath), { recursive: true });
        const params = new URLSearchParams();
        if (job.system_name) {
          params.set("name", String(job.system_name));
        }
        params.set("snapshot", "1");
        params.set("theme", String(payload.theme || "simple_dark"));
        const route = `/internal/sim-snapshot/${encodeURIComponent(String(job.system_id))}?${params.toString()}`;
        await page.goto(joinUrl(baseUrl, route), { waitUntil: "domcontentloaded", timeout: 30000 });
        const panel = page.locator("[data-testid='system-preview-panel']");
        await panel.waitFor({ timeout: 15000 });
        const canvas = page.locator(".system-preview-canvas canvas").first();
        await waitForPaintedCanvas(canvas);
        await page.screenshot({
          path: outPath,
          type: "png",
          animations: "disabled",
        });
        results.push({
          system_id: job.system_id,
          output_path: outPath,
          ok: true,
          elapsed_ms: Date.now() - started,
        });
      } catch (error) {
        results.push({
          system_id: job.system_id,
          output_path: outPath,
          ok: false,
          error: error instanceof Error ? error.message : String(error),
          elapsed_ms: Date.now() - started,
        });
      }
    }
  } finally {
    await browser.close();
  }
  process.stdout.write(`${JSON.stringify({ ok: results.every((item) => item.ok), results }, null, 2)}\n`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exit(1);
});
