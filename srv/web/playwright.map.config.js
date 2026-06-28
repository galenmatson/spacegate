import { defineConfig, devices } from "@playwright/test";
import path from "node:path";

const runId =
  process.env.SPACEGATE_MAP_TEST_RUN_ID ||
  new Date().toISOString().replace(/[:.]/g, "").replace("Z", "Z");
const reportRoot =
  process.env.SPACEGATE_MAP_TEST_REPORT_ROOT ||
  "/data/spacegate/state/reports/map_playwright";
const outputRoot = path.join(reportRoot, runId);

export default defineConfig({
  testDir: "./tests/map",
  timeout: 90_000,
  expect: {
    timeout: 12_000,
  },
  fullyParallel: false,
  workers: 1,
  outputDir: path.join(outputRoot, "artifacts"),
  reporter: [
    ["list"],
    ["json", { outputFile: path.join(outputRoot, "playwright-report.json") }],
    ["html", { outputFolder: path.join(outputRoot, "html"), open: "never" }],
  ],
  use: {
    baseURL: process.env.SPACEGATE_MAP_BASE_URL || "https://10.0.0.12",
    ignoreHTTPSErrors: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "off",
  },
  projects: [
    {
      name: "desktop-1440",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 1440, height: 900 },
      },
    },
    {
      name: "mobile-412",
      use: {
        ...devices["Pixel 7"],
        viewport: { width: 412, height: 915 },
      },
    },
  ],
  metadata: {
    runId,
    reportRoot,
    outputRoot,
  },
});
