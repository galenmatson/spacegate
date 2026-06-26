import { defineConfig, devices } from "@playwright/test";
import path from "node:path";

const runId =
  process.env.SPACEGATE_ADMIN_VISUAL_RUN_ID ||
  new Date().toISOString().replace(/[:.]/g, "").replace("Z", "Z");
const reportRoot =
  process.env.SPACEGATE_ADMIN_VISUAL_REPORT_ROOT ||
  "/data/spacegate/state/reports/admin_visual";
const outputRoot = path.join(reportRoot, runId);
const storageState = process.env.SPACEGATE_ADMIN_STORAGE_STATE || undefined;

export default defineConfig({
  testDir: "./tests/admin-visual",
  timeout: 90_000,
  expect: {
    timeout: 10_000,
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
    baseURL:
      process.env.SPACEGATE_ADMIN_VISUAL_BASE_URL ||
      "https://10.0.0.12/admin/",
    ignoreHTTPSErrors: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "off",
    storageState,
  },
  projects: [
    {
      name: "desktop-1424",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 1424, height: 1000 },
      },
    },
    {
      name: "mobile-390",
      use: {
        ...devices["Pixel 5"],
        viewport: { width: 390, height: 844 },
      },
    },
  ],
  metadata: {
    runId,
    reportRoot,
    outputRoot,
    storageState: storageState ? "configured" : "not configured",
  },
});
