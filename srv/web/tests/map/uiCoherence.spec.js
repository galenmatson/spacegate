import { expect, test } from "@playwright/test";


const THEMES = [
  "simple_light",
  "simple_dark",
  "cyberpunk",
  "lcars",
  "mission_control",
  "aurora",
  "retro_90s",
  "deep_space_minimal",
];

const VIEWPORTS = [
  { id: "desktop", width: 1600, height: 1000 },
  { id: "4k", width: 3840, height: 2160 },
  { id: "ultrawide", width: 3440, height: 1440 },
];

test("System Page envelope remains bounded across themes and wide displays", async ({ page }, testInfo) => {
  test.skip(testInfo.project.name.includes("mobile"), "wide-screen matrix");
  const measurements = [];
  for (const viewport of VIEWPORTS) {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    for (const theme of THEMES) {
      await page.addInitScript((selectedTheme) => {
        window.localStorage.setItem("spacegate.theme", selectedTheme);
      }, theme);
      await page.goto("/systems/17784468", { waitUntil: "domcontentloaded" });
      await expect(page.locator(".system-detail-v2")).toBeVisible();
      const measurement = await page.evaluate(() => {
        const shell = document.querySelector(".app.system-route");
        const prose = document.querySelector(".system-story-card p");
        const rect = shell?.getBoundingClientRect();
        return {
          theme: document.documentElement.dataset.theme,
          shell_width: rect?.width || 0,
          shell_right: rect?.right || 0,
          viewport_width: window.innerWidth,
          document_scroll_width: document.documentElement.scrollWidth,
          prose_width: prose?.getBoundingClientRect().width || 0,
        };
      });
      measurements.push({ viewport: viewport.id, ...measurement });
      expect(measurement.theme).toBe(theme);
      expect(measurement.shell_width).toBeGreaterThan(300);
      expect(measurement.shell_width).toBeLessThanOrEqual(1600.5);
      expect(measurement.shell_right).toBeLessThanOrEqual(viewport.width + 1);
      expect(measurement.document_scroll_width).toBeLessThanOrEqual(viewport.width + 1);
      expect(measurement.prose_width).toBeLessThan(850);
    }
    await page.screenshot({
      path: testInfo.outputPath(`system-page-${viewport.id}.png`),
      fullPage: false,
    });
  }
  await testInfo.attach("system-page-wide-layout-matrix.json", {
    body: Buffer.from(JSON.stringify(measurements, null, 2)),
    contentType: "application/json",
  });
});
