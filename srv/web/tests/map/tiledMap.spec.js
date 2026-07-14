import { expect, test } from "@playwright/test";

async function expectPaintedMap(page) {
  const canvas = page.locator(".map-canvas canvas");
  await expect(canvas).toBeVisible();
  await expect.poll(() => canvas.evaluate((node) => {
    const gl = node.getContext("webgl2") || node.getContext("webgl");
    if (!gl) return 0;
    const width = gl.drawingBufferWidth;
    const height = gl.drawingBufferHeight;
    if (!width || !height) return 0;
    const pixels = new Uint8Array(width * height * 4);
    gl.readPixels(0, 0, width, height, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
    let visible = 0;
    for (let offset = 0; offset < pixels.length; offset += 16) {
      if (pixels[offset] + pixels[offset + 1] + pixels[offset + 2] > 32) visible += 1;
    }
    return visible;
  }), { timeout: 15_000 }).toBeGreaterThan(30);
}

for (const radius of [100, 250]) {
  test(`tiled ${radius}-ly map reaches exact nonblank coverage`, async ({ page }, testInfo) => {
    test.setTimeout(radius === 250 ? 120_000 : 90_000);
    const expected = radius === 100 ? 10_239 : 230_181;
    await page.goto(`/map?radius=${radius}&pixel_probe=1`, { waitUntil: "domcontentloaded" });
    const canvas = page.locator(".map-canvas canvas");
    await canvas.waitFor();
    await expect.poll(() => canvas.evaluate((node) => node.dataset.mapTransport)).toBe("tiled");
    await expect.poll(
      () => canvas.evaluate((node) => Number(node.dataset.mapTileExactSystems || 0)),
      { timeout: radius === 250 ? 60_000 : 30_000 },
    ).toBe(expected);
    await expect.poll(
      () => canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0)),
      { timeout: radius === 250 ? 60_000 : 30_000 },
    ).toBeGreaterThan(radius === 250 ? 20_000 : expected - 1);
    if (radius === 250) {
      expect(await canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0))).toBeLessThan(80_000);
      await expect(canvas).toHaveAttribute("data-map-tile-lod-mode", "mixed_exact_interest_spatial_v1");
    }
    await expect(canvas).toHaveAttribute("data-map-tile-failures", "0");
    await expectPaintedMap(page);
    await page.screenshot({ path: testInfo.outputPath(`tiled-${radius}ly.png`), fullPage: true });
  });
}

test("250-ly search focus survives exact refinement and system handoff", async ({ page }, testInfo) => {
  test.skip(testInfo.project.name.includes("mobile"), "desktop interaction trace");
  test.setTimeout(120_000);
  await page.goto("/map?radius=250", { waitUntil: "domcontentloaded" });
  const canvas = page.locator(".map-canvas canvas");
  await canvas.waitFor();
  const searchToggle = page.locator("[data-testid='map-search-toggle']");
  if (await searchToggle.getAttribute("aria-pressed") !== "true") await searchToggle.click();
  await page.locator("[data-testid='map-star-search-input']").fill("Tau Ceti");
  await page.locator(".map-search-topbar").getByRole("button", { name: /^Search$/ }).click();
  const result = page.locator(".map-search-card").first();
  await expect(result).toContainText(/Tau Ceti/i);
  await result.locator(".map-search-card-actions .map-command-button.primary").click();
  await expect(page.locator("[data-testid='map-system-drill']")).toBeVisible();
  await expect(page.locator("[data-testid='map-system-drill']")).toContainText(/Tau Ceti/i);
  await expect.poll(
    () => canvas.evaluate((node) => Number(node.dataset.mapTileExactSystems || 0)),
    { timeout: 60_000 },
  ).toBe(230_181);
  await expect(page.locator("[data-testid='map-system-drill']")).toContainText(/Tau Ceti/i);
  await page.screenshot({ path: testInfo.outputPath("tiled-250ly-tau-ceti.png"), fullPage: true });
});
