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
      await expect(canvas).toHaveAttribute("data-map-tile-lod-mode", "camera_blended_interest_spatial_v2");
      await expect(canvas).toHaveAttribute(
        "data-map-density-mode",
        testInfo.project.name.startsWith("mobile") ? "performance" : "balanced",
      );
      expect(await canvas.evaluate((node) => Number(node.dataset.mapDetailSystems || 0))).toBeGreaterThan(0);
      await expect.poll(() => canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0))).toBe(
        await canvas.evaluate((node) => Number(node.dataset.mapTileRenderedSystems || 0)),
      );
      const seamRatio = await canvas.evaluate((node) => Number(node.dataset.mapRadialSeamRatio || 0));
      expect(seamRatio).toBeGreaterThan(0.35);
      expect(seamRatio).toBeLessThan(2);
    }
    await expect(canvas).toHaveAttribute("data-map-tile-failures", "0");
    await expectPaintedMap(page);
    await page.screenshot({ path: testInfo.outputPath(`tiled-${radius}ly.png`), fullPage: true });
  });
}

for (const radius of [500, 1000]) {
  test(`progressive ${radius}-ly map keeps exact leaves camera-local`, async ({ page }, testInfo) => {
    test.setTimeout(radius === 1000 ? 120_000 : 90_000);
    await page.goto(`/map?radius=${radius}&pixel_probe=1`, { waitUntil: "domcontentloaded" });
    const canvas = page.locator(".map-canvas canvas");
    await canvas.waitFor();
    await expect(canvas).toHaveAttribute("data-map-tile-progressive", "true");
    await expect(canvas).toHaveAttribute("data-map-tile-manifest-ready", "true");
    await expect.poll(
      () => canvas.getAttribute("data-map-tile-coarse-complete"),
      { timeout: 30_000 },
    ).toBe("true");
    await expect(canvas).toHaveAttribute("data-map-tile-complete", "true", { timeout: 60_000 });
    await expect(canvas).toHaveAttribute("data-map-tile-exact-systems", "0");
    await expect(canvas).toHaveAttribute("data-map-tile-replaced-samples", radius === 1000 ? "64" : "8");
    await expect(canvas).toHaveAttribute("data-map-tile-completed-stage-depth", "4");
    await expect(canvas).toHaveAttribute("data-map-tile-failures", "0");
    await expect.poll(() => canvas.evaluate((node) => Number(node.dataset.mapDetailSystems || 0))).toBeGreaterThan(0);
    const renderedPoints = await canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0));
    expect(renderedPoints).toBeGreaterThan(radius === 1000 ? 80_000 : 10_000);
    expect(renderedPoints).toBeLessThan(radius === 1000 ? 180_000 : 100_000);
    const detailTiles = await canvas.evaluate((node) => Number(node.dataset.mapDetailTiles || 0));
    expect(detailTiles).toBeGreaterThan(0);
    expect(detailTiles).toBeLessThan(radius === 1000 ? 300 : 200);
    await expectPaintedMap(page);
    await page.screenshot({ path: testInfo.outputPath(`progressive-${radius}ly.png`), fullPage: true });
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

test("250-ly detail bubble follows a sustained camera flight", async ({ page }, testInfo) => {
  test.skip(testInfo.project.name.startsWith("mobile"), "Keyboard flight trace is a desktop contract.");
  await page.goto("/map?radius=250&pixel_probe=1");
  const canvas = page.locator(".map-canvas canvas");
  await expect(canvas).toHaveAttribute("data-map-tile-complete", "true");
  await expect.poll(() => canvas.getAttribute("data-map-detail-center-ly")).not.toBe("");
  const initialCenter = await canvas.getAttribute("data-map-detail-center-ly");
  const initialCount = await canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0));
  await page.keyboard.down("Shift");
  await page.keyboard.down("w");
  await page.waitForTimeout(900);
  await page.keyboard.up("w");
  await page.keyboard.up("Shift");
  await expect.poll(() => canvas.getAttribute("data-map-detail-center-ly"), { timeout: 15_000 }).not.toBe(initialCenter);
  await expect.poll(() => canvas.evaluate((node) => Number(node.dataset.mapDetailSystems || 0))).toBeGreaterThan(0);
  const movedCount = await canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0));
  expect(movedCount).toBeGreaterThan(initialCount * 0.8);
  expect(movedCount).toBeLessThan(initialCount * 1.2);
  await expect(page.locator(".map-header-readout")).toContainText(`${movedCount.toLocaleString()} points`);
  await expect(canvas).toHaveAttribute("data-map-tile-failures", "0");
});

test("250-ly density control can render the exact catalog", async ({ page }, testInfo) => {
  test.skip(testInfo.project.name.startsWith("mobile"), "Exact-density stress trace is desktop-only.");
  test.setTimeout(120_000);
  await page.goto("/map?radius=250&pixel_probe=1", { waitUntil: "domcontentloaded" });
  const canvas = page.locator(".map-canvas canvas");
  await expect(canvas).toHaveAttribute("data-map-tile-complete", "true");
  await page.locator(".map-header-menu > summary").click();
  await page.locator("[data-testid='map-density-mode-select']").selectOption("exact");
  await expect(canvas).toHaveAttribute("data-map-density-mode", "exact");
  await expect.poll(
    () => canvas.evaluate((node) => Number(node.dataset.mapStarCount || 0)),
    { timeout: 60_000 },
  ).toBe(230_181);
  expect(await canvas.evaluate((node) => Number(node.dataset.mapLabelCount || 0))).toBeLessThan(100);
  await expect(canvas).toHaveAttribute("data-map-tile-failures", "0");
  await expectPaintedMap(page);
});

test("Bright star style remains nonblank at 4K", async ({ page }, testInfo) => {
  test.skip(testInfo.project.name.startsWith("mobile"), "4K star-style validation is desktop-only.");
  test.setTimeout(120_000);
  await page.setViewportSize({ width: 3840, height: 2160 });
  await page.goto("/map?radius=100&pixel_probe=1", { waitUntil: "domcontentloaded" });
  const canvas = page.locator(".map-canvas canvas");
  await expect(canvas).toHaveAttribute("data-map-tile-complete", "true");
  await page.locator(".map-header-menu > summary").click();
  await page.locator("[data-testid='map-star-render-mode-select']").selectOption("bright");
  await expect(canvas).toHaveAttribute("data-map-star-render-mode", "bright");
  await expect(canvas).toHaveAttribute("data-map-label-class-strategy", "mass_proxy_then_intrinsic_brightness_v2");
  await expectPaintedMap(page);
  await page.screenshot({ path: testInfo.outputPath("map-bright-4k.png"), fullPage: true });
});
