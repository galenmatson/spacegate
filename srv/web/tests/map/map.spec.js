import { expect, test } from "@playwright/test";

async function openMap(page) {
  await page.goto("/map", { waitUntil: "networkidle" });
  await page.locator(".map-canvas canvas").waitFor();
  await page.waitForTimeout(1200);
}

async function canvasBox(page) {
  const box = await page.locator(".map-canvas canvas").boundingBox();
  expect(box, "map canvas box").toBeTruthy();
  return box;
}

test.describe("public 3D map beta", () => {
  test("desktop route tools create, undo, and clear ephemeral measurements", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop route workflow uses right-click");
    await openMap(page);
    const box = await canvasBox(page);

    await page.mouse.click(box.x + box.width / 2 + 170, box.y + box.height / 2 + 40, { button: "right" });
    await expect(page.locator(".map-context-menu")).toBeVisible();
    await page.getByRole("button", { name: /measure from selected/i }).click();

    await expect(page.locator(".map-route-summary")).toContainText(/1 legs/i);
    await expect(page.locator(".map-route-summary")).toContainText(/total/i);
    await expect(page.locator(".map-route-leg-list li")).toHaveCount(1);

    await page.getByRole("button", { name: /undo/i }).click();
    await expect(page.locator(".map-route-summary")).toHaveCount(0);

    await page.mouse.click(box.x + box.width / 2 + 120, box.y + box.height / 2 + 80, { button: "right" });
    await expect(page.locator(".map-context-menu")).toBeVisible();
    await page.getByRole("button", { name: /measure from selected/i }).click();
    await expect(page.locator(".map-route-summary")).toBeVisible();
    await page.getByRole("button", { name: /clear/i }).click();
    await expect(page.locator(".map-route-summary")).toHaveCount(0);
  });

  test("selected snapshot chip opens deterministic snapshot preview", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "hover preview is a desktop affordance");
    const mapResponse = await page.request.get("/api/v1/map/systems", {
      params: { radius_ly: "100", limit: "20000", compact: "true" },
    });
    expect(mapResponse.ok()).toBeTruthy();
    const mapPayload = await mapResponse.json();
    const hasSnapshot = (mapPayload.items || []).some((item) => item.has_snapshot);
    test.skip(!hasSnapshot, "served build has no map systems with deterministic snapshots");

    await openMap(page);
    const chip = page.locator(".map-snapshot-chip.ready").first();
    await expect(chip).toBeVisible();
    await chip.hover();
    await expect(page.locator(".map-snapshot-popover img")).toBeVisible();
    const popoverBox = await page.locator(".map-snapshot-popover").boundingBox();
    const viewport = page.viewportSize();
    expect(popoverBox, "snapshot popover bounds").toBeTruthy();
    expect(popoverBox.x).toBeGreaterThanOrEqual(0);
    expect(popoverBox.x + popoverBox.width).toBeLessThanOrEqual(viewport.width);
  });

  test("mobile layout keeps map controls compact", async ({ page }, testInfo) => {
    test.skip(!testInfo.project.name.includes("mobile"), "mobile-only layout check");
    await openMap(page);
    await expect(page.locator(".map-fullscreen-command")).toBeVisible();
    await expect(page.locator(".map-contacts-panel")).toBeHidden();

    const metrics = await page.evaluate(() => {
      const canvas = document.querySelector(".map-canvas canvas")?.getBoundingClientRect();
      const header = document.querySelector(".map-site-header")?.getBoundingClientRect();
      return {
        canvasWidth: canvas?.width || 0,
        canvasHeight: canvas?.height || 0,
        headerHeight: header?.height || 0,
      };
    });
    expect(metrics.canvasWidth).toBeGreaterThan(100);
    expect(metrics.canvasHeight).toBeGreaterThan(100);
    expect(metrics.headerHeight).toBeLessThanOrEqual(88);
  });

  test("system detail renders live simulation preview", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "preview renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "TRAPPIST-1", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "TRAPPIST-1 system_id").toBeTruthy();

    await page.goto(`/systems/${systemId}`, { waitUntil: "networkidle" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(page.locator(".system-preview-readout")).toContainText(/readiness/i);
    await expect(page.locator("[data-testid='system-preview-visual-scale']")).toContainText(/visual scale/i);
    await expect(page.locator("[data-testid='system-preview-visual-scale']")).toContainText(/clarity/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/SOURCE/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/ASSUMED/i);
    await expect(page.getByRole("button", { name: /pause/i })).toBeVisible();
    await page.getByRole("button", { name: /pause/i }).click();
    await expect(page.getByRole("button", { name: /start/i })).toBeVisible();
    await page.getByRole("button", { name: /start/i }).click();
    await expect(page.getByRole("button", { name: /pause/i })).toBeVisible();
    await expect(page.getByLabel(/speed/i)).toBeVisible();
    await page.getByLabel(/speed/i).selectOption("5");
    await page.getByRole("button", { name: /reset/i }).click();

    await page.getByRole("button", { name: /pause/i }).click();
    await expect(page.getByRole("button", { name: /start/i })).toBeVisible();
    const previewCanvasForView = page.locator(".system-preview-canvas canvas");
    await previewCanvasForView.scrollIntoViewIfNeeded();
    const viewBox = await previewCanvasForView.boundingBox();
    expect(viewBox, "system preview canvas box for view controls").toBeTruthy();
    const initialCamera = await previewCanvasForView.evaluate((canvas) => canvas.dataset.cameraPosition || "");
    expect(initialCamera).toBeTruthy();
    await page.mouse.move(viewBox.x + viewBox.width / 2, viewBox.y + viewBox.height / 2);
    await page.mouse.wheel(0, -700);
    await expect
      .poll(() => previewCanvasForView.evaluate((canvas) => canvas.dataset.cameraPosition || ""), { timeout: 3000 })
      .not.toBe(initialCamera);
    const zoomedCamera = await previewCanvasForView.evaluate((canvas) => canvas.dataset.cameraPosition || "");
    await page.getByRole("button", { name: /reset/i }).click();
    await expect
      .poll(() => previewCanvasForView.evaluate((canvas) => canvas.dataset.cameraPosition || ""), { timeout: 3000 })
      .not.toBe(zoomedCamera);
    const resetCamera = await previewCanvasForView.evaluate((canvas) => canvas.dataset.cameraPosition || "");
    await page.mouse.move(viewBox.x + viewBox.width / 2, viewBox.y + viewBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(viewBox.x + viewBox.width / 2 + 120, viewBox.y + viewBox.height / 2 + 55, { steps: 8 });
    await page.mouse.up();
    await expect
      .poll(() => previewCanvasForView.evaluate((canvas) => canvas.dataset.cameraPosition || ""), { timeout: 3000 })
      .not.toBe(resetCamera);
    await page.getByRole("button", { name: /reset/i }).click();
    await page.getByRole("button", { name: /start/i }).click();

    await page.getByRole("button", { name: /orbits on/i }).click();
    await expect(page.getByRole("button", { name: /orbits off/i })).toBeVisible();
    await page.getByRole("button", { name: /orbits off/i }).click();
    await expect(page.getByRole("button", { name: /orbits on/i })).toBeVisible();

    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await previewCanvas.scrollIntoViewIfNeeded();
    const previewBox = await previewCanvas.boundingBox();
    expect(previewBox, "system preview canvas box").toBeTruthy();
    await page.mouse.click(previewBox.x + previewBox.width / 2, previewBox.y + previewBox.height / 2);
    await expect(page.locator("[data-testid='system-preview-pinned']")).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-pinned']")).toContainText(/star|planet|orbit/i);
    await expect(page.locator("[data-testid='system-preview-pinned'] .evidence-pill").first()).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-pinned']")).toContainText(/SOURCE|DERIVED|ASSUMED|MISSING/i);
    await page.locator("[data-testid='system-preview-pinned'] .evidence-pill").first().focus();
    await expect(page.locator("[data-testid='system-preview-pinned'] .evidence-popover").first()).toBeVisible();
  });

  test("system preview falls back to deterministic snapshot when WebGL is unavailable", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "fallback smoke only needs one browser project");
    await page.addInitScript(() => {
      const originalGetContext = HTMLCanvasElement.prototype.getContext;
      HTMLCanvasElement.prototype.getContext = function patchedGetContext(type, ...args) {
        const contextType = String(type || "").toLowerCase();
        if (contextType === "webgl" || contextType === "webgl2" || contextType === "experimental-webgl") {
          return null;
        }
        return originalGetContext.call(this, type, ...args);
      };
    });

    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "TRAPPIST-1", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "TRAPPIST-1 system_id").toBeTruthy();

    await page.goto(`/systems/${systemId}`, { waitUntil: "networkidle" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-snapshot-fallback']")).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-snapshot-fallback'] img")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toHaveCount(0);
    await expect(page.locator("[data-testid='system-preview-snapshot-fallback']")).toContainText(/WebGL unavailable/i);
  });

  test("mobile system detail keeps live preview usable", async ({ page }, testInfo) => {
    test.skip(!testInfo.project.name.includes("mobile"), "mobile-only simulator layout check");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "TRAPPIST-1", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "TRAPPIST-1 system_id").toBeTruthy();

    await page.goto(`/systems/${systemId}`, { waitUntil: "networkidle" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(page.locator(".system-preview-readout")).toContainText(/rendered planets/i);

    const metrics = await page.evaluate(() => {
      const canvas = document.querySelector(".system-preview-canvas")?.getBoundingClientRect();
      const readout = document.querySelector(".system-preview-readout")?.getBoundingClientRect();
      return {
        canvasWidth: canvas?.width || 0,
        canvasHeight: canvas?.height || 0,
        readoutTop: readout?.top || 0,
        canvasBottom: canvas?.bottom || 0,
      };
    });
    expect(metrics.canvasWidth).toBeGreaterThan(100);
    expect(metrics.canvasHeight).toBeGreaterThan(220);
    expect(metrics.readoutTop).toBeGreaterThanOrEqual(metrics.canvasBottom - 1);

    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await previewCanvas.scrollIntoViewIfNeeded();
    const previewBox = await previewCanvas.boundingBox();
    expect(previewBox, "mobile system preview canvas box").toBeTruthy();
    await page.touchscreen.tap(previewBox.x + previewBox.width / 2, previewBox.y + previewBox.height / 2);
    await expect(page.locator("[data-testid='system-preview-pinned']")).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-pinned'] .evidence-pill").first()).toBeVisible();
  });

  test("multi-star system preview exposes binary render orbits and provenance", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "multi-star renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "66alp Gem", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "66alp Gem system_id").toBeTruthy();

    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    expect(scenePayload.render_scene?.schema_version).toBe("render_scene_v0.2");
    expect(scenePayload.render_scene?.bodies?.stars?.length).toBeGreaterThanOrEqual(6);
    expect(scenePayload.render_scene?.orbits?.length).toBeGreaterThanOrEqual(3);

    await page.goto(`/systems/${systemId}`, { waitUntil: "networkidle" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(page.locator(".system-preview-readout")).toContainText(/rendered orbits/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/SOURCE/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/DERIVED|ASSUMED/i);
  });

  test("nested planet-host preview renders hierarchy planets", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "preview renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "Alpha Centauri", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "Alpha Centauri system_id").toBeTruthy();

    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    expect(scenePayload.render_scene?.bodies?.stars?.length).toBeGreaterThanOrEqual(4);
    expect(scenePayload.render_scene?.bodies?.planets?.length).toBeGreaterThanOrEqual(2);
    expect(scenePayload.render_scene?.bodies?.planets?.some((planet) => planet.host_body_key)).toBeTruthy();

    await page.goto(`/systems/${systemId}`, { waitUntil: "networkidle" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(page.locator(".system-preview-readout")).toContainText(/2\s*rendered planets/i);
  });
});
