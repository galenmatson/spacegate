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
    await openMap(page);
    const chip = page.locator(".map-snapshot-chip.ready").first();
    await expect(chip).toBeVisible();
    await chip.hover();
    await expect(page.locator(".map-snapshot-popover img")).toBeVisible();
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
  });
});
