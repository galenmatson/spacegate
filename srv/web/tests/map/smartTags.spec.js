import { expect, test } from "@playwright/test";


async function resolveSystem(page, query) {
  const response = await page.request.get("/api/v1/systems/search", {
    params: { q: query, limit: "1", sort: "match" },
  });
  expect(response.ok(), `${query} search response`).toBeTruthy();
  const payload = await response.json();
  return payload.items?.[0] || null;
}


async function stellarBadgeMetrics(locator) {
  await expect(locator).toBeVisible();
  return locator.evaluate((node) => {
    const style = window.getComputedStyle(node);
    const core = window.getComputedStyle(node, "::before");
    const bounds = node.getBoundingClientRect();
    return {
      color: style.color,
      fontSize: style.fontSize,
      fontWeight: style.fontWeight,
      width: Math.round(bounds.width),
      height: Math.round(bounds.height),
      coreBackground: core.backgroundImage,
    };
  });
}


test.describe("Smart Tags and concepts", () => {
  test("registry, filters, source context, and bounded evidence agree", async ({ page }) => {
    const registryResponse = await page.request.get("/api/v1/tags");
    expect(registryResponse.ok()).toBeTruthy();
    const registry = await registryResponse.json();
    expect(registry.definitions.length).toBeGreaterThanOrEqual(30);

    const filteredResponse = await page.request.get("/api/v1/systems/search", {
      params: {
        tags_all: "science:system.multiple",
        tags_exclude: "science:system.one_known_star",
        limit: "5",
        sort: "name",
      },
    });
    expect(filteredResponse.ok()).toBeTruthy();
    const filtered = await filteredResponse.json();
    expect(filtered.items.length).toBeGreaterThan(0);
    for (const item of filtered.items) {
      expect(item.star_count).toBeGreaterThanOrEqual(3);
      expect(item.smart_tags.map((tag) => tag.key)).toContain("science:system.multiple");
    }

    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    const tagResponse = await page.request.get(`/api/v1/systems/${castor.system_id}/tags`);
    expect(tagResponse.ok()).toBeTruthy();
    const tagPayload = await tagResponse.json();
    expect(tagPayload.smart_tags.length).toBeGreaterThan(0);
    expect(tagPayload.source_summary.length).toBeGreaterThan(0);
    const sourceKey = tagPayload.source_summary[0].key;
    const sourceResponse = await page.request.get(
      `/api/v1/tag-sources/${encodeURIComponent(sourceKey)}`,
    );
    expect(sourceResponse.ok()).toBeTruthy();
    const sourcePayload = await sourceResponse.json();
    expect(sourcePayload.source.citation_url).toMatch(/^https?:\/\//);

    const assignmentResponse = await page.request.get(
      `/api/v1/systems/${castor.system_id}/tag-assignments`,
      { params: { limit: "3", offset: "0" } },
    );
    expect(assignmentResponse.ok()).toBeTruthy();
    const assignments = await assignmentResponse.json();
    expect(assignments.assignments.length).toBeLessThanOrEqual(3);
    expect(assignments.total).toBeGreaterThan(0);
    expect(assignments.registry_hash).toBe(registry.registry_hash);
  });

  test("shared tag shell supports keyboard, pinning, copy, and concept navigation", async ({ page }, testInfo) => {
    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    await page.goto(`/systems/${castor.system_id}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-object-list']")).toBeVisible();
    await expect(
      page.locator("button button, button a, a button, [role='button'] button, [role='button'] a"),
    ).toHaveCount(0);

    const trigger = page.locator(".system-detail-tags .smart-tag-trigger").first();
    await expect(trigger).toBeVisible();
    await trigger.focus();
    const popover = page.locator(`[id="${await trigger.getAttribute("aria-controls")}"]`);
    await expect(popover).toBeVisible();
    await expect(popover).toContainText(/Basis|Evidence state|Scope/);
    await page.keyboard.press("Escape");
    await expect(popover).not.toBeVisible();

    await trigger.click();
    await expect(popover).toBeVisible();
    await expect(popover.getByRole("button", { name: /Copy/ }).first()).toBeVisible();
    const learn = popover.getByRole("link", { name: "Learn" });
    if (await learn.count()) {
      const href = await learn.getAttribute("href");
      expect(href).toMatch(/^\/concepts\//);
    }
    await page.keyboard.press("Tab");
    await expect(popover.locator("a, button").first()).toBeFocused();
    await page.keyboard.press("Shift+Tab");
    await expect(trigger).toBeFocused();
    await page.mouse.click(4, 4);
    await expect(popover).not.toBeVisible();

    await page.screenshot({
      path: testInfo.outputPath(`smart-tags-${testInfo.project.name}.png`),
      fullPage: true,
    });
    await expect.poll(
      () => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 4),
    ).toBeTruthy();
  });

  test("System Page hero tag popovers clear the simulation stacking context", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name !== "desktop-1440", "Desktop hero and simulation stacking check");
    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    await page.goto(`/systems/${castor.system_id}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator(".system-preview-panel")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();

    const trigger = page.locator(".system-detail-tags .smart-tag-trigger").first();
    await trigger.hover();
    const popover = page.getByRole("dialog", { name: /details$/ }).first();
    await expect(popover).toBeVisible();
    await expect(popover.locator("..")).toHaveClass(/smart-tag-portal/);

    const overlap = await page.evaluate(() => {
      const dialog = document.querySelector(".smart-tag-portal .smart-tag-popover");
      const simulation = document.querySelector(".system-preview-panel");
      if (!dialog || !simulation) {
        return null;
      }
      const dialogBounds = dialog.getBoundingClientRect();
      const simulationBounds = simulation.getBoundingClientRect();
      const left = Math.max(dialogBounds.left, simulationBounds.left);
      const right = Math.min(dialogBounds.right, simulationBounds.right);
      const top = Math.max(dialogBounds.top, simulationBounds.top);
      const bottom = Math.min(dialogBounds.bottom, simulationBounds.bottom);
      if (right <= left || bottom <= top) {
        return { width: 0, height: 0, popoverIsTopmost: false };
      }
      const target = document.elementFromPoint((left + right) / 2, (top + bottom) / 2);
      return {
        width: right - left,
        height: bottom - top,
        popoverIsTopmost: Boolean(target?.closest(".smart-tag-popover")),
      };
    });
    expect(overlap).toBeTruthy();
    expect(overlap.width).toBeGreaterThan(8);
    expect(overlap.height).toBeGreaterThan(8);
    expect(overlap.popoverIsTopmost).toBeTruthy();
    await page.screenshot({
      path: testInfo.outputPath("system-hero-tag-overlay.png"),
      fullPage: false,
    });
  });

  test("expanded lessons remain reachable on short viewports", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name !== "desktop-1440", "Short desktop viewport check");
    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    await page.setViewportSize({ width: 1280, height: 600 });
    await page.goto(`/systems/${castor.system_id}`, { waitUntil: "domcontentloaded" });

    const trigger = page.locator(".system-detail-tags .smart-tag-trigger", {
      hasText: /^Hierarchical$/,
    });
    await expect(trigger).toBeVisible();
    await trigger.click();
    const popover = page.getByRole("dialog", { name: "Hierarchical Multiple System details" });
    await expect(popover).toBeVisible();
    await expect(popover).toContainText("separation of scales");

    const metrics = await popover.evaluate((node) => {
      const bounds = node.getBoundingClientRect();
      const style = window.getComputedStyle(node);
      return {
        top: bounds.top,
        bottom: bounds.bottom,
        viewportHeight: window.innerHeight,
        overflowY: style.overflowY,
        scrollHeight: node.scrollHeight,
        clientHeight: node.clientHeight,
      };
    });
    expect(metrics.top).toBeGreaterThanOrEqual(0);
    expect(metrics.bottom).toBeLessThanOrEqual(metrics.viewportHeight);
    expect(metrics.overflowY).toBe("auto");
    expect(metrics.scrollHeight).toBeGreaterThanOrEqual(metrics.clientHeight);
  });

  test("source tags use reviewed abbreviations without losing full catalog names", async ({ page }) => {
    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    await page.goto(`/systems/${castor.system_id}`, { waitUntil: "domcontentloaded" });

    const sourceTags = page.locator(".system-detail-source-tags .smart-tag-trigger");
    await expect(sourceTags.filter({ hasText: /^MSC$/ })).toHaveCount(1);
    await expect(sourceTags.filter({ hasText: /^SB9$/ })).toHaveCount(1);
    await expect(sourceTags.filter({ hasText: "Multiple Star Catalog" })).toHaveCount(0);
    await expect(sourceTags.filter({ hasText: "Spectroscopic Binary Orbits" })).toHaveCount(0);

    const msc = sourceTags.filter({ hasText: /^MSC$/ });
    await expect(msc).toHaveAttribute("aria-label", "Multiple Star Catalog");
    await msc.hover();
    await expect(page.getByRole("dialog", { name: "Multiple Star Catalog details" })).toBeVisible();
  });

  test("object badges absorb stellar taxonomy details without duplicate hero tags", async ({ page }) => {
    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    await page.goto(`/systems/${castor.system_id}`, { waitUntil: "domcontentloaded" });

    await expect(page.locator(".system-detail-tags [data-tag-category='stellar_class']")).toHaveCount(0);
    await expect(page.locator(".system-detail-tags [data-tag-category='compact_object']")).toHaveCount(0);

    const aStar = page.locator(
      ".system-detail-stellar-tags .smart-tag-trigger[data-stellar-token='a']",
    ).first();
    await expect(aStar).toBeVisible();
    await aStar.hover();
    const detail = page.getByRole("dialog", { name: "A-Type Star details" }).first();
    await expect(detail).toBeVisible();
    await expect(detail).toContainText("strong hydrogen absorption lines");
    await expect(detail).toContainText(/Object|Stellar member/);
    await expect(detail).toContainText("Sources in this system");
    await expect(detail).toContainText("Basis");
  });

  test("stellar class badges reuse the Search icon contract", async ({ page }) => {
    const tauCeti = await resolveSystem(page, "Tau Ceti");
    expect(tauCeti).toBeTruthy();

    await page.goto("/search", { waitUntil: "domcontentloaded" });
    const searchBadge = page.locator(".map-search-spectral.spectral-g").first();
    const searchMetrics = await stellarBadgeMetrics(searchBadge);
    expect(searchMetrics).toMatchObject({
      color: "rgb(7, 17, 31)",
      fontWeight: "800",
      width: 24,
      height: 24,
    });

    await page.goto(`/systems/${tauCeti.system_id}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-object-list']")).toBeVisible();
    await page.waitForTimeout(1200);
    await expect(page.locator("[data-testid='system-preview-object-list']")).toBeVisible();
    const surfaces = [
      page.locator(".system-detail-stellar-tags .stellar-class-chip[data-stellar-token='g']").first(),
      page.locator("[data-testid='system-preview-object-list'] .stellar-class-chip[data-stellar-token='g']").first(),
      page.locator(".hierarchy-panel .stellar-class-chip[data-stellar-token='g']").first(),
    ];
    for (const badge of surfaces) {
      const metrics = await stellarBadgeMetrics(badge);
      expect(metrics).toEqual(searchMetrics);
    }
  });

  test("all reviewed concept routes render representative and related navigation", async ({ page }) => {
    const slugs = [
      "spectral-class",
      "white-dwarf",
      "brown-dwarf",
      "binary-and-multiple-stars",
      "exoplanet",
      "habitable-zone",
      "orbital-period",
      "astronomical-evidence",
    ];
    for (const slug of slugs) {
      await page.goto(`/concepts/${slug}`, { waitUntil: "domcontentloaded" });
      const article = page.locator(".concept-article");
      await expect(article.getByRole("heading", { level: 2 })).toBeVisible();
      await expect(article).toContainText("Systems to inspect");
      await expect(article).toContainText("Related:");
    }
  });

  test("4K tags remain bounded and accessible in light and dark themes", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name !== "desktop-1440", "One 4K desktop pass is sufficient");
    const castor = await resolveSystem(page, "Castor");
    expect(castor).toBeTruthy();
    await page.setViewportSize({ width: 3840, height: 2160 });

    for (const theme of ["simple_dark", "simple_light"]) {
      await page.goto(`/systems/${castor.system_id}`, { waitUntil: "domcontentloaded" });
      await page.evaluate((value) => window.localStorage.setItem("spacegate.theme", value), theme);
      await page.reload({ waitUntil: "domcontentloaded" });
      await expect.poll(
        () => page.evaluate(() => document.documentElement.dataset.theme || ""),
      ).toBe(theme);

      const trigger = page.locator(".system-detail-tags .smart-tag-trigger").first();
      await expect(trigger).toBeVisible();
      await expect(trigger).toHaveAttribute("aria-haspopup", "dialog");
      await expect(trigger).toHaveAttribute("aria-expanded", "false");
      expect((await trigger.getAttribute("aria-label"))?.trim().length).toBeGreaterThan(0);
      await trigger.focus();
      await expect(trigger).toHaveAttribute("aria-expanded", "true");
      const dialog = page.getByRole("dialog", { name: /details$/ }).first();
      await expect(dialog).toBeVisible();
      const bounds = await dialog.boundingBox();
      expect(bounds).toBeTruthy();
      expect(bounds.x).toBeGreaterThanOrEqual(0);
      expect(bounds.y).toBeGreaterThanOrEqual(0);
      expect(bounds.x + bounds.width).toBeLessThanOrEqual(3840);
      expect(bounds.y + bounds.height).toBeLessThanOrEqual(2160);
      await page.keyboard.press("Escape");
      await expect(dialog).not.toBeVisible();

      await page.screenshot({
        path: testInfo.outputPath(`smart-tags-4k-${theme}.png`),
        fullPage: false,
      });
      await expect.poll(
        () => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 4),
      ).toBeTruthy();
    }
  });
});
