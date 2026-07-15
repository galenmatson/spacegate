import { expect, test } from "@playwright/test";
import { PUBLIC_EXPERIENCE_GOLDENS, TECHNICAL_SYSTEM_GOLDENS } from "../fixtures/publicExperienceGoldens.mjs";

async function openMap(page) {
  await page.goto("/map", { waitUntil: "networkidle" });
  await page.locator(".map-canvas canvas").waitFor();
  await page.waitForTimeout(1200);
}

async function openMapPeekFromRecents(page) {
  const searchToggle = page.locator("[data-testid='map-search-toggle']");
  if (await searchToggle.getAttribute("aria-pressed") !== "true") {
    await searchToggle.click();
  }
  await page.locator(".map-search-recents").first().locator(".map-search-recent-pill").first().click();
  if (await searchToggle.getAttribute("aria-pressed") === "true") {
    await searchToggle.click();
  }
  await expect(page.locator("[data-testid='map-system-drill']")).toBeVisible();
}

async function canvasBox(page) {
  const box = await page.locator(".map-canvas canvas").boundingBox();
  expect(box, "map canvas box").toBeTruthy();
  return box;
}

function parseCameraPosition(value) {
  return String(value || "")
    .split(",")
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item));
}

function cameraDistance(a, b) {
  const first = parseCameraPosition(a);
  const second = parseCameraPosition(b);
  if (first.length !== 3 || second.length !== 3) {
    return Number.POSITIVE_INFINITY;
  }
  return Math.hypot(first[0] - second[0], first[1] - second[1], first[2] - second[2]);
}

async function expectPreviewCanvasPainted(previewCanvas, label) {
  await expect.poll(
    () => previewCanvas.evaluate((canvas) => {
      const gl = canvas.getContext("webgl2") || canvas.getContext("webgl") || canvas.getContext("experimental-webgl");
      if (!gl) {
        return 0;
      }
      const width = gl.drawingBufferWidth || canvas.width || 0;
      const height = gl.drawingBufferHeight || canvas.height || 0;
      if (!width || !height) {
        return 0;
      }
      const pixels = new Uint8Array(width * height * 4);
      gl.readPixels(0, 0, width, height, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
      let brightPixels = 0;
      for (let idx = 0; idx < pixels.length; idx += 4) {
        const red = pixels[idx];
        const green = pixels[idx + 1];
        const blue = pixels[idx + 2];
        const alpha = pixels[idx + 3];
        const maxChannel = Math.max(red, green, blue);
        if (alpha > 0 && (red + green + blue > 90 || maxChannel > 48)) {
          brightPixels += 1;
        }
      }
      return brightPixels;
    }),
    { timeout: 5000, message: `${label} preview canvas should paint visible scene pixels` }
  ).toBeGreaterThan(30);
}

async function resolveGoldenSystem(page, golden) {
  const response = await page.request.get("/api/v1/systems/search", {
    params: { q: golden.query, limit: "1", sort: "match" },
  });
  expect(response.ok(), `${golden.query} search response`).toBeTruthy();
  const payload = await response.json();
  return payload.items?.[0] || null;
}

test.describe("public 3D map beta", () => {
  test("mission control browser header keeps utility links visible", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop browser header check");
    await page.addInitScript(() => {
      window.localStorage.setItem("spacegate.theme", "mission_control");
    });
    await page.goto("/search", { waitUntil: "domcontentloaded" });
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("mission_control");
    const expectedLabels = ["HELP", "ABT", "MAP", "SPT", "SRC"];
    const headerBox = await page.locator(".site-header").boundingBox();
    expect(headerBox, "mission control header box").toBeTruthy();
    for (const label of expectedLabels) {
      const link = page.locator(".header-top-link", { hasText: label });
      await expect(link, `${label} header utility link`).toBeVisible();
      await expect(link, `${label} header utility link should be clickable`).toHaveCSS("pointer-events", "auto");
      const box = await link.boundingBox();
      expect(box, `${label} header utility link box`).toBeTruthy();
      expect(box.y - headerBox.y, `${label} should sit in the mission control top strip`).toBeLessThan(32);
    }
  });

  test("default route opens map-native Star Search controls", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop default route check");
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.locator(".map-canvas canvas").waitFor();
    await expect(page.locator(".map-star-search")).toBeVisible();
    await expect(page.locator("[data-testid='map-star-search-input']")).toBeVisible();
    await expect(page.locator(".map-search-sidebar")).toContainText(/Filters/i);
    await expect(page.locator(".map-search-habitable")).toBeVisible();
    const searchToggle = page.locator("[data-testid='map-search-toggle']");
    const minimalToggle = page.locator("[data-testid='map-minimal-toggle']");
    await expect(minimalToggle).toHaveText("MIN");
    await minimalToggle.click();
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "true");
    await expect(page.locator(".map-hud-top")).toBeHidden();
    await expect(page.locator(".map-minimal-notice")).toContainText(/Minimal mode/i);
    await page.keyboard.press("Escape");
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "false");
    await expect(page.locator(".map-hud-top")).toBeVisible();
    await page.keyboard.press("m");
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "true");
    await page.keyboard.press("m");
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "false");
    await expect(searchToggle).toHaveAttribute("aria-pressed", "true");
    await searchToggle.click();
    await expect(page.locator(".map-star-search")).toBeHidden();
    await expect(searchToggle).toHaveAttribute("aria-pressed", "false");
    await searchToggle.click();
    await expect(page.locator(".map-star-search")).toBeVisible();
    await expect(searchToggle).toHaveAttribute("aria-pressed", "true");
    await page.locator(".map-search-recent-pill").first().hover();
    await page.waitForTimeout(150);
    await expect(page.locator(".map-name-popover")).toHaveCount(0);
    await page.locator(".map-menu-button").click();
    await page.locator("[data-testid='map-fps-toggle']").check();
    await expect(page.locator("[data-testid='map-fps-overlay']")).toBeVisible();
    await expect(page.locator("[data-testid='map-fps-overlay']")).toContainText(/FPS/i);
    await expect(page.locator("[data-testid='map-fps-overlay']")).toContainText(/WebGL/i);
    await expect(page.locator("[data-testid='map-fps-overlay']")).toContainText(/Previews/i);
    await expect(page.locator("[data-testid='map-fps-overlay']")).toContainText(/Quality/i);
    await page.locator("[data-testid='map-fps-toggle']").uncheck();
    await expect(page.locator("[data-testid='map-fps-overlay']")).toHaveCount(0);
    await page.locator(".map-search-spectral", { hasText: "G" }).click();
    await expect
      .poll(() => page.locator(".map-canvas canvas").evaluate((node) => node.dataset.mapLabelStrategy || ""), { timeout: 3000 })
      .toBe("star_search_filters");
    await page.locator("[data-testid='map-star-search-input']").fill("Sol");
    await page.locator(".map-search-topbar").getByRole("button", { name: /^Search$/ }).click();
    await expect(page.locator("[data-testid='map-star-search-results']")).toBeVisible();
    await expect(page.locator(".map-search-sort select")).toBeVisible();
    await page.locator(".map-search-sort select").selectOption("distance");
    await expect(page.locator(".map-search-sort select")).toHaveValue("distance");
    await expect(page.locator(".map-search-card").first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".map-search-card").first().getByRole("link", { name: "Detail" })).toBeVisible();
    await expect(page.locator(".map-search-card").first().locator(".stellar-class-chip")).toHaveCount(1);
    await expect.poll(async () => {
      const box = await page.locator(".map-search-card-preview").first().boundingBox();
      return Math.round(box?.height || 0);
    }).toBeGreaterThan(180);
    await expect
      .poll(() => page.locator(".map-search-card-preview img.map-search-card-capture").count(), { timeout: 10000 })
      .toBeGreaterThan(0);
    await expect
      .poll(() => page.locator(".map-search-card-preview .system-preview-canvas canvas").count(), { timeout: 10000 })
      .toBeLessThanOrEqual(1);
    const cachedPreview = page.locator(".map-search-card-preview.is-cached").first();
    await cachedPreview.hover();
    await expect
      .poll(() => page.locator(".map-search-card-preview .system-preview-canvas canvas").count(), { timeout: 10000 })
      .toBeGreaterThan(0);
    await expect(page.locator(".map-search-card-preview .system-preview-hover")).toHaveCount(0);
    await expect(page.locator(".map-search-card-preview [data-testid='system-preview-pinned']")).toHaveCount(0);
    await page.mouse.move(20, 20);
    await expect
      .poll(() => page.locator(".map-search-card-preview img.map-search-card-capture").count(), { timeout: 10000 })
      .toBeGreaterThan(0);
    await expect(page.locator(".map-search-card-preview").getByRole("button", { name: /live preview/i })).toHaveCount(0);
    expect(await page.locator(".map-search-card-preview .system-preview-canvas canvas").count()).toBeLessThanOrEqual(4);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => node.dataset.runtimePreviewPoolBudget || ""),
      { timeout: 3000 }
    ).toMatch(/[1-4]/);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => node.dataset.runtimeQualityTier || ""),
      { timeout: 3000 }
    ).toMatch(/high|balanced|low/);
    await page.locator(".map-search-card-actions .map-command-button.primary").first().click();
    await expect(page.locator("[data-testid='map-system-drill']")).toBeVisible();
    await expect(page.locator("[data-testid='map-star-search-results']")).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-search-card-preview .system-preview-canvas canvas").count(),
      { timeout: 5000 }
    ).toBeLessThanOrEqual(1);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => node.dataset.runtimePreviewPoolBudget || ""),
      { timeout: 3000 }
    ).toBe("1");
  });

  test("standalone Star Search v2 uses bounded simulation previews", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop catalog search preview check");
    await page.addInitScript(() => {
      window.localStorage.setItem("spacegate.theme", "lcars");
    });
    await page.goto("/search?q=Tau%20Ceti&sort=match", { waitUntil: "domcontentloaded" });
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("lcars");
    await expect(page.locator(".header-brand-line")).toContainText("Interstellar Explorer");
    await expect(page.locator(".header-brand-line")).toContainText("Discover and explore nearby systems, stars, and exoplanets.");
    await expect(page.locator(".title-link")).toHaveAttribute("href", "/search");
    const headerMenu = page.locator(".header-menu");
    await expect(headerMenu).toBeVisible();
    await headerMenu.locator("summary").click();
    await expect(headerMenu.locator("[data-testid='global-default-scale-select']")).toBeVisible();
    await expect(page.locator(".catalog-search-topbar")).toBeVisible();
    await expect(page.locator(".catalog-search-sidebar")).toBeVisible();
    await expect.poll(() => page.locator(".catalog-search-sidebar").evaluate((node) => (
      Math.ceil(node.scrollWidth - node.clientWidth)
    ))).toBeLessThanOrEqual(1);
    await expect.poll(() => page.locator(".catalog-search-sidebar").evaluate((node) => {
      const rootStyle = window.getComputedStyle(document.documentElement);
      const railWidth = Number.parseFloat(rootStyle.getPropertyValue("--lcars-rail-width")) || 0;
      const sidebarWidth = node.getBoundingClientRect().width;
      return Math.round(Math.abs(sidebarWidth - railWidth));
    })).toBeLessThanOrEqual(1);
    await expect(page.locator(".results-toolbar")).toBeVisible();
    await expect(page.locator(".results-toolbar-head")).toContainText("Star Search");
    const sortSelect = page.locator(".results-search-options select").first();
    await expect(sortSelect).toHaveValue("match");
    await expect(sortSelect.locator("option[value='planet_count']")).toHaveCount(1);
    await expect(sortSelect.locator("option[value='star_count']")).toHaveCount(1);
    await expect(sortSelect.locator("option[value='hottest']")).toHaveCount(1);
    await expect(sortSelect.locator("option[value='coolest']")).toHaveCount(1);
    await expect(page.locator(".spectral-chip", { hasText: "T" })).toBeVisible();
    await expect(page.locator(".spectral-chip", { hasText: "Y" })).toBeVisible();
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".result-card").first().locator(".result-tags")).toContainText(/Nearby|Exoplanet|Multi-planet|High coolness|NASA|Gaia/i);
    await expect(page.locator(".result-card").first().locator(".result-stellar-tags .stellar-class-chip")).toHaveCount(1);
    await expect(page.locator(".result-card").first().getByRole("link", { name: "Detail" })).toBeVisible();
    await expect(page.locator(".result-card").first().getByRole("button", { name: "Map" })).toBeVisible();
    const firstPreview = page.locator("[data-testid='star-search-simulation-preview']").first();
    await expect(firstPreview).toBeVisible();
    await expect
      .poll(() => page.locator("[data-testid='star-search-simulation-preview'] .system-preview-canvas canvas").count(), { timeout: 10000 })
      .toBeLessThanOrEqual(4);
    await expect
      .poll(() => page.locator("[data-testid='star-search-simulation-preview'][data-preview-state='cached']").count(), { timeout: 15000 })
      .toBeGreaterThan(0);
    await firstPreview.hover();
    await expect
      .poll(() => page.locator("[data-testid='star-search-simulation-preview'] .system-preview-canvas canvas").count(), { timeout: 10000 })
      .toBeGreaterThan(0);
    await expect(page.locator("[data-testid='star-search-simulation-preview'] .system-preview-hover")).toHaveCount(0);
    await sortSelect.selectOption("planet_count");
    await expect(sortSelect).toHaveValue("planet_count");
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await sortSelect.selectOption("star_count");
    await expect(sortSelect).toHaveValue("star_count");
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await sortSelect.selectOption("hottest");
    await expect(sortSelect).toHaveValue("hottest");
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await sortSelect.selectOption("coolest");
    await expect(sortSelect).toHaveValue("coolest");
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
  });

  test("standalone Star Search uses lightweight previews for simple singleton systems", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop catalog preview policy check");
    const sceneRequests = [];
    await page.route("**/api/v1/systems/search**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              system_id: 900001,
              system_name: "Preview Test M Dwarf",
              display_name: "Preview Test M Dwarf",
              stable_object_key: "test.preview.m_dwarf",
              dist_ly: 12.34,
              ra_deg: 0,
              dec_deg: 0,
              star_count: 1,
              planet_count: 0,
              coolness_score: 4.2,
              coolness_rank: null,
              min_star_teff_k: 3200,
              max_star_teff_k: 3200,
              spectral_classes: ["M"],
              preview_tier: "lightweight_singleton",
              preview_basis: ["single_or_unresolved_star", "no_planets", "low_preview_complexity"],
              is_lightweight_preview_safe: true,
              has_prebuilt_simulation_scene: false,
              snapshot: null,
              display_aliases: [],
            },
          ],
          next_cursor: null,
          has_more: false,
          total_count: 1,
          query_time_ms: 1,
          origin: null,
        }),
      });
    });
    await page.route("**/api/v1/systems/*/simulation-scene", async (route) => {
      sceneRequests.push(route.request().url());
      await route.continue();
    });
    await page.goto("/search?spectral_class=M&max_star_count=1&max_planet_count=0&max_coolness_score=19.9&sort=distance&limit=8", { waitUntil: "domcontentloaded" });
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator("[data-testid='lightweight-system-preview']").first()).toBeVisible();
    await expect(page.locator("[data-testid='star-search-simulation-preview'][data-preview-state='lightweight']").first()).toBeVisible();
    await expect.poll(() => page.locator("[data-testid='star-search-simulation-preview'] .system-preview-canvas canvas").count(), { timeout: 3000 }).toBe(0);
    expect(sceneRequests).toHaveLength(0);
  });

  test("standalone Star Search keeps full previews for planet hosts", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop catalog preview policy check");
    const response = await page.request.get("/api/v1/systems/search", {
      params: {
        min_planet_count: "1",
        sort: "distance",
        limit: "5",
      },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    expect(payload.items.length).toBeGreaterThan(0);
    expect(payload.items.some((item) => item.preview_tier === "dynamic_simulation_scene" || item.preview_tier === "prebuilt_simulation_scene")).toBeTruthy();
    await page.goto("/search?min_planet_count=1&sort=distance&limit=5", { waitUntil: "domcontentloaded" });
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator("[data-testid='star-search-simulation-preview'][data-preview-tier='lightweight_singleton']")).toHaveCount(0);
    await expect.poll(
      () => page.locator("[data-testid='star-search-simulation-preview'][data-preview-state='live'], [data-testid='star-search-simulation-preview'][data-preview-state='cached']").count(),
      { timeout: 12000 }
    ).toBeGreaterThan(0);
  });

  test("system page v2 stages simulation, overview, and technical evidence", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop system page anatomy check");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "Tau Ceti", limit: "1", sort: "match" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "Tau Ceti system_id").toBeTruthy();

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator(".system-detail-v2 h1")).toContainText(/tau Cet|Tau Ceti/i);
    await expect(page.locator(".site-header .header-lower")).toHaveCount(0);
    await expect(page.locator(".system-search-topbar .map-search-main")).toBeVisible();
    await expect(page.locator(".system-detail-name-line .id-chip").first()).toBeVisible();
    await expect(page.locator(".system-detail-name-line .id-chip", { hasText: "Unknown" })).toHaveCount(0);
    await expect(page.locator(".system-detail-hero-copy > .system-detail-ids")).toHaveCount(0);
    await expect(page.locator(".system-detail-class-tags .stellar-class-chip").first()).toBeVisible();
    await expect(page.locator(".system-detail-class-tags .result-tag").first()).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-header h3")).toHaveText("System Simulation");
    await expect(page.locator(".system-preview-header h3")).toHaveAttribute("title", /Source-aware system renderer/);
    await expect(page.locator(".system-preview-header .system-preview-actions")).not.toContainText(/LOCAL CLARITY|render_scene/i);
    await expect(page.locator("[data-testid='system-preview-object-list']")).toBeVisible();
    await expect
      .poll(() => page.locator("[data-testid='system-preview-object-list'] .system-preview-object-chip").count())
      .toBeGreaterThanOrEqual(4);
    await expect(page.locator("[data-testid='system-preview-object-list']")).toContainText(/Planet/i);
    await page.locator("[data-testid='system-preview-object-list'] .system-preview-object-chip").first().click();
    const pinnedReadout = page.locator("[data-testid='system-preview-pinned']");
    await expect(pinnedReadout).toBeVisible();
    const readoutTitle = pinnedReadout.locator(".system-preview-pinned-title");
    const beforeDragBox = await pinnedReadout.boundingBox();
    const titleBox = await readoutTitle.boundingBox();
    expect(beforeDragBox, "pinned readout before drag box").toBeTruthy();
    expect(titleBox, "pinned readout title box").toBeTruthy();
    await page.mouse.move(titleBox.x + titleBox.width / 2, titleBox.y + titleBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(titleBox.x + titleBox.width / 2 - 90, titleBox.y + titleBox.height / 2 - 70, { steps: 6 });
    await page.mouse.up();
    await expect.poll(async () => {
      const afterDragBox = await pinnedReadout.boundingBox();
      if (!afterDragBox || !beforeDragBox) {
        return 0;
      }
      return Math.hypot(afterDragBox.x - beforeDragBox.x, afterDragBox.y - beforeDragBox.y);
    }).toBeGreaterThan(20);
    await expect(page.locator(".system-detail-stellar-tags .stellar-class-chip")).toHaveCount(1);
    await expect(page.locator(".hierarchy-panel .hierarchy-node-title-row .stellar-class-chip").first()).toBeVisible();
    await expect(page.locator(".hierarchy-panel .hierarchy-node-title-row .stellar-class-chip").first()).toContainText("K");
    await page.locator(".system-preview-line-menu summary").click();
    await expect(page.locator(".system-preview-line-menu")).toHaveAttribute("open", "");
    await page.locator(".system-detail-v2 h1").click({ force: true });
    await expect(page.locator(".system-preview-line-menu")).not.toHaveAttribute("open", "");
    await expect(page.locator(".system-search-topbar").getByRole("button", { name: "Map" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "What You’re Looking At" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "Why This System Matters" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "Infrared View" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "What We Know" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "What Remains Uncertain" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "Further Exploration" })).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "Future AAA Narrative Slot" })).toHaveCount(0);
    await expect(page.locator(".system-glance-strip")).toContainText(/Distance from Sol/i);
    await expect(page.locator(".hierarchy-panel h3")).toHaveText("Stars and Hierarchy");
    await expect(page.locator(".hierarchy-fact-chip").first()).toHaveAttribute("title", /Spectral class|Effective temperature|Mass|Radius|Luminosity|Visual magnitude|Distance|Separation/i);
    await expect(page.locator(".concept-panel")).toContainText(/Habitable zone/i);
    await expect(page.locator("details.detail-disclosure", { hasText: "Stars and Catalog Rows" })).not.toHaveAttribute("open", "");
    await expect(page.locator("details.detail-disclosure", { hasText: "Planets and Orbits" })).not.toHaveAttribute("open", "");
    const technicalDisclosure = page.locator(".detail-disclosure", { hasText: "Evidence and Technical Data" });
    await expect(technicalDisclosure).toBeVisible();
    await expect(technicalDisclosure).not.toHaveAttribute("open", "");
    await technicalDisclosure.scrollIntoViewIfNeeded();
    await expect.poll(
      () => page.locator("[data-testid='system-preview-panel']").evaluate((node) => node.dataset.simulationRunning || ""),
      { timeout: 5000 }
    ).toBe("false");
    await technicalDisclosure.locator("summary").click();
    await expect(technicalDisclosure.locator(".system-technical-strip")).toContainText(/Sky Position/i);
    await page.locator("[data-global-search-input='true']").fill("Sirius");
    await page.locator(".system-search-topbar").getByRole("button", { name: "Search" }).click();
    await expect(page).toHaveURL(/\/search\?q=Sirius&sort=match/);
    await expect(page.locator(".results-toolbar")).toBeVisible();
  });

  test("public experience goldens resolve through Star Search", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "API-level public golden search check");
    const unresolved = [];
    for (const golden of PUBLIC_EXPERIENCE_GOLDENS) {
      const item = await resolveGoldenSystem(page, golden);
      if (!item) {
        unresolved.push(golden.id);
        expect(golden.expectedStatus, `${golden.query} unresolved`).toBe("known_gap");
        continue;
      }
      const displayName = String(item.display_name || item.system_name || "");
      expect(displayName, `${golden.query} display name`).toMatch(golden.expectedNamePattern);
      if (Number.isFinite(Number(golden.minStars))) {
        expect(Number(item.star_count || 0), `${golden.query} star count`).toBeGreaterThanOrEqual(Number(golden.minStars));
      }
      if (Number.isFinite(Number(golden.minPlanets))) {
        expect(Number(item.planet_count || 0), `${golden.query} planet count`).toBeGreaterThanOrEqual(Number(golden.minPlanets));
      }
    }
    expect(unresolved, "Only documented known-gap public goldens may be unresolved").toEqual(["vega"]);
  });

  test("technical stress goldens remain reachable for system-page checks", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "API-level technical golden search check");
    for (const golden of TECHNICAL_SYSTEM_GOLDENS) {
      const item = await resolveGoldenSystem(page, golden);
      expect(item, `${golden.query} should resolve`).toBeTruthy();
      const displayName = String(item.display_name || item.system_name || "");
      expect(displayName, `${golden.query} display name`).toMatch(golden.expectedNamePattern);
      if (Number.isFinite(Number(golden.minStars))) {
        expect(Number(item.star_count || 0), `${golden.query} star count`).toBeGreaterThanOrEqual(Number(golden.minStars));
      }
      if (Number.isFinite(Number(golden.minPlanets))) {
        expect(Number(item.planet_count || 0), `${golden.query} planet count`).toBeGreaterThanOrEqual(Number(golden.minPlanets));
      }
    }
  });

  test("alias authority resolves member and catalog names without bad fuzzy substitutes", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "API-level alias authority check");

    const search = async (query, limit = 5) => {
      const response = await page.request.get("/api/v1/systems/search", {
        params: { q: query, limit: String(limit), sort: "match" },
      });
      expect(response.ok(), `${query} search response`).toBeTruthy();
      const payload = await response.json();
      return payload.items || [];
    };

    const alpha = (await search("Alpha Centauri", 1))[0];
    const proxima = (await search("Proxima Centauri", 1))[0];
    expect(alpha?.system_id, "Alpha Centauri system id").toBeTruthy();
    expect(proxima?.system_id, "Proxima Centauri system id").toBe(alpha.system_id);
    expect(Number(proxima?.planet_count || 0), "Proxima search keeps planet context").toBeGreaterThanOrEqual(2);

    for (const query of ["HD 128620", "HIP 71683"]) {
      const item = (await search(query, 1))[0];
      expect(item?.wds_id, `${query} accepted system`).toBe("14396-6050");
      expect(String(item?.display_name || ""), `${query} should not become public title`).not.toBe(query);
      expect(String(item?.matched_alias || ""), `${query} matched alias`).toBe(query);
    }

    const gliese412 = (await search("Gliese 412", 1))[0];
    expect(gliese412, "Gliese 412 should resolve").toBeTruthy();
    expect(Number(gliese412.dist_ly || 999), "Gliese 412 distance").toBeLessThan(17);
    expect(String(gliese412.display_name || gliese412.system_name || ""), "Gliese 412 false match guard").not.toMatch(/Gliese 12/i);
    expect([gliese412.matched_alias, ...(gliese412.display_aliases || [])].join(" "), "Gliese 412 alias evidence").toMatch(/Gliese 412|Gl 412|GJ 412/i);

    const alphaLib = (await search("alf02 Lib", 1))[0];
    expect(alphaLib?.wds_id, "alf02 Lib accepted system").toBe("14509-1603");
    expect(String(alphaLib?.display_name || ""), "abbreviated Bayer should not be primary display").not.toBe("alf02 Lib");

    const gliese643 = (await search("Gliese 643", 1))[0];
    expect(gliese643?.wds_id, "Gliese 643 should resolve into V1054 Oph").toBe("16555-0820");

    const v1513 = await search("V1513 Cyg", 3);
    expect(v1513.map((item) => String(item.display_name || item.system_name || "")).join(" "), "V1513 Cyg fuzzy guard").not.toMatch(/V1581 Cyg/i);
  });

  test("public experience golden system pages expose v2 anatomy", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop public golden anatomy smoke");
    const anatomyGoldenIds = new Set(["tau_ceti", "trappist_1", "alpha_centauri", "sirius", "55_cancri"]);
    const targets = PUBLIC_EXPERIENCE_GOLDENS.filter((golden) => anatomyGoldenIds.has(golden.id));
    for (const golden of targets) {
      const item = await resolveGoldenSystem(page, golden);
      expect(item, `${golden.query} should resolve for page anatomy`).toBeTruthy();
      await page.goto(`/systems/${item.system_id}`, { waitUntil: "domcontentloaded" });
      await expect(page.locator(".system-detail-v2 h1")).toContainText(golden.expectedNamePattern);
      await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
      await expect(page.locator(".system-story-card", { hasText: "What You’re Looking At" })).toBeVisible();
      await expect(page.locator(".system-story-card", { hasText: "Why This System Matters" })).toBeVisible();
      await expect(page.locator(".concept-panel")).toContainText(/Spectral class/i);
      await expect(page.locator(".detail-disclosure", { hasText: "Evidence and Technical Data" })).toBeVisible();
    }
  });

  test("mobile Star Search v2 and system page stay readable", async ({ page }, testInfo) => {
    test.skip(!testInfo.project.name.includes("mobile"), "mobile-only public search smoke");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "Tau Ceti", limit: "1", sort: "match" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "Tau Ceti system_id").toBeTruthy();

    await page.goto("/search?q=Tau%20Ceti&sort=match", { waitUntil: "domcontentloaded" });
    await expect(page.locator(".results-toolbar")).toBeVisible();
    await expect(page.locator(".result-card").first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator("[data-testid='star-search-simulation-preview']").first()).toBeVisible();
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 4)).toBeTruthy();

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator(".system-detail-v2 h1")).toContainText(/tau Cet|Tau Ceti/i);
    const prefixedIdCopy = page.locator(
      ".id-chip .id-copy[data-copy-value^='HD '], .id-chip .id-copy[data-copy-value^='HIP '], .id-chip .id-copy[data-copy-value^='Gaia ']"
    );
    await expect(prefixedIdCopy.first()).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-story-card", { hasText: "What You’re Looking At" })).toBeVisible();
    await expect(page.locator("details.detail-disclosure", { hasText: "Stars and Catalog Rows" })).not.toHaveAttribute("open", "");
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 4)).toBeTruthy();
  });

  test("map title comes from public branding config", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop header title check");
    const configResponse = await page.request.get("/api/v1/public-config");
    expect(configResponse.ok()).toBeTruthy();
    const config = await configResponse.json();
    await openMap(page);
    await expect(page.locator(".map-eyebrow-link")).toHaveText("Spacegate Stellar Database");
    await expect(page.locator(".map-eyebrow-link")).toHaveAttribute("href", config.spacegate_url || "/search");
    await expect(page.locator(".map-brand-mark")).toBeVisible();
    await expect(page.locator(".map-brand-mark")).toHaveAttribute("src", "/favicon.svg");
    await expect(page.locator(".map-title-block h1")).toHaveText(config.map_title || "Coolstars Map");
    await expect(page.locator(".map-title-link")).toHaveAttribute("href", "/map");
  });

  test("fast search-result scrolling keeps live preview pool bounded", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop fast-scroll stress check");
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.locator(".map-canvas canvas").waitFor();
    await page.locator(".map-search-topbar").getByRole("button", { name: /^Search$/ }).click();
    await expect(page.locator("[data-testid='map-star-search-results']")).toBeVisible();
    await expect(page.locator(".map-search-card").first()).toBeVisible({ timeout: 10000 });
    for (let idx = 0; idx < 10; idx += 1) {
      await page.mouse.wheel(0, 900);
      await page.waitForTimeout(35);
    }
    await expect(page.locator(".map-canvas canvas")).toBeVisible();
    await expect.poll(
      () => page.locator(".map-search-card-preview .system-preview-canvas canvas").count(),
      { timeout: 10000 }
    ).toBeLessThanOrEqual(4);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => Number(node.dataset.runtimePreviewPoolActive || 0)),
      { timeout: 3000 }
    ).toBeLessThanOrEqual(4);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => node.dataset.runtimeQualityTier || ""),
      { timeout: 3000 }
    ).toMatch(/high|balanced|low/);
  });

  test("map canvas recovers from WebGL context loss", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop context recovery smoke");
    await openMap(page);
    const initialCanvas = page.locator(".map-canvas canvas");
    const box = await initialCanvas.boundingBox();
    expect(box, "map canvas box before forced recovery").toBeTruthy();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    const defaultCamera = await initialCanvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.wheel(0, -420);
    await expect.poll(
      async () => cameraDistance(defaultCamera, await initialCanvas.evaluate((node) => node.dataset.mapCameraPosition || "")),
      { timeout: 3000 }
    ).toBeGreaterThan(0.5);
    const beforeRecoveryCamera = await initialCanvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    expect(parseCameraPosition(beforeRecoveryCamera).length).toBe(3);
    await initialCanvas.evaluate((canvas) => {
      canvas.dispatchEvent(new Event("webglcontextlost", { cancelable: true }));
    });
    await expect(page.locator(".map-context-recovery")).toBeVisible();
    await expect(page.locator(".map-canvas canvas")).toBeVisible();
    await expect.poll(
      async () => cameraDistance(
        beforeRecoveryCamera,
        await page.locator(".map-canvas canvas").evaluate((node) => node.dataset.mapCameraPosition || "")
      ),
      { timeout: 5000 }
    ).toBeLessThan(0.25);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => Number(node.dataset.runtimeContextRecoveries || 0)),
      { timeout: 5000 }
    ).toBeGreaterThanOrEqual(1);
  });

  test("duplicate context-loss events do not cause a recovery storm", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop context recovery regression");
    await openMap(page);
    const initialCanvas = page.locator(".map-canvas canvas");
    await initialCanvas.evaluate((canvas) => {
      canvas.dispatchEvent(new Event("webglcontextlost", { cancelable: true }));
      canvas.dispatchEvent(new Event("webglcontextlost", { cancelable: true }));
    });
    await expect(page.locator(".map-context-recovery")).toBeVisible();
    await page.waitForTimeout(1900);
    await expect.poll(
      () => page.locator(".map-canvas canvas").evaluate((node) => Number(node.dataset.runtimeContextRecoveries || 0)),
      { timeout: 3000 }
    ).toBe(1);
  });

  test("desktop resize preserves the active tile transport", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop resize transport regression");
    let manifestRequests = 0;
    page.on("request", (request) => {
      if (/\/map-tiles\/(?:index|radius-100\/manifest)\.json$/.test(new URL(request.url()).pathname)) {
        manifestRequests += 1;
      }
    });
    await openMap(page);
    const beforeResize = manifestRequests;
    expect(beforeResize).toBeGreaterThanOrEqual(2);
    await page.setViewportSize({ width: 1360, height: 840 });
    await page.waitForTimeout(1800);
    expect(manifestRequests).toBe(beforeResize);
    await expect(page.locator(".map-canvas canvas")).toBeVisible();
  });

  test("parked map suppresses redundant telemetry and label rebuilds", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop idle runtime regression");
    await openMap(page);
    const canvas = page.locator(".map-canvas canvas");
    await expect.poll(
      () => canvas.evaluate((node) => Number(node.dataset.mapTelemetryIdleSkips || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(0);
    const before = await canvas.evaluate((node) => ({
      telemetry: Number(node.dataset.mapTelemetryEmits || 0),
      telemetrySkips: Number(node.dataset.mapTelemetryIdleSkips || 0),
      labelRebuilds: Number(node.dataset.mapLabelRebuilds || 0),
      labelSkips: Number(node.dataset.mapLabelIdleSkips || 0),
      textures: Number(node.dataset.runtimeWebglTextures || 0),
      geometries: Number(node.dataset.runtimeWebglGeometries || 0),
    }));
    await page.waitForTimeout(2200);
    const after = await canvas.evaluate((node) => ({
      telemetry: Number(node.dataset.mapTelemetryEmits || 0),
      telemetrySkips: Number(node.dataset.mapTelemetryIdleSkips || 0),
      labelRebuilds: Number(node.dataset.mapLabelRebuilds || 0),
      labelSkips: Number(node.dataset.mapLabelIdleSkips || 0),
      textures: Number(node.dataset.runtimeWebglTextures || 0),
      geometries: Number(node.dataset.runtimeWebglGeometries || 0),
    }));
    expect(after.telemetry).toBe(before.telemetry);
    expect(after.telemetrySkips).toBeGreaterThan(before.telemetrySkips);
    expect(after.labelRebuilds).toBe(before.labelRebuilds);
    expect(after.labelSkips).toBeGreaterThan(before.labelSkips);
    expect(after.textures).toBe(before.textures);
    expect(after.geometries).toBe(before.geometries);
  });

  test("system detail return restores map camera and selection", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop detail return flow");
    await openMap(page);
    await openMapPeekFromRecents(page);
    const drill = page.locator("[data-testid='map-system-drill']");
    await expect(drill).toBeVisible();
    await drill.getByRole("button", { name: /^Explore$/i }).click();
    await expect(drill).toHaveAttribute("data-drill-mode", "explore");
    const selectedTitle = await drill.locator(".map-system-drill-title .map-name-wrap").innerText();
    const canvas = page.locator(".map-canvas canvas");
    await page.waitForTimeout(1100);
    const cameraBeforeDetail = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    expect(parseCameraPosition(cameraBeforeDetail).length).toBe(3);
    await drill.getByRole("button", { name: /^Detail$/i }).click();
    await expect(page).toHaveURL(/\/systems\/.+from=map.+map_return=/);
    const returnButton = page.locator(".map-return-button");
    await expect(returnButton).toBeVisible();
    await expect(returnButton).toHaveAttribute("href", /\/map\?restore=/);
    await returnButton.click();
    await expect(page).toHaveURL(/\/map\?restore=/);
    const restoredDrill = page.locator("[data-testid='map-system-drill']");
    await expect(restoredDrill).toBeVisible();
    await expect(restoredDrill).toHaveAttribute("data-drill-mode", "explore");
    await expect(restoredDrill.locator(".map-system-drill-title .map-name-wrap")).toContainText(selectedTitle.replace(/\s+/g, " ").trim());
    await expect.poll(
      async () => cameraDistance(
        cameraBeforeDetail,
        await page.locator(".map-canvas canvas").evaluate((node) => node.dataset.mapCameraPosition || "")
      ),
      { timeout: 5000 }
    ).toBeLessThan(0.25);
  });

  test("header menu controls theme and map keybind scheme", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop header menu and keyboard smoke");
    await openMap(page);
    const menu = page.locator(".map-header-menu");
    const canvas = page.locator(".map-canvas canvas");
    await expect(menu).toBeVisible();
    await expect(page.locator(".map-fullscreen-command")).toBeVisible();
    const homeButton = page.getByRole("button", { name: "SOL" });
    await expect(homeButton).toBeVisible();
    await homeButton.click();
    await expect(page.locator("[data-testid='map-system-drill']")).toBeVisible();
    await expect(page.locator(".map-system-drill-title")).toContainText("Sol");
    await expect(page.getByRole("button", { name: /capture mouse/i })).toHaveCount(0);
    await expect(page.getByRole("button", { name: /stabilize/i })).toHaveCount(0);
    await expect(page.locator(".map-actions > .map-theme-select")).toHaveCount(0);
    await menu.locator("summary").click();
    await expect(menu.locator(".map-header-menu-panel")).toBeVisible();
    const themeSelect = menu.locator(".map-theme-select select");
    const keybindSelect = menu.locator(".map-keybind-select select");
    const scaleSelect = menu.locator("[data-testid='map-default-scale-select']");
    const nameStyleSelect = menu.locator("[data-testid='map-name-style-select']");
    const frameSelect = menu.locator("[data-testid='map-frame-select']");
    const starRenderSelect = menu.locator("[data-testid='map-star-render-mode-select']");
    const classBadgesSelect = menu.locator("[data-testid='map-class-badges-select']");
    const directionToggle = menu.locator("[data-testid='map-direction-labels-toggle']");
    const gridToggle = menu.locator("[data-testid='map-grid-overlay-toggle']");
    await expect(themeSelect).toBeVisible();
    await expect(keybindSelect).toBeVisible();
    await expect(scaleSelect).toBeVisible();
    await expect(nameStyleSelect).toBeVisible();
    await expect(frameSelect).toBeVisible();
    await expect(starRenderSelect).toBeVisible();
    await expect(classBadgesSelect).toBeVisible();
    await expect(directionToggle).toBeVisible();
    await expect(gridToggle).toBeVisible();

    await expect(page.locator("[data-testid='map-grid-overlay']")).toBeVisible();
    await gridToggle.uncheck();
    await expect(page.locator("[data-testid='map-grid-overlay']")).toHaveCount(0);
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem("spacegate.map.gridOverlay"))).toBe("false");
    await gridToggle.check();
    await expect(page.locator("[data-testid='map-grid-overlay']")).toBeVisible();

    await themeSelect.selectOption("aurora");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("aurora");

    await starRenderSelect.selectOption("realistic");
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem("spacegate.map.starRenderMode") || "")).toBe("realistic");
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapStarRenderMode || ""),
      { timeout: 3000 }
    ).toBe("realistic");
    await expect.poll(
      () => canvas.evaluate((node) => Number(node.dataset.mapStarLayerCount || 0)),
      { timeout: 3000 }
    ).toBe(2);

    await starRenderSelect.selectOption("bright");
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem("spacegate.map.starRenderMode") || "")).toBe("bright");
    await expect(canvas).toHaveAttribute("data-map-star-render-mode", "bright");
    await expect(canvas).toHaveAttribute("data-map-label-class-strategy", "mass_proxy_then_intrinsic_brightness_v2");
    await expect(canvas).toHaveAttribute("data-map-label-class-badges", "primary");
    await classBadgesSelect.selectOption("all");
    await expect(canvas).toHaveAttribute("data-map-label-class-badges", "all");
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem("spacegate.map.classBadges"))).toBe("all");
    await classBadgesSelect.selectOption("off");
    await expect(canvas).toHaveAttribute("data-map-label-class-badges", "off");

    await keybindSelect.selectOption("esdf");
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapKeybindScheme || ""),
      { timeout: 3000 }
    ).toBe("esdf");
    await expect(page.locator(".map-desktop-hint")).toContainText(/ESDF fly/i);
    const beforeEsdfMove = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.evaluate(() => document.activeElement?.blur());
    await page.keyboard.down("e");
    await page.waitForTimeout(350);
    await page.keyboard.up("e");
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeEsdfMove);

    await nameStyleSelect.selectOption("astronomer_abbrev");
    await expect.poll(() => page.evaluate(() => window.localStorage.getItem("spacegate.nameStyle") || "")).toBe("astronomer_abbrev");
    await expect.poll(
      () => canvas.evaluate((node) => Number(node.dataset.mapTileCacheHits || 0)),
      { timeout: 5000 },
    ).toBeGreaterThan(0);

    await keybindSelect.selectOption("num8456");
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapKeybindScheme || ""),
      { timeout: 3000 }
    ).toBe("num8456");
    await page.waitForTimeout(900);
    const beforeTopRowNumber = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.keyboard.down("8");
    await page.waitForTimeout(350);
    await page.keyboard.up("8");
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""),
      { timeout: 3000 }
    ).toBe(beforeTopRowNumber);

    const beforeNumberMove = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.keyboard.down("Numpad8");
    await page.waitForTimeout(350);
    await page.keyboard.up("Numpad8");
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeNumberMove);

    await expect(directionToggle).toBeEnabled();
    await directionToggle.check();
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapDirectionLabels || ""),
      { timeout: 3000 }
    ).toBe("true");
    await expect.poll(
      () => canvas.evaluate((node) => Number(node.dataset.mapLabelCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(24);

    await frameSelect.selectOption("galactic");
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapFrame || ""),
      { timeout: 3000 }
    ).toBe("galactic");
    await expect(page.locator(".map-header-readout")).toContainText(/Galactic frame/i);
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapDirectionLabels || ""),
      { timeout: 3000 }
    ).toBe("true");

    await page.locator(".map-title-block").click();
    await expect(menu.locator(".map-header-menu-panel")).toBeHidden();

    const beforeArrowMove = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.keyboard.down("ArrowUp");
    await page.waitForTimeout(350);
    await page.keyboard.up("ArrowUp");
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeArrowMove);

    const mapBox = await canvasBox(page);
    const beforeWheelForward = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.move(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2);
    await page.mouse.wheel(0, -500);
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeWheelForward);
    const beforeWheelBack = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.wheel(0, 500);
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeWheelBack);

    const beforeWheelTruck = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.wheel(420, 0);
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeWheelTruck);

    const beforeRightTruck = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.move(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2);
    await page.mouse.down({ button: "right" });
    await page.mouse.move(mapBox.x + mapBox.width / 2 + 150, mapBox.y + mapBox.height / 2, { steps: 8 });
    await page.mouse.up({ button: "right" });
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeRightTruck);
    await expect.poll(() => canvas.evaluate((node) => node.dataset.mapCameraGesture || "")).toBe("right-drag-truck");
    await expect(page.locator(".map-context-menu")).toHaveCount(0);

    const beforeTwoButtonOrbit = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.move(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2);
    await page.mouse.down({ button: "right" });
    await page.mouse.down({ button: "left" });
    await page.mouse.move(mapBox.x + mapBox.width / 2 + 110, mapBox.y + mapBox.height / 2 + 40, { steps: 8 });
    await page.mouse.up({ button: "left" });
    await page.mouse.up({ button: "right" });
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeTwoButtonOrbit);
    await expect.poll(() => canvas.evaluate((node) => node.dataset.mapCameraGesture || "")).toBe("two-button-orbit");
    await expect(page.locator(".map-context-menu")).toHaveCount(0);

    const beforeMiddlePedestal = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.mouse.move(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2);
    await page.mouse.down({ button: "middle" });
    await page.mouse.move(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2 - 130, { steps: 8 });
    await page.mouse.up({ button: "middle" });
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeMiddlePedestal);
    await expect.poll(() => canvas.evaluate((node) => node.dataset.mapCameraGesture || "")).toBe("middle-drag-pedestal");
  });

  test("desktop route tools create, undo, and clear ephemeral measurements", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop route workflow uses right-click");
    await openMap(page);
    const box = await canvasBox(page);

    await page.mouse.click(box.x + box.width / 2 + 170, box.y + box.height / 2 + 40, { button: "right" });
    const contextMenu = page.locator(".map-context-menu");
    await expect(contextMenu).toBeVisible();
    await expect(contextMenu).not.toContainText(/Route Tool/i);
    await expect(contextMenu.getByRole("button", { name: /^Select$/i })).toBeVisible();
    await expect(contextMenu.getByRole("button", { name: /^Explore$/i })).toBeVisible();
    await expect(contextMenu.getByRole("button", { name: /^Measure$/i })).toBeVisible();
    await expect(contextMenu.getByRole("button", { name: /^Neighbors$/i })).toBeVisible();
    await contextMenu.getByRole("button", { name: /^Neighbors$/i }).click();
    const neighborPanel = page.locator("[data-testid='map-neighbor-tool-panel']");
    await expect(neighborPanel).toBeVisible();
    await expect(neighborPanel.getByText(/neighbors/i).first()).toBeVisible();
    await expect(neighborPanel.locator("input[type='range']")).toBeVisible();
    await expect(neighborPanel.getByRole("button", { name: /copy list/i })).toBeVisible();
    await neighborPanel.getByRole("button", { name: /close/i }).click();
    await expect(neighborPanel).toHaveCount(0);

    await page.mouse.click(box.x + box.width / 2 + 170, box.y + box.height / 2 + 40, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await contextMenu.getByRole("button", { name: /^Measure$/i }).click();

    await expect(page.locator(".map-route-summary")).toContainText(/1 legs/i);
    await expect(page.locator(".map-route-summary")).toContainText(/total/i);
    await expect(page.locator(".map-route-leg-list li")).toHaveCount(1);
    await expect(page.locator("[data-testid='map-system-drill']")).toHaveCount(0);

    await page.getByRole("button", { name: /undo/i }).click();
    await expect(page.locator(".map-route-summary")).toHaveCount(0);

    await page.mouse.click(box.x + box.width / 2 + 170, box.y + box.height / 2 + 40, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await page.mouse.click(box.x + 24, box.y + 24, { button: "right" });
    await expect(contextMenu).toHaveCount(0);

    await page.locator("[data-testid='map-minimal-toggle']").click();
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "true");
    await page.mouse.click(box.x + box.width / 2 + 170, box.y + box.height / 2 + 40, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await page.mouse.click(box.x + 24, box.y + 24, { button: "right" });
    await expect(contextMenu).toHaveCount(0);
    await page.keyboard.press("Escape");
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "false");

    await page.mouse.click(box.x + box.width / 2 - 120, box.y + box.height / 2 - 80, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await contextMenu.getByRole("button", { name: /^Measure$/i }).click();
    await expect(page.locator(".map-route-summary")).toBeVisible();
    await page.mouse.click(box.x + box.width / 2 + 120, box.y + box.height / 2 + 80, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await contextMenu.getByRole("button", { name: /^Measure$/i }).click();
    await expect(page.locator(".map-route-leg-list li")).toHaveCount(2);
    await page.locator(".map-route-leg-list li").first().getByRole("button").click();
    await expect(page.locator(".map-route-summary")).toHaveCount(0);

    await page.mouse.click(box.x + box.width / 2 + 120, box.y + box.height / 2 + 80, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await contextMenu.getByRole("button", { name: /^Measure$/i }).click();
    await expect(page.locator(".map-route-summary")).toBeVisible();
    await page.getByRole("button", { name: /clear/i }).click();
    await expect(page.locator(".map-route-summary")).toHaveCount(0);
  });

  test("map selection opens System Simulation peek and explore drill-in", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop drill-in smoke uses hover/canvas layout");
    await openMap(page);
    const selectionRequests = [];
    page.on("request", (request) => {
      const url = new URL(request.url());
      if (url.pathname.startsWith("/api/v1/systems/")) selectionRequests.push(url.pathname);
    });
    await expect(page.locator(".map-contacts-panel")).toHaveCount(0);
    await openMapPeekFromRecents(page);

    const drill = page.locator("[data-testid='map-system-drill']");
    await expect(drill).toBeVisible();
    await expect(drill.locator(".map-system-drill-title .map-name-info")).toHaveCount(0);
    await expect(drill.locator(".map-system-drill-title .map-name-copy")).toHaveCount(0);
    await expect(drill).toHaveAttribute("data-drill-mode", "peek");
    await expect(drill).toContainText(/System:/i);
    await expect(drill).not.toContainText(/System Simulation Peek/i);
    await expect(drill.getByRole("button", { name: /^Close$/i })).toBeVisible();
    await expect(drill.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(drill.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect.poll(() => selectionRequests.filter((path) => path.endsWith("/simulation-scene")).length).toBe(1);
    expect(selectionRequests.filter((path) => /^\/api\/v1\/systems\/\d+$/.test(path))).toHaveLength(0);
    expect(selectionRequests.filter((path) => path === "/api/v1/systems/search")).toHaveLength(0);
    await expect(drill.locator("[data-testid='system-preview-scale-mode']")).toBeVisible();
    await expect(drill.locator(".system-preview-speed select")).toBeVisible();
    await expect(drill.locator(".system-preview-speed select option[value='1000']")).toHaveCount(1);
    await expect(drill.locator(".map-title-stellar-classes .stellar-class-chip").first()).toBeVisible();
    await expect(drill.locator(".map-snapshot-chip")).toHaveCount(0);
    await page.locator("[data-testid='map-minimal-toggle']").click();
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "true");
    await expect(drill).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator(".map-page")).toHaveAttribute("data-map-minimal-mode", "false");
    await expect(drill).toBeVisible();
    const resizeHandle = drill.locator(".map-system-drill-resize");
    await expect(resizeHandle).toBeVisible();
    const titleBox = await drill.locator(".map-system-drill-title").boundingBox();
    const beforeResize = await drill.boundingBox();
    const handleBox = await resizeHandle.boundingBox();
    expect(beforeResize, "peek bounds before resize").toBeTruthy();
    expect(handleBox, "peek resize handle bounds").toBeTruthy();
    expect(titleBox, "peek title bounds").toBeTruthy();
    expect(handleBox.x + handleBox.width).toBeLessThanOrEqual(titleBox.x);
    await page.mouse.move(handleBox.x + handleBox.width / 2, handleBox.y + handleBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(handleBox.x - 80, handleBox.y - 60, { steps: 8 });
    await page.mouse.up();
    await expect.poll(
      () => drill.boundingBox().then((box) => Math.round(box?.width || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(Math.round(beforeResize.width) + 30);
    await expect.poll(
      () => drill.boundingBox().then((box) => Math.round(box?.height || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(Math.round(beforeResize.height) + 20);
    const storedPeekSize = await page.evaluate(() => window.sessionStorage.getItem("spacegate.map.peekSize") || "");
    expect(storedPeekSize).toContain("width");
    await expect(page.locator(".map-contacts-panel")).toHaveCount(0);
    await expect(page.getByText("Next Nearby")).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("peek");
    const mapBox = await canvasBox(page);
    await page.mouse.move(mapBox.x + 28, mapBox.y + 28);
    await page.mouse.wheel(0, 260);
    await expect(drill).toBeVisible();
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("peek");
    await page.mouse.click(mapBox.x + 28, mapBox.y + 28, { button: "right" });
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");

    await openMapPeekFromRecents(page);
    await expect(drill).toBeVisible();
    await drill.locator(".map-system-drill-title").click();
    await expect(drill).toHaveAttribute("data-drill-mode", "explore");
    await expect(drill).toContainText(/System:/i);
    await expect(drill.locator(".map-snapshot-chip")).toHaveCount(0);
    const exploreObjectList = drill.locator("[data-testid='system-preview-object-list']");
    await expect(exploreObjectList).toBeVisible();
    const vitalPills = drill.locator(".map-system-vital-pill");
    await expect(vitalPills).toHaveCount(5);
    const expectedVitalCopy = [
      /stellar parallax.*light-years.*parsecs.*kilometers/is,
      /bound system.*source catalogs/is,
      /known planets.*(dimming|Doppler|Imaging|reflected light|detection details|no canonical planets|cataloged observational)/is,
      /presentation metadata.*Luminosity.*Exotic stars/is,
      /rank 1 is highest.*versioned scoring weights/is,
    ];
    for (let index = 0; index < expectedVitalCopy.length; index += 1) {
      await vitalPills.nth(index).hover();
      await expect(vitalPills.nth(index).locator("[role='tooltip']")).toBeVisible();
      await expect(vitalPills.nth(index).locator("[role='tooltip']")).toContainText(expectedVitalCopy[index]);
    }
    await expect(vitalPills.nth(3).locator("[role='tooltip']")).toContainText(
      /(?:Luminosity|Proper motion|Multiplicity|Nice planets|Weird planets|Proximity|System complexity|Exotic stars): (?!0\.00)\d+\.\d{2} pts/
    );
    await vitalPills.first().focus();
    await expect(vitalPills.first().locator("[role='tooltip']")).toBeVisible();

    const firstExploreObject = exploreObjectList.locator(".system-preview-object-chip").first();
    await firstExploreObject.click();
    const explorePinnedReadout = drill.locator("[data-testid='system-preview-pinned']");
    await expect(explorePinnedReadout).toBeVisible();
    const pinnedPlacement = await drill.evaluate((node) => {
      const drillRect = node.getBoundingClientRect();
      const pinned = node.querySelector("[data-testid='system-preview-pinned']");
      const hierarchy = node.querySelector(".system-preview-readout");
      const pinnedRect = pinned?.getBoundingClientRect();
      return {
        rightGap: pinnedRect ? drillRect.right - pinnedRect.right : -1,
        bottomGap: pinnedRect ? drillRect.bottom - pinnedRect.bottom : -1,
        pinnedZ: Number(pinned ? window.getComputedStyle(pinned).zIndex : 0),
        hierarchyZ: Number(hierarchy ? window.getComputedStyle(hierarchy).zIndex : 0),
      };
    });
    expect(pinnedPlacement.rightGap).toBeGreaterThanOrEqual(8);
    expect(pinnedPlacement.rightGap).toBeLessThanOrEqual(20);
    expect(pinnedPlacement.bottomGap).toBeGreaterThanOrEqual(52);
    expect(pinnedPlacement.bottomGap).toBeLessThanOrEqual(68);
    expect(pinnedPlacement.pinnedZ).toBeGreaterThan(pinnedPlacement.hierarchyZ);
    await explorePinnedReadout.getByRole("button", { name: /close pinned simulator readout/i }).click();
    await expect.poll(
      () => drill.evaluate((node) => {
        const heights = Array.from(
          node.querySelectorAll("[data-testid='system-preview-object-list'] .system-preview-object-chip")
        ).map((pill) => pill.getBoundingClientRect().height);
        return heights.length ? Math.max(...heights) : 0;
      }),
      { timeout: 3000 }
    ).toBeLessThanOrEqual(44);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("explore");
    await expect(drill.getByRole("button", { name: /^Back$/i })).toBeVisible();
    await expect(drill.getByRole("button", { name: "×" })).toBeVisible();

    const fullButton = page.locator(".map-fullscreen-command");
    await expect(fullButton).toBeVisible();
    await fullButton.click();
    await expect(fullButton).toHaveText(/Exit/i);
    await page.keyboard.press("Escape");
    await expect(drill).toBeVisible();
    await expect(drill.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(drill.locator(".system-preview-toggle").last()).toHaveText(/^Reset$/i);
    if (await page.evaluate(() => Boolean(document.fullscreenElement))) {
      await fullButton.click();
    }
    await expect(fullButton).toHaveText(/Full/i);
    await expect(drill).toBeVisible();
    await expect(drill.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(drill.locator(".system-preview-toggle").last()).toHaveText(/^Reset$/i);

    const exploreBox = await drill.boundingBox();
    expect(exploreBox, "explore bounds").toBeTruthy();
    await page.mouse.move(exploreBox.x + exploreBox.width * 0.42, exploreBox.y + exploreBox.height * 0.5);
    await page.mouse.down({ button: "right" });
    await page.mouse.move(exploreBox.x + exploreBox.width * 0.42 + 120, exploreBox.y + exploreBox.height * 0.5, { steps: 8 });
    await page.mouse.up({ button: "right" });
    await expect(drill).toBeVisible();
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("explore");

    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    await expect(menu.locator(".map-header-menu-panel")).toBeVisible();
    const menuLayer = await page.evaluate(() => {
      const panel = document.querySelector(".map-header-menu-panel");
      const drillNode = document.querySelector("[data-testid='map-system-drill']");
      return {
        panelZ: Number(window.getComputedStyle(panel).zIndex),
        drillZ: Number(window.getComputedStyle(drillNode).zIndex),
      };
    });
    expect(menuLayer.panelZ).toBeGreaterThan(menuLayer.drillZ);

    await page.evaluate(() => window.history.back());
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");

    await openMapPeekFromRecents(page);
    await expect(drill).toBeVisible();
    await drill.getByRole("button", { name: /^Explore$/i }).click();
    await expect(drill).toHaveAttribute("data-drill-mode", "explore");
    await drill.getByRole("button", { name: /^Back$/i }).click();
    await expect(drill).toHaveAttribute("data-drill-mode", "peek");
    await drill.getByRole("button", { name: /^Close$/i }).click();
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");
  });

  test("map embedded simulator menus remain clickable across transparent themes", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop menu click regression uses native select controls");
    await openMap(page);
    await openMapPeekFromRecents(page);

    const drill = page.locator("[data-testid='map-system-drill']");
    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    const themeSelect = menu.locator(".map-theme-select select");
    const scaleSelect = drill.locator("[data-testid='system-preview-scale-mode']");
    const speedSelect = drill.locator(".system-preview-speed select");
    const canvas = drill.locator(".system-preview-canvas canvas");

    await expect(drill).toBeVisible();
    await expect(scaleSelect).toBeVisible();
    await expect(speedSelect).toBeVisible();

    for (const themeId of ["aurora", "lcars", "cyberpunk", "retro_90s"]) {
      if (!(await themeSelect.isVisible())) {
        await menu.locator("summary").click();
        await expect(menu.locator(".map-header-menu-panel")).toBeVisible();
      }
      await themeSelect.selectOption(themeId);
      await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe(themeId);
      await scaleSelect.click();
      await scaleSelect.selectOption("log");
      await expect.poll(
        () => canvas.evaluate((node) => node.dataset.scaleMode || ""),
        { timeout: 3000 }
      ).toBe("log");
      await speedSelect.click();
      await speedSelect.selectOption("1000");
      await expect.poll(
        () => canvas.evaluate((node) => node.dataset.simulationSpeed || ""),
        { timeout: 3000 }
      ).toBe("1000");
      await scaleSelect.selectOption("structure");
      await speedSelect.selectOption("1");
    }
  });

  test("geocities map theme uses 90s web chrome", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop theme chrome check");
    await openMap(page);
    await openMapPeekFromRecents(page);

    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    await menu.locator(".map-theme-select select").selectOption("retro_90s");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("retro_90s");

    const themeStyles = await page.evaluate(() => {
      const header = document.querySelector(".map-hud-top");
      const drill = document.querySelector("[data-testid='map-system-drill']");
      const title = document.querySelector(".map-title-block h1");
      const drillTitleGroup = document.querySelector(".map-system-drill-title-group");
      const drillActions = document.querySelector(".map-system-drill-actions");
      const headerStyle = window.getComputedStyle(header);
      const headerTitleStyle = window.getComputedStyle(header, "::before");
      const drillTitleStyle = window.getComputedStyle(drill, "::before");
      const titleStyle = window.getComputedStyle(title);
      const titleRect = drillTitleGroup.getBoundingClientRect();
      const actionsRect = drillActions.getBoundingClientRect();
      return {
        headerBackground: headerStyle.backgroundColor,
        headerBorderTop: headerStyle.borderTopColor,
        headerBorderBottom: headerStyle.borderBottomColor,
        headerTitleContent: headerTitleStyle.content,
        drillTitleContent: drillTitleStyle.content,
        titleColor: titleStyle.color,
        contactsRemoved: document.querySelector(".map-contacts-panel") === null,
        drillHeaderOverlap: titleRect.right > actionsRect.left && titleRect.left < actionsRect.right
          && titleRect.bottom > actionsRect.top && titleRect.top < actionsRect.bottom,
      };
    });
    expect(themeStyles.headerBackground).toBe("rgb(192, 192, 192)");
    expect(themeStyles.headerBorderTop).toBe("rgb(255, 255, 255)");
    expect(themeStyles.headerBorderBottom).toBe("rgb(64, 64, 64)");
    expect(themeStyles.headerTitleContent).toContain("COOLSTARS.EXE");
    expect(themeStyles.drillTitleContent).toContain("SYSTEM_SIM.EXE");
    expect(themeStyles.titleColor).toBe("rgb(255, 255, 0)");
    expect(themeStyles.contactsRemoved).toBe(true);
    expect(themeStyles.drillHeaderOverlap).toBe(false);
  });

  test("enterprise map theme uses LCARS block chrome", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop theme chrome check");
    await openMap(page);
    await openMapPeekFromRecents(page);

    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    await menu.locator(".map-theme-select select").selectOption("lcars");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("lcars");
    const drill = page.locator("[data-testid='map-system-drill']");
    const previewSurface = drill.locator(".system-preview-canvas");
    await expect(previewSurface).toBeVisible();
    await expect.poll(
      () => previewSurface.evaluate((node) => node.getBoundingClientRect().height),
      { timeout: 12000 }
    ).toBeGreaterThan(120);

    const themeStyles = await page.evaluate(() => {
      const header = document.querySelector(".map-hud-top");
      const headerRail = window.getComputedStyle(header, "::before");
      const menuPanel = document.querySelector(".map-header-menu-panel");
      const title = document.querySelector(".map-title-block h1");
      const headerStats = Array.from(document.querySelectorAll(".map-header-readout span"));
      const actionItems = [
        ...Array.from(document.querySelectorAll(".map-command-row > .map-hud-button")).filter((item) => window.getComputedStyle(item).display !== "none"),
        document.querySelector(".map-command-row > .map-header-menu > .map-menu-button"),
      ].filter(Boolean);
      const button = document.querySelector(".map-hud-button");
      const drill = document.querySelector("[data-testid='map-system-drill']");
      const drillRail = window.getComputedStyle(drill, "::before");
      const drillBar = document.querySelector(".map-system-drill-bar");
      const drillTitle = document.querySelector(".map-system-drill-title");
      const previewCanvas = document.querySelector("[data-testid='map-system-drill'] .system-preview-canvas");
      const vitalItems = Array.from(document.querySelectorAll(".map-system-vital-strip > span"));
      const headerStyle = window.getComputedStyle(header);
      const menuPanelStyle = window.getComputedStyle(menuPanel);
      const titleStyle = window.getComputedStyle(title);
      const buttonStyle = window.getComputedStyle(button);
      const drillStyle = window.getComputedStyle(drill);
      const drillBarStyle = window.getComputedStyle(drillBar);
      const drillTitleStyle = window.getComputedStyle(drillTitle);
      const firstVitalStyle = window.getComputedStyle(vitalItems[0]);
      const secondVitalStyle = window.getComputedStyle(vitalItems[1]);
      const lastVitalStyle = window.getComputedStyle(vitalItems[vitalItems.length - 1]);
      const headerRect = header.getBoundingClientRect();
      const menuPanelRect = menuPanel.getBoundingClientRect();
      const drillRect = drill.getBoundingClientRect();
      const previewCanvasRect = previewCanvas.getBoundingClientRect();
      const firstVitalRect = vitalItems[0].getBoundingClientRect();
      const secondVitalRect = vitalItems[1].getBoundingClientRect();
      const firstStatStyle = window.getComputedStyle(headerStats[0]);
      const secondStatStyle = window.getComputedStyle(headerStats[1]);
      const lastStatStyle = window.getComputedStyle(headerStats[headerStats.length - 1]);
      const firstStatRect = headerStats[0].getBoundingClientRect();
      const secondStatRect = headerStats[1].getBoundingClientRect();
      const firstActionStyle = window.getComputedStyle(actionItems[0]);
      const secondActionStyle = window.getComputedStyle(actionItems[1]);
      const lastActionStyle = window.getComputedStyle(actionItems[actionItems.length - 1]);
      const firstActionRect = actionItems[0].getBoundingClientRect();
      const secondActionRect = actionItems[1].getBoundingClientRect();
      return {
        headerBackground: headerStyle.backgroundColor,
        headerBorderTop: headerStyle.borderTopColor,
        headerRadius: headerStyle.borderTopLeftRadius,
        headerRailBackground: headerRail.backgroundColor,
        menuPanelTop: menuPanelRect.top,
        menuPanelZIndex: menuPanelStyle.zIndex,
        headerBottom: headerRect.bottom,
        titleColor: titleStyle.color,
        titleLetterSpacing: titleStyle.letterSpacing,
        buttonBackground: buttonStyle.backgroundColor,
        buttonColor: buttonStyle.color,
        drillBackground: drillStyle.backgroundColor,
        drillRailBackground: drillRail.backgroundColor,
        drillBarPosition: drillBarStyle.position,
        drillTitleBackground: drillTitleStyle.backgroundColor,
        drillTitleColor: drillTitleStyle.color,
        previewCanvasHeightRatio: previewCanvasRect.height / Math.max(1, drillRect.height),
        firstVitalLeftRadius: firstVitalStyle.borderTopLeftRadius,
        secondVitalLeftRadius: secondVitalStyle.borderTopLeftRadius,
        lastVitalRightRadius: lastVitalStyle.borderTopRightRadius,
        vitalGap: Math.round(secondVitalRect.left - firstVitalRect.right),
        firstStatLeftRadius: firstStatStyle.borderTopLeftRadius,
        secondStatLeftRadius: secondStatStyle.borderTopLeftRadius,
        lastStatRightRadius: lastStatStyle.borderTopRightRadius,
        statGap: Math.round(secondStatRect.left - firstStatRect.right),
        firstActionLeftRadius: firstActionStyle.borderTopLeftRadius,
        secondActionLeftRadius: secondActionStyle.borderTopLeftRadius,
        lastActionRightRadius: lastActionStyle.borderTopRightRadius,
        actionGap: Math.round(secondActionRect.left - firstActionRect.right),
        contactsRemoved: document.querySelector(".map-contacts-panel") === null,
      };
    });
    expect(themeStyles.headerBackground).toBe("rgb(0, 0, 0)");
    expect(themeStyles.drillBackground).toBe("rgb(0, 0, 0)");
    expect(themeStyles.headerBorderTop).toBe("rgb(255, 212, 0)");
    expect(themeStyles.headerRadius).toBe("32px");
    expect(themeStyles.headerRailBackground).toBe("rgb(245, 162, 46)");
    expect(themeStyles.menuPanelTop).toBeGreaterThanOrEqual(themeStyles.headerBottom - 1);
    expect(Number(themeStyles.menuPanelZIndex)).toBeGreaterThan(40);
    expect(themeStyles.drillRailBackground).toBe("rgb(245, 162, 46)");
    expect(themeStyles.titleColor).toBe("rgb(245, 162, 46)");
    expect(themeStyles.titleLetterSpacing).not.toBe("normal");
    expect(themeStyles.buttonBackground).toBe("rgb(145, 160, 255)");
    expect(themeStyles.buttonColor).toBe("rgb(20, 15, 27)");
    expect(themeStyles.drillBarPosition).toBe("absolute");
    expect(themeStyles.drillTitleBackground).toBe("rgb(246, 201, 76)");
    expect(themeStyles.drillTitleColor).toBe("rgb(20, 15, 27)");
    expect(themeStyles.previewCanvasHeightRatio).toBeGreaterThan(0.82);
    expect(themeStyles.firstVitalLeftRadius).not.toBe("0px");
    expect(themeStyles.secondVitalLeftRadius).toBe("0px");
    expect(themeStyles.lastVitalRightRadius).not.toBe("0px");
    expect(themeStyles.vitalGap).toBeLessThanOrEqual(0);
    expect(themeStyles.firstStatLeftRadius).not.toBe("0px");
    expect(themeStyles.secondStatLeftRadius).toBe("0px");
    expect(themeStyles.lastStatRightRadius).not.toBe("0px");
    expect(themeStyles.statGap).toBeLessThanOrEqual(0);
    expect(themeStyles.firstActionLeftRadius).not.toBe("0px");
    expect(themeStyles.secondActionLeftRadius).toBe("0px");
    expect(themeStyles.lastActionRightRadius).not.toBe("0px");
    expect(themeStyles.actionGap).toBeLessThanOrEqual(0);
    expect(themeStyles.contactsRemoved).toBe(true);
  });

  test("mission control map theme uses Apollo console chrome", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop theme chrome check");
    await openMap(page);
    await openMapPeekFromRecents(page);

    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    await menu.locator(".map-theme-select select").selectOption("mission_control");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("mission_control");
    await expect(page.locator("[data-testid='map-system-drill'] .system-preview-canvas")).toBeVisible();

    const themeStyles = await page.evaluate(() => {
      const header = document.querySelector(".map-hud-top");
      const headerStrip = window.getComputedStyle(header, "::before");
      const title = document.querySelector(".map-title-block h1");
      const primaryButton = document.querySelector(".map-hud-button.primary");
      const utilityLinks = Array.from(document.querySelectorAll(".map-text-link")).map((link) => {
        const rect = link.getBoundingClientRect();
        return {
          text: link.textContent.trim(),
          visible: rect.width > 0 && rect.height > 0,
          pointerEvents: window.getComputedStyle(link).pointerEvents,
        };
      });
      const readout = document.querySelector(".map-header-readout span");
      const drill = document.querySelector("[data-testid='map-system-drill']");
      const previewCanvas = document.querySelector("[data-testid='map-system-drill'] .system-preview-canvas");
      const headerStyle = window.getComputedStyle(header);
      const titleStyle = window.getComputedStyle(title);
      const buttonStyle = window.getComputedStyle(primaryButton);
      const readoutStyle = window.getComputedStyle(readout);
      const drillStyle = window.getComputedStyle(drill);
      const previewStyle = window.getComputedStyle(previewCanvas);
      const canvas = document.querySelector(".map-canvas canvas");
      return {
        headerStripContent: headerStrip.content,
        headerStripColor: headerStrip.color,
        headerBackground: headerStyle.backgroundImage,
        headerRadius: headerStyle.borderTopLeftRadius,
        titleTransform: titleStyle.textTransform,
        titleLetterSpacing: titleStyle.letterSpacing,
        utilityLinks,
        buttonColor: buttonStyle.color,
        buttonBackground: buttonStyle.backgroundImage,
        buttonShadow: buttonStyle.boxShadow,
        readoutColor: readoutStyle.color,
        readoutBackground: readoutStyle.backgroundImage,
        drillRadius: drillStyle.borderTopLeftRadius,
        previewBackground: previewStyle.backgroundImage,
        contactsRemoved: document.querySelector(".map-contacts-panel") === null,
        labelStrategy: canvas?.dataset.mapLabelStrategy,
        localLabelCount: Number(canvas?.dataset.mapLocalLabelCount || 0),
      };
    });
    expect(themeStyles.headerStripContent).toContain("MOCR 2");
    expect(themeStyles.headerStripContent).toContain("EECOM");
    expect(themeStyles.headerStripColor).toBe("rgb(185, 246, 170)");
    expect(themeStyles.headerBackground).toContain("83, 98, 79");
    expect(themeStyles.headerRadius).toBe("3px");
    expect(themeStyles.titleTransform).toBe("uppercase");
    expect(themeStyles.titleLetterSpacing).not.toBe("normal");
    expect(themeStyles.utilityLinks.map((link) => link.text)).toEqual(["HELP", "DATA"]);
    expect(themeStyles.utilityLinks.every((link) => link.visible && link.pointerEvents === "auto")).toBeTruthy();
    expect(themeStyles.buttonColor).toBe("rgb(24, 26, 18)");
    expect(themeStyles.buttonBackground).toContain("rgb(255, 225, 147)");
    expect(themeStyles.buttonShadow).toContain("inset");
    expect(themeStyles.readoutColor).toBe("rgb(185, 246, 170)");
    expect(themeStyles.readoutBackground).toContain("repeating-linear-gradient");
    expect(themeStyles.drillRadius).toBe("3px");
    expect(themeStyles.previewBackground).toContain("repeating-linear-gradient");
    expect(themeStyles.contactsRemoved).toBe(true);
    expect(themeStyles.labelStrategy).toBe("camera_near_10ly_nearest_plus_coolness");
    expect(themeStyles.localLabelCount).toBeGreaterThan(0);
  });

  test("cyberpunk map theme uses neon explorer chrome", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop theme chrome check");
    await openMap(page);
    await openMapPeekFromRecents(page);

    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    await menu.locator(".map-theme-select select").selectOption("cyberpunk");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("cyberpunk");

    const themeStyles = await page.evaluate(() => {
      const header = document.querySelector(".map-hud-top");
      const drill = document.querySelector("[data-testid='map-system-drill']");
      const title = document.querySelector(".map-title-block h1");
      const drillTitleGroup = document.querySelector(".map-system-drill-title-group");
      const drillActions = document.querySelector(".map-system-drill-actions");
      const headerStyle = window.getComputedStyle(header);
      const headerRuleStyle = window.getComputedStyle(header, "::before");
      const drillStyle = window.getComputedStyle(drill);
      const titleStyle = window.getComputedStyle(title);
      const titleRect = drillTitleGroup.getBoundingClientRect();
      const actionsRect = drillActions.getBoundingClientRect();
      return {
        headerBorderTop: headerStyle.borderTopColor,
        headerShadow: headerStyle.boxShadow,
        headerRuleBackground: headerRuleStyle.backgroundImage,
        drillBackground: drillStyle.backgroundImage,
        titleColor: titleStyle.color,
        titleFont: titleStyle.fontFamily,
        titleShadow: titleStyle.textShadow,
        titleLetterSpacing: titleStyle.letterSpacing,
        drillHeaderOverlap: titleRect.right > actionsRect.left && titleRect.left < actionsRect.right
          && titleRect.bottom > actionsRect.top && titleRect.top < actionsRect.bottom,
      };
    });
    expect(themeStyles.headerBorderTop).toBe("rgba(0, 245, 255, 0.72)");
    expect(themeStyles.headerShadow).toContain("255, 0, 204");
    expect(themeStyles.headerRuleBackground).toContain("0, 245, 255");
    expect(themeStyles.drillBackground).toContain("255, 0, 204");
    expect(themeStyles.titleColor).toBe("rgb(57, 255, 20)");
    expect(themeStyles.titleFont).toMatch(/Orbitron|Audiowide|Michroma|Bank Gothic|Antonio/);
    expect(themeStyles.titleLetterSpacing).not.toBe("normal");
    expect(themeStyles.titleShadow).toContain("0, 245, 255");
    expect(themeStyles.drillHeaderOverlap).toBe(false);
  });

  test("mobile layout keeps map controls compact", async ({ page }, testInfo) => {
    test.skip(!testInfo.project.name.includes("mobile"), "mobile-only layout check");
    await openMap(page);
    await expect(page.locator(".map-fullscreen-command")).toBeVisible();
    await expect(page.locator(".map-contacts-panel")).toHaveCount(0);
    await expect(page.locator(".map-mobile-flight-button")).toHaveCount(6);
    await expect(page.locator("[data-testid='map-mobile-flight-forward']")).toBeVisible();
    const searchToggle = page.locator("[data-testid='map-search-toggle']");
    await expect(searchToggle).toHaveAttribute("aria-pressed", "false");
    await searchToggle.tap();
    await expect(page.locator(".map-star-search")).toBeVisible();
    await expect(page.locator(".map-search-topbar")).toBeVisible();
    await expect(page.locator(".map-search-sidebar")).toBeVisible();
    await expect(searchToggle).toHaveAttribute("aria-pressed", "true");
    await searchToggle.tap();
    await expect(page.locator(".map-star-search")).toBeHidden();
    await expect(searchToggle).toHaveAttribute("aria-pressed", "false");
    await searchToggle.tap();
    await expect(page.locator(".map-star-search")).toBeVisible();

    const metrics = await page.evaluate(() => {
      const canvas = document.querySelector(".map-canvas canvas")?.getBoundingClientRect();
      const header = document.querySelector(".map-site-header")?.getBoundingClientRect();
      const search = document.querySelector(".map-star-search")?.getBoundingClientRect();
      const topbar = document.querySelector(".map-search-topbar")?.getBoundingClientRect();
      const sidebar = document.querySelector(".map-search-sidebar")?.getBoundingClientRect();
      const flightPad = document.querySelector(".map-mobile-flight-pad")?.getBoundingClientRect();
      const overlap = (a, b) => Boolean(
        a && b && a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top
      );
      return {
        canvasWidth: canvas?.width || 0,
        canvasHeight: canvas?.height || 0,
        headerHeight: header?.height || 0,
        headerBottom: header?.bottom || 0,
        searchTop: search?.top || 0,
        topbarTop: topbar?.top || 0,
        sidebarWidth: sidebar?.width || 0,
        sidebarFlightOverlap: overlap(sidebar, flightPad),
      };
    });
    expect(metrics.canvasWidth).toBeGreaterThan(100);
    expect(metrics.canvasHeight).toBeGreaterThan(100);
    expect(metrics.headerHeight).toBeLessThanOrEqual(88);
    expect(metrics.searchTop).toBeGreaterThanOrEqual(metrics.headerBottom);
    expect(metrics.searchTop).toBeLessThanOrEqual(76);
    expect(metrics.topbarTop).toBeLessThanOrEqual(78);
    expect(metrics.sidebarWidth).toBeLessThanOrEqual(186);
    expect(metrics.sidebarFlightOverlap).toBe(false);

    const canvas = page.locator(".map-canvas canvas");
    const beforeMove = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.locator("[data-testid='map-mobile-flight-forward']").evaluate((node) => {
      node.dispatchEvent(new PointerEvent("pointerdown", {
        bubbles: true,
        cancelable: true,
        pointerId: 901,
        pointerType: "touch",
        isPrimary: true,
      }));
    });
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapMobileFlightActive || ""),
      { timeout: 3000 }
    ).toBe("true");
    await page.waitForTimeout(450);
    await page.locator("[data-testid='map-mobile-flight-forward']").evaluate((node) => {
      node.dispatchEvent(new PointerEvent("pointerup", {
        bubbles: true,
        cancelable: true,
        pointerId: 901,
        pointerType: "touch",
        isPrimary: true,
      }));
    });
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""),
      { timeout: 3000 }
    ).not.toBe(beforeMove);

    await page.getByRole("button", { name: /select reticle/i }).tap();
    const drill = page.locator("[data-testid='map-system-drill']");
    await expect(drill).toBeVisible();
    await expect(drill).toHaveAttribute("data-drill-mode", "peek");
    await drill.getByRole("button", { name: /^Close$/i }).tap();
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");
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
    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    const firstPlanetClass = scenePayload.render_scene?.bodies?.planets?.[0]?.fields?.planet_visual_class;
    expect(firstPlanetClass?.layer).toBe("render_scene");
    expect(firstPlanetClass?.status).toMatch(/derived|assumed/);
    expect(firstPlanetClass?.generator_version).toBe("system_preview_planet_visual_class_v1");

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    const sharedClockCanvas = page.locator(".system-preview-canvas canvas");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.simulationClockMode || ""),
      { timeout: 3000 }
    ).toBe("shared_local_beta");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.simulationClockWriters || ""),
      { timeout: 3000 }
    ).toBe("1");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.simulationDays || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(0);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableStarCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.inspectablePlanetCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableOrbitCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.orbitTraceProvenanceCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.orbitTraceProvenanceVersion || ""),
      { timeout: 3000 }
    ).toBe("system_preview_orbit_trace_v1");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.raycasterLineThreshold || ""),
      { timeout: 3000 }
    ).toBe("0.12");
    await sharedClockCanvas.evaluate((canvas) => {
      canvas.dispatchEvent(new Event("webglcontextlost", { cancelable: true }));
    });
    await expect(page.locator("[data-testid='system-preview-context-recovery']")).toBeVisible();
    await expect(page.locator("[data-testid='system-preview-snapshot-fallback']")).toHaveCount(0);
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible({ timeout: 5000 });
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("planet");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("orbit");
    await expect(page.locator(".system-preview-readout")).toContainText(/render policy/i);
    await expect(page.locator(".system-preview-readout")).not.toContainText(/local days/i);
    await expect(page.locator(".system-preview-readout")).not.toContainText(/missing inputs/i);
    const lineMenu = page.locator(".system-preview-line-menu");
    await expect(lineMenu).toBeVisible();
    await expect(lineMenu.locator("summary")).toContainText(/Lines/i);
    await lineMenu.locator("summary").click();
    await expect(page.locator(".system-preview-toggle", { hasText: "HZ On" })).toHaveAttribute("aria-pressed", "true");
    await expect(page.locator(".system-preview-toggle", { hasText: "Vapor Off" })).toHaveAttribute("aria-pressed", "false");
    await expect(page.locator(".system-preview-toggle", { hasText: "Snow Off" })).toHaveAttribute("title", /Water Freeze Line.*deg F/);
    await page.locator(".system-preview-toggle", { hasText: "Snow Off" }).click();
    await expect(page.locator(".system-preview-toggle", { hasText: "Snow On" })).toHaveAttribute("aria-pressed", "true");
    await page.locator(".system-preview-toggle", { hasText: "HZ On" }).click();
    await expect(page.locator(".system-preview-toggle", { hasText: "HZ Off" })).toHaveAttribute("aria-pressed", "false");
    await page.locator(".system-preview-toggle", { hasText: "Reset" }).click();
    await expect(page.locator(".system-preview-toggle", { hasText: "HZ On" })).toHaveAttribute("aria-pressed", "true");
    await expect(page.locator(".system-preview-toggle", { hasText: "Snow Off" })).toHaveAttribute("aria-pressed", "false");
    const scaleModeSelect = page.locator("[data-testid='system-preview-scale-mode']");
    await expect(scaleModeSelect).toBeVisible();
    await expect(scaleModeSelect).toHaveValue("structure");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.scaleMode || ""),
      { timeout: 3000 }
    ).toBe("structure");
    await scaleModeSelect.selectOption("true_orbits");
    await expect(scaleModeSelect).toHaveValue("true_orbits");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.scaleMode || ""),
      { timeout: 3000 }
    ).toBe("true_orbits");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.trueOrbitScaleSampleCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.trueOrbitScaleMaxRelativeError || 1)),
      { timeout: 3000 }
    ).toBeLessThan(0.001);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.trueOrbitMaxBodyToMinOrbitRatio || 1)),
      { timeout: 3000 }
    ).toBeLessThan(0.45);
    await scaleModeSelect.selectOption("true_bodies");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.scaleMode || ""),
      { timeout: 3000 }
    ).toBe("true_bodies");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.planetTrailCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await scaleModeSelect.selectOption("structure");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZoneCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZoneMaxPlaneInclinationDeg || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(80);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.sceneLabelCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(8);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassLabelCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.sceneLabelRenderer || ""),
      { timeout: 3000 }
    ).toBe("troika_sdf_text_v1");
    await page.getByRole("button", { name: /Labels On/i }).click();
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.sceneLabelCount || 0)),
      { timeout: 3000 }
    ).toBe(0);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.sceneLabelRenderer || ""),
      { timeout: 3000 }
    ).toBe("none");
    await page.getByRole("button", { name: /Labels Off/i }).click();
    await expect(page.locator(".system-preview-evidence")).toContainText(/SOURCE/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/ASSUMED/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/Planet class/i);
    const renderPolicy = page.locator("[data-testid='system-preview-policy']");
    await expect(renderPolicy).toBeVisible();
    await expect(renderPolicy).toContainText(/render policy/i);
    await expect(renderPolicy).toContainText(/Local beta day/i);
    await expect(renderPolicy).toContainText(/Structured Scale/i);
    await expect(renderPolicy).toContainText(/persisted|No assumptions/i);
    await expect(renderPolicy).toContainText(/Live 3d|Live 3D/i);
    await expect(renderPolicy).toContainText(/Deterministic Snapshot/i);
    const readoutEvidencePill = page.locator(".system-preview-evidence .evidence-pill").first();
    await readoutEvidencePill.focus();
    const readoutEvidencePopover = page.locator(".system-preview-evidence .evidence-popover").first();
    await expect(readoutEvidencePopover).toBeVisible();
    await expect(readoutEvidencePopover).toContainText(/Basis:/i);
    await expect(readoutEvidencePopover).toContainText(/Confidence:/i);
    await expect(readoutEvidencePopover).toContainText(/Generator:/i);
    await expect(page.getByRole("button", { name: /pause/i })).toBeVisible();
    await page.getByRole("button", { name: /pause/i }).click();
    await expect(page.getByRole("button", { name: /start/i })).toBeVisible();
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.simulationRunning || ""),
      { timeout: 1500 }
    ).toBe("false");
    const pausedSimulationDays = await sharedClockCanvas.evaluate((canvas) => canvas.dataset.simulationDays || "");
    expect(pausedSimulationDays).toBeTruthy();
    await page.waitForTimeout(500);
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.simulationDays || ""),
      { timeout: 1500 }
    ).toBe(pausedSimulationDays);
    await page.getByRole("button", { name: /start/i }).click();
    await expect(page.getByRole("button", { name: /pause/i })).toBeVisible();
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.simulationRunning || ""),
      { timeout: 1500 }
    ).toBe("true");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => Number(canvas.dataset.simulationDays || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(Number(pausedSimulationDays));
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
    await previewCanvasForView.scrollIntoViewIfNeeded();
    const dragBox = await previewCanvasForView.boundingBox();
    expect(dragBox, "system preview canvas box after reset").toBeTruthy();
    await page.mouse.move(dragBox.x + dragBox.width / 2, dragBox.y + dragBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(dragBox.x + dragBox.width / 2 + 120, dragBox.y + dragBox.height / 2 + 55, { steps: 8 });
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
    const pinnedReadout = page.locator("[data-testid='system-preview-pinned']");
    await expect(pinnedReadout).toBeVisible();
    await expect(pinnedReadout).toContainText(/star|planet|orbit/i);
    const idCopy = page.locator("[data-testid='system-preview-id-copy']");
    await expect(idCopy).toBeVisible();
    await expect(idCopy).toHaveAttribute("data-full-id", /star:gaia:/);
    const fullId = await idCopy.getAttribute("data-full-id");
    const visibleId = (await idCopy.locator("span").innerText()).trim();
    expect(fullId?.length || 0).toBeGreaterThan(visibleId.length);
    expect(visibleId).toContain("...");
    await expect(pinnedReadout.locator(".evidence-pill").first()).toBeVisible();
    await expect(pinnedReadout).toContainText(/SOURCE|DERIVED|ASSUMED|MISSING/i);
    await pinnedReadout.locator(".evidence-pill").first().focus();
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

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const fallback = page.locator("[data-testid='system-preview-snapshot-fallback']");
    await expect(fallback).toBeVisible();
    const fallbackImage = fallback.locator("img");
    if ((await fallbackImage.count()) > 0) {
      await expect(fallbackImage).toBeVisible();
    } else {
      await expect(fallback).toContainText(/Snapshot fallback pending/i);
    }
    await expect(page.locator(".system-preview-canvas canvas")).toHaveCount(0);
    await expect(fallback).toContainText(/WebGL unavailable/i);
  });

  test("system preview falls back when the live scene request fails", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "fallback smoke only needs one browser project");
    await page.addInitScript(() => {
      window.__SPACEGATE_DISABLE_SIM_SCENE_CACHE = true;
    });
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "TRAPPIST-1", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "TRAPPIST-1 system_id").toBeTruthy();

    await page.route("**/api/v1/systems/*/simulation-scene**", (route) => route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ detail: "forced simulation-scene failure" }),
    }));

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const fallback = page.locator("[data-testid='system-preview-snapshot-fallback']");
    await expect(fallback).toBeVisible();
    await expect(fallback).toContainText(/Live preview unavailable/i);
    await expect(page.locator(".system-preview-canvas canvas")).toHaveCount(0);
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

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectablePlanetCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableOrbitCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(7);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("planet");

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

    const previewFrame = page.locator(".system-preview-canvas");
    await previewFrame.scrollIntoViewIfNeeded();
    const previewBox = await previewFrame.boundingBox();
    expect(previewBox, "mobile system preview canvas box").toBeTruthy();
    const objectChip = page.locator("[data-testid='system-preview-object-list'] .system-preview-object-chip").first();
    await objectChip.scrollIntoViewIfNeeded();
    await expect(objectChip).toBeVisible();
    await objectChip.tap();
    const pinnedReadout = page.locator("[data-testid='system-preview-pinned']");
    await expect(pinnedReadout).toBeVisible();
    await expect(pinnedReadout).toContainText(/star|planet|orbit/i);
    await expect(pinnedReadout).toContainText(/SOURCE|DERIVED|ASSUMED|MISSING/i);
    await expect(pinnedReadout.locator(".evidence-pill").first()).toBeVisible();
    const idCopy = pinnedReadout.locator("[data-testid='system-preview-id-copy']");
    await expect(idCopy).toBeVisible();
    const fullId = await idCopy.getAttribute("data-full-id");
    const visibleId = (await idCopy.locator("span").innerText()).trim();
    expect(fullId?.length || 0).toBeGreaterThan(visibleId.length);
    expect(visibleId).toContain("...");
    await expect(pinnedReadout.getByRole("button", { name: /close pinned simulator readout/i })).toBeVisible();
    const pinnedBox = await pinnedReadout.boundingBox();
    expect(pinnedBox, "mobile pinned readout box").toBeTruthy();
    expect(pinnedBox.x).toBeGreaterThanOrEqual(previewBox.x - 1);
    expect(pinnedBox.x + pinnedBox.width).toBeLessThanOrEqual(previewBox.x + previewBox.width + 1);
    expect(pinnedBox.y + pinnedBox.height).toBeLessThanOrEqual(previewBox.y + previewBox.height + 1);
    expect(pinnedBox.height).toBeLessThanOrEqual(previewBox.height * 0.5);
    await pinnedReadout.getByRole("button", { name: /close pinned simulator readout/i }).click();
    await expect(pinnedReadout).toHaveCount(0);
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
    expect(scenePayload.render_scene?.bodies?.subsystems?.length).toBeGreaterThanOrEqual(3);
    const castorSubsystemNames = (scenePayload.render_scene?.bodies?.subsystems || []).map(
      (subsystem) => subsystem.display_name
    );
    expect(castorSubsystemNames).toEqual(expect.arrayContaining(["Castor A", "Castor B", "Castor C"]));
    const massPriorStars = (scenePayload.render_scene?.bodies?.stars || []).filter(
      (star) => star.fields?.visual_stellar_class?.basis === "mass_main_sequence_prior_v1"
    );
    expect(massPriorStars).toHaveLength(1);
    for (const star of massPriorStars) {
      expect(star.spectral_class || null).toBeNull();
      expect(star.fields?.visual_stellar_class?.status).toBe("assumed");
      expect(star.fields?.visual_stellar_class?.layer).toBe("render_scene");
    }
    const castorSourceTypes = Object.fromEntries(
      (scenePayload.render_scene?.bodies?.stars || []).map((star) => [
        star.display_name,
        star.fields?.spectral_type_raw,
      ])
    );
    for (const [name, spectralType] of [
      ["Castor AB", "M2-M3"],
      ["Castor BB", "M2-M3"],
      ["Castor CB", "early-M"],
    ]) {
      expect(castorSourceTypes[name]?.value).toBe(spectralType);
      expect(castorSourceTypes[name]?.status).toBe("source");
    }
    for (const subsystem of scenePayload.render_scene?.bodies?.subsystems || []) {
      expect(subsystem.fields?.component_label?.status).toMatch(/source|derived/);
      expect(subsystem.fields?.hierarchy_basis?.status).toBe("derived");
      expect(subsystem.fields?.hierarchy_basis?.layer).toBe(subsystem.fallback_subsystem ? "render_scene" : "arm");
    }
    const subsystemDiagnostics = scenePayload.render_scene?.diagnostics?.subsystem_handle_counts || {};
    const fallbackCount = (scenePayload.render_scene?.bodies?.subsystems || []).filter((subsystem) => subsystem.fallback_subsystem).length;
    expect(subsystemDiagnostics.simulation_tree_fallback || 0).toBe(fallbackCount);
    expect(scenePayload.render_scene?.orbits?.length).toBeGreaterThanOrEqual(3);

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(page.locator(".system-preview-evidence")).toContainText(/SOURCE/i);
    await expect(page.locator(".hierarchy-panel")).toContainText(/Visual prior/i);
    await expect(page.locator(".hierarchy-panel")).toContainText(/ASSUMED/i);
    await expect(page.locator(".system-preview-evidence")).toContainText(/DERIVED|ASSUMED/i);
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => canvas.dataset.scaleMode || ""),
      { timeout: 3000 }
    ).toBe("structure");
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.minStarClearance || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(0);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.collisionAdjustedStarCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.groupMotionCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(2);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.nestedGroupMotionCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.directOrbitGuideCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(3);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.directOrbitTraceCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(6);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.groupOrbitGuideCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(2);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.orbitTraceProvenanceCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(5);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.subsystemMarkerCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(3);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassUnsafeSourceCount || 0)),
      { timeout: 3000 }
    ).toBe(0);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassSourceCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(6);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassAssumedCount || 0)),
      { timeout: 3000 }
    ).toBe(1);
    const objectList = page.locator("[data-testid='system-preview-object-list']");
    await expect(objectList.locator(".stellar-class-chip[data-stellar-token='a']")).toHaveCount(2);
    await expect(objectList.locator(".stellar-class-chip[data-stellar-token='m']")).toHaveCount(4);
    await expect(objectList.locator(".stellar-class-chip[data-stellar-token='u']")).toHaveCount(1);
  });

  test("hierarchical multi-star previews use mass-weighted group motion", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "hierarchical barycentric motion smoke uses desktop detail layout");
    const cases = ["HD 213885"];
    const fieldValue = (owner, key) => owner?.fields?.[key]?.value;

    for (const query of cases) {
      await test.step(query, async () => {
        const response = await page.request.get("/api/v1/systems/search", {
          params: { q: query, limit: "1" },
        });
        expect(response.ok()).toBeTruthy();
        const payload = await response.json();
        const systemId = payload.items?.[0]?.system_id;
        expect(systemId, `${query} system_id`).toBeTruthy();

        const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
        expect(sceneResponse.ok()).toBeTruthy();
        const scenePayload = await sceneResponse.json();
        const stars = scenePayload.render_scene?.bodies?.stars || [];
        const starsByKey = new Map(stars.map((star) => [star.render_key, star]));
        const groupOrbit = (scenePayload.render_scene?.orbits || []).find((orbit) => orbit.endpoint_kind === "group_pair");
        const simulationTree = scenePayload.render_scene?.simulation_tree || {};
        expect(groupOrbit, `${query} group-pair orbit`).toBeTruthy();
        expect(simulationTree.schema_version, `${query} simulation tree schema`).toBe("simulation_tree_v1");
        expect(simulationTree.diagnostics?.nested_orbit_count || 0, `${query} nested tree orbit count`).toBeGreaterThanOrEqual(1);
        expect(simulationTree.diagnostics?.unattached_orbit_count || 0, `${query} unattached tree orbit count`).toBe(0);
        const sideMass = (keys) => (keys || [])
          .map((key) => Number(fieldValue(starsByKey.get(key), "mass_msun")))
          .filter((mass) => Number.isFinite(mass) && mass > 0)
          .reduce((sum, mass) => sum + mass, 0);
        expect(sideMass(groupOrbit.primary_child_body_keys), `${query} primary side mass`).toBeGreaterThan(0);
        expect(sideMass(groupOrbit.secondary_child_body_keys), `${query} secondary side mass`).toBeGreaterThan(0);

        await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
        await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
        const previewCanvas = page.locator(".system-preview-canvas canvas");
        await expect(previewCanvas).toBeVisible();
        await expect.poll(
          () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.groupMotionCount || 0)),
          { timeout: 3000 }
        ).toBeGreaterThanOrEqual(1);
        await expect.poll(
          () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.massWeightedGroupMotionCount || 0)),
          { timeout: 3000 }
        ).toBeGreaterThanOrEqual(1);
        await expect.poll(
          () => previewCanvas.evaluate((canvas) => canvas.dataset.simulationTreeActive || ""),
          { timeout: 3000 }
        ).toBe("true");
        await expect.poll(
          () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.simulationTreeNestedOrbitCount || 0)),
          { timeout: 3000 }
        ).toBeGreaterThanOrEqual(1);
      });
    }
  });

  test("Alpha and Proxima resolve as one accepted system with Proxima planet hosts", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "accepted system identity smoke uses API and desktop detail layout");
    const alphaResponse = await page.request.get("/api/v1/systems/search", {
      params: { q: "Alpha Centauri", limit: "1", sort: "match" },
    });
    const proximaResponse = await page.request.get("/api/v1/systems/search", {
      params: { q: "Proxima Centauri", limit: "1", sort: "match" },
    });
    expect(alphaResponse.ok()).toBeTruthy();
    expect(proximaResponse.ok()).toBeTruthy();
    const alpha = (await alphaResponse.json()).items?.[0];
    const proxima = (await proximaResponse.json()).items?.[0];
    expect(alpha?.system_id, "Alpha Centauri system_id").toBeTruthy();
    expect(proxima?.system_id, "Proxima Centauri system_id").toBeTruthy();
    expect(proxima.system_id, "Proxima should resolve to accepted Alpha/Proxima system").toBe(alpha.system_id);
    expect(alpha.wds_id).toBe("14396-6050");
    expect(proxima.wds_id).toBe("14396-6050");
    expect(Number(alpha.star_count || 0)).toBeGreaterThanOrEqual(3);
    expect(Number(proxima.planet_count || 0)).toBeGreaterThanOrEqual(2);
    expect(String(proxima.display_name || proxima.system_name || "")).toMatch(/Alpha Centauri/i);
    expect(String(proxima.matched_alias || "")).toMatch(/Proxima Centauri/i);

    const sceneResponse = await page.request.get(`/api/v1/systems/${alpha.system_id}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    const stars = scenePayload.render_scene?.bodies?.stars || [];
    const planets = scenePayload.render_scene?.bodies?.planets || [];
    expect(stars.length, "accepted Alpha/Proxima rendered stars").toBeGreaterThanOrEqual(3);
    expect(planets.length, "accepted Alpha/Proxima rendered planets").toBeGreaterThanOrEqual(2);
    const proximaStar = stars.find((star) => /Proxima/i.test(String(star.display_name || "")));
    expect(proximaStar, "rendered Proxima member").toBeTruthy();
    const proximaKey = proximaStar.render_key;
    expect(planets.every((planet) => planet.host_body_key === proximaKey), "Proxima planets should host on Proxima member").toBeTruthy();
    const membership = scenePayload.render_scene?.diagnostics?.membership_reconciliation || {};
    expect(Number(membership.source_hierarchy_leaf_count || 0)).toBeGreaterThanOrEqual(3);
    expect(Number(membership.rendered_stellar_body_count || 0)).toBeGreaterThanOrEqual(3);
    expect(String(membership.membership_gate || "")).toMatch(/source_hierarchy_leaves/);
    expect(Number(membership.unmatched_orbit_endpoint_count || 0)).toBe(0);
    const alphaOrbits = scenePayload.render_scene?.orbits || [];
    expect(alphaOrbits.some((orbit) => orbit.endpoint_kind === "group_pair" && orbit.relation_kind === "hierarchical_pair")).toBeTruthy();
    expect(Number(scenePayload.render_scene?.simulation_tree?.diagnostics?.nested_orbit_count || 0)).toBeGreaterThanOrEqual(1);

    await page.goto(`/systems/${alpha.system_id}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect(previewCanvas).toBeVisible();
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => canvas.dataset.simulationTreeActive || ""),
      { timeout: 3000 }
    ).toBe("true");
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.simulationTreeStaticSiblingLayoutCount || 0)),
      { timeout: 3000 }
    ).toBe(0);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.simulationTreeNestedOrbitCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZoneCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(3);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZoneParentPlaneCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(3);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZonePlanetPlaneCount || 0)),
      { timeout: 3000 }
    ).toBe(0);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZoneMaxPlaneInclinationDeg || 0)),
      { timeout: 3000 }
    ).toBeGreaterThan(100);
    await page.locator(".system-preview-object-chip", { hasText: /Proxima Centauri/i }).first().click();
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => canvas.dataset.cameraTargetObjectId || ""),
      { timeout: 3000 }
    ).toBe(proximaKey);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => {
        const values = String(canvas.dataset.cameraTargetPosition || "")
          .split(",")
          .map((value) => Number(value));
        return values.some((value) => Number.isFinite(value) && Math.abs(value) > 0.05);
      }),
      { timeout: 3000 }
    ).toBeTruthy();
    await expect(page.locator(".system-detail-v2")).toContainText(/Proxima/i);
  });

  test("system hierarchy exposes compact stellar leaves by default", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "hierarchy visibility smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "HD 213885", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "HD 213885 system_id").toBeTruthy();

    const detailResponse = await page.request.get(`/api/v1/systems/${systemId}`);
    expect(detailResponse.ok()).toBeTruthy();
    const detailPayload = await detailResponse.json();
    expect(detailPayload.hierarchy?.counts?.stars).toBe(3);

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    const hierarchyPanel = page.locator(".hierarchy-panel");
    await expect(hierarchyPanel).toBeVisible();
    await expect(hierarchyPanel.getByText("HD 213885 AA", { exact: true })).toBeVisible();
    await expect(hierarchyPanel.getByText("HD 213885 AB", { exact: true })).toBeVisible();
    await expect(hierarchyPanel.getByText("HD 213885 B", { exact: true })).toBeVisible();
  });

  test("messy hierarchy preview preserves Nu Sco source-native leaves", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "messy hierarchy renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "Nu Sco", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "Nu Sco system_id").toBeTruthy();

    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    const renderBodies = scenePayload.render_scene?.bodies || {};
    const stars = renderBodies.stars || [];
    const subsystems = renderBodies.subsystems || [];
    const orbits = scenePayload.render_scene?.orbits || [];
    const nuScoComponents = stars.map((star) => String(star.component || "").toUpperCase()).sort();
    expect(nuScoComponents).toEqual(expect.arrayContaining(["AA", "AB", "AC", "B", "C", "DA", "DB"]));
    expect(stars).toHaveLength(7);
    const unresolvedAB = stars.find((star) => String(star.component || "").toUpperCase() === "AB");
    expect(unresolvedAB?.spectral_class || null).toBeNull();
    expect(unresolvedAB?.fields?.spectral_type_raw?.status).toBe("missing");
    expect(subsystems.map((subsystem) => String(subsystem.component || "").toUpperCase())).toEqual(expect.arrayContaining([
      "AB",
      "A",
      "AAB",
      "CD",
      "D",
    ]));
    for (const subsystem of subsystems) {
      expect(subsystem.fields?.component_label?.status).toMatch(/source|derived/);
      expect(subsystem.fields?.hierarchy_basis?.status).toBe("derived");
      expect(subsystem.fields?.hierarchy_basis?.layer).toBe("arm");
    }
    expect(orbits.filter((orbit) => orbit.endpoint_kind === "star_pair")).toHaveLength(2);
    expect(orbits.filter((orbit) => orbit.endpoint_kind === "group_pair")).toHaveLength(4);
    expect(scenePayload.render_scene?.simulation_tree?.diagnostics?.active_orbit_count).toBe(5);
    expect(scenePayload.render_scene?.simulation_tree?.diagnostics?.skipped_orbits || []).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ orbit_key: "orbit:13216", reason: expect.stringContaining("overlap") }),
      ])
    );

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect(previewCanvas).toBeVisible();
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.directOrbitGuideCount || 0)),
      { timeout: 3000 }
    ).toBe(2);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.groupOrbitGuideCount || 0)),
      { timeout: 3000 }
    ).toBe(4);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.subsystemMarkerCount || 0)),
      { timeout: 3000 }
    ).toBe(5);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableStarCount || 0)),
      { timeout: 3000 }
    ).toBe(7);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableSubsystemCount || 0)),
      { timeout: 3000 }
    ).toBe(5);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableOrbitCount || 0)),
      { timeout: 3000 }
    ).toBe(6);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.orbitTraceProvenanceCount || 0)),
      { timeout: 3000 }
    ).toBe(6);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("subsystem");
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("orbit");
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassUnsafeSourceCount || 0)),
      { timeout: 3000 }
    ).toBe(0);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassMissingCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassAssumedCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(3);
  });

  test("V1054 Oph preview reconciles source leaves without orphan endpoints", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "complex hierarchy renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "V1054 Oph", limit: "20" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const item = (payload.items || []).find((candidate) => candidate.wds_id === "16555-0820");
    const systemId = item?.system_id;
    expect(systemId, "V1054 Oph system_id").toBeTruthy();

    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    const stars = scenePayload.render_scene?.bodies?.stars || [];
    const membership = scenePayload.render_scene?.diagnostics?.membership_reconciliation || {};
    const v1054Components = stars.map((star) => String(star.component || "").toUpperCase()).sort();
    expect(v1054Components).toEqual(expect.arrayContaining(["A", "BA", "BB", "C", "F"]));
    expect(stars).toHaveLength(5);
    expect(stars.map((star) => star.display_name)).not.toContain("V1054 Oph D");
    expect(membership.membership_gate).toBe("source_hierarchy_leaves");
    expect(membership.source_hierarchy_leaf_count).toBe(5);
    expect(membership.rendered_stellar_body_count).toBe(5);
    expect(membership.unmatched_orbit_endpoint_keys || []).toContain("comp:msc:wds:16555-0820:d");

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect(previewCanvas).toBeVisible();
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableStarCount || 0)),
      { timeout: 3000 }
    ).toBe(5);
    const objectList = page.locator("[data-testid='system-preview-object-list']");
    await expect(objectList).toBeVisible();
    await expect(objectList.locator(".system-preview-object-chip")).toHaveCount(6);
    for (const name of ["V1054 Oph", "V1054 Oph BA", "V1054 Oph BB", "GJ 643", "VB 8"]) {
      await expect(objectList.getByText(name, { exact: true })).toBeVisible();
    }
    await expect(objectList.getByText("V1054 Oph D", { exact: true })).toHaveCount(0);
  });

  test("compact companion preview uses assumed visual binary fallback", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "compact companion renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "Sirius", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "Sirius system_id").toBeTruthy();

    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    const stars = scenePayload.render_scene?.bodies?.stars || [];
    const orbits = scenePayload.render_scene?.orbits || [];
    expect(stars.map((star) => star.display_name)).toEqual(expect.arrayContaining(["Sirius", "Sirius B"]));
    expect(stars.map((star) => star.spectral_class)).toEqual(expect.arrayContaining(["A", "D"]));
    const siriusB = stars.find((star) => star.display_name === "Sirius B");
    expect(siriusB?.body_class).toBe("white_dwarf");
    expect(siriusB?.compact_type).toBe("white_dwarf");
    expect(siriusB?.fields?.object_type?.value).toBe("white_dwarf");
    expect(siriusB?.fields?.object_type?.status).toBe("source");
    const fallbackOrbit = orbits.find((orbit) => orbit.relation_kind === "visual_binary_fallback");
    expect(fallbackOrbit, "Sirius visual fallback orbit").toBeTruthy();
    expect(fallbackOrbit.source?.layer).toBe("disc_assumption");
    expect(fallbackOrbit.fields?.period_days?.status).toBe("assumed");
    expect(fallbackOrbit.fields?.semi_major_axis_au?.status).toBe("assumed");

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    await expect(page.locator(".system-preview-canvas canvas")).toBeVisible();
    await expect(page.locator(".system-preview-evidence")).toContainText(/ASSUMED/i);
    await expect.poll(
      () => page.locator(".system-preview-canvas canvas").evaluate((canvas) => Number(canvas.dataset.directOrbitTraceCount || 0)),
      { timeout: 3000 }
    ).toBe(2);
  });

  test("planet-host preview renders hosted planets in a multi-star scene", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "preview renderer smoke uses desktop detail layout");
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "16 Cyg", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "16 Cyg system_id").toBeTruthy();

    const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
    expect(sceneResponse.ok()).toBeTruthy();
    const scenePayload = await sceneResponse.json();
    expect(scenePayload.render_scene?.bodies?.stars?.length).toBe(3);
    expect(scenePayload.render_scene?.bodies?.planets?.length).toBeGreaterThanOrEqual(1);
    expect(scenePayload.render_scene?.bodies?.planets?.some((planet) => planet.host_body_key)).toBeTruthy();
    expect(scenePayload.render_scene?.bodies?.planets?.[0]?.source?.host_resolution).toMatch(/render_star/);

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect(previewCanvas).toBeVisible();
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.planetHostGroupCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
  });

  test("wide-orbit presentation benchmarks expose source, derived, and assumed motion policy", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "wide-orbit benchmark smoke uses desktop API and one visual render");
    const cases = [
      { query: "Alpha Centauri", minStars: 3, minSourceGroup: 1, minNested: 1, maxUnattached: 0 },
      { query: "Tegmine", minStars: 5, minSourceGroup: 1, maxUnattached: 2 },
      { query: "Fomalhaut", minStars: 3, minAssumed: 1 },
      { query: "Xi Scorpii", minStars: 5, minSourceGroup: 1, minNested: 1, maxUnattached: 2 },
      { query: "eps Ind", minStars: 3, minSourceGroup: 1 },
      { query: "Sirius", minStars: 2, minAssumed: 1 },
      { query: "Castor", minStars: 6, minSourceGroup: 2, maxUnattached: 2 },
      { query: "Nu Sco", minStars: 7, minSourceGroup: 2, minNested: 1, maxUnattached: 4 },
      { query: "16 Cyg", minStars: 3, minSkippedOverlap: 1, activeOrbitPattern: /16 Cyg B A - 16 Cyg B B/, maxUnattached: 0 },
    ];
    let alphaSystemId = null;

    for (const benchmark of cases) {
      await test.step(`wide orbit policy ${benchmark.query}`, async () => {
        const response = await page.request.get("/api/v1/systems/search", {
          params: { q: benchmark.query, limit: "1", sort: "match" },
        });
        expect(response.ok()).toBeTruthy();
        const payload = await response.json();
        const systemId = payload.items?.[0]?.system_id;
        expect(systemId, `${benchmark.query} system_id`).toBeTruthy();
        if (benchmark.query === "Alpha Centauri") {
          alphaSystemId = systemId;
        }

        const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
        expect(sceneResponse.ok()).toBeTruthy();
        const scenePayload = await sceneResponse.json();
        const renderScene = scenePayload.render_scene || {};
        const stars = renderScene.bodies?.stars || [];
        const orbits = renderScene.orbits || [];
        const orbitPolicy = renderScene.diagnostics?.orbit_counts?.by_policy || {};
        const treeDiagnostics = renderScene.simulation_tree?.diagnostics || {};
        const treeNodes = renderScene.simulation_tree?.nodes || {};
        expect(stars.length, `${benchmark.query} rendered stars`).toBeGreaterThanOrEqual(benchmark.minStars);
        expect(orbits.length, `${benchmark.query} should not be static-only`).toBeGreaterThan(0);
        expect(Number(orbitPolicy.source_group_orbit || 0)).toBeGreaterThanOrEqual(benchmark.minSourceGroup || 0);
        expect(Number(orbitPolicy.assumed_visual_orbit || 0)).toBeGreaterThanOrEqual(benchmark.minAssumed || 0);
        expect(Number(treeDiagnostics.nested_orbit_count || 0)).toBeGreaterThanOrEqual(benchmark.minNested || 0);
        if (benchmark.minSkippedOverlap) {
          const skipped = Array.isArray(treeDiagnostics.skipped_orbits) ? treeDiagnostics.skipped_orbits : [];
          expect(
            skipped.filter((item) => String(item.reason || "").includes("overlap")).length,
            `${benchmark.query} skipped overlap diagnostics`
          ).toBeGreaterThanOrEqual(benchmark.minSkippedOverlap);
        }
        if (benchmark.activeOrbitPattern) {
          const activeLabels = Object.values(treeNodes)
            .filter((node) => node?.node_type === "barycenter")
            .map((node) => String(node.display_name || ""));
          expect(activeLabels.join(" | ")).toMatch(benchmark.activeOrbitPattern);
        }
        const activeBarycenters = Object.entries(treeNodes)
          .filter(([, node]) => node?.node_type === "barycenter")
          .map(([key, node]) => [key, new Set((node.leaf_body_keys || []).map(String))]);
        for (let i = 0; i < activeBarycenters.length; i += 1) {
          for (let j = i + 1; j < activeBarycenters.length; j += 1) {
            const [leftKey, leftSet] = activeBarycenters[i];
            const [rightKey, rightSet] = activeBarycenters[j];
            const overlap = [...leftSet].filter((item) => rightSet.has(item));
            const leftSubset = [...leftSet].every((item) => rightSet.has(item));
            const rightSubset = [...rightSet].every((item) => leftSet.has(item));
            expect(
              overlap.length === 0 || leftSubset || rightSubset,
              `${benchmark.query} active barycenters should not partially overlap: ${leftKey} vs ${rightKey}`
            ).toBeTruthy();
          }
        }
        if (benchmark.maxUnattached !== undefined) {
          expect(Number(treeDiagnostics.unattached_orbit_count || 0)).toBeLessThanOrEqual(benchmark.maxUnattached);
        }
      });
    }

    await page.goto(`/systems/${alphaSystemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect(previewCanvas).toBeVisible();
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.simulationTreeNestedOrbitCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
  });

  test("benchmark system previews paint nonblank scenes", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "benchmark render smoke uses desktop detail layout");
    const cases = [
      { query: "Alpha Centauri", minStars: 3, minPlanets: 2 },
      { query: "Proxima Centauri", minStars: 3, minPlanets: 2 },
      { query: "55 Cnc", minStars: 1, minPlanets: 5 },
      { query: "Sol", minStars: 1, minPlanets: 8 },
    ];

    for (const benchmark of cases) {
      await test.step(`render ${benchmark.query}`, async () => {
        const response = await page.request.get("/api/v1/systems/search", {
          params: { q: benchmark.query, limit: "1", sort: "match" },
        });
        expect(response.ok()).toBeTruthy();
        const payload = await response.json();
        const systemId = payload.items?.[0]?.system_id;
        expect(systemId, `${benchmark.query} system_id`).toBeTruthy();

        const sceneResponse = await page.request.get(`/api/v1/systems/${systemId}/simulation-scene`);
        expect(sceneResponse.ok()).toBeTruthy();
        const scenePayload = await sceneResponse.json();
        expect(scenePayload.render_scene?.schema_version).toBe("render_scene_v0.2");
        expect(scenePayload.render_scene?.bodies?.stars?.length || 0).toBeGreaterThanOrEqual(benchmark.minStars);
        expect(scenePayload.render_scene?.bodies?.planets?.length || 0).toBeGreaterThanOrEqual(benchmark.minPlanets);

        await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
        await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
        const previewCanvas = page.locator(".system-preview-canvas canvas");
        await expect(previewCanvas).toBeVisible();
        await expect.poll(
          () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.inspectableStarCount || 0)),
          { timeout: 3000 }
        ).toBeGreaterThanOrEqual(benchmark.minStars);
        if (benchmark.query === "Sol") {
          await expect.poll(
            () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.habitableZoneCount || 0)),
            { timeout: 3000 }
          ).toBeGreaterThanOrEqual(1);
          await expect.poll(
            () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.planetDisplayEccentricityCappedCount || 0)),
            { timeout: 3000 }
          ).toBeGreaterThanOrEqual(1);
          await page.locator("[data-testid='system-preview-scale-mode']").selectOption("true_orbits");
          await expect.poll(
            () => previewCanvas.evaluate((canvas) => canvas.dataset.scaleMode || ""),
            { timeout: 3000 }
          ).toBe("true_orbits");
          await expect.poll(
            () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.trueOrbitMaxBodyToMinOrbitRatio || 1)),
            { timeout: 3000 }
          ).toBeLessThan(0.9);
        }
        await expectPreviewCanvasPainted(previewCanvas, benchmark.query);
      });
    }
  });
});
