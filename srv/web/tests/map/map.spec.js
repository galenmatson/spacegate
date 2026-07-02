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

test.describe("public 3D map beta", () => {
  test("map title comes from public branding config", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop header title check");
    const configResponse = await page.request.get("/api/v1/public-config");
    expect(configResponse.ok()).toBeTruthy();
    const config = await configResponse.json();
    await openMap(page);
    await expect(page.locator(".map-eyebrow-link")).toHaveText("Spacegate Stellar Database");
    await expect(page.locator(".map-eyebrow-link")).toHaveAttribute("href", "https://spacegates.org/");
    await expect(page.locator(".map-title-block h1")).toHaveText(config.map_title || "Coolstars Map");
  });

  test("header menu controls theme and map keybind scheme", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop header menu and keyboard smoke");
    await openMap(page);
    const menu = page.locator(".map-header-menu");
    const canvas = page.locator(".map-canvas canvas");
    await expect(menu).toBeVisible();
    await expect(page.locator(".map-actions > .map-theme-select")).toHaveCount(0);
    await menu.locator("summary").click();
    await expect(menu.locator(".map-header-menu-panel")).toBeVisible();
    const themeSelect = menu.locator(".map-theme-select select");
    const keybindSelect = menu.locator(".map-keybind-select select");
    await expect(themeSelect).toBeVisible();
    await expect(keybindSelect).toBeVisible();

    await themeSelect.selectOption("aurora");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("aurora");

    await keybindSelect.selectOption("esdf");
    await expect.poll(
      () => canvas.evaluate((node) => node.dataset.mapKeybindScheme || ""),
      { timeout: 3000 }
    ).toBe("esdf");
    await expect(page.locator(".map-desktop-hint")).toContainText(/ESDF fly/i);
    const beforeEsdfMove = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.keyboard.down("e");
    await page.waitForTimeout(350);
    await page.keyboard.up("e");
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeEsdfMove);

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

    const beforeArrowMove = await canvas.evaluate((node) => node.dataset.mapCameraPosition || "");
    await page.keyboard.down("ArrowUp");
    await page.waitForTimeout(350);
    await page.keyboard.up("ArrowUp");
    await expect
      .poll(() => canvas.evaluate((node) => node.dataset.mapCameraPosition || ""), { timeout: 3000 })
      .not.toBe(beforeArrowMove);

    await page.locator(".map-title-block").click();
    await expect(menu.locator(".map-header-menu-panel")).toBeHidden();

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
    await contextMenu.getByRole("button", { name: /^Measure$/i }).click();

    await expect(page.locator(".map-route-summary")).toContainText(/1 legs/i);
    await expect(page.locator(".map-route-summary")).toContainText(/total/i);
    await expect(page.locator(".map-route-leg-list li")).toHaveCount(1);

    await page.getByRole("button", { name: /undo/i }).click();
    await expect(page.locator(".map-route-summary")).toHaveCount(0);

    await page.mouse.click(box.x + box.width / 2 + 120, box.y + box.height / 2 + 80, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await page.mouse.click(box.x + 24, box.y + 24, { button: "right" });
    await expect(contextMenu).toHaveCount(0);

    await page.mouse.click(box.x + box.width / 2 + 120, box.y + box.height / 2 + 80, { button: "right" });
    await expect(contextMenu).toBeVisible();
    await contextMenu.getByRole("button", { name: /^Measure$/i }).click();
    await expect(page.locator(".map-route-summary")).toBeVisible();
    await page.mouse.click(box.x + box.width / 2 - 120, box.y + box.height / 2 - 80, { button: "right" });
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
    await page.locator(".map-history-pill").first().click();
    const drill = page.locator("[data-testid='map-system-drill']");
    await expect(drill).toBeVisible();
    const chip = drill.locator(".map-snapshot-chip.ready").first();
    test.skip(await chip.count() === 0, "initial selected map system has no deterministic snapshot");
    await expect(chip).toBeVisible();
    await chip.hover();
    await expect(page.locator(".map-snapshot-popover img")).toBeVisible();
    const popoverBox = await page.locator(".map-snapshot-popover").boundingBox();
    const viewport = page.viewportSize();
    expect(popoverBox, "snapshot popover bounds").toBeTruthy();
    expect(popoverBox.x).toBeGreaterThanOrEqual(0);
    expect(popoverBox.x + popoverBox.width).toBeLessThanOrEqual(viewport.width);
  });

  test("map selection opens System Simulation peek and explore drill-in", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop drill-in smoke uses hover/canvas layout");
    await openMap(page);
    await page.locator(".map-history-pill").first().click();
    await expect(page.locator(".map-contacts-panel .map-name-info")).toHaveCount(0);
    await expect(page.locator(".map-contacts-panel .map-name-copy")).toHaveCount(0);

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
    await expect(drill.locator("[data-testid='system-preview-scale-mode']")).toBeVisible();
    await expect(drill.locator(".system-preview-speed select")).toBeVisible();
    await expect(drill.locator(".system-preview-speed select option[value='1000']")).toHaveCount(1);
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
    await expect(page.getByText("Cool Stars Nearby")).toBeVisible();
    await expect(page.getByText("Next Nearby")).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("peek");
    const mapBox = await canvasBox(page);
    await page.mouse.click(mapBox.x + 28, mapBox.y + 28, { button: "right" });
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");

    await page.locator(".map-history-pill").first().click();
    await expect(drill).toBeVisible();
    await drill.locator(".map-system-drill-title").click();
    await expect(drill).toHaveAttribute("data-drill-mode", "explore");
    await expect(drill).toContainText(/System:/i);
    await expect.poll(
      () => drill.evaluate((node) => node.querySelectorAll(
        ".system-preview-readout > div:not(.system-preview-evidence):not(.system-preview-policy)"
      ).length),
      { timeout: 3000 }
    ).toBeGreaterThan(0);
    await expect.poll(
      () => drill.evaluate((node) => {
        const heights = Array.from(
          node.querySelectorAll(".system-preview-readout > div:not(.system-preview-evidence):not(.system-preview-policy)")
        ).map((pill) => pill.getBoundingClientRect().height);
        return heights.length ? Math.max(...heights) : 0;
      }),
      { timeout: 3000 }
    ).toBeLessThanOrEqual(36);
    const diagnostics = drill.locator("[data-testid='system-preview-diagnostics']");
    await expect(diagnostics).toBeVisible();
    await expect(diagnostics).not.toHaveAttribute("open", "");
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("explore");

    await page.evaluate(() => window.history.back());
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");

    await page.locator(".map-history-pill").first().click();
    await expect(drill).toBeVisible();
    await drill.getByRole("button", { name: /^Explore$/i }).click();
    await expect(drill).toHaveAttribute("data-drill-mode", "explore");
    await drill.getByRole("button", { name: /Back to Map/i }).click();
    await expect(drill).toHaveCount(0);
    await expect.poll(
      () => page.locator(".map-page").evaluate((node) => node.getAttribute("data-map-drill-mode") || ""),
      { timeout: 3000 }
    ).toBe("flight");
  });

  test("map embedded simulator menus remain clickable across transparent themes", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop menu click regression uses native select controls");
    await openMap(page);
    await page.locator(".map-history-pill").first().click();

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
    await page.locator(".map-history-pill").first().click();

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
    expect(themeStyles.drillHeaderOverlap).toBe(false);
  });

  test("enterprise map theme uses LCARS block chrome", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop theme chrome check");
    await openMap(page);
    await page.locator(".map-history-pill").first().click();

    const menu = page.locator(".map-header-menu");
    await menu.locator("summary").click();
    await menu.locator(".map-theme-select select").selectOption("lcars");
    await expect.poll(() => page.evaluate(() => document.documentElement.dataset.theme || "")).toBe("lcars");

    const themeStyles = await page.evaluate(() => {
      const header = document.querySelector(".map-hud-top");
      const headerRail = window.getComputedStyle(header, "::before");
      const title = document.querySelector(".map-title-block h1");
      const button = document.querySelector(".map-hud-button");
      const drill = document.querySelector("[data-testid='map-system-drill']");
      const drillRail = window.getComputedStyle(drill, "::before");
      const drillBar = document.querySelector(".map-system-drill-bar");
      const previewCanvas = document.querySelector("[data-testid='map-system-drill'] .system-preview-canvas");
      const headerStyle = window.getComputedStyle(header);
      const titleStyle = window.getComputedStyle(title);
      const buttonStyle = window.getComputedStyle(button);
      const drillStyle = window.getComputedStyle(drill);
      const drillBarStyle = window.getComputedStyle(drillBar);
      const drillRect = drill.getBoundingClientRect();
      const previewCanvasRect = previewCanvas.getBoundingClientRect();
      return {
        headerBackground: headerStyle.backgroundColor,
        headerBorderTop: headerStyle.borderTopColor,
        headerRadius: headerStyle.borderTopLeftRadius,
        headerRailBackground: headerRail.backgroundColor,
        titleColor: titleStyle.color,
        titleLetterSpacing: titleStyle.letterSpacing,
        buttonBackground: buttonStyle.backgroundColor,
        buttonColor: buttonStyle.color,
        drillBackground: drillStyle.backgroundColor,
        drillRailBackground: drillRail.backgroundColor,
        drillBarPosition: drillBarStyle.position,
        previewCanvasHeightRatio: previewCanvasRect.height / Math.max(1, drillRect.height),
      };
    });
    expect(themeStyles.headerBackground).toBe("rgb(0, 0, 0)");
    expect(themeStyles.drillBackground).toBe("rgb(0, 0, 0)");
    expect(themeStyles.headerBorderTop).toBe("rgb(255, 212, 0)");
    expect(themeStyles.headerRadius).toBe("32px");
    expect(themeStyles.headerRailBackground).toBe("rgb(245, 162, 46)");
    expect(themeStyles.drillRailBackground).toBe("rgb(245, 162, 46)");
    expect(themeStyles.titleColor).toBe("rgb(245, 162, 46)");
    expect(themeStyles.titleLetterSpacing).not.toBe("normal");
    expect(themeStyles.buttonBackground).toBe("rgb(145, 160, 255)");
    expect(themeStyles.buttonColor).toBe("rgb(20, 15, 27)");
    expect(themeStyles.drillBarPosition).toBe("absolute");
    expect(themeStyles.previewCanvasHeightRatio).toBeGreaterThan(0.82);
  });

  test("cyberpunk map theme uses neon explorer chrome", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "desktop theme chrome check");
    await openMap(page);
    await page.locator(".map-history-pill").first().click();

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
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("planet");
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.inspectableTargetKinds || ""),
      { timeout: 3000 }
    ).toContain("orbit");
    await expect(page.locator(".system-preview-readout")).toContainText(/readiness/i);
    await expect(page.locator(".system-preview-readout")).not.toContainText(/local days/i);
    await expect(page.locator(".system-preview-readout")).not.toContainText(/missing inputs/i);
    await expect(page.locator("[data-testid='system-preview-visual-scale']")).toContainText(/visual scale/i);
    await expect(page.locator("[data-testid='system-preview-visual-scale']")).toContainText(/structure/i);
    const scaleModeSelect = page.locator("[data-testid='system-preview-scale-mode']");
    await expect(scaleModeSelect).toBeVisible();
    await expect.poll(
      () => sharedClockCanvas.evaluate((canvas) => canvas.dataset.scaleMode || ""),
      { timeout: 3000 }
    ).toBe("structure");
    await scaleModeSelect.selectOption("true_orbits");
    await expect(page.locator("[data-testid='system-preview-visual-scale']")).toContainText(/Orbit/i);
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
    const response = await page.request.get("/api/v1/systems/search", {
      params: { q: "TRAPPIST-1", limit: "1" },
    });
    expect(response.ok()).toBeTruthy();
    const payload = await response.json();
    const systemId = payload.items?.[0]?.system_id;
    expect(systemId, "TRAPPIST-1 system_id").toBeTruthy();

    await page.route("**/api/v1/systems/*/simulation-scene", (route) => route.fulfill({
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
    await expect(page.locator(".system-preview-readout")).toContainText(/rendered planets/i);
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

    await previewCanvas.scrollIntoViewIfNeeded();
    const previewBox = await previewCanvas.boundingBox();
    expect(previewBox, "mobile system preview canvas box").toBeTruthy();
    await page.touchscreen.tap(previewBox.x + previewBox.width / 2, previewBox.y + previewBox.height / 2);
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
    expect(scenePayload.render_scene?.bodies?.subsystems?.some((subsystem) => subsystem.display_name === "Castor AB")).toBeTruthy();
    const massPriorStars = (scenePayload.render_scene?.bodies?.stars || []).filter(
      (star) => star.fields?.visual_stellar_class?.basis === "mass_main_sequence_prior_v1"
    );
    expect(massPriorStars.length).toBeGreaterThanOrEqual(2);
    for (const star of massPriorStars) {
      expect(star.spectral_class || null).toBeNull();
      expect(star.fields?.visual_stellar_class?.status).toBe("assumed");
      expect(star.fields?.visual_stellar_class?.layer).toBe("render_scene");
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
    await expect(page.locator(".system-preview-readout")).toContainText(/rendered subsystems/i);
    await expect(page.locator(".system-preview-readout")).toContainText(/rendered orbits/i);
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
    ).toBeGreaterThanOrEqual(3);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.spectralClassAssumedCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(3);
  });

  test("hierarchical multi-star previews use mass-weighted group motion", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "hierarchical barycentric motion smoke uses desktop detail layout");
    const cases = ["HD 213885", "HD 79210", "eps Ind A", "Alpha Centauri"];
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
        if (query === "eps Ind A") {
          const periodField = groupOrbit.fields?.period_days || {};
          expect(Number(periodField.value), "eps Ind A outer A-B period").toBeGreaterThan(1_000_000);
          expect(periodField.status, "eps Ind A outer A-B period status").toBe("source");
          expect(String(periodField.basis || ""), "eps Ind A outer A-B period basis").toContain("msc_system_details");
        }
        if (query === "Alpha Centauri") {
          const directOrbit = (scenePayload.render_scene?.orbits || []).find((orbit) => orbit.endpoint_kind === "star_pair");
          expect(Number(groupOrbit.display_radius_scene), "Alpha Centauri outer display radius").toBeGreaterThan(Number(directOrbit?.display_radius_scene || 0) * 2);
          expect(Number(groupOrbit.fields?.period_days?.value), "Alpha Centauri AB-C period").toBeGreaterThan(100_000_000);
        }
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
        if (query === "eps Ind A") {
          await expect.poll(
            () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.treeHostedPlanetCount || 0)),
            { timeout: 3000 }
          ).toBeGreaterThanOrEqual(1);
        }
      });
    }
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
    expect(stars.map((star) => star.display_name)).toEqual(expect.arrayContaining([
      "14nu Sco AA",
      "14nu Sco AB",
      "14nu Sco AC",
      "14nu Sco B",
      "14nu Sco C",
      "14nu Sco DA",
      "14nu Sco DB",
    ]));
    expect(stars).toHaveLength(7);
    const unresolvedAB = stars.find((star) => star.display_name === "14nu Sco AB");
    expect(unresolvedAB?.spectral_class || null).toBeNull();
    expect(unresolvedAB?.fields?.spectral_type_raw?.status).toBe("missing");
    expect(subsystems.map((subsystem) => subsystem.display_name)).toEqual(expect.arrayContaining([
      "14nu Sco AB",
      "14nu Sco A",
      "14nu Sco AAB",
      "14nu Sco CD",
      "14nu Sco D",
    ]));
    for (const subsystem of subsystems) {
      expect(subsystem.fields?.component_label?.status).toMatch(/source|derived/);
      expect(subsystem.fields?.hierarchy_basis?.status).toBe("derived");
      expect(subsystem.fields?.hierarchy_basis?.layer).toBe("arm");
    }
    expect(orbits.filter((orbit) => orbit.endpoint_kind === "star_pair")).toHaveLength(2);
    expect(orbits.filter((orbit) => orbit.endpoint_kind === "group_pair")).toHaveLength(2);

    await page.goto(`/systems/${systemId}`, { waitUntil: "domcontentloaded" });
    await expect(page.locator("[data-testid='system-preview-panel']")).toBeVisible();
    const previewCanvas = page.locator(".system-preview-canvas canvas");
    await expect(previewCanvas).toBeVisible();
    await expect(page.locator(".system-preview-readout")).toContainText(/7\s*rendered stars/i);
    await expect(page.locator(".system-preview-readout")).toContainText(/5\s*rendered subsystems/i);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.directOrbitGuideCount || 0)),
      { timeout: 3000 }
    ).toBe(2);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.groupOrbitGuideCount || 0)),
      { timeout: 3000 }
    ).toBe(2);
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
    ).toBe(4);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.orbitTraceProvenanceCount || 0)),
      { timeout: 3000 }
    ).toBe(4);
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
    expect(stars.map((star) => star.display_name)).toEqual(expect.arrayContaining(["Sirius A", "Sirius B"]));
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
    await expect(page.locator(".system-preview-readout")).toContainText(/1\s*rendered orbits/i);
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
    await expect(page.locator(".system-preview-readout")).toContainText(/rendered planet/i);
    await expect.poll(
      () => previewCanvas.evaluate((canvas) => Number(canvas.dataset.planetHostGroupCount || 0)),
      { timeout: 3000 }
    ).toBeGreaterThanOrEqual(1);
  });

  test("benchmark system previews paint nonblank scenes", async ({ page }, testInfo) => {
    test.skip(testInfo.project.name.includes("mobile"), "benchmark render smoke uses desktop detail layout");
    const cases = [
      { query: "Alpha Centauri", minStars: 3, minPlanets: 0 },
      { query: "Proxima Centauri", minStars: 1, minPlanets: 2 },
      { query: "55 Cnc", minStars: 1, minPlanets: 5 },
      { query: "Sol", minStars: 1, minPlanets: 8 },
    ];

    for (const benchmark of cases) {
      await test.step(`render ${benchmark.query}`, async () => {
        const response = await page.request.get("/api/v1/systems/search", {
          params: { q: benchmark.query, limit: "1" },
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
        }
        await expectPreviewCanvasPainted(previewCanvas, benchmark.query);
      });
    }
  });
});
