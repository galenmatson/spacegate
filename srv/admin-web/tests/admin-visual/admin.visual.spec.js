import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";
import path from "node:path";

const screens = [
  { key: "overview", label: "Overview", nav: null },
  { key: "runtime", label: "Runtime", nav: "Runtime" },
  { key: "builds", label: "Builds", nav: "Builds" },
  { key: "dataset", label: "Dataset", nav: "Dataset" },
  { key: "operations", label: "Operations", nav: "Operations" },
  {
    key: "object-diagnostics",
    label: "Object Diagnostics",
    nav: "Objects",
    prepare: prepareObjectDiagnostics,
  },
];

function adminUrl(testInfo, path = "") {
  const baseURL = String(testInfo.project.use.baseURL || "");
  const normalizedBase = baseURL.endsWith("/") ? baseURL : `${baseURL}/`;
  return new URL(path, normalizedBase).toString();
}

function slug(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function artifactPath(testInfo, filename) {
  const outputRoot = testInfo.config.metadata?.outputRoot || testInfo.outputDir;
  const dir = path.join(outputRoot, "captures", slug(testInfo.project.name));
  await fs.mkdir(dir, { recursive: true });
  return path.join(dir, filename);
}

async function visibleText(page) {
  return page.locator("body").innerText({ timeout: 5_000 }).catch(() => "");
}

async function authGateVisible(page) {
  const text = await visibleText(page);
  return /sign in|log in|login|authenticate|unauthenticated|auth required/i.test(text);
}

async function waitForAdminSettled(page) {
  await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
  await page
    .waitForFunction(
      () => {
        const busyPattern =
          /Loading|Refreshing|Searching|Loading diagnostics|Refreshing build state|Refreshing operations/i;
        return !Array.from(
          document.querySelectorAll(".boot, .status-line, button")
        ).some((node) => busyPattern.test(String(node.textContent || "")));
      },
      null,
      { timeout: 15_000 }
    )
    .catch(() => {});
  await page.waitForTimeout(300);
}

async function publicSiteVisible(page) {
  const text = await visibleText(page);
  return /CoolStars|Star Selector|Search systems by name/i.test(text);
}

async function prepareObjectDiagnostics(page) {
  const input = page.getByPlaceholder(/Sol, Mars, Ganymede/i);
  await input.fill("Sol");
  await page.getByRole("button", { name: /^Search$/ }).click();
  await expect(page.locator(".status-line")).toContainText("Ready", {
    timeout: 20_000,
  });
  await expect(page.getByRole("heading", { name: "Readiness" })).toBeVisible();
}

async function pageLayoutMetrics(page) {
  return page.evaluate(() => {
    const body = document.body;
    const root = document.documentElement;
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const horizontalOverflow =
      Math.max(body.scrollWidth, root.scrollWidth) > viewportWidth + 2;
    const fixedOrSticky = Array.from(document.querySelectorAll("*"))
      .filter((element) => {
        const style = window.getComputedStyle(element);
        return style.position === "fixed" || style.position === "sticky";
      })
      .slice(0, 20)
      .map((element) => {
        const rect = element.getBoundingClientRect();
        return {
          tag: element.tagName.toLowerCase(),
          className: String(element.className || ""),
          text: String(element.textContent || "").trim().slice(0, 80),
          rect: {
            x: rect.x,
            y: rect.y,
            width: rect.width,
            height: rect.height,
          },
        };
      });
    return {
      title: document.title,
      viewportWidth,
      viewportHeight,
      bodyScrollWidth: body.scrollWidth,
      bodyScrollHeight: body.scrollHeight,
      horizontalOverflow,
      fixedOrSticky,
      activeNav: Array.from(document.querySelectorAll("nav button.active"))
        .map((node) => node.textContent?.trim())
        .filter(Boolean),
      h1: Array.from(document.querySelectorAll("h1"))
        .map((node) => node.textContent?.trim())
        .filter(Boolean),
      panelCount: document.querySelectorAll(".panel").length,
      kpiCount: document.querySelectorAll(".kpi").length,
      dangerBadgeCount: document.querySelectorAll(".badge.danger, .kpi.danger")
        .length,
      warningBadgeCount: document.querySelectorAll(".badge.warn, .kpi.warn")
        .length,
    };
  });
}

test.describe("Spacegate Admin visual sweep", () => {
  test("captures core admin screens", async ({ page }, testInfo) => {
    const consoleEvents = [];
    const requestFailures = [];
    const pageErrors = [];
    const screenReports = [];

    page.on("console", (message) => {
      if (["error", "warning"].includes(message.type())) {
        consoleEvents.push({
          type: message.type(),
          text: message.text(),
          location: message.location(),
        });
      }
    });
    page.on("requestfailed", (request) => {
      requestFailures.push({
        method: request.method(),
        url: request.url(),
        failure: request.failure()?.errorText || "unknown",
      });
    });
    page.on("pageerror", (error) => {
      pageErrors.push(String(error?.stack || error));
    });

    const entryUrl = adminUrl(testInfo);
    await page.goto(entryUrl, { waitUntil: "domcontentloaded" });
    await waitForAdminSettled(page);

    if (await publicSiteVisible(page)) {
      const shotPath = await artifactPath(testInfo, "wrong-app.png");
      await page.screenshot({ path: shotPath, fullPage: true });
      const report = {
        status: "wrong_app",
        message:
          "Visual QA loaded the public Spacegate/CoolStars UI instead of Admin. Check SPACEGATE_ADMIN_VISUAL_BASE_URL and Admin routing.",
        requestedUrl: entryUrl,
        finalUrl: page.url(),
        baseURL: testInfo.project.use.baseURL,
        screenshot: shotPath,
        consoleEvents,
        requestFailures,
        pageErrors,
      };
      await fs.writeFile(
        await artifactPath(testInfo, "visual-summary.json"),
        JSON.stringify(report, null, 2)
      );
      throw new Error(
        `Admin visual QA loaded the public site instead of Admin: ${page.url()}`
      );
    }

    if (await authGateVisible(page)) {
      const shotPath = await artifactPath(testInfo, "auth-gate.png");
      await page.screenshot({ path: shotPath, fullPage: true });
      const report = {
        status: "auth_required",
        message:
          "Admin auth gate is visible. Provide SPACEGATE_ADMIN_STORAGE_STATE to capture authenticated Admin screens.",
        requestedUrl: entryUrl,
        finalUrl: page.url(),
        baseURL: testInfo.project.use.baseURL,
        screenshot: shotPath,
        consoleEvents,
        requestFailures,
        pageErrors,
      };
      await fs.writeFile(
        await artifactPath(testInfo, "visual-summary.json"),
        JSON.stringify(report, null, 2)
      );
      testInfo.annotations.push({
        type: "auth",
        description:
          "Admin auth required; only auth gate screenshot captured.",
      });
      expect(pageErrors, "page errors before auth gate").toEqual([]);
      return;
    }

    await expect(page.locator(".admin-shell")).toBeVisible({ timeout: 20_000 });

    for (const screen of screens) {
      if (screen.nav) {
        await page.getByRole("button", { name: screen.nav }).click();
      }
      await waitForAdminSettled(page);
      if (screen.prepare) {
        await screen.prepare(page);
        await waitForAdminSettled(page);
      }
      const shotPath = await artifactPath(testInfo, `${slug(screen.key)}.png`);
      await page.screenshot({ path: shotPath, fullPage: true });
      screenReports.push({
        ...screen,
        screenshot: shotPath,
        metrics: await pageLayoutMetrics(page),
      });
    }

    const report = {
      status: "captured",
      project: testInfo.project.name,
      requestedUrl: entryUrl,
      finalUrl: page.url(),
      baseURL: testInfo.project.use.baseURL,
      screens: screenReports,
      consoleEvents,
      requestFailures,
      pageErrors,
    };
    await fs.writeFile(
      await artifactPath(testInfo, "visual-summary.json"),
      JSON.stringify(report, null, 2)
    );

    const unexpectedConsoleErrors = consoleEvents.filter(
      (event) => event.type === "error"
    );
    const unexpectedRequestFailures = requestFailures.filter(
      (failure) => !/favicon|source-map/i.test(failure.url)
    );
    const overflowScreens = screenReports.filter(
      (screen) => screen.metrics?.horizontalOverflow
    );

    expect(pageErrors, "page errors").toEqual([]);
    expect(unexpectedConsoleErrors, "console errors").toEqual([]);
    expect(unexpectedRequestFailures, "failed network requests").toEqual([]);
    expect(overflowScreens, "screens with horizontal overflow").toEqual([]);
  });
});
