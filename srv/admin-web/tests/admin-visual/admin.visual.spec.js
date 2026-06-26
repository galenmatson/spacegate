import { expect, test } from "@playwright/test";
import fs from "node:fs/promises";

const screens = [
  { key: "overview", label: "Overview", nav: null },
  { key: "runtime", label: "Runtime", nav: "Runtime" },
  { key: "builds", label: "Builds", nav: "Builds" },
  { key: "dataset", label: "Dataset", nav: "Dataset" },
  { key: "operations", label: "Operations", nav: "Operations" },
  { key: "object-diagnostics", label: "Object Diagnostics", nav: "Objects" },
];

function slug(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

async function visibleText(page) {
  return page.locator("body").innerText({ timeout: 5_000 }).catch(() => "");
}

async function authGateVisible(page) {
  const text = await visibleText(page);
  return /sign in|log in|login|authenticate|unauthenticated|auth required/i.test(text);
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

    await page.goto("/", { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 20_000 }).catch(() => {});

    if (await authGateVisible(page)) {
      const shotPath = testInfo.outputPath("auth-gate.png");
      await page.screenshot({ path: shotPath, fullPage: true });
      const report = {
        status: "auth_required",
        message:
          "Admin auth gate is visible. Provide SPACEGATE_ADMIN_STORAGE_STATE to capture authenticated Admin screens.",
        baseURL: testInfo.project.use.baseURL,
        screenshot: shotPath,
        consoleEvents,
        requestFailures,
        pageErrors,
      };
      await fs.writeFile(
        testInfo.outputPath("visual-summary.json"),
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
      await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
      await page.waitForTimeout(500);
      const shotPath = testInfo.outputPath(`${slug(screen.key)}.png`);
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
      baseURL: testInfo.project.use.baseURL,
      screens: screenReports,
      consoleEvents,
      requestFailures,
      pageErrors,
    };
    await fs.writeFile(
      testInfo.outputPath("visual-summary.json"),
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
