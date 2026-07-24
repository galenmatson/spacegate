import { expect, test } from "@playwright/test";


test.describe("runtime capacity correctness preflight", () => {
  test("TIC and TOI searches preserve exact evidence outcomes", async ({ page }) => {
    const search = async (query) => {
      const response = await page.request.get("/api/v1/systems/search", {
        params: { q: query, sort: "match", limit: "5" },
      });
      return { response, payload: await response.json() };
    };

    for (const query of ["TIC 307210830", "TOI-700", "TOI-700.01"]) {
      const { response, payload } = await search(query);
      expect(response.ok(), query).toBeTruthy();
      expect(payload.query_resolution?.match_status, query).toBe("exact_match");
      expect(payload.query_resolution?.resolution_status, query).toBe("accepted");
      expect(payload.items, query).toHaveLength(1);
    }

    for (const [query, status] of [
      ["TIC 150320610", "missing"],
      ["TOI-6725.01", "missing"],
      ["TIC 101462", "ambiguous"],
    ]) {
      const { response, payload } = await search(query);
      expect(response.ok(), query).toBeTruthy();
      expect(payload.query_resolution?.match_status, query).toBe("exact_no_match");
      expect(payload.query_resolution?.resolution_status, query).toBe(status);
      expect(payload.query_resolution?.deferred, query).toBeTruthy();
      expect(payload.items, query).toHaveLength(0);
    }

    const unknown = await search("TIC 999999999999");
    expect(unknown.response.ok()).toBeTruthy();
    expect(unknown.payload.query_resolution?.match_status).toBe("exact_no_match");
    expect(unknown.payload.query_resolution?.resolution_status).toBe("not_found");
    expect(unknown.payload.query_resolution?.deferred).toBeFalsy();
    expect(unknown.payload.items).toHaveLength(0);

    const malformed = await page.request.get("/api/v1/systems/search", {
      params: { q: "TIC abc", sort: "match", limit: "5" },
    });
    expect(malformed.status()).toBe(400);
    expect((await malformed.json()).error?.code).toBe("invalid_identifier");

    const fuzzy = await search("Castro");
    expect(fuzzy.response.ok()).toBeTruthy();
    expect(fuzzy.payload.query_resolution).toBeNull();
    expect(fuzzy.payload.items.length).toBeGreaterThan(0);
  });

  test("Star Search never renders unrelated cards for a deferred TIC", async ({ page }) => {
    await page.goto("/search?q=TIC%20150320610&sort=match", {
      waitUntil: "domcontentloaded",
    });
    await expect(page.getByRole("heading", { name: "No matches found" })).toBeVisible();
    await expect(page.locator(".result-card")).toHaveCount(0);
  });
});
