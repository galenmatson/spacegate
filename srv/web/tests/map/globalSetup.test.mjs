import assert from "node:assert/strict";
import test from "node:test";

const { default: globalSetup } = await import("./globalSetup.js");

const jsonResponse = (value) =>
  new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

test("map preflight accepts matching API and four-radius tile identities", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url).endsWith("/api/v1/health")) {
      return jsonResponse({ status: "ok", build_id: "candidate" });
    }
    return jsonResponse({
      build_id: "candidate",
      manifests: {
        "100": "a", "250": "b", "500": "c", "1000": "d",
      },
    });
  };
  try {
    await globalSetup();
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("map preflight rejects mismatched API and tile builds", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) =>
    jsonResponse(
      String(url).endsWith("/api/v1/health")
        ? { status: "ok", build_id: "api-build" }
        : {
            build_id: "tile-build",
            manifests: {
              "100": "a", "250": "b", "500": "c", "1000": "d",
            },
          },
    );
  try {
    await assert.rejects(globalSetup(), /build mismatch/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
