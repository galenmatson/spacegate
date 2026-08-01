import assert from "node:assert/strict";
import test from "node:test";

const { validateMapPreflight } = await import("./globalSetup.js");

test("map preflight accepts matching API and four-radius tile identities", async () => {
  assert.doesNotThrow(() => validateMapPreflight(
    { status: "ok", build_id: "candidate" },
    {
      build_id: "candidate",
      manifests: {
        "100": "a", "250": "b", "500": "c", "1000": "d",
      },
    },
  ));
});

test("map preflight rejects mismatched API and tile builds", async () => {
  assert.throws(
    () => validateMapPreflight(
      { status: "ok", build_id: "api-build" },
      {
        build_id: "tile-build",
        manifests: {
          "100": "a", "250": "b", "500": "c", "1000": "d",
        },
      },
    ),
    /build mismatch/,
  );
});
