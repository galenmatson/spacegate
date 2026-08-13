import { defineConfig, devices } from "@playwright/test";
import baseConfig from "./playwright.map.config.js";


export default defineConfig({
  ...baseConfig,
  projects: [
    {
      name: "desktop-4k",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 3840, height: 2160 },
      },
    },
    {
      name: "desktop-ultrawide",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 3440, height: 1440 },
      },
    },
  ],
});
