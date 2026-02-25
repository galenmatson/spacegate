import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App.jsx";
import "./styles.css";

const THEME_STORAGE_KEY = "spacegate.theme";
const VALID_THEMES = new Set([
  "simple_light",
  "simple_dark",
  "cyberpunk",
  "lcars",
  "mission_control",
  "aurora",
  "retro_90s",
  "deep_space_minimal",
]);
const THEME_ALIASES = {
  light: "simple_light",
  midnight: "simple_dark",
  mission: "mission_control",
};

function normalizeThemeId(raw) {
  const key = String(raw || "").trim().toLowerCase();
  const mapped = THEME_ALIASES[key] || key;
  return VALID_THEMES.has(mapped) ? mapped : "";
}

function bootstrapThemeAttribute() {
  let theme = "simple_light";
  try {
    const stored = normalizeThemeId(window.localStorage.getItem(THEME_STORAGE_KEY));
    if (stored) {
      theme = stored;
    } else if (window.matchMedia?.("(prefers-color-scheme: dark)")?.matches) {
      theme = "simple_dark";
    }
  } catch (_) {
    if (window.matchMedia?.("(prefers-color-scheme: dark)")?.matches) {
      theme = "simple_dark";
    }
  }
  document.documentElement.setAttribute("data-theme", theme);
}

bootstrapThemeAttribute();

const root = createRoot(document.getElementById("root"));
root.render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>
);
