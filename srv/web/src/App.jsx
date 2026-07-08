import React, { useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Link, Route, Routes, useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { fetchHealth, fetchSpectralMix, fetchSystemDetail, fetchSystems } from "./api.js";
import { mapExploreHrefForSystem } from "./mapReturnState.js";
import { StellarClassChips, stellarClassTokensFromRecord, stellarClassTokensFromSystem } from "./stellarClassTags.jsx";

const StarMapPage = React.lazy(() => import("./StarMapPage.jsx"));
const SystemPreviewPanel = React.lazy(() => import("./SystemPreviewPanel.jsx"));

const spectralOptions = ["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D"];
const SPECTRAL_NON_TEMP_OPTIONS = new Set(["D"]);
const THEME_STORAGE_KEY = "spacegate.theme";
const THEME_OPTIONS = [
  { id: "simple_light", label: "Simple Light" },
  { id: "simple_dark", label: "Simple Dark" },
  { id: "cyberpunk", label: "Cyberpunk" },
  { id: "lcars", label: "Enterprise" },
  { id: "mission_control", label: "Mission Control" },
  { id: "aurora", label: "Aurora" },
  { id: "retro_90s", label: "Geocities" },
  { id: "deep_space_minimal", label: "Deep Space Minimal" },
];
const THEME_IDS = new Set(THEME_OPTIONS.map((item) => item.id));
const THEME_ALIASES = {
  light: "simple_light",
  midnight: "simple_dark",
  mission: "mission_control",
};
const triStateOptions = [
  { value: "", label: "Any" },
  { value: "true", label: "Yes" },
  { value: "false", label: "No" },
];
const filterLimits = {
  distance: { min: 0, max: 1000, step: 1, integer: false },
  stars: { min: 0, max: 12, step: 1, integer: true },
  planets: { min: 0, max: 20, step: 1, integer: true },
  coolness: { min: 0, max: 40, step: 0.1, integer: false },
};
const FILTER_PRESETS = [
  { id: "nearby", label: "Nearby", filters: { sort: "distance", minDist: 0, maxDist: 60 } },
  { id: "planet_rich", label: "Planets", filters: { sort: "coolness", minPlanetCount: 1 } },
  { id: "habitable_like", label: "Habitability", filters: { sort: "coolness", hasHabitableMode: "true", maxDist: 200 } },
  { id: "high_coolness", label: "Cool", filters: { sort: "coolness", minCoolnessScore: 20 } },
];
const SPECTRAL_CLASS_PIE_COLORS = {
  O: "#6aa9ff",
  B: "#8cc8ff",
  A: "#d7e9ff",
  F: "#fff2b5",
  G: "#ffd86b",
  K: "#ffb36a",
  M: "#f06a55",
  L: "#cf6b57",
  T: "#8f6bc7",
  Y: "#6fc7d8",
  D: "#c8d2de",
  UNKNOWN: "#7f8ea3",
};
const SPECTRAL_CLASS_TEMP_RANGES = {
  O: [30000, 50000],
  B: [10000, 30000],
  A: [7500, 10000],
  F: [6000, 7500],
  G: [5200, 6000],
  K: [3700, 5200],
  M: [2400, 3700],
  L: [1300, 2400],
  T: [700, 1300],
  Y: [250, 700],
};
const SPECTRAL_TEMP_CLASS_OPTIONS = spectralOptions.filter((token) => !SPECTRAL_NON_TEMP_OPTIONS.has(token));
const SPECTRAL_TEMP_MIN_K = Math.min(...SPECTRAL_TEMP_CLASS_OPTIONS.map((token) => SPECTRAL_CLASS_TEMP_RANGES[token][0]));
const SPECTRAL_TEMP_MAX_K = Math.max(...SPECTRAL_TEMP_CLASS_OPTIONS.map((token) => SPECTRAL_CLASS_TEMP_RANGES[token][1]));
const SPECTRAL_TEMP_LOG_MIN = Math.log10(SPECTRAL_TEMP_MIN_K);
const SPECTRAL_TEMP_LOG_MAX = Math.log10(SPECTRAL_TEMP_MAX_K);
const SPECTRAL_TEMP_SLIDER_MAX = 1000;
const SPECTRAL_CLASS_INFO = {
  O: { sentence: "Very hot blue stars with intense ultraviolet output and short lifetimes.", tempRangeK: [30000, 50000] },
  B: { sentence: "Hot blue-white stars that are luminous and relatively short-lived.", tempRangeK: [10000, 30000] },
  A: { sentence: "White stars with strong hydrogen lines and comparatively high luminosity.", tempRangeK: [7500, 10000] },
  F: { sentence: "Yellow-white stars slightly hotter and more massive than the Sun.", tempRangeK: [6000, 7500] },
  G: { sentence: "Yellow dwarf stars like the Sun, often stable over long timescales.", tempRangeK: [5200, 6000] },
  K: { sentence: "Orange stars that are cooler than the Sun and often long-lived.", tempRangeK: [3700, 5200] },
  M: { sentence: "Cool red dwarfs, the most common stellar class in the Milky Way.", tempRangeK: [2400, 3700] },
  L: { sentence: "Very cool red-brown objects at the star-brown dwarf boundary.", tempRangeK: [1300, 2400] },
  D: { sentence: "Degenerate white dwarfs: compact stellar remnants with no active core fusion." },
  T: { sentence: "Cool methane-rich brown dwarfs with very low thermal emission.", tempRangeK: [700, 1300] },
  Y: { sentence: "Ultra-cool brown dwarfs approaching giant-planet temperatures.", tempRangeK: [250, 700] },
};
const ThemeContext = React.createContext({
  theme: "simple_light",
  setTheme: () => {},
  options: THEME_OPTIONS,
});
const LCARS_FALLBACK_CHIPS = ["Sol", "Sirius", "Alpha Centauri", "Vega"];
const LCARS_FALLBACK_GAIA = [
  "5853498713190528",
  "4472832130949120",
  "5167429001440896",
  "3321984567003136",
  "2144471203989888",
  "783120094455232",
  "1285536706225792",
  "691254228110656",
  "1782230945005568",
  "905511238822400",
  "2441189304725632",
  "4029190001419520",
  "349117264800128",
  "1927500734413568",
  "705128390112384",
  "4588032202741760",
  "2693301958305408",
  "1500139114687744",
  "96022341011200",
  "3175520911024512",
  "4201399047744",
  "632814901220352",
  "2184403905440640",
  "77125290018816",
];
const LCARS_HISTORY_STORAGE_KEY = "spacegate.lcars.history";
const LCARS_HISTORY_LIMIT = 32;
const LCARS_LEFT_DECORATIVE_CHIP_COUNT = 2;
const LCARS_RIGHT_CHIP_COUNT = 4;
const LCARS_TEXT_SLOTS_PER_LINE = 5;
const SEARCH_RESULT_PREVIEW_POOL_SIZE = 4;
const SEARCH_RESULT_PREVIEW_CACHE_LIMIT = 160;
const LCARS_TEXT_ROW_COUNT = 5;
const LCARS_TEXT_MAX_SLOTS = LCARS_TEXT_SLOTS_PER_LINE * LCARS_TEXT_ROW_COUNT;
const GLOBAL_SEARCH_INPUT_SELECTOR = "input[data-global-search-input='true']";
const APP_DISPLAY_NAME = "CoolStars";
const HEADER_HELP_LINK = "/help";
const HEADER_ABOUT_LINK = "/about";
const HEADER_DATA_LINK = "/data";
const HEADER_SPONSOR_LINK = "https://github.com/sponsors/galenmatson";
const HEADER_SOURCE_LINK = "https://github.com/galenmatson/spacegate";
const HEADER_LINKS = [
  { label: "HELP", href: HEADER_HELP_LINK, title: "How to use Coolstars", external: false },
  { label: "ABT", href: HEADER_ABOUT_LINK, title: "About this site", external: false },
  { label: "MAP", href: "/map", title: "3D local star map", external: false },
  { label: "SPT", href: HEADER_SPONSOR_LINK, title: "Support this project", external: true },
  { label: "SRC", href: HEADER_SOURCE_LINK, title: "Source code", external: true },
  { label: "DATA", href: HEADER_DATA_LINK, title: "Source data", external: false },
];
const MARKDOWN_CONTENT = import.meta.glob("../content/*.md", {
  eager: true,
  import: "default",
  query: "?raw",
});
const ABOUT_MARKDOWN = typeof MARKDOWN_CONTENT["../content/about.md"] === "string"
  ? MARKDOWN_CONTENT["../content/about.md"]
  : `# About Spacegate

About content is not available in this checkout.
`;
const HELP_MARKDOWN = typeof MARKDOWN_CONTENT["../content/help.md"] === "string"
  ? MARKDOWN_CONTENT["../content/help.md"]
  : `# Coolstars Help

Help content is not available in this checkout.
`;
const DATA_MARKDOWN = typeof MARKDOWN_CONTENT["../content/sources.md"] === "string"
  ? MARKDOWN_CONTENT["../content/sources.md"]
  : `# Spacegate Source Data Overview

Source data content is not available in this checkout.
`;

function isEditableTarget(target) {
  if (!(target instanceof Element)) {
    return false;
  }
  return Boolean(target.closest("input, textarea, select, [contenteditable], [role='textbox']"));
}

function focusGlobalSearchInput(selectText = true) {
  if (typeof document === "undefined") {
    return false;
  }
  const input = document.querySelector(GLOBAL_SEARCH_INPUT_SELECTOR);
  if (!(input instanceof HTMLInputElement)) {
    return false;
  }
  input.focus({ preventScroll: true });
  if (selectText) {
    input.select();
  }
  return true;
}

function normalizeThemeId(raw) {
  const key = String(raw || "").trim().toLowerCase();
  const mapped = THEME_ALIASES[key] || key;
  return THEME_IDS.has(mapped) ? mapped : "";
}

function detectSystemTheme() {
  if (typeof window === "undefined") {
    return "simple_light";
  }
  return window.matchMedia?.("(prefers-color-scheme: dark)")?.matches ? "simple_dark" : "simple_light";
}

function resolveInitialTheme() {
  if (typeof window === "undefined") {
    return "simple_light";
  }
  const rootTheme = normalizeThemeId(document.documentElement.getAttribute("data-theme"));
  if (rootTheme) {
    return rootTheme;
  }
  try {
    const stored = normalizeThemeId(window.localStorage.getItem(THEME_STORAGE_KEY));
    if (stored) {
      return stored;
    }
  } catch (_) {
    // Ignore storage access failures and use system fallback.
  }
  return detectSystemTheme();
}

function useThemeControls() {
  return React.useContext(ThemeContext);
}

function presetLabelForTheme(preset, theme) {
  if (preset?.id === "habitable_like" && (theme === "cyberpunk" || theme === "retro_90s")) {
    return "Hab Zone";
  }
  return preset?.label || "";
}

function MarkdownContent({ markdown }) {
  return (
    <div className="markdown-content">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h1: ({ children }) => <h2>{children}</h2>,
          h2: ({ children }) => <h3>{children}</h3>,
          h3: ({ children }) => <h4>{children}</h4>,
          a: ({ href, children }) => {
            const url = String(href || "");
            const external = /^https?:\/\//i.test(url);
            return (
              <a
                href={url}
                target={external ? "_blank" : undefined}
                rel={external ? "noreferrer" : undefined}
              >
                {children}
              </a>
            );
          },
        }}
      >
        {markdown}
      </ReactMarkdown>
    </div>
  );
}

function formatBuildVersionLabel(buildId) {
  const raw = String(buildId || "").trim();
  if (!raw) {
    return "";
  }
  return `DB ${raw}`;
}

function systemDisplayName(system) {
  const display = String(system?.display_name || "").trim();
  if (display) {
    return display;
  }
  const fallback = String(system?.system_name || "").trim();
  return fallback || "";
}

function starDisplayName(star) {
  const display = String(star?.display_name || "").trim();
  if (display) {
    return display;
  }
  const fallback = String(star?.star_name || "").trim();
  return fallback || "";
}

function HeaderNavLinks({ className, linkClassName, buildId = "", includeLabels = null }) {
  const buildLabel = formatBuildVersionLabel(buildId);
  const allowed = Array.isArray(includeLabels) ? new Set(includeLabels) : null;
  const items = allowed ? HEADER_LINKS.filter((item) => allowed.has(item.label)) : HEADER_LINKS;
  return (
    <span className={className} aria-label="Site links">
      {items.map((item) => (
        item.external ? (
          <a
            key={item.label}
            href={item.href}
            className={linkClassName}
            target="_blank"
            rel="noreferrer"
            title={item.title}
          >
            {item.label}
          </a>
        ) : (
          item.label === "DATA" ? (
            <span key={item.label} className="header-data-link-group" title={buildLabel || item.title}>
              <Link
                to={item.href}
                className={linkClassName}
                title={item.title}
              >
                {item.label}
              </Link>
              {buildLabel ? <span className="header-build-badge">{buildLabel}</span> : null}
            </span>
          ) : (
            <Link
              key={item.label}
              to={item.href}
              className={linkClassName}
              title={item.title}
            >
              {item.label}
            </Link>
          )
        )
      ))}
    </span>
  );
}

function LcarsDataRail({ buildId = "" }) {
  const buildLabel = formatBuildVersionLabel(buildId);
  return (
    <div className="lcars-data-rail" aria-label="Data links">
      <span className="lcars-data-link-group">
        <Link to={HEADER_DATA_LINK} className="lcars-data-link" title="Source data">
          DATA
        </Link>
        {buildLabel ? <span className="lcars-data-build">{buildLabel}</span> : null}
      </span>
    </div>
  );
}

function LcarsUtilityRail() {
  return (
    <HeaderNavLinks
      className="lcars-utility-rail"
      linkClassName="lcars-utility-link"
      includeLabels={["HELP", "ABT", "SPT", "SRC"]}
    />
  );
}

function pickRandomSystems(items, count) {
  const systems = Array.from(
    new Map(
      (Array.isArray(items) ? items : [])
        .map((item) => ({
          system_id: item?.system_id,
          system_name: systemDisplayName(item),
        }))
        .filter((entry) => entry.system_id !== null && entry.system_id !== undefined && entry.system_name)
        .map((entry) => [String(entry.system_id), entry]),
    ).values(),
  );
  for (let i = systems.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [systems[i], systems[j]] = [systems[j], systems[i]];
  }
  const picked = systems.slice(0, count);
  if (picked.length < count) {
    return [
      ...picked,
      ...LCARS_FALLBACK_CHIPS.map((name) => ({ system_id: null, system_name: name })),
    ].slice(0, count);
  }
  return picked;
}

function pickRandomGaiaEntries(items, count) {
  const gaiaEntries = Array.from(
    new Set(
      (Array.isArray(items) ? items : [])
        .map((item) => {
          if (item?.gaia_id_text) {
            return {
              gaia: String(item.gaia_id_text).trim(),
              system_id: item?.system_id,
              system_name: systemDisplayName(item),
            };
          }
          if (item?.gaia_id !== null && item?.gaia_id !== undefined) {
            return {
              gaia: String(item.gaia_id).trim(),
              system_id: item?.system_id,
              system_name: systemDisplayName(item),
            };
          }
          const stable = String(item?.stable_object_key || "");
          const match = stable.match(/(?:^|:)gaia:(\d+)/i);
          return match?.[1]
              ? {
              gaia: String(match[1]).trim(),
              system_id: item?.system_id,
              system_name: systemDisplayName(item),
            }
            : null;
        })
        .filter((entry) => entry && /^\d{6,}$/.test(entry.gaia))
        .map((entry) => `${entry.gaia}|${entry.system_id ?? ""}|${entry.system_name}`),
    ),
  ).map((value) => {
    const [gaia, systemIdRaw, ...nameRest] = value.split("|");
    const system_id = systemIdRaw ? Number(systemIdRaw) : null;
    return {
      gaia,
      system_id: Number.isFinite(system_id) ? system_id : null,
      system_name: nameRest.join("|"),
    };
  });
  for (let i = gaiaEntries.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [gaiaEntries[i], gaiaEntries[j]] = [gaiaEntries[j], gaiaEntries[i]];
  }
  const selected = gaiaEntries.slice(0, count);
  if (selected.length >= count) {
    return selected;
  }
  const padded = [...selected];
  while (padded.length < count) {
    padded.push({
      gaia: LCARS_FALLBACK_GAIA[padded.length % LCARS_FALLBACK_GAIA.length],
      system_id: null,
      system_name: "",
    });
  }
  return padded;
}

function loadLcarsHistory() {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const raw = window.localStorage.getItem(LCARS_HISTORY_STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((entry) => ({
        system_id: entry?.system_id,
        system_name: String(entry?.system_name || "").trim(),
      }))
      .filter((entry) => entry.system_id !== null && entry.system_id !== undefined && entry.system_name)
      .slice(0, LCARS_HISTORY_LIMIT);
  } catch (_) {
    return [];
  }
}

function saveLcarsHistory(entries) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(LCARS_HISTORY_STORAGE_KEY, JSON.stringify(entries.slice(0, LCARS_HISTORY_LIMIT)));
  } catch (_) {
    // Ignore storage write failures.
  }
}

function updateLcarsHistoryWithSystem(system) {
  const systemId = system?.system_id;
  const systemName = systemDisplayName(system);
  if (systemId === null || systemId === undefined || !systemName) {
    return;
  }
  const existing = loadLcarsHistory();
  const updated = [
    { system_id: systemId, system_name: systemName },
    ...existing.filter((entry) => String(entry.system_id) !== String(systemId)),
  ].slice(0, LCARS_HISTORY_LIMIT);
  saveLcarsHistory(updated);
}

function clampNumber(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function parseRangeParam(searchParams, key, fallback, min, max, integer = false) {
  const raw = searchParams.get(key);
  if (raw === null || raw === "") {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const normalized = integer ? Math.round(parsed) : parsed;
  return clampNumber(normalized, min, max);
}

function parseSpectralTokens(rawValue) {
  return Array.from(
    new Set(
      String(rawValue || "")
        .split(",")
        .map((item) => item.trim().toUpperCase())
        .filter((item) => spectralOptions.includes(item)),
    ),
  );
}

function spectralClassesForTemperatureRange(minTempK, maxTempK) {
  const low = Math.min(Number(minTempK), Number(maxTempK));
  const high = Math.max(Number(minTempK), Number(maxTempK));
  const temperatureEligible = SPECTRAL_TEMP_CLASS_OPTIONS.filter((token) => {
    const [classMin, classMax] = SPECTRAL_CLASS_TEMP_RANGES[token] || [SPECTRAL_TEMP_MIN_K, SPECTRAL_TEMP_MAX_K];
    return classMax >= low && classMin <= high;
  });
  const nonTempAlwaysEligible = spectralOptions.filter((token) => SPECTRAL_NON_TEMP_OPTIONS.has(token));
  return [...temperatureEligible, ...nonTempAlwaysEligible];
}

function spectralTempToSliderPosition(tempK) {
  const safe = clampNumber(Number(tempK), SPECTRAL_TEMP_MIN_K, SPECTRAL_TEMP_MAX_K);
  const logValue = Math.log10(safe);
  const ratio = (logValue - SPECTRAL_TEMP_LOG_MIN) / (SPECTRAL_TEMP_LOG_MAX - SPECTRAL_TEMP_LOG_MIN);
  return clampNumber(Math.round(ratio * SPECTRAL_TEMP_SLIDER_MAX), 0, SPECTRAL_TEMP_SLIDER_MAX);
}

function sliderPositionToSpectralTemp(position) {
  const safe = clampNumber(Number(position), 0, SPECTRAL_TEMP_SLIDER_MAX);
  const ratio = safe / SPECTRAL_TEMP_SLIDER_MAX;
  const exponent = SPECTRAL_TEMP_LOG_MIN + ratio * (SPECTRAL_TEMP_LOG_MAX - SPECTRAL_TEMP_LOG_MIN);
  return clampNumber(Math.round(10 ** exponent), SPECTRAL_TEMP_MIN_K, SPECTRAL_TEMP_MAX_K);
}

function TriStateToggle({ label, value, onChange }) {
  return (
    <div className="field tri-state-field">
      <span>{label}</span>
      <div className="tri-state" role="radiogroup" aria-label={label}>
        {triStateOptions.map((option) => (
          <button
            type="button"
            key={`${label}-${option.value || "any"}`}
            role="radio"
            aria-checked={value === option.value}
            className={`tri-state-btn ${value === option.value ? "active" : ""}`}
            onClick={() => onChange(option.value)}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function CompactRangeControl({
  label,
  minValue,
  maxValue,
  minLimit,
  maxLimit,
  step,
  integer,
  unit = "",
  onChangeMin,
  onChangeMax,
}) {
  const valueMin = clampNumber(Math.min(minValue, maxValue), minLimit, maxLimit);
  const valueMax = clampNumber(Math.max(minValue, maxValue), minLimit, maxLimit);
  const span = maxLimit - minLimit;
  const minPercent = span > 0 ? ((valueMin - minLimit) / span) * 100 : 0;
  const maxPercent = span > 0 ? ((valueMax - minLimit) / span) * 100 : 100;
  const fillLeft = clampNumber(minPercent, 0, 100);
  const fillWidth = clampNumber(maxPercent - minPercent, 0, 100);

  return (
    <div className="field compact-range">
      <div className="compact-range-head">
        <span>{label}</span>
      </div>
      <div className="compact-range-body">
        <div className="compact-range-slider" role="group" aria-label={`${label} range slider`}>
          <div className="compact-range-track-window" aria-hidden="true">
            <div className="compact-range-track" />
            <div
              className="compact-range-track-fill"
              style={{ left: `${fillLeft}%`, width: `${fillWidth}%` }}
            />
          </div>
          <input
            type="range"
            className="dual-range dual-range-min"
            min={minLimit}
            max={maxLimit}
            step={step}
            value={valueMin}
            aria-label={`${label} minimum slider`}
            onChange={(event) => {
              const next = clampNumber(Number(event.target.value), minLimit, valueMax);
              onChangeMin(integer ? Math.round(next) : next);
            }}
          />
          <input
            type="range"
            className="dual-range dual-range-max"
            min={minLimit}
            max={maxLimit}
            step={step}
            value={valueMax}
            aria-label={`${label} maximum slider`}
            onChange={(event) => {
              const next = clampNumber(Number(event.target.value), valueMin, maxLimit);
              onChangeMax(integer ? Math.round(next) : next);
            }}
          />
        </div>

        <div className="compact-range-guides" aria-hidden="true">
          <span>Min</span>
          <span>Max</span>
        </div>

        <div className="compact-range-inputs">
          <label className="compact-bound compact-bound-inline">
            <span className="sr-only">Min</span>
            <input
              type="number"
              min={minLimit}
              max={maxLimit}
              step={step}
              value={valueMin}
              aria-label={`${label} minimum value`}
              onChange={(event) => {
                const parsed = Number(event.target.value);
                if (!Number.isFinite(parsed)) {
                  return;
                }
                const next = clampNumber(parsed, minLimit, valueMax);
                onChangeMin(integer ? Math.round(next) : next);
              }}
            />
          </label>
          <label className="compact-bound compact-bound-inline">
            <span className="sr-only">Max</span>
            <input
              type="number"
              min={minLimit}
              max={maxLimit}
              step={step}
              value={valueMax}
              aria-label={`${label} maximum value`}
              onChange={(event) => {
                const parsed = Number(event.target.value);
                if (!Number.isFinite(parsed)) {
                  return;
                }
                const next = clampNumber(parsed, valueMin, maxLimit);
                onChangeMax(integer ? Math.round(next) : next);
              }}
            />
          </label>
        </div>
      </div>
    </div>
  );
}

function MapStyleRangeControl({
  label,
  minValue,
  maxValue,
  minLimit,
  maxLimit,
  step = 1,
  integer = false,
  format = (value) => formatNumber(value, integer ? 0 : 1),
  onChangeMin,
  onChangeMax,
}) {
  const safeMinLimit = Number.isFinite(Number(minLimit)) ? Number(minLimit) : 0;
  const safeMaxLimit = Math.max(safeMinLimit + Number(step || 1), Number.isFinite(Number(maxLimit)) ? Number(maxLimit) : safeMinLimit + 1);
  const valueMin = clampNumber(Math.min(Number(minValue), Number(maxValue)), safeMinLimit, safeMaxLimit);
  const valueMax = clampNumber(Math.max(Number(minValue), Number(maxValue)), safeMinLimit, safeMaxLimit);
  const span = safeMaxLimit - safeMinLimit || 1;
  const leftPct = ((valueMin - safeMinLimit) / span) * 100;
  const rightPct = 100 - ((valueMax - safeMinLimit) / span) * 100;
  const normalize = (value) => (integer ? Math.round(Number(value)) : Number(value));
  return (
    <div className="map-search-range">
      <div className="map-search-range-head">
        <span>{label}</span>
        <strong>{format(valueMin)} - {format(valueMax)}</strong>
      </div>
      <div className="map-search-range-track" style={{ "--range-left": `${leftPct}%`, "--range-right": `${rightPct}%` }}>
        <input
          type="range"
          min={safeMinLimit}
          max={safeMaxLimit}
          step={step}
          value={valueMin}
          onChange={(event) => onChangeMin(normalize(clampNumber(Number(event.target.value), safeMinLimit, valueMax)))}
          aria-label={`${label} minimum`}
        />
        <input
          type="range"
          min={safeMinLimit}
          max={safeMaxLimit}
          step={step}
          value={valueMax}
          onChange={(event) => onChangeMax(normalize(clampNumber(Number(event.target.value), valueMin, safeMaxLimit)))}
          aria-label={`${label} maximum`}
        />
      </div>
    </div>
  );
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  if (typeof value === "number") {
    return value.toLocaleString(undefined, { maximumFractionDigits: digits });
  }
  return String(value);
}

function formatHumanLargeCount(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Unknown";
  }
  const abs = Math.abs(numeric);
  if (abs >= 1_000_000_000) {
    return `${formatNumber(numeric / 1_000_000_000, abs >= 10_000_000_000 ? 0 : 1)} billion`;
  }
  if (abs >= 1_000_000) {
    return `${formatNumber(numeric / 1_000_000, abs >= 10_000_000 ? 0 : 1)} million`;
  }
  if (abs >= 1_000) {
    return `${formatNumber(numeric / 1_000, abs >= 10_000 ? 0 : 1)} thousand`;
  }
  return formatNumber(numeric, 0);
}

function formatText(value) {
  if (value === null || value === undefined || value === "") {
    return "Unknown";
  }
  return String(value);
}

function formatAliasSummary(aliases, { exclude = [], limit = 8 } = {}) {
  if (!Array.isArray(aliases) || aliases.length === 0) {
    return "";
  }
  const excluded = new Set(
    (exclude || [])
      .map((value) => String(value || "").trim().toLowerCase())
      .filter(Boolean),
  );
  const items = [];
  aliases.forEach((row) => {
    const raw = String(row?.alias_raw || "").trim();
    if (!raw) {
      return;
    }
    if (excluded.has(raw.toLowerCase())) {
      return;
    }
    if (items.includes(raw)) {
      return;
    }
    items.push(raw);
  });
  if (items.length === 0) {
    return "";
  }
  const shown = items.slice(0, Math.max(1, limit));
  if (shown.length < items.length) {
    shown.push(`+${items.length - shown.length} more`);
  }
  return shown.join(" · ");
}

function formatCoordinate(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  return value.toFixed(4);
}

function formatPeriodDaysWithYears(periodDays) {
  if (periodDays === null || periodDays === undefined || Number.isNaN(periodDays)) {
    return "Unknown";
  }
  const days = Number(periodDays);
  if (!Number.isFinite(days) || days <= 0 || Math.abs(days) >= 1e20) {
    return "Unknown";
  }
  const dayLabel = `${formatNumber(days, 2)} d`;
  if (days <= 365.25) {
    return dayLabel;
  }
  const years = days / 365.25;
  const yearDigits = years >= 100 ? 1 : 2;
  return `${dayLabel} (${formatNumber(years, yearDigits)} y)`;
}

function formatOrbitSummary({ periodDays, semiMajorAxisAu, eccentricity, inclinationDeg }) {
  const numericPeriod = Number(periodDays);
  const numericSemiMajorAxis = Number(semiMajorAxisAu);
  const numericEccentricity = Number(eccentricity);
  const numericInclination = Number(inclinationDeg);
  const hasPeriod = Number.isFinite(numericPeriod);
  const hasSemiMajorAxis = Number.isFinite(numericSemiMajorAxis);
  const hasEccentricity = Number.isFinite(numericEccentricity);
  const hasInclination = Number.isFinite(numericInclination);
  const unboundTrajectory =
    (hasEccentricity && numericEccentricity >= 1.0) || (hasSemiMajorAxis && numericSemiMajorAxis <= 0.0);

  const bits = [];
  if (unboundTrajectory) {
    bits.push("Trajectory unbound");
  }
  if (!unboundTrajectory && hasPeriod) {
    const periodLabel = formatPeriodDaysWithYears(numericPeriod);
    if (periodLabel !== "Unknown") {
      bits.push(`P ${periodLabel}`);
    }
  }
  if (!unboundTrajectory && hasSemiMajorAxis) {
    bits.push(`a ${formatNumber(numericSemiMajorAxis, 4)} AU`);
  }
  if (hasEccentricity) {
    bits.push(`e ${formatNumber(numericEccentricity, 4)}`);
  }
  if (hasInclination) {
    bits.push(`i ${formatNumber(numericInclination, 3)} deg`);
  }
  if (bits.length === 0) {
    return "Orbit parameters unavailable";
  }
  return bits.join(" · ");
}

function formatConfidence(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  return Number(value).toFixed(2);
}

function formatKelvin(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  return `${Number(value).toLocaleString(undefined, { maximumFractionDigits: digits })} K`;
}

function formatCoolnessPoints(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return null;
  }
  return `${(Number(value) * 100).toFixed(2)} pts`;
}

function distanceLyToPc(distanceLy) {
  if (distanceLy === null || distanceLy === undefined || Number.isNaN(distanceLy)) {
    return null;
  }
  return Number(distanceLy) / 3.26156;
}

function parallaxMasFromDistanceLy(distanceLy) {
  const distancePc = distanceLyToPc(distanceLy);
  if (distancePc === null || distancePc <= 0) {
    return null;
  }
  return 1000 / distancePc;
}

function spectralPieColor(rawClass) {
  const key = String(rawClass || "").trim().toUpperCase();
  return SPECTRAL_CLASS_PIE_COLORS[key] || SPECTRAL_CLASS_PIE_COLORS.UNKNOWN;
}

function SidebarSpectralMixCard({
  mix,
  loading = false,
  error = "",
  collapsed = false,
}) {
  const rows = Array.isArray(mix?.rows)
    ? mix.rows
      .map((row) => ({
        spectralClass: String(row?.spectral_class || "unknown").trim().toUpperCase(),
        starCount: Number(row?.star_count || 0),
      }))
      .filter((row) => Number.isFinite(row.starCount) && row.starCount > 0)
    : [];
  const totalStars = Number(mix?.total_stars || 0) || rows.reduce((sum, row) => sum + row.starCount, 0);
  const ringRows = rows
    .slice()
    .sort((a, b) => b.starCount - a.starCount)
    .slice(0, 10);
  const shownRows = rows
    .slice()
    .sort((a, b) => b.starCount - a.starCount)
    .slice(0, 8);

  let cursor = 0;
  const gradientParts = ringRows.map((row) => {
    const pct = totalStars > 0 ? (row.starCount / totalStars) * 100 : 0;
    const next = Math.min(100, cursor + pct);
    const part = `${spectralPieColor(row.spectralClass)} ${cursor.toFixed(2)}% ${next.toFixed(2)}%`;
    cursor = next;
    return part;
  });
  if (cursor < 100) {
    gradientParts.push(`${SPECTRAL_CLASS_PIE_COLORS.UNKNOWN} ${cursor.toFixed(2)}% 100%`);
  }

  return (
    <section className={`panel filters-spectrum-card ${collapsed ? "is-collapsed" : ""}`.trim()}>
      <div className="filters-spectrum-head">
        <h4>Stellar Mix</h4>
        <small>{totalStars > 0 ? `${formatNumber(totalStars, 0)} stars` : "No spectral data"}</small>
      </div>
      {error && !rows.length && (
        <p className="filters-spectrum-note">Spectral mix unavailable right now.</p>
      )}
      {loading && !rows.length && !error && (
        <p className="filters-spectrum-note">Loading spectral mix…</p>
      )}
      {!loading && !rows.length && !error && (
        <p className="filters-spectrum-note">No spectral mix rows returned.</p>
      )}
      {rows.length > 0 && (
        <>
          <div
            className="filters-spectrum-pie"
            style={{ background: `conic-gradient(${gradientParts.join(", ")})` }}
            role="img"
            aria-label="Spectral class composition pie chart"
          />
          <div className="filters-spectrum-legend">
            {shownRows.map((row) => {
              const pct = totalStars > 0 ? (row.starCount / totalStars) * 100 : 0;
              return (
                <div key={`mix-${row.spectralClass}`} className="filters-spectrum-legend-item">
                  <span
                    className="filters-spectrum-dot"
                    style={{ backgroundColor: spectralPieColor(row.spectralClass) }}
                    aria-hidden="true"
                  />
                  <span className="filters-spectrum-class">{row.spectralClass}</span>
                  <span className="filters-spectrum-value">{formatNumber(pct, 1)}%</span>
                </div>
              );
            })}
          </div>
        </>
      )}
    </section>
  );
}

async function copyTextToClipboard(rawValue) {
  const text = String(rawValue ?? "").trim();
  if (!text) {
    return false;
  }
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return true;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(textarea);
  if (!copied) {
    throw new Error("copy_failed");
  }
  return true;
}

function CopyGlyph() {
  return (
    <svg className="copy-glyph" viewBox="0 0 16 16" aria-hidden="true" focusable="false">
      <rect x="6" y="3" width="7" height="9" rx="1.4" fill="none" stroke="currentColor" strokeWidth="1.3" />
      <rect x="3" y="6" width="7" height="9" rx="1.4" fill="none" stroke="currentColor" strokeWidth="1.3" />
    </svg>
  );
}

function CopyButton({
  value,
  label,
  className = "",
  stopPropagation = true,
}) {
  const [status, setStatus] = useState("idle");
  const normalized = value === null || value === undefined ? "" : String(value).trim();
  const canCopy = Boolean(normalized);

  const onCopy = async (event) => {
    if (stopPropagation) {
      event.preventDefault();
      event.stopPropagation();
    }
    if (!canCopy) {
      return;
    }
    try {
      await copyTextToClipboard(normalized);
      setStatus("copied");
      window.setTimeout(() => setStatus("idle"), 1200);
    } catch (_) {
      setStatus("failed");
      window.setTimeout(() => setStatus("idle"), 1500);
    }
  };

  const titleBase = canCopy ? `Copy ${label}` : `${label} unavailable`;
  const title =
    status === "copied"
      ? `${titleBase} (copied)`
      : status === "failed"
        ? `${titleBase} (failed)`
        : titleBase;

  return (
    <button
      type="button"
      className={`copy-btn ${className}`.trim()}
      onClick={onCopy}
      disabled={!canCopy}
      aria-label={titleBase}
      title={title}
    >
      {status === "copied" ? <span className="copy-status">✓</span> : <CopyGlyph />}
    </button>
  );
}

function CatalogIdChip({ label, value, hideWhenMissing = false }) {
  const normalized = value === null || value === undefined ? "" : String(value).trim();
  if (hideWhenMissing && !normalized) {
    return null;
  }
  const display = normalized || "Unknown";

  return (
    <span className="id-chip">
      <span className="id-chip-label">{label}</span>
      <code className="id-chip-value">{display}</code>
      <CopyButton value={normalized} label={`${label} ID`} className="id-copy" />
    </span>
  );
}

function resolvedEntityGaiaId(entity) {
  const key = String(entity?.stable_object_key || "");
  const fromKey = key.match(/(?:^|:)gaia:(\d+)/i);
  if (fromKey?.[1]) {
    return fromKey[1];
  }
  if (entity?.gaia_id_text) {
    return String(entity.gaia_id_text);
  }
  if (entity?.gaia_id !== null && entity?.gaia_id !== undefined) {
    return String(entity.gaia_id);
  }
  return "";
}

function resolvedSystemGaiaId(system) {
  return resolvedEntityGaiaId(system);
}

function buildSystemCatalogIds(system) {
  const entries = [
    { label: "HIP", value: system?.hip_id_text ?? system?.hip_id },
    { label: "HD", value: system?.hd_id_text ?? system?.hd_id },
    { label: "Gaia", value: resolvedSystemGaiaId(system) },
  ];
  return entries
    .map((entry) => ({
      ...entry,
      value: entry.value === null || entry.value === undefined ? "" : String(entry.value).trim(),
    }))
    .filter((entry) => entry.value !== "");
}

function MetricChip({ label, value, tooltipLines = [] }) {
  const isLazyTooltip = typeof tooltipLines === "function";
  const [lazyTooltipLines, setLazyTooltipLines] = useState(null);
  const lines = isLazyTooltip
    ? (Array.isArray(lazyTooltipLines) ? lazyTooltipLines : [])
    : (Array.isArray(tooltipLines) ? tooltipLines.filter((line) => Boolean(line)) : []);
  const loadTooltip = () => {
    if (!isLazyTooltip || lazyTooltipLines !== null) {
      return;
    }
    const computed = tooltipLines();
    if (!Array.isArray(computed)) {
      setLazyTooltipLines([]);
      return;
    }
    setLazyTooltipLines(computed.filter((line) => Boolean(line)));
  };
  return (
    <div className="metric-chip" tabIndex={0} onMouseEnter={loadTooltip} onFocus={loadTooltip} onTouchStart={loadTooltip}>
      <span>{label}</span>
      <strong>{value}</strong>
      {lines.length > 0 && (
        <div className="metric-tooltip" role="tooltip">
          {lines.map((line) => (
            <div key={`${label}-${line}`}>{line}</div>
          ))}
        </div>
      )}
    </div>
  );
}

function buildCoolnessTooltipLines(system) {
  const lines = [
    `Rank: ${(system.coolness_rank !== null && system.coolness_rank !== undefined) ? `#${formatNumber(system.coolness_rank, 0)}` : "Unranked"}`,
    `Total score: ${formatNumber(system.coolness_score, 2)}`,
  ];
  const components = [
    ["Luminosity", system.coolness_score_luminosity],
    ["Proper motion", system.coolness_score_proper_motion],
    ["Multiplicity", system.coolness_score_multiplicity],
    ["Nice planets", system.coolness_score_nice_planets],
    ["Weird planets", system.coolness_score_weird_planets],
    ["Proximity", system.coolness_score_proximity],
    ["System complexity", system.coolness_score_system_complexity],
    ["Exotic stars", system.coolness_score_exotic_star],
  ];
  components.forEach(([label, raw]) => {
    const points = formatCoolnessPoints(raw);
    if (points) {
      lines.push(`${label}: ${points}`);
    }
  });
  if (system.coolness_nice_planet_count !== null || system.coolness_weird_planet_count !== null) {
    lines.push(`Signals: nice=${formatNumber(system.coolness_nice_planet_count, 0)}, weird=${formatNumber(system.coolness_weird_planet_count, 0)}`);
  }
  return lines;
}

function buildSpectralTooltipLines(system) {
  const dominantRaw = system.coolness_dominant_spectral_class || (system.spectral_classes?.[0] ?? "");
  const dominant = String(dominantRaw || "").trim().toUpperCase();
  const info = SPECTRAL_CLASS_INFO[dominant] || null;
  const lines = [];
  if (dominant) {
    lines.push(`Dominant class: ${dominant}`);
  }
  if (info?.sentence) {
    lines.push(info.sentence);
  }

  const hasTempSpan = Number.isFinite(system.min_star_teff_k) && Number.isFinite(system.max_star_teff_k);
  const teffCount = Number(system.star_teff_count || 0);
  if (hasTempSpan && teffCount > 0) {
    const minK = Number(system.min_star_teff_k);
    const maxK = Number(system.max_star_teff_k);
    if (Math.abs(minK - maxK) < 0.5) {
      lines.push(`Catalog stellar Teff: ${formatKelvin(minK)}`);
    } else {
      lines.push(`Catalog stellar Teff span: ${formatKelvin(minK)} - ${formatKelvin(maxK)}`);
    }
  } else if (info?.tempRangeK) {
    lines.push("Exact catalog surface temperature is unavailable in this build.");
    lines.push(`Typical class range: ${formatKelvin(info.tempRangeK[0])} - ${formatKelvin(info.tempRangeK[1])}`);
  } else {
    lines.push("Surface temperature unavailable.");
  }
  return lines;
}

function starCatalogRecordLink(star) {
  const sourceCatalog = String(star?.provenance?.source_catalog || "").toLowerCase();
  if (sourceCatalog === "athyg") {
    if (star?.gaia_id) {
      return {
        label: "Gaia DR3 record",
        url: `https://vizier.cds.unistra.fr/viz-bin/VizieR-5?-source=I/355/gaiadr3&Source=${encodeURIComponent(String(star.gaia_id))}`,
        note: "ATHYG aggregate source resolved via Gaia ID",
      };
    }
    if (star?.hip_id) {
      return {
        label: "SIMBAD HIP record",
        url: `https://simbad.cds.unistra.fr/simbad/sim-id?Ident=${encodeURIComponent(`HIP ${star.hip_id}`)}`,
        note: "ATHYG aggregate source resolved via HIP ID",
      };
    }
    if (star?.hd_id) {
      return {
        label: "SIMBAD HD record",
        url: `https://simbad.cds.unistra.fr/simbad/sim-id?Ident=${encodeURIComponent(`HD ${star.hd_id}`)}`,
        note: "ATHYG aggregate source resolved via HD ID",
      };
    }
  }
  return null;
}

function planetCatalogRecordLink(planet) {
  const sourceCatalog = String(planet?.provenance?.source_catalog || "").toLowerCase();
  if (sourceCatalog === "nasa_exoplanet_archive" && planet?.planet_name) {
    return {
      label: "NASA Exoplanet Archive record",
      url: `https://exoplanetarchive.ipac.caltech.edu/overview/${encodeURIComponent(String(planet.planet_name))}`,
    };
  }
  return null;
}

function eclipsingCatalogRecordLink(entry) {
  const sourceCatalog = String(entry?.provenance?.source_catalog || "").toLowerCase();
  if (sourceCatalog === "tess_eb") {
    return {
      label: "TESS EB Catalog",
      url: "https://tessebs.villanova.edu/",
    };
  }
  if (sourceCatalog === "kepler_eb") {
    return {
      label: "Kepler EB Catalog",
      url: "https://keplerebs.villanova.edu/",
    };
  }
  if (sourceCatalog === "debcat") {
    return {
      label: "DEBCat",
      url: "https://www.astro.keele.ac.uk/jkt/debcat/",
    };
  }
  return null;
}

function parseJsonArray(raw) {
  if (!raw) {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw.map((item) => String(item || "").trim()).filter(Boolean);
  }
  try {
    const parsed = JSON.parse(String(raw));
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map((item) => String(item || "").trim()).filter(Boolean);
  } catch (_) {
    return [];
  }
}

function normalizeEvidenceToken(raw) {
  return String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function evidenceLabel(raw) {
  const token = normalizeEvidenceToken(raw);
  if (!token) {
    return "";
  }
  const known = {
    gaia_nss: "Gaia NSS",
    gaia_nss_two_body: "Gaia NSS Two-Body",
    wds: "WDS",
    wds_gaia_xmatch: "WDS-Gaia XMatch",
    msc: "MSC",
    orb6: "ORB6",
    sbx: "SBX",
    vsx: "VSX",
    ultracoolsheet: "UltracoolSheet",
  };
  if (known[token]) {
    return known[token];
  }
  return token
    .split("_")
    .filter(Boolean)
    .map((part) => {
      if (part.length <= 4) {
        return part.toUpperCase();
      }
      return part.charAt(0).toUpperCase() + part.slice(1);
    })
    .join(" ");
}

function truthyEvidenceFlag(value) {
  return value === true || value === 1 || value === "1";
}

function collectEvidenceFromFlags(record) {
  if (!record || typeof record !== "object") {
    return [];
  }
  const tokens = [];
  Object.entries(record).forEach(([key, value]) => {
    const match = key.match(/^has_([a-z0-9_]+)_evidence$/i);
    if (!match || !truthyEvidenceFlag(value)) {
      return;
    }
    const normalized = normalizeEvidenceToken(match[1]);
    if (normalized) {
      tokens.push(normalized);
    }
  });
  return tokens;
}

function collectSystemEvidenceCatalogs(system) {
  const tokens = new Set();
  parseJsonArray(system?.grouping_source_catalogs_json).forEach((raw) => {
    const normalized = normalizeEvidenceToken(raw);
    if (normalized) {
      tokens.add(normalized);
    }
  });
  collectEvidenceFromFlags(system).forEach((token) => tokens.add(token));
  return Array.from(tokens).sort((a, b) => evidenceLabel(a).localeCompare(evidenceLabel(b)));
}

function collectStarEvidenceCatalogs(star) {
  const tokens = new Set();
  parseJsonArray(star?.multiplicity_source_catalogs_json).forEach((raw) => {
    const normalized = normalizeEvidenceToken(raw);
    if (normalized) {
      tokens.add(normalized);
    }
  });
  parseJsonArray(star?.arm_catalogs).forEach((raw) => {
    const normalized = normalizeEvidenceToken(raw);
    if (normalized) {
      tokens.add(normalized);
    }
  });
  collectEvidenceFromFlags(star).forEach((token) => tokens.add(token));
  if (truthyEvidenceFlag(star?.gaia_non_single_star)) {
    tokens.add("gaia_nss");
  }
  if (star?.sbx_sn !== null && star?.sbx_sn !== undefined && String(star.sbx_sn).trim() !== "") {
    tokens.add("sbx");
  }
  if (star?.wds_id) {
    tokens.add("wds");
  }
  return Array.from(tokens).sort((a, b) => evidenceLabel(a).localeCompare(evidenceLabel(b)));
}

function formatEvidenceSummary(tokens) {
  if (!Array.isArray(tokens) || tokens.length === 0) {
    return "None recorded";
  }
  return tokens.map((token) => evidenceLabel(token)).filter(Boolean).join(" · ");
}

function buildSearchResultTags(system, { limit = 6 } = {}) {
  const tags = [];
  const addTag = (label, title = "") => {
    const cleanLabel = String(label || "").trim();
    if (!cleanLabel || tags.some((tag) => tag.label === cleanLabel)) {
      return;
    }
    tags.push({ label: cleanLabel, title });
  };
  const distanceLy = Number(system?.dist_ly);
  const starCount = Number(system?.star_count || 0);
  const planetCount = Number(system?.planet_count || 0);
  const coolness = Number(system?.coolness_score);
  const nicePlanetCount = Number(system?.coolness_nice_planet_count || 0);
  const spectralClasses = Array.isArray(system?.spectral_classes)
    ? system.spectral_classes.map((token) => String(token || "").trim().toUpperCase()).filter(Boolean)
    : [];
  if (Number.isFinite(distanceLy) && distanceLy <= 25) {
    addTag("Nearby", "Within 25 light-years of Sol.");
  } else if (Number.isFinite(distanceLy) && distanceLy <= 100) {
    addTag("Local neighborhood", "Within 100 light-years of Sol.");
  }
  if (planetCount >= 2) {
    addTag("Multi-planet", "More than one confirmed planet is linked to this system.");
  } else if (planetCount === 1) {
    addTag("Exoplanet", "One confirmed planet is linked to this system.");
  }
  if (nicePlanetCount > 0 || system?.has_habitable_candidate) {
    addTag("HZ planet", "Broad habitable-zone-style planet screening signal; not a habitability claim.");
  }
  if (starCount >= 2) {
    addTag("Multistar", "Multiple stellar members are grouped in this system record.");
  }
  if (spectralClasses.some((token) => token === "D")) {
    addTag("White dwarf", "A compact stellar remnant class appears in the spectral summary.");
  }
  if (spectralClasses.some((token) => ["L", "T", "Y"].includes(token))) {
    addTag("Ultracool", "Includes an L, T, or Y ultracool spectral class.");
  }
  if (Number.isFinite(coolness) && coolness >= 20) {
    addTag("High coolness", "Ranks strongly on the active Coolstars discovery profile.");
  }
  collectSystemEvidenceCatalogs(system).slice(0, 2).forEach((token) => {
    addTag(evidenceLabel(token), "Catalog evidence contributing to this system grouping.");
  });
  return tags.slice(0, limit);
}

function formatArmEvidenceDetails(armEvidence) {
  if (!armEvidence || typeof armEvidence !== "object") {
    return "";
  }
  const details = [];
  if (armEvidence?.vsx && typeof armEvidence.vsx === "object") {
    const vsx = armEvidence.vsx;
    const parts = [];
    if (vsx.primary_variability_type_raw) {
      parts.push(String(vsx.primary_variability_type_raw));
    } else if (vsx.primary_variability_family) {
      parts.push(String(vsx.primary_variability_family));
    }
    if (vsx.primary_period_days !== null && vsx.primary_period_days !== undefined) {
      parts.push(`P ${formatNumber(vsx.primary_period_days, 3)} d`);
    }
    if (vsx.primary_amplitude_mag !== null && vsx.primary_amplitude_mag !== undefined) {
      parts.push(`Δmag ${formatNumber(vsx.primary_amplitude_mag, 2)}`);
    }
    if (vsx.any_high_variability === true) {
      parts.push("high variability");
    }
    if (parts.length > 0) {
      details.push(`VSX ${parts.join(" · ")}`);
    } else {
      details.push("VSX");
    }
  }
  if (armEvidence?.ultracoolsheet && typeof armEvidence.ultracoolsheet === "object") {
    const ucd = armEvidence.ultracoolsheet;
    const parts = [];
    if (ucd.object_name) {
      parts.push(String(ucd.object_name));
    }
    if (ucd.age_category) {
      parts.push(String(ucd.age_category));
    }
    if (ucd.youth_evidence) {
      parts.push(String(ucd.youth_evidence));
    }
    if (ucd.spectral_type_opt) {
      parts.push(`opt ${ucd.spectral_type_opt}`);
    } else if (ucd.spectral_type_ir) {
      parts.push(`ir ${ucd.spectral_type_ir}`);
    }
    if (parts.length > 0) {
      details.push(`UltracoolSheet ${parts.join(" · ")}`);
    } else {
      details.push("UltracoolSheet");
    }
  }
  return details.join(" | ");
}

function groupingSourceLabel(groupingBasis, groupingSources) {
  if (groupingSources.length > 0) {
    return groupingSources.map((source) => evidenceLabel(source)).filter(Boolean).join(" · ");
  }
  switch (String(groupingBasis || "").toLowerCase()) {
    case "wds":
      return "WDS-linked grouping";
    case "name_root":
      return "Name-root heuristic";
    case "proximity":
      return "Proximity heuristic";
    case "singleton":
      return "Singleton fallback";
    default:
      return "Unknown";
  }
}

function SnapshotMetadata({ system, snapshot }) {
  const rows = [
    { label: "System", value: formatText(systemDisplayName(system)), copyValue: systemDisplayName(system), copyLabel: "system name" },
    { label: "Stable key", value: formatText(system?.stable_object_key), copyValue: system?.stable_object_key, copyLabel: "stable key" },
    { label: "Gaia ID", value: formatText(resolvedSystemGaiaId(system)), copyValue: resolvedSystemGaiaId(system), copyLabel: "Gaia ID" },
    { label: "HIP ID", value: formatText(system?.hip_id_text ?? system?.hip_id), copyValue: system?.hip_id_text ?? system?.hip_id, copyLabel: "HIP ID" },
    { label: "HD ID", value: formatText(system?.hd_id_text ?? system?.hd_id), copyValue: system?.hd_id_text ?? system?.hd_id, copyLabel: "HD ID" },
    { label: "Distance", value: `${formatNumber(system?.dist_ly, 2)} ly` },
    { label: "Stars", value: formatNumber(system?.star_count, 0) },
    { label: "Planets", value: formatNumber(system?.planet_count, 0) },
    { label: "View", value: formatText(snapshot?.view_type) },
    { label: "Params hash", value: formatText(snapshot?.params_hash), copyValue: snapshot?.params_hash, copyLabel: "snapshot params hash" },
    { label: "Image size", value: (snapshot?.width_px && snapshot?.height_px) ? `${snapshot.width_px} x ${snapshot.height_px}` : "Unknown" },
  ];
  return (
    <div className="snapshot-meta" role="note" aria-label="Snapshot metadata">
      <h4>Snapshot Metadata</h4>
      {rows.map((row) => (
        <div key={row.label} className="snapshot-meta-row">
          <span className="snapshot-meta-label">{row.label}</span>
          <div className="snapshot-meta-value-wrap">
            <code className="snapshot-meta-value">{row.value}</code>
            {row.copyValue ? (
              <CopyButton
                value={row.copyValue}
                label={row.copyLabel || row.label}
                className="copy-btn-inline"
              />
            ) : null}
          </div>
        </div>
      ))}
    </div>
  );
}

function StarSearchSimulationPreview({
  system,
  displayName,
  cachedPreviewImage = "",
  liveActive = false,
  poolSlot = null,
  onActivate,
  onDeactivate,
  onCapture,
}) {
  const ref = React.useRef(null);
  const requestedLiveRef = React.useRef(false);
  const hoverIntentRef = React.useRef(false);
  const [visible, setVisible] = React.useState(false);
  const [hoverIntent, setHoverIntent] = React.useState(false);

  React.useEffect(() => {
    const node = ref.current;
    if (!node || typeof IntersectionObserver === "undefined") {
      setVisible(true);
      return undefined;
    }
    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        setVisible(Boolean(entry?.isIntersecting));
      },
      { root: null, rootMargin: "120px 0px", threshold: 0.18 },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  React.useEffect(() => {
    const wantsLive = visible && (!cachedPreviewImage || hoverIntent);
    if (wantsLive && !requestedLiveRef.current) {
      requestedLiveRef.current = true;
      onActivate?.(system.system_id);
      return;
    }
    if (!wantsLive && requestedLiveRef.current) {
      requestedLiveRef.current = false;
      onDeactivate?.(system.system_id);
    }
  }, [cachedPreviewImage, hoverIntent, onActivate, onDeactivate, system.system_id, visible]);

  const setHovering = React.useCallback((nextValue) => {
    hoverIntentRef.current = nextValue;
    setHoverIntent(nextValue);
  }, []);

  const handleFrameCapture = React.useCallback((dataUrl) => {
    onCapture?.(system.system_id, dataUrl);
    if (!hoverIntentRef.current) {
      requestedLiveRef.current = false;
      onDeactivate?.(system.system_id);
    }
  }, [onCapture, onDeactivate, system.system_id]);

  const showLivePreview = visible && liveActive;
  const showCachedPreview = Boolean(cachedPreviewImage) && !showLivePreview;

  return (
    <div
      ref={ref}
      className={`star-search-preview map-search-card-preview ${showLivePreview ? "is-live" : ""} ${showCachedPreview ? "is-cached" : ""}`}
      data-testid="star-search-simulation-preview"
      data-preview-state={showLivePreview ? "live" : showCachedPreview ? "cached" : visible ? "queued" : "pending"}
      data-preview-pool-slot={poolSlot ?? ""}
      onMouseEnter={() => setHovering(true)}
      onMouseLeave={() => setHovering(false)}
      onFocus={() => setHovering(true)}
      onBlur={() => setHovering(false)}
      tabIndex={0}
      aria-label={`${displayName} simulation preview`}
    >
      {showLivePreview ? (
        <React.Suspense fallback={<div className="map-search-card-fallback">Loading simulation</div>}>
          <SystemPreviewPanel
            key={`catalog-preview:${poolSlot ?? system.system_id}:${system.system_id}`}
            systemId={system.system_id}
            systemName={displayName}
            snapshot={system.snapshot}
            presentationMode="card"
            autoRun={false}
            qualityTier="balanced"
            captureFrame={!cachedPreviewImage}
            onFrameCapture={handleFrameCapture}
          />
        </React.Suspense>
      ) : showCachedPreview ? (
        <>
          <img className="map-search-card-capture" src={cachedPreviewImage} alt={`${displayName} cached System Simulation preview`} />
          <span className="map-search-card-preview-chip">{hoverIntent ? "Live queued" : "Hover to animate"}</span>
        </>
      ) : (
        <div className="map-search-card-fallback">{visible ? "Simulation queued" : "Loading preview"}</div>
      )}
    </div>
  );
}

function hierarchyTypeLabel(componentType) {
  const key = String(componentType || "").trim().toLowerCase();
  const labels = {
    system: "System",
    subsystem: "Subsystem",
    star: "Star",
    planet: "Planet",
    moon: "Moon",
    minor_body: "Minor Body",
    artificial: "Artificial",
  };
  return labels[key] || (key ? key.replace(/_/g, " ") : "Node");
}

function SystemFactPill({ label, value, title = "" }) {
  return (
    <div className="system-fact-pill" title={title || undefined}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function bestSpectralSummary(system, stars = []) {
  const classes = Array.isArray(system?.spectral_classes) ? system.spectral_classes : [];
  if (classes.length > 0) {
    return classes.slice(0, 4).join(", ");
  }
  const fromStars = Array.from(new Set(
    (Array.isArray(stars) ? stars : [])
      .map((star) => String(star?.spectral_class || "").trim().toUpperCase())
      .filter(Boolean),
  ));
  if (fromStars.length > 0) {
    return fromStars.slice(0, 4).join(", ");
  }
  return formatText(system?.coolness_dominant_spectral_class);
}

function systemOverviewSentence(system, stars = [], planets = []) {
  const name = systemDisplayName(system);
  const distance = Number(system?.dist_ly);
  const distanceText = Number.isFinite(distance) ? `${formatNumber(distance, 1)} light-years away` : "at an unknown distance";
  const starCount = Number(system?.star_count ?? stars.length ?? 0);
  const planetCount = Number(system?.planet_count ?? planets.length ?? 0);
  const spectral = bestSpectralSummary(system, stars);
  const starText = starCount === 1 ? "one known stellar member" : `${formatNumber(starCount, 0)} known stellar members`;
  const planetText = planetCount === 1 ? "one confirmed planet" : `${formatNumber(planetCount, 0)} confirmed planets`;
  const spectralText = spectral && spectral !== "Unknown" ? ` Its current spectral summary is ${spectral}.` : "";
  return `${formatText(name)} is ${distanceText} with ${starText} and ${planetText} in the current public database.${spectralText}`;
}

function whySystemMatters(system, stars = [], planets = [], hierarchy = null) {
  const notes = [];
  const planetCount = Number(system?.planet_count ?? planets.length ?? 0);
  const starCount = Number(system?.star_count ?? stars.length ?? 0);
  const coolness = Number(system?.coolness_score);
  const habitableCount = Number(system?.coolness_nice_planet_count || 0);
  const evidenceCatalogs = collectSystemEvidenceCatalogs(system);
  if (planetCount > 0) {
    notes.push(`${planetCount === 1 ? "It has a confirmed planet" : `It has ${formatNumber(planetCount, 0)} confirmed planets`}, making it useful for comparing real orbital systems.`);
  }
  if (habitableCount > 0 || system?.has_habitable_candidate) {
    notes.push("At least one linked planet currently triggers a broad habitable-zone style signal. That is an exploration clue, not a claim of habitability.");
  }
  if (starCount > 1 || Number(hierarchy?.counts?.subsystems || 0) > 0) {
    notes.push("Its stellar hierarchy matters: multi-star systems can reshape or disrupt otherwise simple planetary zones.");
  }
  if (Number.isFinite(coolness) && coolness > 0) {
    notes.push(`Coolstars ranks it at ${formatNumber(coolness, 1)} on the active coolness profile, which blends discovery value, planets, proximity, and other public-interest signals.`);
  }
  if (evidenceCatalogs.length > 0) {
    notes.push(`The public record is assembled from ${formatEvidenceSummary(evidenceCatalogs)} evidence.`);
  }
  if (notes.length === 0) {
    notes.push("This system is part of the nearby-space inventory. Even quiet systems help define the shape, density, and variety of the local stellar neighborhood.");
  }
  return notes.slice(0, 4);
}

function habitabilityContext(system, planets = []) {
  const planetCount = Number(system?.planet_count ?? planets.length ?? 0);
  if (planetCount <= 0) {
    return "No confirmed planets are currently linked to this system in the public database, so habitability context is limited to the stars and their radiation environment.";
  }
  const niceCount = Number(system?.coolness_nice_planet_count || 0);
  if (niceCount > 0 || system?.has_habitable_candidate) {
    return "Spacegate marks at least one planet as habitable-zone-like under broad screening criteria. A real habitability judgment would need atmosphere, composition, stellar activity, orbital stability, and uncertainty review.";
  }
  return "Confirmed planets are present, but none currently pass the broad habitable-zone candidate screen used by the public coolness profile.";
}

function SystemNarrativeScaffold({ system, stars, planets, hierarchy }) {
  const name = systemDisplayName(system);
  const matterNotes = whySystemMatters(system, stars, planets, hierarchy);
  return (
    <section className="system-story-grid">
      <article className="panel system-story-card system-story-card-primary">
        <span className="system-story-kicker">Overview</span>
        <h2>{formatText(name)}</h2>
        <p>{systemOverviewSentence(system, stars, planets)}</p>
      </article>
      <article className="panel system-story-card">
        <span className="system-story-kicker">Why It Matters</span>
        <ul>
          {matterNotes.map((note) => <li key={note}>{note}</li>)}
        </ul>
      </article>
      <article className="panel system-story-card">
        <span className="system-story-kicker">Habitability Context</span>
        <p>{habitabilityContext(system, planets)}</p>
      </article>
      <article className="panel system-story-card">
        <span className="system-story-kicker">Future AAA Narrative Slot</span>
        <p>
          The AI Astronomy Agency will eventually fill this space with reviewed public narration: what we know,
          what remains uncertain, why the system is interesting, and where the evidence comes from.
        </p>
      </article>
    </section>
  );
}

function ConceptExplainerGrid() {
  const items = [
    ["Spectral class", "A star's color and temperature family. O and B stars are hot and blue; K and M stars are cooler and longer-lived; D marks white-dwarf remnants."],
    ["Habitable zone", "A broad orbital region where stellar energy could allow liquid water on a suitable rocky world. It is not proof that a planet is habitable."],
    ["Orbital period", "How long a body takes to complete one orbit. In the simulation, source periods are preferred over derived or assumed values."],
    ["Eccentricity", "How stretched an orbit is. Zero is circular; higher values produce more elongated paths and larger seasonal energy swings."],
    ["Hierarchy", "The nesting of stars, subsystems, planets, and orbits. Multi-star systems need hierarchy so the simulation does not flatten everything into one decorative cluster."],
    ["Uncertainty", "Some values are source facts, some are derived, some are presentation assumptions, and some are missing. Evidence sections keep those roles visible."],
  ];
  return (
    <section className="panel concept-panel">
      <h3>Reading This System</h3>
      <div className="concept-grid">
        {items.map(([label, text]) => (
          <div key={label} className="concept-card">
            <strong>{label}</strong>
            <span>{text}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function hierarchyDisplayType(node, children) {
  const family = String(node?.component_family || node?.component_type || "").trim().toLowerCase();
  const hasStarChildren = Array.isArray(children) && children.some((child) => String(child?.component_family || child?.component_type || "").trim().toLowerCase() === "star");
  if (family === "star" && hasStarChildren) {
    return "subsystem";
  }
  return family || String(node?.component_type || "").trim().toLowerCase();
}

function hierarchyCountSummary(node) {
  const totalTypeCounts = node?.total_type_counts || {};
  const bits = [];
  const stars = Number(node?.total_star_count || 0);
  const planets = Number(totalTypeCounts.planet || 0);
  const moons = Number(totalTypeCounts.moon || 0);
  const minorBodies = Number(totalTypeCounts.minor_body || 0);
  const artificial = Number(totalTypeCounts.artificial || 0);
  if (stars > 0) {
    bits.push(`${formatNumber(stars, 0)} star${stars === 1 ? "" : "s"}`);
  }
  if (planets > 0) {
    bits.push(`${formatNumber(planets, 0)} planet${planets === 1 ? "" : "s"}`);
  }
  if (moons > 0) {
    bits.push(`${formatNumber(moons, 0)} moon${moons === 1 ? "" : "s"}`);
  }
  if (minorBodies > 0) {
    bits.push(`${formatNumber(minorBodies, 0)} minor bod${minorBodies === 1 ? "y" : "ies"}`);
  }
  if (artificial > 0) {
    bits.push(`${formatNumber(artificial, 0)} artificial`);
  }
  return bits.join(" · ");
}

function formatMsun(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "";
  }
  return `${formatNumber(value, 2)} Msun`;
}

function formatRsun(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "";
  }
  return `${formatNumber(value, 2)} Rsun`;
}

function formatVmag(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "";
  }
  return `V ${formatNumber(value, 2)}`;
}

function formatArcsec(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "";
  }
  return `${formatNumber(value, 2)} arcsec`;
}

function HierarchyFactChips({ node }) {
  const facts = node?.quick_facts || {};
  const chips = [];
  if (facts.spectral_type_raw) {
    chips.push({ label: "Spectral", value: String(facts.spectral_type_raw) });
  } else if (facts.spectral_class) {
    chips.push({ label: "Class", value: String(facts.spectral_class) });
  } else if (facts.visual_stellar_class) {
    chips.push({
      label: "Visual prior",
      value: String(facts.visual_stellar_class),
      status: String(facts.visual_stellar_class_status || "assumed").toUpperCase(),
      basis: facts.visual_stellar_class_basis,
    });
  }
  if (facts.teff_k !== null && facts.teff_k !== undefined) {
    chips.push({ label: "Temp", value: formatKelvin(facts.teff_k, 0) });
  }
  if (facts.mass_msun !== null && facts.mass_msun !== undefined) {
    chips.push({ label: "Mass", value: formatMsun(facts.mass_msun) });
  }
  if (facts.radius_rsun !== null && facts.radius_rsun !== undefined) {
    chips.push({ label: "Radius", value: formatRsun(facts.radius_rsun) });
  }
  if (facts.vmag !== null && facts.vmag !== undefined) {
    chips.push({ label: "Vmag", value: formatVmag(facts.vmag) });
  }
  if (facts.dist_ly !== null && facts.dist_ly !== undefined) {
    chips.push({ label: "Dist", value: `${formatNumber(facts.dist_ly, 2)} ly` });
  }
  if (facts.sep_arcsec !== null && facts.sep_arcsec !== undefined) {
    chips.push({ label: "Sep", value: formatArcsec(facts.sep_arcsec) });
  }
  if (chips.length === 0) {
    return null;
  }
  return (
    <div className="hierarchy-fact-chips" role="list" aria-label="Star quick facts">
      {chips.map((chip) => (
        <span
          key={`${chip.label}-${chip.value}`}
          className={`chip hierarchy-fact-chip ${chip.status ? `status-${String(chip.status).toLowerCase()}` : ""}`}
          role="listitem"
          title={chip.basis ? `${chip.label}: ${chip.basis}` : undefined}
        >
          <span className="hierarchy-fact-label">{chip.label}</span>
          <strong>{chip.value}</strong>
          {chip.status ? <span className="hierarchy-fact-status">{chip.status}</span> : null}
        </span>
      ))}
    </div>
  );
}

function HierarchyNodeCard({ node, depth = 0 }) {
  const children = Array.isArray(node?.children) ? node.children : [];
  const compactStellarBranch = children.length > 0
    && Number(node?.total_star_count || 0) > 0
    && Number(node?.descendant_count || 0) <= 6;
  const initialExpanded = !node?.collapsed_by_default && (depth < 2 || compactStellarBranch);
  const [expanded, setExpanded] = useState(initialExpanded || depth === 0);
  const displayName = formatText(node?.display_name);
  const countSummary = hierarchyCountSummary(node);
  const displayType = hierarchyDisplayType(node, children);

  return (
    <div className={`hierarchy-node depth-${Math.min(depth, 4)}`}>
      <div className="hierarchy-node-card">
        <button
          type="button"
          className={`hierarchy-node-head ${children.length ? "is-clickable" : "is-static"}`}
          onClick={() => {
            if (children.length) {
              setExpanded((value) => !value);
            }
          }}
          disabled={!children.length}
          aria-expanded={children.length ? expanded : undefined}
        >
          <div className="hierarchy-node-title-wrap">
            <div className="hierarchy-node-title-row">
              <strong>{displayName}</strong>
              {displayType === "star" ? (
                <StellarClassChips tokens={stellarClassTokensFromRecord(node)} size="compact" />
              ) : null}
              <span className="hierarchy-node-kind">{hierarchyTypeLabel(displayType)}</span>
            </div>
            <div className="muted hierarchy-node-meta">
              {countSummary || "No descendants recorded"}
              {node?.catalog_component_label ? ` · Label ${node.catalog_component_label}` : ""}
              {children.length ? ` · ${formatNumber(children.length, 0)} child node${children.length === 1 ? "" : "s"}` : ""}
            </div>
            {node?.orbit ? (
              <div className="muted hierarchy-node-orbit">
                {formatOrbitSummary({
                  periodDays: node.orbit.period_days,
                  semiMajorAxisAu: node.orbit.semi_major_axis_au,
                  eccentricity: node.orbit.eccentricity,
                  inclinationDeg: node.orbit.inclination_deg,
                })}
              </div>
            ) : null}
            {displayType === "star" ? (
              <HierarchyFactChips node={node} />
            ) : null}
          </div>
          {children.length ? (
            <span className="hierarchy-toggle" aria-hidden="true">
              {expanded ? "Collapse" : "Expand"}
            </span>
          ) : null}
        </button>
        {children.length > 0 && expanded ? (
          <div className="hierarchy-children">
            {children.map((child) => (
              <HierarchyNodeCard key={child.stable_component_key} node={child} depth={depth + 1} />
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function SystemHierarchyPanel({ hierarchy }) {
  const root = hierarchy?.root;
  const counts = hierarchy?.counts || {};
  if (!root) {
    return null;
  }
  return (
    <section className="panel hierarchy-panel">
      <h3>System Hierarchy</h3>
      <p className="muted">
        This view reconstructs the nested structure from the arm graph so multi-level systems, orbiting bodies, and synthetic subsystems appear in one consistent layout.
      </p>
      <div className="hierarchy-kpis">
        <div><strong>Total Stars</strong><span>{formatNumber(counts.stars, 0)}</span></div>
        <div><strong>Total Nodes</strong><span>{formatNumber(counts.nodes, 0)}</span></div>
        <div><strong>Direct Children</strong><span>{formatNumber(counts.direct_children, 0)}</span></div>
      </div>
      <div className="hierarchy-tree">
        <HierarchyNodeCard node={root} depth={0} />
      </div>
    </section>
  );
}

function Layout({ children, headerExtra = null, showSearchLink = true, buildId = "" }) {
  const { theme, setTheme, options } = useThemeControls();
  const location = useLocation();
  const navigate = useNavigate();
  const isLcars = theme === "lcars";
  const [lcarsChipSystems, setLcarsChipSystems] = useState(() => LCARS_FALLBACK_CHIPS.map((name) => ({ system_id: null, system_name: name })));
  const [lcarsGaiaPool, setLcarsGaiaPool] = useState(() => LCARS_FALLBACK_GAIA.map((gaia) => ({ gaia, system_id: null, system_name: "" })));
  const [lcarsHistory, setLcarsHistory] = useState(() => loadLcarsHistory());

  const currentSystemId = useMemo(() => {
    const match = String(location.pathname || "").match(/^\/systems\/(\d+)/);
    return match?.[1] || "";
  }, [location.pathname]);

  const lcarsHistoryDisplay = useMemo(
    () => lcarsHistory
      .filter((entry) => String(entry.system_id) !== currentSystemId)
      .slice(0, LCARS_RIGHT_CHIP_COUNT + LCARS_TEXT_MAX_SLOTS),
    [lcarsHistory, currentSystemId],
  );

  const lcarsRightHistory = useMemo(
    () => lcarsHistoryDisplay.slice(0, LCARS_RIGHT_CHIP_COUNT),
    [lcarsHistoryDisplay],
  );

  const lcarsRightChips = useMemo(() => {
    const historyChips = lcarsRightHistory.map((entry) => ({
      system_id: entry.system_id,
      system_name: entry.system_name,
    }));
    if (historyChips.length >= LCARS_RIGHT_CHIP_COUNT) {
      return historyChips.slice(0, LCARS_RIGHT_CHIP_COUNT);
    }
    const seenIds = new Set(
      historyChips
        .map((entry) => (
          entry?.system_id === null || entry?.system_id === undefined ? "" : String(entry.system_id)
        ))
        .filter(Boolean),
    );
    const fallbackChips = lcarsChipSystems.filter((entry) => {
      const id = entry?.system_id;
      if (id === null || id === undefined) {
        return true;
      }
      if (seenIds.has(String(id))) {
        return false;
      }
      seenIds.add(String(id));
      return true;
    });
    return [...historyChips, ...fallbackChips].slice(0, LCARS_RIGHT_CHIP_COUNT);
  }, [lcarsChipSystems, lcarsRightHistory]);

  const lcarsTextRows = useMemo(() => {
    const historyEntries = lcarsHistoryDisplay.slice(LCARS_RIGHT_CHIP_COUNT).map((entry) => ({
      label: entry.system_name,
      system_id: entry.system_id,
      title: entry.system_name,
    }));
    const poolEntries = lcarsGaiaPool.length > 0
      ? lcarsGaiaPool
      : LCARS_FALLBACK_GAIA.map((gaia) => ({ gaia, system_id: null, system_name: "" }));
    const pool = poolEntries.map((entry) => ({
      label: entry.gaia,
      system_id: entry.system_id,
      title: entry.system_name || `Gaia ${entry.gaia}`,
    }));
    const slots = [];
    for (let idx = 0; idx < LCARS_TEXT_MAX_SLOTS; idx += 1) {
      if (idx < historyEntries.length) {
        slots.push(historyEntries[idx]);
      } else {
        slots.push(pool[idx % pool.length] || { label: "Gaia", system_id: null, title: "Gaia" });
      }
    }
    return Array.from({ length: LCARS_TEXT_ROW_COUNT }, (_, rowIdx) => {
      const start = rowIdx * LCARS_TEXT_SLOTS_PER_LINE;
      return slots.slice(start, start + LCARS_TEXT_SLOTS_PER_LINE);
    });
  }, [lcarsGaiaPool, lcarsHistoryDisplay]);

  useEffect(() => {
    if (!isLcars) {
      return;
    }
    let cancelled = false;
    const loadLcarsSystems = async () => {
      try {
        const data = await fetchSystems({
          limit: "120",
          sort: "coolness",
          has_planets: "true",
        });
        const items = Array.isArray(data?.items) ? data.items : [];
        if (!cancelled) {
          setLcarsChipSystems(pickRandomSystems(items, LCARS_RIGHT_CHIP_COUNT));
          setLcarsGaiaPool(pickRandomGaiaEntries(items, LCARS_TEXT_MAX_SLOTS));
        }
      } catch (_) {
        if (!cancelled) {
          setLcarsChipSystems(pickRandomSystems([], LCARS_RIGHT_CHIP_COUNT));
          setLcarsGaiaPool(
            LCARS_FALLBACK_GAIA
              .slice(0, LCARS_TEXT_MAX_SLOTS)
              .map((gaia) => ({ gaia, system_id: null, system_name: "" })),
          );
        }
      }
    };
    loadLcarsSystems();
    return () => {
      cancelled = true;
    };
  }, [isLcars]);

  useEffect(() => {
    if (!isLcars) {
      return;
    }
    setLcarsHistory(loadLcarsHistory());
  }, [isLcars, location.pathname]);

  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.defaultPrevented || event.repeat) {
        return;
      }
      if (event.metaKey || event.ctrlKey || event.altKey) {
        return;
      }
      if (event.key !== "/") {
        return;
      }
      if (isEditableTarget(event.target)) {
        return;
      }
      event.preventDefault();
      if (focusGlobalSearchInput()) {
        return;
      }
      if (location.pathname !== "/") {
        navigate("/");
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [location.pathname, navigate]);

  return (
    <div className={`app ${isLcars ? "lcars-app" : ""}`}>
      {isLcars && (
        <div className="lcars-topbar">
          <LcarsUtilityRail />
          <div className="lcars-top-left">
            {Array.from({ length: LCARS_LEFT_DECORATIVE_CHIP_COUNT }).map((_, idx) => (
              <span
                key={`lcars-deco-left-${idx}`}
                className={`lcars-left-deco ${idx === 0 ? "lcars-left-deco-top" : "lcars-left-deco-bottom"}`}
                aria-hidden={idx !== 0}
              />
            ))}
          </div>
          <div className="lcars-top-center">
            {lcarsTextRows.map((row, rowIdx) => (
              <div key={`lcars-row-${rowIdx}`} className="lcars-text-row">
                {row.map((entry, idx) => {
                  const label = String(entry?.label || "").trim() || "Gaia";
                  const to = entry?.system_id
                    ? `/systems/${entry.system_id}`
                    : `/?q=${encodeURIComponent(label)}`;
                  return (
                    <Link
                      key={`lcars-token-${rowIdx}-${idx}-${label}`}
                      to={to}
                      className="lcars-text-token"
                      title={entry?.title || label}
                    >
                      {label}
                    </Link>
                  );
                })}
              </div>
            ))}
          </div>
          <div className="lcars-top-right">
            <strong>STARS ACCESSED</strong>
            <div className="lcars-chip-row">
              {lcarsRightChips.map((entry, idx) => {
                const name = String(entry?.system_name || "").trim() || `System ${idx + 1}`;
                const to = entry?.system_id
                  ? `/systems/${entry.system_id}`
                  : `/?q=${encodeURIComponent(name)}`;
                return (
                  <Link key={`lcars-chip-${idx}-${name}`} to={to} className="lcars-chip-link" title={name}>
                    {name}
                  </Link>
                );
              })}
            </div>
          </div>
        </div>
      )}
      {isLcars && (
        <div className="lcars-header-bridge">
          <LcarsDataRail buildId={buildId} />
        </div>
      )}
      <header className="site-header">
        {!isLcars && (
          <div className="header-topline">
            <HeaderNavLinks className="header-top-links" linkClassName="header-top-link" buildId={buildId} />
          </div>
        )}
        <div className="header-main">
          <div className="header-brand">
            <div className="eyebrow">Interstellar Explorer</div>
            <div className="title-row">
              <h1><a href="/" className="title-link">{APP_DISPLAY_NAME}</a></h1>
            </div>
          </div>
          <div className="header-side">
            <div className="header-meta-row">
              <p className="header-subtitle">Discover and explore nearby systems, stars, and exoplanets.</p>
              <div className="header-actions">
                <div className="theme-picker">
                  <label htmlFor="theme-select" className="sr-only">Theme</label>
                  <select
                    id="theme-select"
                    className="theme-select"
                    value={theme}
                    onChange={(event) => setTheme(event.target.value)}
                  >
                    {options.map((option) => (
                      <option key={option.id} value={option.id}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>
                {showSearchLink && <Link to="/" className="button ghost">Search</Link>}
              </div>
            </div>
            {headerExtra && <div className="header-lower">{headerExtra}</div>}
          </div>
        </div>
      </header>
      <main>{children}</main>
    </div>
  );
}

function HeaderSearchBar({
  query,
  setQuery,
  onSubmit,
  onClear,
  onMap,
  loading = false,
  autoFocus = false,
}) {
  return (
    <form className="results-search-row header-search-row" onSubmit={onSubmit}>
      <button className="button compact search-submit-button" type="submit" disabled={loading}>
        {loading ? "Searching..." : "Search"}
      </button>
      {onMap ? (
        <button className="button ghost compact" type="button" onClick={onMap} disabled={loading}>
          Map
        </button>
      ) : null}
      <label className="results-search-field">
        <span className="sr-only">Search systems</span>
        <input
          type="text"
          data-global-search-input="true"
          className="results-search-input"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search systems by name, ID, or catalog key..."
          autoFocus={autoFocus}
        />
      </label>
      <button type="button" className="button ghost compact" onClick={onClear} disabled={loading}>
        Clear
      </button>
    </form>
  );
}

function RouteHeaderSearchBar({ mapSystem = null }) {
  const navigate = useNavigate();
  const location = useLocation();
  const [query, setQuery] = useState("");

  useEffect(() => {
    if (location.pathname === "/" || location.pathname === "/search") {
      const params = new URLSearchParams(location.search);
      setQuery(params.get("q") || "");
      return;
    }
    setQuery("");
  }, [location.pathname, location.search]);

  const onSubmit = (event) => {
    event.preventDefault();
    const nextQuery = query.trim();
    navigate(nextQuery ? `/search?q=${encodeURIComponent(nextQuery)}&sort=match` : "/search");
  };

  const onClear = () => {
    setQuery("");
    navigate("/search");
  };

  const onMap = mapSystem?.system_id
    ? () => navigate(mapExploreHrefForSystem(mapSystem))
    : null;

  const handleMap = () => {
    onMap?.();
  };

  return (
    <HeaderSearchBar
      query={query}
      setQuery={setQuery}
      onSubmit={onSubmit}
      onClear={onClear}
      onMap={onMap ? handleMap : null}
    />
  );
}

function AboutPage({ buildId = "" }) {
  return (
    <Layout buildId={buildId} showSearchLink={false} headerExtra={<RouteHeaderSearchBar />}>
      <section className="detail-layout">
        <section className="panel markdown-panel">
          <MarkdownContent markdown={ABOUT_MARKDOWN} />
        </section>
      </section>
    </Layout>
  );
}

function HelpPage({ buildId = "" }) {
  return (
    <Layout buildId={buildId} showSearchLink={false} headerExtra={<RouteHeaderSearchBar />}>
      <section className="detail-layout">
        <section className="panel markdown-panel">
          <MarkdownContent markdown={HELP_MARKDOWN} />
        </section>
      </section>
    </Layout>
  );
}

function DataPage({ buildId = "" }) {
  return (
    <Layout buildId={buildId} showSearchLink={false} headerExtra={<RouteHeaderSearchBar />}>
      <section className="detail-layout">
        <section className="panel markdown-panel">
          <MarkdownContent markdown={DATA_MARKDOWN} />
        </section>
      </section>
    </Layout>
  );
}

function SearchPage({ buildId = "" }) {
  const { theme } = useThemeControls();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const initialSpectralClassTokens = parseSpectralTokens(searchParams.get("spectral_class") || "");
  const initialSpectralIncludeTokens = parseSpectralTokens(searchParams.get("spectral_include") || "");
  const initialSpectralExcludeTokens = parseSpectralTokens(searchParams.get("spectral_exclude") || "");
  const hasExplicitSpectralOverrides = searchParams.has("spectral_include") || searchParams.has("spectral_exclude");
  const [query, setQuery] = useState(() => searchParams.get("q") || "");
  const [minDist, setMinDist] = useState(() => parseRangeParam(
    searchParams,
    "min_dist_ly",
    filterLimits.distance.min,
    filterLimits.distance.min,
    filterLimits.distance.max,
    filterLimits.distance.integer,
  ));
  const [maxDist, setMaxDist] = useState(() => parseRangeParam(
    searchParams,
    "max_dist_ly",
    filterLimits.distance.max,
    filterLimits.distance.min,
    filterLimits.distance.max,
    filterLimits.distance.integer,
  ));
  const [minStarCount, setMinStarCount] = useState(() => parseRangeParam(
    searchParams,
    "min_star_count",
    filterLimits.stars.min,
    filterLimits.stars.min,
    filterLimits.stars.max,
    filterLimits.stars.integer,
  ));
  const [maxStarCount, setMaxStarCount] = useState(() => parseRangeParam(
    searchParams,
    "max_star_count",
    filterLimits.stars.max,
    filterLimits.stars.min,
    filterLimits.stars.max,
    filterLimits.stars.integer,
  ));
  const [minPlanetCount, setMinPlanetCount] = useState(() => parseRangeParam(
    searchParams,
    "min_planet_count",
    filterLimits.planets.min,
    filterLimits.planets.min,
    filterLimits.planets.max,
    filterLimits.planets.integer,
  ));
  const [maxPlanetCount, setMaxPlanetCount] = useState(() => parseRangeParam(
    searchParams,
    "max_planet_count",
    filterLimits.planets.max,
    filterLimits.planets.min,
    filterLimits.planets.max,
    filterLimits.planets.integer,
  ));
  const [minCoolnessScore, setMinCoolnessScore] = useState(() => parseRangeParam(
    searchParams,
    "min_coolness_score",
    filterLimits.coolness.min,
    filterLimits.coolness.min,
    filterLimits.coolness.max,
    filterLimits.coolness.integer,
  ));
  const [maxCoolnessScore, setMaxCoolnessScore] = useState(() => parseRangeParam(
    searchParams,
    "max_coolness_score",
    filterLimits.coolness.max,
    filterLimits.coolness.min,
    filterLimits.coolness.max,
    filterLimits.coolness.integer,
  ));
  const [sort, setSort] = useState(() => {
    const value = String(searchParams.get("sort") || (searchParams.get("q") ? "match" : "coolness")).toLowerCase();
    return ["match", "coolness", "name", "distance", "planet_count", "star_count", "hottest", "coolest"].includes(value) ? value : "coolness";
  });
  const [spectral, setSpectral] = useState(() => {
    if (hasExplicitSpectralOverrides) {
      return initialSpectralIncludeTokens;
    }
    return initialSpectralClassTokens;
  });
  const [spectralExclude, setSpectralExclude] = useState(() => {
    if (hasExplicitSpectralOverrides) {
      return initialSpectralExcludeTokens;
    }
    if (initialSpectralClassTokens.length > 0 && initialSpectralClassTokens.length < spectralOptions.length) {
      return spectralOptions.filter((token) => !initialSpectralClassTokens.includes(token));
    }
    return [];
  });
  const [minTempK, setMinTempK] = useState(() => parseRangeParam(
    searchParams,
    "min_temp_k",
    SPECTRAL_TEMP_MIN_K,
    SPECTRAL_TEMP_MIN_K,
    SPECTRAL_TEMP_MAX_K,
    true,
  ));
  const [maxTempK, setMaxTempK] = useState(() => parseRangeParam(
    searchParams,
    "max_temp_k",
    SPECTRAL_TEMP_MAX_K,
    SPECTRAL_TEMP_MIN_K,
    SPECTRAL_TEMP_MAX_K,
    true,
  ));
  const [hasHabitableMode, setHasHabitableMode] = useState(() => {
    const value = searchParams.get("has_habitable");
    return value === "true" || value === "false" ? value : "";
  });
  const [pageSize, setPageSize] = useState(() => {
    const raw = Number(searchParams.get("limit") || "50");
    if (Number.isFinite(raw) && raw >= 1 && raw <= 200) {
      return String(Math.trunc(raw));
    }
    return "50";
  });
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [cursor, setCursor] = useState(null);
  const [hasMore, setHasMore] = useState(false);
  const [searchStarted, setSearchStarted] = useState(false);
  const [activeParams, setActiveParams] = useState(null);
  const [totalCount, setTotalCount] = useState(null);
  const [lastQueryStats, setLastQueryStats] = useState(null);
  const [filtersCollapsedY, setFiltersCollapsedY] = useState(false);
  const [spectralMix, setSpectralMix] = useState(null);
  const [spectralMixLoading, setSpectralMixLoading] = useState(true);
  const [spectralMixError, setSpectralMixError] = useState("");
  const [previewPoolAllocations, setPreviewPoolAllocations] = useState([]);
  const [previewSnapshotCache, setPreviewSnapshotCache] = useState(() => new Map());
  const latestSearchTokenRef = useRef(0);
  const latestTotalTokenRef = useRef(0);
  const previewPoolIdsRef = useRef(new Set());
  const previewRequestQueueRef = useRef([]);

  const spectralMixCountByClass = useMemo(() => {
    const map = new Map();
    const rows = Array.isArray(spectralMix?.rows) ? spectralMix.rows : [];
    rows.forEach((row) => {
      const key = String(row?.spectral_class || "").trim().toUpperCase();
      if (!key) {
        return;
      }
      map.set(key, Number(row?.star_count || 0));
    });
    return map;
  }, [spectralMix]);
  const spectralMixTotalStars = Number(spectralMix?.total_stars || 0);
  const eligibleSpectralClasses = useMemo(
    () => spectralClassesForTemperatureRange(minTempK, maxTempK),
    [minTempK, maxTempK],
  );
  const eligibleSpectralSet = useMemo(() => new Set(eligibleSpectralClasses), [eligibleSpectralClasses]);
  const spectralIncludeSet = useMemo(
    () => new Set((spectral || []).map((token) => String(token || "").trim().toUpperCase()).filter((token) => spectralOptions.includes(token))),
    [spectral],
  );
  const spectralExcludeSet = useMemo(
    () => new Set((spectralExclude || []).map((token) => String(token || "").trim().toUpperCase()).filter((token) => spectralOptions.includes(token))),
    [spectralExclude],
  );
  const explicitIncludeOutsideRangeSet = useMemo(
    () => new Set(spectralOptions.filter((token) => spectralIncludeSet.has(token) && !eligibleSpectralSet.has(token))),
    [spectralIncludeSet, eligibleSpectralSet],
  );
  const effectiveSpectralClasses = useMemo(() => {
    return spectralOptions.filter((token) => (
      (eligibleSpectralSet.has(token) && !spectralExcludeSet.has(token))
      || explicitIncludeOutsideRangeSet.has(token)
    ));
  }, [eligibleSpectralSet, spectralExcludeSet, explicitIncludeOutsideRangeSet]);
  const effectiveSpectralSet = useMemo(() => new Set(effectiveSpectralClasses), [effectiveSpectralClasses]);
  const effectiveSpectralCount = useMemo(
    () => effectiveSpectralClasses.reduce((sum, token) => sum + (Number(spectralMixCountByClass.get(token) || 0)), 0),
    [effectiveSpectralClasses, spectralMixCountByClass],
  );
  const effectiveSpectralPct = spectralMixTotalStars > 0 ? (effectiveSpectralCount / spectralMixTotalStars) * 100 : 0;
  const minTempSliderPos = spectralTempToSliderPosition(minTempK);
  const maxTempSliderPos = spectralTempToSliderPosition(maxTempK);
  const defaultFilterState = () => ({
    query: "",
    minDist: filterLimits.distance.min,
    maxDist: filterLimits.distance.max,
    minStarCount: filterLimits.stars.min,
    maxStarCount: filterLimits.stars.max,
    minPlanetCount: filterLimits.planets.min,
    maxPlanetCount: filterLimits.planets.max,
    minCoolnessScore: filterLimits.coolness.min,
    maxCoolnessScore: filterLimits.coolness.max,
    minTempK: SPECTRAL_TEMP_MIN_K,
    maxTempK: SPECTRAL_TEMP_MAX_K,
    sort: "coolness",
    spectral: [],
    spectralExclude: [],
    hasHabitableMode: "",
    pageSize: "50",
  });
  const currentFilterState = () => ({
    query,
    minDist,
    maxDist,
    minStarCount,
    maxStarCount,
    minPlanetCount,
    maxPlanetCount,
    minCoolnessScore,
    maxCoolnessScore,
    minTempK,
    maxTempK,
    sort,
    spectral,
    spectralExclude,
    hasHabitableMode,
    pageSize,
  });
  const applyFilterState = (next) => {
    setQuery(next.query);
    setMinDist(next.minDist);
    setMaxDist(next.maxDist);
    setMinStarCount(next.minStarCount);
    setMaxStarCount(next.maxStarCount);
    setMinPlanetCount(next.minPlanetCount);
    setMaxPlanetCount(next.maxPlanetCount);
    setMinCoolnessScore(next.minCoolnessScore);
    setMaxCoolnessScore(next.maxCoolnessScore);
    setMinTempK(next.minTempK);
    setMaxTempK(next.maxTempK);
    setSort(next.sort);
    setSpectral(next.spectral);
    setSpectralExclude(next.spectralExclude || []);
    setHasHabitableMode(next.hasHabitableMode);
    setPageSize(next.pageSize);
  };

  const buildBaseParamsFromFilters = (filters) => {
    const params = {};
    const distMin = Math.min(filters.minDist, filters.maxDist);
    const distMax = Math.max(filters.minDist, filters.maxDist);
    const starsMin = Math.min(filters.minStarCount, filters.maxStarCount);
    const starsMax = Math.max(filters.minStarCount, filters.maxStarCount);
    const planetsMin = Math.min(filters.minPlanetCount, filters.maxPlanetCount);
    const planetsMax = Math.max(filters.minPlanetCount, filters.maxPlanetCount);
    const coolnessMin = Math.min(filters.minCoolnessScore, filters.maxCoolnessScore);
    const coolnessMax = Math.max(filters.minCoolnessScore, filters.maxCoolnessScore);
    const tempMin = Math.min(filters.minTempK, filters.maxTempK);
    const tempMax = Math.max(filters.minTempK, filters.maxTempK);
    if (filters.query.trim()) {
      params.q = filters.query.trim();
    }
    if (distMin > filterLimits.distance.min) {
      params.min_dist_ly = String(distMin);
    }
    if (distMax < filterLimits.distance.max) {
      params.max_dist_ly = String(distMax);
    }
    if (starsMin > filterLimits.stars.min) {
      params.min_star_count = String(starsMin);
    }
    if (starsMax < filterLimits.stars.max) {
      params.max_star_count = String(starsMax);
    }
    if (planetsMin > filterLimits.planets.min) {
      params.min_planet_count = String(planetsMin);
    }
    if (planetsMax < filterLimits.planets.max) {
      params.max_planet_count = String(planetsMax);
    }
    if (coolnessMin > filterLimits.coolness.min) {
      params.min_coolness_score = String(coolnessMin);
    }
    if (coolnessMax < filterLimits.coolness.max) {
      params.max_coolness_score = String(coolnessMax);
    }
    if (tempMin > SPECTRAL_TEMP_MIN_K) {
      params.min_temp_k = String(Math.round(tempMin));
    }
    if (tempMax < SPECTRAL_TEMP_MAX_K) {
      params.max_temp_k = String(Math.round(tempMax));
    }
    const tempEligibleSpectral = spectralClassesForTemperatureRange(tempMin, tempMax);
    const tempEligibleSet = new Set(tempEligibleSpectral);
    const normalizedInclude = Array.from(
      new Set((filters.spectral || []).map((token) => String(token || "").trim().toUpperCase()).filter(Boolean)),
    ).filter((token) => spectralOptions.includes(token));
    const normalizedExclude = Array.from(
      new Set((filters.spectralExclude || []).map((token) => String(token || "").trim().toUpperCase()).filter(Boolean)),
    ).filter((token) => spectralOptions.includes(token));
    const includeOutsideRange = normalizedInclude.filter((token) => !tempEligibleSet.has(token));
    const excludeSet = new Set(normalizedExclude);
    const includeOutsideSet = new Set(includeOutsideRange);
    const effectiveSpectral = spectralOptions.filter((token) => (
      (tempEligibleSet.has(token) && !excludeSet.has(token))
      || includeOutsideSet.has(token)
    ));
    if (effectiveSpectral.length > 0 && effectiveSpectral.length < spectralOptions.length) {
      params.spectral_class = effectiveSpectral.join(",");
    }
    if (includeOutsideRange.length > 0) {
      params.spectral_include = includeOutsideRange.join(",");
    }
    if (normalizedExclude.length > 0) {
      params.spectral_exclude = normalizedExclude.join(",");
    }
    if (filters.hasHabitableMode) {
      params.has_habitable = filters.hasHabitableMode;
    }
    params.sort = filters.sort === "match" && !filters.query.trim() ? "coolness" : filters.sort;
    params.limit = filters.pageSize;
    return params;
  };
  const buildBaseParams = () => buildBaseParamsFromFilters(currentFilterState());
  const shouldFetchDeferredTotal = (baseParams) => {
    const ignored = new Set(["sort", "limit"]);
    const activeKeys = Object.keys(baseParams || {}).filter((key) => !ignored.has(key));
    return activeKeys.length === 0;
  };

  const fetchDeferredTotalCount = async (baseParams, searchToken) => {
    const totalToken = latestTotalTokenRef.current + 1;
    latestTotalTokenRef.current = totalToken;
    try {
      const totalData = await fetchSystems({
        ...baseParams,
        include_total: "true",
        limit: "1",
      });
      if (latestSearchTokenRef.current !== searchToken || latestTotalTokenRef.current !== totalToken) {
        return;
      }
      if (typeof totalData.total_count === "number" && Number.isFinite(totalData.total_count)) {
        setTotalCount(totalData.total_count);
      }
    } catch (_) {
      if (latestSearchTokenRef.current === searchToken && latestTotalTokenRef.current === totalToken) {
        setTotalCount(null);
      }
    }
  };

  const runSearch = async (cursorValue, reset = false, overrideBaseParams = null) => {
    const searchToken = latestSearchTokenRef.current + 1;
    latestSearchTokenRef.current = searchToken;
    const startedAtMs = typeof performance !== "undefined" ? performance.now() : Date.now();
    const resolvedBase =
      (!reset && cursorValue && activeParams)
        ? activeParams
        : (overrideBaseParams || buildBaseParams());
    const requestParams = { ...resolvedBase };
    if (cursorValue) {
      requestParams.cursor = cursorValue;
    }

    setLoading(true);
    setSearchStarted(true);
    setError("");
    if (reset) {
      setTotalCount(null);
      latestTotalTokenRef.current += 1;
    }
    try {
      const data = await fetchSystems(requestParams);
      if (latestSearchTokenRef.current !== searchToken) {
        return;
      }
      const endedAtMs = typeof performance !== "undefined" ? performance.now() : Date.now();
      const clientQueryMs = Math.max(0, endedAtMs - startedAtMs);
      setHasMore(Boolean(data.has_more));
      setCursor(data.next_cursor || null);
      setResults((prev) => (reset ? data.items : [...prev, ...data.items]));
      if (reset && shouldFetchDeferredTotal(resolvedBase)) {
        void fetchDeferredTotalCount(resolvedBase, searchToken);
      } else if (typeof data.total_count === "number") {
        setTotalCount(data.total_count);
      }
      if (reset || !activeParams || overrideBaseParams) {
        setActiveParams(resolvedBase);
      }
      setLastQueryStats({
        mode: reset ? "search" : "load_more",
        returnedCount: Array.isArray(data.items) ? data.items.length : 0,
        serverMs: (typeof data.query_time_ms === "number" && Number.isFinite(data.query_time_ms))
          ? Number(data.query_time_ms)
          : null,
        clientMs: clientQueryMs,
      });
    } catch (err) {
      if (latestSearchTokenRef.current === searchToken) {
        setError(err?.message || "Data temporarily unavailable.");
        if (reset) {
          setTotalCount(null);
        }
      }
    } finally {
      if (latestSearchTokenRef.current === searchToken) {
        setLoading(false);
      }
    }
  };

  const persistParams = () => {
    setSearchParams(buildBaseParams());
  };

  const onSubmit = (event) => {
    event.preventDefault();
    persistParams();
    runSearch(null, true);
  };

  useEffect(() => {
    runSearch(null, true);
  }, []);

  useEffect(() => {
    previewPoolIdsRef.current = new Set(previewPoolAllocations.map((allocation) => String(allocation.systemId)));
  }, [previewPoolAllocations]);

  useEffect(() => {
    setPreviewPoolAllocations((current) => current.filter((allocation) => (
      results.some((item) => String(item.system_id) === String(allocation.systemId))
    )));
  }, [results]);

  const activateNextPreviewFromQueue = React.useCallback(() => {
    setPreviewPoolAllocations((current) => {
      if (current.length >= SEARCH_RESULT_PREVIEW_POOL_SIZE) {
        return current;
      }
      while (previewRequestQueueRef.current.length > 0) {
        const nextSystemId = previewRequestQueueRef.current.shift();
        if (!nextSystemId || current.some((allocation) => String(allocation.systemId) === String(nextSystemId))) {
          continue;
        }
        const stillInResults = results.some((item) => String(item.system_id) === String(nextSystemId));
        if (!stillInResults) {
          continue;
        }
        const next = [...current, { systemId: nextSystemId, slot: current.length }];
        return next.map((allocation, index) => ({ ...allocation, slot: index }));
      }
      return current;
    });
  }, [results]);

  const requestSearchPreview = React.useCallback((systemId) => {
    const key = String(systemId || "");
    if (!key || previewPoolIdsRef.current.has(key) || previewRequestQueueRef.current.includes(key)) {
      return;
    }
    setPreviewPoolAllocations((current) => {
      if (current.some((allocation) => String(allocation.systemId) === key)) {
        return current;
      }
      if (current.length < SEARCH_RESULT_PREVIEW_POOL_SIZE) {
        return [...current, { systemId: key, slot: current.length }];
      }
      previewRequestQueueRef.current = [
        ...previewRequestQueueRef.current.filter((item) => item !== key),
        key,
      ].slice(-SEARCH_RESULT_PREVIEW_POOL_SIZE * 3);
      return current;
    });
  }, []);

  const releaseSearchPreview = React.useCallback((systemId) => {
    const key = String(systemId || "");
    previewRequestQueueRef.current = previewRequestQueueRef.current.filter((item) => item !== key);
    setPreviewPoolAllocations((current) => (
      current
        .filter((allocation) => String(allocation.systemId) !== key)
        .map((allocation, index) => ({ ...allocation, slot: index }))
    ));
    window.setTimeout(() => activateNextPreviewFromQueue(), 80);
  }, [activateNextPreviewFromQueue]);

  const captureSearchPreview = React.useCallback((systemId, dataUrl) => {
    const key = String(systemId || "");
    if (!key || !dataUrl) {
      return;
    }
    setPreviewSnapshotCache((current) => {
      const next = new Map(current);
      next.delete(key);
      next.set(key, { url: dataUrl, capturedAt: Date.now() });
      while (next.size > SEARCH_RESULT_PREVIEW_CACHE_LIMIT) {
        const oldest = next.keys().next().value;
        next.delete(oldest);
      }
      return next;
    });
  }, []);

  const previewAllocationsBySystemId = useMemo(() => {
    const out = new Map();
    previewPoolAllocations.forEach((allocation, index) => {
      out.set(String(allocation.systemId), { ...allocation, slot: allocation.slot ?? index });
    });
    return out;
  }, [previewPoolAllocations]);

  useEffect(() => {
    let active = true;
    setSpectralMixLoading(true);
    setSpectralMixError("");
    fetchSpectralMix()
      .then((data) => {
        if (!active) {
          return;
        }
        setSpectralMix(data || null);
      })
      .catch((err) => {
        if (!active) {
          return;
        }
        setSpectralMixError(err?.message || "Unable to load spectral mix.");
      })
      .finally(() => {
        if (active) {
          setSpectralMixLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  const applyTemperatureRange = (rawMin, rawMax) => {
    const safeMin = clampNumber(Math.round(Number(rawMin)), SPECTRAL_TEMP_MIN_K, SPECTRAL_TEMP_MAX_K);
    const safeMax = clampNumber(Math.round(Number(rawMax)), SPECTRAL_TEMP_MIN_K, SPECTRAL_TEMP_MAX_K);
    setMinTempK(safeMin);
    setMaxTempK(safeMax);
  };

  const toggleSpectral = (value) => {
    const inRange = eligibleSpectralSet.has(value);
    if (inRange) {
      setSpectralExclude((prev) => {
        const nextSet = new Set(
          (prev || []).map((token) => String(token || "").trim().toUpperCase()).filter((token) => spectralOptions.includes(token)),
        );
        if (nextSet.has(value)) {
          nextSet.delete(value);
        } else {
          nextSet.add(value);
        }
        return spectralOptions.filter((token) => nextSet.has(token));
      });
      setSpectral((prev) => (prev || []).filter((token) => String(token || "").toUpperCase() !== value));
      return;
    }
    setSpectral((prev) => {
      const nextSet = new Set(
        (prev || []).map((token) => String(token || "").trim().toUpperCase()).filter((token) => spectralOptions.includes(token)),
      );
      if (nextSet.has(value)) {
        nextSet.delete(value);
      } else {
        nextSet.add(value);
      }
      return spectralOptions.filter((token) => nextSet.has(token));
    });
    setSpectralExclude((prev) => (prev || []).filter((token) => String(token || "").toUpperCase() !== value));
  };

  const resetFilters = () => {
    const next = defaultFilterState();
    applyFilterState(next);
    setSearchParams({});
    runSearch(null, true, buildBaseParamsFromFilters(next));
  };

  const applyPreset = (preset) => {
    const next = {
      ...defaultFilterState(),
      query: query.trim(),
      ...preset.filters,
    };
    applyFilterState(next);
    const params = buildBaseParamsFromFilters(next);
    setSearchParams(params);
    runSearch(null, true, params);
  };

  const onSortChange = (nextSort) => {
    setSort(nextSort);
    const next = { ...currentFilterState(), sort: nextSort };
    const params = buildBaseParamsFromFilters(next);
    setSearchParams(params);
    runSearch(null, true, params);
  };

  const onResultCardClick = (event, systemId) => {
    if (event.defaultPrevented || event.button !== 0) {
      return;
    }
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return;
    }
    if (event.target instanceof Element) {
      const interactive = event.target.closest("a, button, input, select, textarea, label, .star-search-preview, .map-search-card-preview");
      if (interactive) {
        return;
      }
    }
    const selection = window.getSelection?.();
    if (selection && !selection.isCollapsed && selection.toString().trim()) {
      return;
    }
    navigate(`/systems/${systemId}`);
  };

  const openResultInMap = (event, system) => {
    event.preventDefault();
    event.stopPropagation();
    navigate(mapExploreHrefForSystem(system));
  };

  const filtersBodyCollapsed = filtersCollapsedY;
  const searchLayoutClassName = [
    "search-layout",
    filtersCollapsedY ? "filters-collapsed-y" : "",
  ].filter(Boolean).join(" ");
  return (
    <Layout showSearchLink={false} buildId={buildId}>
      <section className={`${searchLayoutClassName} catalog-search-shell`}>
        <div className="filters-stack">
          <form
            className={[
              "map-search-sidebar",
              "catalog-search-sidebar",
              filtersCollapsedY ? "filters-panel-collapsed-y" : "",
            ].filter(Boolean).join(" ")}
            onSubmit={onSubmit}
          >
            <div className="map-search-sidebar-head">
              <span className="map-panel-label">Filters</span>
              <strong>{totalCount !== null ? formatHumanLargeCount(totalCount) : `${formatNumber(results.length, 0)} loaded`}</strong>
            </div>
            <div className={`filters-body ${filtersBodyCollapsed ? "is-collapsed" : ""}`}>
              <div className="catalog-search-presets">
                {FILTER_PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className="map-command-button ghost"
                    onClick={() => applyPreset(preset)}
                    disabled={loading}
                  >
                    {presetLabelForTheme(preset, theme)}
                  </button>
                ))}
              </div>

              <MapStyleRangeControl
                label="Distance"
                minValue={minDist}
                maxValue={maxDist}
                minLimit={filterLimits.distance.min}
                maxLimit={filterLimits.distance.max}
                step={filterLimits.distance.step}
                integer={filterLimits.distance.integer}
                format={(value) => `${formatNumber(value, 0)} ly`}
                onChangeMin={setMinDist}
                onChangeMax={setMaxDist}
              />

              <MapStyleRangeControl
                label="Stars"
                minValue={minStarCount}
                maxValue={maxStarCount}
                minLimit={filterLimits.stars.min}
                maxLimit={filterLimits.stars.max}
                step={filterLimits.stars.step}
                integer={filterLimits.stars.integer}
                onChangeMin={setMinStarCount}
                onChangeMax={setMaxStarCount}
              />

              <MapStyleRangeControl
                label="Planets"
                minValue={minPlanetCount}
                maxValue={maxPlanetCount}
                minLimit={filterLimits.planets.min}
                maxLimit={filterLimits.planets.max}
                step={filterLimits.planets.step}
                integer={filterLimits.planets.integer}
                onChangeMin={setMinPlanetCount}
                onChangeMax={setMaxPlanetCount}
              />

              <MapStyleRangeControl
                label="Coolness"
                minValue={minCoolnessScore}
                maxValue={maxCoolnessScore}
                minLimit={filterLimits.coolness.min}
                maxLimit={filterLimits.coolness.max}
                step={filterLimits.coolness.step}
                integer={filterLimits.coolness.integer}
                onChangeMin={setMinCoolnessScore}
                onChangeMax={setMaxCoolnessScore}
              />

              <button
                type="button"
                className={`map-search-habitable ${hasHabitableMode === "true" ? "active" : ""}`}
                onClick={() => setHasHabitableMode((current) => (current === "true" ? "" : "true"))}
                aria-pressed={hasHabitableMode === "true"}
                title="Filters to systems with a planet candidate in the broad habitable-zone temperature and mass range."
              >
                Habitable-zone planets
              </button>
              <button type="button" className="map-command-button ghost map-search-reset" onClick={resetFilters}>
                Reset filters
              </button>

              {error && (
                <div className="error-box">
                  <div>{error}</div>
                  <button
                    type="button"
                    className="button ghost retry"
                    onClick={() => runSearch(null, true)}
                    disabled={loading}
                  >
                    Retry
                  </button>
                </div>
              )}
            </div>

            <button
              type="button"
              className={`map-command-button ghost filter-collapse-btn ${filtersCollapsedY ? "active" : ""}`.trim()}
              onClick={() => setFiltersCollapsedY((prev) => !prev)}
              aria-pressed={filtersCollapsedY}
              title={filtersCollapsedY ? "Expand filters" : "Collapse filters"}
            >
              {filtersCollapsedY ? "Expand" : "Collapse"}
            </button>
          </form>

          <SidebarSpectralMixCard
            mix={spectralMix}
            loading={spectralMixLoading}
            error={spectralMixError}
            collapsed={filtersBodyCollapsed}
          />
        </div>

        <section className="results">
          <form className="map-search-topbar catalog-search-topbar" onSubmit={onSubmit}>
            <label className="map-search-main">
              <span className="sr-only">Search systems</span>
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search stars, systems, or catalog IDs..."
                data-global-search-input="true"
                autoFocus
              />
            </label>
            <button type="submit" className="map-command-button primary" disabled={loading}>
              {loading ? "Searching" : "Search"}
            </button>
            <button type="button" className="map-command-button ghost" onClick={resetFilters}>
              Clear
            </button>
            <div className="map-search-spectral-bar" role="group" aria-label="Spectral class filter">
              {spectralOptions.map((option) => {
                const inRange = eligibleSpectralSet.has(option);
                const active = effectiveSpectralSet.has(option);
                const explicitlyIncluded = explicitIncludeOutsideRangeSet.has(option);
                const explicitlyExcluded = inRange && spectralExcludeSet.has(option);
                const overrideHint = explicitlyIncluded
                  ? "Explicit include override"
                  : explicitlyExcluded
                    ? "Explicit exclude override"
                    : inRange
                      ? "Included by temperature range"
                      : "Excluded by temperature range";
                return (
                  <button
                    type="button"
                    key={option}
                    className={[
                      "map-search-spectral",
                      "spectral-chip",
                      `spectral-${option.toLowerCase()}`,
                      active ? "active" : "",
                      explicitlyIncluded ? "explicit-include" : "",
                      explicitlyExcluded ? "explicit-exclude" : "",
                      !inRange && !explicitlyIncluded ? "out-of-range" : "",
                    ].filter(Boolean).join(" ")}
                    onClick={() => toggleSpectral(option)}
                    title={`${option}: ${SPECTRAL_CLASS_INFO[option]?.sentence || "Spectral class filter"} · ${overrideHint}`}
                    aria-pressed={active}
                    aria-label={`${option} spectral class filter`}
                  >
                    {option}
                  </button>
                );
              })}
            </div>
            <MapStyleRangeControl
              label="Temp K"
              minValue={minTempK}
              maxValue={maxTempK}
              minLimit={SPECTRAL_TEMP_MIN_K}
              maxLimit={SPECTRAL_TEMP_MAX_K}
              step={100}
              integer
              format={(value) => formatNumber(value, 0)}
              onChangeMin={(value) => applyTemperatureRange(value, maxTempK)}
              onChangeMax={(value) => applyTemperatureRange(minTempK, value)}
            />
          </form>

          <div className="results-toolbar panel">
            <div className="results-toolbar-head">
              <h3>Star Search</h3>
            </div>

            <div className="results-bottom-row">
              <div className="results-stats-row">
                <div>
                  <strong>{results.length}</strong> loaded
                  {totalCount !== null && (
                    <>
                      {" "}
                      of <strong>{totalCount}</strong>
                    </>
                  )}
                </div>
              </div>

              <div className="results-search-options">
                <label className="results-search-option">
                  <span>Sort</span>
                  <select value={sort} onChange={(event) => onSortChange(event.target.value)}>
                    <option value="match" disabled={!query.trim()}>Relevance</option>
                    <option value="coolness">Coolness</option>
                    <option value="name">Name</option>
                    <option value="distance">Distance</option>
                    <option value="planet_count">Planets</option>
                    <option value="star_count">Stars</option>
                    <option value="hottest">Hottest</option>
                    <option value="coolest">Coolest</option>
                  </select>
                </label>

                <label className="results-search-option">
                  <span>Page size</span>
                  <select value={pageSize} onChange={(event) => setPageSize(event.target.value)}>
                    <option value="25">25</option>
                    <option value="50">50</option>
                    <option value="100">100</option>
                    <option value="200">200</option>
                  </select>
                </label>
              </div>
            </div>
          </div>

          {loading && results.length === 0 && (
            <div className="empty-state">
              <h2>Loading cool systems...</h2>
              <p>Fetching top-ranked systems by the active coolness profile.</p>
            </div>
          )}

          {!searchStarted && !loading && (
            <div className="empty-state">
              <h2>Start typing to search</h2>
              <p>Enter a system name or set filters to begin browsing.</p>
            </div>
          )}

          {searchStarted && !loading && results.length === 0 && (
            <div className="empty-state">
              <h2>No matches found</h2>
              <p>Try relaxing filters or changing the search terms.</p>
              <p className="muted">Hint: try catalog IDs like HIP ####, HD ####, or Gaia source id.</p>
            </div>
          )}

          {results.length > 0 && (
            <div className="results-list">
              {results.map((item) => {
                const displayName = systemDisplayName(item);
                const canonicalName = String(item?.system_name || "").trim();
                const resultTags = buildSearchResultTags(item);
                return (
                <article
                  key={item.system_id}
                  className="result-card"
                  onClick={(event) => onResultCardClick(event, item.system_id)}
                >
                  <div className="result-shell">
                    <div className="result-left-rail">
                      <StarSearchSimulationPreview
                        system={item}
                        displayName={displayName}
                        cachedPreviewImage={previewSnapshotCache.get(String(item.system_id))?.url || ""}
                        liveActive={previewAllocationsBySystemId.has(String(item.system_id))}
                        poolSlot={previewAllocationsBySystemId.get(String(item.system_id))?.slot ?? null}
                        onActivate={requestSearchPreview}
                        onDeactivate={releaseSearchPreview}
                        onCapture={captureSearchPreview}
                      />
                    </div>
                    <div className="result-content">
                      <div className="result-header">
                        <div>
                          <h3>
                            <Link to={`/systems/${item.system_id}`} className="result-title-link">
                              {formatText(displayName)}
                            </Link>
                          </h3>
                          {canonicalName && canonicalName !== displayName ? (
                            <div className="muted">Catalog: {formatText(canonicalName)}</div>
                          ) : null}
                          {Array.isArray(item?.display_aliases) && item.display_aliases.length > 0 ? (
                            <div className="muted">Aliases: {item.display_aliases.slice(0, 4).join(" · ")}</div>
                          ) : null}
                          {resultTags.length > 0 ? (
                            <div className="result-tags" aria-label={`${displayName} notable tags`}>
                              {resultTags.map((tag) => (
                                <span className="result-tag" key={`${item.system_id}-${tag.label}`} title={tag.title || undefined}>
                                  {tag.label}
                                </span>
                              ))}
                            </div>
                          ) : null}
                          <StellarClassChips tokens={stellarClassTokensFromSystem(item)} size="compact" className="result-stellar-tags" />
                        </div>
                        <div className="distance" title="Coolness rank">
                          {(item.coolness_rank !== null && item.coolness_rank !== undefined)
                            ? `Rank #${formatNumber(item.coolness_rank, 0)}`
                            : "Rank unlisted"}
                        </div>
                      </div>
                      <div className="result-actions" aria-label={`${displayName} actions`}>
                        <Link className="button compact ghost" to={`/systems/${item.system_id}`}>
                          Detail
                        </Link>
                        <button className="button compact ghost" type="button" onClick={(event) => openResultInMap(event, item)}>
                          Map
                        </button>
                      </div>
                      <div className="result-metrics">
                        <MetricChip
                          label="Distance"
                          value={`${formatNumber(item.dist_ly, 2)} ly`}
                          tooltipLines={[
                            `Parsecs: ${formatNumber(distanceLyToPc(item.dist_ly), 3)} pc`,
                            `Estimated parallax: ${formatNumber(parallaxMasFromDistanceLy(item.dist_ly), 2)} mas`,
                            `RA/Dec: ${formatCoordinate(item.ra_deg)} / ${formatCoordinate(item.dec_deg)} deg`,
                          ]}
                        />
                        <MetricChip
                          label="Stars"
                          value={formatNumber(item.star_count, 0)}
                          tooltipLines={[
                            "Total stars currently grouped in this system record.",
                          ]}
                        />
                        <MetricChip
                          label="Planets"
                          value={formatNumber(item.planet_count, 0)}
                          tooltipLines={[
                            "Confirmed planets linked to this system in core data.",
                          ]}
                        />
                        <MetricChip
                          label="Coolness"
                          value={formatNumber(item.coolness_score, 2)}
                          tooltipLines={() => buildCoolnessTooltipLines(item)}
                        />
                        <MetricChip
                          label="Habitable-like"
                          value={formatNumber(item.coolness_nice_planet_count, 0)}
                          tooltipLines={[
                            "Count of planets matching current habitable-like signal criteria.",
                          ]}
                        />
                        <MetricChip
                          label="Spectral"
                          value={item.spectral_classes?.length ? item.spectral_classes.join(", ") : formatText(item.coolness_dominant_spectral_class)}
                          tooltipLines={() => buildSpectralTooltipLines(item)}
                        />
                      </div>
                    </div>
                  </div>
                  {(() => {
                    const cardCatalogIds = buildSystemCatalogIds(item);
                    return (
                      <div className="result-source">
                        <span className="result-source-text">
                          Source {formatText(item.provenance?.source_catalog)} · {formatText(item.provenance?.source_version)}
                        </span>
                        {cardCatalogIds.length > 0 && (
                          <span className="result-source-ids">
                            {cardCatalogIds.map((entry) => (
                              <span className="result-source-id" key={`${item.system_id}-${entry.label}-${entry.value}`}>
                                <span className="id-chip-label">{entry.label}</span>
                                <code className="result-source-id-value">{entry.value}</code>
                                <CopyButton
                                  value={entry.value}
                                  label={`${entry.label} ID`}
                                  className="id-copy copy-btn-inline"
                                />
                              </span>
                            ))}
                          </span>
                        )}
                      </div>
                    );
                  })()}
                </article>
                );
              })}
            </div>
          )}

          <div className="pagination-row">
            {lastQueryStats && (
              <div className="results-query-note">
                Query {formatNumber(lastQueryStats.serverMs ?? lastQueryStats.clientMs, 1)} ms
                {lastQueryStats.serverMs !== null
                  ? ` server · ${formatNumber(lastQueryStats.clientMs, 1)} ms end-to-end`
                  : " end-to-end"}
                {typeof totalCount === "number" && Number.isFinite(totalCount)
                  ? ` · returned ${formatNumber(lastQueryStats.returnedCount, 0)} of ${formatHumanLargeCount(totalCount)} rows`
                  : ` · returned ${formatNumber(lastQueryStats.returnedCount, 0)} rows`}
                {lastQueryStats.mode === "load_more" ? " (page append)" : ""}
              </div>
            )}
            {hasMore && (
              <button
                className="button ghost load-more"
                onClick={() => runSearch(cursor, false)}
                disabled={loading}
              >
                {loading ? "Loading..." : "Load more"}
              </button>
            )}
          </div>
        </section>
      </section>
    </Layout>
  );
}

function ProvenanceBlock({ provenance, grouping = null }) {
  if (!provenance) {
    return null;
  }
  const redistribution =
    provenance.redistribution_ok === true
      ? "Allowed"
      : provenance.redistribution_ok === false
        ? "Restricted"
        : "Unknown";
  const groupingSources = parseJsonArray(grouping?.grouping_source_catalogs_json);
  const groupingSourceText = groupingSourceLabel(grouping?.grouping_basis, groupingSources);
  const evidenceCatalogs = collectSystemEvidenceCatalogs(grouping);
  const evidenceText = formatEvidenceSummary(evidenceCatalogs);
  return (
    <div className="provenance">
      {grouping?.grouping_basis ? (
        <>
          <div>
            <strong>Grouping</strong>
            <span>{formatText(grouping.grouping_basis)}</span>
          </div>
          <div>
            <strong>Grouping source</strong>
            <span>{groupingSourceText}</span>
          </div>
          <div>
            <strong>Evidence catalogs</strong>
            <span>{evidenceText}</span>
          </div>
          {grouping?.wds_id ? (
            <div>
              <strong>Grouping key</strong>
              <span>WDS {formatText(grouping.wds_id)}</span>
            </div>
          ) : null}
        </>
      ) : null}
      <div>
        <strong>Source</strong>
        <span>{formatText(provenance.source_catalog)} {formatText(provenance.source_version)}</span>
      </div>
      <div>
        <strong>License</strong>
        <span>{formatText(provenance.license)}</span>
      </div>
      <div>
        <strong>Redistribution</strong>
        <span>
          {redistribution}
          {provenance.redistribution_ok === false && (
            <span className="warning-chip">Restricted</span>
          )}
        </span>
      </div>
      <div>
        <strong>Retrieved</strong>
        <span>{formatText(provenance.retrieved_at)}</span>
      </div>
      <div>
        <strong>Transform</strong>
        <span>{formatText(provenance.transform_version)}</span>
      </div>
      <div>
        <strong>Source URL</strong>
        <span>
          {provenance.source_url ? (
            <a href={String(provenance.source_url)} target="_blank" rel="noreferrer">
              Open source page
            </a>
          ) : (
            "Unknown"
          )}
        </span>
      </div>
    </div>
  );
}

function SystemDetailPage({ buildId = "" }) {
  const { systemId } = useParams();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");
  const fromMap = searchParams.get("from") === "map";
  const mapReturnToken = String(searchParams.get("map_return") || "").replace(/[^a-zA-Z0-9_-]/g, "");
  const mapReturnPath = mapReturnToken ? `/map?restore=${encodeURIComponent(mapReturnToken)}` : "/map";

  React.useEffect(() => {
    let isActive = true;
    setLoading(true);
    setError("");
    fetchSystemDetail(systemId)
      .then((payload) => {
        if (isActive) {
          setData(payload);
          updateLcarsHistoryWithSystem(payload?.system);
        }
      })
      .catch(() => {
        if (isActive) {
          setError("System not found.");
        }
      })
      .finally(() => {
        if (isActive) {
          setLoading(false);
        }
      });
    return () => {
      isActive = false;
    };
  }, [systemId]);

  if (loading) {
    return (
      <Layout buildId={buildId} showSearchLink={false} headerExtra={<RouteHeaderSearchBar />}>
        <div className="panel">Loading system details...</div>
      </Layout>
    );
  }

  if (error || !data) {
    return (
      <Layout buildId={buildId} showSearchLink={false} headerExtra={<RouteHeaderSearchBar />}>
        <div className="panel">
          <h2>System not found</h2>
          <p>{error || "No data returned."}</p>
          <button className="button ghost" onClick={() => navigate("/search")}>Back to search</button>
        </div>
      </Layout>
    );
  }

  const { system, stars, planets, eclipsing_binaries: eclipsingBinaries = [], hierarchy = null } = data;
  const currentSystemDisplayName = systemDisplayName(system);
  const systemAliasSummary = formatAliasSummary(system?.aliases, {
    exclude: [currentSystemDisplayName, system?.system_name],
    limit: 10,
  });
  const armSummary = system?.arm_evidence_summary || {};
  const systemTags = buildSearchResultTags({
    ...system,
    spectral_classes: Array.from(new Set((stars || [])
      .map((star) => String(star.spectral_class || star.spectral_type_raw || "").trim().slice(0, 1).toUpperCase())
      .filter(Boolean))),
  }, { limit: 24 });

  return (
    <Layout showSearchLink={false} buildId={buildId} headerExtra={<RouteHeaderSearchBar mapSystem={system} />}>
      <section className="detail system-detail-v2">
        {fromMap && (
          <div className="map-return-banner">
            <div>
              <strong>Opened from the 3D map</strong>
              <span>Return to the local star map to keep exploring nearby systems.</span>
            </div>
            <Link to={mapReturnPath} className="button map-return-button">Back to 3D map</Link>
          </div>
        )}

        <section className="system-detail-hero panel">
          <div className="system-detail-hero-copy">
            <span className="system-story-kicker">Star Search system page</span>
            <h1>{formatText(currentSystemDisplayName)}</h1>
            {systemAliasSummary ? (
              <p className="system-alias-line">Also cataloged as {systemAliasSummary}</p>
            ) : null}
            <div className="system-detail-facts">
              <SystemFactPill label="Distance" value={`${formatNumber(system.dist_ly, 2)} ly`} />
              <SystemFactPill label="Stars" value={formatNumber(system.star_count, 0)} />
              <SystemFactPill label="Planets" value={formatNumber(system.planet_count, 0)} />
              <SystemFactPill label="Spectral" value={bestSpectralSummary(system, stars)} />
              <SystemFactPill label="Coolness" value={formatNumber(system.coolness_score, 1)} />
            </div>
            <StellarClassChips tokens={stellarClassTokensFromSystem({
              ...system,
              spectral_classes: Array.from(new Set((stars || [])
                .map((star) => String(star.spectral_class || star.spectral_type_raw || "").trim().toUpperCase())
                .filter(Boolean))),
            })} className="system-detail-stellar-tags" />
            {systemTags.length > 0 ? (
              <div className="result-tags system-detail-tags" aria-label={`${currentSystemDisplayName} discovery tags`}>
                {systemTags.map((tag) => (
                  <span className="result-tag" key={`${system.system_id}-${tag.label}`} title={tag.title || undefined}>
                    {tag.label}
                  </span>
                ))}
              </div>
            ) : null}
            <div className="id-line id-line-inline system-detail-ids">
              <CatalogIdChip label="HIP" value={system.hip_id_text ?? system.hip_id} />
              <CatalogIdChip label="HD" value={system.hd_id_text ?? system.hd_id} />
              <CatalogIdChip label="Gaia" value={resolvedSystemGaiaId(system)} />
            </div>
          </div>
        </section>

        <React.Suspense fallback={<section className="panel system-preview-panel">Loading System Simulation...</section>}>
          <SystemPreviewPanel systemId={system.system_id} systemName={currentSystemDisplayName} snapshot={system.snapshot} />
        </React.Suspense>

        <SystemNarrativeScaffold system={system} stars={stars} planets={planets} hierarchy={hierarchy} />

        <section className="quick-facts system-knowledge-strip">
          <div>
            <strong>Distance</strong>
            <span>{formatNumber(system.dist_ly, 2)} ly ({formatNumber(distanceLyToPc(system.dist_ly), 2)} pc)</span>
          </div>
          <div>
            <strong>Sky Position</strong>
            <span>RA {formatCoordinate(system.ra_deg)} / Dec {formatCoordinate(system.dec_deg)} deg</span>
          </div>
          <div>
            <strong>Galactic Map XYZ</strong>
            <span>{formatCoordinate(system.x_helio_ly)}, {formatCoordinate(system.y_helio_ly)}, {formatCoordinate(system.z_helio_ly)} ly</span>
          </div>
          <div>
            <strong>Database Evidence</strong>
            <span>{formatEvidenceSummary(collectSystemEvidenceCatalogs(system))}</span>
          </div>
          <div>
            <strong>ARM Evidence</strong>
            <span>
              {formatNumber(armSummary.stars_with_arm_evidence ?? 0, 0)} stars
              {armSummary.high_variability_stars ? ` · ${formatNumber(armSummary.high_variability_stars, 0)} high variability` : ""}
            </span>
          </div>
        </section>

        <ConceptExplainerGrid />

        <SystemHierarchyPanel hierarchy={hierarchy} />

        <details className="panel detail-disclosure">
          <summary>
            <span>Stars and Catalog Rows</span>
            <strong>{formatNumber(stars.length, 0)} rows</strong>
          </summary>
          {stars.length === 0 && <p className="muted">No star members recorded.</p>}
          {stars.length > 0 && (
            <div className="table">
              {stars.map((star) => (
                <div className="row" key={star.star_id}>
                  {(() => {
	                    const record = starCatalogRecordLink(star);
	                    const currentStarDisplayName = starDisplayName(star);
	                    const starEvidence = collectStarEvidenceCatalogs(star);
	                    const armEvidenceDetails = formatArmEvidenceDetails(star?.arm_evidence);
	                    const starAliasSummary = formatAliasSummary(star?.aliases, {
	                      exclude: [currentStarDisplayName, star?.star_name],
	                      limit: 6,
                    });
                    return (
                      <>
                        <div>
                          <strong className="star-name">{formatText(currentStarDisplayName)}</strong>
                          {star.component ? (
                            <div className="muted">Component {formatText(star.component)}</div>
                          ) : null}
                          {starAliasSummary ? (
                            <div className="muted">Aliases: {starAliasSummary}</div>
                          ) : null}
                        </div>
                        <div>
                          <span>Spectral: {formatText(star.spectral_type_raw)}</span>
                          <span className="muted">
                            Class {formatText(star.spectral_class)} {formatText(star.luminosity_class)} ·
                            Subtype {formatText(star.spectral_subtype)} ·
                            Peculiar {formatText(star.spectral_peculiar)}
                          </span>
                        </div>
                        <div>
                          <span>Distance {formatNumber(star.dist_ly, 2)} ly</span>
                          <span className="muted">Vmag {formatNumber(star.vmag, 2)}</span>
                        </div>
                        <div className="muted">
                          IDs
	                          <div className="id-line">
	                            <CatalogIdChip label="Gaia" value={resolvedEntityGaiaId(star)} />
	                            <CatalogIdChip label="HIP" value={star.hip_id_text ?? star.hip_id} />
	                            <CatalogIdChip label="HD" value={star.hd_id_text ?? star.hd_id} />
	                            <CatalogIdChip label="SBX" value={star.sbx_sn} hideWhenMissing />
	                          </div>
	                        </div>
	                        <div className="muted">
	                          Source {formatText(star.provenance?.source_catalog)} · {formatText(star.provenance?.source_version)}
	                        </div>
		                        <div className="muted">
		                          Evidence {formatEvidenceSummary(starEvidence)}
		                        </div>
		                        {armEvidenceDetails ? (
		                          <div className="muted">
		                            Arm {armEvidenceDetails}
		                          </div>
		                        ) : null}
		                        <div className="muted">
		                          Catalog record{" "}
                          {record ? (
                            <a href={record.url} target="_blank" rel="noreferrer">{record.label}</a>
                          ) : (
                            "Unavailable for this source"
                          )}
                          {record?.note ? ` · ${record.note}` : ""}
                        </div>
                      </>
                    );
                  })()}
                </div>
              ))}
            </div>
          )}
        </details>

        <details className="panel detail-disclosure">
          <summary>
            <span>Planets and Orbits</span>
            <strong>{formatNumber(planets.length, 0)} rows</strong>
          </summary>
          {planets.length === 0 && <p className="muted">No confirmed exoplanets recorded.</p>}
          {planets.length > 0 && (
            <div className="table">
              {planets.map((planet) => (
                <div className="row" key={planet.planet_id}>
                  {(() => {
                    const record = planetCatalogRecordLink(planet);
                    return (
                      <>
                        <div>
                          <strong className="planet-name">{formatText(planet.planet_name)}</strong>
                          <div className="muted">
                            Discovery {formatText(planet.disc_year)} · {formatText(planet.discovery_method)} · {formatText(planet.discovery_facility)}
                          </div>
                          <div className="muted">
                            Telescope {formatText(planet.discovery_telescope)} · Instrument {formatText(planet.discovery_instrument)}
                          </div>
                        </div>
                        <div>
                          <span>Period {formatPeriodDaysWithYears(planet.orbital_period_days)}</span>
                          <span className="muted">
                            SMA {formatNumber(planet.semi_major_axis_au, 3)} AU · Eccentricity {formatNumber(planet.eccentricity, 3)}
                          </span>
                        </div>
                        <div>
                          <span>
                            Radius {formatNumber(planet.radius_earth, 2)} Earth / {formatNumber(planet.radius_jup, 2)} Jupiter
                          </span>
                          <span className="muted">
                            Mass {formatNumber(planet.mass_earth, 2)} Earth / {formatNumber(planet.mass_jup, 2)} Jupiter
                          </span>
                        </div>
                        <div className="muted">
                          Match {formatText(planet.match_method)} · {formatConfidence(planet.match_confidence)}
                          {(planet.match_confidence ?? 1) < 0.7 && (
                            <span className="warning-chip">Low confidence</span>
                          )}
                        </div>
                        <div className="muted">
                          Eq Temp {formatNumber(planet.eq_temp_k, 1)} K · Insolation {formatNumber(planet.insol_earth, 2)} Earth
                        </div>
                        <div className="muted">
                          Notes {formatText(planet.match_notes)}
                        </div>
                        <div className="muted">
                          Source {formatText(planet.provenance?.source_catalog)} · {formatText(planet.provenance?.source_version)}
                        </div>
                        <div className="muted">
                          Catalog record{" "}
                          {record ? (
                            <a href={record.url} target="_blank" rel="noreferrer">{record.label}</a>
                          ) : (
                            "Unavailable for this source"
                          )}
                        </div>
                      </>
                    );
                  })()}
                </div>
              ))}
            </div>
          )}
        </details>

        <details className="panel detail-disclosure">
          <summary>
            <span>Eclipsing Evidence</span>
            <strong>{formatNumber(eclipsingBinaries.length, 0)} rows</strong>
          </summary>
          {eclipsingBinaries.length === 0 && <p className="muted">No eclipsing-binary catalog evidence linked to this system.</p>}
          {eclipsingBinaries.length > 0 && (
            <div className="table">
              {eclipsingBinaries.map((entry) => {
                const record = eclipsingCatalogRecordLink(entry);
                return (
                  <div className="row" key={entry.eclipsing_binary_id}>
                    <div>
                      <strong>{formatText(entry.object_name || entry.source_catalog_object_id)}</strong>
                      <div className="muted">{formatText(entry.source_catalog_object_id)}</div>
                    </div>
                    <div>
                      <span>Period {formatPeriodDaysWithYears(entry.period_days)}</span>
                      <span className="muted">
                        Morphology {formatNumber(entry.morphology, 3)} · Kmag {formatNumber(entry.kmag, 2)}
                      </span>
                    </div>
                    <div className="muted">
                      Match {formatText(entry.match_method)} · {formatConfidence(entry.match_confidence)}
                      {(entry.match_confidence ?? 1) < 0.8 && (
                        <span className="warning-chip">Low confidence</span>
                      )}
                    </div>
                    <div className="muted">
                      Source {formatText(entry.provenance?.source_catalog)} · {formatText(entry.provenance?.source_version)}
                    </div>
                    <div className="muted">
                      Catalog record{" "}
                      {record ? (
                        <a href={record.url} target="_blank" rel="noreferrer">{record.label}</a>
                      ) : (
                        "Unavailable for this source"
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </details>

        <details className="panel detail-disclosure">
          <summary>
            <span>Evidence and Technical Provenance</span>
            <strong>Source chain</strong>
          </summary>
          <ProvenanceBlock provenance={system.provenance} grouping={system} />
          {system.snapshot ? (
            <div className="snapshot-technical-note">
              <SnapshotMetadata system={system} snapshot={system.snapshot} />
            </div>
          ) : null}
        </details>
      </section>
    </Layout>
  );
}

export default function App() {
  const [theme, setTheme] = useState(() => resolveInitialTheme());
  const [buildId, setBuildId] = useState("");

  useEffect(() => {
    if (!THEME_IDS.has(theme)) {
      return;
    }
    document.documentElement.setAttribute("data-theme", theme);
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch (_) {
      // Ignore persistence failures in restricted browser contexts.
    }
  }, [theme]);

  useEffect(() => {
    let active = true;
    fetchHealth()
      .then((payload) => {
        if (!active) {
          return;
        }
        setBuildId(String(payload?.build_id || "").trim());
      })
      .catch(() => {
        if (!active) {
          return;
        }
        setBuildId("");
      });
    return () => {
      active = false;
    };
  }, []);

  const themeContextValue = useMemo(
    () => ({
      theme,
      setTheme,
      options: THEME_OPTIONS,
    }),
    [theme],
  );

  return (
    <ThemeContext.Provider value={themeContextValue}>
      <Routes>
        <Route
          path="/"
          element={(
            <React.Suspense fallback={<div className="route-loading">Loading map...</div>}>
              <StarMapPage
                buildId={buildId}
                theme={theme}
                setTheme={setTheme}
                themeOptions={THEME_OPTIONS}
                defaultSearchOpen
              />
            </React.Suspense>
          )}
        />
        <Route path="/classic-search" element={<SearchPage buildId={buildId} />} />
        <Route path="/search" element={<SearchPage buildId={buildId} />} />
        <Route path="/help" element={<HelpPage buildId={buildId} />} />
        <Route path="/about" element={<AboutPage buildId={buildId} />} />
        <Route path="/data" element={<DataPage buildId={buildId} />} />
        <Route
          path="/map"
          element={(
            <React.Suspense fallback={<div className="route-loading">Loading map...</div>}>
              <StarMapPage
                buildId={buildId}
                theme={theme}
                setTheme={setTheme}
                themeOptions={THEME_OPTIONS}
              />
            </React.Suspense>
          )}
        />
        <Route path="/systems/:systemId" element={<SystemDetailPage buildId={buildId} />} />
      </Routes>
    </ThemeContext.Provider>
  );
}
