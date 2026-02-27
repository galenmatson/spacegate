import React, { useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Link, Route, Routes, useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { fetchSystemDetail, fetchSystems } from "./api.js";

const spectralOptions = ["O", "B", "A", "F", "G", "K", "M", "L"];
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
  { id: "planet_rich", label: "Planets", filters: { sort: "coolness", hasPlanetsMode: "true", minPlanetCount: 1 } },
  { id: "habitable_like", label: "Habitability", filters: { sort: "coolness", hasPlanetsMode: "true", hasHabitableMode: "true", maxDist: 200 } },
  { id: "high_coolness", label: "Cool", filters: { sort: "coolness", minCoolnessScore: 20 } },
];
const SPECTRAL_CLASS_INFO = {
  O: { sentence: "Very hot blue stars with intense ultraviolet output and short lifetimes.", tempRangeK: [30000, 50000] },
  B: { sentence: "Hot blue-white stars that are luminous and relatively short-lived.", tempRangeK: [10000, 30000] },
  A: { sentence: "White stars with strong hydrogen lines and comparatively high luminosity.", tempRangeK: [7500, 10000] },
  F: { sentence: "Yellow-white stars slightly hotter and more massive than the Sun.", tempRangeK: [6000, 7500] },
  G: { sentence: "Yellow dwarf stars like the Sun, often stable over long timescales.", tempRangeK: [5200, 6000] },
  K: { sentence: "Orange stars that are cooler than the Sun and often long-lived.", tempRangeK: [3700, 5200] },
  M: { sentence: "Cool red dwarfs, the most common stellar class in the Milky Way.", tempRangeK: [2400, 3700] },
  L: { sentence: "Very cool red-brown objects at the star-brown dwarf boundary.", tempRangeK: [1300, 2400] },
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
const LCARS_TEXT_ROW_COUNT = 5;
const LCARS_TEXT_MAX_SLOTS = LCARS_TEXT_SLOTS_PER_LINE * LCARS_TEXT_ROW_COUNT;
const GLOBAL_SEARCH_INPUT_SELECTOR = "input[data-global-search-input='true']";
const HEADER_ABOUT_LINK = "/about";
const HEADER_SPONSOR_LINK = "https://github.com/sponsors/galenmatson";
const HEADER_ABOUT_TITLE = "About this site";
const HEADER_SPONSOR_TITLE = "Support this project";
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

function pickRandomSystems(items, count) {
  const systems = Array.from(
    new Map(
      (Array.isArray(items) ? items : [])
        .map((item) => ({
          system_id: item?.system_id,
          system_name: String(item?.system_name || "").trim(),
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
              system_name: String(item?.system_name || "").trim(),
            };
          }
          if (item?.gaia_id !== null && item?.gaia_id !== undefined) {
            return {
              gaia: String(item.gaia_id).trim(),
              system_id: item?.system_id,
              system_name: String(item?.system_name || "").trim(),
            };
          }
          const stable = String(item?.stable_object_key || "");
          const match = stable.match(/(?:^|:)gaia:(\d+)/i);
          return match?.[1]
            ? {
              gaia: String(match[1]).trim(),
              system_id: item?.system_id,
              system_name: String(item?.system_name || "").trim(),
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
  const systemName = String(system?.system_name || "").trim();
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
  const formatValue = (value) => (
    integer ? String(Math.round(value)) : Number(value).toFixed(step < 1 ? 1 : 0)
  );
  const displayUnit = unit ? ` ${unit}` : "";
  const span = maxLimit - minLimit;
  const minPercent = span > 0 ? ((valueMin - minLimit) / span) * 100 : 0;
  const maxPercent = span > 0 ? ((valueMax - minLimit) / span) * 100 : 100;
  const fillLeft = clampNumber(minPercent, 0, 100);
  const fillWidth = clampNumber(maxPercent - minPercent, 0, 100);

  return (
    <div className="field compact-range">
      <div className="compact-range-head">
        <span>{label}</span>
        <small>{formatValue(valueMin)} - {formatValue(valueMax)}{displayUnit}</small>
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

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  if (typeof value === "number") {
    return value.toLocaleString(undefined, { maximumFractionDigits: digits });
  }
  return String(value);
}

function formatText(value) {
  if (value === null || value === undefined || value === "") {
    return "Unknown";
  }
  return String(value);
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
  const dayLabel = `${formatNumber(days, 2)} d`;
  if (!Number.isFinite(days) || days <= 365.25) {
    return dayLabel;
  }
  const years = days / 365.25;
  const yearDigits = years >= 100 ? 1 : 2;
  return `${dayLabel} (${formatNumber(years, yearDigits)} y)`;
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

function CatalogIdChip({ label, value }) {
  const normalized = value === null || value === undefined ? "" : String(value).trim();
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
    { label: "Gaia", value: resolvedSystemGaiaId(system) },
    { label: "HIP", value: system?.hip_id_text ?? system?.hip_id },
    { label: "HD", value: system?.hd_id_text ?? system?.hd_id },
  ];
  return entries
    .map((entry) => ({
      ...entry,
      value: entry.value === null || entry.value === undefined ? "" : String(entry.value).trim(),
    }))
    .filter((entry) => entry.value !== "");
}

function MetricChip({ label, value, tooltipLines = [] }) {
  const lines = tooltipLines.filter((line) => Boolean(line));
  return (
    <div className="metric-chip" tabIndex={0}>
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

function splitDownloadUrls(raw) {
  if (!raw) {
    return [];
  }
  return Array.from(
    new Set(
      String(raw)
        .split(";")
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  );
}

function isCodebergLfsObjectUrl(url) {
  return /https?:\/\/codeberg\.org\/.+\.git\/info\/lfs\/objects\/[0-9a-f]{64}$/i.test(String(url || ""));
}

function resolveDownloadLinks(provenance) {
  const sourceUrl = provenance?.source_url ? String(provenance.source_url) : "";
  const urls = splitDownloadUrls(provenance?.source_download_url);
  if (!urls.length) {
    return [];
  }
  const nonLfsUrls = urls.filter((url) => !isCodebergLfsObjectUrl(url));
  return nonLfsUrls.filter((url) => !sourceUrl || url !== sourceUrl);
}

function SnapshotVisual({ snapshot, systemName, compact = false }) {
  const hasImage = Boolean(snapshot?.url);
  if (!hasImage) {
    return (
      <div className={`snapshot-fallback ${compact ? "compact" : ""}`}>
        <span>Snapshot pending</span>
        <small>Run the snapshot generator for this build to populate deterministic visuals.</small>
      </div>
    );
  }

  const labelBits = [];
  if (snapshot?.view_type) {
    labelBits.push(String(snapshot.view_type));
  }
  if (snapshot?.params_hash) {
    labelBits.push(String(snapshot.params_hash).slice(0, 8));
  }

  return (
    <figure className={`snapshot-frame ${compact ? "compact" : ""}`}>
      <img src={snapshot.url} alt={`${formatText(systemName)} deterministic system snapshot`} loading="lazy" />
      {labelBits.length > 0 && (
        <figcaption className="snapshot-caption">{labelBits.join(" · ")}</figcaption>
      )}
    </figure>
  );
}

function SnapshotMetadata({ system, snapshot }) {
  const rows = [
    { label: "System", value: formatText(system?.system_name), copyValue: system?.system_name, copyLabel: "system name" },
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

function Layout({ children, headerExtra = null, showSearchLink = true }) {
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
          <div className="lcars-top-left">
            {Array.from({ length: LCARS_LEFT_DECORATIVE_CHIP_COUNT }).map((_, idx) => (
              <span
                key={`lcars-deco-left-${idx}`}
                className={`lcars-left-deco ${idx === 0 ? "lcars-left-deco-top" : "lcars-left-deco-bottom"}`}
                aria-hidden={idx !== 0}
              >
                {idx === 0 && (
                  <a
                    href="https://thelcars.com"
                    className="lcars-left-deco-link"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    LCARS interface
                  </a>
                )}
                {idx === 1 && (
                  <span className="lcars-left-deco-bottom-links" aria-label="Site links">
                    <Link to={HEADER_ABOUT_LINK} className="lcars-left-deco-mini-link" title={HEADER_ABOUT_TITLE}>ABT</Link>
                    <a
                      href={HEADER_SPONSOR_LINK}
                      className="lcars-left-deco-mini-link"
                      target="_blank"
                      rel="noreferrer"
                      title={HEADER_SPONSOR_TITLE}
                    >
                      SPT
                    </a>
                  </span>
                )}
              </span>
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
      {isLcars && <div className="lcars-header-bridge" aria-hidden="true" />}
      <header className="site-header">
        {!isLcars && (
          <div className="header-topline">
            <div className="header-top-links" aria-label="Site links">
              <Link to={HEADER_ABOUT_LINK} className="header-top-link" title={HEADER_ABOUT_TITLE}>ABT</Link>
              <a href={HEADER_SPONSOR_LINK} className="header-top-link" target="_blank" rel="noreferrer" title={HEADER_SPONSOR_TITLE}>SPT</a>
            </div>
          </div>
        )}
        <div>
          <div className="eyebrow">Stellar Data Explorer</div>
          <div className="title-row">
            <h1><a href="/" className="title-link">Spacegate</a></h1>
            <p className="header-subtitle">Discover and explore nearby systems, stars, and exoplanets.</p>
          </div>
        </div>
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
          {headerExtra}
          {showSearchLink && <Link to="/" className="button ghost">Search</Link>}
        </div>
      </header>
      <main>{children}</main>
    </div>
  );
}

function AboutPage() {
  return (
    <Layout>
      <section className="detail-layout">
        <section className="panel markdown-panel">
          <MarkdownContent markdown={ABOUT_MARKDOWN} />
        </section>
      </section>
    </Layout>
  );
}

function SearchPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
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
    const value = String(searchParams.get("sort") || "coolness").toLowerCase();
    return ["coolness", "name", "distance"].includes(value) ? value : "coolness";
  });
  const [spectral, setSpectral] = useState(() => {
    const raw = searchParams.get("spectral_class") || "";
    return raw.split(",").map((item) => item.trim().toUpperCase()).filter(Boolean);
  });
  const [hasPlanetsMode, setHasPlanetsMode] = useState(() => {
    const value = searchParams.get("has_planets");
    return value === "true" || value === "false" ? value : "";
  });
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
  const [filtersCollapsedY, setFiltersCollapsedY] = useState(false);

  const spectralSet = useMemo(() => new Set(spectral), [spectral]);
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
    sort: "coolness",
    spectral: [],
    hasPlanetsMode: "",
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
    sort,
    spectral,
    hasPlanetsMode,
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
    setSort(next.sort);
    setSpectral(next.spectral);
    setHasPlanetsMode(next.hasPlanetsMode);
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
    if (filters.spectral.length) {
      params.spectral_class = filters.spectral.join(",");
    }
    if (filters.hasPlanetsMode) {
      params.has_planets = filters.hasPlanetsMode;
    }
    if (filters.hasHabitableMode) {
      params.has_habitable = filters.hasHabitableMode;
    }
    params.sort = filters.sort;
    params.limit = filters.pageSize;
    return params;
  };
  const buildBaseParams = () => buildBaseParamsFromFilters(currentFilterState());

  const runSearch = async (cursorValue, reset = false, overrideBaseParams = null) => {
    const resolvedBase =
      (!reset && cursorValue && activeParams)
        ? activeParams
        : (overrideBaseParams || buildBaseParams());
    const requestParams = { ...resolvedBase };
    if (reset) {
      requestParams.include_total = "true";
    }
    if (cursorValue) {
      requestParams.cursor = cursorValue;
    }

    setLoading(true);
    setSearchStarted(true);
    setError("");
    try {
      const data = await fetchSystems(requestParams);
      setHasMore(Boolean(data.has_more));
      setCursor(data.next_cursor || null);
      setResults((prev) => (reset ? data.items : [...prev, ...data.items]));
      if (reset) {
        setTotalCount(typeof data.total_count === "number" ? data.total_count : null);
      } else if (typeof data.total_count === "number") {
        setTotalCount(data.total_count);
      }
      if (reset || !activeParams || overrideBaseParams) {
        setActiveParams(resolvedBase);
      }
    } catch (err) {
      setError(err?.message || "Data temporarily unavailable.");
      if (reset) {
        setTotalCount(null);
      }
    } finally {
      setLoading(false);
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

  const toggleSpectral = (value) => {
    setSpectral((prev) => {
      if (prev.includes(value)) {
        return prev.filter((item) => item !== value);
      }
      return [...prev, value];
    });
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
      const interactive = event.target.closest("a, button, input, select, textarea, label");
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

  const filtersBodyCollapsed = filtersCollapsedY;
  const searchLayoutClassName = [
    "search-layout",
    filtersCollapsedY ? "filters-collapsed-y" : "",
  ].filter(Boolean).join(" ");

  return (
    <Layout showSearchLink={false}>
      <section className={searchLayoutClassName}>
        <form
          className={[
            "panel",
            "filters-panel",
            filtersCollapsedY ? "filters-panel-collapsed-y" : "",
          ].filter(Boolean).join(" ")}
          onSubmit={onSubmit}
        >
          <div className="filters-head">
            <h3>Filters</h3>
            <div className="filters-head-actions">
              {filtersCollapsedY && (
                <div className="filters-head-presets">
                  {FILTER_PRESETS.map((preset) => (
                    <button
                      key={`head-${preset.id}`}
                      type="button"
                      className="button ghost preset-button preset-button-inline"
                      onClick={() => applyPreset(preset)}
                      disabled={loading}
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
              )}
              <button
                type="button"
                className={`button ghost compact filter-collapse-btn ${filtersCollapsedY ? "active" : ""}`.trim()}
                onClick={() => setFiltersCollapsedY((prev) => !prev)}
                aria-pressed={filtersCollapsedY}
                title={filtersCollapsedY ? "Expand filter height" : "Collapse filters from bottom to top"}
              >
                {filtersCollapsedY ? "Expand" : "Collapse"}
              </button>
            </div>
          </div>

          <div className={`filters-body ${filtersBodyCollapsed ? "is-collapsed" : ""}`}>
            <div className="preset-row">
              {FILTER_PRESETS.map((preset) => (
                <button
                  key={preset.id}
                  type="button"
                  className="button ghost preset-button"
                  onClick={() => applyPreset(preset)}
                  disabled={loading}
                >
                  {preset.label}
                </button>
              ))}
            </div>

            <CompactRangeControl
              label="Distance Range"
              unit="ly"
              minValue={minDist}
              maxValue={maxDist}
              minLimit={filterLimits.distance.min}
              maxLimit={filterLimits.distance.max}
              step={filterLimits.distance.step}
              integer={filterLimits.distance.integer}
              onChangeMin={setMinDist}
              onChangeMax={setMaxDist}
            />

            <CompactRangeControl
              label="Star Count"
              minValue={minStarCount}
              maxValue={maxStarCount}
              minLimit={filterLimits.stars.min}
              maxLimit={filterLimits.stars.max}
              step={filterLimits.stars.step}
              integer={filterLimits.stars.integer}
              onChangeMin={setMinStarCount}
              onChangeMax={setMaxStarCount}
            />

            <CompactRangeControl
              label="Planet Count"
              minValue={minPlanetCount}
              maxValue={maxPlanetCount}
              minLimit={filterLimits.planets.min}
              maxLimit={filterLimits.planets.max}
              step={filterLimits.planets.step}
              integer={filterLimits.planets.integer}
              onChangeMin={setMinPlanetCount}
              onChangeMax={setMaxPlanetCount}
            />

            <CompactRangeControl
              label="Coolness Score"
              minValue={minCoolnessScore}
              maxValue={maxCoolnessScore}
              minLimit={filterLimits.coolness.min}
              maxLimit={filterLimits.coolness.max}
              step={filterLimits.coolness.step}
              integer={filterLimits.coolness.integer}
              onChangeMin={setMinCoolnessScore}
              onChangeMax={setMaxCoolnessScore}
            />

            <TriStateToggle
              label="Confirmed planets"
              value={hasPlanetsMode}
              onChange={setHasPlanetsMode}
            />

            <TriStateToggle
              label="Habitable candidates"
              value={hasHabitableMode}
              onChange={setHasHabitableMode}
            />

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
        </form>

        <section className="results">
          <div className="results-toolbar panel">
            <form className="results-search-row" onSubmit={onSubmit}>
              <button className="button compact search-submit-button" type="submit" disabled={loading}>
                {loading ? "Searching..." : "Search"}
              </button>
              <label className="results-search-field">
                <span className="sr-only">Search systems</span>
                <input
                  type="text"
                  data-global-search-input="true"
                  className="results-search-input"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder="Search systems by name, ID, or catalog key..."
                  autoFocus
                />
              </label>
              <button type="button" className="button ghost compact" onClick={resetFilters} disabled={loading}>
                Clear
              </button>
            </form>

            <div className="results-spectral-row">
              <span className="results-spectral-label">Spectral</span>
              <div className="results-spectral-chips">
                {spectralOptions.map((option) => (
                  <button
                    type="button"
                    key={option}
                    className={`chip spectral-chip spectral-${option.toLowerCase()} ${spectralSet.has(option) ? "active" : ""}`}
                    onClick={() => toggleSpectral(option)}
                    title={`${option}: ${SPECTRAL_CLASS_INFO[option]?.sentence || "Spectral class filter"}`}
                  >
                    {option}
                  </button>
                ))}
              </div>
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
                    <option value="coolness">Coolness</option>
                    <option value="name">Name</option>
                    <option value="distance">Distance</option>
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
              {results.map((item) => (
                <article
                  key={item.system_id}
                  className="result-card"
                  onClick={(event) => onResultCardClick(event, item.system_id)}
                >
                  <div className="result-shell">
                    <div className="result-left-rail">
                      <Link to={`/systems/${item.system_id}`} className="result-snapshot-link">
                        <SnapshotVisual snapshot={item.snapshot} systemName={item.system_name} compact />
                      </Link>
                    </div>
                    <div className="result-content">
                      <div className="result-header">
                        <div>
                          <h3>
                            <Link to={`/systems/${item.system_id}`} className="result-title-link">
                              {formatText(item.system_name)}
                            </Link>
                          </h3>
                        </div>
                        <div className="distance" title="Coolness rank">
                          {(item.coolness_rank !== null && item.coolness_rank !== undefined)
                            ? `Rank #${formatNumber(item.coolness_rank, 0)}`
                            : "Rank unlisted"}
                        </div>
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
                          tooltipLines={buildCoolnessTooltipLines(item)}
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
                          tooltipLines={buildSpectralTooltipLines(item)}
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
              ))}
            </div>
          )}

          <div className="pagination-row">
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

function ProvenanceBlock({ provenance }) {
  if (!provenance) {
    return null;
  }
  const downloadLinks = resolveDownloadLinks(provenance);
  const redistribution =
    provenance.redistribution_ok === true
      ? "Allowed"
      : provenance.redistribution_ok === false
        ? "Restricted"
        : "Unknown";
  return (
    <div className="provenance">
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
      {downloadLinks.length > 0 && (
        <div>
          <strong>Download URL</strong>
          <span>
            {downloadLinks.map((url, idx) => (
              <React.Fragment key={`${url}-${idx}`}>
                {idx > 0 ? " · " : ""}
                <a href={String(url)} target="_blank" rel="noreferrer">
                  {downloadLinks.length > 1 ? `Open download ${idx + 1}` : "Open download"}
                </a>
              </React.Fragment>
            ))}
          </span>
        </div>
      )}
    </div>
  );
}

function SystemDetailPage() {
  const { systemId } = useParams();
  const navigate = useNavigate();
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

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
      <Layout>
        <div className="panel">Loading system details...</div>
      </Layout>
    );
  }

  if (error || !data) {
    return (
      <Layout>
        <div className="panel">
          <h2>System not found</h2>
          <p>{error || "No data returned."}</p>
          <button className="button ghost" onClick={() => navigate("/")}>Back to search</button>
        </div>
      </Layout>
    );
  }

  const { system, stars, planets } = data;

  return (
    <Layout showSearchLink={false}>
      <section className="detail">
        <div className="system-identifiers-row">
          <span className="system-identifiers-name">{formatText(system.system_name)}</span>
          <div className="id-line id-line-inline">
            <CatalogIdChip label="Gaia" value={resolvedSystemGaiaId(system)} />
            <CatalogIdChip label="HIP" value={system.hip_id_text ?? system.hip_id} />
            <CatalogIdChip label="HD" value={system.hd_id_text ?? system.hd_id} />
          </div>
        </div>

        <section className="panel snapshot-panel">
          <h3>System Snapshot</h3>
          <div className="snapshot-panel-layout">
            <SnapshotMetadata system={system} snapshot={system.snapshot} />
            <SnapshotVisual snapshot={system.snapshot} systemName={system.system_name} />
          </div>
        </section>

        <div className="quick-facts">
          <div>
            <strong>Distance</strong>
            <span>{formatNumber(system.dist_ly, 2)} ly ({formatNumber(distanceLyToPc(system.dist_ly), 2)} pc)</span>
          </div>
          <div>
            <strong>RA / Dec</strong>
            <span>{formatCoordinate(system.ra_deg)} / {formatCoordinate(system.dec_deg)} deg</span>
          </div>
          <div>
            <strong>XYZ (helio)</strong>
            <span>
              {formatCoordinate(system.x_helio_ly)}, {formatCoordinate(system.y_helio_ly)}, {formatCoordinate(system.z_helio_ly)}
            </span>
          </div>
          <div>
            <strong>Stars</strong>
            <span>{formatNumber(system.star_count, 0)}</span>
          </div>
          <div>
            <strong>Planets</strong>
            <span>{formatNumber(system.planet_count, 0)}</span>
          </div>
        </div>

        <section className="panel">
          <h3>Stars</h3>
          {stars.length === 0 && <p className="muted">No star members recorded.</p>}
          {stars.length > 0 && (
            <div className="table">
              {stars.map((star) => (
                <div className="row" key={star.star_id}>
                  {(() => {
                    const record = starCatalogRecordLink(star);
                    return (
                      <>
                        <div>
                          <strong className="star-name">{formatText(star.star_name)}</strong>
                          {star.component ? (
                            <div className="muted">Component {formatText(star.component)}</div>
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
                          </div>
                        </div>
                        <div className="muted">
                          Source {formatText(star.provenance?.source_catalog)} · {formatText(star.provenance?.source_version)}
                        </div>
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
        </section>

        <section className="panel">
          <h3>Planets</h3>
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
        </section>

        <section className="panel">
          <h3>Provenance</h3>
          <ProvenanceBlock provenance={system.provenance} />
        </section>
      </section>
    </Layout>
  );
}

export default function App() {
  const [theme, setTheme] = useState(() => resolveInitialTheme());

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
        <Route path="/" element={<SearchPage />} />
        <Route path="/about" element={<AboutPage />} />
        <Route path="/systems/:systemId" element={<SystemDetailPage />} />
      </Routes>
    </ThemeContext.Provider>
  );
}
