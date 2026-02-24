import React, { useEffect, useMemo, useState } from "react";
import { Link, Route, Routes, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { fetchSystemDetail, fetchSystems } from "./api.js";

const spectralOptions = ["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y"];
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

  return (
    <div className="field compact-range">
      <div className="compact-range-head">
        <span>{label}</span>
        <small>{formatValue(valueMin)} - {formatValue(valueMax)}{displayUnit}</small>
      </div>
      <div className="compact-range-body">
        <label className="compact-bound">
          <span>Min</span>
          <input
            type="range"
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
        <label className="compact-bound">
          <span>Max</span>
          <input
            type="range"
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
  return (
    <div className="app">
      <header className="site-header">
        <div>
          <div className="eyebrow">Stellar Data Explorer</div>
          <h1><Link to="/" className="title-link">Spacegate</Link></h1>
          <p>
            Discover and explore nearby systems, stars, and exoplanets.
          </p>
        </div>
        <div className="header-actions">
          {headerExtra}
          {showSearchLink && <Link to="/" className="button ghost">Search</Link>}
        </div>
      </header>
      <main>{children}</main>
    </div>
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

  const sortLabel = {
    coolness: "coolness rank",
    distance: "distance",
    name: "name",
  }[sort] || "name";

  return (
    <Layout showSearchLink={false}>
      <section className="search-layout">
        <form className="panel filters-panel" onSubmit={onSubmit}>
          <div className="filters-head">
            <h3>Filters</h3>
          </div>

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
            label="Has confirmed planets"
            value={hasPlanetsMode}
            onChange={setHasPlanetsMode}
          />

          <TriStateToggle
            label="Habitable-like candidates"
            value={hasHabitableMode}
            onChange={setHasHabitableMode}
          />

          <div className="field-grid compact-selects">
            <label className="field">
              <span>Sort</span>
              <select value={sort} onChange={(event) => onSortChange(event.target.value)}>
                <option value="coolness">Coolness (top-ranked)</option>
                <option value="name">Name (A-Z)</option>
                <option value="distance">Distance (nearest)</option>
              </select>
            </label>

            <label className="field">
              <span>Page size</span>
              <select value={pageSize} onChange={(event) => setPageSize(event.target.value)}>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
                <option value="200">200</option>
              </select>
            </label>
          </div>

          <p className="muted">
            Filters support deterministic cursor pagination, so loading more preserves stable ordering.
          </p>

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
        </form>

        <section className="results">
          <div className="results-toolbar panel">
            <form className="results-search-row" onSubmit={onSubmit}>
              <label className="results-search-field">
                <span className="sr-only">Search systems</span>
                <input
                  type="text"
                  className="results-search-input"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder="Search systems by name, ID, or catalog key..."
                  autoFocus
                />
              </label>
              <button className="button compact" type="submit" disabled={loading}>
                {loading ? "Searching..." : "Search"}
              </button>
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

            <div className="results-stats-row">
              <div>
                <strong>{results.length}</strong> loaded
                {totalCount !== null && (
                  <>
                    {" "}
                    of <strong>{totalCount}</strong>
                  </>
                )}
                {", "}sorted by <strong>{sortLabel}</strong>
              </div>
              <div className="muted">
                {searchStarted ? (hasMore ? "More results available" : "End of result set") : "Run search to load results"}
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
                      <div className="result-ids muted">
                        <CatalogIdChip label="Gaia" value={resolvedSystemGaiaId(item)} />
                        <CatalogIdChip label="HIP" value={item.hip_id_text ?? item.hip_id} />
                        <CatalogIdChip label="HD" value={item.hd_id_text ?? item.hd_id} />
                      </div>
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
                      <div className="result-source">
                        Source {formatText(item.provenance?.source_catalog)} · {formatText(item.provenance?.source_version)}
                      </div>
                    </div>
                  </div>
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
  const [quickSearchQuery, setQuickSearchQuery] = React.useState("");
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  const onQuickSearchSubmit = (event) => {
    event.preventDefault();
    const q = quickSearchQuery.trim();
    if (!q) {
      navigate("/");
      return;
    }
    navigate(`/?q=${encodeURIComponent(q)}`);
  };

  React.useEffect(() => {
    let isActive = true;
    setLoading(true);
    setError("");
    fetchSystemDetail(systemId)
      .then((payload) => {
        if (isActive) {
          setData(payload);
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
    <Layout
      headerExtra={(
        <form className="header-search" onSubmit={onQuickSearchSubmit}>
          <span className="sr-only">Search systems</span>
          <input
            type="text"
            value={quickSearchQuery}
            onChange={(event) => setQuickSearchQuery(event.target.value)}
            placeholder="Search systems..."
          />
        </form>
      )}
    >
      <section className="detail">
        <div className="detail-header">
          <div>
            <h2>{formatText(system.system_name)}</h2>
            <p className="muted detail-keyline">
              <span>{formatText(system.stable_object_key)}</span>
              <CopyButton value={system.stable_object_key} label="stable key" className="copy-btn-inline" />
            </p>
          </div>
          <button className="button ghost" onClick={() => navigate("/")}>Back</button>
        </div>

        <div className="quick-facts">
          <div>
            <strong>Distance</strong>
            <span>{formatNumber(system.dist_ly, 2)} ly</span>
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
          <div>
            <strong>Identifiers</strong>
            <div className="id-line">
              <CatalogIdChip label="Gaia" value={resolvedSystemGaiaId(system)} />
              <CatalogIdChip label="HIP" value={system.hip_id_text ?? system.hip_id} />
              <CatalogIdChip label="HD" value={system.hd_id_text ?? system.hd_id} />
            </div>
          </div>
        </div>

        <section className="panel snapshot-panel">
          <h3>System Snapshot</h3>
          <div className="snapshot-panel-layout">
            <SnapshotMetadata system={system} snapshot={system.snapshot} />
            <SnapshotVisual snapshot={system.snapshot} systemName={system.system_name} />
          </div>
          <p className="muted">Metadata is plain selectable text; the image contains only system visualization.</p>
        </section>

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
                          <span>Period {formatNumber(planet.orbital_period_days, 2)} d</span>
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
  return (
    <Routes>
      <Route path="/" element={<SearchPage />} />
      <Route path="/systems/:systemId" element={<SystemDetailPage />} />
    </Routes>
  );
}
