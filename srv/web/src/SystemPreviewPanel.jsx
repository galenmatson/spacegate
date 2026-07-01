import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import { Text } from "@react-three/drei";
import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";
import { apiUrl, fetchSystemSimulationScene } from "./api.js";

const PLANET_COLORS = ["#75b7ff", "#e6c56f", "#e78a6b", "#9dd9a5", "#c49bf2", "#82d6d8", "#d7dee8"];
const SIM_DAYS_PER_SECOND = 0.7;
const SIM_SPEED_OPTIONS = [
  ["0.25", "0.25x"],
  ["1", "1x"],
  ["5", "5x"],
  ["20", "20x"],
  ["100", "100x"],
  ["500", "500x"],
];
const SCALE_MODE_OPTIONS = [
  { value: "structure", label: "Structured", detail: "Collision-safe clarity scale; preserves hierarchy readability." },
  { value: "true_orbits", label: "Orbit", detail: "Preserves linear planet semi-major-axis ratios within the scene." },
  { value: "true_bodies", label: "Body", detail: "Preserves more body-size contrast while keeping targets inspectable." },
  { value: "log", label: "Log", detail: "Compresses body and orbit ranges with logarithmic transforms." },
];
const DEFAULT_VISUAL_SCALE = {
  schema_version: "visual_scale_beta_v1",
  scale_mode: "clarity_scaled_not_physical",
  default_scale_mode: "structure",
  available_scale_modes: SCALE_MODE_OPTIONS,
  star_radius: { fallback_rsun: 0.55, factor: 0.45, min_scene: 0.18, max_scene: 1.35 },
  planet_radius: { fallback_rearth: 1, factor: 0.085, min_scene: 0.105, max_scene: 0.34 },
  planet_orbit_radius: { fallback_au: 0.08, min_scene: 0.75, span_scene: 2.7 },
  binary_orbit_radius: { direct_pair_multiplier: 1, group_pair_motion_multiplier: 0.55 },
  collision_policy: {
    star_radius_fraction_of_nearest_sep: 0.28,
    min_visible_star_radius_scene: 0.045,
    min_halo_radius_scene: 0.16,
    min_pick_radius_scene: 0.28,
  },
};
const STAR_COLORS = {
  O: "#9fc7ff",
  B: "#b8d7ff",
  A: "#dceaff",
  F: "#fff7d2",
  G: "#fff2b7",
  K: "#ffd37d",
  M: "#ff9d6b",
  L: "#ff7b5f",
  T: "#ba8cff",
  Y: "#8dd9ff",
  D: "#d7dee8",
};
const STAR_COLOR_BY_TEMP = [
  [10000, "#b8d7ff"],
  [7500, "#dceaff"],
  [6000, "#fff2b7"],
  [5000, "#ffd37d"],
  [3500, "#ff9d6b"],
  [0, "#ff6f5e"],
];
const PLANET_VISUAL_PALETTES = {
  gas_giant: ["#d6b16d", "#9f714f", "#f0d697", "#6f5146"],
  ice_giant: ["#7ed6e8", "#3d8fba", "#b4f0ff", "#316f8c"],
  hot_rock: ["#3b2521", "#d15a2c", "#f2b06a", "#120e12"],
  temperate_rock: ["#3f7faa", "#6e9d68", "#d3c087", "#25354a"],
  cold_rock: ["#8b9caf", "#d6dee6", "#59687a", "#2f3a48"],
};
const TRUE_BODY_STAR_RADIUS_FACTOR = 0.13;
const EARTH_RADIUS_IN_SOLAR_RADII = 0.0091577;

function numericField(fields, key) {
  const field = fieldRecord(fields, key);
  const value = Number(field?.value);
  return Number.isFinite(value) ? value : null;
}

function fieldStatus(fields, key) {
  const field = fieldRecord(fields, key);
  return field?.status || "missing";
}

function stellarBodyClass(body) {
  return String(
    body?.compact_type
    || body?.body_class
    || fieldRecord(body?.fields, "object_type")?.value
    || "star"
  ).trim().toLowerCase() || "star";
}

function bodyClassLabel(value) {
  return String(value || "star").replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function fieldRecord(fields, key) {
  if (!fields) {
    return null;
  }
  if (Array.isArray(fields)) {
    return fields.find((item) => item?.key === key) || null;
  }
  return fields[key] || null;
}

function hashAngle(value) {
  const text = String(value || "");
  let hash = 0;
  for (let idx = 0; idx < text.length; idx += 1) {
    hash = (hash * 31 + text.charCodeAt(idx)) >>> 0;
  }
  return (hash / 0xffffffff) * Math.PI * 2;
}

function hashUnit(value, salt = "") {
  const text = `${value || ""}:${salt}`;
  let hash = 2166136261;
  for (let idx = 0; idx < text.length; idx += 1) {
    hash ^= text.charCodeAt(idx);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash / 0xffffffff;
}

function formatNumber(value, digits = 1) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Unknown";
  }
  return numeric.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function starColor(teffK) {
  const temp = Number(teffK);
  if (!Number.isFinite(temp)) {
    return "#ff9d6b";
  }
  return STAR_COLOR_BY_TEMP.find(([threshold]) => temp >= threshold)?.[1] || "#ff6f5e";
}

function clampNumber(value, minValue, maxValue) {
  return Math.min(maxValue, Math.max(minValue, value));
}

function mergeVisualScale(scale) {
  return {
    ...DEFAULT_VISUAL_SCALE,
    ...(scale || {}),
    star_radius: { ...DEFAULT_VISUAL_SCALE.star_radius, ...(scale?.star_radius || {}) },
    planet_radius: { ...DEFAULT_VISUAL_SCALE.planet_radius, ...(scale?.planet_radius || {}) },
    planet_orbit_radius: { ...DEFAULT_VISUAL_SCALE.planet_orbit_radius, ...(scale?.planet_orbit_radius || {}) },
    binary_orbit_radius: { ...DEFAULT_VISUAL_SCALE.binary_orbit_radius, ...(scale?.binary_orbit_radius || {}) },
    collision_policy: { ...DEFAULT_VISUAL_SCALE.collision_policy, ...(scale?.collision_policy || {}) },
  };
}

function normalizeScaleMode(value) {
  const mode = String(value || "").trim().toLowerCase();
  if (mode === "clarity" || mode === "clarity_scaled_not_physical") {
    return "structure";
  }
  return SCALE_MODE_OPTIONS.some((option) => option.value === mode) ? mode : "structure";
}

function scaleModeLabel(value) {
  const mode = normalizeScaleMode(value);
  return SCALE_MODE_OPTIONS.find((option) => option.value === mode)?.label || "Structure";
}

function scaleModeDetail(value) {
  const mode = normalizeScaleMode(value);
  return SCALE_MODE_OPTIONS.find((option) => option.value === mode)?.detail || SCALE_MODE_OPTIONS[0].detail;
}

function scaledStarRadius(radiusRsun, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure") {
  const policy = visualScale.star_radius || DEFAULT_VISUAL_SCALE.star_radius;
  const radius = Number(radiusRsun);
  const source = Number.isFinite(radius) && radius > 0 ? radius : Number(policy.fallback_rsun || 0.55);
  const mode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  if (mode === "true_bodies") {
    return clampNumber(source * TRUE_BODY_STAR_RADIUS_FACTOR, 0.018, Number(policy.max_scene || 1.35));
  }
  if (mode === "log") {
    return clampNumber(0.06 + (Math.log1p(source) / Math.log1p(30)) * 0.78, 0.045, Number(policy.max_scene || 1.35));
  }
  return clampNumber(Math.sqrt(source) * Number(policy.factor || 0.45), Number(policy.min_scene || 0.18), Number(policy.max_scene || 1.35));
}

function scaledPlanetRadius(radiusEarth, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure") {
  const policy = visualScale.planet_radius || DEFAULT_VISUAL_SCALE.planet_radius;
  const radius = Number(radiusEarth);
  const source = Number.isFinite(radius) && radius > 0 ? radius : Number(policy.fallback_rearth || 1);
  const mode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  if (mode === "true_bodies") {
    return clampNumber(source * TRUE_BODY_STAR_RADIUS_FACTOR * EARTH_RADIUS_IN_SOLAR_RADII, 0.0015, 0.035);
  }
  if (mode === "log") {
    return clampNumber(0.035 + (Math.log1p(source) / Math.log1p(15)) * 0.22, 0.035, 0.28);
  }
  return clampNumber(Math.sqrt(source) * Number(policy.factor || 0.085), Number(policy.min_scene || 0.105), Number(policy.max_scene || 0.34));
}

function scaledPlanetOrbitRadius(orbitAu, maxOrbitAu, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure") {
  const policy = visualScale.planet_orbit_radius || DEFAULT_VISUAL_SCALE.planet_orbit_radius;
  const orbit = Number(orbitAu);
  const maxOrbit = Math.max(Number(policy.fallback_au || 0.08), Number(maxOrbitAu) || Number(policy.fallback_au || 0.08));
  const source = Number.isFinite(orbit) && orbit > 0 ? orbit : Number(policy.fallback_au || 0.08);
  const mode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  if (mode === "true_orbits") {
    const outerSceneRadius = Number(policy.min_scene || 0.75) + Number(policy.span_scene || 2.7);
    return (source / maxOrbit) * outerSceneRadius;
  }
  if (mode === "log") {
    const fallback = Math.max(0.0001, Number(policy.fallback_au || 0.08));
    const numerator = Math.log1p(source / fallback);
    const denominator = Math.max(0.0001, Math.log1p(maxOrbit / fallback));
    return Number(policy.min_scene || 0.75) + (numerator / denominator) * Number(policy.span_scene || 2.7);
  }
  return Number(policy.min_scene || 0.75) + Math.sqrt(source / maxOrbit) * Number(policy.span_scene || 2.7);
}

function scaledBinaryOrbitRadius(orbit, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure", fallbackRadius = 1.0) {
  const policy = visualScale.binary_orbit_radius || DEFAULT_VISUAL_SCALE.binary_orbit_radius;
  const source = Number(orbit?.display_radius_scene) || Number(fallbackRadius) || 1;
  const mode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  if (mode === "true_bodies") {
    return source * 1.12;
  }
  if (mode === "log") {
    return Math.max(0.44, Math.log1p(source) * 1.35);
  }
  const multiplier = Number(policy.direct_pair_multiplier || 1);
  return source * multiplier;
}

function planetVisualKindToken(value) {
  const token = String(value || "").trim().toLowerCase().replaceAll(" ", "_").replaceAll("-", "_");
  return PLANET_VISUAL_PALETTES[token] ? token : "";
}

function planetVisualKind(planet) {
  const sourceKind = planetVisualKindToken(fieldRecord(planet.fields, "planet_visual_class")?.value);
  if (sourceKind) {
    return sourceKind;
  }
  const radiusEarth = numericField(planet.fields, "radius_earth") || planet.radiusEarth || 1;
  const eqTempK = numericField(planet.fields, "candidate_eq_temp_k");
  const insolEarth = numericField(planet.fields, "candidate_insol_earth");
  if (radiusEarth >= 6) {
    return "gas_giant";
  }
  if (radiusEarth >= 2.1) {
    return "ice_giant";
  }
  if ((eqTempK && eqTempK >= 650) || (insolEarth && insolEarth >= 15)) {
    return "hot_rock";
  }
  if ((eqTempK && eqTempK <= 180) || (insolEarth && insolEarth <= 0.35)) {
    return "cold_rock";
  }
  return "temperate_rock";
}

function planetVisualKindLabel(kind) {
  return String(planetVisualKindToken(kind) || "temperate_rock").replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function planetVisualKindField(planet) {
  const payloadField = fieldRecord(planet.fields, "planet_visual_class");
  if (payloadField) {
    return {
      ...payloadField,
      value: planetVisualKindLabel(payloadField.value),
    };
  }
  const kind = planetVisualKind(planet);
  const radiusField = fieldRecord(planet.fields, "radius_earth");
  const tempField = fieldRecord(planet.fields, "candidate_eq_temp_k");
  const insolField = fieldRecord(planet.fields, "candidate_insol_earth");
  const usableField = (field) => (field?.value !== null && field?.value !== undefined && field?.value !== "" ? field : null);
  const sourceField = kind === "gas_giant" || kind === "ice_giant"
    ? usableField(radiusField)
    : (usableField(tempField) || usableField(insolField) || usableField(radiusField));
  const status = sourceField ? "derived" : "assumed";
  return {
    key: "planet_visual_class",
    label: "Visual class",
    value: planetVisualKindLabel(kind),
    unit: null,
    status,
    layer: "render_scene",
    source_catalog: sourceField?.source_catalog,
    source_reference: sourceField?.source_reference,
    basis: sourceField
      ? `renderer:${kind}:from_${sourceField.key || "available_planet_fields"}`
      : `renderer:${kind}:fallback_visual_prior`,
    seed: sourceField ? null : String(planet.render_key || planet.key || planet.display_name || planet.name || ""),
    generator_version: "system_preview_planet_visual_class_v1",
    confidence: sourceField ? 0.55 : 0.2,
    notes: sourceField
      ? "Presentation-only visual material class derived from available planet radius, temperature, or insolation fields."
      : "Presentation-only visual material class using fallback renderer defaults because class-driving planet fields are missing.",
    replacement_target: "reviewed planet class or atmospheric/rendering model",
  };
}

function makeCanvasTexture(draw, size = 128) {
  if (typeof document === "undefined") {
    return null;
  }
  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const context = canvas.getContext("2d");
  if (!context) {
    return null;
  }
  draw(context, size);
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  texture.wrapS = THREE.RepeatWrapping;
  texture.wrapT = THREE.ClampToEdgeWrapping;
  texture.anisotropy = 4;
  return texture;
}

function createStarTexture(seed, baseColor) {
  return makeCanvasTexture((context, size) => {
    context.fillStyle = baseColor;
    context.fillRect(0, 0, size, size);
    for (let idx = 0; idx < 260; idx += 1) {
      const x = hashUnit(seed, `sx-${idx}`) * size;
      const y = hashUnit(seed, `sy-${idx}`) * size;
      const radius = 0.6 + hashUnit(seed, `sr-${idx}`) * 2.4;
      const alpha = 0.05 + hashUnit(seed, `sa-${idx}`) * 0.13;
      context.fillStyle = idx % 3 === 0 ? `rgba(255,255,255,${alpha})` : `rgba(0,0,0,${alpha * 0.42})`;
      context.beginPath();
      context.arc(x, y, radius, 0, Math.PI * 2);
      context.fill();
    }
    const gradient = context.createRadialGradient(size * 0.38, size * 0.34, size * 0.05, size * 0.5, size * 0.5, size * 0.72);
    gradient.addColorStop(0, "rgba(255,255,255,0.36)");
    gradient.addColorStop(0.45, "rgba(255,255,255,0.06)");
    gradient.addColorStop(1, "rgba(0,0,0,0.34)");
    context.fillStyle = gradient;
    context.fillRect(0, 0, size, size);
  }, 128);
}

function createPlanetTexture(seed, kind) {
  const palette = PLANET_VISUAL_PALETTES[kind] || PLANET_VISUAL_PALETTES.temperate_rock;
  return makeCanvasTexture((context, size) => {
    const gradient = context.createLinearGradient(0, 0, size, size);
    gradient.addColorStop(0, palette[0]);
    gradient.addColorStop(0.55, palette[1]);
    gradient.addColorStop(1, palette[3]);
    context.fillStyle = gradient;
    context.fillRect(0, 0, size, size);

    if (kind === "gas_giant" || kind === "ice_giant") {
      for (let band = 0; band < 13; band += 1) {
        const y = (band / 13) * size + hashUnit(seed, `band-y-${band}`) * 8 - 4;
        const height = 5 + hashUnit(seed, `band-h-${band}`) * 12;
        const color = palette[band % palette.length];
        context.fillStyle = color;
        context.globalAlpha = 0.26 + hashUnit(seed, `band-a-${band}`) * 0.28;
        context.fillRect(0, y, size, height);
      }
      context.globalAlpha = 0.4;
      context.fillStyle = palette[2];
      context.beginPath();
      context.ellipse(size * (0.35 + hashUnit(seed, "storm-x") * 0.35), size * 0.58, size * 0.08, size * 0.035, -0.25, 0, Math.PI * 2);
      context.fill();
    } else {
      for (let idx = 0; idx < 84; idx += 1) {
        const x = hashUnit(seed, `px-${idx}`) * size;
        const y = hashUnit(seed, `py-${idx}`) * size;
        const radius = 2 + hashUnit(seed, `pr-${idx}`) * 12;
        context.fillStyle = palette[idx % palette.length];
        context.globalAlpha = 0.09 + hashUnit(seed, `pa-${idx}`) * 0.24;
        context.beginPath();
        context.ellipse(x, y, radius, radius * (0.45 + hashUnit(seed, `pe-${idx}`)), hashUnit(seed, `rot-${idx}`) * Math.PI, 0, Math.PI * 2);
        context.fill();
      }
    }
    context.globalAlpha = 1;
    const limb = context.createRadialGradient(size * 0.35, size * 0.3, size * 0.08, size * 0.5, size * 0.5, size * 0.72);
    limb.addColorStop(0, "rgba(255,255,255,0.28)");
    limb.addColorStop(0.55, "rgba(255,255,255,0.02)");
    limb.addColorStop(1, "rgba(0,0,0,0.48)");
    context.fillStyle = limb;
    context.fillRect(0, 0, size, size);
  }, 128);
}

function statusLabel(status) {
  return String(status || "missing").toUpperCase();
}

async function copyTextToClipboard(value) {
  const text = String(value || "");
  if (!text) {
    return false;
  }
  if (navigator?.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return true;
  }
  if (typeof document === "undefined") {
    return false;
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  textarea.style.top = "0";
  document.body.appendChild(textarea);
  textarea.select();
  try {
    return document.execCommand("copy");
  } finally {
    document.body.removeChild(textarea);
  }
}

function compactIdentifier(value, maxLength = 26) {
  const text = String(value || "");
  if (text.length <= maxLength) {
    return text;
  }
  const half = Math.max(8, Math.floor((maxLength - 3) / 2));
  return `${text.slice(0, half)}...${text.slice(-half)}`;
}

function formatFieldValue(field) {
  if (!field) {
    return "Unknown";
  }
  const value = Number(field.value);
  const display = Number.isFinite(value) ? formatNumber(value, Math.abs(value) >= 10 ? 1 : 3) : String(field.value ?? "Unknown");
  return field.unit ? `${display} ${field.unit}` : display;
}

function formatConfidence(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "";
  }
  if (numeric >= 0 && numeric <= 1) {
    return `${formatNumber(numeric * 100, numeric >= 0.995 ? 1 : 0)}%`;
  }
  return formatNumber(numeric, 2);
}

function fieldSummary(fields, key, fallback = "Unknown", digits = 2) {
  const field = fieldRecord(fields, key);
  if (!field || field.value === null || field.value === undefined || field.value === "") {
    return fallback;
  }
  const value = Number(field.value);
  const display = Number.isFinite(value) ? formatNumber(value, digits) : String(field.value);
  return field.unit ? `${display} ${field.unit}` : display;
}

function fieldStatusSummary(fields, key) {
  const field = fieldRecord(fields, key);
  return statusLabel(field?.status || "missing");
}

function readoutRow(fields, key, label, fallback = "Unknown", digits = 2) {
  const field = fieldRecord(fields, key);
  return [label, fieldSummary(fields, key, fallback, digits), fieldStatusSummary(fields, key), field];
}

function staticReadoutRow(label, value, status = "source", field = null) {
  return [label, value, statusLabel(status), field];
}

function strongestStatus(fields, keys = []) {
  const priority = { source: 4, derived: 3, assumed: 2, missing: 1 };
  return keys
    .map((key) => fieldRecord(fields, key)?.status || "missing")
    .reduce((best, status) => (priority[status] > priority[best] ? status : best), "missing");
}

function firstFieldWithStatus(fields, keys = [], wantedStatus = "source") {
  return keys.map((key) => fieldRecord(fields, key)).find((field) => field?.status === wantedStatus) || null;
}

function visualStellarClassValue(body) {
  return fieldRecord(body?.fields, "visual_stellar_class")?.value || body?.spectral_class || "";
}

function starClassProvenanceField(body) {
  const classValue = body?.spectral_class || "";
  const spectralTypeField = fieldRecord(body?.fields, "spectral_type_raw");
  const visualClassField = fieldRecord(body?.fields, "visual_stellar_class");
  const teffField = fieldRecord(body?.fields, "teff_k");
  const objectTypeField = fieldRecord(body?.fields, "object_type");
  if (spectralTypeField?.value) {
    return {
      key: "spectral_class",
      label: "Spectral class",
      value: classValue || String(spectralTypeField.value).slice(0, 1).toUpperCase(),
      unit: null,
      status: spectralTypeField.status || "source",
      layer: spectralTypeField.layer,
      source_catalog: spectralTypeField.source_catalog,
      source_reference: spectralTypeField.source_reference,
      basis: `${spectralTypeField.basis || "spectral_type_raw"}:class_extract`,
      confidence: spectralTypeField.confidence,
      notes: "Display class extracted from the component-specific spectral type field.",
    };
  }
  if (visualClassField?.value) {
    return {
      ...visualClassField,
      label: visualClassField.status === "assumed" ? "Visual prior" : "Visual class",
      notes: visualClassField.notes || "Presentation-only renderer visual class; not a source spectral class.",
    };
  }
  if (classValue && teffField?.value !== null && teffField?.value !== undefined && teffField?.value !== "") {
    return {
      key: "spectral_class",
      label: "Spectral class",
      value: classValue,
      unit: null,
      status: "derived",
      layer: "render_scene",
      source_catalog: teffField.source_catalog,
      source_reference: teffField.source_reference,
      basis: `${teffField.basis || "teff_k"}:visual_class_proxy`,
      confidence: Math.min(Number(teffField.confidence || 0.45), 0.55),
      notes: "Renderer display class inferred from available temperature or visual proxy fields; not component-specific source spectral evidence.",
    };
  }
  if (classValue && objectTypeField?.value && String(objectTypeField.value) !== "star") {
    return {
      key: "spectral_class",
      label: "Spectral class",
      value: classValue,
      unit: null,
      status: "derived",
      layer: "render_scene",
      source_catalog: objectTypeField.source_catalog,
      source_reference: objectTypeField.source_reference,
      basis: `${objectTypeField.basis || "object_type"}:compact_visual_class`,
      confidence: Math.min(Number(objectTypeField.confidence || 0.45), 0.65),
      notes: "Renderer display class derived from source-backed compact-object classification.",
    };
  }
  return {
    key: "spectral_class",
    label: "Spectral class",
    value: classValue || null,
    unit: null,
    status: classValue ? "assumed" : "missing",
    layer: classValue ? "render_scene" : "none",
    basis: classValue ? "visual_class_without_component_spectral_evidence" : "no_component_specific_spectral_class",
    confidence: classValue ? 0.2 : null,
    notes: classValue
      ? "Renderer has a display class but no component-specific spectral source field; treat as a visual prior."
      : "No component-specific spectral class is available for this rendered star.",
  };
}

function payloadId(payload) {
  return String(payload?.id || "");
}

function objectHoverPayload(kind, body) {
  if (!body) {
    return null;
  }
  if (kind === "subsystem") {
    return {
      kind: "Subsystem",
      name: body.display_name || body.name || "Unnamed subsystem",
      id: body.render_key || body.source?.stable_component_key || body.key || "",
      sourceLayer: body.source?.layer || "unknown",
      rows: [
        readoutRow(body.fields, "component_label", "Component", body.component || "Group", 0),
        readoutRow(body.fields, "rendered_child_star_count", "Stars", "Unknown", 0),
        readoutRow(body.fields, "hierarchy_basis", "Basis", body.source?.basis || "hierarchy", 0),
      ],
    };
  }
  if (kind === "star") {
    const bodyClass = stellarBodyClass(body);
    const classField = starClassProvenanceField(body);
    return {
      kind: bodyClassLabel(bodyClass),
      name: body.display_name || body.name || "Unnamed star",
      id: body.render_key || body.stable_object_key || body.source?.stable_component_key || body.source?.stable_object_key || body.key || "",
      sourceLayer: body.source?.layer || "unknown",
      rows: [
        readoutRow(body.fields, "object_type", "Type", bodyClassLabel(bodyClass), 0),
        staticReadoutRow("Class", classField.value || "Unknown", classField.status, classField),
        readoutRow(body.fields, "teff_k", "Temp", "Unknown", 0),
        readoutRow(body.fields, "mass_msun", "Mass", "Unknown", 3),
        readoutRow(body.fields, "radius_rsun", "Radius", "Unknown", 3),
      ],
    };
  }
  const visualClassField = planetVisualKindField(body);
  return {
    kind: "Planet",
    name: body.display_name || body.name || "Unnamed planet",
    id: body.render_key || body.stable_object_key || body.source?.stable_component_key || body.source?.stable_object_key || body.key || "",
    sourceLayer: body.source?.layer || "unknown",
    rows: [
      staticReadoutRow("Class", String(visualClassField.value), visualClassField.status, visualClassField),
      readoutRow(body.fields, "orbital_period_days", "Period", "Unknown", 3),
      readoutRow(body.fields, "semi_major_axis_au", "Orbit", "Unknown", 4),
      readoutRow(body.fields, "eccentricity", "Ecc.", "Unknown", 3),
      readoutRow(body.fields, "radius_earth", "Radius", "Unknown", 2),
    ],
  };
}

function orbitGuideProvenanceField(orbit) {
  const sourceLayer = orbit?.source?.layer || "arm";
  const guideStatus = sourceLayer === "disc_assumption"
    ? "assumed"
    : (strongestStatus(orbit?.fields, ["semi_major_axis_au", "eccentricity", "inclination_deg", "period_days"]) === "missing" ? "missing" : "derived");
  const sourceField = firstFieldWithStatus(orbit?.fields, ["semi_major_axis_au", "eccentricity", "inclination_deg", "period_days"], "source");
  return {
    key: "orbit_guide_trace",
    label: "Orbit guide",
    value: guideStatus === "assumed" ? "Visual prior trace" : "Rendered guide trace",
    unit: null,
    status: guideStatus,
    layer: guideStatus === "assumed" ? "disc_assumption" : (guideStatus === "missing" ? "none" : "render_scene"),
    source_catalog: sourceField?.source_catalog || orbit?.source?.source_catalog,
    source_reference: sourceField?.source_reference,
    basis: guideStatus === "assumed"
      ? "render_scene:disc_visual_orbit_trace"
      : (guideStatus === "missing" ? "render_scene:no_orbit_trace_fields" : "render_scene:orbit_trace_from_payload_fields"),
    seed: guideStatus === "assumed" ? fieldRecord(orbit?.fields, "phase_rad")?.seed : null,
    generator_version: "system_preview_orbit_trace_v1",
    confidence: guideStatus === "assumed" ? 0.2 : (guideStatus === "derived" ? 0.65 : null),
    notes: "Displayed path samples the same eccentricity, inclination, and visual scale used by the animated bodies; it is clarity-scaled, not a physical-size rendering.",
    replacement_target: "source orbit solution with epoch, orientation, uncertainty, and reviewed display transform",
  };
}

function planetOrbitGuideProvenanceField(planet) {
  const guideStatus = strongestStatus(planet?.fields, ["semi_major_axis_au", "eccentricity", "inclination_deg"]);
  const sourceField = firstFieldWithStatus(planet?.fields, ["semi_major_axis_au", "eccentricity", "inclination_deg"], "source");
  const fallbackField = firstFieldWithStatus(planet?.fields, ["semi_major_axis_au", "eccentricity", "inclination_deg"], "derived");
  const assumedField = firstFieldWithStatus(planet?.fields, ["semi_major_axis_au", "eccentricity", "inclination_deg", "phase_rad"], "assumed");
  const status = guideStatus === "source" || guideStatus === "derived" ? "derived" : (guideStatus === "assumed" ? "assumed" : "missing");
  return {
    key: "planet_orbit_trace",
    label: "Orbit trace",
    value: status === "assumed" ? "Visual prior trace" : "Rendered orbit trace",
    unit: null,
    status,
    layer: status === "assumed" ? "disc_assumption" : (status === "missing" ? "none" : "render_scene"),
    source_catalog: sourceField?.source_catalog || fallbackField?.source_catalog,
    source_reference: sourceField?.source_reference || fallbackField?.source_reference,
    basis: status === "assumed"
      ? "render_scene:planet_orbit_trace_from_disc_priors"
      : (status === "missing" ? "render_scene:no_planet_orbit_trace_fields" : "render_scene:planet_orbit_trace_from_payload_fields"),
    seed: status === "assumed" ? assumedField?.seed : null,
    generator_version: "system_preview_orbit_trace_v1",
    confidence: status === "derived" ? 0.7 : (status === "assumed" ? 0.25 : null),
    notes: "Displayed path is generated from the same eccentricity, inclination, and clarity-scaled orbit radius as the animated planet.",
    replacement_target: "source planet orbit solution with epoch, orientation, uncertainty, and reviewed display transform",
  };
}

function orbitHoverPayload(orbit) {
  if (!orbit) {
    return null;
  }
  const guideField = orbitGuideProvenanceField(orbit);
  return {
    kind: String(orbit.relation_kind || "Orbit"),
    name: orbit.display_name || orbit.orbit_key || "Orbit",
    id: orbit.orbit_key || String(orbit.orbit_edge_id || ""),
    sourceLayer: orbit.source?.layer || "arm",
    rows: [
      readoutRow(orbit.fields, "period_days", "Period", "Unknown", 3),
      readoutRow(orbit.fields, "semi_major_axis_au", "Axis", "Visual", 4),
      readoutRow(orbit.fields, "eccentricity", "Ecc.", "Unknown", 3),
      readoutRow(orbit.fields, "inclination_deg", "Incl.", "Unknown", 2),
      staticReadoutRow("Guide", String(guideField.value), guideField.status, guideField),
    ],
  };
}

function normalizeRadians(angle) {
  const twoPi = Math.PI * 2;
  return ((angle % twoPi) + twoPi) % twoPi;
}

function trueAnomalyFromMeanAnomaly(meanAnomaly, eccentricity = 0) {
  const e = Math.min(0.95, Math.max(0, Number(eccentricity) || 0));
  const mean = normalizeRadians(meanAnomaly);
  if (e < 1e-6) {
    return mean;
  }
  let eccentricAnomaly = e < 0.8 ? mean : Math.PI;
  for (let idx = 0; idx < 8; idx += 1) {
    const delta = (eccentricAnomaly - e * Math.sin(eccentricAnomaly) - mean) / (1 - e * Math.cos(eccentricAnomaly));
    eccentricAnomaly -= delta;
    if (Math.abs(delta) < 1e-7) {
      break;
    }
  }
  const sinHalf = Math.sqrt(1 + e) * Math.sin(eccentricAnomaly / 2);
  const cosHalf = Math.sqrt(1 - e) * Math.cos(eccentricAnomaly / 2);
  return normalizeRadians(2 * Math.atan2(sinHalf, cosHalf));
}

function orbitalPosition(trueAnomaly, orbitRadius, eccentricity = 0, inclinationRad = 0) {
  const radiusScale = (1 - eccentricity ** 2) / (1 + eccentricity * Math.cos(trueAnomaly));
  const x = Math.cos(trueAnomaly) * orbitRadius * radiusScale;
  const planeZ = Math.sin(trueAnomaly) * orbitRadius * radiusScale;
  return [x, -planeZ * Math.sin(inclinationRad), planeZ * Math.cos(inclinationRad)];
}

function orbitalPositionFromMeanAnomaly(meanAnomaly, orbitRadius, eccentricity = 0, inclinationRad = 0) {
  return orbitalPosition(trueAnomalyFromMeanAnomaly(meanAnomaly, eccentricity), orbitRadius, eccentricity, inclinationRad);
}

function sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, samples = 192) {
  const vertices = [];
  for (let idx = 0; idx < samples; idx += 1) {
    const phase = (idx / samples) * Math.PI * 2;
    vertices.push(...orbitalPosition(phase, orbitRadius, eccentricity, inclinationRad));
  }
  return new Float32Array(vertices);
}

function sourcePlanetEccentricity(planet) {
  return Math.min(0.85, Math.max(0, numericField(planet?.fields, "eccentricity") || Number(planet?.eccentricity) || 0));
}

function displayPlanetEccentricity(planet) {
  const value = Number(planet?.display_eccentricity_scene);
  return Number.isFinite(value) ? Math.min(0.85, Math.max(0, value)) : sourcePlanetEccentricity(planet);
}

function planetDisplayEccentricityField(planet) {
  if (!planet?.eccentricity_display_capped) {
    return null;
  }
  return {
    key: "display_eccentricity_scene",
    label: "Display eccentricity",
    value: Number(planet.display_eccentricity_scene || 0),
    unit: "dimensionless",
    status: "derived",
    layer: "render_scene",
    source_catalog: "spacegate_renderer",
    basis: "presentation_orbit_spacing_cap",
    confidence: "high",
    notes: "Source eccentricity remains in the Ecc. field; the visible orbit path is capped so compressed presentation-scale paths do not cross neighboring rendered orbits.",
  };
}

function habitableZoneBoundsAu(star) {
  const luminosity = numericField(star?.fields, "luminosity_lsun");
  if (!Number.isFinite(luminosity) || luminosity <= 0) {
    return null;
  }
  return {
    luminosity,
    innerAu: Math.sqrt(luminosity / 1.7),
    outerAu: Math.sqrt(luminosity / 0.35),
  };
}

function habitableZoneGuideField(star, bounds) {
  const luminosityField = fieldRecord(star?.fields, "luminosity_lsun");
  return {
    key: "habitable_zone_broad_flux_guide",
    label: "Habitable zone",
    value: "Broad flux guide",
    unit: null,
    status: luminosityField?.status === "missing" ? "missing" : "derived",
    layer: "render_scene",
    source_catalog: luminosityField?.source_catalog || "spacegate_renderer",
    source_reference: luminosityField?.source_reference,
    basis: "sqrt(luminosity_lsun / stellar_flux_bounds_earth)",
    confidence: luminosityField?.confidence ?? null,
    notes: `Presentation guide from broad insolation bounds 0.35-1.70 Earth flux; inner=${formatNumber(bounds?.innerAu, 3)} AU outer=${formatNumber(bounds?.outerAu, 3)} AU. This is not a climate model or canonical habitability claim.`,
  };
}

function habitableZoneHoverPayload(star, bounds) {
  const guideField = habitableZoneGuideField(star, bounds);
  const planeDeg = Number(star.habitable_zone_plane_inclination_deg);
  const planeField = Number.isFinite(planeDeg) ? {
    key: "habitable_zone_plane_inclination_deg",
    label: "HZ plane inclination",
    value: planeDeg,
    unit: "deg",
    status: "derived",
    layer: "render_scene",
    source_catalog: "spacegate_renderer",
    basis: "median_host_planet_render_inclination",
    confidence: 0.7,
    notes: "Presentation alignment from rendered host-planet orbit inclinations; not a source orbital element for the star.",
  } : null;
  return {
    kind: "Habitable zone",
    name: `${star.display_name || star.name || "Star"} broad HZ`,
    id: `${star.render_key || star.key || "star"}:habitable-zone`,
    sourceLayer: "render_scene",
    rows: [
      readoutRow(star.fields, "luminosity_lsun", "Luminosity", "Unknown", 3),
      staticReadoutRow("Inner edge", `${formatNumber(bounds.innerAu, 3)} AU`, "derived", guideField),
      staticReadoutRow("Outer edge", `${formatNumber(bounds.outerAu, 3)} AU`, "derived", guideField),
      ...(planeField ? [staticReadoutRow("Plane", `${formatNumber(planeDeg, 2)} deg`, "derived", planeField)] : []),
      staticReadoutRow("Basis", "0.35-1.70 Earth flux", "derived", guideField),
    ],
  };
}

function medianNumber(values) {
  const clean = values.map(Number).filter(Number.isFinite).sort((left, right) => left - right);
  if (!clean.length) {
    return null;
  }
  const mid = Math.floor(clean.length / 2);
  return clean.length % 2 ? clean[mid] : (clean[mid - 1] + clean[mid]) / 2;
}

function planetMatchesHostStar(planet, star, layout) {
  const starKey = star.render_key || star.key;
  const hostKey = layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key;
  if (hostKey && starKey && hostKey === starKey) {
    return true;
  }
  const hostStarId = Number(planet.host_star_id);
  const sourceStarId = Number(star?.source?.star_id);
  return Number.isFinite(hostStarId) && Number.isFinite(sourceStarId) && hostStarId === sourceStarId;
}

function applyHabitableZonePlaneAlignment(stars, planetPlacements, layout) {
  return stars.map((star) => {
    const hostPlanets = planetPlacements
      .map(({ planet }) => planet)
      .filter((planet) => planetMatchesHostStar(planet, star, layout));
    const medianInclinationDeg = medianNumber(hostPlanets.map((planet) => numericField(planet.fields, "inclination_deg")));
    const planeInclinationDeg = Number.isFinite(Number(medianInclinationDeg)) ? Number(medianInclinationDeg) : 0;
    return {
      ...star,
      habitable_zone_plane_inclination_deg: planeInclinationDeg,
      habitable_zone_plane_basis: hostPlanets.length ? "median_host_planet_render_inclination" : "default_scene_plane",
    };
  });
}

function scaledOrbitPoints(points, scale) {
  return new Float32Array(Array.from(points, (value) => value * scale));
}

function binaryMassFractions(primary, secondary) {
  return barycentricMassFractions(
    positiveStellarMass(primary),
    positiveStellarMass(secondary),
  );
}

function positiveStellarMass(body) {
  const field = fieldRecord(body?.fields, "mass_msun");
  const mass = numericField(body?.fields, "mass_msun");
  if (!Number.isFinite(mass) || mass <= 0) {
    return null;
  }
  return {
    mass,
    status: field?.status || "missing",
  };
}

function massForBodyKeys(keys, starsByKey) {
  const uniqueKeys = [...new Set((keys || []).filter(Boolean))];
  const records = uniqueKeys
    .map((key) => positiveStellarMass(starsByKey.get(key)))
    .filter((record) => Number.isFinite(record?.mass) && record.mass > 0);
  if (!records.length) {
    return null;
  }
  return {
    mass: records.reduce((sum, record) => sum + record.mass, 0),
    status: weakestMassStatus(records.map((record) => record.status)),
  };
}

function weakestMassStatus(statuses = []) {
  if (statuses.some((status) => status === "assumed")) {
    return "assumed";
  }
  if (statuses.some((status) => status === "missing")) {
    return "missing";
  }
  if (statuses.some((status) => status === "derived")) {
    return "derived";
  }
  return statuses.length ? "source" : "missing";
}

function massRatioBasis(primaryMassRecord, secondaryMassRecord) {
  const status = weakestMassStatus([primaryMassRecord?.status, secondaryMassRecord?.status].filter(Boolean));
  if (status === "assumed") {
    return "assumed_mass_ratio";
  }
  if (status === "derived") {
    return "derived_mass_ratio";
  }
  return "source_mass_ratio";
}

function barycentricMassFractions(primaryMassRecord, secondaryMassRecord) {
  const primaryMass = primaryMassRecord?.mass;
  const secondaryMass = secondaryMassRecord?.mass;
  if (primaryMass && secondaryMass && primaryMass > 0 && secondaryMass > 0) {
    const total = primaryMass + secondaryMass;
    return {
      primary: secondaryMass / total,
      secondary: primaryMass / total,
      basis: massRatioBasis(primaryMassRecord, secondaryMassRecord),
      primaryMass,
      secondaryMass,
    };
  }
  return {
    primary: 0.5,
    secondary: 0.5,
    basis: "equal_mass_visual_fallback",
    primaryMass: primaryMass || null,
    secondaryMass: secondaryMass || null,
  };
}

function binaryTraceProvenanceField(massFractions) {
  const massWeighted = ["source_mass_ratio", "derived_mass_ratio", "assumed_mass_ratio"].includes(massFractions?.basis);
  const sourceBacked = massFractions?.basis === "source_mass_ratio" || massFractions?.basis === "derived_mass_ratio";
  return {
    key: "binary_body_paths",
    label: "Body paths",
    value: massWeighted ? "Mass-weighted barycentric" : "Equal-mass visual fallback",
    unit: null,
    status: sourceBacked ? "derived" : "assumed",
    layer: "render_scene",
    source_catalog: sourceBacked ? "stellar_mass_fields" : null,
    source_reference: null,
    basis: massFractions?.basis || "unknown",
    seed: null,
    generator_version: "system_preview_binary_trace_v1",
    confidence: sourceBacked ? 0.85 : (massWeighted ? 0.45 : 0.35),
    notes: sourceBacked
      ? "Rendered body paths use available stellar masses to split the visual relative orbit around the barycenter."
      : (massWeighted
        ? "Rendered body paths use deterministic assumed stellar masses because one or more source masses are missing."
        : "Rendered body paths assume equal visual masses because one or both stellar masses are missing."),
  };
}

function advanceSimulationDays(ref, elapsedSeconds, running, speedMultiplier = 1) {
  if (ref.lastElapsedSeconds === null || ref.lastElapsedSeconds === undefined) {
    ref.lastElapsedSeconds = elapsedSeconds;
  }
  const delta = Math.max(0, elapsedSeconds - ref.lastElapsedSeconds);
  ref.lastElapsedSeconds = elapsedSeconds;
  if (running) {
    ref.days += delta * SIM_DAYS_PER_SECOND * speedMultiplier;
  }
  return ref.days;
}

function currentSimulationDays(ref) {
  const days = Number(ref?.current?.days);
  return Number.isFinite(days) ? days : 0;
}

function EvidencePill({ field, fallbackStatus = "missing" }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const status = field?.status || fallbackStatus;
  const confidenceText = formatConfidence(field?.confidence);
  const copyPayload = useCallback(() => {
    if (!field) {
      return;
    }
    copyTextToClipboard(JSON.stringify(field, null, 2)).then((ok) => {
      if (!ok) {
        return;
      }
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    }).catch(() => {});
  }, [field]);

  return (
    <span
      className={`evidence-pill ${status}`}
      tabIndex={0}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
      onClick={copyPayload}
      role="button"
      aria-label={`${statusLabel(status)} provenance${field?.label ? ` for ${field.label}` : ""}`}
    >
      {copied ? "COPIED" : statusLabel(status)}
      {open && (
        <span className="evidence-popover" role="tooltip">
          <strong>{field?.label || "Field provenance"}</strong>
          <span>{formatFieldValue(field)}</span>
          <span>Layer: {field?.layer || "unknown"}</span>
          <span>Basis: {field?.basis || "not specified"}</span>
          {field?.source_catalog && <span>Source: {field.source_catalog}</span>}
          {field?.source_reference && <span>Reference: {field.source_reference}</span>}
          {confidenceText && <span>Confidence: {confidenceText}</span>}
          {field?.notes && <span>Notes: {field.notes}</span>}
          {field?.seed && <span>Seed: {field.seed}</span>}
          {field?.generator_version && <span>Generator: {field.generator_version}</span>}
          {field?.replacement_target && <span>Replace with: {field.replacement_target}</span>}
        </span>
      )}
    </span>
  );
}

function SceneLabel({ text, position = [0, -0.4, 0], color = "#e6f6ff", scale = 1, visible = true }) {
  const groupRef = React.useRef(null);
  const textRef = React.useRef(null);
  const worldPositionRef = React.useRef(new THREE.Vector3());
  const { camera, size } = useThree();
  const label = compactIdentifier(text, 24);

  useFrame(() => {
    if (!groupRef.current || !textRef.current || !visible) {
      return;
    }
    const worldPosition = worldPositionRef.current;
    groupRef.current.getWorldPosition(worldPosition);
    const distance = Math.max(0.001, camera.position.distanceTo(worldPosition));
    const fovRad = THREE.MathUtils.degToRad(camera.fov || 43);
    const worldUnitsPerPixel = (2 * Math.tan(fovRad / 2) * distance) / Math.max(1, size.height);
    const targetPixels = clampNumber(15 * scale, 11, 21);
    const fontSize = clampNumber(worldUnitsPerPixel * targetPixels, 0.045, 0.34);
    const fade = clampNumber((34 - distance) / 12, 0.42, 0.96);
    textRef.current.fontSize = fontSize;
    textRef.current.fillOpacity = fade;
    textRef.current.outlineOpacity = Math.min(0.96, fade + 0.18);
    groupRef.current.quaternion.copy(camera.quaternion);
  });

  if (!visible || !label) {
    return null;
  }

  return (
    <group ref={groupRef} position={position} renderOrder={30}>
      <Text
        ref={textRef}
        color={color}
        fontSize={0.12}
        maxWidth={2.8}
        textAlign="center"
        anchorX="center"
        anchorY="middle"
        outlineColor="#02080e"
        outlineWidth={0.012}
        outlineOpacity={0.92}
        fillOpacity={0.94}
        depthOffset={-20}
        material-depthTest={false}
        material-depthWrite={false}
        material-transparent
        raycast={() => {}}
      >
        {label}
      </Text>
    </group>
  );
}

function SelectionHalo({ radius, color = "#ffffff", pulse = false }) {
  const ref = React.useRef(null);

  useFrame(({ clock }) => {
    if (!pulse || !ref.current) {
      return;
    }
    const scale = 1 + Math.sin(clock.elapsedTime * 4.2) * 0.045;
    ref.current.scale.setScalar(scale);
  });

  return (
    <mesh ref={ref}>
      <sphereGeometry args={[radius, 32, 18]} />
      <meshBasicMaterial color={color} transparent opacity={0.16} depthWrite={false} blending={THREE.AdditiveBlending} />
    </mesh>
  );
}

function StarSphere({ star, position = [0, 0, 0], showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const bodyClass = stellarBodyClass(star);
  const compactRadiusFallback = bodyClass === "white_dwarf" ? 0.018 : (bodyClass === "neutron_star" || bodyClass === "pulsar" || bodyClass === "magnetar" ? 0.00003 : 0.55);
  const radiusRsun = numericField(star.fields, "radius_rsun") || Number(star.radiusRsun || compactRadiusFallback);
  const radius = Number(star.display_radius_scene) || scaledStarRadius(radiusRsun, star.visualScale, star.visual_scale_mode);
  const haloRadius = Number(star.display_halo_radius_scene) || Math.max(radius * (bodyClass === "white_dwarf" ? 2.35 : 1.75), radius + 0.18);
  const pickRadius = Number(star.pick_radius_scene) || Math.max(radius * 1.8, 0.34);
  const teffK = numericField(star.fields, "teff_k") || Number(star.teffK || 0);
  const visualClass = String(visualStellarClassValue(star) || "").slice(0, 1).toUpperCase();
  const color = bodyClass === "white_dwarf"
    ? "#dceaff"
    : (teffK && fieldRecord(star.fields, "teff_k")?.status !== "assumed" ? starColor(teffK) : (STAR_COLORS[visualClass] || "#ff9d6b"));
  const texture = useMemo(() => createStarTexture(star.render_key || star.key || star.display_name || star.name, color), [star, color]);
  useEffect(() => () => texture?.dispose?.(), [texture]);
  const hoverPayload = useMemo(() => objectHoverPayload("star", star), [star]);
  const selected = Boolean(selectedObjectId && payloadId(hoverPayload) === selectedObjectId);
  const hoverHandlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(hoverPayload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(hoverPayload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(hoverPayload);
    },
  };
  return (
    <group position={position}>
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[radius, 32, 24]} />
        <meshStandardMaterial color={color} map={texture || null} emissive={color} emissiveIntensity={bodyClass === "white_dwarf" ? 1.45 : 0.9} roughness={0.52} />
      </mesh>
      <mesh>
        <sphereGeometry args={[haloRadius, 32, 20]} />
        <meshBasicMaterial color={color} transparent opacity={selected ? 0.24 : (bodyClass === "white_dwarf" ? 0.22 : 0.16)} depthWrite={false} blending={THREE.AdditiveBlending} />
      </mesh>
      {selected && <SelectionHalo radius={Math.max(radius * 1.82, radius + 0.28)} color="#fff2b7" pulse />}
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[pickRadius, 16, 12]} />
        <meshBasicMaterial transparent opacity={0} depthWrite={false} />
      </mesh>
      <SceneLabel
        text={star.display_name || star.name || "Star"}
        position={[0, -Math.max(radius + 0.28, pickRadius * 0.72), 0]}
        color="#fff4c4"
        scale={bodyClass === "white_dwarf" ? 0.78 : 0.92}
        visible={showLabels}
      />
    </group>
  );
}

function hierarchyRenderKey(node) {
  const key = String(node?.stable_component_key || "");
  if (key.startsWith("canon:leaf:msc:")) {
    return `comp:msc:wds:${key.slice("canon:leaf:msc:".length)}`;
  }
  if (key.startsWith("canon:star:")) {
    return `comp:star:${key}`;
  }
  if (key.startsWith("canon:planet:")) {
    return `comp:planet:${key}`;
  }
  return key;
}

function keyAliasesForBody(body) {
  const aliases = new Set();
  const add = (value) => {
    const text = String(value || "").trim();
    if (text) {
      aliases.add(text);
    }
  };
  add(body?.render_key);
  add(body?.key);
  add(body?.stable_object_key);
  add(body?.source?.stable_component_key);
  add(body?.source?.stable_object_key);
  add(body?.source?.canonical_key);
  if (body?.source?.canonical_key) {
    add(`comp:star:${body.source.canonical_key}`);
    add(`comp:planet:${body.source.canonical_key}`);
  }
  if (body?.stable_object_key) {
    add(`comp:star:${body.stable_object_key}`);
    add(`comp:planet:${body.stable_object_key}`);
  }
  return aliases;
}

function addVector(a, b) {
  return [a[0] + b[0], a[1] + b[1], a[2] + b[2]];
}

function scaledVector(a, scale) {
  return [a[0] * scale, a[1] * scale, a[2] * scale];
}

function averageVector(vectors) {
  const clean = vectors.filter(Boolean);
  if (!clean.length) {
    return [0, 0, 0];
  }
  return scaledVector(clean.reduce((sum, position) => addVector(sum, position), [0, 0, 0]), 1 / clean.length);
}

function sumVectors(vectors) {
  return (vectors || []).filter(Boolean).reduce((sum, position) => addVector(sum, position), [0, 0, 0]);
}

function distanceBetween(a, b) {
  if (!a || !b) {
    return null;
  }
  const dx = a[0] - b[0];
  const dy = a[1] - b[1];
  const dz = a[2] - b[2];
  const distance = Math.sqrt(dx * dx + dy * dy + dz * dz);
  return Number.isFinite(distance) ? distance : null;
}

function sameKeySet(left, right) {
  if (!left || !right || left.size !== right.size) {
    return false;
  }
  for (const key of left) {
    if (!right.has(key)) {
      return false;
    }
  }
  return true;
}

function recordNearestStarPair(nearestByKey, pairs, leftKey, rightKey, separation, source) {
  const cleanSeparation = Number(separation);
  if (!leftKey || !rightKey || leftKey === rightKey || !Number.isFinite(cleanSeparation) || cleanSeparation <= 0) {
    return;
  }
  const update = (key) => {
    const previous = nearestByKey.get(key);
    if (!previous || cleanSeparation < previous.separation) {
      nearestByKey.set(key, { separation: cleanSeparation, partnerKey: key === leftKey ? rightKey : leftKey, source });
    }
  };
  update(leftKey);
  update(rightKey);
  pairs.push({ leftKey, rightKey, separation: cleanSeparation, source });
}

function computeStarSeparationDiagnostics(stars, layout, binaryOrbits, visualScale, scaleMode) {
  const nearestByKey = new Map();
  const pairs = [];
  const keys = stars.map((star) => star.render_key || star.key).filter(Boolean);
  keys.forEach((leftKey, leftIdx) => {
    const leftPosition = layout.starPositions.get(leftKey);
    keys.slice(leftIdx + 1).forEach((rightKey) => {
      const rightPosition = layout.starPositions.get(rightKey);
      const separation = distanceBetween(leftPosition, rightPosition);
      recordNearestStarPair(nearestByKey, pairs, leftKey, rightKey, separation, "layout");
    });
  });
  (binaryOrbits || []).forEach((orbit) => {
    const primaryKey = layout.canonicalKeyByAlias.get(orbit.primary_body_key) || orbit.primary_body_key;
    const secondaryKey = layout.canonicalKeyByAlias.get(orbit.secondary_body_key) || orbit.secondary_body_key;
    const orbitRadius = scaledBinaryOrbitRadius(orbit, visualScale, scaleMode, 0.9);
    const eccentricity = Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0));
    recordNearestStarPair(nearestByKey, pairs, primaryKey, secondaryKey, orbitRadius * (1 - eccentricity), "binary_periapsis");
  });
  const separations = pairs.map((pair) => pair.separation).filter((value) => Number.isFinite(value) && value > 0);
  return {
    nearestByKey,
    pairs,
    minSeparation: separations.length ? Math.min(...separations) : null,
  };
}

function applyCollisionSafeStarRadii(stars, separationDiagnostics, visualScale, scaleMode) {
  const mode = normalizeScaleMode(scaleMode);
  const policy = visualScale.collision_policy || DEFAULT_VISUAL_SCALE.collision_policy;
  const shouldCap = mode === "structure" || mode === "log";
  const capFraction = Number(policy.star_radius_fraction_of_nearest_sep || 0.28);
  const minVisible = Number(policy.min_visible_star_radius_scene || 0.045);
  const minHalo = Number(policy.min_halo_radius_scene || 0.16);
  const minPick = Number(policy.min_pick_radius_scene || 0.28);
  const radiusByKey = new Map();
  let adjustedCount = 0;
  const displayStars = stars.map((star) => {
    const key = star.render_key || star.key;
    const baseRadius = Number(star.display_radius_scene) || scaledStarRadius(numericField(star.fields, "radius_rsun"), visualScale, mode);
    const nearest = separationDiagnostics.nearestByKey.get(key);
    const capRadius = shouldCap && nearest?.separation
      ? Math.max(minVisible, nearest.separation * capFraction)
      : baseRadius;
    const displayRadius = Math.min(baseRadius, capRadius);
    const adjusted = displayRadius < baseRadius - 0.0001;
    if (adjusted) {
      adjustedCount += 1;
    }
    const bodyClass = stellarBodyClass(star);
    const haloFactor = bodyClass === "white_dwarf" ? 2.35 : 1.75;
    const haloRadius = Math.max(displayRadius * haloFactor, displayRadius + minHalo);
    const pickRadius = Math.max(displayRadius * 1.85, minPick);
    radiusByKey.set(key, displayRadius);
    return {
      ...star,
      base_radius_scene: baseRadius,
      display_radius_scene: displayRadius,
      display_halo_radius_scene: haloRadius,
      pick_radius_scene: pickRadius,
      collision_adjusted: adjusted,
      nearest_star_separation_scene: nearest?.separation || null,
      visual_scale_mode: mode,
    };
  });
  const clearances = (separationDiagnostics.pairs || [])
    .map((pair) => {
      const leftRadius = radiusByKey.get(pair.leftKey);
      const rightRadius = radiusByKey.get(pair.rightKey);
      return Number.isFinite(leftRadius) && Number.isFinite(rightRadius)
        ? pair.separation - leftRadius - rightRadius
        : null;
    })
    .filter((value) => Number.isFinite(value));
  return {
    stars: displayStars,
    adjustedCount,
    minClearance: clearances.length ? Math.min(...clearances) : null,
    minSeparation: separationDiagnostics.minSeparation,
  };
}

function applyPlanetDisplayOrbitGeometry(planets, maxOrbit, visualScale, scaleMode) {
  const rows = (planets || []).map((planet, idx) => ({
    planet,
    idx,
    key: planet.render_key || planet.key || `planet-${idx}`,
    orbitRadius: scaledPlanetOrbitRadius(planet.orbitAu, maxOrbit, visualScale, scaleMode),
    sourceEccentricity: sourcePlanetEccentricity(planet),
  })).sort((left, right) => left.orbitRadius - right.orbitRadius);

  const byKey = new Map();
  rows.forEach((row, idx) => {
    const gaps = [];
    if (idx > 0) {
      gaps.push(row.orbitRadius - rows[idx - 1].orbitRadius);
    }
    if (idx < rows.length - 1) {
      gaps.push(rows[idx + 1].orbitRadius - row.orbitRadius);
    }
    const nearestGap = Math.min(...gaps.filter((gap) => Number.isFinite(gap) && gap > 0));
    const spacingCap = Number.isFinite(nearestGap)
      ? Math.max(0.006, (nearestGap * 0.42) / Math.max(row.orbitRadius, 0.001))
      : 0.85;
    const displayEccentricity = Math.min(row.sourceEccentricity, spacingCap, 0.85);
    byKey.set(row.key, {
      ...row.planet,
      orbit_radius_scene: row.orbitRadius,
      source_eccentricity: row.sourceEccentricity,
      display_eccentricity_scene: displayEccentricity,
      eccentricity_display_capped: displayEccentricity < row.sourceEccentricity - 0.001,
    });
  });

  return (planets || []).map((planet, idx) => (
    byKey.get(planet.render_key || planet.key || `planet-${idx}`) || planet
  ));
}

function trueOrbitScaleDiagnostics(planets, scaleMode) {
  if (normalizeScaleMode(scaleMode) !== "true_orbits") {
    return { count: 0, maxRelativeError: null };
  }
  const ratios = (planets || [])
    .map((planet) => {
      const sourceAu = Number(planet.orbitAu);
      const sceneRadius = Number(planet.orbit_radius_scene);
      return Number.isFinite(sourceAu) && sourceAu > 0 && Number.isFinite(sceneRadius) && sceneRadius > 0
        ? sceneRadius / sourceAu
        : null;
    })
    .filter((ratio) => Number.isFinite(ratio) && ratio > 0)
    .sort((left, right) => left - right);
  if (ratios.length < 2) {
    return { count: ratios.length, maxRelativeError: 0 };
  }
  const mid = Math.floor(ratios.length / 2);
  const median = ratios.length % 2 ? ratios[mid] : (ratios[mid - 1] + ratios[mid]) / 2;
  const maxRelativeError = Math.max(
    ...ratios.map((ratio) => Math.abs(ratio - median) / Math.max(0.000001, median)),
  );
  return { count: ratios.length, maxRelativeError };
}

function buildStarLayout(stars, hierarchy, binaryOrbits) {
  const canonicalKeyByAlias = new Map();
  stars.forEach((star) => {
    const canonicalKey = star.render_key || star.key;
    keyAliasesForBody(star).forEach((alias) => {
      canonicalKeyByAlias.set(alias, canonicalKey);
    });
  });

  const collectStarKeys = (node) => {
    if (!node) {
      return [];
    }
    const childKeys = (node.children || []).flatMap((child) => collectStarKeys(child));
    const nodeKey = hierarchyRenderKey(node);
    const mappedKey = canonicalKeyByAlias.get(nodeKey);
    const isStar = ["star", "stellar_component"].includes(String(node.component_type || node.component_family || ""))
      || ["star", "inferred_star_leaf"].includes(String(node.node_kind || ""));
    if (!isStar || childKeys.length) {
      return [...new Set(childKeys)];
    }
    return mappedKey ? [mappedKey] : [];
  };

  const root = hierarchy?.root;
  const hierarchyGroups = new Map();
  const collectHierarchyGroups = (node) => {
    if (!node) {
      return;
    }
    const starKeys = collectStarKeys(node);
    const groupKey = hierarchyRenderKey(node) || node.display_name;
    if (groupKey && starKeys.length > 0 && node !== root) {
      hierarchyGroups.set(groupKey, {
        key: groupKey,
        label: node.display_name,
        starKeys: [...new Set(starKeys)],
      });
    }
    (node.children || []).forEach((child) => collectHierarchyGroups(child));
  };
  collectHierarchyGroups(root);

  const groups = (root?.children || [])
    .map((child) => ({
      key: hierarchyRenderKey(child) || child.display_name,
      label: child.display_name,
      starKeys: collectStarKeys(child),
    }))
    .filter((group) => group.starKeys.length > 0);

  const groupedKeys = new Set(groups.flatMap((group) => group.starKeys));
  const ungroupedKeys = stars
    .map((star) => star.render_key || star.key)
    .filter((key) => key && !groupedKeys.has(key));
  if (ungroupedKeys.length) {
    groups.push({ key: "ungrouped", label: "Ungrouped", starKeys: ungroupedKeys });
  }
  if (!groups.length) {
    groups.push({ key: "root", label: "System", starKeys: stars.map((star) => star.render_key || star.key).filter(Boolean) });
  }

  const starToGroup = new Map();
  const starToGroups = new Map();
  const groupCenters = new Map();
  const starPositions = new Map();
  const groupRadius = groups.length > 1 ? Math.min(4.7, 2.6 + groups.length * 0.4) : 0;

  groups.forEach((group, groupIndex) => {
    const angle = groups.length > 1 ? (groupIndex / groups.length) * Math.PI * 2 + Math.PI / 8 : 0;
    const center = [Math.cos(angle) * groupRadius, 0, Math.sin(angle) * groupRadius];
    groupCenters.set(group.key, center);
    group.starKeys.forEach((key) => starToGroup.set(key, group.key));

    const localRadius = group.starKeys.length > 1 ? Math.min(1.05, 0.44 + group.starKeys.length * 0.1) : 0;
    group.starKeys.forEach((key, idx) => {
      const localAngle = group.starKeys.length > 1 ? (idx / group.starKeys.length) * Math.PI * 2 : 0;
      starPositions.set(key, addVector(center, [Math.cos(localAngle) * localRadius, 0, Math.sin(localAngle) * localRadius]));
    });
  });

  hierarchyGroups.forEach((group, groupKey) => {
    const positions = group.starKeys.map((key) => starPositions.get(key)).filter(Boolean);
    if (positions.length) {
      groupCenters.set(groupKey, averageVector(positions));
    }
    group.starKeys.forEach((starKey) => {
      if (!starToGroups.has(starKey)) {
        starToGroups.set(starKey, new Set());
      }
      starToGroups.get(starKey).add(groupKey);
    });
  });
  starToGroup.forEach((groupKey, starKey) => {
    if (!starToGroups.has(starKey)) {
      starToGroups.set(starKey, new Set());
    }
    starToGroups.get(starKey).add(groupKey);
  });

  const groupAncestorKeys = new Map();
  hierarchyGroups.forEach((group, groupKey) => {
    const groupStars = new Set(group.starKeys);
    const ancestors = [];
    hierarchyGroups.forEach((candidate, candidateKey) => {
      if (candidateKey === groupKey || candidate.starKeys.length <= group.starKeys.length) {
        return;
      }
      const candidateStars = new Set(candidate.starKeys);
      if ([...groupStars].every((key) => candidateStars.has(key))) {
        ancestors.push(candidateKey);
      }
    });
    groupAncestorKeys.set(groupKey, ancestors);
  });

  const orbitCenters = new Map();
  const orbitStarKeys = new Set();
  binaryOrbits.forEach((orbit, idx) => {
    const primaryKey = canonicalKeyByAlias.get(orbit.primary_body_key) || orbit.primary_body_key;
    const secondaryKey = canonicalKeyByAlias.get(orbit.secondary_body_key) || orbit.secondary_body_key;
    const primaryGroup = starToGroup.get(primaryKey);
    const secondaryGroup = starToGroup.get(secondaryKey);
    const primaryCenter = primaryGroup ? groupCenters.get(primaryGroup) : starPositions.get(primaryKey);
    const secondaryCenter = secondaryGroup ? groupCenters.get(secondaryGroup) : starPositions.get(secondaryKey);
    let center = [0, 0, 0];
    if (primaryGroup && primaryGroup === secondaryGroup) {
      center = groupCenters.get(primaryGroup) || center;
    } else if (primaryCenter && secondaryCenter) {
      center = scaledVector(addVector(primaryCenter, secondaryCenter), 0.5);
    } else if (primaryCenter || secondaryCenter) {
      center = primaryCenter || secondaryCenter;
    } else if (binaryOrbits.length > 1) {
      const angle = (idx / binaryOrbits.length) * Math.PI * 2;
      center = [Math.cos(angle) * 2.2, 0, Math.sin(angle) * 2.2];
    }
    orbitCenters.set(orbit.orbit_key || `orbit-${idx}`, center);
    orbitStarKeys.add(primaryKey);
    orbitStarKeys.add(secondaryKey);
  });

  return {
    canonicalKeyByAlias,
    starPositions,
    orbitCenters,
    orbitStarKeys,
    starToGroup,
    starToGroups,
    groupCenters,
    hierarchyGroups,
    groupAncestorKeys,
  };
}

function groupKeysContainingBodyKeys(keys, layout) {
  const groupKeys = new Set();
  (keys || []).forEach((key) => {
    const starKey = layout.canonicalKeyByAlias.get(key) || key;
    const starGroups = layout.starToGroups.get(starKey);
    if (starGroups?.size) {
      starGroups.forEach((groupKey) => groupKeys.add(groupKey));
    }
  });
  return [...groupKeys];
}

function groupKeysForOrbitSide(bodyKey, childKeys, layout) {
  const mappedBodyKey = layout.canonicalKeyByAlias.get(bodyKey) || bodyKey;
  if (mappedBodyKey && layout.hierarchyGroups.has(mappedBodyKey)) {
    return [mappedBodyKey];
  }
  const childStarKeys = new Set((childKeys || []).map((key) => layout.canonicalKeyByAlias.get(key) || key).filter(Boolean));
  if (childStarKeys.size) {
    for (const [groupKey, group] of layout.hierarchyGroups.entries()) {
      if (sameKeySet(new Set(group.starKeys), childStarKeys)) {
        return [groupKey];
      }
    }
  }
  return groupKeysContainingBodyKeys(childKeys, layout);
}

function starKeysForOrbitSide(bodyKey, childKeys, layout) {
  const mappedBodyKey = layout.canonicalKeyByAlias.get(bodyKey) || bodyKey;
  if (mappedBodyKey && layout.starPositions.has(mappedBodyKey)) {
    return [mappedBodyKey];
  }
  if (mappedBodyKey && layout.hierarchyGroups.has(mappedBodyKey)) {
    return layout.hierarchyGroups.get(mappedBodyKey)?.starKeys || [];
  }
  const childStarKeys = (childKeys || [])
    .flatMap((key) => {
      const mappedKey = layout.canonicalKeyByAlias.get(key) || key;
      if (layout.starPositions.has(mappedKey)) {
        return [mappedKey];
      }
      if (layout.hierarchyGroups.has(mappedKey)) {
        return layout.hierarchyGroups.get(mappedKey)?.starKeys || [];
      }
      return [];
    });
  return [...new Set(childStarKeys)];
}

function buildGroupMotionSpecs(groupOrbits, layout, starsByKey, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure") {
  const multiplier = Number(visualScale.binary_orbit_radius?.group_pair_motion_multiplier || 0.55);
  return (groupOrbits || [])
    .map((orbit) => {
      const primaryGroupKeys = groupKeysForOrbitSide(orbit.primary_body_key, orbit.primary_child_body_keys, layout);
      const secondaryGroupKeys = groupKeysForOrbitSide(orbit.secondary_body_key, orbit.secondary_child_body_keys, layout);
      const primaryStarKeys = starKeysForOrbitSide(orbit.primary_body_key, orbit.primary_child_body_keys, layout);
      const secondaryStarKeys = starKeysForOrbitSide(orbit.secondary_body_key, orbit.secondary_child_body_keys, layout);
      const primarySet = new Set(primaryGroupKeys);
      const secondarySet = new Set(secondaryGroupKeys);
      const overlap = [...primarySet].some((key) => secondarySet.has(key));
      if (!primaryGroupKeys.length || !secondaryGroupKeys.length || overlap) {
        return null;
      }
      const massFractions = barycentricMassFractions(
        massForBodyKeys(primaryStarKeys, starsByKey),
        massForBodyKeys(secondaryStarKeys, starsByKey),
      );
      return {
        orbit,
        primaryGroupKeys,
        secondaryGroupKeys,
        primaryStarKeys,
        secondaryStarKeys,
        primaryAncestorGroupKeys: [...new Set(primaryGroupKeys.flatMap((key) => layout.groupAncestorKeys.get(key) || []))],
        secondaryAncestorGroupKeys: [...new Set(secondaryGroupKeys.flatMap((key) => layout.groupAncestorKeys.get(key) || []))],
        massFractions,
        periodDays: Math.max(0.05, numericField(orbit.fields, "period_days") || 80),
        phaseRad: numericField(orbit.fields, "phase_rad") || 0,
        eccentricity: Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0)),
        inclinationRad: THREE.MathUtils.degToRad(numericField(orbit.fields, "inclination_deg") || 0),
        orbitRadius: scaledBinaryOrbitRadius(orbit, visualScale, scaleMode, 1.6) * multiplier,
      };
    })
    .filter(Boolean);
}

function directGroupOffsetAt(groupKey, groupMotionSpecs, simDays, excludeOrbitKey = null) {
  if (!groupKey || !groupMotionSpecs?.length) {
    return [0, 0, 0];
  }
  return groupMotionSpecs.reduce((offset, spec) => {
    if (excludeOrbitKey && spec.orbit?.orbit_key === excludeOrbitKey) {
      return offset;
    }
    const side = spec.primaryGroupKeys.includes(groupKey)
      ? -spec.massFractions.primary
      : (spec.secondaryGroupKeys.includes(groupKey) ? spec.massFractions.secondary : 0);
    if (!side) {
      return offset;
    }
    const phase = spec.phaseRad + (simDays / spec.periodDays) * Math.PI * 2;
    return addVector(offset, scaledVector(orbitalPositionFromMeanAnomaly(phase, spec.orbitRadius, spec.eccentricity, spec.inclinationRad), side));
  }, [0, 0, 0]);
}

function groupOffsetAt(groupKey, groupMotionSpecs, simDays, layout = null, excludeOrbitKey = null) {
  if (!groupKey) {
    return [0, 0, 0];
  }
  const ancestorKeys = layout?.groupAncestorKeys?.get(groupKey) || [];
  return sumVectors([
    directGroupOffsetAt(groupKey, groupMotionSpecs, simDays, excludeOrbitKey),
    ...ancestorKeys.map((ancestorKey) => directGroupOffsetAt(ancestorKey, groupMotionSpecs, simDays, excludeOrbitKey)),
  ]);
}

function combinedGroupOffsetAt(groupKeys, groupMotionSpecs, simDays, layout = null) {
  const uniqueKeys = [...new Set((groupKeys || []).filter(Boolean))];
  const keysWithAncestors = new Set(uniqueKeys);
  if (layout?.groupAncestorKeys) {
    uniqueKeys.forEach((key) => {
      (layout.groupAncestorKeys.get(key) || []).forEach((ancestorKey) => keysWithAncestors.add(ancestorKey));
    });
  }
  return sumVectors([...keysWithAncestors].map((key) => directGroupOffsetAt(key, groupMotionSpecs, simDays)));
}

function averageGroupOffsetAt(groupKeys, groupMotionSpecs, simDays, layout = null, excludeOrbitKey = null) {
  const uniqueKeys = [...new Set((groupKeys || []).filter(Boolean))];
  return averageVector(uniqueKeys.map((key) => groupOffsetAt(key, groupMotionSpecs, simDays, layout, excludeOrbitKey)));
}

function groupPairCenterOffsetAt(primaryGroupKeys, secondaryGroupKeys, groupMotionSpecs, simDays, layout = null, excludeOrbitKey = null) {
  return averageVector([
    averageGroupOffsetAt(primaryGroupKeys, groupMotionSpecs, simDays, layout, excludeOrbitKey),
    averageGroupOffsetAt(secondaryGroupKeys, groupMotionSpecs, simDays, layout, excludeOrbitKey),
  ]);
}

function groupKeysForStarKeys(starKeys, layout) {
  const normalized = (starKeys || []).map((key) => layout.canonicalKeyByAlias.get(key) || key).filter(Boolean);
  if (!normalized.length) {
    return [];
  }
  const [firstKey, ...rest] = normalized;
  const common = new Set(layout.starToGroups.get(firstKey) || []);
  rest.forEach((starKey) => {
    const groups = layout.starToGroups.get(starKey) || new Set();
    [...common].forEach((groupKey) => {
      if (!groups.has(groupKey)) {
        common.delete(groupKey);
      }
    });
  });
  return [...common];
}

function AnimatedStarSphere({ star, position = [0, 0, 0], groupKeys = [], groupMotionSpecs, layout, simClockRef, running = true, speedMultiplier = 1, showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);

  useFrame(() => {
    if (!groupRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    groupRef.current.position.set(...addVector(position, combinedGroupOffsetAt(groupKeys, groupMotionSpecs, simDays, layout)));
  });

  return (
    <group ref={groupRef} position={position}>
      <StarSphere star={star} showLabels={showLabels} selectedObjectId={selectedObjectId} onHover={onHover} onSelect={onSelect} />
    </group>
  );
}

function BinaryOrbit({ orbit, starsByKey, layout, groupMotionSpecs, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure", center = [0, 0, 0], simClockRef, running = true, speedMultiplier = 1, showOrbits = true, showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const primaryRef = React.useRef(null);
  const secondaryRef = React.useRef(null);
  const primary = starsByKey.get(orbit.primary_body_key);
  const secondary = starsByKey.get(orbit.secondary_body_key);
  const periodDays = Math.max(0.05, numericField(orbit.fields, "period_days") || 8);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0));
  const phaseRad = numericField(orbit.fields, "phase_rad") || 0;
  const inclinationDeg = numericField(orbit.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const orbitRadius = scaledBinaryOrbitRadius(orbit, visualScale, scaleMode, 0.9);
  const massFractions = useMemo(() => binaryMassFractions(primary, secondary), [primary, secondary]);
  const relativePathPoints = useMemo(() => sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, 192), [orbitRadius, eccentricity, inclinationRad]);
  const primaryPathPoints = useMemo(() => scaledOrbitPoints(relativePathPoints, -massFractions.primary), [relativePathPoints, massFractions.primary]);
  const secondaryPathPoints = useMemo(() => scaledOrbitPoints(relativePathPoints, massFractions.secondary), [relativePathPoints, massFractions.secondary]);
  const orbitPayload = useMemo(() => {
    const payload = orbitHoverPayload(orbit);
    if (!payload) {
      return null;
    }
    const traceField = binaryTraceProvenanceField(massFractions);
    return {
      ...payload,
      rows: [
        ...payload.rows,
        staticReadoutRow(
          "Trace",
          String(traceField.value),
          traceField.status,
          traceField,
        ),
      ],
    };
  }, [orbit, massFractions]);
  const selected = Boolean(selectedObjectId && payloadId(orbitPayload) === selectedObjectId);
  const orbitHandlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(orbitPayload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(orbitPayload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(orbitPayload);
    },
  };

  useFrame(() => {
    if (!groupRef.current || !primaryRef.current || !secondaryRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    const theta = phaseRad + (simDays / periodDays) * Math.PI * 2;
    const motionGroupKeys = groupKeysForStarKeys([orbit.primary_body_key, orbit.secondary_body_key], layout);
    const centerOffset = combinedGroupOffsetAt(motionGroupKeys, groupMotionSpecs, simDays, layout);
    groupRef.current.position.set(...addVector(center, centerOffset));
    const relative = orbitalPositionFromMeanAnomaly(theta, orbitRadius, eccentricity, inclinationRad);
    primaryRef.current.position.set(...scaledVector(relative, -massFractions.primary));
    secondaryRef.current.position.set(...scaledVector(relative, massFractions.secondary));
  });

  if (!primary || !secondary) {
    return null;
  }
  return (
    <group ref={groupRef} data-testid="system-preview-binary-orbit">
      {showOrbits && (
        <>
          <lineLoop {...orbitHandlers} userData={{ hoverPayload: orbitPayload }}>
            <bufferGeometry>
              <bufferAttribute attach="attributes-position" args={[primaryPathPoints, 3]} />
            </bufferGeometry>
            <lineBasicMaterial color={selected ? "#fff4c4" : "#ffdca8"} transparent opacity={selected ? 0.95 : 0.62} />
          </lineLoop>
          <lineLoop {...orbitHandlers} userData={{ hoverPayload: orbitPayload }}>
            <bufferGeometry>
              <bufferAttribute attach="attributes-position" args={[secondaryPathPoints, 3]} />
            </bufferGeometry>
            <lineBasicMaterial color={massFractions.basis === "source_mass_ratio" ? "#f6c971" : "#fff4c4"} transparent opacity={selected ? 0.72 : 0.34} />
          </lineLoop>
        </>
      )}
      <group ref={primaryRef}>
        <StarSphere star={primary} showLabels={showLabels} selectedObjectId={selectedObjectId} onHover={onHover} onSelect={onSelect} />
      </group>
      <group ref={secondaryRef}>
        <StarSphere star={secondary} showLabels={showLabels} selectedObjectId={selectedObjectId} onHover={onHover} onSelect={onSelect} />
      </group>
    </group>
  );
}

function centerForBodyKeys(keys, layout, starsByKey) {
  const positions = (keys || [])
    .map((key) => {
      const mappedKey = layout.canonicalKeyByAlias.get(key) || key;
      if (layout.starPositions.has(mappedKey)) {
        return layout.starPositions.get(mappedKey);
      }
      const star = starsByKey.get(mappedKey);
      const starKey = star?.render_key || star?.key;
      return starKey && layout.starPositions.has(starKey) ? layout.starPositions.get(starKey) : null;
    })
    .filter(Boolean);
  if (!positions.length) {
    return null;
  }
  return scaledVector(positions.reduce((sum, position) => addVector(sum, position), [0, 0, 0]), 1 / positions.length);
}

function GroupOrbitGuide({ orbit, layout, starsByKey, groupMotionSpecs, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure", simClockRef, running = true, speedMultiplier = 1, showOrbits = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const primaryCenter = centerForBodyKeys(orbit.primary_child_body_keys, layout, starsByKey);
  const secondaryCenter = centerForBodyKeys(orbit.secondary_child_body_keys, layout, starsByKey);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0));
  const inclinationDeg = numericField(orbit.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const orbitRadius = scaledBinaryOrbitRadius(orbit, visualScale, scaleMode, 1.6);
  const payload = useMemo(() => orbitHoverPayload(orbit), [orbit]);
  const center = primaryCenter && secondaryCenter ? scaledVector(addVector(primaryCenter, secondaryCenter), 0.5) : [0, 0, 0];
  const primaryGroupKeys = groupKeysForOrbitSide(orbit.primary_body_key, orbit.primary_child_body_keys, layout);
  const secondaryGroupKeys = groupKeysForOrbitSide(orbit.secondary_body_key, orbit.secondary_child_body_keys, layout);
  const motionSpec = useMemo(
    () => (groupMotionSpecs || []).find((spec) => spec.orbit?.orbit_key === orbit.orbit_key) || null,
    [groupMotionSpecs, orbit],
  );
  const massFractions = motionSpec?.massFractions || { primary: 0.5, secondary: 0.5, basis: "equal_mass_visual_fallback" };
  const relativePathPoints = useMemo(() => sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, 224), [orbitRadius, eccentricity, inclinationRad]);
  const primaryPathPoints = useMemo(() => scaledOrbitPoints(relativePathPoints, -massFractions.primary), [relativePathPoints, massFractions.primary]);
  const secondaryPathPoints = useMemo(() => scaledOrbitPoints(relativePathPoints, massFractions.secondary), [relativePathPoints, massFractions.secondary]);
  const orbitPayload = useMemo(() => {
    if (!payload) {
      return null;
    }
    const traceField = binaryTraceProvenanceField(massFractions);
    return {
      ...payload,
      rows: [
        ...payload.rows,
        staticReadoutRow("Trace", String(traceField.value), traceField.status, traceField),
      ],
    };
  }, [payload, massFractions]);
  const selected = Boolean(selectedObjectId && payloadId(orbitPayload) === selectedObjectId);

  useFrame(() => {
    if (!groupRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    groupRef.current.position.set(...addVector(center, groupPairCenterOffsetAt(primaryGroupKeys, secondaryGroupKeys, groupMotionSpecs, simDays, layout, orbit.orbit_key)));
  });

  if (!showOrbits || !primaryCenter || !secondaryCenter) {
    return null;
  }

  const handlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(orbitPayload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(orbitPayload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(orbitPayload);
    },
  };
  return (
    <group ref={groupRef} position={center} data-testid="system-preview-group-orbit-guide">
      <lineLoop {...handlers} userData={{ hoverPayload: orbitPayload }}>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" args={[primaryPathPoints, 3]} />
        </bufferGeometry>
        <lineBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.52 : 0.24} />
      </lineLoop>
      <lineLoop {...handlers} userData={{ hoverPayload: orbitPayload }}>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" args={[secondaryPathPoints, 3]} />
        </bufferGeometry>
        <lineBasicMaterial color={massFractions.basis === "source_mass_ratio" ? "#f0bf55" : "#fff4c4"} transparent opacity={selected ? 0.88 : 0.44} />
      </lineLoop>
    </group>
  );
}

function simulationTreeNodes(tree) {
  return new Map(Object.entries(tree?.nodes || {}));
}

function treeNodeMassRecord(node, nodesByKey, starsByKey) {
  if (!node) {
    return null;
  }
  if (node.node_type === "body") {
    return positiveStellarMass(starsByKey.get(node.body_key));
  }
  const leafKeys = node.leaf_body_keys || [];
  if (leafKeys.length) {
    return massForBodyKeys(leafKeys, starsByKey);
  }
  const childMasses = (node.children || [])
    .map((childKey) => treeNodeMassRecord(nodesByKey.get(childKey), nodesByKey, starsByKey))
    .filter((record) => Number.isFinite(record?.mass) && record.mass > 0);
  if (!childMasses.length) {
    return null;
  }
  return {
    mass: childMasses.reduce((sum, record) => sum + record.mass, 0),
    status: weakestMassStatus(childMasses.map((record) => record.status)),
  };
}

function treeOrbitSpec(node, nodesByKey, orbitsByKey, starsByKey, visualScale, scaleMode) {
  if (!node || node.node_type !== "barycenter") {
    return null;
  }
  const orbit = orbitsByKey.get(node.orbit_key);
  const children = (node.children || []).map((childKey) => nodesByKey.get(childKey)).filter(Boolean);
  if (!orbit || children.length < 2) {
    return null;
  }
  const primary = children[0];
  const secondary = children[1];
  const massFractions = barycentricMassFractions(
    treeNodeMassRecord(primary, nodesByKey, starsByKey),
    treeNodeMassRecord(secondary, nodesByKey, starsByKey),
  );
  const fallbackRadius = orbit.endpoint_kind === "group_pair" ? 1.6 : 0.9;
  return {
    nodeKey: node.node_key,
    orbit,
    primaryNodeKey: primary.node_key,
    secondaryNodeKey: secondary.node_key,
    massFractions,
    periodDays: Math.max(0.05, numericField(orbit.fields, "period_days") || (orbit.endpoint_kind === "group_pair" ? 80 : 8)),
    phaseRad: numericField(orbit.fields, "phase_rad") || 0,
    eccentricity: Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0)),
    inclinationRad: THREE.MathUtils.degToRad(numericField(orbit.fields, "inclination_deg") || 0),
    orbitRadius: scaledBinaryOrbitRadius(orbit, visualScale, scaleMode, fallbackRadius),
  };
}

function computeSimulationTreeTransforms(tree, nodesByKey, orbitsByKey, starsByKey, visualScale, scaleMode, simDays) {
  const nodePositions = new Map();
  const bodyPositions = new Map();
  const visited = new Set();
  const rootKey = tree?.root_node_key;

  const visit = (nodeKey, position) => {
    if (!nodeKey || visited.has(nodeKey)) {
      return;
    }
    visited.add(nodeKey);
    const node = nodesByKey.get(nodeKey);
    if (!node) {
      return;
    }
    nodePositions.set(nodeKey, position);
    if (node.node_type === "body" && node.body_key) {
      bodyPositions.set(node.body_key, position);
      return;
    }
    const children = node.children || [];
    const spec = treeOrbitSpec(node, nodesByKey, orbitsByKey, starsByKey, visualScale, scaleMode);
    if (spec && children.length >= 2) {
      const meanAnomaly = spec.phaseRad + (simDays / spec.periodDays) * Math.PI * 2;
      const relative = orbitalPositionFromMeanAnomaly(meanAnomaly, spec.orbitRadius, spec.eccentricity, spec.inclinationRad);
      visit(children[0], addVector(position, scaledVector(relative, -spec.massFractions.primary)));
      visit(children[1], addVector(position, scaledVector(relative, spec.massFractions.secondary)));
      children.slice(2).forEach((childKey) => visit(childKey, position));
      return;
    }
    children.forEach((childKey) => visit(childKey, position));
  };

  visit(rootKey, [0, 0, 0]);
  return { nodePositions, bodyPositions };
}

function simulationTreeBodyPositionAt(treeContext, bodyKey, simDays) {
  if (!treeContext || !bodyKey) {
    return null;
  }
  const transforms = computeSimulationTreeTransforms(
    treeContext.simulationTree,
    treeContext.nodesByKey,
    treeContext.orbitsByKey,
    treeContext.starsByKey,
    treeContext.visualScale,
    treeContext.scaleMode,
    simDays,
  );
  return transforms.bodyPositions.get(bodyKey) || null;
}

function TreeOrbitGuide({ spec, groupRefSetter, showOrbits = true, selectedObjectId = "", onHover, onSelect }) {
  const relativePathPoints = useMemo(
    () => sampledOrbitPoints(spec.orbitRadius, spec.eccentricity, spec.inclinationRad, spec.orbit.endpoint_kind === "group_pair" ? 224 : 192),
    [spec.eccentricity, spec.inclinationRad, spec.orbit.endpoint_kind, spec.orbitRadius],
  );
  const primaryPathPoints = useMemo(
    () => scaledOrbitPoints(relativePathPoints, -spec.massFractions.primary),
    [relativePathPoints, spec.massFractions.primary],
  );
  const secondaryPathPoints = useMemo(
    () => scaledOrbitPoints(relativePathPoints, spec.massFractions.secondary),
    [relativePathPoints, spec.massFractions.secondary],
  );
  const orbitPayload = useMemo(() => {
    const payload = orbitHoverPayload(spec.orbit);
    if (!payload) {
      return null;
    }
    const traceField = binaryTraceProvenanceField(spec.massFractions);
    return {
      ...payload,
      rows: [
        ...payload.rows,
        staticReadoutRow("Trace", String(traceField.value), traceField.status, traceField),
      ],
    };
  }, [spec]);
  const selected = Boolean(selectedObjectId && payloadId(orbitPayload) === selectedObjectId);
  const handlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(orbitPayload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(orbitPayload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(orbitPayload);
    },
  };

  return (
    <group ref={(element) => groupRefSetter(spec.nodeKey, element)} data-testid={spec.orbit.endpoint_kind === "group_pair" ? "system-preview-group-orbit-guide" : "system-preview-binary-orbit"}>
      {showOrbits && (
        <>
          <lineLoop {...handlers} userData={{ hoverPayload: orbitPayload }}>
            <bufferGeometry>
              <bufferAttribute attach="attributes-position" args={[primaryPathPoints, 3]} />
            </bufferGeometry>
            <lineBasicMaterial color={selected ? "#fff4c4" : (spec.orbit.endpoint_kind === "group_pair" ? "#7ddcff" : "#ffdca8")} transparent opacity={selected ? 0.78 : (spec.orbit.endpoint_kind === "group_pair" ? 0.28 : 0.62)} />
          </lineLoop>
          <lineLoop {...handlers} userData={{ hoverPayload: orbitPayload }}>
            <bufferGeometry>
              <bufferAttribute attach="attributes-position" args={[secondaryPathPoints, 3]} />
            </bufferGeometry>
            <lineBasicMaterial color={spec.massFractions.basis === "source_mass_ratio" ? "#f0bf55" : "#fff4c4"} transparent opacity={selected ? 0.88 : (spec.orbit.endpoint_kind === "group_pair" ? 0.44 : 0.34)} />
          </lineLoop>
        </>
      )}
    </group>
  );
}

function SimulationTreeObjects({ simulationTree, stars, subsystems = [], renderOrbits = [], starsByKey, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure", simClockRef, showOrbits = true, showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const nodesByKey = useMemo(() => simulationTreeNodes(simulationTree), [simulationTree]);
  const orbitsByKey = useMemo(() => new Map((renderOrbits || []).map((orbit) => [orbit.orbit_key, orbit])), [renderOrbits]);
  const bodyRefs = React.useRef(new Map());
  const orbitRefs = React.useRef(new Map());
  const subsystemRefs = React.useRef(new Map());
  const setMapRef = useCallback((mapRef, key, element) => {
    if (!key) {
      return;
    }
    if (element) {
      mapRef.current.set(key, element);
    } else {
      mapRef.current.delete(key);
    }
  }, []);
  const orbitSpecs = useMemo(() => (
    [...nodesByKey.values()]
      .map((node) => treeOrbitSpec(node, nodesByKey, orbitsByKey, starsByKey, visualScale, scaleMode))
      .filter(Boolean)
  ), [nodesByKey, orbitsByKey, starsByKey, visualScale, scaleMode]);

  useFrame(() => {
    const simDays = currentSimulationDays(simClockRef);
    const transforms = computeSimulationTreeTransforms(
      simulationTree,
      nodesByKey,
      orbitsByKey,
      starsByKey,
      visualScale,
      scaleMode,
      simDays,
    );
    bodyRefs.current.forEach((group, bodyKey) => {
      const position = transforms.bodyPositions.get(bodyKey);
      if (position) {
        group.position.set(...position);
      }
    });
    orbitRefs.current.forEach((group, nodeKey) => {
      const position = transforms.nodePositions.get(nodeKey);
      if (position) {
        group.position.set(...position);
      }
    });
    subsystemRefs.current.forEach((group, subsystemKey) => {
      const subsystem = (subsystems || []).find((item) => (item.render_key || item.key) === subsystemKey);
      const positions = (subsystem?.child_body_keys || [])
        .map((key) => transforms.bodyPositions.get(key))
        .filter(Boolean);
      if (positions.length) {
        group.position.set(...averageVector(positions));
      }
    });
  });

  return (
    <>
      {orbitSpecs.map((spec) => (
        <TreeOrbitGuide
          key={spec.nodeKey}
          spec={spec}
          groupRefSetter={(key, element) => setMapRef(orbitRefs, key, element)}
          showOrbits={showOrbits}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {stars.map((star) => {
        const starKey = star.render_key || star.key;
        return (
          <group key={starKey} ref={(element) => setMapRef(bodyRefs, starKey, element)}>
            <StarSphere
              star={star}
              showLabels={showLabels}
              selectedObjectId={selectedObjectId}
              onHover={onHover}
              onSelect={onSelect}
            />
          </group>
        );
      })}
      {(subsystems || []).map((subsystem) => {
        const subsystemKey = subsystem.render_key || subsystem.key;
        return (
          <group key={subsystemKey} ref={(element) => setMapRef(subsystemRefs, subsystemKey, element)}>
            <SubsystemMarker
              subsystem={subsystem}
              center={[0, 0, 0]}
              groupKeys={[]}
              groupMotionSpecs={[]}
              layout={null}
              simClockRef={simClockRef}
              showLabels={showLabels}
              selectedObjectId={selectedObjectId}
              onHover={onHover}
              onSelect={onSelect}
            />
          </group>
        );
      })}
    </>
  );
}

function SubsystemMarker({ subsystem, center = [0, 0, 0], groupKeys = [], groupMotionSpecs, layout, simClockRef, running = true, speedMultiplier = 1, showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const payload = useMemo(() => objectHoverPayload("subsystem", subsystem), [subsystem]);
  const selected = Boolean(selectedObjectId && payloadId(payload) === selectedObjectId);

  useFrame(() => {
    if (!groupRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    groupRef.current.position.set(...addVector(center, combinedGroupOffsetAt(groupKeys, groupMotionSpecs, simDays, layout)));
  });

  const handlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(payload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(payload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(payload);
    },
  };

  return (
    <group ref={groupRef} position={center} data-testid="system-preview-subsystem-marker">
      <mesh {...handlers} rotation={[Math.PI / 2, 0, 0]} userData={{ hoverPayload: payload }}>
        <torusGeometry args={[selected ? 0.25 : 0.19, selected ? 0.018 : 0.011, 8, 44]} />
        <meshBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.9 : 0.58} />
      </mesh>
      <mesh {...handlers} rotation={[0, Math.PI / 2, 0]} userData={{ hoverPayload: payload }}>
        <torusGeometry args={[selected ? 0.2 : 0.155, selected ? 0.014 : 0.008, 8, 36]} />
        <meshBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.5 : 0.26} />
      </mesh>
      <mesh {...handlers} userData={{ hoverPayload: payload }}>
        <sphereGeometry args={[selected ? 0.055 : 0.04, 12, 8]} />
        <meshBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.92 : 0.62} />
      </mesh>
      <SceneLabel
        text={subsystem.display_name || subsystem.name || "Subsystem"}
        position={[0, -0.36, 0]}
        color="#b7f3ff"
        scale={0.72}
        visible={showLabels}
      />
    </group>
  );
}

function PlanetObject({ planet, orbitRadius, color, center = [0, 0, 0], motionGroupKeys = [], groupMotionSpecs, layout, treeContext = null, treeHostBodyKey = null, simClockRef, running = true, speedMultiplier = 1, showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const periodDays = Math.max(0.05, numericField(planet.fields, "orbital_period_days") || Number(planet.periodDays) || 8 + orbitRadius * 2.2);
  const eccentricity = displayPlanetEccentricity(planet);
  const phaseRad = numericField(planet.fields, "phase_rad") || Number(planet.phaseRad) || 0;
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const visualKind = planetVisualKind(planet);
  const pickRadius = Number(planet.pick_radius_scene) || Math.max(planet.radius * 2.1, 0.2);
  const texture = useMemo(() => createPlanetTexture(planet.render_key || planet.key || planet.display_name || planet.name, visualKind), [planet, visualKind]);
  useEffect(() => () => texture?.dispose?.(), [texture]);
  const hoverPayload = useMemo(() => objectHoverPayload("planet", planet), [planet]);
  const selected = Boolean(selectedObjectId && payloadId(hoverPayload) === selectedObjectId);
  const hoverHandlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(hoverPayload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(hoverPayload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(hoverPayload);
    },
  };

  useFrame(() => {
    if (!groupRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    const meanAnomaly = phaseRad + (simDays / periodDays) * Math.PI * 2;
    const treeCenter = simulationTreeBodyPositionAt(treeContext, treeHostBodyKey, simDays);
    const movingCenter = treeCenter || addVector(center, combinedGroupOffsetAt(motionGroupKeys, groupMotionSpecs, simDays, layout));
    groupRef.current.position.set(...addVector(movingCenter, orbitalPositionFromMeanAnomaly(meanAnomaly, orbitRadius, eccentricity, inclinationRad)));
  });

  return (
    <group ref={groupRef} position={addVector(center, orbitalPositionFromMeanAnomaly(phaseRad, orbitRadius, eccentricity, inclinationRad))}>
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[planet.radius, 18, 14]} />
        <meshStandardMaterial color={color} map={texture || null} roughness={0.72} metalness={0.03} />
      </mesh>
      <mesh>
        <sphereGeometry args={[planet.radius * 1.08, 18, 14]} />
        <meshBasicMaterial color="#b7e2ff" transparent opacity={selected ? 0.2 : (visualKind === "gas_giant" ? 0.05 : 0.09)} depthWrite={false} blending={THREE.AdditiveBlending} />
      </mesh>
      {selected && <SelectionHalo radius={Math.max(pickRadius, 0.22)} color="#b7e2ff" pulse />}
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[pickRadius, 14, 10]} />
        <meshBasicMaterial transparent opacity={0} depthWrite={false} />
      </mesh>
      <SceneLabel
        text={planet.display_name || planet.name || "Planet"}
        position={[0, -Math.max(pickRadius + 0.08, planet.radius + 0.2), 0]}
        color="#d7efff"
        scale={0.72}
        visible={showLabels}
      />
    </group>
  );
}

function CanvasHoverRaycaster({ onHover }) {
  const { camera, gl, raycaster, scene } = useThree();
  const lastPayloadRef = React.useRef(null);

  useEffect(() => {
    const target = gl.domElement;
    const pointer = new THREE.Vector2();
    const previousLineThreshold = raycaster.params.Line?.threshold;
    const previousPointsThreshold = raycaster.params.Points?.threshold;
    raycaster.params.Line = { ...(raycaster.params.Line || {}), threshold: 0.12 };
    raycaster.params.Points = { ...(raycaster.params.Points || {}), threshold: 0.12 };
    target.dataset.raycasterLineThreshold = "0.12";

    const setHover = (payload) => {
      if (lastPayloadRef.current === payload) {
        return;
      }
      lastPayloadRef.current = payload;
      onHover?.(payload);
    };

    const handleMove = (event) => {
      const rect = target.getBoundingClientRect();
      pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
      pointer.y = -(((event.clientY - rect.top) / rect.height) * 2 - 1);
      scene.updateMatrixWorld(true);
      camera.updateMatrixWorld();
      raycaster.setFromCamera(pointer, camera);
      const hit = raycaster
        .intersectObjects(scene.children, true)
        .find((intersection) => intersection.object?.userData?.hoverPayload);
      setHover(hit?.object?.userData?.hoverPayload || null);
    };

    const handleLeave = () => setHover(null);

    target.addEventListener("pointermove", handleMove);
    target.addEventListener("pointerleave", handleLeave);
    return () => {
      target.removeEventListener("pointermove", handleMove);
      target.removeEventListener("pointerleave", handleLeave);
      raycaster.params.Line = { ...(raycaster.params.Line || {}), threshold: previousLineThreshold };
      raycaster.params.Points = { ...(raycaster.params.Points || {}), threshold: previousPointsThreshold };
    };
  }, [camera, gl, onHover, raycaster, scene]);

  return null;
}

function CameraControls({ resetToken = 0 }) {
  const { camera, gl } = useThree();
  const controlsRef = React.useRef(null);
  const writeCameraState = useCallback(() => {
    gl.domElement.dataset.cameraPosition = [
      camera.position.x.toFixed(3),
      camera.position.y.toFixed(3),
      camera.position.z.toFixed(3),
    ].join(",");
  }, [camera, gl]);

  useEffect(() => {
    const controls = new OrbitControls(camera, gl.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
    controls.enablePan = true;
    controls.minDistance = 3.8;
    controls.maxDistance = 34;
    controls.rotateSpeed = 0.62;
    controls.zoomSpeed = 0.72;
    controls.panSpeed = 0.55;
    controls.target.set(0, 0, 0);
    controls.saveState();
    controlsRef.current = controls;
    writeCameraState();
    return () => {
      controls.dispose();
      controlsRef.current = null;
    };
  }, [camera, gl, writeCameraState]);

  useEffect(() => {
    if (!controlsRef.current) {
      return;
    }
    camera.position.set(0, 6.2, 10.8);
    controlsRef.current.target.set(0, 0, 0);
    controlsRef.current.saveState();
    controlsRef.current.reset();
    controlsRef.current.update();
    writeCameraState();
  }, [camera, resetToken, writeCameraState]);

  useFrame(() => {
    if (controlsRef.current) {
      controlsRef.current.update();
      writeCameraState();
    }
  });

  return null;
}

function SceneMotionMetrics({
  directOrbitCount = 0,
  groupOrbitCount = 0,
  subsystemMarkerCount = 0,
  starCount = 0,
  planetCount = 0,
  planetOrbitCount = 0,
  planetTrailCount = 0,
  planetDisplayEccentricityCappedCount = 0,
  trueOrbitScaleSampleCount = 0,
  trueOrbitScaleMaxRelativeError = null,
  habitableZoneCount = 0,
  habitableZoneMaxPlaneInclinationDeg = 0,
  starClassStatusCounts = {},
  scaleMode = "structure",
  collisionAdjustedStarCount = 0,
  minStarClearance = null,
  minStarSeparation = null,
  groupMotionSpecs = [],
  simulationTree = null,
  useSimulationTree = false,
  planetHostGroupCount = 0,
  treeHostedPlanetCount = 0,
  labelCount = 0,
  simClockRef,
  running = true,
  speedMultiplier = 1,
  onClockSample,
}) {
  const { gl } = useThree();
  const lastClockSampleRef = React.useRef(null);
  const nestedCount = useMemo(() => (
    (groupMotionSpecs || []).filter((spec) => (
      (spec.primaryAncestorGroupKeys || []).length > 0
      || (spec.secondaryAncestorGroupKeys || []).length > 0
    )).length
  ), [groupMotionSpecs]);
  const massWeightedGroupMotionCount = useMemo(() => (
    (groupMotionSpecs || []).filter((spec) => ["source_mass_ratio", "derived_mass_ratio", "assumed_mass_ratio"].includes(spec.massFractions?.basis)).length
  ), [groupMotionSpecs]);
  const simulationTreeDiagnostics = simulationTree?.diagnostics || {};
  const inspectableOrbitCount = (directOrbitCount || 0) + (groupOrbitCount || 0) + (planetOrbitCount || 0);
  const inspectableTargetKinds = [
    starCount > 0 ? "star" : null,
    planetCount > 0 ? "planet" : null,
    subsystemMarkerCount > 0 ? "subsystem" : null,
    inspectableOrbitCount > 0 ? "orbit" : null,
  ].filter(Boolean).join(",");

  useEffect(() => {
    gl.domElement.dataset.groupMotionCount = String(groupMotionSpecs?.length || 0);
    gl.domElement.dataset.nestedGroupMotionCount = String(nestedCount);
    gl.domElement.dataset.massWeightedGroupMotionCount = String(massWeightedGroupMotionCount);
    gl.domElement.dataset.simulationTreeVersion = simulationTree?.schema_version || "";
    gl.domElement.dataset.simulationTreeActive = useSimulationTree ? "true" : "false";
    gl.domElement.dataset.simulationTreeNodeCount = String(simulationTreeDiagnostics.node_count || 0);
    gl.domElement.dataset.simulationTreeBodyCount = String(simulationTreeDiagnostics.body_node_count || 0);
    gl.domElement.dataset.simulationTreeOrbitCount = String(simulationTreeDiagnostics.orbit_node_count || 0);
    gl.domElement.dataset.simulationTreeNestedOrbitCount = String(simulationTreeDiagnostics.nested_orbit_count || 0);
    gl.domElement.dataset.simulationTreeUnattachedOrbitCount = String(simulationTreeDiagnostics.unattached_orbit_count || 0);
    gl.domElement.dataset.planetHostGroupCount = String(planetHostGroupCount || 0);
    gl.domElement.dataset.treeHostedPlanetCount = String(treeHostedPlanetCount || 0);
    gl.domElement.dataset.sceneLabelCount = String(labelCount || 0);
    gl.domElement.dataset.sceneLabelRenderer = labelCount > 0 ? "troika_sdf_text_v1" : "none";
    gl.domElement.dataset.directOrbitGuideCount = String(directOrbitCount || 0);
    gl.domElement.dataset.directOrbitTraceCount = String((directOrbitCount || 0) * 2);
    gl.domElement.dataset.groupOrbitGuideCount = String(groupOrbitCount || 0);
    gl.domElement.dataset.subsystemMarkerCount = String(subsystemMarkerCount || 0);
    gl.domElement.dataset.inspectableStarCount = String(starCount || 0);
    gl.domElement.dataset.inspectablePlanetCount = String(planetCount || 0);
    gl.domElement.dataset.planetTrailCount = String(planetTrailCount || 0);
    gl.domElement.dataset.planetDisplayEccentricityCappedCount = String(planetDisplayEccentricityCappedCount || 0);
    gl.domElement.dataset.trueOrbitScaleSampleCount = String(trueOrbitScaleSampleCount || 0);
    gl.domElement.dataset.trueOrbitScaleMaxRelativeError = Number.isFinite(Number(trueOrbitScaleMaxRelativeError))
      ? Number(trueOrbitScaleMaxRelativeError).toExponential(3)
      : "";
    gl.domElement.dataset.habitableZoneCount = String(habitableZoneCount || 0);
    gl.domElement.dataset.habitableZoneMaxPlaneInclinationDeg = Number.isFinite(Number(habitableZoneMaxPlaneInclinationDeg)) ? Number(habitableZoneMaxPlaneInclinationDeg).toFixed(3) : "";
    gl.domElement.dataset.inspectableSubsystemCount = String(subsystemMarkerCount || 0);
    gl.domElement.dataset.inspectableOrbitCount = String(inspectableOrbitCount);
    gl.domElement.dataset.inspectableTargetKinds = inspectableTargetKinds;
    gl.domElement.dataset.orbitTraceProvenanceCount = String(inspectableOrbitCount);
    gl.domElement.dataset.orbitTraceProvenanceVersion = "system_preview_orbit_trace_v1";
    gl.domElement.dataset.scaleMode = normalizeScaleMode(scaleMode);
    gl.domElement.dataset.collisionAdjustedStarCount = String(collisionAdjustedStarCount || 0);
    gl.domElement.dataset.minStarClearance = Number.isFinite(Number(minStarClearance)) ? Number(minStarClearance).toFixed(4) : "";
    gl.domElement.dataset.minStarSeparation = Number.isFinite(Number(minStarSeparation)) ? Number(minStarSeparation).toFixed(4) : "";
    gl.domElement.dataset.spectralClassSourceCount = String(starClassStatusCounts.source || 0);
    gl.domElement.dataset.spectralClassDerivedCount = String(starClassStatusCounts.derived || 0);
    gl.domElement.dataset.spectralClassAssumedCount = String(starClassStatusCounts.assumed || 0);
    gl.domElement.dataset.spectralClassMissingCount = String(starClassStatusCounts.missing || 0);
    gl.domElement.dataset.spectralClassUnsafeSourceCount = String(starClassStatusCounts.unsafeSource || 0);
    gl.domElement.dataset.simulationClockMode = "shared_local_beta";
    gl.domElement.dataset.simulationClockWriters = "1";
    gl.domElement.dataset.simulationRunning = running ? "true" : "false";
    gl.domElement.dataset.simulationSpeed = String(speedMultiplier || 1);
  }, [
    gl,
    directOrbitCount,
    groupMotionSpecs,
    habitableZoneCount,
    habitableZoneMaxPlaneInclinationDeg,
    labelCount,
    massWeightedGroupMotionCount,
    groupOrbitCount,
    inspectableOrbitCount,
    inspectableTargetKinds,
    nestedCount,
    collisionAdjustedStarCount,
    minStarClearance,
    minStarSeparation,
    planetCount,
    planetDisplayEccentricityCappedCount,
    trueOrbitScaleSampleCount,
    trueOrbitScaleMaxRelativeError,
    planetHostGroupCount,
    treeHostedPlanetCount,
    planetTrailCount,
    running,
    scaleMode,
    speedMultiplier,
    starClassStatusCounts,
    starCount,
    subsystemMarkerCount,
    simulationTree,
    simulationTreeDiagnostics,
    useSimulationTree,
  ]);

  useFrame(({ clock }) => {
    if (!simClockRef?.current) {
      return;
    }
    const simDays = advanceSimulationDays(simClockRef.current, clock.elapsedTime, running, speedMultiplier);
    gl.domElement.dataset.simulationDays = simDays.toFixed(3);
    if (onClockSample) {
      const roundedSample = Math.round(simDays * 10) / 10;
      if (lastClockSampleRef.current !== roundedSample) {
        lastClockSampleRef.current = roundedSample;
        onClockSample(roundedSample);
      }
    }
  });

  return null;
}

function PlanetOrbitRing({ planet, orbitRadius, center = [0, 0, 0], motionGroupKeys = [], groupMotionSpecs, layout, treeContext = null, treeHostBodyKey = null, simClockRef, running = true, speedMultiplier = 1, selectedObjectId = "", onHover, onSelect }) {
  const lineRef = React.useRef(null);
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const eccentricity = displayPlanetEccentricity(planet);
  const pathPoints = useMemo(() => sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, 224), [orbitRadius, eccentricity, inclinationRad]);
  const guideField = useMemo(() => planetOrbitGuideProvenanceField(planet), [planet]);
  const displayEccentricityField = useMemo(() => planetDisplayEccentricityField(planet), [planet]);
  const payload = useMemo(() => ({
    kind: "Planet orbit",
    name: `${planet.display_name || planet.name || "Planet"} orbit`,
    id: `${planet.render_key || planet.key || "planet"}:orbit`,
    sourceLayer: planet.source?.layer || "core",
    rows: [
      readoutRow(planet.fields, "orbital_period_days", "Period", "Unknown", 3),
      readoutRow(planet.fields, "semi_major_axis_au", "Orbit", "Unknown", 4),
      readoutRow(planet.fields, "eccentricity", "Ecc.", "Unknown", 3),
      ...(displayEccentricityField ? [
        staticReadoutRow("Display ecc.", formatNumber(displayEccentricityField.value, 3), "derived", displayEccentricityField),
      ] : []),
      readoutRow(planet.fields, "inclination_deg", "Incl.", "Unknown", 2),
      staticReadoutRow("Trace", String(guideField.value), guideField.status, guideField),
    ],
  }), [planet, guideField, displayEccentricityField]);
  const selected = Boolean(selectedObjectId && payloadId(payload) === selectedObjectId);
  const handlers = {
    onPointerOver: (event) => {
      event.stopPropagation();
      onHover?.(payload);
    },
    onPointerMove: (event) => {
      event.stopPropagation();
      onHover?.(payload);
    },
    onPointerOut: (event) => {
      event.stopPropagation();
      onHover?.(null);
    },
    onClick: (event) => {
      event.stopPropagation();
      onSelect?.(payload);
    },
  };

  useFrame(() => {
    if (!lineRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    const treeCenter = simulationTreeBodyPositionAt(treeContext, treeHostBodyKey, simDays);
    lineRef.current.position.set(...(treeCenter || addVector(center, combinedGroupOffsetAt(motionGroupKeys, groupMotionSpecs, simDays, layout))));
  });

  return (
    <lineLoop ref={lineRef} position={center} {...handlers} userData={{ hoverPayload: payload }}>
      <bufferGeometry>
        <bufferAttribute attach="attributes-position" args={[pathPoints, 3]} />
      </bufferGeometry>
      <lineBasicMaterial color={selected ? "#e6f6ff" : "#b1d6ff"} transparent opacity={selected ? 0.9 : 0.5} />
    </lineLoop>
  );
}

function PlanetOrbitTrail({ planet, orbitRadius, color = "#b7e2ff", center = [0, 0, 0], motionGroupKeys = [], groupMotionSpecs, layout, treeContext = null, treeHostBodyKey = null, simClockRef, scaleMode = "structure" }) {
  const lineRef = React.useRef(null);
  const attributeRef = React.useRef(null);
  const periodDays = Math.max(0.05, numericField(planet.fields, "orbital_period_days") || Number(planet.periodDays) || 8 + orbitRadius * 2.2);
  const eccentricity = displayPlanetEccentricity(planet);
  const phaseRad = numericField(planet.fields, "phase_rad") || Number(planet.phaseRad) || 0;
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const sampleCount = 42;
  const trailArc = normalizeScaleMode(scaleMode) === "true_bodies" ? Math.PI * 0.52 : Math.PI * 0.34;
  const initialPoints = useMemo(() => {
    const points = new Float32Array(sampleCount * 3);
    for (let idx = 0; idx < sampleCount; idx += 1) {
      const t = idx / Math.max(1, sampleCount - 1);
      const position = orbitalPositionFromMeanAnomaly(phaseRad - trailArc * t, orbitRadius, eccentricity, inclinationRad);
      points[idx * 3] = position[0];
      points[idx * 3 + 1] = position[1];
      points[idx * 3 + 2] = position[2];
    }
    return points;
  }, [eccentricity, inclinationRad, orbitRadius, phaseRad, trailArc]);

  useFrame(() => {
    if (!lineRef.current || !attributeRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    const theta = phaseRad + (simDays / periodDays) * Math.PI * 2;
    const treeCenter = simulationTreeBodyPositionAt(treeContext, treeHostBodyKey, simDays);
    const movingCenter = treeCenter || addVector(center, combinedGroupOffsetAt(motionGroupKeys, groupMotionSpecs, simDays, layout));
    lineRef.current.position.set(...movingCenter);
    const array = attributeRef.current.array;
    for (let idx = 0; idx < sampleCount; idx += 1) {
      const t = idx / Math.max(1, sampleCount - 1);
      const position = orbitalPositionFromMeanAnomaly(theta - trailArc * t, orbitRadius, eccentricity, inclinationRad);
      array[idx * 3] = position[0];
      array[idx * 3 + 1] = position[1];
      array[idx * 3 + 2] = position[2];
    }
    attributeRef.current.needsUpdate = true;
  });

  return (
    <line ref={lineRef} position={center} data-testid="system-preview-planet-trail">
      <bufferGeometry>
        <bufferAttribute ref={attributeRef} attach="attributes-position" args={[initialPoints, 3]} />
      </bufferGeometry>
      <lineBasicMaterial color={color} transparent opacity={normalizeScaleMode(scaleMode) === "true_bodies" ? 0.76 : 0.28} />
    </line>
  );
}

function HabitableZoneBand({ star, center = [0, 0, 0], maxOrbit = 1, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure", groupKeys = [], groupMotionSpecs, layout, treeContext = null, treeHostBodyKey = null, simClockRef, showLabels = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const bounds = useMemo(() => habitableZoneBoundsAu(star), [star]);
  const planeInclinationDeg = Number(star.habitable_zone_plane_inclination_deg) || 0;
  const planeInclinationRad = THREE.MathUtils.degToRad(planeInclinationDeg);
  const innerRadiusRaw = bounds ? scaledPlanetOrbitRadius(bounds.innerAu, maxOrbit, visualScale, scaleMode) : 0;
  const outerRadiusRaw = bounds ? scaledPlanetOrbitRadius(bounds.outerAu, maxOrbit, visualScale, scaleMode) : 0;
  const [innerRadius, outerRadius] = useMemo(() => {
    const inner = Math.min(innerRadiusRaw, outerRadiusRaw);
    const outer = Math.max(innerRadiusRaw, outerRadiusRaw);
    const minWidth = normalizeScaleMode(scaleMode) === "true_orbits" ? 0.045 : 0.075;
    if (outer - inner >= minWidth) {
      return [inner, outer];
    }
    const mid = (inner + outer) / 2;
    return [Math.max(0.02, mid - minWidth / 2), mid + minWidth / 2];
  }, [innerRadiusRaw, outerRadiusRaw, scaleMode]);
  const innerPoints = useMemo(() => sampledOrbitPoints(innerRadius, 0, planeInclinationRad, 192), [innerRadius, planeInclinationRad]);
  const outerPoints = useMemo(() => sampledOrbitPoints(outerRadius, 0, planeInclinationRad, 192), [outerRadius, planeInclinationRad]);
  const labelPosition = useMemo(() => orbitalPosition(-Math.PI / 2, (innerRadius + outerRadius) / 2, 0, planeInclinationRad), [innerRadius, outerRadius, planeInclinationRad]);
  const hoverPayload = useMemo(() => (bounds ? habitableZoneHoverPayload(star, bounds) : null), [star, bounds]);
  const selected = Boolean(selectedObjectId && payloadId(hoverPayload) === selectedObjectId);

  useFrame(() => {
    if (!groupRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    const treeCenter = simulationTreeBodyPositionAt(treeContext, treeHostBodyKey, simDays);
    groupRef.current.position.set(...(treeCenter || addVector(center, combinedGroupOffsetAt(groupKeys, groupMotionSpecs, simDays, layout))));
  });

  if (!bounds || outerRadius <= innerRadius) {
    return null;
  }

  return (
    <group ref={groupRef} position={center} data-testid="system-preview-habitable-zone">
      <mesh rotation={[Math.PI / 2 + planeInclinationRad, 0, 0]} userData={{ hoverPayload }}>
        <ringGeometry args={[innerRadius, outerRadius, 128]} />
        <meshBasicMaterial color="#76d78f" transparent opacity={selected ? 0.26 : 0.16} depthWrite={false} side={THREE.DoubleSide} />
      </mesh>
      <lineLoop userData={{ hoverPayload }}>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" args={[innerPoints, 3]} />
        </bufferGeometry>
        <lineBasicMaterial color="#d6ff9f" transparent opacity={selected ? 0.95 : 0.64} />
      </lineLoop>
      <lineLoop userData={{ hoverPayload }}>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" args={[outerPoints, 3]} />
        </bufferGeometry>
        <lineBasicMaterial color="#78e38f" transparent opacity={selected ? 0.95 : 0.68} />
      </lineLoop>
      <SceneLabel
        text="Habitable zone"
        position={labelPosition}
        color="#d8ffad"
        scale={0.78}
        visible={showLabels}
      />
    </group>
  );
}

function PreviewObjects({ stars, planets, subsystems = [], renderOrbits = [], simulationTree = null, hierarchy, visualScale = DEFAULT_VISUAL_SCALE, scaleMode = "structure", running = true, speedMultiplier = 1, resetToken = 0, showOrbits = true, showHabitableZones = false, showLabels = true, selectedObjectId = "", onHover, onSelect, onClockSample }) {
  const activeScaleMode = normalizeScaleMode(scaleMode);
  const binaryOrbits = renderOrbits.filter((orbit) => orbit.endpoint_kind !== "group_pair");
  const groupOrbits = renderOrbits.filter((orbit) => orbit.endpoint_kind === "group_pair");
  const layout = useMemo(() => buildStarLayout(stars, hierarchy, binaryOrbits), [stars, hierarchy, binaryOrbits]);
  const separationDiagnostics = useMemo(() => (
    computeStarSeparationDiagnostics(stars, layout, binaryOrbits, visualScale, activeScaleMode)
  ), [stars, layout, binaryOrbits, visualScale, activeScaleMode]);
  const collisionScale = useMemo(() => (
    applyCollisionSafeStarRadii(stars, separationDiagnostics, visualScale, activeScaleMode)
  ), [stars, separationDiagnostics, visualScale, activeScaleMode]);
  const displayStars = collisionScale.stars;
  const simClockRef = React.useRef({ days: 0, lastElapsedSeconds: null });
  useEffect(() => {
    simClockRef.current = { days: 0, lastElapsedSeconds: null };
  }, [resetToken, stars, planets, renderOrbits, activeScaleMode]);
  const starsByKey = useMemo(() => {
    const out = new Map();
    displayStars.forEach((star) => {
      const canonicalKey = star.render_key || star.key;
      keyAliasesForBody(star).forEach((alias) => out.set(alias, star));
      if (canonicalKey) {
        out.set(canonicalKey, star);
      }
    });
    return out;
  }, [displayStars]);
  const groupMotionSpecs = useMemo(
    () => buildGroupMotionSpecs(groupOrbits, layout, starsByKey, visualScale, activeScaleMode),
    [groupOrbits, layout, starsByKey, visualScale, activeScaleMode],
  );
  const useSimulationTree = Boolean(
    simulationTree?.schema_version === "simulation_tree_v1"
    && simulationTree?.root_node_key
    && simulationTree?.nodes
    && Number(simulationTree?.diagnostics?.unattached_orbit_count || 0) === 0
    && (displayStars.length <= 1 || Number(simulationTree?.diagnostics?.orbit_node_count || 0) > 0)
    && displayStars.length,
  );
  const simulationTreeContext = useMemo(() => (
    useSimulationTree
      ? {
        simulationTree,
        nodesByKey: simulationTreeNodes(simulationTree),
        orbitsByKey: new Map((renderOrbits || []).map((orbit) => [orbit.orbit_key, orbit])),
        starsByKey,
        visualScale,
        scaleMode: activeScaleMode,
      }
      : null
  ), [useSimulationTree, simulationTree, renderOrbits, starsByKey, visualScale, activeScaleMode]);
  const looseStars = displayStars.filter((star) => !layout.orbitStarKeys.has(star.render_key || star.key));
  const starCenterByCoreId = new Map();
  const starKeyByCoreId = new Map();
  displayStars.forEach((star) => {
    const starId = star?.source?.star_id;
    const key = star.render_key || star.key;
    if (starId !== undefined && starId !== null && key && layout.starPositions.has(key)) {
      starCenterByCoreId.set(Number(starId), layout.starPositions.get(key));
      starKeyByCoreId.set(Number(starId), key);
    }
  });
  const hzOuterAuValues = displayStars
    .map((star) => habitableZoneBoundsAu(star)?.outerAu)
    .filter((value) => Number.isFinite(value) && value > 0);
  const maxOrbit = Math.max(
    0.1,
    ...planets.map((planet) => planet.orbitAu || 0.1),
    ...hzOuterAuValues,
  );
  const displayPlanets = useMemo(() => (
    applyPlanetDisplayOrbitGeometry(planets, maxOrbit, visualScale, activeScaleMode)
  ), [planets, maxOrbit, visualScale, activeScaleMode]);
  const trueOrbitDiagnostics = useMemo(() => (
    trueOrbitScaleDiagnostics(displayPlanets, activeScaleMode)
  ), [displayPlanets, activeScaleMode]);
  const hostPlacementForPlanet = (planet) => {
    const placementForStarKey = (starKey) => ({
      center: layout.starPositions.get(starKey),
      groupKeys: groupKeysForStarKeys([starKey], layout),
    });
    const hostKey = layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key;
    if (hostKey && layout.starPositions.has(hostKey)) {
      return placementForStarKey(hostKey);
    }
    if (hostKey) {
      const star = starsByKey.get(hostKey);
      const starKey = star?.render_key || star?.key;
      if (starKey && layout.starPositions.has(starKey)) {
        return placementForStarKey(starKey);
      }
    }
    const hostStarId = Number(planet.host_star_id);
    if (Number.isFinite(hostStarId) && starKeyByCoreId.has(hostStarId)) {
      return placementForStarKey(starKeyByCoreId.get(hostStarId));
    }
    if (Number.isFinite(hostStarId) && starCenterByCoreId.has(hostStarId)) {
      return { center: starCenterByCoreId.get(hostStarId), groupKeys: [] };
    }
    return { center: [0, 0, 0], groupKeys: [] };
  };
  const planetPlacements = displayPlanets.map((planet) => ({
    planet,
    placement: hostPlacementForPlanet(planet),
  }));
  const planetHostGroupCount = planetPlacements.filter(({ placement }) => placement.groupKeys?.length).length;
  const treeHostedPlanetCount = simulationTreeContext
    ? planetPlacements.filter(({ planet }) => {
      const hostKey = layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key;
      return Boolean(hostKey && simulationTreeContext.nodesByKey.has(`body:${hostKey}`));
    }).length
    : 0;
  const planetDisplayEccentricityCappedCount = displayPlanets.filter((planet) => planet.eccentricity_display_capped).length;
  const habitableZoneStars = applyHabitableZonePlaneAlignment(
    displayStars.filter((star) => habitableZoneBoundsAu(star)),
    planetPlacements,
    layout,
  );
  const habitableZoneMaxPlaneInclinationDeg = Math.max(
    0,
    ...habitableZoneStars.map((star) => Number(star.habitable_zone_plane_inclination_deg) || 0),
  );
  const sceneLabelCount = showLabels
    ? displayStars.length + planetPlacements.length + subsystems.length + (showHabitableZones ? habitableZoneStars.length : 0)
    : 0;
  const starClassStatusCounts = useMemo(() => {
    const counts = { source: 0, derived: 0, assumed: 0, missing: 0, unsafeSource: 0 };
    displayStars.forEach((star) => {
      const field = starClassProvenanceField(star);
      const status = String(field.status || "missing").toLowerCase();
      counts[status] = (counts[status] || 0) + 1;
      const spectralTypeField = fieldRecord(star?.fields, "spectral_type_raw");
      if (status === "source" && !spectralTypeField?.value) {
        counts.unsafeSource += 1;
      }
    });
    return counts;
  }, [displayStars]);

  return (
    <group>
      <SceneMotionMetrics
        directOrbitCount={binaryOrbits.length}
        groupOrbitCount={groupOrbits.length}
        subsystemMarkerCount={subsystems.length}
        starCount={displayStars.length}
        planetCount={planetPlacements.length}
        planetOrbitCount={showOrbits ? planetPlacements.length : 0}
        planetTrailCount={planetPlacements.length}
        planetDisplayEccentricityCappedCount={planetDisplayEccentricityCappedCount}
        trueOrbitScaleSampleCount={trueOrbitDiagnostics.count}
        trueOrbitScaleMaxRelativeError={trueOrbitDiagnostics.maxRelativeError}
        habitableZoneCount={showHabitableZones ? habitableZoneStars.length : 0}
        habitableZoneMaxPlaneInclinationDeg={showHabitableZones ? habitableZoneMaxPlaneInclinationDeg : 0}
        starClassStatusCounts={starClassStatusCounts}
        scaleMode={activeScaleMode}
        collisionAdjustedStarCount={collisionScale.adjustedCount}
        minStarClearance={collisionScale.minClearance}
        minStarSeparation={collisionScale.minSeparation}
        groupMotionSpecs={groupMotionSpecs}
        simulationTree={simulationTree}
        useSimulationTree={useSimulationTree}
        planetHostGroupCount={planetHostGroupCount}
        treeHostedPlanetCount={treeHostedPlanetCount}
        labelCount={sceneLabelCount}
        simClockRef={simClockRef}
        running={running}
        speedMultiplier={speedMultiplier}
        onClockSample={onClockSample}
      />
      <ambientLight intensity={0.7} />
      <pointLight position={[0, 0, 0]} intensity={2.5} distance={26} />
      {showHabitableZones && habitableZoneStars.map((star) => {
        const starKey = star.render_key || star.key;
        return (
          <HabitableZoneBand
            key={`${starKey}:hz`}
            star={star}
            center={layout.starPositions.get(starKey) || [0, 0, 0]}
            maxOrbit={maxOrbit}
            visualScale={visualScale}
            scaleMode={activeScaleMode}
            groupKeys={groupKeysForStarKeys([starKey], layout)}
            groupMotionSpecs={groupMotionSpecs}
            layout={layout}
            treeContext={simulationTreeContext}
            treeHostBodyKey={starKey}
            simClockRef={simClockRef}
            showLabels={showLabels}
            selectedObjectId={selectedObjectId}
            onHover={onHover}
            onSelect={onSelect}
          />
        );
      })}
      {useSimulationTree && (
        <SimulationTreeObjects
          simulationTree={simulationTree}
          stars={displayStars}
          subsystems={subsystems}
          renderOrbits={renderOrbits}
          starsByKey={starsByKey}
          visualScale={visualScale}
          scaleMode={activeScaleMode}
          simClockRef={simClockRef}
          showOrbits={showOrbits}
          showLabels={showLabels}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      )}
      {!useSimulationTree && binaryOrbits.map((orbit, idx) => (
        <BinaryOrbit
          key={orbit.orbit_key || idx}
          orbit={orbit}
          starsByKey={starsByKey}
          layout={layout}
          groupMotionSpecs={groupMotionSpecs}
          visualScale={visualScale}
          scaleMode={activeScaleMode}
          center={layout.orbitCenters.get(orbit.orbit_key || `orbit-${idx}`) || [0, 0, 0]}
          simClockRef={simClockRef}
          running={running}
          speedMultiplier={speedMultiplier}
          showOrbits={showOrbits}
          showLabels={showLabels}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {!useSimulationTree && looseStars.map((star) => (
        <AnimatedStarSphere
          key={star.render_key || star.key}
          star={star}
          position={layout.starPositions.get(star.render_key || star.key) || [0, 0, 0]}
          groupKeys={groupKeysForStarKeys([star.render_key || star.key], layout)}
          groupMotionSpecs={groupMotionSpecs}
          layout={layout}
          simClockRef={simClockRef}
          running={running}
          speedMultiplier={speedMultiplier}
          showLabels={showLabels}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {!useSimulationTree && groupOrbits.map((orbit, idx) => (
        <GroupOrbitGuide
          key={orbit.orbit_key || `group-orbit-${idx}`}
          orbit={orbit}
          layout={layout}
          starsByKey={starsByKey}
          groupMotionSpecs={groupMotionSpecs}
          visualScale={visualScale}
          scaleMode={activeScaleMode}
          simClockRef={simClockRef}
          running={running}
          speedMultiplier={speedMultiplier}
          showOrbits={showOrbits}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {!useSimulationTree && subsystems.map((subsystem) => {
        const childKeys = subsystem.child_body_keys || [];
        const center = centerForBodyKeys(childKeys, layout, starsByKey);
        if (!center) {
          return null;
        }
        const groupKeys = groupKeysForOrbitSide(subsystem.render_key || subsystem.key, childKeys, layout);
        return (
          <SubsystemMarker
            key={subsystem.render_key || subsystem.key}
            subsystem={subsystem}
            center={center}
            groupKeys={groupKeys}
            groupMotionSpecs={groupMotionSpecs}
            layout={layout}
            simClockRef={simClockRef}
            running={running}
            speedMultiplier={speedMultiplier}
            showLabels={showLabels}
            selectedObjectId={selectedObjectId}
            onHover={onHover}
            onSelect={onSelect}
          />
        );
      })}
      {planetPlacements.map(({ planet, placement }, idx) => {
        const orbitRadius = Number(planet.orbit_radius_scene) || scaledPlanetOrbitRadius(planet.orbitAu, maxOrbit, visualScale, activeScaleMode);
        const color = PLANET_COLORS[idx % PLANET_COLORS.length];
        return (
          <React.Fragment key={planet.key}>
            {showOrbits && (
              <PlanetOrbitRing
                planet={planet}
                orbitRadius={orbitRadius}
                center={placement.center}
                motionGroupKeys={placement.groupKeys}
                groupMotionSpecs={groupMotionSpecs}
                layout={layout}
                treeContext={simulationTreeContext}
                treeHostBodyKey={layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key}
                simClockRef={simClockRef}
                running={running}
                speedMultiplier={speedMultiplier}
                selectedObjectId={selectedObjectId}
                onHover={onHover}
                onSelect={onSelect}
              />
            )}
            <PlanetOrbitTrail
              planet={planet}
              orbitRadius={orbitRadius}
              center={placement.center}
              motionGroupKeys={placement.groupKeys}
              groupMotionSpecs={groupMotionSpecs}
              layout={layout}
              treeContext={simulationTreeContext}
              treeHostBodyKey={layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key}
              simClockRef={simClockRef}
              scaleMode={activeScaleMode}
              color={color}
            />
            <PlanetObject
              planet={planet}
              orbitRadius={orbitRadius}
              center={placement.center}
              motionGroupKeys={placement.groupKeys}
              groupMotionSpecs={groupMotionSpecs}
              layout={layout}
              treeContext={simulationTreeContext}
              treeHostBodyKey={layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key}
              simClockRef={simClockRef}
              color={color}
              showLabels={showLabels}
              running={running}
              speedMultiplier={speedMultiplier}
              selectedObjectId={selectedObjectId}
              onHover={onHover}
              onSelect={onSelect}
            />
          </React.Fragment>
        );
      })}
    </group>
  );
}

function SceneCanvas({ scene, scaleMode = "structure", running = true, speedMultiplier = 1, resetToken = 0, showOrbits = true, showHabitableZones = true, showLabels = true, selectedObjectId = "", transparentBackground = false, onHover, onSelect, onClockSample }) {
  const visualScale = useMemo(() => mergeVisualScale(scene?.render_scene?.visual_scale), [scene]);
  const activeScaleMode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  const renderOrbits = useMemo(() => scene?.render_scene?.orbits || [], [scene]);
  const simulationTree = useMemo(() => scene?.render_scene?.simulation_tree || null, [scene]);
  const stars = useMemo(() => {
    const renderStars = scene?.render_scene?.bodies?.stars || [];
    if (renderStars.length) {
      return renderStars.map((star) => ({
        ...star,
        visualScale,
        display_radius_scene: scaledStarRadius(numericField(star.fields, "radius_rsun"), visualScale, activeScaleMode),
        visual_scale_mode: activeScaleMode,
      }));
    }
    const readinessStars = scene?.simulation_readiness?.stars || [];
    const bodyStars = scene?.bodies?.stars || [];
    return (readinessStars.length ? readinessStars : bodyStars).map((star, idx) => {
      const fields = star.fields || [];
      const teffK = numericField(fields, "teff_k") || Number(star.teff_k || 0);
      return {
        key: star.stable_object_key || star.object_id || star.star_id || `star-${idx}`,
        name: star.display_name || star.star_name || `Star ${idx + 1}`,
        radiusRsun: numericField(fields, "radius_rsun") || Number(star.radius_rsun || 0.55),
        display_radius_scene: scaledStarRadius(numericField(fields, "radius_rsun") || Number(star.radius_rsun || 0.55), visualScale, activeScaleMode),
        visual_scale_mode: activeScaleMode,
        visualScale,
        teffK,
        color: starColor(teffK),
      };
    });
  }, [scene, visualScale, activeScaleMode]);

  const planets = useMemo(() => {
    const renderScene = scene?.render_scene;
    const renderPlanets = renderScene?.bodies?.planets || [];
    if (renderPlanets.length) {
      return renderPlanets.map((planet, idx) => ({
        ...planet,
        key: planet.render_key || planet.stable_object_key || `planet-${idx}`,
        orbitAu: numericField(planet.fields, "semi_major_axis_au") || 0.08 + idx * 0.08,
        radius: scaledPlanetRadius(numericField(planet.fields, "radius_earth"), visualScale, activeScaleMode),
        pick_radius_scene: Math.max(scaledPlanetRadius(numericField(planet.fields, "radius_earth"), visualScale, activeScaleMode) * 2.1, 0.2),
      }));
    }
    const readinessPlanets = scene?.simulation_readiness?.planets || [];
    const bodyPlanets = scene?.bodies?.planets || [];
    return (readinessPlanets.length ? readinessPlanets : bodyPlanets).map((planet, idx) => {
      const fields = planet.fields || [];
      return {
        key: planet.stable_object_key || planet.object_id || planet.planet_id || `planet-${idx}`,
        name: planet.display_name || planet.planet_name || `Planet ${idx + 1}`,
        orbitAu: numericField(fields, "semi_major_axis_au") || Number(planet.semi_major_axis_au || 0.08 + idx * 0.08),
        periodDays: numericField(fields, "orbital_period_days") || Number(planet.orbital_period_days || 0),
        eccentricity: numericField(fields, "eccentricity") || Number(planet.eccentricity || 0),
        phaseRad: hashAngle(`${planet.stable_object_key || planet.object_id || planet.planet_id || planet.planet_name || idx}:phase`),
        radius: scaledPlanetRadius(numericField(fields, "radius_earth") || Number(planet.radius_earth || 1), visualScale, activeScaleMode),
        radiusEarth: numericField(fields, "radius_earth") || Number(planet.radius_earth || 1),
        orbitStatus: fieldStatus(fields, "semi_major_axis_au"),
      };
    });
  }, [scene, visualScale, activeScaleMode]);

  const subsystems = useMemo(() => (
    (scene?.render_scene?.bodies?.subsystems || []).map((subsystem, idx) => ({
      ...subsystem,
      key: subsystem.render_key || subsystem.source?.stable_component_key || `subsystem-${idx}`,
    }))
  ), [scene]);

  return (
    <Canvas
      camera={{ position: [0, 6.2, 10.8], fov: 43 }}
      dpr={[1, 1.75]}
      gl={{ antialias: true, alpha: transparentBackground, preserveDrawingBuffer: true, powerPreference: "high-performance" }}
    >
      {!transparentBackground && <color attach="background" args={["#050b12"]} />}
      <CameraControls resetToken={resetToken} />
      <CanvasHoverRaycaster onHover={onHover} />
      <PreviewObjects
        stars={stars}
        planets={planets}
        subsystems={subsystems}
        renderOrbits={renderOrbits}
        simulationTree={simulationTree}
        hierarchy={scene?.hierarchy}
        visualScale={visualScale}
        scaleMode={activeScaleMode}
        running={running}
        speedMultiplier={speedMultiplier}
        resetToken={resetToken}
        showOrbits={showOrbits}
        showHabitableZones={showHabitableZones}
        showLabels={showLabels}
        selectedObjectId={selectedObjectId}
        onHover={onHover}
        onSelect={onSelect}
        onClockSample={onClockSample}
      />
    </Canvas>
  );
}

function HoverReadout({ object }) {
  if (!object) {
    return null;
  }
  return (
    <div className="system-preview-hover" role="status" aria-live="polite">
      <div>
        <strong>{object.name}</strong>
        <span>{object.kind}</span>
      </div>
      <dl>
        {object.rows.map(([label, value, status, field]) => (
          <React.Fragment key={label}>
            <dt>{label}</dt>
            <dd>
              <span>{value}</span>
              <EvidencePill field={field} fallbackStatus={String(status || "missing").toLowerCase()} />
            </dd>
          </React.Fragment>
        ))}
      </dl>
    </div>
  );
}

function PinnedReadout({ object, onClose }) {
  const [copied, setCopied] = useState(false);
  if (!object) {
    return null;
  }
  const copyId = () => {
    if (!object.id) {
      return;
    }
    copyTextToClipboard(object.id).then((ok) => {
      if (!ok) {
        return;
      }
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    }).catch(() => {});
  };
  return (
    <div className="system-preview-pinned" data-testid="system-preview-pinned">
      <div className="system-preview-pinned-title">
        <div>
          <strong>{object.name}</strong>
          <span>{object.kind} - {String(object.sourceLayer || "unknown").toUpperCase()}</span>
        </div>
        <button type="button" onClick={onClose} aria-label="Close pinned simulator readout">x</button>
      </div>
      {object.id ? (
        <button
          className="system-preview-id-copy"
          type="button"
          onClick={copyId}
          title={String(object.id)}
          data-testid="system-preview-id-copy"
          data-full-id={String(object.id)}
          aria-label={`Copy ${object.kind} identifier ${object.id}`}
        >
          <span>{compactIdentifier(object.id)}</span>
          <em>{copied ? "Copied" : "Copy"}</em>
        </button>
      ) : null}
      <dl>
        {object.rows.map(([label, value, status, field]) => (
          <React.Fragment key={label}>
            <dt>{label}</dt>
            <dd>
              <span>{value}</span>
              <EvidencePill field={field} fallbackStatus={String(status || "missing").toLowerCase()} />
            </dd>
          </React.Fragment>
        ))}
      </dl>
    </div>
  );
}

function collectEvidenceFields(scene) {
  const renderScene = scene?.render_scene || {};
  const items = [];
  const firstOrbit = renderScene.orbits?.[0];
  if (firstOrbit?.fields) {
    items.push(["Binary period", fieldRecord(firstOrbit.fields, "period_days")]);
    items.push(["Binary eccentricity", fieldRecord(firstOrbit.fields, "eccentricity")]);
  }
  const firstPlanet = renderScene.bodies?.planets?.[0];
  if (firstPlanet?.fields) {
    items.push(["Planet class", planetVisualKindField(firstPlanet)]);
    items.push(["Planet period", fieldRecord(firstPlanet.fields, "orbital_period_days")]);
    items.push(["Planet phase", fieldRecord(firstPlanet.fields, "phase_rad")]);
  }
  const firstStar = renderScene.bodies?.stars?.[0];
  if (firstStar?.fields) {
    items.push(["Star radius", fieldRecord(firstStar.fields, "radius_rsun")]);
  }
  return items.filter(([, field]) => field).slice(0, 5);
}

function compactPolicyLabel(value, fallback = "Unknown") {
  const text = String(value || fallback);
  return text.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function orientationSummary(scene) {
  const renderOrbits = scene?.render_scene?.orbits || [];
  const fields = renderOrbits.flatMap((orbit) => Object.values(orbit?.fields || {}).filter(Boolean));
  const inclinationFields = fields.filter((field) => field?.key === "inclination_deg" && field.value !== null && field.value !== undefined);
  const nodeFields = fields.filter((field) => (
    ["longitude_ascending_node_deg", "node_deg"].includes(field?.key)
    && field.value !== null
    && field.value !== undefined
  ));
  const hasSourceNode = nodeFields.some((field) => String(field.status || "").toLowerCase() === "source");
  const hasSourceInclination = inclinationFields.some((field) => String(field.status || "").toLowerCase() === "source");
  const hasAssumedInclination = inclinationFields.some((field) => String(field.status || "").toLowerCase() === "assumed");
  if (hasSourceNode && hasSourceInclination) {
    return {
      label: "SOURCE ORIENTATION",
      detail: "Orbit orientation has source inclination and node-like orientation evidence.",
    };
  }
  if (inclinationFields.length > 0) {
    return {
      label: hasAssumedInclination ? "ASSUMED ROLL" : "PARTIAL SKY-PLANE",
      detail: hasAssumedInclination
        ? "Some orbit planes use deterministic visual assumptions; missing roll is not catalog fact."
        : "Inclination is present, but full 3D roll may be unknown without longitude of ascending node.",
    };
  }
  return {
    label: "LOCAL CLARITY",
    detail: "No full source orientation is available; the renderer uses a readability-first local frame.",
  };
}

function renderPolicyItems(scene, simulationDays = 0, speedMultiplier = 1, scaleMode = "structure") {
  const renderScene = scene?.render_scene || {};
  const visualScale = renderScene.visual_scale || {};
  const timePolicy = renderScene.time || {};
  const assumptionCount = Number(renderScene.assumption_count || 0);
  const persistedAssumptionCount = Number(renderScene.persisted_assumption_count || 0);
  const preferred = compactPolicyLabel(renderScene.preferred_visualization || "live_3d");
  const fallback = compactPolicyLabel(renderScene.fallback_visualization || "deterministic_snapshot");
  const activeScaleMode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  const scale = `${scaleModeLabel(activeScaleMode)} Scale`;
  const orientation = orientationSummary(scene);
  const assumptionText = assumptionCount > 0
    ? `${formatNumber(persistedAssumptionCount, 0)}/${formatNumber(assumptionCount, 0)} persisted`
    : "No assumptions";
  return [
    {
      key: "time",
      label: "Time",
      value: `Local beta day ${formatNumber(simulationDays, 1)} @ ${formatNumber(speedMultiplier, 2)}x`,
      detail: timePolicy.phase_policy || "Local teaching-grade clock; not science-grade epoch propagation.",
    },
    {
      key: "scale",
      label: "Scale",
      value: scale,
      detail: `${scaleModeDetail(activeScaleMode)} ${visualScale.policy_note || "Presentation scale for readability; physical values remain in source fields."}`,
    },
    {
      key: "assumptions",
      label: "Assumptions",
      value: assumptionText,
      detail: renderScene.assumption_generator_version || "No renderer assumption generator reported.",
    },
    {
      key: "orientation",
      label: "Orientation",
      value: orientation.label,
      detail: orientation.detail,
    },
    {
      key: "fallback",
      label: "Fallback",
      value: `${preferred} / ${fallback}`,
      detail: "Live 3D is preferred when capable; deterministic snapshots remain the fallback artifact.",
    },
  ];
}

function hasUsableWebGL() {
  if (typeof document === "undefined") {
    return false;
  }
  try {
    const canvas = document.createElement("canvas");
    return Boolean(canvas.getContext("webgl2") || canvas.getContext("webgl") || canvas.getContext("experimental-webgl"));
  } catch {
    return false;
  }
}

function SnapshotFallbackVisual({ snapshot, systemName, reason = "Preview unavailable" }) {
  const snapshotUrl = snapshot?.url ? apiUrl(snapshot.url) : "";
  return (
    <div className="system-preview-snapshot-fallback" data-testid="system-preview-snapshot-fallback">
      {snapshotUrl ? (
        <img src={snapshotUrl} alt={`${systemName || "System"} deterministic snapshot fallback`} />
      ) : (
        <div className="system-preview-fallback">Snapshot fallback pending</div>
      )}
      <div>
        <strong>{reason}</strong>
        <span>Deterministic snapshot fallback</span>
      </div>
    </div>
  );
}

export default function SystemPreviewPanel({ systemId, systemName, snapshot = null, presentationMode = "detail" }) {
  const [scene, setScene] = useState(null);
  const [status, setStatus] = useState("loading");
  const [webglReady, setWebglReady] = useState(null);
  const [running, setRunning] = useState(true);
  const [speedMultiplier, setSpeedMultiplier] = useState(1);
  const [resetToken, setResetToken] = useState(0);
  const [showOrbits, setShowOrbits] = useState(true);
  const [showHabitableZones, setShowHabitableZones] = useState(true);
  const [showLabels, setShowLabels] = useState(true);
  const [scaleMode, setScaleMode] = useState("structure");
  const [hoveredObject, setHoveredObject] = useState(null);
  const [pinnedObject, setPinnedObject] = useState(null);
  const [simulationDays, setSimulationDays] = useState(0);
  const handleClockSample = useCallback((days) => {
    setSimulationDays(Number.isFinite(Number(days)) ? Number(days) : 0);
  }, []);

  useEffect(() => {
    let active = true;
    const canRenderWebGL = hasUsableWebGL();
    setWebglReady(canRenderWebGL);
    setStatus("loading");
    setScene(null);
    setHoveredObject(null);
    setPinnedObject(null);
    setSimulationDays(0);
    if (!canRenderWebGL) {
      setStatus("fallback");
      return () => {
        active = false;
      };
    }
    fetchSystemSimulationScene(systemId)
      .then((payload) => {
        if (active) {
          setScene(payload);
          setStatus("ready");
        }
      })
      .catch(() => {
        if (active) {
          setStatus("error");
        }
      });
    return () => {
      active = false;
    };
  }, [systemId]);

  const readiness = scene?.simulation_readiness || {};
  const counts = readiness.counts || {};
  const bodies = scene?.bodies || {};
  const renderScene = scene?.render_scene || {};
  const renderBodies = renderScene.bodies || {};
  const renderOrbits = renderScene.orbits || [];
  const visualScale = renderScene.visual_scale || {};
  const evidenceFields = collectEvidenceFields(scene);
  const planetReadiness = scene?.simulation_readiness?.planets || [];
  const assumedOrbitCount = planetReadiness.filter((planet) => fieldStatus(planet.fields, "semi_major_axis_au") === "assumed").length;
  const missingOrbitCount = planetReadiness.filter((planet) => fieldStatus(planet.fields, "semi_major_axis_au") === "missing").length;
  const renderedAssumptionCount = Number.isFinite(Number(renderScene.assumption_count))
    ? Number(renderScene.assumption_count)
    : (counts.assumed || 0) + assumedOrbitCount;
  const activeScaleMode = normalizeScaleMode(scaleMode || visualScale.default_scale_mode || visualScale.scale_mode);
  const policyItems = renderPolicyItems(scene, simulationDays, speedMultiplier, activeScaleMode);
  const orientation = orientationSummary(scene);

  const normalizedPresentationMode = ["detail", "peek", "explore"].includes(presentationMode) ? presentationMode : "detail";
  const compactPresentation = normalizedPresentationMode === "peek";

  return (
    <section
      className={`panel system-preview-panel system-preview-${normalizedPresentationMode}`}
      data-testid="system-preview-panel"
      data-presentation-mode={normalizedPresentationMode}
    >
      <div className="system-preview-header">
        <div>
          <h3>System Simulation v1</h3>
          <p>Source-aware system renderer from the simulation-scene contract. Static snapshot remains the fallback.</p>
        </div>
        <div className="system-preview-actions">
          <button
            className="system-preview-toggle"
            type="button"
            onClick={() => setRunning((value) => !value)}
            aria-pressed={!running}
            disabled={status !== "ready" || webglReady === false}
          >
            {running ? "Pause" : "Start"}
          </button>
          <label className="system-preview-speed">
            <span>Speed</span>
            <select
              value={String(speedMultiplier)}
              onChange={(event) => setSpeedMultiplier(Number(event.target.value) || 1)}
              disabled={status !== "ready" || webglReady === false}
            >
              {SIM_SPEED_OPTIONS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
            </select>
          </label>
          <label className="system-preview-scale">
            <span>Scale</span>
            <select
              value={activeScaleMode}
              onChange={(event) => setScaleMode(normalizeScaleMode(event.target.value))}
              disabled={status !== "ready" || webglReady === false}
              data-testid="system-preview-scale-mode"
              aria-label="System simulator scale mode"
            >
              {SCALE_MODE_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <button
            className="system-preview-toggle"
            type="button"
            onClick={() => {
              setSimulationDays(0);
              setResetToken((value) => value + 1);
            }}
            disabled={status !== "ready" || webglReady === false}
          >
            Reset
          </button>
          <button
            className="system-preview-toggle"
            type="button"
            onClick={() => setShowOrbits((value) => !value)}
            aria-pressed={showOrbits}
            disabled={status !== "ready" || webglReady === false}
          >
            {showOrbits ? "Orbits On" : "Orbits Off"}
          </button>
          <button
            className="system-preview-toggle"
            type="button"
            onClick={() => setShowHabitableZones((value) => !value)}
            aria-pressed={showHabitableZones}
            disabled={status !== "ready" || webglReady === false}
          >
            {showHabitableZones ? "HZ On" : "HZ Off"}
          </button>
          <button
            className="system-preview-toggle"
            type="button"
            onClick={() => setShowLabels((value) => !value)}
            aria-pressed={showLabels}
            disabled={status !== "ready" || webglReady === false}
          >
            {showLabels ? "Labels On" : "Labels Off"}
          </button>
          {status === "ready" && scene && <span className="status-chip" title={orientation.detail}>{orientation.label}</span>}
          {renderScene?.schema_version ? <span className="status-chip">{renderScene.schema_version}</span> : (scene?.schema_version && <span className="status-chip">{scene.schema_version}</span>)}
        </div>
      </div>
      <div className="system-preview-layout">
        <div className="system-preview-canvas" aria-label={`${systemName} System Simulation`}>
          {status === "fallback" || webglReady === false
            ? <SnapshotFallbackVisual snapshot={snapshot} systemName={systemName} reason="WebGL unavailable" />
            : status === "ready" && scene
            ? (
              <SceneCanvas
                scene={scene}
                scaleMode={activeScaleMode}
                running={running}
                speedMultiplier={speedMultiplier}
                resetToken={resetToken}
                showOrbits={showOrbits}
                showHabitableZones={showHabitableZones}
                showLabels={showLabels}
                selectedObjectId={payloadId(pinnedObject)}
                transparentBackground={normalizedPresentationMode !== "detail"}
                onHover={setHoveredObject}
                onSelect={setPinnedObject}
                onClockSample={handleClockSample}
              />
            )
            : (status === "error"
              ? <SnapshotFallbackVisual snapshot={snapshot} systemName={systemName} reason="Live preview unavailable" />
              : <div className="system-preview-fallback">Loading preview...</div>)}
          <HoverReadout object={hoveredObject && !pinnedObject ? hoveredObject : null} />
          <PinnedReadout object={pinnedObject} onClose={() => setPinnedObject(null)} />
        </div>
        <div className="system-preview-readout">
          <div>
            <strong>{formatNumber(renderBodies.stars?.length || bodies.stars?.length, 0)}</strong>
            <span>rendered stars</span>
          </div>
          <div>
            <strong>{formatNumber(renderBodies.planets?.length || bodies.planets?.length, 0)}</strong>
            <span>rendered planets</span>
          </div>
          <div>
            <strong>{formatNumber(renderBodies.subsystems?.length || 0, 0)}</strong>
            <span>rendered subsystems</span>
          </div>
          <div>
            <strong>{formatNumber(renderOrbits.length, 0)}</strong>
            <span>rendered orbits</span>
          </div>
          <div data-testid="system-preview-clock">
            <strong>{formatNumber(simulationDays, 1)}</strong>
            <span>local days</span>
          </div>
          <div>
            <strong>{formatNumber((readiness.score || 0) * 100, 0)}%</strong>
            <span>readiness</span>
          </div>
          <div data-testid="system-preview-visual-scale">
            <strong>{scaleModeLabel(activeScaleMode)}</strong>
            <span>visual scale</span>
          </div>
          <div>
            <strong>{formatNumber(counts.source || 0, 0)}</strong>
            <span>source fields</span>
          </div>
          <div>
            <strong>{formatNumber(counts.derived || 0, 0)}</strong>
            <span>derived fields</span>
          </div>
          <div>
            <strong>{formatNumber(renderedAssumptionCount, 0)}</strong>
            <span>assumptions</span>
          </div>
          <div>
            <strong>{formatNumber((counts.missing || 0) + missingOrbitCount, 0)}</strong>
            <span>missing inputs</span>
          </div>
          {!compactPresentation && evidenceFields.length > 0 && (
            <div className="system-preview-evidence" data-testid="system-preview-evidence">
              <span>evidence</span>
              <ul>
                {evidenceFields.map(([label, field]) => (
                  <li key={`${label}:${field.key}`}>
                    <span>{label}</span>
                    <EvidencePill field={field} />
                  </li>
                ))}
              </ul>
            </div>
          )}
          {!compactPresentation && status === "ready" && scene && (
            <div className="system-preview-policy" data-testid="system-preview-policy">
              <span>render policy</span>
              <ul>
                {policyItems.map((item) => (
                  <li key={item.key}>
                    <strong>{item.label}</strong>
                    <span title={item.detail}>{item.value}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
