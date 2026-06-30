import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
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
];
const DEFAULT_VISUAL_SCALE = {
  schema_version: "visual_scale_beta_v1",
  scale_mode: "clarity_scaled_not_physical",
  star_radius: { fallback_rsun: 0.55, factor: 0.45, min_scene: 0.18, max_scene: 1.35 },
  planet_radius: { fallback_rearth: 1, factor: 0.085, min_scene: 0.105, max_scene: 0.34 },
  planet_orbit_radius: { fallback_au: 0.08, min_scene: 0.75, span_scene: 2.7 },
  binary_orbit_radius: { direct_pair_multiplier: 1, group_pair_motion_multiplier: 0.55 },
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

function numericField(fields, key) {
  const field = fieldRecord(fields, key);
  const value = Number(field?.value);
  return Number.isFinite(value) ? value : null;
}

function fieldStatus(fields, key) {
  const field = fieldRecord(fields, key);
  return field?.status || "missing";
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
  };
}

function scaledStarRadius(radiusRsun, visualScale = DEFAULT_VISUAL_SCALE) {
  const policy = visualScale.star_radius || DEFAULT_VISUAL_SCALE.star_radius;
  const radius = Number(radiusRsun);
  const source = Number.isFinite(radius) && radius > 0 ? radius : Number(policy.fallback_rsun || 0.55);
  return clampNumber(Math.sqrt(source) * Number(policy.factor || 0.45), Number(policy.min_scene || 0.18), Number(policy.max_scene || 1.35));
}

function scaledPlanetRadius(radiusEarth, visualScale = DEFAULT_VISUAL_SCALE) {
  const policy = visualScale.planet_radius || DEFAULT_VISUAL_SCALE.planet_radius;
  const radius = Number(radiusEarth);
  const source = Number.isFinite(radius) && radius > 0 ? radius : Number(policy.fallback_rearth || 1);
  return clampNumber(Math.sqrt(source) * Number(policy.factor || 0.085), Number(policy.min_scene || 0.105), Number(policy.max_scene || 0.34));
}

function scaledPlanetOrbitRadius(orbitAu, maxOrbitAu, visualScale = DEFAULT_VISUAL_SCALE) {
  const policy = visualScale.planet_orbit_radius || DEFAULT_VISUAL_SCALE.planet_orbit_radius;
  const orbit = Number(orbitAu);
  const maxOrbit = Math.max(Number(policy.fallback_au || 0.08), Number(maxOrbitAu) || Number(policy.fallback_au || 0.08));
  const source = Number.isFinite(orbit) && orbit > 0 ? orbit : Number(policy.fallback_au || 0.08);
  return Number(policy.min_scene || 0.75) + Math.sqrt(source / maxOrbit) * Number(policy.span_scene || 2.7);
}

function planetVisualKind(planet) {
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
        staticReadoutRow("Component", body.component || "Group", body.component ? "source" : "derived"),
        readoutRow(body.fields, "rendered_child_star_count", "Stars", "Unknown", 0),
        staticReadoutRow("Basis", body.source?.basis || "hierarchy", "derived"),
      ],
    };
  }
  if (kind === "star") {
    return {
      kind: "Star",
      name: body.display_name || body.name || "Unnamed star",
      id: body.render_key || body.stable_object_key || body.source?.stable_component_key || body.source?.stable_object_key || body.key || "",
      sourceLayer: body.source?.layer || "unknown",
      rows: [
        staticReadoutRow("Class", body.spectral_class || "Unknown", body.spectral_class ? "source" : "missing"),
        readoutRow(body.fields, "teff_k", "Temp", "Unknown", 0),
        readoutRow(body.fields, "mass_msun", "Mass", "Unknown", 3),
        readoutRow(body.fields, "radius_rsun", "Radius", "Unknown", 3),
      ],
    };
  }
  return {
    kind: "Planet",
    name: body.display_name || body.name || "Unnamed planet",
    id: body.render_key || body.stable_object_key || body.source?.stable_component_key || body.source?.stable_object_key || body.key || "",
    sourceLayer: body.source?.layer || "unknown",
    rows: [
      readoutRow(body.fields, "orbital_period_days", "Period", "Unknown", 3),
      readoutRow(body.fields, "semi_major_axis_au", "Orbit", "Unknown", 4),
      readoutRow(body.fields, "eccentricity", "Ecc.", "Unknown", 3),
      readoutRow(body.fields, "radius_earth", "Radius", "Unknown", 2),
    ],
  };
}

function orbitHoverPayload(orbit) {
  if (!orbit) {
    return null;
  }
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
    ],
  };
}

function orbitalPosition(phase, orbitRadius, eccentricity = 0, inclinationRad = 0) {
  const radiusScale = (1 - eccentricity ** 2) / (1 + eccentricity * Math.cos(phase));
  const x = Math.cos(phase) * orbitRadius * radiusScale;
  const planeZ = Math.sin(phase) * orbitRadius * radiusScale;
  return [x, -planeZ * Math.sin(inclinationRad), planeZ * Math.cos(inclinationRad)];
}

function sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, samples = 192) {
  const vertices = [];
  for (let idx = 0; idx < samples; idx += 1) {
    const phase = (idx / samples) * Math.PI * 2;
    vertices.push(...orbitalPosition(phase, orbitRadius, eccentricity, inclinationRad));
  }
  return new Float32Array(vertices);
}

function scaledOrbitPoints(points, scale) {
  return new Float32Array(Array.from(points, (value) => value * scale));
}

function binaryMassFractions(primary, secondary) {
  const primaryMass = numericField(primary?.fields, "mass_msun");
  const secondaryMass = numericField(secondary?.fields, "mass_msun");
  if (primaryMass && secondaryMass && primaryMass > 0 && secondaryMass > 0) {
    const total = primaryMass + secondaryMass;
    return {
      primary: secondaryMass / total,
      secondary: primaryMass / total,
      basis: "source_mass_ratio",
    };
  }
  return {
    primary: 0.5,
    secondary: 0.5,
    basis: "equal_mass_visual_fallback",
  };
}

function binaryTraceProvenanceField(massFractions) {
  const sourceMassRatio = massFractions?.basis === "source_mass_ratio";
  return {
    label: "Binary trace",
    value: sourceMassRatio ? "Mass-weighted barycentric" : "Equal-mass visual fallback",
    unit: null,
    status: sourceMassRatio ? "derived" : "assumed",
    layer: "render_scene",
    source_catalog: sourceMassRatio ? "core_star_mass_fields" : null,
    source_reference: null,
    basis: massFractions?.basis || "unknown",
    seed: null,
    generator_version: "system_preview_binary_trace_v1",
    confidence: sourceMassRatio ? 0.85 : 0.35,
    notes: sourceMassRatio
      ? "Rendered body paths use available stellar masses to split the visual relative orbit around the barycenter."
      : "Rendered body paths assume equal visual masses because one or both stellar masses are missing.",
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

function StarSphere({ star, position = [0, 0, 0], selectedObjectId = "", onHover, onSelect }) {
  const radiusRsun = numericField(star.fields, "radius_rsun") || Number(star.radiusRsun || 0.55);
  const radius = Number(star.display_radius_scene) || scaledStarRadius(radiusRsun, star.visualScale);
  const teffK = numericField(star.fields, "teff_k") || Number(star.teffK || 0);
  const color = teffK ? starColor(teffK) : (STAR_COLORS[String(star.spectral_class || "").slice(0, 1)] || "#ff9d6b");
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
        <meshStandardMaterial color={color} map={texture || null} emissive={color} emissiveIntensity={0.9} roughness={0.52} />
      </mesh>
      <mesh>
        <sphereGeometry args={[Math.max(radius * 1.75, radius + 0.18), 32, 20]} />
        <meshBasicMaterial color={color} transparent opacity={selected ? 0.24 : 0.16} depthWrite={false} blending={THREE.AdditiveBlending} />
      </mesh>
      {selected && <SelectionHalo radius={Math.max(radius * 1.82, radius + 0.28)} color="#fff2b7" pulse />}
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[Math.max(radius * 1.8, 0.34), 16, 12]} />
        <meshBasicMaterial transparent opacity={0} depthWrite={false} />
      </mesh>
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

function buildGroupMotionSpecs(groupOrbits, layout, visualScale = DEFAULT_VISUAL_SCALE) {
  const multiplier = Number(visualScale.binary_orbit_radius?.group_pair_motion_multiplier || 0.55);
  return (groupOrbits || [])
    .map((orbit) => {
      const primaryGroupKeys = groupKeysForOrbitSide(orbit.primary_body_key, orbit.primary_child_body_keys, layout);
      const secondaryGroupKeys = groupKeysForOrbitSide(orbit.secondary_body_key, orbit.secondary_child_body_keys, layout);
      const primarySet = new Set(primaryGroupKeys);
      const secondarySet = new Set(secondaryGroupKeys);
      const overlap = [...primarySet].some((key) => secondarySet.has(key));
      if (!primaryGroupKeys.length || !secondaryGroupKeys.length || overlap) {
        return null;
      }
      return {
        orbit,
        primaryGroupKeys,
        secondaryGroupKeys,
        primaryAncestorGroupKeys: [...new Set(primaryGroupKeys.flatMap((key) => layout.groupAncestorKeys.get(key) || []))],
        secondaryAncestorGroupKeys: [...new Set(secondaryGroupKeys.flatMap((key) => layout.groupAncestorKeys.get(key) || []))],
        periodDays: Math.max(0.05, numericField(orbit.fields, "period_days") || 80),
        phaseRad: numericField(orbit.fields, "phase_rad") || 0,
        eccentricity: Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0)),
        inclinationRad: THREE.MathUtils.degToRad(numericField(orbit.fields, "inclination_deg") || 0),
        orbitRadius: (Number(orbit.display_radius_scene) || 1.6) * multiplier,
      };
    })
    .filter(Boolean);
}

function directGroupOffsetAt(groupKey, groupMotionSpecs, simDays) {
  if (!groupKey || !groupMotionSpecs?.length) {
    return [0, 0, 0];
  }
  return groupMotionSpecs.reduce((offset, spec) => {
    const side = spec.primaryGroupKeys.includes(groupKey) ? -0.5 : (spec.secondaryGroupKeys.includes(groupKey) ? 0.5 : 0);
    if (!side) {
      return offset;
    }
    const phase = spec.phaseRad + (simDays / spec.periodDays) * Math.PI * 2;
    return addVector(offset, scaledVector(orbitalPosition(phase, spec.orbitRadius, spec.eccentricity, spec.inclinationRad), side));
  }, [0, 0, 0]);
}

function groupOffsetAt(groupKey, groupMotionSpecs, simDays, layout = null) {
  if (!groupKey) {
    return [0, 0, 0];
  }
  const ancestorKeys = layout?.groupAncestorKeys?.get(groupKey) || [];
  return sumVectors([
    directGroupOffsetAt(groupKey, groupMotionSpecs, simDays),
    ...ancestorKeys.map((ancestorKey) => directGroupOffsetAt(ancestorKey, groupMotionSpecs, simDays)),
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

function averageGroupOffsetAt(groupKeys, groupMotionSpecs, simDays, layout = null) {
  const uniqueKeys = [...new Set((groupKeys || []).filter(Boolean))];
  return averageVector(uniqueKeys.map((key) => groupOffsetAt(key, groupMotionSpecs, simDays, layout)));
}

function groupPairCenterOffsetAt(primaryGroupKeys, secondaryGroupKeys, groupMotionSpecs, simDays, layout = null) {
  return averageVector([
    averageGroupOffsetAt(primaryGroupKeys, groupMotionSpecs, simDays, layout),
    averageGroupOffsetAt(secondaryGroupKeys, groupMotionSpecs, simDays, layout),
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

function AnimatedStarSphere({ star, position = [0, 0, 0], groupKeys = [], groupMotionSpecs, layout, simClockRef, running = true, speedMultiplier = 1, selectedObjectId = "", onHover, onSelect }) {
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
      <StarSphere star={star} selectedObjectId={selectedObjectId} onHover={onHover} onSelect={onSelect} />
    </group>
  );
}

function BinaryOrbit({ orbit, starsByKey, layout, groupMotionSpecs, center = [0, 0, 0], simClockRef, running = true, speedMultiplier = 1, showOrbits = true, selectedObjectId = "", onHover, onSelect }) {
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
  const orbitRadius = Number(orbit.display_radius_scene) || 0.9;
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
    const relative = orbitalPosition(theta, orbitRadius, eccentricity, inclinationRad);
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
          <lineLoop {...orbitHandlers}>
            <bufferGeometry>
              <bufferAttribute attach="attributes-position" args={[primaryPathPoints, 3]} />
            </bufferGeometry>
            <lineBasicMaterial color={selected ? "#fff4c4" : "#ffdca8"} transparent opacity={selected ? 0.95 : 0.62} />
          </lineLoop>
          <lineLoop {...orbitHandlers}>
            <bufferGeometry>
              <bufferAttribute attach="attributes-position" args={[secondaryPathPoints, 3]} />
            </bufferGeometry>
            <lineBasicMaterial color={massFractions.basis === "source_mass_ratio" ? "#f6c971" : "#fff4c4"} transparent opacity={selected ? 0.72 : 0.34} />
          </lineLoop>
        </>
      )}
      <group ref={primaryRef}>
        <StarSphere star={primary} selectedObjectId={selectedObjectId} onHover={onHover} onSelect={onSelect} />
      </group>
      <group ref={secondaryRef}>
        <StarSphere star={secondary} selectedObjectId={selectedObjectId} onHover={onHover} onSelect={onSelect} />
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

function GroupOrbitGuide({ orbit, layout, starsByKey, groupMotionSpecs, simClockRef, running = true, speedMultiplier = 1, showOrbits = true, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const primaryCenter = centerForBodyKeys(orbit.primary_child_body_keys, layout, starsByKey);
  const secondaryCenter = centerForBodyKeys(orbit.secondary_child_body_keys, layout, starsByKey);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0));
  const inclinationDeg = numericField(orbit.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const orbitRadius = Number(orbit.display_radius_scene) || 1.6;
  const pathPoints = useMemo(() => sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, 224), [orbitRadius, eccentricity, inclinationRad]);
  const haloPathPoints = useMemo(() => sampledOrbitPoints(orbitRadius * 1.045, eccentricity, inclinationRad, 224), [orbitRadius, eccentricity, inclinationRad]);
  const payload = useMemo(() => orbitHoverPayload(orbit), [orbit]);
  const selected = Boolean(selectedObjectId && payloadId(payload) === selectedObjectId);
  const center = primaryCenter && secondaryCenter ? scaledVector(addVector(primaryCenter, secondaryCenter), 0.5) : [0, 0, 0];
  const primaryGroupKeys = groupKeysForOrbitSide(orbit.primary_body_key, orbit.primary_child_body_keys, layout);
  const secondaryGroupKeys = groupKeysForOrbitSide(orbit.secondary_body_key, orbit.secondary_child_body_keys, layout);

  useFrame(() => {
    if (!groupRef.current) {
      return;
    }
    const simDays = currentSimulationDays(simClockRef);
    groupRef.current.position.set(...addVector(center, groupPairCenterOffsetAt(primaryGroupKeys, secondaryGroupKeys, groupMotionSpecs, simDays, layout)));
  });

  if (!showOrbits || !primaryCenter || !secondaryCenter) {
    return null;
  }

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
    <group ref={groupRef} position={center} data-testid="system-preview-group-orbit-guide">
      <lineLoop {...handlers}>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" args={[haloPathPoints, 3]} />
        </bufferGeometry>
        <lineBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.32 : 0.14} />
      </lineLoop>
      <lineLoop {...handlers}>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" args={[pathPoints, 3]} />
        </bufferGeometry>
        <lineBasicMaterial color={selected ? "#fff4c4" : "#f0bf55"} transparent opacity={selected ? 0.88 : 0.44} />
      </lineLoop>
    </group>
  );
}

function SubsystemMarker({ subsystem, center = [0, 0, 0], groupKeys = [], groupMotionSpecs, layout, simClockRef, running = true, speedMultiplier = 1, selectedObjectId = "", onHover, onSelect }) {
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
      <mesh {...handlers} rotation={[Math.PI / 2, 0, 0]}>
        <torusGeometry args={[selected ? 0.25 : 0.19, selected ? 0.018 : 0.011, 8, 44]} />
        <meshBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.9 : 0.58} />
      </mesh>
      <mesh {...handlers} rotation={[0, Math.PI / 2, 0]}>
        <torusGeometry args={[selected ? 0.2 : 0.155, selected ? 0.014 : 0.008, 8, 36]} />
        <meshBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.5 : 0.26} />
      </mesh>
      <mesh {...handlers}>
        <sphereGeometry args={[selected ? 0.055 : 0.04, 12, 8]} />
        <meshBasicMaterial color={selected ? "#fff4c4" : "#7ddcff"} transparent opacity={selected ? 0.92 : 0.62} />
      </mesh>
    </group>
  );
}

function PlanetObject({ planet, orbitRadius, color, center = [0, 0, 0], motionGroupKeys = [], groupMotionSpecs, layout, simClockRef, running = true, speedMultiplier = 1, selectedObjectId = "", onHover, onSelect }) {
  const groupRef = React.useRef(null);
  const periodDays = Math.max(0.05, numericField(planet.fields, "orbital_period_days") || Number(planet.periodDays) || 8 + orbitRadius * 2.2);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(planet.fields, "eccentricity") || Number(planet.eccentricity) || 0));
  const phaseRad = numericField(planet.fields, "phase_rad") || Number(planet.phaseRad) || 0;
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const visualKind = planetVisualKind(planet);
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
    const movingCenter = addVector(center, combinedGroupOffsetAt(motionGroupKeys, groupMotionSpecs, simDays, layout));
    groupRef.current.position.set(...addVector(movingCenter, orbitalPosition(meanAnomaly, orbitRadius, eccentricity, inclinationRad)));
  });

  return (
    <group ref={groupRef} position={addVector(center, orbitalPosition(phaseRad, orbitRadius, eccentricity, inclinationRad))}>
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[planet.radius, 18, 14]} />
        <meshStandardMaterial color={color} map={texture || null} roughness={0.72} metalness={0.03} />
      </mesh>
      <mesh>
        <sphereGeometry args={[planet.radius * 1.08, 18, 14]} />
        <meshBasicMaterial color="#b7e2ff" transparent opacity={selected ? 0.2 : (visualKind === "gas_giant" ? 0.05 : 0.09)} depthWrite={false} blending={THREE.AdditiveBlending} />
      </mesh>
      {selected && <SelectionHalo radius={Math.max(planet.radius * 2.1, 0.22)} color="#b7e2ff" pulse />}
      <mesh {...hoverHandlers} userData={{ hoverPayload }}>
        <sphereGeometry args={[Math.max(planet.radius * 2.1, 0.2), 14, 10]} />
        <meshBasicMaterial transparent opacity={0} depthWrite={false} />
      </mesh>
    </group>
  );
}

function CanvasHoverRaycaster({ onHover }) {
  const { camera, gl, raycaster, scene } = useThree();
  const lastPayloadRef = React.useRef(null);

  useEffect(() => {
    const target = gl.domElement;
    const pointer = new THREE.Vector2();

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

function SceneMotionMetrics({ directOrbitCount = 0, groupOrbitCount = 0, subsystemMarkerCount = 0, groupMotionSpecs = [], planetHostGroupCount = 0, simClockRef, running = true, speedMultiplier = 1 }) {
  const { gl } = useThree();
  const nestedCount = useMemo(() => (
    (groupMotionSpecs || []).filter((spec) => (
      (spec.primaryAncestorGroupKeys || []).length > 0
      || (spec.secondaryAncestorGroupKeys || []).length > 0
    )).length
  ), [groupMotionSpecs]);

  useEffect(() => {
    gl.domElement.dataset.groupMotionCount = String(groupMotionSpecs?.length || 0);
    gl.domElement.dataset.nestedGroupMotionCount = String(nestedCount);
    gl.domElement.dataset.planetHostGroupCount = String(planetHostGroupCount || 0);
    gl.domElement.dataset.directOrbitGuideCount = String(directOrbitCount || 0);
    gl.domElement.dataset.directOrbitTraceCount = String((directOrbitCount || 0) * 2);
    gl.domElement.dataset.groupOrbitGuideCount = String(groupOrbitCount || 0);
    gl.domElement.dataset.subsystemMarkerCount = String(subsystemMarkerCount || 0);
    gl.domElement.dataset.simulationClockMode = "shared_local_beta";
    gl.domElement.dataset.simulationClockWriters = "1";
    gl.domElement.dataset.simulationRunning = running ? "true" : "false";
    gl.domElement.dataset.simulationSpeed = String(speedMultiplier || 1);
  }, [gl, directOrbitCount, groupMotionSpecs, groupOrbitCount, nestedCount, planetHostGroupCount, running, speedMultiplier, subsystemMarkerCount]);

  useFrame(({ clock }) => {
    if (!simClockRef?.current) {
      return;
    }
    const simDays = advanceSimulationDays(simClockRef.current, clock.elapsedTime, running, speedMultiplier);
    gl.domElement.dataset.simulationDays = simDays.toFixed(3);
  });

  return null;
}

function PlanetOrbitRing({ planet, orbitRadius, center = [0, 0, 0], motionGroupKeys = [], groupMotionSpecs, layout, simClockRef, running = true, speedMultiplier = 1, selectedObjectId = "", onHover, onSelect }) {
  const lineRef = React.useRef(null);
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(planet.fields, "eccentricity") || Number(planet.eccentricity) || 0));
  const pathPoints = useMemo(() => sampledOrbitPoints(orbitRadius, eccentricity, inclinationRad, 224), [orbitRadius, eccentricity, inclinationRad]);
  const payload = useMemo(() => ({
    kind: "Planet orbit",
    name: `${planet.display_name || planet.name || "Planet"} orbit`,
    id: `${planet.render_key || planet.key || "planet"}:orbit`,
    sourceLayer: planet.source?.layer || "core",
    rows: [
      readoutRow(planet.fields, "orbital_period_days", "Period", "Unknown", 3),
      readoutRow(planet.fields, "semi_major_axis_au", "Orbit", "Unknown", 4),
      readoutRow(planet.fields, "eccentricity", "Ecc.", "Unknown", 3),
      readoutRow(planet.fields, "inclination_deg", "Incl.", "Unknown", 2),
    ],
  }), [planet]);
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
    lineRef.current.position.set(...addVector(center, combinedGroupOffsetAt(motionGroupKeys, groupMotionSpecs, simDays, layout)));
  });

  return (
    <lineLoop ref={lineRef} position={center} {...handlers}>
      <bufferGeometry>
        <bufferAttribute attach="attributes-position" args={[pathPoints, 3]} />
      </bufferGeometry>
      <lineBasicMaterial color={selected ? "#e6f6ff" : "#b1d6ff"} transparent opacity={selected ? 0.9 : 0.5} />
    </lineLoop>
  );
}

function PreviewObjects({ stars, planets, subsystems = [], renderOrbits = [], hierarchy, visualScale = DEFAULT_VISUAL_SCALE, running = true, speedMultiplier = 1, resetToken = 0, showOrbits = true, selectedObjectId = "", onHover, onSelect }) {
  const binaryOrbits = renderOrbits.filter((orbit) => orbit.endpoint_kind !== "group_pair");
  const groupOrbits = renderOrbits.filter((orbit) => orbit.endpoint_kind === "group_pair");
  const layout = useMemo(() => buildStarLayout(stars, hierarchy, binaryOrbits), [stars, hierarchy, binaryOrbits]);
  const groupMotionSpecs = useMemo(() => buildGroupMotionSpecs(groupOrbits, layout, visualScale), [groupOrbits, layout, visualScale]);
  const simClockRef = React.useRef({ days: 0, lastElapsedSeconds: null });
  useEffect(() => {
    simClockRef.current = { days: 0, lastElapsedSeconds: null };
  }, [resetToken, stars, planets, renderOrbits]);
  const starsByKey = useMemo(() => {
    const out = new Map();
    stars.forEach((star) => {
      const canonicalKey = star.render_key || star.key;
      keyAliasesForBody(star).forEach((alias) => out.set(alias, star));
      if (canonicalKey) {
        out.set(canonicalKey, star);
      }
    });
    return out;
  }, [stars]);
  const looseStars = stars.filter((star) => !layout.orbitStarKeys.has(star.render_key || star.key));
  const starCenterByCoreId = new Map();
  const starKeyByCoreId = new Map();
  stars.forEach((star) => {
    const starId = star?.source?.star_id;
    const key = star.render_key || star.key;
    if (starId !== undefined && starId !== null && key && layout.starPositions.has(key)) {
      starCenterByCoreId.set(Number(starId), layout.starPositions.get(key));
      starKeyByCoreId.set(Number(starId), key);
    }
  });
  const maxOrbit = Math.max(
    0.1,
    ...planets.map((planet) => planet.orbitAu || 0.1),
  );
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
  const planetPlacements = planets.map((planet) => ({
    planet,
    placement: hostPlacementForPlanet(planet),
  }));
  const planetHostGroupCount = planetPlacements.filter(({ placement }) => placement.groupKeys?.length).length;

  return (
    <group>
      <SceneMotionMetrics
        directOrbitCount={binaryOrbits.length}
        groupOrbitCount={groupOrbits.length}
        subsystemMarkerCount={subsystems.length}
        groupMotionSpecs={groupMotionSpecs}
        planetHostGroupCount={planetHostGroupCount}
        simClockRef={simClockRef}
        running={running}
        speedMultiplier={speedMultiplier}
      />
      <ambientLight intensity={0.7} />
      <pointLight position={[0, 0, 0]} intensity={2.5} distance={26} />
      {binaryOrbits.map((orbit, idx) => (
        <BinaryOrbit
          key={orbit.orbit_key || idx}
          orbit={orbit}
          starsByKey={starsByKey}
          layout={layout}
          groupMotionSpecs={groupMotionSpecs}
          center={layout.orbitCenters.get(orbit.orbit_key || `orbit-${idx}`) || [0, 0, 0]}
          simClockRef={simClockRef}
          running={running}
          speedMultiplier={speedMultiplier}
          showOrbits={showOrbits}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {looseStars.map((star) => (
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
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {groupOrbits.map((orbit, idx) => (
        <GroupOrbitGuide
          key={orbit.orbit_key || `group-orbit-${idx}`}
          orbit={orbit}
          layout={layout}
          starsByKey={starsByKey}
          groupMotionSpecs={groupMotionSpecs}
          simClockRef={simClockRef}
          running={running}
          speedMultiplier={speedMultiplier}
          showOrbits={showOrbits}
          selectedObjectId={selectedObjectId}
          onHover={onHover}
          onSelect={onSelect}
        />
      ))}
      {subsystems.map((subsystem) => {
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
            selectedObjectId={selectedObjectId}
            onHover={onHover}
            onSelect={onSelect}
          />
        );
      })}
      {planetPlacements.map(({ planet, placement }, idx) => {
        const orbitRadius = scaledPlanetOrbitRadius(planet.orbitAu, maxOrbit, visualScale);
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
                simClockRef={simClockRef}
                running={running}
                speedMultiplier={speedMultiplier}
                selectedObjectId={selectedObjectId}
                onHover={onHover}
                onSelect={onSelect}
              />
            )}
            <PlanetObject
              planet={planet}
              orbitRadius={orbitRadius}
              center={placement.center}
              motionGroupKeys={placement.groupKeys}
              groupMotionSpecs={groupMotionSpecs}
              layout={layout}
              simClockRef={simClockRef}
              color={PLANET_COLORS[idx % PLANET_COLORS.length]}
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

function SceneCanvas({ scene, running = true, speedMultiplier = 1, resetToken = 0, showOrbits = true, selectedObjectId = "", onHover, onSelect }) {
  const visualScale = useMemo(() => mergeVisualScale(scene?.render_scene?.visual_scale), [scene]);
  const renderOrbits = useMemo(() => scene?.render_scene?.orbits || [], [scene]);
  const stars = useMemo(() => {
    const renderStars = scene?.render_scene?.bodies?.stars || [];
    if (renderStars.length) {
      return renderStars.map((star) => ({
        ...star,
        visualScale,
        display_radius_scene: Number(star.display_radius_scene) || scaledStarRadius(numericField(star.fields, "radius_rsun"), visualScale),
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
        display_radius_scene: scaledStarRadius(numericField(fields, "radius_rsun") || Number(star.radius_rsun || 0.55), visualScale),
        visualScale,
        teffK,
        color: starColor(teffK),
      };
    });
  }, [scene, visualScale]);

  const planets = useMemo(() => {
    const renderScene = scene?.render_scene;
    const renderPlanets = renderScene?.bodies?.planets || [];
    if (renderPlanets.length) {
      return renderPlanets.map((planet, idx) => ({
        ...planet,
        key: planet.render_key || planet.stable_object_key || `planet-${idx}`,
        orbitAu: numericField(planet.fields, "semi_major_axis_au") || 0.08 + idx * 0.08,
        radius: scaledPlanetRadius(numericField(planet.fields, "radius_earth"), visualScale),
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
        radius: scaledPlanetRadius(numericField(fields, "radius_earth") || Number(planet.radius_earth || 1), visualScale),
        radiusEarth: numericField(fields, "radius_earth") || Number(planet.radius_earth || 1),
        orbitStatus: fieldStatus(fields, "semi_major_axis_au"),
      };
    });
  }, [scene, visualScale]);

  const subsystems = useMemo(() => (
    (scene?.render_scene?.bodies?.subsystems || []).map((subsystem, idx) => ({
      ...subsystem,
      key: subsystem.render_key || subsystem.source?.stable_component_key || `subsystem-${idx}`,
    }))
  ), [scene]);

  return (
    <Canvas camera={{ position: [0, 6.2, 10.8], fov: 43 }} dpr={[1, 1.75]}>
      <color attach="background" args={["#050b12"]} />
      <CameraControls resetToken={resetToken} />
      <CanvasHoverRaycaster onHover={onHover} />
      <PreviewObjects
        stars={stars}
        planets={planets}
        subsystems={subsystems}
        renderOrbits={renderOrbits}
        hierarchy={scene?.hierarchy}
        visualScale={visualScale}
        running={running}
        speedMultiplier={speedMultiplier}
        resetToken={resetToken}
        showOrbits={showOrbits}
        selectedObjectId={selectedObjectId}
        onHover={onHover}
        onSelect={onSelect}
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
    items.push(["Planet period", fieldRecord(firstPlanet.fields, "orbital_period_days")]);
    items.push(["Planet phase", fieldRecord(firstPlanet.fields, "phase_rad")]);
  }
  const firstStar = renderScene.bodies?.stars?.[0];
  if (firstStar?.fields) {
    items.push(["Star radius", fieldRecord(firstStar.fields, "radius_rsun")]);
  }
  return items.filter(([, field]) => field).slice(0, 5);
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

export default function SystemPreviewPanel({ systemId, systemName, snapshot = null }) {
  const [scene, setScene] = useState(null);
  const [status, setStatus] = useState("loading");
  const [webglReady, setWebglReady] = useState(null);
  const [running, setRunning] = useState(true);
  const [speedMultiplier, setSpeedMultiplier] = useState(1);
  const [resetToken, setResetToken] = useState(0);
  const [showOrbits, setShowOrbits] = useState(true);
  const [hoveredObject, setHoveredObject] = useState(null);
  const [pinnedObject, setPinnedObject] = useState(null);

  useEffect(() => {
    let active = true;
    const canRenderWebGL = hasUsableWebGL();
    setWebglReady(canRenderWebGL);
    setStatus("loading");
    setScene(null);
    setHoveredObject(null);
    setPinnedObject(null);
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

  return (
    <section className="panel system-preview-panel" data-testid="system-preview-panel">
      <div className="system-preview-header">
        <div>
          <h3>Live System Preview</h3>
          <p>Beta renderer from the simulation-scene contract. Static snapshot remains the fallback.</p>
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
          <button
            className="system-preview-toggle"
            type="button"
            onClick={() => setResetToken((value) => value + 1)}
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
          {renderScene?.schema_version ? <span className="status-chip">{renderScene.schema_version}</span> : (scene?.schema_version && <span className="status-chip">{scene.schema_version}</span>)}
        </div>
      </div>
      <div className="system-preview-layout">
        <div className="system-preview-canvas" aria-label={`${systemName} live system preview`}>
          {status === "fallback" || webglReady === false
            ? <SnapshotFallbackVisual snapshot={snapshot} systemName={systemName} reason="WebGL unavailable" />
            : status === "ready" && scene
            ? (
              <SceneCanvas
                scene={scene}
                running={running}
                speedMultiplier={speedMultiplier}
                resetToken={resetToken}
                showOrbits={showOrbits}
                selectedObjectId={payloadId(pinnedObject)}
                onHover={setHoveredObject}
                onSelect={setPinnedObject}
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
          <div>
            <strong>{formatNumber((readiness.score || 0) * 100, 0)}%</strong>
            <span>readiness</span>
          </div>
          <div data-testid="system-preview-visual-scale">
            <strong>{visualScale.scale_mode === "clarity_scaled_not_physical" ? "Clarity" : "Beta"}</strong>
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
          {evidenceFields.length > 0 && (
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
        </div>
      </div>
    </section>
  );
}
