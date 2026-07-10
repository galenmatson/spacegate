import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import * as THREE from "three";
import { apiUrl, fetchMapSystems, fetchPublicConfig, fetchSystems } from "./api.js";
import { isLightweightPreviewSystem, LightweightSystemPreview } from "./LightweightSystemPreview.jsx";
import { readStoredMapReturnState, writeStoredMapReturnState } from "./mapReturnState.js";
import { NAME_STYLE_OPTIONS, normalizeNameStyle } from "./nameStyle.js";
import { StellarClassChips, stellarClassTokensFromSystem, stellarClassTooltip } from "./stellarClassTags.jsx";

const MAP_RADIUS_LY = 100;
const SystemPreviewPanel = React.lazy(() => import("./SystemPreviewPanel.jsx"));
const STAR_SEARCH_SPECTRAL_OPTIONS = ["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D"];
const STAR_SEARCH_DEFAULT_TEMP_RANGE = [0, 50000];
const STAR_SEARCH_SORT_OPTIONS = [
  { value: "match", label: "Best match" },
  { value: "distance", label: "Nearest view" },
  { value: "coolness", label: "Coolest" },
  { value: "name", label: "Name" },
];
const MAP_RADIUS_OPTIONS_LY = [100, 250, 500, 1000];
const WEBGL_CONTEXT_BUDGET = 6;
const SEARCH_PREVIEW_POOL_SIZE = 4;
const SEARCH_PREVIEW_SNAPSHOT_CACHE_LIMIT = 96;
const SEARCH_PREVIEW_ACTIVATION_INTERVAL_MS = 450;
const SEARCH_PREVIEW_CONTEXT_COOLDOWN_MS = 3500;
const SEARCH_PREVIEW_HIGH_RECOVERY_BUDGET = 1;
const RUNTIME_QUALITY_PROFILES = {
  high: { tier: "high", label: "High", mapDpr: [1, 1.75], previewDpr: [1, 1.5], cardBudget: 4 },
  balanced: { tier: "balanced", label: "Balanced", mapDpr: [1, 1.35], previewDpr: [0.9, 1.25], cardBudget: 3 },
  low: { tier: "low", label: "Low", mapDpr: [0.75, 1], previewDpr: [0.75, 1], cardBudget: 2 },
};
const LY_TO_SCENE = 0.55;
const WORLD_UP = new THREE.Vector3(0, 1, 0);
const SOL_SYSTEM_ID = 17788193;
const PUBLIC_CONFIG_FALLBACK = { site_name: "Coolstars", map_title: "Coolstars Map", spacegate_url: "/search" };
const MAP_UTILITY_LINKS = [
  { label: "HELP", href: "/help", title: "How to use Coolstars", external: false },
  { label: "ABT", href: "/about", title: "About this site", external: false },
  { label: "SPT", href: "https://github.com/sponsors/galenmatson", title: "Support this project", external: true },
  { label: "SRC", href: "https://github.com/galenmatson/spacegate", title: "Source code", external: true },
  { label: "DATA", href: "/data", title: "Source data", external: false },
];
const MAP_VISIBLE_UTILITY_LABELS = new Set(["HELP", "DATA"]);
const MAP_MENU_UTILITY_LABELS = new Set(["ABT", "SPT", "SRC"]);
const SYSTEM_SCALE_MODE_OPTIONS = [
  { value: "structure", label: "Structured" },
  { value: "true_orbits", label: "Orbit" },
  { value: "true_bodies", label: "Body" },
  { value: "log", label: "Log" },
];
const SYSTEM_SCALE_MODE_IDS = new Set(SYSTEM_SCALE_MODE_OPTIONS.map((option) => option.value));
const MAP_PEEK_SIZE_STORAGE_KEY = "spacegate.map.peekSize";
const MAP_KEYBIND_STORAGE_KEY = "spacegate.map.keybindScheme";
const MAP_FRAME_STORAGE_KEY = "spacegate.map.frame";
const MAP_DIRECTION_LABELS_STORAGE_KEY = "spacegate.map.directionLabels";
const MAP_FPS_OVERLAY_STORAGE_KEY = "spacegate.map.fpsOverlay";
const DEFAULT_MAP_PEEK_SIZE = { width: 675, height: 468 };
const DEFAULT_MAP_CAMERA_STATE = {
  position: [0, 3.5, 17],
  yaw: 0,
  pitch: -0.08,
};
const DEFAULT_MOBILE_FLIGHT_STATE = {
  forward: false,
  back: false,
  left: false,
  right: false,
  up: false,
  down: false,
};
const KEYBOARD_BASE_SPEED = 7;
const KEYBOARD_BOOST_SPEED = 18;
const MAP_KEYBIND_SCHEMES = {
  wasd: {
    id: "wasd",
    label: "WASD",
    forward: "w",
    back: "s",
    left: "a",
    right: "d",
    up: "q",
    down: "z",
    hint: "WASD fly · Q/Z vertical",
  },
  esdf: {
    id: "esdf",
    label: "ESDF",
    forward: "e",
    back: "d",
    left: "s",
    right: "f",
    up: "a",
    down: "z",
    hint: "ESDF fly · A/Z vertical",
  },
  num8456: {
    id: "num8456",
    label: "8456",
    forward: "numpad8",
    back: "numpad5",
    left: "numpad4",
    right: "numpad6",
    up: "numpad7",
    down: "numpad1",
    hint: "8456 fly · 7/1 vertical",
  },
};
const MAP_KEYBIND_OPTIONS = Object.values(MAP_KEYBIND_SCHEMES);
const TOUCH_LOOK_SENSITIVITY = 0.003;
const TOUCH_PINCH_SPEED = 0.018;
const TOUCH_PAN_SPEED = 0.012;
const MOUSE_LOOK_SENSITIVITY = 0.002;
const MOUSE_DRAG_TRANSLATE_SPEED = 0.026;
const MOUSE_WHEEL_FLY_SPEED = 1.25;
const MOUSE_WHEEL_TRUCK_SPEED = 0.9;
const MOUSE_ORBIT_SENSITIVITY = 0.006;
const MOUSE_ORBIT_PITCH_SENSITIVITY = 0.0045;
const SPECTRAL_COLORS = {
  O: "#74a9ff",
  B: "#9fc9ff",
  A: "#e8f1ff",
  F: "#fff4bf",
  G: "#ffd56d",
  K: "#ffab67",
  M: "#ff6e5e",
  L: "#d47b62",
  T: "#9b78d8",
  Y: "#68c7d8",
  D: "#d7e0ea",
  UNKNOWN: "#8b99b0",
};
const MAP_FRAME_OPTIONS = {
  icrs: { id: "icrs", label: "ICRS", detail: "Scene up = ICRS Z" },
  galactic: { id: "galactic", label: "Galactic", detail: "Scene up = Galactic North" },
};
const ICRS_TO_GALACTIC = [
  [-0.0548755604, -0.8734370902, -0.4838350155],
  [0.4941094279, -0.4448296300, 0.7469822445],
  [-0.8676661490, -0.1980763734, 0.4559837762],
];

function normalizeSystemScaleMode(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "clarity" || raw === "structured") {
    return "structure";
  }
  return SYSTEM_SCALE_MODE_IDS.has(raw) ? raw : "structure";
}

function formatNumber(value, digits = 1) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "Unknown";
  }
  return numeric.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function formatName(value) {
  const text = String(value || "").trim();
  return text || "Unnamed system";
}

function clampMapPeekSize(size) {
  const viewportWidth = typeof window === "undefined" ? 1440 : window.innerWidth || 1440;
  const viewportHeight = typeof window === "undefined" ? 900 : window.innerHeight || 900;
  const maxWidth = Math.max(360, viewportWidth - 336);
  const maxHeight = Math.max(280, viewportHeight - 128);
  const width = Number(size?.width);
  const height = Number(size?.height);
  return {
    width: Math.round(Math.min(Math.max(Number.isFinite(width) ? width : DEFAULT_MAP_PEEK_SIZE.width, 360), maxWidth)),
    height: Math.round(Math.min(Math.max(Number.isFinite(height) ? height : DEFAULT_MAP_PEEK_SIZE.height, 280), maxHeight)),
  };
}

function readStoredMapPeekSize() {
  if (typeof window === "undefined") {
    return DEFAULT_MAP_PEEK_SIZE;
  }
  try {
    const stored = window.sessionStorage.getItem(MAP_PEEK_SIZE_STORAGE_KEY);
    if (!stored) {
      return DEFAULT_MAP_PEEK_SIZE;
    }
    return clampMapPeekSize(JSON.parse(stored));
  } catch {
    return DEFAULT_MAP_PEEK_SIZE;
  }
}

function readStoredMapKeybindScheme() {
  if (typeof window === "undefined") {
    return "wasd";
  }
  try {
    const stored = window.localStorage.getItem(MAP_KEYBIND_STORAGE_KEY);
    return MAP_KEYBIND_SCHEMES[stored] ? stored : "wasd";
  } catch {
    return "wasd";
  }
}

function readStoredMapFrame() {
  if (typeof window === "undefined") {
    return "icrs";
  }
  try {
    const stored = window.localStorage.getItem(MAP_FRAME_STORAGE_KEY);
    return MAP_FRAME_OPTIONS[stored] ? stored : "icrs";
  } catch {
    return "icrs";
  }
}

function readStoredDirectionLabelsEnabled() {
  if (typeof window === "undefined") {
    return false;
  }
  try {
    return window.localStorage.getItem(MAP_DIRECTION_LABELS_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

function readStoredFpsOverlayEnabled() {
  if (typeof window === "undefined") {
    return false;
  }
  try {
    return window.localStorage.getItem(MAP_FPS_OVERLAY_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

function readDeviceRuntimeProfile() {
  if (typeof window === "undefined") {
    return { width: 1440, height: 900, dpr: 1, touch: false };
  }
  return {
    width: window.innerWidth || 1440,
    height: window.innerHeight || 900,
    dpr: window.devicePixelRatio || 1,
    touch: Boolean(window.matchMedia?.("(pointer: coarse)")?.matches),
  };
}

function runtimeQualityFor({ activeSurfaces = 1, contextLossRecoveries = 0, deviceProfile = readDeviceRuntimeProfile() } = {}) {
  const mobileSized = Number(deviceProfile.width || 0) < 760 || Boolean(deviceProfile.touch);
  const highDpr = Number(deviceProfile.dpr || 1) > 1.75;
  if (contextLossRecoveries >= 2 || activeSurfaces >= WEBGL_CONTEXT_BUDGET || (mobileSized && activeSurfaces >= 3)) {
    return RUNTIME_QUALITY_PROFILES.low;
  }
  if (contextLossRecoveries >= 1 || activeSurfaces >= 4 || mobileSized || highDpr) {
    return RUNTIME_QUALITY_PROFILES.balanced;
  }
  return RUNTIME_QUALITY_PROFILES.high;
}

function cameraStateDistance(a, b) {
  const first = Array.isArray(a?.position) ? a.position : [];
  const second = Array.isArray(b?.position) ? b.position : [];
  if (first.length !== 3 || second.length !== 3) {
    return Number.POSITIVE_INFINITY;
  }
  return Math.hypot(
    Number(first[0] || 0) - Number(second[0] || 0),
    Number(first[1] || 0) - Number(second[1] || 0),
    Number(first[2] || 0) - Number(second[2] || 0),
  );
}

function parseMapCameraDatasetPosition(value) {
  const parts = String(value || "")
    .split(",")
    .map((item) => Number(item));
  return parts.length === 3 && parts.every((item) => Number.isFinite(item)) ? parts : null;
}

function isKeyboardInputTarget(target) {
  const element = target instanceof Element ? target : null;
  return Boolean(element?.closest?.("input, select, textarea, [contenteditable='true']"));
}

function mapMovementToken(event) {
  const code = String(event.code || "").toLowerCase();
  if (/^numpad[0-9]$/.test(code)) {
    return code;
  }
  return String(event.key || "").toLowerCase();
}

function isCatalogFallbackName(value) {
  return /^(gaia|canon:|star:|system:)/i.test(String(value || "").trim());
}

function catalogFamily(value) {
  const text = String(value || "").trim();
  if (/^gaia/i.test(text)) return "Gaia source";
  if (/^wds/i.test(text)) return "WDS double-star";
  if (/^hip/i.test(text)) return "Hipparcos";
  if (/^hd/i.test(text)) return "Henry Draper";
  if (/^gj/i.test(text)) return "Gliese-Jahreiß";
  return "Display name";
}

function shortDisplayName(value) {
  const text = formatName(value);
  const gaia = text.match(/^(Gaia\s+DR\d+\s+)(\d{8,})$/i);
  if (gaia) {
    return `${gaia[1]}…${gaia[2].slice(-5)}`;
  }
  if (text.length > 28) {
    return `${text.slice(0, 16)}…${text.slice(-7)}`;
  }
  return text;
}

function shouldTruncateName(value) {
  return shortDisplayName(value) !== formatName(value);
}

async function copyText(value) {
  if (!navigator.clipboard) {
    return false;
  }
  await navigator.clipboard.writeText(value);
  return true;
}

function distanceBetweenSystems(a, b) {
  if (!a || !b) {
    return 0;
  }
  const dx = Number(a.x_helio_ly || 0) - Number(b.x_helio_ly || 0);
  const dy = Number(a.y_helio_ly || 0) - Number(b.y_helio_ly || 0);
  const dz = Number(a.z_helio_ly || 0) - Number(b.z_helio_ly || 0);
  return Math.hypot(dx, dy, dz);
}

function SystemNameDisplay({ system, linkTo = null, className = "", showCopyButton = true, showInfoButton = true, enablePopover = true }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  if (!system) {
    return null;
  }
  const fullName = formatName(system.display_name || system.system_name);
  const shortName = shortDisplayName(fullName);
  const isTruncated = shouldTruncateName(fullName);
  const copyName = async (event) => {
    event.preventDefault();
    event.stopPropagation();
    const ok = await copyText(fullName).catch(() => false);
    if (ok) {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1100);
    }
  };
  const nameNode = linkTo ? (
    <Link className="map-selection-title-link" to={linkTo}>
      {shortName}
    </Link>
  ) : (
    <span>{shortName}</span>
  );
  return (
    <span
      className={`map-name-wrap ${isTruncated ? "truncated" : ""} ${className}`}
      onMouseEnter={() => enablePopover && setOpen(true)}
      onMouseLeave={() => enablePopover && setOpen(false)}
      onFocusCapture={() => enablePopover && setOpen(true)}
      onBlurCapture={() => enablePopover && setOpen(false)}
    >
      {nameNode}
      {isTruncated && showCopyButton && (
        <button type="button" className="map-name-copy" onClick={copyName} aria-label={`Copy ${fullName}`}>
          {copied ? "Copied" : "Copy"}
        </button>
      )}
      {isTruncated && showInfoButton && (
        <button
          type="button"
          className="map-name-info"
          onClick={(event) => {
            event.preventDefault();
            event.stopPropagation();
            if (enablePopover) {
              setOpen((value) => !value);
            }
          }}
          aria-label={`Show metadata for ${fullName}`}
        >
          i
        </button>
      )}
      {enablePopover && open && (
        <span className="map-name-popover" role="tooltip">
          <strong>{fullName}</strong>
          <span>{catalogFamily(fullName)} · system {system.system_id}</span>
          <span>{formatNumber(system.dist_ly, 2)} ly · {system.dominant_spectral_class} · {formatNumber(system.star_count, 0)} stars · {formatNumber(system.planet_count, 0)} planets</span>
          <span>XYZ {formatNumber(system.x_helio_ly, 2)}, {formatNumber(system.y_helio_ly, 2)}, {formatNumber(system.z_helio_ly, 2)} ly</span>
          <button type="button" className="map-popover-copy" onClick={copyName}>
            {copied ? "Copied" : "Copy full ID"}
          </button>
        </span>
      )}
    </span>
  );
}

function galacticCoordinatesFromIcrs(item) {
  const x = Number(item.x_helio_ly || 0);
  const y = Number(item.y_helio_ly || 0);
  const z = Number(item.z_helio_ly || 0);
  return ICRS_TO_GALACTIC.map((row) => row[0] * x + row[1] * y + row[2] * z);
}

function galacticDirectionToScene(direction, frame = "icrs") {
  const [coreward = 0, spinward = 0, north = 0] = direction;
  if (frame === "galactic") {
    return [coreward, north, spinward];
  }
  const icrsX = ICRS_TO_GALACTIC[0][0] * coreward + ICRS_TO_GALACTIC[1][0] * spinward + ICRS_TO_GALACTIC[2][0] * north;
  const icrsY = ICRS_TO_GALACTIC[0][1] * coreward + ICRS_TO_GALACTIC[1][1] * spinward + ICRS_TO_GALACTIC[2][1] * north;
  const icrsZ = ICRS_TO_GALACTIC[0][2] * coreward + ICRS_TO_GALACTIC[1][2] * spinward + ICRS_TO_GALACTIC[2][2] * north;
  return [icrsX, icrsZ, -icrsY];
}

function mapToScenePosition(item, frame = "icrs") {
  if (frame === "galactic") {
    const [corewardLy, spinwardLy, northLy] = galacticCoordinatesFromIcrs(item);
    return [
      corewardLy * LY_TO_SCENE,
      northLy * LY_TO_SCENE,
      spinwardLy * LY_TO_SCENE,
    ];
  }
  const x = Number(item.x_helio_ly || 0) * LY_TO_SCENE;
  const y = Number(item.z_helio_ly || 0) * LY_TO_SCENE;
  const z = -Number(item.y_helio_ly || 0) * LY_TO_SCENE;
  return [x, y, z];
}

function sceneToHelioCoordinates(position, frame = "icrs") {
  const source = Array.isArray(position) ? position : [0, 0, 0];
  const sceneX = Number(source[0] || 0) / LY_TO_SCENE;
  const sceneY = Number(source[1] || 0) / LY_TO_SCENE;
  const sceneZ = Number(source[2] || 0) / LY_TO_SCENE;
  if (frame === "galactic") {
    const coreward = sceneX;
    const spinward = sceneZ;
    const north = sceneY;
    return {
      x: ICRS_TO_GALACTIC[0][0] * coreward + ICRS_TO_GALACTIC[1][0] * spinward + ICRS_TO_GALACTIC[2][0] * north,
      y: ICRS_TO_GALACTIC[0][1] * coreward + ICRS_TO_GALACTIC[1][1] * spinward + ICRS_TO_GALACTIC[2][1] * north,
      z: ICRS_TO_GALACTIC[0][2] * coreward + ICRS_TO_GALACTIC[1][2] * spinward + ICRS_TO_GALACTIC[2][2] * north,
    };
  }
  return { x: sceneX, y: -sceneZ, z: sceneY };
}

function distanceFromHelioOrigin(system, origin) {
  if (!system || !origin) {
    return Number(system?.dist_ly || 0);
  }
  return Math.hypot(
    Number(system.x_helio_ly || 0) - Number(origin.x || 0),
    Number(system.y_helio_ly || 0) - Number(origin.y || 0),
    Number(system.z_helio_ly || 0) - Number(origin.z || 0),
  );
}

function priorityForSystem(item) {
  const rank = Number(item.coolness_rank);
  const coolness = Number(item.coolness_score);
  const planetBoost = Number(item.planet_count || 0) > 0 ? 2.5 : 0;
  const multiBoost = Number(item.star_count || 0) > 1 ? 1.25 : 0;
  const nearbyBoost = Math.max(0, (100 - Number(item.dist_ly || 100)) / 50);
  const rankScore = Number.isFinite(rank) ? Math.max(0, 8 - Math.log10(rank + 1) * 2.1) : 0;
  const coolScore = Number.isFinite(coolness) ? coolness / 10 : 0;
  return rankScore + coolScore + planetBoost + multiBoost + nearbyBoost;
}

function neighborInterestScore(system, selectedSystem) {
  if (!system || !selectedSystem || system.system_id === selectedSystem.system_id) {
    return -Infinity;
  }
  const coolness = Number(system.coolness_score);
  const coolScore = Number.isFinite(coolness) ? coolness : 0;
  const routeDistance = distanceBetweenSystems(system, selectedSystem);
  const nearbyScore = Math.max(0, 24 - routeDistance) * 0.35;
  const planetBoost = Number(system.planet_count || 0) > 0 ? 20 : 0;
  const multiBoost = Number(system.star_count || 0) > 1 ? 10 : 0;
  const nameBoost = isCatalogFallbackName(system.display_name) ? -8 : 4;
  return coolScore + nearbyScore + planetBoost + multiBoost + nameBoost;
}

function prepareMapItems(rawItems, frame = "icrs") {
  return (rawItems || [])
    .map((item) => {
      const dominant = String(item.dominant_spectral_class || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";
      return {
        ...item,
        display_name: systemDisplayName(item),
        dominant_spectral_class: SPECTRAL_COLORS[dominant] ? dominant : "UNKNOWN",
        scene_position: mapToScenePosition(item, frame),
        map_priority: priorityForSystem(item),
      };
    })
    .filter((item) => item.scene_position.every((value) => Number.isFinite(value)));
}

function systemDisplayName(system) {
  return formatName(system?.display_name || system?.system_name);
}

function mapItemFromSearchResult(item, frame = "icrs") {
  return prepareMapItems([{ ...item, system_name: systemDisplayName(item) }], frame)[0] || null;
}

function createPointTexture() {
  const size = 64;
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d");
  canvas.width = size;
  canvas.height = size;
  const gradient = ctx.createRadialGradient(size / 2, size / 2, 0, size / 2, size / 2, size / 2);
  gradient.addColorStop(0, "rgba(255,255,255,1)");
  gradient.addColorStop(0.48, "rgba(255,255,255,0.92)");
  gradient.addColorStop(0.72, "rgba(255,255,255,0.28)");
  gradient.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, size, size);
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  texture.needsUpdate = true;
  return texture;
}

function StarField({ systems, filterMatchIds = null, filterActive = false }) {
  const geometry = useMemo(() => {
    const positions = new Float32Array(systems.length * 3);
    const colors = new Float32Array(systems.length * 3);
    const matches = filterMatchIds instanceof Set ? filterMatchIds : new Set();
    systems.forEach((system, idx) => {
      const base = idx * 3;
      positions[base] = system.scene_position[0];
      positions[base + 1] = system.scene_position[1];
      positions[base + 2] = system.scene_position[2];
      const color = new THREE.Color(SPECTRAL_COLORS[system.dominant_spectral_class] || SPECTRAL_COLORS.UNKNOWN);
      if (Number(system.planet_count || 0) > 0) {
        color.lerp(new THREE.Color("#95ffcf"), 0.24);
      }
      if (filterActive) {
        if (matches.has(String(system.system_id))) {
          color.lerp(new THREE.Color("#ffffff"), 0.32);
        } else {
          color.multiplyScalar(0.28);
        }
      }
      colors[base] = color.r;
      colors[base + 1] = color.g;
      colors[base + 2] = color.b;
    });
    const next = new THREE.BufferGeometry();
    next.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    next.setAttribute("color", new THREE.BufferAttribute(colors, 3));
    next.computeBoundingSphere();
    return next;
  }, [filterActive, filterMatchIds, systems]);
  const pointTexture = useMemo(() => createPointTexture(), []);

  useEffect(() => () => geometry.dispose(), [geometry]);
  useEffect(() => () => pointTexture.dispose(), [pointTexture]);

  return (
    <points geometry={geometry}>
      <pointsMaterial
        map={pointTexture}
        size={0.16}
        sizeAttenuation
        vertexColors
        transparent
        opacity={filterActive ? 0.74 : 0.86}
        alphaTest={0.04}
        depthWrite={false}
      />
    </points>
  );
}

function DistanceRings() {
  const radii = [10, 25, 50, 100];
  return (
    <group>
      {radii.map((radius) => {
        const sceneRadius = radius * LY_TO_SCENE;
        return (
          <mesh key={radius} rotation={[-Math.PI / 2, 0, 0]}>
            <ringGeometry args={[sceneRadius - 0.018, sceneRadius + 0.018, 160]} />
            <meshBasicMaterial
              color={radius === 100 ? "#6aa7ff" : "#8ac7ff"}
              transparent
              opacity={radius === 100 ? 0.28 : 0.18}
              side={THREE.DoubleSide}
              depthWrite={false}
            />
          </mesh>
        );
      })}
    </group>
  );
}

function DirectionArrow({ direction = [1, 0, 0], color = "#ffe7a3" }) {
  const vector = useMemo(() => new THREE.Vector3().fromArray(direction).normalize(), [direction]);
  const start = vector.clone().multiplyScalar(MAP_RADIUS_LY * LY_TO_SCENE * 0.58);
  const end = vector.clone().multiplyScalar(MAP_RADIUS_LY * LY_TO_SCENE * 0.74);
  const side = new THREE.Vector3().crossVectors(vector, WORLD_UP);
  if (side.lengthSq() < 0.0001) {
    side.crossVectors(vector, new THREE.Vector3(1, 0, 0));
  }
  side.normalize();
  const headBase = end.clone().addScaledVector(vector, -2.6);
  const left = headBase.clone().addScaledVector(side, 1.25);
  const right = headBase.clone().addScaledVector(side, -1.25);
  const positions = new Float32Array([
    start.x, start.y, start.z,
    end.x, end.y, end.z,
    end.x, end.y, end.z,
    left.x, left.y, left.z,
    end.x, end.y, end.z,
    right.x, right.y, right.z,
  ]);
  return (
    <lineSegments>
      <bufferGeometry>
        <bufferAttribute attach="attributes-position" args={[positions, 3]} />
      </bufferGeometry>
      <lineBasicMaterial color={color} transparent opacity={0.62} depthWrite={false} />
    </lineSegments>
  );
}

function DirectionLabels({ visible = false, vectors }) {
  if (!visible) {
    return null;
  }
  const radius = MAP_RADIUS_LY * LY_TO_SCENE * 0.78;
  const positionFor = (vector) => new THREE.Vector3().fromArray(vector).normalize().multiplyScalar(radius).toArray();
  return (
    <group>
      <DirectionArrow direction={vectors.coreward} />
      <DirectionArrow direction={vectors.rimward} />
      <DirectionArrow direction={vectors.spinward} />
      <DirectionArrow direction={vectors.antispinward} />
      <LabelSprite label="Coreward" position={positionFor(vectors.coreward)} tone="direction" />
      <LabelSprite label="Rimward" position={positionFor(vectors.rimward)} tone="direction" />
      <LabelSprite label="Spinward" position={positionFor(vectors.spinward)} tone="direction" />
      <LabelSprite label="Antispinward" position={positionFor(vectors.antispinward)} tone="direction" />
    </group>
  );
}

function OrientationAxes({ frame = "icrs", showDirectionLabels = false }) {
  const directionVectors = useMemo(() => ({
    coreward: galacticDirectionToScene([1, 0, 0], frame),
    rimward: galacticDirectionToScene([-1, 0, 0], frame),
    spinward: galacticDirectionToScene([0, 1, 0], frame),
    antispinward: galacticDirectionToScene([0, -1, 0], frame),
  }), [frame]);
  return (
    <group>
      <mesh position={[0, 0, 0]}>
        <sphereGeometry args={[0.42, 24, 24]} />
        <meshBasicMaterial color="#fff5b8" />
      </mesh>
      <LabelSprite label="Sol" position={[0, 1.15, 0]} selected tone="sol" />
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            args={[new Float32Array([-60, 0, 0, 60, 0, 0]), 3]}
          />
        </bufferGeometry>
        <lineBasicMaterial color="#5f8fff" transparent opacity={0.24} />
      </line>
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            args={[new Float32Array([0, -60, 0, 0, 60, 0]), 3]}
          />
        </bufferGeometry>
        <lineBasicMaterial color="#82ffc5" transparent opacity={0.22} />
      </line>
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            args={[new Float32Array([0, 0, -60, 0, 0, 60]), 3]}
          />
        </bufferGeometry>
        <lineBasicMaterial color="#ff8fd8" transparent opacity={0.18} />
      </line>
      <DirectionLabels visible={showDirectionLabels} vectors={directionVectors} />
    </group>
  );
}

function createLabelTexture(label, { selected = false, tone = "default" } = {}) {
  const pixelRatio = 2;
  const fontSize = 24;
  const paddingX = 14;
  const paddingY = 8;
  const borderRadius = 7;
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d");
  const text = String(label || "System").slice(0, 32);
  ctx.font = `${fontSize}px ui-monospace, SFMono-Regular, Menlo, monospace`;
  const metrics = ctx.measureText(text);
  const width = Math.ceil(metrics.width + paddingX * 2);
  const height = Math.ceil(fontSize + paddingY * 2);
  canvas.width = width * pixelRatio;
  canvas.height = height * pixelRatio;
  ctx.scale(pixelRatio, pixelRatio);
  ctx.font = `${fontSize}px ui-monospace, SFMono-Regular, Menlo, monospace`;
  ctx.textBaseline = "middle";

  const toneStyles = {
    sol: { border: "rgba(255,245,184,0.86)", fill: "rgba(4,10,22,0.78)", ink: "#fff5b8" },
    route: { border: "rgba(151,255,207,0.9)", fill: "rgba(5,29,24,0.82)", ink: "#d9fff0" },
    direction: { border: "rgba(255,220,117,0.92)", fill: "rgba(32,20,5,0.82)", ink: "#ffe7a3" },
    selected: { border: "rgba(125,251,255,0.9)", fill: "rgba(7,27,44,0.9)", ink: "#eef9ff" },
    default: { border: "rgba(158,221,255,0.52)", fill: "rgba(4,10,22,0.78)", ink: "#eef9ff" },
  };
  const style = toneStyles[tone] || (selected ? toneStyles.selected : toneStyles.default);
  const { border, fill, ink } = style;

  ctx.fillStyle = fill;
  ctx.strokeStyle = border;
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.roundRect(1, 1, width - 2, height - 2, borderRadius);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = ink;
  ctx.fillText(text, paddingX, height / 2 + 1);

  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  texture.needsUpdate = true;
  return { texture, width, height };
}

function LabelSprite({
  label,
  position,
  selected = false,
  tone = "default",
  priority = 0,
  labelRank = 0,
  labelCount = 1,
  onSelect = null,
}) {
  const spriteRef = useRef(null);
  const payload = useMemo(
    () => createLabelTexture(label, { selected, tone }),
    [label, selected, tone],
  );

  useEffect(() => () => payload.texture.dispose(), [payload]);

  useFrame(({ camera }) => {
    if (!spriteRef.current) {
      return;
    }
    const distance = camera.position.distanceTo(spriteRef.current.position);
    const sticky = selected || tone === "sol" || tone === "route" || tone === "direction";
    const normalizedPriority = THREE.MathUtils.clamp(Number(priority || 0) / 12, 0, 1);
    const nearFade = 1 - THREE.MathUtils.smoothstep(distance, 12, 42);
    const crowdPressure = THREE.MathUtils.clamp((Number(labelCount || 1) - 34) / 34, 0, 1);
    const rankPressure = THREE.MathUtils.smoothstep(Number(labelRank || 0), 24, Math.max(34, Number(labelCount || 1)));
    const lowPriorityPenalty = (1 - normalizedPriority) * crowdPressure * rankPressure * 0.52;
    const stickyFloor = sticky ? 0.92 : normalizedPriority * (0.46 - crowdPressure * 0.18);
    const opacity = THREE.MathUtils.clamp(Math.max(stickyFloor, nearFade) - lowPriorityPenalty, 0.035, 0.98);
    const scale = THREE.MathUtils.clamp(distance / 18, 0.45, sticky ? 2.8 : 2.15);
    spriteRef.current.scale.set((payload.width / 95) * scale, (payload.height / 95) * scale, 1);
    spriteRef.current.material.opacity = opacity;
  });

  return (
    <sprite
      ref={spriteRef}
      position={position}
      onClick={(event) => {
        if (!onSelect) {
          return;
        }
        event.stopPropagation();
        onSelect();
      }}
    >
      <spriteMaterial map={payload.texture} transparent depthWrite={false} depthTest={false} opacity={1} />
    </sprite>
  );
}

function PriorityLabels({ systems, selectedSystem, onSelect, forcedLabelSystems = null, forcedLabelActive = false }) {
  const { camera, gl } = useThree();
  const updateClockRef = useRef(0);
  const buildLabelSet = useCallback(() => {
    const seen = new Set();
    const candidates = [];
    const add = (system, labelPriority = system?.map_priority, cameraDistanceLy = null) => {
      if (!system?.system_id || seen.has(system.system_id)) {
        return;
      }
      seen.add(system.system_id);
      candidates.push({ ...system, label_priority: labelPriority, label_camera_distance_ly: cameraDistanceLy });
    };
    if (forcedLabelActive) {
      (forcedLabelSystems || [])
        .filter((system) => !isCatalogFallbackName(system.display_name))
        .slice(0, 140)
        .forEach((system) => {
          const position = new THREE.Vector3().fromArray(system.scene_position);
          const cameraDistanceLy = position.distanceTo(camera.position) / LY_TO_SCENE;
          add(system, Math.max(16, Number(system.map_priority || 0)), cameraDistanceLy);
        });
      if (selectedSystem) {
        const selectedPosition = new THREE.Vector3().fromArray(selectedSystem.scene_position);
        const selectedDistanceLy = selectedPosition.distanceTo(camera.position) / LY_TO_SCENE;
        add(selectedSystem, Math.max(18, Number(selectedSystem.map_priority || 0)), selectedDistanceLy);
      }
      return candidates;
    }
    if (selectedSystem) {
      const selectedPosition = new THREE.Vector3().fromArray(selectedSystem.scene_position);
      const selectedDistanceLy = selectedPosition.distanceTo(camera.position) / LY_TO_SCENE;
      add(selectedSystem, Math.max(14, Number(selectedSystem.map_priority || 0)), selectedDistanceLy);
    }
    const scored = systems
      .filter((system) => !isCatalogFallbackName(system.display_name))
      .map((system) => {
        const position = new THREE.Vector3().fromArray(system.scene_position);
        const cameraDistanceLy = position.distanceTo(camera.position) / LY_TO_SCENE;
        const priority = Number(system.map_priority || 0);
        const coolness = Number(system.coolness_score);
        const coolScore = Number.isFinite(coolness) ? coolness / 5 : 0;
        const nearScore = cameraDistanceLy <= 10
          ? 100 - cameraDistanceLy
          : Math.max(0, 26 - cameraDistanceLy) * 0.8;
        return {
          system,
          cameraDistanceLy,
          priority,
          score: nearScore + priority * 1.25 + coolScore,
        };
      })
      .sort((left, right) => right.score - left.score);
    const localSystems = scored.filter(({ cameraDistanceLy }) => cameraDistanceLy <= 10);
    const localCount = localSystems.length;
    const totalBudget = localCount < 12
      ? 58
      : localCount < 28
        ? 50
        : Math.min(72, localCount + 20);
    localSystems.forEach(({ system, priority, cameraDistanceLy }) => add(system, Math.max(12, priority), cameraDistanceLy));
    scored
      .filter(({ cameraDistanceLy }) => cameraDistanceLy > 10 && cameraDistanceLy <= 24)
      .slice(0, Math.max(0, 12 - localCount))
      .forEach(({ system, priority, score, cameraDistanceLy }) => add(system, Math.max(priority, score / 4), cameraDistanceLy));
    scored
      .filter(({ cameraDistanceLy, score }) => cameraDistanceLy > 10 && score >= 9)
      .slice(0, Math.max(0, totalBudget - candidates.length))
      .forEach(({ system, priority, score, cameraDistanceLy }) => add(system, Math.max(priority, score / 4), cameraDistanceLy));
    return candidates;
  }, [camera, forcedLabelActive, forcedLabelSystems, selectedSystem, systems]);
  const [labelSystems, setLabelSystems] = useState(buildLabelSet);

  useEffect(() => {
    setLabelSystems(buildLabelSet());
  }, [buildLabelSet]);

  useEffect(() => {
    gl.domElement.dataset.mapLabelCount = String(labelSystems.length);
    gl.domElement.dataset.mapLocalLabelCount = String(labelSystems.filter((system) => Number(system.label_camera_distance_ly) <= 10).length);
    gl.domElement.dataset.mapLabelStrategy = forcedLabelActive ? "star_search_filters" : "camera_near_10ly_nearest_plus_coolness";
  }, [forcedLabelActive, gl.domElement, labelSystems]);

  useFrame((_, delta) => {
    updateClockRef.current += delta;
    if (updateClockRef.current < 0.42) {
      return;
    }
    updateClockRef.current = 0;
    setLabelSystems(buildLabelSet());
  });

  return (
    <group>
      {labelSystems.map((system, index) => (
        <LabelSprite
          key={system.system_id}
          label={system.display_name}
          position={system.scene_position}
          selected={selectedSystem?.system_id === system.system_id}
          priority={system.label_priority ?? system.map_priority}
          labelRank={index}
          labelCount={labelSystems.length}
          onSelect={() => onSelect(system)}
        />
      ))}
    </group>
  );
}

function SelectionMarker({ system }) {
  const groupRef = useRef(null);
  const { camera } = useThree();

  useFrame(() => {
    if (groupRef.current) {
      groupRef.current.quaternion.copy(camera.quaternion);
    }
  });

  if (!system) {
    return null;
  }
  return (
    <group ref={groupRef} position={system.scene_position}>
      <group rotation={[1.02, 0.16, -0.58]}>
        <mesh renderOrder={7}>
          <ringGeometry args={[0.62, 0.655, 72]} />
          <meshBasicMaterial color="#55dfff" transparent opacity={0.78} side={THREE.DoubleSide} depthWrite={false} />
        </mesh>
        <mesh position={[0.65, 0, 0]} renderOrder={9}>
          <sphereGeometry args={[0.09, 16, 16]} />
          <meshBasicMaterial color="#55dfff" transparent opacity={0.96} depthWrite={false} />
        </mesh>
      </group>
    </group>
  );
}

function RouteSegmentLine({ segment }) {
  const geometry = useMemo(() => {
    const positions = new Float32Array([
      ...segment.from.scene_position,
      ...segment.to.scene_position,
    ]);
    const next = new THREE.BufferGeometry();
    next.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    next.computeBoundingSphere();
    return next;
  }, [segment.from.scene_position, segment.to.scene_position]);

  useEffect(() => () => geometry.dispose(), [geometry]);

  return (
    <line geometry={geometry}>
      <lineBasicMaterial color="#97ffcf" transparent opacity={0.72} depthWrite={false} />
    </line>
  );
}

function RouteSegmentHitTarget({ segment, onClick }) {
  const shape = useMemo(() => {
    const from = new THREE.Vector3().fromArray(segment.from.scene_position);
    const to = new THREE.Vector3().fromArray(segment.to.scene_position);
    const midpoint = from.clone().add(to).multiplyScalar(0.5);
    const direction = to.clone().sub(from);
    const length = direction.length();
    const quaternion = new THREE.Quaternion();
    if (length > 0.0001) {
      quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), direction.normalize());
    }
    return { midpoint, quaternion, length };
  }, [segment.from.scene_position, segment.to.scene_position]);

  return (
    <mesh
      position={shape.midpoint}
      quaternion={shape.quaternion}
      onClick={(event) => {
        event.stopPropagation();
        onClick?.();
      }}
    >
      <cylinderGeometry args={[0.16, 0.16, Math.max(shape.length, 0.01), 8, 1, true]} />
      <meshBasicMaterial transparent opacity={0.001} depthWrite={false} />
    </mesh>
  );
}

function RouteOverlays({ segments, onRemoveSegment }) {
  if (!segments.length) {
    return null;
  }
  const routeSegments = segments.filter((segment) => segment.kind !== "neighbor");
  const total = routeSegments.reduce((sum, segment) => sum + segment.distance_ly, 0);
  const last = segments[segments.length - 1];
  const totalPosition = [
    last.to.scene_position[0],
    last.to.scene_position[1] + 1.2,
    last.to.scene_position[2],
  ];

  return (
    <group>
      {segments.map((segment, index) => {
        const midpoint = [
          (segment.from.scene_position[0] + segment.to.scene_position[0]) / 2,
          (segment.from.scene_position[1] + segment.to.scene_position[1]) / 2 + 0.45,
          (segment.from.scene_position[2] + segment.to.scene_position[2]) / 2,
        ];
        return (
          <group key={segment.id}>
            <RouteSegmentLine segment={segment} />
            <RouteSegmentHitTarget segment={segment} onClick={() => onRemoveSegment?.(index)} />
            <LabelSprite
              label={segment.label || `${formatNumber(segment.distance_ly, 2)} ly`}
              position={midpoint}
              tone="route"
            />
            {routeSegments.length > 1 && segment.kind !== "neighbor" && index === segments.length - 1 && (
              <LabelSprite
                label={`Route ${formatNumber(total, 2)} ly`}
                position={totalPosition}
                tone="route"
              />
            )}
          </group>
        );
      })}
    </group>
  );
}

function nearestSystemAlongRay(origin, direction, systems) {
  let best = null;
  let bestScore = Infinity;
  const candidate = new THREE.Vector3();
  for (const system of systems) {
    candidate.fromArray(system.scene_position);
    candidate.sub(origin);
    const along = candidate.dot(direction);
    if (along <= 0) {
      continue;
    }
    const distanceSq = candidate.lengthSq();
    const perpSq = Math.max(0, distanceSq - along * along);
    const threshold = Math.max(0.3, along * 0.018);
    if (perpSq > threshold * threshold) {
      continue;
    }
    const score = Math.sqrt(perpSq) / threshold + along * 0.0008;
    if (score < bestScore) {
      best = system;
      bestScore = score;
    }
  }
  return best;
}

function nearestSystemToReticle(camera, systems) {
  const direction = new THREE.Vector3();
  camera.getWorldDirection(direction);
  return nearestSystemAlongRay(camera.position, direction, systems);
}

function nearestSystemToPointer(camera, canvas, clientX, clientY, systems) {
  const rect = canvas.getBoundingClientRect();
  const ndc = new THREE.Vector3(
    ((clientX - rect.left) / rect.width) * 2 - 1,
    -(((clientY - rect.top) / rect.height) * 2 - 1),
    0.5,
  );
  const direction = ndc.unproject(camera).sub(camera.position).normalize();
  return nearestSystemAlongRay(camera.position, direction, systems);
}

function pointerDistance(a, b) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function pointerCentroid(a, b) {
  return {
    x: (a.x + b.x) / 2,
    y: (a.y + b.y) / 2,
  };
}

function FlightControls({
  systems,
  onSelect,
  onRouteContext,
  keybindScheme,
  mapFrame,
  showDirectionLabels,
  controlsEnabled,
  stabilizationEnabled,
  onTelemetry,
  onCameraState,
  initialCameraState,
  reticleSelectRequest,
  focusTarget,
  focusToken,
  mobileFlightIntent = DEFAULT_MOBILE_FLIGHT_STATE,
}) {
  const { camera, gl } = useThree();
  const keysRef = useRef(new Set());
  const yawRef = useRef(0);
  const pitchRef = useRef(-0.08);
  const telemetryClockRef = useRef(0);
  const suppressContextMenuRef = useRef(false);
  const touchGestureRef = useRef({
    pointers: new Map(),
    lastPinchDistance: null,
    lastCentroid: null,
    primaryStart: null,
    moved: false,
  });
  const mouseDragRef = useRef({
    active: false,
    mode: "look",
    pointerId: null,
    lastX: 0,
    lastY: 0,
    startX: 0,
    startY: 0,
    startTime: 0,
    moved: false,
  });
  const focusRef = useRef(null);
  const handledFocusTokenRef = useRef(focusToken || 0);
  const initialCameraStateRef = useRef(initialCameraState || DEFAULT_MAP_CAMERA_STATE);
  const handledReticleSelectRequestRef = useRef(0);
  const activeKeybind = MAP_KEYBIND_SCHEMES[keybindScheme] || MAP_KEYBIND_SCHEMES.wasd;

  const selectReticleTarget = useCallback(() => {
    const target = nearestSystemToReticle(camera, systems);
    if (target) {
      onSelect(target);
    }
  }, [camera, onSelect, systems]);

  const selectPointerTarget = useCallback((clientX, clientY) => {
    const target = nearestSystemToPointer(camera, gl.domElement, clientX, clientY, systems);
    if (target) {
      onSelect(target);
    }
  }, [camera, gl.domElement, onSelect, systems]);

  const openRouteContext = useCallback((event) => {
    event.preventDefault();
    const target = nearestSystemToPointer(camera, gl.domElement, event.clientX, event.clientY, systems);
    if (!target) {
      onRouteContext(null);
      return;
    }
    onRouteContext({
      x: Math.min(event.clientX, window.innerWidth - 264),
      y: Math.min(event.clientY, window.innerHeight - 180),
      target,
    });
  }, [camera, gl.domElement, onRouteContext, systems]);

  useEffect(() => {
    if (
      reticleSelectRequest > 0
      && reticleSelectRequest !== handledReticleSelectRequestRef.current
    ) {
      handledReticleSelectRequestRef.current = reticleSelectRequest;
      selectReticleTarget();
    }
  }, [reticleSelectRequest, selectReticleTarget]);

  useEffect(() => {
    if (!focusTarget?.scene_position || !focusToken) {
      return;
    }
    if (focusToken === handledFocusTokenRef.current) {
      return;
    }
    handledFocusTokenRef.current = focusToken;
    const target = new THREE.Vector3().fromArray(focusTarget.scene_position);
    const fromTarget = camera.position.clone().sub(target);
    const approach = fromTarget.lengthSq() > 0.0001
      ? fromTarget.normalize()
      : new THREE.Vector3(0.25, 0.16, 1).normalize();
    const destination = target.clone()
      .addScaledVector(approach, 5.4)
      .addScaledVector(WORLD_UP, 1.35);
    focusRef.current = { target, destination, elapsed: 0 };
  }, [camera, focusTarget, focusToken]);

  const applyLookDelta = useCallback((deltaX, deltaY, sensitivity = 0.002) => {
    yawRef.current -= deltaX * sensitivity;
    pitchRef.current -= deltaY * sensitivity;
    pitchRef.current = Math.max(-1.34, Math.min(1.34, pitchRef.current));
    camera.rotation.set(pitchRef.current, yawRef.current, 0);
  }, [camera]);

  const updateCameraDataset = useCallback((gesture = "") => {
    gl.domElement.dataset.mapCameraPosition = camera.position.toArray().map((value) => value.toFixed(3)).join(",");
    gl.domElement.dataset.mapFrame = mapFrame || "icrs";
    gl.domElement.dataset.mapDirectionLabels = showDirectionLabels ? "true" : "false";
    if (gesture) {
      gl.domElement.dataset.mapCameraGesture = gesture;
    }
    onCameraState?.({
      position: camera.position.toArray(),
      yaw: yawRef.current,
      pitch: pitchRef.current,
      mapFrame: mapFrame || "icrs",
    });
  }, [camera, gl.domElement, mapFrame, onCameraState, showDirectionLabels]);

  const orbitTargetPosition = useCallback(() => {
    const targetSystem = focusTarget?.scene_position
      ? focusTarget
      : systems.find((system) => String(system.display_name || system.system_name || "").toLowerCase() === "sol");
    return new THREE.Vector3().fromArray(targetSystem?.scene_position || [0, 0, 0]);
  }, [focusTarget, systems]);

  const syncYawPitchFromCamera = useCallback(() => {
    const direction = new THREE.Vector3();
    camera.getWorldDirection(direction).normalize();
    yawRef.current = Math.atan2(-direction.x, -direction.z);
    pitchRef.current = Math.asin(THREE.MathUtils.clamp(direction.y, -0.98, 0.98));
  }, [camera]);

  const applyOrbitDelta = useCallback((deltaX, deltaY) => {
    const target = orbitTargetPosition();
    const offset = camera.position.clone().sub(target);
    if (offset.lengthSq() < 0.0001) {
      offset.set(0, 2.5, 8);
    }
    offset.applyAxisAngle(WORLD_UP, -deltaX * MOUSE_ORBIT_SENSITIVITY);
    const strafe = new THREE.Vector3().crossVectors(offset.clone().normalize(), WORLD_UP).normalize();
    if (strafe.lengthSq() > 0.0001) {
      offset.applyAxisAngle(strafe, -deltaY * MOUSE_ORBIT_PITCH_SENSITIVITY);
    }
    const minRadius = 1.2;
    if (offset.length() < minRadius) {
      offset.setLength(minRadius);
    }
    camera.position.copy(target).add(offset);
    camera.lookAt(target);
    syncYawPitchFromCamera();
    updateCameraDataset("two-button-orbit");
  }, [camera, orbitTargetPosition, syncYawPitchFromCamera, updateCameraDataset]);

  useEffect(() => {
    updateCameraDataset();
  }, [updateCameraDataset]);

  useEffect(() => {
    const state = initialCameraStateRef.current || DEFAULT_MAP_CAMERA_STATE;
    const position = Array.isArray(state.position) && state.position.length === 3
      ? state.position
      : DEFAULT_MAP_CAMERA_STATE.position;
    yawRef.current = Number.isFinite(Number(state.yaw))
      ? Number(state.yaw)
      : DEFAULT_MAP_CAMERA_STATE.yaw;
    pitchRef.current = Number.isFinite(Number(state.pitch))
      ? Number(state.pitch)
      : DEFAULT_MAP_CAMERA_STATE.pitch;
    camera.position.set(
      Number(position[0]) || 0,
      Number(position[1]) || 0,
      Number(position[2]) || 0,
    );
    camera.rotation.order = "YXZ";
    camera.rotation.set(pitchRef.current, yawRef.current, 0);
    gl.domElement.dataset.mapCameraPosition = camera.position.toArray().map((value) => value.toFixed(3)).join(",");
    gl.domElement.dataset.mapFrame = mapFrame || "icrs";
    gl.domElement.dataset.mapDirectionLabels = showDirectionLabels ? "true" : "false";
    gl.domElement.dataset.mapCameraGesture = "restore";
    onCameraState?.({
      position: camera.position.toArray(),
      yaw: yawRef.current,
      pitch: pitchRef.current,
      mapFrame: mapFrame || "icrs",
    });
  }, [camera, gl.domElement]);

  useEffect(() => {
    gl.domElement.dataset.mapKeybindScheme = activeKeybind.id;
  }, [activeKeybind.id, gl.domElement]);

  useEffect(() => {
    const movementKeys = new Set([
      activeKeybind.forward,
      activeKeybind.back,
      activeKeybind.left,
      activeKeybind.right,
      activeKeybind.up,
      activeKeybind.down,
      "arrowup",
      "arrowdown",
      "arrowleft",
      "arrowright",
      "shift",
    ]);
    const onKeyDown = (event) => {
      const key = mapMovementToken(event);
      if (isKeyboardInputTarget(event.target)) {
        return;
      }
      if (movementKeys.has(key) && (controlsEnabled || document.pointerLockElement === gl.domElement)) {
        event.preventDefault();
      }
      if (key === "escape") {
        return;
      }
      keysRef.current.add(key);
    };
    const onKeyUp = (event) => {
      keysRef.current.delete(mapMovementToken(event));
    };
    const onMouseMove = (event) => {
      if (document.pointerLockElement !== gl.domElement) {
        return;
      }
      applyLookDelta(event.movementX, event.movementY);
    };
    const onMouseDown = (event) => {
      if (event.button !== 0 || document.pointerLockElement !== gl.domElement) {
        return;
      }
      selectReticleTarget();
    };
    window.addEventListener("keydown", onKeyDown, { passive: false });
    window.addEventListener("keyup", onKeyUp);
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mousedown", onMouseDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("keyup", onKeyUp);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mousedown", onMouseDown);
    };
  }, [activeKeybind, applyLookDelta, controlsEnabled, gl.domElement, selectReticleTarget]);

  useEffect(() => {
    const canvas = gl.domElement;
    const gesture = touchGestureRef.current;
    const mouseDrag = mouseDragRef.current;
    const activePointers = () => Array.from(gesture.pointers.values());

    const resetPinchState = () => {
      const pointers = activePointers();
      if (pointers.length === 2) {
        gesture.lastPinchDistance = pointerDistance(pointers[0], pointers[1]);
        gesture.lastCentroid = pointerCentroid(pointers[0], pointers[1]);
      } else {
        gesture.lastPinchDistance = null;
        gesture.lastCentroid = null;
      }
    };

    const onPointerDown = (event) => {
      if (event.pointerType === "mouse") {
        if (![0, 1, 2].includes(event.button) || document.pointerLockElement === canvas) {
          return;
        }
        if (mouseDrag.active && (event.buttons & 1) && (event.buttons & 2)) {
          event.preventDefault();
          mouseDrag.mode = "orbit";
          mouseDrag.lastX = event.clientX;
          mouseDrag.lastY = event.clientY;
          return;
        }
        if (event.button !== 2 || (event.buttons & 1)) {
          event.preventDefault();
        }
        try {
          canvas.setPointerCapture?.(event.pointerId);
        } catch {
          // Synthetic pointer events used by tests may not create capturable pointers.
        }
        mouseDrag.active = true;
        mouseDrag.mode = (event.buttons & 1) && (event.buttons & 2)
          ? "orbit"
          : event.button === 1 ? "pedestal" : event.button === 2 ? "truck" : "look";
        mouseDrag.pointerId = event.pointerId;
        mouseDrag.lastX = event.clientX;
        mouseDrag.lastY = event.clientY;
        mouseDrag.startX = event.clientX;
        mouseDrag.startY = event.clientY;
        mouseDrag.startTime = performance.now();
        mouseDrag.moved = false;
        return;
      }
      event.preventDefault();
      try {
        canvas.setPointerCapture?.(event.pointerId);
      } catch {
        // Synthetic pointer events used by tests may not create capturable pointers.
      }
      gesture.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
      if (gesture.pointers.size === 1) {
        gesture.primaryStart = { x: event.clientX, y: event.clientY, time: performance.now() };
        gesture.moved = false;
      }
      resetPinchState();
    };

    const onPointerMove = (event) => {
      if (event.pointerType === "mouse") {
        if (!mouseDrag.active || mouseDrag.pointerId !== event.pointerId) {
          return;
        }
        event.preventDefault();
        const deltaX = event.clientX - mouseDrag.lastX;
        const deltaY = event.clientY - mouseDrag.lastY;
        mouseDrag.lastX = event.clientX;
        mouseDrag.lastY = event.clientY;
        const totalMove = Math.hypot(event.clientX - mouseDrag.startX, event.clientY - mouseDrag.startY);
        mouseDrag.moved = mouseDrag.moved || totalMove > 5;
        if (mouseDrag.mode === "orbit" || ((event.buttons & 1) && (event.buttons & 2))) {
          mouseDrag.mode = "orbit";
          applyOrbitDelta(deltaX, deltaY);
        } else if (mouseDrag.mode === "truck") {
          const direction = new THREE.Vector3();
          const strafe = new THREE.Vector3();
          camera.getWorldDirection(direction).normalize();
          strafe.crossVectors(direction, WORLD_UP).normalize();
          camera.position.addScaledVector(strafe, deltaX * MOUSE_DRAG_TRANSLATE_SPEED);
          updateCameraDataset("right-drag-truck");
        } else if (mouseDrag.mode === "pedestal") {
          camera.position.addScaledVector(WORLD_UP, -deltaY * MOUSE_DRAG_TRANSLATE_SPEED);
          updateCameraDataset("middle-drag-pedestal");
        } else {
          applyLookDelta(deltaX, deltaY, MOUSE_LOOK_SENSITIVITY);
        }
        return;
      }
      if (!gesture.pointers.has(event.pointerId)) {
        return;
      }
      event.preventDefault();
      const previous = gesture.pointers.get(event.pointerId);
      gesture.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
      const pointers = activePointers();

      if (pointers.length === 1 && previous) {
        const deltaX = event.clientX - previous.x;
        const deltaY = event.clientY - previous.y;
        if (gesture.primaryStart) {
          const totalMove = Math.hypot(event.clientX - gesture.primaryStart.x, event.clientY - gesture.primaryStart.y);
          gesture.moved = gesture.moved || totalMove > 8;
        }
        applyLookDelta(deltaX, deltaY, TOUCH_LOOK_SENSITIVITY);
        return;
      }

      if (pointers.length === 2) {
        const nextDistance = pointerDistance(pointers[0], pointers[1]);
        const nextCentroid = pointerCentroid(pointers[0], pointers[1]);
        const direction = new THREE.Vector3();
        const strafe = new THREE.Vector3();
        camera.getWorldDirection(direction).normalize();
        strafe.crossVectors(direction, WORLD_UP).normalize();

        if (gesture.lastPinchDistance !== null) {
          camera.position.addScaledVector(direction, (nextDistance - gesture.lastPinchDistance) * TOUCH_PINCH_SPEED);
        }
        if (gesture.lastCentroid) {
          camera.position.addScaledVector(strafe, (nextCentroid.x - gesture.lastCentroid.x) * TOUCH_PAN_SPEED);
          camera.position.addScaledVector(WORLD_UP, -(nextCentroid.y - gesture.lastCentroid.y) * TOUCH_PAN_SPEED);
        }
        gesture.lastPinchDistance = nextDistance;
        gesture.lastCentroid = nextCentroid;
      }
    };

    const onPointerEnd = (event) => {
      if (event.pointerType === "mouse") {
        if (!mouseDrag.active || mouseDrag.pointerId !== event.pointerId) {
          return;
        }
        event.preventDefault();
        try {
          canvas.releasePointerCapture?.(event.pointerId);
        } catch {
          // Ignore non-captured synthetic pointers.
        }
        const duration = performance.now() - mouseDrag.startTime;
        if ((mouseDrag.mode === "truck" || mouseDrag.mode === "orbit") && mouseDrag.moved) {
          suppressContextMenuRef.current = true;
          window.__spacegateMapSuppressNextContextMenu = true;
        }
        if (mouseDrag.mode === "look" && !mouseDrag.moved && duration < 320) {
          selectPointerTarget(event.clientX, event.clientY);
        }
        mouseDrag.active = false;
        mouseDrag.mode = "look";
        mouseDrag.pointerId = null;
        return;
      }
      if (!gesture.pointers.has(event.pointerId)) {
        return;
      }
      event.preventDefault();
      const hadSinglePointer = gesture.pointers.size === 1;
      gesture.pointers.delete(event.pointerId);
      try {
        canvas.releasePointerCapture?.(event.pointerId);
      } catch {
        // Ignore non-captured synthetic pointers.
      }
      if (hadSinglePointer && gesture.primaryStart) {
        const duration = performance.now() - gesture.primaryStart.time;
        if (!gesture.moved && duration < 320) {
          selectReticleTarget();
        }
      }
      if (gesture.pointers.size === 1) {
        const [remaining] = activePointers();
        gesture.primaryStart = { x: remaining.x, y: remaining.y, time: performance.now() };
        gesture.moved = false;
      } else if (gesture.pointers.size === 0) {
        gesture.primaryStart = null;
        gesture.moved = false;
      }
      resetPinchState();
    };

    const onWheel = (event) => {
      if (!controlsEnabled && document.pointerLockElement !== canvas) {
        return;
      }
      event.preventDefault();
      const direction = new THREE.Vector3();
      const strafe = new THREE.Vector3();
      camera.getWorldDirection(direction).normalize();
      strafe.crossVectors(direction, WORLD_UP).normalize();
      if (Math.abs(event.deltaY) > 0) {
        const wheelMagnitude = Math.min(6, Math.max(0.5, Math.abs(event.deltaY) / 90));
        const wheelDirection = event.deltaY < 0 ? 1 : -1;
        camera.position.addScaledVector(direction, wheelDirection * wheelMagnitude * MOUSE_WHEEL_FLY_SPEED);
      }
      if (Math.abs(event.deltaX) > 0) {
        const truckMagnitude = Math.min(5, Math.max(0.25, Math.abs(event.deltaX) / 90));
        const truckDirection = event.deltaX > 0 ? 1 : -1;
        camera.position.addScaledVector(strafe, truckDirection * truckMagnitude * MOUSE_WHEEL_TRUCK_SPEED);
      }
      updateCameraDataset("wheel");
    };

    const onContextMenu = (event) => {
      if (suppressContextMenuRef.current) {
        event.preventDefault();
        suppressContextMenuRef.current = false;
        return;
      }
      openRouteContext(event);
    };

    canvas.addEventListener("pointerdown", onPointerDown, { passive: false });
    canvas.addEventListener("pointermove", onPointerMove, { passive: false });
    canvas.addEventListener("pointerup", onPointerEnd, { passive: false });
    canvas.addEventListener("pointercancel", onPointerEnd, { passive: false });
    canvas.addEventListener("wheel", onWheel, { passive: false });
    canvas.addEventListener("contextmenu", onContextMenu);
    return () => {
      canvas.removeEventListener("pointerdown", onPointerDown);
      canvas.removeEventListener("pointermove", onPointerMove);
      canvas.removeEventListener("pointerup", onPointerEnd);
      canvas.removeEventListener("pointercancel", onPointerEnd);
      canvas.removeEventListener("wheel", onWheel);
      canvas.removeEventListener("contextmenu", onContextMenu);
    };
  }, [applyLookDelta, applyOrbitDelta, camera, controlsEnabled, gl.domElement, openRouteContext, selectPointerTarget, selectReticleTarget, updateCameraDataset]);

  useFrame((_, delta) => {
    if (focusRef.current) {
      const focus = focusRef.current;
      focus.elapsed += delta;
      const progress = Math.min(1, focus.elapsed / 0.9);
      const eased = 1 - Math.pow(1 - progress, 3);
      camera.position.lerp(focus.destination, Math.min(0.18 + eased * 0.24, 0.42));
      const direction = focus.target.clone().sub(camera.position).normalize();
      yawRef.current = Math.atan2(-direction.x, -direction.z);
      pitchRef.current = Math.asin(THREE.MathUtils.clamp(direction.y, -0.98, 0.98));
      camera.rotation.set(pitchRef.current, yawRef.current, 0);
      if (progress >= 1 || camera.position.distanceTo(focus.destination) < 0.08) {
        focusRef.current = null;
      }
    }
    if (!controlsEnabled && document.pointerLockElement !== gl.domElement) {
      return;
    }
    if (stabilizationEnabled) {
      camera.rotation.z = 0;
    }
    const keys = keysRef.current;
    const direction = new THREE.Vector3();
    const strafe = new THREE.Vector3();
    const movement = new THREE.Vector3();
    camera.getWorldDirection(direction).normalize();
    strafe.crossVectors(direction, WORLD_UP).normalize();
    const baseSpeed = keys.has("shift") ? KEYBOARD_BOOST_SPEED : KEYBOARD_BASE_SPEED;
    if (keys.has(activeKeybind.forward) || keys.has("arrowup") || mobileFlightIntent.forward) movement.add(direction);
    if (keys.has(activeKeybind.back) || keys.has("arrowdown") || mobileFlightIntent.back) movement.sub(direction);
    if (keys.has(activeKeybind.right) || keys.has("arrowright") || mobileFlightIntent.right) movement.add(strafe);
    if (keys.has(activeKeybind.left) || keys.has("arrowleft") || mobileFlightIntent.left) movement.sub(strafe);
    if (keys.has(activeKeybind.up) || mobileFlightIntent.up) movement.add(WORLD_UP);
    if (keys.has(activeKeybind.down) || mobileFlightIntent.down) movement.sub(WORLD_UP);
    if (movement.lengthSq() > 0) {
      movement.normalize().multiplyScalar(baseSpeed * delta);
      camera.position.add(movement);
    }
    telemetryClockRef.current += delta;
    if (telemetryClockRef.current >= 0.18) {
      telemetryClockRef.current = 0;
      onTelemetry({
        distLy: camera.position.length() / LY_TO_SCENE,
        speedLyS: baseSpeed / LY_TO_SCENE,
        locked: document.pointerLockElement === gl.domElement,
        cameraScenePosition: camera.position.toArray(),
        yaw: yawRef.current,
        pitch: pitchRef.current,
        originLy: sceneToHelioCoordinates(camera.position.toArray(), mapFrame || "icrs"),
      });
      gl.domElement.dataset.mapKeybindScheme = activeKeybind.id;
      gl.domElement.dataset.mapMobileFlightActive = Object.values(mobileFlightIntent).some(Boolean) ? "true" : "false";
      gl.domElement.dataset.mapCameraPosition = camera.position.toArray().map((value) => value.toFixed(3)).join(",");
      gl.domElement.dataset.mapFrame = mapFrame || "icrs";
      gl.domElement.dataset.mapDirectionLabels = showDirectionLabels ? "true" : "false";
    }
  });

  return null;
}

function MapWebGLContextGuard({ onContextLost }) {
  const { camera, gl } = useThree();

  useEffect(() => {
    const target = gl.domElement;
    if (!target) {
      return undefined;
    }
    const handleContextLost = (event) => {
      event.preventDefault();
      onContextLost?.({
        position: camera.position.toArray(),
        yaw: camera.rotation.y,
        pitch: camera.rotation.x,
        datasetPosition: target.dataset.mapCameraPosition || "",
      });
    };
    target.addEventListener("webglcontextlost", handleContextLost, false);
    return () => {
      target.removeEventListener("webglcontextlost", handleContextLost, false);
    };
  }, [camera, gl, onContextLost]);

  return null;
}

function MapRuntimeBridge({ runtimeDiagnostics, runtimeQuality }) {
  const { gl } = useThree();
  useEffect(() => {
    const target = gl.domElement;
    target.dataset.runtimeQualityTier = runtimeQuality?.tier || "high";
    target.dataset.runtimeActiveSurfaces = String(runtimeDiagnostics?.activeSurfaces ?? "");
    target.dataset.runtimeContextBudget = String(runtimeDiagnostics?.contextBudget ?? "");
    target.dataset.runtimePreviewPoolActive = String(runtimeDiagnostics?.activePreviews ?? "");
    target.dataset.runtimePreviewPoolBudget = String(runtimeDiagnostics?.previewBudget ?? "");
    target.dataset.runtimePreviewCooldown = runtimeDiagnostics?.previewCooldown ? "true" : "false";
    target.dataset.runtimeContextRecoveries = String(runtimeDiagnostics?.contextLossRecoveries ?? "");
    target.dataset.runtimeRadiusOptionsLy = (runtimeDiagnostics?.supportedRadiusSteps || []).join(",");
  }, [gl.domElement, runtimeDiagnostics, runtimeQuality]);

  return null;
}

function StarMapScene({
  systems,
  selectedSystem,
  filterMatchIds,
  filterActive,
  filterLabelSystems,
  onSelect,
  onRouteContext,
  keybindScheme,
  mapFrame,
  showDirectionLabels,
  routeSegments,
  onRemoveRouteSegment,
  controlsEnabled,
  stabilizationEnabled,
  onTelemetry,
  onCameraState,
  initialCameraState,
  reticleSelectRequest,
  focusTarget,
  focusToken,
  mobileFlightIntent,
  onContextLost,
  runtimeQuality,
  runtimeDiagnostics,
}) {
  const initialPosition = Array.isArray(initialCameraState?.position) && initialCameraState.position.length === 3
    ? initialCameraState.position
    : DEFAULT_MAP_CAMERA_STATE.position;
  return (
    <Canvas
      className="map-canvas"
      camera={{ fov: 62, near: 0.01, far: 1200, position: initialPosition }}
      dpr={runtimeQuality?.mapDpr || RUNTIME_QUALITY_PROFILES.high.mapDpr}
      gl={{ antialias: true, alpha: true, preserveDrawingBuffer: false, powerPreference: "high-performance" }}
    >
      <color attach="background" args={["#01030a"]} />
      <fog attach="fog" args={["#01030a", 80, 190]} />
      <MapWebGLContextGuard onContextLost={onContextLost} />
      <MapRuntimeBridge runtimeDiagnostics={runtimeDiagnostics} runtimeQuality={runtimeQuality} />
      <DistanceRings />
      <OrientationAxes frame={mapFrame} showDirectionLabels={showDirectionLabels} />
      <StarField systems={systems} filterMatchIds={filterMatchIds} filterActive={filterActive} />
      <PriorityLabels
        systems={systems}
        selectedSystem={selectedSystem}
        onSelect={onSelect}
        forcedLabelSystems={filterLabelSystems}
        forcedLabelActive={filterActive}
      />
      <RouteOverlays segments={routeSegments} onRemoveSegment={onRemoveRouteSegment} />
      <SelectionMarker system={selectedSystem} />
      <FlightControls
        systems={systems}
        onSelect={onSelect}
        onRouteContext={onRouteContext}
        keybindScheme={keybindScheme}
        mapFrame={mapFrame}
        showDirectionLabels={showDirectionLabels}
        controlsEnabled={controlsEnabled}
        stabilizationEnabled={stabilizationEnabled}
        onTelemetry={onTelemetry}
        onCameraState={onCameraState}
        initialCameraState={initialCameraState}
        reticleSelectRequest={reticleSelectRequest}
        focusTarget={focusTarget}
        focusToken={focusToken}
        mobileFlightIntent={mobileFlightIntent}
      />
    </Canvas>
  );
}

function DualRangeControl({ label, min, max, step = 1, value, onChange, format = (item) => item }) {
  const safeMin = Number.isFinite(Number(min)) ? Number(min) : 0;
  const safeMax = Math.max(safeMin + Number(step || 1), Number.isFinite(Number(max)) ? Number(max) : safeMin + 1);
  const currentMin = Math.min(Math.max(Number(value?.[0] ?? safeMin), safeMin), safeMax);
  const currentMax = Math.max(Math.min(Number(value?.[1] ?? safeMax), safeMax), safeMin);
  const low = Math.min(currentMin, currentMax);
  const high = Math.max(currentMin, currentMax);
  const span = safeMax - safeMin || 1;
  const leftPct = ((low - safeMin) / span) * 100;
  const rightPct = 100 - ((high - safeMin) / span) * 100;
  const updateLow = (nextValue) => onChange([Math.min(Number(nextValue), high), high]);
  const updateHigh = (nextValue) => onChange([low, Math.max(Number(nextValue), low)]);
  return (
    <div className="map-search-range">
      <div className="map-search-range-head">
        <span>{label}</span>
        <strong>{format(low)} - {format(high)}</strong>
      </div>
      <div className="map-search-range-track" style={{ "--range-left": `${leftPct}%`, "--range-right": `${rightPct}%` }}>
        <input
          type="range"
          min={safeMin}
          max={safeMax}
          step={step}
          value={low}
          onChange={(event) => updateLow(event.target.value)}
          aria-label={`${label} minimum`}
        />
        <input
          type="range"
          min={safeMin}
          max={safeMax}
          step={step}
          value={high}
          onChange={(event) => updateHigh(event.target.value)}
          aria-label={`${label} maximum`}
        />
      </div>
    </div>
  );
}

function spectralTokens(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim().toUpperCase()).filter(Boolean);
  }
  return String(value || "")
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function systemMatchesMapFilters(system, filters, origin) {
  const viewpointDistance = distanceFromHelioOrigin(system, origin);
  const starCount = Number(system.star_count || 0);
  const planetCount = Number(system.planet_count || 0);
  const coolness = Number(system.coolness_score || 0);
  const minTemp = Number(system.min_star_teff_k);
  const maxTemp = Number(system.max_star_teff_k);
  const systemSpectral = spectralTokens(system.spectral_classes?.length ? system.spectral_classes : system.dominant_spectral_class);
  const activeSpectral = spectralTokens(filters.spectralClass);
  const tempLow = Number(filters.temperatureRange?.[0] ?? STAR_SEARCH_DEFAULT_TEMP_RANGE[0]);
  const tempHigh = Number(filters.temperatureRange?.[1] ?? STAR_SEARCH_DEFAULT_TEMP_RANGE[1]);
  if (viewpointDistance < filters.distanceRange[0] || viewpointDistance > filters.distanceRange[1]) return false;
  if (starCount < filters.starRange[0] || starCount > filters.starRange[1]) return false;
  if (planetCount < filters.planetRange[0] || planetCount > filters.planetRange[1]) return false;
  if (coolness < filters.coolnessRange[0] || coolness > filters.coolnessRange[1]) return false;
  if (filters.habitableOnly && !system.has_habitable_candidate) return false;
  if (activeSpectral.length && !activeSpectral.some((token) => systemSpectral.includes(token))) return false;
  if ((tempLow > STAR_SEARCH_DEFAULT_TEMP_RANGE[0] || tempHigh < STAR_SEARCH_DEFAULT_TEMP_RANGE[1])) {
    if (!Number.isFinite(minTemp) || !Number.isFinite(maxTemp)) return false;
    if (maxTemp < tempLow || minTemp > tempHigh) return false;
  }
  return true;
}

function buildSearchParamsFromFilters(filters, origin, filterExtents, query = "", sort = "distance", limit = 24) {
  const params = {
    sort: sort || (query.trim() ? "match" : "distance"),
    limit: String(limit),
    origin_x_ly: String(origin.x),
    origin_y_ly: String(origin.y),
    origin_z_ly: String(origin.z),
    origin_label: "camera",
  };
  const q = query.trim();
  if (q) params.q = q;
  if (filters.habitableOnly) params.has_habitable = "true";
  const spectral = spectralTokens(filters.spectralClass);
  if (spectral.length) params.spectral_class = spectral.join(",");
  const distLow = Math.min(Number(filters.distanceRange?.[0] ?? 0), Number(filters.distanceRange?.[1] ?? MAP_RADIUS_LY));
  const distHigh = Math.max(Number(filters.distanceRange?.[0] ?? 0), Number(filters.distanceRange?.[1] ?? MAP_RADIUS_LY));
  const starLow = Math.min(Number(filters.starRange?.[0] ?? 0), Number(filters.starRange?.[1] ?? filterExtents.maxStars));
  const starHigh = Math.max(Number(filters.starRange?.[0] ?? 0), Number(filters.starRange?.[1] ?? filterExtents.maxStars));
  const planetLow = Math.min(Number(filters.planetRange?.[0] ?? 0), Number(filters.planetRange?.[1] ?? filterExtents.maxPlanets));
  const planetHigh = Math.max(Number(filters.planetRange?.[0] ?? 0), Number(filters.planetRange?.[1] ?? filterExtents.maxPlanets));
  const coolnessLow = Math.min(Number(filters.coolnessRange?.[0] ?? 0), Number(filters.coolnessRange?.[1] ?? filterExtents.maxCoolness));
  const coolnessHigh = Math.max(Number(filters.coolnessRange?.[0] ?? 0), Number(filters.coolnessRange?.[1] ?? filterExtents.maxCoolness));
  if (distLow > 0) params.min_dist_ly = String(Math.max(0, distLow));
  if (distHigh < MAP_RADIUS_LY) params.max_dist_ly = String(distHigh);
  if (starLow > 0) params.min_star_count = String(starLow);
  if (starHigh < filterExtents.maxStars) params.max_star_count = String(starHigh);
  if (planetLow > 0) params.min_planet_count = String(planetLow);
  if (planetHigh < filterExtents.maxPlanets) params.max_planet_count = String(planetHigh);
  if (coolnessLow > 0) params.min_coolness_score = String(coolnessLow);
  if (coolnessHigh < filterExtents.maxCoolness) params.max_coolness_score = String(coolnessHigh);
  if (filters.temperatureRange[0] > STAR_SEARCH_DEFAULT_TEMP_RANGE[0]) params.min_temp_k = String(filters.temperatureRange[0]);
  if (filters.temperatureRange[1] < STAR_SEARCH_DEFAULT_TEMP_RANGE[1]) params.max_temp_k = String(filters.temperatureRange[1]);
  return params;
}

function LazyStarSearchPreview({
  system,
  displayName,
  cachedPreviewImage = "",
  liveActive = false,
  poolSlot = null,
  previewDisabledReason = "",
  runtimeQualityTier = "high",
  defaultScaleMode = "structure",
  nameStyle = "public_full",
  onActivate,
  onDeactivate,
  onCapture,
  onRuntimeEvent,
}) {
  const ref = useRef(null);
  const requestedLiveRef = useRef(false);
  const hoverIntentRef = useRef(false);
  const [visible, setVisible] = useState(false);
  const [hoverIntent, setHoverIntent] = useState(false);
  const lightweightPreview = isLightweightPreviewSystem(system);
  useEffect(() => {
    const node = ref.current;
    if (!node) {
      return undefined;
    }
    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        setVisible(Boolean(entry?.isIntersecting));
      },
      { root: null, rootMargin: "0px 0px", threshold: 0.2 }
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const wantsLive = !lightweightPreview && visible && (!cachedPreviewImage || hoverIntent);
    if (wantsLive && !requestedLiveRef.current) {
      requestedLiveRef.current = true;
      onActivate?.(system.system_id);
      return;
    }
    if (!wantsLive && requestedLiveRef.current) {
      requestedLiveRef.current = false;
      onDeactivate?.(system.system_id);
    }
  }, [cachedPreviewImage, hoverIntent, lightweightPreview, onActivate, onDeactivate, system.system_id, visible]);

  const setHovering = useCallback((nextValue) => {
    hoverIntentRef.current = nextValue;
    setHoverIntent(nextValue);
  }, []);

  const handleFrameCapture = useCallback((dataUrl) => {
    onCapture?.(system.system_id, dataUrl);
    if (!hoverIntentRef.current) {
      requestedLiveRef.current = false;
      onDeactivate?.(system.system_id);
    }
  }, [onCapture, onDeactivate, system.system_id]);

  const showLivePreview = visible && liveActive;
  const showCachedPreview = Boolean(cachedPreviewImage) && !showLivePreview;
  if (lightweightPreview) {
    return (
      <div
        ref={ref}
        className="map-search-card-preview is-lightweight"
        data-preview-state="lightweight"
        data-preview-tier={system?.preview_tier || "lightweight_singleton"}
        tabIndex={0}
      >
        <LightweightSystemPreview system={system} displayName={displayName} />
      </div>
    );
  }

  return (
    <div
      ref={ref}
      className={`map-search-card-preview ${showLivePreview ? "is-live" : ""} ${showCachedPreview ? "is-cached" : ""}`}
      data-preview-state={showLivePreview ? "live" : showCachedPreview ? "cached" : (visible && previewDisabledReason ? "paused" : "queued")}
      data-preview-tier={system?.preview_tier || "dynamic_simulation_scene"}
      data-preview-pool-slot={poolSlot ?? ""}
      onMouseEnter={() => setHovering(true)}
      onMouseLeave={() => setHovering(false)}
      onFocus={() => setHovering(true)}
      onBlur={() => setHovering(false)}
      tabIndex={0}
    >
      {showLivePreview ? (
        <React.Suspense fallback={<div className="map-search-card-fallback">Loading preview</div>}>
          <SystemPreviewPanel
            key={`search-preview:${poolSlot ?? system.system_id}:${system.system_id}:${normalizeNameStyle(nameStyle)}`}
            systemId={system.system_id}
            systemName={displayName}
            snapshot={system.snapshot}
            presentationMode="card"
            autoRun={false}
            qualityTier={runtimeQualityTier}
            captureFrame={!cachedPreviewImage}
            onFrameCapture={handleFrameCapture}
            onRuntimeEvent={onRuntimeEvent}
            defaultScaleMode={defaultScaleMode}
            nameStyle={normalizeNameStyle(nameStyle)}
          />
        </React.Suspense>
      ) : showCachedPreview ? (
        <>
          <img className="map-search-card-capture" src={cachedPreviewImage} alt={`${displayName} cached System Simulation preview`} />
          <span className="map-search-card-preview-chip">{hoverIntent ? "Live queued" : "Hover to animate"}</span>
        </>
      ) : (
        <div className="map-search-card-fallback">{visible ? (previewDisabledReason || "Preview queued") : "Loading preview"}</div>
      )}
    </div>
  );
}

function MapStarSearchShell({
  open,
  systems,
  selectedSystem,
  selectionHistory,
  suggestedNeighbors,
  filters,
  filterExtents,
  matchedCount,
  query,
  setQuery,
  sort,
  onSortChange,
  setFilters,
  onSubmitSearch,
  onCloseResults,
  onSelectSystem,
  onExploreSystem,
  results,
  resultsOpen,
  loading,
  error,
  hasMore,
  onLoadMore,
  searchStats,
  previewSnapshotCache,
  previewPoolAllocations,
  previewPoolBudget,
  previewRuntimeQualityTier,
  defaultScaleMode = "structure",
  nameStyle = "public_full",
  previewPaused,
  previewCooldownActive,
  onRequestPreview,
  onReleasePreview,
  onCapturePreview,
  onRuntimeEvent,
}) {
  const updateRange = (key, value) => setFilters((current) => ({ ...current, [key]: value.map((item) => Math.round(Number(item))) }));
  const toggleSpectral = (token) => {
    setFilters((current) => {
      const active = new Set(spectralTokens(current.spectralClass));
      if (active.has(token)) active.delete(token);
      else active.add(token);
      return { ...current, spectralClass: STAR_SEARCH_SPECTRAL_OPTIONS.filter((item) => active.has(item)).join(",") };
    });
  };
  const resetFilters = () => setFilters({
    distanceRange: [0, MAP_RADIUS_LY],
    starRange: [0, filterExtents.maxStars],
    planetRange: [0, filterExtents.maxPlanets],
    coolnessRange: [0, filterExtents.maxCoolness],
    temperatureRange: STAR_SEARCH_DEFAULT_TEMP_RANGE,
    spectralClass: "",
    habitableOnly: false,
  });
  const activeSpectral = new Set(spectralTokens(filters.spectralClass));
  const hasQuery = Boolean(query.trim());
  const selectedSort = !hasQuery && sort === "match" ? "distance" : sort;

  const previewAllocationsBySystemId = useMemo(() => {
    const out = new Map();
    (previewPoolAllocations || []).forEach((allocation, index) => {
      out.set(String(allocation.systemId), { ...allocation, slot: allocation.slot ?? index });
    });
    return out;
  }, [previewPoolAllocations]);

  return (
    <section className={`map-star-search ${open ? "is-open" : ""}`} aria-label="Map-native Star Search">
      <form className="map-search-topbar" onSubmit={onSubmitSearch}>
        <label className="map-search-main">
          <span className="sr-only">Search systems</span>
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search stars, systems, or catalog IDs..."
            data-testid="map-star-search-input"
          />
        </label>
        <button type="submit" className="map-command-button primary" disabled={loading}>
          {loading ? "Searching" : "Search"}
        </button>
        {resultsOpen && (
          <button type="button" className="map-command-button ghost" onClick={onCloseResults}>
            Close Results
          </button>
        )}
        <div className="map-search-spectral-bar" role="group" aria-label="Spectral class filter">
          {STAR_SEARCH_SPECTRAL_OPTIONS.map((token) => (
            <button
              key={token}
              type="button"
              className={`map-search-spectral spectral-${token.toLowerCase()} ${activeSpectral.has(token) ? "active" : ""}`}
              onClick={() => toggleSpectral(token)}
              aria-pressed={activeSpectral.has(token)}
              aria-label={`${token} spectral class filter`}
              title={stellarClassTooltip(token)}
            >
              {token}
            </button>
          ))}
        </div>
        <DualRangeControl
          label="Temp K"
          min={STAR_SEARCH_DEFAULT_TEMP_RANGE[0]}
          max={filterExtents.maxTemp}
          step={100}
          value={filters.temperatureRange}
          onChange={(value) => updateRange("temperatureRange", value)}
          format={(value) => formatNumber(value, 0)}
        />
      </form>

      <aside className="map-search-sidebar">
        <div className="map-search-sidebar-head">
          <span className="map-panel-label">Filters</span>
          <strong>{formatNumber(matchedCount, 0)} labeled</strong>
        </div>
        <DualRangeControl label="Distance" min={0} max={MAP_RADIUS_LY} step={1} value={filters.distanceRange} onChange={(value) => updateRange("distanceRange", value)} format={(value) => `${formatNumber(value, 0)} ly`} />
        <DualRangeControl label="Stars" min={0} max={filterExtents.maxStars} step={1} value={filters.starRange} onChange={(value) => updateRange("starRange", value)} format={(value) => formatNumber(value, 0)} />
        <DualRangeControl label="Planets" min={0} max={filterExtents.maxPlanets} step={1} value={filters.planetRange} onChange={(value) => updateRange("planetRange", value)} format={(value) => formatNumber(value, 0)} />
        <DualRangeControl label="Coolness" min={0} max={filterExtents.maxCoolness} step={1} value={filters.coolnessRange} onChange={(value) => updateRange("coolnessRange", value)} format={(value) => formatNumber(value, 0)} />
        <button
          type="button"
          className={`map-search-habitable ${filters.habitableOnly ? "active" : ""}`}
          onClick={() => setFilters((current) => ({ ...current, habitableOnly: !current.habitableOnly }))}
          aria-pressed={filters.habitableOnly}
          title="Filters to systems with a planet candidate in the broad habitable-zone temperature and mass range."
        >
          Habitable-zone planets
        </button>
        <button type="button" className="map-command-button ghost map-search-reset" onClick={resetFilters}>
          Reset filters
        </button>
        <details className="map-search-recents" open>
          <summary><span className="map-panel-label">Recents</span></summary>
          <div>
            {(selectionHistory || []).slice(0, 6).map((system) => (
              <button key={system.system_id} type="button" className="map-search-recent-pill" onClick={() => onSelectSystem(system)}>
                <SystemNameDisplay system={system} showCopyButton={false} showInfoButton={false} enablePopover={false} />
                <span>{formatNumber(system.dist_ly, 1)} ly</span>
              </button>
            ))}
          </div>
        </details>
        {selectedSystem && suggestedNeighbors?.length > 0 && (
          <details className="map-search-recents" open>
            <summary><span className="map-panel-label">Cool Stars Nearby</span></summary>
            <div>
              {suggestedNeighbors.slice(0, 6).map(({ system, routeDistance }) => (
                <button key={system.system_id} type="button" className="map-search-recent-pill" onClick={() => onSelectSystem(system)}>
                  <SystemNameDisplay system={system} showCopyButton={false} showInfoButton={false} enablePopover={false} />
                  <span>{formatNumber(routeDistance, 1)} ly</span>
                </button>
              ))}
            </div>
          </details>
        )}
      </aside>

      {resultsOpen && (
        <section className="map-search-results" data-testid="map-star-search-results" aria-label="Star Search results">
          <div className="map-search-results-head">
            <div>
              <span className="map-panel-label">Results</span>
              <strong>{searchStats || `${formatNumber(results.length, 0)} systems`}</strong>
            </div>
            <label className="map-search-sort">
              <span>Sort</span>
              <select value={selectedSort} onChange={(event) => onSortChange(event.target.value)} disabled={loading}>
                {STAR_SEARCH_SORT_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value} disabled={option.value === "match" && !hasQuery}>{option.label}</option>
                ))}
              </select>
            </label>
            <button type="button" className="map-command-button ghost" onClick={onCloseResults}>Close</button>
          </div>
          {error && <div className="map-search-error">{error}</div>}
          {loading && results.length === 0 && <div className="map-search-empty">Searching...</div>}
          {!loading && !error && results.length === 0 && <div className="map-search-empty">No systems match the current search.</div>}
          <div className="map-search-card-list">
            {results.map((system) => {
              const displayName = systemDisplayName(system);
              const originDistance = Number(system.origin_distance_ly);
              return (
                <article
                  key={system.system_id}
                  className="map-search-card"
                  title="Open Explorer view for this system"
                  onClick={(event) => {
                    if (event.defaultPrevented || event.button !== 0) {
                      return;
                    }
                    if (event.target instanceof Element) {
                      const interactive = event.target.closest("a, button, input, select, textarea, label, .map-search-card-preview");
                      if (interactive) {
                        return;
                      }
                    }
                    onExploreSystem(system);
                  }}
                >
                  <LazyStarSearchPreview
                    system={system}
                    displayName={displayName}
                    cachedPreviewImage={previewSnapshotCache?.get(`${String(system.system_id)}:${normalizeNameStyle(nameStyle)}`)?.url || ""}
                    liveActive={previewAllocationsBySystemId.has(String(system.system_id))}
                    poolSlot={previewAllocationsBySystemId.get(String(system.system_id))?.slot ?? null}
                    previewDisabledReason={
                      previewPaused
                        ? "Preview paused while inspecting"
                        : previewCooldownActive
                          ? "Preview cooling down"
                          : (previewPoolBudget <= 0 ? "Preview budget full" : "")
                    }
                    runtimeQualityTier={previewRuntimeQualityTier}
                    defaultScaleMode={defaultScaleMode}
                    nameStyle={nameStyle}
                    onActivate={onRequestPreview}
                    onDeactivate={onReleasePreview}
                    onCapture={onCapturePreview}
                    onRuntimeEvent={onRuntimeEvent}
                  />
                  <div className="map-search-card-body">
                    <h3>{displayName}</h3>
                    <StellarClassChips tokens={stellarClassTokensFromSystem(system)} size="compact" className="map-search-stellar-tags" />
                    <div className="map-search-card-metrics">
                      <span>{formatNumber(system.dist_ly, 2)} ly Sol</span>
                      {Number.isFinite(originDistance) && <span>{formatNumber(originDistance, 2)} ly view</span>}
                      <span>{formatNumber(system.star_count, 0)} stars</span>
                      <span>{formatNumber(system.planet_count, 0)} planets</span>
                      <span>cool {formatNumber(system.coolness_score, 1)}</span>
                    </div>
                    <div className="map-search-card-actions">
                      <button type="button" className="map-command-button primary" onClick={() => onSelectSystem(system)}>Peek</button>
                      <button type="button" className="map-command-button ghost" onClick={() => onExploreSystem(system)}>Explore</button>
                      <Link className="map-command-button ghost" to={`/systems/${system.system_id}`}>Detail</Link>
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
          {hasMore && (
            <button type="button" className="map-command-button ghost map-search-load-more" onClick={onLoadMore} disabled={loading}>
              {loading ? "Loading" : "Load more"}
            </button>
          )}
        </section>
      )}
    </section>
  );
}

export default function StarMapPage({
  buildId = "",
  theme,
  setTheme,
  themeOptions = [],
  defaultSearchOpen = false,
  defaultScaleMode = "structure",
  setDefaultScaleMode = () => {},
  nameStyle = "public_full",
  setNameStyle = () => {},
}) {
  const location = useLocation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const restoreStateRef = useRef(readStoredMapReturnState(searchParams.get("restore")));
  const restoredMapState = restoreStateRef.current;
  const [publicConfig, setPublicConfig] = useState(PUBLIC_CONFIG_FALLBACK);
  const [rawSystems, setRawSystems] = useState([]);
  const [systems, setSystems] = useState([]);
  const [summary, setSummary] = useState(null);
  const [selectedSystem, setSelectedSystem] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const controlsEnabled = true;
  const stabilizationEnabled = true;
  const [reticleSelectRequest, setReticleSelectRequest] = useState(0);
  const [telemetry, setTelemetry] = useState({
    distLy: 0,
    speedLyS: 0,
    locked: false,
    cameraScenePosition: restoredMapState?.camera?.position || [0, 3.5, 17],
    yaw: Number.isFinite(Number(restoredMapState?.camera?.yaw)) ? Number(restoredMapState.camera.yaw) : DEFAULT_MAP_CAMERA_STATE.yaw,
    pitch: Number.isFinite(Number(restoredMapState?.camera?.pitch)) ? Number(restoredMapState.camera.pitch) : DEFAULT_MAP_CAMERA_STATE.pitch,
    originLy: sceneToHelioCoordinates(restoredMapState?.camera?.position || [0, 3.5, 17], restoredMapState?.mapFrame || "icrs"),
  });
  const [fullscreenAvailable, setFullscreenAvailable] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [fullscreenEpoch, setFullscreenEpoch] = useState(0);
  const [mapContextEpoch, setMapContextEpoch] = useState(0);
  const [mapContextRecovering, setMapContextRecovering] = useState(false);
  const [mapRecoveryCameraState, setMapRecoveryCameraState] = useState(restoredMapState?.camera || DEFAULT_MAP_CAMERA_STATE);
  const [routeSegments, setRouteSegments] = useState([]);
  const [routeMenu, setRouteMenu] = useState(null);
  const [selectionHistory, setSelectionHistory] = useState([]);
  const [drillMode, setDrillMode] = useState(() => (
    ["peek", "explore"].includes(restoredMapState?.drillMode) ? restoredMapState.drillMode : "flight"
  ));
  const [minimalMode, setMinimalMode] = useState(false);
  const [minimalNotice, setMinimalNotice] = useState(false);
  const [drillStellarClassEntries, setDrillStellarClassEntries] = useState([]);
  const [focusToken, setFocusToken] = useState(0);
  const [mobileFlightIntent, setMobileFlightIntent] = useState(DEFAULT_MOBILE_FLIGHT_STATE);
  const [peekSize, setPeekSize] = useState(() => clampMapPeekSize(restoredMapState?.peekSize || readStoredMapPeekSize()));
  const [keybindScheme, setKeybindScheme] = useState(readStoredMapKeybindScheme);
  const [mapFrame, setMapFrame] = useState(() => (
    MAP_FRAME_OPTIONS[restoredMapState?.mapFrame] ? restoredMapState.mapFrame : readStoredMapFrame()
  ));
  const [showDirectionLabels, setShowDirectionLabels] = useState(() => (
    typeof restoredMapState?.showDirectionLabels === "boolean"
      ? restoredMapState.showDirectionLabels
      : readStoredDirectionLabelsEnabled()
  ));
  const [showFpsOverlay, setShowFpsOverlay] = useState(readStoredFpsOverlayEnabled);
  const [fpsSample, setFpsSample] = useState(0);
  const [deviceRuntimeProfile, setDeviceRuntimeProfile] = useState(readDeviceRuntimeProfile);
  const [contextLossRecoveries, setContextLossRecoveries] = useState(0);
  const [previewPoolAllocations, setPreviewPoolAllocations] = useState([]);
  const [previewCooldownActive, setPreviewCooldownActive] = useState(false);
  const [previewSnapshotCache, setPreviewSnapshotCache] = useState(() => new Map());
  const [mapSearchQuery, setMapSearchQuery] = useState(() => searchParams.get("q") || "");
  const [mapSearchSort, setMapSearchSort] = useState(() => searchParams.get("sort") || (searchParams.get("q") ? "match" : "distance"));
  const [mapSearchFilters, setMapSearchFilters] = useState({
    distanceRange: [0, MAP_RADIUS_LY],
    starRange: [0, 1],
    planetRange: [0, 1],
    coolnessRange: [0, 1],
    temperatureRange: STAR_SEARCH_DEFAULT_TEMP_RANGE,
    spectralClass: "",
    habitableOnly: false,
  });
  const [mapSearchResults, setMapSearchResults] = useState([]);
  const [mapSearchResultsOpen, setMapSearchResultsOpen] = useState(false);
  const [mapSearchLoading, setMapSearchLoading] = useState(false);
  const [mapSearchError, setMapSearchError] = useState("");
  const [mapSearchCursor, setMapSearchCursor] = useState(null);
  const [mapSearchHasMore, setMapSearchHasMore] = useState(false);
  const [mapSearchStats, setMapSearchStats] = useState("");
  const mapSearchTokenRef = useRef(0);
  const mapNameStyleInitializedRef = useRef(false);
  const mapCameraStateRef = useRef(restoredMapState?.camera || DEFAULT_MAP_CAMERA_STATE);
  const previewPoolIdsRef = useRef(new Set());
  const previewRequestQueueRef = useRef([]);
  const previewActivationTimerRef = useRef(null);
  const previewCooldownTimerRef = useRef(null);
  const pageRef = useRef(null);
  const headerMenuRef = useRef(null);
  const drillHistoryPushedRef = useRef(false);
  const mapTitle = publicConfig?.map_title || PUBLIC_CONFIG_FALLBACK.map_title;
  const spacegateUrl = publicConfig?.spacegate_url || PUBLIC_CONFIG_FALLBACK.spacegate_url;
  const activeKeybind = MAP_KEYBIND_SCHEMES[keybindScheme] || MAP_KEYBIND_SCHEMES.wasd;
  const mapSearchOpen = defaultSearchOpen || location.pathname === "/" || location.pathname === "/search";
  const mapSurfaceActive = systems.length > 0;
  const drillSurfaceActive = drillMode !== "flight" && Boolean(selectedSystem?.system_id);
  const activeWebGLSurfaceCount = (mapSurfaceActive ? 1 : 0) + (drillSurfaceActive ? 1 : 0) + previewPoolAllocations.length;
  const runtimeQuality = useMemo(() => runtimeQualityFor({
    activeSurfaces: activeWebGLSurfaceCount,
    contextLossRecoveries,
    deviceProfile: deviceRuntimeProfile,
  }), [activeWebGLSurfaceCount, contextLossRecoveries, deviceRuntimeProfile]);
  const previewRecoveryBudget = contextLossRecoveries >= 6
    ? SEARCH_PREVIEW_HIGH_RECOVERY_BUDGET
    : runtimeQuality.cardBudget;
  const reservedWebGLSurfaces = (mapSurfaceActive ? 1 : 0) + (drillSurfaceActive ? 1 : 0);
  const availablePreviewSlots = Math.max(0, WEBGL_CONTEXT_BUDGET - reservedWebGLSurfaces);
  const desiredPreviewBudget = drillSurfaceActive ? 1 : SEARCH_PREVIEW_POOL_SIZE;
  const previewPoolBudget = mapContextRecovering || previewCooldownActive
    ? 0
    : Math.min(desiredPreviewBudget, previewRecoveryBudget, availablePreviewSlots);
  const previewPaused = drillSurfaceActive && previewPoolBudget <= 0;
  const runtimeDiagnostics = {
    fps: fpsSample,
    activeSurfaces: activeWebGLSurfaceCount,
    contextBudget: WEBGL_CONTEXT_BUDGET,
    activePreviews: previewPoolAllocations.length,
    previewBudget: previewPoolBudget,
    previewCooldown: previewCooldownActive,
    contextLossRecoveries,
    qualityTier: runtimeQuality.tier,
    mapRadiusLy: MAP_RADIUS_LY,
    supportedRadiusSteps: MAP_RADIUS_OPTIONS_LY,
  };

  const clearPreviewActivationTimer = useCallback(() => {
    if (previewActivationTimerRef.current) {
      window.clearTimeout(previewActivationTimerRef.current);
      previewActivationTimerRef.current = null;
    }
  }, []);

  const enterPreviewCooldown = useCallback(() => {
    previewRequestQueueRef.current = [];
    clearPreviewActivationTimer();
    setPreviewPoolAllocations([]);
    setPreviewCooldownActive(true);
    if (previewCooldownTimerRef.current) {
      window.clearTimeout(previewCooldownTimerRef.current);
    }
    previewCooldownTimerRef.current = window.setTimeout(() => {
      previewCooldownTimerRef.current = null;
      setPreviewCooldownActive(false);
    }, SEARCH_PREVIEW_CONTEXT_COOLDOWN_MS);
  }, [clearPreviewActivationTimer]);

  const handleMapCameraState = useCallback((state) => {
    if (!state || !Array.isArray(state.position) || state.position.length !== 3) {
      return;
    }
    mapCameraStateRef.current = {
      position: state.position.map((value) => Number(value) || 0),
      yaw: Number.isFinite(Number(state.yaw)) ? Number(state.yaw) : DEFAULT_MAP_CAMERA_STATE.yaw,
      pitch: Number.isFinite(Number(state.pitch)) ? Number(state.pitch) : DEFAULT_MAP_CAMERA_STATE.pitch,
      mapFrame: state.mapFrame || mapFrame,
    };
  }, [mapFrame]);

  const handleMapTelemetry = useCallback((nextTelemetry) => {
    setTelemetry(nextTelemetry);
    handleMapCameraState({
      position: nextTelemetry?.cameraScenePosition,
      yaw: nextTelemetry?.yaw,
      pitch: nextTelemetry?.pitch,
      mapFrame,
    });
  }, [handleMapCameraState, mapFrame]);

  const handleMapContextLost = useCallback((cameraState = null) => {
    const storedState = mapCameraStateRef.current;
    const datasetPosition = parseMapCameraDatasetPosition(cameraState?.datasetPosition);
    const datasetState = datasetPosition
      ? {
        position: datasetPosition,
        yaw: Number.isFinite(Number(cameraState?.yaw)) ? Number(cameraState.yaw) : storedState.yaw,
        pitch: Number.isFinite(Number(cameraState?.pitch)) ? Number(cameraState.pitch) : storedState.pitch,
        mapFrame,
      }
      : null;
    const eventLooksDefault = cameraStateDistance(cameraState, DEFAULT_MAP_CAMERA_STATE) < 0.01;
    const storedLooksMoved = cameraStateDistance(storedState, DEFAULT_MAP_CAMERA_STATE) > 0.01;
    const datasetLooksMoved = cameraStateDistance(datasetState, DEFAULT_MAP_CAMERA_STATE) > 0.01;
    const restoreState = datasetLooksMoved
      ? datasetState
      : eventLooksDefault && storedLooksMoved ? storedState : cameraState;
    handleMapCameraState(restoreState);
    setMapRecoveryCameraState(restoreState || storedState || DEFAULT_MAP_CAMERA_STATE);
    setContextLossRecoveries((value) => value + 1);
    enterPreviewCooldown();
    setMapContextRecovering(true);
    setMapContextEpoch((value) => value + 1);
    window.setTimeout(() => setMapContextRecovering(false), 1200);
  }, [enterPreviewCooldown, handleMapCameraState, mapFrame]);
  const handleRuntimeEvent = useCallback((event) => {
    if (event?.type === "webgl-context-lost") {
      setContextLossRecoveries((value) => value + 1);
      if (event.surface === "system-preview" && event.presentationMode === "card") {
        enterPreviewCooldown();
      }
    }
  }, [enterPreviewCooldown]);

  const enterMinimalMode = useCallback(() => {
    setMinimalMode(true);
    setMinimalNotice(true);
    window.setTimeout(() => setMinimalNotice(false), 3200);
  }, []);

  const exitMinimalMode = useCallback(() => {
    setMinimalMode(false);
    setMinimalNotice(false);
  }, []);

  const toggleMinimalMode = useCallback(() => {
    if (minimalMode) {
      exitMinimalMode();
    } else {
      enterMinimalMode();
    }
  }, [enterMinimalMode, exitMinimalMode, minimalMode]);

  useEffect(() => {
    let cancelled = false;
    fetchPublicConfig()
      .then((payload) => {
        if (cancelled) {
          return;
        }
        setPublicConfig({
          site_name: payload?.site_name || PUBLIC_CONFIG_FALLBACK.site_name,
          map_title: payload?.map_title || PUBLIC_CONFIG_FALLBACK.map_title,
          spacegate_url: payload?.spacegate_url || PUBLIC_CONFIG_FALLBACK.spacegate_url,
        });
      })
      .catch(() => {
        if (!cancelled) {
          setPublicConfig(PUBLIC_CONFIG_FALLBACK);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    try {
      window.sessionStorage.setItem(MAP_PEEK_SIZE_STORAGE_KEY, JSON.stringify(peekSize));
    } catch {
      // Session persistence is a convenience; the default size remains usable.
    }
  }, [peekSize]);

  useEffect(() => {
    try {
      window.localStorage.setItem(MAP_KEYBIND_STORAGE_KEY, keybindScheme);
    } catch {
      // Control preference persistence is optional.
    }
  }, [keybindScheme]);

  useEffect(() => {
    try {
      window.localStorage.setItem(MAP_FRAME_STORAGE_KEY, mapFrame);
    } catch {
      // Map frame persistence is optional.
    }
  }, [mapFrame]);

  useEffect(() => {
    try {
      window.localStorage.setItem(MAP_DIRECTION_LABELS_STORAGE_KEY, showDirectionLabels ? "true" : "false");
    } catch {
      // Direction-label preference persistence is optional.
    }
  }, [showDirectionLabels]);

  useEffect(() => {
    const updateProfile = () => setDeviceRuntimeProfile(readDeviceRuntimeProfile());
    updateProfile();
    window.addEventListener("resize", updateProfile);
    return () => window.removeEventListener("resize", updateProfile);
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(MAP_FPS_OVERLAY_STORAGE_KEY, showFpsOverlay ? "true" : "false");
    } catch {
      // FPS overlay preference persistence is optional.
    }
  }, [showFpsOverlay]);

  useEffect(() => {
    previewPoolIdsRef.current = new Set(previewPoolAllocations.map((allocation) => String(allocation.systemId)));
  }, [previewPoolAllocations]);

  useEffect(() => () => {
    clearPreviewActivationTimer();
    if (previewCooldownTimerRef.current) {
      window.clearTimeout(previewCooldownTimerRef.current);
      previewCooldownTimerRef.current = null;
    }
  }, [clearPreviewActivationTimer]);

  useEffect(() => {
    if (!mapSearchResultsOpen || previewPoolBudget <= 0) {
      previewRequestQueueRef.current = [];
      clearPreviewActivationTimer();
      setPreviewPoolAllocations([]);
      return;
    }
    const resultIds = new Set(mapSearchResults.map((system) => String(system.system_id)));
    setPreviewPoolAllocations((current) => current
      .filter((allocation) => resultIds.has(String(allocation.systemId)))
      .slice(-previewPoolBudget)
      .map((allocation, index) => ({ ...allocation, slot: index })));
    previewRequestQueueRef.current = previewRequestQueueRef.current.filter((systemId) => resultIds.has(String(systemId)));
  }, [clearPreviewActivationTimer, mapSearchResults, mapSearchResultsOpen, previewPoolBudget]);

  const activateSearchPreviewNow = useCallback((systemId) => {
    if (previewPoolBudget <= 0) {
      return;
    }
    setPreviewPoolAllocations((current) => {
      const next = current.filter((allocation) => String(allocation.systemId) !== String(systemId));
      next.push({ systemId, requestedAt: performance.now() });
      return next.slice(-previewPoolBudget).map((allocation, index) => ({ ...allocation, slot: index }));
    });
  }, [previewPoolBudget]);

  const schedulePreviewActivation = useCallback(() => {
    if (previewActivationTimerRef.current || previewPoolBudget <= 0) {
      return;
    }
    previewActivationTimerRef.current = window.setTimeout(() => {
      previewActivationTimerRef.current = null;
      if (previewPoolBudget <= 0) {
        previewRequestQueueRef.current = [];
        return;
      }
      const nextSystemId = previewRequestQueueRef.current.shift();
      if (nextSystemId) {
        activateSearchPreviewNow(nextSystemId);
      }
      if (previewRequestQueueRef.current.length > 0) {
        schedulePreviewActivation();
      }
    }, SEARCH_PREVIEW_ACTIVATION_INTERVAL_MS);
  }, [activateSearchPreviewNow, previewPoolBudget]);

  const requestSearchPreview = useCallback((systemId) => {
    if (previewPoolBudget <= 0 || !systemId) {
      return;
    }
    const key = String(systemId);
    if (previewPoolIdsRef.current.has(key) || previewRequestQueueRef.current.includes(key)) {
      return;
    }
    previewRequestQueueRef.current.push(key);
    schedulePreviewActivation();
  }, [previewPoolBudget, schedulePreviewActivation]);

  const releaseSearchPreview = useCallback((systemId) => {
    const key = String(systemId);
    previewRequestQueueRef.current = previewRequestQueueRef.current.filter((queuedId) => String(queuedId) !== key);
    setPreviewPoolAllocations((current) => current
      .filter((allocation) => String(allocation.systemId) !== key)
      .map((allocation, index) => ({ ...allocation, slot: index })));
  }, []);

  const captureSearchPreview = useCallback((systemId, dataUrl) => {
    const key = `${String(systemId || "")}:${normalizeNameStyle(nameStyle)}`;
    if (!key || typeof dataUrl !== "string" || !dataUrl.startsWith("data:image/")) {
      return;
    }
    setPreviewSnapshotCache((current) => {
      if (current.get(key)?.url === dataUrl) {
        return current;
      }
      const next = new Map(current);
      next.set(key, { url: dataUrl, capturedAt: performance.now() });
      while (next.size > SEARCH_PREVIEW_SNAPSHOT_CACHE_LIMIT) {
        const oldestKey = next.keys().next().value;
        if (!oldestKey) {
          break;
        }
        next.delete(oldestKey);
      }
      return next;
    });
  }, [nameStyle]);

  useEffect(() => {
    if (!showFpsOverlay) {
      setFpsSample(0);
      return undefined;
    }
    let active = true;
    let lastCommit = performance.now();
    let frameCount = 0;
    let rafId = 0;
    const tick = (now) => {
      if (!active) {
        return;
      }
      frameCount += 1;
      if (now - lastCommit >= 500) {
        const elapsedSeconds = Math.max(0.001, (now - lastCommit) / 1000);
        setFpsSample(Math.round(frameCount / elapsedSeconds));
        frameCount = 0;
        lastCommit = now;
      }
      rafId = window.requestAnimationFrame(tick);
    };
    rafId = window.requestAnimationFrame(tick);
    return () => {
      active = false;
      window.cancelAnimationFrame(rafId);
    };
  }, [showFpsOverlay]);

  useEffect(() => {
    const onPointerDown = (event) => {
      const menu = headerMenuRef.current;
      if (!menu?.open || menu.contains(event.target)) {
        return;
      }
      menu.open = false;
    };
    window.addEventListener("pointerdown", onPointerDown, true);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown, true);
    };
  }, []);

  const beginPeekResize = useCallback((event) => {
    if (drillMode !== "peek") {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    const start = {
      x: event.clientX,
      y: event.clientY,
      width: peekSize.width,
      height: peekSize.height,
    };
    const onPointerMove = (moveEvent) => {
      setPeekSize(clampMapPeekSize({
        width: start.width - (moveEvent.clientX - start.x),
        height: start.height - (moveEvent.clientY - start.y),
      }));
    };
    const onPointerUp = () => {
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
      window.removeEventListener("pointercancel", onPointerUp);
    };
    window.addEventListener("pointermove", onPointerMove, { passive: true });
    window.addEventListener("pointerup", onPointerUp, { passive: true });
    window.addEventListener("pointercancel", onPointerUp, { passive: true });
  }, [drillMode, peekSize.height, peekSize.width]);

  const exitDrillMode = useCallback((consumeHistory = true) => {
    setDrillMode("flight");
    if (consumeHistory && drillHistoryPushedRef.current) {
      window.history.back();
    }
  }, []);

  const backToPeekFromExplore = useCallback(() => {
    drillHistoryPushedRef.current = false;
    setDrillMode("peek");
  }, []);

  const selectSystem = useCallback((system, options = {}) => {
    if (!system) {
      setSelectedSystem(null);
      exitDrillMode();
      return;
    }
    setSelectedSystem(system);
    setDrillStellarClassEntries([]);
    setSelectionHistory((history) => [
      system,
      ...history.filter((item) => item.system_id !== system.system_id),
    ].slice(0, 8));
    if (options.openPeek) {
      setDrillMode("peek");
    }
    if (options.focus) {
      setFocusToken((value) => value + 1);
    }
  }, [exitDrillMode]);

  useEffect(() => {
    if (drillMode !== "explore" || drillHistoryPushedRef.current) {
      return;
    }
    const currentState = window.history.state && typeof window.history.state === "object"
      ? window.history.state
      : {};
    window.history.pushState({ ...currentState, spacegateMapDrill: "explore" }, "", window.location.href);
    drillHistoryPushedRef.current = true;
  }, [drillMode]);

  useEffect(() => {
    const onPopState = () => {
      if (!drillHistoryPushedRef.current) {
        return;
      }
      drillHistoryPushedRef.current = false;
      setDrillMode("flight");
    };
    window.addEventListener("popstate", onPopState);
    return () => {
      window.removeEventListener("popstate", onPopState);
    };
  }, []);

  useEffect(() => {
    let fullscreenFrame = 0;
    const updateFullscreenState = () => {
      setFullscreenAvailable(Boolean(document.fullscreenEnabled && pageRef.current?.requestFullscreen));
      setIsFullscreen(document.fullscreenElement === pageRef.current);
      if (fullscreenFrame) {
        window.cancelAnimationFrame(fullscreenFrame);
      }
      fullscreenFrame = window.requestAnimationFrame(() => setFullscreenEpoch((value) => value + 1));
    };
    updateFullscreenState();
    document.addEventListener("fullscreenchange", updateFullscreenState);
    return () => {
      document.removeEventListener("fullscreenchange", updateFullscreenState);
      if (fullscreenFrame) {
        window.cancelAnimationFrame(fullscreenFrame);
      }
    };
  }, []);

  useEffect(() => {
    const onContextMenu = (event) => {
      if (window.__spacegateMapSuppressNextContextMenu) {
        event.preventDefault();
        event.stopPropagation();
        window.__spacegateMapSuppressNextContextMenu = false;
        return;
      }
      const inContextMenu = event.target?.closest?.(".map-context-menu");
      if (inContextMenu) {
        event.preventDefault();
        return;
      }
      if (routeMenu) {
        event.preventDefault();
        event.stopPropagation();
        setRouteMenu(null);
        return;
      }
      const inSystemDrill = event.target?.closest?.(".map-system-drill");
      if (drillMode === "peek" && !inSystemDrill) {
        event.preventDefault();
        event.stopPropagation();
        exitDrillMode(false);
      }
    };
    window.addEventListener("contextmenu", onContextMenu, true);
    return () => {
      window.removeEventListener("contextmenu", onContextMenu, true);
    };
  }, [drillMode, exitDrillMode, routeMenu]);

  useEffect(() => {
    const onPointerDown = (event) => {
      if (drillMode === "flight" || event.button !== 0) {
        return;
      }
      const target = event.target;
      const inSystemDrill = target?.closest?.(".map-system-drill");
      const inMapHud = target?.closest?.(
        ".map-hud-top, .map-star-search, .map-context-menu, .map-controls-panel, .map-status-panel, .map-return-banner, .map-minimal-toggle"
      );
      if (inSystemDrill || inMapHud) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation?.();
      exitDrillMode(drillMode === "explore");
    };
    window.addEventListener("pointerdown", onPointerDown, true);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown, true);
    };
  }, [drillMode, exitDrillMode]);

  useEffect(() => {
    if (!rawSystems.length) {
      setSystems([]);
      return;
    }
    const prepared = prepareMapItems(rawSystems, mapFrame);
    const byId = new Map(prepared.map((system) => [system.system_id, system]));
    const refreshSystem = (system) => byId.get(system?.system_id) || system || null;
    setSystems(prepared);
    setSelectedSystem((system) => refreshSystem(system));
    setSelectionHistory((history) => history.map(refreshSystem).filter(Boolean).slice(0, 8));
    setRouteSegments((segments) => segments.map((segment) => ({
      ...segment,
      from: refreshSystem(segment.from),
      to: refreshSystem(segment.to),
    })).filter((segment) => segment.from && segment.to));
    setRouteMenu((menu) => (menu?.target ? { ...menu, target: refreshSystem(menu.target) } : menu));
  }, [mapFrame, rawSystems]);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    fetchMapSystems({
      max_dist_ly: String(MAP_RADIUS_LY),
      limit: "20000",
      compact: "true",
      name_style: normalizeNameStyle(nameStyle),
    })
      .then((payload) => {
        if (!active) {
          return;
        }
        const rows = payload?.items || [];
        const prepared = prepareMapItems(rows, mapFrame);
        setRawSystems(rows);
        setSummary(payload || null);
        const restoredSystem = restoredMapState?.selectedSystemId
          ? prepared.find((item) => String(item.system_id) === String(restoredMapState.selectedSystemId))
            || mapItemFromSearchResult(restoredMapState.selectedSystem, mapFrame)
          : null;
        const initial = restoredSystem
          || prepared.find((item) => Number(item.planet_count || 0) > 0 && !isCatalogFallbackName(item.display_name))
          || prepared.find((item) => !isCatalogFallbackName(item.display_name))
          || prepared[0]
          || null;
        setSelectedSystem(initial);
        const historyIds = Array.isArray(restoredMapState?.selectionHistoryIds)
          ? restoredMapState.selectionHistoryIds.map((item) => String(item))
          : [];
        const restoredHistory = historyIds
          .map((id) => prepared.find((item) => String(item.system_id) === id))
          .filter(Boolean);
        setSelectionHistory((initial ? [initial] : [])
          .concat(restoredHistory.filter((item) => item.system_id !== initial?.system_id))
          .slice(0, 8));
      })
      .catch((exc) => {
        if (!active) {
          return;
        }
        setError(exc instanceof Error ? exc.message : "Map data unavailable");
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, [mapFrame, nameStyle, restoredMapState]);

  const routeTotalLy = useMemo(
    () => routeSegments.reduce((sum, segment) => sum + segment.distance_ly, 0),
    [routeSegments],
  );

  const filterExtents = useMemo(() => {
    const maxStars = Math.max(1, ...systems.map((system) => Number(system.star_count || 0)));
    const maxPlanets = Math.max(1, ...systems.map((system) => Number(system.planet_count || 0)));
    const maxCoolness = Math.max(1, Math.ceil(Math.max(...systems.map((system) => Number(system.coolness_score || 0)))));
    const maxTemp = Math.max(
      STAR_SEARCH_DEFAULT_TEMP_RANGE[1],
      Math.ceil(Math.max(...systems.map((system) => Number(system.max_star_teff_k || 0))) / 100) * 100,
    );
    return { maxStars, maxPlanets, maxCoolness, maxTemp };
  }, [systems]);

  useEffect(() => {
    setMapSearchFilters((current) => ({
      ...current,
      starRange: [Math.min(current.starRange[0], filterExtents.maxStars), filterExtents.maxStars],
      planetRange: [Math.min(current.planetRange[0], filterExtents.maxPlanets), filterExtents.maxPlanets],
      coolnessRange: [Math.min(current.coolnessRange[0], filterExtents.maxCoolness), filterExtents.maxCoolness],
      temperatureRange: [
        Math.max(STAR_SEARCH_DEFAULT_TEMP_RANGE[0], current.temperatureRange[0]),
        Math.min(filterExtents.maxTemp, Math.max(current.temperatureRange[1], STAR_SEARCH_DEFAULT_TEMP_RANGE[1])),
      ],
    }));
  }, [filterExtents]);

  const mapSearchOrigin = telemetry.originLy || sceneToHelioCoordinates(telemetry.cameraScenePosition, mapFrame);

  const activeMapFilter = useMemo(() => {
    const filters = mapSearchFilters;
    return (
      filters.distanceRange[0] > 0
      || filters.distanceRange[1] < MAP_RADIUS_LY
      || filters.starRange[0] > 0
      || filters.starRange[1] < filterExtents.maxStars
      || filters.planetRange[0] > 0
      || filters.planetRange[1] < filterExtents.maxPlanets
      || filters.coolnessRange[0] > 0
      || filters.coolnessRange[1] < filterExtents.maxCoolness
      || filters.temperatureRange[0] > STAR_SEARCH_DEFAULT_TEMP_RANGE[0]
      || filters.temperatureRange[1] < STAR_SEARCH_DEFAULT_TEMP_RANGE[1]
      || Boolean(filters.spectralClass)
      || Boolean(filters.habitableOnly)
    );
  }, [filterExtents, mapSearchFilters]);

  const filteredMapSystems = useMemo(() => {
    if (!activeMapFilter) {
      return [];
    }
    return systems
      .filter((system) => systemMatchesMapFilters(system, mapSearchFilters, mapSearchOrigin))
      .sort((left, right) => {
        const leftDistance = distanceFromHelioOrigin(left, mapSearchOrigin);
        const rightDistance = distanceFromHelioOrigin(right, mapSearchOrigin);
        return right.map_priority - left.map_priority || leftDistance - rightDistance;
      });
  }, [activeMapFilter, mapSearchFilters, mapSearchOrigin, systems]);

  const filteredMapIds = useMemo(
    () => new Set(filteredMapSystems.map((system) => String(system.system_id))),
    [filteredMapSystems],
  );

  const suggestedNeighbors = useMemo(() => {
    if (!selectedSystem) {
      return [];
    }
    const scored = systems
      .filter((system) => system.system_id !== selectedSystem.system_id)
      .map((system) => ({
        system,
        routeDistance: distanceBetweenSystems(selectedSystem, system),
        score: neighborInterestScore(system, selectedSystem),
      }))
      .filter((entry) => Number.isFinite(entry.score));
    const preferred = scored
      .filter((entry) => entry.score >= 28 || entry.routeDistance <= 10)
      .sort((left, right) => right.score - left.score || left.routeDistance - right.routeDistance)
      .slice(0, 8);
    if (preferred.length >= 4) {
      return preferred;
    }
    return scored
      .sort((left, right) => right.score - left.score || left.routeDistance - right.routeDistance)
      .slice(0, 8);
  }, [selectedSystem, systems]);

  const addRouteSegment = () => {
    const target = routeMenu?.target;
    if (!selectedSystem || !target || selectedSystem.system_id === target.system_id) {
      setRouteMenu(null);
      return;
    }
    const distanceLy = distanceBetweenSystems(selectedSystem, target);
    setRouteSegments((segments) => [
      ...segments,
      {
        id: `${selectedSystem.system_id}-${target.system_id}-${Date.now()}`,
        from: selectedSystem,
        to: target,
        distance_ly: distanceLy,
      },
    ]);
    setRouteMenu(null);
  };

  const addNeighborSegments = () => {
    const origin = selectedSystem;
    if (!origin?.system_id) {
      setRouteMenu(null);
      return;
    }
    const neighbors = systems
      .filter((system) => system?.system_id && system.system_id !== origin.system_id)
      .map((system) => ({
        system,
        distance_ly: distanceBetweenSystems(origin, system),
      }))
      .filter((entry) => Number.isFinite(entry.distance_ly) && entry.distance_ly <= 10)
      .sort((left, right) => left.distance_ly - right.distance_ly);
    setRouteSegments((segments) => [
      ...segments.filter((segment) => segment.kind !== "neighbor"),
      ...neighbors.map(({ system, distance_ly }) => ({
        id: `neighbor-${origin.system_id}-${system.system_id}`,
        kind: "neighbor",
        from: origin,
        to: system,
        distance_ly,
        label: `${shortDisplayName(system.display_name)} · ${formatNumber(distance_ly, 2)} ly`,
      })),
    ]);
    setRouteMenu(null);
  };

  const undoRouteSegment = () => {
    setRouteSegments((segments) => segments.slice(0, -1));
    setRouteMenu(null);
  };

  const clearRoute = () => {
    setRouteSegments([]);
    setRouteMenu(null);
  };

  const truncateRouteAtSegment = useCallback((index) => {
    setRouteSegments((segments) => segments.slice(0, Math.max(0, index)));
    setRouteMenu(null);
  }, []);

  const toggleFullscreen = () => {
    if (document.fullscreenElement) {
      document.exitFullscreen?.();
      return;
    }
    pageRef.current?.requestFullscreen?.({ navigationUI: "hide" });
  };

  const setMobileFlightDirection = useCallback((direction, active) => {
    setMobileFlightIntent((current) => (
      current[direction] === active ? current : { ...current, [direction]: active }
    ));
  }, []);

  const clearMobileFlight = useCallback(() => {
    setMobileFlightIntent(DEFAULT_MOBILE_FLIGHT_STATE);
  }, []);

  const mobileFlightButtonProps = (direction) => ({
    onPointerDown: (event) => {
      event.preventDefault();
      try {
        event.currentTarget.setPointerCapture?.(event.pointerId);
      } catch {
        // Synthetic pointer events may not create capturable pointers.
      }
      setMobileFlightDirection(direction, true);
    },
    onPointerUp: (event) => {
      event.preventDefault();
      try {
        event.currentTarget.releasePointerCapture?.(event.pointerId);
      } catch {
        // Ignore non-captured synthetic pointers.
      }
      setMobileFlightDirection(direction, false);
    },
    onPointerCancel: () => setMobileFlightDirection(direction, false),
    onPointerLeave: (event) => {
      if (event.buttons) {
        setMobileFlightDirection(direction, false);
      }
    },
    onBlur: () => setMobileFlightDirection(direction, false),
    onContextMenu: (event) => event.preventDefault(),
  });

  const openSystemDetail = useCallback((system) => {
    if (!system?.system_id) {
      return;
    }
    const liveCameraPosition = parseMapCameraDatasetPosition(
      document.querySelector(".map-canvas canvas")?.dataset?.mapCameraPosition || ""
    );
    const cameraState = liveCameraPosition
      ? { ...mapCameraStateRef.current, position: liveCameraPosition }
      : mapCameraStateRef.current;
    const token = writeStoredMapReturnState({
      camera: cameraState,
      selectedSystemId: system.system_id,
      selectedSystemName: system.display_name || system.system_name || "",
      drillMode,
      peekSize,
      mapFrame,
      showDirectionLabels,
      selectionHistoryIds: selectionHistory.map((item) => item.system_id).filter(Boolean),
    });
    const params = new URLSearchParams({ from: "map" });
    if (token) {
      params.set("map_return", token);
    }
    navigate(`/systems/${system.system_id}?${params.toString()}`);
  }, [drillMode, mapFrame, navigate, peekSize, selectionHistory, showDirectionLabels]);

  const selectSearchSystem = useCallback((system, options = {}) => {
    const existing = systems.find((item) => String(item.system_id) === String(system?.system_id));
    const mapItem = existing || mapItemFromSearchResult(system, mapFrame);
    if (!mapItem) {
      return;
    }
    selectSystem(mapItem, { openPeek: true, focus: Boolean(options.focus) });
    if (options.explore) {
      setDrillMode("explore");
      setFocusToken((value) => value + 1);
    }
  }, [mapFrame, selectSystem, systems]);

  const jumpHomeToSol = useCallback(() => {
    const sol = systems.find((item) => (
      String(item.system_id) === String(SOL_SYSTEM_ID)
      || String(item.display_name || item.system_name || "").trim().toLowerCase() === "sol"
    )) || {
      system_id: SOL_SYSTEM_ID,
      system_name: "Sol",
      display_name: "Sol",
      dist_ly: 0,
      x_helio_ly: 0,
      y_helio_ly: 0,
      z_helio_ly: 0,
      scene_position: [0, 0, 0],
      star_count: 1,
      planet_count: 13,
      coolness_score: null,
      coolness_rank: null,
      dominant_spectral_class: "G",
      spectral_classes: ["G"],
    };
    selectSystem(sol, { openPeek: true, focus: true });
  }, [selectSystem, systems]);

  const runMapSearch = useCallback(async ({ cursorValue = null, append = false, sortOverride = null } = {}) => {
    const token = mapSearchTokenRef.current + 1;
    mapSearchTokenRef.current = token;
    const requestedSort = sortOverride || mapSearchSort;
    const effectiveSort = !mapSearchQuery.trim() && requestedSort === "match" ? "distance" : requestedSort;
    const params = buildSearchParamsFromFilters(mapSearchFilters, mapSearchOrigin, filterExtents, mapSearchQuery, effectiveSort, 24);
    params.name_style = normalizeNameStyle(nameStyle);
    const requestParams = cursorValue ? { ...params, cursor: cursorValue } : params;
    setMapSearchLoading(true);
    setMapSearchError("");
    setMapSearchResultsOpen(true);
    if (!append) {
      const nextParams = {};
      if (mapSearchQuery.trim()) {
        nextParams.q = mapSearchQuery.trim();
      }
      if (effectiveSort && effectiveSort !== (mapSearchQuery.trim() ? "match" : "distance")) {
        nextParams.sort = effectiveSort;
      }
      setSearchParams(nextParams, { replace: false });
    }
    try {
      const started = performance.now();
      const payload = await fetchSystems(requestParams);
      if (mapSearchTokenRef.current !== token) {
        return;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      setMapSearchResults((current) => (append ? [...current, ...items] : items));
      setMapSearchCursor(payload.next_cursor || null);
      setMapSearchHasMore(Boolean(payload.has_more));
      const elapsed = Number(payload.query_time_ms);
      setMapSearchStats(`${formatNumber(append ? mapSearchResults.length + items.length : items.length, 0)} systems · ${formatNumber(Number.isFinite(elapsed) ? elapsed : performance.now() - started, 0)} ms`);
    } catch (exc) {
      if (mapSearchTokenRef.current === token) {
        setMapSearchError(exc instanceof Error ? exc.message : "Star Search unavailable");
        if (!append) {
          setMapSearchResults([]);
          setMapSearchCursor(null);
          setMapSearchHasMore(false);
        }
      }
    } finally {
      if (mapSearchTokenRef.current === token) {
        setMapSearchLoading(false);
      }
    }
  }, [filterExtents, mapSearchFilters, mapSearchOrigin, mapSearchQuery, mapSearchResults.length, mapSearchSort, nameStyle, setSearchParams]);

  const closeMapSearchResults = useCallback(() => {
    setMapSearchResultsOpen(false);
    setMapSearchResults([]);
    setMapSearchCursor(null);
    setMapSearchHasMore(false);
    setMapSearchError("");
  }, []);

  useEffect(() => {
    if (!mapNameStyleInitializedRef.current) {
      mapNameStyleInitializedRef.current = true;
      return;
    }
    if (mapSearchResultsOpen || mapSearchResults.length > 0) {
      runMapSearch({ append: false });
    }
  }, [nameStyle]);

  const closeMapSearch = useCallback(() => {
    closeMapSearchResults();
    if (location.pathname === "/" || location.pathname === "/search") {
      navigate("/map");
    }
  }, [closeMapSearchResults, location.pathname, navigate]);

  const toggleMapSearch = useCallback(() => {
    if (mapSearchOpen) {
      closeMapSearch();
      return;
    }
    navigate("/");
  }, [closeMapSearch, mapSearchOpen, navigate]);

  const submitMapSearch = useCallback((event) => {
    event?.preventDefault?.();
    runMapSearch({ append: false });
  }, [runMapSearch]);

  const changeMapSearchSort = useCallback((nextSort) => {
    const requested = STAR_SEARCH_SORT_OPTIONS.some((option) => option.value === nextSort) ? nextSort : "distance";
    const normalized = !mapSearchQuery.trim() && requested === "match" ? "distance" : requested;
    setMapSearchSort(normalized);
    runMapSearch({ append: false, sortOverride: normalized });
  }, [mapSearchQuery, runMapSearch]);

  useEffect(() => {
    const onKeyDown = (event) => {
      const key = String(event.key || "").toLowerCase();
      if (key === "m" && !event.metaKey && !event.ctrlKey && !event.altKey) {
        const target = event.target;
        const editing = target?.closest?.("input, textarea, select, [contenteditable='true']");
        if (!editing) {
          event.preventDefault();
          toggleMinimalMode();
          return;
        }
      }
      if (event.key === "Escape") {
        if (document.fullscreenElement) {
          return;
        }
        if (minimalMode) {
          event.preventDefault();
          exitMinimalMode();
          return;
        }
        setRouteMenu(null);
        if (drillMode !== "flight") {
          event.preventDefault();
          exitDrillMode();
        } else if (mapSearchResultsOpen) {
          event.preventDefault();
          closeMapSearchResults();
        } else if (mapSearchOpen && location.pathname !== "/map") {
          event.preventDefault();
          closeMapSearch();
        }
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [closeMapSearch, closeMapSearchResults, drillMode, exitDrillMode, exitMinimalMode, location.pathname, mapSearchOpen, mapSearchResultsOpen, minimalMode, toggleMinimalMode]);

  return (
    <div
      className={`map-page ${telemetry.locked ? "reticle-active" : ""} ${mapSearchOpen ? "map-search-active" : ""} ${minimalMode ? "map-minimal-mode" : ""} map-drill-${drillMode}`}
      ref={pageRef}
      data-map-drill-mode={drillMode}
      data-map-minimal-mode={minimalMode ? "true" : "false"}
    >
      <div className="map-background-grid" aria-hidden="true" />
      {systems.length > 0 && (
        <StarMapScene
          key={mapContextEpoch}
          systems={systems}
          selectedSystem={selectedSystem}
          filterMatchIds={filteredMapIds}
          filterActive={activeMapFilter}
          filterLabelSystems={filteredMapSystems}
          onSelect={(system) => selectSystem(system, { openPeek: true })}
          onRouteContext={setRouteMenu}
          keybindScheme={keybindScheme}
          mapFrame={mapFrame}
          showDirectionLabels={showDirectionLabels}
          routeSegments={routeSegments}
          onRemoveRouteSegment={truncateRouteAtSegment}
          controlsEnabled={controlsEnabled}
          stabilizationEnabled={stabilizationEnabled}
          onTelemetry={handleMapTelemetry}
          onCameraState={handleMapCameraState}
          initialCameraState={mapRecoveryCameraState}
          reticleSelectRequest={reticleSelectRequest}
          focusTarget={selectedSystem}
          focusToken={focusToken}
          mobileFlightIntent={mobileFlightIntent}
          onContextLost={handleMapContextLost}
          runtimeQuality={runtimeQuality}
          runtimeDiagnostics={runtimeDiagnostics}
        />
      )}

      {mapContextRecovering && (
        <div className="map-context-recovery" role="status" aria-live="polite">
          Restoring star map
        </div>
      )}

      <div className="map-reticle" aria-hidden="true" />

      <button
        type="button"
        className="map-minimal-toggle"
        data-testid="map-minimal-toggle"
        aria-pressed={minimalMode}
        onClick={toggleMinimalMode}
        title={minimalMode ? "Restore map interface (M or Esc)" : "Minimal map interface (M)"}
      >
        {minimalMode ? "UI" : "MIN"}
      </button>

      {minimalNotice && (
        <div className="map-minimal-notice" role="status" aria-live="polite">
          Minimal mode. Press M or Esc to restore the interface.
        </div>
      )}

      <header className="map-hud map-hud-top">
        <div className="map-title-block">
          <a className="map-eyebrow map-eyebrow-link" href={spacegateUrl}>
            Spacegate Stellar Database
          </a>
          <div className="map-title-row">
            <img className="map-brand-mark" src="/favicon.svg" alt="" aria-hidden="true" />
            <h1><a className="map-title-link" href="/map">{mapTitle}</a></h1>
          </div>
          <span className="map-build">100 ly · {buildId ? `build ${buildId}` : "build unknown"}</span>
        </div>
        <div className="map-header-readout" aria-live="polite">
          {loading && <span>Loading 100 ly map</span>}
          {error && <span>Map data unavailable</span>}
          {!loading && !error && summary && (
            <>
              <span>{formatNumber(summary.returned, 0)} systems</span>
              <span>{formatNumber(summary.planet_systems, 0)} planet hosts</span>
              <span>{formatNumber(summary.multi_star_systems, 0)} multi-star</span>
              <span>{mapFrame === "galactic" ? "Galactic frame" : "ICRS J2016"}</span>
            </>
          )}
        </div>
        <nav className="map-actions" aria-label="Map actions">
          <div className="map-link-row" aria-label="Site links">
            {MAP_UTILITY_LINKS.filter((item) => MAP_VISIBLE_UTILITY_LABELS.has(item.label)).map((item) => (
              item.external ? (
                <a
                  key={item.label}
                  href={item.href}
                  className="map-text-link"
                  title={item.title}
                  target="_blank"
                  rel="noreferrer"
                >
                  {item.label}
                </a>
              ) : (
                <Link key={item.label} to={item.href} className="map-text-link" title={item.title}>
                  {item.label}
                </Link>
              )
            ))}
          </div>
          <div className="map-command-row">
            <button
              type="button"
              className={`map-hud-button map-search-toggle ${mapSearchOpen ? "active" : ""}`}
              aria-pressed={mapSearchOpen}
              data-testid="map-search-toggle"
              onClick={toggleMapSearch}
            >
              Search
            </button>
            <button
              type="button"
              className="map-hud-button"
              onClick={jumpHomeToSol}
              title="Return to Sol"
            >
              SOL
            </button>
            <button
              type="button"
              className="map-hud-button"
              onClick={enterMinimalMode}
              title="Minimal map interface (M)"
            >
              MIN
            </button>
            {selectedSystem?.system_id && (
              <button
                type="button"
                className="map-hud-button primary"
                onClick={() => {
                  setDrillMode("explore");
                  setFocusToken((value) => value + 1);
                }}
              >
                Explore
              </button>
            )}
            {fullscreenAvailable && (
              <button type="button" className="map-hud-button map-fullscreen-command" onClick={toggleFullscreen}>
                {isFullscreen ? "Exit" : "Full"}
              </button>
            )}
            <details className="map-header-menu" ref={headerMenuRef}>
              <summary className="map-hud-button map-menu-button" aria-label="Map menu" title="Map menu">
                <span className="map-menu-bars" aria-hidden="true" />
              </summary>
              <div className="map-header-menu-panel">
                <label className="map-menu-field map-theme-select">
                  <span>Theme</span>
                  <select value={theme} onChange={(event) => setTheme(event.target.value)}>
                    {themeOptions.map((option) => (
                      <option key={option.id} value={option.id}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <label className="map-menu-field map-keybind-select">
                  <span>Controls</span>
                  <select
                    value={keybindScheme}
                    onChange={(event) => setKeybindScheme(
                      MAP_KEYBIND_SCHEMES[event.target.value] ? event.target.value : "wasd",
                    )}
                  >
                    {MAP_KEYBIND_OPTIONS.map((option) => (
                      <option key={option.id} value={option.id}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <label className="map-menu-field map-frame-select">
                  <span>Frame</span>
                  <select
                    value={mapFrame}
                    onChange={(event) => setMapFrame(
                      MAP_FRAME_OPTIONS[event.target.value] ? event.target.value : "icrs",
                    )}
                    data-testid="map-frame-select"
                  >
                    {Object.values(MAP_FRAME_OPTIONS).map((option) => (
                      <option key={option.id} value={option.id}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <label className="map-menu-field map-scale-select">
                  <span>Default Scale</span>
                  <select
                    value={normalizeSystemScaleMode(defaultScaleMode)}
                    onChange={(event) => setDefaultScaleMode(normalizeSystemScaleMode(event.target.value))}
                    data-testid="map-default-scale-select"
                  >
                    {SYSTEM_SCALE_MODE_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <label className="map-menu-field map-name-style-select">
                  <span>Name Style</span>
                  <select
                    value={normalizeNameStyle(nameStyle)}
                    onChange={(event) => setNameStyle(normalizeNameStyle(event.target.value))}
                    data-testid="map-name-style-select"
                  >
                    {NAME_STYLE_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <div className="map-menu-links" aria-label="Map menu links">
                  {MAP_UTILITY_LINKS.filter((item) => MAP_MENU_UTILITY_LABELS.has(item.label)).map((item) => (
                    item.external ? (
                      <a
                        key={item.label}
                        href={item.href}
                        className="map-menu-link"
                        title={item.title}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {item.label}
                      </a>
                    ) : (
                      <Link key={item.label} to={item.href} className="map-menu-link" title={item.title}>
                        {item.label}
                      </Link>
                    )
                  ))}
                </div>
                <label className="map-menu-toggle">
                  <input
                    type="checkbox"
                    checked={showDirectionLabels}
                    onChange={(event) => setShowDirectionLabels(event.target.checked)}
                    data-testid="map-direction-labels-toggle"
                  />
                  <span>Direction labels</span>
                </label>
                <label className="map-menu-toggle">
                  <input
                    type="checkbox"
                    checked={showFpsOverlay}
                    onChange={(event) => setShowFpsOverlay(event.target.checked)}
                    data-testid="map-fps-toggle"
                  />
                  <span>Runtime diagnostics</span>
                </label>
                <span className="map-menu-note">Arrow keys always fly.</span>
              </div>
            </details>
          </div>
        </nav>
      </header>

      {showFpsOverlay && (
        <div
          className="map-fps-overlay"
          role="status"
          aria-live="polite"
          data-testid="map-fps-overlay"
          data-quality-tier={runtimeDiagnostics.qualityTier}
          data-active-webgl-surfaces={runtimeDiagnostics.activeSurfaces}
          data-preview-pool-active={runtimeDiagnostics.activePreviews}
          data-preview-pool-budget={runtimeDiagnostics.previewBudget}
          data-context-recoveries={runtimeDiagnostics.contextLossRecoveries}
        >
          <div>
            <strong>{fpsSample > 0 ? fpsSample : "--"}</strong>
            <span>FPS</span>
          </div>
          <div>
            <strong>{runtimeDiagnostics.activeSurfaces}/{runtimeDiagnostics.contextBudget}</strong>
            <span>WebGL</span>
          </div>
          <div>
            <strong>{runtimeDiagnostics.activePreviews}/{runtimeDiagnostics.previewBudget}</strong>
            <span>Previews</span>
          </div>
          <div>
            <strong>{runtimeDiagnostics.contextLossRecoveries}</strong>
            <span>Recoveries</span>
          </div>
          <div>
            <strong>{runtimeDiagnostics.qualityTier}</strong>
            <span>Quality</span>
          </div>
        </div>
      )}

      <MapStarSearchShell
        open={mapSearchOpen}
        systems={systems}
        selectedSystem={selectedSystem}
        selectionHistory={selectionHistory}
        suggestedNeighbors={suggestedNeighbors}
        filters={mapSearchFilters}
        filterExtents={filterExtents}
        matchedCount={activeMapFilter ? filteredMapSystems.length : systems.length}
        query={mapSearchQuery}
        setQuery={setMapSearchQuery}
        sort={mapSearchSort}
        onSortChange={changeMapSearchSort}
        setFilters={setMapSearchFilters}
        onSubmitSearch={submitMapSearch}
        onCloseResults={closeMapSearchResults}
        onSelectSystem={(system) => selectSearchSystem(system)}
        onExploreSystem={(system) => selectSearchSystem(system, { explore: true, focus: true })}
        results={mapSearchResults}
        resultsOpen={mapSearchResultsOpen}
        loading={mapSearchLoading}
        error={mapSearchError}
        hasMore={mapSearchHasMore}
        onLoadMore={() => runMapSearch({ cursorValue: mapSearchCursor, append: true })}
        searchStats={mapSearchStats}
        previewSnapshotCache={previewSnapshotCache}
        previewPoolAllocations={previewPoolAllocations}
        previewPoolBudget={previewPoolBudget}
        previewRuntimeQualityTier={runtimeQuality.tier}
        defaultScaleMode={defaultScaleMode}
        nameStyle={nameStyle}
        previewPaused={previewPaused}
        previewCooldownActive={previewCooldownActive}
        onRequestPreview={requestSearchPreview}
        onReleasePreview={releaseSearchPreview}
        onCapturePreview={captureSearchPreview}
        onRuntimeEvent={handleRuntimeEvent}
      />

      <aside className="map-hud map-controls-panel">
        <span className="map-panel-label">Flight</span>
        <div className="map-control-buttons">
          <button
            type="button"
            className="map-command-button ghost map-reticle-command"
            onClick={() => setReticleSelectRequest((value) => value + 1)}
          >
            Select reticle
          </button>
        </div>
        <div className="map-mobile-flight-pad" aria-label="Mobile flight controls" onPointerCancel={clearMobileFlight}>
          <button
            type="button"
            className="map-mobile-flight-button map-mobile-flight-forward"
            aria-label="Fly forward"
            data-testid="map-mobile-flight-forward"
            {...mobileFlightButtonProps("forward")}
          >
            ↑
          </button>
          <button
            type="button"
            className="map-mobile-flight-button map-mobile-flight-left"
            aria-label="Fly left"
            data-testid="map-mobile-flight-left"
            {...mobileFlightButtonProps("left")}
          >
            ←
          </button>
          <button
            type="button"
            className="map-mobile-flight-button map-mobile-flight-right"
            aria-label="Fly right"
            data-testid="map-mobile-flight-right"
            {...mobileFlightButtonProps("right")}
          >
            →
          </button>
          <button
            type="button"
            className="map-mobile-flight-button map-mobile-flight-back"
            aria-label="Fly backward"
            data-testid="map-mobile-flight-back"
            {...mobileFlightButtonProps("back")}
          >
            ↓
          </button>
          <button
            type="button"
            className="map-mobile-flight-button map-mobile-flight-up"
            aria-label="Fly up"
            data-testid="map-mobile-flight-up"
            {...mobileFlightButtonProps("up")}
          >
            ⇧
          </button>
          <button
            type="button"
            className="map-mobile-flight-button map-mobile-flight-down"
            aria-label="Fly down"
            data-testid="map-mobile-flight-down"
            {...mobileFlightButtonProps("down")}
          >
            ⇩
          </button>
        </div>
        <p className="map-desktop-hint">Desktop: drag look · wheel fly · L+R drag orbit · tilt wheel/RMB drag truck · MMB drag pedestal · {activeKeybind.hint}</p>
        <p className="map-touch-hint">Touch: drag look · hold arrows to fly · tap/select reticle · pinch fly · two-finger pan</p>
        {routeSegments.length > 0 && (
          <div className="map-route-summary">
            <span>{routeSegments.length} legs · {formatNumber(routeTotalLy, 2)} ly total</span>
            <ol className="map-route-leg-list">
              {routeSegments.slice(-4).map((segment, visibleIndex) => {
                const segmentIndex = Math.max(0, routeSegments.length - 4) + visibleIndex;
                return (
                <li key={segment.id}>
                  <button
                    type="button"
                    title="Remove this leg and later route legs"
                    onClick={() => truncateRouteAtSegment(segmentIndex)}
                  >
                    <span>{shortDisplayName(segment.from.display_name)}</span>
                    <span>→</span>
                    <span>{shortDisplayName(segment.to.display_name)}</span>
                    <strong>{formatNumber(segment.distance_ly, 2)} ly</strong>
                  </button>
                </li>
                );
              })}
            </ol>
            <div>
              <button type="button" className="map-mini-command" onClick={undoRouteSegment}>
                Undo
              </button>
              <button type="button" className="map-mini-command" onClick={clearRoute}>
                Clear
              </button>
            </div>
          </div>
        )}
      </aside>

      {routeMenu?.target && (
        <div
          className="map-context-menu"
          style={{ left: `${routeMenu.x}px`, top: `${routeMenu.y}px` }}
          role="menu"
        >
          <strong><SystemNameDisplay system={routeMenu.target} /></strong>
          <span>
            {formatNumber(routeMenu.target.dist_ly, 2)} ly from Sol · {routeMenu.target.dominant_spectral_class}
          </span>
          {selectedSystem && selectedSystem.system_id !== routeMenu.target.system_id && (
            <span>
              {formatNumber(distanceBetweenSystems(selectedSystem, routeMenu.target), 2)} ly from {shortDisplayName(selectedSystem.display_name)}
            </span>
          )}
          <button
            type="button"
            className="map-context-command"
            onClick={() => {
              selectSystem(routeMenu.target, { openPeek: true });
              setRouteMenu(null);
            }}
          >
            Select
          </button>
          <button
            type="button"
            className="map-context-command ghost"
            onClick={() => {
              selectSystem(routeMenu.target, { openPeek: true, focus: true });
              setDrillMode("explore");
              setRouteMenu(null);
            }}
          >
            Explore
          </button>
          <button
            type="button"
            className="map-context-command ghost"
            disabled={!selectedSystem || selectedSystem.system_id === routeMenu.target.system_id}
            onClick={addRouteSegment}
          >
            Measure
          </button>
          <button
            type="button"
            className="map-context-command ghost"
            disabled={!selectedSystem}
            onClick={addNeighborSegments}
            title={selectedSystem ? `Draw 10 ly neighbor spokes from ${shortDisplayName(selectedSystem.display_name)}` : "Select a system first"}
          >
            Neighbors
          </button>
          {routeSegments.length > 0 && (
            <button type="button" className="map-context-command ghost" onClick={clearRoute}>
              Clear route
            </button>
          )}
        </div>
      )}
      {selectedSystem?.system_id && drillMode !== "flight" && (
        <section
          className={`map-system-drill map-system-drill-${drillMode}`}
          data-testid="map-system-drill"
          data-drill-mode={drillMode}
          style={drillMode === "peek" ? {
            "--map-peek-width": `${peekSize.width}px`,
            "--map-peek-height": `${peekSize.height}px`,
          } : undefined}
          aria-label={`${formatName(selectedSystem.display_name)} system simulation`}
        >
          <div className="map-system-drill-bar">
            <div className="map-system-drill-title-group">
              {drillMode === "peek" && (
                <button
                  type="button"
                  className="map-system-drill-resize"
                  aria-label="Resize System Peek"
                  title="Resize System Peek"
                  onPointerDown={beginPeekResize}
                />
              )}
              <button
                type="button"
                className="map-system-drill-title"
                onClick={() => {
                  if (drillMode === "peek") {
                    setDrillMode("explore");
                    setFocusToken((value) => value + 1);
                  } else {
                    openSystemDetail(selectedSystem);
                  }
                }}
              >
                <span>System:</span>
                <SystemNameDisplay system={selectedSystem} showCopyButton={false} showInfoButton={false} />
                {drillStellarClassEntries.length > 0 && (
                  <span className="map-title-stellar-classes" aria-label="Rendered stellar classes">
                    {drillStellarClassEntries.map((entry, index) => (
                      <StellarClassChips
                        key={`${entry.key || entry.name || "star"}:${index}`}
                        tokens={entry.tokens}
                        size="compact"
                        className="map-title-stellar-class"
                      />
                    ))}
                  </span>
                )}
              </button>
            </div>
            <div className="map-system-drill-actions">
              {drillMode === "peek" && (
                <button
                  type="button"
                  className="map-command-button primary"
                  onClick={() => {
                    setDrillMode("explore");
                    setFocusToken((value) => value + 1);
                  }}
                >
                  Explore
                </button>
              )}
              <button type="button" className="map-command-button ghost" onClick={() => openSystemDetail(selectedSystem)}>
                Detail
              </button>
              {drillMode === "explore" ? (
                <>
                  <button type="button" className="map-command-button ghost" onClick={backToPeekFromExplore}>
                    Back
                  </button>
                  <button type="button" className="map-command-button ghost map-system-drill-close" onClick={() => exitDrillMode()}>
                    ×
                  </button>
                </>
              ) : (
                <button type="button" className="map-command-button ghost" onClick={() => exitDrillMode()}>
                  Close
                </button>
              )}
            </div>
          </div>
          <div className="map-system-drill-body">
            <div className="map-system-vital-strip" aria-label={`${formatName(selectedSystem.display_name)} map vitals`}>
              <span>{formatNumber(selectedSystem.dist_ly, 2)} ly</span>
              <span>{formatNumber(selectedSystem.star_count, 0)} stars</span>
              <span>{formatNumber(selectedSystem.planet_count, 0)} planets</span>
              <span>cool {formatNumber(selectedSystem.coolness_score, 1)}</span>
              <span>rank {formatNumber(selectedSystem.coolness_rank, 0)}</span>
            </div>
            <React.Suspense fallback={<section className="panel system-preview-panel system-preview-loading">Loading System Simulation...</section>}>
              <SystemPreviewPanel
                key={`${selectedSystem.system_id}:${drillMode}:${fullscreenEpoch}`}
                systemId={selectedSystem.system_id}
                systemName={formatName(selectedSystem.display_name)}
                presentationMode={drillMode}
                qualityTier={runtimeQuality.tier}
                onRuntimeEvent={handleRuntimeEvent}
                onStellarClassEntries={setDrillStellarClassEntries}
                defaultScaleMode={defaultScaleMode}
                nameStyle={normalizeNameStyle(nameStyle)}
              />
            </React.Suspense>
          </div>
        </section>
      )}
    </div>
  );
}
