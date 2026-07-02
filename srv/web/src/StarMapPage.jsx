import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import * as THREE from "three";
import { fetchMapSystems, fetchPublicConfig } from "./api.js";

const MAP_RADIUS_LY = 100;
const SystemPreviewPanel = React.lazy(() => import("./SystemPreviewPanel.jsx"));
const LY_TO_SCENE = 0.55;
const WORLD_UP = new THREE.Vector3(0, 1, 0);
const PUBLIC_CONFIG_FALLBACK = { site_name: "Coolstars", map_title: "Coolstars Map" };
const MAP_UTILITY_LINKS = [
  { label: "ABT", href: "/about", title: "About this site", external: false },
  { label: "SPT", href: "https://github.com/sponsors/galenmatson", title: "Support this project", external: true },
  { label: "SRC", href: "https://github.com/galenmatson/spacegate", title: "Source code", external: true },
  { label: "DATA", href: "/data", title: "Source data", external: false },
];
const MAP_PEEK_SIZE_STORAGE_KEY = "spacegate.map.peekSize";
const MAP_KEYBIND_STORAGE_KEY = "spacegate.map.keybindScheme";
const MAP_FRAME_STORAGE_KEY = "spacegate.map.frame";
const MAP_DIRECTION_LABELS_STORAGE_KEY = "spacegate.map.directionLabels";
const DEFAULT_MAP_PEEK_SIZE = { width: 675, height: 468 };
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

function SystemNameDisplay({ system, linkTo = null, className = "", showCopyButton = true, showInfoButton = true }) {
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
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocusCapture={() => setOpen(true)}
      onBlurCapture={() => setOpen(false)}
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
            setOpen((value) => !value);
          }}
          aria-label={`Show metadata for ${fullName}`}
        >
          i
        </button>
      )}
      {open && (
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
        display_name: formatName(item.system_name),
        dominant_spectral_class: SPECTRAL_COLORS[dominant] ? dominant : "UNKNOWN",
        scene_position: mapToScenePosition(item, frame),
        map_priority: priorityForSystem(item),
      };
    })
    .filter((item) => item.scene_position.every((value) => Number.isFinite(value)));
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

function StarField({ systems }) {
  const geometry = useMemo(() => {
    const positions = new Float32Array(systems.length * 3);
    const colors = new Float32Array(systems.length * 3);
    systems.forEach((system, idx) => {
      const base = idx * 3;
      positions[base] = system.scene_position[0];
      positions[base + 1] = system.scene_position[1];
      positions[base + 2] = system.scene_position[2];
      const color = new THREE.Color(SPECTRAL_COLORS[system.dominant_spectral_class] || SPECTRAL_COLORS.UNKNOWN);
      if (Number(system.planet_count || 0) > 0) {
        color.lerp(new THREE.Color("#95ffcf"), 0.24);
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
  }, [systems]);
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
        opacity={0.86}
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

function PriorityLabels({ systems, selectedSystem, onSelect }) {
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
  }, [camera, selectedSystem, systems]);
  const [labelSystems, setLabelSystems] = useState(buildLabelSet);

  useEffect(() => {
    setLabelSystems(buildLabelSet());
  }, [buildLabelSet]);

  useEffect(() => {
    gl.domElement.dataset.mapLabelCount = String(labelSystems.length);
    gl.domElement.dataset.mapLocalLabelCount = String(labelSystems.filter((system) => Number(system.label_camera_distance_ly) <= 10).length);
    gl.domElement.dataset.mapLabelStrategy = "camera_near_10ly_nearest_plus_coolness";
  }, [gl.domElement, labelSystems]);

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
  const total = segments.reduce((sum, segment) => sum + segment.distance_ly, 0);
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
              label={`${formatNumber(segment.distance_ly, 2)} ly`}
              position={midpoint}
              tone="route"
            />
            {index === segments.length - 1 && (
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
  reticleSelectRequest,
  focusTarget,
  focusToken,
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
    if (reticleSelectRequest > 0) {
      selectReticleTarget();
    }
  }, [reticleSelectRequest, selectReticleTarget]);

  useEffect(() => {
    if (!focusTarget?.scene_position || !focusToken) {
      return;
    }
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
  }, [camera, gl.domElement, mapFrame, showDirectionLabels]);

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
    camera.position.set(0, 3.5, 17);
    camera.rotation.order = "YXZ";
    camera.rotation.set(pitchRef.current, yawRef.current, 0);
  }, [camera]);

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
    if (keys.has(activeKeybind.forward) || keys.has("arrowup")) movement.add(direction);
    if (keys.has(activeKeybind.back) || keys.has("arrowdown")) movement.sub(direction);
    if (keys.has(activeKeybind.right) || keys.has("arrowright")) movement.add(strafe);
    if (keys.has(activeKeybind.left) || keys.has("arrowleft")) movement.sub(strafe);
    if (keys.has(activeKeybind.up)) movement.add(WORLD_UP);
    if (keys.has(activeKeybind.down)) movement.sub(WORLD_UP);
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
      });
      gl.domElement.dataset.mapKeybindScheme = activeKeybind.id;
      gl.domElement.dataset.mapCameraPosition = camera.position.toArray().map((value) => value.toFixed(3)).join(",");
      gl.domElement.dataset.mapFrame = mapFrame || "icrs";
      gl.domElement.dataset.mapDirectionLabels = showDirectionLabels ? "true" : "false";
    }
  });

  return null;
}

function StarMapScene({
  systems,
  selectedSystem,
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
  reticleSelectRequest,
  focusTarget,
  focusToken,
}) {
  return (
    <Canvas
      className="map-canvas"
      camera={{ fov: 62, near: 0.01, far: 1200, position: [0, 3.5, 17] }}
      gl={{ antialias: true, alpha: true, preserveDrawingBuffer: true, powerPreference: "high-performance" }}
    >
      <color attach="background" args={["#01030a"]} />
      <fog attach="fog" args={["#01030a", 80, 190]} />
      <DistanceRings />
      <OrientationAxes frame={mapFrame} showDirectionLabels={showDirectionLabels} />
      <StarField systems={systems} />
      <PriorityLabels systems={systems} selectedSystem={selectedSystem} onSelect={onSelect} />
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
        reticleSelectRequest={reticleSelectRequest}
        focusTarget={focusTarget}
        focusToken={focusToken}
      />
    </Canvas>
  );
}

export default function StarMapPage({ buildId = "", theme, setTheme, themeOptions = [] }) {
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
  const [telemetry, setTelemetry] = useState({ distLy: 0, speedLyS: 0, locked: false });
  const [fullscreenAvailable, setFullscreenAvailable] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [routeSegments, setRouteSegments] = useState([]);
  const [routeMenu, setRouteMenu] = useState(null);
  const [selectionHistory, setSelectionHistory] = useState([]);
  const [drillMode, setDrillMode] = useState("flight");
  const [focusToken, setFocusToken] = useState(0);
  const [peekSize, setPeekSize] = useState(readStoredMapPeekSize);
  const [keybindScheme, setKeybindScheme] = useState(readStoredMapKeybindScheme);
  const [mapFrame, setMapFrame] = useState(readStoredMapFrame);
  const [showDirectionLabels, setShowDirectionLabels] = useState(readStoredDirectionLabelsEnabled);
  const pageRef = useRef(null);
  const headerMenuRef = useRef(null);
  const drillHistoryPushedRef = useRef(false);
  const mapTitle = publicConfig?.map_title || PUBLIC_CONFIG_FALLBACK.map_title;
  const activeKeybind = MAP_KEYBIND_SCHEMES[keybindScheme] || MAP_KEYBIND_SCHEMES.wasd;

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

  const selectSystem = useCallback((system, options = {}) => {
    if (!system) {
      setSelectedSystem(null);
      exitDrillMode();
      return;
    }
    setSelectedSystem(system);
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
    const updateFullscreenState = () => {
      setFullscreenAvailable(Boolean(document.fullscreenEnabled && pageRef.current?.requestFullscreen));
      setIsFullscreen(document.fullscreenElement === pageRef.current);
    };
    updateFullscreenState();
    document.addEventListener("fullscreenchange", updateFullscreenState);
    return () => {
      document.removeEventListener("fullscreenchange", updateFullscreenState);
    };
  }, []);

  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        setRouteMenu(null);
        if (drillMode !== "flight") {
          event.preventDefault();
          exitDrillMode();
        }
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [drillMode, exitDrillMode]);

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
    fetchMapSystems({ max_dist_ly: String(MAP_RADIUS_LY), limit: "20000", compact: "true" })
      .then((payload) => {
        if (!active) {
          return;
        }
        const rows = payload?.items || [];
        const prepared = prepareMapItems(rows, mapFrame);
        setRawSystems(rows);
        setSummary(payload || null);
        const initial = prepared.find((item) => Number(item.planet_count || 0) > 0 && !isCatalogFallbackName(item.display_name))
          || prepared.find((item) => !isCatalogFallbackName(item.display_name))
          || prepared[0]
          || null;
        setSelectedSystem(initial);
        setSelectionHistory(initial ? [initial] : []);
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
  }, []);

  const routeTotalLy = useMemo(
    () => routeSegments.reduce((sum, segment) => sum + segment.distance_ly, 0),
    [routeSegments],
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

  const systemDetailPath = (system) => (
    system?.system_id ? `/systems/${system.system_id}?from=map` : "/"
  );

  return (
    <div
      className={`map-page ${telemetry.locked ? "reticle-active" : ""} map-drill-${drillMode}`}
      ref={pageRef}
      data-map-drill-mode={drillMode}
    >
      <div className="map-background-grid" aria-hidden="true" />
      {systems.length > 0 && (
        <StarMapScene
          systems={systems}
          selectedSystem={selectedSystem}
          onSelect={(system) => selectSystem(system, { openPeek: true })}
          onRouteContext={setRouteMenu}
          keybindScheme={keybindScheme}
          mapFrame={mapFrame}
          showDirectionLabels={showDirectionLabels}
          routeSegments={routeSegments}
          onRemoveRouteSegment={truncateRouteAtSegment}
          controlsEnabled={controlsEnabled}
          stabilizationEnabled={stabilizationEnabled}
          onTelemetry={setTelemetry}
          reticleSelectRequest={reticleSelectRequest}
          focusTarget={selectedSystem}
          focusToken={focusToken}
        />
      )}

      <div className="map-reticle" aria-hidden="true" />

      <header className="map-hud map-hud-top">
        <div className="map-title-block">
          <a className="map-eyebrow map-eyebrow-link" href="https://spacegates.org/">
            Spacegate Stellar Database
          </a>
          <div className="map-title-row">
            <img className="map-brand-mark" src="/favicon.svg" alt="" aria-hidden="true" />
            <h1>{mapTitle}</h1>
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
          {MAP_UTILITY_LINKS.map((item) => (
            item.external ? (
              <a
                key={item.label}
                href={item.href}
                className="map-hud-button map-utility-link"
                title={item.title}
                target="_blank"
                rel="noreferrer"
              >
                {item.label}
              </a>
            ) : (
              <Link key={item.label} to={item.href} className="map-hud-button map-utility-link" title={item.title}>
                {item.label}
              </Link>
            )
          ))}
          <Link to="/" className="map-hud-button">Search</Link>
          {selectedSystem?.system_id && (
            <Link to={systemDetailPath(selectedSystem)} className="map-hud-button primary">
              Detail
            </Link>
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
              <label className="map-menu-toggle">
                <input
                  type="checkbox"
                  checked={showDirectionLabels}
                  onChange={(event) => setShowDirectionLabels(event.target.checked)}
                  data-testid="map-direction-labels-toggle"
                />
                <span>Direction labels</span>
              </label>
              <span className="map-menu-note">Arrow keys always fly.</span>
            </div>
          </details>
        </nav>
      </header>

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
        <p className="map-desktop-hint">Desktop: drag look · wheel fly · L+R drag orbit · tilt wheel/RMB drag truck · MMB drag pedestal · {activeKeybind.hint}</p>
        <p className="map-touch-hint">Touch: drag look · tap/select reticle · two-finger pinch fly · two-finger drag pan</p>
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

      <aside className="map-hud map-contacts-panel">
        <details className="map-tray-section" open>
          <summary>
            <span className="map-panel-label">Selection History</span>
          </summary>
          <div className="map-history-list">
            {selectionHistory.map((system) => (
              <div
                key={system.system_id}
                role="button"
                tabIndex={0}
                className={`map-history-pill ${selectedSystem?.system_id === system.system_id ? "active" : ""}`}
                onClick={() => selectSystem(system, { openPeek: true })}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    selectSystem(system, { openPeek: true });
                  }
                }}
              >
                <SystemNameDisplay system={system} showCopyButton={false} showInfoButton={false} />
                <span>{formatNumber(system.dist_ly, 1)} ly</span>
                <span>{system.dominant_spectral_class}</span>
                <span>{formatNumber(system.planet_count, 0)}p</span>
              </div>
            ))}
          </div>
        </details>
        {selectedSystem && (
          <details className="map-tray-section" open>
            <summary>
              <span className="map-panel-label">Cool Stars Nearby</span>
            </summary>
            <div className="map-neighbor-list" aria-label="Suggested nearby systems">
              {suggestedNeighbors.slice(0, 8).map(({ system, routeDistance }) => (
                <button
                  type="button"
                  key={system.system_id}
                  className={`map-history-pill map-neighbor-chip ${selectedSystem?.system_id === system.system_id ? "active" : ""}`}
                  onClick={() => selectSystem(system, { openPeek: true, focus: drillMode === "explore" })}
                >
                  <SystemNameDisplay system={system} showCopyButton={false} showInfoButton={false} />
                  <span>{formatNumber(routeDistance, 1)} ly</span>
                  <span>{system.dominant_spectral_class}</span>
                  <span>{formatNumber(system.planet_count, 0)}p</span>
                </button>
              ))}
            </div>
          </details>
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
                  }
                }}
              >
                <span>System:</span>
                <SystemNameDisplay system={selectedSystem} showCopyButton={false} showInfoButton={false} />
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
              <Link to={systemDetailPath(selectedSystem)} className="map-command-button ghost">
                Detail
              </Link>
              <button type="button" className="map-command-button ghost" onClick={() => exitDrillMode()}>
                {drillMode === "peek" ? "Close" : "Back to Map"}
              </button>
            </div>
          </div>
          <div className="map-system-drill-body">
            <div className="map-system-vital-strip" aria-label={`${formatName(selectedSystem.display_name)} map vitals`}>
              <span>{formatNumber(selectedSystem.dist_ly, 2)} ly</span>
              <span>{selectedSystem.dominant_spectral_class}</span>
              <span>{formatNumber(selectedSystem.star_count, 0)} stars</span>
              <span>{formatNumber(selectedSystem.planet_count, 0)} planets</span>
              <span>cool {formatNumber(selectedSystem.coolness_score, 1)}</span>
              <span>rank {formatNumber(selectedSystem.coolness_rank, 0)}</span>
            </div>
            <React.Suspense fallback={<section className="panel system-preview-panel system-preview-loading">Loading System Simulation...</section>}>
              <SystemPreviewPanel
                systemId={selectedSystem.system_id}
                systemName={formatName(selectedSystem.display_name)}
                presentationMode={drillMode}
              />
            </React.Suspense>
          </div>
        </section>
      )}
    </div>
  );
}
