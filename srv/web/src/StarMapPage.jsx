import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import * as THREE from "three";
import { apiUrl, fetchMapSystems, fetchSystemDetail } from "./api.js";

const MAP_RADIUS_LY = 100;
const SystemPreviewPanel = React.lazy(() => import("./SystemPreviewPanel.jsx"));
const LY_TO_SCENE = 0.55;
const WORLD_UP = new THREE.Vector3(0, 1, 0);
const KEYBOARD_BASE_SPEED = 7;
const KEYBOARD_BOOST_SPEED = 18;
const TOUCH_LOOK_SENSITIVITY = 0.003;
const TOUCH_PINCH_SPEED = 0.018;
const TOUCH_PAN_SPEED = 0.012;
const MOUSE_LOOK_SENSITIVITY = 0.002;
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

function SystemNameDisplay({ system, linkTo = null, className = "" }) {
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
      {isTruncated && (
        <button type="button" className="map-name-copy" onClick={copyName} aria-label={`Copy ${fullName}`}>
          {copied ? "Copied" : "Copy"}
        </button>
      )}
      {isTruncated && (
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

function SnapshotStatusChip({ system }) {
  const [open, setOpen] = useState(false);
  const [snapshot, setSnapshot] = useState(null);
  const [status, setStatus] = useState("idle");
  const hasSnapshot = Boolean(system?.has_snapshot);
  const systemId = system?.system_id;

  useEffect(() => {
    setSnapshot(null);
    setStatus("idle");
    setOpen(false);
  }, [systemId]);

  useEffect(() => {
    if (!open || !hasSnapshot || !systemId || snapshot) {
      return;
    }
    let active = true;
    setStatus("loading");
    fetchSystemDetail(systemId)
      .then((payload) => {
        if (!active) {
          return;
        }
        setSnapshot(payload?.system?.snapshot || null);
        setStatus(payload?.system?.snapshot?.url ? "ready" : "missing");
      })
      .catch(() => {
        if (active) {
          setStatus("error");
        }
      });
    return () => {
      active = false;
    };
  }, [hasSnapshot, open, snapshot, systemId]);

  const showPopover = open && hasSnapshot;
  const snapshotUrl = snapshot?.url ? apiUrl(snapshot.url) : "";
  return (
    <span
      className={`map-snapshot-chip ${hasSnapshot ? "ready" : "pending"}`}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
      tabIndex={0}
    >
      <span>Snapshot</span>
      <strong>{hasSnapshot ? "Ready" : "Pending"}</strong>
      {showPopover && (
        <span className="map-snapshot-popover" role="tooltip">
          {status === "loading" && <span>Loading snapshot...</span>}
          {status === "error" && <span>Snapshot metadata unavailable.</span>}
          {(status === "missing" || (status === "ready" && !snapshotUrl)) && <span>Snapshot manifest missing.</span>}
          {status === "ready" && snapshotUrl && (
            <>
              <img src={snapshotUrl} alt={`${formatName(system?.display_name || system?.system_name)} deterministic snapshot`} />
              <span>{snapshot.view_type || "system_card"} / {String(snapshot.params_hash || "").slice(0, 8) || "current"}</span>
            </>
          )}
        </span>
      )}
    </span>
  );
}

function mapToScenePosition(item) {
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

function prepareMapItems(rawItems) {
  return (rawItems || [])
    .map((item) => {
      const dominant = String(item.dominant_spectral_class || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";
      return {
        ...item,
        display_name: formatName(item.system_name),
        dominant_spectral_class: SPECTRAL_COLORS[dominant] ? dominant : "UNKNOWN",
        scene_position: mapToScenePosition(item),
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

function OrientationAxes() {
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

  const border = tone === "sol"
    ? "rgba(255,245,184,0.86)"
    : tone === "route"
      ? "rgba(151,255,207,0.9)"
    : selected
      ? "rgba(125,251,255,0.9)"
      : "rgba(158,221,255,0.52)";
  const fill = tone === "route"
    ? "rgba(5,29,24,0.82)"
    : selected
      ? "rgba(7,27,44,0.9)"
      : "rgba(4,10,22,0.78)";
  const ink = tone === "sol" ? "#fff5b8" : tone === "route" ? "#d9fff0" : "#eef9ff";

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

function LabelSprite({ label, position, selected = false, tone = "default", onSelect = null }) {
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
    const scale = THREE.MathUtils.clamp(distance / 18, 0.45, 2.8);
    spriteRef.current.scale.set((payload.width / 95) * scale, (payload.height / 95) * scale, 1);
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
      <spriteMaterial map={payload.texture} transparent depthWrite={false} depthTest={false} />
    </sprite>
  );
}

function PriorityLabels({ systems, selectedSystem, onSelect }) {
  const labelSystems = useMemo(() => {
    const seen = new Set();
    const candidates = [];
    const add = (system) => {
      if (!system?.system_id || seen.has(system.system_id)) {
        return;
      }
      seen.add(system.system_id);
      candidates.push(system);
    };
    if (selectedSystem) {
      add(selectedSystem);
    }
    systems
      .filter((system) => !isCatalogFallbackName(system.display_name))
      .sort((a, b) => b.map_priority - a.map_priority)
      .slice(0, 28)
      .forEach(add);
    return candidates;
  }, [selectedSystem, systems]);

  return (
    <group>
      {labelSystems.map((system) => (
        <LabelSprite
          key={system.system_id}
          label={system.display_name}
          position={system.scene_position}
          selected={selectedSystem?.system_id === system.system_id}
          onSelect={() => onSelect(system)}
        />
      ))}
    </group>
  );
}

function SelectionMarker({ system }) {
  if (!system) {
    return null;
  }
  return (
    <group position={system.scene_position}>
      <mesh>
        <sphereGeometry args={[0.34, 20, 20]} />
        <meshBasicMaterial color="#ffffff" transparent opacity={0.92} />
      </mesh>
      <mesh rotation={[Math.PI / 2, 0, 0]}>
        <ringGeometry args={[0.68, 0.74, 48]} />
        <meshBasicMaterial color="#7dfbff" transparent opacity={0.82} side={THREE.DoubleSide} />
      </mesh>
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

function RouteOverlays({ segments }) {
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
  const touchGestureRef = useRef({
    pointers: new Map(),
    lastPinchDistance: null,
    lastCentroid: null,
    primaryStart: null,
    moved: false,
  });
  const mouseDragRef = useRef({
    active: false,
    pointerId: null,
    lastX: 0,
    lastY: 0,
    startX: 0,
    startY: 0,
    startTime: 0,
    moved: false,
  });
  const focusRef = useRef(null);

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

  useEffect(() => {
    camera.position.set(0, 3.5, 17);
    camera.rotation.order = "YXZ";
    camera.rotation.set(pitchRef.current, yawRef.current, 0);
  }, [camera]);

  useEffect(() => {
    const movementKeys = new Set(["w", "a", "s", "d", "q", "z", "shift"]);
    const onKeyDown = (event) => {
      const key = event.key.toLowerCase();
      if (movementKeys.has(key) && (controlsEnabled || document.pointerLockElement === gl.domElement)) {
        event.preventDefault();
      }
      if (key === "escape") {
        return;
      }
      keysRef.current.add(key);
    };
    const onKeyUp = (event) => {
      keysRef.current.delete(event.key.toLowerCase());
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
  }, [applyLookDelta, controlsEnabled, gl.domElement, selectReticleTarget]);

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
        if (event.button !== 0 || document.pointerLockElement === canvas) {
          return;
        }
        event.preventDefault();
        try {
          canvas.setPointerCapture?.(event.pointerId);
        } catch {
          // Synthetic pointer events used by tests may not create capturable pointers.
        }
        mouseDrag.active = true;
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
        applyLookDelta(deltaX, deltaY, MOUSE_LOOK_SENSITIVITY);
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
        if (!mouseDrag.moved && duration < 320) {
          selectPointerTarget(event.clientX, event.clientY);
        }
        mouseDrag.active = false;
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

    canvas.addEventListener("pointerdown", onPointerDown, { passive: false });
    canvas.addEventListener("pointermove", onPointerMove, { passive: false });
    canvas.addEventListener("pointerup", onPointerEnd, { passive: false });
    canvas.addEventListener("pointercancel", onPointerEnd, { passive: false });
    canvas.addEventListener("contextmenu", openRouteContext);
    return () => {
      canvas.removeEventListener("pointerdown", onPointerDown);
      canvas.removeEventListener("pointermove", onPointerMove);
      canvas.removeEventListener("pointerup", onPointerEnd);
      canvas.removeEventListener("pointercancel", onPointerEnd);
      canvas.removeEventListener("contextmenu", openRouteContext);
    };
  }, [applyLookDelta, camera, gl.domElement, openRouteContext, selectPointerTarget, selectReticleTarget]);

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
    if (keys.has("w")) movement.add(direction);
    if (keys.has("s")) movement.sub(direction);
    if (keys.has("d")) movement.add(strafe);
    if (keys.has("a")) movement.sub(strafe);
    if (keys.has("q")) movement.add(WORLD_UP);
    if (keys.has("z")) movement.sub(WORLD_UP);
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
    }
  });

  return null;
}

function StarMapScene({
  systems,
  selectedSystem,
  onSelect,
  onRouteContext,
  routeSegments,
  controlsEnabled,
  stabilizationEnabled,
  onTelemetry,
  onCanvasReady,
  reticleSelectRequest,
  focusTarget,
  focusToken,
}) {
  return (
    <Canvas
      className="map-canvas"
      camera={{ fov: 62, near: 0.01, far: 1200, position: [0, 3.5, 17] }}
      gl={{ antialias: true, alpha: true, preserveDrawingBuffer: true, powerPreference: "high-performance" }}
      onCreated={({ gl }) => onCanvasReady(gl.domElement)}
    >
      <color attach="background" args={["#01030a"]} />
      <fog attach="fog" args={["#01030a", 80, 190]} />
      <DistanceRings />
      <OrientationAxes />
      <StarField systems={systems} />
      <PriorityLabels systems={systems} selectedSystem={selectedSystem} onSelect={onSelect} />
      <RouteOverlays segments={routeSegments} />
      <SelectionMarker system={selectedSystem} />
      <FlightControls
        systems={systems}
        onSelect={onSelect}
        onRouteContext={onRouteContext}
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
  const [systems, setSystems] = useState([]);
  const [summary, setSummary] = useState(null);
  const [selectedSystem, setSelectedSystem] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [controlsEnabled, setControlsEnabled] = useState(true);
  const [stabilizationEnabled, setStabilizationEnabled] = useState(true);
  const [reticleSelectRequest, setReticleSelectRequest] = useState(0);
  const [telemetry, setTelemetry] = useState({ distLy: 0, speedLyS: 0, locked: false });
  const [fullscreenAvailable, setFullscreenAvailable] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [routeSegments, setRouteSegments] = useState([]);
  const [routeMenu, setRouteMenu] = useState(null);
  const [selectionHistory, setSelectionHistory] = useState([]);
  const [drillMode, setDrillMode] = useState("flight");
  const [focusToken, setFocusToken] = useState(0);
  const pageRef = useRef(null);
  const canvasRef = useRef(null);

  const selectSystem = useCallback((system, options = {}) => {
    if (!system) {
      setSelectedSystem(null);
      setDrillMode("flight");
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
          setDrillMode("flight");
        }
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [drillMode]);

  useEffect(() => {
    const onWheel = (event) => {
      const inSystemDrill = event.target?.closest?.(".map-system-drill");
      if (drillMode !== "flight" && !inSystemDrill && event.deltaY > 60) {
        setDrillMode("flight");
      }
    };
    window.addEventListener("wheel", onWheel, { passive: true });
    return () => {
      window.removeEventListener("wheel", onWheel);
    };
  }, [drillMode]);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    fetchMapSystems({ max_dist_ly: String(MAP_RADIUS_LY), limit: "20000", compact: "true" })
      .then((payload) => {
        if (!active) {
          return;
        }
        const prepared = prepareMapItems(payload?.items || []);
        setSystems(prepared);
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
      .slice(0, 5);
    if (preferred.length >= 4) {
      return preferred;
    }
    return scored
      .sort((left, right) => right.score - left.score || left.routeDistance - right.routeDistance)
      .slice(0, 5);
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
    selectSystem(target);
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

  const requestPointerLock = () => {
    setControlsEnabled(true);
    canvasRef.current?.requestPointerLock?.();
  };

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
          routeSegments={routeSegments}
          controlsEnabled={controlsEnabled}
          stabilizationEnabled={stabilizationEnabled}
          onTelemetry={setTelemetry}
          onCanvasReady={(canvas) => {
            canvasRef.current = canvas;
          }}
          reticleSelectRequest={reticleSelectRequest}
          focusTarget={selectedSystem}
          focusToken={focusToken}
        />
      )}

      <div className="map-reticle" aria-hidden="true" />

      <header className="map-hud map-hud-top">
        <div className="map-title-block">
          <span className="map-eyebrow">Spacegate 3D Pilot</span>
          <h1>Local Star Map</h1>
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
              <span>ICRS J2016</span>
            </>
          )}
        </div>
        <nav className="map-actions" aria-label="Map actions">
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
          <label className="map-theme-select">
            <span className="sr-only">Theme</span>
            <select value={theme} onChange={(event) => setTheme(event.target.value)}>
              {themeOptions.map((option) => (
                <option key={option.id} value={option.id}>{option.label}</option>
              ))}
            </select>
          </label>
        </nav>
      </header>

      <aside className="map-hud map-selection-panel">
        <span className="map-panel-label">Selected System</span>
        {selectedSystem ? (
          <>
            <h2>
              <SystemNameDisplay system={selectedSystem} linkTo={systemDetailPath(selectedSystem)} />
            </h2>
            <div className="map-chip-row">
              <span className="map-chip">{formatNumber(selectedSystem.dist_ly, 2)} ly</span>
              <span className="map-chip spectral">{selectedSystem.dominant_spectral_class}</span>
              <span className="map-chip">{formatNumber(selectedSystem.star_count, 0)} stars</span>
              <span className="map-chip">{formatNumber(selectedSystem.planet_count, 0)} planets</span>
            </div>
            <dl className="map-fact-grid">
              <div><dt>Coolness</dt><dd>{formatNumber(selectedSystem.coolness_score, 2)}</dd></div>
              <div><dt>Rank</dt><dd>{formatNumber(selectedSystem.coolness_rank, 0)}</dd></div>
              <div><dt>Snapshot</dt><dd><SnapshotStatusChip system={selectedSystem} /></dd></div>
            </dl>
          </>
        ) : (
          <p>Click a star or use Select reticle to lock the center view.</p>
        )}
      </aside>

      <aside className="map-hud map-controls-panel">
        <span className="map-panel-label">Flight</span>
        <div className="map-control-buttons">
          <button type="button" className="map-command-button map-pointer-lock-command" onClick={requestPointerLock}>
            Capture mouse
          </button>
          <button
            type="button"
            className="map-command-button ghost map-reticle-command"
            onClick={() => setReticleSelectRequest((value) => value + 1)}
          >
            Select reticle
          </button>
          <button
            type="button"
            className={`map-command-button ghost ${stabilizationEnabled ? "active" : ""}`}
            onClick={() => setStabilizationEnabled((value) => !value)}
          >
            Stabilize
          </button>
        </div>
        <p className="map-desktop-hint">Desktop: drag canvas to look · click to select · WASD fly · Q/Z vertical · capture mouse for reticle flight</p>
        <p className="map-touch-hint">Touch: drag look · tap/select reticle · two-finger pinch fly · two-finger drag pan</p>
        <span>{telemetry.locked ? "Pointer locked" : "Pointer free"} · speed {formatNumber(telemetry.speedLyS, 1)} ly/s · range {formatNumber(telemetry.distLy, 1)} ly</span>
        {routeSegments.length > 0 && (
          <div className="map-route-summary">
            <span>{routeSegments.length} legs · {formatNumber(routeTotalLy, 2)} ly total</span>
            <ol className="map-route-leg-list">
              {routeSegments.slice(-4).map((segment) => (
                <li key={segment.id}>
                  <span>{shortDisplayName(segment.from.display_name)}</span>
                  <span>→</span>
                  <span>{shortDisplayName(segment.to.display_name)}</span>
                  <strong>{formatNumber(segment.distance_ly, 2)} ly</strong>
                </li>
              ))}
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
        <span className="map-panel-label">Selection History</span>
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
              <SystemNameDisplay system={system} />
              <span>{formatNumber(system.dist_ly, 1)} ly</span>
              <span>{system.dominant_spectral_class}</span>
              <span>{formatNumber(system.planet_count, 0)}p</span>
            </div>
          ))}
        </div>
      </aside>

      {routeMenu && (
        <div
          className="map-context-menu"
          style={{ left: `${routeMenu.x}px`, top: `${routeMenu.y}px` }}
          role="menu"
        >
          {routeMenu.target ? (
            <>
              <span className="map-panel-label">Route Tool</span>
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
                disabled={!selectedSystem || selectedSystem.system_id === routeMenu.target.system_id}
                onClick={addRouteSegment}
              >
                Measure from selected
              </button>
              <button
                type="button"
                className="map-context-command ghost"
                onClick={() => {
                  selectSystem(routeMenu.target, { openPeek: true });
                  setRouteMenu(null);
                }}
              >
                Select system
              </button>
              {routeSegments.length > 0 && (
                <button type="button" className="map-context-command ghost" onClick={clearRoute}>
                  Clear route
                </button>
              )}
            </>
          ) : (
            <>
              <span className="map-panel-label">Route Tool</span>
              <span>No system under cursor.</span>
              {routeSegments.length > 0 && (
                <button type="button" className="map-context-command ghost" onClick={clearRoute}>
                  Clear route
                </button>
              )}
            </>
          )}
          <button type="button" className="map-context-close" onClick={() => setRouteMenu(null)}>
            Close
          </button>
        </div>
      )}
      {selectedSystem?.system_id && drillMode !== "flight" && (
        <section
          className={`map-system-drill map-system-drill-${drillMode}`}
          data-testid="map-system-drill"
          data-drill-mode={drillMode}
          aria-label={`${formatName(selectedSystem.display_name)} system simulation`}
        >
          <div className="map-system-drill-bar">
            <div>
              <span className="map-panel-label">{drillMode === "peek" ? "System Simulation Peek" : "System Simulation Explore"}</span>
              <strong>{shortDisplayName(selectedSystem.display_name)}</strong>
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
              <button type="button" className="map-command-button ghost" onClick={() => setDrillMode("flight")}>
                Back to Map
              </button>
            </div>
          </div>
          <div className="map-system-drill-body">
            <React.Suspense fallback={<section className="panel system-preview-panel system-preview-loading">Loading System Simulation...</section>}>
              <SystemPreviewPanel
                systemId={selectedSystem.system_id}
                systemName={formatName(selectedSystem.display_name)}
                presentationMode={drillMode}
              />
            </React.Suspense>
            <aside className="map-system-suggestions" aria-label="Suggested nearby systems">
              <span className="map-panel-label">Next Nearby</span>
              {suggestedNeighbors.map(({ system, routeDistance, score }) => (
                <button
                  type="button"
                  key={system.system_id}
                  className="map-neighbor-button"
                  onClick={() => selectSystem(system, { openPeek: true, focus: drillMode === "explore" })}
                >
                  <strong>{shortDisplayName(system.display_name)}</strong>
                  <span>{formatNumber(routeDistance, 2)} ly · {system.dominant_spectral_class} · {formatNumber(system.planet_count, 0)}p</span>
                  <em>cool {formatNumber(system.coolness_score, 1)} · signal {formatNumber(score, 1)}</em>
                </button>
              ))}
            </aside>
          </div>
          <div className="map-system-drill-footer">
            <span>{drillMode === "peek" ? "Peek mode inspects this system without moving the map camera." : "Explore mode focuses the map around this system."}</span>
            <span>Esc or Back to Map returns to flight. AAA science layers will attach here after reviewed publication.</span>
          </div>
        </section>
      )}
    </div>
  );
}
