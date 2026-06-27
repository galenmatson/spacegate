import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import * as THREE from "three";
import { fetchMapSystems } from "./api.js";

const MAP_RADIUS_LY = 100;
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

  useEffect(() => () => geometry.dispose(), [geometry]);

  return (
    <points geometry={geometry}>
      <pointsMaterial
        size={0.16}
        sizeAttenuation
        vertexColors
        transparent
        opacity={0.86}
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
    : selected
      ? "rgba(125,251,255,0.9)"
      : "rgba(158,221,255,0.52)";
  const fill = selected ? "rgba(7,27,44,0.9)" : "rgba(4,10,22,0.78)";
  const ink = tone === "sol" ? "#fff5b8" : "#eef9ff";

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

function nearestSystemToReticle(camera, systems) {
  const origin = camera.position;
  const direction = new THREE.Vector3();
  camera.getWorldDirection(direction);
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
  controlsEnabled,
  stabilizationEnabled,
  onTelemetry,
  reticleSelectRequest,
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

  const selectReticleTarget = useCallback(() => {
    const target = nearestSystemToReticle(camera, systems);
    if (target) {
      onSelect(target);
    }
  }, [camera, onSelect, systems]);

  useEffect(() => {
    if (reticleSelectRequest > 0) {
      selectReticleTarget();
    }
  }, [reticleSelectRequest, selectReticleTarget]);

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
          selectReticleTarget();
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
    return () => {
      canvas.removeEventListener("pointerdown", onPointerDown);
      canvas.removeEventListener("pointermove", onPointerMove);
      canvas.removeEventListener("pointerup", onPointerEnd);
      canvas.removeEventListener("pointercancel", onPointerEnd);
    };
  }, [applyLookDelta, camera, gl.domElement, selectReticleTarget]);

  useFrame((_, delta) => {
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
  controlsEnabled,
  stabilizationEnabled,
  onTelemetry,
  onCanvasReady,
  reticleSelectRequest,
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
      <SelectionMarker system={selectedSystem} />
      <FlightControls
        systems={systems}
        onSelect={onSelect}
        controlsEnabled={controlsEnabled}
        stabilizationEnabled={stabilizationEnabled}
        onTelemetry={onTelemetry}
        reticleSelectRequest={reticleSelectRequest}
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
  const pageRef = useRef(null);
  const canvasRef = useRef(null);

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

  const priorityContacts = useMemo(
    () => systems
      .filter((system) => !isCatalogFallbackName(system.display_name))
      .sort((a, b) => b.map_priority - a.map_priority)
      .slice(0, 9),
    [systems],
  );

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
    <div className="map-page" ref={pageRef}>
      <div className="map-background-grid" aria-hidden="true" />
      {systems.length > 0 && (
        <StarMapScene
          systems={systems}
          selectedSystem={selectedSystem}
          onSelect={setSelectedSystem}
          controlsEnabled={controlsEnabled}
          stabilizationEnabled={stabilizationEnabled}
          onTelemetry={setTelemetry}
          onCanvasReady={(canvas) => {
            canvasRef.current = canvas;
          }}
          reticleSelectRequest={reticleSelectRequest}
        />
      )}

      <div className="map-reticle" aria-hidden="true" />

      <header className="map-hud map-hud-top">
        <div className="map-title-block">
          <span className="map-eyebrow">Spacegate 3D Pilot</span>
          <h1>Local Star Map</h1>
          <span className="map-build">100 ly · {buildId ? `build ${buildId}` : "build unknown"}</span>
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

      <aside className="map-hud map-status-panel">
        {loading && <strong>Loading 100 ly map...</strong>}
        {error && (
          <>
            <strong>Map data unavailable</strong>
            <span>{error}</span>
          </>
        )}
        {!loading && !error && summary && (
          <>
            <strong>{formatNumber(summary.returned, 0)} systems rendered</strong>
            <span>{formatNumber(summary.planet_systems, 0)} planet hosts · {formatNumber(summary.multi_star_systems, 0)} multi-star systems</span>
            <span>Frame: ICRS J2016, heliocentric, stabilized vertical</span>
          </>
        )}
      </aside>

      <aside className="map-hud map-selection-panel">
        <span className="map-panel-label">Selected System</span>
        {selectedSystem ? (
          <>
            <h2>
              <Link className="map-selection-title-link" to={systemDetailPath(selectedSystem)}>
                {selectedSystem.display_name}
              </Link>
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
              <div><dt>Snapshot</dt><dd>{selectedSystem.has_snapshot ? "Ready" : "Pending"}</dd></div>
            </dl>
          </>
        ) : (
          <p>Use Select reticle to lock the system under center view.</p>
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
            className="map-command-button ghost"
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
        <p className="map-desktop-hint">Desktop: WASD fly · Q up · Z down · Shift boost · capture mouse for look · Esc releases pointer</p>
        <p className="map-touch-hint">Touch: drag look · tap/select reticle · two-finger pinch fly · two-finger drag pan</p>
        <span>{telemetry.locked ? "Pointer locked" : "Pointer free"} · speed {formatNumber(telemetry.speedLyS, 1)} ly/s · range {formatNumber(telemetry.distLy, 1)} ly</span>
      </aside>

      <aside className="map-hud map-contacts-panel">
        <span className="map-panel-label">Priority Contacts</span>
        <div className="map-contact-list">
          {priorityContacts.map((system) => (
            <button
              key={system.system_id}
              type="button"
              className={selectedSystem?.system_id === system.system_id ? "active" : ""}
              onClick={() => setSelectedSystem(system)}
            >
              <span>{system.display_name}</span>
              <small>{formatNumber(system.dist_ly, 1)} ly · {system.dominant_spectral_class}</small>
            </button>
          ))}
        </div>
      </aside>
    </div>
  );
}
