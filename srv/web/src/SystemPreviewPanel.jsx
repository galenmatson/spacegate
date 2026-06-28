import React, { useEffect, useMemo, useState } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import * as THREE from "three";
import { fetchSystemSimulationScene } from "./api.js";

const PLANET_COLORS = ["#75b7ff", "#e6c56f", "#e78a6b", "#9dd9a5", "#c49bf2", "#82d6d8", "#d7dee8"];
const STAR_COLOR_BY_TEMP = [
  [10000, "#b8d7ff"],
  [7500, "#dceaff"],
  [6000, "#fff2b7"],
  [5000, "#ffd37d"],
  [3500, "#ff9d6b"],
  [0, "#ff6f5e"],
];

function numericField(fields, key) {
  const field = (fields || []).find((item) => item?.key === key);
  const value = Number(field?.value);
  return Number.isFinite(value) ? value : null;
}

function fieldStatus(fields, key) {
  const field = (fields || []).find((item) => item?.key === key);
  return field?.status || "missing";
}

function hashAngle(value) {
  const text = String(value || "");
  let hash = 0;
  for (let idx = 0; idx < text.length; idx += 1) {
    hash = (hash * 31 + text.charCodeAt(idx)) >>> 0;
  }
  return (hash / 0xffffffff) * Math.PI * 2;
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

function PreviewObjects({ stars, planets }) {
  const groupRef = React.useRef(null);
  useFrame((_, delta) => {
    if (groupRef.current) {
      groupRef.current.rotation.y += delta * 0.045;
    }
  });

  const maxOrbit = Math.max(
    0.1,
    ...planets.map((planet) => planet.orbitAu || 0.1),
  );

  return (
    <group ref={groupRef}>
      <ambientLight intensity={0.7} />
      <pointLight position={[0, 0, 0]} intensity={2.5} distance={26} />
      {stars.map((star, idx) => {
        const radius = Math.min(1.35, Math.max(0.46, Math.sqrt(star.radiusRsun || 0.55) * 0.55));
        const offset = stars.length > 1 ? (idx - (stars.length - 1) / 2) * 0.72 : 0;
        return (
          <mesh key={star.key} position={[offset, 0, 0]}>
            <sphereGeometry args={[radius, 32, 24]} />
            <meshStandardMaterial color={star.color} emissive={star.color} emissiveIntensity={0.9} roughness={0.45} />
          </mesh>
        );
      })}
      {planets.map((planet, idx) => {
        const orbitRadius = 1.8 + Math.sqrt((planet.orbitAu || 0.08) / maxOrbit) * 5.6;
        const angle = hashAngle(planet.key || planet.name);
        const x = Math.cos(angle) * orbitRadius;
        const z = Math.sin(angle) * orbitRadius;
        const radius = Math.min(0.28, Math.max(0.08, Math.sqrt(planet.radiusEarth || 1) * 0.065));
        return (
          <React.Fragment key={planet.key}>
            <mesh rotation={[Math.PI / 2, 0, 0]}>
              <torusGeometry args={[orbitRadius, 0.006, 8, 128]} />
              <meshBasicMaterial color="#b1d6ff" transparent opacity={0.42} />
            </mesh>
            <mesh position={[x, 0, z]}>
              <sphereGeometry args={[radius, 18, 14]} />
              <meshStandardMaterial color={PLANET_COLORS[idx % PLANET_COLORS.length]} roughness={0.58} metalness={0.08} />
            </mesh>
          </React.Fragment>
        );
      })}
    </group>
  );
}

function SceneCanvas({ scene }) {
  const stars = useMemo(() => {
    const readinessStars = scene?.simulation_readiness?.stars || [];
    const bodyStars = scene?.bodies?.stars || [];
    return (readinessStars.length ? readinessStars : bodyStars).map((star, idx) => {
      const fields = star.fields || [];
      const teffK = numericField(fields, "teff_k") || Number(star.teff_k || 0);
      return {
        key: star.stable_object_key || star.object_id || star.star_id || `star-${idx}`,
        name: star.display_name || star.star_name || `Star ${idx + 1}`,
        radiusRsun: numericField(fields, "radius_rsun") || Number(star.radius_rsun || 0.55),
        teffK,
        color: starColor(teffK),
      };
    });
  }, [scene]);

  const planets = useMemo(() => {
    const readinessPlanets = scene?.simulation_readiness?.planets || [];
    const bodyPlanets = scene?.bodies?.planets || [];
    return (readinessPlanets.length ? readinessPlanets : bodyPlanets).map((planet, idx) => {
      const fields = planet.fields || [];
      return {
        key: planet.stable_object_key || planet.object_id || planet.planet_id || `planet-${idx}`,
        name: planet.display_name || planet.planet_name || `Planet ${idx + 1}`,
        orbitAu: numericField(fields, "semi_major_axis_au") || Number(planet.semi_major_axis_au || 0.08 + idx * 0.08),
        radiusEarth: numericField(fields, "radius_earth") || Number(planet.radius_earth || 1),
        orbitStatus: fieldStatus(fields, "semi_major_axis_au"),
      };
    });
  }, [scene]);

  return (
    <Canvas camera={{ position: [0, 6.2, 10.8], fov: 43 }} dpr={[1, 1.75]}>
      <color attach="background" args={["#050b12"]} />
      <PreviewObjects stars={stars} planets={planets} />
    </Canvas>
  );
}

export default function SystemPreviewPanel({ systemId, systemName }) {
  const [scene, setScene] = useState(null);
  const [status, setStatus] = useState("loading");

  useEffect(() => {
    let active = true;
    setStatus("loading");
    setScene(null);
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
  const planetReadiness = scene?.simulation_readiness?.planets || [];
  const assumedOrbitCount = planetReadiness.filter((planet) => fieldStatus(planet.fields, "semi_major_axis_au") === "assumed").length;
  const missingOrbitCount = planetReadiness.filter((planet) => fieldStatus(planet.fields, "semi_major_axis_au") === "missing").length;

  return (
    <section className="panel system-preview-panel" data-testid="system-preview-panel">
      <div className="system-preview-header">
        <div>
          <h3>Live System Preview</h3>
          <p>Beta renderer from the simulation-scene contract. Static snapshot remains the fallback.</p>
        </div>
        {scene?.schema_version && <span className="status-chip">{scene.schema_version}</span>}
      </div>
      <div className="system-preview-layout">
        <div className="system-preview-canvas" aria-label={`${systemName} live system preview`}>
          {status === "ready" && scene ? <SceneCanvas scene={scene} /> : <div className="system-preview-fallback">{status === "error" ? "Preview unavailable" : "Loading preview..."}</div>}
        </div>
        <div className="system-preview-readout">
          <div>
            <strong>{formatNumber(bodies.stars?.length, 0)}</strong>
            <span>stars</span>
          </div>
          <div>
            <strong>{formatNumber(bodies.planets?.length, 0)}</strong>
            <span>planets</span>
          </div>
          <div>
            <strong>{formatNumber((readiness.score || 0) * 100, 0)}%</strong>
            <span>readiness</span>
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
            <strong>{formatNumber((counts.assumed || 0) + assumedOrbitCount, 0)}</strong>
            <span>assumptions</span>
          </div>
          <div>
            <strong>{formatNumber((counts.missing || 0) + missingOrbitCount, 0)}</strong>
            <span>missing inputs</span>
          </div>
        </div>
      </div>
    </section>
  );
}
