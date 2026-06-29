import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import * as THREE from "three";
import { fetchSystemSimulationScene } from "./api.js";

const PLANET_COLORS = ["#75b7ff", "#e6c56f", "#e78a6b", "#9dd9a5", "#c49bf2", "#82d6d8", "#d7dee8"];
const SIM_DAYS_PER_SECOND = 0.7;
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

function statusLabel(status) {
  return String(status || "missing").toUpperCase();
}

function formatFieldValue(field) {
  if (!field) {
    return "Unknown";
  }
  const value = Number(field.value);
  const display = Number.isFinite(value) ? formatNumber(value, Math.abs(value) >= 10 ? 1 : 3) : String(field.value ?? "Unknown");
  return field.unit ? `${display} ${field.unit}` : display;
}

function EvidencePill({ field, fallbackStatus = "missing" }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const status = field?.status || fallbackStatus;
  const copyPayload = useCallback(() => {
    if (!navigator?.clipboard || !field) {
      return;
    }
    navigator.clipboard.writeText(JSON.stringify(field, null, 2)).then(() => {
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
          {field?.seed && <span>Seed: {field.seed}</span>}
          {field?.generator_version && <span>Generator: {field.generator_version}</span>}
          {field?.replacement_target && <span>Replace with: {field.replacement_target}</span>}
        </span>
      )}
    </span>
  );
}

function StarSphere({ star, position = [0, 0, 0] }) {
  const radiusRsun = numericField(star.fields, "radius_rsun") || Number(star.radiusRsun || 0.55);
  const radius = Math.min(1.35, Math.max(0.18, Math.sqrt(radiusRsun || 0.55) * 0.45));
  const teffK = numericField(star.fields, "teff_k") || Number(star.teffK || 0);
  const color = teffK ? starColor(teffK) : (STAR_COLORS[String(star.spectral_class || "").slice(0, 1)] || "#ff9d6b");
  return (
    <mesh position={position}>
      <sphereGeometry args={[radius, 32, 24]} />
      <meshStandardMaterial color={color} emissive={color} emissiveIntensity={0.88} roughness={0.45} />
    </mesh>
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

  return { canonicalKeyByAlias, starPositions, orbitCenters, orbitStarKeys };
}

function BinaryOrbit({ orbit, starsByKey, center = [0, 0, 0] }) {
  const groupRef = React.useRef(null);
  const primary = starsByKey.get(orbit.primary_body_key);
  const secondary = starsByKey.get(orbit.secondary_body_key);
  const periodDays = Math.max(0.05, numericField(orbit.fields, "period_days") || 8);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(orbit.fields, "eccentricity") || 0));
  const phaseRad = numericField(orbit.fields, "phase_rad") || 0;
  const inclinationDeg = numericField(orbit.fields, "inclination_deg") || 0;
  const orbitRadius = Number(orbit.display_radius_scene) || 0.9;

  useFrame(({ clock }) => {
    if (!groupRef.current) {
      return;
    }
    const theta = phaseRad + ((clock.elapsedTime * SIM_DAYS_PER_SECOND) / periodDays) * Math.PI * 2;
    groupRef.current.rotation.set(THREE.MathUtils.degToRad(inclinationDeg), -theta, 0);
    groupRef.current.position.set(center[0], center[1], center[2]);
  });

  if (!primary || !secondary) {
    return null;
  }
  const displayRadius = orbitRadius * (1 + eccentricity * 0.18);
  return (
    <group ref={groupRef} data-testid="system-preview-binary-orbit">
      <mesh rotation={[Math.PI / 2, 0, 0]}>
        <torusGeometry args={[orbitRadius, 0.006, 8, 128]} />
        <meshBasicMaterial color="#ffdca8" transparent opacity={0.36} />
      </mesh>
      <StarSphere star={primary} position={[-displayRadius * 0.5, 0, 0]} />
      <StarSphere star={secondary} position={[displayRadius * 0.5, 0, 0]} />
    </group>
  );
}

function PlanetObject({ planet, orbitRadius, color, center = [0, 0, 0] }) {
  const meshRef = React.useRef(null);
  const periodDays = Math.max(0.05, numericField(planet.fields, "orbital_period_days") || Number(planet.periodDays) || 8 + orbitRadius * 2.2);
  const eccentricity = Math.min(0.85, Math.max(0, numericField(planet.fields, "eccentricity") || Number(planet.eccentricity) || 0));
  const phaseRad = numericField(planet.fields, "phase_rad") || Number(planet.phaseRad) || 0;
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  const inclinationRad = THREE.MathUtils.degToRad(inclinationDeg);

  const positionAtPhase = useCallback((phase) => {
    const radiusScale = (1 - eccentricity ** 2) / (1 + eccentricity * Math.cos(phase));
    const x = Math.cos(phase) * orbitRadius * radiusScale;
    const planeZ = Math.sin(phase) * orbitRadius * radiusScale;
    return [x, -planeZ * Math.sin(inclinationRad), planeZ * Math.cos(inclinationRad)];
  }, [eccentricity, inclinationRad, orbitRadius]);

  useFrame(({ clock }) => {
    if (!meshRef.current) {
      return;
    }
    const meanAnomaly = phaseRad + ((clock.elapsedTime * SIM_DAYS_PER_SECOND) / periodDays) * Math.PI * 2;
    meshRef.current.position.set(...addVector(center, positionAtPhase(meanAnomaly)));
  });

  return (
    <mesh ref={meshRef} position={addVector(center, positionAtPhase(phaseRad))}>
      <sphereGeometry args={[planet.radius, 18, 14]} />
      <meshStandardMaterial color={color} roughness={0.58} metalness={0.08} />
    </mesh>
  );
}

function PlanetOrbitRing({ planet, orbitRadius, center = [0, 0, 0] }) {
  const inclinationDeg = numericField(planet.fields, "inclination_deg") || 0;
  return (
    <mesh position={center} rotation={[Math.PI / 2 + THREE.MathUtils.degToRad(inclinationDeg), 0, 0]}>
      <torusGeometry args={[orbitRadius, 0.006, 8, 128]} />
      <meshBasicMaterial color="#b1d6ff" transparent opacity={0.42} />
    </mesh>
  );
}

function PreviewObjects({ stars, planets, hierarchy }) {
  const binaryOrbits = planets.renderOrbits || [];
  const layout = useMemo(() => buildStarLayout(stars, hierarchy, binaryOrbits), [stars, hierarchy, binaryOrbits]);
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
  stars.forEach((star) => {
    const starId = star?.source?.star_id;
    const key = star.render_key || star.key;
    if (starId !== undefined && starId !== null && key && layout.starPositions.has(key)) {
      starCenterByCoreId.set(Number(starId), layout.starPositions.get(key));
    }
  });
  const maxOrbit = Math.max(
    0.1,
    ...planets.map((planet) => planet.orbitAu || 0.1),
  );
  const hostCenterForPlanet = (planet) => {
    const hostKey = layout.canonicalKeyByAlias.get(planet.host_body_key) || planet.host_body_key;
    if (hostKey && layout.starPositions.has(hostKey)) {
      return layout.starPositions.get(hostKey);
    }
    if (hostKey) {
      const star = starsByKey.get(hostKey);
      const starKey = star?.render_key || star?.key;
      if (starKey && layout.starPositions.has(starKey)) {
        return layout.starPositions.get(starKey);
      }
    }
    const hostStarId = Number(planet.host_star_id);
    if (Number.isFinite(hostStarId) && starCenterByCoreId.has(hostStarId)) {
      return starCenterByCoreId.get(hostStarId);
    }
    return [0, 0, 0];
  };

  return (
    <group>
      <ambientLight intensity={0.7} />
      <pointLight position={[0, 0, 0]} intensity={2.5} distance={26} />
      {binaryOrbits.map((orbit, idx) => (
        <BinaryOrbit
          key={orbit.orbit_key || idx}
          orbit={orbit}
          starsByKey={starsByKey}
          center={layout.orbitCenters.get(orbit.orbit_key || `orbit-${idx}`) || [0, 0, 0]}
        />
      ))}
      {looseStars.map((star) => (
        <StarSphere
          key={star.render_key || star.key}
          star={star}
          position={layout.starPositions.get(star.render_key || star.key) || [0, 0, 0]}
        />
      ))}
      {planets.map((planet, idx) => {
        const orbitRadius = 0.75 + Math.sqrt((planet.orbitAu || 0.08) / maxOrbit) * 2.7;
        const center = hostCenterForPlanet(planet);
        return (
          <React.Fragment key={planet.key}>
            <PlanetOrbitRing planet={planet} orbitRadius={orbitRadius} center={center} />
            <PlanetObject planet={planet} orbitRadius={orbitRadius} center={center} color={PLANET_COLORS[idx % PLANET_COLORS.length]} />
          </React.Fragment>
        );
      })}
    </group>
  );
}

function SceneCanvas({ scene }) {
  const stars = useMemo(() => {
    const renderStars = scene?.render_scene?.bodies?.stars || [];
    if (renderStars.length) {
      return renderStars;
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
        teffK,
        color: starColor(teffK),
      };
    });
  }, [scene]);

  const planets = useMemo(() => {
    const renderScene = scene?.render_scene;
    const renderPlanets = renderScene?.bodies?.planets || [];
    if (renderPlanets.length) {
      const mapped = renderPlanets.map((planet, idx) => ({
        ...planet,
        key: planet.render_key || planet.stable_object_key || `planet-${idx}`,
        orbitAu: numericField(planet.fields, "semi_major_axis_au") || 0.08 + idx * 0.08,
        radius: Math.min(0.28, Math.max(0.08, Math.sqrt(numericField(planet.fields, "radius_earth") || 1) * 0.065)),
      }));
      mapped.renderOrbits = renderScene?.orbits || [];
      return mapped;
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
        radius: Math.min(0.28, Math.max(0.08, Math.sqrt(numericField(fields, "radius_earth") || Number(planet.radius_earth || 1)) * 0.065)),
        radiusEarth: numericField(fields, "radius_earth") || Number(planet.radius_earth || 1),
        orbitStatus: fieldStatus(fields, "semi_major_axis_au"),
      };
    });
  }, [scene]);

  return (
    <Canvas camera={{ position: [0, 6.2, 10.8], fov: 43 }} dpr={[1, 1.75]}>
      <color attach="background" args={["#050b12"]} />
      <PreviewObjects stars={stars} planets={planets} hierarchy={scene?.hierarchy} />
    </Canvas>
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
  const renderScene = scene?.render_scene || {};
  const renderBodies = renderScene.bodies || {};
  const renderOrbits = renderScene.orbits || [];
  const evidenceFields = collectEvidenceFields(scene);
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
        {renderScene?.schema_version ? <span className="status-chip">{renderScene.schema_version}</span> : (scene?.schema_version && <span className="status-chip">{scene.schema_version}</span>)}
      </div>
      <div className="system-preview-layout">
        <div className="system-preview-canvas" aria-label={`${systemName} live system preview`}>
          {status === "ready" && scene ? <SceneCanvas scene={scene} /> : <div className="system-preview-fallback">{status === "error" ? "Preview unavailable" : "Loading preview..."}</div>}
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
            <strong>{formatNumber(renderOrbits.length, 0)}</strong>
            <span>rendered orbits</span>
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
