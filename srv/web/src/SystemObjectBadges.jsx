import React from "react";

import { StellarClassChips, stellarClassTokensFromSystem } from "./stellarClassTags.jsx";

function stellarObjectsFor(system, stellarObjects) {
  if (Array.isArray(stellarObjects) && stellarObjects.length) {
    return stellarObjects;
  }
  if (Array.isArray(system?.stellar_object_badges) && system.stellar_object_badges.length) {
    return system.stellar_object_badges;
  }
  const classes = Array.isArray(system?.stellar_class_badges) && system.stellar_class_badges.length
    ? system.stellar_class_badges
    : stellarClassTokensFromSystem(system);
  return classes.map((classificationValue, index) => ({
    classification_value: classificationValue,
    display_name: `Stellar member ${index + 1}`,
    fallback_key: `stellar-${index}`,
  }));
}

function planetsFor(system, planets) {
  if (Array.isArray(planets)) {
    return planets;
  }
  return Array.isArray(system?.planet_object_badges) ? system.planet_object_badges : [];
}

function planetDisplayName(planet, index) {
  return String(planet?.display_name || planet?.planet_name || `Planet ${index + 1}`).trim();
}

export function SystemObjectBadges({
  system = null,
  stellarObjects = null,
  planets = null,
  className = "",
  size = "compact",
}) {
  const stars = stellarObjectsFor(system, stellarObjects);
  const planetRows = planetsFor(system, planets);
  if (!stars.length && !planetRows.length) {
    return null;
  }
  return (
    <span className={`system-object-badges ${className}`.trim()} aria-label="Known stellar and planetary objects">
      {stars.map((star, index) => (
        <span
          className="system-object-badge system-object-badge-star"
          key={star.hierarchy_node_key || star.leaf_component_key || star.stable_object_key || star.fallback_key || `star-${index}`}
          title={star.display_name || `Stellar member ${index + 1}`}
        >
          <StellarClassChips tokens={[star.classification_value || "UNKNOWN"]} size={size} />
        </span>
      ))}
      {planetRows.map((planet, index) => {
        const displayName = planetDisplayName(planet, index);
        return (
          <span
            className="system-object-badge system-object-badge-planet"
            key={planet.stable_object_key || planet.planet_id_text || planet.planet_id || `planet-${index}`}
            title={displayName}
          >
            <span className="system-object-planet-icon" aria-hidden="true" />
            <span>{displayName}</span>
          </span>
        );
      })}
    </span>
  );
}
