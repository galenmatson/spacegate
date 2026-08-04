import React from "react";

export const MAP_PLANET_BADGE_PALETTE = Object.freeze({
  ink: "#07131b",
  light: "#eff8f5",
  hot: "#ff6b45",
  temperate: "#e4c66f",
  cold: "#65c8e8",
});

export const MAP_PLANET_BAND_PALETTE = Object.freeze({
  hot: "#9f2940",
  temperate: "#87651d",
  cold: "#146f91",
});

export const MAP_PLANET_CATEGORIES = [
  { bit: 1, key: "hot_giant", queryKey: "hot_giant", tagKey: "science:planet.hot_gas_giant", filterLabel: "Hot Giant", label: "HG", temperature: "hot", kind: "giant", color: MAP_PLANET_BADGE_PALETTE.hot },
  { bit: 2, key: "temperate_giant", queryKey: "temperate_giant", tagKey: "science:planet.temperate_gas_giant", filterLabel: "Temperate Giant", label: "TG", temperature: "temperate", kind: "giant", color: MAP_PLANET_BADGE_PALETTE.temperate },
  { bit: 4, key: "cold_giant", queryKey: "cold_giant", tagKey: "science:planet.cold_gas_giant", filterLabel: "Cold Giant", label: "CG", temperature: "cold", kind: "giant", color: MAP_PLANET_BADGE_PALETTE.cold },
  { bit: 64, key: "hot_neptune", queryKey: "hot_neptune", tagKey: "science:planet.hot_neptunian", filterLabel: "Hot Neptunian", label: "HN", temperature: "hot", kind: "neptune", color: MAP_PLANET_BADGE_PALETTE.hot },
  { bit: 128, key: "temperate_neptune", queryKey: "temperate_neptune", tagKey: "science:planet.temperate_neptunian", filterLabel: "Temperate Neptunian", label: "TN", temperature: "temperate", kind: "neptune", color: MAP_PLANET_BADGE_PALETTE.temperate },
  { bit: 256, key: "cold_neptune", queryKey: "cold_neptune", tagKey: "science:planet.cold_neptunian", filterLabel: "Cold Neptunian", label: "CN", temperature: "cold", kind: "neptune", color: MAP_PLANET_BADGE_PALETTE.cold },
  { bit: 8, key: "hot_terrestrial", queryKey: "hot_terrestrial", tagKey: "science:planet.hot_terrestrial", filterLabel: "Hot Terrestrial", label: "HT", temperature: "hot", kind: "terrestrial", color: MAP_PLANET_BADGE_PALETTE.hot },
  { bit: 16, key: "temperate_terrestrial", queryKey: "temperate_terrestrial", tagKey: "science:planet.temperate_terrestrial", filterLabel: "Temperate Terrestrial", label: "TT", temperature: "temperate", kind: "terrestrial", color: MAP_PLANET_BADGE_PALETTE.temperate },
  { bit: 32, key: "cold_terrestrial", queryKey: "cold_terrestrial", tagKey: "science:planet.cold_terrestrial", filterLabel: "Cold Terrestrial", label: "CT", temperature: "cold", kind: "terrestrial", color: MAP_PLANET_BADGE_PALETTE.cold },
];

export const MAP_PLANET_UNKNOWN_BADGE = Object.freeze({
  bit: 512,
  key: "unclassified_planet",
  filterLabel: "Unclassified Planet",
  label: "?",
  temperature: "unknown",
  kind: "unknown",
  color: MAP_PLANET_BADGE_PALETTE.light,
});

export const MAP_PLANET_BADGE_STYLES = Object.fromEntries([
  ...MAP_PLANET_CATEGORIES.map((category) => [category.key, category]),
  [MAP_PLANET_UNKNOWN_BADGE.key, MAP_PLANET_UNKNOWN_BADGE],
]);

export function planetCategoryForKey(value) {
  return MAP_PLANET_BADGE_STYLES[String(value || "").trim().toLowerCase()] || MAP_PLANET_UNKNOWN_BADGE;
}

export function planetCategoryTitle(value) {
  return planetCategoryForKey(value).filterLabel;
}

export function MapPlanetCategoryIcon({ category, categoryKey = null, className = "" }) {
  const resolved = category || planetCategoryForKey(categoryKey);
  const accent = resolved?.color || MAP_PLANET_BADGE_PALETTE.temperate;
  const band = MAP_PLANET_BAND_PALETTE[resolved?.temperature] || MAP_PLANET_BADGE_PALETTE.ink;
  const kind = resolved?.kind || "unknown";
  const isGiant = kind === "giant";
  const isNeptune = kind === "neptune";
  const isTerrestrial = kind === "terrestrial";
  return (
    <svg
      className={`map-planet-category-icon ${className}`.trim()}
      viewBox="0 0 32 32"
      aria-hidden="true"
      data-planet-kind={kind}
      data-planet-temperature={resolved?.temperature || "unknown"}
      data-planet-category={resolved?.key || "unclassified_planet"}
    >
      {isGiant && <path d="M1.2 16 A14.8 5.7 0 0 1 30.8 16" transform="rotate(-31 16 16)" fill="none" stroke={MAP_PLANET_BADGE_PALETTE.light} strokeWidth="1.7" strokeLinecap="round" />}
      {isGiant && <ellipse cx="16" cy="16" rx="10.4" ry="8.4" fill={accent} stroke={MAP_PLANET_BADGE_PALETTE.light} strokeWidth="1.4" />}
      {isNeptune && <circle cx="16" cy="16" r="8.4" fill={accent} stroke={MAP_PLANET_BADGE_PALETTE.light} strokeWidth="1.4" />}
      {isTerrestrial && <circle cx="16" cy="16" r="6.5" fill={accent} stroke={MAP_PLANET_BADGE_PALETTE.light} strokeWidth="1.4" />}
      {(isGiant || isNeptune) && (
        <path d={isGiant ? "M8.2 13.1 C12.6 11.9 19.4 14.3 23.8 13.1 M7.2 16.2 C12.4 17.4 19.7 15 24.8 16.2 M8.2 19.1 C13.2 18 19 20.1 23.8 19.1" : "M9.6 13.1 C12.6 11.9 19.4 14.3 22.4 13.1 M8.6 16.2 C12.4 17.4 19.7 15 23.4 16.2 M9.6 19.1 C13.2 18 19 20.1 22.4 19.1"} fill="none" stroke={band} strokeWidth={isGiant ? "1.45" : "1.3"} strokeLinecap="round" />
      )}
      {isTerrestrial && (
        <>
          <circle cx="13.1" cy="13.4" r="1.3" fill={MAP_PLANET_BADGE_PALETTE.ink} opacity="0.78" />
          <circle cx="18.8" cy="18.7" r="1" fill={MAP_PLANET_BADGE_PALETTE.ink} opacity="0.68" />
          <circle cx="19.2" cy="12.4" r="0.65" fill={MAP_PLANET_BADGE_PALETTE.light} opacity="0.78" />
        </>
      )}
      {isGiant && <path d="M1.2 16 A14.8 5.7 0 0 0 30.8 16" transform="rotate(-31 16 16)" fill="none" stroke={MAP_PLANET_BADGE_PALETTE.light} strokeWidth="1.7" strokeLinecap="round" />}
      {kind === "unknown" && (
        <>
          <circle cx="16" cy="16" r="7" fill="none" stroke={MAP_PLANET_BADGE_PALETTE.light} strokeWidth="1.4" strokeDasharray="2 1.6" />
          <text x="16" y="19.3" textAnchor="middle" fill={MAP_PLANET_BADGE_PALETTE.light} fontFamily="ui-monospace, monospace" fontSize="10" fontWeight="800">?</text>
        </>
      )}
    </svg>
  );
}

export function PlanetCategoryBadge({ categoryKey, className = "", title = null }) {
  const category = planetCategoryForKey(categoryKey);
  return (
    <span
      className={`planet-category-badge ${className}`.trim()}
      title={title || category.filterLabel}
      aria-label={title || category.filterLabel}
      data-planet-category={category.key}
    >
      <MapPlanetCategoryIcon category={category} />
    </span>
  );
}
