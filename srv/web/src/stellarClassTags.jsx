import React from "react";

export const STELLAR_CLASS_ORDER = [
  "O",
  "B",
  "A",
  "F",
  "G",
  "K",
  "M",
  "L",
  "T",
  "Y",
  "WR",
  "WD",
  "NS",
  "PULSAR",
  "MAGNETAR",
  "BLACK HOLE",
  "U",
];

export const STELLAR_CLASS_TAGS = {
  O: {
    label: "O",
    name: "O-Type Star",
    color: "#6aa9ff",
    text: "O-type stars are the most massive, hottest, and brightest main-sequence stars. They burn blue-white, emit intense ultraviolet radiation, live only a few million years, and shape galactic evolution through powerful winds and supernovae.",
  },
  B: {
    label: "B",
    name: "B-Type Star",
    color: "#8cc8ff",
    text: "B-type stars are extremely luminous blue stars. They are less extreme than O stars but still massive, short-lived, and common in young stellar associations.",
  },
  A: {
    label: "A",
    name: "A-Type Star",
    color: "#d7e9ff",
    text: "A-type stars are young, hot, white to blue-white stars. Sirius and Vega are famous examples, and their strong radiation environments can make habitable planets challenging.",
  },
  F: {
    label: "F",
    name: "F-Type Star",
    color: "#fff2b5",
    text: "F-type stars are yellow-white stars slightly hotter and more massive than the Sun. They can live for billions of years but emit more ultraviolet light than cooler Sun-like stars.",
  },
  G: {
    label: "G",
    name: "G-Type Star",
    color: "#ffd86b",
    text: "G-type stars are stable yellow-white stars like the Sun. Their long steady lifetimes make them important targets when thinking about complex chemistry and long-term habitability.",
  },
  K: {
    label: "K",
    name: "K-Type Star",
    color: "#ffb36a",
    text: "K-type orange dwarfs are cooler and less massive than the Sun. Their long lifetimes and calmer radiation make them excellent candidates for ancient planetary systems.",
  },
  M: {
    label: "M",
    name: "M-Type Star",
    color: "#f06a55",
    text: "M-type red dwarfs are the smallest, coolest, and most common stars. They can live for trillions of years, but close-in planets may face flares and tidal locking.",
  },
  L: {
    label: "L",
    name: "L-Type Brown Dwarf",
    color: "#cf6b57",
    text: "L-type objects bridge the lowest-mass stars and brown dwarfs. They are dim, infrared-bright, and cool enough for exotic clouds of minerals and metals.",
  },
  T: {
    label: "T",
    name: "T-Type Brown Dwarf",
    color: "#8f6bc7",
    text: "T-type brown dwarfs are cool failed stars with methane-rich atmospheres. They emit almost no visible light and resemble isolated giant planets.",
  },
  Y: {
    label: "Y",
    name: "Y-Type Brown Dwarf",
    color: "#6fc7d8",
    text: "Y-type brown dwarfs are the coldest known brown dwarfs. Some can approach room temperature and are mainly detectable in infrared light.",
  },
  WR: {
    label: "WR",
    name: "Wolf-Rayet Star",
    color: "#71f6ff",
    text: "Wolf-Rayet stars are rare, hyper-luminous, unstable massive stars losing their outer layers through extreme stellar winds before likely supernova deaths.",
  },
  WD: {
    label: "WD",
    name: "White Dwarf",
    color: "#d7dee8",
    text: "A white dwarf is the dense glowing remnant of a low-to-medium mass star. It no longer fuses fuel and shines from stored heat while slowly cooling.",
  },
  NS: {
    label: "NS",
    name: "Neutron Star",
    color: "#b9a7ff",
    text: "A neutron star is the ultra-dense collapsed core of a massive star after a supernova, packing more than a Sun's mass into a city-sized object.",
  },
  PULSAR: {
    label: "PULSAR",
    name: "Pulsar",
    color: "#9bffef",
    text: "A pulsar is a rapidly rotating magnetized neutron star whose radiation beams sweep across space like a cosmic lighthouse.",
  },
  MAGNETAR: {
    label: "MAGNETAR",
    name: "Magnetar",
    color: "#ff6df0",
    text: "A magnetar is a rare neutron star with an extreme magnetic field, capable of violent starquakes and powerful X-ray or gamma-ray bursts.",
  },
  "BLACK HOLE": {
    label: "BLACK HOLE",
    name: "Black Hole",
    color: "#ffcf4a",
    text: "A black hole is a region where gravity is so strong that nothing, not even light, can escape past the event horizon. Its presence is inferred from surrounding matter and companion motion.",
  },
  U: {
    label: "U",
    name: "Unknown Stellar Class",
    color: "#8794a8",
    text: "This object has no reliable stellar class available in the current Spacegate slice or payload. It may be missing spectral evidence, unresolved in a subsystem, or awaiting stronger catalog reconciliation.",
  },
};

function fieldValue(fields, key) {
  if (!fields) {
    return "";
  }
  if (Array.isArray(fields)) {
    const match = fields.find((item) => item?.key === key);
    return match?.value ?? "";
  }
  return fields[key]?.value ?? fields[key] ?? "";
}

function addToken(tokens, rawToken) {
  const token = String(rawToken || "").trim().toUpperCase();
  if (!token) {
    return;
  }
  if (token === "D") {
    tokens.add("WD");
    return;
  }
  if (token === "BH" || token === "BLACKHOLE") {
    tokens.add("BLACK HOLE");
    return;
  }
  if (STELLAR_CLASS_TAGS[token]) {
    tokens.add(token);
  }
}

export function stellarClassTokensFromText(value) {
  const text = String(value || "").trim();
  const upper = text.toUpperCase();
  const tokens = new Set();
  if (!upper) {
    return [];
  }
  if (/\bBLACK[\s_-]*HOLE\b|\bBH\b/.test(upper)) {
    tokens.add("BLACK HOLE");
  }
  if (/\bMAGNETAR\b/.test(upper)) {
    tokens.add("NS");
    tokens.add("MAGNETAR");
  }
  if (/\bPULSAR\b|\bPSR\b/.test(upper)) {
    tokens.add("NS");
    tokens.add("PULSAR");
  }
  if (/\bNEUTRON[\s_-]*STAR\b|\bNS\b/.test(upper)) {
    tokens.add("NS");
  }
  if (/\bWHITE[\s_-]*DWARF\b|\bWD\b/.test(upper) || /^D[A-Z0-9.+/-]*/.test(upper)) {
    tokens.add("WD");
  }
  if (/^W[CNOR][A-Z0-9.+/-]*/.test(upper) || /\bWOLF[\s_-]*RAYET\b/.test(upper)) {
    tokens.add("WR");
  }
  const spectral = upper.match(/^[\s(]*([OBAFGKMLTY])(?=[0-9IVXLCDM\s.+:/-]|$)/);
  if (spectral) {
    addToken(tokens, spectral[1]);
  }
  return Array.from(tokens);
}

export function sortStellarClassTokens(tokens) {
  const rawTokens = tokens instanceof Set
    ? Array.from(tokens)
    : (Array.isArray(tokens) ? tokens : (tokens ? [tokens] : []));
  const clean = new Set(rawTokens.map((token) => String(token || "").trim().toUpperCase()).filter(Boolean));
  if (clean.has("PULSAR") || clean.has("MAGNETAR")) {
    clean.add("NS");
  }
  return STELLAR_CLASS_ORDER.filter((token) => clean.has(token));
}

export function stellarClassTokensFromRecord(record, { includeUnknown = true } = {}) {
  const tokens = new Set();
  const fields = record?.fields || {};
  [
    record?.spectral_class,
    record?.spectral_type_raw,
    record?.visual_stellar_class,
    record?.body_class,
    record?.compact_type,
    record?.object_type,
    record?.kind,
    record?.type,
    fieldValue(fields, "spectral_class"),
    fieldValue(fields, "spectral_type_raw"),
    fieldValue(fields, "visual_stellar_class"),
    fieldValue(fields, "object_type"),
    fieldValue(fields, "body_class"),
    fieldValue(fields, "compact_type"),
  ].forEach((value) => {
    stellarClassTokensFromText(value).forEach((token) => tokens.add(token));
  });
  const sorted = sortStellarClassTokens(tokens);
  return sorted.length || !includeUnknown ? sorted : ["U"];
}

export function stellarClassTokensFromSystem(system, { includeUnknown = true } = {}) {
  const tokens = new Set();
  const classes = Array.isArray(system?.spectral_classes) ? system.spectral_classes : [];
  classes.forEach((value) => stellarClassTokensFromText(value).forEach((token) => tokens.add(token)));
  [
    system?.dominant_spectral_class,
    system?.coolness_dominant_spectral_class,
    system?.spectral_class,
    system?.spectral_type_raw,
  ].forEach((value) => stellarClassTokensFromText(value).forEach((token) => tokens.add(token)));
  const sorted = sortStellarClassTokens(tokens);
  return sorted.length || !includeUnknown ? sorted : ["U"];
}

export function StellarClassChips({
  tokens = [],
  record = null,
  includeUnknown = true,
  className = "",
  size = "normal",
}) {
  const tokenList = tokens instanceof Set
    ? Array.from(tokens)
    : (Array.isArray(tokens) ? tokens : (tokens ? [tokens] : []));
  const resolvedTokens = sortStellarClassTokens(tokenList.length ? tokenList : stellarClassTokensFromRecord(record, { includeUnknown }));
  const displayTokens = resolvedTokens.length || !includeUnknown ? resolvedTokens : ["U"];
  if (!displayTokens.length) {
    return null;
  }
  return (
    <span className={`stellar-class-chips stellar-class-chips-${size} ${className}`.trim()} aria-label="Stellar class tags">
      {displayTokens.map((token) => {
        const tag = STELLAR_CLASS_TAGS[token] || STELLAR_CLASS_TAGS.U;
        return (
          <span
            key={token}
            className="stellar-class-chip"
            data-stellar-token={token.toLowerCase().replace(/[^a-z0-9]+/g, "-")}
            title={`${tag.name}: ${tag.text}`}
            style={{ "--stellar-chip-color": tag.color }}
          >
            {tag.label}
          </span>
        );
      })}
    </span>
  );
}
