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
    text: "O-type stars are the most massive, hottest, and brightest main-sequence stars in the universe. They burn with an intense blue-white hue and emit staggering amounts of ultraviolet radiation. Because they consume their nuclear fuel at a ferocious rate, their lifespans are incredibly short, lasting only a few million years. These celestial titans play a crucial role in galactic evolution by scattering heavy elements when they inevitably explode as supernovae. Consequently, they are exceedingly rare, making up a tiny fraction of the overall stellar population.",
  },
  B: {
    label: "B",
    name: "B-Type Star",
    color: "#8cc8ff",
    text: "B-type stars are extremely luminous and hot, shining with a brilliant blue color. While slightly less massive than their O-type cousins, they still wield immense gravitational influence and emit powerful stellar winds. They typically live for tens of millions of years, burning through their hydrogen fuel rapidly before evolving into massive giant stars. These stars are often found clustered together in young stellar associations where recent star formation has occurred. Well-known examples like Rigel and Spica dominate our night sky due to their sheer brilliance.",
  },
  A: {
    label: "A",
    name: "A-Type Star",
    color: "#d7e9ff",
    text: "A-type stars are young, hot, and rapidly spinning stars that shine with a crisp white or slightly bluish-white light. They are among the most common naked eye stars visible from Earth, counting Sirius and Vega among their ranks. These stars lack strong magnetic fields and stellar winds, which allows their atmospheres to remain relatively calm and layered. They have lifespans of roughly a billion years, making them short-lived compared to our Sun but stable enough to host complex orbital systems. Their intense radiation environments make the formation of habitable planets challenging, but certainly not impossible.",
  },
  F: {
    label: "F",
    name: "F-Type Star",
    color: "#fff2b5",
    text: "F-type stars represent a transitional class of yellow-white stars that are slightly more massive and hotter than our Sun. They boast vigorous boiling convection zones in their outer layers, creating strong magnetic dynamos and active stellar surfaces. These stars have lifespans of a few billion years, providing a potentially stable enough window for advanced planetary systems to evolve. Because they emit a higher fraction of ultraviolet light than cooler stars, any habitable planets would require robust atmospheric shielding. Procyon is a famous nearby example of this bright and energetic stellar class.",
  },
  G: {
    label: "G",
    name: "G-Type Star",
    color: "#ffd86b",
    text: "G-type stars are the familiar, stable yellow-white stars of the cosmos, most famously represented by our own Sun. They strike a perfect cosmic balance, possessing enough mass to burn steadily for roughly ten billion years without exhausting their fuel too quickly. This long-term stability creates an ideal, sustained habitable zone where liquid water can exist on rocky planets for eons. They are characterized by a temperate surface and a moderate emission of ultraviolet radiation, allowing complex chemistry to thrive on neighboring worlds. While they make up only a small percentage of all stars, they are prime targets in the search for extraterrestrial life.",
  },
  K: {
    label: "K",
    name: "K-Type Star",
    color: "#ffb36a",
    text: "K-type stars, or orange dwarfs, are slightly cooler and less massive than our Sun, emitting a warm, orange-tinged light. They are considered by many astrobiologists to be the Goldilocks stars of the universe. Their nuclear fusion proceeds at a relaxed pace, granting them incredibly long lifespans of 15 to 30 billion years, longer than the current age of the universe. Furthermore, they emit far less hazardous radiation than red dwarfs, providing a deeply stable and safe environment for orbiting planets. These abundant and peaceful stars are excellent candidates for hosting ancient, undisturbed planetary systems.",
  },
  M: {
    label: "M",
    name: "M-Type Star",
    color: "#f06a55",
    text: "M-type stars, commonly known as red dwarfs, are the smallest, coolest, and by far the most abundant stars in the galaxy. Because their internal fusion operates so efficiently and slowly, they can burn steadily for trillions of years. However, their early lives are famously chaotic, often unleashing violent solar flares that can strip the atmospheres from tightly orbiting planets. To host liquid water, planets must orbit very close to these dim stars, frequently resulting in them becoming tidally locked with one side constantly facing the sun. Despite their volatile youth, their staggering lifespans make them the ultimate survivors of the stellar family.",
  },
  L: {
    label: "L",
    name: "L-Type Brown Dwarf",
    color: "#cf6b57",
    text: "L-type objects bridge the gap between the lowest-mass true stars and massive substellar brown dwarfs. They are incredibly dim, glowing primarily in the infrared spectrum with a very dark red or magenta hue. Their atmospheres are cool enough that metallic compounds and thick silicate clouds can form, raining liquid iron and sand deep within their interiors. Because they lack the mass to sustain stable hydrogen fusion, most L-dwarfs slowly cool and fade over billions of years. They represent a fascinating gray area where stellar astrophysics begins to resemble planetary meteorology.",
  },
  T: {
    label: "T",
    name: "T-Type Brown Dwarf",
    color: "#8f6bc7",
    text: "T-type brown dwarfs are failed stars that are significantly cooler and darker than L-dwarfs. Their defining feature is the prominent presence of methane in their atmospheres, a chemical marker they share with giant gas planets like Jupiter. They emit almost no visible light, radiating their slowly dissipating internal heat exclusively into the infrared spectrum. Without any nuclear fusion to sustain them, they will continue to cool perpetually, drifting through space as dark, desolate spheres. Exploring a T-dwarf is akin to studying an isolated, massive gas giant untethered from a host star.",
  },
  Y: {
    label: "Y",
    name: "Y-Type Brown Dwarf",
    color: "#6fc7d8",
    text: "Y-type brown dwarfs are the absolute coldest known class of substellar objects, representing the final stage of brown dwarf cooling. Their surface temperatures are astonishingly low, sometimes dipping to room temperature or even matching the freezing cold of Earth's poles. At these extremes, their atmospheres can harbor clouds of water vapor and ammonia, completely blurring the line between a rogue planet and a star. They are practically invisible to optical telescopes and must be hunted using highly sensitive space-based infrared observatories. These phantom objects offer critical clues about the dark, hidden mass drifting through our galactic neighborhood.",
  },
  WR: {
    label: "WR",
    name: "Wolf-Rayet Star",
    color: "#71f6ff",
    text: "Wolf-Rayet stars are rare, hyper-luminous, and incredibly unstable stars approaching the end of their violent lives. They are characterized by monstrous stellar winds that are actively blasting away their outer hydrogen envelopes at millions of miles per hour. This rapid mass loss exposes the star's superheated inner core, which burns at temperatures that can easily exceed 200,000 Kelvin. They are heavily enriched with heavy elements like carbon, nitrogen, and oxygen, which they furiously scatter into the surrounding interstellar medium. These terrifyingly beautiful objects are destined to end their existence in spectacular supernova explosions.",
  },
  WD: {
    label: "WD",
    name: "White Dwarf",
    color: "#d7dee8",
    text: "A white dwarf is the dense, glowing ember left behind after a low-to-medium mass star exhausts its nuclear fuel. Packing the mass of the Sun into a sphere roughly the size of Earth, their matter is crushed into an exotic, ultra-dense state called electron-degenerate gas. Because they no longer generate heat through fusion, they shine purely from trapped residual thermal energy. Over tens of billions of years, a white dwarf will slowly radiate this heat away, eventually fading into a theoretical, cold black dwarf. They are highly stable gravitational anchors, often retaining the surviving outer planets of their original solar systems.",
  },
  NS: {
    label: "NS",
    name: "Neutron Star",
    color: "#b9a7ff",
    text: "A neutron star is the ultra-dense, collapsed core of a massive star that perished in a spectacular supernova explosion. They are so incomprehensibly compact that a mass greater than our Sun is squeezed into a sphere only the size of a city. The immense gravitational pressure crushes protons and electrons together, forming a body composed almost entirely of closely packed neutrons. Their surfaces are solid and fiercely hot, enveloped by magnetic and gravitational fields billions of times stronger than Earth's. Escaping a neutron star's crushing gravity would require traveling at a significant fraction of the speed of light.",
  },
  PULSAR: {
    label: "PULSAR",
    name: "Pulsar",
    color: "#9bffef",
    text: "A pulsar is a highly magnetized, rapidly rotating neutron star that acts as a precise cosmic lighthouse. As it spins, it projects brilliant beams of electromagnetic radiation outward from its magnetic poles. When these poles do not align with the star's rotational axis, the beams sweep across space, pulsing with incredible, clock-like precision if they cross Earth's line of sight. Some millisecond pulsars spin hundreds of times every single second, often accelerated by consuming material from a neighboring companion star. These rhythmic beacons are so precise that astronomers can use them as natural GPS systems and gravitational wave detectors.",
  },
  MAGNETAR: {
    label: "MAGNETAR",
    name: "Magnetar",
    color: "#ff6df0",
    text: "A magnetar is a rare, terrifyingly powerful variant of a neutron star possessing the strongest magnetic fields in the known universe. Their magnetic fields are up to a thousand times stronger than a typical neutron star, capable of tearing atomic structures apart at a distance of hundreds of miles. This extreme magnetic tension regularly twists and snaps the star's solid crust, causing cataclysmic events known as starquakes. These quakes unleash blasts of X-rays and gamma rays so energetic they can temporarily blind satellites located halfway across the galaxy. Over roughly ten thousand years, their magnetic fields naturally decay, causing them to eventually settle down into ordinary neutron stars.",
  },
  "BLACK HOLE": {
    label: "BLACK HOLE",
    name: "Black Hole",
    color: "#ffcf4a",
    text: "A black hole is a region of spacetime where gravity is so overwhelmingly strong that nothing, not even light, can escape its grasp. They are typically born from the catastrophic core collapse of the universe's most massive stars during a supernova or hypernova event. The boundary surrounding the black hole is called the event horizon, a point of no return where the required escape velocity exceeds the speed of light. All of the object's mass is concentrated into an infinitely dense central point known as a singularity, where our current understanding of physics breaks down completely. Though invisible themselves, their presence is revealed by the glowing, superheated accretion disks of doomed matter swirling into their depths.",
  },
  U: {
    label: "U",
    name: "Unknown Stellar Class",
    color: "#8794a8",
    text: "This object has no reliable stellar class available in the current Spacegate slice or payload. It may be missing spectral evidence, unresolved in a subsystem, or awaiting stronger catalog reconciliation.",
  },
};

export function normalizeStellarClassToken(rawToken) {
  const token = String(rawToken || "").trim().toUpperCase();
  if (!token) {
    return "U";
  }
  if (token === "D") {
    return "WD";
  }
  if (token === "BH" || token === "BLACKHOLE") {
    return "BLACK HOLE";
  }
  return STELLAR_CLASS_TAGS[token] ? token : "U";
}

export function stellarClassTooltip(rawToken, suffix = "") {
  const token = normalizeStellarClassToken(rawToken);
  const tag = STELLAR_CLASS_TAGS[token] || STELLAR_CLASS_TAGS.U;
  const trailing = suffix ? ` ${suffix}` : "";
  return `${tag.name}: ${tag.text}${trailing}`;
}

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
  const raw = String(rawToken || "").trim().toUpperCase();
  if (!raw) {
    return;
  }
  const token = normalizeStellarClassToken(raw);
  if (token !== "U") {
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
  const luminosityPrefix = text.match(/^(?:d|sd|esd|usd)([OBAFGKMLTY])/);
  if (/\bWHITE[\s_-]*DWARF\b|\bWD\b/.test(upper) || (!luminosityPrefix && /^D(?:$|[ABCOQZX0-9])/.test(upper))) {
    tokens.add("WD");
  }
  if (/^W[CNOR][A-Z0-9.+/-]*/.test(upper) || /\bWOLF[\s_-]*RAYET\b/.test(upper)) {
    tokens.add("WR");
  }
  const spectral = luminosityPrefix || upper.match(/^[\s(]*([OBAFGKMLTY])(?=[0-9IVXLCDM\s.+:/-]|$)/);
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
  const leafClass = record?.stellar_leaf_classification?.classification_value
    || fieldValue(record?.fields, "stellar_leaf_display_class")
    || record?.quick_facts?.stellar_leaf_display_class;
  if (leafClass) {
    const leafTokens = sortStellarClassTokens(stellarClassTokensFromText(leafClass));
    return leafTokens.length || !includeUnknown ? leafTokens : ["U"];
  }
  const tokens = new Set();
  const fields = record?.fields || {};
  const quickFacts = record?.quick_facts || {};
  [
    record?.spectral_class,
    record?.spectral_type_raw,
    fieldValue(fields, "spectral_class"),
    fieldValue(fields, "spectral_type_raw"),
    quickFacts.spectral_class,
    quickFacts.spectral_type_raw,
  ].forEach((value) => {
    stellarClassTokensFromText(value).forEach((token) => tokens.add(token));
  });
  if (!tokens.size) {
    [
      record?.body_class,
      record?.compact_type,
      record?.object_type,
      record?.kind,
      record?.type,
      fieldValue(fields, "object_type"),
      fieldValue(fields, "body_class"),
      fieldValue(fields, "compact_type"),
      quickFacts.object_type,
      quickFacts.body_class,
      quickFacts.compact_type,
    ].forEach((value) => {
      stellarClassTokensFromText(value).forEach((token) => tokens.add(token));
    });
  }
  if (!tokens.size) {
    [
      record?.visual_stellar_class,
      fieldValue(fields, "visual_stellar_class"),
      quickFacts.visual_stellar_class,
    ].forEach((value) => {
      stellarClassTokensFromText(value).forEach((token) => tokens.add(token));
    });
  }
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
        const normalizedToken = normalizeStellarClassToken(token);
        const tag = STELLAR_CLASS_TAGS[normalizedToken] || STELLAR_CLASS_TAGS.U;
        return (
          <span
            key={token}
            className="stellar-class-chip"
            data-stellar-token={normalizedToken.toLowerCase().replace(/[^a-z0-9]+/g, "-")}
            title={stellarClassTooltip(normalizedToken)}
            style={{ "--stellar-chip-color": tag.color }}
          >
            {tag.label}
          </span>
        );
      })}
    </span>
  );
}
