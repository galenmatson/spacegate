export const NAME_STYLE_STORAGE_KEY = "spacegate.nameStyle";

export const NAME_STYLE_OPTIONS = [
  {
    value: "public_full",
    label: "Public Full",
    title: "Prefer full, layperson-readable names such as Epsilon Indi and Mu Herculis.",
  },
  {
    value: "astronomer_abbrev",
    label: "Astronomer Abbrev",
    title: "Prefer compact traditional forms such as Eps Ind and Mu Her.",
  },
  {
    value: "catalog_compact",
    label: "Catalog Compact",
    title: "Prefer concise catalog-style names where useful.",
  },
  {
    value: "source_technical",
    label: "Source/Technical",
    title: "Prefer source-native technical identifiers such as WDS and Gaia labels.",
  },
];

const NAME_STYLE_IDS = new Set(NAME_STYLE_OPTIONS.map((option) => option.value));

export function normalizeNameStyle(raw) {
  const value = String(raw || "").trim().toLowerCase().replace(/-/g, "_");
  return NAME_STYLE_IDS.has(value) ? value : "public_full";
}

export function readStoredNameStyle() {
  if (typeof window === "undefined") {
    return "public_full";
  }
  try {
    return normalizeNameStyle(window.localStorage.getItem(NAME_STYLE_STORAGE_KEY));
  } catch (_) {
    return "public_full";
  }
}

export function writeStoredNameStyle(nameStyle) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(NAME_STYLE_STORAGE_KEY, normalizeNameStyle(nameStyle));
  } catch (_) {
    // Name style preference is optional.
  }
}
