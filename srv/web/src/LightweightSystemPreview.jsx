import React from "react";
import { STELLAR_CLASS_TAGS, stellarClassTokensFromSystem } from "./stellarClassTags.jsx";

function formatNumber(value, decimals = 1) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "unknown";
  return num.toLocaleString(undefined, {
    maximumFractionDigits: decimals,
    minimumFractionDigits: decimals,
  });
}

function previewTier(system) {
  return String(system?.preview_tier || "").trim().toLowerCase();
}

export function isLightweightPreviewSystem(system) {
  return system?.is_lightweight_preview_safe === true
    || previewTier(system) === "lightweight_singleton";
}

export function LightweightSystemPreview({ system, displayName, stateLabel = "Lightweight preview" }) {
  const tokens = stellarClassTokensFromSystem(system, { includeUnknown: true });
  const primaryToken = tokens[0] || "U";
  const tag = STELLAR_CLASS_TAGS[primaryToken] || STELLAR_CLASS_TAGS.U;
  const color = tag?.color || "#8794a8";
  const teffMin = Number(system?.min_star_teff_k);
  const teffMax = Number(system?.max_star_teff_k);
  const teffLabel = Number.isFinite(teffMin) || Number.isFinite(teffMax)
    ? `${Number.isFinite(teffMin) ? Math.round(teffMin).toLocaleString() : "?"}-${Number.isFinite(teffMax) ? Math.round(teffMax).toLocaleString() : "?"} K`
    : "temp unknown";
  return (
    <div
      className="lightweight-system-preview"
      data-testid="lightweight-system-preview"
      style={{ "--preview-star-color": color }}
      aria-label={`${displayName} lightweight singleton preview`}
    >
      <div className="lightweight-system-star-wrap" aria-hidden="true">
        <div className="lightweight-system-star" />
        <div className="lightweight-system-glow" />
      </div>
      <div className="lightweight-system-preview-readout">
        <strong>{displayName}</strong>
        <span>{tag?.label || primaryToken} class · {formatNumber(system?.dist_ly, 2)} ly</span>
        <span>{teffLabel} · cool {formatNumber(system?.coolness_score, 1)}</span>
      </div>
      <span className="map-search-card-preview-chip">{stateLabel}</span>
    </div>
  );
}
