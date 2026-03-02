# Spacegate v1.2 Source Matrix

This document is the draft field/source precedence matrix for the v1.2 catalog expansion.

Purpose:

- translate the v1.2 hierarchy in `docs/PROJECT.md` into a concrete source plan
- distinguish approved source families from still-open choices
- keep core/packs/detection catalogs separate

Status labels:

- `approved_family`: approved as a source family for this field class; exact extraction path may still need implementation work
- `approved_local`: approved local curated table maintained in-repo or in a pinned sidecar asset
- `review_required`: plausible source, but not approved for canonical merge yet
- `pack_only`: valid source for packs, not for canonical core rows

## Core canonical fields

### systems

| Field family | Preferred source | Fallbacks | Status | Notes |
|---|---|---|---|---|
| `system_name` | curated common-name table | AT-HYG proper/name fields; primary `star_name` | `approved_local` | Small, pinned, human-reviewed table for high-value systems |
| multiplicity / membership | explicit relationship catalogs (WDS-style / multi-star relationship data) | name-root grouping; proximity grouping | `review_required` | Explicit relationships should beat heuristics |
| canonical astrometry (`ra/dec/dist`, xyz) | Gaia-linked astrometry at build epoch `J2016.0` | AT-HYG astrometry projected to `J2016.0`; final fallback: source-epoch coordinates with explicit flagging | `approved_family` | Gaia should win when available |
| coordinate frame / epoch metadata | build metadata + row-level astrometry normalization fields | none | `approved_family` | Build epoch is canonical target; row-level source epoch still needed for mixed sources |

### stars

| Field family | Preferred source | Fallbacks | Status | Notes |
|---|---|---|---|---|
| identifiers (`gaia_id`, `hip_id`, `hd_id`, etc.) | Gaia DR3 source id, then HIP/HD/other catalog IDs | deterministic hash | `approved_family` | Existing stable-key order still holds |
| names / aliases | curated alias table (IAU/common/Bayer/Flamsteed variants) | AT-HYG name fields; ID-derived labels | `approved_local` | Separate canonical display names from source-native aliases |
| astrometry (`ra_deg`, `dec_deg`, `dist_pc`, xyz) | Gaia astrometry | AT-HYG normalized to `J2016.0` when justified | `approved_family` | Prefer Gaia over AT-HYG `x0/y0/z0` whenever Gaia is available |
| kinematics (`pm_*`, `radial_velocity`) | Gaia | AT-HYG | `approved_family` | Keep null when unavailable; do not infer |
| spectral raw + parsed | approved spectroscopic-quality source | AT-HYG spectral string | `review_required` | AT-HYG remains acceptable fallback |
| `Teff` | Gaia astrophysical params or approved spectroscopy | null | `review_required` | Spectral-type-derived values stay out of core |
| radius / mass / luminosity / metallicity | Gaia astrophysical params or approved spectroscopy | null | `review_required` | Inferred values belong in rich, flagged |

### planets

| Field family | Preferred source | Fallbacks | Status | Notes |
|---|---|---|---|---|
| confirmed planet parameters | NASA Exoplanet Archive `pscomppars` | none | `approved_family` | Remains the canonical baseline |
| host matching | Gaia id -> HIP -> HD -> exact hostname | fuzzy hostname (opt-in, audited) | `approved_family` | Fuzzy matching is enhancement-only, lower confidence |

## Pack / extended populations

| Pack family | Candidate source family | Status | Notes |
|---|---|---|---|
| substellar / ultracool dwarfs | UltracoolSheet, Gaia UCD sample | `pack_only` | Can augment browse/render without redefining canonical stars |
| compact remnants | Gaia white dwarf catalog, ATNF, McGill magnetar catalog | `pack_only` | Canonical core should not absorb these blindly |
| special populations / variables | VSX | `pack_only` | Better as tags/pack overlays unless a core use case is explicit |
| extended/superstellar objects | SNR catalogs, cluster catalogs | `pack_only` | These are not point-star replacements for core systems |

## Astrometry normalization policy

This is the intended canonical policy for v1.2:

1. If a star has approved Gaia astrometry, Gaia wins.
2. If a star does not have Gaia astrometry but has sufficient non-Gaia motion data, project it to build epoch `J2016.0` and record the normalization method.
3. If a star lacks enough motion data to justify projection, do not silently mark it as native `J2016.0`. Preserve the source epoch and flag the row until a better source exists.

Implication:

- `build_metadata.coordinate_epoch = J2016.0` remains the canonical target epoch.
- Mixed-source ingestion also needs row-level source epoch / normalization metadata to stay honest.

## Open source decisions

These source families still need a concrete approval call before coding the merge layer:

- exact multiplicity relationship source(s) for canonical system grouping
- exact Gaia extraction path for the <=1000 ly core subset
- which spectroscopy catalogs are approved for canonical stellar physical fields
- whether any compact/substellar classes should ever graduate from `pack_only` into core canonical rows
