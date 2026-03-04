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
- `lookup_only`: useful for human lookup or QA, not approved for automated canonical merge

## Core canonical fields

### systems

| Field family | Preferred source | Fallbacks | Status | Notes |
|---|---|---|---|---|
| `system_name` | curated common-name table | AT-HYG proper/name fields; primary `star_name` | `approved_local` | Small, pinned, human-reviewed table for high-value systems |
| multiplicity / membership | approved exact-join multiplicity evidence plus approved hierarchy/relationship catalogs | name-root grouping; proximity grouping | `review_required` | Exact-ID evidence should beat heuristics; coordinate-led visual catalogs are support, not sole authority |
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

## Multiplicity source candidates

This section records the current evaluation of candidate multistar sources before the canonical merge layer is implemented.

| Source | Best role | Join strength | Status | Notes |
|---|---|---|---|---|
| Gaia DR3 `gaia_source.non_single_star` and NSS tables | exact multiplicity evidence on existing stars | exact `source_id` | `approved_family` | Best primary evidence layer for star-level multiplicity flags and orbit solutions |
| MSC (Tokovinin Multiple Star Catalog) | hierarchical system structure for triples and higher | moderate; WDS-centered with common identifiers | `deferred_pending_terms` | Strong candidate for explicit hierarchy because it is purpose-built for nested multiples and ships bulk tables; sample pass found 3,482 exact-key overlaps with current core, but active ingest is on hold pending usage terms confirmation |
| ORB6 (Sixth Orbit Catalog) | high-confidence visual-binary orbital evidence | moderate; WDS/discoverer-led | `review_required` | Strong support catalog for orbit-confirmed visual binaries; bulk text/SQL files are available; sample pass found 3,282 exact-key overlaps with current core |
| BDB / ILB (Binary Star Database / Identification List of Binaries) | crosswalk between system, pair, and component identifiers across heterogeneous multiplicity catalogs | potentially strong if export is practical | `review_required` | Deferred for now: strategically valuable as a link resolver, but a stable current bulk export/API has not been confirmed and Spacegate should not depend on an uncached remote Russian-hosted source for core ingest |
| WDS | broad visual multiplicity coverage | weak exact IDs; strong coordinate/discoverer designations | `review_required` | Valuable breadth source, but matches to core require confidence-scored crossmatching rather than exact joins; current prototype finds mostly low-confidence positional matches |
| SBX (successor to SB9) | spectroscopic-orbit evidence | moderate; identifier quality varies by system | `review_required` | Best specialized source for spectroscopic binaries; useful as supporting evidence, not a full hierarchy catalog; sampled rows show strong Gaia/HIP/HD overlap |
| Stelle Doppie | operator lookup / manual QA | derivative of WDS pages with added cross-identifiers | `lookup_only` | Useful for manual inspection, but not suitable as a canonical automated source |

Preferred multiplicity stack for v1.2 planning:

1. Gaia NSS for exact star-level multiplicity evidence.
2. MSC for explicit higher-order hierarchy, pending usage terms confirmation.
3. ORB6 and SBX as orbit-quality support catalogs.
4. WDS for broad visual coverage via confidence-scored crossmatch.
5. BDB is deferred unless a stable local mirror/export path is confirmed.

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

- exact multiplicity relationship source(s) for canonical system grouping, especially the Gaia NSS + MSC + WDS interaction model
- exact Gaia extraction path for the <=1000 ly core subset
- which spectroscopy catalogs are approved for canonical stellar physical fields
- whether BDB/ILB can be mirrored locally with a stable machine-ingest path suitable for routine builds
- whether any compact/substellar classes should ever graduate from `pack_only` into core canonical rows
