# Spacegate Source Data

Last updated: July 7, 2026 (UTC)

Spacegate is a Gaia-first astronomy database. It preserves source-native records and retrieval metadata, then builds deterministic public layers for search, analysis, simulation, and presentation.

The public Coolstars site is served from a specific materialized build. The build identifier shown in the header connects the visible site to the database, manifests, cooked artifacts, and verification reports used to produce it.

## Layer Model

Spacegate separates data by role.

| Layer | Role |
| --- | --- |
| `core` | Accepted public inventory and hot-path browse/search summaries. |
| `arm` | Source-native or defensible science relationships, hierarchy, orbital evidence, variability, compact-object overlays, Sol authority data, and simulation contracts. |
| `disc` | Reproducible generated outputs such as coolness scores, snapshots, render assumptions, presentation material, and future AI-generated narration. |
| `rim` | Fictional, lore, and worldbuilding overlays. |

The layer boundary is not just confidence. It is purpose. For example, a star's identity belongs in inventory, but an orbital solution belongs in relationship/orbit evidence even when the solution is published by a reliable source.

## Active Source Families

| Source | Main role | Current use |
| --- | --- | --- |
| Gaia DR3 `gaia_source` | Stellar backbone | Canonical nearby stellar inventory, positions, parallaxes, photometry, and motion basis. |
| Sol authority data and JPL Horizons | Solar system authority | Sun, planets, moons, selected spacecraft/stations, and high-quality Solar System orbital references. |
| NASA Exoplanet Archive `pscomppars` | Exoplanet source | Confirmed public exoplanet inventory and promoted planet summaries. |
| NASA Exoplanet Archive `ps` | Planet solution evidence | Alternate/source-native planet orbital rows for ARM orbital-solution materialization. |
| Open Exoplanet Catalogue, Exoplanet.eu, ExoKyoto | Exoplanet lifecycle context | Candidate, controversial, retracted, and cross-catalog planet state overlays where useful. |
| Gaia NSS | Multiplicity and orbit evidence | Non-single-star evidence and two-body orbital support. |
| WDS | Visual/wide multiplicity | Wide binary and multiple-star evidence. |
| WDS-Gaia bridge | Cross-catalog linkage | Helps connect WDS identifiers to Gaia-backed objects. |
| MSC | Hierarchical multiplicity | Source-native component, subsystem, and orbit evidence for multiple-star systems. |
| ORB6 | Visual binary orbits | Orbit-quality support for binaries and multiples. |
| SBX | Spectroscopic binary evidence | Spectroscopic binary support, replacing the older SB9 path. |
| DEBCat | Detached eclipsing binaries | High-quality binary parameters and validation cases. |
| TESS EB | Eclipsing binaries | Eclipsing-binary context and support evidence. |
| Gaia astrophysical class probabilities | Classification support | Compact-object and object-type probability context. |
| Gaia EDR3 white dwarf candidates | White dwarfs | White-dwarf tagging and classification support. |
| Ultracool dwarf companion sources | Low-temperature companions | L/T/Y dwarf enrichment and companion context. |
| ATNF pulsar catalog | Pulsars | Pulsar and compact-object enrichment. |
| Magnetar catalog | Magnetars | Magnetar enrichment and compact-object context. |
| Open cluster catalogs and memberships | Environmental context | Cluster membership and neighborhood context. |
| Green supernova remnant catalog | Remnant context | Supernova-remnant context. |
| AAVSO VSX | Variable stars | Variable-star family, period, amplitude, and classification overlays. |

## Deferred or Excluded Sources

Some useful catalogs are not in the default public build because they are redundant, fragile to mirror, low yield for the current Gaia slice, or not yet integrated through a reproducible ingest path.

| Source | State | Reason |
| --- | --- | --- |
| Kepler Eclipsing Binary Catalog | Deferred | Useful, but current default-slice linkage yield is limited. |
| Binary Star Database (BDB) | Deferred | No robust bulk mirror path yet. |
| SB9 | Excluded | Superseded by the SBX path for this project. |
| EMAC | Excluded as an ingest source | Useful ecosystem/tool reference, but not a direct canonical catalog feed. |

## Provenance Rules

Spacegate source stewardship follows several rules:

- Preserve raw source snapshots and retrieval metadata.
- Track source catalog, source row key, retrieval timestamp, checksum, and build context where practical.
- Prefer source-native hierarchy and orbital rows when catalogs provide them.
- Avoid count-only or suffix-only reconstruction when better evidence exists.
- Keep alternate orbital solutions rather than collapsing them into a hidden single truth.
- Label source, derived, assumed, and missing values distinctly in presentation.
- Keep science records separate from generated narration, visual assumptions, and fictional overlays.

## Public Data Slice

The public site currently uses a sliced build for performance. The full local build is larger than the public edge server needs for normal browsing, so the served build is optimized for nearby exploration and responsive search.

This means the public site is not a complete mirror of every raw source catalog. It is a curated, reproducible public projection of the Spacegate build.

## Where To See Evidence

The site exposes source context in several places:

- The `DATA` page documents active source families.
- System pages show hierarchy, object vitals, simulation data, and diagnostics.
- System Simulation readouts distinguish source, derived, assumed, and missing values where the interface has room for that detail.
- Build identifiers in the header show which database build is being served.
- The open source repository contains the ingest, build, verification, and API code.

Repository:

https://github.com/galenmatson/spacegate
