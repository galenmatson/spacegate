# Sol Authority Program

This document defines how Spacegate models and ingests the Solar System using authoritative sources.

Goals:

- never ship a build where Sol is missing or incomplete
- keep Sol data provenance/auditability equal to all other canonical science rows
- support future deep hierarchy and animation use-cases without schema kludges

## Scope and Phasing

## S1 (implemented): canonical Sol bootstrap for core UX

S1 includes:

- Sol system root
- Sun host star
- major planets (Mercury..Neptune)
- dwarf planets (Pluto, Ceres, Eris, Haumea, Makemake)
- Sol aliases (`Sol`, `Solar System`, `Sun`)

Terminology:

- scientific classification in canonical science layers remains source-faithful (`dwarf_planet` per source conventions)
- structural/UI supergrouping may expose `subplanet` for navigation and usability

S1 does not include moons/comets/spacecraft as first-class rows in `core`.

## S2 (implemented): natural satellite hierarchy in arm

- add moon nodes and orbit edges in `arm`
- add barycenter nodes where needed for stable animation graph composition
- keep these rows out of core hot paths

## S3 (implemented): small-body expansion

- add named asteroids/comets/TNO support into `arm` with confidence and staleness metadata
- keep canonical core hot paths unchanged
- deterministic bootstrap now covers expanded families:
  - asteroids (including Vesta, Psyche, Bennu, Ryugu, Itokawa, Hector and others)
  - TNOs (including Sedna, Quaoar, Orcus, Gonggong, Varuna, Ixion, Salacia, Varda)
  - comet baseline: 67P/Churyumov-Gerasimenko
- S3 rows are projected into both `arm` and halo arm projection when relevant objects are sliced out of core.

## S4 (implemented): artificial satellites/spacecraft layer

- ingest curated Sol artificial objects from JPL Horizons with explicit freshness windows
- materialize `sol_artificial_objects` + hierarchy/orbit/solution rows in `arm`
- keep artificial rows out of `core` canonical hot paths
- surface S4 evidence in system detail API/UI for Sol
- include S4 stale-row checks in volatile-feed monitoring report

## Source Policy

S1/S2 authoritative source:

- JPL Horizons API (`https://ssd.jpl.nasa.gov/api/horizons.api`)

S1/S2 extractor:

- `scripts/fetch_sol_authority.py`
- writes `raw/sol_authority/sol_system_objects.csv`
- writes manifest `reports/manifests/sol_authority_manifest.json`

S4 extractor:

- `scripts/fetch_sol_artificial.py`
- writes `raw/sol_artificial/sol_artificial_objects.csv`
- writes manifest `reports/manifests/sol_artificial_manifest.json`

S1/S2 retrieval policy:

- deterministic object list and epoch window
- retrieval checksum + timestamp required
- build fails if S1 source is enabled and cooked Sol data is missing

## Ingest Contract

S1/S2 are wired into:

- `scripts/download_core.sh`
- `scripts/cook_core.sh`
- `scripts/ingest_core.py`
- `scripts/verify_build.sh`

S4/volatile refresh wiring:

- `scripts/download_core.sh`
- `scripts/cook_core.sh`
- `scripts/build_arm.py`
- `scripts/refresh_sol_volatile.sh`
- `scripts/report_sol_volatile.py`

Ingest behavior:

- inserts Sol/Sun authoritative rows if absent
- injects Sol planetary rows into `planets` with `source_catalog=sol_authority`
- preserves full provenance fields (`source_*`, checksum, retrieved time)
- adds Sol aliases for search ergonomics
- emits Sol contribution in `catalog_contribution_report.json`
- materializes S2 moon hierarchy/orbit/barycenter rows into `arm.duckdb`
- materializes S3 small-body rows and S4 artificial rows into `arm.duckdb`
- projects arm Sol overlays into halo artifacts when halo is built from galaxy/core pairs

## Release Gate

`scripts/verify_build.sh` enforces Sol gates:

- Sol system row exists
- Sun star row exists and is linked to Sol system
- all 8 major planets are present under Sol
- Sol has at least 8 linked planets

If any Sol gate check fails, verification fails and promotion should be blocked.

S2 arm checks:

- `sol_authority` moon component rows exist in `component_entities`
- Earth->Moon containment edge exists in `system_hierarchy_edges`
- `satellite` orbit edges exist in `orbit_edges`
- Earth-Moon and Pluto-Charon barycenter rows exist in `barycenters`

S3 arm checks:

- `sol_small_body_objects` exists and contains deterministic named-body rows
- asteroid/TNO/comet family coverage is present
- each S3 small body has corresponding `orbit_edges` relation rows (`relation_kind='orbits'`)

S4 arm checks:

- `sol_artificial_objects` exists and contains deterministic curated objects
- deep-space probe coverage is present
- each S4 object has corresponding `orbit_edges` rows (`relation_kind='artificial_orbit'`)

## Volatile Feed Operations

Use:

- `scripts/refresh_sol_volatile.sh`

This performs:

- Sol authority + Sol artificial raw refresh
- lightweight cooked CSV normalization for both feeds
- freshness/staleness report generation at `reports/sol_volatile_report.json`

Promotion note:

- refreshed volatile feeds are not live until a new ingest/promote cycle runs.

## Modeling Notes

S1 intentionally prioritizes canonical discoverability over full Solar-System object breadth.

S2+ use the generic hierarchy model:

- node types: star, planet, subplanet, moon, minor_body, asteroid, comet, spacecraft, barycenter
- edge types: contains, satellite, orbits, artificial_orbit, belongs_to

This allows Castor-like hierarchy handling and Sol-like deep object diversity under one consistent graph model.
