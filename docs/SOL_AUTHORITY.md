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

## S3 (implemented initial set): small-body expansion

- add named asteroids/comets/TNO support into `arm` with confidence and staleness metadata
- keep canonical core hot paths unchanged
- current deterministic bootstrap includes:
  - asteroids: Vesta, Pallas, Juno, Hygiea, Psyche, Eros, Bennu, Ryugu, Itokawa
  - TNOs: Sedna, Quaoar, Orcus, Gonggong, Varuna
  - comet: 67P/Churyumov-Gerasimenko

Note:
- S3 currently materializes in `arm`; `halo` projection integration is pending `halo` artifact materialization completion.

## S4: artificial satellites/spacecraft layer

- ingest volatile orbital feeds with explicit freshness windows and default-off policy in core UX

## Source Policy

S1/S2 authoritative source:

- JPL Horizons API (`https://ssd.jpl.nasa.gov/api/horizons.api`)

S1/S2 extractor:

- `scripts/fetch_sol_authority.py`
- writes `raw/sol_authority/sol_system_objects.csv`
- writes manifest `reports/manifests/sol_authority_manifest.json`

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

Ingest behavior:

- inserts Sol/Sun authoritative rows if absent
- injects Sol planetary rows into `planets` with `source_catalog=sol_authority`
- preserves full provenance fields (`source_*`, checksum, retrieved time)
- adds Sol aliases for search ergonomics
- emits Sol contribution in `catalog_contribution_report.json`
- materializes S2 moon hierarchy/orbit/barycenter rows into `arm.duckdb`

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

## Modeling Notes

S1 intentionally prioritizes canonical discoverability over full Solar-System object breadth.

S2+ use the generic hierarchy model:

- node types: star, planet, subplanet, moon, minor_body, asteroid, comet, spacecraft, barycenter
- edge types: contains, orbits, belongs_to

This allows Castor-like hierarchy handling and Sol-like deep object diversity under one consistent graph model.
