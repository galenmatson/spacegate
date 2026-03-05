# Spacegate Core Schema Contract (Gaia-First)

This document defines the canonical core astronomy contract used by ingestion, QC, and API.

Scope:

- immutable astronomy inventory and relationships
- deterministic build outputs
- complete provenance for served rows

Out of scope:

- generated exposition/images (`SCHEMA_RICH.md`)
- editable fiction/worldbuilding overlays (`SCHEMA_LORE.md`)

## Artifact Contract

Per build:

- `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb`
- `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{stars,systems,planets}.parquet`
- `$SPACEGATE_STATE_DIR/reports/<build_id>/*.json`

Build IDs are immutable and deterministic for pinned inputs and transforms.

## Canonical Inventory Policy

Gaia-first contract:

1. Canonical star inventory originates from Gaia.
2. Crosswalk catalogs may enrich identifiers/aliases but do not define canonical star existence.
3. Multiplicity catalogs define evidence/edges and grouping confidence, not hidden row mutation.

Transitional note:

- AT-HYG may appear as compatibility/crosswalk input during migration.
- AT-HYG is not the canonical inventory source in this contract.

## Coordinate and Unit Contract

### Canonical astrometry

- Frame: `ICRS`
- Epoch: `J2016.0`

These must be recorded in `build_metadata` as:

- `coordinate_frame`
- `coordinate_epoch`

### Distance and position fields

Canonical storage:

- parsec-native:
  - `dist_pc`
  - `x_helio_pc`, `y_helio_pc`, `z_helio_pc`

Materialized convenience:

- light-years:
  - `dist_ly`
  - `x_helio_ly`, `y_helio_ly`, `z_helio_ly`

Rule:

- LY fields are deterministic conversions from PC fields.

### Epoch projection

- Core stores canonical coordinates at build epoch.
- Future time/epoch visualization is derived and must not overwrite canonical coordinates.
- If non-native epoch data is projected, row-level lineage must retain source epoch and normalization method.

## Astrometry Quality Contract

Minimum required quality fields on stars:

- `parallax_mas`
- `parallax_error_mas`
- `parallax_over_error`
- `ruwe` (if available from source family)
- `astrometry_quality` (Spacegate tier label)

Recommended tiering:

- `high`: strong parallax confidence and good astrometric fit
- `moderate`: usable for inventory, caution for neighbor/kinematic operations
- `low`: keep only if policy explicitly allows; must be flagged

Boundary policy (`<=1000 ly`) must be explicit and reproducible:

- pass/fail on nominal threshold
- optional confidence margin flag near boundary

## Spatial Index Contract

Spatial index:

- 63-bit Morton (Z-order) stored in `BIGINT`
- based on heliocentric LY coordinates
- parquet outputs physically sorted by `spatial_index`

Hard constraints:

- ingestion fails if coordinates exceed configured domain
- Morton parameters recorded in `build_metadata`

## IDs and Stable Keys

### Surrogate IDs

- `system_id`, `star_id`, `planet_id` are build-local BIGINT keys

### Stable object keys

Required for cross-dataset joins (`core`/`rich`/`lore`) and rebuild continuity.

Star key priority:

1. `star:gaia:<source_id>`
2. deterministic fallback only when Gaia ID unavailable

System key policy:

- deterministic from explicit grouping source where possible
- no unstable random key generation

Planet key policy:

- deterministic from source name + stable disambiguator

## Multiplicity and Hierarchy Contract

Core must make multiplicity evidence explicit and queryable.

Expected evidence families:

- Gaia NSS
- MSC (optional/default-off)
- WDS/ORB6 (support evidence)

Required system-level fields:

- `grouping_basis`
- `grouping_confidence`
- `grouping_source_catalogs_json`
- `has_gaia_nss_evidence`
- `has_msc_evidence`
- `has_wds_evidence`
- `has_orb6_evidence`

WDS-Gaia bridge policy:

- optional/default-off
- multi-member grouping must pass physical consistency gating
  - distance spread threshold
  - proper-motion spread threshold
  - angular match threshold

Proximity grouping policy:

- nondefault in production
- must be explicitly enabled by config
- confidence lower than explicit catalog hierarchy evidence

## Systems of Systems (Target Contract)

Core should evolve to support explicit hierarchy edges:

- parent system/subsystem relationships
- component-level membership edges
- confidence and provenance on each edge

Until explicit edge tables are fully implemented, grouping confidence fields must clearly indicate approximation level.

## Provenance Contract (Mandatory)

Every row in `stars`, `systems`, and `planets` must include:

- `source_catalog`
- `source_version`
- `source_url`
- `source_download_url`
- `source_doi` (nullable)
- `source_pk`
- `source_row_id` or `source_row_hash`
- `license`
- `redistribution_ok`
- `license_note`
- `retrieval_etag` and/or `retrieval_checksum`
- `retrieved_at`
- `ingested_at`
- `transform_version`

Hard gate:

- provenance completeness failures must fail the build.

## Core Tables

## `build_metadata`

Key-value table for build-wide contract parameters.

Must include at minimum:

- build identity:
  - `build_id`
  - `git_sha`
- astrometry contract:
  - `coordinate_epoch`
  - `coordinate_frame`
- spatial indexing parameters:
  - Morton config fields
- active multiplicity gate parameters:
  - WDS-Gaia thresholds when applicable

## `stars`

Canonical stellar inventory table.

Required core columns:

- identity:
  - `star_id`
  - `stable_object_key`
  - `system_id`
- names and identifiers:
  - `star_name`
  - `star_name_norm`
  - `gaia_id`
  - crosswalk IDs (nullable)
- astrometry:
  - `ra_deg`, `dec_deg`
  - `parallax_mas`, `parallax_error_mas`, `parallax_over_error`
  - `pm_ra_mas_yr`, `pm_dec_mas_yr`
  - `radial_velocity_kms`
  - `dist_pc`
  - `x_helio_pc`, `y_helio_pc`, `z_helio_pc`
  - `dist_ly`
  - `x_helio_ly`, `y_helio_ly`, `z_helio_ly`
  - `spatial_index`
- quality:
  - `ruwe` (nullable)
  - `astrometry_quality`
- multiplicity evidence:
  - `wds_id` (nullable)
  - `multiplicity_match_method`
  - `multiplicity_match_confidence`
  - `multiplicity_source_catalogs_json`
  - Gaia NSS evidence fields
- provenance contract fields

## `systems`

Derived system/grouping table for navigation and search.

Required columns:

- identity:
  - `system_id`
  - `stable_object_key`
- naming:
  - `system_name`
  - `system_name_norm`
- position/anchor:
  - `ra_deg`, `dec_deg`
  - `dist_pc`
  - `x_helio_pc`, `y_helio_pc`, `z_helio_pc`
  - `dist_ly`
  - `x_helio_ly`, `y_helio_ly`, `z_helio_ly`
  - `spatial_index`
- grouping semantics:
  - `wds_id` (nullable)
  - `grouping_basis`
  - `grouping_confidence`
  - `grouping_source_catalogs_json`
  - evidence flags (`has_*_evidence`)
- provenance contract fields

## `planets`

Confirmed exoplanet records matched to canonical hosts.

Required columns:

- identity:
  - `planet_id`
  - `stable_object_key`
- host linkage:
  - `system_id` (nullable only if unmatched by policy)
  - `star_id` (nullable only if unmatched by policy)
  - `host_gaia_id`, plus optional host crosswalk IDs
  - `match_method`
  - `match_confidence`
  - `match_notes`
- planet parameters (source-native where available)
- spatial fields inherited from matched host when matched
- provenance contract fields

## QC Requirements

Build must fail on:

1. provenance contract violation
2. coordinate invariant violation
3. Morton-domain overflow
4. invalid grouping cardinality (missing/duplicate star-to-system assignments)

Reports must include:

- row counts
- multiplicity summary and gate metrics
- matching summary
- provenance summary

## Compatibility and Migration

During Gaia-first migration:

- parallel builds may exist (legacy AT-HYG path vs Gaia-first path)
- API-facing schema fields should remain stable where practical
- any field semantic changes must be documented in release notes and build metadata
