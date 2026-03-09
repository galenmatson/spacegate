# Spacegate Core Schema Contract (Gaia-First)

This document defines the canonical core astronomy contract used by ingestion, QC, and API.

Scope:

- immutable astronomy inventory and relationships
- deterministic build outputs
- complete provenance for served rows

Out of scope:

- generated exposition/images (`SCHEMA_RICH.md`, disc layer)
- editable fiction/worldbuilding overlays (`SCHEMA_LORE.md`, rim layer)
- immutable supplemental science side tables (arm layer)

## Artifact Contract

Per build:

- `$SPACEGATE_STATE_DIR/out/<build_id>/galaxy.duckdb` (target-state full canonical astronomy corpus)
- `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb` (default fast astronomy projection)
- `$SPACEGATE_STATE_DIR/out/<build_id>/halo.duckdb` (complementary opt-in astronomy projection)
- `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{stars,systems,planets}.parquet` (projection-specific export set)
- `$SPACEGATE_STATE_DIR/reports/<build_id>/*.json`

Build IDs are immutable and deterministic for pinned inputs and transforms.

Transition note:

- current runtime may still emit only `core.duckdb` while `galaxy`/`halo` artifact materialization is finalized.

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

Required for cross-dataset joins (`core`/`disc`/`rim`) and rebuild continuity.

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
- slice contract:
  - `slice_profile_id`
  - `slice_profile_version`
- spatial indexing parameters:
  - Morton config fields
- active multiplicity gate parameters:
  - WDS-Gaia thresholds when applicable
- alias/search contract flags:
  - `aliases_enabled`
  - `athyg_alias_crosswalk_enabled`
  - `athyg_supplement_merge_enabled`
- identifier stewardship gates:
  - `identifier_ambiguous_limit`
  - `identifier_gaia_collision_max`
  - `identifier_hip_collision_max`
  - `identifier_hd_collision_max`

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
- spectral normalization note:
  - Gaia DR3 backbone does not provide a complete discrete MK class for all rows.
  - `spectral_class` may be inferred from `teff_gspphot` with `bp_rp` fallback only when no stronger classification evidence exists.
  - keep `spectral_type_raw` as nullable provenance text; do not fabricate MK subtype/luminosity class beyond available evidence.
- classification safety:
  - required canonical field: `object_family` (`star`, `brown_dwarf`, `white_dwarf`, `neutron_star`, `black_hole`, `planetary_nebula`, `other`)
  - recommended evidence fields:
    - `classprob_dsc_combmod_whitedwarf` (nullable)
    - `classprob_dsc_specmod_whitedwarf` (nullable)
    - `classification_evidence_json` (source/value/confidence payload)
  - if remnant evidence is positive, fallback spectral-temperature mapping must not force normal stellar family labels.
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

## `aliases`

Deterministic name and identifier lookup table spanning both system-level and star-level targets.

Required columns:

- identity:
  - `alias_id`
  - `target_type` (`system` or `star`)
  - `target_id` (target row ID in its table)
  - `system_id` (nullable for non-system targets)
  - `star_id` (nullable for non-star targets)
- alias payload:
  - `alias_raw` (display form)
  - `alias_norm` (normalized lookup key)
  - `alias_kind` (for example: `proper_name`, `bayer_name`, `flamsteed_name`, `hip_id`, `hd_id`, `hr_id`, `wds_id`, `member_proper_name`)
  - `alias_priority` (lower = stronger)
  - `is_primary` (boolean)
- source traceability:
  - `source_catalog`
  - `source_version` (nullable where source does not version aliases cleanly)
  - `source_pk` (nullable where source row key is unavailable)

Contract notes:

- alias rows enrich lookup and UX only; they do not define canonical star existence.
- duplicate aliases must be deduplicated per `(target_type, target_id, alias_norm)` by deterministic precedence.
- search must resolve against normalized aliases first-class alongside canonical names.
- Gaia-first builds may use constrained positional matching for named AT-HYG rows without Gaia IDs to recover legacy/common aliases (with tight angular and distance gates).

## `object_identifiers`

Canonical and non-canonical identifier edge table for deterministic ID resolution and stewardship checks.

Required columns:

- identity:
  - `identifier_id`
  - `target_type` (currently `star`)
  - `target_id` (row id in target table)
- identifier payload:
  - `namespace` (`gaia_dr3`, `gaia_legacy`, `hip`, `hd`, `hr`, `gl`, `tyc`, `hyg`, `wds`, ...)
  - `id_value_raw`
  - `id_value_norm`
  - `is_canonical`
- resolution traceability:
  - `resolution_method` (`canonical_column`, `catalog_json`, `gaia_remap_*`, ...)
  - `resolution_confidence`
  - `source_catalog`
  - `source_version`
  - `source_pk`
  - `evidence_json`

Contract notes:

- canonical IDs in this table must reflect `stars` canonical columns.
- non-canonical IDs (for example legacy Gaia remaps) must preserve the original incoming identifier and resolution evidence.
- collisions are evaluated by namespace against distinct targets and enforced through QC gates.

## `identifier_quarantine`

Rows withheld from automatic merge due to ambiguous or conflicting identifier evidence.

Required columns:

- `quarantine_id`
- `source_catalog`
- `source_version`
- `source_pk`
- `gaia_id` (nullable)
- `hip_id` (nullable)
- `hd_id` (nullable)
- `reason` (for example `gaia_id_multi_match`, `hip_hd_conflict`, `positional_ambiguous`)
- `details_json`
- `created_at`

Contract notes:

- quarantined rows are excluded from automatic upsert/insert passes.
- quarantine volume is bounded by QC gate thresholds and must fail build promotion when exceeded.

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

## Supplementary Science Tables

These immutable science tables are emitted alongside `stars/systems/planets` and are intended for enrichment, diagnostics, and future UI/query expansion.

## `compact_objects`

Catalog-native compact/remnant objects (currently ATNF pulsars and McGill magnetars), with optional positional match to core stars.

Expected columns:

- identity:
  - `compact_object_id`
  - `stable_object_key`
- object semantics:
  - `object_family` (currently `neutron_star`)
  - `object_type` (`pulsar` / `magnetar`)
  - `object_name`
- coordinates/kinematics (source-native):
  - `ra_deg`, `dec_deg`
  - `dist_pc`, `dist_ly` (nullable)
  - `parallax_mas` (nullable)
- cross-linking:
  - `star_id` (nullable)
  - `system_id` (nullable)
  - `match_method`
  - `match_confidence`
  - `match_angular_distance_arcsec`
  - `match_distance_delta_ly`
- catalog/source payload:
  - `catalog_ids_json`
  - provenance contract fields

## `open_clusters`

Open cluster catalog rows (Cantat-Gaudin 2020 summary table) as first-class supplemental science objects.

Expected columns:

- identity:
  - `cluster_id`
  - `stable_object_key`
  - `cluster_name`
- coordinates:
  - `ra_deg`, `dec_deg`
  - `glon_deg`, `glat_deg`
  - `radius_r50_deg`
  - `dist_pc`, `dist_ly` (nullable)
- kinematics/summary:
  - `pm_ra_mas_yr`, `pm_dec_mas_yr`
  - `parallax_mas`
  - `member_count_prob_gt_0_7`
  - `source_flag`
- provenance contract fields

## `open_cluster_memberships`

Star-to-open-cluster membership edges derived from the Cantat-Gaudin member table.

Expected columns:

- `cluster_membership_id`
- `cluster_id`
- `cluster_name`
- `star_id`
- `system_id`
- `gaia_id`
- `membership_probability`
- `match_method`
- `match_confidence`

## `superstellar_objects`

Supplementary large-scale/non-stellar objects (currently open clusters + Galactic SNR rows) for future map and exploration features.

Expected columns:

- identity:
  - `superstellar_object_id`
  - `stable_object_key`
- semantics:
  - `object_family`
  - `object_type` (for example `open_cluster`, `supernova_remnant`)
  - `object_name`
- coordinates:
  - `ra_deg`, `dec_deg`
  - `dist_pc`, `dist_ly` (nullable)
- payload:
  - `object_meta_json`
  - provenance contract fields

## QC Requirements

Build must fail on:

1. provenance contract violation
2. coordinate invariant violation
3. Morton-domain overflow
4. invalid grouping cardinality (missing/duplicate star-to-system assignments)
5. classification invariant violation:
   - remnant-positive evidence with non-remnant emitted `object_family` and no explicit override
6. silent classifier downgrade:
   - source-native remnant marker (for example white-dwarf `D*` spectral evidence) overwritten by temperature fallback without override

Reports must include:

- row counts
- multiplicity summary and gate metrics
- matching summary
- provenance summary
- classification safety summary:
  - remnant evidence counts by source
  - remnant vs emitted family mismatch counts
  - explicit override counts and reasons

## Compatibility and Migration

During Gaia-first migration:

- parallel builds may exist (legacy AT-HYG path vs Gaia-first path)
- API-facing schema fields should remain stable where practical
- any field semantic changes must be documented in release notes and build metadata
