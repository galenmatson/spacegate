# Spacegate Core Schema Contract (Gaia-First)

This document defines the canonical core astronomy contract used by ingestion, QC, and API.

Scope:

- immutable astronomy inventory and relationships
- deterministic build outputs
- complete provenance for served rows

Out of scope:

- immutable science evidence/support side tables (`SCHEMA_ARM.md`, arm layer)
- generated exposition/images (`SCHEMA_DISC.md`, disc layer)
- editable fiction/worldbuilding overlays (`SCHEMA_RIM.md`, rim layer)

## Artifact Contract

Per build:

- `$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb` (served canonical astronomy inventory/projection)
- `$SPACEGATE_STATE_DIR/out/<build_id>/parquet/{stars,systems,planets}.parquet` (projection-specific export set)
- `$SPACEGATE_STATE_DIR/reports/<build_id>/*.json`

Build IDs are immutable and deterministic for pinned inputs and transforms.

Transition note:

- Earlier Galaxy/Halo complement artifacts are retired from the active contract.
  The `halo` name is reserved for a possible future larger/full-Gaia product,
  not the old complement projection.

## Canonical Inventory Policy

Gaia-first contract:

1. Canonical star inventory originates from Gaia.
2. Crosswalk catalogs may enrich identifiers/aliases but do not define canonical star existence.
3. Multiplicity catalogs define evidence/edges and grouping confidence, not hidden row mutation.

Narrow completeness bridge:

- Nearby vetted ultracool-dwarf records may be promoted into `core.stars`
  when the Gaia backbone does not contain the object and the source row carries
  usable astrometry. The first bridge is `UltracoolSheet` within the configured
  nearby distance cap, intended to cover blind spots such as WISE 0855-0714 and
  Luhman 16 without waiting for a full WISE/CatWISE survey ingest.
- These rows keep `source_catalog = 'ultracoolsheet'` and full row-level
  provenance. They are accepted inventory rows, not Gaia facts, and their
  source-specific multiplicity flags are preserved as evidence rather than
  expanded into invented component hierarchies.
- Full CatWISE/AllWISE integration remains a separate survey-scale ingest
  milestone because it changes volume, matching, retention, and performance
  characteristics.

Core/arm promotion contract:

- `core` owns accepted canonical inventory rows and promoted hot-path scalar
  facts.
- `arm` owns source-native evidence/support rows, alternate solutions,
  confidence-ranked graph/orbit structures, and deterministic science
  derivatives.
- A value being source-native does not automatically make it core. Source-native
  rows may remain in `arm` when they represent support evidence, competing
  claims, source ontology, or non-hot-path simulation inputs.
- Current `core.planets` orbital scalar columns (`orbital_period_days`,
  `semi_major_axis_au`, `eccentricity`, `inclination_deg`) are retained as
  promoted serving summaries during the migration. The normalized orbit edge,
  source-solution, uncertainty, epoch, ranking, and simulation contract lives in
  `arm.orbit_edges` and `arm.orbital_solutions`.
- Canonical builds must rebuild `arm.duckdb` from the emitted canonical
  `core.duckdb`. Copying bootstrap ARM into a canonical build is invalid
  because component keys would still reference pre-canonical stable object keys.

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
- MSC (mandatory)
- WDS/ORB6 (support evidence)
- SBX spectroscopic binaries (default-on support evidence)

Required system-level fields:

- `grouping_basis`
- `grouping_confidence`
- `grouping_source_catalogs_json`
- `has_gaia_nss_evidence`
- `has_msc_evidence`
- `has_sbx_evidence`
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

Target state note:
- explicit hierarchy/orbit/barycenter graph contracts are defined in `SCHEMA_ARM.md`

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
  - `astrometry_boundary_strategy`
  - `astrometry_boundary_min_parallax_mas` (Gaia-first builds)
  - `astrometry_boundary_distance_ly_approx` (Gaia-first builds)
  - `astrometry_quality_policy_source`
  - `astrometry_quality_min_parallax_over_error`
  - `astrometry_quality_max_parallax_error_mas`
  - `astrometry_quality_max_ruwe`
- slice contract:
  - `slice_profile_id`
  - `slice_profile_version`
- determinism contract:
  - `source_inputs_fingerprint`
  - `determinism_stars_xor_hash`
  - `determinism_systems_xor_hash`
  - `determinism_planets_xor_hash`
- spatial indexing parameters:
  - Morton config fields
- active multiplicity gate parameters:
  - WDS-Gaia thresholds when applicable
  - multiplicity exact-duplicate gates:
    - `SPACEGATE_MULTIPLICITY_GAIA_DUPLICATE_MAX` (default `0`)
    - `SPACEGATE_MULTIPLICITY_WDS_COMPONENT_DUPLICATE_MAX` (default `0`)
- alias/search contract flags:
  - `aliases_enabled`
  - `athyg_alias_crosswalk_enabled`
  - `athyg_supplement_merge_enabled`
- identifier stewardship gates:
  - `identifier_ambiguous_limit`
  - `identifier_gaia_collision_max`
  - `identifier_hip_collision_max`
  - `identifier_hd_collision_max`
  - default gates (overridable by env at ingest time):
    - ambiguous: `10000`
    - gaia collisions: `0`
    - hip collisions: `3000`
    - hd collisions: `3000`

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
  - persist a single canonical `teff_k` in `core.stars` for search/detail UX. Prefer specialist remnant temperatures when available (for example Gaia EDR3 white-dwarf catalog), otherwise use Gaia `teff_gspphot`.
- classification safety:
  - required canonical field: `object_family` (`star`, `brown_dwarf`, `white_dwarf`, `neutron_star`, `black_hole`, `planetary_nebula`, `other`)
  - recommended evidence fields:
    - `classprob_dsc_combmod_whitedwarf` (nullable)
    - `classprob_dsc_specmod_whitedwarf` (nullable)
    - `wd_catalog_pwd` (nullable)
    - `wd_catalog_name` (nullable)
    - `wd_catalog_fit_model` (nullable)
    - `wd_catalog_teff_k` (nullable)
    - `wd_catalog_logg_cgs` (nullable)
    - `wd_catalog_mass_msun` (nullable)
    - `classification_evidence_json` (source/value/confidence payload)
  - if remnant evidence is positive, fallback spectral-temperature mapping must not force normal stellar family labels.
- multiplicity evidence:
  - `wds_id` (nullable)
  - `multiplicity_match_method`
  - `multiplicity_match_confidence`
  - `multiplicity_source_catalogs_json`
  - Gaia NSS evidence fields
  - SBX evidence fields: `sbx_sn`, `sbx_orbit_count`, `sbx_family`, `sbx_position_epoch`, `sbx_position_source`
- provenance contract fields

Stable-key contract:

- source-native bootstrap keys are preserved under the canonical namespace
  when no stronger canonical identifier key exists
- sequential ingestion row numbers are forbidden as emitted stable identity;
  numeric row IDs remain implementation details and may change across rebuilds
- reconciliation may supersede a duplicate source surrogate, but the surviving
  object's source-native identity must remain stable and auditable

## `systems`

Derived system/grouping table for navigation and search.

Required columns:

- identity:
  - `system_id`
  - `stable_object_key`
- naming:
  - `system_name`
  - `system_name_norm`
  - system-side search acceleration may materialize:
    - `star_count`
    - `planet_count`
    - `star_teff_count`
    - `min_star_teff_k`
    - `max_star_teff_k`
    - `spectral_classes_json`
    - `spectral_class_mask`
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

Contract notes:

- System stable keys follow the same source-native rule. Singleton Gaia
  systems, named source systems, Sol, and other accepted source inventories
  must not fall back to `canon:system:legacy:<row_id>`.
- `systems` is the hot-path serving table for browse/search/detail traffic.
- Search/filter UX should prefer precomputed system-side summary fields over runtime scans of `stars` whenever exact system semantics can be preserved.
- `spectral_class_mask` is a deterministic OR-mask over normalized system member spectral buckets using:
  - `O=1`
  - `B=2`
  - `A=4`
  - `F=8`
  - `G=16`
  - `K=32`
  - `M=64`
  - `L=128`
  - `T=256`
  - `Y=512`
  - `D=1024`
- `min_star_teff_k` / `max_star_teff_k` are pruning facets for temperature search and detail summaries; exact per-star temperature filtering may still require row-level confirmation against `stars` when a query asks whether any member falls inside a narrow interval.

## `aliases`

Deterministic name and identifier lookup table spanning object-level targets.
The current hot-path public search implementation materializes system and
star/member aliases first; the target contract reserves the same shape for
planet, compact-object, Sol small-body, and artificial-object aliases as those
routes mature.

Required columns:

- identity:
  - `alias_id`
- `target_type` (`system`, `star`; future/reserved: `planet`,
  `compact_object`, `sol_small_body`, `artificial_object`)
  - `target_id` (target row ID in its table)
  - `system_id` (nullable for non-system targets)
  - `star_id` (nullable for non-star targets)
- alias payload:
  - `alias_raw` (display form)
  - `alias_norm` (normalized lookup key)
- `alias_kind` (for example: `proper_name`, `bayer_name`,
  `bayer_expanded_name`, `flamsteed_name`, `hip_id`, `hd_id`, `hr_id`,
  `wds_id`, `gl_id`, `gliese_id`, `gj_id`, `tic_id`, `toi_id`,
  `member_proper_name`)
  - `alias_priority` (lower = stronger)
  - `is_primary` (boolean)
- source traceability:
  - `source_catalog`
  - `source_version` (nullable where source does not version aliases cleanly)
  - `source_pk` (nullable where source row key is unavailable)

Contract notes:

- alias rows enrich lookup and UX only; they do not define canonical star existence.
- Evidence Lake E7 migration seed `6b4fb210e1b1bcf61299fe7f` preserves all
  1,026,480 reviewed aliases by permanent `stable_object_key` and
  `system_stable_object_key`. It deliberately omits legacy numeric target IDs
  and every scientific scalar; the clean CORE compiler resolves current numeric
  IDs from E2 canonical object nodes when materializing this table.
- duplicate aliases must be deduplicated per `(target_type, target_id, alias_norm)` by deterministic precedence.
- search must resolve against normalized aliases first-class alongside canonical names.
- Gaia-first builds may use constrained positional matching for named AT-HYG
  rows without Gaia IDs to recover legacy/common aliases, with tight angular and
  distance gates.
- Positional AT-HYG matches are weak alias hints, not catalog identifier
  equivalence: they must not promote HIP/HD/HR/GL/TYC/HYG identifiers into
  core stars. Non-compact AT-HYG rows must not be positional-matched onto
  compact-object or white-dwarf targets.
- Build verification includes a compact-alias safety check for Sirius-class
  hazards: a compact-object row without a non-compact sibling must not carry
  bright-primary AT-HYG aliases plus HD/WDS or non-proper primary aliases. The
  check is warn-only during the current served-artifact repair window and can
  be made strict with `SPACEGATE_VERIFY_COMPACT_ALIAS_SAFETY=1`.
- Legacy accepted-supplement ingestion is retired and is not part of the
  canonical production contract. Former cases are preserved as non-executable
  deferred adjudication inputs in `config/deferred_core_adjudications.json`.
  Canonical promotion requires a reusable source/reconciliation rule or a
  reviewed, inspectable adjudication artifact; a local object-specific config
  row is insufficient.
- Gaia-fallback display names may be promoted from matched exoplanet host labels when canonical/common stellar labels are absent.
- host-label precedence in Gaia-fallback promotion should prefer:
  - human/common labels
  - survey/mission-style host labels (for example `TRAPPIST`, `Kepler`, `TOI`, `WASP`, ...)
  - legacy catalog-style labels
  - Gaia IDs last
- Gliese/GJ identifiers present in source catalog ID payloads may emit display
  variants such as `Gl 412A`, `Gl 412 A`, `Gl 412`, `Gliese 412 A`,
  `Gliese 412`, `GJ 412 A`, and `GJ 412`. These are alias/search evidence,
  not companion-rollup evidence; they must not merge `A`/`B` components by
  name alone.
- Public display-name style is derived presentation/search metadata layered on
  top of aliases. The active UI/API policy distinguishes full layperson names
  (`public_full`), traditional abbreviated forms (`astronomer_abbrev`), compact
  catalog labels (`catalog_compact`), and source-native technical labels
  (`source_technical`). This policy never changes source object identity,
  accepted system membership, or canonical science fields.
- Matched aliases remain separate from display names. For example a query for
  `eps ind` may carry `matched_alias = Eps Ind` while the default public
  display name remains `Epsilon Indi`.

## `system_search_terms`

Derived immutable search-acceleration table for system lookup.

This table denormalizes canonical system names plus all aliases already resolved to a `system_id`, including star-target aliases that point into a system. It exists to keep hot-path search off the full `aliases` and `stars` tables on constrained public hosts.

Required columns:

- identity:
  - `search_term_id`
  - `system_id`
  - `target_type`
  - `target_id`
  - `star_id`
  - `alias_id`
- search payload:
  - `term_raw`
  - `term_norm`
  - `term_kind`
  - `term_priority`
  - `is_primary`
- source traceability:
  - `source_catalog`
  - `source_version`
  - `source_pk`

Contract notes:

- `system_search_terms` is an acceleration artifact, not an authority layer.
- rows must be deterministically deduplicated per `(system_id, term_norm)`.
- canonical `systems.system_name_norm` must always be represented.
- any alias with a resolved `system_id` may be included, even when the source alias row targets a `star`.
- target fields preserve member context for search responses. A member alias
  can resolve to the accepted root system while still pointing at the source
  member row for focus/highlight behavior.
- search may use this table for exact/prefix/token/fuzzy candidate generation, while detail UX still reads authoritative aliases from `aliases`.
- Exact-like dense identifiers and variable-star names should disable fuzzy
  alias substitution unless a real exact/prefix search-term hit exists. For
  example, `V1513 Cyg` must not silently resolve to `V1581 Cyg`.

## `alias_authority_diagnostics`

Derived build-report table for auditing alias authority behavior.

Required columns:

- `diagnostic_id`
- `diagnostic_kind`
- `term_norm`
- `row_count`
- `system_count`
- `target_type_count`
- `details_json`

Current diagnostic families:

- `shared_alias_across_systems`: normalized alias/search term appears under
  multiple root systems.
- `alias_attached_to_multiple_target_levels`: the same normalized term appears
  at more than one target level.
- `catalog_display_name_fallback`: public display names still fall back to
  Gaia/WDS-style catalog labels, useful for prioritizing future name-authority
  enrichment.

## Evidence Lake v2 Identity Graph (Pre-CORE Compiler Artifact)

M8.3c-E2 materializes a release-scoped identity and scope graph under
`derived/evidence_lake_v2/identity/<graph_id>/`. This graph is not a CORE table
set and is not served directly. It reads the current CORE as
`stability_reference_not_new_authority`; its policy prohibits inventory,
identity, and containment mutation until the E6 shadow build.

Artifact tables:

- `canonical_object_nodes`: permanent Spacegate system, star, and planet keys
  projected from the stability reference with object type, row ID, root system,
  display name, and reference build ID.
- `identifier_nodes`: identifier values scoped by namespace, source, and
  release. Gaia DR2 and DR3 nodes always have distinct node keys.
- `canonical_identifier_bindings`: provenance-bearing current CORE identifier
  bindings to permanent object nodes. Shared identifiers remain separate
  binding rows for collision/component-scope diagnosis.
- `release_crossmatch_edges`: every preserved official Gaia DR2/DR3
  neighborhood pair, forward/reverse presence, raw and normalized match
  metrics, epoch-propagation flag, row counts, and payload-consistency result.
- `dr2_release_outcomes`: exactly one accepted, missing, excluded, ambiguous,
  or quarantined row per targeted DR2 ID, including candidate/predecessor sets,
  forward/reverse release lineage, canonical binding, high-proper-motion and
  duplicate-system safeguards, reason, and evidence JSON.
- `source_record_bindings`: source-family/DR2 target groups, source scope,
  registered source/release/table, record counts, reconciliation outcome, and
  accepted permanent object binding.
- `scope_claims`: physical identity, system containment, component/subsystem,
  observation-target, and alias/public-name claims. Current hierarchy claims
  are labeled stability references; raw MSC/WDS relations remain candidates.
- `identifier_collision_diagnostics`: multi-object namespace bindings with
  explicit component-aware or review dispositions.
- `identity_quarantine`: ambiguous release outcomes and genuinely unsafe
  canonical identifier collisions with the complete candidate evidence.
- `graph_metadata`: content identity, compiler/policy versions, consumed-input
  fingerprint, and explicit false inventory/containment mutation flags.

Every table also has an ordered Zstandard Parquet representation with rows,
bytes, and SHA-256 recorded in `e2_identity_graph_report.json`. The DuckDB file
is a compiler/inspection convenience, not the only durable representation.

## `object_identifiers`

Canonical and non-canonical identifier edge table for deterministic ID resolution and stewardship checks.

Required columns:

- identity:
  - `identifier_id`
  - `target_type` (currently `star`)
  - `target_id` (row id in target table)
- identifier payload:
  - `namespace` (`gaia_dr3`, `gaia_legacy`, `hip`, `hd`, `hr`, `gl`, `tyc`, `hyg`, `wds`, `tic`, ...)
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
- accepted `tic` identifiers are non-canonical identity edges. They retain
  targeted TIC provenance and the Gaia DR2-to-DR3 or alternate-catalog evidence
  used to resolve the host star.
- TIC split, duplicate, and artifact rows must never become accepted identifier
  edges merely because they have positional or external-catalog candidates.
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
- targeted TIC quarantine reasons include `tic_split`, `tic_duplicate`,
  `tic_artifact`, `tic_duplicate_id`, and
  `best_precedence_multiple_stars`; full competing-candidate evidence remains
  in `details_json`.

## `source_object_reconciliation`

Audit table for source rows that were proven to be duplicate physical-object
surrogates after late identifier enrichment. The first materialized policy is
`strong_identifier_with_physical_sanity_v1`, used to reconcile MSC component
surrogates onto enriched Gaia/accepted source rows before root-system grouping.

Required columns:

- `reconciliation_id`
- `surviving_star_id`
- `duplicate_star_id`
- `surviving_stable_object_key`
- `duplicate_stable_object_key`
- `surviving_name`
- `duplicate_name`
- `target_type`
- `duplicate_role`
- `match_method` (`msc_component_reconciled_hip_hd`, `msc_component_reconciled_hip`, ...)
- `match_confidence`
- `hip_id` / `hd_id` / `wds_id` / `component`
- `dist_delta_ly`
- `ang_sep_arcsec`
- `evidence_json`
- `created_at`

Contract notes:

- reconciliation preserves the surviving source object identity and removes
  only the duplicate surrogate row before system grouping.
- reconciled MSC/WDS component evidence is copied onto the surviving star as
  multiplicity evidence; it does not overwrite Gaia/source object identity.
- ambiguous candidates are excluded from automatic merge and materialized in
  `source_object_reconciliation_quarantine`.
- Alpha Centauri / Proxima Centauri is the benchmark: the Gaia Proxima row
  remains the planet host while inheriting the MSC/WDS component-C evidence
  needed to roll into the accepted Alpha Centauri physical system.

## `source_object_reconciliation_quarantine`

Rows considered for source-object reconciliation but withheld because the
candidate was ambiguous or failed one-to-one ranking.

Required columns:

- `quarantine_id`
- all source/survivor identity fields from `source_object_reconciliation`
- `match_score`
- `duplicate_rank`
- `survivor_rank`
- `duplicate_best_score_count`
- `survivor_best_score_count`
- `evidence_json`
- `created_at`

Contract notes:

- quarantined candidates are diagnostics only; they must not affect root
  system membership.
- quarantine counts are reported in `identifier_report.json`,
  `system_grouping_report.json`, and build QC counts.

## `planets`

Exoplanet records matched to canonical hosts, including lifecycle states that are not default-visible.

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
- lifecycle/status:
  - `planet_status` (`confirmed`, `candidate`, `controversial`, `retracted`)
  - `is_default_visible` (bool; policy-materialized for default science queries)
  - `is_tombstoned` (bool; for retained lineage rows that must not appear in default science views)
  - `status_source_catalog`
  - `status_updated_at`
  - `status_superseded_by` (nullable stable key for replacement/merged objects)
- taxonomy tags (deterministic, versioned):
  - `planet_size_mass_class`
  - `planet_insolation_class`
  - `planet_orbit_class`
  - `planet_composition_proxy_class`
  - `planet_detection_tags_json`
  - `planet_host_context_tags_json`
  - `planet_classifier_version`
  - `planet_classifier_updated_at`
- planet parameters (source-native where available)
- habitability and resource utility:
  - `spacegate_hab_score` (`0..1`)
  - `spacegate_hab_confidence`
  - `spacegate_hab_reasons_json`
  - `host_metallicity_feh` (nullable; source-native if available)
  - `host_metallicity_feh_error` (nullable)
  - `planet_element_richness_score` (`0..1`, nullable)
  - `planet_element_richness_class` (`very_low`, `low`, `moderate`, `high`, `very_high`, `unknown`)
  - `planet_element_richness_method` (`host_spectroscopy_proxy`, `direct_atmosphere_signal`, `mixed`, `unknown`)
  - `planet_element_richness_notes`
- spatial fields inherited from matched host when matched
- provenance contract fields

Contract notes:

- `planet_element_richness_*` is a rim/search utility proxy and must not be presented as direct measured bulk composition unless direct spectral evidence exists.
- `retracted` records may be retained only with `is_tombstoned=true` and `is_default_visible=false`.
- status/taxonomy/habitability/resource tags are deterministic derived fields and must carry explicit versioning.
- canonical science classifications must remain source-faithful; any UI/navigation supergrouping (for example `subplanet`) belongs in derived/structural tags and must not overwrite authoritative class semantics.
- Planet rows are canonical inventory after lifecycle/host adjudication, but
  detailed orbital solution evidence is not automatically core. Core may expose
  promoted source/default scalar fields for browse/search/detail; alternate
  source solutions, reference epochs, fit quality, uncertainty, and
  simulation-oriented orbit contracts belong in `arm.orbit_edges` and
  `arm.orbital_solutions`.
- `stable_object_key` must be unique for visible planet inventory rows. If a
  source row fans out through multiple possible host-star matches, ingest must
  choose one deterministic best match before writing `core.planets`; otherwise
  ARM orbit solutions multiply and simulator rank-1 orbit selection becomes
  ambiguous.

## Planet Lifecycle and Re-Evaluation Contract

Refresh behavior for exoplanet sources must be delta-aware:

- run per-source snapshot diff keyed by deterministic source identity
- materialize lifecycle transitions (`new`, `changed`, `unchanged`, `missing`, `retracted`, `promoted`, `demoted`)
- recompute derived fields for all impacted rows:
  - changed rows
  - rows with changed host parameters used by classifiers (for example metallicity, luminosity, flux-dependent inputs)
  - rows affected by status precedence changes across overlapping catalogs

Required outputs:

- `reports/<build_id>/planet_catalog_delta_report.json`
- `reports/<build_id>/planet_reclassification_report.json`

Hard gate:

- build fails if any served planet row has stale derived-tag version relative to active classifier version for that build.

## Supplementary Science Tables

These immutable science tables are emitted alongside `stars/systems/planets` and are intended for enrichment, diagnostics, and future UI/query expansion.

## `compact_objects`

Catalog-native compact/remnant objects (currently ATNF pulsars, McGill magnetars, and Gaia EDR3 white dwarfs), with optional positional match to core stars.

Expected columns:

- identity:
  - `compact_object_id`
  - `stable_object_key`
- object semantics:
  - `object_family` (`neutron_star` / `white_dwarf`)
  - `object_type` (`pulsar` / `magnetar` / `white_dwarf`)
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

## `eclipsing_binaries`

Supplementary eclipsing-binary evidence table sourced from DEBCat, Kepler EB, and TESS EB exports.

Expected columns:

- identity:
  - `eclipsing_binary_id`
  - `stable_object_key`
  - `source_catalog_object_id`
  - `object_name`
- optional links into canonical inventory:
  - `star_id` (nullable)
  - `system_id` (nullable)
  - `match_method` (`catalog_id_alias`, `catalog_id_system_alias`, `debcat_name_alias`, `debcat_star_name`, `tess_radec_1arcsec`, `tess_radec_2arcsec`, `unmatched`)
  - `match_confidence`
- linkage policy:
  - ID-first deterministic joins via existing alias namespaces (`TIC`, `KIC`, DEBCat names) where uniquely resolvable
  - positional fallback for TESS EB using Gaia-side sky coordinates with conservative radius/confidence bands
  - unmatched rows remain in `eclipsing_binaries` with explicit `match_method='unmatched'` for auditability
- common orbital/phenomenology fields:
  - `period_days`, `period_error_days`
  - `bjd0`, `bjd0_error`
  - `morphology`
  - `glon_deg`, `glat_deg`
  - `kmag`
  - `teff_k`
  - `has_short_cadence`
- DEBCat physical parameters (nullable outside DEBCat rows):
  - primary/secondary spectral types
  - primary/secondary masses, radii, gravities, temperatures, luminosities
  - metallicity with uncertainty
- provenance contract fields

## Extended-Object Tables

`extended_objects` is the canonical serving projection for non-stellar catalog
objects. Its identity is separate from `systems`, `stars`, and `planets`.

Key columns:

- identity: `extended_object_id`, `stable_object_key`, `canonical_name`,
  `display_name`, `entity_kind`, `object_family`, `object_type`
- geometry: ICRS `ra_deg`, `dec_deg`, shape/axis/position-angle fields, selected
  source record, and geometry status
- distance/placement: nullable `dist_pc`, `dist_ly`, interval, method,
  confidence, evidence JSON, `map_domain`, and nullable radius tier
- nullable heliocentric XYZ only for admitted `local_3d` rows
- full provenance contract and policy/transform versions

Supporting tables are `extended_object_aliases`,
`extended_object_identifiers`, `extended_object_search_terms`,
`extended_object_source_reconciliation`, and
`extended_object_identity_quarantine`. Every targeted source record must have an
explicit reconciliation outcome and reason. See `docs/EXTENDED_OBJECTS.md`.

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
7. stale planet derivation state:
   - any `planets` row where lifecycle/taxonomy/habitability/resource-richness fields were not recomputed with the active classifier version for the build

Reports must include:

- row counts
- multiplicity summary and gate metrics
- matching summary
- provenance summary
- classification safety summary:
  - remnant evidence counts by source
  - remnant vs emitted family mismatch counts
  - explicit override counts and reasons
- planet lifecycle/reclassification summary:
  - status transitions by catalog and transition type
  - derived-tag recompute counts and skipped-row counts (must be zero when served)

## Compatibility and Migration

During Gaia-first migration:

- parallel builds may exist (legacy AT-HYG path vs Gaia-first path)
- API-facing schema fields should remain stable where practical
- any field semantic changes must be documented in release notes and build metadata
