# Spacegate Arm Schema Contract

This document defines the immutable supplemental science layer (`arm`).

Purpose:
- store deterministic, provenance-bound science derivatives outside core hot paths
- materialize multiplicity hierarchy as explicit graph edges
- normalize orbital solutions for UI reconstruction and animation readiness scoring

Out of scope:
- editable fiction/user overlays (`SCHEMA_LORE.md`, rim layer)
- generated prose/images/snapshots (`SCHEMA_RICH.md`, disc layer)
- canonical star/system/planet inventory (`SCHEMA_CORE.md`)

## Artifact Contract

Per build:
- `$SPACEGATE_STATE_DIR/out/<build_id>/arm.duckdb` (target-state artifact)

Hard rules:
- arm artifacts are immutable by `build_id`
- rows are deterministic for pinned inputs/transforms
- every row is provenance-complete

## Key Model

Arm uses graph primitives:
- containment graph: `system_hierarchy_edges`
- dynamic graph: `orbit_edges` + `orbital_solutions`

Every edge/derived record includes:
- `build_id`
- `confidence_score`
- `confidence_tier` (`high|medium|low|illustrative`)
- source lineage and transform lineage

## Tables

## `component_entities`

Canonical component registry for hierarchy assembly.

Columns:
- `component_entity_id BIGINT`
- `stable_component_key TEXT` (deterministic, cross-build stable where possible)
- `component_type TEXT` (`system|star|planet|subplanet|moon|brown_dwarf|compact|cluster_member|unresolved_component`)
- `core_object_type TEXT` (`system|star|planet|NULL`)
- `core_object_id BIGINT` (nullable; links to core if present)
- `display_name TEXT`
- `catalog_component_label TEXT` (e.g., `Aa`, `Bb1`)
- `ra_deg DOUBLE` (nullable for unresolved)
- `dec_deg DOUBLE` (nullable for unresolved)
- `dist_pc DOUBLE` (nullable)
- `source_catalog TEXT`
- `source_version TEXT`
- `source_pk TEXT`
- `source_row_hash TEXT`
- `retrieval_checksum TEXT`
- `retrieved_at TIMESTAMP`
- `ingested_at TIMESTAMP`
- `transform_version TEXT`

## `system_hierarchy_edges`

Explicit containment and subsystem structure.

Columns:
- `hierarchy_edge_id BIGINT`
- `parent_component_key TEXT`
- `child_component_key TEXT`
- `edge_kind TEXT` (`contains|subsystem_of|member_of_pair`)
- `member_role TEXT` (e.g., `primary`, `secondary`, `component`, nullable)
- `catalog_relation_label TEXT` (nullable)
- `depth_hint INTEGER` (nullable)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `evidence_catalogs_json TEXT`
- `evidence_ids_json TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Constraints:
- no self-edge
- no duplicate `(parent_component_key, child_component_key, edge_kind, source_pk)`

## `orbit_edges`

Dynamic relationships used for orbital reconstruction.

Columns:
- `orbit_edge_id BIGINT`
- `host_component_key TEXT` (often subsystem or barycenter-hosting group)
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `relation_kind TEXT` (`binary|circumbinary|hierarchical_pair|bound_companion|satellite`)
- `barycenter_key TEXT` (nullable)
- `preferred_solution_id BIGINT` (nullable FK to `orbital_solutions`)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `evidence_catalogs_json TEXT`
- `evidence_ids_json TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `orbital_solutions`

Catalog-normalized orbital element records.

Columns:
- `orbital_solution_id BIGINT`
- `orbit_edge_id BIGINT`
- `solution_source_catalog TEXT` (`gaia_nss|orb6|msc|wds|...`)
- `solution_rank INTEGER` (1 = best/preferred within source)
- `reference_epoch_jyear DOUBLE` (or `reference_epoch_mjd DOUBLE`)
- `period_days DOUBLE` (nullable)
- `semi_major_axis_au DOUBLE` (nullable)
- `semi_major_axis_arcsec DOUBLE` (nullable)
- `eccentricity DOUBLE` (nullable)
- `inclination_deg DOUBLE` (nullable)
- `longitude_ascending_node_deg DOUBLE` (nullable)
- `argument_periastron_deg DOUBLE` (nullable)
- `time_periastron_jd DOUBLE` (nullable)
- `mean_anomaly_deg DOUBLE` (nullable)
- `mass_ratio_q DOUBLE` (nullable)
- `primary_mass_msun DOUBLE` (nullable)
- `secondary_mass_msun DOUBLE` (nullable)
- `rv_semiamplitude_primary_kms DOUBLE` (nullable)
- `rv_semiamplitude_secondary_kms DOUBLE` (nullable)
- `fit_quality_json TEXT` (chi2/residual summaries, nullable)
- `normalization_method TEXT`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Rule:
- never overwrite source-native measurements; normalized columns are additive transforms with lineage.

## `barycenters`

Derived barycenter metadata used by animation and hierarchy layout.

Columns:
- `barycenter_id BIGINT`
- `barycenter_key TEXT`
- `host_component_key TEXT`
- `x_helio_pc DOUBLE` (nullable)
- `y_helio_pc DOUBLE` (nullable)
- `z_helio_pc DOUBLE` (nullable)
- `vx_helio_kms DOUBLE` (nullable)
- `vy_helio_kms DOUBLE` (nullable)
- `vz_helio_kms DOUBLE` (nullable)
- `mass_basis TEXT` (`measured|catalog_ratio|estimated`)
- `mass_estimation_method TEXT` (nullable)
- `mass_input_json TEXT` (nullable)
- `reference_epoch_jyear DOUBLE` (nullable)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `animation_readiness`

Per-system and per-edge readiness assessment.

Columns:
- `animation_readiness_id BIGINT`
- `stable_object_key TEXT` (system-level key)
- `component_key TEXT` (nullable)
- `orbit_edge_id BIGINT` (nullable)
- `readiness_level TEXT` (`full|partial|illustrative|insufficient`)
- `missing_parameters_json TEXT`
- `inferred_parameters_json TEXT`
- `disallowed_fabrication BOOLEAN`
- `notes_json TEXT`
- `computed_at TIMESTAMP`
- `transform_version TEXT`

## `system_neighbors`

Deterministic neighborhood helper for map traversal.

Columns:
- `neighbor_id BIGINT`
- `source_system_key TEXT`
- `neighbor_system_key TEXT`
- `distance_ly DOUBLE`
- `rank INTEGER`
- `method TEXT`
- `confidence_score DOUBLE`
- `computed_at TIMESTAMP`
- `transform_version TEXT`

## `vsx_variability`

Per-observation variability overlay rows linked to `core.stars` via exact Gaia source ID.

Columns:
- `vsx_variability_id BIGINT`
- `stable_object_key TEXT`
- `star_id BIGINT`
- `gaia_id BIGINT`
- `vsx_oid BIGINT`
- `vsx_name TEXT`
- `variability_flag INTEGER`
- `variability_flag_label TEXT` (`variable|suspected|constant_or_nonexisting|possible_duplicate|unknown`)
- `variability_type_raw TEXT`
- `variability_family TEXT` (`eclipsing|pulsating|rotational|eruptive|extragalactic_or_lensing|other|unknown`)
- `max_mag DOUBLE`
- `max_passband TEXT`
- `min_is_amplitude_flag TEXT`
- `min_mag_or_amplitude DOUBLE`
- `min_passband TEXT`
- `amplitude_mag DOUBLE` (derived from VSX min/max semantics)
- `epoch_hjd DOUBLE`
- `period_days DOUBLE`
- `spectral_type TEXT`
- `confidence_score DOUBLE`
- `confidence_tier TEXT` (`high|medium|low|illustrative`)
- `is_default_usable BOOLEAN`
- `is_high_variability BOOLEAN`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `variability_summary`

One canonical variability row per `stable_object_key`, ranked from `vsx_variability`.

Columns:
- `variability_summary_id BIGINT`
- `stable_object_key TEXT`
- `star_id BIGINT`
- `gaia_id BIGINT`
- `vsx_match_count BIGINT`
- `primary_variability_flag INTEGER`
- `primary_variability_flag_label TEXT`
- `primary_variability_type_raw TEXT`
- `primary_variability_family TEXT`
- `primary_amplitude_mag DOUBLE`
- `primary_period_days DOUBLE`
- `primary_epoch_hjd DOUBLE`
- `primary_is_default_usable BOOLEAN`
- `any_high_variability BOOLEAN`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `ultracoolsheet_objects`

UltracoolSheet overlay rows and youth/kinematics metadata, linked to core via Gaia DR3/DR2 IDs.

Columns:
- `ultracoolsheet_object_id BIGINT`
- `stable_object_key TEXT` (nullable when unmatched)
- `star_id BIGINT` (nullable when unmatched)
- `gaia_id BIGINT` (matched core Gaia ID when present)
- `gaia_dr3_source_id BIGINT`
- `gaia_dr2_source_id BIGINT`
- `object_name TEXT`
- `name_simbadable TEXT`
- `ra_deg DOUBLE`
- `dec_deg DOUBLE`
- `plx_mas DOUBLE`
- `pmra_mas_yr DOUBLE`
- `pmdec_mas_yr DOUBLE`
- `rv_kms DOUBLE`
- `dist_pc DOUBLE`
- `dist_source TEXT`
- `spectral_type_opt TEXT`
- `spectral_type_ir TEXT`
- `spectral_numeric DOUBLE`
- `gravity_opt TEXT`
- `gravity_ir TEXT`
- `age_category TEXT`
- `youth_evidence TEXT`
- `banyan_hypothesis_young TEXT`
- `banyan_prob_young DOUBLE`
- `is_exoplanet_host BOOLEAN`
- `has_unresolved_multiplicity BOOLEAN`
- `has_resolved_multiplicity BOOLEAN`
- `has_higher_mass_companion BOOLEAN`
- `match_confidence DOUBLE`
- `confidence_tier TEXT`
- `ref_discovery TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## Lifecycle Audit Mirrors

Arm mirrors lifecycle lineage tables from core so lifecycle diffs/audits remain available even when hot-path core tables are optimized for serving.

## `planet_catalog_observations`

Per-catalog observation snapshots used for lifecycle resolution.

Columns:
- mirrored 1:1 from `core.planet_catalog_observations`
- includes source identity (`source_catalog`, `source_catalog_object_id`), payload fields, retrieval lineage, and transform lineage

## `planet_status_history`

Deterministic status-resolution history per planet.

Columns:
- mirrored 1:1 from `core.planet_status_history`
- includes previous/new status, resolution reasons, classifier versions, and timestamps

## `planet_reclassification_audit`

Derived change log for lifecycle/taxonomy/habitability classifier transitions.

Columns:
- mirrored 1:1 from `core.planet_reclassification_audit`
- includes transition type, impacted flags, and classifier version lineage

## Quality Gates (Arm)

Build fails when:
- any edge references missing component keys
- hierarchy cycle detected in `contains/subsystem_of`
- `preferred_solution_id` points to non-existent orbital solution
- confidence/provenance required fields are null
- `animation_readiness` marks `full` while missing required Kepler set

## MSC Policy

MSC is mandatory in default science ingest and arm hierarchy/orbit derivation.

If MSC retrieval/cook fails:
- ingest must fail
- promotion must not proceed
