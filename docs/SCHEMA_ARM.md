# Spacegate Arm Schema Contract

This document defines the immutable science evidence/support layer (`arm`).

Purpose:
- store deterministic, provenance-bound science support rows outside core hot
  paths
- preserve source-native evidence rows whose ontology should not be promoted
  directly into canonical core
- materialize multiplicity hierarchy as explicit graph edges
- normalize orbital solutions for UI reconstruction and animation readiness scoring
- supply the science-side inputs for `docs/SYSTEM_SIMULATION.md` without storing
  visualization-only assumptions

Out of scope:
- editable fiction/user overlays (`SCHEMA_RIM.md`, rim layer)
- generated prose/images/snapshots (`SCHEMA_DISC.md`, disc layer)
- canonical star/system/planet inventory (`SCHEMA_CORE.md`)

Layer rule:

- `arm` can contain source-native rows. That does not make those rows canonical
  inventory; it makes them auditable evidence for Spacegate's canonical model.
- `arm` can contain deterministic derivatives. Those rows must retain input
  lineage, method/version, and confidence.
- promoted canonical summaries may be copied or projected into `core`, but the
  source evidence and competing/alternate solutions should remain in `arm`.

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
- source-native stellar payload: `stellar_parameters`

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
- `component_type TEXT` (`system|star|planet|subplanet|moon|minor_body|artificial|region|brown_dwarf|compact|cluster_member|unresolved_component`)
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
- containment governance:
  - `edge_kind='contains'` must remain acyclic
  - each child may have one canonical containment parent for navigation; additional links must use non-containment edge types

Source-object boundary:
- `core.source_object_reconciliation` may remove duplicate source surrogates
  before root-system grouping, but ARM remains responsible for relationship
  evidence, hierarchy edges, orbital edges, alternate solutions, and diagnostics
  over the surviving accepted source objects.
- Unmatched or quarantined source-object reconciliation candidates must not be
  promoted into ARM hierarchy membership unless later accepted by a reviewed
  source/override path.
- Alias authority may resolve a public search term to an accepted system/member
  focus, but aliases are not relationship evidence. ARM membership and orbit
  edges must continue to come from source-native hierarchy/orbit evidence,
  accepted reconciliation outputs, or reviewed overrides, never name similarity
  alone.

## `orbit_edges`

Dynamic relationships used for orbital reconstruction.

Columns:
- `orbit_edge_id BIGINT`
- `host_component_key TEXT` (often subsystem or barycenter-hosting group)
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `relation_kind TEXT` (`binary|circumbinary|hierarchical_pair|bound_companion|planetary_orbit|satellite|orbits|artificial_orbit|co_orbit`)
- `barycenter_key TEXT` (nullable)
- `preferred_solution_id BIGINT` (nullable FK to `orbital_solutions`)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `evidence_catalogs_json TEXT`
- `evidence_ids_json TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Notes:
- Gaia NSS unresolved binaries may emit synthetic companion component keys so source-native orbital evidence can be narrated without fabricating canonical core stars.
- Planet rows emit `planetary_orbit` edges from the host system/star component
  to the planet component when a core planet has a resolved system binding.
- `hierarchical_pair` edges may connect subsystem/group component keys rather
  than physical leaf stars. Renderers must preserve that distinction, for
  example by drawing cluster orbit guides instead of reclassifying a group edge
  as a direct binary star orbit.
- Runtime render contracts may expose subsystem nodes as inspectable UI bodies
  with descendant render keys and derived child counts. Those presentation
  handles must remain backed by `component_entities`/`system_hierarchy_edges`
  and must not be treated as additional physical stars or orbital solutions.

## `orbital_solutions`

Catalog-normalized orbital element records.

Columns:
- `orbital_solution_id BIGINT`
- `orbit_edge_id BIGINT`
- `solution_source_catalog TEXT` (`nasa_exoplanet_archive|sol_authority|gaia_nss|orb6|msc|wds|...`)
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
- planet, moon, binary, and artificial-object orbital solutions all follow the
  same policy: a promoted scalar may appear in `core` for hot-path display, but
  source-native solution rows, alternates, epochs, fit-quality fields, and
  simulation-ready element sets belong here.
- NASA Exoplanet Archive `pscomppars` values remain the rank-1 promoted
  default for confirmed exoplanet orbital rows. NASA `ps` literature rows are
  materialized as rank-2+ alternate `source_native_planet_orbit` candidates on
  the same planet orbit edge when the `ps` cooked artifact is present.
- Sol authority planet orbital values are materialized as
  `normalization_method='source_native_planet_orbit'`.
- ORB6 solutions may be attached only when the source row can be mapped safely to a unique binary edge for a WDS-linked system; otherwise keep the source-native row outside generic orbital reconstruction flows.
- illustrative orbit defaults for rendering belong in `disc` assumptions until
  they are backed by reviewed source or derived `arm` rows.

## `stellar_parameters`

Narration-oriented, source-native stellar-parameter rows keyed to core stars.

Columns:
- `stellar_parameter_id BIGINT`
- `star_id BIGINT`
- `system_id BIGINT`
- `stable_object_key TEXT`
- `parameter_source TEXT` (`gaia_dr3_backbone|nasa_pscomppars_host|...`)
- `teff_k DOUBLE` with `teff_lo_k DOUBLE`, `teff_hi_k DOUBLE`
- `logg_cgs DOUBLE` with `logg_lo_cgs DOUBLE`, `logg_hi_cgs DOUBLE`
- `metallicity_feh DOUBLE` with `metallicity_lo_feh DOUBLE`, `metallicity_hi_feh DOUBLE`
- `distance_pc DOUBLE` with `distance_lo_pc DOUBLE`, `distance_hi_pc DOUBLE`
- `radius_rsun DOUBLE` with `radius_err_plus_rsun DOUBLE`, `radius_err_minus_rsun DOUBLE`
- `mass_msun DOUBLE` with `mass_err_plus_msun DOUBLE`, `mass_err_minus_msun DOUBLE`
- `luminosity_log10_lsun DOUBLE` with `luminosity_err_plus_log10_lsun DOUBLE`, `luminosity_err_minus_log10_lsun DOUBLE`
- `density_g_cm3 DOUBLE` with `density_err_plus_g_cm3 DOUBLE`, `density_err_minus_g_cm3 DOUBLE`
- `age_gyr DOUBLE` with `age_err_plus_gyr DOUBLE`, `age_err_minus_gyr DOUBLE`
- `rotation_period_days DOUBLE`
- `radial_velocity_kms DOUBLE` with `radial_velocity_error_kms DOUBLE`
- Gaia photometry/color: `phot_g_mag`, `phot_bp_mag`, `phot_rp_mag`, `bp_rp`, `bp_g`, `g_rp`
- Gaia quality/fit context: `ra_error_mas`, `dec_error_mas`, `pm_ra_error_mas_yr`, `pm_dec_error_mas_yr`, `visibility_periods_used`, `astrometric_params_solved`, `non_single_star`, `duplicated_source`, `has_xp_continuous`, `has_xp_sampled`, `has_rvs`
- `spectral_type_raw TEXT`
- classifier support: `classprob_star`, `classprob_binarystar`, `classprob_galaxy`, `classprob_quasar`, `classprob_whitedwarf_combmod`, `classprob_whitedwarf_specmod`
- `context_json TEXT` (source-specific auxiliary context such as WD specialist payload or NASA host multiplicity counts)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Rules:
- keep source-native values and uncertainty bounds; do not collapse them into inferred prose fields here
- one star may legitimately have multiple rows from different source catalogs

## `derived_physical_parameters`

Deterministic, provenance-bound numeric science candidates used when source
catalogs do not provide a needed physical value. These rows are temporary in the
scientific sense: they are acceptable explicit derivations, but they should be
replaced or superseded when a stronger source measurement is found.

Implementation status: emitted by `scripts/build_arm.py` as
`derived_physical_parameters_v1` for deterministic source-input candidates.

Columns:
- `derived_parameter_id BIGINT`
- `build_id TEXT`
- object binding:
  - `object_type TEXT` (`system|star|planet|component|orbit`)
  - `system_id BIGINT` (nullable)
  - `star_id BIGINT` (nullable)
  - `planet_id BIGINT` (nullable)
  - `stable_object_key TEXT` (nullable)
  - `stable_component_key TEXT` (nullable)
- parameter:
  - `parameter_key TEXT` (for example `teff_k`, `luminosity_lsun`,
    `mass_msun`, `semi_major_axis_au`, `insol_earth`)
  - `value DOUBLE`
  - `unit TEXT`
  - uncertainty/range: `value_lo DOUBLE`, `value_hi DOUBLE`, `sigma DOUBLE`
- derivation:
  - `derivation_method TEXT` (for example `spectral_type_proxy`,
    `stefan_boltzmann_from_radius_teff`, `kepler_from_period_host_mass`)
  - `derivation_version TEXT`
  - `input_parameters_json TEXT`
  - `assumptions_json TEXT`
  - `lossy_transform BOOLEAN`
  - `superseded_by_source BOOLEAN`
  - `replacement_priority TEXT` (`low|normal|high|critical`)
- quality:
  - `confidence_score DOUBLE`
  - `confidence_tier TEXT` (`high|medium|low|illustrative`)
  - `review_status TEXT` (`candidate|accepted|superseded|rejected`)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`,
  `transform_version`)

Rules:
- source-native measurements always win over derived values for science claims.
- spectral-type proxy rows are allowed only when the source spectral evidence is
  preserved and the confidence tier is no higher than `low`.
- v1 intentionally does not persist spectral-type proxy rows; those remain
  runtime diagnostics until the stricter source-input path is validated.
- Mass-only visual stellar classes such as the simulator
  `mass_main_sequence_prior_v1` are presentation/render priors, not ARM
  classifications. They may guide color/material choices and hierarchy UI
  labels when clearly marked as ASSUMED, but they must not be written into ARM
  spectral-class or derived-physical-parameter tables without a separate
  reviewed science derivation policy.
- derived orbital values such as semi-major axis from period and host mass must
  retain the exact input mass/period basis and method version.
- Astronomy Agency enrichment must treat non-superseded rows in this table as a
  prioritized search target for stronger literature/source values.
- rows with `confidence_tier='illustrative'` may support diagnostics but must
  not be used as canonical science assertions.

## `derived_stellar_classifications`

Deterministic, provenance-bound categorical classification candidates for
display, search, filters, and renderer policy when source spectral class is
missing. These rows do not replace `core.stars.spectral_class` or source
spectral type fields.

Implementation status: emitted by `scripts/build_arm.py` as
`derived_stellar_classification_v1` for core stars and source-native MSC
stellar component endpoints.

Columns:
- `derived_classification_id BIGINT`
- `build_id TEXT`
- object binding:
  - `object_type TEXT` (`star|component`)
  - `system_id BIGINT` (nullable)
  - `star_id BIGINT` (nullable)
  - `stable_object_key TEXT` (nullable)
  - `stable_component_key TEXT` (nullable)
- classification:
  - `classification_key TEXT` (`stellar_display_class` in v1)
  - `classification_value TEXT` (`O|B|A|F|G|K|M|L|T|Y|WR|WD|NS|PULSAR|MAGNETAR|BLACK HOLE`)
  - `classification_status TEXT` (`derived|assumed|missing`)
- derivation:
  - `derivation_method TEXT` (`remnant_guard_v1`,
    `teff_visual_class_prior_v1`, `mass_radius_physical_class_prior_v1`,
    `spectral_type_visual_class_prior_v1`,
    `mass_main_sequence_prior_v1`)
  - `derivation_version TEXT`
  - `input_parameters_json TEXT`
  - `assumptions_json TEXT`
  - `lossy_transform BOOLEAN`
  - `superseded_by_source BOOLEAN`
- quality:
  - `confidence_score DOUBLE`
  - `confidence_tier TEXT` (`medium|low|illustrative|missing`)
  - `review_status TEXT` (`candidate|accepted|superseded|rejected`)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`,
  `transform_version`)

Rules:
- Source spectral type/class remains authoritative for catalog facts and must
  supersede derived display classifications.
- Compact/remnant evidence overrides temperature buckets. A hot white dwarf,
  neutron star, pulsar, magnetar, or black hole must never be displayed as an
  ordinary O/B/A/F/G/K/M star merely because of temperature.
- Mass-only main-sequence rows are `classification_status='assumed'` and
  `confidence_tier='illustrative'`; they are useful for visual/search
  ergonomics, not science claims.
- MSC component classifications are emitted only for component keys that exist
  as source-supported `component_entities` star rows. Unmatched MSC detail
  endpoints remain diagnostics/evidence and must not become rendered or
  classified stars.

## `msc_component_details`

MSC component/context rows for subsystem narration and photometry support.

Columns:
- `msc_component_detail_id BIGINT`
- `system_id BIGINT` (nullable)
- `star_id BIGINT` (nullable)
- `stable_object_key TEXT` (nullable system key)
- `stable_component_key TEXT`
- `wds_id TEXT`
- `component_label TEXT`
- `preferred_name TEXT` (nullable)
- `sep_arcsec DOUBLE` (nullable)
- `spectral_type_raw TEXT` (nullable)
- astrometric/kinematic context: `parallax_mas`, `pm_ra_mas_yr`, `pm_dec_mas_yr`, `radial_velocity_kms`
- photometry: `bmag`, `vmag`, `imag`, `jmag`, `hmag`, `kmag`
- `grade TEXT` (nullable)
- `other_identifiers TEXT` (nullable)
- `subsystem_count BIGINT` (nullable)
- `orbit_count BIGINT` (nullable)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `msc_system_details`

MSC `sys.tsv` subsystem rows for source-native hierarchy and endpoint physical
evidence.

Columns:
- `msc_system_detail_id BIGINT`
- `wds_id TEXT`
- `primary_label TEXT`
- `secondary_label TEXT`
- `parent_label TEXT`
- `parent_component_key TEXT`
- `primary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `secondary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `system_type TEXT`
- `period_value DOUBLE`
- `period_unit TEXT`
- `period_days DOUBLE` (nullable normalized helper)
- `separation_value DOUBLE`
- `separation_unit TEXT`
- `separation_arcsec DOUBLE`
- `separation_mas DOUBLE`
- `position_angle_deg DOUBLE`
- endpoint physical hints: `vmag_primary`, `spectral_type_primary`,
  `vmag_secondary`, `spectral_type_secondary`, `mass_primary_msun`,
  `mass_code_primary`, `mass_secondary_msun`, `mass_code_secondary`
- `comment TEXT`
- `source_line_number BIGINT`
- `raw_row TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Rules:
- source labels are case-sensitive before normalization: `AB` is a subsystem
  label while `Ab` is a leaf label.
- `sys.tsv` endpoint spectral/mass hints attach to the endpoint component or
  subsystem they describe; builders and renderers must not blindly inherit a
  parent spectral class onto all descendants.

## `msc_orbit_details`

MSC `orb.tsv` orbital-element rows for source-native orbit solution
materialization.

Columns:
- `msc_orbit_detail_id BIGINT`
- `wds_id TEXT`
- `system_label TEXT`
- `primary_label TEXT`
- `secondary_label TEXT`
- `host_component_key TEXT`
- `primary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `secondary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `period_value DOUBLE`
- `period_unit TEXT`
- `period_days DOUBLE`
- `periastron_epoch DOUBLE`
- `eccentricity DOUBLE`
- `semi_major_axis_arcsec DOUBLE`
- `node_deg DOUBLE`
- `longitude_periastron_deg DOUBLE`
- `inclination_deg DOUBLE`
- `semi_amplitude_primary_kms DOUBLE`
- `semi_amplitude_secondary_kms DOUBLE`
- `center_of_mass_velocity_kms DOUBLE`
- `node_flag TEXT`
- `note TEXT`
- `source_line_number BIGINT`
- `raw_row TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `wds_component_observations`

WDS summary observations and pair-history context for narrated binaries/multiples.

Columns:
- `wds_component_observation_id BIGINT`
- `system_id BIGINT` (nullable)
- `star_id BIGINT` (nullable)
- `stable_object_key TEXT` (nullable system key)
- `stable_component_key TEXT`
- `wds_id TEXT`
- `discoverer TEXT` (nullable)
- `component_label TEXT`
- observation window: `first_year`, `last_year`, `obs_count`
- position history: `theta_first_deg`, `theta_last_deg`, `rho_first_arcsec`, `rho_last_arcsec`
- `mag_primary DOUBLE`, `mag_secondary DOUBLE` (nullable)
- `spectral_type_raw TEXT` (nullable)
- motion context: `pm_primary_ra`, `pm_primary_dec`, `pm_secondary_ra`, `pm_secondary_dec`
- `dm_designation TEXT` (nullable)
- `note TEXT` (nullable)
- `precise_coordinate TEXT` (nullable)
- `ra_deg DOUBLE`, `dec_deg DOUBLE` (nullable)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

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

## `sol_small_body_objects`

Named Sol-system S3 small-body rows (science layer, default kept out of core hot path).
Rows are source-native Sol authority evidence from JPL Horizons; asteroid/TNO
records must be fetched with Horizons small-body selector commands so ARM does
not materialize ambiguous major-planet or satellite solutions under
small-body component keys.

Columns:
- `sol_small_body_id BIGINT`
- `stable_component_key TEXT`
- `body_name TEXT`
- `body_name_norm TEXT`
- `body_kind TEXT` (`asteroid|tno|comet|unknown`)
- `host_component_key TEXT`
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `parent_name TEXT`
- `parent_name_norm TEXT`
- `orbital_period_days DOUBLE` (nullable)
- `semi_major_axis_au DOUBLE` (nullable)
- `eccentricity DOUBLE` (nullable)
- `inclination_deg DOUBLE` (nullable)
- `epoch_tdb_jd DOUBLE` (nullable)
- `body_mass_kg DOUBLE` (nullable)
- `body_radius_km DOUBLE` (nullable)
- `freshness_window_days INTEGER`
- `staleness_days INTEGER`
- `is_stale BOOLEAN`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`, `source_url`)

## `sol_artificial_objects`

Named Sol-system S4 artificial rows (stations/probes/orbiters; science overlay kept out of core hot path).

Columns:
- `sol_artificial_id BIGINT`
- `stable_component_key TEXT`
- `artifact_name TEXT`
- `artifact_name_norm TEXT`
- `artifact_kind TEXT` (`station|space_telescope|deep_space_probe|planetary_orbiter|artificial`)
- `host_component_key TEXT`
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `parent_name TEXT`
- `parent_name_norm TEXT`
- `center_code TEXT`
- `target_body_name TEXT` (raw Horizons target body label)
- `orbital_period_days DOUBLE` (nullable)
- `semi_major_axis_au DOUBLE` (nullable)
- `eccentricity DOUBLE` (nullable)
- `inclination_deg DOUBLE` (nullable)
- `epoch_tdb_jd DOUBLE` (nullable)
- `artifact_mass_kg DOUBLE` (nullable)
- `artifact_radius_km DOUBLE` (nullable)
- `freshness_window_days INTEGER`
- `staleness_days INTEGER`
- `is_stale BOOLEAN`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`, `source_url`)

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

## Planned Agent-Assisted Adjudication Tables

Agent-derived scientific proposals belong in `arm`, never directly in `core`.

## `adjudication_candidates` (planned)

Machine-assisted identity/hierarchy/host-resolution proposals for review or later deterministic promotion.

Columns:
- `adjudication_candidate_id BIGINT`
- `stable_object_key TEXT` (nullable when the object is not yet canonicalized)
- `object_type TEXT` (`system|star|planet|pair|subsystem`)
- `proposal_kind TEXT` (`identity_merge|hierarchy_fix|planet_host_fix|missing_field_fill|source_conflict`)
- `proposal_status TEXT` (`proposed|accepted|rejected|superseded`)
- `target_ids_json TEXT`
- `candidate_values_json TEXT`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `reasoning_summary TEXT`
- `source_citation_ids_json TEXT`
- `generator_version TEXT`
- `model_id TEXT`
- `prompt_version TEXT`
- provenance fields (`source_*` when applicable, `retrieval_*`, `ingested_at`, `transform_version`)

Rules:
- rows are advisory/proposed science support, not canonical truth
- acceptance into deterministic canonical rules must be explicit and auditable

## `missing_field_proposals` (planned)

Agent-proposed fills for scientific fields absent from canonical sources but supported by cited public evidence.

Columns:
- `missing_field_proposal_id BIGINT`
- `stable_object_key TEXT`
- `object_type TEXT`
- `field_name TEXT`
- `proposed_value_json TEXT`
- `units TEXT` (nullable)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `source_citation_ids_json TEXT`
- `generator_version TEXT`
- `model_id TEXT`
- `prompt_version TEXT`
- `created_at TIMESTAMP`

Rules:
- proposals remain in `arm` until separately accepted by deterministic policy
- no agent-filled value may silently overwrite source-native `core` fields

## Quality Gates (Arm)

Build fails when:
- any edge references missing component keys
- hierarchy cycle detected in `contains/subsystem_of`
- `preferred_solution_id` points to non-existent orbital solution
- confidence/provenance required fields are null
- `animation_readiness` marks `full` while missing required Kepler set

## MSC Policy

MSC is mandatory in default science ingest and arm hierarchy/orbit derivation.
MSC `comp.tsv`, `sys.tsv`, and `orb.tsv` must all be preserved as deterministic
cooked inputs. `sys.tsv` supplies source-native subsystem membership and parent
relationships; `orb.tsv` supplies source-native orbit solutions. ARM builders
must not reconstruct complex hierarchy solely from component counts or suffix
pair inference when source subsystem/orbit rows are available.

MSC endpoint labels named by `sys.tsv` or `orb.tsv` are source-native evidence.
If an endpoint is not an exact source subsystem parent label, Spacegate
materializes a deterministic ARM leaf component for that endpoint. This allows
case-sensitive source distinctions such as leaf `Ab` versus subsystem `AB`
without duplicating exact subsystem labels such as `Aab`. Legacy count-expanded
leaf labels are reserved for systems without usable source-native endpoint rows.
Endpoint component type follows source evidence where practical; a very low-mass
endpoint below the hydrogen-burning boundary with no stellar spectral evidence
is preserved as a substellar support component, not counted as a stellar leaf.

Canonical hierarchy emission consumes both source-native nested MSC subsystem
edges and MSC inferred leaf components. When ARM preserves a nested source tree
rather than direct root-to-leaf edges, canonical hierarchy must still expose
auditable descendant leaves for benchmark systems where MSC/WDS evidence
supports them. These leaves remain ARM/canonical-hierarchy support evidence;
they are not silently promoted into flat `core.stars` rows.

If MSC retrieval/cook fails:
- ingest must fail
- promotion must not proceed
