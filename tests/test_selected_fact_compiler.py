from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_facts as compiler  # noqa: E402
import audit_selected_fact_artifact as artifact_audit  # noqa: E402
import verify_selected_fact_reproduction as reproduction  # noqa: E402


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def make_identity_and_core(state: Path) -> tuple[str, str]:
    identity_id = "identity-test"
    identity_dir = state / "derived/evidence_lake_v2/identity" / identity_id
    identity_dir.mkdir(parents=True)
    con = duckdb.connect(str(identity_dir / "identity_graph.duckdb"))
    con.execute(
        """
        CREATE TABLE canonical_identifier_bindings (
          namespace VARCHAR, id_value_norm VARCHAR, object_node_key VARCHAR,
          stable_object_key VARCHAR, system_stable_object_key VARCHAR
        );
        CREATE TABLE canonical_object_nodes (
          object_node_key VARCHAR, stable_object_key VARCHAR,
          system_stable_object_key VARCHAR, object_type VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('gaia_dr3', '123', 'star-node', 'star-key', 'system-key'),
          ('gaia_dr3', '124', 'model-node', 'model-key', 'model-system-key'),
          ('gaia_dr3', '888', 'ambiguous-node-a', 'ambiguous-key-a', 'system-key-a'),
          ('gaia_dr3', '888', 'ambiguous-node-b', 'ambiguous-key-b', 'system-key-b');
        INSERT INTO canonical_object_nodes VALUES
          ('star-node', 'star-key', 'system-key', 'star'),
          ('model-node', 'model-key', 'model-system-key', 'star'),
          ('ambiguous-node-a', 'ambiguous-key-a', 'system-key-a', 'star'),
          ('ambiguous-node-b', 'ambiguous-key-b', 'system-key-b', 'star'),
          ('planet-node', 'planet-key', 'system-key', 'planet');
        """
    )
    con.close()

    core_id = "core-test"
    core_dir = state / "out" / core_id
    core_dir.mkdir(parents=True)
    con = duckdb.connect(str(core_dir / "core.duckdb"))
    con.execute(
        """
        CREATE TABLE planets (
          planet_name_norm VARCHAR, stable_object_key VARCHAR, system_id BIGINT
        );
        INSERT INTO planets VALUES ('test 1 b', 'planet-key', 1);
        """
    )
    con.close()
    return identity_id, core_id


def make_e4_artifact(
    state: Path,
    *,
    build_id: str,
    source_id: str,
    release_id: str,
    object_type: str,
) -> dict[str, object]:
    root = state / "derived/evidence_lake_v2/scientific_evidence" / build_id
    root.mkdir(parents=True)
    database = root / "scientific_evidence.duckdb"
    con = duckdb.connect(str(database))
    con.execute(
        """
        CREATE TABLE source_records (
          source_record_id VARCHAR, source_table VARCHAR, source_context_json JSON
        );
        CREATE TABLE identifier_claim_evidence (
          source_record_id VARCHAR, namespace VARCHAR, identifier_normalized VARCHAR,
          claim_scope VARCHAR, component_scope VARCHAR
        );
        """
    )
    if object_type == "classification":
        con.execute(
            """
            CREATE TABLE stellar_classification_evidence (
              evidence_id VARCHAR, source_record_id VARCHAR,
              component_scope VARCHAR, classification_scheme VARCHAR,
              classification_raw VARCHAR, classification_normalized VARCHAR,
              probability DOUBLE, method VARCHAR, model VARCHAR,
              reference_raw VARCHAR, quality_json JSON
            );
            INSERT INTO source_records VALUES
              ('class-record', 'classification_table', '{"astrom_Gaia":"P"}'),
              ('class-record-ir', 'classification_table', '{"astrom_Gaia":"O"}'),
              ('class-missing', 'classification_table', '{}'),
              ('class-scoped', 'classification_table', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('class-record', 'gaia_dr3_source_id', 'Gaia DR3 123',
               'star_or_substellar_object', NULL),
              ('class-record-ir', 'gaia_dr3_source_id', 'Gaia DR3 123',
               'star_or_substellar_object', NULL),
              ('class-missing', 'gaia_dr3_source_id', 'Gaia DR3 999',
               'star_or_substellar_object', NULL),
              ('class-scoped', 'gaia_dr3_source_id', 'Gaia DR3 123',
               'star_or_substellar_object', NULL);
            INSERT INTO stellar_classification_evidence VALUES
              ('class-opt', 'class-record', NULL, 'optical_spectral_type',
               'M8', NULL, NULL, 'compiled-optical', NULL, 'opt-ref', '{}'),
              ('class-ir', 'class-record-ir', NULL, 'infrared_spectral_type',
               'L0', NULL, NULL, 'compiled-infrared', NULL, 'ir-ref', '{}'),
              ('class-missing-opt', 'class-missing', NULL, 'optical_spectral_type',
               'T5', NULL, NULL, 'compiled-optical', NULL, 'missing-ref', '{}'),
              ('class-scoped-opt', 'class-scoped', 'primary', 'optical_spectral_type',
               'G2V', NULL, NULL, 'compiled-optical', NULL, 'scoped-ref', '{}');
            """
        )
    elif object_type == "model":
        con.execute(
            """
            CREATE TABLE compact_object_evidence (
              evidence_id VARCHAR, source_record_id VARCHAR, compact_kind VARCHAR,
              parameter_set_raw JSON, quality_json JSON
            );
            CREATE TABLE stellar_parameter_sets (
              parameter_set_id VARCHAR, source_record_id VARCHAR,
              component_scope VARCHAR, method VARCHAR, model VARCHAR,
              reference_raw VARCHAR, quality_json JSON
            );
            CREATE TABLE stellar_parameter_evidence (
              evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
              quantity_key VARCHAR, value_raw VARCHAR, normalized_value DOUBLE,
              normalized_unit VARCHAR, uncertainty_lower DOUBLE,
              uncertainty_upper DOUBLE, bound_semantics VARCHAR,
              reference_raw VARCHAR, normalization_version VARCHAR, quality_json JSON
            );
            INSERT INTO source_records VALUES
              ('model-record', 'model_table', '{}'),
              ('model-low-probability', 'model_table', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('model-record', 'gaia_edr3_source_id', 'Gaia EDR3 124', 'star', NULL),
              ('model-low-probability', 'gaia_edr3_source_id', 'Gaia EDR3 124', 'star', NULL);
            INSERT INTO compact_object_evidence VALUES
              ('context-good', 'model-record', 'white_dwarf_candidate',
               '{"Pwd":0.99}', '{"fidelity":1.0}'),
              ('context-low', 'model-low-probability', 'white_dwarf_candidate',
               '{"Pwd":0.5}', '{"fidelity":1.0}');
            INSERT INTO stellar_parameter_sets VALUES
              ('model-h', 'model-record', NULL, 'photometric-model', 'hydrogen', 'model-ref', '{}'),
              ('model-he', 'model-record', NULL, 'photometric-model', 'helium', 'model-ref', '{}'),
              ('model-mixed', 'model-record', NULL, 'photometric-model', 'mixed', 'model-ref', '{}'),
              ('model-low', 'model-low-probability', NULL, 'photometric-model', 'hydrogen', 'model-ref', '{}');
            INSERT INTO stellar_parameter_evidence VALUES
              ('h-teff', 'model-h', 'model-record', 'effective_temperature', '10000', 10000, 'K', 100, 100, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('h-logg', 'model-h', 'model-record', 'log10_surface_gravity', '8.0', 8.0, 'dex', 0.1, 0.1, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('h-mass', 'model-h', 'model-record', 'mass', '0.6', 0.6, 'solMass', 0.05, 0.05, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('h-chi', 'model-h', 'model-record', 'fit_chi_square', '2.0', 2.0, NULL, NULL, NULL, NULL, 'model-ref', 'model-v1', '{}'),
              ('he-teff', 'model-he', 'model-record', 'effective_temperature', '11000', 11000, 'K', 100, 100, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('he-logg', 'model-he', 'model-record', 'log10_surface_gravity', '8.1', 8.1, 'dex', 0.1, 0.1, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('he-mass', 'model-he', 'model-record', 'mass', '0.7', 0.7, 'solMass', 0.05, 0.05, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('he-chi', 'model-he', 'model-record', 'fit_chi_square', '1.0', 1.0, NULL, NULL, NULL, NULL, 'model-ref', 'model-v1', '{}'),
              ('mixed-teff', 'model-mixed', 'model-record', 'effective_temperature', '10500', 10500, 'K', 100, 100, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('mixed-logg', 'model-mixed', 'model-record', 'log10_surface_gravity', '8.05', 8.05, 'dex', 0.1, 0.1, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('mixed-chi', 'model-mixed', 'model-record', 'fit_chi_square', '0.5', 0.5, NULL, NULL, NULL, NULL, 'model-ref', 'model-v1', '{}'),
              ('low-teff', 'model-low', 'model-low-probability', 'effective_temperature', '9000', 9000, 'K', 100, 100, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('low-logg', 'model-low', 'model-low-probability', 'log10_surface_gravity', '8.0', 8.0, 'dex', 0.1, 0.1, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('low-mass', 'model-low', 'model-low-probability', 'mass', '0.5', 0.5, 'solMass', 0.05, 0.05, 'symmetric_error', 'model-ref', 'model-v1', '{}'),
              ('low-chi', 'model-low', 'model-low-probability', 'fit_chi_square', '1.0', 1.0, NULL, NULL, NULL, NULL, 'model-ref', 'model-v1', '{}');
            """
        )
    elif object_type == "coherent":
        schema = json.dumps(
            {
                "fields": [
                    {"name": "parallax", "position": 0, "datatype": "double", "unit": "mas"},
                    {"name": "parallax_error", "position": 1, "datatype": "float", "unit": "mas"},
                    {"name": "variable_flag", "position": 2, "datatype": "char", "unit": ""},
                ]
            },
            separators=(",", ":"),
        )
        con.execute(
            """
            CREATE TABLE coherent_parameter_set_schemas (schema_json JSON);
            CREATE TABLE stellar_source_parameter_sets (
              evidence_id VARCHAR, source_record_id VARCHAR, component_scope VARCHAR,
              parameter_set_kind VARCHAR,
              values_json JSON, method VARCHAR, model VARCHAR, reference_raw VARCHAR,
              normalization_version VARCHAR, quality_json JSON
            );
            INSERT INTO source_records VALUES ('coherent-record', 'gaia_source', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('coherent-record', 'gaia_dr3_source_id', 'Gaia DR3 123', 'star', NULL);
            INSERT INTO stellar_source_parameter_sets VALUES
              ('coherent-set', 'coherent-record', NULL, 'astrometry', '[20.0,0.5,"VARIABLE"]',
               'coherent-method', NULL, 'coherent-ref', 'norm-v1', '{}');
            """
        )
        con.execute(
            "INSERT INTO coherent_parameter_set_schemas VALUES (?::JSON)",
            [schema],
        )
    elif object_type == "bundle":
        con.execute(
            """
            CREATE TABLE astrometry_distance_evidence_bundles (
              bundle_id VARCHAR,
              source_record_id VARCHAR,
              bundle_semantics VARCHAR,
              measurements STRUCT(
                evidence_id VARCHAR, quantity_key VARCHAR, value_raw VARCHAR,
                unit_raw VARCHAR, normalized_value DOUBLE, normalized_unit VARCHAR,
                uncertainty_lower DOUBLE, uncertainty_upper DOUBLE,
                bound_semantics VARCHAR, frame_raw VARCHAR, epoch_raw VARCHAR,
                method VARCHAR, model VARCHAR, reference_raw VARCHAR,
                quality_json JSON, normalization_version VARCHAR
              )[]
            );
            INSERT INTO source_records VALUES
              ('distance-record', 'distance_bundle', '{}'),
              ('distance-missing', 'distance_bundle', '{}'),
              ('distance-ambiguous', 'distance_bundle', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('distance-record', 'gaia_edr3_source_id', 'Gaia EDR3 123', 'star', NULL),
              ('distance-missing', 'gaia_edr3_source_id', 'Gaia EDR3 999', 'star', NULL),
              ('distance-ambiguous', 'gaia_edr3_source_id', 'Gaia EDR3 888', 'star', NULL);
            INSERT INTO astrometry_distance_evidence_bundles VALUES (
              'distance-bundle', 'distance-record',
              'storage_group_only_no_parameter_coherence',
              [
                struct_pack(
                  evidence_id := 'distance-geo',
                  quantity_key := 'geometric_distance_posterior_median',
                  value_raw := '50.0', unit_raw := 'pc',
                  normalized_value := 50.0, normalized_unit := 'pc',
                  uncertainty_lower := 45.0, uncertainty_upper := 57.0,
                  bound_semantics := 'posterior_16th_84th_percentile_interval_endpoints',
                  frame_raw := NULL, epoch_raw := NULL,
                  method := 'bayesian_parallax_distance_estimate',
                  model := 'direction_dependent_geometric_galaxy_prior',
                  reference_raw := '2021AJ....161..147B',
                  quality_json := '{}'::JSON,
                  normalization_version := 'distance-geo-v1'
                ),
                struct_pack(
                  evidence_id := 'distance-photogeo',
                  quantity_key := 'photogeometric_distance_posterior_median',
                  value_raw := '49.0', unit_raw := 'pc',
                  normalized_value := 49.0, normalized_unit := 'pc',
                  uncertainty_lower := 46.0, uncertainty_upper := 53.0,
                  bound_semantics := 'posterior_16th_84th_percentile_interval_endpoints',
                  frame_raw := NULL, epoch_raw := NULL,
                  method := 'bayesian_parallax_photometry_distance_estimate',
                  model := 'direction_color_magnitude_extinction_selection_prior',
                  reference_raw := '2021AJ....161..147B',
                  quality_json := '{}'::JSON,
                  normalization_version := 'distance-photogeo-v1'
                )
              ]
            ), (
              'distance-missing-bundle', 'distance-missing',
              'storage_group_only_no_parameter_coherence',
              [
                struct_pack(
                  evidence_id := 'distance-missing-geo',
                  quantity_key := 'geometric_distance_posterior_median',
                  value_raw := '400.0', unit_raw := 'pc',
                  normalized_value := 400.0, normalized_unit := 'pc',
                  uncertainty_lower := 300.0, uncertainty_upper := 500.0,
                  bound_semantics := 'posterior_16th_84th_percentile_interval_endpoints',
                  frame_raw := NULL, epoch_raw := NULL,
                  method := 'bayesian_parallax_distance_estimate',
                  model := 'direction_dependent_geometric_galaxy_prior',
                  reference_raw := '2021AJ....161..147B',
                  quality_json := '{}'::JSON,
                  normalization_version := 'distance-geo-v1'
                )
              ]
            ), (
              'distance-ambiguous-bundle', 'distance-ambiguous',
              'storage_group_only_no_parameter_coherence',
              [
                struct_pack(
                  evidence_id := 'distance-ambiguous-geo',
                  quantity_key := 'geometric_distance_posterior_median',
                  value_raw := '60.0', unit_raw := 'pc',
                  normalized_value := 60.0, normalized_unit := 'pc',
                  uncertainty_lower := 55.0, uncertainty_upper := 65.0,
                  bound_semantics := 'posterior_16th_84th_percentile_interval_endpoints',
                  frame_raw := NULL, epoch_raw := NULL,
                  method := 'bayesian_parallax_distance_estimate',
                  model := 'direction_dependent_geometric_galaxy_prior',
                  reference_raw := '2021AJ....161..147B',
                  quality_json := '{}'::JSON,
                  normalization_version := 'distance-geo-v1'
                )
              ]
            );
            """
        )
    elif object_type == "star":
        con.execute(
            """
            CREATE TABLE stellar_parameter_sets (
              parameter_set_id VARCHAR, source_record_id VARCHAR,
              component_scope VARCHAR, method VARCHAR, model VARCHAR,
              reference_raw VARCHAR
            );
            CREATE TABLE stellar_parameter_evidence (
              evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
              quantity_key VARCHAR, value_raw VARCHAR, normalized_value DOUBLE,
              normalized_unit VARCHAR, uncertainty_lower DOUBLE,
              uncertainty_upper DOUBLE, bound_semantics VARCHAR,
              reference_raw VARCHAR, normalization_version VARCHAR, quality_json JSON
            );
            CREATE TABLE astrometry_distance_evidence (
              evidence_id VARCHAR, source_record_id VARCHAR, quantity_key VARCHAR,
              value_raw VARCHAR, unit_raw VARCHAR, normalized_value DOUBLE,
              normalized_unit VARCHAR, uncertainty_lower DOUBLE,
              uncertainty_upper DOUBLE, bound_semantics VARCHAR, frame_raw VARCHAR,
              epoch_raw VARCHAR, method VARCHAR, model VARCHAR, reference_raw VARCHAR,
              quality_json JSON, normalization_version VARCHAR
            );
            INSERT INTO source_records VALUES
              ('gaia-record', 'gaia_ap', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('gaia-record', 'gaia_dr3_source_id', 'Gaia DR3 123', 'star', NULL);
            INSERT INTO stellar_parameter_sets VALUES
              ('hot-set', 'gaia-record', NULL, 'hot-method', NULL, 'hot-ref'),
              ('hot-low-set', 'gaia-record', NULL, 'hot-method', NULL, 'hot-low-ref'),
              ('hot-bad-set', 'gaia-record', NULL, 'hot-method', NULL, 'hot-bad-ref'),
              ('fallback-set', 'gaia-record', NULL, 'fallback-method', NULL, 'fallback-ref'),
              ('fundamental-set', 'gaia-record', NULL, 'fundamental-method', NULL, 'fund-ref');
            INSERT INTO stellar_parameter_evidence VALUES
              ('teff-hot', 'hot-set', 'gaia-record', 'effective_temperature', '5800', 5800, 'K', 5700, 5900, 'interval_endpoints', 'hot-ref', 'norm-v1', '{"flag":0,"snr":50}'),
              ('teff-hot-low', 'hot-low-set', 'gaia-record', 'effective_temperature', '5750', 5750, 'K', 5650, 5850, 'interval_endpoints', 'hot-low-ref', 'norm-v1', '{"flag":0,"snr":10}'),
              ('teff-hot-bad', 'hot-bad-set', 'gaia-record', 'effective_temperature', '5900', 5900, 'K', 5800, 6000, 'interval_endpoints', 'hot-bad-ref', 'norm-v1', '{"flag":1,"snr":100}'),
              ('teff-fallback', 'fallback-set', 'gaia-record', 'effective_temperature', '5700', 5700, 'K', 100, 100, 'symmetric_error', 'fallback-ref', 'norm-v1', '{}'),
              ('radius-fund', 'fundamental-set', 'gaia-record', 'stellar_radius', '1.0', 1.0, 'solRad', 0.1, 0.1, 'symmetric_error', 'fund-ref', 'norm-v1', '{}');
            INSERT INTO astrometry_distance_evidence VALUES
              ('gspphot-distance', 'gaia-record', 'gspphot_model_distance',
               '101', 'pc', 101, 'pc', 95, 110,
               'posterior_16th_84th_percentile_interval_endpoints', NULL, NULL,
               'gaia_dr3_gspphot', 'Aeneas_MCMC_BP_RP_spectrophotometry',
               'gaia-ap-ref', '{}', 'gspphot-v1');
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE planet_parameter_sets (
              parameter_set_id VARCHAR, source_record_id VARCHAR,
              parameter_set_kind VARCHAR, method VARCHAR, model VARCHAR,
              reference_raw VARCHAR
            );
            CREATE TABLE planet_parameter_evidence (
              evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
              quantity_key VARCHAR, value_raw VARCHAR, normalized_value DOUBLE,
              normalized_unit VARCHAR, uncertainty_lower DOUBLE,
              uncertainty_upper DOUBLE, bound_semantics VARCHAR,
              reference_raw VARCHAR, normalization_version VARCHAR, quality_json JSON
            );
            INSERT INTO source_records VALUES
              ('planet-default', 'nasa_default', '{"default_flag":"1"}'),
              ('planet-composite', 'nasa_composite', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('planet-default', 'nasa_planet_name', 'Test-1 b', 'planet', NULL),
              ('planet-composite', 'nasa_planet_name', 'Test-1 b', 'planet', NULL);
            INSERT INTO planet_parameter_sets VALUES
              ('planet-default-set', 'planet-default', 'reference', NULL, NULL, 'default-ref'),
              ('planet-composite-set', 'planet-composite', 'composite', NULL, NULL, 'composite-ref');
            INSERT INTO planet_parameter_evidence VALUES
              ('period-default', 'planet-default-set', 'planet-default', 'period', '10', 10, 'd', 0.2, 0.3, 'asymmetric_error', 'default-ref', 'norm-v1', '{}'),
              ('period-composite', 'planet-composite-set', 'planet-composite', 'period', '11', 11, 'd', NULL, NULL, NULL, 'composite-ref', 'norm-v1', '{}');
            """
        )
    con.close()
    database_sha = compiler.file_sha256(database)
    manifest = root / "manifest.json"
    write_json(manifest, {"build_id": build_id})
    return {
        "artifact_path": str(root.relative_to(state)),
        "build_id": build_id,
        "database": database.name,
        "database_bytes": database.stat().st_size,
        "database_sha256": database_sha,
        "manifest_sha256": compiler.file_sha256(manifest),
        "release_ids": {source_id: release_id},
        "source_ids": [source_id],
    }


def fixture_policy(state: Path, tmp_path: Path) -> Path:
    identity_id, core_id = make_identity_and_core(state)
    gaia = make_e4_artifact(
        state,
        build_id="gaia-test",
        source_id="source.gaia",
        release_id="gaia-release",
        object_type="star",
    )
    nasa = make_e4_artifact(
        state,
        build_id="nasa-test",
        source_id="source.nasa",
        release_id="nasa-release",
        object_type="planet",
    )
    coherent = make_e4_artifact(
        state,
        build_id="coherent-test",
        source_id="source.coherent",
        release_id="coherent-release",
        object_type="coherent",
    )
    distance = make_e4_artifact(
        state,
        build_id="distance-test",
        source_id="source.distance",
        release_id="distance-release",
        object_type="bundle",
    )
    classification = make_e4_artifact(
        state,
        build_id="classification-test",
        source_id="source.classification",
        release_id="classification-release",
        object_type="classification",
    )
    model = make_e4_artifact(
        state,
        build_id="model-test",
        source_id="source.model",
        release_id="model-release",
        object_type="model",
    )
    release_id = "release-set-test"
    release_sha = "a" * 64
    write_json(
        state / "derived/evidence_lake_v2/scientific_evidence_sets" / release_id / "manifest.json",
        {
            "schema_version": "spacegate.scientific_evidence_release_set.v1",
            "release_set_id": release_id,
            "release_set_sha256": release_sha,
            "status": "pass",
            "members": [gaia, nasa, coherent, distance, classification, model],
        },
    )
    policy = tmp_path / "policy.json"
    write_json(
        policy,
        {
            "schema_version": "spacegate.selected_fact_policy.v1",
            "policy_version": "test-policy-v1",
            "compiler_version": "test-compiler-v1",
            "evidence_release_set_id": release_id,
            "evidence_release_set_sha256": release_sha,
            "identity_graph_id": identity_id,
            "canonical_reference_build_id": core_id,
            "selection_sources": [
                {
                    "source_id": "source.model",
                    "object_type": "star",
                    "binding_scope": "star",
                    "binding": {
                        "strategy": "authoritative_release_equivalence",
                        "claim_namespace": "gaia_edr3_source_id",
                        "canonical_namespace": "gaia_dr3",
                        "normalization": "unsigned_decimal",
                        "release_equivalence": {
                            "source_release": "gaia_edr3",
                            "canonical_release": "gaia_dr3",
                            "relationship": "identical_source_list_and_astrometry_carried_forward",
                            "source_list_identical": True,
                            "authority_url": "https://gea.esac.esa.int/archive/documentation/GDR3/",
                            "authority_statement": "fixture authoritative release relationship",
                        },
                    },
                    "allowed_claim_scopes": ["star"],
                    "parameter_set_table": "stellar_parameter_sets",
                    "parameter_evidence_table": "stellar_parameter_evidence",
                    "component_scope_field": "component_scope",
                    "applicability_context": {
                        "table": "compact_object_evidence",
                        "filters": {"compact_kind": "white_dwarf_candidate"},
                        "conditions": [
                            {
                                "scope": "applicability_parameters",
                                "path": "$.Pwd",
                                "operator": "gt",
                                "value": 0.75,
                            }
                        ],
                        "reason": "high-confidence model candidate",
                    },
                    "parameter_set_preselection": {
                        "selection_key": "atmosphere_model",
                        "required_quantities": [
                            "effective_temperature",
                            "log10_surface_gravity",
                            "mass",
                        ],
                        "minimum_required_quantities": 3,
                        "order_quantity": "fit_chi_square",
                        "direction": "asc",
                        "minimum_selected_parameter_sets": 1,
                        "expected_selected_parameter_sets": 1,
                        "reason": "complete model with lowest published fit chi-square",
                    },
                    "expected_binding_outcomes": {"accepted": 1, "excluded": 1},
                    "expected_selected_facts": 3,
                    "quantity_groups": [
                        {
                            "group_key": "atmosphere",
                            "quantities": {
                                "effective_temperature": "teff_k",
                                "log10_surface_gravity": "logg_cgs",
                            },
                            "authorities": [
                                {"rank": 5, "method": "photometric-model", "reason": "specialized model"}
                            ],
                        },
                        {
                            "group_key": "fundamental",
                            "quantities": {"mass": "mass_msun"},
                            "authorities": [
                                {"rank": 5, "method": "photometric-model", "reason": "specialized model"}
                            ],
                        },
                    ],
                },
                {
                    "source_id": "source.classification",
                    "object_type": "star",
                    "binding_scope": "star_or_substellar_object",
                    "binding": {
                        "strategy": "canonical_identifier",
                        "claim_namespace": "gaia_dr3_source_id",
                        "canonical_namespace": "gaia_dr3",
                        "normalization": "unsigned_decimal",
                    },
                    "allowed_claim_scopes": ["star_or_substellar_object"],
                    "component_scope_policy": "require_null",
                    "storage": "classification",
                    "classification_evidence_table": "stellar_classification_evidence",
                    "expected_binding_outcomes": {
                        "accepted": 2,
                        "missing": 1,
                        "unresolved": 1,
                    },
                    "expected_selected_facts": 2,
                    "quantity_groups": [
                        {
                            "group_key": "optical_classification",
                            "quantities": {
                                "optical_spectral_type": {
                                    "quantity_key": "spectral_type_optical",
                                    "numeric": False,
                                }
                            },
                            "authorities": [
                                {"rank": 10, "method": "compiled-optical", "reason": "direct optical"}
                            ],
                        },
                        {
                            "group_key": "infrared_classification",
                            "quantities": {
                                "infrared_spectral_type": {
                                    "quantity_key": "spectral_type_infrared",
                                    "numeric": False,
                                }
                            },
                            "authorities": [
                                {"rank": 10, "method": "compiled-infrared", "reason": "direct infrared"}
                            ],
                        },
                    ],
                },
                {
                    "source_id": "source.coherent",
                    "object_type": "star",
                    "binding_scope": "star",
                    "binding": {
                        "strategy": "canonical_identifier",
                        "claim_namespace": "gaia_dr3_source_id",
                        "canonical_namespace": "gaia_dr3",
                        "normalization": "unsigned_decimal",
                    },
                    "storage": "coherent_array",
                    "selection_mode": "authoritative_direct",
                    "parameter_set_table": "stellar_source_parameter_sets",
                    "schema_table": "coherent_parameter_set_schemas",
                    "quantity_groups": [
                        {
                            "group_key": "astrometry",
                            "parameter_set_kinds": ["astrometry"],
                            "quantities": {
                                "parallax": {
                                    "quantity_key": "parallax_mas",
                                    "uncertainty_field": "parallax_error",
                                },
                                "variable_flag": {
                                    "quantity_key": "variability_flag",
                                    "numeric": False,
                                },
                            },
                            "authorities": [
                                {"rank": 10, "method": "coherent-method", "reason": "direct"}
                            ],
                        }
                    ],
                },
                {
                    "source_id": "source.distance",
                    "object_type": "star",
                    "binding_scope": "star",
                    "binding": {
                        "strategy": "authoritative_release_equivalence",
                        "claim_namespace": "gaia_edr3_source_id",
                        "canonical_namespace": "gaia_dr3",
                        "normalization": "unsigned_decimal",
                        "release_equivalence": {
                            "source_release": "gaia_edr3",
                            "canonical_release": "gaia_dr3",
                            "relationship": "identical_source_list_and_astrometry_carried_forward",
                            "source_list_identical": True,
                            "authority_url": "https://gea.esac.esa.int/archive/documentation/GDR3/",
                            "authority_statement": "fixture authoritative release relationship",
                        },
                    },
                    "storage": "measurement_bundle",
                    "selection_mode": "authoritative_direct",
                    "parameter_set_table": "astrometry_distance_evidence_bundles",
                    "bundle_table": "astrometry_distance_evidence_bundles",
                    "bundle_id_field": "bundle_id",
                    "measurements_field": "measurements",
                    "quantity_groups": [
                        {
                            "group_key": "geometric_distance",
                            "quantities": {
                                "geometric_distance_posterior_median": {
                                    "quantity_key": "distance_geometric_pc"
                                }
                            },
                            "authorities": [{"rank": 20, "reason": "geometric"}],
                        },
                        {
                            "group_key": "photogeometric_distance",
                            "quantities": {
                                "photogeometric_distance_posterior_median": {
                                    "quantity_key": "distance_photogeometric_pc"
                                }
                            },
                            "authorities": [{"rank": 30, "reason": "photogeometric"}],
                        },
                    ],
                },
                {
                    "source_id": "source.gaia",
                    "object_type": "star",
                    "binding_scope": "star",
                    "binding": {
                        "strategy": "canonical_identifier",
                        "claim_namespace": "gaia_dr3_source_id",
                        "canonical_namespace": "gaia_dr3",
                        "normalization": "unsigned_decimal",
                    },
                    "parameter_set_table": "stellar_parameter_sets",
                    "parameter_evidence_table": "stellar_parameter_evidence",
                    "component_scope_field": "component_scope",
                    "auxiliary_measurement_groups": [
                        {
                            "table": "astrometry_distance_evidence",
                            "group_key": "gspphot_model_distance",
                            "source_quantity": "gspphot_model_distance",
                            "quantity_key": "distance_gspphot_pc",
                            "requirements": [
                                "positive_value",
                                "positive_ordered_interval",
                            ],
                            "authorities": [
                                {
                                    "rank": 40,
                                    "method": "gaia_dr3_gspphot",
                                    "model": "Aeneas_MCMC_BP_RP_spectrophotometry",
                                    "reason": "source model",
                                }
                            ],
                        }
                    ],
                    "quantity_groups": [
                        {
                            "group_key": "atmosphere",
                            "quantities": {"effective_temperature": "teff_k"},
                            "authorities": [
                                {
                                    "rank": 10,
                                    "method": "hot-method",
                                    "reason": "specialized",
                                    "quality_conditions": [
                                        {
                                            "scope": "evidence_quality",
                                            "path": "$.flag",
                                            "operator": "eq",
                                            "value": 0,
                                        }
                                    ],
                                    "quality_order": {
                                        "scope": "evidence_quality",
                                        "path": "$.snr",
                                        "direction": "desc",
                                    },
                                },
                                {"rank": 20, "method": "fallback-method", "reason": "fallback"},
                            ],
                        },
                        {
                            "group_key": "fundamental",
                            "quantities": {"stellar_radius": "radius_rsun"},
                            "authorities": [
                                {"rank": 10, "method": "fundamental-method", "reason": "direct"}
                            ],
                        },
                    ],
                },
                {
                    "source_id": "source.nasa",
                    "object_type": "planet",
                    "binding_scope": "planet",
                    "binding": {
                        "strategy": "canonical_unique_name",
                        "claim_namespace": "nasa_planet_name",
                        "canonical_table": "planets",
                        "canonical_name_field": "planet_name_norm",
                        "normalization": "spacegate_public_name_v1",
                    },
                    "parameter_set_table": "planet_parameter_sets",
                    "parameter_evidence_table": "planet_parameter_evidence",
                    "quantity_groups": [
                        {
                            "group_key": "orbit",
                            "quantities": {"period": "orbital_period_days"},
                            "authorities": [
                                {
                                    "rank": 10,
                                    "source_table": "nasa_default",
                                    "context_field": "default_flag",
                                    "context_value": "1",
                                    "reason": "default",
                                },
                                {"rank": 20, "source_table": "nasa_composite", "reason": "composite"},
                            ],
                        }
                    ],
                },
            ],
            "derivations": [
                {
                    "derivation_key": "stellar_luminosity_stefan_boltzmann",
                    "version": "sb-test-v1",
                    "formula": "L=R^2(T/5772)^4",
                    "applicability": "test",
                    "uncertainty": "endpoint propagation",
                    "supersedes": ["old-path"],
                }
            ],
        },
    )
    write_json(
        tmp_path / "e5_source_dispositions.json",
        {
            "schema_version": "spacegate.e5_source_dispositions.v1",
            "disposition_version": "test-dispositions-v1",
            "explicit_dispositions": {},
        },
    )
    return policy


def test_selected_fact_compiler_selects_coherent_sets_and_lineage(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy = fixture_policy(state, tmp_path)
    report = compiler.compile_selected_facts(
        state_dir=state,
        policy_path=policy,
        temp_directory=tmp_path / "spill",
        threads=1,
        memory_limit="1GB",
    )

    assert report["status"] == "pass"
    assert report["source_disposition_status"] == "pass"
    assert report["source_disposition_blockers"] == []
    assert report["table_counts"]["selected_facts"] == 14
    assert report["binding_outcomes"]["source.distance"] == {
        "accepted": 1,
        "ambiguous": 1,
        "missing": 1,
    }
    assert report["binding_outcomes"]["source.classification"] == {
        "accepted": 2,
        "missing": 1,
        "unresolved": 1,
    }
    assert report["binding_outcomes"]["source.model"] == {
        "accepted": 1,
        "excluded": 1,
    }
    artifact = state / "derived/evidence_lake_v2/selected_facts" / report["build_id"]
    performance_path = Path(report["performance_report"])
    assert performance_path.is_file()
    performance = json.loads(performance_path.read_text())
    assert performance["status"] == "pass"
    assert performance["active_phase"] is None
    assert any(
        row["phase"] == "integrity_check.selected_source_facts_without_accepted_subject_binding"
        for row in performance["phases"]
    )
    assert (artifact / "selected_facts__teff_k.parquet").is_file()
    assert (artifact / "selection_decisions__atmosphere.parquet").is_file()
    assert sum(report["partition_exports"]["selected_facts"].values()) == 14
    assert sum(report["partition_exports"]["selection_decisions"].values()) == 11
    audit = artifact_audit.audit_artifact(artifact, policy)
    assert audit["status"] == "pass"
    assert audit["failing_checks"] == {}
    con = duckdb.connect(str(artifact / "selected_facts.duckdb"), read_only=True)
    facts = con.execute(
        "SELECT quantity_key, normalized_value, value_lower, value_upper, fact_status "
        "FROM selected_facts ORDER BY quantity_key"
    ).fetchall()
    decisions = con.execute(
        "SELECT quantity_group, selected_parameter_set_id, authority_rank, "
        "selection_quality_score, runner_up_parameter_set_id, runner_up_quality_score "
        "FROM parameter_set_selection_decisions ORDER BY quantity_group"
    ).fetchall()
    supersedes = con.execute(
        "SELECT supersedes_json FROM selected_fact_derivations"
    ).fetchone()[0]
    distance_bindings = con.execute(
        "SELECT source_record_id, binding_status, binding_method, binding_reason, "
        "stable_object_key FROM evidence_object_bindings "
        "WHERE source_id='source.distance' ORDER BY source_record_id"
    ).fetchall()
    classification_facts = con.execute(
        "SELECT quantity_key,value_raw FROM selected_facts "
        "WHERE source_id='source.classification' ORDER BY quantity_key"
    ).fetchall()
    classification_bindings = con.execute(
        "SELECT binding_subject_id,component_scope,binding_status,binding_reason "
        "FROM evidence_object_bindings WHERE source_id='source.classification' "
        "ORDER BY binding_subject_id"
    ).fetchall()
    model_preselection = con.execute(
        "SELECT selected_parameter_set_id,selected_model,selected_completeness,"
        "selected_order_value,candidate_parameter_set_count,"
        "runner_up_parameter_set_id,applicability_evidence_id "
        "FROM source_parameter_set_preselections WHERE source_id='source.model'"
    ).fetchone()
    model_facts = con.execute(
        "SELECT quantity_key,normalized_value,parameter_set_id "
        "FROM selected_facts WHERE source_id='source.model' ORDER BY quantity_key"
    ).fetchall()
    binding_lineage_counts = con.execute(
        "SELECT COUNT(*) FILTER(WHERE fact_status='source_selected' AND binding_id IS NULL),"
        "COUNT(*) FILTER(WHERE fact_status='derived' AND binding_id IS NOT NULL) "
        "FROM selected_facts"
    ).fetchone()
    con.close()

    assert ("teff_k", 5800.0, 5700.0, 5900.0, "source_selected") in facts
    assert ("orbital_period_days", 10.0, 9.8, 10.3, "source_selected") in facts
    assert classification_facts == [
        ("spectral_type_infrared", "L0"),
        ("spectral_type_optical", "M8"),
    ]
    assert classification_bindings == [
        ("class-ir", None, "accepted", "unique current canonical target"),
        ("class-missing-opt", None, "missing", "source identifier absent from current canonical graph"),
        ("class-opt", None, "accepted", "unique current canonical target"),
        (
            "class-scoped-opt",
            "primary",
            "unresolved",
            "component scope requires an explicit compatible binding policy",
        ),
    ]
    assert model_preselection == (
        "model-he",
        "helium",
        3,
        1.0,
        2,
        "model-h",
        "context-good",
    )
    assert model_facts == [
        ("logg_cgs", 8.1, "model-he"),
        ("mass_msun", 0.7, "model-he"),
        ("teff_k", 11000.0, "model-he"),
    ]
    assert binding_lineage_counts == (0, 0)
    assert ("parallax_mas", 20.0, 19.5, 20.5, "source_selected") in facts
    assert ("distance_geometric_pc", 50.0, 45.0, 57.0, "source_selected") in facts
    assert ("distance_photogeometric_pc", 49.0, 46.0, 53.0, "source_selected") in facts
    assert ("distance_gspphot_pc", 101.0, 95.0, 110.0, "source_selected") in facts
    assert ("variability_flag", None, None, None, "source_selected") in facts
    assert any(row[0] == "luminosity_lsun" and row[4] == "derived" for row in facts)
    assert ("atmosphere", "hot-set", 10, 50.0, "hot-low-set", 10.0) in decisions
    assert any(row[:3] == ("orbit", "planet-default-set", 10) for row in decisions)
    assert json.loads(supersedes) == ["old-path"]
    assert distance_bindings == [
        (
            "distance-ambiguous",
            "ambiguous",
            "authoritative_release_equivalence:gaia_edr3->gaia_dr3",
            "multiple current canonical targets",
            None,
        ),
        (
                "distance-missing",
                "missing",
                "authoritative_release_equivalence:gaia_edr3->gaia_dr3",
                "source identifier absent from current canonical graph",
            None,
        ),
        (
            "distance-record",
            "accepted",
            "authoritative_release_equivalence:gaia_edr3->gaia_dr3",
            "authoritative identical release source list; unique current canonical target",
            "star-key",
        ),
    ]


def test_coherent_direct_groups_filter_parameter_set_kind(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    manifest_path = (
        state
        / "derived/evidence_lake_v2/scientific_evidence_sets/release-set-test/manifest.json"
    )
    release = compiler.load_json(manifest_path)
    member = next(
        row
        for row in release["members"]
        if row["source_ids"] == ["source.coherent"]
    )
    database = state / member["artifact_path"] / member["database"]
    con = duckdb.connect(str(database))
    con.execute(
        "INSERT INTO source_records VALUES ('unselected-record','other_table','{}'); "
        "INSERT INTO identifier_claim_evidence VALUES "
        "('unselected-record','gaia_dr3_source_id','Gaia DR3 123','star',NULL); "
        "INSERT INTO stellar_source_parameter_sets VALUES "
        "('unselected-set','unselected-record',NULL,'unselected_context',"
        "'[999.0,99.0,\"WRONG\"]','coherent-method',NULL,'other-ref','norm-v1','{}')"
    )
    con.close()
    member["database_bytes"] = database.stat().st_size
    member["database_sha256"] = compiler.file_sha256(database)
    write_json(manifest_path, release)

    report = compiler.compile_selected_facts(
        state_dir=state,
        policy_path=policy_path,
        temp_directory=tmp_path / "spill",
        threads=1,
        memory_limit="1GB",
    )
    artifact = state / "derived/evidence_lake_v2/selected_facts" / report["build_id"]
    con = duckdb.connect(str(artifact / "selected_facts.duckdb"), read_only=True)
    values = con.execute(
        "SELECT quantity_key,value_raw FROM selected_facts "
        "WHERE source_id='source.coherent' ORDER BY quantity_key"
    ).fetchall()
    con.close()
    assert values == [("parallax_mas", "20.0"), ("variability_flag", "VARIABLE")]


def test_checked_in_selection_policy_is_valid_for_promoted_release_set() -> None:
    policy = compiler.load_json(compiler.DEFAULT_POLICY)
    _, manifest = compiler.release_set_paths(Path("/data/spacegate/state"), policy)
    compiler.validate_policy(policy, manifest)
    assert len(policy["selection_sources"]) == 16
    nasa_programs = [
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "nasa_exoplanet_archive.planetary_systems"
    ]
    assert {source["object_type"] for source in nasa_programs} == {"star", "planet"}
    nasa_host = next(
        source for source in nasa_programs if source["object_type"] == "star"
    )
    assert nasa_host["binding"]["strategy"] == "canonical_identifier_consensus"
    assert nasa_host["expected_binding_outcomes"] == {
        "accepted": 27945,
        "ambiguous": 5,
        "missing": 104628,
    }
    nasa_groups = {
        group["group_key"]: group for group in nasa_host["quantity_groups"]
    }
    assert [
        rule["rank"] for rule in nasa_groups["stellar_atmosphere"]["authorities"]
    ] == [28, 29, 35]
    assert [
        rule["rank"] for rule in nasa_groups["stellar_fundamental"]["authorities"]
    ] == [8, 18, 25]
    vsx = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "classification.vsx"
    )
    assert vsx["binding"] == {
        "strategy": "canonical_identifier",
        "claim_namespace": "gaia_dr3_source_id",
        "canonical_namespace": "gaia_dr3",
        "normalization": "unsigned_decimal",
    }
    assert vsx["expected_binding_outcomes"] == {
        "accepted": 226017,
        "missing": 10078590,
    }
    assert vsx["expected_selected_facts"] == 248712
    assert {
        group["group_key"] for group in vsx["quantity_groups"]
    } == {
        "stellar_variability_classification",
        "stellar_variability_period",
    }
    simbad = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "identity.simbad"
    )
    assert simbad["binding"]["strategy"] == "release_identifier_bridge"
    assert simbad["binding"]["fallback_identifier_namespaces"] == [
        {
            "claim_namespace": "hip_id",
            "canonical_namespace": "hip",
            "normalization": "unsigned_decimal",
        },
        {
            "claim_namespace": "hd_id",
            "canonical_namespace": "hd",
            "normalization": "unsigned_decimal",
        },
    ]
    assert simbad["expected_binding_outcomes"] == {
        "accepted": 324277,
        "ambiguous": 10,
        "missing": 110792,
    }
    wgsn = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "naming.iau_wgsn"
    )
    assert wgsn["storage"] == "identifier_claim"
    assert wgsn["binding"]["strategy"] == "canonical_identifier_consensus"
    assert wgsn["expected_binding_outcomes"] == {
        "accepted": 415,
        "ambiguous": 2,
        "missing": 180,
    }
    iau_standard = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "standards.iau_2015_resolution_b3"
    )
    assert iau_standard["binding"]["strategy"] == "canonical_unique_name"
    assert iau_standard["component_scope_policy"] == "same_record_object_identifier"
    assert iau_standard["expected_selected_facts"] == 1
    ultracool = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "ultracool.ultracoolsheet"
    )
    assert ultracool["binding_applicability"] == {
        "conditions": [
            {
                "scope": "source_context",
                "path": "$.astrom_Gaia",
                "operator": "ne_or_null",
                "value": "P",
                "value_type": "string",
            }
        ],
        "reason": (
            "UltracoolSheet astrom_Gaia=P rows use a higher-mass primary as an "
            "astrometric proxy; object-scoped companion evidence cannot bind "
            "through that primary identifier"
        ),
    }
    assert ultracool["expected_binding_outcomes"] == {
        "accepted": 4843,
        "excluded": 512,
        "missing": 5532,
    }
    assert ultracool["expected_selected_facts"] == 4843
    assert {item["derivation_key"] for item in policy["derivations"]} == {
        "stellar_luminosity_stefan_boltzmann",
        "planet_semimajor_axis_kepler",
        "planet_insolation",
        "planet_equilibrium_temperature",
    }


def test_ranked_eav_candidates_preserve_parameter_set_binding_for_scoped_evidence() -> None:
    con = duckdb.connect()
    con.execute(
        """
        CREATE SCHEMA source_test;
        CREATE TABLE source_test.source_records (
          source_record_id VARCHAR, source_table VARCHAR, source_context_json JSON
        );
        CREATE TABLE source_test.stellar_parameter_sets (
          parameter_set_id VARCHAR, source_record_id VARCHAR,
          component_scope VARCHAR, method VARCHAR, model VARCHAR,
          reference_raw VARCHAR, quality_json JSON
        );
        CREATE TABLE source_test.stellar_parameter_evidence (
          evidence_id VARCHAR, parameter_set_id VARCHAR, source_record_id VARCHAR,
          component_scope VARCHAR, quantity_key VARCHAR, value_raw VARCHAR,
          unit_raw VARCHAR, normalized_value DOUBLE, normalized_unit VARCHAR,
          uncertainty_lower DOUBLE, uncertainty_upper DOUBLE,
          bound_semantics VARCHAR, method VARCHAR, model VARCHAR,
          reference_raw VARCHAR, quality_json JSON, normalization_version VARCHAR
        );
        INSERT INTO source_test.source_records VALUES ('record-1','constants','{}');
        INSERT INTO source_test.stellar_parameter_sets VALUES
          ('set-1','record-1','Sun','iau-method',NULL,'iau-ref','{}');
        INSERT INTO source_test.stellar_parameter_evidence VALUES
          ('evidence-1','set-1','record-1','Sun','iau.effective_temperature',
           '5772.0','K',5772.0,'K',0.8,0.8,'measurement','iau-method',NULL,
           'iau-ref','{}','iau-v1');
        CREATE TABLE evidence_object_bindings (
          binding_id VARCHAR, source_id VARCHAR, object_type VARCHAR,
          binding_subject_kind VARCHAR, binding_subject_id VARCHAR,
          source_record_id VARCHAR, stable_object_key VARCHAR,
          system_stable_object_key VARCHAR, binding_status VARCHAR
        );
        INSERT INTO evidence_object_bindings VALUES
          ('binding-1','standards.test','star','parameter_set','set-1','record-1',
           'sun-key','sol-key','accepted');
        """
    )
    compiler.create_candidate_table(con)
    source = {
        "source_id": "standards.test",
        "object_type": "star",
        "storage": "eav",
        "parameter_set_table": "stellar_parameter_sets",
        "parameter_evidence_table": "stellar_parameter_evidence",
        "component_scope_field": "component_scope",
        "component_scope_policy": "same_record_object_identifier",
        "_policy_version": "test-v1",
        "quantity_groups": [
            {
                "group_key": "atmosphere",
                "quantities": {"iau.effective_temperature": "teff_k"},
                "authorities": [
                    {"rank": 1, "method": "iau-method", "reason": "test authority"}
                ],
            }
        ],
    }
    compiler.insert_candidates(
        con,
        source=source,
        source_alias="source_test",
        member={"build_id": "e4-test"},
        release_id="r1",
    )
    assert con.execute(
        "SELECT stable_object_key,quantity_key,normalized_value,binding_id "
        "FROM fact_candidates"
    ).fetchall() == [("sun-key", "teff_k", 5772.0, "binding-1")]
    con.close()


def test_one_source_can_select_star_and_planet_scopes_without_leakage(
    tmp_path: Path,
) -> None:
    e4_db = tmp_path / "e4.duckdb"
    identity_db = tmp_path / "identity.duckdb"
    core_db = tmp_path / "core.duckdb"
    output_db = tmp_path / "selected.duckdb"

    con = duckdb.connect(str(e4_db))
    con.execute(
        """
        CREATE TABLE source_records(
          source_record_id VARCHAR,source_table VARCHAR,source_context_json JSON
        );
        CREATE TABLE identifier_claim_evidence(
          source_record_id VARCHAR,namespace VARCHAR,identifier_normalized VARCHAR,
          claim_scope VARCHAR,component_scope VARCHAR
        );
        CREATE TABLE stellar_parameter_sets(
          parameter_set_id VARCHAR,source_record_id VARCHAR,method VARCHAR,
          model VARCHAR,reference_raw VARCHAR,quality_json JSON
        );
        CREATE TABLE stellar_parameter_evidence(
          evidence_id VARCHAR,parameter_set_id VARCHAR,source_record_id VARCHAR,
          quantity_key VARCHAR,value_raw VARCHAR,normalized_value DOUBLE,
          normalized_unit VARCHAR,uncertainty_lower DOUBLE,uncertainty_upper DOUBLE,
          bound_semantics VARCHAR,reference_raw VARCHAR,normalization_version VARCHAR,
          quality_json JSON
        );
        CREATE TABLE planet_parameter_sets AS SELECT * FROM stellar_parameter_sets LIMIT 0;
        CREATE TABLE planet_parameter_evidence AS SELECT * FROM stellar_parameter_evidence LIMIT 0;
        INSERT INTO source_records VALUES ('shared','source_table','{}');
        INSERT INTO identifier_claim_evidence VALUES
          ('shared','gaia_dr3_source_id','Gaia DR3 123','star',NULL),
          ('shared','nasa_planet_name','Test 1 b','planet_or_candidate',NULL);
        INSERT INTO stellar_parameter_sets VALUES
          ('star-set','shared','host-method',NULL,'host-ref','{}');
        INSERT INTO stellar_parameter_evidence VALUES
          ('star-mass','star-set','shared','host_mass','1.0',1.0,'M_sun',
           NULL,NULL,'measurement','host-ref','v1','{}');
        INSERT INTO planet_parameter_sets VALUES
          ('planet-set','shared','planet-method',NULL,'planet-ref','{}');
        INSERT INTO planet_parameter_evidence VALUES
          ('planet-radius','planet-set','shared','planet_radius','2.0',2.0,'R_earth',
           NULL,NULL,'measurement','planet-ref','v1','{}');
        """
    )
    con.close()

    con = duckdb.connect(str(identity_db))
    con.execute(
        """
        CREATE TABLE canonical_identifier_bindings(
          namespace VARCHAR,id_value_norm VARCHAR,object_node_key VARCHAR,
          stable_object_key VARCHAR,system_stable_object_key VARCHAR
        );
        CREATE TABLE canonical_object_nodes(
          object_node_key VARCHAR,stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,object_type VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('gaia_dr3','123','star-node','star-key','system-key');
        INSERT INTO canonical_object_nodes VALUES
          ('star-node','star-key','system-key','star'),
          ('planet-node','planet-key','system-key','planet');
        """
    )
    con.close()

    con = duckdb.connect(str(core_db))
    con.execute(
        "CREATE TABLE planets(planet_name_norm VARCHAR,stable_object_key VARCHAR,"
        "system_id BIGINT); INSERT INTO planets VALUES ('test 1 b','planet-key',1)"
    )
    con.close()

    star_source = {
        "source_id": "shared.source",
        "object_type": "star",
        "binding_scope": "host",
        "binding": {
            "strategy": "canonical_identifier",
            "claim_namespace": "gaia_dr3_source_id",
            "canonical_namespace": "gaia_dr3",
            "normalization": "unsigned_decimal",
        },
        "parameter_set_table": "stellar_parameter_sets",
        "parameter_evidence_table": "stellar_parameter_evidence",
        "quantity_groups": [
            {
                "group_key": "stellar_fundamental",
                "quantities": {"host_mass": "mass_msun"},
                "authorities": [{"rank": 10, "reason": "host"}],
            }
        ],
        "_policy_version": "test",
    }
    planet_source = {
        "source_id": "shared.source",
        "object_type": "planet",
        "binding_scope": "planet",
        "binding": {
            "strategy": "canonical_unique_name",
            "claim_namespace": "nasa_planet_name",
            "canonical_table": "planets",
            "canonical_name_field": "planet_name_norm",
            "normalization": "spacegate_public_name_v1",
        },
        "parameter_set_table": "planet_parameter_sets",
        "parameter_evidence_table": "planet_parameter_evidence",
        "quantity_groups": [
            {
                "group_key": "planet_bulk",
                "quantities": {"planet_radius": "radius_earth"},
                "authorities": [{"rank": 10, "reason": "planet"}],
            }
        ],
        "_policy_version": "test",
    }

    con = duckdb.connect(str(output_db))
    compiler.create_schema(con)
    compiler.create_candidate_table(con)
    con.execute(f"ATTACH '{e4_db}' AS e4 (READ_ONLY)")
    con.execute(f"ATTACH '{identity_db}' AS identity (READ_ONLY)")
    con.execute(f"ATTACH '{core_db}' AS core (READ_ONLY)")
    member = {"build_id": "e4-test"}
    assert compiler.create_binding(
        con,
        source=star_source,
        source_alias="e4",
        member=member,
        release_id="r1",
    ) == (1, 1)
    assert compiler.create_binding(
        con,
        source=planet_source,
        source_alias="e4",
        member=member,
        release_id="r1",
    ) == (1, 1)
    compiler.insert_candidates(
        con,
        source=star_source,
        source_alias="e4",
        member=member,
        release_id="r1",
    )
    compiler.insert_candidates(
        con,
        source=planet_source,
        source_alias="e4",
        member=member,
        release_id="r1",
    )
    assert con.execute(
        "SELECT object_type,stable_object_key,quantity_key FROM fact_candidates "
        "ORDER BY object_type"
    ).fetchall() == [
        ("planet", "planet-key", "radius_earth"),
        ("star", "star-key", "mass_msun"),
    ]
    con.close()


def test_release_bridge_uses_same_object_identifier_fallback_only_without_primary(
    tmp_path: Path,
) -> None:
    source_db = tmp_path / "source.duckdb"
    identity_db = tmp_path / "identity.duckdb"
    output_db = tmp_path / "selected.duckdb"

    con = duckdb.connect(str(source_db))
    con.execute(
        """
        CREATE TABLE source_records(source_record_id VARCHAR);
        CREATE TABLE identifier_claim_evidence(
          source_record_id VARCHAR,namespace VARCHAR,identifier_normalized VARCHAR,
          claim_scope VARCHAR,component_scope VARCHAR
        );
        CREATE TABLE stellar_classification_evidence(
          evidence_id VARCHAR,source_record_id VARCHAR,component_scope VARCHAR,
          classification_scheme VARCHAR,classification_raw VARCHAR
        );
        INSERT INTO source_records VALUES ('basic-fallback'),('basic-primary'),
          ('basic-conflict'),('ids-fallback'),('ids-primary'),('ids-conflict-a'),
          ('ids-conflict-b');
        INSERT INTO stellar_classification_evidence VALUES
          ('class-fallback','basic-fallback',NULL,'spectral_type','A1V'),
          ('class-primary','basic-primary',NULL,'spectral_type','F5V'),
          ('class-conflict','basic-conflict',NULL,'spectral_type','K2V');
        INSERT INTO identifier_claim_evidence VALUES
          ('basic-fallback','simbad_oid','1','object',NULL),
          ('ids-fallback','simbad_oid','1','object',NULL),
          ('ids-fallback','hip_id','101','object',NULL),
          ('basic-primary','simbad_oid','2','object',NULL),
          ('ids-primary','simbad_oid','2','object',NULL),
          ('ids-primary','gaia_dr3_source_id','202','object',NULL),
          ('ids-primary','hip_id','102','object',NULL),
          ('basic-conflict','simbad_oid','3','object',NULL),
          ('ids-conflict-a','simbad_oid','3','object',NULL),
          ('ids-conflict-a','hip_id','103','object',NULL),
          ('ids-conflict-b','simbad_oid','3','object',NULL),
          ('ids-conflict-b','hd_id','203','object',NULL);
        """
    )
    con.close()

    con = duckdb.connect(str(identity_db))
    con.execute(
        """
        CREATE TABLE canonical_identifier_bindings(
          namespace VARCHAR,id_value_norm VARCHAR,object_node_key VARCHAR,
          stable_object_key VARCHAR,system_stable_object_key VARCHAR
        );
        CREATE TABLE canonical_object_nodes(
          object_node_key VARCHAR,stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,object_type VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('hip','101','fallback-node','fallback-star','fallback-system'),
          ('gaia_dr3','202','primary-node','primary-star','primary-system'),
          ('hip','102','wrong-node','wrong-star','wrong-system'),
          ('hip','103','conflict-a','conflict-star-a','conflict-system'),
          ('hd','203','conflict-b','conflict-star-b','conflict-system');
        INSERT INTO canonical_object_nodes VALUES
          ('fallback-node','fallback-star','fallback-system','star'),
          ('primary-node','primary-star','primary-system','star'),
          ('wrong-node','wrong-star','wrong-system','star'),
          ('conflict-a','conflict-star-a','conflict-system','star'),
          ('conflict-b','conflict-star-b','conflict-system','star');
        """
    )
    con.close()

    source = {
        "source_id": "identity.test",
        "object_type": "star",
        "binding_scope": "star",
        "binding": {
            "strategy": "release_identifier_bridge",
            "claim_namespace": "simbad_oid",
            "bridge_match_namespace": "simbad_oid",
            "bridge_target_namespace": "gaia_dr3_source_id",
            "canonical_namespace": "gaia_dr3",
            "normalization": "unsigned_decimal",
            "fallback_identifier_namespaces": [
                {
                    "claim_namespace": "hip_id",
                    "canonical_namespace": "hip",
                    "normalization": "unsigned_decimal",
                },
                {
                    "claim_namespace": "hd_id",
                    "canonical_namespace": "hd",
                    "normalization": "unsigned_decimal",
                },
            ],
        },
        "storage": "classification",
        "classification_evidence_table": "stellar_classification_evidence",
        "component_scope_policy": "require_null",
        "quantity_groups": [
            {
                "group_key": "classification",
                "quantities": {"spectral_type": {"quantity_key": "spectral_type_simbad", "numeric": False}},
                "authorities": [{"rank": 40, "method": "test", "reason": "test"}],
            }
        ],
        "_policy_version": "test",
    }
    con = duckdb.connect(str(output_db))
    compiler.create_schema(con)
    con.execute(f"ATTACH '{source_db}' AS source (READ_ONLY)")
    con.execute(f"ATTACH '{identity_db}' AS identity (READ_ONLY)")
    eligible, accepted = compiler.create_binding(
        con,
        source=source,
        source_alias="source",
        member={"build_id": "e4-test"},
        release_id="r1",
    )
    rows = con.execute(
        "SELECT binding_subject_id,stable_object_key,binding_status,binding_method "
        "FROM evidence_object_bindings ORDER BY binding_subject_id"
    ).fetchall()
    con.close()

    assert (eligible, accepted) == (3, 2)
    assert rows == [
        ("class-conflict", None, "ambiguous", "release_same_object_identifier_fallback"),
        ("class-fallback", "fallback-star", "accepted", "release_same_object_identifier_fallback"),
        ("class-primary", "primary-star", "accepted", "release_identifier_bridge"),
    ]


def test_policy_rejects_duplicate_selection_program_scope(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    policy = compiler.load_json(policy_path)
    policy["selection_sources"].append(dict(policy["selection_sources"][0]))
    _, manifest = compiler.release_set_paths(state, policy)
    with pytest.raises(ValueError, match="duplicate or incomplete selection program"):
        compiler.validate_policy(policy, manifest)


def test_policy_rejects_duplicate_source_channel_dispositions(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    policy = compiler.load_json(policy_path)
    policy["selection_sources"][0]["channel_dispositions"] = [
        {"channel": "measurements", "disposition": "selected", "reason": "used"},
        {
            "channel": "measurements",
            "disposition": "evidence_only",
            "reason": "duplicate",
        },
    ]
    _, manifest = compiler.release_set_paths(state, policy)
    with pytest.raises(ValueError, match="invalid source channel dispositions"):
        compiler.validate_policy(policy, manifest)


def test_unique_source_target_policy_requires_one_quantity() -> None:
    policy = compiler.load_json(compiler.DEFAULT_POLICY)
    wgsn = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "naming.iau_wgsn"
    )
    wgsn["quantity_groups"].append(
        {
            "group_key": "invalid_second_name_channel",
            "quantities": {
                "iau_star_name_search_spelling": {
                    "quantity_key": "official_name_search_spelling",
                    "numeric": False,
                }
            },
            "authorities": [{"rank": 1, "reason": "invalid fixture"}],
        }
    )
    _, manifest = compiler.release_set_paths(Path("/data/spacegate/state"), policy)
    with pytest.raises(
        ValueError,
        match="unique-source-target policy requires exactly one source quantity",
    ):
        compiler.validate_policy(policy, manifest)


def test_file_hash_attestor_reuses_only_unchanged_byte_verification(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"first")
    attestor = compiler.FileHashAttestor()
    first = attestor.digest(artifact)
    assert attestor.digest(artifact) == first
    assert attestor.hash_count == 1

    artifact.write_bytes(b"second")
    second = attestor.digest(artifact)
    assert second != first
    assert attestor.hash_count == 2
    with pytest.raises(ValueError, match="immutable input checksum changed"):
        attestor.verify(artifact, first)


def test_scoped_classification_requires_explicit_same_record_policy(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    policy = compiler.load_json(policy_path)
    classification = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "source.classification"
    )
    classification["component_scope_policy"] = "same_record_object_identifier"
    classification["expected_binding_outcomes"] = {"accepted": 3, "missing": 1}
    write_json(policy_path, policy)

    report = compiler.compile_selected_facts(
        state_dir=state,
        policy_path=policy_path,
        temp_directory=tmp_path / "spill",
        threads=1,
        memory_limit="1GB",
    )
    assert report["binding_outcomes"]["source.classification"] == {
        "accepted": 3,
        "missing": 1,
    }
    artifact = state / "derived/evidence_lake_v2/selected_facts" / report["build_id"]
    con = duckdb.connect(str(artifact / "selected_facts.duckdb"), read_only=True)
    scoped = con.execute(
        "SELECT binding_status,stable_object_key,component_scope "
        "FROM evidence_object_bindings "
        "WHERE source_id='source.classification' "
        "AND binding_subject_id='class-scoped-opt'"
    ).fetchone()
    con.close()
    assert scoped == ("accepted", "star-key", "primary")


def test_source_context_binding_applicability_excludes_proxy_identifiers(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    policy = compiler.load_json(policy_path)
    classification = next(
        source
        for source in policy["selection_sources"]
        if source["source_id"] == "source.classification"
    )
    classification["binding_applicability"] = {
        "conditions": [
            {
                "scope": "source_context",
                "path": "$.astrom_Gaia",
                "operator": "ne_or_null",
                "value": "P",
                "value_type": "string",
            }
        ],
        "reason": "primary-proxy identifiers cannot bind companion evidence",
    }
    classification["expected_binding_outcomes"] = {
        "accepted": 1,
        "excluded": 1,
        "missing": 1,
        "unresolved": 1,
    }
    classification["expected_selected_facts"] = 1
    write_json(policy_path, policy)

    report = compiler.compile_selected_facts(
        state_dir=state,
        policy_path=policy_path,
        temp_directory=tmp_path / "spill",
        threads=1,
        memory_limit="1GB",
    )
    assert report["binding_outcomes"]["source.classification"] == {
        "accepted": 1,
        "excluded": 1,
        "missing": 1,
        "unresolved": 1,
    }
    selected = state / "derived/evidence_lake_v2/selected_facts" / report["build_id"]
    con = duckdb.connect(str(selected / "selected_facts.duckdb"), read_only=True)
    proxy_bindings = con.execute(
        "SELECT binding_status,applicability_status,binding_reason "
        "FROM evidence_object_bindings "
        "WHERE source_id='source.classification' AND source_record_id='class-record' "
        "ORDER BY binding_subject_id"
    ).fetchall()
    proxy_facts = con.execute(
        "SELECT COUNT(*) FROM selected_facts "
        "WHERE source_id='source.classification'"
    ).fetchone()[0]
    con.close()

    assert proxy_bindings == [
        (
            "excluded",
            "inapplicable",
            "source evidence fails the configured applicability predicate: "
            "primary-proxy identifiers cannot bind companion evidence",
        )
    ]
    assert proxy_facts == 1


def test_release_equivalence_binding_requires_authoritative_contract(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    policy = compiler.load_json(policy_path)
    release_manifest = compiler.load_json(
        state
        / "derived/evidence_lake_v2/scientific_evidence_sets/release-set-test/manifest.json"
    )
    distance = next(
        source for source in policy["selection_sources"]
        if source["source_id"] == "source.distance"
    )
    del distance["binding"]["release_equivalence"]["source_list_identical"]

    with pytest.raises(ValueError, match="lacks authoritative contract"):
        compiler.validate_policy(policy, release_manifest)


def test_quality_policy_rejects_invalid_and_wrong_scope_rules(tmp_path: Path) -> None:
    state = tmp_path / "state"
    policy_path = fixture_policy(state, tmp_path)
    policy = compiler.load_json(policy_path)
    release_manifest = compiler.load_json(
        state
        / "derived/evidence_lake_v2/scientific_evidence_sets/release-set-test/manifest.json"
    )
    gaia = next(
        source for source in policy["selection_sources"]
        if source["source_id"] == "source.gaia"
    )
    rule = gaia["quantity_groups"][0]["authorities"][0]
    rule["quality_conditions"][0]["path"] = "flag"
    with pytest.raises(ValueError, match="invalid quality JSON path"):
        compiler.validate_policy(policy, release_manifest)

    policy = compiler.load_json(policy_path)
    coherent = next(
        source for source in policy["selection_sources"]
        if source["source_id"] == "source.coherent"
    )
    coherent["quantity_groups"][0]["authorities"][0]["quality_order"] = {
        "scope": "evidence_quality",
        "path": "$.snr",
        "direction": "desc",
    }
    with pytest.raises(ValueError, match="coherent-array quality rules cannot use evidence scope"):
        compiler.validate_policy(policy, release_manifest)


def test_missing_quality_score_only_fails_for_same_authority_competition() -> None:
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE parameter_set_selection_decisions ("
        "selected_source_id VARCHAR,quantity_group VARCHAR,authority_rank INTEGER,"
        "authority_reason VARCHAR,selection_quality_score DOUBLE,"
        "candidate_parameter_set_count INTEGER,runner_up_authority_rank INTEGER)"
    )
    policy = {
        "selection_sources": [{
            "source_id": "source.test",
            "quantity_groups": [{
                "group_key": "stellar_test",
                "authorities": [{
                    "rank": 10,
                    "reason": "quality-ranked test evidence",
                    "quality_order": {
                        "scope": "source_context",
                        "path": "$.score",
                        "direction": "desc",
                    },
                }],
            }],
        }],
    }
    con.execute(
        "INSERT INTO parameter_set_selection_decisions VALUES "
        "('source.test','stellar_test',10,'quality-ranked test evidence',NULL,1,NULL)"
    )
    checks = compiler.selection_quality_integrity_checks(con, policy)
    assert set(checks.values()) == {0}

    con.execute(
        "INSERT INTO parameter_set_selection_decisions VALUES "
        "('source.test','stellar_test',10,'quality-ranked test evidence',NULL,2,10)"
    )
    checks = compiler.selection_quality_integrity_checks(con, policy)
    assert set(checks.values()) == {1}
    con.close()


def test_reproduction_comparison_ignores_duckdb_bytes_but_not_parquet() -> None:
    report = {
        "status": "pass",
        "build_id": "build",
        "build_sha256": "build-sha",
        "policy_version": "policy",
        "evidence_release_set_id": "release",
        "identity_graph_id": "identity",
        "canonical_reference_build_id": "core",
        "table_counts": {"selected_facts": 1},
        "integrity_checks": {"duplicates": 0},
        "logical_content_sha256": "logical",
        "files": {
            "selected_facts.duckdb": {"sha256": "runtime-specific"},
            "selected_facts__teff_k.parquet": {"sha256": "deterministic"},
        },
    }
    reproduced = json.loads(json.dumps(report))
    reproduced["files"]["selected_facts.duckdb"]["sha256"] = "different"
    assert reproduction.compare_reports(report, reproduced) == []
    reproduced["files"]["selected_facts__teff_k.parquet"]["sha256"] = "different"
    assert reproduction.compare_reports(report, reproduced) == ["parquet_files"]
    assert reproduction.parquet_file_differences(report, reproduced) == [
        {
            "file": "selected_facts__teff_k.parquet",
            "reference": {"sha256": "deterministic"},
            "reproduced": {"sha256": "different"},
        }
    ]
