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
          system_stable_object_key VARCHAR
        );
        INSERT INTO canonical_identifier_bindings VALUES
          ('gaia_dr3', '123', 'star-node', 'star-key', 'system-key'),
          ('gaia_dr3', '888', 'ambiguous-node-a', 'ambiguous-key-a', 'system-key-a'),
          ('gaia_dr3', '888', 'ambiguous-node-b', 'ambiguous-key-b', 'system-key-b');
        INSERT INTO canonical_object_nodes VALUES
          ('star-node', 'star-key', 'system-key'),
          ('ambiguous-node-a', 'ambiguous-key-a', 'system-key-a'),
          ('ambiguous-node-b', 'ambiguous-key-b', 'system-key-b'),
          ('planet-node', 'planet-key', 'system-key');
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
          source_record_id VARCHAR, namespace VARCHAR, identifier_normalized VARCHAR
        );
        """
    )
    if object_type == "coherent":
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
              values_json JSON, method VARCHAR, model VARCHAR, reference_raw VARCHAR,
              normalization_version VARCHAR, quality_json JSON
            );
            INSERT INTO source_records VALUES ('coherent-record', 'gaia_source', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('coherent-record', 'gaia_dr3_source_id', 'Gaia DR3 123');
            INSERT INTO stellar_source_parameter_sets VALUES
              ('coherent-set', 'coherent-record', NULL, '[20.0,0.5,"VARIABLE"]',
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
              ('distance-record', 'gaia_edr3_source_id', 'Gaia EDR3 123'),
              ('distance-missing', 'gaia_edr3_source_id', 'Gaia EDR3 999'),
              ('distance-ambiguous', 'gaia_edr3_source_id', 'Gaia EDR3 888');
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
            INSERT INTO source_records VALUES
              ('gaia-record', 'gaia_ap', '{}');
            INSERT INTO identifier_claim_evidence VALUES
              ('gaia-record', 'gaia_dr3_source_id', 'Gaia DR3 123');
            INSERT INTO stellar_parameter_sets VALUES
              ('hot-set', 'gaia-record', NULL, 'hot-method', NULL, 'hot-ref'),
              ('fallback-set', 'gaia-record', NULL, 'fallback-method', NULL, 'fallback-ref'),
              ('fundamental-set', 'gaia-record', NULL, 'fundamental-method', NULL, 'fund-ref');
            INSERT INTO stellar_parameter_evidence VALUES
              ('teff-hot', 'hot-set', 'gaia-record', 'effective_temperature', '5800', 5800, 'K', 5700, 5900, 'interval_endpoints', 'hot-ref', 'norm-v1', '{}'),
              ('teff-fallback', 'fallback-set', 'gaia-record', 'effective_temperature', '5700', 5700, 'K', 100, 100, 'symmetric_error', 'fallback-ref', 'norm-v1', '{}'),
              ('radius-fund', 'fundamental-set', 'gaia-record', 'stellar_radius', '1.0', 1.0, 'solRad', 0.1, 0.1, 'symmetric_error', 'fund-ref', 'norm-v1', '{}');
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
              ('planet-default', 'nasa_planet_name', 'Test-1 b'),
              ('planet-composite', 'nasa_planet_name', 'Test-1 b');
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
    release_id = "release-set-test"
    release_sha = "a" * 64
    write_json(
        state / "derived/evidence_lake_v2/scientific_evidence_sets" / release_id / "manifest.json",
        {
            "release_set_id": release_id,
            "release_set_sha256": release_sha,
            "status": "pass",
            "members": [gaia, nasa, coherent, distance],
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
                    "quantity_groups": [
                        {
                            "group_key": "atmosphere",
                            "quantities": {"effective_temperature": "teff_k"},
                            "authorities": [
                                {"rank": 10, "method": "hot-method", "reason": "specialized"},
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
    assert report["table_counts"]["selected_facts"] == 8
    assert report["binding_outcomes"]["source.distance"] == {
        "accepted": 1,
        "ambiguous": 1,
        "missing": 1,
    }
    artifact = state / "derived/evidence_lake_v2/selected_facts" / report["build_id"]
    assert (artifact / "selected_facts__teff_k.parquet").is_file()
    assert (artifact / "selection_decisions__atmosphere.parquet").is_file()
    assert sum(report["partition_exports"]["selected_facts"].values()) == 8
    assert sum(report["partition_exports"]["selection_decisions"].values()) == 6
    audit = artifact_audit.audit_artifact(artifact, policy)
    assert audit["status"] == "pass"
    assert audit["failing_checks"] == {}
    con = duckdb.connect(str(artifact / "selected_facts.duckdb"), read_only=True)
    facts = con.execute(
        "SELECT quantity_key, normalized_value, value_lower, value_upper, fact_status "
        "FROM selected_facts ORDER BY quantity_key"
    ).fetchall()
    decisions = con.execute(
        "SELECT quantity_group, selected_parameter_set_id, authority_rank "
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
    con.close()

    assert ("teff_k", 5800.0, 5700.0, 5900.0, "source_selected") in facts
    assert ("orbital_period_days", 10.0, 9.8, 10.3, "source_selected") in facts
    assert ("parallax_mas", 20.0, 19.5, 20.5, "source_selected") in facts
    assert ("distance_geometric_pc", 50.0, 45.0, 57.0, "source_selected") in facts
    assert ("distance_photogeometric_pc", 49.0, 46.0, 53.0, "source_selected") in facts
    assert ("variability_flag", None, None, None, "source_selected") in facts
    assert any(row[0] == "luminosity_lsun" and row[4] == "derived" for row in facts)
    assert ("atmosphere", "hot-set", 10) in decisions
    assert ("orbit", "planet-default-set", 10) in decisions
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
            "no current canonical target",
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


def test_checked_in_selection_policy_is_valid_for_promoted_release_set() -> None:
    policy = compiler.load_json(compiler.DEFAULT_POLICY)
    _, manifest = compiler.release_set_paths(Path("/data/spacegate/state"), policy)
    compiler.validate_policy(policy, manifest)
    assert len(policy["selection_sources"]) == 4
    assert {item["derivation_key"] for item in policy["derivations"]} == {
        "stellar_luminosity_stefan_boltzmann",
        "planet_semimajor_axis_kepler",
        "planet_insolation",
        "planet_equilibrium_temperature",
    }


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
