from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_facts as compiler  # noqa: E402


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
          ('gaia_dr3', '123', 'star-node', 'star-key', 'system-key');
        INSERT INTO canonical_object_nodes VALUES
          ('star-node', 'star-key', 'system-key'),
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
    if object_type == "star":
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
    release_id = "release-set-test"
    release_sha = "a" * 64
    write_json(
        state / "derived/evidence_lake_v2/scientific_evidence_sets" / release_id / "manifest.json",
        {
            "release_set_id": release_id,
            "release_set_sha256": release_sha,
            "status": "pass",
            "members": [gaia, nasa],
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
    assert report["table_counts"]["selected_facts"] == 4
    artifact = state / "derived/evidence_lake_v2/selected_facts" / report["build_id"]
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
    con.close()

    assert ("teff_k", 5800.0, 5700.0, 5900.0, "source_selected") in facts
    assert ("orbital_period_days", 10.0, 9.8, 10.3, "source_selected") in facts
    assert any(row[0] == "luminosity_lsun" and row[4] == "derived" for row in facts)
    assert ("atmosphere", "hot-set", 10) in decisions
    assert ("orbit", "planet-default-set", 10) in decisions
    assert json.loads(supersedes) == ["old-path"]


def test_checked_in_selection_policy_is_valid_for_promoted_release_set() -> None:
    policy = compiler.load_json(compiler.DEFAULT_POLICY)
    _, manifest = compiler.release_set_paths(Path("/data/spacegate/state"), policy)
    compiler.validate_policy(policy, manifest)
    assert len(policy["selection_sources"]) == 2
    assert {item["derivation_key"] for item in policy["derivations"]} == {
        "stellar_luminosity_stefan_boltzmann",
        "planet_semimajor_axis_kepler",
        "planet_insolation",
        "planet_equilibrium_temperature",
    }
