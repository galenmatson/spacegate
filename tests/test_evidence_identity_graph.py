from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_evidence_identity_graph import (  # noqa: E402
    classify_release_binding,
    create_release_tables,
    validate_policy,
)
from verify_evidence_identity_reproduction import compare_graphs  # noqa: E402


POLICY_PATH = ROOT / "config" / "evidence_lake" / "identity_graph_policy.json"


def test_checked_in_identity_policy_separates_scope_and_prohibits_mutation() -> None:
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    validate_policy(policy)
    assert set(policy["scope_kinds"]) == {
        "physical_identity",
        "system_containment",
        "component_or_subsystem",
        "observation_target",
        "alias_or_public_name",
    }
    assert policy["production_constraints"]["canonical_inventory_mutation"] is False
    assert policy["production_constraints"]["canonical_containment_mutation"] is False
    assert "never compared" in policy["gaia_release_reconciliation"]["identifier_rule"]
    assert "not evidence" in policy["gaia_release_reconciliation"]["proper_motion_rule"]
    assert {
        "upstream_release_id",
        "forward_registry_source_id",
        "forward_registry_release_id",
        "reverse_registry_source_id",
        "reverse_registry_release_id",
    } <= set(policy["gaia_release_reconciliation"])
    assert all(
        {"scope_kind", "source_id", "release_id", "source_name"} <= set(value)
        for value in policy["source_target_scopes"].values()
    )


@pytest.mark.parametrize(
    ("forward", "reverse", "consistent", "canonical", "expected"),
    [
        (0, None, None, None, ("missing", "no_official_dr3_neighbour")),
        (2, None, None, None, ("ambiguous", "gaia_release_split_candidates")),
        (1, 2, True, 1, ("ambiguous", "gaia_release_merge_candidates")),
        (1, 0, None, None, ("quarantined", "reverse_neighbourhood_incomplete")),
        (1, 1, False, 1, ("quarantined", "forward_reverse_pair_payload_conflict")),
        (1, 1, True, 0, ("excluded", "outside_current_canonical_backbone")),
        (1, 1, True, 2, ("quarantined", "canonical_gaia_dr3_collision")),
        (
            1,
            1,
            True,
            1,
            ("accepted", "unique_bidirectional_release_mapping_and_canonical_binding"),
        ),
    ],
)
def test_release_binding_state_machine(
    forward: int,
    reverse: int | None,
    consistent: bool | None,
    canonical: int | None,
    expected: tuple[str, str],
) -> None:
    assert classify_release_binding(
        forward_candidate_count=forward,
        reverse_predecessor_count=reverse,
        pair_payload_consistent=consistent,
        canonical_match_count=canonical,
    ) == expected


def test_reproduction_comparison_verifies_report_and_artifact_hash(tmp_path: Path) -> None:
    expected_root = tmp_path / "expected"
    actual_root = tmp_path / "actual"
    (expected_root / "parquet").mkdir(parents=True)
    (actual_root / "parquet").mkdir(parents=True)
    payload = b"deterministic-parquet-fixture"
    digest = hashlib.sha256(payload).hexdigest()
    (actual_root / "parquet" / "nodes.parquet").write_bytes(payload)
    table = {
        "row_count": 2,
        "bytes": len(payload),
        "sha256": digest,
        "path": "parquet/nodes.parquet",
    }
    report = {
        "schema_version": "spacegate.evidence_identity_graph_report.v1",
        "graph_id": "graph-test",
        "status": "pass",
        "policy_version": "test.1",
        "inputs": {"fingerprint": "same"},
        "tables": {"nodes": table},
    }
    expected_report = expected_root / "identity_graph_report.json"
    actual_report = actual_root / "identity_graph_report.json"
    expected_report.write_text(json.dumps(report), encoding="utf-8")
    actual_report.write_text(json.dumps(report), encoding="utf-8")

    result = compare_graphs(expected_report, actual_report)

    assert result["status"] == "pass"
    (actual_root / "parquet" / "nodes.parquet").write_bytes(b"changed")
    assert compare_graphs(expected_report, actual_report)["status"] == "fail"


def test_release_tables_preserve_forward_reverse_and_upstream_lineage(
    tmp_path: Path,
) -> None:
    con = duckdb.connect()
    con.execute("set threads=1")
    paths = {
        name: tmp_path / f"{name}.parquet"
        for name in ("forward_targets", "forward_edges", "reverse_targets", "reverse_edges")
    }
    con.execute(
        f"""
        copy (select '10' dr2_source_id, 'fixture' source_families,
                     '1' source_family_count, '1' source_record_count)
        to '{paths['forward_targets']}' (format parquet)
        """
    )
    for name in ("forward_edges", "reverse_edges"):
        con.execute(
            f"""
            copy (select '10' dr2_source_id, '20' dr3_source_id,
                         '0.1' angular_distance, '0.2' magnitude_difference,
                         'true' proper_motion_propagation)
            to '{paths[name]}' (format parquet)
            """
        )
    con.execute(
        f"""
        copy (select '20' dr3_source_id, '1' forward_dr2_source_count,
                     '1' forward_pair_count)
        to '{paths['reverse_targets']}' (format parquet)
        """
    )
    con.execute("create schema core")
    con.execute(
        """
        create table core.stars(
          gaia_id bigint, stable_object_key varchar, system_id bigint,
          pm_ra_mas_yr double, pm_dec_mas_yr double
        )
        """
    )
    con.execute("insert into core.stars values (20, 'star:20', 1, 600, 0)")
    con.execute(
        "create table core.systems(system_id bigint, stable_object_key varchar, star_count bigint)"
    )
    con.execute("insert into core.systems values (1, 'system:1', 2)")
    lineage = {
        "forward_registry_source_id": "gaia.forward",
        "forward_registry_release_id": "forward-r1",
        "reverse_registry_source_id": "gaia.reverse",
        "reverse_registry_release_id": "reverse-r1",
        "upstream_release_id": "gaia-dr3",
        "table": "gaiadr3.dr2_neighbourhood",
    }

    create_release_tables(
        con,
        forward_targets=paths["forward_targets"],
        forward_edges=paths["forward_edges"],
        reverse_targets=paths["reverse_targets"],
        reverse_edges=paths["reverse_edges"],
        high_pm_threshold=500,
        lineage=lineage,
    )

    edge = con.execute(
        """
        select source_id, source_release_id, reverse_source_id,
               reverse_source_release_id, upstream_release_id, source_table
        from release_crossmatch_edges
        """
    ).fetchone()
    assert edge == (
        "gaia.forward",
        "forward-r1",
        "gaia.reverse",
        "reverse-r1",
        "gaia-dr3",
        "gaiadr3.dr2_neighbourhood",
    )
    outcome = con.execute(
        """
        select outcome, crossmatch_source_release_id,
               reverse_crossmatch_source_release_id, high_proper_motion_guard,
               canonical_star_accepted_dr2_count,
               canonical_system_accepted_dr2_count, duplicate_system_guard
        from dr2_release_outcomes
        """
    ).fetchone()
    assert outcome == ("accepted", "forward-r1", "reverse-r1", True, 1, 1, True)
    con.close()
