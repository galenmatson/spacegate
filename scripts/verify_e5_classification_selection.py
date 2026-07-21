#!/usr/bin/env python3
"""Verify E5 classification-subject binding without compiling the full artifact."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

import compile_selected_facts as compiler


DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e5_classification_selection_verification.json"
)
SOURCE_ID = "ultracool.ultracoolsheet"


def selected_source(policy: dict[str, Any]) -> dict[str, Any]:
    matches = [
        source
        for source in policy.get("selection_sources") or []
        if source.get("source_id") == SOURCE_ID
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one {SOURCE_ID} policy, found {len(matches)}")
    source = dict(matches[0])
    source["_policy_version"] = str(policy["policy_version"])
    return source


def compile_projection(
    *, state_dir: Path, policy: dict[str, Any], release_manifest: dict[str, Any]
) -> dict[str, Any]:
    source = selected_source(policy)
    member = compiler.member_by_source(release_manifest)[SOURCE_ID]
    artifact = state_dir / str(member["artifact_path"])
    database = artifact / str(member["database"])
    manifest_path = artifact / "manifest.json"
    if compiler.file_sha256(manifest_path) != member["manifest_sha256"]:
        raise ValueError("classification E4 member manifest changed")
    if database.stat().st_size != int(member["database_bytes"]):
        raise ValueError("classification E4 database size changed")
    if compiler.file_sha256(database) != member["database_sha256"]:
        raise ValueError("classification E4 database checksum changed")

    identity_db = (
        state_dir / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"]) / "identity_graph.duckdb"
    )
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET threads=1")
        compiler.create_schema(con)
        compiler.create_candidate_table(con)
        con.execute(
            f"ATTACH {compiler.sql_literal(str(identity_db))} AS identity (READ_ONLY)"
        )
        con.execute(
            f"ATTACH {compiler.sql_literal(str(database))} AS e4_source (READ_ONLY)"
        )
        release_id = str(member["release_ids"][SOURCE_ID])
        eligible, accepted = compiler.create_binding(
            con,
            source=source,
            source_alias="e4_source",
            member=member,
            release_id=release_id,
        )
        compiler.insert_candidates(
            con,
            source=source,
            source_alias="e4_source",
            member=member,
            release_id=release_id,
        )
        compiler.select_parameter_sets(con, str(policy["policy_version"]))

        outcomes = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        facts_by_quantity = {
            str(quantity): int(count)
            for quantity, count in con.execute(
                "SELECT quantity_key,COUNT(*) FROM selected_facts "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        checks = {
            "binding_subject_accounting": int(
                con.execute("SELECT COUNT(*) FROM evidence_object_bindings").fetchone()[0]
            ) - eligible,
            "accepted_binding_accounting": outcomes.get("accepted", 0) - accepted,
            "unexpected_binding_statuses": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings "
                    "WHERE binding_status NOT IN ('accepted','missing')"
                ).fetchone()[0]
            ),
            "accepted_nonstar_targets": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings b "
                    "JOIN identity.canonical_object_nodes o "
                    "ON o.object_node_key=b.canonical_object_node_key "
                    "WHERE b.binding_status='accepted' AND o.object_type<>'star'"
                ).fetchone()[0]
            ),
            "unresolved_bindings_with_targets": int(
                con.execute(
                    "SELECT COUNT(*) FROM evidence_object_bindings "
                    "WHERE binding_status<>'accepted' AND stable_object_key IS NOT NULL"
                ).fetchone()[0]
            ),
            "selected_nontext_values": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts "
                    "WHERE normalized_value IS NOT NULL OR value_raw IS NULL"
                ).fetchone()[0]
            ),
            "selected_without_accepted_subject": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts f WHERE NOT EXISTS ("
                    "SELECT 1 FROM evidence_object_bindings b "
                    "WHERE b.binding_status='accepted' "
                    "AND b.binding_subject_kind='classification_evidence' "
                    "AND b.binding_subject_id=f.evidence_id)"
                ).fetchone()[0]
            ),
            "duplicate_selected_object_quantities": int(
                con.execute(
                    "SELECT COALESCE(SUM(n-1),0) FROM ("
                    "SELECT COUNT(*) n FROM selected_facts "
                    "GROUP BY stable_object_key,quantity_key HAVING COUNT(*)>1)"
                ).fetchone()[0]
            ),
        }
        selected_facts = int(
            con.execute("SELECT COUNT(*) FROM selected_facts").fetchone()[0]
        )
        decisions = int(
            con.execute(
                "SELECT COUNT(*) FROM parameter_set_selection_decisions"
            ).fetchone()[0]
        )
        logical_rows = {
            "bindings": con.execute(
                "SELECT binding_id,binding_subject_id,binding_status,"
                "coalesce(stable_object_key,''),binding_reason "
                "FROM evidence_object_bindings ORDER BY binding_id"
            ).fetchall(),
            "facts": con.execute(
                "SELECT selected_fact_id,stable_object_key,quantity_key,value_raw,"
                "evidence_id,selection_decision_id FROM selected_facts "
                "ORDER BY selected_fact_id"
            ).fetchall(),
            "decisions": con.execute(
                "SELECT decision_id,stable_object_key,quantity_group,"
                "selected_parameter_set_id,runner_up_parameter_set_id "
                "FROM parameter_set_selection_decisions ORDER BY decision_id"
            ).fetchall(),
        }
    finally:
        con.close()

    return {
        "source_id": SOURCE_ID,
        "release_id": str(member["release_ids"][SOURCE_ID]),
        "evidence_build_id": str(member["build_id"]),
        "eligible_binding_subjects": eligible,
        "accepted_binding_subjects": accepted,
        "binding_outcomes": outcomes,
        "selected_facts": selected_facts,
        "selection_decisions": decisions,
        "facts_by_quantity": facts_by_quantity,
        "checks": checks,
        "logical_content_sha256": compiler.stable_sha256(logical_rows),
    }


def verify(state_dir: Path, policy_path: Path) -> dict[str, Any]:
    policy = compiler.load_json(policy_path)
    _, release_manifest = compiler.release_set_paths(state_dir, policy)
    compiler.validate_policy(policy, release_manifest)
    source = selected_source(policy)
    first = compile_projection(
        state_dir=state_dir, policy=policy, release_manifest=release_manifest
    )
    second = compile_projection(
        state_dir=state_dir, policy=policy, release_manifest=release_manifest
    )
    expected_outcomes = source.get("expected_binding_outcomes") or {}
    expected_selected = int(source.get("expected_selected_facts") or -1)
    failures = {
        key: value
        for key, value in {
            "binding_outcomes": first["binding_outcomes"] != expected_outcomes,
            "selected_fact_count": first["selected_facts"] != expected_selected,
            "decision_fact_count": first["selection_decisions"]
            != first["selected_facts"],
            "deterministic_projection": first != second,
            "scientific_checks": any(first["checks"].values()),
        }.items()
        if value
    }
    return {
        "schema_version": "spacegate.e5_classification_selection_verification.v1",
        "status": "fail" if failures else "pass",
        "policy_version": policy["policy_version"],
        "identity_graph_id": policy["identity_graph_id"],
        "expected_binding_outcomes": expected_outcomes,
        "expected_selected_facts": expected_selected,
        "failures": failures,
        "projection": first,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = verify(args.state_dir.resolve(), args.policy.resolve())
    compiler.atomic_json(args.report, report)
    projection = report["projection"]
    print(
        f"E5 classification selection {report['status']}: "
        f"eligible={projection['eligible_binding_subjects']} "
        f"accepted={projection['accepted_binding_subjects']} "
        f"selected={projection['selected_facts']}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
