#!/usr/bin/env python3
"""Independently audit an immutable E5 selected-fact artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from compile_selected_facts import DEFAULT_POLICY, atomic_json, file_sha256, load_json


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> int:
    return int(con.execute(sql, params or []).fetchone()[0] or 0)


def audit_artifact(artifact: Path, policy_path: Path) -> dict[str, Any]:
    artifact = artifact.resolve(strict=True)
    manifest_path = artifact / "manifest.json"
    database = artifact / "selected_facts.duckdb"
    manifest = load_json(manifest_path)
    policy = load_json(policy_path)
    build_id = artifact.name
    if manifest.get("build_id") != build_id or not database.is_file():
        raise ValueError(f"selected-fact artifact identity is invalid: {artifact}")

    con = duckdb.connect(str(database), read_only=True)
    checks: dict[str, int] = {}
    try:
        report_counts = manifest.get("report", {}).get("table_counts", {})
        for table in [
            "selection_source_accounting",
            "evidence_object_bindings",
            "parameter_set_selection_decisions",
            "selected_facts",
            "selected_fact_derivations",
        ]:
            actual = scalar(con, f'SELECT COUNT(*) FROM "{table}"')
            checks[f"manifest_count_mismatch_{table}"] = int(
                actual != int(report_counts.get(table, -1))
            )
        checks["duplicate_selected_object_quantities"] = scalar(
            con,
            "SELECT COALESCE(SUM(n-1),0) FROM ("
            "SELECT COUNT(*) n FROM selected_facts "
            "GROUP BY object_type,stable_object_key,quantity_key HAVING COUNT(*)>1)",
        )
        checks["selected_source_facts_without_evidence"] = scalar(
            con,
            "SELECT COUNT(*) FROM selected_facts WHERE fact_status='source_selected' "
            "AND (evidence_build_id IS NULL OR evidence_id IS NULL OR parameter_set_id IS NULL)",
        )
        checks["derived_facts_without_derivation"] = scalar(
            con,
            "SELECT COUNT(*) FROM selected_facts f LEFT JOIN selected_fact_derivations d "
            "ON d.output_selected_fact_id=f.selected_fact_id "
            "WHERE f.fact_status='derived' AND d.derivation_id IS NULL",
        )
        checks["duplicate_binding_ids"] = scalar(
            con,
            "SELECT COUNT(*)-COUNT(DISTINCT binding_id) FROM evidence_object_bindings",
        )
        checks["invalid_binding_statuses"] = scalar(
            con,
            "SELECT COUNT(*) FROM evidence_object_bindings "
            "WHERE binding_status NOT IN ('accepted','missing','ambiguous')",
        )
        checks["accepted_bindings_without_targets"] = scalar(
            con,
            "SELECT COUNT(*) FROM evidence_object_bindings "
            "WHERE binding_status='accepted' "
            "AND (canonical_object_node_key IS NULL OR stable_object_key IS NULL)",
        )
        checks["unresolved_bindings_with_targets"] = scalar(
            con,
            "SELECT COUNT(*) FROM evidence_object_bindings "
            "WHERE binding_status<>'accepted' "
            "AND (canonical_object_node_key IS NOT NULL OR stable_object_key IS NOT NULL "
            "OR system_stable_object_key IS NOT NULL)",
        )
        for source in policy.get("selection_sources") or []:
            source_id = str(source["source_id"])
            row = con.execute(
                "SELECT eligible_source_records,accepted_current_bindings,selected_facts "
                "FROM selection_source_accounting WHERE source_id=?",
                [source_id],
            ).fetchone()
            checks[f"missing_source_accounting_{source_id}"] = int(row is None)
            if row is None:
                continue
            checks[f"eligible_floor_{source_id}"] = int(
                int(row[0]) < int(source.get("minimum_eligible_records") or 1)
            )
            checks[f"binding_floor_{source_id}"] = int(
                int(row[1]) < int(source.get("minimum_accepted_bindings") or 1)
            )
            checks[f"selected_fact_floor_{source_id}"] = int(
                int(row[2]) < int(source.get("minimum_selected_facts") or 1)
            )
            outcomes = dict(
                con.execute(
                    "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                    "WHERE source_id=? GROUP BY 1",
                    [source_id],
                ).fetchall()
            )
            checks[f"binding_outcome_accounting_{source_id}"] = abs(
                int(row[0]) - sum(int(value) for value in outcomes.values())
            )
            checks[f"accepted_binding_accounting_{source_id}"] = abs(
                int(row[1]) - int(outcomes.get("accepted", 0))
            )

        fact_rows = dict(
            con.execute(
                "SELECT quantity_key,COUNT(*) FROM selected_facts GROUP BY 1"
            ).fetchall()
        )
        decision_rows = dict(
            con.execute(
                "SELECT quantity_group,COUNT(*) FROM parameter_set_selection_decisions GROUP BY 1"
            ).fetchall()
        )
    finally:
        con.close()

    declared = manifest.get("report", {}).get("partition_exports") or {}
    declared_facts = declared.get("selected_facts") or {}
    declared_decisions = declared.get("selection_decisions") or {}
    checks["missing_fact_partition_declarations"] = sum(
        1 for key in fact_rows if f"selected_facts__{key}.parquet" not in declared_facts
    )
    checks["missing_decision_partition_declarations"] = sum(
        1 for key in decision_rows if f"selection_decisions__{key}.parquet" not in declared_decisions
    )
    checks["missing_fact_partition_files"] = sum(
        1 for key in fact_rows if not (artifact / f"selected_facts__{key}.parquet").is_file()
    )
    checks["missing_decision_partition_files"] = sum(
        1 for key in decision_rows if not (artifact / f"selection_decisions__{key}.parquet").is_file()
    )
    checks["declared_fact_partition_row_mismatch"] = sum(
        1
        for key, count in fact_rows.items()
        if int(declared_facts.get(f"selected_facts__{key}.parquet", -1)) != int(count)
    )
    checks["declared_decision_partition_row_mismatch"] = sum(
        1
        for key, count in decision_rows.items()
        if int(declared_decisions.get(f"selection_decisions__{key}.parquet", -1)) != int(count)
    )
    failing = {key: value for key, value in checks.items() if value}
    return {
        "schema_version": "spacegate.selected_fact_artifact_audit.v1",
        "status": "fail" if failing else "pass",
        "build_id": build_id,
        "artifact": str(artifact),
        "database": str(database),
        "database_bytes": database.stat().st_size,
        "database_sha256": file_sha256(database),
        "manifest_sha256": file_sha256(manifest_path),
        "policy_version": policy["policy_version"],
        "checks": checks,
        "failing_checks": failing,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit_artifact(args.artifact, args.policy)
    atomic_json(args.report, report)
    print(
        f"selected-fact artifact audit {report['status']}: {report['build_id']} "
        f"failures={len(report['failing_checks'])}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
