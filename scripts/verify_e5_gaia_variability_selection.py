#!/usr/bin/env python3
"""Verify deterministic E5 selection from Gaia DR3 variability evidence."""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

import duckdb

import compile_selected_facts as compiler


SOURCE_ID = "gaia.dr3.variability"
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e5_gaia_variability_selection_verification.json"
)
DEFAULT_SCRATCH = Path("/mnt/space/spacegate/e5-focused-verification")


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


def timed(
    timings: list[dict[str, Any]], phase: str, operation: Callable[[], Any]
) -> Any:
    started = time.monotonic()
    result = operation()
    timings.append(
        {"phase": phase, "wall_seconds": round(time.monotonic() - started, 6)}
    )
    return result


def table_fingerprint(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    identity_column: str,
    hash_columns: list[str],
) -> dict[str, Any]:
    columns = ",".join(hash_columns)
    row = con.execute(
        f"SELECT COUNT(*),CAST(coalesce(bit_xor(hash({columns})),0) AS VARCHAR),"
        f"MIN({identity_column}),MAX({identity_column}) FROM {table}"
    ).fetchone()
    return {
        "rows": int(row[0]),
        "xor_hash64": str(row[1]),
        "minimum_identity": row[2],
        "maximum_identity": row[3],
    }


def compile_projection(
    *,
    state_dir: Path,
    scratch_root: Path,
    policy: dict[str, Any],
    release_manifest: dict[str, Any],
    pass_name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source = selected_source(policy)
    member = compiler.member_by_source(release_manifest)[SOURCE_ID]
    artifact = state_dir / str(member["artifact_path"])
    evidence_db = artifact / str(member["database"])
    manifest_path = artifact / "manifest.json"
    if compiler.file_sha256(manifest_path) != member["manifest_sha256"]:
        raise ValueError("Gaia variability E4 member manifest changed")
    if evidence_db.stat().st_size != int(member["database_bytes"]):
        raise ValueError("Gaia variability E4 database size changed")
    if compiler.file_sha256(evidence_db) != member["database_sha256"]:
        raise ValueError("Gaia variability E4 database checksum changed")

    identity_db = (
        state_dir
        / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"])
        / "identity_graph.duckdb"
    )
    scratch_root.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix=f"{pass_name}-", dir=scratch_root))
    database = run_dir / "focused_selection.duckdb"
    spill = run_dir / "spill"
    spill.mkdir()
    timings: list[dict[str, Any]] = []
    con = duckdb.connect(
        str(database),
        config={
            "memory_limit": "16GB",
            "threads": "4",
            "temp_directory": str(spill),
            "preserve_insertion_order": "false",
        },
    )
    try:
        timed(
            timings,
            "schema",
            lambda: (compiler.create_schema(con), compiler.create_candidate_table(con)),
        )
        timed(
            timings,
            "attach_inputs",
            lambda: (
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(identity_db))} AS identity (READ_ONLY)"
                ),
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(evidence_db))} AS e4_source (READ_ONLY)"
                ),
            ),
        )
        release_id = str(member["release_ids"][SOURCE_ID])
        eligible, accepted = timed(
            timings,
            "binding",
            lambda: compiler.create_binding(
                con,
                source=source,
                source_alias="e4_source",
                member=member,
                release_id=release_id,
            ),
        )
        timed(
            timings,
            "direct_selection",
            lambda: compiler.insert_candidates(
                con,
                source=source,
                source_alias="e4_source",
                member=member,
                release_id=release_id,
            ),
        )

        outcomes = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        facts_by_group = {
            str(group): int(count)
            for group, count in con.execute(
                "SELECT quantity_group,COUNT(*) FROM selected_facts GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        facts_by_quantity = {
            str(quantity): int(count)
            for quantity, count in con.execute(
                "SELECT quantity_key,COUNT(*) FROM selected_facts GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        decisions_by_group = {
            str(group): int(count)
            for group, count in con.execute(
                "SELECT quantity_group,COUNT(*) FROM parameter_set_selection_decisions "
                "GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        selected_facts = int(
            con.execute("SELECT COUNT(*) FROM selected_facts").fetchone()[0]
        )
        selected_decisions = int(
            con.execute(
                "SELECT COUNT(*) FROM parameter_set_selection_decisions"
            ).fetchone()[0]
        )
        checks = {
            "binding_subject_accounting": int(
                con.execute("SELECT COUNT(*) FROM evidence_object_bindings").fetchone()[0]
            )
            - eligible,
            "accepted_binding_accounting": outcomes.get("accepted", 0) - accepted,
            "duplicate_object_kind_bindings": int(
                con.execute(
                    "SELECT coalesce(SUM(n-1),0) FROM ("
                    "SELECT b.stable_object_key,ps.parameter_set_kind,COUNT(*) n "
                    "FROM e4_source.variability_activity_rotation_parameter_sets ps "
                    "JOIN evidence_object_bindings b ON b.source_record_id=ps.source_record_id "
                    "AND b.binding_status='accepted' GROUP BY 1,2 HAVING COUNT(*)>1)"
                ).fetchone()[0]
            ),
            "duplicate_decisions": int(
                con.execute(
                    "SELECT coalesce(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM "
                    "parameter_set_selection_decisions GROUP BY stable_object_key,"
                    "quantity_group HAVING COUNT(*)>1)"
                ).fetchone()[0]
            ),
            "duplicate_selected_quantities": int(
                con.execute(
                    "SELECT coalesce(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM "
                    "selected_facts GROUP BY stable_object_key,quantity_key HAVING COUNT(*)>1)"
                ).fetchone()[0]
            ),
            "facts_without_accepted_binding": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts f WHERE NOT EXISTS ("
                    "SELECT 1 FROM evidence_object_bindings b WHERE b.binding_id=f.binding_id "
                    "AND b.binding_status='accepted')"
                ).fetchone()[0]
            ),
            "facts_with_wrong_parameter_set_kind": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts f JOIN "
                    "e4_source.variability_activity_rotation_parameter_sets ps "
                    "ON ps.evidence_id=f.parameter_set_id WHERE NOT ("
                    "(f.quantity_group='stellar_rotation_modulation' AND "
                    "ps.parameter_set_kind='gaia_dr3_rotation_modulation_solution') OR "
                    "(f.quantity_group IN ('stellar_variability_summary',"
                    "'stellar_variability_classification_membership') AND "
                    "ps.parameter_set_kind='gaia_dr3_variability_summary'))"
                ).fetchone()[0]
            ),
            "categorical_flags_with_numeric_values": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts WHERE "
                    "quantity_group='stellar_variability_classification_membership' "
                    "AND normalized_value IS NOT NULL"
                ).fetchone()[0]
            ),
            "categorical_flags_outside_boolean_domain": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts WHERE "
                    "quantity_group='stellar_variability_classification_membership' "
                    "AND lower(value_raw) NOT IN ('true','false')"
                ).fetchone()[0]
            ),
            "numeric_facts_without_numeric_values": int(
                con.execute(
                    "SELECT COUNT(*) FROM selected_facts WHERE "
                    "quantity_group<>'stellar_variability_classification_membership' "
                    "AND normalized_value IS NULL"
                ).fetchone()[0]
            ),
        }
        fingerprints = {
            "bindings": table_fingerprint(
                con,
                table="evidence_object_bindings",
                identity_column="binding_id",
                hash_columns=[
                    "binding_id",
                    "binding_status",
                    "coalesce(stable_object_key,'')",
                    "binding_reason",
                ],
            ),
            "decisions": table_fingerprint(
                con,
                table="parameter_set_selection_decisions",
                identity_column="decision_id",
                hash_columns=[
                    "decision_id",
                    "stable_object_key",
                    "quantity_group",
                    "selected_parameter_set_id",
                    "selected_quantity_count",
                ],
            ),
            "facts": table_fingerprint(
                con,
                table="selected_facts",
                identity_column="selected_fact_id",
                hash_columns=[
                    "selected_fact_id",
                    "stable_object_key",
                    "quantity_key",
                    "value_raw",
                    "coalesce(normalized_value,'NaN'::DOUBLE)",
                    "evidence_id",
                    "binding_id",
                ],
            ),
        }
        projection = {
            "source_id": SOURCE_ID,
            "release_id": release_id,
            "evidence_build_id": str(member["build_id"]),
            "eligible_binding_subjects": eligible,
            "accepted_binding_subjects": accepted,
            "binding_outcomes": outcomes,
            "selected_decisions": selected_decisions,
            "selected_facts": selected_facts,
            "decisions_by_group": decisions_by_group,
            "facts_by_group": facts_by_group,
            "facts_by_quantity": facts_by_quantity,
            "checks": checks,
            "fingerprints": fingerprints,
            "logical_content_sha256": compiler.stable_sha256(fingerprints),
        }
    finally:
        con.close()
        shutil.rmtree(run_dir, ignore_errors=True)
    return projection, timings


def verify(
    *, state_dir: Path, policy_path: Path, scratch_root: Path
) -> dict[str, Any]:
    policy = compiler.load_json(policy_path)
    _, release_manifest = compiler.release_set_paths(state_dir, policy)
    compiler.validate_policy(policy, release_manifest)
    source = selected_source(policy)
    first, first_timings = compile_projection(
        state_dir=state_dir,
        scratch_root=scratch_root,
        policy=policy,
        release_manifest=release_manifest,
        pass_name="first",
    )
    second, second_timings = compile_projection(
        state_dir=state_dir,
        scratch_root=scratch_root,
        policy=policy,
        release_manifest=release_manifest,
        pass_name="second",
    )
    failures = {
        key: value
        for key, value in {
            "eligible_floor": first["eligible_binding_subjects"]
            < int(source["minimum_eligible_records"]),
            "accepted_floor": first["accepted_binding_subjects"]
            < int(source["minimum_accepted_bindings"]),
            "selected_fact_floor": first["selected_facts"]
            < int(source["minimum_selected_facts"]),
            "scientific_checks": any(first["checks"].values()),
            "deterministic_projection": first != second,
        }.items()
        if value
    }
    return {
        "schema_version": "spacegate.e5_gaia_variability_selection_verification.v1",
        "status": "fail" if failures else "pass",
        "policy_version": policy["policy_version"],
        "identity_graph_id": policy["identity_graph_id"],
        "failures": failures,
        "projection": first,
        "timings": {"first_pass": first_timings, "second_pass": second_timings},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument("--scratch-root", type=Path, default=DEFAULT_SCRATCH)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = verify(
        state_dir=args.state_dir.resolve(),
        policy_path=args.policy.resolve(),
        scratch_root=args.scratch_root.resolve(),
    )
    compiler.atomic_json(args.report, report)
    projection = report["projection"]
    print(
        f"E5 Gaia variability selection {report['status']}: "
        f"eligible={projection['eligible_binding_subjects']} "
        f"accepted={projection['accepted_binding_subjects']} "
        f"decisions={projection['selected_decisions']} "
        f"facts={projection['selected_facts']}"
    )
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
