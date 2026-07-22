#!/usr/bin/env python3
"""Compile and audit only the E5 NASA host-star selection program."""

from __future__ import annotations

import argparse
import os
import resource
import shutil
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import duckdb

import compile_selected_facts as compiler


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ID = "nasa_exoplanet_archive.planetary_systems"


def allocated_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(
        entry.stat().st_blocks * 512
        for entry in path.rglob("*")
        if entry.is_file()
    )


def cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return float(usage.ru_utime + usage.ru_stime)


@contextmanager
def timed_phase(
    rows: list[dict[str, Any]], phase: str, *, work_path: Path
) -> Iterator[dict[str, Any]]:
    started_at = compiler.utc_now()
    started = time.monotonic()
    started_cpu = cpu_seconds()
    started_bytes = allocated_bytes(work_path)
    details: dict[str, Any] = {}
    status = "pass"
    try:
        yield details
    except Exception:
        status = "fail"
        raise
    finally:
        rows.append(
            {
                "phase": phase,
                "status": status,
                "started_at": started_at,
                "finished_at": compiler.utc_now(),
                "wall_seconds": round(time.monotonic() - started, 6),
                "cpu_seconds": round(cpu_seconds() - started_cpu, 6),
                "process_peak_rss_kib": int(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                ),
                "work_allocated_bytes": allocated_bytes(work_path),
                "work_allocated_bytes_delta": allocated_bytes(work_path)
                - started_bytes,
                "details": details,
            }
        )


def verify(
    *,
    state_dir: Path,
    policy_path: Path,
    work_dir: Path,
    report_path: Path,
    memory_limit: str,
    threads: int,
    verify_input_hashes: bool,
    reference_selected_db: Path | None,
) -> dict[str, Any]:
    policy = compiler.load_json(policy_path)
    manifest_path, release_manifest = compiler.release_set_paths(state_dir, policy)
    compiler.validate_policy(policy, release_manifest)
    programs = [
        dict(source)
        for source in policy["selection_sources"]
        if source["source_id"] == SOURCE_ID and source["object_type"] == "star"
    ]
    if len(programs) != 1:
        raise ValueError(f"expected one NASA host-star program, found {len(programs)}")
    source = programs[0]
    source["_policy_version"] = str(policy["policy_version"])
    members = compiler.member_by_source(release_manifest)
    member = members[SOURCE_ID]
    release_id = str(member["release_ids"][SOURCE_ID])
    evidence_dir = state_dir / str(member["artifact_path"])
    evidence_db = evidence_dir / str(member["database"])
    identity_dir = (
        state_dir
        / "derived/evidence_lake_v2/identity"
        / str(policy["identity_graph_id"])
    )
    identity_db = identity_dir / "identity_graph.duckdb"
    core_db = (
        state_dir
        / "out"
        / str(policy["canonical_reference_build_id"])
        / "core.duckdb"
    )

    work_dir.mkdir(parents=True, exist_ok=True)
    database = work_dir / "nasa_host_selection.duckdb"
    database.unlink(missing_ok=True)
    phases: list[dict[str, Any]] = []
    started = time.monotonic()
    started_cpu = cpu_seconds()

    if verify_input_hashes:
        with timed_phase(phases, "immutable_input_verification", work_path=work_dir) as details:
            attestor = compiler.FileHashAttestor()
            details.update(
                compiler.verify_e4_member_inputs(
                    state_dir=state_dir,
                    sources=[source],
                    members=members,
                    attestor=attestor,
                    workers=1,
                )
            )
    else:
        phases.append(
            {
                "phase": "immutable_input_verification",
                "status": "skipped",
                "details": {"reason": "disabled by --skip-input-hashes"},
            }
        )

    con: duckdb.DuckDBPyConnection | None = None
    try:
        with timed_phase(phases, "database_and_schema", work_path=work_dir):
            con = duckdb.connect(
                str(database),
                config={
                    "memory_limit": memory_limit,
                    "threads": str(max(1, threads)),
                    "temp_directory": str(work_dir / "spill"),
                    "preserve_insertion_order": "false",
                },
            )
            compiler.create_schema(con)
            compiler.create_candidate_table(con)
            con.execute(
                f"ATTACH {compiler.sql_literal(str(evidence_db))} AS e4 (READ_ONLY)"
            )
            con.execute(
                f"ATTACH {compiler.sql_literal(str(identity_db))} AS identity (READ_ONLY)"
            )
            con.execute(
                f"ATTACH {compiler.sql_literal(str(core_db))} AS core (READ_ONLY)"
            )

        with timed_phase(phases, "host_identity_binding", work_path=work_dir) as details:
            eligible, accepted = compiler.create_binding(
                con,
                source=source,
                source_alias="e4",
                member=member,
                release_id=release_id,
            )
            outcomes = {
                str(status): int(count)
                for status, count in con.execute(
                    "SELECT binding_status,COUNT(*) FROM evidence_object_bindings "
                    "WHERE source_id=? AND object_type='star' GROUP BY 1 ORDER BY 1",
                    [SOURCE_ID],
                ).fetchall()
            }
            details.update(
                {
                    "eligible_binding_subjects": eligible,
                    "accepted_bindings": accepted,
                    "binding_outcomes": outcomes,
                }
            )
            expected = source.get("expected_binding_outcomes")
            if expected is not None and outcomes != expected:
                raise ValueError(
                    f"NASA host binding outcomes changed: expected={expected}:actual={outcomes}"
                )

        with timed_phase(phases, "host_candidate_insertion", work_path=work_dir) as details:
            compiler.insert_candidates(
                con,
                source=source,
                source_alias="e4",
                member=member,
                release_id=release_id,
            )
            details["fact_candidates"] = int(
                con.execute("SELECT COUNT(*) FROM fact_candidates").fetchone()[0]
            )

        with timed_phase(phases, "host_parameter_set_selection", work_path=work_dir) as details:
            compiler.select_parameter_sets(con, str(policy["policy_version"]))
            details["selection_decisions"] = int(
                con.execute(
                    "SELECT COUNT(*) FROM parameter_set_selection_decisions"
                ).fetchone()[0]
            )
            details["selected_facts"] = int(
                con.execute("SELECT COUNT(*) FROM selected_facts").fetchone()[0]
            )

        with timed_phase(phases, "host_selection_audit", work_path=work_dir) as details:
            quantity_counts = {
                str(quantity): int(count)
                for quantity, count in con.execute(
                    "SELECT quantity_key,COUNT(*) FROM selected_facts "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            table_counts = {
                str(source_table): int(count)
                for source_table, count in con.execute(
                    "SELECT sr.source_table,COUNT(*) FROM selected_facts f "
                    "JOIN e4.source_records sr USING(source_record_id) "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            decision_authority_counts = [
                {
                    "quantity_group": str(group),
                    "source_table": str(source_table),
                    "authority_rank": int(rank),
                    "decisions": int(count),
                }
                for group, source_table, rank, count in con.execute(
                    "SELECT d.quantity_group,sr.source_table,d.authority_rank,COUNT(*) "
                    "FROM parameter_set_selection_decisions d "
                    "JOIN e4.source_records sr "
                    "ON sr.source_record_id=d.selected_source_record_id "
                    "GROUP BY 1,2,3 ORDER BY 1,2,3"
                ).fetchall()
            ]
            checks = {
                "non_star_facts": int(
                    con.execute(
                        "SELECT COUNT(*) FROM selected_facts WHERE object_type<>'star'"
                    ).fetchone()[0]
                ),
                "facts_without_accepted_binding": int(
                    con.execute(
                        "SELECT COUNT(*) FROM selected_facts f WHERE NOT EXISTS ("
                        "SELECT 1 FROM evidence_object_bindings b "
                        "WHERE b.binding_id=f.binding_id AND b.binding_status='accepted' "
                        "AND b.object_type=f.object_type AND b.source_id=f.source_id)"
                    ).fetchone()[0]
                ),
                "duplicate_selected_quantity": int(
                    con.execute(
                        "SELECT COUNT(*) FROM (SELECT stable_object_key,quantity_key "
                        "FROM selected_facts GROUP BY 1,2 HAVING COUNT(*)>1)"
                    ).fetchone()[0]
                ),
                "facts_without_evidence_lineage": int(
                    con.execute(
                        "SELECT COUNT(*) FROM selected_facts WHERE evidence_id IS NULL "
                        "OR parameter_set_id IS NULL OR binding_id IS NULL"
                    ).fetchone()[0]
                ),
            }
            details.update(
                {
                    "quantity_counts": quantity_counts,
                    "source_table_counts": table_counts,
                    "decision_authority_counts": decision_authority_counts,
                    "checks": checks,
                }
            )
            if any(checks.values()) or not quantity_counts:
                raise ValueError(f"NASA host selection audit failed: {checks}")

        authority_impact: dict[str, Any] | None = None
        if reference_selected_db is not None:
            with timed_phase(
                phases, "reference_authority_impact", work_path=work_dir
            ) as details:
                con.execute(
                    f"ATTACH {compiler.sql_literal(str(reference_selected_db.resolve()))} "
                    "AS reference_selected (READ_ONLY)"
                )
                con.execute(
                    """
                    CREATE OR REPLACE TEMP TABLE authority_impact_winners AS
                    SELECT n.object_type,n.stable_object_key,n.quantity_group,
                           CASE
                             WHEN r.decision_id IS NULL THEN 'nasa_fill'
                             WHEN n.authority_rank<r.authority_rank THEN 'nasa_outranks'
                             WHEN n.authority_rank>r.authority_rank THEN 'reference_wins'
                             ELSE 'authority_tie'
                           END winner
                    FROM parameter_set_selection_decisions n
                    LEFT JOIN reference_selected.parameter_set_selection_decisions r
                      USING(object_type,stable_object_key,quantity_group)
                    """
                )
                decision_outcomes = [
                    {
                        "quantity_group": str(group),
                        "outcome": str(outcome),
                        "decisions": int(count),
                    }
                    for group, outcome, count in con.execute(
                        "SELECT quantity_group,winner,COUNT(*) "
                        "FROM authority_impact_winners GROUP BY 1,2 ORDER BY 1,2"
                    ).fetchall()
                ]
                authority_ties = sum(
                    row["decisions"]
                    for row in decision_outcomes
                    if row["outcome"] == "authority_tie"
                )
                removed_facts = [
                    {
                        "source_id": str(source_id),
                        "quantity_group": str(group),
                        "quantity_key": str(quantity),
                        "facts": int(count),
                    }
                    for source_id, group, quantity, count in con.execute(
                        "SELECT f.source_id,f.quantity_group,f.quantity_key,COUNT(*) "
                        "FROM reference_selected.selected_facts f "
                        "JOIN authority_impact_winners w "
                        "USING(object_type,stable_object_key,quantity_group) "
                        "WHERE w.winner IN ('nasa_fill','nasa_outranks') "
                        "GROUP BY 1,2,3 ORDER BY 1,2,3"
                    ).fetchall()
                ]
                added_facts = [
                    {
                        "quantity_group": str(group),
                        "quantity_key": str(quantity),
                        "facts": int(count),
                    }
                    for group, quantity, count in con.execute(
                        "SELECT f.quantity_group,f.quantity_key,COUNT(*) "
                        "FROM selected_facts f JOIN authority_impact_winners w "
                        "USING(object_type,stable_object_key,quantity_group) "
                        "WHERE w.winner IN ('nasa_fill','nasa_outranks') "
                        "GROUP BY 1,2 ORDER BY 1,2"
                    ).fetchall()
                ]
                source_count_deltas = [
                    {
                        "source_id": str(source_id),
                        "reference_selected_facts": int(reference_count),
                        "removed_facts": int(removed),
                        "expected_selected_facts": int(reference_count - removed),
                    }
                    for source_id, reference_count, removed in con.execute(
                        "WITH removed AS ("
                        " SELECT f.source_id,COUNT(*) n "
                        " FROM reference_selected.selected_facts f "
                        " JOIN authority_impact_winners w "
                        " USING(object_type,stable_object_key,quantity_group) "
                        " WHERE w.winner IN ('nasa_fill','nasa_outranks') GROUP BY 1),"
                        " totals AS (SELECT source_id,COUNT(*) n "
                        " FROM reference_selected.selected_facts "
                        " WHERE fact_status='source_selected' GROUP BY 1) "
                        "SELECT t.source_id,t.n,r.n FROM totals t JOIN removed r USING(source_id) "
                        "ORDER BY r.n DESC,t.source_id"
                    ).fetchall()
                ]
                authority_impact = {
                    "reference_database": str(reference_selected_db.resolve()),
                    "decision_outcomes": decision_outcomes,
                    "removed_facts": removed_facts,
                    "added_facts": added_facts,
                    "source_count_deltas": source_count_deltas,
                    "authority_ties": authority_ties,
                }
                details.update(authority_impact)
                if authority_ties:
                    raise ValueError(
                        "NASA authority-impact preflight cannot approximate tied "
                        f"global decisions: {authority_ties}"
                    )

        with timed_phase(phases, "checkpoint", work_path=work_dir):
            con.execute("CHECKPOINT")
    finally:
        if con is not None:
            con.close()

    report = {
        "schema_version": "spacegate.e5_nasa_host_selection_verification.v1",
        "status": "pass",
        "generated_at": compiler.utc_now(),
        "policy_version": policy["policy_version"],
        "source_id": SOURCE_ID,
        "object_type": "star",
        "release_id": release_id,
        "release_set_manifest": str(manifest_path),
        "inputs": {
            "evidence_database": str(evidence_db),
            "identity_database": str(identity_db),
            "core_database": str(core_db),
        },
        "database": str(database),
        "database_bytes": database.stat().st_size,
        "wall_seconds": round(time.monotonic() - started, 6),
        "cpu_seconds": round(cpu_seconds() - started_cpu, 6),
        "process_peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        "phases": phases,
        "binding_outcomes": outcomes,
        "quantity_counts": quantity_counts,
        "source_table_counts": table_counts,
        "decision_authority_counts": decision_authority_counts,
        "integrity_checks": checks,
        "reference_authority_impact": authority_impact,
    }
    compiler.atomic_json(report_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/data/spacegate/state"))
    parser.add_argument("--policy", type=Path, default=compiler.DEFAULT_POLICY)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("/mnt/space/spacegate/e5-nasa-host-verification"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(
            "/data/spacegate/state/reports/evidence_lake_v2/"
            "e5_nasa_host_selection_verification.json"
        ),
    )
    parser.add_argument("--memory-limit", default="24GB")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--skip-input-hashes", action="store_true")
    parser.add_argument(
        "--reference-selected-db",
        type=Path,
        default=Path(
            "/data/spacegate/state/derived/evidence_lake_v2/selected_facts/"
            "current/selected_facts.duckdb"
        ),
    )
    parser.add_argument("--clean-work-dir", action="store_true")
    args = parser.parse_args()
    if args.clean_work_dir:
        shutil.rmtree(args.work_dir, ignore_errors=True)
    report = verify(
        state_dir=args.state_dir,
        policy_path=args.policy,
        work_dir=args.work_dir,
        report_path=args.report,
        memory_limit=args.memory_limit,
        threads=args.threads,
        verify_input_hashes=not args.skip_input_hashes,
        reference_selected_db=args.reference_selected_db,
    )
    print(
        f"NASA host selection verification {report['status']}: "
        f"facts={sum(report['quantity_counts'].values())} "
        f"wall={report['wall_seconds']:.3f}s report={args.report}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
