#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def table_exists(con: duckdb.DuckDBPyConnection, catalog: str, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            select 1
            from information_schema.tables
            where table_catalog = ? and table_schema = 'main' and table_name = ?
            limit 1
            """,
            [catalog, table_name],
        ).fetchone()
    )


def grouped_counts(
    con: duckdb.DuckDBPyConnection,
    *,
    alias: str,
    table_name: str,
    key_expression: str,
) -> dict[str, int]:
    if not table_exists(con, alias, table_name):
        return {}
    return {
        str(key): int(count)
        for key, count in con.execute(
            f"""
            select coalesce(cast({key_expression} as varchar), '(null)'), count(*)::bigint
            from {alias}.{table_name}
            group by 1 order by 1
            """
        ).fetchall()
    }


def count(con: duckdb.DuckDBPyConnection, alias: str, table_name: str) -> int:
    if not table_exists(con, alias, table_name):
        return 0
    return int(con.execute(f"select count(*)::bigint from {alias}.{table_name}").fetchone()[0])


def metadata(con: duckdb.DuckDBPyConnection, alias: str) -> dict[str, str]:
    if not table_exists(con, alias, "build_metadata"):
        return {}
    return {
        str(key): "" if value is None else str(value)
        for key, value in con.execute(
            f"select key, value from {alias}.build_metadata order by key"
        ).fetchall()
    }


def stable_key_delta(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    limit: int,
) -> dict[str, Any]:
    removed_count = int(
        con.execute(
            f"""
            select count(*) from (
              select stable_object_key from baseline.{table_name}
              except
              select stable_object_key from candidate.{table_name}
            )
            """
        ).fetchone()[0]
    )
    added_count = int(
        con.execute(
            f"""
            select count(*) from (
              select stable_object_key from candidate.{table_name}
              except
              select stable_object_key from baseline.{table_name}
            )
            """
        ).fetchone()[0]
    )
    removed = [
        str(row[0])
        for row in con.execute(
            f"""
            select stable_object_key from baseline.{table_name}
            except
            select stable_object_key from candidate.{table_name}
            order by 1 limit {int(limit)}
            """
        ).fetchall()
    ]
    added = [
        str(row[0])
        for row in con.execute(
            f"""
            select stable_object_key from candidate.{table_name}
            except
            select stable_object_key from baseline.{table_name}
            order by 1 limit {int(limit)}
            """
        ).fetchall()
    ]
    return {
        "removed_count": removed_count,
        "added_count": added_count,
        "removed_sample": removed,
        "added_sample": added,
        "sample_limit": int(limit),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two immutable Spacegate science builds.")
    parser.add_argument("--baseline-build-dir", required=True)
    parser.add_argument("--candidate-build-dir", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--sample-limit", type=int, default=100)
    args = parser.parse_args()

    baseline_dir = Path(args.baseline_build_dir).resolve()
    candidate_dir = Path(args.candidate_build_dir).resolve()
    for build_dir in (baseline_dir, candidate_dir):
        for filename in ("core.duckdb", "arm.duckdb"):
            if not (build_dir / filename).is_file():
                raise SystemExit(f"Missing {filename}: {build_dir}")

    con = duckdb.connect(":memory:")
    con.execute(f"attach {sql_literal(str(baseline_dir / 'core.duckdb'))} as baseline (read_only)")
    con.execute(f"attach {sql_literal(str(candidate_dir / 'core.duckdb'))} as candidate (read_only)")
    con.execute(f"attach {sql_literal(str(baseline_dir / 'arm.duckdb'))} as baseline_arm (read_only)")
    con.execute(f"attach {sql_literal(str(candidate_dir / 'arm.duckdb'))} as candidate_arm (read_only)")

    core_tables = (
        "systems",
        "stars",
        "planets",
        "aliases",
        "system_search_terms",
        "object_identifiers",
        "identifier_quarantine",
        "extended_objects",
    )
    baseline_counts = {name: count(con, "baseline", name) for name in core_tables}
    candidate_counts = {name: count(con, "candidate", name) for name in core_tables}
    baseline_supplements = int(
        con.execute(
            "select count(*) from baseline.stars where source_catalog='athyg_accepted_supplement'"
        ).fetchone()[0]
    )
    candidate_supplements = int(
        con.execute(
            "select count(*) from candidate.stars where source_catalog='athyg_accepted_supplement'"
        ).fetchone()[0]
    )
    candidate_identifier_orphans = int(
        con.execute(
            """
            select count(*)
            from candidate.object_identifiers oi
            where (oi.target_type='system' and not exists (select 1 from candidate.systems x where x.system_id=oi.target_id))
               or (oi.target_type='star' and not exists (select 1 from candidate.stars x where x.star_id=oi.target_id))
               or (oi.target_type='planet' and not exists (select 1 from candidate.planets x where x.planet_id=oi.target_id))
            """
        ).fetchone()[0]
    )
    candidate_tic_collisions = int(
        con.execute(
            """
            select count(*) from (
              select id_value_norm
              from candidate.object_identifiers
              where namespace='tic'
              group by id_value_norm
              having count(distinct target_id) > 1
            )
            """
        ).fetchone()[0]
    )

    tess_baseline = grouped_counts(
        con,
        alias="baseline_arm",
        table_name="tess_target_identity",
        key_expression="resolution_status",
    )
    tess_candidate = grouped_counts(
        con,
        alias="candidate_arm",
        table_name="tess_target_identity",
        key_expression="resolution_status",
    )
    toi_baseline = grouped_counts(
        con,
        alias="baseline_arm",
        table_name="toi_current_evidence",
        key_expression="disposition",
    )
    toi_candidate = grouped_counts(
        con,
        alias="candidate_arm",
        table_name="toi_current_evidence",
        key_expression="disposition",
    )

    allowed_tess_statuses = {
        "accepted",
        "missing",
        "excluded",
        "ambiguous",
        "source_missing",
    }
    candidate_tess_count = sum(tess_candidate.values())
    candidate_tess_partition_count = sum(
        value for key, value in tess_candidate.items() if key in allowed_tess_statuses
    )
    candidate_tess_distinct_tic_count = int(
        con.execute(
            "select count(distinct tic_id)::bigint from candidate_arm.tess_target_identity"
        ).fetchone()[0]
    )
    baseline_metadata = metadata(con, "baseline")
    candidate_metadata = metadata(con, "candidate")
    baseline_identifier_namespaces = grouped_counts(
        con,
        alias="baseline",
        table_name="object_identifiers",
        key_expression="namespace",
    )
    candidate_identifier_namespaces = grouped_counts(
        con,
        alias="candidate",
        table_name="object_identifiers",
        key_expression="namespace",
    )
    crosswalk_namespaces = ("gl", "hr", "hyg", "tyc")

    checks = {
        "canonical_planet_count_unchanged": candidate_counts["planets"] == baseline_counts["planets"],
        "candidate_accepted_supplements_zero": candidate_supplements == 0,
        "candidate_accepted_supplements_disabled": candidate_metadata.get(
            "accepted_supplements_enabled"
        )
        == "0",
        "candidate_identifier_orphans_zero": candidate_identifier_orphans == 0,
        "candidate_tic_collisions_zero": candidate_tic_collisions == 0,
        "candidate_crosswalk_identifier_coverage_preserved": all(
            candidate_identifier_namespaces.get(namespace, 0)
            >= baseline_identifier_namespaces.get(namespace, 0)
            for namespace in crosswalk_namespaces
        ),
        "candidate_tess_partition_complete": candidate_tess_count > 0
        and candidate_tess_partition_count == candidate_tess_count,
        "candidate_tess_targets_unique": candidate_tess_distinct_tic_count
        == candidate_tess_count,
    }
    report = {
        "generated_at": utc_now(),
        "baseline_build_dir": str(baseline_dir),
        "candidate_build_dir": str(candidate_dir),
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
        "core_counts": {
            "baseline": baseline_counts,
            "candidate": candidate_counts,
            "delta": {
                name: candidate_counts[name] - baseline_counts[name] for name in core_tables
            },
        },
        "stable_key_deltas": {
            table_name: stable_key_delta(
                con,
                table_name=table_name,
                limit=max(1, int(args.sample_limit)),
            )
            for table_name in ("systems", "stars", "planets")
        },
        "star_source_catalog_counts": {
            "baseline": grouped_counts(
                con, alias="baseline", table_name="stars", key_expression="source_catalog"
            ),
            "candidate": grouped_counts(
                con, alias="candidate", table_name="stars", key_expression="source_catalog"
            ),
        },
        "identifier_namespace_counts": {
            "baseline": baseline_identifier_namespaces,
            "candidate": candidate_identifier_namespaces,
        },
        "integrity": {
            "baseline_accepted_supplement_stars": baseline_supplements,
            "candidate_accepted_supplement_stars": candidate_supplements,
            "candidate_identifier_orphans": candidate_identifier_orphans,
            "candidate_tic_collisions": candidate_tic_collisions,
            "candidate_tess_target_rows": candidate_tess_count,
            "candidate_tess_distinct_tic_ids": candidate_tess_distinct_tic_count,
        },
        "build_metadata": {
            "baseline": baseline_metadata,
            "candidate": candidate_metadata,
        },
        "tess_identity_status_counts": {
            "baseline": tess_baseline,
            "candidate": tess_candidate,
        },
        "toi_disposition_counts": {
            "baseline": toi_baseline,
            "candidate": toi_candidate,
        },
    }
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    con.close()
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
