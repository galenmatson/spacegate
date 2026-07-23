#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


REQUIRED_ARM_TABLES = [
    "wise_sources",
    "catwise_sources",
    "allwise_sources",
    "infrared_source_matches",
    "infrared_photometry",
    "infrared_motion_evidence",
    "infrared_candidate_queue",
]
OPTIONAL_ARM_TABLES = ["infrared_image_products"]
VALID_CATALOGS = {"catwise", "allwise"}
VALID_CONFIDENCE = {"high", "medium", "low", "candidate", ""}
VALID_CONFLICT = {
    "accepted_match",
    "ambiguous_candidate",
    "duplicate_source_collision",
    "excluded_outside_acceptance",
    "candidate",
    "conflict",
    "quarantined",
    "",
}


def has_table(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        """
        select 1
        from information_schema.tables
        where table_schema = 'main' and table_name = ?
        limit 1
        """,
        [table],
    ).fetchone()
    return row is not None


def count_rows(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(con.execute(f"select count(*) from {table}").fetchone()[0] or 0)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Spacegate WISE/CatWISE/AllWISE evidence policy.")
    parser.add_argument("--core-db", required=True)
    parser.add_argument("--arm-db", required=True)
    parser.add_argument("--expect-matches-min", type=int, default=0)
    parser.add_argument("--cache-root", default="")
    args = parser.parse_args()

    core_db = Path(args.core_db)
    arm_db = Path(args.arm_db)
    failures: list[str] = []
    report: dict[str, Any] = {
        "schema_version": "wise_evidence_verification_v1",
        "core_db": str(core_db),
        "arm_db": str(arm_db),
        "policy": "WISE/CatWISE/AllWISE rows are ARM evidence and cache products, not core inventory backbones.",
    }
    if not core_db.exists():
        raise SystemExit(f"core DB not found: {core_db}")
    if not arm_db.exists():
        raise SystemExit(f"arm DB not found: {arm_db}")

    core = duckdb.connect(str(core_db), read_only=True)
    arm = duckdb.connect(str(arm_db), read_only=True)
    try:
        arm.execute(f"attach {sql_literal(str(core_db.resolve()))} as verify_core (read_only)")
        missing_tables = [
            table for table in REQUIRED_ARM_TABLES if not has_table(arm, table)
        ]
        if missing_tables:
            failures.append(f"missing ARM WISE tables: {', '.join(missing_tables)}")
        counts = {
            table: count_rows(arm, table)
            for table in [*REQUIRED_ARM_TABLES, *OPTIONAL_ARM_TABLES]
            if table not in missing_tables
            and has_table(arm, table)
        }
        report["arm_counts"] = counts
        report["optional_tables"] = {
            table: "present" if has_table(arm, table) else "not_materialized"
            for table in OPTIONAL_ARM_TABLES
        }
        if counts.get("infrared_source_matches", 0) < int(args.expect_matches_min):
            failures.append(
                "infrared_source_matches below expectation: "
                f"{counts.get('infrared_source_matches', 0)} < {args.expect_matches_min}"
            )

        core_catalog_rows = {}
        for table in ["systems", "stars", "planets"]:
            if has_table(core, table):
                core_catalog_rows[table] = int(
                    core.execute(
                        f"""
                        select count(*)
                        from {table}
                        where lower(coalesce(source_catalog, '')) in
                          ('catwise', 'catwise2020', 'allwise', 'wise', 'wise_allwise')
                        """
                    ).fetchone()[0]
                    or 0
                )
        report["core_wise_source_catalog_rows"] = core_catalog_rows
        leaked = {table: count for table, count in core_catalog_rows.items() if count}
        if leaked:
            failures.append(f"WISE-like source_catalog values leaked into core: {leaked}")

        if "infrared_source_matches" not in missing_tables:
            invalid_rows = arm.execute(
                """
                select count(*)
                from infrared_source_matches
                where lower(coalesce(source_catalog, '')) not in ('catwise', 'allwise')
                   or lower(coalesce(confidence_tier, '')) not in ('high', 'medium', 'low', 'candidate', '')
                   or lower(coalesce(conflict_status, '')) not in (
                     'accepted_match','ambiguous_candidate','duplicate_source_collision',
                     'excluded_outside_acceptance','candidate','conflict','quarantined',''
                   )
                   or target_type is null
                   or target_id is null
                   or system_id is null
                """
            ).fetchone()[0]
            report["invalid_match_rows"] = int(invalid_rows or 0)
            if invalid_rows:
                failures.append(f"invalid infrared_source_matches rows: {invalid_rows}")

            target_mismatches = arm.execute(
                """
                select count(*)
                from infrared_source_matches i
                left join verify_core.stars st
                  on lower(i.target_type) = 'star' and st.star_id = i.target_id
                left join verify_core.systems sy
                  on lower(i.target_type) = 'system' and sy.system_id = i.target_id
                where coalesce(st.system_id, sy.system_id) is null
                   or i.system_id <> coalesce(st.system_id, sy.system_id)
                   or i.stable_object_key <> coalesce(st.stable_object_key, sy.stable_object_key)
                """
            ).fetchone()[0]
            report["canonical_target_mismatches"] = int(target_mismatches or 0)
            if target_mismatches:
                failures.append(
                    f"infrared_source_matches canonical target mismatches: {target_mismatches}"
                )

        for table in ("infrared_photometry", "infrared_motion_evidence"):
            if table in missing_tables:
                continue
            target_mismatches = arm.execute(
                f"""
                select count(*)
                from {table} i
                left join verify_core.stars st
                  on lower(i.target_type) = 'star' and st.star_id = i.target_id
                left join verify_core.systems sy
                  on lower(i.target_type) = 'system' and sy.system_id = i.target_id
                where coalesce(st.system_id, sy.system_id) is null
                   or i.system_id <> coalesce(st.system_id, sy.system_id)
                """
            ).fetchone()[0]
            report[f"{table}_canonical_target_mismatches"] = int(target_mismatches or 0)
            if target_mismatches:
                failures.append(f"{table} canonical target mismatches: {target_mismatches}")

        if "infrared_candidate_queue" not in missing_tables:
            invalid_candidates = arm.execute(
                """
                select count(*)
                from infrared_candidate_queue
                where lower(coalesce(candidate_status, '')) not in
                  ('needs_review', 'accepted', 'rejected', 'quarantined', '')
                   or lower(coalesce(source_catalog, '')) not in ('catwise', 'allwise', '')
                   or (candidate_status is not null and source_key is null)
                """
            ).fetchone()[0]
            report["invalid_candidate_rows"] = int(invalid_candidates or 0)
            if invalid_candidates:
                failures.append(f"invalid infrared_candidate_queue rows: {invalid_candidates}")

            nearest_target_mismatches = arm.execute(
                """
                select count(*)
                from infrared_candidate_queue i
                left join verify_core.stars st
                  on lower(i.nearest_target_type) = 'star' and st.star_id = i.nearest_target_id
                left join verify_core.systems sy
                  on lower(i.nearest_target_type) = 'system' and sy.system_id = i.nearest_target_id
                where coalesce(st.system_id, sy.system_id) is not null
                  and (
                    i.nearest_system_id <> coalesce(st.system_id, sy.system_id)
                    or i.nearest_stable_object_key <> coalesce(st.stable_object_key, sy.stable_object_key)
                  )
                """
            ).fetchone()[0]
            report["candidate_nearest_target_mismatches"] = int(nearest_target_mismatches or 0)
            if nearest_target_mismatches:
                failures.append(
                    "infrared_candidate_queue canonical nearest-target mismatches: "
                    f"{nearest_target_mismatches}"
                )

        if args.cache_root:
            cache_root = Path(args.cache_root)
            cache_files = [path for path in cache_root.rglob("*") if path.is_file()] if cache_root.exists() else []
            report["cache"] = {
                "cache_root": str(cache_root),
                "file_count": len(cache_files),
                "total_bytes": sum(path.stat().st_size for path in cache_files),
            }
    finally:
        arm.execute("detach verify_core")
        core.close()
        arm.close()

    report["status"] = "failed" if failures else "ok"
    report["failures"] = failures
    print(json.dumps(report, indent=2), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
