#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


VALID_CLASSES = {
    "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "WR", "WD",
    "NS", "PULSAR", "MAGNETAR", "BLACK HOLE", "UNKNOWN",
}


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def sql_literal(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the shared hierarchy-leaf stellar display projection.")
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    core = args.build_dir / "core.duckdb"
    arm = args.build_dir / "arm.duckdb"
    hierarchy = args.build_dir / "canonical_hierarchy.duckdb"
    for path in (core, arm, hierarchy):
        if not path.is_file():
            raise SystemExit(f"Missing required artifact: {path}")

    con = duckdb.connect(str(arm), read_only=True)
    failures: list[str] = []
    try:
        con.execute(f"attach {sql_literal(core)} as core (read_only)")
        con.execute(f"attach {sql_literal(hierarchy)} as hierarchy (read_only)")
        tables = {str(row[0]) for row in con.execute("show tables").fetchall()}
        if "stellar_leaf_display_classifications" not in tables:
            raise SystemExit("Missing ARM table stellar_leaf_display_classifications")

        duplicate_keys = scalar(
            con,
            """
            select count(*) from (
              select hierarchy_node_key from stellar_leaf_display_classifications
              group by 1 having count(*) <> 1
            )
            """,
        )
        invalid_rows = scalar(
            con,
            f"""
            select count(*) from stellar_leaf_display_classifications
            where classification_value not in {tuple(sorted(VALID_CLASSES))}
               or classification_status not in
                 ('source','source_model','derived','assumed','missing')
               or projection_version not in (
                 'stellar_leaf_display_classification_v1',
                 'e7_clean_runtime_leaf_classification_v1'
               )
               or (classification_status='missing' and classification_value<>'UNKNOWN')
               or (classification_status<>'missing' and classification_value='UNKNOWN')
               or (classification_status<>'missing' and (
                 evidence_basis is null or source_catalog is null or source_pk is null
               ))
            """,
        )
        membership = con.execute(
            """
            with expected as (
              select n.hierarchy_node_key
              from hierarchy.hierarchy_nodes n
              join core.stars s on n.canonical_key = s.stable_object_key
              where n.node_kind = 'star'
                and n.component_family = 'star'
                and not exists (
                  select 1
                  from hierarchy.hierarchy_edges e
                  join hierarchy.hierarchy_nodes child on child.hierarchy_node_key = e.child_node_key
                  where e.parent_node_key = n.hierarchy_node_key
                    and child.component_family = 'star'
                )
              union all
              select n.hierarchy_node_key
              from hierarchy.hierarchy_nodes n
              where n.node_kind not in ('system','star','planet')
                and n.component_family = 'star'
                and not exists (
                  select 1
                  from hierarchy.hierarchy_edges e
                  join hierarchy.hierarchy_nodes child on child.hierarchy_node_key = e.child_node_key
                  where e.parent_node_key = n.hierarchy_node_key
                    and child.component_family = 'star'
                )
            ), observed as (
              select hierarchy_node_key from stellar_leaf_display_classifications
            )
            select
              (select count(*) from expected)::bigint,
              (select count(*) from observed)::bigint,
              (select count(*) from expected e left join observed o using (hierarchy_node_key) where o.hierarchy_node_key is null)::bigint,
              (select count(*) from observed o left join expected e using (hierarchy_node_key) where e.hierarchy_node_key is null)::bigint
            """
        ).fetchone()
        expected_rows, observed_rows, missing_rows, extra_rows = (int(value or 0) for value in membership)
        system_count_mismatches = scalar(
            con,
            """
            select count(*) from (
              select s.system_id, s.star_count, count(l.hierarchy_node_key)::bigint as leaf_count
              from core.systems s
              left join stellar_leaf_display_classifications l using (system_id)
              group by s.system_id, s.star_count
              having coalesce(s.star_count, 0) <> count(l.hierarchy_node_key)
            )
            """,
        )

        if duplicate_keys:
            failures.append("duplicate_leaf_keys")
        if invalid_rows:
            failures.append("invalid_rows")
        if missing_rows or extra_rows or expected_rows != observed_rows:
            failures.append("leaf_membership_mismatch")

        report = {
            "schema_version": "stellar_leaf_display_classification_verification_v2",
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "build_dir": str(args.build_dir),
            "expected_leaf_rows": expected_rows,
            "observed_leaf_rows": observed_rows,
            "missing_leaf_rows": missing_rows,
            "extra_leaf_rows": extra_rows,
            "duplicate_leaf_keys": duplicate_keys,
            "invalid_rows": invalid_rows,
            "system_star_count_mismatches": system_count_mismatches,
            "system_star_count_mismatch_policy": "reported, not failed; hierarchy leaves are the badge membership authority",
            "named_system_gates": False,
            "classification_status_counts": {
                str(status): int(count)
                for status, count in con.execute(
                    "select classification_status,count(*) from "
                    "stellar_leaf_display_classifications group by 1 order by 1"
                ).fetchall()
            },
            "projection_version_counts": {
                str(version): int(count)
                for version, count in con.execute(
                    "select projection_version,count(*) from "
                    "stellar_leaf_display_classifications group by 1 order by 1"
                ).fetchall()
            },
            "failures": failures,
            "status": "pass" if not failures else "fail",
        }
    finally:
        con.close()

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
