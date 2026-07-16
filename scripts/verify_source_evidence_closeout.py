#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify MSC, WDS, and hierarchy evidence accounting.")
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--hierarchy-db", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    arm_path = args.build_dir / "arm.duckdb"
    if not arm_path.is_file():
        raise SystemExit(f"Missing ARM database: {arm_path}")

    con = duckdb.connect(str(arm_path), read_only=True)
    checks: dict[str, dict[str, object]] = {}
    try:
        tables = {str(row[0]) for row in con.execute("show tables").fetchall()}
        required = {
            "msc_orbit_details",
            "msc_orbit_reconciliation",
            "wds_component_observations",
            "wds_pair_evidence",
        }
        missing = sorted(required - tables)
        checks["required_tables"] = {"pass": not missing, "missing": missing}
        if missing:
            raise RuntimeError("Required source-evidence tables are absent")

        msc_total = scalar(con, "select count(*) from msc_orbit_details")
        msc_accounted = scalar(con, "select count(*) from msc_orbit_reconciliation")
        msc_quarantined = scalar(
            con, "select count(*) from msc_orbit_reconciliation where reconciliation_status = 'quarantined'"
        )
        msc_unaccounted = scalar(
            con,
            """
            select count(*)
            from msc_orbit_details d
            left join msc_orbit_reconciliation r using (msc_orbit_detail_id)
            where r.msc_orbit_reconciliation_id is null
            """,
        )
        msc_status_rows = con.execute(
            """
            select reconciliation_status, reconciliation_reason, count(*)::bigint
            from msc_orbit_reconciliation
            group by 1, 2 order by 1, 2
            """
        ).fetchall()
        checks["msc_orbit_accounting"] = {
            "pass": msc_total == msc_accounted and msc_quarantined == 0 and msc_unaccounted == 0,
            "detail_rows": msc_total,
            "accounted_rows": msc_accounted,
            "quarantined_rows": msc_quarantined,
            "unaccounted_rows": msc_unaccounted,
            "outcomes": [
                {"status": status, "reason": reason, "rows": int(count)}
                for status, reason, count in msc_status_rows
            ],
        }

        wds_total = scalar(con, "select count(*) from wds_component_observations")
        wds_accounted = scalar(con, "select count(*) from wds_pair_evidence")
        wds_invalid_accepted = scalar(
            con,
            """
            select count(*) from wds_pair_evidence
            where match_status = 'accepted'
              and (
                primary_component_key is null
                or secondary_component_key is null
                or primary_component_key = secondary_component_key
              )
            """,
        )
        wds_bound = scalar(con, "select count(*) from wds_pair_evidence where asserts_bound_relationship")
        wds_orbits = scalar(con, "select count(*) from wds_pair_evidence where simulation_ready_orbit")
        wds_status_rows = con.execute(
            "select match_status, count(*)::bigint from wds_pair_evidence group by 1 order by 1"
        ).fetchall()
        checks["wds_pair_accounting"] = {
            "pass": (
                wds_total == wds_accounted
                and wds_invalid_accepted == 0
                and wds_bound == 0
                and wds_orbits == 0
            ),
            "observation_rows": wds_total,
            "accounted_rows": wds_accounted,
            "invalid_accepted_rows": wds_invalid_accepted,
            "bound_relationship_rows": wds_bound,
            "simulation_ready_orbit_rows": wds_orbits,
            "outcomes": {str(status): int(count) for status, count in wds_status_rows},
        }

        if args.hierarchy_db:
            hierarchy_path = args.hierarchy_db.resolve(strict=True)
            hierarchy_sql_path = str(hierarchy_path).replace("'", "''")
            con.execute(f"attach '{hierarchy_sql_path}' as hierarchy (read_only)")
            hierarchy_columns = {
                str(row[0])
                for row in con.execute(
                    """
                    select column_name from information_schema.columns
                    where table_catalog = 'hierarchy'
                      and table_schema = 'main'
                      and table_name = 'hierarchy_nodes'
                    """
                ).fetchall()
            }
            required_columns = {"node_kind", "component_family", "component_type"}
            missing_columns = sorted(required_columns - hierarchy_columns)
            null_types = 0
            inferred_type_mismatches = 0
            if not missing_columns:
                null_types = scalar(
                    con,
                    """
                    select count(*) from hierarchy.hierarchy_nodes
                    where component_family is null or component_type is null
                    """,
                )
                inferred_type_mismatches = scalar(
                    con,
                    """
                    select count(*)
                    from hierarchy.hierarchy_nodes h
                    join component_entities ce
                      on ce.stable_component_key = replace(
                        h.hierarchy_node_key, 'canon:leaf:msc:', 'comp:msc:wds:'
                      )
                    where h.node_kind = 'inferred_star_leaf'
                      and lower(h.component_type) <> lower(ce.component_type)
                    """,
                )
            checks["hierarchy_endpoint_typing"] = {
                "pass": not missing_columns and null_types == 0 and inferred_type_mismatches == 0,
                "missing_columns": missing_columns,
                "null_type_rows": null_types,
                "inferred_type_mismatches": inferred_type_mismatches,
            }
    except Exception as exc:
        checks.setdefault("execution", {"pass": False, "error": str(exc)})
    finally:
        con.close()

    passed = all(bool(check.get("pass")) for check in checks.values())
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "build_dir": str(args.build_dir),
        "hierarchy_db": str(args.hierarchy_db) if args.hierarchy_db else None,
        "status": "pass" if passed else "fail",
        "checks": checks,
    }
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
