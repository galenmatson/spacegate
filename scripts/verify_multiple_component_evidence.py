#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify generic multiple-component source evidence.")
    parser.add_argument("--arm-db", required=True)
    parser.add_argument("--compare-arm-db", default=None)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    arm_path = Path(args.arm_db).resolve()
    con = duckdb.connect(str(arm_path), read_only=True)
    failures: list[str] = []

    required_tables = {
        "sb9_systems",
        "sb9_aliases",
        "sb9_orbits",
        "multiple_component_evidence_matches",
        "multiple_component_stellar_evidence",
    }
    existing = {
        str(row[0])
        for row in con.execute("select table_name from information_schema.tables where table_schema='main'").fetchall()
    }
    missing = sorted(required_tables - existing)
    if missing:
        failures.append(f"missing ARM tables: {missing}")

    counts: dict[str, object] = {}
    determinism: dict[str, object] = {"status": "not_requested"}
    if not missing:
        counts["sb9_systems"] = int(con.execute("select count(*) from sb9_systems").fetchone()[0])
        counts["sb9_aliases"] = int(con.execute("select count(*) from sb9_aliases").fetchone()[0])
        counts["sb9_orbits"] = int(con.execute("select count(*) from sb9_orbits").fetchone()[0])
        counts["matches_by_status"] = {
            f"{catalog}:{status}:{reason}": int(count)
            for catalog, status, reason, count in con.execute(
                """
                select source_catalog, match_status, reason, count(*)::bigint
                from multiple_component_evidence_matches
                group by all order by 1, 2, 3
                """
            ).fetchall()
        }
        counts["stellar_evidence_by_source"] = {
            str(catalog): int(count)
            for catalog, count in con.execute(
                "select source_catalog, count(*)::bigint from multiple_component_stellar_evidence group by 1 order by 1"
            ).fetchall()
        }
        sb9_reference_count = int(
            con.execute(
                """
                select count(*) from msc_system_details
                where regexp_matches(upper(coalesce(comment, '')), 'SB9_[0-9]+')
                """
            ).fetchone()[0]
        )
        sb9_match_count = int(
            con.execute(
                "select count(*) from multiple_component_evidence_matches where source_catalog='sb9'"
            ).fetchone()[0]
        )
        counts["sb9_msc_reference_count"] = sb9_reference_count
        counts["sb9_accounted_match_count"] = sb9_match_count
        if sb9_reference_count != sb9_match_count:
            failures.append(
                f"SB9 MSC reference accounting mismatch: refs={sb9_reference_count}, matches={sb9_match_count}"
            )
        if int(counts["sb9_systems"]) < 4000:
            failures.append("SB9 systems unexpectedly below 4,000 rows")
        if int(counts["sb9_orbits"]) < 5000:
            failures.append("SB9 orbits unexpectedly below 5,000 rows")

        invalid_status = int(
            con.execute(
                """
                select count(*) from multiple_component_evidence_matches
                where match_status not in ('accepted', 'excluded', 'quarantined')
                """
            ).fetchone()[0]
        )
        duplicate_evidence = int(
            con.execute(
                """
                select count(*) from (
                  select source_catalog, source_pk, stable_component_key, count(*)
                  from multiple_component_stellar_evidence
                  group by 1,2,3 having count(*) > 1
                )
                """
            ).fetchone()[0]
        )
        invalid_acceptance = int(
            con.execute(
                """
                select count(*) from multiple_component_evidence_matches
                where match_status = 'accepted'
                  and reason not in (
                    'exact_msc_sb9_sequence_and_resolved_endpoints',
                    'unique_system_and_period_match_with_resolved_endpoints'
                  )
                """
            ).fetchone()[0]
        )
        if invalid_status:
            failures.append(f"invalid match statuses: {invalid_status}")
        if duplicate_evidence:
            failures.append(f"duplicate source/component evidence rows: {duplicate_evidence}")
        if invalid_acceptance:
            failures.append(f"accepted matches with unapproved methods: {invalid_acceptance}")

        castor_rows = con.execute(
            """
            select stable_component_key, classification_value,
                   json_extract_string(input_parameters_json, '$.spectral_type_raw') as spectral_type_raw
            from derived_stellar_classifications
            where stable_component_key like 'comp:msc:wds:07346+3153:%'
              and classification_status = 'source'
              and review_status = 'accepted'
              and source_catalog = 'sb9'
            order by stable_component_key
            """
        ).fetchall()
        counts["castor_sb9_classes"] = [list(row) for row in castor_rows]
        expected = {
            "comp:msc:wds:07346+3153:aa": "A",
            "comp:msc:wds:07346+3153:ab": "M",
            "comp:msc:wds:07346+3153:ba": "A",
            "comp:msc:wds:07346+3153:bb": "M",
            "comp:msc:wds:07346+3153:ca": "M",
            "comp:msc:wds:07346+3153:cb": "M",
        }
        observed = {str(key): str(value) for key, value, _ in castor_rows}
        if observed != expected:
            failures.append(f"Castor SB9 endpoint classes mismatch: {observed}")

        if args.compare_arm_db:
            compare_path = Path(args.compare_arm_db).resolve()
            compare = duckdb.connect(str(compare_path), read_only=True)
            table_orders = {
                "sb9_systems": "sb9_sequence",
                "sb9_aliases": "sb9_sequence, source_line_number",
                "sb9_orbits": "sb9_sequence, orbit_number",
                "multiple_component_evidence_matches": "evidence_match_id",
                "multiple_component_stellar_evidence": "component_evidence_id",
            }
            fingerprints: dict[str, dict[str, object]] = {}
            mismatch_tables: list[str] = []
            for table, order_by in table_orders.items():
                left = con.execute(
                    f"select count(*), sha256(string_agg(to_json(t), '' order by {order_by})) from {table} t"
                ).fetchone()
                right = compare.execute(
                    f"select count(*), sha256(string_agg(to_json(t), '' order by {order_by})) from {table} t"
                ).fetchone()
                fingerprints[table] = {
                    "row_count": int(left[0]),
                    "sha256": str(left[1]),
                    "compare_row_count": int(right[0]),
                    "compare_sha256": str(right[1]),
                }
                if left != right:
                    mismatch_tables.append(table)
            compare.close()
            determinism = {
                "status": "pass" if not mismatch_tables else "fail",
                "compare_arm_db": str(compare_path),
                "mismatch_tables": mismatch_tables,
                "fingerprints": fingerprints,
            }
            if mismatch_tables:
                failures.append(f"determinism mismatch: {mismatch_tables}")

    report = {
        "generated_at": utc_now(),
        "arm_db": str(arm_path),
        "status": "pass" if not failures else "fail",
        "failures": failures,
        "counts": counts,
        "determinism": determinism,
    }
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
