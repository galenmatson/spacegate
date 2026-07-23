#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


NON_SCIENCE_FINGERPRINT_COLUMNS = {"build_id", "ingested_at"}


def science_table_fingerprint(
    con: duckdb.DuckDBPyConnection,
    table: str,
    order_by: str,
) -> tuple[int, str, list[str]]:
    columns = [
        str(row[0])
        for row in con.execute(f"describe {table}").fetchall()
        if str(row[0]) not in NON_SCIENCE_FINGERPRINT_COLUMNS
    ]
    if not columns:
        raise ValueError(f"No scientific fingerprint columns found for {table}")
    projection = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
    row = con.execute(
        f"""
        select count(*), sha256(string_agg(to_json(row({projection})), '' order by {order_by}))
        from {table}
        """
    ).fetchone()
    return int(row[0]), str(row[1]), columns


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

    legacy_required_tables = {
        "sb9_systems",
        "sb9_aliases",
        "sb9_orbits",
        "multiple_component_evidence_matches",
        "multiple_component_stellar_evidence",
    }
    clean_required_tables = {
        "msc_runtime_leaf_bindings",
        "stellar_leaf_display_classifications",
    }
    existing = {
        str(row[0])
        for row in con.execute("select table_name from information_schema.tables where table_schema='main'").fetchall()
    }
    if clean_required_tables <= existing:
        contract = "e7_release_scoped_selected_components"
        missing: list[str] = []
    elif legacy_required_tables <= existing:
        contract = "legacy_materialized_component_evidence"
        missing = []
    else:
        contract = "unknown"
        missing = sorted(
            min(
                (clean_required_tables - existing, legacy_required_tables - existing),
                key=len,
            )
        )
        failures.append(f"missing ARM tables for a supported contract: {missing}")

    counts: dict[str, object] = {}
    determinism: dict[str, object] = {"status": "not_requested"}
    if contract == "e7_release_scoped_selected_components":
        counts["msc_bindings_by_status"] = {
            f"{source_status}:{runtime_status}:{reason}": int(count)
            for source_status, runtime_status, reason, count in con.execute(
                """
                select source_binding_status,runtime_binding_status,
                       runtime_binding_reason,count(*)::bigint
                from msc_runtime_leaf_bindings
                group by all order by 1,2,3
                """
            ).fetchall()
        }
        counts["selected_leaf_component_evidence"] = {
            f"{basis}:{catalog}": int(count)
            for basis, catalog, count in con.execute(
                """
                select evidence_basis,source_catalog,count(*)::bigint
                from stellar_leaf_display_classifications
                where evidence_basis in (
                  'selected_msc_component_spectral_type',
                  'selected_sb9_component_spectral_type',
                  'selected_debcat_component_spectral_type',
                  'selected_msc_component_mass_main_sequence_prior'
                )
                group by all order by 1,2
                """
            ).fetchall()
        }
        clean_checks = {
            "duplicate_component_bindings": int(
                con.execute(
                    "select count(*) from (select component_entity_id from "
                    "msc_runtime_leaf_bindings group by 1 having count(*)<>1)"
                ).fetchone()[0]
            ),
            "invalid_binding_status": int(
                con.execute(
                    "select count(*) from msc_runtime_leaf_bindings where "
                    "runtime_binding_status not in ('accepted','missing','ambiguous','excluded')"
                ).fetchone()[0]
            ),
            "accepted_binding_without_leaf": int(
                con.execute(
                    "select count(*) from msc_runtime_leaf_bindings where "
                    "runtime_binding_status='accepted' and hierarchy_node_key is null"
                ).fetchone()[0]
            ),
            "unaccepted_binding_with_leaf": int(
                con.execute(
                    "select count(*) from msc_runtime_leaf_bindings where "
                    "runtime_binding_status<>'accepted' and hierarchy_node_key is not null"
                ).fetchone()[0]
            ),
            "canonical_containment_promotions": int(
                con.execute(
                    "select count(*) from msc_runtime_leaf_bindings where canonical_containment"
                ).fetchone()[0]
            ),
            "invalid_case_collision": int(
                con.execute(
                    "select count(*) from msc_runtime_leaf_bindings where "
                    "runtime_binding_reason='case_significant_source_collision' and "
                    "(runtime_binding_status<>'ambiguous' or source_candidate_count<2)"
                ).fetchone()[0]
            ),
            "selected_component_evidence_without_accepted_binding": int(
                con.execute(
                    """
                    select count(*)
                    from stellar_leaf_display_classifications l
                    left join msc_runtime_leaf_bindings b
                      on b.hierarchy_node_key=l.hierarchy_node_key
                     and b.runtime_binding_status='accepted'
                    where l.evidence_basis in (
                      'selected_msc_component_spectral_type',
                      'selected_sb9_component_spectral_type',
                      'selected_debcat_component_spectral_type',
                      'selected_msc_component_mass_main_sequence_prior'
                    ) and b.binding_id is null
                    """
                ).fetchone()[0]
            ),
        }
        counts["clean_checks"] = clean_checks
        failures.extend(
            f"{name}: {value}" for name, value in clean_checks.items() if value
        )
        if args.compare_arm_db:
            compare_path = Path(args.compare_arm_db).resolve()
            compare = duckdb.connect(str(compare_path), read_only=True)
            compare_tables = {
                str(row[0]) for row in compare.execute(
                    "select table_name from information_schema.tables "
                    "where table_schema='main'"
                ).fetchall()
            }
            if not clean_required_tables <= compare_tables:
                failures.append("comparison ARM does not use the clean component contract")
            else:
                table_orders = {
                    "msc_runtime_leaf_bindings": "component_entity_id",
                    "stellar_leaf_display_classifications": "hierarchy_node_key",
                }
                fingerprints: dict[str, dict[str, object]] = {}
                mismatch_tables: list[str] = []
                for table, order_by in table_orders.items():
                    left_count, left_hash, left_columns = science_table_fingerprint(
                        con, table, order_by
                    )
                    right_count, right_hash, right_columns = science_table_fingerprint(
                        compare, table, order_by
                    )
                    fingerprints[table] = {
                        "row_count": left_count,
                        "sha256": left_hash,
                        "compare_row_count": right_count,
                        "compare_sha256": right_hash,
                        "fingerprint_columns": left_columns,
                        "excluded_columns": sorted(NON_SCIENCE_FINGERPRINT_COLUMNS),
                    }
                    if left_columns != right_columns or (left_count, left_hash) != (
                        right_count,
                        right_hash,
                    ):
                        mismatch_tables.append(table)
                determinism = {
                    "status": "pass" if not mismatch_tables else "fail",
                    "compare_arm_db": str(compare_path),
                    "mismatch_tables": mismatch_tables,
                    "fingerprints": fingerprints,
                }
                if mismatch_tables:
                    failures.append(f"determinism mismatch: {mismatch_tables}")
            compare.close()
    elif not missing:
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
                left_count, left_hash, fingerprint_columns = science_table_fingerprint(
                    con, table, order_by
                )
                right_count, right_hash, compare_columns = science_table_fingerprint(
                    compare, table, order_by
                )
                left = (left_count, left_hash)
                right = (right_count, right_hash)
                fingerprints[table] = {
                    "row_count": left_count,
                    "sha256": left_hash,
                    "compare_row_count": right_count,
                    "compare_sha256": right_hash,
                    "fingerprint_columns": fingerprint_columns,
                    "excluded_columns": sorted(NON_SCIENCE_FINGERPRINT_COLUMNS),
                }
                if fingerprint_columns != compare_columns or left != right:
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
        "schema_version": "spacegate.multiple_component_evidence_verification.v2",
        "generated_at": utc_now(),
        "arm_db": str(arm_path),
        "contract": contract,
        "named_system_gates": False,
        "status": "pass" if not failures else "fail",
        "failures": failures,
        "counts": counts,
        "determinism": determinism,
    }
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    con.close()
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
