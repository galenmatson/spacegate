#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb


CORE_TABLES = (
    "extended_objects",
    "extended_object_aliases",
    "extended_object_identifiers",
    "extended_object_search_terms",
    "extended_object_source_reconciliation",
    "extended_object_identity_quarantine",
)
ARM_TABLES = (
    "extended_object_source_records",
    "extended_object_geometry_evidence",
    "extended_object_distance_evidence",
    "extended_object_relations",
)


def table_digest(con: duckdb.DuckDBPyConnection, table: str) -> str:
    columns = [row[0] for row in con.execute(f"describe {table}").fetchall()]
    order = ",".join(f'"{column}"' for column in columns)
    digest = hashlib.sha256()
    for row in con.execute(f"select * from {table} order by {order}").fetchall():
        digest.update(json.dumps(row, default=str, separators=(",", ":")).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def object_ids_for_term(con: duckdb.DuckDBPyConnection, term: str) -> set[int]:
    return {
        int(row[0])
        for row in con.execute(
            """
            select distinct extended_object_id
            from extended_object_search_terms
            where term_norm = ?
            """,
            [term],
        ).fetchall()
    }


def require(checks: dict[str, Any], name: str, condition: bool, details: Any) -> None:
    checks[name] = {"passed": bool(condition), "details": details}


def verify(core_path: Path, arm_path: Path | None, compare_core_path: Path | None) -> dict[str, Any]:
    core = duckdb.connect(str(core_path), read_only=True)
    tables = {row[0] for row in core.execute("show tables").fetchall()}
    missing_core = sorted(set(CORE_TABLES) - tables)
    if missing_core:
        raise RuntimeError(f"Missing CORE extended-object tables: {missing_core}")

    checks: dict[str, Any] = {}
    diagnostics: dict[str, Any] = {}
    counts = {table: int(core.execute(f"select count(*) from {table}").fetchone()[0]) for table in CORE_TABLES}
    require(checks, "nonempty_inventory", counts["extended_objects"] > 0, counts)
    unresolved = int(core.execute("select count(*) from extended_object_source_reconciliation where outcome is null or reason is null").fetchone()[0])
    require(checks, "every_source_accounted", unresolved == 0, {"unaccounted": unresolved})
    general_checks = {
        "duplicate_object_ids": int(core.execute(
            "select count(*) from (select extended_object_id from extended_objects group by 1 having count(*)<>1)"
        ).fetchone()[0]),
        "duplicate_stable_keys": int(core.execute(
            "select count(*) from (select stable_object_key from extended_objects group by 1 having count(*)<>1)"
        ).fetchone()[0]),
        "orphan_aliases": int(core.execute(
            "select count(*) from extended_object_aliases a left join extended_objects e using(extended_object_id) where e.extended_object_id is null"
        ).fetchone()[0]),
        "orphan_identifiers": int(core.execute(
            "select count(*) from extended_object_identifiers i left join extended_objects e using(extended_object_id) where e.extended_object_id is null"
        ).fetchone()[0]),
        "orphan_search_terms": int(core.execute(
            "select count(*) from extended_object_search_terms t left join extended_objects e using(extended_object_id) where e.extended_object_id is null"
        ).fetchone()[0]),
        "local_3d_without_selected_geometry": int(core.execute(
            "select count(*) from extended_objects where map_domain='local_3d' and "
            "(dist_pc is null or x_helio_ly is null or y_helio_ly is null or z_helio_ly is null "
            "or distance_method is null or distance_confidence is null or distance_evidence_json is null)"
        ).fetchone()[0]),
        "missing_source_lineage": int(core.execute(
            "select count(*) from extended_objects where source_catalog is null or source_version is null "
            "or source_pk is null or source_row_hash is null or transform_version is null"
        ).fetchone()[0]),
    }
    for name, value in general_checks.items():
        require(checks, name, value == 0, {"rows": value})

    ic4592 = core.execute(
        """
        select canonical_name, display_name, object_type, dist_ly, map_domain, nominal_radius_tier_ly,
               retrieval_checksum, retrieved_at
        from extended_objects e
        join extended_object_search_terms t using(extended_object_id)
        where t.term_norm = 'ic 4592'
        limit 1
        """
    ).fetchone()
    ic_alias_ids = [object_ids_for_term(core, term) for term in ("ic 4592", "lbn 1113", "vdb 100", "blue horsehead nebula")]
    require(
        diagnostics,
        "ic4592_identity_distance",
        bool(ic4592)
        and ic4592[0] == "IC 4592"
        and ic4592[1] == "Blue Horsehead Nebula"
        and ic4592[2] == "reflection_nebula"
        and 400 <= float(ic4592[3]) <= 500
        and ic4592[4] == "local_3d"
        and int(ic4592[5]) == 500
        and len(set().union(*ic_alias_ids)) == 1
        and all(ids for ids in ic_alias_ids),
        {"row": ic4592, "alias_object_ids": [sorted(ids) for ids in ic_alias_ids]},
    )

    pleiades_ids = [object_ids_for_term(core, term) for term in ("m 45", "melotte 22", "pleiades")]
    require(
        diagnostics,
        "pleiades_single_identity",
        all(ids for ids in pleiades_ids) and len(set().union(*pleiades_ids)) == 1,
        [sorted(ids) for ids in pleiades_ids],
    )
    b33_ids = object_ids_for_term(core, "barnard 33")
    ic434_ids = object_ids_for_term(core, "ic 434")
    require(
        diagnostics,
        "horsehead_region_distinct_from_ic434",
        len(b33_ids) == 1 and len(ic434_ids) == 1 and b33_ids.isdisjoint(ic434_ids),
        {"barnard_33": sorted(b33_ids), "ic_434": sorted(ic434_ids)},
    )
    m31 = core.execute(
        """
        select e.map_domain, e.dist_ly
        from extended_objects e join extended_object_search_terms t using(extended_object_id)
        where t.term_norm='m 31' limit 1
        """
    ).fetchone()
    require(diagnostics, "m31_sky_domain", m31 == ("extragalactic_sky", None), {"row": m31})
    generic_parallax = int(core.execute("select count(*) from extended_objects where distance_method ilike '%openngc%parallax%'").fetchone()[0])
    require(checks, "openngc_generic_parallax_rejected", generic_parallax == 0, {"accepted_generic_parallax": generic_parallax})

    core_digests = {table: table_digest(core, table) for table in CORE_TABLES}
    if compare_core_path:
        compare = duckdb.connect(str(compare_core_path), read_only=True)
        compare_digests = {table: table_digest(compare, table) for table in CORE_TABLES}
        compare.close()
        require(checks, "deterministic_core_tables", core_digests == compare_digests, {"actual": core_digests, "comparison": compare_digests})

    arm_counts: dict[str, int] = {}
    if arm_path:
        arm = duckdb.connect(str(arm_path), read_only=True)
        arm_tables = {row[0] for row in arm.execute("show tables").fetchall()}
        missing_arm = sorted(set(ARM_TABLES) - arm_tables)
        clean_projection_rows = int(core.execute(
            "select count(*) from extended_objects where transform_version like 'e7_clean_extended_objects_compiler_%'"
        ).fetchone()[0])
        if not missing_arm:
            require(checks, "arm_evidence_contract", True, {"contract": "legacy_arm_evidence_tables"})
            arm_counts = {table: int(arm.execute(f"select count(*) from {table}").fetchone()[0]) for table in ARM_TABLES}
            accepted_relations = int(arm.execute("select count(*) from extended_object_relations where resolution_status='accepted'").fetchone()[0])
            require(checks, "arm_evidence_nonempty", all(count > 0 for count in arm_counts.values()), arm_counts)
            require(checks, "associated_star_relations_resolve", accepted_relations > 0, {"accepted_relations": accepted_relations})
        elif clean_projection_rows == counts["extended_objects"]:
            require(
                checks,
                "arm_evidence_contract",
                True,
                {
                    "contract": "clean_selected_core_projection",
                    "historical_evidence_tables_publicly_omitted": missing_arm,
                },
            )
        else:
            require(checks, "arm_evidence_contract", False, {"missing": missing_arm})
        arm.close()

    failed = sorted(name for name, value in checks.items() if not value["passed"])
    report = {
        "schema_version": "extended_object_verification_v2",
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "core_path": str(core_path),
        "arm_path": str(arm_path) if arm_path else None,
        "status": "pass" if not failed else "fail",
        "failed_checks": failed,
        "counts": {"core": counts, "arm": arm_counts},
        "core_table_digests": core_digests,
        "checks": checks,
        "named_object_gates": False,
        "named_object_diagnostics": diagnostics,
    }
    core.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Spacegate extended-object science tables.")
    parser.add_argument("--core", type=Path, required=True)
    parser.add_argument("--arm", type=Path)
    parser.add_argument("--compare-core", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.core, args.arm, args.compare_core)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
