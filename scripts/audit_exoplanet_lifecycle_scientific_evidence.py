#!/usr/bin/env python3
"""Audit pinned supplemental exoplanet lifecycle scientific evidence builds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


DEFAULT_REPORT_ROOT = DEFAULT_STATE / "reports" / "evidence_lake_v2"


def database_from_report(path: Path) -> tuple[Path, dict[str, Any]]:
    report = load_json(path)
    build_id = str(report["build_id"])
    database = (
        DEFAULT_STATE
        / "derived"
        / "evidence_lake_v2"
        / "scientific_evidence"
        / build_id
        / "scientific_evidence.duckdb"
    )
    if not database.is_file():
        raise FileNotFoundError(database)
    return database, report


def scalar(con: duckdb.DuckDBPyConnection, sql: str, parameters: list[Any] | None = None) -> int:
    return int(con.execute(sql, parameters or []).fetchone()[0])


def audit(
    exoplanet_eu_report: Path,
    hwc_report: Path,
    oec_report: Path,
) -> dict[str, Any]:
    eu_database, eu_compile = database_from_report(exoplanet_eu_report)
    hwc_database, hwc_compile = database_from_report(hwc_report)
    oec_database, oec_compile = database_from_report(oec_report)
    checks: dict[str, Any] = {}

    with duckdb.connect(str(eu_database), read_only=True) as con:
        checks["exoplanet_eu_source_rows"] = scalar(con, "select count(*) from source_records")
        checks["exoplanet_eu_positive_confirmed_rows"] = scalar(
            con,
            "select count(*) from planet_lifecycle_evidence "
            "where disposition_normalized='CONFIRMED' and evidence_polarity='positive'",
        )
        checks["exoplanet_eu_nonconfirmed_lifecycle_rows"] = scalar(
            con,
            "select count(*) from planet_lifecycle_evidence "
            "where disposition_normalized<>'CONFIRMED' or evidence_polarity<>'positive'",
        )

    with duckdb.connect(str(hwc_database), read_only=True) as con:
        checks["hwc_source_rows"] = scalar(con, "select count(*) from source_records")
        checks["hwc_lifecycle_rows"] = scalar(
            con, "select count(*) from planet_lifecycle_evidence"
        )
        checks["hwc_habitability_rows"] = scalar(
            con,
            "select count(*) from planet_parameter_evidence "
            "where quantity_key='hwc.P_HABITABLE'",
        )
        checks["hwc_habitability_domain"] = {
            str(value): int(count)
            for value, count in con.execute(
                "select value_raw, count(*) from planet_parameter_evidence "
                "where quantity_key='hwc.P_HABITABLE' group by value_raw order by value_raw"
            ).fetchall()
        }

    with duckdb.connect(str(oec_database), read_only=True) as con:
        checks["oec_source_rows"] = scalar(con, "select count(*) from source_records")
        checks["oec_parameter_disposition_rows"] = scalar(
            con, "select coalesce(sum(row_count), 0) from source_native_parameter_dispositions"
        )
        checks["oec_parameter_disposition_pairs"] = scalar(
            con, "select count(*) from source_native_parameter_dispositions"
        )
        checks["oec_planet_parameter_sets"] = scalar(
            con, "select count(*) from planet_parameter_sets"
        )
        checks["oec_stellar_parameter_sets"] = scalar(
            con, "select count(*) from stellar_parameter_sets"
        )
        checks["oec_binary_orbital_solutions"] = scalar(
            con, "select count(*) from orbital_solution_evidence"
        )
        checks["oec_relation_claims"] = scalar(
            con, "select count(*) from relation_claim_evidence"
        )
        checks["oec_observation_product_links"] = scalar(
            con, "select count(*) from observation_product_lineage"
        )
        checks["oec_lifecycle_by_disposition_and_polarity"] = {
            f"{disposition}|{polarity}": int(count)
            for disposition, polarity, count in con.execute(
                "select disposition_normalized, evidence_polarity, count(*) "
                "from planet_lifecycle_evidence group by all order by all"
            ).fetchall()
        }
        checks["oec_legacy_confirmed_label_rows"] = scalar(
            con,
            "select count(*) from planet_lifecycle_evidence "
            "where disposition_normalized='CONFIRMED_PLANETS'",
        )
        checks["oec_duplicate_planet_parameter_set_object_keys"] = scalar(
            con,
            "select count(*) from (select quality_json->>'source_object_key' object_key, "
            "count(*) n from planet_parameter_sets group by object_key having n>1)",
        )
        checks["oec_duplicate_stellar_parameter_set_object_keys"] = scalar(
            con,
            "select count(*) from (select quality_json->>'source_object_key' object_key, "
            "count(*) n from stellar_parameter_sets group by object_key having n>1)",
        )
        checks["oec_limit_semantics"] = {
            str(semantics): int(count)
            for semantics, count in con.execute(
                "select bound_semantics, count(*) from planet_parameter_evidence "
                "group by bound_semantics order by bound_semantics"
            ).fetchall()
        }

    expected = {
        "exoplanet_eu_source_rows": 8261,
        "exoplanet_eu_positive_confirmed_rows": 8261,
        "exoplanet_eu_nonconfirmed_lifecycle_rows": 0,
        "hwc_source_rows": 5599,
        "hwc_lifecycle_rows": 0,
        "hwc_habitability_rows": 5599,
        "hwc_habitability_domain": {"0": 5529, "1": 29, "2": 41},
        "oec_source_rows": 268040,
        "oec_parameter_disposition_rows": 160582,
        "oec_parameter_disposition_pairs": 98,
        "oec_planet_parameter_sets": 9252,
        "oec_stellar_parameter_sets": 7182,
        "oec_binary_orbital_solutions": 219,
        "oec_relation_claims": 16750,
        "oec_observation_product_links": 127,
        "oec_lifecycle_by_disposition_and_polarity": {
            "CANDIDATE|candidate": 3844,
            "CONFIRMED|positive": 5287,
            "CONTROVERSIAL|ambiguous": 100,
            "PLANETS_IN_BINARY_SYSTEMS,_S-TYPE|ambiguous": 1,
            "RETRACTED|negative": 12,
            "SOLAR_SYSTEM|ambiguous": 9,
        },
        "oec_legacy_confirmed_label_rows": 0,
        "oec_duplicate_planet_parameter_set_object_keys": 0,
        "oec_duplicate_stellar_parameter_set_object_keys": 0,
        "oec_limit_semantics": {
            "interval_limit": 39,
            "lower_limit": 28,
            "measurement": 51493,
            "upper_limit": 146,
        },
    }
    mismatches = {
        key: {"expected": expected[key], "actual": checks.get(key)}
        for key in expected
        if checks.get(key) != expected[key]
    }
    return {
        "schema_version": "spacegate.exoplanet_lifecycle_scientific_evidence_audit.v1",
        "status": "pass" if not mismatches else "fail",
        "inputs": {
            "exoplanet_eu": {"report": str(exoplanet_eu_report), "build_id": eu_compile["build_id"]},
            "hwc": {"report": str(hwc_report), "build_id": hwc_compile["build_id"]},
            "oec": {"report": str(oec_report), "build_id": oec_compile["build_id"]},
        },
        "checks": checks,
        "expected": expected,
        "mismatches": mismatches,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--exoplanet-eu-report",
        type=Path,
        default=DEFAULT_REPORT_ROOT / "e4_exoplanet_eu_compile_v2.json",
    )
    parser.add_argument(
        "--hwc-report",
        type=Path,
        default=DEFAULT_REPORT_ROOT / "e4_hwc_compile_v2.json",
    )
    parser.add_argument(
        "--oec-report",
        type=Path,
        default=DEFAULT_REPORT_ROOT / "e4_oec_compile_v6.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT_ROOT / "e4_exoplanet_lifecycle_scientific_evidence_audit_v1.json",
    )
    args = parser.parse_args()
    report = audit(args.exoplanet_eu_report, args.hwc_report, args.oec_report)
    write_json(args.report, report)
    print(
        "exoplanet lifecycle scientific evidence audit "
        f"{report['status']}: mismatches={len(report['mismatches'])}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
