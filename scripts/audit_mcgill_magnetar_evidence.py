#!/usr/bin/env python3
"""Audit the pinned McGill magnetar parameter and bibliography evidence bundle."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import DEFAULT_STATE, load_json, write_json


DEFAULT_REPORT_ROOT = DEFAULT_STATE / "reports" / "evidence_lake_v2"


def latest_typed_manifest(state_dir: Path) -> Path:
    root = (
        state_dir
        / "typed"
        / "evidence_lake_v2"
        / "compact.mcgill_magnetar"
        / "snapshot_20260721_with_bibliography"
    )
    candidates = list(root.rglob("typed_manifest.json"))
    if not candidates:
        raise FileNotFoundError(root)
    return max(candidates, key=lambda path: str(load_json(path)["created_at"]))


def typed_paths(manifest_path: Path) -> tuple[dict[str, Path], dict[str, Any]]:
    manifest = load_json(manifest_path)
    root = manifest_path.parent
    return {
        str(table["source_name"]): root / str(table["parquet_path"])
        for table in manifest["tables"]
    }, manifest


def csv_reference_codes(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    codes: set[str] = set()
    for column in ("Ref_Time", "Ref_Xray", "Ref_Dist", "Ref_Assoc", "Ref_Pos"):
        values = con.execute(
            f"select distinct {column} from read_parquet(?) "
            f"where nullif(trim({column}), '') is not null",
            [str(path)],
        ).fetchall()
        for (value,) in values:
            codes.update(
                token.strip(" []")
                for token in re.split(r"[,;\s]+", str(value))
                if token.strip(" []") and token.strip(" []") != "..."
            )
    return codes


def audit(state_dir: Path, compile_report_path: Path) -> dict[str, Any]:
    manifest_path = latest_typed_manifest(state_dir)
    paths, typed_manifest = typed_paths(manifest_path)
    required_tables = {
        "mcgill_magnetar_catalog",
        "mcgill_html_rows",
        "mcgill_html_reference_links",
        "mcgill_html_reference_index",
        "mcgill_main_html_document",
        "mcgill_cds_readme",
        "mcgill_cds_references",
    }
    missing_tables = sorted(required_tables - set(paths))
    compile_report = load_json(compile_report_path)
    database = (
        state_dir
        / "derived"
        / "evidence_lake_v2"
        / "scientific_evidence"
        / str(compile_report["build_id"])
        / "scientific_evidence.duckdb"
    )
    if not database.is_file():
        raise FileNotFoundError(database)
    checks: dict[str, Any] = {"missing_typed_tables": missing_tables}
    if not missing_tables:
        with duckdb.connect() as con:
            catalog_codes = csv_reference_codes(
                con, paths["mcgill_magnetar_catalog"]
            )
            reference_rows = con.execute(
                "select reference_code_raw, href_absolute, resource_kind, bibcode_raw, "
                "occurrence_count from read_parquet(?) order by reference_code_raw",
                [str(paths["mcgill_html_reference_index"])],
            ).fetchall()
            linked_codes = {str(row[0]) for row in reference_rows}
            checks.update(
                {
                    "catalog_rows": int(
                        con.execute(
                            "select count(*) from read_parquet(?)",
                            [str(paths["mcgill_magnetar_catalog"])],
                        ).fetchone()[0]
                    ),
                    "html_rows": int(
                        con.execute(
                            "select count(*) from read_parquet(?)",
                            [str(paths["mcgill_html_rows"])],
                        ).fetchone()[0]
                    ),
                    "html_data_rows": int(
                        con.execute(
                            "select count(*) from read_parquet(?) where row_kind='data'",
                            [str(paths["mcgill_html_rows"])],
                        ).fetchone()[0]
                    ),
                    "html_section_rows": int(
                        con.execute(
                            "select count(*) from read_parquet(?) where row_kind='section'",
                            [str(paths["mcgill_html_rows"])],
                        ).fetchone()[0]
                    ),
                    "html_resource_links": int(
                        con.execute(
                            "select count(*) from read_parquet(?)",
                            [str(paths["mcgill_html_reference_links"])],
                        ).fetchone()[0]
                    ),
                    "html_external_reference_codes": len(linked_codes),
                    "html_ambiguous_reference_codes": sorted(
                        str(row[0])
                        for row in con.execute(
                            "select reference_code_raw from read_parquet(?) "
                            "group by reference_code_raw having count(distinct href_absolute)>1",
                            [str(paths["mcgill_html_reference_index"])],
                        ).fetchall()
                    ),
                    "catalog_reference_codes": len(catalog_codes),
                    "unresolved_catalog_reference_codes": sorted(
                        catalog_codes - linked_codes
                    ),
                    "unexpected_html_reference_codes": sorted(
                        linked_codes - catalog_codes
                    ),
                    "cds_reference_rows": int(
                        con.execute(
                            "select count(*) from read_parquet(?)",
                            [str(paths["mcgill_cds_references"])],
                        ).fetchone()[0]
                    ),
                    "cds_reference_rows_with_bibcode": int(
                        con.execute(
                            "select count(*) from read_parquet(?) "
                            "where nullif(trim(BibCode), '') is not null",
                            [str(paths["mcgill_cds_references"])],
                        ).fetchone()[0]
                    ),
                }
            )
    with duckdb.connect(str(database), read_only=True) as con:
        checks.update(
            {
                "compact_object_parameter_sets": int(
                    con.execute("select count(*) from compact_object_evidence").fetchone()[0]
                ),
                "citations": int(con.execute("select count(*) from citations").fetchone()[0]),
                "current_object_bibliography_links": int(
                    con.execute(
                        "select count(*) from evidence_citations "
                        "where citation_role='mcgill_current_object_bibliography'"
                    ).fetchone()[0]
                ),
                "direct_source_reference_links": int(
                    con.execute(
                        "select count(*) from evidence_citations "
                        "where citation_role='source_reference'"
                    ).fetchone()[0]
                ),
                "invented_url_rows": int(
                    con.execute(
                        "select count(*) from citations where citation_url is not null "
                        "and source_reference_key in ('cdt+82','cwd+97','fmc+99','wkv+99b')"
                    ).fetchone()[0]
                ),
            }
        )
    expected = {
        "missing_typed_tables": [],
        "catalog_rows": 31,
        "html_rows": 34,
        "html_data_rows": 31,
        "html_section_rows": 3,
        "html_resource_links": 319,
        "html_external_reference_codes": 97,
        "html_ambiguous_reference_codes": [],
        "catalog_reference_codes": 101,
        "unresolved_catalog_reference_codes": [
            "cdt+82",
            "cwd+97",
            "fmc+99",
            "wkv+99b",
        ],
        "unexpected_html_reference_codes": [],
        "cds_reference_rows": 215,
        "cds_reference_rows_with_bibcode": 215,
        "compact_object_parameter_sets": 139,
        "citations": 319,
        "current_object_bibliography_links": 208,
        "direct_source_reference_links": 128,
        "invented_url_rows": 0,
    }
    mismatches = {
        key: {"expected": expected[key], "actual": checks.get(key)}
        for key in expected
        if checks.get(key) != expected[key]
    }
    return {
        "schema_version": "spacegate.mcgill_magnetar_evidence_audit.v1",
        "status": "pass" if not mismatches else "fail",
        "typed_manifest": str(manifest_path),
        "typed_snapshot_id": typed_manifest["typed_snapshot_id"],
        "scientific_evidence_build_id": compile_report["build_id"],
        "checks": checks,
        "expected": expected,
        "mismatches": mismatches,
        "unresolved_policy": (
            "retain unresolved shorthand codes verbatim; do not synthesize citations"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument(
        "--compile-report",
        type=Path,
        default=DEFAULT_REPORT_ROOT / "e4_mcgill_compile_v4.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT_ROOT / "e4_mcgill_evidence_audit_v1.json",
    )
    args = parser.parse_args()
    report = audit(args.state_dir, args.compile_report)
    write_json(args.report, report)
    print(
        f"McGill magnetar evidence audit {report['status']}: "
        f"mismatches={len(report['mismatches'])}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
