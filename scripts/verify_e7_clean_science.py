#!/usr/bin/env python3
"""Independently verify an E7 clean selected-science artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path("/mnt/space/spacegate/e7-clean-science")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(build_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    manifest = load_object(build_dir / "manifest.json")
    failures: dict[str, Any] = {}
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")
    if manifest.get("stability_scientific_values_copied") is not False:
        failures["stability_values_copied"] = manifest.get("stability_scientific_values_copied")
    product_failures: dict[str, Any] = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            product_failures[relative] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        if actual != {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}:
            product_failures[relative] = {"expected": expected, "actual": actual}
    if product_failures:
        failures["products"] = product_failures
    db = build_dir / "clean_science.duckdb"
    actual_counts: dict[str, int] = {}
    checks: dict[str, int] = {}
    summaries: dict[str, Any] = {}
    if db.is_file():
        con = duckdb.connect(str(db), read_only=True)
        try:
            expected_counts = manifest.get("projection_table_counts") or {}
            for table in sorted(expected_counts):
                try:
                    actual_counts[table] = int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
                except duckdb.Error:
                    actual_counts[table] = -1
            mismatched_counts = {
                table: {"expected": int(expected_counts[table]), "actual": actual_counts.get(table)}
                for table in expected_counts
                if actual_counts.get(table) != int(expected_counts[table])
            }
            if mismatched_counts:
                failures["projection_counts"] = mismatched_counts
            checks = {
                "duplicate_parameters": int(con.execute("SELECT count(*) FROM (SELECT star_id FROM selected_stellar_parameters GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
                "duplicate_display_classes": int(con.execute("SELECT count(*) FROM (SELECT star_id FROM selected_stellar_display_classifications GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]),
                "invalid_display_classes": int(con.execute("SELECT count(*) FROM selected_stellar_display_classifications WHERE classification_value NOT IN ('O','B','A','F','G','K','M','L','T','Y','WR','WD','NS','PULSAR','MAGNETAR','BLACK HOLE','UNKNOWN')").fetchone()[0]),
                "selected_without_fact": int(con.execute("SELECT count(*) FROM selected_stellar_display_classifications WHERE classification_status<>'missing' AND selected_fact_id IS NULL").fetchone()[0]),
                "stability_basis": int(con.execute("SELECT count(*) FROM selected_stellar_display_classifications WHERE lower(evidence_basis) LIKE '%stability%' OR lower(evidence_basis) LIKE '%core%fallback%'").fetchone()[0]),
                "metadata_stability_opened": int(con.execute("SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'").fetchone()[0]),
            }
            summaries = {
                "display_classification_status": {
                    str(row[0]): int(row[1]) for row in con.execute(
                        "SELECT classification_status,count(*) FROM selected_stellar_display_classifications GROUP BY 1 ORDER BY 1"
                    ).fetchall()
                },
                "display_evidence_basis": {
                    str(row[0]): int(row[1]) for row in con.execute(
                        "SELECT evidence_basis,count(*) FROM selected_stellar_display_classifications GROUP BY 1 ORDER BY 2 DESC,1"
                    ).fetchall()
                },
                "planet_parameter_subjects": int(con.execute("SELECT count(*) FROM selected_planet_parameters").fetchone()[0]),
            }
        finally:
            con.close()
    else:
        failures["database"] = "missing"
    nonzero = {key: value for key, value in checks.items() if value}
    if nonzero:
        failures["invariants"] = nonzero
    return {
        "schema_version": "spacegate.e7_clean_science_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,"summaries": summaries,"failing_checks": failures,
        "wall_seconds": round(time.monotonic()-started,6),
    }


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root",type=Path,default=DEFAULT_ROOT)
    parser.add_argument("--build-id",required=True)
    parser.add_argument("--report",type=Path)
    args=parser.parse_args()
    report=verify(args.artifact_root.resolve()/args.build_id)
    rendered=json.dumps(report,indent=2,sort_keys=True)+"\n"
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(rendered,encoding="utf-8")
    print(rendered,end="")
    if report["status"]!="pass":
        raise SystemExit(1)


if __name__=="__main__":
    main()
