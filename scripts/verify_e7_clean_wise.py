#!/usr/bin/env python3
"""Independently verify an E7 clean targeted-WISE artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path("/mnt/space/spacegate/e7-clean-wise")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_hash(path: Path) -> str:
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
    if manifest.get("legacy_wise_csv_copied") is not False:
        failures["legacy_wise_csv_copied"] = manifest.get("legacy_wise_csv_copied")
    if manifest.get("core_inventory_promoted") is not False:
        failures["core_inventory_promoted"] = manifest.get("core_inventory_promoted")
    product_failures = {}
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            product_failures[relative] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_hash(path)}
        if actual != {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}:
            product_failures[relative] = {"expected": expected, "actual": actual}
    if product_failures:
        failures["products"] = product_failures

    checks: dict[str, int] = {}
    summaries: dict[str, Any] = {}
    db = build_dir / "clean_wise.duckdb"
    if db.is_file():
        con = duckdb.connect(str(db), read_only=True)
        try:
            expected_counts = manifest.get("counts") or {}
            actual_counts = {
                table: int(con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
                for table in expected_counts
            }
            mismatches = {
                table: {"expected": int(expected), "actual": actual_counts[table]}
                for table, expected in expected_counts.items()
                if actual_counts[table] != int(expected)
            }
            if mismatches:
                failures["counts"] = mismatches
            checks = {
                "duplicate_target_queries": int(con.execute("SELECT count(*) FROM (SELECT catalog,target_index FROM wise_query_outcomes GROUP BY ALL HAVING count(*)<>1)").fetchone()[0]),
                "query_row_accounting_mismatch": int(con.execute("SELECT count(*) FROM wise_target_accounting WHERE source_row_count<>candidate_match_count").fetchone()[0]),
                "accepted_non_primary": int(con.execute("SELECT count(*) FROM infrared_source_matches WHERE conflict_status='accepted_match' AND (match_rank<>1 OR source_target_rank<>1)").fetchone()[0]),
                "multiply_accepted_sources": int(con.execute("SELECT count(*) FROM (SELECT source_catalog,source_key FROM infrared_source_matches WHERE conflict_status='accepted_match' GROUP BY ALL HAVING count(*)>1)").fetchone()[0]),
                "accepted_beyond_policy": int(con.execute("SELECT count(*) FROM infrared_source_matches WHERE conflict_status='accepted_match' AND angular_sep_arcsec>2.5").fetchone()[0]),
                "unknown_outcome": int(con.execute("SELECT count(*) FROM wise_target_accounting WHERE outcome NOT IN ('accepted','ambiguous','missing')").fetchone()[0]),
                "photometry_match_delta": abs(int(con.execute("SELECT count(*) FROM infrared_photometry").fetchone()[0]) - int(con.execute("SELECT count(*) FROM infrared_source_matches").fetchone()[0])),
                "motion_match_delta": abs(int(con.execute("SELECT count(*) FROM infrared_motion_evidence").fetchone()[0]) - int(con.execute("SELECT count(*) FROM infrared_source_matches").fetchone()[0])),
                "candidate_parallax_promoted": int(con.execute("SELECT count(*) FROM infrared_motion_evidence WHERE source_catalog='catwise' AND parallax_like_arcsec IS NOT NULL AND parallax_like_note IS NULL").fetchone()[0]),
                "metadata_stability_opened": int(con.execute("SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'").fetchone()[0]),
                "metadata_core_promoted": int(con.execute("SELECT count(*) FROM build_metadata WHERE key='core_inventory_promoted' AND value<>'0'").fetchone()[0]),
            }
            summaries = {
                "outcomes": {
                    str(row[0]): int(row[1])
                    for row in con.execute("SELECT outcome,count(*) FROM wise_target_accounting GROUP BY 1 ORDER BY 1").fetchall()
                },
                "match_status": {
                    str(row[0]): int(row[1])
                    for row in con.execute("SELECT conflict_status,count(*) FROM infrared_source_matches GROUP BY 1 ORDER BY 1").fetchall()
                },
                "query_fallbacks": {
                    str(row[0]): int(row[1])
                    for row in con.execute("SELECT catalog,count(*) FILTER(WHERE error_response_count>0) FROM wise_query_outcomes GROUP BY 1 ORDER BY 1").fetchall()
                },
            }
        finally:
            con.close()
    else:
        failures["database"] = "missing"
    nonzero = {key: value for key, value in checks.items() if value}
    if nonzero:
        failures["invariants"] = nonzero
    return {
        "schema_version": "spacegate.e7_clean_wise_verification.v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_id": manifest.get("build_id"),
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "summaries": summaries,
        "failing_checks": failures,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.artifact_root.resolve() / args.build_id)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
