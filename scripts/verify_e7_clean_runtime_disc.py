#!/usr/bin/env python3
"""Independently verify an E7 clean runtime DISC artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-disc")


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


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> int:
    return int(con.execute(sql, params or []).fetchone()[0] or 0)


def verify(build_dir: Path) -> dict[str, Any]:
    started = time.monotonic()
    manifest = load_object(build_dir / "manifest.json")
    failures: dict[str, Any] = {}
    if manifest.get("schema_version") != "spacegate.e7_clean_runtime_disc_manifest.v1":
        failures["manifest_schema"] = manifest.get("schema_version")
    if manifest.get("status") != "pass":
        failures["manifest_status"] = manifest.get("status")
    if manifest.get("stability_databases_opened") != []:
        failures["stability_databases_opened"] = manifest.get("stability_databases_opened")
    for relative, expected in sorted((manifest.get("products") or {}).items()):
        path = build_dir / relative
        if not path.is_file():
            failures[f"product_{relative}"] = "missing"
            continue
        actual = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        identity = {"bytes": expected.get("bytes"), "sha256": expected.get("sha256")}
        if actual != identity:
            failures[f"product_{relative}"] = {"expected": identity, "actual": actual}

    db_path = build_dir / "disc.duckdb"
    parquet_path = build_dir / "coolness_scores.parquet"
    counts: dict[str, int] = {}
    checks: dict[str, int] = {}
    if db_path.is_file() and parquet_path.is_file():
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            if tables != {"build_metadata", "coolness_scores"}:
                failures["table_set"] = sorted(tables)
            counts = {
                "coolness_scores": scalar(con, "SELECT count(*) FROM coolness_scores"),
                "parquet_scores": scalar(con, f"SELECT count(*) FROM read_parquet('{str(parquet_path).replace(chr(39), chr(39)*2)}')"),
                "build_metadata": scalar(con, "SELECT count(*) FROM build_metadata"),
            }
            profile = manifest.get("coolness_profile") or {}
            checks = {
                "parquet_count_delta": counts["parquet_scores"] - counts["coolness_scores"],
                "duplicate_system_ids": scalar(con, "SELECT count(*) FROM (SELECT system_id FROM coolness_scores GROUP BY 1 HAVING count(*)<>1)"),
                "duplicate_ranks": scalar(con, "SELECT count(*) FROM (SELECT rank FROM coolness_scores GROUP BY 1 HAVING count(*)<>1)"),
                "noncontiguous_ranks": scalar(con, "SELECT CASE WHEN min(rank)=1 AND max(rank)=count(*) THEN 0 ELSE 1 END FROM coolness_scores"),
                "invalid_scores": scalar(con, "SELECT count(*) FROM coolness_scores WHERE NOT isfinite(score_total) OR score_total<0 OR score_total>100"),
                "invalid_profile": scalar(con, "SELECT count(*) FROM coolness_scores WHERE build_id<>? OR profile_id<>? OR profile_version<>?", [manifest.get("build_id"), profile.get("profile_id"), profile.get("profile_version")]),
                "parquet_identity_delta": scalar(con, f"SELECT count(*) FROM ((SELECT * FROM coolness_scores EXCEPT SELECT * FROM read_parquet('{str(parquet_path).replace(chr(39), chr(39)*2)}')) UNION ALL (SELECT * FROM read_parquet('{str(parquet_path).replace(chr(39), chr(39)*2)}') EXCEPT SELECT * FROM coolness_scores))"),
            }
            metadata = dict(con.execute("SELECT key,value FROM build_metadata").fetchall())
            expected_metadata = {
                "build_id": manifest.get("build_id"),
                "core_build_id": (manifest.get("inputs") or {}).get("clean_runtime_core", {}).get("build_id"),
                "arm_build_id": (manifest.get("inputs") or {}).get("clean_runtime_arm", {}).get("build_id"),
                "profile_id": profile.get("profile_id"),
                "profile_version": profile.get("profile_version"),
                "profile_hash": profile.get("profile_hash"),
                "weights_hash": profile.get("weights_hash"),
                "classification_authority": "selected_arm_only",
                "luminosity_proxy_scope": "disc_presentation_assumption",
            }
            metadata_delta = {
                key: {"expected": value, "actual": metadata.get(key)}
                for key, value in expected_metadata.items()
                if metadata.get(key) != value
            }
            if metadata_delta:
                failures["metadata"] = metadata_delta
        finally:
            con.close()
    failing_checks = {key: value for key, value in checks.items() if value != 0}
    if failing_checks:
        failures["checks"] = failing_checks
    return {
        "schema_version": "spacegate.e7_clean_runtime_disc_verification.v1",
        "build_id": manifest.get("build_id"),
        "verified_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if not failures else "fail",
        "counts": counts,
        "checks": checks,
        "failures": failures,
        "wall_seconds": round(time.monotonic() - started, 6),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = verify(args.artifact_root.resolve() / args.build_id)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
