#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPORT_SCHEMA = "spacegate_derived_build_verification_v1"
UPSTREAM_REPORT_NAMES = (
    "derived_build_verification_report.json",
    "slice_policy_report.json",
    "qc_report.json",
    "match_report.json",
    "provenance_report.json",
    "duplicate_trap_report.json",
    "determinism_report.json",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"pragma table_info('{table_name}')").fetchall()}


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def read_metadata(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    if not scalar(
        con,
        "select count(*) from information_schema.tables where table_schema='main' and table_name='build_metadata'",
    ):
        return {}
    columns = table_columns(con, "build_metadata")
    if not {"key", "value"}.issubset(columns):
        return {}
    return {
        str(key): "" if value is None else str(value)
        for key, value in con.execute("select key, value from build_metadata").fetchall()
    }


def collect_checks(build_dir: Path) -> tuple[dict[str, int], dict[str, int], dict[str, str]]:
    core_path = build_dir / "core.duckdb"
    if not core_path.is_file():
        raise SystemExit(f"Missing derived CORE database: {core_path}")
    con = duckdb.connect(str(core_path), read_only=True)
    try:
        counts = {
            table: scalar(con, f"select count(*) from {table}")
            for table in ("systems", "stars", "planets", "aliases", "system_search_terms")
        }
        checks = {
            "duplicate_system_ids": scalar(con, "select count(*) - count(distinct system_id) from systems"),
            "duplicate_star_ids": scalar(con, "select count(*) - count(distinct star_id) from stars"),
            "duplicate_planet_ids": scalar(con, "select count(*) - count(distinct planet_id) from planets"),
            "duplicate_system_stable_keys": scalar(
                con,
                "select count(*) from (select stable_object_key from systems where stable_object_key is not null group by 1 having count(*) > 1)",
            ),
            "duplicate_star_stable_keys": scalar(
                con,
                "select count(*) from (select stable_object_key from stars where stable_object_key is not null group by 1 having count(*) > 1)",
            ),
            "duplicate_planet_stable_keys": scalar(
                con,
                "select count(*) from (select stable_object_key from planets where stable_object_key is not null group by 1 having count(*) > 1)",
            ),
            "dangling_star_systems": scalar(
                con,
                "select count(*) from stars st left join systems s using(system_id) where st.system_id is not null and s.system_id is null",
            ),
            "dangling_planet_systems": scalar(
                con,
                "select count(*) from planets p left join systems s using(system_id) where p.system_id is not null and s.system_id is null",
            ),
            "dangling_planet_stars": scalar(
                con,
                "select count(*) from planets p left join stars st using(star_id) where p.star_id is not null and st.star_id is null",
            ),
            "missing_required_parquet_artifacts": sum(
                not (build_dir / "parquet" / f"{name}.parquet").is_file()
                for name in ("systems", "stars", "planets", "aliases", "system_search_terms")
            ),
        }
        system_columns = table_columns(con, "systems")
        if {"dist_pc", "dist_ly"}.issubset(system_columns):
            checks["distance_invariant_violations"] = scalar(
                con,
                """
                select count(*) from systems
                where dist_pc is not null and dist_ly is not null
                  and abs(dist_ly - dist_pc * 3.26156) > greatest(1e-6, abs(dist_ly) * 1e-5)
                """,
            )
        metadata = read_metadata(con)
    finally:
        con.close()
    return counts, checks, metadata


def upstream_reports(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    for name in UPSTREAM_REPORT_NAMES:
        report_path = path / name
        if not report_path.is_file():
            continue
        build_id = None
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                build_id = payload.get("build_id")
        except (OSError, json.JSONDecodeError):
            pass
        rows.append(
            {
                "name": name,
                "bytes": report_path.stat().st_size,
                "sha256": sha256_file(report_path),
                "reported_build_id": build_id,
            }
        )
    return rows


def build_report(
    *, build_dir: Path, build_id: str, source_build_id: str, upstream_reports_dir: Path | None
) -> dict[str, Any]:
    counts, checks, metadata = collect_checks(build_dir)
    failures = sorted(name for name, value in checks.items() if value != 0)
    metadata_build_id = metadata.get("build_id")
    if metadata_build_id and metadata_build_id != build_id:
        failures.append("metadata_build_id_mismatch")
    return {
        "schema_version": REPORT_SCHEMA,
        "generated_at": utc_now(),
        "build_id": build_id,
        "source_build_id": source_build_id,
        "artifact_class": "side" if "side_artifact_rebuild_source_build_id" in metadata else "public_slice",
        "slice_profile_id": metadata.get("slice_profile_id"),
        "slice_profile_version": metadata.get("slice_profile_version"),
        "counts": counts,
        "checks": checks,
        "upstream_reports": upstream_reports(upstream_reports_dir),
        "failures": failures,
        "status": "pass" if not failures else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit or verify QC/provenance for immutable derived builds.")
    parser.add_argument("mode", choices=("emit", "verify"))
    parser.add_argument("--build-dir", required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--source-build-id", default="")
    parser.add_argument("--upstream-reports-dir", default="")
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    build_dir = Path(args.build_dir).resolve()
    report_path = Path(args.report).resolve()
    if args.mode == "emit":
        payload = build_report(
            build_dir=build_dir,
            build_id=args.build_id,
            source_build_id=args.source_build_id,
            upstream_reports_dir=(Path(args.upstream_reports_dir).resolve() if args.upstream_reports_dir else None),
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        if not report_path.is_file():
            raise SystemExit(f"Missing derived-build verification report: {report_path}")
        recorded = json.loads(report_path.read_text(encoding="utf-8"))
        source_build_id = str(recorded.get("source_build_id") or args.source_build_id)
        payload = build_report(
            build_dir=build_dir,
            build_id=args.build_id,
            source_build_id=source_build_id,
            upstream_reports_dir=None,
        )
        for key in ("schema_version", "build_id", "source_build_id", "counts", "checks", "status"):
            if recorded.get(key) != payload.get(key):
                raise SystemExit(f"Derived-build report mismatch for {key}")
        if recorded.get("status") != "pass":
            raise SystemExit(f"Derived-build verification failed: {recorded.get('failures')}")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
