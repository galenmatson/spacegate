#!/usr/bin/env python3
"""Audit JPL Horizons typed projections against their immutable responses."""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compile_scientific_evidence import (  # noqa: E402
    DEFAULT_REGISTRY,
    DEFAULT_STATE,
    load_json,
    source_input,
    write_json,
)


SOURCE_IDS = {
    "solar_system.jpl_horizons_authority",
    "solar_system.jpl_horizons_artificial",
}
DEFAULT_REPORT = Path(
    "/data/spacegate/state/reports/evidence_lake_v2/"
    "e4_jpl_horizons_typed_source_audit.json"
)
PARSED_REQUIRED_FIELDS = {
    "source_pk",
    "object_name",
    "object_class",
    "object_kind",
    "parent_object_name",
    "horizons_command",
    "center_code",
    "epoch_tdb_jd",
    "eccentricity",
    "inclination_deg",
    "semi_major_axis_au",
    "orbital_period_days",
    "radius_km",
    "mass_kg",
    "target_body_name",
    "horizons_query_url",
    "horizons_response_path",
    "horizons_response_sha256",
    "operator_seed_version",
    "operator_seed_sha256",
    "retrieved_at",
    "source_row_hash",
}
RESPONSE_REQUIRED_FIELDS = {
    "source_pk",
    "object_name",
    "horizons_command",
    "center_code",
    "query_url",
    "query_parameters_json",
    "response_path",
    "response_sha256",
    "response_bytes",
    "retrieved_at",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def scalar(con: duckdb.DuckDBPyConnection, query: str, paths: list[Path]) -> int:
    return int(con.execute(query, [str(path) for path in paths]).fetchone()[0])


def audit(
    typed_root: Path,
    raw_root: Path,
    typed_manifest: dict[str, Any],
) -> dict[str, Any]:
    source_id = str(typed_manifest["source_id"])
    if source_id not in SOURCE_IDS:
        raise ValueError(f"unsupported JPL Horizons source: {source_id}")
    expected_parsed = (
        "sol_system_objects"
        if source_id.endswith("authority")
        else "sol_artificial_objects"
    )
    expected_responses = (
        "sol_system_horizons_responses"
        if source_id.endswith("authority")
        else "sol_artificial_horizons_responses"
    )
    tables = {str(row["source_name"]): row for row in typed_manifest["tables"]}
    missing_tables = sorted({expected_parsed, expected_responses} - set(tables))
    pending_tables = sorted(
        name for name, row in tables.items() if str(row.get("status")) != "typed"
    )
    parsed_fields = {
        str(row["name"])
        for row in tables.get(expected_parsed, {}).get("columns", [])
    }
    response_fields = {
        str(row["name"])
        for row in tables.get(expected_responses, {}).get("columns", [])
    }
    checks: dict[str, Any] = {
        "missing_required_tables": missing_tables,
        "pending_typed_tables": pending_tables,
        "missing_parsed_fields": sorted(PARSED_REQUIRED_FIELDS - parsed_fields),
        "missing_response_fields": sorted(RESPONSE_REQUIRED_FIELDS - response_fields),
    }
    summaries: dict[str, Any] = {"typed_tables": sorted(tables)}
    if not any(checks.values()):
        parsed_path = typed_root / str(tables[expected_parsed]["parquet_path"])
        response_path = typed_root / str(tables[expected_responses]["parquet_path"])
        con = duckdb.connect()
        try:
            checks.update(
                {
                    "parsed_manifest_row_count_delta": scalar(
                        con,
                        "select count(*) from read_parquet(?)",
                        [parsed_path],
                    )
                    - int(tables[expected_parsed]["row_count"]),
                    "response_manifest_row_count_delta": scalar(
                        con,
                        "select count(*) from read_parquet(?)",
                        [response_path],
                    )
                    - int(tables[expected_responses]["row_count"]),
                    "duplicate_parsed_source_pk_excess": scalar(
                        con,
                        "select count(*)-count(distinct trim(source_pk)) "
                        "from read_parquet(?)",
                        [parsed_path],
                    ),
                    "duplicate_response_source_pk_excess": scalar(
                        con,
                        "select count(*)-count(distinct trim(source_pk)) "
                        "from read_parquet(?)",
                        [response_path],
                    ),
                    "missing_parsed_identity": scalar(
                        con,
                        "select count(*) from read_parquet(?) where "
                        "nullif(trim(source_pk),'') is null or "
                        "nullif(trim(object_name),'') is null or "
                        "nullif(trim(horizons_command),'') is null",
                        [parsed_path],
                    ),
                    "missing_response_identity": scalar(
                        con,
                        "select count(*) from read_parquet(?) where "
                        "nullif(trim(source_pk),'') is null or "
                        "nullif(trim(response_path),'') is null or "
                        "not regexp_full_match(trim(response_sha256),'[0-9a-f]{64}') or "
                        "try_cast(response_bytes as bigint) <= 0",
                        [response_path],
                    ),
                    "parsed_without_response": scalar(
                        con,
                        "select count(*) from read_parquet(?) p left join "
                        "read_parquet(?) r using(source_pk) where r.source_pk is null",
                        [parsed_path, response_path],
                    ),
                    "response_without_parsed": scalar(
                        con,
                        "select count(*) from read_parquet(?) r left join "
                        "read_parquet(?) p using(source_pk) where p.source_pk is null",
                        [response_path, parsed_path],
                    ),
                    "projection_response_metadata_mismatch": scalar(
                        con,
                        "select count(*) from read_parquet(?) p join read_parquet(?) r "
                        "using(source_pk) where trim(p.object_name)<>trim(r.object_name) "
                        "or trim(p.horizons_command)<>trim(r.horizons_command) "
                        "or trim(p.center_code)<>trim(r.center_code) "
                        "or trim(p.horizons_query_url)<>trim(r.query_url) "
                        "or trim(p.horizons_response_path)<>trim(r.response_path) "
                        "or trim(p.horizons_response_sha256)<>trim(r.response_sha256) "
                        "or trim(p.retrieved_at)<>trim(r.retrieved_at)",
                        [parsed_path, response_path],
                    ),
                    "invalid_orbit_values": scalar(
                        con,
                        "select count(*) from read_parquet(?) where "
                        "try_cast(eccentricity as double) < 0 or "
                        "try_cast(inclination_deg as double) not between 0 and 180 or "
                        "try_cast(semi_major_axis_au as double) = 0 or "
                        "try_cast(orbital_period_days as double) = 0",
                        [parsed_path],
                    ),
                    "missing_seed_lineage": scalar(
                        con,
                        "select count(*) from read_parquet(?) where "
                        "nullif(trim(operator_seed_version),'') is null or "
                        "not regexp_full_match(trim(operator_seed_sha256),'[0-9a-f]{64}')",
                        [parsed_path],
                    ),
                }
            )
            response_rows = con.execute(
                "select response_path,response_sha256,try_cast(response_bytes as bigint) "
                "from read_parquet(?) order by source_pk",
                [str(response_path)],
            ).fetchall()
            summaries["coverage"] = {
                "parsed_rows": scalar(
                    con, "select count(*) from read_parquet(?)", [parsed_path]
                ),
                "response_rows": len(response_rows),
                "rows_with_radius": scalar(
                    con,
                    "select count(*) from read_parquet(?) "
                    "where try_cast(radius_km as double)>0",
                    [parsed_path],
                ),
                "rows_with_mass": scalar(
                    con,
                    "select count(*) from read_parquet(?) "
                    "where try_cast(mass_kg as double)>0",
                    [parsed_path],
                ),
                "object_kinds": [
                    {"object_kind": row[0], "row_count": int(row[1])}
                    for row in con.execute(
                        "select trim(object_kind),count(*) from read_parquet(?) "
                        "group by 1 order by 1",
                        [str(parsed_path)],
                    ).fetchall()
                ],
            }
        finally:
            con.close()
        response_root = raw_root / "artifacts" / expected_responses
        raw_root_resolved = response_root.resolve()
        missing_files = 0
        escaping_paths = 0
        checksum_mismatches = 0
        byte_mismatches = 0
        for relative, expected_hash, expected_bytes in response_rows:
            candidate = (response_root / str(relative)).resolve()
            if raw_root_resolved not in candidate.parents:
                escaping_paths += 1
                continue
            if not candidate.is_file():
                missing_files += 1
                continue
            checksum_mismatches += sha256_file(candidate) != str(expected_hash)
            byte_mismatches += candidate.stat().st_size != int(expected_bytes)
        checks.update(
            {
                "escaping_response_paths": escaping_paths,
                "missing_response_files": missing_files,
                "response_checksum_mismatches": checksum_mismatches,
                "response_byte_mismatches": byte_mismatches,
            }
        )
    failed = any(
        bool(value) if isinstance(value, list) else int(value) != 0
        for value in checks.values()
    )
    return {
        "schema_version": "spacegate.jpl_horizons_typed_source_audit.v1",
        "status": "fail" if failed else "pass",
        "source_id": source_id,
        "release_id": typed_manifest["release_id"],
        "raw_snapshot_id": typed_manifest["snapshot_id"],
        "typed_snapshot_id": typed_manifest["typed_snapshot_id"],
        "typed_content_sha256": typed_manifest["content_sha256"],
        "checks": checks,
        "summaries": summaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=sorted(SOURCE_IDS), required=True)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    registry = load_json(args.registry)
    source = next(
        row for row in registry["sources"] if str(row["source_id"]) == args.source
    )
    resolved = source_input(args.state_dir, source)
    report = audit(
        resolved["typed_path"],
        resolved["raw_path"],
        resolved["typed_manifest"],
    )
    write_json(args.report, report)
    coverage = report["summaries"].get("coverage", {})
    print(
        f"JPL Horizons typed source {report['status']}: "
        f"source={report['source_id']} rows={coverage.get('parsed_rows', 0):,} "
        f"responses={coverage.get('response_rows', 0):,}"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
