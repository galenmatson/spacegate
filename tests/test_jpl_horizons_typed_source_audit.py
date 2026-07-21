from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_jpl_horizons_typed_source as horizons_audit  # noqa: E402


def write_parquet(path: Path, row: dict[str, str]) -> list[dict[str, str]]:
    con = duckdb.connect()
    fields = sorted(row)
    con.execute(
        "create table source(" + ",".join(f'\"{field}\" varchar' for field in fields) + ")"
    )
    con.execute(
        "insert into source values (" + ",".join("?" for _ in fields) + ")",
        [row[field] for field in fields],
    )
    con.execute("copy source to ? (format parquet)", [str(path)])
    con.close()
    return [{"name": field, "type": "VARCHAR"} for field in fields]


def test_horizons_audit_binds_projection_to_exact_response(tmp_path: Path) -> None:
    typed_root = tmp_path / "typed"
    raw_root = tmp_path / "raw"
    (typed_root / "tables").mkdir(parents=True)
    response_root = raw_root / "artifacts" / "sol_system_horizons_responses"
    (response_root / "responses").mkdir(parents=True)
    response_bytes = b"JPL Horizons exact response\n"
    response_file = response_root / "responses" / "1_sun.txt"
    response_file.write_bytes(response_bytes)
    response_sha256 = hashlib.sha256(response_bytes).hexdigest()
    common = {
        "source_pk": "1",
        "object_name": "Sun",
        "horizons_command": "10",
        "center_code": "500@0",
        "retrieved_at": "2026-07-21T00:00:00Z",
    }
    parsed = {
        **common,
        "object_class": "star",
        "object_kind": "star",
        "parent_object_name": "",
        "epoch_tdb_jd": "2461242.5",
        "eccentricity": "0",
        "inclination_deg": "0",
        "semi_major_axis_au": "",
        "orbital_period_days": "",
        "radius_km": "695700",
        "mass_kg": "1.98847e30",
        "target_body_name": "Sun (10)",
        "horizons_query_url": "https://ssd.jpl.nasa.gov/api/horizons.api?COMMAND=10",
        "horizons_response_path": "responses/1_sun.txt",
        "horizons_response_sha256": response_sha256,
        "operator_seed_version": "test-v1",
        "operator_seed_sha256": "a" * 64,
        "source_row_hash": "b" * 64,
    }
    response = {
        **common,
        "query_url": parsed["horizons_query_url"],
        "query_parameters_json": '{"COMMAND":"10"}',
        "response_path": parsed["horizons_response_path"],
        "response_sha256": response_sha256,
        "response_bytes": str(len(response_bytes)),
    }
    parsed_path = typed_root / "tables" / "sol_system_objects.parquet"
    response_path = typed_root / "tables" / "sol_system_horizons_responses.parquet"
    parsed_columns = write_parquet(parsed_path, parsed)
    response_columns = write_parquet(response_path, response)
    manifest = {
        "source_id": "solar_system.jpl_horizons_authority",
        "release_id": "test",
        "snapshot_id": "raw",
        "typed_snapshot_id": "typed",
        "content_sha256": "content",
        "tables": [
            {
                "source_name": "sol_system_objects",
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/sol_system_objects.parquet",
                "columns": parsed_columns,
            },
            {
                "source_name": "sol_system_horizons_responses",
                "status": "typed",
                "row_count": 1,
                "parquet_path": "tables/sol_system_horizons_responses.parquet",
                "columns": response_columns,
            },
        ],
    }
    report = horizons_audit.audit(typed_root, raw_root, manifest)
    assert report["status"] == "pass"
    assert not any(report["checks"].values())
    assert report["summaries"]["coverage"]["response_rows"] == 1

    response_file.write_bytes(b"changed\n")
    changed = horizons_audit.audit(typed_root, raw_root, manifest)
    assert changed["status"] == "fail"
    assert changed["checks"]["response_checksum_mismatches"] == 1
    assert changed["checks"]["response_byte_mismatches"] == 1
