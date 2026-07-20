#!/usr/bin/env python3
"""Build a checksum-pinned Gaia DR3 uncertainty-envelope acquisition seed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb


DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
SOURCE_TABLE = "gaia_dr3_source_uncertain_distance_supplement_v1"
SCHEMA_VERSION = "spacegate.gaia_uncertainty_target_seed.v1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build(typed_manifest_path: Path, output_root: Path) -> dict[str, object]:
    typed_manifest = json.loads(typed_manifest_path.read_text(encoding="utf-8"))
    tables = {
        str(table["source_name"]): table for table in typed_manifest.get("tables", [])
    }
    if SOURCE_TABLE not in tables:
        raise ValueError(f"typed manifest lacks {SOURCE_TABLE}")
    table = tables[SOURCE_TABLE]
    parquet_path = typed_manifest_path.parent / str(table["parquet_path"])
    if sha256(parquet_path) != str(table["sha256"]):
        raise ValueError("typed uncertainty-envelope Parquet checksum mismatch")
    with duckdb.connect() as con:
        source_ids = [
            str(row[0])
            for row in con.execute(
                "select distinct cast(source_id as varchar) source_id "
                "from read_parquet(?) where source_id is not null order by source_id",
                [str(parquet_path)],
            ).fetchall()
        ]
    expected_rows = int(table["row_count"])
    if len(source_ids) != expected_rows:
        raise ValueError(
            f"uncertainty target identity is not one-to-one: {len(source_ids)} != {expected_rows}"
        )
    if any(not value.isdecimal() for value in source_ids):
        raise ValueError("Gaia target seed contains a non-decimal source ID")

    identity = {
        "schema_version": SCHEMA_VERSION,
        "typed_content_sha256": typed_manifest["content_sha256"],
        "typed_snapshot_id": typed_manifest["snapshot_id"],
        "source_table": SOURCE_TABLE,
        "source_table_sha256": table["sha256"],
        "source_id_count": len(source_ids),
        "source_ids_sha256": stable_hash(source_ids),
    }
    fingerprint = stable_hash(identity)
    build_id = fingerprint[:24]
    destination = output_root / build_id
    manifest_path = destination / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("input_fingerprint") != fingerprint:
            raise ValueError("immutable Gaia target-seed identity mismatch")
        return existing
    output_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        artifact_path = temporary / "gaia_dr3_source_ids.json"
        write_json(artifact_path, {"source_ids": source_ids})
        artifact = {
            "path": artifact_path.name,
            "bytes": artifact_path.stat().st_size,
            "sha256": sha256(artifact_path),
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "build_id": build_id,
            "input_fingerprint": fingerprint,
            "created_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "artifacts": [artifact],
            "report": {
                **identity,
                "build_id": build_id,
                "coverage": "complete_gaia_dr3_uncertainty_envelope",
            },
        }
        write_json(temporary / "manifest.json", manifest)
        temporary.rename(destination)
        return manifest
    except Exception:
        for path in sorted(temporary.rglob("*"), reverse=True):
            path.unlink() if path.is_file() else path.rmdir()
        temporary.rmdir()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--typed-manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    output_root = (
        args.state_dir
        / "derived/evidence_lake_v2/acquisition_targets/gaia_dr3_uncertainty"
    )
    manifest = build(args.typed_manifest.resolve(), output_root)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        write_json(args.report, manifest)
    print(
        f"Gaia uncertainty target seed {manifest['build_id']}: "
        f"{manifest['report']['source_id_count']:,} source IDs"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
