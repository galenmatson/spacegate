#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import struct
import sys
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_map_tiles import MAGIC, RECORD_STRUCT, atomic_json, utc_now


def read_tile(path: Path) -> tuple[dict[str, Any], set[int]]:
    compressed = path.read_bytes()
    raw = gzip.decompress(compressed)
    if raw[:8] != MAGIC:
        raise ValueError(f"Bad map tile magic: {path}")
    header_length = struct.unpack_from("<I", raw, 8)[0]
    header = json.loads(raw[12:12 + header_length])
    start = 12 + header_length
    ids = {
        RECORD_STRUCT.unpack_from(raw, start + index * RECORD_STRUCT.size)[0]
        for index in range(int(header["emitted_count"]))
    }
    if len(ids) != int(header["emitted_count"]):
        raise ValueError(f"Duplicate system IDs within tile: {path}")
    return header, ids


def verify_radius(build_dir: Path, con: duckdb.DuckDBPyConnection, radius: int) -> dict[str, Any]:
    manifest_path = build_dir / "map_tiles" / f"radius-{radius}" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = int(con.execute(
        """
        select count(*) from systems
        where dist_ly <= ? and x_helio_ly is not null and y_helio_ly is not null and z_helio_ly is not null
        """,
        [radius],
    ).fetchone()[0])
    observed: set[int] = set()
    compressed_bytes = 0
    exact_tiles = [tile for tile in manifest["tiles"] if tile.get("exact")]
    for tile in exact_tiles:
        digest = str(tile["sha256"])
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError(f"Unsafe tile digest: {digest}")
        path = build_dir / "map_tiles" / f"radius-{radius}" / "tiles" / f"{digest}.sgtile.gz"
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError(f"Tile checksum mismatch: {path}")
        header, ids = read_tile(path)
        overlap = observed.intersection(ids)
        if overlap:
            raise ValueError(f"Cross-tile duplicate IDs in radius {radius}: {len(overlap)}")
        observed.update(ids)
        compressed_bytes += len(payload)
        if header["tile_id"] != tile["tile_id"] or not header["exact"]:
            raise ValueError(f"Tile header/manifest mismatch: {path}")
    missing = expected - len(observed)
    extra = len(observed) - expected
    passed = missing == 0 and extra == 0 and int(manifest["counts"]["exact_emitted_systems"]) == expected
    return {
        "radius_ly": radius,
        "passed": passed,
        "expected_systems": expected,
        "observed_unique_systems": len(observed),
        "missing_count": max(0, missing),
        "extra_count": max(0, extra),
        "exact_tiles": len(exact_tiles),
        "exact_compressed_bytes": compressed_bytes,
        "manifest_sha256": manifest["manifest_sha256"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Spacegate map-tile exact coverage and checksums.")
    parser.add_argument("--state-dir", type=Path, default=Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state")))
    parser.add_argument("--build-dir", type=Path, default=None)
    parser.add_argument("--radii", default="100,250")
    parser.add_argument("--report-path", type=Path, default=None)
    args = parser.parse_args()
    build_dir = (args.build_dir or (args.state_dir / "served" / "current")).resolve()
    radii = [int(value) for value in args.radii.split(",") if value.strip()]
    con = duckdb.connect(str(build_dir / "core.duckdb"), read_only=True)
    try:
        results = [verify_radius(build_dir, con, radius) for radius in radii]
        build_id = str(con.execute("select value from build_metadata where key='build_id'").fetchone()[0])
    finally:
        con.close()
    report = {
        "schema_version": "spacegate_map_tile_verification_v1",
        "generated_at": utc_now(),
        "build_id": build_id,
        "passed": all(result["passed"] for result in results),
        "results": results,
    }
    report_path = args.report_path or (args.state_dir / "reports" / build_id / "map_tile_verification_report.json")
    atomic_json(report_path, report)
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
