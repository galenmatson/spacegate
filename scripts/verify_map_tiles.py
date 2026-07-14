#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import struct
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_map_tiles import (
    MAGIC,
    RECORD_STRUCT,
    SPECTRAL_CODES,
    atomic_json,
    choose_display_name_info,
    utc_now,
)


SPECTRAL_CLASSES = tuple(SPECTRAL_CODES)


def read_tile(path: Path) -> tuple[dict[str, Any], dict[int, str], dict[int, str]]:
    compressed = path.read_bytes()
    raw = gzip.decompress(compressed)
    if raw[:8] != MAGIC:
        raise ValueError(f"Bad map tile magic: {path}")
    header_length = struct.unpack_from("<I", raw, 8)[0]
    header = json.loads(raw[12:12 + header_length])
    start = 12 + header_length
    count = int(header["emitted_count"])
    string_start = start + count * RECORD_STRUCT.size
    names: dict[int, str] = {}
    classes: dict[int, str] = {}
    for index in range(count):
        record = RECORD_STRUCT.unpack_from(raw, start + index * RECORD_STRUCT.size)
        system_id = int(record[0])
        name_offset = int(record[7])
        name_length = int(record[8])
        names[system_id] = raw[
            string_start + name_offset:string_start + name_offset + name_length
        ].decode("utf-8")
        classes[system_id] = SPECTRAL_CLASSES[int(record[19])]
    if len(names) != count:
        raise ValueError(f"Duplicate system IDs within tile: {path}")
    return header, names, classes


def expected_public_names(
    con: duckdb.DuckDBPyConnection,
    radius: int,
) -> dict[int, str]:
    aliases: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for system_id, alias_raw, alias_kind, alias_priority in con.execute(
        """
        select a.system_id, a.alias_raw, a.alias_kind, a.alias_priority
        from aliases a
        join systems s on s.system_id = a.system_id
        where a.target_type = 'system'
          and s.dist_ly <= ?
          and s.x_helio_ly is not null
          and s.y_helio_ly is not null
          and s.z_helio_ly is not null
        order by a.system_id, a.alias_priority, a.alias_kind, a.alias_raw
        """,
        [radius],
    ).fetchall():
        aliases[int(system_id)].append({
            "alias_raw": alias_raw,
            "alias_kind": alias_kind,
            "alias_priority": alias_priority,
        })
    return {
        int(system_id): str(choose_display_name_info(
            canonical_name,
            aliases.get(int(system_id), []),
            root_system=True,
            name_style="public_full",
        )["display_name"])
        for system_id, canonical_name in con.execute(
            """
            select system_id, system_name
            from systems
            where dist_ly <= ?
              and x_helio_ly is not null
              and y_helio_ly is not null
              and z_helio_ly is not null
            """,
            [radius],
        ).fetchall()
    }


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
    observed_names: dict[int, str] = {}
    observed_classes: dict[int, str] = {}
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
        header, names, classes = read_tile(path)
        ids = set(names)
        overlap = observed.intersection(ids)
        if overlap:
            raise ValueError(f"Cross-tile duplicate IDs in radius {radius}: {len(overlap)}")
        observed.update(ids)
        observed_names.update(names)
        observed_classes.update(classes)
        compressed_bytes += len(payload)
        if header["tile_id"] != tile["tile_id"] or not header["exact"]:
            raise ValueError(f"Tile header/manifest mismatch: {path}")
    missing = expected - len(observed)
    extra = len(observed) - expected
    name_mismatches: list[dict[str, Any]] = []
    class_mismatches: list[dict[str, Any]] = []
    if radius == 100:
        expected_names = expected_public_names(con, radius)
        name_mismatches = [
            {
                "system_id": system_id,
                "expected": expected_name,
                "observed": observed_names.get(system_id),
            }
            for system_id, expected_name in sorted(expected_names.items())
            if observed_names.get(system_id) != expected_name
        ]
        class_goldens = {"Sirius": "A", "LAWD 25": "WD"}
        for system_name, expected_class in class_goldens.items():
            row = con.execute(
                "select system_id from systems where system_name = ? and dist_ly <= ? order by system_id limit 1",
                [system_name, radius],
            ).fetchone()
            observed_class = observed_classes.get(int(row[0])) if row else None
            if observed_class != expected_class:
                class_mismatches.append({
                    "system_name": system_name,
                    "expected": expected_class,
                    "observed": observed_class,
                })
    passed = (
        missing == 0
        and extra == 0
        and not name_mismatches
        and not class_mismatches
        and int(manifest["counts"]["exact_emitted_systems"]) == expected
    )
    return {
        "radius_ly": radius,
        "passed": passed,
        "expected_systems": expected,
        "observed_unique_systems": len(observed),
        "missing_count": max(0, missing),
        "extra_count": max(0, extra),
        "public_name_mismatch_count": len(name_mismatches),
        "public_name_mismatch_examples": name_mismatches[:20],
        "representative_class_mismatch_count": len(class_mismatches),
        "representative_class_mismatches": class_mismatches,
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
