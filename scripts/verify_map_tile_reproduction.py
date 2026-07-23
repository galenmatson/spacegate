#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalized_index(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    payload.pop("generated_at", None)
    return payload


def tile_payloads(root: Path, radius: int) -> dict[str, str]:
    tile_dir = root / f"radius-{radius}" / "tiles"
    return {
        path.name: file_sha256(path)
        for path in sorted(tile_dir.glob("*.sgtile.gz"))
    }


def compare(reference: Path, reproduction: Path, radii: list[int]) -> dict[str, Any]:
    if reference.resolve() == reproduction.resolve():
        raise ValueError("Reference and reproduction directories must differ")
    checks: dict[str, dict[str, Any]] = {}
    index_match = normalized_index(reference / "index.json") == normalized_index(
        reproduction / "index.json"
    )
    checks["index_contract"] = {"passed": index_match}

    radius_results: dict[str, Any] = {}
    for radius in radii:
        reference_manifest = load_json(reference / f"radius-{radius}" / "manifest.json")
        reproduction_manifest = load_json(
            reproduction / f"radius-{radius}" / "manifest.json"
        )
        reference_tiles = tile_payloads(reference, radius)
        reproduction_tiles = tile_payloads(reproduction, radius)
        manifest_match = (
            reference_manifest.get("manifest_sha256")
            == reproduction_manifest.get("manifest_sha256")
        )
        payload_match = reference_tiles == reproduction_tiles
        radius_results[str(radius)] = {
            "passed": manifest_match and payload_match,
            "manifest_sha256_match": manifest_match,
            "reference_manifest_sha256": reference_manifest.get("manifest_sha256"),
            "reproduction_manifest_sha256": reproduction_manifest.get("manifest_sha256"),
            "tile_payloads_match": payload_match,
            "reference_tile_count": len(reference_tiles),
            "reproduction_tile_count": len(reproduction_tiles),
            "missing_tiles": sorted(set(reference_tiles) - set(reproduction_tiles)),
            "extra_tiles": sorted(set(reproduction_tiles) - set(reference_tiles)),
            "differing_tiles": sorted(
                name
                for name in set(reference_tiles).intersection(reproduction_tiles)
                if reference_tiles[name] != reproduction_tiles[name]
            ),
        }

    passed = index_match and all(item["passed"] for item in radius_results.values())
    return {
        "schema_version": "spacegate_map_tile_reproduction_verification_v1",
        "reference": str(reference),
        "reproduction": str(reproduction),
        "radii_ly": radii,
        "timestamp_fields_ignored": ["index.generated_at", "manifest.generated_at"],
        "passed": passed,
        "checks": checks,
        "radius_results": radius_results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare deterministic map manifests and content-addressed tile payloads."
    )
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--reproduction", type=Path, required=True)
    parser.add_argument("--radii", default="100,250,500,1000")
    parser.add_argument("--report-path", type=Path, required=True)
    args = parser.parse_args()
    radii = [int(value) for value in args.radii.split(",") if value.strip()]
    report = compare(args.reference.resolve(), args.reproduction.resolve(), radii)
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
