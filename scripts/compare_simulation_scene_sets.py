#!/usr/bin/env python3
"""Compare independently named simulation-scene sets for logical determinism."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"scene is not an object: {path}")
    return payload


def _normalize_public_build_id(value: Any, public_build_id: str) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                "<PUBLIC_BUILD_ID>"
                if key == "build_id" and child == public_build_id
                else _normalize_public_build_id(child, public_build_id)
            )
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_normalize_public_build_id(child, public_build_id) for child in value]
    return value


def _logical_bytes(payload: dict[str, Any]) -> bytes:
    public_build_id = str(payload.get("build_id") or "")
    if not public_build_id:
        raise ValueError("scene lacks build_id")
    normalized = _normalize_public_build_id(payload, public_build_id)
    return json.dumps(
        normalized,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def compare(before_dir: Path, after_dir: Path, expected_count: int | None) -> dict[str, Any]:
    before_paths = {path.name: path for path in before_dir.glob("system_*.json.gz")}
    after_paths = {path.name: path for path in after_dir.glob("system_*.json.gz")}
    shared = sorted(before_paths.keys() & after_paths.keys())
    differing: list[dict[str, str]] = []
    differing_count = 0
    combined_before = hashlib.sha256()
    combined_after = hashlib.sha256()

    for name in shared:
        before_digest = hashlib.sha256(_logical_bytes(_load(before_paths[name]))).hexdigest()
        after_digest = hashlib.sha256(_logical_bytes(_load(after_paths[name]))).hexdigest()
        combined_before.update(f"{name}\0{before_digest}\n".encode("ascii"))
        combined_after.update(f"{name}\0{after_digest}\n".encode("ascii"))
        if before_digest != after_digest:
            differing_count += 1
            if len(differing) < 100:
                differing.append(
                    {
                        "artifact": name,
                        "before_logical_sha256": before_digest,
                        "after_logical_sha256": after_digest,
                    }
                )

    expected_matches = expected_count is None or (
        len(before_paths) == len(after_paths) == expected_count
    )
    complete = (
        len(before_paths) == len(after_paths) == len(shared)
        and expected_matches
    )
    logical_match = (
        complete
        and differing_count == 0
        and combined_before.digest() == combined_after.digest()
    )
    return {
        "schema_version": "spacegate.simulation_scene_set_logical_determinism.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "normalization_policy": {
            "version": "public_build_identity_only_v1",
            "description": (
                "Replace values of build_id fields only when they equal the scene's "
                "top-level public build identity; preserve all scientific and source build IDs."
            ),
        },
        "before_dir": str(before_dir),
        "after_dir": str(after_dir),
        "expected_scene_count": expected_count,
        "before_scene_count": len(before_paths),
        "after_scene_count": len(after_paths),
        "shared_scene_count": len(shared),
        "missing_from_before": sorted(after_paths.keys() - before_paths.keys()),
        "missing_from_after": sorted(before_paths.keys() - after_paths.keys()),
        "before_logical_set_sha256": combined_before.hexdigest(),
        "after_logical_set_sha256": combined_after.hexdigest(),
        "differing_scene_count": differing_count,
        "differing_scene_examples": differing,
        "status": "pass" if logical_match else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-dir", required=True, type=Path)
    parser.add_argument("--after-dir", required=True, type=Path)
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = compare(
        args.before_dir.resolve(),
        args.after_dir.resolve(),
        args.expected_count,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "before_scene_count",
                    "after_scene_count",
                    "shared_scene_count",
                    "differing_scene_count",
                )
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
