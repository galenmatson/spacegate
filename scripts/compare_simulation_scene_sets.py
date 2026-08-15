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


def _assumption_key(record: dict[str, Any], public_build_id: str) -> str:
    payload = {
        "build_id": public_build_id,
        "object_type": record.get("object_type"),
        "system_id": record.get("system_id"),
        "star_id": record.get("star_id"),
        "planet_id": record.get("planet_id"),
        "orbit_edge_id": record.get("orbit_edge_id"),
        "stable_object_key": record.get("stable_object_key"),
        "stable_component_key": record.get("stable_component_key"),
        "render_key": record.get("render_key"),
        "parameter_key": record.get("parameter_key"),
        "value_json": record.get("value_json"),
        "assumption_version": record.get("assumption_version"),
        "input_context_json": record.get("input_context_json"),
        "replacement_target": record.get("replacement_target"),
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_public_build_id(value: Any, public_build_id: str) -> Any:
    if isinstance(value, dict):
        build_keyed_assumption = (
            isinstance(value.get("assumption_key"), str)
            and value.get("build_id") == public_build_id
            and value["assumption_key"] == _assumption_key(value, public_build_id)
        )
        return {
            key: (
                "<PUBLIC_BUILD_ID>"
                if key == "build_id" and child == public_build_id
                else "<BUILD_KEYED_ASSUMPTION_KEY>"
                if key == "assumption_key" and build_keyed_assumption
                else _normalize_public_build_id(child, public_build_id)
            )
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_normalize_public_build_id(child, public_build_id) for child in value]
    return value


def _normalize_set_like_diagnostics(payload: dict[str, Any]) -> None:
    """Canonicalize only diagnostic collections whose API contract is set-like."""
    arm = payload.get("arm")
    if not isinstance(arm, dict):
        return
    components = arm.get("components")
    if isinstance(components, dict) and isinstance(components.get("items"), list):
        components["items"].sort(
            key=lambda item: (
                str(item.get("component_type") or ""),
                str(item.get("display_name") or ""),
                str(item.get("stable_component_key") or ""),
            )
        )
    hierarchy_edges = arm.get("hierarchy_edges")
    if isinstance(hierarchy_edges, dict) and isinstance(hierarchy_edges.get("items"), list):
        hierarchy_edges["items"].sort(
            key=lambda item: (
                -(float(item["confidence_score"]) if item.get("confidence_score") is not None else -1.0),
                str(item.get("parent_component_key") or ""),
                str(item.get("child_component_key") or ""),
                str(item.get("edge_kind") or ""),
                str(item.get("member_role") or ""),
                str(item.get("source_catalog") or ""),
            )
        )


def _logical_bytes(payload: dict[str, Any]) -> bytes:
    public_build_id = str(payload.get("build_id") or "")
    if not public_build_id:
        raise ValueError("scene lacks build_id")
    _normalize_set_like_diagnostics(payload)
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
            "version": "public_build_identity_verified_assumption_key_and_diagnostic_sets_v3",
            "description": (
                "Replace values of build_id fields only when they equal the scene's "
                "top-level public build identity. Normalize an assumption_key only after "
                "recomputing and matching its documented public-build-keyed hash. Sort only "
                "the ARM component and hierarchy-edge diagnostic collections, whose API "
                "contract is set-like. Preserve all scientific values and source build IDs."
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
