#!/usr/bin/env python3
"""Audit every materialized scene against the physical-scale v1 contract."""

from __future__ import annotations

import argparse
import gzip
import json
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VALID_APPLICABILITY = {"physical", "unavailable", "rejected"}


def _number(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _axis_value(extent: dict[str, Any]) -> float | None:
    axis = extent.get("semi_major_axis_au")
    if not isinstance(axis, dict):
        return None
    return _number(axis.get("value"))


@dataclass
class ErrorCollector:
    count: int = 0
    examples: list[dict[str, Any]] = field(default_factory=list)

    def add(self, path: Path, message: str) -> None:
        self.count += 1
        if len(self.examples) < 50:
            self.examples.append({"artifact": path.name, "error": message})


def audit(cache_dir: Path, expected_count: int | None) -> dict[str, Any]:
    paths = sorted(cache_dir.glob("system_*.json.gz"))
    applicability: Counter[str] = Counter()
    orbit_kinds: Counter[str] = Counter()
    root_bound_statuses: Counter[str] = Counter()
    errors = ErrorCollector()
    scene_count = 0
    stellar_orbit_count = 0
    planet_orbit_count = 0
    focus_node_count = 0
    bytes_total = 0

    for path in paths:
        bytes_total += path.stat().st_size
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception as exc:  # noqa: BLE001
            errors.add(path, f"unreadable artifact: {exc}")
            continue

        scene_count += 1
        materialization = payload.get("materialization") or {}
        render_scene = payload.get("render_scene") or {}
        physical = render_scene.get("physical_scale") or {}
        focus = render_scene.get("focus_graph") or {}
        visual = render_scene.get("visual_scale") or {}
        if materialization.get("materializer_version") != "simulation_scene_artifact_v17":
            errors.add(path, "materializer version is not v17")
        if physical.get("schema_version") != "simulation_physical_scale_v1":
            errors.add(path, "physical-scale schema mismatch")
        if focus.get("schema_version") != "simulation_focus_graph_v2":
            errors.add(path, "focus-graph schema mismatch")
        if visual.get("schema_version") != "visual_scale_v2":
            errors.add(path, "visual-scale schema mismatch")
        root_key = focus.get("root_focus_key")
        nodes = focus.get("nodes") or {}
        root = nodes.get(root_key) if isinstance(nodes, dict) else None
        if not isinstance(root, dict):
            errors.add(path, "missing root focus node")
        else:
            root_bounds = root.get("physical_bounds") or {}
            root_status = str(root_bounds.get("status") or "missing")
            root_bound_statuses[root_status] += 1
            root_radius = _number(root_bounds.get("radius_au"))
            root_applicability = str(root_bounds.get("view_applicability") or "missing")
            if root_applicability not in {"physical_layout", "local_neighborhood", "planet_orbit", "unavailable"}:
                errors.add(path, "root focus has invalid physical-view applicability")
            if root_applicability == "unavailable" and root_radius is not None:
                errors.add(path, "unavailable root focus carries a physical view radius")
            if root_applicability != "unavailable" and root_radius is None:
                errors.add(path, "available root focus lacks a physical view radius")
            if root_radius is None and root_status not in {"partial", "unavailable", "identity_only"}:
                errors.add(path, "root focus lacks an explicit unavailable or partial bound state")
        focus_node_count += len(nodes) if isinstance(nodes, dict) else 0

        rows: list[tuple[str, dict[str, Any]]] = []
        for orbit in render_scene.get("orbits") or []:
            stellar_orbit_count += 1
            rows.append((str(orbit.get("endpoint_kind") or "stellar_orbit"), orbit))
        bodies = render_scene.get("bodies") or {}
        for planet in bodies.get("planets") or []:
            planet_orbit_count += 1
            rows.append(("planet", planet))

        for kind, row in rows:
            orbit_kinds[kind] += 1
            extent = row.get("physical_extent")
            if not isinstance(extent, dict):
                errors.add(path, f"{kind} lacks physical_extent")
                continue
            state = str(extent.get("applicability") or "missing")
            applicability[state] += 1
            axis = _axis_value(extent)
            if state not in VALID_APPLICABILITY:
                errors.add(path, f"{kind} has invalid applicability {state}")
            elif state == "physical" and (axis is None or axis <= 0):
                errors.add(path, f"{kind} physical extent lacks a positive axis")
            elif state != "physical" and axis is not None:
                errors.add(path, f"{kind} nonphysical extent carries an axis")
            if extent.get("presentation_radius_excluded") is not True:
                errors.add(path, f"{kind} does not exclude presentation radius")

    count_matches = expected_count is None or scene_count == expected_count
    if not count_matches:
        errors.add(cache_dir, f"scene count {scene_count} != expected {expected_count}")
    return {
        "schema_version": "spacegate.simulation_physical_cache_audit.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cache_dir": str(cache_dir),
        "expected_scene_count": expected_count,
        "scene_count": scene_count,
        "scene_count_matches": count_matches,
        "compressed_bytes": bytes_total,
        "stellar_orbit_count": stellar_orbit_count,
        "planet_orbit_count": planet_orbit_count,
        "focus_node_count": focus_node_count,
        "applicability_counts": dict(sorted(applicability.items())),
        "orbit_kind_counts": dict(sorted(orbit_kinds.items())),
        "root_bound_status_counts": dict(sorted(root_bound_statuses.items())),
        "error_count": errors.count,
        "error_examples": errors.examples,
        "status": "pass" if errors.count == 0 else "fail",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = audit(args.cache_dir.resolve(), args.expected_count)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("status", "scene_count", "error_count")}, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
