#!/usr/bin/env python3
"""Rebuild physical simulation contracts twice and compare logical hashes."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app.main import _system_simulation_scene_payload


DEFAULT_SYSTEM_IDS = [17785920, 17784468, 17788040, 2982879, 17787631, 17784413]


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def contract(payload: dict[str, Any]) -> dict[str, Any]:
    render_scene = payload.get("render_scene") or {}
    return {
        "physical_scale": render_scene.get("physical_scale"),
        "focus_graph": render_scene.get("focus_graph"),
        "visual_scale": render_scene.get("visual_scale"),
        "orbit_physical_extents": [
            {
                "orbit_key": orbit.get("orbit_key"),
                "physical_extent": orbit.get("physical_extent"),
            }
            for orbit in render_scene.get("orbits") or []
        ],
        "planet_physical_extents": [
            {
                "object_key": planet.get("render_key") or planet.get("key"),
                "physical_extent": planet.get("physical_extent"),
            }
            for planet in ((render_scene.get("bodies") or {}).get("planets") or [])
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--system-id", action="append", type=int, dest="system_ids")
    args = parser.parse_args()
    rows = []
    for system_id in args.system_ids or DEFAULT_SYSTEM_IDS:
        first = contract(_system_simulation_scene_payload(system_id))
        second = contract(_system_simulation_scene_payload(system_id))
        first_hash = stable_hash(first)
        second_hash = stable_hash(second)
        rows.append(
            {
                "system_id": system_id,
                "first_logical_sha256": first_hash,
                "second_logical_sha256": second_hash,
                "deterministic": first_hash == second_hash,
                "focus_nodes": len((first.get("focus_graph") or {}).get("nodes") or {}),
                "physical_orbits": sum(
                    int((item.get("physical_extent") or {}).get("applicability") == "physical")
                    for item in first.get("orbit_physical_extents") or []
                ),
                "physical_planet_orbits": sum(
                    int((item.get("physical_extent") or {}).get("applicability") == "physical")
                    for item in first.get("planet_physical_extents") or []
                ),
            }
        )
    output = {
        "schema_version": "spacegate.simulation_physical_determinism.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "pass" if all(row["deterministic"] for row in rows) else "fail",
        "systems": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
