#!/usr/bin/env python3
"""Record simulator scale contracts for reproducible before/after comparison."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import ssl
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_TARGETS = [
    "Alpha Centauri",
    "Castor",
    "Sol",
    "TRAPPIST-1",
    "eps Ind",
    "HD 57041",
]


def _get_json(base_url: str, path: str) -> tuple[dict[str, Any], int]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
    )
    context = ssl._create_unverified_context() if base_url.startswith("https://") else None
    with urllib.request.urlopen(request, context=context, timeout=90) as response:
        payload = response.read()
        wire_bytes = len(payload)
        if str(response.headers.get("content-encoding") or "").lower() == "gzip":
            payload = gzip.decompress(payload)
        return json.loads(payload), wire_bytes


def _field(fields: Any, key: str) -> dict[str, Any]:
    if isinstance(fields, dict):
        value = fields.get(key)
        return value if isinstance(value, dict) else {}
    return next((item for item in fields or [] if isinstance(item, dict) and item.get("key") == key), {})


def _number(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _hz_outer(star: dict[str, Any]) -> float | None:
    luminosity = _number(_field(star.get("fields"), "luminosity_lsun").get("value"))
    return math.sqrt(luminosity / 0.35) if luminosity is not None and luminosity > 0 else None


def _current_planet_radius(orbit_au: float, domain_au: float) -> float:
    return (orbit_au / domain_au) * 3.45


def _target_report(base_url: str, query: str) -> dict[str, Any]:
    search, search_wire_bytes = _get_json(
        base_url,
        f"api/v1/systems/search?q={urllib.parse.quote(query)}&limit=5",
    )
    items = search.get("items") or []
    if not items:
        return {"query": query, "status": "missing", "search_wire_bytes": search_wire_bytes}
    system = items[0]
    system_id = int(system["system_id"])
    scene, scene_wire_bytes = _get_json(base_url, f"api/v1/systems/{system_id}/simulation-scene")
    render_scene = scene.get("render_scene") or {}
    stars = (render_scene.get("bodies") or {}).get("stars") or []
    planets = (render_scene.get("bodies") or {}).get("planets") or []
    orbits = render_scene.get("orbits") or []
    hz_values = [value for value in (_hz_outer(star) for star in stars) if value is not None]
    planet_axes = [
        value
        for value in (_number(_field(planet.get("fields"), "semi_major_axis_au").get("value")) for planet in planets)
        if value is not None and value > 0
    ]
    local_domain = max([0.1, *hz_values, *planet_axes])
    orbit_rows = []
    for orbit in orbits:
        fields = orbit.get("fields") or {}
        physical = orbit.get("physical_extent") or {}
        orbit_rows.append(
            {
                "orbit_key": orbit.get("orbit_key"),
                "display_name": orbit.get("display_name"),
                "endpoint_kind": orbit.get("endpoint_kind"),
                "display_radius_scene": orbit.get("display_radius_scene"),
                "period_days": _field(fields, "period_days"),
                "semi_major_axis_au": _field(fields, "semi_major_axis_au"),
                "projected_separation_au": _field(fields, "projected_separation_au"),
                "physical_extent": physical or None,
            }
        )
    root_key = ((render_scene.get("focus_graph") or {}).get("root_focus_key"))
    root_focus = ((render_scene.get("focus_graph") or {}).get("nodes") or {}).get(root_key, {})
    return {
        "query": query,
        "status": "ok",
        "system_id": system_id,
        "display_name": (scene.get("system") or {}).get("display_name") or system.get("display_name"),
        "build_id": scene.get("build_id"),
        "contracts": {
            "render_scene": render_scene.get("schema_version"),
            "visual_scale": (render_scene.get("visual_scale") or {}).get("schema_version"),
            "simulation_tree": (render_scene.get("simulation_tree") or {}).get("schema_version"),
            "physical_scale": (render_scene.get("physical_scale") or {}).get("schema_version"),
            "focus_graph": (render_scene.get("focus_graph") or {}).get("schema_version"),
        },
        "wire_bytes": {"search": search_wire_bytes, "scene": scene_wire_bytes},
        "counts": {"stars": len(stars), "planets": len(planets), "stellar_orbits": len(orbits)},
        "current_mixed_scale": {
            "planet_hz_domain_au": round(local_domain, 9),
            "planet_orbit_samples": [
                {"semi_major_axis_au": round(axis, 9), "display_radius_scene": round(_current_planet_radius(axis, local_domain), 9)}
                for axis in sorted(planet_axes)
            ],
            "stellar_display_radii_scene": [orbit.get("display_radius_scene") for orbit in orbits],
            "single_scene_au_transform": False,
        },
        "physical_contract": {
            "root_focus_key": root_key,
            "root_bounds": root_focus.get("physical_bounds"),
            "orbit_applicability": {
                str(item.get("orbit_key")): ((item.get("physical_extent") or {}).get("applicability") or "not_available")
                for item in orbit_rows
            },
        },
        "orbits": orbit_rows,
    }


def _contract_checks(report: dict[str, Any]) -> dict[str, int]:
    contracts = report.get("contracts") or {}
    orbits = report.get("orbits") or []
    root_bounds = (report.get("physical_contract") or {}).get("root_bounds") or {}
    return {
        "physical_scale_schema_mismatch": int(contracts.get("physical_scale") != "simulation_physical_scale_v1"),
        "focus_graph_schema_mismatch": int(contracts.get("focus_graph") != "simulation_focus_graph_v1"),
        "visual_scale_schema_mismatch": int(contracts.get("visual_scale") != "visual_scale_v2"),
        "missing_root_physical_bound": int(_number(root_bounds.get("radius_au")) is None),
        "orbit_contract_missing": sum(int(not isinstance(item.get("physical_extent"), dict)) for item in orbits),
        "physical_orbit_missing_axis": sum(
            int(
                (item.get("physical_extent") or {}).get("applicability") == "physical"
                and _number((((item.get("physical_extent") or {}).get("semi_major_axis_au") or {}).get("value"))) is None
            )
            for item in orbits
        ),
        "unavailable_orbit_has_axis": sum(
            int(
                (item.get("physical_extent") or {}).get("applicability") == "unavailable"
                and _number((((item.get("physical_extent") or {}).get("semi_major_axis_au") or {}).get("value"))) is not None
            )
            for item in orbits
        ),
        "presentation_radius_not_excluded": sum(
            int((item.get("physical_extent") or {}).get("presentation_radius_excluded") is not True)
            for item in orbits
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="https://10.0.0.12")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--target", action="append", dest="targets")
    parser.add_argument("--require-contract", action="store_true")
    args = parser.parse_args()
    targets = args.targets or DEFAULT_TARGETS
    reports = [_target_report(args.base_url, target) for target in targets]
    if args.require_contract:
        for report in reports:
            report["checks"] = _contract_checks(report)
            report["status"] = "pass" if report.get("status") == "ok" and not any(report["checks"].values()) else "fail"
    output = {
        "schema_version": "spacegate.simulation_physical_scale_audit.v1",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "label": args.label,
        "base_url": args.base_url,
        "targets": reports,
        "status": "pass" if all(item.get("status") in {"ok", "pass"} for item in reports) else "fail",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": output["status"], "output": str(args.output), "targets": len(reports)}, indent=2))
    return 0 if output["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
