#!/usr/bin/env python3
"""Verify defensible wide-orbit presentation behavior through the public API."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_URL = "http://127.0.0.1:8000/api/v1"


@dataclass(frozen=True)
class WideOrbitCase:
    query: str
    min_stars: int = 2
    require_source_group_orbit: bool = False
    require_nested_orbit: bool = False
    require_assumed_orbit: bool = False
    require_skipped_overlap: bool = False
    require_active_orbit_label: str | None = None
    allow_unattached_source_orbits: bool = False


CASES = [
    WideOrbitCase("Alpha Centauri", min_stars=3, require_source_group_orbit=True, require_nested_orbit=True),
    WideOrbitCase("Tegmine", min_stars=5, require_source_group_orbit=True, require_nested_orbit=True, allow_unattached_source_orbits=True),
    WideOrbitCase("Fomalhaut", min_stars=3),
    WideOrbitCase("Xi Scorpii", min_stars=5, require_source_group_orbit=True, require_nested_orbit=True, allow_unattached_source_orbits=True),
    WideOrbitCase("eps Ind", min_stars=3, require_source_group_orbit=True),
    WideOrbitCase("Sirius", min_stars=2, require_assumed_orbit=True),
    WideOrbitCase("Castor", min_stars=6, require_source_group_orbit=True, require_nested_orbit=True),
    WideOrbitCase("Nu Sco", min_stars=7, require_source_group_orbit=True, require_nested_orbit=True, allow_unattached_source_orbits=True),
    WideOrbitCase("16 Cyg", min_stars=3, require_skipped_overlap=True, require_active_orbit_label="16 Cyg B A - 16 Cyg B B"),
]


def fetch_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as response:
        if response.status != 200:
            raise AssertionError(f"{url} expected 200, got {response.status}")
        return json.loads(response.read().decode("utf-8"))


def api_url(base_url: str, path: str, params: dict[str, Any] | None = None) -> str:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    return url


def search_system(base_url: str, query: str) -> dict[str, Any]:
    payload = fetch_json(api_url(base_url, "systems/search", {"q": query, "limit": 1, "sort": "match"}))
    items = payload.get("items") or []
    if not items:
        raise AssertionError(f"{query}: search returned no systems")
    return items[0]


def field_statuses(orbit: dict[str, Any]) -> set[str]:
    fields = orbit.get("fields") if isinstance(orbit.get("fields"), dict) else {}
    return {
        str(field.get("status") or "").lower()
        for field in fields.values()
        if isinstance(field, dict)
    }


def visual_orbit_field_statuses(orbit: dict[str, Any]) -> set[str]:
    fields = orbit.get("fields") if isinstance(orbit.get("fields"), dict) else {}
    return {
        str(field.get("status") or "").lower()
        for key, field in fields.items()
        if key in {"period_days", "semi_major_axis_au", "eccentricity", "inclination_deg"}
        and isinstance(field, dict)
        and field.get("value") is not None
    }


def assert_no_partial_active_barycenter_overlap(query: str, simulation_tree: dict[str, Any]) -> None:
    nodes = simulation_tree.get("nodes") if isinstance(simulation_tree, dict) else {}
    barycenters = [
        (key, set(str(item) for item in (node.get("leaf_body_keys") or [])))
        for key, node in (nodes or {}).items()
        if isinstance(node, dict) and node.get("node_type") == "barycenter"
    ]
    for idx, (left_key, left_leaves) in enumerate(barycenters):
        for right_key, right_leaves in barycenters[idx + 1:]:
            overlap = left_leaves & right_leaves
            if not overlap:
                continue
            if left_leaves <= right_leaves or right_leaves <= left_leaves:
                continue
            raise AssertionError(
                f"{query}: active simulation-tree barycenters partially overlap: "
                f"{left_key}={sorted(left_leaves)} and {right_key}={sorted(right_leaves)}"
            )


def verify_case(base_url: str, case: WideOrbitCase) -> tuple[str, list[str]]:
    item = search_system(base_url, case.query)
    system_id = item.get("system_id")
    if not system_id:
        raise AssertionError(f"{case.query}: missing system_id")
    scene = fetch_json(api_url(base_url, f"systems/{system_id}/simulation-scene"))
    render_scene = scene.get("render_scene") or {}
    bodies = render_scene.get("bodies") or {}
    stars = bodies.get("stars") or []
    orbits = render_scene.get("orbits") or []
    diagnostics = render_scene.get("diagnostics") or {}
    orbit_counts = diagnostics.get("orbit_counts") or {}
    policy_counts = orbit_counts.get("by_policy") or {}
    simulation_tree = render_scene.get("simulation_tree") or {}
    tree_diagnostics = simulation_tree.get("diagnostics") or {}
    membership = diagnostics.get("membership_reconciliation") or {}

    if len(stars) < case.min_stars:
        raise AssertionError(f"{case.query}: expected >= {case.min_stars} rendered stars, got {len(stars)}")
    if len(stars) > 1 and not orbits:
        raise AssertionError(f"{case.query}: multi-star system has no rendered orbit or visual fallback")
    if int(orbit_counts.get("total") or 0) != len(orbits):
        raise AssertionError(f"{case.query}: orbit diagnostics total mismatch")
    if not isinstance(policy_counts, dict):
        raise AssertionError(f"{case.query}: missing orbit policy diagnostics")
    assert_no_partial_active_barycenter_overlap(case.query, simulation_tree)

    source_group_orbits = [
        orbit
        for orbit in orbits
        if orbit.get("endpoint_kind") == "group_pair" and "source" in field_statuses(orbit)
    ]
    assumed_orbits = [
        orbit
        for orbit in orbits
        if orbit.get("relation_kind") == "visual_binary_fallback" or "assumed" in visual_orbit_field_statuses(orbit)
    ]
    derived_projection_orbits = [
        orbit
        for orbit in orbits
        if ((orbit.get("fields") or {}).get("projected_separation_au") or {}).get("status") == "derived"
    ]

    if case.require_source_group_orbit and not source_group_orbits:
        raise AssertionError(f"{case.query}: expected at least one source-backed group-pair orbit")
    if case.require_nested_orbit and int(tree_diagnostics.get("nested_orbit_count") or 0) < 1:
        raise AssertionError(f"{case.query}: expected nested simulation-tree orbit")
    if case.require_assumed_orbit and not assumed_orbits:
        raise AssertionError(f"{case.query}: expected an explicitly assumed visual orbit/fallback")
    if case.require_skipped_overlap:
        skipped = tree_diagnostics.get("skipped_orbits") or []
        if not any("overlap" in str(item.get("reason") or "") for item in skipped if isinstance(item, dict)):
            raise AssertionError(f"{case.query}: expected skipped overlapping orbit diagnostics")
    if case.require_active_orbit_label:
        nodes = simulation_tree.get("nodes") or {}
        active_labels = [
            str(node.get("display_name") or "")
            for node in nodes.values()
            if isinstance(node, dict) and node.get("node_type") == "barycenter"
        ]
        if not any(case.require_active_orbit_label in label for label in active_labels):
            raise AssertionError(f"{case.query}: expected active orbit label containing {case.require_active_orbit_label!r}; got {active_labels}")
    if not case.allow_unattached_source_orbits and int(tree_diagnostics.get("unattached_orbit_count") or 0) > 0:
        raise AssertionError(f"{case.query}: unexpected unattached rendered orbit: {tree_diagnostics.get('warnings')}")

    if int(membership.get("unmatched_orbit_endpoint_count") or 0) > 0 and not case.allow_unattached_source_orbits:
        raise AssertionError(f"{case.query}: unmatched orbit endpoints: {membership.get('unmatched_orbit_endpoint_keys')}")

    warnings: list[str] = []
    if int(tree_diagnostics.get("unattached_orbit_count") or 0) > 0:
        warnings.append(
            f"{case.query}: {tree_diagnostics.get('unattached_orbit_count')} alternate/conflicting source orbit(s) remain unattached: "
            f"{tree_diagnostics.get('warnings') or []}"
        )

    summary = (
        f"{case.query}: system_id={system_id}, display={item.get('display_name')}, "
        f"stars={len(stars)}, orbits={len(orbits)}, "
        f"source_group={len(source_group_orbits)}, derived_projection={len(derived_projection_orbits)}, "
        f"assumed={len(assumed_orbits)}, nested={tree_diagnostics.get('nested_orbit_count') or 0}"
    )
    return summary, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", nargs="?", default=DEFAULT_BASE_URL)
    args = parser.parse_args()

    summaries: list[str] = []
    warnings: list[str] = []
    for case in CASES:
        summary, case_warnings = verify_case(args.base_url, case)
        summaries.append(summary)
        warnings.extend(case_warnings)

    print("Wide-orbit presentation benchmark passed:")
    for summary in summaries:
        print(f"- {summary}")
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Wide-orbit presentation benchmark failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
