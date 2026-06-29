#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

import requests


@dataclass(frozen=True)
class HierarchyFact:
    display_name: str
    facts: dict[str, Any]


@dataclass(frozen=True)
class BenchmarkCase:
    query: str
    expected_wds_id: str | None = None
    max_dist_ly: float | None = None
    min_star_count: int | None = None
    min_planet_count: int | None = None
    expected_aliases: tuple[str, ...] = ()
    required_hierarchy_names: tuple[str, ...] = ()
    required_hierarchy_facts: tuple[HierarchyFact, ...] = ()
    min_scene_stars: int | None = None
    min_scene_planets: int | None = None


BENCHMARKS: tuple[BenchmarkCase, ...] = (
    BenchmarkCase(
        "Castor",
        expected_wds_id="07346+3153",
        min_star_count=6,
        expected_aliases=("Alpha Geminorum",),
        required_hierarchy_names=("Castor AA", "Castor AB", "Castor BA", "Castor BB", "Castor CA", "Castor CB"),
        required_hierarchy_facts=(
            HierarchyFact("Castor AA", {"spectral_type_raw": "A1V", "spectral_class": "A", "mass_msun": 2.37}),
            HierarchyFact("Castor BA", {"spectral_type_raw": "A2Vm", "spectral_class": "A", "mass_msun": 1.79}),
            HierarchyFact("Castor CA", {"spectral_type_raw": "M0.5V", "spectral_class": "M", "mass_msun": 0.6}),
            HierarchyFact("Castor AB", {"spectral_type_raw": None, "spectral_class": None, "mass_msun": 0.39}),
            HierarchyFact("Castor BB", {"spectral_type_raw": None, "spectral_class": None, "mass_msun": 0.39}),
            HierarchyFact("Castor CB", {"spectral_type_raw": None, "spectral_class": None, "mass_msun": 0.6}),
        ),
        min_scene_stars=3,
    ),
    BenchmarkCase(
        "Nu Sco",
        expected_wds_id="16120-1928",
        min_star_count=7,
        expected_aliases=("Jabbah", "Nu Scorpii"),
        required_hierarchy_names=("14nu Sco AA", "14nu Sco AB", "14nu Sco BA", "14nu Sco BB", "14nu Sco CA", "14nu Sco CB"),
        required_hierarchy_facts=(
            HierarchyFact("14nu Sco AA", {"spectral_type_raw": "B3V", "spectral_class": "B", "mass_msun": 6.07}),
            HierarchyFact("14nu Sco AB", {"spectral_type_raw": None, "spectral_class": None, "mass_msun": 2.28}),
        ),
        min_scene_stars=3,
    ),
    BenchmarkCase(
        "Alpha Centauri",
        expected_wds_id="14396-6050",
        max_dist_ly=5.0,
        min_star_count=3,
        expected_aliases=("Toliman", "Alpha Cen"),
        min_scene_stars=3,
    ),
    BenchmarkCase(
        "Sirius",
        max_dist_ly=10.0,
        min_star_count=1,
        expected_aliases=("Alpha Canis Majoris",),
        min_scene_stars=1,
    ),
    BenchmarkCase(
        "TRAPPIST-1",
        max_dist_ly=45.0,
        min_star_count=1,
        min_planet_count=7,
        min_scene_stars=1,
        min_scene_planets=7,
    ),
    BenchmarkCase(
        "55 Cnc",
        max_dist_ly=45.0,
        min_star_count=1,
        min_planet_count=5,
        expected_aliases=("Copernicus", "Rho Cancri"),
        min_scene_stars=1,
        min_scene_planets=5,
    ),
    BenchmarkCase(
        "Sol",
        max_dist_ly=0.1,
        min_star_count=1,
        min_planet_count=8,
        expected_aliases=("Sun", "Solar System"),
        min_scene_stars=1,
        min_scene_planets=8,
    ),
    BenchmarkCase(
        "16 Cyg",
        expected_wds_id="19418+5032",
        min_star_count=3,
        min_planet_count=1,
        min_scene_stars=3,
        min_scene_planets=1,
    ),
)


def get_json(base_url: str, path: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> dict[str, Any]:
    response = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
    if response.status_code != 200:
        raise AssertionError(f"{path} expected 200, got {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def iter_hierarchy_nodes(node: dict[str, Any]):
    yield node
    for child in node.get("children") or []:
        if isinstance(child, dict):
            yield from iter_hierarchy_nodes(child)


def normalize(value: Any) -> str:
    return str(value or "").casefold().strip()


def assert_fact_matches(label: str, fact_key: str, expected: Any, actual: Any) -> None:
    if isinstance(expected, float):
        if actual is None or abs(float(actual) - expected) > 1e-6:
            raise AssertionError(f"{label} expected {fact_key}={expected!r}, got {actual!r}")
    elif expected != actual:
        raise AssertionError(f"{label} expected {fact_key}={expected!r}, got {actual!r}")


def verify_case(base_url: str, case: BenchmarkCase, warnings: list[str]) -> str:
    search = get_json(base_url, "/systems/search", params={"q": case.query, "limit": 5})
    items = search.get("items") or []
    if not items:
        raise AssertionError(f"{case.query}: search returned no results")
    item = items[0]
    system_id = item.get("system_id")
    if system_id is None:
        raise AssertionError(f"{case.query}: first result missing system_id")

    if case.expected_wds_id and item.get("wds_id") != case.expected_wds_id:
        raise AssertionError(f"{case.query}: expected WDS {case.expected_wds_id}, got {item.get('wds_id')!r}")
    if case.max_dist_ly is not None:
        dist_ly = item.get("dist_ly")
        if dist_ly is None or float(dist_ly) > case.max_dist_ly:
            raise AssertionError(f"{case.query}: expected distance <= {case.max_dist_ly}, got {dist_ly!r}")
    if case.min_star_count is not None and int(item.get("star_count") or 0) < case.min_star_count:
        raise AssertionError(f"{case.query}: expected at least {case.min_star_count} stars, got {item.get('star_count')!r}")
    if case.min_planet_count is not None and int(item.get("planet_count") or 0) < case.min_planet_count:
        raise AssertionError(
            f"{case.query}: expected at least {case.min_planet_count} planets, got {item.get('planet_count')!r}"
        )

    detail = get_json(base_url, f"/systems/{system_id}")
    system = detail.get("system") or {}
    alias_text = {
        normalize(system.get("display_name")),
        normalize(system.get("system_name")),
        *(normalize(value) for value in system.get("display_aliases") or []),
        *(normalize((alias or {}).get("alias_raw")) for alias in system.get("aliases") or []),
    }
    for expected_alias in case.expected_aliases:
        if normalize(expected_alias) not in alias_text:
            raise AssertionError(f"{case.query}: missing expected alias {expected_alias!r}")

    hierarchy = detail.get("hierarchy") or {}
    root = hierarchy.get("root")
    if not isinstance(root, dict):
        raise AssertionError(f"{case.query}: missing hierarchy root")
    counts = hierarchy.get("counts") or {}
    if case.min_star_count is not None and int(counts.get("stars") or 0) < case.min_star_count:
        raise AssertionError(
            f"{case.query}: hierarchy expected at least {case.min_star_count} stars, got {counts.get('stars')!r}"
        )
    hierarchy_nodes = list(iter_hierarchy_nodes(root))
    nodes_by_name = {str(node.get("display_name") or ""): node for node in hierarchy_nodes}
    for display_name in case.required_hierarchy_names:
        if display_name not in nodes_by_name:
            raise AssertionError(f"{case.query}: hierarchy missing node {display_name!r}")
    for expected in case.required_hierarchy_facts:
        node = nodes_by_name.get(expected.display_name)
        if not node:
            raise AssertionError(f"{case.query}: hierarchy missing fact node {expected.display_name!r}")
        facts = node.get("quick_facts")
        if not isinstance(facts, dict):
            raise AssertionError(f"{case.query}: hierarchy node {expected.display_name!r} missing quick_facts")
        for fact_key, expected_value in expected.facts.items():
            assert_fact_matches(f"{case.query} {expected.display_name}", fact_key, expected_value, facts.get(fact_key))

    for node in hierarchy_nodes:
        if str(node.get("component_type") or "") not in {"star", "stellar_component"}:
            facts = node.get("quick_facts") or {}
            if facts.get("spectral_type_raw") or facts.get("spectral_class"):
                raise AssertionError(
                    f"{case.query}: non-star hierarchy node {node.get('display_name')!r} carries spectral facts"
                )

    scene = get_json(base_url, f"/systems/{system_id}/simulation-scene")
    bodies = scene.get("bodies") or {}
    scene_stars = bodies.get("stars") or []
    scene_planets = bodies.get("planets") or []
    if case.min_scene_stars is not None and len(scene_stars) < case.min_scene_stars:
        raise AssertionError(f"{case.query}: expected at least {case.min_scene_stars} preview stars, got {len(scene_stars)}")
    if case.min_scene_planets is not None and len(scene_planets) < case.min_scene_planets:
        raise AssertionError(
            f"{case.query}: expected at least {case.min_scene_planets} preview planets, got {len(scene_planets)}"
        )

    hierarchy_star_count = int(counts.get("stars") or 0)
    if hierarchy_star_count > len(scene_stars) and hierarchy_star_count > 1:
        warnings.append(
            f"{case.query}: hierarchy has {hierarchy_star_count} stars but preview exposes {len(scene_stars)} star bodies"
        )
    hierarchy_planet_count = int((counts.get("type_counts") or {}).get("planet") or 0)
    if hierarchy_planet_count > len(scene_planets):
        warnings.append(
            f"{case.query}: hierarchy has {hierarchy_planet_count} planets but preview exposes {len(scene_planets)} planet bodies"
        )

    return (
        f"{case.query}: {system.get('display_name') or item.get('display_name')} "
        f"(system_id={system_id}, stars={counts.get('stars')}, planets={len(scene_planets)})"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify API behavior for known benchmark systems.")
    parser.add_argument("base_url", nargs="?", default="http://127.0.0.1:8000/api/v1")
    parser.add_argument(
        "--strict-preview",
        action="store_true",
        help="Treat preview-vs-hierarchy body-count differences as failures.",
    )
    args = parser.parse_args()

    warnings: list[str] = []
    summaries = [verify_case(args.base_url.rstrip("/"), case, warnings) for case in BENCHMARKS]
    print("Known-system API benchmark passed:")
    for summary in summaries:
        print(f"- {summary}")
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning}")
        if args.strict_preview:
            raise SystemExit("Strict preview mode failed due to warnings above.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Known-system API benchmark failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
