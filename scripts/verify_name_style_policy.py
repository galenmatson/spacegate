#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class NameStyleCase:
    query: str
    style: str
    expected_display_name: str
    expected_matched_alias: str | None = None
    forbidden_display_names: tuple[str, ...] = ()


CASES: tuple[NameStyleCase, ...] = (
    NameStyleCase("Alpha Centauri", "public_full", "Alpha Centauri", "Alpha Centauri", ("WDS 14396-6050", "Toliman")),
    NameStyleCase("Alpha Centauri", "astronomer_abbrev", "Alp Cen"),
    NameStyleCase("Alpha Centauri", "source_technical", "WDS 14396-6050"),
    NameStyleCase("eps ind", "public_full", "Epsilon Indi", "Eps Ind"),
    NameStyleCase("eps ind", "astronomer_abbrev", "Eps Ind", "Eps Ind"),
    NameStyleCase("Mu Her", "public_full", "Mu Herculis", "Mu Her"),
    NameStyleCase("Mu Her", "astronomer_abbrev", "Mu Her", "Mu Her"),
    NameStyleCase("Sirius", "public_full", "Sirius", "Sirius", ("Alpha Canis Majoris", "WDS 06451-1643")),
    NameStyleCase("Gliese 412", "public_full", "Gl 412A", "Gliese 412", ("Gliese 12",)),
    NameStyleCase("55 Cnc", "public_full", "55 Cnc", "55 Cnc"),
)


def get_json(base_url: str, path: str, *, params: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise AssertionError(f"{url} returned non-object JSON")
    return payload


def verify_search_case(base_url: str, case: NameStyleCase) -> int:
    payload = get_json(
        base_url,
        "/systems/search",
        params={"q": case.query, "limit": 1, "sort": "match", "name_style": case.style},
    )
    if payload.get("name_style") != case.style:
        raise AssertionError(f"{case.query}/{case.style} response name_style={payload.get('name_style')!r}")
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise AssertionError(f"{case.query}/{case.style} returned no results")
    first = items[0]
    if first.get("display_name") != case.expected_display_name:
        raise AssertionError(
            f"{case.query}/{case.style} expected display_name {case.expected_display_name!r}, "
            f"got {first.get('display_name')!r}"
        )
    if first.get("requested_name_style") != case.style:
        raise AssertionError(
            f"{case.query}/{case.style} item requested_name_style={first.get('requested_name_style')!r}"
        )
    if case.expected_matched_alias and first.get("matched_alias") != case.expected_matched_alias:
        raise AssertionError(
            f"{case.query}/{case.style} expected matched_alias {case.expected_matched_alias!r}, "
            f"got {first.get('matched_alias')!r}"
        )
    if first.get("display_name") in case.forbidden_display_names:
        raise AssertionError(f"{case.query}/{case.style} displayed forbidden name {first.get('display_name')!r}")
    return int(first.get("system_id"))


def verify_surface_consistency(base_url: str, *, query: str, style: str, expected_display_name: str) -> None:
    system_id = verify_search_case(
        base_url,
        NameStyleCase(query=query, style=style, expected_display_name=expected_display_name),
    )
    detail = get_json(base_url, f"/systems/{system_id}", params={"name_style": style})
    detail_system = detail.get("system") or {}
    if detail_system.get("display_name") != expected_display_name:
        raise AssertionError(f"detail display_name={detail_system.get('display_name')!r}")
    if detail_system.get("requested_name_style") != style:
        raise AssertionError(f"detail requested_name_style={detail_system.get('requested_name_style')!r}")

    scene = get_json(base_url, f"/systems/{system_id}/simulation-scene", params={"name_style": style}, timeout=60)
    scene_system = scene.get("system") or {}
    if scene_system.get("display_name") != expected_display_name:
        raise AssertionError(f"simulation display_name={scene_system.get('display_name')!r}")
    if scene_system.get("requested_name_style") != style:
        raise AssertionError(f"simulation requested_name_style={scene_system.get('requested_name_style')!r}")

    map_payload = get_json(
        base_url,
        "/map/systems",
        params={"max_dist_ly": 5, "limit": 2000, "compact": "true", "name_style": style},
        timeout=60,
    )
    map_item = next((item for item in map_payload.get("items") or [] if item.get("system_id") == system_id), None)
    if not map_item:
        raise AssertionError(f"system {system_id} not present in local map payload")
    if map_item.get("display_name") != expected_display_name:
        raise AssertionError(f"map display_name={map_item.get('display_name')!r}")
    if map_item.get("requested_name_style") != style:
        raise AssertionError(f"map requested_name_style={map_item.get('requested_name_style')!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Spacegate public display-name style policy.")
    parser.add_argument("base_url", nargs="?", default="http://127.0.0.1:8000/api/v1")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")

    failures: list[str] = []
    for case in CASES:
        try:
            verify_search_case(base_url, case)
            print(f"ok name-style search {case.query} [{case.style}]")
        except Exception as exc:
            failures.append(f"{case.query} [{case.style}]: {exc}")
            print(f"FAIL name-style search {case.query} [{case.style}]: {exc}", file=sys.stderr)

    for query, style, display_name in (
        ("Alpha Centauri", "public_full", "Alpha Centauri"),
        ("Alpha Centauri", "source_technical", "WDS 14396-6050"),
    ):
        try:
            verify_surface_consistency(base_url, query=query, style=style, expected_display_name=display_name)
            print(f"ok name-style surfaces {query} [{style}]")
        except Exception as exc:
            failures.append(f"surface {query} [{style}]: {exc}")
            print(f"FAIL name-style surfaces {query} [{style}]: {exc}", file=sys.stderr)

    if failures:
        print("\nName-style policy verification failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("Name-style policy verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
