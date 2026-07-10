#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

import requests


@dataclass(frozen=True)
class AliasCase:
    query: str
    expected_wds_id: str | None = None
    max_dist_ly: float | None = None
    min_star_count: int | None = None
    min_planet_count: int | None = None
    expected_names: tuple[str, ...] = ()
    expected_display_name: str | None = None
    forbidden_display_names: tuple[str, ...] = ()
    expected_matched_aliases: tuple[str, ...] = ()
    allow_unresolved: bool = False
    forbidden_names: tuple[str, ...] = ()
    note: str = ""


ALIAS_CASES: tuple[AliasCase, ...] = (
    AliasCase(
        "Alpha Centauri",
        expected_wds_id="14396-6050",
        max_dist_ly=5.0,
        min_star_count=3,
        min_planet_count=2,
        expected_names=("Alpha Centauri", "Toliman", "Alpha Cen"),
        expected_display_name="Alpha Centauri",
    ),
    AliasCase(
        "Proxima Centauri",
        expected_wds_id="14396-6050",
        max_dist_ly=5.0,
        min_star_count=3,
        min_planet_count=2,
        expected_names=("Proxima Centauri", "Alpha Centauri"),
    ),
    AliasCase(
        "HD 128620",
        expected_wds_id="14396-6050",
        max_dist_ly=5.0,
        min_star_count=3,
        expected_names=("Alpha Centauri", "Toliman", "Alpha Cen"),
        forbidden_display_names=("HD 128620",),
        expected_matched_aliases=("HD 128620",),
    ),
    AliasCase(
        "HIP 71683",
        expected_wds_id="14396-6050",
        max_dist_ly=5.0,
        min_star_count=3,
        expected_names=("Alpha Centauri", "Toliman", "Alpha Cen"),
        forbidden_display_names=("HIP 71683",),
        expected_matched_aliases=("HIP 71683",),
    ),
    AliasCase(
        "Gliese 412",
        max_dist_ly=17.0,
        expected_names=("Gliese 412", "Gl 412", "GJ 412"),
        forbidden_names=("Gliese 12",),
        expected_matched_aliases=("Gliese 412", "Gl 412", "GJ 412"),
        note="Gliese 412 may remain a membership-rollup benchmark gap, but it must resolve to the nearby Gl/GJ 412 source object.",
    ),
    AliasCase(
        "GJ 412",
        max_dist_ly=17.0,
        expected_names=("Gliese 412", "Gl 412", "GJ 412"),
        forbidden_names=("GJ 4122", "Gliese 12"),
        expected_matched_aliases=("GJ 412", "Gl 412"),
    ),
    AliasCase(
        "Alpha Librae",
        expected_wds_id="14509-1603",
        expected_names=("Alpha Librae", "Zubenelgenubi"),
    ),
    AliasCase(
        "Zubenelgenubi",
        expected_wds_id="14509-1603",
        expected_names=("Alpha Librae", "Zubenelgenubi"),
    ),
    AliasCase(
        "alf02 Lib",
        expected_wds_id="14509-1603",
        expected_names=("Alpha Librae", "Zubenelgenubi"),
        forbidden_display_names=("alf02 Lib",),
        expected_matched_aliases=("alf02 Lib",),
    ),
    AliasCase(
        "Gliese 643",
        expected_wds_id="16555-0820",
        max_dist_ly=22.0,
        min_star_count=5,
        expected_names=("V1054 Oph", "Gliese 643", "Gl 643", "VB 8"),
        expected_matched_aliases=("Gliese 643", "Gl 643"),
    ),
    AliasCase(
        "VB 8",
        expected_wds_id="16555-0820",
        max_dist_ly=22.0,
        min_star_count=5,
        expected_names=("V1054 Oph", "VB 8"),
    ),
    AliasCase(
        "V1513 Cyg",
        allow_unresolved=True,
        forbidden_names=("V1581 Cyg",),
        note="Exact-like variable-star queries must not fuzzy-resolve to a neighboring variable name.",
    ),
    AliasCase("Sirius", expected_wds_id="06451-1643", max_dist_ly=10.0, min_star_count=2, expected_names=("Sirius", "Alpha Canis Majoris")),
    AliasCase("Nu Sco", expected_wds_id="16120-1928", min_star_count=7, expected_names=("Nu Sco", "Jabbah", "Nu Scorpii")),
    AliasCase("Jabbah", expected_wds_id="16120-1928", min_star_count=7, expected_names=("Nu Sco", "Jabbah", "Nu Scorpii")),
    AliasCase("Castor", expected_wds_id="07346+3153", min_star_count=6, expected_names=("Castor", "Alpha Geminorum")),
    AliasCase("16 Cyg", expected_wds_id="19418+5032", min_star_count=2, min_planet_count=1, expected_names=("16 Cyg",)),
    AliasCase("eps Ind", expected_wds_id="22034-5647", min_star_count=3, expected_names=("Eps Ind", "eps Ind")),
    AliasCase("Tau Ceti", max_dist_ly=13.0, min_planet_count=3, expected_names=("Tau Ceti", "tau Cet")),
    AliasCase("TRAPPIST-1", max_dist_ly=45.0, min_planet_count=7, expected_names=("TRAPPIST-1",)),
    AliasCase("55 Cnc", max_dist_ly=45.0, min_planet_count=5, expected_names=("55 Cnc", "Copernicus")),
    AliasCase("Copernicus", max_dist_ly=45.0, min_planet_count=5, expected_names=("55 Cnc", "Copernicus")),
)


def normalize(value: Any) -> str:
    return str(value or "").casefold().strip()


def get_json(base_url: str, path: str, *, params: dict[str, Any] | None = None, timeout: int = 20) -> dict[str, Any]:
    response = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
    if response.status_code != 200:
        raise AssertionError(f"{path} expected 200, got {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return payload


def item_names(item: dict[str, Any]) -> set[str]:
    return {
        normalize(item.get("display_name")),
        normalize(item.get("system_name")),
        normalize(item.get("matched_alias")),
        *(normalize(value) for value in item.get("display_aliases") or []),
    }


def verify_case(base_url: str, case: AliasCase) -> None:
    payload = get_json(
        base_url,
        "/systems/search",
        params={"q": case.query, "limit": 8, "sort": "match"},
    )
    items = payload.get("items") or []
    if case.allow_unresolved and not items:
        return
    if not items:
        raise AssertionError(f"{case.query!r} returned no search results")

    first = items[0]
    names = item_names(first)
    forbidden = {normalize(value) for value in case.forbidden_names}
    if names & forbidden:
        raise AssertionError(f"{case.query!r} resolved to forbidden names {sorted(names & forbidden)}: {first!r}")

    display_name = normalize(first.get("display_name"))
    if case.expected_display_name and display_name != normalize(case.expected_display_name):
        raise AssertionError(
            f"{case.query!r} expected display_name {case.expected_display_name!r}, got {first.get('display_name')!r}"
        )
    forbidden_display = {normalize(value) for value in case.forbidden_display_names}
    if display_name in forbidden_display:
        raise AssertionError(f"{case.query!r} displayed forbidden primary name {first.get('display_name')!r}")

    if case.expected_wds_id and first.get("wds_id") != case.expected_wds_id:
        raise AssertionError(f"{case.query!r} expected WDS {case.expected_wds_id}, got {first.get('wds_id')!r}")

    if case.max_dist_ly is not None:
        dist_ly = first.get("dist_ly")
        if dist_ly is None or float(dist_ly) > case.max_dist_ly:
            raise AssertionError(f"{case.query!r} expected distance <= {case.max_dist_ly}, got {dist_ly!r}")

    if case.min_star_count is not None and int(first.get("star_count") or 0) < case.min_star_count:
        raise AssertionError(f"{case.query!r} expected >= {case.min_star_count} stars, got {first.get('star_count')!r}")

    if case.min_planet_count is not None and int(first.get("planet_count") or 0) < case.min_planet_count:
        raise AssertionError(f"{case.query!r} expected >= {case.min_planet_count} planets, got {first.get('planet_count')!r}")

    expected_names = {normalize(value) for value in case.expected_names}
    if expected_names and not (names & expected_names):
        raise AssertionError(
            f"{case.query!r} expected one of {sorted(case.expected_names)!r}, "
            f"got display={first.get('display_name')!r}, aliases={first.get('display_aliases')!r}, matched={first.get('matched_alias')!r}"
        )

    expected_matched = {normalize(value) for value in case.expected_matched_aliases}
    if expected_matched:
        matched_alias = normalize(first.get("matched_alias"))
        if matched_alias not in expected_matched:
            raise AssertionError(
                f"{case.query!r} expected matched_alias in {sorted(case.expected_matched_aliases)!r}, "
                f"got {first.get('matched_alias')!r}"
            )


def verify_alpha_centauri_surfaces(base_url: str) -> None:
    search_payload = get_json(
        base_url,
        "/systems/search",
        params={"q": "Alpha Centauri", "limit": 1, "sort": "match"},
    )
    search_item = (search_payload.get("items") or [None])[0]
    if not isinstance(search_item, dict):
        raise AssertionError("Alpha Centauri search returned no item")
    system_id = search_item.get("system_id")
    if not system_id:
        raise AssertionError(f"Alpha Centauri search returned no system_id: {search_item!r}")
    if search_item.get("display_name") != "Alpha Centauri":
        raise AssertionError(f"Alpha Centauri search display mismatch: {search_item.get('display_name')!r}")

    detail_payload = get_json(base_url, f"/systems/{system_id}")
    detail_system = detail_payload.get("system") or {}
    if detail_system.get("display_name") != "Alpha Centauri":
        raise AssertionError(f"Alpha Centauri detail display mismatch: {detail_system.get('display_name')!r}")

    scene_payload = get_json(base_url, f"/systems/{system_id}/simulation-scene", timeout=40)
    scene_system = scene_payload.get("system") or {}
    if scene_system.get("display_name") != "Alpha Centauri":
        raise AssertionError(f"Alpha Centauri simulation-scene display mismatch: {scene_system.get('display_name')!r}")

    map_payload = get_json(
        base_url,
        "/map/systems",
        params={"max_dist_ly": 5, "limit": 2000, "compact": "true"},
        timeout=40,
    )
    map_items = map_payload.get("items") or []
    map_item = next((item for item in map_items if item.get("system_id") == system_id), None)
    if not map_item:
        raise AssertionError("Alpha Centauri not found in 5 ly map payload")
    if map_item.get("display_name") != "Alpha Centauri":
        raise AssertionError(f"Alpha Centauri map display mismatch: {map_item.get('display_name')!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Spacegate alias authority and preferred display-name behavior.")
    parser.add_argument("base_url", nargs="?", default="http://127.0.0.1:8000/api/v1")
    args = parser.parse_args()

    failures: list[str] = []
    for case in ALIAS_CASES:
        try:
            verify_case(args.base_url.rstrip("/"), case)
            print(f"ok alias {case.query}")
        except Exception as exc:
            failures.append(f"{case.query}: {exc}")
            print(f"FAIL alias {case.query}: {exc}", file=sys.stderr)

    try:
        verify_alpha_centauri_surfaces(args.base_url.rstrip("/"))
        print("ok alias Alpha Centauri surface consistency")
    except Exception as exc:
        failures.append(f"Alpha Centauri surface consistency: {exc}")
        print(f"FAIL alias Alpha Centauri surface consistency: {exc}", file=sys.stderr)

    if failures:
        print("\nAlias authority verification failures:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(f"Alias authority verification passed for {len(ALIAS_CASES)} cases.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
