#!/usr/bin/env python3
import re
import sys
from typing import Any, Dict, Iterable, Optional

import requests


def require_keys(obj: Dict[str, Any], keys, label: str):
    missing = [key for key in keys if key not in obj]
    if missing:
        raise AssertionError(f"{label} missing keys: {missing}")


def assert_status(
    response: requests.Response,
    expected: int | Iterable[int],
    label: str,
) -> None:
    if isinstance(expected, int):
        expected_set = {expected}
    else:
        expected_set = set(expected)
    if response.status_code not in expected_set:
        snippet = response.text[:500]
        raise AssertionError(
            f"{label} expected status {sorted(expected_set)}, got {response.status_code}. Body: {snippet}"
        )


def get_json(
    base_url: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    expected_status: int | Iterable[int] = 200,
    timeout: int = 10,
    label: Optional[str] = None,
) -> tuple[requests.Response, Dict[str, Any]]:
    final_label = label or path
    response = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
    assert_status(response, expected_status, final_label)
    try:
        payload = response.json()
    except Exception as exc:
        raise AssertionError(f"{final_label} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise AssertionError(f"{final_label} returned non-object JSON")
    return response, payload


def assert_non_decreasing(values: list[float], label: str) -> None:
    for prev, current in zip(values, values[1:]):
        if current < prev:
            raise AssertionError(f"{label} not sorted ascending: {prev} then {current}")


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/api/v1"

    _, health_json = get_json(base_url, "/health", label="health")
    require_keys(health_json, ["status", "build_id", "db_path", "time_utc"], "health")

    _, search_json = get_json(
        base_url,
        "/systems/search",
        params={"q": "sol", "limit": 1},
        label="search basic",
    )
    require_keys(search_json, ["items", "has_more", "next_cursor"], "search")
    if not search_json["items"]:
        raise AssertionError("search returned no items")

    first = search_json["items"][0]
    require_keys(first, ["system_id", "system_name", "provenance"], "search item")

    _, detail_json = get_json(
        base_url,
        f"/systems/{first['system_id']}",
        label="detail",
    )
    require_keys(detail_json, ["system", "stars", "planets"], "detail")
    require_keys(detail_json["system"], ["system_id", "system_name", "provenance"], "detail.system")

    _, name_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "name", "limit": 2},
        label="search sort name",
    )
    require_keys(name_page, ["items", "has_more", "next_cursor"], "name page")
    ids_page1 = [item.get("system_id") for item in name_page["items"] if item.get("system_id") is not None]
    if name_page.get("has_more") and name_page.get("next_cursor"):
        _, name_page2 = get_json(
            base_url,
            "/systems/search",
            params={"sort": "name", "limit": 2, "cursor": name_page["next_cursor"]},
            label="search sort name page2",
        )
        ids_page2 = [item.get("system_id") for item in name_page2["items"] if item.get("system_id") is not None]
        overlap = set(ids_page1) & set(ids_page2)
        if overlap:
            raise AssertionError(f"Cursor pagination returned overlapping system_ids: {sorted(overlap)}")

    _, distance_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "distance", "limit": 20},
        label="search sort distance",
    )
    distance_values = [
        float(item["dist_ly"])
        for item in distance_page["items"]
        if item.get("dist_ly") is not None
    ]
    if len(distance_values) >= 2:
        assert_non_decreasing(distance_values, "distance sort")

    coolness_response = requests.get(
        f"{base_url}/systems/search",
        params={"sort": "coolness", "limit": 20},
        timeout=10,
    )
    assert_status(coolness_response, {200, 409}, "search sort coolness")
    if coolness_response.status_code == 200:
        coolness_page = coolness_response.json()
        ranks = [
            int(item["coolness_rank"])
            for item in coolness_page.get("items", [])
            if item.get("coolness_rank") is not None
        ]
        if len(ranks) >= 2:
            assert_non_decreasing([float(value) for value in ranks], "coolness rank sort")

    _, gaia_probe = get_json(
        base_url,
        "/systems/search",
        params={"sort": "coolness", "limit": 50},
        expected_status={200, 409},
        label="gaia probe page",
    )
    gaia_query = None
    gaia_probe_item = None
    if "items" in gaia_probe:
        for item in gaia_probe.get("items", []):
            key = str(item.get("stable_object_key") or "")
            match = re.search(r"(?:^|:)gaia:(\d+)$", key, flags=re.IGNORECASE)
            if match:
                gaia_query = match.group(1)
                gaia_probe_item = item
                break
    if gaia_query:
        _, gaia_search = get_json(
            base_url,
            "/systems/search",
            params={"q": gaia_query, "limit": 5},
            label="search raw gaia numeric",
        )
        if not gaia_search.get("items"):
            raise AssertionError("raw Gaia numeric query returned zero items")
        first_gaia = gaia_search["items"][0]
        if gaia_probe_item and first_gaia.get("stable_object_key") != gaia_probe_item.get("stable_object_key"):
            raise AssertionError("raw Gaia numeric query did not return the expected system first")
        gaia_text = first_gaia.get("gaia_id_text")
        if gaia_text is not None and not str(gaia_text).isdigit():
            raise AssertionError(f"gaia_id_text should be digit string, got {gaia_text!r}")

    _, total_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "coolness", "limit": 25, "include_total": "true"},
        expected_status={200, 409},
        label="search include_total",
    )
    if "items" in total_page:
        total_count = total_page.get("total_count")
        if not isinstance(total_count, int):
            raise AssertionError(f"include_total=true expected integer total_count, got: {total_count!r}")
        if total_count < len(total_page.get("items", [])):
            raise AssertionError(
                f"total_count should be >= items length ({total_count} < {len(total_page.get('items', []))})"
            )

    _, planets_true = get_json(
        base_url,
        "/systems/search",
        params={"has_planets": "true", "min_planet_count": 1, "limit": 20},
        label="search has_planets=true",
    )
    for item in planets_true["items"]:
        if int(item.get("planet_count") or 0) < 1:
            raise AssertionError("has_planets=true returned system with zero planets")

    _, planets_false = get_json(
        base_url,
        "/systems/search",
        params={"has_planets": "false", "max_planet_count": 0, "limit": 20},
        label="search has_planets=false",
    )
    for item in planets_false["items"]:
        if int(item.get("planet_count") or 0) != 0:
            raise AssertionError("has_planets=false returned system with planets")

    _, _ = get_json(
        base_url,
        "/systems/search",
        params={"has_habitable": "true", "limit": 20},
        label="search has_habitable=true",
    )

    validation_cases = [
        ({"min_dist_ly": 20, "max_dist_ly": 10}, 400, "Invalid distance range"),
        ({"min_star_count": 5, "max_star_count": 1}, 400, "Invalid star-count range"),
        ({"min_planet_count": 4, "max_planet_count": 1}, 400, "Invalid planet-count range"),
        ({"min_coolness_score": 30, "max_coolness_score": 5}, 400, "Invalid coolness-score range"),
        ({"has_planets": "maybe"}, 400, "Invalid has_planets filter"),
        ({"has_habitable": "sometimes"}, 400, "Invalid has_habitable filter"),
        ({"include_total": "sometimes"}, 400, "Invalid include_total filter"),
        ({"sort": "not-a-sort"}, 400, "Invalid sort option"),
        ({"spectral_class": "ZZ"}, 400, "Invalid spectral_class filter"),
        ({"cursor": "not_a_valid_cursor"}, 400, "Invalid cursor"),
        ({"min_dist_ly": -1}, 422, None),
        ({"limit": 999}, 422, None),
    ]
    for params, expected_status, expected_message in validation_cases:
        response = requests.get(f"{base_url}/systems/search", params=params, timeout=10)
        assert_status(response, expected_status, f"validation case params={params}")
        if expected_message:
            payload = response.json()
            message = str(payload.get("error", {}).get("message", ""))
            if expected_message not in message:
                raise AssertionError(
                    f"Expected message containing '{expected_message}' for params={params}, got '{message}'"
                )

    print("Integration test passed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Integration test failed: {exc}")
        sys.exit(1)
