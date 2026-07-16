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
    timeout: int = 30,
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


def assert_non_increasing(values: list[float], label: str) -> None:
    for prev, current in zip(values, values[1:]):
        if current > prev:
            raise AssertionError(f"{label} not sorted descending: {prev} then {current}")


def iter_hierarchy_nodes(node: Dict[str, Any]):
    yield node
    for child in node.get("children") or []:
        if isinstance(child, dict):
            yield from iter_hierarchy_nodes(child)


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/api/v1"

    _, health_json = get_json(base_url, "/health", label="health")
    require_keys(health_json, ["status", "build_id", "time_utc"], "health")

    _, seed_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "name", "limit": 1},
        label="seed page",
    )
    require_keys(seed_page, ["items", "has_more", "next_cursor"], "seed page")
    if not seed_page["items"]:
        raise AssertionError("seed page returned no items")
    seed_item = seed_page["items"][0]
    seed_query = (
        str(seed_item.get("display_name") or "").strip()
        or str(seed_item.get("system_name") or "").strip()
        or str(seed_item.get("stable_object_key") or "").strip()
    )
    if not seed_query:
        raise AssertionError("could not derive a seed query from first search result")

    _, search_json = get_json(
        base_url,
        "/systems/search",
        params={"q": seed_query, "limit": 1},
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

    _, scene_json = get_json(
        base_url,
        f"/systems/{first['system_id']}/simulation-scene",
        label="simulation scene",
    )
    require_keys(
        scene_json,
        ["schema_version", "scope", "frame", "system", "bodies", "arm", "simulation_readiness", "policy"],
        "simulation scene",
    )
    if scene_json["schema_version"] != "simulation_scene_v0":
        raise AssertionError(f"unexpected simulation scene schema_version: {scene_json['schema_version']!r}")
    require_keys(scene_json["bodies"], ["stars", "planets"], "simulation scene.bodies")
    require_keys(
        scene_json.get("render_scene") or {},
        ["schema_version", "bodies", "orbits", "visual_scale", "assumptions", "assumption_count"],
        "simulation scene.render_scene",
    )
    visual_scale = scene_json.get("render_scene", {}).get("visual_scale", {})
    if visual_scale.get("schema_version") != "visual_scale_beta_v1":
        raise AssertionError("simulation scene render visual scale policy missing")
    modes = {str(item.get("mode") or item.get("value") or "") for item in visual_scale.get("available_scale_modes", []) if isinstance(item, dict)}
    if not {"structure", "true_orbits", "true_bodies", "log"}.issubset(modes):
        raise AssertionError("simulation scene render visual scale modes missing")
    if not isinstance(visual_scale.get("collision_policy"), dict):
        raise AssertionError("simulation scene render visual scale collision policy missing")
    render_stars = ((scene_json.get("render_scene") or {}).get("bodies") or {}).get("stars") or []
    for star in render_stars:
        visual_class = (star.get("fields") or {}).get("visual_stellar_class")
        if not isinstance(visual_class, dict):
            raise AssertionError(f"rendered star missing visual_stellar_class field: {star}")
        if visual_class.get("status") not in {"derived", "assumed", "missing"}:
            raise AssertionError(f"unexpected visual_stellar_class status: {visual_class}")
        if visual_class.get("status") != "missing" and visual_class.get("layer") != "render_scene":
            raise AssertionError(f"visual_stellar_class must remain render_scene presentation data: {visual_class}")
    render_assumptions = scene_json.get("render_scene", {}).get("assumptions") or []
    if scene_json.get("render_scene", {}).get("assumption_count") != len(render_assumptions):
        raise AssertionError("simulation scene render assumption_count mismatch")
    if scene_json.get("render_scene", {}).get("persisted_assumption_count") is None:
        raise AssertionError("simulation scene render persisted_assumption_count missing")
    for assumption in render_assumptions:
        require_keys(
            assumption,
            ["assumption_key", "parameter_key", "input_context_json", "persistence_status"],
            "simulation scene.render_scene.assumption",
        )
        if assumption.get("persistence_status") not in {"transient", "persisted"}:
            raise AssertionError(
                f"unexpected simulation assumption persistence_status: {assumption.get('persistence_status')!r}"
            )
    require_keys(
        scene_json["arm"],
        ["components", "hierarchy_edges", "orbit_edges", "orbital_solutions"],
        "simulation scene.arm",
    )
    require_keys(
        scene_json["simulation_readiness"],
        ["score", "counts", "required_field_count", "status", "stars", "planets"],
        "simulation scene.simulation_readiness",
    )

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

    _, planet_count_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "planet_count", "limit": 20},
        label="search sort planet_count",
    )
    planet_counts = [
        float(item.get("planet_count") or 0)
        for item in planet_count_page["items"]
    ]
    if len(planet_counts) >= 2:
        assert_non_increasing(planet_counts, "planet_count sort")
    if planet_count_page.get("has_more") and planet_count_page.get("next_cursor"):
        _, planet_count_page2 = get_json(
            base_url,
            "/systems/search",
            params={"sort": "planet_count", "limit": 20, "cursor": planet_count_page["next_cursor"]},
            label="search sort planet_count page2",
        )
        overlap = {
            item.get("system_id")
            for item in planet_count_page["items"]
            if item.get("system_id") is not None
        } & {
            item.get("system_id")
            for item in planet_count_page2["items"]
            if item.get("system_id") is not None
        }
        if overlap:
            raise AssertionError(f"Planet-count cursor pagination returned overlapping system_ids: {sorted(overlap)}")

    _, star_count_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "star_count", "limit": 20},
        label="search sort star_count",
    )
    star_counts = [
        float(item.get("star_count") or 0)
        for item in star_count_page["items"]
    ]
    if len(star_counts) >= 2:
        assert_non_increasing(star_counts, "star_count sort")

    _, hottest_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "hottest", "limit": 20},
        label="search sort hottest",
    )
    hottest_values = [
        float(item["max_star_teff_k"])
        for item in hottest_page["items"]
        if item.get("max_star_teff_k") is not None
    ]
    if len(hottest_values) >= 2:
        assert_non_increasing(hottest_values, "hottest sort")
    if hottest_page.get("has_more") and hottest_page.get("next_cursor"):
        _, hottest_page2 = get_json(
            base_url,
            "/systems/search",
            params={"sort": "hottest", "limit": 20, "cursor": hottest_page["next_cursor"]},
            label="search sort hottest page2",
        )
        overlap = {
            item.get("system_id")
            for item in hottest_page["items"]
            if item.get("system_id") is not None
        } & {
            item.get("system_id")
            for item in hottest_page2["items"]
            if item.get("system_id") is not None
        }
        if overlap:
            raise AssertionError(f"Hottest cursor pagination returned overlapping system_ids: {sorted(overlap)}")

    _, coolest_page = get_json(
        base_url,
        "/systems/search",
        params={"sort": "coolest", "limit": 20},
        label="search sort coolest",
    )
    coolest_values = [
        float(item["min_star_teff_k"])
        for item in coolest_page["items"]
        if item.get("min_star_teff_k") is not None
    ]
    if len(coolest_values) >= 2:
        assert_non_decreasing(coolest_values, "coolest sort")

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
        if gaia_text:
            gaia_system_id = first_gaia.get("system_id")
            _, gaia_detail = get_json(
                base_url,
                f"/systems/{gaia_system_id}",
                label="detail raw gaia text",
            )
            detail_gaia_text = (gaia_detail.get("system") or {}).get("gaia_id_text")
            if detail_gaia_text != str(gaia_text):
                raise AssertionError(
                    f"system detail gaia_id_text should preserve exact identifier {gaia_text!r}, got {detail_gaia_text!r}"
                )
            for star in gaia_detail.get("stars") or []:
                star_gaia_text = star.get("gaia_id_text")
                if star_gaia_text is not None and not str(star_gaia_text).isdigit():
                    raise AssertionError(f"detail star gaia_id_text should be digit string, got {star_gaia_text!r}")
            _, gaia_scene = get_json(
                base_url,
                f"/systems/{gaia_system_id}/simulation-scene",
                label="simulation-scene raw gaia text",
            )
            scene_gaia_text = (gaia_scene.get("system") or {}).get("gaia_id_text")
            if scene_gaia_text != str(gaia_text):
                raise AssertionError(
                    f"simulation-scene gaia_id_text should preserve exact identifier {gaia_text!r}, got {scene_gaia_text!r}"
                )

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

    _, castor_search = get_json(
        base_url,
        "/systems/search",
        params={"q": "WDS 07346+3153", "limit": 5},
        label="search Castor WDS",
    )
    castor_item = next(
        (item for item in castor_search.get("items", []) if item.get("wds_id") == "07346+3153"),
        None,
    )
    if castor_item is None:
        raise AssertionError("Castor WDS lookup returned no 07346+3153 result")

    _, fifty_five_cnc_search = get_json(
        base_url,
        "/systems/search",
        params={"q": "55 Cnc", "limit": 5, "sort": "match"},
        label="search exact 55 Cnc",
    )
    fifty_five_cnc_first = (fifty_five_cnc_search.get("items") or [None])[0]
    if not isinstance(fifty_five_cnc_first, dict):
        raise AssertionError("55 Cnc search returned no items")
    fifty_five_cnc_name = str(
        fifty_five_cnc_first.get("display_name")
        or fifty_five_cnc_first.get("system_name")
        or ""
    ).strip().lower()
    if fifty_five_cnc_name != "55 cnc":
        raise AssertionError(
            f"55 Cnc exact/common-name search should return 55 Cnc first, got {fifty_five_cnc_name!r}"
        )

    _, castor_detail = get_json(
        base_url,
        f"/systems/{castor_item['system_id']}",
        label="Castor detail",
    )
    castor_root = (castor_detail.get("hierarchy") or {}).get("root")
    if not isinstance(castor_root, dict):
        raise AssertionError("Castor detail missing hierarchy root")
    castor_leaf_facts = {
        str(node.get("stable_component_key")): node.get("quick_facts")
        for node in iter_hierarchy_nodes(castor_root)
        if str(node.get("node_kind") or "") in {"inferred_star_leaf", "source_star_leaf"}
    }
    required_castor_leaf_facts = {
        "canon:leaf:msc:07346+3153:aa": {"spectral_type_raw": "A1V", "mass_msun": 2.37, "vmag": 1.98},
        "canon:leaf:msc:07346+3153:ba": {"spectral_type_raw": "A2Vm", "mass_msun": 1.79, "vmag": 2.88},
        "canon:leaf:msc:07346+3153:ca": {"spectral_type_raw": "M0.5V", "mass_msun": 0.6, "vmag": 9.77},
        "canon:leaf:msc:07346+3153:ab": {"spectral_type_raw": "dM1e", "mass_msun": 0.39},
        "canon:leaf:msc:07346+3153:bb": {"spectral_type_raw": "dM1e", "mass_msun": 0.39},
        "canon:leaf:msc:07346+3153:cb": {"spectral_type_raw": "M1_Ve", "mass_msun": 0.6, "vmag": 9.77},
    }
    for leaf_key, expected_facts in required_castor_leaf_facts.items():
        facts = castor_leaf_facts.get(leaf_key)
        if not isinstance(facts, dict):
            raise AssertionError(f"Castor leaf {leaf_key} missing quick_facts")
        for fact_key, expected_value in expected_facts.items():
            value = facts.get(fact_key)
            if isinstance(expected_value, float):
                if value is None or abs(float(value) - expected_value) > 1e-6:
                    raise AssertionError(
                        f"Castor leaf {leaf_key} expected {fact_key}={expected_value}, got {value!r}"
                    )
            elif value != expected_value:
                raise AssertionError(
                    f"Castor leaf {leaf_key} expected {fact_key}={expected_value!r}, got {value!r}"
                )
        if facts.get("vmag") == 0:
            raise AssertionError(f"Castor leaf {leaf_key} has placeholder Vmag 0.0")
        if "spectral_type_raw" not in expected_facts and facts.get("mass_msun") is not None:
            if not facts.get("visual_stellar_class"):
                raise AssertionError(f"Castor leaf {leaf_key} missing mass-based visual class prior")
            if facts.get("visual_stellar_class_status") != "assumed":
                raise AssertionError(f"Castor leaf {leaf_key} visual class prior should be assumed: {facts}")
            if facts.get("visual_stellar_class_basis") != "mass_main_sequence_prior_v1":
                raise AssertionError(f"Castor leaf {leaf_key} visual class prior basis mismatch: {facts}")

    common_name_cases = [
        ("Castor", "07346+3153", None, None, "Castor"),
        ("Alpha Geminorum", "07346+3153", None, None, None),
        ("Toliman", "14396-6050", None, None, "Toliman"),
        ("Alpha Centauri", "14396-6050", None, None, "Alpha Centauri"),
        ("Sirius", None, 10.0, None, "Sirius"),
        ("Jabbah", "16120-1928", None, None, "Jabbah"),
        ("Copernicus", None, None, 5, "Copernicus"),
    ]
    for query, expected_wds_id, max_dist_ly, min_planet_count, expected_display_name in common_name_cases:
        _, payload = get_json(
            base_url,
            "/systems/search",
            params={"q": query, "limit": 5},
            label=f"search common name {query}",
        )
        items = payload.get("items") or []
        if not items:
            raise AssertionError(f"common-name search {query!r} returned no items")
        first_item = items[0]
        if expected_wds_id and first_item.get("wds_id") != expected_wds_id:
            raise AssertionError(
                f"common-name search {query!r} expected WDS {expected_wds_id}, got {first_item.get('wds_id')!r}"
            )
        if max_dist_ly is not None:
            dist_ly = first_item.get("dist_ly")
            if dist_ly is None or float(dist_ly) > max_dist_ly:
                raise AssertionError(
                    f"common-name search {query!r} expected distance <= {max_dist_ly}, got {dist_ly!r}"
                )
        if min_planet_count is not None and int(first_item.get("planet_count") or 0) < min_planet_count:
            raise AssertionError(
                f"common-name search {query!r} expected >= {min_planet_count} planets, "
                f"got {first_item.get('planet_count')!r}"
            )
        if expected_display_name and str(first_item.get("display_name") or "").lower() != expected_display_name.lower():
            aliases = [str(value).lower() for value in first_item.get("display_aliases") or []]
            if expected_display_name.lower() not in aliases:
                raise AssertionError(
                    f"common-name search {query!r} expected display name or alias {expected_display_name!r}, "
                    f"got display={first_item.get('display_name')!r}, aliases={first_item.get('display_aliases')!r}"
                )

    identifier_cases = [
        ("HD 128620", "14396-6050", {"Alpha Centauri", "Alpha Cen", "Toliman"}),
        ("HIP 71683", "14396-6050", {"Alpha Centauri", "Alpha Cen", "Toliman"}),
    ]
    for query, expected_wds_id, expected_context_names in identifier_cases:
        _, payload = get_json(
            base_url,
            "/systems/search",
            params={"q": query, "limit": 5},
            label=f"search catalog identifier {query}",
        )
        items = payload.get("items") or []
        if not items:
            raise AssertionError(f"catalog identifier search {query!r} returned no items")
        first_item = items[0]
        if first_item.get("wds_id") != expected_wds_id:
            raise AssertionError(
                f"catalog identifier search {query!r} expected WDS {expected_wds_id}, got {first_item.get('wds_id')!r}"
            )
        aliases = {str(value).lower() for value in first_item.get("display_aliases") or []}
        display_name = str(first_item.get("display_name") or "").lower()
        expected_names = {str(value).lower() for value in expected_context_names}
        if not ({display_name} | aliases) & expected_names:
            raise AssertionError(
                f"catalog identifier search {query!r} expected accepted-system context {sorted(expected_context_names)!r}, "
                f"got display={first_item.get('display_name')!r}, aliases={first_item.get('display_aliases')!r}"
            )

    for query in ("HD 172167", "HIP 91262"):
        _, payload = get_json(
            base_url,
            "/systems/search",
            params={"q": query, "limit": 5},
            label=f"search absent catalog identifier {query}",
        )
        if payload.get("items"):
            raise AssertionError(
                f"absent catalog identifier search {query!r} should not return fuzzy substitutes: {payload.get('items')!r}"
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
