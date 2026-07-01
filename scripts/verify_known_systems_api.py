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
        min_scene_stars=6,
    ),
    BenchmarkCase(
        "Nu Sco",
        expected_wds_id="16120-1928",
        min_star_count=7,
        expected_aliases=("Jabbah", "Nu Scorpii"),
        required_hierarchy_names=(
            "14nu Sco AA",
            "14nu Sco AB",
            "14nu Sco AC",
            "14nu Sco B",
            "14nu Sco C",
            "14nu Sco DA",
            "14nu Sco DB",
        ),
        required_hierarchy_facts=(
            HierarchyFact("14nu Sco AA", {"spectral_type_raw": "B3V", "spectral_class": "B", "mass_msun": 6.07}),
            HierarchyFact("14nu Sco AB", {"spectral_type_raw": None, "spectral_class": None, "mass_msun": 2.28}),
        ),
        min_scene_stars=7,
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
        expected_wds_id="06451-1643",
        max_dist_ly=10.0,
        min_star_count=2,
        expected_aliases=("Alpha Canis Majoris",),
        min_scene_stars=2,
    ),
    BenchmarkCase(
        "Proxima Centauri",
        max_dist_ly=5.0,
        min_star_count=1,
        min_planet_count=2,
        expected_aliases=("Proxima Centauri",),
        min_scene_stars=1,
        min_scene_planets=2,
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
        "GJ 1061",
        max_dist_ly=13.0,
        min_star_count=1,
        min_planet_count=3,
        min_scene_stars=1,
        min_scene_planets=3,
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


def field_by_key(fields: Any, key: str) -> dict[str, Any] | None:
    if isinstance(fields, dict):
        field = fields.get(key)
        return field if isinstance(field, dict) else None
    if isinstance(fields, list):
        for field in fields:
            if isinstance(field, dict) and field.get("key") == key:
                return field
    return None


def rendered_planet_by_name(planets: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    wanted = normalize(name)
    for planet in planets:
        if normalize(planet.get("display_name")) == wanted:
            return planet
    return None


def numeric_field(body: dict[str, Any], key: str) -> float | None:
    field = field_by_key(body.get("fields"), key)
    if not field or field.get("value") is None:
        return None
    return float(field["value"])


def assert_field_range(case_query: str, body: dict[str, Any], key: str, low: float, high: float) -> None:
    value = numeric_field(body, key)
    if value is None or not (low <= value <= high):
        raise AssertionError(
            f"{case_query}: {body.get('display_name')} expected rendered {key} in {low}..{high}, got {value!r}"
        )


def assert_render_scene_contract(
    case: BenchmarkCase,
    scene: dict[str, Any],
    *,
    warnings: list[str] | None = None,
    allow_stale_public_slice: bool = False,
) -> bool:
    warnings = warnings if warnings is not None else []
    stale_scene_gap = False
    render_scene = scene.get("render_scene") or {}
    if render_scene.get("schema_version") != "render_scene_v0.2":
        raise AssertionError(f"{case.query}: unexpected render_scene schema {render_scene.get('schema_version')!r}")
    bodies = render_scene.get("bodies") or {}
    scene_stars = bodies.get("stars") or []
    scene_planets = bodies.get("planets") or []
    scene_subsystems = bodies.get("subsystems") or []
    render_orbits = render_scene.get("orbits") or []
    assumptions = render_scene.get("assumptions") or []
    diagnostics = render_scene.get("diagnostics") or {}
    visual_scale = render_scene.get("visual_scale") or {}
    if visual_scale.get("schema_version") != "visual_scale_beta_v1":
        raise AssertionError(f"{case.query}: missing visual_scale_beta_v1 policy")
    for scale_key in ("star_radius", "planet_radius", "planet_orbit_radius", "binary_orbit_radius"):
        if not isinstance(visual_scale.get(scale_key), dict):
            raise AssertionError(f"{case.query}: visual scale policy missing {scale_key}")
    modes = {str(item.get("mode") or item.get("value") or "") for item in (visual_scale.get("available_scale_modes") or []) if isinstance(item, dict)}
    for mode in ("structure", "true_orbits", "true_bodies", "log"):
        if mode not in modes:
            raise AssertionError(f"{case.query}: visual scale policy missing {mode} mode")
    if visual_scale.get("default_scale_mode") != "structure":
        raise AssertionError(f"{case.query}: visual scale default mode should be structure")
    if not isinstance(visual_scale.get("collision_policy"), dict):
        raise AssertionError(f"{case.query}: visual scale policy missing collision_policy")
    if render_scene.get("assumption_count") != len(assumptions):
        raise AssertionError(
            f"{case.query}: render_scene.assumption_count does not match assumptions list length"
        )
    body_counts = diagnostics.get("body_counts") or {}
    if body_counts.get("stars") != len(scene_stars) or body_counts.get("planets") != len(scene_planets) or body_counts.get("subsystems") != len(scene_subsystems):
        raise AssertionError(f"{case.query}: render_scene diagnostic body counts do not match rendered bodies")
    subsystem_handle_counts = diagnostics.get("subsystem_handle_counts") or {}
    expected_fallback_subsystems = sum(1 for subsystem in scene_subsystems if subsystem.get("fallback_subsystem"))
    expected_source_subsystems = len(scene_subsystems) - expected_fallback_subsystems
    if subsystem_handle_counts:
        if (
            subsystem_handle_counts.get("simulation_tree_fallback") != expected_fallback_subsystems
            or subsystem_handle_counts.get("source_native") != expected_source_subsystems
        ):
            raise AssertionError(
                f"{case.query}: subsystem handle diagnostics mismatch: "
                f"{subsystem_handle_counts} != source_native={expected_source_subsystems}, "
                f"simulation_tree_fallback={expected_fallback_subsystems}"
            )
    orbit_counts = diagnostics.get("orbit_counts") or {}
    if orbit_counts.get("total") != len(render_orbits):
        raise AssertionError(f"{case.query}: render_scene diagnostic orbit total does not match rendered orbits")
    expected_endpoint_counts: dict[str, int] = {}
    expected_relation_counts: dict[str, int] = {}
    for orbit in render_orbits:
        endpoint_kind = str(orbit.get("endpoint_kind") or "unknown")
        expected_endpoint_counts[endpoint_kind] = expected_endpoint_counts.get(endpoint_kind, 0) + 1
        relation_kind = str(orbit.get("relation_kind") or "unknown")
        expected_relation_counts[relation_kind] = expected_relation_counts.get(relation_kind, 0) + 1
    if (orbit_counts.get("by_endpoint_kind") or {}) != expected_endpoint_counts:
        raise AssertionError(
            f"{case.query}: render_scene diagnostic endpoint counts mismatch: "
            f"{orbit_counts.get('by_endpoint_kind')} != {expected_endpoint_counts}"
        )
    if (orbit_counts.get("by_relation_kind") or {}) != expected_relation_counts:
        raise AssertionError(
            f"{case.query}: render_scene diagnostic relation counts mismatch: "
            f"{orbit_counts.get('by_relation_kind')} != {expected_relation_counts}"
        )
    field_status_counts: dict[str, int] = {"source": 0, "derived": 0, "assumed": 0, "missing": 0}
    for owner in [*scene_stars, *scene_planets, *scene_subsystems, *render_orbits]:
        for field in (owner.get("fields") or {}).values():
            if isinstance(field, dict):
                status = str(field.get("status") or "missing").lower()
                field_status_counts[status] = field_status_counts.get(status, 0) + 1
    if (diagnostics.get("field_status_counts") or {}) != field_status_counts:
        raise AssertionError(
            f"{case.query}: render_scene diagnostic field status counts mismatch: "
            f"{diagnostics.get('field_status_counts')} != {field_status_counts}"
        )
    persistence_counts = diagnostics.get("assumption_persistence_counts") or {}
    persisted_count = sum(1 for assumption in assumptions if assumption.get("persistence_status") == "persisted")
    transient_count = sum(1 for assumption in assumptions if assumption.get("persistence_status") == "transient")
    if persistence_counts.get("persisted") != persisted_count or persistence_counts.get("transient") != transient_count:
        raise AssertionError(f"{case.query}: render_scene assumption persistence diagnostics mismatch")
    for assumption in assumptions:
        required_keys = {
            "assumption_key",
            "object_type",
            "parameter_key",
            "assumption_version",
            "visibility_label",
            "input_context_json",
            "persistence_status",
            "field",
        }
        missing = sorted(key for key in required_keys if key not in assumption)
        if missing:
            raise AssertionError(f"{case.query}: render assumption missing keys {missing}: {assumption}")
        if assumption.get("visibility_label") != "assumed":
            raise AssertionError(f"{case.query}: unexpected assumption visibility {assumption.get('visibility_label')!r}")
        if assumption.get("persistence_status") not in {"transient", "persisted"}:
            raise AssertionError(
                f"{case.query}: unexpected assumption persistence status {assumption.get('persistence_status')!r}"
            )
        field = assumption.get("field") or {}
        if field.get("status") != "assumed":
            raise AssertionError(f"{case.query}: assumption record field is not marked assumed: {field}")
    if case.min_scene_stars is not None and len(scene_stars) < case.min_scene_stars:
        message = f"{case.query}: expected at least {case.min_scene_stars} preview stars, got {len(scene_stars)}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
            stale_scene_gap = True
        else:
            raise AssertionError(message)
    if case.min_scene_planets is not None and len(scene_planets) < case.min_scene_planets:
        message = f"{case.query}: expected at least {case.min_scene_planets} preview planets, got {len(scene_planets)}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
            stale_scene_gap = True
        else:
            raise AssertionError(message)
    for star in scene_stars:
        visual_class = field_by_key(star.get("fields"), "visual_stellar_class")
        if not visual_class:
            raise AssertionError(f"{case.query}: rendered star missing visual_stellar_class field: {star}")
        if visual_class.get("status") not in {"derived", "assumed", "missing"}:
            raise AssertionError(f"{case.query}: malformed visual stellar class status: {visual_class}")
        if visual_class.get("status") != "missing" and visual_class.get("layer") != "render_scene":
            raise AssertionError(f"{case.query}: visual stellar class should stay in render_scene: {visual_class}")
        if visual_class.get("basis") == "mass_main_sequence_prior_v1" and visual_class.get("status") != "assumed":
            raise AssertionError(f"{case.query}: mass visual class prior must be assumed: {visual_class}")
        spectral_type = field_by_key(star.get("fields"), "spectral_type_raw")
        if visual_class.get("basis") == "mass_main_sequence_prior_v1" and spectral_type and spectral_type.get("value"):
            raise AssertionError(f"{case.query}: mass visual class prior used despite spectral type evidence: {star}")
    for planet in scene_planets:
        visual_class = field_by_key(planet.get("fields"), "planet_visual_class")
        if not visual_class:
            raise AssertionError(f"{case.query}: rendered planet missing planet_visual_class field: {planet}")
        if visual_class.get("status") not in {"derived", "assumed"} or visual_class.get("layer") != "render_scene":
            raise AssertionError(f"{case.query}: malformed planet visual class provenance: {visual_class}")
        inclination = field_by_key(planet.get("fields"), "inclination_deg")
        if not inclination or inclination.get("value") is None or inclination.get("status") in {None, "missing"}:
            raise AssertionError(f"{case.query}: rendered planet missing renderable inclination field: {planet}")
        if inclination.get("status") == "assumed" and inclination.get("layer") != "disc_assumption":
            raise AssertionError(f"{case.query}: assumed planet inclination should stay in disc_assumption: {inclination}")

    query_norm = normalize(case.query)
    if stale_scene_gap:
        return True
    if query_norm == "trappist-1":
        period_fields = [field_by_key(planet.get("fields"), "orbital_period_days") for planet in scene_planets]
        period_values = [float(field["value"]) for field in period_fields if field and field.get("value") is not None]
        if len(period_values) < 7:
            raise AssertionError(f"{case.query}: expected seven rendered planet periods, got {len(period_values)}")
        if period_values != sorted(period_values):
            raise AssertionError(f"{case.query}: rendered planet periods should be sorted in orbital order: {period_values}")
        bad_statuses = [field.get("status") for field in period_fields if field and field.get("status") != "source"]
        if bad_statuses:
            raise AssertionError(f"{case.query}: rendered planet periods should be source-backed, got {bad_statuses}")
        phase_assumptions = [
            assumption
            for assumption in assumptions
            if assumption.get("object_type") == "planet" and assumption.get("parameter_key") == "phase_rad"
        ]
        if len(phase_assumptions) < 7:
            raise AssertionError(
                f"{case.query}: expected explicit phase assumptions for seven rendered planets, got {len(phase_assumptions)}"
            )

    if query_norm in {"55 cnc", "sol"}:
        source_periods = [
            field
            for planet in scene_planets
            if (field := field_by_key(planet.get("fields"), "orbital_period_days"))
            and field.get("value") is not None
            and field.get("status") == "source"
        ]
        expected = case.min_scene_planets or 1
        if len(source_periods) < expected:
            raise AssertionError(f"{case.query}: expected at least {expected} source-backed rendered planet periods, got {len(source_periods)}")

    if query_norm == "gj 1061":
        planet_b = rendered_planet_by_name(scene_planets, "GJ 1061 b")
        planet_c = rendered_planet_by_name(scene_planets, "GJ 1061 c")
        planet_d = rendered_planet_by_name(scene_planets, "GJ 1061 d")
        for planet in (planet_b, planet_c, planet_d):
            if not planet:
                raise AssertionError(f"{case.query}: expected rendered GJ 1061 b/c/d planets")
        b_inclination = field_by_key(planet_b.get("fields"), "inclination_deg")
        if not b_inclination or b_inclination.get("status") != "source":
            raise AssertionError(f"{case.query}: GJ 1061 b should retain source inclination: {b_inclination}")
        for planet in (planet_c, planet_d):
            inclination = field_by_key(planet.get("fields"), "inclination_deg")
            if not inclination or inclination.get("status") != "assumed":
                raise AssertionError(f"{case.query}: missing GJ 1061 c/d inclination should remain assumed: {inclination}")
            if "coplanar_with_source_planet_inclination_visual_prior" not in str(inclination.get("basis") or ""):
                raise AssertionError(f"{case.query}: missing GJ 1061 c/d inclination should use coplanar prior: {inclination}")
            value = float(inclination.get("value"))
            source_value = float(b_inclination.get("value"))
            if abs(value - source_value) > 2.0:
                raise AssertionError(
                    f"{case.query}: GJ 1061 c/d assumed inclination should stay near source plane, got {value} vs {source_value}"
                )

    if query_norm == "sol":
        try:
            mercury = rendered_planet_by_name(scene_planets, "Mercury")
            ceres = rendered_planet_by_name(scene_planets, "Ceres")
            if not mercury or not ceres:
                names = [planet.get("display_name") for planet in scene_planets]
                raise AssertionError(f"{case.query}: expected Mercury and Ceres in rendered planets, got {names}")
            assert_field_range(case.query, mercury, "semi_major_axis_au", 0.36, 0.42)
            assert_field_range(case.query, mercury, "orbital_period_days", 80.0, 95.0)
            assert_field_range(case.query, ceres, "semi_major_axis_au", 2.5, 3.1)
            assert_field_range(case.query, ceres, "orbital_period_days", 1500.0, 1900.0)
            sma_values = [
                numeric_field(planet, "semi_major_axis_au")
                for planet in scene_planets
                if numeric_field(planet, "semi_major_axis_au") is not None
            ]
            if sma_values != sorted(sma_values):
                names = [planet.get("display_name") for planet in scene_planets]
                raise AssertionError(f"{case.query}: rendered planets should be sorted by semi-major axis, got {names}")
            first_names = [normalize(planet.get("display_name")) for planet in scene_planets[:4]]
            if first_names[:4] != ["mercury", "venus", "earth", "mars"]:
                raise AssertionError(f"{case.query}: expected inner planets first, got {first_names}")
            if abs(numeric_field(mercury, "semi_major_axis_au") - numeric_field(ceres, "semi_major_axis_au")) < 1e-6:
                raise AssertionError(f"{case.query}: rendered Ceres duplicates Mercury semi-major axis")
            missing_hosts = [
                planet.get("display_name")
                for planet in scene_planets
                if normalize(planet.get("display_name")) in {"mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune"}
                and not planet.get("host_body_key")
            ]
            if missing_hosts:
                raise AssertionError(f"{case.query}: major rendered planets missing host_body_key: {missing_hosts}")
        except AssertionError as exc:
            if allow_stale_public_slice:
                warnings.append(f"[stale-public-slice] {exc}")
                return True
            raise

    if query_norm == "proxima centauri":
        star_names = [normalize(star.get("display_name")) for star in scene_stars]
        if not any("proxima" in name for name in star_names):
            raise AssertionError(f"{case.query}: expected rendered star display name to include Proxima, got {star_names}")
        source_periods = [
            field
            for planet in scene_planets
            if (field := field_by_key(planet.get("fields"), "orbital_period_days"))
            and field.get("value") is not None
            and field.get("status") == "source"
        ]
        if len(source_periods) < 2:
            raise AssertionError(f"{case.query}: expected two source-backed rendered planet periods, got {len(source_periods)}")

    if query_norm == "sirius":
        star_names = {normalize(star.get("display_name")) for star in scene_stars}
        if not {"sirius a", "sirius b"}.issubset(star_names):
            raise AssertionError(f"{case.query}: expected Sirius A/B rendered stars, got {sorted(star_names)}")
        spectral_classes = {str(star.get("spectral_class") or "").upper() for star in scene_stars}
        if not {"A", "D"}.issubset(spectral_classes):
            raise AssertionError(f"{case.query}: expected A primary and D compact companion classes, got {sorted(spectral_classes)}")
        sirius_b = next((star for star in scene_stars if normalize(star.get("display_name")) == "sirius b"), None)
        if not sirius_b:
            raise AssertionError(f"{case.query}: expected Sirius B render body")
        if sirius_b.get("body_class") != "white_dwarf" or sirius_b.get("compact_type") != "white_dwarf":
            raise AssertionError(f"{case.query}: Sirius B should render as white_dwarf, got {sirius_b}")
        object_type_field = field_by_key(sirius_b.get("fields"), "object_type")
        if not object_type_field or object_type_field.get("value") != "white_dwarf" or object_type_field.get("status") != "source":
            raise AssertionError(f"{case.query}: Sirius B object_type field should be source white_dwarf, got {object_type_field}")
        fallback_orbits = [
            orbit
            for orbit in render_orbits
            if orbit.get("relation_kind") == "visual_binary_fallback"
            and (orbit.get("source") or {}).get("layer") == "disc_assumption"
        ]
        if len(fallback_orbits) != 1:
            raise AssertionError(
                f"{case.query}: expected one disc-assumption visual binary fallback orbit, got {len(fallback_orbits)}"
            )
        fallback = fallback_orbits[0]
        for field_key in ("period_days", "semi_major_axis_au", "eccentricity", "inclination_deg", "phase_rad"):
            field = field_by_key(fallback.get("fields"), field_key)
            if not field or field.get("status") != "assumed" or field.get("layer") != "disc_assumption":
                raise AssertionError(f"{case.query}: fallback orbit field {field_key} is not a disc assumption: {field}")

    if query_norm == "castor":
        orbit_count = len(render_orbits)
        if orbit_count < 5:
            raise AssertionError(f"{case.query}: expected at least five rendered stellar orbit entries, got {orbit_count}")
        star_pair_count = sum(1 for orbit in render_orbits if orbit.get("endpoint_kind") == "star_pair")
        group_pair_count = sum(1 for orbit in render_orbits if orbit.get("endpoint_kind") == "group_pair")
        if star_pair_count < 3 or group_pair_count < 2:
            raise AssertionError(
                f"{case.query}: expected at least three direct binary and two hierarchical group-pair orbits, "
                f"got star_pair={star_pair_count}, group_pair={group_pair_count}"
            )
        subsystem_names = {normalize(subsystem.get("display_name")) for subsystem in scene_subsystems}
        for expected_name in ("castor ab", "castor a", "castor b", "castor c"):
            if expected_name not in subsystem_names:
                raise AssertionError(
                    f"{case.query}: expected rendered subsystem {expected_name!r}, got {sorted(subsystem_names)}"
                )
        for subsystem in scene_subsystems:
            child_keys = subsystem.get("child_body_keys") or []
            field = field_by_key(subsystem.get("fields"), "rendered_child_star_count")
            component_field = field_by_key(subsystem.get("fields"), "component_label")
            basis_field = field_by_key(subsystem.get("fields"), "hierarchy_basis")
            if not child_keys or not field or field.get("status") != "derived":
                raise AssertionError(f"{case.query}: malformed rendered subsystem body: {subsystem}")
            if not component_field or component_field.get("status") not in {"source", "derived"}:
                raise AssertionError(f"{case.query}: subsystem missing component label provenance: {subsystem}")
            expected_layer = "render_scene" if subsystem.get("fallback_subsystem") else "arm"
            if not basis_field or basis_field.get("status") != "derived" or basis_field.get("layer") != expected_layer:
                raise AssertionError(f"{case.query}: subsystem missing hierarchy-basis provenance: {subsystem}")
            if subsystem.get("fallback_subsystem") and subsystem.get("source", {}).get("basis") != "simulation_tree_fallback_subsystem":
                raise AssertionError(f"{case.query}: fallback subsystem missing explicit source basis: {subsystem}")
    return False


def verify_case(
    base_url: str,
    case: BenchmarkCase,
    warnings: list[str],
    *,
    allow_stale_public_slice: bool = False,
) -> str:
    search = get_json(base_url, "/systems/search", params={"q": case.query, "limit": 5})
    items = search.get("items") or []
    if not items:
        raise AssertionError(f"{case.query}: search returned no results")
    item = items[0]
    system_id = item.get("system_id")
    if system_id is None:
        raise AssertionError(f"{case.query}: first result missing system_id")

    if case.expected_wds_id and item.get("wds_id") != case.expected_wds_id:
        message = f"{case.query}: expected WDS {case.expected_wds_id}, got {item.get('wds_id')!r}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
        else:
            raise AssertionError(message)
    if case.max_dist_ly is not None:
        dist_ly = item.get("dist_ly")
        if dist_ly is None or float(dist_ly) > case.max_dist_ly:
            raise AssertionError(f"{case.query}: expected distance <= {case.max_dist_ly}, got {dist_ly!r}")
    if case.min_star_count is not None and int(item.get("star_count") or 0) < case.min_star_count:
        message = f"{case.query}: expected at least {case.min_star_count} stars, got {item.get('star_count')!r}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
        else:
            raise AssertionError(message)
    if case.min_planet_count is not None and int(item.get("planet_count") or 0) < case.min_planet_count:
        message = f"{case.query}: expected at least {case.min_planet_count} planets, got {item.get('planet_count')!r}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
        else:
            raise AssertionError(message)

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
            message = f"{case.query}: missing expected alias {expected_alias!r}"
            if allow_stale_public_slice:
                warnings.append(f"[stale-public-slice] {message}")
                continue
            raise AssertionError(message)

    hierarchy = detail.get("hierarchy") or {}
    root = hierarchy.get("root")
    if not isinstance(root, dict):
        raise AssertionError(f"{case.query}: missing hierarchy root")
    counts = hierarchy.get("counts") or {}
    if case.min_star_count is not None and int(counts.get("stars") or 0) < case.min_star_count:
        message = f"{case.query}: hierarchy expected at least {case.min_star_count} stars, got {counts.get('stars')!r}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
        else:
            raise AssertionError(message)
    hierarchy_nodes = list(iter_hierarchy_nodes(root))
    nodes_by_name = {str(node.get("display_name") or ""): node for node in hierarchy_nodes}
    missing_required_names: list[str] = []
    for display_name in case.required_hierarchy_names:
        if display_name not in nodes_by_name:
            missing_required_names.append(display_name)
    if missing_required_names:
        message = f"{case.query}: hierarchy missing nodes {missing_required_names!r}"
        if allow_stale_public_slice:
            warnings.append(f"[stale-public-slice] {message}")
        else:
            raise AssertionError(message)
    for expected in case.required_hierarchy_facts:
        node = nodes_by_name.get(expected.display_name)
        if not node:
            message = f"{case.query}: hierarchy missing fact node {expected.display_name!r}"
            if allow_stale_public_slice:
                warnings.append(f"[stale-public-slice] {message}")
                continue
            raise AssertionError(message)
        facts = node.get("quick_facts")
        if not isinstance(facts, dict):
            message = f"{case.query}: hierarchy node {expected.display_name!r} missing quick_facts"
            if allow_stale_public_slice:
                warnings.append(f"[stale-public-slice] {message}")
                continue
            raise AssertionError(message)
        for fact_key, expected_value in expected.facts.items():
            try:
                assert_fact_matches(f"{case.query} {expected.display_name}", fact_key, expected_value, facts.get(fact_key))
            except AssertionError as exc:
                if allow_stale_public_slice:
                    warnings.append(f"[stale-public-slice] {exc}")
                    continue
                raise
        if (
            expected.facts.get("mass_msun") is not None
            and expected.facts.get("spectral_type_raw") is None
            and expected.facts.get("spectral_class") is None
        ):
            if not facts.get("visual_stellar_class"):
                message = f"{case.query}: hierarchy node {expected.display_name!r} should expose a mass-based visual class prior"
                if allow_stale_public_slice:
                    warnings.append(f"[stale-public-slice] {message}")
                    continue
                raise AssertionError(message)
            if facts.get("visual_stellar_class_status") != "assumed" or facts.get("visual_stellar_class_basis") != "mass_main_sequence_prior_v1":
                message = f"{case.query}: hierarchy node {expected.display_name!r} has malformed visual class prior: {facts}"
                if allow_stale_public_slice:
                    warnings.append(f"[stale-public-slice] {message}")
                    continue
                raise AssertionError(message)

    for node in hierarchy_nodes:
        component_family = str(node.get("component_family") or "")
        component_type = str(node.get("component_type") or "")
        if component_family != "star" and component_type not in {"star", "stellar_component"}:
            facts = node.get("quick_facts") or {}
            if facts.get("spectral_type_raw") or facts.get("spectral_class"):
                raise AssertionError(
                    f"{case.query}: non-star hierarchy node {node.get('display_name')!r} carries spectral facts"
                )

    scene = get_json(base_url, f"/systems/{system_id}/simulation-scene")
    stale_scene_gap = assert_render_scene_contract(
        case,
        scene,
        warnings=warnings,
        allow_stale_public_slice=allow_stale_public_slice,
    )
    render_scene = scene.get("render_scene") or {}
    bodies = render_scene.get("bodies") or scene.get("bodies") or {}
    scene_stars = bodies.get("stars") or []
    scene_planets = bodies.get("planets") or []
    if case.min_scene_stars is not None and len(scene_stars) < case.min_scene_stars and not stale_scene_gap:
        raise AssertionError(f"{case.query}: expected at least {case.min_scene_stars} preview stars, got {len(scene_stars)}")
    if case.min_scene_planets is not None and len(scene_planets) < case.min_scene_planets and not stale_scene_gap:
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
    parser.add_argument(
        "--allow-stale-public-slice",
        action="store_true",
        help=(
            "Downgrade source-native hierarchy/fact benchmark gaps to stale public-slice warnings. "
            "Use only for public edge smoke checks after code-only deploys."
        ),
    )
    args = parser.parse_args()

    warnings: list[str] = []
    summaries = [
        verify_case(
            args.base_url.rstrip("/"),
            case,
            warnings,
            allow_stale_public_slice=args.allow_stale_public_slice,
        )
        for case in BENCHMARKS
    ]
    print("Known-system API benchmark passed:")
    for summary in summaries:
        print(f"- {summary}")
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning}")
        if args.strict_preview and not args.allow_stale_public_slice:
            raise SystemExit("Strict preview mode failed due to warnings above.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Known-system API benchmark failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
