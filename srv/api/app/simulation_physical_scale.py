"""Deterministic physical-orbit and simulator focus contracts."""

from __future__ import annotations

import copy
import math
from typing import Any, Dict, Iterable, Optional


PHYSICAL_SCALE_CONTRACT_VERSION = "simulation_physical_scale_v2"
PHYSICAL_EXTENT_POLICY_VERSION = "physical_orbit_extent_policy_v3"
FOCUS_GRAPH_VERSION = "simulation_focus_graph_v2"
KEPLER_AXIS_DERIVATION_VERSION = "kepler_axis_from_period_total_mass_v2"
ORBIT_COHERENCE_POLICY_VERSION = "stellar_orbit_kepler_coherence_v2"
SELECTED_STELLAR_MASS_POLICY_VERSION = "stellar_leaf_mass_selection_v1"
MIN_PLAUSIBLE_STELLAR_SYSTEM_MASS_MSUN = 0.001
MAX_PLAUSIBLE_STELLAR_SYSTEM_MASS_MSUN = 1000.0
MASS_CONSISTENCY_FACTOR = 10.0

FORMATION_LINE_TEMPERATURES_K = {
    "vaporization": 1500.0,
    "soot": 300.0,
    "water": 160.0,
    "carbon_dioxide": 70.0,
    "methane_co": 25.0,
    "nitrogen": 13.5,
}


def _positive_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) and numeric > 0 else None


def _nonnegative_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) and numeric >= 0 else None


def _field(fields: Any, key: str) -> Dict[str, Any]:
    if isinstance(fields, dict):
        value = fields.get(key)
        return value if isinstance(value, dict) else {}
    if isinstance(fields, list):
        for value in fields:
            if isinstance(value, dict) and value.get("key") == key:
                return value
    return {}


def _accepted_field(field: Dict[str, Any]) -> bool:
    return (
        _positive_float(field.get("value")) is not None
        and str(field.get("status") or "").lower() in {"source", "source_model", "derived"}
    )


def _accepted_orbital_period(field: Dict[str, Any]) -> bool:
    """Accept source periods, never periods estimated from projected separation."""
    return (
        _positive_float(field.get("value")) is not None
        and str(field.get("status") or "").lower() in {"source", "source_model"}
    )


def kepler_semi_major_axis_au(period_days: Any, total_mass_msun: Any) -> Optional[float]:
    """Return the two-body relative semi-major axis using AU, years, and solar masses."""
    period = _positive_float(period_days)
    total_mass = _positive_float(total_mass_msun)
    if period is None or total_mass is None:
        return None
    period_years = period / 365.25
    return (period_years * period_years * total_mass) ** (1.0 / 3.0)


def implied_total_mass_msun(period_days: Any, semi_major_axis_au: Any) -> Optional[float]:
    period = _positive_float(period_days)
    axis = _positive_float(semi_major_axis_au)
    if period is None or axis is None:
        return None
    period_years = period / 365.25
    return (axis ** 3) / (period_years ** 2)


def _axis_coherence(
    source_axis: Dict[str, Any],
    period: Dict[str, Any],
    total_mass_msun: Optional[float],
) -> Dict[str, Any]:
    axis = _positive_float(source_axis.get("value"))
    period_value = (
        _positive_float(period.get("value"))
        if _accepted_orbital_period(period)
        else None
    )
    if axis is None or period_value is None:
        return {
            "status": "not_testable",
            "policy_version": ORBIT_COHERENCE_POLICY_VERSION,
            "reason": "accepted period and axis are not both available",
        }
    implied_mass = implied_total_mass_msun(period_value, axis)
    expected_mass = _positive_float(total_mass_msun)
    ratio: Optional[float]
    if implied_mass is None:
        coherent = False
        ratio = None
        reason = "non-finite Kepler mass"
    elif expected_mass is not None:
        ratio = max(implied_mass / expected_mass, expected_mass / implied_mass)
        coherent = ratio <= MASS_CONSISTENCY_FACTOR
        reason = "consistent with applicable endpoint mass" if coherent else "inconsistent with applicable endpoint mass"
    else:
        ratio = None
        coherent = MIN_PLAUSIBLE_STELLAR_SYSTEM_MASS_MSUN <= implied_mass <= MAX_PLAUSIBLE_STELLAR_SYSTEM_MASS_MSUN
        reason = "physically plausible implied stellar-system mass" if coherent else "implausible implied stellar-system mass"
    return {
        "status": "pass" if coherent else "rejected",
        "policy_version": ORBIT_COHERENCE_POLICY_VERSION,
        "reason": reason,
        "implied_total_mass_msun": round(implied_mass, 9) if implied_mass is not None else None,
        "applicable_total_mass_msun": round(expected_mass, 9) if expected_mass is not None else None,
        "mass_consistency_factor": round(ratio, 6) if ratio is not None else None,
        "accepted_factor_limit": MASS_CONSISTENCY_FACTOR if expected_mass is not None else None,
        "plausible_mass_bounds_msun": [MIN_PLAUSIBLE_STELLAR_SYSTEM_MASS_MSUN, MAX_PLAUSIBLE_STELLAR_SYSTEM_MASS_MSUN],
    }


def _derived_field(
    *,
    key: str,
    label: str,
    value: Optional[float],
    unit: Optional[str],
    basis: str,
    inputs: Iterable[Dict[str, Any]] = (),
    confidence: Optional[float] = None,
    notes: Optional[str] = None,
    value_lower: Optional[float] = None,
    value_upper: Optional[float] = None,
    interval_semantics: Optional[str] = None,
) -> Dict[str, Any]:
    result = {
        "key": key,
        "label": label,
        "value": round(value, 9) if value is not None else None,
        "unit": unit,
        "status": "derived" if value is not None else "missing",
        "layer": "render_scene",
        "basis": basis,
        "generator_version": PHYSICAL_EXTENT_POLICY_VERSION,
        "confidence": confidence,
        "input_lineage": [
            {
                "key": item.get("key"),
                "status": item.get("status"),
                "layer": item.get("layer"),
                "basis": item.get("basis"),
                "source_catalog": item.get("source_catalog"),
                "source_reference": item.get("source_reference"),
            }
            for item in inputs
            if isinstance(item, dict)
        ],
        "notes": notes,
    }
    if value_lower is not None:
        result["value_lower"] = round(value_lower, 9)
    if value_upper is not None:
        result["value_upper"] = round(value_upper, 9)
    if interval_semantics:
        result["interval_semantics"] = interval_semantics
    return result


def _input_interval(field: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    value = _positive_float(field.get("value"))
    if value is None:
        return None, None
    lower = _positive_float(field.get("value_lower"))
    upper = _positive_float(field.get("value_upper"))
    if lower is None or upper is None:
        return None, None
    return min(lower, value), max(upper, value)


def _mass_interval(
    mass_inputs: Iterable[Dict[str, Any]],
) -> tuple[Optional[float], Optional[float]]:
    fields = [field for field in mass_inputs if isinstance(field, dict)]
    if not fields or any(not _accepted_field(field) for field in fields):
        return None, None
    intervals = [_input_interval(field) for field in fields]
    if any(lower is None or upper is None for lower, upper in intervals):
        return None, None
    return (
        sum(float(lower) for lower, _upper in intervals if lower is not None),
        sum(float(upper) for _lower, upper in intervals if upper is not None),
    )


def _missing_field(key: str, label: str, unit: Optional[str], basis: str) -> Dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "value": None,
        "unit": unit,
        "status": "missing",
        "layer": "none",
        "basis": basis,
        "generator_version": PHYSICAL_EXTENT_POLICY_VERSION,
    }


def attach_physical_orbit_extent(
    orbit: Dict[str, Any],
    *,
    total_mass_msun: Optional[float],
    mass_inputs: Iterable[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    """Attach a physical extent without promoting presentation geometry."""
    result = copy.deepcopy(orbit)
    fields = result.get("fields") if isinstance(result.get("fields"), dict) else {}
    source_axis = _field(fields, "semi_major_axis_au")
    period = _field(fields, "period_days")
    eccentricity = _field(fields, "eccentricity")
    coherence = _axis_coherence(source_axis, period, total_mass_msun)
    mass_input_rows = [item for item in mass_inputs if isinstance(item, dict)]
    mass_lower, mass_upper = _mass_interval(mass_input_rows)
    mass_statuses = {
        str(item.get("status") or "missing").lower() for item in mass_input_rows
    }
    mass_basis = (
        "source_model_assisted"
        if "source_model" in mass_statuses
        else "derived_assisted"
        if "derived" in mass_statuses
        else "measurements_only"
        if mass_statuses and mass_statuses <= {"source"}
        else "unavailable"
    )

    axis_field: Dict[str, Any]
    if _accepted_field(source_axis) and coherence.get("status") != "rejected":
        axis_field = copy.deepcopy(source_axis)
        axis_basis = "accepted_orbit_axis"
    elif _accepted_orbital_period(period) and _positive_float(total_mass_msun) is not None:
        axis_value = kepler_semi_major_axis_au(period.get("value"), total_mass_msun)
        period_lower, period_upper = _input_interval(period)
        axis_lower = (
            kepler_semi_major_axis_au(period_lower, mass_lower)
            if period_lower is not None and mass_lower is not None else None
        )
        axis_upper = (
            kepler_semi_major_axis_au(period_upper, mass_upper)
            if period_upper is not None and mass_upper is not None else None
        )
        confidence = (
            0.84 if mass_basis == "measurements_only"
            else 0.72 if mass_basis == "source_model_assisted"
            else 0.64
        )
        axis_field = _derived_field(
            key="semi_major_axis_au",
            label="Semi-major axis",
            value=axis_value,
            unit="au",
            basis=KEPLER_AXIS_DERIVATION_VERSION,
            inputs=[period, *mass_input_rows],
            confidence=confidence,
            value_lower=axis_lower,
            value_upper=axis_upper,
            interval_semantics=(
                "monotonic_bounds_from_period_and_endpoint_mass_intervals"
                if axis_lower is not None and axis_upper is not None else None
            ),
            notes=(
                "Relative semi-major axis derived from the accepted orbital period and "
                "the applicable endpoint mass sum. It is not a fitted catalog axis."
            ),
        )
        axis_basis = "kepler_period_total_mass"
    else:
        axis_field = _missing_field(
            "semi_major_axis_au",
            "Semi-major axis",
            "au",
            "no accepted physical axis or coherent period and endpoint masses",
        )
        axis_basis = "unavailable"

    axis = _positive_float(axis_field.get("value"))
    eccentricity_value = (
        _nonnegative_float(eccentricity.get("value"))
        if str(eccentricity.get("status") or "").lower() in {"source", "derived"}
        else None
    )
    if eccentricity_value is not None and eccentricity_value >= 1:
        eccentricity_value = None
    if axis is not None:
        bound_eccentricity = eccentricity_value if eccentricity_value is not None else 0.0
        apoapsis = axis * (1.0 + bound_eccentricity)
        apoapsis_field = _derived_field(
            key="apoapsis_extent_au",
            label="Apoapsis extent",
            value=apoapsis,
            unit="au",
            basis=(
                "semi_major_axis_times_one_plus_eccentricity"
                if eccentricity_value is not None
                else "semi_major_axis_bound_with_unknown_eccentricity"
            ),
            inputs=[axis_field, eccentricity] if eccentricity_value is not None else [axis_field],
            confidence=axis_field.get("confidence"),
            notes=(
                None
                if eccentricity_value is not None
                else "Eccentricity is unavailable or presentation-only, so the focus bound uses the semi-major axis."
            ),
        )
        applicability = "physical"
        completeness = "complete" if eccentricity_value is not None else "axis_only"
    else:
        apoapsis_field = _missing_field(
            "apoapsis_extent_au",
            "Apoapsis extent",
            "au",
            "semi-major axis unavailable",
        )
        applicability = "unavailable"
        completeness = "unavailable"

    result["physical_extent"] = {
        "schema_version": PHYSICAL_SCALE_CONTRACT_VERSION,
        "policy_version": PHYSICAL_EXTENT_POLICY_VERSION,
        "applicability": applicability,
        "completeness": completeness,
        "axis_basis": axis_basis,
        "semi_major_axis_au": axis_field,
        "eccentricity": copy.deepcopy(eccentricity),
        "apoapsis_extent_au": apoapsis_field,
        "total_mass_msun": round(total_mass_msun, 9) if _positive_float(total_mass_msun) else None,
        "total_mass_interval_msun": (
            [round(mass_lower, 9), round(mass_upper, 9)]
            if mass_lower is not None and mass_upper is not None else None
        ),
        "mass_basis": mass_basis,
        "mass_input_statuses": sorted(mass_statuses),
        "orientation_status": {
            key: str(_field(fields, key).get("status") or "missing")
            for key in (
                "inclination_deg",
                "longitude_ascending_node_deg",
                "argument_periastron_deg",
            )
        },
        "phase_status": str(
            _field(fields, "epoch_periastron_jd").get("status") or "missing"
        ),
        "coherence": coherence,
        "rejected_axis": copy.deepcopy(source_axis) if coherence.get("status") == "rejected" else None,
        "presentation_radius_excluded": True,
        "excluded_inputs": ["display_radius_scene", "projected_separation_au", "static_hierarchy_offset"],
    }
    return result


def _body_mass_field(body: Dict[str, Any]) -> Dict[str, Any]:
    return _field(body.get("fields"), "mass_msun")


def _body_mass(body: Dict[str, Any]) -> Optional[float]:
    field = _body_mass_field(body)
    if str(field.get("status") or "").lower() not in {
        "source",
        "source_model",
        "derived",
    }:
        return None
    return _positive_float(field.get("value"))


def _selected_stellar_body_mass(body: Dict[str, Any]) -> Optional[float]:
    """Return only mass accepted by the shared exact-leaf selection policy."""
    field = _body_mass_field(body)
    if field.get("selection_policy_version") != SELECTED_STELLAR_MASS_POLICY_VERSION:
        return None
    return _body_mass(body)


def attach_physical_extents_to_orbits(
    orbits: Iterable[Dict[str, Any]],
    stars: Iterable[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    stars_by_key = {
        str(body.get("render_key") or body.get("key")): body
        for body in stars
        if body.get("render_key") or body.get("key")
    }

    def side_keys(orbit: Dict[str, Any], side: str) -> list[str]:
        child_key = f"{side}_child_body_keys"
        body_key = f"{side}_body_key"
        keys = orbit.get(child_key) or [orbit.get(body_key)]
        return sorted({str(key) for key in keys if str(key or "") in stars_by_key})

    output = []
    for orbit in orbits:
        keys = sorted(set(side_keys(orbit, "primary") + side_keys(orbit, "secondary")))
        mass_fields = [_body_mass_field(stars_by_key[key]) for key in keys]
        masses = [_selected_stellar_body_mass(stars_by_key[key]) for key in keys]
        total_mass = sum(value for value in masses if value is not None)
        if not keys or any(value is None for value in masses):
            total_mass = 0.0
        output.append(
            attach_physical_orbit_extent(
                orbit,
                total_mass_msun=total_mass or None,
                mass_inputs=mass_fields,
            )
        )
    return output


def attach_physical_extents_to_planets(
    planets: Iterable[Dict[str, Any]],
    stars: Iterable[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    """Attach the same explicit extent contract to each rendered planet orbit."""
    stars_by_key = {
        str(body.get("render_key") or body.get("key")): body
        for body in stars
        if body.get("render_key") or body.get("key")
    }
    output = []
    for planet in planets:
        host = stars_by_key.get(str(planet.get("host_body_key") or ""))
        host_mass_field = _body_mass_field(host or {})
        host_mass = _body_mass(host or {})
        result = attach_physical_orbit_extent(
            planet,
            total_mass_msun=host_mass,
            mass_inputs=[host_mass_field] if host_mass_field else [],
        )
        result["physical_extent"]["orbit_kind"] = "planetary_orbit"
        result["physical_extent"]["host_body_key"] = planet.get("host_body_key")
        output.append(result)
    return output


def _star_neighborhood(star: Dict[str, Any], planets: list[Dict[str, Any]]) -> Dict[str, Any]:
    star_key = str(star.get("render_key") or star.get("key") or "")
    luminosity = _field(star.get("fields"), "luminosity_lsun")
    luminosity_value = _positive_float(luminosity.get("value"))
    hz_outer = math.sqrt(luminosity_value / 0.35) if luminosity_value is not None else None
    formation = {
        key: math.sqrt(luminosity_value) * (278.0 / temp_k) ** 2
        for key, temp_k in FORMATION_LINE_TEMPERATURES_K.items()
    } if luminosity_value is not None else {}
    hosted_planets = [planet for planet in planets if str(planet.get("host_body_key") or "") == star_key]
    planet_extents = []
    for planet in hosted_planets:
        extent = planet.get("physical_extent") if isinstance(planet.get("physical_extent"), dict) else {}
        apoapsis = _positive_float((extent.get("apoapsis_extent_au") or {}).get("value"))
        if extent.get("applicability") == "physical" and apoapsis is not None:
            planet_extents.append(apoapsis)
    base_values = [value for value in [hz_outer, *planet_extents] if value is not None]
    return {
        "star_key": star_key,
        "base_radius_au": max(base_values) if base_values else 0.0,
        "habitable_zone_outer_au": round(hz_outer, 9) if hz_outer is not None else None,
        "formation_line_radii_au": {key: round(value, 9) for key, value in formation.items()},
        "max_overlay_radius_au": round(max(formation.values()), 9) if formation else None,
        "planet_keys": [str(planet.get("render_key") or planet.get("key")) for planet in hosted_planets],
    }


def build_focus_graph(
    *,
    system_name: str,
    simulation_tree: Dict[str, Any],
    stars: Iterable[Dict[str, Any]],
    planets: Iterable[Dict[str, Any]],
    orbits: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    tree_nodes = simulation_tree.get("nodes") if isinstance(simulation_tree, dict) else {}
    tree_nodes = tree_nodes if isinstance(tree_nodes, dict) else {}
    stars_list = list(stars)
    planets_list = list(planets)
    orbits_list = list(orbits)
    orbit_by_key = {str(orbit.get("orbit_key")): orbit for orbit in orbits_list if orbit.get("orbit_key")}
    neighborhoods = {
        item["star_key"]: item
        for item in (_star_neighborhood(star, planets_list) for star in stars_list)
        if item["star_key"]
    }
    nodes: Dict[str, Dict[str, Any]] = {}

    def compile_node(
        tree_key: str,
        parent_focus_key: Optional[str],
    ) -> tuple[str, float, str, float, float]:
        node = tree_nodes.get(tree_key) or {}
        node_type = str(node.get("node_type") or "unknown")
        focus_key = f"focus:{tree_key}"
        child_results = [compile_node(str(child), focus_key) for child in node.get("children") or []]
        child_focus_keys = [item[0] for item in child_results]
        child_view_radii = [item[1] for item in child_results]
        child_statuses = [item[2] for item in child_results]
        child_overlay_radii = [item[3] for item in child_results]
        child_layout_radii = [item[4] for item in child_results]
        view_radius = 0.0
        layout_radius = 0.0
        available_overlay_radius = max(child_overlay_radii, default=0.0)
        status = "unavailable"
        view_applicability = "unavailable"
        basis = "recursive_child_bounds"
        object_key = None
        orbit_key = str(node.get("orbit_key") or "") or None
        if node_type == "body":
            object_key = str(node.get("body_key") or "") or None
            neighborhood = neighborhoods.get(object_key or "", {})
            view_radius = float(neighborhood.get("base_radius_au") or 0.0)
            available_overlay_radius = max(
                view_radius,
                float(neighborhood.get("max_overlay_radius_au") or 0.0),
            )
            status = "complete" if view_radius > 0 else "identity_only"
            view_applicability = "local_neighborhood" if view_radius > 0 else "unavailable"
            basis = "host_planets_and_habitable_zone"
        elif orbit_key:
            orbit = orbit_by_key.get(orbit_key) or {}
            extent = orbit.get("physical_extent") if isinstance(orbit.get("physical_extent"), dict) else {}
            own_radius = _positive_float((extent.get("apoapsis_extent_au") or {}).get("value"))
            if own_radius is not None:
                layout_radius = own_radius + max(child_layout_radii, default=0.0)
                view_radius = max(layout_radius, own_radius + max(child_view_radii, default=0.0))
                available_overlay_radius = max(
                    available_overlay_radius,
                    own_radius + max(child_overlay_radii, default=0.0),
                )
                status = "complete" if extent.get("completeness") == "complete" and all(value == "complete" for value in child_statuses) else "partial"
                view_applicability = "physical_layout"
                basis = "physical_orbit_apoapsis_plus_child_bounds"
            else:
                # Descendant HZs and inner orbits do not locate this relation's
                # children relative to one another. They remain inspectable at
                # their own focus nodes, but cannot certify this parent view.
                view_radius = 0.0
                layout_radius = 0.0
                status = "unavailable"
                view_applicability = "unavailable"
                basis = "physical_orbit_extent_unavailable"
        elif node_type == "root":
            if len(child_results) == 1 and child_view_radii[0] > 0:
                view_radius = child_view_radii[0]
                layout_radius = child_layout_radii[0]
                status = child_statuses[0]
                child_node = nodes.get(child_focus_keys[0]) or {}
                view_applicability = str(
                    (child_node.get("physical_bounds") or {}).get("view_applicability")
                    or "unavailable"
                )
                basis = "single_child_physical_view"
            else:
                # A root with multiple independently placed children has no
                # physical relation establishing their separation.
                status = "unavailable"
                view_applicability = "unavailable"
                basis = "root_physical_layout_unavailable"

        nodes[focus_key] = {
            "focus_key": focus_key,
            "tree_node_key": tree_key,
            "focus_kind": "system" if node_type == "root" else ("orbit" if orbit_key else node_type),
            "display_name": node.get("display_name") or (system_name if node_type == "root" else tree_key),
            "parent_focus_key": parent_focus_key,
            "child_focus_keys": child_focus_keys,
            "object_key": object_key,
            "orbit_key": orbit_key,
            "leaf_body_keys": sorted(str(key) for key in node.get("leaf_body_keys") or []),
            "physical_bounds": {
                "radius_au": round(view_radius, 9) if view_radius > 0 else None,
                "view_radius_au": round(view_radius, 9) if view_radius > 0 else None,
                "layout_radius_au": round(layout_radius, 9) if layout_radius > 0 else None,
                "status": status,
                "view_applicability": view_applicability,
                "basis": basis,
                "includes_active_overlays": False,
                "available_overlay_radius_au": round(available_overlay_radius, 9) if available_overlay_radius > 0 else None,
            },
            "supported_actions": ["select", "focus", "lens"] + (["fit_system"] if node_type == "root" else ["parent"]),
        }
        return focus_key, view_radius, status, available_overlay_radius, layout_radius

    root_tree_key = str(simulation_tree.get("root_node_key") or "")
    if root_tree_key and root_tree_key in tree_nodes:
        root_focus_key, _, _, _, _ = compile_node(root_tree_key, None)
    else:
        root_focus_key = "focus:root:system"
        nodes[root_focus_key] = {
            "focus_key": root_focus_key,
            "tree_node_key": None,
            "focus_kind": "system",
            "display_name": system_name,
            "parent_focus_key": None,
            "child_focus_keys": [],
            "object_key": None,
            "orbit_key": None,
            "leaf_body_keys": sorted(neighborhoods),
            "physical_bounds": {
                "radius_au": None,
                "view_radius_au": None,
                "layout_radius_au": None,
                "status": "unavailable",
                "view_applicability": "unavailable",
                "basis": "simulation_tree_unavailable",
                "includes_active_overlays": False,
                "available_overlay_radius_au": None,
            },
            "supported_actions": ["select", "fit_system"],
        }

    object_focus_key = {
        str(node.get("object_key")): key
        for key, node in nodes.items()
        if node.get("object_key")
    }
    for planet in planets_list:
        planet_key = str(planet.get("render_key") or planet.get("key") or "")
        if not planet_key:
            continue
        host_key = str(planet.get("host_body_key") or "")
        parent_key = object_focus_key.get(host_key, root_focus_key)
        extent = planet.get("physical_extent") if isinstance(planet.get("physical_extent"), dict) else {}
        axis = _positive_float((extent.get("semi_major_axis_au") or {}).get("value")) if extent.get("applicability") == "physical" else None
        focus_key = f"focus:planet:{planet_key}"
        nodes[focus_key] = {
            "focus_key": focus_key,
            "tree_node_key": None,
            "focus_kind": "planet",
            "display_name": planet.get("display_name") or planet.get("name") or planet_key,
            "parent_focus_key": parent_key,
            "child_focus_keys": [],
            "object_key": planet_key,
            "orbit_key": None,
            "leaf_body_keys": [],
            "physical_bounds": {
                "radius_au": round(axis, 9) if axis is not None else None,
                "view_radius_au": round(axis, 9) if axis is not None else None,
                "layout_radius_au": round(axis, 9) if axis is not None else None,
                "status": "complete" if axis is not None else "unavailable",
                "view_applicability": "planet_orbit" if axis is not None else "unavailable",
                "basis": "planet_semi_major_axis",
                "includes_active_overlays": False,
                "available_overlay_radius_au": round(axis, 9) if axis is not None else None,
            },
            "supported_actions": ["select", "focus", "lens", "parent"],
        }
        if parent_key in nodes:
            nodes[parent_key]["child_focus_keys"].append(focus_key)

    for node in nodes.values():
        node["child_focus_keys"] = sorted(
            set(node.get("child_focus_keys") or []),
            key=lambda key: (str(nodes.get(key, {}).get("display_name") or key).casefold(), key),
        )

    return {
        "schema_version": FOCUS_GRAPH_VERSION,
        "physical_scale_schema_version": PHYSICAL_SCALE_CONTRACT_VERSION,
        "root_focus_key": root_focus_key,
        "default_focus_key": root_focus_key,
        "nodes": nodes,
        "star_neighborhoods": neighborhoods,
        "diagnostics": {
            "focus_node_count": len(nodes),
            "physical_bound_count": sum(1 for node in nodes.values() if _positive_float((node.get("physical_bounds") or {}).get("radius_au")) is not None),
            "physical_layout_bound_count": sum(1 for node in nodes.values() if _positive_float((node.get("physical_bounds") or {}).get("layout_radius_au")) is not None),
            "local_neighborhood_bound_count": sum(1 for node in nodes.values() if (node.get("physical_bounds") or {}).get("view_applicability") == "local_neighborhood"),
            "unavailable_bound_count": sum(1 for node in nodes.values() if (node.get("physical_bounds") or {}).get("status") == "unavailable"),
            "partial_bound_count": sum(1 for node in nodes.values() if (node.get("physical_bounds") or {}).get("status") == "partial"),
            "planet_focus_count": sum(1 for node in nodes.values() if node.get("focus_kind") == "planet"),
        },
    }
