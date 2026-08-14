from __future__ import annotations

import math

from srv.api.app.simulation_physical_scale import (
    FOCUS_GRAPH_VERSION,
    PHYSICAL_SCALE_CONTRACT_VERSION,
    attach_physical_orbit_extent,
    attach_physical_extents_to_orbits,
    attach_physical_extents_to_planets,
    build_focus_graph,
    implied_total_mass_msun,
    kepler_semi_major_axis_au,
)


def source_field(key: str, value: float, unit: str | None = None) -> dict:
    return {"key": key, "label": key, "value": value, "unit": unit, "status": "source", "layer": "arm", "basis": "test"}


def selected_mass_field(value: float, *, status: str = "source") -> dict:
    field = source_field("mass_msun", value, "Msun")
    field["status"] = status
    field["selection_policy_version"] = "stellar_leaf_mass_selection_v2"
    return field


def test_kepler_axis_matches_one_au_reference() -> None:
    assert math.isclose(kepler_semi_major_axis_au(365.25, 1.0), 1.0, rel_tol=1e-12)
    assert math.isclose(implied_total_mass_msun(365.25, 1.0), 1.0, rel_tol=1e-12)


def test_physical_extent_derives_axis_from_source_period_and_mass() -> None:
    orbit = {
        "orbit_key": "orbit:test",
        "fields": {
            "period_days": source_field("period_days", 365.25, "days"),
            "semi_major_axis_au": {"key": "semi_major_axis_au", "value": None, "status": "missing"},
            "eccentricity": source_field("eccentricity", 0.1),
        },
        "display_radius_scene": 4.2,
    }
    result = attach_physical_orbit_extent(
        orbit,
        total_mass_msun=1.0,
        mass_inputs=[source_field("mass_msun", 1.0, "Msun")],
    )
    extent = result["physical_extent"]
    assert extent["schema_version"] == PHYSICAL_SCALE_CONTRACT_VERSION
    assert extent["applicability"] == "physical"
    assert extent["axis_basis"] == "kepler_period_total_mass"
    assert math.isclose(extent["semi_major_axis_au"]["value"], 1.0, rel_tol=1e-9)
    assert "value_lower" not in extent["semi_major_axis_au"]
    assert extent["total_mass_interval_msun"] is None
    assert math.isclose(extent["apoapsis_extent_au"]["value"], 1.1, rel_tol=1e-9)
    assert "display_radius_scene" in extent["excluded_inputs"]


def test_source_model_mass_derivation_preserves_basis_and_uncertainty() -> None:
    period = source_field("period_days", 365.25, "days")
    period.update({"value_lower": 360.0, "value_upper": 370.0})
    modeled_mass = source_field("mass_msun", 1.0, "Msun")
    modeled_mass.update({
        "status": "source_model",
        "value_lower": 0.8,
        "value_upper": 1.2,
        "source_catalog": "multiplicity.msc",
        "source_reference": "mass-evidence-1",
    })
    result = attach_physical_orbit_extent(
        {
            "orbit_key": "orbit:modeled-mass",
            "fields": {
                "period_days": period,
                "semi_major_axis_au": {
                    "key": "semi_major_axis_au",
                    "value": None,
                    "status": "missing",
                },
                "eccentricity": {"key": "eccentricity", "value": None, "status": "missing"},
            },
        },
        total_mass_msun=1.0,
        mass_inputs=[modeled_mass],
    )
    extent = result["physical_extent"]
    axis = extent["semi_major_axis_au"]
    assert extent["applicability"] == "physical"
    assert extent["mass_basis"] == "source_model_assisted"
    assert extent["total_mass_interval_msun"] == [0.8, 1.2]
    assert axis["status"] == "derived"
    assert axis["value_lower"] < axis["value"] < axis["value_upper"]
    assert axis["interval_semantics"] == "monotonic_bounds_from_period_and_endpoint_mass_intervals"
    assert axis["input_lineage"][1]["source_reference"] == "mass-evidence-1"


def test_incomplete_endpoint_mass_set_remains_unavailable() -> None:
    stars = [
        {
            "render_key": "star:a",
            "fields": {"mass_msun": selected_mass_field(1.0)},
        },
        {
            "render_key": "star:b",
            "fields": {
                "mass_msun": {
                    "key": "mass_msun",
                    "value": None,
                    "status": "missing",
                }
            },
        },
    ]
    orbit = {
        "orbit_key": "orbit:incomplete-mass",
        "primary_child_body_keys": ["star:a"],
        "secondary_child_body_keys": ["star:b"],
        "fields": {
            "period_days": source_field("period_days", 365.25, "days"),
            "semi_major_axis_au": {
                "key": "semi_major_axis_au",
                "value": None,
                "status": "missing",
            },
            "eccentricity": {"key": "eccentricity", "value": None, "status": "missing"},
        },
    }
    extent = attach_physical_extents_to_orbits([orbit], stars)[0]["physical_extent"]
    assert extent["applicability"] == "unavailable"
    assert extent["mass_basis"] == "unavailable"
    assert extent["semi_major_axis_au"]["value"] is None


def test_stellar_orbit_requires_shared_selected_mass_projection() -> None:
    orbit = {
        "orbit_key": "orbit:selected-masses",
        "primary_child_body_keys": ["star:a"],
        "secondary_child_body_keys": ["star:b"],
        "fields": {
            "period_days": source_field("period_days", 365.25, "days"),
            "semi_major_axis_au": {
                "key": "semi_major_axis_au",
                "value": None,
                "status": "missing",
            },
            "eccentricity": {"key": "eccentricity", "value": None, "status": "missing"},
        },
    }
    legacy_stars = [
        {"render_key": "star:a", "fields": {"mass_msun": source_field("mass_msun", 0.6, "Msun")}},
        {
            "render_key": "star:b",
            "fields": {
                "mass_msun": {
                    **source_field("mass_msun", 0.4, "Msun"),
                    "status": "derived",
                    "basis": "presentation_mass_prior",
                }
            },
        },
    ]
    unavailable = attach_physical_extents_to_orbits([orbit], legacy_stars)[0]
    assert unavailable["physical_extent"]["applicability"] == "unavailable"

    selected_stars = [
        {"render_key": "star:a", "fields": {"mass_msun": selected_mass_field(0.6)}},
        {
            "render_key": "star:b",
            "fields": {"mass_msun": selected_mass_field(0.4, status="source_model")},
        },
    ]
    physical = attach_physical_extents_to_orbits([orbit], selected_stars)[0]
    extent = physical["physical_extent"]
    assert extent["applicability"] == "physical"
    assert extent["mass_basis"] == "source_model_assisted"
    assert math.isclose(extent["semi_major_axis_au"]["value"], 1.0, rel_tol=1e-9)


def test_physical_extent_rejects_projected_or_presentation_only_scale() -> None:
    orbit = {
        "orbit_key": "orbit:test",
        "fields": {
            "period_days": {"key": "period_days", "value": 100, "status": "assumed"},
            "semi_major_axis_au": {"key": "semi_major_axis_au", "value": None, "status": "assumed"},
            "projected_separation_au": {"key": "projected_separation_au", "value": 500, "status": "derived"},
            "eccentricity": {"key": "eccentricity", "value": 0.2, "status": "assumed"},
        },
        "display_radius_scene": 6.2,
    }
    extent = attach_physical_orbit_extent(orbit, total_mass_msun=2.0)["physical_extent"]
    assert extent["applicability"] == "unavailable"
    assert extent["semi_major_axis_au"]["value"] is None


def test_projected_separation_derived_period_cannot_become_physical_axis() -> None:
    orbit = {
        "orbit_key": "orbit:projected-period",
        "fields": {
            "period_days": {
                "key": "period_days",
                "value": 365.25,
                "status": "derived",
                "basis": "projected_separation_kepler_estimate",
            },
            "semi_major_axis_au": {
                "key": "semi_major_axis_au",
                "value": None,
                "status": "missing",
            },
            "projected_separation_au": {
                "key": "projected_separation_au",
                "value": 1.0,
                "status": "derived",
            },
            "eccentricity": {"key": "eccentricity", "value": None, "status": "missing"},
        },
    }

    extent = attach_physical_orbit_extent(
        orbit,
        total_mass_msun=1.0,
        mass_inputs=[selected_mass_field(1.0)],
    )["physical_extent"]

    assert extent["applicability"] == "unavailable"
    assert extent["axis_basis"] == "unavailable"


def test_physical_extent_rejects_a_kepler_incoherent_source_axis() -> None:
    orbit = {
        "orbit_key": "orbit:incoherent",
        "fields": {
            "period_days": source_field("period_days", 9.2, "days"),
            "semi_major_axis_au": source_field("semi_major_axis_au", 120.0, "au"),
            "eccentricity": source_field("eccentricity", 0.48),
        },
    }
    extent = attach_physical_orbit_extent(orbit, total_mass_msun=None)["physical_extent"]
    assert extent["applicability"] == "unavailable"
    assert extent["coherence"]["status"] == "rejected"
    assert extent["coherence"]["implied_total_mass_msun"] > 1_000.0
    assert extent["rejected_axis"]["value"] == 120.0


def test_planet_physical_extent_uses_host_mass_for_kepler_axis() -> None:
    star = {
        "render_key": "star:a",
        "fields": {"mass_msun": source_field("mass_msun", 1.0, "Msun")},
    }
    planet = {
        "render_key": "planet:b",
        "host_body_key": "star:a",
        "fields": {
            "period_days": source_field("period_days", 365.25, "days"),
            "semi_major_axis_au": {"key": "semi_major_axis_au", "value": None, "status": "missing"},
            "eccentricity": source_field("eccentricity", 0.0),
        },
    }
    result = attach_physical_extents_to_planets([planet], [star])[0]
    extent = result["physical_extent"]
    assert extent["orbit_kind"] == "planetary_orbit"
    assert extent["host_body_key"] == "star:a"
    assert extent["applicability"] == "physical"
    assert extent["axis_basis"] == "kepler_period_total_mass"
    assert math.isclose(extent["semi_major_axis_au"]["value"], 1.0, rel_tol=1e-9)


def test_focus_graph_keeps_planet_under_host_and_root_bounds() -> None:
    star = {
        "render_key": "star:a",
        "display_name": "A",
        "fields": {
            "luminosity_lsun": source_field("luminosity_lsun", 1.0, "Lsun"),
            "mass_msun": source_field("mass_msun", 1.0, "Msun"),
        },
    }
    planet = {
        "render_key": "planet:b",
        "display_name": "b",
        "host_body_key": "star:a",
        "fields": {
            "semi_major_axis_au": source_field("semi_major_axis_au", 1.0, "au"),
            "eccentricity": source_field("eccentricity", 0.0),
        },
    }
    tree = {
        "schema_version": "simulation_tree_v1",
        "root_node_key": "root:system",
        "nodes": {
            "root:system": {"node_key": "root:system", "node_type": "root", "display_name": "Test", "children": ["body:star:a"], "leaf_body_keys": ["star:a"]},
            "body:star:a": {"node_key": "body:star:a", "node_type": "body", "body_key": "star:a", "display_name": "A", "children": [], "leaf_body_keys": ["star:a"]},
        },
    }
    planet = attach_physical_extents_to_planets([planet], [star])[0]
    graph = build_focus_graph(system_name="Test", simulation_tree=tree, stars=[star], planets=[planet], orbits=[])
    assert graph["schema_version"] == FOCUS_GRAPH_VERSION
    assert graph["root_focus_key"] == "focus:root:system"
    star_focus = graph["nodes"]["focus:body:star:a"]
    assert "focus:planet:planet:b" in star_focus["child_focus_keys"]
    assert star_focus["physical_bounds"]["radius_au"] > 1.0
    assert star_focus["physical_bounds"]["view_applicability"] == "local_neighborhood"
    assert star_focus["physical_bounds"]["layout_radius_au"] is None
    assert star_focus["physical_bounds"]["available_overlay_radius_au"] > star_focus["physical_bounds"]["radius_au"]
    root_bounds = graph["nodes"][graph["root_focus_key"]]["physical_bounds"]
    assert root_bounds["view_applicability"] == "local_neighborhood"
    assert root_bounds["radius_au"] == star_focus["physical_bounds"]["radius_au"]


def test_unresolved_multistar_relation_is_not_certified_by_descendant_hz() -> None:
    stars = [
        {
            "render_key": "star:a",
            "display_name": "A",
            "fields": {"luminosity_lsun": source_field("luminosity_lsun", 100.0, "Lsun")},
        },
        {"render_key": "star:b", "display_name": "B", "fields": {}},
    ]
    orbit = attach_physical_orbit_extent(
        {
            "orbit_key": "orbit:ab",
            "fields": {
                "period_days": {"key": "period_days", "value": None, "status": "missing"},
                "semi_major_axis_au": {"key": "semi_major_axis_au", "value": None, "status": "missing"},
                "eccentricity": {"key": "eccentricity", "value": None, "status": "missing"},
            },
        },
        total_mass_msun=None,
    )
    tree = {
        "schema_version": "simulation_tree_v1",
        "root_node_key": "root:system",
        "nodes": {
            "root:system": {
                "node_key": "root:system",
                "node_type": "root",
                "display_name": "Unresolved multiple",
                "children": ["orbit:ab"],
                "leaf_body_keys": ["star:a", "star:b"],
            },
            "orbit:ab": {
                "node_key": "orbit:ab",
                "node_type": "barycenter",
                "display_name": "A - B",
                "orbit_key": "orbit:ab",
                "children": ["body:star:a", "body:star:b"],
                "leaf_body_keys": ["star:a", "star:b"],
            },
            "body:star:a": {
                "node_key": "body:star:a",
                "node_type": "body",
                "body_key": "star:a",
                "display_name": "A",
                "children": [],
                "leaf_body_keys": ["star:a"],
            },
            "body:star:b": {
                "node_key": "body:star:b",
                "node_type": "body",
                "body_key": "star:b",
                "display_name": "B",
                "children": [],
                "leaf_body_keys": ["star:b"],
            },
        },
    }
    graph = build_focus_graph(
        system_name="Unresolved multiple",
        simulation_tree=tree,
        stars=stars,
        planets=[],
        orbits=[orbit],
    )

    star_bounds = graph["nodes"]["focus:body:star:a"]["physical_bounds"]
    orbit_bounds = graph["nodes"]["focus:orbit:ab"]["physical_bounds"]
    root_bounds = graph["nodes"]["focus:root:system"]["physical_bounds"]
    assert star_bounds["view_applicability"] == "local_neighborhood"
    assert star_bounds["radius_au"] > 0
    assert orbit_bounds["view_applicability"] == "unavailable"
    assert orbit_bounds["radius_au"] is None
    assert orbit_bounds["available_overlay_radius_au"] > 0
    assert root_bounds["view_applicability"] == "unavailable"
    assert root_bounds["radius_au"] is None


def test_resolved_multistar_relation_provides_physical_layout() -> None:
    stars = [
        {"render_key": "star:a", "fields": {"luminosity_lsun": source_field("luminosity_lsun", 1.0)}},
        {"render_key": "star:b", "fields": {}},
    ]
    orbit = attach_physical_orbit_extent(
        {
            "orbit_key": "orbit:ab",
            "fields": {
                "period_days": source_field("period_days", 365.25, "days"),
                "semi_major_axis_au": source_field("semi_major_axis_au", 1.0, "au"),
                "eccentricity": source_field("eccentricity", 0.1),
            },
        },
        total_mass_msun=1.0,
    )
    tree = {
        "root_node_key": "root:system",
        "nodes": {
            "root:system": {"node_type": "root", "children": ["orbit:ab"], "leaf_body_keys": ["star:a", "star:b"]},
            "orbit:ab": {"node_type": "barycenter", "orbit_key": "orbit:ab", "children": ["body:a", "body:b"], "leaf_body_keys": ["star:a", "star:b"]},
            "body:a": {"node_type": "body", "body_key": "star:a", "children": [], "leaf_body_keys": ["star:a"]},
            "body:b": {"node_type": "body", "body_key": "star:b", "children": [], "leaf_body_keys": ["star:b"]},
        },
    }
    graph = build_focus_graph(system_name="Resolved multiple", simulation_tree=tree, stars=stars, planets=[], orbits=[orbit])
    root_bounds = graph["nodes"]["focus:root:system"]["physical_bounds"]
    orbit_bounds = graph["nodes"]["focus:orbit:ab"]["physical_bounds"]
    assert orbit_bounds["view_applicability"] == "physical_layout"
    assert orbit_bounds["layout_radius_au"] == 1.1
    assert root_bounds["view_applicability"] == "physical_layout"
    assert root_bounds["radius_au"] >= 1.1
