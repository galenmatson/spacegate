from __future__ import annotations

import math

from srv.api.app.simulation_physical_scale import (
    FOCUS_GRAPH_VERSION,
    PHYSICAL_SCALE_CONTRACT_VERSION,
    attach_physical_orbit_extent,
    attach_physical_extents_to_planets,
    build_focus_graph,
    implied_total_mass_msun,
    kepler_semi_major_axis_au,
)


def source_field(key: str, value: float, unit: str | None = None) -> dict:
    return {"key": key, "label": key, "value": value, "unit": unit, "status": "source", "layer": "arm", "basis": "test"}


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
    assert math.isclose(extent["apoapsis_extent_au"]["value"], 1.1, rel_tol=1e-9)
    assert "display_radius_scene" in extent["excluded_inputs"]


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
    assert star_focus["physical_bounds"]["available_overlay_radius_au"] > star_focus["physical_bounds"]["radius_au"]
