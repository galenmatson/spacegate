from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from audit_physical_extent_coverage import audit  # noqa: E402


def _field(key: str, value: float | None, status: str) -> dict:
    return {"key": key, "value": value, "status": status, "basis": "test"}


def _selected_mass(value: float | None, status: str) -> dict:
    return {
        **_field("mass_msun", value, status),
        "selection_policy_version": "stellar_leaf_mass_selection_v1",
    }


def _write_scene(path: Path, *, derived: bool, system_id: int = 1) -> None:
    mass_b = _selected_mass(0.5 if derived else None, "source_model" if derived else "missing")
    extent = {
        "applicability": "physical" if derived else "unavailable",
        "axis_basis": "kepler_period_total_mass" if derived else "unavailable",
        "mass_basis": "source_model_assisted" if derived else "unavailable",
        "coherence": {"status": "not_testable"},
    }
    payload = {
        "build_id": "build-test",
        "materialization": {"materializer_version": "simulation_scene_artifact_v18"},
        "system": {
            "system_id": system_id,
            "stable_object_key": f"system:{system_id}",
            "display_name": "Test",
            "dist_ly": 10.0,
        },
        "render_scene": {
            "physical_scale": {"schema_version": "simulation_physical_scale_v2"},
            "bodies": {
                "stars": [
                    {"render_key": "star:a", "spectral_class": "G", "fields": {"mass_msun": _selected_mass(1.0, "source")}},
                    {"render_key": "star:b", "spectral_class": "M", "fields": {"mass_msun": mass_b}},
                ]
            },
            "focus_graph": {"nodes": {}},
            "orbits": [
                {
                    "orbit_key": "orbit:ab",
                    "display_name": "A - B",
                    "relation_kind": "binary",
                    "endpoint_kind": "stellar_pair",
                    "primary_child_body_keys": ["star:a"],
                    "secondary_child_body_keys": ["star:b"],
                    "source": {"source_catalog": "multiplicity.msc"},
                    "fields": {
                        "period_days": _field("period_days", 365.25, "source"),
                        "semi_major_axis_au": _field("semi_major_axis_au", None, "missing"),
                        "projected_separation_au": _field("projected_separation_au", None, "missing"),
                    },
                    "physical_extent": extent,
                }
            ],
        },
    }
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def test_audit_separates_recovered_and_incomplete_mass_relations(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_scene(cache / "system_1.json.gz", derived=True)
    _write_scene(cache / "system_2.json.gz", derived=False, system_id=2)

    report = audit(cache, label="test", expected_scenes=2)

    assert report["status"] == "pass"
    assert report["counts"]["stellar_relation_rows"] == 2
    assert report["counts"]["kepler_derived_axis"] == 1
    assert report["counts"][
        "accepted_period_incomplete_endpoint_masses_selected_projection"
    ] == 1
    assert report["counts"]["accepted_period_incomplete_endpoint_masses_legacy"] == 1
    assert report["breakdowns"]["mass_basis_by_state"] == {
        "derived|source_model_assisted": 1,
        "unavailable|unavailable": 1,
    }


def test_audit_fails_scene_count_gate(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_scene(cache / "system_1.json.gz", derived=True)

    report = audit(cache, label="test", expected_scenes=2)

    assert report["status"] == "fail"
    assert report["scene_count_matches"] is False


def test_audit_accounts_for_arm_binding_outcomes(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_scene(cache / "system_1.json.gz", derived=True)
    arm = tmp_path / "arm.duckdb"
    con = duckdb.connect(str(arm))
    con.execute(
        """
        CREATE TABLE stellar_orbit_relation_bindings(
          relation_id VARCHAR,binding_status VARCHAR,simulation_eligible BOOLEAN
        );
        INSERT INTO stellar_orbit_relation_bindings VALUES
          ('r1','accepted',true),('r2','one_endpoint_unresolved',false);
        CREATE TABLE selected_stellar_orbit_relations(
          relation_id VARCHAR,source_id VARCHAR
        );
        INSERT INTO selected_stellar_orbit_relations VALUES
          ('r1','multiplicity.msc'),('r2','multiplicity.msc');
        CREATE TABLE stellar_orbit_endpoint_bindings(
          binding_status VARCHAR,endpoint_kind VARCHAR,binding_reason VARCHAR
        );
        INSERT INTO stellar_orbit_endpoint_bindings VALUES
          ('accepted','leaf','exact'),('missing',NULL,'not_found');
        """
    )
    con.close()

    report = audit(cache, label="test", expected_scenes=1, arm_db=arm)

    assert report["status"] == "pass"
    assert report["arm_accounting"]["relation_bindings"] == {
        "accepted|simulation_eligible=true": 1,
        "one_endpoint_unresolved|simulation_eligible=false": 1,
    }
    assert report["arm_accounting"]["endpoint_bindings"] == {
        "accepted|leaf|exact": 1,
        "missing|unknown|not_found": 1,
    }
