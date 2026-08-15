from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from compare_physical_extent_coverage import compare  # noqa: E402


def _report(state: str, *, physical: int) -> dict:
    return {
        "schema_version": "spacegate.physical_extent_coverage_audit.v1",
        "label": state,
        "artifact_set_sha256": state,
        "counts": {"physically_scalable": physical},
        "relation_inventory": [
            {
                "relation_key": "system:1|7|multiplicity.msc",
                "state": state,
                "axis_basis": "kepler_period_total_mass" if state == "derived" else "unavailable",
            }
        ],
    }


def test_compare_reports_exact_recovery() -> None:
    report = compare(
        _report("unavailable", physical=0),
        _report("derived", physical=1),
    )

    assert report["status"] == "pass"
    assert report["recovered_count"] == 1
    assert report["regression_count"] == 0
    assert report["count_deltas"]["physically_scalable"] == {
        "before": 0,
        "after": 1,
        "delta": 1,
    }
    assert report["transitions"] == {"unavailable->derived": 1}


def test_compare_fails_on_physical_regression() -> None:
    report = compare(
        _report("physical", physical=1),
        _report("rejected", physical=0),
    )

    assert report["status"] == "fail"
    assert report["regression_count"] == 1


def test_compare_accepts_retirement_of_legacy_unselected_mass_derivation() -> None:
    before = _report("derived", physical=1)
    before["relation_inventory"][0].update(
        known_endpoint_masses=0,
        missing_endpoint_masses=2,
        legacy_known_endpoint_masses=2,
    )
    after = _report("unavailable", physical=0)
    after["relation_inventory"][0].update(
        known_endpoint_masses=1,
        missing_endpoint_masses=1,
        legacy_known_endpoint_masses=1,
    )

    report = compare(before, after)

    assert report["status"] == "pass"
    assert report["regression_count"] == 0
    assert report["justified_retirement_count"] == 1
    assert report["justified_retirements"][0]["retirement_reason"] == (
        "legacy_kepler_axis_used_unselected_endpoint_mass"
    )


def test_compare_does_not_retire_source_axis_regression() -> None:
    before = _report("physical", physical=1)
    before["relation_inventory"][0].update(
        axis_basis="accepted_source_axis",
        known_endpoint_masses=0,
        legacy_known_endpoint_masses=2,
    )
    after = _report("unavailable", physical=0)
    after["relation_inventory"][0].update(
        known_endpoint_masses=1,
        missing_endpoint_masses=1,
    )

    report = compare(before, after)

    assert report["status"] == "fail"
    assert report["regression_count"] == 1
    assert report["justified_retirement_count"] == 0
