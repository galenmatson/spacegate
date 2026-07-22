from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import analyze_e5_compile_performance as analyzer


def phase(
    name: str,
    wall: float,
    cpu: float,
    *,
    source_id: str | None = None,
) -> dict[str, object]:
    return {
        "phase": name,
        "source_id": source_id,
        "status": "pass",
        "wall_seconds": wall,
        "cpu_seconds": cpu,
        "process_peak_rss_kib": 100,
        "peak_staging_allocated_bytes": 200,
        "peak_spill_allocated_bytes": 300,
        "details": {},
    }


def test_performance_analysis_ranks_measured_targets() -> None:
    timing = {
        "schema_version": "spacegate.e5_compile_performance.v1",
        "status": "pass",
        "build_id": "a" * 24,
        "compiler_version": "selected_fact_compiler_test",
        "phases": [
            phase(
                "source_candidate_insertion",
                50,
                200,
                source_id="gaia.dr3.gaia_source",
            ),
            phase("selected_fact_exports", 20, 100),
            phase("global_parameter_set_selection", 12, 40),
            phase("artifact_hashing", 8, 3),
            phase(
                "immutable_e4_input_verification",
                10,
                5,
            ),
            phase(
                "source_binding",
                15,
                60,
                source_id="distance.gaia_edr3_bailer_jones",
            ),
            phase("integrity_check.binding", 5, 20),
        ],
    }
    report = analyzer.analyze(
        timing,
        {"status": "pass", "build_id": "a" * 24, "table_counts": {"selected_facts": 4}},
    )

    assert report["total"]["wall_seconds"] == 120
    assert report["top_phases"][0]["phase"] == "source_candidate_insertion"
    assert report["categories"][0]["category"] == "source_candidate_insertion"
    assert report["optimization_candidates"][0]["target"] == (
        "gaia_source_direct_fact_materialization"
    )
    assert "measured and rejected" in report["optimization_candidates"][0][
        "next_experiment"
    ]
    assert report["optimization_candidates"][1]["target"] == (
        "global_authority_selection"
    )
    assert any(
        row["target"] == "artifact_hashing_readback"
        for row in report["optimization_candidates"]
    )
    assert report["peak_spill_allocated_bytes"] == 300
