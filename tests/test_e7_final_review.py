from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "assemble_e7_final_review", ROOT / "scripts/assemble_e7_final_review.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_final_review_config_is_complete_and_candidate_scoped() -> None:
    config = json.loads(
        (ROOT / "config/evidence_lake/e7_final_review.json").read_text(encoding="utf-8")
    )
    MODULE.validate_config(config)
    candidate = config["candidate"]["public_build_id"]
    assert candidate in config["candidate_build_dir"]
    for name in (
        "planet_cutover_ab", "science_reproduction", "core_reproduction",
        "arm_reproduction", "disc_reproduction", "map_verification",
        "scene_cold", "scene_warm",
    ):
        assert Path(config["reports"][name]).is_absolute()


def test_final_review_has_no_named_object_scientific_conditions() -> None:
    source = (ROOT / "scripts/assemble_e7_final_review.py").read_text(
        encoding="utf-8"
    ).lower()
    for forbidden in ("castor", "sirius", "nu sco", "trappist"):
        assert forbidden not in source
    assert "canonical_inventory_tables" in source
    assert "shared_selected_fact_consumer_pass" in source
