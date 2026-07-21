from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import verify_e5_gaia_ap_supp_selection as verification


def test_checked_in_gaia_ap_supp_policy_separates_selected_channels() -> None:
    policy = verification.compiler.load_json(verification.compiler.DEFAULT_POLICY)
    source = verification.selected_source(policy, verification.SOURCE_ID)
    dispositions = {
        row["channel"]: row["disposition"]
        for row in source["channel_dispositions"]
    }
    assert dispositions["stellar_parameter_evidence.gsp_spec_ann"] == "selected"
    assert dispositions[
        "stellar_parameter_evidence.gsp_phot_library_alternatives"
    ] == "evidence_only"
    assert dispositions[
        "astrometry_distance_evidence.gsp_phot_library_alternatives"
    ] == "evidence_only"


def test_gaia_ap_supp_uses_official_ann_quality_and_flame_fallback_ranks() -> None:
    policy = verification.compiler.load_json(verification.compiler.DEFAULT_POLICY)
    source = verification.selected_source(policy, verification.SOURCE_ID)
    groups = {row["group_key"]: row for row in source["quantity_groups"]}
    ann = groups["stellar_atmosphere"]["authorities"][0]
    assert ann["method"] == "gaia_dr3_gspspec_ann"
    assert ann["rank"] == 25
    assert ann["quality_conditions"] == [
        {
            "scope": "source_context",
            "path": "$.flags_gspspec_ann",
            "operator": "lt",
            "value": 10000,
        }
    ]
    flame = groups["stellar_fundamental"]["authorities"][0]
    assert flame["rank"] == 15
    assert flame["model"] == "FLAME_spectroscopic"
