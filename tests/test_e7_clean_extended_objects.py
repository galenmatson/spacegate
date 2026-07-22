from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_e7_clean_extended_objects as compiler  # noqa: E402


def policy() -> dict:
    return json.loads(compiler.DEFAULT_POLICY.read_text(encoding="utf-8"))


def test_clean_extended_policy_is_fail_closed() -> None:
    value = policy()
    compiler.validate_policy(value)
    assert value["rules"]["open_stability_databases"] is False
    assert value["rules"]["identity_seed_is_scientific_authority"] is False
    assert value["rules"]["promote_unselected_cluster_distance"] is False
    assert value["rules"]["promote_selected_cluster_distance"] is True
    assert value["rules"]["promote_associated_star_distance_without_selected_binding"] is False
    assert value["rules"]["promote_associated_star_distance_with_selected_binding"] is True


def test_clean_extended_policy_rejects_unselected_distance() -> None:
    value = copy.deepcopy(policy())
    value["rules"]["promote_unselected_cluster_distance"] = True
    with pytest.raises(ValueError, match="unsafe clean extended-object"):
        compiler.validate_policy(value)


def test_clean_extended_policy_rejects_unbound_relation_distance() -> None:
    value = copy.deepcopy(policy())
    value["rules"]["promote_associated_star_distance_without_selected_binding"] = True
    with pytest.raises(ValueError, match="unsafe clean extended-object"):
        compiler.validate_policy(value)


def test_coordinate_helpers() -> None:
    assert compiler.hms("12", "0", "0") == 180.0
    assert compiler.dms("-", "30", "0") == -30.0
    assert compiler.angular_size("3.5x2.5") == (3.5, 2.5)


def test_cluster_coordinate_normalization_wraps_right_ascension() -> None:
    normalized = compiler.normalized_cluster_context(
        "clusters.cantat_gaudin_2020", {"RAdeg": "-0.387", "DEdeg": "61.21"},
    )
    assert normalized["ra_deg"] == pytest.approx(359.613)
    assert normalized["dec_deg"] == 61.21
