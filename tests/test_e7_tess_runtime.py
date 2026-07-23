from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_tess_runtime", ROOT / "scripts/compile_e7_tess_runtime.py"
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_policy_is_bounded_and_fail_closed() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_tess_runtime.json")
    COMPILER.validate_policy(policy)
    assert policy["rules"]["open_stability_databases"] is False
    assert policy["rules"]["trust_legacy_numeric_ids"] is False
    assert policy["rules"]["candidate_or_negative_rows_link_canonical_planets"] is False
    assert policy["rules"]["mutate_canonical_planet_inventory"] is False


def test_policy_accounts_complete_tess_partitions() -> None:
    policy = json.loads(
        (ROOT / "config/evidence_lake/e7_tess_runtime.json").read_text(encoding="utf-8")
    )
    acceptance = policy["acceptance"]
    assert sum(acceptance[key] for key in (
        "targets_accepted", "targets_ambiguous", "targets_excluded",
        "targets_missing", "targets_source_missing",
    )) == acceptance["targeted_tics"]
    assert sum(acceptance[key] for key in (
        "toi_confirmed_known", "toi_candidates", "toi_negative", "toi_unclassified",
    )) == acceptance["tois"]
    assert acceptance["canonical_inventory_mutations"] == 0


def test_compiler_has_no_stability_database_or_named_object_authority() -> None:
    source = (ROOT / "scripts/compile_e7_tess_runtime.py").read_text(encoding="utf-8").lower()
    assert "20260717t0614z_f452835_side" not in source
    assert "stability_reference" not in source
    assert "castor" not in source
    assert "sirius" not in source


def test_policy_rejects_candidate_planet_linking() -> None:
    policy = COMPILER.load_object(ROOT / "config/evidence_lake/e7_tess_runtime.json")
    policy["rules"]["candidate_or_negative_rows_link_canonical_planets"] = True
    try:
        COMPILER.validate_policy(policy)
    except ValueError as exc:
        assert "unsafe E7 TESS runtime rules" in str(exc)
    else:
        raise AssertionError("unsafe candidate-link policy was accepted")
