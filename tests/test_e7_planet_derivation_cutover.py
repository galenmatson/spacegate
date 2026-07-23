from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "audit_e7_planet_derivation_cutover",
    ROOT / "scripts/audit_e7_planet_derivation_cutover.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_cutover_audit_is_domain_bounded_and_has_no_named_goldens() -> None:
    assert MODULE.DERIVED_QUANTITIES == (
        "semi_major_axis_au",
        "insol_earth",
        "eq_temp_k",
    )
    source = (ROOT / "scripts/audit_e7_planet_derivation_cutover.py").read_text(
        encoding="utf-8"
    ).lower()
    for forbidden in ("castor", "sirius", "nu sco", "trappist"):
        assert forbidden not in source
