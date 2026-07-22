from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_selected_compact_evidence as compact  # noqa: E402


def test_atnf_stable_keys_preserve_coordinate_sign() -> None:
    assert compact.atnf_stable_key("J1851+0241") != compact.atnf_stable_key("J1851-0241")
    assert compact.atnf_stable_key(" J0437-4715 ") == "compact:atnf:name:j0437-4715"


def test_atnf_catalog_last_digit_uncertainties() -> None:
    assert compact.atnf_error_scale("6.43", "4") == pytest.approx(0.04)
    assert compact.atnf_error_scale("4.2", "14") == pytest.approx(1.4)
    assert compact.atnf_error_scale("1.23E-4", "5") == pytest.approx(0.000005)
    assert compact.atnf_error_scale("1.23", "0.04") == pytest.approx(0.04)


def test_sexagesimal_coordinate_normalization() -> None:
    assert compact.sexagesimal_ra_deg("04:37:15.89") == pytest.approx(69.31620833333333)
    assert compact.sexagesimal_ra_deg("18 09 51.08696") == pytest.approx(272.4628623333333)
    assert compact.sexagesimal_dec_deg("-47:15:09.1") == pytest.approx(-47.25252777777778)
    assert compact.sexagesimal_dec_deg("+21 53 47.7864") == pytest.approx(21.896607333333332)


def test_mcgill_identity_strips_only_trailing_footnote_markers() -> None:
    assert compact.normalize_mcgill_name("PSR J1846-0258 ##") == "PSR J1846-0258"
    assert compact.mcgill_stable_key("PSR J1846-0258 ##") == compact.mcgill_stable_key("PSR J1846-0258")
