from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv" / "api"))

from app.stellar_classification import (  # noqa: E402
    spectral_class_from_type,
    spectral_type_indicates_white_dwarf,
)


def test_lowercase_luminosity_prefixes_are_not_white_dwarfs() -> None:
    for raw, expected in (
        ("dM1e", "M"),
        ("sdK7", "K"),
        ("esdM3", "M"),
        ("usdL0", "L"),
    ):
        assert spectral_class_from_type(raw) == expected
        assert spectral_type_indicates_white_dwarf(raw) is False


def test_white_dwarf_notation_remains_compact_object_evidence() -> None:
    for raw in ("WD?", "DA2", "DB", "D7"):
        assert spectral_class_from_type(raw) == "D"
        assert spectral_type_indicates_white_dwarf(raw) is True


def test_standard_and_wolf_rayet_spectra_are_preserved() -> None:
    assert spectral_class_from_type("A1V") == "A"
    assert spectral_class_from_type("M0.5V") == "M"
    assert spectral_class_from_type("WN6") == "WR"
