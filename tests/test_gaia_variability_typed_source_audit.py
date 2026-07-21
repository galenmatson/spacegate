from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from audit_gaia_variability_typed_source import parse_vector  # noqa: E402


def test_parse_vector_preserves_gaia_mask_positions() -> None:
    values, invalid = parse_vector("[1.25 --\n 3.5]")
    assert invalid is False
    assert values == [1.25, None, 3.5]


def test_parse_vector_rejects_nonfinite_or_unbracketed_values() -> None:
    assert parse_vector("1.0 2.0")[1] is True
    assert parse_vector("[1.0 NaN]")[1] is True
