from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from audit_gaia_variability_typed_source import field_roles, parse_vector  # noqa: E402


def test_parse_vector_preserves_gaia_mask_positions() -> None:
    values, invalid = parse_vector("[1.25 --\n 3.5]")
    assert invalid is False
    assert values == [1.25, None, 3.5]


def test_parse_vector_rejects_nonfinite_or_unbracketed_values() -> None:
    assert parse_vector("1.0 2.0")[1] is True
    assert parse_vector("[1.0 NaN]")[1] is True


def test_field_roles_exhaustively_partition_variability_columns() -> None:
    columns = [
        {"name": "source_id"},
        {"name": "solution_id"},
        {"name": "num_segments"},
        {"name": "segments_rotation_period"},
        {"name": "best_rotation_period"},
        {"name": "in_vari_rotation_modulation"},
    ]
    roles = field_roles(columns, vector_fields=["segments_rotation_period"])
    assert roles == {
        "identity": ["solution_id", "source_id"],
        "membership_flags": ["in_vari_rotation_modulation"],
        "cardinalities": ["num_segments"],
        "masked_vectors": ["segments_rotation_period"],
        "scalar_solution_fields": ["best_rotation_period"],
    }
    assert sorted(value for values in roles.values() for value in values) == sorted(
        field["name"] for field in columns
    )
