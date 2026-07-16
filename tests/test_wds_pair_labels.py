import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from cook_multiplicity import parse_wds_pair_label


def test_wds_pair_label_preserves_component_scope() -> None:
    assert parse_wds_pair_label("AB") == ("A", "B", "implicit_single_character_pair")
    assert parse_wds_pair_label("Aa,Ab") == ("Aa", "Ab", "explicit_pair")
    assert parse_wds_pair_label("AB,C") == ("AB", "C", "explicit_pair")


def test_wds_pair_label_accounts_for_unusable_rows() -> None:
    assert parse_wds_pair_label("") == (None, None, "unspecified")
    assert parse_wds_pair_label("A-B") == (None, None, "unsupported_label")
