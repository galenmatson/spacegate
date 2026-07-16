from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "srv" / "api"))

from app.main import (  # noqa: E402
    _overlay_stellar_leaf_classifications,
    _stellar_leaf_classification_lookup,
)


class StellarLeafOverlayTests(unittest.TestCase):
    def test_lookup_accepts_canonical_and_evidence_component_keys(self) -> None:
        row = {
            "hierarchy_node_key": "canon:star:test",
            "leaf_component_key": "comp:star:canon:star:test",
            "evidence_component_key": "comp:msc:wds:TEST:a",
        }

        lookup = _stellar_leaf_classification_lookup([row])

        self.assertIs(lookup["canon:star:test"], row)
        self.assertIs(lookup["comp:star:canon:star:test"], row)
        self.assertIs(lookup["comp:msc:wds:TEST:a"], row)

    def test_lookup_drops_ambiguous_aliases(self) -> None:
        rows = [
            {"hierarchy_node_key": "leaf:a", "evidence_component_key": "shared"},
            {"hierarchy_node_key": "leaf:b", "evidence_component_key": "shared"},
        ]

        lookup = _stellar_leaf_classification_lookup(rows)

        self.assertNotIn("shared", lookup)
        self.assertEqual(lookup["leaf:a"], rows[0])
        self.assertEqual(lookup["leaf:b"], rows[1])

    def test_null_quick_facts_are_normalized_before_overlay(self) -> None:
        hierarchy = {
            "root": {
                "stable_component_key": "root",
                "children": [
                    {
                        "stable_component_key": "leaf:a",
                        "quick_facts": None,
                        "children": [],
                    }
                ],
            }
        }
        row = {
            "hierarchy_node_key": "leaf:a",
            "classification_value": "M",
            "classification_status": "derived",
            "evidence_basis": "test",
        }

        _overlay_stellar_leaf_classifications(hierarchy, [row])

        leaf = hierarchy["root"]["children"][0]
        self.assertEqual(leaf["stellar_leaf_classification"], row)
        self.assertEqual(leaf["quick_facts"]["stellar_leaf_display_class"], "M")
        self.assertEqual(leaf["quick_facts"]["stellar_leaf_display_class_status"], "derived")


if __name__ == "__main__":
    unittest.main()
