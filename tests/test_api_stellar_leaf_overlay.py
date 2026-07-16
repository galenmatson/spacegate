from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "srv" / "api"))

from app.main import _overlay_stellar_leaf_classifications  # noqa: E402


class StellarLeafOverlayTests(unittest.TestCase):
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
