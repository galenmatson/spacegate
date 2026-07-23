from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "srv" / "api"))

from app.main import (  # noqa: E402
    _is_planetary_orbit_relation,
    _overlay_stellar_leaf_classifications,
    _planet_orbit_solutions_by_stable_key,
    _stellar_leaf_classification_lookup,
)


class StellarLeafOverlayTests(unittest.TestCase):
    def test_planet_orbit_lookup_accepts_legacy_and_evidence_lake_tokens(self) -> None:
        arm = {
            "orbit_edges": {
                "items": [
                    {
                        "orbit_edge_id": 1,
                        "relation_kind": "planetary_orbit",
                        "secondary_component_key": "comp:planet:legacy:planet",
                    },
                    {
                        "orbit_edge_id": 2,
                        "relation_kind": "planet",
                        "secondary_component_key": "comp:planet:canon:planet:nasa_source:4",
                    },
                    {
                        "orbit_edge_id": 3,
                        "relation_kind": "satellite",
                        "secondary_component_key": "comp:planet:not-a-canonical-planet",
                    },
                ]
            },
            "orbital_solutions": {
                "items": [
                    {"orbit_edge_id": 1, "solution_rank": 1, "period_days": 10.0},
                    {"orbit_edge_id": 2, "solution_rank": 1, "period_days": 365.25},
                    {"orbit_edge_id": 3, "solution_rank": 1, "period_days": 27.0},
                ]
            },
        }

        lookup = _planet_orbit_solutions_by_stable_key(arm)

        self.assertEqual(lookup["legacy:planet"]["period_days"], 10.0)
        self.assertEqual(lookup["canon:planet:nasa_source:4"]["period_days"], 365.25)
        self.assertNotIn("not-a-canonical-planet", lookup)

    def test_planetary_relation_vocabulary_is_bounded(self) -> None:
        self.assertTrue(_is_planetary_orbit_relation("planetary_orbit"))
        self.assertTrue(_is_planetary_orbit_relation("planet"))
        self.assertFalse(_is_planetary_orbit_relation("satellite"))
        self.assertFalse(_is_planetary_orbit_relation("source_elementary_binary"))

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
            "selected_fact_id": "fact:test",
        }

        _overlay_stellar_leaf_classifications(hierarchy, [row])

        leaf = hierarchy["root"]["children"][0]
        self.assertEqual(leaf["stellar_leaf_classification"], row)
        self.assertEqual(leaf["quick_facts"]["stellar_leaf_display_class"], "M")
        self.assertEqual(leaf["quick_facts"]["stellar_leaf_display_class_status"], "derived")
        self.assertEqual(leaf["quick_facts"]["stellar_leaf_display_class_fact_id"], "fact:test")

    def test_overlay_adapts_selected_source_and_mass_evidence(self) -> None:
        hierarchy = {
            "root": {
                "stable_component_key": "root",
                "children": [
                    {"stable_component_key": "leaf:source", "children": []},
                    {"stable_component_key": "leaf:mass", "children": []},
                ],
            }
        }
        rows = [
            {
                "hierarchy_node_key": "leaf:source",
                "classification_value": "A",
                "classification_status": "source",
                "evidence_basis": "selected_msc_component_spectral_type",
                "source_value": "A1V",
                "selected_fact_id": "fact:source",
            },
            {
                "hierarchy_node_key": "leaf:mass",
                "classification_value": "M",
                "classification_status": "assumed",
                "evidence_basis": "selected_msc_component_mass_main_sequence_prior",
                "source_value": "0.39",
                "selected_fact_id": "fact:mass",
            },
        ]

        _overlay_stellar_leaf_classifications(hierarchy, rows)

        source_facts = hierarchy["root"]["children"][0]["quick_facts"]
        self.assertEqual(source_facts["spectral_type_raw"], "A1V")
        self.assertEqual(source_facts["spectral_class"], "A")
        mass_facts = hierarchy["root"]["children"][1]["quick_facts"]
        self.assertEqual(mass_facts["mass_msun"], 0.39)
        self.assertEqual(mass_facts["visual_stellar_class"], "M")
        self.assertEqual(mass_facts["visual_stellar_class_status"], "assumed")


if __name__ == "__main__":
    unittest.main()
