from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.materialize_stellar_leaf_classifications import materialize


class StellarLeafClassificationTests(unittest.TestCase):
    def test_materialization_uses_exact_leaves_and_evidence_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            core_path = root / "core.duckdb"
            hierarchy_path = root / "hierarchy.duckdb"
            arm_path = root / "arm.duckdb"

            core = duckdb.connect(str(core_path))
            core.execute(
                """
                create table systems (
                  system_id bigint, stable_object_key varchar, wds_id varchar
                );
                insert into systems values (1, 'canon:system:test', 'TEST');
                create table stars (
                  system_id bigint, star_id bigint, stable_object_key varchar,
                  star_name varchar, component varchar, spectral_type_raw varchar,
                  spectral_class varchar, object_type varchar, source_catalog varchar,
                  source_pk varchar
                );
                insert into stars values
                  (1, 10, 'canon:star:parent', 'Parent', 'A', 'G2V', 'G', 'star', 'TEST_CORE', '10'),
                  (1, 11, 'canon:star:a', 'Leaf A', 'Aa', 'K4V', 'K', 'star', 'TEST_CORE', '11'),
                  (1, 12, 'canon:star:e', 'Leaf E', 'E', null, null, 'star', 'TEST_CORE', '12');
                """
            )
            core.close()

            hierarchy = duckdb.connect(str(hierarchy_path))
            hierarchy.execute(
                """
                create table hierarchy_nodes (
                  hierarchy_node_key varchar, canonical_key varchar, node_kind varchar,
                  component_family varchar, component_type varchar, source_basis varchar,
                  wds_id varchar, display_name varchar
                );
                insert into hierarchy_nodes values
                  ('hier:test:parent', 'canon:star:parent', 'canonical_star', 'star', 'star', 'core', 'TEST', 'Parent'),
                  ('hier:test:a', 'canon:star:a', 'canonical_star', 'star', 'star', 'core', 'TEST', 'Leaf A'),
                  ('hier:test:e', 'canon:star:e', 'canonical_star', 'star', 'star', 'core', 'TEST', 'Leaf E'),
                  ('hier:wds:TEST:leaf:b', 'inferred:b', 'inferred_star_leaf', 'star', 'star', 'msc', 'TEST', 'Leaf B'),
                  ('hier:wds:TEST:leaf:c', 'inferred:c', 'inferred_star_leaf', 'star', 'star', 'msc', 'TEST', 'Leaf C'),
                  ('hier:wds:TEST:leaf:d', 'inferred:d', 'inferred_star_leaf', 'star', 'brown_dwarf', 'msc', 'TEST', 'Candidate D'),
                  ('hier:test:planet', 'canon:planet:test', 'canonical_planet', 'planet', 'planet', 'core', null, 'Planet');
                create table hierarchy_edges (parent_node_key varchar, child_node_key varchar);
                insert into hierarchy_edges values
                  ('hier:test:parent', 'hier:test:a'),
                  ('hier:test:e', 'hier:test:planet');
                """
            )
            hierarchy.close()

            arm = duckdb.connect(str(arm_path))
            arm.execute(
                """
                create table msc_system_details (
                  primary_component_key varchar, secondary_component_key varchar,
                  spectral_type_primary varchar, spectral_type_secondary varchar,
                  mass_primary_msun double, mass_secondary_msun double,
                  source_catalog varchar, source_version varchar, source_pk varchar,
                  retrieval_checksum varchar, retrieved_at varchar
                );
                insert into msc_system_details values
                  ('comp:msc:wds:TEST:b', 'comp:msc:wds:TEST:c', 'M3V', null, 0.3, 0.5,
                   'TEST_MSC', '1', 'row-1', 'sha256:test', '2026-01-01T00:00:00Z');
                create table derived_stellar_classifications (
                  stable_component_key varchar, classification_key varchar,
                  classification_value varchar, classification_status varchar,
                  review_status varchar, derivation_method varchar, source_catalog varchar,
                  source_version varchar, source_pk varchar, retrieval_checksum varchar,
                  retrieved_at varchar, input_parameters_json varchar, confidence_score double
                );
                """
            )
            arm.close()

            report = materialize(
                core_db=core_path,
                arm_db=arm_path,
                hierarchy_db=hierarchy_path,
                build_id="test-build",
            )
            self.assertEqual(report["status"], "pass")

            result = duckdb.connect(str(arm_path), read_only=True)
            rows = result.execute(
                """
                select hierarchy_node_key, classification_value, classification_status
                from stellar_leaf_display_classifications order by hierarchy_node_key
                """
            ).fetchall()
            result.close()

            self.assertEqual(
                rows,
                [
                    ("hier:test:a", "K", "source"),
                    ("hier:test:e", "UNKNOWN", "missing"),
                    ("hier:wds:TEST:leaf:b", "M", "derived"),
                    ("hier:wds:TEST:leaf:c", "M", "assumed"),
                ],
            )

    def test_e6_projection_replaces_legacy_core_and_component_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            core_path = root / "core.duckdb"
            hierarchy_path = root / "hierarchy.duckdb"
            arm_path = root / "arm.duckdb"

            core = duckdb.connect(str(core_path))
            core.execute(
                """
                create table systems (
                  system_id bigint, stable_object_key varchar, wds_id varchar
                );
                insert into systems values (1, 'canon:system:wds:TEST', 'TEST');
                create table stars (
                  system_id bigint, star_id bigint, stable_object_key varchar,
                  star_name varchar, component varchar, spectral_type_raw varchar,
                  spectral_class varchar, object_type varchar, source_catalog varchar,
                  source_pk varchar
                );
                insert into stars values
                  (1, 10, 'canon:star:a', 'Leaf A', 'A', 'K4V', 'K', 'star',
                   'gaia_dr3', '10');
                """
            )
            core.close()

            hierarchy = duckdb.connect(str(hierarchy_path))
            hierarchy.execute(
                """
                create table hierarchy_nodes (
                  hierarchy_node_key varchar, canonical_key varchar, node_kind varchar,
                  component_family varchar, component_type varchar, source_basis varchar,
                  wds_id varchar, display_name varchar
                );
                insert into hierarchy_nodes values
                  ('hier:test:a', 'canon:star:a', 'canonical_star', 'star', 'star',
                   'core', 'TEST', 'Leaf A'),
                  ('hier:wds:TEST:leaf:b', 'inferred:b', 'inferred_star_leaf',
                   'star', 'star', 'msc', 'TEST', 'Leaf B');
                create table hierarchy_edges (
                  parent_node_key varchar, child_node_key varchar
                );
                """
            )
            hierarchy.close()

            arm = duckdb.connect(str(arm_path))
            arm.execute(
                """
                create table msc_system_details (
                  primary_component_key varchar, secondary_component_key varchar,
                  spectral_type_primary varchar, spectral_type_secondary varchar,
                  mass_primary_msun double, mass_secondary_msun double,
                  source_catalog varchar, source_version varchar, source_pk varchar,
                  retrieval_checksum varchar, retrieved_at varchar
                );
                create table derived_stellar_classifications (
                  stable_component_key varchar, classification_key varchar,
                  classification_value varchar, classification_status varchar,
                  review_status varchar, derivation_method varchar, source_catalog varchar,
                  source_version varchar, source_pk varchar, retrieval_checksum varchar,
                  retrieved_at varchar, input_parameters_json varchar, confidence_score double
                );
                create table e6_selected_stellar_display_classifications (
                  selected_display_classification_id bigint,star_id bigint,
                  classification_value varchar,classification_status varchar,
                  evidence_basis varchar,selected_fact_id varchar,projection_version varchar,
                  source_value varchar,confidence_score double
                );
                insert into e6_selected_stellar_display_classifications values
                  (1,10,'G','source','selected_spectral_type_simbad','fact-core',
                   'e6_selected_consumer_projection_v1','G2 V',0.94);
                create table e6_component_msc_component_entities (
                  component_entity_id varchar,source_id varchar,release_id varchar,
                  canonical_system_stable_object_key varchar,
                  component_label_normalized varchar,binding_status varchar
                );
                insert into e6_component_msc_component_entities values
                  ('entity-b','multiplicity.msc','release-1',
                   'canon:system:wds:TEST','b','accepted');
                create table e6_component_msc_classification_projection (
                  component_entity_id varchar,evidence_id varchar,
                  classification_raw varchar,classification_normalized varchar,
                  projection_status varchar
                );
                insert into e6_component_msc_classification_projection values
                  ('entity-b','fact-component','M4 V',null,
                   'eligible_for_quantity_selection');
                create table e6_component_msc_stellar_parameter_projection (
                  component_entity_id varchar,evidence_id varchar,quantity_key varchar,
                  normalized_value double,value_raw varchar,projection_status varchar
                );
                """
            )
            arm.close()

            report = materialize(
                core_db=core_path,
                arm_db=arm_path,
                hierarchy_db=hierarchy_path,
                build_id="test-e6-build",
            )
            self.assertEqual(report["status"], "pass")

            result = duckdb.connect(str(arm_path), read_only=True)
            rows = result.execute(
                """
                select hierarchy_node_key,classification_value,evidence_basis,
                       selected_fact_id
                from stellar_leaf_display_classifications
                order by hierarchy_node_key
                """
            ).fetchall()
            result.close()
            self.assertEqual(
                rows,
                [
                    (
                        "hier:test:a",
                        "G",
                        "selected_spectral_type_simbad",
                        "fact-core",
                    ),
                    (
                        "hier:wds:TEST:leaf:b",
                        "M",
                        "e6_msc_component_spectral_type_v1",
                        "fact-component",
                    ),
                ],
            )


if __name__ == "__main__":
    unittest.main()
