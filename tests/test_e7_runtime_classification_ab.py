from __future__ import annotations

from pathlib import Path
import sys

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_e7_runtime_classification_ab as audit  # noqa: E402


LEAF_SCHEMA = """
CREATE TABLE stellar_leaf_display_classifications(
  hierarchy_node_key VARCHAR, classification_value VARCHAR,
  classification_status VARCHAR, evidence_basis VARCHAR,
  source_catalog VARCHAR, source_version VARCHAR, source_pk VARCHAR,
  display_name VARCHAR, hierarchy_source_basis VARCHAR, node_kind VARCHAR
)
"""


def make_database(path: Path, *, candidate: bool) -> None:
    con = duckdb.connect(str(path))
    con.execute(LEAF_SCHEMA)
    if candidate:
        con.execute(
            """
            CREATE TABLE msc_runtime_leaf_bindings(
              wds_id_raw VARCHAR,component_label_raw VARCHAR,
              runtime_binding_status VARCHAR,runtime_binding_reason VARCHAR
            );
            INSERT INTO msc_runtime_leaf_bindings VALUES
              ('00001+0001','AB','ambiguous','case_significant_source_collision'),
              ('00001+0001','Ab','ambiguous','case_significant_source_collision');
            INSERT INTO stellar_leaf_display_classifications VALUES
              ('canon:leaf:msc:00001+0001:ab','UNKNOWN','missing',
               'no_selected_leaf_classification',NULL,NULL,NULL,'collision',
               'canonical_hierarchy','star_leaf'),
              ('canon:star:stable:star:ultracoolsheet:1','UNKNOWN','missing',
               'no_selected_leaf_classification',NULL,NULL,NULL,'ultracool',
               'canonical_hierarchy','star_leaf'),
              ('canon:leaf:msc:00002+0002:aa','L','assumed',
               'selected_msc_component_mass_main_sequence_prior','multiplicity.msc',
               'msc-v1','fact-1','new leaf','msc_inferred_leaf','inferred_star_leaf');
            """
        )
    else:
        con.execute(
            """
            INSERT INTO stellar_leaf_display_classifications VALUES
              ('canon:leaf:msc:00001+0001:ab','M','assumed',
               'mass_main_sequence_prior_v1','msc','legacy','legacy-1','collision',
               'canonical_hierarchy','star_leaf'),
              ('canon:star:stable:star:ultracoolsheet:1','T','source',
               'core_leaf_source_class_v1','ultracoolsheet',NULL,'legacy-2','ultracool',
               'canonical_hierarchy','star_leaf');
            """
        )
    con.close()


def test_explicit_deferrals_and_clean_inferred_leaf_are_accounted(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate.duckdb"
    reference = tmp_path / "reference.duckdb"
    make_database(candidate, candidate=True)
    make_database(reference, candidate=False)

    report = audit.audit(
        candidate_arm=candidate, reference_arm=reference, build_id="test-build"
    )

    assert report["status"] == "pass"
    assert report["checks"] == {
        "duplicate_candidate_leaf_keys": 0,
        "reference_only_leaves": 0,
        "unaccounted_candidate_only_leaves": 0,
        "unaccounted_known_to_unknown": 0,
        "gaia_white_dwarf_known_to_unknown": 0,
        "nonmissing_candidate_without_lineage": 0,
    }
    assert report["classification_transitions"]["known_to_unknown"] == 2


def test_unexplained_regression_fails(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate.duckdb"
    reference = tmp_path / "reference.duckdb"
    make_database(candidate, candidate=True)
    make_database(reference, candidate=False)
    con = duckdb.connect(str(candidate))
    con.execute(
        """
        INSERT INTO stellar_leaf_display_classifications VALUES
          ('canon:star:gaia:1','UNKNOWN','missing','no_selected_leaf_classification',
           NULL,NULL,NULL,'bad','canonical_hierarchy','star_leaf')
        """
    )
    con.close()
    con = duckdb.connect(str(reference))
    con.execute(
        """
        INSERT INTO stellar_leaf_display_classifications VALUES
          ('canon:star:gaia:1','WD','source','core_leaf_source_class_v1',
           'gaia_dr3','dr3','1','bad','canonical_hierarchy','star_leaf')
        """
    )
    con.close()

    report = audit.audit(
        candidate_arm=candidate, reference_arm=reference, build_id="test-build"
    )

    assert report["status"] == "fail"
    assert report["checks"]["unaccounted_known_to_unknown"] == 1
    assert report["checks"]["gaia_white_dwarf_known_to_unknown"] == 1
