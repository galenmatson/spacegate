from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "audit_stellar_leaf_parameter_projection.py"
)
SPEC = importlib.util.spec_from_file_location(
    "audit_stellar_leaf_parameter_projection", SCRIPT
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_legacy_msc_mass_leaf_accounting(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    con = duckdb.connect(str(baseline))
    con.execute(
        """
        CREATE TABLE stellar_leaf_display_classifications(
          hierarchy_node_key VARCHAR,evidence_basis VARCHAR
        );
        INSERT INTO stellar_leaf_display_classifications VALUES
          ('leaf:a','selected_msc_component_mass_main_sequence_prior'),
          ('leaf:b','selected_msc_component_mass_main_sequence_prior');
        """
    )
    con.close()

    con = duckdb.connect(str(candidate))
    con.execute(
        """
        CREATE TABLE stellar_leaf_parameter_evidence(
          hierarchy_node_key VARCHAR,source_id VARCHAR,quantity_key VARCHAR,
          msc_mass_code VARCHAR,mass_method_class VARCHAR,
          applicability_decision VARCHAR
        );
        INSERT INTO stellar_leaf_parameter_evidence VALUES
          ('leaf:a','multiplicity.msc','mass_msun','v',
           'calibrated_source_model','accepted_exact_leaf_calibrated_estimate'),
          ('leaf:b','multiplicity.msc','mass_msun','m',
           'source_minimum_mass_bound','excluded_minimum_mass_is_lower_bound_only');

        CREATE TABLE stellar_leaf_parameter_binding_outcomes(
          hierarchy_node_key VARCHAR,source_id VARCHAR,quantity_key VARCHAR
        );
        INSERT INTO stellar_leaf_parameter_binding_outcomes VALUES
          ('leaf:a','multiplicity.msc','mass'),
          ('leaf:b','multiplicity.msc','mass');

        CREATE TABLE stellar_leaf_selected_parameters(
          hierarchy_node_key VARCHAR,quantity_key VARCHAR,selection_status VARCHAR
        );
        INSERT INTO stellar_leaf_selected_parameters VALUES
          ('leaf:a','mass_msun','accepted'),
          ('leaf:b','mass_msun','missing');

        CREATE TABLE stellar_leaf_display_classifications(
          hierarchy_node_key VARCHAR,classification_status VARCHAR,
          evidence_basis VARCHAR
        );
        INSERT INTO stellar_leaf_display_classifications VALUES
          ('leaf:a','assumed','selected_leaf_mass_main_sequence_prior'),
          ('leaf:b','missing','no_selected_leaf_classification');
        """
    )
    con.close()

    report = MODULE.audit(baseline, candidate, expected=2)
    assert report["status"] == "pass"
    assert report["binding_accounted_leaf_count"] == 2
    assert report["evidence_accounted_leaf_count"] == 2
    assert report["selection_outcomes"] == {"accepted": 1, "missing": 1}

