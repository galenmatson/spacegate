from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "compile_e7_selected_stellar_classifications",
    ROOT / "scripts/compile_e7_selected_stellar_classifications.py",
)
assert SPEC and SPEC.loader
COMPILER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPILER)


def test_checked_in_policy_is_bounded_and_noncanonical() -> None:
    policy = COMPILER.load_object(COMPILER.DEFAULT_POLICY)
    COMPILER.validate_policy(policy)
    assert policy["gaia_dsc_white_dwarf"]["probability_threshold"] == 0.5
    assert policy["rules"]["create_canonical_identity"] is False
    assert policy["rules"]["create_canonical_containment"] is False


def test_materialize_selects_only_thresholded_exact_gaia_bindings() -> None:
    con = duckdb.connect(":memory:")
    con.execute("ATTACH ':memory:' AS core; ATTACH ':memory:' AS gaia_ap")
    con.execute(
        """
        CREATE TABLE core.object_identifiers(
          target_type VARCHAR,target_id HUGEINT,stable_object_key VARCHAR,
          system_stable_object_key VARCHAR,namespace VARCHAR,id_value_norm VARCHAR
        );
        CREATE TABLE core.stars(star_id HUGEINT,system_id HUGEINT);
        INSERT INTO core.object_identifiers VALUES
          ('star',1,'canon:star:one','canon:system:one','gaia_dr3','101'),
          ('star',2,'canon:star:collision-a','canon:system:collision','gaia_dr3','303'),
          ('star',3,'canon:star:collision-b','canon:system:collision','gaia_dr3','303');
        INSERT INTO core.stars VALUES (1,11),(2,22),(3,22);
        CREATE TABLE gaia_ap.stellar_classification_evidence(
          evidence_id VARCHAR,source_record_id VARCHAR,classification_scheme VARCHAR,
          method VARCHAR,model VARCHAR,reference_raw VARCHAR,quality_json JSON
        );
        CREATE TABLE gaia_ap.identifier_claim_evidence(
          source_record_id VARCHAR,namespace VARCHAR,identifier_raw VARCHAR,
          identifier_normalized VARCHAR
        );
        INSERT INTO gaia_ap.stellar_classification_evidence VALUES
          ('accepted','source-101','gaia_dr3_source_classifier_probability_vectors',
           'gaia_dsc',NULL,'gaia-docs',
           '{"models":{"DSC_combmod":{"white_dwarf":0.91},"DSC_specmod":{"white_dwarf":0.4}}}'),
          ('below','source-202','gaia_dr3_source_classifier_probability_vectors',
           'gaia_dsc',NULL,'gaia-docs',
           '{"models":{"DSC_combmod":{"white_dwarf":0.2},"DSC_specmod":{"white_dwarf":0.3}}}'),
          ('collision','source-303','gaia_dr3_source_classifier_probability_vectors',
           'gaia_dsc',NULL,'gaia-docs',
           '{"models":{"DSC_combmod":{"white_dwarf":0.6},"DSC_specmod":{"white_dwarf":0.8}}}'),
          ('missing','source-404','gaia_dr3_source_classifier_probability_vectors',
           'gaia_dsc',NULL,'gaia-docs',
           '{"models":{"DSC_combmod":{"white_dwarf":0.7},"DSC_specmod":{"white_dwarf":0.1}}}');
        INSERT INTO gaia_ap.identifier_claim_evidence VALUES
          ('source-101','gaia_dr3_source_id','101','101'),
          ('source-202','gaia_dr3_source_id','202','202'),
          ('source-303','gaia_dr3_source_id','303','303'),
          ('source-404','gaia_dr3_source_id','404','404');
        """
    )
    policy = COMPILER.load_object(COMPILER.DEFAULT_POLICY)
    counts = COMPILER.materialize(
        con,
        build_id="test-build",
        contract=policy["gaia_dsc_white_dwarf"],
        source_id="gaia.dr3.astrophysical_parameters",
        release_id="test-release",
        policy_version="test-policy",
    )
    assert counts == {
        "source_records": 4,
        "threshold_candidates": 3,
        "selected_classifications": 1,
    }
    assert con.execute(
        "SELECT binding_status,count(*) FROM stellar_model_classification_bindings "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall() == [("accepted", 1), ("ambiguous", 1), ("missing", 1)]
    assert con.execute(
        "SELECT star_id,classification_value,classification_status,evidence_basis,"
        "confidence_score,model FROM selected_stellar_model_classifications"
    ).fetchone() == (
        1,
        "WD",
        "source_model",
        "selected_gaia_dsc_white_dwarf_probability",
        0.91,
        "DSC_combmod",
    )


def test_policy_rejects_identity_promotion() -> None:
    policy = COMPILER.load_object(COMPILER.DEFAULT_POLICY)
    policy["rules"]["create_canonical_identity"] = True
    try:
        COMPILER.validate_policy(policy)
    except ValueError as error:
        assert "unsafe" in str(error)
    else:
        raise AssertionError("identity promotion must fail closed")
