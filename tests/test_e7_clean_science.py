from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import compile_e7_clean_science as compiler  # noqa: E402


def test_checked_in_clean_science_policy_is_fail_closed() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    compiler.validate_policy(policy)
    assert policy["rules"]["open_stability_databases"] is False
    assert policy["rules"]["copy_stability_scientific_values"] is False
    assert policy["rules"]["allow_core_classification_fallback"] is False


def test_policy_rejects_stability_fallback() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    policy["rules"]["allow_core_classification_fallback"] = True
    with pytest.raises(ValueError, match="unsafe E7 clean science rules"):
        compiler.validate_policy(policy)


def test_white_dwarf_catalog_classification_precedes_visual_proxies() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    con = __import__("duckdb").connect(":memory:")
    con.execute("ATTACH ':memory:' AS core")
    con.execute("ATTACH ':memory:' AS selected")
    con.execute(
        """
        CREATE TABLE core.stars(
          star_id HUGEINT,system_id HUGEINT,stable_object_key VARCHAR
        );
        CREATE TABLE selected_stellar_classification(
          star_id HUGEINT,spectral_type_optical VARCHAR,
          spectral_type_optical_fact_id VARCHAR,spectral_type_infrared VARCHAR,
          spectral_type_infrared_fact_id VARCHAR,spectral_type_simbad VARCHAR,
          spectral_type_simbad_fact_id VARCHAR
        );
        CREATE TABLE selected_stellar_physics(
          star_id HUGEINT,teff_k DOUBLE,teff_k_fact_id VARCHAR,
          mass_msun DOUBLE,mass_msun_fact_id VARCHAR
        );
        CREATE TABLE selected_stellar_photometry(
          star_id HUGEINT,gaia_bp_rp_mag DOUBLE,gaia_bp_rp_mag_fact_id VARCHAR
        );
        CREATE TABLE selected.selected_facts(
          selected_fact_id VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,
          source_id VARCHAR,quantity_key VARCHAR,fact_status VARCHAR
        );
        CREATE TABLE evidence_stellar_model_selected_stellar_model_classifications(
          star_id HUGEINT,selected_fact_id VARCHAR,source_value VARCHAR,
          confidence_score DOUBLE
        );
        INSERT INTO core.stars VALUES
          (1,10,'canon:star:wd'),(2,10,'canon:star:direct');
        INSERT INTO selected_stellar_classification VALUES
          (1,NULL,NULL,NULL,NULL,NULL,NULL),
          (2,'G2V','direct-fact',NULL,NULL,NULL,NULL);
        INSERT INTO selected_stellar_physics VALUES
          (1,12000,'wd-fact',0.6,'wd-mass'),
          (2,12000,'wd-fact-2',0.6,'wd-mass-2');
        INSERT INTO selected_stellar_photometry VALUES
          (1,0.0,'color-1'),(2,0.0,'color-2');
        INSERT INTO selected.selected_facts VALUES
          ('wd-fact','star','canon:star:wd','compact.gaia_edr3_white_dwarf','teff_k','source_selected'),
          ('wd-fact-2','star','canon:star:direct','compact.gaia_edr3_white_dwarf','teff_k','source_selected');
        """
    )

    compiler.materialize_display_classes(
        con,
        "test-build",
        "selected",
        policy["classification_evidence_sources"],
    )

    rows = con.execute(
        "SELECT star_id,classification_value,evidence_basis,has_classification_conflict "
        "FROM selected_stellar_display_classifications ORDER BY star_id"
    ).fetchall()
    assert rows == [
        (1, "WD", "selected_white_dwarf_catalog_applicability", False),
        (2, "G", "selected_spectral_type_optical", True),
    ]


def test_gaia_dsc_model_precedes_visual_proxy_but_not_direct_spectrum() -> None:
    policy = compiler.load_object(compiler.DEFAULT_POLICY)
    con = __import__("duckdb").connect(":memory:")
    con.execute("ATTACH ':memory:' AS core; ATTACH ':memory:' AS selected")
    con.execute(
        """
        CREATE TABLE core.stars(star_id HUGEINT,system_id HUGEINT,stable_object_key VARCHAR);
        CREATE TABLE selected_stellar_classification(
          star_id HUGEINT,spectral_type_optical VARCHAR,spectral_type_optical_fact_id VARCHAR,
          spectral_type_infrared VARCHAR,spectral_type_infrared_fact_id VARCHAR,
          spectral_type_simbad VARCHAR,spectral_type_simbad_fact_id VARCHAR
        );
        CREATE TABLE selected_stellar_physics(
          star_id HUGEINT,teff_k DOUBLE,teff_k_fact_id VARCHAR,
          mass_msun DOUBLE,mass_msun_fact_id VARCHAR
        );
        CREATE TABLE selected_stellar_photometry(
          star_id HUGEINT,gaia_bp_rp_mag DOUBLE,gaia_bp_rp_mag_fact_id VARCHAR
        );
        CREATE TABLE selected.selected_facts(
          selected_fact_id VARCHAR,object_type VARCHAR,stable_object_key VARCHAR,
          source_id VARCHAR,quantity_key VARCHAR,fact_status VARCHAR
        );
        CREATE TABLE evidence_stellar_model_selected_stellar_model_classifications(
          star_id HUGEINT,selected_fact_id VARCHAR,source_value VARCHAR,
          confidence_score DOUBLE
        );
        INSERT INTO core.stars VALUES
          (1,10,'canon:star:model'),(2,20,'canon:star:direct');
        INSERT INTO selected_stellar_classification VALUES
          (1,NULL,NULL,NULL,NULL,NULL,NULL),
          (2,'G2V','direct-g',NULL,NULL,NULL,NULL);
        INSERT INTO selected_stellar_physics VALUES
          (1,11000,'teff-1',NULL,NULL),(2,11000,'teff-2',NULL,NULL);
        INSERT INTO selected_stellar_photometry VALUES
          (1,0.0,'color-1'),(2,0.0,'color-2');
        INSERT INTO evidence_stellar_model_selected_stellar_model_classifications VALUES
          (1,'dsc-1','{"selected_probability":0.91}',0.91),
          (2,'dsc-2','{"selected_probability":0.89}',0.89);
        """
    )

    compiler.materialize_display_classes(
        con, "test-build", "selected", policy["classification_evidence_sources"]
    )

    assert con.execute(
        "SELECT star_id,classification_value,classification_status,evidence_basis "
        "FROM selected_stellar_display_classifications ORDER BY star_id"
    ).fetchall() == [
        (1, "WD", "source_model", "selected_gaia_dsc_white_dwarf_probability"),
        (2, "G", "source", "selected_spectral_type_optical"),
    ]


def test_failed_phase_is_written_incrementally(tmp_path: Path) -> None:
    trace = tmp_path / "trace.json"
    timings = compiler.Timings(trace)

    def fail() -> None:
        raise RuntimeError("test failure")

    with pytest.raises(RuntimeError, match="test failure"):
        timings.run("failure_probe", fail)
    payload = json.loads(trace.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["timing"]["phases"][-1]["phase"] == "failure_probe"
    assert payload["timing"]["phases"][-1]["status"] == "fail"
