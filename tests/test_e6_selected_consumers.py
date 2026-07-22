from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import materialize_e6_selected_consumers as consumers  # noqa: E402


def _quantity_columns(names: list[str]) -> str:
    return ",".join(
        f"{name} DOUBLE,{name}_lower DOUBLE,{name}_upper DOUBLE,{name}_fact_id VARCHAR"
        for name in names
    )


def test_selected_display_classification_uses_shared_general_precedence(
    tmp_path: Path,
) -> None:
    core_db = tmp_path / "core.duckdb"
    arm_db = tmp_path / "arm.duckdb"

    con = duckdb.connect(str(core_db))
    con.execute(
        """
        CREATE TABLE stars(
          star_id BIGINT,system_id BIGINT,stable_object_key VARCHAR,
          object_type VARCHAR,spectral_type_raw VARCHAR,spectral_class VARCHAR,
          source_catalog VARCHAR
        );
        INSERT INTO stars VALUES
          (1,1,'star:direct','star',NULL,NULL,'gaia_dr3'),
          (2,2,'star:gaia-generated','star','K','K','gaia_dr3'),
          (3,3,'star:source-fallback','star','F8 V','F','legacy_catalog'),
          (4,4,'star:teff','star',NULL,NULL,'gaia_dr3'),
          (5,5,'star:colour','star',NULL,NULL,'gaia_dr3'),
          (6,6,'star:mass','star',NULL,NULL,'gaia_dr3'),
          (7,7,'star:compact','white_dwarf',NULL,NULL,'gaia_dr3'),
          (8,8,'star:unknown','star',NULL,NULL,'gaia_dr3');
        """
    )
    con.close()

    con = duckdb.connect(str(arm_db))
    con.execute(
        "CREATE TABLE e6_selected_stellar_astrometry("
        "star_id BIGINT,system_id BIGINT,stable_object_key VARCHAR,"
        "ra_error_mas DOUBLE,dec_error_mas DOUBLE,"
        "radial_velocity_km_s DOUBLE,gaia_non_single_star_status VARCHAR)"
    )
    con.execute(
        "CREATE TABLE e6_selected_stellar_physics(star_id BIGINT," +
        _quantity_columns(
            [
                "teff_k", "logg_cgs", "metallicity_m_h", "radius_rsun",
                "mass_msun", "luminosity_lsun", "luminosity_log10_lsun",
                "density_g_cm3", "age_gyr",
                "distance_geometric_pc", "distance_photogeometric_pc",
                "rotation_period_days",
            ]
        ) + ")"
    )
    con.execute(
        "CREATE TABLE e6_selected_stellar_photometry("
        "star_id BIGINT,gaia_g_mag DOUBLE,gaia_bp_mag DOUBLE,gaia_rp_mag DOUBLE,"
        "gaia_bp_rp_mag DOUBLE,gaia_bp_rp_mag_fact_id VARCHAR)"
    )
    con.execute(
        """
        CREATE TABLE e6_selected_stellar_classification(
          star_id BIGINT,spectral_type_optical VARCHAR,
          spectral_type_optical_fact_id VARCHAR,spectral_type_infrared VARCHAR,
          spectral_type_infrared_fact_id VARCHAR,spectral_type_simbad VARCHAR,
          spectral_type_simbad_fact_id VARCHAR
        )
        """
    )
    con.execute(
        "INSERT INTO e6_selected_stellar_astrometry(star_id,system_id,stable_object_key) "
        "SELECT i,i,'star:' || i::VARCHAR FROM range(1,9) AS rows(i)"
    )
    con.execute(
        "INSERT INTO e6_selected_stellar_physics(star_id,teff_k,teff_k_fact_id,"
        "mass_msun,mass_msun_fact_id,luminosity_log10_lsun,"
        "luminosity_log10_lsun_fact_id,density_g_cm3,density_g_cm3_fact_id) VALUES "
        "(1,3200,'teff-1',NULL,NULL,NULL,NULL,NULL,NULL),"
        "(2,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
        "(3,3200,'teff-3',NULL,NULL,NULL,NULL,NULL,NULL),"
        "(4,5800,'teff-4',NULL,NULL,0.1,'lum-4',1.2,'density-4'),"
        "(5,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
        "(6,NULL,NULL,0.5,'mass-6',NULL,NULL,NULL,NULL),"
        "(7,5000,'teff-7',NULL,NULL,NULL,NULL,NULL,NULL),"
        "(8,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
    )
    con.execute(
        "INSERT INTO e6_selected_stellar_photometry(star_id,gaia_bp_rp_mag,"
        "gaia_bp_rp_mag_fact_id) VALUES "
        "(1,2.0,'colour-1'),(2,3.0,'colour-2'),(3,NULL,NULL),"
        "(4,2.0,'colour-4'),(5,0.5,'colour-5'),(6,NULL,NULL),"
        "(7,1.0,'colour-7'),(8,NULL,NULL)"
    )
    con.execute(
        "INSERT INTO e6_selected_stellar_classification(" 
        "star_id,spectral_type_optical,spectral_type_optical_fact_id) VALUES "
        "(1,'G2 V','spectral-1'),(7,'K0 V','spectral-7')"
    )
    con.close()

    report = consumers.materialize(
        core_db=core_db, arm_db=arm_db, build_id="fixture"
    )
    assert report["status"] == "pass"
    assert report["classification_conflicts"] == 1
    assert report["classification_alternative_disagreements"] == 4
    assert report["phase_timings"]

    con = duckdb.connect(str(arm_db), read_only=True)
    rows = con.execute(
        "SELECT star_id,classification_value,evidence_basis,selected_fact_id,"
        "has_classification_conflict,has_alternative_disagreement "
        "FROM e6_selected_stellar_display_classifications ORDER BY star_id"
    ).fetchall()
    assert rows == [
        (1, "G", "selected_spectral_type_optical", "spectral-1", False, True),
        (2, "L", "selected_bp_rp_visual_class_prior", "colour-2", False, False),
        (3, "F", "stability_core_source_class_fallback", None, False, True),
        (4, "G", "selected_teff_visual_class_prior", "teff-4", False, True),
        (5, "F", "selected_bp_rp_visual_class_prior", "colour-5", False, False),
        (6, "M", "selected_mass_main_sequence_prior", "mass-6", False, False),
        (7, "WD", "canonical_object_type", None, True, True),
        (8, "UNKNOWN", "no_selected_or_fallback_classification", None, False, False),
    ]
    assert con.execute(
        "SELECT lineage_kind,lineage_id FROM "
        "e6_selected_stellar_display_classifications WHERE star_id=7"
    ).fetchone() == ("canonical_core_star", "core:star:7")
    assert con.execute(
        "SELECT teff_k,teff_lo_k,teff_hi_k,luminosity_log10_lsun,density_g_cm3,context_json "
        "FROM e6_selected_stellar_parameters WHERE star_id=4"
    ).fetchone()[:5] == (5800.0, None, None, 0.1, 1.2)
    con.close()
