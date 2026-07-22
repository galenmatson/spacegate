from __future__ import annotations

import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import score_coolness as scorer  # noqa: E402


def test_coolness_prefers_selected_classification_and_luminosity(tmp_path: Path) -> None:
    core_path = tmp_path / "core.duckdb"
    arm_path = tmp_path / "arm.duckdb"
    disc_path = tmp_path / "disc.duckdb"

    con = duckdb.connect(str(core_path))
    con.execute(
        """
        CREATE TABLE systems(
          system_id BIGINT,stable_object_key VARCHAR,system_name VARCHAR,dist_ly DOUBLE
        );
        CREATE TABLE stars(
          star_id BIGINT,system_id BIGINT,spectral_class VARCHAR,
          luminosity_class VARCHAR,spectral_type_raw VARCHAR,
          pm_ra_mas_yr DOUBLE,pm_dec_mas_yr DOUBLE
        );
        CREATE TABLE planets(
          system_id BIGINT,star_id BIGINT,match_confidence DOUBLE,eq_temp_k DOUBLE,
          insol_earth DOUBLE,semi_major_axis_au DOUBLE,eccentricity DOUBLE,
          orbital_period_days DOUBLE,mass_earth DOUBLE,mass_jup DOUBLE
        );
        INSERT INTO systems VALUES (10,'system:test','Test',10);
        INSERT INTO stars VALUES (1,10,'M','V','M5 V',0,0);
        INSERT INTO planets VALUES (10,1,1,NULL,NULL,1,0,365,1,NULL);
        """
    )
    con.close()

    con = duckdb.connect(str(arm_path))
    con.execute(
        """
        CREATE TABLE e6_selected_stellar_display_classifications(
          star_id BIGINT,classification_value VARCHAR
        );
        CREATE TABLE e6_selected_stellar_parameters(
          star_id BIGINT,luminosity_log10_lsun DOUBLE
        );
        INSERT INTO e6_selected_stellar_display_classifications VALUES (1,'WD');
        INSERT INTO e6_selected_stellar_parameters VALUES (1,0);
        """
    )
    con.close()

    scorer.build_scores(
        core_db_path=core_path,
        disc_db_path=disc_path,
        arm_db_path=arm_path,
        weights=scorer.DEFAULT_WEIGHTS,
        build_id="test-build",
        profile_id="default",
        profile_version="1",
    )
    con = duckdb.connect(str(disc_path), read_only=True)
    row = con.execute(
        """
        SELECT dominant_spectral_class,nice_planet_proxy_insolation_count,
               exotic_star_feature
        FROM coolness_scores
        """
    ).fetchone()
    con.close()

    assert row == ("WD", 1, 1.0)

    legacy_disc_path = tmp_path / "legacy-disc.duckdb"
    scorer.build_scores(
        core_db_path=core_path,
        disc_db_path=legacy_disc_path,
        weights=scorer.DEFAULT_WEIGHTS,
        build_id="legacy-build",
        profile_id="default",
        profile_version="1",
    )
    con = duckdb.connect(str(legacy_disc_path), read_only=True)
    legacy_row = con.execute(
        """
        SELECT dominant_spectral_class,nice_planet_proxy_insolation_count,
               exotic_star_feature
        FROM coolness_scores
        """
    ).fetchone()
    con.close()
    assert legacy_row == ("M", 0, 0.0)
