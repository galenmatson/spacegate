from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "srv/api"))

from app.queries import fetch_infrared_evidence_for_system  # noqa: E402


@pytest.mark.parametrize("clean_schema", [False, True])
def test_infrared_motion_adapter_supports_legacy_and_clean_columns(
    tmp_path: Path, clean_schema: bool
) -> None:
    arm_path = tmp_path / "arm.duckdb"
    arm = duckdb.connect(str(arm_path))
    motion_columns = (
        "pmra double,pmdec double,pmra_error double,pmdec_error double"
        if clean_schema
        else "pm_ra double,pm_dec double,pm_ra_error double,pm_dec_error double"
    )
    arm.execute(
        f"""
        CREATE TABLE infrared_source_matches(
          infrared_match_id bigint,target_type varchar,target_id bigint,
          system_id bigint,source_catalog varchar,source_version varchar,
          source_key varchar,source_designation varchar,angular_sep_arcsec double,
          match_rank bigint,match_score double,confidence_tier varchar,
          match_method varchar,conflict_status varchar
        );
        CREATE TABLE infrared_photometry(
          source_catalog varchar,source_key varchar,target_type varchar,target_id bigint,
          w1_mag double,w2_mag double,w3_mag double,w4_mag double,
          w1_snr double,w2_snr double,w3_snr double,w4_snr double,
          quality_flags varchar,artifact_flags varchar
        );
        CREATE TABLE infrared_motion_evidence(
          source_catalog varchar,source_key varchar,target_type varchar,target_id bigint,
          {motion_columns},pm_unit varchar,parallax_like_arcsec double,
          parallax_like_error_arcsec double,parallax_like_note varchar
        );
        INSERT INTO infrared_source_matches VALUES
          (1,'star',20,10,'catwise','2020','source-1','CWISE Test',0.2,1,0.9,
           'high','position','none');
        INSERT INTO infrared_photometry VALUES
          ('catwise','source-1','star',20,10,9,8,7,30,25,20,15,'A','0');
        INSERT INTO infrared_motion_evidence VALUES
          ('catwise','source-1','star',20,1.5,-2.5,0.1,0.2,'mas/yr',0.01,0.001,'candidate');
        """
    )
    arm.close()

    con = duckdb.connect()
    try:
        payload = fetch_infrared_evidence_for_system(
            con, system_id=10, star_ids=[20], arm_db_path=str(arm_path)
        )
    finally:
        con.close()

    motion = payload["matches"][0]["motion"]
    assert motion == {
        "pm_ra": 1.5,
        "pm_dec": -2.5,
        "pm_unit": "mas/yr",
        "pm_ra_error": 0.1,
        "pm_dec_error": 0.2,
        "parallax_like_arcsec": 0.01,
        "parallax_like_error_arcsec": 0.001,
        "parallax_like_note": "candidate",
    }
