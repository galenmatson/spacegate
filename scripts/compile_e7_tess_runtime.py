#!/usr/bin/env python3
"""Compile clean TESS identity, candidate, and disposition runtime evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_tess_runtime.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-tess-runtime")
PRODUCTS = (
    "tess_target_identity",
    "tess_missing_object_audit",
    "toi_current_evidence",
    "toi_disposition_history",
)


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def write_object_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_tess_runtime_policy.v1":
        raise ValueError("unsupported E7 TESS runtime policy")
    expected_inputs = {
        "clean_runtime_core", "clean_science", "tess_scientific_evidence",
        "toi_history_seed",
    }
    if set(policy.get("inputs") or {}) != expected_inputs:
        raise ValueError("incomplete E7 TESS runtime inputs")
    expected_rules = {
        "open_stability_databases": False,
        "rebind_stable_keys_against_clean_core": True,
        "trust_legacy_numeric_ids": False,
        "candidate_or_negative_rows_link_canonical_planets": False,
        "mutate_canonical_planet_inventory": False,
        "preserve_unresolved_targets": True,
        "preserve_disposition_history": True,
    }
    if policy.get("rules") != expected_rules:
        raise ValueError("unsafe E7 TESS runtime rules")


def accepted_input(state: Path, spec: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    root = (state / spec["relative_path"]).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or file_sha256(manifest_path) != spec["manifest_sha256"]:
        raise ValueError(f"input manifest mismatch: {root}")
    manifest = load_object(manifest_path)
    status = manifest.get("status") or (manifest.get("report") or {}).get("status")
    if manifest.get("build_id") != spec["build_id"] or status != "pass":
        raise ValueError(f"unaccepted input: {root}")
    return root, manifest


def resolve_inputs(policy: dict[str, Any], state: Path) -> dict[str, Any]:
    core_root, core_manifest = accepted_input(state, policy["inputs"]["clean_runtime_core"])
    science_root, science_manifest = accepted_input(state, policy["inputs"]["clean_science"])
    tess_root, tess_manifest = accepted_input(state, policy["inputs"]["tess_scientific_evidence"])
    core = core_root / "core.duckdb"
    science = science_root / "clean_science.duckdb"
    tess = tess_root / "scientific_evidence.duckdb"
    if not core.is_file() or not science.is_file() or not tess.is_file():
        raise FileNotFoundError("E7 TESS runtime database input is missing")
    tess_spec = policy["inputs"]["tess_scientific_evidence"]
    if (
        tess_manifest.get("database_sha256") != tess_spec["database_sha256"]
        or file_sha256(tess) != tess_spec["database_sha256"]
    ):
        raise ValueError("TESS scientific evidence database checksum mismatch")
    history_spec = policy["inputs"]["toi_history_seed"]
    history = (state / history_spec["relative_path"]).resolve()
    if not history.is_file() or file_sha256(history) != history_spec["sha256"]:
        raise ValueError("TOI disposition history seed checksum mismatch")
    return {
        "core": core, "science": science, "tess": tess, "history": history,
        "manifests": {
            "clean_runtime_core": core_manifest,
            "clean_science": science_manifest,
            "tess_scientific_evidence": tess_manifest,
        },
    }


def configure(con: duckdb.DuckDBPyConnection, staging: Path) -> None:
    temporary = staging / "duckdb-tmp"
    temporary.mkdir()
    con.execute("SET threads=16")
    con.execute("SET memory_limit='32GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory={sql_literal(temporary)}")


def materialize(
    con: duckdb.DuckDBPyConnection, policy: dict[str, Any], inputs: dict[str, Any],
    build_id: str,
) -> None:
    con.execute(f"ATTACH {sql_literal(inputs['core'])} AS core (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(inputs['science'])} AS science (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(inputs['tess'])} AS tess (READ_ONLY)")
    con.execute(
        f"CREATE VIEW toi_history_seed AS SELECT * FROM read_csv_auto("
        f"{sql_literal(inputs['history'])},header=true,all_varchar=true)"
    )
    version = sql_literal(policy["policy_version"])
    build = sql_literal(build_id)
    con.execute(
        f"""
        CREATE TEMP TABLE mast_identifiers AS
        SELECT source_record_id,
          max(CASE WHEN namespace='gaia_dr2_source_id' THEN identifier_normalized END)
            AS gaia_dr2_id,
          max(CASE WHEN namespace='hip_id' THEN identifier_normalized END) AS hip_id,
          max(CASE WHEN namespace='tyc_id' THEN identifier_normalized END) AS tyc_id,
          max(CASE WHEN namespace='twomass_designation' THEN identifier_normalized END)
            AS twomass_id
        FROM tess.identifier_claim_evidence
        GROUP BY source_record_id;

        CREATE TEMP TABLE mast_astrometry AS
        SELECT source_record_id,
          max(normalized_value) FILTER (quantity_key='right_ascension') AS ra_deg,
          max(normalized_value) FILTER (quantity_key='declination') AS dec_deg,
          max(normalized_value) FILTER (quantity_key='distance') AS distance_pc
        FROM tess.astrometry_distance_evidence GROUP BY source_record_id;

        CREATE TEMP TABLE clean_target_bindings AS
        SELECT b.*,sr.release_id,sr.source_context_json,sr.source_row_sha256,
          sr.raw_artifact_sha256,sr.retrieved_at,
          try_cast(mi.gaia_dr2_id AS HUGEINT) AS gaia_dr2_id,
          try_cast(mi.hip_id AS BIGINT) AS hip_id,mi.tyc_id,mi.twomass_id,
          ma.ra_deg,ma.dec_deg,ma.distance_pc,
          cs.star_id,cy.system_id,
          CASE
            WHEN b.binding_status='accepted' AND cs.star_id IS NOT NULL
              AND cy.system_id IS NOT NULL THEN 'accepted'
            WHEN b.binding_status='accepted' THEN 'missing'
            ELSE b.binding_status
          END::VARCHAR AS clean_binding_status,
          CASE
            WHEN b.binding_status='accepted' AND
              (cs.star_id IS NULL OR cy.system_id IS NULL)
              THEN 'accepted_key_missing_from_clean_core'
            ELSE b.binding_reason
          END::VARCHAR AS clean_binding_reason
        FROM science.evidence_planet_evidence_tess_target_bindings b
        LEFT JOIN tess.source_records sr
          ON sr.source_record_id=b.mast_source_record_id
        LEFT JOIN mast_identifiers mi
          ON mi.source_record_id=b.mast_source_record_id
        LEFT JOIN mast_astrometry ma
          ON ma.source_record_id=b.mast_source_record_id
        LEFT JOIN core.stars cs ON cs.stable_object_key=b.canonical_star_key
        LEFT JOIN core.systems cy ON cy.stable_object_key=b.canonical_system_key;

        CREATE TABLE tess_target_identity AS
        SELECT row_number() OVER (ORDER BY try_cast(tic_id AS UBIGINT))::BIGINT
            AS tess_identity_id,
          try_cast(tic_id AS UBIGINT)::BIGINT AS tic_id,source_families,
          json_extract_string(source_context_json,'$.version')::VARCHAR AS tic_version,
          clean_binding_status AS resolution_status,
          clean_binding_reason AS resolution_reason,
          CASE WHEN clean_binding_status='accepted' THEN star_id END::BIGINT AS star_id,
          CASE WHEN clean_binding_status='accepted' THEN system_id END::HUGEINT AS system_id,
          CASE WHEN clean_binding_status='accepted' THEN 1.0
               WHEN clean_binding_status='ambiguous' THEN 0.5 END::DOUBLE
            AS resolution_confidence,
          gaia_dr2_id,hip_id,tyc_id,twomass_id,ra_deg,dec_deg,distance_pc,
          tic_disposition,
          NULL::VARCHAR AS duplicate_id,canonical_candidate_count AS candidate_star_count,
          NULL::BIGINT AS neighbourhood_row_count,NULL::BIGINT AS dr3_candidate_count,
          NULL::BIGINT AS gaia_dr3_source_count,NULL::BIGINT AS gaia_dr3_in_scope_count,
          NULL::DOUBLE AS gaia_dr3_max_parallax_mas,
          to_json(struct_pack(graph_outcome:=graph_outcome,graph_reason:=graph_reason,
            clean_star_key:=canonical_star_key,
            clean_system_key:=canonical_system_key))::VARCHAR AS candidates_json,
          source_row_sha256::VARCHAR AS source_row_hash,release_id::VARCHAR AS source_version,
          'https://mast.stsci.edu/api/v0/invoke'::VARCHAR AS source_url,
          raw_artifact_sha256::VARCHAR AS retrieval_checksum,
          strftime(retrieved_at,'%Y-%m-%dT%H:%M:%SZ')::VARCHAR AS retrieved_at,
          strftime(retrieved_at,'%Y-%m-%dT%H:%M:%SZ')::VARCHAR AS ingested_at,
          'e7_clean_tess_identity_projection_v1'::VARCHAR AS transform_version
        FROM clean_target_bindings ORDER BY try_cast(tic_id AS UBIGINT);

        CREATE TABLE tess_missing_object_audit AS
        SELECT row_number() OVER (ORDER BY tic_id)::BIGINT AS audit_id,
          tic_id,source_families,resolution_status,resolution_reason,
          CASE
            WHEN resolution_status='excluded' THEN 'tic_artifact_split_join_or_duplicate'
            WHEN resolution_status='ambiguous' THEN 'ambiguous_identity'
            WHEN resolution_status='source_missing' THEN 'source_missing'
            WHEN resolution_reason='outside_current_canonical_backbone'
              THEN 'outside_distance_scope'
            WHEN resolution_reason='accepted_key_missing_from_clean_core'
              THEN 'clean_core_identity_gap'
            WHEN resolution_reason LIKE '%gaia_dr3%' THEN 'valid_gaia_dr3_excluded_or_absent'
            WHEN resolution_reason LIKE '%gaia_dr2%' THEN 'gaia_dr2_only_or_unmapped'
            ELSE 'insufficient_evidence'
          END::VARCHAR AS gap_class,
          gaia_dr2_id,hip_id,tyc_id,twomass_id,ra_deg,dec_deg,distance_pc,
          tic_disposition,duplicate_id,candidate_star_count,neighbourhood_row_count,
          dr3_candidate_count,candidates_json,source_row_hash,gaia_dr3_source_count,
          gaia_dr3_in_scope_count,gaia_dr3_max_parallax_mas
        FROM tess_target_identity WHERE resolution_status<>'accepted'
        ORDER BY tic_id;

        CREATE TEMP TABLE toi_identifiers AS
        SELECT source_record_id,
          max(identifier_normalized) FILTER (namespace='ctoi_id') AS ctoi_alias,
          max(identifier_normalized) FILTER (namespace='toi_host_prefix') AS toi_prefix
        FROM tess.identifier_claim_evidence GROUP BY source_record_id;

        CREATE TEMP TABLE toi_astrometry AS
        SELECT source_record_id,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.ra') AS ra_deg,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.dec') AS dec_deg,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.st_pmra') AS pm_ra_mas_yr,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.st_pmdec') AS pm_dec_mas_yr,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.st_dist') AS stellar_distance_pc
        FROM tess.astrometry_distance_evidence GROUP BY source_record_id;

        CREATE TEMP TABLE toi_photometry AS
        SELECT source_record_id,max(normalized_value) AS tmag
        FROM tess.photometry_extinction_evidence
        WHERE quantity_key='magnitude' AND bandpass='T'
        GROUP BY source_record_id;

        CREATE TEMP TABLE toi_stellar AS
        SELECT source_record_id,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.st_teff') AS stellar_teff_k,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.st_logg') AS stellar_logg_cgs,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.st_rad') AS stellar_radius_solar
        FROM tess.stellar_parameter_evidence GROUP BY source_record_id;

        CREATE TEMP TABLE toi_planet AS
        SELECT source_record_id,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_pnum') AS planet_number,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_orbper') AS orbital_period_days,
          max(uncertainty_upper) FILTER (quantity_key='nasa_exoplanet_archive.pl_orbper') AS orbital_period_err_plus,
          -max(uncertainty_lower) FILTER (quantity_key='nasa_exoplanet_archive.pl_orbper') AS orbital_period_err_minus,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_rade') AS planet_radius_earth,
          max(uncertainty_upper) FILTER (quantity_key='nasa_exoplanet_archive.pl_rade') AS planet_radius_err_plus,
          -max(uncertainty_lower) FILTER (quantity_key='nasa_exoplanet_archive.pl_rade') AS planet_radius_err_minus,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_insol') AS insolation_earth,
          max(uncertainty_upper) FILTER (quantity_key='nasa_exoplanet_archive.pl_insol') AS insolation_err_plus,
          -max(uncertainty_lower) FILTER (quantity_key='nasa_exoplanet_archive.pl_insol') AS insolation_err_minus,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_eqt') AS equilibrium_temp_k
        FROM tess.planet_parameter_evidence GROUP BY source_record_id;

        CREATE TEMP TABLE toi_transit AS
        SELECT source_record_id,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_tranmid') AS transit_epoch_bjd,
          max(uncertainty_upper) FILTER (quantity_key='nasa_exoplanet_archive.pl_tranmid') AS transit_epoch_err_plus,
          -max(uncertainty_lower) FILTER (quantity_key='nasa_exoplanet_archive.pl_tranmid') AS transit_epoch_err_minus,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_trandurh') AS transit_duration_hours,
          max(uncertainty_upper) FILTER (quantity_key='nasa_exoplanet_archive.pl_trandurh') AS transit_duration_err_plus,
          -max(uncertainty_lower) FILTER (quantity_key='nasa_exoplanet_archive.pl_trandurh') AS transit_duration_err_minus,
          max(normalized_value) FILTER (quantity_key='nasa_exoplanet_archive.pl_trandep') AS transit_depth_ppm,
          max(uncertainty_upper) FILTER (quantity_key='nasa_exoplanet_archive.pl_trandep') AS transit_depth_err_plus,
          -max(uncertainty_lower) FILTER (quantity_key='nasa_exoplanet_archive.pl_trandep') AS transit_depth_err_minus
        FROM tess.transit_observation_evidence GROUP BY source_record_id;

        CREATE TEMP TABLE clean_toi_bindings AS
        SELECT c.*,sr.release_id,sr.source_context_json,sr.source_row_sha256,
          sr.raw_artifact_sha256,sr.retrieved_at,ti.ctoi_alias,ti.toi_prefix,
          ta.*,tp.tmag,ts.stellar_teff_k,ts.stellar_logg_cgs,ts.stellar_radius_solar,
          pp.*,tr.*,
          cs.star_id,cy.system_id,cp.planet_id AS clean_planet_id,
          CASE WHEN c.host_binding_status='accepted' AND cs.star_id IS NOT NULL
            AND cy.system_id IS NOT NULL THEN 'accepted'
            WHEN c.host_binding_status='accepted' THEN 'missing'
            ELSE c.host_binding_status END::VARCHAR AS clean_host_status
        FROM science.evidence_planet_evidence_tess_candidate_projection c
        JOIN tess.source_records sr USING(source_record_id)
        LEFT JOIN toi_identifiers ti USING(source_record_id)
        LEFT JOIN toi_astrometry ta USING(source_record_id)
        LEFT JOIN toi_photometry tp USING(source_record_id)
        LEFT JOIN toi_stellar ts USING(source_record_id)
        LEFT JOIN toi_planet pp USING(source_record_id)
        LEFT JOIN toi_transit tr USING(source_record_id)
        LEFT JOIN core.stars cs ON cs.stable_object_key=c.canonical_star_key
        LEFT JOIN core.systems cy ON cy.stable_object_key=c.canonical_system_key
        LEFT JOIN core.planets cp ON cp.stable_object_key=c.canonical_planet_key;

        CREATE TABLE toi_current_evidence AS
        SELECT row_number() OVER (ORDER BY try_cast(toi_id AS DOUBLE),source_record_id)::BIGINT
            AS toi_evidence_id,
          ('TOI-' || toi_id)::VARCHAR AS source_key,try_cast(tic_id AS BIGINT) AS tic_id,
          toi_id::VARCHAR AS toi,('TOI-' || toi_id)::VARCHAR AS toi_display,
          coalesce(toi_prefix,split_part(toi_id,'.',1))::VARCHAR AS toi_prefix,
          ctoi_alias::VARCHAR,try_cast(planet_number AS INTEGER) AS planet_number,
          disposition_raw::VARCHAR AS disposition,
          CASE WHEN clean_host_status='accepted' THEN star_id END::BIGINT AS star_id,
          CASE WHEN clean_host_status='accepted' THEN system_id END::HUGEINT AS system_id,
          CASE WHEN disposition_raw IN ('CP','KP') AND planet_link_status='accepted'
            THEN clean_planet_id END::HUGEINT AS planet_id,
          clean_host_status::VARCHAR AS host_resolution_status,
          CASE WHEN clean_host_status='accepted' THEN 'exact_stable_key_clean_core_rebind'
               WHEN host_binding_status='accepted' THEN 'accepted_key_missing_from_clean_core'
               ELSE host_binding_status END::VARCHAR AS host_resolution_reason,
          CASE WHEN clean_host_status='accepted' THEN 1.0
               WHEN clean_host_status='ambiguous' THEN 0.5 END::DOUBLE
            AS host_resolution_confidence,
          planet_link_method,planet_period_delta_days,
          ra_deg,dec_deg,pm_ra_mas_yr,pm_dec_mas_yr,tmag,
          transit_epoch_bjd,transit_epoch_err_plus,transit_epoch_err_minus,
          orbital_period_days,orbital_period_err_plus,orbital_period_err_minus,
          transit_duration_hours,transit_duration_err_plus,transit_duration_err_minus,
          transit_depth_ppm,transit_depth_err_plus,transit_depth_err_minus,
          planet_radius_earth,planet_radius_err_plus,planet_radius_err_minus,
          insolation_earth,insolation_err_plus,insolation_err_minus,
          equilibrium_temp_k,stellar_distance_pc,stellar_teff_k,stellar_logg_cgs,
          stellar_radius_solar,
          json_extract_string(source_context_json,'$.sectors')::VARCHAR AS sectors,
          json_extract_string(lifecycle_quality_json,'$.source_context.toi_created')::VARCHAR
            AS toi_created,
          effective_at_raw::VARCHAR AS row_updated_at,
          json_extract_string(source_context_json,'$.release_date')::VARCHAR AS release_date,
          source_row_sha256::VARCHAR AS source_row_hash,
          'nasa_toi'::VARCHAR AS source_catalog,release_id::VARCHAR AS source_version,
          'https://exoplanetarchive.ipac.caltech.edu/TAP/sync'::VARCHAR AS source_url,
          raw_artifact_sha256::VARCHAR AS retrieval_checksum,
          strftime(retrieved_at,'%Y-%m-%dT%H:%M:%SZ')::VARCHAR AS retrieved_at,
          strftime(retrieved_at,'%Y-%m-%dT%H:%M:%SZ')::VARCHAR AS ingested_at,
          'e7_clean_toi_evidence_projection_v1'::VARCHAR AS transform_version
        FROM clean_toi_bindings
        ORDER BY try_cast(toi_id AS DOUBLE),source_record_id;

        CREATE TABLE toi_disposition_history AS
        SELECT row_number() OVER (
            ORDER BY h.source_key,h.effective_at,h.disposition
          )::BIGINT AS history_id,
          h.source_key,try_cast(h.tic_id AS BIGINT) AS tic_id,h.toi_display,h.disposition,
          nullif(h.effective_at,'')::VARCHAR AS effective_at,
          nullif(h.release_date,'')::VARCHAR AS release_date,
          h.source_row_hash,nullif(h.first_observed_at,'')::VARCHAR AS first_observed_at,
          nullif(h.last_observed_at,'')::VARCHAR AS last_observed_at,
          'nasa_toi'::VARCHAR AS source_catalog,sr.release_id::VARCHAR AS source_version,
          'https://exoplanetarchive.ipac.caltech.edu/TAP/sync'::VARCHAR AS source_url,
          sr.raw_artifact_sha256::VARCHAR AS retrieval_checksum,
          strftime(sr.retrieved_at,'%Y-%m-%dT%H:%M:%SZ')::VARCHAR AS retrieved_at,
          strftime(sr.retrieved_at,'%Y-%m-%dT%H:%M:%SZ')::VARCHAR AS ingested_at,
          'e7_clean_toi_history_projection_v1'::VARCHAR AS transform_version
        FROM toi_history_seed h
        JOIN clean_toi_bindings c ON ('TOI-' || c.toi_id)=h.source_key
        JOIN tess.source_records sr ON sr.source_record_id=c.source_record_id
        ORDER BY h.source_key,h.effective_at,h.disposition;

        CREATE TABLE build_metadata AS
        SELECT {build}::VARCHAR AS build_id,{version}::VARCHAR AS policy_version,
          '0'::VARCHAR AS stability_database_opened,
          (SELECT count(*) FROM core.planets)::BIGINT AS canonical_planet_count_before,
          (SELECT count(*) FROM core.planets)::BIGINT AS canonical_planet_count_after;
        """
    )


def verify(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> dict[str, Any]:
    scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
    counts = {
        "targeted_tics": scalar("SELECT count(*) FROM tess_target_identity"),
        "targets_accepted": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='accepted'"),
        "targets_ambiguous": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='ambiguous'"),
        "targets_excluded": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='excluded'"),
        "targets_missing": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='missing'"),
        "targets_source_missing": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='source_missing'"),
        "missing_object_audit": scalar("SELECT count(*) FROM tess_missing_object_audit"),
        "tois": scalar("SELECT count(*) FROM toi_current_evidence"),
        "toi_confirmed_known": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('CP','KP')"),
        "toi_candidates": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('PC','APC')"),
        "toi_negative": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('FP','FA')"),
        "toi_unclassified": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IS NULL"),
        "toi_confirmed_known_planet_links": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('CP','KP') AND planet_id IS NOT NULL"),
        "toi_history_events": scalar("SELECT count(*) FROM toi_disposition_history"),
        "canonical_inventory_mutations": scalar("SELECT abs(canonical_planet_count_after-canonical_planet_count_before) FROM build_metadata"),
    }
    expected = {key: int(value) for key, value in policy["acceptance"].items()}
    checks = {
        "acceptance_count_delta": sum(abs(counts.get(key, -1)-value) for key, value in expected.items()),
        "duplicate_tic_ids": scalar("SELECT count(*) FROM (SELECT tic_id FROM tess_target_identity GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_toi_ids": scalar("SELECT count(*) FROM (SELECT source_key FROM toi_current_evidence GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_history_events": scalar("SELECT count(*) FROM (SELECT source_key,disposition,effective_at FROM toi_disposition_history GROUP BY ALL HAVING count(*)<>1)"),
        "invalid_target_status": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status NOT IN ('accepted','missing','excluded','ambiguous','source_missing')"),
        "accepted_target_without_clean_ids": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status='accepted' AND (star_id IS NULL OR system_id IS NULL)"),
        "unaccepted_target_with_clean_ids": scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status<>'accepted' AND (star_id IS NOT NULL OR system_id IS NOT NULL)"),
        "candidate_or_negative_planet_links": scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('PC','APC','FP','FA') AND planet_id IS NOT NULL"),
        "confirmed_link_without_clean_planet": scalar("SELECT count(*) FROM toi_current_evidence t LEFT JOIN core.planets p USING(planet_id) WHERE t.planet_id IS NOT NULL AND p.planet_id IS NULL"),
        "accepted_host_without_clean_objects": scalar("SELECT count(*) FROM toi_current_evidence t LEFT JOIN core.stars s ON s.star_id=t.star_id LEFT JOIN core.systems y ON y.system_id=t.system_id WHERE t.host_resolution_status='accepted' AND (s.star_id IS NULL OR y.system_id IS NULL)"),
        "history_without_current_toi": scalar("SELECT count(*) FROM toi_disposition_history h LEFT JOIN toi_current_evidence t USING(source_key) WHERE t.source_key IS NULL"),
        "current_toi_without_history": scalar("SELECT count(*) FROM toi_current_evidence t LEFT JOIN toi_disposition_history h USING(source_key) WHERE h.source_key IS NULL"),
        "history_current_disposition_mismatch": scalar("SELECT count(*) FROM toi_current_evidence t JOIN toi_disposition_history h USING(source_key) WHERE t.disposition IS DISTINCT FROM h.disposition"),
        "missing_target_audit_delta": abs(scalar("SELECT count(*) FROM tess_target_identity WHERE resolution_status<>'accepted'")-counts["missing_object_audit"]),
        "missing_source_provenance": scalar("SELECT count(*) FROM toi_current_evidence WHERE source_row_hash IS NULL OR retrieval_checksum IS NULL OR source_version IS NULL"),
        "stability_database_opened": scalar("SELECT count(*) FROM build_metadata WHERE stability_database_opened<>'0'"),
    }
    failures = {key: value for key, value in checks.items() if value}
    return {
        "status": "pass" if not failures else "fail",
        "counts": counts,
        "checks": checks,
        "failing_checks": failures,
    }


def compile_runtime(
    policy_path: Path, state: Path, output_root: Path, *, link_into_state: bool,
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    policy = load_object(policy_path)
    validate_policy(policy)
    inputs = resolve_inputs(policy, state)
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    input_identity = {
        name: ({"sha256": spec["sha256"]} if name == "toi_history_seed" else {
            "build_id": spec["build_id"], "manifest_sha256": spec["manifest_sha256"],
        })
        for name, spec in policy["inputs"].items()
    }
    build_id = stable_hash({
        "compiler_sha256": compiler_sha, "policy_sha256": policy_sha,
        "inputs": input_identity,
    })[:24]
    final = output_root / build_id
    if (final / "manifest.json").is_file():
        return load_object(final / "manifest.json")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    try:
        con = duckdb.connect()
        configure(con, staging)
        try:
            materialize(con, policy, inputs, build_id)
            verification = verify(con, policy)
            if verification["status"] != "pass":
                raise ValueError(f"TESS runtime verification failed: {verification['failing_checks']}")
            products: dict[str, Any] = {}
            for table in PRODUCTS:
                path = staging / f"{table}.parquet"
                con.execute(
                    f"COPY (SELECT * FROM {table} ORDER BY ALL) TO {sql_literal(path)} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)"
                )
                products[path.name] = {
                    "rows": int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),
                    "bytes": path.stat().st_size,"sha256": file_sha256(path),
                    "determinism": "byte_exact",
                }
        finally:
            con.close()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        manifest = {
            "schema_version": "spacegate.e7_tess_runtime_manifest.v1",
            "build_id": build_id,"status": "pass",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha,"compiler_sha256": compiler_sha,
            "inputs": input_identity,"stability_databases_opened": [],
            "canonical_inventory_mutations": 0,
            "verification": verification,"products": products,
            "performance": {
                "wall_seconds": round(time.monotonic()-started, 6),
                "cpu_seconds": round(time.process_time()-cpu_started, 6),
                "peak_rss_kib": int(usage.ru_maxrss),
            },
        }
        write_object_atomic(staging / "manifest.json", manifest)
        shutil.rmtree(staging / "duckdb-tmp", ignore_errors=True)
        os.replace(staging, final)
        if link_into_state:
            root = state / "derived/evidence_lake_v2/tess_runtime"
            root.mkdir(parents=True, exist_ok=True)
            link = root / build_id
            if not link.exists() and not link.is_symlink():
                link.symlink_to(final)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_runtime(
        args.policy.resolve(),args.state_dir.resolve(),args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps({
        "build_id": manifest["build_id"],"status": manifest["status"],
        "counts": manifest["verification"]["counts"],
        "wall_seconds": manifest["performance"]["wall_seconds"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
