#!/usr/bin/env python3
"""Compose an API-compatible CORE from pinned Evidence Lake artifacts only."""

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
from typing import Any, Callable

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_runtime_core.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-core")


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


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Timings:
    def __init__(self) -> None:
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.phases: list[dict[str, Any]] = []

    def run(self, name: str, fn: Callable[[], Any]) -> Any:
        started = time.monotonic()
        cpu_started = time.process_time()
        before = resource.getrusage(resource.RUSAGE_SELF)
        result = fn()
        after = resource.getrusage(resource.RUSAGE_SELF)
        self.phases.append({
            "phase": name,
            "wall_seconds": round(time.monotonic() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "peak_rss_kib_after": int(after.ru_maxrss),
            "input_blocks_delta": int(after.ru_inblock - before.ru_inblock),
            "output_blocks_delta": int(after.ru_oublock - before.ru_oublock),
        })
        return result

    def report(self) -> dict[str, Any]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {
            "wall_seconds": round(time.monotonic() - self.started, 6),
            "cpu_seconds": round(time.process_time() - self.cpu_started, 6),
            "peak_rss_kib": int(usage.ru_maxrss),
            "phases": self.phases,
        }


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_runtime_core_policy.v1":
        raise ValueError("unsupported clean runtime CORE policy")
    required_rules = {
        "open_stability_databases": False,
        "preserve_api_core_schema": True,
        "canonical_inventory_from_clean_foundation_only": True,
        "scientific_values_from_selected_facts_only": True,
        "preserve_unbound_planet_identity": True,
        "preserve_unbound_tess_eb_evidence": True,
        "canonical_white_dwarfs_remain_stars": True,
        "compact_evidence_without_canonical_star_binding_remains_separate": True,
        "cluster_membership_creates_containment": False,
    }
    rules = policy.get("rules") or {}
    if any(rules.get(key) is not expected for key, expected in required_rules.items()):
        raise ValueError("unsafe clean runtime CORE rules")
    required_inputs = {
        "clean_foundation", "clean_science", "clean_clusters", "clean_extended_objects"
    }
    if set(policy.get("inputs") or {}) != required_inputs:
        raise ValueError("clean runtime CORE inputs are incomplete")
    for name, spec in policy["inputs"].items():
        if not isinstance(spec, dict) or not spec.get("build_id") or not spec.get("relative_path"):
            raise ValueError(f"invalid input contract: {name}")
        if len(str(spec.get("manifest_sha256") or "")) != 64:
            raise ValueError(f"invalid manifest checksum: {name}")
        path = Path(str(spec["relative_path"]))
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"unbounded input path: {name}")


def resolve_inputs(policy: dict[str, Any], state: Path) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for name, spec in policy["inputs"].items():
        root = (state / spec["relative_path"]).resolve()
        manifest_path = root / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(manifest_path)
        manifest_hash = file_sha256(manifest_path)
        if manifest_hash != spec["manifest_sha256"]:
            raise ValueError(f"manifest checksum mismatch: {name}")
        manifest = load_object(manifest_path)
        if manifest.get("build_id") != spec["build_id"] or manifest.get("status") != "pass":
            raise ValueError(f"unaccepted input manifest: {name}")
        resolved[name] = {
            "root": root,
            "manifest": manifest,
            "manifest_sha256": manifest_hash,
            "build_id": spec["build_id"],
        }
    return resolved


def product_path(input_spec: dict[str, Any], relative: str) -> Path:
    path = input_spec["root"] / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    products = input_spec["manifest"].get("products") or input_spec["manifest"].get("deterministic_files") or {}
    expected = products.get(relative)
    if not isinstance(expected, dict) or len(str(expected.get("sha256") or "")) != 64:
        raise ValueError(f"unregistered input product: {path}")
    actual = file_sha256(path)
    if actual != expected["sha256"]:
        raise ValueError(f"input product checksum mismatch: {path}")
    return path


def configure(con: duckdb.DuckDBPyConnection, staging: Path) -> None:
    temp_dir = staging / "duckdb-tmp"
    temp_dir.mkdir(exist_ok=True)
    con.execute("SET threads=16")
    con.execute("SET memory_limit='48GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")


def create_input_views(
    con: duckdb.DuckDBPyConnection,
    foundation: dict[str, Path],
    science_db: Path,
    hierarchy_db: Path,
    clusters: dict[str, Path],
    extended: dict[str, Path],
) -> None:
    for name, path in foundation.items():
        con.execute(
            f"CREATE TEMP VIEW foundation_{name} AS SELECT * FROM read_parquet({sql_literal(path)})"
        )
    for name, path in clusters.items():
        con.execute(
            f"CREATE TEMP VIEW cluster_{name} AS SELECT * FROM read_parquet({sql_literal(path)})"
        )
    for name, path in extended.items():
        con.execute(
            f"CREATE TEMP VIEW extended_{name} AS SELECT * FROM read_parquet({sql_literal(path)})"
        )
    con.execute(f"ATTACH {sql_literal(science_db)} AS science (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(hierarchy_db)} AS hierarchy (READ_ONLY)")


def create_identity_tables(con: duckdb.DuckDBPyConnection, build_id: str, policy: dict[str, Any]) -> None:
    con.execute(
        f"""
        CREATE TABLE aliases AS SELECT * FROM foundation_aliases ORDER BY alias_id;
        CREATE TABLE object_identifiers AS SELECT * FROM foundation_object_identifiers ORDER BY identifier_id;
        CREATE TABLE identifier_quarantine AS SELECT * FROM foundation_identifier_quarantine ORDER BY quarantine_key;
        CREATE TABLE system_search_terms AS SELECT * FROM foundation_system_search_terms ORDER BY system_id,target_type,target_id,term_norm;
        CREATE TABLE extended_object_aliases AS SELECT * FROM extended_extended_object_aliases ORDER BY extended_object_alias_id;
        CREATE TABLE extended_object_identifiers AS SELECT * FROM extended_extended_object_identifiers ORDER BY extended_object_identifier_id;
        CREATE TABLE extended_object_identity_quarantine AS SELECT * FROM extended_extended_object_identity_quarantine ORDER BY extended_object_quarantine_id;
        CREATE TABLE extended_object_source_reconciliation AS SELECT * FROM extended_extended_object_source_reconciliation ORDER BY extended_object_reconciliation_id;
        CREATE TABLE extended_object_search_terms AS SELECT * FROM extended_extended_object_search_terms ORDER BY extended_object_search_term_id;
        CREATE TABLE build_metadata(key VARCHAR,value VARCHAR);
        INSERT INTO build_metadata VALUES
          ('build_id',{sql_literal(build_id)}),
          ('build_kind','e7_clean_runtime_core'),
          ('policy_version',{sql_literal(policy['policy_version'])}),
          ('compiler_version',{sql_literal(policy['compiler_version'])}),
          ('stability_database_opened','0'),
          ('scientific_values_from_selected_facts_only','1');
        """
    )


def create_identifier_projection(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE star_identifier_projection AS
        SELECT target_id AS star_id,
          max(try_cast(id_value_norm AS BIGINT)) FILTER (namespace='gaia_dr3') AS gaia_id,
          max(try_cast(id_value_norm AS BIGINT)) FILTER (namespace='hip') AS hip_id,
          max(try_cast(id_value_norm AS BIGINT)) FILTER (namespace='hd') AS hd_id,
          max(id_value_raw) FILTER (namespace='wds') AS wds_id,
          json_object(
            'gaia_dr3',max(id_value_raw) FILTER (namespace='gaia_dr3'),
            'hip',max(id_value_raw) FILTER (namespace='hip'),
            'hd',max(id_value_raw) FILTER (namespace='hd'),
            'wds',max(id_value_raw) FILTER (namespace='wds'),
            'tic',max(id_value_raw) FILTER (namespace='tic'),
            'tyc',max(id_value_raw) FILTER (namespace='tyc'),
            'gl',max(id_value_raw) FILTER (namespace='gl'),
            'hr',max(id_value_raw) FILTER (namespace='hr')
          ) AS catalog_ids_json
        FROM foundation_object_identifiers
        WHERE target_type='star'
        GROUP BY target_id;

        CREATE TEMP TABLE hierarchy_star_projection AS
        SELECT canonical_key AS stable_object_key,
          member_role AS component,
          row_number() OVER (
            PARTITION BY canonical_key
            ORDER BY CASE node_kind WHEN 'canonical_star' THEN 0 ELSE 1 END,hierarchy_node_key
          ) AS choice_rank
        FROM hierarchy.hierarchy_nodes
        WHERE component_family='star' AND canonical_key IS NOT NULL;
        """
    )


def create_stars(
    con: duckdb.DuckDBPyConnection, build_id: str, lineage_timestamp: str
) -> None:
    con.execute(
        f"""
        CREATE TABLE stars AS
        WITH joined AS (
          SELECT f.*,
            coalesce(p.ra_deg,sys.ra_deg) selected_ra_deg,
            coalesce(p.dec_deg,sys.dec_deg) selected_dec_deg,
            coalesce(p.distance_pc,sys.distance_pc) selected_distance_pc,
            sys.x_helio_ly system_x_helio_ly,sys.y_helio_ly system_y_helio_ly,
            sys.z_helio_ly system_z_helio_ly,sys.star_count system_star_count,
            sys.parallax_mas system_parallax_mas,sys.placement_source system_placement_source,
            sys.placement_method system_placement_method,
            p.teff_k,p.logg_cgs,p.mass_msun,
            p.pmra_mas_yr,p.pmdec_mas_yr,p.radial_velocity_km_s,p.gaia_ruwe,
            a.parallax_mas AS selected_parallax_mas,
            a.parallax_mas_lower,a.parallax_mas_upper,a.gaia_non_single_star_status,
            phot.gaia_bp_rp_mag,
            c.spectral_type_optical,c.spectral_type_infrared,c.spectral_type_simbad,
            coalesce(c.spectral_type_optical,c.spectral_type_infrared,c.spectral_type_simbad)
              selected_spectral_type,
            d.classification_value,d.classification_status,d.evidence_basis,
            d.selected_fact_id,d.confidence_score,d.candidate_classes_json,
            i.gaia_id,i.hip_id,i.hd_id,i.wds_id,i.catalog_ids_json,
            h.component
          FROM foundation_stars f
          JOIN foundation_systems sys USING (system_id)
          LEFT JOIN science.selected_stellar_parameters p USING (star_id)
          LEFT JOIN science.selected_stellar_astrometry a USING (star_id)
          LEFT JOIN science.selected_stellar_photometry phot USING (star_id)
          LEFT JOIN science.selected_stellar_classification c USING (star_id)
          LEFT JOIN science.selected_stellar_display_classifications d USING (star_id)
          LEFT JOIN star_identifier_projection i USING (star_id)
          LEFT JOIN hierarchy_star_projection h ON h.stable_object_key=f.stable_object_key AND h.choice_rank=1
        )
        SELECT
          star_id,spatial_index,system_id,stable_object_key,star_name,star_name_norm,
          component,
          selected_ra_deg AS ra_deg,selected_dec_deg AS dec_deg,
          selected_distance_pc*3.26156 AS dist_ly,
          coalesce(selected_parallax_mas,
            CASE WHEN system_star_count=1 THEN system_parallax_mas END) AS parallax_mas,
          CASE WHEN parallax_mas_lower IS NOT NULL AND parallax_mas_upper IS NOT NULL
            THEN abs(parallax_mas_upper-parallax_mas_lower)/2 END AS parallax_error_mas,
          CASE WHEN selected_parallax_mas>0 AND parallax_mas_lower IS NOT NULL AND parallax_mas_upper IS NOT NULL
            THEN selected_parallax_mas/nullif(abs(parallax_mas_upper-parallax_mas_lower)/2,0) END AS parallax_over_error,
          gaia_ruwe AS ruwe,
          NULL::INTEGER AS gaia_ucd_hmac_cluster_id,NULL::VARCHAR AS gaia_ucd_banyan_cluster,
          NULL::DOUBLE AS gaia_ucd_banyan_probability,
          coalesce(
            selected_distance_pc*3.26156*cos(radians(selected_dec_deg))*cos(radians(selected_ra_deg)),
            system_x_helio_ly
          ) AS x_helio_ly,
          coalesce(
            selected_distance_pc*3.26156*cos(radians(selected_dec_deg))*sin(radians(selected_ra_deg)),
            system_y_helio_ly
          ) AS y_helio_ly,
          coalesce(selected_distance_pc*3.26156*sin(radians(selected_dec_deg)),system_z_helio_ly) AS z_helio_ly,
          NULL::DOUBLE AS x_gal_ly,NULL::DOUBLE AS y_gal_ly,NULL::DOUBLE AS z_gal_ly,
          pmra_mas_yr AS pm_ra_mas_yr,pmdec_mas_yr AS pm_dec_mas_yr,
          radial_velocity_km_s AS radial_velocity_kms,
          selected_spectral_type AS spectral_type_raw,
          CASE classification_value WHEN 'WD' THEN 'D' ELSE classification_value END AS spectral_class,
          NULL::VARCHAR AS spectral_subtype,NULL::VARCHAR AS luminosity_class,
          NULL::VARCHAR AS spectral_peculiar,NULL::DOUBLE AS vmag,NULL::DOUBLE AS absmag,
          gaia_bp_rp_mag AS color_index,teff_k,gaia_id,hip_id,hd_id,wds_id,
          CASE WHEN component IS NOT NULL THEN 'permanent_hierarchy_component' END AS multiplicity_match_method,
          CASE WHEN component IS NOT NULL THEN 1.0::DECIMAL(3,2) END AS multiplicity_match_confidence,
          CASE WHEN component IS NOT NULL THEN '["permanent_hierarchy"]' END AS multiplicity_source_catalogs_json,
          (gaia_non_single_star_status IS NOT NULL) AS gaia_non_single_star,
          CASE WHEN gaia_non_single_star_status IS NOT NULL THEN 1 ELSE 0 END::BIGINT AS gaia_nss_solution_count,
          CASE WHEN gaia_non_single_star_status IS NOT NULL
            THEN to_json([gaia_non_single_star_status])::VARCHAR ELSE '[]' END AS gaia_nss_solution_types_json,
          NULL::DOUBLE AS gaia_nss_significance_max,
          (gaia_non_single_star_status IS NOT NULL) AS has_gaia_nss_evidence,
          EXISTS(SELECT 1 FROM science.evidence_component_msc_system_bindings m
            JOIN foundation_systems fs ON fs.stable_object_key=m.canonical_system_stable_object_key
            WHERE fs.system_id=joined.system_id AND m.binding_status='accepted') AS has_msc_evidence,
          (wds_id IS NOT NULL) AS has_wds_evidence,
          EXISTS(SELECT 1 FROM science.evidence_component_orb6_relation_bindings o
            JOIN foundation_systems fs ON fs.stable_object_key=o.canonical_system_stable_object_key
            WHERE fs.system_id=joined.system_id AND o.binding_status='accepted') AS has_orb6_evidence,
          NULL::VARCHAR AS sbx_sn,
          0::BIGINT AS sbx_orbit_count,NULL::VARCHAR AS sbx_family,
          NULL::DOUBLE AS sbx_position_epoch,NULL::VARCHAR AS sbx_position_source,
          catalog_ids_json,
          'evidence_lake_v2'::VARCHAR AS source_catalog,{sql_literal(build_id)}::VARCHAR AS source_version,
          NULL::VARCHAR AS source_url,NULL::VARCHAR AS source_download_url,NULL::VARCHAR AS source_doi,
          star_id::BIGINT AS source_pk,star_id::BIGINT AS source_row_id,
          sha256(concat_ws('|',stable_object_key,coalesce(selected_fact_id,''),{sql_literal(build_id)})) AS source_row_hash,
          'Spacegate evidence compiler'::VARCHAR AS license,TRUE AS redistribution_ok,
          'Selected-fact compatibility projection; inspect evidence lineage in ARM.'::VARCHAR AS license_note,
          NULL::VARCHAR AS retrieval_etag,NULL::VARCHAR AS retrieval_checksum,
          NULL::VARCHAR AS retrieved_at,{sql_literal(lineage_timestamp)}::VARCHAR AS ingested_at,
          'e7_clean_runtime_core_v1'::VARCHAR AS transform_version,
          CASE classification_value
            WHEN 'WD' THEN 'compact' WHEN 'NS' THEN 'compact' WHEN 'PULSAR' THEN 'compact'
            WHEN 'MAGNETAR' THEN 'compact' WHEN 'BLACK HOLE' THEN 'compact' ELSE 'stellar' END AS object_family,
          CASE classification_value
            WHEN 'WD' THEN 'white_dwarf' WHEN 'NS' THEN 'neutron_star' WHEN 'PULSAR' THEN 'pulsar'
            WHEN 'MAGNETAR' THEN 'magnetar' WHEN 'BLACK HOLE' THEN 'black_hole' ELSE 'star' END AS object_type,
          NULL::DOUBLE AS classprob_dsc_combmod_whitedwarf,
          NULL::DOUBLE AS classprob_dsc_specmod_whitedwarf,
          CASE WHEN classification_value='WD' THEN star_name END AS wd_catalog_name,
          NULL::DOUBLE AS wd_catalog_pwd,NULL::VARCHAR AS wd_catalog_fit_model,
          CASE WHEN classification_value='WD' THEN teff_k END AS wd_catalog_teff_k,
          CASE WHEN classification_value='WD' THEN logg_cgs END AS wd_catalog_logg_cgs,
          CASE WHEN classification_value='WD' THEN mass_msun END AS wd_catalog_mass_msun,
          json_object('classification_value',classification_value,'classification_status',classification_status,
            'evidence_basis',evidence_basis,'selected_fact_id',selected_fact_id,
            'confidence_score',confidence_score,'candidates',candidate_classes_json)::VARCHAR AS classification_evidence_json,
          json_object(
            'parallax_lineage',CASE WHEN selected_parallax_mas IS NOT NULL THEN 'selected_star_fact'
              WHEN system_star_count=1 AND system_parallax_mas IS NOT NULL THEN 'selected_singleton_system_context'
              ELSE 'missing' END,
            'system_placement_source',system_placement_source,
            'system_placement_method',system_placement_method
          )::VARCHAR AS selected_astrometry_lineage_json,
          '[]'::VARCHAR AS open_cluster_tags_json
        FROM joined
        ORDER BY star_id;
        """
    )


def create_systems(
    con: duckdb.DuckDBPyConnection, build_id: str, lineage_timestamp: str
) -> None:
    con.execute(
        f"""
        CREATE TABLE systems AS
        WITH stats AS (
          SELECT system_id,count(*)::BIGINT star_count,count(teff_k)::BIGINT star_teff_count,
            min(teff_k) min_star_teff_k,max(teff_k) max_star_teff_k,
            coalesce(to_json(list(DISTINCT spectral_class ORDER BY spectral_class)
              FILTER (spectral_class IS NOT NULL))::VARCHAR,'[]') spectral_classes_json,
            coalesce(sum(DISTINCT CASE spectral_class WHEN 'O' THEN 1 WHEN 'B' THEN 2 WHEN 'A' THEN 4
              WHEN 'F' THEN 8 WHEN 'G' THEN 16 WHEN 'K' THEN 32 WHEN 'M' THEN 64 WHEN 'L' THEN 128
              WHEN 'T' THEN 256 WHEN 'Y' THEN 512 WHEN 'D' THEN 1024 ELSE 0 END),0)::BIGINT spectral_class_mask,
            max(wds_id) wds_id,max(gaia_id) gaia_id,max(hip_id) hip_id,max(hd_id) hd_id,
            bool_or(has_gaia_nss_evidence) has_gaia_nss_evidence,
            bool_or(has_msc_evidence) has_msc_evidence,bool_or(has_wds_evidence) has_wds_evidence,
            bool_or(has_orb6_evidence) has_orb6_evidence
          FROM stars GROUP BY system_id
        )
        SELECT f.system_id,f.spatial_index,f.stable_object_key,f.system_name,f.system_name_norm,
          s.wds_id,
          CASE WHEN coalesce(s.has_msc_evidence,false) OR coalesce(s.has_wds_evidence,false)
            OR coalesce(s.has_orb6_evidence,false) OR f.star_count>1 THEN 'evidence_graph' ELSE 'singleton' END grouping_basis,
          CASE WHEN f.star_count>1 AND (coalesce(s.has_msc_evidence,false) OR coalesce(s.has_wds_evidence,false)
            OR coalesce(s.has_orb6_evidence,false)) THEN 1.0::DECIMAL(3,2)
            WHEN f.star_count>1 THEN 0.8::DECIMAL(3,2) ELSE 1.0::DECIMAL(3,2) END grouping_confidence,
          CASE WHEN f.star_count>1 THEN 'accepted' ELSE 'singleton' END grouping_confidence_tier,
          json_array(
            CASE WHEN s.has_gaia_nss_evidence THEN 'gaia_nss' END,
            CASE WHEN s.has_msc_evidence THEN 'msc' END,
            CASE WHEN s.has_wds_evidence THEN 'wds' END,
            CASE WHEN s.has_orb6_evidence THEN 'orb6' END
          )::VARCHAR grouping_source_catalogs_json,
          coalesce(s.has_gaia_nss_evidence,false) has_gaia_nss_evidence,
          coalesce(s.has_msc_evidence,false) has_msc_evidence,
          EXISTS(SELECT 1 FROM science.evidence_component_sbx_system_bindings b
            WHERE b.canonical_system_stable_object_key=f.stable_object_key AND b.binding_status='accepted') has_sbx_evidence,
          coalesce(s.has_wds_evidence,false) has_wds_evidence,
          coalesce(s.has_orb6_evidence,false) has_orb6_evidence,
          f.star_count,f.planet_count,coalesce(s.star_teff_count,0) star_teff_count,
          s.min_star_teff_k,s.max_star_teff_k,coalesce(s.spectral_classes_json,'[]') spectral_classes_json,
          coalesce(s.spectral_class_mask,0) spectral_class_mask,
          f.ra_deg,f.dec_deg,f.dist_ly,f.x_helio_ly,f.y_helio_ly,f.z_helio_ly,
          NULL::DOUBLE x_gal_ly,NULL::DOUBLE y_gal_ly,NULL::DOUBLE z_gal_ly,
          s.gaia_id,s.hip_id,s.hd_id,
          'evidence_lake_v2'::VARCHAR source_catalog,{sql_literal(build_id)}::VARCHAR source_version,
          NULL::VARCHAR source_url,NULL::VARCHAR source_download_url,NULL::VARCHAR source_doi,
          f.system_id::BIGINT source_pk,f.system_id::BIGINT source_row_id,
          sha256(concat_ws('|',f.stable_object_key,f.placement_source,{sql_literal(build_id)})) source_row_hash,
          'Spacegate evidence compiler'::VARCHAR license,TRUE redistribution_ok,
          'Permanent identity plus selected-system placement.'::VARCHAR license_note,
          NULL::VARCHAR retrieval_etag,NULL::VARCHAR retrieval_checksum,NULL::VARCHAR retrieved_at,
          {sql_literal(lineage_timestamp)}::VARCHAR ingested_at,'e7_clean_runtime_core_v1'::VARCHAR transform_version
        FROM foundation_systems f LEFT JOIN stats s USING (system_id)
        ORDER BY f.system_id;
        """
    )


def create_planets(
    con: duckdb.DuckDBPyConnection,
    build_id: str,
    lineage_timestamp: str,
    classification: dict[str, Any],
) -> None:
    terrestrial_radius = float(classification["terrestrial_max_radius_earth"])
    jupiter_radius = float(classification["jupiter_min_radius_earth"])
    terrestrial_mass = float(classification["terrestrial_max_mass_earth"])
    jupiter_mass = float(classification["jupiter_min_mass_earth"])
    hot_temperature = float(classification["hot_min_equilibrium_temperature_k"])
    temperate_temperature = float(classification["temperate_min_equilibrium_temperature_k"])
    insolation_scale = float(classification["insolation_temperature_scale_k"])
    con.execute(
        f"""
        CREATE TABLE planets AS
        WITH lifecycle_ranked AS (
          SELECT canonical_planet_id,
            disposition_normalized,evidence_polarity,source_id,effective_at_raw,quality_json,
            row_number() OVER (PARTITION BY canonical_planet_id ORDER BY
              CASE disposition_normalized WHEN 'RETRACTED' THEN 0 WHEN 'CONFIRMED' THEN 1
                WHEN 'CANDIDATE' THEN 2 WHEN 'CONTROVERSIAL' THEN 3 ELSE 4 END,
              effective_at_raw DESC NULLS LAST,evidence_id) choice_rank
          FROM science.evidence_planet_evidence_planet_lifecycle_projection
          WHERE binding_status='accepted' AND canonical_planet_id IS NOT NULL
        ), lifecycle AS (
          SELECT * FROM lifecycle_ranked WHERE choice_rank=1
        ), identifiers AS (
          SELECT target_id AS star_id,
            max(try_cast(id_value_norm AS BIGINT)) FILTER (namespace='gaia_dr3') host_gaia_id,
            max(try_cast(id_value_norm AS BIGINT)) FILTER (namespace='hip') host_hip_id,
            max(try_cast(id_value_norm AS BIGINT)) FILTER (namespace='hd') host_hd_id
          FROM foundation_object_identifiers WHERE target_type='star' GROUP BY target_id
        ), selected AS (
          SELECT f.*,sp.orbital_period_days,sp.semi_major_axis_au,sp.eccentricity,sp.inclination_deg,
            coalesce(sp.radius_earth,sp.radius_jup*11.209) radius_earth_selected,
            coalesce(sp.radius_jup,sp.radius_earth/11.209) radius_jup_selected,
            coalesce(sp.best_mass_earth,sp.mass_earth,sp.minimum_mass_earth,
              sp.best_mass_jup*317.83,sp.mass_jup*317.83,sp.minimum_mass_jup*317.83) mass_earth_selected,
            coalesce(sp.best_mass_jup,sp.mass_jup,sp.minimum_mass_jup,
              sp.best_mass_earth/317.83,sp.mass_earth/317.83,sp.minimum_mass_earth/317.83) mass_jup_selected,
            sp.eq_temp_k,sp.insol_earth,l.disposition_normalized,l.source_id lifecycle_source,
            l.effective_at_raw,l.quality_json lifecycle_quality,
            s.star_name host_name,sys.x_helio_ly,sys.y_helio_ly,sys.z_helio_ly,
            i.host_gaia_id,i.host_hip_id,i.host_hd_id,phys.metallicity_m_h
          FROM foundation_planets f
          LEFT JOIN science.selected_planet_parameters sp USING (planet_id)
          LEFT JOIN lifecycle l ON l.canonical_planet_id=try_cast(f.planet_id AS BIGINT)
          LEFT JOIN foundation_stars s ON s.star_id=f.star_id
          LEFT JOIN foundation_systems sys ON sys.system_id=f.system_id
          LEFT JOIN identifiers i ON i.star_id=f.star_id
          LEFT JOIN science.selected_stellar_physics phys ON phys.star_id=f.star_id
        ), categorized AS (
          SELECT *,
            CASE
              WHEN radius_earth_selected IS NOT NULL AND radius_earth_selected<={terrestrial_radius} THEN 'terrestrial'
              WHEN radius_earth_selected IS NOT NULL AND radius_earth_selected>={jupiter_radius} THEN 'jupiter'
              WHEN radius_earth_selected IS NULL AND mass_earth_selected<={terrestrial_mass} THEN 'terrestrial'
              WHEN radius_earth_selected IS NULL AND mass_earth_selected>={jupiter_mass} THEN 'jupiter'
            END composition_class,
            coalesce(eq_temp_k,CASE WHEN insol_earth>0 THEN {insolation_scale}*pow(insol_earth,0.25) END)
              classification_temperature_k
          FROM selected
        )
        SELECT planet_id,spatial_index,stable_object_key,system_id,star_id,planet_name,planet_name_norm,
          try_cast(json_extract_string(lifecycle_quality,'$.source_context.discovered') AS INTEGER) disc_year,
          json_extract_string(lifecycle_quality,'$.source_context.detection_type') discovery_method,
          NULL::VARCHAR discovery_facility,NULL::VARCHAR discovery_telescope,NULL::VARCHAR discovery_instrument,
          orbital_period_days,semi_major_axis_au,eccentricity,inclination_deg,
          radius_jup_selected radius_jup,radius_earth_selected radius_earth,
          mass_earth_selected mass_earth,mass_jup_selected mass_jup,eq_temp_k,insol_earth,
          metallicity_m_h host_metallicity_feh,NULL::DOUBLE host_metallicity_feh_error,
          host_name host_name_raw,lower(host_name) host_name_norm,
          host_gaia_id,host_hip_id,host_hd_id,
          CASE WHEN star_id IS NOT NULL THEN 'permanent_identity_host_binding'
            WHEN system_id IS NOT NULL THEN 'permanent_identity_system_binding' ELSE 'unbound_preserved' END match_method,
          CASE WHEN star_id IS NOT NULL THEN 1.0 WHEN system_id IS NOT NULL THEN 0.8 ELSE 0.0 END::DECIMAL(3,2) match_confidence,
          CASE WHEN star_id IS NULL THEN 'Canonical planet identity retained without an accepted host-star binding.' END match_notes,
          x_helio_ly,y_helio_ly,z_helio_ly,
          'evidence_lake_v2' source_catalog,{sql_literal(build_id)} source_version,
          NULL::VARCHAR source_url,NULL::VARCHAR source_download_url,NULL::VARCHAR source_doi,
          planet_id::BIGINT source_pk,planet_id::BIGINT source_row_id,
          sha256(concat_ws('|',stable_object_key,coalesce(lifecycle_source,''),{sql_literal(build_id)})) source_row_hash,
          'Spacegate evidence compiler' license,TRUE redistribution_ok,
          'Selected planet facts and lifecycle evidence.' license_note,
          NULL::VARCHAR retrieval_etag,NULL::VARCHAR retrieval_checksum,NULL::VARCHAR retrieved_at,
          {sql_literal(lineage_timestamp)} ingested_at,'e7_clean_runtime_core_v1' transform_version,
          CASE disposition_normalized WHEN 'RETRACTED' THEN 'retracted' WHEN 'CANDIDATE' THEN 'candidate'
            WHEN 'CONTROVERSIAL' THEN 'controversial' ELSE 'confirmed' END planet_status,
          (coalesce(disposition_normalized,'CONFIRMED') NOT IN ('RETRACTED','CONTROVERSIAL')) is_default_visible,
          (disposition_normalized='RETRACTED') is_tombstoned,
          lifecycle_source status_source_catalog,effective_at_raw status_updated_at,NULL::VARCHAR status_superseded_by,
          composition_class planet_size_mass_class,
          CASE WHEN classification_temperature_k>{hot_temperature} THEN 'hot'
            WHEN classification_temperature_k>={temperate_temperature} THEN 'temperate'
            WHEN classification_temperature_k IS NOT NULL THEN 'cold' END planet_insolation_class,
          CASE WHEN composition_class IS NOT NULL AND classification_temperature_k>{hot_temperature}
              THEN 'hot_'||composition_class
            WHEN composition_class IS NOT NULL AND classification_temperature_k>={temperate_temperature}
              THEN 'temperate_'||composition_class
            WHEN composition_class IS NOT NULL AND classification_temperature_k IS NOT NULL
              THEN 'cold_'||composition_class END planet_orbit_class,
          composition_class planet_composition_proxy_class,
          CASE WHEN json_extract_string(lifecycle_quality,'$.source_context.detection_type') IS NULL THEN '[]'
            ELSE to_json([lower(replace(json_extract_string(
              lifecycle_quality,'$.source_context.detection_type'),' ','_'))])::VARCHAR END planet_detection_tags_json,
          CASE WHEN star_id IS NULL THEN '["unbound_host"]' ELSE '[]' END planet_host_context_tags_json,
          'planet_map_category_v1' planet_classifier_version,{sql_literal(lineage_timestamp)} planet_classifier_updated_at,
          NULL::DOUBLE spacegate_hab_score,NULL::DOUBLE spacegate_hab_confidence,
          NULL::VARCHAR spacegate_hab_reasons_json,
          NULL::DOUBLE planet_element_richness_score,NULL::VARCHAR planet_element_richness_class,
          NULL::VARCHAR planet_element_richness_method,NULL::VARCHAR planet_element_richness_notes
        FROM categorized
        ORDER BY planet_id;
        """
    )


def create_extended_and_cluster_tables(
    con: duckdb.DuckDBPyConnection, build_id: str, lineage_timestamp: str
) -> None:
    con.execute(
        f"""
        CREATE TABLE extended_objects AS SELECT * FROM extended_extended_objects ORDER BY extended_object_id;

        CREATE TABLE open_clusters AS
        WITH selected AS (
          SELECT *,row_number() OVER (PARTITION BY canonical_cluster_stable_object_key
            ORDER BY authority_rank,evidence_id) choice_rank
          FROM cluster_cluster_evidence_projection
          WHERE projection_status='eligible_for_quantity_selection'
        ), membership AS (
          SELECT canonical_cluster_stable_object_key,
            count(*) FILTER (membership_probability>0.7)::BIGINT member_count_prob_gt_0_7
          FROM cluster_cluster_membership_projection
          WHERE projection_status='probability_bearing_membership_evidence'
          GROUP BY 1
        )
        SELECT e.extended_object_id cluster_id,e.stable_object_key,
          e.display_name cluster_name,e.ra_deg,e.dec_deg,
          try_cast(json_extract_string(s.parameter_set_raw,'$.GLON') AS DOUBLE) glon_deg,
          try_cast(json_extract_string(s.parameter_set_raw,'$.GLAT') AS DOUBLE) glat_deg,
          try_cast(json_extract_string(s.parameter_set_raw,'$.r50') AS DOUBLE) radius_r50_deg,
          coalesce(m.member_count_prob_gt_0_7,0) member_count_prob_gt_0_7,
          try_cast(json_extract_string(s.parameter_set_raw,'$.pmRA') AS DOUBLE) pm_ra_mas_yr,
          try_cast(json_extract_string(s.parameter_set_raw,'$.pmDE') AS DOUBLE) pm_dec_mas_yr,
          try_cast(json_extract_string(s.parameter_set_raw,'$.Plx') AS DOUBLE) parallax_mas,
          e.dist_pc,e.dist_ly,
          s.source_id source_flag,s.source_id source_catalog,s.release_id source_version,
          NULL::VARCHAR source_url,NULL::VARCHAR source_download_url,NULL::VARCHAR source_doi,
          e.extended_object_id source_pk,e.extended_object_id source_row_id,
          sha256(concat_ws('|',e.stable_object_key,s.evidence_id,{sql_literal(build_id)})) source_row_hash,
          'Source catalog terms' license,TRUE redistribution_ok,
          'Clean release-scoped cluster evidence projection.' license_note,
          NULL::VARCHAR retrieval_etag,NULL::VARCHAR retrieval_checksum,NULL::VARCHAR retrieved_at,
          {sql_literal(lineage_timestamp)} ingested_at,'e7_clean_runtime_core_v1' transform_version
        FROM selected s
        JOIN extended_extended_objects e ON e.stable_object_key=s.canonical_cluster_stable_object_key
        LEFT JOIN membership m USING (canonical_cluster_stable_object_key)
        WHERE s.choice_rank=1
        ORDER BY e.extended_object_id;

        CREATE TABLE open_cluster_memberships AS
        SELECT row_number() OVER (ORDER BY p.source_id,p.release_id,p.source_record_id)::BIGINT cluster_membership_id,
          c.cluster_id,c.cluster_name,sys.system_id,st.star_id,
          try_cast(regexp_extract(p.member_identity_raw,'[0-9]+',0) AS BIGINT) gaia_id,
          p.membership_probability,
          'release_scoped_identity_graph' match_method,
          CASE WHEN p.member_binding_status='accepted' THEN 1.0 ELSE 0.0 END::DECIMAL(2,1) match_confidence
        FROM cluster_cluster_membership_projection p
        JOIN open_clusters c ON c.stable_object_key=p.canonical_cluster_stable_object_key
        LEFT JOIN foundation_systems sys ON sys.stable_object_key=p.member_system_stable_object_key
        LEFT JOIN foundation_stars st ON st.stable_object_key=p.member_stable_object_key
        WHERE p.projection_status='probability_bearing_membership_evidence'
        ORDER BY 1;

        CREATE TABLE superstellar_objects AS
        SELECT row_number() OVER (ORDER BY extended_object_id)::BIGINT superstellar_object_id,
          stable_object_key,object_family,object_type,display_name object_name,
          ra_deg,dec_deg,dist_pc,dist_ly,
          json_object('extended_object_id',extended_object_id,'map_domain',map_domain,
            'geometry_status',geometry_status,'distance_method',distance_method) object_meta_json,
          source_catalog,source_version,source_url,source_download_url,
          try_cast(source_doi AS VARCHAR) source_doi,extended_object_id source_pk,
          try_cast(source_row_id AS BIGINT) source_row_id,source_row_hash,license,redistribution_ok,
          license_note,try_cast(retrieval_etag AS VARCHAR) retrieval_etag,retrieval_checksum,
          retrieved_at,ingested_at,transform_version
        FROM extended_extended_objects e
        WHERE e.object_type='supernova_remnant'
           OR EXISTS (SELECT 1 FROM open_clusters c WHERE c.stable_object_key=e.stable_object_key)
        ORDER BY extended_object_id;
        """
    )


def create_compact_objects(
    con: duckdb.DuckDBPyConnection, build_id: str, lineage_timestamp: str
) -> None:
    con.execute(
        f"""
        CREATE TABLE compact_objects AS
        WITH canonical_compact AS (
          SELECT stable_object_key,system_id,star_id,star_name object_name,object_family,object_type,
            ra_deg,dec_deg,dist_ly,dist_ly/3.26156 dist_pc,parallax_mas,
            'selected_display_classification' match_method,1.0::DECIMAL(3,2) match_confidence,
            NULL::DOUBLE match_angular_distance_arcsec,NULL::DOUBLE match_distance_delta_ly,
            catalog_ids_json,'evidence_lake_v2' source_catalog,{sql_literal(build_id)} source_version,
            source_row_hash
          FROM stars WHERE object_family='compact'
        ), external_compact AS (
          SELECT n.stable_object_key,NULL::HUGEINT system_id,NULL::HUGEINT star_id,n.display_name object_name,
            n.object_family,n.object_type,o.ra_deg,o.dec_deg,o.distance_pc*3.26156 dist_ly,o.distance_pc dist_pc,
            o.parallax_mas,'release_scoped_compact_identity' match_method,
            CASE WHEN o.outcome='accepted' THEN 1.0 ELSE 0.0 END::DECIMAL(3,2) match_confidence,
            NULL::DOUBLE match_angular_distance_arcsec,NULL::DOUBLE match_distance_delta_ly,
            json_object(n.inventory_namespace,n.inventory_identifier) catalog_ids_json,
            n.source_id source_catalog,n.release_id source_version,
            sha256(concat_ws('|',n.stable_object_key,o.outcome,{sql_literal(build_id)})) source_row_hash
          FROM science.evidence_compact_compact_identity_nodes n
          JOIN science.evidence_compact_compact_envelope_outcomes o USING (object_node_key)
          WHERE NOT EXISTS (SELECT 1 FROM stars s WHERE s.stable_object_key=n.stable_object_key)
        ), combined AS (
          SELECT * FROM canonical_compact UNION ALL SELECT * FROM external_compact
        )
        SELECT row_number() OVER (ORDER BY stable_object_key)::BIGINT compact_object_id,
          stable_object_key,system_id,star_id,object_name,object_family,object_type,ra_deg,dec_deg,
          dist_ly,dist_pc,parallax_mas,match_method,match_confidence,match_angular_distance_arcsec,
          match_distance_delta_ly,catalog_ids_json,source_catalog,source_version,
          NULL::VARCHAR source_url,NULL::VARCHAR source_download_url,NULL::VARCHAR source_doi,
          row_number() OVER (ORDER BY stable_object_key)::BIGINT source_pk,
          row_number() OVER (ORDER BY stable_object_key)::BIGINT source_row_id,source_row_hash,
          'Source catalog terms' license,TRUE redistribution_ok,
          'Canonical compact stars plus separately scoped compact evidence objects.' license_note,
          NULL::VARCHAR retrieval_etag,NULL::VARCHAR retrieval_checksum,NULL::VARCHAR retrieved_at,
          {sql_literal(lineage_timestamp)} ingested_at,'e7_clean_runtime_core_v1' transform_version
        FROM combined ORDER BY stable_object_key;
        """
    )


def create_eclipsing_binaries(
    con: duckdb.DuckDBPyConnection, build_id: str, lineage_timestamp: str
) -> None:
    con.execute(
        f"""
        CREATE TABLE eclipsing_binaries AS
        WITH orbit AS (
          SELECT source_record_id,
            try_cast(json_extract_string(parameter_set_raw,'$.period_days') AS DOUBLE) period_days,
            try_cast(json_extract_string(parameter_set_raw,'$.period_error_days') AS DOUBLE) period_error_days,
            try_cast(json_extract_string(parameter_set_raw,'$.bjd0') AS DOUBLE) bjd0,
            try_cast(json_extract_string(parameter_set_raw,'$.bjd0_error') AS DOUBLE) bjd0_error,
            try_cast(json_extract_string(parameter_set_raw,'$.morphology') AS DOUBLE) morphology
          FROM science.evidence_component_tess_eb_orbital_solution_projection
        ), parameters AS (
          SELECT source_record_id,
            max(normalized_value) FILTER (quantity_key='effective_temperature') teff_k,
            max(normalized_value) FILTER (quantity_key='metallicity_m_h') metallicity_dex
          FROM science.evidence_component_tess_eb_stellar_parameter_projection GROUP BY 1
        ), photometry AS (
          SELECT source_record_id,max(normalized_value) FILTER (quantity_key='apparent_magnitude') kmag
          FROM science.evidence_component_tess_eb_photometry_projection GROUP BY 1
        )
        SELECT row_number() OVER (ORDER BY b.source_record_id)::BIGINT eclipsing_binary_id,
          ('evidence:tess_eb:'||b.source_record_id) stable_object_key,b.tic_id_normalized source_catalog_object_id,
          coalesce(s.star_name,sys.system_name,'TIC '||b.tic_id_normalized) object_name,
          s.star_id,sys.system_id,b.binding_method match_method,
          CASE b.binding_status WHEN 'accepted' THEN 1.0 ELSE 0.0 END::DECIMAL(2,1) match_confidence,
          o.period_days,o.period_error_days,o.bjd0,o.bjd0_error,o.morphology,
          NULL::DOUBLE glon_deg,NULL::DOUBLE glat_deg,p.kmag,sp.teff_k,
          NULL::VARCHAR spectral_type_primary,NULL::VARCHAR spectral_type_secondary,
          NULL::DOUBLE mass_primary_msun,NULL::DOUBLE mass_primary_err_msun,
          NULL::DOUBLE mass_secondary_msun,NULL::DOUBLE mass_secondary_err_msun,
          NULL::DOUBLE radius_primary_rsun,NULL::DOUBLE radius_primary_err_rsun,
          NULL::DOUBLE radius_secondary_rsun,NULL::DOUBLE radius_secondary_err_rsun,
          NULL::DOUBLE logg_primary_cgs,NULL::DOUBLE logg_primary_err_cgs,
          NULL::DOUBLE logg_secondary_cgs,NULL::DOUBLE logg_secondary_err_cgs,
          NULL::DOUBLE teff_primary_k,NULL::DOUBLE teff_primary_err_k,
          NULL::DOUBLE teff_secondary_k,NULL::DOUBLE teff_secondary_err_k,
          NULL::DOUBLE lum_primary_lsun,NULL::DOUBLE lum_primary_err_lsun,
          NULL::DOUBLE lum_secondary_lsun,NULL::DOUBLE lum_secondary_err_lsun,
          sp.metallicity_dex,NULL::DOUBLE metallicity_err_dex,FALSE has_short_cadence,
          b.source_id source_catalog,b.release_id source_version,
          NULL::VARCHAR source_url,NULL::VARCHAR source_download_url,NULL::VARCHAR source_doi,
          row_number() OVER (ORDER BY b.source_record_id)::BIGINT source_pk,
          row_number() OVER (ORDER BY b.source_record_id)::BIGINT source_row_id,
          sha256(concat_ws('|',b.source_record_id,b.binding_status,{sql_literal(build_id)})) source_row_hash,
          'Source catalog terms' license,TRUE redistribution_ok,
          'TESS EB evidence; missing canonical targets remain explicitly unbound.' license_note,
          NULL::VARCHAR retrieval_etag,NULL::VARCHAR retrieval_checksum,NULL::VARCHAR retrieved_at,
          {sql_literal(lineage_timestamp)} ingested_at,'e7_clean_runtime_core_v1' transform_version
        FROM science.evidence_component_tess_eb_target_bindings b
        LEFT JOIN foundation_stars s ON s.stable_object_key=b.canonical_stable_object_key
        LEFT JOIN foundation_systems sys ON sys.stable_object_key=b.canonical_system_stable_object_key
        LEFT JOIN orbit o USING (source_record_id)
        LEFT JOIN parameters sp USING (source_record_id)
        LEFT JOIN photometry p USING (source_record_id)
        ORDER BY b.source_record_id;
        """
    )


EXPORT_ORDER = {
    "aliases": "alias_id",
    "build_metadata": "key",
    "compact_objects": "compact_object_id",
    "eclipsing_binaries": "eclipsing_binary_id",
    "extended_object_aliases": "extended_object_alias_id",
    "extended_object_identifiers": "extended_object_identifier_id",
    "extended_object_identity_quarantine": "extended_object_quarantine_id",
    "extended_object_search_terms": "extended_object_search_term_id",
    "extended_object_source_reconciliation": "extended_object_reconciliation_id",
    "extended_objects": "extended_object_id",
    "identifier_quarantine": "quarantine_key",
    "object_identifiers": "identifier_id",
    "open_cluster_memberships": "cluster_membership_id",
    "open_clusters": "cluster_id",
    "planets": "planet_id",
    "stars": "star_id",
    "superstellar_objects": "superstellar_object_id",
    "system_search_terms": "system_id,target_type,target_id,term_norm",
    "systems": "system_id",
}


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE UNIQUE INDEX systems_id_uq ON systems(system_id);
        CREATE UNIQUE INDEX systems_key_uq ON systems(stable_object_key);
        CREATE INDEX systems_name_norm_idx ON systems(system_name_norm);
        CREATE UNIQUE INDEX stars_id_uq ON stars(star_id);
        CREATE UNIQUE INDEX stars_key_uq ON stars(stable_object_key);
        CREATE INDEX stars_system_idx ON stars(system_id);
        CREATE INDEX stars_name_norm_idx ON stars(star_name_norm);
        CREATE UNIQUE INDEX planets_id_uq ON planets(planet_id);
        CREATE UNIQUE INDEX planets_key_uq ON planets(stable_object_key);
        CREATE INDEX planets_system_idx ON planets(system_id);
        CREATE INDEX planets_star_idx ON planets(star_id);
        CREATE INDEX planets_name_norm_idx ON planets(planet_name_norm);
        CREATE INDEX aliases_norm_idx ON aliases(alias_norm);
        CREATE INDEX aliases_system_idx ON aliases(system_id);
        CREATE INDEX identifiers_namespace_value_idx ON object_identifiers(namespace,id_value_norm);
        CREATE INDEX search_terms_norm_idx ON system_search_terms(term_norm);
        CREATE INDEX search_terms_system_idx ON system_search_terms(system_id);
        CREATE UNIQUE INDEX extended_objects_id_uq ON extended_objects(extended_object_id);
        CREATE UNIQUE INDEX extended_objects_key_uq ON extended_objects(stable_object_key);
        CREATE INDEX extended_search_norm_idx ON extended_object_search_terms(term_norm);
        CREATE INDEX compact_system_idx ON compact_objects(system_id);
        CREATE INDEX eb_system_idx ON eclipsing_binaries(system_id);
        CREATE INDEX cluster_membership_system_idx ON open_cluster_memberships(system_id);
        """
    )


def verification_report(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    counts = {
        table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
        for table in EXPORT_ORDER
    }
    checks = {
        "system_inventory_delta": int(con.execute(
            "SELECT (SELECT count(*) FROM systems)-(SELECT count(*) FROM foundation_systems)"
        ).fetchone()[0]),
        "star_inventory_delta": int(con.execute(
            "SELECT (SELECT count(*) FROM stars)-(SELECT count(*) FROM foundation_stars)"
        ).fetchone()[0]),
        "planet_inventory_delta": int(con.execute(
            "SELECT (SELECT count(*) FROM planets)-(SELECT count(*) FROM foundation_planets)"
        ).fetchone()[0]),
        "duplicate_system_ids": int(con.execute(
            "SELECT count(*) FROM (SELECT system_id FROM systems GROUP BY 1 HAVING count(*)<>1)"
        ).fetchone()[0]),
        "duplicate_star_ids": int(con.execute(
            "SELECT count(*) FROM (SELECT star_id FROM stars GROUP BY 1 HAVING count(*)<>1)"
        ).fetchone()[0]),
        "duplicate_planet_ids": int(con.execute(
            "SELECT count(*) FROM (SELECT planet_id FROM planets GROUP BY 1 HAVING count(*)<>1)"
        ).fetchone()[0]),
        "orphan_stars": int(con.execute(
            "SELECT count(*) FROM stars s LEFT JOIN systems y USING(system_id) WHERE y.system_id IS NULL"
        ).fetchone()[0]),
        "bound_planets_with_missing_system": int(con.execute(
            "SELECT count(*) FROM planets p LEFT JOIN systems y USING(system_id) "
            "WHERE p.system_id IS NOT NULL AND y.system_id IS NULL"
        ).fetchone()[0]),
        "selected_teff_mismatches": int(con.execute(
            "SELECT count(*) FROM stars s JOIN science.selected_stellar_parameters p USING(star_id) "
            "WHERE s.teff_k IS DISTINCT FROM p.teff_k"
        ).fetchone()[0]),
        "selected_display_class_mismatches": int(con.execute(
            "SELECT count(*) FROM stars s JOIN science.selected_stellar_display_classifications d USING(star_id) "
            "WHERE s.spectral_class IS DISTINCT FROM CASE d.classification_value WHEN 'WD' THEN 'D' ELSE d.classification_value END"
        ).fetchone()[0]),
        "selected_planet_period_mismatches": int(con.execute(
            "SELECT count(*) FROM planets p JOIN science.selected_planet_parameters e USING(planet_id) "
            "WHERE p.orbital_period_days IS DISTINCT FROM e.orbital_period_days"
        ).fetchone()[0]),
        "tess_eb_accounting_delta": int(con.execute(
            "SELECT (SELECT count(*) FROM eclipsing_binaries)-"
            "(SELECT count(*) FROM science.evidence_component_tess_eb_target_bindings)"
        ).fetchone()[0]),
        "superstellar_projection_delta": int(con.execute(
            "SELECT (SELECT count(*) FROM superstellar_objects)-"
            "((SELECT count(*) FROM open_clusters)+"
            "(SELECT count(*) FROM extended_objects WHERE object_type='supernova_remnant'))"
        ).fetchone()[0]),
        "cluster_membership_accounting_delta": int(con.execute(
            "SELECT (SELECT count(*) FROM open_cluster_memberships)-"
            "(SELECT count(*) FROM cluster_cluster_membership_projection "
            "WHERE projection_status='probability_bearing_membership_evidence')"
        ).fetchone()[0]),
        "cluster_membership_containment_promotions": int(con.execute(
            "SELECT count(*) FROM cluster_cluster_membership_projection WHERE canonical_containment_promotion"
        ).fetchone()[0]),
        "invalid_planet_categories": int(con.execute(
            "SELECT count(*) FROM planets WHERE planet_size_mass_class NOT IN ('terrestrial','jupiter') "
            "OR planet_insolation_class NOT IN ('hot','temperate','cold')"
        ).fetchone()[0]),
        "stability_metadata_opened": int(con.execute(
            "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"
        ).fetchone()[0]),
    }
    null_compatibility_fields = {
        "systems.x_gal_ly": int(con.execute("SELECT count(*) FROM systems WHERE x_gal_ly IS NULL").fetchone()[0]),
        "stars.x_gal_ly": int(con.execute("SELECT count(*) FROM stars WHERE x_gal_ly IS NULL").fetchone()[0]),
        "stars.vmag": int(con.execute("SELECT count(*) FROM stars WHERE vmag IS NULL").fetchone()[0]),
        "stars.wd_probability": int(con.execute(
            "SELECT count(*) FROM stars WHERE object_type='white_dwarf' AND wd_catalog_pwd IS NULL"
        ).fetchone()[0]),
        "planets.discovery_facility": int(con.execute(
            "SELECT count(*) FROM planets WHERE discovery_facility IS NULL"
        ).fetchone()[0]),
    }
    failing = {key: value for key, value in checks.items() if value}
    return {
        "counts": counts,
        "checks": checks,
        "failing_checks": failing,
        "compatibility_null_accounting": null_compatibility_fields,
        "status": "pass" if not failing else "fail",
    }


def export_tables(
    con: duckdb.DuckDBPyConnection, parquet_dir: Path
) -> dict[str, dict[str, Any]]:
    products: dict[str, dict[str, Any]] = {}
    for table, order in EXPORT_ORDER.items():
        path = parquet_dir / f"{table}.parquet"
        con.execute(
            f"COPY (SELECT * FROM {table} ORDER BY {order}) TO {sql_literal(path)} "
            "(FORMAT PARQUET,COMPRESSION ZSTD)"
        )
        products[f"parquet/{path.name}"] = {
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
            "determinism": "byte_exact",
        }
    return products


def update_hierarchy_metadata(
    hierarchy_path: Path,
    build_id: str,
    policy: dict[str, Any],
    foundation_build_id: str,
) -> None:
    con = duckdb.connect(str(hierarchy_path))
    try:
        con.execute("DELETE FROM build_metadata")
        con.executemany(
            "INSERT INTO build_metadata VALUES (?,?)",
            [
                ("build_id", build_id),
                ("build_kind", "e7_clean_runtime_core"),
                ("policy_version", policy["policy_version"]),
                ("foundation_build_id", foundation_build_id),
                ("stability_database_opened", "0"),
            ],
        )
        con.execute("CHECKPOINT")
    finally:
        con.close()


def database_products(core_db: Path, hierarchy_db: Path) -> dict[str, dict[str, Any]]:
    return {
        "core.duckdb": {
            "bytes": core_db.stat().st_size,
            "sha256": file_sha256(core_db),
            "determinism": "logical_tables",
        },
        "canonical_hierarchy.duckdb": {
            "bytes": hierarchy_db.stat().st_size,
            "sha256": file_sha256(hierarchy_db),
            "determinism": "logical_tables",
        },
    }


def compile_runtime_core(
    policy_path: Path,
    state: Path,
    output_root: Path,
    *,
    link_into_state: bool,
) -> dict[str, Any]:
    timing = Timings()
    policy = load_object(policy_path)
    validate_policy(policy)
    inputs = timing.run("validate_input_manifests", lambda: resolve_inputs(policy, state))

    foundation_names = (
        "aliases", "identifier_quarantine", "object_identifiers", "planets", "stars",
        "system_search_terms", "systems",
    )
    foundation = {
        name: timing.run(
            f"verify_foundation_{name}",
            lambda name=name: product_path(inputs["clean_foundation"], f"parquet/{name}.parquet"),
        )
        for name in foundation_names
    }
    hierarchy_db = timing.run(
        "verify_foundation_hierarchy",
        lambda: product_path(inputs["clean_foundation"], "canonical_hierarchy.duckdb"),
    )
    science_db = timing.run(
        "verify_clean_science",
        lambda: product_path(inputs["clean_science"], "clean_science.duckdb"),
    )
    cluster_names = (
        "cluster_evidence_projection", "cluster_membership_projection",
    )
    clusters = {
        name: timing.run(
            f"verify_cluster_{name}",
            lambda name=name: product_path(inputs["clean_clusters"], f"parquet/{name}.parquet"),
        )
        for name in cluster_names
    }
    extended_names = (
        "extended_object_aliases", "extended_object_identifiers",
        "extended_object_identity_quarantine", "extended_object_search_terms",
        "extended_object_source_reconciliation", "extended_objects",
    )
    extended = {
        name: timing.run(
            f"verify_extended_{name}",
            lambda name=name: product_path(inputs["clean_extended_objects"], f"parquet/{name}.parquet"),
        )
        for name in extended_names
    }
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    input_identity = {
        name: {"build_id": spec["build_id"], "manifest_sha256": spec["manifest_sha256"]}
        for name, spec in inputs.items()
    }
    lineage_timestamp = max(
        str(spec["manifest"].get("generated_at") or "1970-01-01T00:00:00Z")
        for spec in inputs.values()
    )
    build_id = stable_hash({
        "compiler_sha256": compiler_sha,
        "policy_sha256": policy_sha,
        "inputs": input_identity,
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("clean runtime CORE build collision")
        return manifest

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    core_db = staging / "core.duckdb"
    runtime_hierarchy = staging / "canonical_hierarchy.duckdb"
    parquet_dir = staging / "parquet"
    parquet_dir.mkdir()
    try:
        con = duckdb.connect(str(core_db))
        configure(con, staging)
        try:
            timing.run(
                "attach_clean_inputs",
                lambda: create_input_views(con, foundation, science_db, hierarchy_db, clusters, extended),
            )
            timing.run("identity_tables", lambda: create_identity_tables(con, build_id, policy))
            timing.run("identifier_projection", lambda: create_identifier_projection(con))
            timing.run(
                "stellar_projection",
                lambda: create_stars(con, build_id, lineage_timestamp),
            )
            timing.run(
                "system_projection",
                lambda: create_systems(con, build_id, lineage_timestamp),
            )
            timing.run(
                "planet_projection",
                lambda: create_planets(
                    con, build_id, lineage_timestamp, policy["planet_classification"]
                ),
            )
            timing.run(
                "extended_cluster_projection",
                lambda: create_extended_and_cluster_tables(
                    con, build_id, lineage_timestamp
                ),
            )
            timing.run(
                "compact_projection",
                lambda: create_compact_objects(con, build_id, lineage_timestamp),
            )
            timing.run(
                "tess_eb_projection",
                lambda: create_eclipsing_binaries(con, build_id, lineage_timestamp),
            )
            verification = timing.run("internal_verification", lambda: verification_report(con))
            if verification["status"] != "pass":
                raise ValueError(f"clean runtime CORE verification failed: {verification['failing_checks']}")
            timing.run("indexes", lambda: create_indexes(con))
            products = timing.run("parquet_export", lambda: export_tables(con, parquet_dir))
            timing.run("core_checkpoint", lambda: con.execute("CHECKPOINT"))
        finally:
            con.close()

        timing.run("hierarchy_copy", lambda: shutil.copy2(hierarchy_db, runtime_hierarchy))
        timing.run(
            "hierarchy_metadata_checkpoint",
            lambda: update_hierarchy_metadata(
                runtime_hierarchy,
                build_id,
                policy,
                inputs["clean_foundation"]["build_id"],
            ),
        )
        products.update(
            timing.run(
                "database_hashing",
                lambda: database_products(core_db, runtime_hierarchy),
            )
        )
        manifest = {
            "schema_version": "spacegate.e7_clean_runtime_core_manifest.v1",
            "build_id": build_id,
            "status": "pass",
            "generated_at": utc_now(),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha,
            "compiler_sha256": compiler_sha,
            "inputs": input_identity,
            "stability_databases_opened": [],
            "verification": verification,
            "products": products,
            "performance": timing.report(),
        }
        write_object_atomic(staging / "manifest.json", manifest)
        os.replace(staging, final_dir)
        if link_into_state:
            link_root = state / "derived/evidence_lake_v2/clean_runtime_core"
            link_root.mkdir(parents=True, exist_ok=True)
            link = link_root / build_id
            if link.is_symlink() or link.exists():
                if link.resolve() != final_dir.resolve():
                    raise ValueError(f"clean runtime CORE state link collision: {link}")
            else:
                link.symlink_to(final_dir)
        return manifest
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_runtime_core(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps({
        "build_id": manifest["build_id"],
        "status": manifest["status"],
        "counts": manifest["verification"]["counts"],
        "wall_seconds": manifest["performance"]["wall_seconds"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
