#!/usr/bin/env python3
"""Compile one evidence-backed spatial placement for every canonical system."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_system_placement_policies.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_SCRATCH = Path("/mnt/space/spacegate")
SELECTED_PLACEMENT_QUANTITIES = (
    "ra_deg",
    "dec_deg",
    "parallax_mas",
    "distance_geometric_pc",
    "distance_photogeometric_pc",
    "distance_gspphot_pc",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def phase(started: float, cpu_started: float) -> dict[str, Any]:
    return {
        "wall_seconds": round(time.monotonic() - started, 6),
        "cpu_seconds": round(time.process_time() - cpu_started, 6),
        "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
    }


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.system_placement_policy.v1":
        raise ValueError("unsupported system placement policy")
    precedence = policy.get("placement_precedence") or []
    ranks = [int(row["rank"]) for row in precedence]
    sources = [str(row["source"]) for row in precedence]
    if len(ranks) != len(set(ranks)) or len(sources) != len(set(sources)):
        raise ValueError("system placement precedence must be unique")
    if sources != [
        "selected_star", "msc_component", "sbx_system_context",
        "ultracoolsheet_system_context", "heliocentric_origin",
    ]:
        raise ValueError("system placement source order changed")
    anchor = policy.get("name_anchor") or {}
    required_anchor = {
        "enabled_for_system_context_only": True,
        "require_exact_case_sensitive_match": True,
        "require_unique_source_name": True,
        "require_unique_permanent_system_label": True,
        "promote_physical_identity": False,
    }
    if any(anchor.get(key) != value for key, value in required_anchor.items()):
        raise ValueError("unsafe system-context name anchor")


def attest_selected_fact_inputs(
    policy: dict[str, Any], paths: dict[str, Path], input_hashes: dict[str, str]
) -> dict[str, Any]:
    manifest = load_object(paths["selected_fact_manifest"])
    if manifest.get("build_id") != policy["selected_fact_build_id"]:
        raise ValueError("selected-fact manifest build identity mismatch")
    registered_files = ((manifest.get("report") or {}).get("files") or {})
    checks: dict[str, bool] = {}
    for quantity in SELECTED_PLACEMENT_QUANTITIES:
        key = f"selected_facts_{quantity}"
        filename = f"selected_facts__{quantity}.parquet"
        registered = registered_files.get(filename) or {}
        checks[f"{quantity}_sha256"] = (
            registered.get("sha256") == input_hashes[key]
        )
        checks[f"{quantity}_bytes"] = (
            int(registered.get("bytes", -1)) == paths[key].stat().st_size
        )
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise ValueError(f"selected-fact product attestation failed: {failed}")
    return {
        "selected_fact_build_id": manifest["build_id"],
        "selected_fact_build_sha256": manifest.get("build_sha256"),
        "registered_product_checks": checks,
    }


def resolve_inputs(policy: dict[str, Any], state: Path) -> dict[str, Path]:
    typed = policy["ultracoolsheet_typed"]
    selected_fact_root = (
        state / "derived/evidence_lake_v2/selected_facts"
        / policy["selected_fact_build_id"]
    )
    paths = {
        "identity": state / "derived/evidence_lake_v2/identity" / policy["identity_graph_id"] / "identity_graph.duckdb",
        "identity_seed_manifest": state / "derived/evidence_lake_v2/permanent_identity_seed" / policy["permanent_identity_seed_id"] / "manifest.json",
        "identity_seed_edges": state / "derived/evidence_lake_v2/permanent_identity_seed" / policy["permanent_identity_seed_id"] / "hierarchy_edges.parquet",
        "selected_fact_manifest": selected_fact_root / "manifest.json",
        "selected_components": state / "derived/evidence_lake_v2/selected_components" / policy["selected_component_build_id"] / "selected_components.duckdb",
        "ultracool_evidence": state / "derived/evidence_lake_v2/scientific_evidence" / policy["ultracoolsheet_evidence_build_id"] / "scientific_evidence.duckdb",
        "ultracool_typed": state / "typed/evidence_lake_v2" / typed["source_id"] / typed["release_id"] / typed["raw_snapshot_id"] / typed["typed_snapshot_id"] / typed["table_path"],
    }
    paths.update({
        f"selected_facts_{quantity}":
            selected_fact_root / f"selected_facts__{quantity}.parquet"
        for quantity in SELECTED_PLACEMENT_QUANTITIES
    })
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError("missing system placement inputs: " + ", ".join(missing))
    return paths


def attach(con: duckdb.DuckDBPyConnection, alias: str, path: Path) -> None:
    con.execute(f"ATTACH {sql_literal(str(path))} AS {alias} (READ_ONLY)")


def compile_placements(
    policy_path: Path,
    state: Path,
    scratch_parent: Path,
    output_root: Path | None = None,
) -> dict[str, Any]:
    policy = load_object(policy_path)
    validate_policy(policy)
    paths = resolve_inputs(policy, state)
    timings: dict[str, Any] = {}
    started = time.monotonic()
    cpu_started = time.process_time()
    input_hashes = {key: file_sha256(path) for key, path in paths.items()}
    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    input_attestation = attest_selected_fact_inputs(policy, paths, input_hashes)
    timings["input_attestation"] = phase(started, cpu_started)
    build_id = stable_hash({
        "policy_sha256": policy_sha,
        "input_sha256": input_hashes,
        "input_attestation": input_attestation,
        "compiler_version": policy["compiler_version"],
        "compiler_sha256": compiler_sha,
    })[:24]
    root = output_root or state / "derived/evidence_lake_v2/selected_system_placements"
    final_dir = root / build_id
    temp_dir = root / f".{build_id}.tmp"
    if final_dir.is_dir():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("existing system placement artifact identity mismatch")
        return manifest
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    scratch_parent.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path(tempfile.mkdtemp(prefix="e5-system-placement-", dir=scratch_parent))
    work_db = scratch_dir / "work.duckdb"
    pc_to_ly = float(policy["coordinate_contract"]["parsec_to_light_year"])
    ranks = {row["source"]: int(row["rank"]) for row in policy["placement_precedence"]}
    con = duckdb.connect(str(work_db))
    try:
        con.execute("SET threads=12")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET memory_limit='48GB'")
        attach(con, "identity", paths["identity"])
        attach(con, "components", paths["selected_components"])
        attach(con, "ultra", paths["ultracool_evidence"])

        p_started, p_cpu = time.monotonic(), time.process_time()
        fact_paths = {
            quantity: sql_literal(str(paths[f"selected_facts_{quantity}"]))
            for quantity in SELECTED_PLACEMENT_QUANTITIES
        }
        con.execute(
            f"""
            CREATE TABLE star_candidates AS
            WITH ra AS (
              SELECT stable_object_key,system_stable_object_key system_key,
                     normalized_value ra_deg,selected_fact_id ra_fact_id,
                     source_id,release_id
              FROM read_parquet({fact_paths['ra_deg']}) WHERE object_type='star'
            ), dec AS (
              SELECT stable_object_key,normalized_value dec_deg,
                     selected_fact_id dec_fact_id
              FROM read_parquet({fact_paths['dec_deg']}) WHERE object_type='star'
            ), px AS (
              SELECT stable_object_key,normalized_value parallax_mas,
                     selected_fact_id parallax_fact_id
              FROM read_parquet({fact_paths['parallax_mas']}) WHERE object_type='star'
            ), dg AS (
              SELECT stable_object_key,normalized_value distance_geometric_pc,
                     selected_fact_id distance_geometric_fact_id
              FROM read_parquet({fact_paths['distance_geometric_pc']}) WHERE object_type='star'
            ), dpg AS (
              SELECT stable_object_key,normalized_value distance_photogeometric_pc,
                     selected_fact_id distance_photogeometric_fact_id
              FROM read_parquet({fact_paths['distance_photogeometric_pc']}) WHERE object_type='star'
            ), dsp AS (
              SELECT stable_object_key,normalized_value distance_gspphot_pc,
                     selected_fact_id distance_gspphot_fact_id
              FROM read_parquet({fact_paths['distance_gspphot_pc']}) WHERE object_type='star'
            ), pivoted AS (
              SELECT ra.system_key,ra.stable_object_key representative_object_key,
                     ra_deg,dec_deg,parallax_mas,distance_geometric_pc,
                     distance_photogeometric_pc,distance_gspphot_pc,
                     ra_fact_id,dec_fact_id,parallax_fact_id,
                     distance_geometric_fact_id,distance_photogeometric_fact_id,
                     distance_gspphot_fact_id,source_id,release_id
              FROM ra JOIN dec USING(stable_object_key)
              JOIN px USING(stable_object_key)
              LEFT JOIN dg USING(stable_object_key)
              LEFT JOIN dpg USING(stable_object_key)
              LEFT JOIN dsp USING(stable_object_key)
            ), placed AS (
              SELECT *,
                coalesce(distance_geometric_pc,distance_photogeometric_pc,
                         distance_gspphot_pc,
                         CASE WHEN parallax_mas>0 THEN 1000.0/parallax_mas END) distance_pc,
                CASE WHEN distance_geometric_pc IS NOT NULL THEN 'distance_geometric_pc'
                     WHEN distance_photogeometric_pc IS NOT NULL THEN 'distance_photogeometric_pc'
                     WHEN distance_gspphot_pc IS NOT NULL THEN 'distance_gspphot_pc'
                     ELSE 'inverse_selected_parallax' END distance_basis
              FROM pivoted WHERE ra_deg IS NOT NULL AND dec_deg IS NOT NULL
            ), root_stars AS (
              SELECT parent_node_key system_key,child_node_key star_key
              FROM read_parquet({sql_literal(str(paths['identity_seed_edges']))})
              WHERE source_basis='canonical_root_star'
            )
            SELECT p.system_key,{ranks['selected_star']} source_rank,
                   'selected_star' placement_source,representative_object_key,
                   ra_deg,dec_deg,distance_pc,parallax_mas,
                   'ICRS' coordinate_frame,'J2016.0' coordinate_epoch,
                   'selected_star_astrometry_and_distance' placement_method,
                   source_id,release_id,
                   to_json(struct_pack(ra_fact_id:=ra_fact_id,dec_fact_id:=dec_fact_id,
                     parallax_fact_id:=parallax_fact_id,
                     distance_geometric_fact_id:=distance_geometric_fact_id,
                     distance_photogeometric_fact_id:=distance_photogeometric_fact_id,
                     distance_gspphot_fact_id:=distance_gspphot_fact_id)) evidence_ids_json,
                   to_json(struct_pack(distance_basis:=distance_basis,
                     representative_policy:='canonical_root_star_then_stable_key')) derivation_json
            FROM placed p LEFT JOIN root_stars r
              ON r.system_key=p.system_key AND r.star_key=p.representative_object_key
            WHERE distance_pc>0
            QUALIFY row_number() OVER(PARTITION BY p.system_key
              ORDER BY (r.star_key IS NOT NULL) DESC,representative_object_key)=1
            """
        )
        timings["selected_star_candidates"] = phase(p_started, p_cpu)

        p_started, p_cpu = time.monotonic(), time.process_time()
        con.execute(
            f"""
            CREATE TABLE msc_candidates AS
            WITH pivoted AS (
              SELECT p.canonical_system_stable_object_key system_key,p.target_key,
                     max(p.normalized_value) FILTER(WHERE quantity_key='right_ascension') ra_deg,
                     max(p.normalized_value) FILTER(WHERE quantity_key='declination') dec_deg,
                     max(p.normalized_value) FILTER(WHERE quantity_key='parallax') parallax_mas,
                     max(p.evidence_id) FILTER(WHERE quantity_key='right_ascension') ra_evidence_id,
                     max(p.evidence_id) FILTER(WHERE quantity_key='declination') dec_evidence_id,
                     max(p.evidence_id) FILTER(WHERE quantity_key='parallax') parallax_evidence_id,
                     min(e.component_label_normalized) component_label,
                     min(e.source_id) source_id,min(e.release_id) release_id
              FROM components.msc_astrometry_projection p
              JOIN components.msc_component_entities e
                ON e.source_component_key=p.target_key AND e.binding_status='accepted'
              WHERE p.projection_status='eligible_for_quantity_selection'
                AND p.quantity_key IN ('right_ascension','declination','parallax')
              GROUP BY 1,2
              HAVING count(DISTINCT p.quantity_key)=3
                 AND min(CASE WHEN quantity_key='parallax' THEN normalized_value END)>0
            )
            SELECT system_key,{ranks['msc_component']} source_rank,
                   'msc_component' placement_source,target_key representative_object_key,
                   ra_deg,dec_deg,1000.0/parallax_mas distance_pc,parallax_mas,
                   'ICRS' coordinate_frame,'J2000.0' coordinate_epoch,
                   'msc_coherent_component_astrometry_inverse_parallax' placement_method,
                   source_id,release_id,
                   to_json(struct_pack(ra_evidence_id:=ra_evidence_id,
                     dec_evidence_id:=dec_evidence_id,
                     parallax_evidence_id:=parallax_evidence_id)) evidence_ids_json,
                   to_json(struct_pack(distance_basis:='inverse_source_parallax',
                     component_label:=component_label,
                     representative_policy:='component_A_then_shortest_label_then_source_key')) derivation_json
            FROM pivoted
            QUALIFY row_number() OVER(PARTITION BY system_key ORDER BY
              CASE WHEN component_label='A' THEN 0 WHEN component_label='Aa' THEN 1 ELSE 2 END,
              length(component_label),component_label,target_key)=1
            """
        )
        timings["msc_candidates"] = phase(p_started, p_cpu)

        p_started, p_cpu = time.monotonic(), time.process_time()
        con.execute(
            f"""
            CREATE TABLE sbx_candidates AS
            WITH pivoted AS (
              SELECT canonical_system_stable_object_key system_key,
                     projected_relation_id,min(source_record_id) source_record_id,
                     max(normalized_value) FILTER(WHERE quantity_key='right_ascension') ra_deg,
                     max(normalized_value) FILTER(WHERE quantity_key='declination') dec_deg,
                     max(normalized_value) FILTER(WHERE quantity_key='parallax') parallax_mas,
                     max(epoch_raw) FILTER(WHERE quantity_key='right_ascension') position_epoch_raw,
                     max(evidence_id) FILTER(WHERE quantity_key='right_ascension') ra_evidence_id,
                     max(evidence_id) FILTER(WHERE quantity_key='declination') dec_evidence_id,
                     max(evidence_id) FILTER(WHERE quantity_key='parallax') parallax_evidence_id
              FROM components.sbx_astrometry_projection
              WHERE projection_status='context_only_evidence'
                AND quantity_key IN ('right_ascension','declination','parallax')
              GROUP BY 1,2
              HAVING count(DISTINCT quantity_key)=3
                 AND min(CASE WHEN quantity_key='parallax' THEN normalized_value END)>0
            )
            SELECT p.system_key,{ranks['sbx_system_context']} source_rank,
                   'sbx_system_context' placement_source,projected_relation_id representative_object_key,
                   ra_deg,dec_deg,1000.0/parallax_mas distance_pc,parallax_mas,
                   'ICRS' coordinate_frame,'J'||position_epoch_raw coordinate_epoch,
                   'sbx_observation_target_context_inverse_parallax' placement_method,
                   b.source_id,b.release_id,
                   to_json(struct_pack(ra_evidence_id:=ra_evidence_id,
                     dec_evidence_id:=dec_evidence_id,
                     parallax_evidence_id:=parallax_evidence_id)) evidence_ids_json,
                   to_json(struct_pack(distance_basis:='inverse_source_parallax',
                     scope:='system_observation_target_context')) derivation_json
            FROM pivoted p JOIN components.sbx_system_bindings b
              ON b.source_record_id=p.source_record_id
             AND b.binding_status='accepted'
            QUALIFY row_number() OVER(PARTITION BY system_key ORDER BY projected_relation_id)=1
            """
        )
        timings["sbx_candidates"] = phase(p_started, p_cpu)

        p_started, p_cpu = time.monotonic(), time.process_time()
        con.execute(
            f"""
            CREATE TEMP TABLE ultracool_rows AS
            SELECT row_number() OVER() source_row_number,
                   sha256(to_json(source_row)) source_row_sha256
            FROM read_parquet({sql_literal(str(paths['ultracool_typed']))}) source_row
            """
        )
        con.execute(
            f"""
            CREATE TABLE ultracool_candidates AS
            WITH base AS (
              SELECT t.source_row_number,r.source_record_id,
                     json_extract_string(r.logical_key_json,'$.name') source_name
              FROM ultracool_rows t JOIN ultra.source_records r
                ON r.source_row_sha256=t.source_row_sha256
            ), direct AS (
              SELECT b.source_record_id,min(c.system_stable_object_key) system_key
              FROM base b JOIN ultra.identifier_claim_evidence ic
                ON ic.source_record_id=b.source_record_id
               AND ic.namespace='gaia_dr3_source_id'
               AND ic.claim_scope='star_or_substellar_object'
              JOIN identity.canonical_identifier_bindings c
                ON c.namespace='gaia_dr3'
               AND c.id_value_norm=regexp_extract(ic.identifier_normalized,'([0-9]+)$',1)
              GROUP BY 1
            ), release_bridge AS (
              SELECT b.source_record_id,min(s.canonical_system_stable_object_key) system_key
              FROM base b JOIN ultra.identifier_claim_evidence ic
                ON ic.source_record_id=b.source_record_id
               AND ic.namespace='gaia_dr2_source_id'
              JOIN identity.source_record_bindings s
                ON s.source_id='ultracool.ultracoolsheet'
               AND s.dr2_source_id=regexp_extract(ic.identifier_normalized,'([0-9]+)$',1)
               AND s.outcome='accepted'
              GROUP BY 1
            ), row_anchor AS (
              SELECT b.source_record_id,o.stable_object_key system_key
              FROM base b JOIN identity.canonical_object_nodes o
                ON o.object_type='system'
               AND o.stable_object_key='canon:system:stable:system:ultracoolsheet:'
                   || b.source_row_number::varchar
            ), unique_source_names AS (
              SELECT source_name FROM base GROUP BY 1 HAVING count(*)=1
            ), unique_system_names AS (
              SELECT display_name,min(stable_object_key) system_key
              FROM identity.canonical_object_nodes WHERE object_type='system'
              GROUP BY 1 HAVING count(*)=1
            ), name_anchor AS (
              SELECT b.source_record_id,u.system_key
              FROM base b JOIN unique_source_names USING(source_name)
              JOIN unique_system_names u ON u.display_name=b.source_name
            ), mapped AS (
              SELECT b.*,
                     coalesce(d.system_key,r.system_key,a.system_key,n.system_key) system_key,
                     CASE WHEN d.system_key IS NOT NULL THEN 'direct_gaia_dr3_object'
                          WHEN r.system_key IS NOT NULL THEN 'official_dr2_dr3_system_context'
                          WHEN a.system_key IS NOT NULL THEN 'source_row_permanent_identity'
                          ELSE 'unique_exact_name_system_context' END binding_method
              FROM base b LEFT JOIN direct d USING(source_record_id)
              LEFT JOIN release_bridge r USING(source_record_id)
              LEFT JOIN row_anchor a USING(source_record_id)
              LEFT JOIN name_anchor n USING(source_record_id)
            ), pivoted AS (
              SELECT m.system_key,m.source_record_id,m.source_name,m.binding_method,
                     max(e.normalized_value) FILTER(WHERE quantity_key='right_ascension') ra_deg,
                     max(e.normalized_value) FILTER(WHERE quantity_key='declination') dec_deg,
                     max(e.normalized_value) FILTER(WHERE quantity_key='maintainer_selected_distance') distance_pc,
                     max(e.normalized_value) FILTER(WHERE quantity_key='maintainer_selected_parallax') parallax_mas,
                     max(e.evidence_id) FILTER(WHERE quantity_key='right_ascension') ra_evidence_id,
                     max(e.evidence_id) FILTER(WHERE quantity_key='declination') dec_evidence_id,
                     max(e.evidence_id) FILTER(WHERE quantity_key='maintainer_selected_distance') distance_evidence_id,
                     max(e.evidence_id) FILTER(WHERE quantity_key='maintainer_selected_parallax') parallax_evidence_id
              FROM mapped m JOIN ultra.astrometry_distance_evidence e USING(source_record_id)
              WHERE m.system_key IS NOT NULL AND e.quantity_key IN
                ('right_ascension','declination','maintainer_selected_distance','maintainer_selected_parallax')
              GROUP BY 1,2,3,4
            )
            SELECT system_key,{ranks['ultracoolsheet_system_context']} source_rank,
                   'ultracoolsheet_system_context' placement_source,
                   source_record_id representative_object_key,
                   ra_deg,dec_deg,distance_pc,parallax_mas,
                   'ICRS' coordinate_frame,'J2000.0' coordinate_epoch,
                   'ultracoolsheet_maintainer_selected_system_context' placement_method,
                   'ultracool.ultracoolsheet' source_id,
                   {sql_literal(str(policy['ultracoolsheet_typed']['release_id']))} release_id,
                   to_json(struct_pack(ra_evidence_id:=ra_evidence_id,
                     dec_evidence_id:=dec_evidence_id,distance_evidence_id:=distance_evidence_id,
                     parallax_evidence_id:=parallax_evidence_id)) evidence_ids_json,
                   to_json(struct_pack(distance_basis:='maintainer_selected_distance',
                     identity_binding_method:=binding_method,source_name:=source_name,
                     scope:='system_context_only')) derivation_json
            FROM pivoted
            WHERE ra_deg IS NOT NULL AND dec_deg IS NOT NULL AND distance_pc>0
            QUALIFY row_number() OVER(PARTITION BY system_key ORDER BY binding_method,source_record_id)=1
            """
        )
        timings["ultracool_candidates"] = phase(p_started, p_cpu)

        p_started, p_cpu = time.monotonic(), time.process_time()
        sol_key = str(policy["coordinate_contract"]["sol_system_key"])
        con.execute(
            f"""
            CREATE TABLE origin_candidate AS
            SELECT {sql_literal(sol_key)} system_key,{ranks['heliocentric_origin']} source_rank,
              'heliocentric_origin',NULL,0.0,0.0,0.0,NULL,
              'ICRS','J2016.0','defined_heliocentric_origin',
              'spacegate.coordinate_contract','heliocentric_icrs_j2016_v1',
              '{{}}','{{"distance_basis":"defined_origin"}}'
            """
        )
        con.execute(
            f"""
            CREATE TEMP VIEW placement_winners AS
            SELECT * FROM star_candidates
            UNION ALL
            SELECT m.* FROM msc_candidates m
              ANTI JOIN star_candidates s USING(system_key)
            UNION ALL
            SELECT b.* FROM sbx_candidates b
              ANTI JOIN star_candidates s USING(system_key)
              ANTI JOIN msc_candidates m USING(system_key)
            UNION ALL
            SELECT u.* FROM ultracool_candidates u
              ANTI JOIN star_candidates s USING(system_key)
              ANTI JOIN msc_candidates m USING(system_key)
              ANTI JOIN sbx_candidates b USING(system_key)
            UNION ALL
            SELECT o.* FROM origin_candidate o
              ANTI JOIN star_candidates s USING(system_key)
              ANTI JOIN msc_candidates m USING(system_key)
              ANTI JOIN sbx_candidates b USING(system_key)
              ANTI JOIN ultracool_candidates u USING(system_key)
            """
        )
        con.execute(
            f"""
            CREATE TABLE selected_system_placements AS
            SELECT system_key system_stable_object_key,representative_object_key,
                   ra_deg,dec_deg,distance_pc,parallax_mas,
                   distance_pc*{pc_to_ly} dist_ly,
                   distance_pc*cos(radians(dec_deg))*cos(radians(ra_deg))*{pc_to_ly} x_helio_ly,
                   distance_pc*cos(radians(dec_deg))*sin(radians(ra_deg))*{pc_to_ly} y_helio_ly,
                   distance_pc*sin(radians(dec_deg))*{pc_to_ly} z_helio_ly,
                   coordinate_frame,coordinate_epoch,placement_source,placement_method,
                   'selected' placement_status,{sql_literal(str(policy['policy_version']))} policy_version
            FROM placement_winners
            """
        )
        con.execute(
            f"""
            CREATE TABLE selected_system_placement_lineage AS
            SELECT system_key system_stable_object_key,placement_source,placement_method,
                   source_id,release_id,evidence_ids_json::JSON evidence_ids_json,
                   derivation_json::JSON derivation_json,
                   {sql_literal(str(policy['policy_version']))} policy_version
            FROM placement_winners
            """
        )
        timings["winner_selection"] = phase(p_started, p_cpu)

        p_started, p_cpu = time.monotonic(), time.process_time()
        placement_path = temp_dir / "selected_system_placements.parquet"
        lineage_path = temp_dir / "selected_system_placement_lineage.parquet"
        con.execute(
            f"COPY (SELECT * FROM selected_system_placements ORDER BY system_stable_object_key) "
            f"TO {sql_literal(str(placement_path))} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 1000000)"
        )
        con.execute(
            f"COPY (SELECT * FROM selected_system_placement_lineage ORDER BY system_stable_object_key) "
            f"TO {sql_literal(str(lineage_path))} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 1000000)"
        )
        timings["parquet_export"] = phase(p_started, p_cpu)

        p_started, p_cpu = time.monotonic(), time.process_time()
        counts = {str(source): int(count) for source, count in con.execute(
            "SELECT placement_source,count(*) FROM selected_system_placements GROUP BY 1 ORDER BY 1"
        ).fetchall()}
        checks = {
            "inventory_delta": int(con.execute(
                "SELECT (SELECT count(*) FROM selected_system_placements) - "
                "(SELECT count(*) FROM identity.canonical_object_nodes WHERE object_type='system')"
            ).fetchone()[0]),
            "duplicate_systems": int(con.execute(
                "SELECT count(*) FROM (SELECT system_stable_object_key FROM selected_system_placements GROUP BY 1 HAVING count(*)>1)"
            ).fetchone()[0]),
            "missing_canonical_systems": int(con.execute(
                "SELECT count(*) FROM identity.canonical_object_nodes s LEFT JOIN selected_system_placements p ON p.system_stable_object_key=s.stable_object_key WHERE s.object_type='system' AND p.system_stable_object_key IS NULL"
            ).fetchone()[0]),
            "unknown_systems": int(con.execute(
                "SELECT count(*) FROM selected_system_placements p LEFT JOIN identity.canonical_object_nodes s ON s.object_type='system' AND s.stable_object_key=p.system_stable_object_key WHERE s.stable_object_key IS NULL"
            ).fetchone()[0]),
            "invalid_coordinates": int(con.execute(
                "SELECT count(*) FROM selected_system_placements WHERE ra_deg NOT BETWEEN 0 AND 360 OR dec_deg NOT BETWEEN -90 AND 90 OR distance_pc<0"
            ).fetchone()[0]),
            "missing_coordinate_metadata": int(con.execute(
                "SELECT count(*) FROM selected_system_placements WHERE coordinate_frame IS NULL OR coordinate_epoch IS NULL"
            ).fetchone()[0]),
            "cartesian_norm_mismatches": int(con.execute(
                "SELECT count(*) FROM selected_system_placements WHERE abs(sqrt(x_helio_ly*x_helio_ly+y_helio_ly*y_helio_ly+z_helio_ly*z_helio_ly)-dist_ly)>1e-8"
            ).fetchone()[0]),
            "lineage_inventory_delta": int(con.execute(
                "SELECT (SELECT count(*) FROM selected_system_placement_lineage)-(SELECT count(*) FROM selected_system_placements)"
            ).fetchone()[0]),
            "expected_winner_count_mismatches": sum(
                int(counts.get(source, 0) != int(expected))
                for source, expected in policy["expected_winner_counts"].items()
            ),
        }
        if any(checks.values()):
            raise ValueError(f"selected system placement verification failed: {checks}; counts={counts}")
        timings["verification"] = phase(p_started, p_cpu)
    finally:
        con.close()
        shutil.rmtree(scratch_dir)

    products = {}
    for path in (placement_path, lineage_path):
        products[path.stem] = {
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
    manifest = {
        "schema_version": "spacegate.selected_system_placements_manifest.v1",
        "build_id": build_id,
        "status": "pass",
        "created_at": utc_now(),
        "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha,
        "compiler_version": policy["compiler_version"],
        "compiler_sha256": compiler_sha,
        "input_sha256": input_hashes,
        "input_attestation": input_attestation,
        "products": products,
        "winner_counts": counts,
        "verification": checks,
        "timings": timings,
        "scratch_removed": not scratch_dir.exists(),
    }
    (temp_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temp_dir, final_dir)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--scratch-parent", type=Path, default=DEFAULT_SCRATCH)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = compile_placements(
        args.policy.resolve(), args.state_dir.resolve(), args.scratch_parent.resolve(),
        args.output_root.resolve() if args.output_root else None,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
