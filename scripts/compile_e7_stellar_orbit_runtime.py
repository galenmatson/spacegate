#!/usr/bin/env python3
"""Compile cross-source stellar orbit roles without inventing endpoint identity."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_stellar_orbit_runtime.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-stellar-orbit-runtime")


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


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_stellar_orbit_runtime_policy.v1":
        raise ValueError("unsupported stellar orbit runtime policy")
    expected_rules = {
        "open_stability_databases": False,
        "source_relations_create_containment": False,
        "fieldwise_cross_source_composites": False,
        "simulation_requires_coherent_visual_solution": True,
        "casefold_component_identity_without_uniqueness": False,
        "context_only_solutions_create_runtime_edges": False,
    }
    if policy.get("rules") != expected_rules:
        raise ValueError("unsafe stellar orbit runtime rules")
    if policy.get("simulation_authority") != ["orb6", "msc_orb"]:
        raise ValueError("unexpected simulation authority order")
    source_roles = policy.get("source_roles") or {}
    if set(source_roles) != {
        "orb6", "msc_orb", "msc_sys", "sb9", "debcat", "sbx",
        "gaia_nss", "tess_eb",
    }:
        raise ValueError("incomplete stellar orbit source roles")
    spec = policy.get("input") or {}
    relative = Path(str(spec.get("relative_path") or ""))
    if (
        not spec.get("build_id")
        or len(str(spec.get("manifest_sha256") or "")) != 64
        or len(str(spec.get("database_sha256") or "")) != 64
        or relative.is_absolute()
        or ".." in relative.parts
    ):
        raise ValueError("invalid selected-component input contract")


def resolve_input(policy: dict[str, Any], state: Path) -> tuple[Path, dict[str, Any]]:
    spec = policy["input"]
    root = (state / spec["relative_path"]).resolve()
    manifest_path = root / "manifest.json"
    database = root / "selected_components.duckdb"
    if file_sha256(manifest_path) != spec["manifest_sha256"]:
        raise ValueError("selected-component manifest checksum mismatch")
    manifest = load_object(manifest_path)
    if manifest.get("build_id") != spec["build_id"]:
        raise ValueError("selected-component build mismatch")
    if any(int(value or 0) != 0 for value in (manifest.get("verification") or {}).values()):
        raise ValueError("selected-component verification is not accepted")
    registered = (manifest.get("files") or {}).get("selected_components.duckdb") or {}
    if (
        registered.get("sha256") != spec["database_sha256"]
        or file_sha256(database) != spec["database_sha256"]
    ):
        raise ValueError("selected-component database checksum mismatch")
    return database, manifest


def materialize(con: duckdb.DuckDBPyConnection, policy: dict[str, Any], build_id: str) -> None:
    policy_version = sql_literal(policy["policy_version"])
    build = sql_literal(build_id)
    con.execute(
        f"""
        CREATE TEMP TABLE orbit_solution_candidates AS
        WITH msc AS (
          SELECT o.msc_projected_relation_id relation_id,o.evidence_id,o.source_record_id,
            r.source_id,r.release_id,o.source_table,
            CASE WHEN o.source_table='msc_orb' THEN 'msc_orb' ELSE 'msc_sys' END source_kind,
            o.authority_role,o.primary_source_component_key,o.secondary_source_component_key,
            o.canonical_system_stable_object_key,o.solution_key,o.parameter_set_raw,
            o.epoch_raw,o.frame_raw,o.method,o.model,o.reference_raw,o.quality_json,
            o.normalization_version,
            CASE json_extract_string(o.parameter_set_raw,'$.Punit')
              WHEN 'd' THEN try_cast(coalesce(json_extract_string(o.parameter_set_raw,'$.Period'),json_extract_string(o.parameter_set_raw,'$.P')) AS DOUBLE)
              WHEN 'y' THEN try_cast(coalesce(json_extract_string(o.parameter_set_raw,'$.Period'),json_extract_string(o.parameter_set_raw,'$.P')) AS DOUBLE)*365.25
              WHEN 'k' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.P') AS DOUBLE)*365250.0
              WHEN 'M' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.P') AS DOUBLE)*365250000.0
            END period_days,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.a') AS DOUBLE) END semi_major_axis_arcsec,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.e') AS DOUBLE) END eccentricity,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.Incl') AS DOUBLE) END inclination_deg,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.Node') AS DOUBLE) END longitude_ascending_node_deg,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.LongP') AS DOUBLE) END argument_periastron_deg,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.T') AS DOUBLE) END time_periastron_value,
            CASE WHEN o.source_table='msc_orb' THEN CASE json_extract_string(o.parameter_set_raw,'$.Punit') WHEN 'y' THEN 'jyear' WHEN 'd' THEN 'mjd' END END time_periastron_unit,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.K1') AS DOUBLE) END rv_semiamplitude_primary_kms,
            CASE WHEN o.source_table='msc_orb' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.K2') AS DOUBLE) END rv_semiamplitude_secondary_kms,
            NULL::DOUBLE quality_rank,NULL::DOUBLE last_observed_year
          FROM selected.msc_orbital_solution_projection o
          JOIN selected.msc_relation_evidence_projection r
            ON r.projected_relation_id=o.msc_projected_relation_id
          WHERE o.projection_status='eligible_for_quantity_selection'
        ), orb6 AS (
          SELECT o.msc_projected_relation_id,o.evidence_id,o.source_record_id,
            b.source_id,b.release_id,'orb6' source_table,'orb6' source_kind,
            o.authority_role,o.primary_source_component_key,o.secondary_source_component_key,
            o.canonical_system_stable_object_key,o.solution_key,o.parameter_set_raw,
            o.epoch_raw,o.frame_raw,o.method,o.model,o.reference_raw,o.quality_json,
            o.normalization_version,
            CASE json_extract_string(o.parameter_set_raw,'$.period_unit')
              WHEN 'd' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.period_raw') AS DOUBLE)
              WHEN 'y' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.period_raw') AS DOUBLE)*365.25
              WHEN 'c' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.period_raw') AS DOUBLE)*36525.0
              WHEN 'h' THEN try_cast(json_extract_string(o.parameter_set_raw,'$.period_raw') AS DOUBLE)/24.0
            END,
            try_cast(json_extract_string(o.parameter_set_raw,'$.semimajor_axis_raw') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.eccentricity_raw') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.inclination_deg_raw') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.ascending_node_deg_raw') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.longitude_periastron_deg_raw') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.periastron_epoch_raw') AS DOUBLE),
            json_extract_string(o.parameter_set_raw,'$.periastron_epoch_unit'),
            NULL::DOUBLE,NULL::DOUBLE,
            try_cast(json_extract_string(o.quality_json,'$.orbit_grade_raw') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.last_observed_year_raw') AS DOUBLE)
          FROM selected.orb6_orbital_solution_projection o
          JOIN selected.orb6_relation_bindings b USING(relation_binding_id)
          WHERE o.projection_status='eligible_for_quantity_selection'
        ), sb9 AS (
          SELECT b.msc_projected_relation_id,o.evidence_id,o.source_record_id,
            b.source_id,b.release_id,'sb9','sb9',o.authority_role,
            o.primary_source_component_key,o.secondary_source_component_key,
            o.canonical_system_stable_object_key,o.solution_key,o.parameter_set_raw,
            o.epoch_raw,o.frame_raw,o.method,o.model,o.reference_raw,o.quality_json,
            o.normalization_version,
            try_cast(json_extract_string(o.parameter_set_raw,'$.Per') AS DOUBLE),
            NULL::DOUBLE,try_cast(json_extract_string(o.parameter_set_raw,'$.e') AS DOUBLE),
            NULL::DOUBLE,NULL::DOUBLE,
            try_cast(json_extract_string(o.parameter_set_raw,'$.omega') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.T0') AS DOUBLE),'jd'::VARCHAR,
            try_cast(json_extract_string(o.parameter_set_raw,'$.K1') AS DOUBLE),
            try_cast(json_extract_string(o.parameter_set_raw,'$.K2') AS DOUBLE),
            try_cast(json_extract_string(o.quality_json,'$.Grade') AS DOUBLE),NULL::DOUBLE
          FROM selected.sb9_orbital_solution_projection o
          JOIN selected.sb9_relation_bindings b USING(relation_binding_id)
          WHERE o.projection_status='eligible_for_quantity_selection'
        ), debcat AS (
          SELECT b.msc_projected_relation_id,o.evidence_id,o.source_record_id,
            b.source_id,b.release_id,'debcat','debcat',o.authority_role,
            o.primary_source_component_key,o.secondary_source_component_key,
            o.canonical_system_stable_object_key,o.solution_key,o.parameter_set_raw,
            o.epoch_raw,o.frame_raw,o.method,o.model,o.reference_raw,o.quality_json,
            o.normalization_version,
            try_cast(json_extract_string(o.parameter_set_raw,'$.period_days_raw') AS DOUBLE),
            NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,
            NULL::DOUBLE,NULL::VARCHAR,
            NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE
          FROM selected.debcat_orbital_solution_projection o
          JOIN selected.debcat_relation_bindings b USING(relation_binding_id)
          WHERE o.projection_status='eligible_for_quantity_selection'
        )
        SELECT * FROM msc UNION ALL SELECT * FROM orb6
        UNION ALL SELECT * FROM sb9 UNION ALL SELECT * FROM debcat;

        CREATE TEMP TABLE orbit_solutions_with_completeness AS
        SELECT *,
          source_kind IN ('orb6','msc_orb')
            AND period_days IS NOT NULL AND semi_major_axis_arcsec IS NOT NULL
            AND eccentricity IS NOT NULL AND inclination_deg IS NOT NULL
            AND longitude_ascending_node_deg IS NOT NULL
            AND argument_periastron_deg IS NOT NULL
            AND time_periastron_value IS NOT NULL AS simulation_complete
        FROM orbit_solution_candidates;

        CREATE TEMP TABLE ranked_orbit_solutions AS
        SELECT *,
          CASE WHEN simulation_complete THEN row_number() OVER (
            PARTITION BY relation_id,simulation_complete
            ORDER BY
              CASE source_kind WHEN 'orb6' THEN 0 WHEN 'msc_orb' THEN 1 ELSE 2 END,
              CASE WHEN source_kind='orb6' THEN coalesce(quality_rank,99) ELSE 0 END,
              last_observed_year DESC NULLS LAST,evidence_id
          ) END AS simulation_rank
        FROM orbit_solutions_with_completeness;

        CREATE TABLE selected_stellar_orbit_solutions AS
        SELECT sha256(concat_ws('|','selected-stellar-orbit',evidence_id,{policy_version}))
            selected_orbit_solution_id,
          relation_id,evidence_id,source_record_id,source_id,release_id,source_kind,
          authority_role,primary_source_component_key,secondary_source_component_key,
          canonical_system_stable_object_key,solution_key,parameter_set_raw,
          epoch_raw,frame_raw,method,model,reference_raw,quality_json,
          normalization_version,period_days,semi_major_axis_arcsec,eccentricity,
          inclination_deg,longitude_ascending_node_deg,argument_periastron_deg,
          time_periastron_value,time_periastron_unit,
          rv_semiamplitude_primary_kms,rv_semiamplitude_secondary_kms,
          quality_rank,last_observed_year,simulation_complete,simulation_rank,
          CASE WHEN simulation_rank=1 THEN 'preferred_simulation'
               WHEN simulation_complete THEN 'alternate_simulation'
               WHEN source_kind='sb9' THEN 'spectroscopic_context'
               WHEN source_kind='debcat' THEN 'eclipsing_period_context'
               WHEN source_kind='msc_sys' THEN 'relation_period_or_separation_context'
               ELSE 'incomplete_visual_context' END selection_role,
          false canonical_containment,{policy_version} policy_version,{build} build_id
        FROM ranked_orbit_solutions
        ORDER BY relation_id,source_kind,evidence_id;

        CREATE TABLE selected_stellar_orbit_relations AS
        SELECT s.relation_id,r.relation_evidence_id,r.source_record_id,
          r.source_id,r.release_id,r.canonical_system_stable_object_key,
          r.left_source_component_key primary_source_component_key,
          r.right_source_component_key secondary_source_component_key,
          r.relation_kind,r.relation_scope,r.method,r.reference_raw,r.quality_json,
          count(*) solution_count,count(*) FILTER (WHERE s.simulation_complete) simulation_candidate_count,
          max(s.selected_orbit_solution_id) FILTER (WHERE s.simulation_rank=1)
            preferred_simulation_solution_id,
          to_json(list_sort(list_distinct(list(s.source_kind))))::VARCHAR source_kinds_json,
          false canonical_containment,{policy_version} policy_version,{build} build_id
        FROM selected_stellar_orbit_solutions s
        JOIN selected.msc_relation_evidence_projection r
          ON r.projected_relation_id=s.relation_id
        GROUP BY ALL
        ORDER BY s.relation_id;

        CREATE TABLE deferred_stellar_orbit_context AS
        SELECT * FROM (VALUES
          ('sbx','permanent_component_identity_required',
            (SELECT count(*) FROM selected.sbx_orbital_solution_projection WHERE projection_status='eligible_for_quantity_selection')),
          ('gaia_nss','physical_relation_adjudication_required',
            (SELECT count(*) FROM selected.gaia_nss_orbital_solution_projection WHERE projection_status='context_only_evidence')),
          ('tess_eb','physical_relation_adjudication_required',
            (SELECT count(*) FROM selected.tess_eb_orbital_solution_projection WHERE projection_status='context_only_evidence'))
        ) v(source_kind,defer_reason,solution_count);
        """
    )


def verify(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> dict[str, Any]:
    scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
    counts = {
        "selected_relations": scalar("SELECT count(*) FROM selected_stellar_orbit_relations"),
        "selected_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions"),
        "msc_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE source_kind IN ('msc_orb','msc_sys')"),
        "orb6_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE source_kind='orb6'"),
        "sb9_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE source_kind='sb9'"),
        "debcat_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE source_kind='debcat'"),
        "preferred_simulation_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE selection_role='preferred_simulation'"),
        "deferred_sbx_solutions": scalar("SELECT solution_count FROM deferred_stellar_orbit_context WHERE source_kind='sbx'"),
        "deferred_gaia_nss_solutions": scalar("SELECT solution_count FROM deferred_stellar_orbit_context WHERE source_kind='gaia_nss'"),
        "deferred_tess_eb_solutions": scalar("SELECT solution_count FROM deferred_stellar_orbit_context WHERE source_kind='tess_eb'"),
        "canonical_containment_promotions": scalar("SELECT (SELECT count(*) FROM selected_stellar_orbit_relations WHERE canonical_containment)+(SELECT count(*) FROM selected_stellar_orbit_solutions WHERE canonical_containment)"),
    }
    expected = {key: int(value) for key, value in policy["acceptance"].items()}
    checks = {
        "acceptance_count_delta": sum(abs(counts.get(key, -1)-value) for key,value in expected.items()),
        "duplicate_relations": scalar("SELECT count(*) FROM (SELECT relation_id FROM selected_stellar_orbit_relations GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_solutions": scalar("SELECT count(*) FROM (SELECT selected_orbit_solution_id FROM selected_stellar_orbit_solutions GROUP BY 1 HAVING count(*)<>1)"),
        "multiple_preferred_solutions": scalar("SELECT count(*) FROM (SELECT relation_id FROM selected_stellar_orbit_solutions WHERE selection_role='preferred_simulation' GROUP BY 1 HAVING count(*)<>1)"),
        "incomplete_preferred_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE selection_role='preferred_simulation' AND NOT simulation_complete"),
        "preferred_relation_delta": scalar("SELECT count(*) FROM selected_stellar_orbit_relations WHERE preferred_simulation_solution_id IS NOT NULL") - counts["preferred_simulation_solutions"],
        "orphan_solutions": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions s LEFT JOIN selected_stellar_orbit_relations r USING(relation_id) WHERE r.relation_id IS NULL"),
        "fieldwise_composites": scalar("SELECT count(*) FROM selected_stellar_orbit_solutions WHERE source_id IS NULL OR evidence_id IS NULL OR parameter_set_raw IS NULL"),
    }
    failing = {key:value for key,value in checks.items() if value}
    return {"status":"pass" if not failing else "fail","counts":counts,"expected_counts":expected,"checks":checks,"failing_checks":failing}


def compile_runtime(policy_path: Path, state: Path, output_root: Path, *, link_into_state: bool) -> dict[str, Any]:
    started=time.monotonic(); cpu_started=time.process_time()
    policy=load_object(policy_path); validate_policy(policy)
    database,input_manifest=resolve_input(policy,state)
    policy_sha=file_sha256(policy_path); compiler_sha=file_sha256(Path(__file__).resolve())
    build_id=stable_hash({"policy_sha256":policy_sha,"compiler_sha256":compiler_sha,"input_manifest_sha256":policy["input"]["manifest_sha256"]})[:24]
    final=output_root/build_id
    if (final/"manifest.json").is_file(): return load_object(final/"manifest.json")
    output_root.mkdir(parents=True,exist_ok=True)
    staging=Path(tempfile.mkdtemp(prefix=f".{build_id}.",dir=output_root))
    try:
        con=duckdb.connect(); products={}
        try:
            con.execute(f"ATTACH {sql_literal(database)} AS selected (READ_ONLY)")
            materialize(con,policy,build_id); verification=verify(con,policy)
            if verification["status"]!="pass": raise ValueError(f"stellar orbit runtime verification failed: {verification['failing_checks']}")
            for table in ("selected_stellar_orbit_relations","selected_stellar_orbit_solutions","deferred_stellar_orbit_context"):
                path=staging/f"{table}.parquet"
                con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO {sql_literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)")
                products[path.name]={"rows":int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),"bytes":path.stat().st_size,"sha256":file_sha256(path),"determinism":"byte_exact"}
        finally: con.close()
        manifest={"schema_version":"spacegate.e7_stellar_orbit_runtime_manifest.v1","build_id":build_id,"status":"pass","generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),"policy_version":policy["policy_version"],"compiler_version":policy["compiler_version"],"policy_sha256":policy_sha,"compiler_sha256":compiler_sha,"input":{"build_id":input_manifest["build_id"],"manifest_sha256":policy["input"]["manifest_sha256"]},"stability_databases_opened":[],"verification":verification,"products":products,"performance":{"wall_seconds":round(time.monotonic()-started,6),"cpu_seconds":round(time.process_time()-cpu_started,6),"peak_rss_kib":int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)}}
        write_object_atomic(staging/"manifest.json",manifest);os.replace(staging,final)
        if link_into_state:
            root=state/"derived/evidence_lake_v2/stellar_orbit_runtime";root.mkdir(parents=True,exist_ok=True);link=root/build_id
            if not link.exists() and not link.is_symlink():link.symlink_to(final)
        return manifest
    except Exception:
        shutil.rmtree(staging,ignore_errors=True);raise


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--policy",type=Path,default=DEFAULT_POLICY);parser.add_argument("--state-dir",type=Path,default=DEFAULT_STATE);parser.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT);parser.add_argument("--no-state-link",action="store_true");parser.add_argument("--report",type=Path);args=parser.parse_args()
    manifest=compile_runtime(args.policy.resolve(),args.state_dir.resolve(),args.output_root.resolve(),link_into_state=not args.no_state_link)
    if args.report:write_object_atomic(args.report.resolve(),manifest)
    print(json.dumps({"build_id":manifest["build_id"],"status":manifest["status"],"counts":manifest["verification"]["counts"],"wall_seconds":manifest["performance"]["wall_seconds"]},indent=2,sort_keys=True));return 0


if __name__=="__main__": raise SystemExit(main())
