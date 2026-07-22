#!/usr/bin/env python3
"""Bind selected stellar-orbit endpoints to exact clean runtime leaves."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_stellar_orbit_endpoint_bridge.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-stellar-orbit-endpoint-bridge")


def load_object(path: Path) -> dict[str, Any]:
    value=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value,dict):raise ValueError(f"expected JSON object: {path}")
    return value


def write_object_atomic(path: Path,value: Any)->None:
    path.parent.mkdir(parents=True,exist_ok=True);temporary=path.with_name(f".{path.name}.{os.getpid()}.tmp");temporary.write_text(json.dumps(value,indent=2,sort_keys=True)+"\n",encoding="utf-8");os.replace(temporary,path)


def file_sha256(path: Path)->str:
    digest=hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda:handle.read(8*1024*1024),b""):digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any)->str:
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()


def sql_literal(value: Any)->str:
    return "'"+str(value).replace("'","''")+"'"


def validate_policy(policy: dict[str,Any])->None:
    if policy.get("schema_version")!="spacegate.e7_stellar_orbit_endpoint_bridge_policy.v1":raise ValueError("unsupported endpoint bridge policy")
    expected={"open_stability_databases":False,"name_or_coordinate_endpoint_matching":False,"casefold_matching_requires_unique_source_and_runtime_leaf":True,"unresolved_endpoints_create_runtime_components":False,"unresolved_relations_create_runtime_edges":False,"source_relations_create_containment":False,"simulation_requires_selected_coherent_solution":True}
    if policy.get("rules")!=expected:raise ValueError("unsafe endpoint bridge rules")
    if set(policy.get("inputs") or {})!={"selected_orbits","selected_components","clean_runtime_core"}:raise ValueError("incomplete endpoint bridge inputs")
    for name,spec in policy["inputs"].items():
        relative=Path(str(spec.get("relative_path") or ""))
        if not spec.get("build_id") or len(str(spec.get("manifest_sha256") or ""))!=64 or relative.is_absolute() or ".." in relative.parts:raise ValueError(f"invalid endpoint bridge input: {name}")


def accepted_manifest(root: Path,spec: dict[str,Any],*,component: bool=False)->dict[str,Any]:
    path=root/"manifest.json"
    if not path.is_file() or file_sha256(path)!=spec["manifest_sha256"]:raise ValueError(f"input manifest mismatch: {root}")
    manifest=load_object(path)
    if manifest.get("build_id")!=spec["build_id"]:raise ValueError(f"input build mismatch: {root}")
    if component:
        if any(int(value or 0)!=0 for value in (manifest.get("verification") or {}).values()):raise ValueError("selected component input is not accepted")
    elif manifest.get("status")!="pass":raise ValueError(f"input is not accepted: {root}")
    return manifest


def resolve_inputs(policy: dict[str,Any],state: Path)->dict[str,Path]:
    roots={name:(state/spec["relative_path"]).resolve() for name,spec in policy["inputs"].items()}
    orbit_manifest=accepted_manifest(roots["selected_orbits"],policy["inputs"]["selected_orbits"])
    component_manifest=accepted_manifest(roots["selected_components"],policy["inputs"]["selected_components"],component=True)
    core_manifest=accepted_manifest(roots["clean_runtime_core"],policy["inputs"]["clean_runtime_core"])
    paths={
        "relations":roots["selected_orbits"]/"selected_stellar_orbit_relations.parquet",
        "solutions":roots["selected_orbits"]/"selected_stellar_orbit_solutions.parquet",
        "components":roots["selected_components"]/"selected_components.duckdb",
        "hierarchy":roots["clean_runtime_core"]/"canonical_hierarchy.duckdb",
    }
    for filename,key in (("selected_stellar_orbit_relations.parquet","relations"),("selected_stellar_orbit_solutions.parquet","solutions")):
        expected=(orbit_manifest.get("products",{}).get(filename) or {}).get("sha256")
        if file_sha256(paths[key])!=expected:raise ValueError(f"selected orbit product mismatch: {filename}")
    if file_sha256(paths["components"])!=policy["inputs"]["selected_components"]["database_sha256"] or (component_manifest.get("files",{}).get("selected_components.duckdb") or {}).get("sha256")!=policy["inputs"]["selected_components"]["database_sha256"]:raise ValueError("selected component database mismatch")
    if file_sha256(paths["hierarchy"])!=policy["inputs"]["clean_runtime_core"]["hierarchy_sha256"] or (core_manifest.get("products",{}).get("canonical_hierarchy.duckdb") or {}).get("sha256")!=policy["inputs"]["clean_runtime_core"]["hierarchy_sha256"]:raise ValueError("clean hierarchy database mismatch")
    return paths


def materialize(con: duckdb.DuckDBPyConnection,policy: dict[str,Any],build_id: str)->None:
    version=sql_literal(policy["policy_version"]);build=sql_literal(build_id)
    con.execute(f"""
      CREATE TEMP TABLE selected_endpoint_keys AS
      SELECT primary_source_component_key source_component_key FROM orbit_relations
      UNION SELECT secondary_source_component_key FROM orbit_relations;

      CREATE TEMP TABLE accepted_source_case_groups AS
      SELECT wds_id_raw,lower(component_label_raw) casefold_label,count(*) source_candidate_count
      FROM component_scope.msc_component_entities WHERE binding_status='accepted'
      GROUP BY ALL;

      CREATE TEMP TABLE clean_runtime_leaf_groups AS
      SELECT wds_id,lower(split_part(hierarchy_node_key,':',5)) casefold_label,
        count(*) runtime_candidate_count,min(hierarchy_node_key) hierarchy_node_key,
        min('comp:msc:wds:'||wds_id||':'||lower(split_part(hierarchy_node_key,':',5))) runtime_component_key
      FROM hierarchy.hierarchy_nodes WHERE node_kind='inferred_star_leaf'
      GROUP BY ALL;

      CREATE TABLE stellar_orbit_endpoint_bindings AS
      SELECT sha256(concat_ws('|','stellar-orbit-endpoint',e.source_component_key,{version})) endpoint_binding_id,
        e.source_component_key,c.component_entity_id,c.source_id,c.release_id,
        c.wds_id_raw,c.component_label_raw,c.component_label_normalized,
        c.canonical_system_stable_object_key,c.scope_semantics,
        coalesce(s.source_candidate_count,0)::BIGINT source_candidate_count,
        coalesce(h.runtime_candidate_count,0)::BIGINT runtime_candidate_count,
        CASE WHEN s.source_candidate_count=1 AND h.runtime_candidate_count=1 THEN h.hierarchy_node_key END hierarchy_node_key,
        CASE WHEN s.source_candidate_count=1 AND h.runtime_candidate_count=1 THEN h.runtime_component_key END runtime_component_key,
        CASE WHEN s.source_candidate_count=1 AND h.runtime_candidate_count=1 THEN 'accepted'
             WHEN s.source_candidate_count>1 THEN 'ambiguous'
             WHEN h.runtime_candidate_count IS NULL THEN 'missing'
             ELSE 'ambiguous' END binding_status,
        CASE WHEN s.source_candidate_count=1 AND h.runtime_candidate_count=1 THEN 'exact_wds_unique_casefold_component'
             WHEN s.source_candidate_count>1 THEN 'case_significant_source_collision'
             WHEN h.runtime_candidate_count IS NULL THEN 'runtime_leaf_missing'
             ELSE 'runtime_leaf_collision' END binding_reason,
        false creates_runtime_component,{version} policy_version,{build} build_id
      FROM selected_endpoint_keys e
      JOIN component_scope.msc_component_entities c USING(source_component_key)
      LEFT JOIN accepted_source_case_groups s ON s.wds_id_raw=c.wds_id_raw AND s.casefold_label=lower(c.component_label_raw)
      LEFT JOIN clean_runtime_leaf_groups h ON h.wds_id=c.wds_id_raw AND h.casefold_label=lower(c.component_label_raw)
      ORDER BY e.source_component_key;

      CREATE TABLE stellar_orbit_relation_bindings AS
      SELECT sha256(concat_ws('|','stellar-orbit-relation-bridge',r.relation_id,{version})) relation_binding_id,
        r.relation_id,r.relation_evidence_id,r.canonical_system_stable_object_key,
        r.primary_source_component_key,r.secondary_source_component_key,
        p.endpoint_binding_id primary_endpoint_binding_id,s.endpoint_binding_id secondary_endpoint_binding_id,
        p.runtime_component_key primary_runtime_component_key,s.runtime_component_key secondary_runtime_component_key,
        r.preferred_simulation_solution_id,
        p.binding_status='accepted' AND s.binding_status='accepted' runtime_eligible,
        r.preferred_simulation_solution_id IS NOT NULL AND p.binding_status='accepted' AND s.binding_status='accepted' simulation_eligible,
        CASE WHEN p.binding_status='accepted' AND s.binding_status='accepted' THEN 'accepted'
             WHEN p.binding_status<>'accepted' AND s.binding_status<>'accepted' THEN 'both_endpoints_unresolved'
             ELSE 'one_endpoint_unresolved' END binding_status,
        false creates_runtime_edge,false canonical_containment,{version} policy_version,{build} build_id
      FROM orbit_relations r
      JOIN stellar_orbit_endpoint_bindings p ON p.source_component_key=r.primary_source_component_key
      JOIN stellar_orbit_endpoint_bindings s ON s.source_component_key=r.secondary_source_component_key
      ORDER BY r.relation_id;
    """)


def verify(con: duckdb.DuckDBPyConnection,policy: dict[str,Any])->dict[str,Any]:
    scalar=lambda sql:int(con.execute(sql).fetchone()[0] or 0)
    counts={
      "endpoints":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings"),
      "endpoints_accepted":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status='accepted'"),
      "endpoints_missing_runtime_leaf":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_reason='runtime_leaf_missing'"),
      "endpoints_ambiguous_source_case":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_reason='case_significant_source_collision'"),
      "relations":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings"),
      "relations_runtime_eligible":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE runtime_eligible"),
      "relations_one_endpoint_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE binding_status='one_endpoint_unresolved'"),
      "relations_both_endpoints_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE binding_status='both_endpoints_unresolved'"),
      "preferred_relations":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL"),
      "preferred_relations_runtime_eligible":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL AND runtime_eligible"),
      "preferred_relations_one_endpoint_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL AND binding_status='one_endpoint_unresolved'"),
      "preferred_relations_both_endpoints_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL AND binding_status='both_endpoints_unresolved'"),
      "containment_promotions":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE canonical_containment"),
    }
    expected={key:int(value) for key,value in policy["acceptance"].items()}
    checks={"acceptance_count_delta":sum(abs(counts.get(key,-1)-value) for key,value in expected.items()),"duplicate_endpoints":scalar("SELECT count(*) FROM (SELECT source_component_key FROM stellar_orbit_endpoint_bindings GROUP BY 1 HAVING count(*)<>1)"),"duplicate_relations":scalar("SELECT count(*) FROM (SELECT relation_id FROM stellar_orbit_relation_bindings GROUP BY 1 HAVING count(*)<>1)"),"accepted_without_runtime_key":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status='accepted' AND (runtime_component_key IS NULL OR hierarchy_node_key IS NULL)"),"unaccepted_with_runtime_key":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status<>'accepted' AND runtime_component_key IS NOT NULL"),"unresolved_runtime_edges":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE NOT runtime_eligible AND creates_runtime_edge"),"invalid_simulation_eligibility":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible AND (NOT runtime_eligible OR preferred_simulation_solution_id IS NULL)"),"system_scope_mismatch":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings r JOIN stellar_orbit_endpoint_bindings p ON p.endpoint_binding_id=r.primary_endpoint_binding_id JOIN stellar_orbit_endpoint_bindings s ON s.endpoint_binding_id=r.secondary_endpoint_binding_id WHERE p.canonical_system_stable_object_key<>r.canonical_system_stable_object_key OR s.canonical_system_stable_object_key<>r.canonical_system_stable_object_key")}
    failing={key:value for key,value in checks.items() if value};return {"status":"pass" if not failing else "fail","counts":counts,"expected_counts":expected,"checks":checks,"failing_checks":failing}


def compile_bridge(policy_path: Path,state: Path,output_root: Path,*,link_into_state: bool)->dict[str,Any]:
    started=time.monotonic();cpu_started=time.process_time();policy=load_object(policy_path);validate_policy(policy);paths=resolve_inputs(policy,state);policy_sha=file_sha256(policy_path);compiler_sha=file_sha256(Path(__file__).resolve());build_id=stable_hash({"policy_sha256":policy_sha,"compiler_sha256":compiler_sha,"inputs":{name:spec["manifest_sha256"] for name,spec in policy["inputs"].items()}})[:24];final=output_root/build_id
    if (final/"manifest.json").is_file():return load_object(final/"manifest.json")
    output_root.mkdir(parents=True,exist_ok=True);staging=Path(tempfile.mkdtemp(prefix=f".{build_id}.",dir=output_root))
    try:
      con=duckdb.connect();products={}
      try:
        con.execute(f"ATTACH {sql_literal(paths['components'])} AS component_scope (READ_ONLY)");con.execute(f"ATTACH {sql_literal(paths['hierarchy'])} AS hierarchy (READ_ONLY)");con.execute(f"CREATE VIEW orbit_relations AS SELECT * FROM read_parquet({sql_literal(paths['relations'])})");con.execute(f"CREATE VIEW orbit_solutions AS SELECT * FROM read_parquet({sql_literal(paths['solutions'])})");materialize(con,policy,build_id);verification=verify(con,policy)
        if verification["status"]!="pass":raise ValueError(f"endpoint bridge verification failed: {verification['failing_checks']}")
        for table in ("stellar_orbit_endpoint_bindings","stellar_orbit_relation_bindings"):
          path=staging/f"{table}.parquet";con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO {sql_literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)");products[path.name]={"rows":int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),"bytes":path.stat().st_size,"sha256":file_sha256(path),"determinism":"byte_exact"}
      finally:con.close()
      manifest={"schema_version":"spacegate.e7_stellar_orbit_endpoint_bridge_manifest.v1","build_id":build_id,"status":"pass","generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),"policy_version":policy["policy_version"],"compiler_version":policy["compiler_version"],"policy_sha256":policy_sha,"compiler_sha256":compiler_sha,"inputs":{name:{"build_id":spec["build_id"],"manifest_sha256":spec["manifest_sha256"]} for name,spec in policy["inputs"].items()},"stability_databases_opened":[],"verification":verification,"products":products,"performance":{"wall_seconds":round(time.monotonic()-started,6),"cpu_seconds":round(time.process_time()-cpu_started,6),"peak_rss_kib":int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)}};write_object_atomic(staging/"manifest.json",manifest);os.replace(staging,final)
      if link_into_state:
        root=state/"derived/evidence_lake_v2/stellar_orbit_endpoint_bridge";root.mkdir(parents=True,exist_ok=True);link=root/build_id
        if not link.exists() and not link.is_symlink():link.symlink_to(final)
      return manifest
    except Exception:shutil.rmtree(staging,ignore_errors=True);raise


def main()->int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--policy",type=Path,default=DEFAULT_POLICY);parser.add_argument("--state-dir",type=Path,default=DEFAULT_STATE);parser.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT);parser.add_argument("--no-state-link",action="store_true");parser.add_argument("--report",type=Path);args=parser.parse_args();manifest=compile_bridge(args.policy.resolve(),args.state_dir.resolve(),args.output_root.resolve(),link_into_state=not args.no_state_link)
    if args.report:write_object_atomic(args.report.resolve(),manifest)
    print(json.dumps({"build_id":manifest["build_id"],"status":manifest["status"],"counts":manifest["verification"]["counts"],"wall_seconds":manifest["performance"]["wall_seconds"]},indent=2,sort_keys=True));return 0


if __name__=="__main__":raise SystemExit(main())
