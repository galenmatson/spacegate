#!/usr/bin/env python3
"""Bind selected stellar-orbit endpoints to exact leaves or source-scoped groups."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
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
    if policy.get("schema_version")!="spacegate.e7_stellar_orbit_endpoint_bridge_policy.v4":raise ValueError("unsupported endpoint bridge policy")
    expected={"open_stability_databases":False,"name_or_coordinate_endpoint_matching":False,"casefold_matching_requires_unique_source_and_runtime_leaf":True,"source_numeric_component_designations_define_groups":True,"unresolved_endpoints_create_runtime_components":False,"unresolved_relations_create_runtime_edges":False,"source_relations_create_containment":False,"source_subsystem_groups_create_canonical_containment":False,"source_subsystem_groups_create_presentation_endpoints":True,"simulation_requires_selected_solution_or_source_relation_period":True}
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

      CREATE TEMP TABLE needed_leaf_endpoint_keys AS
      SELECT source_component_key FROM selected_endpoint_keys
      UNION
      SELECT child.source_component_key
      FROM selected_endpoint_keys selected
      JOIN component_scope.msc_component_entities parent
        ON parent.source_component_key=selected.source_component_key
       AND parent.binding_status='accepted'
      JOIN component_scope.msc_component_entities child
        ON child.source_id=parent.source_id
       AND child.release_id=parent.release_id
       AND child.wds_id_raw=parent.wds_id_raw
       AND child.binding_status='accepted'
       AND starts_with(child.component_label_raw,parent.component_label_raw)
       AND regexp_full_match(
             substr(child.component_label_raw,length(parent.component_label_raw)+1),
             '[0-9]+'
           );

      CREATE TEMP TABLE accepted_source_label_groups AS
      SELECT wds_id_raw,component_label_raw,count(*) source_candidate_count
      FROM component_scope.msc_component_entities WHERE binding_status='accepted'
      GROUP BY ALL;

      CREATE TEMP TABLE clean_runtime_leaf_groups AS
      WITH RECURSIVE hierarchy_star_leaves AS (
        SELECT system.wds_id,e.child_node_key,
          lower(coalesce(nullif(trim(e.member_role),''),nullif(trim(child.member_role),'')))
            AS casefold_label,
          child.node_kind,child.canonical_key,1::INTEGER depth
        FROM hierarchy.hierarchy_nodes system
        JOIN hierarchy.hierarchy_edges e ON e.parent_node_key=system.hierarchy_node_key
        JOIN hierarchy.hierarchy_nodes child ON child.hierarchy_node_key=e.child_node_key
        WHERE system.node_kind='system' AND system.wds_id IS NOT NULL
        UNION ALL
        SELECT parent.wds_id,e.child_node_key,
          lower(coalesce(nullif(trim(e.member_role),''),nullif(trim(child.member_role),'')))
            AS casefold_label,
          child.node_kind,child.canonical_key,parent.depth+1
        FROM hierarchy_star_leaves parent
        JOIN hierarchy.hierarchy_edges e ON e.parent_node_key=parent.child_node_key
        JOIN hierarchy.hierarchy_nodes child ON child.hierarchy_node_key=e.child_node_key
        WHERE parent.depth<8 AND parent.node_kind NOT IN ('star','inferred_star_leaf')
      ), candidates AS (
        SELECT wds_id,lower(split_part(hierarchy_node_key,':',5)) casefold_label,
          hierarchy_node_key,
          'comp:msc:wds:'||wds_id||':'||lower(split_part(hierarchy_node_key,':',5))
            runtime_component_key
        FROM hierarchy.hierarchy_nodes WHERE node_kind='inferred_star_leaf'
        UNION ALL
        SELECT wds_id,casefold_label,child_node_key,
          'comp:star:'||canonical_key
        FROM hierarchy_star_leaves
        WHERE node_kind='star' AND casefold_label IS NOT NULL
      )
      SELECT wds_id,casefold_label,
        count(*) runtime_candidate_count,min(hierarchy_node_key) hierarchy_node_key,
        min(runtime_component_key) runtime_component_key
      FROM candidates
      GROUP BY ALL;

      CREATE TEMP TABLE stellar_orbit_leaf_endpoint_candidates AS
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
        CASE WHEN s.source_candidate_count=1 AND h.runtime_candidate_count=1 THEN 'exact_source_label_unique_runtime_component'
             WHEN s.source_candidate_count>1 THEN 'exact_source_label_collision'
             WHEN h.runtime_candidate_count IS NULL THEN 'runtime_leaf_missing'
             ELSE 'runtime_leaf_collision' END binding_reason,
        false creates_runtime_component
      FROM needed_leaf_endpoint_keys e
      JOIN component_scope.msc_component_entities c USING(source_component_key)
      LEFT JOIN accepted_source_label_groups s ON s.wds_id_raw=c.wds_id_raw AND s.component_label_raw=c.component_label_raw
      LEFT JOIN clean_runtime_leaf_groups h ON h.wds_id=c.wds_id_raw AND h.casefold_label=lower(c.component_label_raw)
      WHERE c.binding_status='accepted'
      ORDER BY e.source_component_key
    """)

    component_columns = [
        "component_entity_id","source_id","release_id","wds_id_raw",
        "component_label_raw","component_label_normalized",
        "canonical_system_stable_object_key",
    ]
    component_rows = {
        str(row[0]): dict(zip(["source_component_key",*component_columns],row))
        for row in con.execute(
            "SELECT source_component_key,"+",".join(component_columns)
            +" FROM component_scope.msc_component_entities WHERE binding_status='accepted'"
        ).fetchall()
    }
    leaf_columns = [row[0] for row in con.execute(
        "DESCRIBE stellar_orbit_leaf_endpoint_candidates"
    ).fetchall()]
    leaf_rows = {
        str(row[leaf_columns.index("source_component_key")]): dict(zip(leaf_columns,row))
        for row in con.execute("SELECT * FROM stellar_orbit_leaf_endpoint_candidates").fetchall()
    }
    projection_rows = {
        str(row[0]): {
            "period_value_raw":row[1],"period_value":row[2],
            "period_unit_raw":row[3],"quality_json":row[4],
            "source_orbit_evidence_id":row[5],
        }
        for row in con.execute("""
          SELECT relation_evidence_id,period_value_raw,period_value,period_unit_raw,
            quality_json,source_orbit_evidence_id
          FROM component_scope.msc_relation_evidence_projection
          WHERE projection_status='accepted_relation_evidence'
        """).fetchall()
    }
    orbit_columns = [row[0] for row in con.execute("DESCRIBE orbit_relations").fetchall()]
    orbit_rows = [
        dict(zip(orbit_columns,row))
        for row in con.execute("SELECT * FROM orbit_relations ORDER BY relation_id").fetchall()
    ]

    components_by_label: dict[tuple[str,str,str,str],list[str]] = {}
    for source_key,row in component_rows.items():
        key=(
            str(row["source_id"]),str(row["release_id"]),str(row["wds_id_raw"]),
            str(row["component_label_raw"]),
        )
        components_by_label.setdefault(key,[]).append(source_key)
    component_labels_by_scope: dict[
        tuple[str,str,str],list[tuple[str,list[str]]]
    ] = {}
    for (source_id,release_id,wds_id,label),source_keys in components_by_label.items():
        component_labels_by_scope.setdefault(
            (source_id,release_id,wds_id),[]
        ).append((label,source_keys))

    group_candidates: dict[str,list[dict[str,Any]]] = {}
    relation_projection: dict[str,dict[str,Any]] = {}
    for relation in orbit_rows:
        projection=projection_rows.get(str(relation["relation_evidence_id"])) or {}
        relation_projection[str(relation["relation_id"])]=projection
        try:
            quality=json.loads(str(projection.get("quality_json") or relation.get("quality_json") or "{}"))
        except json.JSONDecodeError:
            quality={}
        parent_label=str(quality.get("Parent") or "").strip()
        left=component_rows.get(str(relation["primary_source_component_key"]))
        if not left or parent_label in {"","*","t"}:
            continue
        parent_matches=components_by_label.get((
            str(left["source_id"]),str(left["release_id"]),str(left["wds_id_raw"]),parent_label
        ),[])
        if len(parent_matches)!=1:
            continue
        group_candidates.setdefault(parent_matches[0],[]).append({
            "relation_id":str(relation["relation_id"]),
            "children":(
                str(relation["primary_source_component_key"]),
                str(relation["secondary_source_component_key"]),
            ),
        })
    group_definitions={
        key:{**rows[0],"definition_basis":"exact_source_parent_relation"}
        for key,rows in group_candidates.items() if len(rows)==1
    }
    for source_key,component in sorted(component_rows.items()):
        if source_key in group_candidates:
            continue
        label=str(component["component_label_raw"] or "").strip()
        if not label:
            continue
        numeric_descendants=[]
        scope=(
            str(component["source_id"]),str(component["release_id"]),
            str(component["wds_id_raw"]),
        )
        for child_label,candidate_keys in component_labels_by_scope.get(scope,[]):
            if re.fullmatch(re.escape(label)+r"\d+",child_label) is None:
                continue
            if len(candidate_keys)!=1:
                numeric_descendants=[]
                break
            numeric_descendants.append((child_label,candidate_keys[0]))
        if len(numeric_descendants)>=2:
            numeric_descendants.sort(
                key=lambda item:(int(item[0][len(label):]),item[0])
            )
            group_definitions[source_key]={
                "relation_id":None,
                "children":tuple(item[1] for item in numeric_descendants),
                "definition_basis":"exact_source_numeric_descendants",
            }

    resolved_cache: dict[str,dict[str,Any]]={}
    def resolve_endpoint(source_key: str,path: tuple[str,...]=())->dict[str,Any]:
        cached=resolved_cache.get(source_key)
        if cached is not None:
            return cached
        component=component_rows.get(source_key)
        if component is None:
            result={"status":"missing","reason":"source_component_missing","kind":"unresolved","leaves":[]}
        elif source_key in path:
            result={"status":"ambiguous","reason":"source_subsystem_cycle","kind":"unresolved","leaves":[]}
        elif source_key in group_candidates and source_key not in group_definitions:
            result={"status":"ambiguous","reason":"multiple_source_subsystem_definitions","kind":"unresolved","leaves":[]}
        elif source_key in group_definitions:
            children=group_definitions[source_key]["children"]
            child_results=[resolve_endpoint(child,(*path,source_key)) for child in children]
            leaves=[leaf for item in child_results for leaf in item["leaves"]]
            if any(item["status"]!="accepted" for item in child_results):
                result={"status":"missing","reason":"source_subsystem_child_unresolved","kind":"group","leaves":[]}
            elif len(leaves)!=len(set(leaves)):
                result={"status":"ambiguous","reason":"source_subsystem_descendant_overlap","kind":"group","leaves":[]}
            elif len(leaves)<2:
                result={"status":"missing","reason":"source_subsystem_has_fewer_than_two_leaves","kind":"group","leaves":[]}
            else:
                label=str(component["component_label_raw"]).strip()
                definition_basis=str(group_definitions[source_key]["definition_basis"])
                result={
                    "status":"accepted",
                    "reason":(
                        "source_subsystem_numeric_descendant_leaf_set"
                        if definition_basis=="exact_source_numeric_descendants"
                        else "source_subsystem_exact_descendant_leaf_set"
                    ),
                    "kind":"group","leaves":sorted(leaves),
                    "runtime_key":f"comp:msc_group:wds:{component['wds_id_raw']}:{label}",
                    "definition_basis":definition_basis,
                }
        else:
            leaf=leaf_rows.get(source_key)
            if leaf and leaf["binding_status"]=="accepted":
                result={
                    "status":"accepted","reason":str(leaf["binding_reason"]),
                    "kind":"leaf","leaves":[str(leaf["runtime_component_key"])],
                    "runtime_key":str(leaf["runtime_component_key"]),
                }
            else:
                result={
                    "status":str((leaf or {}).get("binding_status") or "missing"),
                    "reason":str((leaf or {}).get("binding_reason") or "runtime_leaf_missing"),
                    "kind":"leaf","leaves":[],
                }
        resolved_cache[source_key]=result
        return result

    endpoint_rows=[]
    for source_key in sorted({str(row["primary_source_component_key"]) for row in orbit_rows}
                             |{str(row["secondary_source_component_key"]) for row in orbit_rows}):
        component=component_rows[source_key]
        leaf=leaf_rows.get(source_key) or {}
        resolved=resolve_endpoint(source_key)
        leaves=resolved["leaves"]
        endpoint_rows.append((
            stable_hash(["stellar-orbit-endpoint",source_key,policy["policy_version"]]),
            source_key,component["component_entity_id"],component["source_id"],component["release_id"],
            component["wds_id_raw"],component["component_label_raw"],component["component_label_normalized"],
            component["canonical_system_stable_object_key"],"source_scoped_subsystem" if resolved["kind"]=="group" else "exact_source_component",
            int(leaf.get("source_candidate_count") or 1),int(leaf.get("runtime_candidate_count") or (1 if resolved["kind"]=="group" else 0)),
            leaf.get("hierarchy_node_key") if resolved["kind"]=="leaf" else None,
            resolved.get("runtime_key"),resolved["kind"],json.dumps(leaves,separators=(",",":")),len(leaves),
            resolved["status"],resolved["reason"],False,policy["policy_version"],build_id,
        ))
    con.execute("""
      CREATE TABLE stellar_orbit_endpoint_bindings (
        endpoint_binding_id VARCHAR,source_component_key VARCHAR,component_entity_id VARCHAR,
        source_id VARCHAR,release_id VARCHAR,wds_id_raw VARCHAR,component_label_raw VARCHAR,
        component_label_normalized VARCHAR,canonical_system_stable_object_key VARCHAR,
        scope_semantics VARCHAR,source_candidate_count BIGINT,runtime_candidate_count BIGINT,
        hierarchy_node_key VARCHAR,runtime_component_key VARCHAR,endpoint_kind VARCHAR,
        descendant_leaf_keys_json JSON,descendant_leaf_count BIGINT,binding_status VARCHAR,
        binding_reason VARCHAR,creates_runtime_component BOOLEAN,policy_version VARCHAR,build_id VARCHAR
      )
    """)
    con.executemany("INSERT INTO stellar_orbit_endpoint_bindings VALUES ("+",".join(["?"]*22)+")",endpoint_rows)

    endpoint_by_key={
        row[1]:dict(zip([
            "endpoint_binding_id","source_component_key","component_entity_id","source_id","release_id",
            "wds_id_raw","component_label_raw","component_label_normalized","canonical_system_stable_object_key",
            "scope_semantics","source_candidate_count","runtime_candidate_count","hierarchy_node_key",
            "runtime_component_key","endpoint_kind","descendant_leaf_keys_json","descendant_leaf_count",
            "binding_status","binding_reason","creates_runtime_component","policy_version","build_id",
        ],row)) for row in endpoint_rows
    }
    membership_rows=[]
    for group_key,definition in sorted(group_definitions.items()):
        group_binding=endpoint_by_key.get(group_key)
        if not group_binding or group_binding["binding_status"]!="accepted":
            continue
        group_component=component_rows[group_key]
        for child_key in definition["children"]:
            child=resolve_endpoint(child_key)
            child_component=component_rows[child_key]
            if child["status"]!="accepted":
                continue
            membership_rows.append((
                stable_hash(["stellar-orbit-group-member",group_key,child_key,policy["policy_version"]]),
                group_key,group_binding["runtime_component_key"],child_key,child["runtime_key"],
                group_component["canonical_system_stable_object_key"],
                group_component["source_id"],group_component["release_id"],
                group_component["wds_id_raw"],
                group_component["component_label_raw"],child_component["component_label_raw"],child["kind"],
                json.dumps(child["leaves"],separators=(",",":")),"accepted",
                (
                    "exact_source_numeric_descendant_definition"
                    if definition["definition_basis"]=="exact_source_numeric_descendants"
                    else "exact_source_subsystem_definition"
                ),
                False,policy["policy_version"],build_id,
            ))
    con.execute("""
      CREATE TABLE stellar_orbit_group_memberships (
        group_membership_id VARCHAR,group_source_component_key VARCHAR,
        group_runtime_component_key VARCHAR,child_source_component_key VARCHAR,
        child_runtime_component_key VARCHAR,canonical_system_stable_object_key VARCHAR,
        source_id VARCHAR,release_id VARCHAR,wds_id_raw VARCHAR,
        group_component_label VARCHAR,child_component_label VARCHAR,
        child_endpoint_kind VARCHAR,child_descendant_leaf_keys_json JSON,binding_status VARCHAR,
        binding_reason VARCHAR,canonical_containment BOOLEAN,policy_version VARCHAR,build_id VARCHAR
      )
    """)
    if membership_rows:
        con.executemany("INSERT INTO stellar_orbit_group_memberships VALUES ("+",".join(["?"]*18)+")",membership_rows)

    def period_days(projection:dict[str,Any])->float|None:
        try:value=float(projection.get("period_value"))
        except (TypeError,ValueError):return None
        unit=str(projection.get("period_unit_raw") or "").strip()
        multiplier={"d":1.0,"y":365.25,"k":365250.0,"M":365250000.0}.get(unit)
        return value*multiplier if multiplier is not None and value>0 else None

    relation_rows=[]
    for relation in orbit_rows:
        primary=endpoint_by_key[str(relation["primary_source_component_key"])]
        secondary=endpoint_by_key[str(relation["secondary_source_component_key"])]
        primary_leaves=set(json.loads(primary["descendant_leaf_keys_json"]))
        secondary_leaves=set(json.loads(secondary["descendant_leaf_keys_json"]))
        accepted=(primary["binding_status"]=="accepted" and secondary["binding_status"]=="accepted"
                  and not (primary_leaves & secondary_leaves))
        if accepted:binding_status="accepted"
        elif primary["binding_status"]!="accepted" and secondary["binding_status"]!="accepted":binding_status="both_endpoints_unresolved"
        elif primary_leaves & secondary_leaves:binding_status="endpoint_descendant_overlap"
        else:binding_status="one_endpoint_unresolved"
        projection=relation_projection[str(relation["relation_id"])]
        source_period_days=period_days(projection)
        simulation_eligible=accepted and (
            relation.get("preferred_simulation_solution_id") is not None
            or source_period_days is not None
        )
        relation_rows.append((
            stable_hash(["stellar-orbit-relation-bridge",relation["relation_id"],policy["policy_version"]]),
            relation["relation_id"],relation["relation_evidence_id"],
            relation["canonical_system_stable_object_key"],
            relation["primary_source_component_key"],relation["secondary_source_component_key"],
            primary["endpoint_binding_id"],secondary["endpoint_binding_id"],
            primary["runtime_component_key"] if accepted else None,
            secondary["runtime_component_key"] if accepted else None,
            primary["endpoint_kind"],secondary["endpoint_kind"],
            primary["descendant_leaf_keys_json"],secondary["descendant_leaf_keys_json"],
            relation.get("preferred_simulation_solution_id"),
            projection.get("source_orbit_evidence_id"),projection.get("period_value_raw"),
            projection.get("period_value"),projection.get("period_unit_raw"),source_period_days,
            accepted,simulation_eligible,binding_status,simulation_eligible,False,
            policy["policy_version"],build_id,
        ))
    con.execute("""
      CREATE TABLE stellar_orbit_relation_bindings (
        relation_binding_id VARCHAR,relation_id VARCHAR,relation_evidence_id VARCHAR,
        canonical_system_stable_object_key VARCHAR,primary_source_component_key VARCHAR,
        secondary_source_component_key VARCHAR,primary_endpoint_binding_id VARCHAR,
        secondary_endpoint_binding_id VARCHAR,primary_runtime_component_key VARCHAR,
        secondary_runtime_component_key VARCHAR,primary_endpoint_kind VARCHAR,
        secondary_endpoint_kind VARCHAR,primary_descendant_leaf_keys_json JSON,
        secondary_descendant_leaf_keys_json JSON,preferred_simulation_solution_id VARCHAR,
        source_orbit_evidence_id VARCHAR,source_period_value_raw VARCHAR,
        source_period_value DOUBLE,source_period_unit_raw VARCHAR,source_period_days DOUBLE,
        runtime_eligible BOOLEAN,simulation_eligible BOOLEAN,binding_status VARCHAR,
        creates_runtime_edge BOOLEAN,canonical_containment BOOLEAN,policy_version VARCHAR,build_id VARCHAR
      )
    """)
    con.executemany("INSERT INTO stellar_orbit_relation_bindings VALUES ("+",".join(["?"]*27)+")",relation_rows)


def verify(con: duckdb.DuckDBPyConnection,policy: dict[str,Any])->dict[str,Any]:
    scalar=lambda sql:int(con.execute(sql).fetchone()[0] or 0)
    counts={
      "endpoints":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings"),
      "endpoints_accepted":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status='accepted'"),
      "endpoints_missing_runtime_leaf":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_reason='runtime_leaf_missing'"),
      "endpoints_ambiguous_source_case":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_reason='exact_source_label_collision'"),
      "relations":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings"),
      "relations_runtime_eligible":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE runtime_eligible"),
      "relations_one_endpoint_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE binding_status='one_endpoint_unresolved'"),
      "relations_both_endpoints_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE binding_status='both_endpoints_unresolved'"),
      "preferred_relations":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL"),
      "preferred_relations_runtime_eligible":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL AND runtime_eligible"),
      "preferred_relations_one_endpoint_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL AND binding_status='one_endpoint_unresolved'"),
      "preferred_relations_both_endpoints_unresolved":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE preferred_simulation_solution_id IS NOT NULL AND binding_status='both_endpoints_unresolved'"),
      "containment_promotions":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE canonical_containment"),
      "group_endpoints_accepted":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE endpoint_kind='group' AND binding_status='accepted'"),
      "group_memberships":scalar("SELECT count(*) FROM stellar_orbit_group_memberships"),
      "preferred_or_source_period_relations_runtime_eligible":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible"),
    }
    expected={key:int(value) for key,value in policy["acceptance"].items()}
    checks={"acceptance_count_delta":sum(abs(counts.get(key,-1)-value) for key,value in expected.items()),"duplicate_endpoints":scalar("SELECT count(*) FROM (SELECT source_component_key FROM stellar_orbit_endpoint_bindings GROUP BY 1 HAVING count(*)<>1)"),"duplicate_accepted_group_runtime_keys":scalar("SELECT count(*) FROM (SELECT runtime_component_key FROM stellar_orbit_endpoint_bindings WHERE endpoint_kind='group' AND binding_status='accepted' GROUP BY 1 HAVING count(*)<>1)"),"duplicate_relations":scalar("SELECT count(*) FROM (SELECT relation_id FROM stellar_orbit_relation_bindings GROUP BY 1 HAVING count(*)<>1)"),"duplicate_group_memberships":scalar("SELECT count(*) FROM (SELECT group_runtime_component_key,child_runtime_component_key FROM stellar_orbit_group_memberships GROUP BY ALL HAVING count(*)<>1)"),"accepted_without_runtime_key":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status='accepted' AND runtime_component_key IS NULL"),"accepted_leaf_without_hierarchy_key":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status='accepted' AND endpoint_kind='leaf' AND hierarchy_node_key IS NULL"),"unaccepted_with_runtime_key":scalar("SELECT count(*) FROM stellar_orbit_endpoint_bindings WHERE binding_status<>'accepted' AND runtime_component_key IS NOT NULL"),"unresolved_runtime_edges":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE NOT runtime_eligible AND creates_runtime_edge"),"invalid_simulation_eligibility":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible AND (NOT runtime_eligible OR (preferred_simulation_solution_id IS NULL AND source_period_days IS NULL))"),"group_membership_containment_promotions":scalar("SELECT count(*) FROM stellar_orbit_group_memberships WHERE canonical_containment"),"relation_endpoint_overlap":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE runtime_eligible AND list_has_any(primary_descendant_leaf_keys_json::VARCHAR[],secondary_descendant_leaf_keys_json::VARCHAR[])"),"system_scope_mismatch":scalar("SELECT count(*) FROM stellar_orbit_relation_bindings r JOIN stellar_orbit_endpoint_bindings p ON p.endpoint_binding_id=r.primary_endpoint_binding_id JOIN stellar_orbit_endpoint_bindings s ON s.endpoint_binding_id=r.secondary_endpoint_binding_id WHERE p.canonical_system_stable_object_key<>r.canonical_system_stable_object_key OR s.canonical_system_stable_object_key<>r.canonical_system_stable_object_key")}
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
        for table in ("stellar_orbit_endpoint_bindings","stellar_orbit_group_memberships","stellar_orbit_relation_bindings"):
          path=staging/f"{table}.parquet";con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO {sql_literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)");products[path.name]={"rows":int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),"bytes":path.stat().st_size,"sha256":file_sha256(path),"determinism":"byte_exact"}
      finally:con.close()
      manifest={"schema_version":"spacegate.e7_stellar_orbit_endpoint_bridge_manifest.v4","build_id":build_id,"status":"pass","generated_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),"policy_version":policy["policy_version"],"compiler_version":policy["compiler_version"],"policy_sha256":policy_sha,"compiler_sha256":compiler_sha,"inputs":{name:{"build_id":spec["build_id"],"manifest_sha256":spec["manifest_sha256"]} for name,spec in policy["inputs"].items()},"stability_databases_opened":[],"verification":verification,"products":products,"performance":{"wall_seconds":round(time.monotonic()-started,6),"cpu_seconds":round(time.process_time()-cpu_started,6),"peak_rss_kib":int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)}};write_object_atomic(staging/"manifest.json",manifest);os.replace(staging,final)
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
