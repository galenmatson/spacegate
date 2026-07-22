#!/usr/bin/env python3
"""Compile selected runtime ARM surfaces without opening stability databases."""

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

from materialize_stellar_leaf_classifications import spectral_class_sql


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_runtime_arm.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-clean-runtime-arm")
PARSECS_TO_LIGHT_YEARS = 3.26156


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
        try:
            result = fn()
        except Exception:
            status = "fail"
            raise
        else:
            status = "pass"
            return result
        finally:
            after = resource.getrusage(resource.RUSAGE_SELF)
            self.phases.append({
                "phase": name,
                "wall_seconds": round(time.monotonic() - started, 6),
                "cpu_seconds": round(time.process_time() - cpu_started, 6),
                "peak_rss_kib_after": int(after.ru_maxrss),
                "input_blocks_delta": int(after.ru_inblock - before.ru_inblock),
                "output_blocks_delta": int(after.ru_oublock - before.ru_oublock),
                "status": status,
            })

    def report(self) -> dict[str, Any]:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {
            "wall_seconds": round(time.monotonic() - self.started, 6),
            "cpu_seconds": round(time.process_time() - self.cpu_started, 6),
            "peak_rss_kib": int(usage.ru_maxrss),
            "phases": self.phases,
        }


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_runtime_arm_policy.v1":
        raise ValueError("unsupported clean runtime ARM policy")
    required_rules = {
        "open_stability_databases": False,
        "canonical_containment_from_runtime_hierarchy_only": True,
        "scientific_values_from_selected_facts_only": True,
        "source_relation_claims_create_containment": False,
        "context_only_orbits_create_renderable_edges": False,
        "copy_historical_evidence_warehouse_into_runtime_arm": False,
    }
    rules = policy.get("rules") or {}
    if any(rules.get(key) is not expected for key, expected in required_rules.items()):
        raise ValueError("unsafe clean runtime ARM rules")
    if set(policy.get("inputs") or {}) != {
        "clean_runtime_core", "clean_science", "clean_wise"
    }:
        raise ValueError("clean runtime ARM inputs are incomplete")
    for name, spec in policy["inputs"].items():
        path = Path(str(spec.get("relative_path") or ""))
        if (
            not spec.get("build_id")
            or len(str(spec.get("manifest_sha256") or "")) != 64
            or path.is_absolute()
            or ".." in path.parts
        ):
            raise ValueError(f"invalid bounded input contract: {name}")
    science_tables = [str(value) for value in policy.get("selected_science_tables") or []]
    wise_tables = [str(value) for value in policy.get("clean_wise_tables") or []]
    if len(science_tables) != len(set(science_tables)) or len(wise_tables) != len(set(wise_tables)):
        raise ValueError("duplicate runtime ARM input table")
    if "selected_stellar_parameters" not in science_tables:
        raise ValueError("selected stellar consumer surface is required")


def resolve_inputs(policy: dict[str, Any], state: Path) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for name, spec in policy["inputs"].items():
        root = (state / spec["relative_path"]).resolve()
        manifest_path = root / "manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(manifest_path)
        manifest_sha = file_sha256(manifest_path)
        if manifest_sha != spec["manifest_sha256"]:
            raise ValueError(f"manifest checksum mismatch: {name}")
        manifest = load_object(manifest_path)
        if manifest.get("build_id") != spec["build_id"] or manifest.get("status") != "pass":
            raise ValueError(f"unaccepted input manifest: {name}")
        resolved[name] = {
            "root": root,
            "manifest": manifest,
            "manifest_sha256": manifest_sha,
            "build_id": str(spec["build_id"]),
        }
    return resolved


def product_path(input_spec: dict[str, Any], relative: str) -> Path:
    path = input_spec["root"] / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    products = input_spec["manifest"].get("products") or {}
    entry = products.get(relative)
    if not isinstance(entry, dict) or len(str(entry.get("sha256") or "")) != 64:
        raise ValueError(f"unregistered input product: {path}")
    if file_sha256(path) != entry["sha256"]:
        raise ValueError(f"input product checksum mismatch: {path}")
    return path


def configure(con: duckdb.DuckDBPyConnection, staging: Path) -> None:
    temporary = staging / "duckdb-tmp"
    temporary.mkdir()
    con.execute("SET threads=16")
    con.execute("SET memory_limit='48GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory={sql_literal(temporary)}")


def attach_inputs(
    con: duckdb.DuckDBPyConnection,
    *,
    core_db: Path,
    hierarchy_db: Path,
    science_db: Path,
    wise_db: Path,
) -> None:
    con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(hierarchy_db)} AS hierarchy (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(science_db)} AS science (READ_ONLY)")
    con.execute(f"ATTACH {sql_literal(wise_db)} AS wise (READ_ONLY)")


def copy_selected_surfaces(
    con: duckdb.DuckDBPyConnection,
    *,
    policy: dict[str, Any],
    build_id: str,
    timing: Timings,
) -> None:
    def create_metadata() -> None:
        con.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
        con.executemany(
            "INSERT INTO build_metadata VALUES (?,?)",
            [
                ("build_id", build_id),
                ("build_kind", "e7_clean_runtime_arm"),
                ("policy_version", policy["policy_version"]),
                ("compiler_version", policy["compiler_version"]),
                ("stability_database_opened", "0"),
                ("scientific_values_from_selected_facts_only", "1"),
                ("canonical_containment_from_runtime_hierarchy_only", "1"),
            ],
        )

    timing.run("create_build_metadata", create_metadata)
    for table in policy["selected_science_tables"]:
        timing.run(
            f"copy_science_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM science.{table}"
            ),
        )
    for table in policy["clean_wise_tables"]:
        timing.run(
            f"copy_wise_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM wise.{table}"
            ),
        )

    def create_compatibility_views() -> None:
        # Transitional read-only names keep current runtime consumers on the
        # shared selection rather than preserving a second selection path.
        con.execute(
            "CREATE VIEW e6_selected_stellar_parameters AS "
            "SELECT *, 'evidence_lake_v2_selected_facts'::VARCHAR AS parameter_source "
            "FROM selected_stellar_parameters"
        )
        con.execute(
            "CREATE VIEW e6_selected_stellar_display_classifications AS "
            "SELECT * FROM selected_stellar_display_classifications"
        )
        con.execute(
            "CREATE VIEW e6_selected_planet_parameters AS "
            "SELECT * FROM selected_planet_parameters"
        )

    timing.run(
        "create_selected_surface_compatibility_views",
        create_compatibility_views,
    )


def component_key_sql(alias: str) -> str:
    return f"""
      CASE
        WHEN {alias}.node_kind='system' THEN 'comp:system:' || {alias}.canonical_key
        WHEN {alias}.node_kind='star' THEN 'comp:star:' || {alias}.canonical_key
        WHEN {alias}.node_kind='planet' THEN 'comp:planet:' || {alias}.canonical_key
        WHEN {alias}.node_kind='inferred_star_leaf' THEN
          'comp:msc:wds:' || {alias}.wds_id || ':' || lower(split_part({alias}.hierarchy_node_key, ':', 5))
        ELSE 'comp:hierarchy:' || {alias}.hierarchy_node_key
      END
    """


def create_component_graph(con: duckdb.DuckDBPyConnection, build_id: str) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_runtime_component_nodes AS
        SELECT n.*,
          ('comp:system:' || n.canonical_key)::VARCHAR AS stable_component_key,
          'system'::VARCHAR AS runtime_component_type,
          'system'::VARCHAR AS core_object_type,
          sys.system_id::HUGEINT AS system_id,sys.system_id::HUGEINT AS core_object_id,
          sys.ra_deg::DOUBLE AS ra_deg,sys.dec_deg::DOUBLE AS dec_deg,
          (sys.dist_ly/{PARSECS_TO_LIGHT_YEARS})::DOUBLE AS dist_pc,
          NULL::VARCHAR AS catalog_component_label
        FROM hierarchy.hierarchy_nodes n
        JOIN core.systems sys ON n.canonical_key=sys.stable_object_key
        WHERE n.node_kind='system'
        UNION ALL
        SELECT n.*,'comp:star:' || n.canonical_key,'star','star',
          s.system_id,s.star_id,s.ra_deg,s.dec_deg,
          s.dist_ly/{PARSECS_TO_LIGHT_YEARS},nullif(trim(s.component),'')
        FROM hierarchy.hierarchy_nodes n
        JOIN core.stars s ON n.canonical_key=s.stable_object_key
        WHERE n.node_kind='star'
        UNION ALL
        SELECT n.*,'comp:planet:' || n.canonical_key,'planet','planet',
          p.system_id,p.planet_id,NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::VARCHAR
        FROM hierarchy.hierarchy_nodes n
        JOIN core.planets p ON n.canonical_key=p.stable_object_key
        WHERE n.node_kind='planet'
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE runtime_node_system_bindings AS
        WITH RECURSIVE descendants(hierarchy_node_key,system_id,depth) AS (
          SELECT e.child_node_key,parent.system_id,1::INTEGER
          FROM canonical_runtime_component_nodes parent
          JOIN hierarchy.hierarchy_edges e
            ON e.parent_node_key=parent.hierarchy_node_key
          JOIN hierarchy.hierarchy_nodes child
            ON child.hierarchy_node_key=e.child_node_key
          WHERE child.node_kind NOT IN ('system','star','planet')
          UNION ALL
          SELECT e.child_node_key,d.system_id,d.depth+1
          FROM descendants d
          JOIN hierarchy.hierarchy_edges e
            ON e.parent_node_key=d.hierarchy_node_key
          JOIN hierarchy.hierarchy_nodes child
            ON child.hierarchy_node_key=e.child_node_key
          WHERE d.depth<8
            AND child.node_kind NOT IN ('system','star','planet')
        )
        SELECT hierarchy_node_key,min(system_id)::HUGEINT AS system_id,
          count(DISTINCT system_id)::BIGINT AS distinct_system_count
        FROM descendants
        GROUP BY hierarchy_node_key
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE runtime_component_nodes AS
        SELECT * FROM canonical_runtime_component_nodes
        UNION ALL
        SELECT n.*,
          CASE WHEN n.node_kind='inferred_star_leaf' THEN
            'comp:msc:wds:' || n.wds_id || ':' || lower(split_part(n.hierarchy_node_key, ':', 5))
            ELSE 'comp:hierarchy:' || n.hierarchy_node_key END,
          CASE WHEN n.node_kind='inferred_star_leaf' THEN 'star'
            ELSE coalesce(n.component_type,'component') END,
          NULL::VARCHAR,b.system_id,NULL::HUGEINT,
          NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,
          CASE WHEN n.node_kind='inferred_star_leaf'
            THEN split_part(n.hierarchy_node_key, ':', 5) END
        FROM hierarchy.hierarchy_nodes n
        LEFT JOIN runtime_node_system_bindings b
          ON b.hierarchy_node_key=n.hierarchy_node_key
        WHERE n.node_kind NOT IN ('system','star','planet')
        """
    )
    con.execute(
        f"""
        CREATE TABLE component_entities AS
        SELECT row_number() OVER (ORDER BY hierarchy_node_key)::BIGINT AS component_entity_id,
          stable_component_key,runtime_component_type AS component_type,
          core_object_type,core_object_id,display_name,catalog_component_label,
          ra_deg,dec_deg,dist_pc,
          'canonical_hierarchy'::VARCHAR AS source_catalog,
          {sql_literal(build_id)}::VARCHAR AS source_version,
          hierarchy_node_key::VARCHAR AS source_pk,
          sha256(concat_ws('|',hierarchy_node_key,stable_component_key,source_basis))::VARCHAR
            AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,'e7_clean_runtime_component_graph_v1'::VARCHAR
            AS transform_version
        FROM runtime_component_nodes
        ORDER BY hierarchy_node_key
        """
    )
    con.execute(
        f"""
        CREATE TABLE system_hierarchy_edges AS
        SELECT e.hierarchy_edge_id,
          parent.stable_component_key::VARCHAR AS parent_component_key,
          child.stable_component_key::VARCHAR AS child_component_key,
          e.edge_kind,e.member_role,NULL::VARCHAR AS catalog_relation_label,
          NULL::INTEGER AS depth_hint,e.confidence_score,
          CASE WHEN e.confidence_score>=0.95 THEN 'high'
               WHEN e.confidence_score>=0.70 THEN 'medium' ELSE 'low' END::VARCHAR
            AS confidence_tier,
          to_json([e.source_basis])::VARCHAR AS evidence_catalogs_json,
          to_json([e.source_hierarchy_edge_id])::JSON AS evidence_ids_json,
          'canonical_hierarchy'::VARCHAR AS source_catalog,
          {sql_literal(build_id)}::VARCHAR AS source_version,
          e.hierarchy_edge_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',e.hierarchy_edge_id,parent.stable_component_key,
            child.stable_component_key,e.source_basis))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,'e7_clean_runtime_component_graph_v1'::VARCHAR
            AS transform_version
        FROM hierarchy.hierarchy_edges e
        JOIN runtime_component_nodes parent ON parent.hierarchy_node_key=e.parent_node_key
        JOIN runtime_component_nodes child ON child.hierarchy_node_key=e.child_node_key
        ORDER BY e.hierarchy_edge_id
        """
    )


def create_leaf_classifications(con: duckdb.DuckDBPyConnection, build_id: str) -> None:
    msc_class = spectral_class_sql("cp.classification_raw", "cp.classification_normalized", "'star'")
    con.execute(
        f"""
        CREATE TABLE stellar_leaf_display_classifications AS
        WITH leaves AS (
          SELECT n.*,
            sys.stable_object_key AS system_stable_object_key,
            CASE WHEN n.node_kind='star' THEN n.core_object_id END::HUGEINT AS star_id,
            CASE WHEN n.node_kind='star' THEN n.canonical_key END::VARCHAR AS stable_object_key,
            n.stable_component_key AS leaf_component_key,
            CASE WHEN n.node_kind='inferred_star_leaf'
              THEN n.stable_component_key END::VARCHAR AS evidence_component_key
          FROM runtime_component_nodes n
          LEFT JOIN core.systems sys ON sys.system_id=n.system_id
          WHERE n.component_family='star'
            AND NOT EXISTS (
              SELECT 1 FROM hierarchy.hierarchy_edges e
              JOIN hierarchy.hierarchy_nodes child
                ON child.hierarchy_node_key=e.child_node_key
              WHERE e.parent_node_key=n.hierarchy_node_key
                AND child.component_family='star'
            )
        ), candidates AS (
          SELECT l.hierarchy_node_key,0::INTEGER evidence_rank,
            d.classification_value,d.classification_status,d.evidence_basis,
            d.selected_fact_id,d.source_value,d.confidence_score,
            'evidence_lake_v2'::VARCHAR source_catalog,
            d.projection_version::VARCHAR source_version,
            d.selected_display_classification_id::VARCHAR source_pk
          FROM leaves l
          JOIN selected_stellar_display_classifications d USING(star_id)
          WHERE d.classification_value<>'UNKNOWN'
          UNION ALL
          SELECT l.hierarchy_node_key,10,{msc_class}::VARCHAR,'source',
            'selected_msc_component_spectral_type',cp.evidence_id,
            cp.classification_raw,0.90,ce.source_id,ce.release_id,cp.evidence_id
          FROM leaves l
          JOIN science.evidence_component_msc_component_entities ce
            ON ce.binding_status='accepted'
           AND ce.canonical_system_stable_object_key=l.system_stable_object_key
           AND lower(ce.component_label_normalized)=lower(l.catalog_component_label)
          JOIN science.evidence_component_msc_classification_projection cp
            ON cp.component_entity_id=ce.component_entity_id
           AND cp.projection_status='eligible_for_quantity_selection'
          WHERE l.node_kind='inferred_star_leaf' AND {msc_class} IS NOT NULL
          UNION ALL
          SELECT l.hierarchy_node_key,20,
            CASE WHEN sp.normalized_value<0.08 THEN 'L'
                 WHEN sp.normalized_value<0.65 THEN 'M'
                 WHEN sp.normalized_value<0.85 THEN 'K'
                 WHEN sp.normalized_value<1.04 THEN 'G'
                 WHEN sp.normalized_value<1.40 THEN 'F'
                 WHEN sp.normalized_value<2.10 THEN 'A'
                 WHEN sp.normalized_value<16.0 THEN 'B' ELSE 'O' END,
            'assumed','selected_msc_component_mass_main_sequence_prior',sp.evidence_id,
            sp.value_raw,0.35,ce.source_id,ce.release_id,sp.evidence_id
          FROM leaves l
          JOIN science.evidence_component_msc_component_entities ce
            ON ce.binding_status='accepted'
           AND ce.canonical_system_stable_object_key=l.system_stable_object_key
           AND lower(ce.component_label_normalized)=lower(l.catalog_component_label)
          JOIN science.evidence_component_msc_stellar_parameter_projection sp
            ON sp.component_entity_id=ce.component_entity_id
           AND sp.projection_status='eligible_for_quantity_selection'
           AND sp.quantity_key='mass' AND sp.normalized_value>0
          WHERE l.node_kind='inferred_star_leaf'
        ), ranked AS (
          SELECT *,row_number() OVER (PARTITION BY hierarchy_node_key
            ORDER BY evidence_rank,confidence_score DESC,selected_fact_id,classification_value) rn
          FROM candidates
          WHERE classification_value IN
            ('O','B','A','F','G','K','M','L','T','Y','WR','WD','NS','PULSAR','MAGNETAR','BLACK HOLE')
        ), conflicts AS (
          SELECT hierarchy_node_key,count(DISTINCT classification_value)::INTEGER
              AS distinct_candidate_class_count,
            to_json(list(DISTINCT classification_value ORDER BY classification_value))::VARCHAR
              AS candidate_classes_json
          FROM candidates GROUP BY hierarchy_node_key
        )
        SELECT row_number() OVER (ORDER BY l.system_id,l.hierarchy_node_key)::BIGINT
            AS stellar_leaf_classification_id,
          {sql_literal(build_id)}::VARCHAR AS build_id,l.system_id,
          l.system_stable_object_key,l.hierarchy_node_key,l.leaf_component_key,
          l.evidence_component_key,l.star_id,l.stable_object_key,l.display_name,
          l.catalog_component_label,l.node_kind,l.source_basis AS hierarchy_source_basis,
          coalesce(r.classification_value,'UNKNOWN')::VARCHAR AS classification_value,
          coalesce(r.classification_status,'missing')::VARCHAR AS classification_status,
          coalesce(r.evidence_basis,'no_selected_leaf_classification')::VARCHAR AS evidence_basis,
          r.selected_fact_id,r.source_catalog,r.source_version,r.source_pk,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,r.source_value,
          coalesce(r.confidence_score,0.0)::DOUBLE AS confidence_score,
          coalesce(c.distinct_candidate_class_count,0)::INTEGER AS distinct_candidate_class_count,
          coalesce(c.candidate_classes_json,'[]')::VARCHAR AS candidate_classes_json,
          (coalesce(c.distinct_candidate_class_count,0)>1)::BOOLEAN
            AS has_classification_conflict,
          'e7_clean_runtime_leaf_classification_v1'::VARCHAR AS projection_version
        FROM leaves l
        LEFT JOIN ranked r ON r.hierarchy_node_key=l.hierarchy_node_key AND r.rn=1
        LEFT JOIN conflicts c ON c.hierarchy_node_key=l.hierarchy_node_key
        ORDER BY l.system_id,l.hierarchy_node_key
        """
    )


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    statements = (
        "CREATE UNIQUE INDEX component_entities_key_uq ON component_entities(stable_component_key)",
        "CREATE INDEX component_entities_core_idx ON component_entities(core_object_type,core_object_id)",
        "CREATE INDEX hierarchy_parent_idx ON system_hierarchy_edges(parent_component_key)",
        "CREATE INDEX hierarchy_child_idx ON system_hierarchy_edges(child_component_key)",
        "CREATE UNIQUE INDEX leaf_hierarchy_uq ON stellar_leaf_display_classifications(hierarchy_node_key)",
        "CREATE INDEX leaf_system_idx ON stellar_leaf_display_classifications(system_id)",
        "CREATE UNIQUE INDEX selected_stellar_parameters_star_uq ON selected_stellar_parameters(star_id)",
        "CREATE UNIQUE INDEX selected_display_star_uq ON selected_stellar_display_classifications(star_id)",
        "CREATE UNIQUE INDEX selected_planet_parameters_planet_uq ON selected_planet_parameters(planet_id)",
    )
    for statement in statements:
        con.execute(statement)


def verify(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> dict[str, Any]:
    scalar = lambda query: int(con.execute(query).fetchone()[0] or 0)
    counts = {
        table: scalar(f"SELECT count(*) FROM {table}")
        for table in (
            "component_entities", "system_hierarchy_edges",
            "stellar_leaf_display_classifications", "selected_stellar_parameters",
            "selected_stellar_display_classifications", "selected_planet_parameters",
            "selected_stellar_variability", "wise_sources",
        )
    }
    checks = {
        "stability_metadata_opened": scalar(
            "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"
        ),
        "component_inventory_delta": abs(
            counts["component_entities"] - scalar("SELECT count(*) FROM hierarchy.hierarchy_nodes")
        ),
        "hierarchy_edge_inventory_delta": abs(
            counts["system_hierarchy_edges"] - scalar("SELECT count(*) FROM hierarchy.hierarchy_edges")
        ),
        "orphan_hierarchy_parents": scalar(
            "SELECT count(*) FROM system_hierarchy_edges e LEFT JOIN component_entities c "
            "ON c.stable_component_key=e.parent_component_key WHERE c.component_entity_id IS NULL"
        ),
        "orphan_hierarchy_children": scalar(
            "SELECT count(*) FROM system_hierarchy_edges e LEFT JOIN component_entities c "
            "ON c.stable_component_key=e.child_component_key WHERE c.component_entity_id IS NULL"
        ),
        "duplicate_component_keys": scalar(
            "SELECT count(*) FROM (SELECT stable_component_key FROM component_entities "
            "GROUP BY 1 HAVING count(*)<>1)"
        ),
        "duplicate_leaf_keys": scalar(
            "SELECT count(*) FROM (SELECT hierarchy_node_key FROM stellar_leaf_display_classifications "
            "GROUP BY 1 HAVING count(*)<>1)"
        ),
        "selected_stellar_inventory_delta": abs(
            counts["selected_stellar_parameters"] - scalar("SELECT count(*) FROM core.stars")
        ),
        "selected_display_inventory_delta": abs(
            counts["selected_stellar_display_classifications"] - scalar("SELECT count(*) FROM core.stars")
        ),
        "selected_planet_inventory_delta": abs(
            counts["selected_planet_parameters"]
            - scalar("SELECT count(*) FROM science.selected_planet_parameters")
        ),
        "selected_planets_outside_canonical_inventory": scalar(
            "SELECT count(*) FROM selected_planet_parameters p LEFT JOIN core.planets c USING(planet_id) "
            "WHERE c.planet_id IS NULL"
        ),
        "ambiguous_component_system_bindings": scalar(
            "SELECT count(*) FROM runtime_node_system_bindings WHERE distinct_system_count<>1"
        ),
        "unbound_noncanonical_components": scalar(
            "SELECT count(*) FROM runtime_component_nodes "
            "WHERE node_kind NOT IN ('system','star','planet') AND system_id IS NULL"
        ),
        "invalid_leaf_classes": scalar(
            "SELECT count(*) FROM stellar_leaf_display_classifications WHERE classification_value NOT IN "
            "('O','B','A','F','G','K','M','L','T','Y','WR','WD','NS','PULSAR','MAGNETAR','BLACK HOLE','UNKNOWN')"
        ),
        "source_claim_containment_edges": scalar(
            "SELECT count(*) FROM system_hierarchy_edges WHERE source_catalog<>'canonical_hierarchy'"
        ),
    }
    failing = {key: value for key, value in checks.items() if value != 0}
    return {
        "status": "pass" if not failing else "fail",
        "counts": counts,
        "checks": checks,
        "failing_checks": failing,
        "accounting": {
            "canonical_planets_without_selected_parameters": scalar(
                "SELECT count(*) FROM core.planets p LEFT JOIN selected_planet_parameters s USING(planet_id) "
                "WHERE s.planet_id IS NULL"
            )
        },
        "runtime_graph_status": policy["runtime_graph_status"],
    }


def compile_runtime_arm(
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
    core_db = timing.run(
        "verify_runtime_core", lambda: product_path(inputs["clean_runtime_core"], "core.duckdb")
    )
    hierarchy_db = timing.run(
        "verify_runtime_hierarchy",
        lambda: product_path(inputs["clean_runtime_core"], "canonical_hierarchy.duckdb"),
    )
    science_db = timing.run(
        "verify_clean_science", lambda: product_path(inputs["clean_science"], "clean_science.duckdb")
    )
    wise_db = timing.run(
        "verify_clean_wise", lambda: product_path(inputs["clean_wise"], "clean_wise.duckdb")
    )
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    input_identity = {
        name: {"build_id": spec["build_id"], "manifest_sha256": spec["manifest_sha256"]}
        for name, spec in inputs.items()
    }
    build_id = stable_hash({
        "compiler_sha256": compiler_sha,
        "policy_sha256": policy_sha,
        "inputs": input_identity,
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("clean runtime ARM build collision")
        return manifest

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    arm_db = staging / "arm.duckdb"
    try:
        con = duckdb.connect(str(arm_db))
        configure(con, staging)
        try:
            timing.run(
                "attach_clean_inputs",
                lambda: attach_inputs(
                    con, core_db=core_db, hierarchy_db=hierarchy_db,
                    science_db=science_db, wise_db=wise_db,
                ),
            )
            copy_selected_surfaces(
                con,
                policy=policy,
                build_id=build_id,
                timing=timing,
            )
            timing.run("component_graph", lambda: create_component_graph(con, build_id))
            timing.run("stellar_leaf_classifications", lambda: create_leaf_classifications(con, build_id))
            verification = timing.run("internal_verification", lambda: verify(con, policy))
            if verification["status"] != "pass":
                raise ValueError(f"clean runtime ARM verification failed: {verification['failing_checks']}")
            timing.run("indexes", lambda: create_indexes(con))
            timing.run("checkpoint", lambda: con.execute("CHECKPOINT"))
        finally:
            con.close()
        product = {
            "bytes": arm_db.stat().st_size,
            "sha256": timing.run("database_hashing", lambda: file_sha256(arm_db)),
            "determinism": "logical_tables",
        }
        manifest = {
            "schema_version": "spacegate.e7_clean_runtime_arm_manifest.v1",
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
            "products": {"arm.duckdb": product},
            "performance": timing.report(),
        }
        write_object_atomic(staging / "manifest.json", manifest)
        shutil.rmtree(staging / "duckdb-tmp", ignore_errors=True)
        os.replace(staging, final_dir)
        if link_into_state:
            link_root = state / "derived/evidence_lake_v2/clean_runtime_arm"
            link_root.mkdir(parents=True, exist_ok=True)
            link = link_root / build_id
            if link.is_symlink() or link.exists():
                if link.resolve() != final_dir.resolve():
                    raise ValueError(f"clean runtime ARM state link collision: {link}")
            else:
                link.symlink_to(final_dir)
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
    manifest = compile_runtime_arm(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps({
        "build_id": manifest["build_id"],
        "status": manifest["status"],
        "counts": manifest["verification"]["counts"],
        "runtime_graph_status": manifest["verification"]["runtime_graph_status"],
        "wall_seconds": manifest["performance"]["wall_seconds"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
