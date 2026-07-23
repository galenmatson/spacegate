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
    if policy.get("schema_version") != "spacegate.e7_clean_runtime_arm_policy.v5":
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
        "clean_runtime_core", "clean_science", "clean_wise",
        "solar_identity", "solar_runtime", "stellar_orbits",
        "stellar_orbit_bridge",
        "tess_runtime",
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
    solar_identity_tables = [str(value) for value in policy.get("solar_identity_tables") or []]
    solar_runtime_tables = [str(value) for value in policy.get("solar_runtime_tables") or []]
    stellar_orbit_tables = [str(value) for value in policy.get("stellar_orbit_tables") or []]
    stellar_orbit_bridge_tables = [str(value) for value in policy.get("stellar_orbit_bridge_tables") or []]
    tess_runtime_tables = [str(value) for value in policy.get("tess_runtime_tables") or []]
    all_tables = (science_tables + wise_tables + solar_identity_tables
                  + solar_runtime_tables + stellar_orbit_tables
                  + stellar_orbit_bridge_tables + tess_runtime_tables)
    if len(all_tables) != len(set(all_tables)):
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


def register_parquet_inputs(
    con: duckdb.DuckDBPyConnection,
    *,
    solar_identity_products: dict[str, Path],
    solar_runtime_products: dict[str, Path],
    stellar_orbit_products: dict[str, Path],
    stellar_orbit_bridge_products: dict[str, Path],
    tess_runtime_products: dict[str, Path],
) -> None:
    con.execute("CREATE SCHEMA solar_identity")
    con.execute("CREATE SCHEMA solar_runtime")
    con.execute("CREATE SCHEMA stellar_orbits")
    con.execute("CREATE SCHEMA stellar_orbit_bridge")
    con.execute("CREATE SCHEMA tess_runtime")
    for table, path in sorted(solar_identity_products.items()):
        con.execute(
            f"CREATE VIEW solar_identity.{table} AS "
            f"SELECT * FROM read_parquet({sql_literal(path)})"
        )
    for table, path in sorted(solar_runtime_products.items()):
        con.execute(
            f"CREATE VIEW solar_runtime.{table} AS "
            f"SELECT * FROM read_parquet({sql_literal(path)})"
        )
    for table, path in sorted(stellar_orbit_products.items()):
        con.execute(
            f"CREATE VIEW stellar_orbits.{table} AS "
            f"SELECT * FROM read_parquet({sql_literal(path)})"
        )
    for table, path in sorted(stellar_orbit_bridge_products.items()):
        con.execute(
            f"CREATE VIEW stellar_orbit_bridge.{table} AS "
            f"SELECT * FROM read_parquet({sql_literal(path)})"
        )
    for table, path in sorted(tess_runtime_products.items()):
        con.execute(
            f"CREATE VIEW tess_runtime.{table} AS "
            f"SELECT * FROM read_parquet({sql_literal(path)})"
        )


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
    for table in policy["solar_identity_tables"]:
        timing.run(
            f"copy_solar_identity_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM solar_identity.{table}"
            ),
        )
    for table in policy["solar_runtime_tables"]:
        timing.run(
            f"copy_solar_runtime_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM solar_runtime.{table}"
            ),
        )
    for table in policy["stellar_orbit_tables"]:
        timing.run(
            f"copy_stellar_orbit_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM stellar_orbits.{table}"
            ),
        )
    for table in policy["stellar_orbit_bridge_tables"]:
        timing.run(
            f"copy_stellar_orbit_bridge_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM stellar_orbit_bridge.{table}"
            ),
        )
    for table in policy["tess_runtime_tables"]:
        timing.run(
            f"copy_tess_runtime_{table}",
            lambda table=table: con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM tess_runtime.{table}"
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
        CREATE TEMP TABLE canonical_incoming_member_labels AS
        SELECT child_node_key,min(trim(member_role))::VARCHAR AS member_role
        FROM hierarchy.hierarchy_edges
        WHERE nullif(trim(member_role),'') IS NOT NULL
        GROUP BY child_node_key
        HAVING count(DISTINCT lower(trim(member_role)))=1;

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
          s.dist_ly/{PARSECS_TO_LIGHT_YEARS},
          coalesce(nullif(trim(s.component),''),nullif(trim(n.member_role),''),
                   incoming.member_role)::VARCHAR
        FROM hierarchy.hierarchy_nodes n
        JOIN core.stars s ON n.canonical_key=s.stable_object_key
        LEFT JOIN canonical_incoming_member_labels incoming
          ON incoming.child_node_key=n.hierarchy_node_key
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


def extend_solar_component_graph(con: duckdb.DuckDBPyConnection, build_id: str) -> None:
    """Add ARM-owned Solar identities and non-containment relation evidence."""
    con.execute(
        f"""
        CREATE TEMP TABLE solar_arm_component_nodes AS
        SELECT i.*,
          CASE WHEN i.identity_kind='artificial' THEN 'artificial'
               ELSE i.object_class END::VARCHAR AS runtime_component_type,
          sys.system_id::HUGEINT AS system_id
        FROM solar_component_identities i
        JOIN core.systems sys
          ON sys.stable_object_key=i.system_stable_object_key
        WHERE i.identity_status='accepted'
          AND i.core_object_id IS NULL;

        INSERT INTO component_entities
        SELECT base.max_id + row_number() OVER (
            ORDER BY i.identity_kind,i.source_record_key
          )::BIGINT AS component_entity_id,
          i.stable_component_key,i.runtime_component_type,
          NULL::VARCHAR AS core_object_type,NULL::HUGEINT AS core_object_id,
          i.display_name,NULL::VARCHAR AS catalog_component_label,
          NULL::DOUBLE AS ra_deg,NULL::DOUBLE AS dec_deg,NULL::DOUBLE AS dist_pc,
          CASE WHEN i.identity_kind='artificial'
            THEN 'sol_artificial' ELSE 'sol_authority' END::VARCHAR AS source_catalog,
          i.release_id::VARCHAR AS source_version,
          i.source_record_key::VARCHAR AS source_pk,
          sha256(concat_ws('|',i.solar_identity_id,i.stable_component_key,
            i.identity_method))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_solar_identity_projection_v1'::VARCHAR AS transform_version
        FROM solar_arm_component_nodes i
        CROSS JOIN (SELECT coalesce(max(component_entity_id),0)::BIGINT max_id
                    FROM component_entities) base;

        INSERT INTO system_hierarchy_edges
        SELECT base.max_id + row_number() OVER (
            ORDER BY t.identity_kind,t.source_record_id
          )::BIGINT AS hierarchy_edge_id,
          r.center_component_key::VARCHAR AS parent_component_key,
          r.target_component_key::VARCHAR AS child_component_key,
          'orbits'::VARCHAR AS edge_kind,
          CASE WHEN t.identity_kind='artificial' THEN 'artificial_object'
               WHEN t.object_class='moon' THEN 'satellite'
               ELSE 'minor_body' END::VARCHAR AS member_role,
          r.relation_kind::VARCHAR AS catalog_relation_label,
          NULL::INTEGER AS depth_hint,
          CASE WHEN t.identity_kind='artificial' THEN 0.985 ELSE 0.995 END::DOUBLE
            AS confidence_score,
          'high'::VARCHAR AS confidence_tier,
          to_json([r.source_id])::VARCHAR AS evidence_catalogs_json,
          to_json([r.relation_evidence_id])::JSON AS evidence_ids_json,
          CASE WHEN t.identity_kind='artificial'
            THEN 'sol_artificial' ELSE 'sol_authority' END::VARCHAR AS source_catalog,
          r.release_id::VARCHAR AS source_version,
          r.relation_evidence_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',r.binding_id,r.center_component_key,
            r.target_component_key))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_solar_relation_projection_v1'::VARCHAR AS transform_version
        FROM selected_solar_relation_bindings r
        JOIN selected_solar_target_bindings t USING(source_record_id)
        CROSS JOIN (SELECT coalesce(max(hierarchy_edge_id),0)::BIGINT max_id
                    FROM system_hierarchy_edges) base
        WHERE r.binding_status='accepted'
          AND NOT r.canonical_containment
          AND t.core_object_id IS NULL;
        """
    )


def create_solar_runtime_projections(
    con: duckdb.DuckDBPyConnection, build_id: str
) -> None:
    """Project selected Solar evidence into stable runtime compatibility tables."""
    con.execute(
        """
        CREATE TEMP TABLE solar_runtime_orbits AS
        SELECT row_number() OVER (
            ORDER BY o.identity_kind,t.source_record_id,o.evidence_id
          )::BIGINT AS runtime_orbit_id,
          o.*,t.system_stable_object_key,t.display_name,t.object_class,t.object_kind,
          t.parent_object_name,t.freshness_window_days,t.target_body_name,
          t.jpl_horizons_target,t.source_record_id AS source_record_key,
          t.source_id,t.release_id,
          r.center_command
        FROM selected_solar_orbital_solutions o
        JOIN selected_solar_target_bindings t USING(source_record_id)
        JOIN selected_solar_relation_bindings r
          ON r.relation_evidence_id=o.relation_claim_id
        WHERE o.runtime_eligible
          AND o.solution_contract_valid
          AND o.external_reference_origin IS NULL;

        CREATE TABLE orbit_edges AS
        SELECT runtime_orbit_id AS orbit_edge_id,
          ('comp:system:' || system_stable_object_key)::VARCHAR AS host_component_key,
          center_component_key::VARCHAR AS primary_component_key,
          target_component_key::VARCHAR AS secondary_component_key,
          CASE WHEN identity_kind='artificial' THEN 'artificial_orbit'
               WHEN object_class='moon' THEN 'satellite'
               WHEN object_class IN ('planet','dwarf_planet') THEN 'planet'
               ELSE 'orbits' END::VARCHAR AS relation_kind,
          NULL::VARCHAR AS barycenter_key,
          runtime_orbit_id::BIGINT AS preferred_solution_id,
          CASE WHEN identity_kind='artificial' THEN 0.985 ELSE 0.995 END::DOUBLE
            AS confidence_score,
          'high'::VARCHAR AS confidence_tier,
          to_json([source_id])::VARCHAR AS evidence_catalogs_json,
          to_json([evidence_id,relation_claim_id])::JSON AS evidence_ids_json,
          CASE WHEN identity_kind='artificial'
            THEN 'sol_artificial' ELSE 'sol_authority' END::VARCHAR AS source_catalog,
          release_id::VARCHAR AS source_version,evidence_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',orbital_solution_id,target_component_key,
            center_component_key))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_solar_orbit_projection_v1'::VARCHAR AS transform_version
        FROM solar_runtime_orbits
        ORDER BY runtime_orbit_id;

        CREATE TABLE orbital_solutions AS
        SELECT runtime_orbit_id AS orbital_solution_id,
          runtime_orbit_id AS orbit_edge_id,
          CASE WHEN identity_kind='artificial'
            THEN 'sol_artificial' ELSE 'sol_authority' END::VARCHAR
            AS solution_source_catalog,
          1::INTEGER AS solution_rank,
          (2000.0 + ((epoch_tdb_jd-2451545.0)/365.25))::DOUBLE
            AS reference_epoch_jyear,
          (epoch_tdb_jd-2400000.5)::DOUBLE AS reference_epoch_mjd,
          orbital_period_days::DOUBLE AS period_days,
          semi_major_axis_au::DOUBLE AS semi_major_axis_au,
          NULL::DOUBLE AS semi_major_axis_arcsec,eccentricity::DOUBLE AS eccentricity,
          inclination_deg::DOUBLE AS inclination_deg,
          longitude_ascending_node_deg::DOUBLE AS longitude_ascending_node_deg,
          argument_periapsis_deg::DOUBLE AS argument_periastron_deg,
          time_periapsis_tdb_jd::DOUBLE AS time_periastron_jd,
          mean_anomaly_deg::DOUBLE AS mean_anomaly_deg,NULL::DOUBLE AS mass_ratio_q,
          NULL::DOUBLE AS primary_mass_msun,NULL::DOUBLE AS secondary_mass_msun,
          NULL::DOUBLE AS rv_semiamplitude_primary_kms,
          NULL::DOUBLE AS rv_semiamplitude_secondary_kms,
          CASE WHEN identity_kind='artificial' THEN 0.985 ELSE 0.995 END::DOUBLE
            AS confidence_score,
          quality_json::JSON AS fit_quality_json,
          normalization_version::VARCHAR AS normalization_method,
          'high'::VARCHAR AS confidence_tier,
          CASE WHEN identity_kind='artificial'
            THEN 'sol_artificial' ELSE 'sol_authority' END::VARCHAR AS source_catalog,
          release_id::VARCHAR AS source_version,evidence_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',orbital_solution_id,solution_key,
            normalization_version))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_solar_orbit_projection_v1'::VARCHAR AS transform_version
        FROM solar_runtime_orbits
        ORDER BY runtime_orbit_id;

        CREATE TABLE sol_small_body_objects AS
        SELECT row_number() OVER (ORDER BY t.source_record_id)::BIGINT
            AS sol_small_body_id,
          t.stable_component_key,t.display_name::VARCHAR AS body_name,
          lower(trim(t.display_name))::VARCHAR AS body_name_norm,
          t.object_kind::VARCHAR AS body_kind,
          ('comp:system:' || t.system_stable_object_key)::VARCHAR AS host_component_key,
          o.center_component_key::VARCHAR AS primary_component_key,
          t.stable_component_key::VARCHAR AS secondary_component_key,
          t.parent_object_name::VARCHAR AS parent_name,
          lower(trim(t.parent_object_name))::VARCHAR AS parent_name_norm,
          o.orbital_period_days::DOUBLE AS orbital_period_days,
          o.semi_major_axis_au::DOUBLE AS semi_major_axis_au,
          o.eccentricity::DOUBLE AS eccentricity,
          o.inclination_deg::DOUBLE AS inclination_deg,
          o.epoch_tdb_jd::DOUBLE AS epoch_tdb_jd,
          p.mass_kg::DOUBLE AS body_mass_kg,p.radius_km::DOUBLE AS body_radius_km,
          t.freshness_window_days::INTEGER AS freshness_window_days,
          NULL::INTEGER AS staleness_days,
          NULL::BOOLEAN AS is_stale,0.995::DOUBLE AS confidence_score,
          'high'::VARCHAR AS confidence_tier,'sol_authority'::VARCHAR AS source_catalog,
          t.release_id::VARCHAR AS source_version,t.source_record_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',o.orbital_solution_id,t.stable_component_key,
            p.selected_parameter_set_id))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_solar_object_projection_v1'::VARCHAR AS transform_version,
          NULL::VARCHAR AS source_url
        FROM selected_solar_target_bindings t
        JOIN selected_solar_orbital_solutions o USING(source_record_id)
        LEFT JOIN selected_solar_physical_parameters p USING(source_record_id)
        WHERE t.object_class='minor_body' AND o.runtime_eligible
        ORDER BY t.source_record_id;

        CREATE TABLE sol_artificial_objects AS
        SELECT row_number() OVER (ORDER BY t.source_record_id)::BIGINT
            AS sol_artificial_id,
          t.stable_component_key,t.display_name::VARCHAR AS artifact_name,
          lower(trim(t.display_name))::VARCHAR AS artifact_name_norm,
          t.object_kind::VARCHAR AS artifact_kind,
          ('comp:system:' || t.system_stable_object_key)::VARCHAR AS host_component_key,
          o.center_component_key::VARCHAR AS primary_component_key,
          t.stable_component_key::VARCHAR AS secondary_component_key,
          t.parent_object_name::VARCHAR AS parent_name,
          lower(trim(t.parent_object_name))::VARCHAR AS parent_name_norm,
          r.center_command::VARCHAR AS center_code,
          t.target_body_name::VARCHAR AS target_body_name,
          o.orbital_period_days::DOUBLE AS orbital_period_days,
          o.semi_major_axis_au::DOUBLE AS semi_major_axis_au,
          o.eccentricity::DOUBLE AS eccentricity,
          o.inclination_deg::DOUBLE AS inclination_deg,
          o.epoch_tdb_jd::DOUBLE AS epoch_tdb_jd,
          p.mass_kg::DOUBLE AS artifact_mass_kg,p.radius_km::DOUBLE AS artifact_radius_km,
          t.freshness_window_days::INTEGER AS freshness_window_days,
          NULL::INTEGER AS staleness_days,
          NULL::BOOLEAN AS is_stale,0.985::DOUBLE AS confidence_score,
          'high'::VARCHAR AS confidence_tier,'sol_artificial'::VARCHAR AS source_catalog,
          t.release_id::VARCHAR AS source_version,t.source_record_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',o.orbital_solution_id,t.stable_component_key,
            p.selected_parameter_set_id))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_solar_object_projection_v1'::VARCHAR AS transform_version,
          NULL::VARCHAR AS source_url
        FROM selected_solar_target_bindings t
        JOIN selected_solar_orbital_solutions o USING(source_record_id)
        JOIN selected_solar_relation_bindings r USING(source_record_id)
        LEFT JOIN selected_solar_physical_parameters p USING(source_record_id)
        WHERE t.identity_kind='artificial' AND o.runtime_eligible
        ORDER BY t.source_record_id;
        """
    )


def extend_stellar_runtime_orbits(
    con: duckdb.DuckDBPyConnection, build_id: str
) -> None:
    """Append only exactly bound stellar relations to the runtime orbit graph."""
    con.execute(
        f"""
        CREATE TEMP TABLE stellar_runtime_relations AS
        SELECT row_number() OVER (ORDER BY b.relation_id)::BIGINT
              + (SELECT coalesce(max(orbit_edge_id),0) FROM orbit_edges) AS orbit_edge_id,
          b.*,r.source_id,r.release_id,r.relation_kind,r.method,r.reference_raw,
          r.quality_json,r.source_kinds_json
        FROM stellar_orbit_relation_bindings b
        JOIN selected_stellar_orbit_relations r USING(relation_id)
        WHERE b.runtime_eligible
          AND b.binding_status='accepted'
          AND NOT b.canonical_containment
        ORDER BY b.relation_id;

        CREATE TEMP TABLE stellar_runtime_solutions AS
        SELECT row_number() OVER (
            ORDER BY rr.orbit_edge_id,s.simulation_rank NULLS LAST,
              s.selected_orbit_solution_id
          )::BIGINT
              + (SELECT coalesce(max(orbital_solution_id),0) FROM orbital_solutions)
            AS orbital_solution_id,
          rr.orbit_edge_id,rr.primary_runtime_component_key,
          rr.secondary_runtime_component_key,s.*,
          host.dist_pc AS selected_system_distance_pc
        FROM stellar_runtime_relations rr
        JOIN selected_stellar_orbit_solutions s USING(relation_id)
        JOIN component_entities host
          ON host.stable_component_key=
            ('comp:system:' || s.canonical_system_stable_object_key)
        ORDER BY rr.orbit_edge_id,s.simulation_rank NULLS LAST,
          s.selected_orbit_solution_id;

        INSERT INTO orbital_solutions
        SELECT orbital_solution_id,orbit_edge_id,source_id::VARCHAR,
          row_number() OVER (
            PARTITION BY orbit_edge_id
            ORDER BY CASE WHEN selection_role='preferred_simulation' THEN 0 ELSE 1 END,
              simulation_rank NULLS LAST,selected_orbit_solution_id
          )::INTEGER AS solution_rank,
          NULL::DOUBLE AS reference_epoch_jyear,NULL::DOUBLE AS reference_epoch_mjd,
          period_days::DOUBLE,
          CASE WHEN semi_major_axis_arcsec>0 AND selected_system_distance_pc>0
            THEN semi_major_axis_arcsec*selected_system_distance_pc END::DOUBLE
            AS semi_major_axis_au,
          semi_major_axis_arcsec::DOUBLE,eccentricity::DOUBLE,
          inclination_deg::DOUBLE,longitude_ascending_node_deg::DOUBLE,
          argument_periastron_deg::DOUBLE,
          CASE
            WHEN time_periastron_unit='jd' THEN time_periastron_value
            WHEN time_periastron_unit='mjd' THEN time_periastron_value+2400000.5
            WHEN time_periastron_unit='jyear'
              THEN 2451545.0+(time_periastron_value-2000.0)*365.25
          END::DOUBLE AS time_periastron_jd,
          NULL::DOUBLE AS mean_anomaly_deg,NULL::DOUBLE AS mass_ratio_q,
          NULL::DOUBLE AS primary_mass_msun,NULL::DOUBLE AS secondary_mass_msun,
          rv_semiamplitude_primary_kms::DOUBLE,
          rv_semiamplitude_secondary_kms::DOUBLE,
          CASE WHEN selection_role='preferred_simulation' THEN 0.95
               WHEN selection_role='alternate_simulation' THEN 0.85
               ELSE 0.70 END::DOUBLE AS confidence_score,
          json_merge_patch(
            coalesce(quality_json,'{{}}'::JSON),
            to_json(struct_pack(
              selection_role := selection_role,
              source_time_periastron_value := time_periastron_value,
              source_time_periastron_unit := time_periastron_unit,
              selected_system_distance_pc := selected_system_distance_pc,
              angular_axis_to_au_derivation :=
                CASE WHEN semi_major_axis_arcsec>0 AND selected_system_distance_pc>0
                  THEN 'semi_major_axis_arcsec_times_selected_system_distance_pc_v1'
                END
            ))
          )::JSON AS fit_quality_json,
          concat_ws(';',normalization_version,
            CASE WHEN semi_major_axis_arcsec>0 AND selected_system_distance_pc>0
              THEN 'angular_axis_times_selected_system_distance_v1' END
          )::VARCHAR AS normalization_method,
          CASE WHEN selection_role='preferred_simulation' THEN 'high'
               WHEN selection_role='alternate_simulation' THEN 'medium'
               ELSE 'context' END::VARCHAR AS confidence_tier,
          source_id::VARCHAR AS source_catalog,release_id::VARCHAR AS source_version,
          evidence_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',selected_orbit_solution_id,relation_id,
            source_id,normalization_version))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_stellar_orbit_projection_v1'::VARCHAR AS transform_version
        FROM stellar_runtime_solutions
        ORDER BY orbital_solution_id;

        INSERT INTO orbit_edges
        SELECT rr.orbit_edge_id,
          ('comp:system:' || rr.canonical_system_stable_object_key)::VARCHAR
            AS host_component_key,
          rr.primary_runtime_component_key::VARCHAR AS primary_component_key,
          rr.secondary_runtime_component_key::VARCHAR AS secondary_component_key,
          rr.relation_kind::VARCHAR,NULL::VARCHAR AS barycenter_key,
          preferred.orbital_solution_id::BIGINT AS preferred_solution_id,
          CASE WHEN preferred.orbital_solution_id IS NOT NULL THEN 0.95
               ELSE 0.80 END::DOUBLE AS confidence_score,
          CASE WHEN preferred.orbital_solution_id IS NOT NULL THEN 'high'
               ELSE 'context' END::VARCHAR AS confidence_tier,
          rr.source_kinds_json::VARCHAR AS evidence_catalogs_json,
          to_json([rr.relation_evidence_id])::JSON AS evidence_ids_json,
          rr.source_id::VARCHAR AS source_catalog,rr.release_id::VARCHAR AS source_version,
          rr.relation_evidence_id::VARCHAR AS source_pk,
          sha256(concat_ws('|',rr.relation_id,rr.primary_runtime_component_key,
            rr.secondary_runtime_component_key))::VARCHAR AS source_row_hash,
          NULL::VARCHAR AS retrieval_checksum,NULL::VARCHAR AS retrieved_at,
          NULL::VARCHAR AS ingested_at,
          'e7_selected_stellar_orbit_projection_v1'::VARCHAR AS transform_version
        FROM stellar_runtime_relations rr
        LEFT JOIN stellar_runtime_solutions preferred
          ON preferred.selected_orbit_solution_id=
            rr.preferred_simulation_solution_id
        ORDER BY rr.orbit_edge_id;
        """
    )


def create_leaf_classifications(con: duckdb.DuckDBPyConnection, build_id: str) -> None:
    msc_class = spectral_class_sql("cp.classification_raw", "cp.classification_normalized", "'star'")
    sb9_class = spectral_class_sql("sb9.classification_raw", "sb9.classification_normalized", "'star'")
    debcat_class = spectral_class_sql(
        "debcat.classification_raw", "debcat.classification_normalized", "'star'"
    )
    con.execute(
        f"""
        CREATE TEMP TABLE runtime_stellar_leaves AS
        SELECT n.*,
          sys.stable_object_key AS system_stable_object_key,
          sys.wds_id AS system_wds_id,
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
          );

        CREATE TABLE msc_runtime_leaf_bindings AS
        WITH source_groups AS (
          SELECT trim(wds_id_raw) AS wds_id,
            lower(trim(component_label_normalized)) AS casefold_label,
            count(*)::BIGINT AS source_candidate_count
          FROM science.evidence_component_msc_component_entities
          WHERE binding_status='accepted'
            AND nullif(trim(wds_id_raw),'') IS NOT NULL
            AND nullif(trim(component_label_normalized),'') IS NOT NULL
          GROUP BY 1,2
        ), direct_runtime_groups AS (
          SELECT hierarchy_node_key,
            count(*)::BIGINT AS runtime_candidate_count,
            min(leaf_component_key)::VARCHAR AS runtime_component_key,
            min(system_stable_object_key)::VARCHAR AS runtime_system_stable_object_key
          FROM runtime_stellar_leaves
          WHERE hierarchy_node_key LIKE 'canon:leaf:msc:%'
          GROUP BY 1
        ), scoped_runtime_groups AS (
          SELECT trim(system_wds_id) AS wds_id,
            lower(trim(catalog_component_label)) AS casefold_label,
            count(*)::BIGINT AS runtime_candidate_count,
            min(hierarchy_node_key)::VARCHAR AS hierarchy_node_key,
            min(leaf_component_key)::VARCHAR AS runtime_component_key,
            min(system_stable_object_key)::VARCHAR AS runtime_system_stable_object_key
          FROM runtime_stellar_leaves
          WHERE nullif(trim(system_wds_id),'') IS NOT NULL
            AND nullif(trim(catalog_component_label),'') IS NOT NULL
          GROUP BY 1,2
        )
        SELECT row_number() OVER (ORDER BY ce.component_entity_id)::BIGINT AS binding_id,
          {sql_literal(build_id)}::VARCHAR AS build_id,
          ce.component_entity_id,ce.source_component_key,ce.source_id,ce.release_id,
          ce.wds_id_raw,ce.component_label_raw,ce.component_label_normalized,
          ce.canonical_system_stable_object_key AS source_system_stable_object_key,
          ce.binding_status AS source_binding_status,ce.binding_method,ce.binding_reason,
          coalesce(s.source_candidate_count,0)::BIGINT AS source_candidate_count,
          coalesce(d.runtime_candidate_count,r.runtime_candidate_count,0)::BIGINT
            AS runtime_candidate_count,
          CASE WHEN ce.binding_status='accepted' AND s.source_candidate_count=1
                 AND coalesce(d.runtime_candidate_count,r.runtime_candidate_count)=1
                THEN coalesce(d.hierarchy_node_key,r.hierarchy_node_key) END
            AS hierarchy_node_key,
          CASE WHEN ce.binding_status='accepted' AND s.source_candidate_count=1
                 AND coalesce(d.runtime_candidate_count,r.runtime_candidate_count)=1
                THEN coalesce(d.runtime_component_key,r.runtime_component_key) END
            AS runtime_component_key,
          CASE WHEN ce.binding_status='accepted' AND s.source_candidate_count=1
                 AND coalesce(d.runtime_candidate_count,r.runtime_candidate_count)=1
                THEN coalesce(d.runtime_system_stable_object_key,
                  r.runtime_system_stable_object_key) END
            AS runtime_system_stable_object_key,
          CASE WHEN ce.binding_status<>'accepted' THEN ce.binding_status
               WHEN s.source_candidate_count>1 THEN 'ambiguous'
               WHEN coalesce(d.runtime_candidate_count,r.runtime_candidate_count) IS NULL
                 THEN 'missing'
               WHEN coalesce(d.runtime_candidate_count,r.runtime_candidate_count)>1
                 THEN 'ambiguous'
               ELSE 'accepted' END::VARCHAR AS runtime_binding_status,
          CASE WHEN ce.binding_status<>'accepted' THEN 'source_component_not_accepted'
               WHEN s.source_candidate_count>1 THEN 'case_significant_source_collision'
               WHEN d.runtime_candidate_count=1 THEN 'exact_msc_hierarchy_leaf_key'
               WHEN r.runtime_candidate_count IS NULL THEN 'runtime_leaf_missing'
               WHEN r.runtime_candidate_count>1 THEN 'runtime_leaf_collision'
               ELSE 'exact_wds_unique_casefold_component' END::VARCHAR
            AS runtime_binding_reason,
          false::BOOLEAN AS canonical_containment,
          'e7_msc_runtime_leaf_binding_v1'::VARCHAR AS policy_version
        FROM science.evidence_component_msc_component_entities ce
        LEFT JOIN source_groups s
          ON s.wds_id=trim(ce.wds_id_raw)
         AND s.casefold_label=lower(trim(ce.component_label_normalized))
        LEFT JOIN direct_runtime_groups d
          ON d.hierarchy_node_key=('canon:leaf:msc:' || lower(trim(ce.wds_id_raw)) ||
            ':' || lower(trim(ce.component_label_normalized)))
        LEFT JOIN scoped_runtime_groups r
          ON r.wds_id=trim(ce.wds_id_raw)
         AND r.casefold_label=lower(trim(ce.component_label_normalized))
        ORDER BY ce.component_entity_id;

        CREATE TABLE stellar_leaf_display_classifications AS
        WITH leaves AS (
          SELECT * FROM runtime_stellar_leaves
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
          JOIN msc_runtime_leaf_bindings b
            ON b.runtime_binding_status='accepted'
           AND b.hierarchy_node_key=l.hierarchy_node_key
          JOIN science.evidence_component_msc_component_entities ce
            ON ce.component_entity_id=b.component_entity_id
          JOIN science.evidence_component_msc_classification_projection cp
            ON cp.component_entity_id=ce.component_entity_id
           AND cp.projection_status='eligible_for_quantity_selection'
          WHERE {msc_class} IS NOT NULL
          UNION ALL
          SELECT l.hierarchy_node_key,9,{sb9_class}::VARCHAR,'source',
            'selected_sb9_component_spectral_type',sb9.evidence_id,
            sb9.classification_raw,0.92,'multiplicity.sb9'::VARCHAR,
            'cds_b_sb9_snapshot_20260715'::VARCHAR,sb9.evidence_id
          FROM leaves l
          JOIN msc_runtime_leaf_bindings b
            ON b.runtime_binding_status='accepted'
           AND b.hierarchy_node_key=l.hierarchy_node_key
          JOIN science.evidence_component_sb9_classification_projection sb9
            ON sb9.target_key=b.source_component_key
           AND sb9.projection_status='eligible_for_quantity_selection'
          WHERE {sb9_class} IS NOT NULL
          UNION ALL
          SELECT l.hierarchy_node_key,8,{debcat_class}::VARCHAR,'source',
            'selected_debcat_component_spectral_type',debcat.evidence_id,
            debcat.classification_raw,0.94,'multiplicity.debcat'::VARCHAR,
            'debcat_2025-12-08'::VARCHAR,debcat.evidence_id
          FROM leaves l
          JOIN msc_runtime_leaf_bindings b
            ON b.runtime_binding_status='accepted'
           AND b.hierarchy_node_key=l.hierarchy_node_key
          JOIN science.evidence_component_debcat_classification_projection debcat
            ON debcat.target_key=b.source_component_key
           AND debcat.projection_status='eligible_for_quantity_selection'
          WHERE {debcat_class} IS NOT NULL
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
          JOIN msc_runtime_leaf_bindings b
            ON b.runtime_binding_status='accepted'
           AND b.hierarchy_node_key=l.hierarchy_node_key
          JOIN science.evidence_component_msc_component_entities ce
            ON ce.component_entity_id=b.component_entity_id
          JOIN science.evidence_component_msc_stellar_parameter_projection sp
            ON sp.component_entity_id=ce.component_entity_id
           AND sp.projection_status='eligible_for_quantity_selection'
           AND sp.quantity_key='mass' AND sp.normalized_value>0
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
        "CREATE UNIQUE INDEX msc_runtime_binding_component_uq ON msc_runtime_leaf_bindings(component_entity_id)",
        "CREATE INDEX msc_runtime_binding_leaf_idx ON msc_runtime_leaf_bindings(hierarchy_node_key)",
        "CREATE UNIQUE INDEX selected_stellar_parameters_star_uq ON selected_stellar_parameters(star_id)",
        "CREATE UNIQUE INDEX selected_display_star_uq ON selected_stellar_display_classifications(star_id)",
        "CREATE UNIQUE INDEX selected_planet_parameters_planet_uq ON selected_planet_parameters(planet_id)",
        "CREATE UNIQUE INDEX orbit_edges_id_uq ON orbit_edges(orbit_edge_id)",
        "CREATE INDEX orbit_edges_host_idx ON orbit_edges(host_component_key)",
        "CREATE INDEX orbit_edges_secondary_idx ON orbit_edges(secondary_component_key)",
        "CREATE UNIQUE INDEX orbital_solutions_id_uq ON orbital_solutions(orbital_solution_id)",
        "CREATE INDEX orbital_solutions_edge_idx ON orbital_solutions(orbit_edge_id)",
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
            "msc_runtime_leaf_bindings",
            "selected_stellar_display_classifications", "selected_planet_parameters",
            "selected_stellar_variability", "wise_sources", "orbit_edges",
            "orbital_solutions", "sol_small_body_objects", "sol_artificial_objects",
            "solar_component_identities", "selected_solar_orbital_solutions",
            "selected_stellar_orbit_relations", "selected_stellar_orbit_solutions",
            "stellar_orbit_endpoint_bindings", "stellar_orbit_relation_bindings",
            "tess_target_identity", "tess_missing_object_audit",
            "toi_current_evidence", "toi_disposition_history",
        )
    }
    checks = {
        "stability_metadata_opened": scalar(
            "SELECT count(*) FROM build_metadata WHERE key='stability_database_opened' AND value<>'0'"
        ),
        "component_inventory_delta": abs(
            counts["component_entities"] - (
                scalar("SELECT count(*) FROM hierarchy.hierarchy_nodes")
                + scalar("SELECT count(*) FROM solar_component_identities WHERE core_object_id IS NULL")
            )
        ),
        "hierarchy_edge_inventory_delta": abs(
            counts["system_hierarchy_edges"] - (
                scalar("SELECT count(*) FROM hierarchy.hierarchy_edges")
                + scalar("SELECT count(*) FROM selected_solar_target_bindings WHERE core_object_id IS NULL")
            )
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
        "msc_runtime_binding_inventory_delta": abs(
            counts["msc_runtime_leaf_bindings"]
            - scalar("SELECT count(*) FROM science.evidence_component_msc_component_entities")
        ),
        "duplicate_msc_runtime_component_bindings": scalar(
            "SELECT count(*) FROM (SELECT component_entity_id FROM msc_runtime_leaf_bindings "
            "GROUP BY 1 HAVING count(*)<>1)"
        ),
        "accepted_msc_runtime_bindings_without_leaf": scalar(
            "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE runtime_binding_status='accepted' "
            "AND (hierarchy_node_key IS NULL OR runtime_component_key IS NULL "
            "OR runtime_system_stable_object_key IS NULL)"
        ),
        "unaccepted_msc_runtime_bindings_with_leaf": scalar(
            "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE runtime_binding_status<>'accepted' "
            "AND (hierarchy_node_key IS NOT NULL OR runtime_component_key IS NOT NULL)"
        ),
        "msc_runtime_containment_promotions": scalar(
            "SELECT count(*) FROM msc_runtime_leaf_bindings WHERE canonical_containment"
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
            "SELECT count(*) FROM system_hierarchy_edges "
            "WHERE source_catalog<>'canonical_hierarchy' AND edge_kind='contains'"
        ),
        "solar_canonical_containment_promotions": scalar(
            "SELECT (SELECT count(*) FROM solar_relation_identity_outcomes WHERE canonical_containment) + "
            "(SELECT count(*) FROM selected_solar_relation_bindings WHERE canonical_containment) + "
            "(SELECT count(*) FROM selected_solar_orbital_solutions WHERE canonical_containment)"
        ),
        "solar_arm_identity_delta": abs(
            scalar("SELECT count(*) FROM component_entities WHERE source_catalog IN ('sol_authority','sol_artificial')")
            - scalar("SELECT count(*) FROM solar_component_identities WHERE core_object_id IS NULL")
        ),
        "solar_runtime_orbit_delta": abs(
            scalar("SELECT count(*) FROM orbit_edges WHERE source_catalog IN ('sol_authority','sol_artificial')")
            - scalar("SELECT count(*) FROM selected_solar_orbital_solutions WHERE runtime_eligible")
        ),
        "stellar_runtime_orbit_delta": abs(
            scalar("SELECT count(*) FROM orbit_edges WHERE transform_version='e7_selected_stellar_orbit_projection_v1'")
            - scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE runtime_eligible")
        ),
        "stellar_runtime_solution_delta": abs(
            scalar("SELECT count(*) FROM orbital_solutions WHERE transform_version='e7_selected_stellar_orbit_projection_v1'")
            - scalar("SELECT count(*) FROM selected_stellar_orbit_solutions s JOIN stellar_orbit_relation_bindings b USING(relation_id) WHERE b.runtime_eligible")
        ),
        "stellar_preferred_solution_delta": abs(
            scalar("SELECT count(*) FROM orbit_edges WHERE transform_version='e7_selected_stellar_orbit_projection_v1' AND preferred_solution_id IS NOT NULL")
            - scalar("SELECT count(*) FROM stellar_orbit_relation_bindings WHERE simulation_eligible")
        ),
        "unresolved_stellar_runtime_edges": scalar(
            "SELECT count(*) FROM orbit_edges e JOIN stellar_orbit_relation_bindings b "
            "ON b.relation_evidence_id=e.source_pk WHERE NOT b.runtime_eligible"
        ),
        "nonphysical_stellar_preferences": scalar(
            "SELECT count(*) FROM orbit_edges e JOIN orbital_solutions s "
            "ON s.orbital_solution_id=e.preferred_solution_id "
            "WHERE e.transform_version='e7_selected_stellar_orbit_projection_v1' "
            "AND (s.period_days<=0 OR s.semi_major_axis_arcsec<=0 "
            "OR s.eccentricity<0 OR s.eccentricity>=1)"
        ),
        "tess_candidate_or_negative_planet_links": scalar(
            "SELECT count(*) FROM toi_current_evidence "
            "WHERE disposition IN ('PC','APC','FP','FA') AND planet_id IS NOT NULL"
        ),
        "tess_confirmed_link_delta": abs(
            scalar("SELECT count(*) FROM toi_current_evidence WHERE disposition IN ('CP','KP') AND planet_id IS NOT NULL")
            - 824
        ),
        "tess_target_partition_delta": abs(counts["tess_target_identity"]-27930),
        "toi_inventory_delta": abs(counts["toi_current_evidence"]-8064),
        "toi_history_delta": abs(counts["toi_disposition_history"]-8064),
        "toi_history_orphans": scalar(
            "SELECT count(*) FROM toi_disposition_history h LEFT JOIN toi_current_evidence t "
            "USING(source_key) WHERE t.source_key IS NULL"
        ),
        "orphan_orbit_primaries": scalar(
            "SELECT count(*) FROM orbit_edges e LEFT JOIN component_entities c "
            "ON c.stable_component_key=e.primary_component_key WHERE c.component_entity_id IS NULL"
        ),
        "orphan_orbit_secondaries": scalar(
            "SELECT count(*) FROM orbit_edges e LEFT JOIN component_entities c "
            "ON c.stable_component_key=e.secondary_component_key WHERE c.component_entity_id IS NULL"
        ),
        "orphan_orbital_solutions": scalar(
            "SELECT count(*) FROM orbital_solutions s LEFT JOIN orbit_edges e USING(orbit_edge_id) "
            "WHERE e.orbit_edge_id IS NULL"
        ),
        "periodic_hyperbolic_solutions": scalar(
            "SELECT count(*) FROM selected_solar_orbital_solutions "
            "WHERE render_mode='hyperbolic_trajectory' AND periodic_renderable"
        ),
        "small_body_projection_delta": abs(counts["sol_small_body_objects"] - 35),
        "artificial_projection_delta": abs(counts["sol_artificial_objects"] - 11),
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
    solar_identity_products = {
        table: timing.run(
            f"verify_solar_identity_{table}",
            lambda table=table: product_path(
                inputs["solar_identity"], f"{table}.parquet"
            ),
        )
        for table in policy["solar_identity_tables"]
    }
    solar_runtime_products = {
        table: timing.run(
            f"verify_solar_runtime_{table}",
            lambda table=table: product_path(
                inputs["solar_runtime"], f"{table}.parquet"
            ),
        )
        for table in policy["solar_runtime_tables"]
    }
    stellar_orbit_products = {
        table: timing.run(
            f"verify_stellar_orbit_{table}",
            lambda table=table: product_path(
                inputs["stellar_orbits"], f"{table}.parquet"
            ),
        )
        for table in policy["stellar_orbit_tables"]
    }
    stellar_orbit_bridge_products = {
        table: timing.run(
            f"verify_stellar_orbit_bridge_{table}",
            lambda table=table: product_path(
                inputs["stellar_orbit_bridge"], f"{table}.parquet"
            ),
        )
        for table in policy["stellar_orbit_bridge_tables"]
    }
    tess_runtime_products = {
        table: timing.run(
            f"verify_tess_runtime_{table}",
            lambda table=table: product_path(
                inputs["tess_runtime"], f"{table}.parquet"
            ),
        )
        for table in policy["tess_runtime_tables"]
    }
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
            timing.run(
                "register_selected_parquet_inputs",
                lambda: register_parquet_inputs(
                    con,
                    solar_identity_products=solar_identity_products,
                    solar_runtime_products=solar_runtime_products,
                    stellar_orbit_products=stellar_orbit_products,
                    stellar_orbit_bridge_products=stellar_orbit_bridge_products,
                    tess_runtime_products=tess_runtime_products,
                ),
            )
            copy_selected_surfaces(
                con,
                policy=policy,
                build_id=build_id,
                timing=timing,
            )
            timing.run("component_graph", lambda: create_component_graph(con, build_id))
            timing.run(
                "solar_component_graph",
                lambda: extend_solar_component_graph(con, build_id),
            )
            timing.run(
                "solar_runtime_projections",
                lambda: create_solar_runtime_projections(con, build_id),
            )
            timing.run(
                "stellar_runtime_orbits",
                lambda: extend_stellar_runtime_orbits(con, build_id),
            )
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
            "schema_version": "spacegate.e7_clean_runtime_arm_manifest.v5",
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
