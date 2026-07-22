#!/usr/bin/env python3
"""Compile the E7 canonical identity/search foundation from pinned clean inputs."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_foundation.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-clean-foundation")


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


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalized(expr: str) -> str:
    return (
        "trim(regexp_replace(regexp_replace(lower(coalesce("
        + expr
        + ", '')), '[^0-9a-z]+', ' ', 'g'), '\\\\s+', ' ', 'g'))"
    )


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
            "max_rss_kib_after": int(after.ru_maxrss),
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
    if policy.get("schema_version") != "spacegate.e7_clean_foundation_policy.v1":
        raise ValueError("unsupported clean foundation policy")
    rules = policy.get("rules") or {}
    expected = {
        "open_stability_databases": False,
        "scientific_authority_from_identity_seed": False,
        "require_every_canonical_system_placed": True,
        "require_every_alias_bound": True,
        "require_every_identifier_bound": True,
        "semantic_search_duplicates_allowed": False,
    }
    if any(rules.get(key) is not value for key, value in expected.items()):
        raise ValueError("unsafe clean foundation rules")
    required = {"identity_graph", "hierarchy_nodes", "hierarchy_edges", "aliases", "system_placements"}
    if set(policy.get("inputs") or {}) != required:
        raise ValueError("clean foundation inputs are incomplete")
    for name, spec in policy["inputs"].items():
        if not spec.get("relative_path") or len(str(spec.get("sha256") or "")) != 64:
            raise ValueError(f"invalid pinned input: {name}")


def resolve_inputs(policy: dict[str, Any], state_dir: Path) -> tuple[dict[str, Path], dict[str, str]]:
    paths: dict[str, Path] = {}
    hashes: dict[str, str] = {}
    for name, spec in policy["inputs"].items():
        path = state_dir / str(spec["relative_path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        actual = file_sha256(path)
        if actual != spec["sha256"]:
            raise ValueError(f"pinned input checksum mismatch: {name}")
        paths[name] = path
        hashes[name] = actual
    return paths, hashes


def configure(con: duckdb.DuckDBPyConnection, staging: Path) -> None:
    temp_dir = staging / "duckdb-tmp"
    temp_dir.mkdir(exist_ok=True)
    con.execute("SET threads=16")
    con.execute("SET memory_limit='48GB'")
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")


def compile_foundation(
    policy_path: Path,
    state_dir: Path,
    output_root: Path,
    *,
    link_into_state: bool,
) -> dict[str, Any]:
    timing = Timings()
    policy = load_object(policy_path)
    validate_policy(policy)
    paths, input_hashes = timing.run("validate_pinned_inputs", lambda: resolve_inputs(policy, state_dir))
    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    build_id = stable_hash({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "inputs": input_hashes,
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        manifest = load_object(final_dir / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("clean foundation build collision")
        return manifest

    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    core_db = staging / "clean_core_foundation.duckdb"
    hierarchy_db = staging / "canonical_hierarchy.duckdb"
    parquet_dir = staging / "parquet"
    parquet_dir.mkdir()
    con = duckdb.connect(str(core_db))
    configure(con, staging)
    try:
        con.execute(f"ATTACH {sql_literal(paths['identity_graph'])} AS identity (READ_ONLY)")
        con.execute(f"CREATE TEMP VIEW hierarchy_nodes_input AS SELECT * FROM read_parquet({sql_literal(paths['hierarchy_nodes'])})")
        con.execute(f"CREATE TEMP VIEW hierarchy_edges_input AS SELECT * FROM read_parquet({sql_literal(paths['hierarchy_edges'])})")
        con.execute(f"CREATE TEMP VIEW aliases_input AS SELECT * FROM read_parquet({sql_literal(paths['aliases'])})")
        con.execute(f"CREATE TEMP VIEW placements_input AS SELECT * FROM read_parquet({sql_literal(paths['system_placements'])})")

        timing.run("canonical_object_materialization", lambda: con.execute(
            """
            CREATE TABLE canonical_objects AS
            SELECT object_node_key,object_type,stable_object_key,
                   try_cast(canonical_row_id AS HUGEINT) canonical_id,
                   system_stable_object_key,display_name
            FROM identity.canonical_object_nodes
            ORDER BY object_type,canonical_id
            """
        ))
        timing.run("canonical_inventory_materialization", lambda: con.execute(
            f"""
            CREATE TABLE systems AS
            WITH counts AS (
              SELECT system_stable_object_key,
                     count(*) FILTER (WHERE object_type='star')::BIGINT star_count,
                     count(*) FILTER (WHERE object_type='planet')::BIGINT planet_count
              FROM canonical_objects GROUP BY 1
            )
            SELECT o.canonical_id system_id,o.canonical_id::BIGINT spatial_index,
                   o.stable_object_key,o.display_name system_name,
                   {normalized('o.display_name')} system_name_norm,
                   coalesce(c.star_count,0) star_count,coalesce(c.planet_count,0) planet_count,
                   p.ra_deg,p.dec_deg,p.distance_pc,p.parallax_mas,p.dist_ly,
                   p.x_helio_ly,p.y_helio_ly,p.z_helio_ly,
                   p.coordinate_frame,p.coordinate_epoch,p.placement_source,
                   p.placement_method,p.policy_version placement_policy_version
            FROM canonical_objects o
            JOIN placements_input p ON p.system_stable_object_key=o.stable_object_key
            LEFT JOIN counts c ON c.system_stable_object_key=o.stable_object_key
            WHERE o.object_type='system'
            ORDER BY o.canonical_id;

            CREATE TABLE stars AS
            SELECT o.canonical_id star_id,o.canonical_id::BIGINT spatial_index,
                   s.canonical_id system_id,o.stable_object_key,
                   o.display_name star_name,{normalized('o.display_name')} star_name_norm
            FROM canonical_objects o
            JOIN canonical_objects s ON s.object_type='system'
             AND s.stable_object_key=o.system_stable_object_key
            WHERE o.object_type='star'
            ORDER BY o.canonical_id;

            CREATE TABLE planets AS
            SELECT o.canonical_id planet_id,o.canonical_id::BIGINT spatial_index,
                   s.canonical_id system_id,host.canonical_id star_id,
                   o.stable_object_key,o.display_name planet_name,
                   {normalized('o.display_name')} planet_name_norm
            FROM canonical_objects o
            LEFT JOIN canonical_objects s ON s.object_type='system'
             AND s.stable_object_key=o.system_stable_object_key
            LEFT JOIN hierarchy_edges_input e ON e.child_node_key=o.stable_object_key
            LEFT JOIN canonical_objects host ON host.object_type='star'
             AND host.stable_object_key=e.parent_node_key
            WHERE o.object_type='planet'
            QUALIFY row_number() OVER(PARTITION BY o.stable_object_key ORDER BY host.canonical_id NULLS LAST)=1
            ORDER BY o.canonical_id
            """
        ))
        timing.run("alias_materialization", lambda: con.execute(
            """
            CREATE TABLE aliases AS
            SELECT a.alias_seed_id alias_id,a.target_type,o.canonical_id target_id,
                   s.canonical_id system_id,
                   CASE WHEN a.target_type='star' THEN o.canonical_id END star_id,
                   a.stable_object_key,a.system_stable_object_key,
                   a.alias_raw,a.alias_norm,a.alias_kind,
                   a.alias_priority,a.is_primary,a.source_catalog,a.source_version,a.source_pk
            FROM aliases_input a
            JOIN canonical_objects o ON o.object_type=a.target_type
             AND o.stable_object_key=a.stable_object_key
            JOIN canonical_objects s ON s.object_type='system'
             AND s.stable_object_key=a.system_stable_object_key
            ORDER BY a.alias_seed_id
            """
        ))
        prefixes = [(key, value) for key, value in sorted(policy["identifier_display_prefixes"].items())]
        con.execute("CREATE TEMP TABLE identifier_prefixes(namespace VARCHAR,prefix VARCHAR)")
        con.executemany("INSERT INTO identifier_prefixes VALUES (?,?)", prefixes)
        timing.run("identifier_materialization", lambda: con.execute(
            """
            CREATE TABLE object_identifiers AS
            SELECT row_number() OVER(ORDER BY b.stable_object_key,b.namespace,b.id_value_norm,b.binding_key)::BIGINT identifier_id,
                   o.object_type target_type,o.canonical_id target_id,o.stable_object_key,
                   o.system_stable_object_key,b.namespace,b.id_value_raw,b.id_value_norm,
                   b.is_canonical,b.resolution_method,b.resolution_confidence,
                   b.source_catalog,b.source_version,b.source_record_id source_pk,
                   b.identifier_source_id,b.identifier_release_id,b.binding_key,b.evidence_json
            FROM identity.canonical_identifier_bindings b
            JOIN canonical_objects o ON o.object_node_key=b.object_node_key
            ORDER BY b.stable_object_key,b.namespace,b.id_value_norm,b.binding_key;

            CREATE TABLE identifier_quarantine AS
            SELECT * FROM identity.identity_quarantine ORDER BY quarantine_key
            """
        ))
        timing.run("search_term_materialization", lambda: con.execute(
            f"""
            CREATE TEMP TABLE search_candidates AS
            SELECT o.system_stable_object_key,o.object_type target_type,o.canonical_id target_id,
                   CASE WHEN o.object_type='star' THEN o.canonical_id END star_id,
                   NULL::BIGINT alias_id,o.display_name term_raw,{normalized('o.display_name')} term_norm,
                   'canonical_name' term_kind,10::INTEGER term_priority,true is_primary,
                   'spacegate_identity' source_catalog,policy_version source_version,NULL::VARCHAR source_pk
            FROM canonical_objects o
            CROSS JOIN (SELECT {sql_literal(policy['policy_version'])} policy_version)
            UNION ALL
            SELECT a.system_stable_object_key,a.target_type,a.target_id,a.star_id,a.alias_id,
                   a.alias_raw,a.alias_norm,a.alias_kind,a.alias_priority,a.is_primary,
                   a.source_catalog,a.source_version,cast(a.source_pk AS VARCHAR)
            FROM aliases a
            UNION ALL
            SELECT i.system_stable_object_key,i.target_type,i.target_id,
                   CASE WHEN i.target_type='star' THEN i.target_id END,NULL::BIGINT,
                   CASE WHEN coalesce(p.prefix,'')='' THEN i.id_value_raw ELSE p.prefix||' '||i.id_value_raw END,
                   {normalized("CASE WHEN coalesce(p.prefix,'')='' THEN i.id_value_raw ELSE p.prefix||' '||i.id_value_raw END")},
                   'identifier:'||i.namespace,30::INTEGER,i.is_canonical,
                   i.source_catalog,i.source_version,i.source_pk
            FROM object_identifiers i LEFT JOIN identifier_prefixes p USING(namespace);

            CREATE TABLE system_search_terms AS
            SELECT row_number() OVER(ORDER BY system_id,term_norm,target_type,target_id,term_priority,term_raw)::BIGINT search_term_id,
                   system_id,target_type,target_id,star_id,alias_id,term_raw,term_norm,
                   term_kind,term_priority,is_primary,source_catalog,source_version,source_pk
            FROM (
              SELECT s.canonical_id system_id,c.* EXCLUDE(system_stable_object_key),
                     row_number() OVER(PARTITION BY c.system_stable_object_key,c.target_type,c.target_id,c.term_norm
                       ORDER BY c.term_priority,c.is_primary DESC,c.term_kind,c.term_raw,c.source_catalog,c.source_version,c.source_pk) choice
              FROM search_candidates c
              JOIN canonical_objects s ON s.object_type='system'
               AND s.stable_object_key=c.system_stable_object_key
              WHERE nullif(c.term_norm,'') IS NOT NULL
            ) WHERE choice=1
            ORDER BY system_id,term_norm,target_type,target_id,term_priority,term_raw
            """
        ))
        timing.run("core_metadata_and_indexes", lambda: con.execute(
            f"""
            CREATE TABLE build_metadata(key VARCHAR,value VARCHAR);
            INSERT INTO build_metadata VALUES
              ('build_id',{sql_literal(build_id)}),
              ('build_kind','e7_clean_foundation'),
              ('policy_version',{sql_literal(policy['policy_version'])}),
              ('compiler_version',{sql_literal(policy['compiler_version'])}),
              ('identity_graph_id',{sql_literal(policy['inputs']['identity_graph']['id'])}),
              ('scientific_authority_from_identity_seed','0'),
              ('stability_database_opened','0');
            CREATE INDEX systems_stable_key_idx ON systems(stable_object_key);
            CREATE INDEX stars_system_idx ON stars(system_id);
            CREATE INDEX stars_stable_key_idx ON stars(stable_object_key);
            CREATE INDEX planets_system_idx ON planets(system_id);
            CREATE INDEX planets_stable_key_idx ON planets(stable_object_key);
            CREATE INDEX aliases_norm_idx ON aliases(alias_norm);
            CREATE INDEX object_identifiers_lookup_idx ON object_identifiers(namespace,id_value_norm);
            CREATE INDEX system_search_terms_norm_idx ON system_search_terms(term_norm)
            """
        ))
        timing.run("core_checkpoint", lambda: con.execute("CHECKPOINT"))

        tables = ["canonical_objects", "systems", "stars", "planets", "aliases", "object_identifiers", "identifier_quarantine", "system_search_terms"]
        def export_core() -> None:
            for table in tables:
                con.execute(
                    f"COPY (SELECT * FROM {table}) TO {sql_literal(parquet_dir / (table + '.parquet'))} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
                )
        timing.run("core_parquet_export", export_core)

        def verify() -> dict[str, int]:
            return {
                "invalid_canonical_ids": int(con.execute("SELECT count(*) FROM canonical_objects WHERE canonical_id IS NULL").fetchone()[0]),
                "duplicate_canonical_keys": int(con.execute("SELECT count(*) FROM (SELECT stable_object_key FROM canonical_objects GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
                "canonical_system_placement_delta": int(con.execute("SELECT (SELECT count(*) FROM systems)-(SELECT count(*) FROM canonical_objects WHERE object_type='system')").fetchone()[0]),
                "canonical_star_inventory_delta": int(con.execute("SELECT (SELECT count(*) FROM stars)-(SELECT count(*) FROM canonical_objects WHERE object_type='star')").fetchone()[0]),
                "canonical_planet_inventory_delta": int(con.execute("SELECT (SELECT count(*) FROM planets)-(SELECT count(*) FROM canonical_objects WHERE object_type='planet')").fetchone()[0]),
                "alias_input_output_delta": int(con.execute("SELECT (SELECT count(*) FROM aliases)-(SELECT count(*) FROM aliases_input)").fetchone()[0]),
                "identifier_input_output_delta": int(con.execute("SELECT (SELECT count(*) FROM object_identifiers)-(SELECT count(*) FROM identity.canonical_identifier_bindings)").fetchone()[0]),
                "duplicate_alias_ids": int(con.execute("SELECT count(*) FROM (SELECT alias_id FROM aliases GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
                "duplicate_identifier_ids": int(con.execute("SELECT count(*) FROM (SELECT identifier_id FROM object_identifiers GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
                "duplicate_search_semantics": int(con.execute("SELECT count(*) FROM (SELECT system_id,target_type,target_id,term_norm FROM system_search_terms GROUP BY 1,2,3,4 HAVING count(*)>1)").fetchone()[0]),
                "empty_search_terms": int(con.execute("SELECT count(*) FROM system_search_terms WHERE nullif(trim(term_norm),'') IS NULL").fetchone()[0]),
                "orphan_search_systems": int(con.execute("SELECT count(*) FROM system_search_terms t LEFT JOIN systems s USING(system_id) WHERE s.system_id IS NULL").fetchone()[0]),
            }
        checks = timing.run("row_accounting_and_invariants", verify)
        if any(checks.values()):
            raise ValueError(f"clean foundation verification failed: {checks}")
        counts = {table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]) for table in tables}
        accounting = {
            "planets_without_canonical_system_binding": int(con.execute("SELECT count(*) FROM planets WHERE system_id IS NULL").fetchone()[0]),
            "planets_without_canonical_star_binding": int(con.execute("SELECT count(*) FROM planets WHERE star_id IS NULL").fetchone()[0]),
        }
    finally:
        con.close()

    def build_hierarchy() -> None:
        hcon = duckdb.connect(str(hierarchy_db))
        configure(hcon, staging)
        try:
            hcon.execute(f"CREATE TABLE hierarchy_nodes AS SELECT * FROM read_parquet({sql_literal(paths['hierarchy_nodes'])}) ORDER BY hierarchy_node_key")
            hcon.execute(f"CREATE TABLE hierarchy_edges AS SELECT * FROM read_parquet({sql_literal(paths['hierarchy_edges'])}) ORDER BY hierarchy_edge_id")
            hcon.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
            hcon.executemany("INSERT INTO build_metadata VALUES (?,?)", [
                ("build_id", build_id), ("build_kind", "e7_clean_foundation"),
                ("policy_version", policy["policy_version"]),
                ("identity_seed_id", policy["inputs"]["hierarchy_nodes"]["id"]),
                ("stability_database_opened", "0"),
            ])
            hcon.execute("CREATE INDEX hierarchy_nodes_key_idx ON hierarchy_nodes(hierarchy_node_key)")
            hcon.execute("CREATE INDEX hierarchy_edges_parent_idx ON hierarchy_edges(parent_node_key)")
            hcon.execute("CREATE INDEX hierarchy_edges_child_idx ON hierarchy_edges(child_node_key)")
            hcon.execute("CHECKPOINT")
        finally:
            hcon.close()
    timing.run("hierarchy_materialization_and_indexes", build_hierarchy)

    products: dict[str, Any] = {}
    for path in sorted(staging.glob("*.duckdb")) + sorted(parquet_dir.glob("*.parquet")):
        key = str(path.relative_to(staging))
        is_database = path.suffix == ".duckdb"
        products[key] = {
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
            "artifact_class": "regenerable_query_database" if is_database else "canonical_columnar_artifact",
            "determinism": "logical_tables" if is_database else "byte_exact",
        }
    manifest = {
        "schema_version": "spacegate.e7_clean_foundation_manifest.v1",
        "build_id": build_id,
        "status": "pass",
        "created_at": utc_now(),
        "policy_version": policy["policy_version"],
        "compiler_version": policy["compiler_version"],
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "stability_databases_opened": [],
        "scientific_authority_from_identity_seed": False,
        "inputs": {name: {"path": str(paths[name]), "sha256": digest} for name, digest in input_hashes.items()},
        "counts": counts,
        "accounting": accounting,
        "verification": checks,
        "products": products,
        "determinism_contract": {
            "canonical_parquet_products": "byte_exact",
            "duckdb_query_databases": "logical_tables",
            "reason": "DuckDB page layout is not a stable serialization; every authoritative table is exported as deterministic Parquet.",
        },
        "timing": timing.report(),
    }
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    shutil.rmtree(staging / "duckdb-tmp", ignore_errors=True)
    os.replace(staging, final_dir)
    if link_into_state:
        link_root = state_dir / "derived/evidence_lake_v2/clean_foundation"
        link_root.mkdir(parents=True, exist_ok=True)
        link = link_root / build_id
        if not link.exists() and not link.is_symlink():
            temporary_link = link_root / f".{build_id}.link"
            temporary_link.unlink(missing_ok=True)
            temporary_link.symlink_to(final_dir)
            os.replace(temporary_link, link)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = compile_foundation(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
