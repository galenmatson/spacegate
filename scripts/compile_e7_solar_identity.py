#!/usr/bin/env python3
"""Compile permanent Solar component identity from clean source evidence."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_solar_identity.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-solar-identity")


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
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def write_object_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_solar_identity_policy.v1":
        raise ValueError("unsupported Solar identity policy")
    expected_rules = {
        "open_stability_databases": False,
        "scientific_authority": False,
        "operator_seed_identity_only": True,
        "name_only_core_binding": False,
        "source_relations_create_canonical_containment": False,
        "preserve_legacy_component_key_crosswalk": True,
    }
    if policy.get("rules") != expected_rules:
        raise ValueError("unsafe Solar identity rules")
    if set(policy.get("inputs") or {}) != {"scientific_evidence", "clean_runtime_core"}:
        raise ValueError("Solar identity inputs are incomplete")
    for name, spec in policy["inputs"].items():
        relative = Path(str(spec.get("relative_path") or ""))
        if (
            not spec.get("build_id")
            or len(str(spec.get("manifest_sha256") or "")) != 64
            or relative.is_absolute()
            or ".." in relative.parts
        ):
            raise ValueError(f"invalid bounded Solar identity input: {name}")
    contract = policy.get("identity_contract") or {}
    if contract.get("natural_source_table") != "sol_system_objects":
        raise ValueError("natural Solar source table is not pinned")
    if contract.get("artificial_source_table") != "sol_artificial_objects":
        raise ValueError("artificial Solar source table is not pinned")
    if contract.get("external_reference_origins") != {"0": "solar_system_barycenter"}:
        raise ValueError("Solar external origin contract is incomplete")


def resolve_input(state: Path, spec: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    root = (state / spec["relative_path"]).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or file_sha256(manifest_path) != spec["manifest_sha256"]:
        raise ValueError(f"Solar identity input manifest mismatch: {root}")
    manifest = load_object(manifest_path)
    observed_id = manifest.get("build_id") or manifest.get("seed_id")
    observed_status = manifest.get("status") or (manifest.get("report") or {}).get("status")
    if observed_id != spec["build_id"] or observed_status != "pass":
        raise ValueError(f"unaccepted Solar identity input: {root}")
    return root, manifest


def resolve_database(root: Path, manifest: dict[str, Any], relative: str) -> Path:
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    if relative == manifest.get("database"):
        expected = manifest.get("database_sha256")
    else:
        expected = ((manifest.get("products") or {}).get(relative) or {}).get("sha256")
    if len(str(expected or "")) != 64 or file_sha256(path) != expected:
        raise ValueError(f"Solar identity input product mismatch: {path}")
    return path


def create_identity_tables(
    con: duckdb.DuckDBPyConnection, policy: dict[str, Any], build_id: str
) -> None:
    contract = policy["identity_contract"]
    natural = sql_literal(contract["natural_source_table"])
    artificial = sql_literal(contract["artificial_source_table"])
    system_key = sql_literal(contract["sol_system_stable_key"])
    sun_key = sql_literal(contract["sun_stable_object_key"])
    planet_prefix = sql_literal(contract["canonical_planet_key_prefix"])
    arm_prefix = sql_literal(contract["arm_identity_prefix"])
    policy_version = sql_literal(policy["policy_version"])
    build = sql_literal(build_id)
    con.execute(
        f"""
        CREATE TEMP TABLE solar_source_targets AS
        WITH pivoted AS (
          SELECT r.source_record_id,r.source_id,r.release_id,r.source_table,
            json_extract_string(r.logical_key_json,'$.source_pk') source_record_key,
            json_extract_string(r.source_context_json,'$.object_class') object_class,
            json_extract_string(r.source_context_json,'$.object_kind') object_kind,
            json_extract_string(r.source_context_json,'$.parent_object_name') parent_object_name,
            json_extract_string(r.source_context_json,'$.operator_seed_version') operator_seed_version,
            json_extract_string(r.source_context_json,'$.operator_seed_sha256') operator_seed_sha256,
            max(i.identifier_normalized) FILTER (
              WHERE i.namespace='jpl_horizons_target'
                AND i.claim_scope IN ('natural_solar_system_target','artificial_object_target')
            ) jpl_horizons_target,
            max(i.identifier_raw) FILTER (
              WHERE i.namespace='spacegate_operator_seed_name'
            ) display_name,
            max(i.identifier_normalized) FILTER (
              WHERE i.namespace='spacegate_operator_seed_target_key'
            ) operator_seed_target_key
          FROM evidence.source_records r
          JOIN evidence.identifier_claim_evidence i USING(source_record_id)
          WHERE r.source_table IN ({natural},{artificial})
          GROUP BY 1,2,3,4,5,6,7,8,9,10
        ) SELECT * FROM pivoted;

        CREATE TEMP TABLE solar_identity_candidates AS
        SELECT t.*,
          CASE
            WHEN t.source_table={natural} AND t.object_class='star'
              AND t.source_record_key='1' THEN {sun_key}
            WHEN t.source_table={natural}
              AND t.object_class IN ('planet','dwarf_planet')
              THEN {planet_prefix} || t.source_record_key
          END canonical_stable_object_key
        FROM solar_source_targets t;

        CREATE TABLE solar_component_identities AS
        SELECT
          sha256(concat_ws('|','solar-identity',t.source_table,t.source_record_key))
            AS solar_identity_id,
          CASE
            WHEN s.star_id IS NOT NULL THEN 'comp:star:' || s.stable_object_key
            WHEN p.planet_id IS NOT NULL THEN 'comp:planet:' || p.stable_object_key
            ELSE {arm_prefix} ||
              CASE WHEN t.source_table={natural} THEN 'natural:' ELSE 'artificial:' END ||
              t.source_record_key
          END AS stable_component_key,
          {system_key} AS system_stable_object_key,
          CASE WHEN t.source_table={natural} THEN 'natural' ELSE 'artificial' END
            AS identity_kind,
          t.display_name,lower(trim(t.display_name)) AS normalized_name,
          t.object_class,t.object_kind,t.parent_object_name,t.jpl_horizons_target,
          t.operator_seed_target_key,t.operator_seed_version,t.operator_seed_sha256,
          CASE
            WHEN t.object_class='star' THEN 'comp:star:sol:' || lower(trim(t.display_name))
            WHEN t.object_class IN ('planet','dwarf_planet')
              THEN 'comp:planet:sol:' || lower(trim(t.display_name))
            WHEN t.object_class='moon'
              THEN 'comp:moon:sol:' || trim(regexp_replace(lower(t.display_name),'[^a-z0-9]+',' ','g'))
            WHEN t.object_class='minor_body'
              THEN 'comp:minor_body:sol:' || trim(regexp_replace(lower(t.display_name),'[^a-z0-9]+',' ','g'))
            ELSE 'comp:artificial:sol:' || trim(regexp_replace(lower(t.display_name),'[^a-z0-9]+',' ','g'))
          END AS legacy_component_key,
          CASE WHEN s.star_id IS NOT NULL THEN 'star'
               WHEN p.planet_id IS NOT NULL THEN 'planet' END AS core_object_type,
          coalesce(s.star_id,p.planet_id)::HUGEINT AS core_object_id,
          coalesce(s.stable_object_key,p.stable_object_key) AS core_stable_object_key,
          CASE WHEN s.star_id IS NOT NULL THEN 'exact_configured_sun_stable_key'
               WHEN p.planet_id IS NOT NULL THEN 'exact_operator_seed_to_canonical_key'
               ELSE 'reviewed_operator_seed_arm_identity' END AS identity_method,
          'accepted'::VARCHAR AS identity_status,
          t.source_record_id,t.source_id,t.release_id,t.source_table,t.source_record_key,
          {policy_version} AS policy_version,{build} AS build_id
        FROM solar_identity_candidates t
        LEFT JOIN core.stars s
          ON t.canonical_stable_object_key=s.stable_object_key
         AND t.object_class='star'
         AND lower(trim(t.display_name))=lower(trim(s.star_name))
        LEFT JOIN core.planets p
          ON t.canonical_stable_object_key=p.stable_object_key
         AND t.object_class IN ('planet','dwarf_planet')
         AND lower(trim(t.display_name))=lower(trim(p.planet_name))
        ORDER BY t.source_table,t.source_record_key;

        CREATE TABLE solar_component_aliases AS
        SELECT sha256(concat_ws('|',solar_identity_id,'primary-name')) alias_id,
          solar_identity_id,stable_component_key,display_name alias_raw,
          normalized_name alias_normalized,'primary_name' alias_kind,true is_primary,
          source_id,release_id,source_record_id,{policy_version} policy_version
        FROM solar_component_identities ORDER BY stable_component_key;

        CREATE TABLE solar_component_identifiers AS
        SELECT sha256(concat_ws('|',solar_identity_id,namespace,id_value_normalized)) identifier_id,
          solar_identity_id,stable_component_key,namespace,id_value_raw,
          id_value_normalized,true is_canonical,source_id,release_id,source_record_id,
          {policy_version} policy_version
        FROM (
          SELECT *, 'jpl_horizons_target'::VARCHAR namespace,
            jpl_horizons_target id_value_raw,jpl_horizons_target id_value_normalized
          FROM solar_component_identities
          UNION ALL
          SELECT *, 'spacegate_solar_seed_id',operator_seed_target_key,operator_seed_target_key
          FROM solar_component_identities
          UNION ALL
          SELECT *, 'legacy_component_key',legacy_component_key,lower(legacy_component_key)
          FROM solar_component_identities
        ) q ORDER BY stable_component_key,namespace;

        CREATE TABLE solar_relation_identity_outcomes AS
        SELECT sha256(concat_ws('|','solar-relation-identity',r.evidence_id)) relation_identity_id,
          r.evidence_id relation_evidence_id,r.source_record_id,r.relation_kind,
          r.relation_scope,r.left_identity_raw target_command,
          r.right_identity_raw center_command,target.stable_component_key target_component_key,
          center.stable_component_key center_component_key,
          CASE WHEN r.right_identity_raw='0' THEN 'solar_system_barycenter' END
            external_reference_origin,
          CASE WHEN target.solar_identity_id IS NOT NULL AND center.solar_identity_id IS NOT NULL
                 THEN 'accepted'
               WHEN target.solar_identity_id IS NOT NULL AND r.right_identity_raw='0'
                 THEN 'reference_origin'
               WHEN target.solar_identity_id IS NULL THEN 'missing_target'
               ELSE 'missing_center' END relation_status,
          false AS canonical_containment,
          r.method,r.reference_raw,r.epoch_raw,r.quality_json,
          target.source_id,target.release_id,{policy_version} policy_version,{build} build_id
        FROM evidence.relation_claim_evidence r
        JOIN evidence.source_records sr USING(source_record_id)
        LEFT JOIN solar_component_identities target
          ON target.jpl_horizons_target=r.left_identity_raw
        LEFT JOIN solar_component_identities center
          ON center.jpl_horizons_target=r.right_identity_raw
        WHERE sr.source_table IN ({natural},{artificial})
          AND r.relation_kind IN ('jpl_horizons_orbit_center','jpl_horizons_trajectory_center')
        ORDER BY r.evidence_id;
        """
    )


def verify(con: duckdb.DuckDBPyConnection, policy: dict[str, Any]) -> dict[str, Any]:
    scalar = lambda sql: int(con.execute(sql).fetchone()[0] or 0)
    counts = {
        "identities": scalar("SELECT count(*) FROM solar_component_identities"),
        "natural_identities": scalar("SELECT count(*) FROM solar_component_identities WHERE identity_kind='natural'"),
        "artificial_identities": scalar("SELECT count(*) FROM solar_component_identities WHERE identity_kind='artificial'"),
        "core_bound_identities": scalar("SELECT count(*) FROM solar_component_identities WHERE core_object_id IS NOT NULL"),
        "arm_only_identities": scalar("SELECT count(*) FROM solar_component_identities WHERE core_object_id IS NULL"),
        "aliases": scalar("SELECT count(*) FROM solar_component_aliases"),
        "identifiers": scalar("SELECT count(*) FROM solar_component_identifiers"),
        "relation_outcomes": scalar("SELECT count(*) FROM solar_relation_identity_outcomes"),
        "relations_accepted": scalar("SELECT count(*) FROM solar_relation_identity_outcomes WHERE relation_status='accepted'"),
        "relations_reference_origin": scalar("SELECT count(*) FROM solar_relation_identity_outcomes WHERE relation_status='reference_origin'"),
        "canonical_containment_promotions": scalar("SELECT count(*) FROM solar_relation_identity_outcomes WHERE canonical_containment"),
    }
    expected = {key: int(value) for key, value in policy["acceptance"].items()}
    checks = {
        "acceptance_count_delta": sum(abs(counts.get(key, -1) - value) for key, value in expected.items()),
        "duplicate_stable_keys": scalar("SELECT count(*) FROM (SELECT stable_component_key FROM solar_component_identities GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_jpl_targets": scalar("SELECT count(*) FROM (SELECT jpl_horizons_target FROM solar_component_identities GROUP BY 1 HAVING count(*)<>1)"),
        "duplicate_legacy_keys": scalar("SELECT count(*) FROM (SELECT legacy_component_key FROM solar_component_identities GROUP BY 1 HAVING count(*)<>1)"),
        "missing_identity_fields": scalar("SELECT count(*) FROM solar_component_identities WHERE stable_component_key IS NULL OR display_name IS NULL OR object_class IS NULL OR object_kind IS NULL OR jpl_horizons_target IS NULL OR operator_seed_target_key IS NULL"),
        "core_candidates_not_bound": scalar("SELECT count(*) FROM solar_component_identities WHERE object_class IN ('star','planet','dwarf_planet') AND core_object_id IS NULL"),
        "noncore_candidates_bound": scalar("SELECT count(*) FROM solar_component_identities WHERE object_class IN ('moon','minor_body','artificial') AND core_object_id IS NOT NULL"),
        "name_only_binding_methods": scalar("SELECT count(*) FROM solar_component_identities WHERE identity_method LIKE '%name_only%'"),
        "orphan_aliases": scalar("SELECT count(*) FROM solar_component_aliases a LEFT JOIN solar_component_identities i USING(solar_identity_id) WHERE i.solar_identity_id IS NULL"),
        "orphan_identifiers": scalar("SELECT count(*) FROM solar_component_identifiers x LEFT JOIN solar_component_identities i USING(solar_identity_id) WHERE i.solar_identity_id IS NULL"),
        "unresolved_relations": scalar("SELECT count(*) FROM solar_relation_identity_outcomes WHERE relation_status NOT IN ('accepted','reference_origin')"),
        "relations_without_targets": scalar("SELECT count(*) FROM solar_relation_identity_outcomes WHERE target_component_key IS NULL"),
        "accepted_relations_without_centers": scalar("SELECT count(*) FROM solar_relation_identity_outcomes WHERE relation_status='accepted' AND center_component_key IS NULL"),
    }
    failing = {key: value for key, value in checks.items() if value}
    return {
        "status": "pass" if not failing else "fail",
        "counts": counts,
        "expected_counts": expected,
        "checks": checks,
        "failing_checks": failing,
    }


def compile_identity(
    policy_path: Path, state: Path, output_root: Path, *, link_into_state: bool
) -> dict[str, Any]:
    started = time.monotonic()
    cpu_started = time.process_time()
    policy = load_object(policy_path)
    validate_policy(policy)
    evidence_root, evidence_manifest = resolve_input(state, policy["inputs"]["scientific_evidence"])
    core_root, core_manifest = resolve_input(state, policy["inputs"]["clean_runtime_core"])
    evidence_db = resolve_database(evidence_root, evidence_manifest, "scientific_evidence.duckdb")
    core_db = resolve_database(core_root, core_manifest, "core.duckdb")
    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    input_identity = {
        name: {
            "build_id": spec["build_id"],
            "manifest_sha256": spec["manifest_sha256"],
        }
        for name, spec in policy["inputs"].items()
    }
    build_id = stable_hash({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "inputs": input_identity,
    })[:24]
    final = output_root / build_id
    if (final / "manifest.json").is_file():
        manifest = load_object(final / "manifest.json")
        if manifest.get("build_id") != build_id:
            raise ValueError("Solar identity build collision")
        return manifest
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    products: dict[str, dict[str, Any]] = {}
    try:
        con = duckdb.connect()
        try:
            con.execute(f"ATTACH {sql_literal(evidence_db)} AS evidence (READ_ONLY)")
            con.execute(f"ATTACH {sql_literal(core_db)} AS core (READ_ONLY)")
            create_identity_tables(con, policy, build_id)
            verification = verify(con, policy)
            if verification["status"] != "pass":
                raise ValueError(f"Solar identity verification failed: {verification['failing_checks']}")
            for table in (
                "solar_component_identities", "solar_component_aliases",
                "solar_component_identifiers", "solar_relation_identity_outcomes",
            ):
                path = staging / f"{table}.parquet"
                con.execute(
                    f"COPY (SELECT * FROM {table} ORDER BY ALL) TO {sql_literal(path)} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)"
                )
                products[path.name] = {
                    "rows": int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),
                    "bytes": path.stat().st_size,
                    "sha256": file_sha256(path),
                    "determinism": "byte_exact",
                }
        finally:
            con.close()
        manifest = {
            "schema_version": "spacegate.e7_solar_identity_manifest.v1",
            "build_id": build_id,
            "status": "pass",
            "scientific_authority": False,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha,
            "compiler_sha256": compiler_sha,
            "inputs": input_identity,
            "stability_databases_opened": [],
            "verification": verification,
            "products": products,
            "performance": {
                "wall_seconds": round(time.monotonic() - started, 6),
                "cpu_seconds": round(time.process_time() - cpu_started, 6),
                "peak_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
            },
        }
        write_object_atomic(staging / "manifest.json", manifest)
        os.replace(staging, final)
        if link_into_state:
            links = state / "derived/evidence_lake_v2/solar_identity"
            links.mkdir(parents=True, exist_ok=True)
            link = links / build_id
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
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_identity(
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
