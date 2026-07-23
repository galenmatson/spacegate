#!/usr/bin/env python3
"""Select bounded stellar source-model classifications with exact evidence lineage."""

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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_selected_stellar_classifications.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-selected-stellar-classifications")


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

    def run(self, name: str, operation: Callable[[], Any]) -> Any:
        started = time.monotonic()
        cpu_started = time.process_time()
        before = resource.getrusage(resource.RUSAGE_SELF)
        status = "pass"
        try:
            return operation()
        except Exception:
            status = "fail"
            raise
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
    if policy.get("schema_version") != "spacegate.e7_selected_stellar_classifications_policy.v1":
        raise ValueError("unsupported selected stellar classification policy")
    if policy.get("rules") != {
        "open_stability_databases": False,
        "create_canonical_identity": False,
        "create_canonical_containment": False,
        "require_exact_release_identifier": True,
        "preserve_source_model_semantics": True,
    }:
        raise ValueError("unsafe selected stellar classification rules")
    contract = policy.get("gaia_dsc_white_dwarf") or {}
    if (
        contract.get("classification_scheme")
        != "gaia_dr3_source_classifier_probability_vectors"
        or contract.get("models") != ["DSC_combmod", "DSC_specmod"]
        or float(contract.get("probability_threshold", -1)) != 0.5
        or contract.get("classification_value") != "WD"
    ):
        raise ValueError("invalid Gaia DSC white-dwarf contract")
    ultracool = policy.get("ultracoolsheet_source_native_classification") or {}
    if (
        ultracool.get("identifier_namespace") != "ultracoolsheet_name"
        or ultracool.get("permanent_alias_kind") != "ultracoolsheet_name"
        or ultracool.get("permanent_source_catalog") != "ultracoolsheet"
        or ultracool.get("classification_schemes")
        != ["optical_spectral_type", "infrared_spectral_type"]
    ):
        raise ValueError("invalid UltracoolSheet source-native classification contract")


def declared_database(manifest: dict[str, Any], database: str) -> tuple[int, str]:
    products = manifest.get("products") or {}
    if database in products:
        return int(products[database]["bytes"]), str(products[database]["sha256"])
    if manifest.get("database") == database:
        return int(manifest["database_bytes"]), str(manifest["database_sha256"])
    raise ValueError(f"input manifest does not declare {database}")


def materialize(
    con: duckdb.DuckDBPyConnection,
    *,
    build_id: str,
    contract: dict[str, Any],
    source_id: str,
    release_id: str,
    policy_version: str,
) -> dict[str, int]:
    threshold = float(contract["probability_threshold"])
    scheme = sql_literal(str(contract["classification_scheme"]))
    source_namespace = sql_literal(str(contract["identifier_namespace"]))
    canonical_namespace = sql_literal(str(contract["canonical_namespace"]))
    basis = sql_literal(str(contract["evidence_basis"]))
    status = sql_literal(str(contract["classification_status"]))
    classification = sql_literal(str(contract["classification_value"]))
    combmod = (
        "try_cast(json_extract_string(ce.quality_json," 
        "'$.models.DSC_combmod.white_dwarf') AS DOUBLE)"
    )
    specmod = (
        "try_cast(json_extract_string(ce.quality_json," 
        "'$.models.DSC_specmod.white_dwarf') AS DOUBLE)"
    )
    probability = f"greatest(coalesce({combmod},0.0),coalesce({specmod},0.0))"
    con.execute(
        f"""
        CREATE TEMP TABLE threshold_candidates AS
        SELECT ce.evidence_id,ce.source_record_id,ce.classification_scheme,
          ce.method,ce.model,ce.reference_raw,ce.quality_json,
          i.identifier_raw AS gaia_id_raw,i.identifier_normalized AS gaia_id,
          {combmod}::DOUBLE AS combmod_probability,
          {specmod}::DOUBLE AS specmod_probability,
          {probability}::DOUBLE AS selected_probability,
          CASE WHEN coalesce({combmod},0.0)>=coalesce({specmod},0.0)
            THEN 'DSC_combmod' ELSE 'DSC_specmod' END::VARCHAR AS selected_model
        FROM gaia_ap.stellar_classification_evidence ce
        JOIN gaia_ap.identifier_claim_evidence i USING(source_record_id)
        WHERE ce.classification_scheme={scheme}
          AND i.namespace={source_namespace}
          AND {probability}>={threshold};

        CREATE TABLE stellar_model_classification_bindings AS
        WITH source_counts AS (
          SELECT gaia_id,count(*)::BIGINT AS source_candidate_count
          FROM threshold_candidates GROUP BY 1
        ), canonical AS (
          SELECT id_value_norm AS gaia_id,count(*)::BIGINT AS canonical_candidate_count,
            min(target_id)::HUGEINT AS star_id,
            min(stable_object_key)::VARCHAR AS stable_object_key,
            min(system_stable_object_key)::VARCHAR AS system_stable_object_key
          FROM core.object_identifiers
          WHERE target_type='star' AND namespace={canonical_namespace}
          GROUP BY 1
        )
        SELECT row_number() OVER (ORDER BY c.evidence_id)::BIGINT AS binding_id,
          {sql_literal(build_id)}::VARCHAR AS build_id,c.evidence_id,c.source_record_id,
          {sql_literal(source_id)}::VARCHAR AS source_id,
          {sql_literal(release_id)}::VARCHAR AS release_id,
          c.gaia_id_raw,c.gaia_id,c.combmod_probability,c.specmod_probability,
          c.selected_probability,c.selected_model,
          s.source_candidate_count,
          coalesce(k.canonical_candidate_count,0)::BIGINT AS canonical_candidate_count,
          CASE WHEN s.source_candidate_count=1 AND k.canonical_candidate_count=1
            THEN k.star_id END::HUGEINT AS star_id,
          CASE WHEN s.source_candidate_count=1 AND k.canonical_candidate_count=1
            THEN k.stable_object_key END::VARCHAR AS stable_object_key,
          CASE WHEN s.source_candidate_count=1 AND k.canonical_candidate_count=1
            THEN k.system_stable_object_key END::VARCHAR AS system_stable_object_key,
          CASE WHEN s.source_candidate_count>1 THEN 'ambiguous'
               WHEN k.canonical_candidate_count IS NULL THEN 'missing'
               WHEN k.canonical_candidate_count>1 THEN 'ambiguous'
               ELSE 'accepted' END::VARCHAR AS binding_status,
          CASE WHEN s.source_candidate_count>1 THEN 'duplicate_source_gaia_identifier'
               WHEN k.canonical_candidate_count IS NULL THEN 'canonical_gaia_identifier_missing'
               WHEN k.canonical_candidate_count>1 THEN 'canonical_gaia_identifier_collision'
               ELSE 'exact_release_gaia_dr3_identifier' END::VARCHAR AS binding_reason,
          false::BOOLEAN AS creates_canonical_identity,
          false::BOOLEAN AS creates_canonical_containment,
          {sql_literal(policy_version)}::VARCHAR AS policy_version
        FROM threshold_candidates c
        JOIN source_counts s USING(gaia_id)
        LEFT JOIN canonical k USING(gaia_id)
        ORDER BY c.evidence_id;

        CREATE TABLE selected_stellar_model_classifications AS
        SELECT row_number() OVER (ORDER BY b.star_id,b.evidence_id)::BIGINT
            AS selected_classification_id,
          {sql_literal(build_id)}::VARCHAR AS build_id,b.star_id,
          s.system_id,b.stable_object_key,b.system_stable_object_key,
          {classification}::VARCHAR AS classification_value,
          {status}::VARCHAR AS classification_status,{basis}::VARCHAR AS evidence_basis,
          sha256(concat_ws('|',{sql_literal(build_id)},b.evidence_id,b.stable_object_key,
            {classification}))::VARCHAR AS selected_fact_id,
          to_json(struct_pack(
            selected_model := b.selected_model,
            selected_probability := b.selected_probability,
            DSC_combmod_white_dwarf := b.combmod_probability,
            DSC_specmod_white_dwarf := b.specmod_probability
          ))::VARCHAR AS source_value,
          b.selected_probability::DOUBLE AS confidence_score,b.source_id,b.release_id,
          b.evidence_id,b.source_record_id,c.method,b.selected_model AS model,
          c.reference_raw,c.quality_json,
          {sql_literal(policy_version)}::VARCHAR AS policy_version
        FROM stellar_model_classification_bindings b
        JOIN threshold_candidates c USING(evidence_id,source_record_id)
        JOIN core.stars s ON s.star_id=b.star_id
        WHERE b.binding_status='accepted'
        ORDER BY b.star_id,b.evidence_id;
        """
    )
    return {
        "source_records": int(con.execute(
            "SELECT count(*) FROM gaia_ap.stellar_classification_evidence "
            "WHERE classification_scheme=?", [contract["classification_scheme"]]
        ).fetchone()[0]),
        "threshold_candidates": int(con.execute(
            "SELECT count(*) FROM threshold_candidates"
        ).fetchone()[0]),
        "selected_classifications": int(con.execute(
            "SELECT count(*) FROM selected_stellar_model_classifications"
        ).fetchone()[0]),
    }


def materialize_ultracool_source_classifications(
    con: duckdb.DuckDBPyConnection,
    *,
    build_id: str,
    contract: dict[str, Any],
    source_id: str,
    release_id: str,
    policy_version: str,
) -> dict[str, int]:
    namespace = sql_literal(str(contract["identifier_namespace"]))
    alias_kind = sql_literal(str(contract["permanent_alias_kind"]))
    source_catalog = sql_literal(str(contract["permanent_source_catalog"]))
    schemes = "(" + ",".join(sql_literal(value) for value in contract["classification_schemes"]) + ")"
    parsed_class = spectral_class_sql("ce.classification_raw", "NULL", "'star'")
    con.execute(
        f"""
        CREATE TEMP TABLE ultracool_permanent_identifiers AS
        SELECT trim(alias_raw)::VARCHAR source_native_identifier,
          count(*)::BIGINT permanent_candidate_count,min(star_id)::HUGEINT star_id,
          min(system_id)::HUGEINT system_id,min(stable_object_key)::VARCHAR stable_object_key,
          min(system_stable_object_key)::VARCHAR system_stable_object_key
        FROM core.aliases
        WHERE target_type='star' AND alias_kind={alias_kind}
          AND source_catalog={source_catalog} AND trim(coalesce(alias_raw,''))<>''
        GROUP BY 1;

        CREATE TEMP TABLE ultracool_release_identifiers AS
        SELECT trim(identifier_raw)::VARCHAR source_native_identifier,
          count(*)::BIGINT source_candidate_count,
          min(source_record_id)::VARCHAR source_record_id,
          min(evidence_id)::VARCHAR identifier_evidence_id
        FROM ultracool.identifier_claim_evidence
        WHERE namespace={namespace} AND trim(coalesce(identifier_raw,''))<>''
        GROUP BY 1;

        CREATE TABLE source_classification_bindings AS
        SELECT row_number() OVER(ORDER BY p.source_native_identifier)::BIGINT binding_id,
          {sql_literal(build_id)}::VARCHAR build_id,
          {sql_literal(source_id)}::VARCHAR source_id,
          {sql_literal(release_id)}::VARCHAR release_id,
          p.source_native_identifier,p.permanent_candidate_count,
          coalesce(r.source_candidate_count,0)::BIGINT source_candidate_count,
          r.source_record_id,r.identifier_evidence_id,
          CASE WHEN p.permanent_candidate_count=1 AND r.source_candidate_count=1
            THEN p.star_id END::HUGEINT star_id,
          CASE WHEN p.permanent_candidate_count=1 AND r.source_candidate_count=1
            THEN p.system_id END::HUGEINT system_id,
          CASE WHEN p.permanent_candidate_count=1 AND r.source_candidate_count=1
            THEN p.stable_object_key END::VARCHAR stable_object_key,
          CASE WHEN p.permanent_candidate_count=1 AND r.source_candidate_count=1
            THEN p.system_stable_object_key END::VARCHAR system_stable_object_key,
          CASE WHEN p.permanent_candidate_count>1 THEN 'ambiguous'
               WHEN r.source_candidate_count IS NULL THEN 'missing'
               WHEN r.source_candidate_count>1 THEN 'ambiguous'
               ELSE 'accepted' END::VARCHAR binding_status,
          CASE WHEN p.permanent_candidate_count>1 THEN 'permanent_source_identifier_collision'
               WHEN r.source_candidate_count IS NULL THEN 'current_release_identifier_missing'
               WHEN r.source_candidate_count>1 THEN 'current_release_source_identifier_collision'
               ELSE 'exact_release_source_native_identifier' END::VARCHAR binding_reason,
          false::BOOLEAN creates_canonical_identity,
          false::BOOLEAN creates_canonical_containment,
          {sql_literal(policy_version)}::VARCHAR policy_version
        FROM ultracool_permanent_identifiers p
        LEFT JOIN ultracool_release_identifiers r USING(source_native_identifier)
        ORDER BY p.source_native_identifier;

        CREATE TABLE source_classification_evidence_projection AS
        SELECT row_number() OVER(ORDER BY b.star_id,ce.classification_scheme,ce.evidence_id)::BIGINT
            source_classification_id,
          {sql_literal(build_id)}::VARCHAR build_id,b.star_id,b.system_id,
          b.stable_object_key,b.system_stable_object_key,
          ce.classification_scheme,{parsed_class}::VARCHAR classification_value,
          'source'::VARCHAR classification_status,
          concat('selected_ultracoolsheet_',ce.classification_scheme)::VARCHAR evidence_basis,
          sha256(concat_ws('|',{sql_literal(build_id)},ce.evidence_id,b.stable_object_key,
            ce.classification_scheme))::VARCHAR selected_fact_id,
          ce.classification_raw::VARCHAR source_value,
          CASE WHEN ce.classification_scheme='optical_spectral_type' THEN 0.98 ELSE 0.96 END::DOUBLE
            confidence_score,
          b.source_id,b.release_id,ce.evidence_id,ce.source_record_id,
          ce.method,ce.model,ce.reference_raw,ce.quality_json,
          {sql_literal(policy_version)}::VARCHAR policy_version
        FROM source_classification_bindings b
        JOIN ultracool.stellar_classification_evidence ce USING(source_record_id)
        WHERE b.binding_status='accepted' AND ce.classification_scheme IN {schemes}
          AND {parsed_class} IS NOT NULL
        ORDER BY b.star_id,ce.classification_scheme,ce.evidence_id;
        """
    )
    result = {
        "target_identifiers": int(con.execute(
            "SELECT count(*) FROM source_classification_bindings"
        ).fetchone()[0]),
        "classification_evidence": int(con.execute(
            "SELECT count(*) FROM source_classification_evidence_projection"
        ).fetchone()[0]),
        "classified_stars": int(con.execute(
            "SELECT count(DISTINCT star_id) FROM source_classification_evidence_projection"
        ).fetchone()[0]),
    }
    for scheme, count in con.execute(
        "SELECT classification_scheme,count(*) FROM source_classification_evidence_projection "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall():
        result[f"classification_evidence_{scheme}"] = int(count)
    return result


def compile_selected(
    policy_path: Path,
    state: Path,
    output_root: Path,
    *,
    link_into_state: bool = True,
) -> dict[str, Any]:
    timing = Timings()
    policy = load_object(policy_path)
    validate_policy(policy)
    inputs: dict[str, dict[str, Any]] = {}
    for name, spec in policy["inputs"].items():
        root = state / spec["relative_path"]
        manifest_path = root / "manifest.json"
        database_path = root / spec["database"]
        manifest = load_object(manifest_path)
        if file_sha256(manifest_path) != spec["manifest_sha256"]:
            raise ValueError(f"{name} manifest checksum mismatch")
        if manifest.get("build_id") != spec["build_id"]:
            raise ValueError(f"{name} build identity mismatch")
        accepted = manifest.get("status") == "pass" or (manifest.get("report") or {}).get("status") == "pass"
        if not accepted:
            raise ValueError(f"{name} is not accepted")
        expected_bytes, expected_sha = declared_database(manifest, spec["database"])
        if database_path.stat().st_size != expected_bytes:
            raise ValueError(f"{name} database size mismatch")
        inputs[name] = {
            **spec,
            "root": root,
            "manifest_path": manifest_path,
            "database_path": database_path,
            "database_bytes": expected_bytes,
            "database_sha256": expected_sha,
        }

    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    build_id = stable_hash({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "inputs": {name: row["manifest_sha256"] for name, row in inputs.items()},
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        return load_object(final_dir / "manifest.json")

    def verify_input_bytes() -> None:
        for name, row in inputs.items():
            if file_sha256(row["database_path"]) != row["database_sha256"]:
                raise ValueError(f"{name} database checksum mismatch")

    timing.run("verify_immutable_input_bytes", verify_input_bytes)
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    database = staging / "selected_stellar_classifications.duckdb"
    parquet_dir = staging / "parquet"
    parquet_dir.mkdir()
    try:
        con = duckdb.connect(str(database), config={
            "threads": "16", "memory_limit": "32GB",
            "temp_directory": str(staging / "duckdb-tmp"),
            "preserve_insertion_order": "true",
        })
        try:
            con.execute(
                f"ATTACH {sql_literal(inputs['clean_foundation']['database_path'])} "
                "AS core (READ_ONLY)"
            )
            con.execute(
                f"ATTACH {sql_literal(inputs['gaia_ap_evidence']['database_path'])} "
                "AS gaia_ap (READ_ONLY)"
            )
            con.execute(
                f"ATTACH {sql_literal(inputs['ultracool_evidence']['database_path'])} "
                "AS ultracool (READ_ONLY)"
            )
            counts = timing.run("select_gaia_dsc_white_dwarfs", lambda: materialize(
                con,
                build_id=build_id,
                contract=policy["gaia_dsc_white_dwarf"],
                source_id=inputs["gaia_ap_evidence"]["source_id"],
                release_id=inputs["gaia_ap_evidence"]["release_id"],
                policy_version=policy["policy_version"],
            ))
            outcomes = {
                str(status): int(count)
                for status, count in con.execute(
                    "SELECT binding_status,count(*) FROM stellar_model_classification_bindings "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            ultracool_counts = timing.run(
                "select_ultracoolsheet_source_native_classifications",
                lambda: materialize_ultracool_source_classifications(
                    con,
                    build_id=build_id,
                    contract=policy["ultracoolsheet_source_native_classification"],
                    source_id=inputs["ultracool_evidence"]["source_id"],
                    release_id=inputs["ultracool_evidence"]["release_id"],
                    policy_version=policy["policy_version"],
                ),
            )
            source_outcomes = {
                str(status): int(count)
                for status, count in con.execute(
                    "SELECT binding_status,count(*) FROM source_classification_bindings "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            checks = {
                "source_record_delta": counts["source_records"]
                - int(policy["gaia_dsc_white_dwarf"]["expected_source_records"]),
                "threshold_candidate_delta": counts["threshold_candidates"]
                - int(policy["gaia_dsc_white_dwarf"]["expected_threshold_candidates"]),
                "selected_delta": counts["selected_classifications"]
                - int(policy["gaia_dsc_white_dwarf"]["expected_binding_outcomes"]["accepted"]),
                "binding_partition_delta": counts["threshold_candidates"] - sum(outcomes.values()),
                "duplicate_selected_stars": int(con.execute(
                    "SELECT count(*) FROM (SELECT star_id FROM selected_stellar_model_classifications "
                    "GROUP BY 1 HAVING count(*)<>1)"
                ).fetchone()[0]),
                "selected_without_evidence": int(con.execute(
                    "SELECT count(*) FROM selected_stellar_model_classifications "
                    "WHERE selected_fact_id IS NULL OR evidence_id IS NULL"
                ).fetchone()[0]),
                "identity_promotions": int(con.execute(
                    "SELECT (SELECT count(*) FROM stellar_model_classification_bindings "
                    "WHERE creates_canonical_identity OR creates_canonical_containment) + "
                    "(SELECT count(*) FROM source_classification_bindings "
                    "WHERE creates_canonical_identity OR creates_canonical_containment)"
                ).fetchone()[0]),
                "source_target_identifier_delta": ultracool_counts["target_identifiers"]
                    - int(policy["ultracoolsheet_source_native_classification"]["expected_target_identifiers"]),
                "source_binding_partition_delta": ultracool_counts["target_identifiers"]
                    - sum(source_outcomes.values()),
            }
            expected_outcomes = policy["gaia_dsc_white_dwarf"]["expected_binding_outcomes"]
            for status, expected in expected_outcomes.items():
                checks[f"binding_{status}_delta"] = outcomes.get(status, 0) - int(expected)
            source_contract = policy["ultracoolsheet_source_native_classification"]
            for status, expected in source_contract["expected_binding_outcomes"].items():
                checks[f"source_binding_{status}_delta"] = source_outcomes.get(status, 0) - int(expected)
            for scheme, expected in source_contract["expected_classification_evidence"].items():
                checks[f"source_{scheme}_delta"] = (
                    ultracool_counts.get(f"classification_evidence_{scheme}", 0) - int(expected)
                )
            if any(checks.values()):
                raise ValueError(f"selected stellar classification verification failed: {checks}")
            timing.run("indexes", lambda: con.execute(
                "CREATE UNIQUE INDEX selected_model_star_uq ON "
                "selected_stellar_model_classifications(star_id);"
                "CREATE INDEX model_binding_gaia_idx ON "
                "stellar_model_classification_bindings(gaia_id);"
                "CREATE UNIQUE INDEX source_binding_identifier_uq ON "
                "source_classification_bindings(source_native_identifier);"
                "CREATE INDEX source_classification_star_idx ON "
                "source_classification_evidence_projection(star_id);"
            ))
            timing.run("canonical_parquet_export", lambda: (
                con.execute(
                    f"COPY stellar_model_classification_bindings TO "
                    f"{sql_literal(parquet_dir / 'stellar_model_classification_bindings.parquet')} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
                ),
                con.execute(
                    f"COPY source_classification_bindings TO "
                    f"{sql_literal(parquet_dir / 'source_classification_bindings.parquet')} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
                ),
                con.execute(
                    f"COPY source_classification_evidence_projection TO "
                    f"{sql_literal(parquet_dir / 'source_classification_evidence_projection.parquet')} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
                ),
                con.execute(
                    f"COPY selected_stellar_model_classifications TO "
                    f"{sql_literal(parquet_dir / 'selected_stellar_model_classifications.parquet')} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
                ),
            ))
            timing.run("checkpoint", lambda: con.execute("CHECKPOINT"))
        finally:
            con.close()

        def hash_products() -> dict[str, Any]:
            result: dict[str, Any] = {}
            for path in [database, *sorted(parquet_dir.glob("*.parquet"))]:
                relative = str(path.relative_to(staging))
                result[relative] = {
                    "bytes": path.stat().st_size,
                    "sha256": file_sha256(path),
                    "determinism": "logical_tables" if path.suffix == ".duckdb" else "byte_exact",
                }
            return result

        products = timing.run("hash_products", hash_products)
        manifest = {
            "schema_version": "spacegate.e7_selected_stellar_classifications_manifest.v1",
            "build_id": build_id,
            "status": "pass",
            "generated_at": utc_now(),
            "policy_version": policy["policy_version"],
            "compiler_version": policy["compiler_version"],
            "policy_sha256": policy_sha,
            "compiler_sha256": compiler_sha,
            "inputs": {
                name: {
                    "build_id": row["build_id"],
                    "manifest_sha256": row["manifest_sha256"],
                    "database_sha256": row["database_sha256"],
                }
                for name, row in inputs.items()
            },
            "stability_databases_opened": [],
            "counts": {**counts, **{f"source_{key}": value for key, value in ultracool_counts.items()}},
            "binding_outcomes": outcomes,
            "source_binding_outcomes": source_outcomes,
            "verification": checks,
            "products": products,
            "performance": timing.report(),
        }
        write_object_atomic(staging / "manifest.json", manifest)
        shutil.rmtree(staging / "duckdb-tmp", ignore_errors=True)
        os.replace(staging, final_dir)
        if link_into_state:
            link_root = state / "derived/evidence_lake_v2/selected_stellar_classifications"
            link_root.mkdir(parents=True, exist_ok=True)
            link = link_root / build_id
            if not link.exists() and not link.is_symlink():
                temporary = link_root / f".{build_id}.link"
                temporary.symlink_to(final_dir)
                os.replace(temporary, link)
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
    manifest = compile_selected(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
    )
    if args.report:
        write_object_atomic(args.report.resolve(), manifest)
    print(json.dumps({
        "build_id": manifest["build_id"],
        "status": manifest["status"],
        "counts": manifest["counts"],
        "binding_outcomes": manifest["binding_outcomes"],
        "wall_seconds": manifest["performance"]["wall_seconds"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
