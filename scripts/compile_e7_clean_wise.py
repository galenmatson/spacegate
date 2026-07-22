#!/usr/bin/env python3
"""Compile clean targeted WISE evidence and compatibility projections."""

from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from compile_e7_clean_science import Timings


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_wise.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_OUTPUT = Path("/mnt/space/spacegate/e7-clean-wise")


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def file_hash(path: Path) -> str:
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


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != "spacegate.e7_clean_wise_policy.v1":
        raise ValueError("unsupported E7 clean WISE policy")
    expected = {
        "open_stability_databases": False,
        "copy_legacy_wise_csv": False,
        "accept_only_unique_nearest_source": True,
        "candidate_only_parallax_like": True,
        "no_core_inventory_promotion": True,
    }
    if policy.get("rules") != expected:
        raise ValueError("unsafe E7 clean WISE rules")
    catalogs = [str(row.get("catalog") or "") for row in policy.get("sources") or []]
    if catalogs != ["catwise", "allwise"]:
        raise ValueError("clean WISE requires exactly CatWISE then AllWISE inputs")


def resolve_inputs(policy: dict[str, Any], state: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    target_path = state / policy["target_set"]["path"]
    target = load_object(target_path)
    targets = target.get("targets")
    if not isinstance(targets, list):
        raise ValueError("target-set report lacks targets")
    if stable_hash(targets) != policy["target_set"]["targets_sha256"]:
        raise ValueError("target-set content mismatch")
    if target.get("policy_sha256") != policy["target_set"]["policy_sha256"]:
        raise ValueError("target-set policy mismatch")
    if len(targets) != int(policy["target_set"]["target_count"]):
        raise ValueError("target-set count mismatch")

    sources = []
    for configured in policy["sources"]:
        row = dict(configured)
        manifest_path = state / row["typed_manifest"]
        manifest = load_object(manifest_path)
        if manifest.get("content_sha256") != row["typed_content_sha256"]:
            raise ValueError(f"typed content mismatch: {row['catalog']}")
        table = next(
            (item for item in manifest["tables"] if item["source_name"] == row["table"]),
            None,
        )
        if not table or table.get("status") != "typed":
            raise ValueError(f"typed table unavailable: {row['catalog']}")
        parquet = manifest_path.parent / table["parquet_path"]
        if file_hash(parquet) != row["table_sha256"]:
            raise ValueError(f"typed table checksum mismatch: {row['catalog']}")
        query_manifest_path = state / row["query_manifest"]
        if file_hash(query_manifest_path) != row["query_manifest_sha256"]:
            raise ValueError(f"query manifest checksum mismatch: {row['catalog']}")
        query_manifest = load_object(query_manifest_path)
        if query_manifest.get("targets_sha256") != policy["target_set"]["targets_sha256"]:
            raise ValueError(f"query target mismatch: {row['catalog']}")
        row.update(
            {
                "typed_manifest_path": manifest_path,
                "typed_manifest": manifest,
                "parquet": parquet,
                "query_manifest_path": query_manifest_path,
                "query_manifest_content": query_manifest,
            }
        )
        sources.append(row)
    return target, sources


def insert_target_rows(con: duckdb.DuckDBPyConnection, target: dict[str, Any]) -> None:
    con.execute(
        """
        CREATE TABLE targeted_stars(
          target_index INTEGER,star_id HUGEINT,system_id HUGEINT,
          stable_object_key VARCHAR,system_stable_object_key VARCHAR,
          star_name VARCHAR,system_name VARCHAR,dist_ly DOUBLE,
          ra_deg DOUBLE,dec_deg DOUBLE,pmra_mas_yr DOUBLE,pmdec_mas_yr DOUBLE,
          query_coordinate_basis VARCHAR,selection_reasons_json VARCHAR,
          ra_deg_fact_id VARCHAR,dec_deg_fact_id VARCHAR,
          pmra_mas_yr_fact_id VARCHAR,pmdec_mas_yr_fact_id VARCHAR
        )
        """
    )
    rows = []
    for item in target["targets"]:
        rows.append(
            (
                item["target_index"], item["star_id"], item["system_id"],
                item["stable_object_key"], item["system_stable_object_key"],
                item["star_name"], item["system_name"], item["dist_ly"],
                item["ra_deg"], item["dec_deg"], item.get("pmra_mas_yr"),
                item.get("pmdec_mas_yr"), item["query_coordinate_basis"],
                json.dumps(item["selection_reasons"], separators=(",", ":")),
                item.get("ra_deg_fact_id"), item.get("dec_deg_fact_id"),
                item.get("pmra_mas_yr_fact_id"), item.get("pmdec_mas_yr_fact_id"),
            )
        )
    con.executemany("INSERT INTO targeted_stars VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)


def insert_query_outcomes(
    con: duckdb.DuckDBPyConnection,
    sources: list[dict[str, Any]],
) -> None:
    con.execute(
        """
        CREATE TABLE wise_query_outcomes(
          catalog VARCHAR,source_id VARCHAR,release_id VARCHAR,
          target_index INTEGER,star_id HUGEINT,system_id HUGEINT,
          stable_object_key VARCHAR,query_response_member VARCHAR,
          query_ra_deg DOUBLE,query_dec_deg DOUBLE,query_radius_arcsec DOUBLE,
          source_row_count INTEGER,source_status VARCHAR,response_sha256 VARCHAR,
          response_bytes BIGINT,error_response_count INTEGER,
          error_responses_json VARCHAR,query_url VARCHAR
        )
        """
    )
    rows = []
    target_by_index = {
        int(row[0]): row
        for row in con.execute(
            "SELECT target_index,star_id,system_id,stable_object_key FROM targeted_stars"
        ).fetchall()
    }
    for source in sources:
        responses = source["query_manifest_content"]["responses"]
        if len(responses) != len(target_by_index):
            raise ValueError(f"query response accounting mismatch: {source['catalog']}")
        for response in responses:
            target_index = int(response["target"]["target_index"])
            canonical = target_by_index[target_index]
            if response["target"]["stable_object_key"] != canonical[3]:
                raise ValueError(f"query target binding mismatch: {source['catalog']}:{target_index}")
            errors = response.get("error_responses") or []
            rows.append(
                (
                    source["catalog"], source["source_id"], source["release_id"],
                    target_index, canonical[1], canonical[2], canonical[3],
                    response["filename"], response["query_ra_deg"], response["query_dec_deg"],
                    response["query_radius_arcsec"], response["row_count"],
                    response.get("source_status", "table"), response["sha256"],
                    response["bytes"], len(errors),
                    json.dumps(errors, sort_keys=True, separators=(",", ":")), response["url"],
                )
            )
    con.executemany("INSERT INTO wise_query_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)


def create_evidence_tables(
    con: duckdb.DuckDBPyConnection,
    sources: list[dict[str, Any]],
    policy: dict[str, Any],
) -> None:
    catwise = next(row for row in sources if row["catalog"] == "catwise")
    allwise = next(row for row in sources if row["catalog"] == "allwise")
    con.execute(
        f"CREATE VIEW typed_catwise AS SELECT * FROM read_parquet({sql_literal(catwise['parquet'])})"
    )
    con.execute(
        f"CREATE VIEW typed_allwise AS SELECT * FROM read_parquet({sql_literal(allwise['parquet'])})"
    )
    con.execute(
        """
        CREATE TABLE catwise_sources AS
        SELECT row_number() OVER(ORDER BY source_key)::BIGINT catwise_source_id,* EXCLUDE(source_key)
        FROM (
          SELECT coalesce(nullif(source_id,''),nullif(source_name,'')) source_key,
                 source_name source_designation,source_id source_native_id,
                 ra ra_deg,dec dec_deg,w1mpro w1_mag,w2mpro w2_mag,
                 w1snr w1_snr,w2snr w2_snr,pmra,pmdec,sigpmra pmra_error,
                 sigpmdec pmdec_error,par_pm parallax_like_arcsec,
                 par_pmsig parallax_like_error_arcsec,cc_flags,ab_flags,n_aw,dist_cc,
                 sha256(concat_ws('|',source_name,source_id,ra,dec,w1mpro,w2mpro,
                   w1snr,w2snr,pmra,pmdec,sigpmra,sigpmdec,par_pm,par_pmsig,
                   cc_flags,ab_flags,n_aw,dist_cc)) source_row_sha256,
                 row_number() OVER(PARTITION BY coalesce(nullif(source_id,''),nullif(source_name,''))
                   ORDER BY query_response_member,dist) source_occurrence_rank
          FROM typed_catwise
          WHERE coalesce(nullif(source_id,''),nullif(source_name,'')) IS NOT NULL
        ) WHERE source_occurrence_rank=1 ORDER BY source_key
        """
    )
    con.execute(
        """
        CREATE TABLE allwise_sources AS
        SELECT row_number() OVER(ORDER BY source_key)::BIGINT allwise_source_id,* EXCLUDE(source_key)
        FROM (
          SELECT designation source_key,designation source_designation,
                 ra ra_deg,dec dec_deg,w1mpro w1_mag,w2mpro w2_mag,w3mpro w3_mag,w4mpro w4_mag,
                 w1snr w1_snr,w2snr w2_snr,w3snr w3_snr,w4snr w4_snr,
                 pmra,pmdec,sigpmra pmra_error,sigpmdec pmdec_error,
                 cc_flags,ph_qual quality_flags,ext_flg,nb,na,
                 sha256(concat_ws('|',designation,ra,dec,w1mpro,w2mpro,w3mpro,w4mpro,
                   w1snr,w2snr,w3snr,w4snr,pmra,pmdec,sigpmra,sigpmdec,
                   cc_flags,ph_qual,ext_flg,nb,na)) source_row_sha256,
                 row_number() OVER(PARTITION BY designation ORDER BY query_response_member,dist) source_occurrence_rank
          FROM typed_allwise WHERE nullif(designation,'') IS NOT NULL
        ) WHERE source_occurrence_rank=1 ORDER BY source_key
        """
    )
    con.execute(
        """
        CREATE TABLE wise_sources AS
        SELECT row_number() OVER(ORDER BY source_catalog,source_key)::BIGINT wise_source_id,*
        FROM (
          SELECT 'catwise'::VARCHAR source_catalog,'CatWISE2020'::VARCHAR source_version,
                 source_native_id source_key,source_designation,ra_deg,dec_deg,source_row_sha256
          FROM catwise_sources
          UNION ALL
          SELECT 'allwise','AllWISE Source Catalog',source_designation,source_designation,
                 ra_deg,dec_deg,source_row_sha256 FROM allwise_sources
        ) ORDER BY source_catalog,source_key
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE raw_matches AS
        SELECT 'catwise'::VARCHAR source_catalog,'CatWISE2020'::VARCHAR source_version,
               coalesce(nullif(r.source_id,''),nullif(r.source_name,'')) source_key,
               r.source_name source_designation,r.query_response_member,r.dist angular_sep_arcsec,
               r.w1mpro w1_mag,r.w2mpro w2_mag,NULL::DOUBLE w3_mag,NULL::DOUBLE w4_mag,
               r.w1snr w1_snr,r.w2snr w2_snr,NULL::DOUBLE w3_snr,NULL::DOUBLE w4_snr,
               r.pmra,r.pmdec,'arcsec/yr'::VARCHAR pm_unit,r.sigpmra pmra_error,r.sigpmdec pmdec_error,
               r.par_pm parallax_like_arcsec,r.par_pmsig parallax_like_error_arcsec,
               r.cc_flags,r.ab_flags,NULL::VARCHAR quality_flags,
               json_object('ab_flags',r.ab_flags,'n_aw',r.n_aw,'dist_cc',r.dist_cc)::VARCHAR blend_flags
        FROM typed_catwise r
        UNION ALL
        SELECT 'allwise','AllWISE Source Catalog',r.designation,r.designation,
               r.query_response_member,r.dist,r.w1mpro,r.w2mpro,r.w3mpro,r.w4mpro,
               r.w1snr,r.w2snr,r.w3snr,r.w4snr,r.pmra,r.pmdec,'mas/yr',r.sigpmra,r.sigpmdec,
               NULL,NULL,r.cc_flags,NULL,r.ph_qual,
               json_object('ext_flg',r.ext_flg,'nb',r.nb,'na',r.na)::VARCHAR
        FROM typed_allwise r
        """
    )
    accepted = float(policy["matching"]["accepted_separation_arcsec"])
    ambiguous = float(policy["matching"]["ambiguous_separation_arcsec"])
    con.execute(
        f"""
        CREATE TABLE infrared_source_matches AS
        WITH bound AS (
          SELECT r.*,q.target_index,q.star_id,q.system_id,q.stable_object_key,
                 row_number() OVER(PARTITION BY r.source_catalog,q.target_index
                   ORDER BY r.angular_sep_arcsec,r.source_key) match_rank,
                 row_number() OVER(PARTITION BY r.source_catalog,r.source_key
                   ORDER BY r.angular_sep_arcsec,q.stable_object_key) source_target_rank,
                 count(DISTINCT q.stable_object_key) OVER(PARTITION BY r.source_catalog,r.source_key)
                   source_target_count
          FROM raw_matches r JOIN wise_query_outcomes q
            ON q.catalog=r.source_catalog AND q.query_response_member=r.query_response_member
        ), classified AS (
          SELECT *,CASE
            WHEN match_rank=1 AND source_target_rank=1 AND angular_sep_arcsec<={accepted}
              THEN 'accepted_match'
            WHEN source_target_rank>1 AND angular_sep_arcsec<={ambiguous}
              THEN 'duplicate_source_collision'
            WHEN match_rank=1 AND angular_sep_arcsec<={ambiguous}
              THEN 'ambiguous_candidate'
            ELSE 'excluded_outside_acceptance'
          END conflict_status
          FROM bound
        )
        SELECT row_number() OVER(ORDER BY source_catalog,target_index,angular_sep_arcsec,source_key)::BIGINT
                 infrared_match_id,
               'star'::VARCHAR target_type,star_id target_id,system_id,stable_object_key,
               source_catalog,source_version,source_key,source_designation,query_response_member,
               angular_sep_arcsec,match_rank,
               greatest(0.0,1.0-angular_sep_arcsec/{ambiguous})::DOUBLE match_score,
               CASE WHEN conflict_status='accepted_match' THEN 'high'
                    WHEN conflict_status='ambiguous_candidate' THEN 'medium' ELSE 'low' END confidence_tier,
               'irsa_propagated_cone_unique_nearest_v2'::VARCHAR match_method,
               conflict_status,source_target_rank,source_target_count
        FROM classified ORDER BY source_catalog,target_index,angular_sep_arcsec,source_key
        """
    )
    con.execute(
        """
        CREATE TABLE infrared_photometry AS
        SELECT row_number() OVER(ORDER BY m.infrared_match_id)::BIGINT infrared_photometry_id,
               m.infrared_match_id,m.source_catalog,m.source_version,m.source_key,
               m.target_type,m.target_id,m.system_id,m.stable_object_key,m.conflict_status,
               r.w1_mag,r.w2_mag,r.w3_mag,r.w4_mag,r.w1_snr,r.w2_snr,r.w3_snr,r.w4_snr,
               r.quality_flags,r.cc_flags artifact_flags,r.blend_flags
        FROM infrared_source_matches m JOIN raw_matches r
          ON r.source_catalog=m.source_catalog AND r.source_key=m.source_key
         AND r.query_response_member=m.query_response_member
        ORDER BY m.infrared_match_id
        """
    )
    con.execute(
        """
        CREATE TABLE infrared_motion_evidence AS
        SELECT row_number() OVER(ORDER BY m.infrared_match_id)::BIGINT infrared_motion_id,
               m.infrared_match_id,m.source_catalog,m.source_version,m.source_key,
               m.target_type,m.target_id,m.system_id,m.stable_object_key,m.conflict_status,
               r.pmra,r.pmdec,r.pm_unit,r.pmra_error,r.pmdec_error,
               r.parallax_like_arcsec,r.parallax_like_error_arcsec,
               CASE WHEN m.source_catalog='catwise' AND r.parallax_like_arcsec IS NOT NULL
                 THEN 'candidate_evidence_not_distance_authority' ELSE NULL END parallax_like_note
        FROM infrared_source_matches m JOIN raw_matches r
          ON r.source_catalog=m.source_catalog AND r.source_key=m.source_key
         AND r.query_response_member=m.query_response_member
        ORDER BY m.infrared_match_id
        """
    )
    color_min = float(policy["matching"]["candidate_color_w1_minus_w2_min"])
    motion_min = float(policy["matching"]["candidate_motion_arcsec_yr_min"])
    snr_min = float(policy["matching"]["candidate_w2_snr_min"])
    con.execute(
        f"""
        CREATE TABLE infrared_candidate_queue AS
        WITH signals AS (
          SELECT m.*,p.w1_mag-p.w2_mag w1_minus_w2,p.w2_snr,p.artifact_flags,
                 sqrt(power(coalesce(e.pmra,0),2)+power(coalesce(e.pmdec,0),2)) /
                   CASE WHEN e.pm_unit='mas/yr' THEN 1000.0 ELSE 1.0 END pm_total_arcsec_yr
          FROM infrared_source_matches m
          JOIN infrared_photometry p USING(infrared_match_id)
          JOIN infrared_motion_evidence e USING(infrared_match_id)
        )
        SELECT row_number() OVER(ORDER BY source_catalog,source_key,stable_object_key)::BIGINT
                 infrared_candidate_id,
               'needs_review'::VARCHAR candidate_status,
               'nearby_ultracool_or_brown_dwarf'::VARCHAR candidate_kind,
               target_type nearest_target_type,target_id nearest_target_id,
               system_id nearest_system_id,stable_object_key nearest_stable_object_key,
               source_catalog,source_version,source_key,source_designation,angular_sep_arcsec,
               w1_minus_w2,pm_total_arcsec_yr,w2_snr,match_score candidate_score,
               'red_w1_w2_high_motion_wise_candidate_v2'::VARCHAR review_reason,
               conflict_status
        FROM signals
        WHERE w1_minus_w2>={color_min} AND pm_total_arcsec_yr>={motion_min}
          AND w2_snr>={snr_min}
          AND (artifact_flags IS NULL OR artifact_flags='' OR regexp_matches(artifact_flags,'^0+$'))
        ORDER BY source_catalog,source_key,stable_object_key
        """
    )
    con.execute(
        """
        CREATE TABLE wise_target_accounting AS
        SELECT q.catalog,q.target_index,q.star_id,q.system_id,q.stable_object_key,
               q.source_row_count,q.query_radius_arcsec,q.error_response_count,
               CASE WHEN count(m.infrared_match_id) FILTER(WHERE m.conflict_status='accepted_match')>0
                    THEN 'accepted'
                    WHEN q.source_row_count=0 THEN 'missing'
                    ELSE 'ambiguous'
               END outcome,
               cast(count(m.infrared_match_id) AS INTEGER) candidate_match_count,
               cast(count(m.infrared_match_id) FILTER(
                 WHERE m.conflict_status='accepted_match') AS INTEGER) accepted_match_count
        FROM wise_query_outcomes q LEFT JOIN infrared_source_matches m
          ON m.source_catalog=q.catalog AND m.query_response_member=q.query_response_member
        GROUP BY q.catalog,q.target_index,q.star_id,q.system_id,q.stable_object_key,
                 q.source_row_count,q.query_radius_arcsec,q.error_response_count
        ORDER BY q.catalog,q.target_index
        """
    )


def compile_wise(
    policy_path: Path,
    state: Path,
    output_root: Path,
    *,
    link_into_state: bool,
    trace_path: Path | None,
) -> dict[str, Any]:
    timing = Timings(trace_path)
    policy = load_object(policy_path)
    validate_policy(policy)
    target, sources = timing.run("verify_inputs", lambda: resolve_inputs(policy, state))
    policy_sha256 = file_hash(policy_path)
    compiler_sha256 = file_hash(Path(__file__).resolve())
    helper_sha256 = file_hash(ROOT / "scripts/compile_e7_clean_science.py")
    build_id = stable_hash(
        {
            "policy_sha256": policy_sha256,
            "compiler_sha256": compiler_sha256,
            "helper_sha256": helper_sha256,
            "targets_sha256": policy["target_set"]["targets_sha256"],
            "typed_content": {row["catalog"]: row["typed_content_sha256"] for row in sources},
            "query_manifests": {row["catalog"]: row["query_manifest_sha256"] for row in sources},
        }
    )[:24]
    final = output_root / build_id
    if (final / "manifest.json").exists():
        return load_object(final / "manifest.json")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    completed = False

    def cleanup() -> None:
        if not completed:
            shutil.rmtree(staging, ignore_errors=True)

    atexit.register(cleanup)
    db = staging / "clean_wise.duckdb"
    parquet_dir = staging / "parquet"
    parquet_dir.mkdir()
    con = duckdb.connect(str(db), config={"threads": "4", "memory_limit": "8GB"})
    try:
        timing.run("materialize_targets", lambda: insert_target_rows(con, target))
        timing.run("materialize_query_outcomes", lambda: insert_query_outcomes(con, sources))
        timing.run("materialize_evidence", lambda: create_evidence_tables(con, sources, policy))
        con.execute(
            """
            CREATE UNIQUE INDEX wise_sources_key_uq ON wise_sources(source_catalog,source_key);
            CREATE INDEX infrared_match_target_idx ON infrared_source_matches(target_id);
            CREATE INDEX infrared_match_source_idx ON infrared_source_matches(source_catalog,source_key);
            CREATE UNIQUE INDEX wise_target_accounting_uq ON wise_target_accounting(catalog,target_index)
            """
        )
        tables = [
            "targeted_stars", "wise_query_outcomes", "catwise_sources", "allwise_sources",
            "wise_sources", "infrared_source_matches", "infrared_photometry",
            "infrared_motion_evidence", "infrared_candidate_queue", "wise_target_accounting",
        ]
        counts = {table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]) for table in tables}
        checks = {
            "target_inventory_delta": counts["targeted_stars"] - int(policy["target_set"]["target_count"]),
            "query_outcome_delta": counts["wise_query_outcomes"] - 2 * int(policy["target_set"]["target_count"]),
            "target_accounting_delta": counts["wise_target_accounting"] - 2 * int(policy["target_set"]["target_count"]),
            "unbound_typed_rows": int(con.execute("SELECT count(*) FROM raw_matches r LEFT JOIN wise_query_outcomes q ON q.catalog=r.source_catalog AND q.query_response_member=r.query_response_member WHERE q.target_index IS NULL").fetchone()[0]),
            "accepted_non_primary": int(con.execute("SELECT count(*) FROM infrared_source_matches WHERE conflict_status='accepted_match' AND (match_rank<>1 OR source_target_rank<>1)").fetchone()[0]),
            "multiply_accepted_sources": int(con.execute("SELECT count(*) FROM (SELECT source_catalog,source_key FROM infrared_source_matches WHERE conflict_status='accepted_match' GROUP BY ALL HAVING count(*)>1)").fetchone()[0]),
            "candidate_parallax_promoted": int(con.execute("SELECT count(*) FROM infrared_motion_evidence WHERE parallax_like_arcsec IS NOT NULL AND parallax_like_note IS NULL AND source_catalog='catwise'").fetchone()[0]),
            "core_inventory_rows": 0,
            "stability_database_reads": 0,
        }
        if any(checks.values()):
            raise ValueError(f"clean WISE verification failed: {checks}")
        con.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
        con.executemany(
            "INSERT INTO build_metadata VALUES (?,?)",
            [
                ("build_id", build_id), ("build_kind", "e7_clean_wise"),
                ("policy_version", policy["policy_version"]),
                ("stability_database_opened", "0"), ("core_inventory_promoted", "0"),
            ],
        )
        timing.run("checkpoint", lambda: con.execute("CHECKPOINT"))

        def export() -> None:
            for table in tables:
                con.execute(
                    f"COPY (SELECT * FROM {table}) TO {sql_literal(parquet_dir / (table + '.parquet'))} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)"
                )

        timing.run("canonical_parquet_export", export)
    finally:
        con.close()

    def products() -> dict[str, Any]:
        result = {}
        for path in [db, *sorted(parquet_dir.glob("*.parquet"))]:
            result[str(path.relative_to(staging))] = {
                "bytes": path.stat().st_size,
                "sha256": file_hash(path),
                "determinism": "logical_tables" if path.suffix == ".duckdb" else "byte_exact",
            }
        return result

    product_rows = timing.run("hash_products", products)
    outcome_counts = {}
    with duckdb.connect(str(db), read_only=True) as verify:
        outcome_counts = {
            str(row[0]): int(row[1])
            for row in verify.execute("SELECT outcome,count(*) FROM wise_target_accounting GROUP BY 1 ORDER BY 1").fetchall()
        }
        match_counts = {
            str(row[0]): int(row[1])
            for row in verify.execute("SELECT conflict_status,count(*) FROM infrared_source_matches GROUP BY 1 ORDER BY 1").fetchall()
        }
    manifest = {
        "schema_version": "spacegate.e7_clean_wise_manifest.v1",
        "build_id": build_id,
        "status": "pass",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_version": policy["policy_version"],
        "compiler_version": policy["compiler_version"],
        "policy_sha256": policy_sha256,
        "compiler_sha256": compiler_sha256,
        "target_set_sha256": policy["target_set"]["targets_sha256"],
        "source_inputs": {
            row["catalog"]: {
                "source_id": row["source_id"], "release_id": row["release_id"],
                "typed_content_sha256": row["typed_content_sha256"],
                "query_manifest_sha256": row["query_manifest_sha256"],
            }
            for row in sources
        },
        "stability_databases_opened": [],
        "legacy_wise_csv_copied": False,
        "core_inventory_promoted": False,
        "counts": counts,
        "outcome_counts": outcome_counts,
        "match_status_counts": match_counts,
        "verification": checks,
        "products": product_rows,
        "timing": timing.report(),
    }
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(staging, final)
    if link_into_state:
        links = state / "derived/evidence_lake_v2/clean_wise"
        links.mkdir(parents=True, exist_ok=True)
        link = links / build_id
        if not link.exists() and not link.is_symlink():
            temporary = links / f".{build_id}.link"
            temporary.unlink(missing_ok=True)
            temporary.symlink_to(final)
            os.replace(temporary, link)
    completed = True
    atexit.unregister(cleanup)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-state-link", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_wise(
        args.policy.resolve(), args.state_dir.resolve(), args.output_root.resolve(),
        link_into_state=not args.no_state_link,
        trace_path=args.report.resolve() if args.report else None,
    )
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
