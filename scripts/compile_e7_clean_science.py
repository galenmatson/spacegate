#!/usr/bin/env python3
"""Compile clean selected-science and shared-consumer projections for E7."""

from __future__ import annotations

import argparse
import atexit
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
from typing import Any, Callable

import duckdb

import compile_e6_shadow_build as e6_helpers
from materialize_stellar_leaf_classifications import spectral_class_sql


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/evidence_lake/e7_clean_science.json"
DEFAULT_STATE = Path("/data/spacegate/state")
DEFAULT_FOUNDATION_ROOT = Path("/mnt/space/spacegate/e7-clean-foundation")
DEFAULT_OUTPUT_ROOT = Path("/mnt/space/spacegate/e7-clean-science")
VALID_CLASSES = (
    "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "WR", "WD",
    "NS", "PULSAR", "MAGNETAR", "BLACK HOLE", "UNKNOWN",
)


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


def sql_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value):
        raise ValueError(f"unsafe SQL identifier: {value}")
    return '"' + value + '"'


class Timings:
    def __init__(self, trace_path: Path | None = None) -> None:
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.phases: list[dict[str, Any]] = []
        self.trace_path = trace_path

    def write_trace(self, status: str, error: str | None = None) -> None:
        if self.trace_path is None:
            return
        payload = {"status": status, "timing": self.report()}
        if error is not None:
            payload["error"] = error
        self.trace_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.trace_path.with_name(f".{self.trace_path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, self.trace_path)

    def run(self, name: str, operation: Callable[[], Any]) -> Any:
        started = time.monotonic()
        cpu_started = time.process_time()
        before = resource.getrusage(resource.RUSAGE_SELF)
        status = "pass"
        error: str | None = None
        try:
            result = operation()
        except Exception as exception:
            status = "fail"
            error = f"{type(exception).__name__}: {exception}"
            raise
        finally:
            after = resource.getrusage(resource.RUSAGE_SELF)
            self.phases.append(
                {
                    "phase": name,
                    "wall_seconds": round(time.monotonic() - started, 6),
                    "cpu_seconds": round(time.process_time() - cpu_started, 6),
                    "peak_rss_kib_after": int(after.ru_maxrss),
                    "input_blocks_delta": int(after.ru_inblock - before.ru_inblock),
                    "output_blocks_delta": int(after.ru_oublock - before.ru_oublock),
                    "status": status,
                }
            )
            self.write_trace("failed" if status == "fail" else "in_progress", error)
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
    if policy.get("schema_version") != "spacegate.e7_clean_science_policy.v1":
        raise ValueError("unsupported E7 clean science policy")
    rules = policy.get("rules") or {}
    expected = {
        "open_stability_databases": False,
        "copy_stability_scientific_values": False,
        "every_selected_value_requires_fact_id": True,
        "allow_core_classification_fallback": False,
        "copy_all_domain_projection_tables": True,
    }
    if any(rules.get(key) is not value for key, value in expected.items()):
        raise ValueError("unsafe E7 clean science rules")
    families = [str(row.get("family") or "") for row in policy.get("selected_artifacts") or []]
    if len(families) != len(set(families)) or "selected_facts" not in families:
        raise ValueError("selected artifact families are missing or duplicated")
    star_projected: set[str] = set()
    for group in policy.get("star_projection_groups") or []:
        sql_identifier(str(group["table"]))
        for quantity in group["quantities"]:
            if quantity in star_projected:
                raise ValueError(f"duplicate projected quantity: {quantity}")
            star_projected.add(str(quantity))
    planet_projected: set[str] = set()
    for quantity in policy["planet_projection"]["quantities"]:
        if quantity in planet_projected:
            raise ValueError(f"duplicate projected quantity: {quantity}")
        planet_projected.add(str(quantity))
    categorical = set(policy["categorical_quantities"])
    boolean = set(policy["boolean_quantities"])
    if categorical & boolean or not (categorical | boolean).issubset(
        star_projected | planet_projected
    ):
        raise ValueError("invalid selected quantity type contract")
    wd = (policy.get("classification_evidence_sources") or {}).get(
        "white_dwarf_catalog_applicability"
    )
    expected_wd = {
        "source_id": "compact.gaia_edr3_white_dwarf",
        "quantity_key": "teff_k",
        "fact_status": "source_selected",
        "classification_value": "WD",
        "classification_status": "source",
        "evidence_basis": "selected_white_dwarf_catalog_applicability",
        "confidence_score": 0.95,
        "candidate_count": 164425,
        "selected_without_higher_direct_classification": 78157,
    }
    if wd != expected_wd:
        raise ValueError("invalid white-dwarf classification evidence contract")
    gaia_dsc = (policy.get("classification_evidence_sources") or {}).get(
        "gaia_dsc_white_dwarf_model"
    )
    expected_gaia_dsc = {
        "source_table": "evidence_stellar_model_selected_stellar_model_classifications",
        "classification_value": "WD",
        "classification_status": "source_model",
        "evidence_basis": "selected_gaia_dsc_white_dwarf_probability",
        "evidence_rank": 25,
        "candidate_count": 70028,
        "selected_without_higher_evidence": 4054,
    }
    if gaia_dsc != expected_gaia_dsc:
        raise ValueError("invalid Gaia DSC white-dwarf classification contract")


def table_names(con: duckdb.DuckDBPyConnection, alias: str) -> list[str]:
    return [
        str(row[0])
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema='main' AND table_type='BASE TABLE' ORDER BY 1",
            [alias],
        ).fetchall()
    ]


def copy_domain_tables(
    con: duckdb.DuckDBPyConnection,
    inputs: list[dict[str, Any]],
    compact_fact_tables: set[str],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in inputs:
        alias = str(item["alias"])
        family = str(item["family"])
        prefix = str(item["arm_prefix"])
        for source_table in table_names(con, alias):
            if family == "selected_facts" and source_table not in compact_fact_tables:
                continue
            output = f"evidence_{prefix}_{source_table}"
            con.execute(
                f"CREATE TABLE {sql_identifier(output)} AS SELECT * FROM "
                f"{sql_identifier(alias)}.{sql_identifier(source_table)}"
            )
            counts[output] = int(con.execute(f"SELECT count(*) FROM {sql_identifier(output)}").fetchone()[0])
    return counts


def materialize_selected_parameters(con: duckdb.DuckDBPyConnection) -> int:
    con.execute(
        """
        CREATE TABLE selected_stellar_parameters AS
        SELECT s.star_id,s.system_id,s.stable_object_key,
               p.teff_k,p.teff_k_lower,p.teff_k_upper,
               p.logg_cgs,p.logg_cgs_lower,p.logg_cgs_upper,
               p.metallicity_m_h,p.metallicity_m_h_lower,p.metallicity_m_h_upper,
               p.alpha_fe,p.radius_rsun,p.mass_msun,p.luminosity_lsun,
               p.luminosity_log10_lsun,p.density_g_cm3,p.age_gyr,
               coalesce(p.distance_geometric_pc,p.distance_photogeometric_pc,p.distance_gspphot_pc) distance_pc,
               p.rotation_period_days,p.projected_rotation_velocity_km_s,
               a.ra_deg,a.dec_deg,a.parallax_mas,a.pmra_mas_yr,a.pmdec_mas_yr,
               a.radial_velocity_km_s,a.gaia_ruwe,
               phot.gaia_g_mag,phot.gaia_bp_mag,phot.gaia_rp_mag,phot.gaia_bp_rp_mag,
               coalesce(c.spectral_type_optical,c.spectral_type_infrared,c.spectral_type_simbad) spectral_type_raw,
               p.teff_k_fact_id,p.mass_msun_fact_id,p.radius_rsun_fact_id,
               p.luminosity_lsun_fact_id,p.luminosity_log10_lsun_fact_id,
               a.ra_deg_fact_id,a.dec_deg_fact_id,a.parallax_mas_fact_id,
               c.spectral_type_optical_fact_id,c.spectral_type_infrared_fact_id,
               c.spectral_type_simbad_fact_id
        FROM core.stars s
        LEFT JOIN selected_stellar_physics p USING(star_id)
        LEFT JOIN selected_stellar_astrometry a USING(star_id)
        LEFT JOIN selected_stellar_photometry phot USING(star_id)
        LEFT JOIN selected_stellar_classification c USING(star_id)
        ORDER BY s.star_id
        """
    )
    return int(con.execute("SELECT count(*) FROM selected_stellar_parameters").fetchone()[0])


def materialize_display_classes(
    con: duckdb.DuckDBPyConnection,
    build_id: str,
    selected_alias: str,
    classification_sources: dict[str, Any],
) -> int:
    optical = spectral_class_sql("c.spectral_type_optical", "NULL", "'star'")
    infrared = spectral_class_sql("c.spectral_type_infrared", "NULL", "'star'")
    simbad = spectral_class_sql("c.spectral_type_simbad", "NULL", "'star'")
    selected_table = f"{sql_identifier(selected_alias)}.selected_facts"
    wd = classification_sources["white_dwarf_catalog_applicability"]
    gaia_dsc = classification_sources["gaia_dsc_white_dwarf_model"]
    gaia_dsc_table = sql_identifier(str(gaia_dsc["source_table"]))
    con.execute(
        f"""
        CREATE TABLE selected_stellar_display_classifications AS
        WITH candidates AS (
          SELECT s.star_id,s.system_id,s.stable_object_key,10 evidence_rank,
                 {optical}::VARCHAR classification_value,'source'::VARCHAR classification_status,
                 'selected_spectral_type_optical'::VARCHAR evidence_basis,
                 c.spectral_type_optical_fact_id selected_fact_id,
                 c.spectral_type_optical source_value,0.98::DOUBLE confidence_score
          FROM core.stars s JOIN selected_stellar_classification c USING(star_id)
          WHERE {optical} IS NOT NULL
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,11,{infrared},'source',
                 'selected_spectral_type_infrared',c.spectral_type_infrared_fact_id,
                 c.spectral_type_infrared,0.96
          FROM core.stars s JOIN selected_stellar_classification c USING(star_id)
          WHERE {infrared} IS NOT NULL
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,12,{simbad},'source',
                 'selected_spectral_type_simbad',c.spectral_type_simbad_fact_id,
                 c.spectral_type_simbad,0.94
          FROM core.stars s JOIN selected_stellar_classification c USING(star_id)
          WHERE {simbad} IS NOT NULL
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,20,
                 {sql_literal(wd['classification_value'])},
                 {sql_literal(wd['classification_status'])},
                 {sql_literal(wd['evidence_basis'])},f.selected_fact_id,
                 concat(f.source_id,':Pwd>0.75'),
                 {float(wd['confidence_score'])}::DOUBLE
          FROM core.stars s
          JOIN {selected_table} f
            ON f.object_type='star'
           AND f.stable_object_key=s.stable_object_key
           AND f.source_id={sql_literal(wd['source_id'])}
           AND f.quantity_key={sql_literal(wd['quantity_key'])}
           AND f.fact_status={sql_literal(wd['fact_status'])}
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,
                 {int(gaia_dsc['evidence_rank'])},
                 {sql_literal(gaia_dsc['classification_value'])},
                 {sql_literal(gaia_dsc['classification_status'])},
                 {sql_literal(gaia_dsc['evidence_basis'])},m.selected_fact_id,
                 m.source_value,m.confidence_score
          FROM core.stars s
          JOIN {gaia_dsc_table} m USING(star_id)
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,30,
                 CASE WHEN p.teff_k>=30000 THEN 'O' WHEN p.teff_k>=10000 THEN 'B'
                      WHEN p.teff_k>=7500 THEN 'A' WHEN p.teff_k>=6000 THEN 'F'
                      WHEN p.teff_k>=5200 THEN 'G' WHEN p.teff_k>=3700 THEN 'K'
                      WHEN p.teff_k>=2400 THEN 'M' WHEN p.teff_k IS NOT NULL THEN 'L' END,
                 'derived','selected_teff_visual_class_prior',p.teff_k_fact_id,
                 cast(p.teff_k AS VARCHAR),0.62
          FROM core.stars s JOIN selected_stellar_physics p USING(star_id)
          WHERE p.teff_k IS NOT NULL
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,35,
                 CASE WHEN phot.gaia_bp_rp_mag < -0.20 THEN 'O'
                      WHEN phot.gaia_bp_rp_mag < 0.00 THEN 'B'
                      WHEN phot.gaia_bp_rp_mag < 0.30 THEN 'A'
                      WHEN phot.gaia_bp_rp_mag < 0.58 THEN 'F'
                      WHEN phot.gaia_bp_rp_mag < 0.81 THEN 'G'
                      WHEN phot.gaia_bp_rp_mag < 1.40 THEN 'K'
                      WHEN phot.gaia_bp_rp_mag < 2.40 THEN 'M' ELSE 'L' END,
                 'assumed','selected_bp_rp_visual_class_prior',phot.gaia_bp_rp_mag_fact_id,
                 cast(phot.gaia_bp_rp_mag AS VARCHAR),0.40
          FROM core.stars s JOIN selected_stellar_photometry phot USING(star_id)
          WHERE phot.gaia_bp_rp_mag IS NOT NULL
          UNION ALL
          SELECT s.star_id,s.system_id,s.stable_object_key,40,
                 CASE WHEN p.mass_msun<0.08 THEN 'L' WHEN p.mass_msun<0.65 THEN 'M'
                      WHEN p.mass_msun<0.85 THEN 'K' WHEN p.mass_msun<1.04 THEN 'G'
                      WHEN p.mass_msun<1.40 THEN 'F' WHEN p.mass_msun<2.10 THEN 'A'
                      WHEN p.mass_msun<16.0 THEN 'B' ELSE 'O' END,
                 'assumed','selected_mass_main_sequence_prior',p.mass_msun_fact_id,
                 cast(p.mass_msun AS VARCHAR),0.35
          FROM core.stars s JOIN selected_stellar_physics p USING(star_id)
          WHERE p.mass_msun>0
        ), valid AS (
          SELECT * FROM candidates WHERE classification_value IN {VALID_CLASSES[:-1]}
            AND selected_fact_id IS NOT NULL
        ), ranked AS (
          SELECT *,row_number() OVER(PARTITION BY star_id ORDER BY evidence_rank,
            confidence_score DESC,selected_fact_id,classification_value) choice_rank
          FROM valid
        ), conflicts AS (
          SELECT star_id,count(DISTINCT classification_value)::INTEGER distinct_candidate_class_count,
                 to_json(list(DISTINCT classification_value ORDER BY classification_value))::VARCHAR candidate_classes_json
          FROM valid GROUP BY star_id
        ), direct_conflicts AS (
          SELECT star_id,count(DISTINCT classification_value)::INTEGER distinct_direct_class_count,
                 to_json(list(DISTINCT classification_value ORDER BY classification_value))::VARCHAR direct_classes_json
          FROM valid WHERE evidence_rank<30 GROUP BY star_id
        )
        SELECT row_number() OVER(ORDER BY s.star_id)::BIGINT selected_display_classification_id,
               {sql_literal(build_id)}::VARCHAR build_id,s.star_id,s.system_id,s.stable_object_key,
               coalesce(r.classification_value,'UNKNOWN')::VARCHAR classification_value,
               coalesce(r.classification_status,'missing')::VARCHAR classification_status,
               coalesce(r.evidence_basis,'no_selected_classification')::VARCHAR evidence_basis,
               r.selected_fact_id,r.source_value,coalesce(r.confidence_score,0.0) confidence_score,
               CASE WHEN r.selected_fact_id IS NOT NULL THEN 'selected_fact' ELSE 'missing' END::VARCHAR lineage_kind,
               r.selected_fact_id lineage_id,
               coalesce(cf.distinct_candidate_class_count,0) distinct_candidate_class_count,
               coalesce(cf.candidate_classes_json,'[]') candidate_classes_json,
               coalesce(dc.distinct_direct_class_count,0) distinct_direct_class_count,
               coalesce(dc.direct_classes_json,'[]') direct_classes_json,
               (coalesce(dc.distinct_direct_class_count,0)>1) has_classification_conflict,
               (coalesce(cf.distinct_candidate_class_count,0)>1) has_alternative_disagreement,
               'e7_selected_consumer_projection_v1'::VARCHAR projection_version
        FROM core.stars s
        LEFT JOIN ranked r ON r.star_id=s.star_id AND r.choice_rank=1
        LEFT JOIN conflicts cf ON cf.star_id=s.star_id
        LEFT JOIN direct_conflicts dc ON dc.star_id=s.star_id
        ORDER BY s.star_id
        """
    )
    return int(con.execute("SELECT count(*) FROM selected_stellar_display_classifications").fetchone()[0])


def compile_science(
    policy_path: Path, state_dir: Path, foundation_root: Path, output_root: Path,
    *, link_into_state: bool, trace_path: Path | None = None,
) -> dict[str, Any]:
    timing = Timings(trace_path)
    policy = load_object(policy_path)
    validate_policy(policy)
    foundation_dir = foundation_root / policy["foundation"]["build_id"]
    foundation_manifest_path = foundation_dir / "manifest.json"
    if file_sha256(foundation_manifest_path) != policy["foundation"]["manifest_sha256"]:
        raise ValueError("clean foundation manifest checksum mismatch")
    foundation_manifest = load_object(foundation_manifest_path)
    if foundation_manifest.get("status") != "pass" or foundation_manifest.get("stability_databases_opened") != []:
        raise ValueError("clean foundation is not accepted")
    inputs = timing.run("verify_selected_artifacts", lambda: e6_helpers.artifact_inputs(state_dir, policy))
    policy_sha = file_sha256(policy_path)
    compiler_sha = file_sha256(Path(__file__).resolve())
    helper_hashes = {
        "e6_projection_helpers": file_sha256(Path(e6_helpers.__file__).resolve()),
        "spectral_parser": file_sha256(ROOT / "scripts/materialize_stellar_leaf_classifications.py"),
    }
    build_id = stable_hash({
        "policy_sha256": policy_sha,
        "compiler_sha256": compiler_sha,
        "foundation_manifest_sha256": policy["foundation"]["manifest_sha256"],
        "helpers": helper_hashes,
        "selected_artifacts": {row["family"]: row["database_sha256"] for row in inputs},
    })[:24]
    final_dir = output_root / build_id
    if (final_dir / "manifest.json").is_file():
        return load_object(final_dir / "manifest.json")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    completed = False

    def cleanup_failed_staging() -> None:
        if not completed:
            shutil.rmtree(staging, ignore_errors=True)

    atexit.register(cleanup_failed_staging)
    db = staging / "clean_science.duckdb"
    parquet_dir = staging / "parquet"
    parquet_dir.mkdir()
    temp_dir = staging / "duckdb-tmp"
    temp_dir.mkdir()
    con = duckdb.connect(str(db), config={
        "threads": "16", "memory_limit": "48GB", "preserve_insertion_order": "true",
        "temp_directory": str(temp_dir),
    })
    try:
        con.execute(f"ATTACH {sql_literal(foundation_dir / 'clean_core_foundation.duckdb')} AS core (READ_ONLY)")
        for index, row in enumerate(inputs):
            alias = f"input_{index}_{row['family']}".replace("-", "_")
            row["alias"] = alias
            con.execute(f"ATTACH {sql_literal(row['database_path'])} AS {sql_identifier(alias)} (READ_ONLY)")
        selected = next(row for row in inputs if row["family"] == "selected_facts")
        selected_alias = str(selected["alias"])
        quantity_contract = timing.run(
            "selected_quantity_contract",
            lambda: e6_helpers.quantity_contract_check(con, policy, selected_alias),
        )
        copied_counts = timing.run(
            "copy_domain_projection_tables",
            lambda: copy_domain_tables(con, inputs, set(policy["selected_fact_compact_tables"])),
        )
        categorical = set(policy["categorical_quantities"])
        boolean = set(policy["boolean_quantities"])
        projection_counts: dict[str, int] = {}
        for group in policy["star_projection_groups"]:
            table = str(group["table"])
            projection_counts[table] = timing.run(
                f"materialize_{table}",
                lambda table=table, quantities=list(group["quantities"]): e6_helpers.create_wide_projection(
                    con, output_table=table, object_type="star", quantities=quantities,
                    categorical=categorical, boolean=boolean, selected_alias=selected_alias,
                ),
            )
        planet = policy["planet_projection"]
        projection_counts[str(planet["table"])] = timing.run(
            "materialize_selected_planet_parameters",
            lambda: e6_helpers.create_wide_projection(
                con, output_table=str(planet["table"]), object_type="planet",
                quantities=list(planet["quantities"]), categorical=categorical,
                boolean=boolean, selected_alias=selected_alias,
            ),
        )
        parameter_count = timing.run("materialize_selected_stellar_parameters", lambda: materialize_selected_parameters(con))
        display_count = timing.run(
            "materialize_selected_display_classifications",
            lambda: materialize_display_classes(
                con,
                build_id,
                selected_alias,
                policy["classification_evidence_sources"],
            ),
        )
        projection_counts["selected_stellar_parameters"] = parameter_count
        projection_counts["selected_stellar_display_classifications"] = display_count
        timing.run("shared_consumer_indexes", lambda: con.execute(
            """
            CREATE UNIQUE INDEX selected_stellar_parameters_star_uq ON selected_stellar_parameters(star_id);
            CREATE INDEX selected_stellar_parameters_system_idx ON selected_stellar_parameters(system_id);
            CREATE UNIQUE INDEX selected_display_star_uq ON selected_stellar_display_classifications(star_id);
            CREATE INDEX selected_display_system_idx ON selected_stellar_display_classifications(system_id)
            """
        ))
        con.execute("CREATE TABLE build_metadata(key VARCHAR,value VARCHAR)")
        con.executemany("INSERT INTO build_metadata VALUES (?,?)", [
            ("build_id", build_id), ("build_kind", "e7_clean_science"),
            ("foundation_build_id", policy["foundation"]["build_id"]),
            ("policy_version", policy["policy_version"]), ("stability_database_opened", "0"),
        ])
        timing.run("checkpoint", lambda: con.execute("CHECKPOINT"))
        export_tables = [
            *(str(group["table"]) for group in policy["star_projection_groups"]),
            str(planet["table"]), "selected_stellar_parameters",
            "selected_stellar_display_classifications",
        ]
        def export() -> None:
            for table in export_tables:
                con.execute(
                    f"COPY (SELECT * FROM {sql_identifier(table)}) TO {sql_literal(parquet_dir / (table + '.parquet'))} "
                    "(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 250000)"
                )
        timing.run("canonical_parquet_export", export)
        checks = {
            "stellar_parameter_inventory_delta": parameter_count - int(foundation_manifest["counts"]["stars"]),
            "display_classification_inventory_delta": display_count - int(foundation_manifest["counts"]["stars"]),
            "duplicate_stellar_parameters": int(con.execute("SELECT count(*) FROM (SELECT star_id FROM selected_stellar_parameters GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
            "duplicate_display_classifications": int(con.execute("SELECT count(*) FROM (SELECT star_id FROM selected_stellar_display_classifications GROUP BY 1 HAVING count(*)>1)").fetchone()[0]),
            "invalid_display_classes": int(con.execute(f"SELECT count(*) FROM selected_stellar_display_classifications WHERE classification_value NOT IN {VALID_CLASSES}").fetchone()[0]),
            "selected_display_without_fact": int(con.execute("SELECT count(*) FROM selected_stellar_display_classifications WHERE classification_status<>'missing' AND selected_fact_id IS NULL").fetchone()[0]),
            "stability_classification_basis": int(con.execute("SELECT count(*) FROM selected_stellar_display_classifications WHERE lower(evidence_basis) LIKE '%stability%' OR lower(evidence_basis) LIKE '%core%fallback%'").fetchone()[0]),
            "unknown_selected_planet_keys": int(con.execute("SELECT count(*) FROM selected_planet_parameters p LEFT JOIN core.planets c USING(planet_id) WHERE c.planet_id IS NULL").fetchone()[0]),
            "white_dwarf_catalog_candidate_delta": int(
                con.execute(
                    f"SELECT count(*) FROM {sql_identifier(selected_alias)}.selected_facts "
                    "WHERE object_type='star' AND source_id=? AND quantity_key=? AND fact_status=?",
                    [
                        policy["classification_evidence_sources"]["white_dwarf_catalog_applicability"]["source_id"],
                        policy["classification_evidence_sources"]["white_dwarf_catalog_applicability"]["quantity_key"],
                        policy["classification_evidence_sources"]["white_dwarf_catalog_applicability"]["fact_status"],
                    ],
                ).fetchone()[0]
            ) - int(policy["classification_evidence_sources"]["white_dwarf_catalog_applicability"]["candidate_count"]),
            "white_dwarf_catalog_selected_delta": int(
                con.execute(
                    "SELECT count(*) FROM selected_stellar_display_classifications WHERE evidence_basis=?",
                    [policy["classification_evidence_sources"]["white_dwarf_catalog_applicability"]["evidence_basis"]],
                ).fetchone()[0]
            ) - int(policy["classification_evidence_sources"]["white_dwarf_catalog_applicability"]["selected_without_higher_direct_classification"]),
            "gaia_dsc_white_dwarf_candidate_delta": int(
                con.execute(
                    "SELECT count(*) FROM evidence_stellar_model_selected_stellar_model_classifications"
                ).fetchone()[0]
            ) - int(policy["classification_evidence_sources"]["gaia_dsc_white_dwarf_model"]["candidate_count"]),
            "gaia_dsc_white_dwarf_selected_delta": int(
                con.execute(
                    "SELECT count(*) FROM selected_stellar_display_classifications WHERE evidence_basis=?",
                    [policy["classification_evidence_sources"]["gaia_dsc_white_dwarf_model"]["evidence_basis"]],
                ).fetchone()[0]
            ) - int(policy["classification_evidence_sources"]["gaia_dsc_white_dwarf_model"]["selected_without_higher_evidence"]),
        }
        if any(checks.values()):
            raise ValueError(f"clean science verification failed: {checks}")
    finally:
        con.close()
    def hash_products() -> dict[str, Any]:
        result: dict[str, Any] = {}
        for path in [db, *sorted(parquet_dir.glob("*.parquet"))]:
            relative = str(path.relative_to(staging))
            is_db = path.suffix == ".duckdb"
            result[relative] = {
                "bytes": path.stat().st_size, "sha256": file_sha256(path),
                "artifact_class": "regenerable_query_database" if is_db else "canonical_columnar_artifact",
                "determinism": "logical_tables" if is_db else "byte_exact",
            }
        return result

    products = timing.run("hash_products", hash_products)
    manifest = {
        "schema_version": "spacegate.e7_clean_science_manifest.v1",
        "build_id": build_id,"status": "pass",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_version": policy["policy_version"],"compiler_version": policy["compiler_version"],
        "policy_sha256": policy_sha,"compiler_sha256": compiler_sha,
        "foundation_build_id": policy["foundation"]["build_id"],
        "stability_databases_opened": [],"stability_scientific_values_copied": False,
        "selected_artifacts": {row["family"]: {"build_id":row["build_id"],"database_sha256":row["database_sha256"]} for row in inputs},
        "quantity_contract": quantity_contract,"projection_table_counts": {**copied_counts, **projection_counts},
        "verification": checks,"products": products,"timing": timing.report(),
    }
    (staging / "manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    shutil.rmtree(temp_dir, ignore_errors=True)
    os.replace(staging, final_dir)
    if link_into_state:
        link_root = state_dir / "derived/evidence_lake_v2/clean_science"
        link_root.mkdir(parents=True, exist_ok=True)
        link = link_root / build_id
        if not link.exists() and not link.is_symlink():
            temporary = link_root / f".{build_id}.link"
            temporary.unlink(missing_ok=True)
            temporary.symlink_to(final_dir)
            os.replace(temporary, link)
    completed = True
    atexit.unregister(cleanup_failed_staging)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy",type=Path,default=DEFAULT_POLICY)
    parser.add_argument("--state-dir",type=Path,default=DEFAULT_STATE)
    parser.add_argument("--foundation-root",type=Path,default=DEFAULT_FOUNDATION_ROOT)
    parser.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-state-link",action="store_true")
    parser.add_argument("--report",type=Path)
    args=parser.parse_args()
    report=compile_science(
        args.policy.resolve(),args.state_dir.resolve(),args.foundation_root.resolve(),
        args.output_root.resolve(),link_into_state=not args.no_state_link,
        trace_path=args.report.resolve() if args.report else None,
    )
    rendered=json.dumps(report,indent=2,sort_keys=True)+"\n"
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(rendered,encoding="utf-8")
    print(rendered,end="")


if __name__ == "__main__":
    main()
