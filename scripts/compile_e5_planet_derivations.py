#!/usr/bin/env python3
"""Compile the independently regenerable E5 planet-derivation shard."""

from __future__ import annotations

import argparse
import atexit
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
DEFAULT_POLICY = ROOT / "config/evidence_lake/e5_planet_derivations.json"
DEFAULT_OUTPUT_ROOT = Path("/data/spacegate/state/derived/evidence_lake_v2/selected_planet_derivations")


def load_json(path: Path) -> dict[str, Any]:
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


class Timings:
    def __init__(self) -> None:
        self.started = time.monotonic()
        self.cpu_started = time.process_time()
        self.phases: list[dict[str, Any]] = []

    def run(self, phase: str, operation: Callable[[], Any]) -> Any:
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
                "phase": phase,
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


def validate_policy(policy_path: Path, policy: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if policy.get("schema_version") != "spacegate.e5_planet_derivation_policy.v1":
        raise ValueError("unsupported E5 planet-derivation policy schema")
    selection_path = ROOT / str(policy["selection_policy"]["path"])
    selection = load_json(selection_path)
    if selection.get("policy_version") != policy["selection_policy"]["policy_version"]:
        raise ValueError("selected-fact policy version mismatch")
    derivations = {
        str(row["derivation_key"]): row for row in selection.get("derivations") or []
    }
    expected = list(policy["derivation_keys"])
    if len(expected) != len(set(expected)):
        raise ValueError("duplicate derivation key")
    missing = sorted(set(expected) - set(derivations))
    if missing:
        raise ValueError(f"selection policy is missing derivations: {missing}")
    for name in ("selected_facts", "foundation"):
        item = policy[name]
        artifact = Path(item["path"])
        manifest = artifact / "manifest.json"
        database = artifact / str(item["database"])
        if not manifest.is_file() or not database.is_file():
            raise ValueError(f"missing pinned {name} artifact")
        if file_sha256(manifest) != item["manifest_sha256"]:
            raise ValueError(f"{name} manifest checksum mismatch")
        declared = load_json(manifest)
        if declared.get("build_id") != item["build_id"]:
            raise ValueError(f"{name} build identity mismatch")
        if name == "selected_facts":
            entry = ((declared.get("report") or {}).get("files") or {}).get(item["database"]) or {}
            if (declared.get("report") or {}).get("status") != "pass":
                raise ValueError("selected facts are not accepted")
        else:
            entry = (declared.get("products") or {}).get(item["database"]) or {}
            if declared.get("status") != "pass":
                raise ValueError("foundation is not accepted")
        if entry.get("sha256") != item["database_sha256"]:
            raise ValueError(f"{name} database checksum declaration mismatch")
    return selection, {key: derivations[key] for key in expected}


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE selected_facts AS
        SELECT * FROM base.selected_facts WHERE false;
        CREATE TABLE selected_fact_derivations AS
        SELECT * FROM base.selected_fact_derivations WHERE false;
        """
    )


def compile_semimajor_axes(
    con: duckdb.DuckDBPyConnection,
    derivation: dict[str, Any],
    policy: dict[str, Any],
) -> int:
    constants = policy["constants"]
    version = str(derivation["version"])
    policy_version = str(policy["policy_version"])
    earth_per_sun = float(constants["earth_masses_per_solar_mass"])
    jup_per_sun = float(constants["jupiter_masses_per_solar_mass"])
    days_per_year = float(constants["days_per_julian_year"])
    con.execute(
        f"""
        CREATE TEMP TABLE semimajor_candidates AS
        WITH planet_mass_candidates AS (
          SELECT stable_object_key, selected_fact_id, normalized_value,
                 value_lower, value_upper, quantity_key,
                 row_number() OVER (
                   PARTITION BY stable_object_key
                   ORDER BY CASE quantity_key
                     WHEN 'best_mass_earth' THEN 1 WHEN 'mass_earth' THEN 2
                     WHEN 'best_mass_jup' THEN 3 WHEN 'mass_jup' THEN 4
                     WHEN 'minimum_mass_earth' THEN 5 WHEN 'minimum_mass_jup' THEN 6
                     ELSE 99 END
                 ) AS preference
          FROM base.selected_facts
          WHERE object_type='planet'
            AND quantity_key IN (
              'best_mass_earth','mass_earth','best_mass_jup','mass_jup',
              'minimum_mass_earth','minimum_mass_jup'
            )
            AND normalized_value > 0
        ), planet_mass AS (
          SELECT stable_object_key, selected_fact_id,
                 CASE WHEN quantity_key LIKE '%_jup'
                   THEN normalized_value/{jup_per_sun}
                   ELSE normalized_value/{earth_per_sun} END AS mass_msun,
                 CASE WHEN value_lower > 0 THEN
                   CASE WHEN quantity_key LIKE '%_jup'
                     THEN value_lower/{jup_per_sun} ELSE value_lower/{earth_per_sun} END
                 END AS mass_lower_msun,
                 CASE WHEN value_upper > 0 THEN
                   CASE WHEN quantity_key LIKE '%_jup'
                     THEN value_upper/{jup_per_sun} ELSE value_upper/{earth_per_sun} END
                 END AS mass_upper_msun
          FROM planet_mass_candidates WHERE preference=1
        ), candidates AS (
          SELECT p.stable_object_key, per.system_stable_object_key,
                 per.selected_fact_id AS period_fact_id,
                 hm.selected_fact_id AS host_mass_fact_id,
                 pm.selected_fact_id AS planet_mass_fact_id,
                 pow(
                   (hm.normalized_value + coalesce(pm.mass_msun,0.0))
                   * pow(per.normalized_value/{days_per_year},2.0),
                   1.0/3.0
                 ) AS value,
                 CASE WHEN hm.value_lower>0 AND per.value_lower>0 THEN
                   pow(
                     (hm.value_lower + coalesce(pm.mass_lower_msun,pm.mass_msun,0.0))
                     * pow(per.value_lower/{days_per_year},2.0),
                     1.0/3.0
                   ) END AS value_lower,
                 CASE WHEN hm.value_upper>0 AND per.value_upper>0 THEN
                   pow(
                     (hm.value_upper + coalesce(pm.mass_upper_msun,pm.mass_msun,0.0))
                     * pow(per.value_upper/{days_per_year},2.0),
                     1.0/3.0
                   ) END AS value_upper
          FROM core.planets p
          JOIN core.stars host ON host.star_id=p.star_id
          JOIN base.selected_facts per
            ON per.object_type='planet'
           AND per.stable_object_key=p.stable_object_key
           AND per.quantity_key='orbital_period_days'
          JOIN base.selected_facts hm
            ON hm.object_type='star'
           AND hm.stable_object_key=host.stable_object_key
           AND hm.quantity_key='mass_msun'
          LEFT JOIN planet_mass pm ON pm.stable_object_key=p.stable_object_key
          LEFT JOIN base.selected_facts direct
            ON direct.object_type='planet'
           AND direct.stable_object_key=p.stable_object_key
           AND direct.quantity_key='semi_major_axis_au'
          WHERE direct.selected_fact_id IS NULL
            AND per.normalized_value>0 AND hm.normalized_value>0
        )
        SELECT sha256(concat_ws('|',stable_object_key,'{derivation["derivation_key"]}','{version}')) derivation_id,
               sha256(concat_ws('|','planet',stable_object_key,'semi_major_axis_au',
                                '{derivation["derivation_key"]}','{version}','{policy_version}')) selected_fact_id,
               *
        FROM candidates;

        INSERT INTO selected_facts
        SELECT selected_fact_id,'planet',stable_object_key,system_stable_object_key,
               'planet_orbit','semi_major_axis_au',cast(value AS VARCHAR),value,'au',
               value_lower,value_upper,'propagated_selected_interval_endpoints','derived',
               NULL,NULL,NULL,NULL,NULL,'spacegate.derivation','{version}',
               '{derivation["derivation_key"]}',NULL,NULL,NULL,NULL,NULL,'{policy_version}',
               '{version}',json_object('days_per_julian_year',{days_per_year},
                 'earth_masses_per_solar_mass',{earth_per_sun},
                 'jupiter_masses_per_solar_mass',{jup_per_sun}),NULL
        FROM semimajor_candidates;

        INSERT INTO selected_fact_derivations
        SELECT derivation_id,selected_fact_id,stable_object_key,'semi_major_axis_au',
               '{derivation["derivation_key"]}','{version}',
               to_json(list_filter([period_fact_id,host_mass_fact_id,planet_mass_fact_id],x -> x IS NOT NULL)),
               ?,?,json_object('days_per_julian_year',{days_per_year},
                 'earth_masses_per_solar_mass',{earth_per_sun},
                 'jupiter_masses_per_solar_mass',{jup_per_sun}),
               ?,'medium',to_json(?::VARCHAR[]),'{policy_version}'
        FROM semimajor_candidates
        """,
        [
            derivation["applicability"], derivation["formula"], derivation["uncertainty"],
            list(derivation["supersedes"]),
        ],
    )
    return int(con.execute("SELECT count(*) FROM semimajor_candidates").fetchone()[0])


def compile_insolation(
    con: duckdb.DuckDBPyConnection,
    derivation: dict[str, Any],
    policy: dict[str, Any],
) -> int:
    version = str(derivation["version"])
    policy_version = str(policy["policy_version"])
    con.execute(
        f"""
        CREATE TEMP VIEW all_selected_facts AS
        SELECT * FROM base.selected_facts UNION ALL SELECT * FROM selected_facts;
        CREATE TEMP TABLE insolation_candidates AS
        WITH candidates AS (
          SELECT p.stable_object_key, sma.system_stable_object_key,
                 lum.selected_fact_id AS luminosity_fact_id,
                 sma.selected_fact_id AS semimajor_fact_id,
                 lum.normalized_value/pow(sma.normalized_value,2.0) AS value,
                 CASE WHEN lum.value_lower>0 AND sma.value_upper>0
                   THEN lum.value_lower/pow(sma.value_upper,2.0) END AS value_lower,
                 CASE WHEN lum.value_upper>0 AND sma.value_lower>0
                   THEN lum.value_upper/pow(sma.value_lower,2.0) END AS value_upper
          FROM core.planets p
          JOIN core.stars host ON host.star_id=p.star_id
          JOIN all_selected_facts sma
            ON sma.object_type='planet'
           AND sma.stable_object_key=p.stable_object_key
           AND sma.quantity_key='semi_major_axis_au'
          JOIN base.selected_facts lum
            ON lum.object_type='star'
           AND lum.stable_object_key=host.stable_object_key
           AND lum.quantity_key='luminosity_lsun'
          LEFT JOIN base.selected_facts direct
            ON direct.object_type='planet'
           AND direct.stable_object_key=p.stable_object_key
           AND direct.quantity_key='insol_earth'
          WHERE direct.selected_fact_id IS NULL
            AND sma.normalized_value>0 AND lum.normalized_value>0
        )
        SELECT sha256(concat_ws('|',stable_object_key,'{derivation["derivation_key"]}','{version}')) derivation_id,
               sha256(concat_ws('|','planet',stable_object_key,'insol_earth',
                                '{derivation["derivation_key"]}','{version}','{policy_version}')) selected_fact_id,
               * FROM candidates;

        INSERT INTO selected_facts
        SELECT selected_fact_id,'planet',stable_object_key,system_stable_object_key,
               'planet_environment','insol_earth',cast(value AS VARCHAR),value,'Searth',
               value_lower,value_upper,'propagated_selected_interval_endpoints','derived',
               NULL,NULL,NULL,NULL,NULL,'spacegate.derivation','{version}',
               '{derivation["derivation_key"]}',NULL,NULL,NULL,NULL,NULL,'{policy_version}',
               '{version}',json_object('geometry','inverse_square'),NULL
        FROM insolation_candidates;

        INSERT INTO selected_fact_derivations
        SELECT derivation_id,selected_fact_id,stable_object_key,'insol_earth',
               '{derivation["derivation_key"]}','{version}',
               to_json([luminosity_fact_id,semimajor_fact_id]),?,?,json_object('geometry','inverse_square'),
               ?,'medium',to_json(?::VARCHAR[]),'{policy_version}'
        FROM insolation_candidates
        """,
        [
            derivation["applicability"], derivation["formula"], derivation["uncertainty"],
            list(derivation["supersedes"]),
        ],
    )
    return int(con.execute("SELECT count(*) FROM insolation_candidates").fetchone()[0])


def compile_equilibrium_temperature(
    con: duckdb.DuckDBPyConnection,
    derivation: dict[str, Any],
    policy: dict[str, Any],
) -> int:
    constants = policy["constants"]
    version = str(derivation["version"])
    policy_version = str(policy["policy_version"])
    earth_temperature = float(constants["earth_equilibrium_temperature_k"])
    albedo = float(constants["bond_albedo"])
    redistribution = str(constants["heat_redistribution"])
    con.execute(
        f"""
        CREATE TEMP TABLE equilibrium_candidates AS
        WITH candidates AS (
          SELECT p.stable_object_key, insol.system_stable_object_key,
                 insol.selected_fact_id AS insolation_fact_id,
                 {earth_temperature}*pow(insol.normalized_value,0.25) AS value,
                 CASE WHEN insol.value_lower>0
                   THEN {earth_temperature}*pow(insol.value_lower,0.25) END AS value_lower,
                 CASE WHEN insol.value_upper>0
                   THEN {earth_temperature}*pow(insol.value_upper,0.25) END AS value_upper
          FROM core.planets p
          JOIN all_selected_facts insol
            ON insol.object_type='planet'
           AND insol.stable_object_key=p.stable_object_key
           AND insol.quantity_key='insol_earth'
          LEFT JOIN base.selected_facts direct
            ON direct.object_type='planet'
           AND direct.stable_object_key=p.stable_object_key
           AND direct.quantity_key='eq_temp_k'
          WHERE direct.selected_fact_id IS NULL AND insol.normalized_value>0
        )
        SELECT sha256(concat_ws('|',stable_object_key,'{derivation["derivation_key"]}','{version}')) derivation_id,
               sha256(concat_ws('|','planet',stable_object_key,'eq_temp_k',
                                '{derivation["derivation_key"]}','{version}','{policy_version}')) selected_fact_id,
               * FROM candidates;

        INSERT INTO selected_facts
        SELECT selected_fact_id,'planet',stable_object_key,system_stable_object_key,
               'planet_environment','eq_temp_k',cast(value AS VARCHAR),value,'K',
               value_lower,value_upper,'propagated_selected_interval_endpoints','derived',
               NULL,NULL,NULL,NULL,NULL,'spacegate.derivation','{version}',
               '{derivation["derivation_key"]}',NULL,NULL,NULL,NULL,NULL,'{policy_version}',
               '{version}',json_object('earth_equilibrium_temperature_k',{earth_temperature},
                 'bond_albedo',{albedo},'heat_redistribution','{redistribution}'),NULL
        FROM equilibrium_candidates;

        INSERT INTO selected_fact_derivations
        SELECT derivation_id,selected_fact_id,stable_object_key,'eq_temp_k',
               '{derivation["derivation_key"]}','{version}',to_json([insolation_fact_id]),
               ?,?,json_object('earth_equilibrium_temperature_k',{earth_temperature},
                 'bond_albedo',{albedo},'heat_redistribution','{redistribution}'),
               ?,'medium',to_json(?::VARCHAR[]),'{policy_version}'
        FROM equilibrium_candidates
        """,
        [
            derivation["applicability"], derivation["formula"], derivation["uncertainty"],
            list(derivation["supersedes"]),
        ],
    )
    return int(con.execute("SELECT count(*) FROM equilibrium_candidates").fetchone()[0])


def checks(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        "duplicate_object_quantities": int(con.execute(
            "SELECT count(*) FROM (SELECT stable_object_key,quantity_key FROM selected_facts GROUP BY 1,2 HAVING count(*)>1)"
        ).fetchone()[0]),
        "derived_overrides_direct": int(con.execute(
            """
            SELECT count(*) FROM selected_facts d JOIN base.selected_facts b
              USING(object_type,stable_object_key,quantity_key)
            """
        ).fetchone()[0]),
        "derived_without_lineage": int(con.execute(
            """
            SELECT count(*) FROM selected_facts f LEFT JOIN selected_fact_derivations d
              ON d.output_selected_fact_id=f.selected_fact_id
            WHERE d.derivation_id IS NULL
            """
        ).fetchone()[0]),
        "lineage_without_fact": int(con.execute(
            """
            SELECT count(*) FROM selected_fact_derivations d LEFT JOIN selected_facts f
              ON f.selected_fact_id=d.output_selected_fact_id
            WHERE f.selected_fact_id IS NULL
            """
        ).fetchone()[0]),
        "invalid_values": int(con.execute(
            "SELECT count(*) FROM selected_facts WHERE normalized_value IS NULL OR NOT isfinite(normalized_value) OR normalized_value<=0"
        ).fetchone()[0]),
        "unresolved_semimajor_applicable": int(con.execute(
            """
            SELECT count(*) FROM core.planets p
            JOIN core.stars host ON host.star_id=p.star_id
            JOIN base.selected_facts per ON per.object_type='planet' AND per.stable_object_key=p.stable_object_key
              AND per.quantity_key='orbital_period_days' AND per.normalized_value>0
            JOIN base.selected_facts hm ON hm.object_type='star' AND hm.stable_object_key=host.stable_object_key
              AND hm.quantity_key='mass_msun' AND hm.normalized_value>0
            LEFT JOIN all_selected_facts result ON result.object_type='planet'
              AND result.stable_object_key=p.stable_object_key AND result.quantity_key='semi_major_axis_au'
            WHERE result.selected_fact_id IS NULL
            """
        ).fetchone()[0]),
        "unresolved_insolation_applicable": int(con.execute(
            """
            SELECT count(*) FROM core.planets p
            JOIN core.stars host ON host.star_id=p.star_id
            JOIN all_selected_facts sma ON sma.object_type='planet' AND sma.stable_object_key=p.stable_object_key
              AND sma.quantity_key='semi_major_axis_au' AND sma.normalized_value>0
            JOIN base.selected_facts lum ON lum.object_type='star' AND lum.stable_object_key=host.stable_object_key
              AND lum.quantity_key='luminosity_lsun' AND lum.normalized_value>0
            LEFT JOIN all_selected_facts result ON result.object_type='planet'
              AND result.stable_object_key=p.stable_object_key AND result.quantity_key='insol_earth'
            WHERE result.selected_fact_id IS NULL
            """
        ).fetchone()[0]),
        "unresolved_equilibrium_applicable": int(con.execute(
            """
            SELECT count(*) FROM core.planets p
            JOIN all_selected_facts insol ON insol.object_type='planet' AND insol.stable_object_key=p.stable_object_key
              AND insol.quantity_key='insol_earth' AND insol.normalized_value>0
            LEFT JOIN all_selected_facts result ON result.object_type='planet'
              AND result.stable_object_key=p.stable_object_key AND result.quantity_key='eq_temp_k'
            WHERE result.selected_fact_id IS NULL
            """
        ).fetchone()[0]),
    }


def compile_shard(policy_path: Path, output_root: Path) -> dict[str, Any]:
    timing = Timings()
    policy = load_json(policy_path)
    selection, derivations = timing.run("validate_pinned_inputs", lambda: validate_policy(policy_path, policy))
    compiler_sha = file_sha256(Path(__file__).resolve())
    policy_sha = file_sha256(policy_path)
    selection_sha = file_sha256(ROOT / str(policy["selection_policy"]["path"]))
    build_id = stable_hash({
        "compiler_sha256": compiler_sha,
        "policy_sha256": policy_sha,
        "selection_policy_sha256": selection_sha,
        "selected_facts_database_sha256": policy["selected_facts"]["database_sha256"],
        "foundation_database_sha256": policy["foundation"]["database_sha256"],
    })[:24]
    final = output_root / build_id
    if (final / "manifest.json").is_file():
        return load_json(final / "manifest.json")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=output_root))
    complete = False

    def cleanup() -> None:
        if not complete:
            shutil.rmtree(staging, ignore_errors=True)

    atexit.register(cleanup)
    database = staging / "selected_planet_derivations.duckdb"
    parquet_facts = staging / "selected_planet_derivations.parquet"
    parquet_lineage = staging / "selected_planet_derivation_lineage.parquet"
    con = duckdb.connect(str(database), config={"threads": "4", "memory_limit": "4GB"})
    try:
        con.execute(f"ATTACH {sql_literal(Path(policy['selected_facts']['path']) / policy['selected_facts']['database'])} AS base (READ_ONLY)")
        con.execute(f"ATTACH {sql_literal(Path(policy['foundation']['path']) / policy['foundation']['database'])} AS core (READ_ONLY)")
        timing.run("create_schema", lambda: create_schema(con))
        counts = {
            "planet_semimajor_axis_kepler": timing.run(
                "derive_planet_semimajor_axis",
                lambda: compile_semimajor_axes(con, derivations["planet_semimajor_axis_kepler"], policy),
            ),
            "planet_insolation": timing.run(
                "derive_planet_insolation",
                lambda: compile_insolation(con, derivations["planet_insolation"], policy),
            ),
            "planet_equilibrium_temperature": timing.run(
                "derive_planet_equilibrium_temperature",
                lambda: compile_equilibrium_temperature(
                    con, derivations["planet_equilibrium_temperature"], policy
                ),
            ),
        }
        verification = timing.run("verify_applicability_and_lineage", lambda: checks(con))
        if any(verification.values()):
            raise ValueError(f"planet derivation verification failed: {verification}")
        timing.run("canonical_parquet_export", lambda: con.execute(
            f"""
            COPY (SELECT * FROM selected_facts ORDER BY stable_object_key,quantity_key)
              TO {sql_literal(parquet_facts)} (FORMAT PARQUET,COMPRESSION ZSTD);
            COPY (SELECT * FROM selected_fact_derivations ORDER BY stable_object_key,quantity_key)
              TO {sql_literal(parquet_lineage)} (FORMAT PARQUET,COMPRESSION ZSTD);
            CHECKPOINT;
            """
        ))
    finally:
        con.close()
    products = {}
    for path in (database, parquet_facts, parquet_lineage):
        products[path.name] = {
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
            "determinism": "byte_exact" if path.suffix == ".parquet" else "logical_tables",
        }
    manifest = {
        "schema_version": "spacegate.e5_planet_derivation_manifest.v1",
        "build_id": build_id,
        "compiler_version": policy["compiler_version"],
        "compiler_sha256": compiler_sha,
        "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha,
        "selection_policy_version": selection["policy_version"],
        "selection_policy_sha256": selection_sha,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inputs": {
            "selected_facts_build_id": policy["selected_facts"]["build_id"],
            "selected_facts_database_sha256": policy["selected_facts"]["database_sha256"],
            "foundation_build_id": policy["foundation"]["build_id"],
            "foundation_database_sha256": policy["foundation"]["database_sha256"],
            "verification": "accepted immutable manifests and declared database checksums",
        },
        "counts": counts,
        "total_derived_facts": sum(counts.values()),
        "verification": verification,
        "products": products,
        "timing": timing.report(),
        "status": "pass",
    }
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(staging, final)
    complete = True
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    manifest = compile_shard(args.policy.resolve(), args.output_root.resolve())
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
