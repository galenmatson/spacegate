#!/usr/bin/env python3
"""Build a deterministic SIMBAD target/delta seed from Gaia-envelope evidence."""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from compile_scientific_evidence import (
    DEFAULT_REGISTRY,
    DEFAULT_STATE,
    file_hash,
    load_json,
    source_input,
    stable_hash,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
POLICY_VERSION = "simbad_gaia_dr3_target_seed_v1"
CONTRACT = "spacegate.simbad_gaia_target_seed.v1"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def table_paths(input_row: dict[str, Any]) -> dict[str, Path]:
    return {
        str(table["source_name"]): input_row["typed_path"] / str(table["parquet_path"])
        for table in input_row["typed_manifest"]["tables"]
    }


def compile_seed_tables(
    *,
    gaia_tables: dict[str, Path],
    bridge_path: Path,
    basic_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    if not gaia_tables:
        raise ValueError("Gaia envelope has no typed source tables")
    for path in [*gaia_tables.values(), bridge_path, basic_path]:
        if not path.exists():
            raise FileNotFoundError(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    targets_path = output_dir / "target_seed.parquet"
    with duckdb.connect() as con:
        con.execute("set threads=4")
        envelope = " union all ".join(
            "select source_id, "
            + sql_string(table_name)
            + " envelope_product from read_parquet("
            + sql_string(str(path))
            + ")"
            for table_name, path in sorted(gaia_tables.items())
        )
        con.execute(f"create temp view envelope as {envelope}")
        con.execute(
            "create temp view simbad_bridge as select "
            "oidref simbad_oid, try_cast(substr(id, 10) as ubigint) gaia_dr3_source_id "
            f"from read_parquet({sql_string(str(bridge_path))}) "
            "where id like 'Gaia DR3 %'"
        )
        con.execute(
            "create temp view simbad_basic as select distinct oid simbad_oid "
            f"from read_parquet({sql_string(str(basic_path))})"
        )
        con.execute(
            f"""
            copy (
              select distinct
                b.gaia_dr3_source_id,
                b.simbad_oid,
                e.envelope_product,
                base.simbad_oid is not null base_basic_present
              from simbad_bridge b
              join envelope e on e.source_id=b.gaia_dr3_source_id
              left join simbad_basic base using (simbad_oid)
              order by b.simbad_oid, b.gaia_dr3_source_id, e.envelope_product
            ) to {sql_string(str(targets_path))}
            (format parquet, compression zstd, row_group_size 122880)
            """
        )
        counts = con.execute(
            f"""
            with targets as (select * from read_parquet({sql_string(str(targets_path))})),
            target_oids as (select distinct simbad_oid, base_basic_present from targets)
            select
              (select count(*) from envelope),
              (select count(*) from simbad_bridge),
              (select count(*) from simbad_basic),
              (select count(*) from targets),
              (select count(*) from target_oids),
              (select count(*) from target_oids where base_basic_present),
              (select count(*) from target_oids where not base_basic_present),
              (select count(*) from simbad_basic b anti join target_oids t using (simbad_oid)),
              (select count(*) from (
                 select simbad_oid from targets group by simbad_oid
                 having count(distinct gaia_dr3_source_id)>1
               ))
            """
        ).fetchone()
        missing_oids = [
            int(row[0])
            for row in con.execute(
                f"select distinct simbad_oid from read_parquet({sql_string(str(targets_path))}) "
                "where not base_basic_present order by simbad_oid"
            ).fetchall()
        ]
    keys = (
        "gaia_envelope_rows",
        "simbad_bridge_rows",
        "simbad_basic_rows",
        "target_bridge_rows",
        "target_object_oids",
        "target_present_basic",
        "target_missing_basic",
        "basic_outside_gaia_target",
        "target_oids_with_multiple_gaia_ids",
    )
    summary = dict(zip(keys, (int(value) for value in counts), strict=True))
    write_json(output_dir / "missing_oids.json", {"simbad_oids": missing_oids})
    return {
        "summary": summary,
        "missing_oids": missing_oids,
        "target_seed_bytes": targets_path.stat().st_size,
        "target_seed_sha256": file_hash(targets_path),
        "missing_oids_sha256": file_hash(output_dir / "missing_oids.json"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--require-gaia-table", action="append", default=[])
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    registry = load_json(args.registry)
    registry_sources = {str(row["source_id"]): row for row in registry["sources"]}
    gaia_input = source_input(args.state_dir, registry_sources["gaia.dr3.gaia_source"])
    simbad_input = source_input(args.state_dir, registry_sources["identity.simbad"])
    gaia_paths = {
        name: path
        for name, path in table_paths(gaia_input).items()
        if name.startswith("gaia_dr3_source_")
    }
    missing_required = sorted(set(args.require_gaia_table) - set(gaia_paths))
    if missing_required:
        raise ValueError(f"required Gaia envelope tables are absent: {missing_required}")
    simbad_paths = table_paths(simbad_input)
    bridge_name = "simbad_gaia_dr3_identity_bridge_v1"
    basic_name = "simbad_nearby_basic_v1"
    input_fingerprint = stable_hash(
        {
            "policy_version": POLICY_VERSION,
            "script_sha256": file_hash(Path(__file__).resolve()),
            "gaia_raw": gaia_input["raw_manifest"]["content_sha256"],
            "gaia_typed": gaia_input["typed_manifest"]["content_sha256"],
            "simbad_raw": simbad_input["raw_manifest"]["content_sha256"],
            "simbad_typed": simbad_input["typed_manifest"]["content_sha256"],
            "gaia_tables": sorted(gaia_paths),
        }
    )
    build_id = input_fingerprint[:24]
    root = (
        args.state_dir
        / "derived"
        / "evidence_lake_v2"
        / "acquisition_targets"
        / "simbad_gaia"
    )
    destination = root / build_id
    manifest_path = destination / "manifest.json"
    report_path = args.report or (
        args.state_dir
        / "reports"
        / "evidence_lake_v2"
        / "e3_simbad_gaia_target_seed.json"
    )
    if manifest_path.exists():
        manifest = load_json(manifest_path)
        for artifact in manifest["artifacts"]:
            path = destination / str(artifact["path"])
            if file_hash(path) != artifact["sha256"]:
                raise ValueError(f"immutable target-seed artifact changed: {path}")
        write_json(report_path, manifest["report"])
        print(f"SIMBAD Gaia target seed {build_id} cached")
        return 0

    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{build_id}.", dir=root))
    try:
        result = compile_seed_tables(
            gaia_tables=gaia_paths,
            bridge_path=simbad_paths[bridge_name],
            basic_path=simbad_paths[basic_name],
            output_dir=temporary,
        )
        coverage = (
            "complete"
            if any("uncertain_distance_supplement" in name for name in gaia_paths)
            else "hard_envelope_only"
        )
        report = {
            "schema_version": CONTRACT,
            "build_id": build_id,
            "policy_version": POLICY_VERSION,
            "input_fingerprint": input_fingerprint,
            "coverage": coverage,
            "gaia_tables": sorted(gaia_paths),
            **result,
        }
        artifacts = [
            {
                "path": name,
                "bytes": (temporary / name).stat().st_size,
                "sha256": file_hash(temporary / name),
            }
            for name in ("target_seed.parquet", "missing_oids.json")
        ]
        write_json(
            temporary / "manifest.json",
            {
                "schema_version": CONTRACT,
                "build_id": build_id,
                "input_fingerprint": input_fingerprint,
                "artifacts": artifacts,
                "report": report,
            },
        )
        os.replace(temporary, destination)
        write_json(report_path, report)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    print(
        f"SIMBAD Gaia target seed {build_id} {coverage}: "
        f"missing={result['summary']['target_missing_basic']:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
