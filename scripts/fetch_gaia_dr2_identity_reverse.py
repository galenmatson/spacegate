#!/usr/bin/env python3
"""Collect reverse Gaia DR3-to-DR2 neighbourhood rows for merge accounting."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import duckdb

from fetch_gaia_dr2_identity import (
    EXPECTED_FIELDS,
    GAIA_DOC_URL,
    GAIA_TAP_URL,
    atomic_write_json,
    chunks,
    manifest_entry,
    sha256_file,
    stable_hash,
    tap_query,
    tree_fingerprint,
    utc_now,
    validate_tap_csv,
)


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_reverse_target_set(forward_chunks: Path, output: Path) -> dict[str, Any]:
    csv_glob = forward_chunks / "*.csv"
    if not list(forward_chunks.glob("*.csv")):
        raise FileNotFoundError(f"forward neighborhood has no CSV chunks: {forward_chunks}")
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("set threads=1")
    con.execute("set preserve_insertion_order=true")
    try:
        con.execute(
            f"""
            copy (
              select dr3_source_id,
                     count(distinct dr2_source_id)::integer forward_dr2_source_count,
                     count(*)::bigint forward_pair_count
              from read_csv_auto({sql_literal(csv_glob)}, union_by_name=true)
              where dr3_source_id is not null
              group by dr3_source_id order by dr3_source_id
            ) to {sql_literal(output)} (format csv, header true, delimiter ',')
            """
        )
        target_count = int(
            con.execute(
                f"""
                select count(distinct dr3_source_id)
                from read_csv_auto({sql_literal(csv_glob)}, union_by_name=true)
                where dr3_source_id is not null
                """
            ).fetchone()[0]
        )
        pair_count = int(
            con.execute(
                f"select count(*) from read_csv_auto({sql_literal(csv_glob)}, union_by_name=true)"
            ).fetchone()[0]
        )
    finally:
        con.close()
    forward_hash, forward_bytes = tree_fingerprint(forward_chunks)
    return {
        "target_count": target_count,
        "forward_pair_count": pair_count,
        "forward_chunks": str(forward_chunks),
        "forward_tree_sha256": forward_hash,
        "forward_bytes": forward_bytes,
        "target_set_sha256": sha256_file(output),
    }


def reverse_target_ids(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        values = [str(int(row["dr3_source_id"])) for row in csv.DictReader(handle)]
    if values != sorted(set(values), key=int):
        raise ValueError("Gaia DR3 reverse target set is not unique and numerically ordered")
    return values


def default_forward_chunks(state_dir: Path) -> Path:
    manifest_path = state_dir / "reports" / "manifests" / "gaia_dr2_identity_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"forward Gaia identity manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    matches = [
        entry
        for entry in payload
        if entry.get("source_name") == "gaia_dr2_neighbourhood_union"
    ]
    if len(matches) != 1:
        raise ValueError(
            "forward Gaia identity manifest must contain exactly one "
            "gaia_dr2_neighbourhood_union entry"
        )
    path = Path(str(matches[0].get("dest_path") or ""))
    resolved = path if path.is_absolute() else state_dir / path
    if not resolved.is_dir():
        raise FileNotFoundError(f"forward Gaia neighborhood directory not found: {resolved}")
    return resolved


def fetch_reverse_chunk(
    index: int,
    ids: list[str],
    output_dir: Path,
    *,
    timeout_s: int,
    retries: int,
    max_records: int,
) -> dict[str, Any]:
    csv_path = output_dir / f"dr3_neighbourhood_{index:05d}.csv"
    query_path = output_dir / f"dr3_neighbourhood_{index:05d}.adql"
    query = (
        "select dr2_source_id,dr3_source_id,angular_distance,magnitude_difference,"
        "proper_motion_propagation from gaiadr3.dr2_neighbourhood "
        f"where dr3_source_id in ({','.join(ids)}) "
        "order by dr3_source_id,angular_distance,dr2_source_id"
    )
    if csv_path.exists() and query_path.exists() and query_path.read_text(encoding="utf-8") == query + "\n":
        return {"index": index, "row_count": validate_tap_csv(csv_path.read_bytes()), "reused": True}
    payload = tap_query(query, timeout_s=timeout_s, retries=retries, max_records=max_records)
    row_count = validate_tap_csv(payload)
    temp_csv = csv_path.with_suffix(".csv.tmp")
    temp_query = query_path.with_suffix(".adql.tmp")
    temp_csv.write_bytes(payload)
    temp_query.write_text(query + "\n", encoding="utf-8")
    os.replace(temp_csv, csv_path)
    os.replace(temp_query, query_path)
    return {"index": index, "row_count": row_count, "reused": False}


def publish_manifest(state_dir: Path, final: Path, report: dict[str, Any]) -> None:
    target = report["target_set"]
    entries = [
        manifest_entry(
            source_name="gaia_dr3_identity_target_set",
            source_version=report["source_version"],
            path=final / "target_set.csv",
            state_dir=state_dir,
            retrieved_at=report["retrieved_at"],
            row_count=int(target["target_count"]),
            query_signature=target["target_set_sha256"],
            extra={
                "url": "spacegate://evidence-lake/identity-reverse-target-union",
                "forward_tree_sha256": target["forward_tree_sha256"],
                "forward_pair_count": target["forward_pair_count"],
                "snapshot_id": report["snapshot_id"],
            },
        ),
        manifest_entry(
            source_name="gaia_dr2_neighbourhood_reverse_union",
            source_version=report["source_version"],
            path=final / "neighbourhood_chunks",
            state_dir=state_dir,
            retrieved_at=report["retrieved_at"],
            row_count=int(report["neighbourhood_row_count"]),
            query_signature=report["query_signature"],
            extra={
                "target_set_sha256": target["target_set_sha256"],
                "target_count": target["target_count"],
                "chunk_size": report["chunk_size"],
                "chunk_count": report["chunk_count"],
                "official_table": "gaiadr3.dr2_neighbourhood",
                "snapshot_id": report["snapshot_id"],
            },
        ),
    ]
    # The helper defaults source URLs by artifact name; override the derived target URL.
    entries[0]["url"] = "spacegate://evidence-lake/identity-reverse-target-union"
    atomic_write_json(
        state_dir / "reports" / "manifests" / "gaia_dr2_identity_reverse_manifest.json",
        entries,
    )
    atomic_write_json(
        state_dir / "reports" / "evidence_lake_v2" / "e2_gaia_dr2_reverse_acquisition.json",
        report,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--forward-chunks", type=Path)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-s", type=int, default=300)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--max-records", type=int, default=100_000)
    parser.add_argument("--targets-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 1 <= args.chunk_size <= 20_000:
        raise SystemExit("--chunk-size must be between 1 and 20000")
    if not 1 <= args.workers <= 8:
        raise SystemExit("--workers must be between 1 and 8")
    state_dir = args.state_dir.resolve()
    forward_chunks = args.forward_chunks or default_forward_chunks(state_dir)
    staging = state_dir / "tmp" / "gaia_dr3_identity_targets.csv"
    target_report = build_reverse_target_set(forward_chunks, staging)
    target_hash = target_report["target_set_sha256"]
    snapshot_id = f"reverse_union_{target_hash[:16]}"
    source_version = f"gaiadr3_dr2_neighbourhood_{snapshot_id}"
    root = state_dir / "raw" / "gaia_dr2_identity_reverse"
    final = root / "snapshots" / snapshot_id
    work = root / f".{snapshot_id}.work"
    if final.exists():
        report = json.loads((final / "snapshot_report.json").read_text(encoding="utf-8"))
        publish_manifest(state_dir, final, report)
        print(f"Gaia reverse identity snapshot already complete: {final}")
        return 0
    work.mkdir(parents=True, exist_ok=True)
    target_path = work / "target_set.csv"
    if target_path.exists() and sha256_file(target_path) != target_hash:
        raise SystemExit(f"stale target set in resumable work directory: {work}")
    if not target_path.exists():
        shutil.copy2(staging, target_path)
    staging.unlink(missing_ok=True)
    atomic_write_json(work / "target_set_report.json", target_report)
    if args.targets_only:
        print(f"Gaia DR3 reverse targets: {target_report['target_count']:,} -> {work}")
        return 0

    ids = reverse_target_ids(target_path)
    specs = list(chunks(ids, args.chunk_size))
    chunk_dir = work / "neighbourhood_chunks"
    chunk_dir.mkdir(exist_ok=True)
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                fetch_reverse_chunk,
                index,
                id_chunk,
                chunk_dir,
                timeout_s=args.timeout_s,
                retries=args.retries,
                max_records=args.max_records,
            ): index
            for index, id_chunk in specs
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            results.append(future.result())
            if completed % 10 == 0 or completed == len(futures):
                rows = sum(int(item["row_count"]) for item in results)
                print(
                    f"Gaia reverse neighbourhood: chunks={completed}/{len(futures)} rows={rows:,}",
                    flush=True,
                )
    results.sort(key=lambda item: int(item["index"]))
    row_count = sum(int(item["row_count"]) for item in results)
    query_signature = stable_hash(
        {
            "table": "gaiadr3.dr2_neighbourhood",
            "filter": "dr3_source_id",
            "columns": EXPECTED_FIELDS,
            "target_set_sha256": target_hash,
            "chunk_size": args.chunk_size,
            "ordering": ["dr3_source_id", "angular_distance", "dr2_source_id"],
        }
    )
    report = {
        "schema_version": "spacegate.gaia_dr2_identity_reverse_snapshot.v1",
        "snapshot_id": snapshot_id,
        "source_version": source_version,
        "retrieved_at": utc_now(),
        "target_set": target_report,
        "query_signature": query_signature,
        "chunk_size": args.chunk_size,
        "chunk_count": len(results),
        "neighbourhood_row_count": row_count,
        "reused_chunk_count": sum(bool(item["reused"]) for item in results),
        "official_table": "gaiadr3.dr2_neighbourhood",
        "official_documentation": GAIA_DOC_URL,
        "endpoint": GAIA_TAP_URL,
    }
    atomic_write_json(work / "snapshot_report.json", report)
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(work, final)
    publish_manifest(state_dir, final, report)
    print(
        f"Gaia reverse identity snapshot complete: targets={len(ids):,} "
        f"neighbourhood_rows={row_count:,} -> {final}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
