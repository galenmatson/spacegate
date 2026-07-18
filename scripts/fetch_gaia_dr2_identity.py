#!/usr/bin/env python3
"""Collect the official Gaia DR2-to-DR3 neighbourhood for active identity paths."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import tempfile
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb


GAIA_TAP_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
GAIA_DOC_URL = (
    "https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/"
    "chap_datamodel/sec_dm_cross-matches/ssec_dm_dr2_neighbourhood.html"
)
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
EXPECTED_FIELDS = [
    "dr2_source_id",
    "dr3_source_id",
    "angular_distance",
    "magnitude_difference",
    "proper_motion_propagation",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def tree_fingerprint(path: Path) -> tuple[str, int]:
    rows = []
    total = 0
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        size = child.stat().st_size
        total += size
        rows.append(
            {
                "path": child.relative_to(path).as_posix(),
                "bytes": size,
                "sha256": sha256_file(child),
            }
        )
    return stable_hash(rows), total


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    )
    temp = Path(handle.name)
    try:
        with handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp, path)
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def typed_paths(report_path: Path) -> dict[tuple[str, str], Path]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    paths: dict[tuple[str, str], Path] = {}
    typed_root = report_path.parents[2] / "typed" / "evidence_lake_v2"
    for source in report["sources"]:
        root = (
            typed_root
            / source["source_id"]
            / source["release_id"]
            / source["snapshot_id"]
            / source["typed_snapshot_id"]
        )
        for table in source.get("tables", []):
            if table.get("status") == "typed":
                paths[(source["source_id"], table["source_name"])] = root / table["parquet_path"]
    return paths


def required_path(paths: dict[tuple[str, str], Path], key: tuple[str, str]) -> Path:
    path = paths.get(key)
    if path is None or not path.exists():
        raise FileNotFoundError(f"missing active typed table: {key}")
    return path


def target_source_queries(paths: dict[tuple[str, str], Path]) -> list[tuple[str, str]]:
    nasa_ps = required_path(paths, ("nasa_exoplanet_archive.planetary_systems", "ps"))
    nasa_composite = required_path(
        paths, ("nasa_exoplanet_archive.planetary_systems", "pscomppars")
    )
    tess = required_path(paths, ("tess.identity_and_candidate_evidence", "mast_tic_targeted"))
    clusters = required_path(
        paths, ("clusters.cantat_gaudin_2020", "cantat_gaudin_2020_members")
    )
    white_dwarfs = required_path(
        paths, ("compact.gaia_edr3_white_dwarf", "gaia_edr3_white_dwarf_main")
    )
    ultracool = required_path(paths, ("ultracool.ultracoolsheet", "UltracoolSheet_Main"))
    return [
        (
            "nasa_planet_reference_rows",
            "select try_cast(regexp_extract(gaia_dr2_id, '([0-9]{10,})', 1) as bigint) "
            f"as dr2_source_id from read_parquet({sql_literal(nasa_ps)})",
        ),
        (
            "nasa_planet_composite_rows",
            "select try_cast(regexp_extract(gaia_dr2_id, '([0-9]{10,})', 1) as bigint) "
            f"as dr2_source_id from read_parquet({sql_literal(nasa_composite)})",
        ),
        (
            "tess_targeted_tic",
            f"select try_cast(GAIA as bigint) as dr2_source_id from read_parquet({sql_literal(tess)})",
        ),
        (
            "cantat_gaudin_2020_cluster_members",
            "select try_cast(\"GaiaDR2\" as bigint) as dr2_source_id "
            f"from read_parquet({sql_literal(clusters)})",
        ),
        (
            "gaia_edr3_white_dwarf_crosswalk",
            f"select dr2_source_id from read_parquet({sql_literal(white_dwarfs)})",
        ),
        (
            "ultracoolsheet",
            "select try_cast(\"sourceID_Gaia_DR2\" as bigint) as dr2_source_id "
            f"from read_parquet({sql_literal(ultracool)})",
        ),
    ]


def build_target_set(typed_report: Path, output: Path) -> dict[str, Any]:
    paths = typed_paths(typed_report)
    queries = target_source_queries(paths)
    con = duckdb.connect()
    con.execute("set threads=1")
    con.execute("set preserve_insertion_order=true")
    try:
        unions = []
        for family, query in queries:
            unions.append(
                "select "
                f"{sql_literal(family)}::varchar as source_family, dr2_source_id "
                f"from ({query}) where dr2_source_id is not null and dr2_source_id > 0"
            )
        con.execute(
            "create temp table target_evidence as " + " union all ".join(unions)
        )
        family_rows = con.execute(
            """
            select source_family, count(*)::bigint source_record_count,
                   count(distinct dr2_source_id)::bigint distinct_dr2_source_count
            from target_evidence group by source_family order by source_family
            """
        ).fetchall()
        output.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"""
            copy (
              with per_family as (
                select dr2_source_id, source_family, count(*)::bigint source_record_count
                from target_evidence group by dr2_source_id, source_family
              )
              select dr2_source_id,
                     string_agg(source_family, ',' order by source_family) source_families,
                     count(*)::integer source_family_count,
                     sum(source_record_count)::bigint source_record_count
              from per_family group by dr2_source_id order by dr2_source_id
            ) to {sql_literal(output)}
            (format csv, header true, delimiter ',')
            """
        )
        target_count = int(
            con.execute("select count(distinct dr2_source_id) from target_evidence").fetchone()[0]
        )
        return {
            "target_count": target_count,
            "family_counts": [
                {
                    "source_family": row[0],
                    "source_record_count": int(row[1]),
                    "distinct_dr2_source_count": int(row[2]),
                }
                for row in family_rows
            ],
            "typed_report": str(typed_report),
            "typed_report_sha256": sha256_file(typed_report),
            "target_set_sha256": sha256_file(output),
        }
    finally:
        con.close()


def chunks(values: list[str], size: int) -> Iterable[tuple[int, list[str]]]:
    for offset in range(0, len(values), size):
        yield offset // size + 1, values[offset : offset + size]


def target_ids(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    values = [str(int(row["dr2_source_id"])) for row in rows]
    if values != sorted(set(values), key=int):
        raise ValueError("Gaia DR2 target set is not unique and numerically ordered")
    return values


def tap_query(query: str, *, timeout_s: int, retries: int, max_records: int) -> bytes:
    encoded = urllib.parse.urlencode(
        {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": "csv",
            "MAXREC": str(max_records),
            "QUERY": query,
        }
    ).encode("ascii")
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(
                GAIA_TAP_URL,
                data=encoded,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": USER_AGENT,
                },
            )
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read()
            validate_tap_csv(payload)
            return payload
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(f"Gaia TAP query failed after {retries} attempts: {last_error}")


def validate_tap_csv(payload: bytes) -> int:
    text = payload.decode("utf-8-sig", errors="replace")
    if "<VOTABLE" in text[:500].upper() or "QUERY_STATUS" in text[:1000].upper():
        raise ValueError(f"Gaia TAP returned an error document: {text[:500]}")
    reader = csv.reader(text.splitlines())
    header = next(reader, [])
    if header != EXPECTED_FIELDS:
        raise ValueError(f"Gaia TAP schema drift: {header} != {EXPECTED_FIELDS}")
    return sum(1 for row in reader if row)


def fetch_chunk(
    index: int,
    ids: list[str],
    output_dir: Path,
    *,
    timeout_s: int,
    retries: int,
    max_records: int,
) -> dict[str, Any]:
    csv_path = output_dir / f"dr2_neighbourhood_{index:05d}.csv"
    query_path = output_dir / f"dr2_neighbourhood_{index:05d}.adql"
    query = (
        "select dr2_source_id,dr3_source_id,angular_distance,magnitude_difference,"
        "proper_motion_propagation from gaiadr3.dr2_neighbourhood "
        f"where dr2_source_id in ({','.join(ids)}) "
        "order by dr2_source_id,angular_distance,dr3_source_id"
    )
    if csv_path.exists() and query_path.exists() and query_path.read_text(encoding="utf-8") == query + "\n":
        row_count = validate_tap_csv(csv_path.read_bytes())
        return {"index": index, "row_count": row_count, "reused": True}
    payload = tap_query(
        query, timeout_s=timeout_s, retries=retries, max_records=max_records
    )
    row_count = validate_tap_csv(payload)
    temp_csv = csv_path.with_suffix(".csv.tmp")
    temp_query = query_path.with_suffix(".adql.tmp")
    temp_csv.write_bytes(payload)
    temp_query.write_text(query + "\n", encoding="utf-8")
    os.replace(temp_csv, csv_path)
    os.replace(temp_query, query_path)
    return {"index": index, "row_count": row_count, "reused": False}


def manifest_entry(
    *,
    source_name: str,
    source_version: str,
    path: Path,
    state_dir: Path,
    retrieved_at: str,
    row_count: int,
    query_signature: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    digest, total_bytes = tree_fingerprint(path.parent if path.is_file() else path)
    return {
        "source_name": source_name,
        "source_version": source_version,
        "url": GAIA_TAP_URL if source_name != "gaia_dr2_identity_target_set" else "spacegate://evidence-lake/identity-target-union",
        "dest_path": str(path.relative_to(state_dir)),
        "retrieved_at": retrieved_at,
        "checked_at": retrieved_at,
        "sha256": sha256_file(path) if path.is_file() else digest,
        "bytes_written": path.stat().st_size if path.is_file() else total_bytes,
        "row_count": row_count,
        "query_signature": query_signature,
        "citation_url": GAIA_DOC_URL,
        **(extra or {}),
    }


def publish_manifest(
    state_dir: Path,
    final: Path,
    snapshot_report: dict[str, Any],
) -> None:
    retrieved_at = str(snapshot_report["retrieved_at"])
    source_version = str(snapshot_report["source_version"])
    target_report = snapshot_report["target_set"]
    query_signature = str(snapshot_report["query_signature"])
    manifest = [
        manifest_entry(
            source_name="gaia_dr2_identity_target_set",
            source_version=source_version,
            path=final / "target_set.csv",
            state_dir=state_dir,
            retrieved_at=retrieved_at,
            row_count=int(target_report["target_count"]),
            query_signature=str(target_report["target_set_sha256"]),
            extra={
                "typed_report": target_report["typed_report"],
                "typed_report_sha256": target_report["typed_report_sha256"],
                "family_counts": target_report["family_counts"],
                "snapshot_id": snapshot_report["snapshot_id"],
            },
        ),
        manifest_entry(
            source_name="gaia_dr2_neighbourhood_union",
            source_version=source_version,
            path=final / "neighbourhood_chunks",
            state_dir=state_dir,
            retrieved_at=retrieved_at,
            row_count=int(snapshot_report["neighbourhood_row_count"]),
            query_signature=query_signature,
            extra={
                "target_set_sha256": target_report["target_set_sha256"],
                "target_count": target_report["target_count"],
                "chunk_size": snapshot_report["chunk_size"],
                "chunk_count": snapshot_report["chunk_count"],
                "official_table": snapshot_report["official_table"],
                "snapshot_id": snapshot_report["snapshot_id"],
            },
        ),
    ]
    atomic_write_json(
        state_dir / "reports" / "manifests" / "gaia_dr2_identity_manifest.json",
        manifest,
    )
    atomic_write_json(
        state_dir / "reports" / "evidence_lake_v2" / "e2_gaia_dr2_acquisition.json",
        snapshot_report,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--typed-report", type=Path)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-s", type=int, default=300)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--max-records", type=int, default=100_000)
    parser.add_argument("--targets-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.chunk_size < 1 or args.chunk_size > 20_000:
        raise SystemExit("--chunk-size must be between 1 and 20000")
    if args.workers < 1 or args.workers > 8:
        raise SystemExit("--workers must be between 1 and 8")
    state_dir = args.state_dir.resolve()
    typed_report = args.typed_report
    if typed_report is None:
        report_root = state_dir / "reports" / "evidence_lake_v2"
        typed_report = report_root / "e2_typed_cook_report.json"
        if not typed_report.exists():
            typed_report = report_root / "e1_typed_cook_report.json"
    staging = state_dir / "tmp" / "gaia_dr2_identity_targets.csv"
    target_report = build_target_set(typed_report, staging)
    target_hash = target_report["target_set_sha256"]
    snapshot_id = f"target_union_{target_hash[:16]}"
    source_version = f"gaiadr3_dr2_neighbourhood_{snapshot_id}"
    root = state_dir / "raw" / "gaia_dr2_identity"
    final = root / "snapshots" / snapshot_id
    work = root / f".{snapshot_id}.work"
    if final.exists():
        snapshot_report = json.loads(
            (final / "snapshot_report.json").read_text(encoding="utf-8")
        )
        publish_manifest(state_dir, final, snapshot_report)
        print(f"Gaia DR2 identity snapshot already complete: {final}")
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
        print(f"Gaia DR2 identity targets: {target_report['target_count']:,} -> {work}")
        return 0

    ids = target_ids(target_path)
    chunk_specs = list(chunks(ids, args.chunk_size))
    chunk_dir = work / "neighbourhood_chunks"
    chunk_dir.mkdir(exist_ok=True)
    started_at = utc_now()
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                fetch_chunk,
                index,
                id_chunk,
                chunk_dir,
                timeout_s=args.timeout_s,
                retries=args.retries,
                max_records=args.max_records,
            ): index
            for index, id_chunk in chunk_specs
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            results.append(result)
            if completed % 10 == 0 or completed == len(futures):
                rows = sum(int(item["row_count"]) for item in results)
                print(
                    f"Gaia DR2 neighbourhood: chunks={completed}/{len(futures)} rows={rows:,}",
                    flush=True,
                )
    results.sort(key=lambda item: int(item["index"]))
    row_count = sum(int(item["row_count"]) for item in results)
    retrieved_at = utc_now()
    query_signature = stable_hash(
        {
            "table": "gaiadr3.dr2_neighbourhood",
            "columns": EXPECTED_FIELDS,
            "target_set_sha256": target_hash,
            "chunk_size": args.chunk_size,
            "ordering": ["dr2_source_id", "angular_distance", "dr3_source_id"],
        }
    )
    snapshot_report = {
        "schema_version": "spacegate.gaia_dr2_identity_snapshot.v1",
        "snapshot_id": snapshot_id,
        "source_version": source_version,
        "started_at": started_at,
        "retrieved_at": retrieved_at,
        "target_set": target_report,
        "query_signature": query_signature,
        "chunk_size": args.chunk_size,
        "chunk_count": len(results),
        "neighbourhood_row_count": row_count,
        "reused_chunk_count": sum(bool(item["reused"]) for item in results),
        "official_table": "gaiadr3.dr2_neighbourhood",
        "official_documentation": GAIA_DOC_URL,
    }
    atomic_write_json(work / "snapshot_report.json", snapshot_report)
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(work, final)

    publish_manifest(state_dir, final, snapshot_report)
    print(
        f"Gaia DR2 identity snapshot complete: targets={len(ids):,} "
        f"neighbourhood_rows={row_count:,} -> {final}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
