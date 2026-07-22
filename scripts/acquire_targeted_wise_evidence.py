#!/usr/bin/env python3
"""Acquire immutable targeted CatWISE/AllWISE response sets from clean inputs."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import duckdb
from astropy.io.votable import parse_single_table


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config" / "evidence_lake" / "targeted_wise_acquisition.json"
DEFAULT_STATE = Path(os.environ.get("SPACEGATE_STATE_DIR", "/data/spacegate/state"))
FOUNDATION_ROOT = Path("/mnt/space/spacegate/e7-clean-foundation")
SCIENCE_ROOT = Path("/mnt/space/spacegate/e7-clean-science")
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_hash(path: Path, *, exclude_names: set[str] | None = None) -> str:
    excluded = exclude_names or set()
    files = [
        {
            "path": child.relative_to(path).as_posix(),
            "bytes": child.stat().st_size,
            "sha256": file_hash(child),
        }
        for child in sorted(path.rglob("*"))
        if child.is_file() and child.name not in excluded
    ]
    return stable_hash(files)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def verify_pinned_manifest(root: Path, expected_sha256: str) -> dict[str, Any]:
    path = root / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(path)
    actual = file_hash(path)
    if actual != expected_sha256:
        raise ValueError(f"pinned manifest mismatch: {path}: {actual} != {expected_sha256}")
    return json.loads(path.read_text(encoding="utf-8"))


def target_candidates(
    foundation_db: Path,
    science_db: Path,
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    quotas = policy["target_quotas"]
    selection = policy["selection_policy"]
    limit = int(policy["target_limit"])
    con = duckdb.connect()
    con.execute(f"attach {sql_string(str(foundation_db))} as foundation (read_only)")
    con.execute(f"attach {sql_string(str(science_db))} as science (read_only)")
    maximum_distance_ly = float(selection["maximum_distance_ly"])
    con.execute(
        f"""
        create temp view eligible as
        select
          st.star_id,
          st.system_id,
          st.stable_object_key,
          st.star_name,
          sy.stable_object_key as system_stable_object_key,
          sy.system_name,
          sy.dist_ly,
          sy.star_count,
          sy.planet_count,
          coalesce(a.ra_deg, sy.ra_deg) as ra_deg,
          coalesce(a.dec_deg, sy.dec_deg) as dec_deg,
          a.pmra_mas_yr,
          a.pmdec_mas_yr,
          a.ra_deg_fact_id,
          a.dec_deg_fact_id,
          a.pmra_mas_yr_fact_id,
          a.pmdec_mas_yr_fact_id,
          case when a.ra_deg is not null and a.dec_deg is not null
            then 'selected_stellar_astrometry'
            else 'selected_system_placement'
          end as query_coordinate_basis,
          d.classification_value,
          d.classification_status,
          d.selected_fact_id as classification_fact_id
        from foundation.stars st
        join foundation.systems sy using (system_id)
        left join science.selected_stellar_astrometry a using (star_id)
        join science.selected_stellar_display_classifications d using (star_id)
        where coalesce(a.ra_deg, sy.ra_deg) is not null
          and coalesce(a.dec_deg, sy.dec_deg) is not null
          and sy.dist_ly <= {maximum_distance_ly:.9f}
        """
    )

    chosen: dict[str, dict[str, Any]] = {}

    def add(rows: list[dict[str, Any]], reason: str, quota: int) -> None:
        added = 0
        for row in rows:
            key = str(row["stable_object_key"])
            if key in chosen:
                reasons = chosen[key]["selection_reasons"]
                if reason not in reasons:
                    reasons.append(reason)
                continue
            if added >= quota or len(chosen) >= limit:
                break
            row["selection_reasons"] = [reason]
            chosen[key] = row
            added += 1

    def rows(query: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        result = con.execute(query, parameters or []).fetchall()
        names = [item[0] for item in con.description]
        return [dict(zip(names, row, strict=True)) for row in result]

    seeds = [str(item["stable_object_key"]) for item in policy["operator_seed_stable_object_keys"]]
    if seeds:
        placeholders = ",".join("?" for _ in seeds)
        seed_rows = rows(
            f"select * from eligible where stable_object_key in ({placeholders}) "
            "order by dist_ly, stable_object_key",
            seeds,
        )
        missing = sorted(set(seeds) - {str(row["stable_object_key"]) for row in seed_rows})
        if missing:
            raise ValueError(f"operator WISE seeds are not eligible: {missing}")
        add(seed_rows, "operator_seed", int(quotas["operator_seed"]))

    ultracool = [str(value) for value in selection["source_ultracool_classes"]]
    placeholders = ",".join("?" for _ in ultracool)
    add(
        rows(
            f"select * from eligible where classification_status='source' "
            f"and classification_value in ({placeholders}) order by dist_ly, stable_object_key",
            ultracool,
        ),
        "source_ultracool",
        int(quotas["source_ultracool"]),
    )
    add(
        rows("select * from eligible where planet_count > 0 order by dist_ly, stable_object_key"),
        "planet_host",
        int(quotas["planet_host"]),
    )
    add(
        rows("select * from eligible where star_count > 1 order by dist_ly, stable_object_key"),
        "multiple_system",
        int(quotas["multiple_system"]),
    )
    add(
        rows("select * from eligible where dist_ly <= 25 order by dist_ly, stable_object_key"),
        "nearby_25ly",
        int(quotas["nearby_25ly"]),
    )
    compact = [str(value) for value in selection["compact_classes"]]
    placeholders = ",".join("?" for _ in compact)
    add(
        rows(
            f"select * from eligible where classification_status='source' "
            f"and classification_value in ({placeholders}) order by dist_ly, stable_object_key",
            compact,
        ),
        "compact_object",
        int(quotas["compact_object"]),
    )
    add(
        rows("select * from eligible order by dist_ly, stable_object_key limit ?", [limit * 4]),
        "nearest_remainder",
        limit,
    )
    con.close()
    result = sorted(chosen.values(), key=lambda row: (float(row["dist_ly"]), row["stable_object_key"]))
    if len(result) != limit:
        raise ValueError(f"target policy produced {len(result)} targets, expected {limit}")
    for index, row in enumerate(result, start=1):
        row["target_index"] = index
        row["selection_reasons"].sort()
    return result


def load_pinned_target_set(
    report_path: Path,
    *,
    policy: dict[str, Any],
    policy_sha256: str,
) -> tuple[list[dict[str, Any]], str] | None:
    if not report_path.exists():
        return None
    report = json.loads(report_path.read_text(encoding="utf-8"))
    targets = report.get("targets")
    if (
        report.get("schema_version") != "spacegate.targeted_wise_target_set.v1"
        or report.get("policy_version") != policy["policy_version"]
        or report.get("policy_sha256") != policy_sha256
        or not isinstance(targets, list)
        or len(targets) != int(policy["target_limit"])
    ):
        return None
    targets_sha256 = stable_hash(targets)
    if targets_sha256 != report.get("targets_sha256"):
        raise ValueError(f"target-set report checksum mismatch: {report_path}")
    return targets, targets_sha256


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def propagated_position(row: dict[str, Any], from_epoch: float, to_epoch: float) -> tuple[float, float]:
    ra = float(row["ra_deg"])
    dec = float(row["dec_deg"])
    delta = to_epoch - from_epoch
    pmra = float(row["pmra_mas_yr"] or 0.0)
    pmdec = float(row["pmdec_mas_yr"] or 0.0)
    cos_dec = max(0.01, abs(math.cos(math.radians(dec))))
    ra = (ra + pmra * delta / (1000.0 * 3600.0 * cos_dec)) % 360.0
    dec = max(-90.0, min(90.0, dec + pmdec * delta / (1000.0 * 3600.0)))
    return ra, dec


def query_url(
    catalog: dict[str, Any],
    target: dict[str, Any],
    query: dict[str, Any],
    *,
    radius_arcsec: float,
) -> tuple[str, float, float]:
    ra, dec = propagated_position(
        target,
        float(query["source_position_epoch_year"]),
        float(catalog["query_epoch_year"]),
    )
    params = {
        "catalog": catalog["catalog"],
        "spatial": "cone",
        "objstr": f"{ra:.10f},{dec:.10f}",
        "radius": f"{radius_arcsec:.3f}",
        "outfmt": "3",
        "selcols": ",".join(catalog["columns"]),
        "outrows": str(int(query["outrows"])),
    }
    return f"{query['endpoint']}?{urllib.parse.urlencode(params)}", ra, dec


def response_shape(path: Path) -> tuple[int, list[str], str]:
    try:
        table = parse_single_table(path)
    except (ValueError, IndexError) as exc:
        if "No table found" in str(exc):
            return 0, [], "no_table"
        raise
    return (
        len(table.array),
        [str(field.name or field.ID) for field in table.fields],
        "table",
    )


def fetch_one(
    *,
    catalog: dict[str, Any],
    target: dict[str, Any],
    query: dict[str, Any],
    directory: Path,
    timeout_s: float,
    retries: int,
) -> dict[str, Any]:
    filename = f"{int(target['target_index']):04d}_{stable_hash(target['stable_object_key'])[:16]}.vot"
    path = directory / filename
    requested_radius = float(query["radius_arcsec"])
    url, query_ra, query_dec = query_url(
        catalog,
        target,
        query,
        radius_arcsec=requested_radius,
    )
    if path.exists():
        payload = path.read_bytes()
        row_count, fields, source_status = response_shape(path)
        return {
            "filename": filename,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "row_count": row_count,
            "fields": fields,
            "source_status": source_status,
            "url": url,
            "query_ra_deg": query_ra,
            "query_dec_deg": query_dec,
            "query_radius_arcsec": requested_radius,
            "error_responses": [],
            "target": target,
            "resume_status": "verified_existing",
        }
    for stale in directory.glob(f".{filename}.*.tmp"):
        stale.unlink()
    error_responses: list[dict[str, Any]] = []
    last_error: Exception | None = None
    radii = [requested_radius, *[float(value) for value in query["overflow_fallback_radius_arcsec"]]]
    for radius_index, radius in enumerate(radii):
        url, query_ra, query_dec = query_url(
            catalog,
            target,
            query,
            radius_arcsec=radius,
        )
        for attempt in range(1, retries + 1):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(request, timeout=timeout_s) as response:
                    payload = response.read()
                decoded = payload[:4000].decode("utf-8", "replace")
                source_error = bool(
                    re.search(r'<INFO\s+name="(?:QUERY_STATUS|Error)"\s+value="ERROR"', decoded)
                    or 'struct stat="ERROR"' in decoded
                )
                if source_error:
                    error_dir = directory / "query_errors"
                    error_dir.mkdir(exist_ok=True)
                    error_name = (
                        f"{filename}.radius_{radius:g}.attempt_{attempt}.source_error.xml"
                    )
                    error_path = error_dir / error_name
                    error_path.write_bytes(payload)
                    error_responses.append(
                        {
                            "path": f"query_errors/{error_name}",
                            "radius_arcsec": radius,
                            "attempt": attempt,
                            "bytes": len(payload),
                            "sha256": hashlib.sha256(payload).hexdigest(),
                            "message": re.sub(r"\s+", " ", decoded)[:1000],
                        }
                    )
                    if "Exceeding output table size limit" in decoded and radius_index + 1 < len(radii):
                        break
                    raise RuntimeError(decoded[:1000])
                temporary = path.with_name(f".{filename}.{os.getpid()}.tmp")
                temporary.write_bytes(payload)
                row_count, fields, source_status = response_shape(temporary)
                os.replace(temporary, path)
                return {
                    "filename": filename,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "row_count": row_count,
                    "fields": fields,
                    "source_status": source_status,
                    "url": url,
                    "query_ra_deg": query_ra,
                    "query_dec_deg": query_dec,
                    "query_radius_arcsec": radius,
                    "error_responses": error_responses,
                    "target": target,
                    "attempt": attempt,
                    "resume_status": "downloaded",
                }
            except Exception as exc:
                last_error = exc
                if attempt < retries:
                    time.sleep(min(2**attempt, 20))
        else:
            continue
        # A density-limit source response intentionally advances to the next
        # declared radius after the first preserved error response.
        continue
    raise RuntimeError(f"{catalog['catalog']} {target['stable_object_key']}: {last_error}")


def acquire_catalog(
    *,
    policy: dict[str, Any],
    policy_sha256: str,
    targets: list[dict[str, Any]],
    targets_sha256: str,
    catalog: dict[str, Any],
    state_dir: Path,
    workers: int,
    timeout_s: float,
    retries: int,
) -> dict[str, Any]:
    query = policy["query"]
    snapshot_id = stable_hash(
        {
            "source_id": catalog["source_id"],
            "release_id": policy["release_id"],
            "policy_sha256": policy_sha256,
            "targets_sha256": targets_sha256,
            "catalog": catalog,
            "query": {key: value for key, value in query.items() if key != "catalogs"},
        }
    )[:24]
    root = (
        state_dir
        / "raw"
        / "evidence_lake_v2_acquisition"
        / catalog["source_id"].replace(".", "_")
        / policy["release_id"]
        / "snapshots"
        / snapshot_id
        / catalog["source_name"]
    )
    if root.exists():
        manifest = json.loads((root / "product_manifest.json").read_text(encoding="utf-8"))
        if tree_hash(root, exclude_names={"product_manifest.json"}) != manifest["artifact_tree_sha256"]:
            raise ValueError(f"immutable targeted WISE artifact changed: {root}")
        return manifest["legacy_manifest_entry"]

    staging = state_dir / "tmp" / "evidence_lake_v2_wise" / snapshot_id / catalog["source_name"]
    staging.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                fetch_one,
                catalog=catalog,
                target=target,
                query=query,
                directory=staging,
                timeout_s=timeout_s,
                retries=retries,
            ): target
            for target in targets
        }
        for completed, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append(
                    {
                        "stable_object_key": futures[future]["stable_object_key"],
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
            if completed % 25 == 0 or completed == len(futures):
                print(
                    f"{utc_now()} {catalog['catalog']} {completed}/{len(futures)} "
                    f"elapsed={time.monotonic() - started:.1f}s",
                    flush=True,
                )
    if errors:
        write_json(
            staging / "acquisition_errors.json",
            {
                "schema_version": "spacegate.targeted_wise_acquisition_errors.v1",
                "catalog": catalog["catalog"],
                "error_count": len(errors),
                "errors": errors,
            },
        )
        raise RuntimeError(
            f"{catalog['catalog']} acquisition has {len(errors)} failed members; "
            f"see {staging / 'acquisition_errors.json'}"
        )
    results.sort(key=lambda row: int(row["target"]["target_index"]))
    schemas = {tuple(row["fields"]) for row in results if row["fields"]}
    if len(schemas) != 1:
        raise ValueError(f"source schema drift inside {catalog['catalog']}: {len(schemas)} schemas")
    response_set = {
        "schema_version": "spacegate.targeted_wise_query_manifest.v1",
        "snapshot_id": snapshot_id,
        "source_id": catalog["source_id"],
        "release_id": policy["release_id"],
        "catalog": catalog,
        "policy_sha256": policy_sha256,
        "targets_sha256": targets_sha256,
        "target_count": len(targets),
        "responses": results,
    }
    write_json(staging / "query_manifest.json", response_set)
    fields = next(iter(schemas))
    retrieved_at = utc_now()
    product_manifest = {
        "schema_version": "spacegate.targeted_wise_product_manifest.v1",
        "snapshot_id": snapshot_id,
        "source_id": catalog["source_id"],
        "release_id": policy["release_id"],
        "source_name": catalog["source_name"],
        "retrieved_at": retrieved_at,
        "query_signature": stable_hash(response_set),
        "target_count": len(targets),
        "response_count": len(results),
        "row_count": sum(int(row["row_count"]) for row in results),
        "field_dispositions": [
            {"column_name": field, "disposition": "preserve", "reason": "source-native targeted response field"}
            for field in fields
        ],
        "elapsed_seconds": round(time.monotonic() - started, 6),
    }
    write_json(staging / "product_manifest.json", product_manifest)
    artifact_tree_sha256 = tree_hash(staging, exclude_names={"product_manifest.json"})
    bytes_written = sum(path.stat().st_size for path in staging.rglob("*") if path.is_file())
    legacy = {
        "source_name": catalog["source_name"],
        "source_version": policy["release_id"],
        "url": query["endpoint"],
        "dest_path": str(root),
        "retrieved_at": retrieved_at,
        "checked_at": retrieved_at,
        "sha256": artifact_tree_sha256,
        "bytes_written": bytes_written,
        "row_count": product_manifest["row_count"],
        "snapshot_id": snapshot_id,
        "query_signature": product_manifest["query_signature"],
        "field_disposition_report": str(root / "product_manifest.json"),
    }
    product_manifest["artifact_tree_sha256"] = artifact_tree_sha256
    product_manifest["bytes_written"] = bytes_written
    product_manifest["legacy_manifest_entry"] = legacy
    write_json(staging / "product_manifest.json", product_manifest)
    # The product manifest contains the tree hash of the response/query payloads,
    # excluding its own final bookkeeping mutation.
    root.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, root)
    return legacy


def merge_manifest(path: Path, entries: list[dict[str, Any]]) -> None:
    existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    replacements = {str(row["source_name"]): row for row in entries}
    merged = [row for row in existing if str(row.get("source_name")) not in replacements]
    merged.extend(entries)
    merged.sort(key=lambda row: str(row["source_name"]))
    write_json(path, merged)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout-s", type=float, default=60.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--rebuild-target-set",
        action="store_true",
        help="recompute the pinned target set from clean foundation/science inputs",
    )
    args = parser.parse_args()
    policy = json.loads(args.policy.read_text(encoding="utf-8"))
    if policy.get("schema_version") != "spacegate.targeted_wise_acquisition.v1":
        raise ValueError("unsupported targeted WISE policy")
    policy_sha256 = file_hash(args.policy)
    foundation_root = FOUNDATION_ROOT / policy["foundation"]["build_id"]
    science_root = SCIENCE_ROOT / policy["science"]["build_id"]
    verify_pinned_manifest(foundation_root, policy["foundation"]["manifest_sha256"])
    verify_pinned_manifest(science_root, policy["science"]["manifest_sha256"])
    report_path = args.state_dir / "reports" / "evidence_lake_v2" / "e3_targeted_wise_target_set.json"
    pinned = None if args.rebuild_target_set else load_pinned_target_set(
        report_path,
        policy=policy,
        policy_sha256=policy_sha256,
    )
    if pinned:
        targets, targets_sha256 = pinned
        target_set_status = "verified_reuse"
    else:
        targets = target_candidates(
            foundation_root / "clean_core_foundation.duckdb",
            science_root / "clean_science.duckdb",
            policy,
        )
        targets_sha256 = stable_hash(targets)
        target_set_status = "rebuilt"
    target_report = {
        "schema_version": "spacegate.targeted_wise_target_set.v1",
        "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha256,
        "targets_sha256": targets_sha256,
        "target_count": len(targets),
        "materialization_status": target_set_status,
        "reason_counts": {
            reason: sum(reason in row["selection_reasons"] for row in targets)
            for reason in sorted({reason for row in targets for reason in row["selection_reasons"]})
        },
        "targets": targets,
    }
    write_json(report_path, target_report)
    print(json.dumps({key: value for key, value in target_report.items() if key != "targets"}, indent=2))
    if args.prepare_only:
        return 0
    entries = [
        acquire_catalog(
            policy=policy,
            policy_sha256=policy_sha256,
            targets=targets,
            targets_sha256=targets_sha256,
            catalog=catalog,
            state_dir=args.state_dir,
            workers=max(1, args.workers),
            timeout_s=args.timeout_s,
            retries=max(1, args.retries),
        )
        for catalog in policy["query"]["catalogs"]
    ]
    manifest = args.state_dir / "reports" / "manifests" / "targeted_wise_manifest.json"
    merge_manifest(manifest, entries)
    print(json.dumps({"manifest": str(manifest), "entries": entries}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
