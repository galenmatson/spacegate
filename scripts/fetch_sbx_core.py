#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile
import time
import urllib.parse
import urllib.request
from io import StringIO
from pathlib import Path

SBX_TAP_SYNC_URL = "https://astro.ulb.ac.be/sbx/tap/sync"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
DEFAULT_MIN_PARALLAX_MAS = 3.26156
DEFAULT_BUCKETS = 23
DEFAULT_TIMEOUT_S = 360
DEFAULT_RETRIES = 6
DEFAULT_DELTA_MODE = "resume"
DEFAULT_DELTA_MAX_AGE_HOURS = 24.0 * 30.0
LEGACY_SBX_VERSION = "sbx_tap_parallax_gte_3.26156"
EVIDENCE_SBX_VERSION = "sbx_tap_full_rolling_snapshot_v1"


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def tap_query_csv(adql: str, timeout_s: int, retries: int) -> str:
    payload = urllib.parse.urlencode(
        {
            "request": "doQuery",
            "version": "1.0",
            "lang": "ADQL",
            "format": "text/csv",
            "query": adql,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        SBX_TAP_SYNC_URL,
        data=payload,
        headers={"User-Agent": USER_AGENT},
    )
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                body = response.read()
            text = body.decode("utf-8", errors="replace")
            low = text.lstrip().lower()
            if low.startswith("error") or "<html" in low[:200]:
                raise RuntimeError(f"SBX TAP returned error payload: {text[:300]}")
            return text
        except Exception as exc:  # pragma: no cover - network error path
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = min(2**attempt, 30)
            log(
                f"SBX TAP retry {attempt}/{retries - 1} failed: {type(exc).__name__}: {exc}; sleeping {sleep_s}s"
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"SBX TAP query failed after {retries} attempts: {last_exc}")


def is_stale(path: Path, *, max_age_s: float) -> bool:
    if max_age_s <= 0:
        return False
    try:
        age_s = max(0.0, time.time() - path.stat().st_mtime)
    except FileNotFoundError:
        return True
    return age_s > max_age_s


def write_partitioned_csv(
    *,
    label: str,
    query_template: str,
    query_signature: str,
    buckets: int,
    out_path: Path,
    timeout_s: int,
    retries: int,
    delta_mode: str,
    delta_max_age_hours: float,
) -> tuple[int, dict[str, int]]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    query_tag = hashlib.sha1(
        f"{label}|{query_signature}|{buckets}".encode("utf-8")
    ).hexdigest()[:12]
    parts_dir = out_path.parent / f"{out_path.stem}.parts.{query_tag}"
    parts_dir.mkdir(parents=True, exist_ok=True)
    expected_fields: list[str] | None = None
    max_age_s = delta_max_age_hours * 3600.0
    bucket_stats = {
        "buckets_total": buckets,
        "buckets_fetched": 0,
        "buckets_reused": 0,
        "buckets_reused_empty": 0,
        "buckets_refreshed": 0,
    }

    def csv_header(path: Path) -> list[str]:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            row = next(reader, [])
            return [str(col) for col in row]

    def csv_row_count(path: Path) -> int:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            return sum(1 for _ in reader)

    for bucket in range(buckets):
        part_path = parts_dir / f"bucket_{bucket:04d}.csv"
        empty_marker_path = parts_dir / f"bucket_{bucket:04d}.empty"
        should_fetch = True
        refresh_reason = "missing"

        if delta_mode != "refresh":
            if part_path.exists() and part_path.stat().st_size > 0:
                if delta_mode == "delta" and is_stale(part_path, max_age_s=max_age_s):
                    refresh_reason = "stale"
                else:
                    should_fetch = False
            elif empty_marker_path.exists():
                if delta_mode == "delta" and is_stale(empty_marker_path, max_age_s=max_age_s):
                    refresh_reason = "stale"
                else:
                    should_fetch = False
                    bucket_stats["buckets_reused_empty"] += 1
                    log(f"{label}: bucket {bucket + 1}/{buckets} reuse empty marker")
                    continue
        elif part_path.exists() or empty_marker_path.exists():
            refresh_reason = "refresh"

        if not should_fetch:
            part_header = csv_header(part_path)
            if not part_header:
                raise RuntimeError(f"{label}: empty header in {part_path}")
            if expected_fields is None:
                expected_fields = part_header
            elif part_header != expected_fields:
                raise RuntimeError(
                    f"{label}: schema mismatch in existing part {part_path.name}: "
                    f"{part_header} != {expected_fields}"
                )
            part_rows = csv_row_count(part_path)
            log(f"{label}: bucket {bucket + 1}/{buckets} resume rows={part_rows:,}")
            bucket_stats["buckets_reused"] += 1
            continue

        if part_path.exists():
            part_path.unlink()
        if empty_marker_path.exists():
            empty_marker_path.unlink()
        if refresh_reason != "missing":
            bucket_stats["buckets_refreshed"] += 1
            log(f"{label}: bucket {bucket + 1}/{buckets} refreshing ({refresh_reason})")

        adql = query_template.format(bucket=bucket, buckets=buckets)
        text = tap_query_csv(adql, timeout_s=timeout_s, retries=retries)
        bucket_stats["buckets_fetched"] += 1
        reader = csv.DictReader(StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            log(f"{label}: bucket {bucket + 1}/{buckets} returned no header/rows")
            empty_marker_path.write_text(
                f"bucket={bucket} empty=true checked_at={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n",
                encoding="utf-8",
            )
            continue
        if expected_fields is None:
            expected_fields = fieldnames
        elif fieldnames != expected_fields:
            raise RuntimeError(
                f"{label}: schema mismatch in bucket {bucket + 1}: "
                f"{fieldnames} != {expected_fields}"
            )

        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            delete=False,
            dir=str(parts_dir),
            prefix=part_path.name + ".tmp.",
        ) as tmp_part:
            tmp_part_path = Path(tmp_part.name)
            writer = csv.DictWriter(tmp_part, fieldnames=expected_fields)
            writer.writeheader()
            bucket_rows = 0
            for row in reader:
                writer.writerow(row)
                bucket_rows += 1
        tmp_part_path.replace(part_path)
        log(f"{label}: bucket {bucket + 1}/{buckets} rows={bucket_rows:,}")

    if expected_fields is None:
        raise RuntimeError(f"{label}: no data returned from SBX TAP")

    total_rows = 0
    with tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        encoding="utf-8",
        delete=False,
        dir=str(out_path.parent),
        prefix=out_path.name + ".tmp.",
    ) as tmp_out:
        tmp_out_path = Path(tmp_out.name)
        writer = csv.DictWriter(tmp_out, fieldnames=expected_fields)
        writer.writeheader()
        for bucket in range(buckets):
            part_path = parts_dir / f"bucket_{bucket:04d}.csv"
            if not part_path.exists() or part_path.stat().st_size == 0:
                continue
            with part_path.open("r", encoding="utf-8", newline="") as part_file:
                reader = csv.DictReader(part_file)
                part_fields = list(reader.fieldnames or [])
                if part_fields != expected_fields:
                    raise RuntimeError(
                        f"{label}: schema mismatch while stitching {part_path.name}: "
                        f"{part_fields} != {expected_fields}"
                    )
                for row in reader:
                    writer.writerow(row)
                    total_rows += 1

    tmp_out_path.replace(out_path)
    return total_rows, bucket_stats


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def write_manifest(
    manifest_path: Path,
    entries: list[dict[str, object]],
    *,
    source_version: str,
) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    manifest_rows = []
    for entry in entries:
        path_abs = Path(str(entry["path_abs"]))
        path_rel = str(entry["path_rel"])
        manifest_rows.append(
            {
                "source_name": str(entry["source_name"]),
                "source_version": source_version,
                "url": SBX_TAP_SYNC_URL,
                "dest_path": path_rel,
                "retrieved_at": ts,
                "checked_at": ts,
                "sha256": file_sha256(path_abs),
                "bytes_written": path_abs.stat().st_size,
                "row_count": int(entry["row_count"]),
                "query_signature": str(entry["query_signature"]),
                "delta_update": entry.get("delta_update", {}),
            }
        )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest_rows, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch SBX inputs for legacy core ingest or Evidence Lake v2."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument(
        "--profile",
        choices=["legacy-core", "evidence-v2"],
        default="legacy-core",
        help=(
            "legacy-core preserves the currently served bounded projection; "
            "evidence-v2 acquires every source-native SBX row and field into a separate manifest"
        ),
    )
    parser.add_argument("--min-parallax-mas", type=float, default=DEFAULT_MIN_PARALLAX_MAS)
    parser.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS)
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument(
        "--delta-mode",
        choices=["resume", "delta", "refresh"],
        default=DEFAULT_DELTA_MODE,
        help="resume=reuse bucket parts, delta=refresh stale buckets only, refresh=refetch all buckets.",
    )
    parser.add_argument(
        "--delta-max-age-hours",
        type=float,
        default=DEFAULT_DELTA_MAX_AGE_HOURS,
        help="In delta mode, local bucket parts older than this are refreshed.",
    )
    args = parser.parse_args()

    if args.buckets < 1:
        raise SystemExit("--buckets must be >= 1")
    if args.profile == "legacy-core" and args.min_parallax_mas <= 0:
        raise SystemExit("--min-parallax-mas must be > 0")
    if args.delta_mode == "delta" and args.delta_max_age_hours <= 0:
        raise SystemExit("--delta-max-age-hours must be > 0 in --delta-mode delta")

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    evidence_profile = args.profile == "evidence-v2"
    if evidence_profile:
        raw_rel_dir = Path("raw/evidence_lake_v2_acquisition/sbx")
        raw_dir = state_dir / raw_rel_dir
        manifest_path = (
            state_dir / "reports" / "manifests" / "sbx_evidence_v2_manifest.json"
        )
        source_version = EVIDENCE_SBX_VERSION
    else:
        raw_rel_dir = Path("raw/sbx")
        raw_dir = state_dir / raw_rel_dir
        manifest_path = state_dir / "reports" / "manifests" / "sbx_manifest.json"
        source_version = LEGACY_SBX_VERSION
    raw_dir.mkdir(parents=True, exist_ok=True)

    min_parallax = f"{args.min_parallax_mas:.8f}"
    if evidence_profile:
        systems_query_template = (
            "SELECT s.* FROM systems s "
            "WHERE MOD(s.sn, {buckets}) = {bucket} ORDER BY sn"
        )
        alias_query_template = (
            "SELECT a.* FROM alias a "
            "WHERE MOD(a.sn, {buckets}) = {bucket} "
            "ORDER BY sn, catalog, version, identifier"
        )
        config_query_template = (
            "SELECT c.* FROM configurations c "
            "WHERE MOD(c.sn, {buckets}) = {bucket} ORDER BY sn"
        )
        orbit_query_template = (
            "SELECT o.* FROM orbits o "
            "WHERE MOD(o.sn, {buckets}) = {bucket} ORDER BY sn, \"on\""
        )
    else:
        systems_query_template = (
            "SELECT s.sn, s.ra, s.dec, s.parallax, s.pmra, s.pmdec, s.mag1, "
            "s.position_epoch, s.position_source, s.st1 "
            "FROM systems s "
            f"WHERE s.parallax >= {min_parallax} AND MOD(s.sn, {{buckets}}) = {{bucket}} "
            "ORDER BY 1"
        )
        alias_query_template = (
            "SELECT a.sn, a.catalog, a.version, a.identifier "
            "FROM alias a "
            "JOIN systems s ON s.sn = a.sn "
            f"WHERE s.parallax >= {min_parallax} "
            "AND MOD(a.sn, {buckets}) = {bucket} "
            "AND ((a.catalog = 'Gaia' AND a.version = 'DR3') OR a.catalog IN ('HIP','HD','WDS','ADS')) "
            "ORDER BY 1,2,3"
        )
        config_query_template = (
            "SELECT c.sn, c.family, c.parent, c.child1, c.child2, c.in_triple "
            "FROM configurations c "
            "JOIN systems s ON s.sn = c.sn "
            f"WHERE s.parallax >= {min_parallax} AND MOD(c.sn, {{buckets}}) = {{bucket}} "
            "ORDER BY 1"
        )
        orbit_query_template = (
            "SELECT o.sn, COUNT(*) AS orbit_count "
            "FROM orbits o "
            "JOIN systems s ON s.sn = o.sn "
            f"WHERE s.parallax >= {min_parallax} AND MOD(o.sn, {{buckets}}) = {{bucket}} "
            "GROUP BY o.sn ORDER BY 1"
        )

    systems_path = raw_dir / "sbx_systems.csv"
    alias_path = raw_dir / "sbx_alias.csv"
    config_path = raw_dir / "sbx_configurations.csv"
    orbit_path = raw_dir / "sbx_orbits.csv"

    log(
        "SBX fetch start "
        f"(profile={args.profile}, source_version={source_version}, "
        f"min_parallax_mas={args.min_parallax_mas if not evidence_profile else 'not_applicable_full_catalog'}, "
        f"buckets={args.buckets}, timeout_s={args.timeout_s}, "
        f"delta_mode={args.delta_mode}, delta_max_age_hours={args.delta_max_age_hours})"
    )

    systems_rows, systems_stats = write_partitioned_csv(
        label="sbx_systems",
        query_template=systems_query_template,
        query_signature=systems_query_template.replace("{bucket}", "<bucket>").replace(
            "{buckets}", str(args.buckets)
        ),
        buckets=args.buckets,
        out_path=systems_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        delta_mode=args.delta_mode,
        delta_max_age_hours=args.delta_max_age_hours,
    )
    alias_rows, alias_stats = write_partitioned_csv(
        label="sbx_alias",
        query_template=alias_query_template,
        query_signature=alias_query_template.replace("{bucket}", "<bucket>").replace(
            "{buckets}", str(args.buckets)
        ),
        buckets=args.buckets,
        out_path=alias_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        delta_mode=args.delta_mode,
        delta_max_age_hours=args.delta_max_age_hours,
    )
    config_rows, config_stats = write_partitioned_csv(
        label="sbx_configurations",
        query_template=config_query_template,
        query_signature=config_query_template.replace("{bucket}", "<bucket>").replace(
            "{buckets}", str(args.buckets)
        ),
        buckets=args.buckets,
        out_path=config_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        delta_mode=args.delta_mode,
        delta_max_age_hours=args.delta_max_age_hours,
    )
    orbit_rows, orbit_stats = write_partitioned_csv(
        label="sbx_orbits",
        query_template=orbit_query_template,
        query_signature=orbit_query_template.replace("{bucket}", "<bucket>").replace(
            "{buckets}", str(args.buckets)
        ),
        buckets=args.buckets,
        out_path=orbit_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        delta_mode=args.delta_mode,
        delta_max_age_hours=args.delta_max_age_hours,
    )

    write_manifest(
        manifest_path,
        entries=[
            {
                "source_name": "sbx_systems",
                "path_abs": systems_path,
                "path_rel": raw_rel_dir / "sbx_systems.csv",
                "row_count": systems_rows,
                "query_signature": systems_query_template.replace("{bucket}", "<bucket>").replace(
                    "{buckets}", str(args.buckets)
                ),
                "delta_update": {"mode": args.delta_mode, "max_age_hours": args.delta_max_age_hours, **systems_stats},
            },
            {
                "source_name": "sbx_alias",
                "path_abs": alias_path,
                "path_rel": raw_rel_dir / "sbx_alias.csv",
                "row_count": alias_rows,
                "query_signature": alias_query_template.replace("{bucket}", "<bucket>").replace(
                    "{buckets}", str(args.buckets)
                ),
                "delta_update": {"mode": args.delta_mode, "max_age_hours": args.delta_max_age_hours, **alias_stats},
            },
            {
                "source_name": "sbx_configurations",
                "path_abs": config_path,
                "path_rel": raw_rel_dir / "sbx_configurations.csv",
                "row_count": config_rows,
                "query_signature": config_query_template.replace("{bucket}", "<bucket>").replace(
                    "{buckets}", str(args.buckets)
                ),
                "delta_update": {"mode": args.delta_mode, "max_age_hours": args.delta_max_age_hours, **config_stats},
            },
            {
                "source_name": "sbx_orbits",
                "path_abs": orbit_path,
                "path_rel": raw_rel_dir / "sbx_orbits.csv",
                "row_count": orbit_rows,
                "query_signature": orbit_query_template.replace("{bucket}", "<bucket>").replace(
                    "{buckets}", str(args.buckets)
                ),
                "delta_update": {"mode": args.delta_mode, "max_age_hours": args.delta_max_age_hours, **orbit_stats},
            },
        ],
        source_version=source_version,
    )

    log(
        "SBX fetch complete "
        f"(systems={systems_rows:,}, alias={alias_rows:,}, configurations={config_rows:,}, orbits={orbit_rows:,}, "
        f"manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
