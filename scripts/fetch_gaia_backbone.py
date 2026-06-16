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

GAIA_TAP_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
DEFAULT_MIN_PARALLAX_MAS = 3.26156
DEFAULT_BUCKETS = 53
DEFAULT_TIMEOUT_S = 240
DEFAULT_RETRIES = 4
DEFAULT_MAX_REC = 500000
DEFAULT_COUNT_TIMEOUT_S = 600
DEFAULT_DELTA_MODE = "resume"
DEFAULT_DELTA_MAX_AGE_HOURS = 24.0 * 30.0
GAIA_BACKBONE_VERSION = "dr3_gaia_source_parallax_gte_3.26156"


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def tap_query_csv(adql: str, timeout_s: int, retries: int, max_rec: int) -> str:
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "csv",
        "MAXREC": str(max_rec),
        "QUERY": adql,
    }
    url = GAIA_TAP_SYNC_URL + "?" + urllib.parse.urlencode(params)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read()
            text = payload.decode("utf-8", errors="replace")
            low = text.lstrip().lower()
            if low.startswith("error") or "usagefault" in low:
                raise RuntimeError(f"Gaia TAP returned error payload: {text[:300]}")
            return text
        except Exception as exc:  # pragma: no cover - network error path
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = min(2**attempt, 20)
            log(
                f"Gaia TAP retry {attempt}/{retries - 1} failed: {type(exc).__name__}: {exc}; sleeping {sleep_s}s"
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"Gaia TAP query failed after {retries} attempts: {last_exc}")


def tap_query_count(adql: str, timeout_s: int, retries: int) -> int:
    text = tap_query_csv(adql, timeout_s=timeout_s, retries=retries, max_rec=10_000)
    reader = csv.DictReader(StringIO(text))
    row = next(reader, None)
    if row is None:
        raise RuntimeError("Gaia TAP count query returned no rows")
    value = None
    if "row_count" in row:
        value = row.get("row_count")
    elif row:
        value = next(iter(row.values()))
    if value is None:
        raise RuntimeError("Gaia TAP count query returned empty value")
    try:
        return int(float(str(value).strip()))
    except ValueError as exc:
        raise RuntimeError(f"Invalid Gaia TAP count value: {value!r}") from exc


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
    select_fields: str,
    where_clause: str,
    buckets: int,
    out_path: Path,
    timeout_s: int,
    retries: int,
    max_rec: int,
    delta_mode: str,
    delta_max_age_hours: float,
) -> tuple[int, dict[str, int]]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    query_tag = hashlib.sha1(
        f"{select_fields}|{where_clause}|{buckets}|{max_rec}".encode("utf-8")
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
                    log(
                        f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} reuse empty marker"
                    )
                    continue
        elif part_path.exists() or empty_marker_path.exists():
            refresh_reason = "refresh"

        if not should_fetch:
            part_header = csv_header(part_path)
            if not part_header:
                raise RuntimeError(f"gaia_dr3_backbone: empty header in {part_path}")
            if expected_fields is None:
                expected_fields = part_header
            elif part_header != expected_fields:
                raise RuntimeError(
                    "gaia_dr3_backbone: schema mismatch in existing part "
                    f"{part_path.name}: {part_header} != {expected_fields}"
                )
            part_rows = csv_row_count(part_path)
            log(
                f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} resume rows={part_rows:,}"
            )
            bucket_stats["buckets_reused"] += 1
            continue

        if part_path.exists():
            part_path.unlink()
        if empty_marker_path.exists():
            empty_marker_path.unlink()
        if refresh_reason != "missing":
            bucket_stats["buckets_refreshed"] += 1
            log(
                f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} refreshing ({refresh_reason})"
            )

        adql = (
            f"select {select_fields} from gaiadr3.gaia_source "
            f"where {where_clause} and mod(source_id, {buckets}) = {bucket} "
        )
        text = tap_query_csv(adql, timeout_s=timeout_s, retries=retries, max_rec=max_rec)
        bucket_stats["buckets_fetched"] += 1
        reader = csv.DictReader(StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            log(f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} returned no header/rows")
            empty_marker_path.write_text(
                f"bucket={bucket} empty=true checked_at={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n",
                encoding="utf-8",
            )
            continue
        if expected_fields is None:
            expected_fields = fieldnames
        elif fieldnames != expected_fields:
            raise RuntimeError(
                "gaia_dr3_backbone: schema mismatch in bucket "
                f"{bucket + 1}: {fieldnames} != {expected_fields}"
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
        log(
            f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} rows={bucket_rows:,}"
        )

    if expected_fields is None:
        raise RuntimeError("gaia_dr3_backbone: no data returned from Gaia TAP")

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
                        "gaia_dr3_backbone: schema mismatch while stitching "
                        f"{part_path.name}: {part_fields} != {expected_fields}"
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
    *,
    out_path_abs: Path,
    out_path_rel: Path,
    query_signature: str,
    count_query: str | None,
    expected_row_count: int | None,
    row_count_match: bool | None,
    row_count: int,
    delta_update: dict[str, object] | None = None,
) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    entries = [
        {
            "source_name": "gaia_dr3_backbone",
            "source_version": GAIA_BACKBONE_VERSION,
            "url": GAIA_TAP_SYNC_URL,
            "dest_path": str(out_path_rel),
            "retrieved_at": ts,
            "checked_at": ts,
            "sha256": file_sha256(out_path_abs),
            "bytes_written": out_path_abs.stat().st_size,
            "row_count": row_count,
            "query_signature": query_signature,
            "count_query": count_query,
            "expected_row_count": expected_row_count,
            "row_count_match": row_count_match,
            "delta_update": delta_update or {},
        }
    ]
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Gaia DR3 canonical backbone input for Spacegate core ingest."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--min-parallax-mas", type=float, default=DEFAULT_MIN_PARALLAX_MAS)
    parser.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS)
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--count-timeout-s", type=int, default=DEFAULT_COUNT_TIMEOUT_S)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-rec", type=int, default=DEFAULT_MAX_REC)
    parser.add_argument(
        "--skip-count-check",
        action="store_true",
        help="Skip Gaia TAP expected-row count verification (not recommended).",
    )
    parser.add_argument(
        "--delta-mode",
        choices=["resume", "delta", "refresh"],
        default=DEFAULT_DELTA_MODE,
        help="resume=always reuse bucket parts, delta=refresh stale buckets only, refresh=refetch all buckets.",
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
    if args.min_parallax_mas <= 0:
        raise SystemExit("--min-parallax-mas must be > 0")
    if args.max_rec < 1:
        raise SystemExit("--max-rec must be >= 1")
    if args.delta_mode == "delta" and args.delta_max_age_hours <= 0:
        raise SystemExit("--delta-max-age-hours must be > 0 in --delta-mode delta")
    if args.count_timeout_s < 1:
        raise SystemExit("--count-timeout-s must be >= 1")

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    raw_dir = state_dir / "raw" / "gaia_backbone"
    manifest_path = state_dir / "reports" / "manifests" / "gaia_backbone_manifest.json"
    out_path = raw_dir / "gaia_dr3_backbone.csv"

    min_parallax = f"{args.min_parallax_mas:.8f}"
    select_fields = ",".join(
        [
            "source_id",
            "ref_epoch",
            "ra as ra_deg",
            "ra_error as ra_error_mas",
            "dec as dec_deg",
            "dec_error as dec_error_mas",
            "parallax as parallax_mas",
            "parallax_error as parallax_error_mas",
            "parallax_over_error",
            "pmra as pm_ra_mas_yr",
            "pmra_error as pm_ra_error_mas_yr",
            "pmdec as pm_dec_mas_yr",
            "pmdec_error as pm_dec_error_mas_yr",
            "radial_velocity as radial_velocity_kms",
            "radial_velocity_error as radial_velocity_error_kms",
            "ruwe",
            "phot_g_mean_mag as phot_g_mag",
            "phot_bp_mean_mag as phot_bp_mag",
            "phot_rp_mean_mag as phot_rp_mag",
            "bp_rp",
            "bp_g",
            "g_rp",
            "teff_gspphot",
            "teff_gspphot_lower",
            "teff_gspphot_upper",
            "logg_gspphot",
            "logg_gspphot_lower",
            "logg_gspphot_upper",
            "mh_gspphot",
            "mh_gspphot_lower",
            "mh_gspphot_upper",
            "distance_gspphot",
            "distance_gspphot_lower",
            "distance_gspphot_upper",
            "non_single_star",
            "has_xp_continuous",
            "has_xp_sampled",
            "has_rvs",
            "visibility_periods_used",
            "astrometric_params_solved",
            "duplicated_source",
        ]
    )
    where_clause = f"parallax >= {min_parallax}"
    count_query = f"select count(*) as row_count from gaiadr3.gaia_source where {where_clause}"

    log(
        "Gaia backbone fetch start "
        f"(min_parallax_mas={args.min_parallax_mas}, buckets={args.buckets}, timeout_s={args.timeout_s}, "
        f"max_rec={args.max_rec}, delta_mode={args.delta_mode}, delta_max_age_hours={args.delta_max_age_hours})"
    )
    row_count, bucket_stats = write_partitioned_csv(
        select_fields=select_fields,
        where_clause=where_clause,
        buckets=args.buckets,
        out_path=out_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        max_rec=args.max_rec,
        delta_mode=args.delta_mode,
        delta_max_age_hours=args.delta_max_age_hours,
    )
    expected_row_count: int | None = None
    row_count_match: bool | None = None
    if not args.skip_count_check:
        expected_row_count = tap_query_count(
            count_query, timeout_s=args.count_timeout_s, retries=args.retries
        )
        row_count_match = row_count == expected_row_count
        if not row_count_match:
            raise SystemExit(
                "Gaia backbone completeness check failed: "
                f"expected_rows={expected_row_count:,} actual_rows={row_count:,}. "
                "This suggests partial/truncated retrieval; rerun with --delta-mode refresh or increase buckets/timeouts."
            )
        log(
            "Gaia backbone completeness check passed "
            f"(expected_rows={expected_row_count:,}, actual_rows={row_count:,})"
        )

    write_manifest(
        manifest_path,
        out_path_abs=out_path,
        out_path_rel=Path("raw/gaia_backbone/gaia_dr3_backbone.csv"),
        query_signature=(
            f"SELECT {select_fields} FROM gaiadr3.gaia_source "
            f"WHERE {where_clause} AND MOD(source_id, {args.buckets}) = <bucket>"
        ),
        count_query=count_query if not args.skip_count_check else None,
        expected_row_count=expected_row_count,
        row_count_match=row_count_match,
        row_count=row_count,
        delta_update={
            "mode": args.delta_mode,
            "max_age_hours": args.delta_max_age_hours,
            "max_rec": args.max_rec,
            **bucket_stats,
        },
    )
    log(
        "Gaia backbone fetch complete "
        f"(rows={row_count:,}, bytes={out_path.stat().st_size:,}, fetched={bucket_stats['buckets_fetched']}, "
        f"reused={bucket_stats['buckets_reused']}, reused_empty={bucket_stats['buckets_reused_empty']}, "
        f"refreshed={bucket_stats['buckets_refreshed']}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
