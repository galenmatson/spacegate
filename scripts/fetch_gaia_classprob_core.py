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
DEFAULT_BUCKETS = 211
DEFAULT_TIMEOUT_S = 360
DEFAULT_RETRIES = 6
DEFAULT_MAX_REC = 500000
DEFAULT_COUNT_TIMEOUT_S = 900
DEFAULT_DELTA_MODE = "resume"
DEFAULT_DELTA_MAX_AGE_HOURS = 24.0 * 30.0
GAIA_CLASSPROB_VERSION = "dr3_astrophysical_parameters_parallax_gte_3.26156"


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
                f"Gaia TAP retry {attempt}/{retries - 1} failed: "
                f"{type(exc).__name__}: {exc}; sleeping {sleep_s}s"
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"Gaia TAP query failed after {retries} attempts: {last_exc}")


def tap_query_count(adql: str, timeout_s: int, retries: int) -> int:
    # Count queries are scalar and do not require large MAXREC values.
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
    min_parallax_mas: float,
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
        f"{min_parallax_mas}|{buckets}|{max_rec}".encode("utf-8")
    ).hexdigest()[:12]
    parts_dir = out_path.parent / f"{out_path.stem}.parts.{query_tag}"
    parts_dir.mkdir(parents=True, exist_ok=True)
    max_age_s = delta_max_age_hours * 3600.0

    min_parallax = f"{min_parallax_mas:.8f}"
    expected_fields: list[str] | None = None
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
                    log(f"gaia_dr3_classprob: bucket {bucket + 1}/{buckets} reuse empty marker")
                    continue
        elif part_path.exists() or empty_marker_path.exists():
            refresh_reason = "refresh"

        if not should_fetch:
            part_header = csv_header(part_path)
            if not part_header:
                raise RuntimeError(f"gaia_dr3_classprob: empty header in {part_path}")
            if expected_fields is None:
                expected_fields = part_header
            elif part_header != expected_fields:
                raise RuntimeError(
                    "gaia_dr3_classprob: schema mismatch in existing part "
                    f"{part_path.name}: {part_header} != {expected_fields}"
                )
            part_rows = csv_row_count(part_path)
            log(f"gaia_dr3_classprob: bucket {bucket + 1}/{buckets} resume rows={part_rows:,}")
            bucket_stats["buckets_reused"] += 1
            continue

        if part_path.exists():
            part_path.unlink()
        if empty_marker_path.exists():
            empty_marker_path.unlink()
        if refresh_reason != "missing":
            bucket_stats["buckets_refreshed"] += 1
            log(
                f"gaia_dr3_classprob: bucket {bucket + 1}/{buckets} refreshing ({refresh_reason})"
            )

        adql = f"""
        select
          ap.source_id,
          ap.classprob_dsc_combmod_whitedwarf,
          ap.classprob_dsc_specmod_whitedwarf,
          ap.classprob_dsc_combmod_star,
          ap.classprob_dsc_specmod_star,
          ap.classprob_dsc_combmod_binarystar,
          ap.classprob_dsc_specmod_binarystar,
          ap.classprob_dsc_combmod_galaxy,
          ap.classprob_dsc_specmod_galaxy,
          ap.classprob_dsc_combmod_quasar,
          ap.classprob_dsc_specmod_quasar
        from gaiadr3.astrophysical_parameters ap
        join gaiadr3.gaia_source gs on gs.source_id = ap.source_id
        where gs.parallax >= {min_parallax}
          and mod(gs.source_id, {buckets}) = {bucket}
        """
        text = tap_query_csv(adql, timeout_s=timeout_s, retries=retries, max_rec=max_rec)
        bucket_stats["buckets_fetched"] += 1
        reader = csv.DictReader(StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            log(f"gaia_dr3_classprob: bucket {bucket + 1}/{buckets} returned no header/rows")
            empty_marker_path.write_text(
                f"bucket={bucket} empty=true checked_at={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n",
                encoding="utf-8",
            )
            continue
        if expected_fields is None:
            expected_fields = fieldnames
        elif fieldnames != expected_fields:
            raise RuntimeError(
                "gaia_dr3_classprob: schema mismatch in bucket "
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
        if bucket_rows >= max_rec:
            raise RuntimeError(
                "gaia_dr3_classprob: bucket reached MAXREC guard "
                f"(bucket={bucket + 1}/{buckets}, rows={bucket_rows:,}, max_rec={max_rec:,}). "
                "Increase --max-rec and/or --buckets to avoid truncation."
            )
        tmp_part_path.replace(part_path)
        log(f"gaia_dr3_classprob: bucket {bucket + 1}/{buckets} rows={bucket_rows:,}")

    if expected_fields is None:
        raise RuntimeError("gaia_dr3_classprob: no data returned from Gaia TAP")

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
                        "gaia_dr3_classprob: schema mismatch while stitching "
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
            "source_name": "gaia_dr3_astrophysical_classprob",
            "source_version": GAIA_CLASSPROB_VERSION,
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
        description="Fetch Gaia DR3 classifier probabilities for Spacegate ingest."
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
    if args.count_timeout_s < 1:
        raise SystemExit("--count-timeout-s must be >= 1")
    if args.delta_mode == "delta" and args.delta_max_age_hours <= 0:
        raise SystemExit("--delta-max-age-hours must be > 0 in --delta-mode delta")

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    raw_dir = state_dir / "raw" / "gaia_classprob"
    manifest_path = state_dir / "reports" / "manifests" / "gaia_classprob_manifest.json"
    out_path = raw_dir / "gaia_dr3_astrophysical_classprob.csv"

    log(
        "Gaia classifier fetch start "
        f"(min_parallax_mas={args.min_parallax_mas}, buckets={args.buckets}, timeout_s={args.timeout_s}, "
        f"delta_mode={args.delta_mode}, delta_max_age_hours={args.delta_max_age_hours})"
    )
    row_count, bucket_stats = write_partitioned_csv(
        min_parallax_mas=args.min_parallax_mas,
        buckets=args.buckets,
        out_path=out_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        max_rec=args.max_rec,
        delta_mode=args.delta_mode,
        delta_max_age_hours=args.delta_max_age_hours,
    )
    count_query = (
        "select count(*) as row_count "
        "from gaiadr3.astrophysical_parameters ap "
        "join gaiadr3.gaia_source gs on gs.source_id = ap.source_id "
        f"where gs.parallax >= {args.min_parallax_mas:.8f}"
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
                "Gaia classifier completeness check failed: "
                f"expected_rows={expected_row_count:,} actual_rows={row_count:,}. "
                "This suggests partial/truncated retrieval; rerun with --delta-mode refresh."
            )
        log(
            "Gaia classifier completeness check passed "
            f"(expected_rows={expected_row_count:,}, actual_rows={row_count:,})"
        )

    write_manifest(
        manifest_path,
        out_path_abs=out_path,
        out_path_rel=Path("raw/gaia_classprob/gaia_dr3_astrophysical_classprob.csv"),
        query_signature=(
            "SELECT <classprob fields> FROM gaiadr3.astrophysical_parameters ap "
            "JOIN gaiadr3.gaia_source gs ON gs.source_id = ap.source_id "
            f"WHERE gs.parallax >= {args.min_parallax_mas:.8f} AND MOD(gs.source_id, {args.buckets}) = <bucket>"
        ),
        count_query=count_query if not args.skip_count_check else None,
        expected_row_count=expected_row_count,
        row_count_match=row_count_match,
        row_count=row_count,
        delta_update={
            "mode": args.delta_mode,
            "max_age_hours": args.delta_max_age_hours,
            **bucket_stats,
        },
    )
    log(
        "Gaia classifier fetch complete "
        f"(rows={row_count:,}, bytes={out_path.stat().st_size:,}, fetched={bucket_stats['buckets_fetched']}, "
        f"reused={bucket_stats['buckets_reused']}, reused_empty={bucket_stats['buckets_reused_empty']}, "
        f"refreshed={bucket_stats['buckets_refreshed']}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
