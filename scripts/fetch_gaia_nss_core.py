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
DEFAULT_TIMEOUT_S = 360
DEFAULT_RETRIES = 6


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def tap_query_csv(adql: str, timeout_s: int, retries: int) -> str:
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "csv",
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


def write_partitioned_csv(
    *,
    label: str,
    select_fields: str,
    table_name: str,
    where_clause: str,
    buckets: int,
    out_path: Path,
    timeout_s: int,
    retries: int,
) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    query_tag = hashlib.sha1(
        f"{label}|{select_fields}|{table_name}|{where_clause}|{buckets}".encode("utf-8")
    ).hexdigest()[:12]
    parts_dir = out_path.parent / f"{out_path.stem}.parts.{query_tag}"
    parts_dir.mkdir(parents=True, exist_ok=True)
    expected_fields: list[str] | None = None

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
        if part_path.exists() and part_path.stat().st_size > 0:
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
            continue

        adql = (
            f"select {select_fields} from {table_name} "
            f"where {where_clause} and mod(source_id, {buckets}) = {bucket} "
        )
        text = tap_query_csv(adql, timeout_s=timeout_s, retries=retries)
        reader = csv.DictReader(StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            log(f"{label}: bucket {bucket + 1}/{buckets} returned no header/rows")
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
        raise RuntimeError(f"{label}: no data returned from Gaia TAP")

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
    return total_rows


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
    non_single_path_abs: Path,
    non_single_path_rel: Path,
    non_single_query: str,
    non_single_rows: int,
    two_body_path_abs: Path,
    two_body_path_rel: Path,
    two_body_query: str,
    two_body_rows: int,
) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    entries = [
        {
            "source_name": "gaia_dr3_non_single_star",
            "url": GAIA_TAP_SYNC_URL,
            "dest_path": str(non_single_path_rel),
            "retrieved_at": ts,
            "checked_at": ts,
            "sha256": file_sha256(non_single_path_abs),
            "bytes_written": non_single_path_abs.stat().st_size,
            "row_count": non_single_rows,
            "query_signature": non_single_query,
        },
        {
            "source_name": "gaia_dr3_nss_two_body_orbit",
            "url": GAIA_TAP_SYNC_URL,
            "dest_path": str(two_body_path_rel),
            "retrieved_at": ts,
            "checked_at": ts,
            "sha256": file_sha256(two_body_path_abs),
            "bytes_written": two_body_path_abs.stat().st_size,
            "row_count": two_body_rows,
            "query_signature": two_body_query,
        },
    ]
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Gaia DR3 NSS inputs for Spacegate core ingest."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--min-parallax-mas", type=float, default=DEFAULT_MIN_PARALLAX_MAS)
    parser.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS)
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    args = parser.parse_args()

    if args.buckets < 1:
        raise SystemExit("--buckets must be >= 1")
    if args.min_parallax_mas <= 0:
        raise SystemExit("--min-parallax-mas must be > 0")

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    raw_dir = state_dir / "raw" / "gaia_nss"
    manifest_path = state_dir / "reports" / "manifests" / "gaia_nss_manifest.json"
    non_single_path = raw_dir / "gaia_dr3_non_single_star.csv"
    two_body_path = raw_dir / "gaia_dr3_nss_two_body_orbit.csv"

    min_parallax = f"{args.min_parallax_mas:.8f}"
    non_single_fields = (
        "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,radial_velocity,non_single_star"
    )
    non_single_where = f"non_single_star = 1 and parallax >= {min_parallax}"
    two_body_fields = (
        "source_id,nss_solution_type,ra,dec,parallax,parallax_error,pmra,pmdec,period,eccentricity,"
        "center_of_mass_velocity,semi_amplitude_primary,mass_ratio,inclination,flags,significance"
    )
    two_body_where = f"parallax >= {min_parallax}"

    log(
        "Gaia NSS fetch start "
        f"(min_parallax_mas={args.min_parallax_mas}, buckets={args.buckets}, timeout_s={args.timeout_s})"
    )

    non_single_rows = write_partitioned_csv(
        label="gaia_dr3_non_single_star",
        select_fields=non_single_fields,
        table_name="gaiadr3.gaia_source",
        where_clause=non_single_where,
        buckets=args.buckets,
        out_path=non_single_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
    )
    two_body_rows = write_partitioned_csv(
        label="gaia_dr3_nss_two_body_orbit",
        select_fields=two_body_fields,
        table_name="gaiadr3.nss_two_body_orbit",
        where_clause=two_body_where,
        buckets=args.buckets,
        out_path=two_body_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
    )

    write_manifest(
        manifest_path,
        non_single_path_abs=non_single_path,
        non_single_path_rel=Path("raw/gaia_nss/gaia_dr3_non_single_star.csv"),
        non_single_query=(
            f"SELECT {non_single_fields} FROM gaiadr3.gaia_source "
            f"WHERE {non_single_where} AND MOD(source_id, {args.buckets}) = <bucket>"
        ),
        non_single_rows=non_single_rows,
        two_body_path_abs=two_body_path,
        two_body_path_rel=Path("raw/gaia_nss/gaia_dr3_nss_two_body_orbit.csv"),
        two_body_query=(
            f"SELECT {two_body_fields} FROM gaiadr3.nss_two_body_orbit "
            f"WHERE {two_body_where} AND MOD(source_id, {args.buckets}) = <bucket>"
        ),
        two_body_rows=two_body_rows,
    )
    log(
        "Gaia NSS fetch complete "
        f"(non_single_rows={non_single_rows:,}, two_body_rows={two_body_rows:,}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
