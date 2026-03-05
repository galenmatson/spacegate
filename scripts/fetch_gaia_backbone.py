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
GAIA_BACKBONE_VERSION = "dr3_gaia_source_parallax_gte_3.26156"


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
    select_fields: str,
    where_clause: str,
    buckets: int,
    out_path: Path,
    timeout_s: int,
    retries: int,
) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    expected_fields: list[str] | None = None
    with tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        encoding="utf-8",
        delete=False,
        dir=str(out_path.parent),
        prefix=out_path.name + ".tmp.",
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        writer: csv.DictWriter | None = None
        for bucket in range(buckets):
            adql = (
                f"select {select_fields} from gaiadr3.gaia_source "
                f"where {where_clause} and mod(source_id, {buckets}) = {bucket} "
            )
            text = tap_query_csv(adql, timeout_s=timeout_s, retries=retries)
            reader = csv.DictReader(StringIO(text))
            fieldnames = list(reader.fieldnames or [])
            if not fieldnames:
                log(f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} returned no header/rows")
                continue
            if expected_fields is None:
                expected_fields = fieldnames
                writer = csv.DictWriter(tmp_file, fieldnames=expected_fields)
                writer.writeheader()
            elif fieldnames != expected_fields:
                raise RuntimeError(
                    "gaia_dr3_backbone: schema mismatch in bucket "
                    f"{bucket + 1}: {fieldnames} != {expected_fields}"
                )

            bucket_rows = 0
            for row in reader:
                if writer is None:
                    raise RuntimeError("gaia_dr3_backbone: writer not initialized")
                writer.writerow(row)
                total_rows += 1
                bucket_rows += 1
            log(
                f"gaia_dr3_backbone: bucket {bucket + 1}/{buckets} rows={bucket_rows:,} total={total_rows:,}"
            )
    tmp_path.replace(out_path)
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
    out_path_abs: Path,
    out_path_rel: Path,
    query_signature: str,
    row_count: int,
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

    log(
        "Gaia backbone fetch start "
        f"(min_parallax_mas={args.min_parallax_mas}, buckets={args.buckets}, timeout_s={args.timeout_s})"
    )
    row_count = write_partitioned_csv(
        select_fields=select_fields,
        where_clause=where_clause,
        buckets=args.buckets,
        out_path=out_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
    )

    write_manifest(
        manifest_path,
        out_path_abs=out_path,
        out_path_rel=Path("raw/gaia_backbone/gaia_dr3_backbone.csv"),
        query_signature=(
            f"SELECT {select_fields} FROM gaiadr3.gaia_source "
            f"WHERE {where_clause} AND MOD(source_id, {args.buckets}) = <bucket>"
        ),
        row_count=row_count,
    )
    log(
        "Gaia backbone fetch complete "
        f"(rows={row_count:,}, bytes={out_path.stat().st_size:,}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
