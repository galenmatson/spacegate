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


def write_partitioned_csv(
    *,
    min_parallax_mas: float,
    buckets: int,
    out_path: Path,
    timeout_s: int,
    retries: int,
    max_rec: int,
) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    query_tag = hashlib.sha1(
        f"{min_parallax_mas}|{buckets}|{max_rec}".encode("utf-8")
    ).hexdigest()[:12]
    parts_dir = out_path.parent / f"{out_path.stem}.parts.{query_tag}"
    parts_dir.mkdir(parents=True, exist_ok=True)

    min_parallax = f"{min_parallax_mas:.8f}"
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
            continue

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
        reader = csv.DictReader(StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            log(f"gaia_dr3_classprob: bucket {bucket + 1}/{buckets} returned no header/rows")
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
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-rec", type=int, default=DEFAULT_MAX_REC)
    args = parser.parse_args()

    if args.buckets < 1:
        raise SystemExit("--buckets must be >= 1")
    if args.min_parallax_mas <= 0:
        raise SystemExit("--min-parallax-mas must be > 0")
    if args.max_rec < 1:
        raise SystemExit("--max-rec must be >= 1")

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
        f"(min_parallax_mas={args.min_parallax_mas}, buckets={args.buckets}, timeout_s={args.timeout_s})"
    )
    row_count = write_partitioned_csv(
        min_parallax_mas=args.min_parallax_mas,
        buckets=args.buckets,
        out_path=out_path,
        timeout_s=args.timeout_s,
        retries=args.retries,
        max_rec=args.max_rec,
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
        row_count=row_count,
    )
    log(
        "Gaia classifier fetch complete "
        f"(rows={row_count:,}, bytes={out_path.stat().st_size:,}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
