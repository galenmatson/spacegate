#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

XMATCH_URL = "https://cdsxmatch.u-strasbg.fr/xmatch/api/v1/sync"
CAT1_DEFAULT = "vizier:B/wds/wds"
CAT2_DEFAULT = "vizier:I/355/gaiadr3"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def count_csv_rows(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def write_manifest(
    manifest_path: Path,
    *,
    relative_dest: str,
    query_signature: dict[str, str],
    row_count: int,
    abs_path: Path,
) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload = [
        {
            "source_name": "wds_gaia_xmatch_best",
            "url": XMATCH_URL,
            "dest_path": relative_dest,
            "retrieved_at": ts,
            "checked_at": ts,
            "sha256": sha256_file(abs_path),
            "bytes_written": abs_path.stat().st_size,
            "row_count": row_count,
            "query_signature": query_signature,
        }
    ]
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch WDS<->Gaia DR3 best-match crosswalk via CDS XMatch."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--dist-max-arcsec", type=float, default=2.0)
    parser.add_argument("--max-rec", type=int, default=2_000_000)
    parser.add_argument("--selection", choices=["best", "all"], default="best")
    parser.add_argument("--cat1", default=CAT1_DEFAULT)
    parser.add_argument("--cat2", default=CAT2_DEFAULT)
    parser.add_argument("--area", choices=["allsky", "cone"], default="allsky")
    parser.add_argument("--cone-ra", type=float, default=0.0)
    parser.add_argument("--cone-dec", type=float, default=0.0)
    parser.add_argument("--cone-radius-deg", type=float, default=1.0)
    args = parser.parse_args()

    if args.dist_max_arcsec <= 0:
        raise SystemExit("--dist-max-arcsec must be > 0")
    if args.max_rec < 1:
        raise SystemExit("--max-rec must be >= 1")

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    raw_rel = "raw/wds_gaia_xmatch/wds_gaia_best.csv"
    out_path = state_dir / raw_rel
    manifest_path = state_dir / "reports" / "manifests" / "wds_gaia_xmatch_manifest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    params = {
        "request": "xmatch",
        "cat1": args.cat1,
        "cat2": args.cat2,
        "distMaxArcsec": f"{args.dist_max_arcsec:.6f}",
        "selection": args.selection,
        "responseFormat": "csv",
        "maxRec": str(args.max_rec),
        "cols1": "WDS,Comp,RAJ2000,DEJ2000,Obs2,pa2,sep2,mag1,mag2",
        "cols2": "Source,RAdeg,DEdeg,Plx,pmRA,pmDE,RUWE,Gmag,DR3Name",
        "area": args.area,
    }
    if args.area == "cone":
        params["coneRA"] = str(args.cone_ra)
        params["coneDec"] = str(args.cone_dec)
        params["coneRadiusDeg"] = str(args.cone_radius_deg)

    data = urllib.parse.urlencode(params).encode("utf-8")
    request = urllib.request.Request(
        XMATCH_URL,
        data=data,
        headers={"User-Agent": USER_AGENT},
    )

    log(
        "WDS Gaia XMatch fetch start "
        f"(cat1={args.cat1}, cat2={args.cat2}, selection={args.selection}, dist_max_arcsec={args.dist_max_arcsec}, area={args.area})"
    )
    with urllib.request.urlopen(request, timeout=600) as response:
        payload = response.read()
    out_path.write_bytes(payload)
    row_count = count_csv_rows(out_path)
    write_manifest(
        manifest_path,
        relative_dest=raw_rel,
        query_signature=params,
        row_count=row_count,
        abs_path=out_path,
    )
    log(
        f"WDS Gaia XMatch fetch complete (rows={row_count:,}, bytes={out_path.stat().st_size:,}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
