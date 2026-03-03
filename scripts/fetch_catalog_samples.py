#!/usr/bin/env python3
import argparse
import csv
import gzip
import io
import json
import math
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb

from catalog_eval import default_state_dir, init_env


GAIA_TAP_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
WDS_URL = "https://astro.gsu.edu/wds/wdsweb_summ2.txt"


def http_get(url: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def gaia_query_url(adql: str) -> str:
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "csv",
        "QUERY": adql,
    }
    return GAIA_TAP_SYNC_URL + "?" + urllib.parse.urlencode(params)


def write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def core_gaia_ids(core_db_path: Path, limit: int, seed: str) -> list[int]:
    con = duckdb.connect(str(core_db_path), read_only=True)
    rows = con.execute(
        f"""
        select gaia_id
        from (
          select distinct gaia_id
          from stars
          where gaia_id is not null
        )
        order by md5(cast(gaia_id as varchar) || ?)
        limit ?
        """,
        [seed, limit],
    ).fetchall()
    return [int(row[0]) for row in rows]


def fetch_gaia_samples(state_dir: Path, sample_size: int, seed: str) -> dict:
    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core database for Gaia overlap sample: {core_db_path}")

    output_dir = state_dir / "cooked" / "gaia_dr3_sample"
    output_dir.mkdir(parents=True, exist_ok=True)

    overlap_ids = core_gaia_ids(core_db_path, sample_size, seed)
    if not overlap_ids:
        raise SystemExit("No Gaia IDs found in current core database.")

    select_fields = (
        "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,radial_velocity,"
        "phot_g_mean_mag,bp_rp,teff_gspphot,logg_gspphot,mh_gspphot,non_single_star"
    )
    overlap_query = (
        f"select {select_fields} from gaiadr3.gaia_source "
        f"where source_id in ({','.join(str(value) for value in overlap_ids)})"
    )
    random_query = (
        f"select top {sample_size} {select_fields} from gaiadr3.gaia_source "
        "where parallax >= 3.26156 order by source_id"
    )

    overlap_csv = http_get(gaia_query_url(overlap_query))
    random_csv = http_get(gaia_query_url(random_query))

    overlap_path = output_dir / "gaia_dr3_overlap_sample.csv"
    random_path = output_dir / "gaia_dr3_random_sample.csv"
    write_bytes(overlap_path, overlap_csv)
    write_bytes(random_path, random_csv)

    combined_path = output_dir / "gaia_dr3_sample.csv"
    with combined_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = None
        seen_ids: set[str] = set()
        for sample_origin, path in (("overlap", overlap_path), ("random", random_path)):
            with path.open(newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)
                fieldnames = list(reader.fieldnames or []) + ["sample_origin"]
                if writer is None:
                    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                    writer.writeheader()
                for row in reader:
                    source_id = (row.get("source_id") or "").strip()
                    if not source_id or source_id in seen_ids:
                        continue
                    seen_ids.add(source_id)
                    row["sample_origin"] = sample_origin
                    writer.writerow(row)

    return {
        "combined_path": str(combined_path),
        "random_path": str(random_path),
        "overlap_path": str(overlap_path),
        "row_count_combined": len(seen_ids),
        "query_random": random_query,
        "query_overlap_count": len(overlap_ids),
    }


def parse_wds_coord(value: str) -> tuple[float | None, float | None]:
    coord = value.strip()
    if len(coord) < 16:
        return None, None
    sign_index = max(coord.find("+"), coord.find("-"))
    if sign_index < 0:
        return None, None
    ra_text = coord[:sign_index]
    dec_text = coord[sign_index:]
    if len(ra_text) < 6 or len(dec_text) < 7:
        return None, None
    try:
        ra_h = int(ra_text[0:2])
        ra_m = int(ra_text[2:4])
        ra_s = float(ra_text[4:])
        dec_sign = -1.0 if dec_text[0] == "-" else 1.0
        dec_d = int(dec_text[1:3])
        dec_m = int(dec_text[3:5])
        dec_s = float(dec_text[5:])
    except ValueError:
        return None, None
    ra_deg = (ra_h + ra_m / 60.0 + ra_s / 3600.0) * 15.0
    dec_deg = dec_sign * (dec_d + dec_m / 60.0 + dec_s / 3600.0)
    return ra_deg, dec_deg


def parse_wds_pm(value: str) -> tuple[float | None, float | None]:
    text = value.strip()
    if len(text) < 8:
        return None, None
    try:
        ra_pm = int(text[0:4]) / 1000.0
        dec_pm = int(text[4:8]) / 1000.0
    except ValueError:
        return None, None
    return ra_pm, dec_pm


def fetch_wds_sample(state_dir: Path) -> dict:
    raw_path = state_dir / "raw" / "wds" / "wdsweb_summ2.txt"
    cooked_path = state_dir / "cooked" / "wds" / "wds_summary.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    cooked_path.parent.mkdir(parents=True, exist_ok=True)

    payload = http_get(WDS_URL)
    write_bytes(raw_path, payload)

    line_count = 0
    with raw_path.open("r", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "wds_id",
                "discoverer",
                "component",
                "first_year",
                "last_year",
                "obs_count",
                "theta_first_deg",
                "theta_last_deg",
                "rho_first_arcsec",
                "rho_last_arcsec",
                "mag_primary",
                "mag_secondary",
                "spectral_type_raw",
                "pm_primary_ra",
                "pm_primary_dec",
                "pm_secondary_ra",
                "pm_secondary_dec",
                "dm_designation",
                "note",
                "precise_coordinate",
                "ra_deg",
                "dec_deg",
            ],
        )
        writer.writeheader()
        for raw_line in in_f:
            line = raw_line.rstrip("\n")
            if not line or line.startswith("<pre>") or line.startswith("</pre>"):
                continue
            if line.startswith("    WDS") or line.startswith("Identifier") or set(line.strip()) == {"-"}:
                continue
            if len(line) < 120:
                continue
            precise_coordinate = line[112:].strip()
            ra_deg, dec_deg = parse_wds_coord(precise_coordinate)
            pm_primary_ra, pm_primary_dec = parse_wds_pm(line[78:86])
            pm_secondary_ra, pm_secondary_dec = parse_wds_pm(line[86:94])
            writer.writerow(
                {
                    "wds_id": line[0:10].strip(),
                    "discoverer": line[10:17].strip(),
                    "component": line[17:22].strip(),
                    "first_year": line[22:27].strip(),
                    "last_year": line[27:32].strip(),
                    "obs_count": line[32:37].strip(),
                    "theta_first_deg": line[37:41].strip(),
                    "theta_last_deg": line[41:45].strip(),
                    "rho_first_arcsec": line[45:51].strip(),
                    "rho_last_arcsec": line[51:57].strip(),
                    "mag_primary": line[57:63].strip(),
                    "mag_secondary": line[63:69].strip(),
                    "spectral_type_raw": line[69:79].strip(),
                    "pm_primary_ra": pm_primary_ra,
                    "pm_primary_dec": pm_primary_dec,
                    "pm_secondary_ra": pm_secondary_ra,
                    "pm_secondary_dec": pm_secondary_dec,
                    "dm_designation": line[94:107].strip(),
                    "note": line[107:112].strip(),
                    "precise_coordinate": precise_coordinate,
                    "ra_deg": ra_deg,
                    "dec_deg": dec_deg,
                }
            )
            line_count += 1

    return {
        "raw_path": str(raw_path),
        "cooked_path": str(cooked_path),
        "row_count": line_count,
        "source_url": WDS_URL,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch sample inputs for catalog quality evaluation."
    )
    parser.add_argument(
        "--catalog",
        action="append",
        choices=["gaia_dr3_sample", "wds"],
        help="Catalog sample to fetch. Defaults to both.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Number of Gaia rows to request for random/overlap samples (default: 100).",
    )
    parser.add_argument(
        "--seed",
        default="spacegate-v1-2",
        help="Deterministic seed used for Gaia overlap ID selection.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    init_env(root)
    state_dir = default_state_dir(root)
    selected = args.catalog or ["gaia_dr3_sample", "wds"]

    summary = {}
    if "gaia_dr3_sample" in selected:
        summary["gaia_dr3_sample"] = fetch_gaia_samples(state_dir, args.sample_size, args.seed)
    if "wds" in selected:
        summary["wds"] = fetch_wds_sample(state_dir)

    summary_path = state_dir / "reports" / "catalog_eval_inputs" / "latest_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(str(summary_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
