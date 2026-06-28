#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import ssl
import tarfile
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb

from catalog_eval import default_state_dir, init_env


GAIA_TAP_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
SBX_TAP_SYNC_URL = "https://astro.ulb.ac.be/sbx/tap/sync"
WDS_URL = "https://astro.gsu.edu/wds/wdsweb_summ2.txt"
PUBLIC_BASE_URL = (os.getenv("SPACEGATE_PUBLIC_BASE_URL") or "https://spacegates.org").rstrip("/")
MSC_SOURCE_URL = "https://www.ctio.noirlab.edu/~atokovin/stars/newmsc-20260619.tar.gz"
MSC_URL = os.getenv("MSC_URL") or os.getenv("SPACEGATE_MSC_MIRROR_URL") or MSC_SOURCE_URL
MSC_SAMPLE_ALLOW_INSECURE_TLS = (
    os.getenv("SPACEGATE_MSC_SAMPLE_ALLOW_INSECURE_TLS", "0").strip() == "1"
)
ORB6_URL = "https://crf.usno.navy.mil/data_products/WDS/orb6/orb6orbits.sql"


def http_get(url: str, allow_insecure_tls: bool = False) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
        },
    )
    context = None
    if allow_insecure_tls and url.startswith("https://"):
        context = ssl._create_unverified_context()
    with urllib.request.urlopen(request, timeout=120, context=context) as response:
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


def core_numeric_ids(core_db_path: Path, column: str, limit: int, seed: str) -> list[int]:
    con = duckdb.connect(str(core_db_path), read_only=True)
    rows = con.execute(
        f"""
        select {column}
        from (
          select distinct {column}
          from stars
          where {column} is not null
        )
        order by md5(cast({column} as varchar) || ?)
        limit ?
        """,
        [seed, limit],
    ).fetchall()
    return [int(row[0]) for row in rows]


def core_gaia_ids(core_db_path: Path, limit: int, seed: str) -> list[int]:
    return core_numeric_ids(core_db_path, "gaia_id", limit, seed)


def core_hip_ids(core_db_path: Path, limit: int, seed: str) -> list[int]:
    return core_numeric_ids(core_db_path, "hip_id", limit, seed)


def core_hd_ids(core_db_path: Path, limit: int, seed: str) -> list[int]:
    return core_numeric_ids(core_db_path, "hd_id", limit, seed)


def parse_float(value: str) -> float | None:
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_orb6_ra_dec(ra_text: str, dec_text: str) -> tuple[float | None, float | None]:
    ra_value = ra_text.strip()
    dec_value = dec_text.strip()
    if len(ra_value) < 6 or len(dec_value) < 7:
        return None, None
    try:
        ra_h = int(ra_value[0:2])
        ra_m = int(ra_value[2:4])
        ra_s = float(ra_value[4:])
        dec_sign = -1.0 if dec_value[0] == "-" else 1.0
        dec_d = int(dec_value[1:3])
        dec_m = int(dec_value[3:5])
        dec_s = float(dec_value[5:])
    except ValueError:
        return None, None

    ra_deg = (ra_h + ra_m / 60.0 + ra_s / 3600.0) * 15.0
    dec_deg = dec_sign * (dec_d + dec_m / 60.0 + dec_s / 3600.0)
    return ra_deg, dec_deg


def sbx_query_csv(adql: str) -> bytes:
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
        headers={
            "User-Agent": "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def adql_int_list(values: list[int]) -> str:
    return ",".join(str(value) for value in values)


def adql_string_list(values: list[int | str]) -> str:
    return ",".join("'" + str(value).replace("'", "''") + "'" for value in values)


def read_csv_rows(payload: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(payload.decode("utf-8").splitlines()))


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


def fetch_gaia_non_single_samples(state_dir: Path, sample_size: int, seed: str) -> dict:
    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core database for Gaia overlap sample: {core_db_path}")

    output_dir = state_dir / "cooked" / "gaia_dr3_non_single_sample"
    output_dir.mkdir(parents=True, exist_ok=True)

    overlap_ids = core_gaia_ids(core_db_path, sample_size, seed + "-nssflag")
    select_fields = (
        "source_id,ra,dec,parallax,parallax_error,pmra,pmdec,radial_velocity,"
        "phot_g_mean_mag,bp_rp,teff_gspphot,logg_gspphot,mh_gspphot,non_single_star"
    )
    overlap_query = (
        f"select {select_fields} from gaiadr3.gaia_source "
        f"where non_single_star = 1 and source_id in ({','.join(str(value) for value in overlap_ids)})"
    )
    random_query = (
        f"select top {sample_size} {select_fields} from gaiadr3.gaia_source "
        "where non_single_star = 1 and parallax >= 3.26156 order by source_id"
    )

    overlap_csv = http_get(gaia_query_url(overlap_query))
    random_csv = http_get(gaia_query_url(random_query))

    overlap_path = output_dir / "gaia_dr3_non_single_overlap_sample.csv"
    random_path = output_dir / "gaia_dr3_non_single_random_sample.csv"
    write_bytes(overlap_path, overlap_csv)
    write_bytes(random_path, random_csv)

    combined_path = output_dir / "gaia_dr3_non_single_sample.csv"
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
    }


def fetch_gaia_nss_two_body_samples(state_dir: Path, sample_size: int, seed: str) -> dict:
    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core database for Gaia overlap sample: {core_db_path}")

    output_dir = state_dir / "cooked" / "gaia_dr3_nss_two_body_sample"
    output_dir.mkdir(parents=True, exist_ok=True)

    overlap_ids = core_gaia_ids(core_db_path, sample_size, seed + "-nssorbit")
    select_fields = (
        "source_id,nss_solution_type,ra,dec,parallax,pmra,pmdec,period,eccentricity,"
        "center_of_mass_velocity,semi_amplitude_primary,mass_ratio,inclination,flags,significance"
    )
    overlap_query = (
        f"select {select_fields} from gaiadr3.nss_two_body_orbit "
        f"where source_id in ({','.join(str(value) for value in overlap_ids)})"
    )
    random_query = (
        f"select top {sample_size} {select_fields} from gaiadr3.nss_two_body_orbit order by source_id"
    )

    overlap_csv = http_get(gaia_query_url(overlap_query))
    random_csv = http_get(gaia_query_url(random_query))

    overlap_path = output_dir / "gaia_dr3_nss_two_body_overlap_sample.csv"
    random_path = output_dir / "gaia_dr3_nss_two_body_random_sample.csv"
    write_bytes(overlap_path, overlap_csv)
    write_bytes(random_path, random_csv)

    combined_path = output_dir / "gaia_dr3_nss_two_body_sample.csv"
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


def parse_wds_pm_token(value: str) -> tuple[float | None, float | None]:
    text = value.strip()
    if not re.fullmatch(r"[+-]\d{3}[+-]\d{3}", text):
        return None, None
    try:
        ra_pm = float(int(text[0:4]))
        dec_pm = float(int(text[4:8]))
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
            pm_tokens = re.findall(r"[+-]\d{3}[+-]\d{3}", line[78:94])
            pm_primary_ra, pm_primary_dec = (
                parse_wds_pm_token(pm_tokens[0]) if len(pm_tokens) >= 1 else (None, None)
            )
            pm_secondary_ra, pm_secondary_dec = (
                parse_wds_pm_token(pm_tokens[1]) if len(pm_tokens) >= 2 else (None, None)
            )
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


def fetch_msc_sample(state_dir: Path) -> dict:
    raw_path = state_dir / "raw" / "msc" / "newmsc-20260619.tar.gz"
    cooked_path = state_dir / "cooked" / "msc" / "msc_components.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    cooked_path.parent.mkdir(parents=True, exist_ok=True)

    payload = http_get(MSC_URL, allow_insecure_tls=MSC_SAMPLE_ALLOW_INSECURE_TLS)
    write_bytes(raw_path, payload)

    subsystem_count: dict[str, int] = {}
    orbit_count: dict[str, int] = {}
    line_count = 0

    with tarfile.open(raw_path, "r:gz") as archive:
        sys_member = archive.getmember("export/sys.tsv")
        with archive.extractfile(sys_member) as sys_f:
            if sys_f is None:
                raise SystemExit("MSC archive missing export/sys.tsv")
            for raw_line in sys_f.read().decode("utf-8", "replace").splitlines():
                if not raw_line.strip():
                    continue
                fields = raw_line.split("|")
                if len(fields) < 10:
                    continue
                wds_id = fields[0].strip()
                if wds_id:
                    subsystem_count[wds_id] = subsystem_count.get(wds_id, 0) + 1

        orb_member = archive.getmember("export/orb.tsv")
        with archive.extractfile(orb_member) as orb_f:
            if orb_f is None:
                raise SystemExit("MSC archive missing export/orb.tsv")
            for raw_line in orb_f.read().decode("utf-8", "replace").splitlines():
                if not raw_line.strip():
                    continue
                fields = raw_line.split("|")
                if len(fields) < 10:
                    continue
                wds_id = fields[0].strip()
                if wds_id:
                    orbit_count[wds_id] = orbit_count.get(wds_id, 0) + 1

        comp_member = archive.getmember("export/comp.tsv")
        with archive.extractfile(comp_member) as comp_f, cooked_path.open(
            "w", newline="", encoding="utf-8"
        ) as out_f:
            if comp_f is None:
                raise SystemExit("MSC archive missing export/comp.tsv")
            writer = csv.DictWriter(
                out_f,
                fieldnames=[
                    "wds_id",
                    "ra_deg",
                    "dec_deg",
                    "parallax_mas",
                    "parallax_ref",
                    "pm_ra_mas_yr",
                    "pm_dec_mas_yr",
                    "radial_velocity_kms",
                    "component",
                    "sep_arcsec",
                    "spectral_type_raw",
                    "hip_id",
                    "hd_id",
                    "bmag",
                    "vmag",
                    "imag",
                    "jmag",
                    "hmag",
                    "kmag",
                    "ncomp",
                    "grade",
                    "other_identifiers",
                    "subsystem_count",
                    "orbit_count",
                ],
            )
            writer.writeheader()
            for raw_line in comp_f.read().decode("utf-8", "replace").splitlines():
                if not raw_line.strip():
                    continue
                fields = raw_line.split("|")
                if len(fields) < 21:
                    continue
                wds_id = fields[0].strip()
                writer.writerow(
                    {
                        "wds_id": wds_id,
                        "ra_deg": parse_float(fields[1]),
                        "dec_deg": parse_float(fields[2]),
                        "parallax_mas": parse_float(fields[3]),
                        "parallax_ref": fields[4].strip(),
                        "pm_ra_mas_yr": parse_float(fields[5]),
                        "pm_dec_mas_yr": parse_float(fields[6]),
                        "radial_velocity_kms": parse_float(fields[7]),
                        "component": fields[8].strip(),
                        "sep_arcsec": parse_float(fields[9]),
                        "spectral_type_raw": fields[10].strip(),
                        "hip_id": parse_int(fields[11]),
                        "hd_id": parse_int(fields[12]),
                        "bmag": parse_float(fields[13]),
                        "vmag": parse_float(fields[14]),
                        "imag": parse_float(fields[15]),
                        "jmag": parse_float(fields[16]),
                        "hmag": parse_float(fields[17]),
                        "kmag": parse_float(fields[18]),
                        "ncomp": parse_int(fields[19]),
                        "grade": parse_int(fields[20]),
                        "other_identifiers": fields[21].strip() if len(fields) > 21 else "",
                        "subsystem_count": subsystem_count.get(wds_id, 0),
                        "orbit_count": orbit_count.get(wds_id, 0),
                    }
                )
                line_count += 1

    return {
        "raw_path": str(raw_path),
        "cooked_path": str(cooked_path),
        "row_count": line_count,
        "source_url": MSC_SOURCE_URL,
        "source_download_url": MSC_URL,
        "security_note": (
            "TLS verification stays enabled by default; set "
            "SPACEGATE_MSC_SAMPLE_ALLOW_INSECURE_TLS=1 only for targeted upstream TLS debugging."
        ),
    }


def fetch_orb6_sample(state_dir: Path) -> dict:
    raw_path = state_dir / "raw" / "orb6" / "orb6orbits.sql"
    cooked_path = state_dir / "cooked" / "orb6" / "orb6_orbits.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    cooked_path.parent.mkdir(parents=True, exist_ok=True)

    payload = http_get(ORB6_URL)
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
                "ads_id",
                "hd_id",
                "hip_id",
                "ra_deg",
                "dec_deg",
                "mag_primary",
                "mag_secondary",
                "period_value",
                "period_unit",
                "period_error",
                "semi_major_axis_arcsec",
                "axis_qualifier",
                "axis_error",
                "inclination_deg",
                "inclination_error",
                "node_deg",
                "node_error",
                "periastron_epoch",
                "epoch_unit",
                "eccentricity",
                "eccentricity_error",
                "long_periastron_deg",
                "long_periastron_error",
                "equinox",
                "last_observed_year",
                "grade",
                "notes_flag",
                "reference_code",
                "png_file",
            ],
        )
        writer.writeheader()
        for raw_line in in_f:
            line = raw_line.rstrip("\n")
            if (
                not line
                or line.startswith("Sixth Catalog")
                or line.startswith("RA(2000).")
            ):
                continue
            fields = line.split("|")
            if len(fields) < 34:
                continue
            ra_deg, dec_deg = parse_orb6_ra_dec(fields[0], fields[1])
            writer.writerow(
                {
                    "wds_id": fields[2].strip(),
                    "discoverer": fields[3].strip(),
                    "ads_id": parse_int(fields[4]),
                    "hd_id": parse_int(fields[5]),
                    "hip_id": parse_int(fields[6]),
                    "ra_deg": ra_deg,
                    "dec_deg": dec_deg,
                    "mag_primary": parse_float(fields[7]),
                    "mag_secondary": parse_float(fields[9]),
                    "period_value": parse_float(fields[11]),
                    "period_unit": fields[12].strip(),
                    "period_error": parse_float(fields[13]),
                    "semi_major_axis_arcsec": parse_float(fields[14]),
                    "axis_qualifier": fields[15].strip(),
                    "axis_error": parse_float(fields[16]),
                    "inclination_deg": parse_float(fields[17]),
                    "inclination_error": parse_float(fields[18]),
                    "node_deg": parse_float(fields[19]),
                    "node_error": parse_float(fields[21]),
                    "periastron_epoch": parse_float(fields[22]),
                    "epoch_unit": fields[23].strip(),
                    "eccentricity": parse_float(fields[24]),
                    "eccentricity_error": parse_float(fields[25]),
                    "long_periastron_deg": parse_float(fields[26]),
                    "long_periastron_error": parse_float(fields[27]),
                    "equinox": parse_int(fields[28]),
                    "last_observed_year": parse_int(fields[29]),
                    "grade": parse_int(fields[30]),
                    "notes_flag": fields[31].strip(),
                    "reference_code": fields[32].strip(),
                    "png_file": fields[33].strip(),
                }
            )
            line_count += 1

    return {
        "raw_path": str(raw_path),
        "cooked_path": str(cooked_path),
        "row_count": line_count,
        "source_url": ORB6_URL,
    }


def fetch_sbx_sample(state_dir: Path, sample_size: int, seed: str) -> dict:
    core_db_path = state_dir / "served" / "current" / "core.duckdb"
    if not core_db_path.exists():
        raise SystemExit(f"Missing core database for SBX overlap sample: {core_db_path}")

    output_dir = state_dir / "cooked" / "sbx_sample"
    output_dir.mkdir(parents=True, exist_ok=True)

    overlap_ids = core_gaia_ids(core_db_path, sample_size, seed + "-sbx")
    overlap_query = (
        "SELECT TOP {limit} s.sn, s.ra, s.dec, s.parallax, s.pmra, s.pmdec, "
        "s.mag1, s.position_epoch, s.position_source, s.st1 "
        "FROM systems s JOIN alias a ON s.sn = a.sn "
        "WHERE a.catalog = 'Gaia' AND a.version = 'DR3' "
        "AND a.identifier IN ({gaia_ids}) ORDER BY 1"
    ).format(limit=sample_size, gaia_ids=adql_string_list(overlap_ids))
    random_query = (
        "SELECT TOP {limit} s.sn, s.ra, s.dec, s.parallax, s.pmra, s.pmdec, "
        "s.mag1, s.position_epoch, s.position_source, s.st1 "
        "FROM systems s WHERE s.parallax >= 3.26156 ORDER BY 1"
    ).format(limit=sample_size)

    overlap_rows = read_csv_rows(sbx_query_csv(overlap_query))
    random_rows = read_csv_rows(sbx_query_csv(random_query))
    selected_sns: list[int] = []
    seen_sns: set[int] = set()
    for rows in (overlap_rows, random_rows):
        for row in rows:
            sn = parse_int(row.get("sn", ""))
            if sn is None or sn in seen_sns:
                continue
            seen_sns.add(sn)
            selected_sns.append(sn)

    if not selected_sns:
        raise SystemExit("No SBX sample rows were returned.")

    sn_list = adql_int_list(selected_sns)
    system_rows = read_csv_rows(
        sbx_query_csv(
            "SELECT s.sn, s.ra, s.dec, s.parallax, s.pmra, s.pmdec, s.mag1, "
            "s.position_epoch, s.position_source, s.st1 "
            f"FROM systems s WHERE s.sn IN ({sn_list}) ORDER BY 1"
        )
    )
    alias_rows = read_csv_rows(
        sbx_query_csv(
            "SELECT sn, catalog, version, identifier "
            "FROM alias "
            f"WHERE sn IN ({sn_list}) AND catalog IN ('Gaia','HIP','HD','WDS','ADS') "
            "ORDER BY sn, catalog, version"
        )
    )
    config_rows = read_csv_rows(
        sbx_query_csv(
            "SELECT sn, family, parent, child1, child2, in_triple "
            "FROM configurations "
            f"WHERE sn IN ({sn_list}) ORDER BY sn"
        )
    )
    orbit_rows = read_csv_rows(
        sbx_query_csv(
            "SELECT sn, COUNT(*) AS orbit_count "
            "FROM orbits "
            f"WHERE sn IN ({sn_list}) GROUP BY sn ORDER BY sn"
        )
    )

    alias_by_sn: dict[int, dict[str, str]] = {}
    for row in alias_rows:
        sn = parse_int(row.get("sn", ""))
        if sn is None:
            continue
        alias_map = alias_by_sn.setdefault(sn, {})
        catalog = (row.get("catalog") or "").strip()
        version = (row.get("version") or "").strip()
        key = catalog if not version else f"{catalog}:{version}"
        alias_map[key] = (row.get("identifier") or "").strip()

    config_by_sn = {parse_int(row.get("sn", "")): row for row in config_rows if parse_int(row.get("sn", "")) is not None}
    orbit_count_by_sn = {
        parse_int(row.get("sn", "")): parse_int(row.get("orbit_count", "")) or 0
        for row in orbit_rows
        if parse_int(row.get("sn", "")) is not None
    }
    overlap_sn_set = {
        parse_int(row.get("sn", ""))
        for row in overlap_rows
        if parse_int(row.get("sn", "")) is not None
    }

    random_path = output_dir / "sbx_random_sample.csv"
    overlap_path = output_dir / "sbx_overlap_sample.csv"
    combined_path = output_dir / "sbx_sample.csv"
    fieldnames = [
        "sn",
        "gaia_id",
        "hip_id",
        "hd_id",
        "wds_id",
        "ads_id",
        "ra_deg",
        "dec_deg",
        "parallax_mas",
        "pm_ra_mas_yr",
        "pm_dec_mas_yr",
        "mag_primary",
        "position_epoch",
        "position_source",
        "spectral_type_raw",
        "family",
        "parent",
        "child1",
        "child2",
        "in_triple",
        "orbit_count",
        "sample_origin",
    ]

    def write_subset(path: Path, allowed_origin: str) -> int:
        count = 0
        with path.open("w", newline="", encoding="utf-8") as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fieldnames)
            writer.writeheader()
            for row in system_rows:
                sn = parse_int(row.get("sn", ""))
                if sn is None:
                    continue
                origin = "overlap" if sn in overlap_sn_set else "random"
                if origin != allowed_origin:
                    continue
                alias_map = alias_by_sn.get(sn, {})
                config = config_by_sn.get(sn, {})
                writer.writerow(
                    {
                        "sn": sn,
                        "gaia_id": alias_map.get("Gaia:DR3", ""),
                        "hip_id": alias_map.get("HIP", ""),
                        "hd_id": alias_map.get("HD", ""),
                        "wds_id": alias_map.get("WDS", ""),
                        "ads_id": alias_map.get("ADS", ""),
                        "ra_deg": row.get("ra", ""),
                        "dec_deg": row.get("dec", ""),
                        "parallax_mas": row.get("parallax", ""),
                        "pm_ra_mas_yr": row.get("pmra", ""),
                        "pm_dec_mas_yr": row.get("pmdec", ""),
                        "mag_primary": row.get("mag1", ""),
                        "position_epoch": row.get("position_epoch", ""),
                        "position_source": row.get("position_source", ""),
                        "spectral_type_raw": row.get("st1", ""),
                        "family": config.get("family", ""),
                        "parent": config.get("parent", ""),
                        "child1": config.get("child1", ""),
                        "child2": config.get("child2", ""),
                        "in_triple": config.get("in_triple", ""),
                        "orbit_count": orbit_count_by_sn.get(sn, 0),
                        "sample_origin": origin,
                    }
                )
                count += 1
        return count

    overlap_count = write_subset(overlap_path, "overlap")
    random_count = write_subset(random_path, "random")

    with combined_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()
        for path in (overlap_path, random_path):
            with path.open(newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)
                for row in reader:
                    writer.writerow(row)

    return {
        "combined_path": str(combined_path),
        "random_path": str(random_path),
        "overlap_path": str(overlap_path),
        "row_count_combined": overlap_count + random_count,
        "query_random": random_query,
        "query_overlap_count": len(overlap_ids),
        "source_url": SBX_TAP_SYNC_URL,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch sample inputs for catalog quality evaluation."
    )
    parser.add_argument(
        "--catalog",
        action="append",
        choices=[
            "gaia_dr3_sample",
            "gaia_dr3_non_single_sample",
            "gaia_dr3_nss_two_body_sample",
            "wds",
            "msc",
            "orb6",
            "sbx_sample",
        ],
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
    selected = args.catalog or [
        "gaia_dr3_sample",
        "gaia_dr3_non_single_sample",
        "gaia_dr3_nss_two_body_sample",
        "wds",
        "msc",
        "orb6",
        "sbx_sample",
    ]

    summary = {}
    if "gaia_dr3_sample" in selected:
        summary["gaia_dr3_sample"] = fetch_gaia_samples(state_dir, args.sample_size, args.seed)
    if "gaia_dr3_non_single_sample" in selected:
        summary["gaia_dr3_non_single_sample"] = fetch_gaia_non_single_samples(state_dir, args.sample_size, args.seed)
    if "gaia_dr3_nss_two_body_sample" in selected:
        summary["gaia_dr3_nss_two_body_sample"] = fetch_gaia_nss_two_body_samples(state_dir, args.sample_size, args.seed)
    if "wds" in selected:
        summary["wds"] = fetch_wds_sample(state_dir)
    if "msc" in selected:
        summary["msc"] = fetch_msc_sample(state_dir)
    if "orb6" in selected:
        summary["orb6"] = fetch_orb6_sample(state_dir)
    if "sbx_sample" in selected:
        summary["sbx_sample"] = fetch_sbx_sample(state_dir, args.sample_size, args.seed)

    summary_path = state_dir / "reports" / "catalog_eval_inputs" / "latest_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(str(summary_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
