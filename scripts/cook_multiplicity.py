#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import tarfile
from pathlib import Path


def parse_float(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def choose_preferred_name(raw_value: str) -> str:
    prefixes = (
        "ADS",
        "BD",
        "CD",
        "CPD",
        "GJ",
        "GL",
        "HD",
        "HIP",
        "HR",
        "LHS",
        "LP",
        "LTT",
        "NLTT",
        "TOI",
        "TYC",
        "2MASS",
    )
    for raw_token in str(raw_value or "").split(","):
        token = raw_token.strip()
        if not token:
            continue
        token = token.replace("_", " ")
        token = token.replace("V* ", "")
        token = token.replace("V*", "")
        token = token.lstrip("*").strip()
        upper = token.upper()
        if upper.startswith(prefixes):
            continue
        if any(ch.isalpha() for ch in token):
            return token
    return ""


def parse_orb6_ra_dec(ra_text: str, dec_text: str) -> tuple[float | None, float | None]:
    ra_value = str(ra_text or "").strip()
    dec_value = str(dec_text or "").strip()
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


def parse_wds_coord(value: str) -> tuple[float | None, float | None]:
    coord = str(value or "").strip()
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
    text = str(value or "").strip()
    if len(text) != 8 or text[0] not in "+-" or text[4] not in "+-":
        return None, None
    try:
        ra_pm = float(int(text[0:4]))
        dec_pm = float(int(text[4:8]))
    except ValueError:
        return None, None
    return ra_pm, dec_pm


def cook_wds(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
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
            pm_primary_ra, pm_primary_dec = parse_wds_pm_token(line[79:87].strip())
            pm_secondary_ra, pm_secondary_dec = parse_wds_pm_token(line[87:95].strip())
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
    return line_count


def cook_msc(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
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
                    "preferred_name",
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
                        "preferred_name": choose_preferred_name(fields[21] if len(fields) > 21 else ""),
                        "subsystem_count": subsystem_count.get(wds_id, 0),
                        "orbit_count": orbit_count.get(wds_id, 0),
                    }
                )
                line_count += 1
    return line_count


def cook_orb6(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
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
            if not line or line.startswith("Sixth Catalog") or line.startswith("RA(2000)."):
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
    return line_count


def cook_gaia_nss_non_single(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    line_count = 0
    with raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "source_id",
                "non_single_star",
                "ra_deg",
                "dec_deg",
                "parallax_mas",
                "parallax_error_mas",
                "pm_ra_mas_yr",
                "pm_dec_mas_yr",
                "radial_velocity_kms",
            ],
        )
        writer.writeheader()
        seen_ids: set[int] = set()
        for row in reader:
            source_id = parse_int(row.get("source_id", ""))
            if source_id is None or source_id in seen_ids:
                continue
            non_single = parse_int(row.get("non_single_star", "")) or 0
            if non_single != 1:
                continue
            seen_ids.add(source_id)
            writer.writerow(
                {
                    "source_id": source_id,
                    "non_single_star": 1,
                    "ra_deg": parse_float(row.get("ra", "")),
                    "dec_deg": parse_float(row.get("dec", "")),
                    "parallax_mas": parse_float(row.get("parallax", "")),
                    "parallax_error_mas": parse_float(row.get("parallax_error", "")),
                    "pm_ra_mas_yr": parse_float(row.get("pmra", "")),
                    "pm_dec_mas_yr": parse_float(row.get("pmdec", "")),
                    "radial_velocity_kms": parse_float(row.get("radial_velocity", "")),
                }
            )
            line_count += 1
    return line_count


def cook_gaia_nss_two_body(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    line_count = 0
    with raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "source_id",
                "nss_solution_type",
                "ra_deg",
                "dec_deg",
                "parallax_mas",
                "parallax_error_mas",
                "pm_ra_mas_yr",
                "pm_dec_mas_yr",
                "period_days",
                "eccentricity",
                "center_of_mass_velocity_kms",
                "semi_amplitude_primary_kms",
                "mass_ratio",
                "inclination_deg",
                "flags",
                "significance",
            ],
        )
        writer.writeheader()
        for row in reader:
            source_id = parse_int(row.get("source_id", ""))
            if source_id is None:
                continue
            writer.writerow(
                {
                    "source_id": source_id,
                    "nss_solution_type": str(row.get("nss_solution_type", "")).strip(),
                    "ra_deg": parse_float(row.get("ra", "")),
                    "dec_deg": parse_float(row.get("dec", "")),
                    "parallax_mas": parse_float(row.get("parallax", "")),
                    "parallax_error_mas": parse_float(row.get("parallax_error", "")),
                    "pm_ra_mas_yr": parse_float(row.get("pmra", "")),
                    "pm_dec_mas_yr": parse_float(row.get("pmdec", "")),
                    "period_days": parse_float(row.get("period", "")),
                    "eccentricity": parse_float(row.get("eccentricity", "")),
                    "center_of_mass_velocity_kms": parse_float(row.get("center_of_mass_velocity", "")),
                    "semi_amplitude_primary_kms": parse_float(row.get("semi_amplitude_primary", "")),
                    "mass_ratio": parse_float(row.get("mass_ratio", "")),
                    "inclination_deg": parse_float(row.get("inclination", "")),
                    "flags": str(row.get("flags", "")).strip(),
                    "significance": parse_float(row.get("significance", "")),
                }
            )
            line_count += 1
    return line_count


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    state_dir = Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")
    raw_dir = state_dir / "raw"
    cooked_dir = state_dir / "cooked"

    jobs = [
        ("wds", raw_dir / "wds" / "wdsweb_summ2.txt", cooked_dir / "wds" / "wds_summary.csv", cook_wds),
        ("msc", raw_dir / "msc" / "newmsc-20240101.tar.gz", cooked_dir / "msc" / "msc_components.csv", cook_msc),
        ("orb6", raw_dir / "orb6" / "orb6orbits.sql", cooked_dir / "orb6" / "orb6_orbits.csv", cook_orb6),
        (
            "gaia_nss_non_single",
            raw_dir / "gaia_nss" / "gaia_dr3_non_single_star.csv",
            cooked_dir / "gaia_nss" / "gaia_dr3_non_single_star.csv",
            cook_gaia_nss_non_single,
        ),
        (
            "gaia_nss_two_body",
            raw_dir / "gaia_nss" / "gaia_dr3_nss_two_body_orbit.csv",
            cooked_dir / "gaia_nss" / "gaia_dr3_nss_two_body_orbit.csv",
            cook_gaia_nss_two_body,
        ),
    ]

    for label, raw_path, cooked_path, handler in jobs:
        if not raw_path.exists():
            print(f"skip {label}: missing {raw_path}")
            continue
        count = handler(raw_path, cooked_path)
        print(f"cooked {label}: {count} rows -> {cooked_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
