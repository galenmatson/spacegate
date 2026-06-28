#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import os
import re
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


def parse_catalog_int(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    direct = parse_int(text)
    if direct is not None:
        return direct
    match = re.search(r"([0-9]+)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def normalize_wds_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    token = raw.split(",")[0].strip()
    token = token.replace(" ", "")
    if token.startswith("J") and len(token) >= 10:
        token = token[1:]
    match = re.search(r"([0-9]{5}[+-][0-9]{4})", token)
    if match:
        return match.group(1)
    if len(token) >= 10 and token[5] in {"+", "-"}:
        return token[:10]
    return ""


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
    cooked_systems_path = cooked_path.parent / "msc_systems.csv"
    cooked_orbits_path = cooked_path.parent / "msc_orbits.csv"
    max_archive_bytes = int(os.getenv("SPACEGATE_MSC_MAX_ARCHIVE_BYTES", str(128 * 1024 * 1024)))
    max_member_bytes = int(os.getenv("SPACEGATE_MSC_MAX_MEMBER_BYTES", str(64 * 1024 * 1024)))
    expected_member_sets = (
        ("export/sys.tsv", "export/orb.tsv", "export/comp.tsv"),
        ("sys.tsv", "orb.tsv", "comp.tsv"),
    )
    archive_bytes = raw_path.stat().st_size
    if archive_bytes > max_archive_bytes:
        raise SystemExit(
            f"MSC archive too large: {archive_bytes} bytes exceeds limit {max_archive_bytes} "
            "(SPACEGATE_MSC_MAX_ARCHIVE_BYTES)."
        )

    def require_member(archive: tarfile.TarFile, member_name: str) -> tarfile.TarInfo:
        try:
            member = archive.getmember(member_name)
        except KeyError as exc:
            raise SystemExit(f"MSC archive missing {member_name}") from exc
        if not member.isfile():
            raise SystemExit(f"MSC archive member is not a regular file: {member_name}")
        if member.size > max_member_bytes:
            raise SystemExit(
                f"MSC archive member too large: {member_name} has {member.size} bytes "
                f"(limit {max_member_bytes}, SPACEGATE_MSC_MAX_MEMBER_BYTES)."
            )
        return member

    def iter_member_lines(binary_handle: tarfile.ExFileObject):
        wrapper = io.TextIOWrapper(binary_handle, encoding="utf-8", errors="replace", newline="")
        try:
            for raw_line in wrapper:
                yield raw_line.rstrip("\n")
        finally:
            wrapper.detach()

    subsystem_count: dict[str, int] = {}
    orbit_count: dict[str, int] = {}
    line_count = 0
    with tarfile.open(raw_path, "r:gz") as archive:
        archive_member_names = {member.name for member in archive.getmembers()}
        selected_members = next(
            (
                member_set
                for member_set in expected_member_sets
                if all(name in archive_member_names for name in member_set)
            ),
            None,
        )
        if selected_members is None:
            expected = " or ".join("{" + ", ".join(member_set) + "}" for member_set in expected_member_sets)
            raise SystemExit(f"MSC archive missing expected members: {expected}")
        sys_name, orb_name, comp_name = selected_members

        sys_member = require_member(archive, sys_name)
        with archive.extractfile(sys_member) as sys_f, cooked_systems_path.open(
            "w", newline="", encoding="utf-8"
        ) as sys_out_f:
            if sys_f is None:
                raise SystemExit(f"MSC archive missing {sys_name}")
            sys_writer = csv.DictWriter(
                sys_out_f,
                fieldnames=[
                    "wds_id",
                    "primary_label",
                    "secondary_label",
                    "parent_label",
                    "system_type",
                    "period_value",
                    "period_unit",
                    "separation_value",
                    "separation_unit",
                    "position_angle_deg",
                    "vmag_primary",
                    "spectral_type_primary",
                    "vmag_secondary",
                    "spectral_type_secondary",
                    "mass_primary_msun",
                    "mass_code_primary",
                    "mass_secondary_msun",
                    "mass_code_secondary",
                    "comment",
                    "source_line_number",
                    "raw_row",
                ],
            )
            sys_writer.writeheader()
            for source_line_number, raw_line in enumerate(iter_member_lines(sys_f), start=1):
                if not raw_line.strip():
                    continue
                fields = raw_line.split("|")
                if len(fields) < 10:
                    continue
                wds_id = fields[0].strip()
                if wds_id:
                    subsystem_count[wds_id] = subsystem_count.get(wds_id, 0) + 1
                    sys_writer.writerow(
                        {
                            "wds_id": wds_id,
                            "primary_label": fields[1].strip() if len(fields) > 1 else "",
                            "secondary_label": fields[2].strip() if len(fields) > 2 else "",
                            "parent_label": fields[3].strip() if len(fields) > 3 else "",
                            "system_type": fields[4].strip() if len(fields) > 4 else "",
                            "period_value": parse_float(fields[5]) if len(fields) > 5 else None,
                            "period_unit": fields[6].strip() if len(fields) > 6 else "",
                            "separation_value": parse_float(fields[7]) if len(fields) > 7 else None,
                            "separation_unit": fields[8].strip() if len(fields) > 8 else "",
                            "position_angle_deg": parse_float(fields[9]) if len(fields) > 9 else None,
                            "vmag_primary": parse_float(fields[10]) if len(fields) > 10 else None,
                            "spectral_type_primary": fields[11].strip() if len(fields) > 11 else "",
                            "vmag_secondary": parse_float(fields[12]) if len(fields) > 12 else None,
                            "spectral_type_secondary": fields[13].strip() if len(fields) > 13 else "",
                            "mass_primary_msun": parse_float(fields[14]) if len(fields) > 14 else None,
                            "mass_code_primary": fields[15].strip() if len(fields) > 15 else "",
                            "mass_secondary_msun": parse_float(fields[16]) if len(fields) > 16 else None,
                            "mass_code_secondary": fields[17].strip() if len(fields) > 17 else "",
                            "comment": fields[18].strip() if len(fields) > 18 else "",
                            "source_line_number": source_line_number,
                            "raw_row": raw_line,
                        }
                    )

        orb_member = require_member(archive, orb_name)
        with archive.extractfile(orb_member) as orb_f, cooked_orbits_path.open(
            "w", newline="", encoding="utf-8"
        ) as orb_out_f:
            if orb_f is None:
                raise SystemExit(f"MSC archive missing {orb_name}")
            orb_writer = csv.DictWriter(
                orb_out_f,
                fieldnames=[
                    "wds_id",
                    "system_label",
                    "primary_label",
                    "secondary_label",
                    "period_value",
                    "periastron_epoch",
                    "eccentricity",
                    "semi_major_axis_arcsec",
                    "node_deg",
                    "longitude_periastron_deg",
                    "inclination_deg",
                    "semi_amplitude_primary_kms",
                    "semi_amplitude_secondary_kms",
                    "center_of_mass_velocity_kms",
                    "node_flag",
                    "period_unit",
                    "note",
                    "source_line_number",
                    "raw_row",
                ],
            )
            orb_writer.writeheader()
            for source_line_number, raw_line in enumerate(iter_member_lines(orb_f), start=1):
                if not raw_line.strip():
                    continue
                fields = raw_line.split("|")
                if len(fields) < 10:
                    continue
                wds_id = fields[0].strip()
                if wds_id:
                    orbit_count[wds_id] = orbit_count.get(wds_id, 0) + 1
                    system_label = fields[1].strip() if len(fields) > 1 else ""
                    pair_parts = [
                        part.strip()
                        for part in re.split(r"[,.;+]", system_label)
                        if part.strip()
                    ]
                    orb_writer.writerow(
                        {
                            "wds_id": wds_id,
                            "system_label": system_label,
                            "primary_label": pair_parts[0] if len(pair_parts) > 0 else "",
                            "secondary_label": pair_parts[1] if len(pair_parts) > 1 else "",
                            "period_value": parse_float(fields[2]) if len(fields) > 2 else None,
                            "periastron_epoch": parse_float(fields[3]) if len(fields) > 3 else None,
                            "eccentricity": parse_float(fields[4]) if len(fields) > 4 else None,
                            "semi_major_axis_arcsec": parse_float(fields[5]) if len(fields) > 5 else None,
                            "node_deg": parse_float(fields[6]) if len(fields) > 6 else None,
                            "longitude_periastron_deg": parse_float(fields[7]) if len(fields) > 7 else None,
                            "inclination_deg": parse_float(fields[8]) if len(fields) > 8 else None,
                            "semi_amplitude_primary_kms": parse_float(fields[9]) if len(fields) > 9 else None,
                            "semi_amplitude_secondary_kms": parse_float(fields[10]) if len(fields) > 10 else None,
                            "center_of_mass_velocity_kms": parse_float(fields[11]) if len(fields) > 11 else None,
                            "node_flag": fields[12].strip() if len(fields) > 12 else "",
                            "period_unit": fields[13].strip() if len(fields) > 13 else "",
                            "note": fields[14].strip() if len(fields) > 14 else "",
                            "source_line_number": source_line_number,
                            "raw_row": raw_line,
                        }
                    )

        comp_member = require_member(archive, comp_name)
        with archive.extractfile(comp_member) as comp_f, cooked_path.open(
            "w", newline="", encoding="utf-8"
        ) as out_f:
            if comp_f is None:
                raise SystemExit(f"MSC archive missing {comp_name}")
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
            for raw_line in iter_member_lines(comp_f):
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


def cook_wds_gaia_xmatch(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    line_count = 0
    seen: set[tuple[str, str, int]] = set()
    with raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "wds_id",
                "component",
                "gaia_id",
                "ang_dist_arcsec",
                "obs_last_year",
                "pa_last_deg",
                "sep_last_arcsec",
                "mag_primary",
                "mag_secondary",
                "wds_raj2000",
                "wds_dej2000",
                "gaia_dr3_name",
                "gaia_ra_deg",
                "gaia_dec_deg",
                "gaia_plx_mas",
                "gaia_pmra_mas_yr",
                "gaia_pmdec_mas_yr",
                "gaia_ruwe",
                "gaia_gmag",
            ],
        )
        writer.writeheader()
        for row in reader:
            wds_id = str(row.get("WDS", "")).strip()
            component = str(row.get("Comp", "")).strip()
            gaia_id = parse_int(row.get("Source", ""))
            if not wds_id or gaia_id is None:
                continue
            key = (wds_id, component, gaia_id)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow(
                {
                    "wds_id": wds_id,
                    "component": component,
                    "gaia_id": gaia_id,
                    "ang_dist_arcsec": parse_float(row.get("angDist", "")),
                    "obs_last_year": parse_int(row.get("Obs2", "")),
                    "pa_last_deg": parse_float(row.get("pa2", "")),
                    "sep_last_arcsec": parse_float(row.get("sep2", "")),
                    "mag_primary": parse_float(row.get("mag1", "")),
                    "mag_secondary": parse_float(row.get("mag2", "")),
                    "wds_raj2000": str(row.get("RAJ2000", "")).strip(),
                    "wds_dej2000": str(row.get("DEJ2000", "")).strip(),
                    "gaia_dr3_name": str(row.get("DR3Name", "")).strip(),
                    "gaia_ra_deg": parse_float(row.get("RAdeg", "")),
                    "gaia_dec_deg": parse_float(row.get("DEdeg", "")),
                    "gaia_plx_mas": parse_float(row.get("Plx", "")),
                    "gaia_pmra_mas_yr": parse_float(row.get("pmRA", "")),
                    "gaia_pmdec_mas_yr": parse_float(row.get("pmDE", "")),
                    "gaia_ruwe": parse_float(row.get("RUWE", "")),
                    "gaia_gmag": parse_float(row.get("Gmag", "")),
                }
            )
            line_count += 1
    return line_count


def cook_sbx(
    systems_raw_path: Path,
    alias_raw_path: Path,
    configurations_raw_path: Path,
    orbits_raw_path: Path,
    cooked_path: Path,
) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)

    systems: dict[int, dict[str, object]] = {}
    with systems_raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f:
        reader = csv.DictReader(in_f)
        for row in reader:
            sn = parse_int(row.get("sn", ""))
            if sn is None:
                continue
            systems[sn] = {
                "sn": sn,
                "ra_deg": parse_float(row.get("ra", "")),
                "dec_deg": parse_float(row.get("dec", "")),
                "parallax_mas": parse_float(row.get("parallax", "")),
                "pm_ra_mas_yr": parse_float(row.get("pmra", "")),
                "pm_dec_mas_yr": parse_float(row.get("pmdec", "")),
                "mag_primary": parse_float(row.get("mag1", "")),
                "position_epoch": parse_float(row.get("position_epoch", "")),
                "position_source": str(row.get("position_source", "")).strip(),
                "spectral_type_raw": str(row.get("st1", "")).strip(),
            }

    aliases: dict[int, dict[str, object]] = {}
    with alias_raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f:
        reader = csv.DictReader(in_f)
        for row in reader:
            sn = parse_int(row.get("sn", ""))
            if sn is None:
                continue
            rec = aliases.setdefault(
                sn,
                {
                    "gaia_id": None,
                    "hip_id": None,
                    "hd_id": None,
                    "wds_id": "",
                    "ads_id": "",
                },
            )
            catalog = str(row.get("catalog", "")).strip()
            version = str(row.get("version", "")).strip()
            identifier = str(row.get("identifier", "")).strip()
            if not catalog or not identifier:
                continue
            if catalog == "Gaia" and version == "DR3":
                if rec["gaia_id"] is None:
                    rec["gaia_id"] = parse_catalog_int(identifier)
            elif catalog == "HIP":
                if rec["hip_id"] is None:
                    rec["hip_id"] = parse_catalog_int(identifier)
            elif catalog == "HD":
                if rec["hd_id"] is None:
                    rec["hd_id"] = parse_catalog_int(identifier)
            elif catalog == "WDS" and not rec["wds_id"]:
                rec["wds_id"] = normalize_wds_identifier(identifier)
            elif catalog == "ADS" and not rec["ads_id"]:
                rec["ads_id"] = identifier

    configurations: dict[int, dict[str, str]] = {}
    with configurations_raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f:
        reader = csv.DictReader(in_f)
        for row in reader:
            sn = parse_int(row.get("sn", ""))
            if sn is None:
                continue
            configurations[sn] = {
                "family": str(row.get("family", "")).strip(),
                "parent": str(row.get("parent", "")).strip(),
                "child1": str(row.get("child1", "")).strip(),
                "child2": str(row.get("child2", "")).strip(),
                "in_triple": str(row.get("in_triple", "")).strip(),
            }

    orbit_counts: dict[int, int] = {}
    with orbits_raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f:
        reader = csv.DictReader(in_f)
        for row in reader:
            sn = parse_int(row.get("sn", ""))
            if sn is None:
                continue
            orbit_counts[sn] = parse_int(row.get("orbit_count", "")) or 0

    line_count = 0
    with cooked_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
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
            ],
        )
        writer.writeheader()
        for sn in sorted(systems.keys()):
            system = systems[sn]
            alias = aliases.get(sn, {})
            config = configurations.get(sn, {})
            writer.writerow(
                {
                    "sn": sn,
                    "gaia_id": alias.get("gaia_id"),
                    "hip_id": alias.get("hip_id"),
                    "hd_id": alias.get("hd_id"),
                    "wds_id": alias.get("wds_id", ""),
                    "ads_id": alias.get("ads_id", ""),
                    "ra_deg": system["ra_deg"],
                    "dec_deg": system["dec_deg"],
                    "parallax_mas": system["parallax_mas"],
                    "pm_ra_mas_yr": system["pm_ra_mas_yr"],
                    "pm_dec_mas_yr": system["pm_dec_mas_yr"],
                    "mag_primary": system["mag_primary"],
                    "position_epoch": system["position_epoch"],
                    "position_source": system["position_source"],
                    "spectral_type_raw": system["spectral_type_raw"],
                    "family": config.get("family", ""),
                    "parent": config.get("parent", ""),
                    "child1": config.get("child1", ""),
                    "child2": config.get("child2", ""),
                    "in_triple": config.get("in_triple", ""),
                    "orbit_count": orbit_counts.get(sn, 0),
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
        ("msc", raw_dir / "msc" / "newmsc-20260619.tar.gz", cooked_dir / "msc" / "msc_components.csv", cook_msc),
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
        (
            "wds_gaia_xmatch",
            raw_dir / "wds_gaia_xmatch" / "wds_gaia_best.csv",
            cooked_dir / "wds_gaia_xmatch" / "wds_gaia_matches.csv",
            cook_wds_gaia_xmatch,
        ),
    ]

    for label, raw_path, cooked_path, handler in jobs:
        if not raw_path.exists():
            print(f"skip {label}: missing {raw_path}")
            continue
        count = handler(raw_path, cooked_path)
        print(f"cooked {label}: {count} rows -> {cooked_path}")

    sbx_systems_raw = raw_dir / "sbx" / "sbx_systems.csv"
    sbx_alias_raw = raw_dir / "sbx" / "sbx_alias.csv"
    sbx_config_raw = raw_dir / "sbx" / "sbx_configurations.csv"
    sbx_orbits_raw = raw_dir / "sbx" / "sbx_orbits.csv"
    sbx_cooked = cooked_dir / "sbx" / "sbx_catalog.csv"
    if all(
        path.exists()
        for path in (sbx_systems_raw, sbx_alias_raw, sbx_config_raw, sbx_orbits_raw)
    ):
        count = cook_sbx(
            sbx_systems_raw,
            sbx_alias_raw,
            sbx_config_raw,
            sbx_orbits_raw,
            sbx_cooked,
        )
        print(f"cooked sbx: {count} rows -> {sbx_cooked}")
    else:
        missing = [
            str(path)
            for path in (sbx_systems_raw, sbx_alias_raw, sbx_config_raw, sbx_orbits_raw)
            if not path.exists()
        ]
        print(f"skip sbx: missing {', '.join(missing)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
