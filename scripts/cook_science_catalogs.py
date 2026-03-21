#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import html
import math
import os
import re
import struct
import tarfile
from pathlib import Path

FITS_CARD_BYTES = 80
FITS_BLOCK_BYTES = 2880


def parse_float(value: str | None) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("−", "-")
    text = re.sub(r"[^0-9eE+.\-]", "", text)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = re.sub(r"[^0-9+\-]", "", text)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_bool(value: str | None) -> bool | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None


def is_missing_text(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    return text.lower() in {"null", "nan", "none", "n/a", "na", "--"}


def clean_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"null", "nan", "none", "n/a", "na", "--"}:
        return None
    return text


def parse_log_quantity(value: str | None) -> float | None:
    parsed = parse_float(value)
    if parsed is None:
        return None
    if parsed <= -9.0:
        return None
    try:
        return float(math.pow(10.0, parsed))
    except (OverflowError, ValueError):
        return None


def parse_with_missing_sentinel(value: str | None) -> float | None:
    parsed = parse_float(value)
    if parsed is None:
        return None
    if parsed <= -9.0:
        return None
    return parsed


def parse_hms_to_deg(value: str | None) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    parts = [p for p in re.split(r"[:\s]+", text) if p]
    if len(parts) < 2:
        return None
    try:
        hours = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2]) if len(parts) > 2 else 0.0
    except ValueError:
        return None
    if hours < 0:
        return None
    return (hours + minutes / 60.0 + seconds / 3600.0) * 15.0


def parse_dms_to_deg(value: str | None) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    sign = -1.0 if text.startswith("-") else 1.0
    text = text.lstrip("+-")
    parts = [p for p in re.split(r"[:\s]+", text) if p]
    if len(parts) < 2:
        return None
    try:
        deg = float(parts[0])
        arcmin = float(parts[1])
        arcsec = float(parts[2]) if len(parts) > 2 else 0.0
    except ValueError:
        return None
    return sign * (deg + arcmin / 60.0 + arcsec / 3600.0)


def first_token(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parts = text.split()
    return parts[0] if parts else ""


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def _read_fits_header(handle) -> tuple[list[str], bool]:
    cards: list[str] = []
    while True:
        block = handle.read(FITS_BLOCK_BYTES)
        if not block or len(block) < FITS_BLOCK_BYTES:
            return cards, False
        for offset in range(0, FITS_BLOCK_BYTES, FITS_CARD_BYTES):
            card = block[offset : offset + FITS_CARD_BYTES].decode("ascii", "replace")
            cards.append(card)
            if card.startswith("END"):
                return cards, True


def _fits_card_value(card: str) -> str | None:
    if "=" not in card[:10]:
        return None
    raw = card[10:]
    return raw.split("/", 1)[0].strip().strip("'").strip()


def _fits_tform_width(tform: str) -> tuple[int, str, int]:
    text = (tform or "").strip()
    m = re.fullmatch(r"(\d*)([A-Z])", text)
    if not m:
        raise ValueError(f"Unsupported FITS TFORM: {tform!r}")
    repeat = int(m.group(1) or "1")
    code = m.group(2)
    item_size = {
        "L": 1,
        "X": 0,
        "B": 1,
        "I": 2,
        "J": 4,
        "K": 8,
        "A": 1,
        "E": 4,
        "D": 8,
        "C": 8,
        "M": 16,
    }.get(code)
    if item_size is None:
        raise ValueError(f"Unsupported FITS TFORM code: {code}")
    width = (repeat + 7) // 8 if code == "X" else repeat * item_size
    return width, code, repeat


def _fits_scalar_from_row(
    row: bytes, offset: int, width: int, code: str, repeat: int
) -> str | float | int | bool | None:
    chunk = row[offset : offset + width]
    if len(chunk) == 0:
        return None
    if code == "A":
        return chunk.decode("ascii", "replace").strip() or None
    if code == "L":
        value = chunk[0:1].decode("ascii", "replace").strip().upper()
        if value == "T":
            return True
        if value == "F":
            return False
        return None
    if code == "B":
        return int(chunk[0]) if repeat == 1 else None
    if code == "I":
        return struct.unpack(">h", chunk[:2])[0] if repeat == 1 else None
    if code == "J":
        return struct.unpack(">i", chunk[:4])[0] if repeat == 1 else None
    if code == "K":
        return struct.unpack(">q", chunk[:8])[0] if repeat == 1 else None
    if code == "E":
        value = struct.unpack(">f", chunk[:4])[0] if repeat == 1 else None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if code == "D":
        value = struct.unpack(">d", chunk[:8])[0] if repeat == 1 else None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    return None


def cook_white_dwarf(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with gzip.open(raw_path, "rb") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        _, ok = _read_fits_header(in_f)
        if not ok:
            raise SystemExit(f"Invalid FITS primary header: {raw_path}")
        extension_cards, ok = _read_fits_header(in_f)
        if not ok:
            raise SystemExit(f"Invalid FITS extension header: {raw_path}")
        card_map: dict[str, str] = {}
        for card in extension_cards:
            key = card[:8].strip()
            value = _fits_card_value(card)
            if key and value is not None:
                card_map[key] = value
        if card_map.get("XTENSION", "").strip().strip("'") != "BINTABLE":
            raise SystemExit(f"White dwarf FITS missing BINTABLE extension: {raw_path}")
        naxis1 = int(card_map.get("NAXIS1", "0"))
        naxis2 = int(card_map.get("NAXIS2", "0"))
        tfields = int(card_map.get("TFIELDS", "0"))
        if naxis1 <= 0 or naxis2 <= 0 or tfields <= 0:
            raise SystemExit(f"White dwarf FITS missing table dimensions: {raw_path}")

        column_offsets: dict[str, tuple[int, int, str, int]] = {}
        running_offset = 0
        for idx in range(1, tfields + 1):
            name = (card_map.get(f"TTYPE{idx}") or "").strip()
            tform = (card_map.get(f"TFORM{idx}") or "").strip()
            if not name or not tform:
                continue
            width, code, repeat = _fits_tform_width(tform)
            column_offsets[name] = (running_offset, width, code, repeat)
            running_offset += width
        if running_offset != naxis1:
            raise SystemExit(
                f"White dwarf FITS row width mismatch: header={naxis1} parsed={running_offset}"
            )
        required_columns = [
            "source_id",
            "WDJ_name",
            "designation",
            "ra",
            "dec",
            "parallax",
            "parallax_error",
            "parallax_over_error",
            "Pwd",
            "ruwe",
            "teff_H",
            "logg_H",
            "mass_H",
            "chisq_H",
            "teff_He",
            "logg_He",
            "mass_He",
            "chisq_He",
            "phot_g_mean_mag",
            "phot_bp_mean_mag",
            "phot_rp_mean_mag",
            "bp_rp",
        ]
        missing = [name for name in required_columns if name not in column_offsets]
        if missing:
            raise SystemExit(
                f"White dwarf FITS missing required columns: {', '.join(sorted(missing))}"
            )

        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "source_id",
                "wdj_name",
                "designation",
                "ra_deg",
                "dec_deg",
                "parallax_mas",
                "parallax_error_mas",
                "parallax_over_error",
                "pwd",
                "ruwe",
                "fit_model",
                "teff_best_k",
                "logg_best_cgs",
                "mass_best_msun",
                "teff_h_k",
                "logg_h_cgs",
                "mass_h_msun",
                "chisq_h",
                "teff_he_k",
                "logg_he_cgs",
                "mass_he_msun",
                "chisq_he",
                "phot_g_mag",
                "phot_bp_mag",
                "phot_rp_mag",
                "bp_rp",
            ],
        )
        writer.writeheader()

        chunk_rows = 25000
        for base in range(0, naxis2, chunk_rows):
            take = min(chunk_rows, naxis2 - base)
            payload = in_f.read(take * naxis1)
            if len(payload) != take * naxis1:
                raise SystemExit(f"Unexpected EOF while reading {raw_path} at row {base}")
            for row_idx in range(take):
                row = payload[row_idx * naxis1 : (row_idx + 1) * naxis1]

                def scalar(name: str):
                    offset, width, code, repeat = column_offsets[name]
                    return _fits_scalar_from_row(row, offset, width, code, repeat)

                source_id = scalar("source_id")
                if not isinstance(source_id, int) or source_id <= 0:
                    continue
                pwd = scalar("Pwd")
                teff_h = scalar("teff_H")
                teff_he = scalar("teff_He")
                logg_h = scalar("logg_H")
                logg_he = scalar("logg_He")
                mass_h = scalar("mass_H")
                mass_he = scalar("mass_He")
                chisq_h = scalar("chisq_H")
                chisq_he = scalar("chisq_He")
                fit_model = ""
                use_h = isinstance(chisq_h, float)
                use_he = isinstance(chisq_he, float)
                if use_h and use_he:
                    fit_model = "H" if chisq_h <= chisq_he else "He"
                elif use_h:
                    fit_model = "H"
                elif use_he:
                    fit_model = "He"
                teff_best = teff_h if fit_model == "H" else teff_he if fit_model == "He" else None
                logg_best = logg_h if fit_model == "H" else logg_he if fit_model == "He" else None
                mass_best = mass_h if fit_model == "H" else mass_he if fit_model == "He" else None
                writer.writerow(
                    {
                        "source_id": source_id,
                        "wdj_name": scalar("WDJ_name"),
                        "designation": scalar("designation"),
                        "ra_deg": scalar("ra"),
                        "dec_deg": scalar("dec"),
                        "parallax_mas": scalar("parallax"),
                        "parallax_error_mas": scalar("parallax_error"),
                        "parallax_over_error": scalar("parallax_over_error"),
                        "pwd": pwd if isinstance(pwd, float) and math.isfinite(pwd) else None,
                        "ruwe": scalar("ruwe"),
                        "fit_model": fit_model or None,
                        "teff_best_k": teff_best,
                        "logg_best_cgs": logg_best,
                        "mass_best_msun": mass_best,
                        "teff_h_k": teff_h,
                        "logg_h_cgs": logg_h,
                        "mass_h_msun": mass_h,
                        "chisq_h": chisq_h,
                        "teff_he_k": teff_he,
                        "logg_he_cgs": logg_he,
                        "mass_he_msun": mass_he,
                        "chisq_he": chisq_he,
                        "phot_g_mag": scalar("phot_g_mean_mag"),
                        "phot_bp_mag": scalar("phot_bp_mean_mag"),
                        "phot_rp_mag": scalar("phot_rp_mean_mag"),
                        "bp_rp": scalar("bp_rp"),
                    }
                )
                row_count += 1

        # Consume extension padding if present.
        data_len = naxis1 * naxis2
        pad = (FITS_BLOCK_BYTES - (data_len % FITS_BLOCK_BYTES)) % FITS_BLOCK_BYTES
        if pad:
            in_f.read(pad)
    return row_count


def cook_gaia_classprob(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "source_id",
                "classprob_dsc_combmod_whitedwarf",
                "classprob_dsc_specmod_whitedwarf",
                "classprob_dsc_combmod_star",
                "classprob_dsc_specmod_star",
                "classprob_dsc_combmod_binarystar",
                "classprob_dsc_specmod_binarystar",
                "classprob_dsc_combmod_galaxy",
                "classprob_dsc_specmod_galaxy",
                "classprob_dsc_combmod_quasar",
                "classprob_dsc_specmod_quasar",
            ],
        )
        writer.writeheader()
        for row in reader:
            source_id = parse_int(row.get("source_id"))
            if source_id is None:
                continue
            writer.writerow(
                {
                    "source_id": source_id,
                    "classprob_dsc_combmod_whitedwarf": parse_float(
                        row.get("classprob_dsc_combmod_whitedwarf")
                    ),
                    "classprob_dsc_specmod_whitedwarf": parse_float(
                        row.get("classprob_dsc_specmod_whitedwarf")
                    ),
                    "classprob_dsc_combmod_star": parse_float(
                        row.get("classprob_dsc_combmod_star")
                    ),
                    "classprob_dsc_specmod_star": parse_float(
                        row.get("classprob_dsc_specmod_star")
                    ),
                    "classprob_dsc_combmod_binarystar": parse_float(
                        row.get("classprob_dsc_combmod_binarystar")
                    ),
                    "classprob_dsc_specmod_binarystar": parse_float(
                        row.get("classprob_dsc_specmod_binarystar")
                    ),
                    "classprob_dsc_combmod_galaxy": parse_float(
                        row.get("classprob_dsc_combmod_galaxy")
                    ),
                    "classprob_dsc_specmod_galaxy": parse_float(
                        row.get("classprob_dsc_specmod_galaxy")
                    ),
                    "classprob_dsc_combmod_quasar": parse_float(
                        row.get("classprob_dsc_combmod_quasar")
                    ),
                    "classprob_dsc_specmod_quasar": parse_float(
                        row.get("classprob_dsc_specmod_quasar")
                    ),
                }
            )
            row_count += 1
    return row_count


def _parse_atnf_block(block_lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in block_lines:
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("@"):
            break
        key = raw.split()[0]
        value = raw[len(key) :].strip()
        if key and value:
            fields[key] = value
    return fields


def _type_from_atnf(type_value: str) -> str:
    text = (type_value or "").upper()
    if "MAGNETAR" in text:
        return "magnetar"
    return "pulsar"


def cook_atnf(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with tarfile.open(raw_path, "r:gz") as archive:
        member = archive.getmember("psrcat_tar/psrcat.db")
        with archive.extractfile(member) as handle, cooked_path.open(
            "w", newline="", encoding="utf-8"
        ) as out_f:
            if handle is None:
                raise SystemExit("ATNF archive missing psrcat_tar/psrcat.db")
            writer = csv.DictWriter(
                out_f,
                fieldnames=[
                    "psrj",
                    "psrb",
                    "ra_deg",
                    "dec_deg",
                    "parallax_mas",
                    "distance_pc",
                    "type_raw",
                    "assoc_raw",
                    "period_s",
                    "period_derivative",
                    "spin_frequency_hz",
                    "spin_frequency_derivative_hz_s",
                    "object_type",
                ],
            )
            writer.writeheader()
            block_lines: list[str] = []
            for raw_line in handle.read().decode("utf-8", "replace").splitlines():
                if raw_line.startswith("@"):
                    if block_lines:
                        fields = _parse_atnf_block(block_lines)
                        psrj = first_token(fields.get("PSRJ", ""))
                        psrb = first_token(fields.get("PSRB", ""))
                        ra_deg = parse_hms_to_deg(first_token(fields.get("RAJ", "")))
                        dec_deg = parse_dms_to_deg(first_token(fields.get("DECJ", "")))
                        parallax_mas = parse_float(first_token(fields.get("PX", "")))
                        dist_kpc = (
                            parse_float(first_token(fields.get("DIST", "")))
                            or parse_float(first_token(fields.get("DIST_DM", "")))
                            or parse_float(first_token(fields.get("DIST_AMN", "")))
                        )
                        distance_pc = None
                        if parallax_mas is not None and parallax_mas > 0:
                            distance_pc = 1000.0 / parallax_mas
                        elif dist_kpc is not None and dist_kpc > 0:
                            distance_pc = dist_kpc * 1000.0
                        period_s = parse_float(first_token(fields.get("P0", "")))
                        period_derivative = parse_float(first_token(fields.get("P1", "")))
                        spin_freq_hz = parse_float(first_token(fields.get("F0", "")))
                        spin_freq_deriv = parse_float(first_token(fields.get("F1", "")))
                        type_raw = fields.get("TYPE", "")
                        assoc_raw = fields.get("ASSOC", "")
                        if psrj or psrb:
                            writer.writerow(
                                {
                                    "psrj": psrj,
                                    "psrb": psrb,
                                    "ra_deg": ra_deg,
                                    "dec_deg": dec_deg,
                                    "parallax_mas": parallax_mas,
                                    "distance_pc": distance_pc,
                                    "type_raw": type_raw,
                                    "assoc_raw": assoc_raw,
                                    "period_s": period_s,
                                    "period_derivative": period_derivative,
                                    "spin_frequency_hz": spin_freq_hz,
                                    "spin_frequency_derivative_hz_s": spin_freq_deriv,
                                    "object_type": _type_from_atnf(type_raw),
                                }
                            )
                            row_count += 1
                    block_lines = []
                    continue
                block_lines.append(raw_line.rstrip("\n"))
    return row_count


def cook_magnetar(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "name",
                "ra_deg",
                "dec_deg",
                "distance_pc",
                "period_s",
                "period_dot",
                "assoc_raw",
                "activity_raw",
                "bands_raw",
            ],
        )
        writer.writeheader()
        for row in reader:
            writer.writerow(
                {
                    "name": str(row.get("Name", "")).strip(),
                    "ra_deg": parse_hms_to_deg(row.get("RA")),
                    "dec_deg": parse_dms_to_deg(row.get("Decl")),
                    "distance_pc": (
                        parse_float(row.get("Dist")) * 1000.0
                        if parse_float(row.get("Dist")) is not None
                        else None
                    ),
                    "period_s": parse_float(row.get("Period")),
                    "period_dot": parse_float(row.get("Pdot")),
                    "assoc_raw": str(row.get("Assoc", "")).strip(),
                    "activity_raw": str(row.get("Activity", "")).strip(),
                    "bands_raw": str(row.get("Bands", "")).strip(),
                }
            )
            row_count += 1
    return row_count


def cook_gaia_ucd(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with raw_path.open("r", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "source_id",
                "hmac_cluster_id",
                "banyan_cluster",
                "banyan_probability",
            ],
        )
        writer.writeheader()
        for line in in_f:
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            source_id = parse_int(raw[0:19])
            if source_id is None:
                continue
            hmac_cluster_id = parse_int(raw[22:24])
            banyan_cluster = raw[28:35].strip()
            if banyan_cluster in {"--", "-", ""}:
                banyan_cluster = ""
            writer.writerow(
                {
                    "source_id": source_id,
                    "hmac_cluster_id": hmac_cluster_id,
                    "banyan_cluster": banyan_cluster or None,
                    "banyan_probability": parse_float(raw[36:40]),
                }
            )
            row_count += 1
    return row_count


def extract_gaia_source_id(value: str | None) -> tuple[int | None, str | None]:
    text = str(value or "").strip()
    if not text:
        return (None, None)
    match = re.search(r"Gaia\s+DR([23])\s+([0-9]{8,20})", text, flags=re.IGNORECASE)
    if not match:
        return (None, None)
    release = f"DR{match.group(1)}"
    source_id = parse_int(match.group(2))
    return (source_id, release)


def normalize_variability_family(var_type_raw: str | None) -> str:
    token = (var_type_raw or "").strip().upper()
    if not token:
        return "unknown"
    if token.startswith(("EA", "EB", "EW", "ELL")):
        return "eclipsing"
    if token.startswith(
        (
            "RR",
            "CEP",
            "DCEP",
            "MIRA",
            "SR",
            "DSCT",
            "SXPHE",
            "GDOR",
            "BCEP",
            "SPB",
        )
    ):
        return "pulsating"
    if token.startswith(("ROT", "BY", "ACV", "SXARI")):
        return "rotational"
    if token.startswith(("UG", "UV", "N", "SN", "CV", "AM", "DQ", "ZAND", "RCB")):
        return "eruptive"
    if token.startswith(("L", "QSO", "AGN")):
        return "extragalactic_or_lensing"
    return "other"


def cook_vsx(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with open_text(raw_path) as in_f, cooked_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "vsx_oid",
                "name",
                "variability_flag",
                "ra_deg",
                "dec_deg",
                "variability_type_raw",
                "variability_family",
                "max_mag",
                "max_passband",
                "min_is_amplitude_flag",
                "min_mag_or_amplitude",
                "min_passband",
                "epoch_hjd",
                "period_days",
                "spectral_type",
                "gaia_source_id",
                "gaia_release",
            ],
        )
        writer.writeheader()
        for line in in_f:
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            # VSX is fixed-width 209 chars in CDS ReadMe; tolerate short lines.
            fixed = raw.ljust(209)
            name = clean_text(fixed[9:39])
            gaia_source_id, gaia_release = extract_gaia_source_id(name)
            var_type = clean_text(fixed[62:92])
            writer.writerow(
                {
                    "vsx_oid": parse_int(fixed[0:8]),
                    "name": name,
                    "variability_flag": parse_int(fixed[40:41]),
                    "ra_deg": parse_float(fixed[42:51]),
                    "dec_deg": parse_float(fixed[52:61]),
                    "variability_type_raw": var_type,
                    "variability_family": normalize_variability_family(var_type),
                    "max_mag": parse_float(fixed[95:102]),
                    "max_passband": clean_text(fixed[105:115]),
                    "min_is_amplitude_flag": clean_text(fixed[116:117]),
                    "min_mag_or_amplitude": parse_float(fixed[120:127]),
                    "min_passband": clean_text(fixed[130:138]),
                    "epoch_hjd": parse_float(fixed[139:153]),
                    "period_days": parse_float(fixed[158:177]),
                    "spectral_type": clean_text(fixed[180:209]),
                    "gaia_source_id": gaia_source_id,
                    "gaia_release": gaia_release,
                }
            )
            row_count += 1
    return row_count


def cook_ultracoolsheet(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with open_text(raw_path) as in_f, cooked_path.open("w", newline="", encoding="utf-8") as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "source_row_num",
                "object_name",
                "name_simbadable",
                "gaia_dr3_source_id",
                "gaia_dr2_source_id",
                "ra_j2000_deg",
                "dec_j2000_deg",
                "plx_mas",
                "pmra_mas_yr",
                "pmdec_mas_yr",
                "rv_kms",
                "dist_pc",
                "dist_source",
                "spectral_type_opt",
                "spectral_type_ir",
                "spectral_numeric",
                "gravity_opt",
                "gravity_ir",
                "age_category",
                "youth_evidence",
                "banyan_hypothesis_young",
                "banyan_prob_young",
                "is_exoplanet_host_flag",
                "multiple_unresolved_flag",
                "multiple_resolved_flag",
                "has_higher_mass_companion_flag",
                "ref_discovery",
                "source_url",
            ],
        )
        writer.writeheader()
        for idx, row in enumerate(reader, start=1):
            gaia_dr3 = parse_int(clean_text(row.get("sourceID_Gaia_DR3")))
            gaia_dr2 = parse_int(clean_text(row.get("sourceID_Gaia_DR2")))
            writer.writerow(
                {
                    "source_row_num": idx,
                    "object_name": clean_text(row.get("name")),
                    "name_simbadable": clean_text(row.get("name_simbadable")),
                    "gaia_dr3_source_id": gaia_dr3,
                    "gaia_dr2_source_id": gaia_dr2,
                    "ra_j2000_deg": parse_float(clean_text(row.get("ra_j2000_formula"))),
                    "dec_j2000_deg": parse_float(clean_text(row.get("dec_j2000_formula"))),
                    "plx_mas": parse_float(clean_text(row.get("plx_formula"))),
                    "pmra_mas_yr": parse_float(clean_text(row.get("pmra_formula"))),
                    "pmdec_mas_yr": parse_float(clean_text(row.get("pmdec_formula"))),
                    "rv_kms": parse_float(clean_text(row.get("rv_formula"))),
                    "dist_pc": parse_float(clean_text(row.get("dist_formula"))),
                    "dist_source": clean_text(row.get("dist_formula_source")),
                    "spectral_type_opt": clean_text(row.get("spt_opt")),
                    "spectral_type_ir": clean_text(row.get("spt_ir")),
                    "spectral_numeric": parse_float(clean_text(row.get("sptnum_formula"))),
                    "gravity_opt": clean_text(row.get("grav_opt")),
                    "gravity_ir": clean_text(row.get("grav_ir")),
                    "age_category": clean_text(row.get("age_category")),
                    "youth_evidence": clean_text(row.get("youth_evidence")),
                    "banyan_hypothesis_young": clean_text(row.get("banyan_sigma_max_hypo_young")),
                    "banyan_prob_young": parse_float(clean_text(row.get("banyan_sigma_max_prob_young"))),
                    "is_exoplanet_host_flag": clean_text(row.get("exoplanet")),
                    "multiple_unresolved_flag": clean_text(row.get("multiplesystem_unresolved_in_this_table")),
                    "multiple_resolved_flag": clean_text(row.get("multiplesystem_resolved_in_this_table")),
                    "has_higher_mass_companion_flag": clean_text(row.get("has_higher_mass_companion")),
                    "ref_discovery": clean_text(row.get("ref_discovery")),
                    "source_url": clean_text(row.get("url_simpleDB")),
                }
            )
            row_count += 1
    return row_count


def cook_open_clusters_table1(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with open_text(raw_path) as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "cluster_name",
                "ra_deg",
                "dec_deg",
                "glon_deg",
                "glat_deg",
                "radius_r50_deg",
                "member_count_prob_gt_0_7",
                "pm_ra_mas_yr",
                "pm_ra_sigma_mas_yr",
                "pm_dec_mas_yr",
                "pm_dec_sigma_mas_yr",
                "parallax_mas",
                "parallax_sigma_mas",
                "flag",
                "age_log_yr",
                "av_mag",
                "distance_modulus_mag",
                "distance_pc",
                "x_gal_pc",
                "y_gal_pc",
                "z_gal_pc",
                "rgc_pc",
            ],
        )
        writer.writeheader()
        for line in in_f:
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            writer.writerow(
                {
                    "cluster_name": raw[0:17].strip(),
                    "ra_deg": parse_float(raw[18:25]),
                    "dec_deg": parse_float(raw[26:33]),
                    "glon_deg": parse_float(raw[34:41]),
                    "glat_deg": parse_float(raw[42:49]),
                    "radius_r50_deg": parse_float(raw[50:55]),
                    "member_count_prob_gt_0_7": parse_int(raw[56:60]),
                    "pm_ra_mas_yr": parse_float(raw[61:68]),
                    "pm_ra_sigma_mas_yr": parse_float(raw[69:75]),
                    "pm_dec_mas_yr": parse_float(raw[76:83]),
                    "pm_dec_sigma_mas_yr": parse_float(raw[84:90]),
                    "parallax_mas": parse_float(raw[91:97]),
                    "parallax_sigma_mas": parse_float(raw[98:103]),
                    "flag": raw[104:118].strip(),
                    "age_log_yr": parse_float(raw[119:123]),
                    "av_mag": parse_float(raw[124:129]),
                    "distance_modulus_mag": parse_float(raw[130:135]),
                    "distance_pc": parse_float(raw[136:142]),
                    "x_gal_pc": parse_float(raw[143:150]),
                    "y_gal_pc": parse_float(raw[151:158]),
                    "z_gal_pc": parse_float(raw[159:165]),
                    "rgc_pc": parse_float(raw[166:172]),
                }
            )
            row_count += 1
    return row_count


def cook_open_cluster_members(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with open_text(raw_path) as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "gaia_dr2_source_id",
                "cluster_name",
                "membership_probability",
                "ra_deg",
                "dec_deg",
            ],
        )
        writer.writeheader()
        for line in in_f:
            raw = line.rstrip("\n")
            if not raw.strip():
                continue
            gaia_dr2 = parse_int(raw[44:63])
            cluster_name = raw[475:492].strip()
            if gaia_dr2 is None or not cluster_name:
                continue
            writer.writerow(
                {
                    "gaia_dr2_source_id": gaia_dr2,
                    "cluster_name": cluster_name,
                    "membership_probability": parse_float(raw[467:474]),
                    "ra_deg": parse_float(raw[0:21]),
                    "dec_deg": parse_float(raw[22:43]),
                }
            )
            row_count += 1
    return row_count


def cook_snr(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    text = raw_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"<PRE>(.*?)</PRE>", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return 0
    block = html.unescape(m.group(1))
    row_count = 0
    with cooked_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "galactic_name",
                "glon_deg",
                "glat_deg",
                "ra_deg",
                "dec_deg",
                "size_major_arcmin",
                "size_minor_arcmin",
                "morphology_type",
                "flux_1ghz_jy_raw",
                "spectral_index_raw",
                "other_names",
            ],
        )
        writer.writeheader()
        for line in block.splitlines():
            clean = re.sub(r"<[^>]+>", " ", line)
            clean = re.sub(r"\s+", " ", clean).strip()
            if not clean:
                continue
            if clean.startswith("l b") or clean.startswith("/deg") or set(clean) == {"-"}:
                continue
            tokens = clean.split(" ")
            if len(tokens) < 11:
                continue
            glon = parse_float(tokens[0])
            glat = parse_float(tokens[1])
            if glon is None or glat is None:
                continue
            ra_h = parse_float(tokens[2])
            ra_m = parse_float(tokens[3])
            ra_s = parse_float(tokens[4])
            dec_d = parse_float(tokens[5])
            dec_m = parse_float(tokens[6])
            if None in (ra_h, ra_m, ra_s, dec_d, dec_m):
                continue
            ra_deg = (float(ra_h) + float(ra_m) / 60.0 + float(ra_s) / 3600.0) * 15.0
            dec_sign = -1.0 if str(tokens[5]).startswith("-") else 1.0
            dec_deg = dec_sign * (abs(float(dec_d)) + float(dec_m) / 60.0)

            size_token = tokens[7]
            if "x" in size_token.lower():
                parts = re.split(r"[xX]", size_token, maxsplit=1)
                size_major = parse_float(parts[0])
                size_minor = parse_float(parts[1])
            else:
                size_major = parse_float(size_token)
                size_minor = size_major
            writer.writerow(
                {
                    "galactic_name": f"G{glon:+.1f}{glat:+.1f}".replace("+", "+"),
                    "glon_deg": glon,
                    "glat_deg": glat,
                    "ra_deg": ra_deg,
                    "dec_deg": dec_deg,
                    "size_major_arcmin": size_major,
                    "size_minor_arcmin": size_minor,
                    "morphology_type": tokens[8],
                    "flux_1ghz_jy_raw": tokens[9],
                    "spectral_index_raw": tokens[10],
                    "other_names": " ".join(tokens[11:]).strip(),
                }
            )
            row_count += 1
    return row_count


def cook_debcat(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with raw_path.open("r", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "system_name",
                "spectral_type_primary",
                "spectral_type_secondary",
                "period_days",
                "vmag",
                "b_minus_v",
                "mass_primary_msun",
                "mass_primary_err_msun",
                "mass_secondary_msun",
                "mass_secondary_err_msun",
                "radius_primary_rsun",
                "radius_primary_err_rsun",
                "radius_secondary_rsun",
                "radius_secondary_err_rsun",
                "logg_primary_cgs",
                "logg_primary_err_cgs",
                "logg_secondary_cgs",
                "logg_secondary_err_cgs",
                "teff_primary_k",
                "teff_primary_err_k",
                "teff_secondary_k",
                "teff_secondary_err_k",
                "lum_primary_lsun",
                "lum_primary_err_lsun",
                "lum_secondary_lsun",
                "lum_secondary_err_lsun",
                "metallicity_dex",
                "metallicity_err_dex",
            ],
        )
        writer.writeheader()
        for raw in in_f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            tokens = line.split()
            if len(tokens) < 28:
                continue
            writer.writerow(
                {
                    "system_name": tokens[0].replace("_", " "),
                    "spectral_type_primary": tokens[1],
                    "spectral_type_secondary": tokens[2],
                    "period_days": parse_with_missing_sentinel(tokens[3]),
                    "vmag": parse_with_missing_sentinel(tokens[4]),
                    "b_minus_v": parse_with_missing_sentinel(tokens[5]),
                    "mass_primary_msun": parse_log_quantity(tokens[6]),
                    "mass_primary_err_msun": parse_with_missing_sentinel(tokens[7]),
                    "mass_secondary_msun": parse_log_quantity(tokens[8]),
                    "mass_secondary_err_msun": parse_with_missing_sentinel(tokens[9]),
                    "radius_primary_rsun": parse_log_quantity(tokens[10]),
                    "radius_primary_err_rsun": parse_with_missing_sentinel(tokens[11]),
                    "radius_secondary_rsun": parse_log_quantity(tokens[12]),
                    "radius_secondary_err_rsun": parse_with_missing_sentinel(tokens[13]),
                    "logg_primary_cgs": parse_with_missing_sentinel(tokens[14]),
                    "logg_primary_err_cgs": parse_with_missing_sentinel(tokens[15]),
                    "logg_secondary_cgs": parse_with_missing_sentinel(tokens[16]),
                    "logg_secondary_err_cgs": parse_with_missing_sentinel(tokens[17]),
                    "teff_primary_k": parse_log_quantity(tokens[18]),
                    "teff_primary_err_k": parse_with_missing_sentinel(tokens[19]),
                    "teff_secondary_k": parse_log_quantity(tokens[20]),
                    "teff_secondary_err_k": parse_with_missing_sentinel(tokens[21]),
                    "lum_primary_lsun": parse_log_quantity(tokens[22]),
                    "lum_primary_err_lsun": parse_with_missing_sentinel(tokens[23]),
                    "lum_secondary_lsun": parse_log_quantity(tokens[24]),
                    "lum_secondary_err_lsun": parse_with_missing_sentinel(tokens[25]),
                    "metallicity_dex": parse_with_missing_sentinel(tokens[26]),
                    "metallicity_err_dex": parse_with_missing_sentinel(tokens[27]),
                }
            )
            row_count += 1
    return row_count


def cook_kepler_eb(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    text = raw_path.read_text(encoding="utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]

    header_line = None
    for line in lines:
        if line.startswith("#KIC,") or line.startswith("KIC,"):
            header_line = line.lstrip("#")
            break
    if not header_line:
        return 0

    data_lines: list[str] = [header_line]
    for line in lines:
        if line.startswith("#"):
            continue
        if line == header_line or line == "#" + header_line:
            continue
        if "," in line:
            data_lines.append(line)

    reader = csv.DictReader(data_lines)
    row_count = 0
    with cooked_path.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "kic_id",
                "period_days",
                "period_error_days",
                "bjd0",
                "bjd0_error",
                "morphology",
                "glon_deg",
                "glat_deg",
                "kmag",
                "teff_k",
                "has_short_cadence",
            ],
        )
        writer.writeheader()
        for row in reader:
            kic_value = parse_int(row.get("KIC"))
            if kic_value is None:
                continue
            writer.writerow(
                {
                    "kic_id": kic_value,
                    "period_days": parse_with_missing_sentinel(row.get("period")),
                    "period_error_days": parse_with_missing_sentinel(row.get("period_err")),
                    "bjd0": parse_with_missing_sentinel(row.get("bjd0")),
                    "bjd0_error": parse_with_missing_sentinel(row.get("bjd0_err")),
                    "morphology": parse_with_missing_sentinel(row.get("morph")),
                    "glon_deg": parse_with_missing_sentinel(row.get("GLon")),
                    "glat_deg": parse_with_missing_sentinel(row.get("GLat")),
                    "kmag": parse_with_missing_sentinel(row.get("kmag")),
                    "teff_k": parse_with_missing_sentinel(row.get("Teff")),
                    "has_short_cadence": parse_bool(row.get("SC")),
                }
            )
            row_count += 1
    return row_count


def cook_tess_eb(raw_path: Path, cooked_path: Path) -> int:
    cooked_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with raw_path.open("r", newline="", encoding="utf-8", errors="replace") as in_f, cooked_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        writer = csv.DictWriter(
            out_f,
            fieldnames=[
                "tic_id",
                "in_catalog",
                "sectors",
                "ra_deg",
                "dec_deg",
                "glon_deg",
                "glat_deg",
                "pm_ra_mas_yr",
                "pm_dec_mas_yr",
                "tmag",
                "teff_k",
                "logg_cgs",
                "metallicity_dex",
                "bjd0",
                "bjd0_error",
                "period_days",
                "period_error_days",
                "morphology",
                "source",
                "flags",
            ],
        )
        writer.writeheader()
        for row in reader:
            tic_id = parse_int(row.get("tic_id"))
            if tic_id is None:
                continue
            writer.writerow(
                {
                    "tic_id": tic_id,
                    "in_catalog": parse_bool(row.get("in_catalog")),
                    "sectors": (row.get("sectors") or "").strip(),
                    # Coordinates/proper motion can be strongly negative; do not apply the <= -9 sentinel rule here.
                    "ra_deg": parse_float(row.get("ra_deg")),
                    "dec_deg": parse_float(row.get("dec_deg")),
                    "glon_deg": parse_float(row.get("glon_deg")),
                    "glat_deg": parse_float(row.get("glat_deg")),
                    "pm_ra_mas_yr": parse_float(row.get("pm_ra_mas_yr")),
                    "pm_dec_mas_yr": parse_float(row.get("pm_dec_mas_yr")),
                    "tmag": parse_with_missing_sentinel(row.get("tmag")),
                    "teff_k": parse_with_missing_sentinel(row.get("teff_k")),
                    "logg_cgs": parse_with_missing_sentinel(row.get("logg_cgs")),
                    "metallicity_dex": parse_with_missing_sentinel(row.get("metallicity_dex")),
                    "bjd0": parse_with_missing_sentinel(row.get("bjd0")),
                    "bjd0_error": parse_with_missing_sentinel(row.get("bjd0_error")),
                    "period_days": parse_with_missing_sentinel(row.get("period_days")),
                    "period_error_days": parse_with_missing_sentinel(row.get("period_error_days")),
                    "morphology": parse_with_missing_sentinel(row.get("morphology")),
                    "source": (row.get("source") or "").strip(),
                    "flags": (row.get("flags") or "").strip(),
                }
            )
            row_count += 1
    return row_count


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    state_dir = Path(os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or root / "data")
    raw_dir = state_dir / "raw"
    cooked_dir = state_dir / "cooked"

    jobs = [
        (
            "gaia_classprob",
            raw_dir / "gaia_classprob" / "gaia_dr3_astrophysical_classprob.csv",
            cooked_dir / "gaia_classprob" / "gaia_dr3_astrophysical_classprob.csv",
            cook_gaia_classprob,
        ),
        (
            "atnf",
            raw_dir / "atnf" / "psrcat_pkg.tar.gz",
            cooked_dir / "atnf" / "pulsars.csv",
            cook_atnf,
        ),
        (
            "magnetar",
            raw_dir / "magnetar" / "TabO1.csv",
            cooked_dir / "magnetar" / "magnetars.csv",
            cook_magnetar,
        ),
        (
            "white_dwarf",
            raw_dir / "white_dwarf" / "gaiaedr3_wd_main.fits.gz",
            cooked_dir / "white_dwarf" / "gaiaedr3_white_dwarf.csv",
            cook_white_dwarf,
        ),
        (
            "gaia_ucd",
            raw_dir / "gaia_ucd" / "table4.dat",
            cooked_dir / "gaia_ucd" / "gaia_ucd_memberships.csv",
            cook_gaia_ucd,
        ),
        (
            "vsx",
            raw_dir / "vsx" / "vsx.dat",
            cooked_dir / "vsx" / "vsx_variability.csv",
            cook_vsx,
        ),
        (
            "ultracoolsheet",
            raw_dir / "ultracoolsheet" / "ultracoolsheet_main.csv",
            cooked_dir / "ultracoolsheet" / "ultracoolsheet_objects.csv",
            cook_ultracoolsheet,
        ),
        (
            "clusters",
            raw_dir / "clusters" / "table1.dat",
            cooked_dir / "clusters" / "open_clusters.csv",
            cook_open_clusters_table1,
        ),
        (
            "cluster_members",
            raw_dir / "clusters" / "nodup.dat.gz",
            cooked_dir / "clusters" / "open_cluster_members.csv",
            cook_open_cluster_members,
        ),
        (
            "snr",
            raw_dir / "snr" / "snrs.data.html",
            cooked_dir / "snr" / "green_snr.csv",
            cook_snr,
        ),
        (
            "debcat",
            raw_dir / "debcat" / "debs.dat",
            cooked_dir / "debcat" / "debcat_binaries.csv",
            cook_debcat,
        ),
        (
            "kepler_eb",
            raw_dir / "kepler_eb" / "kepler_eb_catalog.csv",
            cooked_dir / "kepler_eb" / "kepler_eb_catalog.csv",
            cook_kepler_eb,
        ),
        (
            "tess_eb",
            raw_dir / "tess_eb" / "tess_eb_catalog.csv",
            cooked_dir / "tess_eb" / "tess_eb_catalog.csv",
            cook_tess_eb,
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
