#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

from horizons_snapshot import (
    ResponseCapture,
    SOL_AUTHORITY_RESPONSE_SOURCE_NAME,
    SOL_AUTHORITY_TABLE_SOURCE_NAME,
    center_target_command,
    parse_horizons_elements,
    seed_sha256,
    write_horizons_snapshot,
)

HORIZONS_API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
OPERATOR_SEED_VERSION = "sol_authority_bootstrap_v1"

# Sol authority scope:
# - Sun
# - 8 major planets
# - 5 commonly accepted dwarf planets (UI supergroup alias retained)
# - S2 bootstrap moons for hierarchy/orbit graphing in arm
# - S3 named small-body bootstrap (minor bodies with deterministic family tags)
OBJECTS = [
    {"source_pk": 1, "name": "Sun", "object_class": "star", "object_kind": "star", "command": "10", "center": "500@0"},
    {"source_pk": 2, "name": "Mercury", "object_class": "planet", "object_kind": "planet", "command": "199", "center": "500@10"},
    {"source_pk": 3, "name": "Venus", "object_class": "planet", "object_kind": "planet", "command": "299", "center": "500@10"},
    {"source_pk": 4, "name": "Earth", "object_class": "planet", "object_kind": "planet", "command": "399", "center": "500@10"},
    {"source_pk": 5, "name": "Mars", "object_class": "planet", "object_kind": "planet", "command": "499", "center": "500@10"},
    {"source_pk": 6, "name": "Jupiter", "object_class": "planet", "object_kind": "planet", "command": "599", "center": "500@10"},
    {"source_pk": 7, "name": "Saturn", "object_class": "planet", "object_kind": "planet", "command": "699", "center": "500@10"},
    {"source_pk": 8, "name": "Uranus", "object_class": "planet", "object_kind": "planet", "command": "799", "center": "500@10"},
    {"source_pk": 9, "name": "Neptune", "object_class": "planet", "object_kind": "planet", "command": "899", "center": "500@10"},
    {"source_pk": 10, "name": "Pluto", "object_class": "dwarf_planet", "object_kind": "dwarf_planet", "command": "999", "center": "500@10"},
    {"source_pk": 11, "name": "Ceres", "object_class": "dwarf_planet", "object_kind": "dwarf_planet", "command": "1;", "center": "500@10"},
    {"source_pk": 12, "name": "Eris", "object_class": "dwarf_planet", "object_kind": "dwarf_planet", "command": "136199;", "center": "500@10"},
    {"source_pk": 13, "name": "Haumea", "object_class": "dwarf_planet", "object_kind": "dwarf_planet", "command": "136108;", "center": "500@10"},
    {"source_pk": 14, "name": "Makemake", "object_class": "dwarf_planet", "object_kind": "dwarf_planet", "command": "136472;", "center": "500@10"},
    {"source_pk": 15, "name": "Moon", "object_class": "moon", "object_kind": "moon", "command": "301", "center": "500@399"},
    {"source_pk": 16, "name": "Phobos", "object_class": "moon", "object_kind": "moon", "command": "401", "center": "500@499"},
    {"source_pk": 17, "name": "Deimos", "object_class": "moon", "object_kind": "moon", "command": "402", "center": "500@499"},
    {"source_pk": 18, "name": "Io", "object_class": "moon", "object_kind": "moon", "command": "501", "center": "500@599"},
    {"source_pk": 19, "name": "Europa", "object_class": "moon", "object_kind": "moon", "command": "502", "center": "500@599"},
    {"source_pk": 20, "name": "Ganymede", "object_class": "moon", "object_kind": "moon", "command": "503", "center": "500@599"},
    {"source_pk": 21, "name": "Callisto", "object_class": "moon", "object_kind": "moon", "command": "504", "center": "500@599"},
    {"source_pk": 22, "name": "Titan", "object_class": "moon", "object_kind": "moon", "command": "606", "center": "500@699"},
    {"source_pk": 23, "name": "Enceladus", "object_class": "moon", "object_kind": "moon", "command": "602", "center": "500@699"},
    {"source_pk": 24, "name": "Triton", "object_class": "moon", "object_kind": "moon", "command": "801", "center": "500@899"},
    {"source_pk": 25, "name": "Charon", "object_class": "moon", "object_kind": "moon", "command": "901", "center": "500@999"},
    {"source_pk": 101, "name": "Vesta", "object_class": "minor_body", "object_kind": "asteroid", "command": "4;", "center": "500@10"},
    {"source_pk": 102, "name": "Pallas", "object_class": "minor_body", "object_kind": "asteroid", "command": "2;", "center": "500@10"},
    {"source_pk": 103, "name": "Juno", "object_class": "minor_body", "object_kind": "asteroid", "command": "3;", "center": "500@10"},
    {"source_pk": 104, "name": "Hygiea", "object_class": "minor_body", "object_kind": "asteroid", "command": "10;", "center": "500@10"},
    {"source_pk": 105, "name": "Psyche", "object_class": "minor_body", "object_kind": "asteroid", "command": "16;", "center": "500@10"},
    {"source_pk": 106, "name": "Eros", "object_class": "minor_body", "object_kind": "asteroid", "command": "433;", "center": "500@10"},
    {"source_pk": 107, "name": "Bennu", "object_class": "minor_body", "object_kind": "asteroid", "command": "101955;", "center": "500@10"},
    {"source_pk": 108, "name": "Ryugu", "object_class": "minor_body", "object_kind": "asteroid", "command": "162173;", "center": "500@10"},
    {"source_pk": 109, "name": "Itokawa", "object_class": "minor_body", "object_kind": "asteroid", "command": "25143;", "center": "500@10"},
    {"source_pk": 110, "name": "Hebe", "object_class": "minor_body", "object_kind": "asteroid", "command": "6;", "center": "500@10"},
    {"source_pk": 111, "name": "Iris", "object_class": "minor_body", "object_kind": "asteroid", "command": "7;", "center": "500@10"},
    {"source_pk": 112, "name": "Europa", "object_class": "minor_body", "object_kind": "asteroid", "command": "52;", "center": "500@10"},
    {"source_pk": 113, "name": "Cybele", "object_class": "minor_body", "object_kind": "asteroid", "command": "65;", "center": "500@10"},
    {"source_pk": 114, "name": "Sylvia", "object_class": "minor_body", "object_kind": "asteroid", "command": "87;", "center": "500@10"},
    {"source_pk": 115, "name": "Thisbe", "object_class": "minor_body", "object_kind": "asteroid", "command": "88;", "center": "500@10"},
    {"source_pk": 116, "name": "Minerva", "object_class": "minor_body", "object_kind": "asteroid", "command": "93;", "center": "500@10"},
    {"source_pk": 117, "name": "Kleopatra", "object_class": "minor_body", "object_kind": "asteroid", "command": "216;", "center": "500@10"},
    {"source_pk": 118, "name": "Ida", "object_class": "minor_body", "object_kind": "asteroid", "command": "243;", "center": "500@10"},
    {"source_pk": 119, "name": "Mathilde", "object_class": "minor_body", "object_kind": "asteroid", "command": "253;", "center": "500@10"},
    {"source_pk": 120, "name": "Davida", "object_class": "minor_body", "object_kind": "asteroid", "command": "511;", "center": "500@10"},
    {"source_pk": 121, "name": "Interamnia", "object_class": "minor_body", "object_kind": "asteroid", "command": "704;", "center": "500@10"},
    {"source_pk": 122, "name": "Gaspra", "object_class": "minor_body", "object_kind": "asteroid", "command": "951;", "center": "500@10"},
    {"source_pk": 123, "name": "Hector", "object_class": "minor_body", "object_kind": "asteroid", "command": "624;", "center": "500@10"},
    {"source_pk": 201, "name": "Sedna", "object_class": "minor_body", "object_kind": "tno", "command": "90377;", "center": "500@10"},
    {"source_pk": 202, "name": "Quaoar", "object_class": "minor_body", "object_kind": "tno", "command": "50000;", "center": "500@10"},
    {"source_pk": 203, "name": "Orcus", "object_class": "minor_body", "object_kind": "tno", "command": "90482;", "center": "500@10"},
    {"source_pk": 204, "name": "Gonggong", "object_class": "minor_body", "object_kind": "tno", "command": "225088;", "center": "500@10"},
    {"source_pk": 205, "name": "Varuna", "object_class": "minor_body", "object_kind": "tno", "command": "20000;", "center": "500@10"},
    {"source_pk": 206, "name": "Ixion", "object_class": "minor_body", "object_kind": "tno", "command": "28978;", "center": "500@10"},
    {"source_pk": 207, "name": "Lempo", "object_class": "minor_body", "object_kind": "tno", "command": "47171;", "center": "500@10"},
    {"source_pk": 208, "name": "2002 TX300", "object_class": "minor_body", "object_kind": "tno", "command": "55636;", "center": "500@10"},
    {"source_pk": 209, "name": "Salacia", "object_class": "minor_body", "object_kind": "tno", "command": "120347;", "center": "500@10"},
    {"source_pk": 210, "name": "2004 TY364", "object_class": "minor_body", "object_kind": "tno", "command": "120348;", "center": "500@10"},
    {"source_pk": 211, "name": "Varda", "object_class": "minor_body", "object_kind": "tno", "command": "174567;", "center": "500@10"},
    {"source_pk": 301, "name": "67P/Churyumov-Gerasimenko", "object_class": "minor_body", "object_kind": "comet", "command": "1000012", "center": "500@10"},
]

PARENT_BY_NAME = {
    "moon": "Earth",
    "phobos": "Mars",
    "deimos": "Mars",
    "io": "Jupiter",
    "europa": "Jupiter",
    "ganymede": "Jupiter",
    "callisto": "Jupiter",
    "titan": "Saturn",
    "enceladus": "Saturn",
    "triton": "Neptune",
    "charon": "Pluto",
}

OBJECT_CLASS_ALIASES = {
    "dwarf_planet": '["subplanet"]',
}

SMALL_BODY_SENTINEL_RANGES = {
    "ceres": {"semi_major_axis_au": (2.5, 3.1), "orbital_period_days": (1500.0, 1900.0)},
    "vesta": {"semi_major_axis_au": (2.1, 2.6), "orbital_period_days": (1200.0, 1450.0)},
    "pallas": {"semi_major_axis_au": (2.5, 3.1), "orbital_period_days": (1500.0, 1900.0)},
    "juno": {"semi_major_axis_au": (2.4, 2.9), "orbital_period_days": (1450.0, 1750.0)},
    "hebe": {"semi_major_axis_au": (2.2, 2.7), "orbital_period_days": (1250.0, 1550.0)},
    "iris": {"semi_major_axis_au": (2.1, 2.7), "orbital_period_days": (1200.0, 1550.0)},
    "interamnia": {"semi_major_axis_au": (2.8, 3.3), "orbital_period_days": (1800.0, 2200.0)},
    "hector": {"semi_major_axis_au": (4.8, 5.6), "orbital_period_days": (3900.0, 4600.0)},
}


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def parse_float_token(raw: str | None) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    text = text.replace("~", "").strip()
    text = text.split("+-", 1)[0].strip()
    text = text.split("+/-", 1)[0].strip()
    try:
        return float(text)
    except Exception:
        return None


def api_get_payload(
    params: dict[str, str], *, timeout_s: int, retries: int
) -> tuple[bytes, str]:
    url = f"{HORIZONS_API_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read()
            return payload, url
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = min(2 ** (attempt - 1), 10)
            log(
                f"Horizons fetch retry {attempt}/{retries - 1} failed: {exc.__class__.__name__}: {exc}; sleeping {sleep_s}s"
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"Horizons request failed after {retries} attempts: {last_exc}")


def parse_physical_radius_km(payload: str) -> float | None:
    patterns = [
        r"Vol\.\s*Mean Radius\s*\(km\)\s*=\s*([~0-9eE+\-\.]+)",
        r"Vol\.\s*mean radius,\s*km\s*=\s*([~0-9eE+\-\.]+)",
        r"\bRAD\s*=\s*([~0-9eE+\-\.]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, payload, flags=re.IGNORECASE)
        if m:
            return parse_float_token(m.group(1))
    return None


def parse_mass_kg(payload: str) -> float | None:
    kg_per_km3_s2 = 1_000_000_000.0 / 6.67430e-11
    patterns = [
        r"Mass\s*x10\^([+\-]?\d+)\s*\(kg\)\s*=\s*([~0-9eE+\-\.]+)",
        r"Mass,\s*10\^([+\-]?\d+)\s*kg\s*=\s*([~0-9eE+\-\.]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, payload, flags=re.IGNORECASE)
        if not m:
            continue
        exp = int(m.group(1))
        coeff = parse_float_token(m.group(2))
        if coeff is None:
            continue
        return coeff * (10 ** exp)
    m = re.search(r"\bGM\s*=\s*([~0-9eE+\-\.]+)", payload, flags=re.IGNORECASE)
    if m:
        gm_km3_s2 = parse_float_token(m.group(1))
        if gm_km3_s2 is not None:
            return gm_km3_s2 * kg_per_km3_s2
    return None


def build_query_params(
    *,
    command: str,
    center: str,
    start_time: str,
    stop_time: str,
) -> dict[str, str]:
    return {
        "format": "text",
        "COMMAND": f"'{command}'",
        "EPHEM_TYPE": "ELEMENTS",
        "CENTER": f"'{center}'",
        "CSV_FORMAT": "YES",
        "START_TIME": f"'{start_time}'",
        "STOP_TIME": f"'{stop_time}'",
        "STEP_SIZE": "'1 d'",
        "REF_PLANE": "ECLIPTIC",
        "REF_SYSTEM": "ICRF",
        "OUT_UNITS": "AU-D",
    }


def validate_object_config() -> None:
    for obj in OBJECTS:
        object_class = str(obj["object_class"]).strip().lower()
        object_kind = str(obj.get("object_kind") or object_class).strip().lower()
        command = str(obj["command"])
        if object_kind in {"asteroid", "tno"} and not command.endswith(";"):
            raise RuntimeError(
                f"Sol authority object {obj['name']} must use Horizons small-body command syntax: {command!r}"
            )
        if object_class == "dwarf_planet" and str(obj["name"]).casefold() != "pluto" and not command.endswith(";"):
            raise RuntimeError(
                f"Sol authority dwarf small body {obj['name']} must use Horizons small-body command syntax: {command!r}"
            )


def validate_rows(rows: list[dict[str, object]]) -> None:
    rows_by_name = {str(row.get("object_name") or "").casefold(): row for row in rows}
    for name, ranges in SMALL_BODY_SENTINEL_RANGES.items():
        row = rows_by_name.get(name)
        if not row:
            raise RuntimeError(f"Sol authority sentinel {name!r} missing from fetched rows")
        for key, (low, high) in ranges.items():
            value = row.get(key)
            if value is None:
                raise RuntimeError(f"Sol authority sentinel {name!r} missing {key}")
            numeric = float(value)
            if not (low <= numeric <= high):
                raise RuntimeError(
                    f"Sol authority sentinel {name!r} has implausible {key}={numeric}; "
                    f"expected {low}..{high}. This usually means Horizons resolved the wrong target."
                )
    mercury = rows_by_name.get("mercury")
    ceres = rows_by_name.get("ceres")
    if mercury and ceres:
        for key in ("semi_major_axis_au", "orbital_period_days"):
            mercury_value = mercury.get(key)
            ceres_value = ceres.get(key)
            if mercury_value is not None and ceres_value is not None and abs(float(mercury_value) - float(ceres_value)) < 1e-9:
                raise RuntimeError(f"Sol authority Ceres duplicates Mercury {key}; Horizons command disambiguation failed")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch authoritative Sol-system S1 bootstrap rows from JPL Horizons."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--start-time", default="2016-01-01")
    parser.add_argument("--stop-time", default="2016-01-02")
    parser.add_argument("--timeout-s", type=int, default=120)
    parser.add_argument("--retries", type=int, default=4)
    args = parser.parse_args()
    validate_object_config()

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    raw_rel = "raw/sol_authority/sol_system_objects.csv"
    manifest_path = state_dir / "reports" / "manifests" / "sol_authority_manifest.json"

    log(
        "Sol authority fetch start "
        f"(objects={len(OBJECTS)}, start={args.start_time}, stop={args.stop_time}, timeout_s={args.timeout_s})"
    )

    retrieved_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows: list[dict[str, object]] = []
    captures: list[ResponseCapture] = []
    targets = [dict(obj) for obj in OBJECTS]
    operator_seed_sha256 = seed_sha256(OPERATOR_SEED_VERSION, targets)
    for obj in OBJECTS:
        params = build_query_params(
            command=str(obj["command"]),
            center=str(obj["center"]),
            start_time=str(args.start_time),
            stop_time=str(args.stop_time),
        )
        payload_bytes, query_url = api_get_payload(
            params, timeout_s=args.timeout_s, retries=args.retries
        )
        payload = payload_bytes.decode("utf-8", errors="replace")
        elements = parse_horizons_elements(payload)
        if "$$SOE" not in payload:
            raise RuntimeError(
                f"Horizons response for {obj['name']} ({obj['command']}) missing ephemeris block."
            )
        object_name_norm = str(obj["name"]).strip().lower()
        object_class = str(obj["object_class"]).strip().lower()
        object_kind = str(obj.get("object_kind") or object_class).strip().lower()
        capture = ResponseCapture(
            source_pk=str(obj["source_pk"]),
            object_name=str(obj["name"]),
            horizons_command=str(obj["command"]),
            center_code=str(obj["center"]),
            query_url=query_url,
            query_parameters=params,
            payload=payload_bytes,
        )
        captures.append(capture)
        parent_object_name = ""
        if object_class in {"planet", "dwarf_planet", "subplanet", "minor_body"}:
            parent_object_name = "Sun"
        elif object_class == "moon":
            parent_object_name = PARENT_BY_NAME.get(object_name_norm, "")
            if not parent_object_name:
                raise RuntimeError(f"Missing moon parent mapping for {obj['name']}")
        row: dict[str, object] = {
            "source_pk": int(obj["source_pk"]),
            "object_name": str(obj["name"]),
            "object_class": object_class,
            "object_kind": object_kind,
            "object_class_aliases_json": OBJECT_CLASS_ALIASES.get(object_class, "[]"),
            "parent_object_name": parent_object_name,
            "horizons_command": str(obj["command"]),
            "center_code": str(obj["center"]),
            "center_target_command": center_target_command(str(obj["center"])),
            "epoch_tdb_jd": elements.get("epoch_tdb_jd"),
            "calendar_date_tdb": elements.get("calendar_date_tdb"),
            "eccentricity": elements.get("eccentricity"),
            "periapsis_distance_au": elements.get("periapsis_distance_au"),
            "inclination_deg": elements.get("inclination_deg"),
            "longitude_ascending_node_deg": elements.get("longitude_ascending_node_deg"),
            "argument_periapsis_deg": elements.get("argument_periapsis_deg"),
            "time_periapsis_tdb_jd": elements.get("time_periapsis_tdb_jd"),
            "mean_motion_deg_day": elements.get("mean_motion_deg_day"),
            "mean_anomaly_deg": elements.get("mean_anomaly_deg"),
            "true_anomaly_deg": elements.get("true_anomaly_deg"),
            "semi_major_axis_au": elements.get("semi_major_axis_au"),
            "apoapsis_distance_au": elements.get("apoapsis_distance_au"),
            "orbital_period_days": elements.get("orbital_period_days"),
            "radius_km": parse_physical_radius_km(payload),
            "mass_kg": parse_mass_kg(payload),
            "horizons_query_url": query_url,
            "horizons_response_path": capture.response_path,
            "horizons_response_sha256": capture.response_sha256,
            "operator_seed_version": OPERATOR_SEED_VERSION,
            "operator_seed_sha256": operator_seed_sha256,
            "retrieved_at": retrieved_at,
        }
        row_hash_payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
        row["source_row_hash"] = hashlib.sha256(row_hash_payload.encode("utf-8")).hexdigest()
        rows.append(row)

    validate_rows(rows)

    fieldnames = [
        "source_pk",
        "object_name",
        "object_class",
        "object_kind",
        "object_class_aliases_json",
        "parent_object_name",
        "horizons_command",
        "center_code",
        "center_target_command",
        "epoch_tdb_jd",
        "calendar_date_tdb",
        "eccentricity",
        "periapsis_distance_au",
        "inclination_deg",
        "longitude_ascending_node_deg",
        "argument_periapsis_deg",
        "time_periapsis_tdb_jd",
        "mean_motion_deg_day",
        "mean_anomaly_deg",
        "true_anomaly_deg",
        "semi_major_axis_au",
        "apoapsis_distance_au",
        "orbital_period_days",
        "radius_km",
        "mass_kg",
        "horizons_query_url",
        "horizons_response_path",
        "horizons_response_sha256",
        "operator_seed_version",
        "operator_seed_sha256",
        "retrieved_at",
        "source_row_hash",
    ]
    source_version = f"horizons_s1_{args.start_time}"
    query_signature = {
        "start_time": args.start_time,
        "stop_time": args.stop_time,
        "ephem_type": "ELEMENTS",
        "center_policy": (
            "500@0 for Sun, heliocentric for planets/dwarf_planets/minor_bodies, "
            "host-centered for moons"
        ),
        "objects": ",".join(str(obj["command"]) for obj in OBJECTS),
    }
    snapshot_path, manifest_payload = write_horizons_snapshot(
        state_dir=state_dir,
        family="sol_authority",
        table_source_name=SOL_AUTHORITY_TABLE_SOURCE_NAME,
        response_source_name=SOL_AUTHORITY_RESPONSE_SOURCE_NAME,
        parsed_filename="sol_system_objects.csv",
        legacy_relative_path=raw_rel,
        manifest_filename=manifest_path.name,
        source_version=source_version,
        source_url=HORIZONS_API_URL,
        retrieved_at=retrieved_at,
        rows=rows,
        fieldnames=fieldnames,
        captures=captures,
        seed_version=OPERATOR_SEED_VERSION,
        targets=targets,
        collector_path=Path(__file__).resolve(),
        query_signature=query_signature,
    )
    log(
        f"Sol authority fetch complete (rows={len(rows):,}, "
        f"snapshot={snapshot_path.name}, artifacts={len(manifest_payload)}, "
        f"manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
