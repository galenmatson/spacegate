#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

from horizons_snapshot import (
    ResponseCapture,
    SOL_ARTIFICIAL_RESPONSE_SOURCE_NAME,
    SOL_ARTIFICIAL_TABLE_SOURCE_NAME,
    center_target_command,
    parse_horizons_elements,
    seed_sha256,
    write_horizons_snapshot,
)

HORIZONS_API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"
ORBITAL_PERIOD_SENTINEL_DAYS = 1e20
OPERATOR_SEED_VERSION = "sol_artificial_bootstrap_v1"

ARTIFICIAL_OBJECTS = [
    {
        "source_pk": 4001,
        "name": "International Space Station",
        "object_kind": "station",
        "command": "-125544",
        "center": "500@399",
        "parent_object_name": "Earth",
        "freshness_window_days": 14,
    },
    {
        "source_pk": 4002,
        "name": "Hubble Space Telescope",
        "object_kind": "space_telescope",
        "command": "-48",
        "center": "500@399",
        "parent_object_name": "Earth",
        "freshness_window_days": 30,
    },
    {
        "source_pk": 4003,
        "name": "TESS",
        "object_kind": "space_telescope",
        "command": "-95",
        "center": "500@399",
        "parent_object_name": "Earth",
        "freshness_window_days": 30,
    },
    {
        "source_pk": 4101,
        "name": "James Webb Space Telescope",
        "object_kind": "space_telescope",
        "command": "-170",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 30,
    },
    {
        "source_pk": 4201,
        "name": "Parker Solar Probe",
        "object_kind": "deep_space_probe",
        "command": "-96",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 45,
    },
    {
        "source_pk": 4202,
        "name": "New Horizons",
        "object_kind": "deep_space_probe",
        "command": "-98",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 90,
    },
    {
        "source_pk": 4203,
        "name": "Voyager 1",
        "object_kind": "deep_space_probe",
        "command": "-31",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 90,
    },
    {
        "source_pk": 4204,
        "name": "Voyager 2",
        "object_kind": "deep_space_probe",
        "command": "-32",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 90,
    },
    {
        "source_pk": 4301,
        "name": "Mars Reconnaissance Orbiter",
        "object_kind": "planetary_orbiter",
        "command": "-74",
        "center": "500@499",
        "parent_object_name": "Mars",
        "freshness_window_days": 45,
    },
    {
        "source_pk": 4302,
        "name": "BepiColombo",
        "object_kind": "planetary_orbiter",
        "command": "-121",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 45,
    },
    {
        "source_pk": 4303,
        "name": "Hera",
        "object_kind": "planetary_orbiter",
        "command": "-91",
        "center": "500@10",
        "parent_object_name": "Sun",
        "freshness_window_days": 45,
    },
]


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
    ]
    for pattern in patterns:
        m = re.search(pattern, payload, flags=re.IGNORECASE)
        if m:
            return parse_float_token(m.group(1))
    return None


def parse_mass_kg(payload: str) -> float | None:
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
    return None


def parse_target_name(payload: str) -> str | None:
    m = re.search(r"Target body name:\s*([^\n]+)", payload, flags=re.IGNORECASE)
    if not m:
        return None
    return str(m.group(1)).strip() or None


def parse_elements(payload: str) -> dict[str, float | str | None]:
    elements = parse_horizons_elements(payload)
    eccentricity = elements.get("eccentricity")
    semi_major_axis_au = elements.get("semi_major_axis_au")
    orbital_period_days = elements.get("orbital_period_days")

    if eccentricity is not None and not math.isfinite(eccentricity):
        eccentricity = None
    if semi_major_axis_au is not None and not math.isfinite(semi_major_axis_au):
        semi_major_axis_au = None
    if orbital_period_days is not None:
        if (
            not math.isfinite(orbital_period_days)
            or abs(orbital_period_days) >= ORBITAL_PERIOD_SENTINEL_DAYS
            or orbital_period_days <= 0.0
        ):
            orbital_period_days = None

    # Horizons can emit sentinel/degenerate values for escape trajectories.
    if (
        orbital_period_days is not None
        and ((eccentricity is not None and eccentricity >= 1.0) or (semi_major_axis_au is not None and semi_major_axis_au <= 0.0))
    ):
        orbital_period_days = None

    elements["eccentricity"] = eccentricity
    elements["semi_major_axis_au"] = semi_major_axis_au
    elements["orbital_period_days"] = orbital_period_days
    return elements


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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Sol artificial-object orbital rows from JPL Horizons."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--start-time", default="2026-01-01")
    parser.add_argument("--stop-time", default="2026-01-02")
    parser.add_argument("--timeout-s", type=int, default=120)
    parser.add_argument("--retries", type=int, default=4)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        args.state_dir
        or os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or (root / "data")
    )
    raw_rel = "raw/sol_artificial/sol_artificial_objects.csv"
    manifest_path = state_dir / "reports" / "manifests" / "sol_artificial_manifest.json"

    log(
        "Sol artificial fetch start "
        f"(objects={len(ARTIFICIAL_OBJECTS)}, start={args.start_time}, stop={args.stop_time}, timeout_s={args.timeout_s})"
    )

    retrieved_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows: list[dict[str, object]] = []
    captures: list[ResponseCapture] = []
    targets = [dict(obj) for obj in ARTIFICIAL_OBJECTS]
    operator_seed_sha256 = seed_sha256(OPERATOR_SEED_VERSION, targets)
    for obj in ARTIFICIAL_OBJECTS:
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
        if "$$SOE" not in payload:
            raise RuntimeError(
                f"Horizons response for {obj['name']} ({obj['command']}) missing ephemeris block."
            )
        elements = parse_elements(payload)
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
        row: dict[str, object] = {
            "source_pk": int(obj["source_pk"]),
            "object_name": str(obj["name"]),
            "object_class": "artificial",
            "object_kind": str(obj["object_kind"]).strip().lower(),
            "parent_object_name": str(obj["parent_object_name"]),
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
            "freshness_window_days": int(obj["freshness_window_days"]),
            "target_body_name": parse_target_name(payload),
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

    fieldnames = [
        "source_pk",
        "object_name",
        "object_class",
        "object_kind",
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
        "freshness_window_days",
        "target_body_name",
        "horizons_query_url",
        "horizons_response_path",
        "horizons_response_sha256",
        "operator_seed_version",
        "operator_seed_sha256",
        "retrieved_at",
        "source_row_hash",
    ]
    source_version = f"horizons_s4_{args.start_time}"
    query_signature = {
        "start_time": args.start_time,
        "stop_time": args.stop_time,
        "ephem_type": "ELEMENTS",
        "objects": ",".join(str(obj["command"]) for obj in ARTIFICIAL_OBJECTS),
    }
    snapshot_path, manifest_payload = write_horizons_snapshot(
        state_dir=state_dir,
        family="sol_artificial",
        table_source_name=SOL_ARTIFICIAL_TABLE_SOURCE_NAME,
        response_source_name=SOL_ARTIFICIAL_RESPONSE_SOURCE_NAME,
        parsed_filename="sol_artificial_objects.csv",
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
        f"Sol artificial fetch complete (rows={len(rows):,}, "
        f"snapshot={snapshot_path.name}, artifacts={len(manifest_payload)}, "
        f"manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
