#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

HORIZONS_API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
USER_AGENT = "Spacegate/0.1 (+https://github.com/galenmatson/spacegate)"

# S1 bootstrap scope:
# - Sun
# - 8 major planets
# - 5 commonly accepted dwarf planets (Pluto, Ceres, Eris, Haumea, Makemake)
OBJECTS = [
    {"source_pk": 1, "name": "Sun", "object_class": "star", "command": "10", "center": "500@0"},
    {"source_pk": 2, "name": "Mercury", "object_class": "planet", "command": "199", "center": "500@10"},
    {"source_pk": 3, "name": "Venus", "object_class": "planet", "command": "299", "center": "500@10"},
    {"source_pk": 4, "name": "Earth", "object_class": "planet", "command": "399", "center": "500@10"},
    {"source_pk": 5, "name": "Mars", "object_class": "planet", "command": "499", "center": "500@10"},
    {"source_pk": 6, "name": "Jupiter", "object_class": "planet", "command": "599", "center": "500@10"},
    {"source_pk": 7, "name": "Saturn", "object_class": "planet", "command": "699", "center": "500@10"},
    {"source_pk": 8, "name": "Uranus", "object_class": "planet", "command": "799", "center": "500@10"},
    {"source_pk": 9, "name": "Neptune", "object_class": "planet", "command": "899", "center": "500@10"},
    {"source_pk": 10, "name": "Pluto", "object_class": "dwarf_planet", "command": "999", "center": "500@10"},
    {"source_pk": 11, "name": "Ceres", "object_class": "dwarf_planet", "command": "1", "center": "500@10"},
    {"source_pk": 12, "name": "Eris", "object_class": "dwarf_planet", "command": "136199", "center": "500@10"},
    {"source_pk": 13, "name": "Haumea", "object_class": "dwarf_planet", "command": "136108", "center": "500@10"},
    {"source_pk": 14, "name": "Makemake", "object_class": "dwarf_planet", "command": "136472", "center": "500@10"},
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


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def api_get_text(params: dict[str, str], *, timeout_s: int, retries: int) -> tuple[str, str]:
    url = f"{HORIZONS_API_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                payload = response.read().decode("utf-8", errors="replace")
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


def parse_elements(payload: str) -> dict[str, float | str | None]:
    lines = payload.splitlines()
    in_block = False
    first_csv_line: str | None = None
    for line in lines:
        if line.strip() == "$$SOE":
            in_block = True
            continue
        if line.strip() == "$$EOE":
            break
        if in_block and line.strip():
            first_csv_line = line.strip()
            break

    if not first_csv_line:
        return {
            "epoch_tdb_jd": None,
            "eccentricity": None,
            "inclination_deg": None,
            "semi_major_axis_au": None,
            "orbital_period_days": None,
        }

    parts = [part.strip() for part in first_csv_line.split(",")]
    if len(parts) < 14:
        return {
            "epoch_tdb_jd": None,
            "eccentricity": None,
            "inclination_deg": None,
            "semi_major_axis_au": None,
            "orbital_period_days": None,
        }
    return {
        "epoch_tdb_jd": parse_float_token(parts[0]),
        "eccentricity": parse_float_token(parts[2]),
        "inclination_deg": parse_float_token(parts[4]),
        "semi_major_axis_au": parse_float_token(parts[11]),
        "orbital_period_days": parse_float_token(parts[13]),
    }


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
        description="Fetch authoritative Sol-system S1 bootstrap rows from JPL Horizons."
    )
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--start-time", default="2016-01-01")
    parser.add_argument("--stop-time", default="2016-01-02")
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
    raw_rel = "raw/sol_authority/sol_system_objects.csv"
    out_path = state_dir / raw_rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = state_dir / "reports" / "manifests" / "sol_authority_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    log(
        "Sol authority fetch start "
        f"(objects={len(OBJECTS)}, start={args.start_time}, stop={args.stop_time}, timeout_s={args.timeout_s})"
    )

    retrieved_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows: list[dict[str, object]] = []
    for obj in OBJECTS:
        params = build_query_params(
            command=str(obj["command"]),
            center=str(obj["center"]),
            start_time=str(args.start_time),
            stop_time=str(args.stop_time),
        )
        payload, query_url = api_get_text(params, timeout_s=args.timeout_s, retries=args.retries)
        elements = parse_elements(payload)
        row: dict[str, object] = {
            "source_pk": int(obj["source_pk"]),
            "object_name": str(obj["name"]),
            "object_class": str(obj["object_class"]),
            "parent_object_name": ("" if str(obj["object_class"]) == "star" else "Sun"),
            "horizons_command": str(obj["command"]),
            "epoch_tdb_jd": elements.get("epoch_tdb_jd"),
            "eccentricity": elements.get("eccentricity"),
            "inclination_deg": elements.get("inclination_deg"),
            "semi_major_axis_au": elements.get("semi_major_axis_au"),
            "orbital_period_days": elements.get("orbital_period_days"),
            "radius_km": parse_physical_radius_km(payload),
            "mass_kg": parse_mass_kg(payload),
            "horizons_query_url": query_url,
            "retrieved_at": retrieved_at,
        }
        row_hash_payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
        row["source_row_hash"] = hashlib.sha256(row_hash_payload.encode("utf-8")).hexdigest()
        rows.append(row)

    fieldnames = [
        "source_pk",
        "object_name",
        "object_class",
        "parent_object_name",
        "horizons_command",
        "epoch_tdb_jd",
        "eccentricity",
        "inclination_deg",
        "semi_major_axis_au",
        "orbital_period_days",
        "radius_km",
        "mass_kg",
        "horizons_query_url",
        "retrieved_at",
        "source_row_hash",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    manifest_payload = [
        {
            "source_name": "sol_system_objects",
            "source_version": f"horizons_s1_{args.start_time}",
            "url": HORIZONS_API_URL,
            "dest_path": raw_rel,
            "retrieved_at": retrieved_at,
            "checked_at": retrieved_at,
            "bytes_written": out_path.stat().st_size,
            "row_count": len(rows),
            "sha256": sha256_file(out_path),
            "query_signature": {
                "start_time": args.start_time,
                "stop_time": args.stop_time,
                "ephem_type": "ELEMENTS",
                "center_policy": "500@10 for planets/dwarf_planets, 500@0 for Sun",
                "objects": ",".join(str(obj["command"]) for obj in OBJECTS),
            },
        }
    ]
    manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n", encoding="utf-8")
    log(
        f"Sol authority fetch complete (rows={len(rows):,}, bytes={out_path.stat().st_size:,}, manifest={manifest_path})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
