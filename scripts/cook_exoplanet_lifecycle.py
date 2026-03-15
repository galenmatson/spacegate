#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import os
import re
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path


def normalize_name(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"[^0-9A-Za-z]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def now_utc() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def row_hash(payload: dict[str, object]) -> str:
    parts: list[str] = []
    for key in sorted(payload.keys()):
        value = payload[key]
        parts.append(f"{key}={'' if value is None else value}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def map_status(raw: str | None) -> str | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if any(token in text for token in ("retracted", "refuted", "false positive", "withdrawn")):
        return "retracted"
    if any(token in text for token in ("controversial", "disputed")):
        return "controversial"
    if any(token in text for token in ("candidate", "unconfirmed", "toi", "koi")):
        return "candidate"
    if any(token in text for token in ("confirmed", "secure", "validated")):
        return "confirmed"
    return None


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


def open_csv_reader(path: Path) -> csv.DictReader:
    raw = path.read_text(encoding="utf-8", errors="replace")
    sample = raw[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.get_dialect("excel")
    return csv.DictReader(io.StringIO(raw), dialect=dialect)


def first_value(row: dict[str, str], keys: list[str]) -> str:
    lowered = {str(k).strip().lower(): ("" if v is None else str(v)) for k, v in row.items()}
    for key in keys:
        value = lowered.get(key, "")
        if value.strip():
            return value.strip()
    return ""


def cook_exoplanet_eu(
    raw_path: Path,
    status_writer: csv.DictWriter,
    observed_at: str,
) -> int:
    if not raw_path.exists():
        return 0
    count = 0
    reader = open_csv_reader(raw_path)
    for i, row in enumerate(reader, start=1):
        planet_name = first_value(row, ["name", "planet_name", "pl_name"])
        if not planet_name:
            continue
        observed_status = map_status(first_value(row, ["planet_status", "status", "category"]))
        if not observed_status:
            continue
        source_pk = first_value(row, ["id", "rowid", "oid"])
        payload = {
            "source_catalog": "exoplanet_eu",
            "source_version": "catalog_csv",
            "source_pk": source_pk or str(i),
            "planet_name": planet_name,
            "planet_name_norm": normalize_name(planet_name),
            "observed_status": observed_status,
            "observed_at": observed_at,
            "notes": "exoplanet.eu status layer",
        }
        payload["source_row_hash"] = row_hash(payload)
        status_writer.writerow(payload)
        count += 1
    return count


def cook_hwc(
    raw_path: Path,
    status_writer: csv.DictWriter,
    features_writer: csv.DictWriter,
    observed_at: str,
) -> tuple[int, int]:
    if not raw_path.exists():
        return (0, 0)
    status_count = 0
    feature_count = 0
    reader = open_csv_reader(raw_path)
    for i, row in enumerate(reader, start=1):
        planet_name = first_value(row, ["p_name", "name", "planet_name"])
        if not planet_name:
            continue
        source_pk = first_value(row, ["p_name", "name"]) or str(i)
        base = {
            "source_catalog": "hwc",
            "source_version": "hwc_csv",
            "source_pk": source_pk,
            "planet_name": planet_name,
            "planet_name_norm": normalize_name(planet_name),
            "observed_status": "confirmed",
            "observed_at": observed_at,
            "notes": "HWC confirmed inventory mirror",
        }
        base["source_row_hash"] = row_hash(base)
        status_writer.writerow(base)
        status_count += 1

        hwc_p_habitable = parse_float(first_value(row, ["p_habitable"]))
        hwc_esi = parse_float(first_value(row, ["p_esi", "esi"]))
        features = {
            "source_catalog": "hwc",
            "source_version": "hwc_csv",
            "source_pk": source_pk,
            "planet_name": planet_name,
            "planet_name_norm": normalize_name(planet_name),
            "hwc_p_habitable": hwc_p_habitable,
            "hwc_esi": hwc_esi,
        }
        features["source_row_hash"] = row_hash(features)
        features_writer.writerow(features)
        feature_count += 1
    return (status_count, feature_count)


def cook_oec_aliases(raw_path: Path, alias_writer: csv.DictWriter) -> int:
    if not raw_path.exists():
        return 0
    count = 0
    with tarfile.open(raw_path, "r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile() or not member.name.endswith(".xml"):
                continue
            if "/systems/" not in member.name and "/systems_kepler/" not in member.name:
                continue
            handle = archive.extractfile(member)
            if handle is None:
                continue
            try:
                root = ET.fromstring(handle.read())
            except ET.ParseError:
                continue
            for idx, planet in enumerate(root.findall(".//planet"), start=1):
                names = [str(x.text or "").strip() for x in planet.findall("name")]
                names = [name for name in names if name]
                if len(names) < 2:
                    continue
                primary = names[0]
                primary_norm = normalize_name(primary)
                for alias in names[1:]:
                    alias_norm = normalize_name(alias)
                    if not alias_norm or alias_norm == primary_norm:
                        continue
                    payload = {
                        "source_catalog": "open_exoplanet_catalogue",
                        "source_version": "tarball_master",
                        "source_pk": f"{member.name}:{idx}",
                        "planet_name": primary,
                        "alias_name": alias,
                        "alias_name_norm": alias_norm,
                        "alias_kind": "alt_name",
                    }
                    payload["source_row_hash"] = row_hash(payload)
                    alias_writer.writerow(payload)
                    count += 1
    return count


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    state_dir = Path(
        os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or root / "data"
    )
    raw_dir = state_dir / "raw"
    cooked_dir = state_dir / "cooked" / "exoplanet_lifecycle"
    cooked_dir.mkdir(parents=True, exist_ok=True)

    status_path = cooked_dir / "status_rows.csv"
    alias_path = cooked_dir / "alias_rows.csv"
    features_path = cooked_dir / "features_rows.csv"
    observed_at = now_utc()

    with (
        status_path.open("w", newline="", encoding="utf-8") as status_f,
        alias_path.open("w", newline="", encoding="utf-8") as alias_f,
        features_path.open("w", newline="", encoding="utf-8") as features_f,
    ):
        status_writer = csv.DictWriter(
            status_f,
            fieldnames=[
                "source_catalog",
                "source_version",
                "source_pk",
                "planet_name",
                "planet_name_norm",
                "observed_status",
                "source_row_hash",
                "observed_at",
                "notes",
            ],
        )
        status_writer.writeheader()

        alias_writer = csv.DictWriter(
            alias_f,
            fieldnames=[
                "source_catalog",
                "source_version",
                "source_pk",
                "planet_name",
                "alias_name",
                "alias_name_norm",
                "alias_kind",
                "source_row_hash",
            ],
        )
        alias_writer.writeheader()

        features_writer = csv.DictWriter(
            features_f,
            fieldnames=[
                "source_catalog",
                "source_version",
                "source_pk",
                "planet_name",
                "planet_name_norm",
                "hwc_p_habitable",
                "hwc_esi",
                "source_row_hash",
            ],
        )
        features_writer.writeheader()

        exoplanet_eu_status = cook_exoplanet_eu(
            raw_dir / "exoplanet_eu" / "catalog.csv",
            status_writer,
            observed_at,
        )
        hwc_status, hwc_features = cook_hwc(
            raw_dir / "hwc" / "hwc.csv",
            status_writer,
            features_writer,
            observed_at,
        )
        oec_aliases = cook_oec_aliases(
            raw_dir / "open_exoplanet_catalogue" / "open_exoplanet_catalogue.tar.gz",
            alias_writer,
        )

    print(f"cooked exoplanet_lifecycle_status: {exoplanet_eu_status + hwc_status} rows -> {status_path}")
    print(f"cooked exoplanet_lifecycle_aliases: {oec_aliases} rows -> {alias_path}")
    print(f"cooked exoplanet_lifecycle_features: {hwc_features} rows -> {features_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
