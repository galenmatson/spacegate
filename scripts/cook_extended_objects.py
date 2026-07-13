#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from astropy.coordinates import FK4, ICRS, SkyCoord
from astropy.time import Time
import astropy.units as u


OPENNGC_VERSION = "36cb178a0f69dba8bfc03a99c10512831edf1c6b"
TRANSFORM_VERSION = "extended_object_source_normalization_v1"


@dataclass
class SourceRecord:
    source_record_key: str
    source_catalog: str
    source_version: str
    source_pk: str
    primary_name: str
    object_type_raw: str
    ra_deg: float | None
    dec_deg: float | None
    source_frame: str
    source_epoch: str
    major_axis_arcmin: float | None = None
    minor_axis_arcmin: float | None = None
    position_angle_deg: float | None = None
    area_sq_deg: float | None = None
    parallax_mas_raw: float | None = None
    distance_pc_raw: float | None = None
    distance_method_raw: str = ""
    outcome_hint: str = "accepted_candidate"
    metadata: dict = field(default_factory=dict)


def text(value: object) -> str:
    return str(value or "").strip()


def number(value: object) -> float | None:
    raw = text(value).replace("−", "-")
    if not raw:
        return None
    try:
        parsed = float(raw.rstrip(":?vV"))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def integer(value: object) -> int | None:
    parsed = number(value)
    return int(parsed) if parsed is not None else None


def hms_deg(hours: object, minutes: object, seconds: object = 0) -> float | None:
    h, m, s = number(hours), number(minutes), number(seconds)
    if h is None or m is None:
        return None
    return 15.0 * (h + m / 60.0 + (s or 0.0) / 3600.0)


def dms_deg(sign: str, degrees: object, minutes: object, seconds: object = 0) -> float | None:
    d, m, s = number(degrees), number(minutes), number(seconds)
    if d is None or m is None:
        return None
    value = abs(d) + m / 60.0 + (s or 0.0) / 3600.0
    return -value if text(sign).startswith("-") else value


def sexagesimal_ra(value: str) -> float | None:
    parts = re.split(r"[:\s]+", text(value))
    return hms_deg(*(parts + ["0", "0"])[:3]) if len(parts) >= 2 else None


def sexagesimal_dec(value: str) -> float | None:
    raw = text(value)
    parts = re.split(r"[:\s]+", raw.lstrip("+-"))
    return dms_deg(raw[:1], *(parts + ["0", "0"])[:3]) if len(parts) >= 2 else None


def to_icrs(ra_deg: float | None, dec_deg: float | None, epoch: str) -> tuple[float | None, float | None]:
    if ra_deg is None or dec_deg is None:
        return None, None
    if epoch == "J2000":
        return ra_deg % 360.0, dec_deg
    coordinate = SkyCoord(
        ra=ra_deg * u.deg,
        dec=dec_deg * u.deg,
        frame=FK4(equinox=Time(epoch)),
    ).transform_to(ICRS())
    return float(coordinate.ra.deg), float(coordinate.dec.deg)


def normalize_designation(value: str) -> str:
    raw = " ".join(text(value).split())
    match = re.fullmatch(r"(?i)(NGC|IC|M|MEL(?:OTTE)?|LBN|LDN|VDB|SH\s*2|BARNARD|B)\s*0*([0-9]+)([A-Za-z]?)", raw)
    if not match:
        return raw
    prefix = match.group(1).upper().replace(" ", "")
    prefix = {"SH2": "Sh 2-", "VDB": "vdB ", "B": "Barnard ", "BARNARD": "Barnard ", "MEL": "Melotte ", "MELOTTE": "Melotte "}.get(prefix, prefix + " ")
    return f"{prefix}{int(match.group(2))}{match.group(3)}"


def row_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def source_lines(path: Path) -> list[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as stream:
        return stream.read().splitlines()


def append_alias(aliases: list[dict], record_key: str, alias: str, kind: str, priority: int, catalog: str) -> None:
    clean = normalize_designation(alias)
    if not clean:
        return
    aliases.append({
        "source_record_key": record_key,
        "alias_raw": clean,
        "alias_kind": kind,
        "alias_priority": priority,
        "source_catalog": catalog,
    })


def append_relation(relations: list[dict], record_key: str, kind: str, namespace: str, value: str, catalog: str, confidence: str = "high") -> None:
    clean = text(value)
    if not clean:
        return
    relations.append({
        "source_record_key": record_key,
        "relation_kind": kind,
        "target_namespace": namespace,
        "target_value": clean,
        "confidence_tier": confidence,
        "source_catalog": catalog,
    })


def openngc_rows(path: Path, aliases: list[dict], relations: list[dict]) -> Iterable[SourceRecord]:
    with path.open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream, delimiter=";"):
            name = text(row.get("Name"))
            if not name:
                continue
            key = f"openngc:{name.lower()}"
            type_raw = text(row.get("Type"))
            outcome = {
                "Dup": "redirect",
                "NonEx": "excluded_nonexistent",
                "*": "excluded_stellar_domain",
                "**": "excluded_stellar_domain",
                "Nova": "excluded_event_domain",
                "Other": "quarantine_unclassified",
            }.get(type_raw, "accepted_candidate")
            append_alias(aliases, key, name, "catalog_designation", 10, "openngc")
            for field_name, kind, priority in (("M", "messier_id", 3), ("NGC", "ngc_id", 4), ("IC", "ic_id", 5)):
                for token in re.split(r"[,|]", text(row.get(field_name))):
                    if token:
                        append_alias(aliases, key, f"{field_name} {token}", kind, priority, "openngc")
            for token in re.split(r"[,|]", text(row.get("Identifiers"))):
                append_alias(aliases, key, token, "catalog_identifier", 20, "openngc")
            for token in re.split(r"[,|]", text(row.get("Common names"))):
                append_alias(aliases, key, token, "common_name", 1, "openngc")
            for token in re.split(r"[,|]", text(row.get("Cstar Names"))):
                append_relation(relations, key, "central_star", "name", token, "openngc", "medium")
            yield SourceRecord(
                source_record_key=key,
                source_catalog="openngc",
                source_version=OPENNGC_VERSION,
                source_pk=name,
                primary_name=normalize_designation(name),
                object_type_raw=type_raw,
                ra_deg=sexagesimal_ra(text(row.get("RA"))),
                dec_deg=sexagesimal_dec(text(row.get("Dec"))),
                source_frame="ICRS",
                source_epoch="J2000",
                major_axis_arcmin=number(row.get("MajAx")),
                minor_axis_arcmin=number(row.get("MinAx")),
                position_angle_deg=number(row.get("PosAng")),
                parallax_mas_raw=number(row.get("Pax")),
                outcome_hint=outcome,
                metadata={"sources": text(row.get("Sources")), "constellation": text(row.get("Const"))},
            )


def lbn_rows(path: Path, aliases: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        seq = integer(raw[1:5])
        if seq is None:
            continue
        key = f"lbn:{seq}"
        name = f"LBN {seq}"
        append_alias(aliases, key, name, "lbn_id", 5, "lbn")
        other = text(raw[60:68])
        append_alias(aliases, key, other, "cross_identifier", 10, "lbn")
        ra, dec = to_icrs(hms_deg(raw[20:22], raw[23:25]), dms_deg(raw[27:28], raw[28:30], raw[31:33]), "B1950")
        yield SourceRecord(key, "lbn", "VII/9", str(seq), name, "bright_nebula", ra, dec, "FK4", "B1950", number(raw[35:39]), number(raw[40:43]), area_sq_deg=number(raw[44:51]), metadata={"color_class": text(raw[52:53]), "brightness_class": text(raw[54:55]), "complex_id": text(raw[56:59])})


def ldn_rows(path: Path, aliases: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        seq = integer(raw[50:54])
        if seq is None:
            continue
        ldn = integer(raw[0:4])
        catalog_id = str(ldn) if ldn is not None else f"seq-{seq}"
        key = f"ldn:{catalog_id.lower()}"
        name = f"LDN {ldn}" if ldn is not None else f"LDN sequence {seq}"
        append_alias(aliases, key, name, "ldn_id", 5, "ldn")
        for offset in range(60, min(len(raw), 92), 4):
            barnard = text(raw[offset:offset + 4])
            if barnard:
                append_alias(aliases, key, f"Barnard {barnard}", "barnard_id", 8, "ldn")
        ra, dec = to_icrs(hms_deg(raw[5:7], raw[8:12]), dms_deg(raw[15:16], raw[16:18], raw[19:21]), "B1950")
        yield SourceRecord(key, "ldn", "VII/7A", catalog_id, name, "dark_nebula", ra, dec, "FK4", "B1950", area_sq_deg=number(raw[36:43]), metadata={"opacity_class": text(raw[44:45]), "complex_id": text(raw[46:49]), "sequence": seq})


def barnard_rows(path: Path, aliases: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        catalog_id = text(raw[1:5])
        if not catalog_id:
            continue
        key = f"barnard:{catalog_id.lower()}"
        name = f"Barnard {catalog_id}"
        append_alias(aliases, key, name, "barnard_id", 5, "barnard")
        yield SourceRecord(key, "barnard", "VII/220A", catalog_id, name, "dark_nebula", hms_deg(raw[22:24], raw[25:27], raw[28:30]), dms_deg(raw[32:33], raw[33:35], raw[36:38]), "ICRS", "J2000", number(raw[39:44]), number(raw[39:44]))


def magakian_rows(path: Path, aliases: list[dict], relations: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        seq = integer(raw[0:3])
        if seq is None:
            continue
        key = f"magakian:{seq}"
        name = f"Magakian {seq}"
        append_alias(aliases, key, name, "magakian_id", 30, "magakian")
        fields = ((raw[35:39], "vdB ", "vdb_id"), (raw[50:62], "", "ngc_ic_id"), (raw[86:94], "Ced ", "cederblad_id"), (raw[98:121], "", "cross_identifier"))
        for value, prefix, kind in fields:
            for token in re.split(r"[,/]", text(value)):
                if token and token != "*":
                    append_alias(aliases, key, f"{prefix}{token}", kind, 10, "magakian")
        for match in re.finditer(r"([0-9]{4,6})(?:-([0-9]{1,6}))?", text(raw[136:154])):
            start = int(match.group(1))
            values = [start]
            if match.group(2):
                suffix = match.group(2)
                end = int(str(start)[:-len(suffix)] + suffix) if len(suffix) < len(str(start)) else int(suffix)
                values.extend(range(start + 1, end + 1))
            for hd in values:
                append_relation(relations, key, "illuminated_by", "hd", str(hd), "magakian")
        yield SourceRecord(key, "magakian", "J/A+A/399/141", str(seq), name, text(raw[177:183]) or "reflection_nebula", hms_deg(raw[4:6], raw[7:9], raw[10:12]), dms_deg(raw[13:14], raw[14:16], raw[17:19], raw[20:22]), "ICRS", "J2000", metadata={"comments": text(raw[184:263])})


def vdb_rows(path: Path, aliases: list[dict], relations: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        catalog_id = integer(raw[1:4])
        if catalog_id is None:
            continue
        key = f"vdb:{catalog_id}"
        name = f"vdB {catalog_id}"
        append_alias(aliases, key, name, "vdb_id", 5, "vdb")
        hd = integer(raw[16:22])
        if hd is not None:
            append_relation(relations, key, "illuminated_by", "hd", str(hd), "vdb")
        yield SourceRecord(key, "vdb", "VII/21", str(catalog_id), name, "reflection_nebula", None, None, "galactic_legacy", "source_native", number(raw[70:75]), number(raw[76:81]), metadata={"dm_id": text(raw[5:15]), "spectral_type": text(raw[35:44]), "nebula_type": text(raw[50:56]), "surface_brightness": text(raw[57:61]), "color_class": text(raw[61:65])})


def sharpless_rows(path: Path, aliases: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        catalog_id = integer(raw[0:4])
        if catalog_id is None:
            continue
        key = f"sh2:{catalog_id}"
        name = f"Sh 2-{catalog_id}"
        append_alias(aliases, key, name, "sharpless_id", 5, "sharpless")
        ra, dec = to_icrs(hms_deg(raw[34:36], raw[36:38], number(raw[38:41]) / 10.0 if number(raw[38:41]) is not None else None), dms_deg(raw[41:42], raw[42:44], raw[44:46], raw[46:48]), "B1950")
        yield SourceRecord(key, "sharpless", "VII/20", str(catalog_id), name, "hii_region", ra, dec, "FK4", "B1950", number(raw[48:52]), number(raw[48:52]), metadata={"form_class": text(raw[52:53]), "structure_class": text(raw[53:54]), "brightness_class": text(raw[54:55]), "associated_star_count": integer(raw[55:57])})


def cederblad_rows(path: Path, aliases: list[dict], relations: list[dict]) -> Iterable[SourceRecord]:
    for raw in source_lines(path):
        base_id = integer(raw[0:3])
        if base_id is None:
            continue
        catalog_id = f"{base_id}{text(raw[3:4])}"
        key = f"cederblad:{catalog_id.lower()}"
        name = f"Ced {catalog_id}"
        append_alias(aliases, key, name, "cederblad_id", 8, "cederblad")
        append_alias(aliases, key, text(raw[5:16]), "cross_identifier", 10, "cederblad")
        star_name = text(raw[44:56])
        append_relation(relations, key, "associated_with", "name", star_name, "cederblad", "historical")
        ra, dec = to_icrs(hms_deg(raw[16:18], raw[19:23]), dms_deg(raw[24:25], raw[25:27], raw[28:30]), "B1900")
        yield SourceRecord(key, "cederblad", "VII/231", catalog_id, name, "bright_diffuse_nebula", ra, dec, "FK4", "B1900", number(raw[95:100]), number(raw[101:106]), parallax_mas_raw=(number(raw[108:114]) * 1000.0 if number(raw[108:114]) is not None else None), distance_pc_raw=number(raw[124:128]), distance_method_raw=text(raw[116:124]), metadata={"associated_star": star_name, "nebula_spectrum": text(raw[86:89]), "nebula_class": text(raw[90:93]), "distance_quality": "historical_low"})


def write_outputs(records: list[SourceRecord], aliases: list[dict], relations: list[dict], cooked_dir: Path) -> None:
    cooked_dir.mkdir(parents=True, exist_ok=True)
    record_fields = [
        "source_record_key", "source_catalog", "source_version", "source_pk", "primary_name", "object_type_raw",
        "ra_deg", "dec_deg", "source_frame", "source_epoch", "major_axis_arcmin", "minor_axis_arcmin",
        "position_angle_deg", "area_sq_deg", "parallax_mas_raw", "distance_pc_raw", "distance_method_raw",
        "outcome_hint", "metadata_json", "source_row_hash", "transform_version",
    ]
    with (cooked_dir / "source_records.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=record_fields)
        writer.writeheader()
        for record in sorted(records, key=lambda item: item.source_record_key):
            payload = {key: getattr(record, key) for key in record_fields if hasattr(record, key)}
            payload["metadata_json"] = json.dumps(record.metadata, sort_keys=True, separators=(",", ":"))
            payload["source_row_hash"] = row_hash(payload)
            payload["transform_version"] = TRANSFORM_VERSION
            writer.writerow(payload)
    for filename, rows, fields in (
        ("source_aliases.csv", aliases, ["source_record_key", "alias_raw", "alias_kind", "alias_priority", "source_catalog"]),
        ("source_relations.csv", relations, ["source_record_key", "relation_kind", "target_namespace", "target_value", "confidence_tier", "source_catalog"]),
    ):
        deduped = {tuple(row.get(field) for field in fields): row for row in rows}
        with (cooked_dir / filename).open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(deduped[key] for key in sorted(deduped))


def _record_hashes(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8", newline="") as stream:
        return {
            row["source_record_key"]: row["source_row_hash"]
            for row in csv.DictReader(stream)
        }


def cook_bundle(raw_dir: Path, cooked_dir: Path, delta_report_path: Path | None = None) -> dict[str, int]:
    previous_hashes = _record_hashes(cooked_dir / "source_records.csv")
    aliases: list[dict] = []
    relations: list[dict] = []
    records: list[SourceRecord] = []
    inputs = [
        (raw_dir / "openngc" / "NGC.csv", openngc_rows, (aliases, relations)),
        (raw_dir / "openngc" / "addendum.csv", openngc_rows, (aliases, relations)),
        (raw_dir / "vizier" / "VII_9_catalog.dat", lbn_rows, (aliases,)),
        (raw_dir / "vizier" / "VII_7A_ldn", ldn_rows, (aliases,)),
        (raw_dir / "vizier" / "VII_220A_barnard.dat", barnard_rows, (aliases,)),
        (raw_dir / "vizier" / "J_A+A_399_141_table1.dat", magakian_rows, (aliases, relations)),
        (raw_dir / "vizier" / "VII_21_catalog.dat", vdb_rows, (aliases, relations)),
        (raw_dir / "vizier" / "VII_20_catalog.dat.gz", sharpless_rows, (aliases,)),
        (raw_dir / "vizier" / "VII_231_catalog.dat", cederblad_rows, (aliases, relations)),
    ]
    missing = [str(path) for path, _, _ in inputs if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing extended-object raw inputs: " + ", ".join(missing))
    for path, handler, extra in inputs:
        records.extend(handler(path, *extra))
    write_outputs(records, aliases, relations, cooked_dir)
    current_hashes = _record_hashes(cooked_dir / "source_records.csv")
    if delta_report_path:
        previous_keys = set(previous_hashes)
        current_keys = set(current_hashes)
        report = {
            "schema_version": "extended_object_snapshot_delta_v1",
            "previous_record_count": len(previous_hashes),
            "current_record_count": len(current_hashes),
            "added_count": len(current_keys - previous_keys),
            "removed_count": len(previous_keys - current_keys),
            "changed_count": sum(
                previous_hashes[key] != current_hashes[key]
                for key in previous_keys & current_keys
            ),
            "added_source_record_keys": sorted(current_keys - previous_keys),
            "removed_source_record_keys": sorted(previous_keys - current_keys),
        }
        delta_report_path.parent.mkdir(parents=True, exist_ok=True)
        delta_report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"source_records": len(records), "source_aliases": len(aliases), "source_relations": len(relations)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize Spacegate extended-object catalog sources.")
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args()
    state_dir = Path(args.state_dir)
    counts = cook_bundle(
        state_dir / "raw" / "extended_objects",
        state_dir / "cooked" / "extended_objects",
        state_dir / "reports" / "extended_object_snapshot_delta_report.json",
    )
    print(json.dumps(counts, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
