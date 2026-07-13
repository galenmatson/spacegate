from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb


LY_PER_PC = 3.26156
CORE_TRANSFORM_VERSION = "extended_object_identity_v1"
DISTANCE_POLICY_VERSION = "extended_object_distance_v1"
TYPE_POLICY_VERSION = "extended_object_type_v1"

SOURCE_PRIORITY = {
    "openngc": 0,
    "cantat_gaudin_2020": 1,
    "green_snr": 2,
    "lbn": 3,
    "ldn": 4,
    "barnard": 5,
    "magakian": 6,
    "vdb": 7,
    "sharpless": 8,
    "cederblad": 9,
}

MANIFEST_SOURCES = {
    "openngc": ("openngc_ngc", "openngc_addendum"),
    "lbn": ("lbn_vii_9",),
    "ldn": ("ldn_vii_7a",),
    "barnard": ("barnard_vii_220a",),
    "magakian": ("magakian_2003",),
    "vdb": ("vdb_vii_21",),
    "sharpless": ("sharpless_vii_20",),
    "cederblad": ("cederblad_vii_231",),
}

OPENNGC_TYPES = {
    "G": ("physical_object", "galaxy", "galaxy"),
    "GPair": ("composite", "galaxy", "galaxy_pair"),
    "GTrpl": ("composite", "galaxy", "galaxy_triplet"),
    "GGroup": ("composite", "galaxy", "galaxy_group"),
    "OCl": ("physical_object", "star_cluster", "open_cluster"),
    "GCl": ("physical_object", "star_cluster", "globular_cluster"),
    "Cl+N": ("composite", "nebula", "cluster_with_nebulosity"),
    "*Ass": ("association", "star_cluster", "stellar_association"),
    "PN": ("physical_object", "nebula", "planetary_nebula"),
    "HII": ("catalog_region", "nebula", "hii_region"),
    "DrkN": ("catalog_region", "nebula", "dark_nebula"),
    "EmN": ("catalog_region", "nebula", "emission_nebula"),
    "RfN": ("catalog_region", "nebula", "reflection_nebula"),
    "Neb": ("catalog_region", "nebula", "nebula"),
    "SNR": ("physical_object", "nebula", "supernova_remnant"),
}

SOURCE_TYPES = {
    "bright_nebula": ("catalog_region", "nebula", "bright_nebula"),
    "bright_diffuse_nebula": ("catalog_region", "nebula", "bright_diffuse_nebula"),
    "dark_nebula": ("catalog_region", "nebula", "dark_nebula"),
    "reflection_nebula": ("catalog_region", "nebula", "reflection_nebula"),
    "hii_region": ("catalog_region", "nebula", "hii_region"),
    "open_cluster": ("physical_object", "star_cluster", "open_cluster"),
    "supernova_remnant": ("physical_object", "nebula", "supernova_remnant"),
}


class UnionFind:
    def __init__(self, keys: list[str]):
        self.parent = {key: key for key in keys}

    def find(self, key: str) -> str:
        parent = self.parent[key]
        if parent != key:
            self.parent[key] = self.find(parent)
        return self.parent[key]

    def union(self, left: str, right: str) -> None:
        a, b = self.find(left), self.find(right)
        if a == b:
            return
        if a < b:
            self.parent[b] = a
        else:
            self.parent[a] = b


def normalize_search(value: str) -> str:
    return " ".join(re.sub(r"[^0-9a-z]+", " ", str(value or "").lower()).split())


def identity_token(value: str) -> tuple[str, str] | None:
    normalized = normalize_search(value)
    patterns = (
        (r"^m(?:essier)? ([0-9]+)$", "messier"),
        (r"^ngc ([0-9]+)([a-z]?)$", "ngc"),
        (r"^ic ([0-9]+)([a-z]?)$", "ic"),
        (r"^lbn ([0-9]+)$", "lbn"),
        (r"^ldn ([0-9]+)$", "ldn"),
        (r"^(?:barnard|b) ([0-9]+[a-z]?)$", "barnard"),
        (r"^vdb ([0-9]+)$", "vdb"),
        (r"^sh 2 ([0-9]+)$", "sh2"),
        (r"^ced(?:erblad)? ([0-9]+[a-z]?)$", "cederblad"),
        (r"^(?:mel|melotte) ([0-9]+)$", "melotte"),
        (r"^collinder ([0-9]+)$", "collinder"),
        (r"^trumpler ([0-9]+)$", "trumpler"),
    )
    for pattern, namespace in patterns:
        match = re.fullmatch(pattern, normalized)
        if match:
            return namespace, "".join(part or "" for part in match.groups()).lower()
    return None


def canonical_name_rank(alias: dict) -> tuple[int, int, str, str]:
    token = identity_token(alias["alias_raw"])
    namespace_rank = {
        "messier": 0,
        "ngc": 1,
        "ic": 2,
        "lbn": 3,
        "ldn": 4,
        "barnard": 5,
        "vdb": 6,
        "sh2": 7,
        "cederblad": 8,
        "melotte": 9,
        "collinder": 10,
        "trumpler": 11,
    }.get(token[0], 50) if token else 50
    return namespace_rank, int(alias.get("alias_priority") or 99), normalize_search(alias["alias_raw"]), str(alias["alias_raw"])


def stable_id(stable_key: str) -> int:
    return int(hashlib.sha256(stable_key.encode()).hexdigest()[:15], 16)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def source_url(catalog: str) -> str:
    return {
        "openngc": "https://github.com/mattiaverga/OpenNGC",
        "lbn": "https://cdsarc.cds.unistra.fr/ftp/VII/9/",
        "ldn": "https://cdsarc.cds.unistra.fr/ftp/VII/7A/",
        "barnard": "https://cdsarc.cds.unistra.fr/ftp/VII/220A/",
        "magakian": "https://cdsarc.cds.unistra.fr/ftp/J/A+A/399/141/",
        "vdb": "https://cdsarc.cds.unistra.fr/ftp/VII/21/",
        "sharpless": "https://cdsarc.cds.unistra.fr/ftp/VII/20/",
        "cederblad": "https://cdsarc.cds.unistra.fr/ftp/VII/231/",
        "cantat_gaudin_2020": "https://cdsarc.cds.unistra.fr/ftp/J/A+A/640/A1/",
        "green_snr": "https://www.mrao.cam.ac.uk/surveys/snrs/",
    }.get(catalog, "")


def license_fields(catalog: str) -> tuple[str, bool, str]:
    if catalog == "openngc":
        return "CC-BY-SA-4.0", True, "OpenNGC source boundary remains CC-BY-SA-4.0."
    return "catalog_citation_required", True, "Public catalog facts redistributed with source citation; retain source-specific license review metadata."


def load_source_provenance(state_dir: Path) -> dict[str, dict[str, str | None]]:
    manifest_path = state_dir / "reports" / "manifests" / "extended_objects_manifest.json"
    entries = json.loads(manifest_path.read_text(encoding="utf-8"))
    by_name = {str(entry["source_name"]): entry for entry in entries}
    provenance: dict[str, dict[str, str | None]] = {}
    for catalog, source_names in MANIFEST_SOURCES.items():
        selected = [by_name[name] for name in source_names]
        digest = hashlib.sha256(
            "|".join(str(entry["sha256"]) for entry in selected).encode()
        ).hexdigest()
        provenance[catalog] = {
            "source_download_url": str(selected[0]["url"]),
            "retrieval_checksum": digest,
            "retrieved_at": max(str(entry["retrieved_at"]) for entry in selected),
        }
    return provenance


def add_existing_core_sources(con: duckdb.DuckDBPyConnection, records: list[dict], aliases: list[dict]) -> None:
    tables = {row[0] for row in con.execute("show tables").fetchall()}
    if "open_clusters" in tables:
        columns = [row[1] for row in con.execute("pragma table_info('open_clusters')").fetchall()]
        select_columns = [name for name in (
            "cluster_id", "cluster_name", "ra_deg", "dec_deg", "radius_r50_deg", "dist_pc", "dist_ly",
            "source_url", "source_download_url", "retrieval_etag", "retrieval_checksum", "retrieved_at", "source_row_hash",
        ) if name in columns]
        for row in con.execute(f"select {','.join(select_columns)} from open_clusters").fetchall():
            item = dict(zip(select_columns, row))
            cluster_id = str(item["cluster_id"])
            key = f"cantat_gaudin_2020:{cluster_id}"
            name = str(item.get("cluster_name") or f"Open cluster {cluster_id}")
            records.append({
                "source_record_key": key,
                "source_catalog": "cantat_gaudin_2020",
                "source_version": "J/A+A/640/A1",
                "source_pk": cluster_id,
                "primary_name": name.replace("_", " "),
                "object_type_raw": "open_cluster",
                "ra_deg": item.get("ra_deg"),
                "dec_deg": item.get("dec_deg"),
                "source_frame": "ICRS",
                "source_epoch": "J2000",
                "major_axis_arcmin": float(item["radius_r50_deg"]) * 120.0 if item.get("radius_r50_deg") is not None else None,
                "minor_axis_arcmin": float(item["radius_r50_deg"]) * 120.0 if item.get("radius_r50_deg") is not None else None,
                "position_angle_deg": None,
                "area_sq_deg": None,
                "parallax_mas_raw": None,
                "distance_pc_raw": item.get("dist_pc") or (float(item["dist_ly"]) / LY_PER_PC if item.get("dist_ly") else None),
                "distance_method_raw": "cantat_gaudin_2020_cluster_distance",
                "outcome_hint": "accepted_candidate",
                "metadata_json": "{}",
                "source_row_hash": item.get("source_row_hash") or "",
                "transform_version": CORE_TRANSFORM_VERSION,
                "source_url": item.get("source_url"),
                "source_download_url": item.get("source_download_url"),
                "retrieval_etag": item.get("retrieval_etag"),
                "retrieval_checksum": item.get("retrieval_checksum"),
                "retrieved_at": item.get("retrieved_at"),
            })
            variants = {name, name.replace("_", " ")}
            match = re.fullmatch(r"(?i)Melotte[_ ]0*([0-9]+)", name)
            if match:
                variants.update({f"Melotte {int(match.group(1))}", f"Mel {int(match.group(1))}"})
            for alias in sorted(variants):
                aliases.append({"source_record_key": key, "alias_raw": alias, "alias_kind": "cluster_catalog_id", "alias_priority": "10", "source_catalog": "cantat_gaudin_2020"})
    if "superstellar_objects" in tables:
        rows = con.execute("select stable_object_key,object_name,ra_deg,dec_deg,dist_pc,dist_ly,object_meta_json,source_url,source_download_url,retrieval_etag,retrieval_checksum,retrieved_at,source_row_hash from superstellar_objects where object_type='supernova_remnant'").fetchall()
        for stable_key, name, ra, dec, dist_pc, dist_ly, meta_json, source_source_url, source_download_url, retrieval_etag, retrieval_checksum, retrieved_at, source_hash in rows:
            key = f"green_snr:{stable_key}"
            records.append({
                "source_record_key": key, "source_catalog": "green_snr", "source_version": "2024-10", "source_pk": str(stable_key),
                "primary_name": str(name or stable_key), "object_type_raw": "supernova_remnant", "ra_deg": ra, "dec_deg": dec,
                "source_frame": "ICRS", "source_epoch": "J2000", "major_axis_arcmin": None, "minor_axis_arcmin": None,
                "position_angle_deg": None, "area_sq_deg": None, "parallax_mas_raw": None, "distance_pc_raw": dist_pc or (float(dist_ly) / LY_PER_PC if dist_ly else None),
                "distance_method_raw": "", "outcome_hint": "accepted_candidate", "metadata_json": meta_json or "{}", "source_row_hash": source_hash or "", "transform_version": CORE_TRANSFORM_VERSION,
                "source_url": source_source_url, "source_download_url": source_download_url,
                "retrieval_etag": retrieval_etag, "retrieval_checksum": retrieval_checksum, "retrieved_at": retrieved_at,
            })
            aliases.append({"source_record_key": key, "alias_raw": str(name or stable_key), "alias_kind": "snr_catalog_id", "alias_priority": "10", "source_catalog": "green_snr"})
            try:
                metadata = json.loads(meta_json or "{}")
            except Exception:
                metadata = {}
            for alias in re.split(r"[,;]", str(metadata.get("other_names") or "")):
                if alias.strip():
                    aliases.append({"source_record_key": key, "alias_raw": alias.strip(), "alias_kind": "cross_identifier", "alias_priority": "20", "source_catalog": "green_snr"})


def classify(group_records: list[dict]) -> tuple[str, str, str]:
    ordered = sorted(group_records, key=lambda row: SOURCE_PRIORITY.get(str(row["source_catalog"]), 99))
    for record in ordered:
        raw = str(record.get("object_type_raw") or "")
        if record["source_catalog"] == "openngc" and raw in OPENNGC_TYPES:
            return OPENNGC_TYPES[raw]
        if raw in SOURCE_TYPES:
            return SOURCE_TYPES[raw]
        if record["source_catalog"] == "magakian" and raw not in {"PN?", "Gal?"}:
            return "catalog_region", "nebula", "reflection_nebula"
    return "catalog_region", "other", "unclassified"


def preferred_geometry(group_records: list[dict]) -> dict:
    candidates = sorted(
        group_records,
        key=lambda row: (
            SOURCE_PRIORITY.get(str(row["source_catalog"]), 99),
            0 if float_or_none(row.get("major_axis_arcmin")) is not None else 1,
            str(row["source_record_key"]),
        ),
    )
    for row in candidates:
        if float_or_none(row.get("ra_deg")) is not None and float_or_none(row.get("dec_deg")) is not None:
            major = float_or_none(row.get("major_axis_arcmin"))
            minor = float_or_none(row.get("minor_axis_arcmin"))
            area = float_or_none(row.get("area_sq_deg"))
            shape = "ellipse" if major is not None and minor is not None else "point"
            if major is None and area is not None and area > 0:
                major = minor = 120.0 * math.sqrt(area / math.pi)
                shape = "equivalent_circle"
            return {
                "ra_deg": float(row["ra_deg"]), "dec_deg": float(row["dec_deg"]), "shape_kind": shape,
                "major_axis_arcmin": major, "minor_axis_arcmin": minor, "position_angle_deg": float_or_none(row.get("position_angle_deg")),
                "geometry_source_record_key": row["source_record_key"], "geometry_status": "derived_area" if shape == "equivalent_circle" else "source",
            }
    return {"ra_deg": None, "dec_deg": None, "shape_kind": "missing", "major_axis_arcmin": None, "minor_axis_arcmin": None, "position_angle_deg": None, "geometry_source_record_key": None, "geometry_status": "missing"}


def star_distance_for_group(con: duckdb.DuckDBPyConnection, source_keys: set[str], relations: list[dict]) -> tuple[dict | None, list[dict]]:
    relation_rows = [row for row in relations if row["source_record_key"] in source_keys and row["relation_kind"] in {"illuminated_by", "central_star"}]
    resolved: list[dict] = []
    for relation in relation_rows:
        if relation["target_namespace"] != "hd" or not str(relation["target_value"]).isdigit():
            continue
        rows = con.execute(
            "select star_id,system_id,stable_object_key,hd_id,parallax_mas,parallax_error_mas,parallax_over_error,ruwe,dist_ly from stars where hd_id=?",
            [int(relation["target_value"])],
        ).fetchall()
        if len(rows) != 1:
            continue
        star_id, system_id, stable_key, hd_id, parallax, error, snr, ruwe, dist_ly = rows[0]
        resolved.append({
            **relation, "star_id": int(star_id), "system_id": int(system_id), "stable_object_key": stable_key, "hd_id": int(hd_id),
            "parallax_mas": float_or_none(parallax), "parallax_error_mas": float_or_none(error), "parallax_over_error": float_or_none(snr),
            "ruwe": float_or_none(ruwe), "dist_ly": float_or_none(dist_ly), "resolution_status": "accepted",
        })
    quality = [row for row in resolved if row["parallax_mas"] and row["parallax_mas"] > 0 and (row["parallax_over_error"] or 0) >= 10 and (row["ruwe"] or 99) <= 1.4]
    if not quality:
        return None, resolved
    systems = {row["system_id"] for row in quality}
    distances = [float(row["dist_ly"]) for row in quality if row["dist_ly"] is not None]
    agrees = len(systems) == 1 or (distances and max(distances) / min(distances) <= 1.10)
    if not agrees:
        return None, resolved
    weighted = []
    for row in quality:
        error = row["parallax_error_mas"]
        if error and error > 0:
            weighted.append((row["parallax_mas"], 1.0 / (error * error)))
    parallax = sum(value * weight for value, weight in weighted) / sum(weight for _, weight in weighted) if weighted else sum(row["parallax_mas"] for row in quality) / len(quality)
    dist_pc = 1000.0 / parallax
    lows, highs = [], []
    for row in quality:
        p, e = row["parallax_mas"], row["parallax_error_mas"] or 0.0
        lows.append(1000.0 / (p + e))
        highs.append(1000.0 / max(0.000001, p - e))
    return {
        "dist_pc": dist_pc, "dist_ly": dist_pc * LY_PER_PC, "distance_low_pc": min(lows), "distance_high_pc": max(highs),
        "distance_method": "associated_star_gaia_dr3_v1", "distance_confidence": "high" if len(quality) > 1 else "medium",
        "distance_evidence_json": json.dumps({"policy": DISTANCE_POLICY_VERSION, "hd_ids": sorted({row["hd_id"] for row in quality}), "star_ids": sorted({row["star_id"] for row in quality}), "system_ids": sorted(systems)}, sort_keys=True),
    }, resolved


def placement(object_family: str, object_type: str, distance: dict | None) -> tuple[str, int | None]:
    if object_family == "galaxy" or object_type in {"globular_cluster"}:
        return "extragalactic_sky" if object_family == "galaxy" else "deep_galactic", None
    if not distance:
        return "sky_only", None
    dist_ly = float(distance["dist_ly"])
    if dist_ly > 1000:
        return "deep_galactic", None
    tier = next((radius for radius in (100, 250, 500, 1000) if dist_ly <= radius), 1000)
    return "local_3d", tier


def materialize_core(
    con: duckdb.DuckDBPyConnection,
    state_dir: Path,
    root_dir: Path,
    report_path: Path | None = None,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    cooked = state_dir / "cooked" / "extended_objects"
    records = load_csv(cooked / "source_records.csv")
    aliases = load_csv(cooked / "source_aliases.csv")
    relations = load_csv(cooked / "source_relations.csv")
    if not records:
        raise RuntimeError(f"Missing cooked extended-object records under {cooked}")
    ingested_at = ingested_at or "1970-01-01T00:00:00Z"
    source_provenance = load_source_provenance(state_dir)
    add_existing_core_sources(con, records, aliases)
    by_key = {row["source_record_key"]: row for row in records}
    union = UnionFind(list(by_key))
    token_records: dict[tuple[str, str], set[str]] = defaultdict(set)
    for alias in aliases:
        if alias["source_record_key"] not in by_key:
            continue
        token = identity_token(alias["alias_raw"])
        if token:
            token_records[token].add(alias["source_record_key"])
    collisions: list[dict] = []
    for token, keys in sorted(token_records.items()):
        active = sorted(key for key in keys if by_key[key].get("outcome_hint") not in {"excluded_stellar_domain", "excluded_event_domain", "excluded_nonexistent"})
        if len(active) > 1:
            first = active[0]
            for other in active[1:]:
                union.union(first, other)
        if len(keys) > 8:
            collisions.append({"namespace": token[0], "value": token[1], "source_record_count": len(keys), "source_record_keys": sorted(keys)})
    groups: dict[str, list[dict]] = defaultdict(list)
    for key, record in by_key.items():
        groups[union.find(key)].append(record)

    public_names_payload = json.loads((root_dir / "config" / "extended_object_public_names.json").read_text(encoding="utf-8"))
    public_names = {(row["identity_namespace"], str(row["identity_value"]).lower()): row for row in public_names_payload.get("names", []) if row.get("review_status") == "accepted"}
    aliases_by_key: dict[str, list[dict]] = defaultdict(list)
    for alias in aliases:
        aliases_by_key[alias["source_record_key"]].append(alias)

    accepted: list[dict] = []
    reconciliations: list[dict] = []
    quarantine: list[dict] = []
    accepted_aliases: list[dict] = []
    accepted_identifiers: list[dict] = []
    resolved_relation_cache: dict[int, list[dict]] = {}
    for group_records in groups.values():
        candidates = [row for row in group_records if row.get("outcome_hint") == "accepted_candidate"]
        if not candidates:
            for row in group_records:
                reason = row.get("outcome_hint") or "excluded"
                reconciliations.append({"source_record_key": row["source_record_key"], "extended_object_id": None, "outcome": reason, "reason": reason})
                if reason.startswith("quarantine"):
                    quarantine.append({"source_record_key": row["source_record_key"], "reason": reason, "details_json": "{}"})
            continue
        master = min(candidates, key=lambda row: (SOURCE_PRIORITY.get(row["source_catalog"], 99), row["source_record_key"]))
        stable_key = f"extended:{master['source_record_key']}"
        object_id = stable_id(stable_key)
        source_keys = {row["source_record_key"] for row in group_records}
        group_aliases = [alias for key in sorted(source_keys) for alias in aliases_by_key.get(key, [])]
        tokens = {token for alias in group_aliases if (token := identity_token(alias["alias_raw"]))}
        public_name = next((public_names[token] for token in sorted(tokens) if token in public_names), None)
        entity_kind, object_family, object_type = classify(group_records)
        geometry = preferred_geometry(group_records)
        specialist_distance = next((row for row in sorted(group_records, key=lambda row: SOURCE_PRIORITY.get(row["source_catalog"], 99)) if row["source_catalog"] == "cantat_gaudin_2020" and float_or_none(row.get("distance_pc_raw")) is not None), None)
        resolved_relations: list[dict] = []
        if specialist_distance:
            dist_pc = float(specialist_distance["distance_pc_raw"])
            distance = {"dist_pc": dist_pc, "dist_ly": dist_pc * LY_PER_PC, "distance_low_pc": None, "distance_high_pc": None, "distance_method": specialist_distance.get("distance_method_raw") or "specialist_catalog", "distance_confidence": "high", "distance_evidence_json": json.dumps({"source_record_key": specialist_distance["source_record_key"]}, sort_keys=True)}
        else:
            distance, resolved_relations = star_distance_for_group(con, source_keys, relations)
        map_domain, tier = placement(object_family, object_type, distance)
        canonical_alias_candidates = sorted(group_aliases, key=canonical_name_rank)
        canonical_name = canonical_alias_candidates[0]["alias_raw"] if canonical_alias_candidates else master["primary_name"]
        display_name = public_name["public_name"] if public_name else canonical_name
        ra, dec = geometry["ra_deg"], geometry["dec_deg"]
        x = y = z = None
        if distance and ra is not None and dec is not None and map_domain == "local_3d":
            radius = math.radians(ra)
            declination = math.radians(dec)
            dist_ly = float(distance["dist_ly"])
            x = dist_ly * math.cos(declination) * math.cos(radius)
            y = dist_ly * math.cos(declination) * math.sin(radius)
            z = dist_ly * math.sin(declination)
        license_name, redistribution_ok, license_note = license_fields(master["source_catalog"])
        provenance = source_provenance.get(master["source_catalog"], {})
        accepted.append({
            "extended_object_id": object_id, "stable_object_key": stable_key, "canonical_name": canonical_name, "display_name": display_name,
            "entity_kind": entity_kind, "object_family": object_family, "object_type": object_type, **geometry,
            "dist_pc": distance["dist_pc"] if distance else None, "dist_ly": distance["dist_ly"] if distance else None,
            "distance_low_pc": distance["distance_low_pc"] if distance else None, "distance_high_pc": distance["distance_high_pc"] if distance else None,
            "distance_method": distance["distance_method"] if distance else "missing", "distance_confidence": distance["distance_confidence"] if distance else "missing",
            "distance_evidence_json": distance["distance_evidence_json"] if distance else "{}", "map_domain": map_domain, "nominal_radius_tier_ly": tier,
            "x_helio_ly": x, "y_helio_ly": y, "z_helio_ly": z, "type_policy_version": TYPE_POLICY_VERSION,
            "source_catalog": master["source_catalog"], "source_version": master["source_version"], "source_url": master.get("source_url") or source_url(master["source_catalog"]),
            "source_download_url": provenance.get("source_download_url") or master.get("source_download_url") or source_url(master["source_catalog"]), "source_doi": None, "source_pk": master["source_pk"], "source_row_id": None,
            "source_row_hash": master.get("source_row_hash") or "", "license": license_name, "redistribution_ok": redistribution_ok, "license_note": license_note,
            "retrieval_etag": master.get("retrieval_etag"), "retrieval_checksum": provenance.get("retrieval_checksum") or master.get("retrieval_checksum"), "retrieved_at": provenance.get("retrieved_at") or master.get("retrieved_at"), "ingested_at": ingested_at, "transform_version": CORE_TRANSFORM_VERSION,
        })
        resolved_relation_cache[object_id] = resolved_relations
        if public_name:
            group_aliases.append({"source_record_key": master["source_record_key"], "alias_raw": public_name["public_name"], "alias_kind": "reviewed_public_name", "alias_priority": "0", "source_catalog": "reviewed_public_names"})
        dedup_aliases: dict[str, dict] = {}
        for alias in group_aliases:
            norm = normalize_search(alias["alias_raw"])
            if not norm:
                continue
            candidate = {"extended_object_id": object_id, "alias_raw": alias["alias_raw"], "alias_norm": norm, "alias_kind": alias["alias_kind"], "alias_priority": int(alias.get("alias_priority") or 99), "source_catalog": alias["source_catalog"], "source_record_key": alias["source_record_key"]}
            if norm not in dedup_aliases or candidate["alias_priority"] < dedup_aliases[norm]["alias_priority"]:
                dedup_aliases[norm] = candidate
            token = identity_token(alias["alias_raw"])
            if token:
                accepted_identifiers.append({"extended_object_id": object_id, "namespace": token[0], "id_value_raw": token[1], "id_value_norm": token[1], "source_catalog": alias["source_catalog"], "source_record_key": alias["source_record_key"]})
        accepted_aliases.extend(dedup_aliases.values())
        for row in group_records:
            hint = row.get("outcome_hint") or "accepted_candidate"
            outcome = "accepted" if row["source_record_key"] == master["source_record_key"] else ("redirected" if hint == "redirect" else "reconciled")
            reconciliations.append({"source_record_key": row["source_record_key"], "extended_object_id": object_id, "outcome": outcome, "reason": "explicit_identifier_group" if outcome != "accepted" else "identity_master"})

    con.execute("drop table if exists extended_objects")
    con.execute("drop table if exists extended_object_aliases")
    con.execute("drop table if exists extended_object_identifiers")
    con.execute("drop table if exists extended_object_search_terms")
    con.execute("drop table if exists extended_object_source_reconciliation")
    con.execute("drop table if exists extended_object_identity_quarantine")
    _create_from_rows(con, "extended_objects", accepted)
    _create_from_rows(con, "extended_object_aliases", sorted(accepted_aliases, key=lambda row: (row["extended_object_id"], row["alias_priority"], row["alias_norm"])), add_id="extended_object_alias_id")
    identifier_dedup = {(row["extended_object_id"], row["namespace"], row["id_value_norm"]): row for row in accepted_identifiers}
    _create_from_rows(con, "extended_object_identifiers", [identifier_dedup[key] for key in sorted(identifier_dedup)], add_id="extended_object_identifier_id")
    search_rows = sorted(
        ({"extended_object_id": row["extended_object_id"], "term_raw": row["alias_raw"], "term_norm": row["alias_norm"], "term_kind": row["alias_kind"], "term_priority": row["alias_priority"]} for row in accepted_aliases),
        key=lambda row: (row["extended_object_id"], row["term_priority"], row["term_norm"], row["term_raw"]),
    )
    _create_from_rows(con, "extended_object_search_terms", search_rows, add_id="extended_object_search_term_id")
    _create_from_rows(con, "extended_object_source_reconciliation", reconciliations, add_id="extended_object_reconciliation_id")
    _create_from_rows(con, "extended_object_identity_quarantine", quarantine, add_id="extended_object_quarantine_id")
    report = {
        "schema_version": "extended_object_coverage_v1", "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "counts": {"source_records": len(records), "accepted_objects": len(accepted), "aliases": len(accepted_aliases), "identifiers": len(identifier_dedup), "quarantine": len(quarantine)},
        "source_outcomes": _count_values(reconciliations, "outcome"), "object_types": _count_values(accepted, "object_type"),
        "map_domains": _count_values(accepted, "map_domain"), "identity_collisions": collisions,
    }
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _create_from_rows(con: duckdb.DuckDBPyConnection, table_name: str, rows: list[dict], add_id: str | None = None) -> None:
    if not rows:
        raise RuntimeError(f"Cannot create {table_name} without rows")
    columns = list(rows[0])
    temp_name = f"_{table_name}_rows"
    payload = [{key: row.get(key) for key in columns} for row in rows]
    import pandas as pd
    frame = pd.DataFrame(payload)
    con.register(temp_name, frame)
    if add_id:
        con.execute(f"create table {table_name} as select row_number() over ()::bigint as {add_id}, * from {temp_name}")
    else:
        con.execute(f"create table {table_name} as select * from {temp_name}")
    con.unregister(temp_name)


def _count_values(rows: list[dict], field: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get(field) or "missing")] += 1
    return dict(sorted(counts.items()))


def materialize_arm(con: duckdb.DuckDBPyConnection, state_dir: Path, build_id: str, ingested_at: str) -> dict[str, int]:
    cooked = state_dir / "cooked" / "extended_objects"
    records_path = str(cooked / "source_records.csv").replace("'", "''")
    relations_path = str(cooked / "source_relations.csv").replace("'", "''")
    con.execute(f"create table extended_object_source_records as select r.*, x.extended_object_id, x.outcome as reconciliation_outcome, x.reason as reconciliation_reason, '{build_id}'::varchar as build_id from read_csv_auto('{records_path}', all_varchar=true) r left join core.extended_object_source_reconciliation x using(source_record_key)")
    con.execute("""
        create table extended_object_geometry_evidence as
        select row_number() over ()::bigint as geometry_evidence_id, extended_object_id, source_record_key,
               try_cast(ra_deg as double) ra_deg, try_cast(dec_deg as double) dec_deg, source_frame, source_epoch,
               try_cast(major_axis_arcmin as double) major_axis_arcmin, try_cast(minor_axis_arcmin as double) minor_axis_arcmin,
               try_cast(position_angle_deg as double) position_angle_deg, try_cast(area_sq_deg as double) area_sq_deg,
               case when major_axis_arcmin is not null and minor_axis_arcmin is not null then 'ellipse'
                    when area_sq_deg is not null then 'area_only' else 'point' end shape_kind,
               source_catalog, source_version, source_pk, source_row_hash, transform_version
        from extended_object_source_records where extended_object_id is not null and ra_deg is not null and dec_deg is not null
    """)
    con.execute("""
        create table extended_object_distance_evidence as
        select row_number() over ()::bigint as distance_evidence_id, extended_object_id, source_record_key,
               try_cast(distance_pc_raw as double) distance_pc, try_cast(parallax_mas_raw as double) parallax_mas,
               distance_method_raw distance_method,
               case when source_catalog='cederblad' then 'historical_low' else 'source_native_unreviewed' end confidence_tier,
               false is_preferred, source_catalog, source_version, source_pk, source_row_hash, transform_version
        from extended_object_source_records
        where extended_object_id is not null and (distance_pc_raw is not null or parallax_mas_raw is not null)
        union all
        select row_number() over ()::bigint + 1000000000, extended_object_id, geometry_source_record_key,
               dist_pc, null, distance_method, distance_confidence, true, source_catalog, source_version, source_pk, source_row_hash, transform_version
        from core.extended_objects where dist_pc is not null
    """)
    con.execute(f"create temp view _extended_relations_raw as select * from read_csv_auto('{relations_path}', all_varchar=true)")
    con.execute("""
        create table extended_object_relations as
        with base as (
          select row_number() over ()::bigint relation_id, x.extended_object_id, r.source_record_key, r.relation_kind,
                 r.target_namespace, r.target_value, r.confidence_tier, r.source_catalog
          from _extended_relations_raw r
          join core.extended_object_source_reconciliation x using(source_record_key)
          where x.extended_object_id is not null
        ), resolved as (
          select b.*, s.star_id target_star_id, s.system_id target_system_id, s.stable_object_key target_stable_object_key,
                 case when b.target_namespace='hd' and s.star_id is not null then 'accepted' else 'unresolved' end resolution_status
          from base b left join core.stars s on b.target_namespace='hd' and try_cast(b.target_value as bigint)=s.hd_id
        ) select *, ?::varchar build_id, ?::varchar ingested_at, ?::varchar transform_version from resolved
    """, [build_id, ingested_at, CORE_TRANSFORM_VERSION])
    return {
        "extended_object_source_records": con.execute("select count(*) from extended_object_source_records").fetchone()[0],
        "extended_object_geometry_evidence": con.execute("select count(*) from extended_object_geometry_evidence").fetchone()[0],
        "extended_object_distance_evidence": con.execute("select count(*) from extended_object_distance_evidence").fetchone()[0],
        "extended_object_relations": con.execute("select count(*) from extended_object_relations").fetchone()[0],
    }
