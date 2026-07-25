from __future__ import annotations

import gzip
import json
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import db
from .queries import choose_display_name_info


EXPECTED_PROJECTION_SCHEMA = "spacegate.public_read.v2"
EXPECTED_SEARCH_SCHEMA = "spacegate.search.v2"
DEFAULT_CANDIDATE_TERMS = 5000
DEFAULT_CANDIDATE_SYSTEMS = 2000


class PublicReadUnavailable(RuntimeError):
    pass


class PublicReadIncompatible(PublicReadUnavailable):
    pass


@dataclass(frozen=True)
class PublicReadPaths:
    database: Path
    manifest: Path


_STATS_LOCK = threading.Lock()
_STATS: dict[str, int] = {
    "projection_hits": 0,
    "projection_misses": 0,
    "compatibility_fallbacks": 0,
    "incompatible_artifacts": 0,
    "search_requests": 0,
    "summary_requests": 0,
    "hierarchy_requests": 0,
    "singleton_seed_requests": 0,
}


def _increment(key: str) -> None:
    with _STATS_LOCK:
        _STATS[key] = int(_STATS.get(key, 0)) + 1


def runtime_stats() -> dict[str, Any]:
    with _STATS_LOCK:
        return dict(_STATS)


def record_compatibility_fallback() -> None:
    _increment("compatibility_fallbacks")


def _state_dir() -> Path:
    root = Path(__file__).resolve().parents[3]
    return Path(
        os.getenv("SPACEGATE_STATE_DIR")
        or os.getenv("SPACEGATE_DATA_DIR")
        or root / "data"
    )


def paths_for_build(build_id: str) -> PublicReadPaths:
    explicit = os.getenv("SPACEGATE_PUBLIC_READ_PATH", "").strip()
    if explicit:
        database = Path(explicit)
        return PublicReadPaths(database=database, manifest=database.parent / "manifest.json")
    root = _state_dir() / "derived" / "public_read" / build_id
    return PublicReadPaths(database=root / "public_read.sqlite", manifest=root / "manifest.json")


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise PublicReadUnavailable("public-read manifest unavailable") from exc
    if not isinstance(value, dict):
        raise PublicReadIncompatible("public-read manifest is not an object")
    return value


def _connect_path(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise PublicReadUnavailable("public-read artifact unavailable")
    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    con = sqlite3.connect(uri, uri=True, timeout=2.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    con.execute("PRAGMA cache_size=-32768")
    con.execute("PRAGMA mmap_size=268435456")
    return con


def connect(expected_build_id: str | None = None) -> sqlite3.Connection:
    if str(os.getenv("SPACEGATE_PUBLIC_READ_DISABLED", "")).strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        raise PublicReadUnavailable("public-read projection disabled")
    build_id = expected_build_id or db.build_id()
    if not build_id:
        raise PublicReadUnavailable("active build identity unavailable")
    paths = paths_for_build(build_id)
    manifest = _read_manifest(paths.manifest)
    if manifest.get("status") != "pass":
        _increment("incompatible_artifacts")
        raise PublicReadIncompatible("public-read manifest did not pass verification")
    if manifest.get("sample_limit") is not None:
        _increment("incompatible_artifacts")
        raise PublicReadIncompatible("sample public-read artifact cannot serve public traffic")
    if manifest.get("build_id") != build_id:
        _increment("incompatible_artifacts")
        raise PublicReadIncompatible("public-read artifact build identity mismatch")
    if manifest.get("projection_schema_version") != EXPECTED_PROJECTION_SCHEMA:
        _increment("incompatible_artifacts")
        raise PublicReadIncompatible("public-read projection schema mismatch")
    if manifest.get("search_schema_version") != EXPECTED_SEARCH_SCHEMA:
        _increment("incompatible_artifacts")
        raise PublicReadIncompatible("public-read search schema mismatch")
    con = _connect_path(paths.database)
    metadata = dict(con.execute("SELECT key,value FROM metadata").fetchall())
    if metadata.get("build_id") != build_id:
        con.close()
        _increment("incompatible_artifacts")
        raise PublicReadIncompatible("public-read database build identity mismatch")
    _increment("projection_hits")
    return con


def available(expected_build_id: str | None = None) -> bool:
    try:
        con = connect(expected_build_id)
    except PublicReadUnavailable:
        _increment("projection_misses")
        return False
    con.close()
    return True


def _json_list(value: Any) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _bool_fields(value: dict[str, Any], fields: Iterable[str]) -> None:
    for field in fields:
        value[field] = bool(value.get(field))


def _system_payload(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    _bool_fields(
        item,
        [
            "has_gaia_nss_evidence",
            "has_msc_evidence",
            "has_sbx_evidence",
            "has_wds_evidence",
            "has_orb6_evidence",
            "has_habitable_candidate",
        ],
    )
    item["system_id"] = int(item["system_id"])
    item["spatial_index"] = int(item["system_id"])
    item["star_count"] = int(item.get("star_count") or 0)
    item["planet_count"] = int(item.get("planet_count") or 0)
    item["star_teff_count"] = int(item.get("star_teff_count") or 0)
    item["spectral_classes"] = [
        str(token) for token in _json_list(item.pop("spectral_classes_json", "[]"))
    ]
    item["gaia_id"] = item.get("gaia_id_text")
    item["hip_id"] = item.get("hip_id_text")
    item["hd_id"] = item.get("hd_id_text")
    item["snapshot"] = None
    item["provenance"] = {
        "source_catalog": item.pop("source_catalog", None),
        "source_version": item.pop("source_version", None),
        "source_pk": item.pop("source_pk_text", None),
        "source_row_hash": item.pop("source_row_hash", None),
        "transform_version": item.pop("transform_version", None),
    }
    return item


def _star_payload(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["star_id"] = int(item["star_id"])
    item["system_id"] = int(item["system_id"])
    item["spatial_index"] = item["star_id"]
    item["gaia_id"] = item.get("gaia_id_text")
    item["hip_id"] = item.get("hip_id_text")
    item["hd_id"] = item.get("hd_id_text")
    item["spectral_class"] = item.get("selected_classification") or "UNKNOWN"
    item["classification_evidence_json"] = json.dumps(
        {
            "classification_value": item.get("selected_classification"),
            "classification_status": item.get("classification_status"),
            "evidence_basis": item.get("classification_basis"),
            "selected_fact_id": item.get("classification_fact_id"),
            "confidence_score": item.get("classification_confidence"),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    item["selected_parameters"] = {
        "teff_k": item.get("teff_k"),
        "teff_k_lower": item.get("teff_k_lower"),
        "teff_k_upper": item.get("teff_k_upper"),
        "teff_k_fact_id": item.get("teff_k_fact_id"),
        "radius_rsun": item.get("radius_rsun"),
        "radius_rsun_fact_id": item.get("radius_rsun_fact_id"),
        "mass_msun": item.get("mass_msun"),
        "mass_msun_fact_id": item.get("mass_msun_fact_id"),
        "luminosity_lsun": item.get("luminosity_lsun"),
        "luminosity_lsun_fact_id": item.get("luminosity_lsun_fact_id"),
        "luminosity_lsun_status": item.get("luminosity_status"),
        "luminosity_lsun_basis": item.get("luminosity_basis"),
        "parameter_source": item.get("parameter_source"),
    }
    item["provenance"] = {
        "source_catalog": item.pop("source_catalog", None),
        "source_version": item.pop("source_version", None),
        "source_row_hash": item.pop("source_row_hash", None),
        "transform_version": item.pop("transform_version", None),
    }
    return item


def _planet_payload(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["planet_id"] = int(item["planet_id"])
    item["system_id"] = int(item["system_id"])
    if item.get("star_id") is not None:
        item["star_id"] = int(item["star_id"])
    item["planet_size_mass_class"] = item.pop("size_mass_class", None)
    item["planet_insolation_class"] = item.pop("insolation_class", None)
    item["planet_composition_proxy_class"] = item.pop("composition_proxy_class", None)
    item["planet_classifier_version"] = item.pop("classifier_version", None)
    encoded_lineage = item.pop("selected_fact_lineage_json", None)
    try:
        item["selected_fact_lineage"] = (
            json.loads(encoded_lineage) if encoded_lineage else {}
        )
    except (TypeError, ValueError) as exc:
        raise PublicReadIncompatible("planet selected-fact lineage is invalid") from exc
    item["provenance"] = {
        "source_catalog": item.pop("source_catalog", None),
        "source_version": item.pop("source_version", None),
        "source_row_hash": item.pop("source_row_hash", None),
        "transform_version": item.pop("transform_version", None),
    }
    return item


def aliases_for_systems(
    con: sqlite3.Connection, system_ids: Sequence[int]
) -> dict[int, list[dict[str, Any]]]:
    if not system_ids:
        return {}
    placeholders = ",".join("?" for _ in system_ids)
    result: dict[int, list[dict[str, Any]]] = {}
    for row in con.execute(
        f"""
        SELECT system_id,star_id,target_type,target_id,stable_object_key,
               alias_raw,alias_norm,alias_kind,alias_priority,is_primary,
               source_catalog,source_version
        FROM aliases
        WHERE system_id IN ({placeholders})
        ORDER BY system_id,alias_priority,alias_kind,alias_raw,alias_id
        """,
        [int(value) for value in system_ids],
    ):
        item = dict(row)
        system_id = int(item.pop("system_id"))
        item["is_primary"] = bool(item.get("is_primary"))
        result.setdefault(system_id, []).append(item)
    return result


def stellar_badges_for_systems(
    con: sqlite3.Connection, system_ids: Sequence[int]
) -> dict[int, list[dict[str, Any]]]:
    if not system_ids:
        return {}
    placeholders = ",".join("?" for _ in system_ids)
    result: dict[int, list[dict[str, Any]]] = {}
    for row in con.execute(
        f"""
        SELECT system_id,star_id,stable_object_key,star_name,
               selected_classification,classification_status,
               classification_basis,classification_fact_id
        FROM stars
        WHERE system_id IN ({placeholders})
        ORDER BY system_id,star_id
        """,
        [int(value) for value in system_ids],
    ):
        system_id = int(row["system_id"])
        result.setdefault(system_id, []).append(
            {
                "hierarchy_node_key": row["stable_object_key"],
                "leaf_component_key": f"comp:star:{row['stable_object_key']}",
                "evidence_component_key": None,
                "star_id_text": str(row["star_id"]),
                "stable_object_key": row["stable_object_key"],
                "display_name": row["star_name"],
                "classification_value": row["selected_classification"] or "UNKNOWN",
                "classification_status": row["classification_status"] or "missing",
                "evidence_basis": row["classification_basis"],
                "selected_fact_id": row["classification_fact_id"],
            }
        )
    return result


def planet_badges_for_systems(
    con: sqlite3.Connection, system_ids: Sequence[int]
) -> dict[int, list[dict[str, Any]]]:
    if not system_ids:
        return {}
    placeholders = ",".join("?" for _ in system_ids)
    result: dict[int, list[dict[str, Any]]] = {}
    for row in con.execute(
        f"""
        SELECT system_id,planet_id,stable_object_key,planet_name
        FROM planets
        WHERE system_id IN ({placeholders})
        ORDER BY system_id,lower(planet_name),planet_id
        """,
        [int(value) for value in system_ids],
    ):
        result.setdefault(int(row["system_id"]), []).append(
            {
                "planet_id_text": str(row["planet_id"]),
                "stable_object_key": row["stable_object_key"],
                "display_name": row["planet_name"],
            }
        )
    return result


def system_summary(
    con: sqlite3.Connection,
    system_id: int,
    *,
    name_style: str = "public_full",
) -> dict[str, Any] | None:
    _increment("summary_requests")
    row = con.execute("SELECT * FROM systems WHERE system_id=?", [int(system_id)]).fetchone()
    if row is None:
        return None
    item = _system_payload(row)
    aliases = aliases_for_systems(con, [system_id]).get(int(system_id), [])
    display = choose_display_name_info(
        item.get("system_name"),
        aliases,
        root_system=True,
        name_style=name_style,
    )
    item.update(display)
    item["aliases"] = aliases
    badges = stellar_badges_for_systems(con, [system_id]).get(int(system_id), [])
    item["stellar_object_badges"] = badges
    item["stellar_class_badges"] = [
        row.get("classification_value") or "UNKNOWN" for row in badges
    ]
    item["planet_object_badges"] = planet_badges_for_systems(
        con, [system_id]
    ).get(int(system_id), [])
    return item


def preview_policy(summary: dict[str, Any]) -> dict[str, Any]:
    representation = str(summary.get("scene_representation") or "").strip()
    if representation in {"singleton_seed", "compact_seed"}:
        return {
            "preview_tier": "lightweight_singleton",
            "preview_basis": [f"public_read:{representation}"],
            "is_lightweight_preview_safe": True,
            "has_prebuilt_simulation_scene": False,
        }
    if representation == "full_scene":
        return {
            "preview_tier": "dynamic_simulation_scene",
            "preview_basis": ["public_read:full_scene"],
            "is_lightweight_preview_safe": False,
            "has_prebuilt_simulation_scene": False,
        }
    raise PublicReadIncompatible(
        f"unknown public-read scene representation: {representation or 'missing'}"
    )


def system_objects(
    con: sqlite3.Connection,
    system_id: int,
    *,
    name_style: str = "public_full",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    aliases = aliases_for_systems(con, [system_id]).get(int(system_id), [])
    star_aliases: dict[int, list[dict[str, Any]]] = {}
    for alias in aliases:
        if alias.get("star_id") is not None:
            star_aliases.setdefault(int(alias["star_id"]), []).append(alias)
    stars: list[dict[str, Any]] = []
    for row in con.execute(
        "SELECT * FROM stars WHERE system_id=? ORDER BY star_id", [int(system_id)]
    ):
        star = _star_payload(row)
        own_aliases = star_aliases.get(int(star["star_id"]), [])
        display = choose_display_name_info(
            star.get("star_name"),
            own_aliases,
            root_system=False,
            name_style=name_style,
        )
        star.update(display)
        star["aliases"] = own_aliases
        star["arm_evidence"] = {"selected_parameters": star.pop("selected_parameters")}
        star["arm_catalogs"] = [star["arm_evidence"]["selected_parameters"].get("parameter_source")]
        star["arm_catalogs"] = [value for value in star["arm_catalogs"] if value]
        stars.append(star)
    planets = [
        _planet_payload(row)
        for row in con.execute(
            "SELECT * FROM planets WHERE system_id=? ORDER BY planet_id", [int(system_id)]
        )
    ]
    return stars, planets


def singleton_scene_seed(
    con: sqlite3.Connection, system_id: int
) -> dict[str, Any] | None:
    _increment("singleton_seed_requests")
    row = con.execute(
        "SELECT * FROM singleton_scene_seeds WHERE system_id=?", [int(system_id)]
    ).fetchone()
    return _row_dict(row)


def hierarchy_bundle(
    con: sqlite3.Connection, system_id: int
) -> dict[str, Any] | None:
    _increment("hierarchy_requests")
    row = con.execute(
        "SELECT * FROM hierarchy_bundles WHERE system_id=?", [int(system_id)]
    ).fetchone()
    if row is None:
        return None
    item = dict(row)
    encoded = item.pop("payload_gzip", None)
    if encoded:
        item["payload"] = json.loads(gzip.decompress(encoded).decode("utf-8"))
    return item


def singleton_hierarchy(
    summary: dict[str, Any], star: dict[str, Any]
) -> dict[str, Any]:
    system_key = str(summary["stable_object_key"])
    star_key = str(star["stable_object_key"])
    root_key = f"comp:system:{system_key}"
    leaf_key = f"comp:star:{star_key}"
    selected = star.get("arm_evidence", {}).get("selected_parameters", {})
    classification = {
        "system_id": int(summary["system_id"]),
        "system_stable_object_key": system_key,
        "hierarchy_node_key": star_key,
        "leaf_component_key": leaf_key,
        "evidence_component_key": None,
        "star_id": int(star["star_id"]),
        "stable_object_key": star_key,
        "display_name": star.get("display_name") or star.get("star_name"),
        "catalog_component_label": star.get("component"),
        "node_kind": "star",
        "hierarchy_source_basis": "canonical_star",
        "classification_value": star.get("selected_classification") or "UNKNOWN",
        "classification_status": star.get("classification_status") or "missing",
        "evidence_basis": star.get("classification_basis"),
        "selected_fact_id": star.get("classification_fact_id"),
        "source_catalog": "public_read_v2",
        "source_version": EXPECTED_PROJECTION_SCHEMA,
        "confidence_score": star.get("classification_confidence"),
    }
    quick_facts = {
        "spectral_type_raw": star.get("spectral_type_raw"),
        "spectral_class": star.get("selected_classification") or "UNKNOWN",
        "visual_stellar_class": star.get("selected_classification") or "UNKNOWN",
        "visual_stellar_class_status": star.get("classification_status") or "missing",
        "visual_stellar_class_basis": star.get("classification_basis"),
        "teff_k": selected.get("teff_k"),
        "mass_msun": selected.get("mass_msun"),
        "radius_rsun": selected.get("radius_rsun"),
        "luminosity_lsun": selected.get("luminosity_lsun"),
        "luminosity_lsun_fact_id": selected.get("luminosity_lsun_fact_id"),
        "luminosity_lsun_status": selected.get("luminosity_lsun_status"),
        "luminosity_lsun_basis": selected.get("luminosity_lsun_basis"),
        "vmag": star.get("vmag"),
        "dist_ly": star.get("dist_ly"),
        "stellar_leaf_display_class": classification["classification_value"],
        "stellar_leaf_display_class_status": classification["classification_status"],
        "stellar_leaf_display_class_basis": classification["evidence_basis"],
        "stellar_leaf_display_class_fact_id": classification["selected_fact_id"],
    }
    leaf = {
        "stable_component_key": leaf_key,
        "component_type": star.get("object_type") or "star",
        "component_family": star.get("object_family") or "stellar",
        "core_object_type": "star",
        "core_object_id": int(star["star_id"]),
        "display_name": star.get("display_name") or star.get("star_name"),
        "catalog_component_label": star.get("component"),
        "source_catalog": "public_read_v2",
        "synthetic": False,
        "orbit": None,
        "quick_facts": quick_facts,
        "node_kind": "star",
        "self_star_count": 1,
        "member_role": None,
        "catalog_relation_label": None,
        "edge_kind": "contains",
        "depth": 1,
        "children": [],
        "child_count": 0,
        "descendant_count": 0,
        "direct_type_counts": {},
        "total_type_counts": {"star": 1},
        "total_star_count": 1,
        "collapsed_by_default": False,
        "stellar_leaf_classification": classification,
    }
    root = {
        "stable_component_key": root_key,
        "component_type": "system",
        "component_family": "system",
        "core_object_type": "system",
        "core_object_id": int(summary["system_id"]),
        "display_name": summary.get("display_name") or summary.get("system_name"),
        "catalog_component_label": None,
        "source_catalog": "public_read_v2",
        "synthetic": False,
        "orbit": None,
        "quick_facts": None,
        "node_kind": "system",
        "self_star_count": 0,
        "depth": 0,
        "children": [leaf],
        "child_count": 1,
        "descendant_count": 1,
        "direct_type_counts": {"star": 1},
        "total_type_counts": {"system": 1, "star": 1},
        "total_star_count": 1,
        "collapsed_by_default": False,
    }
    return {
        "root": root,
        "counts": {
            "stars": 1,
            "nodes": 2,
            "direct_children": 1,
            "type_counts": {"system": 1, "star": 1},
        },
        "preferred_root_key": root_key,
        "root_keys_considered": [root_key],
    }


def projected_system_detail(
    con: sqlite3.Connection,
    system_id: int,
    *,
    name_style: str = "public_full",
) -> dict[str, Any] | None:
    summary = system_summary(con, system_id, name_style=name_style)
    if summary is None:
        return None
    bundle = hierarchy_bundle(con, system_id)
    if bundle and isinstance(bundle.get("payload"), dict):
        payload = dict(bundle["payload"])
        payload.setdefault("read_backend", "public_read_v2_bundle")
        return payload
    if summary.get("hierarchy_representation") != "singleton_seed":
        raise PublicReadIncompatible("required hierarchy bundle is not materialized")
    stars, planets = system_objects(con, system_id, name_style=name_style)
    if len(stars) != 1 or planets:
        raise PublicReadIncompatible("singleton representation disagrees with projected objects")
    hierarchy = singleton_hierarchy(summary, stars[0])
    classification = hierarchy["root"]["children"][0]["stellar_leaf_classification"]
    return {
        "system": summary,
        "stars": stars,
        "planets": planets,
        "eclipsing_binaries": [],
        "infrared_evidence": {
            "summary": {
                "match_count": 0,
                "catalog_counts": {},
                "policy": "No targeted infrared evidence is attached to this projected singleton.",
            },
            "stars": {},
            "matches": [],
        },
        "narrative_blocks": [],
        "hierarchy": hierarchy,
        "stellar_leaf_classifications": [classification],
        "read_backend": "public_read_v2_singleton",
    }


def identifier_resolution(
    con: sqlite3.Connection, namespace: str, identifier_norm: str
) -> dict[str, Any] | None:
    records = [
        dict(row)
        for row in con.execute(
            """
            SELECT * FROM identifier_outcomes
            WHERE namespace=? AND identifier_norm=?
            ORDER BY outcome,system_id,star_id
            """,
            [namespace.lower(), identifier_norm.lower()],
        )
    ]
    if not records:
        return None
    accepted = [
        row for row in records if row.get("outcome") in {"accepted", "exact_match"}
    ]
    bound_system_ids = sorted(
        {int(row["system_id"]) for row in accepted if row.get("system_id") is not None}
    )
    if bound_system_ids:
        status = "exact_match"
        reason = "accepted_identifier_binding"
    else:
        status = "exact_no_match"
        reason = records[0].get("reason") or records[0].get("outcome")
    return {
        "namespace": namespace.lower(),
        "identifier_norm": identifier_norm.lower(),
        "match_status": status,
        "reason": reason,
        "bound_system_ids": bound_system_ids,
        "outcomes": records,
    }


def _escape_fts(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _trigrams(value: str) -> list[str]:
    return sorted({value[index : index + 3] for index in range(max(0, len(value) - 2))})


def _bounded_levenshtein(left: str, right: str, maximum: int) -> int:
    if abs(len(left) - len(right)) > maximum:
        return maximum + 1
    prior = list(range(len(right) + 1))
    for row_index, left_char in enumerate(left, start=1):
        current = [row_index]
        row_minimum = row_index
        for column_index, right_char in enumerate(right, start=1):
            value = min(
                current[-1] + 1,
                prior[column_index] + 1,
                prior[column_index - 1] + (left_char != right_char),
            )
            current.append(value)
            row_minimum = min(row_minimum, value)
        if row_minimum > maximum:
            return maximum + 1
        prior = current
    return prior[-1]


def _candidate_terms(
    con: sqlite3.Connection,
    q_norm: str,
    *,
    max_terms: int = DEFAULT_CANDIDATE_TERMS,
) -> tuple[list[dict[str, Any]], str]:
    exact = [
        dict(row)
        for row in con.execute(
            """
            SELECT * FROM search_terms
            WHERE term_norm=?
            ORDER BY term_priority,is_primary DESC,search_term_id
            LIMIT ?
            """,
            [q_norm, max_terms],
        )
    ]
    if exact:
        return exact, "exact"
    if len(q_norm) < 2:
        return [], "exact_no_match"
    prefix = [
        dict(row)
        for row in con.execute(
            """
            SELECT * FROM search_terms
            WHERE term_norm >= ? AND term_norm < ?
            ORDER BY term_norm,term_priority,is_primary DESC,search_term_id
            LIMIT ?
            """,
            [q_norm, q_norm + "\uffff", max_terms],
        )
    ]
    if prefix:
        return prefix, "prefix"
    if len(q_norm) >= 3:
        substring = [
            dict(row)
            for row in con.execute(
                """
                SELECT st.*
                FROM search_terms_fts f
                JOIN search_terms st ON st.search_term_id=f.rowid
                WHERE search_terms_fts MATCH ?
                ORDER BY st.term_priority,st.is_primary DESC,st.search_term_id
                LIMIT ?
                """,
                [_escape_fts(q_norm), max_terms],
            )
        ]
        if substring:
            return substring, "substring"
    if len(q_norm) < 4:
        return [], "exact_no_match"
    maximum = 1 if len(q_norm) <= 5 else 2 if len(q_norm) <= 10 else 3
    trigrams = _trigrams(q_norm)
    if not trigrams:
        return [], "exact_no_match"
    fts_query = " OR ".join(_escape_fts(value) for value in trigrams)
    possible = [
        dict(row)
        for row in con.execute(
            """
            SELECT st.*
            FROM search_terms_fts f
            JOIN search_terms st ON st.search_term_id=f.rowid
            WHERE search_terms_fts MATCH ?
              AND abs(length(st.term_norm) - length(?)) <= ?
            ORDER BY st.search_term_id
            LIMIT ?
            """,
            [fts_query, q_norm, maximum, max_terms],
        )
    ]
    fuzzy: list[dict[str, Any]] = []
    for row in possible:
        distance = _bounded_levenshtein(q_norm, str(row["term_norm"]), maximum)
        if distance <= maximum:
            row["edit_distance"] = distance
            fuzzy.append(row)
    fuzzy.sort(
        key=lambda row: (
            int(row.get("edit_distance") or 0),
            int(row.get("term_priority") or 0),
            -int(row.get("is_primary") or 0),
            int(row["search_term_id"]),
        )
    )
    return fuzzy[:max_terms], "fuzzy" if fuzzy else "exact_no_match"


def _filter_clause(
    *,
    max_dist_ly: float | None,
    min_dist_ly: float | None,
    origin: tuple[float, float, float] | None,
    min_star_count: int | None,
    max_star_count: int | None,
    min_planet_count: int | None,
    max_planet_count: int | None,
    min_temp_k: float | None,
    max_temp_k: float | None,
    spectral_mask: int,
    has_planets: bool | None,
    has_habitable: bool | None,
    planet_category_mask: int,
    min_coolness_score: float | None,
    max_coolness_score: float | None,
) -> tuple[list[str], list[Any], str]:
    terms: list[str] = []
    params: list[Any] = []
    if origin:
        x, y, z = origin
        distance_expr = (
            "sqrt((coalesce(x_helio_ly,0)-?)*(coalesce(x_helio_ly,0)-?)"
            "+(coalesce(y_helio_ly,0)-?)*(coalesce(y_helio_ly,0)-?)"
            "+(coalesce(z_helio_ly,0)-?)*(coalesce(z_helio_ly,0)-?))"
        )
        distance_params = [x, x, y, y, z, z]
    else:
        distance_expr = "dist_ly"
        distance_params = []
    if max_dist_ly is not None:
        terms.append(f"{distance_expr} <= ?")
        params.extend(distance_params)
        params.append(max_dist_ly)
    if min_dist_ly is not None:
        terms.append(f"{distance_expr} >= ?")
        params.extend(distance_params)
        params.append(min_dist_ly)
    for field, minimum, maximum in [
        ("star_count", min_star_count, max_star_count),
        ("planet_count", min_planet_count, max_planet_count),
    ]:
        if minimum is not None:
            terms.append(f"{field} >= ?")
            params.append(minimum)
        if maximum is not None:
            terms.append(f"{field} <= ?")
            params.append(maximum)
    if min_temp_k is not None:
        terms.extend(["star_teff_count > 0", "max_star_teff_k >= ?"])
        params.append(min_temp_k)
    if max_temp_k is not None:
        terms.extend(["star_teff_count > 0", "min_star_teff_k <= ?"])
        params.append(max_temp_k)
    if spectral_mask:
        terms.append("(spectral_class_mask & ?) <> 0")
        params.append(spectral_mask)
    if has_planets is not None:
        terms.append("planet_count > 0" if has_planets else "planet_count = 0")
    if has_habitable is not None:
        terms.append(
            "has_habitable_candidate = 1" if has_habitable else "has_habitable_candidate = 0"
        )
    if planet_category_mask:
        terms.append("(planet_category_mask & ?) <> 0")
        params.append(planet_category_mask)
    if min_coolness_score is not None:
        terms.append("coolness_score >= ?")
        params.append(min_coolness_score)
    if max_coolness_score is not None:
        terms.append("coolness_score <= ?")
        params.append(max_coolness_score)
    return terms, params, distance_expr


def search_systems(
    con: sqlite3.Connection,
    *,
    q_norm: str | None,
    system_id_exact: int | None,
    identifier_namespace: str | None,
    identifier_norm: str | None,
    max_dist_ly: float | None,
    min_dist_ly: float | None,
    origin: tuple[float, float, float] | None,
    min_star_count: int | None,
    max_star_count: int | None,
    min_planet_count: int | None,
    max_planet_count: int | None,
    min_temp_k: float | None,
    max_temp_k: float | None,
    spectral_mask: int,
    has_planets: bool | None,
    has_habitable: bool | None,
    planet_category_mask: int,
    min_coolness_score: float | None,
    max_coolness_score: float | None,
    sort: str,
    limit: int,
    include_total: bool,
    name_style: str,
    cursor_values: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int | None, dict[str, Any] | None]:
    _increment("search_requests")
    started = time.perf_counter()
    query_resolution = None
    candidate_terms: list[dict[str, Any]] = []
    match_kind = "none"
    candidate_system_ids: list[int] | None = None
    if system_id_exact is not None:
        candidate_system_ids = [int(system_id_exact)]
        match_kind = "exact"
    elif identifier_namespace and identifier_norm:
        query_resolution = identifier_resolution(
            con, identifier_namespace, identifier_norm
        )
        if query_resolution is None:
            query_resolution = {
                "namespace": identifier_namespace.lower(),
                "identifier_norm": identifier_norm.lower(),
                "match_status": "exact_no_match",
                "reason": "identifier_not_in_registered_evidence",
                "bound_system_ids": [],
                "outcomes": [],
            }
        candidate_system_ids = [
            int(value) for value in query_resolution.get("bound_system_ids") or []
        ]
        if not candidate_system_ids:
            return [], 0 if include_total else None, query_resolution
    if candidate_system_ids is None and q_norm:
        candidate_terms, match_kind = _candidate_terms(con, q_norm)
        best_by_system: dict[int, dict[str, Any]] = {}
        for term in candidate_terms:
            system_id = int(term["system_id"])
            if system_id not in best_by_system:
                best_by_system[system_id] = term
            if len(best_by_system) >= DEFAULT_CANDIDATE_SYSTEMS:
                break
        candidate_system_ids = list(best_by_system)
    else:
        best_by_system = {}

    filter_terms, params, distance_expr = _filter_clause(
        max_dist_ly=max_dist_ly,
        min_dist_ly=min_dist_ly,
        origin=origin,
        min_star_count=min_star_count,
        max_star_count=max_star_count,
        min_planet_count=min_planet_count,
        max_planet_count=max_planet_count,
        min_temp_k=min_temp_k,
        max_temp_k=max_temp_k,
        spectral_mask=spectral_mask,
        has_planets=has_planets,
        has_habitable=has_habitable,
        planet_category_mask=planet_category_mask,
        min_coolness_score=min_coolness_score,
        max_coolness_score=max_coolness_score,
    )
    if candidate_system_ids is not None:
        if not candidate_system_ids:
            return [], 0 if include_total else None, query_resolution
        filter_terms.append(
            "system_id IN (" + ",".join("?" for _ in candidate_system_ids) + ")"
        )
        params.extend(candidate_system_ids)
    where = "WHERE " + " AND ".join(filter_terms) if filter_terms else ""
    total_count = None
    if include_total:
        total_count = int(
            con.execute(f"SELECT COUNT(*) FROM systems {where}", params).fetchone()[0]
        )
    order = {
        "distance": "origin_distance_ly ASC, system_id ASC" if origin else "dist_ly ASC, system_id ASC",
        "coolness": "coalesce(coolness_rank,9223372036854775807),system_name_norm,system_id",
        "planet_count": "planet_count DESC,system_name_norm,system_id",
        "star_count": "star_count DESC,system_name_norm,system_id",
        "hottest": "max_star_teff_k DESC NULLS LAST,system_name_norm,system_id",
        "coolest": "min_star_teff_k ASC NULLS LAST,system_name_norm,system_id",
        "name": "system_name_norm,system_id",
    }.get(sort, "system_name_norm,system_id")
    sql_limit = (
        DEFAULT_CANDIDATE_SYSTEMS
        if candidate_system_ids is not None
        else limit
    )
    query_params = list(params)
    select_distance = "NULL AS origin_distance_ly"
    if origin:
        x, y, z = origin
        select_distance = (
            "sqrt((coalesce(x_helio_ly,0)-?)*(coalesce(x_helio_ly,0)-?)"
            "+(coalesce(y_helio_ly,0)-?)*(coalesce(y_helio_ly,0)-?)"
            "+(coalesce(z_helio_ly,0)-?)*(coalesce(z_helio_ly,0)-?))"
            " AS origin_distance_ly"
        )
        query_params = [x, x, y, y, z, z] + query_params
    cursor_clause = ""
    cursor_params: list[Any] = []
    if cursor_values and not (q_norm and sort == "match"):
        if sort == "distance":
            field = "origin_distance_ly" if origin else "dist_ly"
            cursor_clause = (
                f"WHERE (coalesce({field},1e12) > ? OR "
                f"(coalesce({field},1e12) = ? AND system_id > ?))"
            )
            cursor_params = [
                cursor_values.get("dist", 1e12),
                cursor_values.get("dist", 1e12),
                cursor_values.get("id"),
            ]
        elif sort == "coolness":
            cursor_clause = (
                "WHERE (coalesce(coolness_rank,9223372036854775807) > ? OR "
                "(coalesce(coolness_rank,9223372036854775807) = ? "
                "AND system_name_norm > ?) OR "
                "(coalesce(coolness_rank,9223372036854775807) = ? "
                "AND system_name_norm = ? AND system_id > ?))"
            )
            rank = cursor_values.get("cool_rank", 9223372036854775807)
            name = cursor_values.get("name", "")
            cursor_params = [rank, rank, name, rank, name, cursor_values.get("id")]
        elif sort in {"planet_count", "star_count"}:
            field = sort
            count = cursor_values.get("count", 0)
            name = cursor_values.get("name", "")
            cursor_clause = (
                f"WHERE ({field} < ? OR ({field} = ? AND system_name_norm > ?) OR "
                f"({field} = ? AND system_name_norm = ? AND system_id > ?))"
            )
            cursor_params = [count, count, name, count, name, cursor_values.get("id")]
        elif sort in {"hottest", "coolest"}:
            field = "max_star_teff_k" if sort == "hottest" else "min_star_teff_k"
            sentinel = -1e18 if sort == "hottest" else 1e18
            comparison = "<" if sort == "hottest" else ">"
            temperature = cursor_values.get("temp", sentinel)
            name = cursor_values.get("name", "")
            cursor_clause = (
                f"WHERE (coalesce({field},{sentinel}) {comparison} ? OR "
                f"(coalesce({field},{sentinel}) = ? AND system_name_norm > ?) OR "
                f"(coalesce({field},{sentinel}) = ? AND system_name_norm = ? "
                "AND system_id > ?))"
            )
            cursor_params = [
                temperature,
                temperature,
                name,
                temperature,
                name,
                cursor_values.get("id"),
            ]
        else:
            name = cursor_values.get("name", "")
            cursor_clause = (
                "WHERE (system_name_norm > ? OR "
                "(system_name_norm = ? AND system_id > ?))"
            )
            cursor_params = [name, name, cursor_values.get("id")]
    fetched = [
        row
        for row in con.execute(
            f"""
            SELECT *
            FROM (
              SELECT systems.*,{select_distance}
              FROM systems
              {where}
            )
            {cursor_clause}
            ORDER BY {order}
            LIMIT ?
            """,
            query_params + cursor_params + [sql_limit],
        )
    ]
    if q_norm and sort == "match":
        rank = {"exact": 0, "prefix": 1, "substring": 2, "fuzzy": 3}
        fetched.sort(
            key=lambda row: (
                rank.get(match_kind, 9),
                int((best_by_system.get(int(row["system_id"])) or {}).get("edit_distance") or 0),
                float(row["dist_ly"]) if row["dist_ly"] is not None else 1e12,
                row["system_name_norm"],
                int(row["system_id"]),
            )
        )
        if cursor_values:
            cursor_key = (
                int(cursor_values.get("match_rank") or 0),
                float(cursor_values.get("dist") or 1e12),
                str(cursor_values.get("name") or ""),
                int(cursor_values.get("id") or 0),
            )
            match_rank = {"exact": 0, "prefix": 1, "substring": 2, "fuzzy": 3}.get(
                match_kind, 9
            )
            fetched = [
                row
                for row in fetched
                if (
                    match_rank,
                    float(row["dist_ly"]) if row["dist_ly"] is not None else 1e12,
                    str(row["system_name_norm"] or ""),
                    int(row["system_id"]),
                )
                > cursor_key
            ]
    selected_rows = fetched[:limit]
    items = [_system_payload(row) for row in selected_rows]
    system_ids = [int(item["system_id"]) for item in items]
    aliases = aliases_for_systems(con, system_ids)
    stellar_badges = stellar_badges_for_systems(con, system_ids)
    planet_badges = planet_badges_for_systems(con, system_ids)
    for item in items:
        system_id = int(item["system_id"])
        display = choose_display_name_info(
            item.get("system_name"),
            aliases.get(system_id, []),
            preferred_query_norm=q_norm,
            root_system=True,
            name_style=name_style,
        )
        item.update(display)
        badges = stellar_badges.get(system_id, [])
        item["stellar_object_badges"] = badges
        item["stellar_class_badges"] = [
            row.get("classification_value") or "UNKNOWN" for row in badges
        ]
        item["planet_object_badges"] = planet_badges.get(system_id, [])
        matched = best_by_system.get(system_id)
        item["match_rank"] = {"exact": 0, "prefix": 1, "substring": 2, "fuzzy": 3}.get(
            match_kind, 0
        )
        if matched:
            item.update(
                {
                    "matched_alias": matched.get("term_raw"),
                    "matched_term_norm": matched.get("term_norm"),
                    "matched_term_kind": matched.get("term_kind"),
                    "matched_term_priority": matched.get("term_priority"),
                    "matched_is_primary": bool(matched.get("is_primary")),
                    "matched_target_type": matched.get("target_type"),
                    "matched_target_id": matched.get("target_id"),
                    "matched_star_id": matched.get("star_id"),
                    "matched_planet_id": (
                        matched.get("target_id")
                        if matched.get("target_type") == "planet"
                        else None
                    ),
                    "resolved_system_id": system_id,
                    "match_resolution": match_kind,
                }
            )
        item["query_backend"] = "public_read_v2"
    _ = time.perf_counter() - started
    return items, total_count, query_resolution
