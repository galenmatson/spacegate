#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from smart_tag_registry import LoadedRegistry, canonical_json, load_registry

API_ROOT = Path(__file__).resolve().parents[1] / "srv" / "api"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))
from app.planet_categories import planet_category_bit_sql  # noqa: E402


SCHEMA_VERSION = "spacegate.smart_tags.v4"
MANIFEST_SCHEMA = "spacegate.smart_tags_manifest.v3"
ASSIGNMENT_SCHEMA = "spacegate.smart_tag_assignments.v2"
SOURCE_SUMMARY_SCHEMA = "spacegate.smart_tag_source_summary.v3"
SOURCE_CONTRIBUTION_SCHEMA = "spacegate.smart_tag_source_contributions.v1"
COMPILER_VERSION = "spacegate.smart_tags_compiler.v2.7"
HOT_ARTIFACT_MAX_BYTES = 1536 * 1024**2
PLANET_CATEGORY_BIT_TO_TAG = {
    1: "science:planet.hot_gas_giant",
    2: "science:planet.temperate_gas_giant",
    4: "science:planet.cold_gas_giant",
    8: "science:planet.hot_terrestrial",
    16: "science:planet.temperate_terrestrial",
    32: "science:planet.cold_terrestrial",
    64: "science:planet.hot_neptunian",
    128: "science:planet.temperate_neptunian",
    256: "science:planet.cold_neptunian",
}
EVIDENCE_STATUS_BITS = {
    "source": 1,
    "accepted": 1,
    "derived": 2,
    "assumed": 4,
    "screen": 8,
    "candidate": 16,
    "ambiguous": 32,
    "quarantined": 64,
    "missing": 128,
    "source_model": 256,
}
CLAIM_MODE_CODES = {
    "observed": 1,
    "accepted": 2,
    "derived": 3,
    "modeled": 4,
    "likely": 5,
    "candidate": 6,
    "disputed": 7,
    "contextual": 8,
}
HERO_FAMILY_CODES = {
    "architecture": 1,
    "exceptional_science": 2,
    "planet_environment": 3,
}
HERO_SIGNAL_BITS = {
    "rare": 1,
    "direct": 2,
    "concept": 4,
    "specific": 8,
    "modeled": 16,
    "member_focus": 32,
}
SUBJECT_SCOPE_CODES = {
    "star": 1,
    "planet": 2,
}
EVIDENCE_STATUS_CODES = {
    "source": 1,
    "accepted": 2,
    "derived": 3,
    "assumed": 4,
    "screen": 5,
    "candidate": 6,
    "ambiguous": 7,
    "quarantined": 8,
    "missing": 9,
    "source_model": 10,
}
BASIS_KIND_CODES = {
    "selected_public_fact": 1,
    "versioned_derivation": 2,
    "accepted_hierarchy_projection": 3,
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def input_fingerprints(paths: Iterable[Path], repo_root: Path) -> dict[str, Any]:
    return {
        str(path.relative_to(repo_root)): {
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in paths
    }


def assert_inputs_unchanged(
    expected: dict[str, Any], paths: Iterable[Path], repo_root: Path
) -> None:
    current = input_fingerprints(paths, repo_root)
    if current != expected:
        changed = sorted(set(expected) | set(current))
        changed = [key for key in changed if expected.get(key) != current.get(key)]
        raise ValueError(
            "smart-tag compiler inputs changed during compilation: "
            + ", ".join(changed)
        )


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def public_build_id(path: Path) -> str:
    con = sqlite3.connect(f"file:{path.resolve()}?mode=ro&immutable=1", uri=True)
    try:
        row = con.execute("SELECT value FROM metadata WHERE key='build_id'").fetchone()
        if row is None:
            raise ValueError("public-read artifact has no build_id")
        return str(row[0])
    finally:
        con.close()


def create_work_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        PRAGMA temp_store=FILE;
        CREATE TABLE metadata(
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE tag_definitions(
          tag_key TEXT PRIMARY KEY,
          label TEXT NOT NULL,
          name TEXT NOT NULL,
          category TEXT NOT NULL,
          kind TEXT NOT NULL,
          layer TEXT NOT NULL,
          target_types_json TEXT NOT NULL,
          visual_token TEXT NOT NULL,
          compact_priority INTEGER NOT NULL,
          normal_priority INTEGER NOT NULL,
          expanded_priority INTEGER NOT NULL,
          concept_slug TEXT,
          tooltip TEXT NOT NULL,
          short_tooltip TEXT NOT NULL,
          source_policy TEXT NOT NULL,
          evaluator_id TEXT NOT NULL,
          evaluator_version INTEGER NOT NULL,
          evaluator_params_json TEXT NOT NULL,
          filterable INTEGER NOT NULL,
          rollup TEXT NOT NULL,
          application_profile TEXT NOT NULL,
          application_json TEXT NOT NULL,
          hero_profile TEXT NOT NULL,
          hero_json TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE tag_assignments(
          target_type TEXT NOT NULL,
          stable_object_key TEXT NOT NULL,
          system_id INTEGER NOT NULL,
          tag_key TEXT NOT NULL,
          basis_kind TEXT NOT NULL,
          basis_ref TEXT,
          evidence_status TEXT NOT NULL,
          confidence REAL,
          evaluator_id TEXT NOT NULL,
          evaluator_version INTEGER NOT NULL
        );
        CREATE TABLE system_tag_membership(
          system_id INTEGER NOT NULL,
          tag_key TEXT NOT NULL,
          member_count INTEGER NOT NULL,
          primary_target_type TEXT NOT NULL,
          primary_target_key TEXT NOT NULL,
          basis_kind TEXT NOT NULL,
          evidence_status_mask INTEGER NOT NULL,
          min_confidence REAL,
          max_confidence REAL,
          PRIMARY KEY(system_id,tag_key)
        ) WITHOUT ROWID;
        CREATE TABLE system_hero_tags(
          system_id INTEGER NOT NULL,
          tag_key TEXT NOT NULL,
          hero_rank INTEGER NOT NULL,
          hero_score INTEGER NOT NULL,
          hero_family TEXT NOT NULL,
          signal_mask INTEGER NOT NULL,
          origin_target_type TEXT NOT NULL,
          origin_target_key TEXT NOT NULL,
          claim_mode TEXT NOT NULL,
          PRIMARY KEY(system_id,hero_rank),
          UNIQUE(system_id,tag_key)
        ) WITHOUT ROWID;
        CREATE TABLE source_definitions(
          source_key TEXT PRIMARY KEY,
          source_id TEXT NOT NULL,
          release_id TEXT,
          publisher TEXT,
          citation_url TEXT,
          license_name TEXT,
          license_url TEXT,
          authority_roles_json TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE system_sources(
          system_id INTEGER NOT NULL,
          source_key TEXT NOT NULL,
          contribution_kind TEXT NOT NULL,
          member_count INTEGER NOT NULL,
          PRIMARY KEY(system_id,source_key,contribution_kind)
        ) WITHOUT ROWID;
        CREATE TABLE source_contributions(
          system_id INTEGER NOT NULL,
          source_key TEXT NOT NULL,
          contribution_kind TEXT NOT NULL,
          target_key TEXT NOT NULL
        );
        CREATE TABLE quarantine(
          target_type TEXT,
          stable_object_key TEXT,
          evaluator_id TEXT,
          reason_code TEXT NOT NULL,
          detail_json TEXT NOT NULL
        );
        """
    )


def create_hot_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        PRAGMA temp_store=FILE;
        CREATE TABLE metadata(
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE tag_definitions(
          tag_id INTEGER PRIMARY KEY,
          tag_key TEXT NOT NULL UNIQUE,
          label TEXT NOT NULL,
          name TEXT NOT NULL,
          category TEXT NOT NULL,
          kind TEXT NOT NULL,
          layer TEXT NOT NULL,
          target_types_json TEXT NOT NULL,
          visual_token TEXT NOT NULL,
          compact_priority INTEGER NOT NULL,
          normal_priority INTEGER NOT NULL,
          expanded_priority INTEGER NOT NULL,
          concept_slug TEXT,
          tooltip TEXT NOT NULL,
          short_tooltip TEXT NOT NULL,
          source_policy TEXT NOT NULL,
          evaluator_id TEXT NOT NULL,
          evaluator_version INTEGER NOT NULL,
          evaluator_params_json TEXT NOT NULL,
          filterable INTEGER NOT NULL,
          rollup TEXT NOT NULL,
          application_profile TEXT NOT NULL,
          application_json TEXT NOT NULL,
          hero_profile TEXT NOT NULL,
          hero_json TEXT NOT NULL
        );
        CREATE TABLE system_tag_membership(
          system_id INTEGER NOT NULL,
          tag_id INTEGER NOT NULL,
          member_count INTEGER NOT NULL,
          scope_code INTEGER NOT NULL,
          basis_code INTEGER NOT NULL,
          evidence_status_mask INTEGER NOT NULL,
          min_confidence REAL,
          max_confidence REAL,
          PRIMARY KEY(system_id,tag_id)
        ) WITHOUT ROWID;
        CREATE TABLE system_hero_tags(
          system_id INTEGER NOT NULL,
          tag_id INTEGER NOT NULL,
          hero_rank INTEGER NOT NULL,
          hero_score INTEGER NOT NULL,
          hero_family_code INTEGER NOT NULL,
          signal_mask INTEGER NOT NULL,
          origin_scope_code INTEGER NOT NULL,
          origin_target_key TEXT NOT NULL,
          claim_mode_code INTEGER NOT NULL,
          PRIMARY KEY(system_id,hero_rank),
          UNIQUE(system_id,tag_id)
        ) WITHOUT ROWID;
        CREATE TABLE subject_tag_assignments(
          system_id INTEGER NOT NULL,
          scope_code INTEGER NOT NULL,
          target_object_id INTEGER NOT NULL,
          target_key TEXT NOT NULL,
          tag_id INTEGER NOT NULL,
          evidence_status_code INTEGER NOT NULL,
          confidence REAL,
          basis_code INTEGER NOT NULL,
          PRIMARY KEY(system_id,scope_code,target_object_id,target_key,tag_id)
        ) WITHOUT ROWID;
        CREATE TABLE source_definitions(
          source_num INTEGER PRIMARY KEY,
          source_key TEXT NOT NULL UNIQUE,
          source_id TEXT NOT NULL,
          release_id TEXT,
          public_name TEXT NOT NULL,
          short_name TEXT NOT NULL,
          publisher TEXT,
          description TEXT NOT NULL,
          mission_instrument TEXT,
          citation_url TEXT,
          license_name TEXT,
          license_url TEXT,
          authority_roles_json TEXT NOT NULL
        );
        CREATE TABLE system_sources(
          system_id INTEGER NOT NULL,
          source_num INTEGER NOT NULL,
          contribution_kind TEXT NOT NULL,
          member_count INTEGER NOT NULL,
          PRIMARY KEY(system_id,source_num,contribution_kind)
        ) WITHOUT ROWID;
        CREATE TABLE quarantine(
          target_type TEXT,
          stable_object_key TEXT,
          evaluator_id TEXT,
          reason_code TEXT NOT NULL,
          detail_json TEXT NOT NULL
        );
        CREATE INDEX idx_system_tag_membership_tag
          ON system_tag_membership(tag_id,system_id);
        CREATE INDEX idx_system_hero_tags_tag
          ON system_hero_tags(tag_id,system_id);
        CREATE INDEX idx_subject_tag_assignments_tag
          ON subject_tag_assignments(tag_id,system_id);
        CREATE INDEX idx_system_sources_source
          ON system_sources(source_num,system_id);
        """
    )


def insert_definitions(con: sqlite3.Connection, registry: LoadedRegistry) -> None:
    rows = []
    for definition in registry.definitions:
        priority = definition["priority"]
        evaluator = definition["evaluator"]
        rows.append(
            (
                definition["key"],
                definition["label"],
                definition["name"],
                definition["category"],
                definition["kind"],
                definition["layer"],
                json.dumps(definition["target_types"], separators=(",", ":")),
                definition["visual_token"],
                priority["compact"],
                priority["normal"],
                priority["expanded"],
                definition.get("concept_slug"),
                definition["tooltip"],
                definition["short_tooltip"],
                definition["source_policy"],
                evaluator["id"],
                evaluator["version"],
                json.dumps(evaluator["params"], sort_keys=True, separators=(",", ":")),
                int(bool(definition.get("filterable"))),
                definition["rollup"],
                definition["application_profile"],
                json.dumps(definition["application"], sort_keys=True, separators=(",", ":")),
                definition["hero_profile"],
                json.dumps(definition["hero"], sort_keys=True, separators=(",", ":")),
            )
        )
    con.executemany("INSERT INTO tag_definitions VALUES (" + ",".join("?" * 24) + ")", rows)


def insert_source_definitions(
    con: sqlite3.Connection, source_registry_path: Path
) -> dict[str, str]:
    payload = json.loads(source_registry_path.read_text(encoding="utf-8"))
    mapping: dict[str, str] = {}
    rows = []
    for source in payload.get("sources") or []:
        source_id = str(source["source_id"])
        source_key = "source:" + source_id.lower().replace("/", ".").replace(" ", "_")
        mapping[source_id.lower()] = source_key
        license_value = source.get("license") or {}
        rows.append(
            (
                source_key,
                source_id,
                source.get("release_id"),
                source.get("publisher"),
                source.get("citation_url"),
                license_value.get("name"),
                license_value.get("url"),
                json.dumps(
                    source.get("authority_roles") or {},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        )
    con.executemany("INSERT INTO source_definitions VALUES (?,?,?,?,?,?,?,?)", rows)
    return mapping


def load_source_presentation(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(row["source_id"]): row
        for row in payload.get("sources") or []
        if isinstance(row, dict) and row.get("source_id")
    }


def definition_by_evaluator(
    registry: LoadedRegistry, evaluator_id: str
) -> Iterable[dict[str, Any]]:
    return (
        definition
        for definition in registry.definitions
        if definition["evaluator"]["id"] == evaluator_id
    )


def _attach_public(con: sqlite3.Connection, public_read: Path) -> None:
    escaped = str(public_read.resolve()).replace("'", "''")
    con.execute(f"ATTACH DATABASE 'file:{escaped}?mode=ro&immutable=1' AS public")


def prepare_sample_scope(
    con: sqlite3.Connection, sample_limit: int | None
) -> int | None:
    if sample_limit is None:
        return None
    if sample_limit < 1:
        raise ValueError("sample_limit must be positive")
    con.execute(
        "CREATE TEMP TABLE compiler_system_scope(system_id INTEGER PRIMARY KEY)"
    )
    con.execute(
        """
        INSERT INTO compiler_system_scope
        SELECT system_id
        FROM public.systems
        ORDER BY system_id
        LIMIT ?
        """,
        (sample_limit,),
    )
    return int(
        con.execute("SELECT count(*) FROM compiler_system_scope").fetchone()[0]
    )


def _scope_join(alias: str, sample_limit: int | None) -> str:
    if sample_limit is None:
        return ""
    return (
        f" JOIN compiler_system_scope compiler_scope"
        f" ON compiler_scope.system_id={alias}.system_id"
    )


def insert_system_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    scope_join = _scope_join("s", sample_limit)
    for definition in definition_by_evaluator(registry, "system_count_v1"):
        params = definition["evaluator"]["params"]
        field = params["field"]
        if field not in {"star_count", "planet_count"}:
            raise ValueError(f"unsupported system_count_v1 field: {field}")
        predicates = []
        values: list[Any] = [definition["key"], definition["source_policy"]]
        if "equals" in params:
            predicates.append(f"s.{field} = ?")
            values.append(params["equals"])
        if "minimum" in params:
            predicates.append(f"s.{field} >= ?")
            values.append(params["minimum"])
        where = " AND ".join(predicates)
        con.execute(
            f"""
            INSERT INTO tag_assignments
            SELECT 'system',s.stable_object_key,s.system_id,?,
                   'selected_public_fact',?,'accepted',1.0,
                   'system_count_v1',1
            FROM public.systems s
            {scope_join}
            WHERE {where}
            """,
            values,
        )
    for definition in definition_by_evaluator(registry, "system_numeric_v1"):
        params = definition["evaluator"]["params"]
        if params.get("field") != "dist_ly":
            raise ValueError("system_numeric_v1 only supports dist_ly")
        con.execute(
            f"""
            INSERT INTO tag_assignments
            SELECT 'system',s.stable_object_key,s.system_id,?,
                   'selected_public_fact','dist_ly','accepted',1.0,
                   'system_numeric_v1',1
            FROM public.systems s
            {scope_join}
            WHERE s.dist_ly IS NOT NULL AND s.dist_ly <= ?
            """,
            (definition["key"], params["maximum"]),
        )
    for definition in definition_by_evaluator(registry, "system_range_v1"):
        params = definition["evaluator"]["params"]
        if params.get("field") != "dist_ly":
            raise ValueError("system_range_v1 only supports dist_ly")
        con.execute(
            f"""
            INSERT INTO tag_assignments
            SELECT 'system',s.stable_object_key,s.system_id,?,
                   'selected_public_fact','dist_ly','accepted',1.0,
                   'system_range_v1',1
            FROM public.systems s
            {scope_join}
            WHERE s.dist_ly > ? AND s.dist_ly <= ?
            """,
            (
                definition["key"],
                params["minimum_exclusive"],
                params["maximum"],
            ),
        )


def insert_star_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    rows = []
    for definition in definition_by_evaluator(registry, "stellar_class_v1"):
        for stellar_class in definition["evaluator"]["params"].get("classes") or []:
            rows.append((str(stellar_class).upper(), definition["key"]))
    con.execute(
        "CREATE TEMP TABLE class_tag_map("
        "classification TEXT,tag_key TEXT,"
        "PRIMARY KEY(classification,tag_key))"
    )
    con.executemany("INSERT INTO class_tag_map VALUES (?,?)", rows)
    scope_join = _scope_join("s", sample_limit)
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'star',s.stable_object_key,s.system_id,m.tag_key,
               CASE WHEN s.classification_status='assumed'
                    THEN 'versioned_derivation' ELSE 'selected_public_fact' END,
               s.classification_fact_id,
               coalesce(s.classification_status,'missing'),
               s.classification_confidence,
               'stellar_class_v1',1
        FROM public.stars s
        {scope_join}
        JOIN class_tag_map m
          ON m.classification=upper(trim(s.selected_classification))
        WHERE NOT EXISTS (
          SELECT 1 FROM public.stellar_badge_overlays o
          WHERE o.system_id=s.system_id
        )
        """
    )
    overlay_scope_join = _scope_join("o", sample_limit)
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'star',o.hierarchy_node_key,o.system_id,m.tag_key,
               CASE WHEN o.classification_status='assumed'
                    THEN 'versioned_derivation' ELSE 'selected_public_fact' END,
               o.selected_fact_id,
               coalesce(o.classification_status,'missing'),
               NULL,
               'stellar_class_v1',1
        FROM public.stellar_badge_overlays o
        {overlay_scope_join}
        JOIN class_tag_map m
          ON m.classification=upper(trim(o.classification_value))
        """
    )


def insert_planet_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    allowed = {definition["key"] for definition in registry.definitions}
    con.execute("CREATE TEMP TABLE planet_tag_map(category_bit INTEGER PRIMARY KEY,tag_key TEXT)")
    con.executemany(
        "INSERT INTO planet_tag_map VALUES (?,?)",
        [
            (category_bit, tag)
            for category_bit, tag in PLANET_CATEGORY_BIT_TO_TAG.items()
            if tag in allowed
        ],
    )
    scope_join = _scope_join("p", sample_limit)
    category_bit = planet_category_bit_sql("p")
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'planet',p.stable_object_key,p.system_id,m.tag_key,
               'versioned_derivation','spacegate.planet_category.v2','derived',1.0,
               'planet_category_v2',2
        FROM public.planets p
        {scope_join}
        JOIN planet_tag_map m
          ON m.category_bit=({category_bit})
        WHERE lower(coalesce(p.planet_status,'confirmed')) IN
              ('confirmed','known','published')
        """
    )
    usp = next(
        definition
        for definition in definition_by_evaluator(registry, "planet_numeric_v1")
    )
    values: list[Any] = [usp["key"], usp["evaluator"]["params"]["less_than"]]
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'planet',p.stable_object_key,p.system_id,?,
               'selected_public_fact',
               json_extract(p.selected_fact_lineage_json,
                            '$.orbital_period_days.fact_id'),
               'accepted',1.0,'planet_numeric_v1',1
        FROM public.planets p
        {scope_join}
        WHERE p.orbital_period_days IS NOT NULL
          AND p.orbital_period_days < ?
          AND lower(coalesce(p.planet_status,'confirmed')) IN
              ('confirmed','known','published')
        """,
        values,
    )
    hz = next(
        definition
        for definition in definition_by_evaluator(
            registry, "habitable_zone_screen_v1"
        )
    )
    values = [hz["key"]]
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'planet',p.stable_object_key,p.system_id,?,
               'versioned_derivation',p.classifier_version,'screen',1.0,
               'habitable_zone_screen_v1',1
        FROM public.planets p
        {scope_join}
        WHERE p.insolation_class='temperate'
          AND lower(coalesce(p.planet_status,'confirmed')) IN
              ('confirmed','known','published')
        """,
        values,
    )


def _contains_nested_group(node: Any, depth: int = 0) -> bool:
    if not isinstance(node, dict):
        return False
    family = str(node.get("component_family") or "").lower()
    kind = str(node.get("node_kind") or "").lower()
    child_count = int(node.get("child_count") or len(node.get("children") or []))
    total_star_count = int(node.get("total_star_count") or 0)
    if depth > 0 and child_count > 0 and total_star_count >= 2:
        return True
    if depth > 0 and (family in {"group", "subsystem"} or "group" in kind):
        return True
    return any(_contains_nested_group(child, depth + 1) for child in node.get("children") or [])


def insert_hierarchy_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    definition = next(
        definition
        for definition in definition_by_evaluator(registry, "hierarchy_nested_v2")
    )
    sql = (
        "SELECT h.system_id,s.stable_object_key,h.payload_gzip,h.payload_sha256 "
        "FROM public.hierarchy_bundles h JOIN public.systems s USING(system_id) "
        f"{_scope_join('h', sample_limit)} "
        "WHERE s.star_count>=3"
    )
    assignments = []
    quarantine = []
    for row in con.execute(sql):
        try:
            payload = json.loads(gzip.decompress(row[2]))
            root = (payload.get("hierarchy") or {}).get("root")
            if _contains_nested_group(root):
                assignments.append(
                    (
                        "system",
                        row[1],
                        row[0],
                        definition["key"],
                        "accepted_hierarchy_projection",
                        row[3],
                        "accepted",
                        1.0,
                        "hierarchy_nested_v2",
                        2,
                    )
                )
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            quarantine.append(
                (
                    "system",
                    row[1],
                    "hierarchy_nested_v2",
                    "invalid_hierarchy_bundle",
                    json.dumps({"error": str(exc)}, sort_keys=True),
                )
            )
    con.executemany("INSERT INTO tag_assignments VALUES (?,?,?,?,?,?,?,?,?,?)", assignments)
    con.executemany("INSERT INTO quarantine VALUES (?,?,?,?,?)", quarantine)


def build_rollups(con: sqlite3.Connection) -> None:
    con.execute(
        """
        INSERT INTO system_tag_membership
        SELECT a.system_id,a.tag_key,count(*),
               min(a.target_type),min(a.stable_object_key),
               CASE WHEN min(a.target_type)='system'
                    THEN 'direct' ELSE 'member_rollup' END,
               sum(DISTINCT CASE lower(a.evidence_status)
                 WHEN 'source' THEN 1
                 WHEN 'accepted' THEN 1
                 WHEN 'derived' THEN 2
                 WHEN 'assumed' THEN 4
                 WHEN 'screen' THEN 8
                 WHEN 'candidate' THEN 16
                 WHEN 'ambiguous' THEN 32
                 WHEN 'quarantined' THEN 64
                 WHEN 'missing' THEN 128
                 WHEN 'source_model' THEN 256
                 ELSE 0 END),
               min(a.confidence),max(a.confidence)
        FROM tag_assignments a
        JOIN tag_definitions d USING(tag_key)
        WHERE d.rollup IN ('direct','member_to_system')
        GROUP BY a.system_id,a.tag_key
        """
    )


def claim_mode_for_assignment(status: str, configured: str) -> str:
    normalized = str(status or "").strip().lower()
    if configured != "evidence_bound":
        return configured
    if normalized in {"source", "accepted"}:
        return "accepted"
    if normalized == "derived":
        return "derived"
    if normalized in {"source_model", "screen"}:
        return "modeled"
    if normalized in {"assumed", "candidate"}:
        return "candidate"
    if normalized in {"ambiguous", "quarantined", "missing"}:
        return "disputed"
    return "disputed"


def build_hero_selections(
    con: sqlite3.Connection, registry: LoadedRegistry
) -> dict[str, Any]:
    """Compose a sparse, explainable hero projection without changing assignments."""
    definitions = {
        str(row["key"]): row
        for row in registry.definitions
        if bool((row.get("hero") or {}).get("eligible"))
    }
    if not definitions:
        return {"selected": 0, "systems": 0, "candidates": 0, "by_family": {}}

    scope_exists = con.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE type='table' AND name='compiler_system_scope'"
    ).fetchone()
    total_systems = int(
        con.execute(
            "SELECT count(*) FROM compiler_system_scope"
            if scope_exists
            else "SELECT count(*) FROM public.systems"
        ).fetchone()[0]
    )
    prevalence = {
        str(row[0]): int(row[1])
        for row in con.execute(
            """
            SELECT tag_key,count(*)
            FROM system_tag_membership
            WHERE tag_key IN ("""
            + ",".join("?" for _ in definitions)
            + ") GROUP BY tag_key",
            sorted(definitions),
        )
    }
    placeholders = ",".join("?" for _ in definitions)
    cursor = con.execute(
        f"""
        SELECT system_id,tag_key,target_type,stable_object_key,evidence_status,
               confidence
        FROM tag_assignments
        WHERE tag_key IN ({placeholders})
        ORDER BY system_id,tag_key,stable_object_key
        """,
        sorted(definitions),
    )

    claim_rank = {
        "observed": 0,
        "accepted": 1,
        "derived": 2,
        "likely": 3,
        "modeled": 4,
        "candidate": 5,
        "disputed": 6,
        "contextual": 7,
    }
    allowed = {"observed", "accepted", "derived", "modeled", "likely"}
    best: dict[tuple[int, str], dict[str, Any]] = {}
    for row in cursor:
        system_id = int(row[0])
        tag_key = str(row[1])
        definition = definitions[tag_key]
        claim_mode = claim_mode_for_assignment(
            str(row[4]), str(definition["application"]["claim_mode"])
        )
        if claim_mode not in allowed:
            continue
        confidence = float(row[5]) if row[5] is not None else 0.5
        candidate = {
            "system_id": system_id,
            "tag_key": tag_key,
            "target_type": str(row[2]),
            "target_key": str(row[3]),
            "claim_mode": claim_mode,
            "confidence": confidence,
        }
        key = (system_id, tag_key)
        current = best.get(key)
        candidate_order = (
            claim_rank[claim_mode],
            -confidence,
            candidate["target_type"],
            candidate["target_key"],
        )
        if current is None or candidate_order < current["order"]:
            candidate["order"] = candidate_order
            best[key] = candidate

    grouped: dict[int, list[dict[str, Any]]] = {}
    for candidate in best.values():
        definition = definitions[candidate["tag_key"]]
        hero = definition["hero"]
        fraction = max(
            1.0 / max(1, total_systems),
            prevalence.get(candidate["tag_key"], 0) / max(1, total_systems),
        )
        rarity = min(28, round(-math.log10(fraction) * float(hero["rarity_weight"])))
        direct = candidate["target_type"] == "system"
        score = (
            float(hero["base_interest"])
            + rarity
            + float(hero["specificity"]) * 2
            + (8 if direct else 3)
            + (4 if definition.get("concept_slug") else 0)
            - (8 if candidate["claim_mode"] == "modeled" else 0)
            - (2 if candidate["claim_mode"] == "derived" else 0)
        )
        signal_mask = 0
        if rarity >= 8:
            signal_mask |= HERO_SIGNAL_BITS["rare"]
        signal_mask |= HERO_SIGNAL_BITS["direct" if direct else "member_focus"]
        if definition.get("concept_slug"):
            signal_mask |= HERO_SIGNAL_BITS["concept"]
        if float(hero["specificity"]) >= 3:
            signal_mask |= HERO_SIGNAL_BITS["specific"]
        if candidate["claim_mode"] == "modeled":
            signal_mask |= HERO_SIGNAL_BITS["modeled"]
        candidate.update(
            {
                "score": int(round(score)),
                "family": str(hero["family"]),
                "exclusive_group": str(hero["exclusive_group"]),
                "signal_mask": signal_mask,
            }
        )
        grouped.setdefault(candidate["system_id"], []).append(candidate)

    family_limits = {"architecture": 1, "exceptional_science": 2, "planet_environment": 1}
    rows: list[tuple[Any, ...]] = []
    by_family: dict[str, int] = {}
    for system_id in sorted(grouped):
        candidates = sorted(
            grouped[system_id],
            key=lambda row: (-row["score"], row["tag_key"], row["target_key"]),
        )
        exclusive_seen: set[str] = set()
        family_counts: dict[str, int] = {}
        composed: list[dict[str, Any]] = []
        for candidate in candidates:
            family = candidate["family"]
            exclusive = candidate["exclusive_group"]
            if exclusive in exclusive_seen:
                continue
            if family_counts.get(family, 0) >= family_limits.get(family, 0):
                continue
            composed.append(candidate)
            exclusive_seen.add(exclusive)
            family_counts[family] = family_counts.get(family, 0) + 1
            if len(composed) == 4:
                break
        for rank, candidate in enumerate(composed, start=1):
            rows.append(
                (
                    system_id,
                    candidate["tag_key"],
                    rank,
                    candidate["score"],
                    candidate["family"],
                    candidate["signal_mask"],
                    candidate["target_type"],
                    candidate["target_key"],
                    candidate["claim_mode"],
                )
            )
            by_family[candidate["family"]] = by_family.get(candidate["family"], 0) + 1
    con.executemany(
        "INSERT INTO system_hero_tags VALUES (?,?,?,?,?,?,?,?,?)", rows
    )
    return {
        "selected": len(rows),
        "systems": len({row[0] for row in rows}),
        "candidates": len(best),
        "by_family": dict(sorted(by_family.items())),
        "family_limits": family_limits,
        "maximum_per_system": 4,
    }


def reject_duplicate_assignments(con: sqlite3.Connection) -> None:
    duplicate = con.execute(
        """
        SELECT target_type,stable_object_key,system_id,tag_key,count(*) AS n
        FROM tag_assignments
        GROUP BY target_type,stable_object_key,system_id,tag_key
        HAVING count(*)>1
        ORDER BY n DESC,target_type,stable_object_key,tag_key
        LIMIT 1
        """
    ).fetchone()
    if duplicate is not None:
        raise ValueError(
            "duplicate smart-tag assignment: "
            f"{tuple(duplicate)}"
        )


def reject_unknown_evidence_statuses(con: sqlite3.Connection) -> None:
    unknown = [
        str(row[0])
        for row in con.execute(
            "SELECT DISTINCT lower(evidence_status) FROM tag_assignments"
        )
        if str(row[0]) not in EVIDENCE_STATUS_BITS
    ]
    if unknown:
        raise ValueError(
            "unknown smart-tag evidence status: " + ", ".join(sorted(unknown))
        )


SOURCE_ID_ALIASES = {
    "athyg_crosswalk": "transitional.athyg",
    "gaia_dr3": "gaia.dr3.gaia_source",
    "msc": "multiplicity.msc",
    "mast_tic": "tess.identity_and_candidate_evidence",
    "nasa_exoplanet_archive": "nasa_exoplanet_archive.planetary_systems",
    "sb9": "multiplicity.sb9",
    "sbx": "multiplicity.sbx",
    "wds": "multiplicity.wds",
    "orb6": "multiplicity.orb6",
    "gaia_nss": "gaia.dr3.non_single_star",
    "gaia_nss_two_body": "gaia.dr3.non_single_star",
}


def _normalize_source_id(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return SOURCE_ID_ALIASES.get(normalized, normalized)


def _source_contribution_kind(
    path: tuple[str, ...], record: dict[str, Any] | None = None
) -> str:
    context = list(path)
    if record:
        context.extend(
            str(record.get(key) or "")
            for key in (
                "node_kind",
                "component_family",
                "object_type",
                "field_key",
                "kind",
            )
        )
    lowered = "/".join(context).lower()
    if "orbit" in lowered:
        return "orbit"
    if "classification" in lowered or "spectral" in lowered:
        return "classification"
    if "eclipsing" in lowered or "observation" in lowered:
        return "observation"
    if "planet" in lowered:
        return "planet"
    if "hierarchy" in lowered or "relation" in lowered:
        return "multiplicity"
    return "displayed_evidence"


def _walk_source_contributions(
    value: Any,
    known_source_ids: set[str],
    path: tuple[str, ...] = (),
) -> set[tuple[str, str, str]]:
    found: set[tuple[str, str, str]] = set()
    if isinstance(value, list):
        for index, item in enumerate(value):
            found.update(
                _walk_source_contributions(
                    item, known_source_ids, (*path, str(index))
                )
            )
        return found
    if not isinstance(value, dict):
        return found
    source_id = _normalize_source_id(value.get("source_catalog"))
    if source_id in known_source_ids:
        target_key = str(
            value.get("stable_object_key")
            or value.get("stable_component_key")
            or value.get("selected_fact_id")
            or value.get("evidence_id")
            or value.get("source_pk")
            or "/".join(path)
        )
        found.add(
            (
                source_id,
                _source_contribution_kind(path, value),
                target_key,
            )
        )
    provenance = value.get("provenance")
    if isinstance(provenance, dict):
        provenance_source = _normalize_source_id(
            provenance.get("source_catalog")
        )
        if provenance_source in known_source_ids:
            target_key = str(
                value.get("stable_object_key")
                or provenance.get("source_row_id")
                or provenance.get("source_pk")
                or "/".join(path)
            )
            found.add(
                (
                    provenance_source,
                    _source_contribution_kind(path, value),
                    target_key,
                )
            )
    for key, item in value.items():
        if key != "provenance":
            found.update(
                _walk_source_contributions(
                    item, known_source_ids, (*path, str(key))
                )
            )
    return found


def build_sources(
    con: sqlite3.Connection,
    sample_limit: int | None,
    source_mapping: dict[str, str],
) -> dict[str, int]:
    sql = (
        "SELECT h.system_id,h.payload_gzip "
        "FROM public.hierarchy_bundles h"
        f"{_scope_join('h', sample_limit)}"
    )
    accounting = {
        "bundles_checked": 0,
        "bundles_invalid": 0,
        "exact_contributions": 0,
    }
    contribution_rows: list[tuple[int, str, str, str]] = []
    for system_id, payload_gzip in con.execute(sql):
        accounting["bundles_checked"] += 1
        try:
            payload = json.loads(gzip.decompress(payload_gzip))
        except (OSError, json.JSONDecodeError, TypeError):
            accounting["bundles_invalid"] += 1
            continue
        contributions = _walk_source_contributions(
            payload, set(source_mapping)
        )
        for source_id, kind, target_key in contributions:
            contribution_rows.append(
                (
                    int(system_id),
                    source_mapping[source_id],
                    kind,
                    target_key,
                )
            )
        accounting["exact_contributions"] += len(contributions)
    con.executemany(
        "INSERT INTO source_contributions VALUES (?,?,?,?)",
        sorted(contribution_rows),
    )
    con.execute(
        """
        INSERT INTO system_sources
        SELECT system_id,source_key,contribution_kind,count(*)
        FROM source_contributions
        GROUP BY system_id,source_key,contribution_kind
        """
    )
    accounting["system_source_rows"] = int(
        con.execute("SELECT count(*) FROM system_sources").fetchone()[0]
    )
    return accounting


def create_indexes(con: sqlite3.Connection) -> None:
    # The work database is a disposable compiler spool. Persistent indexes live
    # only in the normalized hot projection.
    return None


def build_hot_database(
    work: sqlite3.Connection,
    database: Path,
    *,
    source_registry_path: Path,
    source_presentation_path: Path | None,
) -> dict[str, int]:
    hot = sqlite3.connect(database)
    try:
        create_hot_schema(hot)
        metadata = list(work.execute("SELECT key,value FROM metadata ORDER BY key"))
        hot.executemany("INSERT INTO metadata VALUES (?,?)", metadata)
        definitions = list(
            work.execute("SELECT * FROM tag_definitions ORDER BY tag_key")
        )
        tag_ids = {
            str(row[0]): index
            for index, row in enumerate(definitions, start=1)
        }
        hot.executemany(
            "INSERT INTO tag_definitions VALUES ("
            + ",".join("?" * 25)
            + ")",
            [
                (tag_ids[str(row[0])], *tuple(row))
                for row in definitions
            ],
        )
        membership_rows = []
        for row in work.execute(
            """
            SELECT system_id,tag_key,member_count,primary_target_type,basis_kind,
                   evidence_status_mask,min_confidence,max_confidence
            FROM system_tag_membership
            ORDER BY system_id,tag_key
            """
        ):
            membership_rows.append(
                (
                    int(row[0]),
                    tag_ids[str(row[1])],
                    int(row[2]),
                    0 if row[3] == "system" else 1,
                    0 if row[4] == "direct" else 1,
                    int(row[5]),
                    row[6],
                    row[7],
                )
            )
            if len(membership_rows) >= 100_000:
                hot.executemany(
                    "INSERT INTO system_tag_membership VALUES (?,?,?,?,?,?,?,?)",
                    membership_rows,
                )
                membership_rows.clear()
        if membership_rows:
            hot.executemany(
                "INSERT INTO system_tag_membership VALUES (?,?,?,?,?,?,?,?)",
                membership_rows,
            )

        hero_rows = []
        for row in work.execute(
            """
            SELECT system_id,tag_key,hero_rank,hero_score,hero_family,signal_mask,
                   origin_target_type,origin_target_key,claim_mode
            FROM system_hero_tags ORDER BY system_id,hero_rank
            """
        ):
            hero_rows.append(
                (
                    int(row[0]),
                    tag_ids[str(row[1])],
                    int(row[2]),
                    int(row[3]),
                    HERO_FAMILY_CODES[str(row[4])],
                    int(row[5]),
                    0 if row[6] == "system" else (1 if row[6] == "star" else 2),
                    str(row[7]),
                    CLAIM_MODE_CODES[str(row[8])],
                )
            )
        hot.executemany(
            "INSERT INTO system_hero_tags VALUES (?,?,?,?,?,?,?,?,?)",
            hero_rows,
        )

        subject_rows = []
        missing_subjects = 0
        for row in work.execute(
            """
            SELECT a.system_id,a.target_type,
                   CASE a.target_type
                     WHEN 'star' THEN coalesce(s.star_id,0)
                     WHEN 'planet' THEN p.planet_id
                   END AS target_object_id,
                   CASE
                     WHEN a.target_type='star' AND s.star_id IS NULL
                       THEN a.stable_object_key
                     ELSE ''
                   END AS target_key,
                   a.tag_key,a.evidence_status,a.confidence,a.basis_kind
            FROM tag_assignments a
            LEFT JOIN public.stars s
              ON a.target_type='star'
             AND s.system_id=a.system_id
             AND s.stable_object_key=a.stable_object_key
            LEFT JOIN public.planets p
              ON a.target_type='planet'
             AND p.system_id=a.system_id
             AND p.stable_object_key=a.stable_object_key
            WHERE a.target_type IN ('star','planet')
            ORDER BY a.system_id,a.target_type,target_object_id,a.tag_key
            """
        ):
            if row[2] is None or (int(row[2]) == 0 and not str(row[3])):
                missing_subjects += 1
                continue
            subject_rows.append(
                (
                    int(row[0]),
                    SUBJECT_SCOPE_CODES[str(row[1])],
                    int(row[2]),
                    str(row[3]),
                    tag_ids[str(row[4])],
                    EVIDENCE_STATUS_CODES[str(row[5]).lower()],
                    row[6],
                    BASIS_KIND_CODES[str(row[7])],
                )
            )
            if len(subject_rows) >= 100_000:
                hot.executemany(
                    "INSERT INTO subject_tag_assignments VALUES (?,?,?,?,?,?,?,?)",
                    subject_rows,
                )
                subject_rows.clear()
        if missing_subjects:
            raise ValueError(
                f"{missing_subjects} object tag assignments lack a Public Read subject"
            )
        if subject_rows:
            hot.executemany(
                "INSERT INTO subject_tag_assignments VALUES (?,?,?,?,?,?,?,?)",
                subject_rows,
            )

        source_payload = json.loads(
            source_registry_path.read_text(encoding="utf-8")
        )
        presentation = load_source_presentation(source_presentation_path)
        source_rows = []
        source_nums: dict[str, int] = {}
        for source_num, source in enumerate(
            sorted(
                source_payload.get("sources") or [],
                key=lambda row: str(row["source_id"]),
            ),
            start=1,
        ):
            source_id = str(source["source_id"])
            source_key = (
                "source:"
                + source_id.lower().replace("/", ".").replace(" ", "_")
            )
            source_nums[source_key] = source_num
            display = presentation.get(source_id, {})
            license_value = source.get("license") or {}
            public_name = str(
                display.get("public_name")
                or source.get("publisher")
                or source_id
            )
            short_name = str(display.get("short_name") or public_name)
            roles = source.get("authority_roles") or {}
            role_text = ", ".join(
                key.replace("_", " ") for key in sorted(roles)
            )
            description = str(
                display.get("description")
                or f"{public_name} contributes reviewed {role_text or 'catalog'} evidence."
            )
            source_rows.append(
                (
                    source_num,
                    source_key,
                    source_id,
                    source.get("release_id"),
                    public_name,
                    short_name,
                    source.get("publisher"),
                    description,
                    display.get("mission_instrument"),
                    source.get("citation_url"),
                    license_value.get("name"),
                    license_value.get("url"),
                    json.dumps(roles, sort_keys=True, separators=(",", ":")),
                )
            )
        hot.executemany(
            "INSERT INTO source_definitions VALUES ("
            + ",".join("?" * 13)
            + ")",
            source_rows,
        )
        source_rows_hot = []
        for row in work.execute(
            """
            SELECT system_id,source_key,contribution_kind,member_count
            FROM system_sources ORDER BY system_id,source_key,contribution_kind
            """
        ):
            source_num = source_nums.get(str(row[1]))
            if source_num is not None:
                source_rows_hot.append(
                    (int(row[0]), source_num, str(row[2]), int(row[3]))
                )
        hot.executemany(
            "INSERT INTO system_sources VALUES (?,?,?,?)",
            source_rows_hot,
        )
        hot.executemany(
            "INSERT INTO quarantine VALUES (?,?,?,?,?)",
            work.execute(
                """
                SELECT target_type,stable_object_key,evaluator_id,reason_code,
                       detail_json
                FROM quarantine
                ORDER BY target_type,stable_object_key,evaluator_id,reason_code
                """
            ),
        )
        hot.commit()
        hot.execute("VACUUM")
        hot.commit()
        return {
            "tag_definitions": len(definitions),
            "system_tag_membership": int(
                hot.execute(
                    "SELECT count(*) FROM system_tag_membership"
                ).fetchone()[0]
            ),
            "system_hero_tags": len(hero_rows),
            "subject_tag_assignments": int(
                hot.execute(
                    "SELECT count(*) FROM subject_tag_assignments"
                ).fetchone()[0]
            ),
            "source_definitions": len(source_rows),
            "system_sources": len(source_rows_hot),
            "quarantine": int(
                hot.execute("SELECT count(*) FROM quarantine").fetchone()[0]
            ),
        }
    finally:
        hot.close()


def logical_table_hash(con: sqlite3.Connection, table: str, columns: list[str]) -> str:
    digest = hashlib.sha256()
    order = ",".join(columns)
    for row in con.execute(f"SELECT {order} FROM {table} ORDER BY {order}"):
        digest.update(canonical_json(list(row)))
        digest.update(b"\n")
    return digest.hexdigest()


def export_assignments(con: sqlite3.Connection, output: Path) -> int:
    cursor = con.execute(
        """
        SELECT target_type,stable_object_key,system_id,tag_key,basis_kind,
               basis_ref,evidence_status,confidence,evaluator_id,evaluator_version
        FROM tag_assignments
        ORDER BY system_id,target_type,stable_object_key,tag_key
        """
    )
    schema = pa.schema(
        [
            ("target_type", pa.string()),
            ("stable_object_key", pa.string()),
            ("system_id", pa.int64()),
            ("tag_key", pa.string()),
            ("basis_kind", pa.string()),
            ("basis_ref", pa.string()),
            ("evidence_status", pa.string()),
            ("confidence", pa.float64()),
            ("evaluator_id", pa.string()),
            ("evaluator_version", pa.int32()),
        ]
    )
    writer = pq.ParquetWriter(output, schema, compression="zstd")
    count = 0
    try:
        while rows := cursor.fetchmany(100_000):
            columns = list(zip(*rows))
            table = pa.Table.from_arrays(
                [pa.array(column, type=field.type) for column, field in zip(columns, schema)],
                schema=schema,
            )
            writer.write_table(table)
            count += len(rows)
    finally:
        writer.close()
    return count


def export_source_contributions(
    con: sqlite3.Connection, output: Path
) -> int:
    cursor = con.execute(
        """
        SELECT system_id,source_key,contribution_kind,target_key
        FROM source_contributions
        ORDER BY system_id,source_key,contribution_kind,target_key
        """
    )
    schema = pa.schema(
        [
            ("system_id", pa.int64()),
            ("source_key", pa.string()),
            ("contribution_kind", pa.string()),
            ("target_key", pa.string()),
        ]
    )
    writer = pq.ParquetWriter(output, schema, compression="zstd")
    count = 0
    try:
        while rows := cursor.fetchmany(100_000):
            columns = list(zip(*rows))
            table = pa.Table.from_arrays(
                [
                    pa.array(column, type=field.type)
                    for column, field in zip(columns, schema)
                ],
                schema=schema,
            )
            writer.write_table(table)
            count += len(rows)
    finally:
        writer.close()
    return count


def compile_tags(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    registry = load_registry(args.registry)
    public_read = args.public_read.resolve(strict=True)
    build_id = public_build_id(public_read)
    output_root = args.output_root.resolve()
    final_dir = output_root / build_id / registry.registry_hash
    compiler_sources = (
        Path(__file__).resolve(),
        (args.repo_root / "scripts/smart_tag_registry.py").resolve(strict=True),
    )
    compiler_inputs = input_fingerprints(compiler_sources, args.repo_root)
    source_registry_path = (
        args.repo_root / registry.registry["source_registry"]
    ).resolve(strict=True)
    registry_sources = tuple(registry.source_files) + (source_registry_path,)
    registry_inputs = input_fingerprints(registry_sources, args.repo_root)
    if final_dir.exists() and not args.force:
        manifest = json.loads((final_dir / "manifest.json").read_text(encoding="utf-8"))
        if (
            manifest.get("status") == "pass"
            and manifest.get("compiler_version") == COMPILER_VERSION
            and (manifest.get("input_lineage") or {}).get("compiler_files")
            == compiler_inputs
        ):
            return manifest
        raise ValueError(
            f"stale or incomplete smart-tag output exists: {final_dir}; "
            "use --force only after retaining the previous artifact"
        )
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{build_id}.{registry.registry_hash[:12]}.",
            dir=output_root,
        )
    )
    timings: dict[str, float] = {}
    try:
        work_database = staging / "working.sqlite"
        database = staging / "smart_tags.sqlite"
        con = sqlite3.connect(work_database)
        con.row_factory = sqlite3.Row
        try:
            phase = time.perf_counter()
            create_work_schema(con)
            insert_definitions(con, registry)
            source_mapping = insert_source_definitions(con, source_registry_path)
            _attach_public(con, public_read)
            sampled_system_count = prepare_sample_scope(con, args.sample_limit)
            con.executemany(
                "INSERT INTO metadata VALUES (?,?)",
                [
                    ("schema_version", SCHEMA_VERSION),
                    ("assignment_schema_version", ASSIGNMENT_SCHEMA),
                    ("source_summary_schema_version", SOURCE_SUMMARY_SCHEMA),
                    (
                        "source_contribution_schema_version",
                        SOURCE_CONTRIBUTION_SCHEMA,
                    ),
                    ("build_id", build_id),
                    ("registry_id", registry.registry["registry_id"]),
                    ("registry_version", registry.registry["registry_version"]),
                    ("registry_hash", registry.registry_hash),
                    ("compiler_version", COMPILER_VERSION),
                    (
                        "claim_grammar_json",
                        json.dumps(
                            registry.application_policies["claim_grammar"],
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                    (
                        "application_policy_id",
                        registry.application_policies["policy_id"],
                    ),
                    (
                        "application_policy_version",
                        registry.application_policies["policy_version"],
                    ),
                    (
                        "sample_limit",
                        "" if args.sample_limit is None else str(args.sample_limit),
                    ),
                ],
            )
            con.commit()
            timings["schema_seconds"] = time.perf_counter() - phase

            for name, function in (
                ("systems", insert_system_assignments),
                ("stars", insert_star_assignments),
                ("planets", insert_planet_assignments),
                ("hierarchy", insert_hierarchy_assignments),
            ):
                phase = time.perf_counter()
                function(con, registry, args.sample_limit)
                con.commit()
                timings[f"{name}_seconds"] = time.perf_counter() - phase
            phase = time.perf_counter()
            reject_duplicate_assignments(con)
            reject_unknown_evidence_statuses(con)
            build_rollups(con)
            hero_accounting = build_hero_selections(con, registry)
            source_accounting = build_sources(
                con, args.sample_limit, source_mapping
            )
            create_indexes(con)
            con.commit()
            timings["rollup_and_indexes_seconds"] = time.perf_counter() - phase

            counts = {
                table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                for table in (
                    "tag_definitions",
                    "tag_assignments",
                    "system_tag_membership",
                    "system_hero_tags",
                    "source_definitions",
                    "system_sources",
                    "source_contributions",
                    "quarantine",
                )
            }
            counts["sampled_systems"] = (
                sampled_system_count
                if sampled_system_count is not None
                else int(
                    con.execute(
                        "SELECT count(*) FROM public.systems"
                    ).fetchone()[0]
                )
            )
            hashes = {
                "tag_definitions": logical_table_hash(
                    con,
                    "tag_definitions",
                    [
                        "tag_key",
                        "label",
                        "name",
                        "category",
                        "kind",
                        "layer",
                        "target_types_json",
                        "visual_token",
                        "compact_priority",
                        "normal_priority",
                        "expanded_priority",
                        "concept_slug",
                        "tooltip",
                        "short_tooltip",
                        "source_policy",
                        "evaluator_id",
                        "evaluator_version",
                        "evaluator_params_json",
                        "filterable",
                        "rollup",
                        "application_profile",
                        "application_json",
                        "hero_profile",
                        "hero_json",
                    ],
                ),
                "tag_assignments": logical_table_hash(
                    con,
                    "tag_assignments",
                    [
                        "target_type",
                        "stable_object_key",
                        "tag_key",
                        "system_id",
                        "basis_kind",
                        "basis_ref",
                        "evidence_status",
                        "confidence",
                        "evaluator_id",
                        "evaluator_version",
                    ],
                ),
                "system_tag_membership": logical_table_hash(
                    con,
                    "system_tag_membership",
                    [
                        "system_id",
                        "tag_key",
                        "member_count",
                        "primary_target_type",
                        "primary_target_key",
                        "basis_kind",
                        "evidence_status_mask",
                        "min_confidence",
                        "max_confidence",
                    ],
                ),
                "system_hero_tags": logical_table_hash(
                    con,
                    "system_hero_tags",
                    [
                        "system_id",
                        "tag_key",
                        "hero_rank",
                        "hero_score",
                        "hero_family",
                        "signal_mask",
                        "origin_target_type",
                        "origin_target_key",
                        "claim_mode",
                    ],
                ),
                "system_sources": logical_table_hash(
                    con,
                    "system_sources",
                    [
                        "system_id",
                        "source_key",
                        "contribution_kind",
                        "member_count",
                    ],
                ),
                "source_contributions": logical_table_hash(
                    con,
                    "source_contributions",
                    [
                        "system_id",
                        "source_key",
                        "contribution_kind",
                        "target_key",
                    ],
                ),
            }
            assignment_counts_by_tag = {
                row[0]: row[1]
                for row in con.execute(
                    "SELECT tag_key,count(*) FROM tag_assignments GROUP BY tag_key"
                )
            }
            integrity = con.execute("PRAGMA quick_check").fetchone()[0]
            if integrity != "ok":
                raise ValueError(f"smart-tag SQLite quick_check failed: {integrity}")
            phase = time.perf_counter()
            parquet_count = export_assignments(con, staging / "assignments.parquet")
            timings["parquet_seconds"] = time.perf_counter() - phase
            if parquet_count != counts["tag_assignments"]:
                raise ValueError("Parquet assignment count does not match SQLite")
            phase = time.perf_counter()
            source_contribution_count = export_source_contributions(
                con, staging / "source_contributions.parquet"
            )
            timings["source_contributions_parquet_seconds"] = time.perf_counter() - phase
            if source_contribution_count != counts["source_contributions"]:
                raise ValueError(
                    "Parquet source-contribution count does not match SQLite"
                )
            phase = time.perf_counter()
            hot_projection_counts = build_hot_database(
                con,
                database,
                source_registry_path=source_registry_path,
                source_presentation_path=(
                    args.repo_root
                    / registry.registry.get(
                        "source_presentation",
                        "config/tags/source_presentation.json",
                    )
                ),
            )
            timings["hot_projection_seconds"] = time.perf_counter() - phase
            con.execute("DETACH DATABASE public")
        finally:
            con.close()
        work_database.unlink(missing_ok=True)
        if database.stat().st_size > HOT_ARTIFACT_MAX_BYTES:
            raise ValueError(
                "smart-tag hot artifact exceeds 1.5 GiB budget: "
                f"{database.stat().st_size}"
            )

        atomic_json(staging / "registry.json", registry.snapshot())
        proposal_accounting = {
            "schema_version": "spacegate.smart_tag_proposal_accounting.v1",
            "status": "pass",
            "build_id": build_id,
            "registry_hash": registry.registry_hash,
            "compiler_version": COMPILER_VERSION,
            "proposal_inventory": registry.proposal_inventory,
            "legacy_token_inventory": registry.legacy_token_inventory,
        }
        atomic_json(
            staging / "proposal_accounting.json", proposal_accounting
        )
        feasibility_rows = []
        feasibility_policy = registry.application_policies["proposal_feasibility"]
        for proposal in registry.proposal_inventory["proposals"]:
            feasibility_rows.append(
                {
                    **proposal,
                    **feasibility_policy[str(proposal["family"])],
                }
            )
        atomic_json(
            staging / "proposal_feasibility.json",
            {
                "schema_version": "spacegate.smart_tag_proposal_feasibility.v1",
                "status": "pass",
                "build_id": build_id,
                "registry_hash": registry.registry_hash,
                "policy_id": registry.application_policies["policy_id"],
                "policy_version": registry.application_policies["policy_version"],
                "proposal_count": len(feasibility_rows),
                "proposals": feasibility_rows,
            },
        )
        atomic_json(
            staging / "hero_accounting.json",
            {
                "schema_version": "spacegate.smart_tag_hero_accounting.v1",
                "status": "pass",
                "build_id": build_id,
                "registry_hash": registry.registry_hash,
                "policy_id": registry.application_policies["policy_id"],
                "policy_version": registry.application_policies["policy_version"],
                **hero_accounting,
            },
        )
        atomic_json(
            staging / "source_accounting.json",
            {
                "schema_version": "spacegate.smart_tag_source_accounting.v1",
                "status": (
                    "pass"
                    if source_accounting["bundles_invalid"] == 0
                    else "review"
                ),
                "build_id": build_id,
                "registry_hash": registry.registry_hash,
                **source_accounting,
            },
        )
        coverage = {
            "schema_version": "spacegate.smart_tag_coverage.v1",
            "status": "pass",
            "build_id": build_id,
            "registry_hash": registry.registry_hash,
            "sample_limit": args.sample_limit,
            "counts": counts,
            "hot_projection_counts": hot_projection_counts,
            "assignment_counts_by_tag": assignment_counts_by_tag,
            "proposal_inventory": registry.proposal_inventory,
            "legacy_token_inventory": registry.legacy_token_inventory,
            "source_accounting": source_accounting,
            "hero_accounting": hero_accounting,
        }
        atomic_json(staging / "coverage.json", coverage)
        atomic_json(
            staging / "quarantine.json",
            {
                "schema_version": "spacegate.smart_tag_quarantine.v1",
                "status": "pass" if counts["quarantine"] == 0 else "review",
                "count": counts["quarantine"],
            },
        )
        timings["total_seconds"] = time.perf_counter() - started
        atomic_json(
            staging / "timings.json",
            {
                "schema_version": "spacegate.smart_tag_timings.v1",
                "status": "pass",
                "build_id": build_id,
                "registry_hash": registry.registry_hash,
                "timings": timings,
            },
        )
        report_artifacts = {}
        for name in (
            "registry.json",
            "coverage.json",
            "quarantine.json",
            "proposal_accounting.json",
            "proposal_feasibility.json",
            "hero_accounting.json",
            "source_accounting.json",
            "timings.json",
        ):
            path = staging / name
            report_artifacts[name.removesuffix(".json")] = {
                "path": name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        assert_inputs_unchanged(compiler_inputs, compiler_sources, args.repo_root)
        assert_inputs_unchanged(registry_inputs, registry_sources, args.repo_root)
        manifest = {
            "schema_version": MANIFEST_SCHEMA,
            "status": "pass",
            "build_id": build_id,
            "registry_id": registry.registry["registry_id"],
            "registry_version": registry.registry["registry_version"],
            "registry_hash": registry.registry_hash,
            "compiler_version": COMPILER_VERSION,
            "tag_schema_version": SCHEMA_VERSION,
            "assignment_schema_version": ASSIGNMENT_SCHEMA,
            "source_summary_schema_version": SOURCE_SUMMARY_SCHEMA,
            "source_contribution_schema_version": SOURCE_CONTRIBUTION_SCHEMA,
            "sample_limit": args.sample_limit,
            "public_read": {
                "path": str(public_read),
                "bytes": public_read.stat().st_size,
                "sha256": sha256_file(public_read) if args.hash_input else None,
            },
            "input_lineage": {
                "compiler_files": compiler_inputs,
                "registry_files": registry_inputs,
                "public_read_hash_recorded": bool(args.hash_input),
            },
            "artifacts": {
                "database": {
                    "path": "smart_tags.sqlite",
                    "bytes": database.stat().st_size,
                    "sha256": sha256_file(database),
                },
                "assignments": {
                    "path": "assignments.parquet",
                    "bytes": (staging / "assignments.parquet").stat().st_size,
                    "sha256": sha256_file(staging / "assignments.parquet"),
                },
                "source_contributions": {
                    "path": "source_contributions.parquet",
                    "bytes": (
                        staging / "source_contributions.parquet"
                    ).stat().st_size,
                    "sha256": sha256_file(
                        staging / "source_contributions.parquet"
                    ),
                },
                **report_artifacts,
            },
            "counts": counts,
            "logical_hashes": hashes,
            "timings": timings,
            "budgets": {
                "hot_artifact_max_bytes": HOT_ARTIFACT_MAX_BYTES,
                "hot_artifact_bytes": database.stat().st_size,
                "hot_artifact_status": "pass",
            },
            "source_accounting": source_accounting,
            "hero_accounting": hero_accounting,
        }
        atomic_json(staging / "manifest.json", manifest)
        if final_dir.exists():
            if args.force:
                shutil.rmtree(final_dir)
            else:
                raise ValueError(f"smart-tag output appeared concurrently: {final_dir}")
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final_dir)
        current = final_dir.parent / "current"
        temporary_link = final_dir.parent / f".current.{os.getpid()}"
        temporary_link.symlink_to(registry.registry_hash)
        os.replace(temporary_link, current)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Compile immutable Spacegate smart-tag artifacts."
    )
    parser.add_argument(
        "--registry", type=Path, default=root / "config/tags/registry.json"
    )
    parser.add_argument("--public-read", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(
            os.getenv("SPACEGATE_STATE_DIR", "/data/spacegate/state")
        )
        / "derived/smart_tags",
    )
    parser.add_argument("--sample-limit", type=int)
    parser.add_argument("--hash-input", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.set_defaults(repo_root=root)
    return parser.parse_args()


def main() -> int:
    result = compile_tags(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
