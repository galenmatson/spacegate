#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from smart_tag_registry import LoadedRegistry, canonical_json, load_registry


SCHEMA_VERSION = "spacegate.smart_tags.v1"
MANIFEST_SCHEMA = "spacegate.smart_tags_manifest.v1"
ASSIGNMENT_SCHEMA = "spacegate.smart_tag_assignments.v1"
SOURCE_SUMMARY_SCHEMA = "spacegate.smart_tag_source_summary.v1"
CLASS_TO_TAG = {
    "O": "science:stellar.o",
    "B": "science:stellar.b",
    "A": "science:stellar.a",
    "F": "science:stellar.f",
    "G": "science:stellar.g",
    "K": "science:stellar.k",
    "M": "science:stellar.m",
    "L": "science:stellar.l",
    "T": "science:stellar.t",
    "Y": "science:stellar.y",
    "WR": "science:stellar.wolf_rayet",
    "WD": "science:stellar.white_dwarf",
    "D": "science:stellar.white_dwarf",
    "NS": "science:stellar.neutron_star",
    "PULSAR": "science:stellar.pulsar",
    "MAGNETAR": "science:stellar.magnetar",
    "BH": "science:stellar.black_hole",
    "BLACK HOLE": "science:stellar.black_hole",
}
PLANET_CATEGORY_TO_TAG = {
    ("jupiter", "hot"): "science:planet.hot_gas_giant",
    ("jupiter", "temperate"): "science:planet.temperate_gas_giant",
    ("jupiter", "cold"): "science:planet.cold_gas_giant",
    ("terrestrial", "hot"): "science:planet.hot_terrestrial",
    ("terrestrial", "temperate"): "science:planet.temperate_terrestrial",
    ("terrestrial", "cold"): "science:planet.cold_terrestrial",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def create_schema(con: sqlite3.Connection) -> None:
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
          rollup TEXT NOT NULL
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
          evaluator_version INTEGER NOT NULL,
          PRIMARY KEY(target_type,stable_object_key,tag_key)
        ) WITHOUT ROWID;
        CREATE TABLE system_tag_membership(
          system_id INTEGER NOT NULL,
          tag_key TEXT NOT NULL,
          member_count INTEGER NOT NULL,
          primary_target_type TEXT NOT NULL,
          primary_target_key TEXT NOT NULL,
          basis_kind TEXT NOT NULL,
          PRIMARY KEY(system_id,tag_key)
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
        CREATE TABLE quarantine(
          target_type TEXT,
          stable_object_key TEXT,
          evaluator_id TEXT,
          reason_code TEXT NOT NULL,
          detail_json TEXT NOT NULL
        );
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
            )
        )
    con.executemany("INSERT INTO tag_definitions VALUES (" + ",".join("?" * 20) + ")", rows)


def insert_source_definitions(
    con: sqlite3.Connection, source_registry_path: Path
) -> dict[str, str]:
    payload = json.loads(source_registry_path.read_text(encoding="utf-8"))
    mapping: dict[str, str] = {}
    rows = []
    for source in payload.get("sources") or []:
        source_id = str(source["source_id"])
        source_key = "source:" + source_id.lower().replace("/", ".").replace(" ", "_")
        mapping[source_id] = source_key
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


def insert_system_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    limit = f" AND s.system_id <= {int(sample_limit)}" if sample_limit else ""
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
            WHERE {where}{limit}
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
            WHERE s.dist_ly IS NOT NULL AND s.dist_ly <= ?{limit}
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
            WHERE s.dist_ly > ? AND s.dist_ly <= ?{limit}
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
    allowed = {
        definition["key"]
        for definition in definition_by_evaluator(registry, "stellar_class_v1")
    }
    rows = [
        (stellar_class, tag_key)
        for stellar_class, tag_key in CLASS_TO_TAG.items()
        if tag_key in allowed
    ]
    con.execute(
        "CREATE TEMP TABLE class_tag_map(classification TEXT PRIMARY KEY,tag_key TEXT)"
    )
    con.executemany("INSERT INTO class_tag_map VALUES (?,?)", rows)
    limit = "WHERE s.system_id <= ?" if sample_limit else ""
    params: tuple[Any, ...] = (sample_limit,) if sample_limit else ()
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
        JOIN class_tag_map m
          ON m.classification=upper(trim(s.selected_classification))
        {limit}
        """,
        params,
    )


def insert_planet_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    allowed = {definition["key"] for definition in registry.definitions}
    con.execute(
        "CREATE TEMP TABLE planet_tag_map(size_class TEXT,insolation_class TEXT,"
        "tag_key TEXT,PRIMARY KEY(size_class,insolation_class))"
    )
    con.executemany(
        "INSERT INTO planet_tag_map VALUES (?,?,?)",
        [
            (size_class, insolation, tag)
            for (size_class, insolation), tag in PLANET_CATEGORY_TO_TAG.items()
            if tag in allowed
        ],
    )
    limit = "AND p.system_id <= ?" if sample_limit else ""
    params: tuple[Any, ...] = (sample_limit,) if sample_limit else ()
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'planet',p.stable_object_key,p.system_id,m.tag_key,
               'versioned_derivation',p.classifier_version,'derived',1.0,
               'planet_category_v1',1
        FROM public.planets p
        JOIN planet_tag_map m
          ON m.size_class=p.size_mass_class
         AND m.insolation_class=p.insolation_class
        WHERE lower(coalesce(p.planet_status,'confirmed')) IN
              ('confirmed','known','published'){limit}
        """,
        params,
    )
    usp = next(
        definition
        for definition in definition_by_evaluator(registry, "planet_numeric_v1")
    )
    values: list[Any] = [usp["key"], usp["evaluator"]["params"]["less_than"]]
    if sample_limit:
        values.append(sample_limit)
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'planet',p.stable_object_key,p.system_id,?,
               'selected_public_fact',
               json_extract(p.selected_fact_lineage_json,
                            '$.orbital_period_days.fact_id'),
               'accepted',1.0,'planet_numeric_v1',1
        FROM public.planets p
        WHERE p.orbital_period_days IS NOT NULL
          AND p.orbital_period_days < ?
          AND lower(coalesce(p.planet_status,'confirmed')) IN
              ('confirmed','known','published'){limit}
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
    if sample_limit:
        values.append(sample_limit)
    con.execute(
        f"""
        INSERT INTO tag_assignments
        SELECT 'planet',p.stable_object_key,p.system_id,?,
               'versioned_derivation',p.classifier_version,'screen',1.0,
               'habitable_zone_screen_v1',1
        FROM public.planets p
        WHERE p.insolation_class='temperate'
          AND lower(coalesce(p.planet_status,'confirmed')) IN
              ('confirmed','known','published'){limit}
        """,
        values,
    )


def _contains_nested_group(node: Any, depth: int = 0) -> bool:
    if not isinstance(node, dict):
        return False
    family = str(node.get("component_family") or "").lower()
    kind = str(node.get("node_kind") or "").lower()
    if depth > 0 and (family in {"group", "subsystem"} or "group" in kind):
        return True
    return any(_contains_nested_group(child, depth + 1) for child in node.get("children") or [])


def insert_hierarchy_assignments(
    con: sqlite3.Connection, registry: LoadedRegistry, sample_limit: int | None
) -> None:
    definition = next(
        definition
        for definition in definition_by_evaluator(registry, "hierarchy_nested_v1")
    )
    sql = (
        "SELECT h.system_id,s.stable_object_key,h.payload_gzip,h.payload_sha256 "
        "FROM public.hierarchy_bundles h JOIN public.systems s USING(system_id) "
        "WHERE s.star_count>=3"
    )
    params: tuple[Any, ...] = ()
    if sample_limit:
        sql += " AND h.system_id<=?"
        params = (sample_limit,)
    assignments = []
    quarantine = []
    for row in con.execute(sql, params):
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
                        "hierarchy_nested_v1",
                        1,
                    )
                )
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            quarantine.append(
                (
                    "system",
                    row[1],
                    "hierarchy_nested_v1",
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
                    THEN 'direct' ELSE 'member_rollup' END
        FROM tag_assignments a
        JOIN tag_definitions d USING(tag_key)
        WHERE d.rollup IN ('direct','member_to_system')
        GROUP BY a.system_id,a.tag_key
        """
    )


def build_sources(con: sqlite3.Connection, sample_limit: int | None) -> None:
    limit = f" AND s.system_id <= {int(sample_limit)}" if sample_limit else ""
    source_rules = [
        (
            "source:gaia.dr3.non_single_star",
            "multiplicity",
            "s.has_gaia_nss_evidence=1",
        ),
        ("source:multiplicity.msc", "multiplicity", "s.has_msc_evidence=1"),
        ("source:multiplicity.sb9", "multiplicity", "s.has_sbx_evidence=1"),
        ("source:multiplicity.wds", "multiplicity", "s.has_wds_evidence=1"),
        ("source:multiplicity.orb6", "orbit", "s.has_orb6_evidence=1"),
    ]
    known = {
        row[0] for row in con.execute("SELECT source_key FROM source_definitions")
    }
    for source_key, contribution, predicate in source_rules:
        if source_key not in known:
            continue
        con.execute(
            f"""
            INSERT INTO system_sources
            SELECT s.system_id,?,'{contribution}',1
            FROM public.systems s WHERE {predicate}{limit}
            """,
            (source_key,),
        )


def create_indexes(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE INDEX idx_tag_assignments_system
          ON tag_assignments(system_id,target_type,tag_key);
        CREATE INDEX idx_tag_assignments_tag
          ON tag_assignments(tag_key,system_id);
        CREATE INDEX idx_system_tag_membership_tag
          ON system_tag_membership(tag_key,system_id);
        CREATE INDEX idx_system_sources_system
          ON system_sources(system_id,source_key);
        CREATE INDEX idx_system_sources_source
          ON system_sources(source_key,system_id);
        """
    )


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
        ORDER BY target_type,stable_object_key,tag_key
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


def compile_tags(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    registry = load_registry(args.registry)
    public_read = args.public_read.resolve(strict=True)
    build_id = public_build_id(public_read)
    output_root = args.output_root.resolve()
    final_dir = output_root / build_id / registry.registry_hash
    if final_dir.exists() and not args.force:
        manifest = json.loads((final_dir / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("status") == "pass":
            return manifest
        raise ValueError(f"incomplete smart-tag output exists: {final_dir}")
    output_root.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{build_id}.{registry.registry_hash[:12]}.",
            dir=output_root,
        )
    )
    timings: dict[str, float] = {}
    try:
        database = staging / "smart_tags.sqlite"
        con = sqlite3.connect(database)
        con.row_factory = sqlite3.Row
        try:
            phase = time.perf_counter()
            create_schema(con)
            insert_definitions(con, registry)
            source_registry_path = (
                args.repo_root / registry.registry["source_registry"]
            ).resolve(strict=True)
            insert_source_definitions(con, source_registry_path)
            _attach_public(con, public_read)
            con.executemany(
                "INSERT INTO metadata VALUES (?,?)",
                [
                    ("schema_version", SCHEMA_VERSION),
                    ("assignment_schema_version", ASSIGNMENT_SCHEMA),
                    ("source_summary_schema_version", SOURCE_SUMMARY_SCHEMA),
                    ("build_id", build_id),
                    ("registry_id", registry.registry["registry_id"]),
                    ("registry_version", registry.registry["registry_version"]),
                    ("registry_hash", registry.registry_hash),
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
            build_rollups(con)
            build_sources(con, args.sample_limit)
            create_indexes(con)
            con.commit()
            timings["rollup_and_indexes_seconds"] = time.perf_counter() - phase

            counts = {
                table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                for table in (
                    "tag_definitions",
                    "tag_assignments",
                    "system_tag_membership",
                    "source_definitions",
                    "system_sources",
                    "quarantine",
                )
            }
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
            }
            con.execute("DETACH DATABASE public")
            con.execute("VACUUM")
            integrity = con.execute("PRAGMA quick_check").fetchone()[0]
            if integrity != "ok":
                raise ValueError(f"smart-tag SQLite quick_check failed: {integrity}")
            phase = time.perf_counter()
            parquet_count = export_assignments(con, staging / "assignments.parquet")
            timings["parquet_seconds"] = time.perf_counter() - phase
            if parquet_count != counts["tag_assignments"]:
                raise ValueError("Parquet assignment count does not match SQLite")
        finally:
            con.close()

        atomic_json(staging / "registry.json", registry.snapshot())
        coverage = {
            "schema_version": "spacegate.smart_tag_coverage.v1",
            "status": "pass",
            "build_id": build_id,
            "registry_hash": registry.registry_hash,
            "sample_limit": args.sample_limit,
            "counts": counts,
            "assignment_counts_by_tag": {},
            "proposal_inventory": registry.proposal_inventory,
        }
        read = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
        try:
            coverage["assignment_counts_by_tag"] = {
                row[0]: row[1]
                for row in read.execute(
                    "SELECT tag_key,count(*) FROM tag_assignments GROUP BY tag_key"
                )
            }
        finally:
            read.close()
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
        manifest = {
            "schema_version": MANIFEST_SCHEMA,
            "status": "pass",
            "build_id": build_id,
            "registry_id": registry.registry["registry_id"],
            "registry_version": registry.registry["registry_version"],
            "registry_hash": registry.registry_hash,
            "tag_schema_version": SCHEMA_VERSION,
            "assignment_schema_version": ASSIGNMENT_SCHEMA,
            "source_summary_schema_version": SOURCE_SUMMARY_SCHEMA,
            "sample_limit": args.sample_limit,
            "public_read": {
                "path": str(public_read),
                "bytes": public_read.stat().st_size,
                "sha256": sha256_file(public_read) if args.hash_input else None,
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
                "registry": {
                    "path": "registry.json",
                    "bytes": (staging / "registry.json").stat().st_size,
                    "sha256": sha256_file(staging / "registry.json"),
                },
            },
            "counts": counts,
            "logical_hashes": hashes,
            "timings": timings,
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
