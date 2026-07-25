#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config" / "public_read" / "projection_v2.json"
DEFAULT_BATCH_SIZE = 20_000
BUILDER_VERSION = "public_read_compiler_v2"
SPECTRAL_CLASS_MASKS = {
    "O": 1,
    "B": 2,
    "A": 4,
    "F": 8,
    "G": 16,
    "K": 32,
    "M": 64,
    "L": 128,
    "T": 256,
    "Y": 512,
    "D": 1024,
    "WD": 1024,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def rows(cur: duckdb.DuckDBPyConnection, batch_size: int) -> Iterator[tuple[Any, ...]]:
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            return
        yield from batch


def chunks(values: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def table_exists(con: duckdb.DuckDBPyConnection, catalog: str, table: str) -> bool:
    resolved_catalog = (
        str(con.execute("SELECT current_database()").fetchone()[0])
        if catalog == "main"
        else catalog
    )
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE lower(table_catalog) = lower(?)
              AND lower(table_name) = lower(?)
            LIMIT 1
            """,
            [resolved_catalog, table],
        ).fetchone()
        is not None
    )


def build_id_from_core(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        "SELECT value FROM build_metadata WHERE key = 'build_id' LIMIT 1"
    ).fetchone()
    if not row or not row[0]:
        raise ValueError("core build_metadata has no build_id")
    return str(row[0])


def attach_if_present(
    con: duckdb.DuckDBPyConnection, path: Path, alias: str
) -> bool:
    if not path.is_file():
        return False
    escaped = str(path.resolve()).replace("'", "''")
    con.execute(f"ATTACH '{escaped}' AS {alias} (READ_ONLY)")
    return True


def configure_sqlite(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA page_size=4096")
    con.execute("PRAGMA auto_vacuum=NONE")
    con.execute("PRAGMA journal_mode=OFF")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA temp_store=FILE")
    con.execute("PRAGMA cache_size=-262144")
    con.execute("PRAGMA locking_mode=EXCLUSIVE")


def create_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE systems (
          system_id INTEGER PRIMARY KEY,
          stable_object_key TEXT NOT NULL UNIQUE,
          system_name TEXT,
          system_name_norm TEXT NOT NULL,
          wds_id TEXT,
          grouping_basis TEXT,
          grouping_confidence REAL,
          grouping_confidence_tier TEXT,
          grouping_source_catalogs_json TEXT,
          has_gaia_nss_evidence INTEGER NOT NULL,
          has_msc_evidence INTEGER NOT NULL,
          has_sbx_evidence INTEGER NOT NULL,
          has_wds_evidence INTEGER NOT NULL,
          has_orb6_evidence INTEGER NOT NULL,
          star_count INTEGER NOT NULL,
          planet_count INTEGER NOT NULL,
          star_teff_count INTEGER NOT NULL,
          min_star_teff_k REAL,
          max_star_teff_k REAL,
          spectral_classes_json TEXT NOT NULL,
          spectral_class_mask INTEGER NOT NULL,
          planet_category_mask INTEGER NOT NULL DEFAULT 0,
          has_habitable_candidate INTEGER NOT NULL DEFAULT 0,
          ra_deg REAL,
          dec_deg REAL,
          dist_ly REAL,
          x_helio_ly REAL,
          y_helio_ly REAL,
          z_helio_ly REAL,
          gaia_id_text TEXT,
          hip_id_text TEXT,
          hd_id_text TEXT,
          coolness_rank INTEGER,
          coolness_score REAL,
          coolness_nice_planet_count INTEGER,
          coolness_weird_planet_count INTEGER,
          coolness_dominant_spectral_class TEXT,
          coolness_score_luminosity REAL,
          coolness_score_proper_motion REAL,
          coolness_score_multiplicity REAL,
          coolness_score_nice_planets REAL,
          coolness_score_weird_planets REAL,
          coolness_score_proximity REAL,
          coolness_score_system_complexity REAL,
          coolness_score_exotic_star REAL,
          source_catalog TEXT,
          source_version TEXT,
          source_pk_text TEXT,
          source_row_hash TEXT,
          transform_version TEXT,
          hierarchy_representation TEXT NOT NULL,
          scene_representation TEXT NOT NULL
        );

        CREATE TABLE stars (
          star_id INTEGER PRIMARY KEY,
          system_id INTEGER NOT NULL,
          stable_object_key TEXT NOT NULL UNIQUE,
          star_name TEXT,
          star_name_norm TEXT,
          component TEXT,
          ra_deg REAL,
          dec_deg REAL,
          dist_ly REAL,
          spectral_type_raw TEXT,
          spectral_class TEXT,
          spectral_subtype TEXT,
          luminosity_class TEXT,
          spectral_peculiar TEXT,
          vmag REAL,
          gaia_id_text TEXT,
          hip_id_text TEXT,
          hd_id_text TEXT,
          wds_id TEXT,
          object_family TEXT,
          object_type TEXT,
          selected_classification TEXT NOT NULL,
          classification_status TEXT NOT NULL,
          classification_basis TEXT,
          classification_fact_id TEXT,
          classification_confidence REAL,
          teff_k REAL,
          teff_k_lower REAL,
          teff_k_upper REAL,
          teff_k_fact_id TEXT,
          radius_rsun REAL,
          radius_rsun_fact_id TEXT,
          mass_msun REAL,
          mass_msun_fact_id TEXT,
          luminosity_lsun REAL,
          luminosity_lsun_fact_id TEXT,
          luminosity_status TEXT,
          luminosity_basis TEXT,
          parameter_source TEXT,
          source_catalog TEXT,
          source_version TEXT,
          source_row_hash TEXT,
          transform_version TEXT
        );

        CREATE TABLE stellar_badge_overlays (
          system_id INTEGER NOT NULL,
          badge_order INTEGER NOT NULL,
          hierarchy_node_key TEXT,
          leaf_component_key TEXT,
          evidence_component_key TEXT,
          star_id_text TEXT,
          stable_object_key TEXT,
          display_name TEXT,
          catalog_component_label TEXT,
          classification_value TEXT NOT NULL,
          classification_status TEXT NOT NULL,
          evidence_basis TEXT,
          selected_fact_id TEXT,
          source_catalog TEXT,
          source_version TEXT,
          PRIMARY KEY (system_id, badge_order)
        ) WITHOUT ROWID;

        CREATE TABLE planets (
          planet_id INTEGER PRIMARY KEY,
          system_id INTEGER NOT NULL,
          star_id INTEGER,
          stable_object_key TEXT NOT NULL UNIQUE,
          planet_name TEXT,
          planet_name_norm TEXT,
          disc_year INTEGER,
          discovery_method TEXT,
          discovery_facility TEXT,
          discovery_telescope TEXT,
          discovery_instrument TEXT,
          orbital_period_days REAL,
          semi_major_axis_au REAL,
          eccentricity REAL,
          inclination_deg REAL,
          radius_earth REAL,
          radius_jup REAL,
          mass_earth REAL,
          mass_jup REAL,
          eq_temp_k REAL,
          insol_earth REAL,
          match_method TEXT,
          match_confidence REAL,
          match_notes TEXT,
          planet_status TEXT,
          size_mass_class TEXT,
          insolation_class TEXT,
          composition_proxy_class TEXT,
          classifier_version TEXT,
          selected_fact_lineage_json TEXT NOT NULL,
          source_catalog TEXT,
          source_version TEXT,
          source_row_hash TEXT,
          transform_version TEXT
        );

        CREATE TABLE aliases (
          alias_id INTEGER PRIMARY KEY,
          system_id INTEGER NOT NULL,
          star_id INTEGER,
          target_type TEXT NOT NULL,
          target_id INTEGER,
          stable_object_key TEXT,
          alias_raw TEXT NOT NULL,
          alias_norm TEXT NOT NULL,
          alias_kind TEXT,
          alias_priority INTEGER NOT NULL,
          is_primary INTEGER NOT NULL,
          source_catalog TEXT,
          source_version TEXT
        );

        CREATE TABLE search_terms (
          search_term_id INTEGER PRIMARY KEY,
          system_id INTEGER NOT NULL,
          target_type TEXT NOT NULL,
          target_id INTEGER,
          star_id INTEGER,
          alias_id INTEGER,
          term_raw TEXT NOT NULL,
          term_norm TEXT NOT NULL,
          term_kind TEXT,
          term_priority INTEGER NOT NULL,
          is_primary INTEGER NOT NULL,
          source_catalog TEXT,
          source_version TEXT
        );

        CREATE TABLE exact_identifiers (
          identifier_id INTEGER PRIMARY KEY,
          system_id INTEGER NOT NULL,
          target_type TEXT NOT NULL,
          target_id INTEGER,
          stable_object_key TEXT NOT NULL,
          namespace TEXT NOT NULL,
          id_value_raw TEXT NOT NULL,
          id_value_norm TEXT NOT NULL,
          is_canonical INTEGER NOT NULL,
          resolution_method TEXT,
          resolution_confidence REAL,
          source_catalog TEXT,
          source_version TEXT,
          source_pk TEXT,
          identifier_source_id TEXT,
          identifier_release_id TEXT,
          binding_key TEXT,
          evidence_json TEXT
        );

        CREATE TABLE identifier_outcomes (
          outcome_key TEXT PRIMARY KEY,
          namespace TEXT NOT NULL,
          identifier_norm TEXT NOT NULL,
          outcome TEXT NOT NULL,
          reason TEXT,
          system_id INTEGER,
          star_id INTEGER,
          object_focus_key TEXT,
          source_catalog TEXT,
          source_version TEXT,
          lineage_json TEXT
        ) WITHOUT ROWID;

        CREATE TABLE identifier_quarantine (
          quarantine_key TEXT PRIMARY KEY,
          quarantine_kind TEXT NOT NULL,
          outcome TEXT NOT NULL,
          reason TEXT,
          subject_node_key TEXT,
          candidate_nodes_json TEXT,
          evidence_json TEXT
        ) WITHOUT ROWID;

        CREATE TABLE hierarchy_bundles (
          system_id INTEGER PRIMARY KEY,
          stable_object_key TEXT NOT NULL,
          bundle_kind TEXT NOT NULL,
          bundle_version TEXT NOT NULL,
          payload_gzip BLOB,
          payload_sha256 TEXT,
          uncompressed_bytes INTEGER,
          source TEXT NOT NULL
        );
        """
    )


def provenance_value(value: Any) -> str | None:
    return None if value is None else str(value)


def load_planet_facets(
    core: duckdb.DuckDBPyConnection,
    *,
    sample_limit: int | None,
) -> dict[int, tuple[int, bool]]:
    api_root = ROOT / "srv" / "api"
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))
    from app.planet_categories import (  # noqa: PLC0415
        planet_category_bit_sql,
        planet_category_eligibility_sql,
    )

    limited_join = ""
    if sample_limit:
        limited_join = (
            "JOIN (SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)}) limited ON limited.system_id = p.system_id"
        )
    category = planet_category_bit_sql("p")
    eligible = planet_category_eligibility_sql("p")
    cur = core.execute(
        f"""
        SELECT
          CAST(p.system_id AS BIGINT),
          bit_or(CASE WHEN {eligible} THEN ({category}) ELSE 0 END)::BIGINT,
          bool_or(
            COALESCE(p.match_confidence, 0.0) >= 0.80
            AND COALESCE(p.eq_temp_k, -1.0) BETWEEN 180.0 AND 350.0
            AND COALESCE(p.mass_earth, p.mass_jup * 317.8, -1.0) BETWEEN 0.3 AND 8.0
            AND COALESCE(p.eccentricity, 0.0) <= 0.35
          )
        FROM planets p
        {limited_join}
        WHERE p.system_id IS NOT NULL
        GROUP BY p.system_id
        ORDER BY p.system_id
        """
    )
    return {
        int(system_id): (int(mask or 0), bool(habitable))
        for system_id, mask, habitable in cur.fetchall()
    }


def insert_systems(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
    planet_facets: dict[int, tuple[int, bool]],
    hierarchy_bundle_ids: set[int],
    compact_seed_ids: set[int],
    full_scene_ids: set[int],
) -> int:
    coolness_select = (
        """
        c.rank, c.score_total, c.nice_planet_count, c.weird_planet_count,
        c.dominant_spectral_class, c.score_luminosity, c.score_proper_motion,
        c.score_multiplicity, c.score_nice_planets, c.score_weird_planets,
        c.score_proximity, c.score_system_complexity, c.score_exotic_star
        """
        if table_exists(source, "disc_db", "coolness_scores")
        else "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL"
    )
    coolness_join = (
        "LEFT JOIN disc_db.coolness_scores c USING (system_id)"
        if table_exists(source, "disc_db", "coolness_scores")
        else ""
    )
    limit = f"LIMIT {int(sample_limit)}" if sample_limit else ""
    cur = source.execute(
        f"""
        SELECT
          CAST(s.system_id AS BIGINT), s.stable_object_key, s.system_name,
          s.system_name_norm, s.wds_id, s.grouping_basis,
          CAST(s.grouping_confidence AS DOUBLE), s.grouping_confidence_tier,
          s.grouping_source_catalogs_json,
          s.has_gaia_nss_evidence, s.has_msc_evidence, s.has_sbx_evidence,
          s.has_wds_evidence, s.has_orb6_evidence,
          s.star_count, s.planet_count, s.star_teff_count,
          s.min_star_teff_k, s.max_star_teff_k,
          COALESCE(s.spectral_classes_json, '[]'), COALESCE(s.spectral_class_mask, 0),
          s.ra_deg, s.dec_deg, s.dist_ly, s.x_helio_ly, s.y_helio_ly, s.z_helio_ly,
          CAST(s.gaia_id AS VARCHAR), CAST(s.hip_id AS VARCHAR), CAST(s.hd_id AS VARCHAR),
          {coolness_select},
          s.source_catalog, s.source_version, CAST(s.source_pk AS VARCHAR),
          s.source_row_hash, s.transform_version
        FROM systems s
        {coolness_join}
        ORDER BY s.system_id
        {limit}
        """
    )
    sql = "INSERT INTO systems VALUES (" + ",".join("?" for _ in range(52)) + ")"
    inserted = 0
    batch: list[tuple[Any, ...]] = []
    for row in rows(cur, DEFAULT_BATCH_SIZE):
        system_id = int(row[0])
        category_mask, has_habitable = planet_facets.get(system_id, (0, False))
        star_count = int(row[14] or 0)
        planet_count = int(row[15] or 0)
        hierarchy_representation = (
            "bundle_required" if system_id in hierarchy_bundle_ids else "singleton_seed"
        )
        if system_id in full_scene_ids:
            scene_representation = "full_scene"
        elif system_id in compact_seed_ids:
            scene_representation = "compact_seed"
        else:
            scene_representation = "singleton_seed"
        batch.append(
            (
                system_id,
                *row[1:21],
                int(category_mask),
                int(has_habitable),
                *row[21:],
                hierarchy_representation,
                scene_representation,
            )
        )
        if len(batch) >= DEFAULT_BATCH_SIZE:
            target.executemany(sql, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        target.executemany(sql, batch)
        inserted += len(batch)
    target.commit()
    return inserted


def insert_stars(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
) -> int:
    limited_join = ""
    if sample_limit:
        limited_join = (
            "JOIN (SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)}) limited USING (system_id)"
        )
    cur = source.execute(
        f"""
        SELECT
          CAST(st.star_id AS BIGINT), CAST(st.system_id AS BIGINT),
          st.stable_object_key, st.star_name, st.star_name_norm, st.component,
          st.ra_deg, st.dec_deg, st.dist_ly, st.spectral_type_raw,
          st.spectral_class, st.spectral_subtype, st.luminosity_class,
          st.spectral_peculiar, st.vmag, CAST(st.gaia_id AS VARCHAR),
          CAST(st.hip_id AS VARCHAR), CAST(st.hd_id AS VARCHAR), st.wds_id,
          st.object_family, st.object_type,
          COALESCE(cls.classification_value, 'UNKNOWN'),
          COALESCE(cls.classification_status, 'missing'),
          cls.evidence_basis, cls.selected_fact_id, cls.confidence_score,
          p.teff_k, p.teff_k_lower, p.teff_k_upper, p.teff_k_fact_id,
          p.radius_rsun, p.radius_rsun_fact_id, p.mass_msun, p.mass_msun_fact_id,
          p.luminosity_lsun, p.luminosity_lsun_fact_id,
          p.luminosity_lsun_status, p.luminosity_lsun_basis, p.parameter_source,
          st.source_catalog, st.source_version, st.source_row_hash,
          st.transform_version
        FROM stars st
        {limited_join}
        LEFT JOIN arm_db.e6_selected_stellar_parameters p USING (star_id)
        LEFT JOIN arm_db.e6_selected_stellar_display_classifications cls USING (star_id)
        ORDER BY st.star_id
        """
    )
    sql = "INSERT INTO stars VALUES (" + ",".join("?" for _ in range(43)) + ")"
    inserted = 0
    batch: list[tuple[Any, ...]] = []
    for row in rows(cur, DEFAULT_BATCH_SIZE):
        batch.append(tuple(row))
        if len(batch) >= DEFAULT_BATCH_SIZE:
            target.executemany(sql, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        target.executemany(sql, batch)
        inserted += len(batch)
    target.commit()
    return inserted


def insert_stellar_badge_overlays(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
) -> tuple[int, int]:
    """Store only leaf projections that differ from the canonical star rows."""
    scope = ""
    if sample_limit:
        scope = (
            " AND CAST(leaf.system_id AS BIGINT) IN "
            "(SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)})"
        )
    cursor = source.execute(
        f"""
        WITH leaf_counts AS (
          SELECT CAST(system_id AS BIGINT) AS system_id, COUNT(*) AS leaf_count
          FROM arm_db.stellar_leaf_display_classifications
          WHERE system_id IS NOT NULL
          GROUP BY 1
        ),
        mismatched AS (
          SELECT DISTINCT CAST(leaf.system_id AS BIGINT) AS system_id
          FROM arm_db.stellar_leaf_display_classifications leaf
          LEFT JOIN arm_db.e6_selected_stellar_display_classifications selected
            ON CAST(selected.system_id AS BIGINT) = CAST(leaf.system_id AS BIGINT)
           AND selected.star_id = leaf.star_id
          WHERE leaf.system_id IS NOT NULL
            AND (
              leaf.star_id IS NULL
              OR COALESCE(leaf.classification_value, 'UNKNOWN')
                 <> COALESCE(selected.classification_value, 'UNKNOWN')
            )
        ),
        eligible AS (
          SELECT counts.system_id
          FROM leaf_counts counts
          JOIN systems system_row USING (system_id)
          WHERE counts.leaf_count <> COALESCE(system_row.star_count, 0)
             OR counts.system_id IN (SELECT system_id FROM mismatched)
        )
        SELECT
          CAST(leaf.system_id AS BIGINT), leaf.hierarchy_node_key,
          leaf.leaf_component_key, leaf.evidence_component_key,
          CAST(leaf.star_id AS VARCHAR), leaf.stable_object_key,
          leaf.display_name, leaf.catalog_component_label,
          COALESCE(leaf.classification_value, 'UNKNOWN'),
          COALESCE(leaf.classification_status, 'missing'),
          leaf.evidence_basis, leaf.selected_fact_id,
          leaf.source_catalog, leaf.source_version
        FROM arm_db.stellar_leaf_display_classifications leaf
        JOIN eligible
          ON eligible.system_id = CAST(leaf.system_id AS BIGINT)
        WHERE leaf.system_id IS NOT NULL {scope}
        ORDER BY CAST(leaf.system_id AS BIGINT), leaf.hierarchy_node_key,
                 leaf.leaf_component_key
        """
    )
    insert_sql = (
        "INSERT INTO stellar_badge_overlays VALUES ("
        + ",".join("?" for _ in range(15))
        + ")"
    )
    inserted = 0
    systems: dict[int, list[str]] = {}
    current_system_id: int | None = None
    badge_order = 0
    batch: list[tuple[Any, ...]] = []
    for row in rows(cursor, DEFAULT_BATCH_SIZE):
        system_id = int(row[0])
        if system_id != current_system_id:
            current_system_id = system_id
            badge_order = 0
        classification = str(row[8] or "UNKNOWN").strip().upper()
        batch.append((system_id, badge_order, *row[1:]))
        systems.setdefault(system_id, []).append(classification)
        badge_order += 1
        if len(batch) >= DEFAULT_BATCH_SIZE:
            target.executemany(insert_sql, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        target.executemany(insert_sql, batch)
        inserted += len(batch)

    updates: list[tuple[int, str, int, int]] = []
    for system_id, classifications in sorted(systems.items()):
        normalized = sorted(
            {
                "D" if token == "WD" else token
                for token in classifications
                if token and token != "UNKNOWN"
            }
        )
        mask = 0
        for token in classifications:
            mask |= SPECTRAL_CLASS_MASKS.get(token, 0)
        updates.append(
            (
                len(classifications),
                canonical_json(normalized),
                mask,
                system_id,
            )
        )
    target.executemany(
        """
        UPDATE systems
        SET star_count=?, spectral_classes_json=?, spectral_class_mask=?
        WHERE system_id=?
        """,
        updates,
    )
    target.commit()
    return inserted, len(systems)


def insert_planets(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
) -> int:
    limited_join = ""
    if sample_limit:
        limited_join = (
            "JOIN (SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)}) limited ON limited.system_id = p.system_id"
        )
    cur = source.execute(
        f"""
        SELECT
          CAST(p.planet_id AS BIGINT), CAST(p.system_id AS BIGINT),
          CAST(p.star_id AS BIGINT), p.stable_object_key, p.planet_name,
          p.planet_name_norm, p.disc_year, p.discovery_method,
          p.discovery_facility, p.discovery_telescope, p.discovery_instrument,
          p.orbital_period_days, p.semi_major_axis_au, p.eccentricity,
          p.inclination_deg, p.radius_earth, p.radius_jup, p.mass_earth,
          p.mass_jup, p.eq_temp_k, p.insol_earth, p.match_method,
          CAST(p.match_confidence AS DOUBLE), p.match_notes, p.planet_status,
          p.planet_size_mass_class, p.planet_insolation_class,
          p.planet_composition_proxy_class, p.planet_classifier_version,
          to_json(struct_pack(
            lineage_version := 'spacegate.planet_selected_fact_lineage.v1',
            orbital_period_days := struct_pack(
              lower := sp.orbital_period_days_lower,
              upper := sp.orbital_period_days_upper,
              fact_id := sp.orbital_period_days_fact_id
            ),
            semi_major_axis_au := struct_pack(
              lower := sp.semi_major_axis_au_lower,
              upper := sp.semi_major_axis_au_upper,
              fact_id := sp.semi_major_axis_au_fact_id
            ),
            eccentricity := struct_pack(
              lower := sp.eccentricity_lower,
              upper := sp.eccentricity_upper,
              fact_id := sp.eccentricity_fact_id
            ),
            inclination_deg := struct_pack(
              lower := sp.inclination_deg_lower,
              upper := sp.inclination_deg_upper,
              fact_id := sp.inclination_deg_fact_id
            ),
            radius_earth := struct_pack(
              lower := sp.radius_earth_lower,
              upper := sp.radius_earth_upper,
              fact_id := sp.radius_earth_fact_id
            ),
            radius_jup := struct_pack(
              lower := sp.radius_jup_lower,
              upper := sp.radius_jup_upper,
              fact_id := sp.radius_jup_fact_id
            ),
            mass_earth := struct_pack(
              lower := sp.best_mass_earth_lower,
              upper := sp.best_mass_earth_upper,
              fact_id := sp.best_mass_earth_fact_id
            ),
            mass_jup := struct_pack(
              lower := sp.best_mass_jup_lower,
              upper := sp.best_mass_jup_upper,
              fact_id := sp.best_mass_jup_fact_id
            ),
            eq_temp_k := struct_pack(
              lower := sp.eq_temp_k_lower,
              upper := sp.eq_temp_k_upper,
              fact_id := sp.eq_temp_k_fact_id
            ),
            insol_earth := struct_pack(
              lower := sp.insol_earth_lower,
              upper := sp.insol_earth_upper,
              fact_id := sp.insol_earth_fact_id
            )
          )),
          p.source_catalog, p.source_version, p.source_row_hash,
          p.transform_version
        FROM planets p
        LEFT JOIN arm_db.e6_selected_planet_parameters sp USING (planet_id)
        {limited_join}
        WHERE p.system_id IS NOT NULL
        ORDER BY p.planet_id
        """
    )
    sql = "INSERT INTO planets VALUES (" + ",".join("?" for _ in range(34)) + ")"
    batch = list(rows(cur, DEFAULT_BATCH_SIZE))
    for part in chunks(batch, DEFAULT_BATCH_SIZE):
        target.executemany(sql, part)
    target.commit()
    return len(batch)


def insert_aliases_and_terms(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
) -> tuple[int, int]:
    system_filter = ""
    if sample_limit:
        system_filter = (
            " AND system_id IN (SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)})"
        )
    alias_cur = source.execute(
        f"""
        SELECT alias_id, CAST(system_id AS BIGINT), CAST(star_id AS BIGINT),
               target_type, CAST(target_id AS BIGINT), stable_object_key,
               alias_raw, alias_norm, alias_kind, alias_priority, is_primary,
               source_catalog, source_version
        FROM aliases
        WHERE system_id IS NOT NULL {system_filter}
        ORDER BY alias_id
        """
    )
    alias_sql = "INSERT INTO aliases VALUES (" + ",".join("?" for _ in range(13)) + ")"
    alias_count = 0
    batch: list[tuple[Any, ...]] = []
    for row in rows(alias_cur, DEFAULT_BATCH_SIZE):
        batch.append(tuple(row))
        if len(batch) >= DEFAULT_BATCH_SIZE:
            target.executemany(alias_sql, batch)
            alias_count += len(batch)
            batch.clear()
    if batch:
        target.executemany(alias_sql, batch)
        alias_count += len(batch)
    target.commit()

    term_cur = source.execute(
        f"""
        SELECT search_term_id, CAST(system_id AS BIGINT), target_type,
               CAST(target_id AS BIGINT), CAST(star_id AS BIGINT), alias_id,
               term_raw, term_norm, term_kind, term_priority, is_primary,
               source_catalog, source_version
        FROM system_search_terms
        WHERE system_id IS NOT NULL {system_filter}
        ORDER BY search_term_id
        """
    )
    term_sql = "INSERT INTO search_terms VALUES (" + ",".join("?" for _ in range(13)) + ")"
    term_count = 0
    batch.clear()
    for row in rows(term_cur, DEFAULT_BATCH_SIZE):
        batch.append(tuple(row))
        if len(batch) >= DEFAULT_BATCH_SIZE:
            target.executemany(term_sql, batch)
            term_count += len(batch)
            batch.clear()
    if batch:
        target.executemany(term_sql, batch)
        term_count += len(batch)
    target.commit()
    return alias_count, term_count


def insert_exact_identifiers(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
) -> int:
    scope = ""
    if sample_limit:
        scope = (
            " AND s.system_id IN "
            "(SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)})"
        )
    cursor = source.execute(
        f"""
        SELECT oi.identifier_id, CAST(s.system_id AS BIGINT), oi.target_type,
               CAST(oi.target_id AS BIGINT), oi.stable_object_key,
               lower(oi.namespace), oi.id_value_raw, oi.id_value_norm,
               oi.is_canonical, oi.resolution_method,
               oi.resolution_confidence, oi.source_catalog, oi.source_version,
               oi.source_pk, oi.identifier_source_id, oi.identifier_release_id,
               oi.binding_key, oi.evidence_json
        FROM object_identifiers oi
        JOIN systems s
          ON s.stable_object_key = oi.system_stable_object_key
        WHERE oi.id_value_norm IS NOT NULL {scope}
        ORDER BY oi.identifier_id
        """
    )
    insert_sql = (
        "INSERT INTO exact_identifiers VALUES ("
        + ",".join("?" for _ in range(18))
        + ")"
    )
    count = 0
    batch: list[tuple[Any, ...]] = []
    for row in rows(cursor, DEFAULT_BATCH_SIZE):
        batch.append(tuple(row))
        if len(batch) >= DEFAULT_BATCH_SIZE:
            target.executemany(insert_sql, batch)
            count += len(batch)
            batch.clear()
    if batch:
        target.executemany(insert_sql, batch)
        count += len(batch)
    target.commit()
    return count


def insert_identifier_outcomes(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    *,
    sample_limit: int | None,
) -> int:
    allowed = ""
    identifier_scope = ""
    if sample_limit:
        allowed = (
            " AND (system_id IS NULL OR system_id IN "
            "(SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)}))"
        )
        identifier_scope = (
            " AND (s.system_id IS NULL OR s.system_id IN "
            "(SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)}))"
        )
    rows_to_insert: list[tuple[Any, ...]] = []
    for row in source.execute(
        f"""
        SELECT lower(oi.namespace), oi.id_value_norm, oi.target_type,
               CAST(oi.target_id AS BIGINT), oi.stable_object_key,
               CAST(s.system_id AS BIGINT), oi.source_catalog,
               oi.source_version, oi.evidence_json
        FROM object_identifiers oi
        JOIN systems s
          ON s.stable_object_key = oi.system_stable_object_key
        WHERE lower(oi.namespace) = 'tic'
          AND oi.id_value_norm IS NOT NULL
          {identifier_scope}
        ORDER BY oi.id_value_norm, oi.target_type, oi.target_id,
                 oi.stable_object_key
        """
    ).fetchall():
        identifier = f"tic {str(row[1]).strip().lower().removeprefix('tic').strip()}"
        lineage = {
            "target_type": row[2],
            "target_id": int(row[3]) if row[3] is not None else None,
            "identifier_evidence": json.loads(row[8]) if row[8] else None,
        }
        values = (
            "tic",
            identifier,
            "accepted",
            "accepted_identifier_binding",
            int(row[5]),
            int(row[3]) if row[2] == "star" and row[3] is not None else None,
            row[4],
            row[6],
            row[7],
            canonical_json(lineage),
        )
        rows_to_insert.append((sha256_text(canonical_json(values)), *values))
    if table_exists(source, "arm_db", "tess_target_identity"):
        for row in source.execute(
            f"""
            SELECT tic_id, resolution_status, resolution_reason,
                   CAST(system_id AS BIGINT), star_id, source_version,
                   candidates_json
            FROM arm_db.tess_target_identity
            WHERE tic_id IS NOT NULL {allowed}
            ORDER BY tic_id, resolution_status, system_id, star_id
            """
        ).fetchall():
            identifier = f"tic {int(row[0])}"
            values = (
                    "tic",
                    identifier,
                    str(row[1] or "missing"),
                    row[2],
                    int(row[3]) if row[3] is not None else None,
                    int(row[4]) if row[4] is not None else None,
                    None,
                    "tess_target_identity",
                    row[5],
                    row[6],
                )
            rows_to_insert.append((sha256_text(canonical_json(values)), *values))
    if table_exists(source, "arm_db", "toi_current_evidence"):
        for row in source.execute(
            f"""
            SELECT toi_display, host_resolution_status, host_resolution_reason,
                   CAST(system_id AS BIGINT), star_id, source_version,
                   source_key
            FROM arm_db.toi_current_evidence
            WHERE toi_display IS NOT NULL {allowed}
            ORDER BY toi_display, host_resolution_status, system_id, star_id
            """
        ).fetchall():
            toi_text = str(row[0]).lower().replace("toi-", "")
            host, _, component = toi_text.partition(".")
            identifier = f"toi {int(host)}"
            if component:
                identifier += f" {int(component):02d}"
            values = (
                    "toi",
                    identifier,
                    str(row[1] or "missing"),
                    row[2],
                    int(row[3]) if row[3] is not None else None,
                    int(row[4]) if row[4] is not None else None,
                    None,
                    "toi_current_evidence",
                    row[5],
                    canonical_json({"source_key": row[6]}),
                )
            rows_to_insert.append((sha256_text(canonical_json(values)), *values))
            host_values = (
                "toi",
                f"toi {int(host)}",
                str(row[1] or "missing"),
                row[2],
                int(row[3]) if row[3] is not None else None,
                int(row[4]) if row[4] is not None else None,
                None,
                "toi_current_evidence",
                row[5],
                canonical_json({"source_key": row[6], "scope": "host"}),
            )
            rows_to_insert.append(
                (sha256_text(canonical_json(host_values)), *host_values)
            )
    target.executemany(
        "INSERT OR IGNORE INTO identifier_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        rows_to_insert,
    )
    target.commit()
    return target.execute("SELECT COUNT(*) FROM identifier_outcomes").fetchone()[0]


def insert_identifier_quarantine(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
) -> int:
    target.executemany(
        "INSERT INTO identifier_quarantine VALUES (?,?,?,?,?,?,?)",
        rows(
            source.execute(
                """
                SELECT quarantine_key, quarantine_kind, outcome, reason,
                       subject_node_key, CAST(candidate_nodes_json AS VARCHAR),
                       evidence_json
                FROM identifier_quarantine
                ORDER BY quarantine_key
                """
            ),
            DEFAULT_BATCH_SIZE,
        ),
    )
    target.commit()
    return int(
        target.execute("SELECT COUNT(*) FROM identifier_quarantine").fetchone()[0]
    )


def insert_singleton_seeds(
    target: sqlite3.Connection,
    policy: dict[str, Any],
) -> int:
    target.executemany(
        "INSERT OR REPLACE INTO metadata(key,value) VALUES (?,?)",
        [
            ("singleton_scene_seed_version", policy["singleton_scene_seed_version"]),
            ("render_policy_version", policy["render_policy_version"]),
            (
                "habitable_zone_policy_version",
                policy["habitable_zone_policy_version"],
            ),
        ],
    )
    target.execute(
        """
        CREATE VIEW singleton_scene_seeds AS
        SELECT
          s.system_id, s.stable_object_key, s.system_name,
          st.star_id, st.stable_object_key AS star_stable_object_key, st.star_name,
          st.selected_classification, st.classification_status,
          st.classification_fact_id, st.teff_k, st.teff_k_fact_id,
          st.radius_rsun, st.radius_rsun_fact_id, st.mass_msun,
          st.mass_msun_fact_id, st.luminosity_lsun,
          st.luminosity_lsun_fact_id, st.luminosity_status,
          st.luminosity_basis,
          (SELECT value FROM metadata WHERE key='singleton_scene_seed_version')
            AS seed_version,
          (SELECT value FROM metadata WHERE key='render_policy_version')
            AS render_policy_version,
          (SELECT value FROM metadata WHERE key='habitable_zone_policy_version')
            AS habitable_zone_policy_version
        FROM systems s
        JOIN stars st USING (system_id)
        WHERE s.star_count = 1
          AND s.planet_count = 0
          AND s.scene_representation IN ('singleton_seed', 'compact_seed')
        """
    )
    count = int(target.execute("SELECT COUNT(*) FROM singleton_scene_seeds").fetchone()[0])
    target.commit()
    return count


def representation_policies(
    source: duckdb.DuckDBPyConnection,
    policy: dict[str, Any],
    *,
    sample_limit: int | None,
) -> tuple[set[int], set[int], set[int], dict[str, int]]:
    scope = ""
    if sample_limit:
        scope = (
            " AND system_id IN (SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)})"
        )
    full_scene_ids = {
        int(row[0])
        for row in source.execute(
            f"""
            SELECT system_id
            FROM systems
            WHERE system_id IS NOT NULL
              AND (COALESCE(star_count,0) > 1 OR COALESCE(planet_count,0) > 0)
              {scope}
            ORDER BY system_id
            """
        ).fetchall()
    }
    leaf_multistar_ids: set[int] = set()
    if table_exists(source, "arm_db", "stellar_leaf_display_classifications"):
        leaf_scope = ""
        if sample_limit:
            leaf_scope = (
                " AND CAST(system_id AS BIGINT) IN "
                "(SELECT system_id FROM systems ORDER BY system_id "
                f"LIMIT {int(sample_limit)})"
            )
        leaf_multistar_ids = {
            int(row[0])
            for row in source.execute(
                f"""
                SELECT CAST(system_id AS BIGINT)
                FROM arm_db.stellar_leaf_display_classifications
                WHERE system_id IS NOT NULL {leaf_scope}
                GROUP BY CAST(system_id AS BIGINT)
                HAVING COUNT(*) > 1
                ORDER BY CAST(system_id AS BIGINT)
                """
            ).fetchall()
        }
        full_scene_ids.update(leaf_multistar_ids)
    rank_max = int(
        (policy.get("full_scene_policy") or {}).get("include_high_coolness_rank_max")
        or 0
    )
    if rank_max and table_exists(source, "disc_db", "coolness_scores"):
        full_scene_ids.update(
            int(row[0])
            for row in source.execute(
                f"""
                SELECT system_id
                FROM disc_db.coolness_scores
                WHERE rank <= ? {scope}
                ORDER BY system_id
                """,
                [rank_max],
            ).fetchall()
        )
    compact_tokens = ("WD", "WR", "NS", "PULSAR", "MAGNETAR", "BLACK HOLE")
    compact_seed_ids = {
        int(row[0])
        for row in source.execute(
            f"""
            SELECT DISTINCT cls.system_id
            FROM arm_db.e6_selected_stellar_display_classifications cls
            JOIN systems s USING (system_id)
            WHERE cls.system_id IS NOT NULL
              AND upper(cls.classification_value) IN ({','.join('?' for _ in compact_tokens)})
              AND COALESCE(s.star_count,0) = 1
              AND COALESCE(s.planet_count,0) = 0
              {
                  "AND cls.system_id IN (SELECT system_id FROM systems ORDER BY system_id LIMIT "
                  + str(int(sample_limit))
                  + ")"
                  if sample_limit
                  else ""
              }
            ORDER BY cls.system_id
            """,
            list(compact_tokens),
        ).fetchall()
    }
    hierarchy_bundle_ids = set(full_scene_ids)
    if table_exists(source, "main", "eclipsing_binaries"):
        hierarchy_bundle_ids.update(
            int(row[0])
            for row in source.execute(
                f"""
                SELECT DISTINCT system_id
                FROM eclipsing_binaries
                WHERE system_id IS NOT NULL {scope}
                ORDER BY system_id
                """
            ).fetchall()
        )
    if table_exists(source, "arm_db", "infrared_source_matches"):
        hierarchy_bundle_ids.update(
            int(row[0])
            for row in source.execute(
                f"""
                SELECT DISTINCT system_id
                FROM arm_db.infrared_source_matches
                WHERE system_id IS NOT NULL {scope}
                ORDER BY system_id
                """
            ).fetchall()
        )
    return (
        hierarchy_bundle_ids,
        compact_seed_ids,
        full_scene_ids,
        {
            "hierarchy_bundle_required": len(hierarchy_bundle_ids),
            "leaf_multistar_required": len(leaf_multistar_ids),
            "compact_singleton_seed": len(compact_seed_ids - full_scene_ids),
            "full_scene_required": len(full_scene_ids),
        },
    )


def create_indexes(target: sqlite3.Connection) -> None:
    target.executescript(
        """
        CREATE INDEX systems_name_idx ON systems(system_name_norm, system_id);
        CREATE INDEX systems_distance_idx ON systems(dist_ly, system_id);
        CREATE INDEX systems_coolness_sort_idx
          ON systems(
            coalesce(coolness_rank,9223372036854775807),
            system_name_norm,
            system_id
          );
        CREATE INDEX systems_star_count_idx ON systems(star_count DESC, system_name_norm, system_id);
        CREATE INDEX systems_planet_count_idx ON systems(planet_count DESC, system_name_norm, system_id);
        CREATE INDEX systems_hottest_idx ON systems(max_star_teff_k DESC, system_name_norm, system_id);
        CREATE INDEX systems_coolest_idx ON systems(min_star_teff_k, system_name_norm, system_id);
        CREATE INDEX systems_facets_idx ON systems(spectral_class_mask, planet_category_mask);
        CREATE INDEX stars_system_idx ON stars(system_id, star_id);
        CREATE INDEX planets_system_idx ON planets(system_id, planet_id);
        CREATE INDEX aliases_system_idx ON aliases(system_id, alias_priority, alias_id);
        CREATE INDEX aliases_star_idx ON aliases(star_id, alias_priority, alias_id);
        CREATE INDEX search_terms_exact_idx ON search_terms(term_norm, term_priority, system_id, search_term_id);
        CREATE INDEX search_terms_system_idx ON search_terms(system_id, term_priority, search_term_id);
        CREATE INDEX exact_identifiers_lookup_idx
          ON exact_identifiers(namespace, id_value_norm, system_id, identifier_id);
        CREATE INDEX exact_identifiers_system_idx
          ON exact_identifiers(system_id, target_type, target_id, identifier_id);
        CREATE INDEX identifier_outcomes_lookup_idx
          ON identifier_outcomes(namespace, identifier_norm, outcome, system_id);
        CREATE INDEX identifier_quarantine_subject_idx
          ON identifier_quarantine(subject_node_key, quarantine_kind);
        """
    )
    target.execute(
        """
        CREATE VIRTUAL TABLE search_terms_fts USING fts5(
          term_norm,
          content='search_terms',
          content_rowid='search_term_id',
          tokenize='trigram'
        )
        """
    )
    target.execute("INSERT INTO search_terms_fts(search_terms_fts) VALUES('rebuild')")
    target.commit()


def logical_digest(con: sqlite3.Connection, table: str, columns: Sequence[str]) -> str:
    digest = hashlib.sha256()
    selected = ",".join(columns)
    order = ",".join(str(index + 1) for index in range(len(columns)))
    for row in con.execute(f"SELECT {selected} FROM {table} ORDER BY {order}"):
        digest.update(canonical_json(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def verify(
    con: sqlite3.Connection,
    *,
    expected: dict[str, int],
) -> dict[str, Any]:
    checks: dict[str, Any] = {
        "sqlite_integrity": con.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_key_check_count": len(con.execute("PRAGMA foreign_key_check").fetchall()),
        "counts": {},
        "duplicate_counts": {},
    }
    for table in [
        "systems",
        "stars",
        "stellar_badge_overlays",
        "planets",
        "aliases",
        "search_terms",
        "exact_identifiers",
        "identifier_outcomes",
        "identifier_quarantine",
        "singleton_scene_seeds",
        "hierarchy_bundles",
    ]:
        checks["counts"][table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    checks["duplicate_counts"] = {
        "systems": int(
            con.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT system_id) FROM systems"
            ).fetchone()[0]
        ),
        "stars": int(
            con.execute("SELECT COUNT(*) - COUNT(DISTINCT star_id) FROM stars").fetchone()[0]
        ),
        "search_terms": int(
            con.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT search_term_id) FROM search_terms"
            ).fetchone()[0]
        ),
    }
    checks["expected_counts"] = expected
    checks["count_mismatches"] = {
        key: {"expected": value, "actual": checks["counts"].get(key)}
        for key, value in expected.items()
        if checks["counts"].get(key) != value
    }
    checks["status"] = (
        "pass"
        if checks["sqlite_integrity"] == "ok"
        and checks["foreign_key_check_count"] == 0
        and not checks["count_mismatches"]
        and not any(checks["duplicate_counts"].values())
        else "fail"
    )
    return checks


def source_accounting(
    source: duckdb.DuckDBPyConnection,
    *,
    sample_limit: int | None,
) -> dict[str, Any]:
    scope = ""
    if sample_limit:
        scope = (
            " AND system_id IN (SELECT system_id FROM systems ORDER BY system_id "
            f"LIMIT {int(sample_limit)})"
        )
    counts = {
        "canonical_systems": int(
            source.execute(
                f"SELECT COUNT(*) FROM (SELECT system_id FROM systems ORDER BY system_id "
                f"{'LIMIT ' + str(int(sample_limit)) if sample_limit else ''})"
            ).fetchone()[0]
        ),
        "system_bound_stars": int(
            source.execute(
                f"SELECT COUNT(*) FROM stars WHERE system_id IS NOT NULL {scope}"
            ).fetchone()[0]
        ),
        "all_planet_rows": int(source.execute("SELECT COUNT(*) FROM planets").fetchone()[0]),
        "system_bound_planets": int(
            source.execute(
                f"SELECT COUNT(*) FROM planets WHERE system_id IS NOT NULL {scope}"
            ).fetchone()[0]
        ),
        "all_alias_rows": int(source.execute("SELECT COUNT(*) FROM aliases").fetchone()[0]),
        "system_bound_aliases": int(
            source.execute(
                f"SELECT COUNT(*) FROM aliases WHERE system_id IS NOT NULL {scope}"
            ).fetchone()[0]
        ),
        "all_search_terms": int(
            source.execute("SELECT COUNT(*) FROM system_search_terms").fetchone()[0]
        ),
        "system_bound_search_terms": int(
            source.execute(
                f"SELECT COUNT(*) FROM system_search_terms WHERE system_id IS NOT NULL {scope}"
            ).fetchone()[0]
        ),
        "all_object_identifiers": int(
            source.execute("SELECT COUNT(*) FROM object_identifiers").fetchone()[0]
        ),
        "identifier_quarantine_rows": int(
            source.execute("SELECT COUNT(*) FROM identifier_quarantine").fetchone()[0]
        ),
    }
    exclusions = {
        "unbound_planet_rows": counts["all_planet_rows"]
        - counts["system_bound_planets"],
        "aliases_without_system_binding": counts["all_alias_rows"]
        - counts["system_bound_aliases"],
        "search_terms_without_system_binding": counts["all_search_terms"]
        - counts["system_bound_search_terms"],
    }
    accounting: dict[str, Any] = {
        "counts": counts,
        "notes": [
            "Only rows bound to a canonical public system can enter system-keyed read models.",
            "Unbound science remains in CORE/ARM and is not deleted or treated as a public system.",
            "Object identifiers are accounted separately from search terms; explicit TIC/TOI outcomes preserve negative resolution semantics.",
        ],
    }
    if sample_limit:
        accounting["sample_scope_differences"] = exclusions
        accounting["notes"].append(
            "Sample-scope differences are not scientific exclusions and are not used for full-build acceptance."
        )
    else:
        accounting["deliberate_exclusions"] = exclusions
    return accounting


def compile_projection(args: argparse.Namespace) -> dict[str, Any]:
    build_dir = Path(args.build_dir).resolve(strict=True)
    policy_path = Path(args.policy).resolve(strict=True)
    policy = load_json(policy_path)
    if policy.get("schema_version") != "spacegate.public_read_policy.v1":
        raise ValueError("unsupported public-read policy")

    core_path = build_dir / "core.duckdb"
    arm_path = build_dir / "arm.duckdb"
    disc_path = build_dir / "disc.duckdb"
    if not core_path.is_file() or not arm_path.is_file():
        raise ValueError(f"build lacks core/arm databases: {build_dir}")

    source = duckdb.connect(str(core_path), read_only=True)
    attach_if_present(source, arm_path, "arm_db")
    attach_if_present(source, disc_path, "disc_db")
    build_id = build_id_from_core(source)

    output_root = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else Path(args.state_dir).resolve() / "derived" / "public_read" / build_id
    )
    final_db = output_root / "public_read.sqlite"
    temporary_root = output_root.with_name(f".{output_root.name}.tmp.{os.getpid()}")
    if temporary_root.exists():
        shutil.rmtree(temporary_root)
    temporary_root.mkdir(parents=True)
    temporary_db = temporary_root / final_db.name

    started = time.perf_counter()
    phase_timings: list[dict[str, Any]] = []

    def phase(name: str, began: float, count: int | None = None) -> None:
        row: dict[str, Any] = {
            "phase": name,
            "wall_seconds": round(time.perf_counter() - began, 6),
        }
        if count is not None:
            row["rows"] = count
        phase_timings.append(row)

    target = sqlite3.connect(str(temporary_db))
    try:
        configure_sqlite(target)
        create_schema(target)
        metadata = {
            "build_id": build_id,
            "builder_version": BUILDER_VERSION,
            "projection_schema_version": policy["projection_schema_version"],
            "search_schema_version": policy["search_schema_version"],
            "policy_sha256": sha256_file(policy_path),
            "source_core_sha256": sha256_file(core_path),
            "source_arm_sha256": sha256_file(arm_path),
            "source_disc_sha256": sha256_file(disc_path) if disc_path.is_file() else "",
        }
        target.executemany(
            "INSERT INTO metadata(key,value) VALUES (?,?)",
            sorted(metadata.items()),
        )
        target.commit()

        began = time.perf_counter()
        planet_facets = load_planet_facets(source, sample_limit=args.sample_limit)
        phase("planet_facets", began, len(planet_facets))

        began = time.perf_counter()
        (
            hierarchy_bundle_ids,
            compact_seed_ids,
            full_scene_ids,
            representation_counts,
        ) = representation_policies(
            source,
            policy,
            sample_limit=args.sample_limit,
        )
        phase(
            "representation_policies",
            began,
            sum(representation_counts.values()),
        )

        began = time.perf_counter()
        system_count = insert_systems(
            source,
            target,
            sample_limit=args.sample_limit,
            planet_facets=planet_facets,
            hierarchy_bundle_ids=hierarchy_bundle_ids,
            compact_seed_ids=compact_seed_ids,
            full_scene_ids=full_scene_ids,
        )
        phase("systems", began, system_count)

        began = time.perf_counter()
        star_count = insert_stars(source, target, sample_limit=args.sample_limit)
        phase("stars", began, star_count)

        began = time.perf_counter()
        stellar_badge_overlay_count, stellar_badge_overlay_system_count = (
            insert_stellar_badge_overlays(
                source,
                target,
                sample_limit=args.sample_limit,
            )
        )
        phase(
            "stellar_badge_overlays",
            began,
            stellar_badge_overlay_count,
        )

        began = time.perf_counter()
        planet_count = insert_planets(source, target, sample_limit=args.sample_limit)
        phase("planets", began, planet_count)

        began = time.perf_counter()
        alias_count, term_count = insert_aliases_and_terms(
            source, target, sample_limit=args.sample_limit
        )
        phase("aliases_and_search_terms", began, alias_count + term_count)

        began = time.perf_counter()
        exact_identifier_count = insert_exact_identifiers(
            source, target, sample_limit=args.sample_limit
        )
        phase("exact_identifiers", began, exact_identifier_count)

        began = time.perf_counter()
        identifier_count = insert_identifier_outcomes(
            source, target, sample_limit=args.sample_limit
        )
        phase("identifier_outcomes", began, identifier_count)

        began = time.perf_counter()
        quarantine_count = insert_identifier_quarantine(source, target)
        phase("identifier_quarantine", began, quarantine_count)

        began = time.perf_counter()
        seed_count = insert_singleton_seeds(target, policy)
        phase("singleton_scene_seeds", began, seed_count)

        began = time.perf_counter()
        create_indexes(target)
        target.execute("ANALYZE")
        target.commit()
        phase("indexes_and_fts", began)

        expected = {
            "systems": system_count,
            "stars": star_count,
            "stellar_badge_overlays": stellar_badge_overlay_count,
            "planets": planet_count,
            "aliases": alias_count,
            "search_terms": term_count,
            "exact_identifiers": exact_identifier_count,
            "identifier_outcomes": identifier_count,
            "identifier_quarantine": quarantine_count,
            "singleton_scene_seeds": seed_count,
            "hierarchy_bundles": 0,
        }
        began = time.perf_counter()
        verification = verify(target, expected=expected)
        phase("verification", began)
        if verification["status"] != "pass":
            raise RuntimeError(canonical_json(verification))

        accounting = source_accounting(source, sample_limit=args.sample_limit)
        logical_hashes = {
            "metadata": logical_digest(target, "metadata", ["key", "value"]),
            "systems": logical_digest(
                target,
                "systems",
                ["system_id", "stable_object_key", "system_name_norm", "star_count", "planet_count"],
            ),
            "stars": logical_digest(
                target,
                "stars",
                ["star_id", "system_id", "stable_object_key", "selected_classification"],
            ),
            "stellar_badge_overlays": logical_digest(
                target,
                "stellar_badge_overlays",
                [
                    "system_id",
                    "badge_order",
                    "leaf_component_key",
                    "classification_value",
                ],
            ),
            "search_terms": logical_digest(
                target,
                "search_terms",
                ["search_term_id", "system_id", "term_norm", "target_type"],
            ),
        }
        target.execute("VACUUM")
        target.close()
        target = None

        artifact_sha256 = sha256_file(temporary_db)
        manifest = {
            "schema_version": "spacegate.public_read_manifest.v1",
            "status": "pass",
            "build_id": build_id,
            "builder_version": BUILDER_VERSION,
            "projection_schema_version": policy["projection_schema_version"],
            "search_schema_version": policy["search_schema_version"],
            "policy": {
                "path": str(policy_path.relative_to(ROOT)),
                "sha256": sha256_file(policy_path),
            },
            "artifact": {
                "path": final_db.name,
                "sha256": artifact_sha256,
                "bytes": temporary_db.stat().st_size,
                "sqlite_version": sqlite3.sqlite_version,
            },
            "source_artifacts": {
                "core": {"sha256": sha256_file(core_path), "bytes": core_path.stat().st_size},
                "arm": {"sha256": sha256_file(arm_path), "bytes": arm_path.stat().st_size},
                "disc": (
                    {"sha256": sha256_file(disc_path), "bytes": disc_path.stat().st_size}
                    if disc_path.is_file()
                    else None
                ),
            },
            "counts": verification["counts"],
            "representation_counts": representation_counts,
            "stellar_badge_overlay_system_count": stellar_badge_overlay_system_count,
            "source_accounting": accounting,
            "logical_hashes": logical_hashes,
            "verification": verification,
            "phase_timings": phase_timings,
            "sample_limit": args.sample_limit,
            "wall_seconds": round(time.perf_counter() - started, 6),
            "generated_at_utc": utc_now(),
        }
        atomic_write_json(temporary_root / "manifest.json", manifest)
        atomic_write_json(temporary_root / "verification.json", verification)

        output_root.parent.mkdir(parents=True, exist_ok=True)
        if output_root.exists():
            existing_manifest = output_root / "manifest.json"
            if not args.replace:
                if (
                    existing_manifest.is_file()
                    and load_json(existing_manifest).get("artifact", {}).get("sha256")
                    == artifact_sha256
                ):
                    shutil.rmtree(temporary_root)
                    return load_json(existing_manifest)
                raise FileExistsError(f"output exists; use --replace: {output_root}")
            shutil.rmtree(output_root)
        os.replace(temporary_root, output_root)
        return manifest
    finally:
        if target is not None:
            target.close()
        source.close()
        if temporary_root.exists():
            shutil.rmtree(temporary_root)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--build-dir", required=True)
    value.add_argument(
        "--state-dir",
        default=os.getenv("SPACEGATE_STATE_DIR", "/data/spacegate/state"),
    )
    value.add_argument("--output-dir")
    value.add_argument("--policy", default=str(DEFAULT_POLICY))
    value.add_argument("--sample-limit", type=int)
    value.add_argument("--replace", action="store_true")
    return value


def main() -> int:
    args = parser().parse_args()
    if args.sample_limit is not None and args.sample_limit < 1:
        raise SystemExit("--sample-limit must be positive")
    manifest = compile_projection(args)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
