import json
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from .utils import row_to_dict


PROVENANCE_FIELDS = [
    "source_catalog",
    "source_version",
    "source_url",
    "source_download_url",
    "source_doi",
    "source_pk",
    "source_row_id",
    "source_row_hash",
    "license",
    "redistribution_ok",
    "license_note",
    "retrieval_etag",
    "retrieval_checksum",
    "retrieved_at",
    "ingested_at",
    "transform_version",
]


def split_provenance(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    provenance = {field: row.get(field) for field in PROVENANCE_FIELDS}
    data = {k: v for k, v in row.items() if k not in PROVENANCE_FIELDS}
    return data, provenance


def _parse_catalog_ids(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return None


def _attach_rich_db(
    con: duckdb.DuckDBPyConnection,
    rich_db_path: Optional[str],
    *,
    alias: str = "rich_db",
) -> bool:
    if not rich_db_path:
        return False
    try:
        attached = {str(row[1]) for row in con.execute("PRAGMA database_list").fetchall()}
        if alias in attached:
            return True
    except Exception:
        pass
    try:
        rich_path_sql = str(rich_db_path).replace("'", "''")
        con.execute(f"ATTACH '{rich_path_sql}' AS {alias} (READ_ONLY)")
        return True
    except Exception:
        return False


def _has_table(
    con: duckdb.DuckDBPyConnection,
    *,
    alias: str,
    table_name: str,
) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_catalog = ?
              AND table_schema = 'main'
              AND table_name = ?
            LIMIT 1
            """,
            [alias, table_name],
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def fetch_build_id(con: duckdb.DuckDBPyConnection) -> Optional[str]:
    try:
        row = con.execute(
            "SELECT value FROM build_metadata WHERE key = 'build_id'"
        ).fetchone()
        if not row:
            return None
        return row[0]
    except Exception:
        return None


def fetch_spectral_mix(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    rows = con.execute(
        """
        WITH spectral_buckets AS (
          SELECT
            CASE
              WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE 'D%' THEN 'D'
              WHEN spectral_class IN ('O', 'B', 'A', 'F', 'G', 'K', 'M', 'L', 'T', 'Y') THEN spectral_class
              ELSE 'unknown'
            END AS spectral_bucket
          FROM stars
        )
        SELECT spectral_bucket, COUNT(*)::BIGINT AS star_count
        FROM spectral_buckets
        GROUP BY spectral_bucket
        ORDER BY
          CASE spectral_bucket
            WHEN 'O' THEN 1
            WHEN 'B' THEN 2
            WHEN 'A' THEN 3
            WHEN 'F' THEN 4
            WHEN 'G' THEN 5
            WHEN 'K' THEN 6
            WHEN 'M' THEN 7
            WHEN 'L' THEN 8
            WHEN 'T' THEN 9
            WHEN 'Y' THEN 10
            WHEN 'D' THEN 11
            ELSE 12
          END,
          spectral_bucket
        """
    ).fetchall()
    total = sum(int(row[1] or 0) for row in rows)
    return {
        "total_stars": int(total),
        "rows": [
            {
                "spectral_class": str(row[0]),
                "star_count": int(row[1] or 0),
                "pct_of_stars": (float(row[1]) / float(total) * 100.0) if total else 0.0,
            }
            for row in rows
        ],
    }


def fetch_system_by_id(con: duckdb.DuckDBPyConnection, system_id: int) -> Optional[Dict[str, Any]]:
    cursor = con.execute("SELECT * FROM systems WHERE system_id = ?", [system_id])
    row = cursor.fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in cursor.description]
    data = row_to_dict(columns, row)
    payload, provenance = split_provenance(data)
    payload["provenance"] = provenance
    return payload


def fetch_system_by_key(
    con: duckdb.DuckDBPyConnection, stable_object_key: str
) -> Optional[Dict[str, Any]]:
    cursor = con.execute(
        "SELECT * FROM systems WHERE stable_object_key = ?", [stable_object_key]
    )
    row = cursor.fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in cursor.description]
    data = row_to_dict(columns, row)
    payload, provenance = split_provenance(data)
    payload["provenance"] = provenance
    return payload


def fetch_stars_for_system(
    con: duckdb.DuckDBPyConnection, system_id: int
) -> List[Dict[str, Any]]:
    cursor = con.execute(
        """
        SELECT *
        FROM stars
        WHERE system_id = ?
        ORDER BY component ASC NULLS LAST, star_name_norm ASC NULLS LAST, star_id ASC
        """,
        [system_id],
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    results: List[Dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(columns, row)
        catalog_ids = _parse_catalog_ids(data.pop("catalog_ids_json", None))
        payload, provenance = split_provenance(data)
        payload["catalog_ids"] = catalog_ids
        payload["provenance"] = provenance
        results.append(payload)
    return results


def fetch_planets_for_system(
    con: duckdb.DuckDBPyConnection, system_id: int
) -> List[Dict[str, Any]]:
    cursor = con.execute(
        """
        SELECT *
        FROM planets
        WHERE system_id = ?
        ORDER BY planet_name_norm ASC NULLS LAST, planet_id ASC
        """,
        [system_id],
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    results: List[Dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(columns, row)
        payload, provenance = split_provenance(data)
        payload["provenance"] = provenance
        results.append(payload)
    return results


def fetch_counts_for_system(
    con: duckdb.DuckDBPyConnection, system_id: int
) -> Tuple[int, int]:
    star_count = con.execute(
        "SELECT COUNT(*) FROM stars WHERE system_id = ?", [system_id]
    ).fetchone()[0]
    planet_count = con.execute(
        "SELECT COUNT(*) FROM planets WHERE system_id = ?", [system_id]
    ).fetchone()[0]
    return int(star_count), int(planet_count)


def fetch_snapshot_for_system(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    stable_object_key: Optional[str],
    rich_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not _attach_rich_db(con, rich_db_path, alias="rich_snap"):
        return None
    if not _has_table(con, alias="rich_snap", table_name="snapshot_manifest"):
        return None
    row = con.execute(
        """
        SELECT
          sm.build_id AS snapshot_build_id,
          sm.view_type AS snapshot_view_type,
          sm.artifact_path AS snapshot_artifact_path,
          sm.params_hash AS snapshot_params_hash,
          sm.width_px AS snapshot_width_px,
          sm.height_px AS snapshot_height_px
        FROM rich_snap.snapshot_manifest sm
        JOIN (
          SELECT value AS build_id
          FROM build_metadata
          WHERE key = 'build_id'
          LIMIT 1
        ) b ON sm.build_id = b.build_id
        WHERE sm.object_type = 'system'
          AND sm.view_type IN ('system_card', 'system')
          AND (sm.system_id = ? OR sm.stable_object_key = ?)
        ORDER BY
          CASE WHEN sm.view_type = 'system_card' THEN 0 ELSE 1 END ASC,
          sm.created_at DESC
        LIMIT 1
        """,
        [system_id, stable_object_key],
    ).fetchone()
    if not row:
        return None
    return {
        "build_id": row[0],
        "view_type": row[1],
        "artifact_path": row[2],
        "params_hash": row[3],
        "width_px": int(row[4]) if row[4] is not None else None,
        "height_px": int(row[5]) if row[5] is not None else None,
    }


def search_systems(
    con: duckdb.DuckDBPyConnection,
    *,
    q_norm: Optional[str],
    q_raw: Optional[str],
    system_id_exact: Optional[int],
    id_query: Optional[Dict[str, Any]],
    max_dist_ly: Optional[float],
    min_dist_ly: Optional[float],
    min_star_count: Optional[int],
    max_star_count: Optional[int],
    min_planet_count: Optional[int],
    max_planet_count: Optional[int],
    spectral_classes: List[str],
    has_planets: Optional[bool],
    has_habitable: Optional[bool],
    min_coolness_score: Optional[float],
    max_coolness_score: Optional[float],
    sort: str,
    match_mode: bool,
    limit: int,
    include_total: bool,
    cursor_values: Optional[Dict[str, Any]],
    rich_db_path: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    conditions: List[str] = []
    params: List[Any] = []

    match_rank_expr = "0 AS match_rank"
    match_params: List[Any] = []
    match_clauses: List[str] = []

    if q_norm:
        short_query_mode = len(q_norm) < 2
        exact_parts = ["s.system_name_norm = ?", "s.stable_object_key = ?"]
        match_params.extend([q_norm, q_raw])
        if not short_query_mode:
            exact_parts.append(
                "EXISTS (SELECT 1 FROM stars st WHERE st.system_id = s.system_id "
                "AND st.star_name_norm = ?)"
            )
            match_params.append(q_norm)
        exact_clause = "(" + " OR ".join(exact_parts) + ")"

        prefix_parts = ["s.system_name_norm LIKE ?"]
        prefix_pattern = f"{q_norm}%"
        match_params.append(prefix_pattern)
        if not short_query_mode:
            prefix_parts.append(
                "EXISTS (SELECT 1 FROM stars st WHERE st.system_id = s.system_id "
                "AND st.star_name_norm LIKE ?)"
            )
            match_params.append(prefix_pattern)
        prefix_clause = "(" + " OR ".join(prefix_parts) + ")"

        tokens = [token for token in q_norm.split(" ") if token]
        token_clauses: List[str] = []
        for token in tokens:
            if short_query_mode or len(token) < 2:
                continue
            token_clause = (
                "(s.system_name_norm LIKE ? OR EXISTS (SELECT 1 FROM stars st "
                "WHERE st.system_id = s.system_id AND st.star_name_norm LIKE ?))"
            )
            token_clauses.append(token_clause)
            token_pattern = f"%{token}%"
            match_params.extend([token_pattern, token_pattern])
        token_and_clause = " AND ".join(token_clauses) if token_clauses else None

        match_lines: List[str] = [f"WHEN {exact_clause} THEN 0", f"WHEN {prefix_clause} THEN 1"]
        match_clauses.extend([exact_clause, prefix_clause])
        if short_query_mode:
            contains_clause = "(s.system_name_norm LIKE ?)"
            match_lines.append(f"WHEN {contains_clause} THEN 2")
            match_clauses.append(contains_clause)
            match_params.append(f"%{q_norm}%")
        if token_and_clause:
            match_lines.append(f"WHEN {token_and_clause} THEN 2")
            match_clauses.append(f"({token_and_clause})")

        if id_query:
            column = {
                "hd": "hd_id",
                "hip": "hip_id",
                "gaia": "gaia_id",
            }.get(id_query.get("kind"))
            if column:
                id_clause = (
                    f"(s.{column} = ? OR EXISTS (SELECT 1 FROM stars st "
                    f"WHERE st.system_id = s.system_id AND st.{column} = ?))"
                )
                match_lines.append(f"WHEN {id_clause} THEN 3")
                match_clauses.append(id_clause)
                match_params.extend([id_query.get("value"), id_query.get("value")])
            elif id_query.get("kind") == "catalog_numeric":
                id_clause = (
                    "(s.hip_id = ? OR EXISTS (SELECT 1 FROM stars st "
                    "WHERE st.system_id = s.system_id AND st.hip_id = ?) "
                    "OR s.hd_id = ? OR EXISTS (SELECT 1 FROM stars st "
                    "WHERE st.system_id = s.system_id AND st.hd_id = ?))"
                )
                match_lines.append(f"WHEN {id_clause} THEN 3")
                match_clauses.append(id_clause)
                value = id_query.get("value")
                match_params.extend([value, value, value, value])

        match_rank_expr = "CASE " + " ".join(match_lines) + " ELSE NULL END AS match_rank"

    if system_id_exact is not None:
        conditions.append("s.system_id = ?")
        params.append(system_id_exact)

    if max_dist_ly is not None:
        conditions.append("s.dist_ly <= ?")
        params.append(max_dist_ly)

    if min_dist_ly is not None:
        conditions.append("s.dist_ly >= ?")
        params.append(min_dist_ly)

    if spectral_classes:
        spectral_filters: List[str] = []
        for token in spectral_classes:
            if token == "D":
                spectral_filters.append(
                    "(st.spectral_class = ? OR UPPER(COALESCE(st.spectral_type_raw, '')) LIKE 'D%')"
                )
                params.append("D")
            else:
                spectral_filters.append("st.spectral_class = ?")
                params.append(token)
        conditions.append(
            "EXISTS (SELECT 1 FROM stars st WHERE st.system_id = s.system_id "
            f"AND ({' OR '.join(spectral_filters)}))"
        )

    if has_planets is True:
        conditions.append(
            "EXISTS (SELECT 1 FROM planets p WHERE p.system_id = s.system_id)"
        )
    elif has_planets is False:
        conditions.append(
            "NOT EXISTS (SELECT 1 FROM planets p WHERE p.system_id = s.system_id)"
        )

    if min_star_count is not None:
        conditions.append(
            "(SELECT COUNT(*) FROM stars st WHERE st.system_id = s.system_id) >= ?"
        )
        params.append(min_star_count)

    if max_star_count is not None:
        conditions.append(
            "(SELECT COUNT(*) FROM stars st WHERE st.system_id = s.system_id) <= ?"
        )
        params.append(max_star_count)

    if min_planet_count is not None:
        conditions.append(
            "(SELECT COUNT(*) FROM planets p WHERE p.system_id = s.system_id) >= ?"
        )
        params.append(min_planet_count)

    if max_planet_count is not None:
        conditions.append(
            "(SELECT COUNT(*) FROM planets p WHERE p.system_id = s.system_id) <= ?"
        )
        params.append(max_planet_count)

    habitability_clause = (
        "EXISTS (SELECT 1 FROM planets p WHERE p.system_id = s.system_id "
        "AND COALESCE(p.match_confidence, 0.0) >= 0.80 "
        "AND COALESCE(p.eq_temp_k, -1.0) BETWEEN 180.0 AND 350.0 "
        "AND COALESCE(p.mass_earth, p.mass_jup * 317.8, -1.0) BETWEEN 0.3 AND 8.0 "
        "AND COALESCE(p.eccentricity, 0.0) <= 0.35)"
    )
    if has_habitable is True:
        conditions.append(habitability_clause)
    elif has_habitable is False:
        conditions.append(f"NOT {habitability_clause}")

    rich_attached = _attach_rich_db(con, rich_db_path, alias="rich_db")
    has_coolness_scores = rich_attached and _has_table(
        con,
        alias="rich_db",
        table_name="coolness_scores",
    )
    has_snapshot_manifest = rich_attached and _has_table(
        con,
        alias="rich_db",
        table_name="snapshot_manifest",
    )
    if sort == "coolness" and not match_mode and not has_coolness_scores:
        raise ValueError(
            "Coolness sort is unavailable for this build; run score_coolness first."
        )

    coolness_filters_requested = (
        min_coolness_score is not None or max_coolness_score is not None
    )
    if coolness_filters_requested and not has_coolness_scores:
        raise ValueError(
            "Coolness filters are unavailable for this build; run score_coolness first."
        )
    if min_coolness_score is not None:
        conditions.append("COALESCE(c.coolness_score, -1e18) >= ?")
        params.append(min_coolness_score)
    if max_coolness_score is not None:
        conditions.append("COALESCE(c.coolness_score, 1e18) <= ?")
        params.append(max_coolness_score)

    use_coolness_sort = sort == "coolness" and not match_mode and has_coolness_scores

    order_by = "system_name_norm ASC NULLS LAST, system_id ASC"
    cursor_clause = ""
    filtered_clause = ""
    cursor_params: List[Any] = []

    if match_mode:
        order_by = (
            "match_rank ASC, COALESCE(dist_ly, 1e12) ASC, "
            "COALESCE(system_name_norm, '') ASC, system_id ASC"
        )
        if cursor_values:
            cursor_match = cursor_values.get("match_rank", 0)
            cursor_dist = cursor_values.get("dist")
            if cursor_dist is None:
                cursor_dist = 1e12
            cursor_name = cursor_values.get("name", "")
            cursor_id = cursor_values.get("id")
            cursor_clause = (
                "(match_rank > ? OR "
                "(match_rank = ? AND COALESCE(dist_ly, 1e12) > ?) OR "
                "(match_rank = ? AND COALESCE(dist_ly, 1e12) = ? AND "
                "COALESCE(system_name_norm, '') > ?) OR "
                "(match_rank = ? AND COALESCE(dist_ly, 1e12) = ? AND "
                "COALESCE(system_name_norm, '') = ? AND system_id > ?))"
            )
            cursor_params.extend(
                [
                    cursor_match,
                    cursor_match,
                    cursor_dist,
                    cursor_match,
                    cursor_dist,
                    cursor_name,
                    cursor_match,
                    cursor_dist,
                    cursor_name,
                    cursor_id,
                ]
            )
        filtered_clause = "WHERE match_rank IS NOT NULL"
    else:
        if sort == "distance":
            order_by = "dist_ly ASC NULLS LAST, system_id ASC"
            if cursor_values:
                cursor_dist = cursor_values.get("dist")
                if cursor_dist is None:
                    cursor_dist = 1e12
                cursor_clause = (
                    "(COALESCE(dist_ly, 1e12) > ? OR "
                    "(COALESCE(dist_ly, 1e12) = ? AND system_id > ?))"
                )
                cursor_params.extend(
                    [
                        cursor_dist,
                        cursor_dist,
                        cursor_values.get("id"),
                    ]
                )
        elif sort == "coolness" and use_coolness_sort:
            order_by = (
                "COALESCE(coolness_rank, 9223372036854775807) ASC, "
                "COALESCE(system_name_norm, '') ASC, system_id ASC"
            )
            if cursor_values:
                cursor_rank = cursor_values.get("cool_rank")
                if cursor_rank is None:
                    cursor_rank = 9223372036854775807
                cursor_clause = (
                    "(COALESCE(coolness_rank, 9223372036854775807) > ? OR "
                    "(COALESCE(coolness_rank, 9223372036854775807) = ? AND COALESCE(system_name_norm, '') > ?) OR "
                    "(COALESCE(coolness_rank, 9223372036854775807) = ? AND COALESCE(system_name_norm, '') = ? AND system_id > ?))"
                )
                cursor_params.extend(
                    [
                        cursor_rank,
                        cursor_rank,
                        cursor_values.get("name", ""),
                        cursor_rank,
                        cursor_values.get("name", ""),
                        cursor_values.get("id"),
                    ]
                )
        else:
            if cursor_values:
                cursor_clause = (
                    "(COALESCE(system_name_norm, '') > ? OR "
                    "(COALESCE(system_name_norm, '') = ? AND system_id > ?))"
                )
                cursor_params.extend(
                    [
                        cursor_values.get("name", ""),
                        cursor_values.get("name", ""),
                        cursor_values.get("id"),
                    ]
                )

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)
    paged_where = f"WHERE {cursor_clause}" if cursor_clause else ""
    use_coolness_in_sql = has_coolness_scores and (use_coolness_sort or coolness_filters_requested)
    coolness_cte = ""
    coolness_join = ""
    coolness_select = (
        "NULL::BIGINT AS coolness_rank, "
        "NULL::DOUBLE AS coolness_score, "
        "NULL::BIGINT AS coolness_nice_planet_count, "
        "NULL::BIGINT AS coolness_weird_planet_count, "
        "NULL::VARCHAR AS coolness_dominant_spectral_class, "
        "NULL::DOUBLE AS coolness_score_luminosity, "
        "NULL::DOUBLE AS coolness_score_proper_motion, "
        "NULL::DOUBLE AS coolness_score_multiplicity, "
        "NULL::DOUBLE AS coolness_score_nice_planets, "
        "NULL::DOUBLE AS coolness_score_weird_planets, "
        "NULL::DOUBLE AS coolness_score_proximity, "
        "NULL::DOUBLE AS coolness_score_system_complexity, "
        "NULL::DOUBLE AS coolness_score_exotic_star,"
    )
    if use_coolness_in_sql:
        coolness_cte = """
        coolness AS (
            SELECT
                system_id,
                rank AS coolness_rank,
                score_total AS coolness_score,
                nice_planet_count AS coolness_nice_planet_count,
                weird_planet_count AS coolness_weird_planet_count,
                dominant_spectral_class AS coolness_dominant_spectral_class,
                score_luminosity AS coolness_score_luminosity,
                score_proper_motion AS coolness_score_proper_motion,
                score_multiplicity AS coolness_score_multiplicity,
                score_nice_planets AS coolness_score_nice_planets,
                score_weird_planets AS coolness_score_weird_planets,
                score_proximity AS coolness_score_proximity,
                score_system_complexity AS coolness_score_system_complexity,
                score_exotic_star AS coolness_score_exotic_star
            FROM rich_db.coolness_scores
        ),
        """
        coolness_join = "LEFT JOIN coolness c ON c.system_id = s.system_id"
        coolness_select = (
            "c.coolness_rank, "
            "c.coolness_score, "
            "c.coolness_nice_planet_count, "
            "c.coolness_weird_planet_count, "
            "c.coolness_dominant_spectral_class, "
            "c.coolness_score_luminosity, "
            "c.coolness_score_proper_motion, "
            "c.coolness_score_multiplicity, "
            "c.coolness_score_nice_planets, "
            "c.coolness_score_weird_planets, "
            "c.coolness_score_proximity, "
            "c.coolness_score_system_complexity, "
            "c.coolness_score_exotic_star,"
        )

    base_cte = f"""
        WITH
        {coolness_cte}
        base AS (
            SELECT
                s.*,
                CAST(s.gaia_id AS VARCHAR) AS gaia_id_text,
                CAST(s.hip_id AS VARCHAR) AS hip_id_text,
                CAST(s.hd_id AS VARCHAR) AS hd_id_text,
                {coolness_select}
                {match_rank_expr}
            FROM systems s
            {coolness_join}
            {where_sql}
        ),
        filtered AS (
            SELECT * FROM base
            {filtered_clause}
        )
    """

    sql = f"""
        {base_cte}
        SELECT *
        FROM filtered
        {paged_where}
        ORDER BY {order_by}
        LIMIT ?
    """
    all_params = match_params + params + cursor_params + [limit]
    cursor = con.execute(sql, all_params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    results: List[Dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(columns, row)
        payload, provenance = split_provenance(data)
        payload["spectral_classes"] = []
        payload["star_count"] = 0
        payload["planet_count"] = 0
        if payload.get("coolness_nice_planet_count") is not None:
            payload["coolness_nice_planet_count"] = int(payload["coolness_nice_planet_count"])
        if payload.get("coolness_weird_planet_count") is not None:
            payload["coolness_weird_planet_count"] = int(payload["coolness_weird_planet_count"])
        payload["snapshot"] = None
        payload["provenance"] = provenance
        results.append(payload)

    system_ids: List[int] = [int(item["system_id"]) for item in results if item.get("system_id") is not None]
    if system_ids:
        placeholders = ",".join(["?"] * len(system_ids))

        star_rows = con.execute(
            f"""
            SELECT
              system_id,
              COUNT(*)::BIGINT AS star_count,
              ARRAY_AGG(DISTINCT spectral_class) FILTER (WHERE spectral_class IS NOT NULL) AS spectral_classes
            FROM stars
            WHERE system_id IN ({placeholders})
            GROUP BY system_id
            """,
            system_ids,
        ).fetchall()
        star_map: Dict[int, Tuple[int, List[str]]] = {}
        for sid, count, spectral in star_rows:
            star_map[int(sid)] = (
                int(count or 0),
                [token for token in (spectral or []) if token],
            )

        planet_rows = con.execute(
            f"""
            SELECT system_id, COUNT(*)::BIGINT AS planet_count
            FROM planets
            WHERE system_id IN ({placeholders})
            GROUP BY system_id
            """,
            system_ids,
        ).fetchall()
        planet_map: Dict[int, int] = {int(sid): int(count or 0) for sid, count in planet_rows}

        for item in results:
            sid = int(item.get("system_id") or 0)
            star_count, spectral_classes = star_map.get(sid, (0, []))
            item["star_count"] = star_count
            item["spectral_classes"] = spectral_classes
            item["planet_count"] = planet_map.get(sid, 0)

        if has_coolness_scores and not use_coolness_in_sql:
            coolness_rows = con.execute(
                f"""
                SELECT
                  system_id,
                  rank AS coolness_rank,
                  score_total AS coolness_score,
                  nice_planet_count AS coolness_nice_planet_count,
                  weird_planet_count AS coolness_weird_planet_count,
                  dominant_spectral_class AS coolness_dominant_spectral_class,
                  score_luminosity AS coolness_score_luminosity,
                  score_proper_motion AS coolness_score_proper_motion,
                  score_multiplicity AS coolness_score_multiplicity,
                  score_nice_planets AS coolness_score_nice_planets,
                  score_weird_planets AS coolness_score_weird_planets,
                  score_proximity AS coolness_score_proximity,
                  score_system_complexity AS coolness_score_system_complexity,
                  score_exotic_star AS coolness_score_exotic_star
                FROM rich_db.coolness_scores
                WHERE system_id IN ({placeholders})
                """,
                system_ids,
            ).fetchall()
            coolness_map: Dict[int, Dict[str, Any]] = {}
            for row in coolness_rows:
                coolness_map[int(row[0])] = {
                    "coolness_rank": row[1],
                    "coolness_score": row[2],
                    "coolness_nice_planet_count": row[3],
                    "coolness_weird_planet_count": row[4],
                    "coolness_dominant_spectral_class": row[5],
                    "coolness_score_luminosity": row[6],
                    "coolness_score_proper_motion": row[7],
                    "coolness_score_multiplicity": row[8],
                    "coolness_score_nice_planets": row[9],
                    "coolness_score_weird_planets": row[10],
                    "coolness_score_proximity": row[11],
                    "coolness_score_system_complexity": row[12],
                    "coolness_score_exotic_star": row[13],
                }
            for item in results:
                sid = int(item.get("system_id") or 0)
                cool = coolness_map.get(sid)
                if not cool:
                    continue
                for key, value in cool.items():
                    item[key] = value
                if item.get("coolness_nice_planet_count") is not None:
                    item["coolness_nice_planet_count"] = int(item["coolness_nice_planet_count"])
                if item.get("coolness_weird_planet_count") is not None:
                    item["coolness_weird_planet_count"] = int(item["coolness_weird_planet_count"])

        if has_snapshot_manifest:
            snapshot_rows = con.execute(
                f"""
                WITH snapshot_ranked AS (
                    SELECT
                      sm.system_id,
                      sm.build_id AS snapshot_build_id,
                      sm.view_type AS snapshot_view_type,
                      sm.artifact_path AS snapshot_artifact_path,
                      sm.params_hash AS snapshot_params_hash,
                      sm.width_px AS snapshot_width_px,
                      sm.height_px AS snapshot_height_px,
                      ROW_NUMBER() OVER (
                        PARTITION BY sm.system_id
                        ORDER BY
                          CASE WHEN sm.view_type = 'system_card' THEN 0 ELSE 1 END ASC,
                          sm.created_at DESC
                      ) AS snapshot_rn
                    FROM rich_db.snapshot_manifest sm
                    JOIN (
                      SELECT value AS build_id
                      FROM build_metadata
                      WHERE key = 'build_id'
                      LIMIT 1
                    ) b ON sm.build_id = b.build_id
                    WHERE sm.object_type = 'system'
                      AND sm.view_type IN ('system_card', 'system')
                      AND sm.system_id IN ({placeholders})
                )
                SELECT
                  system_id,
                  snapshot_build_id,
                  snapshot_view_type,
                  snapshot_artifact_path,
                  snapshot_params_hash,
                  snapshot_width_px,
                  snapshot_height_px
                FROM snapshot_ranked
                WHERE snapshot_rn = 1
                """,
                system_ids,
            ).fetchall()
            snapshot_map: Dict[int, Dict[str, Any]] = {}
            for row in snapshot_rows:
                snapshot_map[int(row[0])] = {
                    "build_id": row[1],
                    "view_type": row[2],
                    "artifact_path": row[3],
                    "params_hash": row[4],
                    "width_px": int(row[5]) if row[5] is not None else None,
                    "height_px": int(row[6]) if row[6] is not None else None,
                }
            for item in results:
                sid = int(item.get("system_id") or 0)
                item["snapshot"] = snapshot_map.get(sid)

    total_count: Optional[int] = None
    if include_total:
        count_sql = f"""
            {base_cte}
            SELECT COUNT(*)::BIGINT
            FROM filtered
        """
        count_row = con.execute(count_sql, match_params + params).fetchone()
        total_count = int(count_row[0] if count_row else 0)

    return results, total_count
