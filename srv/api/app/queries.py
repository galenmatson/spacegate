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

GAIA_NAME_PREFIXES = ("gaia dr3 ", "gaia ")
CATALOG_NAME_PREFIXES = ("gaia dr3 ", "gaia ", "hip ", "hd ", "hr ", "tyc ", "hyg ", "wds ")
ALIAS_KIND_RANK = {
    "proper_name": 0,
    "member_proper_name": 1,
    "bayer_name": 2,
    "member_bayer_name": 3,
    "flamsteed_name": 4,
    "member_flamsteed_name": 5,
    "gl_id": 6,
    "member_gl_id": 7,
    "hip_id": 8,
    "member_hip_id": 9,
    "hd_id": 10,
    "member_hd_id": 11,
    "hr_id": 12,
    "member_hr_id": 13,
    "tyc_id": 14,
    "member_tyc_id": 15,
    "hyg_id": 16,
    "member_hyg_id": 17,
    "wds_id": 18,
    "member_wds_id": 19,
    "gaia_id": 30,
    "gaia_id_short": 31,
}


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


def _has_local_table(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            LIMIT 1
            """,
            [table_name],
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _clean_name(value: Any) -> str:
    return str(value or "").strip()


def _name_norm(value: Any) -> str:
    return _clean_name(value).lower()


def _is_gaia_placeholder_name(value: Any) -> bool:
    norm = _name_norm(value)
    return any(norm.startswith(prefix) for prefix in GAIA_NAME_PREFIXES)


def _is_catalog_style_name(value: Any) -> bool:
    norm = _name_norm(value)
    return any(norm.startswith(prefix) for prefix in CATALOG_NAME_PREFIXES)


def _alias_rank(row: Dict[str, Any]) -> Tuple[int, int, int, str]:
    kind = str(row.get("alias_kind") or "").strip()
    kind_rank = ALIAS_KIND_RANK.get(kind, 99)
    try:
        priority = int(row.get("alias_priority"))
    except Exception:
        priority = 999
    raw = _clean_name(row.get("alias_raw"))
    return (kind_rank, priority, len(raw), raw.lower())


def choose_display_name(
    canonical_name: Any,
    aliases: List[Dict[str, Any]],
    *,
    alt_limit: int = 8,
) -> Tuple[str, List[str]]:
    canonical = _clean_name(canonical_name)
    alias_rows = [row for row in aliases if _clean_name(row.get("alias_raw"))]
    alias_rows.sort(key=_alias_rank)

    ordered_names: List[str] = []
    seen_norm: set[str] = set()
    for row in alias_rows:
        raw = _clean_name(row.get("alias_raw"))
        norm = raw.lower()
        if norm in seen_norm:
            continue
        seen_norm.add(norm)
        ordered_names.append(raw)

    display_name = canonical
    if not display_name or _is_gaia_placeholder_name(display_name):
        for candidate in ordered_names:
            if _is_gaia_placeholder_name(candidate):
                continue
            display_name = candidate
            break
        if not display_name and ordered_names:
            display_name = ordered_names[0]

    secondary: List[str] = []
    secondary_seen: set[str] = set()
    display_norm = _name_norm(display_name)
    if canonical and canonical.lower() != display_norm and not _is_gaia_placeholder_name(canonical):
        secondary.append(canonical)
        secondary_seen.add(canonical.lower())
    for candidate in ordered_names:
        norm = candidate.lower()
        if norm == display_norm or norm in secondary_seen:
            continue
        if _is_gaia_placeholder_name(candidate):
            continue
        secondary.append(candidate)
        secondary_seen.add(norm)
        if len(secondary) >= max(1, alt_limit):
            break
    return display_name, secondary


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
              WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE 'D%'
                OR COALESCE(object_type, '') = 'white_dwarf'
              THEN 'D'
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


def fetch_aliases_for_system(
    con: duckdb.DuckDBPyConnection,
    system_id: int,
    *,
    limit: int = 128,
) -> List[Dict[str, Any]]:
    if not _has_local_table(con, "aliases"):
        return []
    cursor = con.execute(
        """
        SELECT alias_raw, alias_norm, alias_kind, alias_priority, is_primary, source_catalog, source_version
        FROM aliases
        WHERE target_type = 'system'
          AND system_id = ?
        ORDER BY alias_priority ASC, alias_kind ASC, alias_raw ASC
        LIMIT ?
        """,
        [system_id, limit],
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [row_to_dict(columns, row) for row in rows]


def fetch_aliases_for_stars(
    con: duckdb.DuckDBPyConnection,
    star_ids: List[int],
    *,
    per_star_limit: int = 24,
) -> Dict[int, List[Dict[str, Any]]]:
    if not star_ids or not _has_local_table(con, "aliases"):
        return {}
    placeholders = ",".join(["?"] * len(star_ids))
    cursor = con.execute(
        f"""
        WITH ranked AS (
          SELECT
            star_id,
            alias_raw,
            alias_norm,
            alias_kind,
            alias_priority,
            is_primary,
            source_catalog,
            source_version,
            row_number() OVER (
              PARTITION BY star_id
              ORDER BY alias_priority ASC, alias_kind ASC, alias_raw ASC
            ) AS rn
          FROM aliases
          WHERE target_type = 'star'
            AND star_id IN ({placeholders})
        )
        SELECT
          star_id,
          alias_raw,
          alias_norm,
          alias_kind,
          alias_priority,
          is_primary,
          source_catalog,
          source_version
        FROM ranked
        WHERE rn <= ?
        ORDER BY star_id ASC, alias_priority ASC, alias_kind ASC, alias_raw ASC
        """,
        [*star_ids, per_star_limit],
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        item = row_to_dict(columns, row)
        sid_raw = item.pop("star_id", None)
        if sid_raw is None:
            continue
        sid = int(sid_raw)
        grouped.setdefault(sid, []).append(item)
    return grouped


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
    alias_cte_parts: List[str] = []
    alias_cte_params: List[Any] = []
    identifier_cte_parts: List[str] = []
    identifier_cte_params: List[Any] = []
    has_aliases = _has_local_table(con, "aliases")

    if q_norm:
        short_query_mode = len(q_norm) < 2
        id_clause = None
        if id_query:
            column = {
                "hd": "hd_id",
                "hip": "hip_id",
                "gaia": "gaia_id",
            }.get(id_query.get("kind"))
            if column:
                id_clause = "s.system_id IN (SELECT system_id FROM id_match)"
                identifier_cte_parts.append(
                    f"""
                    id_match AS (
                        SELECT system_id
                        FROM systems
                        WHERE {column} = ?
                        UNION
                        SELECT system_id
                        FROM stars
                        WHERE {column} = ?
                    )
                    """
                )
                identifier_cte_params.extend([id_query.get("value"), id_query.get("value")])
            elif id_query.get("kind") == "catalog_numeric":
                id_clause = "s.system_id IN (SELECT system_id FROM id_match)"
                identifier_cte_parts.append(
                    """
                    id_match AS (
                        SELECT system_id
                        FROM systems
                        WHERE hip_id = ?
                        UNION
                        SELECT system_id
                        FROM stars
                        WHERE hip_id = ?
                        UNION
                        SELECT system_id
                        FROM systems
                        WHERE hd_id = ?
                        UNION
                        SELECT system_id
                        FROM stars
                        WHERE hd_id = ?
                    )
                    """
                )
                value = id_query.get("value")
                identifier_cte_params.extend([value, value, value, value])
        identifier_mode = id_clause is not None
        enable_alias_match = has_aliases and not short_query_mode and not identifier_mode

        exact_parts = ["s.system_name_norm = ?", "s.stable_object_key = ?"]
        exact_params: List[Any] = [q_norm, q_raw]
        if enable_alias_match:
            alias_cte_parts.append(
                """
                alias_match_exact AS (
                    SELECT DISTINCT system_id
                    FROM aliases
                    WHERE target_type = 'system'
                      AND alias_norm = ?
                )
                """
            )
            alias_cte_params.append(q_norm)
            exact_parts.append("s.system_id IN (SELECT system_id FROM alias_match_exact)")
        exact_clause = "(" + " OR ".join(exact_parts) + ")"

        prefix_parts = ["s.system_name_norm LIKE ?"]
        prefix_pattern = f"{q_norm}%"
        prefix_params: List[Any] = [prefix_pattern]
        if enable_alias_match:
            alias_cte_parts.append(
                """
                alias_match_prefix AS (
                    SELECT DISTINCT system_id
                    FROM aliases
                    WHERE target_type = 'system'
                      AND alias_norm LIKE ?
                )
                """
            )
            alias_cte_params.append(prefix_pattern)
            prefix_parts.append("s.system_id IN (SELECT system_id FROM alias_match_prefix)")
        prefix_clause = "(" + " OR ".join(prefix_parts) + ")"

        tokens = [token for token in q_norm.split(" ") if token]
        token_clauses: List[str] = []
        token_params: List[Any] = []
        token_idx = 0
        for token in tokens:
            # Identifier-mode queries (HIP/HD/Gaia numeric) don't include token_AND rank clauses.
            # Skip token params here to keep SQL placeholders aligned with bound params.
            if identifier_mode or short_query_mode or len(token) < 2:
                continue
            token_pattern = f"%{token}%"
            token_parts = ["s.system_name_norm LIKE ?"]
            token_params.append(token_pattern)
            if enable_alias_match:
                cte_name = f"alias_match_token_{token_idx}"
                token_idx += 1
                alias_cte_parts.append(
                    f"""
                    {cte_name} AS (
                        SELECT DISTINCT system_id
                        FROM aliases
                        WHERE target_type = 'system'
                          AND alias_norm LIKE ?
                    )
                    """
                )
                alias_cte_params.append(token_pattern)
                token_parts.append(f"s.system_id IN (SELECT system_id FROM {cte_name})")
            token_clauses.append("(" + " OR ".join(token_parts) + ")")
        token_and_clause = " AND ".join(token_clauses) if token_clauses else None

        next_rank = 0
        match_lines: List[str] = []
        if id_clause:
            match_lines.append(f"WHEN {id_clause} THEN {next_rank}")
            match_clauses.append(id_clause)
            next_rank += 1
        if not identifier_mode:
            match_lines.append(f"WHEN {exact_clause} THEN {next_rank}")
            match_clauses.append(exact_clause)
            match_params.extend(exact_params)
            next_rank += 1
            match_lines.append(f"WHEN {prefix_clause} THEN {next_rank}")
            match_clauses.append(prefix_clause)
            match_params.extend(prefix_params)
            next_rank += 1
            if token_and_clause:
                match_lines.append(f"WHEN {token_and_clause} THEN {next_rank}")
                match_clauses.append(f"({token_and_clause})")
                match_params.extend(token_params)

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
                    "("
                    "st.spectral_class = ? OR "
                    "UPPER(COALESCE(st.spectral_type_raw, '')) LIKE 'D%' OR "
                    "COALESCE(st.object_type, '') = 'white_dwarf'"
                    ")"
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
        )
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

    cte_parts: List[str] = []
    if coolness_cte.strip():
        cte_parts.append(coolness_cte.strip())
    cte_parts.extend([part.strip() for part in alias_cte_parts if part.strip()])
    cte_parts.extend([part.strip() for part in identifier_cte_parts if part.strip()])
    cte_parts.append(
        f"""
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
        )
        """.strip()
    )
    cte_parts.append(
        f"""
        filtered AS (
            SELECT * FROM base
            {filtered_clause}
        )
        """.strip()
    )
    base_cte = "WITH\n" + ",\n".join(cte_parts)

    sql = f"""
        {base_cte}
        SELECT *
        FROM filtered
        {paged_where}
        ORDER BY {order_by}
        LIMIT ?
    """
    all_params = alias_cte_params + identifier_cte_params + match_params + params + cursor_params + [limit]
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
        if has_aliases:
            alias_rows = con.execute(
                f"""
                SELECT
                  system_id,
                  alias_raw,
                  alias_kind,
                  alias_priority
                FROM aliases
                WHERE target_type = 'system'
                  AND system_id IN ({placeholders})
                ORDER BY system_id ASC, alias_priority ASC, alias_kind ASC, alias_raw ASC
                """,
                system_ids,
            ).fetchall()
            system_aliases: Dict[int, List[Dict[str, Any]]] = {}
            for system_id, alias_raw, alias_kind, alias_priority in alias_rows:
                sid = int(system_id)
                system_aliases.setdefault(sid, []).append(
                    {
                        "alias_raw": alias_raw,
                        "alias_kind": alias_kind,
                        "alias_priority": alias_priority,
                    }
                )
            for item in results:
                sid = int(item.get("system_id") or 0)
                display_name, display_aliases = choose_display_name(
                    item.get("system_name"),
                    system_aliases.get(sid, []),
                )
                item["display_name"] = display_name
                item["display_aliases"] = display_aliases
        else:
            for item in results:
                display_name = _clean_name(item.get("system_name"))
                item["display_name"] = display_name
                item["display_aliases"] = []

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
        count_row = con.execute(
            count_sql,
            alias_cte_params + identifier_cte_params + match_params + params,
        ).fetchone()
        total_count = int(count_row[0] if count_row else 0)

    return results, total_count
