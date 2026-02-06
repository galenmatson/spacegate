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


def search_systems(
    con: duckdb.DuckDBPyConnection,
    *,
    q_norm: Optional[str],
    q_raw: Optional[str],
    system_id_exact: Optional[int],
    id_query: Optional[Dict[str, Any]],
    max_dist_ly: Optional[float],
    min_dist_ly: Optional[float],
    spectral_classes: List[str],
    has_planets: Optional[bool],
    sort: str,
    match_mode: bool,
    limit: int,
    cursor_values: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    conditions: List[str] = []
    params: List[Any] = []

    match_rank_expr = "0 AS match_rank"
    match_params: List[Any] = []
    match_clauses: List[str] = []

    if q_norm:
        exact_clause = (
            "(s.system_name_norm = ? OR s.stable_object_key = ? OR "
            "EXISTS (SELECT 1 FROM stars st WHERE st.system_id = s.system_id "
            "AND st.star_name_norm = ?))"
        )
        match_params.extend([q_norm, q_raw, q_norm])

        prefix_clause = (
            "(s.system_name_norm LIKE ? OR EXISTS (SELECT 1 FROM stars st "
            "WHERE st.system_id = s.system_id AND st.star_name_norm LIKE ?))"
        )
        prefix_pattern = f"{q_norm}%"
        match_params.extend([prefix_pattern, prefix_pattern])

        tokens = [token for token in q_norm.split(" ") if token]
        token_clauses: List[str] = []
        for token in tokens:
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
        placeholders = ",".join(["?"] * len(spectral_classes))
        conditions.append(
            "EXISTS (SELECT 1 FROM stars st WHERE st.system_id = s.system_id "
            f"AND st.spectral_class IN ({placeholders}))"
        )
        params.extend(spectral_classes)

    if has_planets is True:
        conditions.append(
            "EXISTS (SELECT 1 FROM planets p WHERE p.system_id = s.system_id)"
        )

    order_by = "system_name_norm ASC NULLS LAST, system_id ASC"
    cursor_clause = ""
    outer_clause = ""

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
            params.extend(
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
        outer_clause = "WHERE match_rank IS NOT NULL"
        if cursor_clause:
            outer_clause = f"{outer_clause} AND {cursor_clause}"
    else:
        if sort == "distance":
            order_by = "dist_ly ASC NULLS LAST, system_id ASC"
            if cursor_values:
                cursor_dist = cursor_values.get("dist")
                if cursor_dist is None:
                    cursor_dist = 1e12
                cursor_clause = (
                    "(COALESCE(s.dist_ly, 1e12) > ? OR "
                    "(COALESCE(s.dist_ly, 1e12) = ? AND s.system_id > ?))"
                )
                params.extend(
                    [
                        cursor_dist,
                        cursor_dist,
                        cursor_values.get("id"),
                    ]
                )
        else:
            if cursor_values:
                cursor_clause = (
                    "(COALESCE(s.system_name_norm, '') > ? OR "
                    "(COALESCE(s.system_name_norm, '') = ? AND s.system_id > ?))"
                )
                params.extend(
                    [
                        cursor_values.get("name", ""),
                        cursor_values.get("name", ""),
                        cursor_values.get("id"),
                    ]
                )

    if cursor_clause and not match_mode:
        conditions.append(cursor_clause)

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    sql = f"""
        WITH star_agg AS (
            SELECT
                system_id,
                COUNT(*) AS star_count,
                ARRAY_AGG(DISTINCT spectral_class) FILTER (WHERE spectral_class IS NOT NULL) AS spectral_classes
            FROM stars
            GROUP BY system_id
        ),
        planet_agg AS (
            SELECT
                system_id,
                COUNT(*) AS planet_count
            FROM planets
            WHERE system_id IS NOT NULL
            GROUP BY system_id
        ),
        base AS (
            SELECT
                s.*,
                sa.star_count,
                sa.spectral_classes,
                pa.planet_count,
                {match_rank_expr}
            FROM systems s
            LEFT JOIN star_agg sa ON sa.system_id = s.system_id
            LEFT JOIN planet_agg pa ON pa.system_id = s.system_id
            {where_sql}
        )
        SELECT * FROM base
        {outer_clause}
        ORDER BY {order_by}
        LIMIT ?
    """
    all_params = match_params + params + [limit]
    cursor = con.execute(sql, all_params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    results: List[Dict[str, Any]] = []
    for row in rows:
        data = row_to_dict(columns, row)
        payload, provenance = split_provenance(data)
        spectral = payload.get("spectral_classes") or []
        if spectral is None:
            spectral = []
        payload["spectral_classes"] = [cls for cls in spectral if cls]
        payload["star_count"] = int(payload.get("star_count") or 0)
        payload["planet_count"] = int(payload.get("planet_count") or 0)
        payload["provenance"] = provenance
        results.append(payload)

    return results
