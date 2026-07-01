import json
import re
from collections import defaultdict
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
    "bayer_root_name": 2,
    "bayer_expanded_name": 2,
    "member_bayer_name": 3,
    "member_bayer_root_name": 3,
    "member_bayer_expanded_name": 3,
    "flamsteed_name": 4,
    "member_flamsteed_name": 5,
    "member_star_name": 6,
    "gl_id": 6,
    "member_gl_id": 7,
    "hd_id": 8,
    "member_hd_id": 9,
    "hip_id": 10,
    "member_hip_id": 11,
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


def normalize_sql_expr(expr: str) -> str:
    return (
        "trim(regexp_replace(regexp_replace(lower(coalesce("
        + expr
        + ", '')), '[^0-9a-z]+', ' ', 'g'), '\\\\s+', ' ', 'g'))"
    )


def _attach_side_db(
    con: duckdb.DuckDBPyConnection,
    disc_db_path: Optional[str],
    *,
    alias: str = "disc_db",
) -> bool:
    if not disc_db_path:
        return False
    try:
        attached = {str(row[1]) for row in con.execute("PRAGMA database_list").fetchall()}
        if alias in attached:
            return True
    except Exception:
        pass
    try:
        db_path_sql = str(disc_db_path).replace("'", "''")
        con.execute(f"ATTACH '{db_path_sql}' AS {alias} (READ_ONLY)")
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


def _has_local_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name = ?
              AND column_name = ?
            LIMIT 1
            """,
            [table_name, column_name],
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


def _canonical_name_rank(value: Any) -> int:
    norm = _name_norm(value)
    if not norm:
        return 99
    if _is_gaia_placeholder_name(norm):
        return 99
    if norm.startswith("hd "):
        return 8
    if norm.startswith("hip "):
        return 10
    if norm.startswith("hr "):
        return 12
    if norm.startswith("tyc "):
        return 14
    if norm.startswith("hyg "):
        return 16
    if norm.startswith("wds "):
        return 18
    if norm.startswith("gj ") or norm.startswith("gl "):
        return 20
    if re.match(r"^[a-z]{3}\s+[a-z]{3}$", norm):
        return 2
    if re.match(r"^\d+\s+[a-z]{3}$", norm):
        return 4
    if re.match(r"^\d+[a-z]{3}\s+[a-z]{3}$", norm):
        return 6
    return 0


def _query_name_match_rank(name: Any, preferred_query_norm: Optional[str]) -> Optional[Tuple[int, int, str]]:
    query_norm = _name_norm(preferred_query_norm)
    name_norm = _name_norm(name)
    if not query_norm or not name_norm:
        return None
    if name_norm == query_norm:
        return (0, len(name_norm), name_norm)
    if name_norm.startswith(query_norm):
        return (1, len(name_norm), name_norm)
    tokens = [token for token in query_norm.split(" ") if token]
    if tokens and all(token in name_norm for token in tokens):
        return (2, len(name_norm), name_norm)
    return None


def _alias_rank(row: Dict[str, Any]) -> Tuple[int, int, int, str]:
    kind = str(row.get("alias_kind") or "").strip()
    kind_rank = ALIAS_KIND_RANK.get(kind, 99)
    try:
        priority = int(row.get("alias_priority"))
    except Exception:
        priority = 999
    raw = _clean_name(row.get("alias_raw"))
    return (kind_rank, priority, len(raw), raw.lower())


def _spectral_filter_mask(tokens: List[str]) -> int:
    mask = 0
    for token in tokens:
        mask |= SPECTRAL_CLASS_MASKS.get(str(token).strip().upper(), 0)
    return mask


def _has_fast_search_hit(
    con: duckdb.DuckDBPyConnection,
    *,
    q_norm: str,
    has_system_search_terms: bool,
    has_aliases: bool,
) -> bool:
    if not q_norm:
        return False
    prefix_pattern = f"{q_norm}%"
    if has_system_search_terms:
        row = con.execute(
            """
            SELECT 1
            FROM system_search_terms
            WHERE term_norm = ?
               OR term_norm LIKE ?
            LIMIT 1
            """,
            [q_norm, prefix_pattern],
        ).fetchone()
        return bool(row)
    if has_aliases:
        row = con.execute(
            """
            SELECT 1
            FROM aliases
            WHERE system_id IS NOT NULL
              AND alias_norm IS NOT NULL
              AND (alias_norm = ? OR alias_norm LIKE ?)
            LIMIT 1
            """,
            [q_norm, prefix_pattern],
        ).fetchone()
        return bool(row)
    return False


def choose_display_name(
    canonical_name: Any,
    aliases: List[Dict[str, Any]],
    *,
    alt_limit: int = 8,
    preferred_query_norm: Optional[str] = None,
) -> Tuple[str, List[str]]:
    canonical = _clean_name(canonical_name)
    alias_rows = [row for row in aliases if _clean_name(row.get("alias_raw"))]
    alias_rows.sort(key=_alias_rank)

    ordered_names: List[str] = []
    ordered_name_rows: List[Dict[str, Any]] = []
    seen_norm: set[str] = set()
    for row in alias_rows:
        raw = _clean_name(row.get("alias_raw"))
        norm = raw.lower()
        if norm in seen_norm:
            continue
        seen_norm.add(norm)
        ordered_names.append(raw)
        ordered_name_rows.append(row)

    display_name = canonical
    best_match_name: Optional[str] = None
    best_match_key: Optional[Tuple[int, int, str, int]] = None
    canonical_match = _query_name_match_rank(canonical, preferred_query_norm)
    if canonical_match is not None:
        best_match_name = canonical
        best_match_key = canonical_match + (_canonical_name_rank(canonical),)
    for row in alias_rows:
        candidate = _clean_name(row.get("alias_raw"))
        candidate_match = _query_name_match_rank(candidate, preferred_query_norm)
        if candidate_match is None:
            continue
        candidate_key = candidate_match + (_alias_rank(row)[0],)
        if best_match_key is None or candidate_key < best_match_key:
            best_match_name = candidate
            best_match_key = candidate_key
    if best_match_name:
        display_name = best_match_name
    else:
        best_alias_name: Optional[str] = None
        best_alias_rank = 99
        for row, candidate in zip(ordered_name_rows, ordered_names):
            if _is_gaia_placeholder_name(candidate):
                continue
            best_alias_name = candidate
            best_alias_rank = _alias_rank(row)[0]
            break
        canonical_rank = _canonical_name_rank(canonical)
        if best_alias_name and (
            not display_name
            or _is_gaia_placeholder_name(display_name)
            or best_alias_rank < canonical_rank
        ):
            display_name = best_alias_name
        elif (not display_name) and ordered_names:
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


def fetch_eclipsing_for_system(
    con: duckdb.DuckDBPyConnection, system_id: int
) -> List[Dict[str, Any]]:
    if not _has_local_table(con, "eclipsing_binaries"):
        return []
    cursor = con.execute(
        """
        SELECT *
        FROM eclipsing_binaries
        WHERE system_id = ?
        ORDER BY source_catalog ASC, source_catalog_object_id ASC, eclipsing_binary_id ASC
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


def summarize_star_temperatures(stars: List[Dict[str, Any]]) -> Dict[str, Any]:
    temps = [
        float(star["teff_k"])
        for star in stars
        if star.get("teff_k") is not None
    ]
    if not temps:
        return {
            "star_teff_count": 0,
            "min_star_teff_k": None,
            "max_star_teff_k": None,
        }
    return {
        "star_teff_count": len(temps),
        "min_star_teff_k": min(temps),
        "max_star_teff_k": max(temps),
    }


def _parse_spectral_classes(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(token).strip().upper() for token in raw if str(token).strip()]
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(token).strip().upper() for token in parsed if str(token).strip()]


def _spectral_class_from_type(raw: Any) -> Optional[str]:
    value = str(raw or "").strip().upper()
    if not value:
        return None
    for prefix in ("ESD", "USD", "SD"):
        if value.startswith(prefix) and len(value) > len(prefix):
            value = value[len(prefix):]
            break
    if value[:1] in SPECTRAL_CLASS_MASKS:
        return value[:1]
    match = re.search(r"[^A-Z]([OBAFGKMLTYD])", value)
    return match.group(1) if match else None


def _visual_stellar_class_from_mass_prior(mass_msun: Any) -> Optional[str]:
    try:
        mass = float(mass_msun)
    except Exception:
        return None
    if mass <= 0:
        return None
    if mass < 0.08:
        return "L"
    if mass < 0.65:
        return "M"
    if mass < 0.85:
        return "K"
    if mass < 1.04:
        return "G"
    if mass < 1.4:
        return "F"
    if mass < 2.1:
        return "A"
    if mass < 16.0:
        return "B"
    return "O"


def fetch_map_systems(
    con: duckdb.DuckDBPyConnection,
    *,
    max_dist_ly: float = 100.0,
    limit: int = 20000,
    disc_db_path: Optional[str] = None,
    compact: bool = False,
) -> Dict[str, Any]:
    radius = max(0.0, min(float(max_dist_ly), 100.0))
    row_limit = max(1, min(int(limit), 50000))
    disc_attached = _attach_side_db(con, disc_db_path, alias="disc_db")
    has_coolness_scores = disc_attached and _has_table(
        con,
        alias="disc_db",
        table_name="coolness_scores",
    )
    has_snapshot_manifest = disc_attached and _has_table(
        con,
        alias="disc_db",
        table_name="snapshot_manifest",
    )

    coolness_join = ""
    coolness_select = """
        NULL::BIGINT AS coolness_rank,
        NULL::DOUBLE AS coolness_score,
        NULL::VARCHAR AS dominant_spectral_class,
        NULL::BIGINT AS nice_planet_count,
        NULL::BIGINT AS weird_planet_count
    """
    if has_coolness_scores:
        coolness_join = """
            LEFT JOIN disc_db.coolness_scores c
              ON c.system_id = s.system_id
        """
        coolness_select = """
            c.rank AS coolness_rank,
            c.score_total AS coolness_score,
            c.dominant_spectral_class,
            c.nice_planet_count,
            c.weird_planet_count
        """

    snapshot_join = ""
    snapshot_select = "FALSE AS has_snapshot"
    if has_snapshot_manifest:
        snapshot_join = """
            LEFT JOIN (
              SELECT DISTINCT system_id
              FROM disc_db.snapshot_manifest
            ) sm ON sm.system_id = s.system_id
        """
        snapshot_select = "sm.system_id IS NOT NULL AS has_snapshot"

    cursor = con.execute(
        f"""
        SELECT
          s.system_id,
          s.stable_object_key,
          s.system_name,
          s.dist_ly,
          s.x_helio_ly,
          s.y_helio_ly,
          s.z_helio_ly,
          COALESCE(s.star_count, 0)::BIGINT AS star_count,
          COALESCE(s.planet_count, 0)::BIGINT AS planet_count,
          s.star_teff_count,
          s.min_star_teff_k,
          s.max_star_teff_k,
          s.spectral_classes_json,
          {coolness_select},
          {snapshot_select}
        FROM systems s
        {coolness_join}
        {snapshot_join}
        WHERE s.dist_ly <= ?
          AND s.x_helio_ly IS NOT NULL
          AND s.y_helio_ly IS NOT NULL
          AND s.z_helio_ly IS NOT NULL
        ORDER BY COALESCE(coolness_rank, 9223372036854775807) ASC,
                 s.dist_ly ASC,
                 s.system_id ASC
        LIMIT ?
        """,
        [radius, row_limit],
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    items: List[Dict[str, Any]] = []
    spectral_counts: Dict[str, int] = {}
    planet_systems = 0
    multi_star_systems = 0

    for row in rows:
        item = row_to_dict(columns, row)
        spectral_classes = _parse_spectral_classes(item.pop("spectral_classes_json", None))
        dominant = str(item.get("dominant_spectral_class") or "").strip().upper()
        if dominant in ("", "?"):
            dominant = spectral_classes[0] if spectral_classes else "UNKNOWN"
        item["spectral_classes"] = spectral_classes
        item["dominant_spectral_class"] = dominant
        item["star_count"] = int(item.get("star_count") or 0)
        item["planet_count"] = int(item.get("planet_count") or 0)
        item["nice_planet_count"] = int(item.get("nice_planet_count") or 0)
        item["weird_planet_count"] = int(item.get("weird_planet_count") or 0)
        item["has_snapshot"] = bool(item.get("has_snapshot"))
        if compact:
            for key in ("dist_ly", "x_helio_ly", "y_helio_ly", "z_helio_ly"):
                value = item.get(key)
                if value is not None:
                    item[key] = round(float(value), 6)
            score = item.get("coolness_score")
            if score is not None:
                item["coolness_score"] = round(float(score), 3)
            item.pop("stable_object_key", None)
            item.pop("star_teff_count", None)
            item.pop("min_star_teff_k", None)
            item.pop("max_star_teff_k", None)
            item.pop("nice_planet_count", None)
            item.pop("weird_planet_count", None)
            item.pop("spectral_classes", None)
        spectral_counts[dominant] = spectral_counts.get(dominant, 0) + 1
        if item["planet_count"] > 0:
            planet_systems += 1
        if item["star_count"] > 1:
            multi_star_systems += 1
        items.append(item)

    total_available = int(
        con.execute(
            """
            SELECT COUNT(*)::BIGINT
            FROM systems
            WHERE dist_ly <= ?
              AND x_helio_ly IS NOT NULL
              AND y_helio_ly IS NOT NULL
              AND z_helio_ly IS NOT NULL
            """,
            [radius],
        ).fetchone()[0]
        or 0
    )
    return {
        "scope": "systems",
        "frame": "heliocentric_icrs_j2016",
        "max_dist_ly": radius,
        "limit": row_limit,
        "total_available": total_available,
        "returned": len(items),
        "truncated": len(items) < total_available,
        "spectral_counts": spectral_counts,
        "planet_systems": planet_systems,
        "multi_star_systems": multi_star_systems,
        "items": items,
    }


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


def fetch_arm_evidence_for_stars(
    con: duckdb.DuckDBPyConnection,
    star_ids: List[int],
    *,
    arm_db_path: Optional[str],
) -> Dict[int, Dict[str, Any]]:
    if not star_ids or not arm_db_path:
        return {}
    if not _attach_side_db(con, arm_db_path, alias="arm_db"):
        return {}
    has_variability_summary = _has_table(con, alias="arm_db", table_name="variability_summary")
    has_ultracoolsheet = _has_table(con, alias="arm_db", table_name="ultracoolsheet_objects")
    if not has_variability_summary and not has_ultracoolsheet:
        return {}

    placeholders = ",".join(["?"] * len(star_ids))
    grouped: Dict[int, Dict[str, Any]] = {}

    def ensure(star_id: int) -> Dict[str, Any]:
        payload = grouped.setdefault(star_id, {"catalogs": []})
        catalogs = payload.setdefault("catalogs", [])
        if not isinstance(catalogs, list):
            payload["catalogs"] = []
        return payload

    def add_catalog(star_payload: Dict[str, Any], catalog: str) -> None:
        catalogs = star_payload.setdefault("catalogs", [])
        if catalog not in catalogs:
            catalogs.append(catalog)

    if has_variability_summary:
        rows = con.execute(
            f"""
            SELECT
              star_id,
              vsx_match_count,
              primary_variability_type_raw,
              primary_variability_family,
              primary_amplitude_mag,
              primary_period_days,
              any_high_variability,
              confidence_tier
            FROM arm_db.variability_summary
            WHERE star_id IN ({placeholders})
            """,
            star_ids,
        ).fetchall()
        for (
            star_id,
            vsx_match_count,
            primary_variability_type_raw,
            primary_variability_family,
            primary_amplitude_mag,
            primary_period_days,
            any_high_variability,
            confidence_tier,
        ) in rows:
            sid = int(star_id)
            payload = ensure(sid)
            add_catalog(payload, "vsx")
            payload["vsx"] = {
                "vsx_match_count": int(vsx_match_count or 0),
                "primary_variability_type_raw": primary_variability_type_raw,
                "primary_variability_family": primary_variability_family,
                "primary_amplitude_mag": (
                    float(primary_amplitude_mag) if primary_amplitude_mag is not None else None
                ),
                "primary_period_days": (
                    float(primary_period_days) if primary_period_days is not None else None
                ),
                "any_high_variability": bool(any_high_variability),
                "confidence_tier": confidence_tier,
            }

    if has_ultracoolsheet:
        rows = con.execute(
            f"""
            WITH ranked AS (
              SELECT
                star_id,
                object_name,
                name_simbadable,
                age_category,
                youth_evidence,
                banyan_hypothesis_young,
                banyan_prob_young,
                is_exoplanet_host,
                has_unresolved_multiplicity,
                has_resolved_multiplicity,
                has_higher_mass_companion,
                spectral_type_opt,
                spectral_type_ir,
                match_confidence,
                count(*) OVER (PARTITION BY star_id) AS match_count,
                row_number() OVER (
                  PARTITION BY star_id
                  ORDER BY coalesce(match_confidence, 0.0) DESC, ultracoolsheet_object_id ASC
                ) AS rn
              FROM arm_db.ultracoolsheet_objects
              WHERE star_id IN ({placeholders})
            )
            SELECT
              star_id,
              object_name,
              name_simbadable,
              age_category,
              youth_evidence,
              banyan_hypothesis_young,
              banyan_prob_young,
              is_exoplanet_host,
              has_unresolved_multiplicity,
              has_resolved_multiplicity,
              has_higher_mass_companion,
              spectral_type_opt,
              spectral_type_ir,
              match_confidence,
              match_count
            FROM ranked
            WHERE rn = 1
            """,
            star_ids,
        ).fetchall()
        for (
            star_id,
            object_name,
            name_simbadable,
            age_category,
            youth_evidence,
            banyan_hypothesis_young,
            banyan_prob_young,
            is_exoplanet_host,
            has_unresolved_multiplicity,
            has_resolved_multiplicity,
            has_higher_mass_companion,
            spectral_type_opt,
            spectral_type_ir,
            match_confidence,
            match_count,
        ) in rows:
            sid = int(star_id)
            payload = ensure(sid)
            add_catalog(payload, "ultracoolsheet")
            payload["ultracoolsheet"] = {
                "match_count": int(match_count or 0),
                "object_name": object_name,
                "name_simbadable": name_simbadable,
                "age_category": age_category,
                "youth_evidence": youth_evidence,
                "banyan_hypothesis_young": banyan_hypothesis_young,
                "banyan_prob_young": (
                    float(banyan_prob_young) if banyan_prob_young is not None else None
                ),
                "is_exoplanet_host": bool(is_exoplanet_host),
                "has_unresolved_multiplicity": bool(has_unresolved_multiplicity),
                "has_resolved_multiplicity": bool(has_resolved_multiplicity),
                "has_higher_mass_companion": bool(has_higher_mass_companion),
                "spectral_type_opt": spectral_type_opt,
                "spectral_type_ir": spectral_type_ir,
                "match_confidence": float(match_confidence) if match_confidence is not None else None,
            }

    for payload in grouped.values():
        catalogs = payload.get("catalogs")
        if isinstance(catalogs, list):
            payload["catalogs"] = sorted({str(token) for token in catalogs if str(token).strip()})

    return grouped


def fetch_sol_hierarchy_for_system(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    stable_object_key: Optional[str],
    arm_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    is_sol = bool(
        con.execute(
            """
            SELECT 1
            FROM systems
            WHERE system_id = ?
              AND (
                lower(coalesce(system_name_norm, '')) = 'sol'
                OR lower(coalesce(stable_object_key, '')) = 'system:sol'
                OR lower(coalesce(?::varchar, '')) = 'system:sol'
              )
            LIMIT 1
            """,
            [system_id, stable_object_key or ""],
        ).fetchone()
    )
    if not is_sol or not arm_db_path:
        return None
    if not _attach_side_db(con, arm_db_path, alias="arm_db"):
        return None
    if not _has_table(con, alias="arm_db", table_name="component_entities"):
        return None

    moons: List[Dict[str, Any]] = []
    if (
        _has_table(con, alias="arm_db", table_name="system_hierarchy_edges")
        and _has_table(con, alias="arm_db", table_name="orbit_edges")
        and _has_table(con, alias="arm_db", table_name="orbital_solutions")
    ):
        moon_rows = con.execute(
            """
            SELECT
              c.stable_component_key,
              c.display_name,
              c.catalog_component_label,
              parent.display_name AS parent_name,
              os.period_days,
              os.semi_major_axis_au,
              os.eccentricity,
              os.inclination_deg
            FROM arm_db.component_entities c
            LEFT JOIN arm_db.system_hierarchy_edges h
              ON h.child_component_key = c.stable_component_key
             AND h.source_catalog = 'sol_authority'
             AND h.member_role = 'satellite'
            LEFT JOIN arm_db.component_entities parent
              ON parent.stable_component_key = h.parent_component_key
            LEFT JOIN arm_db.orbit_edges oe
              ON oe.secondary_component_key = c.stable_component_key
             AND oe.relation_kind = 'satellite'
            LEFT JOIN arm_db.orbital_solutions os
              ON os.orbit_edge_id = oe.orbit_edge_id
            WHERE c.component_type = 'moon'
              AND c.source_catalog = 'sol_authority'
            ORDER BY lower(coalesce(parent.display_name, '')), lower(coalesce(c.display_name, ''))
            """
        ).fetchall()
        for (
            stable_component_key,
            display_name,
            catalog_component_label,
            parent_name,
            period_days,
            semi_major_axis_au,
            eccentricity,
            inclination_deg,
        ) in moon_rows:
            moons.append(
                {
                    "stable_component_key": stable_component_key,
                    "display_name": display_name,
                    "catalog_component_label": catalog_component_label,
                    "parent_name": parent_name,
                    "period_days": float(period_days) if period_days is not None else None,
                    "semi_major_axis_au": (
                        float(semi_major_axis_au) if semi_major_axis_au is not None else None
                    ),
                    "eccentricity": float(eccentricity) if eccentricity is not None else None,
                    "inclination_deg": float(inclination_deg) if inclination_deg is not None else None,
                }
            )

    small_bodies: List[Dict[str, Any]] = []
    if _has_table(con, alias="arm_db", table_name="sol_small_body_objects"):
        rows = con.execute(
            """
            SELECT
              stable_component_key,
              body_name,
              body_kind,
              parent_name,
              orbital_period_days,
              semi_major_axis_au,
              eccentricity,
              inclination_deg,
              staleness_days,
              freshness_window_days,
              is_stale
            FROM arm_db.sol_small_body_objects
            ORDER BY
              CASE body_kind
                WHEN 'asteroid' THEN 1
                WHEN 'tno' THEN 2
                WHEN 'comet' THEN 3
                ELSE 9
              END,
              lower(coalesce(body_name, ''))
            """
        ).fetchall()
        for (
            stable_component_key,
            body_name,
            body_kind,
            parent_name,
            orbital_period_days,
            semi_major_axis_au,
            eccentricity,
            inclination_deg,
            staleness_days,
            freshness_window_days,
            is_stale,
        ) in rows:
            small_bodies.append(
                {
                    "stable_component_key": stable_component_key,
                    "body_name": body_name,
                    "body_kind": body_kind,
                    "parent_name": parent_name,
                    "orbital_period_days": (
                        float(orbital_period_days) if orbital_period_days is not None else None
                    ),
                    "semi_major_axis_au": (
                        float(semi_major_axis_au) if semi_major_axis_au is not None else None
                    ),
                    "eccentricity": float(eccentricity) if eccentricity is not None else None,
                    "inclination_deg": float(inclination_deg) if inclination_deg is not None else None,
                    "staleness_days": int(staleness_days or 0),
                    "freshness_window_days": int(freshness_window_days or 0),
                    "is_stale": bool(is_stale),
                }
            )

    artificial_objects: List[Dict[str, Any]] = []
    if _has_table(con, alias="arm_db", table_name="sol_artificial_objects"):
        rows = con.execute(
            """
            SELECT
              stable_component_key,
              artifact_name,
              artifact_kind,
              parent_name,
              center_code,
              target_body_name,
              orbital_period_days,
              semi_major_axis_au,
              eccentricity,
              inclination_deg,
              staleness_days,
              freshness_window_days,
              is_stale
            FROM arm_db.sol_artificial_objects
            ORDER BY
              lower(coalesce(parent_name, '')),
              lower(coalesce(artifact_kind, '')),
              lower(coalesce(artifact_name, ''))
            """
        ).fetchall()
        for (
            stable_component_key,
            artifact_name,
            artifact_kind,
            parent_name,
            center_code,
            target_body_name,
            orbital_period_days,
            semi_major_axis_au,
            eccentricity,
            inclination_deg,
            staleness_days,
            freshness_window_days,
            is_stale,
        ) in rows:
            artificial_objects.append(
                {
                    "stable_component_key": stable_component_key,
                    "artifact_name": artifact_name,
                    "artifact_kind": artifact_kind,
                    "parent_name": parent_name,
                    "center_code": center_code,
                    "target_body_name": target_body_name,
                    "orbital_period_days": (
                        float(orbital_period_days) if orbital_period_days is not None else None
                    ),
                    "semi_major_axis_au": (
                        float(semi_major_axis_au) if semi_major_axis_au is not None else None
                    ),
                    "eccentricity": float(eccentricity) if eccentricity is not None else None,
                    "inclination_deg": float(inclination_deg) if inclination_deg is not None else None,
                    "staleness_days": int(staleness_days or 0),
                    "freshness_window_days": int(freshness_window_days or 0),
                    "is_stale": bool(is_stale),
                }
            )

    small_body_kind_counts: Dict[str, int] = {}
    for row in small_bodies:
        kind = str(row.get("body_kind") or "unknown")
        small_body_kind_counts[kind] = int(small_body_kind_counts.get(kind, 0)) + 1

    artificial_kind_counts: Dict[str, int] = {}
    for row in artificial_objects:
        kind = str(row.get("artifact_kind") or "unknown")
        artificial_kind_counts[kind] = int(artificial_kind_counts.get(kind, 0)) + 1

    return {
        "is_sol": True,
        "counts": {
            "moons": len(moons),
            "small_bodies": len(small_bodies),
            "artificial_objects": len(artificial_objects),
            "small_body_kind_counts": small_body_kind_counts,
            "artificial_kind_counts": artificial_kind_counts,
            "stale_small_bodies": sum(1 for row in small_bodies if row.get("is_stale")),
            "stale_artificial_objects": sum(
                1 for row in artificial_objects if row.get("is_stale")
            ),
        },
        "moons": moons,
        "small_bodies": small_bodies,
        "artificial_objects": artificial_objects,
    }


def _arm_star_overlay_expr(system_alias: str = "s") -> str:
    return (
        "COALESCE(("
        "WITH RECURSIVE descendants(component_key) AS ("
        "  SELECT h.child_component_key "
        "  FROM arm_db.system_hierarchy_edges h "
        "  WHERE "
        f"    {system_alias}.wds_id IS NOT NULL "
        f"    AND h.parent_component_key = ('comp:msc_system:wds:' || {system_alias}.wds_id) "
        "  UNION ALL "
        "  SELECT h.child_component_key "
        "  FROM arm_db.system_hierarchy_edges h "
        "  JOIN descendants d ON d.component_key = h.parent_component_key"
        ") "
        "SELECT COUNT(*)::BIGINT "
        "FROM descendants d "
        "JOIN arm_db.component_entities ce "
        "  ON ce.stable_component_key = d.component_key "
        "WHERE ce.component_type = 'star'"
        "), 0)"
    )


def _fetch_arm_star_overlay_counts_for_systems(
    con: duckdb.DuckDBPyConnection,
    systems: List[Dict[str, Any]],
    *,
    arm_db_path: Optional[str],
) -> Dict[int, int]:
    if not systems or not arm_db_path:
        return {}
    if not _attach_side_db(con, arm_db_path, alias="arm_db"):
        return {}
    if not _has_table(con, alias="arm_db", table_name="component_entities"):
        return {}
    if not _has_table(con, alias="arm_db", table_name="system_hierarchy_edges"):
        return {}

    refs: List[Tuple[int, str]] = []
    for row in systems:
        system_id = row.get("system_id")
        wds_id = _clean_name(row.get("wds_id"))
        if system_id is None or not wds_id:
            continue
        refs.append((int(system_id), wds_id))
    if not refs:
        return {}

    values_sql = ",".join(["(?, ?)"] * len(refs))
    bind_params: List[Any] = []
    for system_id, wds_id in refs:
        bind_params.extend([system_id, wds_id])
    rows = con.execute(
        f"""
        WITH RECURSIVE ref(system_id, wds_id) AS (
          VALUES {values_sql}
        ), descendants(system_id, component_key) AS (
          SELECT
            ref.system_id,
            h.child_component_key
          FROM ref
          JOIN arm_db.system_hierarchy_edges h
            ON h.parent_component_key = ('comp:msc_system:wds:' || ref.wds_id)
          UNION ALL
          SELECT
            d.system_id,
            h.child_component_key
          FROM descendants d
          JOIN arm_db.system_hierarchy_edges h
            ON h.parent_component_key = d.component_key
        )
        SELECT
          d.system_id,
          COUNT(*)::BIGINT AS overlay_star_count
        FROM descendants d
        JOIN arm_db.component_entities ce
          ON ce.stable_component_key = d.component_key
         AND ce.component_type = 'star'
        GROUP BY d.system_id
        """,
        bind_params,
    ).fetchall()
    return {int(system_id): int(count or 0) for system_id, count in rows}


def _orbit_solution_payload(
    period_days: Any,
    semi_major_axis_au: Any,
    eccentricity: Any,
    inclination_deg: Any,
    confidence_tier: Any,
    source_catalog: Any,
) -> Optional[Dict[str, Any]]:
    has_values = any(
        value is not None
        for value in (period_days, semi_major_axis_au, eccentricity, inclination_deg)
    )
    if not has_values:
        return None
    return {
        "period_days": float(period_days) if period_days is not None else None,
        "semi_major_axis_au": float(semi_major_axis_au) if semi_major_axis_au is not None else None,
        "eccentricity": float(eccentricity) if eccentricity is not None else None,
        "inclination_deg": float(inclination_deg) if inclination_deg is not None else None,
        "confidence_tier": _clean_name(confidence_tier) or None,
        "source_catalog": _clean_name(source_catalog) or None,
    }


def _label_from_component_stem(
    host_name: str,
    primary_node: Dict[str, Any],
    secondary_node: Dict[str, Any],
) -> str:
    labels = [
        _clean_name(primary_node.get("catalog_component_label")).lower(),
        _clean_name(secondary_node.get("catalog_component_label")).lower(),
    ]
    if labels[0] and labels[1]:
        shared = ""
        for left, right in zip(labels[0], labels[1]):
            if left != right:
                break
            shared += left
        if shared:
            return f"{host_name} {shared.upper()}".strip()
    return f"{host_name} Pair".strip()


def _hierarchy_type_rank(component_type: Any) -> int:
    return {
        "system": 0,
        "subsystem": 1,
        "star": 2,
        "brown_dwarf": 3,
        "planet": 4,
        "moon": 5,
        "minor_body": 6,
        "artificial": 7,
    }.get(_clean_name(component_type).lower(), 99)


def _hierarchy_derived_explanation(
    *,
    source_basis: Any,
    member_role: Any,
    catalog_component_label: Any,
    orbit_relation_kind: Any = None,
) -> str | None:
    basis = _clean_name(source_basis).lower()
    role = (_clean_name(member_role) or _clean_name(catalog_component_label) or "").upper()
    relation = _clean_name(orbit_relation_kind).lower()
    if basis == "msc_inferred_leaf":
        if role:
            return (
                f"Derived from MSC subsystem role {role}. "
                "This leaf is inferred from multiplicity evidence and is not a separately matched core star row."
            )
        return (
            "Derived from MSC subsystem evidence. "
            "This leaf is inferred from multiplicity evidence and is not a separately matched core star row."
        )
    if relation == "binary":
        return (
            "Derived subsystem created from orbital evidence so the paired components can be shown under a shared binary node."
        )
    if basis in {"msc_role_leaf", "canonical_host_planet", "fallback_root_planet"}:
        return f"Derived from {basis.replace('_', ' ')}."
    return "Derived from supporting hierarchy or orbital evidence."


def _hierarchy_family(component_type: Any, core_object_type: Any) -> str:
    return {
        "star": "star",
        "planet": "planet",
    }.get(_clean_name(core_object_type).lower(), {
        "system": "system",
        "subsystem": "subsystem",
        "star": "star",
        "main_sequence": "star",
        "brown_dwarf": "brown_dwarf",
        "compact": "star",
        "planet": "planet",
        "subplanet": "planet",
        "moon": "moon",
        "minor_body": "minor_body",
        "artificial": "artificial",
    }.get(_clean_name(component_type).lower(), _clean_name(component_type).lower() or "unknown"))


def _build_hierarchy_node_payload(
    node_key: str,
    *,
    node_map: Dict[str, Dict[str, Any]],
    children_map: Dict[str, List[str]],
    depth: int,
) -> Dict[str, Any]:
    node = dict(node_map[node_key])
    child_keys = sorted(
        children_map.get(node_key, []),
        key=lambda key: (
            _hierarchy_type_rank(node_map.get(key, {}).get("component_family")),
            _clean_name(node_map.get(key, {}).get("display_name")).lower(),
            key,
        ),
    )
    children = [
        _build_hierarchy_node_payload(
            child_key,
            node_map=node_map,
            children_map=children_map,
            depth=depth + 1,
        )
        for child_key in child_keys
    ]
    direct_type_counts: Dict[str, int] = {}
    total_type_counts: Dict[str, int] = {
        _clean_name(node.get("component_family")).lower() or "unknown": 1,
    }
    self_star_count = node.get("self_star_count")
    if self_star_count is None:
        self_star_count = 1 if _clean_name(node.get("component_family")).lower() == "star" else 0
    total_star_count = int(self_star_count or 0)
    for child in children:
        child_type = _clean_name(child.get("component_family")).lower() or "unknown"
        direct_type_counts[child_type] = int(direct_type_counts.get(child_type, 0)) + 1
        total_star_count += int(child.get("total_star_count") or 0)
        for token, count in (child.get("total_type_counts") or {}).items():
            if not token:
                continue
            total_type_counts[token] = int(total_type_counts.get(token, 0)) + int(count or 0)
    node["depth"] = int(depth)
    node["children"] = children
    node["child_count"] = len(children)
    node["descendant_count"] = sum(int(child.get("descendant_count") or 0) + 1 for child in children)
    node["direct_type_counts"] = direct_type_counts
    node["total_type_counts"] = total_type_counts
    node["total_star_count"] = int(total_star_count)
    compact_stellar_branch = (
        len(children) > 0
        and int(total_star_count) > 0
        and int(total_star_count) <= 4
        and node["descendant_count"] <= 6
    )
    node["collapsed_by_default"] = bool(len(children) >= 6 or (depth >= 2 and not compact_stellar_branch))
    return node


def _enrich_hierarchy_star_nodes(
    con: duckdb.DuckDBPyConnection,
    *,
    node_map: Dict[str, Dict[str, Any]],
    arm_attached: bool,
) -> None:
    star_nodes = {
        key: node
        for key, node in node_map.items()
        if _clean_name(node.get("component_family")).lower() == "star"
    }
    if not star_nodes:
        return

    core_star_refs = [
        (key, int(node["core_object_id"]))
        for key, node in star_nodes.items()
        if _clean_name(node.get("core_object_type")).lower() == "star"
        and node.get("core_object_id") is not None
    ]
    star_facts: Dict[str, Dict[str, Any]] = {}

    if core_star_refs:
        star_ids = [star_id for _, star_id in core_star_refs]
        placeholders = ",".join(["?"] * len(star_ids))
        core_rows = con.execute(
            f"""
            SELECT
              star_id,
              spectral_type_raw,
              spectral_class,
              teff_k,
              vmag,
              dist_ly
            FROM stars
            WHERE star_id IN ({placeholders})
            """,
            star_ids,
        ).fetchall()
        core_by_star_id = {
            int(star_id): {
                "spectral_type_raw": _clean_name(spectral_type_raw) or None,
                "spectral_class": _clean_name(spectral_class) or None,
                "teff_k": float(teff_k) if teff_k is not None else None,
                "vmag": float(vmag) if vmag is not None else None,
                "dist_ly": float(dist_ly) if dist_ly is not None else None,
            }
            for star_id, spectral_type_raw, spectral_class, teff_k, vmag, dist_ly in core_rows
        }
        for node_key, star_id in core_star_refs:
            if star_id in core_by_star_id:
                star_facts[node_key] = dict(core_by_star_id[star_id])

        if arm_attached and _has_table(con, alias="arm_db", table_name="stellar_parameters"):
            param_rows = con.execute(
                f"""
                WITH ranked AS (
                  SELECT
                    star_id,
                    mass_msun,
                    radius_rsun,
                    luminosity_log10_lsun,
                    row_number() OVER (
                      PARTITION BY star_id
                      ORDER BY
                        CASE parameter_source
                          WHEN 'nasa_pscomppars_host' THEN 0
                          WHEN 'gaia_dr3_backbone' THEN 1
                          ELSE 9
                        END ASC,
                        (
                          CASE WHEN mass_msun IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN radius_rsun IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN luminosity_log10_lsun IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN teff_k IS NOT NULL THEN 1 ELSE 0 END
                        ) DESC,
                        stellar_parameter_id ASC
                    ) AS rn
                  FROM arm_db.stellar_parameters
                  WHERE star_id IN ({placeholders})
                )
                SELECT
                  star_id,
                  mass_msun,
                  radius_rsun,
                  luminosity_log10_lsun
                FROM ranked
                WHERE rn = 1
                """,
                star_ids,
            ).fetchall()
            params_by_star_id = {
                int(star_id): {
                    "mass_msun": float(mass_msun) if mass_msun is not None else None,
                    "radius_rsun": float(radius_rsun) if radius_rsun is not None else None,
                    "luminosity_log10_lsun": (
                        float(luminosity_log10_lsun)
                        if luminosity_log10_lsun is not None
                        else None
                    ),
                }
                for star_id, mass_msun, radius_rsun, luminosity_log10_lsun in param_rows
            }
            for node_key, star_id in core_star_refs:
                if star_id not in params_by_star_id:
                    continue
                star_facts.setdefault(node_key, {}).update(params_by_star_id[star_id])

    msc_lookup_by_node_key = {
        key: key
        for key in star_nodes
        if key.startswith("comp:msc:")
    }
    for key in star_nodes:
        if key.startswith("canon:leaf:msc:"):
            msc_lookup_by_node_key[key] = key.replace("canon:leaf:msc:", "comp:msc:wds:", 1)
    msc_keys = sorted(set(msc_lookup_by_node_key.values()))
    if msc_keys and arm_attached and _has_table(con, alias="arm_db", table_name="msc_component_details"):
        placeholders = ",".join(["?"] * len(msc_keys))
        msc_rows = con.execute(
            f"""
            SELECT
              stable_component_key,
              spectral_type_raw,
              vmag,
              sep_arcsec
            FROM arm_db.msc_component_details
            WHERE stable_component_key IN ({placeholders})
            """,
            msc_keys,
        ).fetchall()
        node_keys_by_component_key: Dict[str, List[str]] = defaultdict(list)
        for node_key, component_key in msc_lookup_by_node_key.items():
            node_keys_by_component_key[component_key].append(node_key)
        for stable_component_key, spectral_type_raw, vmag, sep_arcsec in msc_rows:
            component_key = str(stable_component_key)
            for node_key in node_keys_by_component_key.get(component_key, []):
                facts = star_facts.setdefault(node_key, {})
                if not facts.get("spectral_type_raw"):
                    facts["spectral_type_raw"] = _clean_name(spectral_type_raw) or None
                if not facts.get("spectral_class"):
                    facts["spectral_class"] = _spectral_class_from_type(spectral_type_raw)
                if facts.get("vmag") is None and vmag is not None and float(vmag) > 0.0:
                    facts["vmag"] = float(vmag)
                if sep_arcsec is not None:
                    facts["sep_arcsec"] = float(sep_arcsec)

    if msc_keys and arm_attached and _has_table(con, alias="arm_db", table_name="msc_system_details"):
        placeholders = ",".join(["?"] * len(msc_keys))
        endpoint_rows = con.execute(
            f"""
            SELECT
              primary_component_key,
              secondary_component_key,
              spectral_type_primary,
              spectral_type_secondary,
              mass_primary_msun,
              mass_secondary_msun,
              vmag_primary,
              vmag_secondary
            FROM arm_db.msc_system_details
            WHERE primary_component_key IN ({placeholders})
               OR secondary_component_key IN ({placeholders})
            """,
            msc_keys + msc_keys,
        ).fetchall()
        node_keys_by_component_key: Dict[str, List[str]] = defaultdict(list)
        for node_key, component_key in msc_lookup_by_node_key.items():
            node_keys_by_component_key[component_key].append(node_key)

        for (
            primary_component_key,
            secondary_component_key,
            spectral_type_primary,
            spectral_type_secondary,
            mass_primary_msun,
            mass_secondary_msun,
            vmag_primary,
            vmag_secondary,
        ) in endpoint_rows:
            for component_key, spectral_type_raw, mass_msun, vmag in (
                (primary_component_key, spectral_type_primary, mass_primary_msun, vmag_primary),
                (secondary_component_key, spectral_type_secondary, mass_secondary_msun, vmag_secondary),
            ):
                component_key = _clean_name(component_key)
                if not component_key:
                    continue
                for node_key in node_keys_by_component_key.get(component_key, []):
                    facts = star_facts.setdefault(node_key, {})
                    if not facts.get("spectral_type_raw"):
                        facts["spectral_type_raw"] = _clean_name(spectral_type_raw) or None
                    if not facts.get("spectral_class"):
                        facts["spectral_class"] = _spectral_class_from_type(spectral_type_raw)
                    if facts.get("mass_msun") is None and mass_msun is not None:
                        facts["mass_msun"] = float(mass_msun)
                    if facts.get("vmag") is None and vmag is not None and float(vmag) > 0.0:
                        facts["vmag"] = float(vmag)

    for node_key, facts in star_facts.items():
        if node_key not in node_map:
            continue
        visual_stellar_class = None
        visual_stellar_class_status = None
        visual_stellar_class_basis = None
        if not facts.get("spectral_type_raw") and not facts.get("spectral_class"):
            visual_stellar_class = _visual_stellar_class_from_mass_prior(facts.get("mass_msun"))
            if visual_stellar_class:
                visual_stellar_class_status = "assumed"
                visual_stellar_class_basis = "mass_main_sequence_prior_v1"
        node_map[node_key]["quick_facts"] = {
            "spectral_type_raw": facts.get("spectral_type_raw"),
            "spectral_class": facts.get("spectral_class"),
            "visual_stellar_class": visual_stellar_class,
            "visual_stellar_class_status": visual_stellar_class_status,
            "visual_stellar_class_basis": visual_stellar_class_basis,
            "teff_k": facts.get("teff_k"),
            "mass_msun": facts.get("mass_msun"),
            "radius_rsun": facts.get("radius_rsun"),
            "luminosity_log10_lsun": facts.get("luminosity_log10_lsun"),
            "vmag": facts.get("vmag"),
            "dist_ly": facts.get("dist_ly"),
            "sep_arcsec": facts.get("sep_arcsec"),
        }


def _fetch_canonical_hierarchy_for_system(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    stable_object_key: Optional[str],
    canonical_hierarchy_db_path: Optional[str],
    arm_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not _attach_side_db(con, canonical_hierarchy_db_path, alias="canon_hier"):
        return None
    if not _has_table(con, alias="canon_hier", table_name="hierarchy_nodes"):
        return None
    if not _has_table(con, alias="canon_hier", table_name="hierarchy_edges"):
        return None

    root_key = _clean_name(stable_object_key) or None
    if not root_key:
        row = con.execute(
            """
            SELECT stable_object_key
            FROM systems
            WHERE system_id = ?
            LIMIT 1
            """,
            [system_id],
        ).fetchone()
        root_key = _clean_name(row[0]) if row else None
    if not root_key:
        return None

    root_exists = con.execute(
        """
        SELECT 1
        FROM canon_hier.hierarchy_nodes
        WHERE hierarchy_node_key = ?
          AND node_kind = 'system'
        LIMIT 1
        """,
        [root_key],
    ).fetchone()
    if not root_exists:
        return None

    hierarchy_rows = con.execute(
        """
        WITH RECURSIVE tree(node_key, depth) AS (
          SELECT ? AS node_key, 0 AS depth
          UNION ALL
          SELECT e.child_node_key, tree.depth + 1
          FROM tree
          JOIN canon_hier.hierarchy_edges e
            ON e.parent_node_key = tree.node_key
          WHERE tree.depth < 12
        )
        SELECT DISTINCT node_key, depth
        FROM tree
        """,
        [root_key],
    ).fetchall()
    if not hierarchy_rows:
        return None

    hierarchy_node_keys = [str(row[0]) for row in hierarchy_rows if row and row[0]]
    key_placeholders = ",".join(["?"] * len(hierarchy_node_keys))
    entity_rows = con.execute(
        f"""
        SELECT
          hierarchy_node_key,
          node_kind,
          canonical_key,
          display_name,
          wds_id,
          member_role,
          source_basis
        FROM canon_hier.hierarchy_nodes
        WHERE hierarchy_node_key IN ({key_placeholders})
        """,
        hierarchy_node_keys,
    ).fetchall()

    node_kind_to_family = {
        "system": "system",
        "subsystem": "subsystem",
        "star": "star",
        "inferred_star_leaf": "star",
        "planet": "planet",
    }
    node_map: Dict[str, Dict[str, Any]] = {}
    canonical_keys_by_type: Dict[str, List[str]] = defaultdict(list)
    for (
        hierarchy_node_key,
        node_kind,
        canonical_key,
        display_name,
        node_wds_id,
        member_role,
        source_basis,
    ) in entity_rows:
        clean_kind = _clean_name(node_kind).lower() or "unknown"
        canonical = _clean_name(canonical_key) or None
        component_family = node_kind_to_family.get(clean_kind, clean_kind)
        component_type = "star" if clean_kind == "inferred_star_leaf" else component_family
        key = str(hierarchy_node_key)
        node_map[key] = {
            "stable_component_key": key,
            "component_type": component_type,
            "component_family": component_family,
            "core_object_type": component_family if canonical else None,
            "core_object_id": None,
            "display_name": _clean_name(display_name) or canonical or key,
            "catalog_component_label": _clean_name(member_role) or None,
            "member_role": _clean_name(member_role) or None,
            "source_catalog": _clean_name(source_basis) or "canonical_hierarchy",
            "synthetic": clean_kind == "inferred_star_leaf",
            "orbit": None,
            "quick_facts": None,
            "canonical_key": canonical,
            "node_kind": clean_kind,
            "self_star_count": 1 if clean_kind == "inferred_star_leaf" else None,
            "wds_id": _clean_name(node_wds_id) or None,
        }
        if node_map[key]["synthetic"]:
            node_map[key]["derived_explanation"] = _hierarchy_derived_explanation(
                source_basis=source_basis,
                member_role=member_role,
                catalog_component_label=member_role,
            )
        if canonical:
            canonical_keys_by_type[component_family].append(canonical)

    edge_rows = con.execute(
        f"""
        SELECT
          parent_node_key,
          child_node_key,
          edge_kind,
          member_role,
          source_basis
        FROM canon_hier.hierarchy_edges
        WHERE parent_node_key IN ({key_placeholders})
        """,
        hierarchy_node_keys,
    ).fetchall()
    parent_by_child: Dict[str, str] = {}
    children_map: Dict[str, List[str]] = defaultdict(list)
    for parent_node_key, child_node_key, edge_kind, member_role, source_basis in edge_rows:
        parent = str(parent_node_key)
        child = str(child_node_key)
        if parent not in node_map or child not in node_map:
            continue
        if child in parent_by_child:
            continue
        parent_by_child[child] = parent
        children_map[parent].append(child)
        child_node = node_map.get(child)
        if child_node is not None:
            child_node["catalog_relation_label"] = _clean_name(source_basis) or None
            child_node["edge_kind"] = _clean_name(edge_kind) or None
            if not child_node.get("member_role"):
                child_node["member_role"] = _clean_name(member_role) or None
            if not child_node.get("catalog_component_label"):
                child_node["catalog_component_label"] = _clean_name(member_role) or None

    for object_type, table_name, id_column in (
        ("system", "systems", "system_id"),
        ("star", "stars", "star_id"),
        ("planet", "planets", "planet_id"),
    ):
        canonical_keys = sorted(set(canonical_keys_by_type.get(object_type) or []))
        if not canonical_keys:
            continue
        placeholders = ",".join(["?"] * len(canonical_keys))
        rows = con.execute(
            f"""
            SELECT stable_object_key, {id_column}
            FROM {table_name}
            WHERE stable_object_key IN ({placeholders})
            """,
            canonical_keys,
        ).fetchall()
        id_by_key = {str(stable_key): int(object_id) for stable_key, object_id in rows if stable_key}
        for node in node_map.values():
            if node.get("component_family") != object_type:
                continue
            canonical_key = node.get("canonical_key")
            if canonical_key in id_by_key:
                node["core_object_type"] = object_type
                node["core_object_id"] = id_by_key[canonical_key]

    for node_key, node in node_map.items():
        if _clean_name(node.get("component_family")).lower() != "star":
            node["self_star_count"] = 0
            continue
        if node.get("node_kind") == "inferred_star_leaf":
            node["self_star_count"] = 1
            continue
        child_keys = children_map.get(node_key, [])
        has_leaf_children = any(
            _clean_name(node_map.get(child_key, {}).get("node_kind")).lower() == "inferred_star_leaf"
            for child_key in child_keys
        )
        node["self_star_count"] = 0 if has_leaf_children else 1

    arm_attached = _attach_side_db(con, arm_db_path, alias="arm_db")
    _enrich_hierarchy_star_nodes(
        con,
        node_map=node_map,
        arm_attached=arm_attached,
    )

    root_payload = _build_hierarchy_node_payload(
        root_key,
        node_map=node_map,
        children_map=children_map,
        depth=0,
    )
    counts = {
        "stars": int(root_payload.get("total_star_count") or 0),
        "nodes": int(root_payload.get("descendant_count") or 0) + 1,
        "direct_children": int(root_payload.get("child_count") or 0),
        "type_counts": dict(root_payload.get("total_type_counts") or {}),
    }
    return {
        "root": root_payload,
        "counts": counts,
        "preferred_root_key": root_key,
        "root_keys_considered": [root_key],
    }


def _fetch_arm_hierarchy_for_system(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    stable_object_key: Optional[str],
    wds_id: Optional[str],
    arm_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not arm_db_path:
        return None
    arm_attached = _attach_side_db(con, arm_db_path, alias="arm_db")
    if not arm_attached:
        return None
    if not _has_table(con, alias="arm_db", table_name="component_entities"):
        return None
    if not _has_table(con, alias="arm_db", table_name="system_hierarchy_edges"):
        return None

    candidate_keys: List[str] = []
    if stable_object_key:
        candidate_keys.append(f"comp:system:{stable_object_key}")
    if wds_id:
        candidate_keys.append(f"comp:msc_system:wds:{wds_id}")
    candidate_keys = [key for key in candidate_keys if key]
    if not candidate_keys:
        return None

    placeholders = ",".join(["?"] * len(candidate_keys))
    candidate_rows = con.execute(
        f"""
        SELECT
          ce.stable_component_key,
          ce.display_name,
          ce.component_type,
          COUNT(*) FILTER (WHERE child.component_type = 'star')::BIGINT AS direct_star_count,
          COUNT(h.child_component_key)::BIGINT AS direct_child_count
        FROM arm_db.component_entities ce
        LEFT JOIN arm_db.system_hierarchy_edges h
          ON h.parent_component_key = ce.stable_component_key
        LEFT JOIN arm_db.component_entities child
          ON child.stable_component_key = h.child_component_key
        WHERE ce.stable_component_key IN ({placeholders})
        GROUP BY ce.stable_component_key, ce.display_name, ce.component_type
        """,
        candidate_keys,
    ).fetchall()
    if not candidate_rows:
        return None

    preferred_root_key = sorted(
        candidate_rows,
        key=lambda row: (
            0 if str(row[0]).startswith("comp:msc_system:") else 1,
            -int(row[4] or 0),
            -int(row[3] or 0),
            _clean_name(row[1]).lower(),
        ),
    )[0][0]

    hierarchy_rows = con.execute(
        """
        WITH RECURSIVE tree(component_key, depth) AS (
          SELECT ? AS component_key, 0 AS depth
          UNION ALL
          SELECT h.child_component_key, tree.depth + 1
          FROM tree
          JOIN arm_db.system_hierarchy_edges h
            ON h.parent_component_key = tree.component_key
          WHERE tree.depth < 12
        )
        SELECT DISTINCT component_key, depth
        FROM tree
        """,
        [preferred_root_key],
    ).fetchall()
    if not hierarchy_rows:
        return None

    component_keys = [str(row[0]) for row in hierarchy_rows if row and row[0]]
    key_placeholders = ",".join(["?"] * len(component_keys))

    entity_rows = con.execute(
        f"""
        SELECT
          stable_component_key,
          component_type,
          core_object_type,
          core_object_id,
          display_name,
          catalog_component_label,
          source_catalog
        FROM arm_db.component_entities
        WHERE stable_component_key IN ({key_placeholders})
        """,
        component_keys,
    ).fetchall()
    node_map: Dict[str, Dict[str, Any]] = {}
    for (
        component_key,
        component_type,
        core_object_type,
        core_object_id,
        display_name,
        catalog_component_label,
        source_catalog,
    ) in entity_rows:
        clean_component_type = _clean_name(component_type) or "unknown"
        clean_core_object_type = _clean_name(core_object_type) or None
        clean_source_catalog = _clean_name(source_catalog) or None
        component_family = _hierarchy_family(component_type, core_object_type)
        is_source_stellar_leaf = (
            component_family == "star"
            and clean_component_type == "star"
            and clean_core_object_type != "star"
            and clean_source_catalog in {"msc", "orb6", "sbx", "gaia_nss"}
        )
        node_map[str(component_key)] = {
            "stable_component_key": str(component_key),
            "component_type": clean_component_type,
            "component_family": component_family,
            "core_object_type": clean_core_object_type,
            "core_object_id": int(core_object_id) if core_object_id is not None else None,
            "display_name": _clean_name(display_name),
            "catalog_component_label": _clean_name(catalog_component_label) or None,
            "source_catalog": clean_source_catalog,
            "synthetic": False,
            "orbit": None,
            "quick_facts": None,
            "node_kind": "source_star_leaf" if is_source_stellar_leaf else clean_component_type,
            "self_star_count": 1 if clean_core_object_type == "star" or clean_component_type == "star" else 0,
        }
    if preferred_root_key not in node_map:
        return None

    edge_rows = con.execute(
        f"""
        SELECT
          parent_component_key,
          child_component_key,
          edge_kind,
          member_role,
          catalog_relation_label
        FROM arm_db.system_hierarchy_edges
        WHERE parent_component_key IN ({key_placeholders})
        """,
        component_keys,
    ).fetchall()
    parent_by_child: Dict[str, str] = {}
    children_map: Dict[str, List[str]] = defaultdict(list)
    for parent_key, child_key, edge_kind, member_role, catalog_relation_label in edge_rows:
        parent = str(parent_key)
        child = str(child_key)
        if parent not in node_map or child not in node_map:
            continue
        if child in parent_by_child:
            continue
        parent_by_child[child] = parent
        children_map[parent].append(child)
        child_node = node_map.get(child)
        if child_node is not None:
            child_node["member_role"] = _clean_name(member_role) or None
            child_node["catalog_relation_label"] = _clean_name(catalog_relation_label) or None
            child_node["edge_kind"] = _clean_name(edge_kind) or None

    orbit_rows = []
    if _has_table(con, alias="arm_db", table_name="orbit_edges"):
        orbit_rows = con.execute(
            f"""
            SELECT
              oe.host_component_key,
              oe.primary_component_key,
              oe.secondary_component_key,
              oe.relation_kind,
              oe.orbit_edge_id,
              os.period_days,
              os.semi_major_axis_au,
              os.eccentricity,
              os.inclination_deg,
              COALESCE(os.confidence_tier, oe.confidence_tier) AS confidence_tier,
              COALESCE(os.source_catalog, oe.source_catalog) AS solution_source_catalog
            FROM arm_db.orbit_edges oe
            LEFT JOIN arm_db.orbital_solutions os
              ON os.orbit_edge_id = oe.orbit_edge_id
             AND (
               oe.preferred_solution_id IS NULL
               OR os.orbital_solution_id = oe.preferred_solution_id
             )
            WHERE oe.host_component_key IN ({key_placeholders})
            ORDER BY oe.host_component_key, oe.primary_component_key, oe.secondary_component_key
            """,
            component_keys,
        ).fetchall()

    for (
        host_component_key,
        primary_component_key,
        secondary_component_key,
        relation_kind,
        orbit_edge_id,
        period_days,
        semi_major_axis_au,
        eccentricity,
        inclination_deg,
        confidence_tier,
        solution_source_catalog,
    ) in orbit_rows:
        host_key = str(host_component_key)
        primary_key = str(primary_component_key)
        secondary_key = str(secondary_component_key)
        if host_key not in node_map or primary_key not in node_map or secondary_key not in node_map:
            continue
        relation = _clean_name(relation_kind).lower()
        orbit_payload = _orbit_solution_payload(
            period_days,
            semi_major_axis_au,
            eccentricity,
            inclination_deg,
            confidence_tier,
            solution_source_catalog,
        )
        if relation == "binary":
            synthetic_key = f"synthetic:orbit:{int(orbit_edge_id or 0)}"
            if synthetic_key not in node_map:
                host_name = _clean_name(node_map.get(host_key, {}).get("display_name"))
                display_name = _label_from_component_stem(
                    host_name or "Subsystem",
                    node_map[primary_key],
                    node_map[secondary_key],
                )
                node_map[synthetic_key] = {
                    "stable_component_key": synthetic_key,
                    "component_type": "subsystem",
                    "component_family": "subsystem",
                    "core_object_type": None,
                    "core_object_id": None,
                    "display_name": display_name,
                    "catalog_component_label": None,
                    "source_catalog": node_map.get(host_key, {}).get("source_catalog"),
                    "synthetic": True,
                    "orbit": orbit_payload,
                    "orbit_relation_kind": relation or None,
                    "derived_explanation": _hierarchy_derived_explanation(
                        source_basis="orbit_binary_subsystem",
                        member_role=None,
                        catalog_component_label=None,
                        orbit_relation_kind=relation,
                    ),
                }
                if primary_key in parent_by_child:
                    previous = parent_by_child[primary_key]
                    if primary_key in children_map.get(previous, []):
                        children_map[previous].remove(primary_key)
                if secondary_key in parent_by_child:
                    previous = parent_by_child[secondary_key]
                    if secondary_key in children_map.get(previous, []):
                        children_map[previous].remove(secondary_key)
                parent_by_child[synthetic_key] = host_key
                children_map[host_key].append(synthetic_key)
                parent_by_child[primary_key] = synthetic_key
                parent_by_child[secondary_key] = synthetic_key
                children_map[synthetic_key] = [primary_key, secondary_key]
            continue
        if parent_by_child.get(secondary_key) in (host_key, None):
            if secondary_key in parent_by_child:
                previous = parent_by_child[secondary_key]
                if secondary_key in children_map.get(previous, []):
                    children_map[previous].remove(secondary_key)
            parent_by_child[secondary_key] = primary_key
            children_map[primary_key].append(secondary_key)
            secondary_node = node_map.get(secondary_key)
            if secondary_node is not None:
                secondary_node["orbit"] = orbit_payload
                secondary_node["orbit_relation_kind"] = relation or None

    _enrich_hierarchy_star_nodes(
        con,
        node_map=node_map,
        arm_attached=arm_attached,
    )

    root_payload = _build_hierarchy_node_payload(
        preferred_root_key,
        node_map=node_map,
        children_map=children_map,
        depth=0,
    )
    counts = {
        "stars": int(root_payload.get("total_star_count") or 0),
        "nodes": int(root_payload.get("descendant_count") or 0) + 1,
        "direct_children": int(root_payload.get("child_count") or 0),
        "type_counts": dict(root_payload.get("total_type_counts") or {}),
    }
    return {
        "root": root_payload,
        "counts": counts,
        "preferred_root_key": preferred_root_key,
        "root_keys_considered": candidate_keys,
    }


def _hierarchy_payload_star_count(payload: Optional[Dict[str, Any]]) -> int:
    if not isinstance(payload, dict):
        return 0
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    return int((counts or {}).get("stars") or 0)


def fetch_system_hierarchy_for_system(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    stable_object_key: Optional[str],
    wds_id: Optional[str],
    canonical_hierarchy_db_path: Optional[str],
    arm_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    canonical_payload = _fetch_canonical_hierarchy_for_system(
        con,
        system_id=system_id,
        stable_object_key=stable_object_key,
        canonical_hierarchy_db_path=canonical_hierarchy_db_path,
        arm_db_path=arm_db_path,
    )
    arm_payload = _fetch_arm_hierarchy_for_system(
        con,
        system_id=system_id,
        stable_object_key=stable_object_key,
        wds_id=wds_id,
        arm_db_path=arm_db_path,
    )

    if canonical_payload is None:
        return arm_payload
    if arm_payload is None:
        return canonical_payload
    if _hierarchy_payload_star_count(arm_payload) > _hierarchy_payload_star_count(canonical_payload):
        return arm_payload
    return canonical_payload


def fetch_snapshot_for_system(
    con: duckdb.DuckDBPyConnection,
    *,
    system_id: int,
    stable_object_key: Optional[str],
    disc_db_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not _attach_side_db(con, disc_db_path, alias="disc_snap"):
        return None
    if not _has_table(con, alias="disc_snap", table_name="snapshot_manifest"):
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
        FROM disc_snap.snapshot_manifest sm
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
    min_temp_k: Optional[float],
    max_temp_k: Optional[float],
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
    disc_db_path: Optional[str] = None,
    arm_db_path: Optional[str] = None,
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
    has_system_search_terms = _has_local_table(con, "system_search_terms")
    has_system_star_count = _has_local_column(con, "systems", "star_count")
    has_system_planet_count = _has_local_column(con, "systems", "planet_count")
    has_system_star_teff_count = _has_local_column(con, "systems", "star_teff_count")
    has_system_min_star_teff_k = _has_local_column(con, "systems", "min_star_teff_k")
    has_system_max_star_teff_k = _has_local_column(con, "systems", "max_star_teff_k")
    has_system_spectral_classes_json = _has_local_column(con, "systems", "spectral_classes_json")
    has_system_spectral_class_mask = _has_local_column(con, "systems", "spectral_class_mask")
    has_star_teff_k = _has_local_column(con, "stars", "teff_k")
    star_teff_select = "teff_k" if has_star_teff_k else "NULL::DOUBLE AS teff_k"
    arm_attached = bool(arm_db_path and _attach_side_db(con, arm_db_path, alias="arm_db"))
    has_arm_star_overlay = (
        arm_attached
        and _has_table(con, alias="arm_db", table_name="component_entities")
        and _has_table(con, alias="arm_db", table_name="system_hierarchy_edges")
    )
    effective_star_count_expr = (
        f"GREATEST(COALESCE(s.star_count, 0), {_arm_star_overlay_expr('s')})"
        if has_arm_star_overlay and has_system_star_count
        else "COALESCE(s.star_count, 0)"
        if has_system_star_count
        else "(SELECT COUNT(*) FROM stars st WHERE st.system_id = s.system_id)"
    )

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
        enable_search_terms = has_system_search_terms and not identifier_mode
        enable_alias_match = (
            not has_system_search_terms and has_aliases and not short_query_mode and not identifier_mode
        )
        fuzzy_alias_distance = 1 if len(q_norm) <= 5 else 2 if len(q_norm) <= 10 else 3
        enable_fuzzy_match = len(q_norm) >= 4 and not _has_fast_search_hit(
            con,
            q_norm=q_norm,
            has_system_search_terms=enable_search_terms,
            has_aliases=enable_alias_match,
        )

        exact_parts = ["s.stable_object_key = ?"]
        exact_params: List[Any] = [q_raw]
        if enable_search_terms:
            alias_cte_parts.append(
                """
                search_term_match_exact AS (
                    SELECT DISTINCT system_id
                    FROM system_search_terms
                    WHERE term_norm = ?
                )
                """
            )
            alias_cte_params.append(q_norm)
            exact_parts.append("s.system_id IN (SELECT system_id FROM search_term_match_exact)")
        else:
            exact_parts.insert(0, "s.system_name_norm = ?")
            exact_params.insert(0, q_norm)
            exact_parts.append(
                "s.system_id IN ("
                "SELECT DISTINCT st.system_id FROM stars st "
                "WHERE st.system_id IS NOT NULL AND "
                + normalize_sql_expr("st.star_name")
                + " = ?)"
            )
            exact_params.append(q_norm)
        if enable_alias_match:
            alias_cte_parts.append(
                """
                alias_match_exact AS (
                    SELECT DISTINCT system_id
                    FROM aliases
                    WHERE system_id IS NOT NULL
                      AND alias_norm = ?
                )
                """
            )
            alias_cte_params.append(q_norm)
            exact_parts.append("s.system_id IN (SELECT system_id FROM alias_match_exact)")
        exact_clause = "(" + " OR ".join(exact_parts) + ")"

        prefix_parts: List[str] = []
        prefix_pattern = f"{q_norm}%"
        prefix_params: List[Any] = []
        if enable_search_terms and not short_query_mode:
            alias_cte_parts.append(
                """
                search_term_match_prefix AS (
                    SELECT DISTINCT system_id
                    FROM system_search_terms
                    WHERE term_norm LIKE ?
                )
                """
            )
            alias_cte_params.append(prefix_pattern)
            prefix_parts.append("s.system_id IN (SELECT system_id FROM search_term_match_prefix)")
        else:
            prefix_parts.append("s.system_name_norm LIKE ?")
            prefix_params.append(prefix_pattern)
            prefix_parts.append(
                "s.system_id IN ("
                "SELECT DISTINCT st.system_id FROM stars st "
                "WHERE st.system_id IS NOT NULL AND "
                + normalize_sql_expr("st.star_name")
                + " LIKE ?)"
            )
            prefix_params.append(prefix_pattern)
        if enable_alias_match:
            alias_cte_parts.append(
                """
                alias_match_prefix AS (
                    SELECT DISTINCT system_id
                    FROM aliases
                    WHERE system_id IS NOT NULL
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
            token_parts: List[str] = []
            if enable_search_terms:
                cte_name = f"search_term_match_token_{token_idx}"
                token_idx += 1
                alias_cte_parts.append(
                    f"""
                    {cte_name} AS (
                        SELECT DISTINCT system_id
                        FROM system_search_terms
                        WHERE term_norm LIKE ?
                    )
                    """
                )
                alias_cte_params.append(token_pattern)
                token_parts.append(f"s.system_id IN (SELECT system_id FROM {cte_name})")
            else:
                token_parts.append("s.system_name_norm LIKE ?")
                token_params.append(token_pattern)
            if enable_alias_match:
                cte_name = f"alias_match_token_{token_idx}"
                token_idx += 1
                alias_cte_parts.append(
                    f"""
                    {cte_name} AS (
                        SELECT DISTINCT system_id
                        FROM aliases
                        WHERE system_id IS NOT NULL
                          AND alias_norm LIKE ?
                    )
                    """
                )
                alias_cte_params.append(token_pattern)
                token_parts.append(f"s.system_id IN (SELECT system_id FROM {cte_name})")
            token_clauses.append("(" + " OR ".join(token_parts) + ")")
        token_and_clause = " AND ".join(token_clauses) if token_clauses else None
        fuzzy_clause = None
        if enable_search_terms and enable_fuzzy_match:
            alias_cte_parts.append(
                f"""
                search_term_match_fuzzy AS (
                    SELECT DISTINCT system_id
                    FROM system_search_terms
                    WHERE term_norm IS NOT NULL
                      AND term_norm <> ''
                      AND abs(length(term_norm) - {len(q_norm)}) <= {fuzzy_alias_distance}
                      AND left(term_norm, 1) = left(?, 1)
                      AND levenshtein(term_norm, ?) <= {fuzzy_alias_distance}
                )
                """
            )
            alias_cte_params.extend([q_norm, q_norm])
            fuzzy_clause = "s.system_id IN (SELECT system_id FROM search_term_match_fuzzy)"
        elif enable_alias_match and enable_fuzzy_match:
            alias_cte_parts.append(
                f"""
                alias_match_fuzzy AS (
                    SELECT DISTINCT system_id
                    FROM aliases
                    WHERE system_id IS NOT NULL
                      AND alias_norm IS NOT NULL
                      AND alias_norm <> ''
                      AND abs(length(alias_norm) - {len(q_norm)}) <= {fuzzy_alias_distance}
                      AND left(alias_norm, 1) = left(?, 1)
                      AND levenshtein(alias_norm, ?) <= {fuzzy_alias_distance}
                )
                """
            )
            alias_cte_params.extend([q_norm, q_norm])
            fuzzy_clause = "s.system_id IN (SELECT system_id FROM alias_match_fuzzy)"

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
                next_rank += 1
            if fuzzy_clause:
                match_lines.append(f"WHEN {fuzzy_clause} THEN {next_rank}")
                match_clauses.append(fuzzy_clause)

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

    if min_temp_k is not None or max_temp_k is not None:
        has_system_temperature_facets = (
            has_system_star_teff_count
            and has_system_min_star_teff_k
            and has_system_max_star_teff_k
        )
        if not has_star_teff_k and not has_system_temperature_facets:
            raise ValueError(
                "Temperature filters are unavailable for this build; rebuild with core.stars.teff_k."
            )
        if has_system_temperature_facets:
            conditions.append("COALESCE(s.star_teff_count, 0) > 0")
            if min_temp_k is not None:
                conditions.append("COALESCE(s.max_star_teff_k, -1e18) >= ?")
                params.append(min_temp_k)
            if max_temp_k is not None:
                conditions.append("COALESCE(s.min_star_teff_k, 1e18) <= ?")
                params.append(max_temp_k)
        teff_terms = ["st.teff_k is not null"]
        if min_temp_k is not None:
            teff_terms.append("st.teff_k >= ?")
            params.append(min_temp_k)
        if max_temp_k is not None:
            teff_terms.append("st.teff_k <= ?")
            params.append(max_temp_k)
        if has_star_teff_k:
            conditions.append(
                "EXISTS (SELECT 1 FROM stars st WHERE st.system_id = s.system_id "
                f"AND ({' AND '.join(teff_terms)}))"
            )

    if spectral_classes:
        if has_system_spectral_class_mask:
            spectral_mask = _spectral_filter_mask(spectral_classes)
            conditions.append("(COALESCE(s.spectral_class_mask, 0) & ?) <> 0")
            params.append(spectral_mask)
        else:
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
        if has_system_planet_count:
            conditions.append("COALESCE(s.planet_count, 0) > 0")
        else:
            conditions.append(
                "EXISTS (SELECT 1 FROM planets p WHERE p.system_id = s.system_id)"
            )
    elif has_planets is False:
        if has_system_planet_count:
            conditions.append("COALESCE(s.planet_count, 0) = 0")
        else:
            conditions.append(
                "NOT EXISTS (SELECT 1 FROM planets p WHERE p.system_id = s.system_id)"
            )

    if min_star_count is not None:
        conditions.append(f"{effective_star_count_expr} >= ?")
        params.append(min_star_count)

    if max_star_count is not None:
        conditions.append(f"{effective_star_count_expr} <= ?")
        params.append(max_star_count)

    if min_planet_count is not None:
        if has_system_planet_count:
            conditions.append("COALESCE(s.planet_count, 0) >= ?")
        else:
            conditions.append(
                "(SELECT COUNT(*) FROM planets p WHERE p.system_id = s.system_id) >= ?"
            )
        params.append(min_planet_count)

    if max_planet_count is not None:
        if has_system_planet_count:
            conditions.append("COALESCE(s.planet_count, 0) <= ?")
        else:
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

    disc_attached = _attach_side_db(con, disc_db_path, alias="disc_db")
    has_coolness_scores = disc_attached and _has_table(
        con,
        alias="disc_db",
        table_name="coolness_scores",
    )
    has_snapshot_manifest = disc_attached and _has_table(
        con,
        alias="disc_db",
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
            FROM disc_db.coolness_scores
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
        raw_spectral_json = payload.pop("spectral_classes_json", None)
        spectral_classes: List[str] = []
        if isinstance(raw_spectral_json, str) and raw_spectral_json.strip():
            try:
                parsed = json.loads(raw_spectral_json)
                if isinstance(parsed, list):
                    spectral_classes = [
                        str(token).strip()
                        for token in parsed
                        if str(token).strip()
                    ]
            except Exception:
                spectral_classes = []
        payload["spectral_classes"] = spectral_classes
        payload["star_count"] = int(payload.get("star_count") or 0)
        payload["planet_count"] = int(payload.get("planet_count") or 0)
        if payload.get("coolness_nice_planet_count") is not None:
            payload["coolness_nice_planet_count"] = int(payload["coolness_nice_planet_count"])
        if payload.get("coolness_weird_planet_count") is not None:
            payload["coolness_weird_planet_count"] = int(payload["coolness_weird_planet_count"])
        payload["snapshot"] = None
        payload["provenance"] = provenance
        results.append(payload)

    system_ids: List[int] = [int(item["system_id"]) for item in results if item.get("system_id") is not None]
    if system_ids:
        arm_star_overlay_counts = _fetch_arm_star_overlay_counts_for_systems(
            con,
            results,
            arm_db_path=arm_db_path,
        )
        placeholders = ",".join(["?"] * len(system_ids))
        needs_star_rollup = not (
            has_system_star_count
            and has_system_star_teff_count
            and has_system_min_star_teff_k
            and has_system_max_star_teff_k
            and has_system_spectral_classes_json
        )
        needs_planet_rollup = not has_system_planet_count
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
            star_name_rows = con.execute(
                f"""
                SELECT
                  system_id,
                  star_name
                FROM stars
                WHERE system_id IN ({placeholders})
                  AND nullif(star_name, '') IS NOT NULL
                ORDER BY system_id ASC, star_name ASC
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
            for system_id, star_name in star_name_rows:
                sid = int(system_id)
                system_aliases.setdefault(sid, []).append(
                    {
                        "alias_raw": star_name,
                        "alias_kind": "member_star_name",
                        "alias_priority": 500,
                    }
                )
            for item in results:
                sid = int(item.get("system_id") or 0)
                display_name, display_aliases = choose_display_name(
                    item.get("system_name"),
                    system_aliases.get(sid, []),
                    preferred_query_norm=q_norm if match_mode else None,
                )
                item["display_name"] = display_name
                item["display_aliases"] = display_aliases
        else:
            for item in results:
                display_name = _clean_name(item.get("system_name"))
                item["display_name"] = display_name
                item["display_aliases"] = []

        star_map: Dict[int, Tuple[int, int, Optional[float], Optional[float], List[str]]] = {}
        if needs_star_rollup:
            star_rows = con.execute(
                f"""
                WITH star_buckets AS (
                  SELECT
                    system_id,
                    {star_teff_select},
                    CASE
                      WHEN UPPER(COALESCE(spectral_type_raw, '')) LIKE 'D%' THEN 'D'
                      WHEN spectral_class IN ('O', 'B', 'A', 'F', 'G', 'K', 'M', 'L', 'T', 'Y', 'D') THEN spectral_class
                      ELSE NULL
                    END AS spectral_bucket
                  FROM stars
                  WHERE system_id IN ({placeholders})
                )
                SELECT
                  system_id,
                  COUNT(*)::BIGINT AS star_count,
                  COUNT(teff_k)::BIGINT AS star_teff_count,
                  MIN(teff_k) AS min_star_teff_k,
                  MAX(teff_k) AS max_star_teff_k,
                  LIST(DISTINCT spectral_bucket) FILTER (WHERE spectral_bucket IS NOT NULL) AS spectral_classes
                FROM star_buckets
                GROUP BY system_id
                """,
                system_ids,
            ).fetchall()
            for sid, count, teff_count, min_teff_k, max_teff_k, spectral in star_rows:
                normalized = sorted(
                    {
                        str(token).strip()
                        for token in (spectral or [])
                        if str(token).strip()
                    }
                )
                star_map[int(sid)] = (
                    int(count or 0),
                    int(teff_count or 0),
                    float(min_teff_k) if min_teff_k is not None else None,
                    float(max_teff_k) if max_teff_k is not None else None,
                    normalized,
                )

        planet_map: Dict[int, int] = {}
        if needs_planet_rollup:
            planet_rows = con.execute(
                f"""
                SELECT system_id, COUNT(*)::BIGINT AS planet_count
                FROM planets
                WHERE system_id IN ({placeholders})
                GROUP BY system_id
                """,
                system_ids,
            ).fetchall()
            planet_map = {int(sid): int(count or 0) for sid, count in planet_rows}

        for item in results:
            sid = int(item.get("system_id") or 0)
            if needs_star_rollup:
                star_count, teff_count, min_teff_k, max_teff_k, spectral_classes = star_map.get(
                    sid,
                    (0, 0, None, None, []),
                )
                item["star_count"] = star_count
                item["star_teff_count"] = teff_count
                item["min_star_teff_k"] = min_teff_k
                item["max_star_teff_k"] = max_teff_k
                item["spectral_classes"] = spectral_classes
            else:
                item["star_count"] = int(item.get("star_count") or 0)
                item["star_teff_count"] = int(item.get("star_teff_count") or 0)
                item["min_star_teff_k"] = (
                    float(item["min_star_teff_k"]) if item.get("min_star_teff_k") is not None else None
                )
                item["max_star_teff_k"] = (
                    float(item["max_star_teff_k"]) if item.get("max_star_teff_k") is not None else None
                )
                item["spectral_classes"] = [
                    str(token).strip()
                    for token in item.get("spectral_classes", [])
                    if str(token).strip()
                ]
            if arm_star_overlay_counts:
                item["star_count"] = max(
                    int(item.get("star_count") or 0),
                    int(arm_star_overlay_counts.get(sid, 0)),
                )
            if needs_planet_rollup:
                item["planet_count"] = planet_map.get(sid, 0)
            else:
                item["planet_count"] = int(item.get("planet_count") or 0)

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
                FROM disc_db.coolness_scores
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
                    FROM disc_db.snapshot_manifest sm
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
