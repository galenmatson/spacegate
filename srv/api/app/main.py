from __future__ import annotations

import datetime
import json
import math
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

import duckdb
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from . import admin_actions
from . import admin_db
from . import auth
from . import db
from .db import DatabaseUnavailable
from .queries import (
    fetch_build_id,
    fetch_counts_for_system,
    fetch_planets_for_system,
    fetch_snapshot_for_system,
    fetch_stars_for_system,
    fetch_system_by_id,
    fetch_system_by_key,
    search_systems,
)
from .utils import (
    decode_cursor,
    encode_cursor,
    normalize_query_text,
    parse_bool,
    parse_identifier_query,
    parse_spectral_classes,
)


app = FastAPI(title="Spacegate API", version="0.1")
ROOT_DIR = Path(__file__).resolve().parents[3]
SCORE_COOLNESS_SCRIPT = ROOT_DIR / "scripts" / "score_coolness.py"
SUPPORTED_SEARCH_SORTS = {"name", "distance", "coolness"}
SUPPORTED_SPECTRAL_FILTERS = {"O", "B", "A", "F", "G", "K", "M", "L", "T", "Y"}

COOLNESS_WEIGHT_KEYS = [
    ("luminosity", "luminosity_feature"),
    ("proper_motion", "proper_motion_feature"),
    ("multiplicity", "multiplicity_feature"),
    ("nice_planets", "nice_planets_feature"),
    ("weird_planets", "weird_planets_feature"),
    ("proximity", "proximity_feature"),
    ("system_complexity", "system_complexity_feature"),
    ("exotic_star", "exotic_star_feature"),
]


def _state_dir() -> Path:
    configured = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if configured:
        return Path(configured)
    db_path = Path(db.get_db_path())
    # Expected default: <state>/served/current/core.duckdb
    if db_path.name == "core.duckdb" and len(db_path.parents) >= 3:
        return db_path.parents[2]
    return ROOT_DIR / "data"


def _resolve_rich_db_path() -> Optional[str]:
    candidate = Path(db.get_db_path()).with_name("rich.duckdb")
    if candidate.exists():
        return str(candidate)
    return None


def _snapshot_asset_url(build_id: str, artifact_path: str) -> str:
    build_token = quote(str(build_id), safe="")
    rel = str(artifact_path or "").lstrip("/")
    rel_token = quote(rel, safe="/._-")
    return f"/api/v1/snapshots/{build_token}/{rel_token}"


def _attach_snapshot_url(payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        return payload
    build_id = snapshot.get("build_id")
    artifact_path = snapshot.get("artifact_path")
    if build_id and artifact_path:
        snapshot["url"] = _snapshot_asset_url(str(build_id), str(artifact_path))
    payload["snapshot"] = snapshot
    return payload


@app.on_event("startup")
def startup_checks():
    auth.initialize()


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


@app.middleware("http")
async def attach_auth_context(request: Request, call_next):
    try:
        auth.attach_auth_context(request)
    except Exception:
        request.state.auth_user = None
        request.state.clear_auth_cookie = True
    response = await call_next(request)
    if getattr(request.state, "clear_auth_cookie", False):
        auth.clear_auth_cookies(response)
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if isinstance(exc.detail, dict):
        code = exc.detail.get("code", "bad_request")
        message = exc.detail.get("message", "Bad request")
        details = exc.detail.get("details", {})
    else:
        code = exc.detail if isinstance(exc.detail, str) else "bad_request"
        message = exc.detail if isinstance(exc.detail, str) else "Bad request"
        details = {}
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": details,
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "Internal server error",
                "details": {},
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


@app.exception_handler(DatabaseUnavailable)
async def db_unavailable_handler(request: Request, exc: DatabaseUnavailable):
    return JSONResponse(
        status_code=503,
        content={
            "error": {
                "code": "db_unavailable",
                "message": "Database not available",
                "details": {"reason": str(exc), "db_path": db.get_db_path()},
                "request_id": getattr(request.state, "request_id", None),
            }
        },
    )


cors_origins = ["*"]
raw_cors = os.getenv("SPACEGATE_CORS_ORIGINS")
if raw_cors:
    cors_origins = [origin.strip() for origin in raw_cors.split(",") if origin.strip()]
allow_credentials = cors_origins != ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/v1/auth/login/google")
def auth_login_google(
    request: Request,
    next_path: Optional[str] = Query(default=None, alias="next"),
):
    return auth.login_redirect(request, next_path=next_path)


@app.get("/api/v1/auth/callback/google")
def auth_callback_google(
    request: Request,
    code: str = Query(...),
    state: str = Query(...),
):
    return auth.auth_callback(request, code=code, state=state)


@app.post("/api/v1/auth/logout")
def auth_logout(request: Request):
    return auth.logout(request)


@app.get("/api/v1/auth/me")
def auth_me(request: Request):
    return auth.auth_me(request)


@app.get("/api/v1/health")
def health():
    with db.connection_scope() as con:
        build_id = fetch_build_id(con)
    return {
        "status": "ok",
        "build_id": build_id,
        "db_path": db.get_db_path(),
        "time_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


@app.get("/api/v1/systems/search")
def systems_search(
    q: Optional[str] = Query(default=None),
    max_dist_ly: Optional[float] = Query(default=None, ge=0),
    min_dist_ly: Optional[float] = Query(default=None, ge=0),
    min_star_count: Optional[int] = Query(default=None, ge=0),
    max_star_count: Optional[int] = Query(default=None, ge=0),
    min_planet_count: Optional[int] = Query(default=None, ge=0),
    max_planet_count: Optional[int] = Query(default=None, ge=0),
    has_habitable: Optional[str] = Query(default=None),
    min_coolness_score: Optional[float] = Query(default=None),
    max_coolness_score: Optional[float] = Query(default=None),
    spectral_class: Optional[str] = Query(default=None),
    has_planets: Optional[str] = Query(default=None),
    sort: str = Query(default="name"),
    limit: int = Query(default=50, ge=1, le=200),
    include_total: Optional[str] = Query(default=None),
    cursor: Optional[str] = Query(default=None),
):
    if max_dist_ly is not None and min_dist_ly is not None and min_dist_ly > max_dist_ly:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid distance range",
                "details": {},
            },
        )
    if max_star_count is not None and min_star_count is not None and min_star_count > max_star_count:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid star-count range",
                "details": {},
            },
        )
    if max_planet_count is not None and min_planet_count is not None and min_planet_count > max_planet_count:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid planet-count range",
                "details": {},
            },
        )
    if (
        max_coolness_score is not None
        and min_coolness_score is not None
        and min_coolness_score > max_coolness_score
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid coolness-score range",
                "details": {},
            },
        )

    q_norm = normalize_query_text(q or "")
    id_query = parse_identifier_query(q_norm)
    system_id_exact: Optional[int] = None
    if q and q.strip().isdigit() and not id_query and len(q.strip()) <= 9:
        try:
            system_id_exact = int(q.strip())
        except ValueError:
            system_id_exact = None

    sort_key = sort.lower() if sort else "name"
    if sort_key not in SUPPORTED_SEARCH_SORTS:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid sort option",
                "details": {
                    "sort": sort,
                    "allowed": sorted(SUPPORTED_SEARCH_SORTS),
                },
            },
        )

    rich_db_path = _resolve_rich_db_path()

    match_mode = bool(q_norm) or bool(id_query)

    cursor_values: Optional[Dict[str, Any]] = None
    if cursor:
        try:
            cursor_values = decode_cursor(cursor)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "invalid_cursor",
                    "message": "Invalid cursor",
                    "details": {},
                },
            )
        if cursor_values.get("sort") and cursor_values.get("sort") != (
            "match" if match_mode else sort_key
        ):
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "invalid_cursor",
                    "message": "Invalid cursor",
                    "details": {},
                },
            )

    spectral_classes = parse_spectral_classes(spectral_class)
    invalid_spectral = [value for value in spectral_classes if value not in SUPPORTED_SPECTRAL_FILTERS]
    if invalid_spectral:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid spectral_class filter",
                "details": {
                    "invalid": invalid_spectral,
                    "allowed": sorted(SUPPORTED_SPECTRAL_FILTERS),
                },
            },
        )
    has_planets_bool = parse_bool(has_planets)
    if has_planets is not None and has_planets_bool is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid has_planets filter",
                "details": {"value": has_planets, "allowed": ["true", "false"]},
            },
        )
    has_habitable_bool = parse_bool(has_habitable)
    if has_habitable is not None and has_habitable_bool is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid has_habitable filter",
                "details": {"value": has_habitable, "allowed": ["true", "false"]},
            },
        )

    include_total_bool = parse_bool(include_total)
    if include_total is not None and include_total_bool is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": "Invalid include_total filter",
                "details": {"value": include_total, "allowed": ["true", "false"]},
            },
        )

    try:
        with db.connection_scope() as con:
            rows, total_count = search_systems(
                con,
                q_norm=q_norm or None,
                q_raw=q,
                system_id_exact=system_id_exact,
                id_query=id_query,
                max_dist_ly=max_dist_ly,
                min_dist_ly=min_dist_ly,
                min_star_count=min_star_count,
                max_star_count=max_star_count,
                min_planet_count=min_planet_count,
                max_planet_count=max_planet_count,
                spectral_classes=spectral_classes,
                has_planets=has_planets_bool,
                has_habitable=has_habitable_bool,
                min_coolness_score=min_coolness_score,
                max_coolness_score=max_coolness_score,
                sort=sort_key,
                match_mode=match_mode,
                limit=limit + 1,
                include_total=bool(include_total_bool),
                cursor_values=cursor_values,
                rich_db_path=rich_db_path,
            )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": str(exc),
                "details": {},
            },
        )

    has_more = len(rows) > limit
    items = rows[:limit]
    for item in items:
        _attach_snapshot_url(item)
    next_cursor = None
    if has_more and items:
        last = items[-1]
        if match_mode:
            dist_value = last.get("dist_ly")
            if dist_value is None:
                dist_value = 1e12
            next_cursor = encode_cursor(
                {
                    "sort": "match",
                    "match_rank": last.get("match_rank"),
                    "dist": dist_value,
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )
        elif sort_key == "distance":
            dist_value = last.get("dist_ly")
            if dist_value is None:
                dist_value = 1e12
            next_cursor = encode_cursor(
                {
                    "sort": "distance",
                    "dist": dist_value,
                    "id": last.get("system_id"),
                }
            )
        elif sort_key == "coolness":
            rank_value = last.get("coolness_rank")
            if rank_value is None:
                rank_value = 9223372036854775807
            next_cursor = encode_cursor(
                {
                    "sort": sort_key,
                    "cool_rank": rank_value,
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )
        else:
            next_cursor = encode_cursor(
                {
                    "sort": "name",
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )

    return {
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "total_count": total_count,
    }


@app.get("/api/v1/systems/{system_id}")
def system_detail(system_id: int):
    rich_db_path = _resolve_rich_db_path()
    with db.connection_scope() as con:
        system = fetch_system_by_id(con, system_id)
        if not system:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "not_found",
                    "message": "System not found",
                    "details": {"system_id": system_id},
                },
            )
        stars = fetch_stars_for_system(con, system_id)
        planets = fetch_planets_for_system(con, system_id)
        star_count, planet_count = fetch_counts_for_system(con, system_id)
        snapshot = fetch_snapshot_for_system(
            con,
            system_id=system_id,
            stable_object_key=system.get("stable_object_key"),
            rich_db_path=rich_db_path,
        )

    system["star_count"] = star_count
    system["planet_count"] = planet_count
    system["snapshot"] = snapshot
    _attach_snapshot_url(system)
    return {"system": system, "stars": stars, "planets": planets}


@app.get("/api/v1/systems/by-key/{stable_object_key}")
def system_detail_by_key(stable_object_key: str):
    rich_db_path = _resolve_rich_db_path()
    with db.connection_scope() as con:
        system = fetch_system_by_key(con, stable_object_key)
        if not system:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "not_found",
                    "message": "System not found",
                    "details": {"stable_object_key": stable_object_key},
                },
            )
        system_id = system.get("system_id")
        stars = fetch_stars_for_system(con, system_id)
        planets = fetch_planets_for_system(con, system_id)
        star_count, planet_count = fetch_counts_for_system(con, system_id)
        snapshot = fetch_snapshot_for_system(
            con,
            system_id=int(system_id),
            stable_object_key=stable_object_key,
            rich_db_path=rich_db_path,
        )

    system["star_count"] = star_count
    system["planet_count"] = planet_count
    system["snapshot"] = snapshot
    _attach_snapshot_url(system)
    return {"system": system, "stars": stars, "planets": planets}


@app.get("/api/v1/snapshots/{build_id}/{artifact_path:path}")
def system_snapshot_asset(build_id: str, artifact_path: str):
    out_root = (_state_dir() / "out" / build_id).resolve()
    candidate = (out_root / artifact_path).resolve()
    if out_root not in candidate.parents and candidate != out_root:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Snapshot not found",
                "details": {},
            },
        )
    if not candidate.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Snapshot not found",
                "details": {},
            },
        )
    ext = candidate.suffix.lower()
    media_type = "image/svg+xml" if ext == ".svg" else "application/octet-stream"
    return FileResponse(
        str(candidate),
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


admin_router = APIRouter(prefix="/api/v1/admin")


class ActionRunRequest(BaseModel):
    action: str = Field(min_length=1, max_length=64)
    params: Dict[str, Any] = Field(default_factory=dict)
    confirmation: Optional[str] = Field(default=None, max_length=256)


class CoolnessPreviewRequest(BaseModel):
    profile_id: Optional[str] = Field(default=None, max_length=128)
    profile_version: Optional[str] = Field(default=None, max_length=128)
    weights: Dict[str, float] = Field(default_factory=dict)
    top_n: int = Field(default=200, ge=20, le=1000)


def _run_score_coolness_json(args: list[str]) -> Dict[str, Any]:
    cmd = [str(SCORE_COOLNESS_SCRIPT), *args]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=504,
            detail={
                "code": "timeout",
                "message": "Coolness command timed out",
                "details": {"command": args},
            },
        )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        message = stderr or stdout or f"score_coolness exited {proc.returncode}"
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": message,
                "details": {"command": args},
            },
        )
    raw = (proc.stdout or "").strip()
    if not raw:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Coolness command returned empty output",
                "details": {"command": args},
            },
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Coolness command returned non-JSON output",
                "details": {"command": args, "output_prefix": raw[:300]},
            },
        )


def _is_duckdb_lock_conflict(exc: Exception) -> bool:
    message = str(exc)
    return (
        "Could not set lock on file" in message
        or "Conflicting lock is held" in message
        or "Can't open a connection to same database file" in message
    )


def _coolness_preview_from_rich_db(weights: Dict[str, float], top_n: int) -> Dict[str, Any]:
    core_db_path = Path(db.get_db_path())
    rich_db_path = core_db_path.with_name("rich.duckdb")
    if not rich_db_path.exists():
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": "Missing rich.duckdb for current build; run score_coolness first",
                "details": {"rich_db_path": str(rich_db_path)},
            },
        )

    score_terms = []
    score_params: list[float] = []
    subscore_terms = []
    subscore_params: list[float] = []
    subscore_aliases: list[tuple[str, str]] = []
    for weight_key, feature_col in COOLNESS_WEIGHT_KEYS:
        score_terms.append(f"? * COALESCE({feature_col}, 0.0)")
        score_params.append(float(weights.get(weight_key, 0.0)))
        alias = f"sub_{weight_key}"
        subscore_terms.append(f"ROUND(100.0 * (? * COALESCE({feature_col}, 0.0)), 6) AS {alias}")
        subscore_params.append(float(weights.get(weight_key, 0.0)))
        subscore_aliases.append((weight_key, alias))
    score_expr = "100.0 * (" + " + ".join(score_terms) + ")"
    subscore_expr = ",\n    ".join(subscore_terms)

    sql = f"""
WITH ranked AS (
  SELECT
    system_id,
    stable_object_key,
    system_name,
    dist_ly,
    dominant_spectral_class,
    star_count,
    planet_count,
    nice_planet_count,
    weird_planet_count,
    {subscore_expr},
    ROUND({score_expr}, 6) AS score_total
  FROM coolness_scores
)
SELECT *
FROM ranked
ORDER BY score_total DESC, system_id ASC
LIMIT ?
    """
    params = [*subscore_params, *score_params, int(top_n)]
    con = None
    try:
        con = duckdb.connect(str(rich_db_path), read_only=True)
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    except duckdb.Error as exc:
        if _is_duckdb_lock_conflict(exc):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "conflict",
                    "message": "Coolness preview is temporarily unavailable while scoring is writing outputs; retry in a few seconds",
                    "details": {"error": str(exc), "rich_db_path": str(rich_db_path), "retryable": True},
                },
            )
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Failed to compute coolness diversity preview",
                "details": {"error": str(exc), "rich_db_path": str(rich_db_path)},
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Failed to compute coolness diversity preview",
                "details": {"error": str(exc), "rich_db_path": str(rich_db_path)},
            },
        )
    finally:
        if con is not None:
            con.close()

    spectral_counts: Dict[str, int] = {}
    with_planets = 0
    without_planets = 0
    multi_star = 0
    single_star = 0
    weird_planet_systems = 0
    for row in rows:
        spectral = str(row.get("dominant_spectral_class") or "?")
        spectral_counts[spectral] = spectral_counts.get(spectral, 0) + 1
        if int(row.get("planet_count") or 0) > 0:
            with_planets += 1
        else:
            without_planets += 1
        if int(row.get("star_count") or 0) > 1:
            multi_star += 1
        else:
            single_star += 1
        if int(row.get("weird_planet_count") or 0) > 0:
            weird_planet_systems += 1

    top_systems = []
    for row in rows[:25]:
        subscores: Dict[str, float] = {}
        for weight_key, alias in subscore_aliases:
            subscores[weight_key] = float(row.get(alias) or 0.0)
        top_systems.append(
            {
                "system_id": int(row["system_id"]),
                "stable_object_key": row.get("stable_object_key"),
                "system_name": row.get("system_name"),
                "dist_ly": float(row.get("dist_ly")) if row.get("dist_ly") is not None else None,
                "dominant_spectral_class": row.get("dominant_spectral_class"),
                "star_count": int(row.get("star_count") or 0),
                "planet_count": int(row.get("planet_count") or 0),
                "nice_planet_count": int(row.get("nice_planet_count") or 0),
                "weird_planet_count": int(row.get("weird_planet_count") or 0),
                "subscores": subscores,
                "score_total": float(row.get("score_total") or 0.0),
            }
        )

    spectral_distribution = [
        {"spectral_class": key, "systems": count}
        for key, count in sorted(spectral_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    probs = [float(item["systems"]) / float(len(rows)) for item in spectral_distribution if len(rows) > 0 and item["systems"] > 0]
    shannon_nats = -sum(p * math.log(p) for p in probs) if probs else 0.0
    shannon_bits = shannon_nats / math.log(2.0) if shannon_nats > 0 else 0.0
    effective_classes = math.exp(shannon_nats) if shannon_nats > 0 else 1.0
    max_classes = len(probs)
    shannon_normalized = (shannon_nats / math.log(max_classes)) if max_classes > 1 else 0.0

    return {
        "rich_db_path": str(rich_db_path),
        "top_n": int(top_n),
        "sample_size": len(rows),
        "diversity_scores": {
            "spectral_shannon_bits": round(shannon_bits, 6),
            "spectral_shannon_normalized": round(shannon_normalized, 6),
            "spectral_effective_classes": round(effective_classes, 6),
        },
        "type_distribution": {
            "with_planets": with_planets,
            "without_planets": without_planets,
            "multi_star": multi_star,
            "single_star": single_star,
            "weird_planet_systems": weird_planet_systems,
        },
        "spectral_distribution": spectral_distribution,
        "top_systems": top_systems,
    }


@admin_router.get("/status")
def admin_status(request: Request):
    user = auth.require_admin(request)
    with db.connection_scope() as con:
        build_id = fetch_build_id(con)
    return {
        "status": "ok",
        "build_id": build_id,
        "db_path": db.get_db_path(),
        "auth": auth.auth_runtime_status(),
        "user": {
            "user_id": user["user_id"],
            "email": user["email"],
            "display_name": user["display_name"],
            "roles": sorted(set(user.get("roles", []))),
        },
        "time_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


@admin_router.get("/coolness/state")
def admin_coolness_state(request: Request):
    auth.require_admin(request)
    data = _run_score_coolness_json(["list"])
    active = data.get("active")
    active_profile = None
    profiles = data.get("profiles") or []
    if isinstance(active, dict):
        for row in profiles:
            if not isinstance(row, dict):
                continue
            if (
                str(row.get("profile_id")) == str(active.get("profile_id"))
                and str(row.get("profile_version")) == str(active.get("profile_version"))
            ):
                active_profile = row
                break
    return {
        "profile_store": data.get("profile_store"),
        "active": active,
        "active_profile": active_profile,
        "profiles": profiles,
    }


@admin_router.post("/coolness/preview")
def admin_coolness_preview(request: Request, payload: CoolnessPreviewRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)

    args = ["preview"]
    profile_id = (payload.profile_id or "").strip()
    profile_version = (payload.profile_version or "").strip()
    if profile_id or profile_version:
        if not profile_id or not profile_version:
            # Tolerate partial selector input from UI and fall back to active baseline.
            profile_id = ""
            profile_version = ""
        else:
            args.extend(["--profile-id", profile_id, "--profile-version", profile_version])

    if payload.weights:
        args.extend(["--weights-json", json.dumps(payload.weights, sort_keys=True)])

    preview = _run_score_coolness_json(args)
    candidate = preview.get("candidate") if isinstance(preview, dict) else None
    if not isinstance(candidate, dict):
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Malformed preview payload from score_coolness",
                "details": {},
            },
        )
    candidate_weights = candidate.get("weights")
    if not isinstance(candidate_weights, dict):
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Preview candidate is missing weights",
                "details": {},
            },
        )

    diversity = _coolness_preview_from_rich_db(
        {k: float(v) for k, v in candidate_weights.items()},
        top_n=int(payload.top_n),
    )

    auth.audit_event(
        request,
        event_type="admin.coolness.preview",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "profile_id": candidate.get("profile_id"),
            "profile_version": candidate.get("profile_version"),
            "top_n": int(payload.top_n),
        },
    )
    return {
        "preview": preview,
        "diversity": diversity,
    }


@admin_router.get("/actions/catalog")
def admin_actions_catalog(request: Request):
    auth.require_admin(request)
    return {"items": admin_actions.list_actions()}


@admin_router.post("/actions/run")
def admin_action_run(request: Request, payload: ActionRunRequest):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        job = admin_actions.start_job(
            action=payload.action.strip(),
            params=payload.params,
            requested_by_user_id=int(user["user_id"]),
            user_roles=sorted(set(user.get("roles", []))),
            confirmation=payload.confirmation,
        )
    except admin_actions.ActionValidationError as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"reason": "validation", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=400,
            detail={
                "code": "bad_request",
                "message": str(exc),
                "details": {"action": payload.action},
            },
        )
    except admin_actions.ActionPermissionError as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"reason": "permission", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=403,
            detail={
                "code": "forbidden",
                "message": str(exc),
                "details": {"action": payload.action},
            },
        )
    except RuntimeError as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="deny",
            actor_user_id=int(user["user_id"]),
            details={"reason": "capacity", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": str(exc),
                "details": {"action": payload.action},
            },
        )
    except Exception as exc:
        auth.audit_event(
            request,
            event_type="admin.action.run",
            result="error",
            actor_user_id=int(user["user_id"]),
            details={"reason": "unexpected", "message": str(exc), "action": payload.action},
        )
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_error",
                "message": "Failed to start admin action",
                "details": {"action": payload.action},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.action.run",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={
            "action": payload.action,
            "job_id": job["job_id"],
            "correlation_id": job["job_id"],
        },
    )
    return {"job": job}


@admin_router.get("/actions/jobs")
def admin_actions_jobs(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
):
    auth.require_admin(request)
    return {"items": admin_actions.list_jobs(limit=limit)}


@admin_router.get("/actions/jobs/{job_id}")
def admin_actions_job(request: Request, job_id: str):
    auth.require_admin(request)
    try:
        return {"job": admin_actions.get_job(job_id)}
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )


@admin_router.get("/actions/jobs/{job_id}/log")
def admin_actions_job_log(
    request: Request,
    job_id: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=65536, ge=1024, le=1048576),
):
    auth.require_admin(request)
    try:
        return admin_actions.read_job_log(job_id, offset=offset, limit=limit)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )


@admin_router.get("/actions/jobs/{job_id}/log/download", response_class=PlainTextResponse)
def admin_actions_job_log_download(request: Request, job_id: str):
    auth.require_admin(request)
    try:
        payload = admin_actions.read_full_job_log(job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )
    headers = {
        "Content-Disposition": f'attachment; filename="{job_id}.log"',
        "X-Job-Status": str(payload["status"]),
    }
    return PlainTextResponse(content=str(payload["chunk"]), headers=headers)


@admin_router.post("/actions/jobs/{job_id}/cancel")
def admin_actions_job_cancel(request: Request, job_id: str):
    user = auth.require_admin(request)
    auth.enforce_csrf(request, user)
    try:
        job = admin_actions.cancel_job(job_id=job_id)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "not_found",
                "message": "Job not found",
                "details": {"job_id": job_id},
            },
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "conflict",
                "message": str(exc),
                "details": {"job_id": job_id},
            },
        )
    auth.audit_event(
        request,
        event_type="admin.action.cancel",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"job_id": job_id, "correlation_id": job_id, "action": job.get("action")},
    )
    return {"job": job}


@admin_router.get("/backups")
def admin_backups(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
):
    auth.require_admin(request)
    return admin_actions.list_backups(limit=limit)


@admin_router.get("/audit")
def admin_audit_log(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    before_audit_id: Optional[int] = Query(default=None, ge=1),
    event_type: Optional[str] = Query(default=None, min_length=1, max_length=128),
    event_prefix: Optional[str] = Query(default=None, min_length=1, max_length=128),
    result: Optional[str] = Query(default=None, min_length=1, max_length=16),
    request_id: Optional[str] = Query(default=None, min_length=1, max_length=128),
    actor_user_id: Optional[int] = Query(default=None, ge=1),
):
    auth.require_admin(request)
    if result:
        normalized_result = result.strip().lower()
        if normalized_result not in {"success", "deny", "error"}:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "bad_request",
                    "message": "Invalid result filter",
                    "details": {"result": result},
                },
            )
        result = normalized_result
    if event_type:
        event_type = event_type.strip()
    if event_prefix:
        event_prefix = event_prefix.strip()
    if request_id:
        request_id = request_id.strip()

    where_clauses = []
    params: list[Any] = []
    if before_audit_id is not None:
        where_clauses.append("audit_id < ?")
        params.append(before_audit_id)
    if event_type:
        where_clauses.append("event_type = ?")
        params.append(event_type)
    if event_prefix:
        where_clauses.append("event_type LIKE ?")
        params.append(f"{event_prefix}%")
    if result:
        where_clauses.append("result = ?")
        params.append(result)
    if request_id:
        where_clauses.append("request_id = ?")
        params.append(request_id)
    if actor_user_id is not None:
        where_clauses.append("actor_user_id = ?")
        params.append(actor_user_id)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
SELECT audit_id, actor_user_id, event_type, result, request_id, route, method, details_json, created_at
FROM audit_log
{where_sql}
ORDER BY audit_id DESC
LIMIT ?
    """
    params.append(limit)

    items = []
    with admin_db.connection_scope() as con:
        rows = con.execute(query, params).fetchall()
    for row in rows:
        details: Any = {}
        raw_details = row["details_json"]
        if raw_details:
            try:
                details = json.loads(raw_details)
            except Exception:
                details = {"raw": raw_details}
        correlation_id = None
        if isinstance(details, dict):
            correlation_id = details.get("correlation_id") or details.get("job_id")
        items.append(
            {
                "audit_id": row["audit_id"],
                "actor_user_id": row["actor_user_id"],
                "event_type": row["event_type"],
                "result": row["result"],
                "request_id": row["request_id"],
                "route": row["route"],
                "method": row["method"],
                "details": details,
                "correlation_id": correlation_id,
                "created_at": row["created_at"],
            }
        )
    next_before = items[-1]["audit_id"] if items else None
    return {"items": items, "next_before_audit_id": next_before}


app.include_router(admin_router)


@app.get("/admin", response_class=HTMLResponse)
@app.get("/api/v1/admin/ui", response_class=HTMLResponse)
def admin_home(request: Request):
    if not auth.is_enabled():
        return HTMLResponse("<h1>Spacegate Admin</h1><p>Auth is disabled.</p>", status_code=503)
    next_path = request.url.path if str(request.url.path).startswith("/api/v1/admin/") else "/api/v1/admin/ui"
    if not getattr(request.state, "auth_user", None):
        return auth.login_redirect(request, next_path=next_path)
    user = auth.require_admin(request)
    email = user["email"]
    display_name = user["display_name"]
    csrf_cookie_name = auth.get_config().csrf_cookie_name
    body = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Spacegate Admin</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #f7f8fa;
        --text: #1f2937;
        --muted: #6b7280;
        --card: #ffffff;
        --border: #d1d5db;
        --ok: #15803d;
        --warn: #b45309;
        --err: #b91c1c;
        --brand: #0f766e;
      }}
      body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 1.25rem; line-height: 1.4; background: var(--bg); color: var(--text); }}
      h1, h2, h3 {{ margin: 0.5rem 0; }}
      .toolbar {{ display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem; }}
      .screen-nav {{ display: flex; gap: 0.45rem; flex-wrap: wrap; margin-bottom: 0.9rem; }}
      .screen-nav button.active {{ border-color: var(--brand); color: var(--brand); }}
      .screen {{ display: none; }}
      .screen.active {{ display: block; }}
      .section {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 0.75rem; margin-bottom: 0.9rem; }}
      .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 0.75rem; }}
      .coolness-layout {{ grid-template-columns: minmax(300px, 35fr) minmax(420px, 65fr); }}
      .action-card {{ border: 1px solid var(--border); border-radius: 8px; padding: 0.65rem; }}
      .action-meta {{ color: var(--muted); font-size: 0.9rem; margin-bottom: 0.5rem; }}
      .field {{ margin-bottom: 0.45rem; }}
      .field label {{ display: block; font-size: 0.86rem; color: var(--muted); margin-bottom: 0.15rem; }}
      .field input[type=text], .field input[type=number], .field select {{ width: 100%; box-sizing: border-box; padding: 0.35rem; border: 1px solid var(--border); border-radius: 6px; }}
      .small {{ font-size: 0.82rem; color: var(--muted); }}
      code {{ background: #eef2f7; padding: 0.1rem 0.25rem; border-radius: 4px; }}
      button {{ padding: 0.45rem 0.65rem; cursor: pointer; border: 1px solid var(--border); background: white; border-radius: 6px; }}
      button.primary {{ background: var(--brand); color: white; border-color: var(--brand); }}
      button.warn {{ border-color: var(--warn); color: var(--warn); }}
      button.danger {{ border-color: var(--err); color: var(--err); }}
      button.preset-btn.active {{ background: #ecfeff; border-color: #0891b2; color: #0e7490; font-weight: 700; }}
      .status-badge {{ display: inline-block; border-radius: 999px; padding: 0.05rem 0.45rem; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.02em; border: 1px solid; }}
      .status-running {{ color: #1d4ed8; border-color: #93c5fd; }}
      .status-queued {{ color: #4338ca; border-color: #c4b5fd; }}
      .status-succeeded {{ color: var(--ok); border-color: #86efac; }}
      .status-failed {{ color: var(--err); border-color: #fca5a5; }}
      .status-cancelled {{ color: #6b7280; border-color: #d1d5db; }}
      .inline {{ display: inline-flex; align-items: center; gap: 0.35rem; flex-wrap: wrap; }}
      .jobs-list, .audit-list, .backup-list {{ list-style: none; margin: 0; padding: 0; }}
      .jobs-list li, .audit-list li, .backup-list li {{ border-bottom: 1px solid #eef2f7; padding: 0.35rem 0; }}
      .jobs-list li:last-child, .audit-list li:last-child, .backup-list li:last-child {{ border-bottom: 0; }}
      pre {{ background: #0f172a; color: #e2e8f0; padding: 0.6rem; border-radius: 8px; overflow: auto; max-height: 28rem; }}
      .audit-presets button.active {{ border-color: var(--brand); color: var(--brand); }}
      .muted {{ color: var(--muted); }}
      .weight-grid {{ display: grid; gap: 0.45rem; }}
      .weight-row {{ display: grid; grid-template-columns: 140px 1fr 76px; gap: 0.5rem; align-items: center; }}
      .weight-row input[type=range] {{ width: 100%; }}
      .weight-row input[type=number] {{ width: 100%; }}
      .json-box {{ background: #0f172a; color: #e2e8f0; padding: 0.6rem; border-radius: 8px; overflow: auto; max-height: 16rem; }}
      .guidance {{ margin: 0.25rem 0 0.5rem 1.1rem; color: var(--muted); }}
      .guidance li {{ margin: 0.15rem 0; }}
      .note-box {{ border: 1px dashed var(--border); border-radius: 8px; padding: 0.5rem; background: #fafafa; }}
      .preview-grid {{ display: grid; gap: 0.6rem; }}
      .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr)); gap: 0.5rem; }}
      .kpi {{ border: 1px solid var(--border); border-radius: 8px; padding: 0.45rem; background: #fff; }}
      .kpi .k {{ color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.02em; }}
      .kpi .v {{ font-size: 1rem; font-weight: 700; }}
      .changes-list {{ list-style: none; margin: 0; padding: 0; }}
      .changes-list li {{ border-bottom: 1px solid #eef2f7; padding: 0.28rem 0; }}
      .changes-list li:last-child {{ border-bottom: 0; }}
      .bar-list {{ display: grid; gap: 0.35rem; }}
      .bar-row {{ display: grid; grid-template-columns: 100px 1fr 90px; gap: 0.45rem; align-items: center; }}
      .bar-track {{ background: #eef2f7; border-radius: 999px; height: 10px; overflow: hidden; }}
      .bar-fill {{ background: var(--brand); height: 100%; border-radius: 999px; }}
      .mini-table {{ width: 100%; border-collapse: collapse; font-size: 0.86rem; }}
      .mini-table th, .mini-table td {{ border-bottom: 1px solid #eef2f7; padding: 0.28rem 0.2rem; text-align: left; }}
      .mini-table th {{ color: var(--muted); font-weight: 600; }}
      @media (max-width: 1100px) {{
        .coolness-layout {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <h1>Spacegate Admin</h1>
    <p>Signed in as <strong>{display_name}</strong> (<code>{email}</code>).</p>
    <p class="muted">Actions are allowlisted. Destructive actions require explicit confirmation phrases.</p>

    <div class="toolbar">
      <button id="logout" class="danger">Log out</button>
      <button id="refreshStatus">Refresh Status</button>
      <button id="refreshJobs">Refresh Jobs</button>
      <button id="refreshBackups">Refresh Backups</button>
      <button id="refreshAudit">Refresh Audit</button>
    </div>

    <div class="screen-nav">
      <button id="screenTabOperations" class="active">Operations</button>
      <button id="screenTabCoolness">Coolness</button>
      <button id="screenTabActivity">Activity</button>
    </div>

    <div id="screenOperations" class="screen active">
      <div class="section">
        <h2>Operations</h2>
        <div id="actionsOps" class="grid"></div>
      </div>
      <div class="section">
        <h2>Status</h2>
        <pre id="out"></pre>
      </div>
    </div>

    <div id="screenCoolness" class="screen">
      <div class="section">
        <h2>Coolness Tuning</h2>
        <p class="muted">Sandbox mode: you can turn knobs and run without entering any IDs first.</p>
        <p class="muted">
          Why this exists: a raw catalog naturally over-represents common objects, so discovery can feel repetitive.
          Coolness tuning intentionally balances the ranking to surface a wider range of stellar and planetary systems
          while keeping scientific source data unchanged.
        </p>
        <p class="muted">
          In plain terms: this lets you steer what visitors discover first, so the map stays educational, surprising,
          and varied instead of collapsing into one kind of object.
        </p>
        <ul class="guidance">
          <li><strong>Preview</strong>: auto-refreshed summary from the latest run output; no manual preview action needed.</li>
          <li><strong>Run</strong>: writes rich ranking outputs (`rich.duckdb`, Parquet, report) for the current build using your current sliders, but does not persist a new profile version.</li>
          <li><strong>Save Profile</strong>: stores the current slider mix as an immutable profile version, without activating it.</li>
          <li><strong>Activate Profile</strong>: points the active profile to a saved immutable version.</li>
          <li>Core astronomy data is not modified by coolness tuning.</li>
          <li>Profiles are immutable by version: changed weights require a new profile version.</li>
        </ul>
        <details>
          <summary>Advanced script actions</summary>
          <div id="actionsCoolness" class="grid" style="margin-top:0.6rem;"></div>
        </details>
      </div>
      <div class="section">
        <h2>Tuning Preview</h2>
        <p class="muted">Adjust weights with sliders, run read-only preview, use Run for ephemeral scoring, Save Profile when you want to persist, and Activate Profile when you want that version live.</p>
        <div class="grid coolness-layout">
          <div class="action-card">
            <h3>Weight Controls</h3>
            <div class="field">
              <label for="coolProfileId">Profile ID</label>
              <input id="coolProfileId" type="text" placeholder="default" />
            </div>
            <div class="field">
              <label for="coolProfileVersion">Profile version (optional for Save)</label>
              <input id="coolProfileVersion" type="text" placeholder="auto" />
            </div>
            <div class="field">
              <label for="coolSavedProfiles">Saved profiles</label>
              <div class="inline">
                <select id="coolSavedProfiles" style="min-width: 260px;"></select>
                <button id="coolLoadProfileBtn" type="button">Load Saved</button>
              </div>
            </div>
            <div class="field">
              <label for="coolTopN">Preview sample size (Top N)</label>
              <input id="coolTopN" type="number" min="20" max="1000" step="10" value="200" />
            </div>
            <div class="inline" style="margin-bottom: 0.45rem;">
              <button id="presetBalanced" type="button" class="preset-btn">Balanced</button>
              <button id="presetExotic" type="button" class="preset-btn">Exotic</button>
              <button id="presetHabitable" type="button" class="preset-btn">Habitable</button>
              <button id="presetNearby" type="button" class="preset-btn">Nearby</button>
              <button id="coolResetActive" type="button" class="preset-btn">Reset To Active</button>
            </div>
            <div id="coolnessSliders" class="weight-grid"></div>
            <div class="field">
              <details>
                <summary>Current weights JSON (advanced)</summary>
                <pre id="coolWeightsJson" class="json-box"></pre>
              </details>
            </div>
            <div class="inline">
              <button id="coolApplyBtn" type="button" class="primary">Run</button>
              <button id="coolSaveBtn" type="button" class="warn">Save Profile</button>
              <button id="coolActivateBtn" type="button">Activate Profile</button>
            </div>
            <div id="coolRunStatus" class="note-box small" style="margin-top:0.45rem;">Status: idle</div>
            <div id="coolPreviewNotice" class="note-box small" style="margin-top:0.45rem;"></div>
          </div>
          <div class="action-card">
            <h3>Preview Summary</h3>
            <div class="preview-grid">
              <div id="coolPreviewSummary" class="kpis"></div>
              <div>
                <h4>Weight Changes vs Active</h4>
                <ul id="coolPreviewChanges" class="changes-list"></ul>
              </div>
              <div>
                <h4>Type Distribution</h4>
                <div id="coolPreviewTypeDist" class="bar-list"></div>
              </div>
              <div>
                <h4>Spectral Distribution</h4>
                <div id="coolPreviewSpectralDist" class="bar-list"></div>
              </div>
              <div>
                <h4>Top Systems (Preview)</h4>
                <table class="mini-table">
                  <thead>
                    <tr><th>Rank</th><th>System</th><th>Score</th><th>Dist (ly)</th><th>Stars</th><th>Planets</th><th>Type</th><th>Subscores</th></tr>
                  </thead>
                  <tbody id="coolPreviewTopSystems"></tbody>
                </table>
              </div>
              <details>
                <summary>Raw preview JSON</summary>
                <pre id="coolPreviewOut" class="json-box"></pre>
              </details>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div id="screenActivity" class="screen">
      <div class="section grid">
        <div>
          <h2>Jobs</h2>
          <ul id="jobs" class="jobs-list"></ul>
        </div>
        <div>
          <h2>Backups</h2>
          <h3>Admin DB Snapshots</h3>
          <ul id="backupsAdminDb" class="backup-list"></ul>
          <h3>Release Metadata Snapshots</h3>
          <ul id="backupsReleaseMeta" class="backup-list"></ul>
        </div>
      </div>

      <div class="section">
        <h2>Selected Job Log</h2>
        <div id="selectedJob" class="small"></div>
        <pre id="log"></pre>
      </div>

      <div class="section">
        <h2>Audit Log</h2>
        <div class="inline audit-presets">
          <button id="auditPresetAll" class="active">All</button>
          <button id="auditPresetAuth">Auth</button>
          <button id="auditPresetActions">Admin Actions</button>
        </div>
        <div class="inline" style="margin-top: 0.45rem;">
          <label for="auditEventType">event_type</label>
          <input id="auditEventType" placeholder="auth.login.denied" />
          <label for="auditResult">result</label>
          <select id="auditResult">
            <option value="">(any)</option>
            <option value="success">success</option>
            <option value="deny">deny</option>
            <option value="error">error</option>
          </select>
          <label for="auditRequestId">request_id</label>
          <input id="auditRequestId" placeholder="req_..." />
          <button id="loadOlderAudit">Load Older</button>
        </div>
        <ul id="audit" class="audit-list"></ul>
        <h3>Selected Audit Details</h3>
        <pre id="auditDetails"></pre>
      </div>
    </div>

    <script>
      const out = document.getElementById('out');
      const actionsOpsEl = document.getElementById('actionsOps');
      const actionsCoolnessEl = document.getElementById('actionsCoolness');
      const jobsEl = document.getElementById('jobs');
      const selectedJobEl = document.getElementById('selectedJob');
      const logEl = document.getElementById('log');
      const auditEl = document.getElementById('audit');
      const auditDetailsEl = document.getElementById('auditDetails');
      const backupsAdminDbEl = document.getElementById('backupsAdminDb');
      const backupsReleaseMetaEl = document.getElementById('backupsReleaseMeta');
      const auditEventTypeEl = document.getElementById('auditEventType');
      const auditResultEl = document.getElementById('auditResult');
      const auditRequestIdEl = document.getElementById('auditRequestId');
      const coolProfileIdEl = document.getElementById('coolProfileId');
      const coolProfileVersionEl = document.getElementById('coolProfileVersion');
      const coolTopNEl = document.getElementById('coolTopN');
      const coolResetActiveEl = document.getElementById('coolResetActive');
      const coolSavedProfilesEl = document.getElementById('coolSavedProfiles');
      const coolLoadProfileBtnEl = document.getElementById('coolLoadProfileBtn');
      const coolnessSlidersEl = document.getElementById('coolnessSliders');
      const coolWeightsJsonEl = document.getElementById('coolWeightsJson');
      const coolPreviewOutEl = document.getElementById('coolPreviewOut');
      const coolPreviewSummaryEl = document.getElementById('coolPreviewSummary');
      const coolPreviewChangesEl = document.getElementById('coolPreviewChanges');
      const coolPreviewTypeDistEl = document.getElementById('coolPreviewTypeDist');
      const coolPreviewSpectralDistEl = document.getElementById('coolPreviewSpectralDist');
      const coolPreviewTopSystemsEl = document.getElementById('coolPreviewTopSystems');
	      const coolRunStatusEl = document.getElementById('coolRunStatus');
		      const coolPreviewNoticeEl = document.getElementById('coolPreviewNotice');
		      const coolApplyBtnEl = document.getElementById('coolApplyBtn');
		      const coolSaveBtnEl = document.getElementById('coolSaveBtn');
	      const coolActivateBtnEl = document.getElementById('coolActivateBtn');
		      const presetBalancedBtnEl = document.getElementById('presetBalanced');
	      const presetExoticBtnEl = document.getElementById('presetExotic');
	      const presetHabitableBtnEl = document.getElementById('presetHabitable');
	      const presetNearbyBtnEl = document.getElementById('presetNearby');
	      const csrfCookieName = '{csrf_cookie_name}';
	      const actionCatalog = new Map();
	      let currentJobId = null;
      let currentOffset = 0;
      let nextAuditBeforeId = null;
      let auditPreset = 'all';
	      let currentScreen = 'operations';
	      let activeCoolnessProfile = null;
	      let activeCoolnessPointer = null;
	      let coolnessProfiles = [];
	      let latestJobs = [];
	      let coolnessFollowJobId = null;
      let activePreset = 'custom';
      const coolnessDefaultWeights = {{
        luminosity: 0.22,
        proper_motion: 0.10,
        multiplicity: 0.14,
        nice_planets: 0.12,
        weird_planets: 0.14,
        proximity: 0.08,
        system_complexity: 0.12,
        exotic_star: 0.08,
      }};
      const coolnessPresetWeights = {{
        balanced: {{ ...coolnessDefaultWeights }},
        exotic: {{
          ...coolnessDefaultWeights,
          exotic_star: 0.22,
          weird_planets: 0.20,
          luminosity: 0.16,
          proximity: 0.04,
        }},
        habitable: {{
          ...coolnessDefaultWeights,
          nice_planets: 0.28,
          proximity: 0.16,
          luminosity: 0.14,
          exotic_star: 0.04,
        }},
        nearby: {{
          ...coolnessDefaultWeights,
          proximity: 0.28,
          proper_motion: 0.18,
          luminosity: 0.14,
          weird_planets: 0.08,
        }},
      }};
      const coolnessLabels = {{
        luminosity: 'Luminosity',
        proper_motion: 'Proper Motion',
        multiplicity: 'Multiplicity',
        nice_planets: 'Nice Planets',
        weird_planets: 'Weird Planets',
        proximity: 'Proximity',
        system_complexity: 'System Complexity',
        exotic_star: 'Exotic Star',
      }};
      let currentCoolnessWeights = {{ ...coolnessDefaultWeights }};

      function setScreen(screenName) {{
        currentScreen = screenName;
        document.getElementById('screenOperations').classList.toggle('active', screenName === 'operations');
        document.getElementById('screenCoolness').classList.toggle('active', screenName === 'coolness');
        document.getElementById('screenActivity').classList.toggle('active', screenName === 'activity');
        document.getElementById('screenTabOperations').classList.toggle('active', screenName === 'operations');
        document.getElementById('screenTabCoolness').classList.toggle('active', screenName === 'coolness');
        document.getElementById('screenTabActivity').classList.toggle('active', screenName === 'activity');
      }}

      function setAuditPreset(name) {{
        auditPreset = name;
        document.getElementById('auditPresetAll').classList.toggle('active', name === 'all');
        document.getElementById('auditPresetAuth').classList.toggle('active', name === 'auth');
        document.getElementById('auditPresetActions').classList.toggle('active', name === 'actions');
      }}

      function csrfToken() {{
        const csrfCookie = document.cookie.split('; ').find(x => x.startsWith(csrfCookieName + '=')) || '';
        return decodeURIComponent((csrfCookie.split('=')[1] || ''));
      }}

	      async function fetchJson(url, options = undefined) {{
        const res = await fetch(url, options);
        let data = null;
        const text = await res.text();
        try {{
          data = text ? JSON.parse(text) : {{}};
        }} catch (_) {{
          data = {{ raw: text }};
	        }}
	        return {{ res, data }};
	      }}

	      function sleep(ms) {{
	        return new Promise((resolve) => setTimeout(resolve, ms));
	      }}

	      function isRunningStatus(status) {{
	        return status === 'queued' || status === 'running';
	      }}

	      const profileTokenRe = /^[A-Za-z0-9_.-]+$/;

	      function sanitizeProfileToken(raw) {{
	        const trimmed = String(raw || '').trim();
	        if (!trimmed) return '';
	        if (profileTokenRe.test(trimmed)) return trimmed;
	        return trimmed
	          .replace(/[^A-Za-z0-9_.-]+/g, '-')
	          .replace(/-+/g, '-')
	          .replace(/^[.-]+|[.-]+$/g, '');
	      }}

	      function normalizeProfileFields() {{
	        const originalId = String(coolProfileIdEl.value || '').trim();
	        const originalVersion = String(coolProfileVersionEl.value || '').trim();
	        const profileId = sanitizeProfileToken(originalId);
	        const profileVersion = sanitizeProfileToken(originalVersion);
	        if (profileId !== originalId) coolProfileIdEl.value = profileId;
	        if (profileVersion !== originalVersion) coolProfileVersionEl.value = profileVersion;
	        return {{
	          originalId,
	          originalVersion,
	          profileId,
	          profileVersion,
	        }};
	      }}

	      function findActiveCoolnessJob() {{
	        return (latestJobs || []).find((job) => {{
	          if (!job || String(job.action || '') !== 'score_coolness') return false;
	          return isRunningStatus(String(job.status || ''));
	        }}) || null;
	      }}

	      async function waitForJobTerminal(jobId, timeoutMs = 20 * 60 * 1000) {{
	        const started = Date.now();
	        setRunStatus('running', `job ${{jobId}}`);
	        while (Date.now() - started <= timeoutMs) {{
	          const {{ res, data }} = await fetchJson(`/api/v1/admin/actions/jobs/${{jobId}}`, {{ credentials: 'include' }});
	          if (!res.ok) {{
	            setRunStatus('error', `job lookup failed (${{jobId}})`);
	            return {{ ok: false, reason: 'lookup_failed', data }};
	          }}
	          const job = (data && data.job) || {{}};
	          const status = String(job.status || '');
	          if (!isRunningStatus(status)) {{
	            return {{ ok: true, job }};
	          }}
	          setRunStatus(status, `job ${{jobId}}`);
	          await sleep(1500);
	        }}
	        setRunStatus('timeout', `job ${{jobId}}`);
	        return {{ ok: false, reason: 'timeout', data: {{}} }};
	      }}

	      async function followCoolnessJobAndPreview(jobId) {{
	        coolnessFollowJobId = String(jobId || '');
	        const waitResult = await waitForJobTerminal(jobId);
	        if (coolnessFollowJobId !== String(jobId || '')) {{
	          return;
	        }}
	        await loadJobs();
	        if (!waitResult.ok) {{
	          if (waitResult.reason === 'timeout') {{
	            setRunStatus('running', `job ${{jobId}} still in progress`);
	            setPreviewNotice(`Scoring job ${{jobId}} is still running. Check Activity > Jobs and run preview when complete.`);
	            return;
	          }}
	          setRunStatus('error', `job ${{jobId}} status unavailable`);
	          setPreviewNotice(`Could not fetch status for scoring job ${{jobId}}. Check Activity > Jobs.`);
	          return;
	        }}
	        const finalJob = waitResult.job || {{}};
	        const finalStatus = String(finalJob.status || '');
	        if (finalStatus !== 'succeeded') {{
	          setRunStatus(finalStatus || 'failed', `job ${{jobId}}`);
	          setPreviewNotice(`Scoring job ${{jobId}} finished with status '${{finalStatus}}'. Check Activity > Jobs log.`);
	          return;
	        }}
	        setRunStatus('refreshing', `job ${{jobId}} completed`);
	        await loadCoolnessState({{ preserveEditor: true }});
	        for (let attempt = 0; attempt < 5; attempt += 1) {{
	          const outcome = await previewCoolness({{ suppressAlert: true }});
	          if (outcome && outcome.ok) {{
	            setRunStatus('ready', `job ${{jobId}} applied`);
	            setPreviewNotice(`Scoring job ${{jobId}} completed. Preview refreshed from persisted outputs.`);
	            return;
	          }}
	          const reason = String((outcome && outcome.reason) || '');
	          if (reason !== 'conflict' && reason !== 'job_running') {{
	            return;
	          }}
	          await sleep(1200);
	        }}
	        setRunStatus('ready', `job ${{jobId}} applied`);
	        setPreviewNotice(`Scoring job ${{jobId}} completed. Preview is temporarily busy; retry in a few seconds.`);
	      }}

	      async function followSaveProfileJob(jobId, profileId, profileVersion) {{
	        const waitResult = await waitForJobTerminal(jobId);
	        await loadJobs();
        if (!waitResult.ok) {{
          if (waitResult.reason === 'timeout') {{
            setRunStatus('running', `save job ${{jobId}} still in progress`);
            setPreviewNotice(`Save job ${{jobId}} is still running. Check Activity > Jobs.`);
            return;
          }}
          setRunStatus('error', `save job ${{jobId}} status unavailable`);
          setPreviewNotice(`Could not fetch status for save job ${{jobId}}. Check Activity > Jobs.`);
          return;
        }}
	        const finalJob = waitResult.job || {{}};
	        const finalStatus = String(finalJob.status || '');
	        if (finalStatus !== 'succeeded') {{
	          let failureHint = '';
	          try {{
	            const res = await fetch(`/api/v1/admin/actions/jobs/${{jobId}}/log/download`, {{ credentials: 'include' }});
	            if (res.ok) {{
	              const text = await res.text();
	              const lines = String(text || '').split(/\\r?\\n/).map((line) => line.trim()).filter(Boolean);
	              for (let i = lines.length - 1; i >= 0; i -= 1) {{
	                const line = lines[i];
	                if (!line) continue;
	                if (line.startsWith('[error]')) {{
	                  failureHint = line.replace(/^\\[error\\]\\s*/, '').trim();
	                  break;
	                }}
	                if (line.startsWith('[20') && line.includes('Finished status=')) continue;
	                if (line.startsWith('[') && line.endsWith('Starting action')) continue;
	                if (line.startsWith('Execution:')) continue;
	                if (line.startsWith('Params:')) continue;
	                if (line.startsWith('Action:')) continue;
	                failureHint = line;
	                break;
	              }}
	            }}
	          }} catch (_) {{
	            failureHint = '';
	          }}
	          setRunStatus(finalStatus || 'failed', `save job ${{jobId}}`);
	          if (failureHint) {{
	            setPreviewNotice(`Save job ${{jobId}} failed: ${{failureHint}}`);
	          }} else {{
	            setPreviewNotice(`Save job ${{jobId}} finished with status '${{finalStatus}}'. Check Activity > Jobs log.`);
	          }}
	          return;
	        }}
        await loadCoolnessState({{ preserveEditor: true, suppressNotice: true }});
        renderSavedProfilesOptions();
        if (coolSavedProfilesEl) {{
          coolSavedProfilesEl.value = profileOptionValue(profileId, profileVersion);
        }}
        setRunStatus('saved', `${{profileId}}@${{profileVersion}}`);
	        setPreviewNotice(
	          `Saved immutable profile ${{profileId}}@${{profileVersion}}. It is stored but not active; active profile remains ${{
	            String((activeCoolnessPointer || {{}}).profile_id || (activeCoolnessProfile || {{}}).profile_id || 'n/a')
          }}@${{
            String((activeCoolnessPointer || {{}}).profile_version || (activeCoolnessProfile || {{}}).profile_version || 'n/a')
          }}.`
	        );
	      }}

	      async function followActivateProfileJob(jobId, profileId, profileVersion) {{
	        const waitResult = await waitForJobTerminal(jobId);
	        await loadJobs();
	        if (!waitResult.ok) {{
	          if (waitResult.reason === 'timeout') {{
	            setRunStatus('running', `activate job ${{jobId}} still in progress`);
	            setPreviewNotice(`Activate job ${{jobId}} is still running. Check Activity > Jobs.`);
	            return;
	          }}
	          setRunStatus('error', `activate job ${{jobId}} status unavailable`);
	          setPreviewNotice(`Could not fetch status for activate job ${{jobId}}. Check Activity > Jobs.`);
	          return;
	        }}
	        const finalJob = waitResult.job || {{}};
	        const finalStatus = String(finalJob.status || '');
	        if (finalStatus !== 'succeeded') {{
	          let failureHint = '';
	          try {{
	            const res = await fetch(`/api/v1/admin/actions/jobs/${{jobId}}/log/download`, {{ credentials: 'include' }});
	            if (res.ok) {{
	              const text = await res.text();
	              const lines = String(text || '').split(/\\r?\\n/).map((line) => line.trim()).filter(Boolean);
	              for (let i = lines.length - 1; i >= 0; i -= 1) {{
	                const line = lines[i];
	                if (!line) continue;
	                if (line.startsWith('[error]')) {{
	                  failureHint = line.replace(/^\\[error\\]\\s*/, '').trim();
	                  break;
	                }}
	                if (line.startsWith('[20') && line.includes('Finished status=')) continue;
	                if (line.startsWith('[') && line.endsWith('Starting action')) continue;
	                if (line.startsWith('Execution:')) continue;
	                if (line.startsWith('Params:')) continue;
	                if (line.startsWith('Action:')) continue;
	                failureHint = line;
	                break;
	              }}
	            }}
	          }} catch (_) {{
	            failureHint = '';
	          }}
	          setRunStatus(finalStatus || 'failed', `activate job ${{jobId}}`);
	          if (failureHint) {{
	            setPreviewNotice(`Activate job ${{jobId}} failed: ${{failureHint}}`);
	          }} else {{
	            setPreviewNotice(`Activate job ${{jobId}} finished with status '${{finalStatus}}'. Check Activity > Jobs log.`);
	          }}
	          return;
	        }}
	        await loadCoolnessState({{ preserveEditor: false, suppressNotice: true }});
	        const outcome = await previewCoolness({{ suppressAlert: true }});
	        setRunStatus('activated', `${{profileId}}@${{profileVersion}}`);
	        if (outcome && outcome.ok) {{
	          setPreviewNotice(`Activated profile ${{profileId}}@${{profileVersion}} and refreshed preview.`);
	        }} else {{
	          setPreviewNotice(`Activated profile ${{profileId}}@${{profileVersion}}.`);
	        }}
	      }}

	      function statusClass(status) {{
        const key = String(status || '').toLowerCase();
        if (key === 'running') return 'status-running';
        if (key === 'queued') return 'status-queued';
        if (key === 'succeeded') return 'status-succeeded';
        if (key === 'failed') return 'status-failed';
        if (key === 'cancelled') return 'status-cancelled';
        return '';
      }}

      function normalizeCoolnessWeights(raw) {{
        const outWeights = {{ ...coolnessDefaultWeights }};
        if (!raw || typeof raw !== 'object') return outWeights;
        Object.keys(coolnessDefaultWeights).forEach((key) => {{
          const value = Number(raw[key]);
          if (Number.isFinite(value) && value >= 0) outWeights[key] = value;
        }});
        return outWeights;
      }}

      function weightsEqual(left, right) {{
        const l = normalizeCoolnessWeights(left);
        const r = normalizeCoolnessWeights(right);
        return Object.keys(coolnessDefaultWeights).every((key) => Math.abs((l[key] || 0) - (r[key] || 0)) < 1e-12);
      }}

      function suggestNextProfileVersion(currentVersion) {{
        const raw = String(currentVersion || '').trim();
        if (!raw) return '1';
        if (/^\\d+$/.test(raw)) {{
          const n = Number.parseInt(raw, 10);
          if (Number.isFinite(n) && n >= 0) return String(n + 1);
        }}
        const suffixMatch = raw.match(/^(.*?)-(\\d+)$/);
        if (suffixMatch) {{
          const base = suffixMatch[1];
          const n = Number.parseInt(suffixMatch[2], 10);
          if (Number.isFinite(n) && n >= 0) return `${{base}}-${{n + 1}}`;
        }}
        return `${{raw}}.1`;
      }}

      function toNumber(value, fallback = 0) {{
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
      }}

      function pct(part, total) {{
        const p = toNumber(part, 0);
        const t = toNumber(total, 0);
        if (t <= 0) return 0;
        return (p / t) * 100;
      }}

      function clearPreviewVisuals() {{
        coolPreviewSummaryEl.innerHTML = '';
        coolPreviewChangesEl.innerHTML = '';
        coolPreviewTypeDistEl.innerHTML = '';
        coolPreviewSpectralDistEl.innerHTML = '';
        coolPreviewTopSystemsEl.innerHTML = '';
      }}

      function setPreviewNotice(message) {{
        coolPreviewNoticeEl.textContent = message || '';
      }}

      function setRunStatus(status, detail = '') {{
        const head = String(status || 'idle').trim().toLowerCase() || 'idle';
        coolRunStatusEl.textContent = detail ? `Status: ${{head}} | ${{detail}}` : `Status: ${{head}}`;
      }}

      function setActivePreset(name) {{
        activePreset = String(name || 'custom');
        const mapping = {{
          balanced: presetBalancedBtnEl,
          exotic: presetExoticBtnEl,
          habitable: presetHabitableBtnEl,
          nearby: presetNearbyBtnEl,
          reset: coolResetActiveEl,
        }};
        Object.entries(mapping).forEach(([key, btn]) => {{
          if (!btn) return;
          btn.classList.toggle('active', key === activePreset);
        }});
      }}

      function renderKpiCard(label, value) {{
        const box = document.createElement('div');
        box.className = 'kpi';
        const k = document.createElement('div');
        k.className = 'k';
        k.textContent = label;
        const v = document.createElement('div');
        v.className = 'v';
        v.textContent = String(value);
        box.appendChild(k);
        box.appendChild(v);
        return box;
      }}

      function renderBarList(target, rows, total) {{
        target.innerHTML = '';
        if (!rows.length) {{
          const empty = document.createElement('div');
          empty.className = 'small muted';
          empty.textContent = 'No data';
          target.appendChild(empty);
          return;
        }}
        rows.forEach((row) => {{
          const line = document.createElement('div');
          line.className = 'bar-row';
          const label = document.createElement('div');
          label.textContent = String(row.label || '');
          const track = document.createElement('div');
          track.className = 'bar-track';
          const fill = document.createElement('div');
          fill.className = 'bar-fill';
          const amount = toNumber(row.value, 0);
          const share = Math.max(0, Math.min(100, pct(amount, total)));
          fill.style.width = `${{share.toFixed(1)}}%`;
          track.appendChild(fill);
          const value = document.createElement('div');
          value.className = 'small';
          value.textContent = `${{amount}} (${{share.toFixed(1)}}%)`;
          line.appendChild(label);
          line.appendChild(track);
          line.appendChild(value);
          target.appendChild(line);
        }});
      }}

      function renderCoolnessPreview(data) {{
        clearPreviewVisuals();
        if (!data || typeof data !== 'object') {{
          return;
        }}

        const preview = (data.preview && typeof data.preview === 'object') ? data.preview : {{}};
        const diversity = (data.diversity && typeof data.diversity === 'object') ? data.diversity : {{}};
        const diversityScores = (diversity.diversity_scores && typeof diversity.diversity_scores === 'object') ? diversity.diversity_scores : {{}};
        const candidate = (preview.candidate && typeof preview.candidate === 'object') ? preview.candidate : {{}};
        const diff = (preview.diff_vs_active && typeof preview.diff_vs_active === 'object') ? preview.diff_vs_active : {{}};
        const changed = Array.isArray(diff.changed) ? diff.changed : [];
        const sampleSize = toNumber(diversity.sample_size, 0);
        const topN = toNumber(diversity.top_n, toNumber(coolTopNEl.value, 200));

        coolPreviewSummaryEl.appendChild(renderKpiCard('Source', preview.source || 'n/a'));
        coolPreviewSummaryEl.appendChild(renderKpiCard('Profile', `${{candidate.profile_id || 'n/a'}}@${{candidate.profile_version || 'n/a'}}`));
        coolPreviewSummaryEl.appendChild(renderKpiCard('Weights Changed', toNumber(diff.changed_count, changed.length)));
        coolPreviewSummaryEl.appendChild(renderKpiCard('Sample Size', `${{sampleSize}} / ${{topN}}`));
        coolPreviewSummaryEl.appendChild(
          renderKpiCard(
            'Shannon Diversity',
            `${{(toNumber(diversityScores.spectral_shannon_normalized, 0) * 100).toFixed(1)}}%`
          )
        );
        coolPreviewSummaryEl.appendChild(
          renderKpiCard(
            'Effective Spectral Classes',
            toNumber(diversityScores.spectral_effective_classes, 1).toFixed(2)
          )
        );

        if (!changed.length) {{
          const li = document.createElement('li');
          li.className = 'small muted';
          li.textContent = 'No changes vs active weights.';
          coolPreviewChangesEl.appendChild(li);
        }} else {{
          changed.slice(0, 12).forEach((row) => {{
            const li = document.createElement('li');
            const delta = toNumber(row.delta, 0);
            const sign = delta >= 0 ? '+' : '';
            li.textContent = `${{row.key}}: ${{toNumber(row.left, 0).toFixed(2)}} -> ${{toNumber(row.right, 0).toFixed(2)}} (${{sign}}${{delta.toFixed(2)}})`;
            coolPreviewChangesEl.appendChild(li);
          }});
        }}

        const td = (diversity.type_distribution && typeof diversity.type_distribution === 'object') ? diversity.type_distribution : {{}};
        renderBarList(
          coolPreviewTypeDistEl,
          [
            {{ label: 'With planets', value: toNumber(td.with_planets, 0) }},
            {{ label: 'Without planets', value: toNumber(td.without_planets, 0) }},
            {{ label: 'Multi-star', value: toNumber(td.multi_star, 0) }},
            {{ label: 'Single-star', value: toNumber(td.single_star, 0) }},
            {{ label: 'Weird planets', value: toNumber(td.weird_planet_systems, 0) }},
          ],
          sampleSize
        );

        const spectral = Array.isArray(diversity.spectral_distribution) ? diversity.spectral_distribution : [];
        renderBarList(
          coolPreviewSpectralDistEl,
          spectral.slice(0, 8).map((row) => ({{
            label: String(row.spectral_class || '?'),
            value: toNumber(row.systems, 0),
          }})),
          sampleSize
        );

        const topSystems = Array.isArray(diversity.top_systems) ? diversity.top_systems : [];
        if (!topSystems.length) {{
          const tr = document.createElement('tr');
          const tdEmpty = document.createElement('td');
          tdEmpty.colSpan = 8;
          tdEmpty.className = 'small muted';
          tdEmpty.textContent = 'No top systems in preview.';
          tr.appendChild(tdEmpty);
          coolPreviewTopSystemsEl.appendChild(tr);
        }} else {{
          const subscoreKeys = [
            ['L', 'luminosity'],
            ['PM', 'proper_motion'],
            ['Mul', 'multiplicity'],
            ['Nice', 'nice_planets'],
            ['Weird', 'weird_planets'],
            ['Near', 'proximity'],
            ['Complex', 'system_complexity'],
            ['Exotic', 'exotic_star'],
          ];
          topSystems.slice(0, 12).forEach((row, idx) => {{
            const tr = document.createElement('tr');
            const rank = document.createElement('td');
            rank.textContent = String(idx + 1);
            const name = document.createElement('td');
            name.textContent = String(row.system_name || row.stable_object_key || row.system_id || '?');
            const score = document.createElement('td');
            score.textContent = toNumber(row.score_total, 0).toFixed(2);
            const dist = document.createElement('td');
            const distLy = row.dist_ly;
            dist.textContent = (distLy === null || distLy === undefined) ? 'n/a' : toNumber(distLy, 0).toFixed(1);
            const stars = document.createElement('td');
            stars.textContent = String(toNumber(row.star_count, 0));
            const planets = document.createElement('td');
            planets.textContent = String(toNumber(row.planet_count, 0));
            const typ = document.createElement('td');
            typ.textContent = String(row.dominant_spectral_class || '?');
            const subs = document.createElement('td');
            const subscores = (row.subscores && typeof row.subscores === 'object') ? row.subscores : {{}};
            subs.textContent = subscoreKeys.map(([label, key]) => `${{label}}:${{toNumber(subscores[key], 0).toFixed(1)}}`).join(' ');
            tr.appendChild(rank);
            tr.appendChild(name);
            tr.appendChild(score);
            tr.appendChild(dist);
            tr.appendChild(stars);
            tr.appendChild(planets);
            tr.appendChild(typ);
            tr.appendChild(subs);
            coolPreviewTopSystemsEl.appendChild(tr);
          }});
        }}
      }}

      function updateCoolnessJsonPreview() {{
        coolWeightsJsonEl.textContent = JSON.stringify(currentCoolnessWeights, null, 2);
      }}

	      function setCoolnessWeights(nextWeights, presetName = 'custom') {{
	        currentCoolnessWeights = normalizeCoolnessWeights(nextWeights);
	        renderCoolnessSliders();
        setActivePreset(presetName);
	      }}

	      function findCoolnessProfile(profileId, profileVersion) {{
	        const pid = String(profileId || '');
	        const pver = String(profileVersion || '');
	        return (coolnessProfiles || []).find((row) => {{
	          if (!row || typeof row !== 'object') return false;
	          return String(row.profile_id || '') === pid && String(row.profile_version || '') === pver;
	        }}) || null;
	      }}

      function profileOptionValue(profileId, profileVersion) {{
        return `${{String(profileId || '')}}@@${{String(profileVersion || '')}}`;
      }}

      function splitProfileOptionValue(raw) {{
        const value = String(raw || '');
        const sep = value.indexOf('@@');
        if (sep < 0) return [value, ''];
        return [value.slice(0, sep), value.slice(sep + 2)];
      }}

      function renderSavedProfilesOptions() {{
        if (!coolSavedProfilesEl) return;
        const previous = coolSavedProfilesEl.value;
        const rows = (coolnessProfiles || [])
          .filter((row) => row && typeof row === 'object')
          .slice()
          .sort((a, b) => {{
            const ap = String(a.profile_id || '');
            const bp = String(b.profile_id || '');
            if (ap !== bp) return ap.localeCompare(bp);
            const av = String(a.profile_version || '');
            const bv = String(b.profile_version || '');
            if (/^\\d+$/.test(av) && /^\\d+$/.test(bv)) return Number.parseInt(bv, 10) - Number.parseInt(av, 10);
            return bv.localeCompare(av);
          }});

        coolSavedProfilesEl.innerHTML = '';
        if (!rows.length) {{
          const opt = document.createElement('option');
          opt.value = '';
          opt.textContent = '(no saved profiles)';
          coolSavedProfilesEl.appendChild(opt);
          return;
        }}
        const activeId = String((activeCoolnessPointer || {{}}).profile_id || (activeCoolnessProfile || {{}}).profile_id || '');
        const activeVersion = String((activeCoolnessPointer || {{}}).profile_version || (activeCoolnessProfile || {{}}).profile_version || '');
        rows.forEach((row) => {{
          const pid = String(row.profile_id || '');
          const pver = String(row.profile_version || '');
          const opt = document.createElement('option');
          opt.value = profileOptionValue(pid, pver);
          const activeTag = (pid === activeId && pver === activeVersion) ? ' [active]' : '';
          opt.textContent = `${{pid}}@${{pver}}${{activeTag}}`;
          coolSavedProfilesEl.appendChild(opt);
        }});
        const fallback = profileOptionValue(
          String(coolProfileIdEl.value || activeId || ''),
          String(coolProfileVersionEl.value || activeVersion || '')
        );
        const wanted = previous || fallback;
        if (Array.from(coolSavedProfilesEl.options).some((opt) => opt.value === wanted)) {{
          coolSavedProfilesEl.value = wanted;
        }}
      }}

      function loadSelectedSavedProfile() {{
        const [profileId, profileVersion] = splitProfileOptionValue(coolSavedProfilesEl ? coolSavedProfilesEl.value : '');
        if (!profileId || !profileVersion) {{
          setPreviewNotice('Pick a saved profile before loading.');
          return;
        }}
        const profile = findCoolnessProfile(profileId, profileVersion);
        if (!profile) {{
          setPreviewNotice(`Saved profile ${{profileId}}@${{profileVersion}} was not found.`);
          return;
        }}
        coolProfileIdEl.value = profileId;
        coolProfileVersionEl.value = profileVersion;
        setCoolnessWeights(profile.weights || coolnessDefaultWeights, 'custom');
        setRunStatus('loaded', `${{profileId}}@${{profileVersion}}`);
        setPreviewNotice(`Loaded saved profile ${{profileId}}@${{profileVersion}} into weight controls.`);
      }}

      function nextUnusedProfileVersion(profileId, startVersion = '') {{
        const pid = String(profileId || '').trim();
        const used = new Set(
          (coolnessProfiles || [])
            .filter((row) => row && typeof row === 'object' && String(row.profile_id || '') === pid)
            .map((row) => String(row.profile_version || ''))
        );

        let candidate = String(startVersion || '').trim();
        if (!candidate) {{
          let maxNumeric = 0;
          let hasNumeric = false;
          used.forEach((v) => {{
            if (!/^\\d+$/.test(v)) return;
            const n = Number.parseInt(v, 10);
            if (Number.isFinite(n)) {{
              hasNumeric = true;
              if (n > maxNumeric) maxNumeric = n;
            }}
          }});
          candidate = hasNumeric ? String(maxNumeric + 1) : '1';
        }}
        let guard = 0;
        while (used.has(candidate) && guard < 200) {{
          candidate = suggestNextProfileVersion(candidate);
          guard += 1;
        }}
        return candidate;
      }}

	      function resetCoolnessToActive() {{
	        const active = activeCoolnessPointer || {{}};
	        const activeProfile = activeCoolnessProfile || {{}};
	        coolProfileIdEl.value = String(active.profile_id || activeProfile.profile_id || 'default');
	        coolProfileVersionEl.value = String(active.profile_version || activeProfile.profile_version || '1');
	        setCoolnessWeights(activeProfile.weights || coolnessDefaultWeights, 'reset');
	        setPreviewNotice('Reset sliders and profile selectors to active profile values.');
	      }}

      function renderCoolnessSliders() {{
        coolnessSlidersEl.innerHTML = '';
        Object.keys(coolnessDefaultWeights).forEach((key) => {{
          const row = document.createElement('div');
          row.className = 'weight-row';

          const label = document.createElement('label');
          label.textContent = coolnessLabels[key] || key;
          label.htmlFor = `coolWeightRange-${{key}}`;
          row.appendChild(label);

          const range = document.createElement('input');
          range.type = 'range';
          range.min = '0';
          range.max = '1';
          range.step = '0.01';
          range.id = `coolWeightRange-${{key}}`;
          range.value = String(currentCoolnessWeights[key] ?? 0);
          row.appendChild(range);

          const number = document.createElement('input');
          number.type = 'number';
          number.min = '0';
          number.max = '1';
          number.step = '0.01';
          number.value = String(currentCoolnessWeights[key] ?? 0);
          row.appendChild(number);

	          const syncValue = (raw) => {{
	            const value = Number(raw);
	            if (!Number.isFinite(value) || value < 0) return;
	            const clamped = Math.min(1, Math.max(0, value));
	            currentCoolnessWeights[key] = clamped;
	            range.value = clamped.toFixed(2);
	            number.value = clamped.toFixed(2);
	            updateCoolnessJsonPreview();
              if (activePreset !== 'custom') setActivePreset('custom');
	          }};
          range.addEventListener('input', () => syncValue(range.value));
          number.addEventListener('change', () => syncValue(number.value));

          coolnessSlidersEl.appendChild(row);
        }});
        updateCoolnessJsonPreview();
      }}

	      async function loadCoolnessState(options = undefined) {{
	        const preserveEditor = !!(options && options.preserveEditor);
        const suppressNotice = !!(options && options.suppressNotice);
	        const {{ res, data }} = await fetchJson('/api/v1/admin/coolness/state', {{ credentials: 'include' }});
	        if (!res.ok) {{
	          coolPreviewOutEl.textContent = JSON.stringify(data, null, 2);
	          setPreviewNotice('Could not load active coolness profile.');
	          return;
        }}
	        const active = data.active || {{}};
	        const activeProfile = data.active_profile || {{}};
	        coolnessProfiles = Array.isArray(data.profiles) ? data.profiles : [];
	        activeCoolnessPointer = active;
	        activeCoolnessProfile = activeProfile;
        renderSavedProfilesOptions();
	        if (!preserveEditor) {{
	          coolProfileIdEl.value = String(active.profile_id || activeProfile.profile_id || 'default');
	          coolProfileVersionEl.value = String(active.profile_version || activeProfile.profile_version || '1');
	          const activeWeights = activeProfile.weights || coolnessDefaultWeights;
	          setCoolnessWeights(activeWeights, 'reset');
	        }}
		        if (!suppressNotice) setPreviewNotice(
		          `Active profile is ${{
		            String(active.profile_id || activeProfile.profile_id || 'default')
		          }}@${{
		            String(active.profile_version || activeProfile.profile_version || '1')
		          }}. Preview is read-only; Run updates rich outputs ephemerally; Save Profile persists versions; Activate Profile switches what is live.`
		        );
	      }}

	      async function previewCoolness(options = undefined) {{
	        setRunStatus('previewing', 'read-only simulation');
	        const suppressAlert = !!(options && options.suppressAlert);
	        const runningJob = findActiveCoolnessJob();
	        if (runningJob) {{
	          setRunStatus(String(runningJob.status || 'running'), `job ${{runningJob.job_id}}`);
	          setPreviewNotice(
	            `Scoring job ${{runningJob.job_id}} is ${{runningJob.status}}. Preview is temporarily unavailable until it finishes.`
	          );
	          return {{ ok: false, reason: 'job_running', data: {{ job: runningJob }} }};
	        }}
	        const topN = Math.max(20, Math.min(1000, Number.parseInt(String(coolTopNEl.value || '200'), 10) || 200));
	        coolTopNEl.value = String(topN);
	        const normalized = normalizeProfileFields();
	        const profileId = normalized.profileId;
	        const profileVersion = normalized.profileVersion;
	        const hasProfileRef = !!(profileId && profileVersion);
	        const payload = {{
	          profile_id: hasProfileRef ? profileId : null,
	          profile_version: hasProfileRef ? profileVersion : null,
	          weights: currentCoolnessWeights,
	          top_n: topN,
	        }};
        const {{ res, data }} = await fetchJson('/api/v1/admin/coolness/preview', {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken(),
          }},
          body: JSON.stringify(payload),
        }});
	        coolPreviewOutEl.textContent = JSON.stringify(data, null, 2);
	        renderCoolnessPreview(data);
	        if (!res.ok) {{
	          const err = (data && data.error) || {{}};
	          if (String(err.code || '') === 'conflict') {{
	            setRunStatus('busy', 'scoring output lock');
	            setPreviewNotice(
	              err.message || 'Preview is temporarily unavailable while scoring writes outputs. Retry in a few seconds.'
	            );
	            return {{ ok: false, reason: 'conflict', res, data }};
	          }}
		          const message = String(
		            (err && err.message) ||
		            (data && data.error && data.error.message) ||
		            'Preview failed'
		          );
		          setRunStatus('error', 'preview failed');
		          setPreviewNotice(`Preview failed: ${{message}}`);
	          if (!suppressAlert) {{
	            alert(`Preview failed: ${{(data && data.error && data.error.message) || res.status}}`);
	          }}
	          return {{ ok: false, reason: 'error', res, data }};
	        }} else {{
	          setRunStatus('ready', 'preview updated');
	          setPreviewNotice('Preview completed. No data was persisted.');
	          return {{ ok: true, reason: 'ok', res, data }};
	        }}
	      }}

      async function applyCoolness() {{
        setRunStatus('running', 'starting score job');
        const runResult = await runAction('score_coolness', {{
          weights_json: JSON.stringify(currentCoolnessWeights),
          ephemeral: true,
        }});
        if (!runResult || !runResult.ok) {{
          setRunStatus('error', 'score job failed to start');
          setPreviewNotice('Run failed. Check job logs in Activity and retry.');
          return;
        }}
	        const job = (runResult.data && runResult.data.job) || {{}};
	        const jobId = String(job.job_id || '');
	        if (!jobId) {{
	          setRunStatus('queued', 'job created');
	          setPreviewNotice('Scoring job queued. Watch Activity > Jobs for completion and run preview when complete.');
		        await loadCoolnessState({{ preserveEditor: true }});
	          return;
	        }}
	        setRunStatus('queued', `job ${{jobId}}`);
        setPreviewNotice(`Scoring job ${{jobId}} queued. Waiting for completion, then refreshing preview automatically.`);
        await loadJobs();
        void followCoolnessJobAndPreview(jobId);
      }}

	      async function saveCoolnessProfile() {{
        setRunStatus('saving', 'persisting immutable profile');
        if (!Array.isArray(coolnessProfiles) || !coolnessProfiles.length) {{
          await loadCoolnessState({{ preserveEditor: true }});
        }}
	        const fallbackProfileId = String((activeCoolnessPointer || {{}}).profile_id || (activeCoolnessProfile || {{}}).profile_id || 'default');
	        const normalized = normalizeProfileFields();
	        const profileId = normalized.profileId || fallbackProfileId;
	        const profileVersionRaw = normalized.profileVersion;
	        let profileVersion = profileVersionRaw;
	        coolProfileIdEl.value = profileId;
	        if (normalized.originalId && normalized.profileId && normalized.originalId !== normalized.profileId) {{
	          setPreviewNotice(`Normalized Profile ID for storage: '${{normalized.originalId}}' -> '${{normalized.profileId}}'.`);
	        }}
	        if (normalized.originalVersion && normalized.profileVersion && normalized.originalVersion !== normalized.profileVersion) {{
	          setPreviewNotice(`Normalized Profile version for storage: '${{normalized.originalVersion}}' -> '${{normalized.profileVersion}}'.`);
	        }}
	        if (!profileId) {{
	          setRunStatus('error', 'save failed');
	          setPreviewNotice('Save failed: profile ID is required.');
	          return;
	        }}
	        if (!profileVersion) {{
	          profileVersion = nextUnusedProfileVersion(profileId, '');
	          coolProfileVersionEl.value = profileVersion;
	          setPreviewNotice(`Auto-selected next profile version: ${{profileId}}@${{profileVersion}}.`);
	        }}

        const existingProfile = findCoolnessProfile(profileId, profileVersion);
        if (existingProfile && !weightsEqual(currentCoolnessWeights, existingProfile.weights || {{}})) {{
          const bumped = nextUnusedProfileVersion(profileId, suggestNextProfileVersion(profileVersion));
          profileVersion = bumped;
          coolProfileVersionEl.value = profileVersion;
          setPreviewNotice(
            `Auto-bumped profile version to ${{profileVersion}} because ${{profileId}}@${{String(existingProfile.profile_version || '')}} already exists with different immutable weights.`
          );
        }}

        const saveResult = await runAction('save_coolness_profile', {{
          profile_id: profileId,
          profile_version: profileVersion,
          weights_json: JSON.stringify(currentCoolnessWeights),
          notes: 'saved from admin coolness tuning',
        }});
        if (!saveResult || !saveResult.ok) {{
          setRunStatus('error', 'save failed');
          setPreviewNotice('Save failed. Pick a new profile version and retry.');
          return;
        }}
        const job = (saveResult.data && saveResult.data.job) || {{}};
        const jobId = String(job.job_id || '');
        if (!jobId) {{
          setRunStatus('queued', 'save job created');
          setPreviewNotice('Save job queued. Check Activity > Jobs for completion.');
          return;
        }}
        setRunStatus('queued', `save job ${{jobId}}`);
        setPreviewNotice(`Save job ${{jobId}} queued. Waiting for completion...`);
        await loadJobs();
	        await followSaveProfileJob(jobId, profileId, profileVersion);
	      }}

	      async function activateCoolnessProfile() {{
	        setRunStatus('activating', 'updating active profile');
	        if (!Array.isArray(coolnessProfiles) || !coolnessProfiles.length) {{
	          await loadCoolnessState({{ preserveEditor: true }});
	        }}
	        const normalized = normalizeProfileFields();
	        let profileId = normalized.profileId;
	        let profileVersion = normalized.profileVersion;
	        if (!profileId || !profileVersion) {{
	          const selected = splitProfileOptionValue(coolSavedProfilesEl ? coolSavedProfilesEl.value : '');
	          if (!profileId) profileId = selected[0];
	          if (!profileVersion) profileVersion = selected[1];
	        }}
	        if (!profileId || !profileVersion) {{
	          setRunStatus('error', 'activate failed');
	          setPreviewNotice('Activate failed: choose a saved profile (ID + version) first.');
	          return;
	        }}
	        const profile = findCoolnessProfile(profileId, profileVersion);
	        if (!profile) {{
	          setRunStatus('error', 'activate failed');
	          setPreviewNotice(`Activate failed: saved profile ${{profileId}}@${{profileVersion}} was not found.`);
	          return;
	        }}
	        coolProfileIdEl.value = profileId;
	        coolProfileVersionEl.value = profileVersion;
	        const result = await runAction('apply_coolness_profile', {{
	          profile_id: profileId,
	          profile_version: profileVersion,
	          reason: 'activated from admin coolness tuning',
	        }});
	        if (!result || !result.ok) {{
	          setRunStatus('error', 'activate failed');
	          setPreviewNotice('Activate failed. Check job logs in Activity and retry.');
	          return;
	        }}
	        const job = (result.data && result.data.job) || {{}};
	        const jobId = String(job.job_id || '');
	        if (!jobId) {{
	          setRunStatus('queued', 'activate job created');
	          setPreviewNotice('Activate job queued. Check Activity > Jobs for completion.');
	          return;
	        }}
	        setRunStatus('queued', `activate job ${{jobId}}`);
	        setPreviewNotice(`Activate job ${{jobId}} queued. Waiting for completion...`);
	        await loadJobs();
	        await followActivateProfileJob(jobId, profileId, profileVersion);
	      }}

	      async function callStatus() {{
        const {{ data }} = await fetchJson('/api/v1/admin/status', {{ credentials: 'include' }});
        out.textContent = JSON.stringify(data, null, 2);
      }}

      function parseFieldValue(type, input) {{
        if (type === 'boolean') {{
          return !!input.checked;
        }}
        if (type === 'integer') {{
          const value = (input.value || '').trim();
          if (!value) return null;
          return Number.parseInt(value, 10);
        }}
        return (input.value || '').trim();
      }}

      function createParamField(actionName, paramName, spec) {{
        const wrap = document.createElement('div');
        wrap.className = 'field';
        const label = document.createElement('label');
        label.htmlFor = `param-${{actionName}}-${{paramName}}`;
        label.textContent = spec.label || paramName;
        wrap.appendChild(label);
        let input;
        const typ = spec.type || 'string';
        if (typ === 'boolean') {{
          input = document.createElement('input');
          input.type = 'checkbox';
          input.checked = !!spec.default;
        }} else {{
          input = document.createElement('input');
          input.type = typ === 'integer' ? 'number' : 'text';
          if (spec.placeholder) input.placeholder = String(spec.placeholder);
          if (spec.default !== undefined && spec.default !== null && spec.default !== '') {{
            input.value = String(spec.default);
          }}
        }}
        input.id = `param-${{actionName}}-${{paramName}}`;
        input.dataset.paramName = paramName;
        input.dataset.paramType = typ;
        input.dataset.required = String(!!spec.required);
        wrap.appendChild(input);
        if (spec.required) {{
          const req = document.createElement('div');
          req.className = 'small';
          req.textContent = 'Required';
          wrap.appendChild(req);
        }}
        return wrap;
      }}

      async function runAction(actionName, params, confirmation) {{
        const payload = {{ action: actionName, params: params || {{}} }};
        if (confirmation) payload.confirmation = confirmation;
        const {{ res, data }} = await fetchJson('/api/v1/admin/actions/run', {{
          method: 'POST',
          credentials: 'include',
          headers: {{
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken(),
          }},
          body: JSON.stringify(payload),
        }});
        if (!res.ok) {{
          alert(`Action failed: ${{(data && data.error && data.error.message) || res.status}}`);
          return {{ ok: false, data }};
        }}
        await loadJobs();
        await loadAudit(false);
        return {{ ok: true, data }};
      }}

      async function loadCatalog() {{
        const {{ data }} = await fetchJson('/api/v1/admin/actions/catalog', {{ credentials: 'include' }});
        actionsOpsEl.innerHTML = '';
        actionsCoolnessEl.innerHTML = '';
        (data.items || []).forEach((item) => {{
          actionCatalog.set(item.name, item);
          const card = document.createElement('div');
          card.className = 'action-card';
          const title = document.createElement('h3');
          title.textContent = item.display_name || item.name;
          card.appendChild(title);
          const meta = document.createElement('div');
          meta.className = 'action-meta';
          const category = item.category || 'operations';
          meta.textContent = `category=${{category}} risk=${{item.risk_level || 'low'}} roles=${{(item.required_roles || []).join(',')}}`;
          card.appendChild(meta);
          const desc = document.createElement('div');
          desc.className = 'small';
          desc.textContent = item.description || '';
          card.appendChild(desc);

          const form = document.createElement('form');
          form.dataset.action = item.name;
          form.style.marginTop = '0.45rem';
          const schema = item.params_schema || {{}};
          Object.entries(schema).forEach(([paramName, spec]) => {{
            form.appendChild(createParamField(item.name, paramName, spec || {{}}));
          }});
          if (item.requires_confirmation) {{
            form.appendChild(createParamField(item.name, 'confirmation', {{
              type: 'string',
              required: true,
              allow_empty: false,
              placeholder: item.confirmation_phrase || '',
              label: 'Confirmation phrase',
            }}));
          }}
          const runBtn = document.createElement('button');
          runBtn.className = (item.risk_level === 'high') ? 'warn' : 'primary';
          runBtn.type = 'submit';
          runBtn.textContent = 'Run';
          form.appendChild(runBtn);

          form.onsubmit = async (e) => {{
            e.preventDefault();
            const params = {{}};
            let confirmation = '';
            const inputs = form.querySelectorAll('input[data-param-name]');
            for (const input of inputs) {{
              const paramName = input.dataset.paramName;
              const paramType = input.dataset.paramType || 'string';
              const required = input.dataset.required === 'true';
              const value = parseFieldValue(paramType, input);
              if (paramName === 'confirmation') {{
                confirmation = String(value || '').trim();
                if (required && !confirmation) {{
                  alert('Confirmation phrase is required.');
                  return;
                }}
                continue;
              }}
              if (required && (value === null || value === '')) {{
                alert(`Missing required parameter: ${{paramName}}`);
                return;
              }}
              if (value === null || value === '') continue;
              params[paramName] = value;
            }}
            await runAction(item.name, params, confirmation);
          }};

          card.appendChild(form);
          if ((item.category || 'operations') === 'coolness') {{
            actionsCoolnessEl.appendChild(card);
          }} else {{
            actionsOpsEl.appendChild(card);
          }}
        }});
      }}

      async function cancelJob(jobId) {{
        const {{ res, data }} = await fetchJson(`/api/v1/admin/actions/jobs/${{jobId}}/cancel`, {{
          method: 'POST',
          credentials: 'include',
          headers: {{ 'X-CSRF-Token': csrfToken() }},
        }});
        if (!res.ok) {{
          alert(`Cancel failed: ${{(data && data.error && data.error.message) || res.status}}`);
          return;
        }}
        await loadJobs();
        await loadAudit(false);
      }}

	      async function loadJobs() {{
	        const {{ data }} = await fetchJson('/api/v1/admin/actions/jobs?limit=50', {{ credentials: 'include' }});
	        latestJobs = Array.isArray(data.items) ? data.items : [];
	        jobsEl.innerHTML = '';
	        latestJobs.forEach((job) => {{
          const li = document.createElement('li');
          const top = document.createElement('div');
          top.className = 'inline';
          const badge = document.createElement('span');
          badge.className = `status-badge ${{statusClass(job.status)}}`;
          badge.textContent = job.status;
          top.appendChild(badge);
          const title = document.createElement('strong');
          title.textContent = `${{job.action}} (${{job.job_id}})`;
          top.appendChild(title);
          li.appendChild(top);

          const controls = document.createElement('div');
          controls.className = 'inline';
          const viewBtn = document.createElement('button');
          viewBtn.textContent = 'View Log';
          viewBtn.onclick = async (e) => {{
            e.preventDefault();
            currentJobId = job.job_id;
            currentOffset = 0;
            logEl.textContent = '';
            selectedJobEl.textContent = `${{job.action}} (${{job.job_id}})`;
            await pollLog();
          }};
          controls.appendChild(viewBtn);

          const download = document.createElement('a');
          download.href = `/api/v1/admin/actions/jobs/${{job.job_id}}/log/download`;
          download.textContent = 'Download Log';
          controls.appendChild(download);

          if (job.status === 'queued') {{
            const cancelBtn = document.createElement('button');
            cancelBtn.className = 'danger';
            cancelBtn.textContent = 'Cancel';
            cancelBtn.onclick = async (e) => {{
              e.preventDefault();
              await cancelJob(job.job_id);
            }};
            controls.appendChild(cancelBtn);
          }}

          li.appendChild(controls);
          jobsEl.appendChild(li);
        }});
      }}

      function fillActionParam(actionName, paramName, value) {{
        const el = document.getElementById(`param-${{actionName}}-${{paramName}}`);
        if (!el) return;
        if (el.type === 'checkbox') {{
          el.checked = !!value;
        }} else {{
          el.value = String(value || '');
        }}
      }}

      async function loadBackups() {{
        const {{ data }} = await fetchJson('/api/v1/admin/backups?limit=100', {{ credentials: 'include' }});
        backupsAdminDbEl.innerHTML = '';
        backupsReleaseMetaEl.innerHTML = '';

        (data.admin_db || []).forEach((item) => {{
          const li = document.createElement('li');
          const useBtn = document.createElement('button');
          useBtn.textContent = 'Use';
          useBtn.onclick = () => fillActionParam('restore_admin_db', 'backup_name', item.name);
          li.appendChild(useBtn);
          const label = document.createElement('span');
          label.textContent = ` ${{item.name}} (${{item.bytes}} bytes)`;
          li.appendChild(label);
          backupsAdminDbEl.appendChild(li);
        }});

        (data.release_metadata || []).forEach((item) => {{
          const li = document.createElement('li');
          const useBtn = document.createElement('button');
          useBtn.textContent = 'Use';
          useBtn.onclick = () => fillActionParam('restore_release_metadata', 'backup_id', item.backup_id);
          li.appendChild(useBtn);
          const label = document.createElement('span');
          label.textContent = ` ${{item.backup_id}}`;
          li.appendChild(label);
          backupsReleaseMetaEl.appendChild(li);
        }});
      }}

      function buildAuditQuery(loadOlder) {{
        const params = new URLSearchParams();
        params.set('limit', '50');
        if (loadOlder && nextAuditBeforeId) {{
          params.set('before_audit_id', String(nextAuditBeforeId));
        }}
        if (auditPreset === 'auth') {{
          params.set('event_prefix', 'auth.');
        }} else if (auditPreset === 'actions') {{
          params.set('event_prefix', 'admin.action.');
        }}
        const eventType = (auditEventTypeEl.value || '').trim();
        const result = (auditResultEl.value || '').trim();
        const requestId = (auditRequestIdEl.value || '').trim();
        if (eventType) params.set('event_type', eventType);
        if (result) params.set('result', result);
        if (requestId) params.set('request_id', requestId);
        return params.toString();
      }}

      function renderAuditItems(items, append) {{
        if (!append) {{
          auditEl.innerHTML = '';
          auditDetailsEl.textContent = '';
        }}
        (items || []).forEach((entry) => {{
          const li = document.createElement('li');
          const a = document.createElement('a');
          a.href = '#';
          a.textContent = `#${{entry.audit_id}} [${{entry.result}}] ${{entry.event_type}} ${{entry.request_id || ''}} ${{entry.correlation_id || ''}}`;
          a.onclick = (e) => {{
            e.preventDefault();
            auditDetailsEl.textContent = JSON.stringify(entry, null, 2);
          }};
          li.appendChild(a);
          const meta = document.createElement('span');
          meta.textContent = ` (${{entry.created_at}} ${{entry.method || ''}} ${{entry.route || ''}})`;
          li.appendChild(meta);
          auditEl.appendChild(li);
        }});
      }}

      async function loadAudit(loadOlder = false) {{
        const query = buildAuditQuery(loadOlder);
        const {{ res, data }} = await fetchJson(`/api/v1/admin/audit?${{query}}`, {{ credentials: 'include' }});
        if (!res.ok) {{
          auditDetailsEl.textContent = JSON.stringify(data, null, 2);
          return;
        }}
        const items = data.items || [];
        renderAuditItems(items, loadOlder);
        nextAuditBeforeId = data.next_before_audit_id || null;
      }}

      async function pollLog() {{
        if (!currentJobId) return;
        const {{ data }} = await fetchJson(`/api/v1/admin/actions/jobs/${{currentJobId}}/log?offset=${{currentOffset}}&limit=65536`, {{ credentials: 'include' }});
        const chunk = data.chunk || '';
        if (chunk) {{
          logEl.textContent += chunk;
          currentOffset = data.next_offset || currentOffset;
          logEl.scrollTop = logEl.scrollHeight;
        }}
        if (!data.eof) {{
          setTimeout(pollLog, 1000);
        }}
      }}

      async function doLogout() {{
        await fetch('/api/v1/auth/logout', {{
          method: 'POST',
          credentials: 'include',
          headers: {{ 'X-CSRF-Token': csrfToken() }},
        }});
        window.location.href = '/api/v1/admin/ui';
      }}

      document.getElementById('logout').addEventListener('click', doLogout);
      document.getElementById('screenTabOperations').addEventListener('click', () => setScreen('operations'));
      document.getElementById('screenTabCoolness').addEventListener('click', () => setScreen('coolness'));
      document.getElementById('screenTabActivity').addEventListener('click', () => setScreen('activity'));
      document.getElementById('refreshStatus').addEventListener('click', callStatus);
      document.getElementById('refreshJobs').addEventListener('click', loadJobs);
      document.getElementById('refreshBackups').addEventListener('click', loadBackups);
      document.getElementById('refreshAudit').addEventListener('click', () => loadAudit(false));
      document.getElementById('loadOlderAudit').addEventListener('click', () => loadAudit(true));
      document.getElementById('auditPresetAll').addEventListener('click', () => {{ setAuditPreset('all'); loadAudit(false); }});
      document.getElementById('auditPresetAuth').addEventListener('click', () => {{ setAuditPreset('auth'); loadAudit(false); }});
      document.getElementById('auditPresetActions').addEventListener('click', () => {{ setAuditPreset('actions'); loadAudit(false); }});
      document.getElementById('presetBalanced').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.balanced, 'balanced'));
      document.getElementById('presetExotic').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.exotic, 'exotic'));
      document.getElementById('presetHabitable').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.habitable, 'habitable'));
      document.getElementById('presetNearby').addEventListener('click', () => setCoolnessWeights(coolnessPresetWeights.nearby, 'nearby'));
      coolResetActiveEl.addEventListener('click', resetCoolnessToActive);
	      coolApplyBtnEl.addEventListener('click', () => {{ void applyCoolness(); }});
	      coolSaveBtnEl.addEventListener('click', () => {{ void saveCoolnessProfile(); }});
	      coolActivateBtnEl.addEventListener('click', () => {{ void activateCoolnessProfile(); }});
	      coolLoadProfileBtnEl.addEventListener('click', loadSelectedSavedProfile);
      setScreen('operations');
      renderCoolnessSliders();
      renderCoolnessPreview(null);
	      setPreviewNotice('Preview is safe and read-only. Run updates ranking outputs ephemerally; Save Profile persists a chosen version; Activate Profile switches what is live.');
      setRunStatus('idle', 'ready');
      callStatus();
      loadCatalog();
      loadCoolnessState();
      loadJobs();
      loadBackups();
      loadAudit();
      setInterval(loadJobs, 5000);
    </script>
  </body>
</html>
    """
    return HTMLResponse(body, status_code=200)
