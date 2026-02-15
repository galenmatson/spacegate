from __future__ import annotations

import datetime
import os
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import db
from .db import DatabaseUnavailable
from .queries import (
    fetch_build_id,
    fetch_counts_for_system,
    fetch_planets_for_system,
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


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
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
    spectral_class: Optional[str] = Query(default=None),
    has_planets: Optional[str] = Query(default=None),
    sort: str = Query(default="name"),
    limit: int = Query(default=50, ge=1, le=200),
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

    q_norm = normalize_query_text(q or "")
    id_query = parse_identifier_query(q_norm)
    system_id_exact: Optional[int] = None
    if q and q.strip().isdigit():
        try:
            system_id_exact = int(q.strip())
        except ValueError:
            system_id_exact = None

    sort_key = sort.lower() if sort else "name"
    if sort_key not in {"name", "distance"}:
        sort_key = "name"

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
    has_planets_bool = parse_bool(has_planets)

    with db.connection_scope() as con:
        rows = search_systems(
            con,
            q_norm=q_norm or None,
            q_raw=q,
            system_id_exact=system_id_exact,
            id_query=id_query,
            max_dist_ly=max_dist_ly,
            min_dist_ly=min_dist_ly,
            spectral_classes=spectral_classes,
            has_planets=has_planets_bool,
            sort=sort_key,
            match_mode=match_mode,
            limit=limit + 1,
            cursor_values=cursor_values,
        )

    has_more = len(rows) > limit
    items = rows[:limit]
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
        else:
            next_cursor = encode_cursor(
                {
                    "sort": "name",
                    "name": last.get("system_name_norm") or "",
                    "id": last.get("system_id"),
                }
            )

    return {"items": items, "next_cursor": next_cursor, "has_more": has_more}


@app.get("/api/v1/systems/{system_id}")
def system_detail(system_id: int):
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

    system["star_count"] = star_count
    system["planet_count"] = planet_count
    return {"system": system, "stars": stars, "planets": planets}


@app.get("/api/v1/systems/by-key/{stable_object_key}")
def system_detail_by_key(stable_object_key: str):
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

    system["star_count"] = star_count
    system["planet_count"] = planet_count
    return {"system": system, "stars": stars, "planets": planets}
