from __future__ import annotations

import datetime
import json
import os
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
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


admin_router = APIRouter(prefix="/api/v1/admin")


class ActionRunRequest(BaseModel):
    action: str = Field(min_length=1, max_length=64)
    params: Dict[str, Any] = Field(default_factory=dict)
    confirmation: Optional[str] = Field(default=None, max_length=256)


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
        <h2>Coolness</h2>
        <p class="muted">Use profile/version and optional JSON weight overrides for tuning runs.</p>
        <div id="actionsCoolness" class="grid"></div>
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
      const csrfCookieName = '{csrf_cookie_name}';
      const actionCatalog = new Map();
      let currentJobId = null;
      let currentOffset = 0;
      let nextAuditBeforeId = null;
      let auditPreset = 'all';
      let currentScreen = 'operations';

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

      function statusClass(status) {{
        const key = String(status || '').toLowerCase();
        if (key === 'running') return 'status-running';
        if (key === 'queued') return 'status-queued';
        if (key === 'succeeded') return 'status-succeeded';
        if (key === 'failed') return 'status-failed';
        if (key === 'cancelled') return 'status-cancelled';
        return '';
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
          return;
        }}
        await loadJobs();
        await loadAudit(false);
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
        jobsEl.innerHTML = '';
        (data.items || []).forEach((job) => {{
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
      setScreen('operations');
      callStatus();
      loadCatalog();
      loadJobs();
      loadBackups();
      loadAudit();
      setInterval(loadJobs, 5000);
    </script>
  </body>
</html>
    """
    return HTMLResponse(body, status_code=200)
