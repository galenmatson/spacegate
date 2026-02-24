from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse, Response
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token

from . import admin_db


DEFAULT_AUTH_ISSUER = "https://accounts.google.com"
DEFAULT_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
DEFAULT_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
STATE_COOKIE_MAX_AGE_SECONDS = 600


@dataclass(frozen=True)
class AuthConfig:
    enabled: bool
    provider: str
    issuer: str
    client_id: str
    client_secret: str
    redirect_uri: str
    success_redirect: str
    session_secret: str
    session_cookie_name: str
    csrf_cookie_name: str
    state_cookie_name: str
    cookie_secure: bool
    cookie_samesite: str
    cookie_domain: str | None
    session_ttl_hours: int
    session_idle_minutes: int
    csrf_enable: bool
    bind_user_agent: bool
    bind_ip_prefix: bool
    auth_url: str
    token_url: str


def _parse_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _get_cookie_samesite() -> str:
    raw = os.getenv("SPACEGATE_SESSION_COOKIE_SAMESITE", "lax").strip().lower()
    if raw not in {"lax", "strict", "none"}:
        return "lax"
    return raw


def _safe_local_path(path: str | None, fallback: str) -> str:
    if not path:
        return fallback
    value = path.strip()
    if not value.startswith("/"):
        return fallback
    if value.startswith("//"):
        return fallback
    return value


def _normalize_cookie_name(name: str, *, cookie_secure: bool, fallback: str) -> str:
    value = (name or "").strip() or fallback
    if not cookie_secure and value.startswith("__Host-"):
        value = value[len("__Host-") :].strip() or fallback
    return value


def _load_config() -> AuthConfig:
    enabled = admin_db.auth_enabled()
    provider = os.getenv("SPACEGATE_OIDC_PROVIDER", "google").strip().lower() or "google"
    issuer = os.getenv("SPACEGATE_OIDC_ISSUER", DEFAULT_AUTH_ISSUER).strip() or DEFAULT_AUTH_ISSUER
    client_id = os.getenv("SPACEGATE_OIDC_CLIENT_ID", "").strip()
    client_secret = os.getenv("SPACEGATE_OIDC_CLIENT_SECRET", "").strip()
    redirect_uri = os.getenv("SPACEGATE_OIDC_REDIRECT_URI", "").strip()
    success_redirect = _safe_local_path(
        os.getenv("SPACEGATE_AUTH_SUCCESS_REDIRECT", "/api/v1/admin/ui"),
        "/api/v1/admin/ui",
    )
    cookie_secure = admin_db.parse_env_bool(os.getenv("SPACEGATE_SESSION_COOKIE_SECURE"), default=True)
    session_secret = os.getenv("SPACEGATE_SESSION_SECRET", "").strip()
    session_cookie_default = "__Host-spacegate_session" if cookie_secure else "spacegate_session"
    csrf_cookie_default = "__Host-spacegate_csrf" if cookie_secure else "spacegate_csrf"
    state_cookie_default = "__Host-spacegate_oidc_state" if cookie_secure else "spacegate_oidc_state"
    session_cookie_name = _normalize_cookie_name(
        os.getenv("SPACEGATE_SESSION_COOKIE_NAME", session_cookie_default),
        cookie_secure=cookie_secure,
        fallback=session_cookie_default,
    )
    csrf_cookie_name = _normalize_cookie_name(
        os.getenv("SPACEGATE_CSRF_COOKIE_NAME", csrf_cookie_default),
        cookie_secure=cookie_secure,
        fallback=csrf_cookie_default,
    )
    state_cookie_name = _normalize_cookie_name(
        os.getenv("SPACEGATE_OIDC_STATE_COOKIE_NAME", state_cookie_default),
        cookie_secure=cookie_secure,
        fallback=state_cookie_default,
    )
    cookie_domain = os.getenv("SPACEGATE_SESSION_COOKIE_DOMAIN", "").strip() or None
    session_ttl_hours = _parse_env_int("SPACEGATE_SESSION_TTL_HOURS", 12)
    session_idle_minutes = _parse_env_int("SPACEGATE_SESSION_IDLE_MINUTES", 60)
    csrf_enable = admin_db.parse_env_bool(os.getenv("SPACEGATE_CSRF_ENABLE"), default=True)
    bind_user_agent = admin_db.parse_env_bool(os.getenv("SPACEGATE_SESSION_BIND_USER_AGENT"), default=False)
    bind_ip_prefix = admin_db.parse_env_bool(os.getenv("SPACEGATE_SESSION_BIND_IP_PREFIX"), default=False)
    auth_url = os.getenv("SPACEGATE_OIDC_AUTH_URL", DEFAULT_GOOGLE_AUTH_URL).strip() or DEFAULT_GOOGLE_AUTH_URL
    token_url = os.getenv("SPACEGATE_OIDC_TOKEN_URL", DEFAULT_GOOGLE_TOKEN_URL).strip() or DEFAULT_GOOGLE_TOKEN_URL
    return AuthConfig(
        enabled=enabled,
        provider=provider,
        issuer=issuer,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        success_redirect=success_redirect,
        session_secret=session_secret,
        session_cookie_name=session_cookie_name,
        csrf_cookie_name=csrf_cookie_name,
        state_cookie_name=state_cookie_name,
        cookie_secure=cookie_secure,
        cookie_samesite=_get_cookie_samesite(),
        cookie_domain=cookie_domain,
        session_ttl_hours=session_ttl_hours,
        session_idle_minutes=session_idle_minutes,
        csrf_enable=csrf_enable,
        bind_user_agent=bind_user_agent,
        bind_ip_prefix=bind_ip_prefix,
        auth_url=auth_url,
        token_url=token_url,
    )


def get_config() -> AuthConfig:
    return _load_config()


def is_enabled() -> bool:
    return get_config().enabled


def initialize() -> None:
    cfg = get_config()
    if not cfg.enabled:
        return
    if cfg.provider != "google":
        raise RuntimeError(f"Unsupported OIDC provider: {cfg.provider}")
    required = {
        "SPACEGATE_OIDC_CLIENT_ID": cfg.client_id,
        "SPACEGATE_OIDC_CLIENT_SECRET": cfg.client_secret,
        "SPACEGATE_OIDC_REDIRECT_URI": cfg.redirect_uri,
        "SPACEGATE_SESSION_SECRET": cfg.session_secret,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Auth enabled but missing required env vars: {', '.join(missing)}")
    admin_db.initialize()


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_iso(value: dt.datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> dt.datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sign_payload(payload_b64: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256).hexdigest()


def _build_signed_state_cookie(next_path: str) -> str:
    cfg = get_config()
    payload = {
        "state": secrets.token_urlsafe(24),
        "nonce": secrets.token_urlsafe(24),
        "next": _safe_local_path(next_path, cfg.success_redirect),
        "iat": int(_utc_now().timestamp()),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = urllib.parse.quote_from_bytes(raw, safe="")
    sig = _sign_payload(payload_b64, cfg.session_secret)
    return f"{payload_b64}.{sig}"


def _parse_signed_state_cookie(cookie_value: str) -> Dict[str, Any]:
    cfg = get_config()
    if "." not in cookie_value:
        raise ValueError("invalid_state_cookie")
    payload_b64, sig = cookie_value.rsplit(".", 1)
    expected = _sign_payload(payload_b64, cfg.session_secret)
    if not hmac.compare_digest(sig, expected):
        raise ValueError("invalid_state_signature")
    raw = urllib.parse.unquote_to_bytes(payload_b64)
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid_state_payload")
    return payload


def _exchange_code_for_token(code: str) -> Dict[str, Any]:
    cfg = get_config()
    data = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
            "redirect_uri": cfg.redirect_uri,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        cfg.token_url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid_token_response")
    return payload


def _verify_google_id_token(raw_id_token: str, expected_nonce: str) -> Dict[str, Any]:
    cfg = get_config()
    request_adapter = google_requests.Request()
    claims = google_id_token.verify_oauth2_token(
        raw_id_token,
        request_adapter,
        cfg.client_id,
    )
    issuer = str(claims.get("iss", ""))
    accepted_issuers = {cfg.issuer, cfg.issuer.removeprefix("https://")}
    if issuer not in accepted_issuers:
        raise ValueError("invalid_issuer")
    nonce = str(claims.get("nonce", ""))
    if nonce != expected_nonce:
        raise ValueError("invalid_nonce")
    if not claims.get("email_verified"):
        raise ValueError("email_not_verified")
    return claims


def _extract_ip_prefix(host: str) -> str:
    host = (host or "").strip()
    if "." in host:
        parts = host.split(".")
        if len(parts) >= 3:
            return ".".join(parts[:3])
    if ":" in host:
        parts = host.split(":")
        if len(parts) >= 4:
            return ":".join(parts[:4])
    return host


def _hash_user_agent(request: Request) -> str:
    ua = request.headers.get("user-agent", "").strip()
    return _sha256_hex(ua)


def _hash_ip_prefix(request: Request) -> str:
    host = request.client.host if request.client else ""
    prefix = _extract_ip_prefix(host)
    return _sha256_hex(prefix)


def _allowlist_match(
    con: sqlite3.Connection,
    *,
    provider: str,
    issuer: str,
    provider_sub: str,
    email_norm: str,
) -> bool:
    row = con.execute(
        """
SELECT 1
FROM admin_allowlist
WHERE enabled = 1
  AND (provider IS NULL OR provider = ?)
  AND (issuer IS NULL OR issuer = ?)
  AND (
    (provider_sub IS NOT NULL AND provider_sub = ?)
    OR (email_norm IS NOT NULL AND email_norm = ?)
  )
LIMIT 1
        """,
        (provider, issuer, provider_sub, email_norm),
    ).fetchone()
    return row is not None


def _ensure_admin_user(
    con: sqlite3.Connection,
    *,
    provider: str,
    issuer: str,
    provider_sub: str,
    email_norm: str,
    display_name: str,
    email_verified: bool,
    claims: Dict[str, Any],
) -> Dict[str, Any]:
    now = _to_iso(_utc_now())

    row = con.execute(
        """
SELECT u.user_id, u.status
FROM auth_identities ai
JOIN users u ON u.user_id = ai.user_id
WHERE ai.provider = ? AND ai.issuer = ? AND ai.provider_sub = ?
        """,
        (provider, issuer, provider_sub),
    ).fetchone()

    if row is not None:
        user_id = int(row["user_id"])
        status = str(row["status"])
        con.execute(
            """
UPDATE users
SET email_norm = ?, display_name = ?, updated_at = ?, last_login_at = ?
WHERE user_id = ?
            """,
            (email_norm, display_name, now, now, user_id),
        )
    else:
        existing = con.execute(
            "SELECT user_id, status FROM users WHERE email_norm = ?",
            (email_norm,),
        ).fetchone()
        if existing is not None:
            user_id = int(existing["user_id"])
            status = str(existing["status"])
            con.execute(
                """
UPDATE users
SET display_name = ?, updated_at = ?, last_login_at = ?
WHERE user_id = ?
                """,
                (display_name, now, now, user_id),
            )
        else:
            cur = con.execute(
                """
INSERT INTO users(email_norm, display_name, status, created_at, updated_at, last_login_at)
VALUES (?, ?, 'active', ?, ?, ?)
                """,
                (email_norm, display_name, now, now, now),
            )
            user_id = int(cur.lastrowid)
            status = "active"

    con.execute(
        """
INSERT INTO auth_identities(
  user_id, provider, issuer, provider_sub, email_at_login, email_verified, claims_json, created_at, last_login_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(provider, issuer, provider_sub) DO UPDATE SET
  user_id = excluded.user_id,
  email_at_login = excluded.email_at_login,
  email_verified = excluded.email_verified,
  claims_json = excluded.claims_json,
  last_login_at = excluded.last_login_at
        """,
        (
            user_id,
            provider,
            issuer,
            provider_sub,
            email_norm,
            1 if email_verified else 0,
            json.dumps(claims, separators=(",", ":"), sort_keys=True),
            now,
            now,
        ),
    )

    role_row = con.execute("SELECT role_id FROM roles WHERE role_code = 'admin'").fetchone()
    if role_row is None:
        raise RuntimeError("missing admin role seed")
    con.execute(
        "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES (?, ?)",
        (user_id, int(role_row["role_id"])),
    )

    role_rows = con.execute(
        """
SELECT r.role_code
FROM user_roles ur
JOIN roles r ON r.role_id = ur.role_id
WHERE ur.user_id = ?
        """,
        (user_id,),
    ).fetchall()
    roles = sorted({str(item["role_code"]) for item in role_rows})
    return {"user_id": user_id, "status": status, "roles": roles}


def _create_session(con: sqlite3.Connection, request: Request, user_id: int) -> Dict[str, str]:
    cfg = get_config()
    now = _utc_now()
    now_iso = _to_iso(now)
    expires_at = _to_iso(now + dt.timedelta(hours=cfg.session_ttl_hours))
    idle_expires_at = _to_iso(now + dt.timedelta(minutes=cfg.session_idle_minutes))
    session_id = secrets.token_urlsafe(48)
    csrf_secret = secrets.token_urlsafe(32)
    user_agent_hash = _hash_user_agent(request) if cfg.bind_user_agent else None
    ip_prefix_hash = _hash_ip_prefix(request) if cfg.bind_ip_prefix else None

    con.execute(
        """
INSERT INTO sessions(
  session_id, user_id, created_at, last_seen_at, expires_at, idle_expires_at,
  revoked_at, csrf_secret_hash, user_agent_hash, ip_prefix_hash
) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
        """,
        (
            session_id,
            user_id,
            now_iso,
            now_iso,
            expires_at,
            idle_expires_at,
            _sha256_hex(csrf_secret),
            user_agent_hash,
            ip_prefix_hash,
        ),
    )
    return {"session_id": session_id, "csrf_secret": csrf_secret, "expires_at": expires_at}


def _audit(
    request: Request,
    *,
    event_type: str,
    result: str,
    actor_user_id: int | None,
    details: Dict[str, Any] | None = None,
) -> None:
    if not is_enabled():
        return
    details = details or {}
    try:
        with admin_db.connection_scope() as con:
            con.execute(
                """
INSERT INTO audit_log(actor_user_id, event_type, result, request_id, route, method, details_json, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    actor_user_id,
                    event_type,
                    result,
                    getattr(request.state, "request_id", None),
                    str(request.url.path),
                    request.method,
                    json.dumps(details, separators=(",", ":"), sort_keys=True),
                    _to_iso(_utc_now()),
                ),
            )
            con.commit()
    except Exception:
        return


def audit_event(
    request: Request,
    *,
    event_type: str,
    result: str,
    actor_user_id: int | None,
    details: Dict[str, Any] | None = None,
) -> None:
    _audit(
        request,
        event_type=event_type,
        result=result,
        actor_user_id=actor_user_id,
        details=details,
    )


def login_redirect(request: Request, next_path: str | None = None) -> RedirectResponse:
    cfg = get_config()
    if not cfg.enabled:
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "Auth is disabled", "details": {}})
    next_path = _safe_local_path(next_path, cfg.success_redirect)
    state_cookie = _build_signed_state_cookie(next_path=next_path)
    state_payload = _parse_signed_state_cookie(state_cookie)

    query = urllib.parse.urlencode(
        {
            "client_id": cfg.client_id,
            "redirect_uri": cfg.redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state_payload["state"],
            "nonce": state_payload["nonce"],
            "access_type": "offline",
            "prompt": "select_account",
        }
    )
    response = RedirectResponse(url=f"{cfg.auth_url}?{query}", status_code=302)
    response.set_cookie(
        key=cfg.state_cookie_name,
        value=state_cookie,
        max_age=STATE_COOKIE_MAX_AGE_SECONDS,
        secure=cfg.cookie_secure,
        httponly=True,
        samesite=cfg.cookie_samesite,
        path="/",
        domain=cfg.cookie_domain,
    )
    _audit(request, event_type="auth.login.start", result="success", actor_user_id=None, details={"provider": cfg.provider})
    return response


def auth_callback(request: Request, code: str, state: str) -> RedirectResponse:
    cfg = get_config()
    if not cfg.enabled:
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "Auth is disabled", "details": {}})

    cookie_val = request.cookies.get(cfg.state_cookie_name)
    if not cookie_val:
        _audit(request, event_type="auth.login.denied", result="deny", actor_user_id=None, details={"reason": "missing_state_cookie"})
        raise HTTPException(status_code=400, detail={"code": "invalid_state", "message": "Missing auth state", "details": {}})

    try:
        payload = _parse_signed_state_cookie(cookie_val)
    except Exception:
        _audit(request, event_type="auth.login.denied", result="deny", actor_user_id=None, details={"reason": "invalid_state_cookie"})
        raise HTTPException(status_code=400, detail={"code": "invalid_state", "message": "Invalid auth state", "details": {}})

    if state != payload.get("state"):
        _audit(request, event_type="auth.login.denied", result="deny", actor_user_id=None, details={"reason": "state_mismatch"})
        raise HTTPException(status_code=400, detail={"code": "invalid_state", "message": "Auth state mismatch", "details": {}})

    issued_at = int(payload.get("iat", 0))
    if issued_at <= 0 or int(_utc_now().timestamp()) - issued_at > STATE_COOKIE_MAX_AGE_SECONDS:
        _audit(request, event_type="auth.login.denied", result="deny", actor_user_id=None, details={"reason": "state_expired"})
        raise HTTPException(status_code=400, detail={"code": "invalid_state", "message": "Auth state expired", "details": {}})

    try:
        token_payload = _exchange_code_for_token(code)
        raw_id_token = str(token_payload.get("id_token", ""))
        if not raw_id_token:
            raise ValueError("missing_id_token")
        claims = _verify_google_id_token(raw_id_token, expected_nonce=str(payload.get("nonce", "")))
    except Exception as exc:
        _audit(request, event_type="auth.login.error", result="error", actor_user_id=None, details={"reason": str(exc)})
        raise HTTPException(status_code=401, detail={"code": "auth_failed", "message": "Authentication failed", "details": {}})

    provider_sub = str(claims.get("sub", ""))
    email_norm = str(claims.get("email", "")).strip().lower()
    display_name = str(claims.get("name") or email_norm)
    email_verified = bool(claims.get("email_verified"))

    if not provider_sub or not email_norm:
        _audit(request, event_type="auth.login.denied", result="deny", actor_user_id=None, details={"reason": "missing_identity_claims"})
        raise HTTPException(status_code=401, detail={"code": "auth_failed", "message": "Missing identity claims", "details": {}})

    with admin_db.connection_scope() as con:
        if not _allowlist_match(
            con,
            provider=cfg.provider,
            issuer=cfg.issuer,
            provider_sub=provider_sub,
            email_norm=email_norm,
        ):
            con.commit()
            _audit(
                request,
                event_type="auth.login.denied",
                result="deny",
                actor_user_id=None,
                details={"reason": "allowlist", "email": email_norm},
            )
            raise HTTPException(status_code=403, detail={"code": "forbidden", "message": "Account not allowlisted", "details": {}})

        user = _ensure_admin_user(
            con,
            provider=cfg.provider,
            issuer=cfg.issuer,
            provider_sub=provider_sub,
            email_norm=email_norm,
            display_name=display_name,
            email_verified=email_verified,
            claims=claims,
        )
        if user["status"] != "active":
            con.commit()
            _audit(
                request,
                event_type="auth.login.denied",
                result="deny",
                actor_user_id=int(user["user_id"]),
                details={"reason": "user_disabled"},
            )
            raise HTTPException(status_code=403, detail={"code": "forbidden", "message": "User is disabled", "details": {}})

        session = _create_session(con, request, int(user["user_id"]))
        con.commit()

    response = RedirectResponse(url=str(payload.get("next") or cfg.success_redirect), status_code=302)
    _set_session_cookie(response, session["session_id"])
    _set_csrf_cookie(response, session["csrf_secret"])
    response.delete_cookie(
        key=cfg.state_cookie_name,
        path="/",
        domain=cfg.cookie_domain,
    )
    _audit(
        request,
        event_type="auth.login.success",
        result="success",
        actor_user_id=int(user["user_id"]),
        details={"email": email_norm},
    )
    return response


def _set_session_cookie(response: Response, session_id: str) -> None:
    cfg = get_config()
    response.set_cookie(
        key=cfg.session_cookie_name,
        value=session_id,
        max_age=cfg.session_ttl_hours * 3600,
        secure=cfg.cookie_secure,
        httponly=True,
        samesite=cfg.cookie_samesite,
        path="/",
        domain=cfg.cookie_domain,
    )


def _set_csrf_cookie(response: Response, csrf_secret: str) -> None:
    cfg = get_config()
    response.set_cookie(
        key=cfg.csrf_cookie_name,
        value=csrf_secret,
        max_age=cfg.session_ttl_hours * 3600,
        secure=cfg.cookie_secure,
        httponly=False,
        samesite=cfg.cookie_samesite,
        path="/",
        domain=cfg.cookie_domain,
    )


def clear_auth_cookies(response: Response) -> None:
    cfg = get_config()
    response.delete_cookie(key=cfg.session_cookie_name, path="/", domain=cfg.cookie_domain)
    response.delete_cookie(key=cfg.csrf_cookie_name, path="/", domain=cfg.cookie_domain)


def _fetch_session_context(request: Request, session_id: str) -> Optional[Dict[str, Any]]:
    cfg = get_config()
    now = _utc_now()
    with admin_db.connection_scope() as con:
        row = con.execute(
            """
SELECT
  s.session_id,
  s.user_id,
  s.expires_at,
  s.idle_expires_at,
  s.revoked_at,
  s.csrf_secret_hash,
  s.user_agent_hash,
  s.ip_prefix_hash,
  u.email_norm,
  u.display_name,
  u.status
FROM sessions s
JOIN users u ON u.user_id = s.user_id
WHERE s.session_id = ?
            """,
            (session_id,),
        ).fetchone()
        if row is None:
            return None
        if row["revoked_at"] is not None:
            return None
        if str(row["status"]) != "active":
            return None
        expires_at = _parse_iso(str(row["expires_at"]))
        idle_expires_at = _parse_iso(str(row["idle_expires_at"]))
        if now >= expires_at or now >= idle_expires_at:
            con.execute(
                "UPDATE sessions SET revoked_at = ? WHERE session_id = ?",
                (_to_iso(now), session_id),
            )
            con.commit()
            return None
        if cfg.bind_user_agent and row["user_agent_hash"]:
            if str(row["user_agent_hash"]) != _hash_user_agent(request):
                return None
        if cfg.bind_ip_prefix and row["ip_prefix_hash"]:
            if str(row["ip_prefix_hash"]) != _hash_ip_prefix(request):
                return None

        role_rows = con.execute(
            """
SELECT r.role_code
FROM user_roles ur
JOIN roles r ON r.role_id = ur.role_id
WHERE ur.user_id = ?
            """,
            (int(row["user_id"]),),
        ).fetchall()
        roles: Set[str] = {str(item["role_code"]) for item in role_rows}
        new_idle_exp = _to_iso(now + dt.timedelta(minutes=cfg.session_idle_minutes))
        con.execute(
            "UPDATE sessions SET last_seen_at = ?, idle_expires_at = ? WHERE session_id = ?",
            (_to_iso(now), new_idle_exp, session_id),
        )
        con.commit()
        return {
            "session_id": str(row["session_id"]),
            "user_id": int(row["user_id"]),
            "email": str(row["email_norm"]),
            "display_name": str(row["display_name"] or row["email_norm"]),
            "roles": roles,
            "csrf_hash": str(row["csrf_secret_hash"]),
            "expires_at": str(row["expires_at"]),
            "idle_expires_at": new_idle_exp,
        }


def attach_auth_context(request: Request) -> None:
    request.state.auth_user = None
    request.state.clear_auth_cookie = False
    if not is_enabled():
        return
    cfg = get_config()
    sid = request.cookies.get(cfg.session_cookie_name)
    if not sid:
        return
    context = _fetch_session_context(request, sid)
    if context is None:
        request.state.clear_auth_cookie = True
        return
    request.state.auth_user = context


def require_authenticated(request: Request) -> Dict[str, Any]:
    if not is_enabled():
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "Auth is disabled", "details": {}})
    context = getattr(request.state, "auth_user", None)
    if not context:
        _audit(request, event_type="auth.access.denied", result="deny", actor_user_id=None, details={"reason": "unauthenticated"})
        raise HTTPException(status_code=401, detail={"code": "unauthorized", "message": "Authentication required", "details": {}})
    return context


def require_admin(request: Request) -> Dict[str, Any]:
    context = require_authenticated(request)
    if "admin" not in set(context.get("roles", [])):
        _audit(
            request,
            event_type="auth.access.denied",
            result="deny",
            actor_user_id=int(context["user_id"]),
            details={"reason": "missing_admin_role"},
        )
        raise HTTPException(status_code=403, detail={"code": "forbidden", "message": "Admin access required", "details": {}})
    return context


def enforce_csrf(request: Request, context: Dict[str, Any]) -> None:
    cfg = get_config()
    if not cfg.csrf_enable:
        return
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    token_header = request.headers.get("x-csrf-token", "")
    token_cookie = request.cookies.get(cfg.csrf_cookie_name, "")
    if not token_header or not token_cookie:
        raise HTTPException(status_code=403, detail={"code": "csrf_failed", "message": "Missing CSRF token", "details": {}})
    if not hmac.compare_digest(token_header, token_cookie):
        raise HTTPException(status_code=403, detail={"code": "csrf_failed", "message": "CSRF token mismatch", "details": {}})
    if not hmac.compare_digest(_sha256_hex(token_header), str(context.get("csrf_hash", ""))):
        raise HTTPException(status_code=403, detail={"code": "csrf_failed", "message": "Invalid CSRF token", "details": {}})


def logout(request: Request) -> Response:
    context = require_authenticated(request)
    enforce_csrf(request, context)
    with admin_db.connection_scope() as con:
        con.execute(
            "UPDATE sessions SET revoked_at = ? WHERE session_id = ?",
            (_to_iso(_utc_now()), str(context["session_id"])),
        )
        con.commit()
    _audit(
        request,
        event_type="auth.logout",
        result="success",
        actor_user_id=int(context["user_id"]),
        details={},
    )
    response = Response(status_code=204)
    clear_auth_cookies(response)
    return response


def auth_me(request: Request) -> Dict[str, Any]:
    if not is_enabled():
        return {"auth_enabled": False, "authenticated": False}
    cfg = get_config()
    context = getattr(request.state, "auth_user", None)
    if not context:
        return {
            "auth_enabled": True,
            "authenticated": False,
            "csrf": {"cookie_name": cfg.csrf_cookie_name, "header_name": "X-CSRF-Token"},
        }
    return {
        "auth_enabled": True,
        "authenticated": True,
        "user": {
            "user_id": int(context["user_id"]),
            "email": str(context["email"]),
            "display_name": str(context["display_name"]),
            "roles": sorted(set(context.get("roles", []))),
        },
        "session": {
            "expires_at": str(context["expires_at"]),
            "idle_expires_at": str(context["idle_expires_at"]),
        },
        "csrf": {"cookie_name": cfg.csrf_cookie_name, "header_name": "X-CSRF-Token"},
    }


def auth_runtime_status() -> Dict[str, Any]:
    cfg = get_config()
    return {
        "enabled": cfg.enabled,
        "provider": cfg.provider,
        "issuer": cfg.issuer,
        "redirect_uri": cfg.redirect_uri,
        "admin_db_path": admin_db.get_admin_db_path_str(),
    }
