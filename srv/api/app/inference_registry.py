from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from cryptography.fernet import Fernet, InvalidToken

from . import admin_db


SUPPORTED_PROVIDERS = {"openai_compatible", "openai", "google", "custom"}
SUPPORTED_AUTH_MODES = {"none", "env", "stored"}


class RegistryError(ValueError):
    pass


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_key(value: str) -> str:
    key = str(value or "").strip().lower()
    out = []
    for ch in key:
        if ch.isalnum() or ch in {"-", "_"}:
            out.append(ch)
        elif ch.isspace():
            out.append("-")
    key = "".join(out).strip("-_")
    if not key:
        raise RegistryError("endpoint_key is required")
    if len(key) > 80:
        raise RegistryError("endpoint_key must be <= 80 characters")
    return key


def _normalize_base_url(value: str) -> str:
    url = str(value or "").strip().rstrip("/")
    if not url:
        raise RegistryError("base_url is required")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RegistryError("base_url must be an absolute http(s) URL")
    return url


def _normalize_provider(value: str) -> str:
    provider = str(value or "").strip().lower()
    if provider not in SUPPORTED_PROVIDERS:
        raise RegistryError(f"provider must be one of: {', '.join(sorted(SUPPORTED_PROVIDERS))}")
    return provider


def _normalize_auth_mode(value: str) -> str:
    mode = str(value or "none").strip().lower()
    if mode not in SUPPORTED_AUTH_MODES:
        raise RegistryError(f"auth_mode must be one of: {', '.join(sorted(SUPPORTED_AUTH_MODES))}")
    return mode


def _fernet() -> Fernet:
    secret = os.getenv("SPACEGATE_SESSION_SECRET", "").strip()
    if not secret:
        raise RegistryError("SPACEGATE_SESSION_SECRET is required to store endpoint secrets")
    digest = hashlib.sha256(f"spacegate-inference-registry:{secret}".encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _encrypt_secret(value: str) -> str:
    return _fernet().encrypt(value.encode("utf-8")).decode("ascii")


def _decrypt_secret(value: str) -> str:
    try:
        return _fernet().decrypt(value.encode("ascii")).decode("utf-8")
    except InvalidToken as exc:
        raise RegistryError("Stored endpoint secret could not be decrypted") from exc


def _row_to_endpoint(row: sqlite3.Row, *, include_models: bool = False) -> Dict[str, Any]:
    endpoint_id = int(row["endpoint_id"])
    try:
        role_defaults = json.loads(row["role_defaults_json"] or "{}")
    except Exception:
        role_defaults = {}
    payload: Dict[str, Any] = {
        "endpoint_id": endpoint_id,
        "endpoint_key": row["endpoint_key"],
        "display_name": row["display_name"],
        "provider": row["provider"],
        "base_url": row["base_url"],
        "auth_mode": row["auth_mode"],
        "api_key_env": row["api_key_env"],
        "api_key_configured": bool(row["api_key_ciphertext"]) or (
            bool(row["api_key_env"]) and bool(os.getenv(str(row["api_key_env"])))
        ),
        "default_model": row["default_model"],
        "role_defaults": role_defaults,
        "timeout_s": int(row["timeout_s"] or 30),
        "enabled": bool(row["enabled"]),
        "notes": row["notes"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "deleted_at": row["deleted_at"],
    }
    if include_models:
        payload["models"] = list_models(endpoint_id)
        payload["last_probe"] = latest_probe(endpoint_id)
    return payload


def list_endpoints(*, include_deleted: bool = False, include_models: bool = True) -> List[Dict[str, Any]]:
    where = "" if include_deleted else "WHERE deleted_at IS NULL"
    with admin_db.connection_scope() as con:
        rows = con.execute(
            f"""
SELECT *
FROM inference_endpoints
{where}
ORDER BY enabled DESC, display_name COLLATE NOCASE ASC, endpoint_id ASC
            """
        ).fetchall()
    return [_row_to_endpoint(row, include_models=include_models) for row in rows]


def get_endpoint(endpoint_id: int, *, include_models: bool = True) -> Dict[str, Any]:
    with admin_db.connection_scope() as con:
        row = con.execute(
            "SELECT * FROM inference_endpoints WHERE endpoint_id = ? AND deleted_at IS NULL",
            (int(endpoint_id),),
        ).fetchone()
    if row is None:
        raise KeyError(endpoint_id)
    return _row_to_endpoint(row, include_models=include_models)


def create_endpoint(payload: Dict[str, Any]) -> Dict[str, Any]:
    endpoint_key = _normalize_key(str(payload.get("endpoint_key") or payload.get("display_name") or ""))
    display_name = str(payload.get("display_name") or "").strip()
    if not display_name:
        raise RegistryError("display_name is required")
    provider = _normalize_provider(str(payload.get("provider") or "openai_compatible"))
    base_url = _normalize_base_url(str(payload.get("base_url") or ""))
    auth_mode = _normalize_auth_mode(str(payload.get("auth_mode") or "none"))
    api_key_env = str(payload.get("api_key_env") or "").strip() or None
    api_key_plain = str(payload.get("api_key") or "").strip()
    api_key_ciphertext = _encrypt_secret(api_key_plain) if api_key_plain else None
    default_model = str(payload.get("default_model") or "").strip() or None
    role_defaults = payload.get("role_defaults") or {}
    if not isinstance(role_defaults, dict):
        raise RegistryError("role_defaults must be an object")
    timeout_s = int(payload.get("timeout_s") or 30)
    if timeout_s < 1 or timeout_s > 600:
        raise RegistryError("timeout_s must be between 1 and 600")
    enabled = 1 if bool(payload.get("enabled", True)) else 0
    notes = str(payload.get("notes") or "").strip() or None
    now = _utc_now()

    with admin_db.connection_scope() as con:
        try:
            cur = con.execute(
                """
INSERT INTO inference_endpoints(
  endpoint_key, display_name, provider, base_url, auth_mode, api_key_env,
  api_key_ciphertext, default_model, role_defaults_json, timeout_s, enabled,
  notes, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    endpoint_key,
                    display_name,
                    provider,
                    base_url,
                    auth_mode,
                    api_key_env,
                    api_key_ciphertext,
                    default_model,
                    json.dumps(role_defaults, sort_keys=True),
                    timeout_s,
                    enabled,
                    notes,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise RegistryError(f"endpoint_key already exists: {endpoint_key}") from exc
        con.commit()
        endpoint_id = int(cur.lastrowid)
    return get_endpoint(endpoint_id)


def update_endpoint(endpoint_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    current = get_endpoint(endpoint_id, include_models=False)
    updates: Dict[str, Any] = {}
    for key in (
        "display_name",
        "base_url",
        "api_key_env",
        "default_model",
        "notes",
    ):
        if key in payload:
            value = str(payload.get(key) or "").strip()
            updates[key] = value or None
    if "endpoint_key" in payload:
        updates["endpoint_key"] = _normalize_key(str(payload.get("endpoint_key") or ""))
    if "provider" in payload:
        updates["provider"] = _normalize_provider(str(payload.get("provider") or ""))
    if "base_url" in updates and updates["base_url"]:
        updates["base_url"] = _normalize_base_url(str(updates["base_url"]))
    if "auth_mode" in payload:
        updates["auth_mode"] = _normalize_auth_mode(str(payload.get("auth_mode") or "none"))
    if "role_defaults" in payload:
        role_defaults = payload.get("role_defaults") or {}
        if not isinstance(role_defaults, dict):
            raise RegistryError("role_defaults must be an object")
        updates["role_defaults_json"] = json.dumps(role_defaults, sort_keys=True)
    if "timeout_s" in payload:
        timeout_s = int(payload.get("timeout_s") or 30)
        if timeout_s < 1 or timeout_s > 600:
            raise RegistryError("timeout_s must be between 1 and 600")
        updates["timeout_s"] = timeout_s
    if "enabled" in payload:
        updates["enabled"] = 1 if bool(payload.get("enabled")) else 0
    if str(payload.get("api_key") or "").strip():
        updates["api_key_ciphertext"] = _encrypt_secret(str(payload.get("api_key")).strip())
    if bool(payload.get("clear_api_key", False)):
        updates["api_key_ciphertext"] = None
    if not updates:
        return current
    updates["updated_at"] = _utc_now()

    set_sql = ", ".join(f"{key} = ?" for key in updates)
    params = list(updates.values()) + [int(endpoint_id)]
    with admin_db.connection_scope() as con:
        try:
            con.execute(
                f"UPDATE inference_endpoints SET {set_sql} WHERE endpoint_id = ? AND deleted_at IS NULL",
                params,
            )
        except sqlite3.IntegrityError as exc:
            raise RegistryError("endpoint_key already exists") from exc
        con.commit()
    return get_endpoint(endpoint_id)


def delete_endpoint(endpoint_id: int) -> None:
    now = _utc_now()
    with admin_db.connection_scope() as con:
        cur = con.execute(
            """
UPDATE inference_endpoints
SET deleted_at = ?, updated_at = ?, enabled = 0
WHERE endpoint_id = ? AND deleted_at IS NULL
            """,
            (now, now, int(endpoint_id)),
        )
        con.commit()
    if cur.rowcount == 0:
        raise KeyError(endpoint_id)


def list_models(endpoint_id: int) -> List[Dict[str, Any]]:
    with admin_db.connection_scope() as con:
        rows = con.execute(
            """
SELECT model_id, model_root, max_model_len, owned_by, first_seen_at, last_seen_at
FROM inference_model_cache
WHERE endpoint_id = ?
ORDER BY model_id COLLATE NOCASE ASC
            """,
            (int(endpoint_id),),
        ).fetchall()
    return [
        {
            "model_id": row["model_id"],
            "model_root": row["model_root"],
            "max_model_len": row["max_model_len"],
            "owned_by": row["owned_by"],
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
        }
        for row in rows
    ]


def latest_probe(endpoint_id: int) -> Optional[Dict[str, Any]]:
    with admin_db.connection_scope() as con:
        row = con.execute(
            """
SELECT status, model_count, latency_ms, error_message, probed_at
FROM inference_endpoint_probes
WHERE endpoint_id = ?
ORDER BY probe_id DESC
LIMIT 1
            """,
            (int(endpoint_id),),
        ).fetchone()
    if row is None:
        return None
    return {
        "status": row["status"],
        "model_count": int(row["model_count"] or 0),
        "latency_ms": row["latency_ms"],
        "error_message": row["error_message"],
        "probed_at": row["probed_at"],
    }


def _auth_header(endpoint: Dict[str, Any], row: sqlite3.Row) -> Optional[str]:
    mode = str(endpoint.get("auth_mode") or "none")
    token = ""
    if mode == "env":
        env_name = str(endpoint.get("api_key_env") or "").strip()
        if env_name:
            token = os.getenv(env_name, "").strip()
        if not token and env_name == "SPACEGATE_OPENAI_API_KEY":
            token = os.getenv("OPENAI_API_KEY", "").strip()
        if not token and env_name == "SPACEGATE_GOOGLE_API_KEY":
            token = os.getenv("GOOGLE_API_KEY", "").strip()
    elif mode == "stored":
        ciphertext = row["api_key_ciphertext"]
        if ciphertext:
            token = _decrypt_secret(str(ciphertext))
    if not token:
        return None
    return f"Bearer {token}"


def _poll_url(endpoint: Dict[str, Any]) -> str:
    base_url = str(endpoint["base_url"]).rstrip("/")
    provider = str(endpoint["provider"])
    if provider == "google":
        return f"{base_url}/models"
    if base_url.endswith("/v1"):
        return f"{base_url}/models"
    return f"{base_url}/v1/models"


def poll_models(endpoint_id: int) -> Dict[str, Any]:
    with admin_db.connection_scope() as con:
        row = con.execute(
            "SELECT * FROM inference_endpoints WHERE endpoint_id = ? AND deleted_at IS NULL",
            (int(endpoint_id),),
        ).fetchone()
    if row is None:
        raise KeyError(endpoint_id)
    endpoint = _row_to_endpoint(row, include_models=False)
    if not endpoint["enabled"]:
        raise RegistryError("endpoint is disabled")

    headers = {"Accept": "application/json"}
    auth_header = _auth_header(endpoint, row)
    if auth_header:
        headers["Authorization"] = auth_header
    url = _poll_url(endpoint)
    if endpoint["provider"] == "google" and not auth_header:
        env_name = str(endpoint.get("api_key_env") or "SPACEGATE_GOOGLE_API_KEY")
        key = os.getenv(env_name, "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()
        if key:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}key={urllib.parse.quote(key)}"

    started = time.monotonic()
    now = _utc_now()
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=int(endpoint["timeout_s"])) as resp:
            raw_text = resp.read().decode("utf-8")
        raw = json.loads(raw_text)
        models = _extract_models(raw, provider=str(endpoint["provider"]))
        latency_ms = int((time.monotonic() - started) * 1000)
        _store_probe(endpoint_id, status="ok", model_count=len(models), latency_ms=latency_ms, error_message=None, probed_at=now)
        _store_models(endpoint_id, models=models, seen_at=now)
        return {
            "endpoint": get_endpoint(endpoint_id),
            "probe": latest_probe(endpoint_id),
            "models": list_models(endpoint_id),
        }
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, RegistryError) as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        message = str(exc)
        if isinstance(exc, urllib.error.HTTPError):
            message = f"HTTP {exc.code}: {exc.reason}"
        _store_probe(endpoint_id, status="error", model_count=0, latency_ms=latency_ms, error_message=message[:500], probed_at=now)
        raise RegistryError(message) from exc


def _extract_models(raw: Dict[str, Any], *, provider: str) -> List[Dict[str, Any]]:
    if provider == "google":
        items = raw.get("models") or raw.get("data") or []
    else:
        items = raw.get("data") or raw.get("models") or []
    if not isinstance(items, list):
        raise RegistryError("model list response did not contain an array")
    models = []
    for item in items:
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or item.get("name") or "").strip()
        if not model_id:
            continue
        if provider == "google" and model_id.startswith("models/"):
            model_id = model_id.split("/", 1)[1]
        models.append(
            {
                "model_id": model_id,
                "model_root": item.get("root") or item.get("name"),
                "max_model_len": item.get("max_model_len") or item.get("inputTokenLimit"),
                "owned_by": item.get("owned_by") or item.get("baseModelId") or provider,
                "raw_json": item,
            }
        )
    return models


def _store_probe(
    endpoint_id: int,
    *,
    status: str,
    model_count: int,
    latency_ms: Optional[int],
    error_message: Optional[str],
    probed_at: str,
) -> None:
    with admin_db.connection_scope() as con:
        con.execute(
            """
INSERT INTO inference_endpoint_probes(endpoint_id, status, model_count, latency_ms, error_message, probed_at)
VALUES (?, ?, ?, ?, ?, ?)
            """,
            (int(endpoint_id), status, int(model_count), latency_ms, error_message, probed_at),
        )
        con.commit()


def _store_models(endpoint_id: int, *, models: List[Dict[str, Any]], seen_at: str) -> None:
    with admin_db.connection_scope() as con:
        for model in models:
            con.execute(
                """
INSERT INTO inference_model_cache(
  endpoint_id, model_id, model_root, max_model_len, owned_by, raw_json, first_seen_at, last_seen_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(endpoint_id, model_id) DO UPDATE SET
  model_root = excluded.model_root,
  max_model_len = excluded.max_model_len,
  owned_by = excluded.owned_by,
  raw_json = excluded.raw_json,
  last_seen_at = excluded.last_seen_at
                """,
                (
                    int(endpoint_id),
                    model["model_id"],
                    model.get("model_root"),
                    model.get("max_model_len"),
                    model.get("owned_by"),
                    json.dumps(model.get("raw_json") or {}, sort_keys=True),
                    seen_at,
                    seen_at,
                ),
            )
        con.commit()


def usage_stats() -> Dict[str, Any]:
    with admin_db.connection_scope() as con:
        rows = con.execute(
            """
SELECT
  e.endpoint_id,
  e.endpoint_key,
  e.display_name,
  u.model_id,
  count(*) AS request_count,
  coalesce(sum(u.prompt_tokens), 0) AS prompt_tokens,
  coalesce(sum(u.completion_tokens), 0) AS completion_tokens,
  coalesce(sum(u.total_tokens), 0) AS total_tokens,
  avg(u.latency_ms) AS avg_latency_ms,
  max(u.created_at) AS last_used_at
FROM inference_usage_events u
LEFT JOIN inference_endpoints e ON e.endpoint_id = u.endpoint_id
GROUP BY e.endpoint_id, e.endpoint_key, e.display_name, u.model_id
ORDER BY request_count DESC, last_used_at DESC
            """
        ).fetchall()
    return {
        "items": [
            {
                "endpoint_id": row["endpoint_id"],
                "endpoint_key": row["endpoint_key"],
                "display_name": row["display_name"],
                "model_id": row["model_id"],
                "request_count": int(row["request_count"] or 0),
                "prompt_tokens": int(row["prompt_tokens"] or 0),
                "completion_tokens": int(row["completion_tokens"] or 0),
                "total_tokens": int(row["total_tokens"] or 0),
                "avg_latency_ms": float(row["avg_latency_ms"]) if row["avg_latency_ms"] is not None else None,
                "last_used_at": row["last_used_at"],
            }
            for row in rows
        ]
    }
