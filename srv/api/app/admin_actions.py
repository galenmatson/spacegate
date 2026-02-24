from __future__ import annotations

import datetime as dt
import json
import os
import secrets
import sqlite3
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, TextIO

from . import admin_db


ROOT_DIR = Path(__file__).resolve().parents[3]

RUNNING_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}

_RUNNER_LOCK = threading.Lock()


class ActionValidationError(ValueError):
    pass


class ActionPermissionError(PermissionError):
    pass


@dataclass(frozen=True)
class ActionSpec:
    name: str
    description: str
    params_schema: Dict[str, Dict[str, Any]]
    display_name: str | None = None
    category: str = "operations"
    hidden: bool = False
    risk_level: str = "low"
    required_roles: Sequence[str] = ("admin",)
    requires_confirmation: bool = False
    confirmation_phrase: str | None = None
    build_command: Callable[[Dict[str, Any]], List[str]] | None = None
    run_native: Callable[[Dict[str, Any], TextIO], int] | None = None


@dataclass(frozen=True)
class ExecutionPlan:
    kind: str  # command | native
    argv: List[str] | None = None
    native_handler: str | None = None


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_iso(value: dt.datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _state_dir() -> Path:
    raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR")
    if raw:
        return Path(raw).expanduser()
    return ROOT_DIR / "data"


def _jobs_dir() -> Path:
    raw = os.getenv("SPACEGATE_ADMIN_JOBS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser()
    return _state_dir() / "admin" / "jobs"


def _backups_dir() -> Path:
    raw = os.getenv("SPACEGATE_ADMIN_BACKUPS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser()
    return _state_dir() / "admin" / "backups"


def _dl_root() -> Path:
    raw = os.getenv("SPACEGATE_DL_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path("/srv/spacegate/dl")


def _ensure_jobs_dir() -> Path:
    path = _jobs_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _ensure_backups_subdir(name: str) -> Path:
    path = _backups_dir() / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _is_safe_build_id(value: str) -> bool:
    if not value:
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:")
    return all(ch in allowed for ch in value)


def _is_safe_backup_name(value: str) -> bool:
    if not value:
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    return all(ch in allowed for ch in value) and "/" not in value and ".." not in value


def _ts_slug() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%SZ")


def _max_concurrent_jobs() -> int:
    return _parse_env_int("SPACEGATE_ADMIN_MAX_RUNNING_JOBS", 1)


def _max_queued_jobs() -> int:
    return _parse_env_int("SPACEGATE_ADMIN_MAX_QUEUED_JOBS", 20)


def _count_running_jobs(con: sqlite3.Connection) -> int:
    row = con.execute(
        "SELECT COUNT(*) FROM admin_jobs WHERE status = 'running'"
    ).fetchone()
    return int(row[0]) if row else 0


def _count_queued_jobs(con: sqlite3.Connection) -> int:
    row = con.execute(
        "SELECT COUNT(*) FROM admin_jobs WHERE status = 'queued'"
    ).fetchone()
    return int(row[0]) if row else 0


def _normalize_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    raise ActionValidationError("Expected boolean value")


def _normalize_integer(value: Any) -> int:
    if isinstance(value, bool):
        raise ActionValidationError("Expected integer value")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError as exc:
            raise ActionValidationError("Expected integer value") from exc
    raise ActionValidationError("Expected integer value")


def _normalize_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_command_build_core(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "build_core.sh")]
    if params.get("overwrite", False):
        cmd.append("--overwrite")
    return cmd


def _build_command_verify_build(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "verify_build.sh")]
    build_id = str(params.get("build_id", "") or "").strip()
    if build_id:
        cmd.append(build_id)
    return cmd


def _build_command_publish_db(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "publish_db.sh")]
    build_id = str(params.get("build_id", "") or "").strip()
    if build_id:
        cmd.append(build_id)
    return cmd


def _build_command_restart_services(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "run_spacegate.sh"), "--restart"]
    if params.get("web_dev", False):
        cmd.append("--web-dev")
    else:
        cmd.append("--api-only")
    return cmd


def _build_command_stop_services(params: Dict[str, Any]) -> List[str]:
    return [str(ROOT_DIR / "scripts" / "run_spacegate.sh"), "--stop"]


def _build_command_score_coolness(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "score_coolness.sh")]
    build_id = str(params.get("build_id", "") or "").strip()
    if build_id:
        cmd.extend(["--build-id", build_id])
    profile_id = str(params.get("profile_id", "") or "").strip()
    if profile_id:
        cmd.extend(["--profile-id", profile_id])
    profile_version = str(params.get("profile_version", "") or "").strip()
    if profile_version:
        cmd.extend(["--profile-version", profile_version])
    weights_json = str(params.get("weights_json", "") or "").strip()
    if weights_json:
        cmd.extend(["--weights-json", weights_json])
    if _normalize_boolean(params.get("ephemeral", False)):
        cmd.append("--ephemeral")
    return cmd


def _build_command_generate_snapshots(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "generate_snapshots.sh")]
    build_id = str(params.get("build_id", "") or "").strip()
    if build_id:
        cmd.extend(["--build-id", build_id])
    top_coolness = _normalize_integer(params.get("top_coolness", 200))
    if top_coolness <= 0:
        raise ActionValidationError("top_coolness must be > 0")
    cmd.extend(["--top-coolness", str(top_coolness)])
    view_type = str(params.get("view_type", "") or "").strip()
    if view_type:
        cmd.extend(["--view-type", view_type])
    if _normalize_boolean(params.get("force", False)):
        cmd.append("--force")
    return cmd


def _build_command_save_coolness_profile(params: Dict[str, Any]) -> List[str]:
    profile_id = str(params.get("profile_id", "") or "").strip()
    profile_version = str(params.get("profile_version", "") or "").strip()
    if not profile_id or not profile_version:
        raise ActionValidationError("profile_id and profile_version are required")
    cmd = [
        str(ROOT_DIR / "scripts" / "score_coolness.sh"),
        "save",
        "--profile-id",
        profile_id,
        "--profile-version",
        profile_version,
    ]
    weights_json = str(params.get("weights_json", "") or "").strip()
    if weights_json:
        cmd.extend(["--weights-json", weights_json])
    notes = str(params.get("notes", "") or "").strip()
    if notes:
        cmd.extend(["--notes", notes])
    return cmd


def _build_command_apply_coolness_profile(params: Dict[str, Any]) -> List[str]:
    profile_id = str(params.get("profile_id", "") or "").strip()
    profile_version = str(params.get("profile_version", "") or "").strip()
    if not profile_id or not profile_version:
        raise ActionValidationError("profile_id and profile_version are required")
    cmd = [
        str(ROOT_DIR / "scripts" / "score_coolness.sh"),
        "apply",
        "--profile-id",
        profile_id,
        "--profile-version",
        profile_version,
    ]
    reason = str(params.get("reason", "") or "").strip()
    if reason:
        cmd.extend(["--reason", reason])
    return cmd


def _create_admin_db_backup(logf: TextIO, suffix: str = "") -> Path:
    src_path = admin_db.get_admin_db_path()
    if not src_path.exists():
        raise ActionValidationError(f"Admin DB not found: {src_path}")

    backup_dir = _ensure_backups_subdir("admin_db")
    backup_name = f"admin_{_ts_slug()}_{secrets.token_hex(3)}"
    if suffix:
        backup_name = f"{backup_name}_{suffix}"
    out_path = backup_dir / f"{backup_name}.sqlite3"

    src_con = sqlite3.connect(str(src_path))
    dst_con = sqlite3.connect(str(out_path))
    try:
        src_con.backup(dst_con)
    finally:
        dst_con.close()
        src_con.close()

    logf.write(f"Created admin DB backup: {out_path}\n")
    return out_path


def _sqlite_table_exists(con: sqlite3.Connection, schema: str, table: str) -> bool:
    row = con.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _fetch_job_user_ids(con: sqlite3.Connection) -> List[int]:
    rows = con.execute(
        "SELECT DISTINCT requested_by_user_id FROM admin_jobs"
    ).fetchall()
    out: List[int] = []
    for row in rows:
        value = row[0]
        if value is None:
            continue
        out.append(int(value))
    return sorted(set(out))


def _snapshot_users(con: sqlite3.Connection, user_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
    if not user_ids:
        return {}
    placeholders = ",".join("?" for _ in user_ids)
    rows = con.execute(
        f"""
SELECT user_id, email_norm, display_name, status, created_at, updated_at, last_login_at
FROM users
WHERE user_id IN ({placeholders})
        """,
        tuple(user_ids),
    ).fetchall()
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        user_id = int(row["user_id"])
        out[user_id] = {
            "user_id": user_id,
            "email_norm": str(row["email_norm"]),
            "display_name": row["display_name"],
            "status": str(row["status"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "last_login_at": row["last_login_at"],
        }
    return out


def _snapshot_user_role_codes(
    con: sqlite3.Connection, user_ids: Sequence[int]
) -> Dict[int, List[str]]:
    if not user_ids:
        return {}
    placeholders = ",".join("?" for _ in user_ids)
    rows = con.execute(
        f"""
SELECT ur.user_id AS user_id, r.role_code AS role_code
FROM user_roles ur
JOIN roles r ON r.role_id = ur.role_id
WHERE ur.user_id IN ({placeholders})
        """,
        tuple(user_ids),
    ).fetchall()
    out: Dict[int, List[str]] = {}
    for row in rows:
        user_id = int(row["user_id"])
        role_code = str(row["role_code"])
        out.setdefault(user_id, []).append(role_code)
    return out


def _run_native_backup_admin_db(params: Dict[str, Any], logf: TextIO) -> int:
    _create_admin_db_backup(logf)
    return 0


def _run_native_restore_admin_db(params: Dict[str, Any], logf: TextIO) -> int:
    backup_name = str(params.get("backup_name", "") or "").strip()
    if not _is_safe_backup_name(backup_name):
        raise ActionValidationError("Invalid backup_name format")

    backup_dir = _ensure_backups_subdir("admin_db")
    backup_path = backup_dir / backup_name
    if backup_path.suffix != ".sqlite3":
        raise ActionValidationError("backup_name must end with .sqlite3")
    if not backup_path.exists():
        raise ActionValidationError(f"Backup not found: {backup_name}")

    # Always capture the current DB before restore.
    _create_admin_db_backup(logf, suffix="pre_restore")

    attached = False
    in_tx = False
    with admin_db.connection_scope() as con:
        con.execute("PRAGMA foreign_keys = OFF")
        try:
            job_user_ids = _fetch_job_user_ids(con)
            user_snapshot = _snapshot_users(con, job_user_ids)
            role_snapshot = _snapshot_user_role_codes(con, job_user_ids)

            con.execute("ATTACH DATABASE ? AS backup_db", (str(backup_path),))
            attached = True

            required_backup_tables = [
                "users",
                "auth_identities",
                "roles",
                "user_roles",
                "admin_allowlist",
                "sessions",
                "audit_log",
            ]
            missing_tables = [
                name
                for name in required_backup_tables
                if not _sqlite_table_exists(con, "backup_db", name)
            ]
            if missing_tables:
                raise ActionValidationError(
                    "Backup schema is missing required tables: "
                    + ", ".join(sorted(missing_tables))
                )

            con.execute("BEGIN IMMEDIATE")
            in_tx = True

            # Restore selected tables in FK-safe order. Keep admin_jobs unchanged so
            # the currently running restore job remains visible/tracked.
            for table in (
                "auth_identities",
                "user_roles",
                "sessions",
                "admin_allowlist",
                "audit_log",
                "users",
                "roles",
            ):
                con.execute(f"DELETE FROM {table}")

            con.execute(
                """
INSERT INTO roles(role_id, role_code)
SELECT role_id, role_code
FROM backup_db.roles
                """
            )
            con.execute(
                """
INSERT INTO users(user_id, email_norm, display_name, status, created_at, updated_at, last_login_at)
SELECT user_id, email_norm, display_name, status, created_at, updated_at, last_login_at
FROM backup_db.users
                """
            )
            con.execute(
                """
INSERT INTO auth_identities(
  identity_id, user_id, provider, issuer, provider_sub, email_at_login, email_verified,
  claims_json, created_at, last_login_at
)
SELECT identity_id, user_id, provider, issuer, provider_sub, email_at_login, email_verified,
       claims_json, created_at, last_login_at
FROM backup_db.auth_identities
                """
            )
            con.execute(
                """
INSERT INTO user_roles(user_id, role_id)
SELECT user_id, role_id
FROM backup_db.user_roles
                """
            )
            con.execute(
                """
INSERT INTO admin_allowlist(
  allow_id, provider, issuer, provider_sub, email_norm, enabled, note, created_at, updated_at
)
SELECT allow_id, provider, issuer, provider_sub, email_norm, enabled, note, created_at, updated_at
FROM backup_db.admin_allowlist
                """
            )
            con.execute(
                """
INSERT INTO sessions(
  session_id, user_id, created_at, last_seen_at, expires_at, idle_expires_at, revoked_at,
  csrf_secret_hash, user_agent_hash, ip_prefix_hash
)
SELECT session_id, user_id, created_at, last_seen_at, expires_at, idle_expires_at, revoked_at,
       csrf_secret_hash, user_agent_hash, ip_prefix_hash
FROM backup_db.sessions
                """
            )
            con.execute(
                """
INSERT INTO audit_log(
  audit_id, actor_user_id, event_type, result, request_id, route, method, details_json, created_at
)
SELECT audit_id, actor_user_id, event_type, result, request_id, route, method, details_json, created_at
FROM backup_db.audit_log
                """
            )

            # If backup predates users referenced by existing admin jobs, re-insert
            # minimal rows and their prior role assignments to preserve FK integrity.
            if job_user_ids:
                placeholders = ",".join("?" for _ in job_user_ids)
                rows = con.execute(
                    f"SELECT user_id FROM users WHERE user_id IN ({placeholders})",
                    tuple(job_user_ids),
                ).fetchall()
                present_ids = {int(row["user_id"]) for row in rows}
                missing_ids = [uid for uid in job_user_ids if uid not in present_ids]
            else:
                missing_ids = []

            for user_id in missing_ids:
                payload = user_snapshot.get(user_id)
                if payload is None:
                    continue
                con.execute(
                    """
INSERT OR IGNORE INTO users(
  user_id, email_norm, display_name, status, created_at, updated_at, last_login_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["user_id"],
                        payload["email_norm"],
                        payload["display_name"],
                        payload["status"],
                        payload["created_at"],
                        payload["updated_at"],
                        payload["last_login_at"],
                    ),
                )
                for role_code in role_snapshot.get(user_id, []):
                    con.execute(
                        "INSERT OR IGNORE INTO roles(role_code) VALUES (?)",
                        (role_code,),
                    )
                    role_row = con.execute(
                        "SELECT role_id FROM roles WHERE role_code = ?",
                        (role_code,),
                    ).fetchone()
                    if role_row is not None:
                        con.execute(
                            "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES (?, ?)",
                            (user_id, int(role_row["role_id"])),
                        )

            dangling_row = con.execute(
                """
SELECT j.job_id, j.requested_by_user_id
FROM admin_jobs j
LEFT JOIN users u ON u.user_id = j.requested_by_user_id
WHERE u.user_id IS NULL
LIMIT 1
                """
            ).fetchone()
            if dangling_row is not None:
                raise ActionValidationError(
                    "Restore would leave admin_jobs referencing missing users"
                )

            fk_issue = con.execute("PRAGMA foreign_key_check").fetchone()
            if fk_issue is not None:
                raise ActionValidationError("Restore failed foreign key validation")

            con.execute("COMMIT")
            in_tx = False
        finally:
            if in_tx:
                con.execute("ROLLBACK")
            if attached:
                con.execute("DETACH DATABASE backup_db")
            con.execute("PRAGMA foreign_keys = ON")

    # Re-run bootstrap to ensure seeded roles/allowlist are present for env config.
    admin_db.initialize()
    logf.write(
        "Restored admin auth/audit tables from backup and preserved active admin job references.\n"
    )
    logf.write(f"Backup source: {backup_name}\n")
    return 0


def _run_native_backup_release_metadata(params: Dict[str, Any], logf: TextIO) -> int:
    dl_root = _dl_root()
    current_json = dl_root / "current.json"
    current_link = dl_root / "current"

    if not current_json.exists():
        raise ActionValidationError(f"Missing metadata file: {current_json}")

    backup_id = f"meta_{_ts_slug()}_{secrets.token_hex(3)}"
    out_dir = _ensure_backups_subdir("release_metadata") / backup_id
    out_dir.mkdir(parents=True, exist_ok=True)

    out_current = out_dir / "current.json"
    out_current.write_text(current_json.read_text(encoding="utf-8"), encoding="utf-8")

    symlink_target = None
    if current_link.is_symlink():
        symlink_target = os.readlink(current_link)

    manifest = {
        "backup_id": backup_id,
        "created_at": _to_iso(_utc_now()),
        "source_dl_root": str(dl_root),
        "source_current_json": str(current_json),
        "current_symlink_target": symlink_target,
        "files": ["current.json"],
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    logf.write(f"Created release metadata backup: {out_dir}\n")
    return 0


def _run_native_restore_release_metadata(params: Dict[str, Any], logf: TextIO) -> int:
    backup_id = str(params.get("backup_id", "") or "").strip()
    restore_symlink = bool(params.get("restore_symlink", True))
    if not _is_safe_backup_name(backup_id):
        raise ActionValidationError("Invalid backup_id format")

    backup_root = _ensure_backups_subdir("release_metadata")
    src_dir = backup_root / backup_id
    if not src_dir.exists() or not src_dir.is_dir():
        raise ActionValidationError(f"Backup not found: {backup_id}")

    src_current = src_dir / "current.json"
    src_manifest = src_dir / "manifest.json"
    if not src_current.exists() or not src_manifest.exists():
        raise ActionValidationError(f"Backup missing current.json/manifest.json: {backup_id}")

    manifest = json.loads(src_manifest.read_text(encoding="utf-8"))
    dl_root = _dl_root()
    dl_root.mkdir(parents=True, exist_ok=True)

    target_current = dl_root / "current.json"
    target_current.write_text(src_current.read_text(encoding="utf-8"), encoding="utf-8")
    logf.write(f"Restored current.json from backup: {backup_id}\n")

    if restore_symlink:
        symlink_target = str(manifest.get("current_symlink_target") or "").strip()
        if symlink_target:
            target_link = dl_root / "current"
            if target_link.exists() or target_link.is_symlink():
                target_link.unlink()
            target_link.symlink_to(symlink_target)
            logf.write(f"Restored current symlink -> {symlink_target}\n")

    return 0


def _confirmation_for(action_name: str) -> str:
    return f"RUN {action_name}"


ACTION_SPECS: Dict[str, ActionSpec] = {
    "build_core": ActionSpec(
        name="build_core",
        display_name="Build Core",
        description="Run full core pipeline: download, cook, ingest, promote, verify.",
        params_schema={
            "overwrite": {
                "type": "boolean",
                "default": False,
                "label": "Overwrite cached inputs",
            }
        },
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("build_core"),
        build_command=_build_command_build_core,
    ),
    "verify_build": ActionSpec(
        name="verify_build",
        display_name="Verify Build",
        description="Verify served/current build or a specific build_id.",
        params_schema={
            "build_id": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "placeholder": "2026-02-19T221543Z_2774126",
                "label": "Build ID (optional)",
            }
        },
        risk_level="low",
        build_command=_build_command_verify_build,
    ),
    "publish_db": ActionSpec(
        name="publish_db",
        display_name="Publish Database",
        description="Package promoted build and update /dl metadata and reports.",
        params_schema={
            "build_id": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "placeholder": "leave empty for served/current",
                "label": "Build ID (optional)",
            }
        },
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("publish_db"),
        build_command=_build_command_publish_db,
    ),
    "restart_services": ActionSpec(
        name="restart_services",
        display_name="Restart Services",
        description="Restart API service via run_spacegate.sh (optionally web dev server).",
        params_schema={
            "web_dev": {
                "type": "boolean",
                "default": False,
                "label": "Enable web dev mode",
            }
        },
        risk_level="medium",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("restart_services"),
        build_command=_build_command_restart_services,
    ),
    "stop_services": ActionSpec(
        name="stop_services",
        display_name="Stop Services",
        description="Stop API/web processes tracked by run_spacegate.sh pid files.",
        params_schema={},
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("stop_services"),
        build_command=_build_command_stop_services,
    ),
    "score_coolness": ActionSpec(
        name="score_coolness",
        display_name="Score Coolness",
        description="Generate rich coolness ranking + report for a build (supports ephemeral scoring).",
        params_schema={
            "build_id": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "placeholder": "leave empty for served/current",
                "label": "Build ID (optional)",
            },
            "profile_id": {
                "type": "string",
                "required": False,
                "default": "default",
                "allow_empty": False,
                "label": "Profile ID",
            },
            "profile_version": {
                "type": "string",
                "required": False,
                "default": "1",
                "allow_empty": False,
                "label": "Profile Version",
            },
            "weights_json": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "placeholder": "{\"weird_planets\":0.20,\"exotic_star\":0.12}",
                "label": "Weight Overrides JSON (optional)",
            },
            "ephemeral": {
                "type": "boolean",
                "required": False,
                "default": False,
                "label": "Ephemeral (do not persist profile)",
            },
        },
        category="coolness",
        risk_level="low",
        build_command=_build_command_score_coolness,
    ),
    "generate_snapshots": ActionSpec(
        name="generate_snapshots",
        display_name="Generate Snapshots",
        description="Render system snapshot images for top coolness-ranked systems (defaults to top 200).",
        params_schema={
            "build_id": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "placeholder": "leave empty for served/current",
                "label": "Build ID (optional)",
            },
            "top_coolness": {
                "type": "integer",
                "required": False,
                "default": 200,
                "min": 1,
                "max": 10000,
                "label": "Top coolness systems",
            },
            "view_type": {
                "type": "string",
                "required": False,
                "default": "system",
                "allow_empty": False,
                "label": "View type",
            },
            "force": {
                "type": "boolean",
                "required": False,
                "default": False,
                "label": "Force regenerate existing images",
            },
        },
        category="coolness",
        risk_level="low",
        build_command=_build_command_generate_snapshots,
    ),
    "save_coolness_profile": ActionSpec(
        name="save_coolness_profile",
        display_name="Save Coolness Profile",
        description="Persist an immutable coolness profile version without activating it.",
        params_schema={
            "profile_id": {
                "type": "string",
                "required": True,
                "default": "default",
                "allow_empty": False,
                "label": "Profile ID",
            },
            "profile_version": {
                "type": "string",
                "required": True,
                "default": "1",
                "allow_empty": False,
                "label": "Profile Version",
            },
            "weights_json": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "placeholder": "{\"weird_planets\":0.20,\"exotic_star\":0.12}",
                "label": "Weight Overrides JSON (optional)",
            },
            "notes": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Notes (optional)",
            },
        },
        category="coolness",
        risk_level="low",
        build_command=_build_command_save_coolness_profile,
    ),
    "apply_coolness_profile": ActionSpec(
        name="apply_coolness_profile",
        display_name="Activate Coolness Profile",
        description="Activate a saved immutable coolness profile version.",
        params_schema={
            "profile_id": {
                "type": "string",
                "required": True,
                "default": "default",
                "allow_empty": False,
                "label": "Profile ID",
            },
            "profile_version": {
                "type": "string",
                "required": True,
                "default": "1",
                "allow_empty": False,
                "label": "Profile Version",
            },
            "reason": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Reason (optional)",
            },
        },
        category="coolness",
        risk_level="medium",
        build_command=_build_command_apply_coolness_profile,
    ),
    "backup_admin_db": ActionSpec(
        name="backup_admin_db",
        display_name="Backup Admin DB",
        description="Create a point-in-time backup of admin auth/audit SQLite DB.",
        params_schema={},
        risk_level="low",
        run_native=_run_native_backup_admin_db,
    ),
    "restore_admin_db": ActionSpec(
        name="restore_admin_db",
        display_name="Restore Admin DB",
        description="Restore admin auth/audit DB from a backup file in admin backups.",
        params_schema={
            "backup_name": {
                "type": "string",
                "required": True,
                "allow_empty": False,
                "placeholder": "admin_20260220T190000Z_ab12cd.sqlite3",
                "label": "Backup filename",
            }
        },
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("restore_admin_db"),
        run_native=_run_native_restore_admin_db,
    ),
    "backup_release_metadata": ActionSpec(
        name="backup_release_metadata",
        display_name="Backup Release Metadata",
        description="Backup /dl/current.json and current symlink metadata.",
        params_schema={},
        risk_level="low",
        run_native=_run_native_backup_release_metadata,
    ),
    "restore_release_metadata": ActionSpec(
        name="restore_release_metadata",
        display_name="Restore Release Metadata",
        description="Restore /dl/current.json (and optionally current symlink) from backup.",
        params_schema={
            "backup_id": {
                "type": "string",
                "required": True,
                "allow_empty": False,
                "placeholder": "meta_20260220T190000Z_ab12cd",
                "label": "Backup ID",
            },
            "restore_symlink": {
                "type": "boolean",
                "default": True,
                "label": "Restore current symlink target",
            },
        },
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("restore_release_metadata"),
        run_native=_run_native_restore_release_metadata,
    ),
}


def list_actions() -> List[Dict[str, Any]]:
    data = []
    for spec in sorted(ACTION_SPECS.values(), key=lambda s: s.name):
        if spec.hidden:
            continue
        data.append(
            {
                "name": spec.name,
                "display_name": spec.display_name or spec.name,
                "description": spec.description,
                "params_schema": spec.params_schema,
                "category": spec.category,
                "risk_level": spec.risk_level,
                "required_roles": list(spec.required_roles),
                "requires_confirmation": bool(spec.requires_confirmation),
                "confirmation_phrase": spec.confirmation_phrase or "",
            }
        )
    return data


def _canonicalize_params(params: Dict[str, Any] | None) -> Dict[str, Any]:
    if params is None:
        return {}
    if not isinstance(params, dict):
        raise ActionValidationError("params must be an object")
    return params


def _validate_params(spec: ActionSpec, params: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    schema = spec.params_schema or {}

    unknown_keys = sorted(set(params.keys()) - set(schema.keys()))
    if unknown_keys:
        raise ActionValidationError(f"Unsupported params: {', '.join(unknown_keys)}")

    for name, field in schema.items():
        required = bool(field.get("required", False))
        has_value = name in params
        if not has_value:
            if "default" in field:
                normalized[name] = field.get("default")
                continue
            if required:
                raise ActionValidationError(f"Missing required parameter: {name}")
            continue

        raw_value = params.get(name)
        field_type = str(field.get("type", "string"))
        if field_type == "boolean":
            value = _normalize_boolean(raw_value)
        elif field_type == "integer":
            value = _normalize_integer(raw_value)
            min_value = field.get("min")
            max_value = field.get("max")
            if min_value is not None and value < int(min_value):
                raise ActionValidationError(f"{name} must be >= {min_value}")
            if max_value is not None and value > int(max_value):
                raise ActionValidationError(f"{name} must be <= {max_value}")
        else:
            value = _normalize_string(raw_value)
            allow_empty = bool(field.get("allow_empty", False))
            if required and not value:
                raise ActionValidationError(f"Missing required parameter: {name}")
            if not allow_empty and "default" not in field and not value:
                raise ActionValidationError(f"{name} cannot be empty")

        enum_values = field.get("enum")
        if enum_values is not None and value not in enum_values:
            raise ActionValidationError(f"{name} must be one of: {', '.join(map(str, enum_values))}")

        normalized[name] = value

    if spec.name in {"verify_build", "publish_db", "build_core", "score_coolness", "generate_snapshots"}:
        build_id = str(normalized.get("build_id", "") or "").strip()
        if build_id and not _is_safe_build_id(build_id):
            raise ActionValidationError("Invalid build_id format")
        if "build_id" in normalized:
            normalized["build_id"] = build_id

    if spec.name == "restore_admin_db":
        backup_name = str(normalized.get("backup_name", "") or "").strip()
        if not _is_safe_backup_name(backup_name):
            raise ActionValidationError("Invalid backup_name format")
        if not backup_name.endswith(".sqlite3"):
            raise ActionValidationError("backup_name must end with .sqlite3")
        normalized["backup_name"] = backup_name

    if spec.name == "restore_release_metadata":
        backup_id = str(normalized.get("backup_id", "") or "").strip()
        if not _is_safe_backup_name(backup_id):
            raise ActionValidationError("Invalid backup_id format")
        normalized["backup_id"] = backup_id

    return normalized


def _validate_and_plan(
    *,
    action: str,
    params: Dict[str, Any],
    user_roles: Sequence[str],
    confirmation: str | None,
) -> tuple[ActionSpec, Dict[str, Any], ExecutionPlan]:
    spec = ACTION_SPECS.get(action)
    if spec is None:
        raise ActionValidationError(f"Unsupported action: {action}")

    role_set = {str(role) for role in user_roles}
    missing_roles = [role for role in spec.required_roles if role not in role_set]
    if missing_roles:
        raise ActionPermissionError(f"Missing required role(s): {', '.join(missing_roles)}")

    normalized = _validate_params(spec, params)

    if spec.requires_confirmation:
        expected_phrase = spec.confirmation_phrase or _confirmation_for(spec.name)
        if str(confirmation or "").strip() != expected_phrase:
            raise ActionValidationError(
                f"Confirmation phrase mismatch. Expected: {expected_phrase}"
            )

    if spec.build_command is not None:
        argv = spec.build_command(normalized)
        return spec, normalized, ExecutionPlan(kind="command", argv=argv)

    if spec.run_native is not None:
        return spec, normalized, ExecutionPlan(kind="native", native_handler=spec.name)

    raise ActionValidationError(f"Action has no execution handler: {spec.name}")


def _insert_job(
    con: sqlite3.Connection,
    *,
    job_id: str,
    action: str,
    requested_by_user_id: int,
    params_json: str,
    command_json: str,
    log_path: str,
) -> None:
    now = _to_iso(_utc_now())
    con.execute(
        """
INSERT INTO admin_jobs(
  job_id, action, status, requested_by_user_id, params_json, command_json,
  log_path, created_at, started_at, finished_at, exit_code, error_message
) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
        """,
        (
            job_id,
            action,
            requested_by_user_id,
            params_json,
            command_json,
            log_path,
            now,
        ),
    )


def _plan_to_json(plan: ExecutionPlan) -> str:
    payload: Dict[str, Any] = {"kind": plan.kind}
    if plan.argv is not None:
        payload["argv"] = plan.argv
    if plan.native_handler is not None:
        payload["native_handler"] = plan.native_handler
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _plan_from_json(raw: str) -> ExecutionPlan:
    payload = json.loads(raw or "{}")
    kind = str(payload.get("kind", ""))
    if kind == "command":
        argv = payload.get("argv")
        if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
            raise ActionValidationError("Invalid command plan")
        return ExecutionPlan(kind="command", argv=list(argv))
    if kind == "native":
        native_handler = str(payload.get("native_handler", "")).strip()
        if not native_handler:
            raise ActionValidationError("Invalid native execution plan")
        return ExecutionPlan(kind="native", native_handler=native_handler)
    raise ActionValidationError("Unknown execution plan kind")


def _start_queued_jobs() -> None:
    jobs_to_launch: List[Dict[str, Any]] = []
    with _RUNNER_LOCK:
        with admin_db.connection_scope() as con:
            running = _count_running_jobs(con)
            max_running = _max_concurrent_jobs()
            slots = max(0, max_running - running)
            if slots == 0:
                return

            rows = con.execute(
                """
SELECT job_id, action, requested_by_user_id, params_json, command_json, log_path
FROM admin_jobs
WHERE status = 'queued'
ORDER BY created_at ASC
LIMIT ?
                """,
                (slots,),
            ).fetchall()
            if not rows:
                return

            started_at = _to_iso(_utc_now())
            for row in rows:
                con.execute(
                    "UPDATE admin_jobs SET status='running', started_at=? WHERE job_id=? AND status='queued'",
                    (started_at, str(row["job_id"])),
                )
                jobs_to_launch.append(
                    {
                        "job_id": str(row["job_id"]),
                        "action": str(row["action"]),
                        "requested_by_user_id": int(row["requested_by_user_id"]),
                        "params_json": str(row["params_json"] or "{}"),
                        "command_json": str(row["command_json"] or "{}"),
                        "log_path": str(row["log_path"]),
                    }
                )
            con.commit()

    for item in jobs_to_launch:
        thread = threading.Thread(
            target=_run_job_worker,
            kwargs={
                "job_id": item["job_id"],
                "action": item["action"],
                "params_json": item["params_json"],
                "command_json": item["command_json"],
                "log_path": Path(item["log_path"]),
                "actor_user_id": item["requested_by_user_id"],
            },
            daemon=True,
        )
        thread.start()


def start_job(
    *,
    action: str,
    params: Dict[str, Any] | None,
    requested_by_user_id: int,
    user_roles: Sequence[str],
    confirmation: str | None = None,
) -> Dict[str, Any]:
    parsed_params = _canonicalize_params(params)
    spec, normalized_params, plan = _validate_and_plan(
        action=action,
        params=parsed_params,
        user_roles=user_roles,
        confirmation=confirmation,
    )

    jobs_dir = _ensure_jobs_dir()
    job_id = f"job_{_utc_now().strftime('%Y%m%dT%H%M%SZ')}_{secrets.token_hex(5)}"
    log_path = jobs_dir / f"{job_id}.log"

    with _RUNNER_LOCK:
        with admin_db.connection_scope() as con:
            queued = _count_queued_jobs(con)
            max_queued = _max_queued_jobs()
            if queued >= max_queued:
                raise RuntimeError(
                    f"Too many queued jobs ({queued}/{max_queued}); try again later."
                )
            _insert_job(
                con,
                job_id=job_id,
                action=spec.name,
                requested_by_user_id=requested_by_user_id,
                params_json=json.dumps(normalized_params, separators=(",", ":"), sort_keys=True),
                command_json=_plan_to_json(plan),
                log_path=str(log_path),
            )
            con.commit()

    _start_queued_jobs()
    return get_job(job_id)


def cancel_job(*, job_id: str) -> Dict[str, Any]:
    with _RUNNER_LOCK:
        with admin_db.connection_scope() as con:
            row = con.execute(
                "SELECT status FROM admin_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Job not found: {job_id}")
            status = str(row["status"])
            if status != "queued":
                raise RuntimeError(f"Only queued jobs can be cancelled (current status: {status})")
            finished_at = _to_iso(_utc_now())
            con.execute(
                """
UPDATE admin_jobs
SET status='cancelled', finished_at=?, exit_code=NULL, error_message=?
WHERE job_id = ?
                """,
                (finished_at, "cancelled by operator", job_id),
            )
            con.commit()
    return get_job(job_id)


def _run_command(command: List[str], logf: TextIO) -> tuple[int, str | None]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    try:
        proc = subprocess.Popen(
            command,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            logf.write(line)
        proc.wait()
        return int(proc.returncode), None
    except Exception as exc:
        return 1, str(exc)


def _run_native(handler_name: str, params: Dict[str, Any], logf: TextIO) -> tuple[int, str | None]:
    spec = ACTION_SPECS.get(handler_name)
    if spec is None or spec.run_native is None:
        return 1, f"Unknown native handler: {handler_name}"
    try:
        code = int(spec.run_native(params, logf))
        return code, None
    except Exception as exc:
        return 1, str(exc)


def _run_job_worker(
    *,
    job_id: str,
    action: str,
    params_json: str,
    command_json: str,
    log_path: Path,
    actor_user_id: int,
) -> None:
    started_at = _to_iso(_utc_now())
    params: Dict[str, Any] = {}
    try:
        loaded = json.loads(params_json or "{}")
        if isinstance(loaded, dict):
            params = loaded
    except Exception:
        params = {}

    plan = _plan_from_json(command_json)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as logf:
        logf.write(f"[{started_at}] Starting action\n")
        logf.write(f"Action: {action}\n")
        logf.write(f"Params: {json.dumps(params, sort_keys=True)}\n")
        logf.write(f"Execution: {json.dumps(json.loads(command_json), sort_keys=True)}\n\n")
        logf.flush()

        if plan.kind == "command":
            assert plan.argv is not None
            exit_code, error_message = _run_command(plan.argv, logf)
        elif plan.kind == "native":
            assert plan.native_handler is not None
            exit_code, error_message = _run_native(plan.native_handler, params, logf)
        else:
            exit_code, error_message = 1, f"Unsupported execution plan kind: {plan.kind}"

        finished_at = _to_iso(_utc_now())
        status = "succeeded" if exit_code == 0 and not error_message else "failed"
        logf.write(f"\n[{finished_at}] Finished status={status} exit_code={exit_code}\n")
        if error_message:
            logf.write(f"[error] {error_message}\n")
        logf.flush()

    with admin_db.connection_scope() as con:
        con.execute(
            """
UPDATE admin_jobs
SET status=?, finished_at=?, exit_code=?, error_message=?
WHERE job_id=?
            """,
            (status, finished_at, exit_code, error_message, job_id),
        )
        con.execute(
            """
INSERT INTO audit_log(
  actor_user_id, event_type, result, request_id, route, method, details_json, created_at
) VALUES (?, ?, ?, NULL, NULL, NULL, ?, ?)
            """,
            (
                actor_user_id,
                "admin.action.complete",
                "success" if status == "succeeded" else "error",
                json.dumps(
                    {
                        "job_id": job_id,
                        "action": action,
                        "status": status,
                        "exit_code": exit_code,
                        "error_message": error_message,
                        "correlation_id": job_id,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                finished_at,
            ),
        )
        con.commit()

    _start_queued_jobs()


def _row_to_job(row: sqlite3.Row) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    plan_payload: Dict[str, Any] = {}
    try:
        loaded = json.loads(row["params_json"] or "{}")
        if isinstance(loaded, dict):
            params = loaded
    except Exception:
        params = {}
    try:
        loaded_plan = json.loads(row["command_json"] or "{}")
        if isinstance(loaded_plan, dict):
            plan_payload = loaded_plan
        elif isinstance(loaded_plan, list):
            # Legacy compatibility for older rows that only stored argv list.
            plan_payload = {"kind": "command", "argv": loaded_plan}
    except Exception:
        plan_payload = {}

    return {
        "job_id": str(row["job_id"]),
        "action": str(row["action"]),
        "status": str(row["status"]),
        "requested_by_user_id": int(row["requested_by_user_id"]),
        "params": params,
        "execution": plan_payload,
        "log_path": str(row["log_path"]),
        "created_at": str(row["created_at"]),
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "exit_code": row["exit_code"],
        "error_message": row["error_message"],
    }


def get_job(job_id: str) -> Dict[str, Any]:
    with admin_db.connection_scope() as con:
        row = con.execute(
            """
SELECT job_id, action, status, requested_by_user_id, params_json, command_json,
       log_path, created_at, started_at, finished_at, exit_code, error_message
FROM admin_jobs
WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
    if row is None:
        raise KeyError(f"Job not found: {job_id}")
    return _row_to_job(row)


def list_jobs(limit: int = 20) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    with admin_db.connection_scope() as con:
        rows = con.execute(
            """
SELECT job_id, action, status, requested_by_user_id, params_json, command_json,
       log_path, created_at, started_at, finished_at, exit_code, error_message
FROM admin_jobs
ORDER BY created_at DESC
LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_row_to_job(row) for row in rows]


def read_job_log(job_id: str, offset: int = 0, limit: int = 65536) -> Dict[str, Any]:
    job = get_job(job_id)
    log_path = Path(job["log_path"])
    safe_offset = max(0, int(offset))
    safe_limit = max(1024, min(int(limit), 1024 * 1024))
    if not log_path.exists():
        return {
            "job_id": job_id,
            "offset": safe_offset,
            "next_offset": safe_offset,
            "chunk": "",
            "eof": True,
            "status": job["status"],
        }
    size = log_path.stat().st_size
    if safe_offset > size:
        safe_offset = size
    with log_path.open("rb") as f:
        f.seek(safe_offset)
        data = f.read(safe_limit)
    next_offset = safe_offset + len(data)
    eof = next_offset >= size and job["status"] in TERMINAL_STATUSES
    return {
        "job_id": job_id,
        "offset": safe_offset,
        "next_offset": next_offset,
        "chunk": data.decode("utf-8", errors="replace"),
        "eof": eof,
        "status": job["status"],
    }


def read_full_job_log(job_id: str) -> Dict[str, Any]:
    job = get_job(job_id)
    log_path = Path(job["log_path"])
    if not log_path.exists():
        return {"job_id": job_id, "status": job["status"], "chunk": ""}
    content = log_path.read_text(encoding="utf-8", errors="replace")
    return {"job_id": job_id, "status": job["status"], "chunk": content}


def list_backups(limit: int = 100) -> Dict[str, Any]:
    limit = max(1, min(int(limit), 500))
    out: Dict[str, Any] = {
        "admin_db": [],
        "release_metadata": [],
    }

    admin_dir = _ensure_backups_subdir("admin_db")
    admin_files = sorted(admin_dir.glob("*.sqlite3"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in admin_files[:limit]:
        st = path.stat()
        out["admin_db"].append(
            {
                "name": path.name,
                "bytes": st.st_size,
                "mtime_utc": _to_iso(dt.datetime.fromtimestamp(st.st_mtime, tz=dt.timezone.utc)),
            }
        )

    meta_root = _ensure_backups_subdir("release_metadata")
    meta_dirs = sorted(
        [p for p in meta_root.iterdir() if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for path in meta_dirs[:limit]:
        manifest = path / "manifest.json"
        item: Dict[str, Any] = {
            "backup_id": path.name,
            "mtime_utc": _to_iso(dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)),
        }
        if manifest.exists():
            try:
                payload = json.loads(manifest.read_text(encoding="utf-8"))
                item["created_at"] = payload.get("created_at")
                item["current_symlink_target"] = payload.get("current_symlink_target")
            except Exception:
                pass
        out["release_metadata"].append(item)

    return out
