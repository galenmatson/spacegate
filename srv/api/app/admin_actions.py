from __future__ import annotations

import datetime as dt
import json
import os
import re
import secrets
import sqlite3
import subprocess
import threading
from dataclasses import dataclass, field
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
    group_key: str = "operations"
    operator_guidance: Dict[str, Any] = field(default_factory=dict)
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
    if Path("/data/spacegate").exists():
        return Path("/data/spacegate/dl")
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


def _build_command_build_database(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "build_database.sh")]
    if params.get("overwrite", False):
        cmd.append("--overwrite")
    return cmd


def _build_command_build_database_slice(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "build_database_slice.sh")]
    from_cooked = _normalize_boolean(params.get("from_cooked", True))
    if from_cooked:
        cmd.append("--from-cooked")
    else:
        cmd.append("--full-pipeline")
    if _normalize_boolean(params.get("overwrite", False)):
        cmd.append("--overwrite")

    for param_name, flag_name in (
        ("max_distance_ly", "--max-distance-ly"),
        ("min_parallax_over_error", "--min-parallax-over-error"),
        ("max_parallax_error_mas", "--max-parallax-error-mas"),
        ("max_ruwe", "--max-ruwe"),
    ):
        raw = str(params.get(param_name, "") or "").strip()
        if raw:
            cmd.extend([flag_name, raw])

    if _normalize_boolean(params.get("require_spectral_class", False)):
        cmd.append("--require-spectral-class")
    if _normalize_boolean(params.get("require_color_index", False)):
        cmd.append("--require-color-index")
    allowed_spectral = str(params.get("allowed_spectral_classes", "") or "").strip()
    if allowed_spectral:
        cmd.extend(["--allowed-spectral-classes", allowed_spectral])
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


def _build_command_retention_dry_run(params: Dict[str, Any]) -> List[str]:
    cmd = [str(ROOT_DIR / "scripts" / "prune_state_retention.sh")]
    keep_builds = _normalize_integer(params.get("keep_builds", os.getenv("SPACEGATE_RETENTION_KEEP_BUILDS", "12")))
    keep_reports = _normalize_integer(params.get("keep_reports", os.getenv("SPACEGATE_RETENTION_KEEP_REPORTS", "24")))
    if keep_builds < 0:
        raise ActionValidationError("keep_builds must be >= 0")
    if keep_reports < 0:
        raise ActionValidationError("keep_reports must be >= 0")
    cmd.extend(["--keep-builds", str(keep_builds), "--keep-reports", str(keep_reports)])
    if _normalize_boolean(params.get("skip_tmp", False)):
        cmd.append("--no-prune-tmp")
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
    top_coolness = _normalize_integer(params.get("top_coolness", 100))
    if top_coolness <= 0:
        raise ActionValidationError("top_coolness must be > 0")
    cmd.extend(["--top-coolness", str(top_coolness)])
    for param_name, flag_name in (
        ("min_dist_ly", "--min-dist-ly"),
        ("max_dist_ly", "--max-dist-ly"),
        ("min_star_count", "--min-star-count"),
        ("max_star_count", "--max-star-count"),
        ("min_planet_count", "--min-planet-count"),
        ("max_planet_count", "--max-planet-count"),
        ("min_coolness_score", "--min-coolness-score"),
        ("max_coolness_score", "--max-coolness-score"),
    ):
        raw = params.get(param_name)
        if raw is None or str(raw).strip() == "":
            continue
        cmd.extend([flag_name, str(raw)])
    view_type = str(params.get("view_type", "") or "").strip()
    if view_type == "system":
        view_type = "system_card"
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


ACTION_GROUPS: List[Dict[str, Any]] = [
    {
        "key": "build",
        "title": "Build Pipeline",
        "description": "Build, verify, and publish deterministic science artifacts in order.",
        "actions": ["build_database", "verify_build", "publish_db", "retention_dry_run"],
        "sequence": ["Build Database", "Verify Build", "Publish Database", "Retention after verified promotion"],
    },
    {
        "key": "presentation",
        "title": "Presentation Generation",
        "description": "Generate ranking and snapshot artifacts without changing canonical science rows.",
        "actions": ["score_coolness", "save_coolness_profile", "apply_coolness_profile", "generate_snapshots"],
        "sequence": ["Score Coolness", "Save Profile", "Activate Profile", "Generate Snapshots"],
    },
    {
        "key": "recovery",
        "title": "Backups and Recovery",
        "description": "Create rollback points and recover admin or release metadata state.",
        "actions": ["backup_admin_db", "restore_admin_db", "backup_release_metadata", "restore_release_metadata"],
        "sequence": ["Backup First", "Restore Only When Needed", "Verify Auth or Release Metadata"],
    },
    {
        "key": "service",
        "title": "Service Control",
        "description": "Legacy process-runner controls for API/web service state.",
        "actions": ["restart_services", "stop_services"],
        "sequence": ["Confirm Runtime Mode", "Run Action", "Verify Health"],
    },
]


ACTION_OPERATOR_GUIDANCE: Dict[str, Dict[str, Any]] = {
    "build_database": {
        "group_key": "build",
        "purpose": "Runs the full deterministic pipeline: download, cook, ingest, promote, and verify.",
        "prerequisites": "Use when you intentionally want a fresh served build from current source inputs.",
        "writes_to": "$SPACEGATE_STATE_DIR/raw, cooked, out/<build_id>, reports/<build_id>, and served/current through scripts.",
        "outputs": ["core.duckdb", "arm.duckdb", "disc artifacts", "per-build reports", "served/current promotion"],
        "expected_duration": "long",
        "success_next_actions": ["Inspect the job log.", "Review verification report.", "Publish only after verification is clean."],
        "failure_next_actions": ["Inspect final log lines.", "Keep failed artifacts until the root cause is captured.", "Do not run retention while diagnosing."],
        "warnings": ["Long-running and high impact.", "Do not manually edit raw, cooked, out, reports, or served artifacts."],
        "docs_links": ["docs/PROJECT.md", "docs/RETENTION.md"],
    },
    "verify_build": {
        "group_key": "build",
        "purpose": "Checks served/current, or a specific build id, against schema, provenance, and runtime gates.",
        "prerequisites": "Run after build/promotion and before publish, deployment, or cleanup recommendations.",
        "writes_to": "Verification reports under the state report tree.",
        "outputs": ["verification status", "QC/provenance/contract report signals"],
        "expected_duration": "short_to_medium",
        "success_next_actions": ["Publish or continue with deployment/retention decisions."],
        "failure_next_actions": ["Inspect the report and job log before rebuilding or retrying."],
        "warnings": ["Verification failure should block promotion/deployment recommendations."],
        "docs_links": ["docs/PROJECT.md"],
    },
    "publish_db": {
        "group_key": "build",
        "purpose": "Packages the promoted build and updates download metadata for public release artifacts.",
        "prerequisites": "Requires a verified build. Leave build id empty to publish served/current.",
        "writes_to": "Public download metadata and package/report files under the configured dl root.",
        "outputs": ["download metadata", "release package/report artifacts"],
        "expected_duration": "medium",
        "success_next_actions": ["Confirm release metadata.", "Keep or create release metadata backup before later risky changes."],
        "failure_next_actions": ["Use release metadata backup if public download metadata points at the wrong release."],
        "warnings": ["This affects what download clients see; it does not change immutable build contents."],
        "docs_links": ["docs/ADMIN_V2.md"],
    },
    "retention_dry_run": {
        "group_key": "build",
        "purpose": "Runs the retention script in dry-run mode and logs exactly which build/report/tmp paths would be pruned.",
        "prerequisites": "Use only after the served build is verified and temporary outputs have been reviewed.",
        "writes_to": "Admin job log only. It does not delete artifacts because --apply is never passed.",
        "outputs": ["retention candidate list", "estimated reclaimable bytes", "dry-run job log"],
        "expected_duration": "short",
        "success_next_actions": ["Review the job log.", "Only run apply manually after confirming the candidate list is safe."],
        "failure_next_actions": ["Check state directory permissions and the retention script output."],
        "warnings": ["This action is intentionally read-only; apply/delete remains outside Admin v2 for now."],
        "docs_links": ["docs/RETENTION.md"],
    },
    "score_coolness": {
        "group_key": "presentation",
        "purpose": "Generates deterministic disc coolness ranking and scoring reports for a build.",
        "prerequisites": "Use after a valid build exists. Ephemeral scoring is useful for experiments.",
        "writes_to": "Disc scoring artifacts and reports, unless ephemeral mode is selected.",
        "outputs": ["coolness_scores", "coolness_report.json"],
        "expected_duration": "medium",
        "success_next_actions": ["Save a profile if the result is worth preserving.", "Activate deliberately."],
        "failure_next_actions": ["Inspect scoring report/log and verify the target build has required data."],
        "warnings": ["Presentation artifacts must not alter canonical science rows."],
        "docs_links": ["docs/SCHEMA_DISC.md"],
    },
    "save_coolness_profile": {
        "group_key": "presentation",
        "purpose": "Persists an immutable coolness profile version without activating it.",
        "prerequisites": "Use after evaluating weights through preview or scoring jobs.",
        "writes_to": "Coolness profile metadata.",
        "outputs": ["saved profile version"],
        "expected_duration": "short",
        "success_next_actions": ["Activate the saved profile when it should become default presentation policy."],
        "failure_next_actions": ["Check profile id/version and weight JSON validity."],
        "warnings": [],
        "docs_links": ["docs/SCHEMA_DISC.md"],
    },
    "apply_coolness_profile": {
        "group_key": "presentation",
        "purpose": "Activates a saved immutable coolness profile version.",
        "prerequisites": "The profile id and version should already exist and be reviewed.",
        "writes_to": "Active coolness profile selection metadata.",
        "outputs": ["active profile pointer"],
        "expected_duration": "short",
        "success_next_actions": ["Regenerate scores or snapshots if visible presentation should change."],
        "failure_next_actions": ["Confirm the profile exists and has the expected version."],
        "warnings": ["Changes ranking policy used by presentation workflows."],
        "docs_links": ["docs/SCHEMA_DISC.md"],
    },
    "generate_snapshots": {
        "group_key": "presentation",
        "purpose": "Renders snapshot images for filtered coolness-ranked systems.",
        "prerequisites": "Run after scoring when top targets or view parameters changed.",
        "writes_to": "Snapshot assets and manifests in disc/build artifact paths.",
        "outputs": ["snapshot files", "snapshot_manifest rows/artifacts"],
        "expected_duration": "medium_to_long",
        "success_next_actions": ["Reload public search/detail views to confirm new images are referenced correctly."],
        "failure_next_actions": ["Inspect renderer errors and target build coolness availability."],
        "warnings": ["Can create many generated files; keep it scoped when testing."],
        "docs_links": ["docs/SCHEMA_DISC.md"],
    },
    "backup_admin_db": {
        "group_key": "recovery",
        "purpose": "Creates a point-in-time backup of admin auth, sessions, jobs, audit, and registry state.",
        "prerequisites": "Run before restore operations or risky auth/admin changes.",
        "writes_to": "$SPACEGATE_STATE_DIR/admin/backups/admin_db.",
        "outputs": ["admin DB snapshot"],
        "expected_duration": "short",
        "success_next_actions": ["Record the backup filename if it is a pre-change rollback point."],
        "failure_next_actions": ["Check admin DB path and filesystem permissions."],
        "warnings": ["Backup files may contain sensitive admin state; do not commit them."],
        "docs_links": ["docs/ADMIN_AUTH_SPEC.md"],
    },
    "restore_admin_db": {
        "group_key": "recovery",
        "purpose": "Restores admin auth/audit database tables from a named backup file.",
        "prerequisites": "Create a fresh backup first unless the current DB is already known bad.",
        "writes_to": "Admin DB auth, allowlist, sessions, audit, inference registry, and related admin tables.",
        "outputs": ["restored admin DB state"],
        "expected_duration": "short",
        "success_next_actions": ["Verify login, allowlist, audit visibility, and endpoint registry."],
        "failure_next_actions": ["Inspect restore log and preserve current DB for diagnosis."],
        "warnings": ["Can change who can log in and what audit/history is visible."],
        "docs_links": ["docs/ADMIN_AUTH_SPEC.md"],
    },
    "backup_release_metadata": {
        "group_key": "recovery",
        "purpose": "Backs up public release metadata and the current download symlink target.",
        "prerequisites": "Run before publish or before manually repairing download metadata.",
        "writes_to": "$SPACEGATE_STATE_DIR/admin/backups/release_metadata.",
        "outputs": ["release metadata backup manifest"],
        "expected_duration": "short",
        "success_next_actions": ["Use the backup id if download metadata needs rollback."],
        "failure_next_actions": ["Check dl root path and metadata permissions."],
        "warnings": [],
        "docs_links": ["docs/ADMIN_V2.md"],
    },
    "restore_release_metadata": {
        "group_key": "recovery",
        "purpose": "Restores /dl/current.json and optionally the /dl/current symlink from a metadata backup.",
        "prerequisites": "Use when publish/deploy left public download metadata pointing at the wrong release or missing fields.",
        "writes_to": "Download metadata and, when selected, the current download symlink.",
        "outputs": ["restored current.json", "optional restored current symlink"],
        "expected_duration": "short",
        "success_next_actions": ["Verify public download status and release metadata."],
        "failure_next_actions": ["Inspect release metadata backup manifest and dl root state."],
        "warnings": ["Does not rebuild science artifacts or change served/current; it repairs what release/download clients see."],
        "docs_links": ["docs/ADMIN_V2.md"],
    },
    "restart_services": {
        "group_key": "service",
        "purpose": "Restarts API/web processes tracked by the legacy service runner.",
        "prerequisites": "Use for local process-runner mode, not Docker compose deployments unless the host is configured that way.",
        "writes_to": "Runtime process state and logs.",
        "outputs": ["restarted local service processes"],
        "expected_duration": "short",
        "success_next_actions": ["Verify API/web health after restart."],
        "failure_next_actions": ["Use host shell/system service logs if Admin becomes unavailable."],
        "warnings": ["The Admin UI may briefly disconnect."],
        "docs_links": ["docs/ADMIN_V2.md"],
    },
    "stop_services": {
        "group_key": "service",
        "purpose": "Stops API/web processes tracked by the legacy service runner.",
        "prerequisites": "Use only when intentionally taking those services down.",
        "writes_to": "Runtime process state.",
        "outputs": ["stopped local service processes"],
        "expected_duration": "short",
        "success_next_actions": ["Start services from the host if Admin becomes unavailable."],
        "failure_next_actions": ["Use host shell/system service controls."],
        "warnings": ["The Admin UI may disconnect immediately."],
        "docs_links": ["docs/ADMIN_V2.md"],
    },
}


ACTION_SPECS: Dict[str, ActionSpec] = {
    "build_database": ActionSpec(
        name="build_database",
        display_name="Build Database",
        description="Run full database pipeline: download, cook, canonical ingest, promote, verify.",
        params_schema={
            "overwrite": {
                "type": "boolean",
                "default": False,
                "label": "Overwrite cached inputs",
            }
        },
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("build_database"),
        build_command=_build_command_build_database,
    ),
    "build_database_slice": ActionSpec(
        name="build_database_slice",
        display_name="Build Sliced Database",
        description="Apply dataset slice policy filters and rebuild/publish a trimmed core build.",
        hidden=True,
        params_schema={
            "from_cooked": {
                "type": "boolean",
                "default": True,
                "label": "Reuse cooked catalogs (skip download/cook)",
            },
            "overwrite": {
                "type": "boolean",
                "default": False,
                "label": "Overwrite cached inputs (full pipeline only)",
            },
            "max_distance_ly": {
                "type": "string",
                "required": False,
                "default": "1000",
                "allow_empty": True,
                "label": "Max distance ly (optional)",
            },
            "min_parallax_over_error": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Min parallax_over_error (optional)",
            },
            "max_parallax_error_mas": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Max parallax error mas (optional)",
            },
            "max_ruwe": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Max RUWE (optional)",
            },
            "require_spectral_class": {
                "type": "boolean",
                "default": False,
                "label": "Require spectral class",
            },
            "require_color_index": {
                "type": "boolean",
                "default": False,
                "label": "Require color index",
            },
            "allowed_spectral_classes": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Allowed spectral classes CSV (optional)",
            },
        },
        risk_level="high",
        requires_confirmation=True,
        confirmation_phrase=_confirmation_for("build_database_slice"),
        build_command=_build_command_build_database_slice,
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
    "retention_dry_run": ActionSpec(
        name="retention_dry_run",
        display_name="Retention Dry Run",
        description="Preview stale build/report/tmp paths that the retention script would prune.",
        params_schema={
            "keep_builds": {
                "type": "integer",
                "required": False,
                "default": 12,
                "min": 0,
                "max": 200,
                "label": "Keep newest build dirs",
            },
            "keep_reports": {
                "type": "integer",
                "required": False,
                "default": 24,
                "min": 0,
                "max": 500,
                "label": "Keep newest report dirs",
            },
            "skip_tmp": {
                "type": "boolean",
                "required": False,
                "default": False,
                "label": "Do not include out/*.tmp",
            },
        },
        risk_level="low",
        build_command=_build_command_retention_dry_run,
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
        description="Generate disc coolness ranking + report for a build (supports ephemeral scoring).",
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
        description="Render system snapshot images for filtered top coolness-ranked systems (defaults to top 100).",
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
                "default": 100,
                "min": 1,
                "max": 10000,
                "label": "Top coolness systems",
            },
            "min_dist_ly": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Min distance ly (optional)",
            },
            "max_dist_ly": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Max distance ly (optional)",
            },
            "min_star_count": {
                "type": "integer",
                "required": False,
                "default": "",
                "label": "Min stars (optional)",
            },
            "max_star_count": {
                "type": "integer",
                "required": False,
                "default": "",
                "label": "Max stars (optional)",
            },
            "min_planet_count": {
                "type": "integer",
                "required": False,
                "default": "",
                "label": "Min planets (optional)",
            },
            "max_planet_count": {
                "type": "integer",
                "required": False,
                "default": "",
                "label": "Max planets (optional)",
            },
            "min_coolness_score": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Min coolness score (optional)",
            },
            "max_coolness_score": {
                "type": "string",
                "required": False,
                "default": "",
                "allow_empty": True,
                "label": "Max coolness score (optional)",
            },
            "view_type": {
                "type": "string",
                "required": False,
                "default": "system_card",
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
        guidance = dict(spec.operator_guidance or ACTION_OPERATOR_GUIDANCE.get(spec.name, {}))
        group_key = str(guidance.get("group_key") or spec.group_key or spec.category or "operations")
        data.append(
            {
                "name": spec.name,
                "display_name": spec.display_name or spec.name,
                "description": spec.description,
                "params_schema": spec.params_schema,
                "category": spec.category,
                "group_key": group_key,
                "risk_level": spec.risk_level,
                "required_roles": list(spec.required_roles),
                "requires_confirmation": bool(spec.requires_confirmation),
                "confirmation_phrase": spec.confirmation_phrase or "",
                "operator_guidance": guidance,
            }
        )
    return data


def action_groups() -> List[Dict[str, Any]]:
    return [dict(group) for group in ACTION_GROUPS]


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

    if spec.name in {"verify_build", "publish_db", "build_database", "score_coolness", "generate_snapshots"}:
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


def _path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file() or path.is_symlink():
        try:
            return int(path.stat().st_size)
        except OSError:
            return 0
    total = 0
    for child in path.rglob("*"):
        try:
            if child.is_file() or child.is_symlink():
                total += int(child.stat().st_size)
        except OSError:
            continue
    return total


def _path_mtime_iso(path: Path) -> str | None:
    try:
        return _to_iso(dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc))
    except OSError:
        return None


def _served_current_target(state_dir: Path) -> Dict[str, Any]:
    current = state_dir / "served" / "current"
    out: Dict[str, Any] = {
        "path": str(current),
        "exists": current.exists() or current.is_symlink(),
        "is_symlink": current.is_symlink(),
        "target": None,
        "build_id": None,
    }
    try:
        target = current.resolve()
        out["target"] = str(target)
        if target.name and target.parent.name == "out":
            out["build_id"] = target.name
    except OSError:
        pass
    return out


def _report_file_summary(reports_dir: Path) -> Dict[str, Any]:
    if not reports_dir.exists() or not reports_dir.is_dir():
        return {"exists": False, "count": 0, "latest_mtime_utc": None, "files": []}
    files = sorted([p for p in reports_dir.glob("*.json") if p.is_file()], key=lambda p: p.name)
    latest = None
    for path in files:
        mtime = _path_mtime_iso(path)
        if mtime and (latest is None or mtime > latest):
            latest = mtime
    return {
        "exists": True,
        "count": len(files),
        "latest_mtime_utc": latest,
        "files": [p.name for p in files[:80]],
    }


def _read_report_json(path: Path) -> tuple[Dict[str, Any] | None, str | None]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, str(exc)
    if not isinstance(data, dict):
        return None, "report root is not a JSON object"
    return data, None


def _report_presence(reports_dir: Path, names: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name in names:
        path = reports_dir / name
        out.append(
            {
                "name": name,
                "exists": path.exists() and path.is_file(),
                "mtime_utc": _path_mtime_iso(path) if path.exists() else None,
            }
        )
    return out


def _build_verification_summary(reports_dir: Path, build_id: str) -> Dict[str, Any]:
    required_names = ("qc_report.json", "match_report.json", "provenance_report.json")
    supplemental_names = (
        "duplicate_trap_report.json",
        "determinism_report.json",
        "planet_catalog_delta_report.json",
        "planet_reclassification_report.json",
        "classification_safety_report.json",
    )
    required = _report_presence(reports_dir, required_names)
    supplemental = _report_presence(reports_dir, supplemental_names)
    missing_required = [item["name"] for item in required if not item["exists"]]
    issues: List[str] = []
    warnings: List[str] = []
    checks: Dict[str, Any] = {}

    if not reports_dir.exists() or not reports_dir.is_dir():
        issues.append("Report directory is missing.")
    if missing_required:
        issues.append(f"Missing required verify reports: {', '.join(missing_required)}.")

    qc_path = reports_dir / "qc_report.json"
    qc: Dict[str, Any] | None = None
    if qc_path.exists():
        qc, error = _read_report_json(qc_path)
        if error:
            issues.append(f"qc_report.json could not be parsed: {error}")
        elif qc is not None:
            checks["qc_build_id"] = qc.get("build_id")
            checks["qc_build_id_matches"] = qc.get("build_id") == build_id
            if qc.get("build_id") != build_id:
                issues.append(f"QC build_id mismatch: {qc.get('build_id')} != {build_id}.")
            violations = qc.get("dist_invariant_violations")
            checks["dist_invariant_violations"] = violations
            if violations is None:
                issues.append("QC report is missing dist_invariant_violations.")
            elif int(violations or 0) != 0:
                issues.append(f"Distance invariant violations: {violations}.")
            counts = qc.get("counts") if isinstance(qc.get("counts"), dict) else {}
            checks["counts"] = {key: counts.get(key) for key in ("systems", "stars", "planets")}
            if not all(int(counts.get(key) or 0) > 0 for key in ("systems", "stars", "planets")):
                issues.append("QC counts for systems, stars, or planets are zero/missing.")
            tap_keys = (
                "gaia_backbone_row_count_check_match",
                "gaia_classprob_row_count_check_match",
                "gaia_nss_non_single_row_count_check_match",
                "gaia_nss_two_body_row_count_check_match",
            )
            failed_tap = [key for key in tap_keys if qc.get(key) is False]
            checks["gaia_row_count_checks_failed"] = failed_tap
            if failed_tap:
                issues.append(f"Gaia TAP row-count checks failed: {', '.join(failed_tap)}.")
            lifecycle_enabled = bool(qc.get("exoplanet_lifecycle_catalogs_enabled"))
            checks["exoplanet_lifecycle_catalogs_enabled"] = lifecycle_enabled
            if lifecycle_enabled:
                for name in ("planet_catalog_delta_report.json", "planet_reclassification_report.json"):
                    if not (reports_dir / name).exists():
                        issues.append(f"Missing lifecycle report required by QC: {name}.")

    prov_path = reports_dir / "provenance_report.json"
    if prov_path.exists():
        prov, error = _read_report_json(prov_path)
        if error:
            issues.append(f"provenance_report.json could not be parsed: {error}")
        elif prov is not None:
            checks["provenance_build_id"] = prov.get("build_id")
            checks["provenance_build_id_matches"] = prov.get("build_id") == build_id
            checks["provenance_table_count"] = len(prov.get("tables") or {}) if isinstance(prov.get("tables"), dict) else 0
            if prov.get("build_id") and prov.get("build_id") != build_id:
                issues.append(f"Provenance build_id mismatch: {prov.get('build_id')} != {build_id}.")

    match_path = reports_dir / "match_report.json"
    if match_path.exists():
        match, error = _read_report_json(match_path)
        if error:
            issues.append(f"match_report.json could not be parsed: {error}")
        elif match is not None:
            checks["match_build_id"] = match.get("build_id")
            checks["match_build_id_matches"] = match.get("build_id") == build_id
            if match.get("build_id") and match.get("build_id") != build_id:
                issues.append(f"Match report build_id mismatch: {match.get('build_id')} != {build_id}.")

    duplicate_path = reports_dir / "duplicate_trap_report.json"
    if duplicate_path.exists():
        duplicate, error = _read_report_json(duplicate_path)
        if error:
            warnings.append(f"duplicate_trap_report.json could not be parsed: {error}")
        elif duplicate is not None:
            near = duplicate.get("near_pair_totals") if isinstance(duplicate.get("near_pair_totals"), dict) else {}
            checks["duplicate_near_pair_totals"] = {
                "candidate_pairs": near.get("candidate_pairs"),
                "likely_duplicate_pairs": near.get("likely_duplicate_pairs"),
                "high_confidence_pairs": near.get("high_confidence_pairs"),
            }
            if duplicate.get("build_id") and duplicate.get("build_id") != build_id:
                warnings.append(f"Duplicate trap build_id mismatch: {duplicate.get('build_id')} != {build_id}.")
    else:
        warnings.append("duplicate_trap_report.json is absent; default verification may allow this, strict verification may not.")

    if issues:
        status = "failed"
    elif missing_required:
        status = "missing_reports"
    elif warnings:
        status = "attention"
    elif all(item["exists"] for item in required):
        status = "passed_reports"
    else:
        status = "unknown"

    return {
        "status": status,
        "required_reports": required,
        "supplemental_reports": supplemental,
        "missing_required_reports": missing_required,
        "issues": issues,
        "warnings": warnings,
        "checks": checks,
    }


def _snapshot_report_summary(reports_dir: Path) -> Dict[str, Any]:
    path = reports_dir / "snapshot_report.json"
    out: Dict[str, Any] = {
        "has_report": path.exists() and path.is_file(),
        "status": "missing",
        "path": str(path),
        "generated_at": None,
        "generator_version": None,
        "view_type": None,
        "force": None,
        "params_hash": None,
        "requested": None,
        "generated": None,
        "reused": None,
        "manifest_rows_upserted": None,
        "manifest_parquet": None,
        "null_result": False,
        "parse_error": None,
    }
    if not out["has_report"]:
        return out
    data, error = _read_report_json(path)
    if error or data is None:
        out["status"] = "parse_error"
        out["parse_error"] = error
        return out
    for key in (
        "generated_at",
        "generator_version",
        "view_type",
        "force",
        "params_hash",
        "requested",
        "generated",
        "reused",
        "manifest_rows_upserted",
        "manifest_parquet",
    ):
        out[key] = data.get(key)
    requested = int(data.get("requested") or 0)
    generated = int(data.get("generated") or 0)
    reused = int(data.get("reused") or 0)
    upserted = int(data.get("manifest_rows_upserted") or 0)
    out["null_result"] = requested == 0 and generated == 0 and reused == 0 and upserted == 0
    if out["null_result"]:
        out["status"] = "null_result"
    elif generated > 0:
        out["status"] = "generated"
    elif reused > 0:
        out["status"] = "reused"
    elif requested == 0:
        out["status"] = "completed_zero_requested"
    else:
        out["status"] = "completed_zero_generated"
    return out


def _coolness_report_summary(reports_dir: Path) -> Dict[str, Any]:
    path = reports_dir / "coolness_report.json"
    out: Dict[str, Any] = {"has_report": path.exists() and path.is_file(), "status": "missing", "parse_error": None}
    if not out["has_report"]:
        return out
    data, error = _read_report_json(path)
    if error or data is None:
        out["status"] = "parse_error"
        out["parse_error"] = error
        return out
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
    out.update(
        {
            "status": "present",
            "profile_id": profile.get("profile_id") or data.get("profile_id"),
            "profile_version": profile.get("profile_version") or data.get("profile_version"),
            "scored_rows": summary.get("scored_rows") or summary.get("rows") or data.get("scored_rows"),
        }
    )
    return out


def _path_health(path: Path, *, label: str, required: bool = True, require_writable: bool = False) -> Dict[str, Any]:
    exists = path.exists() or path.is_symlink()
    is_dir = path.is_dir()
    readable = bool(exists and os.access(path, os.R_OK))
    traversable = bool(exists and (not is_dir or os.access(path, os.X_OK)))
    writable = bool(exists and os.access(path, os.W_OK))
    issues: List[str] = []
    if required and not exists:
        issues.append("missing")
    if exists and not readable:
        issues.append("not readable")
    if exists and is_dir and not traversable:
        issues.append("not traversable")
    if require_writable and exists and not writable:
        issues.append("not writable")
    if issues:
        status = "error" if required or require_writable else "warning"
    else:
        status = "ok" if exists else "optional_missing"
    return {
        "label": label,
        "path": str(path),
        "exists": exists,
        "is_dir": is_dir,
        "is_symlink": path.is_symlink(),
        "readable": readable,
        "traversable": traversable,
        "writable": writable,
        "required": required,
        "require_writable": require_writable,
        "status": status,
        "issues": issues,
        "mtime_utc": _path_mtime_iso(path) if exists else None,
    }


def _build_artifact_summary(state_dir: Path, build_dir: Path) -> Dict[str, Any]:
    build_id = build_dir.name
    reports_dir = state_dir / "reports" / build_id
    core_db = build_dir / "core.duckdb"
    arm_db = build_dir / "arm.duckdb"
    disc_db = build_dir / "disc.duckdb"
    halo_db = build_dir / "halo.duckdb"
    galaxy_db = build_dir / "galaxy.duckdb"
    parquet_dir = build_dir / "parquet"
    reports = _report_file_summary(reports_dir)
    missing_required = []
    if not core_db.exists():
        missing_required.append("core.duckdb")
    if not arm_db.exists():
        missing_required.append("arm.duckdb")
    parquet_required = {
        "stars": parquet_dir / "stars.parquet",
        "systems": parquet_dir / "systems.parquet",
        "planets": parquet_dir / "planets.parquet",
    }
    missing_parquet = [f"parquet/{name}.parquet" for name, path in parquet_required.items() if not path.exists()]
    return {
        "build_id": build_id,
        "path": str(build_dir),
        "mtime_utc": _path_mtime_iso(build_dir),
        "size_bytes": _path_size_bytes(build_dir),
        "reports_dir": str(reports_dir),
        "reports": reports,
        "artifacts": {
            "core_db": core_db.exists(),
            "arm_db": arm_db.exists(),
            "disc_db": disc_db.exists(),
            "halo_db": halo_db.exists(),
            "galaxy_db": galaxy_db.exists(),
            "parquet": parquet_dir.exists(),
            "stars_parquet": parquet_required["stars"].exists(),
            "systems_parquet": parquet_required["systems"].exists(),
            "planets_parquet": parquet_required["planets"].exists(),
        },
        "artifact_sizes_bytes": {
            "core_db": core_db.stat().st_size if core_db.exists() else 0,
            "arm_db": arm_db.stat().st_size if arm_db.exists() else 0,
            "disc_db": disc_db.stat().st_size if disc_db.exists() else 0,
            "halo_db": halo_db.stat().st_size if halo_db.exists() else 0,
            "galaxy_db": galaxy_db.stat().st_size if galaxy_db.exists() else 0,
            "parquet": _path_size_bytes(parquet_dir),
        },
        "promotable": not missing_required and not missing_parquet,
        "missing_required": missing_required + missing_parquet,
        "missing_artifacts": missing_required,
        "missing_parquet": missing_parquet,
        "verification": _build_verification_summary(reports_dir, build_id),
        "snapshot": _snapshot_report_summary(reports_dir),
        "coolness": _coolness_report_summary(reports_dir),
    }


def _recent_builds(state_dir: Path, limit: int = 12) -> List[Dict[str, Any]]:
    out_dir = state_dir / "out"
    if not out_dir.exists() or not out_dir.is_dir():
        return []
    candidates = [
        path for path in out_dir.iterdir()
        if path.is_dir() and not path.name.endswith(".tmp")
    ]
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return [_build_artifact_summary(state_dir, path) for path in candidates[: max(1, limit)]]


def _tmp_builds(state_dir: Path, limit: int = 20) -> List[Dict[str, Any]]:
    out_dir = state_dir / "out"
    if not out_dir.exists() or not out_dir.is_dir():
        return []
    candidates = [
        path for path in out_dir.iterdir()
        if path.is_dir() and path.name.endswith(".tmp")
    ]
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    items = []
    for path in candidates[: max(1, limit)]:
        items.append(
            {
                "name": path.name,
                "build_id": path.name[:-4],
                "path": str(path),
                "mtime_utc": _path_mtime_iso(path),
                "size_bytes": _path_size_bytes(path),
            }
        )
    return items


def _is_retention_build_dir_name(name: str) -> bool:
    return bool(re.match(r"^(\d{4}-\d{2}-\d{2}|\d{8})T\d{6}Z_[A-Za-z0-9._-]+$", name or ""))


def _retention_candidate(path: Path, reason: str) -> Dict[str, Any]:
    return {
        "name": path.name,
        "path": str(path),
        "reason": reason,
        "mtime_utc": _path_mtime_iso(path),
        "size_bytes": _path_size_bytes(path),
    }


def _retention_plan(
    state_dir: Path,
    *,
    keep_builds: int,
    keep_reports: int,
    prune_tmp: bool = True,
) -> Dict[str, Any]:
    out_dir = state_dir / "out"
    reports_dir = state_dir / "reports"
    served = _served_current_target(state_dir)
    served_build_id = str(served.get("build_id") or "")

    out_names = sorted(
        [path.name for path in out_dir.iterdir() if path.is_dir()],
        reverse=True,
    ) if out_dir.exists() and out_dir.is_dir() else []
    report_names = sorted(
        [path.name for path in reports_dir.iterdir() if path.is_dir()],
        reverse=True,
    ) if reports_dir.exists() and reports_dir.is_dir() else []

    build_names = [name for name in out_names if not name.endswith(".tmp") and _is_retention_build_dir_name(name)]
    tmp_names = [name for name in out_names if name.endswith(".tmp")]
    report_build_names = [name for name in report_names if _is_retention_build_dir_name(name)]

    keep_build_names = set(build_names[: max(0, keep_builds)])
    keep_report_names = set(report_build_names[: max(0, keep_reports)])
    if served_build_id:
        keep_build_names.add(served_build_id)
        keep_report_names.add(served_build_id)

    build_candidates = [
        _retention_candidate(out_dir / name, f"older than newest {keep_builds} build dirs and not served/current")
        for name in build_names
        if name not in keep_build_names
    ]
    tmp_candidates = [
        _retention_candidate(out_dir / name, "temporary ingest output")
        for name in tmp_names
    ] if prune_tmp else []
    report_candidates = [
        _retention_candidate(reports_dir / name, f"older than newest {keep_reports} report dirs and not served/current")
        for name in report_build_names
        if name not in keep_report_names
    ]
    total_bytes = sum(int(item.get("size_bytes") or 0) for item in build_candidates + tmp_candidates + report_candidates)
    return {
        "mode": "dry_run",
        "script": "scripts/prune_state_retention.sh",
        "keep_builds": keep_builds,
        "keep_reports": keep_reports,
        "prune_tmp": prune_tmp,
        "served_build_id": served_build_id or None,
        "build_dir_count": len(build_names),
        "report_dir_count": len(report_build_names),
        "tmp_dir_count": len(tmp_names),
        "kept_build_count": len(keep_build_names.intersection(build_names)),
        "kept_report_count": len(keep_report_names.intersection(report_build_names)),
        "candidates": {
            "builds": build_candidates,
            "tmp": tmp_candidates,
            "reports": report_candidates,
        },
        "candidate_count": len(build_candidates) + len(tmp_candidates) + len(report_candidates),
        "estimated_reclaimable_bytes": total_bytes,
        "notes": [
            "This plan is informational; the Admin action runs the script without --apply.",
            "raw/ and cooked/ are never retention candidates.",
            "served/current is kept even if older than the keep window.",
        ],
    }


def _build_path_health(state_dir: Path) -> Dict[str, Any]:
    return {
        "state_dir": _path_health(state_dir, label="State root", required=True, require_writable=True),
        "raw_dir": _path_health(state_dir / "raw", label="Raw snapshots", required=True, require_writable=False),
        "cooked_dir": _path_health(state_dir / "cooked", label="Cooked exports", required=True, require_writable=False),
        "out_dir": _path_health(state_dir / "out", label="Immutable build outputs", required=True, require_writable=True),
        "reports_dir": _path_health(state_dir / "reports", label="Build reports", required=True, require_writable=True),
        "served_dir": _path_health(state_dir / "served", label="Served metadata", required=True, require_writable=True),
        "served_current": _path_health(state_dir / "served" / "current", label="Served current symlink", required=True, require_writable=False),
    }


def _retention_summary(state_dir: Path, active_build_jobs: List[Dict[str, Any]], tmp_builds: List[Dict[str, Any]]) -> Dict[str, Any]:
    retention_blockers = []
    if active_build_jobs:
        retention_blockers.append("A build, verify, or publish job is active.")
    if tmp_builds:
        retention_blockers.append("Temporary ingest output directories exist; inspect them before pruning.")
    keep_builds = int(os.getenv("SPACEGATE_RETENTION_KEEP_BUILDS", "12"))
    keep_reports = int(os.getenv("SPACEGATE_RETENTION_KEEP_REPORTS", "24"))
    return {
        "default_keep_builds": keep_builds,
        "default_keep_reports": keep_reports,
        "script": "scripts/prune_state_retention.sh",
        "dry_run_available": not active_build_jobs,
        "can_run_now": not retention_blockers,
        "blocked_reasons": retention_blockers,
        "dry_run": _retention_plan(state_dir, keep_builds=keep_builds, keep_reports=keep_reports, prune_tmp=True),
        "notes": [
            "Retention must not prune raw/ or cooked/.",
            "Run retention only after successful promotion and verification.",
            "Use dry-run first unless the cleanup target is already reviewed.",
        ],
    }


def _build_next_actions(
    *,
    served: Dict[str, Any],
    current_build: Dict[str, Any] | None,
    recent_builds: List[Dict[str, Any]],
    tmp_builds: List[Dict[str, Any]],
    active_build_jobs: List[Dict[str, Any]],
    path_health: Dict[str, Any],
    retention: Dict[str, Any],
) -> List[Dict[str, str]]:
    actions: List[Dict[str, str]] = []
    path_errors = [item for item in path_health.values() if item.get("status") == "error"]
    if path_errors:
        actions.append(
            {
                "priority": "high",
                "title": "Fix filesystem targets before running builds",
                "detail": f"{len(path_errors)} configured build path(s) are missing or inaccessible.",
                "action": "Open Runtime or inspect the path health table below.",
            }
        )
    if active_build_jobs:
        job = active_build_jobs[0]
        actions.append(
            {
                "priority": "normal",
                "title": "Wait for active build job",
                "detail": f"{job.get('action') or 'build action'} is {job.get('status') or 'active'} as {job.get('job_id') or 'a job'}.",
                "action": "Open Operations to monitor logs before starting another build/publish action.",
            }
        )
    if not served.get("build_id"):
        actions.append(
            {
                "priority": "high",
                "title": "Promote a verified build",
                "detail": "served/current does not resolve to an out/<build_id> directory.",
                "action": "Run Build Database if no good build exists, then Verify Build and Publish Database.",
            }
        )
    elif current_build:
        verification = current_build.get("verification") or {}
        snapshot = current_build.get("snapshot") or {}
        if not current_build.get("promotable"):
            actions.append(
                {
                    "priority": "high",
                    "title": "Investigate missing served artifacts",
                    "detail": f"Missing: {', '.join(current_build.get('missing_required') or ['required artifacts'])}.",
                    "action": "Rebuild or restore before publishing/deploying.",
                }
            )
        if verification.get("status") not in {"passed_reports", "attention"}:
            actions.append(
                {
                    "priority": "high",
                    "title": "Run or inspect Verify Build",
                    "detail": f"Verification summary is {verification.get('status') or 'unknown'}.",
                    "action": "Run Verify Build from the runbook, then inspect any failed report gates.",
                }
            )
        elif verification.get("warnings"):
            actions.append(
                {
                    "priority": "normal",
                    "title": "Review verification warnings",
                    "detail": "; ".join((verification.get("warnings") or [])[:2]),
                    "action": "Warnings may be acceptable, but should be noted before retention or deployment.",
                }
            )
        if not snapshot.get("has_report"):
            actions.append(
                {
                    "priority": "normal",
                    "title": "Generate or record snapshot state",
                    "detail": "No snapshot_report.json is present for the served build.",
                    "action": "Generate snapshots when visual/card artifacts are expected for this build.",
                }
            )
        elif snapshot.get("null_result"):
            actions.append(
                {
                    "priority": "normal",
                    "title": "Record snapshot null result",
                    "detail": "Snapshot generation completed with zero requested/generated/reused rows.",
                    "action": "Treat as a scientific null result unless the selection parameters were unintended.",
                }
            )
    elif recent_builds:
        actions.append(
            {
                "priority": "normal",
                "title": "Select the served build for inspection",
                "detail": "Recent builds exist, but the served build was not found in the recent window.",
                "action": "Increase inspection depth or check served/current manually.",
            }
        )
    else:
        actions.append(
            {
                "priority": "normal",
                "title": "Create first build artifact",
                "detail": "No immutable build directories were found under out/.",
                "action": "Run Build Database, then Verify Build.",
            }
        )
    if tmp_builds:
        actions.append(
            {
                "priority": "normal",
                "title": "Review temporary build outputs",
                "detail": f"{len(tmp_builds)} temporary output director{'y' if len(tmp_builds) == 1 else 'ies'} exist.",
                "action": "Capture the failure cause before pruning .tmp outputs.",
            }
        )
    elif retention.get("can_run_now") and current_build and (current_build.get("verification") or {}).get("status") in {"passed_reports", "attention"}:
        actions.append(
            {
                "priority": "low",
                "title": "Retention dry-run is available",
                "detail": "No active build blockers or temporary outputs are reported.",
                "action": "Run scripts/prune_state_retention.sh without --apply first.",
            }
        )
    return actions[:8]


def _build_related_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    build_related_actions = {"build_database", "build_database_slice", "verify_build", "publish_db", "retention_dry_run"}
    return [
        job for job in jobs
        if job.get("status") in RUNNING_STATUSES and job.get("action") in build_related_actions
    ]


def builds_status() -> Dict[str, Any]:
    state_dir = _state_dir().resolve()
    jobs_error = None
    try:
        jobs = list_jobs(limit=100)
    except Exception as exc:
        jobs = []
        jobs_error = str(exc)
    active_build_jobs = _build_related_jobs(jobs)
    served = _served_current_target(state_dir)
    recent_builds = _recent_builds(state_dir, limit=16)
    tmp_builds = _tmp_builds(state_dir, limit=20)
    incomplete_builds = [item for item in recent_builds if not item.get("promotable")]
    current_build = next((item for item in recent_builds if item.get("build_id") == served.get("build_id")), None)
    path_health = _build_path_health(state_dir)
    retention = _retention_summary(state_dir, active_build_jobs, tmp_builds)
    out_dir = state_dir / "out"
    reports_dir = state_dir / "reports"
    next_actions = _build_next_actions(
        served=served,
        current_build=current_build,
        recent_builds=recent_builds,
        tmp_builds=tmp_builds,
        active_build_jobs=active_build_jobs,
        path_health=path_health,
        retention=retention,
    )
    if jobs_error:
        next_actions.insert(
            0,
            {
                "priority": "normal",
                "title": "Job state unavailable",
                "detail": f"Build status loaded, but job history could not be read: {jobs_error}",
                "action": "Check admin DB permissions or use the Operations workspace after the API runtime can read the DB.",
            },
        )
    return {
        "status": "ok",
        "generated_at_utc": _to_iso(_utc_now()),
        "paths": {
            "state_dir": str(state_dir),
            "raw_dir": str(state_dir / "raw"),
            "cooked_dir": str(state_dir / "cooked"),
            "out_dir": str(out_dir),
            "reports_dir": str(reports_dir),
            "served_current": str(state_dir / "served" / "current"),
        },
        "path_health": path_health,
        "served_current": served,
        "current_build": current_build,
        "recent": recent_builds,
        "tmp": tmp_builds,
        "incomplete_recent": incomplete_builds,
        "out_count": len([p for p in out_dir.iterdir() if p.is_dir() and not p.name.endswith(".tmp")]) if out_dir.exists() else 0,
        "report_build_count": len([p for p in reports_dir.iterdir() if p.is_dir()]) if reports_dir.exists() else 0,
        "tmp_count": len(tmp_builds),
        "active_build_jobs": active_build_jobs,
        "jobs_error": jobs_error,
        "retention": retention,
        "next_actions": next_actions[:8],
    }


def operations_status() -> Dict[str, Any]:
    state_dir = _state_dir().resolve()
    jobs_dir = _jobs_dir().resolve()
    backups_dir = _backups_dir().resolve()
    dl_root = _dl_root().resolve()
    backups = list_backups(limit=100)
    jobs = list_jobs(limit=100)
    active_jobs = [job for job in jobs if job.get("status") in RUNNING_STATUSES]
    running_jobs = [job for job in jobs if job.get("status") == "running"]
    queued_jobs = [job for job in jobs if job.get("status") == "queued"]
    failed_jobs = [job for job in jobs if job.get("status") == "failed"]
    active_build_jobs = _build_related_jobs(jobs)
    latest_high_risk = None
    for job in jobs:
        spec = ACTION_SPECS.get(str(job.get("action") or ""))
        if spec and spec.risk_level == "high":
            latest_high_risk = job
            break
    admin_backups = backups.get("admin_db") or []
    release_backups = backups.get("release_metadata") or []
    recent_builds = _recent_builds(state_dir, limit=12)
    tmp_builds = _tmp_builds(state_dir, limit=20)
    incomplete_builds = [item for item in recent_builds if not item.get("promotable")]
    retention = _retention_summary(state_dir, active_build_jobs, tmp_builds)

    return {
        "status": "ok",
        "generated_at_utc": _to_iso(_utc_now()),
        "paths": {
            "state_dir": str(state_dir),
            "jobs_dir": str(jobs_dir),
            "backups_dir": str(backups_dir),
            "dl_root": str(dl_root),
            "out_dir": str(state_dir / "out"),
            "reports_dir": str(state_dir / "reports"),
            "served_current": str(state_dir / "served" / "current"),
        },
        "runner": {
            "max_running_jobs": _max_concurrent_jobs(),
            "max_queued_jobs": _max_queued_jobs(),
            "running_count": len(running_jobs),
            "queued_count": len(queued_jobs),
            "active_count": len(active_jobs),
            "available_running_slots": max(_max_concurrent_jobs() - len(running_jobs), 0),
        },
        "jobs": {
            "recent": jobs[:20],
            "active": active_jobs,
            "latest_failures": failed_jobs[:8],
            "latest_high_risk": latest_high_risk,
        },
        "backups": {
            "admin_db_count": len(admin_backups),
            "release_metadata_count": len(release_backups),
            "latest_admin_db": admin_backups[0] if admin_backups else None,
            "latest_release_metadata": release_backups[0] if release_backups else None,
        },
        "builds": {
            "served_current": _served_current_target(state_dir),
            "recent": recent_builds,
            "tmp": tmp_builds,
            "incomplete_recent": incomplete_builds,
            "out_count": len([p for p in (state_dir / "out").iterdir() if p.is_dir() and not p.name.endswith(".tmp")]) if (state_dir / "out").exists() else 0,
            "tmp_count": len(tmp_builds),
        },
        "retention": {
            **retention,
        },
        "action_groups": action_groups(),
    }
