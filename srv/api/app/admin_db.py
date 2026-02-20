from __future__ import annotations

import contextlib
import os
import sqlite3
from pathlib import Path
from typing import Iterator, List


ROOT_DIR = Path(__file__).resolve().parents[3]


def parse_env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def auth_enabled() -> bool:
    return parse_env_bool(os.getenv("SPACEGATE_AUTH_ENABLE"), default=False)


def get_admin_db_path() -> Path:
    raw = os.getenv("SPACEGATE_ADMIN_DB_PATH")
    if raw:
        return Path(raw).expanduser()
    state_raw = os.getenv("SPACEGATE_STATE_DIR") or os.getenv("SPACEGATE_DATA_DIR") or str(ROOT_DIR / "data")
    return Path(state_raw).expanduser() / "admin" / "admin.sqlite3"


def get_admin_db_path_str() -> str:
    return str(get_admin_db_path())


def _parse_csv_env(var_name: str) -> List[str]:
    raw = os.getenv(var_name, "")
    if not raw:
        return []
    values = []
    for part in raw.split(","):
        item = part.strip()
        if item:
            values.append(item)
    return values


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    db_path = get_admin_db_path()
    _ensure_dir(db_path)
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA busy_timeout = 5000")
    return con


@contextlib.contextmanager
def connection_scope() -> Iterator[sqlite3.Connection]:
    con = get_connection()
    try:
        yield con
    finally:
        con.close()


def initialize() -> None:
    with connection_scope() as con:
        con.executescript(
            """
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY AUTOINCREMENT,
  email_norm TEXT UNIQUE NOT NULL,
  display_name TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS auth_identities (
  identity_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  provider TEXT NOT NULL,
  issuer TEXT NOT NULL,
  provider_sub TEXT NOT NULL,
  email_at_login TEXT,
  email_verified INTEGER NOT NULL DEFAULT 0,
  claims_json TEXT,
  created_at TEXT NOT NULL,
  last_login_at TEXT NOT NULL,
  UNIQUE(provider, issuer, provider_sub),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS roles (
  role_id INTEGER PRIMARY KEY AUTOINCREMENT,
  role_code TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS user_roles (
  user_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  PRIMARY KEY (user_id, role_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
  FOREIGN KEY (role_id) REFERENCES roles(role_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS admin_allowlist (
  allow_id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT,
  issuer TEXT,
  provider_sub TEXT,
  email_norm TEXT,
  enabled INTEGER NOT NULL DEFAULT 1,
  note TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (provider_sub IS NOT NULL OR email_norm IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  idle_expires_at TEXT NOT NULL,
  revoked_at TEXT,
  csrf_secret_hash TEXT NOT NULL,
  user_agent_hash TEXT,
  ip_prefix_hash TEXT,
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_log (
  audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor_user_id INTEGER,
  event_type TEXT NOT NULL,
  result TEXT NOT NULL,
  request_id TEXT,
  route TEXT,
  method TEXT,
  details_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (actor_user_id) REFERENCES users(user_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS admin_jobs (
  job_id TEXT PRIMARY KEY,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  requested_by_user_id INTEGER NOT NULL,
  params_json TEXT NOT NULL,
  command_json TEXT NOT NULL,
  log_path TEXT NOT NULL,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  exit_code INTEGER,
  error_message TEXT,
  FOREIGN KEY (requested_by_user_id) REFERENCES users(user_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_allowlist_email ON admin_allowlist(email_norm);
CREATE INDEX IF NOT EXISTS idx_allowlist_sub ON admin_allowlist(provider_sub);
CREATE INDEX IF NOT EXISTS idx_admin_jobs_status ON admin_jobs(status);
CREATE INDEX IF NOT EXISTS idx_admin_jobs_created_at ON admin_jobs(created_at);
            """
        )
        con.execute(
            "INSERT OR IGNORE INTO roles(role_code) VALUES (?)",
            ("admin",),
        )
        con.execute(
            "INSERT OR IGNORE INTO roles(role_code) VALUES (?)",
            ("user",),
        )
        _seed_allowlist_from_env(con)
        con.commit()


def _seed_allowlist_from_env(con: sqlite3.Connection) -> None:
    emails = [item.lower() for item in _parse_csv_env("SPACEGATE_ADMIN_ALLOWLIST_EMAILS")]
    subs = _parse_csv_env("SPACEGATE_ADMIN_ALLOWLIST_SUBS")
    provider = os.getenv("SPACEGATE_OIDC_PROVIDER", "google").strip().lower() or "google"
    issuer = os.getenv("SPACEGATE_OIDC_ISSUER", "https://accounts.google.com").strip() or "https://accounts.google.com"
    now_expr = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"

    for email in emails:
        exists = con.execute(
            """
SELECT 1
FROM admin_allowlist
WHERE provider = ? AND issuer = ? AND provider_sub IS NULL AND email_norm = ?
LIMIT 1
            """,
            (provider, issuer, email),
        ).fetchone()
        if exists is None:
            con.execute(
                f"""
INSERT INTO admin_allowlist(
  provider, issuer, provider_sub, email_norm, enabled, note, created_at, updated_at
) VALUES (?, ?, NULL, ?, 1, ?, {now_expr}, {now_expr})
                """,
                (provider, issuer, email, "seeded-from-env"),
            )
    for provider_sub in subs:
        exists = con.execute(
            """
SELECT 1
FROM admin_allowlist
WHERE provider = ? AND issuer = ? AND provider_sub = ? AND email_norm IS NULL
LIMIT 1
            """,
            (provider, issuer, provider_sub),
        ).fetchone()
        if exists is None:
            con.execute(
                f"""
INSERT INTO admin_allowlist(
  provider, issuer, provider_sub, email_norm, enabled, note, created_at, updated_at
) VALUES (?, ?, ?, NULL, 1, ?, {now_expr}, {now_expr})
                """,
                (provider, issuer, provider_sub, "seeded-from-env"),
            )
