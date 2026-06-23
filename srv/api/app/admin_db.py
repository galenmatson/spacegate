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

CREATE TABLE IF NOT EXISTS inference_endpoints (
  endpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
  endpoint_key TEXT UNIQUE NOT NULL,
  display_name TEXT NOT NULL,
  provider TEXT NOT NULL,
  base_url TEXT NOT NULL,
  auth_mode TEXT NOT NULL DEFAULT 'none',
  api_key_env TEXT,
  api_key_ciphertext TEXT,
  default_model TEXT,
  role_defaults_json TEXT NOT NULL DEFAULT '{}',
  timeout_s INTEGER NOT NULL DEFAULT 30,
  enabled INTEGER NOT NULL DEFAULT 1,
  notes TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS inference_model_cache (
  model_cache_id INTEGER PRIMARY KEY AUTOINCREMENT,
  endpoint_id INTEGER NOT NULL,
  model_id TEXT NOT NULL,
  model_root TEXT,
  max_model_len INTEGER,
  owned_by TEXT,
  raw_json TEXT,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  UNIQUE(endpoint_id, model_id),
  FOREIGN KEY (endpoint_id) REFERENCES inference_endpoints(endpoint_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS inference_endpoint_probes (
  probe_id INTEGER PRIMARY KEY AUTOINCREMENT,
  endpoint_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  model_count INTEGER NOT NULL DEFAULT 0,
  latency_ms INTEGER,
  error_message TEXT,
  probed_at TEXT NOT NULL,
  FOREIGN KEY (endpoint_id) REFERENCES inference_endpoints(endpoint_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS inference_usage_events (
  usage_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  endpoint_id INTEGER,
  model_id TEXT,
  role TEXT,
  request_kind TEXT,
  prompt_tokens INTEGER,
  completion_tokens INTEGER,
  total_tokens INTEGER,
  latency_ms INTEGER,
  success INTEGER NOT NULL DEFAULT 1,
  error_class TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (endpoint_id) REFERENCES inference_endpoints(endpoint_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS agent_object_dossiers (
  dossier_id TEXT PRIMARY KEY,
  stable_object_key TEXT NOT NULL,
  object_type TEXT NOT NULL,
  display_name TEXT,
  dossier_status TEXT NOT NULL DEFAULT 'seeded',
  queue_reason TEXT,
  queue_priority TEXT,
  source_build_id TEXT,
  freshness_state TEXT NOT NULL DEFAULT 'current',
  review_state TEXT NOT NULL DEFAULT 'unreviewed',
  publication_state TEXT NOT NULL DEFAULT 'not_published',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_by_user_id INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  stale_at TEXT,
  published_at TEXT,
  archived_at TEXT,
  FOREIGN KEY (created_by_user_id) REFERENCES users(user_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS agent_source_documents (
  source_document_id TEXT PRIMARY KEY,
  dossier_id TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  allowlist_tier TEXT,
  trust_score REAL,
  title TEXT,
  publisher TEXT,
  published_at TEXT,
  accessed_at TEXT NOT NULL,
  retrieval_status TEXT NOT NULL DEFAULT 'pending',
  content_hash TEXT,
  archive_path TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (dossier_id) REFERENCES agent_object_dossiers(dossier_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS agent_claim_bundles (
  claim_bundle_id TEXT PRIMARY KEY,
  dossier_id TEXT NOT NULL,
  source_document_id TEXT,
  bundle_kind TEXT NOT NULL,
  extraction_method TEXT,
  model_id TEXT,
  endpoint_id INTEGER,
  prompt_version TEXT,
  temperature REAL,
  token_limit INTEGER,
  prompt_hash TEXT,
  bundle_hash TEXT,
  status TEXT NOT NULL DEFAULT 'created',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  FOREIGN KEY (dossier_id) REFERENCES agent_object_dossiers(dossier_id) ON DELETE CASCADE,
  FOREIGN KEY (source_document_id) REFERENCES agent_source_documents(source_document_id) ON DELETE SET NULL,
  FOREIGN KEY (endpoint_id) REFERENCES inference_endpoints(endpoint_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS agent_extracted_claims (
  claim_id TEXT PRIMARY KEY,
  dossier_id TEXT NOT NULL,
  claim_bundle_id TEXT NOT NULL,
  source_document_id TEXT,
  subject_stable_key TEXT,
  subject_label TEXT,
  subject_resolution_mode TEXT NOT NULL DEFAULT 'ambiguous',
  claim_family TEXT NOT NULL,
  predicate TEXT NOT NULL,
  value_json TEXT NOT NULL,
  unit TEXT,
  qualifier TEXT,
  confidence REAL,
  schema_fit TEXT NOT NULL DEFAULT 'schema_gap',
  rigor_tier TEXT NOT NULL DEFAULT 'contextual',
  review_status TEXT NOT NULL DEFAULT 'proposed',
  citation_ids_json TEXT NOT NULL DEFAULT '[]',
  reasoning_summary TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (dossier_id) REFERENCES agent_object_dossiers(dossier_id) ON DELETE CASCADE,
  FOREIGN KEY (claim_bundle_id) REFERENCES agent_claim_bundles(claim_bundle_id) ON DELETE CASCADE,
  FOREIGN KEY (source_document_id) REFERENCES agent_source_documents(source_document_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS agent_portfolio_journal_entries (
  journal_entry_id TEXT PRIMARY KEY,
  dossier_id TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  actor_id TEXT,
  stage TEXT NOT NULL,
  title TEXT NOT NULL,
  narrative TEXT NOT NULL,
  outcome TEXT NOT NULL,
  linked_json TEXT NOT NULL DEFAULT '{}',
  machine_payload_json TEXT NOT NULL DEFAULT '{}',
  model_id TEXT,
  endpoint_id INTEGER,
  prompt_version TEXT,
  token_usage_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  FOREIGN KEY (dossier_id) REFERENCES agent_object_dossiers(dossier_id) ON DELETE CASCADE,
  FOREIGN KEY (endpoint_id) REFERENCES inference_endpoints(endpoint_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_allowlist_email ON admin_allowlist(email_norm);
CREATE INDEX IF NOT EXISTS idx_allowlist_sub ON admin_allowlist(provider_sub);
CREATE INDEX IF NOT EXISTS idx_admin_jobs_status ON admin_jobs(status);
CREATE INDEX IF NOT EXISTS idx_admin_jobs_created_at ON admin_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_inference_endpoints_enabled ON inference_endpoints(enabled);
CREATE INDEX IF NOT EXISTS idx_inference_model_endpoint ON inference_model_cache(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_inference_probes_endpoint ON inference_endpoint_probes(endpoint_id, probed_at);
CREATE INDEX IF NOT EXISTS idx_inference_usage_endpoint_model ON inference_usage_events(endpoint_id, model_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_dossiers_status ON agent_object_dossiers(dossier_status, updated_at);
CREATE INDEX IF NOT EXISTS idx_agent_dossiers_object ON agent_object_dossiers(stable_object_key);
CREATE INDEX IF NOT EXISTS idx_agent_sources_dossier ON agent_source_documents(dossier_id, accessed_at);
CREATE INDEX IF NOT EXISTS idx_agent_sources_domain ON agent_source_documents(source_domain);
CREATE INDEX IF NOT EXISTS idx_agent_bundles_dossier ON agent_claim_bundles(dossier_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_bundles_source ON agent_claim_bundles(source_document_id);
CREATE INDEX IF NOT EXISTS idx_agent_claims_dossier ON agent_extracted_claims(dossier_id, review_status);
CREATE INDEX IF NOT EXISTS idx_agent_claims_subject ON agent_extracted_claims(subject_stable_key);
CREATE INDEX IF NOT EXISTS idx_agent_claims_predicate ON agent_extracted_claims(predicate);
CREATE INDEX IF NOT EXISTS idx_agent_journal_dossier ON agent_portfolio_journal_entries(dossier_id, created_at);
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
        _seed_inference_endpoints_from_env(con)
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


def _seed_inference_endpoints_from_env(con: sqlite3.Connection) -> None:
    now_expr = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"

    def _seed(
        *,
        endpoint_key: str,
        display_name: str,
        provider: str,
        base_url: str,
        auth_mode: str = "none",
        api_key_env: str | None = None,
        default_model: str | None = None,
        notes: str | None = None,
    ) -> None:
        existing = con.execute(
            "SELECT endpoint_id, notes FROM inference_endpoints WHERE endpoint_key = ? LIMIT 1",
            (endpoint_key,),
        ).fetchone()
        if existing is not None:
            note = str(existing["notes"] or "")
            if note.startswith("Seeded from "):
                con.execute(
                    f"""
UPDATE inference_endpoints
SET display_name = ?,
    provider = ?,
    base_url = ?,
    auth_mode = ?,
    api_key_env = ?,
    default_model = ?,
    notes = ?,
    updated_at = {now_expr}
WHERE endpoint_id = ?
                    """,
                    (
                        display_name,
                        provider,
                        base_url,
                        auth_mode,
                        api_key_env,
                        default_model,
                        notes,
                        int(existing["endpoint_id"]),
                    ),
                )
            return
        con.execute(
            f"""
INSERT INTO inference_endpoints(
  endpoint_key, display_name, provider, base_url, auth_mode, api_key_env,
  default_model, role_defaults_json, timeout_s, enabled, notes, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, '{{}}', 30, 1, ?, {now_expr}, {now_expr})
            """,
            (
                endpoint_key,
                display_name,
                provider,
                base_url,
                auth_mode,
                api_key_env,
                default_model,
                notes,
            ),
        )

    llm_base = os.getenv("SPACEGATE_LLM_BASE_URL", "").strip()
    if llm_base:
        _seed(
            endpoint_key="photon-local",
            display_name="Photon Local",
            provider="openai_compatible",
            base_url=llm_base,
            default_model=os.getenv("SPACEGATE_LLM_MODEL", "").strip() or None,
            notes="Seeded from SPACEGATE_LLM_BASE_URL.",
        )

    fallback_base = os.getenv("SPACEGATE_LLM_FALLBACK_BASE_URL", "").strip()
    if fallback_base:
        _seed(
            endpoint_key="positron-fallback",
            display_name="Positron Fallback",
            provider="openai_compatible",
            base_url=fallback_base,
            default_model=os.getenv("SPACEGATE_LLM_FALLBACK_MODEL", "").strip() or None,
            notes="Seeded from SPACEGATE_LLM_FALLBACK_BASE_URL.",
        )

    openai_model = os.getenv("SPACEGATE_FRONTIER_OPENAI_MODEL", "").strip()
    if os.getenv("SPACEGATE_OPENAI_API_KEY", "").strip() or os.getenv("OPENAI_API_KEY", "").strip() or openai_model:
        _seed(
            endpoint_key="openai-frontier",
            display_name="OpenAI Frontier",
            provider="openai",
            base_url=os.getenv("SPACEGATE_OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
            or "https://api.openai.com/v1",
            auth_mode="env",
            api_key_env="SPACEGATE_OPENAI_API_KEY",
            default_model=openai_model or None,
            notes="Seeded from frontier OpenAI environment.",
        )

    google_model = os.getenv("SPACEGATE_FRONTIER_GOOGLE_MODEL", "").strip()
    if os.getenv("SPACEGATE_GOOGLE_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip() or google_model:
        _seed(
            endpoint_key="google-frontier",
            display_name="Google Gemini",
            provider="google",
            base_url=os.getenv("SPACEGATE_GOOGLE_BASE_URL", "https://generativelanguage.googleapis.com/v1beta").strip()
            or "https://generativelanguage.googleapis.com/v1beta",
            auth_mode="env",
            api_key_env="SPACEGATE_GOOGLE_API_KEY",
            default_model=google_model or None,
            notes="Seeded from frontier Google environment.",
        )
