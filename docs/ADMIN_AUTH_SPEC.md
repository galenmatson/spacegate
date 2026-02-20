# Spacegate Admin Auth Spec (v0.1.5 Checkpoint A)

Status: Checkpoint A implemented (pending operational rollout)  
Last updated: 2026-02-20

## Scope
This spec defines the first implementation slice of `v0.1.5: Admin Control Plane + Authentication Foundation`:
- secure admin login
- RBAC scaffold (`admin` active, `user` reserved)
- identity/session data model
- server-side admin allowlist enforcement

It does not include admin job execution UI yet (`Checkpoint C`), beyond auth and access-control prerequisites.

## Goals
- Add secure authentication for `/admin` and `/api/v1/admin/*`.
- Keep auth foundation general so later per-user lore features reuse the same identity/session model.
- Keep core astronomy data immutable and separate from auth state.

## Non-goals (Checkpoint A)
- No public user signup.
- No lore edit UI.
- No arbitrary command execution from web.
- No replacement of existing script workflows yet.

## Architecture Decisions
- Identity provider: OIDC, Google-supported first.
- Authorization: server-side RBAC with explicit admin allowlist.
- Storage: separate writable admin DB (SQLite with WAL by default), not `core.duckdb`.
- Session model: server-side sessions (opaque cookie token), not self-contained JWT auth.

## Data Storage
Default path:
- `SPACEGATE_ADMIN_DB_PATH=$SPACEGATE_STATE_DIR/admin/admin.sqlite3`

Rationale:
- Avoid mutating/locking `core.duckdb`.
- Keep auth/audit writable and isolated.

## Data Model (Checkpoint A)
Minimum tables:

1. `users`
- `user_id` (PK)
- `email_norm` (UNIQUE, lowercase)
- `display_name`
- `status` (`active|disabled`)
- `created_at`, `updated_at`, `last_login_at`

2. `auth_identities`
- `identity_id` (PK)
- `user_id` (FK -> users)
- `provider` (e.g., `google`)
- `issuer` (OIDC issuer)
- `provider_sub` (provider subject, UNIQUE with provider+issuer)
- `email_at_login`
- `email_verified` (bool)
- `claims_json` (minimal retained claims)
- `created_at`, `last_login_at`

3. `roles`
- `role_id` (PK)
- `role_code` (UNIQUE; initial: `admin`, `user`)

4. `user_roles`
- `user_id` (FK -> users)
- `role_id` (FK -> roles)
- PK (`user_id`, `role_id`)

5. `admin_allowlist`
- `allow_id` (PK)
- `provider` (nullable; default any)
- `issuer` (nullable; default any)
- `provider_sub` (nullable)
- `email_norm` (nullable)
- `enabled` (bool)
- `note`
- `created_at`, `updated_at`

Rule:
- At least one of `provider_sub` or `email_norm` must be set.
- Server grants admin access only if an enabled allowlist row matches.

6. `sessions`
- `session_id` (PK, random opaque token id)
- `user_id` (FK -> users)
- `created_at`
- `last_seen_at`
- `expires_at` (absolute)
- `idle_expires_at` (idle timeout)
- `revoked_at` (nullable)
- `csrf_secret_hash`
- `user_agent_hash` (optional hardening)
- `ip_prefix_hash` (optional hardening)

7. `audit_log`
- `audit_id` (PK)
- `actor_user_id` (nullable FK -> users)
- `event_type` (e.g., `auth.login.success`, `auth.login.denied`, `auth.logout`)
- `result` (`success|deny|error`)
- `request_id`
- `route`
- `method`
- `details_json`
- `created_at`

## Auth Flow
### Login
1. Client requests `GET /api/v1/auth/login/google`.
2. Server creates OIDC `state` + `nonce`, stores signed transient cookie, redirects to provider.
3. Provider redirects to `GET /api/v1/auth/callback/google?code=...&state=...`.
4. Server validates `state`, exchanges code, validates ID token (`iss`, `aud`, `exp`, `nonce`, `email_verified`).
5. Server checks admin allowlist match.
6. If allowed: upsert user + identity, ensure `admin` role, create session, set secure cookie.
7. Server writes audit event and redirects to `/admin`.

### Logout
- `POST /api/v1/auth/logout`: revoke session row, clear cookie, audit log.

### Session Introspection
- `GET /api/v1/auth/me`: return authenticated identity summary + roles + session expiry.

## API and Route Protection
Protected route groups:
- `/admin/*` (UI assets/pages)
- `/api/v1/admin/*` (admin APIs)

Policy:
- Unauthenticated -> `401`.
- Authenticated but not admin -> `403`.
- Disabled user -> `403`.

Dependency/middleware requirements:
- Auth middleware resolves session from cookie.
- `require_admin` dependency guards admin handlers.
- Request ID must be included in audit entries.

## Security Controls
- Cookies:
  - `HttpOnly`, `Secure`, `SameSite=Lax` (or `Strict` if compatible).
  - Name prefix `__Host-` when TLS and path constraints allow.
- CSRF:
  - Required for mutating endpoints (`POST/PUT/PATCH/DELETE`).
  - Double-submit or synchronizer token tied to server session.
- Session lifetime defaults:
  - `SESSION_TTL_HOURS=12` absolute
  - `SESSION_IDLE_MINUTES=60` idle
- Token handling:
  - Store only session IDs server-side; never expose OIDC tokens to frontend JS.
- Logging:
  - No secrets/tokens in logs.
  - Audit denials and auth failures.

## Configuration
Required env vars (production):
- `SPACEGATE_AUTH_ENABLE=1`
- `SPACEGATE_OIDC_PROVIDER=google`
- `SPACEGATE_OIDC_ISSUER=https://accounts.google.com`
- `SPACEGATE_OIDC_CLIENT_ID=...`
- `SPACEGATE_OIDC_CLIENT_SECRET=...`
- `SPACEGATE_OIDC_REDIRECT_URI=https://spacegates.org/api/v1/auth/callback/google`
- `SPACEGATE_AUTH_SUCCESS_REDIRECT=/api/v1/admin/ui`
- `SPACEGATE_ADMIN_DB_PATH=/data/spacegate/data/admin/admin.sqlite3`
- `SPACEGATE_SESSION_SECRET=...` (high-entropy secret for signing)

Recommended:
- `SPACEGATE_SESSION_TTL_HOURS=12`
- `SPACEGATE_SESSION_IDLE_MINUTES=60`
- `SPACEGATE_CSRF_ENABLE=1`

Admin allowlist source options:
- DB table (`admin_allowlist`) is authoritative.
- Optional bootstrap env/file for first admin seed, then removed.

## Implementation Checkpoints (A)
1. Schema + migration
- Create tables above and seed roles (`admin`, `user`).
- Add migration workflow for admin DB schema changes.

2. OIDC login/logout/me endpoints
- Implement Google OIDC auth code flow.
- Validate ID token claims and enforce allowlist.

3. Session + RBAC middleware
- Implement session cookie parsing and session lifecycle checks.
- Implement `require_admin` dependency.

4. Audit trail for auth events
- Log success/deny/error for login/logout/access checks.

## Acceptance Criteria (Checkpoint A)
- Allowlisted Google account can log in and access `/admin`.
- Non-allowlisted Google account is denied and audited.
- `/api/v1/admin/*` returns `401` when unauthenticated.
- `/api/v1/admin/*` returns `403` for authenticated non-admin user.
- Session expiry/idle timeout are enforced.
- CSRF validation blocks invalid mutating requests.
- Auth events create `audit_log` records with `request_id`.

## Rollout Plan
1. Deploy with `SPACEGATE_AUTH_ENABLE=0` in production (dark launch).
2. Run migrations and seed one admin allowlist entry.
3. Enable auth; verify login + `/admin` access with test account.
4. Verify deny path with non-allowlisted account.
5. Monitor audit logs and error rates.

## Open Decisions for Review
- SQLite vs Postgres for admin DB on `antiproton` long-term.
- Exact session timeout values for admin operations.
- Whether to require step-up re-auth before future `Checkpoint C` mutation actions.
