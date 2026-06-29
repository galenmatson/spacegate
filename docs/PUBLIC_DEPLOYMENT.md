# Public Deployment Runbook

This runbook covers the Photon-to-antiproton public deployment path for
`coolstars.org` and `spacegates.org`.

## Hosts and Roles

- Photon builds and verifies canonical/public database artifacts.
- Antiproton serves the public web/API containers and the public download root.
- Proton is reference/fallback only; do not mutate it unless explicitly asked.

Antiproton is internet exposed. Prefer prebuilt, verified artifacts from Photon
over rebuilding catalogs on antiproton. The public VPS has limited CPU/RAM and
should spend its resources serving requests, not cooking full catalogs.

## Public Slice Policy

The active public-host profile is tracked in `docs/SLICE_PROFILES.md`.

For constrained public service, the expected artifact shape is:

- `core.duckdb` sliced for the public-host profile
- `arm.duckdb` sliced to retained systems/components and required Sol side data
- `canonical_hierarchy.duckdb` sliced to retained system roots/descendants
- `disc.duckdb` and `disc/*.parquet` sliced to retained systems

Do not publish a core-only slice with full side artifacts to antiproton unless
that is an intentional emergency fallback. Full side artifacts increase transfer
time and public API memory pressure.

## Preflight on Photon

Confirm the local build is the intended published artifact:

```bash
readlink -f /data/spacegate/dl/current
cat /data/spacegate/dl/current.json
```

Run local verification before touching antiproton:

```bash
SPACEGATE_STATE_DIR=/data/spacegate/state scripts/verify_build.sh <build_id>
.venv/bin/python scripts/test_api_integration.py http://127.0.0.1:8000/api/v1
.venv/bin/python scripts/verify_known_systems_api.py http://127.0.0.1:8000/api/v1
```

For the June 29, 2026 public side-sliced build, the expected build id is:

```text
20260629T_public_aliasfix_v3_side
```

## SSH Hygiene

Antiproton runs UFW and fail2ban. Deploy scripts should use a small cooldown
between SSH connections to avoid looking like a bursty automation probe.

Use:

```bash
--ssh-cooldown 2
```

Do not run independent SSH-heavy diagnostic commands in parallel against
antiproton during deploy. If a connection is refused after a burst, wait before
retrying and check UFW/fail2ban status from an existing trusted session if
available.

## Publish the Database Archive

Publishing copies the current local DB archive and metadata to
`/srv/spacegate/dl` on antiproton. This does not activate the runtime database.

```bash
scripts/push_published_db.sh \
  --remote sgdeploy@158.69.198.29 \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 2 \
  --skip-catalogs \
  --set-current-link
```

After upload, confirm the remote download pointer:

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "ls -lh /srv/spacegate/dl/current.json /srv/spacegate/dl/current && readlink -f /srv/spacegate/dl/current"
```

## Activate the Runtime Database

Activation copies/verifies the published archive from antiproton's local
download root, extracts it into `/srv/spacegate/data/out/<build_id>`, and
promotes `/srv/spacegate/data/served/current`.

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "cd /srv/spacegate/app && SPACEGATE_STATE_DIR=/srv/spacegate/data scripts/bootstrap_core_db.sh --meta-url file:///srv/spacegate/dl/current.json --base-url file:///srv/spacegate/dl/"
```

Use `--overwrite` only after checking that a partial extracted build exists and
that replacing it is intentional.

## Deploy Application Code

After the runtime DB is activated, sync the app and restart containers:

```bash
scripts/deploy_antiproton.sh \
  --remote sgdeploy@158.69.198.29 \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 2 \
  --public-url https://coolstars.org \
  --expect-auth enabled
```

The deploy script preserves remote environment files and rebuilds/restarts the
Compose services.

## Public Verification

Verify the public build and key API flows:

```bash
curl -fsS https://coolstars.org/api/v1/health
.venv/bin/python scripts/test_api_integration.py https://coolstars.org/api/v1
.venv/bin/python scripts/verify_known_systems_api.py https://coolstars.org/api/v1
```

The health response must report the newly promoted build id before considering
the deployment complete.

## Rollback

Preserve the previous extracted build in `/srv/spacegate/data/out/` until the
new public deployment is verified. The current served symlink is the critical
rollback lever:

```text
/srv/spacegate/data/served/current
```

If the new build fails public verification, restore that symlink to the previous
known-good build and restart the API/web containers. Do not delete old extracted
builds or old public archives until rollback is no longer needed.

## Diagnostics

Use sequential SSH diagnostics with cooldown discipline:

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "cd /srv/spacegate/app && scripts/compose_spacegate.sh ps"
```

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "cd /srv/spacegate/app && scripts/compose_spacegate.sh logs --tail=120 api"
```

Avoid `docker compose config` in routine diagnostics because expanded
environment output may contain secrets.
