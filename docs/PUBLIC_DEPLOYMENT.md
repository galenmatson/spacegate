# Public Deployment Runbook

This runbook covers the Photon-to-antiproton deployment path for
`coolstars.org` and `spacegates.org`. Antiproton is internet exposed. Never
build science there, print expanded secret-bearing Compose configuration, or
activate an artifact that has not passed the release manifest checks.

## Release Shape

Public edge release v2 deploys four immutable artifacts sharing one exact
build ID:

1. the scientific CORE/ARM/DISC/hierarchy/map bundle;
2. the immutable Search v2 and system-summary SQLite projection;
3. the frozen policy-selected simulation-scene set;
4. the compact Smart Tag hot projection and portable assignment/source
   evidence archive.

The API fails visibly when the active scientific build and Search v2 projection
do not agree. Once Smart Tags are required, the active tag manifest must also
match the build and registry schemas. The frozen scene archive must be unpacked into the build-keyed
runtime scene cache; copying the archive without installing it does not warm the
public service.

The first three rows below record the already deployed July 26 Public Read v1
release. The fourth row is the locally accepted M8.3e.2 candidate and has not
been deployed:

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Scientific archive | 17,052,804,724 | `c2954d6a1b641347968f56cb0753ea1d2ef7b4625d6f830fb78cede4462642e9` |
| Public Read v2 SQLite | 16,455,413,760 | `0748a315ece80813c3349d4e8cc3495fbd0ffeb67745ba2aa3c225acc60e621f` |
| Frozen scenes | 80,752,521 | `519ac2c7951a791bdd2b9cae2b7142475a42c706348e8bb14d2c8dedb5aeba9c` |
| Smart Tags v2.2 | 374,478,854 | `80169a905eb96c0069bd80c6a48ffb79865e090c2e90d0e2c7cd12e3cb2e95bc` |

The historical three-artifact transfer was 33,588,971,005 bytes. The verified
four-artifact candidate is 33,963,449,859 bytes. Its release manifest is:

```text
/data/spacegate/state/releases/e7_24cb15211f430a37f199f462_full_public/smart-tags-v2/80a761ba3eb2fff23f339e172275b668f25cade40c26921d93241bd1edc635ec/release.json
```

## Runtime Contract

The accepted 6-vCPU/12-GiB capacity campaign requires:

```dotenv
SPACEGATE_API_DUCKDB_MEMORY_LIMIT=5GB
SPACEGATE_API_DUCKDB_THREADS=1
SPACEGATE_API_DB_POOL_SIZE=6
SPACEGATE_API_DB_ACQUIRE_TIMEOUT_SECONDS=30
SPACEGATE_PUBLIC_READ_COMPATIBILITY_FALLBACK=0
SPACEGATE_SMART_TAGS_REQUIRED=1
```

Authentication remains enabled through the existing private OIDC and session
settings. The release tool updates only the bounded non-secret runtime keys and
preserves every other environment line.

## Photon Preflight

Use the repository virtual environment and verify the release:

```bash
cd /srv/spacegate/app
.venv/bin/python scripts/public_edge_release.py verify-source \
  --manifest /data/spacegate/state/releases/e7_24cb15211f430a37f199f462_full_public/smart-tags-v2/80a761ba3eb2fff23f339e172275b668f25cade40c26921d93241bd1edc635ec/release.json
```

Run normal local verification and confirm Docker health before transfer:

```bash
SPACEGATE_STATE_DIR=/data/spacegate/state \
  scripts/verify_build.sh e7_24cb15211f430a37f199f462_full_public
.venv/bin/python scripts/test_api_integration.py http://127.0.0.1:8000/api/v1
.venv/bin/python scripts/verify_known_systems_api.py http://127.0.0.1:8000/api/v1
scripts/compose_spacegate.sh ps
```

## Edge Disk Gate

The transfer helper enforces 57,356,235,850 free bytes before sending the first
large file and a 15-GiB reserve after every stage. This covers both peak
scientific extraction and the final installed artifact set.

On July 26, 2026 the reviewed preflight removed only:

- superseded extracted build `20260717T0336Z_8bee500_side`;
- its unreferenced published archive;
- a stranded July 14 bootstrap download cache;
- unused Docker BuildKit cache.

The active `20260717T0614Z_f452835_side` build and its published archive remain
the immediate rollback. Antiproton has 70,120,824,832 bytes free after cleanup.
Do not repeat the cleanup by pattern or delete the active archive.

## Sync Code Without Restart

First push the release tooling and application tree without changing the
running containers:

```bash
cd /srv/spacegate/app
scripts/deploy_antiproton.sh \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 3 \
  --sync-only
```

Then configure and verify the measured edge limits. This changes the private
environment file but does not restart the current service:

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "cd /srv/spacegate/app && \
   python3 scripts/public_edge_release.py configure-runtime-env \
     --manifest /srv/spacegate/data/incoming/public-edge/e7_24cb15211f430a37f199f462_full_public/release.json \
     --env-file .spacegate.local.env"
```

The release manifest must be present remotely before that command. The transfer
helper sends it before applying its runtime and disk gates; alternatively copy
that small file first as shown in the operator handoff.

## Streamed Transfer And Staging

Do not use the legacy `push_published_db.sh` path for Public Read v2. It knows
only the scientific archive and cannot produce an atomic four-artifact release.

Run:

```bash
scripts/push_public_edge_release.sh \
  --manifest /data/spacegate/state/releases/e7_24cb15211f430a37f199f462_full_public/release.json \
  --remote sgdeploy@158.69.198.29 \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 3
```

The helper:

1. re-hashes every local source;
2. checks remote free space and measured runtime settings;
3. transfers the scientific archive with resumable `rsync`;
4. verifies and extracts it into `out/<build_id>`;
5. removes only that temporary incoming archive;
6. transfers Search v2 directly into its versioned derived location;
7. verifies and unpacks the frozen scene set;
8. verifies and unpacks the Smart Tag hot and portable evidence artifacts;
9. verifies the complete installed release.

It deliberately does not change `served/current` or restart containers.
Interrupted transfers retain rsync partial files and are safe to rerun.

## Activation

After staging has passed, activate the release and rebuild the public containers
at one reviewed checkpoint:

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "cd /srv/spacegate/app && \
   python3 scripts/public_edge_release.py activate \
     --manifest /srv/spacegate/data/incoming/public-edge/e7_24cb15211f430a37f199f462_full_public/release.json \
     --state-dir /srv/spacegate/data"

scripts/deploy_antiproton.sh \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 3 \
  --skip-auto-score
```

The activation command verifies all four installed artifacts, installs the
build-local Smart Tag `current` pointer, then atomically replaces
`served/current` and records both previous targets. `--skip-auto-score`
is mandatory because immutable DISC output is already verified.

Frozen public scenes must be runtime-readable before activation. The installer
normalizes each build-keyed cache directory to `0755` and its manifest and
scene payloads to `0644`; `verify-installed` rejects a cache that cannot be
traversed or read independently of the deploy user's group. Do not repair this
by making the API container privileged or by granting write access to immutable
scene payloads.

## Public Verification

```bash
curl -fsS https://coolstars.org/api/v1/health
.venv/bin/python scripts/test_api_integration.py https://coolstars.org/api/v1
.venv/bin/python scripts/verify_known_systems_api.py https://coolstars.org/api/v1
```

Also run exact TIC/TOI, hierarchy/nested-orbit, map, simulation, desktop/mobile,
and authentication checks. Health must report the new build ID. Runtime
telemetry must report Public Read v2 hits with no compatibility fallbacks.

## Rollback

If activation or public verification fails:

```bash
ssh -i ~/.ssh/spacegate_antiproton \
  -o IdentitiesOnly=yes \
  -o BatchMode=yes \
  -o ConnectTimeout=8 \
  sgdeploy@158.69.198.29 \
  "cd /srv/spacegate/app && \
   python3 scripts/public_edge_release.py rollback \
     --build-id e7_24cb15211f430a37f199f462_full_public \
     --state-dir /srv/spacegate/data && \
   scripts/compose_spacegate.sh up -d --build api web"
```

Rollback uses the recorded prior extracted and Smart Tag targets. Preserve
them, the new installed artifacts, and both activation records until public
soak is accepted.

## SSH Hygiene

Use the private operator route, `IdentitiesOnly`, `BatchMode`, an eight-second
connect timeout, and at least a two-second cooldown between independent SSH
connections. Do not run SSH-heavy diagnostics in parallel. Avoid
`docker compose config`, which can expose expanded secrets.

## July 26, 2026 Activation Record

Release `e7_24cb15211f430a37f199f462_full_public` was staged, activated,
rolled back on the first failed search gate, repaired through general hotfix
`555c46b`, shadow-verified, and reactivated. The failure was a `2700`
build-cache directory inherited from the staging process: the API could read
the scene files by mode but could not traverse their parent. The rollback
compatibility flag was also present in the host environment but absent from the
Compose environment contract; the same hotfix closes both defects.

The corrected public release passes API integration, exact TIC/TOI outcomes,
known-system and nested-orbit benchmarks, all four map manifests, desktop and
mobile browser smoke, 4K canvas-pixel verification, and progressive 1,000-ly
rendering. Compatibility fallback is disabled. Retain the July 17 extracted
build and publication artifacts until soak acceptance.
