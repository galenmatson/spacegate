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

The rows below record the already deployed July 26 Public Read v1 release and
its later compatibility Smart Tag candidate:

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

M8.3e.2b upgrades the next release contract to
`spacegate.smart_tags.v4`/compiler v2.7. The complete build-matched August 6
candidate is:

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Scientific archive | 17,082,150,067 | `004fbfd42ca26e53c099595116d3b318c533542c15f981729207b5ae4985173c` |
| Public Read v2 SQLite | 16,455,512,064 | `87edcf9cd40a3171d91b892007ad9584925cb4dc7c6eec8071dc6ded48922086` |
| Frozen scenes | 80,782,995 | `78a6309fe93632caa1c980d0899f31d700bc430bc7c38613d94b8b0d978e5cfa` |
| Smart Tags v4 | 452,101,060 | `9169b50cab0debd812de24d5d1063a686a4fe1973870181192bcd3ba4ca3de5b` |

The verified transfer is 34,070,546,186 bytes. The manifest is:

```text
/data/spacegate/state/releases/20260804T1130Z_68fd99b_a2_planet_badges/smart-tags-v4/79ad0373cd586867e821537211f50b7b516166eb5637c2d6544543fdaf085f13/release.json
```

`verify-source` passes all four roles. This release is not deployed. Code was
synchronized without restart, but the currently served July release remains
active until the edge disk gate can retain rollback safety.

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
  --manifest /data/spacegate/state/releases/20260804T1130Z_68fd99b_a2_planet_badges/smart-tags-v4/79ad0373cd586867e821537211f50b7b516166eb5637c2d6544543fdaf085f13/release.json
```

Run normal local verification and confirm Docker health before transfer:

```bash
SPACEGATE_STATE_DIR=/data/spacegate/state \
  scripts/verify_build.sh 20260804T1130Z_68fd99b_a2_planet_badges
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

The August 6 candidate requires exactly 58,368,187,518 bytes free before
staging, including the enforced 15-GiB post-stage reserve. A read-only preflight
found 27,666,776,064 bytes free on the 102,888,095,744-byte root filesystem,
leaving a 30,701,411,454-byte deficit. Spacegate therefore added a separate
200-GB ext4 data volume. The volume is mounted at `/data`, but active DuckDB,
SQLite, tile, and scene reads remain on the faster root filesystem.

On August 8, the inactive July 17 rollback was copied to the cold volume,
inventoried, checksummed, independently verified, and only then retired from
root. Root free space increased from 27,591,524,352 to 47,976,538,112 bytes.
This improves operating reserve but does not yet satisfy the candidate's
58,368,187,518-byte hot install gate. The dual-root release path therefore
stages and verifies the complete candidate on `/data` first, then installs it
onto root only when a separate measured hot-capacity gate passes. Do not weaken
the reserve.

## Edge Cold Rollback Tier

`/data/spacegate` is a failure-contained cold tier for inactive rollback and
staging artifacts. It is not a live database path. The tool requires all of:

- `/data` is a distinct mounted filesystem;
- `/data/.antiproton-data-volume-id` contains the expected volume UUID;
- the marker and cold root reside on the same filesystem;
- the hot and cold roots reside on different filesystems;
- the requested build is not currently served.

The retained July 17 snapshot is:

```text
/data/spacegate/rollbacks/20260717T0614Z_f452835_side/
```

It contains 4,838 build files totaling 12,887,145,223 bytes and the
7,487,390,124-byte publication archive. Its logical SHA-256 is
`ce80c3bdf63a8e533edcbfd55a352c9911e18aef859e5ec65415fa8f84d5a66c`.

Verify it without changing runtime state:

```bash
python3 scripts/public_edge_cold_storage.py verify-snapshot \
  --cold-root /data/spacegate \
  --hot-state-dir /srv/spacegate/data \
  --volume-id a243664c-231f-4cf8-8487-bb39f82d555d \
  --build-id 20260717T0614Z_f452835_side
```

Cold rollback is deliberately not activated in place. Restore the verified
build to the fast root before changing the served pointer:

```bash
python3 scripts/public_edge_cold_storage.py restore \
  --cold-root /data/spacegate \
  --hot-state-dir /srv/spacegate/data \
  --volume-id a243664c-231f-4cf8-8487-bb39f82d555d \
  --build-id 20260717T0614Z_f452835_side

python3 scripts/public_edge_release.py rollback \
  --build-id e7_24cb15211f430a37f199f462_full_public \
  --state-dir /srv/spacegate/data
```

The restore enforces a 15-GiB free-space reserve and exact inventory equality.
The rollback command accepts the deployed v1 activation record only because it
predates Smart Tags and contains no prior Smart Tag pointer. A v1 record that
claims Smart Tag state is rejected.

## Sync Code Without Restart

First push the release tooling and application tree without changing the
running containers:

```bash
cd /srv/spacegate/app
scripts/deploy_antiproton.sh \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 15 \
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
     --manifest /data/spacegate/incoming/public-edge/20260804T1130Z_68fd99b_a2_planet_badges/release.json \
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
  --manifest /data/spacegate/state/releases/20260804T1130Z_68fd99b_a2_planet_badges/smart-tags-v4/79ad0373cd586867e821537211f50b7b516166eb5637c2d6544543fdaf085f13/release.json \
  --remote sgdeploy@158.69.198.29 \
  --remote-cold-root /data/spacegate \
  --cold-volume-id a243664c-231f-4cf8-8487-bb39f82d555d \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 15
```

The helper:

1. re-hashes every local source;
2. verifies the mounted cold-volume UUID and separate filesystem identity;
3. checks cold-stage free space and measured runtime settings;
4. transfers the scientific archive to `/data` with resumable `rsync`;
5. verifies and extracts it into the cold staged state;
6. removes only that temporary incoming archive;
7. transfers and stages Search v2, frozen scenes, and Smart Tags on `/data`;
8. verifies the complete cold release through the normal installed-release
   contract;
9. reports the exact missing hot bytes and 15-GiB reserve requirement.

It deliberately does not change `served/current` or restart containers.
Interrupted transfers retain rsync partial files and are safe to rerun.

After the reported hot-capacity gate passes, install the verified release onto
the fast state filesystem without activating it:

```bash
python3 scripts/public_edge_release.py install-from-state \
  --manifest /data/spacegate/incoming/public-edge/20260804T1130Z_68fd99b_a2_planet_badges/release.json \
  --source-state-dir /data/spacegate/staged/public-edge \
  --state-dir /srv/spacegate/data
```

This operation re-verifies the cold release, copies each managed directory
through a target-filesystem temporary path, reuses already verified units on a
retry, verifies the resulting hot release, and preserves the cold stage. It
fails before copying when hot free space cannot retain the complete missing
closure plus 15 GiB. `--install-hot` may be added to the transfer helper only
when that gate is already known to pass.

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
     --manifest /data/spacegate/incoming/public-edge/20260804T1130Z_68fd99b_a2_planet_badges/release.json \
     --state-dir /srv/spacegate/data"

scripts/deploy_antiproton.sh \
  --ssh-key ~/.ssh/spacegate_antiproton \
  --ssh-cooldown 15 \
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
     --build-id 20260804T1130Z_68fd99b_a2_planet_badges \
     --state-dir /srv/spacegate/data && \
   scripts/compose_spacegate.sh up -d --build api web"
```

Rollback uses the recorded prior extracted and Smart Tag targets. Preserve
them, the new installed artifacts, and both activation records until public
soak is accepted.

## SSH Hygiene

Use the private operator route, `IdentitiesOnly`, `BatchMode`, an eight-second
connect timeout, and a 15-second cooldown between independent SSH
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
