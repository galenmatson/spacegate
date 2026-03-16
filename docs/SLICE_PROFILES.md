# Spacegate Slice Profiles and SLO Targets

This document defines deterministic `core` slice profiles derived from `galaxy`, plus service-level objectives (SLOs) used for promotion gates.

Scope:

- astronomy dataset shaping (`galaxy` -> `core` + complementary `halo`)
- default and optional profile definitions
- latency/resource/error SLO targets for each profile class

Out of scope:

- scientific truth policy (see `docs/PROJECT.md`)
- canonical schema details (see `docs/SCHEMA_CORE.md`)

## Slice Contract

A slice profile is a versioned policy over star rows at ingest time.

Current profile knobs (must map directly to ingest/runtime flags):

- `max_distance_ly`
- `min_parallax_over_error`
- `max_parallax_error_mas`
- `max_ruwe`
- `require_spectral_class`
- `require_color_index`
- `allowed_spectral_classes`

Execution contract:

1. Build full `galaxy` from pinned inputs.
2. Apply exactly one named `core` profile.
3. Materialize `halo` as the deterministic complement.
4. Emit `reports/<build_id>/slice_policy_report.json`.
5. Promote only if profile SLO gates pass.

Identity/provenance rules:

- same `stable_object_key` across `galaxy`, `core`, and `halo`
- no row mutation between projections
- reversibility by rebuild from the same `galaxy` build + different profile

## Profile Catalog (Draft v1)

Profile IDs are immutable once published. Changes require a new `profile_version`.

### `core.default@v1` (recommended default)

Intent:

- broad nearby-sky coverage with reliability floor
- baseline for public browse/search performance

Filters:

- `max_distance_ly=1000`
- `min_parallax_over_error=5`
- `max_ruwe=1.4`
- `max_parallax_error_mas` unset
- `require_spectral_class=false`
- `require_color_index=false`
- `allowed_spectral_classes=[]` (all)

Expected scale band:

- approximately 40-50% of current `<1000 ly` `galaxy` stars (based on March 2026 Gaia counts)

### `core.performance@v1`

Intent:

- faster default search/detail for constrained hardware

Filters:

- `max_distance_ly=750`
- `min_parallax_over_error=8`
- `max_parallax_error_mas=0.2`
- `max_ruwe=1.3`
- `require_spectral_class=false`
- `require_color_index=false`
- `allowed_spectral_classes=[]`

Expected scale band:

- approximately 20-35% of current `<1000 ly` `galaxy` stars

### `core.precision@v1`

Intent:

- high-confidence astrometry for hierarchy/orbit-heavy analysis workflows

Filters:

- `max_distance_ly=500`
- `min_parallax_over_error=10`
- `max_parallax_error_mas=0.1`
- `max_ruwe=1.2`
- `require_spectral_class=false`
- `require_color_index=false`
- `allowed_spectral_classes=[]`

Expected scale band:

- approximately 10-20% of current `<1000 ly` `galaxy` stars

### `core.visual@v1`

Intent:

- rendering-first subset minimizing unknown-color edge cases

Filters:

- `max_distance_ly=1000`
- `min_parallax_over_error=5`
- `max_ruwe=1.4`
- `require_spectral_class=true`
- `require_color_index=true`
- `allowed_spectral_classes=[]`

Expected scale band:

- near `core.default@v1` when Gaia photometry coverage is high; should be tracked per build

## Halo Policy (Draft)

`halo` is not independently tuned. It is always:

- `halo = galaxy - core(profile)`

Serving rule:

- `halo` is excluded from default browse/search
- included only via explicit user intent (for example, advanced toggle/deep mode)

## SLO Targets (Draft v1)

Promotion must evaluate SLOs against the selected active profile.

### SLI Definitions

- `search_latency_ms`: `GET /api/v1/systems/search?q=a&limit=50`
- `detail_latency_ms`: representative system detail API call
- `error_rate_pct`: non-2xx/3xx during stress run
- `api_rss_bytes`: API process steady RSS during load
- `api_peak_rss_bytes`: API VmHWM during run

Measurement tooling:

- `scripts/spacegate_stress.sh` (`smoke`, `mixed`, `search-heavy`, optional `spike`)
- admin status endpoint (`/api/v1/admin/status/dataset`) for memory/size/context

### Profile SLO Classes

`core.default@v1`:

- mixed-load `p95 search_latency_ms <= 1200`
- mixed-load `p99 search_latency_ms <= 2500`
- detail `p95 <= 900`
- `error_rate_pct <= 1.0`
- steady `api_rss_bytes <= 3.5 GiB`
- peak `api_peak_rss_bytes <= 8.0 GiB`

`core.performance@v1`:

- mixed-load `p95 search_latency_ms <= 800`
- mixed-load `p99 search_latency_ms <= 1600`
- detail `p95 <= 700`
- `error_rate_pct <= 0.8`
- steady `api_rss_bytes <= 2.5 GiB`
- peak `api_peak_rss_bytes <= 6.0 GiB`

`core.precision@v1`:

- mixed-load `p95 search_latency_ms <= 1000`
- mixed-load `p99 search_latency_ms <= 2000`
- detail `p95 <= 800`
- `error_rate_pct <= 1.0`
- steady `api_rss_bytes <= 3.0 GiB`
- peak `api_peak_rss_bytes <= 7.0 GiB`

`halo`-inclusive deep mode (nondefault):

- mixed-load `p95 search_latency_ms <= 3500`
- mixed-load `p99 search_latency_ms <= 6000`
- `error_rate_pct <= 2.0`
- no impact on default-mode SLO compliance

## Promotion Gate Procedure

1. Build `galaxy`.
2. Build `core` with explicit `profile_id@profile_version`.
3. Build complementary `halo`.
4. Run `scripts/check_profile_slo.py` against the promoted candidate build endpoint.
5. Compare measured SLI values to profile SLO class (search/detail p95/p99, error rate, API memory).
6. Promote only on pass; otherwise rollback `served/current` to the previous build.

## Execution Runbook (Current Scripts)

1. Materialize existing full build as `galaxy`:
   - `scripts/materialize_galaxy.sh <build_id>`
2. Build/promote sliced core with explicit profile metadata:
   - `scripts/build_core_slice.sh --from-cooked --profile-id <id> --profile-version <ver> --source-galaxy-build-id <build_id> ...slice knobs...`
   - promotion now runs `scripts/check_profile_slo.py` by default for profile-tagged builds (`SPACEGATE_PROMOTE_ENFORCE_PROFILE_SLO=1`)
3. Build halo complement from the (`galaxy`, `core`) pair:
   - `scripts/build_halo.sh --galaxy-build-id <galaxy_build_id> --core-build-id <core_build_id>`

## Known Constraint (Current Builds)

Admin slice preview operates against currently served `stars` columns.

If a build does not expose a threshold column (for example `parallax_over_error`), preview warns and skips that filter. Ingest-time profile enforcement remains authoritative.

Follow-up requirement:

- store profile ID/version and all applied thresholds in `build_metadata` on every sliced build.
