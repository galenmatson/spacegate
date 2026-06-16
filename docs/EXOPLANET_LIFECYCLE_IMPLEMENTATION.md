# Exoplanet Lifecycle Implementation Plan

This document translates the roadmap/schema decisions into concrete DDL and execution steps.

Implementation status (2026-03-15):

- lifecycle/status materialization in `core` is implemented
- lifecycle reports (`planet_catalog_delta_report.json`, `planet_reclassification_report.json`) are emitted
- lifecycle audit lineage tables are now mirrored into `arm.duckdb` each build

Scope:

- multi-catalog exoplanet lifecycle ingestion before taxonomy/habitability scoring
- deterministic status precedence and pruning/tombstone behavior
- catalog-diff-triggered re-evaluation of derived planet tags

Authoritative policy references:

- `docs/PROJECT.md` (lifecycle + diff policy)
- `docs/SCHEMA_CORE.md` (core contract)
- `docs/MILESTONES.md` (`M5.3` then `M5.5`)

## 1) Core DDL Delta (`core.duckdb`)

Reference DDL for `planets` lifecycle + derived fields:

```sql
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_status TEXT DEFAULT 'confirmed';
ALTER TABLE planets ADD COLUMN IF NOT EXISTS is_default_visible BOOLEAN DEFAULT TRUE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS is_tombstoned BOOLEAN DEFAULT FALSE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS status_source_catalog TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS status_updated_at TIMESTAMP;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS status_superseded_by TEXT;

ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_size_mass_class TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_insolation_class TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_orbit_class TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_composition_proxy_class TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_detection_tags_json TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_host_context_tags_json TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_classifier_version TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_classifier_updated_at TIMESTAMP;

ALTER TABLE planets ADD COLUMN IF NOT EXISTS spacegate_hab_score DOUBLE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS spacegate_hab_confidence DOUBLE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS spacegate_hab_reasons_json TEXT;

ALTER TABLE planets ADD COLUMN IF NOT EXISTS host_metallicity_feh DOUBLE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS host_metallicity_feh_error DOUBLE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_element_richness_score DOUBLE;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_element_richness_class TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_element_richness_method TEXT;
ALTER TABLE planets ADD COLUMN IF NOT EXISTS planet_element_richness_notes TEXT;
```

Status/materialization invariants:

```sql
-- enforce resolved defaults in ingest SQL
-- candidate      -> visible=true, tombstoned=false
-- controversial  -> visible=false, tombstoned=false
-- retracted      -> visible=false, tombstoned=true
-- confirmed      -> visible=true, tombstoned=false
```

Recommended query index (validate cost/benefit on DuckDB runtime):

```sql
CREATE INDEX IF NOT EXISTS idx_planets_status_hab
ON planets(planet_status, is_default_visible, spacegate_hab_score);
```

## 2) Arm DDL (`arm.duckdb`) for Audit and Diff Traceability

```sql
CREATE TABLE IF NOT EXISTS planet_catalog_observations (
  build_id TEXT,
  stable_object_key TEXT,
  source_catalog TEXT,
  source_version TEXT,
  source_pk TEXT,
  source_row_hash TEXT,
  observed_status TEXT,
  observed_at TIMESTAMP,
  payload_json TEXT
);

CREATE TABLE IF NOT EXISTS planet_status_history (
  build_id TEXT,
  stable_object_key TEXT,
  previous_status TEXT,
  resolved_status TEXT,
  transition_type TEXT,        -- new/promoted/demoted/retracted/unchanged/missing
  resolved_by_catalog TEXT,
  resolved_at TIMESTAMP,
  details_json TEXT
);

CREATE TABLE IF NOT EXISTS planet_reclassification_audit (
  build_id TEXT,
  stable_object_key TEXT,
  classifier_version TEXT,
  previous_classifier_version TEXT,
  reclass_reason TEXT,         -- source_delta/host_delta/preference_delta/manual_policy
  fields_recomputed_json TEXT,
  recomputed_at TIMESTAMP
);
```

## 3) Status Precedence Rules

Deterministic precedence for resolved `planet_status`:

1. `retracted` (if any authoritative source flags as retracted/refuted)
2. `confirmed`
3. `candidate`
4. `controversial`

Visibility/tombstone policy:

- `confirmed`: visible
- `candidate`: visible
- `controversial`: default hidden, optional include toggle
- `retracted`: hidden + tombstoned (kept for lineage and rim references)

## 4) Pipeline Tasks by Stage

### 4.1 Download

1. Add source entries/manifests in `scripts/catalogs.sh`:
   - `exoplanet_eu`
   - `open_exoplanet_catalogue`
   - `hwc`
2. Wire to `scripts/download_core.sh` with environment flags and mandatory manifest checks.
3. Emit manifests under `reports/manifests/` for each new source.

### 4.2 Cook

1. Add a dedicated normalizer (recommended: `scripts/cook_exoplanet_lifecycle.py`).
2. Normalize IDs, host keys, units, and source status vocabulary.
3. Produce typed cooked outputs:
   - `cooked/exoplanet_lifecycle/source_status_rows.csv`
   - `cooked/exoplanet_lifecycle/source_alias_rows.csv`
   - `cooked/exoplanet_lifecycle/source_habitability_features.csv`

### 4.3 Ingest

1. Extend `scripts/ingest_core.py` planet build path:
   - load lifecycle cooked tables
   - resolve per-planet status by precedence
   - materialize visibility/tombstone fields
2. Run taxonomy + habitability + element-richness classifiers.
3. Write `planet_catalog_observations`, `planet_status_history`, `planet_reclassification_audit` into `arm.duckdb`.

### 4.4 Diff and Re-evaluation

1. Compare current source snapshot against prior promoted build snapshot by deterministic source key.
2. Build impacted set:
   - changed source rows
   - rows whose host-star classifier inputs changed
   - rows impacted by cross-source status precedence changes
3. Recompute all derived fields for impacted set with active classifier version.
4. Fail build if any served row retains stale classifier version.

### 4.5 Reports

Emit:

- `reports/<build_id>/planet_catalog_delta_report.json`
- `reports/<build_id>/planet_reclassification_report.json`
- update dataset/admin summary payload with lifecycle transition counts and tag recompute counts

### 4.6 API and UI

1. API filters:
   - `include_controversial` (default false)
   - `exclude_retracted` always true for default science responses
   - `hab_score_min` / `hab_score_max`
2. UI:
   - controversial toggle
   - habitability slider + quick top-N
   - expose element-richness class as optional filter chip

### 4.7 Verify and Promote Gates

`scripts/verify_build.sh` must fail on:

- missing lifecycle/reclassification reports
- stale classifier version in served `planets`
- invalid status->visibility/tombstone combinations

## 5) Practical Execution Order

1. implement download + cook for new exoplanet lifecycle sources
2. implement ingest status resolution + lifecycle columns
3. implement reports and verify gates
4. implement taxonomy/habitability/element-richness classifiers
5. implement API filters + UI controls
6. run full build and review contribution/overlap + transition metrics

## 6) Notes on Scientific Interpretation

- `planet_element_richness_*` is intentionally a proxy for utility/rim ranking and should be labeled as inferred.
- direct atmospheric/abundance measurements, when present, must override host-metallicity proxy logic.
- HWC remains a comparison/reference signal; Spacegate owns the canonical `spacegate_hab_score`.
