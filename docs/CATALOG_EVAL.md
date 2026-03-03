# Catalog Evaluation Workflow

Use `scripts/catalog_eval.sh` to create deterministic sample sets and coverage summaries for candidate source catalogs before changing field precedence.

Default behavior:

- discovers locally available built-in catalogs
- writes reports under `$SPACEGATE_STATE_DIR/reports/catalog_eval/<run_id>/`
- emits:
  - `summary.md`
  - `summary.json`
  - per-catalog `*_summary.json`
  - per-catalog `*_random_sample.csv`
  - per-catalog `*_overlap_sample.csv`

Current built-ins:

- `gaia_dr3_non_single_sample`
- `gaia_dr3_nss_two_body_sample`
- `gaia_dr3_sample`
- `athyg`
- `msc`
- `nasa_exoplanet_archive`
- `orb6`
- `sbx_sample`
- `wds`

Fetch the sample inputs first:

```bash
cd /srv/spacegate/app
scripts/fetch_catalog_samples.sh --sample-size 100
```

Current overlap target:

- `served/current/core.duckdb` star identifiers and normalized names

Example usage:

```bash
cd /srv/spacegate/app
scripts/catalog_eval.sh --sample-size 100
```

Evaluate a specific catalog:

```bash
cd /srv/spacegate/app
scripts/catalog_eval.sh --catalog athyg --sample-size 100
```

Recommended workflow:

1. Fetch sample inputs for the candidate source families.
2. Run `catalog_eval.py` on each candidate source family.
3. Review `summary.md` for coverage and key-overlap counts.
4. Inspect the random and overlap CSV samples manually.
5. For multiplicity sources, run `scripts/multiplicity_crossmatch.sh` to distinguish exact-ID overlap from confidence-scored coordinate matches.
6. Update source precedence only after the comparison notes are written.

Multiplicity crossmatch example:

```bash
cd /srv/spacegate/app
scripts/multiplicity_crossmatch.sh --catalog wds --catalog msc --catalog orb6
```

Interpretation notes:

- Random sample is deterministic by `sample_key + seed`, so reruns stay comparable.
- Overlap sample is biased toward rows that intersect current core star identifiers or normalized names.
- Lack of overlap does not necessarily mean low quality; it can also indicate missing identifier harmonization or a different object class.
- `catalog_eval` overlap counts are exact-key only. Coordinate-led multiplicity catalogs such as `WDS` need the separate crossmatch report before they can be judged fairly.
- As of the March 3, 2026 evaluation pass:
  - `MSC` and `ORB6` showed strong `HIP/HD` overlap with current core.
  - `SBX` samples showed strong `Gaia/HIP/HD` overlap and good astrometric coverage.
  - `WDS` showed broad coverage, but only modest medium/high-confidence coordinate matches against current core.
- Operational note:
  - the official `MSC` host currently required an unverified-TLS fallback during sample fetch on proton; treat that as a source-access caveat until a cleaner verified path or mirror is pinned.
  - current overlap/crossmatch reports were generated against the presently served core build; `scripts/ingest_core.py` now includes an AT-HYG RA-hours-to-degrees fix, so coordinate-led comparisons should be rerun after the next core rebuild.
