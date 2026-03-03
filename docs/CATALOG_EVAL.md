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
- `nasa_exoplanet_archive`
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
5. Update source precedence only after the comparison notes are written.

Interpretation notes:

- Random sample is deterministic by `sample_key + seed`, so reruns stay comparable.
- Overlap sample is biased toward rows that intersect current core star identifiers or normalized names.
- Lack of overlap does not necessarily mean low quality; it can also indicate missing identifier harmonization or a different object class.
