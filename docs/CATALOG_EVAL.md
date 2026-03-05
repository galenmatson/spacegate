# Catalog Evaluation Workflow (Gaia-First)

Use `scripts/catalog_eval.sh` to create deterministic sample sets and coverage summaries before changing canonical source policy.

Default behavior:

- discovers locally available built-in catalogs
- writes reports under `$SPACEGATE_STATE_DIR/reports/catalog_eval/<run_id>/`
- emits:
  - `summary.md`
  - `summary.json`
  - per-catalog `*_summary.json`
  - per-catalog `*_random_sample.csv`
  - per-catalog `*_overlap_sample.csv`

Current built-ins include canonical and auxiliary candidates:

- `gaia_dr3_non_single_sample`
- `gaia_dr3_nss_two_body_sample`
- `gaia_dr3_sample`
- `athyg` (transitional only)
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
- when Gaia backbone pilots are available, comparisons should prefer Gaia-ID overlap first

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
6. Update source precedence only after comparison notes and acceptance rationale are written.

Multiplicity crossmatch example:

```bash
cd /srv/spacegate/app
scripts/multiplicity_crossmatch.sh --catalog wds --catalog msc --catalog orb6
```

Interpretation notes:

- Random sample is deterministic by `sample_key + seed`, so reruns stay comparable.
- Overlap sample is biased toward rows that intersect current core IDs or normalized names.
- Lack of overlap does not necessarily mean low quality; it can also indicate missing identifier harmonization or a different object class.
- `catalog_eval` overlap counts are exact-key only. Coordinate-led multiplicity catalogs such as `WDS` need the separate crossmatch report before they can be judged fairly.
- For Gaia-first decisions, prioritize evidence in this order:
  1. exact Gaia-ID overlap and quality coverage
  2. deterministic crosswalk overlap (HIP/HD/other stable IDs)
  3. confidence-scored coordinate overlap
- Security reminder:
  - if a source requires insecure transport or unstable routing, treat it as optional/deferred until a verified mirror or integrity path is established.
