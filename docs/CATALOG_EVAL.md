# Catalog Evaluation Workflow (Gaia-First)

Use `scripts/evaluate_catalog_contribution.sh` for the full end-to-end evaluation run before changing source policy.

Default behavior:

- fetches deterministic sample inputs (including SBX sample slices)
- runs catalog scoring across canonical, support, and candidate sources
- writes reports under `$SPACEGATE_STATE_DIR/reports/catalog_eval/<run_id>/`
- emits:
  - `summary.md`
  - `summary.json`
  - per-catalog `*_summary.json`
  - per-catalog `*_random_sample.csv`
  - per-catalog `*_overlap_sample.csv`

Built-in evaluated catalogs currently include:

- `gaia_dr3_non_single_sample`
- `gaia_dr3_nss_two_body_sample`
- `gaia_dr3_sample`
- `athyg` (transitional only)
- `debcat`
- `kepler_eb`
- `msc`
- `nasa_exoplanet_archive`
- `orb6`
- `sbx_sample`
- `wds`

Primary command:

```bash
cd /srv/spacegate/app
scripts/evaluate_catalog_contribution.sh
```

This generates a ranking with these tiers:

- `indispensable`
- `strong`
- `situational`
- `meh`
- `needs_crossmatch` (coordinate-led catalogs needing multiplicity crossmatch interpretation)

If you want manual step control:

```bash
cd /srv/spacegate/app
scripts/fetch_catalog_samples.sh --sample-size 100
scripts/catalog_eval.sh --sample-size 100
```

Evaluate a specific catalog:

```bash
cd /srv/spacegate/app
scripts/catalog_eval.sh --catalog athyg --sample-size 100
```

Recommended workflow:

1. Run `scripts/evaluate_catalog_contribution.sh`.
2. Review `summary.md` ranking and per-catalog tier rationale.
3. Check per-catalog overlap sample CSVs for false positives/false negatives.
4. For multiplicity catalogs (`wds`, `msc`, `orb6`), run `scripts/multiplicity_crossmatch.sh` before final tier decisions.
5. Update source policy only after acceptance rationale is documented.

Multiplicity crossmatch example:

```bash
cd /srv/spacegate/app
scripts/multiplicity_crossmatch.sh --catalog wds --catalog msc --catalog orb6
```

Interpretation notes:

- Random sample is deterministic by `sample_key + seed`, so reruns stay comparable.
- Overlap sample is biased toward rows that intersect current core IDs or normalized names.
- Low overlap does not necessarily mean low quality; it can indicate missing ID harmonization or a different object class.
- `catalog_eval` overlap counts are exact-key only. Coordinate-led multiplicity catalogs such as `WDS` need the separate crossmatch report before they can be judged fairly.
- For Gaia-first decisions, prioritize evidence in this order:
  1. exact Gaia-ID overlap and quality coverage
  2. deterministic crosswalk overlap (HIP/HD/other stable IDs)
  3. confidence-scored coordinate overlap
- SB9 policy:
  - SB9 is superseded by SBX and is not part of default evaluation or ingest policy.
  - SBX is now the default spectroscopic-binary ingest source; keep SB9 for historical reproducibility only.
- Security reminder:
  - if a source requires insecure transport or unstable routing, treat it as optional/deferred until a verified mirror or integrity path is established.
