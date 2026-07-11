# Multiplicity Golden-System Exam

This document defines the post-ingest "exam" for multiplicity hierarchy quality.

Goal:
- detect regressions in hierarchical system reconstruction
- verify orbit-edge coverage and animation readiness for known benchmark systems

Primary benchmark:
- Castor (alpha Gem) with six stellar components: `Aa`, `Ab`, `Ba`, `Bb`, `Ca`, `Cb`

## Required Output Shape (Castor)

Hierarchy expectation:
- top-level Castor system
- AB subsystem and C (YY Gem) subsystem
- inner pairs: `Aa-Ab`, `Ba-Bb`, `Ca-Cb`

Minimum exam checks:
- component count: exactly 6 stellar leaf components
- all component labels present
- all three inner binary pair edges present
- nested containment edges for Castor AB/C, A/B, A->Aa/Ab, B->Ba/Bb,
  and Cab->Ca/Cb
- MSC source orbital-solution periods for Aa-Ab, Ba-Bb, Ca-Cb, A-B, and AB-C
- evidence/provenance populated on hierarchy and orbit edges
- confidence tier not below configured floor

## Validator Entry Point

Script:
- `scripts/verify_multiplicity_goldens.py`

Fixture:
- `scripts/fixtures/multiplicity_goldens.json`

Default behavior:
- validates against promoted build unless explicit DB paths are passed
- expects arm graph tables when `--require-arm` is used
- `scripts/verify_build.sh` runs this exam by default and passes `--require-arm`

Example:

```bash
scripts/verify_multiplicity_goldens.py --require-arm
```

Explicit build paths:

```bash
scripts/verify_multiplicity_goldens.py \
  --core-db "$SPACEGATE_STATE_DIR/out/<build_id>/core.duckdb" \
  --arm-db "$SPACEGATE_STATE_DIR/out/<build_id>/arm.duckdb" \
  --fixture /srv/spacegate/app/scripts/fixtures/multiplicity_goldens.json \
  --require-arm
```

## Pass/Fail Policy

- Any failed golden system check fails the exam.
- Missing arm tables fails when `--require-arm` is set.
- Exam output is machine-readable JSON plus human-readable summary lines.

## Current Scope

Current fixture scope:

- required hierarchy exam: Castor
- required presence/regression checks (core): 55 Cnc, GJ 667 C, TRAPPIST-1, 16 Cyg B
- required quantitative regression check (arm): Nu Scorpii source-native MSC stellar leaf count
- required quantitative regression check (arm): Castor source-native MSC stellar leaf count remains six
- required quantitative regression check (arm): V1054 Oph hierarchy-reachable
  source-native MSC stellar leaf count remains five, excluding orphaned/conflict
  endpoint rows such as the current MSC `D` branch from render membership
- required quantitative regression check (arm): 70 Oph has an MSC source A/B
  orbital solution normalized into `arm.orbital_solutions`
- required quantitative neighborhood check (core): minimum nearby-system count within 10 ly
- optional presence checks (galaxy/halo scope): PSR B1620-26, TYC 7037-89-1

Planned expansion set:
- Sirius (A/B, remnant handling)
- Alpha Centauri (A/B + Proxima relation handling)
- AR Cassiopeiae / HD 221253 as a septuple-confidence benchmark with one excluded background component (`E`)
- Nu Scorpii / HD 145502 as a septuple benchmark with explicit subsystem-label validation
- selected Sol-neighborhood systems used in manual QA

Adjudication/watchlist set:
- HD 235299 / WDS 20379+5106 as a catalog-backed high-multiplicity candidate that currently lacks strong literature/name authority
- Gamma Cassiopeiae / HD 5394 as a literature-ahead-of-catalog case where WDS/MSC support only a conservative core multiplicity while broader common-proper-motion claims remain soft
- Beta Monocerotis / HD 45725 as an over-inference risk: public descriptions consistently treat it as a triple while current WDS/MSC reconstruction expands it to six

Current guardrails:
- canonical hierarchy suppresses singleton MSC subdivisions, so systems like Beta Monocerotis no longer invent one-off `BA`-style child leaves from a lone role label
- MSC source endpoint labels should materialize as ARM support leaves before any
  count-expanded fallback labels are used, so systems like Nu Scorpii expose
  `Aa`, `Ab`, `Ac`, `B`, `C`, `Da`, and `Db` semantics without fabricating
  measured core stars
- source-native nonstellar endpoints remain support components without
  inflating benchmark stellar counts; Castor remains a six-stellar-leaf
  benchmark even when the MSC source preserves a low-mass `Cc` endpoint
- search/display should prefer matched member-star names when no system-level common-name alias exists, so variable-star lookups like `AR Cas` still surface the correct system card cleanly
