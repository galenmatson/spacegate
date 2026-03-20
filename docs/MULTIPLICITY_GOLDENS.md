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
  --core-db /data/spacegate/data/out/<build_id>/core.duckdb \
  --arm-db /data/spacegate/data/out/<build_id>/arm.duckdb \
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
- required quantitative regression check (core): Nu Scorpii total star count
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
