# E6 v7 Scientific A/B Review - 2026-07-22

## Decision Candidate

Compare stability build `20260717T0614Z_f452835_side` with unserved Evidence
Lake shadow `e6_95e7af54d69f3d9602d81e5b_shadow` and public projection
`e6_95e7af54d69f3d9602d81e5b_public`.

Recommendation: accept E6 v7 science for the local E7 promotion and rollback
drill. Do not deploy to antiproton. E7 remains incomplete because the shadow
compiler still composes permanent inventory and identity from the stability
databases; local promotion validates products, not retirement of that bootstrap
dependency.

## Preserved Invariants

- 5,869,091 systems, 5,874,636 stars, and 6,311 canonical planets are unchanged.
- Canonical hierarchy inventory and planet lifecycle integrity are unchanged.
- The expanded independent shadow audit passes 373 checks.
- Clean isolated reproduction passes all generated/mutated logical table hashes
  in 5:51.12. DuckDB container-byte differences are diagnostic, not scientific.
- Public 100/250/500/1,000-ly tile membership, counts, names, representative
  classes, and complete parsed tile arrays equal the browser-tested v6 product.

## Intended Scientific Changes

- Selected-fact v15 contributes 123,288,872 evidence-backed facts with exact
  lineage and release-scoped component policy v9.
- Selected stellar consumers fill 2,683,321 distances, 2,048,889 luminosities,
  1,273,733 masses, 2,048,889 radii, and 357,925 temperatures that were null in
  the stability projection.
- Display classification changes 338,681 leaves, moves 885 unknown leaves to a
  known class, and moves zero known leaves to unknown. Direct/source evidence
  remains distinct from temperature, color, and mass presentation priors.
- Component evidence supplies 5,519 direct spectral classifications and 8,422
  explicitly labeled mass-based priors without named-system transforms.
- Coolness is deterministically rescored from the accepted profile. Inventory,
  score bounds, rank validity, build identity, and profile identity all pass.

## Legacy Difference Accounting

The stability projection has 711 populated stellar values where the selected
public projection chooses no replacement scalar: 260 luminosities, 138 masses,
310 radii, and three temperatures. Every row has a selection decision and an
acceptable retained lower-authority coherent alternative; no acceptable
higher-authority alternative loses, and no selected fact is missing from ARM.
Two rows differ from the current source release and are labeled source-release
supersession. This is evidence-preserving authority selection, not source-data
deletion.

## Compact Objects

E5 compact build `f0d7273f65371efeda365611` creates 4,425 permanent non-Gaia
release identities: 4,394 ATNF pulsars and 31 McGill magnetars. Source-distance
interval policy accounts 22 accepted, 421 excluded, and 3,982 missing outcomes;
156 selected facts retain exact E4 lineage. The J0437-4715 optical-companion
route remains quarantined and is never merged. Compact identities are copied to
full ARM but are not promoted into canonical stellar inventory or public map
objects; presentation remains a later objective.

## Product And Performance Gates

- E6 compile: 3:35.37, 36.43 GiB peak RSS, no swap or spill.
- Public slice: 4:37.57 with exact inventory parity.
- Four-radius map tiles: 4:38.87; verification: 17.29 seconds.
- Bounded scenes: 24 generated in 34.42 seconds, 24 reused in 1.14 seconds.
- Alias gate: 0.74 seconds; API integration: 41.11 seconds; strict known-system
  search/detail/hierarchy/simulation benchmark: 39.68 seconds.
- Production browser acceptance is inherited through exact parsed tile-payload
  equivalence with v6 and no frontend change; v6 passed twelve applicable
  Playwright cases and all 312 fixed performance checks without raised budgets.

## E7 Preconditions

Before local promotion, freeze the repository revision and reports and preserve
both stability and v6/v7 review artifacts. The fail-closed retention dry-run
accounts the superseded v5 shadow as 18,582,962,176 reclaimable bytes, but no
apply has occurred and the v5 public product remains outside that contract. Then
atomically promote v7 locally, rebuild/restart Photon containers, run production
topology smoke and browser identity checks, roll back to the stability build,
verify it, and re-promote v7. No legacy compiler path or artifact is retired
until the clean pinned-input Evidence Lake entrypoint no longer depends on the
stability CORE/ARM/hierarchy/DISC products.
