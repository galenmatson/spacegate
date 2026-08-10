# Selected Hierarchy Scientific A/B, 2026-08-09

## Decision

The candidate is a general correction to evidence scope and active hierarchy,
not a Tau Bootis exception. Permanent source identity remains preserved. Public
membership is compiled separately from currently accepted relation evidence.

## Root Cause

MSC uses case-significant component labels. `Ab` may be a stellar leaf while
`AB` is a subsystem. The permanent hierarchy had normalized these labels into a
case-folded key. It could therefore preserve an inferred leaf after the relation
that created it was rejected or reclassified as planetary context. Rebuilding
only simulation scenes hid the extra body in one surface but left map badges,
Public Read hierarchy, and detail consumers stale.

## Evidence Selection

Component policy v11 preserves 13 relations explicitly described by MSC as a
planet, planetary companion, or exoplanet as context-only evidence. It also
makes 15 linked parameter sets, 29 parameter facts, 11 classifications, 25 MSC
orbit rows, and one SB9 row nonselectable as stellar science. Raw and typed
source values, comments, and lineage remain intact.

## Hierarchy Delta

Selected-hierarchy build `75197558b76a7a448be4029f` starts from the permanent
hierarchy and current selected component evidence.

| Measure | Before | After | Delta |
|---|---:|---:|---:|
| Hierarchy nodes | 11,759,440 | 11,759,346 | -94 |
| Hierarchy edges | 5,886,947 | 5,886,853 | -94 |
| Canonical nodes | 11,750,038 | 11,750,038 | 0 |
| Canonical edges | 5,877,545 | 5,877,545 | 0 |

The 94 removed source-derived nodes affect 55 WDS systems:

- 22 planetary-context leaves;
- 2 planetary-context leaves with a case-folded group collision;
- 27 subsystem/group labels previously presented as stellar leaves;
- 40 leaves with no current selected relation endpoint;
- 3 source groups left empty after their unsupported leaves were removed.

Verification reports zero duplicate nodes or edges, missing parents or children,
unsupported MSC leaves, empty source groups, canonical node delta, or canonical
edge delta. The detailed per-node ledger is retained in the build's
`scientific_ab.json`.

## Public Inventory

The complete 1,000-ly candidate slice preserves the accepted inventory exactly:

| Public table | Accepted | Candidate | Delta |
|---|---:|---:|---:|
| Systems | 5,869,091 | 5,869,091 | 0 |
| Stars | 5,874,636 | 5,874,636 | 0 |
| Planets | 6,311 | 6,311 | 0 |
| Aliases | 1,026,480 | 1,026,480 | 0 |
| Search terms | 12,768,410 | 12,768,410 | 0 |

System, star, and planet stable-key set differences are all empty. Identifier
orphans, TIC collisions, sequential legacy keys, and canonical supplement rows
remain zero.

The retained-versus-candidate Public Read comparison also passes its reviewed
v2 gate. It removes 91 inferred stellar badges exactly matching the selected
hierarchy leaf ledger. Three canonical stars move from hierarchy overlays back
to ordinary base-star rows and remain present; 12 existing canonical stars
become visible in nested overlays. Of the retained badge identities, 157 move
from `UNKNOWN` to source or explicitly assumed classifications through the
general MSC, SB9, and DEBCat policies. No retained badge identity field,
immutable system field, or immutable planet field changes.

The 695 planet-category mask changes only add already compiled 3x3 planet
facets that the retained Public Read artifact predates. No mask bit is removed,
and no planet value or classification changes. The machine-readable report is
`public_read_scientific_ab_reviewed.json`; its comparator is reusable for later
selection-contract releases.

## Promotion Result

Full public build `e7_524b4c016779a77bc9780053_full_public` passed the local
promotion gate:

- all 14,141 required Public Read hierarchy bundles materialized with zero
  failures and SQLite integrity `ok`;
- exact map membership passed at 100, 250, 500, and 1,000 ly with zero missing,
  extra, name, or representative-class mismatches;
- all 7,719 policy-selected simulation scenes materialized under scene artifact
  v12 with zero failures and froze into an 80,643,691-byte immutable archive;
- the finalized Public Read scientific A/B gate passed every invariant and
  reviewed-presentation check;
- the normal build verifier, 65 focused compiler/API tests, and desktop/mobile
  Playwright checks passed against the locally promoted Photon runtime.

Tau Bootis now reports the same two stellar members and selected classes, `F`
and `UNKNOWN`, in map/search, System Page, OBJECTS, Stars and Hierarchy, and the
frozen scene. Tau Boo b is bound to the F star through its permanent host
identity. The source still lacks an accepted class for component C; its M-like
simulation material remains an explicitly illustrative visual prior, not a
stellar classification badge. Any unexplained inventory or cross-surface delta
continues to block later promotion.
