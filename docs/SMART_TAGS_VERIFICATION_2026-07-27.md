# Smart Tags v1 Verification

Status: pass

Completed: 2026-07-27 UTC

Branch: `feature/smart-tags-v1`

Scientific/Public Read build:
`e7_24cb15211f430a37f199f462_full_public`

Registry:
`80a761ba3eb2fff23f339e172275b668f25cade40c26921d93241bd1edc635ec`

Compiler: `spacegate.smart_tags_compiler.v2.2`

## Vocabulary Accounting

- 137 proposals: 52 enabled, 78 deferred, 6 rejected, 1 retired
- 22 proposal families: 8 enabled, 14 deferred
- 25 legacy surfaces: 6 enabled, 2 deferred, 3 retired, 13 rejected,
  1 compatibility-only
- 31 enabled definitions
- science, presentation, evidence, source, and reserved RIM semantics remain
  distinct even where they use the shared interaction shell
- observer/time/camera context remains nonpersistent and deferred

## Scientific And Source Accounting

- 5,869,091 systems evaluated
- 11,418,384 typed assignments
- 11,417,955 system memberships
- 3,260 hierarchical-system memberships from `hierarchy_nested_v2`
- 90,613 exact source contributions from 14,145 valid hierarchy bundles
- 37,472 bounded system/source summary rows
- 50 source presentation definitions
- 0 quarantine rows

The compiler does not infer source contribution from catalog-presence
booleans. In particular, `has_gaia_nss_evidence` no longer creates an invalid
Gaia NSS token on 5,866,306 systems. Source tokens mean an exact accepted
contribution to displayed lineage; their absence is not an assertion that the
Evidence Lake contains no evidence from that source.

## Artifact Results

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Hot SQLite | 407,265,280 | `4a0327ca46f9fa322f42ba193537aa6b113d00fcd153fa5dc2a9c7a219ae17b9` |
| Assignments Parquet | 294,656,487 | `9233f87f941bf83e92c82e154e65877314657672e14eb423560d45c182fd9d9a` |
| Source contributions Parquet | 756,799 | `434a827cc64e32ab189fafe91703f2591f502b92f4b12310bdd091ca96087c13` |

The hot database is 6,114,582,528 bytes smaller than the v1
6,521,847,808-byte baseline and uses 25.3% of the 1.5-GiB gate.

Two clean builds to independent roots completed in 228.781 and 228.515 seconds.
Database, both Parquet files, registry, inputs, counts, and all five logical
table hashes are identical.

Machine report:

```text
/data/spacegate/state/reports/smart_tags/e7_24cb15211f430a37f199f462_full_public/smart_tag_determinism_report.json
```

## Runtime Capacity

The pinned campaign models aggregate 6-vCPU/12-GiB resource quantity on Photon;
it does not claim identical OVH core speed.

| Profile | Requests | p95 | Requests/s | Peak container memory |
| --- | ---: | ---: | ---: | ---: |
| Photon mixed c12 control | 1,097 | 2.658 s | 17.9 | 228,126,720 B |
| targeted cold c1 | 2,266 | 89.3 ms | 37.7 | 153,894,912 B |
| exact TIC/name c1 | 7,698 | 10.1 ms | 128.3 | 154,906,624 B |
| tag filters c1 | 748 | 104.1 ms | 12.5 | 188,608,512 B |
| tag/summary/source c1 | 8,210 | 32.0 ms | 136.8 | 189,837,312 B |
| sustained mixed c12 | 2,185 | 2.747 s | 18.0 | 244,600,832 B |
| idle | 0 | 0 | 0 | 240,332,800 B |

The c1/c2/c4/c6/c8/c12 staircase passed every step. c12 completed at
2.720-second p95 and 17.4 requests/s with 245,510,144 bytes peak aggregate
memory. Recovery returned to 98.8-ms p95 and 35.8 requests/s. Across the
campaign there were zero request errors, timeouts, OOM events, safety stops, or
SLO failures.

Machine report root:

```text
/data/spacegate/state/reports/runtime_capacity_gate/smart_tags_v1/final_20260726_v2_2_r2/
```

## API, Browser, And Release

- 37 focused registry/compiler/API/Public Read/release/capacity tests pass.
- production Vite build passes.
- dedicated Playwright coverage passes seven desktop/mobile tests with one
  intentional mobile skip for the desktop-only 4K case.
- the final aggregate run
  `smart-tags-v1-full-parity-final-r2-20260727` passes 67 map, system,
  simulation, tile, Smart Tag, concept, theme, and accessibility checks with
  49 intentional project/environment skips and no unexpected failures.
- exact registry, definition, source, system membership, bounded assignment,
  any/all/exclude, unknown/nonfilterable, TIC/TOI, and mixed-filter contracts
  pass.
- keyboard, touch pinning, Escape, outside click, copy, Learn, Find More,
  desktop/mobile overflow, all eight concept routes, 4K, Simple Dark, and
  Simple Light tag presentation pass.
- the 374,478,854-byte Smart Tag release package is deterministic with SHA-256
  `80169a905eb96c0069bd80c6a48ffb79865e090c2e90d0e2c7cd12e3cb2e95bc`.
- the 33,963,449,859-byte four-artifact release verifies science, Public Read,
  scenes, and Smart Tags. Synthetic staging, activation, rollback, path,
  permission, identity, and corruption gates pass.

Normal Photon remains in compatibility mode with
`SPACEGATE_SMART_TAGS_REQUIRED=0`. Required mode passed in the isolated capacity
stack and is encoded in the rehearsed release contract. No antiproton
deployment occurred and Proton was not mutated.

## Editorial v2 Addendum

Completed: 2026-08-05 UTC

Public tooltip copy now follows the teaching contract in `docs/TAGS.md` and
`docs/SMART_TAGS.md`. Full definitions normally use three to six sentences to
define the term, build physical intuition, explain the accepted fact or screen
that activates it, and integrate uncertainty as part of the lesson. Compact
surfaces retain their separate `short_tooltip` contract.

All 34 enabled tag definitions and 50 source definitions were reviewed. The
deferred vocabulary was also corrected without enabling it; future-state tags
remain model predictions, observer-dependent tags remain contextual, and broad
classifications do not assert climates, hazards, origins, or inevitable
remnants.

Two clean compiles against Public Read build
`20260804T1130Z_68fd99b_a2_planet_badges` completed in 231.901 and 231.494
seconds. They reproduce registry hash
`8e2f35dfafe0c122ab8a1289d34ab7ae87f0a1ce6c790cdd4bc1ab82a25c8999`,
the 407,322,624-byte SQLite, both Parquet artifacts, input lineage, counts, and
all logical table hashes. Membership remains 11,419,175 assignments and
11,418,608 system rollups with zero quarantines.

Compiler v2.5 snapshots compiler and registry input hashes before work begins
and compares them again before promotion. A concurrent edit now fails the build
instead of allowing the manifest lineage to describe different bytes from the
registry actually compiled.

Machine report:

```text
/data/spacegate/state/reports/smart_tags/20260804T1130Z_68fd99b_a2_planet_badges/editorial_v2_determinism.json
```
