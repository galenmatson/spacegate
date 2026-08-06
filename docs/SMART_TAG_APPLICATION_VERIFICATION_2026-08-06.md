# Smart Tag Application and Hero Salience v1 Verification

Status: local Photon candidate accepted for branch review. No antiproton
deployment was performed.

## Verified Contract

- registry version: `2026-08-06-v2`
- registry hash: `79ad0373cd586867e821537211f50b7b516166eb5637c2d6544543fdaf085f13`
- compiler: `spacegate.smart_tags_compiler.v2.6`
- manifest: `spacegate.smart_tags_manifest.v3`
- hot tag schema: `spacegate.smart_tags.v3`
- Public Read build: `20260804T1130Z_68fd99b_a2_planet_badges`

Application policy resolves all 34 active definitions. The feasibility report
accounts for all 137 editorial proposals. No deferred family was activated and
the compiler reports zero quarantines.

## Full Build Results

| Metric | Result |
| --- | ---: |
| assignments | 11,419,175 |
| system memberships | 11,418,608 |
| exact source contributions | 90,771 |
| bounded system/source rows | 37,474 |
| hero selections | 183,047 |
| systems with hero selections | 182,885 |
| hero candidates before composition | 186,941 |
| hot SQLite | 421,978,112 bytes |
| assignment Parquet | 294,888,664 bytes |
| source contribution Parquet | 763,812 bytes |

Hero rows divide into 5,247 architecture, 175,835 exceptional-science, and
1,965 planetary/environment selections. Verification rejects more than four
rows per system, family-limit violations, invalid claim modes, missing origin
keys, or hero rows without a corresponding system membership.

## Determinism

Two clean compiles to independent output roots completed in 285.425 and
287.297 seconds. The comparison passed for build and compiler identity, input
lineage, counts, registry hash, every logical table hash, and byte SHA-256 for
SQLite plus both Parquet artifacts.

Machine report:

`$SPACEGATE_STATE_DIR/reports/smart_tags/20260804T1130Z_68fd99b_a2_planet_badges/smart_tag_application_v1_determinism_2026-08-06.json`

## Behavioral Gates

- API exposes resolved application policy, claim grammar, complete membership,
  bounded hero selection, score signals, and exact member origins.
- Hero presentation suppresses source/evidence tokens and facts already carried
  by object glyphs while All Tags retains complete bounded membership and exact
  source tokens.
- Nonaccepted claim modes have visible text markers independent of color.
- The pinned inspector supports hover preview, keyboard, click, touch, Escape,
  focus return, bounded history, concept navigation, member-object focus, page
  scroll return, and existing map-return token camera restoration.
- AAA adjudication packets require evidence, counterevidence, model identity,
  confidence, alternatives, reviewer, decision, and revisit triggers.

Focused Python integration, release, Public Read, and capacity-harness tests
pass. The final desktop/mobile/high-resolution Playwright run passes 18 tests
with four intentional project-specific skips. The frontend production build also
passes. The known npm
`globalignorefile` warning and existing bundle-size advisory are unrelated to
this milestone.

Production-dependency audit reports `GHSA-qwww-vcr4-c8h2` against React Router
7.18.1. The advisory concerns React Server Component action execution;
Spacegate's public web build is a client-rendered Vite SPA and does not use that
path. npm currently offers only a breaking downgrade, while its current latest
7.18.2 remains in the reported range, so this checkpoint records the advisory
without making an unrelated routing migration.

## Runtime Capacity Delta

The build-matched Smart Tag application campaign supplements the prior complete
M8.3e.2 capacity campaign. It runs the v3 registry, definition, indexed filter,
system hero, and bounded assignment endpoints under the same 6-vCPU/12-GiB
model.

| Profile | p95 | Throughput |
| --- | ---: | ---: |
| constrained registry/definition c1 | 6.1 ms | 322.4 requests/s |
| constrained tag filter c1 | 70.2 ms | 16.0 requests/s |
| constrained system tags/evidence c1 | 33.7 ms | 102.2 requests/s |
| constrained mixed c12 | 1.977 s | 20.8 requests/s |
| staircase c12 | 1.874 s | 21.2 requests/s |
| recovery | 65.7 ms | 40.2 requests/s |

All six profiles and the c1/c4/c8/c12 staircase pass with zero errors,
timeouts, OOMs, or safety stops. Peak aggregate constrained container memory is
178,307,072 bytes. The machine campaign is retained at:

`$SPACEGATE_STATE_DIR/reports/runtime_capacity_gate/smart_tag_application_v1/20260806/`

## Boundaries

The application and hero projection is DISC-owned. It does not mutate CORE,
ARM, RIM, Public Read, coolness, canonical counts, search membership, or
scientific assignment truth. Future-remnant, tidal-locking, resonance, rogue,
uncertain-emission, hazard, and comparable judgment-sensitive families remain
deferred pending an accepted reusable policy or reviewed AAA adjudication path.
