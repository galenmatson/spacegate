# Smart Tag Subject Placement Verification

Date: 2026-08-06

Status: locally accepted on `feature/smart-tag-application-v1`. No Proton or
antiproton state was mutated.

## Result

M8.3e.2b closes the gap between object-scoped assignments and their public
presentation. The System Page hero remains a bounded DISC summary. A lower All
Tags section groups direct assignments by their real system, star, planet, or
reviewed hierarchy leaf; source references remain separate. Hierarchy and
simulation object lists can display nonredundant subject-local tags, and every
inspector retains the subject identity needed for focus and concept return.

The normal read path uses `spacegate.smart_tags.v4` and
`spacegate.smart_tag_subjects.v1`. Its integer-keyed
`subject_tag_assignments` table contains 5,546,540 rows. Complete assignment and
source evidence remains in portable Parquet and is read only by the explicit
bounded audit endpoint.

## Scientific A/B

The reviewed stellar badge overlay covers 5,312 systems and 16,167 hierarchy
leaves. For those systems, compiler v2.7 evaluates the visible overlay leaves
instead of incomplete flat-star rows. This general policy produces 11,431,099
assignments and 11,426,038 system memberships. Relative to v3 it adds 7,557
system/class memberships and removes 127 flat-row class memberships that are
not present on accepted visible leaves. Two affected systems have no remaining
stellar class tag because every visible leaf is still unclassified. No
nonstellar membership, canonical object, hierarchy, planet lifecycle state, or
science-layer record changed.

The machine A/B is:

```text
/data/spacegate/state/reports/smart_tag_subject_placement_v1/20260806/scientific_ab.json
```

## Determinism And Size

Two clean full builds completed in 340.803 and 343.099 seconds. Their SQLite,
assignment Parquet, source-contribution Parquet, logical table hashes, input
lineage, counts, registry hash, and compiler version are identical.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Hot SQLite | 682,602,496 | `523d160762d598ff9937da0721cb15b2ad725aff2047d7edfa07d8fe34df1ea8` |
| Assignments Parquet | 295,377,586 | `39671f3ee8407a650c298a040e527b1b334b96f092faa4640622509084a7726b` |
| Source contributions | 763,812 | `7c3d655b2208c5939278637449bb7a8f68da27dd331edaa43fce49b1288b953f` |

The hot artifact grew 260,624,384 bytes over v3 and remains 928.0 MB (885.0
MiB) below the 1.5-GiB gate. The deterministic comparison is:

```text
/data/spacegate/state/reports/smart_tag_subject_placement_v1/20260806/determinism.json
```

## Runtime And UI

A warm indexed sample of 300 single-system reads across overlay systems,
planet hosts, and singleton systems measured 0.172 ms p50, 0.237 ms p95,
0.305 ms p99, and 0.479 ms maximum on Photon. The live Castor detail response
returns one public-named system subject and seven distinct stellar leaves.

The focused Python suite passes 36 tests. The repository-wide suite records 529
passes and five existing Evidence Lake accounting failures outside this diff:
one source batch test expects 26 rather than 27 completed sources, one
disposition audit does not know the selected IAU 2015 Resolution B3 source, the
completion contract still requires retention-removed E6/E7 artifacts, and two
derivation-inventory checks expect `planet_environment_badge_mask_v1` while the
checked-in map compiler declares v2. These remain explicit checklist work and
were not bypassed here.

The complete Smart Tag Playwright run
passes 20 applicable desktop/mobile checks with four intentional profile skips.
The final naming-focused run passes both device profiles. Screenshots and
machine reports are retained under:

```text
/data/spacegate/state/reports/map_playwright/smart-tag-subject-placement-full-20260806/
/data/spacegate/state/reports/map_playwright/smart-tag-subject-placement-final-20260806/
/data/spacegate/state/reports/smart_tag_subject_placement_v1/20260806/
```

## Promotion And Rollback

Photon `current` points to the v4 generation suffixed `.v4`; the unsuffixed v3
generation remains untouched as rollback because the vocabulary registry hash
did not change across the schema/compiler upgrade. The API and web containers
are healthy at `https://10.0.0.12`. The release packager accepts only the v4
schema/compiler contract, and its local 452,101,060-byte rehearsal archive
verified successfully. No public deployment was performed.
