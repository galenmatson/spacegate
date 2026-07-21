# Spacegate Evidence Lake v2

Status: active main quest. E0-E2 completed July 18, 2026; registered E3
acquisition completed July 20, 2026; E4 typed scientific materialization is in
progress.

E0 checkpoint:

- `config/evidence_lake/source_releases.json` registers 44 active,
  transitional, expansion-pending, and planned source releases with domain
  authority, identity, retrieval, license, schema, and storage contracts.
- `config/evidence_lake/schema_baseline.json` pins 139 active manifest entries,
  5,962 machine-enumerated fields, and exact format contracts for source formats
  whose schemas live in official source documents. Three superseded SIMBAD
  pilot entries remain immutable but are separately checksum-declared rather
  than treated as active. The reviewed baseline fingerprint is
  `7e862068e7e4a18516e84bfc3a7d2e2dbb7dd0e4d780d1b62708869a33aa9752`.
- `scripts/evidence_lake_registry.py` emits registry/schema/field and storage
  audits. Full-refresh preflight now fails on unregistered sources, schema
  drift, missing active artifacts, or an acquisition-floor breach.
- Reference-aware retention preserved 11 served/published/rollback lineage
  builds and first reclaimed 196.21 GiB of unreferenced immutable builds. The
  later exact-hash legacy-build pass reclaimed another 364.82 GiB without
  touching raw, typed, report, or E4 artifacts. Photon has about 489 GiB free
  on `/data`, above the 300 GiB acquisition floor.
- Machine reports are under
  `/data/spacegate/state/reports/evidence_lake_v2/`.

E1 completion checkpoint:

- `scripts/evidence_lake_store.py` materializes content-addressed immutable raw
  snapshots and independently versioned typed Parquet snapshots. All 25
  available non-planned releases are snapshotted: 59 artifacts containing 403
  files and 10.41 GiB of active raw content.
- Shape-checked lexical CSV cooking, declared-schema MAST JSON cooking, source-
  specific parser versioning, row/hash verification, and atomic promotion are
  active. A NASA composite-table delimiter defect and a MAST null-batch schema
  defect now fail closed instead of producing plausible lossy tables.
- The AT-HYG v33 continuation contract preserves 2,552,165 rows and 34 fields
  across its header-bearing first part and headerless second part. This fixed an
  E0 audit error that had interpreted the first row of part 2 as a schema.
- Official WDS and CDS format documents now drive fixed-width parsing. MSC
  retains components, subsystems, orbits, and its previously stranded notes;
  ORB6 retains all 35 source fields; ATNF retains repeated parameters, source
  conflict comments, glitches, references, and an archive-member index; Green
  SNR retains uncertainty markers; and the Gaia EDR3 white-dwarf FITS table
  retains all 161 columns and its H/He/mixed atmosphere alternatives.
- The completed typed report contains 68 tables, 48,936,930 rows, 2,539 table-
  column occurrences, and 5,097,614,282 active Parquet bytes with zero pending
  tables. Verification accounts for all 59 raw artifacts.
- Parallel unordered DuckDB output initially reproduced the same rows and
  schemas with different Parquet hashes for large Gaia/TESS tables. Source-
  order-preserving single-thread serialization is now part of the parser
  contract. `scripts/verify_evidence_lake_reproduction.py` rebuilt all 25
  releases in a clean temporary root, matched every content/table hash, and
  removed the scratch tree.
- `config/evidence_lake/observation_product_policy.json` establishes metadata-
  first indexes, bounded checksum-addressed on-demand caching, source-host
  allowlisting, and explicit prohibitions on bulk Gaia/TIC/product mirroring.
  E3 acquires the actual product indexes; E4 gives them typed evidence tables.
- Targeted TESS parser contract v7 preserves archive-member lineage when one
  typed table combines multiple official crossmatch files. Typed snapshot
  `c41373862bf6d04c13acdb78` accounts 122,772 rows across TOI, target-set,
  MAST TIC, Gaia release-neighborhood, external-crossmatch, and targeted Gaia
  DR3 tables. All 137 external rows retain their exact `hip_` or `twomass_`
  member path; source namespaces are never guessed from identifier shape.
  Verification and clean raw-to-typed reproduction pass content hash
  `1f2b60e6f23d31f0ac8992dfd3cc4faeeede83eae154ce3b8bc0f8007c976b06`.

E2 completion checkpoint:

- `scripts/fetch_gaia_dr2_identity.py` derives a deterministic 1,542,049-ID
  Gaia DR2 target universe from every active DR2 fallback family and preserves
  155 exact TAP queries with 1,626,847 official neighborhood rows. Five targets
  have no official DR3 neighbor.
- `scripts/fetch_gaia_dr2_identity_reverse.py` independently queries all
  1,625,665 distinct forward DR3 candidates. Its 163 exact TAP queries preserve
  1,776,331 reverse rows, exposing predecessor merges that a forward-only
  subset cannot detect. Both acquisitions reproduce byte-for-byte through E1.
- The reviewed registry now covers 34 sources and 63 manifest artifacts with
  1,824 machine-enumerated fields. The consolidated active lake contains 27
  source releases, 72 typed tables, 55,507,822 rows, and 5,213,454,799 Parquet
  bytes with zero pending artifacts.
- `config/evidence_lake/identity_graph_policy.json` defines release-scoped
  identifier and scope semantics. `scripts/compile_evidence_identity_graph.py`
  consumes exactly 13 typed tables plus the current CORE as a labeled stability
  reference, never as new authority.
- Graph `c84389ad55f17081fff008b4` accounts for every DR2 target exactly once:
  226,392 accepted current-object bindings, 1,234,609 accepted release mappings
  excluded from the current canonical backbone, 79,671 DR2 splits, 1,372 DR3
  merges, and five missing targets. No Gaia DR3 canonical collision or
  forward/reverse payload conflict was found.
- Physical identity, containment, component/subsystem, observation-target, and
  alias/name claims are separate. The graph retains 5,877,462 current
  containment links as `stability_reference_not_new_authority` and 186,198
  MSC/WDS source relation claims as candidates; verification found zero source
  relation promotions into canonical containment.
- Every crossmatch edge, target outcome, and source-family binding carries its
  registered source, release, and typed-table lineage. Family-by-family
  accounting and duplicate-system guards pass; 18 accepted component bindings
  share a root system without collapsing their permanent star identities.
- Ordered Parquet tables and the graph database live under
  `/data/spacegate/state/derived/evidence_lake_v2/identity/<graph_id>/` with an
  atomic `current` pointer. The artifact is not served CORE and cannot mutate
  canonical inventory or hierarchy. Clean-compile comparison is recorded in
  `reports/evidence_lake_v2/e2_identity_reproduction.json`.

This plan replaces the narrow Catalog Evidence Utilization v2 rebuild with a
clean, release-scoped collection and evidence-compilation architecture. It
preserves Spacegate's canonical identity work, layer boundaries, immutable
build model, provenance rules, quarantine paths, and no-one-off policy.

The objective is not to accumulate the largest possible pile of catalog files.
It is to preserve the richest useful scientific evidence for Spacegate's
inventory, simulations, search, explanation, and future Astronomy Agency while
keeping every public choice reproducible and inspectable.

## Architectural Position

Spacegate will treat a convenient selected value as a compiled projection, not
as the source record itself:

`source release -> raw snapshot -> source-native typed rows -> identity and
scope reconciliation -> typed evidence -> versioned selection -> CORE/ARM/DISC
and public products`

The redesign must retain:

- permanent Spacegate object and system identities
- release-scoped catalog identifiers and provenance-bearing crossmatch edges
- conservative CORE inventory and ARM evidence ownership
- deterministic immutable builds, manifests, quarantine, and verification
- source/derived/assumed separation
- the prohibition on named-object transforms in production cooking

The redesign may replace collectors, cooked formats, evidence schemas,
selection policies, and build orchestration where the existing implementation
cannot satisfy these contracts.

## Data Stages

### E0. Source and Field Registry

Every source release must declare:

- stable source and release identifiers
- authority role by scientific domain; there is no universal source ranking
- retrieval endpoint, exact query or artifact URL, license, citation, and
  release cadence
- upstream schema snapshot and expected row/count/completeness checks
- identity namespace, epoch, coordinate frame, units, null/limit semantics,
  and quality flags
- a disposition for every upstream field: preserve, normalize, index-only, or
  deliberately omit with a reason
- schema-drift behavior and operational storage class

The registry must make uncollected and unused upstream features visible. A new
source release cannot silently drop or add scientifically relevant fields.

Implementation:

- `config/evidence_lake/source_releases.json`
- `config/evidence_lake/schema_baseline.json`
- `scripts/evidence_lake_registry.py`
- `tests/test_evidence_lake_registry.py`

### E1. Immutable Raw and Source-Native Typed Lake

Preserve byte-identical source files or TAP/API responses with retrieval
timestamps, queries, response headers where useful, checksums, row counts,
schemas, and manifests. Raw source artifacts are append-only and never rewritten
to look like a later release.

Original FITS, VOTable, CSV, and compressed artifacts remain authoritative.
Large remote products such as spectra, light curves, and survey images may use
an index-plus-on-demand-cache policy when full mirroring is not justified.

Normalize each source independently into typed Parquet/Arrow. Do not merge
catalogs or choose winners at this stage. Preserve source record identity,
parameter-set/reference grouping, component scope, flags, uncertainty, limits,
epochs, frames, original units, and normalized units.

Use partitioning that matches actual access patterns and avoids small-file
explosion. DuckDB is a compiler/query engine over these artifacts, not the only
durable scientific representation.

### E2. Identity and Scope Graph

Catalog identifiers are nodes scoped by source and release. Authoritative or
reviewed crossmatches are typed edges with method, confidence, provenance, and
outcome. Gaia DR2 and DR3 identifiers must never be compared as interchangeable
values.

Identity compilation must separately resolve:

- physical object identity
- system membership and containment
- component or subsystem scope
- observation target scope
- aliases and public names

Every attempted target must end accepted, missing, excluded, ambiguous, or
quarantined with an explicit reason. Source relation claims do not become
canonical containment merely because they exist.

Implementation:

- `config/evidence_lake/identity_graph_policy.json`
- `scripts/fetch_gaia_dr2_identity.py`
- `scripts/fetch_gaia_dr2_identity_reverse.py`
- `scripts/compile_evidence_identity_graph.py`
- `scripts/verify_evidence_identity_reproduction.py`

The official Gaia neighborhood table is an association-candidate table, not a
license to equate release identifiers. Automatic release reconciliation
requires one forward DR3 candidate and one reverse DR2 predecessor. A unique
release mapping binds to a permanent Spacegate star only when that DR3 ID has
one current canonical target. Valid mappings outside the current public
backbone are `excluded`, not identity failures; splits and merges remain
`ambiguous`; missing rows and conflicting or colliding evidence remain
explicit.

`proper_motion_propagation` records that Gaia applied epoch propagation. It is
not a high-proper-motion classification. E2 preserves that safeguard and
separately flags 812 accepted current stars whose canonical vector motion is at
least 500 mas/yr.

### E3. Foundational Source Acquisition

Acquire the bounded source program defined below through the source registry
and raw/typed contracts. Acquisition is complete only when every targeted
source record and upstream field is present or has an explicit omission reason.
Large observation products follow the metadata-first storage policy.

E3 acquisition checkpoint (completed July 20, 2026):

- `config/evidence_lake/e3_acquisition_program.json` and
  `scripts/evidence_tap_acquire.py` define exact schema-accounted TAP products,
  deterministic partitions, resumable response sets, checksums, row/MAXREC
  gates, complete UWS attempt lineage, pre-retry abort of nonterminal jobs,
  bounded read-stall recovery, inter-process manifest locking, and atomic
  promotion.
- `config/evidence_lake/e3_http_sources.json` and
  `scripts/evidence_http_acquire.py` provide content-addressed, Range-resumable,
  checksum-gated release-file acquisition with a socket-inactivity timeout
  independent of the overall job budget. WGSN and all six registered GCVS
  artifacts are pinned, raw-snapshotted, typed, and verified. The WGSN cook
  validates its declared 16-field table contract and preserves 597 names,
  source row identity, and linked resources while explicitly excluding the
  page footer and unrelated calendar table.
- GCVS's documented fixed-width rows use structural trailing `|` separators in
  four slices. Registry-controlled parser v2 removes exactly one such separator
  and remaining layout padding while preserving internal `|` classifications
  and the exact `raw_row`. Typed snapshot `ef540a47c43892e17ddc2bae` accounts
  203,740 normalized cells across `Exists`, `Ident`, `VarName`, and `f_NSV`.
  A typed A/B report proves no other cell, row, schema, or raw checksum changed;
  verification and clean reproduction pass content hash
  `7e6b3cd985b8df6df7b25eb43949ae112e3eea32ad094cc3b90ce6972639ff20`.
- The hard-parallax branch of the Gaia 1,250-ly envelope contains 31,987,126
  source rows and is published with all 152 `gaia_source` columns. A disjoint
  supplement selects only Gaia DR3 rows outside that branch through the Gaia
  Archive's hosted `external.gaiaedr3_distance` relation when the geometric
  posterior median is within 383.245 pc or its lower 16th-percentile bound is
  within the 306.601-pc public sphere. Bailer-Jones values remain a separate
  evidence source; the selection join does not merge them into Gaia rows.
- The uncertainty branch acquisition is complete at 189,145 rows. All 127
  partitions account exactly, the largest contains 1,592 rows against a
  400,000-row cap, and the hard/uncertain union therefore contains 32,176,271
  source-native Gaia rows. Raw snapshot `fcd1f77edf401a7e19c72197` preserves
  the two branches as separate 152-field tables. Typed snapshot
  `35a41010cf74f950e61b5412` preserves all rows in separate Parquet tables and
  passes raw/typed verification. Clean reproduction from `/mnt/space` matches
  both tables, typed snapshot identity, and content hash
  `1e8db7b0971badce3141dac2296bfd34b7c57135f5f58e0a83bbcd81b9f16a35`;
  the scratch tree was removed after the durable report was written.
- Fifteen Gaia-derived AP, supplementary-AP, NSS, variability/rotation, and
  official external-crossmatch products have explicit disjoint posterior-
  overlap companions. The first archive-side three-way join plans hit Gaia
  VMEM limits or remained nonterminal under 3-, 7-, and 31-way partitioning.
  `scripts/build_gaia_uncertainty_target_seed.py` therefore derives the exact
  189,145-source DR3 target set from the accepted uncertainty-envelope typed
  table, verifies its Parquet checksum, and publishes content-addressed seed
  `638c3ff4e58abcd355029e0f`. The nine remaining products query their source
  tables directly through 31 checksum-pinned target buckets; this changes query
  execution, not scientific membership.
- The acquisition report passes with 56/56 products, 170,253,376 rows,
  23,970,068,085 response bytes, and no pending product. Every direct-target
  product records seed build, artifact/value hashes, exact coverage, 189,145
  values, and all 31 nonempty buckets in its immutable manifest.
- The program accounts for all 764 upstream columns across `gaia_source`, AP
  main and supplementary, NSS orbit, variability summary, and rotation-
  modulation tables. The five expanded Gaia source releases are raw-
  snapshotted and typed into 30 Parquet tables, 83,908,762 rows, 1,320 column
  occurrences, and 6,575,792,259 bytes. Per-release verification and clean-state
  reproduction pass for snapshots `1f13c88951b996b95e702913`,
  `c80bde75b53fb38389c242a2`, `ba9869a742ae9a00aeda0bc2`,
  `4c00e4b5b40a8f32c56a4459`, and `17cf207d8471b3ec00e1cb07`.
- Expanded NSS preserves 87,075 complete 77-column orbit rows. The hard branch
  uses authoritative `gaia_source.parallax` and contains 85,724 rows; the
  checksum-bound uncertainty branch contributes 1,351 disjoint rows. The
  complete NASA acquisition is also typed and verified: 12 planet, host, TOI,
  K2, Kepler-name, KOI, TCE, and transit-detection tables preserve 206,989 rows
  and all 2,093 upstream fields with zero omissions; a clean raw-to-typed
  reproduction matches the promoted typed hashes. The bounded Bailer-Jones
  EDR3 distance release is also immutable, typed, and clean-reproducible:
  17,310,560 rows preserve all 10 fields. Its typed schema records
  deterministic aliases for case-only lower/upper percentile name collisions
  while retaining the exact VizieR names in lineage. Hunt-Reffert is complete
  at this layer with 7,167 cluster, 1,291,929 membership, and 29,956 crossmatch
  rows across 161 field occurrences. These are source-native facts; E4/E5 still
  own scope and scientific selection. E3 source acquisition is complete; E4
  still owns normalization, scope, evidence contracts, and scientific
  selection for these source-native tables.
- SIMBAD is deliberately staged rather than mirrored: acquire the release-
  pinned Gaia DR3 identity bridge, intersect locally with the Evidence Lake
  envelope, then request basic, alias, and bibliography evidence for the
  matched object set. SIMBAD remains identity/naming evidence, not a canonical
  inventory catalog.
- The hard-envelope SIMBAD pilot proved the staged contract end to end. Of
  1,807,040 matched SIMBAD objects, 64 were absent from the bounded basic slice;
  checksum-bound targeted queries added their 64 basic rows, 293 aliases, and
  173 bibliography links. Raw/typed snapshot `716d91848006667527d3e588`
  preserves 35,088,164 source-native rows across all eight pilot tables and
  cleanly reproduces.
- The complete 32,176,271-row Gaia union has now regenerated final SIMBAD target
  seed `8d940fdc1bc8eee0dc8efa7e`: 14,188,016 Gaia-bridge rows bind 1,831,202
  target bridge rows to 1,831,201 SIMBAD objects. Of those, 24,218 are absent
  from the base basic-data slice. The acquisition compiler checksum-pins that
  exact list and deterministically divides it into 31 modulo buckets of
  732-835 object IDs; basic, alias, and bibliography queries retain separate
  content identities and independent response caps. All 93 targeted queries
  completed with 24,218 basic rows, 140,962 identifiers, and 68,928
  bibliography links. Immutable raw snapshot `7e251164da42ef2a93627d84`
  and typed snapshot `55a9bfcaaa943ddd035df3ab` preserve 35,321,742 rows
  across the eight active tables. Raw/typed verification and clean scratch
  reproduction pass content hash
  `d7b78dd6cb77e5ee2cd9c03771e1e7b893bb7439aa8d2489a95442c7e1182100`.
- The 7,862,084-row Gaia AP multiple-object-analysis table initially saturated
  every 400,000-row response under a 17-way partition. The reviewed 31-way
  contract completes exactly at 252,595-254,593 rows per partition; canonical
  acquisition snapshot `9a262636fd0c7b48d8063169` replaces the transient
  partition experiment without changing source rows.
- The El-Badry, Rix, and Heintz Gaia EDR3 wide-binary release is pinned and
  typed as source-native evidence: 1,817,594 catalog rows across 217 columns,
  517,993 shifted-control rows across 201 columns, and both published method
  scripts. Immutable snapshot `aea36fe5a6753de90be33301` passes schema/row
  verification and clean typed-hash reproduction. E4 now performs the local
  envelope intersection and relation materialization described below.
- The complete staged SIMBAD identity/naming/bibliography slice, expanded Gaia NSS,
  GALAH DR4, and all three LAMOST DR11 stellar releases pass immutable
  raw-to-typed verification and clean reproduction. APOGEE DR17 also passes
  after adding a reusable, schema-gated multi-HDU FITS adapter: its 733,901-row
  234-field allStar table, model-grid metadata, and field-version metadata are
  separate typed tables, and fixed-size FITS arrays remain typed arrays.
- TAP field selection quotes nonregular upstream identifiers and emits explicit
  regular output aliases while retaining the exact upstream-to-output mapping.
  This preserves VizieR names such as `CMDCl2.5` without weakening response-
  schema validation or misclassifying aliased fields as omissions.

### E4. Typed Scientific Evidence

Use domain tables rather than one universal wide row or an unconstrained EAV
store. Initial contracts should cover:

- stellar parameter sets and classification observations
- astrometry and distance estimates
- photometry and extinction observations
- spectra and observation-product indexes
- variability, activity, and rotation solutions
- binary/multiple relation claims and orbital solutions
- cluster membership and cluster physical context
- planet parameter sets, lifecycle evidence, and transit/RV observations
- compact-object observations and alternative model fits
- extended-object geometry and distance evidence
- citations, source documents, and product lineage

Every evidence row must retain the source record, object/component binding,
method or model, reference, epoch, uncertainty or bound semantics, quality
flags, and raw-to-normalized lineage.

E4 compiler checkpoint (July 19, 2026, in progress):

- `config/evidence_lake/e4_scientific_evidence.json` defines 23 bounded domain
  tables, controlled binding/mapping states, and data-driven source adapters.
  This is not a universal EAV store: each scientific family has its own typed
  contract and controlled quantity vocabulary.
- `scripts/compile_scientific_evidence.py` emits immutable, content-addressed
  builds under `derived/evidence_lake_v2/scientific_evidence/<build_id>/` with
  logical per-table hashes, source/field accounting, and explicit unresolved
  binding outcomes. `scripts/verify_scientific_evidence_reproduction.py`
  rebuilds into a temporary scratch root, compares deterministic logical hashes,
  and removes the scratch artifact; the NASA foundation reproduction passes.
  Logical verification uses `sha256_bucketed_multiset_v1`, which hashes every
  row, streams globally ordered fixed-width row hashes in 65,536-row batches,
  and deterministically hashes bounded prefix buckets. It is order independent
  and duplicate sensitive without aggregating or sorting full row JSON in
  memory. Hash sorting has a dedicated spill directory and fails if persistent
  DuckDB block allocation changes during verification.
  `scripts/verify_scientific_evidence_artifact.py` independently audits scope,
  identifier, probability/statistic, citation, uncertainty, parameter-set, and
  source-record integrity.
- External row-envelope membership is a compiler input, not an ambient local
  join. A selection target must name its registered source/release, exact raw
  and typed snapshots, typed content hash, table and field, and Parquet table
  hash. The compiler verifies the file beneath that exact typed snapshot,
  supports an explicit OR across release-compatible envelope tables, records
  the lineage in reports, and includes it in immutable build identity. Gaia IDs
  use unsigned-decimal normalization so storage type and leading zeroes cannot
  change membership. This general contract bounds APOGEE, GALAH, and LAMOST
  evidence without catalog-specific distance heuristics or bulk E4 admission.
- Compiler/contract v54 adds constant source references, explicit posterior-
  interval endpoint semantics, and dual typed/source-native field names. These
  are general evidence contracts: legal Parquet aliases no longer obscure an
  upstream case-only field distinction, and published percentile endpoints are
  not mislabeled as symmetric measurement errors. Bailer-Jones diagnostic
  `520df722a1564ee857b1ae43` accounts all 17,310,560 EDR3 distance rows and all
  10 source fields. It emits 17,310,560 release-scoped EDR3 identities and
  coherent distance bundles containing 33,225,308 geometric and photogeometric
  posterior measurements, each linked to `2021AJ....161..147B`. Copied Gaia
  coordinates remain preserved in E1 but are deliberately excluded from E4
  because release-native Gaia owns coordinate evidence. The build has zero
  pending fields and logical hash
  `b74aabea2625f660ab85e0b723d7598a4b6cd9af6010c196d51229f743e84381`;
  the independent generic artifact audit passes. The stricter source audit
  rejected all 17,310,560 rows because fields declared `exclude` were still
  redundantly copied into `source_context_json`. Compiler/contract v56 fixes
  that general disposition bug so `context` and `lineage` fields materialize
  there while `exclude` fields do not; excluded values remain in immutable E1
  typed storage. A v55 attempt was stopped before promotion when review showed
  that lineage fields must remain. Accepted v56
  `2147d1c60f6401fdc725d96e` passes compilation plus generic and source-specific
  audits with `nonempty_redundant_source_context=0`; it retains the same exact
  scientific counts and has logical hash
  `eceb390e97cba1b69d8a5780181b8947dfed6ed78c51167316ad4936b4506730`.
  Clean reproduction matches that logical hash with no differing sections and
  removes its USB scratch tree. v56 is the accepted checkpoint; neither v54 nor
  v55 is accepted.
- Compiler/contract v58 applies the checksum-bound external-membership contract
  to APOGEE DR17's explicit Gaia EDR3 identifiers. Checkpoint
  `efc517c3dd6f6389abab7603` retains 178,099 of 733,901 allStar rows inside the
  registered Bailer-Jones EDR3 envelope, plus all model-grid and field-version
  metadata. It emits 3,280,268 coherent ASPCAP stellar measurements,
  1,357,072 photometry/extinction measurements, 529,676 coordinate/RV
  measurements, 173,478 spectrum-product locators, and 890,495 release-scoped
  identifier claims. All 243 table-field occurrences are accounted as 211
  materialized and 32 reviewed exclusions with zero pending fields. Copied Gaia
  values and redundant ASPCAP arrays remain in E1 Parquet rather than becoming
  duplicate E4 facts. Generic and APOGEE-specific audits pass; the logical hash
  is `d2609ad76ea2ffc4f66d9bfd01c5fb7084aa0d88c937c513d8f416ebeced2a18`.
  The measured 41:53 compile and 9.17-GB peak RSS expose repeated wide-source
  scans and branch buffering; selected-row caching and incremental inserts must
  prove the identical logical hash before GALAH/LAMOST materialization.
- Compiler/contract v60 closes that scaling defect with an exact-hash-verified
  DuckDB temporary selected-row table and incremental branch insertion. APOGEE
  checkpoint `e794324a7c7e86e80a3ea614` has scientific-content hash
  `194eede6937b26f8c0cd508f6dd7dd0a39ef34b2a455000d1f57ee18c8a5f31b`,
  exactly matching the v58 projection across every table except the necessarily
  changing `evidence_build` metadata row. Runtime falls from 41:53 to 11:54 and
  peak RSS from 9.17 GB to 6.53 GB. The temporary cache accounts all 178,099
  selected rows with zero source-row hash mismatches and is removed after close;
  both generic and source-specific audits pass.
  Clean reproduction completes in 11:56 with no differing report sections and
  removes its `/mnt/space` artifact and compiler scratch after comparison.
- Compiler/contract v62 applies the same bounded path to GALAH DR4. Accepted
  checkpoint `a4fc03c66ea1cfb44c25df28` retains 117,885 of 917,588 allStar rows
  through an OR across the exact hard-parallax and uncertainty-supplement Gaia
  DR3 tables. It accounts all 184 fields as 169 materialized and 15 reviewed
  copied-Gaia/2MASS/AllWISE exclusions; emits 353,655 release-scoped identity
  claims, 4,052,282 coherent stellar/model measurements, 857,173 coordinate,
  distance, and RV measurements, 973,436 extinction/interstellar measurements,
  and 623,253 line/activity measurements; and leaves every object binding
  unresolved for E5. The first v61 build was rejected after the source FITS
  descriptions and official schema showed that `r_med/r_lo/r_hi` are distance
  estimates used for parallax gravity, not stellar radii. v62 preserves them as
  explicit distance median/lower/upper facts and keeps SB2 percentiles as three
  values rather than inventing symmetric errors. Generic and source-specific
  audits pass, including an explicit no-clamping gate for extreme source E(B-V)
  evidence. Clean reproduction matches logical hash
  `7c0a367810903b18dad7e408d3feade5821325bfa8a670b5e051e1534cded8db`
  with no differing sections and removes its external scratch. The allStar
  release has no source-native spectrum URL column, so spectrum-product locator
  acquisition remains an explicit E3 follow-up rather than a manufactured E4
  URL template.
- Compiler/contract v63 materializes all three registered LAMOST DR11 v2.0
  stellar products as independent observation evidence. Accepted checkpoint
  `a583819f0a4f3896c312f19e` retains 661,941 of 7,898,024 LRS stellar rows,
  496,415 of 926,048 LRS M-star rows, and 500,925 of 2,594,070 MRS stellar
  rows through the same checksum-bound Gaia DR3 hard/uncertainty-envelope OR.
  It accounts all 185 field occurrences as 170 materialized and 15 reviewed
  copied-catalog exclusions. LRS LASP physics, M-star molecular/activity
  diagnostics, and MRS LASP/CNN physics and raw/corrected arm/combined radial
  velocities remain distinct coherent contexts rather than field-wise
  composites. Official `obsid`/`mobsid` values provide on-demand spectrum
  locators without fabricated archive URLs. `gaia_source_id` is explicitly
  Gaia DR3; `uid`/`gp_id` catalog scope is preserved rather than conflated with
  Gaia identity. Generic and bounded LAMOST-specific audits pass, and clean
  reproduction matches logical hash
  `eeb6dd86c096100175dc92d829508c8c36636d20f507993750e1f9a0b5a73d37`
  with no differing sections and removes compiler scratch.
- The SIMBAD adapter adds grouped astrometry/distance measurement bundles,
  source spectral classifications, release-scoped identifier claims,
  authoritative bibliography rows, and object/reference links without selecting
  public winners. A diagnostic exposed an `OR` citation join whose physical
  plan estimated 199,495,267,914 intermediate rows. Compiler v36 replaced it
  with bounded equality matching and completed a 42.8-GB diagnostic artifact,
  but the independent audit correctly rejected 285 blank normalized HIP claims
  produced from component-suffixed aliases such as `HIP 10280A`; v36 is not an
  accepted checkpoint.
- Compiler/contract v37 records every failed numeric normalization in an
  explicit rejection table and emits only usable normalized claims. Its final
  SIMBAD diagnostic failed closed at the intentional 16-GB DuckDB limit while
  expanding all bundled astrometry citations in one join; the host did not OOM
  and no artifact was promoted. Compiler/contract v38 partitions that exact
  join into 32 deterministic source-record hash buckets and disables
  unnecessary insertion-order preservation while retaining explicitly ordered,
  duplicate-sensitive logical hashes. Operator-configured scratch still places
  disposable spill outside `/data`. Checkpoint `fc5bd4e6398d72bde50ba6d5`
  materializes all 161 registered field occurrences into 22,951,059 exact source
  records, 50,453,123 identifier claims, 435,079 source classifications,
  1,862,866 astrometry bundles, 465,243 citations, and 30,744,757 evidence links.
  All 285 component-suffixed HIP normalization failures remain explicit
  rejections rather than blank claims. The independent artifact audit passes,
  and clean reproduction matches logical hash
  `673cebbbfcc4055fb7a6a007824ba11eac75bcc7b038bb138a15abf6cf9288d7`
  with no differing sections and removes its external scratch tree.
- Compiler/contract v39 adds the official IAU WGSN naming adapter and permits a
  configured same-row citation link without a redundant `evidence_citations`
  field disposition. Required-link accounting now follows materialized source
  records, so row-selection exclusions cannot create false missing-link errors.
  Checkpoint `0ff30b04008b93aafb3de66f` accounts all 597 name records and 22
  fields as 3,847 scoped identifier claims, 91 meaningful source references,
  and 564 name-to-reference links. Raw `-`/`--` placeholders remain context and
  are not promoted as identifiers or citations. Shared HIP and Bayer values
  remain separate observation-target or system/component-ambiguous claims; they
  never merge objects or imply containment. Independent artifact and scope
  audits pass, and clean reproduction matches logical hash
  `512b05b67ca0632bbe164b82e1b96182643e9b4e911da6b8ce9d8bdba1d37fe5`.
- Compiler/contract v40 adds reusable source-native sexagesimal coordinate
  measurements, deterministic aggregation of repeated-key bibliography lines,
  predicate-scoped composite identifiers, and explicitly lexical configured
  evidence. GCVS checkpoint `a6f6669d2bd48eac5d6204d2` accounts all 340,839
  rows from the six typed release tables as 340,839 source records, 705,684
  identifier claims, 289,892 astrometric measurements, 29,042 source spectral
  classifications, 444,566 variability observations, 21,526 citations, and
  756,305 evidence links. GCVS/NSV component suffixes never assert the base
  numeric identity, and all object bindings remain unresolved for E2/E5.
  Source variability classes and stellar spectral classifications remain
  separate evidence families. The 1,020 NSV declinations whose source sign
  column conflicts with an embedded negative degree token preserve both raw
  fields and normalize using the embedded sign with an explicit quality flag.
  Independent artifact and source/scope audits pass, and clean reproduction
  matches logical hash
  `a4d78bb721d6017031a2e9a53e2b86701395d0c67ff0dd6016af639bad416967`.
- Compiler/contract v41 adds typed cluster context and probability-bearing
  membership adapters plus target-predicate cross-table row selection. It also
  reconciles exact source column names to pinned query output names before E4
  field accounting, so VizieR names such as `CMDCl2.5` retain upstream lineage
  through their legal typed alias `CMDCl2_5`. Hunt/Reffert checkpoint
  `7e66e0690aa962c837d43a86` applies the published 16th-percentile distance
  overlap policy and materializes 465 clusters, 51,017 member claims, and 451
  literature crossmatches as 916 cluster contexts and 51,017 memberships. All
  161 fields are materialized; 154,883 endpoint identity claims and 51,933
  source citations remain release scoped and unresolved. Independent artifact
  and cluster/scope audits pass with zero relation or orbit promotion, and clean
  reproduction matches logical hash
  `14351918254e338cd28f796b3d1837eeeed1ad094c23d0ea27d408effea8d78b`.
- Compiler/contract v42 materializes the pinned OpenNGC and constituent nebula
  catalogs as source-scoped extended-object evidence. Checkpoint
  `54d1b0b6a841344c48327991` accounts all 19,868 rows and 238 fields: 19,012
  catalog objects become extended-object records while 856 ReadMe lines remain
  method documents. It emits 21,107 exact OpenNGC, Messier, NGC, IC, LBN, LDN,
  Barnard, Magakian, vdB, Sharpless, Cederblad, and source-designation claims.
  List-valued aliases remain raw parameters for E2 instead of being split by an
  unreviewed parser; Cederblad component records never claim their base identity.
  Independent artifact and extended-object scope audits pass with zero relation
  or orbit promotion, and clean reproduction matches logical hash
  `456e7a36cfd7e08ea5f7ce19c44817114de5d54d1e077ae365e2668c8191bd2d`.
- Compiler/contract v43 adds source-qualified composite relation endpoints,
  dynamic component scopes, source-status polarity, and numeric
  `zero_is_missing` semantics. It also replaces the compiler's one-letter
  Parquet alias: DuckDB's case-insensitive binding let MSC's `T` periastron
  column shadow `to_json(t)`, which collapsed rows sharing an epoch. The
  unambiguous `source_row` alias and regression gate now hash the complete row
  for every source schema, including schemas containing `T`.
- MSC checkpoint `fc7e9dcabb0b27167c8f188c` accounts all 43,418 rows and 73
  field occurrences from the release archive, ReadMe, component, elementary-
  binary, orbit, and note tables one-for-one. It materializes 15,748 WDS-
  qualified relation claims, including 14,505 positive, 883 ambiguous, and 360
  negative source-status claims; 19,366 coherent orbit records; 19,473 scoped
  classifications; and component physics, astrometry, and photometry. Numeric
  zero and signed-zero source sentinels cannot become measurements. Root
  markers never become component identities, list-valued aliases and orbit
  pair strings remain unsplit, and every binding remains unresolved. Generic
  artifact and MSC source/scope audits pass; clean reproduction matches logical
  hash `d5fb69fba951c886b2a01d30640188f0889ecd6f8dfedab357ad90970baf4fa1`.
- Compiler/contract v44 adds reusable numeric validity bounds to configured
  measurements. Exact source sentinels and out-of-domain values remain in
  source-native Parquet and source-record lineage but cannot become normalized
  measurements.
- WDS checkpoint `ad98d4e369c5a0addc6477a0` accounts 157,476 WDS data/method
  rows and 140,416 CDS WDS-Gaia rows, with all 43 field occurrences
  materialized. Bare pair labels such as `AB` never become identities; only
  WDS-qualified pair keys are emitted. WDS spectral text remains opaque because
  the source field may describe component A or two components. Relative
  astrometry, observation history, photometry, source-convention proper motion,
  and exact J2000 coordinate strings remain source-scoped and unresolved.
- The CDS bridge is explicitly a best angular match within 2 arcseconds, not a
  probability or accepted identity. All 140,416 rows remain candidate
  positional-crossmatch relations with angular separation statistics, zero
  strict probabilities, complete citations, and copied Gaia columns retained
  only as match context. WDS observation rows create no relation, orbit, or
  canonical containment. Artifact and WDS source/scope audits pass; clean
  reproduction matches logical hash
  `7b277d9f190599a1b0cf797dabffa864b5991d956973c3ac29ff4ff3af20cba6`.
- Compiler/contract v45 supports multiple independently keyed cluster-
  membership claims from one source row and permits deterministic published
  assignments with no probability. The latter remain null; the compiler never
  synthesizes a confidence value to fit a probability-bearing schema.
- Gaia DR3 ultracool-association checkpoint `78016b90e02689547c3f53dd`
  accounts all 7,630 catalog rows, 93 ReadMe lines, and eight field occurrences.
  It materializes 7,630 release-scoped Gaia DR3 identities, 6,259 HMAC
  unsupervised cluster assignments with null probability, and 2,840 BANYAN
  best-hypothesis memberships with source probabilities from 0.5 to 1.0. The
  source table contains association assignments, not spectral types; no
  classification is invented from sample membership. All bindings remain
  unresolved, placeholders remain source context, and no relation, orbit, or
  canonical containment is promoted. Generic artifact and source/scope audits
  pass; clean reproduction matches logical hash
  `27a516ce3fbfd67062584099c9323038e9c87f4dcb81b67d3479713d6d2958a0`.
- Compiler/contract v49 rejects non-finite numeric values in configured
  measurements and uncertainties, supports source-declared uncertainty bounds,
  fixed or field-based epochs, multiple memberships, lexical measurements, and
  per-product missing-value contracts. Exact `nan`, negative sentinel, and
  placeholder URL lexemes remain in source-native rows.
- UltracoolSheet checkpoint `20fdb1c95d25d441160d3bd9` accounts all 3,890
  pinned source rows and 242 fields. It materializes 32,841 release-scoped
  identity claims, 149,636 astrometry/distance observations, 50,134 photometry
  observations across 23 bandpasses, 10,887 direct/context classifications,
  23,859 maintainer-derived or context parameters, 3,875 BANYAN memberships,
  3,079 SimpleDB product locators, 1,001 source-reference records, and 152,122
  evidence links. Direct optical/IR classifications remain separate from
  maintainer numeric encodings and selected formulas. Gaia DR2 and DR3 IDs stay
  in distinct namespaces; pipe-delimited SIMBAD alias inventories remain
  unsplit source context. Multiplicity and exoplanet flags lack safe endpoints
  here and create no relation or planet rows. All bindings remain unresolved.
  Artifact and source/scope audits pass; clean reproduction matches logical hash
  `2a7cfb5f4c34df4c17cf2e6e2fa35639d1d0181b984983f7d4779407e62e1bab`.
- Compiler/contract v53 adds reusable multi-relation tables, source-member
  lineage, table-level unit overrides, asymmetric uncertainty fields, literal-
  prefix identifier normalization, and source-row qualification for relation
  columns that collide with lineage names such as `source_id`. Configured
  photometry and domain evidence now record the exact lower/upper source field
  names alongside their values.
- Targeted TESS checkpoint `11aa9bd00cc710f971b01837` accounts all 122,772
  source rows and 239 field occurrences with no pending field or duplicate row.
  It preserves 27,930 targeted TIC rows, 8,064 TOIs, 29,302 official Gaia
  DR2-to-DR3 neighborhood rows, 137 member-qualified official external
  crossmatches, and 29,409 targeted Gaia DR3 rows. The two official raw TOI
  forms, `101.01` and `TOI-101.01`, normalize to one release-scoped identity
  while both remain inspectable.
- The checkpoint keeps 1,332 confirmed/known, 5,383 candidate, and 1,346
  negative TOI lifecycle claims separate. It emits 27,775 positive TIC-to-Gaia
  DR2 associations, 29,302 candidate release-neighborhood relations, 137
  candidate external best-neighbor relations, and 78 duplicate/split TIC
  relations. All bindings remain unresolved and no CORE inventory table exists
  in the artifact. Generic and TESS-specific audits pass, including exact TIC
  target coverage, DR2/DR3 separation, high-proper-motion and TESS EB controls,
  relation endpoint scopes, and 131,309 asymmetric TIC measurements. Clean
  reproduction matches logical hash
  `5e17ca0f67e7d41a9459898ef26efc42dbd4c90f3b58e7ec4f00dd84c2a8c35a`.
- NASA checkpoint build `cb82c09179afa740b02e2cdf` accounts 206,989 source
  rows as 203,932 exact
  source records and preserves 3,057 repeated identical row occurrences through
  duplicate counts. It materializes 750,151 release-scoped identifier claims
  and 72,809 planet-lifecycle claims. Identifier fields carry their own star,
  host, planet/candidate, observation-target, signal, component-label, or
  product scope rather than inheriting a mixed source row's broad scope. The
  compiler emits 697,952 explicit unresolved record/scope binding outcomes for
  later E2-graph reconciliation. Confirmed
  claims are positive evidence, candidates remain candidate evidence, and false
  positives, false alarms, and refuted claims are negative evidence; none of
  these rows alter canonical planet inventory.
- The NASA adapter is field-complete: all 2,093 fields have reviewed
  dispositions, 2,081 are materially represented, and 12 archive spatial-index
  helpers are deliberately excluded from scientific evidence while remaining
  in the immutable typed source. This adapter passes; E4 remains in progress
  until the other registered source adapters meet the same gate.
- Wide-binary checkpoint `aaf262b1791d98ce3e9f96e7` intersects the complete
  El-Badry release with the registered 1,250-ly buffer when either component's
  three-sigma parallax interval overlaps the boundary. It retains 877,307 main
  candidate pairs and 239,406 shifted-sky controls while explicitly accounting
  for 1,218,874 excluded rows. The source's `R_chance_align` value is a KDE
  density ratio that approximates chance-alignment probability but is not a
  strict probability and may exceed one. E4 therefore stores all 1,116,713
  values as typed confidence statistics, stores zero strict probabilities,
  preserves shifted controls as negative evidence, and promotes no relation to
  canonical containment. Each retained pair has distinct `left` and `right`
  Gaia EDR3 endpoint claims and binding scopes. All 422 source fields are
  accounted as 24 materialized identity/relation/document fields or 398
  reviewed copied-Gaia exclusions whose exact values remain in immutable typed
  Parquet and are owned scientifically by release-native Gaia adapters.
- The bounded wide-binary artifact is 11,129,073,664 bytes. Clean reproduction
  matches logical hash `2b45feebcbe9bb3f18743b1043613ca2c454abf9cb393836e9a0c542d220dcaf`,
  and the independent artifact audit reports zero endpoint, scope, sentinel,
  citation, probability, uncertainty, parameter-set, or lineage failures.
- Scientific-evidence contract v2 adds explicit `component_scope` to stellar
  classifications, stellar parameter sets, and their measurements. A source
  row may therefore carry multiple coherent component parameter sets without
  projecting primary, secondary, or system values onto the wrong object.
- ORB6 checkpoint `fcbb6466bea0a7798ae8d2ed` exhaustively retains 4,051 rows,
  37 fields, 4,051 visual-orbit parameter sets, 16,397 catalog identity claims,
  and 799 reference codes. The source's combined discoverer designation stays
  an opaque pair-scope claim until the identity/scope graph resolves it; E4
  does not split labels such as `Aa,Ab` or infer canonical endpoints.
- DEBCat checkpoint `b3a141c0caf953aa83c4e52b` exhaustively retains 374 rows
  and 30 fields while materializing component-scoped mass, radius, surface
  gravity, temperature, luminosity, and spectral classification plus
  system-scoped metallicity and integrated photometry. Published logarithmic
  values remain logarithmic source measurements. Missing-value sentinels are
  recorded in the adapter and filtered from evidence without erasing the exact
  source row. All primary/secondary binding outcomes remain unresolved.
- Contract v3 adds an extended-object parameter set beside geometry and
  distance, plus declarative composite identifiers and configured source-native
  scalar evidence. Green SNR checkpoint `d08c5aa9af7dc8bcdbf0d6c3` uses that
  contract to preserve 310 SNR rows, all 15 fields, exact angular geometry,
  1-GHz flux/index uncertainty markers, aliases, and detail locators under
  deterministic Galactic `G...` identities.
- TESS EB checkpoint `255678b2daa6e8bf46e6dcd9` preserves 17,605 rows and all
  20 fields. TIC identifiers retain their zero-padded raw values and normalize
  to unsigned decimal IDs. `in_catalog=true` is required before an orbit
  solution is emitted; false rows remain negative catalog-membership evidence.
  Sectors, source/flags, morphology, Tmag, astrometry, and unresolved target
  stellar context remain independently typed and provenance-bound.
- White-dwarf checkpoint `486e4975af015d4e5f5a3c9b` applies
  `buffered_posterior_distance_overlap_v1`: a candidate is retained when the
  published geometric-distance posterior lower bound is at or inside 383.245
  pc, so its interval overlaps the 1,250-ly buffer. This retains 337,272 of
  1,280,266 source rows and reports the 942,994 excluded rows explicitly.
- Hydrogen, helium, and mixed-atmosphere Teff/log-g/mass/chi-square solutions
  remain 597,608 distinct coherent parameter sets containing 2,390,432
  measurements; E4 does not choose a preferred atmosphere. Candidate
  probability/context is a separate compact-object parameter set. Copied
  Gaia/SDSS scalars remain exact in typed Parquet and source-record lineage but
  are deliberately owned by release-native Gaia, distance, and survey adapters.
- Contract v3 also supports deterministic adapter table order, predicate-scoped
  identity claims, and authoritative source citation catalogs. ATNF checkpoint
  `64c55c19a5a10a88877d4cd2` uses those general contracts to materialize its
  bibliography before dependent parameter/glitch rows and to expose PSRJ/PSRB
  only on the matching parameter occurrence. The source package contributes
  190,671 source records, including 91,214 repeated parameter occurrences, 644
  glitches, 1,210 full references, 97,472 comments, and source-document/archive
  context. It emits 91,858 compact parameter sets and 97,424 identities.
- ATNF's fourth lexical parameter token is not always a bibliography key. Only
  exact matches to the 1,210 source reference codes populate `reference_raw`
  and create the 84,388 evidence-citation links. The remaining 959 populated
  tokens are preserved in `parameter_set_raw` without manufacturing citations.
  The 286,011,392-byte artifact has logical hash
  `5bcf94b69a5a0e1a1905f2a891fd95d7f852c6c9af55531cdf6d9448f6747834`;
  clean reproduction and the independent artifact audit pass.
- Compiler v24 permits one source row to emit multiple uniquely typed compact
  parameter sets with independent predicates, references, and methods. McGill
  magnetar checkpoint `c599c951590451ace4248934` uses this contract to account
  all 31 rows and 47 fields without flattening timing, X-ray, distance,
  position, and association/activity contexts together. The result contains
  139 parameter sets: 26 timing, 26 X-ray, 25 distance, 31 position, and 31
  source context.
- McGill's seven trailing `#`/`##` source footnote markers remain in raw names
  but are removed from normalized `magnetar_name` identity. Its timing, X-ray,
  distance, association, and position reference families remain exact source
  codes, producing 96 distinct records and 128 evidence links. The publisher's
  expanded bibliography is not in the pinned `TabO1.csv`; acquiring and
  validating it remains an explicit follow-up rather than inventing citation
  text. The 6,041,600-byte artifact has logical hash
  `9d95e4669d24ff8c0db396f253436d96be42a81c9f390d0cd2b9883cf93f2979`;
  clean reproduction and independent audit pass.
- Compiler v27 makes relation endpoint component scopes explicit rather than
  assuming every catalog uses `left`/`right`. It also supports required,
  ambiguity-failing orbital links to a relation in another source table through
  exact source-native logical keys. SB9 uses primary/secondary scopes and links
  each `orbits.Seq` to exactly one `main.Seq` relation claim; a missing or
  multiply matched required link fails the build.
- SB9 checkpoint `72663823963198c8fcbbe569` accounts 30,153 source rows and all
  62 table-column occurrences. It materializes 4,079 positive binary claims,
  5,099 separate orbit solutions linked to 4,078 relation claims, 3,478 primary
  plus 601 secondary spectral classifications, and 3,978 primary plus 425
  secondary magnitude measurements. Source gaps remain gaps; no secondary
  classification or magnitude is inferred.
- The alias table contributes release-distinct Gaia DR2/DR3 identities through
  declared prefix stripping and unsigned-decimal normalization while retaining
  the raw alias. Direct 19-character ADS bibcodes are recognized without
  rewriting the citation text; 1,807 of 1,826 references receive ADS locators.
  The 95,956,992-byte artifact has logical hash
  `1406dc3e6c30b4b1e92bfc333abb953478d0f38b1f473ba7419c70c9750c2ddf`.
  Clean reproduction and the independent audit pass; no relation is promoted
  to canonical containment.
- Compiler/contract v29 adds predicate-bounded relation emission,
  classification-only component contexts, dynamic source band/reference
  support, and complete consumption of configured epoch/reference/quality
  fields. These are general evidence contracts; none branches on a named
  astronomical object.
- The Evidence Lake SBX acquisition profile is separate from the served legacy
  core projection. Because the complete upstream catalog is small, it uses no
  spatial truncation and preserves 4,080 systems, 102,459 aliases, 261
  configurations, 5,169 orbit rows, and all 73 table-column occurrences in
  immutable raw/typed snapshot `ea236790d0501967b3c30466`.
- Served legacy SBX, Gaia backbone/classifier/NSS, and NASA `ps`/`pscomppars`
  manifests are registered as disabled E6 stability-reference releases. This
  keeps schema and retention accounting complete without allowing a lossy
  projection to compete with its active full-release source.
- SBX checkpoint `37ffa7255d026c8d930af6d4` emits 4,080 positive
  primary/secondary binary relations, 94 child/parent hierarchy relations with
  explicit endpoint scopes, and 5,169 source-native orbit solutions. Every
  orbit links by exact `sn` to one binary relation. The complete alias evidence
  remains source/release scoped; HD/HIP component suffixes are designations,
  not malformed integer IDs. Its logical hash is
  `0ac0ff9babcd641446d2a4fdab0abcd7c19cc8ce7278c136e129507cb5663fc0`;
  clean reproduction and independent audit pass, and zero claims become
  canonical containment.
- Expanded Gaia NSS uses the generic coherent-orbit contract without inventing
  binary endpoints. Each `(source_id, solution_id, nss_solution_type)` retains
  its source model, all fitted values and errors, the correlation vector and bit
  index, observation counts, fit diagnostics, quality flags, ICRS J2016.0 frame, and
  Gaia data-model reference in one solution. Corrected compiler-v65 checkpoint
  `1881e02d8e9f1d33a1d9b64a` accounts all 87,075 rows
  and 154 table-field occurrences with zero pending mappings or solution-key
  collisions. Source-specific, generic artifact, and clean-reproduction audits
  pass logical hash
  `3aeabe350ec4e224ab9b04dceae6fab9678cdd27a5337919ed6c1c8912f51e5a`.
- Compiler/contract v65 extends the scoped stellar-parameter adapter with an
  explicit uncertainty-field semantic. Ordinary error columns remain absolute
  magnitudes; source-published lower and upper posterior columns remain absolute
  interval endpoints and require a named bound semantic. A focused regression
  test prevents either representation from being silently converted into the
  other. This shared contract is required before Gaia AP parameter sets are
  materialized and does not select any public winning value.
- Compiler/contract v66 materializes the complete bounded Gaia DR3 AllWISE,
  2MASS, Hipparcos-2, Tycho-2, and RAVE DR6 official best-neighbour products as
  candidate crossmatch relations, never accepted identity merges. Checkpoint
  `81b0cc4aa29453088a62f3de` accounts 24,045,693 source rows, 48,091,386
  endpoint identifiers, 24,045,693 angular-separation-bearing relation claims,
  72,137,079 unresolved binding scopes, and all 62 field occurrences with zero
  pending fields or normalization rejections. Source-specific and independent
  artifact audits pass; clean reproduction on the bulk scratch disk matches
  logical hash
  `2cd08ee00ab39b699627eb2614392a7e0c4f241fe9214a476762c6cab15d87a0`.
  Large exact materialization uses one thread, disabled insertion-order
  preservation, a 32-GB Photon memory ceiling, and primary-key deduplication
  without a release-wide `UNION DISTINCT` aggregation.
- Compiler/contract v67 adds coherent source-classifier probability bundles so
  multi-model probability vectors remain together without hundreds of millions
  of scalar rows or an invented cross-model winner. It also makes configured
  domain uncertainty fields distinguish error magnitudes from absolute interval
  endpoints. The independent audit permits signed interval bounds, rejects
  reversed endpoints, and reports source-native central estimates outside their
  published intervals without rewriting them. These general gates precede Gaia
  AP materialization.
- Compiler/contract v68 maps all 482 hard/supplement field occurrences in the
  main Gaia DR3 astrophysical-parameters release with zero schema-name remainder.
  It keeps DSC/ESP-ELS probability vectors coherent; separates GSP-Phot,
  photometric FLAME, GSP-Spec atmosphere/abundances, GSP-Spec CN/DIB features,
  ESP-ELS, ESP-HS, ESP-CS, ESP-UCD, MSC system, MSC component, and OA neuron
  contexts; preserves posterior endpoints; and gives MSC primary/secondary
  model components explicit unresolved scopes. Source-native units were checked
  against the typed VOTable schema, which corrected DIB equivalent-width and
  ESP-CS activity-index declarations before build. Contract validation, exact
  schema reconciliation, 65 compiler tests, and real-row smoke materialization
  pass. `solution_id` is one constant processing-release value across all rows,
  so it remains lineage rather than producing 51 million false object-identity
  claims; each source-table row keys and binds by Gaia source at star scope.
  The large immutable source build and independent audits remain pending.
- The first full AP build materialized the scientific tables but failed closed
  at the final release-wide unresolved-binding insert under the 32-GB cap.
  Compiler/contract v69 emits the same deterministic primary-keyed scopes after
  each source table instead of aggregating all ten tables at release end. This
  bounds working state without changing evidence or binding identity. The sole
  manifestless v68 staging tree was retired whole through exact-hash retention;
  the first v69 retry was interrupted before report publication and its closed,
  unreferenced staging tree was likewise retired through exact-hash retention.
- The tmux-isolated v69 retry completed all per-table binding work and then
  failed closed while linking ordinary evidence citations: only nested
  astrometry-bundle citations used the existing 32-bucket execution policy, so
  a single evidence-table join reached the 32-GB cap. Compiler/contract v70
  applies deterministic source-record hash buckets to every evidence-reference
  table while preserving the same citation keys and counts. Accepted checkpoint
  `393b08fa1268bbd42bb40225` accounts all 51,164,425 rows and 482 fields with
  zero pending mappings, exclusions, duplicates, or normalization rejections;
  it links 134,743,089 evidence citations under the bounded policy. The
  source-specific and generic artifact audits pass. Clean reproduction on USB
  scratch matches logical hash
  `b84be6a482e90bd4527f498f87f4381f1439b0e67a7ec5762c19530976ec6596`
  and removes its scratch tree.
- The source audit also pins 271,975 source-native non-bracketing intervals:
  271,968 hard-envelope and four uncertainty-supplement FLAME luminosities plus
  three GSP-Spec Mg/Fe measurements. Direct typed-Parquet counts match the
  evidence artifact exactly. These published values and endpoints remain
  unchanged and visible as anomalies; reversed endpoints remain fatal.
- Contract v71 adds the four-table Gaia DR3 supplementary-parameter adapter.
  MARCS, PHOENIX, OB, and A GSP-Phot posteriors remain four coherent atmosphere-
  library alternatives with their own temperatures, gravities, metallicities,
  radii, model distances, extinction, magnitudes, posterior scores, and sampler
  acceptance. The publisher's `libname_best_gspphot` remains quality/selection
  lineage and does not erase the other three solutions. GSP-Spec ANN and
  spectroscopic FLAME remain separate coherent parameter sets; FLAME evolution
  stage and bolometric correction retain their proper classification and
  photometry domains. The disjoint uncertainty-envelope tables inherit the
  identical contracts. All 354 field occurrences reconcile, 67 compiler tests
  and a representative real-row materialization pass, and direct source audit
  finds no reversed bounds plus two preserved hard-envelope FLAME luminosity
  intervals whose central estimate does not bracket. Immutable build
  `c4a6b5fd297f8ef9cceb6340` now passes the source-specific and independent
  artifact audits with all 8,019,372 source records, 354 fields, and
  53,257,759 evidence/citation links accounted and no duplicate keys. Clean
  scratch reproduction remains pending.
- Compiler/contract v71/v72 removes retained DuckDB primary-key and unique ART
  indexes from immutable analytical evidence tables. Those indexes enforced
  transient write-time uniqueness but occupied most of the accepted main AP
  artifact: table storage accounts for roughly 58 GiB of its 167 GiB database,
  with retained constraint indexes accounting for most of the remainder. The
  compiler now constructs deterministic namespaced keys, explicitly
  deduplicates unresolved bindings, and runs an exact fail-closed uniqueness
  audit over every table key plus the source-record natural key before atomic
  promotion. The independent artifact verifier repeats that audit. A same-row
  supplementary-AP smoke A/B preserves every logical table hash, reports zero
  duplicate keys, and reduces the database from 8,925,184 to 5,255,168 bytes
  (41.1%). This changes storage and integrity enforcement, not scientific
  content.
- `config/evidence_lake/e4_source_scope.json` now accounts every one of the 44
  registered source releases at the E4 boundary. Thirty have scientific-
  evidence adapters; official DR2/DR3 neighbourhood products remain E2-only
  identity edges; disabled lossy projections and transitional AT-HYG remain E6
  stability/identity references; and legacy Cantat-Gaudin DR2 cluster evidence
  is retained source-native while Hunt/Reffert is the active E4 cluster
  authority. No registered source is unaccounted. The machine audit remains
  `in_progress` on five real adapter requirements: Gaia source, Gaia
  variability, VSX, natural JPL Horizons, and artificial JPL Horizons.
- The pinned VSX object table now has a machine-readable pre-adapter audit. It
  accounts all 10,304,568 rows, verifies unique source OIDs, valid coordinates,
  status/limit/uncertainty flag domains, and positive published periods, and
  records the two duplicated public-name strings without treating names as
  identity keys. The audit remains `incomplete`: the registered acquisition
  preserves `vsx.dat` and its ReadMe but omits the source-documented
  object-to-bibcode `refs.dat` table. VSX cannot pass E4 until a pinned
  bibliography artifact is acquired, typed, and linked without guessing
  references for rows that do not publish one.
  The collector now includes that endpoint for the next pinned snapshot. The
  available CDS artifact is a clean but historical partial relation (830,415
  unique OID/bibcode links, 586,530 OIDs, maximum OID 683,950, server-modified
  in 2022), so coverage gaps against the 2026 object table will remain explicit.
  Schema-driven scratch typing and the expanded audit pass every row and exact
  pair. The report retains 2,072 OID links absent from the current object table
  and 54 noncanonical reference strings as missing-binding/raw-reference
  evidence rather than discarding or guessing them.
- The source-native Gaia DR3 backbone now passes its independent pre-adapter
  audit. Its 31,987,126-row hard-parallax branch and 189,145-row uncertainty
  supplement have identical complete 152-field schemas, unique and disjoint
  Gaia DR3 identities, correct envelope polarity, one solution release and
  J2016.0 epoch, and valid coordinate, uncertainty, correlation, and probability
  domains. The audit records 2,929,216 radial-velocity rows, 5,778,039
  XP-continuous product indexes, 548,038 epoch-photometry indexes, and 206,781
  RVS product indexes for E4 materialization. Signed `*_over_error` ratios remain
  distinct from nonnegative uncertainty magnitudes.
- The Gaia variability pre-adapter audit is now reproducible from a checked-in
  script. It passes 592,197 rows and all 52 rotation-vector fields with zero
  source-ID, schema, token, length, period-error, or false-alarm-probability
  defect. Its 99 wholly absent vectors remain distinct from 2,533,499 valid
  `--` element masks. E4 will preserve variability summaries and rotation
  solutions as coherent per-source parameter sets rather than independent
  scalar selections.
- JPL Horizons collection now has a shared immutable snapshot writer. It keeps
  byte-identical API response bodies, exact query parameters/URLs, checksums,
  sizes, retrieval time, reviewed operator-target seed and collector checksum,
  plus the parsed CSV projection. The mutable legacy CSV remains only an atomic
  compatibility projection. An 11-target artificial-object scratch run passed
  raw/typed artifact accounting and the source-specific projection/response
  audit: one-to-one identity and query lineage, path containment, exact response
  hashes and byte counts, and valid hyperbolic trajectory conventions. Photon
  registry cutover, natural-source collection, current artificial refresh, and
  both E4 adapters remain pending until the active Gaia build reproduces.
- The build materializes 9,689,745 stellar, astrometric, photometric, rotation,
  planet, lifecycle, transit, and RV evidence rows; 272,355 coherent stellar and
  planet parameter sets; 111,084 on-demand Kepler validation products; 2,961
  parsed source references; and 4,656,423 evidence-citation links. Raw units and
  reference fragments remain intact beside versioned aliases and parsed ADS
  metadata.
- Logical keys are source semantics, not artificial uniqueness claims. Exact
  row hashes provide deterministic record identity when a catalog's apparent
  key repeats; row-array position is never used. Compiler timestamps derive
  from pinned retrieval lineage, and cached artifacts verify their DuckDB
  checksum before reuse. Build identity also hashes the compiler source,
  registry file, contract, Python/DuckDB runtime versions, and every raw/typed
  input, preventing unversioned code or registry edits from reusing old output.

### E5. Selection and Derivation Compiler

Versioned policies select public facts by quantity and applicability domain.
Selection should prefer coherent parameter sets and must not create a
field-by-field composite that hides incompatible assumptions or references.
Alternatives and conflicts remain queryable.

The general precedence is:

1. accepted direct or dynamical measurement appropriate to the quantity
2. accepted calibrated source estimate or model appropriate to the object
3. defensible physical derivation with compatible inputs and propagated error
4. empirical relation within its documented applicability domain
5. labeled presentation prior

This ordering is specialized per quantity. Mass, age, temperature, luminosity,
orbit, classification, and membership do not share one global authority list.

Every selected fact references its evidence or derivation record. Every
derivation records its inputs, algorithm version, applicability, confidence,
and supersession state. Verification must detect a fallback selected despite
acceptable higher-authority evidence.

### E6. Reproducible Product Projections and Shadow Build

CORE, ARM, canonical hierarchy, DISC, search indexes, public slices, map tiles,
simulation scenes, and later AAA evidence packets are disposable projections
from the evidence compiler. UI and scene code may format selected facts but may
not independently choose scientific winners or implement hidden fallbacks.

The current served build remains the stability reference while Evidence Lake
v2 is built in parallel. A candidate requires a complete A/B scientific diff,
not merely a successful database build.

Compare at minimum:

- object inventory, identity, aliases, component scope, and hierarchy
- source and field accounting
- selected and conflicting stellar parameters
- spectral/remnant classification and fallback utilization
- luminosity and habitable-zone evidence basis
- planet counts, status, mass provenance, transit/RV fields, and categories
- orbit coverage and simulation assumptions
- cluster, variability, compact-object, and extended-object evidence
- API/search behavior, public slice integrity, storage, and performance

### E7. Promotion, Cutover, and Legacy Retirement

After the E6 scientific review is accepted, promote atomically while retaining
the previous build for rollback. Retire or formally deprecate the old
collectors, cookers, evidence schemas, and duplicated selection paths only
after the new build is reproducible from pinned inputs.

## Initial Source Program

The first source program is release-pinned and bounded to Spacegate's scientific
inventory plus an uncertainty-aware ingestion envelope around the public
1,000-ly sphere.

Foundation sources:

- Gaia DR3 source, relevant astrophysical-parameter main/supplementary tables,
  non-single-star solutions, variability/activity/rotation products, and
  official external-catalog crossmatches
- a documented Gaia distance-estimate catalog as supplementary evidence, not
  an astrometric identity replacement
- SIMBAD, GCVS/VSX, IAU WGSN, and source-native identifiers for naming,
  bibliography, and alias scope
- current Gaia DR3 open-cluster membership/physical catalogs with older
  catalogs retained as independent claims

Multiplicity sources:

- Gaia NSS, WDS, ORB6, SB9/SBX, MSC, DEBCat, TESS EB, Kepler EB where useful,
  and a probability-bearing Gaia wide-binary catalog

Planet sources:

- NASA reference-specific Planetary Systems rows, composite rows, stellar
  hosts, TOI, Kepler/K2 candidates and TCE/status evidence
- current supplemental lifecycle catalogs only through the documented M5.3
  precedence and tombstone policy

Stellar-physics sources:

- Gaia Apsis/RVS products and matched APOGEE, GALAH, and LAMOST parameter and
  abundance releases, retaining source-native flags and parameter sets
- existing white-dwarf, pulsar, magnetar, ultracool, and variability sources

Observation products:

- comprehensive metadata and retrieval locators for useful spectra, light
  curves, atmosphere spectra, and imagery
- bounded checksum-addressed caching based on public interest, research jobs,
  or explicit operator requests rather than unconditional bulk mirroring

## Boundary and Storage Policy

A hard observed-parallax cut is not an adequate scientific ingest boundary.
The source program must define an uncertainty-aware envelope outside the public
radius, retain boundary candidates, and report inclusion/exclusion reasons.

Before large acquisition or a full build:

- run the documented retention audit without deleting served, rollback,
  published, referenced, or unique-source artifacts
- set explicit raw, typed, build, report, and product-cache storage budgets
- keep durable source snapshots on reliable internal storage or backed up;
  `/mnt/space/spacegate` is suitable for bulk document/product caches but not as
  the only copy of irreplaceable inputs
- keep spectra, light curves, and imagery out of hot served databases; store
  product indexes and bounded caches instead

## Hard Gates

Evidence Lake v2 is not promotable until:

- every registered source release and field has an explicit disposition
- raw snapshots reproduce typed outputs from clean state
- schemas, units, limits, uncertainties, and null semantics validate
- every targeted identity and scope reconciliation has an explicit outcome
- release-scoped IDs are never equated without an accepted crossmatch edge
- every selected public fact points to evidence or a versioned derivation
- acceptable source evidence prevents lower-authority fallback selection
- alternatives, conflicts, negative evidence, and tombstones remain preserved
- exact inventory and hierarchy changes are explained by reusable policies
- deterministic reruns match and the A/B scientific report is reviewed
- no production transform branches on a named benchmark object
- public API, search, map, simulation, and slice gates pass against the shadow
  build before promotion

## Explicit Non-Goals

This milestone does not include:

- public evidence presentation or interactive analysis tools
- automatic AAA adjudication or publication
- full mirroring of Gaia, TIC, spectra, light curves, or survey imagery
- changing RIM/worldbuilding ownership
- object-specific corrections added to satisfy goldens
- deployment to antiproton before the shadow-build acceptance gates pass

## Later Public Evidence Objectives

### Collapsed Evidence Inspector

After Evidence Lake v2 establishes stable public evidence contracts, add a
collapsed section low on the System Page that shows the selected value, why it
was selected, competing values, uncertainties, source/model/reference, and
lineage. It must remain approachable by default while allowing a visitor to
inspect the full evidence without implying that the chosen value is infallible.

### Interactive Observation Labs

Build reusable, source-attributed viewers for spectra, exoplanet atmosphere
spectra, light curves, images, and related observation products. Start with a
spectrum lab that supports pan/zoom, uncertainty, wavelength/unit controls,
line overlays, redshift or radial-velocity context, and element-identification
explanations.

Gamification may use challenges such as matching absorption/emission lines,
finding a transit, or comparing model spectra, but it must not reward false
precision or write visitor interpretations into canonical science. Curated
missions, scoring, and saved progress belong in presentation/community layers;
the underlying product, calibration, provenance, and accepted interpretation
remain scientific evidence.

## Related Documents

- `docs/PROJECT.md`
- `docs/MILESTONES.md`
- `docs/CHECKLIST.md`
- `docs/DATA_SOURCES.md`
- `docs/SCHEMA_CORE.md`
- `docs/SCHEMA_ARM.md`
- `docs/SCHEMA_DISC.md`
- `docs/CANONICAL_INGEST.md`
- `docs/RETENTION.md`
- `docs/SOURCE_CATALOG_UTILIZATION_AUDIT_2026-07-17.md`
- `docs/DATASET_ITERATION_HISTORY.md`
