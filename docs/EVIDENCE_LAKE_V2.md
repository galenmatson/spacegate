# Spacegate Evidence Lake v2

Status: active main quest. E0-E2 completed July 18, 2026; E3 acquisition is in
progress.

E0 checkpoint:

- `config/evidence_lake/source_releases.json` registers 34 active,
  transitional, expansion-pending, and planned source releases with domain
  authority, identity, retrieval, license, schema, and storage contracts.
- `config/evidence_lake/schema_baseline.json` pins 63 current manifest entries,
  1,824 machine-enumerated fields, and exact format contracts for source formats
  whose schemas live in official source documents. The baseline fingerprint is
  `5f1ec5ec044733acf6da32a7532a84cc8d1d118f6ba168d6f9f5e05530f53cbf`.
- `scripts/evidence_lake_registry.py` emits registry/schema/field and storage
  audits. Full-refresh preflight now fails on unregistered sources, schema
  drift, missing active artifacts, or an acquisition-floor breach.
- Reference-aware retention preserved 11 served/published/rollback lineage
  builds and reclaimed 196.21 GiB of unreferenced immutable builds. Photon has
  about 385 GiB free on `/data`, above the 300 GiB acquisition floor. The
  separate 54 GiB scratch area was not pruned.
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

E3 acquisition checkpoint (July 19, 2026, in progress):

- `config/evidence_lake/e3_acquisition_program.json` and
  `scripts/evidence_tap_acquire.py` define exact schema-accounted TAP products,
  deterministic partitions, resumable response sets, checksums, row/MAXREC
  gates, UWS lineage, bounded read-stall recovery, inter-process manifest
  locking, and atomic promotion.
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
  official external-crossmatch products now have explicit disjoint posterior-
  overlap companions. A checked acquisition contract requires matching source,
  table, field-selection, partition, cap, and disposition semantics so later
  products cannot silently fall back to hard-parallax-only coverage.
- The program accounts for all 764 upstream columns across `gaia_source`, AP
  main and supplementary, NSS orbit, variability summary, and rotation-
  modulation tables; large exact acquisitions continue in crash-resilient
  tmux jobs.
- Expanded NSS already preserves 50,762 complete 77-column orbit rows. The
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
  own scope and scientific selection. Remaining registered sources retain E3
  completion and typed-coverage exit gates.
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
  binary endpoints. Each `(source_id, solution_id)` retains its source model,
  all fitted values and errors, the correlation vector and bit index,
  observation counts, fit diagnostics, quality flags, ICRS J2016.0 frame, and
  Gaia data-model reference in one solution. Checkpoint
  `e198804d34abcf04d209d116` accounts all 50,762 rows and 77 fields with zero
  pending mappings and reproduces logical hash
  `cadc76e161e0042dbcb4cc7bed43e9c3fef273ede390e86b9588f8e8e5351e51`.
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
