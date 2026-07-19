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
- The Gaia 1,250-ly envelope is measured at 31,987,126 source rows. The program
  accounts for all 764 upstream columns across `gaia_source`, AP main and
  supplementary, NSS orbit, variability summary, and rotation-modulation
  tables; large exact acquisitions continue in crash-resilient tmux jobs.
- Expanded NSS already preserves 50,762 complete 77-column orbit rows. The
  complete NASA acquisition is also typed and verified: 12 planet, host, TOI,
  K2, Kepler-name, KOI, TCE, and transit-detection tables preserve 206,989 rows
  and all 2,093 upstream fields with zero omissions; a clean raw-to-typed
  reproduction matches the promoted typed hashes. The Bailer-Jones distance
  envelope, Hunt-Reffert cluster tables, staged SIMBAD Gaia DR3 bridge,
  El-Badry wide binaries, and APOGEE/GALAH/LAMOST releases now have explicit
  registered contracts. Completion and typed coverage for those remaining
  sources remain E3 exit gates, not assumed outcomes.
- SIMBAD is deliberately staged rather than mirrored: acquire the release-
  pinned Gaia DR3 identity bridge, intersect locally with the Evidence Lake
  envelope, then request basic, alias, and bibliography evidence for the
  matched object set. SIMBAD remains identity/naming evidence, not a canonical
  inventory catalog.
- The El-Badry, Rix, and Heintz Gaia EDR3 wide-binary release is pinned and
  typed as source-native evidence: 1,817,594 catalog rows across 217 columns,
  517,993 shifted-control rows across 201 columns, and both published method
  scripts. Immutable snapshot `aea36fe5a6753de90be33301` passes schema/row
  verification and clean typed-hash reproduction. Relation materialization and
  local envelope intersection remain E4 work.

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

- `config/evidence_lake/e4_scientific_evidence.json` defines 22 bounded domain
  tables, controlled binding/mapping states, and data-driven source adapters.
  This is not a universal EAV store: each scientific family has its own typed
  contract and controlled quantity vocabulary.
- `scripts/compile_scientific_evidence.py` emits immutable, content-addressed
  builds under `derived/evidence_lake_v2/scientific_evidence/<build_id>/` with
  logical per-table hashes, source/field accounting, and explicit unresolved
  binding outcomes. `scripts/verify_scientific_evidence_reproduction.py`
  rebuilds into a temporary scratch root, compares deterministic logical hashes,
  and removes the scratch artifact; the NASA foundation reproduction passes.
- NASA checkpoint build `d97673cd1a443d5e3e092c62` accounts 206,989 source
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
- All 2,093 fields have dispositions; 188 reviewed identity, lifecycle, lineage,
  or context fields are materialized and 1,905 fields remain `declared_pending`
  until their domain adapters write actual evidence rows.
- Logical keys are source semantics, not artificial uniqueness claims. Exact
  row hashes provide deterministic record identity when a catalog's apparent
  key repeats; row-array position is never used. Compiler timestamps derive
  from pinned retrieval lineage, and cached artifacts verify their DuckDB
  checksum before reuse.

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
