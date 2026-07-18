# Spacegate Evidence Lake v2

Status: active main quest. E0 and E1 completed July 18, 2026; E2 is next.

E0 checkpoint:

- `config/evidence_lake/source_releases.json` registers 32 active,
  transitional, expansion-pending, and planned source releases with domain
  authority, identity, retrieval, license, schema, and storage contracts.
- `config/evidence_lake/schema_baseline.json` pins 59 current manifest entries,
  1,807 machine-enumerated fields, and exact format contracts for source formats
  whose schemas live in official source documents. The baseline fingerprint is
  `037f3c98ecb4cdb990a01de53d746072233a1a68c5bc4b50d4e912ea9889e184`.
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

### E3. Foundational Source Acquisition

Acquire the bounded source program defined below through the source registry
and raw/typed contracts. Acquisition is complete only when every targeted
source record and upstream field is present or has an explicit omission reason.
Large observation products follow the metadata-first storage policy.

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
