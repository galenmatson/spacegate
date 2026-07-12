# TESS Identity, Candidate, and Observation Integration

Status:

- approved side quest
- implementation branch: `feature/tess-evidence-v1`
- identity and inventory gates precede candidate and observation presentation

## Purpose

TESS integration is not one catalog import. Spacegate needs four distinct
products with different authority and storage rules:

1. TIC and TOI identifiers for exact identity lookup.
2. Recovery of real, relevant stellar objects missing from canonical inventory.
3. TOI candidate, disposition, and transit evidence.
4. Links and later cached products for light curves, validation reports, and
   other TESS observations.

The full TESS Input Catalog is not a Spacegate inventory source. TIC v8 is a
roughly 1.5-billion-row compiled catalog based on Gaia DR2 and other surveys.
Bulk ingestion would duplicate the Gaia DR3 backbone, import TIC split/join/
artifact complexity, and add substantial storage and reconciliation cost.

Spacegate will instead retrieve the targeted TIC rows needed to resolve TESS
objects that are already scientifically relevant to the product.

## Current Baseline

Local served build `20260711T_wise_v1_seed_side`:

- `17,605` TESS EB evidence rows
- `6,235` TESS EB rows linked to Spacegate systems
- `6,231` searchable `TIC` aliases, almost entirely from TESS EB linkage
- `897` confirmed planets attributed to TESS in the canonical NASA planet set
- `764` canonical planets with TOI-style names
- no general NASA TOI table ingestion
- no candidate planets in the currently served canonical planet table

NASA Exoplanet Archive TOI snapshot observed July 12, 2026:

- `8,064` TOI rows
- `4,900` planetary candidates (`PC`)
- `483` ambiguous planetary candidates (`APC`)
- `739` confirmed planets (`CP`)
- `593` known planets (`KP`)
- `1,246` false positives (`FP`)
- `100` false alarms (`FA`)
- within 100 ly: `25` PC rows across `25` hosts
- within 1000 ly: `1,663` PC rows across `1,543` hosts

Counts are evaluation snapshots, not hard-coded acceptance expectations. Build
reports must record current counts and deltas.

Implementation checkpoint `ecc217f` (July 12, 2026):

- default-on targeted acquisition from NASA, MAST, and Gaia with no bulk TIC
- `27,930` unique target TIC IDs from TOI, NASA hosts, TESS EB, and reviewed
  seed inputs
- content-addressed raw snapshots plus request-hash sidecars and deterministic
  cooked artifacts
- zero-change acquisition rerun verified for TOI and targeted TIC rows
- deterministic core identifier/alias/search materialization and ARM identity,
  missing-object, current-TOI, and disposition-history tables implemented
- full bootstrap/canonical acceptance build in progress; final accepted,
  quarantine, missing, and planet-link counts belong in its build reports

## Source Policy

Authoritative inputs:

- NASA Exoplanet Archive `toi` TAP table for reproducible TOI snapshots
- ExoFOP-TESS as the freshness/reference destination for current dispositions
  and follow-up context
- MAST targeted TIC queries and TESS observation/product metadata
- Gaia DR3 `dr2_neighbourhood` for TIC Gaia DR2 to Spacegate Gaia DR3 identity
  reconciliation, targeted `gaia_source` rows for distance/scope evidence, and
  targeted Hipparcos/Tycho-2/2MASS best-neighbor tables for the no-Gaia-DR2 tail
- existing Villanova TESS EB snapshot for eclipsing-binary evidence

Rules:

- never assume a TIC Gaia DR2 source ID equals the Gaia DR3 source ID
- do not bulk ingest TIC, CTL, TCE, FFI, target-pixel, or light-curve corpora
- preserve TIC `disposition` and `duplicate_id`; artifact/split/join rows cannot
  create canonical inventory without explicit reconciliation
- exact identifiers outrank positional matching
- multipart resume requires the exact saved query hash; filenames alone never
  authorize cache reuse
- ambiguous identity remains quarantined
- TOI candidates and false positives do not become canonical planets
- raw observation files remain external or in bounded/bulk caches; durable
  metadata, hashes, source URLs, and processing versions remain in Spacegate

## Goal T0: Reproducible Source Snapshot

Deliverables:

- download and manifest NASA `toi` through TAP
- preserve the raw source snapshot unchanged
- cook typed TOI rows with dispositions, identifiers, coordinates, transit
  parameters, stellar context, uncertainties, and source update timestamps
- create a deterministic targeted TIC-ID set from:
  - TOI host TIC IDs
  - current NASA planet-host TIC IDs when available
  - TESS EB TIC IDs
  - reviewed operator/AAA TIC-ID requests
- retrieve only those TIC rows and record query/input hashes
- emit source-delta reports for TOI and targeted TIC rows

Acceptance gates:

- identical pinned inputs produce identical cooked rows
- row counts and disposition totals match the source snapshot
- every cooked row has provenance and a stable source key
- source removal or disposition change is visible in the delta report

## Goal T1: TIC Identity Authority

Deliverables:

- normalize `TIC <id>` as an identifier namespace, not merely display text
- reconcile targeted TIC rows using this precedence:
  1. existing accepted TIC identifier
  2. TIC Gaia DR2 ID through Gaia DR3 `dr2_neighbourhood`
  3. exact HIP/TYC/2MASS/other identifier agreement
  4. proper-motion-aware positional/photometric match
  5. quarantine
- materialize accepted TIC identifiers into `object_identifiers`, aliases, and
  `system_search_terms` with target-object focus metadata
- retain all competing candidates and match evidence in diagnostics

Acceptance gates:

- `TIC <id>` exact search resolves every accepted linked target
- aliases focus the matched star while opening its accepted containing system
- no TIC ID is silently assigned to multiple canonical stars
- no split/join/artifact TIC row bypasses quarantine policy
- representative TOI, TESS EB, bright-star, high-proper-motion, and multiple-
  system goldens pass
- coverage report partitions every targeted TIC ID into accepted, missing from
  Spacegate, ambiguous, TIC artifact/split/join, or source-missing

## Goal T2: Missing Real-Object Recovery

Purpose:

- identify scientifically relevant, real TESS targets that should be in the
  <=1000 ly Spacegate inventory but are absent after identity reconciliation

Deliverables:

- deterministic missing-object report over targeted TIC/TOI hosts
- classify each gap as:
  - existing Spacegate object missed by identity resolution
  - valid Gaia DR3 object excluded by the current slice/profile
  - valid non-Gaia or Gaia-DR2-only target needing accepted-supplement review
  - TIC split/join/artifact/duplicate
  - outside distance/quality scope
  - ambiguous or insufficient evidence
- repair identity/slice policy before adding supplement rows
- allow a narrow reviewed TESS target supplement only for real, in-scope
  objects with sufficient astrometry and provenance

Acceptance gates:

- every in-scope TOI host is accounted for by a canonical object, an explicit
  exclusion reason, or quarantine
- no TOI host is invented from a transit candidate alone
- accepted supplements remain source-labeled and reversible
- inventory additions pass duplicate, provenance, astrometry, and hierarchy
  gates

## Goal T3: TOI Candidate and Transit Evidence

Deliverables:

- ARM tables for current TOI evidence and disposition history
- link `CP` and `KP` rows to canonical planets where identity is deterministic
- preserve `PC` and `APC` as candidate evidence and review-queue inputs
- preserve `FP` and `FA` as negative evidence
- retain transit period, epoch, duration, depth, radius, insolation, and
  uncertainty fields without overwriting stronger accepted planet solutions
- emit explicit unmatched/ambiguous host and planet diagnostics

Acceptance gates:

- confirmed/known TOIs do not duplicate canonical planets
- candidates never appear as ordinary confirmed planets
- false positives and false alarms cannot leak into default planet counts
- disposition changes are append/audit visible
- alternate transit ephemerides remain source-scoped rather than silently
  replacing preferred orbital solutions

## Goal T4: Search and Public Evidence Surface

Deliverables:

- exact TIC and TOI search with member/planet focus context
- system evidence summary showing TESS observation/candidate status without
  promoting unreviewed candidates
- deterministic external links to NASA Exoplanet Archive, ExoFOP, and MAST
- concise candidate/disposition tags only after tag vocabulary is reviewed
- API fields that expose provenance and current disposition timestamps

Acceptance gates:

- paper-style lookups using only TIC or TOI identifiers reach the right object
- system, star, and planet targets are not collapsed into false system aliases
- public surfaces distinguish confirmed, candidate, ambiguous, and rejected
  signals
- no raw catalog table is required for a reader to understand why TESS matters

## Goal T5: Observation Product Index

This goal indexes availability; it does not bulk-download mission products.

Deliverables:

- targeted MAST product inventory for accepted TIC/TOI targets
- sector, cadence, pipeline, product type, data release, URI, size, checksum,
  and observation-time metadata
- explicit product families for light curves, target pixels, data validation,
  TCE summaries, and useful high-level science products
- bounded lazy cache policy with bulk storage under `/mnt/space/spacegate`
- evidence-packet hooks for the AI Astronomy Agency

Acceptance gates:

- Spacegate can answer whether and how TESS observed a selected target without
  downloading its files
- cached products are reproducible from durable metadata
- lost USB cache content is detectable and re-fetchable
- product indexing remains bounded to Spacegate targets and research queues

## Deferred Presentation and Analysis

Light curves should become an observation narrative, not a file browser.

Target presentation:

- lead with the question the observation answers
- show a compact, attributed phase-folded or time-series plot
- explain the visible signal in ordinary language
- separate observed features from fitted/model-derived interpretation
- link the signal to the System Simulation phase where defensible
- leave raw FITS, pipeline, cadence, aperture, detrending, and validation
  details under evidence/technical disclosures

Layer ownership:

- `arm`: source observation metadata, measured/fitted values, quality evidence
- `disc`: reproducible plots, phase-folded products, explanatory blocks
- AAA: cited narrative assembled from reviewed structured evidence
- bulk storage: cached FITS/PDF/pixel/time-series products

The deterministic observation and plot contracts must exist before AAA prose
is allowed to summarize them. The narrator connects evidence; it does not
create the underlying measurement.

## Pinned Future Work

- orbital distinction tags such as inclined, eccentric, edge-on, and
  retrograde will be authored in `docs/TAGS.md` before implementation
- rotation-period evidence should later combine appropriate Gaia/TESS/TARS and
  literature sources in ARM
- source-backed stellar rotation may drive textured-body spin at simulation
  time scale; assumed rotation must remain visibly labeled
- exoplanet rotation is expected to be sparse and must not be inferred as fact
- future flare, spot, transit, eclipse, pulsar-beam, magnetic-field, and
  synchrotron-radiation presentations require separate reviewed render and
  concept contracts

## Exit and Stop Rules

T0 through T3 are the foundation side quest. T4 may ship a minimal identity and
evidence surface. T5 indexes observation availability but does not require a
full light-curve UI.

The side quest must not block Tiled Deep Map work on bulk TIC ingestion,
complete light-curve analysis, rotation integration, textured bodies, or AAA
narration. Unresolved identity tails become queues after the deterministic
matched path and coverage report are complete.
