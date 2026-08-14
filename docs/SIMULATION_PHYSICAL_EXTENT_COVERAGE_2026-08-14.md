# Simulation Physical Extent Coverage Recovery

Date: 2026-08-14  
Milestone: M8.3e.3b Physical Extent Coverage Recovery v1  
Status: verification in progress on Photon; not deployed

## Purpose

M8.3e.3b repairs a scientific projection gap rather than relaxing the meaning
of Physical Orbits. The prior runtime retained exact MSC component masses well
enough to derive 8,429 component classifications, but it did not expose those
mass inputs through a typed, selected leaf-parameter contract. A relation could
therefore carry an accepted period while its exact endpoint masses appeared
missing to the physical-scale compiler.

Physical Orbits remains restricted to an accepted coherent source axis or a
Kepler-derived relative axis from an accepted orbital period and a complete,
applicable endpoint mass set. Projected separations, static hierarchy offsets,
display radii, and procedural mass priors remain ineligible.

The requested `docs/INGEST_V2.md` name is not present in this checkout. The
active ingest direction was reviewed through `docs/CANONICAL_INGEST.md`,
`docs/INGEST_RECOVERY.md`, and `docs/EVIDENCE_LAKE_V2.md` instead.

## Reproduced Baseline

The baseline was rebuilt as a clean scene closure rather than inferred from a
mixed runtime cache. It contains:

| State | Relations |
| --- | ---: |
| Stellar relation rows | 9,307 |
| Physically scalable | 1,431 |
| Accepted source axes | 1,423 |
| Legacy Kepler-derived axes | 8 |
| Rejected by coherence checks | 135 |
| Unavailable, excluding rejected | 7,741 |
| Accepted periods with incomplete endpoint masses | 7,489 |
| No accepted axis, period, or projected separation | 252 |

The complete breakdown by source, relation kind, endpoint scope,
classification, distance availability, hierarchy depth, and mass availability
is recorded in `baseline-v18-current-arm.json` under the machine-report root.

## Exact Leaf Projection

Runtime ARM now contains three versioned tables:

- `stellar_leaf_parameter_binding_outcomes` accounts for every attempted
  source-to-leaf binding;
- `stellar_leaf_parameter_evidence` preserves every bound candidate and its
  applicability;
- `stellar_leaf_selected_parameters` provides one accepted, conflicted, or
  missing mass decision per relevant exact leaf.

The finalized projection contains 22,836 binding outcomes, 15,110 evidence
rows, and 15,503 selected or explicitly unresolved leaf rows. Selected values
include 23 DEBCat dynamical masses, 1,255 MSC publication masses, 11,626 MSC
source-model masses, 749 Gaia model masses, 43 NASA host masses, and 249
white-dwarf leaf selections across all applicable source families. Seven equal
authority conflicts remain explicit and 1,800 relevant leaves remain missing.

MSC mass method codes follow the pinned catalog documentation. Publication
values (`r`) are source evidence; magnitude, color, spectral, and mass-ratio
estimates (`v`, `k`, `a`, and `q`) are source-model evidence; subsystem sums
(`s`) apply only to an exact unresolved subsystem; minimum secondary masses
(`m`) remain lower bounds; and unknown methods remain preserved but excluded.

## Runtime Identity Repair

The first candidate audit found a general alias-boundary defect. Some MSC
relations retain a WDS source-system identity while their exact runtime leaves
belong to a canonical Gaia-based system. Filtering relevant leaves by equal
system keys discarded otherwise accepted endpoint masses.

`stellar_leaf_mass_selection_v2` now selects relevance from the accepted
relation endpoint-to-leaf bindings. It does not infer identity from names or
copy a source system mass to a component. A regression control covers a WDS
source relation whose two exact MSC leaves live under a Gaia runtime system.

An end-to-end candidate scene demonstrates two MSC source-model endpoint masses
of 1.43 and 1.16 solar masses feeding
`kepler_axis_from_period_total_mass_v2`. The accepted 343.56-day period produces
a 1.318 AU relative semi-major axis. The scene retains both evidence IDs,
parameter-set IDs, source-model status, total mass, independent orientation and
phase status, and the fact that the axis is derived rather than fitted.

## Determinism

Two clean finalized ARM compiles to separate roots produced the same immutable
build ID `573ed7899255f17b9ca83283`. All 46 table schemas, row counts, and
order-independent logical hashes match. DuckDB file byte hashes differ because
physical page layout and generation metadata are not the scientific
determinism contract.

The compiles took 191.66 and 189.26 seconds of compiler wall time, peaked at
about 46.4 GiB RSS, and produced approximately 13.8 GB ARM databases. The
independent ARM verifier passes every identity, scope, applicability,
containment, lineage, orbit, TESS, solar-system, and inventory check.

## Scene And Scientific A/B

The final two clean v19 scene closures, coverage A/B, named-system goldens,
runtime latency measurements, local promotion and rollback evidence, and final
retention disposition are recorded below when the active materialization gate
finishes.

Machine reports are stored under:

`/data/spacegate/state/reports/system-simulation-physical-scale-v1/physical-extent-coverage-v1/`

Large clean-build artifacts are staged under:

`/space/spacegate/physical-extent-coverage-v1/`

