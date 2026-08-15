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

The finalized projection contains 22,836 binding outcomes, 15,747 evidence
rows, and 16,064 selected or explicitly unresolved leaf rows. Selected values
include 23 DEBCat dynamical masses, 1,310 MSC publication masses, 12,054 MSC
source-model masses, 818 Gaia model masses, and 48 NASA host masses. This
includes 269 accepted white-dwarf leaf selections across applicable source
families. Seven equal-authority conflicts remain explicit and 1,804 relevant
leaves remain missing.

An intermediate full A/B exposed an overly narrow population boundary before
promotion. All 8,429 leaves whose prior display class used an MSC component
mass are now accounted in the typed evidence projection. Of those leaves,
7,758 retain an accepted point mass and the corresponding mass-based display
class. Two have equal-authority mass conflicts. The remaining 669 lose the
class because their only mass is not a selectable point estimate: 640 carry a
minimum-mass lower bound, 27 have an unresolved MSC mass method, and two carry
a subsystem sum that is inapplicable to an individual leaf. Those values remain
preserved as evidence rather than disappearing or being promoted into orbital
inputs.

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

`stellar_leaf_mass_selection_v3` selects relevance from the union of accepted
relation endpoint-to-leaf bindings and exact leaves with eligible bound
component mass evidence. It does not infer identity from names or copy a source
system mass to a component. Regression controls cover both a WDS source
relation whose exact MSC leaves live under a Gaia runtime system and a bound
mass leaf that is not currently used by an eligible physical relation.

An end-to-end candidate scene demonstrates two MSC source-model endpoint masses
of 1.43 and 1.16 solar masses feeding
`kepler_axis_from_period_total_mass_v2`. The accepted 343.56-day period produces
a 1.318 AU relative semi-major axis. The scene retains both evidence IDs,
parameter-set IDs, source-model status, total mass, independent orientation and
phase status, and the fact that the axis is derived rather than fitted.

## Determinism

Two clean finalized ARM compiles to separate roots produced the same immutable
build ID `e0560ce5c334e461945732c9`. All 46 table schemas, row counts, and
order-independent logical hashes match. DuckDB file byte hashes differ because
physical page layout and generation metadata are not the scientific
determinism contract.

The compiles took 207.94 and 186.71 seconds of compiler wall time, peaked at
about 46.5 GiB RSS, and produced 13.81 GB ARM databases. The
independent ARM verifier passes every identity, scope, applicability,
containment, lineage, orbit, TESS, solar-system, and inventory check.

## Scene And Scientific A/B

Candidate D materialized all 7,719 policy-selected v20 scenes with no failures.
The closure took 2,273.31 seconds with 24 workers and produced 93,190,086 bytes
of compressed scene artifacts. The forkserver worker pool does not expose child
CPU and RSS through the parent process counters, so the materializer's small
reported parent RSS is not treated as total memory use. Host observation during
the build peaked near 72 GiB used with about 50 GiB still available and no OOM.

The clean scientific A/B accounts for all 9,307 stellar relations:

| State | Baseline | Candidate D | Delta |
| --- | ---: | ---: | ---: |
| Physically scalable | 1,431 | 7,366 | +5,935 |
| Accepted source axis | 1,423 | 1,059 | -364 |
| Kepler-derived axis | 8 | 6,307 | +6,299 |
| Rejected | 135 | 22 | -113 |
| Unavailable, excluding rejected | 7,741 | 1,919 | -5,822 |

There are 5,938 relation-level recoveries. Of these, 5,825 move from
unavailable to derived, 113 move from rejected to derived, and 364 previously
accepted axes fail the mass and period coherence test but retain physical scale
through a defensible Kepler derivation. The remaining 22 rejected relations
imply implausible stellar-system masses. Another 477 source axes fail coherence
but have complete selected endpoint masses and therefore use the derived axis.
No coherence check was bypassed.

Of the 7,489 baseline relations with an accepted period but incomplete endpoint
masses, 5,822 now have a physical extent and 1,667 remain unavailable because
at least one exact endpoint still lacks an applicable selected mass. Another
252 relations have no accepted axis, accepted period, or projected separation;
the compiler leaves them structural rather than inventing an orbit.

Three legacy derived axes are deliberately retired rather than counted as
regressions. Two used a spectral-class mass prior that did not pass the new
exact-leaf applicability policy. The third used a selected mass attached to a
canonical parent star that the hierarchy resolves into a subsystem, so it is
not an exact mass for the relation endpoint. The A/B gate records these under
`legacy_kepler_axis_used_unselected_endpoint_mass`; it still fails any loss of
an accepted source axis or a derivation backed by the shared selected-mass
projection. There are zero true regressions.

The final independent candidate E scene closure, scene logical determinism,
named-system goldens, runtime latency measurements, local promotion and
rollback evidence, and final retention disposition are recorded when their
active gates finish.

Machine reports are stored under:

`/data/spacegate/state/reports/system-simulation-physical-scale-v1/physical-extent-coverage-v1/`

Large clean-build artifacts are staged under:

`/space/spacegate/physical-extent-coverage-v1/`
