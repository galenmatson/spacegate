# Simulation Physical Extent Coverage Recovery

Date: 2026-08-14  
Milestone: M8.3e.3b Physical Extent Coverage Recovery v1  
Status: accepted and locally promoted on Photon; not deployed

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

The independent D and E public projections contain identical schemas, counts,
representation policies, source accounting, and logical hashes for systems,
stars, search terms, and stellar badge overlays. Each prebundle SQLite artifact
is 16,378,339,328 bytes. Their public build IDs differ because each build retains
its own immutable runtime-bundle lineage; that identity difference is not
normalized into scientific equality.

Both scene closures contain the same 7,719 policy-selected systems and the same
logical scene-set hash
`a3596ddcd1a3d0d1a17064ec30b39bd9df2a531a258426784d65258d07bf023e`.
The comparator replaces a public build ID only where a field exactly matches the
scene's top-level build identity. It replaces a build-keyed assumption key only
after recomputing and matching the documented hash, and it canonicalizes only
the explicitly set-like ARM component and hierarchy-edge diagnostic lists. All
scientific values, source build IDs, and ordered renderer contracts remain
byte-sensitive. Zero scenes differ under that policy.

## Scene And Scientific A/B

Candidate D materialized all 7,719 policy-selected v20 scenes with no failures.
The closure took 2,273.31 seconds with 24 workers and produced 93,190,086 bytes
of compressed scene artifacts. The forkserver worker pool does not expose child
CPU and RSS through the parent process counters, so the materializer's small
reported parent RSS is not treated as total memory use. Host observation during
the build peaked near 72 GiB used with about 50 GiB still available and no OOM.
Candidate E independently materialized the same 7,719-scene closure with zero
failures in 2,252.71 seconds. Its compressed artifacts occupy 93,192,087 bytes,
compared with 93,190,086 bytes for D; the small byte difference is build-keyed
metadata, while the verified logical scene set is identical.

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

Candidate D's cache contains 10,182 physically applicable orbit extents and
1,951 explicitly unavailable extents across 9,307 stellar and 2,826 planet
orbits. Its focus graph contains 38,335 nodes: 2,460 roots have complete physical
bounds, 3,361 are intentionally partial, and 1,898 remain unavailable. Candidate
E reproduces every count.

## Artifact Closure

Candidates D and E each contain all 14,141 required hierarchy bundles with no
failures. Both closures contain 70,727,957 compressed bytes, 342,515,482
uncompressed bytes, and logical SHA-256
`5920e5c4bcc6a2fd443ba6317335c984c2d4b0d98502cb31c1d35aba9c696f4a`.
Candidate D built the closure in 3,723.92 seconds with 16 workers. Candidate E
resumed from 3,200 verified bundles and completed in 2,635.71 seconds. SQLite
integrity and the full artifact verifier pass for both. Public Read hierarchy
bundles are a required postprojection artifact; the promotion preflight caught
their absence in the initial candidate copy and blocked cutover before any
served pointer changed.

Candidate D's final Smart Tag artifact attaches to the complete hierarchy
closure. Its hot SQLite projection is 682,520,576 bytes, portable assignments
are 295,356,814 bytes, and the complete artifact is 978,966,758 bytes. It
contains 11,430,393 assignments, 11,425,606 system memberships, 90,044 exact
source contributions, and no quarantine rows. The live source and tag smoke
test passes.

## Local Acceptance

Candidate D is locally served at `https://10.0.0.12`. The live physical audit
passes Alpha Centauri, Castor, Groombridge 34, Nu Scorpii, AR Cas, `32alf Leo`,
Sol, TRAPPIST-1, a wide multiple, an evolved-star control, and compact-object
and incomplete-mass controls. The accepted source-style alias `32alf Leo`
resolves; the prose form `32 Alpha Leonis` is not currently a registered alias
and was not added as a milestone exception.

The rollback drill promoted the retained
`e7_524b4c016779a77bc9780053_full_public` build, verified its health and build
identity, then re-promoted candidate D and verified it again. Exact TIC and TOI
search, deferred identifier behavior, known-system API checks, hierarchy and
nested orbit checks, map and scene verification, and the full repository suite
pass. The final suite contains 607 Python tests, six Node subtests, 11 simulator
tests, tile tests, a production web build, and desktop, mobile, touch, keyboard,
4K, ultrawide, all-theme, screenshot, and canvas-pixel coverage.

## Performance

| Operation | Result |
| --- | --- |
| Runtime ARM D / E | 207.94 / 186.71 s; about 46.5 GiB peak RSS |
| Public slice D | 270.32 s; 32,958,544 KiB peak RSS |
| Scene closure D / E | 2,273.31 / 2,252.71 s; 7,719 scenes each |
| Hierarchy closure D / E | 3,723.92 / 2,635.71 s; 14,141 bundles each |
| Final Smart Tags | 389.47 s; 397,996 KiB peak RSS |
| Warm exact search c1 | 12.86 ms p95; zero errors |
| Warm fuzzy/filtered search c1 | 181.47 ms p95; zero errors |
| Summary and hierarchy c1 | 15.27 ms p95; zero errors |
| Prebuilt scenes c12 | 89.52 ms p95; zero errors |
| Mixed full stack c6 | 1,819.49 ms p95, 26.99 req/s; zero errors or OOM |

The complete candidate build occupies 24,266,357,686 bytes, its final Public
Read directory occupies 16,455,125,559 bytes, and its Smart Tag artifact
occupies 978,966,758 bytes. The clean D and E reproducibility roots each occupy
about 54 GB. No served, rollback, evidentiary, or unique-source artifact was
deleted. The retention dry run identifies only explicit postacceptance
candidates.

Machine reports are stored under:

`/data/spacegate/state/reports/system-simulation-physical-scale-v1/physical-extent-coverage-v1/`

The consolidated build and runtime measurement report is
`performance-summary-v1.json` in that directory.

Large clean-build artifacts are staged under:

`/space/spacegate/physical-extent-coverage-v1/`
