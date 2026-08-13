# System Simulation Physical Scale v1 Verification

Date: 2026-08-13  
Branch: `feature/system-simulation-physical-scale-v1`  
Science/Public Read build: `e7_524b4c016779a77bc9780053_full_public`  
Scene artifact: `simulation_scene_artifact_v16`

## Decision

M8.3e.3a is accepted locally for review. Physical Orbits uses one linear AU
transform inside the active focus. Structure remains schematic, Local Orbit is
a focus-local presentation envelope, Body Contrast preserves readable bodies,
and Log is explicitly nonlinear. Body meshes, halos, labels, pick targets,
scale beacons, and the scale lens are presentation aids rather than physical
body-size claims.

No canonical science, selected evidence, system membership, or planet inventory
was changed. The current immutable science build remains intact.

## Contract Results

The API contract audit passed Alpha Centauri, Castor, Sol, TRAPPIST-1, eps Ind,
and HD 57041. Every target exposes:

- `simulation_physical_scale_v1`;
- `simulation_focus_graph_v1`;
- `visual_scale_v2`;
- a root focus with a defensible recursive physical bound;
- an explicit physical, unavailable, or rejected state for every orbit;
- exclusion of display radius, projected separation, and static hierarchy
  placement from physical semi-major-axis selection.

Two independent in-process assemblies produced identical physical/focus logical
hashes for all six systems. Their focus graphs contain 4 to 15 nodes. The
controls deliberately include both complete and incomplete physical extent
cases. The final audit explicitly accounts for 23 physical planet orbits across
Alpha Centauri, Sol, TRAPPIST-1, and eps Ind, in addition to the applicable
stellar and hierarchical orbits. A planet without a defensible physical axis
uses a labeled unavailable marker rather than a fallback radius.

## ORB6 Unit Finding

The audit found that ORB6 unit flag `m` denotes milliarcseconds while `a`
denotes arcseconds. The previous stellar-orbit runtime compiler treated both as
arcseconds. Compiler policy `2026-08-13.e7-stellar-orbit-runtime.4` now divides
`m` axes by 1,000 before converting angular axes to AU and records that
normalization in lineage.

The isolated complete compiler audit accounts for 116 milliarcsecond axes and
1,042 arcsecond axes, with zero populated unrecognized units and no inventory
gate changes. The runtime contract adds an independent period/axis coherence
test: an axis that implies an implausible total mass is retained as rejected
evidence and shown as unavailable in Physical Orbits. The corrected source
values enter the next immutable science build; this milestone does not rewrite
the currently served build.

## Browser Performance

The retained master frontend and v16 frontend were measured with the same
Alpha Centauri scene, Photon API, headless Chromium, and desktop 1440, desktop
4K, and mobile 412 profiles. Headless frame intervals reflect the available
software-rendering cadence and are useful as relative gates, not a claim about
real-device GPU frame rates.

| Profile | Structure median before / after | Structure p95 before / after | Ready before / after | Selection before / after | Lens p95 | Contexts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Desktop 1440 | 33.3 / 33.3 ms | 33.4 / 50.0 ms | 1,457 / 980 ms | 175 / 86 ms | 50.0 ms | 1 |
| Desktop 4K | 83.4 / 83.4 ms | 100.1 / 100.1 ms | 1,170 / 970 ms | 210 / 193 ms | 100.1 ms | 1 |
| Mobile 412 | 16.7 / 16.7 ms | 16.8 / 16.8 ms | 1,140 / 956 ms | 234 / 88 ms | 16.8 ms | 1 |

The measured budget accepts a normal-mode median no more than 25 percent above
baseline, p95 no more than 50 percent above baseline or one 50 ms cadence step,
selection below 300 ms, heap below 64 MB or 125 percent of baseline, nonblank
canvases, no console errors, and a lens p95 within 25 percent of current
Structure mode. All profiles pass. The lens uses one canvas and one WebGL
context and mounts its manual second viewport only while open.

## Interaction And Visual Results

- Desktop double click enters a stable nonroot focus and Back restores root.
- Explicit Focus, Fit System, Parent, Back, siblings, and breadcrumbs pass.
- The lens supports pinning, bounded zoom, Focus, Open, Escape, and viewport
  containment without taking right-drag panning.
- Physical scale beacons are bounded and collision managed.
- All eight themes render nonblank Physical mode screenshots.
- Mobile renders the ruler, navigation, and lens within the viewport with one
  WebGL canvas.
- Focus changes preserve selection, simulation time, and presentation settings.

## Evidence

Machine reports are under:

`/data/spacegate/state/reports/system-simulation-physical-scale-v1/`

Key files:

- `baseline/scale-contract.json`
- `after/scale-contract-v16-final.json`
- `after/determinism-v16-final.json`
- `orb6-unit-normalization.json`
- `browser/before-master-structure.json`
- `browser/after-v16-structure.json`
- `browser/after-v16-physical-lens.json`
- `browser/performance-comparison-v16.json`
- `screenshots/alpha-centauri-physical-beacons-desktop.png`
- `../map_playwright/system-physical-scale-v16-final/`

The scene runtime cache is regenerable. The complete v16 policy set is warmed
with the resumable admin materializer before branch closeout. A frozen public
deployment artifact is intentionally deferred to a later reviewed release.
