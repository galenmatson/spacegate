# Spacegate System Simulation Contract

This document defines the beta contract for System Simulation, formerly called
System Simulation v1 and Live System Preview in early implementation notes. The goal is to
let the public web client render deterministic stars, planets, and orbits while
preserving Spacegate's layer boundaries.

## Current Readiness

Already in place:

- public naming: `System Simulation` is the public/runtime name for the
  renderer; internal schema/version identifiers remain explicit and the API route remains
  `/api/v1/systems/{system_id}/simulation-scene`
- public system detail rows with stable `system_id` and `stable_object_key`
- `arm.component_entities`, `arm.system_hierarchy_edges`, `arm.orbit_edges`,
  and `arm.orbital_solutions`
- Admin Object Diagnostics simulation-readiness fields that classify inputs as
  `source`, `derived`, `assumed`, or `missing`
- additive `render_scene_v0.2` payload for live previews with renderer-ready
  star bodies, planet bodies, binary orbit groups, and provenance-bearing fields
- `render_scene.diagnostics` summarizes final rendered body counts, orbit
  endpoint/relation counts, field SOURCE/DERIVED/ASSUMED/MISSING counts, and
  assumption persistence counts for API-level audit checks. It also reports
  `subsystem_handle_counts.source_native` and
  `subsystem_handle_counts.simulation_tree_fallback` so public-edge checks can
  distinguish source hierarchy materialization from acceptable runtime
  fallback handles on stale slices.
- `render_scene.simulation_tree` exposes `simulation_tree_v1`, a derived
  renderer tree with root, barycenter, and body nodes assembled from emitted
  ARM-backed orbit rows. When an outer orbit side exactly matches an inner
  orbit's rendered leaves, the outer node references the inner barycenter
  rather than the individual stars. This lets Castor/HD 213885-style systems
  animate as nested barycentric hierarchies instead of static clusters with
  ad hoc offsets.
- hierarchy-pair period fallback is evidence-aware: source orbital solutions
  win first, MSC system-row periods win second, projected MSC separation plus
  endpoint masses may produce a low-confidence Kepler estimate third, and only
  then does the renderer use a bounded DISC visual period fallback.
- `render_scene_v0.2` reconciles body lists against canonical hierarchy when
  hierarchy exposes richer physical membership than direct core rows or ARM
  orbit endpoints alone
- the first browser renderer uses the hierarchy tree for stable visual cluster
  centers and hosts nested planets around their render host/body group
- hosted planet orbit guides and bodies inherit the full containing host-group
  motion used by the stellar hierarchy renderer, so planets attached to a
  rendered star remain centered on that host through nested group animation
- when `simulation_tree_v1` is active, hosted planet orbit guides, planet
  trails, planet bodies, and host-star habitable-zone bands use the current
  tree body position rather than the legacy static layout center. This keeps
  eps Ind A-style planets/HZ bands attached to the moving A component while
  the brown-dwarf pair follows its own nested barycenter.
- `render_scene_v0.2` attaches core planet render bodies to rendered host stars
  with `host_body_key` when `core.planets.star_id` resolves cleanly; the
  payload records the host-resolution basis for audit/debugging
- simple source-native component leaves such as MSC A/B/C labels can reuse
  matching core star vitals for rendering, and catalog-equivalent core star
  IDs may bridge planet hosts onto those rendered source-native components
- source object identity and accepted root-system membership are separate.
  Alpha Centauri / Proxima Centauri is the benchmark: Proxima remains the
  direct Gaia/source object and planet host for Proxima b/d while MSC/WDS
  component-C evidence rolls it into the accepted Alpha Centauri physical
  system. Search aliases may focus Proxima while opening the owning accepted
  system.
- stellar render bodies preserve compact-object classification through
  `body_class`, nullable `compact_type`, and a source-backed `object_type`
  provenance field while keeping `object_type="star"` as the render role
- `render_scene_v0.2` includes inspectable subsystem bodies for hierarchy nodes
  with multiple rendered stellar descendants. These bodies are UI handles over
  existing hierarchy evidence, not new science inventory; their visible
  component label, hierarchy basis, and rendered-child count are
  provenance-backed fields.
- If explicit hierarchy-backed subsystem bodies are absent, `render_scene_v0.2`
  may derive inspectable fallback subsystem handles from
  `render_scene.simulation_tree` barycenter nodes. These handles are labeled as
  `DERIVED` `render_scene` presentation/runtime structure with
  `source.basis="simulation_tree_fallback_subsystem"`; they do not create
  source-native hierarchy facts and must not replace better ARM subsystem
  evidence when present.
- When source-native hierarchy leaves are available, `render_scene_v0.2` uses
  those leaves as the stellar-body membership authority. MSC/WDS/ORB6 detail
  or orbit endpoints that do not reconcile to source hierarchy leaves are
  preserved as diagnostics/evidence and must not become rendered stars. The
  payload exposes this in
  `render_scene.diagnostics.membership_reconciliation`, including source leaf
  count, rendered stellar body count, active membership gate, and unmatched
  endpoint keys.
- Source-object reconciliation runs before root-system grouping. Reconciled MSC
  component surrogates are audited in `core.source_object_reconciliation`, while
  ambiguous candidates are retained in
  `core.source_object_reconciliation_quarantine`. The simulator consumes the
  resulting accepted membership graph; it must not recreate duplicate rendered
  bodies from quarantined or unmatched source rows.
- Rendered stellar bodies expose `fields.visual_stellar_class` as the material
  and label class used by the simulator. Source spectral evidence wins, then
  defensible temperature/color constraints, compact-object evidence overrides
  main-sequence priors, and mass-only visual classes use the deterministic
  `mass_main_sequence_prior_v1` policy with `status="assumed"` and
  `layer="render_scene"`. This field is not a catalog spectral class.
- the first browser renderer has a single-writer shared pauseable local
  animation clock for all moving scene objects, sampled eccentric/inclined
  orbit guide paths, and hover vitals for rendered bodies; Pause freezes this
  local clock and Start resumes it from the same simulation day. The system
  preview render-policy summary exposes the current local beta day without
  implying science-grade epoch propagation.
- direct binary orbit traces are drawn as the two rendered body paths around
  the visual barycenter, using source mass ratios when both stellar masses are
  available and an explicit equal-mass visual fallback when they are not; the
  trace basis is exposed as a structured simulator provenance field
- hierarchical group-pair motion is also split around the rendered barycenter
  by the summed positive rendered masses of each side when available. If any
  mass is deterministic/procedural rather than source-backed, the trace
  provenance is `ASSUMED`; if either side has no usable mass at all, the
  preview falls back to an explicit equal-mass visual prior. Non-positive
  source mass placeholders are treated as missing in render-scene fields.
- animated bodies advance by mean anomaly and solve Kepler's equation for true
  anomaly before placement, so eccentric preview motion follows the rendered
  orbit path with physically plausible speed variation. This remains a
  presentation-scale Keplerian preview, not a full N-body integration.
- rendered planets are ordered by semi-major axis when available, then orbital
  period, so Sol/TRAPPIST-like systems present in orbital order rather than
  catalog/name order
- the beta browser renderer adds speed control, reset, orbit-trace visibility
  toggle, camera orbit/zoom/pan controls with reset-view support, click/tap
  pinned inspection, truncated copyable render/source identifiers with full
  values preserved for copy/tooltips, a compact mobile pinned-inspection sheet,
  and touch-safe canvas gesture handling
- planet bodies include short animated trail lines in the live preview so
  small bodies remain findable in stricter body-scale views
- habitable-zone bands are visible by default and can be toggled in the live
  preview. They are derived render-scene guides from stellar `luminosity_lsun`
  and broad 0.35-1.70 Earth flux bounds, not climate models or canonical
  habitability claims. When a host star has rendered planets, the band is
  aligned to the median rendered host-planet orbital inclination so compact
  transiting systems such as TRAPPIST-1 do not show the HZ at right angles to
  the planet orbits. HZ display scaling is normalized against both planet
  orbit radii and all rendered HZ outer bounds in the scene so planetless
  multi-star systems do not inflate every HZ band to planet-orbit scale.
- optional formation/freeze-line rings can be toggled in the simulator for
  vaporization, soot, water snowline, carbon dioxide, methane/carbon monoxide,
  and nitrogen boundaries. These are renderer-only luminosity/temperature
  guides based on a simple blackbody-radius approximation, default off, and
  must not be treated as disk-chemistry source facts or canonical habitability
  claims.
- AAA/narration work should separately analyze whether binary/multiple-star
  orbital architecture disrupts a nominal HZ. Sirius A/B is the benchmark case:
  the renderer can draw the broad HZ guide, while narration should explain how
  the companion orbit changes the practical habitability story.
- object labels are visible by default and can be toggled off. Labels are
  renderer-only Drei/Troika SDF text billboards placed just below stars,
  planets, and subsystem handles; HZ labels are placed on the band itself.
  They are screen-size scaled for zoom readability, carry dark outlines for
  contrast, do not participate in picking, and do not create or alter
  science-layer fields. Dense-scene label priority/collision management remains
  future presentation work.
- star labels include a compact color-coded spectral/visual-class badge above
  the star when a source, derived, or explicit visual-prior class is available.
  The badge uses the same provenance-safe class policy as simulator readouts
  and does not turn visual priors into catalog spectral facts.
- the 3D map can open System Simulation as a coordinated drill-in layer. Peek
  mode inspects the selected system in a framed overlay without moving the map
  camera. Explore mode flies the map camera toward the selected system and
  expands the same simulation layer for deeper inspection. This is intentionally
  a two-layer runtime for v1, not yet a single physically continuous map-to-AU
  renderer.
- orientation is surfaced transparently. When the payload has source-backed
  inclination and node-like fields the renderer labels it source-oriented. With
  inclination but no full node/roll evidence it labels partial sky-plane
  orientation. With deterministic visual roll fallbacks it labels assumed roll.
  With no renderable orientation evidence it labels local clarity layout. These
  labels describe the renderer basis; they do not promote display orientation
  into canonical science.
- planet, binary, and group orbit inspection readouts carry the same
  provenance field objects as body readouts, so SOURCE/DERIVED/ASSUMED/MISSING
  pills can be focused/copied from orbit paths as well as bodies; popovers
  expose layer, basis, source catalog/reference, confidence, notes, and
  procedural assumption metadata when present
- the beta scene contract includes `visual_scale_beta_v1`, an explicit
  presentation-scale policy for star radii, planet radii, planet orbit spacing,
  and binary/group orbit display radii; the browser exposes Structure,
  True Orbits, True Bodies, and Log Scale modes and summarizes the active
  local-time, scale-mode, assumption-persistence, and fallback policy. True
  Orbits/Orbit mode keeps planet orbit radii linearly proportional inside the
  scene envelope and therefore uses deliberately tiny presentation body meshes
  so close-in orbits are not swallowed by oversized stars or planets. True
  Bodies mode uses Earth-to-Sun radius conversion for planet meshes relative to
  star meshes, while trails, labels, halos, and pick radii preserve usability.
- selected-system `disc.simulation_assumptions` materialization is available
  through `scripts/materialize_simulation_assumptions.py`; the API annotates
  matching rendered assumptions with `persistence_status="persisted"`
- the browser renderer checks WebGL capability before mounting R3F. If 3D is
  unavailable or the live scene load fails, the preview panel uses the
  deterministic snapshot artifact as a last-resort fallback. Transient WebGL
  context loss inside an otherwise capable simulator panel is treated as
  recoverable: the live canvas remounts with a short recovery notice instead of
  immediately demoting the user to a static snapshot.
- browser QA covers both WebGL-unavailable and failed-scene-load fallback paths
  so the preview panel must render a deterministic snapshot fallback instead of
  a blank or broken canvas
- `render_scene_v0.2` includes `endpoint_kind='group_pair'` orbit entries for
  hierarchical subsystem pairs such as Castor A-B and AB-C; these render as
  cluster orbit guides rather than false individual-star binaries
- the browser renderer visually distinguishes direct binary guides,
  hierarchical group-pair guides, and subsystem handles so complex systems can
  be inspected without flattening the hierarchy into one undifferentiated set
  of rings
- the browser renderer uses `simulation_tree_v1` when available for stars,
  binary/group orbit traces, and subsystem handles. Each barycenter node owns
  one Keplerian presentation transform and child nodes inherit that transform
  recursively, so inner binaries inherit outer subsystem motion without adding
  broad hierarchy-group offsets multiple times.
- `render_scene_v0.2` may emit a single
  `relation_kind='visual_binary_fallback'` orbit for two rendered stars with no
  source orbit edge. This is a DISC presentation assumption for legibility
  only, useful for compact-companion scenes such as Sirius until a reviewed
  orbit edge/solution is available.
- ARM `planetary_orbit` edges and `source_native_planet_orbit` solutions for
  currently host-linked NASA Exoplanet Archive and Sol authority planet rows
- Sol hierarchy/orbit arm rows for planets, moons, selected small bodies, and
  curated artificial objects
- React 19 + Three.js/R3F runtime foundations from the 3D map

Not ready yet:

- full epoch controls and science-grade propagation policy beyond the beta
  clarity-scaled local animation clock
- source-faithful galactic/map alignment for every system simulation; many
  catalog inclinations are relative to observer geometry and lack the complete
  3D orientation needed for an unambiguous galaxy-aligned local scene
- public-edge deployment of the current sliced/rebuilt simulator dataset and
  restored snapshot fallback artifacts
- uncertainty visualization
- physical-scale/precision display modes for stellar radii, planetary radii,
  orbital distances, labels, and time acceleration
- reviewed curation and broader batch policy for persisted `disc` assumption
  rows beyond selected-system materialization

## Public API

Initial endpoint:

- `GET /api/v1/systems/{system_id}/simulation-scene`

Response shape:

```json
{
  "schema_version": "simulation_scene_v0",
  "scope": "system_simulation_scene",
  "generated_at_utc": "2026-06-28T00:00:00Z",
  "frame": "heliocentric_icrs_j2016",
  "system": {},
  "bodies": {
    "stars": [],
    "planets": []
  },
  "hierarchy": {},
  "arm": {
    "components": {"count": 0, "items": []},
    "hierarchy_edges": {"count": 0, "items": []},
    "orbit_edges": {"count": 0, "items": []},
    "orbital_solutions": {"count": 0, "items": []},
    "msc_system_details": {"count": 0, "items": []},
    "stellar_parameters": {"count": 0, "items": []},
    "derived_physical_parameters": {"count": 0, "items": []}
  },
  "simulation_readiness": {
    "score": 0.0,
    "counts": {"source": 0, "derived": 0, "assumed": 0, "missing": 0},
    "required_field_count": 0,
    "status": "missing",
    "stars": [],
    "planets": []
  },
  "render_scene": {
    "schema_version": "render_scene_v0.2",
    "assumption_generator_version": "procedural_prior_v1",
    "visual_scale": {
      "schema_version": "visual_scale_beta_v1",
      "scale_mode": "clarity_scaled_not_physical",
      "default_scale_mode": "structure",
      "available_scale_modes": [
        {"mode": "structure", "label": "Structure/Clarity"},
        {"mode": "true_orbits", "label": "True Orbits"},
        {"mode": "true_bodies", "label": "True Bodies"},
        {"mode": "log", "label": "Log Scale"}
      ],
      "scene_unit": "arbitrary_scene_unit",
      "presentation_only": true,
      "collision_policy": {
        "applies_to_modes": ["structure", "log"],
        "star_radius_fraction_of_nearest_sep": 0.28
      }
    },
    "bodies": {"stars": [], "planets": []},
    "orbits": [],
    "assumptions": [],
    "assumption_count": 0,
    "persisted_assumption_count": 0
  },
  "policy": {
    "canonical_layer": "core",
    "derived_layer": "arm",
    "presentation_assumption_layer": "disc",
    "fiction_overlay_layer": "rim",
    "time_policy": "static_epoch_scene_until_client_simulation_clock_contract",
    "missing_orbit_policy": "do_not_invent_canonical_orbits",
    "agency_policy": "unreviewed_agency_output_must_not_write_core"
  }
}
```

Rules:

- `core` rows are source-faithful canonical inventory/projection rows.
- `arm` rows are deterministic science support, adjudication, hierarchy, and
  orbital-solution rows. Arm may hold source-native rows when they are evidence
  for a canonical model rather than canonical inventory themselves.
- `disc` rows may hold generated rendering assumptions and reproducible
  presentation artifacts.
- `rim` rows are fictional/worldbuilding overlays only.
- Missing orbital data must not be silently replaced with canonical-looking
  values. Visualization defaults belong in `disc` and must be labeled.
- `render_scene_v0.2` may emit deterministic assumptions using
  `procedural_prior_v1`; selected systems can be persisted into
  `disc.simulation_assumptions` by `scripts/materialize_simulation_assumptions.py`.
- `render_scene_v0.2` also exports every rendered `status="assumed"` field in
  `render_scene.assumptions` using the `disc.simulation_assumptions`
  object-binding shape. API records include a stable `assumption_key` and
  `persistence_status`; persisted rows remain presentation assumptions and do
  not become science facts.
- `visual_scale_beta_v1` is a presentation contract. It tells clients how the
  beta renderer exaggerates/normalizes radii and orbit spacing for selectable
  presentation modes, and must not be interpreted as source physical scale.
  The default `structure` mode is collision-safe and hierarchy-first. It caps
  visible stellar mesh radius against nearest rendered stellar separation while
  keeping glow and pick radii separate. `true_orbits` uses a pure linear
  semi-major-axis-to-scene transform with no fixed inner padding, so rendered
  planet orbit radii preserve their source AU ratios inside the current scene
  envelope; close-in worlds may therefore become visually tight. In that mode
  body meshes are intentionally reduced toward marker scale, with halos, trails,
  labels, and pick radii carrying readability. `true_bodies` preserves more
  body-size contrast, and `log` compresses large ranges. All modes are
  browser/render transforms only; source values remain in provenance fields and
  core/ARM rows.
- Future scale work should add a distinct true-physical mode where bodies and
  orbits share one linear scale. This mode is expected to make most bodies tiny
  and most compact inner systems difficult to inspect without extended zoom
  range, halos, labels, and camera aids. It is a reference/education mode, not
  the default readability mode.
- Unreviewed Agency output may propose evidence or assumptions, but must not
  write directly into `core`.

## Simulation Model

The first renderer should treat the endpoint as a scene-description contract,
not as a physics engine.

Minimum body model:

- stable object identity
- display name and aliases
- object type (`star`, `planet`, `moon`, `subsystem`, etc. as rows become
  available)
- parent/child containment from hierarchy edges
- preferred dynamic relation from orbit edges
- planet host linkage through `host_star_id` plus renderer-ready
  `host_body_key` when the canonical planet host star is present in the scene
- physical render inputs with source/derived/assumed/missing status:
  - stellar effective temperature, luminosity, mass, radius
  - planet orbital period, semi-major axis, eccentricity, radius, mass,
    incident flux, equilibrium temperature
- provenance source catalog/version/row key where available

Minimum orbit model:

- attach orbit elements to an `orbit_edge_id`, not only to a body
- include units, reference epoch, and confidence/provenance when present
- support absent elements without fabricating a canonical solution
- treat planet orbits like multiplicity orbits: promoted/default scalar fields
  may exist on core planet rows for serving, but full source-native solutions,
  alternate fits, epochs, uncertainty, and simulation-ready elements belong in
  `arm.orbital_solutions`
- start with static ellipses and clearly labeled uncertainty
- defer true N-body simulation; client animation should be Keplerian/teaching
  grade until a stronger numerical contract exists

## Source Refresh Strategy

Near-term source priorities:

- NASA Exoplanet Archive `ps` / `pscomppars`: canonical confirmed planet
  baseline. `pscomppars` supplies the one-row-per-planet promoted display
  default; `ps` supplies source/reference-specific alternate orbital solution
  rows in ARM when downloaded and cooked.
- Gaia DR3 NSS: current Gaia non-single-star baseline until Gaia DR4 is
  available. Gaia DR4 is scheduled for December 2, 2026, so this should become
  a planned ingest transition rather than an assumption that DR3 remains final.
- WDS and ORB6: visual-binary support evidence and orbital solutions, mapped
  only when Spacegate can attach rows to a unique, confidence-gated system edge.
- MSC: mandatory multiplicity hierarchy evidence. Spacegate now targets the
  upstream June 19, 2026 archive (`newmsc-20260619.tar.gz`). The current archive
  places `sys.tsv`, `orb.tsv`, and `comp.tsv` at the archive root; the cooker
  accepts both that layout and the older `export/*.tsv` layout. Local canonical
  build `20260628T1210Z_msc20260619` promoted on June 28, 2026 and passed the
  required multiplicity golden checks. Spacegate now preserves `sys.tsv` as
  `msc_system_details` and `orb.tsv` as `msc_orbit_details`, materializing MSC
  hierarchy/orbit edges and source-native `arm.orbital_solutions` where
  supported endpoint keys exist. Source-native endpoint labels that appear in
  MSC `sys.tsv`/`orb.tsv` become deterministic ARM leaf nodes when they are not
  exact source subsystem parent labels, even when no flat core star row exists;
  count-expanded leaves are only fallback scaffolding when source endpoint rows
  are absent. Low-mass, spectrumless endpoints are preserved as nonstellar
  support components so the simulator does not inflate stellar multiplicity.
  Castor is the primary regression benchmark for nested AB/C hierarchy, six
  stellar leaves, inner binary periods, and no unsafe spectral inheritance.
  Canonical hierarchy emit must bridge source-native nested MSC
  subsystem edges back into descendant-aware canonical hierarchy leaves, so
  systems such as Nu Sco retain effective star counts even when some source
  leaves are represented as ARM support components rather than direct core
  star rows.
- SBX: current spectroscopic-binary support source. Keep SB9 as historical
  context only; default ingestion should prefer SBX where licensing and format
  checks pass.
- JPL Horizons / SBDB: Sol-system orbital authority for planets, moons, small
  bodies, and selected artificial objects.

Agency-compatible ingest plan:

1. Fetch and checksum current source snapshots into raw/cooked manifests.
2. Normalize source-native rows without adjudication.
3. Materialize deterministic `arm` candidates for hierarchy/orbit/parameter
   evidence with provenance and confidence.
4. Run golden-system checks before promotion.
5. Let Agency/literature workflows propose fixes, but require reviewed
   citations and verdict state before any public `arm` or `disc` materialization.
6. Keep rejected/conflicting proposals in audit tables rather than deleting
   evidence.

## Beta Success Criteria

The beta system preview renderer is scoped to a small target set:

- Sol
- Alpha Centauri
- TRAPPIST-1
- one compact multi-star system with an ORB6/SBX solution
- one messy hierarchy watchlist system

Success criteria:

- each scene renders without blocking the map
- the renderer is lazy-loaded from system detail pages
- browser QA covers at least one live-preview scene path
- planet motion uses source orbital periods when present and deterministic
  seeded phases for reproducible non-aligned starting positions
- planets with missing source inclination use a deterministic
  `disc_assumption` render fallback in `render_scene`; when same-host source
  planet inclinations exist, the fallback uses a coplanar visual prior with a
  tiny seeded offset, otherwise it uses the older centered low-tilt prior.
  Readiness diagnostics may still report the underlying source inclination as
  missing
- TRAPPIST-1 benchmark scenes source period, semi-major axis, eccentricity, and
  inclination from `arm.orbital_solutions` before falling back to promoted
  `core.planets` summary scalars
- multi-star previews render ARM hierarchy/orbit evidence as barycentric visual
  groups when connected binary edges are available
- hierarchical subsystem orbit edges render as distinct group-pair guides so
  nested structure is visible without flattening the system into sibling stars
- benchmark hierarchical scenes require `render_scene.simulation_tree` with
  nested barycenter nodes and no unattached orbit rows for compact triples such
  as HD 213885, HD 79210, and eps Ind A
- Nu Sco acts as the messy hierarchy browser benchmark: seven source-native
  leaves, subsystem handles, direct/group orbit guides, and unresolved children
  without inherited source-like spectral facts
- group-pair edges also drive deterministic visual-scale child-cluster motion in
  the browser preview. This is a mass-weighted presentation transform over ARM
  evidence when positive side masses are available, not a source-scaled
  ephemeris or full N-body solution
- single-star scenes can render a human system alias as the body display name
  while preserving the canonical star key and exposing the display-name basis
- direct binary stars follow the same rendered body-path traces that are shown
  in the preview, rather than a full relative-separation guide; mass-weighted
  traces are a derived presentation transform and equal-mass traces are labeled
  as an assumed visual fallback
- users can pause/start, change speed, reset the local clock, hide/show orbit
  traces, orbit/zoom/pan the preview camera, reset the view, hover
  bodies/orbits, and pin a copyable object/orbit readout
- the compact render-policy summary covers local beta time, active scale mode,
  assumption persistence, and deterministic snapshot fallback; the standalone
  local-days readout pill was removed as redundant with that Time field
- missing-input counts remain available in readiness/API diagnostics, but are
  not shown as a primary simulator pill because they are mainly a data-quality
  debugging signal
- the detail-page side rail prioritizes rendered object chips for stars,
  subsystems, and planets. These chips are inspection/navigation affordances
  derived from the `render_scene` payload; they do not create new hierarchy
  evidence or alter `core`/`arm` records.
- pinned stars, planets, and orbit paths also receive in-scene visual feedback
  so the selected readout has a visible target in the 3D view
- hover and pinned readouts use the same source/derived/assumed/missing
  evidence-pill affordance as the summary panel, including focusable provenance
  popovers in pinned readouts
- orbit path hover/pin readouts include explicit guide/trace provenance and
  the shared raycaster uses a wider line threshold so orbit inspection is not a
  pixel hunt
- stellar class readouts use field-backed provenance instead of treating every
  display class as source; renderer diagnostics flag any source-like class
  without component-specific spectral evidence
- browser diagnostics expose registered inspectable star, planet, subsystem, and
  orbit-target counts so Playwright can catch lost hover/pin coverage without
  relying on fragile pixel-perfect click coordinates
- the preview canvas preserves its drawing buffer so browser QA can perform
  pixel-level nonblank checks across benchmark systems without relying only on
  DOM presence or scene metadata
- planet render bodies, hover/pinned readouts, and the preview evidence strip
  surface an API-backed renderer-only planet visual class with `render_scene`
  provenance, so material choices are inspectable without treating them as
  science taxonomy
- star and planet surfaces use deterministic procedural renderer materials
  based on existing scene fields and stable object keys; they are visual
  presentation only, not source surface maps or persisted assumptions
- planet and star radii use bounded presentation caps/floors so compact systems
  remain inspectable in the beta preview; Structure mode additionally caps
  visible star meshes against nearest rendered stellar separation, while halos
  and picking radii remain separate readability aids. `visual_scale_beta_v1`
  documents the active transform and fully physical rendering remains future
  work
- compressed presentation modes may cap the displayed eccentricity of planet
  orbit paths when neighboring rendered orbit spacing would otherwise make
  visible paths cross. Source eccentricity remains unchanged in the provenance
  readout; the capped display eccentricity is labeled as a `render_scene`
  derived presentation value.
- toggled habitable-zone overlays are labeled, hover-inspectable presentation
  aids. Their readouts expose luminosity, inner/outer AU bounds, planet-plane
  alignment basis, and broad-flux basis from the render-scene calculations
  while avoiding click/drag handlers that could block camera controls.
- default object labels use SDF text billboards rather than canvas texture
  sprites so zoomed-out system names remain sharper and browser QA can assert
  the active `sceneLabelRenderer` diagnostic.
- WebGL-disabled browsers receive the deterministic system snapshot in the live
  preview panel instead of a blank or broken canvas
- WebGL context loss inside a simulator panel is trapped and the live canvas is
  remounted. After repeated unrecoverable panel-level context loss, the panel may
  demote to the deterministic snapshot artifact rather than spinning forever.
  Star Search cards are live-preview-first through bounded preview pools: the
  Star Map owns the pool for map-native search cards, while standalone Star
  Search v2 owns a separate small pool for `/search`. Both reuse captured first
  frames as cached card imagery and promote a card back to live simulation on
  hover/focus when budget is available. Deterministic snapshots remain
  last-resort no-WebGL/load-failure/repeated-failure artifacts, not the
  preferred context-loss response. Bulk browser-rendered PNG snapshot generation
  was tested and removed because headless WebGL rendering was too slow and
  CPU-heavy for routine presentation updates.
- The original deterministic concentric-ring snapshot generator is now legacy
  fallback/reference infrastructure. It should not be expanded as a public
  visual target; the next static path should be a high-fidelity generator that
  consumes the same simulation-scene contract as the live renderer. Admin
  controls for the old generator can be hidden or removed after that fallback
  path covers no-WebGL, crawler, share-card, and reference needs.
- Formation/freeze-line hover readouts use the same explanatory text as the
  Lines disclosure controls, so the snowline and other chemistry boundaries
  read as teaching overlays rather than generic radius guides.
- `/api/v1/systems/{system_id}/simulation-scene` uses a small in-process LRU
  cache keyed by served build and system id. This avoids recomputing ARM
  diagnostics, readiness, assumptions, and render-scene contracts when users
  scroll back through recently previewed systems.
- `scripts/materialize_simulation_scenes.py` can prebuild compressed
  `disc/simulation_scenes/system_<system_id>.json.gz` artifacts for hot search
  systems. The API serves those artifacts first, then falls back to the
  in-process LRU cache, then runtime assembly. This removes first-pass CPU
  spikes for materialized systems without changing the simulation-scene
  contract. Photon default hot-cache command:
  `SPACEGATE_STATE_DIR=/data/spacegate/state .venv/bin/python scripts/materialize_simulation_scenes.py --limit 1000 --sort distance --max-dist-ly 100`.
- Prebuilt scene artifacts are renderer-contract artifacts, not canonical
  source data. The API may bypass stale artifacts when they lack current
  required diagnostics such as `membership_reconciliation`, falling back to
  live assembly until scenes are regenerated for the served build.
- Star Search and map search now use explicit preview tiers:
  - Tier 0: summary fields only.
  - Tier 1: `lightweight_singleton`, a client-rendered singleton preview for
    ordinary one-star/no-planet/no-exotic systems.
  - Tier 2: `prebuilt_simulation_scene`, a full scene served from
    `disc/simulation_scenes/`.
  - Tier 3: `dynamic_simulation_scene`, a full scene assembled from the live
    API contract and then cached in process.
  The tier is a presentation/runtime decision only. It does not assert that a
  system is scientifically simple beyond the visible preview policy.
- For preview-scene prebuilds, use
  `SPACEGATE_STATE_DIR=/data/spacegate/state .venv/bin/python scripts/materialize_simulation_scenes.py --priority-profile search-preview --sort coolness --limit 2000 --max-dist-ly 100`.
  The `search-preview` profile selects planet hosts, multistar systems,
  compact/white-dwarf style systems, public UX goldens, and top coolness-ranked
  systems when coolness data is available.
- True-orbit, true-body, and log scale modes allow much closer camera zoom
  than structure mode so users can inspect inner systems inside wide-orbit
  systems. This changes only camera control limits; it does not alter the
  visual scale transforms or science values.
- Habitable-zone and condensation/freezing-line toggles are grouped in a
  compact Lines disclosure to keep the simulator controls readable while
  retaining tooltips for each line.
- On Star Search v2 system pages, System Simulation is the primary visual
  anchor immediately after the compact hero. Reader-facing At-a-Glance facts
  and narrative cards surround it, while raw coordinates, low-level source
  provenance, and snapshot metadata live in secondary Evidence and Technical
  Data disclosures.
- `scripts/verify_snapshot_fallback.py` verifies that a served build advertises
  map snapshot coverage and that sampled detail snapshot URLs resolve to SVG
  fallback assets
- source/derived/assumed/missing fields surface as visible provenance pills
- every rendered assumption is visible in the readiness/render payloads
- every rendered assumption is exported as a structured render-scene assumption
  record suitable for selected-system `disc.simulation_assumptions`
  materialization
- benchmark simulator assumptions are materialized in the current DISC artifact
  with `simulation_assumptions_materializer_v1`; broader reviewed curation and
  batch policy remain future work
- static snapshots remain the fallback for browsers/devices without usable 3D
- no `rim` artifacts or fictional orbits are mixed into science scenes

Resolved local benchmark blockers:

- Local build `20260630T_sim_beta_api_alias_v4` first made Sirius a valid
  compact-companion benchmark, and current local served build
  `20260630T_sim_beta_sol_smallbody_v1` retains it: Sirius A is a reviewed
  `athyg_accepted_supplement` core row, Sirius B remains the Gaia white-dwarf
  row, WDS components are linked, and accepted-supplement AT-HYG aliases such
  as `Sirius`, `Alpha Canis Majoris`, `Alp CMa`, and `9 CMa` resolve at both
  star and system levels. This is not simulator fabrication. It is a reviewed
  core inventory exception plus ARM/API hierarchy handling. The beta
  render-scene contract also verifies Sirius B as a `white_dwarf`
  `body_class`/`compact_type` render body with a source-backed compact
  object-type provenance field. Public antiproton still needs a safe sliced
  deployment before the fix is public.
- June 30, 2026 Sol authority source-refresh fix makes Horizons small-body
  commands explicit for asteroid/TNO/dwarf-small-body records and adds
  source/build/API gates for Ceres, Vesta, Pallas, Juno, Hebe, Iris,
  Interamnia, and Hector. This prevents the live simulator from rendering
  Ceres/Vesta-class objects with Mercury/Mars/Venus/Earth/Saturn/Uranus-like
  orbital solutions after ambiguous bare Horizons numeric commands.
