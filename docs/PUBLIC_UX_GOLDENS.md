# Public UX Goldens

Public UX goldens are not ingestion goldens. They are user-experience review
targets for Star Search v2, System Simulation, and simulation-first system
pages. They answer a different question: can a curious visitor search for a
recognizable system, understand why it matters, inspect the simulation, and
open evidence without being crushed by raw catalog fields?

## Public Experience Goldens

These systems should be used for layout, search relevance, narrative staging,
simulation quality, and data clarity checks:

| System | Purpose | Current local status |
| --- | --- | --- |
| Tau Ceti | Nearby exoplanet system; layperson narrative benchmark | Resolves |
| TRAPPIST-1 | Compact seven-planet system; orbital and HZ teaching benchmark | Resolves |
| Alpha Centauri | Nearest famous multi-star system; map-to-system benchmark | Resolves as the accepted Alpha/Proxima physical system with Proxima planets attached to Proxima |
| Proxima Centauri | Nearest known exoplanet host; Alpha/Proxima relationship watch item | Resolves into the accepted Alpha Centauri system with Proxima member context |
| Sirius | Bright public-recognition benchmark with compact companion | Resolves |
| 55 Cancri | Multi-planet benchmark for search, cards, and simulation ordering | Resolves |
| Epsilon Eridani | Nearby K-star exoplanet system | Resolves |
| Barnard's Star | Famous high-proper-motion nearby system | Resolves |
| Wolf 359 | Nearby red dwarf and public-recognition benchmark | Resolves |
| Vega | Bright public-recognition benchmark | Known current gap: `Vega`, `Alpha Lyrae`, `HD 172167`, and `HIP 91262` are absent from the current served core/source alias coverage |
| Fomalhaut | Bright debris-disk/public-recognition benchmark | Resolves |

## Alias Authority Goldens

These checks focus on names, identifiers, and public display-name policy rather
than page layout:

| Query | Expected behavior |
| --- | --- |
| `Gliese 412` / `GJ 412` | Resolve to the nearby Gl/GJ 412 source object, not `Gliese 12` or `GJ 4122` |
| `Gliese 643` | Resolve into the V1054 Oph accepted system / WDS 16555-0820 context |
| `VB 8` | Resolve into V1054 Oph member context |
| `Alpha Librae` / `Zubenelgenubi` / `alf02 Lib` | Resolve to WDS 14509-1603, preferring a human-readable display name over the abbreviated Bayer token |
| `HD 128620` / `HIP 71683` | Resolve exactly to Alpha Centauri while keeping the catalog ID secondary |
| `V1513 Cyg` | Must not fuzzy-resolve to `V1581 Cyg`; unresolved is acceptable until a real source alias is present |

`scripts/verify_alias_authority.py` owns the API-level version of these
goldens. The Playwright map suite includes a lighter public smoke check.

## Technical Stress Goldens

These remain useful for simulator and hierarchy stress checks, but they should
not be mistaken for the public UX set:

| System | Purpose |
| --- | --- |
| Castor | Nested multiplicity, source-native hierarchy, binary dynamics |
| Nu Sco | Hierarchical multiple-star rendering and spectral inheritance policy |
| HD 213885 | Multi-star plus planet simulation structure |
| eps Ind | Wide A plus brown-dwarf pair hierarchy and scale modes |
| 16 Cyg | Multiple-star plus exoplanet benchmark |

## Verification

The Playwright map suite imports
`srv/web/tests/fixtures/publicExperienceGoldens.mjs` and verifies:

- public goldens resolve through `/api/v1/systems/search` unless explicitly
  marked as a known gap
- representative public goldens expose the Star Search v2 system-page anatomy:
  System Simulation, overview, why-it-matters, concept explainer, and evidence
  disclosure
- technical stress goldens remain reachable for simulator/system-page checks
- `tiledMap.spec.js` verifies exact 100/250-ly artifact accounting, bounded
  mixed LOD points, desktop/mobile nonblank canvas pixels and screenshots,
  camera-detail recentering, radial-seam limits, Exact density, and search
  focus/Peek continuity through exact refinement; a desktop-only 4K check keeps
  Bright mode nonblank and verifies the representative-class label contract
- the map parity subset verifies WebGL recovery, routes, naming modes, system
  detail return, mobile controls, and simulation Peek/Explorer behavior on the
  tiled production path
- Search v2 parity verifies exact accepted TIC/TOI identifiers, explicit
  missing/deferred outcomes without unrelated fuzzy leakage, exact and prefix
  names, bounded typo-tolerant search, filters, stable ordering, and object
  focus through the immutable projection.
- Public Read v2 parity verifies that Search Cards, map sidebars, System Page
  heroes, Peek, Explorer, object badges, hierarchy, and handoff routes consume
  one build-keyed system-summary/hierarchy contract while preserving visible
  source, derived, assumed, missing, ambiguous, and quarantined states.
- Simulation parity covers both ordinary singleton seeds and policy-selected
  full scenes. Singleton systems must render without synchronous ARM/hierarchy
  assembly; multistar, planet-host, compact/exotic, and high-interest systems
  must retain the full scene contract, nested orbits, planets, HZ inputs,
  classifications, and diagnostics.
- Simulation-rate parity verifies physical labels such as simulated days or
  years per real second, retains the multiplier secondarily, exposes manual
  5,000x and 10,000x rates without overflowing desktop/mobile controls, and
  explains that static hierarchy placements without orbit solutions cannot be
  animated by increasing time.
- Public UI coherence verifies that Map Search Results remain mounted, hidden,
  and inert during Peek/Explorer, then restore the exact query, filters, sort,
  pagination, and scroll position. Same-system presentation controls survive
  Peek/Explorer/Detail while elapsed simulation time does not; a new system
  starts at `1x`.
- System Page layout checks cover the 1,600 CSS pixel outer envelope at desktop,
  4K, and ultrawide widths while narrative text remains at most `76ch` wide.
  Catalog and Map peer navigation, clean Catalog brand return, and contextual
  Back to Map behavior remain keyboard and touch accessible in every theme.
- WISE checks cover idle metadata, near-viewport preview loading, hit, miss,
  negative cache, same-key coalescing, bounded retry/rate behavior, client
  abandonment events, eviction, `ETag` validation, `304`, desktop, and mobile.
- Fresh Star Map sessions expose the deterministic device-default tier in
  runtime diagnostics. Explicit radius URLs and saved density/star-style
  preferences take precedence; enhanced desktop, standard desktop, enhanced
  touch, and constrained profiles select their documented bounded
  radius/density/style defaults without rendering a screen-space grid.
- Map-menu parity keeps the stored `realistic` preference compatible while
  presenting that mode as `Natural Color`, exposes the active style and default
  simulation-scale explanations without hover, and gives abbreviated map
  commands complete accessible names. A 360-by-640 regression check keeps the
  expanded settings menu within the viewport with bounded internal scrolling.
- System-page narration parity presents deterministic fallback blocks as
  `Spacegate Summary` rather than exposing machine status tokens, while pinned
  simulator readouts translate storage layers into plain public language.
- Future M8.3g Wavelength View goldens must cover aligned visible/infrared
  fields for high-proper-motion, saturated, crowded, ordinary, ultracool,
  compact, and extended targets; discrete wavelength stops, missing coverage,
  survey epoch, false-color semantics, source attribution, and provider outage
  behavior remain explicit.
- Desktop and constrained-mobile browser runs must prove a nonblank map and
  simulator canvas, stable selection and object focus, usable results scrolling,
  and no incoherent overlap after the projection migration.
- Real-device M8.3e.4 mobile goldens include repeated Android
  Peek/Explorer-to-Detail navigation and dense-field label taps proving that a
  touched visible label outranks unrelated background points.
- Smart Tag parity verifies registry identity, exact source context, bounded
  assignment evidence, any/all/exclude filters, Search/System/Peek/Explorer
  presentation, keyboard and touch pinning, Escape/outside close, copy, Learn,
  Find More, all eight concept routes, and bounded desktop/mobile/4K behavior in
  Simple Dark and Simple Light. Science, evidence, source, presentation, and
  future RIM tokens retain text/shape semantics rather than relying on color.
- Smart Tag application parity verifies a maximum four-tag hero composition,
  one architecture/two exceptional/one planetary family limits, visible claim
  modes, exact member origin, a complete All Tags path, pinned inspector history,
  simulation focus, concept return navigation, keyboard focus return, and mobile
  viewport containment. Hero ranking must never change membership or filters.
- Smart Tag subject-placement parity verifies a visible lower All Tags section
  grouped by actual system, star, planet, or reviewed hierarchy leaf; system
  architecture tags at the hierarchy root; sources separated from taxonomy;
  exact object focus; and no duplicate stellar or planet category beside an
  equivalent glyph. The hero remains bounded and links to this section.
- Simple Light simulation parity uses the shared scene contrast contract across
  Search Cards, Peek, Explorer, and System Page Detail. Canvas checks require a
  light field with dark and chromatic scene pixels, while desktop/mobile
  screenshots retain visible stars, planets, labels, orbit and hierarchy
  guides, HZ bands, and controls without changing the scientific scene.
- Physical-scale simulation parity verifies Alpha Centauri, Castor, Sol,
  TRAPPIST-1, eps Ind, and a missing-axis control. Physical Orbits must use one
  linear AU transform inside the active focus; Structure must say that distance
  is presentation scaled; Log must identify its nonlinear scale and omit a
  conventional ruler; unavailable axes must remain visibly unavailable.
- Desktop double click and explicit keyboard/touch Focus must reach the same
  stable focus node. Fit System, Parent, Back, sibling navigation, breadcrumb,
  selection, and pinned inspection must survive transitions without changing
  scientific membership or presentation settings.
- Wide-system goldens require bounded, collision-managed scale beacons or
  offscreen indicators instead of enlarged inner orbits. A scale lens must use
  one existing WebGL context, remain dismissible and viewport-contained, and
  preserve right-drag camera panning.
- Desktop, 4K, and mobile physical-scale captures must remain nonblank in all
  eight themes. The measured gate compares readiness, frame cadence, selection
  latency, heap, canvas pixels, renderer allocations, and lens cadence against
  the retained pre-change Structure baseline under
  `state/reports/system-simulation-physical-scale-v1/`.

The accepted M8.3e local parity run
`public-read-v2-full-v7-20260725` completed in 225.008 seconds with 62 passing
tests, 48 intentional environment skips, and no unexpected failures or flaky
retries. The skips cover tests whose checked-in contracts intentionally exclude
the current local environment; they are not hidden failures.

The M8.3e.2 full parity run
`smart-tags-v1-full-parity-final-r2-20260727` passed 67 desktop/mobile map,
system, simulation, tile, tag, concept, theme, and accessibility checks with
49 intentional project/environment skips and no unexpected failures. Its
dedicated Smart Tag coverage contributed seven passes and one intentional
mobile skip for the desktop-only 4K case. Machine screenshots, traces, and the
JSON/HTML reports remain under the normal Playwright artifact root.

Stellar-class badges use the same legible icon contract in the Search spectral
bar, Peek, Explorer, System Detail, hierarchy, and simulation object lists.
The focused desktop/mobile run `stellar-badge-unification-r2-20260731`
compares computed ink, font, 24-pixel geometry, and stellar-core rendering
across those shared consumers. The final Smart Tag UI run
`stellar-badge-ui-final-20260731` passes nine applicable checks with one
intentional mobile skip for the desktop-only 4K profile, and the existing
Peek-to-Explorer workflow passes separately.

Smart Tag dialogs render in a shared viewport overlay rather than inside panel
stacking contexts. The `smart-tag-overlay-final-full-20260731` desktop/mobile suite
passes all ten applicable registry, interaction, concept, badge, theme, and
layout checks with two intentional profile skips. The focused
`smart-tag-overlay-visual-r2-20260731` regression waits for the live System
Simulation canvas, proves the hero dialog and canvas overlap, and verifies by
hit testing that the dialog remains topmost; its screenshot is retained with
the Playwright report.

Source tokens use reviewed compact labels while preserving full names in their
dialogs and accessible labels. Stellar and compact-class Smart Tags are not
repeated beside an existing object-badge inventory; their richer explanation,
scope, evidence state, evaluator basis, and bounded source context attach to
the object badge instead. The focused desktop/mobile run
`smart-tag-compact-source-object-badges-20260731` verifies `MSC`/`SB9`, full
catalog-name recovery, duplicate suppression, expanded A-star content, and
unchanged Search-icon geometry with seven passes and one intentional
desktop-only stacking skip on mobile. The complete follow-up
`smart-tag-compact-labels-final-20260731` suite passes all 14 applicable
desktop/mobile checks with two intentional profile skips.

The M8.3e.2b focused run
`smart-tag-subject-placement-final-20260806` passes both desktop and mobile
subject placement checks. The complete Smart Tag run
`smart-tag-subject-placement-full-20260806` passes all 20 applicable checks
with four intentional device/profile skips. Retained desktop/mobile screenshots
show the lower grouped assignment view without clipping or overlap.

Vega is intentionally recorded as a current public-search gap. Fixing it should
be part of the source/alias reconciliation milestone, not a one-off frontend
label patch.
