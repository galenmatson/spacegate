# Spacegate Public UX Spec

Status: current public direction. This document supersedes the legacy
pre-map Star Browser contract. The public experience is now split into two
complementary surfaces:

- `/map`: immersive 3D exploration of nearby systems.
- `/search`: Star Search v2, the readable catalog/search counterpart for
  visitors arriving by name, catalog ID, media reference, or curiosity.

The system detail route is simulation-first. Static deterministic snapshots are
fallback/reference artifacts, not the preferred capable-browser experience.

## UX Principles

- Help laypeople become lay astronomers without hiding evidence.
- Stage complexity: overview first, raw catalog detail later.
- Never present assumptions as source facts.
- Keep source, ARM, DISC, render presentation, and RIM/lore roles visible when
  they affect interpretation.
- Prefer System Simulation for visual understanding, with cached captures or
  deterministic snapshots only where live WebGL is inappropriate.
- Keep map exploration and catalog search connected but not fused into one
  overloaded page.

## Global UI

- Public naming is **Star Search**, not Star Browser.
- Public renderer naming is **System Simulation**. Internal contracts may keep
  schema/version identifiers such as `simulation_scene_v0`,
  `render_scene_v0.2`, and `visual_scale_beta_v1`.
- Theme labels currently exposed in UI:
  `Simple Light`, `Simple Dark`, `Cyberpunk`, `Enterprise`, `Mission Control`,
  `Aurora`, `Geocities`, and `Deep Space Minimal`.
- Name Style is a user preference, not a source-identity choice. The default
  is `Public Full`, which favors layperson-readable full names such as
  `Alpha Centauri`, `Epsilon Indi`, and `Mu Herculis`. Optional styles are
  `Astronomer Abbrev` (`Eps Ind`, `Mu Her`), `Catalog Compact`, and
  `Source/Technical` for WDS/Gaia/HIP/HD-style inspection.
- Matched aliases should remain visible when useful, but a matched abbreviation
  or catalog ID must not force the public title to become that raw alias.
- `/` focuses route-level search where available, unless the user is already
  typing in an editable field.
- Catalog IDs should be copyable, but not visually dominant.

## Page: 3D Map

The map is the immersive explorer. It should feel like flying through the local
stellar neighborhood, not operating a form-heavy database.

Primary behavior:

- WASD/arrow/touch navigation for free flight.
- Map-native search and filters can materialize labels without leaving the map.
- Selecting a system opens a lightweight System Simulation Peek.
- Explore opens the focused map drill-in state.
- Following through to `/systems/:id?from=map&map_return=...` must preserve
  return context when practical.

Map overlays:

- Recents and Cool Stars Nearby live in the Search sidebar as compact
  discovery aids.
- Search/filter UI can be hidden to free the viewport, and minimal mode can
  hide passive chrome while keeping requested Peek/Explore and context menus
  available.
- Labels should adapt to camera position and active filters.
- Orientation markers and galactic-direction labels are presentation overlays;
  they do not change science-layer coordinates.
- Star Style is a presentation preference. `Discovery` subtly emphasizes
  high-coolness, planet-hosting, multistar, and nearby systems to encourage
  exploration. `Realistic` reduces that guidance and uses physically motivated,
  lightly tinted stellar colors. `Bright` increases core and halo visibility
  for large/high-resolution displays. None of these modes changes science-layer
  facts.
- Representative stellar-class badges are an independently persisted map-menu
  preference. Disabling them retains the same bounded label set and star style.

## Page: Star Search v2

Star Search is the structured catalog/search experience. It should be more
article-like and readable than the map, but still visually connected to
Coolstars.

### Search Input

- Placeholder direction: search by system name, alias, catalog ID, or stable
  key.
- Query text is passed raw to the API; the API normalizes and ranks matches.
- `sort=match` is the default for named queries.
- Star Search v2 uses the same compact search strip visual language as the
  map-native search overlay so users do not have to relearn the controls when
  moving between catalog search and free-flight exploration.

### Filters

Preserve or expose filters where data supports them:

- distance
- spectral class
- temperature
- star count
- planet count
- coolness
- habitable-zone candidates
- compact objects or notable classes

Filters should be discoverable without making the page feel like an expert
catalog front-end.
The standalone Star Search page should keep these filters in a tight sidebar
that visually matches the map search sidebar where practical.

### Sorting

Useful sort axes:

- relevance / match
- coolness
- distance
- name
- planet count
- star count
- hottest / coolest stellar temperature where meaningful

### Result Cards

Each result card should be compact, readable, and simulation-aware:

- display name and best aliases
- distance
- spectral summary
- star count
- planet count
- coolness score
- habitability indicators
- notable tags
- copyable IDs where useful

Notable tags are presentation/discovery cues derived from existing search
payload fields, such as proximity, planet count, stellar multiplicity,
habitable-zone screening signals, ultracool/white-dwarf spectral classes,
coolness score, and source evidence catalogs. They must not create new
science-layer facts.

Current public discovery tag vocabulary:

- `Nearby`: within 25 ly
- `Local neighborhood`: within 100 ly
- `Exoplanet`: one confirmed linked planet
- `Multi-planet`: two or more confirmed linked planets
- `HZ planet`: broad habitable-zone planet screening signal, not a
  habitability claim
- `Multistar`: multiple stellar members grouped in the system record
- `White dwarf`: compact-remnant spectral summary includes `D`
- `Ultracool`: spectral summary includes `L`, `T`, or `Y`
- `High coolness`: strong score on the active Coolstars discovery profile
- source/evidence tags such as `NASA`, `Gaia`, `MSC`, `WDS`, `ORB6`, `SBX`,
  `VSX`, and similar catalog labels where the search/detail payload exposes
  that evidence

Search result cards should show only the highest-priority subset of tags.
System pages may show the fuller public tag set because they have more room.

Broader planned taxonomy tag families remain future work: planet size/mass,
insolation/temperature, orbit class, composition proxy, detection method,
host context, lifecycle/confidence, stellar element-richness proxy, variability,
compact-object subtype, debris-disk/context, and other deterministic tags
derived from source/ARM/DISC presentation evidence.

Future concept pages should make these tags clickable teaching paths at
`/concepts/:slug`. A tag such as `White dwarf` should open a readable concept
page with a simple explanation first, then deeper science, representative
systems, images or animations where useful, links to adjacent concepts, and a
"Find more" link back into Star Search with the relevant filter active. The
concept backlog and page contract live in `docs/CONCEPTS.md`.

Visual policy:

- Result cards use bounded System Simulation previews.
- The normal card state should reuse a cached first-frame capture when
  available.
- Live preview is promoted on hover/focus only when the WebGL budget allows.
- Avoid many simultaneous live WebGL contexts.
- Deterministic snapshots remain fallback/reference metadata.

## Page: Simulation-First System Detail

The system detail page should feel like a staged explanation, not a raw catalog
dump.

### Anatomy

1. Hero: public display name, best aliases, copyable IDs, stellar-class pills,
   and priority discovery tags in a compact first card.
2. System Simulation: primary visual anchor.
3. What You’re Looking At: short layperson-facing summary from current facts.
4. Why This System Matters: discovery hooks such as planets, multiplicity, proximity,
   coolness, or evidence diversity.
5. Infrared View: WISE/AllWISE observational imagery explanation when image
   products or infrared evidence are available.
6. What We Know: inventory and source-backed summary without raw table noise.
7. What Remains Uncertain: missing fields, derived support, and simulation
   assumptions explained plainly.
8. Further Exploration: concept hooks and guided next steps.
9. Reading This System: concept explainer for spectral class, habitable zone,
   orbital period, eccentricity, hierarchy, and uncertainty.
10. Stars and Hierarchy: nested structure from ARM hierarchy/orbit
   relationships with stellar-class pills, compact vitals, readable orbital
   facts, and hover explanations.
11. Stars and Catalog Rows: collapsed or secondary raw star facts.
12. Planets and Orbits: collapsed or secondary raw planet facts.
13. Evidence and Technical Data: source chain, grouping, coordinates,
    snapshots, and diagnostic metadata.

Technical coordinates, raw catalog rows, and low-level provenance should not
compete with the first-screen simulation and overview. Keep those details
available, but staged behind disclosures or secondary panels.

### Simulation Controls

Expose useful controls without overwhelming the page:

- scale mode
- speed
- labels
- habitable zone
- temperature/freeze lines through a disclosure
- pause/reset

The simulator should stop advancing time and throttle rendering when its panel
is clearly scrolled out of view, then resume the user's chosen running/paused
state when it returns. This keeps the simulation-first page efficient while a
reader explores narrative, hierarchy, and evidence sections.

### Narrative Blocks

The public page consumes `narrative_blocks` from the system-detail API. These
blocks are DISC-scoped presentation artifacts. Today the API provides
deterministic fallback prose from existing facts when reviewed DISC rows are
absent. Future AAA-written blocks may replace them only after review/publication
state is explicit.

The simulation must distinguish source values, derived values, assumptions, and
presentation-only render choices.

### Narrative Slots

Reserve page slots for future AI Astronomy Agency content:

- short public summary
- why it matters
- what we know
- what remains uncertain
- worlds and orbits
- further reading / evidence

Reviewed/generated DISC narration may fill these slots later. RIM or
pop-culture hooks are future optional overlays and must not be mixed into
canonical science.

## Display Names and Aliases

Public display names should prefer recognizable, stable names while preserving
source names and catalog identifiers as aliases.

Rules:

- Exact user query matches can drive display name in search results only when
  the matched term is public-facing. Catalog IDs, Gaia/WDS/HIP/HD labels,
  raw Gl/GJ identifiers, and abbreviated Bayer forms should remain matched
  aliases/context, not necessarily public titles.
- Detail pages have no query context, so they use stable display policy.
- Proper names can outrank abbreviated catalog/system names.
- Abbreviated Bayer names such as `alp1 Cen` may promote to full expanded names
  such as `Alpha Centauri`.
- Canonical Flamsteed names such as `55 Cnc` should not be displaced by a Bayer
  expansion unless another stronger public name exists.
- Gaia/WDS/HIP/HD identifiers should remain copyable but secondary.
- A query for a member alias may open the owning accepted physical system while
  preserving member context. Example: `Proxima Centauri` should resolve into
  the accepted Alpha Centauri system, with Proxima still visible as the member
  planet host for Proxima b/d.
- Search result payloads may expose `matched_alias`, `matched_target_type`,
  `matched_target_id`, and `focus_object_key` so UI can explain context such as
  “Proxima Centauri in Alpha Centauri” without flattening source identity.
- Dense exact-like queries should be conservative. If `V1513 Cyg` has no exact
  authority hit, it is better to show no result than a fuzzy result for
  `V1581 Cyg`.

Known current public source/alias coverage gap:

- Vega / Alpha Lyrae / HD 172167 / HIP 91262 is absent from the current served
  core/source alias coverage and is tracked in `docs/PUBLIC_UX_GOLDENS.md`.

## Provenance Display

Top-level public summaries should not be cluttered with provenance pills unless
uncertainty is central to interpretation. Detailed evidence belongs in
collapsible evidence/technical sections.

Use these statuses consistently where practical:

- `SOURCE`: catalog/source value.
- `DERIVED`: deterministic ARM/presentation derivation from source data.
- `ASSUMED`: DISC or render assumption.
- `MISSING`: absent value.

Never write visual assumptions into core or ARM.

## Public UX Goldens

Public-experience goldens are defined in `docs/PUBLIC_UX_GOLDENS.md` and used
by Playwright fixtures in
`srv/web/tests/fixtures/publicExperienceGoldens.mjs`.

They are distinct from ingestion/multiplicity goldens. Public goldens judge
search relevance, layout, narrative staging, simulation quality, and data
clarity.

## Accessibility and Performance

- Keep search usable by keyboard.
- Keep mobile layouts readable and tappable.
- Do not load all systems or all live previews at once.
- Bound active WebGL previews and recover gracefully from context loss.
- Preserve map return context from system pages when opened from the map.
- Use cached scene artifacts and preview captures to reduce repeated API and
  client render work.
- Search result previews are tiered. Ordinary singleton systems use a
  lightweight HTML/CSS preview from existing result fields; planet hosts,
  multistars, compact/exotic systems, high-coolness systems, and public goldens
  remain eligible for full System Simulation previews. This keeps blank-search
  scrolling cheap without reducing Peek, Explore, or system-page fidelity.
- This preview-tier contract is a prerequisite for larger streamed map radii:
  a 250/500/1000 ly explorer cannot afford per-card dynamic scene assembly for
  the ordinary red/brown-dwarf long tail.
