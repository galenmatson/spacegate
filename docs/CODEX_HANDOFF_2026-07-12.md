# Codex Handoff - Spacegate State, July 12 2026

This note is for a future Codex session picking up Spacegate after the long
Star Map, System Simulation, Star Search v2, WISE, and data-quality push. It is
not the canonical architecture spec; use it as orientation, then read the
authoritative docs it references.

Authoritative docs to reread first:

- `docs/PROJECT.md`
- `docs/MILESTONES.md`
- `docs/CHECKLIST.md`
- `docs/3D_MAP.md`
- `docs/SYSTEM_SIMULATION.md`
- `docs/UX_SPEC.md`
- `docs/API_SPEC.md`
- `docs/SCHEMA_CORE.md`
- `docs/SCHEMA_ARM.md`
- `docs/SCHEMA_DISC.md`
- `docs/DATASET_ITERATION_HISTORY.md`
- `docs/TAGS.md`
- `docs/CONCEPTS.md`
- `docs/CATWISE_ALLWISE_PLAN.md`
- `docs/AGENTS.md`
- `docs/AGENT_FRAMEWORK.md`
- `docs/RETENTION.md` before cleanup
- `docs/CANONICAL_INGEST.md` before canonicalization/adjudication work

## Current Strategic State

Spacegate has moved from a functioning prototype into a much more coherent
exploration platform. The public product now has three related surfaces:

1. **3D Map**: immersive nearby-space exploration, fast point-cloud rendering,
   map-native search/filtering, Peek and Explorer drill-in, route/neighborhood
   tools, mobile controls, theme support, and minimal mode.
2. **System Simulation**: a live Three.js/R3F renderer for systems, replacing
   the old static snapshot as the preferred capable-browser visual. It now
   handles planets, hierarchy, multistar barycentric motion, scale modes,
   habitable/formation lines, object inspection, and provenance-aware values.
3. **Star Search v2 / System Page**: a readable catalog/search counterpart for
   laypeople becoming lay astronomers. It is simulation-first, narrative-ready,
   tag-aware, WISE/IR-aware, and much less like a raw catalog dump.

The database is also broader and cleaner:

- Gaia remains the core backbone.
- MSC/WDS/ORB6/SBX/Gaia NSS support has become much more important for
  multiplicity, hierarchy, orbit evidence, and simulation.
- NASA Exoplanet Archive planet solutions were normalized further into ARM.
- WISE/CatWISE/AllWISE is now treated as infrared evidence and image support,
  not as a bulk primary object backbone.
- Alias, display-name, companion rollup, and source-vs-derived boundaries are
  much clearer than they were earlier in the project.

The next hard work is likely:

- larger map radii and streamed/LOD map data,
- deeper source evidence utilization,
- better wide-orbit presentation and mass/orbit priors,
- better tags and concept pages,
- and the AI Astronomy Agency.

## Repository And Runtime Snapshot

At the time this note was written:

- working directory: `/srv/spacegate/app`
- active branch observed during recent work: `star-search-v2-redo`
- local served build ID observed from health check:
  `20260711T_wise_v1_seed_side`
- local containers were healthy after rebuild
- recent relevant commits:
  - `e6ce741 Render compact hierarchy leaves in simulations`
  - `81bce92 Clear map search query with filters`
  - `50f3818 Add system narration foundation`
  - `5debadc Fix simulation readout drag and orbit tree conflicts`
  - `6171fee Improve simulation object class chip contrast`

Always run `git status --short` before editing. The user sometimes edits docs
or UI while Codex is working. Do not revert user changes.

Do not assume production has the same code/data as Photon. The public VPS has
often lagged by a sliced database build or a code deploy. Verify both code and
served build before diagnosing public behavior.

## Product Principles Reaffirmed

The project direction is stable:

- Spacegate is for public astronomy and worldbuilding.
- It should make nearby space browsable, understandable, and compelling.
- Scientific data and fiction/worldbuilding overlays must remain separate.
- Canonical astronomy must be auditable and reproducible.
- Source facts, derived science, presentation assumptions, generated prose, and
  fiction are different things and must not be blurred.
- The target audience includes curious laypeople, science-fiction/game fans,
  and worldbuilders. The aim is to turn them into lay astronomers without
  crushing them under raw catalog data.

Layer rule:

- `core`: accepted inventory and selected hot-path facts.
- `arm`: source-native evidence/support rows, graph/orbit evidence, alternate
  solutions, deterministic science derivatives, classification support.
- `disc`: deterministic presentation artifacts, coolness, generated prose,
  render assumptions, narrative blocks.
- `rim`: fiction/worldbuilding overlays.

Do not use confidence alone as the layer boundary. Use role/purpose.

## 3D Map Work Completed

The map started as a pilot and is now the most important public exploration
surface.

Major delivered pieces:

- React 19 + Three.js/R3F path retained; Babylon.js rejected.
- Desktop flight controls:
  - WASD default, arrow keys always work,
  - user-selectable WASD / ESDF / numpad controls,
  - Q/Z or equivalent up/down,
  - mouse wheel forward/back,
  - tilt-wheel lateral translation,
  - mouse-button truck/pedestal/orbit gestures.
- Mobile flight controls:
  - touch navigation,
  - directional arrows for forward/back/left/right/up/down,
  - touch selection,
  - mobile HUD cleanup.
- Header and menu:
  - burger menu,
  - theme selector,
  - keybind selector,
  - default scale selector,
  - FPS diagnostics toggle,
  - name style selector,
  - star rendering mode selector,
  - minimal mode toggle.
- Minimal mode:
  - hides UI clutter,
  - keeps Peek/Explore and right-click menu usable,
  - `m` and `Esc` restore the interface.
- Search overlay:
  - map-native Star Search sidebar,
  - query bar,
  - dual-handle filters for distance, stars, planets, coolness, temperature,
  - spectral class buttons,
  - habitable-zone planet toggle,
  - sorting,
  - recents/nearby integrated into sidebar,
  - CLEAR now clears both filters and query.
- Map labels:
  - camera-proximity/coolness label strategy,
  - filter-driven label materialization,
  - selected-system highlight changed to an oblique orbit mark rather than a
    large center glyph,
  - spectral class pills added where appropriate.
- Route tools:
  - right-click context menu generalized to Select / Explore / Measure /
    Neighbors,
  - route measurement with persistent legs and total,
  - route segment removal,
  - neighbor panel with distance slider/list/copy controls.
- Peek/Explorer:
  - selecting systems can open a border-light Peek over the map,
  - Explore shifts to a focused simulation view while preserving map context,
  - back/close behavior improved,
  - browser/map return flow improved,
  - map position is preserved through more transitions.
- Star rendering:
  - richer star sprite rendering,
  - spectral color,
  - realism/discovery mode toggle,
  - high-coolness systems can subtly pop without writing emphasis back into
    science data.
- Galactic orientation:
  - galactic north orientation toggle and coreward/rimward/spinward labels were
    added, with caveats that orientation semantics remain presentation-level.

Themes were heavily polished:

- Enterprise/LCARS was rewritten for black panels, bright borders, solid colors,
  less glow, and better map/explorer behavior.
- Mission Control gained Apollo-era inspired buttons and header treatment.
- Cyberpunk became more terminal/neon.
- Geocities became more stereotypically 1990s web.
- Aurora became more vivid and flow-like without causing page-width instability.
- Simple Light and Geocities received less transparency where readability
  suffered.

Known map issues / future work:

- The map is still a 100 ly primary rendered volume. Search can jump to systems
  outside the radius, but the broader universe is not streamed/LOD loaded.
- The next radius expansion must not simply dump 1000 ly into the current
  client payload. Plan chunk/tiling/LOD first.
- Labels are still sparse in regions with few human-readable names. A future
  filter/materialization slider system should let users surface systems by
  distance, coolness, class, planet count, velocity, etc.
- Header/menu z-index and theme clipping issues have been fixed repeatedly; be
  careful when changing header or Explorer stacking.

## System Simulation Work Completed

The old "Live System Preview" was renamed conceptually to **System Simulation**.
It is now the preferred representation for capable browsers.

Major delivered pieces:

- `/api/v1/systems/{system_id}/simulation-scene` contract now emits
  renderer-ready bodies, orbits, fields, diagnostics, assumptions, and a
  `simulation_tree_v1`.
- System Simulation supports:
  - single-star systems,
  - planetary systems,
  - binary systems,
  - hierarchical multiple-star systems,
  - compact objects,
  - broad fallback behavior when orbit evidence is incomplete.
- Scale modes:
  - Structure/Clarity,
  - True Orbits,
  - True Bodies,
  - Log Scale.
- Controls:
  - pause/play,
  - reset,
  - speed selector including high speeds,
  - labels toggle,
  - HZ/temperature-line controls,
  - default scale preference.
- Habitable and formation lines:
  - habitable zone default on,
  - vaporization line,
  - soot line,
  - water snow line,
  - CO2 freeze line,
  - methane/CO freeze line,
  - nitrogen freeze line,
  - tooltips/explanations sourced from `docs/TAGS.md` where practical.
- Planet orbit traces now match eccentricity/inclination/current position
  better than the earlier circular guide lines.
- Planet phases are deterministic rather than all planets starting aligned.
- TRAPPIST-1 was used as a key compact benchmark.
- GJ 1061 inclination logic was corrected so missing inclinations do not create
  arbitrary right-angle systems.
- Scale modes were made more honest:
  - True Bodies uses actual body-size ratios where practical,
  - True Orbits preserves orbit radii,
  - Structure sacrifices scale for readability.
- Habitable-zone orientation is based on the system/orbit plane rather than
  producing obviously perpendicular rings.
- Labels were improved and made less absurd at close zoom; still watch for text
  scale issues.
- Selected object can become camera orbit/pivot target. This matters for wide
  systems such as Alpha Centauri/Proxima.
- Object inspection:
  - hover/focus/tap inspection,
  - click pins readout,
  - readout panel can be dragged and position persists across inspected objects
    within the simulator.
- System Simulation now pauses/throttles when scrolled out of view to save CPU.
- Peek tooltips were simplified so compact previews are not overwhelmed by
  provenance pills.
- System Simulation OBJECTS tree:
  - added to System Page and Explorer,
  - includes hierarchy-like object list,
  - includes stellar class chips,
  - compact styling improved,
  - star chip text contrast fixed.

Important structural simulation fixes:

- Multistar motion moved from flat offsets toward barycentric hierarchy.
- Active simulation tree now avoids partially overlapping barycenter orbits:
  disjoint or properly nested active trees only.
- Conflicting/overlapping source orbits remain in diagnostics rather than
  producing impossible Bohr-like layouts.
- 16 Cyg B was improved by selecting the compatible A-B active orbit and
  skipping conflicting A-C / AC-B overlaps.
- Sirius compact-remnant rendering was fixed generally: hierarchy leaves with
  `component_family: star` or compact leaf kinds such as `white_dwarf` are now
  renderable bodies. Sirius renders Sirius A + Sirius B, and Sirius B is a
  white dwarf.
- Alpha Centauri improved substantially:
  - Proxima is represented as a wide member rather than collapsed into the AB
    pair,
  - Proxima planets orbit Proxima,
  - selected-object camera pivot makes inspecting Proxima possible.

Known simulation issues / future work:

- Castor CC appears as a brown dwarf/candidate brown dwarf under Castor C. The
  verifier currently reports an unmatched MSC `cc` endpoint. Treat this as a
  source endpoint reconciliation issue, not a one-off rendering bug.
- Proxima labels may still appear as `alp1 cen c` in some simulation/render
  paths, despite better public display naming elsewhere.
- Wide companion orbits need a principled "defensible presentation orbit"
  system:
  source-backed group orbits first, derived Kepler presentation orbits from
  projected separation/mass second, deterministic DISC/render assumptions last.
- Very wide systems need better camera/pivot/label behavior.
- Simulator is still a presentation-scale Keplerian visualization, not true
  epoch propagation or N-body dynamics.
- Static snapshots are deprecated in spirit but not fully removed. Do not
  resurrect the old concentric-ring SVG generator as the preferred visual.

## Star Search v2 And System Page Work Completed

The old Star Browser was renamed conceptually to **Star Search**. The current
direction is that the 3D Map is immersive exploration, while Star Search is the
structured catalog/search and readable system-page counterpart.

Major delivered pieces:

- Star Search page uses the map-style sidebar/search controls.
- Search cards:
  - cleaner layout,
  - detail/system page behavior,
  - Map/Explore behavior,
  - tags,
  - stellar class pills,
  - compact IDs,
  - sorting,
  - lightweight preview policy.
- Preview performance:
  - tiered preview policy was implemented:
    - summary-only,
    - lightweight singleton preview,
    - prebuilt/full simulation scene,
    - dynamic fallback.
  - simple lone stars no longer fetch full simulation scenes for every card.
  - search list scrolling CPU spikes were greatly reduced.
  - live WebGL previews are limited and cached/captured after load where
    practical.
  - tooltips were suppressed in embedded search previews because they blocked
    interaction.
- System Page:
  - simulation-first layout,
  - shortened title/hero card,
  - priority tags and stellar pills,
  - catalog ID chips copy full prefix+value rather than only numeric suffix,
  - sections for overview, why it matters, stars/hierarchy, planets/orbits,
    habitability, evidence/technical data,
  - raw fields moved downward/collapsed,
  - System Hierarchy expanded by default,
  - orbital parameter labels made more verbose with tooltips,
  - System Simulation appears earlier.
- System Simulation card cleanup:
  - header text removed/reduced,
  - controls compacted,
  - LINES menu overlays instead of reshaping header.
- Tags:
  - `docs/TAGS.md` is now an important content source.
  - stellar class chips implemented for O/B/A/F/G/K/M/L/T/Y/WR/WD/NS/PULSAR/
    MAGNETAR/BLACK HOLE plus Unknown.
  - tag color taxonomy is emerging but not final.
- Concept-page hooks:
  - tags are intended to become links to concept pages.
  - `docs/CONCEPTS.md` tracks future educational rabbit holes:
    habitable zones, white dwarfs, flare stars, eccentricity, CNO cycle,
    supernovae, compact objects, super-puffs, etc.

Known Star Search/System Page issues / future work:

- Concept pages are not built yet.
- Tag priority needs a real policy: compact contexts should show only important
  tags; large contexts can show more.
- External linkouts are not built yet. IDs should eventually link to CDS/SIMBAD,
  NASA Exoplanet Archive, VizieR, Wikipedia/Wikidata where appropriate, Google
  search, and source catalog pages only when useful.
- Alias discovery and display-name policy still need continued work, especially
  missing Gliese/GJ aliases and common-name preference.
- The system page still needs more polished deterministic/AAA narrative blocks
  and better staging for lay audiences.

## Deterministic Narration Foundation

The System Narration Foundation was started:

- `srv/api/app/narration.py` generates deterministic public narrative blocks.
- System pages can render narrative blocks when present/fallback-generated.
- Blocks include things like:
  - what you are looking at,
  - why it matters,
  - infrared view,
  - what we know,
  - uncertainty,
  - further exploration.
- This is intentionally not full AI Astronomy Agency narration yet.
- Deterministic text belongs in `disc`, not `core` or `arm`.

Policy:

- LLM/AAA output must not replace deterministic public science text unless it is
  reviewed or clearly labeled according to policy.
- Future AAA narration should cite/trace evidence and preserve uncertainty.
- Pop-culture/science-fiction hooks are future RIM/DISC-adjacent optional
  content; do not mix them into canonical science.

## WISE / Infrared Work Completed

The project investigated WISE/CatWISE/AllWISE after noticing missing nearby
ultracool objects such as Luhman 16 and WISE 0855.

Decision:

- Do not bulk ingest WISE-only rows into `core`.
- Treat CatWISE/AllWISE as infrared evidence/cross-reference support in `arm`.
- Use WISE imagery as a system-page visual/evidence feature.
- Missing nearby ultracool objects should be handled through accepted-inventory
  bridge/review queue, not by dumping all WISE sources into the canonical
  inventory.

Delivered pieces:

- `docs/CATWISE_ALLWISE_PLAN.md`.
- WISE/IR evidence policy and image-cache plan.
- System Page "Infrared Sky View" style panel using WISE/IRSA-backed imagery.
- Lazy load and cache behavior with bounded cache policy.
- WISE imagery is framed as observational survey imagery, not artist
  impression.
- WISE/IR evidence is intended to become part of AAA evidence packets.

Known WISE future work:

- Broaden cross-reference coverage for existing Spacegate objects.
- Add/strengthen ARM tables for CatWISE/AllWISE source matches and photometry.
- Candidate review queue for missing ultracool/brown-dwarf objects.
- 4 GB default cache under `/data/spacegate/state/cache/wise_images`; larger
  cache may go under bulk storage if intentionally enabled.
- WISE skyboxes / infrared sky layers are future presentation work.

## Database And Ingest Improvements

Major data-quality themes addressed:

- Multiplicity:
  - Castor, Nu Sco, V1054 Oph, 16 Cyg, Alpha Centauri, eps Ind, Sirius, HD
    213885, etc. became recurring benchmarks.
  - MSC `comp.tsv`, `sys.tsv`, and `orb.tsv` preservation and use were improved.
  - Source-native hierarchy and endpoint labels are preferred over suffix-only
    guessing.
  - Orbit rows should be materialized as ARM evidence when endpoints reconcile
    deterministically.
- V1054 Oph:
  - added as a complex nearby multiple benchmark.
  - source hierarchy/render body reconciliation improved.
  - unmatched source endpoints should remain diagnostics, not fake bodies.
- Alpha/Proxima:
  - accepted system rollup improved so Proxima is a member/focus object, not an
    unrelated root system.
  - Proxima planets attach to Proxima context.
- Alias/display naming:
  - centralized name-style support:
    - Public Full,
    - Astronomer Abbrev,
    - Catalog Compact,
    - Source/Technical.
  - Public Full default prefers Alpha Centauri, Epsilon Indi, Mu Herculis, etc.
  - Matched alias and display title are separate concepts.
  - map/search/detail/sim are more consistent, but not perfect.
- Planet orbital data:
  - NASA `ps` alternate planet orbital solutions investigated/normalized further
    into ARM.
  - Simulation prefers ARM orbit solutions, then legacy core scalar fallback
    only when necessary.
- Stellar physical classification:
  - source spectral class remains source-faithful.
  - ARM-derived physical/display class policy was introduced for safe derived
    classification.
  - visual class priors must not become core source facts.
  - subclass-aware mass priors are planned/partially implemented and should
    replace crude class-only mass assumptions where safe.
- Source evidence utilization:
  - MSC masses/orbit details and other preserved-but-unused fields became a
    major concern.
  - `scripts/audit_source_evidence_utilization.py` exists and should be
    expanded.

Known data issues:

- Castor CC endpoint reconciliation needs a general fix.
- Some Proxima/Alpha labels still surface abbreviated source names.
- Some systems still disagree between simulation OBJECTS and lower System
  Hierarchy. Use this as a diagnostic signal; do not patch per-system UI.
- Some accepted systems still have incomplete or misleading wide companion
  motion due to missing source orbits.
- WISE-only missing nearby ultracool objects need a review/candidate path.
- External aliases and preferred common names are still incomplete.

## Admin And Operations Work

Admin was rebuilt earlier and is now good enough for operational support, but
the main quest recently stayed focused on public UX and science data.

Recent/important Admin notes:

- Presentation controls were moved/promoted because coolness scoring is heavily
  used.
- Coolness jobs now avoid failing just because the operator reused a name.
- Ephemeral scoring was removed/reduced because the use pattern is tune/check/
  tune/check.
- Presentation status chips were added so failures are visible without hunting
  in Operations/Jobs.
- Build diagnostics had issues with `/data` free-space reporting and
  verification status; some fixes were applied.
- Retention dry-runs can free large amounts of data; use `docs/RETENTION.md`
  before applying.
- Public deploy path to antiproton exists but should only be used after local
  checks pass.

Security:

- Keep sensitive security audit specifics outside the public repo, under a
  private host-local path owned by the user/operator.
- Public docs can reference that a private security audit log exists, but must
  not copy host-specific secrets or sensitive findings.
- Future hardening milestone remains important:
  - admin route protection,
  - deploy account/Docker group risks,
  - non-root runtime identity,
  - fail2ban/UFW discipline,
  - secret-scanning before public pushes/deploys.

## Performance Lessons

WebGL:

- Many simultaneous live canvases will crash or lose contexts. Browsers have a
  hard-ish WebGL context budget and will sacrifice older contexts.
- The map canvas is the priority canvas; search result previews must not starve
  it.
- The best current policy:
  - lightweight singleton previews for boring lone stars,
  - full live sim only for complex/interesting cards,
  - capture/cache after load,
  - unload or pause when offscreen,
  - avoid hover tooltips in tiny embedded previews,
  - keep Peek/Explorer responsive.

API/server:

- Full simulation-scene generation for every search result can spike Photon CPU.
- Preview tier policy greatly reduced search-scroll CPU load.
- Prebuilt simulation-scene artifacts are useful but must be invalidated or
  compatibility-checked when membership/render contracts change.

Frontend:

- Large chunks remain. Vite warns about chunks over 500 kB. This is not urgent
  but will matter as map/search/sim grow.
- Dynamic import/code-splitting may become important before 250/500/1000 ly map
  expansion.

Data:

- Public VPS performance depends heavily on sliced DB size.
- Antiproton cannot realistically run the full catalog build quickly; build on
  Photon, slice, publish/deploy.
- Public build/database may lag local. Always verify served build and code.

## Verification Failure Resolution

Resolved during the July 12 stability checkpoint:

- Castor's unmatched MSC endpoint `comp:msc:wds:07346+3153:cc` remains exposed
  as a diagnostic-only warning. The wide-orbit verifier allowlists that exact
  literature-weak endpoint while continuing to fail on unexpected unmatched
  endpoints.
- Proxima's renderer label now preserves the core member display name
  `Proxima Centauri` rather than allowing MSC shorthand `alp1 cen c` to
  override it.
- The broad alias suite also exposed a missing `VB 8` member search term in the
  current side build. Exact member-star fallback now resolves it to V1054 Oph
  with member focus metadata; canonical emitters materialize member-star terms
  for future builds.

## Next Major Quest: Larger Star Map Radius

Do not expand from 100 ly to 250/500/1000 ly by just raising `max_dist_ly` and
shipping a larger monolithic payload.

Recommended architecture:

1. Keep a high-detail live bubble around the camera, probably starting around
   100 ly.
2. Add chunked/streamed map data:
   - spatial tiles/cells,
   - client cache,
   - server cache,
   - abortable requests,
   - priority loading around camera path,
   - LOD tiers.
3. Use compact Tier 0/Tier 1 payloads for most systems:
   - position,
   - display name or technical fallback,
   - spectral/visual class,
   - coolness,
   - star/planet counts,
   - a few bitmasks/tags.
4. Only fetch detail/simulation data when selected, searched, or high-interest.
5. Let high-end clients turn it up:
   - larger loaded radius,
   - more labels,
   - richer point/glow layers,
   - more cached chunks.
6. Keep mobile defaults conservative but not crippled.

Important design constraints:

- The local database can cover much more than the initial map volume.
- Coordinates at 1000 ly should be safe enough in JavaScript/Three if handled
  carefully, but floating precision and camera-relative rendering should be
  considered before pushing much farther.
- Labels, not points, are likely to become the clutter bottleneck first.
- Server request load must be bounded; do not let continuous flight issue
  unbounded tile requests.
- This work is prerequisite for a true "carve tunnels through the 1000 ly
  database" experience.

## Next Major Quest: AI Astronomy Agency

The AAA is now the most important non-UI frontier.

The user wants AAA to:

- study publications,
- assemble evidence portfolios,
- update/propose better data,
- generate public narration,
- triage research targets,
- use frontier models for high-stakes comprehension,
- use Photon local inference for scale/cost control where safe,
- never let untrusted text or model output mutate canonical science directly.

Best path:

1. Harden agent security before broad autonomous ingestion:
   - prompt injection policy,
   - tool allowlists,
   - source quarantine,
   - no direct core mutation,
   - review gates,
   - evals.
2. Build evidence portfolio pipeline:
   - system/object target queues,
   - source retrieval,
   - citation manifests,
   - claim extraction,
   - uncertainty/conflict recording.
3. Add admin-only "Promote for research" and "Needs review" controls.
4. Feed AAA from high-value targets:
   - coolness,
   - data gaps,
   - public goldens,
   - weird systems,
   - missing aliases,
   - WISE/IR evidence conflicts.
5. Let deterministic narration exist first; AAA can replace or augment only
   after review/labeling.

Important AAA boundaries:

- `core` remains deterministic/conservative.
- `arm` can receive proposals, derived evidence, and adjudication candidates.
- `disc` can receive generated narrative/factsheet artifacts.
- `rim` is for fiction/worldbuilding, not unreviewed science.

Photon hardware should be used for local LLM experimentation, but do not assume
local models are good enough for high-stakes scientific adjudication without
evals.

## Suggested Immediate Follow-Up Order

If future Codex is asked "what next?", a pragmatic sequence is:

1. Fix the current verifier failures:
   - Castor CC endpoint reconciliation,
   - Proxima display-name propagation in render/sim paths.
2. Finish the source evidence utilization / stellar parameter normalization
   work:
   - broaden audit report,
   - use preserved MSC/WDS/ORB6/SBX/Gaia NSS fields where deterministic,
   - subclass-aware mass priors,
   - no unsafe core writes.
3. Plan and implement streamed/LOD map radius expansion.
4. Build Concept Tag Foundation:
   - robust tag taxonomy,
   - tag priorities,
   - one-paragraph tooltips,
   - future `/concepts/:slug` pages.
5. Start AAA hardening and evidence portfolio pipeline.

## Style Guidance For Future Codex

The user values:

- structural fixes over one-offs,
- careful layer boundaries,
- public-facing beauty and clarity,
- practical next steps,
- aggressive but thoughtful improvement,
- accurate acknowledgment of uncertainty.

Push back when a requested shortcut would corrupt science semantics or create
long-term architecture debt. But do not use "we need a plan" as a reason to
avoid making obvious safe improvements.

When debugging a weird system:

1. Check search result.
2. Check system detail.
3. Check System Hierarchy.
4. Check `simulation-scene`.
5. Check `render_scene.diagnostics`.
6. Check source/ARM evidence.
7. Decide whether the problem is:
   - source data,
   - cooked source preservation,
   - ARM normalization,
   - accepted membership graph,
   - display-name/alias policy,
   - simulation rendering,
   - UI presentation.
8. Fix the lowest responsible layer. Avoid UI masks for data/modeling bugs.

Spacegate is now good enough that the hard problems are no longer "can we make
something appear?" The hard problems are identity, evidence, scale,
presentation honesty, and making the experience teach without lying.
