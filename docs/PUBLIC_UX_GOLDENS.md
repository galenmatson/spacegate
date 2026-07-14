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
  mixed LOD points, desktop/mobile nonblank canvas pixels and screenshots, and
  search focus/Peek continuity through exact refinement
- the map parity subset verifies WebGL recovery, routes, naming modes, system
  detail return, mobile controls, and simulation Peek/Explorer behavior on the
  tiled production path

Vega is intentionally recorded as a current public-search gap. Fixing it should
be part of the source/alias reconciliation milestone, not a one-off frontend
label patch.
