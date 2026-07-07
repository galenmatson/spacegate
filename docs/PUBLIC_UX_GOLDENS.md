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
| Alpha Centauri | Nearest famous multi-star system; map-to-system benchmark | Resolves; Alpha/Proxima planet-host rollup remains future data work |
| Proxima Centauri | Nearest known exoplanet host; Alpha/Proxima relationship watch item | Resolves separately from Alpha Centauri |
| Sirius | Bright public-recognition benchmark with compact companion | Resolves |
| 55 Cancri | Multi-planet benchmark for search, cards, and simulation ordering | Resolves |
| Epsilon Eridani | Nearby K-star exoplanet system | Resolves |
| Barnard's Star | Famous high-proper-motion nearby system | Resolves |
| Wolf 359 | Nearby red dwarf and public-recognition benchmark | Resolves |
| Vega | Bright public-recognition benchmark | Known current gap: `Vega`, `Alpha Lyrae`, `HD 172167`, and `HIP 91262` do not resolve correctly on the local served build |
| Fomalhaut | Bright debris-disk/public-recognition benchmark | Resolves |

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

Vega is intentionally recorded as a current public-search gap. Fixing it should
be part of the alias/display-name data milestone, not a one-off frontend label
patch.
