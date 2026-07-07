# About Spacegate

Spacegate is an open source astronomy and worldbuilding platform for browsing nearby space. Its public site, Coolstars, is built to make real stars, planets, and stellar systems easier to explore without burying visitors under catalog tables.

The project has two goals:

- Keep canonical astronomy auditable, reproducible, and separated from speculation.
- Present nearby space in a way that is understandable, beautiful, and compelling.

Coolstars is the public exploration site. Spacegate is the underlying database, build system, API, and software project.

- Public exploration site: https://coolstars.org
- Project and technical site: https://spacegates.org
- Source code: https://github.com/galenmatson/spacegate

## What You Can Do Here

Coolstars currently includes:

- A 3D local star map centered on nearby space.
- Map-native Star Search with filters for distance, spectral class, temperature, planet count, star count, coolness, and habitable-zone planet candidates.
- System Simulation for stars, planets, orbits, habitable zones, and temperature threshold lines.
- Search results with live or cached simulation previews.
- System pages with hierarchy, object vitals, source-aware simulation data, and technical details.
- Data-source documentation and build identifiers for reproducibility.

The 3D map is the main exploration interface. Star Search v2 is the structured catalog counterpart: readable result cards, simulation previews, and simulation-first system pages for visitors who arrive by name, catalog ID, or curiosity.

## Data Philosophy

Spacegate does not treat all information as equally certain.

- Source facts are preserved with catalog provenance.
- Deterministic derived data is kept separate from canonical inventory.
- Visual assumptions are labeled as assumptions.
- Fictional and worldbuilding material belongs in separate overlays.

That separation is central to the project. A star catalog, a simulation renderer, an AI-written explanation, and a fictional political map of space may all refer to the same star system, but they should not be confused with one another.

## Project Layers

Spacegate uses layered data so scientific records, derived products, generated content, and fiction can evolve without contaminating each other.

- `core`: accepted public inventory and hot-path browse/search summaries.
- `arm`: source-native and defensible science relationships, orbits, hierarchy, and other analytical overlays.
- `disc`: reproducible generated outputs such as scores, presentation artifacts, assumptions, and future AI narration.
- `rim`: fictional, lore, and worldbuilding overlays.

The boundaries are based on role and purpose, not simply confidence. For example, orbital solutions are scientific evidence, but they belong in a relationship/orbit layer rather than being treated as simple immutable object identity.

## The Current Build

The public site serves a sliced nearby-space build optimized for the current VPS. It focuses on nearby systems and trims some distant low-mass stars to keep the public service responsive.

Each served build has a build identifier visible in the interface. That identifier matters: it ties the website back to a specific materialized database build, source manifest, and verification run.

## Why Spacegate?

The name comes from a fictional faster-than-light network concept developed for a hard science fiction setting. The long-term vision includes real astronomy, public education, and optional worldbuilding overlays where users can draw routes, borders, stations, spacegates, and other speculative artifacts without mixing them into the science database.

## The AI Astronomy Agency

Spacegate is also building the AI Astronomy Agency, or AAA. The AAA is planned as a research and narration framework that can study astronomical publications, help maintain evidence portfolios, and eventually write clear public explanations grounded in source material.

The AAA is not allowed to silently promote unreviewed model output into canonical science. Its job is to assist research, triage, explanation, and review while preserving provenance.

## Creator

Spacegate was created by Galen Matson, an engineer interested in astrophysics, large-scale systems, and science fiction worldbuilding.

The project is developed and hosted out of pocket. Sponsorship helps keep Coolstars public and free:

https://github.com/sponsors/galenmatson

## Contact

Questions, suggestions, corrections, and collaboration ideas are welcome.

**ahoy@spacegates.org**
