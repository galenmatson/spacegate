# About Spacegate

Spacegate is a public, browsable database of nearby stars and exoplanets designed for exploration, education, and worldbuilding.

Most online star maps are either visually impressive but scientifically shallow, or scientifically dense but difficult to explore. Spacegate aims to combine the strengths of both: rigorous astronomical data presented through an interface built for curiosity and discovery.

https://coolstars.org is the official public spacegate website available to all for free. Technical project information and aggregated astronomical data is available the project's official page https://spacegates.org

Spacegate is an open source project, you can clone or contribute at https://github.com/galenmatson/spacegate

---

## What Spacegate Is

Spacegate currently provides:

- A versioned nearby-space database centered on Gaia DR3
- Searchable systems with strong alias coverage (Gaia, HIP, HD, Bayer/Flamsteed, mission and catalog names)
- Multiplicity evidence from Gaia NSS, WDS, MSC, ORB6, and SBX
- Exoplanets from NASA plus lifecycle overlays from additional planet catalogs
- Provenance metadata and retrieval lineage for derived records

The data is real. Nothing is fabricated. All values trace back to public astronomical catalogs.

---

## The Data Backbone

Spacegate is now Gaia-first:

- **Gaia DR3 backbone** for canonical stellar inventory
- **NASA Exoplanet Archive (pscomppars)** for canonical exoplanets
- **Multiplicity overlays** (Gaia NSS, WDS, MSC, ORB6, SBX)
- **Science overlays** in arm (compact objects, variability, cluster context, Sol authority overlays)

Raw source files are preserved in immutable snapshots. From there, deterministic scripts:

- Normalize identifiers and aliases
- Resolve star/system joins with confidence gates
- Build canonical `core` plus supplemental `arm` science overlays
- Attach provenance and retrieval metadata for each transformed record

Each build produces:

- A versioned served build (DuckDB + Parquet exports)
- QC, contribution, and lineage reports
- Deterministic artifacts suitable for reproducible deployment

Spatial indexing uses a 63-bit Morton (Z-order) index over heliocentric light-year coordinates to enable efficient 3D queries and future visualization work.

If invariants fail (missing provenance, coordinate inconsistencies, etc.), the build fails.

The database is reproducible, inspectable, and designed for long-lived data stewardship.

---

## Curation Principles

- No invented data  
- No silent inference  
- Core astronomy remains immutable  
- Derived enrichments are layered separately  
- Every derived artifact is traceable to source facts  

Future enrichment (descriptions, snapshots, visualizations) will always remain grounded in documented data.

---

## Why “Spacegate”?

The name comes from a fictional faster-than-light network concept developed for my own hard science fiction setting. The idea of gate or portal travel in scifi goes back to the 1800s but was popularized in stories by Arthur C. Clark and Robert Heinlein before become widespread in television as Star Gates in Stargate, and Ring Gates in the Expanse. 

---

## The Creator

Spacegate was created by Galen Matson, an engineer with a long-standing interest in astrophysics, large-scale systems, and science fiction worldbuilding.

The goal is to build a tool that is scientifically rigorous, openly reproducible, and genuinely enjoyable to explore.

For more of my stuff you can check out galenmatson.com

You can buy my nerd merch at engineeritees.printify.me

I develop and host this project out of pocket. If you think it is worthwhile please consider sponsoring my work at: https://github.com/sponsors/galenmatson

---

## Contact

Questions, suggestions, or collaboration ideas are welcome.

**ahoy@spacegates.org**
