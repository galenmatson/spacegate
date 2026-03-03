# About Spacegate

Spacegate is a public, browsable database of nearby stars and exoplanets designed for exploration, education, and worldbuilding.

Most online star maps are either visually impressive but scientifically shallow, or scientifically dense but difficult to explore. Spacegate aims to combine the strengths of both: rigorous astronomical data presented through an interface built for curiosity and discovery.

https://coolstars.org is the official public spacegate website available to all for free. Technical project information and aggregated astronomical data is available the project's official page https://spacegates.org

Spacegate is an open source project, you can clone or contribute at https://github.com/galenmatson/spacegate

---

## What Spacegate Is

Spacegate currently provides:

- A versioned database of stars and exoplanets within ~1000 light-years  
- Searchable systems with Gaia, HIP, and HD identifiers  
- Structured spectral classifications and spatial coordinates  
- Exoplanet host matching with documented confidence levels  
- Provenance metadata for every derived record  

The data is real. Nothing is fabricated. All values trace back to public astronomical catalogs.

---

## The Data Backbone

Spacegate currently builds from:

- **AT-HYG** stellar catalog  
- **NASA Exoplanet Archive (pscomppars)**  

Raw source files are preserved in an immutable layer. From there, deterministic scripts:

- Normalize identifiers  
- Parse spectral types  
- Join exoplanets to host stars using a documented priority system  
- Attach full provenance metadata  

Each build produces:

- A versioned DuckDB database  
- Parquet exports for sharing and interoperability  
- Match and quality control reports  

Spatial indexing uses a 63-bit Morton (Z-order) index over heliocentric light-year coordinates to enable efficient 3D queries and future visualization work.

If invariants fail (missing provenance, coordinate inconsistencies, etc.), the build fails.

The database is designed to be reproducible and inspectable.

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