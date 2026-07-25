# Spacegate Data

Last updated: July 25, 2026 (UTC)

Spacegate is not a single star catalog and it is not a loose merge of downloaded
tables. It is a reproducible scientific data compiler for nearby space.

The project collects authoritative astronomy catalogs, preserves their original
records, reconciles their identities and scopes, retains competing evidence, and
then compiles a versioned public view of stars, planets, systems, relationships,
and selected physical properties. The result is a dataset built for exploration
without giving up the provenance needed for serious inspection.

Within its nearby-space scope, Spacegate is deliberately comprehensive. It
combines the Gaia stellar backbone with major sources for names, distances,
stellar physics, spectroscopy, multiplicity, orbital solutions, exoplanets,
variability, clusters, compact objects, ultracool dwarfs, infrared detections,
extended objects, and the Solar System. Just as importantly, it records what
could not be matched or selected instead of quietly discarding inconvenient
rows.

## The Dataset At A Glance

The accepted July 2026 Evidence Lake generation includes:

| Measure | Current scale |
| --- | ---: |
| Registered source and release contracts | 49 |
| Pinned source-product manifests | 150 |
| Machine-accounted upstream fields | 6,273 |
| Acquired source rows covered by the foundational acquisition report | More than 170 million |
| Accepted selected scientific facts | More than 123 million |
| Canonical star systems | 5,869,091 |
| Canonical stars | 5,874,636 |
| Public planet records | 6,311 |
| Searchable aliases | 1,026,480 |
| Object identifiers | 6,669,279 |
| Deduplicated system search terms | 12,768,410 |
| Explicit identity quarantines | 81,043 |
| Accepted full public-generation artifact | About 22.5 GiB |

The `Current Served Database` card above is authoritative for the build this
website is using. The figures here describe the accepted July 2026 Evidence Lake
generation and compiler line; a public server may retain an earlier verified
build until the newer generation passes its runtime and deployment gates.

Those numbers describe different levels of the compilation. The 123 million
selected facts are not 123 million objects. They are evidence-backed quantities
and classifications attached to objects, parameter sets, methods, references,
and derivations. The public runtime is smaller because it contains the views
needed to browse and explain Spacegate, not duplicate copies of every source
table.

The complete accepted evidence chain is much larger than the public download.
Pinned raw inputs, source-native typed tables, accepted evidence, selected facts,
and the public generation occupy about 608 GiB before temporary build space,
rollback generations, reports, and historical retention. A constrained clean
source build needs at least 1.25 TB of usable storage; 2 TB is the recommended
minimum for a practical full builder.

## What Spacegate Collects

Spacegate assigns authority by scientific question. There is no universal
"best catalog." Gaia may be the strongest source for astrometry, a dedicated
binary catalog may be better for an orbit, and a spectroscopic survey may be the
appropriate authority for chemical abundance.

Major source families include:

- **Stellar inventory and astrometry:** Gaia DR3, its astrophysical-parameter,
  supplementary, variability, non-single-star, and official crossmatch products,
  plus release-aware Gaia DR2/EDR3/DR3 neighborhood evidence.
- **Distances:** Gaia source astrometry and Bailer-Jones geometric and
  photogeometric distance estimates, preserved as distinct kinds of evidence.
- **Names and identifiers:** SIMBAD, the IAU Working Group on Star Names,
  AT-HYG crosswalk evidence, catalog aliases, and release-scoped identifier
  relationships.
- **Stellar physics and spectroscopy:** Gaia AP and FLAME products, APOGEE DR17,
  GALAH DR4, and LAMOST DR11, including source quality flags and coherent
  parameter sets.
- **Multiple-star systems and orbits:** Gaia NSS, WDS, MSC, ORB6, SB9, SBX,
  DEBCat, the El-Badry wide-binary catalog, and TESS eclipsing-binary evidence.
- **Exoplanets and candidates:** NASA Exoplanet Archive reference-specific and
  composite tables, TESS Objects of Interest and targeted TIC identity evidence,
  the Open Exoplanet Catalogue, Exoplanet.eu, and the Habitable Worlds Catalog.
- **Variability and stellar activity:** Gaia variability products, AAVSO VSX,
  GCVS, rotation and activity evidence where available.
- **Compact and low-temperature objects:** ATNF pulsars, the McGill magnetar
  catalog, Gaia white-dwarf candidates, UltracoolSheet, targeted CatWISE2020 and
  AllWISE evidence, and published ultracool samples.
- **Clusters and extended objects:** Cantat-Gaudin and Hunt/Reffert cluster
  evidence, OpenNGC and nebula catalogs, and the Green supernova-remnant catalog.
- **The Solar System:** JPL Horizons, IAU constants, and Spacegate's permanent
  identities for the Sun, planets, moons, selected minor bodies, and selected
  artificial objects.

Spacegate does not bulk-mirror all of Gaia, all of TIC, or every available
spectrum, light curve, and survey image. That would be wasteful and would not
improve the nearby-space model by itself. Large observation products use
metadata-first indexes and bounded, checksum-addressed retrieval when a visitor,
research task, or Astronomy Agency investigation actually needs them.

## How A Catalog Becomes Spacegate Data

The compiler has a sequence of explicit stages. Each stage produces immutable
artifacts and machine-readable verification reports.

### 1. Register The Source

Before acquisition, a source release receives a machine-readable contract:

- source and release identity
- scientific authority roles
- exact URL, TAP query, or retrieval procedure
- schema and expected products
- citation and license
- identifier namespace, coordinate frame, epoch, and units
- null, uncertainty, bound, and limit meanings
- a disposition for every upstream field

New fields, missing products, and schema changes fail the gate until reviewed.
This prevents a catalog update from silently changing the scientific meaning of
a build.

### 2. Preserve The Original

Spacegate stores byte-identical source files or exact API/TAP responses with
queries, timestamps, checksums, schemas, counts, and retrieval metadata. Raw
snapshots are append-only. A later release never rewrites an earlier one.

This is the first reproducibility guarantee: the evidence used by a build can be
identified and checked independently.

### 3. Create Source-Native Typed Tables

Each release is converted independently into typed Parquet tables. Values are
not yet merged and no winner is selected.

The typed layer preserves source record identity, parameter-set grouping,
component scope, methods, models, references, epochs, frames, flags,
uncertainties, lower and upper bounds, limits, original units, and normalized
units. A source column is preserved, normalized, used only for indexing, or
deliberately omitted with a recorded reason.

### 4. Reconcile Identity And Scope

Catalog numbers are not treated as universal object identities. `Gaia DR2`,
`Gaia EDR3`, and `Gaia DR3` identifiers belong to different release namespaces.
Spacegate connects them only through official or reviewed crossmatches.

The identity compiler separately asks:

- Is this the same physical object?
- Is this row about a whole system, a star, a component, a planet, or an
  observation target?
- Does this relation describe containment, a catalog grouping, or merely a
  measurement pairing?
- Is the match accepted, missing, excluded, ambiguous, quarantined, or still
  unresolved?

This separation is critical for multiple systems. A measurement of a blended
binary cannot safely be copied onto both component stars, and a catalog relation
does not automatically become canonical hierarchy.

### 5. Compile Typed Scientific Evidence

Accepted source-native rows enter domain-specific evidence tables for astrometry,
distances, photometry, extinction, stellar parameters, classifications,
spectroscopy, variability, activity, rotation, relationships, orbits, clusters,
planets, transits, radial velocity, compact objects, extended objects, and
observation-product lineage.

Competing measurements remain available. Negative evidence, false positives,
retractions, upper limits, alternative atmosphere fits, and unresolved
relationships are retained rather than flattened into a misleading single row.

### 6. Select Facts And Derivations

Spacegate then applies versioned authority and applicability policies for each
scientific quantity. It does not rank entire catalogs from best to worst.

The general order is:

1. an accepted direct or dynamical measurement appropriate to the quantity
2. an accepted calibrated source estimate or model
3. a defensible physical derivation from compatible inputs
4. an empirical relation inside its documented applicability range
5. a clearly labeled presentation prior

Mass, temperature, luminosity, distance, spectral classification, orbit, and
cluster membership each have their own policy. Coherent source parameter sets
are preferred over scientifically incompatible field-by-field mixtures.

Every selected value points to its evidence. Every derived value records its
inputs, formula or algorithm version, assumptions, applicability, uncertainty
method, confidence, and supersession state. Verification fails if a lower-quality
fallback wins while acceptable higher-authority evidence exists.

### 7. Build The Spacegate Layers

The selected compilation is projected into layers with different responsibilities:

| Layer | Responsibility |
| --- | --- |
| `core` | Accepted canonical inventory, stable object relationships, and compact public facts. |
| `arm` | Source-native evidence, competing solutions, hierarchy and orbit support, and reproducible scientific derivations. |
| `disc` | Deterministic presentation products such as coolness scores, render assumptions, scene artifacts, tags, and future grounded narration. |
| `rim` | Fiction, lore, and worldbuilding overlays. |

These boundaries are about purpose, not simply confidence. A published orbital
solution is real scientific evidence, but it belongs in a relationship/orbit
model rather than being confused with the permanent identity of a star.

Fiction never becomes astronomy because it is popular, and AI-generated text
never becomes canonical science because it sounds convincing.

### 8. Produce Public Products

The public generation contains the databases and compact artifacts needed for
search, maps, system pages, simulations, and API responses. Map tiles, search
terms, hierarchy projections, scene artifacts, and presentation scores are tied
to the same build identity.

The accepted full public-generation artifact contains all 5.87 million compiled
systems with no radius trimming. Its 24.2 billion logical bytes compress to a
verified archive of about 15.9 GiB. The public package is a projection of the
evidence lake, not a replacement for it.

### 9. Verify, Compare, And Promote

A candidate build must pass row accounting, schema, identity, collision,
component-scope, hierarchy, planet-lifecycle, selected-fact lineage, deterministic
reproduction, API, search, map, simulation, and browser checks.

Spacegate compares a new candidate against the previous stable build and explains
scientific changes through evidence and reusable policy. Named systems such as
Sirius, Castor, Alpha Centauri, and Nu Scorpii are regression tests, not excuses
for one-off production code.

Promotion is atomic. The previous verified generation is retained for rollback.
The build identifier shown by the site connects what you see to a particular set
of source releases, compiler policies, products, and verification reports.

## Reproducibility Is A Product Feature

Reproducibility in Spacegate means more than keeping a citation in a text field.

- Raw inputs are immutable and checksum-addressed.
- Exact retrieval requests and release identities are recorded.
- Typed source tables can be rebuilt from raw snapshots.
- Ordered Parquet artifacts provide byte-for-byte scientific reproduction.
- Identity, selection, and derivation policies are versioned.
- Every target binding and selected fact is accounted for.
- Ambiguity is quarantined instead of resolved by guesswork.
- Candidate builds are compared scientifically before promotion.
- Public artifacts carry a build identity and retain a tested rollback.

DuckDB is used as a powerful compiler and query engine, but its database-file
layout is not treated as the only durable scientific representation. The
canonical reproduction interfaces are the pinned inputs, manifests, schemas,
ordered data products, logical hashes, and independent verification reports.

## What "Comprehensive" Means Here

Spacegate is not a complete copy of every astronomical archive, and it does not
claim certainty where astronomy has not earned it. Its scope is the construction
of a rich, searchable, physically organized model of nearby space.

Within that scope, the project is unusually broad:

- millions of stellar systems share one identity and hierarchy model
- major astrometric, physical, spectroscopic, multiplicity, planetary, and
  classification sources are compiled together without erasing their differences
- more than 123 million selected facts retain evidence or derivation lineage
- uncertain identities and relationships remain visible to the build system
- canonical science, derived science, presentation, AI output, and fiction are
  kept structurally separate

That is what makes Spacegate more than a beautiful star map. A visitor can fly
through millions of systems, search by familiar or obscure identifiers, inspect
complex stellar hierarchies, and watch planets and stars move in simulations
whose inputs can be traced back through the compiler. The visual experience is
the surface of a serious scientific data system.

## Open Source And Inspectable

The Spacegate source repository contains the collectors, registries, compilers,
schemas, policies, verification tools, API, and public interface:

https://github.com/galenmatson/spacegate

The public site identifies its served build so the visible experience can be
connected to the exact data generation behind it. Future evidence-inspection
tools will make competing measurements and selection decisions progressively
easier to examine directly from system and object pages.
