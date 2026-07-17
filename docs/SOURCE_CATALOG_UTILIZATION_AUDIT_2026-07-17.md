# Source Catalog Utilization Audit - 2026-07-17

## Executive Decision

Do not run another full canonical rebuild merely to promote NASA best-mass
values. The current ingredients expose several independent source-to-evidence
gaps. Implement them as one bounded **Catalog Evidence Utilization v2** bundle,
then perform one full canonical rebuild, one public slice, and one verification
cycle.

This audit found no reason to change Spacegate's layer architecture. The main
defect is that acquisition and source-shaped cooking have outgrown the shared
ARM evidence projection:

1. some valuable fields are available upstream but are not acquired;
2. some fields are cooked but remain trapped in source-specific tables;
3. some source rows are normalized only when a downstream graph edge already
   exists, without an exhaustive reconciliation partition;
4. one Gaia DR2 membership source is joined directly to Gaia DR3 identifiers;
5. several roadmap features depend on evidence that the source catalogs already
   contain.

The machine-readable local measurement is:

`/data/spacegate/state/reports/source_catalog_utilization_report_20260717.json`

Regenerate it with:

```bash
.venv/bin/python scripts/audit_catalog_feature_utilization.py \
  --build-dir /data/spacegate/state/served/current \
  --state-dir /data/spacegate/state \
  --json-output /data/spacegate/state/reports/source_catalog_utilization_report_20260717.json
```

## Scope and Method

The audit compared:

- the 33 existing cooked catalog products in the July 16 catalog pipeline;
- the served July 17 public-side CORE and ARM databases;
- raw/cooked headers and non-null field counts;
- source manifests and build reports;
- `PROJECT.md`, `MILESTONES.md`, `CHECKLIST.md`, `DATA_SOURCES.md`, and schema
  contracts;
- the simulation-scene and habitable-zone consumers;
- official Gaia DR3 and NASA Exoplanet Archive data models.

The served checkpoint contains 5,869,091 systems, 5,874,636 CORE stars, 6,311
planets, 17,823 eclipsing-binary rows, 177,558 compact-object rows, 2,017 open
clusters, and 2,327 superstellar objects.

Upstream availability counts are point-in-time TAP results retrieved on July
17, 2026 using the same `parallax >= 3.26156 mas` outer source boundary as the
Gaia backbone. They are not immutable until captured by a Spacegate source
manifest.

## Priority Findings

### P0: Gaia DR3 physical parameters are the largest missed source

Spacegate already queries `gaiadr3.astrophysical_parameters` for 15,864,770
nearby-source classifier rows, but the extract retains only DSC class
probabilities. The same official table contains FLAME luminosity, radius, mass,
age, evolutionary stage, bounds, and quality flags, plus extinction, activity,
H-alpha, and high-resolution spectral classifications.

Official TAP counts at the Spacegate outer boundary:

| Available field family | Rows |
| --- | ---: |
| FLAME luminosity and radius | 3,428,436 |
| FLAME mass | 1,136,048 |
| FLAME age | 1,026,163 |
| FLAME evolutionary stage | 1,490,658 |
| GSP-Phot G-band extinction | 3,998,692 |
| ESP-CS chromospheric activity | 676,065 |
| ESP-ELS H-alpha and spectral type | 3,847,191 |
| GSP-Spec alpha abundance | 734,425 |

The served ARM currently has 5,544,667 Gaia parameter rows and 1,982,472 Gaia
temperature/log-g/metallicity rows, but **zero Gaia radius, mass, luminosity,
age, or rotation values**. Only 2,002 matched NASA host rows currently provide
source luminosity.

Action:

- create a pinned, narrow `gaia_astrophysical_parameters_v2` extract;
- preserve source values, bounds, flags, model family, and source version in
  ARM;
- prefer quality-gated FLAME source luminosity over display priors;
- retain evolutionary stage and extinction for classification safety,
  narration, HZ sizing, stellar textures, and dust-aware map analysis;
- do not write FLAME estimates into immutable CORE spectral facts;
- gate remnants and bad FLAME solutions before using luminosity for HZs.

References:

- <https://gea.esac.esa.int/archive/documentation/GDR3/index.html>
- <https://gaia.aip.de/metadata/gaiadr3/astrophysical_parameters/>

The aggregate TAP queries used for this audit were:

```sql
SELECT
  COUNT(*) AS n,
  COUNT(ap.lum_flame) AS lum_n,
  COUNT(ap.radius_flame) AS radius_n,
  COUNT(ap.mass_flame) AS mass_n,
  COUNT(ap.age_flame) AS age_n
FROM gaiadr3.gaia_source AS g
JOIN gaiadr3.astrophysical_parameters AS ap ON g.source_id = ap.source_id
WHERE g.parallax >= 3.26156;

SELECT
  COUNT(ap.ag_gspphot) AS extinction_n,
  COUNT(ap.evolstage_flame) AS evolstage_n,
  COUNT(ap.activityindex_espcs) AS activity_n,
  COUNT(ap.ew_espels_halpha) AS halpha_n,
  COUNT(ap.spectraltype_esphs) AS spectraltype_n,
  COUNT(ap.alphafe_gspspec) AS alpha_n
FROM gaiadr3.gaia_source AS g
JOIN gaiadr3.astrophysical_parameters AS ap ON g.source_id = ap.source_id
WHERE g.parallax >= 3.26156;
```

### P0: Gaia DR2 open-cluster membership is not reconciled to DR3

The Cantat-Gaudin membership file contains 234,128 Gaia DR2 IDs. Current ingest
joins those values directly to `core.stars.gaia_id`, which is Gaia DR3. The full
build linked only 7,710 rows; the served 1,000-ly slice retains 4,567.

This violates the identity rule established during TESS integration: Gaia DR2
and DR3 source IDs are not interchangeable.

Action:

- use the official Gaia `dr2_neighbourhood` relation;
- resolve in the full canonical universe before slicing;
- require unique, quality-gated DR2-to-DR3 outcomes;
- partition every membership row into accepted, ambiguous, excluded, or
  missing with a reason;
- apply the same reusable resolver to the four UltracoolSheet DR2-only rows and
  any other DR2 fallback path.

### P0: NASA planet physical evidence is acquired but flattened

The current `pscomppars` snapshot contains 6,298 planets. It has 6,267 best
masses with `pl_bmassprov`, 6,157 densities, 6,248 radii, 4,479 transit depths,
5,986 host luminosities, 5,980 host radii, 6,289 host masses, 4,868 host ages,
and 917 host rotation periods. CORE currently exposes mass for 2,396 planets
because it promotes the narrower true-mass fields.

That conservative CORE behavior is preferable to pretending every best mass is
a direct measurement. The gap is the missing typed evidence layer: estimates,
limits, uncertainties, references, and provenance are largely flattened or
discarded outside the already-normalized orbital solutions.

Action:

- materialize source-specific `ps` physical parameter observations in ARM;
- preserve composite `pscomppars` values separately from literature solutions;
- retain bounds, limit flags, solution/reference IDs, reference links, update
  dates, detection flags, transit observables, density, and best-mass
  provenance;
- classify `Mass`, `Msini`, deprojected estimates, and mass-radius-relation
  estimates as different evidence types;
- promote only policy-approved values to CORE and map categories.

NASA explicitly describes `ps` as one row per planet per reference and
`pscomppars` as a more complete but not necessarily self-consistent composite:
<https://exoplanetarchive.ipac.caltech.edu/docs/API_PS_columns.html>.

### P0: ORB6 normalization is edge-dependent and incomplete

ORB6 cooking preserves 4,051 orbit rows across 3,681 WDS scopes. Of those rows,
4,049 have a period, 4,037 have semimajor axis and inclination, 2,437 have
eccentricity, and 3,081 have an orbit grade.

In the served build:

- 1,434 ORB6 rows find a CORE WDS system;
- only 56 find the already-existing unique binary edge required by the ARM
  builder;
- 2,617 are outside the served inventory or otherwise unlinked;
- only 56 become normalized ARM orbital solutions.

The 56 accepted rows are safe, but the remainder is not adequately accounted
for. ORB6 should be evidence capable of helping create or reconcile the binary
edge, not only evidence accepted after some other source already created it.

Action:

- preserve all ORB6 source rows in an ARM detail table;
- reconcile discoverer/component scope through WDS pair evidence and canonical
  component endpoints;
- accept only unique component-scoped bindings;
- emit accepted, ambiguous, excluded, and missing-endpoint outcomes for all
  4,051 rows;
- never turn a WDS coordinate pair into a bound orbit solely because ORB6 and
  WDS share a system label.

### P1: Gaia NSS acquisition omits orientation and uncertainty evidence

The cooked Gaia NSS two-body extract has 36,151 rows, all with period,
eccentricity, flags, and significance. It contains zero semimajor axes,
inclinations, primary velocity amplitudes, or mass ratios. The served ARM
normalizes 31,429 of those rows, but every one lacks inclination and semimajor
axis.

The official table contains solution-type-dependent fitted parameters,
Thiele-Innes elements, uncertainties, correlations, time of periastron, and
additional spectroscopic/eclipsing parameters. Spacegate's downloader selected
only a narrow early subset.

Action:

- expand the pinned Gaia NSS extract to preserve fitted parameters,
  uncertainties/covariance metadata, and solution-type bit flags;
- derive orientation only with the documented Gaia transform and quality
  policy;
- retain unsolved parameters as null rather than generating arbitrary
  scientific values;
- keep visual simulation priors clearly separate.

Reference:
<https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_non--single_stars_tables/ssec_dm_nss_two_body_orbit.html>.

### P1: high-value source physics is stranded in side tables

| Source | Available now | Current limitation | Recommended projection |
| --- | --- | --- | --- |
| DEBCat | 374 rows with component mass, radius, Teff, log-g; 299 with luminosity | 373 rows survive as EB records, 31 link to a served system, and only a small exact subset binds component evidence | Bind unique system+period+endpoint matches into shared stellar-parameter evidence; quarantine the rest |
| ATNF | 4,393 pulsars; 2,272 periods, 890 period derivatives, 2,059 spin frequencies | CORE keeps identity/location but drops native spin and association physics | Add ARM compact-object observation table for pulsar animation, timing, and narration |
| McGill magnetars | 31 rows; 26 periods, 25 period derivatives, 28 activity/band records | CORE keeps identity/location only | Preserve spin, activity, associations, and bands in ARM |
| Gaia EDR3 WD | 1,280,266 candidates; 305,150 best physical fits plus H/He alternatives and chi-square | CORE keeps selected best values but drops competing atmosphere-model evidence | Preserve H/He fits and fit quality in ARM; keep selected projection explainable |
| Open clusters | 2,017 rows; 1,867 ages, extinction, distance modulus, and Galactic positions | CORE drops age, extinction, dispersions, distance modulus, and Galactic context | Add source-native cluster physical evidence; use it for formation/age concepts and AAA context |
| Green SNR | 310 rows with extent, morphology, 1 GHz flux, spectral index | Most metadata survives JSON, but 1 GHz flux is dropped | Preserve flux and citation-native fields in typed ARM evidence |
| TESS EB | 17,605 rows with sectors, Tmag, Teff, log-g, metallicity, source, flags | sectors/source/flags are dropped; Tmag is written into a legacy field named `kmag` | Add explicit Tmag and source metadata; preserve sector/flag evidence; do not relabel Tmag as K magnitude |

### P1: milestone/source-policy mismatch needs an explicit decision

M5.3 specifies exoplanet.eu, OEC, and HWC lifecycle evidence, candidate defaults,
and reversible status history. The current default build has zero lifecycle
status/history rows because those catalogs are optional and disabled. TESS
candidates are correctly retained as ARM evidence and do not contaminate
canonical planet counts, but the broader M5.3 contract is not fulfilled by the
production profile.

Before the next rebuild, choose and document one of these policies:

1. implement M5.3 as ARM lifecycle evidence with NASA-confirmed planets
   remaining the CORE inventory authority; or
2. narrow M5.3's stated acceptance criteria and explicitly defer the broader
   lifecycle sources.

The first option better matches Spacegate's auditable-evidence goals, provided
candidate evidence remains visibly distinct from confirmed CORE planets.

## Source Matrix

### Strongly utilized; no rebuild blocker

| Source family | Assessment |
| --- | --- |
| Gaia backbone astrometry/photometry | CORE inventory and ARM uncertainty/quality fields are well used; add cheap `phot_variable_flag`, `grvs_mag`, and `vbroad` fields during the v2 refresh rather than creating a separate source job |
| Gaia DSC class probabilities | Correctly used for remnant safety; retain as one module of the broader AP extract |
| MSC | Source component, system, and orbit details are preserved; 4,627 of 4,633 orbit rows normalize and six are explicitly excluded |
| WDS | All 157,299 observations are accounted for as non-binding evidence; correctly creates no orbit by itself |
| SB9/SBX | Complementary source-native identities, spectra, aliases, and orbit evidence are preserved with conservative endpoint binding |
| VSX | Period, amplitude, variability type/family, and confidence survive in ARM; do not replace it with Gaia variability, which is complementary |
| Gaia UCD and UltracoolSheet | Youth, gravity, kinematics, multiplicity, and nearby-inventory evidence are preserved; repair only generic DR2 fallback resolution |
| TESS T0-T3 | TIC/TOI identity partition, candidate/negative evidence, search, and canonical planet-count isolation are in good shape |
| WISE/CatWISE/AllWISE | Targeted identity, photometry, motion, and candidate evidence follow the intended bounded policy |
| Extended objects | Source identity, geometry, relations, and available distance evidence are separated correctly; sparse distances largely reflect source limitations |
| Sol authority/artificial | Orbits and hierarchy are authoritative, separated, and lineage-complete for the current bounded scope |
| AT-HYG | Correctly transitional for aliases/recovery and not canonical inventory authority |

### Intentionally deferred; do not block the evidence rebuild

| Source/product | Reason |
| --- | --- |
| Full TIC, CTL, TCE, and bulk TESS light curves | Scale and presentation contract remain separate TESS goals |
| Gaia epoch photometry/light curves | Valuable for variability and flare presentation, but bulk time-series storage needs its own artifact policy |
| Gaia BP/RP and RVS spectra | Valuable for narration and spectra concepts; use targeted/datalink retrieval before considering bulk ingest |
| Gaia `vari_rotation_modulation` | High-value bounded future acquisition for rotational animation; evaluate after the shared rotation evidence contract exists |
| Kepler EB | Re-run the old low-yield evaluation against the 1,000-ly identity graph; do not enable by habit |
| Full WISE/CatWISE inventory | Targeted policy remains appropriate; nearby candidate discovery belongs to reviewed AAA workflows |
| Survey imagery and extended-object rendering | Presentation layer, not canonical rebuild input |

## Habitable-Zone Disk Diagnosis

The missing disks are mostly deliberate, not a WebGL defect.

`SystemPreviewPanel.jsx` draws a habitable-zone disk only when the scene exposes
a positive `luminosity_lsun`. Scene generation uses this precedence:

1. source ARM luminosity;
2. ARM derived luminosity;
3. Stefan-Boltzmann luminosity from source radius and Teff;
4. a guarded O/B/A/F/G/K/M main-sequence spectral prior;
5. no HZ disk.

The main-sequence prior is rejected for giants, subgiants, remnants, compact
objects, unsupported classes, and uncertain/unclassified endpoints. That avoids
placing a solar-like HZ around a giant, white dwarf, neutron star, or brown
dwarf from an invalid dwarf-class assumption.

Approximate served-star accounting:

| Scene luminosity outcome | Stars |
| --- | ---: |
| Guarded main-sequence prior available | 4,966,395 |
| Source luminosity or source radius+Teff | 2,002 |
| No HZ: L/T/Y ultracool or brown dwarf | 387,425 |
| No HZ: unclassified/unsupported | 342,020 |
| No HZ: remnant/compact | 176,695 |
| No HZ: evolved without usable physics | 99 |

This is an approximation over CORE stars. Source-only hierarchy leaves and
per-scene component evidence can change individual systems.

The current policy is scientifically preferable to drawing every disk. The
problem is that almost all displayed ordinary-star HZs are based on illustrative
spectral priors, while only 2,002 served stars have source luminosity. Gaia
FLAME should replace many priors with evidence and recover defensible HZs for
unclassified or evolved stars. It should not automatically enable HZs for
remnants, bad-quality solutions, or unresolved components.

The visualization also remains a single-host radiative approximation. It does
not yet calculate combined irradiation in close multiple systems or long-term
HZ stability. Those limitations should remain explicit when the planet/HZ
classifier is upgraded.

## Recommended Single Rebuild Bundle

### G0. Freeze, baseline, and retention

- capture current source manifests, current utilization JSON, and build hashes;
- run the documented retention audit before acquiring new Gaia products;
- `/data` is currently 87% used with 190 GB free, while immutable `out/`
  artifacts occupy about 817 GB;
- preserve served, rollback, published, referenced, and unique-source artifacts;
  remove nothing ad hoc.

### G1. Shared identity reconciliation

- implement one full-canonical Gaia DR2-to-DR3 resolver;
- use it for open clusters and every remaining DR2 fallback;
- emit exhaustive outcome and ambiguity reports before slicing.

### G2. Gaia evidence v2

- acquire a narrow pinned AP/FLAME/evolution/extinction/activity column set;
- expand NSS fitted-parameter and uncertainty acquisition;
- add cheap Gaia source variability/RVS support columns;
- project source-native values into ARM with quality flags and bounds.

### G3. Multiplicity and component physics

- preserve and reconcile all ORB6 rows;
- project accepted DEBCat component physics into shared stellar evidence;
- keep SB9/MSC/WDS safeguards and no-one-off policy unchanged.

### G4. Planet evidence v2

- materialize NASA physical parameter observations, uncertainties, limits,
  references, transit observables, density, and mass provenance;
- update broad map categories only after typed evidence acceptance;
- retain unchanged canonical planet counts as a hard gate.

### G5. Compact, cluster, SNR, and TESS EB native evidence

- preserve already-cooked source physics in ARM;
- correct the TESS Tmag schema/name;
- retain source rows even when canonical binding is missing;
- expose no new public claims until consumers are ready.

### G6. One canonical rebuild and one public slice

- run deterministic A/B rebuild comparison;
- verify exhaustive source accounting and zero object-specific transforms;
- compare HZ source/prior/missing coverage before and after;
- compare planet-category changes with reason codes;
- regenerate hierarchy, ARM, DISC, tiles, and scene-v4 artifacts;
- verify desktop/mobile API, search, map, Peek, Explorer, and System pages;
- deploy only after the single candidate passes all gates.

## Acceptance Gates

- every DR2 membership, ORB6 row, and component-physics binding has an explicit
  accepted, ambiguous, excluded, missing, or quarantined outcome;
- no DR2 identifier is treated as a DR3 identifier by equality alone;
- Gaia physical values retain bounds, flags, method, and source version;
- HZ disks report source/derived/prior/missing basis and never use an unguarded
  dwarf proxy for evolved or remnant objects;
- NASA best mass never loses `pl_bmassprov`, uncertainty, or limit semantics;
- TESS magnitude is not labeled as K-band magnitude;
- compact-object spin/activity and cluster age/extinction survive into ARM;
- canonical planet count and object identities remain stable unless an
  independently reviewed inventory rule justifies a delta;
- repeated builds from pinned inputs reproduce scientific hashes;
- no source-specific finding is repaired with a named-system transform.
