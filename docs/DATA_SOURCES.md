# Spacegate Data Sources (Gaia-First)

## Evidence Lake v2 Registry

The active machine-readable source contract is
`config/evidence_lake/source_releases.json`. It records source/release identity,
domain-specific authority roles, retrieval implementation, license/citation,
cadence, identifier namespace, frame/epoch context, schema policy, and storage
class. `config/evidence_lake/schema_baseline.json` pins the current source schema
and field-disposition baseline. The current July 22 baseline contains 150
active manifest entries and 6,273 fields. It includes the reviewed complete
JPL Horizons element set and the 22-field CatWISE2020 plus 24-field AllWISE
targeted response contracts.

Validate and audit before acquisition:

```bash
.venv/bin/python scripts/evidence_lake_registry.py validate
.venv/bin/python scripts/evidence_lake_registry.py audit \
  --state-dir /data/spacegate/state \
  --report /data/spacegate/state/reports/evidence_lake_v2/e0_registry_audit.json
```

The default field disposition is `preserve`: an acquired upstream column is
kept in the immutable raw snapshot and source-native typed layer unless a
reviewed field rule explicitly normalizes, indexes only, or omits it with a
reason. Evidence promotion is a later per-quantity policy and must not be
confused with source preservation.

Full-refresh preflight runs this gate automatically. New manifest entries,
missing active artifacts, and schema changes fail until the registry and pinned
baseline are reviewed. Planned sources may lack manifests until E3 acquisition;
active sources may not.

The current filesystem reports about 80 GiB free on `/data`; the stricter
300-GiB Evidence Lake acquisition floor therefore remains unmet and
`acquisition_ready=false`. Large acquisition or build work must wait for a
reviewed retention action or added capacity. The unrecognized E6 build IDs are
protected candidate/stability products that require explicit retention
accounting; they are not cleanup permission.

Evidence Lake v2 active storage paths are:

- `raw/evidence_lake_v2/<source>/<release>/<snapshot>/`: immutable byte-
  preserving source snapshots
- `typed/evidence_lake_v2/<source>/<release>/<raw>/<parser>/`: independently
  versioned source-native Parquet tables
- `reports/evidence_lake_v2/e1_typed_cook_report.json`: typed coverage, rows,
  fields, parser contracts, and payload sizes
- `reports/evidence_lake_v2/e1_snapshot_verification.json`: raw and typed hash,
  row, and artifact-accounting verification
- `reports/evidence_lake_v2/e1_clean_reproduction.json`: clean-root deterministic
  rebuild evidence
- `raw/gaia_dr2_identity*/snapshots/`: immutable forward and independently
  acquired reverse Gaia release-neighborhood snapshots with exact ADQL chunks
- `derived/evidence_lake_v2/identity/<graph_id>/`: immutable E2 identity/scope
  graph database plus ordered Parquet tables
- `reports/evidence_lake_v2/e2_identity_graph_report.json`: exhaustive target,
  reverse-universe, collision, quarantine, scope, and artifact accounting
- `reports/evidence_lake_v2/e2_identity_reproduction.json`: independent graph
  compile comparison by rows, bytes, and SHA-256
- `derived/evidence_lake_v2/scientific_evidence/<build_id>/`: immutable E4
  source-shard databases and manifests
- `derived/evidence_lake_v2/scientific_evidence_sets/<release_set_id>/`: atomic
  E4 accepted-artifact manifest and table-shard index; the active set references
  source shards read-only and does not duplicate their 449.2 GB of evidence
- `raw/evidence_lake_v2_acquisition/<source>/<release>/snapshots/`: E3 exact
  TAP response sets with ADQL, UWS lineage, source schemas, field dispositions,
  row/MAXREC checks, and response hashes
- `raw/evidence_lake_v2_http/<source>/<release>/snapshots/`: E3 pinned release
  files with resumable transfer state, expected size/checksum gates, and
  content-addressed product manifests
- `reports/evidence_lake_v2/e3_source_coverage_report.json`: registered TAP
  product, row, byte, upstream-field, and deliberate-omission coverage
- `reports/evidence_lake_v2/e3_http_acquisition_report.json`: pinned release-
  file completion, byte, checksum, and pending-product coverage
- `reports/evidence_lake_v2/e0_e7_completion_audit.json`: current checkpoint
  evidence, missing reports/artifacts, and explicit open E7 gates

Run the complete E1 gates with:

```bash
.venv/bin/python scripts/evidence_lake_store.py \
  --state-dir /data/spacegate/state cook
.venv/bin/python scripts/evidence_lake_store.py \
  --state-dir /data/spacegate/state verify \
  --report /data/spacegate/state/reports/evidence_lake_v2/e1_snapshot_verification.json
.venv/bin/python scripts/verify_evidence_lake_reproduction.py \
  --state-dir /data/spacegate/state \
  --report /data/spacegate/state/reports/evidence_lake_v2/e1_clean_reproduction.json
```

Observation payloads follow
`config/evidence_lake/observation_product_policy.json`: durable metadata and
lineage stay in the typed lake, while spectra, light curves, and imagery are
retrieved into a bounded checksum-addressed cache only on an approved trigger.
Arbitrary caller URLs, unbounded transfers, and bulk Gaia/TIC/product mirrors
are prohibited.

E3 collectors are data-driven:

```bash
.venv/bin/python scripts/evidence_tap_acquire.py --product PRODUCT_NAME
.venv/bin/python scripts/evidence_http_acquire.py --source-id SOURCE_ID
```

Both collectors separate long job budgets from stalled transfers. TAP response
reads and resumable HTTP downloads default to a 180-second socket-inactivity
timeout, so a route change or dead stream retries promptly without discarding
completed buckets or partial release files. Operators may override it with
`--read-stall-timeout`. An asynchronous TAP retry first aborts its nonterminal
UWS job and preserves every attempt and cleanup result in the bucket lineage;
this prevents timed-out queries from continuing upstream beside replacements.
If the abort itself cannot be confirmed, retry is suppressed and the bucket
fails closed. An operator may resume only after the prior job's terminal state
is observed and recorded.

The Gaia program preserves a disjoint two-branch ingestion envelope. The
measured hard-parallax branch contains 31,987,126 `gaia_source` rows at
`parallax >= 2.609272` mas. The uncertainty branch emits only Gaia DR3 fields
for rows outside that cut, selected through the Gaia Archive's hosted
`external.gaiaedr3_distance` relation when the geometric posterior median is
within the 383.245-pc build buffer or the lower 16th-percentile bound overlaps
the 306.601-pc public sphere. The Bailer-Jones release is acquired separately;
its estimates are evidence and never replace Gaia source astrometry.

For source tables beyond `gaia_source`, repeatedly executing that three-way
archive join proved unsafe: some plans exceeded Gaia VMEM and smaller plans
remained nonterminal. The accepted uncertainty-envelope Parquet table is now
the authoritative acquisition target input. A deterministic compiler verifies
its checksum and emits content-addressed seed `638c3ff4e58abcd355029e0f`
containing exactly 189,145 unique sorted Gaia DR3 source IDs. Direct `VALUES`
queries are divided into 31 modulo buckets and every product manifest pins the
seed, values, artifact, coverage, and bucket hashes. This is an execution
optimization only; it cannot add targets outside or omit targets inside the
accepted envelope.

The completed E3 acquisition report accounts 56 products, 170,253,376 rows,
23,970,068,085 bytes, and no pending product. Expanded Gaia AP,
supplementary-AP, NSS, variability/rotation, and external-crossmatch snapshots
produce 30 verified typed tables with 83,908,762 rows and 1,320 column
occurrences. Each release also passes an independent clean raw-to-typed
reproduction.

E4 compiler/contract v72/v73 materializes all 592,197 bounded Gaia variability
and rotation rows through four ordered coherent schemas. The source-native
Parquet retains exact vector strings; E4 normalizes the 52 rotation-vector
fields to nullable numeric arrays while preserving whole-vector absence and
positional source masks as distinct states. All 268 field occurrences and
592,197 citation links are accounted, and clean reproduction matches logical
hash `d98283bb5477211963902e072b4aaf7095740435efeff567950dbcfe934dea2b`.

The separate Bailer-Jones envelope snapshot contains 17,310,560 rows and all
10 source fields; typed snapshot `5a60000592215924b3305095` passes verification
and clean reproduction. VizieR names its lower and upper percentile columns
with case-only distinctions such as `b_rgeo` and `B_rgeo`. Because DuckDB
resolves identifiers case-insensitively, the VOTable cooker assigns a
deterministic `__source_case_N` typed alias to the later collision and records
the exact upstream name beside it in schema lineage. Values remain distinct;
the query engine can no longer rename one silently.

E4 diagnostic `520df722a1564ee857b1ae43` consumes that release independently
of Gaia astrometry. It preserves 17,310,560 `gaia_edr3_source_id` claims and
17,310,560 coherent distance bundles containing 33,225,308 published geometric
and photogeometric posterior measurements. The lower/upper fields retain
16th/84th-percentile endpoint semantics rather than being represented as
symmetric errors, and all measurements link to `2021AJ....161..147B`. The two
copied Gaia coordinate fields remain in typed source records but are reviewed
E4 exclusions because Gaia owns coordinate evidence. All 10 fields are
accounted and the generic artifact audit passes logical hash
`b74aabea2625f660ab85e0b723d7598a4b6cd9af6010c196d51229f743e84381`.
The source-specific audit rejects this v54 artifact because `exclude` fields
were nevertheless copied into `source_context_json`. Compiler v56 corrects the
general disposition rule while retaining intentional context and lineage. A
v55 attempt was stopped before promotion when that distinction was found.
Accepted v56 `2147d1c60f6401fdc725d96e` passes generic and source-specific
audits with all checks at zero and logical hash
`eceb390e97cba1b69d8a5780181b8947dfed6ed78c51167316ad4936b4506730`;
clean reproduction matches with no differing sections and removes its USB
scratch tree. v56 is accepted; v54 and v55 are not accepted checkpoints.

E5 policy `2026-07-21.e5-selection.3` consumes accepted v56 through an explicit
authoritative Gaia EDR3-to-DR3 release relationship, not generic source-ID
equality. It binds 4,662,948 of the 17,310,560 bounded records to current stars
and records the other 12,647,612 as missing from the current canonical
population. The accepted rows select 4,662,948 geometric and 4,344,950
photogeometric posterior estimates with exact endpoint, method, model,
reference, evidence, bundle, and source-record lineage. The 1,203,650 current
Gaia stars absent from this bounded source are not negative evidence; 1,200,620
have parallax signal-to-noise below five and remain an explicit E6 population
review rather than being silently dropped.

Hunt-Reffert typed snapshot `cbfa7c6ec8c2e3bfbc226898` independently
preserves 7,167 cluster rows/78 fields, 1,291,929 membership rows/66 fields,
and 29,956 literature-crossmatch rows/17 fields. Its raw/typed verification and
clean reproduction pass. E4, not E1, owns interpretation of membership
probabilities and cluster physical parameter sets.

E4 Hunt/Reffert checkpoint `7e66e0690aa962c837d43a86` selects clusters whose
published 16th-percentile distance overlaps 383.245 pc, then retains every
membership and literature-crossmatch row attached to those selected cluster
IDs. The result is 465 clusters, 51,017 probability-bearing member claims, and
451 crossmatches with all 161 fields accounted. Exact VizieR source names map
through the pinned query output-name contract rather than losing lineage when a
legal Parquet alias is required. Membership remains evidence, never canonical
containment.

E5 cluster artifact `a6169c9ec351db81104e8518` resolves the two endpoint
families independently. Exact published source/literature designations bind 62
clusters one-to-one, leave 393 outside the current reference, and retain 10
ambiguous collision outcomes instead of collapsing distinct source clusters.
Only accepted coherent cluster posterior sets are quantity-selection inputs.
Exact Gaia DR3 identity binds 17,273 of 51,017 member endpoints; 4,247 claims
have both endpoints accepted. Published HDBSCAN probabilities remain evidence
and never create canonical containment.

E5 extended-object artifact `3790054572476ea189aaff06` uses exact catalog
source-reconciliation keys rather than coordinates or visual overlap. It binds
all 310 Green SNR rows and 17,800 of 19,012 OpenNGC-family rows; the remainder
stay explicitly excluded (803), quarantined (404), or unresolved redirects
(five). All geometry, distances, physical context, names, components, and
citations remain in the extended-object evidence domain. No row is projected
as a stellar selected fact.

SIMBAD is not bulk-mirrored or used as inventory: its release-pinned Gaia DR3
bridge is intersected locally before targeted alias, basic-data, and
bibliography acquisition. Full TIC, Gaia observation products, and survey
spectra remain excluded from bulk mirroring; registered metadata and product
locators drive bounded on-demand retrieval.

The complete staged SIMBAD snapshot contributes 35,321,742 typed rows across
the bounded Gaia-envelope bridge, basic, identifier, and bibliography tables.
E4 checkpoint `fc5bd4e6398d72bde50ba6d5` accounts every registered field,
materializes identity, classification, astrometry, and bibliography evidence,
and quarantines 285 component-suffixed HIP aliases that cannot normalize as
numeric HIP claims. Its independent audit and clean logical-hash reproduction
pass. E5 policy v10 selects 321,584 source spectral classifications only through
a unique same-release SIMBAD-OID-to-Gaia bridge. Eight multi-target bridges stay
ambiguous and 113,487 classification subjects stay missing. All other SIMBAD
channels remain evidence and SIMBAD does not create canonical inventory.

Pinned HTML sources must declare their semantic table contract when one exists.
The WGSN snapshot declares and validates all 16 catalog headers, preserves row
identity and linked resources, and excludes unrelated page tables and footer
controls with machine-counted reasons. The exact HTML remains the immutable raw
authority.

E4 WGSN checkpoint `0ff30b04008b93aafb3de66f` treats WGSN strictly as a
naming authority. It emits official-name, NEC+, catalog-designation, HIP,
Bayer, SIMBAD-search-spelling, and exact derived HR/HD/HIP/GJ identifier claims
with separate alias, observation-target, source-record, and ambiguous system-
or-component scopes. Source coordinates and magnitude remain naming context,
not astrometric or photometric authority. The `-` designation and 28 `--`
reference placeholders remain raw context rather than fabricated identity or
citation evidence. All 22 fields are accounted; artifact, scope, and clean-
reproduction gates pass. E5 policy v10 selects 415 exact official proper names
whose HIP/HD/HR/GJ claims converge on one unique canonical star. It keeps 180
missing names and the Izar/Pulcherrima two-record component collision explicit;
search spellings, Bayer scope, provenance, coordinates, and imagery remain
evidence rather than competing winners.

IAU 2015 Resolution B3 is pinned separately as an official standards source,
not folded into WGSN naming evidence. Raw snapshot `5c07a3926211b87eab6f72bf`
preserves the exact six-page PDF; typed snapshot `d25948decca32f65e9783065`
fails closed on the pinned PDF hash, page count, reviewed source fragments, and
`pypdf==6.14.2` extraction contract. It preserves all eleven exact nominal
conversion constants as reference-standard context and separately preserves
the published current best solar effective-temperature estimate
`5772.0 +/- 0.8 K`. Nominal constants are never labeled as measurements.

E4 artifact `74972d83b964ccf0dc06641c` routes only that physical best estimate
to stellar parameter evidence. E5 build `33006bde9bedd1fb365238b5` binds the
scoped Sun parameter set through the general unique-name identity contract and
selects one `teff_k` fact with exact evidence lineage. No production transform
branches on the Sun or any benchmark identifier.

GCVS uses a documented fixed-width lexical contract. Its registry explicitly
declares `|` as a trailing layout delimiter, so parser v2 removes one trailing
separator after slicing and trims only the remaining layout padding. It does
not split or remove internal delimiters such as `SR|Cst`, and `raw_row` remains
byte-faithful. Typed snapshot `ef540a47c43892e17ddc2bae` preserves 340,839
rows across all six artifacts; its 203,740 scoped cell normalizations, table
schemas, raw checksums, verification, and clean reproduction are machine
accounted.

E4 GCVS checkpoint `a6f6669d2bd48eac5d6204d2` materializes every typed row
without choosing public winners: 705,684 release-scoped identifier claims,
289,892 source astrometry measurements, 29,042 source spectral classifications,
444,566 variability observations, 21,526 citations, and 756,305 citation links.
Repeated bibliography lines aggregate under their source key; component-
suffixed GCVS and NSV records do not claim the unsuffixed numeric identity.
Source sign/component fields remain raw evidence when normalized coordinates
require an explicit conflict flag. All object bindings remain unresolved for
the identity and selection compilers.

The July 19 NASA Exoplanet Archive snapshot preserves 12 independent TAP
tables rather than pre-merging parameter sets: 206,989 rows and 2,093 fields
cover reference-specific planets, composite parameters, hosts, TOIs, K2,
Kepler/K2 name bridges, DR25 and cumulative KOIs, DR25 TCEs, and transit-
detection metadata. Product manifests account every upstream field as selected;
winner selection and scientific normalization occur only in E4/E5.

APOGEE DR17, GALAH DR4, and LAMOST DR11 are preserved as pinned release FITS
artifacts and source-native typed Parquet. The FITS contract supports fixed-size
numeric/string arrays and registry-declared multiple table HDUs. APOGEE's
allStar, model-grid, and field-version extensions therefore remain distinct and
schema-gated; no extension or vector-valued parameter set is silently dropped.
APOGEE E4 admission is bounded through the exact registered Bailer-Jones EDR3
typed snapshot because `GAIAEDR3_SOURCE_ID` is release-specific; it is never
compared directly with Gaia DR3 IDs. The accepted v58 checkpoint retains
178,099 allStar rows and materializes calibrated scalar ASPCAP physics,
abundances, target photometry/extinction, coordinates/RV, release-scoped
identities, citations, and product locators. Copied Gaia columns and redundant
ASPCAP arrays remain preserved in E1 but are deliberate E4 exclusions.

GALAH E4 admission is bounded by the checksum-bound OR of the registered Gaia
DR3 hard-parallax and uncertainty-supplement envelope tables. Accepted v62
checkpoint `a4fc03c66ea1cfb44c25df28` retains 117,885 allStar rows and accounts
all 184 fields. The adapter keeps the pipeline spectroscopy and 31 elemental
abundances coherent; separates source isochrone mass, age, bolometric
correction, and luminosity; and materializes source-native coordinates,
distance estimates, primary/candidate-secondary and SB2 radial velocities,
extinction, interstellar absorption lines, and hydrogen/lithium diagnostics.
Copied Gaia, 2MASS, and AllWISE values remain E1-preserved reviewed exclusions.
The source descriptions define `r_med/r_lo/r_hi` as distance values used to
calculate parallax gravity; the rejected v61 diagnostic mislabeled them as
stellar radii, and v62 corrects this without rewriting either immutable source
snapshot. All values retain source flags, model/method, reference, and lineage.
E5 policy `2026-07-21.e5-selection.4` admits only `flag_sp=0` atmosphere
solutions with CCD3 S/N above 30, using S/N to order repeats after scientific
authority and coherent-set completeness. The allStar file supplies no
source-native spectrum locator, so a registered GALAH spectra-index acquisition
remains pending rather than deriving an unverified URL.

LAMOST E4 admission uses the same checksum-bound Gaia DR3 hard/uncertainty
envelope without comparing DR2, EDR3, and DR3 identifiers as interchangeable.
Accepted v63 checkpoint `a583819f0a4f3896c312f19e` retains 661,941 LRS
stellar, 496,415 LRS M-star, and 500,925 MRS stellar observations and accounts
all 185 field occurrences. Source-native LASP, CNN, molecular-index/activity,
and raw/zero-point-corrected arm/combined RV contexts remain separate.
Official `obsid`/`mobsid` identifiers are on-demand spectrum-product locators;
the compiler does not invent archive URLs. Copied Pan-STARRS/Gaia photometry is
preserved in E1 and excluded from E4 competition. Generic and bounded LAMOST
audits plus clean reproduction pass. E5 now binds all eligible records through
their release-scoped Gaia DR3 claims, accounts 844,959 accepted, 806,226
missing, and 14 ambiguous records, and selects source-published M-star, MRS
LASP, LRS LASP, then MRS CNN atmosphere tiers with the appropriate source S/N
field used only to order repeats.

APOGEE E5 binding uses the same explicit, release-specific EDR3-to-DR3
source-list contract as the distance evidence; it does not introduce generic
cross-release equality. ASPCAP atmosphere candidates must have positive S/N
and no `STAR_BAD` summary bit, following the
[official APOGEE DR17 bit definitions](https://www.sdss4.org/dr17/irspec/apogee-bitmasks/).
The GALAH threshold follows its
[official flag guidance](https://www.galah-survey.org/dr4/flags/) and
[data-use recommendations](https://www.galah-survey.org/dr4/using_the_data/);
LAMOST repeat ordering retains the quality fields defined by its
[DR11 LRS production documentation](https://lamost.org/dr11/v2.0/doc/lr-data-production-description).
Across the three spectroscopy releases, E5 selects 265,705 coherent atmosphere
sets and adds temperature/gravity coverage for 40,647 stars. Elemental
`[Fe/H]` remains distinct from global `[M/H]` and is not silently remapped.

E4 source adapters are declared in
`config/evidence_lake/e4_scientific_evidence.json`. They assign every upstream
field a domain, identity, lineage, context, or reviewed-exclusion disposition
and separately report whether that destination has actually been materialized.
The compiler uses exact row hashes plus source-native logical keys, preserving
duplicate occurrences and avoiding point/array position as scientific identity.
NASA checkpoint `cb82c09179afa740b02e2cdf` materializes 750,151 identifier
claims and 72,809 planet-lifecycle claims from the 12 pinned products. Candidate
and negative claims remain evidence only and cannot inflate canonical planet
inventory. Identifier claims carry semantic scope independent of the mixed
planet/host/target source row, producing separate auditable binding outcomes.
The field-complete adapter also emits 9,689,745 typed science rows, 272,355
coherent parameter sets, 111,084 on-demand Kepler validation-product locators,
2,961 parsed source references, and 4,656,423 evidence-citation links. All 2,093
source fields are materially represented or deliberately excluded with a reason;
raw units and reference fragments remain preserved beside normalized aliases and
parsed metadata.

The El-Badry Gaia EDR3 wide-binary release is preserved under the same raw and
typed contracts: 1,817,594 confidence-bearing main-catalog rows (217 columns),
517,993 shifted-control rows (201 columns), and both published method scripts.
Snapshot `aea36fe5a6753de90be33301` passes clean typed reproduction. E4 build
`aaf262b1791d98ce3e9f96e7` retains 877,307 candidates and 239,406 negative
controls whose three-sigma parallax intervals overlap the 1,250-ly buffer, and
accounts for 1,218,874 excluded rows. `R_chance_align` is preserved as the
paper's KDE chance-alignment density ratio, not converted to a strict
probability or `1-R`. Every relation retains separate left/right Gaia EDR3
identity and binding scope. No row is accepted as canonical containment at this
stage. E5 relation artifact `c59bf6664db0b60960dc36a1` independently accounts
both endpoints, retains 95,045 fully bound `R_chance_align < 0.1`
high-confidence evidence rows and 3,043 fully bound shifted-sky negative
controls, and still emits no containment or hierarchy edge. The official Gaia
EDR3/DR3 source-list continuity is the only endpoint release bridge used.

## Gaia Release Identity Evidence

Gaia DR2 and DR3 source IDs are different release-scoped namespaces. Spacegate
uses the official `gaiadr3.dr2_neighbourhood` table and never compares those
values as interchangeable identifiers. The E2 target universe includes every
active DR2 fallback in NASA planet rows, targeted TIC, Cantat-Gaudin cluster
membership, the Gaia EDR3 white-dwarf catalog, and UltracoolSheet.

The forward collector preserves all official candidates for each targeted DR2
ID. The reverse collector then queries every distinct forward DR3 candidate so
that another DR2 predecessor outside Spacegate's initial target union cannot be
missed. Unique automatic reconciliation requires one candidate in both
directions; splits and merges remain ambiguous. Every compiled edge, outcome,
and source-family binding retains its registered source/release/table lineage.

Rebuild the registered typed index and identity graph with:

```bash
.venv/bin/python scripts/evidence_lake_store.py \
  --state-dir /data/spacegate/state cook \
  --report /data/spacegate/state/reports/evidence_lake_v2/e2_typed_cook_report.json
.venv/bin/python scripts/compile_evidence_identity_graph.py \
  --state-dir /data/spacegate/state
```

The graph reads the served CORE only as
`stability_reference_not_new_authority`. It cannot add objects, merge targets,
or mutate containment. Its atomic `current` pointer is an internal derived-
evidence pointer, not the public `served/current` build pointer.

This document defines active, optional, and transitional data sources for Spacegate.

It is normative for:

- downloader/cooker behavior
- source provenance requirements
- security/transport policy

## Directory Semantics

Within `$SPACEGATE_STATE_DIR`:

- `raw/`: immutable upstream snapshots
- `cooked/`: deterministic source-shaped typed outputs
- `out/<build_id>/`: immutable build artifacts
- `served/current`: promoted build pointer
- `reports/manifests/`: source retrieval manifests
- `reports/<build_id>/`: build QC/provenance reports

## Layer Terminology

Spacegate layer names:

- `core`: canonical immutable served astronomy inventory/projection
- `arm`: immutable supplemental science artifacts
- `disc`: reproducible derived artifacts
- `rim`: editable worldbuilding overlays

## Source Classification

Each source is classified as one of:

1. `canonical`:
   - defines canonical inventory fields
2. `auxiliary`:
   - enriches IDs, hierarchy, or confidence
3. `transitional`:
   - temporary migration support
4. `deferred`:
   - intentionally not in default ingest path

## Operational Source Policy Matrix

Interpretation note:

- this matrix reflects the Gaia-first production profile (`SPACEGATE_ENABLE_GAIA_BACKBONE=1`), not every legacy code path.

| Source family | Policy status | Default state | Primary toggle(s) | Why |
| --- | --- | --- | --- | --- |
| Gaia DR3 backbone (`gaia_source`) | mandatory | on in Gaia-first profile | `SPACEGATE_ENABLE_GAIA_BACKBONE=1` | canonical star inventory substrate |
| Sol authority bootstrap (`sol_authority`) | mandatory (S1/S2 release gate) | on | `SPACEGATE_ENABLE_SOL_AUTHORITY` | guarantees Sol/Sun/major-planet coverage plus arm moon/barycenter hierarchy from authoritative JPL source |
| Sol artificial overlay (`sol_artificial`) | default-on (`arm` overlay) | on | `SPACEGATE_ENABLE_SOL_ARTIFICIAL` | curated Sol stations/probes/orbiters with freshness windows for arm/UI overlays |
| NASA Exoplanet Archive (`ps` / `pscomppars`) | mandatory | on | (always in core catalog set) | canonical planet baseline; use `ps` for source-specific solutions and `pscomppars` for display/default composites |
| MSC | mandatory | on | `SPACEGATE_ENABLE_MSC` (must remain `1`) | required multiplicity hierarchy evidence; ingest blocks when off |
| WDS | mandatory (current default science ingest) | on | (always in Gaia-first core catalog set) | broad multiplicity support evidence |
| ORB6 | mandatory (current default science ingest) | on | (always in Gaia-first core catalog set) | orbit-quality support evidence |
| Gaia class probabilities | default-on | on | `SPACEGATE_ENABLE_GAIA_CLASSPROB` | remnant-safe classification guardrails |
| Gaia UCD memberships (`J/A+A/669/A139 table4`) | default-on | on | `SPACEGATE_ENABLE_GAIA_UCD` | ultracool dwarf cluster/membership tags (HMAC/BANYAN) for star enrichment |
| VSX variability index | default-on | on | `SPACEGATE_ENABLE_VSX` | variable-star evidence overlay in `arm` (exact Gaia joins only) |
| UltracoolSheet | default-on | on | `SPACEGATE_ENABLE_ULTRACOOLSHEET`, `SPACEGATE_ENABLE_NEARBY_ULTRACOOL_INVENTORY` | ultracool/youth/kinematics overlay in `arm`; narrow nearby core-inventory bridge when Gaia misses vetted UCDs |
| Gaia NSS | default-on | on | `SPACEGATE_ENABLE_GAIA_NSS` | Gaia-linked multiplicity evidence |
| SBX (ULB spectroscopic binaries) | default-on | on | `SPACEGATE_ENABLE_SBX` | spectroscopic-binary multiplicity evidence via exact Gaia/HIP/HD joins |
| SB9 (CDS `B/sb9`) | default-on ARM evidence | on | `SPACEGATE_ENABLE_SB9` | component-specific spectral types, aliases, and spectroscopic orbits; exact MSC sequence references bind evidence to graph endpoints |
| DEBCat | default-on | on | `SPACEGATE_ENABLE_ECLIPSING_CATALOGS` | eclipsing-binary enrichment/validation; unique canonical-system + period matches may bind component evidence in ARM |
| Kepler EB catalog | deferred (optional) | off | `SPACEGATE_ENABLE_KEPLER_EB` (with `SPACEGATE_ENABLE_ECLIPSING_CATALOGS=1`) | Kepler-era eclipsing support with low in-slice linkage in Gaia-first core |
| Compact-object bundle (`ATNF`, `magnetar`, `white_dwarf`) | default-on | on | `SPACEGATE_ENABLE_COMPACT_OBJECT_CATALOGS` | compact/remnant support evidence (includes cooked+ingested Gaia EDR3 white-dwarf catalog) |
| Superstellar bundle (`clusters`, `snr`) | default-on | on | `SPACEGATE_ENABLE_SUPERSTELLAR_CATALOGS` | open-cluster and remnant-nebula context |
| Extended-object bundle (OpenNGC, LBN, LDN, Barnard, Magakian, vdB, Sharpless, Cederblad) | default-on | on | `SPACEGATE_ENABLE_EXTENDED_OBJECTS` | separate catalog identity, geometry, distance, and evidence for nebulae, clusters, galaxies, and other non-stellar landmarks; see `docs/EXTENDED_OBJECTS.md` |
| Exoplanet lifecycle support (`exoplanet.eu`, OEC, HWC) | optional | off | `SPACEGATE_ENABLE_EXOPLANET_LIFECYCLE_CATALOGS` | status overlays and derived-tag support; OEC alias bridge improves lifecycle matching; canonical planets stay NASA-rooted |
| WDS↔Gaia bridge (`wds_gaia_xmatch`) | optional | off | `SPACEGATE_ENABLE_WDS_GAIA_XMATCH` | useful for bridge experiments, but confidence-gated and conservative by default |
| Proximity grouping | optional runtime behavior | off | `SPACEGATE_ENABLE_PROXIMITY` | nondefault to avoid weak/inexact grouping in production |
| AT-HYG | transitional | alias crosswalk on by default; supplement merge opt-in | `SPACEGATE_ENABLE_ATHYG_ALIAS_CROSSWALK`, `SPACEGATE_ENABLE_ATHYG_SUPPLEMENT_MERGE` | migration compatibility for names/legacy IDs; not canonical inventory authority |
| BDB/ILB-like non-mirrored sources | deferred/disregarded for default ingest | off | n/a | high-risk dependency until mirrored + integrity-pinned |
| TESS EB catalog (Villanova) | default-on | on | `SPACEGATE_ENABLE_TESS_EB` | eclipsing/variability expansion beyond Kepler with deterministic paginated export capture |
| NASA TESS Objects of Interest + targeted TIC/MAST/Gaia metadata | default-on identity + `arm` evidence | on | `SPACEGATE_ENABLE_TESS_EVIDENCE` | exact TIC/TOI lookup, missing-object audit, candidate/transit evidence, and targeted Gaia reconciliation without bulk TIC ingestion; see `docs/TESS_INTEGRATION.md` |

Evidence Lake E4 source checkpoints:

- Gaia DR3 source `ab7f7e6bc211bee146885987`: all 32,176,271 buffered
  source rows, 32,176,271 release-scoped identifiers, and one compact coherent
  source solution per row. The two 125-field schemas preserve astrometry,
  photometry, radial velocity, classification/membership, and product
  availability; copied GSP-Phot projections remain E1-preserved exclusions in
  favor of the richer AP tables. Source and generic artifact audits pass, while
  clean-state reproduction remains in progress
- WDS and CDS WDS-Gaia `ad98d4e369c5a0addc6477a0`: all 157,476 WDS
  summary/method rows, 140,416 positional-match rows, and 43 field occurrences;
  WDS-qualified pair identities, bounded relative-astrometry measurements,
  opaque spectral text, and 140,416 candidate matches with angular-distance
  statistics, zero strict probabilities, and no canonical promotion
- MSC `fc7e9dcabb0b27167c8f188c`: all 43,418 rows/73 field occurrences;
  WDS-qualified component identities, 15,748 polarity-bearing source relation
  claims, 19,366 coherent orbit records, scoped classifications/physics,
  explicit zero-is-unknown semantics, and no canonical binding or heuristic
  parsing of alias lists or orbit pair labels
- OpenNGC and constituent nebula catalogs `54d1b0b6a841344c48327991`:
  19,012 typed object rows, 856 ReadMe lines, all 238 fields, 21,107 exact
  catalog identities, and no heuristic splitting of list-valued aliases
- Hunt/Reffert 2024 `7e66e0690aa962c837d43a86`: 465 uncertainty-overlap
  clusters, 51,017 probability-bearing memberships, 451 literature
  crossmatches, all 161 fields, and zero canonical containment promotion
- GCVS `a6f6669d2bd48eac5d6204d2`: all 340,839 rows from six release
  tables, 705,684 scoped identity claims, typed astrometry, distinct variable
  and stellar classifications, deterministic bibliography aggregation, and no
  canonical binding
- Green SNR `d08c5aa9af7dc8bcdbf0d6c3`: 310 rows/15 fields with
  deterministic Galactic identifiers, geometry, uncertain flux/index strings,
  aliases, and detail lineage
- Villanova TESS EB `255678b2daa6e8bf46e6dcd9`: 17,605 rows/20 fields with
  normalized TIC identity, positive/negative catalog membership, sector,
  morphology, flag, Tmag, astrometry, target-context physics, and 4,584
  positive-member orbit solutions
- Targeted NASA/MAST/Gaia TIC/TOI `03acb9eb0fb2cbc0f8203dd8`: 122,772 rows/239
  fields, 27,930 exact targeted TIC records, 8,064 TOIs, 29,302 official
  DR2-to-DR3 neighborhood rows, 137 member-qualified external best neighbors,
  and 29,409 targeted Gaia DR3 rows. All 524 nonblank TIC dispositions remain
  row-inspectable. E5 artifact `86aa5553053db35d81ff26e0` gives every target
  and TOI an explicit outcome while leaving canonical planet inventory unchanged.
- Gaia EDR3 White Dwarf `486e4975af015d4e5f5a3c9b`: 337,272 posterior-
  interval-overlap candidates from 1,280,266 raw rows, 161 fields accounted,
  and independent H/He/mixed atmosphere parameter sets. E5 policy v6 applies
  the [Gentile Fusillo et al. (2021)](https://doi.org/10.1093/mnras/stab2672)
  `Pwd > 0.75` general-purpose threshold and chooses one complete model by
  minimum published fit chi-square without deleting alternatives
- ATNF Pulsar Catalogue `64c55c19a5a10a88877d4cd2`: 190,671 package rows,
  91,858 parameter/glitch contexts, 97,424 identifier claims, 1,210 full source
  references, and 84,388 exact citation links; unmatched lexical reference
  tokens remain source-native evidence rather than placeholder bibliography.
  The E5 compact-scope audit finds no distinct safe canonical pulsar leaf; its
  sole exact current route, J0437-4715, is quarantined as an unresolved
  pulsar/optical-companion collision rather than selected by name or position.
  E5 compact build `f0d7273f65371efeda365611` resolves the 4,482 source-native
  names through ATNF's own PSRJ claims into 4,394 release-scoped physical
  identities while preserving all 97,516 identifier claims. Twenty-two source
  parallax intervals overlap the 1,250-ly evidence envelope
- McGill Magnetar Catalogue `99c17afd7461a9a6972a9348`: all 31 catalog rows
  and 139 separate timing, X-ray, distance, position, and source-context
  parameter sets, now joined only through exact source codes to the pinned
  publisher HTML and CDS bibliography. The release retains 97 exact external
  reference-code URLs, 208 current-object bibliography links, and all 215 CDS
  references; four historical shorthand codes remain explicitly unresolved
  rather than receiving guessed citations. No exact current canonical magnetar
  leaf exists. E5 assigns all 31 rows permanent release-scoped magnetar
  identities; none has source distance evidence overlapping the current
  envelope, so no canonical stellar leaf or public inventory row is invented
- SB9 `72663823963198c8fcbbe569`: all 30,153 ReadMe/system/alias/orbit rows and
  62 table-column occurrences; 4,079 primary/secondary binary claims, 5,099
  linked orbit solutions, 4,079 component spectra, 4,403 component magnitudes,
  release-distinct Gaia DR2/DR3 aliases, and 1,807 recognized ADS bibcodes
- SBX `37ffa7255d026c8d930af6d4`: complete unbounded source snapshot with 4,080
  systems, 102,459 aliases, 261 configurations, 5,169 full orbit rows, and all
  73 table-column occurrences; 4,080 scoped binary claims, 94 hierarchy claims,
  5,169 linked orbit solutions, 3,550 component classifications, 4,498
  component magnitudes, and 20,152 astrometric measurements

### Exoplanet Lifecycle Notes

- `exoplanet.eu`: independently pinned and typed status evidence; the current
  release contributes 8,261 positive confirmed assertions and no candidate or
  negative lifecycle rows. It does not supersede NASA authority.
- `HWC`: independently pinned habitability feature evidence. Its 5,599 rows
  include 29 conservative and 41 optimistic habitable flags, but the E4 adapter
  emits zero lifecycle assertions and cannot confirm a planet.
- `OEC`: independently pinned at Git commit
  `18fb506ab3a4bb857b453486993bab797a33c5c0`. Its archive-member plus local XML
  node path is the source object identity. E4 preserves 5,287 confirmed, 3,844
  candidate, 100 controversial, 12 retracted negative, and 10 other ambiguous
  lifecycle rows alongside 160,582 fully accounted parameters and 16,750
  relations. It improves alias/crosswalk coverage but cannot alter canonical
  planet counts before E5/E6 review.
- `EMAC TT9`: removed from active ingest pipeline because current endpoint is a resource page without deterministic bulk candidate rows.

### 2026 Source-Refresh Watchlist

- Gaia DR3 remains the current Spacegate backbone; Gaia DR4 is scheduled for
  December 2, 2026 and has an explicit release-scoped transition contract in
  `docs/E7_CUTOVER_AND_GAIA_DR4.md` and
  `config/evidence_lake/gaia_dr4_adapter_plan.json`. DR3 and DR4 source IDs are
  distinct namespaces joined only through the official neighborhood/crossmatch;
  neither release owns permanent Spacegate object identity.
- MSC is mandatory hierarchy evidence. Spacegate now targets the upstream June
  19, 2026 archive (`newmsc-20260619.tar.gz`) and verifies insecure fallback
  downloads with an explicit SHA-256 pin when CTIO TLS is not usable. Local
  canonical build `20260628T1210Z_msc20260619` was promoted on June 28, 2026
  after passing required multiplicity golden checks. Local bootstrap mirror
  snapshot `20260628T1210Z_msc20260619` now contains refreshed MSC raw and
  cooked artifacts; sync this mirror to `spacegates.org` during the next public
  deployment.
- SBX and SB9 are complementary independent releases. The legacy SBX core
  projection retained only ten system columns and aggregate orbit counts; the
  Evidence Lake profile now acquires the complete small catalog, including both
  component spectral types, magnitudes/bands, uncertainties, references,
  configurations, every alias family, and full orbit solutions. SB9 remains a
  separately published historical/current orbit and component evidence source.
  Neither catalog creates canonical component inventory by itself.
- A bounded canonical recovery path may use an exact unique HIP+HD agreement
  between SBX and the transitional AT-HYG crosswalk for Gaia-missing systems,
  but only with no Gaia identifier on the source rows, usable distance,
  positional sanity, and SBX orbit evidence. This is a reusable catalog rule;
  it does not make AT-HYG a canonical inventory authority and it cannot accept
  named-system exceptions.
- WDS and ORB6 remain default visual-binary support sources, but ORB6 rows must
  only attach to unique, confidence-gated binary edges.
- JPL Horizons/SBDB remain the Sol-system orbital authority path for volatile
  small-body and satellite data.

## Mandatory Retrieval Metadata

All downloader-manifest entries must include:

- `source_name`
- `url`
- `dest_path`
- `retrieved_at`
- `checked_at`
- `bytes_written`
- `sha256` and/or integrity equivalent (etag/retrieval tag)

Gaia TAP extracts (`gaia_backbone`, `gaia_classprob`, `gaia_nss`) additionally persist:

- `count_query`
- `expected_row_count`
- `row_count_match`

`row_count_match=false` indicates potential partial/truncated retrieval and should be treated as a build blocker.

## Core Canonical Sources

## 1) Gaia DR3 (`gaia_source`)

Classification: `canonical`

Role:

- canonical star inventory substrate
- canonical astrometry and photometry fields
- arm-side source-native stellar-parameter enrichment (`stellar_parameters`) for narration, filters, and uncertainty-aware inference

Required policy:

- `<1000 ly` boundary from parallax policy
- explicit quality tiers (`parallax_over_error`, `ruwe`, etc.)
- canonical epoch/frame metadata in `build_metadata`

Source endpoint:

- ESA Gaia Archive TAP
- `https://gea.esac.esa.int/tap-server/tap/sync`

## 2) NASA Exoplanet Archive (`pscomppars`)

Classification: `canonical` (for current confirmed exoplanet layer)

Role:

- planet records and planetary parameters
- host matching against canonical stars/systems
- arm-side host-star physical parameter rows for narration/evidence display when matched back to canonical stars
- broad map planet bins can use the selected `pscomppars` radius (`pl_rade`),
  best mass (`pl_bmasse`/`pl_bmassj`), equilibrium temperature (`pl_eqt`),
  insolation (`pl_insol`), and semimajor axis (`pl_orbsmax`). The detailed `ps`
  rows remain the source-specific literature solutions needed to distinguish
  direct measurements, estimates, limits, uncertainties, and competing values;
  a populated composite field must not be narrated as a direct measurement
  without inspecting that evidence.
- CORE currently promotes `pl_masse`/`pl_massj`, not the broader best-mass
  `pl_bmasse`/`pl_bmassj` pair. The latter must travel with `pl_bmassprov`: the
  current cooked snapshot includes true `Mass`, `Msini`, `Msin(i)/sin(i)`, and
  `M-R relationship` values. A future classifier may use these as typed
  evidence, but must not collapse lower bounds and mass-radius estimates into a
  measured canonical mass.

Source endpoint:

- `https://exoplanetarchive.ipac.caltech.edu/TAP/sync?...`

## 2b) Sol authority bootstrap (`sol_authority`)

Classification: `canonical` (Sol-specific authoritative override layer)

Role:

- ensure Sol and Sun are always present and linked
- ensure required Sol major-planet coverage independent of exoplanet catalogs
- provide S2 moon hierarchy/orbit/barycenter evidence in `arm`
- provide S3 named small-body evidence in `arm` (asteroid/TNO/comet families with staleness metadata)
- provide deterministic Sol provenance for release gating

Source endpoint:

- JPL Horizons API: `https://ssd.jpl.nasa.gov/api/horizons.api`

Implementation:

- downloader: `scripts/fetch_sol_authority.py`
- immutable acquisition helper: `scripts/horizons_snapshot.py`
- contract doc: `docs/SOL_AUTHORITY.md`

Evidence Lake collection preserves each exact Horizons response body, exact
query parameters and URL, response checksum/size, the reviewed operator target
seed and collector checksum, and the parsed CSV projection in one immutable
content-addressed snapshot. The legacy `raw/sol_authority` CSV is an atomic
compatibility projection; it is not the sole durable source artifact.
Parsed projections preserve both the exact Horizons `center_code` and its
syntax-derived `center_target_command`; later relation binding must prefer that
source identifier over the operator seed's human-readable parent name.
The production-shaped Evidence Lake preview types both the parsed table and
response index for all 60 natural targets and passes raw/typed accounting,
source-response integrity, and clean reproduction. The checked-in E4 adapter
uses parsed Horizons target/center commands as JPL-scoped identities, retains
reviewed operator keys/names in separate seed namespaces, and materializes each
exact response as observation-product lineage. The shared parser maps the source
header and preserves all 12 numeric `ELEMENTS` columns plus TDB calendar context;
schema drift fails closed. Combined natural/artificial E4 build
`b4edc4ea6eccba69794a92df` accounts all 85 registered fields and passes source,
artifact, complete-element, and clean-reproduction gates.

E5 natural-object artifact `d61c6890588ee40c46ea7d56` binds all 60 reviewed
targets through exact source keys or canonical JPL-command metadata. It resolves
59 target/physical-center pairs and retains command `0` as the non-object Solar
System barycenter reference origin. All 60 solutions keep their exact TDB epoch,
ICRF/ecliptic/AU-D frame, model, method, query/response, and complete 12-element
lineage. Thirty-six physical sets expose 36 radii and 20 masses on exact targets.
No source relation is promoted into canonical hierarchy at E5.

E7 permanent Solar identity build `9f9f8ef5e690d3390d36f482` removes the E5
selector's former stability-ARM component-binding dependency. It consumes the
same pinned E4 evidence plus clean CORE, binds the Sun and thirteen canonical
planets through explicit permanent-key contracts, and assigns stable numeric
operator-seed identities to 57 moons, minor bodies, and artificial objects.
Legacy name-derived component keys remain identifier crosswalks only. All 71
JPL target/center relation outcomes are preserved with zero canonical-containment
promotions. Runtime orbit and physical selection must consume this identity
artifact rather than the old stability-bound target table.

Selected runtime artifact `0f1ac54fb1d4e9b71472abab` now performs that clean
join for both natural and artificial targets. It preserves 68 complete periodic
solutions, three complete period-null hyperbolic escape trajectories, one
barycenter reference context, and 36 natural-body physical parameter sets.
Periodic renderability and hyperbolic trajectory eligibility remain distinct;
the selector does not invent a period for an unbound path.

Clean runtime ARM build `376285dd79d73a52972d74fd` consumes this selected
artifact and the permanent identity artifact without opening a stability
database. It projects 57 ARM-only Solar identities, 57 non-containment graph
relations, 70 accepted runtime orbit/solution rows, 35 minor-body compatibility
rows, and 11 artificial-object compatibility rows. The selected barycenter
reference context remains evidence-only. All component endpoints resolve, no
source relation becomes canonical containment, and isolated logical reproduction
passes.

Cross-source stellar-orbit artifact `131d37fd3cdaa3867ff8337b` consumes the
audited E5 component-scope product without reading a stability database. ORB6
is authoritative for complete published visual geometry, with MSC compiled
visual solutions as the simulation fallback. SB9 spectroscopic solutions,
DEBCat eclipsing periods, and MSC elementary relation periods remain coherent
context rather than donating individual fields to a composite. The artifact
accounts 17,170 eligible solutions and selects one physically valid visual set
for 1,959 of 11,250 exact endpoint relations. Positive period/axis and bounded
eccentricity are required; source placeholders cannot qualify through non-null
tests. SBX, Gaia NSS, and TESS EB deferrals
remain machine-readable and create no runtime relation or containment.

Endpoint artifact `f57b4dfc9f554072bc41fe5d` then reconciles the selected MSC
source endpoints against the clean runtime hierarchy. Exact WDS scope and a
label unique before and after casefolding are mandatory; names and coordinates
are not matching evidence. It accepts 7,643 of 22,396 endpoints and makes 3,238
relations, including 808 preferred visual solutions, eligible for ARM. The
14,318 missing leaves and 435 case-significant collisions remain explicit and
cannot create runtime components or edges.

Clean ARM `34069ba67abe3b4331c26adc` consumes both immutable artifacts. It
retains all selected and deferred evidence tables while projecting 3,238 bound
relations and 6,518 coherent solutions into compatibility orbit tables. It
creates no stellar identities or containment from catalog relation claims.

Clean TESS runtime artifact `ab880f46a111428e8021e47e` combines the selected
TESS projection with exact E4 source-native measurements and provenance, then
rebinds all stable keys against clean CORE. The existing append-only TOI history
CSV is a checksum-pinned migration seed only; it supplies first/last-observed
timestamps and is not identity or disposition authority. The artifact preserves
all current candidates and negative controls while permitting canonical planet
IDs only on the 824 confirmed/known exact clean-key links.

ARM v4 `e3e82312eaa3cab931e9e756` is the first clean runtime ARM to carry the
four TESS compatibility tables. It copies the accepted artifact byte content
into ARM without independent disposition logic, retains the full unresolved
tail, and preserves the public TIC/TOI consumer contract without contaminating
canonical planet counts.

## 2c) Sol artificial overlay (`sol_artificial`)

Classification: `auxiliary` (`arm` supplemental science overlay)

Role:

- provide curated Sol artificial-object rows (station/probe/orbiter classes) in `arm`
- support Sol hierarchy/UI evidence without polluting `core` canonical inventory tables
- attach explicit freshness windows + staleness fields for volatile-feed monitoring

Source endpoint:

- JPL Horizons API: `https://ssd.jpl.nasa.gov/api/horizons.api`

Implementation:

- downloader: `scripts/fetch_sol_artificial.py`
- immutable acquisition helper: `scripts/horizons_snapshot.py`
- volatile refresh runbook: `scripts/refresh_sol_volatile.sh`
- freshness monitor: `scripts/report_sol_volatile.py`

Artificial-object snapshots use the same raw-response contract. Per-row
freshness windows remain applicability evidence; an expired trajectory is not
silently treated as a current orbit merely because its catalog snapshot still
falls within the broader feed refresh window.
The corresponding 11-target preview passes the same gates independently from
the natural-object source. Its E4 trajectory relations use only parsed JPL
target and center commands; human-readable operator parent names remain source
context and cannot create a relation endpoint.

## 3) Gaia DR3 astrophysical classifier probabilities

Classification: `canonical` (classification safety support for canonical stars)

Role:

- remnant-safe classification support (`classprob_dsc_*_whitedwarf` and related families)
- prevents temperature fallback from mislabeling remnant objects

Source endpoint:

- ESA Gaia Archive TAP (`gaiadr3.astrophysical_parameters` and
  `gaiadr3.astrophysical_parameters_supp`)
- official supplementary-table contract:
  `https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_astrophysical_parameter_tables/ssec_dm_astrophysical_parameters_supp.html`

July 17, 2026 utilization audit:

- the current extract selects only DSC probability columns even though the same
  source table contains FLAME luminosity/radius/mass/age, evolutionary stage,
  extinction, activity, H-alpha, spectral-type, uncertainty, and quality fields
- at the current outer parallax boundary, official TAP count queries return
  3,428,436 rows with FLAME luminosity/radius, 1,136,048 with FLAME mass, and
  1,026,163 with FLAME age
- E5 policy `2026-07-21.e5-selection.8` treats the supplementary table as
  alternatives and fallback evidence, not as a second copy of the main AP
  result. The main table remains authoritative for the publisher-selected
  GSP-Phot library. Supplementary ANN atmosphere/alpha values require the
  official best-quality `flags_gspspec_ann < 10000` criterion and rank behind
  primary GSP-Spec; spectroscopic FLAME ranks behind primary photometric FLAME.
  Library-specific GSP-Phot parameters, distances, and extinction remain
  coherent evidence-only alternatives with explicit channel dispositions.
- acquire a narrow pinned physical-parameter v2 extract and keep those modeled
  source values in ARM; do not silently promote them into CORE facts
- full findings and the one-rebuild plan are in
  `docs/SOURCE_CATALOG_UTILIZATION_AUDIT_2026-07-17.md`

## 3b) Gaia ultracool dwarf memberships (`J/A+A/669/A139`, `table4`)

Classification: `auxiliary`

Role:

- published Gaia DR3 ultracool-dwarf sample identity
- HMAC unsupervised cluster assignments and BANYAN best-hypothesis membership
  probabilities (`HMACcl`, `BANYANcl`, `BANYANprob`)
- evidence-only enrichment; does not define canonical star existence
- this table contains no spectral-type field; UCD sample membership cannot by
  itself become a selected stellar classification

Source endpoint:

- CDS HTTPS mirror (`https://cdsarc.cds.unistra.fr/ftp/J/A+A/669/A139/table4.dat`)

Evidence Lake v2 checkpoint:

- raw snapshot `d1be498af5b1dfe7964c3891`; typed snapshot
  `60f97d02344bdd773438fac2`
- E4 build `78016b90e02689547c3f53dd` preserves 7,630 catalog rows and
  93 ReadMe lines, with separate HMAC hard-assignment and BANYAN probability
  semantics; all bindings remain unresolved for E5

## Core Auxiliary Multiplicity Sources

## 4) Gaia DR3 NSS support extracts

Classification: `auxiliary`

Role:

- star-level multiplicity evidence
- hierarchy confidence support

Datasets:

- `non_single_star`
- `nss_two_body_orbit`

Evidence Lake status:

- E5 artifact `f5358c0a0983958e5d4f76c5` accounts all 87,075 coherent NSS
  source/model solutions. Exact Gaia DR3 identity anchors 56,617 rows on 54,794
  current canonical observation targets; 30,458 rows remain outside the current
  canonical reference and none is ambiguous.
- Complete fitted values/errors, correlation vectors, diagnostics, models,
  frames, and references remain one solution context. NSS does not expose
  inspectable component endpoints, so every row requires relation adjudication
  and none creates a companion, containment edge, relation, selected scalar, or
  simulation-ready orbit.

Source endpoint:

- ESA Gaia Archive TAP

## 5) WDS (Washington Double Star)

Classification: `auxiliary`

Role:

- broad multiplicity evidence and grouping support
- source-native first/last relative astrometry, observation counts, magnitudes,
  proper-motion context, spectral text, discoverer designations, and note flags

Policy:

- WDS-based grouping from bridge paths is confidence-gated
- default production path keeps conservative thresholds
- the Evidence Lake adapter never treats bare `A`, `AB`, `Aa,Ab`, or similar
  labels as globally usable identities; pair keys are qualified by WDS identity
- source spectral text remains opaque because the published field may describe
  component A or two components; E2/E5 must resolve scope before selection
- documented/source-observed sentinels and invalid angle domains remain in raw
  and typed rows but cannot become normalized measurements
- the CDS best-match bridge is candidate positional evidence within 2 arcseconds,
  not a probability, authoritative crossmatch, physical relation, or orbit

Evidence Lake status:

- E5 artifact `33f2a90275378a35be21a704` accounts every one of the 157,299
  visual-pair summaries. The bounded documented parser resolves 5,282 rows to
  one exact WDS-qualified accepted MSC relation; 152,016 lack that relation and
  one is ambiguous.
- The 4,303 relation-bound spectral strings remain opaque pair context. The
  10,388 relation-bound component magnitudes remain contextual because WDS does
  not provide one consistent bandpass, and 57,572 relation-bound astrometry,
  epoch, position, and source-convention proper-motion rows remain contextual
  without unit reinterpretation.
- The E5 projection does not use the CDS positional candidate as identity and
  creates no canonical component, containment, hierarchy, or selected scalar.

Source endpoint:

- USNO/GSU WDS published data

## 6) ORB6

Classification: `auxiliary`

Role:

- orbit-quality support evidence for multiplicity confidence

Evidence Lake status:

- the legacy served build normalizes only 56 rows through a unique binary-edge
  shortcut; Evidence Lake does not reuse that unsafe system-level rule
- Evidence Lake E4 checkpoint `fcbb6466bea0a7798ae8d2ed` now preserves all
  4,051 source rows and 37 fields as coherent visual-orbit evidence with
  WDS/discoverer/ADS/HD/HIP claims. E5 artifact
  `6def85dff374034cfe125b6b` accounts every row through exact WDS combined-pair
  matching and WDS-qualified MSC endpoints. It makes 1,159 solutions eligible,
  retains 646 missing WDS pairs and 2,246 missing MSC relations, and promotes no
  canonical containment or hierarchy.

Source endpoint:

- USNO ORB6 export

## 7) DEBCat (Detached Eclipsing Binary Catalogue)

Classification: `auxiliary`

Role:

- high-quality detached eclipsing binary physical parameter table
- benchmark and enrichment support for orbital/mass/radius validation
- accepted unique component bindings should project source mass, radius, Teff,
  luminosity, log-g, and metallicity into shared ARM stellar evidence rather
  than remaining only in the source-specific eclipsing-binary table

Evidence Lake status:

- checkpoint `b3a141c0caf953aa83c4e52b` preserves all 374 rows/30 fields and
  materializes separate primary/secondary physics and classifications,
  system-scoped metallicity and integrated photometry, and binary period
  solutions
- `-9.99`, `-1.00`, and `none` are explicit source missing sentinels; they stay
  visible in source-native rows but are not measurements or classifications
- E5 component-scope artifact `f5358c0a0983958e5d4f76c5` accounts all 374
  systems through exact priority-aware name resolution. Twenty systems bind to
  one accepted WDS/MSC relation within `max(0.01 day, 1%)`; 337 systems remain
  outside the canonical reference and 17 resolved systems have no compatible
  relation. The accepted projection makes 216 parameter measurements, 32
  classifications, 74 system-integrated photometry rows, and 20 period
  solutions eligible for later global selection. Eligibility does not make a
  source value the selected public winner.

Source endpoint:

- https://www.astro.keele.ac.uk/jkt/debcat/debs.dat

## 8) Kepler Eclipsing Binary Catalog

Classification: `deferred` (optional)

Role:

- large eclipsing-binary candidate/phenomenology dataset (period, morphology, KIC IDs)
- supplementary evidence set for binary behavior and follow-up crossmatching

Policy:

- default-off in Gaia-first production profile
- opt-in only when explicit Kepler-focused analysis is needed (`SPACEGATE_ENABLE_KEPLER_EB=1`)
- rationale: current Gaia-slice overlap/linkage is low, so default ingest cost is not justified

Source endpoint:

- https://keplerebs.villanova.edu/ (CSV export workflow)

## 8A) SBX (ULB Spectroscopic Binary Catalog)

Classification: `auxiliary`

Evidence Lake status:

- Combined component artifact `f5358c0a0983958e5d4f76c5` accounts all 4,080
  SBX systems and all 8,160 release-scoped primary/secondary identities. Exact
  Gaia DR3, officially reconciled Gaia DR2, HIP, HD, and TIC claims resolve
  2,354 systems uniquely; 1,699 are absent from the current canonical reference
  and 27 retain conflicting system candidates.
- The accepted projection exposes 2,561 component magnitudes, 2,208 component
  classifications, and 3,043 published spectroscopic-orbit solutions for later
  quantity-specific selection. It does not equate the observation target with
  the primary component.
- All 20,152 SBX astrometry rows remain observation-target or photocenter
  context. The 94 source hierarchy claims retain independently resolved
  endpoints and cannot create CORE containment or canonical ARM hierarchy.

## 9) MSC (Tokovinin Multiple Star Catalog)

Classification: `auxiliary` (mandatory)

Role:

- explicit hierarchy candidate source

Policy:

- required in default science ingest and multiplicity derivation
- ingest/promotion fail if MSC retrieval/cook/manifest lineage is missing
- still quantify contribution in comparison reports for observability
- E4 preserves the release independently from legacy ARM cooking. Local
  component labels are never global identifiers: the typed evidence key is
  WDS plus the exact, case-significant source label, while raw `Primary`,
  `Secondary`, `Parent`,
  and `System` strings remain inspectable. `Type` status determines evidence
  polarity but does not accept containment, and source numeric zero, including
  signed zero, is unknown per the release ReadMe.
- E4 checkpoint `fc7e9dcabb0b27167c8f188c` passes complete row/field,
  component-scope, relation-polarity, orbit, citation, zero-sentinel, artifact,
  and clean-reproduction gates. E5 component-scope artifact
  `f5358c0a0983958e5d4f76c5` now accounts 6,937 WDS systems, 32,790 source
  component identities, and 15,748 relations. It anchors 24,671 components and
  accepts 12,052 two-endpoint relation-evidence rows while retaining 8,119
  missing components, 3,693 unresolved relations, and three invalid source
  self-relations. It also projects 44,130 selectable mass/apparent-V facts,
  16,182 classifications, 54,902 photometry rows, 62,214 astrometry/motion rows,
  and 14,939 exact relation-bound orbit solutions while keeping relative
  separations as context-only evidence.
- E5 component policy v9 preserves distinctions such as subsystem `AB` versus
  terminal star `Ab` and rejects duplicate accepted WDS/label keys. It removes
  238 case-fold collision groups without a named-system rule. Cross-table
  notation matching may ignore case only after proving a single compatible
  relation and recording the outcome.

Security/transport note:

- historical retrieval context requires explicit caution
- preferred mirror path, when published on the public bootstrap host, is `SPACEGATE_PUBLIC_BASE_URL/dl/catalogs/current/raw/msc/newmsc-20260619.tar.gz`
- default code should support overriding MSC retrieval to that mirror without changing source provenance
- preserve the CTIO/NOIRLab MSC export URL as authoritative source provenance
- maintain mirrored/pinned retrieval strategy for production stability
## 10) VSX (AAVSO Variable Star Index)

Classification: `auxiliary`

Role:

- variable-star observational overlay (type/family/amplitude/period)
- confidence-tiered variability summaries for narrative and query filtering
- stored in `arm` to preserve core query performance

Source endpoint:

- CDS HTTPS mirror (`https://cdsarc.cds.unistra.fr/ftp/B/vsx/vsx.dat`)
- source schema/method document (`https://cdsarc.cds.unistra.fr/ftp/B/vsx/ReadMe`)
- historical OID-to-bibcode relation
  (`https://cdsarc.cds.unistra.fr/ftp/B/vsx/refs.dat.gz`)

The bibliography relation is source-published and exact but incomplete for the
current rolling object table: the server artifact was last modified in 2022,
contains 830,415 links for 586,530 distinct OIDs, and reaches OID 683,950,
whereas the pinned 2026 object table contains 10,304,568 OIDs. Spacegate keeps
that lineage as a partial historical relation and reports uncovered current
rows; it must not imply that absence from `refs.dat` means no publication exists.

The July 21 release updates the object inventory to 10,304,607 rows and pins the
bibliography in the same three-artifact Evidence Lake release. The complete
source audit passes with 2,080 bibliography links whose 1,833 distinct
historical OIDs are absent from the current object table. E4's stricter
structural ADS validation preserves 56 links across 9 distinct noncanonical
reference strings verbatim and never fabricates their URLs. Clean typed
reproduction passes. The release delta reports 47 added OIDs, 8 removed OIDs,
and 243 scientifically changed retained rows; line-number shifts caused by the
rolling export are lineage changes only.

The checked-in registry now selects this release. E4 build
`d9780b76333132c0a05098b7` materializes exact OID/name/Gaia DR3 claims,
coordinates, source spectral classifications, one coherent 16-field
variability record per object, and exact object bibliography links. E5 policy
v9 resolves only the 226,017 rows carrying a unique current Gaia DR3 binding
and selects their source-native variability class plus 22,695 usable periods.
The remaining 10,078,590 rows stay explicit missing outcomes. Spectral strings,
photometric extrema and context, and bibliography remain evidence-only; neither
E4 nor E5 creates canonical variable stars or promotes names merely because VSX
publishes them.

## 11) UltracoolSheet

Classification: `auxiliary`

Role:

- ultracool object metadata and youth indicators
- Gaia DR3/DR2-linked arm overlay for detailed UCD context
- supports later disc enrichment without widening core hot-path tables
- narrow accepted-inventory completeness bridge for nearby rows missing from
  the Gaia backbone. Controlled by
  `SPACEGATE_ENABLE_NEARBY_ULTRACOOL_INVENTORY` and
  `SPACEGATE_NEARBY_ULTRACOOL_INVENTORY_MAX_DIST_PC` (default 10 pc).
- examples of intended coverage: WISE 0855-0714 and Luhman 16. This bridge
  admits vetted nearby ultracool objects as `core.stars` with
  `source_catalog = 'ultracoolsheet'`; it does not expand composite or binary
  ultracool rows into invented component hierarchies.
- limitation: this is not a full WISE/CatWISE/AllWISE survey ingest. Large
  infrared survey integration remains a separate performance-planned milestone.
  See `docs/CATWISE_ALLWISE_PLAN.md`.

Source endpoint:

- Google Sheets published CSV endpoint (pinned URL in `scripts/catalogs.sh`)

Evidence Lake v2 checkpoint:

- raw snapshot `14fd785307af12849666a603`; typed snapshot
  `32d437d41bfdfa7242bd8a4a`; E4 build `a328a9e13d6c2b44f8d57861`
- all 242 fields are accounted. Direct optical/infrared spectral and gravity
  classifications remain distinct from maintainer numeric encodings, selected
  astrometry, propagated positions, and photometric-distance formulas
- 23 Pan-STARRS1/Gaia/2MASS/MKO/WISE/Spitzer bandpasses retain values,
  uncertainties, references, and available quality context; literal `nan`,
  `null`, and negative Pan-STARRS uncertainty sentinels cannot normalize
- Gaia DR2 and DR3 IDs remain release-distinct. `astrom_Gaia=O` identifies
  object-owned Gaia astrometry; `astrom_Gaia=P` identifies a higher-mass
  primary used as the companion row's astrometric proxy. The latter is typed
  as `associated_primary_astrometric_proxy` and cannot bind object-scoped
  companion classifications to the primary. Pipe-delimited SIMBAD aliases are
  retained exactly but not split by an undocumented parser
- multiplicity and exoplanet flags remain source context until an endpoint- and
  scope-safe relation/lifecycle adapter exists; they do not alter CORE inventory
- E5 policy v15 independently gates all 10,887 populated classification
  subjects against both claim scope and source astrometry ownership: 4,843 are
  accepted, 512 proxy-scoped subjects are excluded, and 5,532 are missing from
  the current canonical identity graph. All 4,843 accepted object-owned facts
  are selected with exact lineage in verified build
  `fa4aaed18aebcffb8632d978`
- E7 modular selector `3f645ac3de3323637ded93d5` additionally reconciles the
  exact release-scoped `ultracoolsheet_name` namespace for permanent
  source-native inventory objects that lack a usable Gaia binding. All 76
  targeted identifiers end 51 accepted or 25 missing-current-release; accepted
  bindings expose 10 optical and 50 infrared direct classifications for 51
  stars. No generic display-name match, row-number equivalence, identity
  creation, or containment creation is allowed.

Core bridge diagnostics:

- `reports/<build_id>/nearby_ultracool_inventory_report.json`
- `scripts/verify_nearby_ultracool_inventory.py --build-dir <out/build_id>`

## 12) TESS Identity and TOI Evidence

Classification: `core identity authority` plus `arm evidence`

Default toggle: `SPACEGATE_ENABLE_TESS_EVIDENCE=1`

Authoritative inputs:

- NASA Exoplanet Archive `toi` TAP table
- targeted MAST TIC rows for TOI hosts, NASA planet hosts, TESS EB targets, and
  reviewed operator/AAA seeds
- Gaia DR3 `dr2_neighbourhood`, targeted `gaia_source` rows, and targeted
  Hipparcos/Tycho-2/2MASS best-neighbor crossmatches

Artifacts:

- content-addressed raw snapshots under
  `$SPACEGATE_STATE_DIR/raw/tess_evidence/snapshots/<snapshot_id>/`
- normalized inputs and append-only disposition history under
  `$SPACEGATE_STATE_DIR/cooked/tess_evidence/`
- `reports/manifests/tess_evidence_manifest.json`
- `reports/tess_source_delta_report.json`
- per-build identity coverage, resolution, and missing-object reports
- Evidence Lake raw/typed snapshots under
  `$SPACEGATE_STATE_DIR/{raw,typed}/evidence_lake_v2/tess.identity_and_candidate_evidence/`
- immutable E4 build `03acb9eb0fb2cbc0f8203dd8` plus compiler, generic
  artifact, targeted-source audit, and clean-reproduction reports under
  `$SPACEGATE_STATE_DIR/{derived,reports}/evidence_lake_v2/`

Policy:

- never bulk ingest TIC, CTL, TCE, or TESS observation products
- the targeted-universe manifest records the versioned operator seed plus the
  NASA confirmed-host and TESS EB dependency paths/checksums and per-family
  target counts, including zero-count families
- never assume Gaia DR2 and DR3 source IDs are interchangeable
- combined external-crossmatch tables must retain the exact archive member
  path; Hipparcos, Tycho-2, and 2MASS namespaces are assigned from that lineage,
  never inferred from the identifier lexeme
- TIC artifact/split/duplicate rows remain excluded or quarantined
- TOI candidates and negative dispositions remain ARM evidence
- independently supported missing real objects remain deferred until a reusable
  canonical reconciliation rule or an inspectable adjudication record accepts
  them; a local object-specific supplement file is not sufficient
- Villanova TESS EB zero-padded TIC strings preserve their raw spelling but
  normalize to unsigned decimal TIC IDs. `in_catalog=false` rows are negative
  membership evidence and never receive an EB orbit solely because they appear
  in the export.
- E5 component artifact `f5358c0a0983958e5d4f76c5` gives every one of the
  17,605 rows an exact TIC observation-target outcome. It binds 6,605 rows and
  records 11,000 missing-current-reference outcomes, with zero ambiguous or
  identifier-less records. The bound rows include 2,228 positive and 4,377
  negative catalog memberships. All sector, morphology, source/flag, Tmag,
  target-physics, astrometry, and timing evidence remains contextual until both
  physical binary endpoints are adjudicated; no canonical containment or
  simulation orbit is created.

## 13) WISE / CatWISE2020 / AllWISE

Classification: `auxiliary`

Role:

- infrared identity, photometry, color, and motion support for existing
  Spacegate objects
- targeted cross-reference source for public goldens, high-coolness systems,
  planet hosts, multistars, ultracool dwarfs, compact objects, and
  AAA-promoted research targets
- IRSA-backed WISE image cutouts for system pages and future evidence
  portfolios
- conservative candidate review queue for missing nearby ultracool/brown-dwarf
  objects

Source endpoints:

- IRSA Gator CatWISE2020 catalog (`catwise_2020`)
- IRSA Gator AllWISE Source Catalog (`allwise_p3as_psd`)
- IRSA SIA / IBE AllWISE image products for W1/W2/W3 cutouts

Policy:

- CatWISE2020 and AllWISE are evidence sources, not primary core-inventory
  backbones.
- WISE-only rows must not be bulk-promoted into `core`.
- CatWISE parallax-like fields are candidate evidence only and must not be
  treated as Gaia-grade distances.
- WISE/CatWISE/AllWISE identifiers are secondary metadata unless no better
  public name exists.
- Generated image previews are lazy, bounded cache products outside the repo.

Artifacts:

- immutable raw response sets under
  `state/raw/evidence_lake_v2_acquisition/infrared_*_targeted/`
- source-native typed snapshots under
  `state/typed/evidence_lake_v2/infrared.*_targeted/`
- deterministic target-set report
  `state/reports/evidence_lake_v2/e3_targeted_wise_target_set.json`
- clean evidence build `state/derived/evidence_lake_v2/clean_wise/ec8e218402c3a4a3b55b2811`
- `state/cooked/wise/wise_sources.csv`
- `state/cooked/wise/infrared_source_matches.csv`
- `state/cooked/wise/infrared_photometry.csv`
- `state/cooked/wise/infrared_motion_evidence.csv`
- `state/cooked/wise/infrared_candidate_queue.csv`
- `arm.wise_sources`, `arm.catwise_sources`, `arm.allwise_sources`
- `arm.infrared_source_matches`
- `arm.infrared_photometry`
- `arm.infrared_motion_evidence`
- `arm.infrared_candidate_queue`
- runtime cache: `$SPACEGATE_STATE_DIR/cache/wise_images`

Scripts:

- `scripts/acquire_targeted_wise_evidence.py`
- `scripts/compile_e7_clean_wise.py`
- `scripts/verify_e7_clean_wise.py`
- `scripts/verify_e7_clean_wise_reproduction.py`
- `scripts/collect_wise_evidence.py`
- `scripts/verify_wise_evidence.py`

Evidence Lake v2 checkpoint (July 22, 2026):

- The legacy collector is retained only until consumer cutover; its cooked CSV
  products are not clean compiler inputs because it did not retain exact IRSA
  responses.
- The replacement release queries 500 deterministic targets in both catalogs,
  preserves all 1,000 exact responses and query URLs, and carries the response
  member into every typed row. Seven CatWISE density-limit responses are
  retained together with deterministic 10/3-arcsec fallback lineage.
- Clean build `ec8e218402c3a4a3b55b2811` contains 4,212 unique catalog sources,
  4,404 scoped match rows, 210 candidate-review rows, and complete 1,000-row
  target/catalog accounting. It creates no CORE inventory.

Extended-object permanent identity is retained separately in seed
`555fa1890943b97dd0e4ef3d`. That seed preserves public IDs, aliases,
identifiers, reconciliation outcomes, and quarantine only; source-native E4/E5
evidence remains the required authority for geometry and distance.

Clean build `c203e4f451890660ec02086a` selects geometry, cluster distance, and
search from accepted E5 evidence plus clean multi-release cluster bindings. It
retains 20,160 geometry and 1,965 distance candidates, selects 1,909 distances,
and opens no stability database. Its source-native HD relation projection
preserves 849 outcomes and selects 59 distances through permanent identity and
clean system placements. Nineteen ineligible non-cluster scope claims remain
rejected and HD 97472 remains an explicit missing identity. The artifact remains
unserved until consumer A/B and local promotion pass.

Cantat-Gaudin is now an active supplementary E4 adapter rather than only a
source-native reference. Artifact `03a28284466d6821e8d5693e` preserves all
2,017 cluster rows, 234,128 UPMASK Gaia DR2 membership claims, 57 fields, native
units/flags, and citation lineage. Hunt/Reffert remains the current cluster
authority; Cantat-Gaudin may win only as a lower-authority fallback after clean
cluster identity, scope, and official DR2-to-DR3 endpoint reconciliation.

Clean selector `171d3096b5cd7ad5f53a016b` now performs that reconciliation
jointly with Hunt/Reffert. It preserves every identity and member outcome,
accepts only unique release-scoped cluster bindings of `open_cluster` scope,
and creates no canonical containment. Its selected contexts are unserved until
the clean extended-object and CORE/ARM cutovers consume them.

## Transitional Sources

## 14) AT-HYG

Classification: `transitional`

Role:

- migration compatibility for names/legacy crosswalk ergonomics only

Not allowed in target state:

- AT-HYG defining canonical star inventory existence

Retirement condition:

- remove when replacement crosswalk/naming coverage and benchmark quality gates are satisfied.

## Deferred Sources

Examples:

- BDB/ILB and other non-mirrored high-risk dependencies

Policy:

- no default dependency on sources lacking stable mirror/integrity strategy

## Additional Orbital Repositories (Evaluation Queue)

These are credible orbital-parameter repositories not yet wired into default ingest:

1. TESS rotation/time-series high-level products beyond TOI and current TESS EB
   - role candidate: rotation, activity, flare, variability, and narration
     evidence
   - status: deferred until the bounded identity/candidate/product-index goals
     in `docs/TESS_INTEGRATION.md` are complete

SB9 policy:

- acquire the CDS `B/sb9` `ReadMe`, `main.dat`, `alias.dat`, and `orbits.dat`
  together and preserve hashes in `sb9_manifest.json`
- cook source-native systems, aliases, component spectral types, and orbital
  solutions without merging them into canonical inventory
- accept component bindings only when MSC contains an exact `SB9_<sequence>`
  reference, that sequence is unique in the MSC source rows, and both MSC
  endpoints exist in the ARM component graph
- quarantine missing, ambiguous, or unresolved endpoint matches; do not fall
  back to name-only or coordinate-only component assignment
- DEBCat component evidence uses a separate unique canonical-system + period
  match and the same endpoint-existence gates
- E5 artifact `f5358c0a0983958e5d4f76c5` applies the SB9 rule to all 4,079
  catalog relations. It accepts 790 unique references with two resolved MSC
  endpoints, retains 3,104 missing references, eight ambiguous references, and
  177 unresolved referenced relations, and makes 874 component magnitudes, 940
  classifications, and 1,052 orbit solutions eligible for global selection

## Current Manifest Files

Typical manifest files:

- `reports/manifests/core_manifest.json`
- `reports/manifests/gaia_backbone_manifest.json`
- `reports/manifests/gaia_classprob_manifest.json`
- `reports/manifests/gaia_nss_manifest.json`
- `reports/manifests/sbx_manifest.json`
- `reports/manifests/sbx_evidence_v2_manifest.json` (complete Evidence Lake
  profile; separate from the served legacy projection)
- `reports/manifests/sb9_manifest.json`
- `reports/manifests/wds_manifest.json`
- `reports/manifests/orb6_manifest.json`
- `reports/manifests/debcat_manifest.json`
- `reports/manifests/kepler_eb_manifest.json`
- `reports/manifests/tess_eb_manifest.json`
- `reports/manifests/tess_evidence_manifest.json`
- `reports/manifests/msc_manifest.json` (required)
- `reports/manifests/wds_gaia_xmatch_manifest.json` (when enabled)
- `reports/manifests/atnf_manifest.json`
- `reports/manifests/magnetar_manifest.json`
- `reports/manifests/clusters_manifest.json`
- `reports/manifests/snr_manifest.json`
- `reports/manifests/vsx_manifest.json`
- `reports/manifests/ultracoolsheet_manifest.json`

Source-delta tracking files:

- `reports/source_delta_report.json` (latest per-source delta summary)
- `reports/source_delta_snapshot.json` (current baseline signatures)
- `reports/source_delta_history/*.json` (run history)
- `reports/impacted_rows_plan.json` (domain/row impact plan + execution mode)

Differential refresh scripts:

- `scripts/scan_source_deltas.py` (manifest snapshot diff)
- `scripts/plan_impacted_rows.py` (impact planner + mode routing)
- `scripts/cook_delta.sh` (selective cook for planet/lifecycle-only deltas)
- `scripts/ingest_incremental_planets.py` (incremental rebuild of planets + lifecycle side tables)
- `scripts/refresh_core.sh` (end-to-end orchestrator: differential or full path + promote + verify)

## WDS-Gaia Bridge Policy

Bridge source:

- CDS XMatch (`vizier:B/wds/wds` -> `vizier:I/355/gaiadr3`)

Classification: `auxiliary` (optional/default-off)

Grouping policy:

- multi-member WDS groups must pass physical consistency gates before grouping:
  - distance spread threshold
  - proper-motion spread threshold
  - angular distance threshold

This path remains optional while false-positive/false-negative tradeoffs are actively tuned.

## Security Requirements

1. Source integrity evidence must be recorded in manifests.
2. If transport is insecure or unreliable, source must be mirrored/pinned before default dependency.
3. Production default ingest must avoid fragile/insecure dependencies.
4. License and redistribution constraints must be documented per source family.

## Catalog Mirror Workflow (spacegates bootstrap)

Mirror target:

- `$SPACEGATE_DL_ROOT/catalogs` (auto-default: `/data/spacegate/dl` when `/data/spacegate` exists, else `/srv/spacegate/dl`)

Host env requirement:

- set `SPACEGATE_STATE_DIR` and `SPACEGATE_DL_ROOT` in the server host env files (`/etc/spacegate/spacegate.env`, `.spacegate.env`, or `.spacegate.local.env`)
- do not rely on `docker-compose.yml` alone for these values; host-side publishers/promoters also read them through `scripts/lib/env_loader.sh`

Snapshot publisher:

- `scripts/publish_catalog_mirror.py`

Recommended run sequence after successful ingest/promote/verify:

```bash
scripts/publish_catalog_mirror.py
scripts/publish_db.sh
```

Behavior:

- publishes immutable snapshot at `dl/catalogs/snapshots/<snapshot_id>/`
- updates `dl/catalogs/current` symlink and `dl/catalogs/current.json`
- preserves **original raw upstream artifacts** exactly as downloaded
- publishes **cooked Spacegate-normalized artifacts** as convenience layer for downstream bootstrap users

Operational rule:

- never replace raw artifacts with cooked variants; raw remains canonical provenance evidence.

## Provenance Expectations by Build

Each served row in `core` must map back to source lineage:

- source family
- source version snapshot
- retrieval metadata
- transform version

Any missing required provenance is a build failure.

## Notes on Storage Planning

Gaia-first builds at `<1000 ly` are multi-million-row scale.

Operational expectations:

- plan storage for backbone + product slice + reports + backups
- avoid root-disk-bound state paths for large runs
- keep retention policy explicit (build count, backup cadence, archive compression)
