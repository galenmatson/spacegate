# Smart Tags and Concepts v1

Status: M8.3e.2, M8.3e.2a application and hero policy, and M8.3e.2b subject
placement are locally accepted on `feature/smart-tag-application-v1`; not
publicly deployed.

## Purpose

Smart Tags are Spacegate's shared public vocabulary for discovery,
explanation, filtering, and concept navigation. They replace scattered
frontend labels with reviewed definitions and deterministic assignments while
preserving the distinction between:

- canonical identity and containment in CORE;
- source evidence and selected facts in ARM;
- derived and presentation policy in DISC;
- fictional or user-authored overlays in RIM.

A common visual and interaction shell does not make every token the same kind
of claim. Science concepts, evidence states, source references, and future RIM
tags retain distinct namespaces, layers, evaluators, and colors.

## Registry

The reviewed registry begins at:

```text
config/tags/registry.json
config/tags/definitions/*.json
config/tags/application_policies.json
config/tags/aaa_tag_adjudication.schema.json
config/tags/proposal_inventory.json
config/tags/legacy_token_inventory.json
config/tags/source_presentation.json
```

Definitions use stable namespaced keys such as:

```text
science:stellar.white_dwarf
science:system.multiple
science:planet.temperate_terrestrial
presentation:distance.nearby
```

Each resolved definition includes a public label/name, category, kind, layer, explicit
target types, visual token, surface priorities, short/full explanations,
optional concept route, source policy, bounded evaluator ID/version/parameters,
filter/rollup policy, application semantics, and hero policy. The application
registry has one exact binding for every enabled definition; missing, extra, or
unknown bindings fail validation.

The two explanation fields serve different jobs. `short_tooltip` is the terse
fallback for constrained surfaces. The full `tooltip` is a compact lesson,
normally three to six sentences: it defines the idea without assumed technical
vocabulary, builds physical intuition, and explains how astronomers recognize
or study the phenomenon when useful. Material uncertainty should be woven into
the astronomy instead of appended as a policy disclaimer. Accuracy does not
require dryness, and inspiration does not permit unsupported climate,
habitability, hazard, composition, or evolutionary claims.

Public tooltip copy must not narrate schemas, selection policy, evaluator
thresholds, storage, retention, or compiler behavior. Those implementation and
provenance details remain available through structured Basis, evidence, and
source fields and a future science view. Public lessons should not explain away
implausible literal readings of ordinary astronomical terms. They use direct
prose and avoid unnecessary hyphenated compounds.

Registry files cannot contain SQL or executable expressions. Compiler code
accepts only reviewed evaluator IDs. `scripts/smart_tag_registry.py` fails on
unknown evaluators, malformed namespaces, missing source policy, invalid
targets, or duplicate keys.

`config/tags/proposal_inventory.json` accounts for every proposal in
`docs/TAGS.md`; `legacy_token_inventory.json` separately accounts for existing
public pills, chips, badges, source tokens, evidence states, and controls. Both
use `enabled`, `deferred`, `retired`, `rejected`, or `compatibility-only`.
Deferred proposals remain design input, not silently active assignments.

Source presentation v2 records both a full `public_name` and a reviewed
`short_name`. Compact source tokens use established forms such as `MSC`, `SB9`,
`WDS`, `ORB6`, `Gaia NSS`, and `TOI`; dialogs, accessible names, citations, and
copied details retain the full catalog or mission name. No frontend acronym
heuristic invents abbreviations.

## Application And Claim Contract

Assignment truth and public prominence are separate decisions. Every resolved
definition records claim mode, primary object scope, evidence requirements,
uncertainty and conflict policy, rollup behavior, eligible surfaces, evaluator
identity, and its revisit trigger.

The public claim grammar is `observed`, `accepted`, `derived`, `modeled`,
`likely`, `candidate`, `disputed`, and `contextual`. A compact title may omit
technical qualification, but it must not change the truth conditions of the
claim. Material uncertainty remains visible as text or a marker independent of color. A
modeled possible outcome may use a concise question such as `BLACK HOLE FATE?`;
it may not use categorical `FUTURE BLACK HOLE` and expect an unopened tooltip
to repair the overstatement.

Definitions with `claim_mode=evidence_bound` resolve their public mode from the
exact assignment. Accepted source evidence stays accepted, deterministic
calculation stays derived, source models and screens stay modeled, assumptions
and candidates stay candidate, and ambiguous, quarantined, or missing evidence
cannot enter the hero projection.

## Hero Salience

Hero salience is a versioned DISC presentation policy. It does not alter tag
membership, scientific confidence, coolness, search filtering, or canonical
data. The compiler first applies the evidence gate, then scores eligible tags
using bounded rarity, specificity, concept value, direct scope, and reviewed
base interest. Every selected row preserves its originating system, star, or
planet and records rank, score, family, claim mode, and score signals.

Composition is deliberate rather than a raw top-N sort:

- no more than one architecture tag;
- no more than two exceptional science tags;
- no more than one planetary or environmental tag;
- no more than four tags total;
- only the most specific member of an exclusive family may occupy a slot.

Generic one-star, ordinary spectral, planet-host, distance, source, and evidence
tokens are not hero candidates. Stellar and compact classifications already
communicated by object glyphs remain available through those glyphs and the
expanded All Tags view. A member rollup remains a discovery pointer to its
originating object rather than becoming a direct system claim.

## Intrinsic and Contextual Tags

Intrinsic tags describe an object or accepted system using the build's
selected facts. They may be compiled and cached.

Contextual tags depend on an observer, time, camera, or session. Examples
include `visible now`, `circumpolar from this latitude`, and `near the current
camera`. The v1 context contract reserves explicit latitude, longitude,
timestamp, and camera-position inputs. Context results are evaluated live and
must not be persisted as intrinsic science.

## Compiler and Artifacts

Run:

```bash
.venv/bin/python scripts/compile_smart_tags.py \
  --public-read /data/spacegate/state/derived/public_read/<build_id>/public_read.sqlite
```

The compiler consumes only immutable selected Public Read fields. It does not
select among competing evidence and does not mutate CORE, ARM, DISC, or RIM.
Compiler v2.6 snapshots every compiler and registry input before work begins
and verifies the same hashes before promotion. If an input changes during the
run, compilation fails closed rather than publishing a manifest with misleading
lineage.
Output is:

```text
$SPACEGATE_STATE_DIR/derived/smart_tags/<build_id>/<registry_hash>/
  smart_tags.sqlite
  assignments.parquet
  source_contributions.parquet
  registry.json
  manifest.json
  coverage.json
  quarantine.json
  proposal_accounting.json
  proposal_feasibility.json
  hero_accounting.json
  source_accounting.json
  timings.json
```

An atomic `current` symlink within the build directory identifies the promoted
registry hash. The manifest records build and registry identity, schema
versions, input identity, counts, logical table hashes, checksums, bytes, and
phase timing.

`spacegate.smart_tags.v4` separates the hot serving projection from complete
portable evidence. The normalized SQLite contains integer-keyed definitions,
system rollups, sparse hero selections, bounded exact-contribution source
summaries, compact direct subject assignments, and quarantine. Direct star and
planet assignments use Public Read integer IDs. Reviewed hierarchy leaves that
do not have a flat object row retain a stable leaf key and resolve through the
build-matched stellar badge overlay.

Complete object-level assignments and exact source contribution records remain
in sorted Parquet artifacts. The compact subject table is an interactive index,
not a replacement evidentiary store. This replaces the v1 baseline that took 232.1
seconds and produced a 6,521,847,808-byte SQLite plus a 319,843,581-byte
Parquet for 11,415,117 assignments. The hot SQLite has a hard 1.5-GiB gate;
complete evidence is not discarded to satisfy it.

Compilation is a resumable build-keyed Admin job, not a mandatory scientific
compiler phase. Two clean full outputs must reproduce logical hashes before
release packaging.

The accepted v2.2 full build materializes 11,418,384 object/system
assignments and 11,417,955 system memberships for 5,869,091 systems. Its hot
SQLite is 407,265,280 bytes, portable assignments are 294,656,487 bytes, and
exact source contributions are 756,799 bytes. The clean build takes about
229 seconds on Photon. Two separate clean outputs reproduce every logical and
physical hash; both remain retained with the comparison report for this
acceptance checkpoint.

## Initial Enabled Policies

The first reviewed set includes:

- selected O/B/A/F/G/K/M/L/T/Y, Wolf-Rayet, and compact stellar classes;
- one-known-star, binary, multiple, hierarchical, and confirmed-planet-host
  system architecture, with `hierarchy_nested_v2` recognizing a non-root
  accepted subsystem that contains at least two stellar descendants even when
  Public Read represents that composite node with a star-like source label;
- hot/temperate/cold gas-giant and terrestrial planet map categories;
- the explicitly non-habitability `HZ Screen`;
- confirmed planets with selected periods shorter than one day;
- current within-25-ly and 25-to-100-ly presentation bands;
- source-release reference summaries for exact accepted contributions.

Stellar tags retain each selected classification's `source`, `derived`, or
`assumed` state and component scope. Member tags may roll up for discovery,
but remain labeled as member rollups. Planet categories require a
confirmed/known/published planet and selected size/mass and insolation classes.
The HZ Screen remains a screening result, not a habitability claim.
Ultra-short period requires a selected period below one day. Distance bands
use the selected public point estimate because Public Read v2 does not carry a
distance interval; these bands do not claim boundary uncertainty propagation.

Missing data never creates a positive assignment. Tags never infer `rogue`
from a missing orbit. Eccentric, inclined, edge-on,
retrograde, resonant, tidally locked, evolutionary, hazard, fusion,
population, constellation, and observer-context families remain deferred until
their thresholds, reference frames, uncertainty, and applicability policies
are reviewed.

## Public API

The tag artifact is attached read-only to Public Read. It is not merged into
the 16-GB Search v2 database.

- `GET /api/v1/tags`
- `GET /api/v1/tags/{tag_key}`
- `GET /api/v1/tag-sources/{source_key}`
- `GET /api/v1/systems/{system_id}/tags`
- `GET /api/v1/systems/{system_id}/tag-assignments?offset=0&limit=100`
- `GET /api/v1/systems/search?tags_any=...`
- `GET /api/v1/systems/search?tags_all=...`
- `GET /api/v1/systems/search?tags_exclude=...`

Search results and system summaries include `smart_tags` and
`source_summary`. Main system detail and hierarchy bundles additionally expose
`subject_tag_schema_version=spacegate.smart_tag_subjects.v1` and
`subject_tags`, grouped by the actual system, star, planet, or reviewed
hierarchy leaf. These normal reads use indexed SQLite and never scan the
portable assignment Parquet. Any/all/exclude use indexed
`EXISTS`/bounded-count queries; they never run fuzzy matching over tag
definitions.

The public hero remains a maximum four-tag DISC composition. Its All Tags action
navigates to a visible lower section grouped by subject. System architecture
appears at the hierarchy root, direct object tags remain with their object, and
source references remain a separate evidence list. Stellar and planet class
tags are not repeated beside equivalent object glyphs, but the glyph inspector
retains the full lesson and evidence state.

Source-summary v3 adds the reviewed `short_name` alongside `public_name`.
Consumers use the short form only for the visible token and retain the full
name everywhere explanatory context is needed.

The v2.3 compiler reproduced source-summary v3 from clean output roots in
230.4 and 228.6 seconds with identical logical hashes and byte-identical
SQLite/Parquet artifacts. The machine comparison is retained at
`reports/smart_tags/<build_id>/smart_tag_source_summary_v3_determinism_report.json`.

Compiler v2.4 extends the shared broad planet evaluator to the complete
Giant/Neptunian/Terrestrial by Hot/Temperate/Cold matrix. It imports the same
versioned category SQL used by map tiles and API filtering, materializes
object-scoped assignments, and rolls them up for system discovery. Neptunian
is explicitly a radius/mass proxy rather than a composition claim. The first
full v2.4 compile completed in 231.4 seconds with 11,419,175 assignments,
11,418,608 system memberships, 34 definitions, zero quarantines, and a
407,285,760-byte hot artifact.

During the one-release compatibility window, a genuinely absent artifact
leaves ordinary reads untagged. A present but mismatched, sampled, corrupt, or
schema-incompatible artifact fails visibly. Set
`SPACEGATE_SMART_TAGS_REQUIRED=1` after the first public tag promotion.

Source tokens are exact contribution claims, not catalog-presence claims.
They appear only when a source/release identifier contributes accepted
displayed hierarchy, relation, orbit, classification, selected-fact, or
explicit-alternative lineage. Boolean presence fields such as
`has_gaia_nss_evidence` never generate source-token fanout. Absence of a source
token does not prove the Evidence Lake contains no evidence from that source.

## Interaction Contract

The shared component supports hover and keyboard focus, click/touch pinning,
Escape/outside close, concept and filtered-search links, copied links,
source-policy display, and bounded source summaries. Text and accessible
labels carry meaning in addition to color.

Hover and keyboard focus provide a temporary preview. Click or touch opens a
viewport-level pinned inspector above panels and WebGL canvases. The inspector
retains a bounded back trail when another tag is opened, can focus the
originating simulation object for member rollups, and carries return context
into concept pages. The expanded All Tags control exposes the complete system
vocabulary and exact source tokens without crowding the hero.

Where a surface already renders per-object stellar or compact-class badges,
the parallel system-rollup taxonomy token is suppressed as duplicate
presentation. The assignment remains available to search and the API. Each
object badge carries the full registry explanation, member scope, evidence
state, evaluator basis, and bounded system source context.

Non-source claim modes use visible letter markers and border patterns where a
qualification is material: `O` observed, `D` derived, `M` modeled, `L` likely,
`?` candidate, `!` disputed, and `@` contextual. Accepted claims need no extra
marker. Structured evidence details continue to distinguish source, assumption,
screen, ambiguity, quarantine, and missing states.

Map world labels remain reserved for stellar and planet glyphs. General Smart
Tags appear in selection, Peek/Explorer, sidebars, search results, and System
Pages rather than crowding every 3D label.

## AAA Adjudication

`config/tags/aaa_tag_adjudication.schema.json` defines the packet for a tag that
cannot be assigned by a reviewed deterministic policy. It requires an explicit
subject, proposed claim mode, evidence and counterevidence, model identity and
applicability, calibrated confidence, alternatives, recommendation, review
state, reviewer, and revisit triggers.

AAA review should improve reusable evaluator policy or produce an auditable
reviewed exception. It never creates compiler branches for named objects and
never writes CORE directly. Future remnant, tidal locking, resonance, rogue,
uncertain emission, hazard, and similar families remain deferred until their
feasibility and review requirements pass.

## Concept Pages

The first reviewed pages are:

- `/concepts/spectral-class`
- `/concepts/white-dwarf`
- `/concepts/brown-dwarf`
- `/concepts/binary-and-multiple-stars`
- `/concepts/exoplanet`
- `/concepts/habitable-zone`
- `/concepts/orbital-period`
- `/concepts/astronomical-evidence`

Interactive visualizations and deeper observation tools remain later
milestones. Concept text must not promote a screen, assumption, candidate, or
model estimate into a measured fact.

## RIM Extension

The `rim:` namespace and display boundary are reserved, but v1 provides no
public tag editing. Future RIM tags must identify owner/pack, layer, visibility,
review state, and target scope and remain visually distinct from science.
They must never share the intrinsic science assignment tables.

## Acceptance Evidence

The two clean full builds, artifact comparison, capacity campaign, API/browser
coverage, and four-artifact release rehearsal passed. The constrained campaign
completed seven cold/warm/idle profiles and the c1-c12 staircase with zero
errors, timeouts, OOM events, or safety stops. Its principal p95 results were:

| Profile | p95 | Throughput |
| --- | ---: | ---: |
| targeted cold c1 | 89.3 ms | 37.7 requests/s |
| exact TIC/name c1 | 10.1 ms | 128.3 requests/s |
| filtered search c1 | 104.1 ms | 12.5 requests/s |
| summary/tag/source/assignment c1 | 32.0 ms | 136.8 requests/s |
| sustained mixed c12 | 2.747 s | 18.0 requests/s |
| staircase c12 | 2.720 s | 17.4 requests/s |
| recovery | 98.8 ms | 35.8 requests/s |

Peak aggregate constrained container memory was 245,510,144 bytes. Ordinary
Photon compatibility mode still permits an absent artifact; the constrained
campaign and rehearsed public release require the exact matching artifact.

See `docs/SMART_TAGS_VERIFICATION_2026-07-27.md` for the acceptance record and
machine-report paths.

M8.3e.2a compiler v2.6 adds the application and hero projection without
changing scientific assignment membership. Two clean full builds finish in
285.4 and 287.3 seconds and reproduce every physical artifact and logical table
hash. Each contains 11,419,175 assignments, 11,418,608 system memberships,
90,771 exact source contributions, 183,047 composed hero rows across 182,885
systems, and zero quarantines. The hot SQLite is 421,978,112 bytes, a 3.6%
increase over v2.5 and well below the 1.5-GiB serving gate.

The build-matched 6-vCPU/12-GiB delta campaign also passes every profile and
the c1/c4/c8/c12 staircase with zero errors, timeouts, OOMs, or safety stops.
Constrained registry/definition, tag-filter, and system-tag/evidence reads are
6.1, 70.2, and 33.7 ms p95; mixed c12 traffic is 1.977 seconds p95 and recovers
to 65.7 ms p95.

See `docs/SMART_TAG_APPLICATION_VERIFICATION_2026-08-06.md` for the application,
scope, hero, interaction, capacity, and deterministic-build acceptance record.

M8.3e.2b compiler v2.7 adds 5,546,540 compact direct subject assignments to
the hot projection. Two clean full builds finish in 340.8 and 343.1 seconds and
produce byte-identical 682,602,496-byte SQLite, 295,377,586-byte assignment
Parquet, and 763,812-byte source-contribution Parquet artifacts. The 61.8%
SQLite growth over v3 is the measured cost of indexed object placement and
still leaves 928.0 MB (885.0 MiB) below the 1.5-GiB serving gate.

The general overlay rule evaluates the 16,167 reviewed stellar leaves in 5,312
systems instead of attaching classifications to incomplete flat-star rows. The
scientific A/B adds 7,557 system/class discovery memberships and removes 127
legacy flat-row memberships that disagree with the visible leaves; no
nonstellar membership changes. Indexed single-system subject reads across 300
representative overlay, planet-host, and singleton systems measure 0.172 ms
p50, 0.237 ms p95, and 0.479 ms maximum on warm Photon storage.

See `docs/SMART_TAG_SUBJECT_PLACEMENT_VERIFICATION_2026-08-06.md` for the
determinism, scientific A/B, runtime, UI, and local-promotion record.
