# Smart Tags and Concepts v1

Status: M8.3e.2 locally accepted on `feature/smart-tags-v1`; not publicly
deployed.

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

Each definition includes a public label/name, category, kind, layer, explicit
target types, visual token, surface priorities, short/full explanations,
optional concept route, source policy, bounded evaluator ID/version/parameters,
and filter/rollup policy.

The two explanation fields serve different jobs. `short_tooltip` is the terse
fallback for constrained surfaces. The full `tooltip` is a compact lesson,
normally three to six sentences: it defines the idea without assumed technical
vocabulary, builds physical intuition, explains how Spacegate recognizes the
tag, and weaves material uncertainty into the lesson. Accuracy does not require
dryness, and inspiration does not permit unsupported climate, habitability,
hazard, composition, or evolutionary claims.

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
Compiler v2.5 snapshots every compiler and registry input before work begins
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
  source_accounting.json
  timings.json
```

An atomic `current` symlink within the build directory identifies the promoted
registry hash. The manifest records build and registry identity, schema
versions, input identity, counts, logical table hashes, checksums, bytes, and
phase timing.

`spacegate.smart_tags.v2` separates the hot serving projection from complete
portable evidence. The normalized SQLite contains integer-keyed definitions,
system rollups, bounded exact-contribution source summaries, and quarantine.
Object-level assignments and exact source contribution records remain in
sorted Parquet artifacts. This replaces the v1 baseline that took 232.1
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
`source_summary`. Any/all/exclude use indexed `EXISTS`/bounded-count queries;
they never run fuzzy matching over tag definitions.

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

Where a surface already renders per-object stellar or compact-class badges,
the parallel system-rollup taxonomy token is suppressed as duplicate
presentation. The assignment remains available to search and the API. Each
object badge carries the full registry explanation, member scope, evidence
state, evaluator basis, and bounded system source context.

Non-source evidence states use visible letter markers and border patterns:
`D` derived, `A` assumed, `E` source model estimate, `S` screen, `C`
candidate, `?` ambiguous, `Q` quarantined, `-` missing, and `M` mixed. They
describe how the displayed claim was obtained; they are not intrinsic object
taxonomy.

Map world labels remain reserved for stellar and planet glyphs. General Smart
Tags appear in selection, Peek/Explorer, sidebars, search results, and System
Pages rather than crowding every 3D label.

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
