# Smart Tags and Concepts v1

Status: M8.3e.2 implementation checkpoint on `feature/smart-tags-v1`.

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

Registry files cannot contain SQL or executable expressions. Compiler code
accepts only reviewed evaluator IDs. `scripts/smart_tag_registry.py` fails on
unknown evaluators, malformed namespaces, missing source policy, invalid
targets, or duplicate keys.

`config/tags/proposal_inventory.json` accounts for proposals in
`docs/TAGS.md` as enabled, deferred, retired, or rejected. Deferred proposals
remain design input, not silently active assignments.

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
Output is:

```text
$SPACEGATE_STATE_DIR/derived/smart_tags/<build_id>/<registry_hash>/
  smart_tags.sqlite
  assignments.parquet
  registry.json
  manifest.json
  coverage.json
  quarantine.json
```

An atomic `current` symlink within the build directory identifies the promoted
registry hash. The manifest records build and registry identity, schema
versions, input identity, counts, logical table hashes, checksums, bytes, and
phase timing. SQLite contains definitions, object assignments, system rollups,
source definitions, bounded system-source summaries, and quarantine rows.
Parquet preserves portable assignment evidence.

## Initial Enabled Policies

The first reviewed set includes:

- selected O/B/A/F/G/K/M/L/T/Y, Wolf-Rayet, and compact stellar classes;
- one-known-star, binary, multiple, hierarchical, and confirmed-planet-host
  system architecture;
- hot/temperate/cold gas-giant and terrestrial planet map categories;
- the explicitly non-habitability `HZ Screen`;
- confirmed planets with selected periods shorter than one day;
- current within-25-ly and 25-to-100-ly presentation bands;
- source-release reference summaries for accepted multiplicity/orbit inputs.

Tags never infer `rogue` from a missing orbit. Eccentric, inclined, edge-on,
retrograde, resonant, tidally locked, evolutionary, hazard, fusion,
population, constellation, and observer-context families remain deferred until
their thresholds, reference frames, uncertainty, and applicability policies
are reviewed.

## Public API

The tag artifact is attached read-only to Public Read. It is not merged into
the 16-GB Search v2 database.

- `GET /api/v1/tags`
- `GET /api/v1/tags/{tag_key}`
- `GET /api/v1/systems/{system_id}/tags`
- `GET /api/v1/systems/search?tags_any=...`
- `GET /api/v1/systems/search?tags_all=...`
- `GET /api/v1/systems/search?tags_exclude=...`

Search results and system summaries include `smart_tags` and
`source_summary`. Any/all/exclude use indexed `EXISTS`/bounded-count queries;
they never run fuzzy matching over tag definitions.

During the one-release compatibility window, a genuinely absent artifact
leaves ordinary reads untagged. A present but mismatched, sampled, corrupt, or
schema-incompatible artifact fails visibly. Set
`SPACEGATE_SMART_TAGS_REQUIRED=1` after the first public tag promotion.

## Interaction Contract

The shared component supports hover and keyboard focus, click/touch pinning,
Escape/outside close, concept and filtered-search links, copied links,
source-policy display, and bounded source summaries. Text and accessible
labels carry meaning in addition to color.

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
