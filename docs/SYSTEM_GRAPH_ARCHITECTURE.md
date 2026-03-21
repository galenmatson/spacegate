# Spacegate System Graph Architecture

This document defines the cross-layer graph model used for scientific hierarchy, navigation, and worldbuilding overlays.

## Why This Exists

Spacegate must support both:

- scientific correctness and provenance
- deep, user-extensible structures (systems, subsystems, infrastructure, lore)

A pure tree is too rigid. A pure graph is hard to navigate and can become chaotic.

## Graph Model

Use two complementary structures:

1. Containment spine (tree, acyclic):
- primary navigation path
- one canonical parent per node for stable traversal/UI

2. Relation graph (multi-edge, cyclic allowed):
- physical and logical cross-links
- supports bridges, shared infrastructure, gateways, and other non-tree relationships

## Layer Ownership (Important)

`node types` and `edge types` are canonical vocabulary, not a statement that all rows live in `core`.

Storage by layer:

- `galaxy` / `core` / `halo`:
  - canonical astronomy inventory and source-native science fields
  - official classifications (source-normalized)
- `arm`:
  - scientific graph materialization (hierarchy edges, orbit edges, barycenters, readiness metadata)
  - deterministic science-derived structural nodes/edges outside core hot paths
- `disc`:
  - derived/rebuildable presentation artifacts
- `rim`:
  - editable user/lore nodes and edges anchored to scientific nodes

## Classification Policy

Two-class approach:

1. Scientific classification (authoritative):
- stored in science layers (`galaxy/core/halo`, plus supporting evidence in `arm`)
- must follow source material and official conventions

2. Structural/navigation typing:
- used to power hierarchy and simulation scaffolding
- may use practical groupings (for example `subplanet`) for UX and tools

Rule:
- UX supergroups must never overwrite or replace authoritative scientific classification fields.

Example:
- Ceres: scientific class `dwarf_planet`, structural type may be `subplanet`.
- Vesta: scientific class `asteroid`/minor-body family, structural type `minor_body`.

## Canonical Node Vocabulary

Initial shared vocabulary:

- `system`
- `star`
- `planet`
- `subplanet` (structural supergroup)
- `moon`
- `minor_body`
- `barycenter`
- `region` (for belts, clouds, Lagrange regions)
- `infrastructure` (usually rim)
- `artifact` (usually rim)

## Canonical Edge Vocabulary

Containment spine:

- `contains` (tree-only, acyclic, single primary parent per child)

Scientific/dynamic graph (primarily `arm`):

- `orbits`
- `co_orbits`
- `member_of_pair`
- `subsystem_of`

Overlay/logical graph (primarily `rim`):

- `anchored_to`
- `located_in`
- `gateway_to`
- `stabilized_by`
- `influences`

## Loop Handling

Loops are allowed in relation edges and disallowed in `contains`.

Pattern:

- choose one canonical containment parent for navigation
- represent cross-links with relation edges

This handles cases like shared Lagrange infrastructure without corrupting hierarchy.

## Generator Compatibility

Procedural tools must reuse this same graph contract:

- generated nodes/edges are written to `rim`
- scientific layers remain immutable
- generated rows carry seed/model/provenance fields
- generated structures remain anchorable and traversable through the same node/edge vocabulary

## Minimum Integrity Rules

- `contains` must remain acyclic.
- every node has at most one `primary_parent`.
- every node records `root_system`.
- every non-core edge includes provenance, confidence, and transform lineage.
- rim imports never mutate canonical science rows.

