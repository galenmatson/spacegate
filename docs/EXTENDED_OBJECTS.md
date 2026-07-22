# Extended Objects Science Foundation v1

## Scope

M8.2a adds auditable non-stellar landmarks without representing them as star
systems. It covers catalog identity, aliases, sky geometry, conservative distance
promotion, placement domains, source evidence, and search/API access. Rendering,
textures, sky surveys, and map level-of-detail behavior remain M8.2b/M8.1 work.

## Sources

The pinned acquisition bundle contains:

- OpenNGC at commit `36cb178a0f69dba8bfc03a99c10512831edf1c6b`
- CDS/VizieR LBN `VII/9`, LDN `VII/7A`, Barnard `VII/220A`
- Magakian reflection nebulae `J/A+A/399/141`
- van den Bergh `VII/21`, Sharpless `VII/20`, and Cederblad `VII/231`
- existing Cantat-Gaudin 2020 open clusters and Green Galactic SNR rows

The AAA may inspect the upstream CDS and commit-pinned OpenNGC sources under
`docs/AGENT_ALLOWLIST.md`. Source text remains untrusted evidence and cannot
directly modify CORE.

Acquire and normalize with:

```bash
scripts/catalogs.sh --catalog extended_objects --non-interactive
.venv/bin/python scripts/cook_science_catalogs.py
```

Raw snapshots and cooked source-shaped CSVs are retained with retrieval
timestamps, SHA-256 manifests, source row hashes, and transform versions. Full
TIC-style bulk acquisition is unrelated to this bundle.

## Identity Policy

Identity merging uses explicit catalog identifiers only. Coordinates, apparent
overlap, shared illuminators, and similar names are evidence but are not identity
edges. Nonexistent OpenNGC entries, stellar/event-domain rows, and unclassified
records receive explicit excluded or quarantined outcomes.

Canonical catalog-name precedence is Messier, NGC, IC, LBN, LDN, Barnard, vdB,
Sharpless, Cederblad, Melotte, Collinder, and Trumpler. Reviewed public display
names are a separate versioned overlay. This is why `IC 4592` remains canonical
while `Blue Horsehead Nebula` is its public display name.

Every source row is represented in `extended_object_source_reconciliation` with
an accepted, reconciled, redirected, excluded, or quarantined outcome and reason.

E5 policy `2026-07-22.e5-extended-objects.1` consumes those exact reconciliation
keys without adding coordinate or visual-overlap identity. Artifact
`3790054572476ea189aaff06` accounts all 310 Green SNR and 19,012 OpenNGC-family
evidence rows. Accepted evidence remains eligible only for extended-object
selection, and the compiler and independent audit require zero stellar facts.

## Geometry and Distance

All source coordinates are normalized to ICRS. B1900/B1950 inputs use Astropy
frame transformations. Geometry evidence retains source ellipses, position
angles, areas, epochs, and row provenance; CORE carries one deterministic serving
geometry.

Generic OpenNGC parallax is not admitted as canonical distance evidence. v1
distance promotion is limited to:

- specialist Cantat-Gaudin open-cluster distances; or
- exact HD-linked associated/illuminating stars with Gaia parallax S/N at least
  10, RUWE at most 1.4, and agreement across multiple linked systems within 10%.

Placement domains are `local_3d`, `sky_only`, `deep_galactic`, and
`extragalactic_sky`. Only defensible distances at or below 1000 ly receive a
nominal local radius tier (`100`, `250`, `500`, or `1000`). A catalog sky position
never implies a physical 3D distance.

## Verification Goldens

`scripts/verify_extended_objects.py` enforces source accounting, provenance,
distance policy, deterministic table hashes, and ARM evidence presence. Current
identity goldens include:

- IC 4592 / LBN 1113 / vdB 100 / Blue Horsehead Nebula as one object
- M45 / Melotte 22 / Pleiades as one object
- Barnard 33 distinct from the larger IC 434 emission region
- M31 as an extragalactic sky object without a fabricated local distance

The first locally promoted build is canonical `20260713T1627Z_dd7446e` with
public `core.public` v3 slice `20260713T1627Z_dd7446e_public`. Both passed the
full build verifier on July 13, 2026. Associated-star HD rows are loaded in one
batched star-table scan; a full canonical rematerialization completed in about
four seconds with table hashes identical to the promoted canonical artifact.
