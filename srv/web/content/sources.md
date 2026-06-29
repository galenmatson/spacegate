# Spacegate Source Data

Last updated: March 17, 2026 (UTC)

Spacegate is now Gaia-first. We preserve source-native records with provenance, then build deterministic layers for search, analysis, and enrichment.

## Layer Model

- `core`: canonical served inventory/projection for default browse/search
- `arm`: supplemental science overlays (hierarchy, orbital/variability support, Sol authority overlays)
- `disc`: reproducible derived outputs (scores, snapshots, enrichments)
- `rim`: user-editable worldbuilding overlays

## Active Sources (Current Production Profile)

| Source | Role | Status | Default |
| --- | --- | --- | --- |
| Gaia DR3 backbone (`gaia_source`) | Canonical star inventory | Mandatory | On |
| Sol authority (`sol_authority`, JPL Horizons) | Sol/Sun/major planets + authoritative Sol orbital scaffold | Mandatory | On |
| Sol artificial (`sol_artificial`, JPL Horizons) | Sol spacecraft/station overlay in `arm` | Auxiliary | On |
| NASA Exoplanet Archive (`pscomppars`) | Canonical exoplanet layer + aliases | Mandatory | On |
| Exoplanet lifecycle overlays (OEC, Exoplanet.eu, ExoKyoto) | Candidate/controversial lifecycle tracking in `arm` | Auxiliary | On |
| Gaia NSS (`non_single_star`, `nss_two_body_orbit`) | Multiplicity evidence/orbit support | Mandatory | On |
| WDS | Wide multiplicity evidence | Auxiliary | On |
| WDS-Gaia bridge | WDS to Gaia linkage support | Auxiliary | On |
| MSC | Hierarchical multiplicity evidence | Mandatory | On |
| ORB6 | Orbit-quality multiplicity support | Auxiliary | On |
| SBX (SB9 successor) | Spectroscopic binary evidence | Auxiliary | On |
| DEBCat | High-quality detached eclipsing binaries | Auxiliary | On |
| TESS EB | Eclipsing binary support | Auxiliary | On |
| Gaia astrophysical class probabilities | Compact/remnant classification support | Auxiliary | On |
| Gaia EDR3 white dwarf candidate catalog | White dwarf tagging support | Auxiliary | On |
| Gaia ultracool dwarf companion sheet | Ultracool-dwarf enrichment support | Auxiliary | On |
| ATNF pulsar catalog | Pulsars and compact object enrichment | Auxiliary | On |
| Magnetar catalog | Magnetar enrichment | Auxiliary | On |
| Open cluster catalog + memberships | Cluster context and membership edges | Auxiliary | On |
| Green SNR catalog | Supernova remnant context | Auxiliary | On |
| VSX (AAVSO) | Variable-star family/amplitude/period overlays in `arm` | Auxiliary | On |

## Optional, Deferred, or Excluded

| Source | State | Why |
| --- | --- | --- |
| Kepler Eclipsing Binary Catalog | Deferred (toggle available) | Useful, but current Gaia-slice linkage yield is low for default profile |
| BDB (Binary Star Database) | Deferred | No robust bulk mirror path yet; avoid fragile runtime dependence |
| SB9 (legacy spectroscopic binaries) | Excluded | Superseded by SBX path |
| EMAC | Excluded as ingest source | Tooling/resource ecosystem, not a direct canonical catalog feed |

## Source Stewardship Rules

- Provenance first: source catalog, version, retrieval checksum, and retrieval timestamp are retained.
- Deterministic transforms: cooked outputs are reproducible from raw manifests.
- Conservative matching: confidence-gated cross-catalog joins avoid speculative merges.
- Canonical separation: immutable science layers (`core/arm`) remain distinct from generated/user layers (`disc/rim`).

## Transport and Security Notes

- HTTPS/TLS sources are preferred by default.
- Any insecure transport source is explicitly risk-flagged and should be mirrored/pinned before production-default use.
- Build verification includes source lineage and quality gates before promotion.

## Where To Find More Detail

- Full source policy and matrix: `docs/DATA_SOURCES.md`
- Iteration history and catalog decisions: `docs/DATASET_ITERATION_HISTORY.md`
- Sol authority and volatile overlays: `docs/SOL_AUTHORITY.md`
