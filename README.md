  What Spacegate Is

  - A public, richly browseable 3D star map + worldbuilding layer grounded in real astronomy. It prioritizes fun exploration and factual,
    engaging descriptions while keeping core science data immutable and provenance‑clean.

  Scope and Deliverables

  - Core datasets: systems, stars, planets (AT‑HYG + NASA Exoplanet Archive).
  - Optional “packs” (v2.1+): substellar, compact, superstellar, etc., as separate, read‑only artifacts.
  - Enrichment (v1.1+): derived artifacts like blurbs, reference links, snapshots.
  - A browser 3D map (v2) with filters and overlays.

  Data & Pipeline Model

  - raw/ immutable upstream artifacts.
  - cooked/ normalized, catalog‑shaped (no joins).
  - served/ queryable outputs; served/current points to the promoted out/<build_id>/.
  - Strong provenance required in all rows.

  Schema / Rules Highlights

  - Core artifacts: DuckDB + Parquet, sorted by Morton Z‑order spatial_index.
  - Stable object keys for systems/stars/planets; strict provenance fields required 100%.
  - Planet → host matching prioritized by Gaia DR3 ID, then HIP, HD, then hostname.

  Packs Contract

  - Pack schema requires stable_object_key, object type, coordinates, and full provenance.
  - Discovered via packs_manifest.json.

  Roadmap (high level)

  - v1.1: static snapshot generation (SVG) with deterministic rendering rules.
  - v1.2: factual “facts → blurb” generation + reference links.
  - v1.2.2: precomputed 10‑nearest neighbor graph.
  - v2: browser 3D map.
  - v2.1+: optional catalogs as packs.
