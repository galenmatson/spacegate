# Spacegate v1.1 UX Spec (Public Database Browser)

Scope: search/browse core systems, stars, and planets from `core.duckdb`, with read-only rich overlays for coolness ranking and deterministic system snapshots. No 3D map, no lore.

## Global UI Principles
- Primary task: find a system and understand what is known about it.
- Secondary task: compare systems by distance and basic characteristics.
- Always show provenance and match confidence where applicable.
- Never fabricate values. If a field is null, show "Unknown" and avoid derived guesses.
- Units: distance in light-years (ly), angles in degrees.

## Page: Search / Results

### Layout
- Header: "Spacegate Browser" title, short subtitle, and a global search input.
- Filters panel (left on desktop, collapsible on mobile).
- Results list (right on desktop, full width on mobile).
- Results are clickable rows/cards that navigate to System Detail.

### Search Input
- Placeholder: "Search systems by name, ID, or catalog key..."
- Search matches `system_name_norm` and exact `stable_object_key`. The UI should pass the raw input to the API; the API handles normalization.

### Filters
- Max distance (ly): numeric input, optional.
- Spectral class: multi-select (O, B, A, F, G, K, M, L, T, Y), optional.
- Has planets: toggle, optional.

### Sorting
- Default: Coolness (top-ranked).
- Alternate: Distance (nearest first).
- Alternate: Name (A-Z).

Sorting rules:
- Coolness: `rich.coolness_scores.rank` ascending, tie-breakers `system_name_norm`, `system_id`.
- Name: `system_name_norm` ascending, tie-breaker `system_id`.
- Distance: `dist_ly` ascending, tie-breaker `system_id`.

### Result Card Fields (per system)
- Deterministic snapshot thumbnail (if available in `snapshot_manifest`; otherwise show pending state)
- System name
- Distance (ly)
- RA/Dec (deg)
- Star count
- Planet count
- Spectral classes present (from member stars)
- IDs: Gaia/HIP/HD where present
- Provenance badge (source catalog + version)

### Empty / Error States
- No query + no filters: show a "Start typing to search" empty state.
- Query with zero results: show "No systems match this search" and suggest relaxing filters.
- API error: show "Data temporarily unavailable" with a retry button.

### Field-to-Schema Mapping (Search/Results)
- System name: `systems.system_name`
- Distance (ly): `systems.dist_ly`
- RA/Dec (deg): `systems.ra_deg`, `systems.dec_deg`
- XYZ (helio, ly): `systems.x_helio_ly`, `systems.y_helio_ly`, `systems.z_helio_ly` (optional in UI)
- IDs: `systems.gaia_id`, `systems.hip_id`, `systems.hd_id`
- Stable key: `systems.stable_object_key`
- Star count: `COUNT(stars.star_id)` grouped by `stars.system_id`
- Planet count: `COUNT(planets.planet_id)` grouped by `planets.system_id`
- Spectral classes: `DISTINCT stars.spectral_class` per `stars.system_id`
- Provenance badge: `systems.source_catalog`, `systems.source_version`

## Page: System Detail

### Layout
- Header section with system name and identifiers.
- Deterministic snapshot panel near top of page.
- Quick facts grid (distance, coordinates, counts).
- Stars section: table/list of member stars.
- Planets section: table/list of known exoplanets.
- Provenance & Trust section with full provenance details.

### Header
- System name (primary)
- Stable key (secondary)
- Catalog IDs: Gaia/HIP/HD (if available)

### Quick Facts
- Distance (ly)
- RA/Dec (deg)
- XYZ (helio, ly)
- Star count
- Planet count

### Snapshot Panel
- Show deterministic snapshot image when present.
- Show neutral pending state when a snapshot has not been generated yet.
- Snapshot metadata may include `view_type` and `params_hash` for provenance/debugging.

### Stars Section (per star)
- Star name (or "Unnamed")
- Component (A/B/C) if available
- Spectral type (raw + parsed fields where available)
- Distance (ly)
- Apparent magnitude (Vmag) if available
- IDs: Gaia/HIP/HD

### Planets Section (per planet)
- Planet name
- Discovery year / method / facility
- Orbital period (days)
- Semi-major axis (AU)
- Eccentricity
- Planet radius/mass (Earth or Jupiter units as available)
- Equilibrium temperature (K) and insolation (Earth=1) if available
- Host match provenance: method, confidence, and notes

### Empty / Error States
- If no stars are linked: show "No star members recorded".
- If no planets are linked: show "No confirmed exoplanets recorded".
- If system not found: show "System not found" with a link back to search.

### Field-to-Schema Mapping (System Detail)
System fields:
- System name: `systems.system_name`
- Stable key: `systems.stable_object_key`
- IDs: `systems.gaia_id`, `systems.hip_id`, `systems.hd_id`
- Distance (ly): `systems.dist_ly`
- RA/Dec (deg): `systems.ra_deg`, `systems.dec_deg`
- XYZ (helio, ly): `systems.x_helio_ly`, `systems.y_helio_ly`, `systems.z_helio_ly`
- Provenance: all required provenance fields from `systems` (see Trust rules)

Star fields:
- Star name: `stars.star_name`
- Component: `stars.component`
- Spectral raw/parsed: `stars.spectral_type_raw`, `stars.spectral_class`, `stars.spectral_subtype`, `stars.luminosity_class`, `stars.spectral_peculiar`
- Distance (ly): `stars.dist_ly`
- Apparent magnitude: `stars.vmag`
- IDs: `stars.gaia_id`, `stars.hip_id`, `stars.hd_id`
- Provenance: all required provenance fields from `stars`

Planet fields:
- Planet name: `planets.planet_name`
- Discovery: `planets.disc_year`, `planets.discovery_method`, `planets.discovery_facility`, `planets.discovery_telescope`, `planets.discovery_instrument`
- Orbital: `planets.orbital_period_days`, `planets.semi_major_axis_au`, `planets.eccentricity`, `planets.inclination_deg`
- Physical: `planets.radius_earth`, `planets.radius_jup`, `planets.mass_earth`, `planets.mass_jup`, `planets.eq_temp_k`, `planets.insol_earth`
- Host identifiers: `planets.host_name_raw`, `planets.host_gaia_id`, `planets.host_hip_id`, `planets.host_hd_id`
- Match provenance: `planets.match_method`, `planets.match_confidence`, `planets.match_notes`
- Provenance: all required provenance fields from `planets`

## Trust / Provenance Display Rules
- Always show source catalog and version for system, stars, and planets.
- Show license and redistribution flag. If `redistribution_ok` is false, display a warning tag.
- Show retrieval date (`retrieved_at`) and transform version (`transform_version`).
- For planets, always show host match method + confidence. If `match_confidence` < 0.7, show a caution label.
- Include source URLs (source_url and source_download_url) as external links in the Provenance section.

## Accessibility & Performance
- Make the search input keyboard-focused on page load.
- Results should be paginated; do not attempt to load all systems at once.
- Keep UI usable on mobile with a single-column layout.
