# Coolstars Help

Coolstars is the public exploration interface for Spacegate. It combines a 3D local star map, Star Search, and System Simulation so you can move from "what is that star?" to "what do we know about this system?" without leaving the experience.

This page explains the main features, where to find information, and how to navigate the site.

## Quick Start

Start at the 3D map:

https://coolstars.org/map

Use the top `Search` button to show or hide the map search interface.

- Search visible: use filters, system cards, and search results.
- Search hidden: fly the map with minimal screen clutter.

Click or tap a star label to open a System Simulation Peek. Use `Explore` to open a larger focused system view. Use `Close` or the browser Back button to return to the map.

## Top Navigation

The top navigation appears in the Star Search, system pages, and the 3D map.

| Link | Meaning |
| --- | --- |
| `HELP` | This guide. |
| `ABT` | About Spacegate and Coolstars. |
| `MAP` | The 3D local star map. |
| `SPT` | Sponsor/support link. |
| `SRC` | Source code on GitHub. |
| `DATA` | Source-data and provenance overview. |

On the 3D map, `Search` is a toggle. It opens and closes the map-native Star Search controls. `Home` recenters the map on Sol without reloading the page.

## The 3D Map

The map is a live WebGL view of nearby space. It is currently centered on the local neighborhood and optimized for the public server.

The map shows:

- Nearby star systems.
- Labels for selected, nearby, high-coolness, and filter-matching systems.
- Spectral-color star points.
- Search results and selected-system simulations.
- Optional route measurements.
- Optional galactic direction labels.

### Map Labels

The map tries to balance usefulness and clutter. It labels nearby systems, selected systems, and higher-coolness systems more aggressively than anonymous low-interest systems.

When Star Search filters are active, labels shift toward systems matching those filters. For example, selecting only G-type stars or systems with planets makes the visible labels follow that search intent.

### Flying the Map

Desktop controls:

- Drag mouse: look around.
- Mouse wheel up/down: fly forward/back.
- WASD: fly forward, left, back, right.
- Arrow keys: fly forward, left, back, right.
- `Q`: move up.
- `Z`: move down.
- Right mouse drag: truck left/right.
- Middle mouse drag: pedestal up/down.
- Left + right mouse together: orbit the selected system, or Sol if nothing is selected.

The burger menu lets you switch keyboard layouts:

- `WASD`: standard game controls.
- `ESDF`: shifted right; `A` up, `Z` down.
- `8456`: numpad-style movement; `7` up, `1` down.

Arrow keys always fly regardless of the selected keybind set.

Mobile controls:

- Drag: look around.
- Pinch: move forward/back.
- Two-finger pan: translate.
- On-screen arrows: fly forward, back, left, right, up, and down.
- Tap a system label or point: select/open it.
- Use `Search` to hide the search interface for clearer free flight.
- Use `Home` if you get lost and want to jump back to Sol.

## Map Star Search

The map search overlay is designed for visual exploration rather than a traditional table-only catalog search.

Open it with the top `Search` button.

You can search by:

- Common star or system names.
- Catalog identifiers.
- Aliases such as Gaia, HIP, HD, WDS, Bayer/Flamsteed names, and other known identifiers.

The filter sidebar includes:

- Distance from the current map viewpoint.
- Number of stars in the system.
- Number of planets.
- Coolness score.
- Habitable-zone planet candidate toggle.

The spectral selector bar includes:

- Spectral classes O, B, A, F, G, K, M, L, T, Y, and D.
- A temperature range filter.

Search results can be sorted by:

- Best match.
- Nearest to current viewpoint.
- Coolest.
- Name.
- Planet count.
- Star count.
- Hottest or coolest stellar temperature.

### Search Cards

Search result cards show system vitals and a preview of the System Simulation.
Small tags call out useful discovery cues such as nearby systems, exoplanets,
multi-star systems, ultracool dwarfs, white dwarfs, habitable-zone-style
signals, and major evidence catalogs. These tags are search aids, not new
catalog facts.

To avoid overwhelming your browser with too many WebGL contexts, cards usually show a cached image after the first live render. Hovering or focusing a card can temporarily animate the preview when resources are available.

Use:

- `Peek`: open a lightweight simulation overlay while staying on the map.
- `Explore`: open the larger focused System Explorer.

## Selection History and Cool Stars Nearby

The map sidebar includes compact lists:

- Selection history: recently selected systems.
- Cool Stars Nearby: nearby systems weighted toward interesting systems rather than only the closest red dwarfs.

These lists are meant to encourage exploration. They are not a scientific ranking of importance.

## System Simulation Peek

Peek is the lightweight system view that opens over the map.

It is useful when you want to inspect a system without leaving the map context.

Peek includes:

- A running 3D system simulation.
- Star, planet, and orbit labels.
- Habitable-zone display.
- Scale and speed controls.
- Key system vitals.
- `Explore` entry for a deeper view.
- `Close` to return to free flight.

Peek intentionally hides some detailed provenance and diagnostic information. The goal is quick inspection, not a full technical report.

## System Explorer

Explore opens a larger System Simulation view. It is the best place to inspect a system in detail.

Explorer includes:

- Live star and planet motion.
- Multi-star hierarchy rendering where available.
- Planetary orbit traces.
- Habitable zones.
- Temperature threshold lines.
- Scale modes.
- Speed controls.
- Object labels.
- Hover or pinned readouts.
- Collapsible diagnostics.

The browser Back button should return you to the map where you came from, preserving your map context where possible.

## Scale Modes

System Simulation supports multiple scale modes because real star systems are difficult to display honestly on one screen.

| Mode | Purpose |
| --- | --- |
| Structure / Clarity | Default readable layout. Preserves system structure and avoids obvious collisions. Not physically to scale. |
| True Orbits | Orbital distances are proportional. Bodies are visually reduced so orbits do not disappear inside oversized stars. |
| True Bodies | Body sizes are closer to true relative scale. Planets can become hard to see. |
| Log Scale | Compresses wide dynamic range so inner and outer systems can both remain visible. |

No single mode tells the whole truth. Use Structure for understanding the arrangement, True Orbits for orbital spacing, True Bodies for size contrast, and Log Scale for very wide systems.

## Simulation Speed

The speed selector changes how fast orbital motion advances.

High speed is useful for long-period binaries and outer planets. Low speed is useful for compact systems such as TRAPPIST-1.

The simulation is a visualization, not a high-precision ephemeris. Where source orbital data is available, Spacegate uses it. Where data is missing, the renderer may use explicit derived or assumed presentation values.

## Habitable Zones and Temperature Lines

System Simulation can show several orbital-temperature landmarks.

Habitable zone:

- The region where a planet could receive star light compatible with surface liquid water under the right atmospheric assumptions.
- It does not prove habitability.
- Binary companions, eccentricity, atmosphere, tidal locking, flares, and planetary history can all matter.

Temperature threshold lines:

- Vaporization line.
- Soot line.
- Water freeze line, often called the snow line.
- Carbon dioxide freeze line.
- Methane and carbon monoxide freeze line.
- Nitrogen freeze line.

These are educational overlays. They help explain where different solids and ices can survive during planet formation.

## Routes and Distance Measurement

Right click or long-press a system to open a context menu. From there you can select or measure.

Route measurement lets you:

- Choose systems as waypoints.
- Draw straight-line route segments.
- See leg distances and total distance.
- Remove route segments by selecting them.

Routes are currently ephemeral map tools. They are not saved as worldbuilding data and they are not written to the Rim/lore layer.

## Themes

Coolstars includes several visual themes, including:

- Simple Light.
- Simple Dark.
- Cyberpunk.
- Enterprise / LCARS-inspired.
- Mission Control.
- Aurora.
- Geocities.
- Deep Space Minimal.

Themes change presentation only. They do not change the data.

The burger menu in the map header contains theme, keybind, frame, direction-label, and diagnostics controls.

## Coordinate Frames and Direction Labels

The map currently uses an ICRS/Gaia-oriented scene as its base coordinate frame. A Galactic frame toggle is available for orientation work.

Direction labels can show:

- Coreward.
- Rimward.
- Spinward.
- Antispinward.

These labels are orientation aids. They are not meant to imply fictional borders or territories.

## System Pages

System pages provide the more traditional information view for a star system.

Look there for:

- A large System Simulation at the top of the page.
- A plain-language overview and "why this matters" section.
- System hierarchy.
- Star and planet lists.
- Object vitals.
- Identifiers and aliases.
- Source/evidence notes.
- Technical catalog rows and diagnostics in secondary sections.

Star Search v2 and system pages are designed for progressive disclosure: start with the visual simulation and public explanation, then open the detailed catalog/evidence sections when you want the source-level rows.

## Reading Provenance Labels

Spacegate uses provenance labels to avoid presenting guesses as facts.

Common statuses:

- `SOURCE`: directly from a catalog or authority source.
- `DERIVED`: calculated deterministically from source data.
- `ASSUMED`: an explicit presentation or simulation assumption.
- `MISSING`: no usable value is available.

In small Peek views, detailed provenance may be hidden for readability. In deeper system views, more detail is available.

## Coolness Score

Coolness is a presentation ranking used to help visitors find interesting systems.

It can consider things such as:

- Distance.
- Planet count.
- Habitability indicators.
- Multiple-star structure.
- Stellar type.
- Notable catalog or science context.

Coolness is not a scientific measurement. It is a discovery aid for browsing.

## Finding Source Information

Use `DATA` in the top navigation to see the current source-data overview.

For a specific system, use the system page and simulation diagnostics. For code-level details, use `SRC` to inspect the open source repository.

## Known Limits

Coolstars is under active development.

Current limitations include:

- The public build is sliced for performance and is not the full raw catalog universe.
- Some long catalog identifiers are difficult to read and are still being refined in the interface.
- Some multiple-star systems remain limited by incomplete or ambiguous source orbital data.
- High-quality static simulation-derived snapshots are still future work; live System Simulation is the preferred visual surface for capable browsers.
- AI-generated narration is planned, but the public site does not yet contain the full AI Astronomy Agency narrative layer.

## Tips

- Use `Search` to hide the overlay before free flying.
- Use the map filters to make labels appear for the kinds of systems you care about.
- Use Peek for quick curiosity.
- Use Explore for deeper inspection.
- Use DATA when you want source context.
- Use SRC if you want to audit or contribute to the project.
