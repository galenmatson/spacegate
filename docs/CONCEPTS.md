# Spacegate Concept Pages

Spacegate concept pages are public educational pages for turning object tags,
system-page terms, and map/search discoveries into readable science paths.
They are part of the Spacegate public UX, not canonical catalog evidence.

Planned route:

- `/concepts/:slug`

Concept pages should be linked from stellar-class pills, discovery tags,
tooltips, system pages, Star Search filters, and future AAA-reviewed
narration. A concept page should never blur the boundary between source facts,
ARM derivations, DISC narration/assumptions, and RIM fiction or user overlays.

## Educational Intent

Spacegate should make it easy for a visitor to move from a familiar surface
idea to real astrophysics without feeling pushed into a textbook. A user might
arrive because they searched for Tau Ceti, clicked a tag for a K-type star,
read a tooltip about habitable zones, opened a concept page about stellar
lifetimes, then eventually find themselves reading about proton-proton fusion,
CNO catalyzed fusion, stellar metallicity, and why mass dominates a star's
fate.

The teaching path should be layered:

1. **Tag or pill:** one or two words, color-coded, visible in tight UI.
2. **Tooltip or popover:** one readable paragraph that explains the idea with
   no prerequisite jargon.
3. **Concept page opening:** plain-language explanation, examples, and why the
   idea matters in Spacegate.
4. **Deeper science:** several sections that introduce the real physical
   mechanism, caveats, equations only where helpful, and observational limits.
5. **Interactive visualization:** levers and knobs that let users play with
   the concept when a simulation would teach better than prose.
6. **Discovery path:** representative systems, related concepts, and a
   Star Search link such as "Find more white dwarfs" or "Find systems with
   eccentric planets."

The goal is not to hide complexity. The goal is to stage complexity so that
curiosity naturally deepens.

## Page Contract

Each concept page should include:

- a one-screen plain-language explanation
- a deeper science section for lay astronomers
- representative Spacegate systems and objects
- related concepts
- a "Find more" Star Search link with the relevant filter active
- evidence and limitations where the concept is observationally fuzzy
- optional interactive visualization when the concept benefits from play

Concept content can begin as static reviewed editorial text, then later accept
AAA-generated drafts after review. Generated narration belongs in DISC or a
reviewed publication layer, not in core or ARM science tables.

## Tone And Structure

Concept pages should sound like an expert guide pointing at the sky, not a raw
catalog entry. Use concrete examples first, then introduce the technical name.
Avoid implying more certainty than the observations support. Prefer "this can
mean" and "astronomers infer" where the catalog evidence is indirect.

Recommended page anatomy:

1. **What it is:** accessible explanation in one short screen.
2. **Why it matters:** why an explorer, worldbuilder, or lay astronomer should
   care.
3. **How astronomers know:** observation methods and uncertainty.
4. **How it appears in Spacegate:** which tags, filters, pages, and simulator
   overlays use the concept.
5. **Representative systems:** examples from public UX goldens and nearby
   interesting objects.
6. **Play with it:** optional visualization or simulator mode.
7. **Go deeper:** related concepts in an intentional learning path.
8. **Find more:** Star Search query/filter link.

## Tag And Tooltip Contract

Tags are the first rung of the concept ladder. They should be compact enough
for Search cards, Map overlays, System Simulation object chips, and hierarchy
rows, but educational enough to invite clicks.

Each public tag should eventually define:

- `label`: compact text shown in UI
- `slug`: concept page route slug
- `priority`: compact / normal / expanded display tier
- `category`: stellar class, planet type, orbit, activity, evidence,
  habitability, chemistry, system architecture, or worldbuilding
- `tooltip`: one accessible paragraph
- `short_tooltip`: optional tight-space version
- `find_more_params`: Star Search filters or query parameters
- `representative_systems`: examples for pages and tests
- `source_policy`: source fact, ARM-derived fact, DISC presentation
  assumption, or RIM/user overlay

Tooltips should be useful even when the user never opens the concept page.
Concept pages should reward the click with substantially more context, not
repeat the tooltip.

## Learning Paths

These are intentionally staged routes from common public questions toward
deeper science.

### From Star Color To Fusion

- Spectral class
- Effective temperature
- Main sequence
- Stellar mass
- Proton-proton chain
- CNO catalyzed fusion
- Stellar lifetime
- Red giant evolution
- White dwarf / neutron star / black hole endpoints

This is the path for turning "why is this star red?" into "why do massive stars
burn by the CNO cycle and die young?"

### From Habitable Zone To Planetary Climate

- Habitable zone
- Stellar luminosity
- Planet atmosphere
- Tidal locking
- Stellar flare
- Magnetic field
- Eccentricity
- Habitable-zone disruption
- Biosignature caveats

This is the path for turning "is this planet habitable?" into a careful
explanation of why the habitable zone is a useful screen, not a promise.

### From System Picture To Orbital Mechanics

- Orbital period
- Semi-major axis
- Eccentricity
- Inclination
- Barycenter
- Binary star
- Multi-star hierarchy
- Orbital resonance
- Dynamical stability

This is the path for turning System Simulation visuals into real orbital
intuition.

### From Weird Worlds To Planet Formation

- Exoplanet
- Terrestrial planet
- Gas giant
- Ice giant
- Super-Earth
- Mini-Neptune
- Super-puff planet
- Snow line
- Soot line
- Migration
- Atmospheric escape

This is the path for explaining strange exoplanets without implying that all
worlds look like Solar System analogues.

### From Compact Objects To Extreme Physics

- White dwarf
- Chandrasekhar limit
- Neutron star
- Pulsar
- Magnetar
- Black hole
- Accretion disk
- Nova
- Supernova
- Core collapse

This is the path for turning a compact-object tag into density, degeneracy,
relativity, and stellar death.

## First Pages To Build

These are the best first candidates because they already appear in public UI or
would clarify common user questions:

- Spectral Class
- Habitable Zone
- White Dwarf
- Brown Dwarf
- Binary Star
- Flare Star
- Eccentricity
- Super-Puff Planet
- Exoplanet
- Neutron Star
- Black Hole
- Main Sequence
- Stellar Mass
- Orbital Period
- Semi-major Axis
- Inclination

## Concept Backlog

### Stellar Classes And Remnants

- O-type star
- B-type star
- A-type star
- F-type star
- G-type star
- K-type star
- M-type star
- L dwarf
- T dwarf
- Y dwarf
- Wolf-Rayet star
- White dwarf
- Neutron star
- Pulsar
- Magnetar
- Black hole
- Brown dwarf
- Ultracool dwarf
- Unknown stellar class

### Stellar Behavior And Lifecycle

- Main sequence
- Stellar mass
- Stellar lifetime
- Proton-proton chain
- CNO catalyzed fusion
- Red giant
- Planetary nebula
- Supernova
- Core collapse
- Chandrasekhar limit
- Accretion disk
- Stellar wind
- Solar flare
- Coronal mass ejection
- Flare star
- Variable star
- Cataclysmic variable

### Orbits And System Architecture

- Orbital period
- Semi-major axis
- Eccentricity
- Inclination
- Barycenter
- Orbital resonance
- Tidal locking
- Multi-star hierarchy
- Binary star
- Eclipsing binary
- Habitable-zone disruption

### Planet And Disk Context

- Exoplanet
- Terrestrial planet
- Gas giant
- Ice giant
- Super-Earth
- Mini-Neptune
- Super-puff planet
- Hot Jupiter
- Rogue planet
- Habitable zone
- Vaporization line
- Soot line
- Water freeze line / snow line
- Carbon dioxide freeze line
- Methane and carbon monoxide freeze line
- Nitrogen freeze line
- Debris disk

### Measurements And Coordinates

- Light-year
- Parsec
- Parallax
- Proper motion
- Radial velocity
- Right ascension and declination
- ICRS
- Galactic coordinates
- Absolute magnitude
- Apparent magnitude
- Effective temperature
- Luminosity

## Candidate Interactive Visualizations

- Super-puff planet: move a close, low-density planet inward and watch its
  diffuse envelope expand and stream away under stellar wind.
- Core-collapse model: remove support energy from a massive stellar core and
  show collapse, rebound, and supernova caveats.
- Compact-remnant mass slider: cross white-dwarf, neutron-star, and black-hole
  thresholds while explaining the Chandrasekhar limit and neutron-star maximum
  mass uncertainty.
- Eccentricity toy orbit: drag eccentricity from circular to elongated and show
  changing stellar energy over the orbit.
- Habitable-zone disruption: place a stellar companion on an eccentric orbit
  and show when a nominal habitable zone becomes dynamically hostile.
