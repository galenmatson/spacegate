# Spacegate Tag Vocabulary

This file is the expandable editorial proposal inventory for tags and related
concepts. It is intentionally broader than the active product vocabulary.
Implemented definitions live in the reviewed machine registry under
`config/tags/`; every family here must be recorded as enabled, deferred,
retired, or rejected in `config/tags/proposal_inventory.json`.

The v1 architecture, compiler, API, interaction contract, and RIM boundary are
defined in `docs/SMART_TAGS.md`. Missing data is never positive evidence for a
tag, and evocative prose here must be reviewed before it becomes public copy.

## Editorial Standard

Spacegate tags are the first step in an educational path, not merely database
labels. A public full tooltip should normally use three to six sentences and
assume an interested reader whose astronomy knowledge comes primarily from
school, documentaries, games, and science fiction. It should:

1. define the term in plain language;
2. build physical intuition for why the phenomenon occurs or matters;
3. explain what observation, selected fact, or versioned screen activates the
   tag in Spacegate; and
4. incorporate important uncertainty or non-claims into the explanation
   naturally rather than appending a perfunctory disclaimer.

Vivid language is welcome when the physics earns it. Superlatives, hazards,
surface conditions, evolutionary outcomes, and implications for life must not
be invented from a broader proxy. Compact surfaces use the separate
`short_tooltip`; the full tooltip should teach something worth opening.

Source tokens follow the same educational standard but explain what the source
observed or compiled, how that evidence helps Spacegate, and where its scope
ends. They must never imply that catalog membership makes every value certain
or that a source is authoritative outside the scientific domain it measures.

## Tag Color Schema

Tag colors should teach category at a glance without becoming the only carrier
of meaning. Every tag still needs a text label, tooltip, and eventually a
concept-page link where useful.

- Stellar spectral and physical classes use physically suggestive colors: O/B
  blue, A white, F pale yellow-white, G yellow, K orange, M red, L/T/Y deep
  infrared magenta/indigo, WD white/cyan, NS/PULSAR/MAGNETAR electric
  cyan-violet, and BLACK HOLE black with a bright rim.
- Planet, habitability, and chemistry tags use green/teal for habitability and
  biosignature-adjacent context, cyan for water/ice, orange/red for hot inner
  thresholds, and muted blue-gray for cold outer-system chemistry.
- System architecture tags such as Single, Binary, Multiple, Planet Host, and
  Circumbinary use blue families.
- Motion and dynamics tags such as High Proper Motion, Runaway, Hypervelocity,
  Resonant, Eccentric, and Inclined use violet/purple families.
- Activity, hazard, and transient tags such as Flare Star, X-Ray Source, Nova,
  Supernova, Magnetar, and Radiation Hazard use red/orange families.
- Evidence/provenance tags keep the existing semantic palette: SOURCE blue,
  DERIVED cyan, ASSUMED amber, MISSING gray.
- Catalog, survey, and admin/review tags use restrained neutral/slate colors so
  they do not compete with object-science tags.
- RIM, lore, and user/worldbuilding tags must use a visually distinct palette
  from science tags so fiction and user overlays are never mistaken for
  canonical or derived astronomy.

## Broad Planet Map Tags

The map's glance-level planet vocabulary is a deterministic 3x3 screen for
confirmed planets. Rows encode broad bulk scale (Giant, Neptunian,
Terrestrial); columns encode the selected temperature proxy (Hot, Temperate,
Cold). These are derived navigation categories, not source-supplied planet
types and not claims about atmosphere, composition, surface conditions, or
habitability.

| | Hot | Temperate | Cold |
|---|---|---|---|
| Giant | `science:planet.hot_gas_giant` | `science:planet.temperate_gas_giant` | `science:planet.cold_gas_giant` |
| Neptunian | `science:planet.hot_neptunian` | `science:planet.temperate_neptunian` | `science:planet.cold_neptunian` |
| Terrestrial | `science:planet.hot_terrestrial` | `science:planet.temperate_terrestrial` | `science:planet.cold_terrestrial` |

Bulk scale prefers selected radius: no more than 2 Earth radii is terrestrial,
2 to 6 Earth radii is Neptunian, and at least 6 Earth radii is giant. When
radius is absent, selected mass is used: no more than 10 Earth masses is
terrestrial, 10 to 50 Earth masses is Neptunian, and at least 50 Earth masses
is giant. The temperature screen uses selected equilibrium temperature, or an
insolation-derived proxy when available: above 320 K is hot, 200 to 320 K is
temperate, and below 200 K is cold. Boundary behavior and exact SQL are pinned
in `srv/api/app/planet_categories.py`.

The word Neptunian intentionally means the intermediate radius/mass bin. It
does not prove an ice-giant composition. Likewise, the giant glyph's annulus
visually distinguishes the broad category and is not evidence for rings.

## Orbital Characteristics

These explain orbital attributes so users understand what it takes to create an orbital solution.

### ORBITAL PERIOD

An orbital period is the time required for one complete revolution around a companion or shared barycenter. Astronomers can measure it from repeating transits, eclipses, radial-velocity shifts, timing signals, or changing positions on the sky; short periods may be known exquisitely well while a centuries-long visual orbit may remain uncertain. Together with the system's mass, the period sets the scale of the orbit through Kepler's laws. Spacegate should display the selected period with its source or fitted uncertainty so the simulator's rhythm remains tied to evidence.

### SEMIMAJOR AXIS

The semimajor axis is half the longest diameter of an elliptical orbit and is the standard measure of that orbit's physical scale. It is not the body's instantaneous distance and, for an eccentric orbit, is not simply the time-averaged separation. Binary-star catalogs may describe the relative orbit between two stars or the smaller barycentric orbit of one component, so component scope matters. In planetary systems it helps determine received stellar energy, but the star's luminosity, orbital eccentricity, and planetary atmosphere are needed before discussing climate.

### ECCENTRICITY

Eccentricity describes the shape of an orbit: zero is circular, values between zero and one are bound ellipses, and values near one are strongly elongated. A body on an eccentric orbit receives changing stellar energy and moves fastest near periastron, but the climatic result also depends on the star, atmosphere, oceans, rotation, and thermal inertia. Eccentricity can preserve clues to migration and gravitational encounters without identifying one unique history. Spacegate should present the fitted value and uncertainty rather than turning every nonzero orbit into an extreme-world label.

### INCLINATION

Inclination measures an orbit's tilt relative to a named reference plane. Exoplanet and binary catalogs may measure against the plane of the sky, while a system visualization may compare one orbit with a chosen system plane; those angles answer different questions and cannot be silently exchanged. Near-edge-on orbits are favorable for eclipses and transits, but mutual inclination between orbits is what reveals a warped system. Spacegate must state the reference frame and evidence behind any displayed inclination.

## Orbital Parameter Tags

### ULTRASHORT PERIOD

An ultra-short-period planet completes an entire orbit in less than one Earth day. Such a compact orbit places the planet close to its star, where intense irradiation, tidal forces, and atmospheric loss can reshape it, although the outcome depends on the star and the planet's composition. These systems test how planets migrate inward and how long they can survive near their stars. Spacegate applies the conventional one-day threshold to a confirmed planet's selected orbital period.

### CONTACT BINARY

A contact binary contains two stars orbiting so closely that both fill their Roche lobes and share an outer envelope. They remain distinct stellar cores rather than two solid surfaces simply touching, while gas and energy can flow through the common envelope and produce continuously changing light. Angular momentum loss and mass transfer may eventually drive a merger, but contact does not make the timing or outcome certain. Spacegate should activate this tag from an accepted contact-binary classification or suitable light-curve and orbital evidence, not from a short period alone.

### HIERARCHICAL

A system of three or more stars is difficult to preserve when every star tugs strongly on every other at comparable distances. Long-lived multiples usually arrange themselves as nested orbits: a close pair acts almost like one combined mass to a much more distant companion, while still wider members can orbit the inner group. This separation of scales prevents many of the close encounters that would scramble or eject stars. Spacegate applies HIERARCHICAL when accepted component relationships contain a stellar subgroup inside a larger system, revealing the architecture that can make a complex stellar family durable.

### TROJAN

A Trojan shares a 1:1 orbital resonance with a more massive body and librates around a stable region near the leading or trailing Lagrange points. It does not follow the exact same path at a fixed distance: the smaller body oscillates around the equilibrium region, and other kinds of co-orbital motion also exist. Jupiter's Trojan asteroids show how this geometry can preserve large populations for long periods. A Spacegate tag would require evidence for the resonant behavior, not merely two objects with similar reported periods.

### ROGUE

A rogue planet or planetary-mass object travels through space without being gravitationally bound to a star. Some may have been ejected from young planetary systems, while others may have formed more like isolated brown dwarfs, so freedom from a host does not reveal a single origin. Internal heat can persist after starlight is gone, leaving atmospheres or subsurface environments as physical possibilities rather than known properties. Spacegate must require positive evidence for an unbound object; a missing host, orbit, or catalog relationship is never enough.

### ECCENTRIC

An eccentric tag would call attention to an orbit whose measured shape departs substantially from a circle. The scientific distinction needs a reviewed threshold, uncertainty rule, and target-specific context because a value important for a compact planet system may be ordinary for a wide binary. Eccentric orbits can produce dramatic changes in separation and interaction strength without proving a violent origin or an unstable future. Spacegate should show the value and reference solution alongside the tag so the distinction remains inspectable.

### INCLINED

An inclined tag would identify an orbit tilted substantially relative to a specified comparison plane. That might mean the plane of the sky, a dominant planetary plane, a stellar equator, or another accepted orbit, and each choice carries a different physical meaning. Large mutual inclinations can preserve evidence of migration, scattering, capture, or disk warping, but the angle alone does not select among those histories. Spacegate must establish the reference plane, threshold, and uncertainty before enabling this distinction.

### EDGE ON

An edge-on orbit is viewed nearly along its orbital plane, so the moving bodies can pass in front of one another from Earth's perspective. This geometry makes transits and eclipses possible and lets astronomers recover radii, periods, and other properties from changes in light. The alignment need not be perfect, and the angular tolerance depends on the sizes and separation of the bodies. Spacegate should tie the tag to a defined sky-plane inclination or observed eclipse evidence rather than the visual impression of the simulator.

### RETROGRADE

A retrograde orbit carries angular momentum opposite to a named reference rotation or orbital plane. Such motion can emerge through scattering, secular interactions, a tilted formation disk, mass transfer, or capture, so it does not automatically mark an interstellar intruder. Retrograde planets are often identified through the projected angle between a transit chord and the star's rotation, which may not reveal the complete three-dimensional geometry. Spacegate needs an explicit reference direction and uncertainty policy before assigning this tag.

### RESONANT

Orbital resonance occurs when repeated gravitational tugs happen in a regular pattern, often associated with a ratio of orbital periods built from small integers. A near-integer ratio is a clue, but true resonance is normally demonstrated when a resonant angle librates instead of circulating freely. Resonances can stabilize an architecture, drive eccentricity, or heat an interior, depending on the bodies and geometry involved. Spacegate should distinguish a measured or modeled resonance from a period-ratio screen.

### CIRCUMBINARY

A circumbinary body orbits both members of an inner binary rather than belonging to either star alone. From far enough away, the pair's combined gravity can support a durable outer orbit, while paths too near the binary are often destabilized by its changing gravitational field. The resulting seasons, transits, and illumination can be complex even when the orbit itself is stable. Spacegate should apply this tag from an accepted host relationship and preserve whether the orbit is measured, modeled, or assumed.

### TIDALLY LOCKED

A tidally locked body rotates once per orbit so the same hemisphere continually faces its companion. Tides dissipate rotational energy until this synchronized state becomes favorable, especially for close planets and moons, but the timescale depends on internal structure and orbital history. Permanent day and night do not by themselves dictate a scorched hemisphere and frozen one: an atmosphere, ocean, clouds, and circulation can redistribute heat. Because exoplanet rotation is rarely measured directly, Spacegate should distinguish an observed constraint from a model-based locking expectation.

## Temperature Lines

### Vaporization Line

The refractory-vaporization boundary marks the hot inner disk region where even many rock-forming solids cannot survive. Its location depends on pressure, mineral composition, stellar luminosity, and the changing structure of the young disk, so it is a modeled transition rather than a universal ring at one temperature. Beyond it, refractory grains can remain solid long enough to participate in planet formation. A Spacegate overlay should name its model and assumptions instead of implying that present-day planets formed at their current positions.

### Soot Line

The soot line is a modeled region inside which heat destroys or alters refractory carbon-rich grains. Crossing it can change the carbon available to forming planetesimals and may help explain differences in planetary composition. The relevant chemistry and temperature depend on the disk environment, and the line evolves as the young star and disk change. Spacegate should present it as a formation-model concept, not a fixed boundary proving how carbon reached a particular planet.

### Water Freeze Line

The water snow line is the disk region beyond which water vapor can condense efficiently as ice. Icy grains add solid material and change how planetesimals grow, making the snow line important to theories of giant-planet cores and water delivery. Its location moves as the young star brightens or dims and as disk pressure and opacity evolve. A present-day orbit beyond a rendered snow line does not prove that the planet formed there or reveal how much water it contains.

### Carbon Dioxide Freeze Line

Farther into a cold protoplanetary disk, carbon dioxide can condense onto grains and alter the inventory of volatile material available to forming worlds. The transition depends on local pressure, chemistry, and disk history rather than one exact temperature everywhere. Migration and later heating can separate a planet's current orbit from the region where its building blocks condensed. Spacegate should render this as a model-dependent chemistry overlay with its assumptions available.

### Methane & Carbon Monoxide Freeze Line

Methane and carbon monoxide condense under different chemical and pressure conditions in the cold outer disk, so they should not ultimately share one undifferentiated physical boundary. Both transitions matter because they change where carbon-bearing volatiles remain in gas or become incorporated into icy solids. The locations evolve with disk temperature and may be altered by chemical reactions and irradiation. This combined proposal should remain a teaching placeholder until Spacegate adopts separate model definitions.

### Nitrogen Freeze Line

The nitrogen condensation region lies among the coldest parts of a planet-forming disk, where nitrogen-bearing volatiles can be trapped in ices. Its location depends on whether nitrogen is present as molecular nitrogen, ammonia, or other compounds as well as on pressure and disk chemistry. Pluto's nitrogen ice shows that such material survives in today's outer Solar System, but it does not locate a universal formation boundary. Spacegate should expose the chosen chemical model whenever this line is rendered.

### Habitable Zone

The circumstellar habitable zone is the range of stellar energy where a rocky planet with a suitable atmosphere might maintain liquid water on its surface. It is a family of climate-model boundaries, not a precise promise written into space: atmospheric composition, pressure, clouds, planetary mass, orbit, and stellar activity all matter. The zone also moves as a star evolves. Spacegate uses a deliberately broad HZ SCREEN to identify systems worth investigating while reserving any claim about actual surface water or life for much stronger evidence.

## Galactic Reference Frame

### COREWARD

Coreward points from Sol toward the center of the Milky Way in a specified Galactic coordinate frame. Flying that way looks into denser regions of the Galactic disk, though dust and survey selection can strongly shape what appears on the map. A directional tag would describe position on the sky, not prove that an object belongs to the bulge or is physically moving inward. Spacegate needs a versioned frame and angular boundary before assigning it.

### RIMWARD

Rimward points away from the Galactic center toward the outer disk. The direction provides useful orientation for exploring the Milky Way from Sol, but nearby stars labeled rimward may still have very different Galactic orbits. Survey depth and dust can make the apparent stellar density differ from the true population. Spacegate must define the coordinate frame and angular region before this becomes a tag.

### SPINWARD

Spinward points along the direction of the Milky Way's rotation at the Sun's location. It is a navigational direction in a Galactocentric frame, not a claim that every star in that part of the sky moves with the local circular flow. Peculiar velocities and the Sun's own motion complicate the relationship between sightline and motion. Spacegate needs a pinned Galactic model and angular boundary before enabling the label.

### TRAILING

Trailing points opposite the local direction of Galactic rotation. Like spinward, it gives explorers a useful orientation while saying nothing by itself about an individual star's velocity. A star seen in the trailing hemisphere may be moving in almost any direction relative to Sol. Spacegate should keep the positional direction separate from kinematic evidence.

### GALACTIC ZENITH

Galactic zenith points toward the north Galactic pole, above the Milky Way's midplane in the adopted coordinate system. Looking this way leaves the dusty disk relatively quickly and opens a view toward the Galactic halo and distant universe. Nearby objects in that direction are not automatically halo members or physically traveling upward. Spacegate needs an explicit Galactic frame and angular region for the tag.

### GALACTIC NADIR

Galactic nadir points toward the south Galactic pole, below the Milky Way's midplane in the adopted coordinate system. It is the opposite navigational direction from Galactic zenith and likewise looks out of the dense disk. Position in that hemisphere does not establish population membership or vertical motion. Spacegate should define it through the same versioned Galactic frame as the other directional tags.

## Equatorial & Observational Tags

### NORTH

North describes positive declination in a pinned equatorial coordinate frame. It does not mean an object is visible only from Earth's Northern Hemisphere: observers in both hemispheres can see across much of the celestial equator depending on latitude and horizon. The label is useful for organizing the sky but should not be confused with Galactic north or an object's physical location above the Milky Way. Spacegate must state the frame and epoch.

### SOUTH

South describes negative declination in a pinned equatorial coordinate frame. Many southern objects are visible from northern latitudes and vice versa, with the observer's latitude setting the actual limit. Equatorial south is a direction on Earth's sky rather than a Galactic population or motion claim. Spacegate should keep this coordinate label separate from live visibility calculations.

### CIRCUMPOLAR

A circumpolar object never sets below the horizon for a particular observer because its daily path circles a celestial pole. The result depends directly on observer latitude: a star can be circumpolar from one location, seasonal from another, and never rise from a third. This makes circumpolar an excellent live observing aid but not an intrinsic property of the star. Spacegate should calculate it from explicit location and time context instead of storing it permanently.

### ZODIACAL

A zodiacal object lies within a defined band around the ecliptic, the Sun's apparent yearly path across Earth's sky. The Moon and planets remain near this band, so stars and extended objects there are more likely to experience conjunctions or occultations. The ecliptic crosses thirteen modern IAU constellations, while cultural zodiac traditions divide the sky differently. Spacegate needs an explicit band width, epoch, and cultural context before assigning the tag.

## Kinematic (Motion) Tags

### HIGH PROPER MOTION

Proper motion is the angular drift of an object across the sky after annual parallax is separated out. A large value often calls attention to a nearby star, but a more distant object moving rapidly across our line of sight can also qualify. The apparent rate combines true tangential velocity with distance and carries measurement uncertainty. Spacegate needs a reviewed threshold and release-scoped astrometry before turning it into a distinction.

### APPROACHING

An approaching object has a negative radial velocity in a specified reference frame, meaning its line-of-sight distance is currently decreasing. Spectral lines reveal that motion through their Doppler shift, but the measurement says nothing about sideways velocity and does not imply a future encounter with the Solar System. Binary orbital motion can also move a star toward and away from us around a system average. Spacegate must define the velocity frame, systemic value, threshold, and uncertainty.

### RECEDING

A receding object has a positive radial velocity in a specified reference frame, meaning its line-of-sight distance is currently increasing. Its spectrum is shifted toward longer wavelengths by that motion, while proper motion supplies the missing sideways component. Receding does not mean the star is escaping the Galaxy or will quickly disappear from the sky. Spacegate should use a systemic radial velocity and explicit frame rather than a single phase of binary motion.

### CO-MOVING

Co-moving objects have three-dimensional velocities consistent within a defined tolerance after measurement uncertainties are considered. Similar motion can support a wide-binary, moving-group, or cluster relationship, but it does not by itself prove common birth or gravitational binding. Position, age, chemistry, and long-term dynamics provide additional tests. Spacegate needs a versioned velocity metric and reference population before assigning this tag.

## Source and Evidence

### The Canonical Backbone & Astrometry

#### Gaia DR3

Gaia repeatedly measured nearly two billion points of light, turning tiny shifts on the sky into positions, parallaxes, proper motions, brightnesses, and colors. Those measurements form the geometric backbone of Spacegate's nearby map and help separate stars that only appear close together from objects moving through space together. This token appears when an exact Gaia DR3 record contributes to the displayed object; distance estimates and physical interpretations remain distinct evidence.

#### Bailer-Jones

Parallax becomes difficult to translate directly into distance when the measurement is small or uncertain. The Bailer-Jones catalog combines Gaia EDR3 astrometry, and for photogeometric estimates stellar brightness and color, with an explicit model of the Galaxy to produce probability-based distance estimates. Spacegate preserves these as modeled evidence rather than treating them as exact measured locations.

### Planets & Exoplanet Habitability

#### NASA Exoplanet Archive

The NASA Exoplanet Archive gathers published discoveries and measurements from many planet-search methods into a curated, evolving record. It contributes confirmed-planet identity, discovery context, and both reference-specific and composite values for planets and their host stars. Spacegate preserves the references and uncertainties behind those values because a convenient composite is not a substitute for the evidence that produced it.

#### TOI

TESS searches for repeated dips in starlight that may be caused by planets crossing their stars. TIC identifiers name the observed targets, while TOI records track signals selected for follow-up and their changing dispositions. Spacegate connects that evidence to the correct host or component while preserving the difference between a candidate, a confirmed planet, a false positive, and a false alarm.

#### Habitable Worlds Catalog

The Planetary Habitability Laboratory's Habitable Worlds Catalog collects published planet data and applies external habitability-oriented screens and metrics. It is useful for finding worlds that merit closer examination and for comparing how different screening assumptions behave. Spacegate presents it as a published assessment from that source, never as proof that a planet supports liquid water or life.

### Multiplicity & Orbital Dynamics

#### WDS

The Washington Double Star Catalog is a long-running record of pairs and multiple components measured close together on the sky. It preserves component labels, relative positions, and observation histories that may span generations of observers. A WDS pair is not automatically gravitationally bound, so Spacegate uses the measurements as relation evidence and requires stronger support before turning proximity into canonical system membership.

#### MSC

The Multiple Star Catalog reconstructs how the components of triples and richer stellar families are arranged into nested subsystems. That hierarchy is essential because a close pair and a distant companion cannot be represented faithfully as three unrelated points or one flat list. Spacegate preserves MSC component scope and source-native orbit context, accepting only relationships that survive identity and collision checks.

#### ORB6

ORB6 collects published orbit solutions for visual binaries whose changing separation and position angle can be followed on the sky. A solution turns those measurements into one coherent set of orbital elements, but its reliability depends on how much of the orbit has been observed and is summarized by its grade and references. Spacegate preserves the complete solution and its provenance rather than mixing convenient elements from unrelated fits.

#### SB9

Spectroscopic binaries reveal their orbits through periodic Doppler shifts even when telescopes cannot cleanly separate the stars. SB9 gathers published radial-velocity orbit solutions, aliases, component information, and bibliography for these systems. Spacegate keeps each solution coherent and component-scoped so a measured period or velocity amplitude is not silently attached to the wrong star or combined with an incompatible orbit.

#### DEBCat

Detached eclipsing binaries let astronomers combine eclipses with radial velocities to measure stellar masses and radii with unusual precision. DEBCat curates well-studied systems, generally emphasizing measurements near the two-percent level while preserving published uncertainties and references. These stars are powerful tests of stellar models, but the catalog is a literature compilation rather than a perfectly homogeneous experiment.

### Nomenclature & Publications

#### SIMBAD

Astronomical objects accumulate names as different surveys, instruments, and papers observe them. SIMBAD links those identifiers to literature-reviewed objects and supplies object types, classifications, and bibliography, making it one of astronomy's essential cross-reference services. Spacegate uses individual SIMBAD contributions with object and component scope instead of treating every alias or historical classification as equally authoritative.

#### IAU WGSN

The IAU Working Group on Star Names standardizes proper names so one spelling and one stellar component can be used consistently across astronomy. Its catalog also records the historical or cultural attribution behind those names. Spacegate prefers an accepted proper name where appropriate while retaining catalog identifiers and never treating a named star as scientifically more important merely because it has a memorable name.

#### Harvard ADS

The Astrophysics Data System is a digital library portal operated by the Smithsonian Astrophysical Observatory and funded by NASA. It indexes virtually every piece of peer-reviewed astronomical literature, preprint, and scientific paper ever published. When this tag appears, it means Spacegate's AI agents have bypassed standard databases and extracted specific, bleeding-edge facts directly from a published scientific paper. This represents the narrative, evidence-backed enrichment layer of the platform, where raw data meets human scientific interpretation. It is the gold standard for tracing a system's parameters back to the exact scientists who discovered them.

### Compact & Ultracool Objects

#### ATNF Pulsar

Pulsars announce their rotation through precisely timed pulses detected at radio and other wavelengths. The ATNF Pulsar Catalogue brings together published identities, spin behavior, distances, timing parameters, and binary context for these objects. Spacegate preserves the measurement and reference behind each contribution because derived quantities such as characteristic age or magnetic field depend on a physical model.

#### McGill Magnetar

Magnetars are neutron stars recognized through their high-energy activity, spin behavior, and exceptionally strong inferred magnetic fields. The McGill Magnetar Catalog reviews the small known population and assembles timing, burst, and observational context from multiple missions. Spacegate uses that reviewed classification while keeping model-derived field strengths and ages distinct from direct observations.

#### UltracoolSheet

Ultracool dwarfs are faint enough that their identities and classifications are scattered across infrared surveys and specialist papers. UltracoolSheet links that literature into a practical compilation of optical and infrared spectral types, astrometry, and related measurements. Spacegate uses the exact classifications and references while respecting that spectral type, mass, and star-versus-brown-dwarf status are different questions.

### Deep Sky, Infrared, & Spectroscopy

#### APOGEE

APOGEE uses high-resolution near-infrared spectra to look through Galactic dust and read the absorption fingerprints in stellar atmospheres. DR17 contributes radial velocities, temperatures, surface gravities, and abundances for many chemical elements, together with the flags needed to judge those results. Spacegate keeps each calibrated parameter set and its quality context intact rather than constructing a chemically precise star from unrelated best-looking numbers.

#### OpenNGC

Stars live within a larger geography of clusters, nebulae, galaxies, and remnants rather than in an empty coordinate grid. OpenNGC and the companion nebula catalogs contribute names, positions, dimensions, object types, and aliases for those extended structures. Spacegate stores the structures separately from stellar systems and requires explicit evidence before claiming that a particular star belongs to, illuminates, or lies inside one.

#### Cantat-Gaudin

Open clusters are families of stars that formed together and still share related positions and motions through the Galaxy. Cantat-Gaudin and collaborators used Gaia astrometry to identify cluster populations and assign probability-bearing memberships. Spacegate preserves those probabilities and the Gaia-release context, because a likely co-moving member is evidence of association rather than an infallible family label.

#### CatWISE2020

WISE and NEOWISE repeatedly scanned the whole sky in infrared light, making cold and dusty objects visible and revealing their motion over time. Targeted CatWISE2020 records contribute positions, proper motions, and infrared photometry for matched cool or fast-moving objects in Spacegate. The token means that exact counterpart evidence contributes here, not that every property of the object was discovered by CatWISE.

### Solar System & Transitional

#### JPL Horizons

Solar System positions change continuously and depend on the requested time, coordinate frame, and observing location. JPL Horizons computes ephemerides, state vectors, and osculating elements from observation-based dynamical solutions for planets, moons, small bodies, and spacecraft. Spacegate pins the query, epoch, center, frame, and returned evidence so a rendered position can be reproduced rather than treated as a timeless coordinate.

#### AT-HYG

AT-HYG combines several classic bright- and nearby-star catalogs that powered many earlier digital star maps. Modern Gaia astrometry usually provides a stronger geometric foundation, but the older compilation remains valuable for familiar names, historical identifiers, and compatibility with previous datasets. Spacegate uses those identity contributions without allowing transitional measurements to overrule better release-scoped evidence.

## Stellar Tags

### Spectral and Compact Classes

#### O (O-Type Star)

O-type stars are the hottest members of the familiar OBAFGKM spectral sequence, with blue light and strong ionizing ultraviolet radiation. Their enormous luminosity comes at a cost: massive O stars consume their nuclear fuel quickly and usually live for only a few million years. Their radiation and winds can carve cavities in nearby gas, light up nebulae, and influence the birth of other stars. Spacegate applies this tag from the selected spectrum or stellar classification; the letter describes the observed atmosphere and temperature class, while luminosity class and evolutionary state provide the rest of the story.

#### B (B-Type Star)

B-type stars are hot, blue-white stars whose spectra place them just below O stars in temperature. Many are several times the Sun's mass and shine so intensely that they remain conspicuous across hundreds of light-years. They burn their fuel faster than cooler stars and are common in young clusters and stellar associations, although a B spectrum alone does not prove a star is young or on the main sequence. Spacegate uses the selected stellar classification for this tag rather than inferring it from color alone.

#### A (A-Type Star)

A-type stars appear white or blue-white and are best known for the strong hydrogen absorption lines in their spectra. They are hotter and usually more massive than the Sun, so a typical A star has a shorter main-sequence lifetime and a brighter ultraviolet environment. Sirius A and Vega are nearby examples, but the class also includes stars at different evolutionary stages and some chemically or magnetically peculiar objects. Spacegate assigns the tag from a selected spectral classification, not from brightness or apparent color by itself.

#### F (F-Type Star)

F-type stars are yellow-white stars that bridge the gap between hot A stars and Sun-like G stars. A typical main-sequence F star is somewhat hotter and more massive than the Sun, radiates a larger share of ultraviolet light, and uses its fuel more quickly. This makes F-star planetary systems interesting laboratories for how stellar lifetime and radiation shape planetary environments. Spacegate derives this tag from the selected spectral classification; the F label does not by itself specify the star's age, size, or evolutionary stage.

#### G (G-Type Star)

G-type stars are yellow-white stars in the same broad spectral class as the Sun. Their spectra reveal surface temperatures and absorption features produced in their atmospheres; on the main sequence they can burn hydrogen steadily for billions of years. The Sun makes this class familiar, but G-type stars also include evolved subgiants and giants with very different sizes and histories. Spacegate applies the tag from the selected classification, so it means Sun-like in spectral type, not necessarily Sun-like in age, planets, or habitability.

#### K (K-Type Star)

K-type stars have orange light and cooler atmospheres than the Sun. Main-sequence K dwarfs consume hydrogen slowly and can remain stable for longer than G dwarfs, which makes their planetary systems especially interesting to astrobiologists. The same K spectrum can also belong to an evolved giant, so temperature class must not be confused with size or age. Spacegate assigns this tag from the selected stellar classification and presents luminosity or evolutionary information separately when it is known.

#### M (M-Type Star)

M is the coolest major class in the traditional stellar sequence, producing red light and spectra rich in molecules. Most stars in the Galaxy are small M dwarfs, which burn fuel slowly enough to outlive the present age of the universe, but young M dwarfs can also flare powerfully. Large evolved red giants may have the same M-type atmosphere even though their size and history are entirely different. Spacegate therefore uses this tag only for the selected spectral class and keeps dwarf, giant, activity, and age claims separate.

#### L (L-Type Brown Dwarf)

L-type objects are so cool that their spectra are shaped by metal hydrides, alkali lines, and clouds of mineral condensates. Most are brown dwarfs that never achieved sustained hydrogen fusion, but the warmest L objects can be extremely low-mass stars. Their faint visible glow gives way to much stronger infrared emission, where their weather-filled atmospheres can be studied. Spacegate calls them ultracool objects because an L spectrum alone does not settle which side of the star–brown-dwarf boundary an individual object occupies.

#### T (T-Type Brown Dwarf)

T-type brown dwarfs are substellar objects cooler than L dwarfs, with near-infrared spectra strongly shaped by methane and water absorption. They do not sustain ordinary hydrogen fusion, so they gradually radiate away the heat left from their formation. Their masses overlap those of giant planets, but they are classified through their spectra and formation context rather than simply by appearance. Spacegate applies this tag from an accepted T classification, usually built from infrared observations.

#### Y (Y-Type Brown Dwarf) (Added for completeness)

Y-type brown dwarfs occupy the coldest recognized brown-dwarf spectral class, with some known examples having temperatures comparable to those found on Earth. At such low temperatures their atmospheres can contain ammonia and water-bearing clouds, making them resemble giant planets more than conventional stars. They are extraordinarily faint in visible light and are found mainly through sensitive infrared surveys and nearby-motion searches. Spacegate uses an accepted Y classification; temperature, mass, and age remain separate measured or modeled quantities.

#### WR (Wolf-Rayet) (Added for completeness)

Wolf-Rayet stars are hot, evolved stars whose spectra are dominated by broad emission lines from powerful, fast-moving winds. Those winds have stripped away much of the outer star and exposed material altered by nuclear reactions, returning enriched gas to the surrounding interstellar medium. Many Wolf-Rayet stars began with very high masses, while some are produced through mass transfer in binary systems. Spacegate applies the tag from an accepted WR classification; the class signals an extreme evolutionary state without predicting the exact manner or timing of the star's death.

#### WD (White Dwarf)

A white dwarf is the compact remnant left when a low- or intermediate-mass star sheds its outer layers. Roughly a star's worth of mass can be compressed into a body about the size of Earth, supported not by ordinary gas pressure but by electron degeneracy pressure from quantum mechanics. With no sustained core fusion, it shines by releasing stored heat and cools over immense spans of time. Spacegate applies this tag only when selected evidence identifies the object as a white dwarf, keeping candidates and mass-based guesses distinct.

#### NS (Neutron Star)

A neutron star is the collapsed core left by some massive stars after core collapse. More than a Sun's worth of matter can be compressed into a sphere only tens of kilometers across, where atomic nuclei are forced into matter dominated by neutrons and the surface gravity is extreme. Some neutron stars appear as pulsars or magnetars, while others are difficult to detect at all. Spacegate applies this tag from accepted compact-object evidence rather than inferring a neutron star merely because a companion is unseen.

#### PULSAR

A pulsar is a rotating neutron star detected through remarkably regular pulses of radiation. Its magnetic field channels emission into beams that sweep through space; when a beam crosses Earth, telescopes record a pulse like the flash of a lighthouse. Pulse timing can reveal rotation, orbital motion, surrounding plasma, and even tiny disturbances in spacetime. Spacegate uses an accepted pulsar classification, while the broader neutron-star tag describes the underlying compact object.

#### MAGNETAR

A magnetar is a neutron star whose activity is powered largely by an exceptionally strong magnetic field. Stress in its crust and magnetic field can produce bursts of X-rays and gamma rays, and rare giant flares can briefly become visible across a large part of the Galaxy. Magnetars rotate and slow down in ways that help astronomers estimate their magnetic behavior, although the internal field cannot be measured directly. Spacegate applies this tag from accepted magnetar evidence and retains neutron star as the object's underlying physical class.

#### BLACK HOLE

A stellar-mass black hole forms when enough mass collapses inside an event horizon, a boundary from which light cannot return. The black hole itself emits no ordinary light, so astronomers infer it through effects such as a companion's orbit, gravitational waves, lensing, or radiation from nearby infalling matter. General relativity predicts a singularity inside, but present physics cannot tell us whether that mathematical prediction describes the true interior. Spacegate applies this tag only when accepted compact-object evidence supports a black-hole interpretation, not merely when a massive companion is invisible.

### 1. Evolutionary Stage

#### Protostar

- **Tooltip:** A protostar is a collapsing concentration of gas and dust that is still gathering mass. Gravity supplies most of its heat; sustained hydrogen fusion has not yet established the balance that defines a main-sequence star. Jets, disks, and dusty envelopes often make this stage easier to see in infrared light than in visible light. Spacegate should apply this tag only from accepted evolutionary-stage evidence, not merely because an object lies in a star-forming region.
- **Examples:** T Tauri (460 LY), HL Tauri (450 LY), V1009 Persei (850 LY)

#### Main Sequence

- **Tooltip:** A main-sequence star supports itself by fusing hydrogen into helium in its core. Fusion pressure and gravity settle into a long-lived balance, making this the stage in which stars spend most of their active lives. Massive stars burn much brighter and exhaust their fuel far sooner than low-mass stars, so main sequence does not imply one age, size, or color. Spacegate should derive this tag from an accepted luminosity class or evolutionary solution.
- **Examples:** The Sun (0 LY), Sirius A (8.6 LY), Tau Ceti (11.9 LY)

#### Subgiant

- **Tooltip:** A subgiant has begun leaving the main sequence after depleting much of the hydrogen available for fusion in its core. Hydrogen fusion continues mainly in a shell around the changing core, and the star expands while its surface generally cools. This is a transitional state, not simply a star whose radius happens to be somewhat larger than the Sun's. Spacegate should require an accepted luminosity class or evolutionary-stage estimate.
- **Examples:** Procyon A (11.4 LY), Beta Hydri (24 LY), Delta Eridani (29 LY)

#### Red Giant

- **Tooltip:** A red giant is an evolved star whose core structure has changed after central hydrogen fusion ended. Fusion in shells around the core can drive the outer atmosphere outward, producing a large radius and a cooler, reddish surface even as the star remains luminous. Later evolution depends strongly on mass and composition, so red giant is a broad stage rather than a single inevitable script. Spacegate should use accepted evolutionary or luminosity-class evidence.
- **Examples:** Arcturus (37 LY), Aldebaran (65 LY), Gacrux (88 LY)

#### Horizontal Branch

- **Tooltip:** A horizontal-branch star is a low-mass evolved star that has begun fusing helium in its core while hydrogen fusion continues in a surrounding shell. Such stars occupy a roughly horizontal feature on a cluster's color-magnitude diagram because similar luminosities appear across a range of temperatures. The name describes that population pattern, not a physical branch in space. Spacegate should apply it from an accepted evolutionary solution with suitable composition and population context.
- **Examples:** HD 109995 (800 LY), RR Lyrae (860 LY)

#### Asymptotic Giant Branch

- **Tooltip:** An asymptotic giant branch star has an inert core surrounded by helium- and hydrogen-burning shells beneath an enormous, cool envelope. Pulses in the helium-burning shell can alter the star's brightness and dredge newly formed elements toward the surface. Strong winds return gas and dust to interstellar space, but the details depend on the star's mass and composition. Spacegate should require accepted evolutionary-stage evidence rather than infer AGB status from red color alone.
- **Examples:** R Leonis (225 LY), Mira (299 LY), Chi Cygni (550 LY)

#### Post-AGB

- **Tooltip:** A post-AGB star is crossing the short evolutionary interval after the asymptotic giant branch and before the white-dwarf cooling track. Its envelope has been greatly reduced, exposing an increasingly hot core while previously expelled material moves away. If the timing and gas conditions are suitable, ultraviolet radiation can illuminate that material as a planetary nebula; not every object produces an easily visible one. Spacegate should use an accepted post-AGB classification.
- **Examples:** R Scuti (~850 LY), 89 Herculis (~1,000 LY), U Monocerotis (3,600 LY)

#### Wolf-Rayet

- **Tooltip:** A Wolf-Rayet star has a hot spectrum dominated by broad emission lines formed in a dense, fast stellar wind. Many are evolved massive stars whose winds or binary interaction have exposed helium- or heavier-element-rich layers, although some retain hydrogen. Their spectra reveal extreme mass loss and rapid evolution, but do not by themselves specify the star's final explosion or remnant. Spacegate should apply this tag from an accepted WR spectral classification.
- **Examples:** Gamma Velorum (1,090 LY), WR 104 (8,000 LY), Theta Muscae (7,400 LY)

### 2. Size & Luminosity Class

#### Hypergiant

- **Tooltip:** Hypergiant is a descriptive classification for exceptionally luminous, massive stars with evidence of atmospheric instability or extreme mass loss. It is not simply a synonym for the largest radius or a guarantee that the present mass exceeds a fixed threshold. These rare stars can change measurably as their dense winds remove material. Spacegate should require an accepted hypergiant classification rather than derive the label from luminosity alone.
- **Examples:** Rho Cassiopeiae (3,400 LY), VY Canis Majoris (3,900 LY), RW Cephei (11,500 LY)

#### Supergiant

- **Tooltip:** A supergiant has luminosity class I: its spectrum indicates a very extended, luminous atmosphere compared with a main-sequence star of similar temperature. Supergiants span blue through red temperatures, so the class does not describe one color or radius. Many evolved massive supergiants may eventually undergo core collapse, but classification alone does not fix the timing or outcome. Spacegate should use an accepted luminosity class.
- **Examples:** Polaris (430 LY), Antares (550 LY), Betelgeuse (642 LY)

#### Bright Giant

- **Tooltip:** A bright giant has luminosity class II, identified through spectral features sensitive to atmospheric pressure and gravity. It lies between ordinary giants and supergiants in the luminosity-class system, but that ordering is a classification rather than a universal evolutionary staircase. Stars of different masses can pass through this region by different routes. Spacegate should preserve the source luminosity class and its component scope.
- **Examples:** Canopus (310 LY), Epsilon Canis Majoris (430 LY), Alpha Persei (500 LY)

#### Giant

- **Tooltip:** A giant has luminosity class III: its spectrum indicates lower surface gravity and a more extended atmosphere than a main-sequence star of similar temperature. Many giants have left core hydrogen burning, but their internal fusion stages are not all the same. The tag therefore describes luminosity class, not a complete evolutionary diagnosis. Spacegate should use accepted classification evidence rather than infer it from radius alone.
- **Examples:** Pollux (34 LY), Arcturus (37 LY), Capella A (42 LY)

#### Subdwarf

- **Tooltip:** A cool subdwarf is less luminous than an ordinary main-sequence star of similar spectral type and is often old and metal-poor. Hot subdwarfs are a physically different family of compact evolved stars, so the word cannot be interpreted safely without its full classification. Their spectra and surface gravity provide the important evidence. Spacegate should keep the source subtype instead of flattening every subdwarf into one evolutionary story.
- **Examples:** Kapteyn's Star (12.8 LY), Mu Cassiopeiae (24.6 LY), Groombridge 1830 (29.9 LY)

#### Dwarf

- **Tooltip:** In luminosity classification, dwarf usually means luminosity class V, the main-sequence class that includes the Sun. The word compares atmospheric structure and luminosity with giants; it does not mean every dwarf is physically tiny. White dwarfs and brown dwarfs use the same word for entirely different kinds of objects. Spacegate should show the full class so those meanings remain distinct.
- **Examples:** The Sun (0 LY), Epsilon Eridani (10.5 LY), Tau Ceti (11.9 LY)

#### White Dwarf

- **Tooltip:** A white dwarf is an exposed stellar remnant supported mainly by electron degeneracy pressure rather than ongoing core fusion. It can contain a substantial fraction of the Sun's mass in a body roughly comparable to Earth in size, so its gravity and density are extreme. It shines as stored heat escapes and usually cools over very long times, while accretion or a companion can produce additional activity. Spacegate should apply the tag from accepted compact-object classification evidence.
- **Examples:** Sirius B (8.6 LY), Procyon B (11.4 LY), Van Maanen 2 (14.1 LY)

### 3. Explosive & Variable Events

#### Nova

- **Tooltip:** A classical nova is a thermonuclear eruption in material accumulated on a white dwarf from a companion. The runaway fusion brightens the binary enormously and ejects some of the surface layer, but unlike a supernova it normally leaves the white dwarf intact. A later eruption is possible if accretion resumes. Spacegate should distinguish an observed nova classification from a system merely capable of one.
- **Examples:** V603 Aquilae (815 LY), GK Persei (1,500 LY), DQ Herculis (1,600 LY)

##### Recurring

- **Tooltip:** A recurrent nova is a nova system with more than one recorded eruption. A relatively massive white dwarf, rapid accretion, and a small ignition layer can shorten the interval between eruptions, but recurrence times vary widely. Some are studied as possible Type Ia progenitor channels, which is a research question rather than a prediction for every system. Spacegate should require documented recurrent eruptions.
- **Examples:** T Coronae Borealis (3,000 LY), RS Ophiuchi (5,000 LY)

##### Micronova

- **Tooltip:** A micronova is a much smaller thermonuclear event thought to burn accreted material confined near a magnetic white dwarf's pole. It can release immense energy by human standards while involving far less material than a classical nova. The class is new and its physical interpretation is still being tested. Spacegate should require a published event classification rather than infer one from magnetic accretion alone.
- **Examples:** TV Columbae (1,200 LY), EI Ursae Majoris (Extragalactic)

#### Supernova

- **Tooltip:** Supernova is a family of stellar explosions, not one mechanism. Some result from the collapse of a massive stellar core; thermonuclear varieties disrupt part or all of a white dwarf. Their expanding ejecta and shocks reshape surrounding gas and distribute newly made or previously stored elements, but yields and remnants differ by subtype. Spacegate should attach this tag to accepted events or remnants with their subtype and evidence when known.
- **Examples (Historical Remnants):** Vela Pulsar (900 LY), Crab Nebula (6,500 LY)

##### Type Ia

- **Tooltip:** A Type Ia supernova is a thermonuclear explosion of a carbon-oxygen white dwarf in a binary system. Several progenitor routes are under study, including accretion and white-dwarf mergers, so one simple mass-transfer story is not sufficient. Their light curves can be standardized to measure cosmic distances, but they are not perfectly identical candles. Spacegate should preserve the observed subtype and cited event evidence.
- **Examples (Progenitor Candidates):** IK Pegasi (150 LY)

##### Type Iax

- **Tooltip:** Type Iax describes a diverse, generally fainter class of thermonuclear supernovae related to Type Ia events. Leading models involve incomplete burning, and some events may leave a bound white-dwarf remnant, but that outcome is not established for every member. Their range of brightness and spectra is part of what makes the class scientifically useful. Spacegate should use a published subtype rather than derive it from brightness alone.
- **Examples:** SN 2012Z (Extragalactic - NGC 1309)

##### Core Collapse

- **Tooltip:** Core collapse begins when a massive star can no longer support its central regions with energy-producing fusion. The inner core falls inward while neutrinos, shocks, rotation, and magnetic fields help determine whether the outer star is expelled. The remnant may be a neutron star or black hole, and some collapses may produce little visible explosion. Spacegate should reserve this tag for accepted event classifications or well-supported remnants.
- **Examples (Progenitor Candidates):** Spica (250 LY), Antares (550 LY), Betelgeuse (642 LY)

##### Pair Instability

- **Tooltip:** Pair instability can occur in an extremely massive, hot stellar core when energetic photons create electron-positron pairs and reduce radiation pressure. The contraction can trigger explosive oxygen burning; in the full pair-instability regime, models predict complete disruption with no compact remnant. A related pulsational regime ejects shells without necessarily destroying the star at once. Spacegate should require a published event or progenitor interpretation and preserve which regime is claimed.
- **Examples (Progenitor Candidate):** Eta Carinae (7,500 LY)

#### Hypernova

- **Tooltip:** Hypernova is an informal label often used for unusually energetic core-collapse supernovae, especially broad-lined Type Ic events. Some are associated with long gamma-ray bursts and central engines involving rapid rotation, strong magnetic fields, or black-hole formation. The term is not a single sharply bounded physical class. Spacegate should show it only when a source explicitly uses and supports the classification.
- **Examples (Progenitor Candidates):** Eta Carinae (7,500 LY), WR 104 (8,000 LY)

#### Kilonova

- **Tooltip:** A kilonova is a rapidly changing glow powered by radioactive nuclei made in neutron-rich debris from a compact-object merger. The best-established case followed a neutron-star merger detected in gravitational waves; neutron-star-black-hole mergers can also produce one when matter is ejected. These events help create heavy r-process elements, although their total contribution across cosmic history remains an active question. Spacegate should bind the tag to an observed transient and its event evidence.
- **Examples:** GW170817 (130 Million LY - Extragalactic)

### 4. Terminal State / Future Remnant

#### Future White Dwarf

- **Tooltip:** Current stellar-evolution models predict that many low- and intermediate-mass stars will ultimately leave white-dwarf remnants. The path can include giant phases, mass loss, and sometimes a visible planetary nebula, while binary interaction can redirect the evolution. A future state is a model prediction, not a property already observed. Spacegate should show this only with the model, inputs, uncertainty, and applicability visible.
- **Examples:** The Sun (0 LY), Alpha Centauri A (4.3 LY), Sirius A (8.6 LY)

#### Future Neutron Star

- **Tooltip:** Some massive-star models predict a neutron-star remnant after core collapse. The outcome depends on the final core mass, mass loss, rotation, composition, binary interaction, and explosion physics, many of which are uncertain long before collapse. This tag would therefore express a probability-bearing model result, not a destiny. Spacegate should not enable it until those model and uncertainty contracts are defined.
- **Examples:** Spica (250 LY), Bellatrix (250 LY), Zeta Ophiuchi (366 LY)

#### Future Black Hole

- **Tooltip:** Some stellar-evolution models predict that a massive star will leave a black hole, either after an explosion or through a collapse with little electromagnetic display. Mass loss, metallicity, rotation, binarity, and uncertain explosion physics can change that result. The presence of a massive star today does not make one remnant inevitable, and a singularity is not an observable classification. Spacegate should keep this disabled until it can present a versioned probabilistic model rather than a prophecy.
- **Examples:** Rigel (860 LY), Alnitak (1,260 LY), Deneb (2,600 LY)

#### Obliterated

- **Tooltip:** Some explosion models predict that no bound stellar remnant survives, including many normal Type Ia and full pair-instability models. Other thermonuclear events can leave remnants, and observations may not directly reveal what survived. “Obliterated” is vivid but scientifically too broad unless it is tied to a specific event model and evidence. Spacegate should keep this proposal disabled or replace it with a precise remnant-status field.
- **Examples (Progenitor Candidates):** IK Pegasi (150 LY), Eta Carinae (7,500 LY)

#### Associated Nebula

- **Tooltip:** An associated nebula is an extended cloud linked to a star or stellar event by accepted positional, kinematic, historical, or physical evidence. It may contain expelled stellar material, swept-up interstellar gas, illuminated nearby gas, or a mixture of all three. Association does not automatically mean the star created every part of the cloud. Spacegate should preserve the relation type and source rather than reduce it to proximity.

##### Planetary Nebula

- **Tooltip:** A planetary nebula is ionized gas expelled by a low- or intermediate-mass star near the end of its giant phases and illuminated by the exposed hot core. Winds, pulses, companions, and magnetic fields can sculpt shells, rings, and bipolar forms. The name is historical: early telescopes made some appear planet-like, but they are not planets or planetary systems. Spacegate should connect the nebula and central star through accepted relation evidence.
- **Examples:** Helix Nebula (650 LY), Dumbbell Nebula (1,360 LY), Ring Nebula (2,570 LY)

##### Supernova Remnant

- **Tooltip:** A supernova remnant is the expanding structure left as supernova ejecta and shock waves interact with surrounding material. Different regions can glow in radio, infrared, visible, X-ray, and gamma-ray bands as magnetic fields accelerate particles and shocked gas heats or cools. Some remnants contain a neutron star or pulsar, while others have no detected compact object. Spacegate should preserve the event association and evidence rather than infer it from overlap alone.
- **Examples:** Vela Supernova Remnant (800 LY), Geminga (815 LY), Cygnus Loop (1,500 LY)

### 5. Spectral & Emission Signatures

#### Radio Emitter

- **Tooltip:** A radio emitter produces detectable electromagnetic radiation at wavelengths longer than infrared light. Stars and stellar systems can generate radio emission through magnetic activity, coherent bursts, winds, jets, shocks, accretion, or pulsar beams, so radio detection does not identify one mechanism by itself. The observing band, variability, polarization, and source resolution provide essential context. Spacegate should attach this tag to a cited detection with those details when available.
- **Examples:** UV Ceti (8.7 LY), Vela Pulsar (900 LY), Crab Pulsar (6,500 LY)

#### Infrared Excess

- **Tooltip:** Infrared excess means an object is brighter at infrared wavelengths than an appropriate photospheric model predicts. Warm or cold dust is a common explanation, including protoplanetary disks, debris disks, or material lost by an evolved star, but unresolved companions, background sources, and model errors can imitate the signal. The wavelength range and significance determine what can reasonably be inferred. Spacegate should preserve the measurement and interpretation separately.
- **Examples:** Epsilon Eridani (10.5 LY), Vega (25 LY), Fomalhaut (25 LY)

#### UV Dominant

- **Tooltip:** Ultraviolet-dominant means that a source or selected spectral-energy model places a large share of its emitted power beyond visible violet light. Hot stars and compact remnants can meet that description, while flares can create temporary ultraviolet enhancements in cooler stars. Interstellar dust absorbs ultraviolet light strongly, so apparent and intrinsic emission must not be confused. Spacegate should define the band, comparison, extinction treatment, and persistence before enabling this tag.
- **Examples:** Regulus (79 LY), Spica (250 LY), Adhara (430 LY)

#### X-Ray Source

- **Tooltip:** An X-ray source emits photons energetic enough to trace million-degree plasma, strong magnetic activity, shocks, or matter heated during accretion. Ordinary stellar coronae, flares, colliding winds, and compact-object binaries can all produce X-rays at very different strengths. A positional match alone can be ambiguous because X-ray instruments and surveys have different resolution. Spacegate should require a cited counterpart association and retain band, flux, epoch, and uncertainty where available.
- **Examples:** Sirius (8.6 LY), Capella (42 LY), Algol (90 LY)

#### Gamma-Ray Source

- **Tooltip:** A gamma-ray source emits at the highest-energy part of the electromagnetic spectrum. Pulsars, magnetars, compact binaries, relativistic jets, particle collisions, and transient explosions can contribute, often with uncertain counterpart associations. Detection reveals energetic particle or nuclear processes but does not by itself determine which object produced them. Spacegate should preserve the instrument, energy band, localization, time behavior, and association confidence.
- **Examples:** Geminga (815 LY), Vela Pulsar (900 LY), Crab Pulsar (6,500 LY)

### 6. Core Mechanics & Composition

#### PP Chain

- **Tooltip:** The proton-proton chain is a set of fusion reactions that converts hydrogen into helium and supplies most of the Sun's nuclear energy. It dominates central hydrogen burning in many low-mass stars, while the CNO cycle contributes increasingly at higher core temperatures. Astronomers usually infer the balance from stellar models rather than observe the individual reactions directly, apart from constraints such as solar neutrinos. Spacegate should present this as a model-dependent mechanism with applicability, not an object label inferred from spectral type alone.
- **Examples:** The Sun (0 LY), Epsilon Indi (11.8 LY), Tau Ceti (11.9 LY)

#### CNO Cycle

- **Tooltip:** The CNO cycle converts hydrogen into helium through reactions in which carbon, nitrogen, and oxygen nuclei act as catalysts. Its rate rises steeply with core temperature, so it becomes the dominant hydrogen-burning route in stars hotter and more massive than the Sun. The transition is gradual and model-dependent rather than a clean spectral boundary. Spacegate should show this only from an applicable stellar model and retain its assumptions.
- **Examples:** Sirius A (8.6 LY), Vega (25 LY), Fomalhaut (25 LY)

#### Triple-Alpha

- **Tooltip:** The triple-alpha process fuses helium nuclei into carbon at the high temperatures found in helium-burning stellar interiors. It can begin after central hydrogen is depleted, but not every red giant is already burning helium in its core, and later stages can burn helium in shells. Surface observations usually constrain this state through evolutionary models and oscillations rather than direct inspection of the core. Spacegate should require an accepted evolutionary solution.
- **Examples:** Arcturus (37 LY), Capella A (42 LY), Aldebaran (65 LY)

#### Population I

- **Tooltip:** Population I is a historical label for relatively metal-rich stars associated mainly with the Milky Way's disk, including the Sun. “Metals” in astronomy means every element heavier than helium, and the abundance pattern carries information about earlier generations of stars. The populations overlap and modern studies often use measured chemistry and kinematics instead of a binary label. Spacegate should define the abundance and population criteria before enabling it.
- **Examples:** The Sun (0 LY), Alpha Centauri (4.3 LY), Sirius (8.6 LY)

#### Population II

- **Tooltip:** Population II is a historical label for generally old, metal-poor stars common in the Galactic halo, thick disk, and globular clusters. Their chemistry records formation from gas enriched by fewer earlier stellar generations, but low metallicity does not map to one exact age or orbit. Modern population assignments combine abundances, ages, and Galactic motion. Spacegate should expose those criteria and their uncertainty rather than infer the tag from location alone.
- **Examples:** Kapteyn's Star (12.8 LY), Groombridge 1830 (29.9 LY), HD 140283 "Methuselah Star" (200 LY)

#### Chemically Peculiar

- **Tooltip:** A chemically peculiar star has spectral abundance patterns that depart from those expected for otherwise similar stars. Diffusion, magnetic fields, rotation, winds, internal mixing, or mass transfer can create different peculiar families, so the tag should never imply one mechanism. Surface abundance also need not represent the star's entire interior. Spacegate should preserve the published peculiarity subtype and measurements.
- **Examples:** Alpha Circini (54 LY), Alioth (81 LY), Cor Caroli (110 LY)

### 7. Multiplicity & Kinematics

#### Single

- **Tooltip:** SINGLE means that Spacegate's accepted hierarchy currently contains one stellar member. It does not prove that no faint, close, distant, or as-yet undetected companion exists. Detecting companions depends on angular resolution, time coverage, orbital orientation, brightness contrast, and distance. The tag is useful as an inventory statement only when its observational limits are visible.
- **Examples:** The Sun (0 LY), Barnard's Star (5.9 LY), Altair (16.7 LY)

#### Binary

- **Tooltip:** A binary system contains two stars bound into one gravitational system. Both orbit their shared center of mass, or barycenter, with the more massive star moving on the smaller path. Binaries let astronomers measure stellar masses and test gravity because each star's motion constrains the other. Spacegate applies this tag when the accepted hierarchy contains two stellar members, even when the available observations do not yet provide a complete orbit.
- **Examples:** Sirius (8.6 LY), Procyon (11.4 LY), Capella (42 LY)

#### Multiple

- **Tooltip:** A multiple system contains at least three accepted stellar members bound into one larger family. If several stars orbit at comparable distances, their repeated gravitational encounters can exchange energy, alter orbits, or eject a member. Long-lived systems therefore often organize themselves into separated orbital scales, such as a tight pair with a distant companion. Spacegate uses MULTIPLE for the member count and adds HIERARCHICAL when the accepted relationships reveal that nested structure.
- **Examples:** Alpha Centauri (4.3 LY - Trinary), Castor (51 LY - Sextuple), Mizar/Alcor (83 LY - Sextuple)

#### High Proper Motion

- **Tooltip:** Proper motion is a star's measured angular drift across the sky, usually expressed in milliarcseconds per year. A large value can result from nearness, high sideways velocity, or both; distance is needed to turn the angular motion into a physical tangential speed. Over decades the shift can become visible in repeated images even though the star remains enormously distant. Spacegate should define the threshold and reference frame before enabling HIGH PROPER MOTION.
- **Examples:** Barnard's Star (5.9 LY), Kapteyn's Star (12.8 LY), Groombridge 1830 (29.9 LY)

#### Runaway

- **Tooltip:** A runaway star moves unusually fast relative to its local stellar population or birthplace. Gravitational encounters in a dense group and disruption of a binary by a supernova are leading ejection routes, but tracing a specific origin requires full three-dimensional motion and age consistency. Some runaways form infrared bow shocks where their winds meet surrounding gas; many do not show an obvious one. Spacegate should require a defined velocity frame and accepted kinematic evidence.
- **Examples:** Zeta Ophiuchi (366 LY), Mu Columbae (1,300 LY), AE Aurigae (1,460 LY)

#### Hypervelocity

- **Tooltip:** A hypervelocity star has an exceptionally large Galactic speed, sometimes high enough to be unbound under a chosen Milky Way mass model. Encounters with the central black hole are one production channel, but supernova disruption and other dynamical mechanisms can also accelerate stars. Whether an object escapes depends on its full velocity, position, uncertainties, and the adopted Galactic potential. Spacegate should never reduce that model-dependent probability to speed alone.
- **Examples:** S5-HVS1 (29,000 LY), US 708 (62,000 LY), HE 0437-5439 (200,000 LY)

#### Halo Star

- **Tooltip:** A halo star belongs kinematically or chemically to the Milky Way's extended stellar halo rather than its thin disk. Many are old and metal-poor and follow inclined or eccentric Galactic orbits, but the population contains streams and accreted structures with varied histories. A nearby star can be a halo member while physically passing through the Solar neighborhood. Spacegate should base the tag on an explicit population model with uncertainties.
- **Examples:** Kapteyn's Star (12.8 LY), Groombridge 1830 (29.9 LY), HD 140283 (200 LY)

## Planet Matrix Tooltips (3×3 Taxonomy)

### 1. Hot Giant Planet

- **Tooltip:** A hot giant is a confirmed planet in Spacegate's largest size or mass bin that also receives intense stellar energy. Many well-studied examples orbit close to their stars, where their atmospheres can expand, circulate at extreme speeds, or escape into space. Those outcomes are possibilities, not properties inferred by this tag. Spacegate uses the selected radius when available, otherwise selected mass, together with a temperature or irradiation screen; the glyph's annulus identifies the giant category rather than observed rings.

- **Examples:** 51 Pegasi b (Dimidium), WASP-12b, HD 209458 b (Osiris), WASP-76b.

### 2. Temperate Giant Planet

- **Tooltip:** A temperate giant is a confirmed planet in Spacegate's largest size or mass bin receiving a moderate level of stellar energy. A giant planet is not expected to offer an Earth-like solid surface, but its atmosphere and any moons could make the system scientifically compelling. Spacegate cannot infer those environments from the map category alone. The tag combines selected radius or fallback mass with a temperature or irradiation screen, and the glyph's annulus denotes scale rather than observed rings.

- **Examples:** HD 28185 b, Kepler-16b, Kepler-47c, PH2 b (Kepler-86b).

### 3. Cold Giant Planet

- **Tooltip:** A cold giant is a confirmed planet in Spacegate's largest size or mass bin receiving relatively little energy from its star. Such worlds often occupy wide, long-period orbits where volatile compounds can condense and direct detection may become easier than it is beside a brilliant star. Their actual clouds, internal heat, moons, and composition generally remain separate observational questions. Spacegate combines selected radius or fallback mass with a temperature or irradiation screen; the annulus in the glyph marks the giant bin, not detected rings.

- **Examples:** Jupiter, Saturn, Epsilon Eridani b (Aegir), 47 Ursae Majoris b.

### 4. Hot Neptunian Planet

- **Tooltip:** A hot Neptunian is a confirmed planet in the broad scale between terrestrial worlds and giants while receiving intense stellar energy. This region includes diverse planets, from rocky cores with thick envelopes to volatile-rich worlds losing gas under strong irradiation. Spacegate cannot determine that composition from size or mass alone. The tag combines selected radius or fallback mass with a temperature or irradiation screen, using Neptunian as a map-scale category rather than proof of a Neptune-like interior.

- **Examples:** Gliese 436 b, HAT-P-11b, WASP-107b, GJ 3470 b.

### 5. Temperate Neptunian Planet

- **Tooltip:** A temperate Neptunian is a confirmed planet in the broad scale between terrestrial worlds and giants receiving moderate stellar energy. Planets in this range have no exact Solar System counterpart and may include deep atmospheres, volatile-rich interiors, or large oceans beneath high pressure. Those possibilities make them fascinating targets for atmospheric study, but none follows from the tag alone. Spacegate combines selected radius or fallback mass with a temperature or irradiation screen and does not treat the result as evidence of a surface or habitability.

- **Examples:** K2-18 b, TOI-1231 b, LP 791-18 c, Sub-Neptune LHS 1140 b.

### 6. Cold Neptunian Planet

- **Tooltip:** A cold Neptunian is a confirmed planet in the broad scale between terrestrial worlds and giants receiving relatively little stellar energy. A world in this bin could resemble an ice giant, a gas-rich super-Earth, or something with no close Solar System analogue. Its distant orbit and cool atmosphere can make detailed characterization difficult. Spacegate combines selected radius or fallback mass with a temperature or irradiation screen, so Neptunian names a useful scale rather than a measured composition.

- **Examples:** Uranus, Neptune, OGLE-2005-BLG-390Lb, MOA-2007-BLG-192Lb.

### 7. Hot Terrestrial Planet

- **Tooltip:** A hot terrestrial is a confirmed planet in Spacegate's smallest size or mass bin receiving intense stellar energy. Some such worlds may have rocky surfaces, lava regions, or eroded atmospheres, while others near the category boundary may retain substantial volatile envelopes. Size and mass make those scenarios testable but do not choose between them. Spacegate combines selected radius or fallback mass with a temperature or irradiation screen and reserves claims about surface conditions for stronger evidence.

- **Examples:** 55 Cancri e (Janssen), Kepler-10b, CoRoT-7b, K2-141b.

### 8. Temperate Terrestrial Planet

- **Tooltip:** A temperate terrestrial is a confirmed planet in Spacegate's smallest size or mass bin receiving a moderate level of stellar energy. That combination makes it an obvious target in the search for rocky, potentially clement worlds, but radius, mass, and irradiation do not reveal an atmosphere, ocean, magnetic field, or biosphere. Even a planet in the habitable zone can resemble Venus, Mars, or something unfamiliar. Spacegate uses this tag as a discovery screen that points toward the next questions rather than answering them prematurely.

- **Examples:** Earth, TRAPPIST-1 e, Proxima Centauri b, TOI-700 d, Kepler-186f.

### 9. Cold Terrestrial Planet

- **Tooltip:** A cold terrestrial is a confirmed planet in Spacegate's smallest size or mass bin receiving relatively little energy from its star. Its surface could be rocky, ice-covered, volatile-rich, or hidden beneath an atmosphere, and internal or tidal heating may matter as much as starlight. Weak irradiation therefore does not mean a world is geologically inactive or scientifically dull. Spacegate combines selected radius or fallback mass with a temperature or irradiation screen while leaving composition and surface conditions open.

- **Examples:** Mars, Earth's Moon, Proxima Centauri c, OGLE-2016-BLG-1195Lb.
