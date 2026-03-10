# Spacegate Source Data Overview

This document describes the major astronomical surveys and catalogs that form (or are planned to form) the backbone of Spacegate.  

The goal of Spacegate is to preserve **original published data** with full provenance, while also constructing curated, cross-linked “data packs” that allow stars, planets, and systems to be explored as a unified knowledge graph.

Where relevant, this document explains:

- What kind of observations were made  
- The physical quantities measured  
- Data quality and limitations  
- How the survey compares to others  
- Institutional and national origins  
- Major scientific results  
- What Spacegate currently ingests from it  

## Current Spacegate Build Focus

Today the published Spacegate core is still built from a conservative baseline:

- `AT-HYG` for the initial nearby-star stellar backbone
- NASA Exoplanet Archive `pscomppars` for planet and host-star linkage
- Derived rich outputs such as coolness rankings and deterministic snapshots

The next ingestion wave is focused on improving astrometry, multiplicity, and cross-identification quality without losing provenance. The current evaluation stack is:

- Gaia DR3 astrometry and Non-Single Star tables
- WDS for broad visual-multiplicity coverage
- ORB6 for orbital evidence
- DEBCat and Kepler Eclipsing Binary catalogs for eclipsing-system support data
- MSC as a mandatory hierarchy source in default science builds

Spacegate targets `J2016.0 / ICRS` as the canonical coordinate epoch/frame for normalized core outputs. Source-native epochs and units are preserved in provenance-aware ingest logic wherever possible.

---

# Gaia (European Space Agency)

**Full Name:** Gaia mission, operated by the European Space Agency (ESA)  
**Managing Organization:** European Space Agency (ESA)  
**Data Processing:** Gaia Data Processing and Analysis Consortium (DPAC), an international collaboration  

## What Gaia Does

Gaia is a space-based astrometry mission.  

**Astrometry** means the precise measurement of:
- Positions (right ascension and declination)
- Parallax (apparent annual shift due to Earth’s orbit)
- Proper motion (motion across the sky)

Gaia also provides:
- Broad-band photometry (brightness in G, BP, RP bands)
- Radial velocities (for brighter stars)
- Derived astrophysical parameters

### Scale

Gaia Data Release 3 (DR3) contains:
- ~1.8 billion sources total  
- ~1.46 billion with full 5-parameter astrometric solutions  
- Hundreds of millions with derived astrophysical parameters  

This is the largest and most precise all-sky stellar catalog ever produced.

## Key Fields Used by Spacegate

Core astrometry:
- `source_id`
- `ra`, `dec`
- `parallax`, `parallax_error`
- `pmra`, `pmdec`
- `radial_velocity`
- `ruwe` (Renormalized Unit Weight Error; astrometric quality metric)

Photometry:
- `phot_g_mean_mag`
- `phot_bp_mean_mag`
- `phot_rp_mean_mag`
- `bp_rp`

Derived astrophysical parameters:
- `teff_gspphot` (effective temperature)
- `logg_gspphot` (surface gravity; log10 of cm/s²)
- `mh_gspphot` (metallicity proxy)
- extinction estimates

Multiplicity:
- Non-Single Star (NSS) solutions
- Orbital parameters for binaries

Spectra (specialized tables):
- XP continuous mean spectra (low-resolution BP/RP spectra)
- RVS (Radial Velocity Spectrometer) spectra

## Data Quality

Gaia provides the most precise parallaxes ever measured.  

However:
- Parallax systematics exist (e.g., global zero-point offset)
- Distance inversion becomes unstable at large distances
- Derived astrophysical parameters have heterogeneous reliability

Gaia is generally superior for:
- Positions
- Parallax
- Proper motion

Other surveys may surpass Gaia in:
- Detailed chemical abundances
- High-resolution spectroscopy

## Major Discoveries

- Discovery of Galactic phase-space spirals (evidence of past mergers)
- Mapping the Milky Way’s warp and bar
- Identification of numerous binary systems
- Discovery of hypervelocity stars

## Spacegate Status

Currently included:
- Core astrometry subset
- Photometry
- Basic quality flags

Planned:
- Non-Single Star orbital tables
- Astrophysical parameters
- Variability summaries
- Spectral products (as optional heavy packs)

---

# APOGEE (Apache Point Observatory Galactic Evolution Experiment)

**Full Name:** Apache Point Observatory Galactic Evolution Experiment (APOGEE)  
**Parent Project:** Sloan Digital Sky Survey (SDSS)  
**Institutional Origin:** Multi-institution U.S.-led collaboration  

## What APOGEE Measures

APOGEE is a high-resolution near-infrared spectroscopic survey.

- Wavelength: 1.51–1.70 microns (H-band)
- Resolution: R ≈ 22,500

**Resolution (R)** is defined as λ/Δλ and indicates the ability to separate spectral features.

APOGEE targets mostly red giant stars across the Milky Way.

## Important Terms

**[Fe/H] (Iron-to-Hydrogen Ratio):**  
Logarithmic metallicity measure relative to the Sun.

**Abundance Ratios (e.g., [Mg/Fe]):**  
Elemental ratios used in “chemical tagging” of stellar populations.

## Key Fields Used by Spacegate

- `teff`
- `logg`
- `[Fe/H]`
- Individual element abundances (C, N, O, Mg, Si, etc.)
- Radial velocity
- Signal-to-noise ratio

## Data Quality

APOGEE provides:
- High precision radial velocities
- Detailed chemical abundances (up to ~20 elements)
- Excellent temperature and gravity estimates

It is superior to Gaia for:
- Chemical composition
- Detailed stellar parameters

It is inferior to Gaia for:
- Astrometry

## Major Discoveries

- Identification of chemically distinct Galactic populations
- Evidence for radial migration in the Milky Way disk
- Detailed abundance mapping of the Galactic bulge

## Spacegate Status

Currently included:
- Planned ingestion of allStar summary catalog

Planned:
- Full abundance vectors for enrichment tagging
- Radial velocity cross-validation

---

# LAMOST (Large Sky Area Multi-Object Fiber Spectroscopic Telescope)

**Full Name:** Large Sky Area Multi-Object Fiber Spectroscopic Telescope  
**Also Known As:** Guo Shou Jing Telescope  
**Lead Institution:** National Astronomical Observatories of China (NAOC)  

## Survey Programs

LEGUE — LAMOST Experiment for Galactic Understanding and Evolution

## Observations

- Optical spectroscopy
- Wavelength range: 3700–9000 Ångströms
- Resolution: low to medium (R ≈ 1,800–7,500 depending on mode)
- Tens of millions of spectra

## Important Term

**Ca II Triplet:**  
Three strong calcium absorption lines near 8500 Å used for radial velocity and metallicity measurement.

LAMOST overlaps with this region and with RAVE.

## Key Fields

- Effective temperature (Teff)
- Surface gravity (logg)
- [Fe/H]
- Radial velocity
- Signal-to-noise ratio
- Spectral classification

## Data Quality

LAMOST is:
- Massive in scale
- Lower spectral resolution than APOGEE
- Excellent for broad statistical stellar population studies

It complements APOGEE:
- APOGEE = fewer stars, higher resolution, infrared
- LAMOST = many more stars, optical, lower resolution

## Major Discoveries

- Large-scale mapping of the Milky Way disk
- Identification of stellar streams
- Discovery of chemically peculiar stars

## Spacegate Status

Planned ingestion:
- Stellar parameter catalogs (not full raw spectra)
- Radial velocity integration
- Quality-flag-based filtering

---

# RAVE (Radial Velocity Experiment)

**Full Name:** Radial Velocity Experiment  
**Institutional Base:** European/Australian collaboration  

## Observations

- Medium resolution spectroscopy (R ≈ 7,500)
- Focused on Ca II triplet region (8410–8795 Å)
- ~450,000 unique stars

## Strengths

- Radial velocity precision
- Kinematic studies of nearby disk stars

## Limitations

- Smaller scale than LAMOST
- Narrow wavelength coverage

## Spacegate Status

Planned:
- Radial velocity cross-check layer
- Secondary parameter validation

---

# TESS and the TESS Input Catalog (TIC)

**Full Name:** Transiting Exoplanet Survey Satellite (TESS)  
**Managing Agency:** National Aeronautics and Space Administration (NASA)  
**Archive:** Mikulski Archive for Space Telescopes (MAST)

## TESS Mission

TESS detects exoplanets via the transit method.

**Transit Method:**  
A planet passes in front of its host star, causing a small dip in brightness.

## TESS Input Catalog (TIC)

The TIC is a compiled meta-catalog (~1.7 billion sources) built from:
- Gaia
- 2MASS (Two Micron All-Sky Survey)
- LAMOST
- RAVE
- APOGEE
- Tycho-2
- Hipparcos

It provides:
- Cross-identifications
- Stellar radius estimates
- Mass estimates
- Temperature estimates

## How Spacegate Uses TIC

- Cross-identification validation
- Join verification
- Parameter priors
- Conflict detection

TIC data is treated as:
- Supporting evidence, not ground truth

---

# Washington Double Star Catalog (WDS)

**Maintained By:** United States Naval Observatory (USNO)

## What It Contains

- Visual double and multiple star systems
- Angular separation
- Position angle
- Magnitude differences
- Component identifiers

## Role in Spacegate

Critical for:
- Explicit star-star relationships
- Multi-star system reconstruction
- Fixing single-component misrepresentations

---

# Survey Complementarity Summary

| Survey | Best At | Weak At |
|--------|--------|--------|
| Gaia | Astrometry, photometry | Detailed abundances |
| APOGEE | Chemistry, high-res IR spectra | Astrometry |
| LAMOST | Massive-scale parameters | High precision abundances |
| RAVE | Radial velocities | Spectral breadth |
| TESS | Planet detection, crossmatch | Fundamental stellar precision |
| WDS | Multiplicity relationships | Physical parameters |

---

# Design Philosophy in Spacegate

Spacegate preserves:

1. **Raw published values**
2. **Uncertainties**
3. **Quality flags**
4. **Provenance (catalog + version)**

Curated “best values” are computed in resolver layers, not by overwriting original data.

---

# Future Additions

Planned survey integrations:
- Gaia Non-Single Star solutions
- Gaia variability
- Multiple Star Catalog (MSC)
- Sixth Orbit Catalog (ORB6)
- SBX spectroscopic binary orbits
- APOGEE abundance vectors
- LAMOST DR11 expanded catalogs
- Spectral feature embeddings for AI enrichment

---

# Closing Note

Spacegate aims to become a unified stellar and planetary knowledge system.  

The Milky Way is approximately 100,000 light-years across. Gaia already samples a large fraction of its observable stellar population. Combining Gaia’s geometric precision with spectroscopic surveys’ chemical insight enables:

- Stellar genealogy
- Galactic archaeology
- Exoplanet-host contextualization
- Machine-assisted scientific discovery

This document will evolve as ingestion progresses.
