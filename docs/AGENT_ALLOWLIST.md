# Allowed Internet Science Sources

Machine-readable source policy lives in `config/agent_source_allowlist.json`.
Admin v2 loads that default and writes operator changes to
`$SPACEGATE_STATE_DIR/config/agent_source_allowlist.json` from the Agency
Source Allowlist tab. Agent retrieval code should read the runtime JSON first
and fall back to the repo default. This Markdown file remains the human-readable
policy overview and review guide.

## TIER 0 - CANONICAL
trust >= 0.98
These can directly support core-adjacent facts (still routed via arm → adjudication).

- domain: cds.unistra.fr
  org: Centre de Données astronomiques de Strasbourg (CDS)
  type: catalog_host (SIMBAD, VizieR)
  trust_score: 1.00
  allowed_uses: [identifiers, crossmatch, object metadata, bibliography]
  notes: Primary backbone for object identity

- domain: simbad.u-strasbg.fr
  org: CDS SIMBAD
  type: object_database
  trust_score: 1.00
  allowed_uses: [object identity, aliases, bibliography]

- domain: vizier.cds.unistra.fr
  org: CDS VizieR
  type: catalog_archive
  trust_score: 1.00
  allowed_uses: [catalog data, tables, measurements]

- domain: archives.esac.esa.int
  org: ESA
  type: mission_archive (Gaia)
  trust_score: 1.00
  allowed_uses: [astrometry, distances, motions]

- domain: exoplanetarchive.ipac.caltech.edu
  org: NASA Exoplanet Archive
  type: catalog
  trust_score: 1.00
  allowed_uses: [exoplanet parameters, host associations]

- domain: mast.stsci.edu
  org: STScI / NASA
  type: mission_archive (Hubble, Kepler, TESS)
  trust_score: 0.99
  allowed_uses: [observational data, light curves]
  
  
## TIER 1 - SCIENTIFIC LITERATURE
High confidence. Primary enrichment content.
Trust 0.93–0.99

- domain: scixplorer.org
  org: NASA / SciX
  type: literature_index
  trust_score: 0.97
  allowed_uses: [paper discovery, citation linking]
  notes: Good entry point; verify PDF source

- domain: ui.adsabs.harvard.edu
  org: NASA ADS / Harvard
  type: literature_index
  trust_score: 1.00
  allowed_uses: [paper discovery, citations, bibcodes]
  notes: BEST starting point for papers

- domain: arxiv.org
  org: Cornell / arXiv
  type: preprint_server
  trust_score: 0.95
  allowed_uses: [recent research, system descriptions]
  notes: Prefer when peer-reviewed version unavailable

- domain: iopscience.iop.org
  org: Institute of Physics
  type: journal
  trust_score: 0.97
  allowed_uses: [peer-reviewed results]

- domain: academic.oup.com
  org: Oxford University Press (MNRAS)
  type: journal
  trust_score: 0.98

- domain: link.springer.com
  org: Springer Nature
  type: journal
  trust_score: 0.97

- domain: nature.com
  org: Nature Publishing
  type: journal
  trust_score: 0.99

- domain: science.org
  org: AAAS
  type: journal
  trust_score: 0.99

- domain: aanda.org
  org: Astronomy & Astrophysics
  type: journal
  trust_score: 0.98

- domain: pasp.aas.org
  org: American Astronomical Society
  type: journal
  trust_score: 0.98
  
  
## TIER 2 - INSTITUTIONAL / OBSERVATORY
Good explanations, systems summaries, educational content, sometimes extracted values (with caution).
Trust 0.85–0.95

- domain: nasa.gov
  org: NASA
  type: institutional
  trust_score: 0.95
  allowed_uses: [context, summaries, mission data]

- domain: esa.int
  org: ESA
  type: institutional
  trust_score: 0.95

- domain: noirlab.edu
  org: NSF NOIRLab
  type: observatory
  trust_score: 0.93

- domain: eso.org
  org: European Southern Observatory
  type: observatory
  trust_score: 0.95

- domain: stsci.edu
  org: Space Telescope Science Institute
  type: observatory
  trust_score: 0.95

- domain: caltech.edu
  org: Caltech
  type: academic
  trust_score: 0.93

- domain: harvard.edu
  org: Harvard
  type: academic
  trust_score: 0.93

- domain: cam.ac.uk
  org: Cambridge
  type: academic
  trust_score: 0.93
  
  
## TIER 3 - CURATED AGGREGATORS
Discovery, cross checking, candidate leads
Trust 0.70–0.85

- domain: wikipedia.org
  org: Wikimedia
  type: aggregator
  trust_score: 0.80
  allowed_uses: [lead discovery, references]
  notes: MUST follow citations to primary sources

- domain: stellarium.org
  org: Stellarium
  type: curated_dataset
  trust_score: 0.75

- domain: in-the-sky.org
  org: aggregator
  trust_score: 0.70

- domain: heavens-above.com
  org: aggregator
  trust_score: 0.70
  

## TIER 4 - CONTEXT / NARRATIVE ONLY
Only for narrative enrichment tone and public facing explanations

- domain: space.com
  trust_score: 0.60

- domain: universetoday.com
  trust_score: 0.60

- domain: phys.org
  trust_score: 0.65
  
- domain: stars.astro.illinois.edu
  org: University of Illinois (Jim Kaler)
  type: academic_curated_narrative
  trust_score: 0.88
