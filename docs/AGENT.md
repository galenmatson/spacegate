# Agent Enrichment


## Important functions:
- enrich the systems, stars, and planets in order by coolness 
- prioritize resolving ambiguous cases
- read scientific source material (like papers and articles with good rigor)
- link those sources in disc
- build up a good context from which it can:
  - enrich the disc database with narratives and factsheets
  - fill in missing fields in arm
  - resolve omissions, errors, and ambiguities

- maintain an explicit evidence dossier per object, not just a final answer
- rank and cache sources by quality, recency, and relevance
- detect contradictions across sources instead of averaging them away
- abstain cleanly when evidence is weak or conflicting
- generate proposed fixes as structured arm rows, never direct core edits
- attach every generated fact/narrative claim to citation IDs in disc
- track model/prompt/version provenance for every output
- support review states: proposed, accepted, rejected, superseded
- monitor stale dossiers and re-check important systems when new catalogs or papers appear
- produce eval artifacts so we can compare agent output to goldens instead of trusting vibes

## EXPLICIT ALLOWLIST
File: docs/AGENT_ALLOWLIST.md
Only use these sites. Each site comes with a trust score. Do not follow links off site unless they are direct document downloads.

### INGESTION POLICY
allowlist_policy:
  min_trust_for_claims: 0.90
  min_trust_for_context: 0.80

  enforce_origin_domain: true
  require_citation: true

  domain_rules:
    tier0:
      allow_core_adjacent: true
    tier1:
      allow_core_adjacent: true
    tier2:
      allow_core_adjacent: false
      allow_context: true
    tier3:
      discovery_only: true
    tier4:
      narrative_only: true

  conflict_policy:
    tier1_vs_tier1: escalate_to_arm
    tier1_vs_lower: prefer_tier1
    insufficient_evidence: abstain


## Local Model Adapter (LM Studio on positron)

Use local GPU inference for enrichment experiments before spending cloud tokens.

Script:
- `scripts/local_llm_adapter.py`

Supported commands:
- `models` (connectivity + available model IDs)
- `chat` (single completion)

Environment variables:
- `SPACEGATE_LLM_BASE_URL` (example: `http://192.168.1.174:1234`)
- `SPACEGATE_LLM_MODEL` (example: `openai/gpt-oss-20b`)
- `SPACEGATE_LLM_TIMEOUT_S` (default: `90`)
- `SPACEGATE_LLM_TEMPERATURE` (default: `0.1`)
- `SPACEGATE_LLM_MAX_TOKENS` (default: `256`)
- `SPACEGATE_LLM_MAX_INPUT_CHARS` (default: `12000`)
- `SPACEGATE_LLM_API_KEY` (optional, only if your endpoint requires auth)

Quick smoke test:
```bash
SPACEGATE_LLM_BASE_URL=http://192.168.1.174:1234 \
scripts/local_llm_adapter.py models

SPACEGATE_LLM_BASE_URL=http://192.168.1.174:1234 \
SPACEGATE_LLM_MODEL=openai/gpt-oss-20b \
SPACEGATE_LLM_MAX_TOKENS=64 \
scripts/local_llm_adapter.py chat \
  --prompt "Reply with exactly: SPACEGATE_LOCAL_OK" \
  --temperature 0 \
  --show-usage
```

Budget controls:
- keep `SPACEGATE_LLM_MAX_TOKENS` low for extraction passes
- keep `SPACEGATE_LLM_MAX_INPUT_CHARS` bounded to avoid oversized context
- always run with `--show-usage` during tuning so token spend is observable


## Pipeline Optimization Strategies

To manage context limits and improve generation efficiency, the pipeline must adhere to the following data handling rules:

* **Extract, Don't Dump (Paper Pre-processing):** Do not ingest full raw PDFs or research papers. Pre-filter all scientific literature to extract only the Abstract, Introduction, Results, and Conclusion. Omit dense methodology and reference sections to preserve token space.
* **Dense Data Formatting:** Maintain all structured data (e.g., Simbad, NASA Exoplanet Archive) in minified, strict YAML or JSON formats. Do not feed the model raw HTML tables or uncleaned web scrape data.
* **Two-Pass Architecture:** 1.  **Pass 1 (Fact Extraction):** Route raw source texts through a faster, lightweight model to extract a dense, bulleted list of 10-15 key physical traits, anomalies, and parameters.
    2.  **Pass 2 (Narrative Generation):** Feed only the Pass 1 extracted list alongside the structured JSON/YAML data to the primary narrative generator.


## Important guardrails:

- use an allowlist of source domains first, not open-ended web search
- prefer primary sources and catalog docs before Wikipedia-style summaries
- store links and structured evidence, not just prose
- never let the agent silently merge canonical identities
- require citations for every nontrivial claim
- make reruns idempotent and budget-aware
- evaluate on a fixed golden set like Castor, 16 Cyg, Sol, Sirius, Alpha Cen

## Mandatory citation structure (disc layer)
Every extracted claim must store:
{
  "claim": "...",
  "value": "...",
  "unit": "...",
  "source_url": "...",
  "source_domain": "...",
  "trust_score": 0.97,
  "citation_type": "peer_reviewed | institutional | aggregator",
  "extraction_method": "llm | parsed | manual",
  "confidence": 0.85
}


# INSTRUCTIONS

## SYSTEM / ROLE

You are a scientific narrative generator for Spacegate.

Your task is to transform structured astrophysical data and cited scientific sources into vivid, accurate, and educational prose.

You must obey the following constraints:

1. Do NOT invent scientific facts.
2. Every claim must be traceable to either:
   - provided structured data, or
   - provided citations.
3. You may extrapolate ONLY when:
   - it follows directly from known physics,
   - and you clearly signal it as inference.
4. You must preserve scientific correctness over drama.
5. You must aim to both:
   - enthrall (imagination, imagery, scale)
   - and teach (physics, chemistry, astrophysics).

Output must be written in clear, elegant prose suitable for a scientifically literate audience.

## INPUT BLOCK (STRUCTURED)

OBJECT:
- name: WASP-76 b
- type: ultra-hot Jupiter
- host_star:
    spectral_type: F7
    mass_msun: 1.46
    luminosity_lsun: 6.3
- orbital_period_days: 1.81
- semi_major_axis_au: 0.033
- equilibrium_temp_k: 2400
- tidal_locking: true

DERIVED FACTSHEET:
- dayside_temperature_k: ~2500+
- nightside_temperature_k: ~1500
- iron_vapor_present: true
- atmospheric_circulation: strong day→night winds
- condensation_mechanism: iron vapor condenses on nightside

CITATIONS:
1. Ehrenreich et al. 2020 (Nature) – detection of iron vapor
2. Seidel et al. 2021 – atmospheric circulation modeling
3. NASA Exoplanet Archive – orbital parameters
4. ESA press release – observational interpretation

CONSTRAINT FLAGS:
- emphasize_chemistry: true
- emphasize_extreme_physics: true
- include_scale_comparisons: true

## INSTRUCTION BLOCK

Write a narrative description of this system.

Structure:

1. Opening (sense of place)
   - immediately situate the reader physically in the system
   - use scale, light, motion

2. Star → Planet relationship
   - explain luminosity vs distance
   - explain why conditions are extreme
   - optionally explain scaling laws (e.g. luminosity ~ M^3–4)

3. Atmospheric physics
   - describe heat distribution
   - describe phase transitions (vaporization, condensation)
   - explicitly explain the chemistry (iron vapor → rain)

4. Dynamics
   - tidal locking
   - wind speeds
   - energy transport

5. Interpretive teaching moments
   - embed short explanations:
     - why metals vaporize
     - why temperature gradients drive winds
     - why close orbits create tidal locking

6. Closing
   - return to imagery
   - connect back to universal physics or other systems

STYLE RULES:
- Avoid clichés
- Avoid fantasy language
- Prefer physical descriptions over metaphor when possible
- Use metaphor only to clarify real physics

ANNOTATION RULES:
- Do NOT include inline citations in the prose
- Maintain scientific traceability implicitly
