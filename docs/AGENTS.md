# Agent Enrichment

This file is the prompt/runtime behavior note for agent-driven enrichment work.

Authoritative companion documents:

- `docs/AGENT_FRAMEWORK.md`: architecture, lifecycle, and storage contract
- `docs/AGENT_ITERATION_HISTORY.md`: implementation timeline, benchmark pivots, and lessons learned while building the first half of the agent pipeline
- `docs/AGENT_ALLOWLIST.md`: allowed web domains and source-trust policy
- `docs/AGENT_SCOUT.md`: target discovery, prioritization, and refresh seeding
- `docs/AGENT_DESIGNATOR.md`: pre-extraction identifier disambiguation, same-object lookup links, and binding-scope quarantine
- `docs/AGENT_RETRIEVER.md`: allowlisted fetch, archive, and source-file creation
- `docs/AGENT_EXTRACTOR.md`: source-to-claim extraction
- `docs/AGENT_RESOLVER.md`: claim subject mapping onto stable objects and relations
- `docs/AGENT_STRUCTURE.md`: reviewed derived subsystem and dynamical-tree reduction
- `docs/AGENT_REVIEWER.md`: review and acceptance policy
- `docs/AGENT_TAGGER.md`: search/discovery labeling and mode-aware tag classification
- `docs/AGENT_PUBLISHER.md`: publication/materialization surfaces
- `docs/AGENT_NARRATOR.md`: reviewed-evidence narrative generation
- `docs/AGENT_WATCHER.md`: continuous refresh, drift detection, and reconciliation
- `docs/AGENT_EVALS.md`: local-model golden-case evaluation, anomaly inbox, and role routing

## Pipeline Order

The intended end-to-end enrichment pipeline is:

1. `AGENT_SCOUT`
2. `AGENT_DESIGNATOR`
3. `AGENT_RETRIEVER`
4. `AGENT_EXTRACTOR`
5. `AGENT_RESOLVER`
6. `AGENT_STRUCTURE`
7. `AGENT_REVIEWER`
8. `AGENT_TAGGER`
9. `AGENT_PUBLISHER`
10. `AGENT_NARRATOR`
11. `AGENT_WATCHER`

Current implementation status:

- live now:
  - scout-selected pre-extraction designator pass
  - retriever
  - extractor
  - resolver
- reviewer for scalar, identity, naming, orbit-relation, and derived-subsystem proposals
- early publisher staging through accepted rows and overlay reconciliation
- partially live:
  - structure, via reviewed derived subsystem proposals and reviewed orbit-relation acceptance
- still mostly design:
  - narrator as a publication-stage consumer of reviewed evidence
  - watcher as the long-running literature-refresh and reconciliation loop

Reviewer stance:

- treat review as adversarial validation, not extractor confirmation
- prefer deterministic plausibility checks first
- use local-model review to attack claims for faults and inconsistencies
- reserve frontier-model adjudication for ambiguous or high-impact tail cases

## Operator Vocabulary

Use these terms in admin UI, runbooks, and planning discussions:

- `Evidence Portfolio`: the object-level case file. It corresponds primarily to one `object_dossier` plus its sources, evidence sets, findings, proposals, review decisions, and publication artifacts.
- `Source File`: one archived paper, catalog page, dataset, or publisher page attached to the portfolio. Storage-level rows are `source_documents` plus a `claim_bundles` row with `bundle_kind='retrieval'`.
- `Extraction Set`: one saved extraction pass over a source file. Storage-level rows are `claim_bundles` with `bundle_kind='extraction'` plus their `harvested_claims` and `extracted_claims`.
- `Findings`: normalized extracted claims. Storage-level rows are `extracted_claims`.
- `Proposals`: deterministic recommended database changes derived from findings. Storage-level rows live in proposal tables such as field, schema-gap, identifier-link, naming, subsystem, and orbit proposals.
- `Review` / `Verdict`: accepted, rejected, deferred, or escalated decisions on proposals and evidence.

Implementation note: storage still uses `claim_bundle_id` and `claim_bundles` for both source-file and extraction-set records. Treat that as a schema/API identifier, not the operator-facing term.


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

## Evaluation and Anomaly Inbox

Use `scripts/agent_eval.py` and the tracked cases under
`evals/spacegate_agent/cases/` before promoting a model or prompt into an agent
role. The eval harness compares models by pipeline role (`extract`, `identify`,
`criticize`, `adjudicate`, and related stages), not as a single global winner.

Surprising findings discovered during extraction or review belong in the
anomaly inbox concept described in `docs/AGENT_EVALS.md`. They are quarantined
signals, not accepted facts. Future production persistence should route them to
reviewed `disc`/`arm` surfaces and never directly into `core`.

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

Admin UI:

- Agent Workspace -> Adjudicator -> Inference Runtime provides the shared runtime probe surface for local OpenAI-compatible endpoints, OpenAI, and Google Gemini.
- Catalog probes are the cheap reachability check.
- Generation tests make one tiny model call and should be used deliberately for paid providers.
- The same runtime selector is the intended control surface for future extraction, review, adjudication, narration, and illustration defaults.

Script:
- `scripts/local_llm_adapter.py`

Supported commands:
- `models` (connectivity + available model IDs)
- `chat` (single completion)

Environment variables:
- `SPACEGATE_LLM_BASE_URL` (example: `http://10.0.0.11:1234`)
- `SPACEGATE_LLM_MODEL` (example: `google/gemma-4-e4b`)
- `SPACEGATE_LLM_TIMEOUT_S` (default: `90`)
- `SPACEGATE_LLM_TEMPERATURE` (global override for all extractor passes)
- `SPACEGATE_LLM_TEMPERATURE_GENERAL` (default extractor general-pass temperature: `0.2`)
- `SPACEGATE_LLM_TEMPERATURE_TABLE` (default table-harvest temperature: `0.3`)
- `SPACEGATE_LLM_TEMPERATURE_TARGET` (default focused target/relation-pass temperature: `0.1`)
- `SPACEGATE_LLM_MAX_TOKENS` (API extractor default: `3000`; standalone adapter default: `256`)
- `SPACEGATE_LLM_MAX_TOKENS_RETRY_CAP` (API extractor retry cap after length-truncated local completions; default: `8000`)
- `SPACEGATE_LLM_MAX_INPUT_CHARS` (default: `12000`)
- `SPACEGATE_REVIEWER_MAX_INPUT_CHARS` (default: `10000`)
- `SPACEGATE_REVIEWER_MAX_TOKENS` (default: `700`)
- `SPACEGATE_FRONTIER_MAX_INPUT_CHARS` (default: `128000`, unless overridden in the runtime environment)
- `SPACEGATE_FRONTIER_MAX_TOKENS` (default: `3000`)
- `SPACEGATE_LLM_API_KEY` (optional, only if your endpoint requires auth)
- `SPACEGATE_INFERENCE_VERBOSE` (`1` to emit pass-level extractor progress to stderr for API-side inference calls)
- `SPACEGATE_GROBID_URL` (optional scholarly-PDF preprocessing endpoint, for example `http://10.0.0.10:8070`)
- `SPACEGATE_GROBID_TIMEOUT_S` (default: `45`)
- `SPACEGATE_GROBID_IMAGE` (default: `grobid/grobid:0.9.0-full` when using the local compose-managed service)
- `SPACEGATE_ADS_SCAN_MAX_PAGES` (default: `80`; maximum OCR pages fetched from ADS Scan Explorer records)

Quick smoke test:
```bash
SPACEGATE_LLM_BASE_URL=http://10.0.0.11:1234 \
scripts/local_llm_adapter.py models

SPACEGATE_LLM_BASE_URL=http://10.0.0.11:1234 \
SPACEGATE_LLM_MODEL=google/gemma-4-e4b \
SPACEGATE_LLM_MAX_TOKENS=64 \
scripts/local_llm_adapter.py chat \
  --prompt "Reply with exactly: SPACEGATE_LOCAL_OK" \
  --temperature 0 \
  --show-usage
```

Budget controls:
- keep `SPACEGATE_LLM_MAX_TOKENS` high enough for valid structured JSON; tables and focused target passes commonly need thousands of output tokens
- raise `SPACEGATE_LLM_MAX_INPUT_CHARS` deliberately when testing richer dossiers against high-context local models
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
Use a normalized claim layer rather than repeating full metadata on every scalar:

1. `source_documents`
```json
{
  "source_document_id": "src_...",
  "canonical_url": "...",
  "source_domain": "...",
  "trust_score": 0.97,
  "citation_type": "peer_reviewed | institutional | aggregator"
}
```

2. `claim_bundles` storage records (`Source File` or `Extraction Set` in operator UI)
```json
{
  "claim_bundle_id": "bundle_...",
  "source_document_id": "src_...",
  "stable_object_key": "...",
  "extraction_method": "llm | parsed | manual",
  "model_id": "...",
  "prompt_version": "..."
}
```

3. `extracted_claims`
```json
{
  "claim_id": "claim_...",
  "claim_bundle_id": "bundle_...",
  "claim": "...",
  "value": "...",
  "unit": "...",
  "confidence": 0.85
}
```

Multiple extracted values from one paper or archive page should usually share the same `source_document_id` and extraction-set `claim_bundle_id`.


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
