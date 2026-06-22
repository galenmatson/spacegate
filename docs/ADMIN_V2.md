# Spacegate Admin v2 Architecture

This document defines the Admin v2 direction after the Photon migration and
local inference experiments.

Related contracts:

- `docs/API_SPEC.md`: public and admin API shape
- `docs/ADMIN_AUTH_SPEC.md`: OIDC, sessions, RBAC, audit
- `docs/AGENT_FRAMEWORK.md`: agent lifecycle and storage boundaries
- `docs/AGENT_EVALS.md`: model/role evaluation harness
- `docs/SCHEMA_ARM.md`: reviewed supplemental science and proposals
- `docs/SCHEMA_DISC.md`: citations, dossiers, factsheets, exposition
- `docs/RETENTION.md`: build/report/research storage policy

## Goals

Admin v2 is the Spacegate operating console. It should be calm, dense, and
workflow-oriented. It is not a public product surface.

Primary goals:

- make build, dataset, and deployment state obvious
- make local/cloud inference explicit, testable, and budget-aware
- make every agent step inspectable as a dossier journal entry
- keep scientific facts, proposals, generated prose, and operator actions in
  their proper layers
- turn complex agent work into a readable chain of evidence instead of a hidden
  black box

Non-goals for the first rewrite:

- no autonomous publication of scientific overlays
- no public user management beyond admin auth
- no direct edits to `core`
- no attempt to make Spacegate own all model-serving infrastructure yet

## Information Architecture

Admin v2 should use a persistent left navigation with these top-level areas.

### Overview

Purpose: one-screen operational status.

Show:

- current served build, git SHA, build age, and verification status
- API/web health, Photon local HTTPS status, and container status
- current dataset counts and largest warnings
- running or failed jobs
- inference endpoint summary
- newest high-priority review items

### Builds

Purpose: operate the deterministic science pipeline.

Show:

- raw/cooked/out/served state summary
- build, verify, promote, publish, and retention actions
- recent build report cards
- failed build cleanup candidates
- immutable build artifact locations

Actions remain allowlisted jobs with confirmation phrases for high-risk steps.

### Dataset

Purpose: understand the current science artifact.

Show:

- source contribution and overlap
- schema/QC gates
- determinism fields
- product slice and deep-query readiness
- catalog lifecycle and classifier drift summaries

This screen should be read-heavy. Actions should be limited to previewing or
launching controlled rebuilds.

### Inference

Purpose: manage model endpoints and model suitability.

Show:

- endpoint registry: local Photon vLLM, Positron LM Studio fallback, OpenAI,
  Google Gemini, and any later endpoints
- cheap endpoint probe: `/v1/models` or provider equivalent
- deliberate generation smoke test
- selected model per pipeline role: `discover`, `prune`, `compile`,
  `identify`, `extract`, `criticize`, `adjudicate`, `narrate`
- context/token limits, temperature, timeout, quantization, runtime, and
  endpoint auth status
- eval report history from `reports/agent_eval`

The Admin UI should treat model serving as an external runtime for now.
Spacegate consumes OpenAI-compatible endpoints and records runtime metadata.
It should not fold vLLM container lifecycle into the main app until the runtime
contract stabilizes.

### Agency

Purpose: run and inspect the scientific enrichment workflow.

Primary object: the Evidence Portfolio.

Screens:

- work queue: seeded, gathering, extracted, review-ready, published, stale,
  blocked
- portfolio detail: target object, queue reason, freshness, source coverage,
  findings, proposals, review state, publication artifacts
- source file detail: URL, domain tier, retrieval metadata, local archive path,
  content hash, extracted sections, source notes
- extraction set detail: model/prompt/runtime metadata, source context, parsed
  claims, raw output, parse errors
- proposal review: deterministic checks, adversarial review, conflict flags,
  accept/reject/defer/escalate controls
- journal timeline: human-readable narrative of the portfolio's construction

### Audit

Purpose: accountability and recovery.

Show:

- auth events
- admin job events
- agent/inference actions
- backup/restore actions
- search/query audit events

Audit entries are not a substitute for dossier journal entries. Audit answers
"who did what"; the journal answers "how the evidence case developed."

## Dossier Journal

Every nontrivial agency step should append a journal entry that a human or LLM
can follow later.

Journal entries should be plain-language but structured:

- timestamp
- actor type: `system`, `operator`, `agent`, `model`, `reviewer`
- actor id or model id when applicable
- stage: `seed`, `retrieve`, `extract`, `resolve`, `structure`, `review`,
  `publish`, `watch`
- short title
- narrative body
- links to source documents, extraction sets, claims, proposals, jobs, and
  reports
- outcome: `created`, `updated`, `accepted`, `rejected`, `deferred`,
  `escalated`, `blocked`
- machine payload for exact reproducibility

Example style:

```text
Retrieved SIMBAD bibliography for Castor and attached three candidate source
files. Two sources discuss component-level orbits; one is a naming-only lead and
was kept for context but excluded from scalar extraction.
```

Rules:

- journal prose may summarize evidence, but accepted facts still come from
  structured findings/proposals
- source links and local archived paths must be visible
- raw model output must remain available for review/debugging
- failed or abstained steps are first-class entries, not hidden errors
- generated journal prose belongs to the operational dossier, not `core`

## Inference Runtime Policy

Blank-slate inference should be role-based and evidence-first.

Recommended v1 policy:

- default bulk work to local Photon inference through an OpenAI-compatible
  endpoint
- keep Positron LM Studio as a fallback endpoint
- use frontier/cloud models only for hard-tail review, ambiguous adjudication,
  and occasional eval baselines
- assign models per role, not globally
- require model/prompt/runtime metadata on every generated finding, review, and
  exposition
- require deterministic checks before model review wherever possible
- keep all scientific outputs human-gated until the review system earns trust

The current Photon vLLM setup should remain host-local until there is a stable
runtime contract. Admin should probe and use it, not own it.

Minimum endpoint record:

- endpoint id and provider
- base URL
- auth mode, without displaying secrets
- reachable status and last probe time
- available model ids
- default model per role
- context limit and known caveats
- operator notes

Minimum generation metadata:

- endpoint id
- provider and runtime (`vllm`, `lm_studio`, `openai`, `google`, etc.)
- model id
- served model name
- quantization when known
- prompt version
- role
- temperature and sampling settings
- max input/output tokens
- input/output hashes
- started/completed timestamps
- success/failure status and error class

## Storage Boundaries

Use these locations by default on Photon:

- `$SPACEGATE_STATE_DIR` (`/data/spacegate/state`): builds, reports, admin DB,
  jobs, reproducible generated artifacts
- `/mnt/space/spacegate`: bulk research material, archived papers, retrieved
  source documents, OCR/intermediate text, large dossier attachments, and
  reusable science-document cache
- `/data/models`: model weights and inference caches shared across projects
- `/srv/spacegate`: repo checkouts and host-local config

The USB-backed `/mnt/space/spacegate` is large and fast but less trustworthy
than internal storage. Anything there that matters to scientific traceability
must be referenced by metadata stored in `disc`/admin state with hashes,
retrieval timestamps, and source URLs. The cache can be regenerated or repaired
from those records.

## First Implementation Slices

1. Admin shell extraction
   - move Admin UI out of the large FastAPI HTML string into a dedicated admin
     frontend
   - keep existing `/api/v2/admin/*` APIs and auth
   - preserve the old v1 routes as deprecated aliases

2. Overview + Inference foundation
   - show auth/session status, service health, current build, container status,
     and endpoint probes
   - read model ids from configured endpoints
   - show recent eval reports

3. Agency read model
   - define API shapes for Evidence Portfolio, Source File, Extraction Set,
     Findings, Proposals, Review, and Journal Entry
   - start with read-only/mock or report-backed data where production tables do
     not exist yet

4. Work queue and journal persistence
   - seed portfolios from coolness/adjudication queues
   - append journal entries for queue seed, source retrieval, extraction, and
     review actions

5. Human-gated review
   - accept/reject/defer/escalate proposals
   - write accepted science overlays only to reviewed `arm`/`disc` surfaces

## Design Notes

- Favor dense tables, split panes, timelines, and diffable JSON/prose views over
  marketing-style cards.
- Every screen should answer: current state, why it matters, what changed, and
  what action is safe next.
- Prefer small explicit actions over wizard flows except for risky build or
  publish paths.
- The Agency UI should feel like reading a lab notebook plus evidence binder,
  not monitoring a chatbot.
