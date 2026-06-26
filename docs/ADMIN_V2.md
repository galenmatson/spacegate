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
- container-visible path health for raw/cooked/out/reports/served targets
- verification gate summaries from required reports instead of frontend-only
  assumptions
- snapshot generation outcomes, including explicit zero-row/null-result runs
- snapshot operation control with requested/processed/generated/reused/failed/
  skipped counts, elapsed time, output root, selected artifact size, estimated
  large-run footprint, latest warning/error, advisory thresholds, and
  queued-job cancellation
- snapshot view type selection should be an explicit selector populated from
  the Admin action schema, not a free-text field
- links from build, verification, coolness, snapshot, retention, and temporary
  output status cards back to the relevant Operations job detail when
  detectable
- API-derived next actions that point to predecessor/successor pipeline steps
- retention dry-run planning with candidate paths, estimated reclaimable bytes,
  and an auditable dry-run job action
- guarded retention apply that requires a recent matching dry-run, unchanged
  candidate hash, explicit confirmation, and protected `served/current`
- after a dry-run succeeds, the apply card should summarize the exact candidate
  count, reclaimable bytes, candidate hash, and matching dry-run job before it
  can be started

Actions remain allowlisted jobs with confirmation phrases for high-risk steps.

### Dataset

Purpose: understand the current science artifact.

Show:

- source contribution and overlap
- schema/QC gates
- determinism fields
- product slice and deep-query readiness
- catalog lifecycle and classifier drift summaries
- planet environment coverage, separating source equilibrium temperature,
  source insolation, proxy-derivable insolation, broad HZ candidates, and
  missing environment evidence

This screen should be read-heavy. Actions should be limited to previewing or
launching controlled rebuilds.

### Object Diagnostics

Purpose: answer what Spacegate knows about a specific system and where that
knowledge lives.

Show:

- system/object search by canonical name, alias, stable object key, catalog id,
  `system <id>`, `star <id>`, `planet <id>`, or arm component name/key
- core identity, counts, coordinates, grouping basis, aliases, and provenance
- member stars and planets with key scalar/orbital fields
- per-planet environment evidence basis, candidate insolation/temperature,
  broad HZ triage flag, and nice-planet candidate flag
- arm-only components such as moons, minor bodies, artificial objects, and
  future exomoon candidates when they are present in `arm.component_entities`
- grouped/collapsible arm-only component sections so Sol-scale systems remain
  navigable while sparse extrasolar systems stay simple
- direct star/planet focus diagnostics from member tables, including scalar
  fields and provenance without leaving the system dossier
- direct component focus diagnostics from member tables, graph nodes, and
  parent/child relation links
- single-row browser-local recent-object shortcuts for returning to recently
  inspected systems, stars, planets, or arm components
- component navigation ordered by object class and orbital distance when a
  core-linked planet/subplanet provides a semi-major axis
- simulation readiness diagnostics that label needed numeric fields as
  `source`, `derived`, `assumed`, or `missing`, including the layer where each
  value belongs and the stronger replacement value administrators should seek
- persisted `arm.derived_physical_parameters` rows when present, preferred over
  runtime fallback derivations in the Simulation tab
- layer artifact paths for core, arm, and disc
- provenance completeness checks for system, star, and planet rows
- arm component, hierarchy, orbit-edge, orbital-solution, and stellar-parameter
  diagnostics
- disc coolness score, snapshot manifest rows, public/API links, and artifact
  URLs
- coolness explanation separating normalized feature values from weighted score
  contributions
- coolness signal counts, including source-temperature, source-insolation, and
  proxy-insolation basis counts for nice-planet signals
- read-only, clickable node/edge relation diagram for arm containment and orbit
  edges, with common-name labels when available
- orbit-edge and orbital-solution diagnostics with common names or component
  labels alongside technical IDs
- readiness status for public detail, coolness, snapshots, arm graph, orbital
  solutions, and provenance, including probable cause and next operator action
  when data is missing or not applicable

The first implementation is still system-dossier scoped. Star and planet
inspection reuses the selected system payload, and arm-only component
inspection reuses immutable component/edge rows, rather than introducing
mutable object records or direct layer writes.

### Inference

Purpose: manage model endpoints and model suitability.

Show:

- dynamic endpoint registry: local Photon vLLM, Positron LM Studio fallback,
  OpenAI, Google Gemini, and any later endpoints
- add/remove endpoints from Admin v2 without code changes
- endpoint auth mode: none, environment variable, or stored encrypted API key
- named provider credential env suggestions from runtime, with configured flags
  but no secret values
- cheap endpoint probe: `/v1/models` or provider equivalent
- cached available model ids from probe results
- per-endpoint notes, default model, timeout, enabled flag, and auth status
- aggregate usage stats by endpoint/model: request count, token totals, average
  latency, and last use
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

### Runtime

Purpose: read-only operator diagnostics for the current Admin/API runtime.

Show:

- active build id, git head, generated timestamp, and API process metrics
- auth/OIDC configuration status without exposing secrets
- container-visible paths, storage capacity, and mount gaps
- redacted environment variable status
- sensitive key configured/missing flags
- inference endpoint last-probe reachability summaries
- Docker/container visibility note, without requiring the Docker socket
- runtime hardening observations from inside the API process, including UID/GID,
  capability masks, no-new-privileges, seccomp mode, write probes, configured
  umask, bytecode-write status, and state-root owner/mode

Runtime must emit structured filesystem alerts for configured targets that are
missing, unreadable, untraversable, unwritable, or expected to be mounted but not
visible from inside the API container. The UI should display these as actionable
operator alerts with the affected env var/path and the next troubleshooting
step, rather than relying on hardcoded frontend path assumptions.

Runtime environment diagnostics should distinguish:

- `configured`: explicitly present in the API container
- `optional`: not set because the runtime uses a default or the setting belongs
  to another container
- `alias satisfied`: an alternate/preferred variable is set, such as
  `SPACEGATE_OPENAI_API_KEY` satisfying the legacy `OPENAI_API_KEY` alias
- `missing`: required by the current runtime and absent from the API container

The Runtime page should offer a backend-generated redacted diagnostics bundle
that can be copied, opened in a tab, or downloaded. The bundle should contain
filesystem alerts, path status, env configured/missing/alias flags, container
status, runtime hardening observations, auth status, and inference reachability
without secret values. Non-secret environment values should also be omitted from
the shareable bundle so operators do not accidentally leak local topology or
deployment details.

Runtime should also display launcher-observed config sources. The source list is
metadata passed into the API container by the Spacegate launcher; the API does
not read or mount the source files, and it never exposes file contents.

Config source ownership:

- `/etc/spacegate/spacegate.env`: canonical host-level Spacegate secrets and
  deployment-sensitive config, including OIDC secrets, session secrets, admin
  allowlist bootstrap, and provider API keys such as
  `SPACEGATE_OPENAI_API_KEY` or named future variants.
- `/srv/spacegate/<host>.env`: host-local nonsecret runtime config such as
  paths, ports, cache roots, model directories, and LAN/local endpoint URLs.
- `$repo/.spacegate.env`: optional repo-local nonsecret overrides for local
  development.
- `$repo/.spacegate.local.env`: optional private repo-local overrides that must
  remain untracked.
- `$SPACEGATE_ENV_FILE`: explicit highest-precedence override for temporary or
  special-purpose launches.

Config precedence is low-to-high in the order above. Existing process
environment values still override file values.

Runtime permission contract:

- `scripts/compose_spacegate.sh` exports `SPACEGATE_CONTAINER_UID` and
  `SPACEGATE_CONTAINER_GID` from the invoking host user unless explicitly
  overridden.
- The API container runs as that UID/GID, so bind-mounted state files are not
  created as `root`.
- `SPACEGATE_UMASK` defaults to `0002`, making generated files group-writable
  and generated directories cooperative for the configured host group.
- `PYTHONDONTWRITEBYTECODE=1` is set for the API container so non-root runtime
  processes do not try to create Python bytecode under `/app`.
- The API container drops Linux capabilities, sets `no-new-privileges`, and
  runs with a read-only root filesystem plus explicit tmpfs scratch mounts.

For existing root-owned generated state, use the dry-run-first normalizer:

```bash
scripts/normalize_state_permissions.sh
sudo scripts/normalize_state_permissions.sh --apply
```

The normalizer tightens `$SPACEGATE_STATE_DIR` itself non-recursively, then
targets generated/admin paths under it (`admin`, `backups`, `cache`, `logs`,
`out`, `reports`, and `served`). It deliberately does not touch `raw/` or
`cooked/`.

### Admin Visual QA

Admin v2 has a Playwright visual QA harness for layout and usability checks.
It captures desktop and mobile screenshots, browser console errors, failed
network requests, and basic layout overflow metrics for the core Admin screens.

Run:

```bash
scripts/run_admin_visual_qa.sh
```

By default the runner uses the official Playwright Docker image so Photon does
not need browser GUI dependencies installed on the host. Use
`scripts/run_admin_visual_qa.sh --local` after installing local Playwright
browser dependencies.

Reports and screenshots are written outside git under:

```text
$SPACEGATE_STATE_DIR/reports/admin_visual/<run_id>/
```

Durable screenshots and per-viewport summaries live in
`captures/<viewport>/`. The harness records requested/final URLs and fails with
`wrong_app` if routing sends it to the public CoolStars UI instead of Admin.
The default target is `https://photon.spacegates.org/admin/`; Docker mode adds
`photon.spacegates.org:10.0.0.12` to the container hosts file so local OAuth
cookies and the tested origin match.

Authentication:

- Without `SPACEGATE_ADMIN_STORAGE_STATE`, the harness captures the Admin auth
  gate and records `auth_required`.
- To capture authenticated screens, create a Playwright `storageState` JSON
  from an already authenticated Admin browser session:

```bash
scripts/create_admin_storage_state.sh
```

Then paste the Admin request `Cookie` header into the hidden prompt. The
default output is:

```text
$SPACEGATE_STATE_DIR/admin/playwright/admin-storage-state.json
```

`scripts/run_admin_visual_qa.sh` uses that default storage state automatically
when the file exists. To use another storage file, pass it explicitly:

```bash
SPACEGATE_ADMIN_STORAGE_STATE=/secure/local/path/admin-storage-state.json \
  scripts/run_admin_visual_qa.sh
```

Treat Cookie headers and storage state files like session secrets. Keep them
outside git, do not paste them into chat or logs, and delete the storage state
when it is no longer needed.

To get the Cookie header from an authenticated browser session:

1. Open `https://photon.spacegates.org/admin/` and sign in.
2. Open browser developer tools, then the Network tab.
3. Reload the Admin page or click an Admin API request such as
   `/api/v2/auth/me`.
4. Copy the request header named `Cookie`.
5. Run `scripts/create_admin_storage_state.sh` on Photon and paste the Cookie
   header into the hidden prompt.

Execution modes:

- Docker mode is the default and uses `mcr.microsoft.com/playwright` with host
  networking and host UID/GID. It avoids installing browser libraries on
  Photon.
- Local mode uses the host Node/Playwright install:

```bash
scripts/run_admin_visual_qa.sh --local
```

If local mode fails on missing Chromium libraries, either install Playwright's
browser dependencies with sudo from an interactive shell:

```bash
cd srv/admin-web
sudo npx playwright install-deps chromium
```

or use the default Docker mode.

## Operations, Jobs, and Audit Workspace

The embedded Admin UI currently exposes these operational surfaces:

- action catalog: allowlisted actions with parameter schemas, role checks, risk
  levels, and confirmation phrases
- action runner: queued background jobs with a single-worker default
- job history: job metadata, command/native execution plan, status, timestamps,
  exit code, and error message
- job logs: chunk polling and full log download from
  `$SPACEGATE_STATE_DIR/admin/jobs`
- cancellation: queued jobs only; running jobs are not interrupted
- backups: admin DB snapshots and release metadata snapshots
- audit log: auth, admin action, inference, and query events with filters,
  actor identity metadata, and correlation IDs

Admin v2 should migrate this as an **Operations** workspace, with Audit either
as a tab in that workspace or a persistent adjacent top-level screen. The mental
model should be:

1. Action launcher: "what can I safely do next?"
2. Jobs: "what is running, what happened, and what did it output?"
3. Backups/recovery: "what can I roll back, and what must I back up first?"
4. Audit trail: "who did what, from which route/request, and what correlated
   records explain it?"

### Operator Model

Jobs are execution records. Audit entries are accountability records. Dossier
journal entries are scientific evidence-history records. The UI should link
these records but not collapse them.

- A job should link to its launch audit event, completion audit event, log file,
  parameters, command/native execution plan, and any produced reports.
- Job rows should show the requesting actor when the admin user record is still
  available, and job detail should expose correlated audit entries.
- Job detail should show derived output hints for expected or detected reports,
  backups, release metadata, snapshots, retention summaries, and profile files.
  These hints are for operator navigation and troubleshooting; immutable
  provenance still lives in the build/report/database artifacts themselves.
- Job detail should show structured lifecycle events for queue/start/execution
  and terminal states. Older jobs may show derived timeline events from existing
  timestamps until enough structured history exists.
- Job logs should be readable in-browser with severity highlighting, search,
  line filtering, summary counts, reload, embedded and full-page reader modes,
  raw text access, and separate download options.
- An audit event with `correlation_id` or `job_id` should link back to the
  matching job detail.
- Future agency jobs should additionally link to Evidence Portfolio journal
  entries, source files, extraction sets, claims, proposals, and review records.

### Workspace Layout

Use a dense operations console rather than a card pile.

Header summary:

- active jobs, queued jobs, recent failed jobs
- latest high-risk action
- admin DB backup age and release metadata backup age
- current served build and last verification status when available
- job runner capacity from `SPACEGATE_ADMIN_MAX_RUNNING_JOBS` and
  `SPACEGATE_ADMIN_MAX_QUEUED_JOBS`

Primary tabs:

1. **Runbook**
   - grouped action launcher with sequences and safe next steps
   - workflow rails for common sequences:
     - Build Database -> Verify Build -> Publish Database -> retention
     - Score Coolness -> Generate Snapshots -> Save Profile -> Activate Profile
     - Backup Admin DB -> Restore Admin DB
     - Backup Release Metadata -> Restore Release Metadata
   - each action should show purpose, prerequisites, writes/outputs, risk,
     expected duration, required confirmation phrase, and next recommended step
2. **Jobs**
   - queue table with status, action, actor, created/started/finished times,
     duration, exit code, and compact error
   - split-pane job detail with parameters, execution plan, log tail, full log
     download, linked audit events, and produced artifact hints
   - status traps for missing log file, queued too long, running unusually long,
     failed exit code, and stale page data
3. **Backups**
   - admin DB snapshot list and release metadata snapshot list
   - create-backup actions near restore actions
   - restore warnings that explain what state is affected and what is preserved
   - "backup first" guidance before high-risk restore paths
4. **Audit Trail**
   - presets for auth, admin actions, inference, searches, errors, and denies
   - exact filters for event type, result, request id, actor id, and correlation
     id when available, with an obvious reset path
   - timeline/list with selected-event actor, route, correlation, and JSON detail
   - links from correlated audit events to job detail or inference endpoint

### Action Grouping

Do not present all actions as one flat list. Group by operator intent:

- Build pipeline: `build_database`, `verify_build`, `publish_db`
- Presentation generation: `score_coolness`, `save_coolness_profile`,
  `apply_coolness_profile`, `generate_snapshots`
- Backups and recovery: `backup_admin_db`, `restore_admin_db`,
  `backup_release_metadata`, `restore_release_metadata`
- Service control: `restart_services`, `stop_services`
- Advanced/hidden: sliced builds and future one-off maintenance actions

The action catalog should eventually grow structured metadata instead of
forcing the React UI to hardcode guidance:

- `prerequisites`
- `writes_to`
- `outputs`
- `expected_duration`
- `preflight_checks`
- `success_next_actions`
- `failure_next_actions`
- `warnings`
- `docs_links`

### Hints and Safety Feedback

The UI should be generous with inline operational guidance, especially for a new
administrator.

- Build actions should remind operators that raw/cooked/out/served artifacts are
  managed by scripts, not manual edits.
- Verify should be framed as the required checkpoint before promotion,
  retention, or deployment recommendations.
- Publish should explain that it updates public download metadata, not the
  immutable science build itself.
- Retention should remain unavailable or clearly disabled while ingest/build
  jobs are running, and should only run after successful promotion and
  verification.
- Restore Admin DB should warn that auth, allowlist, sessions, and audit tables
  may change; active job references must remain valid.
- Restore Release Metadata should warn that it changes download metadata and
  optionally the `current` symlink.
- Stop Services should warn that the Admin UI may disconnect and should require
  explicit confirmation.
- Failed jobs should show the last log lines, exit code, compact error, and
  links to the full log and correlated audit events.
- Snapshot generation should warn when requesting more than 10,000 coolness
  systems, but should not hard-cap Photon-era bulk runs from Admin.
- Empty states should explain what the operator can do next, not merely say
  "no rows."

### Backend Gaps to Close

The backend now exposes action metadata from `ActionSpec` and
`GET /admin/operations/status` for runner capacity, backup age, recent failures,
build artifacts, retention readiness, and runbook grouping. Future operators
will benefit from several additional API additions:

- optional `parent_job_id` / `predecessor_job_id` so sequential workflows can be
  represented explicitly

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

When an inference runtime is outside the API container, store the URL that the
API container can actually reach. On Photon, host-side tools may use
`http://127.0.0.1:8001/v1`, while the Admin/API container should use the
container-network URL `http://photon-vllm:8000/v1` when `photon-vllm` is joined
to the app network. Use `SPACEGATE_CONTAINER_LLM_BASE_URL` for that
container-side override so `SPACEGATE_LLM_BASE_URL` can remain convenient for
host-side tools.

Minimum endpoint record:

- endpoint id and provider
- base URL
- auth mode, without displaying secrets
- encrypted stored API key or environment variable pointer
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
- `$SPACEGATE_BULK_DIR` (`/mnt/space/spacegate` on Photon): bulk research
  material, archived papers, retrieved source documents, OCR/intermediate text,
  large dossier attachments, and reusable science-document cache
- `/data/models`: model weights and inference caches shared across projects
- `/srv/spacegate`: repo checkouts and host-local config

The USB-backed `/mnt/space/spacegate` is large and fast but less trustworthy
than internal storage. Anything there that matters to scientific traceability
must be referenced by metadata stored in `disc`/admin state with hashes,
retrieval timestamps, and source URLs. The cache can be regenerated or repaired
from those records.

Docker deployments should bind-mount `$SPACEGATE_BULK_DIR` into the API
container at the same path so Runtime diagnostics and future research/archive
workflows see the same location as host-side tools.

## First Implementation Slices

1. Admin shell extraction
   - scaffold the dedicated React/Vite Admin frontend at `srv/admin-web`
   - serve it from nginx at `/admin/`
   - move Admin UI out of the large FastAPI HTML string into the dedicated admin
     frontend one top-level workspace at a time
   - keep existing `/api/v2/admin/*` APIs and auth
   - preserve the old v1 routes as deprecated aliases
   - keep the embedded FastAPI Admin UI only as a temporary fallback until each
     migrated workspace reaches parity

2. Overview + Inference foundation
   - show auth/session status, service health, current build, container status,
     and endpoint probes
   - start with a read-only Overview page in React that aggregates admin status,
     dataset status, jobs, and inference endpoint health
   - read model ids from configured dynamic endpoints
   - support add/remove endpoint records and stored or environment-backed API
     keys
   - show basic endpoint/model usage counters once the inference runner records
     calls
   - edit per-endpoint role defaults for `discover`, `prune`, `compile`,
     `identify`, `extract`, `criticize`, `adjudicate`, and `narrate`
   - run bounded endpoint smoke tests that record usage telemetry without
     persisting prompt text
   - show recent eval reports, role-suitability summaries, and quarantined
     anomaly inbox items
   - add Runtime workspace backed by `/api/v2/admin/runtime/status` for
     read-only path, storage, auth, config, process, and endpoint diagnostics

3. Operations + Audit migration
   - migrate action launcher, job queue, selected job log, backups, and audit
     filters into React
   - preserve current API behavior while improving grouping, guidance, and
     correlation links
   - add backend action metadata and operations summary endpoint only after the
     first React screen proves the operator workflow

4. Dataset migration
   - migrate served dataset status, source contribution, overlap, determinism,
     storage, and runtime diagnostics into React
   - keep the first pass read-heavy; slice/rebuild controls should remain
     deliberate and tied to the build runbook
   - preserve `/api/v2/admin/status/dataset` as the single source of truth for
     this workspace

5. Agency read model
   - define API shapes for Evidence Portfolio, Source File, Extraction Set,
     Findings, Proposals, Review, and Journal Entry
   - start with read-only/mock or report-backed data where production tables do
     not exist yet
   - first pass exposes `/api/v2/admin/agency/status` and a React Agency shell
     for workflow stages, storage readiness, eval reports, anomaly inbox, and
     the portfolio-scoped interaction model

6. Work queue and journal persistence
   - seed portfolios from coolness/adjudication queues
   - append journal entries for queue seed, source retrieval, extraction, and
     review actions
   - first persistence foundation uses admin DB tables for Evidence
     Portfolios, Source Files, Extraction Sets, Findings, and Journal Entries
   - `/api/v2/admin/agency/portfolios` and
     `/api/v2/admin/agency/portfolios/{dossier_id}` expose read-only portfolio
     state for the React Admin workspace
   - `/api/v2/admin/agency/seed-candidates` exposes ranked coolness candidates
     from the current `disc.coolness_scores`
   - `/api/v2/admin/agency/portfolios` `POST` creates only an admin dossier and
     first journal entry; it does not run retrieval, extraction, model calls,
     claim/proposal generation, publication, or layer materialization

7. Human-gated review
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
