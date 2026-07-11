# Spacegate Agent Framework

This document defines how agent-driven enrichment fits into Spacegate's layered architecture.

Related documents:

- `docs/PROJECT.md`: overall project direction and layer boundaries
- `docs/AGENTS.md`: prompt/runtime behavior for enrichment agents
- `docs/AGENT_ALLOWLIST.md`: internet source policy and trust tiers
- `docs/SCHEMA_ARM.md`: audited supplemental science and adjudication tables
- `docs/SCHEMA_DISC.md`: audited citations, claims, factsheets, expositions, and other derived artifacts
- `docs/AGENT_EVALS.md`: role-based local-model evaluation and anomaly inbox policy

## Purpose

Agents exist to expand Spacegate's evidence-backed explanatory layer without mutating canonical science rows.

Primary functions:

- prioritize scientifically or narratively valuable objects for enrichment
- gather source material from an explicit allowlist
- extract evidence and proposed facts with citations
- write reproducible factsheets and exposition into `disc`
- propose higher-confidence science values, ambiguity resolutions, and missing-field fills into `arm`
- abstain cleanly when evidence is weak, contradictory, or underspecified

## Layer Contract

Agents must respect the layer model:

- `core`: deterministic conservative astronomy baseline; agents never edit it directly
- `arm`: curator-managed supplemental science plus agent proposals, ambiguity dossiers, adjudication candidates, and accepted science overlays
- `disc`: curator-managed citations, normalized claims, evidence manifests, factsheets, expositions, and other presentation-layer outputs
- `rim`: user/worldbuilding content only; not part of the scientific enrichment path

Serve-time precedence may later prefer accepted `arm` or `disc` overlays over `core` for selected hot-path facts, but that precedence must remain explicit, auditable, and reversible.

Operational mutability:

- `arm` and `disc` are mutable through administrative curation, review tooling, and generator runs
- `arm` and `disc` remain immutable from the public/user perspective
- changes should preserve lineage and review history instead of erasing prior state where practical

## Current Execution Scope

Near-term agent work should focus on:

1. evidence-link and citation pipeline in `disc`
2. deterministic object dossiers for high-value systems, stars, and planets
3. intake from `arm.adjudication_candidates`, `arm.multiplicity_adjudication_candidates`, and `arm.host_resolution_candidates`
4. local-LLM backed extraction experiments before spending cloud-model budget
5. WISE/CatWISE/AllWISE evidence packets for ultracool dwarfs, dusty systems,
   and ambiguous infrared counterparts. These packets may include matched WISE
   identifiers, photometry, motion evidence, quality flags, and IRSA image
   cutouts, but they remain evidence until reviewed.

## Claim Normalization

The claim layer should be normalized so claim-specific rows stay small.

Operator-facing vocabulary:

- `Evidence Portfolio`: the object-level investigative case file (`object_dossiers` plus attached source files, extraction sets, findings, proposals, reviews, and publication artifacts).
- `Source File`: one archived source document in the portfolio. Stored as `source_documents` plus a `claim_bundles` row with `bundle_kind='retrieval'`.
- `Extraction Set`: one saved extraction pass over a source file. Stored as a `claim_bundles` row with `bundle_kind='extraction'`.
- `Findings`: normalized extracted claims (`extracted_claims`).
- `Proposals`: candidate database changes generated from findings and still requiring review.
- `Review` / `Verdict`: accepted, rejected, deferred, or escalated decisions.

Storage note: `claim_bundles` remains the schema table and `claim_bundle_id` remains the API identifier. UI and runbooks should prefer `Source File` and `Extraction Set` unless discussing schema internals.

Recommended shape:

1. `source_documents`
   - one row per fetched paper/archive page/catalog document
   - holds shared metadata like URL, publisher, trust tier, publication date, and content hash
2. `claim_bundles`
   - one row per object-scoped source-file or extraction-set record
   - holds shared extraction metadata like method, model, prompt version, and bundle hash
3. `extracted_claims`
   - one row per concrete factual claim
   - holds only object key, predicate, value, units, confidence, role, and status plus foreign keys

This avoids attaching eight or more provenance fields to every scalar value when several claims came from the same source pass.

Implementation note:

- Active Evidence Portfolio workflow state is currently implemented in the
  Admin SQLite database as `agent_object_dossiers`,
  `agent_source_documents`, `agent_claim_bundles`,
  `agent_extracted_claims`, and `agent_portfolio_journal_entries`.
- These rows are mutable admin/operator state. They are not public served
  science artifacts.
- Reviewed publication surfaces in `disc` and proposal/overlay surfaces in
  `arm` should be materialized deliberately from those rows, with generator
  versions, hashes, citations, and review state preserved.

Portfolio interaction note:

- Forked chat or side-agent sessions must be separate interaction records with
  their own session id, parent interaction id, explicit context snapshot, and
  context snapshot hash.
- A fork may read selected portfolio context, but it must not share hidden
  mutable memory with the parent run and must not write claims, proposals, or
  publication artifacts by default.
- Outputs from forked interactions can enter the evidence process only through
  explicit reviewed imports that preserve prompt/runtime metadata, citations,
  and journal history.

## Claim Family Registry

Claim expansion should be governed by an explicit registry, not by allowing arbitrary new predicates whenever a model emits something interesting.

Primary claim families:

1. `identity`
   - catalog IDs
   - external archive identifiers
   - alias strings observed in sources
2. `naming`
   - preferred common-name candidates
   - display-name candidates
   - name-scope and precedence hints
3. `scalar_param`
   - object-bound physical parameters such as mass, radius, temperature, metallicity, period
4. `structure`
   - multiplicity/membership statements
   - component/subcomponent relationships
   - host-object statements
5. `orbit_relation`
   - orbital element sets for a relation between components/subsystems
   - dynamical notes tied to a specific pair or barycentric orbit
6. `context`
   - explanatory summaries
   - architecture notes
   - discovery or atmosphere notes

Policy:

- new claim families or predicates should be added by schema review, not ad hoc prompt expansion
- extraction prompts should remain family-aware and object-aware
- only a subset of claim families should ever become serve-time overlays

## Concrete Workflow Objects

The minimum concrete workflow should use four object families:

1. `object_dossiers` / `agent_object_dossiers`
   - one operational dossier per target object
   - tracks enrichment state, freshness, and publication readiness
2. `agent_work_items` in the operational agent store
   - durable overlay above generated ranking tables such as `disc.enrichment_priority_queue` and `arm.*_candidates`
   - preserves operator state like claim/bump/defer/block/complete without mutating deterministic source queues
3. `source_documents` / `agent_source_documents`
   - one row per fetched source document or canonical archive page
4. `claim_bundles` and `extracted_claims` / `agent_claim_bundles` and
   `agent_extracted_claims`
   - source-file row = one archived source attached to an evidence portfolio
   - extraction-set row = one extraction/review pass over one source for one target object
   - finding = one narrow factual statement
5. `agent_portfolio_journal_entries`
   - append-friendly plain-language timeline of queue, retrieval, extraction,
     review, interaction, and publication events
6. proposal / overlay rows in `arm`
   - proposal rows reference supporting claim IDs
   - accepted overlays reference the selected supporting claim IDs and remain audit-visible

## Adversarial Review Contract

Review is not a confirmation pass.
It is a fault-finding pass that attempts to falsify proposals before acceptance.

The intended order is:

1. deterministic plausibility checks
2. adversarial local-model review
3. frontier-model escalation for the hard tail only
4. human/admin accept or reject

This contract matters because extraction failures are often structured rather than random:

- wrong-subject binding
- row/column drift
- unit mistakes
- decimal/sign errors
- astrophysically implausible values attached to the wrong object

Reviewer policy:

- do not trust extractor confidence by itself
- preserve the exact local source context needed to attack a claim later
- store review flags as structured metadata, not just prose
- treat unusually interesting claims as more suspicious, not less

## Dossier Subject vs Claim Subject

A dossier is a research container, not a guarantee that every accepted claim targets the dossier root object.

Rules:

- every dossier has one primary target object
- every claim must also have its own explicit subject target
- the claim subject may be:
  - the dossier target itself
  - a resolved related object inside the dossier target's hierarchy
  - a resolved orbit/subsystem relation
- unresolved related-object claims should not be promoted into reviewed overlays until the subject mapping is explicit

Examples:

- a `Castor` system dossier may contain:
  - system-level multiplicity claims about `system:wds:07346+3153`
  - star-level temperature claims about `Castor Aa`
  - orbit-element claims about the `Castor A-B` or `Castor AB-C` relation
- the bundle remains attached to the Castor dossier, but the individual claims must target the exact star or orbit relation they describe

## Evidence Graph vs Accepted Dynamical Tree

Multiplicity catalogs and literature-derived orbital solutions should not be forced directly into one public hierarchy.

Spacegate should treat hierarchical multiple systems as two related products:

1. evidence graph
   - source-facing nodes and edges from `arm.component_entities`, `arm.system_hierarchy_edges`, and `arm.orbit_edges`
   - may be incomplete, conflicting, pair-centric, or missing intermediate subsystem nodes
2. accepted dynamical tree
   - one reviewed reduction of that evidence into the best current physical hierarchy for navigation, orbit rendering, and simulation
   - may contain reviewed derived internal subsystem nodes that were not handed to us cleanly by one source catalog

Hard rules:

- catalogs are evidence inputs, not the final authority for the reduced dynamical tree
- the LLM may help extract explicit structure/orbit claims from literature, but it must not silently invent the published hierarchy
- derived hierarchy should come from deterministic reduction over reviewed evidence, not opaque prompt behavior

### Example: Castor

The physically useful reviewed tree for Castor is:

- `Castor`
- children: `AB`, `C`
- `AB` children: `A`, `B`
- `A` children: `Aa`, `Ab`
- `B` children: `Ba`, `Bb`
- `C` children: `Ca`, `Cb`

The source catalogs already give useful evidence for `Aa-Ab`, `Ba-Bb`, `Ca-Cb`, and subsystem labels `A/B/C`, but they do not consistently materialize the composite subsystem `AB` or the outer `AB-C` relation as clean runtime-ready structure.

Therefore:

- reviewed derived subsystem nodes such as `AB` belong in `arm`
- reviewed hierarchy edges such as `AB -> A` and `AB -> B` belong in `arm`
- reviewed orbit relations such as `AB-C` belong in `arm`
- future illustrative fallbacks for missing dynamics still belong in `disc`

### Design implications

- a subsystem node and an orbit relation are not the same thing
- subsystem nodes exist so the browse tree, planet hosts, and simulation hierarchy can refer to stable internal groups such as `AB`
- orbit relations exist so orbital elements can be attached to sibling pairs such as `Aa-Ab` or composite relations such as `AB-C`
- the accepted dynamical tree should usually be binary-tree shaped for clearly hierarchical systems, but unresolved/non-hierarchical systems should remain unresolved rather than being forced into a false clean tree

## Related-Object Target Resolution

The extraction/review pipeline needs a deterministic target-resolution stage between raw extraction and accepted use.

Resolution modes:

- `dossier_subject`
  - claim clearly applies to the dossier target
- `resolved_related_object`
  - claim maps cleanly to a child star, planet, subsystem, or companion object
- `resolved_relation`
  - claim maps to a specific orbit/subsystem relation, not a scalar object row
- `ambiguous`
  - claim cannot be attached safely and must stay review-only

Resolver inputs may include:

- canonical stable keys already present in the source text
- catalog component labels such as `Aa`, `Ab`, `C`, `AB-C`
- WDS/MSC/ORB6 pair labels
- local object aliases and cross-catalog IDs
- hierarchy context from `arm.component_entities`, `arm.system_hierarchy_edges`, and `arm.orbit_edges`

Hard rule:

- if a claim cannot be resolved safely to one subject, keep it as ambiguous evidence rather than guessing

## Claim Adjudication Axes

Every extracted claim should be classified on two independent axes before proposal generation:

1. `schema_fit`
   - `core_field`
   - `arm_field`
   - `disc_field`
   - `schema_gap`
   - `reject`
2. `rigor_tier`
   - `catalog_grade`
   - `literature_grade`
   - `contextual`
   - `illustrative`

Policy:

- `core_field`
  - hot-path scalar fields already supported by the reviewed proposal surface
- `arm_field`
  - reviewed science claims that belong in audited supplemental science or dedicated review tables
- `disc_field`
  - contextual, narrative, or illustrative claims that may still be useful in factsheets/exposition
- `schema_gap`
  - scientifically useful claims that fit Spacegate conceptually but do not yet have a reviewed schema destination
- `reject`
  - unsupported, low-value, or too-ambiguous claims

`schema_gap` is an explicit queue, not a silent drop. This is where the system should capture values like stellar `log g`, stellar density, linear luminosity, mutual inclinations, and other useful literature facts that do not yet have a first-class field.

## Anomaly Inbox

Agent runs may notice useful issues that are not direct claim acceptances:
catalog conflicts, source conflicts, identity/host ambiguity, schema gaps, stale
consensus, plausibility failures, observational limitations, or interesting
hypotheses. These should be tracked as quarantined anomaly-inbox items.

Policy:

- an anomaly is a review signal, not accepted science
- every anomaly must reference the source or eval case that triggered it
- anomaly items may motivate a dossier, proposal, deterministic check, or human
  review, but they must not mutate `core`
- high-impact or speculative anomalies require accepted supporting claims before
  any public-facing narrative or overlay uses them

The first concrete implementation is the eval-report anomaly inbox emitted by
`scripts/agent_eval.py`; production persistence should later map the same
concept into reviewed `disc`/`arm` rows.

## Schema-Gap Workflow

When extraction finds a useful but unsupported claim:

1. keep the normalized `extracted_claims` row
2. classify it as `schema_fit = schema_gap`
3. generate a `schema_gap_proposal`
4. review it into one of:
   - `promote_to_core_candidate`
   - `add_arm_field`
   - `add_disc_field`
   - `discard`

The first implementation may accept/reject the suggested disposition rather than offering full branching review in the UI, but the stored proposal should preserve enough metadata to support the richer adjudication later.

## Review State Machine

The review state machine should be explicit and small:

### Dossier lifecycle

- `seeded`
  - object has been queued but not yet worked
- `gathering`
  - sources are being collected
- `extracted`
  - claims exist, but no human/admin review has happened yet
- `review_ready`
  - enough evidence exists for review or publication
- `published`
  - factsheet/exposition and any accepted overlays are live
- `stale`
  - source set or canonical object state changed enough to require refresh
- `archived`
  - dossier is intentionally retired but retained for audit

### Claim lifecycle

- `proposed`
  - extracted and available for review
- `accepted`
  - approved as a trusted supporting claim
- `rejected`
  - reviewed and intentionally not used
- `superseded`
  - once acceptable, but replaced by a newer or stronger claim

### Proposal lifecycle in `arm`

- `proposed`
  - generated from claims, not yet active for serving
- `accepted`
  - approved for serve-time use or deterministic promotion policy
- `rejected`
  - not approved
- `superseded`
  - replaced by a later accepted proposal

### Active overlay lifecycle

- `active`
  - currently selected as the serve-time override
- `superseded`
  - replaced by a newer active overlay
- `revoked`
  - intentionally disabled without deleting lineage

Transition rule:

- state transitions should be append/audit friendly
- deleting prior claims/proposals/overlays is discouraged; status changes should preserve lineage

## Serve-Time Precedence Contract

Serve-time precedence must stay narrow and explicit.

### Eligible overlay classes

Accepted `arm.accepted_science_overlays` may supersede `core` only for allowlisted hot-path scientific fields such as:

- stellar scalar facts:
  - `star.teff_k`
  - `star.mass_msun`
  - `star.radius_rsun`
  - `star.luminosity_log10_lsun`
  - `star.age_gyr`
  - `star.metallicity_feh`
  - `star.rotation_period_days`
- planetary scalar facts:
  - `planet.mass_mearth`
  - `planet.minimum_mass_mearth`
  - `planet.radius_rearth`
  - `planet.density_g_cm3`
  - `planet.equilibrium_temp_k`
  - `planet.semi_major_axis_au`
  - `planet.orbital_period_days`
- system explanatory summary facts that are not identity-defining

### Unit contract

Overlay proposals store hot-path numeric values in one canonical schema unit per field, while preserving the source-native value/unit in proposal review notes.

- `planet.mass_mearth` canonicalizes to `Mearth`; source `MJupiter`/`MJup` values are converted at proposal generation and retained in `review_notes.unit_contract`.
- `planet.minimum_mass_mearth` stores radial-velocity/transit-timing lower-limit mass terms such as `m sin i` separately from true mass and canonicalizes to `Mearth`.
- `planet.radius_rearth` canonicalizes to `Rearth`; source `RJupiter`/`RJup` values are converted at proposal generation and retained in `review_notes.unit_contract`.
- reviewer/frontier packets should show both source-native and canonical values when a conversion was applied, so adjudicators can reason from the paper language without allowing mixed units into serve-time overlays.

### Non-eligible classes

Generic overlay precedence must not be used for:

- stable identities and keys
- canonical inventory membership
- hierarchy/containment edges
- host-object identity
- lifecycle state
- raw source aliases / identifier crosswalks
- coordinates and system anchors unless there is a dedicated deterministic review workflow for them
- orbit relation element sets or subsystem dynamics

Those need dedicated adjudication/promotion paths, not generic field overrides.

## Identity and Naming Policy

Identity and naming deserve dedicated workflows because they influence search, cross-referencing, and presentation but must not destabilize canonical inventory.

Recommended split:

- `identity` claims capture observed aliases and catalog IDs from sources
- `naming` claims capture preferred-name candidates and display-name evidence
- accepted identity/naming outputs should land in dedicated reviewed tables, not `accepted_science_overlays`

Rules:

- no accepted identity/naming row may change a stable key
- accepted aliases and IDs should expand lookupability and cross-reference coverage
- identifier adjudication must preserve binding scope; only `same_object` designators may become exact lookup/cross-reference links without an explicit collision override
- system-level or historically unresolved designators for modern component targets should be quarantined as reviewed unresolved evidence instead of silently becoming component aliases
- final frontier field adjudication packets must include identity/designator binding context when claims depend on source-local or historically unresolved names, and the adjudicator must treat non-`same_object` scopes as target-binding risks
- preferred-name selection should remain reversible and provenance-bound
- competing preferred names such as `Alpha Centauri` vs `Toliman` must remain comparable rather than silently overwritten

## Orbit Relations and Simulation Readiness

For future animation and dynamics work, the system needs a stable identity for orbital relations, not just for objects.

Required model distinction:

- object-bound scalar claims attach to a stable object key
- orbital element claims attach to a stable orbit/subsystem relation key

Observed/curated orbital solutions:

- belong in `arm`
- must remain provenance-bound and reviewable
- may drive science-facing orbit displays when accepted

Illustrative or inferred orbital solutions:

- belong in `disc`
- may support animation, teaching graphics, or plausible simulation when the science record is incomplete
- must always expose explicit provenance and reliability labels such as `illustrative`, `inferred`, or `simulation_only`

Hard rule:

- models may suggest or infer simulation parameters, but inferred values must never be presented as if they were observed catalog/literature measurements

### Resolution order

For an allowlisted overlay field:

1. if exactly one `active` overlay exists for `(stable_object_key, field_name)`, serve that value
2. otherwise fall back to `core`
3. if multiple active overlays somehow exist, fail closed to `core` and emit an admin-visible integrity error

### Provenance requirement

Every served overlay must expose:

- the selected value
- the supporting claim IDs
- the accepting actor/tool
- acceptance timestamp
- the underlying conservative `core` value when available for audit/inspection

### Publication rule

Narratives and factsheets in `disc` may describe accepted overlays, but they must not silently imply that `core` itself changed.

## Hypothetical Dossier Format

An evidence portfolio should have:

1. structured database rows
   - `object_dossiers`
   - `source_documents`
   - `claim_bundles`
   - `extracted_claims`
2. optional archived portfolio package
   - compressed JSON/Markdown/plain-text package for audit and later refresh

Recommended portfolio package sections:

- object header
  - `stable_object_key`, object type, display name, queue priority, dossier status
- current baseline snapshot
  - selected hot-path `core` values relevant to the object
- source inventory
  - source URLs, domains, trust tier, publication dates, access times
- extraction sets
  - one section per source pass, with method/model/prompt metadata
- normalized findings
  - accepted, rejected, contradictory, and contextual findings
- proposal summary
  - candidate overlays or adjudication proposals generated from the claims
- publication summary
  - factsheet/exposition hashes and publication timestamps when present
- audit trail
  - state transitions and operator/reviewer actions

The archived package should be treated as a convenience/audit artifact, not as the source of truth over the normalized database rows.

## Dossier Retention and Storage

Retention should distinguish hot structured data from cold archival artifacts.

### Keep hot in `/data`

- normalized database rows for active and published dossiers:
  - `object_dossiers`
  - `source_documents`
  - `claim_bundles`
  - `extracted_claims`
  - accepted overlays / proposals
- small textual summaries or hashes needed for review and serve-time provenance

These rows remain useful after publication because:

- future refreshes need prior claims for comparison
- supersession and contradiction tracking require lineage
- accepted overlays must keep their supporting references
- stale detection needs the prior dossier state

### Push cold to `/mnt/space`

- archived dossier packages
- extracted text snippets beyond what is needed inline for review
- optional source snapshots for published/accepted dossiers
- bulky intermediate retrieval artifacts

Recommended root:

- `/mnt/space/spacegate/agent_archive/`

### Do not retain forever

- transient prompt assembly buffers
- temporary HTML/PDF downloads once normalized extraction is complete, unless the dossier is published or otherwise marked worth archiving
- duplicate retrieval artifacts with the same `source_locator_hash` and `content_hash`

### Retention policy recommendation

- keep normalized claim-layer rows indefinitely unless explicitly archived/retracted by admin policy
- keep archived dossier packages for `published`, `accepted`, or historically important dossiers
- keep unpublished/rejected bulky artifacts for a short TTL only, for example 30-90 days
- compress dossier packages with a strong text-friendly codec such as `zstd`

### Rough mass estimate

If you store only normalized rows plus compact summaries:

- roughly tens of KB per enriched object is realistic
- 100k enriched objects should be comfortably single-digit GB to low tens of GB

If you also archive compressed dossier packages with excerpts and review notes:

- roughly 10-60 KB compressed per object is plausible for text-heavy dossiers
- 100k dossiers would likely land in the low single-digit GB range up to several GB
- 1M dossiers would likely move into the tens of GB range, which is feasible on `/mnt/space` but not a great fit for hot `/data`

Conclusion:

- yes, it is wise to retain dossiers after claims and metadata are saved, but mostly as compact normalized rows plus compressed cold archives
- no, it is not wise to keep all temporary raw retrieval artifacts in hot storage indefinitely

## Observability, Monitoring, and Audit Traps

The agent pipeline should emit stage-level events even when a step abstains or partially succeeds.

The `agent_events` stream should make it easy to answer:

- which stage failed
- which object/source/model was involved
- whether the failure is transient, policy-related, or a schema/design gap
- whether the run is safe to retry automatically

Recommended tracked conditions:

### Intake / retrieval

- source host not allowlisted
- redirect escapes the allowlist
- bot-wall / anti-automation challenge (`403`, challenge redirect, JS interstitial)
- repeated domain-specific fetch failure spikes
- content-type mismatch (HTML landing page vs PDF vs binary blob)
- download too large / truncated / timeout
- archive write failure or archive path unavailable
- retrieved text too short to be useful
- duplicate source document already archived

### Parsing / extraction

- PDF/HTML text extraction yielded no text
- extracted text exceeds configured input budget and had to be truncated
- likely context-window overflow risk before LLM submission
- model timeout / connection failure / remote server unavailable
- model returned empty content, reasoning-only content, or invalid JSON
- schema validation failure on structured output
- claim count unexpectedly zero for a source that looks content-rich
- extraction produced only unsupported predicates
- extraction produced claims for the wrong object family
- extraction produced ambiguous subject targets that could not be resolved safely
- duplicate/overlapping note claims merged within a bundle

### Proposal generation / review

- claim maps to a useful concept but no reviewed predicate/field exists yet
- proposal would duplicate an existing accepted overlay/name/id
- proposal conflicts with current `core` or accepted `arm` values
- proposal references missing supporting claims
- proposal acceptance would create a structural cycle or orphaned subtree
- reviewed orbit relation references missing runtime nodes
- accepted subsystem reduction removes all core-backed descendants

### Publication / serving

- factsheet generation found no accepted reviewed evidence
- citation row could not be anchored to a source excerpt
- source URL exists but no compact excerpt is available
- reviewed publication payload differs from runtime hierarchy reduction
- accepted overlay is no longer needed because refreshed `core` caught up
- refreshed `core` conflicts with an accepted overlay and needs reconciliation review

### Infrastructure / retention

- agent archive path unavailable or offline (`/mnt/space` disconnected)
- hot `/data` usage crosses warning/error thresholds
- cold archive usage crosses warning/error thresholds
- stale dossiers or source archives exceed retention targets
- background admin job wedged, restarted, or reconciled on API startup

Event payload guidance:

- `event_type`
- `event_status`
- `stable_object_key`
- `dossier_id`
- source domain / source document id when applicable
- model id / prompt version when applicable
- structured `details_json` with retryability and counters

The important distinction is to separate:

- transient operational failure
- source-policy failure
- model/output failure
- schema gap
- genuine scientific ambiguity

If those collapse into one generic error string, the pipeline will be difficult to operate at scale.

## Pipeline Shape

Agents should operate as a staged pipeline:

1. Select target objects
   - prioritize by coolness plus review urgency
   - systems first, then stars, then planets unless a queue policy overrides that order
2. Build evidence dossier
   - gather allowed sources
   - keep URLs, trust scores, citation metadata, and retrieval timestamps
   - preserve contradictory claims rather than collapsing them early
3. Extract structured facts
   - prefer machine-readable catalog/archive values first
   - use LLM extraction only when needed and keep outputs bounded, typed, and cited
   - normalize shared provenance into source-document and extraction-set records rather than repeating it on every finding
   - classify each source first (`orbital_dynamics`, `stellar_parameters`, `planetary_characterization`, `structural_context`, `general_reference`)
   - select target families from that classification instead of asking one prompt to cover the entire dossier hierarchy
   - run focused passes by family:
     - system-level context and inventory
     - subsystem-level structure
     - star-by-star scalar extraction for component-specific papers
     - planet-by-planet scalar extraction for host/planet papers
     - relation-by-relation orbital extraction for dynamics papers
4. Produce outputs
   - `disc`: source documents, evidence sets, extracted findings, evidence links, factsheets, expositions, generated summaries
   - `arm`: proposed values, ambiguity resolutions, missing-field candidates, adjudication rows, and accepted science overlays that reference supporting claim IDs
5. Review / accept / supersede
   - proposed outputs remain explicitly reviewable
   - accepted outputs may become serve-time overlays
   - rejected or superseded outputs remain lineage-visible

Concrete linkage:

- `disc.extracted_claims.claim_id` is the atomic evidence reference
- `arm` proposal rows should store `supporting_claim_ids_json`
- accepted serve-time overlays in `arm` should record the exact accepted claim IDs that justified activation

Recommended next design slice before widening extraction:

1. add identity and naming claim families
2. add claim-subject resolution for related stars, planets, subsystems, and orbit relations
3. add stable orbit-relation keys plus reviewed orbital-solution ingestion in `arm`
4. add a separate `disc` path for illustrative/simulation-only orbital solutions
5. only then widen extraction prompts and proposal generation beyond the current scalar-note baseline

## Source Policy

Agents must use the source policy described in `docs/AGENT_ALLOWLIST.md`.
Machine-readable policy lives in `config/agent_source_allowlist.json`, with
operator overrides at `$SPACEGATE_STATE_DIR/config/agent_source_allowlist.json`
and previous runtime versions under
`$SPACEGATE_STATE_DIR/config/agent_source_allowlist.history/`.

Retrieval code must load the runtime JSON first and fall back to the shipped
repo default. An `enabled=false` source remains part of the audit trail but is
not allowed for retrieval or portfolio-context assembly.

Required rules:

- prefer Tier 0 and Tier 1 sources for factual claims
- use Tier 2 for contextual support, not canonical measurement authority
- use Tier 3 only as discovery leads that must resolve to better citations
- never use Tier 4 as a sole basis for scientific claims
- every nontrivial claim must have a citation trail
- off-allowlist browsing is not permitted without an explicit policy change

## Evidence and Claims

Each meaningful extracted claim should preserve:

- claim/value/unit
- confidence
- supporting extraction-set `claim_bundle_id`
- supporting `source_document_id` through the source file / extraction set

Shared metadata such as URL, domain, trust tier, extraction method, model, prompt version, and retrieval timestamp should usually live on the normalized parent records rather than every individual claim row.

Agents must keep unresolved contradictions visible. They should not average or silently merge disagreeing values.

## Adjudication Model

Review states should support at least:

- `proposed`
- `accepted`
- `rejected`
- `superseded`

Typical reasons for adjudication:

- multiple high-quality sources disagree
- host identity or hierarchy remains ambiguous
- a source suggests a higher-confidence value than the current `core` baseline
- a value is scientifically plausible but not yet strong enough to serve by default

## Budget and Runtime Policy

Use local inference on `positron` first for extraction and triage experiments:

- adapter: `scripts/local_llm_adapter.py`
- host: `http://10.0.0.11:1234`

Budget discipline:

- keep extraction prompts narrow and typed
- prefer two-pass extraction over dumping full papers into a large model
- preserve model/version/prompt metadata for generated outputs
- make runs idempotent so retries do not duplicate artifacts

## Non-Negotiable Constraints

- no direct mutation of `core`
- no uncited generated scientific claims
- no silent identity merges
- no hidden precedence over deterministic baseline facts
- no open-ended web search outside the allowlist policy

## Success Criteria

The agent framework is working when:

- enriched outputs are auditable and reproducible
- citations are first-class artifacts, not afterthoughts
- ambiguity queues become more reviewable instead of more opaque
- accepted overlays improve the user-facing answer without erasing the conservative baseline
