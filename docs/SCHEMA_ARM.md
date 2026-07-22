# Spacegate Arm Schema Contract

This document defines the immutable science evidence/support layer (`arm`).

Purpose:
- store deterministic, provenance-bound science support rows outside core hot
  paths
- preserve source-native evidence rows whose ontology should not be promoted
  directly into canonical core
- materialize multiplicity hierarchy as explicit graph edges
- normalize orbital solutions for UI reconstruction and animation readiness scoring
- supply the science-side inputs for `docs/SYSTEM_SIMULATION.md` without storing
  visualization-only assumptions

Out of scope:
- editable fiction/user overlays (`SCHEMA_RIM.md`, rim layer)
- generated prose/images/snapshots (`SCHEMA_DISC.md`, disc layer)
- canonical star/system/planet inventory (`SCHEMA_CORE.md`)

Layer rule:

- `arm` can contain source-native rows. That does not make those rows canonical
  inventory; it makes them auditable evidence for Spacegate's canonical model.
- `arm` can contain deterministic derivatives. Those rows must retain input
  lineage, method/version, and confidence.
- promoted canonical summaries may be copied or projected into `core`, but the
  source evidence and competing/alternate solutions should remain in `arm`.

Evidence Lake v2 staging rule:

- M8.3c-E2's release-scoped identity/scope graph is a pre-CORE/pre-ARM compiler
  artifact, not an ARM build. It references the current CORE only as a labeled
  stability reference.
- E4 materializes source-native scientific domain evidence from the typed lake;
  the current NASA checkpoint includes source-scoped identifier and lifecycle
  evidence but deliberately leaves object bindings unresolved. Mixed source
  rows create separate binding outcomes for star, host, planet/candidate,
  observation-target, signal, component-label, and product scopes. E6 then
  shadow-builds this schema from those contracts; no E2 graph row
  is promoted into ARM merely because it has an accepted identity binding.
- E4 compiler artifacts live at
  `$SPACEGATE_STATE_DIR/derived/evidence_lake_v2/scientific_evidence/<build_id>/`.
  Each contains `scientific_evidence.duckdb` and a manifest with input,
  contract, logical-table, and integrity hashes. This pre-ARM artifact owns the
  bounded domain tables, exact source-record lineage, field dispositions, and
  accepted/missing/excluded/ambiguous/quarantined/unresolved binding outcomes.
  A field declaration is not materialization: `declared_pending` fields keep an
  E4 build in progress until the destination table contains their evidence or a
  reviewed exclusion is recorded.
- The complete E4 input is an immutable release set, not a duplicate monolithic
  DuckDB file. Release set `a188a3adc6207d3a217d54a9` pins each accepted shard
  manifest/database/logical/scientific hash and provides a table-to-shard index.
  E5 opens only those databases read-only. This preserves one atomic scientific
  build identity while avoiding another 449.2 GB copy of already immutable
  domain tables.
- E5 selected-fact artifacts live at
  `$SPACEGATE_STATE_DIR/derived/evidence_lake_v2/selected_facts/<build_id>/`.
  `selected_facts.duckdb` preserves object/system keys, quantity group/key,
  raw and normalized values, interval semantics, exact E4 evidence and
  parameter-set lineage, authority decision, policy, normalization, and
  quality metadata. Compiler v7 also stores the exact accepted `binding_id` on
  every source-selected fact, replacing inferred multi-column binding lookup;
  derived facts keep null binding IDs and point to derivation/input fact IDs.
  `parameter_set_selection_decisions` records the selected
  coherent set, selected source-native quality score, runner-up authority, and
  runner-up quality score; `selected_fact_derivations` records
  input selected-fact IDs, algorithm/version, applicability, formula,
  assumptions, uncertainty method, confidence, and superseded paths. These are
  pre-ARM compiler artifacts until the E6 shadow build passes.
- E5 selected-relation artifacts live at
  `$SPACEGATE_STATE_DIR/derived/evidence_lake_v2/selected_relations/<build_id>/`.
  `relation_endpoint_bindings` gives the left and right evidence endpoints
  independent `accepted|missing|excluded|ambiguous` outcomes and withholds a
  target key unless exactly one compatible canonical object resolves.
  `relation_evidence_projection` retains the source claim, polarity,
  confidence statistic, citation, both endpoint outcomes, and projection
  disposition. A high-confidence evidence row is not a hierarchy edge,
  `orbit_edge`, or CORE containment assertion. Negative controls remain
  negative evidence. Deterministic ordered Parquet files are the reproduction
  interface, and an independent audit checks hashes, endpoint accounting,
  polarity, thresholds, and the prohibition on fabricated probabilities.
- E5 selected-component artifacts live at
  `$SPACEGATE_STATE_DIR/derived/evidence_lake_v2/selected_components/<build_id>/`.
  `msc_system_bindings` resolves punctuation-preserving WDS identifiers to the
  canonical reference system while retaining release-graph disagreement as a
  diagnostic. `msc_component_entities` gives source-defined component and
  subsystem labels release-scoped keys without asserting that they are CORE
  stars. `msc_relation_evidence_projection` retains both independently bound
  endpoints and explicitly rejects source self-relations; it is evidence, not
  hierarchy or containment. `msc_component_parameter_set_bindings` and the MSC
  parameter, classification, photometry, and astrometry projections expose only
  exact WDS-scoped targets; relative separation is context-only.
  `msc_orbit_solution_bindings` accounts both hierarchy-table periods and the
  separate orbit table, requiring explicit pair endpoints and one accepted
  relation before an orbit is eligible. DEBCat system, relation, and
  parameter-set binding tables require a unique best-priority exact name match
  and a unique WDS/period-compatible MSC relation before component facts become
  eligible.
  Integrated photometry and metallicity remain system scoped. The projection
  also binds SB9 magnitudes, classifications, and spectroscopic orbits only
  through a unique exact MSC `SB9_<sequence>` reference with accepted
  endpoints. `orb6_relation_bindings` separately requires one exact WDS
  identifier plus constructed discoverer/pair designation and one accepted MSC
  relation with the parsed WDS-qualified endpoints;
  `orb6_orbital_solution_projection` retains all rejected outcomes and makes
  only those exact bindings eligible. It is an E6 compiler input, not an
  independently served or canonical database.
- `evidence_object_bindings` contains exactly one outcome per eligible evidence
  subject. `binding_subject_kind` and `binding_subject_id` distinguish an
  unscoped source record, classification evidence row, or scoped parameter set;
  `source_record_id` remains the parent lineage. The row retains component and
  identifier-claim scope, applicability status/reason/evidence, and one of
  `accepted|missing|excluded|ambiguous|quarantined|unresolved`. `accepted`
  requires one compatible canonical object and stable key. Every other status
  retains no selected target and cannot emit facts. Per-source outcome totals
  must equal eligible-subject totals, and accepted totals must equal
  `selection_source_accounting`.
- `source_parameter_set_preselections` records source-internal model choice
  before cross-source authority ranking. It retains the selected coherent set
  and model, required-quantity completeness, uncertainty coverage, source-native
  ordering value, candidate count, runner-up set/model/value, applicability
  evidence, reason, and policy. It never deletes alternative E4 evidence.
- Compiler v6 names the corresponding source-accounting columns
  `eligible_binding_subjects` and `nonaccepted_binding_subjects`; earlier
  artifacts retain the legacy record-named columns and remain independently
  auditable.
- `config/evidence_lake/e5_source_dispositions.json` is the fail-closed boundary
  ledger for accepted E4 sources not yet named in a selected-fact policy. A
  source must be selected or have exactly one explicit evidence-only/deferred
  disposition with ownership, blocker state, and reason. The compiler hashes
  this ledger and emits its audit status and blocker list; a disposition does
  not authorize selection or canonical projection.
- Coherent-array sources may contain multiple source-native parameter-set kinds
  with different schemas. E5 policy v7/compiler v8 requires each heterogeneous
  quantity group to name its applicable `parameter_set_kinds`; eligibility,
  duplicate detection, authority decisions, and selected facts apply the same
  filter. Gaia DR3 variability summary rows and rotation-modulation solutions
  consequently remain separate coherent contexts even though they share one E4
  destination table. Source-native false membership flags are selected as
  categorical negative context with null numeric values, not discarded or
  converted into measurements.
- A selected source may declare `channel_dispositions` for scientifically
  distinct E4 destinations or model families. Every declaration is either
  `selected` or `evidence_only` and carries a reason; duplicate or reasonless
  channels fail policy validation. Evidence-only means the typed evidence stays
  inspectable and may participate in the future public evidence inspector, not
  that it is deleted or silently ignored. Gaia supplementary GSP-Phot libraries
  use this contract because the publisher-selected best-library values already
  enter through the main AP table.
- Bailer-Jones distance selection preserves `gaia_edr3_source_id` separately
  from canonical `gaia_dr3`. Binding is permitted only by the policy-pinned,
  authoritative EDR3-to-DR3 source-list relationship and records that release
  transition as its method. Geometric and photogeometric posterior medians are
  supplementary calibrated model estimates, not replacements for Gaia source
  parallax; their lower and upper values are exact 16th/84th-percentile
  endpoints rather than symmetric errors.
- Ranked EAV selection may declare bounded quality conditions over evidence,
  parameter-set, or source-record JSON plus one numeric ordering signal.
  Conditions determine eligibility; ordering is authority rank, coherent-set
  quantity completeness, uncertainty coverage, reference coverage, quality
  score, then stable parameter-set ID. Scores are source-native and therefore
  compare repeat observations only within an explicit authority tier; survey
  precedence is expressed by per-quantity authority ranks, not by pretending
  unlike instrument S/N values are calibrated to one scale.
- Large selected-fact exports are deterministically partitioned by
  `quantity_key`; decision exports are partitioned by `quantity_group`. The
  artifact gate requires every expected partition and verifies its Parquet row
  count before hashing or promotion. DuckDB remains the queryable compiler
  form, while the partitions are the stable reproduction and downstream
  projection interface.
- `config/evidence_lake/e5_legacy_derivation_inventory.json` is the audited
  transition contract for pre-E5 derived, inferred, and assumed values. It
  separates ARM science derivations from empirical classifications and
  DISC/render presentation priors, records their uncertainty limitations and
  supersession gates, and maps current materialized method values. New legacy
  methods fail `scripts/audit_legacy_derivation_inventory.py` until explicitly
  classified; registration does not authorize a method as an E5 public fact.
- `source_field_dispositions.source_field` is the legal typed-column name, while
  `source_native_field` retains the exact upstream spelling. They differ only
  when a source format requires an alias, including case-only VOTable collisions
  such as `b_rgeo` and `B_rgeo`; field accounting and evidence lineage preserve
  both names.
- `source_records.source_context_json` materializes only fields declared
  `context` or `lineage`. Fields declared `exclude` remain losslessly preserved
  in E1 typed storage and explicit field accounting but are not redundantly
  copied into E4 source context merely because their bookkeeping destination is
  `source_records`.
- `source_native_parameter_dispositions` is the exhaustive routing ledger for
  source-native parameter stores such as OEC. Every configured object-kind and
  parameter-name pair records its typed scientific destination or an explicit
  exclusion; aggregate row counts must equal the E1 parameter table exactly.
  This bounded EAV input does not become a universal ARM EAV schema.
- Coherent high-dimensional source solutions use a normalized two-table
  contract: `coherent_parameter_set_schemas` stores one ordered field schema,
  while the domain parameter-set table stores positional typed values and exact
  source-record lineage. The schema retains datatype, unit, UCD, description,
  and transformation semantics. Gaia masked vectors normalize to nullable
  numeric arrays, preserving a null whole vector separately from null masked
  elements. Artifact verification requires schema/domain agreement and exact
  value/schema arity.
- Gaia DR3 source rows use `stellar_source_parameter_sets`, separate from the
  narrower selected or model-specific `stellar_parameter_sets`. Each source
  row retains one coherent 125-value solution plus an ordered schema whose
  fields carry explicit scientific-domain annotations. This prevents a
  32-million-row scalar expansion and preserves the covariance context of the
  Gaia source solution; it does not select public facts or copy those rows into
  ARM. Build `ab7f7e6bc211bee146885987` remains an internal E4 artifact until
  E5 selection and E6 shadow review pass.
- Exoplanet lifecycle adapters remain independent source scopes. OEC source
  object keys combine archive member with local XML node path, and relation
  endpoints use the same composite identity. Its confirmed, candidate,
  controversial, and retracted states populate lifecycle evidence with
  positive, candidate, ambiguous, and negative polarity respectively. HWC
  populates habitability feature evidence only and cannot create a confirmation
  or canonical planet. Exoplanet.eu confirmations remain corroborating source
  assertions, not a replacement for NASA authority.
- JPL Horizons natural and artificial sources use the same E4 domain contract
  without merging their release scopes. Exact response bodies are represented
  by `observation_product_lineage`; parsed elements are coherent
  `orbital_solution_evidence` linked to `relation_claim_evidence`; published
  radius/mass values are coherent solar-system physical parameter sets. Source
  relations bind only parsed `jpl_horizons_target` commands. Reviewed operator
  seed keys/names retain distinct namespaces and cannot create containment or
  orbit-center relations. Build `236a7b7822c52fef8b903d58` is an internal E4
  checkpoint, not a promoted ARM or canonical Sol hierarchy.
- Complete-envelope SIMBAD checkpoint `fc5bd4e6398d72bde50ba6d5` validates the
  bounded citation and identity contracts at scale. Its 32-bucket astrometry-
  citation expansion preserves the same exhaustive evidence relation under a
  16-GB DuckDB limit; 285 component-suffixed HIP aliases remain normalization
  rejections rather than empty claims. Independent audit and clean logical-hash
  reproduction pass. E5 policy v10 may select a source spectral classification
  only after the basic record's SIMBAD OID traverses one same-release bridge to
  one current Gaia DR3 star. SIMBAD aliases, astrometry, object types, and
  bibliography remain evidence and cannot create canonical inventory.
- WGSN checkpoint `0ff30b04008b93aafb3de66f` keeps official naming evidence
  source scoped. Proper-name/search-spelling aliases, source catalog records,
  observation-target identifiers, and Bayer system-or-component ambiguity are
  distinct claim scopes. Shared values remain multiple evidence rows and cannot
  merge objects or create containment. Coordinates and magnitude are retained
  as naming context, while source placeholders are not promoted as identifiers
  or citations. E5 policy v10 selects the exact official name only when matched
  HIP/HD/HR/GJ identifiers converge and no second WGSN name reaches the same
  canonical star. Izar/Pulcherrima therefore remains an explicit two-record
  component-scope ambiguity rather than an arbitrary winner.
- GCVS checkpoint `a6f6669d2bd48eac5d6204d2` keeps variable-star identity,
  astrometry, variability, stellar classification, and bibliography evidence
  source scoped. Component-suffixed records do not claim base numeric identity;
  variable classes and spectral classes occupy separate typed domains; repeated
  source-key bibliography lines aggregate deterministically. All bindings stay
  unresolved until the E2/E5 identity and selection policies adjudicate them.
- VSX checkpoint `d9780b76333132c0a05098b7` applies the same boundary at the
  rolling-catalog scale. VSX OID is release-scoped identity; public names and
  embedded Gaia DR3 names are separate claims. Variability values remain one
  schema-backed 16-field source record per OID, spectral strings remain stellar
  classification evidence, and historical bibliography binds only through
  exact OID/reference pairs. Missing current OIDs and structurally invalid ADS
  strings stay unresolved/raw evidence. E5 policy v9 selects only source-native
  `variability_class_source_native` and `variability_period_days` for exact,
  unique Gaia DR3 bindings; it does not create ARM or CORE inventory. All other
  VSX channels retain their source-record lineage for E6 review.
- Hunt/Reffert checkpoint `7e66e0690aa962c837d43a86` keeps cluster physical
  contexts, probability-bearing membership claims, literature crossmatches,
  and both cluster/member endpoint identities source scoped. Cross-table
  selection follows the chosen cluster's published distance-posterior overlap;
  it does not turn membership into a relation claim or canonical containment.
  Exact source field names remain in lineage when typed query outputs require a
  legal alias.
- Extended-catalog checkpoint `54d1b0b6a841344c48327991` keeps OpenNGC and
  constituent nebula identities, geometry, distances, physical context, and
  source documents separate from canonical object inventory. Catalog component
  markers remain scoped, and raw list-valued aliases remain evidence until E2
  applies an explicit parser/reconciliation policy.
- `astrometry_distance_evidence_bundles` is a storage grouping, not a coherent
  physical parameter set or selected-fact record. It keeps multiple typed
  astrometry/distance/velocity measurements attached to one exact source record
  without multiplying very large source tables into one physical row per
  field. Every nested measurement retains its own stable evidence ID, quantity,
  raw/normalized value and unit, uncertainty, frame/epoch, method/model,
  reference, quality, and normalization version. Selection must unnest these
  measurements and may not infer compatibility merely because they share a
  storage bundle.
- NASA checkpoint `cb82c09179afa740b02e2cdf` is the first field-complete source
  adapter. It preserves source units/reference fragments, adds versioned unit
  aliases and parsed citation metadata, groups measurement companions into
  coherent parameter sets, and indexes observation products for on-demand
  retrieval. Its `pass` status applies to that adapter, not to unimplemented E4
  sources or ARM/public promotion.
- El-Badry checkpoint `aaf262b1791d98ce3e9f96e7` extends the same contract to
  bounded relation evidence. `relation_claim_evidence` distinguishes strict
  `probability` from raw/normalized confidence statistics and their semantics,
  carries release-scoped namespaces for both endpoints, and labels candidate
  versus negative-control polarity. Endpoint identifier claims and unresolved
  binding outcomes carry `left`/`right` component scope. The source's KDE ratio
  may exceed one, so it is never written to the strict probability column.
- Scientific-evidence contract v2 adds nullable `component_scope` to
  `stellar_parameter_sets`, `stellar_parameter_evidence`, and
  `stellar_classification_evidence`. Scoped evidence must agree with its
  parameter set and must have an explicit unresolved or resolved
  `stellar_component` binding outcome. A null scope is system/record scoped;
  it must not be copied onto a component without later accepted binding policy.
- `orbital_solution_evidence` preserves one coherent source solution and may
  retain an unresolved `relation_claim_id`. ORB6 combined pair designations are
  identity evidence, not instructions to manufacture endpoints. DEBCat primary
  and secondary measurements remain separate parameter sets, while metallicity
  and integrated photometry remain system scoped.
- Scientific-evidence contract v3 adds `parameter_set_raw` to
  `extended_object_evidence`; geometry, distance, and physical/source
  parameters no longer share one overloaded JSON object. Composite source
  identities are deterministic evidence claims, not canonical object creation.
  Configured scalar measurements preserve raw values even when no numeric
  normalization is valid, as with TESS EB status/sectors/flags and Green SNR
  uncertainty-marked fluxes.
- `compact_object_evidence` contains one provenance-bound source-native
  parameter context; alternatives that are genuine stellar model solutions
  remain separate `stellar_parameter_sets`. The EDR3 white-dwarf adapter uses
  this division for candidate probability/quality versus H, He, and mixed
  atmosphere fits. E5 policy v6 requires `Pwd > 0.75`, then selects one complete
  H, He, or mixed Teff/log-g/mass set by minimum published fit chi-square. The
  selected mass and atmosphere values come from that same model; alternatives
  remain evidence and are never flattened field by field.
- Source citation catalogs are materialized before evidence tables that refer
  to them. A compact-object row may populate `reference_raw` only when its
  source-native token exactly matches an authoritative `source_reference_key`;
  unmatched tokens remain in `parameter_set_raw` and do not receive synthetic
  citations. Predicate-scoped identifier claims similarly expose PSRJ/PSRB
  aliases only from the ATNF parameter occurrences that assert those names.
- A single source record may emit multiple `compact_object_evidence` rows when
  it contains genuinely distinct coherent parameter contexts. Each
  `compact_kind` has a collision-free evidence identity, its own applicability
  predicate, method, reference, parameter JSON, and quality JSON. McGill
  timing, X-ray, distance, position, and association/activity records use this
  contract rather than one incompatible field-wise composite.
- Compact evidence selection additionally requires a distinct compatible
  canonical compact-object leaf. Exact designation/OID/Gaia traversal alone is
  insufficient when the target carries ordinary stellar evidence consistent
  with an optical companion. The E5 compact audit therefore quarantines ATNF
  J0437-4715 instead of copying pulsar facts onto its current K-spectrum Gaia
  leaf; ATNF and McGill contexts remain evidence until E6 repairs identity.
- `relation_claim_evidence` stores `left_component_scope` and
  `right_component_scope` explicitly. Audits resolve each endpoint through an
  identifier claim and its matching binding scope; they do not assume all
  sources use generic `left`/`right` labels. SB9 therefore retains source-native
  `primary`/`secondary`, while the El-Badry wide-pair adapter retains
  `left`/`right`.
- Configured measurement evidence records `uncertainty_field`,
  `uncertainty_lower_field`, and `uncertainty_upper_field` in `quality_json`.
  The names accompany the normalized uncertainty magnitudes and exact source
  record, so asymmetric bounds remain auditable without reconstructing a
  compiler contract from column naming conventions.
- `orbital_solution_evidence.relation_claim_id` may be resolved across source
  tables by an exact configured logical-key mapping. Required mappings fail on
  zero or multiple matches. This links SB9 orbit rows to system relation claims
  by `Seq` without name parsing or array-position identity.
- Gaia NSS rows do not assert inspectable component endpoints, so their
  `orbital_solution_evidence.relation_claim_id` remains null. The source and
  solution identifiers, dynamic NSS model, complete fitted parameter/error
  set, correlation vector, fit diagnostics, frame, and reference stay together
  in one coherent solution row. Later adjudication may bind a solution to a
  reviewed relation; E4 does not manufacture companions or containment.
- SBX uses the same general contracts with source-native `primary`/`secondary`
  component endpoints and separate `child_subsystem`/`parent_subsystem`
  hierarchy endpoints. Only configuration rows with an asserted parent emit a
  hierarchy claim; inverse child slots and family flags remain exact source-row
  context and do not become duplicate canonical edges.
- SBX orbit rows link by exact source `sn` to one binary relation, while
  multiple `on` solutions remain distinct. Component classifications and
  magnitudes retain primary/secondary scope. Catalog aliases remain
  source/release-scoped observation-target or system claims; co-occurrence does
  not promote a source relation or alias into canonical identity/containment.
- E5 system anchoring accepts SBX only when exact Gaia DR3, officially
  reconciled Gaia DR2, HIP, HD, or TIC evidence converges on one canonical
  system. The resulting `source-component:multiplicity.sbx:<release>:SBX_<sn>:`
  keys are release-scoped evidence targets, never permanent object IDs.
  Astrometry targets the accepted canonical system as context only because the
  source observation may describe an unresolved target or photocenter; it is
  not copied to `primary` or `secondary`.

## Artifact Contract

Per build:
- `$SPACEGATE_STATE_DIR/out/<build_id>/arm.duckdb` (target-state artifact)

Hard rules:
- arm artifacts are immutable by `build_id`
- rows are deterministic for pinned inputs/transforms
- every row is provenance-complete

## Key Model

Arm uses graph primitives:
- containment graph: `system_hierarchy_edges`
- dynamic graph: `orbit_edges` + `orbital_solutions`
- source-native stellar payload: `stellar_parameters`

Every edge/derived record includes:
- `build_id`
- `confidence_score`
- `confidence_tier` (`high|medium|low|illustrative`)
- source lineage and transform lineage

## Tables

## `component_entities`

Canonical component registry for hierarchy assembly.

Columns:
- `component_entity_id BIGINT`
- `stable_component_key TEXT` (deterministic, cross-build stable where possible)
- `component_type TEXT` (`system|star|planet|subplanet|moon|minor_body|artificial|region|brown_dwarf|compact|cluster_member|unresolved_component`)
- `core_object_type TEXT` (`system|star|planet|NULL`)
- `core_object_id BIGINT` (nullable; links to core if present)
- `display_name TEXT`
- `catalog_component_label TEXT` (e.g., `Aa`, `Bb1`)
- `ra_deg DOUBLE` (nullable for unresolved)
- `dec_deg DOUBLE` (nullable for unresolved)
- `dist_pc DOUBLE` (nullable)
- `source_catalog TEXT`
- `source_version TEXT`
- `source_pk TEXT`
- `source_row_hash TEXT`
- `retrieval_checksum TEXT`
- `retrieved_at TIMESTAMP`
- `ingested_at TIMESTAMP`
- `transform_version TEXT`

## `system_hierarchy_edges`

Explicit containment and subsystem structure.

Columns:
- `hierarchy_edge_id BIGINT`
- `parent_component_key TEXT`
- `child_component_key TEXT`
- `edge_kind TEXT` (`contains|subsystem_of|member_of_pair`)
- `member_role TEXT` (e.g., `primary`, `secondary`, `component`, nullable)
- `catalog_relation_label TEXT` (nullable)
- `depth_hint INTEGER` (nullable)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `evidence_catalogs_json TEXT`
- `evidence_ids_json TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Constraints:
- no self-edge
- no duplicate `(parent_component_key, child_component_key, edge_kind, source_pk)`
- containment governance:
  - `edge_kind='contains'` must remain acyclic
  - each child may have one canonical containment parent for navigation; additional links must use non-containment edge types

Source-object boundary:
- `core.source_object_reconciliation` may remove duplicate source surrogates
  before root-system grouping, but ARM remains responsible for relationship
  evidence, hierarchy edges, orbital edges, alternate solutions, and diagnostics
  over the surviving accepted source objects.
- Unmatched or quarantined source-object reconciliation candidates must not be
  promoted into ARM hierarchy membership unless later accepted by a reviewed
  source/override path.
- Alias authority may resolve a public search term to an accepted system/member
  focus, but aliases are not relationship evidence. ARM membership and orbit
  edges must continue to come from source-native hierarchy/orbit evidence,
  accepted reconciliation outputs, or reviewed overrides, never name similarity
  alone.

## `orbit_edges`

Dynamic relationships used for orbital reconstruction.

Columns:
- `orbit_edge_id BIGINT`
- `host_component_key TEXT` (often subsystem or barycenter-hosting group)
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `relation_kind TEXT` (`binary|circumbinary|hierarchical_pair|bound_companion|planetary_orbit|satellite|orbits|artificial_orbit|co_orbit`)
- `barycenter_key TEXT` (nullable)
- `preferred_solution_id BIGINT` (nullable FK to `orbital_solutions`)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `evidence_catalogs_json TEXT`
- `evidence_ids_json TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Notes:
- Gaia NSS unresolved binaries may emit synthetic companion component keys so source-native orbital evidence can be narrated without fabricating canonical core stars.
- Planet rows emit `planetary_orbit` edges from the host system/star component
  to the planet component when a core planet has a resolved system binding.
- `hierarchical_pair` edges may connect subsystem/group component keys rather
  than physical leaf stars. Renderers must preserve that distinction, for
  example by drawing cluster orbit guides instead of reclassifying a group edge
  as a direct binary star orbit.
- Source-native wide/group endpoints may be reconciled to rendered barycenter
  leaf sets when `system_hierarchy_edges` makes the descendant set
  deterministic. This reconciliation is a render/API bridge over ARM
  hierarchy evidence, not a source-object merge or a suffix-only inference.
- MSC `sys.tsv` and `orb.tsv` rows may both create deterministic `binary` or
  `hierarchical_pair` edges when the host and both endpoint component keys
  resolve. Every `orb.tsv` row is accounted for in
  `msc_orbit_reconciliation`; excluded or quarantined rows must not create fake
  simulator endpoints.
- WDS pair observations are preserved separately in
  `wds_component_observations` and reconciled in `wds_pair_evidence`. Accepted
  bindings identify two source-scoped endpoints and remain sky-projection
  measurements. They do not assert gravitational binding and never become
  fitted orbital solutions without an independent orbit source.
- Runtime render contracts may expose subsystem nodes as inspectable UI bodies
  with descendant render keys and derived child counts. Those presentation
  handles must remain backed by `component_entities`/`system_hierarchy_edges`
  and must not be treated as additional physical stars or orbital solutions.

## `orbital_solutions`

Catalog-normalized orbital element records.

Columns:
- `orbital_solution_id BIGINT`
- `orbit_edge_id BIGINT`
- `solution_source_catalog TEXT` (`nasa_exoplanet_archive|sol_authority|gaia_nss|orb6|msc|wds|...`)
- `solution_rank INTEGER` (1 = best/preferred within source)
- `reference_epoch_jyear DOUBLE` (or `reference_epoch_mjd DOUBLE`)
- `period_days DOUBLE` (nullable)
- `semi_major_axis_au DOUBLE` (nullable)
- `semi_major_axis_arcsec DOUBLE` (nullable)
- `eccentricity DOUBLE` (nullable)
- `inclination_deg DOUBLE` (nullable)
- `longitude_ascending_node_deg DOUBLE` (nullable)
- `argument_periastron_deg DOUBLE` (nullable)
- `time_periastron_jd DOUBLE` (nullable)
- `mean_anomaly_deg DOUBLE` (nullable)
- `mass_ratio_q DOUBLE` (nullable)
- `primary_mass_msun DOUBLE` (nullable)
- `secondary_mass_msun DOUBLE` (nullable)
- `rv_semiamplitude_primary_kms DOUBLE` (nullable)
- `rv_semiamplitude_secondary_kms DOUBLE` (nullable)
- `fit_quality_json TEXT` (chi2/residual summaries, nullable)
- `normalization_method TEXT`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Rule:
- never overwrite source-native measurements; normalized columns are additive transforms with lineage.
- planet, moon, binary, and artificial-object orbital solutions all follow the
  same evidence-preserving table contract. MSC `orb.tsv` rows are normalized as
  `solution_source_catalog='msc'` with period, angular semi-major axis,
  eccentricity, inclination, node, periastron longitude, velocity amplitudes,
  notes/reference strings, and ranking where available.
  same policy: a promoted scalar may appear in `core` for hot-path display, but
  source-native solution rows, alternates, epochs, fit-quality fields, and
  simulation-ready element sets belong here.
- NASA Exoplanet Archive `pscomppars` values remain the rank-1 promoted
  default for confirmed exoplanet orbital rows. NASA `ps` literature rows are
  materialized as rank-2+ alternate `source_native_planet_orbit` candidates on
  the same planet orbit edge when the `ps` cooked artifact is present.
- Sol authority planet orbital values are materialized as
  `normalization_method='source_native_planet_orbit'`.
- ORB6 solutions may be attached only when the source row can be mapped safely to a unique binary edge for a WDS-linked system; otherwise keep the source-native row outside generic orbital reconstruction flows.
- illustrative orbit defaults for rendering belong in `disc` assumptions until
  they are backed by reviewed source or derived `arm` rows.
- Projected-separation support values computed from angular separation and
  distance are derived presentation support fields. They may help
  `/simulation-scene` choose visual scale or low-confidence Kepler
  presentation periods, but they are not fitted `semi_major_axis_au`
  measurements and must not replace source orbital solutions.

## `stellar_parameters`

Narration-oriented, source-native stellar-parameter rows keyed to core stars.

Columns:
- `stellar_parameter_id BIGINT`
- `star_id BIGINT`
- `system_id BIGINT`
- `stable_object_key TEXT`
- `parameter_source TEXT` (`gaia_dr3_backbone|nasa_pscomppars_host|...`)
- `teff_k DOUBLE` with `teff_lo_k DOUBLE`, `teff_hi_k DOUBLE`
- `logg_cgs DOUBLE` with `logg_lo_cgs DOUBLE`, `logg_hi_cgs DOUBLE`
- `metallicity_feh DOUBLE` with `metallicity_lo_feh DOUBLE`, `metallicity_hi_feh DOUBLE`
- `distance_pc DOUBLE` with `distance_lo_pc DOUBLE`, `distance_hi_pc DOUBLE`
- `radius_rsun DOUBLE` with `radius_err_plus_rsun DOUBLE`, `radius_err_minus_rsun DOUBLE`
- `mass_msun DOUBLE` with `mass_err_plus_msun DOUBLE`, `mass_err_minus_msun DOUBLE`
- `luminosity_log10_lsun DOUBLE` with `luminosity_err_plus_log10_lsun DOUBLE`, `luminosity_err_minus_log10_lsun DOUBLE`
- `density_g_cm3 DOUBLE` with `density_err_plus_g_cm3 DOUBLE`, `density_err_minus_g_cm3 DOUBLE`
- `age_gyr DOUBLE` with `age_err_plus_gyr DOUBLE`, `age_err_minus_gyr DOUBLE`
- `rotation_period_days DOUBLE`
- `radial_velocity_kms DOUBLE` with `radial_velocity_error_kms DOUBLE`
- Gaia photometry/color: `phot_g_mag`, `phot_bp_mag`, `phot_rp_mag`, `bp_rp`, `bp_g`, `g_rp`
- Gaia quality/fit context: `ra_error_mas`, `dec_error_mas`, `pm_ra_error_mas_yr`, `pm_dec_error_mas_yr`, `visibility_periods_used`, `astrometric_params_solved`, `non_single_star`, `duplicated_source`, `has_xp_continuous`, `has_xp_sampled`, `has_rvs`
- `spectral_type_raw TEXT`
- classifier support: `classprob_star`, `classprob_binarystar`, `classprob_galaxy`, `classprob_quasar`, `classprob_whitedwarf_combmod`, `classprob_whitedwarf_specmod`
- `context_json TEXT` (source-specific auxiliary context such as WD specialist payload or NASA host multiplicity counts)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Rules:
- keep source-native values and uncertainty bounds; do not collapse them into inferred prose fields here
- one star may legitimately have multiple rows from different source catalogs

## `derived_physical_parameters`

Deterministic, provenance-bound numeric science candidates used when source
catalogs do not provide a needed physical value. These rows are temporary in the
scientific sense: they are acceptable explicit derivations, but they should be
replaced or superseded when a stronger source measurement is found.

Implementation status: emitted by `scripts/build_arm.py` as
`derived_physical_parameters_v1` for deterministic source-input candidates.
Global stellar spectral-subclass mass priors are intentionally not bulk
materialized here yet because applying them to every spectral-class star would
add many millions of low-authority support rows. The simulator/API may expose
bounded runtime priors with provenance when useful, while the audit report keeps
the source-mass coverage gap visible for a later capped/materialized policy.

Columns:
- `derived_parameter_id BIGINT`
- `build_id TEXT`
- object binding:
  - `object_type TEXT` (`system|star|planet|component|orbit`)
  - `system_id BIGINT` (nullable)
  - `star_id BIGINT` (nullable)
  - `planet_id BIGINT` (nullable)
  - `stable_object_key TEXT` (nullable)
  - `stable_component_key TEXT` (nullable)
- parameter:
  - `parameter_key TEXT` (for example `teff_k`, `luminosity_lsun`,
    `mass_msun`, `semi_major_axis_au`, `insol_earth`)
  - `value DOUBLE`
  - `unit TEXT`
  - uncertainty/range: `value_lo DOUBLE`, `value_hi DOUBLE`, `sigma DOUBLE`
- derivation:
  - `derivation_method TEXT` (for example `spectral_type_proxy`,
    `stefan_boltzmann_from_radius_teff`, `kepler_from_period_host_mass`)
  - `derivation_version TEXT`
  - `input_parameters_json TEXT`
  - `assumptions_json TEXT`
  - `lossy_transform BOOLEAN`
  - `superseded_by_source BOOLEAN`
  - `replacement_priority TEXT` (`low|normal|high|critical`)
- quality:
  - `confidence_score DOUBLE`
  - `confidence_tier TEXT` (`high|medium|low|illustrative`)
  - `review_status TEXT` (`candidate|accepted|superseded|rejected`)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`,
  `transform_version`)

Runtime note:

- `/simulation-scene` may expose ARM-scoped derived support fields even before
  they are bulk-materialized in this table. For example,
  `stellar_luminosity_from_radius_teff_v1` derives `luminosity_lsun` from
  available radius and effective temperature for hierarchy-rendered stars so
  HZ and temperature-line overlays have auditable inputs. These values are
  render/API support facts, not core source facts, and remain supersedable by a
  reviewed source luminosity row.

Rules:
- source-native measurements always win over derived values for science claims.
- spectral-type proxy rows are allowed only when the source spectral evidence is
  preserved and the confidence tier is no higher than `low`.
- v1 intentionally does not persist spectral-type proxy rows; those remain
  runtime diagnostics until the stricter source-input path is validated.
- Mass-only visual stellar classes such as the simulator
  `mass_main_sequence_prior_v1` are presentation/render priors, not ARM
  classifications. They may guide color/material choices and hierarchy UI
  labels when clearly marked as ASSUMED, but they must not be written into ARM
  spectral-class or derived-physical-parameter tables without a separate
  reviewed science derivation policy.
- derived orbital values such as semi-major axis from period and host mass must
  retain the exact input mass/period basis and method version.
- Astronomy Agency enrichment must treat non-superseded rows in this table as a
  prioritized search target for stronger literature/source values.
- rows with `confidence_tier='illustrative'` may support diagnostics but must
  not be used as canonical science assertions.

## `derived_stellar_classifications`

Deterministic, provenance-bound categorical classification candidates for
display, search, filters, and renderer policy when source spectral class is
missing. These rows do not replace `core.stars.spectral_class` or source
spectral type fields.

Implementation status: emitted by `scripts/build_arm.py` as
`derived_stellar_classification_v1` for core stars and source-native MSC
stellar component endpoints.

Columns:
- `derived_classification_id BIGINT`
- `build_id TEXT`
- object binding:
  - `object_type TEXT` (`star|component`)
  - `system_id BIGINT` (nullable)
  - `star_id BIGINT` (nullable)
  - `stable_object_key TEXT` (nullable)
  - `stable_component_key TEXT` (nullable)
- classification:
  - `classification_key TEXT` (`stellar_display_class` in v1)
  - `classification_value TEXT` (`O|B|A|F|G|K|M|L|T|Y|WR|WD|NS|PULSAR|MAGNETAR|BLACK HOLE`)
  - `classification_status TEXT` (`derived|assumed|missing`)
- derivation:
  - `derivation_method TEXT` (`remnant_guard_v1`,
    `teff_visual_class_prior_v1`, `mass_radius_physical_class_prior_v1`,
    `spectral_type_visual_class_prior_v1`,
    `mass_main_sequence_prior_v1`)
  - `derivation_version TEXT`
  - `input_parameters_json TEXT`
  - `assumptions_json TEXT`
  - `lossy_transform BOOLEAN`
  - `superseded_by_source BOOLEAN`
- quality:
  - `confidence_score DOUBLE`
  - `confidence_tier TEXT` (`medium|low|illustrative|missing`)
  - `review_status TEXT` (`candidate|accepted|superseded|rejected`)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`,
  `transform_version`)

Rules:
- Source spectral type/class remains authoritative for catalog facts and must
  supersede derived display classifications.
- Compact/remnant evidence overrides temperature buckets. A hot white dwarf,
  neutron star, pulsar, magnetar, or black hole must never be displayed as an
  ordinary O/B/A/F/G/K/M star merely because of temperature.
- Mass-only main-sequence rows are `classification_status='assumed'` and
  `confidence_tier='illustrative'`; they are useful for visual/search
  ergonomics, not science claims.
- MSC component classifications are emitted only for component keys that exist
  as source-supported `component_entities` star rows. Unmatched MSC detail
  endpoints remain diagnostics/evidence and must not become rendered or
  classified stars.

## `stellar_leaf_display_classifications`

Deterministic display projection with exactly one row for every eligible
stellar leaf in `canonical_hierarchy.duckdb`. This is the shared membership and
classification contract for map labels, simulation bodies, OBJECTS lists,
system-page heroes, and hierarchy leaves. It does not alter CORE spectral facts
or promote an assumed class into canonical astronomy.

Implementation status: emitted after ARM and canonical hierarchy construction
by `scripts/materialize_stellar_leaf_classifications.py` as
`stellar_leaf_display_classification_v1`.

Key columns:
- identity: `system_id`, `system_stable_object_key`, `hierarchy_node_key`,
  `leaf_component_key`, nullable `evidence_component_key`, `star_id`, and
  `stable_object_key`
- display context: `display_name`, `catalog_component_label`, `node_kind`, and
  `hierarchy_source_basis`
- result: `classification_value`, `classification_status`, `evidence_basis`,
  `confidence_score`, and `projection_version`
- conflict visibility: `distinct_candidate_class_count`,
  `candidate_classes_json`, and `has_classification_conflict`
- source lineage: `source_catalog`, `source_version`, `source_pk`,
  `retrieval_checksum`, `retrieved_at`, and `source_value`

Evidence precedence is exact CORE leaf source class, accepted exact-component
source class, exact MSC endpoint spectral derivation, other derived
exact-component class, then an explicitly assumed exact-component mass prior.
Missing leaves remain `UNKNOWN`. Aggregate/barycenter nodes and inferred
nonstellar endpoints are excluded. A source conflict is preserved on the row
even when deterministic precedence selects one public display value.

Every eligible hierarchy leaf must appear exactly once. Clients must not add a
representative class, deduplicate repeated classes, infer membership from a
display name, or substitute an aggregate-node class. `system.star_count`
differences are reported separately because audited hierarchy leaves, not a
legacy count facet, define badge membership.

Renderers may resolve a row through `hierarchy_node_key`, `leaf_component_key`,
or `evidence_component_key`, but ambiguous aliases must fail closed. When this
projection is available, raw ARM component endpoints absent from it are
evidence, not additional rendered stellar members.

## `msc_component_details`

MSC component/context rows for subsystem narration and photometry support.

Columns:
- `msc_component_detail_id BIGINT`
- `system_id BIGINT` (nullable)
- `star_id BIGINT` (nullable)
- `stable_object_key TEXT` (nullable system key)
- `stable_component_key TEXT`
- `wds_id TEXT`
- `component_label TEXT`
- `preferred_name TEXT` (nullable)
- `sep_arcsec DOUBLE` (nullable)
- `spectral_type_raw TEXT` (nullable)
- astrometric/kinematic context: `parallax_mas`, `pm_ra_mas_yr`, `pm_dec_mas_yr`, `radial_velocity_kms`
- photometry: `bmag`, `vmag`, `imag`, `jmag`, `hmag`, `kmag`
- `grade TEXT` (nullable)
- `other_identifiers TEXT` (nullable)
- `subsystem_count BIGINT` (nullable)
- `orbit_count BIGINT` (nullable)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `msc_system_details`

MSC `sys.tsv` subsystem rows for source-native hierarchy and endpoint physical
evidence.

Columns:
- `msc_system_detail_id BIGINT`
- `wds_id TEXT`
- `primary_label TEXT`
- `secondary_label TEXT`
- `parent_label TEXT`
- `parent_component_key TEXT`
- `primary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `secondary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `system_type TEXT`
- `period_value DOUBLE`
- `period_unit TEXT`
- `period_days DOUBLE` (nullable normalized helper)
- `separation_value DOUBLE`
- `separation_unit TEXT`
- `separation_arcsec DOUBLE`
- `separation_mas DOUBLE`
- `position_angle_deg DOUBLE`
- endpoint physical hints: `vmag_primary`, `spectral_type_primary`,
  `vmag_secondary`, `spectral_type_secondary`, `mass_primary_msun`,
  `mass_code_primary`, `mass_secondary_msun`, `mass_code_secondary`
- `comment TEXT`
- `source_line_number BIGINT`
- `raw_row TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

Rules:
- source labels are case-sensitive before normalization: `AB` is a subsystem
  label while `Ab` is a leaf label.
- `sys.tsv` endpoint spectral/mass hints attach to the endpoint component or
  subsystem they describe; builders and renderers must not blindly inherit a
  parent spectral class onto all descendants.

## `msc_orbit_details`

MSC `orb.tsv` orbital-element rows for source-native orbit solution
materialization.

Columns:
- `msc_orbit_detail_id BIGINT`
- `wds_id TEXT`
- `system_label TEXT`
- `primary_label TEXT`
- `secondary_label TEXT`
- `host_component_key TEXT`
- `primary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `secondary_component_key TEXT` (nullable when endpoint policy does not support materialization)
- `period_value DOUBLE`
- `period_unit TEXT`
- `period_days DOUBLE`
- `periastron_epoch DOUBLE`
- `eccentricity DOUBLE`
- `semi_major_axis_arcsec DOUBLE`
- `node_deg DOUBLE`
- `longitude_periastron_deg DOUBLE`
- `inclination_deg DOUBLE`
- `semi_amplitude_primary_kms DOUBLE`
- `semi_amplitude_secondary_kms DOUBLE`
- `center_of_mass_velocity_kms DOUBLE`
- `node_flag TEXT`
- `note TEXT`
- `source_line_number BIGINT`
- `raw_row TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `wds_component_observations`

WDS summary observations and pair-history context for narrated binaries/multiples.

Columns:
- `wds_component_observation_id BIGINT`
- `system_id BIGINT` (nullable)
- `star_id BIGINT` (nullable)
- `stable_object_key TEXT` (nullable system key)
- `stable_component_key TEXT`
- `wds_id TEXT`
- `discoverer TEXT` (nullable)
- `source_component_label TEXT` (case- and punctuation-preserving WDS value)
- `component_label TEXT`
- `pair_primary_label TEXT`, `pair_secondary_label TEXT` (nullable)
- `pair_parse_status TEXT`
- observation window: `first_year`, `last_year`, `obs_count`
- position history: `theta_first_deg`, `theta_last_deg`, `rho_first_arcsec`, `rho_last_arcsec`
- `mag_primary DOUBLE`, `mag_secondary DOUBLE` (nullable)
- `spectral_type_raw TEXT` (nullable)
- motion context: `pm_primary_ra`, `pm_primary_dec`, `pm_secondary_ra`, `pm_secondary_dec`
- `dm_designation TEXT` (nullable)
- `note TEXT` (nullable)
- `precise_coordinate TEXT` (nullable)
- `ra_deg DOUBLE`, `dec_deg DOUBLE` (nullable)
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `wds_pair_evidence`

Deterministic endpoint accounting for WDS pair-observation rows.

- source observation/system/WDS identifiers and the source-native pair label
- parsed primary and secondary labels plus parse status
- resolved primary/secondary component keys and candidate counts
- `match_status TEXT` (`accepted|missing_endpoint|ambiguous|excluded`)
- `match_reason TEXT`
- `evidence_role TEXT` (`sky_projection_measurement` in v1)
- `asserts_bound_relationship BOOLEAN` (false in v1)
- `simulation_ready_orbit BOOLEAN` (false in v1)
- full source and transform provenance

An accepted row means only that both WDS endpoints bind uniquely at their
source-declared component/subsystem scope. Separation and position angle remain
projected observations, not Kepler elements or proof that the pair is bound.

## `msc_orbit_reconciliation`

One deterministic outcome per preserved MSC `orb.tsv` row.

- source orbit/WDS identity and endpoint keys
- unique `msc_orbit_detail_id` plus the source-native orbit PK (which is not
  assumed unique across preserved source rows)
- `reconciliation_status TEXT` (`accepted|excluded|quarantined`)
- `reconciliation_reason TEXT`
- canonical-system and MSC-system-relationship presence flags
- full source and transform provenance

Rows outside canonical inventory with no corresponding MSC system relationship
are explicitly excluded. Any in-scope row that still lacks a normalized edge
is quarantined and must fail the source-evidence gate.

## `barycenters`

Derived barycenter metadata used by animation and hierarchy layout.

Columns:
- `barycenter_id BIGINT`
- `barycenter_key TEXT`
- `host_component_key TEXT`
- `x_helio_pc DOUBLE` (nullable)
- `y_helio_pc DOUBLE` (nullable)
- `z_helio_pc DOUBLE` (nullable)
- `vx_helio_kms DOUBLE` (nullable)
- `vy_helio_kms DOUBLE` (nullable)
- `vz_helio_kms DOUBLE` (nullable)
- `mass_basis TEXT` (`measured|catalog_ratio|estimated`)
- `mass_estimation_method TEXT` (nullable)
- `mass_input_json TEXT` (nullable)
- `reference_epoch_jyear DOUBLE` (nullable)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `animation_readiness`

Per-system and per-edge readiness assessment.

Columns:
- `animation_readiness_id BIGINT`
- `stable_object_key TEXT` (system-level key)
- `component_key TEXT` (nullable)
- `orbit_edge_id BIGINT` (nullable)
- `readiness_level TEXT` (`full|partial|illustrative|insufficient`)
- `missing_parameters_json TEXT`
- `inferred_parameters_json TEXT`
- `disallowed_fabrication BOOLEAN`
- `notes_json TEXT`
- `computed_at TIMESTAMP`
- `transform_version TEXT`

## `system_neighbors`

Deterministic neighborhood helper for map traversal.

Columns:
- `neighbor_id BIGINT`
- `source_system_key TEXT`
- `neighbor_system_key TEXT`
- `distance_ly DOUBLE`
- `rank INTEGER`
- `method TEXT`
- `confidence_score DOUBLE`
- `computed_at TIMESTAMP`
- `transform_version TEXT`

## `sol_small_body_objects`

Named Sol-system S3 small-body rows (science layer, default kept out of core hot path).
Rows are source-native Sol authority evidence from JPL Horizons; asteroid/TNO
records must be fetched with Horizons small-body selector commands so ARM does
not materialize ambiguous major-planet or satellite solutions under
small-body component keys.

Columns:
- `sol_small_body_id BIGINT`
- `stable_component_key TEXT`
- `body_name TEXT`
- `body_name_norm TEXT`
- `body_kind TEXT` (`asteroid|tno|comet|unknown`)
- `host_component_key TEXT`
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `parent_name TEXT`
- `parent_name_norm TEXT`
- `orbital_period_days DOUBLE` (nullable)
- `semi_major_axis_au DOUBLE` (nullable)
- `eccentricity DOUBLE` (nullable)
- `inclination_deg DOUBLE` (nullable)
- `epoch_tdb_jd DOUBLE` (nullable)
- `body_mass_kg DOUBLE` (nullable)
- `body_radius_km DOUBLE` (nullable)
- `freshness_window_days INTEGER`
- `staleness_days INTEGER`
- `is_stale BOOLEAN`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`, `source_url`)

## `sol_artificial_objects`

Named Sol-system S4 artificial rows (stations/probes/orbiters; science overlay kept out of core hot path).

Columns:
- `sol_artificial_id BIGINT`
- `stable_component_key TEXT`
- `artifact_name TEXT`
- `artifact_name_norm TEXT`
- `artifact_kind TEXT` (`station|space_telescope|deep_space_probe|planetary_orbiter|artificial`)
- `host_component_key TEXT`
- `primary_component_key TEXT`
- `secondary_component_key TEXT`
- `parent_name TEXT`
- `parent_name_norm TEXT`
- `center_code TEXT`
- `target_body_name TEXT` (raw Horizons target body label)
- `orbital_period_days DOUBLE` (nullable)
- `semi_major_axis_au DOUBLE` (nullable)
- `eccentricity DOUBLE` (nullable)
- `inclination_deg DOUBLE` (nullable)
- `epoch_tdb_jd DOUBLE` (nullable)
- `artifact_mass_kg DOUBLE` (nullable)
- `artifact_radius_km DOUBLE` (nullable)
- `freshness_window_days INTEGER`
- `staleness_days INTEGER`
- `is_stale BOOLEAN`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`, `source_url`)

## `vsx_variability`

Per-observation variability overlay rows linked to `core.stars` via exact Gaia source ID.

Columns:
- `vsx_variability_id BIGINT`
- `stable_object_key TEXT`
- `star_id BIGINT`
- `gaia_id BIGINT`
- `vsx_oid BIGINT`
- `vsx_name TEXT`
- `variability_flag INTEGER`
- `variability_flag_label TEXT` (`variable|suspected|constant_or_nonexisting|possible_duplicate|unknown`)
- `variability_type_raw TEXT`
- `variability_family TEXT` (`eclipsing|pulsating|rotational|eruptive|extragalactic_or_lensing|other|unknown`)
- `max_mag DOUBLE`
- `max_passband TEXT`
- `min_is_amplitude_flag TEXT`
- `min_mag_or_amplitude DOUBLE`
- `min_passband TEXT`
- `amplitude_mag DOUBLE` (derived from VSX min/max semantics)
- `epoch_hjd DOUBLE`
- `period_days DOUBLE`
- `spectral_type TEXT`
- `confidence_score DOUBLE`
- `confidence_tier TEXT` (`high|medium|low|illustrative`)
- `is_default_usable BOOLEAN`
- `is_high_variability BOOLEAN`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `variability_summary`

One canonical variability row per `stable_object_key`, ranked from `vsx_variability`.

Columns:
- `variability_summary_id BIGINT`
- `stable_object_key TEXT`
- `star_id BIGINT`
- `gaia_id BIGINT`
- `vsx_match_count BIGINT`
- `primary_variability_flag INTEGER`
- `primary_variability_flag_label TEXT`
- `primary_variability_type_raw TEXT`
- `primary_variability_family TEXT`
- `primary_amplitude_mag DOUBLE`
- `primary_period_days DOUBLE`
- `primary_epoch_hjd DOUBLE`
- `primary_is_default_usable BOOLEAN`
- `any_high_variability BOOLEAN`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `ultracoolsheet_objects`

UltracoolSheet overlay rows and youth/kinematics metadata, linked to core via Gaia DR3/DR2 IDs.

Columns:
- `ultracoolsheet_object_id BIGINT`
- `stable_object_key TEXT` (nullable when unmatched)
- `star_id BIGINT` (nullable when unmatched)
- `gaia_id BIGINT` (matched core Gaia ID when present)
- `gaia_dr3_source_id BIGINT`
- `gaia_dr2_source_id BIGINT`
- `object_name TEXT`
- `name_simbadable TEXT`
- `ra_deg DOUBLE`
- `dec_deg DOUBLE`
- `plx_mas DOUBLE`
- `pmra_mas_yr DOUBLE`
- `pmdec_mas_yr DOUBLE`
- `rv_kms DOUBLE`
- `dist_pc DOUBLE`
- `dist_source TEXT`
- `spectral_type_opt TEXT`
- `spectral_type_ir TEXT`
- `spectral_numeric DOUBLE`
- `gravity_opt TEXT`
- `gravity_ir TEXT`
- `age_category TEXT`
- `youth_evidence TEXT`
- `banyan_hypothesis_young TEXT`
- `banyan_prob_young DOUBLE`
- `is_exoplanet_host BOOLEAN`
- `has_unresolved_multiplicity BOOLEAN`
- `has_resolved_multiplicity BOOLEAN`
- `has_higher_mass_companion BOOLEAN`
- `match_confidence DOUBLE`
- `confidence_tier TEXT`
- `ref_discovery TEXT`
- provenance fields (`source_*`, `retrieval_*`, `ingested_at`, `transform_version`)

## `wise_sources`, `catwise_sources`, `allwise_sources`

Targeted WISE/CatWISE/AllWISE source rows matched around existing Spacegate
objects. These are infrared evidence rows, not a primary inventory backbone.

Columns:
- `wise_source_id BIGINT`
- `source_catalog TEXT` (`catwise|allwise`)
- `source_version TEXT`
- `source_key TEXT`
- `source_designation TEXT`
- `source_id TEXT`
- `ra_deg DOUBLE`
- `dec_deg DOUBLE`
- `retrieved_at TIMESTAMP`
- `provenance_json JSON`
- `source_row_hash TEXT`

Rules:
- Do not bulk-promote WISE-only rows into `core`.
- CatWISE/AllWISE identifiers are secondary/copyable metadata unless no better
  public name exists.
- Source positions and motion fields are evidence for matching and AAA
  investigation, not Gaia-grade identity or distance authority.
- `catwise_sources` and `allwise_sources` are filtered convenience tables over
  `wise_sources`.

## `infrared_source_matches`

Deterministic cross-reference decisions between existing Spacegate targets and
WISE/CatWISE/AllWISE source rows.

Columns:
- `infrared_match_id BIGINT`
- target binding: `target_type TEXT`, `target_id BIGINT`, `system_id BIGINT`,
  `stable_object_key TEXT`
- source binding: `source_catalog TEXT`, `source_version TEXT`,
  `source_key TEXT`, `source_designation TEXT`
- matching: `angular_sep_arcsec DOUBLE`, `match_rank INTEGER`,
  `match_score DOUBLE`, `confidence_tier TEXT`, `match_method TEXT`,
  `conflict_status TEXT`
- `provenance_json JSON`

Rules:
- Match scoring may use angular separation, epoch/proper-motion propagation,
  source designation, W1/W2 color, SNR, and quality/artifact/blend flags.
- Materialization must resolve `target_id`, `system_id`, and
  `stable_object_key` against the current canonical CORE. Cooked collection-time
  row IDs and pre-canonical stable keys are lineage inputs, not serving keys.
- The WISE verifier fails when a bound target does not resolve, its system does
  not match the canonical target's system, or its stable key differs from CORE.
- Low-confidence or ambiguous rows must remain candidates or diagnostics; they
  must not create new core objects.
- Accepted matches attach to the correct object level when deterministic:
  system, member star, brown dwarf, compact object, or unresolved evidence row.

## `infrared_photometry`

WISE-band photometry and quality context associated with an infrared match.

Columns:
- `infrared_photometry_id BIGINT`
- source and target binding fields
- `w1_mag`, `w2_mag`, `w3_mag`, `w4_mag`
- `w1_snr`, `w2_snr`, `w3_snr`, `w4_snr`
- `quality_flags TEXT`
- `artifact_flags TEXT`
- `blend_flags JSON`
- `provenance_json JSON`

## `infrared_motion_evidence`

WISE/CatWISE apparent motion and parallax-like support fields.

Columns:
- `infrared_motion_id BIGINT`
- source and target binding fields
- `pm_ra DOUBLE`
- `pm_dec DOUBLE`
- `pm_unit TEXT`
- `pm_ra_error DOUBLE`
- `pm_dec_error DOUBLE`
- `parallax_like_arcsec DOUBLE`
- `parallax_like_error_arcsec DOUBLE`
- `parallax_like_note TEXT`
- `provenance_json JSON`

Rules:
- CatWISE `par_pm`-style values are candidate evidence only. They must not be
  treated as Gaia-grade parallaxes or as accepted distances without review.

## `infrared_candidate_queue`

Conservative review queue for WISE/CatWISE/AllWISE sources that may indicate
missing nearby ultracool or brown-dwarf objects. In v1 the queue is narrow and
is generated only from targeted WISE queries around priority Spacegate objects.

Columns:
- `infrared_candidate_id BIGINT`
- `candidate_status TEXT` (`needs_review|accepted|rejected|quarantined`)
- `candidate_kind TEXT`
- nearest Spacegate context: `nearest_target_type TEXT`,
  `nearest_target_id BIGINT`, `nearest_system_id BIGINT`,
  `nearest_stable_object_key TEXT`
- source binding: `source_catalog TEXT`, `source_version TEXT`,
  `source_key TEXT`, `source_designation TEXT`
- position and ranking: `ra_deg DOUBLE`, `dec_deg DOUBLE`,
  `angular_sep_arcsec DOUBLE`, `candidate_score DOUBLE`
- infrared/motion basis: `w1_minus_w2 DOUBLE`, `pm_total_arcsec_yr DOUBLE`,
  `w2_snr DOUBLE`, `review_reason TEXT`
- `provenance_json JSON`

Rules:
- Queue rows are review prompts, not inventory rows.
- Nearest-target context is re-resolved against current CORE when the target is
  still present; stale collection-time system IDs and stable keys must not be
  exposed as current bindings.
- `accepted` means accepted for further curation or a reviewed bridge process;
  it still does not directly create `core` rows without an explicit inventory
  promotion path.
- `rejected` and `quarantined` statuses must be preserved for auditability.

## `infrared_image_products`

Metadata for WISE/IRSA image products associated with Spacegate objects. In v1
the API writes lazy cache metadata outside the immutable build artifact; this
ARM table is reserved for build-time or reviewed image-product materialization.

Columns:
- `infrared_image_product_id BIGINT`
- object binding fields
- `source_catalog TEXT`
- `source_version TEXT`
- `collection TEXT`
- `bands_json JSON`
- `center_ra_deg DOUBLE`
- `center_dec_deg DOUBLE`
- `cutout_size_arcmin DOUBLE`
- `source_url TEXT`
- `derivative_path TEXT`
- `retrieved_at TIMESTAMP`
- `attribution TEXT`
- `provenance_json JSON`

Rules:
- Generated image derivatives must live outside the repo and outside immutable
  science semantics.
- Image panels must link back to IRSA/source products and clearly state that the
  imagery is observational survey imagery, not an artist impression.

## Lifecycle Audit Mirrors

Arm mirrors lifecycle lineage tables from core so lifecycle diffs/audits remain available even when hot-path core tables are optimized for serving.

## `planet_catalog_observations`

Per-catalog observation snapshots used for lifecycle resolution.

Columns:
- mirrored 1:1 from `core.planet_catalog_observations`
- includes source identity (`source_catalog`, `source_catalog_object_id`), payload fields, retrieval lineage, and transform lineage

## `planet_status_history`

Deterministic status-resolution history per planet.

Columns:
- mirrored 1:1 from `core.planet_status_history`
- includes previous/new status, resolution reasons, classifier versions, and timestamps

## `planet_reclassification_audit`

Derived change log for lifecycle/taxonomy/habitability classifier transitions.

Columns:
- mirrored 1:1 from `core.planet_reclassification_audit`
- includes transition type, impacted flags, and classifier version lineage

## TESS Identity and Transit Evidence

These tables are immutable per build and materialized only from the pinned,
targeted TESS evidence snapshot. They do not expand canonical planet inventory.

## `tess_target_identity`

One row for every targeted TIC ID. It partitions every target into `accepted`,
`missing`, `excluded`, `ambiguous`, or `source_missing`; preserves target-source
families, TIC artifact flags, identifiers, Gaia candidates, match evidence, and
provenance; and points accepted rows to the resolved core star and system.

## `tess_missing_object_audit`

The non-accepted identity subset, classified as TIC artifact/split/duplicate,
ambiguous identity, source missing, outside distance scope, valid Gaia DR3
excluded or absent, Gaia DR2-only/unmapped, or insufficient evidence. This is a
review queue and cannot create core rows by itself.

## `toi_current_evidence`

Current NASA TOI evidence with TIC/TOI identifiers, disposition, resolved host,
transit measurements and uncertainties, stellar context, source timestamps,
and explicit match methods. `CP`/`KP` rows link to canonical planets only when
host and orbital-period agreement are unique. `PC`/`APC` remain candidate
evidence; `FP`/`FA` remain negative evidence. Neither class may affect default
planet counts.

## `toi_disposition_history`

Append-only disposition events keyed by TOI, disposition, and effective source
timestamp. Each event records first/last observation timestamps, source row
hash, retrieval checksum, and transform lineage. A disposition change adds an
event rather than rewriting prior history.

## Multiple-Component Source Evidence

### `sb9_systems`, `sb9_aliases`, `sb9_orbits`

Source-native Ninth Catalogue of Spectroscopic Binary Orbits rows. Systems
preserve primary and secondary spectral types separately; aliases and orbital
solutions retain their SB9 sequence and source row lineage. These tables are
evidence and do not create canonical stars or systems.

### `multiple_component_evidence_matches`

Deterministic reconciliation outcomes between a component-bearing source row
and existing ARM component endpoints.

Required fields include source catalog/record ID, WDS identity where
applicable, primary/secondary endpoint keys, `match_status`, explicit `reason`,
match/version metadata, structured evidence, and full source provenance.

Accepted v1 methods are:

- SB9: exact, unique MSC `SB9_<sequence>` reference plus two existing stellar
  endpoints
- DEBCat: exact canonical system plus a unique MSC period match within the
  declared tolerance plus two existing stellar endpoints

Missing, ambiguous, and graph-unresolved matches remain quarantined. Name-only
component matching is forbidden.

### `multiple_component_stellar_evidence`

One source-native spectral-type observation per accepted component endpoint and
role. Accepted evidence may add a high-confidence `source` row to
`derived_stellar_classifications`; it does not overwrite the source row or
delete lower-priority mass/temperature derivations.

## Planned Agent-Assisted Adjudication Tables

Agent-derived scientific proposals belong in `arm`, never directly in `core`.

## `adjudication_candidates` (planned)

Machine-assisted identity/hierarchy/host-resolution proposals for review or later deterministic promotion.

Columns:
- `adjudication_candidate_id BIGINT`
- `stable_object_key TEXT` (nullable when the object is not yet canonicalized)
- `object_type TEXT` (`system|star|planet|pair|subsystem`)
- `proposal_kind TEXT` (`identity_merge|hierarchy_fix|planet_host_fix|missing_field_fill|source_conflict`)
- `proposal_status TEXT` (`proposed|accepted|rejected|superseded`)
- `target_ids_json TEXT`
- `candidate_values_json TEXT`
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `reasoning_summary TEXT`
- `source_citation_ids_json TEXT`
- `generator_version TEXT`
- `model_id TEXT`
- `prompt_version TEXT`
- provenance fields (`source_*` when applicable, `retrieval_*`, `ingested_at`, `transform_version`)

Rules:
- rows are advisory/proposed science support, not canonical truth
- acceptance into deterministic canonical rules must be explicit and auditable

## `missing_field_proposals` (planned)

Agent-proposed fills for scientific fields absent from canonical sources but supported by cited public evidence.

Columns:
- `missing_field_proposal_id BIGINT`
- `stable_object_key TEXT`
- `object_type TEXT`
- `field_name TEXT`
- `proposed_value_json TEXT`
- `units TEXT` (nullable)
- `confidence_score DOUBLE`
- `confidence_tier TEXT`
- `source_citation_ids_json TEXT`
- `generator_version TEXT`
- `model_id TEXT`
- `prompt_version TEXT`
- `created_at TIMESTAMP`

Rules:
- proposals remain in `arm` until separately accepted by deterministic policy
- no agent-filled value may silently overwrite source-native `core` fields

## Extended-Object Evidence

- `extended_object_source_records`: source-shaped normalized rows joined to the
  CORE reconciliation outcome
- `extended_object_geometry_evidence`: all usable source positions and extent
  observations, not only the selected CORE geometry
- `extended_object_distance_evidence`: source-native/historical distance fields
  plus the explicitly preferred CORE distance decision
- `extended_object_relations`: illuminating/central-star links with resolved
  star/system targets where exact catalog identity permits

These tables preserve disagreement and lineage. They do not create canonical
stars, systems, planets, or planet counts.

## Quality Gates (Arm)

Build fails when:
- any edge references missing component keys
- hierarchy cycle detected in `contains/subsystem_of`
- `preferred_solution_id` points to non-existent orbital solution
- confidence/provenance required fields are null
- `animation_readiness` marks `full` while missing required Kepler set
- MSC orbit reconciliation leaves any preserved row unaccounted or quarantined
  at promotion time
- an accepted WDS pair lacks two distinct endpoints, or any WDS-only row
  asserts binding / simulation-ready orbit status

Run `scripts/verify_source_evidence_closeout.py` against candidate ARM and
canonical-hierarchy artifacts to emit the machine-readable gate report.

## WDS Evidence Lake Policy

Evidence Lake E4 preserves the WDS fixed-width summary and its pinned format
document as release-scoped source records before ARM selection. WDS identity,
discoverer designation, and any pair designation remain separate. A local pair
label is usable only inside a WDS-qualified composite key; a bare `A`, `AB`, or
`Aa,Ab` value is never a component identity.

The WDS spectral field is deliberately opaque because its source definition
allows the value to describe component A or two components. Relative position
angles, separations, first/last observation years, measure counts, component
magnitudes, source-convention proper motions, and exact J2000 coordinate text
remain source-scoped evidence. Invalid/source-sentinel values stay in immutable
typed rows but cannot enter normalized measurement columns.

The CDS WDS-Gaia bridge is a best positional match within 2 arcseconds. ARM may
consume it only as a candidate relation with angular-distance evidence; it is
not a probability, accepted physical identity, containment edge, or orbital
solution. Copied Gaia columns in the bridge are match context, while the
release-native Gaia adapters own their scientific parameter semantics.

## Gaia UCD Association Evidence Policy

The pinned `J/A+A/669/A139 table4` release is a published set of HMAC and
BANYAN association assignments for Gaia DR3 ultracool-dwarf sample rows. It is
not a spectral-type table and does not independently define canonical object
existence. E4 preserves the Gaia identity, exact source row, and method
document, then emits two separately keyed membership families.

HMAC integer cluster labels are deterministic source assignments with null
membership probability. BANYAN rows retain only the published best-hypothesis
label and its probability. A missing `--` assignment stays source context; ARM
must not turn it into a cluster identity or a zero-confidence membership.
Neither family creates containment or a selected stellar classification until
E5 resolves identity and applies a quantity-specific policy.

## UltracoolSheet Evidence Policy

Evidence Lake E4 preserves the pinned 242-column source row before ARM
selection. Direct optical and infrared spectral/gravity classifications remain
separate evidence families. Spreadsheet-selected astrometry, propagated
coordinates, ages, spectral numeric encodings, and photometric distances are
explicitly labeled maintainer formula/model results; they do not become direct
measurements or silently outrank release-native Gaia/source evidence.

Configured numeric evidence must be finite. Source `nan`/`null` lexemes and
negative Pan-STARRS uncertainty sentinels remain exact source context but cannot
enter normalized values. Gaia DR2 and DR3 identities use distinct namespaces.
Pipe-delimited SIMBAD aliases remain unsplit until a versioned parser proves
token semantics. Multiplicity and exoplanet flags without safe target endpoints
stay source context and cannot create relation, containment, planet lifecycle,
or canonical inventory rows. E5 must bind each source row and apply
quantity-specific authority before selecting any public value.

E5 policy v6 binds each populated classification evidence row independently to
a unique current Gaia DR3 star. Direct optical/infrared spectral types and
gravity classes remain separate quantities; age category, literature flag, and
youth evidence remain categorical strings. Component-scoped rows remain
unresolved under the default null-scope policy rather than inheriting a
source-record target.

## Targeted TESS Evidence Policy

The Evidence Lake TESS adapter is bounded to the versioned target universe; it
does not mirror TIC. TIC v8 Gaia identifiers remain Gaia DR2 claims and reach
Gaia DR3 only through official release-neighborhood evidence. Combined
Hipparcos/Tycho-2/2MASS best-neighbor rows retain their source member path before
receiving an external namespace.

Numeric and display TOI forms remain separate raw claims normalized to the same
release-scoped TOI identity. Confirmed and known dispositions are positive
evidence, candidate and ambiguous-candidate dispositions remain candidate
evidence, and false positives/false alarms remain negative evidence. E4 keeps
all object bindings unresolved and contains no canonical system, star, planet,
or alias inventory table; E5/E6 must reconcile identities and lifecycle policy
before any selected or public projection changes.

## MSC Policy

MSC is mandatory in default science ingest and arm hierarchy/orbit derivation.
Evidence Lake E4 preserves MSC before ARM selection as release-scoped component,
relation, orbital, stellar, astrometric, photometric, note, and citation
evidence. Component identity is the composite WDS plus source label; a repeated
local label such as `A` is never a global identity. `Type` status is preserved
as positive, ambiguous, or negative relation evidence, but no E4 relation
asserts canonical containment. The release's documented numeric-zero-is-
unknown rule applies to normalized measurements while exact lexical values
remain in source-native Parquet and source-record lineage.
MSC `comp.tsv`, `sys.tsv`, and `orb.tsv` must all be preserved as deterministic
cooked inputs. `sys.tsv` supplies source-native subsystem membership and parent
relationships; `orb.tsv` supplies source-native orbit solutions. ARM builders
must not reconstruct complex hierarchy solely from component counts or suffix
pair inference when source subsystem/orbit rows are available.

MSC endpoint labels named by `sys.tsv` or `orb.tsv` are source-native evidence.
If an endpoint is not an exact source subsystem parent label, Spacegate
materializes a deterministic ARM leaf component for that endpoint. This allows
case-sensitive source distinctions such as leaf `Ab` versus subsystem `AB`
without duplicating exact subsystem labels such as `Aab`. Legacy count-expanded
leaf labels are reserved for systems without usable source-native endpoint rows.
Endpoint component type follows source evidence where practical; a very low-mass
endpoint below the hydrogen-burning boundary with no stellar spectral evidence
is preserved as a substellar support component, not counted as a stellar leaf.

Canonical hierarchy emission consumes both source-native nested MSC subsystem
edges and MSC inferred leaf components. When ARM preserves a nested source tree
rather than direct root-to-leaf edges, canonical hierarchy must still expose
auditable descendant leaves for benchmark systems where MSC/WDS evidence
supports them. These leaves remain ARM/canonical-hierarchy support evidence;
they are not silently promoted into flat `core.stars` rows.

If MSC retrieval/cook fails:
- ingest must fail
- promotion must not proceed
