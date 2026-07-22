# Retention Policy

This policy governs Spacegate build artifacts in `$SPACEGATE_STATE_DIR`.

## Scope

- `out/<build_id>/` immutable build artifacts (`core.duckdb`, `arm.duckdb`, parquet, disc outputs, `map_tiles/`)
- `reports/<build_id>/` per-build reports

Out of scope (never pruned by retention script):

- `raw/` catalog downloads
- `cooked/` normalized catalog exports
- `reports/manifests/` source manifests
- bounded runtime caches such as WISE image previews and simulation scenes; these have their own cache
  enforcement because they are mutable runtime artifacts, not immutable builds

Published bootstrap metadata may reference checksummed build reports. The
bootstrapper installs only bounded relative JSON paths under
`reports/<build_id>/`, verifies byte counts and SHA-256 digests, and stages the
directory before promotion. Published report directories follow the same
retention protection as local build reports; do not remove reports referenced
by the active `dl/current.json`.

Derived public-slice and side builds preserve verification lineage through
`derived_build_verification_report.json`. Retain the report with its build; it
contains the hashes and reported build IDs of the applicable upstream reports,
while strict verification recomputes its slice-native counts and integrity
checks against the derived database.

Simulation-scene runtime artifacts live under
`$SPACEGATE_STATE_DIR/cache/simulation_scenes/<build_id>/`. They are fully
regenerable presentation products and must never be treated as science inputs.
Each artifact carries a materializer contract version. The API must reject
older semantic versions after scene classification, naming, membership, or
evidence-precedence behavior changes; regenerate the bounded priority set with
the current materializer instead of treating stale presentation output as
compatible. Side-build materialization may reuse a scene only when both its
contract version and embedded target build ID match; a copied scene from the
source build must be regenerated even when its schema version is current.
The API opportunistically prunes oldest artifacts to a 2 GiB default cap;
operators may set `SPACEGATE_SIMULATION_SCENE_CACHE_LIMIT_BYTES`.
The Admin `Warm Simulation Scenes` action writes to this runtime-cache location,
including a regenerable `materialization_report.json`; it must never target the
served symlink or an immutable build directory after promotion. Deferred scene
warming does not make a build incomplete and is not promotion evidence.
Keep the served build's directory during normal operation; directories for
build IDs no longer retained in `out/` or referenced by `served/` may be pruned
as a separate cache cleanup after promotion verification.

## Default Policy

- Keep the currently served build (`served/current`) regardless of age.
- On Photon, keep newest 12 build directories in `out/`.
- On Photon, keep newest 24 per-build report directories in `reports/`.
- On smaller hosts, use at least newest 6 build directories and newest 12 report
  directories unless disk pressure requires tighter local overrides.
- Remove stale temporary ingest paths (`out/*.tmp`).
- Build/report directory detection accepts both dashed build IDs
  (`YYYY-MM-DDT...`) and compact build IDs (`YYYYMMDDT...`), with either
  minute-resolution (`HHMMZ`) or second-resolution (`HHMMSSZ`) timestamps.

## Scripted Cleanup

Use:

```bash
scripts/prune_state_retention.sh
```

This is dry-run by default. To apply:

```bash
scripts/prune_state_retention.sh --apply
```

Admin v2 exposes retention controls in the Builds workspace:

- **Retention Dry Run** runs this script without `--apply`, records an
  auditable job log, and shows the parsed candidate plan in the Builds page.
- **Retention Apply** is a guarded high-risk action. It requires a matching
  successful dry-run from the last 6 hours, an unchanged candidate hash, and the
  confirmation phrase. It deletes only the exact candidate directories from the
  checked plan. `raw/`, `cooked/`, and `served/current` are protected.
  After a matching dry-run succeeds, the Builds page updates the apply card with
  the dry-run job id, candidate count, estimated reclaimable space, and candidate
  hash.
- If a dry run finds zero candidates, that is a successful null result. Admin
  should report zero reclaimable bytes and leave Retention Apply disabled
  because there is nothing to delete.

Useful overrides:

```bash
scripts/prune_state_retention.sh --keep-builds 8 --keep-reports 16 --apply
scripts/prune_state_retention.sh --no-prune-tmp
```

Photon's current generous default:

```bash
scripts/prune_state_retention.sh --keep-builds 12 --keep-reports 24 --apply
```

Builds created before the timestamped-ID contract may use the legacy
`YYYYMMDDT_<label>` form. They remain invisible by default because a named
workspace can have the same shape. Include them only in a reviewed maintenance
window with `--include-legacy-builds`; applying that mode requires the exact
`--expected-candidate-set-sha256` printed by the immediately preceding dry run.
Retain historical reports independently when their size is negligible, for
example with `--keep-reports 1000`.

Before applying retention, preserve every published, rollback, or otherwise
referenced build reported by the Evidence Lake storage audit. Supply the set
explicitly so an unserved but required checkpoint cannot be selected merely by
age:

```bash
jq -r '.build_references | keys[]' \
  /data/spacegate/state/reports/evidence_lake_v2/e0_storage_audit.json \
  > /data/spacegate/state/reports/evidence_lake_v2/protected_builds.txt

SPACEGATE_STATE_DIR=/data/spacegate/state \
  scripts/prune_state_retention.sh \
    --keep-builds 12 \
    --keep-reports 24 \
    --protect-file /data/spacegate/state/reports/evidence_lake_v2/protected_builds.txt
```

Review that dry run before adding `--apply`. The script always protects
`served/current`; `--protect-build` and `--protect-file` extend that set for
metadata references the retention script cannot safely infer on its own.

Run retention only after successful promotion and verification. Do not run it
during ingest or while diagnosing a failed build.

Published database archives under `dl/db` have an independent rollback policy.
Keep the current archive and at least two verified rollback archives with their
matching `dl/reports` directories. Use the fail-closed dry-run/apply sequence;
the current symlink must resolve inside `dl/db`, and apply requires the exact
reviewed candidate hash:

```bash
.venv/bin/python scripts/prune_published_downloads.py \
  --dl-root /data/spacegate/dl \
  --keep-archives 3 \
  --reason 'Retain current plus two verified rollback archives' \
  --report /data/spacegate/state/reports/evidence_lake_v2/published_retention_dry_run.json

.venv/bin/python scripts/prune_published_downloads.py \
  --dl-root /data/spacegate/dl \
  --keep-archives 3 \
  --reason 'Retain current plus two verified rollback archives' \
  --report /data/spacegate/state/reports/evidence_lake_v2/published_retention_applied.json \
  --apply --expected-candidate-set-sha256 '<reviewed-hash>'
```

Refresh the Evidence Lake storage audit after published retention and before
state-build retention. Protect every canonical/public/side dependency still
referenced by the retained published reports.

On July 20, 2026, an E0 storage audit found 621 GiB under `state/out` while the
ordinary dry run returned zero candidates: 18 superseded builds used the
legacy name form. No ingest/compiler process was active, LAMOST v63 had passed
clean reproduction and been pushed, and the refreshed protection file listed
all 11 published/rollback/served references. A build-only dry run retained the
newest 12 builds and all reports; exact candidate hash
`e32226b51121daf22850650296cfae330606010998a858ae30f6617c8eced540`
authorized removal of 364.82 GiB. Post-apply verification found every protected
build and `served/current`, and `/data` rose to 495.6 GiB free. Raw, typed,
cooked, report, accepted E4, and source artifacts were not candidates.

Evidence Lake v2 raw and typed snapshots are not `out/` retention candidates.
Keep the active raw snapshot and active parser-contract snapshot for every
registered source, plus any snapshot referenced by a build, report, publication,
rollback, or adjudication packet. Superseded parser outputs may be proposed for
retirement only after the replacement passes
`scripts/verify_evidence_lake_reproduction.py` and E7 records that no retained
lineage references them. Never prune individual Parquet files from inside a
typed snapshot. After E2's forward/reverse Gaia release evidence, the active
estate is 63 raw artifacts and 5,213,454,799 typed Parquet bytes across 27
source releases; older immutable parser checkpoints explain why the physical
typed directory is larger until E7 retirement.

E7 cutover planning does not authorize cleanup. The pre-promotion ledger in
`config/evidence_lake/e7_legacy_path_inventory.json` marks every old cooker,
compiler, and diagnostic path as transitional, retained, or deprecated; none is
retired. Promotion must first pass the scientific review, atomic local pointer
change, production-topology verification, rollback drill, and re-promotion in
`docs/E7_CUTOVER_AND_GAIA_DR4.md`. Only then may specifically enumerated,
reproducible cooked or build artifacts enter the ordinary reviewed retention
dry-run. Raw/typed evidence, permanent identity outcomes, manifests, citations,
reports, served and rollback builds, and transitive inputs remain protected.

Content-addressed acquisition target seeds under
`derived/evidence_lake_v2/acquisition_targets/` are also lineage artifacts, not
scratch. Preserve seed `638c3ff4e58abcd355029e0f` while any accepted Gaia
uncertainty-supplement product manifest references it. A future seed may be
retired only after every dependent product is superseded, the replacement raw
and typed releases pass clean reproduction, and E7 proves no retained manifest,
report, publication, rollback, or adjudication packet references the old seed.
Never prune individual seed artifacts or rewrite their manifests.

The July 19 E4 wide-binary compiler work produced bounded checkpoint
`aaf262b1791d98ce3e9f96e7` (11,129,073,664 bytes) and several hidden temporary
scientific-evidence directories from failed or deliberately interrupted
diagnostic builds. Never delete those directories manually. General state
retention remains prohibited while E3 acquisition or E4 verification is active.
When a stopped compiler diagnostic itself creates material storage pressure, the
only allowed narrower exception is the explicit fail-closed command below. Its
interrupted-build mode accepts direct hidden children only and refuses
manifests, symlinks, shared files, and live file descriptors. A separate
independent-audit mode accepts a manifest-bearing 24-hex build only when the
external artifact audit has `status=fail`, identifies the same build and
database, contains at least one nonzero check, and the database still matches
the immutable manifest checksum. Both modes require a reviewed dry-run
candidate hash before whole-directory removal:

```bash
.venv/bin/python scripts/prune_evidence_lake_artifacts.py \
  --state-dir /data/spacegate/state \
  --candidate .<build-id>.<temporary-suffix> \
  --reason '<specific reproducible failure and replacement evidence>' \
  --report /data/spacegate/state/reports/evidence_lake_v2/e4_retention_dry_run.json

.venv/bin/python scripts/prune_evidence_lake_artifacts.py \
  --state-dir /data/spacegate/state \
  --candidate .<build-id>.<temporary-suffix> \
  --reason '<same reviewed reason>' \
  --expected-candidate-set-sha256 '<exact dry-run hash>' \
  --apply \
  --report /data/spacegate/state/reports/evidence_lake_v2/e4_retention_applied.json
```

For an immutable diagnostic that failed independent audit, substitute:

```bash
.venv/bin/python scripts/prune_evidence_lake_artifacts.py \
  --state-dir /data/spacegate/state \
  --failed-audit '<build-id>=/absolute/path/to/artifact_audit.json' \
  --reason '<specific audit failure and preserved reproduction inputs>' \
  --report /data/spacegate/state/reports/evidence_lake_v2/e4_failed_build_dry_run.json
```

Apply still requires the exact dry-run `candidate_set_sha256`. The durable
compiler and audit reports remain outside the retired artifact and preserve its
identity, metrics, checksum, and failure reason.

The minimum age is 60 minutes by default. Reducing it is an explicit operator
decision and does not weaken the no-live-process, no-manifest, or whole-artifact
gates. Accepted, served, published, rollback, and merely superseded artifacts,
source snapshots, typed snapshots, identity graphs, and individual files inside
an artifact remain outside this command's scope.

The targeted TESS scientific-evidence audit contract is explicitly allowlisted
for failed whole-artifact retirement. After accepted build
`11aa9bd00cc710f971b01837` passed generic/source-specific audits and clean
logical-hash reproduction, reviewed candidate-set hash
`9164bca7a24f0e9fe57d6c5930b3c9daef1f235b974e946ecc18e4320788517d`
retired two manifestless compiler attempts and three independently failed v50-
v52 artifacts. The apply report reclaimed 2,600,095,744 allocated bytes while
preserving the v53 artifact, raw/typed snapshots, and all diagnostic reports.

Corrected TESS E4 artifact `03acb9eb0fb2cbc0f8203dd8`, its compile, generic,
targeted-source, and clean-reproduction reports, and release set
`6c19de054e9b807674c37d3c` are protected E4/E5 inputs. Initial artifact
`11aa9bd00cc710f971b01837` remains retained diagnostic history until E6 and a
reviewed dry run account for its references; it is not the accepted TESS shard.
E5 artifact `86aa5553053db35d81ff26e0`, its nine Parquet projections, policy,
compiler, compile/reproduction reports, and independent audit are protected E6
inputs. No partial projection or transitive E4/identity/canonical input may be
removed independently.

On July 19, the reviewed dry run
`e4_simbad_failed_artifact_retention_dry_run.json` identified exactly two closed,
manifest-less SIMBAD compiler diagnostics affected by the confirmed
199,495,267,914-row disjunctive citation-join plan. Candidate-set hash
`39ba9c0104621131a5c4b1673bda9a086f6689e7491ec6b7c85467e14fa0eece`
authorized the applied report
`e4_simbad_failed_artifact_retention_applied.json`, reclaiming
73,183,408,128 allocated bytes. Immutable raw and typed SIMBAD inputs were not
changed. Compiler v36 removed the disjunctive join but failed a later identity
audit; v37 retains bounded equality matching and explicitly quarantines failed
identifier normalizations.

The complete-envelope v37 retry then failed closed at the configured 16-GB
DuckDB limit while expanding bundled astrometry citations in one operation. Its
manifestless temporary had no live file descriptors and contained only the
compiler database. Under storage pressure, the explicit zero-age dry run
`e4_simbad_v37_failed_temporary_retention_dry_run.json` selected only that tree
with candidate hash
`0dc54bd5a607cff1da7f5315ea147a5075f6389c6cb28e29a2523215fca23204`.
The matching applied report reclaimed 36,925,886,464 allocated bytes. Raw/typed
inputs, failure logs, immutable v36, and the active v38 replacement remained
protected.

The v38 replacement then passed independent artifact audit and clean logical-
hash reproduction as build `fc5bd4e6398d72bde50ba6d5`. Only after those
reports were durable did
`e4_simbad_v36_failed_artifact_retention_dry_run.json` select immutable v36
build `07230826efefffce913a3569`, whose independent audit failed exactly 285
blank identifier claims. Candidate hash
`de47f05ca412b29f501f0eb1ee7e23b3be2327f7d5834de7f7114fe1f96af8f5`
authorized the matching applied report and reclaimed 42,799,505,408 allocated
bytes. The raw and typed snapshots, accepted v38 artifact, audit, reproduction,
compiler, and retention reports remain protected.

Evidence Lake identity graphs under
`derived/evidence_lake_v2/identity/<graph_id>/` are immutable compiler
artifacts. Preserve the graph named by the adjacent atomic `current` pointer and
any graph referenced by a build, report, publication, rollback, or adjudication
packet. Never prune individual graph Parquet files or its DuckDB inspection
database independently. A superseded, unserved compiler iteration may be
removed only after the replacement passes
`scripts/verify_evidence_identity_reproduction.py` and the machine report no
longer references it. Clean reproduction graphs belong under `tmp/`; remove the
whole scratch graph only after the comparison report is durably written.

Scientific evidence compiler artifacts under
`derived/evidence_lake_v2/scientific_evidence/<build_id>/` are also immutable
units. Preserve any artifact named by a current pointer or referenced by a
shadow/served build, report, publication, rollback, or adjudication packet.
Never remove `scientific_evidence.duckdb` independently from its manifest. An
in-progress compiler checkpoint may be retired only as a whole after its
replacement has a durable logical-hash comparison; accepted E4/E5 inputs remain
protected until E7 records that no retained projection references them. Clean
reproduction uses `scripts/verify_scientific_evidence_reproduction.py`; its
scratch tree is removed only after comparison and the durable report is written.

GCVS typed snapshot `ef540a47c43892e17ddc2bae` supersedes parser-v1 snapshot
`85d1e49a6ce11402c4d06a5f` for downstream work. The immutable raw snapshot is
unchanged, and the typed A/B plus clean-reproduction reports are durable. Keep
both typed snapshots until the combined E4/E5 input set has been verified; the
GCVS E4 adapter now passes, but this checkpoint authorizes no typed-snapshot
cleanup. Accepted scientific-evidence checkpoint `a6f6669d2bd48eac5d6204d2`
and its compile, artifact-audit, source/scope-audit, and clean-reproduction
reports are protected E4/E5 inputs. Provisional GCVS compiler artifacts may be
retired only as whole immutable units through the retention dry-run after this
accepted replacement and its reports are durable.
The source-specific GCVS scientific audit is an explicitly allowlisted failed-
artifact audit contract; arbitrary audit schemas cannot authorize cleanup. On
July 19, candidate-set hash
`d4d63bcd3f16cea22353667725c5e1ec2bb27c6a29854f1b05c2f07ebac21ca5`
authorized whole-artifact removal of independently failed provisional builds
`4d5c71f6e7e537f4c7d56693` and `41c15d3394bba2eef8d278a5`, reclaiming
1,855,528,960 allocated bytes. Their audit and retention reports remain; the
accepted checkpoint and every raw/typed input remain protected.

Large local compiles may set `SPACEGATE_E4_TEMP_DIRECTORY` to an operator-owned
scratch root such as `/mnt/space/spacegate/tmp/evidence_lake_v2/e4_spill`.
DuckDB spill there is disposable execution state, never an evidence artifact;
the compiler assigns a build-specific directory and removes it after closing the
database. A spill tree left by a crash must still be associated with a dead
process and explicit failed build before whole-tree cleanup. Never direct this
setting into raw, typed, served, rollback, or published artifact storage.

Large scientific-evidence audits use the same operator scratch policy. Generic
and source-specific verifiers default to a 16-GB DuckDB limit and accept
`--temp-directory`; their reports record the memory limit, thread count, scratch
policy, and confirmed spill removal. A crash-left audit spill is disposable
execution state only after its process is dead and its owning audit/build is
identified; never remove an active verifier's open temporary files.

The field-complete NASA checkpoint `cb82c09179afa740b02e2cdf` is approximately
4.2 GiB (`4,497,354,752` database bytes) and is protected as the current E4
reference. Earlier content-addressed experiments and interrupted hidden
temporary directories are cleanup candidates only through a retention dry-run;
do not remove the current reference, a reproduction input, or a rollback build.
Complete-envelope SIMBAD checkpoint `fc5bd4e6398d72bde50ba6d5`, its immutable
raw/typed inputs, independent audit, and clean-reproduction report are protected
inputs to E4/E5.
Official WGSN checkpoint `0ff30b04008b93aafb3de66f`, raw snapshot
`ec563be8ca8038acd3cfe78e`, typed snapshot `437e2f27863efe5adc0423ab`,
and its compile, artifact-audit, scope-audit, and clean-reproduction reports are
also protected E4/E5 inputs.
Official GCVS checkpoint `a6f6669d2bd48eac5d6204d2`, raw and typed snapshot
`ef540a47c43892e17ddc2bae`, and its compile, artifact-audit, source/scope-audit,
and clean-reproduction reports are likewise protected E4/E5 inputs.
Hunt/Reffert checkpoint `7e66e0690aa962c837d43a86`, complete raw and typed
snapshot `cbfa7c6ec8c2e3bfbc226898`, and its compile, artifact-audit,
cluster/scope-audit, and clean-reproduction reports are protected E4/E5 inputs.
Extended-catalog checkpoint `54d1b0b6a841344c48327991`, complete raw and typed
snapshot `7753816661175edcb526c676`, and its compile, artifact-audit,
extended-scope-audit, and clean-reproduction reports are protected E4/E5 inputs.
MSC checkpoint `fc7e9dcabb0b27167c8f188c`, raw snapshot
`028096033a6805e740df3b66`, typed snapshot `d04364cc77130406a257dc89`,
and its compile, artifact-audit, MSC source/scope-audit, and clean-reproduction
reports are protected E4/E5 inputs. The MSC scientific audit is an explicitly
allowlisted failed-artifact audit contract; it does not authorize removal of
this accepted checkpoint or either source snapshot.
WDS checkpoint `ad98d4e369c5a0addc6477a0`, WDS raw snapshot
`9c84c16b098fc5339dbf0f98`, WDS typed snapshot
`d0c9fefb3acb5b2cab3d75f8`, CDS bridge raw snapshot
`d089e992715ba33e2c3c04cc`, bridge typed snapshot
`f9f48b3f691a6a38138ce5d7`, and its compile, artifact-audit, WDS source/scope-
audit, and clean-reproduction reports are protected E4/E5 inputs. The WDS
scientific audit is an explicitly allowlisted failed-artifact audit contract;
it does not authorize removal of this accepted checkpoint or source snapshots.
Gaia UCD association checkpoint `78016b90e02689547c3f53dd`, raw snapshot
`d1be498af5b1dfe7964c3891`, typed snapshot `60f97d02344bdd773438fac2`,
and its compile, artifact-audit, source/scope-audit, and clean-reproduction
reports are protected E4/E5 inputs. The Gaia UCD scientific audit is an
explicitly allowlisted failed-artifact audit contract; it does not authorize
removal of this accepted checkpoint or either source snapshot.
UltracoolSheet checkpoint `a328a9e13d6c2b44f8d57861`, raw snapshot
`14fd785307af12849666a603`, typed snapshot `32d437d41bfdfa7242bd8a4a`,
and its compile, artifact-audit, source/scope-audit, and clean-reproduction
reports are protected E4/E5 inputs. The UltracoolSheet scientific audit is an
explicitly allowlisted failed-artifact audit contract; it does not authorize
removal of this accepted checkpoint or either source snapshot. Superseded
checkpoint `20fdb1c95d25d441160d3bd9` remains protected through E5 v14 and its
E6 shadow references until replacement promotion and rollback retention are
complete. Independently
failed v46-v48 artifacts were retired as whole artifacts only after dry-run
candidate-set hash
`9db9a29f47011b94e037d8dee4e0e444e7fc9b3f2f78c403a3e9cedc26c1ea95`
was replayed during apply; 448,364,544 allocated bytes were reclaimed while
preserving the accepted build and all raw/typed inputs and reports.

Bailer-Jones accepted checkpoint `2147d1c60f6401fdc725d96e`, raw snapshot
`8920e4e6eda798c9567ca7a8`, typed snapshot `5a60000592215924b3305095`,
and its compile, bounded generic/source-audit, and clean-reproduction reports
are protected E4/E5 inputs. Its 36,958,384,128-byte database is not a cleanup
candidate. The build and reproduction used `SPACEGATE_E4_TEMP_DIRECTORY` on
`/mnt/space`; disposable spill and reproduction trees were removed
automatically only after database close and hash agreement. A prior interrupted spill was
retired through fail-closed candidate hash
`85bbf28537997740b4a056c27e854a3cf5ee82ecd9e4cfbdcff8a5ada591e39a`,
reclaiming 18,954,346,496 allocated bytes without deleting a manifest-bearing
artifact or any raw/typed evidence.
The Bailer-Jones source-audit schema is explicitly allowlisted for failed whole-
artifact retirement; this does not bypass the immutable checksum, external
failed-report, nonzero-check, no-live-handle, minimum-age, candidate-hash, or
explicit-apply gates.
After v56 passed every gate, dry-run candidate-set hash
`0ed620c92b5b47ba18f4524b90383b00e8ca388de5aec4a0fbef921e55ebee5a`
authorized removal of failed v54 plus four closed manifestless Bailer-Jones
attempts. Apply reclaimed 44,452,454,400 allocated bytes while preserving v56,
raw/typed inputs, and diagnostic/acceptance reports.

The accepted APOGEE DR17 E4 checkpoint `efc517c3dd6f6389abab7603`, its exact
raw/typed inputs, and its compile, generic-audit, and APOGEE-source-audit reports
are protected E4/E5 inputs and scientific A/B references. Its 5.4-GB database
is not a cleanup candidate. The APOGEE source-audit schema is allowlisted only
to classify a failed whole artifact for the existing fail-closed retention
workflow; it cannot authorize deletion without the other checksum, age,
live-handle, candidate-hash, and explicit-apply gates.
Optimized v60 `e794324a7c7e86e80a3ea614` supersedes the compiler execution
path while preserving v58 as the scientific A/B reference. Retention candidate
hash `f5bb515adecfb310166a1cf9a89d62056795acccfbaa1c2e02ac1581823eb494`
removed exactly three manifestless APOGEE attempts and reclaimed 3,238,584,320
allocated bytes. v60 clean reproduction passes and removes its external scratch
tree. Valid v58/v59 artifacts remain protected pending a reviewed successful-
artifact retirement policy.

Accepted GALAH DR4 v62 checkpoint `a4fc03c66ea1cfb44c25df28`, raw snapshot
`d5e378390f1922e70396fdaf`, typed snapshot `c5a93f54ed5899e03efc188c`, and its
compile, generic audit, GALAH source audit, and clean-reproduction reports are
protected E4/E5 inputs. Its 5,091,897,344-byte database is not a cleanup
candidate. The GALAH source-audit schema is allowlisted only for the existing
fail-closed whole-artifact retirement workflow. Diagnostic v61
`3d331821ee7b22d996a8efe3` was not accepted because it mislabeled published
distance fields as radii. After v62 acceptance, the independent failed audit
and fail-closed dry run selected only v61; exact candidate hash
`9f93b59b7ab0ddde233063585b0ee19c4ad2a248a2cfdd08fb20ff810a74da4a`
authorized removal of 5,170,827,264 allocated bytes. v62 compilation and
reproduction used `/mnt/space` scratch and removed the build-specific trees
after database close and logical-hash agreement.

Accepted LAMOST DR11 v63 checkpoint `a583819f0a4f3896c312f19e`, raw snapshot
`bb2975b809866ddbeba17085`, typed snapshot
`340242f63e18c31899b1d735`, and its compile, generic audit, bounded LAMOST
source audit, and clean-reproduction reports are protected E4/E5 inputs. Its
26,090,418,176-byte database is not a cleanup candidate. The LAMOST source-
audit schema is allowlisted only for fail-closed whole-artifact retirement and
cannot independently authorize removal. Compile and reproduction scratch were
removed after logical comparison.

Before the LAMOST build, exact candidate hash
`ce4b84fa18cb9cef35b8adfdf102e850c8c37d2da9135128a1a5a182e65879ba`
authorized removal of ten old hidden, manifestless, unreferenced, idle E4
staging trees and reclaimed 71,672,885,248 allocated bytes. The dry-run and
apply reports remain under `state/reports/evidence_lake_v2`; served, rollback,
published, referenced, accepted, raw, and typed artifacts were excluded.

The E4 source checkpoints `aaf262b1791d98ce3e9f96e7` (bounded wide binary),
`fcbb6466bea0a7798ae8d2ed` (ORB6), and `b3a141c0caf953aa83c4e52b`
(DEBCat), `d08c5aa9af7dc8bcdbf0d6c3` (Green SNR), and
`255678b2daa6e8bf46e6dcd9` (TESS EB) remain protected inputs to the combined
E4/E5 build. White-dwarf checkpoint `486e4975af015d4e5f5a3c9b` and its
clean-reproduction report are likewise protected. ATNF checkpoint
`64c55c19a5a10a88877d4cd2`, its pinned raw/typed package, and its audit and
clean-reproduction reports are also protected. McGill magnetar checkpoint
`c599c951590451ace4248934`, its pinned `TabO1.csv` raw/typed snapshots, and its
audit and clean-reproduction reports are likewise protected. SB9 checkpoint
`72663823963198c8fcbbe569`, all four pinned source tables, and its audit and
clean-reproduction reports are protected. SBX checkpoint
`37ffa7255d026c8d930af6d4`, complete raw/typed snapshot
`ea236790d0501967b3c30466`, and its audit and clean-reproduction reports are
protected. The served legacy SBX projection remains protected as an E6 A/B
input. Earlier SBX scientific-evidence artifact
`c0b729ea1ec32ade4548a7b7` predates the accepted component-designation
normalization; `da07cc5c9fbf36faa314d98f` and
`3f24effe31db3258870e48cb` predate complete explicit registry coverage for the
preserved legacy projections. All are retention dry-run candidates,
not manual deletion targets. Earlier SB9 compiler artifacts
`5a216d394e0773f6175ff226` and `407c81af12127feb9cf61048` predate the accepted
endpoint-scope/cross-table-link contract and are retention dry-run candidates,
not manual deletion targets. Earlier
source-specific compiler iterations may be proposed only by a retention
dry-run after the replacement's clean-reproduction report is durable; never
manually remove a hidden interrupted compiler directory.

E5 SBX checkpoint `7ae9b19a56212bfdc4f44d3b`, its compile report, independent
audit, and deterministic reproduction evidence remain protected until the
schema-aware retention audit confirms that the WDS-complete successor and its
rollback coverage are sufficient. Neither it nor the older
`bbc7f0083646dfd5a602467b` is a manual deletion target.

Combined E5 component artifact `67fea5f99500b57419ebdeb0`, its compile and
clean-reproduction reports, independent audit, ordered Parquet projections,
and source policies are protected current E6 inputs. Its policy-v8 predecessor
`f5358c0a0983958e5d4f76c5` remains the component artifact pinned by the
selected-fact-v15 source-disposition ledger and is therefore also protected.
Policy-v7 artifact `9e59131b92205068f7246a94` is an older predecessor, but it
remains a retention dry-run candidate rather than a manual deletion target
until the schema-aware audit confirms rollback and report references.

E5 cluster artifact `a6169c9ec351db81104e8518`, its ordered Parquet
projections, compiler policy, compile and clean-reproduction reports, and
independent artifact audit are protected E5/E6 inputs. Hunt/Reffert E4 artifact
`7e66e0690aa962c837d43a86` and its raw/typed lineage remain protected source
inputs; neither is a manual cleanup target.

E5 extended-object artifact `3790054572476ea189aaff06`, its ordered binding and
evidence Parquet projections, policy, compiler, compile and clean-reproduction
reports, and independent audit are protected E5/E6 inputs. Green SNR E4 artifact
`d08c5aa9af7dc8bcdbf0d6c3`, OpenNGC-family E4 artifact
`54d1b0b6a841344c48327991`, their raw/typed snapshots, and the canonical
reference remain protected transitive inputs. Neither individual Parquet files
nor a ledger-referenced whole artifact may be removed by an ordinary space
cleanup.

The white-dwarf adapter's first v21 attempt failed closed before promotion when
an unqualified source column collided with compiler lineage `source_id`. Its
hidden temporary artifact remains a retention dry-run candidate; the v22
replacement qualifies source expressions generally and reproduces cleanly.

The July 19 dry-run against `/data/spacegate/state` identified eight old build
trees and twelve old report trees with an estimated 148.86 GiB reclaimable. No
cleanup was applied: the candidates include recent canonical/public/side builds
and require served, rollback, publication, and reference confirmation first.
Evidence Lake raw/typed snapshots and the current E4 reference were not listed.

Side-artifact rebuilds created by `scripts/rebuild_side_artifacts.py` are normal
immutable `out/<build_id>/` artifacts once the `.tmp` directory is renamed into
place. Interrupted runs leave `out/<build_id>.tmp`, which is covered by the
default stale temporary ingest cleanup. The script intentionally copies only
known served artifacts and does not copy orphaned internal DuckDB temp
directories such as `core.duckdb.tmp`.
Presentation-only side builds should use `--preserve-arm` so adding immutable
map artifacts cannot change ARM evidence or expose core/ARM search-contract
drift. ARM regeneration remains the default for science-side changes.

### Tiled map artifacts

`map_tiles/` is part of the immutable served build. Content-addressed tile files
must not be pruned independently, even when the same hash occurs in another
build. The current index and radius manifests can reference any hash contained
in their own build directory. Retire tiles only by retiring the complete,
unserved `out/<build_id>/` through the normal retention dry run. Temporary tile
builds belong under `$SPACEGATE_STATE_DIR/tmp` and may be removed only after a
verified immutable build has been promoted and retained.

## Bulk Research Storage

Large research/document material should not live under repo paths or inside
`out/` build artifacts.

Photon default bulk root:

```bash
SPACEGATE_BULK_DIR=/mnt/space/spacegate
```

Use this root for:

- archived papers and source documents
- retrieved HTML/PDF/source pages
- OCR/intermediate text
- large object-dossier attachments
- reusable science-document caches
- one-off model/eval research bundles that are too large for git or reports

Recommended layout:

```text
/mnt/space/spacegate/
  research/
    sources/
    papers/
    ocr/
    dossiers/
    eval-runs/
  cache/
    source-documents/
    literature-indexes/
```

Container deployments should bind-mount `$SPACEGATE_BULK_DIR` into the API
container at the same path. If this mount is absent, Admin Runtime will report
the bulk root as missing from the container even when the host USB filesystem is
mounted correctly.

The USB-backed `/mnt/space` drive is large and fast but less trustworthy than
internal NVMe. Anything required for auditability must have durable metadata in
Spacegate state or generated databases:

- canonical URL
- source domain and trust tier
- retrieval timestamp
- content hash/checksum
- local cache path
- transform/prompt/model versions where applicable

If `/mnt/space` content is lost, Spacegate should be able to identify missing
attachments and re-fetch or mark dossiers stale from metadata.

### TESS source snapshots

Targeted TESS acquisition preserves raw inputs under
`$SPACEGATE_STATE_DIR/raw/tess_evidence/snapshots/<snapshot_id>/`. Snapshot IDs
are content-addressed across TOI, target-set, TIC, Gaia-neighborhood,
external-crossmatch, and targeted Gaia DR3 payloads. Multipart requests include
query-hash sidecars so interrupted-run resume cannot reuse a chunk for a
different target set.

- raw TESS snapshots are protected source evidence, not ordinary build
  retention candidates
- identical content reuses the same snapshot ID
- do not prune a snapshot referenced by a retained build manifest or source-
  delta history
- any future cleanup begins with a reference-counted dry run; directory age is
  not sufficient evidence for deletion

## Docker and Model Storage

Docker data and model caches are intentionally outside the Spacegate build
retention script:

- Docker data root: `/data/docker`
- model weights/caches: `/data/models`

Clean Docker image/container/build-cache slag with Docker-native tools after
checking active containers. Do not teach `scripts/prune_state_retention.sh` to
delete Docker or model data.

## Operational Notes

- Admin/API Docker jobs are expected to run as the host operator UID/GID via
  `scripts/compose_spacegate.sh`, with `SPACEGATE_UMASK=0002` by default. This
  prevents new generated state from becoming root-owned on the host.
- If older artifacts were created by root-owned container processes, normalize
  generated/admin state before cleanup:

```bash
scripts/normalize_state_permissions.sh
sudo scripts/normalize_state_permissions.sh --apply
```

- The permission normalizer is dry-run by default and avoids `raw/` and
  `cooked/`. It tightens the state root itself non-recursively, then repairs
  generated/admin subtrees.
- Production deploys grant a narrow ACL for the API runtime UID on served-build
  presentation artifacts before deploy-time presentation checks so uploaded
  build artifacts remain writable for coolness and snapshot outputs.
- Edge bootstrap from a local published `file://` archive reads the bounded
  download artifact in place instead of duplicating it under
  `cache/downloads/`. Published immutable builds with verified DISC outputs are
  promoted with `bootstrap_core_db.sh --skip-auto-score`; do not mutate them by
  re-running coolness scoring during activation.
- Run retention after successful promotion/verification, not during ingest.
- If large one-off caches (for example external catalog mirrors) are stored under the state root, move them outside `out/` and `reports/` so retention remains deterministic.
- Failed builds may be kept temporarily for diagnosis, but once the root cause is
  captured in a report or issue, remove them through the retention script rather
  than manual edits inside immutable build directories.
- Large clean-reproduction builds may use
  `verify_scientific_evidence_reproduction.py --scratch-parent` on
  `/mnt/space/spacegate/evidence-lake-reproduction`. Only disposable verifier
  scratch belongs there: accepted manifests and evidence databases remain on
  internal storage, the report must record `scratch_removed=true`, and a failed
  verifier must be inspected before its scratch tree is removed.
- The Gaia external-crossmatch v66 scale diagnostics created two manifestless
  failed staging trees. Separate zero-age dry runs and exact candidate hashes
  authorized whole-tree retirement, reclaiming 30,327,984,128 and
  30,865,903,616 allocated bytes. The accepted 47.3-GiB artifact and all raw,
  typed, report, served, and rollback data were not candidates.
- The first Gaia AP v68 full build failed closed at release-wide unresolved
  binding insertion after scientific materialization. A zero-age dry run proved
  exactly one manifestless, unreferenced, closed staging tree; candidate hash
  `fae1354fe191776ed93d66967563075b762d0af37d1df518e76670f9f93eccb7`
  authorized whole-tree retirement of 110,793,879,552 allocated bytes. No raw,
  typed, accepted, served, rollback, or report artifact was selected.
- The Gaia AP v69 retry was interrupted when its terminal/session ended before
  report publication or atomic promotion. The compiler process was absent and a
  zero-age dry run proved exactly one manifestless, unreferenced, closed staging
  tree. Candidate hash
  `0732040b55edb45e63974719f2e1b932e0a56b9d388ca93f86403e0223e41191`
  authorized retirement of 128,085,159,936 allocated bytes. The refreshed E0
  storage audit reported 440.7 GiB free, no unrecognized build IDs, and
  `acquisition_ready=true`; no raw, typed, accepted, served, rollback, or report
  artifact was selected.
- The tmux-isolated Gaia AP retry then completed the bounded per-table binding
  stage but failed closed when the still-unbounded ordinary evidence-citation
  join reached the 32-GB compiler cap. The traceback remains in
  `e4_gaia_ap_compile_v70.log`; compiler/contract v70 replaces that operation
  with 32 deterministic source-record hash buckets. A zero-age dry run selected
  exactly one manifestless, unreferenced, closed staging tree. Candidate hash
  `a546c331f63e4c09e9a7b8afdeb9a46058a5d3261acde897f36449be851aa79e`
  authorized retirement of 128,089,096,192 allocated bytes. The refreshed E0
  storage audit again reported 440.7 GiB free and `acquisition_ready=true`; no
  raw, typed, accepted, served, rollback, or report artifact was selected.
- Accepted Gaia AP checkpoint `393b08fa1268bbd42bb40225` occupies about 168
  GiB and its clean reproduction used USB scratch, matched the reference logical
  hash, and removed the scratch tree. After acceptance, published-download
  retention kept current archive `20260717T0614Z_f452835_side` plus rollback
  archives `20260717T0336Z_8bee500_side` and
  `20260717T0057Z_868b4d9_side`, retiring ten older archives and matching report
  directories under exact candidate hash
  `4ea4af63377c533bc47c53cff8667140cc69d727e1408d39b54c3ab2bde28d01`
  for 83,772,035,729 bytes.
- A subsequent standard state-retention pass explicitly protected those three
  side builds and their canonical/public dependency chains. After confirming
  zero surviving references, exact candidate hash
  `41619e24dbe0fa0243615f8e656fdfb20b04156ab0a9514f23b5282fcd60bc19`
  retired five older superseded build trees and 32 matching report directories,
  reclaiming about 93.46 GiB. The refreshed E0 audit reports 445.1 GiB free, no
  alerts, and `acquisition_ready=true`; the 57.7-GB legacy scratch pool remains
  intact for a separate report-preserving retention decision.
- Gaia supplementary AP attempts v72 and v73 each ended before manifest
  promotion: v72 received a keyboard interrupt after its tmux window became the
  attached client, and detached v73 was intentionally stopped once main-AP
  block accounting proved retained uniqueness-index amplification should be
  removed before another large compile. Zero-age fail-closed dry runs proved
  one closed, manifestless, unreferenced staging tree each. Exact hashes
  `444bb02ec5702d9c84db491bfcd4a47338516f634c5317cc569cd2d529934675`
  and `4bc6ae94b04dd1346e62fc32579d90c3f35c4030081f805f4c39b028974d134f`
  authorized whole-tree retirement of 1,129,861,120 and 1,131,433,984 bytes.
  The raw/typed supplementary release, accepted artifacts, published builds,
  rollback generations, and retained reports were not candidates.

## Extended-Object Catalog Artifacts

Extended-object raw snapshots under `raw/extended_objects`, cooked CSVs under
`cooked/extended_objects`, and `reports/manifests/extended_objects_manifest.json`
are reproducibility inputs, not disposable build output. Normal build-retention
pruning must not remove them. Failed `out/<build_id>` and per-build reports use
the standard retention policy after diagnostic evidence has been captured.

## JPL Horizons Raw Responses

Content-addressed directories below `raw/sol_authority/snapshots` and
`raw/sol_artificial/snapshots` contain exact API response bodies, query records,
operator seeds, collector metadata, response indexes, and parsed projections.
They are durable raw science inputs, not cache or disposable build output.
Never prune a snapshot referenced by a source manifest, Evidence Lake raw/typed
snapshot, accepted or served build, report, rollback generation, or
publication. The mutable legacy CSV paths beside them are compatibility
projections and may be replaced atomically by the collector, but their
referenced immutable source snapshot must remain.

The initial combined JPL E4 checkpoint `236a7b7822c52fef8b903d58` and E5
natural Solar System artifact `64e2bc581745f1491217fd7e` are retained
diagnostic history: E5 review proved that their projection parsed only four of
the 12 standard numeric Horizons `ELEMENTS` fields. They are superseded and
must not be selected as accepted scientific evidence, but remain protected
until the E6 comparison and a reviewed retention dry run account for their
reports and references.

The accepted complete-element JPL E4 checkpoint
`b4edc4ea6eccba69794a92df`, natural raw/typed snapshot
`164c147ee3b98ab3dab603bb`, artificial raw/typed snapshot
`32654e1013dae08f24b92cdc`, their exact compiler/source/generic/
complete-element/clean-reproduction reports, and release set
`fde14e4687a853c844b0e341` are protected E4/E5 inputs. The release set pins 38
sources and 36 artifacts totaling 449,199,915,008 bytes. A failed manifestless
v76 staging tree was retired whole only after the earlier v77 checkpoint passed
its then-current gates: reviewed candidate hash
`989a230ebb4219d6decb901f16ac155d6f5051454d6b6f80f35e59c228c6b573`
authorized removal of 1,851,392 allocated bytes. No accepted artifact or source
snapshot was a candidate.

E5 natural Solar System artifact `d61c6890588ee40c46ea7d56`, its policy and
compiler, four ordered Parquet projections, compile and clean-reproduction
reports, and independent artifact audit are protected E5/E6 inputs. Its
canonical CORE/ARM reference and complete-element JPL E4 checkpoint remain protected
transitive inputs. A space cleanup may not remove an individual projection or
the ledger-referenced artifact.

## Gaia Source Evidence Checkpoint

The accepted pre-ARM Gaia source artifact
`derived/evidence_lake_v2/scientific_evidence/ab7f7e6bc211bee146885987`, its
compiler/source/generic-audit reports, and the two referenced Gaia raw and typed
snapshots are protected E4/E5 inputs. It contains all 32,176,271 buffered Gaia
DR3 source rows and is not disposable merely because a later combined shadow
build will consume it. Three tiny manifestless staging trees from interrupted
contract-review attempts may be considered only through an exact-candidate
retention dry run after the clean reproduction gate passes; no accepted source,
artifact, report, served build, or rollback build may enter that candidate set.
The clean reproduction subsequently passed with no differing sections. Exact
candidate-set dry-run/apply hash
`63e4e34a26031102104091eee6cd09e7741cff926fe48fb404d4db3414a048b6`
retired only those three manifestless trees and reclaimed 110,592 allocated
bytes; the accepted Gaia artifact and all referenced inputs remain protected.

## Lifecycle and McGill Evidence Checkpoints

The accepted lifecycle E1 snapshots and E4 artifacts are protected E4/E5
inputs: Exoplanet.eu raw/typed `2c25fa68ee57066e723c9117` /
`4c8e4bbb3563dfe07f7a7f2e`, OEC raw/typed
`6fa12fc3a8296ed95450a935` / `967f39cfa4db28c8e7657e97`, HWC raw/typed
`a5c45a4623051bb163a5794a` / `544dc3d33d7cca5a64726cdf`, and E4 builds
`0a4d68cf938de29a229946a5`, `c2bfe4c2ea04107e81e0de20`, and
`e94a2f86a3410bdf371ef9ef`. Their acquisition, verification, audit, and clean-
reproduction reports are also protected.

McGill raw/typed snapshots `352900b60aa93716b8d75e16` /
`1340b932003999c157d4910b`, E4 build `99c17afd7461a9a6972a9348`, and the pinned
catalog, publisher HTML, CDS ReadMe, and CDS bibliography are protected until
the combined E4 artifact and E5 selection lineage supersede them reproducibly.
Earlier McGill or lifecycle diagnostics are not deletion candidates merely
because a later adapter exists; retire only specifically enumerated,
unreferenced artifacts through the exact-candidate dry-run/apply workflow.

## E4 Scientific Evidence Release Set

Release set `51b08e537e768acf63e554e1` under
`derived/evidence_lake_v2/scientific_evidence_sets/` is the active E5 input. Its
manifest references 36 accepted shard directories totaling 448,814,563,328
database bytes. Every referenced artifact, manifest, database, raw/typed input,
audit, and reproduction report is protected. The set deliberately stores no
copied evidence database; deleting a referenced shard would corrupt the set.
Earlier set `6c19de054e9b807674c37d3c` remains protected through verified E5 v14,
the existing E5 policy-family artifacts, and their E6 rollback lineage. Set
`a188a3adc6207d3a217d54a9` remains protected through the earlier verified E5
rollback artifact.
Retention tooling must resolve active and rollback release-set manifests before
considering any E4 artifact candidate. Superseded diagnostics remain subject to
the existing exact-candidate dry-run/apply rules, never an age-only sweep.

## E5 Selected-Fact Artifacts

`derived/evidence_lake_v2/selected_facts/<build_id>/` contains immutable E5
compiler outputs. Protect the active `current` target, every build referenced
by an E6/E7 report or rollback, its pinned E4 release set and all transitive E4
shards, the E2 identity graph, and the canonical stability reference. A failed
staging directory may be considered only after no compiler process is active;
use an exact candidate dry run before deletion. DuckDB spill under
`/mnt/space/spacegate/e5-selection-spill` is temporary and may be removed only
after the compiler exits and the accepted artifact/report are verified.
The active E5 checkpoint is `0a57f778ce13de1c2c800103`; its compile,
deterministic Parquet exports, manifest, independent audit, clean-reproduction,
timing, and performance-analysis reports are protected. Clean reproduction
matched every logical and per-file Parquet hash, reported no differing
sections, and removed its external scratch tree. The reference compile peaked
at 74,092,281,856 staging and 159,543,382,016 spill allocated bytes;
reproduction peaked at 74,111,700,992 staging and 160,832,151,552 spill.
Unserved host-policy candidate `16708b8ed193aeae9b2ab995` is USB-backed under
`/mnt/space/spacegate/e5-selection-v13/` with a protected symlink in the normal
selected-fact namespace. Its 121,306,839 facts, manifest, compile and phase
timing reports, independent audit, and clean-reproduction report remain
protected as the immediate pre-v14 scientific reference. The reproduction ran
all 103 phases in 24:49.54,
matched logical hash
`d7e38431f403844a4a0736201a61200a2ab95070b9192c0b24be83cfd6f01208`,
and removed its USB scratch tree.
Unserved v14 candidate `929bf92b4c5dbd5aef7e5972` is USB-backed under
`/mnt/space/spacegate/e5-selection-v14/` with a protected symlink in the normal
selected-fact namespace. Its 123,289,311 facts, 43,061,309 decisions, manifest,
compile/timing/performance reports, independent audit, and clean-reproduction
reports are protected E6 inputs. Clean reproduction matches logical hash
`af1155454dc91f8d653735e81ae8c153cdb5c7454e93ea4ab69301ea59d4be1f`
and every compared section, then removes its isolated work tree. Neither v13
nor v14 is the accepted `current` target before E7 promotion.
Unserved v15 candidate `fa4aaed18aebcffb8632d978` is USB-backed under
`/mnt/space/spacegate/e5-selection-v15/` with a protected symlink in the normal
selected-fact namespace. Its 123,288,872 facts, 43,060,870 decisions, manifest,
compile/timing/performance reports, independent audit, and clean-reproduction
reports are protected as the current E6 input. Reproduction matched logical
hash `1b4fd75c00f9a21deb69e0c2136c9c39f7b25bb082b3bd378c260487d417685e`
and every compared section in 28:58.22, then removed scratch. V15 also remains
unserved until E6 scientific A/B and E7 acceptance.
The compiler's immutable-input attestation is process-local and is not a durable
artifact or substitute for a checksum. Each invocation byte-hashes every pinned
E4 input against its expected SHA; within that process only, the result may be
reused while device, inode, size, mtime, and ctime remain unchanged. No retained
metadata cache authorizes deletion, mutation, or checksum bypass. The immediate
verified rollback `f04aa4bc9c86d0c6f97a34da`, its compile, audit, reproduction,
and transitive E4 inputs remain protected. Earlier verified rollback
`d3f255b55e4573676347b206`, its
28,307,894,272-byte DuckDB, deterministic Parquet exports, manifest,
independent audit, and clean-reproduction report remain protected. That
reproduction matched logical hash
`54cc5e9fb95ce52b8743be4336e6c0a6033a0729eb6147550aba3580613655dd`
and removed its external scratch tree. Future E5 runs must retain at least the
new 161.6-GB measured spill margin plus operating headroom. Prior distance build `bfe3e1da9ddc5257f79b6838`
remains a passing historical reference. A current compatibility audit found
that coherent-source build `e8cb1529df6dbcc7c5baadee`, complete monolithic
build `5c84220e408e8fea5f4da218`, and foundation build
`237158e09fce993f1b033414` omitted millions of explicit missing-binding outcomes
required by the accepted accounting contract. Exact acknowledged-report and
candidate-set hash
`dc2adb94f838b1745f6c361b7e7f891893c6ae0a115ea36950444164bde7f6af`
authorized removal of only those three independently rejected artifacts for
112,118,509,568 allocated bytes. Their six small historical reports remain and
their hashes are recorded in the applied report. Diagnostics
`a8a74dbc173b9566fc4d5e5c` (zero Gaia-source coverage) and
`b68c1e6b5649588175854701` (missing required partitions) are not rollback
artifacts. On July 21, 2026, `scripts/audit_selected_fact_artifact.py`
independently rejected both artifacts, and
`scripts/prune_selected_fact_artifacts.py` removed exactly that two-artifact set
through candidate hash
`7e53eabad6412f57b767c20dee777fd6da57f14c5e57728e2114c8019118d17e`.
The applied report records 34,929,528,832 reclaimed allocated bytes. Future E5
cleanup requires the same independent failed-artifact audit, reference/current/
process/link checks, and exact candidate-set dry-run/apply process.

Policy-v11 intermediate `c27804da6fe9e6ada61184b0` failed independent quality-
order audit and was never an accepted rollback. After policy-v12 compile,
lineage-aware audit, and clean reproduction passed, exact candidate-set hash
`85b3c10f7c0853e994d27e9f59ad51762efb2a48a1d8b57ebe82570c7a295279`
authorized removal of that one artifact and reclaimed 74,069,770,240 allocated
bytes. Its failed audit, compile, timing, performance, and retention reports
remain protected diagnostic history.

## E5 Selected-Relation Artifacts

`derived/evidence_lake_v2/selected_relations/<build_id>/` contains immutable E5
relation-evidence projections. Protect the active or ledger-referenced artifact,
its relation policy and compiler hash, E2 identity graph, every referenced E4
source shard, deterministic Parquet files, manifest, independent audit, and
clean-reproduction reports. Never remove the DuckDB inspection database or one
Parquet partition independently. Whole staging or superseded experiment trees
may be removed only after the compiler exits, no config/current/build/report
reference names them, and the comparison or failure report is durable.

Artifact `c59bf6664db0b60960dc36a1` is the protected El-Badry checkpoint. It
passes independent audit and clean reproduction with identical endpoint and
relation-projection Parquet hashes. Two immediately preceding local experiment
trees that lacked final compiler-hash lineage were unreferenced, superseded by
this artifact, and removed as whole units after their reports were retained.

## E5 Selected-Component Artifacts

`derived/evidence_lake_v2/selected_components/<build_id>/` contains immutable
MSC, DEBCat, SB9, ORB6, SBX, and WDS component/relation/context evidence
projections. Protect every artifact
named by the E5 disposition ledger, its component policy and compiler hashes,
the canonical reference build, E2 identity graph, all referenced E4 shards,
ordered Parquet files, manifest, independent audit, and clean-reproduction
report. Never prune a DuckDB inspection database or individual Parquet member
from a retained artifact.

Artifact `67fea5f99500b57419ebdeb0` is the protected current checkpoint. It
passes independent audit and clean reproduction across the combined
seven-source component artifact, including exhaustive SBX, WDS, and Gaia NSS
projections and the case-significant MSC component-identity contract. Artifact
`f5358c0a0983958e5d4f76c5` is its policy-v8 predecessor and remains protected
by the selected-fact-v15 source-disposition ledger. Artifact
`9e59131b92205068f7246a94` is the earlier policy-v7 predecessor and may enter a
future exact-candidate dry-run only after all current config, build, report,
process, and rollback protections pass.
Its predecessors `33f2a90275378a35be21a704`,
`079ac01403b8971e12c99228`, `7ae9b19a56212bfdc4f44d3b`,
`bbc7f0083646dfd5a602467b`,
`6def85dff374034cfe125b6b`, and `1dddf975f24d9bba9590d046` predate the WDS,
SBX, full MSC, or ORB6 projections and are no longer ledger-current. Earlier
artifacts
`58f4c58cf2fff4d18e7a32c4` and
`78b28bd541f82c49cd0ff5b7` are unreferenced experiments: the first lacked the
exact DEBCat source-native quantity-authority keys, while the second preceded a
rerun-report fix. Artifact `434206631ac5f0037d610529` fixed that report path but
did not yet gate the identity-graph diagnostic totals, while
`40f3215ac0fed37a9ece1533` predates the SB9 projection, and
`778c2f2defc91b9e230fd368` predates the required v2 artifact-schema declaration.
They may be considered by a future dry-run only after all current/reference/
process and report protections pass; this documentation is not deletion
authorization.

The policy-v6/compiler-v6 diagnostic staging tree
`.0a50fc1790001c87c8568bae.wczxlv1h` was stopped after scientific
materialization when a pathological integrity anti-join consumed 2 hours 25
minutes without progress. The current pointer remained on verified build
`d3f255b55e4573676347b206`. After the query plan and partial row accounting were
captured, the extended E5 fail-closed retention workflow proved that the
manifestless staging tree and `/mnt/space/spacegate/e5-selection-spill` were
closed, unreferenced, real trees with no symlinks or shared files. Exact
candidate hash
`b714aacda3b912e8eec26a00dc6808d81e2859c085cd3b1ea72d582ea67b6998`
authorized removal of only those two trees and reclaimed 110,415,441,920
allocated bytes. Dry-run and applied reports are retained under
`state/reports/evidence_lake_v2`; accepted selected facts and all E4/raw/typed
inputs were excluded.

An explicit `state/tmp` retention pass separately retired eleven superseded
June/July ARM, hierarchy, WISE rekey, and TESS test workspaces. The fail-closed
tool verified direct-child paths, age, process liveness, hardlinks, and exact
tree identities; internal symlinks were recorded and unlinked without following
their protected targets. Candidate hash
`28163dc9b354d6912b8c68880e9ba8bd67cff1d3f873b5fdc61cfcce9cf3a808`
authorized 57,562,382,336 allocated bytes. Gaia DR2/DR3 identity target files,
active caches, accepted artifacts, served builds, and reports were excluded.
The refreshed storage audit reports 258.7 GiB free. This supports bounded E5
compilation against already pinned sources, but remains below the 300-GiB new-
acquisition gate and 400-GiB post-retention target; do not acquire another large
release until a later reviewed retention pass restores that floor.

The E5 source-disposition ledger and its machine-readable audit report are
protected compiler-policy inputs. Current build `0a57f778ce13de1c2c800103`
hashes the zero-blocker ledger; rollback `f04aa4bc9c86d0c6f97a34da` remains
protected across the current-release cutover.

Focused reports `e5_classification_selection_verification.json` and
`e5_white_dwarf_selection_verification.json` are protected E5 policy evidence.
They verify the v6 policy now materialized by current build
`f04aa4bc9c86d0c6f97a34da` against immutable E4 shards and the identity graph.
Their source E4 builds, identity graph, and reports remain protected compiler
inputs. The reports do not themselves authorize pruning any current or rollback
artifact.

## E6 Shadow Product Checkpoint

Unserved shadow `out/e6_cfcdf2d9add2cd7e2b96af68_shadow`, corrected public
candidate `out/e6_cfcdf2d9add2cd7e2b96af68_public`, their manifests, audits,
reproduction and A/B reports, generated map/simulation artifacts, E6
policy/compiler/auditor, stability reference, and all pinned E5 artifacts
are protected E6 inputs. The shadow occupies approximately 18 GiB and is not a
served/current build.
Unserved compact-identity shadow `out/e6_95e7af54d69f3d9602d81e5b_shadow`
and public candidate `out/e6_95e7af54d69f3d9602d81e5b_public` supersede v6 as
the current E7 review candidate but do not make v6 disposable. Both generations
and their referenced E5 artifacts remain protected through local promotion and
rollback acceptance.
Reproduction scratch is created under `/mnt/space/spacegate` and
removed after the comparison, whether it passes or fails.

Earlier E6 experiments `e6_878b7fc1d46108f4180df8d5_shadow` and
`e6_461e794f1d4afa971cf7b089_shadow` are unserved, superseded diagnostics. The
latter failed independent audit because Boolean variability facts projected as
null numeric values; the former predates that independent gate. They are not
manual deletion targets. Add an E6-aware exact-candidate retention dry-run that
checks manifests, reports, process liveness, links, served/current/rollback
references, and transitive inputs before retiring either whole directory. The
accepted E6 shadow and its source artifacts must never enter that candidate
set.

The July 22 E7 pre-promotion dry-run explicitly accounts superseded v5 shadow
`e6_6e2449e225cd33f9055df6c0_shadow` against verified v7 replacement, exact
tree identity, process/pointer/manifest references, and six acknowledged v5
reports. It reports 18,582,962,176 reclaimable allocated bytes and candidate-set
hash `f76221956608c7a3d701b58e97b2d300f6f52b806b61bdbb8f3068302e67df09`.
No apply was performed. The separate v5 public artifact is not covered by that
report and remains protected pending a public-product-aware retention contract.

Verified foundation `e6_994a6301c335ac385f5dc052_shadow` is now a superseded
historical checkpoint. Candidate `e6_9147013123a439d7e9c2e4a5_shadow` is a
scientific diagnostic only: consumer tables were added after its manifest and
product hashes, so it is ineligible for promotion or reproduction. Together
with the two earlier experiments, superseded E6 directories occupy about 66
GiB on `/data`. Retain them until the E6-specific dry-run accounts for their
reports and exact bytes; then reclaim only the reviewed candidate set using its
hash. Human-readable timing and scientific A/B reports should be retained even
when their reproducible product directory is retired.

The E6-specific retention gate subsequently verified current candidate
`e6_2da376053461c8220bee06ad_shadow` byte-for-byte against its manifest,
independent audit, and clean reproduction, while excluding all served/current/
rollback pointers, live processes, shared files, symlinks, and unacknowledged
references. Exact candidate hash
`e798e3104597e985ae7ae38dd163cadaf0364260e2f4af681d9075943721b674`
authorized removal of only the four listed superseded shadows and reclaimed
68,429,119,488 allocated bytes. Retention audit reports are protected records,
not dependencies on the retired artifacts; the tool tests this distinction to
avoid a dry-run/apply self-reference loop. `/data` fell from 93% to 89% used.

A later exact-candidate run used corrected v6 as the verified replacement and
reclaimed three additional superseded unpromoted shadow directories under set
hash `3e3f6120d03638cea5e58c410f4a8a61890211ad1b195311719ad0eaebb5cb0d`,
recovering 55,580,577,792 allocated bytes. A refreshed July 22 dry-run then
used verified v7 as replacement, acknowledged all seven surviving report
references, and proved the immediately preceding v5 shadow unserved,
unreferenced, process-closed, and rollback-ineligible. Exact candidate hash
`d057da2886af4fbf19aee615d4600328b74793bac22eec2edd0263c2f5f9edf6`
authorized removal of only `e6_6e2449e225cd33f9055df6c0_shadow` and reclaimed
18,582,962,176 allocated bytes. The v5 public artifact, all seven reports,
verified v7, served/current/rollback builds, and transitive evidence remain
protected.

## WISE Image Cache

WISE/IRSA image previews are runtime cache products, not build artifacts and
not repo files.

Default location:

```bash
$SPACEGATE_STATE_DIR/cache/wise_images
```

Default cap:

```bash
SPACEGATE_WISE_IMAGE_CACHE_LIMIT_BYTES=4294967296
```

Optional bulk-storage mode:

```bash
SPACEGATE_WISE_IMAGE_CACHE_PREFER_BULK=1
```

When bulk mode is enabled and `/mnt/space/spacegate` is mounted in the API
container, the default cache root becomes:

```text
/mnt/space/spacegate/cache/wise_images
```

Operators may set `SPACEGATE_WISE_IMAGE_CACHE_DIR` to an explicit path. A larger
cap, such as 20-50 GB, is reasonable on `/mnt/space/spacegate` if the mount is
healthy and the cache is treated as re-fetchable.

The API enforces the cap opportunistically when WISE metadata/previews are
requested. It removes oldest cached files first. Cache metadata must retain IRSA
source URLs, retrieval timestamp, bands, cutout size, and attribution so lost
previews can be regenerated or marked stale.
