# Spacegate Agent Evaluation Harness

This document defines the first repeatable evaluation surface for local-model
agent work. The harness exists to choose models per pipeline role, not to bless
one universal "best" model.

## Purpose

Use fixed golden cases to evaluate whether a model can:

- extract cited astronomical claims without inventing facts
- preserve units, limits, uncertainty semantics, and source-native qualifiers
- bind claims to the right system, star, planet, subsystem, or orbit relation
- find contradictions without averaging them away
- quarantine surprising findings in an anomaly inbox
- emit parseable structured output with model/runtime provenance

Eval runs are experiments. They must not mutate production `core`, `arm`, or
`disc` artifacts.

## Pipeline Roles

The same model should not be assumed optimal for every stage.

| Role | What It Tests | Preferred Model Shape |
| --- | --- | --- |
| `discover` | Find promising sources, contradictions, and leads | high recall, cheap, broad context |
| `prune` | Reject weak/off-policy/noisy sources | fast, conservative, low hallucination |
| `compile` | Build clean multi-source dossiers | long context, strong summarization |
| `identify` | Resolve object, host, component, and relation binding | precise, conservative, abstains well |
| `extract` | Emit narrow typed claims from source text | schema-following, low temperature |
| `criticize` | Attack extraction/proposal failures | adversarial, ideally different model family |
| `adjudicate` | Final claim-level accept/reject/defer packets | strongest local model first |
| `narrate` | Explain accepted evidence for readers | clear prose, citation discipline |

The slow "inquisitor" model belongs only in the hard-tail `adjudicate` path:
after deterministic checks and faster local models disagree, abstain, or flag a
high-impact case.

## Golden Cases

Tracked cases live under:

```bash
evals/spacegate_agent/cases/
```

The seed suite includes:

- exoplanet traps: TOI-2431 b, TOI-1080 b, TOI-7166 b, Barnard's Star planets,
  TRAPPIST-1, candidate/non-detection cases, and a synthetic Kepler/HZ case
- ambiguity and restraint traps: Kepler-51d, WASP-107 b, 55 Cancri e
- identity and hierarchy traps: Castor, 16 Cyg, Alpha Centauri, Sirius, Sol
- contradiction/anomaly traps: MWC 656, conflicting stellar temperatures,
  schema gaps, stale consensus, Roche/plausibility failure, and a controlled
  large-context synthesis case

Golden cases are compact fixtures, not production source documents. They are
designed to test model behavior before wiring broader retrieval or PDF parsing.

## Anomaly Inbox

The anomaly inbox is the quarantine surface for things the pipeline notices
while doing other work. An anomaly is not an accepted fact.

Initial anomaly types:

- `catalog_conflict`
- `source_conflict`
- `identity_or_host_ambiguity`
- `schema_gap`
- `stale_consensus`
- `derived_plausibility_failure`
- `interesting_hypothesis`
- `needs_human_review`
- `unsupported_claim`
- `multi_model_measurement`
- `observational_limitation`

Every anomaly emitted by the harness is written with `status="quarantined"` in
the report summary. Future production integration should persist the same
concept in reviewed `disc`/`arm` surfaces, never directly in `core`.

## Running

Validate fixtures:

```bash
scripts/agent_eval.py validate
```

List cases:

```bash
scripts/agent_eval.py list
```

Run an oracle smoke test without calling a model:

```bash
scripts/agent_eval.py run --oracle
```

Oracle mode copies each case's golden expected output into the scorer instead
of calling a model. It should score `1.0`; if it does not, the fixture or
scoring logic is broken independently of model quality.

Rescore existing JSON reports after fixture or scorer changes without spending
tokens:

```bash
scripts/agent_eval.py rescore reports/agent_eval/agent_eval_*.json --details
```

Rescoring also re-parses saved raw model content when the original run had a
recoverable JSON framing issue. It does not repair genuinely truncated output.

Run against the current Photon vLLM endpoint:

```bash
scripts/agent_eval.py run \
  --provider local \
  --model gemma-4-31b-it-qat-w4a16-ct \
  --role extract
```

Run against the configured default frontier provider:

```bash
scripts/agent_eval.py run --provider frontier --case-id toi_1080_upper_limit
```

Run against OpenAI explicitly:

```bash
scripts/agent_eval.py run \
  --provider openai \
  --model "$SPACEGATE_FRONTIER_OPENAI_MODEL" \
  --case-id toi_1080_upper_limit
```

Run against Google Gemini explicitly:

```bash
scripts/agent_eval.py run \
  --provider google \
  --model "$SPACEGATE_FRONTIER_GOOGLE_MODEL" \
  --case-id toi_1080_upper_limit
```

When `/etc/spacegate/spacegate.env` exists, the harness loads it by default for
missing environment variables. Use `--env-file /path/to/file` to override that
behavior. API keys are used only for authentication and are not written to eval
reports.

Reports are written under `reports/agent_eval/`, which is treated as
regenerable report state.

## Frontier Secret File

Photon frontier credentials should live outside git:

```bash
/etc/spacegate/spacegate.env
```

Expected keys:

```bash
SPACEGATE_OPENAI_API_KEY=...
SPACEGATE_GOOGLE_API_KEY=...
SPACEGATE_FRONTIER_OPENAI_MODEL=gpt-5.5
SPACEGATE_FRONTIER_GOOGLE_MODEL=gemini-pro-latest
SPACEGATE_FRONTIER_DEFAULT_PROVIDER=openai
```

Legacy `OPENAI_API_KEY` and `GOOGLE_API_KEY` aliases may still be recognized by
some tools, but new Spacegate host config should prefer the `SPACEGATE_*`
names so Runtime diagnostics can distinguish project-scoped secrets from a
generic shell environment.

Recommended ownership is `root:spacegate` with `0640` permissions. The account
or systemd service that runs the agent pipeline should be in the `spacegate`
group, or the service should use `Group=spacegate`.

## Scoring

The v1 scorer is deterministic and intentionally simple:

- 75% claim score
- 20% anomaly score
- 5% schema validity
- small penalty for extra claims beyond the expected target set

Claim checks compare:

- subject
- predicate
- value with optional numeric tolerance
- unit
- qualifier
- status

Expected rows may define `subject_aliases` when a compact fixture has several
acceptable ways to name the same synthetic or source-described object. Numeric
checks accept numeric strings with uncertainty text, such as `5350 +/- 40`, when
the parsed numeric value falls within tolerance. Duplicate same-subject
predicates are matched by best value/qualifier/status/unit fit instead of by
first occurrence. Fixtures may also define field-specific aliases such as
`value_aliases`, `qualifier_aliases`, `status_aliases`, or `unit_aliases` for
local semantic equivalents.

Anomaly checks compare:

- anomaly type
- severity
- subject
- summary containing the expected cue

Expected anomalies may define `anomaly_type_aliases` for fixture-local
equivalent labels where the pipeline distinction is not under test. They may
also use field-specific aliases such as `severity_aliases` when that distinction
is not the target of the case.

This is not a substitute for human or frontier review. It is a reproducible
filter for model/prompt/runtime comparison.

Prompt version `agent_eval_v2` explicitly permits deterministic derived claims
when a case task asks the model to evaluate, calculate, infer, or adjudicate a
quantity and the excerpt provides enough inputs. The model must still mark the
claim as `derived` or `inferred_*` and must not treat the derived value as a
source-native measurement.

## Output Contract

Model outputs should be JSON objects with:

```json
{
  "claims": [
    {
      "claim_id": "short_stable_id",
      "subject": "object or relation name",
      "predicate": "claim family and field",
      "value": "string, number, boolean, or null",
      "unit": "unit string or null",
      "qualifier": "measured | upper_limit_3sigma | m_sin_i | schema_gap | ...",
      "status": "accepted | rejected | deferred",
      "supporting_citation_ids": ["citation_id"],
      "reasoning_summary": "brief evidence-grounded rationale"
    }
  ],
  "anomalies": [
    {
      "anomaly_type": "source_conflict",
      "severity": "low | medium | high",
      "subject": "object or relation name",
      "summary": "short quarantined finding",
      "recommended_next_action": "review | deterministic_check | source_followup | frontier_escalation | discard"
    }
  ],
  "verdict": {
    "case_status": "pass | partial | fail | abstain",
    "summary": "brief claim-level outcome"
  }
}
```

Schema-constrained decoding should be added later, but the prompt still states
the schema because structured-output engine field descriptions are not model
instructions.
