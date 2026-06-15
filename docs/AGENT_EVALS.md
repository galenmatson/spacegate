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

Run against the current Photon vLLM endpoint:

```bash
SPACEGATE_LLM_BASE_URL=http://127.0.0.1:8000/v1 \
SPACEGATE_LLM_MODEL=gemma-4-31b-it-qat-w4a16-ct \
scripts/agent_eval.py run --role extract
```

Reports are written under `reports/agent_eval/`, which is treated as
regenerable report state.

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

Anomaly checks compare:

- anomaly type
- severity
- subject
- summary containing the expected cue

This is not a substitute for human or frontier review. It is a reproducible
filter for model/prompt/runtime comparison.

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
