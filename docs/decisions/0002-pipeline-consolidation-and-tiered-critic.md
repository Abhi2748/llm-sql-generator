# ADR 0002 — Consolidate the Agent Pipeline: Deterministic Planning, Tiered Critic, Cached Schema Indexing

**Status:** Accepted
**Date:** 2026-07-16

## Context

ADR 0001 keeps the deterministic-compiler-over-LLM-IR architecture. Given that, the
question is whether the *scaffolding* around it — currently 4 sequential LLM calls
per request minimum (`schema_summarizer` → `intent_agent` → `plan_agent` →
`critic_agent`), up to 6+ with a retry (`repair_agent` → recompile → `critic_agent`
again) — is doing proportionate work for its cost and latency, or whether some of it
is spending an LLM call on something deterministic code could do for free.

## Decision

Three changes to the pipeline:

1. **Remove `plan_agent` as an LLM call.** Its job — producing 2-3 candidate
   row-grains (doc-per-row / event-per-row / item-per-row) — is derivable
   deterministically from `schema_index["arrays"]` (which arrays exist) and the
   paths already selected in `query_spec` (which arrays those paths pass through).
   This is a graph lookup, not a reasoning task. Replace with a pure function.

2. **Make `critic_agent` conditional, not unconditional.** Only invoke it when
   `static_validate` actually flags something (unknown paths, missing FLATTEN,
   score below a threshold). When static validation already confirms a clean
   candidate, an LLM call to re-derive that confirmation is spending money on
   something deterministic code already answered.

3. **Separate schema indexing from query answering, and cache the former.**
   `schema_index` (deterministic) and `schema_summary` (1 LLM call) depend only on
   the *shape* of the JSON sample, not on the question being asked. Re-running both
   for every question against the same table is redundant. Cache them keyed by a
   hash of the schema's shape (sorted list of field paths + inferred types from
   `schema_index`, not raw JSON values, so unrelated data changes don't invalidate
   the cache).

## Reasoning — the numbers

Using this project's actual default model (`gpt-4o-mini`, per
`workflow/llm.py:default_llm_config`) and estimated per-call token counts:

| | LLM calls | Est. cost/query | Est. cost @ 100K queries/mo |
|---|---|---|---|
| Current, no retry | 4 | $0.0011 | $114 |
| Current, 1 retry | 6 | $0.0018 | $182 |
| Redesigned, best case (schema cached, validation clean) | 1 | $0.00035 | $35 |
| Redesigned, worst case (schema not cached, 1 retry) | 4 | $0.0014 | $141 |

Best case is a ~69% cost reduction. The more operationally important number is
sequential LLM round-trips: 1 instead of 4 in the common case, which is a direct
latency win for whoever's waiting on the response — the dollar figure is the easier
number to say out loud, but latency is what a user actually feels.

These are estimates from approximate token counts, not measurements. Real numbers
require instrumenting token usage per node (see Consequences below) — the estimate
is directionally trustworthy (4-6 calls vs 1-4 calls is not sensitive to the exact
token assumptions) but should be replaced with measured data once available.

## Alternatives considered

- **Model tiering** (cheap/fast model for schema summarization + intent extraction,
  stronger model reserved for the critic) — a real further cost lever, deferred
  rather than bundled into this change, to keep this ADR's before/after comparison
  isolated to the pipeline-shape change alone.
- **Retrieval-augmented few-shot prompting** (retrieve similar verified past
  questions from the golden set as in-context examples for the intent call) — a
  real accuracy lever used by production text-to-SQL systems, deferred as a
  separate, larger scope item. Noted here so it isn't lost, not because it was
  rejected on merits.
- **Fine-tuning a smaller model** on QuerySpec generation — rejected. The golden
  set (36 examples) is a test set, not remotely enough training data, and
  retrieval-augmented prompting captures most of the same benefit for a fraction
  of the engineering cost.
- **In-memory schema cache vs. a real cache service (Redis/Memcached).** For this
  project's scale (portfolio demo, single Cloud Run instance), an in-memory dict
  keyed by schema-shape hash is sufficient and avoids adding infrastructure.
  Explicitly noted as a scope decision: a multi-instance production deployment
  would need a shared cache (Redis) since in-memory state doesn't survive across
  Cloud Run instances/cold starts. Documented as a known scaling limitation, not
  an oversight.

## Consequences

- `workflow/nodes/plan_agent.py` (LLM-backed) is replaced by a deterministic
  function; the LLM-backed version and its prompt (`prompts/plan_agent.md`) are
  removed, not just bypassed, to avoid dead code.
- `graph.py`'s conditional edges gain a new branch after `static_validate`: route
  to `critic_agent` only if validation issues exist or score is below threshold,
  otherwise route directly to finalize.
- A new cache layer needs a place to live — a module-level dict is fine for a
  single-process demo; the interface should be a small abstraction
  (`get_cached_schema(shape_hash)` / `set_cached_schema(...)`) so swapping the
  backing store later doesn't touch calling code.
- This ADR's cost table should be replaced with real measured numbers once
  token-usage logging is added (see: add per-node token counting to `llm.py`'s
  `LLM.invoke` wrapper, log to stdout or a simple counter for now).
- Directly enables the Phase 2/3 baseline comparison to also report cost/latency,
  not just structural correctness — "faster and cheaper AND more correct" is a
  stronger comparison than correctness alone.
