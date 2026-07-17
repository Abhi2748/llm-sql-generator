# ADR 0003 — Repair Is Deterministic Patch Application, Not a Second LLM Reconstruction

**Status:** Accepted
**Date:** 2026-07-16

## Context

`repair_agent_node` originally made its own LLM call: given the critic's issues,
ask a model to produce a *complete* replacement `query_spec`, then overwrite
`state["query_spec"]` with whatever came back.

This caused three separate, independently-discovered live bugs in one session,
each looking different on the surface:
1. A repair silently dropped `order_by`/`limit` from a `query_spec` because the
   repair LLM's freshly-generated output didn't include them (they weren't part
   of what it was asked to fix).
2. A repair "fixed" a stray-column issue by folding a high-cardinality field into
   `GROUP BY`, fragmenting aggregation grain — again, a side effect of asking an
   LLM to regenerate a whole spec rather than apply a targeted change.
3. Confirmed via a from-scratch repro (`tests/test_repair_patch_application_repro.py`,
   built specifically to answer "is this a shallow-merge bug or a full-replace
   bug"): `repair_agent_node` did `state["query_spec"] = obj["query_spec"]` — a
   full replace, not a merge. Any field the critic's patch didn't explicitly
   mention was lost.

## Decision

`repair_agent` makes no LLM call. The critic already returns a structured,
targeted `query_spec_patch` / `plan_patch` — that patch *is* the repair,
expressed precisely. Apply it deterministically:

```python
def apply_repair_patch(query_spec, plan, critic_repairs):
    new_query_spec = {**query_spec, **(critic_repairs.get("query_spec_patch") or {})}
    new_plan = {**plan, **(critic_repairs.get("plan_patch") or {})}
    return new_query_spec, new_plan
```

Patch keys overwrite; everything the patch doesn't mention survives unchanged.

## Reasoning

- Removes an entire class of bug at the source rather than patching each
  symptom. Three different-looking bugs across this session shared one root
  cause; fixing the root cause is stronger than three targeted patches would
  have been (and the three-round attempt at exactly that, before this decision,
  is documented in `docs/FINDINGS.md` §3.2 and §3.4 as a cautionary example).
- Removes an LLM call from the worst-case pipeline path, directly extending
  ADR 0002's cost thesis: retry-path worst case drops from 4 calls to 3.
- Makes repair testable without an LLM at all — `apply_repair_patch` is a pure
  function; `tests/test_repair_patch_application_repro.py` verifies the merge
  semantics deterministically.
- This same pattern was later reused for chat-based corrections (a human
  supplies the "patch" via free text instead of the critic; one small LLM call
  translates the free text into the identical `query_spec_patch` shape, then
  the same `apply_repair_patch` function runs) — validating that the patch
  abstraction generalizes beyond its original use case.

## Alternatives considered

- **Keep the LLM reconstruction call, but instruct it more carefully to preserve
  unmentioned fields.** Rejected — this is a prompt-engineering patch on top of
  an architecturally fragile pattern (an LLM regenerating state it wasn't asked
  to change), and prompt instructions are not a reliable substitute for a
  correctness property that should hold by construction.
- **Shallow-merge the LLM's full reconstructed output onto the previous spec**
  (`{**before, **llm_output}`). Rejected — this only works if the LLM's
  reconstruction is itself correct for the fields it *does* return; a targeted
  patch from the critic is a narrower, more reliable contract than "regenerate
  everything and hope the unchanged parts match."

## Consequences

- `prompts/repair_agent.md` is deleted, not just unused — no dead prompt for a
  call that no longer happens.
- The critic's `query_spec_patch`/`plan_patch` output shape is now a
  load-bearing contract used by two callers (the automated repair loop and
  chat corrections), not just internal detail of one node. Changes to that
  shape need to consider both call sites.
- ADR 0002's worst-case LLM call-count table should be read as superseded by
  this ADR for the repair step specifically (3 calls worst-case, not 4).
