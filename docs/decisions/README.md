# Architecture Decision Records

Each file here documents one architectural decision: the context, the decision, the
alternatives considered and why they were rejected, and (once available) the actual
measured result — not just the projection made at decision time.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-deterministic-compiler-vs-llm-sql.md) | Deterministic SQL compiler over a structured IR, not LLM-generated SQL text | Accepted |
| [0002](0002-pipeline-consolidation-and-tiered-critic.md) | Consolidate the agent pipeline: deterministic planning, tiered critic, cached schema indexing | Accepted |
| [0003](0003-deterministic-repair-patch-application.md) | Repair is deterministic patch application, not a second LLM reconstruction | Accepted |
| [0004](0004-cost-benchmark-vs-snowflake-cortex-analyst.md) | Cost benchmarked against Snowflake's native equivalent (Cortex Analyst) | Accepted |

## Format

- **Status** — Proposed / Accepted / Superseded (by which ADR)
- **Context** — what problem forced this decision
- **Decision** — the call that was made, stated plainly
- **Reasoning** — why, grounded in evidence from this project where possible
  (a bug that was found, a benchmark that was run) rather than general theory
- **Alternatives considered** — what else was on the table and why it lost
- **Consequences** — what this decision commits us to, including honest scope
  limitations (e.g. "in-memory cache is fine at this scale, would need Redis
  at production scale — documented as a known limitation, not an oversight")

New ADRs get the next sequential number. Superseding an old decision means adding
a new ADR and marking the old one's status as Superseded — never silently editing
or deleting a past ADR, since the record of "we used to think X, here's why we
changed our mind" is itself valuable.
