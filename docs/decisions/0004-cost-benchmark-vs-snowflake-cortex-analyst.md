# ADR 0004 — Cost Benchmarked Against Snowflake's Native Equivalent (Cortex Analyst)

**Status:** Accepted
**Date:** 2026-07-17
**Pricing reference date:** August 2025 (matches this project's original build window;
see Consequences for why this date matters and how to update it)

## Context

ADR 0002 estimated this pipeline's per-query cost against itself (before/after
consolidation). That's useful for showing the redesign worked, but it doesn't
answer the more interesting outside question: how does this compare to what a
company would otherwise pay for the same capability, out of the box, from
Snowflake itself?

Snowflake's native answer to "natural language question in, SQL out" is
**Cortex Analyst** — a fully managed text-to-SQL service, GA since 2024,
usable via Snowflake Intelligence or a standalone REST API.

## Findings

As of August 2025, Cortex Analyst billed **per message** (per natural-language
question), not per token:

- 6.7 Snowflake Credits per 100 messages = 0.067 credits/query
- Snowflake Credit price depended on edition: $2 (Standard) / $3 (Enterprise) /
  $4 (Business Critical) / $6 (VPS)
- Cost per query: **$0.134 (Standard) to $0.40 (VPS)**, with $0.20/query
  (Enterprise, $3/credit) as the most commonly cited reference point
- This price covers only SQL *generation* — executing the resulting SQL is a
  separate, additional virtual-warehouse compute charge, billed the same way
  regardless of which system generated the SQL

This project's pipeline, per ADR 0002's estimates (`gpt-4o-mini`, same generation-only
scope — this project also doesn't execute the SQL it produces):

- Best case (schema cached, clean validation): **~$0.00035/query**
- Worst case (cold schema, one retry): **~$0.0014/query**

**Result: roughly 100–570x cheaper than Snowflake's own native offering**,
depending on which edition and which case is compared.

## Decision

Report this comparison as a legitimate, sourced data point (not an unqualified
"we're better than Snowflake" claim — see Consequences for the caveats that
belong alongside it every time this number is cited).

## Reasoning

- Same scope on both sides (generation only, not execution) — not an
  apples-to-oranges comparison inflated by excluding a cost on one side.
- Sourced from Snowflake's own documented pricing model at the time, not
  estimated or guessed.
- Directly reinforces ADR 0001's thesis with an external reference point: the
  cost advantage of this architecture isn't just "cheaper than a naive
  single-shot baseline we built ourselves" (which could be dismissed as a
  strawman) — it's cheaper than what the platform vendor itself charges for a
  comparable managed capability.

## Consequences — required caveats whenever this number is used

- **Not a capability-equivalent comparison.** Cortex Analyst is a fully managed
  product: semantic-model UI, multi-turn conversation, enterprise support,
  native platform integration. This project is a portfolio-scale system that
  now also supports multi-turn correction (see the chat-correction feature,
  `docs/FINDINGS.md`), but doesn't claim feature parity with a shipped
  enterprise product. The claim is specifically: *the core SQL-generation step
  is dramatically cheaper to build this way.*
- **Pricing is dated on purpose.** Snowflake overhauled its AI pricing in
  April 2026, introducing a separate flat-rate "AI Credits" meter
  ($2.00 global / $2.20 regional) decoupled from edition pricing. This ADR
  intentionally benchmarks against the pricing model in effect during this
  project's original build window (August 2025), not current pricing. Anyone
  citing this comparison today should either re-verify current Cortex Analyst
  rates or state the August 2025 anchor date explicitly, the way this ADR does.
- If this project's own model or architecture changes materially, this ADR's
  cost side of the comparison should be re-estimated — it's derived from
  ADR 0002's numbers, which are themselves labeled estimates, not measurements.
