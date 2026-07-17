# JSON → Snowflake SQL

Turn a natural-language question and a sample of JSON stored in a Snowflake
VARIANT column into ranked, validated SQL — using a deterministic compiler over
an LLM-populated intermediate representation, not an LLM freehanding SQL text.

**[Live demo →](https://llm-sql-generator.vercel.app/)** · **[Architecture decisions →](docs/decisions/)** · **[Full findings →](docs/FINDINGS.md)**

## Why this exists

Most "AI SQL generator" tools ask a model to write SQL directly. That works for
flat tables and falls apart on the case that actually matters in production:
deeply nested JSON, arrays inside arrays, and sparse fields that only show up on
some rows. This project takes a different approach — an LLM extracts *intent*
into a small structured spec; deterministic code compiles that spec into
syntactically correct Snowflake SQL (`:` traversal, `::` casts, chained
`LATERAL FLATTEN` for nested arrays). Every candidate ships with a validation
score and a named list of what's wrong, checked against the real schema — not a
black box.

## What it does

- Infers a schema from a small JSON sample — the same sample size the UI itself
  asks for, not an idealized large export
- Generates 2–3 ranked SQL candidates per question, each scored by a
  deterministic validator (real path checks, `LATERAL FLATTEN` coverage,
  `GROUP BY`/aggregation shape checks — not string heuristics)
- Escalates to an LLM critic only when deterministic validation actually finds
  something wrong — measured at **1.52 mean LLM calls per query**, against
  2.0 for a single-shot-plus-self-critique baseline on the same model
- Supports conversational correction — tell it what's wrong in plain English
  ("that's the wrong field, I want the tracking carrier") and it corrects the
  same query instead of starting over
- Costs an estimated **100–570x less** per query than Snowflake's own native
  text-to-SQL offering (Cortex Analyst), for the same scope of work — see
  [ADR 0004](docs/decisions/0004-cost-benchmark-vs-snowflake-cortex-analyst.md)

## Results

Measured against a hand-verified golden set of **38 questions** (29 scored
structurally, 9 adversarial/ambiguous/known-limitation cases tested but not
numerically scored) across 3 real JSON schemas — a synthetic e-commerce sample,
real GitHub Events API data, and real GA4 BigQuery public sample data — not
curated to flatter the system:

| | Single-shot baseline | Baseline + self-critique | This pipeline |
|---|---|---|---|
| Structural correctness (approx.) | 93.7% | 95.1% | **97.0%** |
| Mean LLM calls / query | 1.0 | 2.0 | **1.52** |

**12 real bugs** were found and permanently fixed through live testing that a
fully green unit-test suite did not catch — including a nested-array SQL
compilation bug, a silent `GROUP BY` data-loss bug, a fan-out/double-counting
aggregation bug, and a retry-exhaustion failure that reported success when it
hadn't actually resolved anything. Every one has a permanent regression test.
Full writeup, including the ones that took multiple rounds to root-cause
properly: [`docs/FINDINGS.md`](docs/FINDINGS.md).

**Known limitations**, found and documented rather than hidden: cross-field
arithmetic in aggregations, post-aggregation (`HAVING`-style) filtering, and
key/value pivot-array patterns (common in GA4/Segment-style event schemas) are
not yet supported by the query spec — scoped as future work, not silently
unsupported.

## Architecture

Seven pipeline stages, two kinds of trust: some stages reason about intent (LLM
calls), everything downstream of intent is deterministic, independently
unit-tested code.

```
schema_index → summarizer → intent → plan → compile → validate → [critic]
   (det.)        (LLM)       (LLM)   (det.)   (det.)     (det.)   (conditional)
```

Full reasoning for every major architectural decision — including alternatives
considered and why they were rejected — is in [`docs/decisions/`](docs/decisions/):

- [0001](docs/decisions/0001-deterministic-compiler-vs-llm-sql.md) — deterministic compiler vs. LLM-generated SQL
- [0002](docs/decisions/0002-pipeline-consolidation-and-tiered-critic.md) — pipeline consolidation for cost/latency
- [0003](docs/decisions/0003-deterministic-repair-patch-application.md) — deterministic repair, not a second LLM call
- [0004](docs/decisions/0004-cost-benchmark-vs-snowflake-cortex-analyst.md) — cost vs. Snowflake's native equivalent

## Stack

Python · LangGraph · FastAPI · gpt-4o-mini · Snowflake VARIANT/`LATERAL FLATTEN` ·
vanilla HTML/CSS/JS frontend · pytest

Deployed on Google Cloud Run (backend) and Vercel (frontend).

## Running it locally

```bash
pip install -r requirements.txt
cp .env.example .env   # add your OPENAI_API_KEY

# backend
uvicorn api.index:app --reload --port 8000

# frontend (separate terminal)
cd public && python -m http.server 5500
```

Open `http://localhost:5500/console.html`.

## Tests

```bash
pytest tests/
```

**61 tests, all deterministic/fake-LLM (no API key required, no network calls),
runtime under 1 second.**
