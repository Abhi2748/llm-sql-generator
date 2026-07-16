# ADR 0001 — Use a Deterministic SQL Compiler Over a Structured IR, Not LLM-Generated SQL Text

**Status:** Accepted
**Date:** 2026-07-16

## Context

The core task: convert a natural-language question, plus a sample of JSON stored in
a Snowflake VARIANT column, into correct SQL (using `:` traversal, `::` casts, and
`LATERAL FLATTEN` for nested arrays). Two architectural families were on the table:

1. **LLM writes SQL text directly.** One prompt: schema + question in, SQL string out.
2. **LLM populates a structured intermediate representation (QuerySpec); deterministic
   code compiles that IR into SQL.** This is what the project already had going in
   (`workflow/nodes/sql_compiler.py`), and the question was whether to keep it or
   simplify toward (1).

## Decision

Keep architecture (2). Do not let the LLM freehand SQL text.

## Reasoning

This wasn't a theoretical call — it's grounded in bugs actually found and fixed in
this codebase (see Phase 0 fixes, tests in `tests/test_sql_compiler.py`):

- **The nested-array alias bug** (`expr_for_path` resolving the wrong `LATERAL FLATTEN`
  alias 2+ levels deep) was a deterministic string-matching bug: reproducible, unit-
  testable, fixed once, fixed forever, verified by a permanent regression test.
  The equivalent mistake in freehand LLM-generated SQL wouldn't be a bug to fix — it'd
  be a *failure mode* that resurfaces unpredictably on some fraction of nested-array
  questions, with no way to write a regression test against natural-language phrasing
  variation.
- **The `= NULL` vs `IS NULL` issue** and the **string-encoded-timestamp cast issue**
  (both documented in `eval/golden/*.json` as known limitations) are exactly the class
  of syntactically-fiddly, easy-to-subtly-botch detail where "the LLM figures out
  correct Snowflake VARIANT syntax from scratch, every time, per request" is a strictly
  worse bet than "the LLM extracts intent into a small structured spec; code compiles
  syntax deterministically, and the syntax rules only need to be gotten right once."

More generally: semantic understanding (turning "top 5 categories by revenue" into
*which fields, which grain, which aggregation*) is a fuzzy task suited to an LLM.
Syntax generation (correctly nesting `LATERAL FLATTEN` CTEs, picking the right cast,
applying `IS NULL` instead of `= NULL`) is a rigid, rule-based task suited to code.
Conflating both in a single LLM call makes both harder: you can't unit-test "did the
LLM understand the question" and "is the SQL syntactically correct" independently,
and a fix for one class of syntax mistake doesn't generalize the way a code fix does.

## Alternatives considered

- **LLM writes SQL text directly, with a SQL linter/formatter as a safety net.**
  Rejected — a linter can catch malformed syntax, but not semantically-wrong-but-
  syntactically-valid SQL (wrong alias, wrong grain, missing FLATTEN), which is
  precisely the failure class this project's bugs fell into.
- **LLM writes SQL, validated only by actually executing it against a real warehouse.**
  Rejected as the sole safety net — expensive (real Snowflake compute per attempt),
  slow (feedback loop through a live warehouse), and still non-deterministic across
  retries for the same underlying mistake.

## Consequences

- The deterministic compiler (`sql_compiler.py`) remains the single source of truth
  for SQL syntax correctness, and stays independently unit-testable from the LLM
  agents (`tests/test_sql_compiler.py` requires no LLM to run).
- The QuerySpec/plan IR becomes the actual "product surface" the LLM needs to get
  right — which is a narrower, more checkable target than SQL text.
- This decision is empirically testable, not just asserted: the baseline model
  (Phase 2 — single-shot LLM-writes-SQL-directly) exists specifically to produce
  the comparison data for this claim. See `eval/` results once Phase 2/3 land;
  this ADR should be revisited with those numbers rather than left as an unverified
  assertion.
