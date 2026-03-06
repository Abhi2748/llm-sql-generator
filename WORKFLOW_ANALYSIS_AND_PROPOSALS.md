# Workflow Analysis & Improvement Proposals

## 1. What Was Tested

- **Schema-only test** (no API): `python test_schema_only.py` — runs successfully and shows exactly what context is sent to the LLM.
- **Full workflow test** (with OpenAI): `python test_workflow.py` — requires `OPENAI_API_KEY` in `.env` and `pip install -r requirements.txt`.

You reported that the app **did not generate proper answers**. Below is why that likely happens and proposed fixes.

---

## 2. Issues Identified

### 2.1 Context sent to the LLM is limiting

- **Only 15 JSON paths** are included in the prompt, while the sample has **101 paths**. Important paths (e.g. `transaction:total_amount`, `transaction:items[*]:price`) may never be seen by the model.
- Paths are **indexed with `[0]`** (e.g. `ecommerce_events[0]:user:email`). The LLM may:
  - Generate SQL that only addresses the first element.
  - Be unclear whether the table has **one row = whole document** or **one row = one event** (after flattening).
- **No explicit data model** in the prompt: “This VARIANT column holds one document per row” vs “one event/element per row” is not stated.

### 2.2 LLM output parsing is fragile

- The code expects **raw JSON** from the LLM. Many models return markdown-wrapped JSON, e.g.:
  ```text
  ```json
  { "sql": "...", "explanation": "..." }
  ```
  ```
- `json.loads(response_content)` then fails and the fallback puts the **entire response** (including markdown) into `result["sql"]`, so the UI shows invalid “SQL”.

### 2.3 Validation is too weak

- **Only hard failure**: “No SELECT statement found”. Everything else is at most a **warning**.
- So incorrect but “SELECT-y” SQL (wrong paths, wrong structure, no FLATTEN where needed) is still marked **valid** and no retry is triggered.
- There is **no check** that the JSON paths used in the SQL exist in the schema context.

### 2.4 Retry logic doesn’t target quality

- Retries only happen when `is_valid` is False (e.g. no SELECT).
- Bad semantics (wrong path, wrong aggregation) don’t trigger a retry, so the first poor answer is often the final one.

### 2.5 Prompt/context format

- Path list uses `[0]`-style paths; the prompt also mentions “level1.level2” in one place. Mixing **dot** and **colon** can confuse the model; Snowflake uses **colon** for path segments.

---

## 3. Proposed Workflow Improvements

Below are **three options** (from minimal to larger change). We can pick one and refine it.

---

### Option A — Quick fixes (minimal change)

Goal: better answers with minimal architectural change.

1. **Richer context for the LLM**
   - Increase paths shown (e.g. 30–40) or **prioritize** paths: queryable fields first, then paths that contain key nouns from the user query (e.g. “amount”, “transaction”, “items”).
   - Add one line to the prompt: **data model** (e.g. “Assume one row per document; the VARIANT column holds the full JSON document including the root key.”).

2. **Robust JSON extraction from LLM response**
   - Before `json.loads(response_content)`:
     - Strip markdown code blocks (e.g. match `` ```json ... ``` `` or `` ``` ... ``` `` and use the inner content).
     - Optionally try to find a `{ ... }` substring if the whole string isn’t valid JSON.

3. **Stricter validation (optional retry trigger)**
   - If there are **any** warnings (e.g. “No type casting (::) detected”), set `is_valid = False` so the graph retries once with a prompt that says “Add proper :: type casting.”
   - Or: add a simple heuristic (e.g. “SQL must contain both `:` and `::`”) and treat violation as invalid.

4. **Prompt cleanup**
   - Use **colon** consistently for Snowflake paths; remove or replace “level1.level2” with a colon example.

**Pros:** Small code changes, no new dependencies.  
**Cons:** Doesn’t fix fundamental “wrong path” or “wrong shape” issues; retries still not semantics-driven.

---

### Option B — Two-phase generation + validation (recommended)

Goal: better SQL and fewer “silly” answers by separating structure from generation and validating against the schema.

1. **Phase 1 — Query understanding**
   - Small LLM call (or rules): from the **user question** + **list of queryable fields/paths**, output:
     - **Intent**: e.g. “list events with user email and transaction total”
     - **Required paths**: e.g. `["user:email", "transaction:total_amount"]`
     - **Need FLATTEN?**: e.g. “yes, over transaction:items”
   - This can be a short structured prompt with a small model or the same model with a strict JSON schema.

2. **Phase 2 — SQL generation**
   - Pass to the main SQL prompt:
     - The **intent** and **required paths** (and whether to flatten).
     - A **curated path list** (e.g. only paths that appear in “required paths” or in the top N queryable fields), plus a clear **data model** line.
   - Ask for **only** Snowflake SQL (no explanation in the same call), or keep JSON with `sql` + `explanation` but with strict format instructions and **robust extraction** (Option A).

3. **Validation**
   - **Schema-aware**: Check that every path referenced in the SQL (e.g. after `raw_data:` or `f.value:`) is in the schema agent’s path list (or a normalized form); if not, set `is_valid = False` and optionally add the missing path to the retry prompt.
   - Keep existing checks (SELECT, `:`, `::`, dangerous keywords). Optionally treat “no type casting” as invalid to force retry.

4. **Retry**
   - On validation failure: **retry with** “Required paths: X, Y, Z. Previous SQL was invalid because: [issues]. Generate again using only these paths and Snowflake syntax.”

**Pros:** Aligns SQL with the actual schema and user intent; retries are meaningful.  
**Cons:** Two LLM calls (or one small + one main); a bit more code (query understanding + path extraction from SQL).

---

### Option C — Agentic / iterative workflow

Goal: maximum robustness and self-correction.

1. **Steps**
   - **Analyze** JSON (as now).
   - **Plan**: LLM produces a short plan: “Flatten ecommerce_events, then select event_id, user:email, transaction:total_amount.”
   - **Generate**: LLM produces SQL from the plan + schema.
   - **Validate**: Schema-aware + syntax checks.
   - **Execute (optional)**: If you have a Snowflake test connection, run the query and check for errors; on failure, add error message to a **repair** step.
   - **Repair**: If validation (or execution) fails, LLM gets “Plan was X, SQL was Y, error was Z; produce corrected SQL.”

2. **Validation**
   - Same as Option B (schema-aware + syntax). Execution adds a hard check when available.

**Pros:** Best chance of correct SQL when execution is available; clear audit trail (plan → SQL → error → repair).  
**Cons:** More latency, more tokens, more code; execution requires Snowflake config.

---

## 4. Recommendation and Next Step

- **Short term:** Implement **Option A** (quick fixes), especially:
  - **Robust JSON extraction** (strip markdown, optional `{...}` fallback).
  - **More paths** or **prioritized paths** + **one-line data model** in the prompt.
  - **Consistent colon-only** path examples.
- **Medium term:** Move toward **Option B** (two-phase: query understanding → SQL generation, plus schema-aware validation and retry).

After you run `test_workflow.py` with a real API key, you can share one or two example queries and the (wrong) outputs you get. Then we can:
- Refine the “required paths” and validation rules, and
- Lock one final workflow (A, B, or C) and implement it step by step.

---

## 5. How to Run the Tests

```bash
# Schema only (no API key)
python test_schema_only.py

# Full workflow (needs OPENAI_API_KEY in .env and dependencies installed)
pip install -r requirements.txt
python test_workflow.py
```

If you want, the next step can be implementing the Option A quick fixes in the repo (file-by-file changes).
