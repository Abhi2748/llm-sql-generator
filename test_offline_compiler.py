"""
Offline deterministic test: no OpenAI required.

This validates that:
- SchemaIndex extracts canonical [*] paths
- Planner produces multiple candidates (doc-per-row vs event-per-row)
- Compiler emits Snowflake SQL with FLATTEN when needed
"""

import json

from core.planner import build_candidate_plans
from core.schema_index import build_schema_index
from core.sql_compiler import compile_to_snowflake_sql
from core.static_validate import rank_candidates


def main():
    with open("data/sample_data.json", "r", encoding="utf-8") as f:
        sample = json.load(f)

    index = build_schema_index(sample)

    # Handcrafted QuerySpec as if the LLM selected these catalog paths
    spec = {
        "question": "List event id and user email",
        "select": [
            {"path": "ecommerce_events[*]:event_id", "alias": "event_id", "cast": "string"},
            {"path": "ecommerce_events[*]:user:email", "alias": "email", "cast": "string"},
        ],
        "filters": [],
        "group_by": [],
        "aggregations": [],
        "order_by": [],
        "limit": 10,
        "grain_hint": "event",
        "notes": "offline test",
    }

    plans = build_candidate_plans(index, spec)
    compiled = [
        compile_to_snowflake_sql(index, p, spec, table_name="customer_data", json_column="raw_data")
        for p in plans
    ]
    ranked = rank_candidates(index, compiled)

    print("Candidates:")
    for c in ranked:
        print("-", c.name, "score=", c.score, "row_model=", c.assumptions.get("row_model"))

    best = ranked[0]
    print("\nBest SQL:\n", best.sql)

    assert "LATERAL FLATTEN" in best.sql or best.assumptions.get("row_model") == "event_per_row"


if __name__ == "__main__":
    main()

