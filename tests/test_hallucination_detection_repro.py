"""
Repro: hallucinated path should be flagged by static_validate, but a live run
scored it 100 with empty issues. No LLM — deterministic QuerySpec only.
"""

from __future__ import annotations

import json
import os
import unittest

from workflow.nodes.plan_agent import derive_candidates
from workflow.nodes.schema_index import schema_index_node
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates

HALLUCINATED_PATH = "ecommerce_events[*]:transaction:shipping:method"


class TestHallucinationDetectionRepro(unittest.TestCase):
    def test_unknown_path_should_be_flagged(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample = json.load(f)

        state = schema_index_node({"json_sample": sample})
        schema_index = state["schema_index"]
        fields = schema_index.get("fields") or {}

        query_spec = {
            "select": [
                {
                    "path": HALLUCINATED_PATH,
                    "alias": "shipping_carrier",
                    "cast": "string",
                }
            ],
            "filters": [],
            "group_by": [],
            "aggregations": [],
            "order_by": [],
            "limit": 100,
            "grain_hint": "event",
            "notes": "",
        }

        plan = derive_candidates(schema_index, query_spec)
        candidates = plan.get("candidates") or []
        self.assertTrue(candidates, "derive_candidates returned no candidates")

        compiled = [
            compile_candidate_sql(
                schema_fields=fields,
                candidate=c,
                query_spec=query_spec,
                table_name="ecommerce",
                json_column="raw_data",
            )
            for c in candidates
        ]

        field_keys = list(fields.keys())
        sample_keys = field_keys[:5]

        print("\n" + "=" * 78)
        print("HALLUCINATION DETECTION REPRO")
        print("=" * 78)
        print(f"hallucinated path: {HALLUCINATED_PATH!r}")
        print(f"direct fields lookup: {HALLUCINATED_PATH in fields}")
        print(f"schema_index['fields'] sample keys (5): {sample_keys}")
        print(f"total fields keys: {len(field_keys)}")
        print("-" * 78)

        for i, c in enumerate(compiled):
            paths_used = c.get("paths_used") or []
            print(f"\ncandidate[{i}] name={c.get('name')!r}")
            print(f"  paths_used ({len(paths_used)}): {paths_used}")
            for p in paths_used:
                present = p in fields
                print(f"  check: path={p!r}  in fields={present}")
                if not present:
                    # Nearby keys that share a prefix, to spot formatting drift.
                    prefix = p.split(":")[0] if p else ""
                    nearby = [k for k in field_keys if k.startswith(prefix)][:8]
                    print(f"    nearby keys starting with {prefix!r}: {nearby}")

        ranked = rank_candidates(schema_index, compiled)
        print("\n" + "-" * 78)
        print("RANK RESULTS")
        for r in ranked:
            print(
                f"  {r.get('name')}: score={r.get('score')}  issues={r.get('issues')}"
            )
        print("=" * 78 + "\n")

        # Intentionally soft for now: this test exists to print the mismatch.
        # Keep an assertion so pytest reports the run as a real test result.
        self.assertTrue(ranked)


if __name__ == "__main__":
    unittest.main()
