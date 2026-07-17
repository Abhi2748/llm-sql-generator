"""
Retry exhaustion must surface as degraded/unresolved, not a silent clean score.

ecom-09 shape: critic keeps patching an ``in`` filter with a non-list value;
max_retries exhausts; final result must set retry_exhausted_unresolved.
"""

from __future__ import annotations

import json
import os
import unittest

from workflow.graph import run_workflow
from workflow.nodes.repair_agent import apply_repair_patch


class _Resp:
    def __init__(self, content: str):
        self.content = content


EVENT_TYPE_PATH = "ecommerce_events[*]:event_type"
EVENT_ID_PATH = "ecommerce_events[*]:event_id"

# Uncoercible string — not a Python list literal (unlike "['a','b']").
BAD_IN_VALUE = "purchase or product_view"


class ExhaustingCriticFakeLLM:
    """
    Intent returns a malformed ``in`` filter; critic always retries with the
    same uncoercible string value (repair is deterministic — no LLM there).
    """

    def __init__(self):
        self.critic_calls = 0

    def invoke(self, messages):
        system = messages[0].content if hasattr(messages[0], "content") else str(messages[0])

        if "schema_summary" in system and "root_array_keys" in system:
            return _Resp(
                json.dumps(
                    {
                        "schema_summary": "ecommerce_events array of events.",
                        "root_array_keys": ["ecommerce_events"],
                        "important_arrays": ["ecommerce_events[*]"],
                        "recommended_row_models": ["doc_per_row"],
                        "notes": "",
                    }
                )
            )

        if "QuerySpec" in system and "FieldCatalog" in system:
            return _Resp(
                json.dumps(
                    {
                        "select": [
                            {"path": EVENT_ID_PATH, "alias": "event_id", "cast": "string"},
                            {"path": EVENT_TYPE_PATH, "alias": "event_type", "cast": "string"},
                        ],
                        "filters": [
                            {
                                "path": EVENT_TYPE_PATH,
                                "op": "in",
                                "value": BAD_IN_VALUE,
                                "cast": "string",
                            }
                        ],
                        "group_by": [],
                        "aggregations": [],
                        "order_by": [],
                        "limit": 100,
                        "grain_hint": "event",
                        "notes": "",
                    }
                )
            )

        if "strict reviewer" in system and "should_retry" in system:
            self.critic_calls += 1
            return _Resp(
                json.dumps(
                    {
                        "should_retry": True,
                        "top_issues": [
                            "Filter op 'in' requires a list value, got str"
                        ],
                        "repairs": {
                            "query_spec_patch": {
                                "filters": [
                                    {
                                        "path": EVENT_TYPE_PATH,
                                        "op": "in",
                                        # Live ecom-09 sometimes used a stringy
                                        # list-repr; use a non-literal string so
                                        # sanitization rejects and the bug persists.
                                        "value": BAD_IN_VALUE,
                                        "cast": "string",
                                    }
                                ]
                            },
                            "plan_patch": None,
                        },
                        "notes": "Retry with corrected in-filter value.",
                    }
                )
            )

        return _Resp(json.dumps({}))


class TestInFilterPatchSanitization(unittest.TestCase):
    def test_string_list_literal_is_coerced(self):
        before = {
            "select": [],
            "filters": [
                {"path": EVENT_TYPE_PATH, "op": "in", "value": "bogus", "cast": "string"}
            ],
            "group_by": [],
            "aggregations": [],
            "order_by": [],
            "limit": 100,
        }
        patch = {
            "filters": [
                {
                    "path": EVENT_TYPE_PATH,
                    "op": "in",
                    "value": "['purchase', 'product_view']",
                    "cast": "string",
                }
            ]
        }
        new_qs, _plan, notes = apply_repair_patch(
            before, {}, {"query_spec_patch": patch}
        )
        self.assertEqual(
            new_qs["filters"][0]["value"],
            ["purchase", "product_view"],
        )
        self.assertTrue(any("Coerced" in n for n in notes), msg=notes)

    def test_uncoercible_in_value_falls_back_to_previous(self):
        before = {
            "filters": [
                {"path": EVENT_TYPE_PATH, "op": "in", "value": "old-bad", "cast": "string"}
            ]
        }
        patch = {
            "filters": [
                {"path": EVENT_TYPE_PATH, "op": "in", "value": BAD_IN_VALUE, "cast": "string"}
            ]
        }
        new_qs, _plan, notes = apply_repair_patch(
            before, {}, {"query_spec_patch": patch}
        )
        self.assertEqual(new_qs["filters"][0]["value"], "old-bad")
        self.assertTrue(any("Rejected" in n for n in notes), msg=notes)


class TestRetryExhaustionReporting(unittest.TestCase):
    def test_exhausted_retries_marked_unresolved(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample = json.load(f)

        llm = ExhaustingCriticFakeLLM()
        result = run_workflow(
            question="List events where the event type is purchase or product_view.",
            json_sample=sample,
            table_name="ecommerce",
            json_column="raw_data",
            max_retries=2,
            llm=llm,
        )

        self.assertGreaterEqual(llm.critic_calls, 2)
        self.assertTrue(
            result.get("retry_exhausted_unresolved"),
            msg=f"expected degraded flag; loop_exit={result.get('loop_exit_reason')!r}",
        )
        self.assertEqual(result.get("loop_exit_reason"), "hit_max_retries")

        ranked = result.get("ranked_candidates") or []
        self.assertTrue(ranked)
        top = ranked[0]
        self.assertTrue(top.get("retry_exhausted_unresolved"))
        self.assertTrue(
            any("retry_exhausted_unresolved" in str(i) for i in (top.get("issues") or [])),
            msg=top.get("issues"),
        )


if __name__ == "__main__":
    unittest.main()
