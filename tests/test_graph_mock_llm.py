import json
import os
import unittest

from workflow.graph import run_workflow


class _Resp:
    def __init__(self, content: str):
        self.content = content


class FakeLLM:
    """
    A very small fake LLM that returns deterministic JSON for each agent prompt.
    It looks at the SystemMessage content to decide which agent is calling.
    """

    def invoke(self, messages):
        system = messages[0].content if hasattr(messages[0], "content") else str(messages[0])

        if "schema_summary" in system and "root_array_keys" in system:
            return _Resp(
                json.dumps(
                    {
                        "schema_summary": "Root object has ecommerce_events array; flatten ecommerce_events[*] for event rows.",
                        "root_array_keys": ["ecommerce_events"],
                        "important_arrays": ["ecommerce_events[*]", "ecommerce_events[*]:transaction:items[*]"],
                        "recommended_row_models": ["doc_per_row", "event_per_row"],
                        "notes": "",
                    }
                )
            )

        if "QuerySpec" in system and "FieldCatalog" in system:
            return _Resp(
                json.dumps(
                    {
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
                        "notes": "",
                    }
                )
            )

        if "candidate SQL strategies" in system and "flatten_arrays" in system:
            return _Resp(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "name": "CandidateA_DocPerRow",
                                "row_model": "doc_per_row",
                                "grain": "event",
                                "flatten_arrays": ["ecommerce_events[*]"],
                                "path_rewrite": {"strip_root_array_key": None},
                                "notes": "",
                            },
                            {
                                "name": "CandidateB_EventPerRow",
                                "row_model": "event_per_row",
                                "grain": "event",
                                "flatten_arrays": [],
                                "path_rewrite": {"strip_root_array_key": "ecommerce_events"},
                                "notes": "",
                            },
                        ],
                        "notes": "",
                    }
                )
            )

        if "strict reviewer" in system and "should_retry" in system:
            return _Resp(json.dumps({"should_retry": False, "top_issues": [], "repairs": None, "notes": ""}))

        if "apply patches" in system and "query_spec" in system:
            return _Resp(json.dumps({"query_spec": {}, "plan": {}, "notes": ""}))

        return _Resp(json.dumps({}))


class TestGraphMockLLM(unittest.TestCase):
    def test_graph_runs_with_fake_llm(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample = json.load(f)

        result = run_workflow(
            question="List event ids and emails",
            json_sample=sample,
            table_name="customer_data",
            json_column="raw_data",
            llm=FakeLLM(),
            max_retries=0,
        )

        ranked = result.get("ranked_candidates") or []
        self.assertTrue(len(ranked) >= 1)
        top_sql = ranked[0].get("sql") or ""
        self.assertIn("SELECT", top_sql.upper())


if __name__ == "__main__":
    unittest.main()

