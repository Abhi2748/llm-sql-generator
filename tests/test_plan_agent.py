import json
import os
import unittest

from workflow.nodes.plan_agent import derive_candidates
from workflow.nodes.schema_index import build_schema_index_and_catalog


class TestPlanAgent(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample = json.load(f)
        payload = build_schema_index_and_catalog(sample)
        cls.schema_index = {
            "root_type": payload["root_type"],
            "root_array_keys": payload["root_array_keys"],
            "arrays": payload["arrays"],
            "fields": payload["fields"],
        }

    def test_root_level_fields_produce_doc_and_event_without_nested_flatten(self):
        query_spec = {
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
        }
        plan = derive_candidates(self.schema_index, query_spec)
        candidates = plan["candidates"]
        self.assertEqual(len(candidates), 2)

        doc = next(c for c in candidates if c["row_model"] == "doc_per_row")
        event = next(c for c in candidates if c["row_model"] == "event_per_row")

        self.assertEqual(doc["flatten_arrays"], ["ecommerce_events[*]"])
        self.assertIsNone(doc["path_rewrite"]["strip_root_array_key"])

        self.assertEqual(event["flatten_arrays"], [])
        self.assertEqual(event["path_rewrite"]["strip_root_array_key"], "ecommerce_events")

        self.assertIn("derived deterministically", plan["notes"])

    def test_nested_array_path_included_in_flatten_arrays(self):
        query_spec = {
            "select": [
                {
                    "path": "ecommerce_events[*]:transaction:items[*]:price",
                    "alias": "price",
                    "cast": "number",
                }
            ],
            "filters": [],
            "group_by": ["ecommerce_events[*]:transaction:items[*]:category"],
            "aggregations": [],
            "order_by": [],
            "limit": 10,
            "grain_hint": "event",
        }
        plan = derive_candidates(self.schema_index, query_spec)
        candidates = plan["candidates"]
        self.assertGreaterEqual(len(candidates), 2)

        doc = next(c for c in candidates if c["row_model"] == "doc_per_row")
        self.assertIn("ecommerce_events[*]", doc["flatten_arrays"])
        self.assertIn("ecommerce_events[*]:transaction:items[*]", doc["flatten_arrays"])

    def test_item_grain_hint_adds_third_candidate(self):
        query_spec = {
            "select": [
                {
                    "path": "ecommerce_events[*]:transaction:items[*]:price",
                    "alias": "price",
                    "cast": "number",
                }
            ],
            "filters": [],
            "group_by": [],
            "aggregations": [],
            "order_by": [],
            "limit": 10,
            "grain_hint": "item",
        }
        plan = derive_candidates(self.schema_index, query_spec)
        candidates = plan["candidates"]
        self.assertEqual(len(candidates), 3)

        item = next(c for c in candidates if c["grain"] == "item" and c["name"] == "CandidateC_ItemPerRow")
        self.assertIn("ecommerce_events[*]", item["flatten_arrays"])
        self.assertIn("ecommerce_events[*]:transaction:items[*]", item["flatten_arrays"])


if __name__ == "__main__":
    unittest.main()
