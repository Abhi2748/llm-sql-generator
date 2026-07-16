import copy
import json
import os
import unittest

from workflow.graph import run_workflow
from workflow.nodes.schema_index import build_schema_index_and_catalog
from workflow.schema_cache import (
    clear_schema_cache,
    get_cached_schema,
    set_cached_schema,
    shape_hash,
)
from tests.test_graph_mock_llm import FakeLLM


class CountingSchemaFakeLLM(FakeLLM):
    """Counts schema-summarizer invocations specifically."""

    def __init__(self):
        super().__init__()
        self.schema_summarizer_calls = 0

    def invoke(self, messages):
        system = messages[0].content if hasattr(messages[0], "content") else str(messages[0])
        if "schema_summary" in system and "root_array_keys" in system:
            self.schema_summarizer_calls += 1
        return super().invoke(messages)


class TestSchemaCache(unittest.TestCase):
    def setUp(self):
        clear_schema_cache()

    def tearDown(self):
        clear_schema_cache()

    def _schema_index_from_sample(self, sample):
        payload = build_schema_index_and_catalog(sample)
        return {
            "root_type": payload["root_type"],
            "root_array_keys": payload["root_array_keys"],
            "arrays": payload["arrays"],
            "fields": payload["fields"],
        }

    def test_same_shape_different_data_same_hash(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample_a = json.load(f)
        sample_b = copy.deepcopy(sample_a)
        # Change leaf values without changing structure.
        sample_b["ecommerce_events"][0]["event_id"] = "CHANGED-EVENT-ID"
        if sample_b["ecommerce_events"][0].get("user"):
            sample_b["ecommerce_events"][0]["user"]["email"] = "other@example.com"

        idx_a = self._schema_index_from_sample(sample_a)
        idx_b = self._schema_index_from_sample(sample_b)
        self.assertEqual(shape_hash(idx_a), shape_hash(idx_b))

    def test_different_shape_different_hash(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample_a = json.load(f)
        sample_b = copy.deepcopy(sample_a)
        sample_b["ecommerce_events"][0]["brand_new_structural_field"] = {"nested": 1}

        idx_a = self._schema_index_from_sample(sample_a)
        idx_b = self._schema_index_from_sample(sample_b)
        self.assertNotEqual(shape_hash(idx_a), shape_hash(idx_b))

    def test_get_set_roundtrip(self):
        key = "abc123"
        self.assertIsNone(get_cached_schema(key))
        set_cached_schema(key, "summary text", {"notes": "meta"})
        cached = get_cached_schema(key)
        self.assertEqual(cached["schema_summary"], "summary text")
        self.assertEqual(cached["schema_summary_meta"], {"notes": "meta"})

    def test_cache_hit_skips_schema_summarizer_llm(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample = json.load(f)

        llm1 = CountingSchemaFakeLLM()
        run_workflow(
            question="List event ids and emails",
            json_sample=sample,
            table_name="customer_data",
            json_column="raw_data",
            llm=llm1,
            max_retries=0,
        )
        self.assertEqual(llm1.schema_summarizer_calls, 1)

        # Same shape, different leaf values — should hit cache.
        sample2 = copy.deepcopy(sample)
        sample2["ecommerce_events"][0]["event_id"] = "DIFFERENT-ID"

        llm2 = CountingSchemaFakeLLM()
        result = run_workflow(
            question="List event ids and emails again",
            json_sample=sample2,
            table_name="customer_data",
            json_column="raw_data",
            llm=llm2,
            max_retries=0,
        )
        self.assertEqual(llm2.schema_summarizer_calls, 0)
        self.assertTrue(result.get("schema_summary"))


if __name__ == "__main__":
    unittest.main()
