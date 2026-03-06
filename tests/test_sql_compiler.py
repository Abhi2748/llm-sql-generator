import json
import os
import unittest

from workflow.nodes.schema_index import build_schema_index_and_catalog
from workflow.nodes.sql_compiler import compile_candidate_sql


class TestSQLCompiler(unittest.TestCase):
    def test_compiles_flatten_for_doc_per_row(self):
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            sample = json.load(f)

        payload = build_schema_index_and_catalog(sample)
        schema_fields = payload["fields"]

        query_spec = {
            "select": [
                {"path": "ecommerce_events[*]:event_id", "alias": "event_id", "cast": "string"},
                {"path": "ecommerce_events[*]:user:email", "alias": "email", "cast": "string"},
            ],
            "filters": [],
            "group_by": [],
            "aggregations": [],
            "limit": 10,
        }

        candidate = {
            "name": "CandidateA_DocPerRow",
            "row_model": "doc_per_row",
            "grain": "event",
            "flatten_arrays": ["ecommerce_events[*]"],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }

        compiled = compile_candidate_sql(
            schema_fields=schema_fields,
            candidate=candidate,
            query_spec=query_spec,
            table_name="customer_data",
            json_column="raw_data",
        )

        self.assertIn("LATERAL FLATTEN", compiled["sql"])
        self.assertIn("v0:ecommerce_events", compiled["sql"])
        self.assertIn("event:event_id::string", compiled["sql"])


if __name__ == "__main__":
    unittest.main()

