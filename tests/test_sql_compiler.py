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

    def test_nested_array_alias_resolution(self):
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": ["ecommerce_events[*]:transaction:items[*]:category"],
            "aggregations": [
                {
                    "func": "sum",
                    "path": "ecommerce_events[*]:transaction:items[*]:price",
                    "alias": "total_price",
                    "cast": "number",
                }
            ],
            "limit": 100,
        }
        candidate = {
            "name": "Candidate_NestedItems",
            "row_model": "doc_per_row",
            "grain": "item",
            "flatten_arrays": [
                "ecommerce_events[*]",
                "ecommerce_events[*]:transaction:items[*]",
            ],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }
        compiled = compile_candidate_sql(
            schema_fields={},
            candidate=candidate,
            query_spec=query_spec,
            table_name="customer_data",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        self.assertIn("item:price", sql)
        self.assertIn("item:category", sql)
        self.assertNotIn("event:transaction:items:price", sql)

    def test_filter_operators_and_unknown_op_issues(self):
        candidate = {
            "name": "Candidate_Filters",
            "row_model": "doc_per_row",
            "grain": "event",
            "flatten_arrays": ["ecommerce_events[*]"],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }
        query_spec = {
            "select": [
                {"path": "ecommerce_events[*]:event_id", "alias": "event_id", "cast": "string"},
            ],
            "filters": [
                {"path": "ecommerce_events[*]:amount", "op": "gte", "value": 100, "cast": "number"},
                {
                    "path": "ecommerce_events[*]:status",
                    "op": "in",
                    "value": ["paid", "pending"],
                    "cast": "string",
                },
                {"path": "ecommerce_events[*]:event_id", "op": "regex", "value": "abc"},
            ],
            "group_by": [],
            "aggregations": [],
            "limit": 10,
        }
        compiled = compile_candidate_sql(
            schema_fields={},
            candidate=candidate,
            query_spec=query_spec,
            table_name="customer_data",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        self.assertIn(">=", sql)
        self.assertIn("IN (", sql)
        self.assertIn("'paid'", sql)
        self.assertIn("'pending'", sql)
        self.assertNotIn("regex", sql.lower())
        self.assertTrue(any("Unrecognized filter op" in i for i in compiled["issues"]))

    def test_order_by_and_limit_with_group_by(self):
        candidate = {
            "name": "Candidate_OrderBy",
            "row_model": "doc_per_row",
            "grain": "event",
            "flatten_arrays": ["ecommerce_events[*]"],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": ["ecommerce_events[*]:user:email"],
            "aggregations": [{"func": "count", "alias": "cnt"}],
            "order_by": [{"expr_alias": "cnt", "direction": "desc"}],
            "limit": 5,
        }
        compiled = compile_candidate_sql(
            schema_fields={},
            candidate=candidate,
            query_spec=query_spec,
            table_name="customer_data",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        self.assertIn("ORDER BY cnt DESC", sql)
        self.assertIn("LIMIT 5", sql)

    def test_count_aggregation_omits_cast_on_path(self):
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": ["events[*]:geo:country"],
            "aggregations": [
                {
                    "func": "count",
                    "path": "events[*]:event_name",
                    "alias": "event_count",
                    "cast": "number",
                }
            ],
            "order_by": [],
            "limit": 5,
        }
        candidate = {
            "name": "CandidateA_DocPerRow",
            "row_model": "doc_per_row",
            "grain": "document",
            "flatten_arrays": ["events[*]"],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }
        schema_fields = {
            "events[*]:event_name": {"type": "string"},
            "events[*]:geo:country": {"type": "string"},
        }
        compiled = compile_candidate_sql(
            schema_fields=schema_fields,
            candidate=candidate,
            query_spec=query_spec,
            table_name="ga4",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        self.assertIn("COUNT(event:event_name)", sql)
        self.assertNotIn("COUNT(event:event_name::number)", sql)
        self.assertNotIn("COUNT(event:event_name::string)", sql)


if __name__ == "__main__":
    unittest.main()

