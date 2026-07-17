"""
GROUP BY / aggregation shape validation (Bugs A and B from live runs).
"""

from __future__ import annotations

import unittest

from workflow.graph import CRITIC_SKIP_SCORE_THRESHOLD
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates


class TestGroupByValidation(unittest.TestCase):
    def test_bug_a_duplicate_alias_and_ungrouped_raw_column(self):
        """
        Hand-built invalid SQL still caught by static_validate (duplicate alias case).
        """
        compiled = [
            {
                "name": "BugA_MaxPrice",
                "sql": (
                    "SELECT\n"
                    "  item:price::number AS max_price,\n"
                    "  item:name::string AS item_name,\n"
                    "  MAX(item:price::number) AS max_price\n"
                    "FROM lvl2\n"
                    "GROUP BY item_name\n"
                ),
                "assumptions": {
                    "flatten_arrays": [
                        "ecommerce_events[*]",
                        "ecommerce_events[*]:transaction:items[*]",
                    ]
                },
                "paths_used": [
                    "ecommerce_events[*]:transaction:items[*]:price",
                    "ecommerce_events[*]:transaction:items[*]:name",
                ],
                "issues": [],
                "select_items": [
                    "item:price::number AS max_price",
                    "item:name::string AS item_name",
                ],
                "select_aliases": ["max_price", "item_name"],
                "select_exprs": ["item:price::number", "item:name::string"],
                "agg_items": ["MAX(item:price::number) AS max_price"],
                "agg_aliases": ["max_price"],
                "group_exprs": ["item_name"],
            }
        ]
        schema_index = {
            "fields": {
                "ecommerce_events[*]:transaction:items[*]:price": {"type": "number"},
                "ecommerce_events[*]:transaction:items[*]:name": {"type": "string"},
            }
        }
        ranked = rank_candidates(schema_index, compiled)
        top = ranked[0]
        issues_text = " ".join(top["issues"])

        self.assertTrue(
            any("Duplicate alias 'max_price'" in i for i in top["issues"]),
            msg=top["issues"],
        )
        self.assertNotIn("SELECT item 'item_name'", issues_text)
        self.assertLess(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)

    def test_compiler_drops_only_duplicate_alias_select(self):
        """Duplicate-alias raw select is dropped; other columns kept and grouped."""
        query_spec = {
            "select": [
                {
                    "path": "ecommerce_events[*]:transaction:items[*]:price",
                    "alias": "max_price",
                    "cast": "number",
                },
                {
                    "path": "ecommerce_events[*]:transaction:items[*]:name",
                    "alias": "item_name",
                    "cast": "string",
                },
            ],
            "filters": [],
            "group_by": ["ecommerce_events[*]:transaction:items[*]:name"],
            "aggregations": [
                {
                    "func": "max",
                    "path": "ecommerce_events[*]:transaction:items[*]:price",
                    "alias": "max_price",
                    "cast": "number",
                }
            ],
            "order_by": [],
            "limit": 1,
            "grain_hint": "item",
        }
        candidate = {
            "name": "CandidateA_DocPerRow",
            "row_model": "doc_per_row",
            "grain": "item",
            "flatten_arrays": [
                "ecommerce_events[*]",
                "ecommerce_events[*]:transaction:items[*]",
            ],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }
        schema_fields = {
            "ecommerce_events[*]:transaction:items[*]:price": {"type": "number"},
            "ecommerce_events[*]:transaction:items[*]:name": {"type": "string"},
        }
        compiled = compile_candidate_sql(
            schema_fields=schema_fields,
            candidate=candidate,
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        self.assertNotIn("item:price::number AS max_price", sql)
        self.assertIn("MAX(item:price::number) AS max_price", sql)
        self.assertIn("item:name", sql)
        self.assertEqual(compiled["select_aliases"].count("max_price"), 0)
        self.assertTrue(
            any("duplicates an aggregation alias" in i for i in compiled["issues"]),
            msg=compiled["issues"],
        )

    def test_no_group_by_when_aggregations_empty(self):
        """Plain select queries must not emit GROUP BY even if group_by is populated."""
        query_spec = {
            "select": [
                {"path": "ecommerce_events[*]:event_id", "alias": "event_id", "cast": "string"},
                {"path": "ecommerce_events[*]:user:email", "alias": "email", "cast": "string"},
            ],
            "filters": [{"path": "ecommerce_events[*]:event_id", "op": "eq", "value": "evt_001"}],
            "group_by": ["ecommerce_events[*]:event_id", "ecommerce_events[*]:user:email"],
            "aggregations": [],
            "order_by": [],
            "limit": 100,
            "grain_hint": "event",
        }
        candidate = {
            "name": "CandidateA_DocPerRow",
            "row_model": "doc_per_row",
            "grain": "event",
            "flatten_arrays": ["ecommerce_events[*]"],
            "path_rewrite": {"strip_root_array_key": None},
            "notes": "",
        }
        schema_fields = {
            "ecommerce_events[*]:event_id": {"type": "string"},
            "ecommerce_events[*]:user:email": {"type": "string"},
        }
        compiled = compile_candidate_sql(
            schema_fields=schema_fields,
            candidate=candidate,
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        self.assertNotIn("GROUP BY", compiled["sql"])
        self.assertTrue(
            any("Ignored group_by because aggregations is empty" in i for i in compiled["issues"]),
            msg=compiled["issues"],
        )


if __name__ == "__main__":
    unittest.main()
