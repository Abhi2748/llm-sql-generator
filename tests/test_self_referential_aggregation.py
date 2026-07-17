"""
Self-referential aggregation: MIN/MAX/SUM/AVG grouped by their own target field
is a no-op and must be flagged. COUNT(field) GROUP BY field is a legitimate idiom.
"""

from __future__ import annotations

import unittest

from workflow.graph import CRITIC_SKIP_SCORE_THRESHOLD
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates

PRICE_PATH = "ecommerce_events[*]:transaction:items[*]:price"
NAME_PATH = "ecommerce_events[*]:transaction:items[*]:name"


def _item_candidate():
    return {
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


def _item_fields():
    return {
        PRICE_PATH: {"type": "number"},
        NAME_PATH: {"type": "string"},
    }


class TestSelfReferentialAggregation(unittest.TestCase):
    def test_max_grouped_by_same_path_is_flagged(self):
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": [PRICE_PATH],
            "aggregations": [
                {"func": "max", "path": PRICE_PATH, "alias": "max_x", "cast": "number"}
            ],
            "order_by": [{"expr_alias": "max_x", "direction": "desc"}],
            "limit": 1,
            "grain_hint": "item",
        }
        compiled = compile_candidate_sql(
            schema_fields=_item_fields(),
            candidate=_item_candidate(),
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        ranked = rank_candidates({"fields": _item_fields()}, [compiled])
        top = ranked[0]
        self.assertTrue(
            any(
                "self-referential" in i
                and "max" in i
                and PRICE_PATH in i
                and "no-op" in i
                for i in top["issues"]
            ),
            msg=top["issues"],
        )
        self.assertLess(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)

    def test_count_grouped_by_same_path_is_not_flagged(self):
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": [PRICE_PATH],
            "aggregations": [
                {"func": "count", "path": PRICE_PATH, "alias": "price_count"}
            ],
            "order_by": [{"expr_alias": "price_count", "direction": "desc"}],
            "limit": 1,
            "grain_hint": "item",
        }
        compiled = compile_candidate_sql(
            schema_fields=_item_fields(),
            candidate=_item_candidate(),
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        ranked = rank_candidates({"fields": _item_fields()}, [compiled])
        top = ranked[0]
        self.assertFalse(
            any("self-referential" in i for i in top["issues"]),
            msg=top["issues"],
        )
        self.assertGreaterEqual(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)

    def test_top_n_order_by_limit_compiles_without_group_by(self):
        query_spec = {
            "select": [
                {"path": NAME_PATH, "alias": "name", "cast": "string"},
                {"path": PRICE_PATH, "alias": "price", "cast": "number"},
            ],
            "filters": [],
            "group_by": [],
            "aggregations": [],
            "order_by": [{"expr_alias": "price", "direction": "desc"}],
            "limit": 1,
            "grain_hint": "item",
        }
        compiled = compile_candidate_sql(
            schema_fields=_item_fields(),
            candidate=_item_candidate(),
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        self.assertNotIn("GROUP BY", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("LIMIT 1", sql)

        ranked = rank_candidates({"fields": _item_fields()}, [compiled])
        top = ranked[0]
        self.assertFalse(
            any("self-referential" in i for i in top["issues"]),
            msg=top["issues"],
        )
        self.assertEqual(top["issues"], [])
        self.assertGreaterEqual(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)


if __name__ == "__main__":
    unittest.main()
