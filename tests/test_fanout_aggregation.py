"""
Fan-out / double-counting: SUM/AVG/MIN/MAX of an event-level field while
flattened to item grain multiplies the value once per child row.
"""

from __future__ import annotations

import unittest

from workflow.graph import CRITIC_SKIP_SCORE_THRESHOLD
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates

TOTAL_AMOUNT_PATH = "ecommerce_events[*]:transaction:total_amount"
ITEM_PRICE_PATH = "ecommerce_events[*]:transaction:items[*]:price"
EVENT_ID_PATH = "ecommerce_events[*]:event_id"


def _item_grain_candidate():
    return {
        "name": "CandidateA_ItemGrain",
        "row_model": "doc_per_row",
        "grain": "item",
        "flatten_arrays": [
            "ecommerce_events[*]",
            "ecommerce_events[*]:transaction:items[*]",
        ],
        "path_rewrite": {"strip_root_array_key": None},
        "notes": "",
    }


def _schema_fields():
    return {
        TOTAL_AMOUNT_PATH: {"type": "number"},
        ITEM_PRICE_PATH: {"type": "number"},
        EVENT_ID_PATH: {"type": "string"},
    }


class TestFanoutAggregation(unittest.TestCase):
    def test_sum_event_field_at_item_grain_is_flagged(self):
        """
        evt_001 total_amount=299.99 with 2 items → SUM at item grain yields 599.98.
        Must be flagged as fan-out.
        """
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": [],
            "aggregations": [
                {
                    "func": "sum",
                    "path": TOTAL_AMOUNT_PATH,
                    "alias": "total_revenue",
                    "cast": "number",
                }
            ],
            "order_by": [],
            "limit": 100,
            "grain_hint": "item",
        }
        compiled = compile_candidate_sql(
            schema_fields=_schema_fields(),
            candidate=_item_grain_candidate(),
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        ranked = rank_candidates({"fields": _schema_fields()}, [compiled])
        top = ranked[0]
        self.assertTrue(
            any(
                "fan-out" in i
                and "sum" in i
                and TOTAL_AMOUNT_PATH in i
                and "shallower grain" in i
                for i in top["issues"]
            ),
            msg=top["issues"],
        )
        self.assertLess(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)

    def test_sum_item_field_at_item_grain_is_not_flagged(self):
        """SUM(items[*]:price) at item grain matches grain — no fan-out."""
        query_spec = {
            "select": [],
            "filters": [],
            "group_by": [],
            "aggregations": [
                {
                    "func": "sum",
                    "path": ITEM_PRICE_PATH,
                    "alias": "item_revenue",
                    "cast": "number",
                }
            ],
            "order_by": [],
            "limit": 100,
            "grain_hint": "item",
        }
        compiled = compile_candidate_sql(
            schema_fields=_schema_fields(),
            candidate=_item_grain_candidate(),
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        ranked = rank_candidates({"fields": _schema_fields()}, [compiled])
        top = ranked[0]
        self.assertFalse(
            any("fan-out" in i for i in top["issues"]),
            msg=top["issues"],
        )
        self.assertGreaterEqual(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)


if __name__ == "__main__":
    unittest.main()
