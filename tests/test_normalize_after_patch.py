"""
normalize_query_spec must run on apply_repair_patch results so bare-string
select/aggregation entries from critic or chat-correction patches cannot reach
compile_candidate_sql.
"""

from __future__ import annotations

import unittest

from workflow.nodes.intent_agent import normalize_query_spec
from workflow.nodes.repair_agent import apply_repair_patch
from workflow.nodes.sql_compiler import compile_candidate_sql

EVENT_ID = "ecommerce_events[*]:event_id"
TAGS = "ecommerce_events[*]:metadata:tags[*]"
PRICE = "ecommerce_events[*]:transaction:items[*]:price"


def _base_query_spec():
    return {
        "select": [
            {"path": EVENT_ID, "alias": "event_id", "cast": "string"},
        ],
        "filters": [],
        "group_by": [],
        "aggregations": [],
        "order_by": [],
        "limit": 100,
        "grain_hint": "event",
        "notes": "",
    }


def _candidate():
    return {
        "name": "CandidateA_DocPerRow",
        "row_model": "doc_per_row",
        "grain": "event",
        "flatten_arrays": ["ecommerce_events[*]", "ecommerce_events[*]:metadata:tags[*]"],
        "path_rewrite": {"strip_root_array_key": None},
        "notes": "",
    }


def _fields():
    return {
        EVENT_ID: {"type": "string"},
        TAGS: {"type": "string"},
        PRICE: {"type": "number"},
        "ecommerce_events[*]:transaction:items[*]:name": {"type": "string"},
    }


class TestNormalizeAfterPatch(unittest.TestCase):
    def test_bare_string_aggregations_in_patch_normalize_and_compile(self):
        before = _base_query_spec()
        new_qs, _plan, _notes = apply_repair_patch(
            before,
            {"candidates": [_candidate()]},
            {
                "query_spec_patch": {
                    "aggregations": [TAGS],
                    "select": [],
                }
            },
        )

        aggs = new_qs.get("aggregations") or []
        self.assertTrue(aggs, "expected normalized aggregations")
        for a in aggs:
            self.assertIsInstance(a, dict)
            self.assertIn("func", a)
            self.assertEqual(a.get("path"), TAGS)

        compiled = compile_candidate_sql(
            schema_fields=_fields(),
            candidate=_candidate(),
            query_spec=new_qs,
            table_name="ecommerce",
            json_column="raw_data",
        )
        self.assertIn("COUNT(", compiled["sql"])
        self.assertTrue(compiled.get("sql"))

    def test_bare_string_select_in_patch_normalize_and_compile(self):
        before = _base_query_spec()
        new_qs, _plan, _notes = apply_repair_patch(
            before,
            {"candidates": [_candidate()]},
            {
                "query_spec_patch": {
                    "select": [EVENT_ID, TAGS],
                }
            },
        )

        selects = new_qs.get("select") or []
        self.assertEqual(len(selects), 2)
        for s in selects:
            self.assertIsInstance(s, dict)
            self.assertIn("path", s)
            self.assertIn("alias", s)
        self.assertEqual(selects[0]["path"], EVENT_ID)
        self.assertEqual(selects[1]["path"], TAGS)
        self.assertEqual(selects[0]["alias"], "event_id")
        self.assertEqual(selects[1]["alias"], "tags")

        compiled = compile_candidate_sql(
            schema_fields=_fields(),
            candidate=_candidate(),
            query_spec=new_qs,
            table_name="ecommerce",
            json_column="raw_data",
        )
        self.assertIn("event_id", compiled["sql"])
        self.assertIn("tags", compiled["sql"])

    def test_well_formed_patch_is_noop_for_normalize(self):
        before = _base_query_spec()
        well_formed_select = [
            {"path": EVENT_ID, "alias": "event_id", "cast": "string"},
            {"path": TAGS, "alias": "tags", "cast": "string"},
        ]
        well_formed_aggs = [
            {"func": "count", "path": TAGS, "alias": "tag_count", "cast": None},
        ]
        new_qs, _plan, _notes = apply_repair_patch(
            before,
            {"candidates": []},
            {
                "query_spec_patch": {
                    "select": well_formed_select,
                    "aggregations": well_formed_aggs,
                    "group_by": ["event_id"],
                    "limit": 50,
                }
            },
        )

        self.assertEqual(new_qs["select"], well_formed_select)
        self.assertEqual(new_qs["aggregations"], well_formed_aggs)
        self.assertEqual(new_qs["group_by"], ["event_id"])
        self.assertEqual(new_qs["limit"], 50)
        # Unpatched keys survive; normalize adds no notes on clean input.
        self.assertEqual(new_qs.get("grain_hint"), "event")
        self.assertEqual(new_qs.get("filters"), [])
        self.assertNotIn("normalization_notes", new_qs)

        # Direct normalize on already-correct input is also a no-op.
        again = normalize_query_spec(dict(new_qs))
        self.assertEqual(again["select"], well_formed_select)
        self.assertEqual(again["aggregations"], well_formed_aggs)
        self.assertNotIn("normalization_notes", again)


if __name__ == "__main__":
    unittest.main()
