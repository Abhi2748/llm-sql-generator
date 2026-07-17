"""
GROUP BY source of truth: compiler emits GROUP BY only from query_spec.group_by.
Stray raw SELECT columns alongside aggregations are flagged, not auto-grouped.
"""

from __future__ import annotations

import unittest

from workflow.graph import CRITIC_SKIP_SCORE_THRESHOLD
from workflow.nodes.plan_agent import derive_candidates
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates


def _github_candidate():
    return {
        "name": "CandidateA_DocPerRow",
        "row_model": "doc_per_row",
        "grain": "document",
        "flatten_arrays": ["events[*]"],
        "path_rewrite": {"strip_root_array_key": None},
        "notes": "",
    }


def _github_fields():
    return {
        "events[*]:actor:login": {"type": "string"},
        "events[*]:id": {"type": "string"},
        "events[*]:event_name": {"type": "string"},
        "events[*]:geo:country": {"type": "string"},
    }


class TestGroupBySourceOfTruth(unittest.TestCase):
    def test_stray_select_not_added_to_group_by_and_flagged(self):
        """gh-03-shaped bad intent output: actor_login grouped, event_id stray raw select."""
        query_spec = {
            "select": [
                {"path": "events[*]:actor:login", "alias": "actor_login", "cast": "string"},
                {"path": "events[*]:id", "alias": "event_id", "cast": "string"},
            ],
            "filters": [],
            "group_by": ["actor_login"],
            "aggregations": [{"func": "count", "path": "events[*]:id", "alias": "event_count"}],
            "order_by": [{"expr_alias": "event_count", "direction": "desc"}],
            "limit": 1,
            "grain_hint": "document",
        }
        schema_fields = _github_fields()
        compiled = compile_candidate_sql(
            schema_fields=schema_fields,
            candidate=_github_candidate(),
            query_spec=query_spec,
            table_name="github",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        group_by_clause = sql.split("GROUP BY", 1)[1].split("\n", 1)[0]

        # GROUP BY built only from query_spec.group_by — never event_id.
        self.assertIn("actor_login", group_by_clause)
        self.assertNotIn("event_id", group_by_clause)
        self.assertEqual(compiled["group_by_declared"], ["actor_login"])

        ranked = rank_candidates({"fields": schema_fields}, [compiled])
        top = ranked[0]
        self.assertTrue(
            any(
                "SELECT item 'event_id'" in i and "QuerySpec inconsistency" in i
                for i in top["issues"]
            ),
            msg=top["issues"],
        )
        self.assertGreaterEqual(top["score"], 0)
        self.assertLess(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)

    def test_well_formed_aggregation_query_compiles_cleanly(self):
        query_spec = {
            "select": [
                {"path": "events[*]:actor:login", "alias": "actor_login", "cast": "string"},
            ],
            "filters": [],
            "group_by": ["events[*]:actor:login"],
            "aggregations": [{"func": "count", "path": None, "alias": "event_count"}],
            "order_by": [{"expr_alias": "event_count", "direction": "desc"}],
            "limit": 1,
            "grain_hint": "document",
        }
        schema_index = {
            "root_array_keys": ["events"],
            "arrays": ["events[*]"],
            "fields": _github_fields(),
        }
        plan = derive_candidates(schema_index, query_spec)
        schema_fields = schema_index["fields"]
        compiled = compile_candidate_sql(
            schema_fields=schema_fields,
            candidate=plan["candidates"][0],
            query_spec=query_spec,
            table_name="github",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        group_by_clause = sql.split("GROUP BY", 1)[1].split("\n", 1)[0]

        self.assertIn("actor:login", group_by_clause)
        self.assertNotIn("event_id", group_by_clause)
        self.assertIn("COUNT(*)", sql)
        self.assertIn("actor_login", sql)

        ranked = rank_candidates(schema_index, [compiled])
        top = ranked[0]
        self.assertGreaterEqual(top["score"], CRITIC_SKIP_SCORE_THRESHOLD)
        self.assertEqual(top["issues"], [])

    def test_manual_trace_gh03_bad_shape_sql(self):
        """
        Manual trace: real intent-agent failure shape must not put event_id in GROUP BY.
        Produces:
          SELECT actor_login, event_id, COUNT(event:id) ...
          GROUP BY actor_login   -- NOT event_id
        and flags event_id as QuerySpec inconsistency.
        """
        query_spec = {
            "select": [
                {"path": "events[*]:actor:login", "alias": "actor_login", "cast": "string"},
                {"path": "events[*]:id", "alias": "event_id", "cast": "string"},
            ],
            "filters": [],
            "group_by": ["actor_login"],
            "aggregations": [{"func": "count", "path": "events[*]:id", "alias": "event_count"}],
            "order_by": [{"expr_alias": "event_count", "direction": "desc"}],
            "limit": 1,
            "grain_hint": "document",
        }
        compiled = compile_candidate_sql(
            schema_fields=_github_fields(),
            candidate=_github_candidate(),
            query_spec=query_spec,
            table_name="github",
            json_column="raw_data",
        )
        sql = compiled["sql"]
        print("\n--- gh-03 bad shape SQL trace ---")
        print(sql)
        print("group_exprs:", compiled["group_exprs"])
        print("group_by_declared:", compiled["group_by_declared"])
        print("select_aliases:", compiled["select_aliases"])

        self.assertIn("event:actor:login::string AS actor_login", sql)
        self.assertIn("event:id::string AS event_id", sql)
        self.assertIn("COUNT(event:id) AS event_count", sql)
        self.assertIn("GROUP BY actor_login", sql)
        self.assertNotRegex(sql, r"GROUP BY[^\n]*event_id")

        ranked = rank_candidates({"fields": _github_fields()}, [compiled])
        self.assertLess(ranked[0]["score"], CRITIC_SKIP_SCORE_THRESHOLD)
        self.assertTrue(
            any("event_id" in i and "QuerySpec inconsistency" in i for i in ranked[0]["issues"])
        )


if __name__ == "__main__":
    unittest.main()
