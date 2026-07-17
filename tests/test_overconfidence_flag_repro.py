"""
Eval-harness repros: overconfidence FLATTEN false positive + agg-alias token score.
No LLM — pure helpers from eval/_shared.py.
"""

from __future__ import annotations

import unittest

from eval._shared import (
    collect_required_tokens,
    extract_sql_colon_path_leaves,
    overconfidence_flag,
    token_score,
)

# gh-02-shaped correct SQL: FLATTEN mentions v0:events; real fields are event:type / id.
GH02_CORRECT_SQL = """
WITH
  base AS (
  SELECT t.raw_data AS v0
  FROM github t
),
  lvl1 AS (
  SELECT
    v0 AS v0,
    f1.value AS event
  FROM base,
  LATERAL FLATTEN(input => v0:events) f1
)
SELECT
  event:type::string AS type,
  COUNT(event:id) AS event_count
FROM lvl1
GROUP BY type
ORDER BY event_count DESC
"""


class TestOverconfidenceFlagRepro(unittest.TestCase):
    def test_flatten_input_events_not_flagged_when_fields_exist(self):
        """Exact gh-02 false-positive shape: 'events' only appears in FLATTEN input."""
        leaves = extract_sql_colon_path_leaves(GH02_CORRECT_SQL)
        self.assertNotIn("events", leaves)
        self.assertIn("type", leaves)
        self.assertIn("id", leaves)

        tier_leaves = {"type", "id", "login"}
        result = overconfidence_flag(
            sql=GH02_CORRECT_SQL,
            system="pipeline",
            run_out={"paths_used": [], "raw": {}, "static_validate_issues": []},
            tier_leaves=tier_leaves,
            non_leaf_container_keys={"events"},
        )
        self.assertFalse(
            result["overconfidence_flag"],
            msg=result,
        )
        self.assertNotIn("events", result["unsupported_leaves_in_sql"])
        self.assertNotIn("events", result["referenced_leaves"])

    def test_real_hallucinated_field_still_flagged_beside_flatten(self):
        sql = """
SELECT
  event:nonexistent_field::string AS bad,
  COUNT(event:id) AS cnt
FROM lvl1,
LATERAL FLATTEN(input => v0:events) f1
GROUP BY 1
"""
        tier_leaves = {"type", "id", "login"}
        result = overconfidence_flag(
            sql=sql,
            system="pipeline",
            run_out={"paths_used": [], "raw": {}, "static_validate_issues": []},
            tier_leaves=tier_leaves,
            non_leaf_container_keys={"events"},
        )
        self.assertTrue(result["overconfidence_flag"], msg=result)
        self.assertIn("nonexistent_field", result["unsupported_leaves_in_sql"])
        self.assertNotIn("events", result["unsupported_leaves_in_sql"])

    def test_org_parent_object_not_flagged_when_org_login_present(self):
        """
        ``WHERE f.value:org IS NOT NULL`` references container key ``org``, which
        is never a leaf in schema_index["fields"] when ``org:login`` exists.
        """
        sql = """
SELECT
  event:id::string AS id,
  event:org:login::string AS org_login
FROM lvl1,
LATERAL FLATTEN(input => v0:events) f1
WHERE f.value:org IS NOT NULL
"""
        tier_leaves = {"id", "login"}  # org:login leaf is "login"
        non_leaf = {"events", "org", "actor", "payload"}
        result = overconfidence_flag(
            sql=sql,
            system="pipeline",
            run_out={"paths_used": [], "raw": {}, "static_validate_issues": []},
            tier_leaves=tier_leaves,
            non_leaf_container_keys=non_leaf,
        )
        self.assertFalse(result["overconfidence_flag"], msg=result)
        self.assertNotIn("org", result["unsupported_leaves_in_sql"])
        self.assertNotIn("events", result["unsupported_leaves_in_sql"])


class TestAggregationAliasNotRequired(unittest.TestCase):
    def test_different_agg_alias_still_scores_perfect(self):
        expected = {
            "select": [],
            "filters": [],
            "group_by": ["events[*]:type"],
            "aggregations": [{"func": "count", "path": None, "alias": "cnt"}],
            "order_by": [{"expr_alias": "cnt", "direction": "desc"}],
        }
        required = collect_required_tokens(expected)
        self.assertIn("type", required)
        self.assertNotIn("cnt", [t.lower() for t in required])

        # Correct aggregation, different cosmetic alias than golden "cnt".
        sql = (
            "SELECT event:type::string AS type, COUNT(*) AS event_count "
            "FROM lvl1 GROUP BY type ORDER BY event_count DESC"
        )
        scored = token_score(sql, required)
        self.assertEqual(scored["score"], 1.0, msg=scored)
        self.assertEqual(scored["missing_tokens"], [])


if __name__ == "__main__":
    unittest.main()
