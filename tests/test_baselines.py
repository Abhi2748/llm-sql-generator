"""
Fake-LLM unit tests for eval baselines A/B.
No real API calls — safe under default `pytest tests/` (not marked live).
"""

from __future__ import annotations

import unittest

from baseline.single_shot import generate_sql_baseline_a
from baseline.single_shot_with_retry import generate_sql_baseline_b


class _Resp:
    def __init__(self, content: str):
        self.content = content


SQL_V1 = (
    "SELECT f.value:geo:country::string AS country, COUNT(*) AS cnt\n"
    "FROM ga4 t, LATERAL FLATTEN(input => t.raw_data:events) f\n"
    "GROUP BY 1\n"
    "ORDER BY cnt DESC\n"
    "LIMIT 5"
)

SQL_V2 = (
    "SELECT f.value:geo:country::string AS country, COUNT(*) AS event_count\n"
    "FROM ga4 t, LATERAL FLATTEN(input => t.raw_data:events) f\n"
    "GROUP BY 1\n"
    "ORDER BY event_count DESC\n"
    "LIMIT 5"
)


class BaselineFakeLLM:
    """
    Deterministic stand-in: first call (single-shot prompt) returns SQL_V1;
    second call (self-critique prompt) returns whatever critique_response is set to.
    """

    def __init__(self, critique_response: str):
        self.critique_response = critique_response
        self.calls = 0

    def invoke(self, messages):
        self.calls += 1
        system = messages[0].content if hasattr(messages[0], "content") else str(messages[0])
        if "reviewing Snowflake SQL" in system or "LOOKS CORRECT" in system:
            return _Resp(self.critique_response)
        return _Resp(SQL_V1)


class TestBaselines(unittest.TestCase):
    def test_baseline_a_one_call_and_sql_key(self):
        llm = BaselineFakeLLM(critique_response="unused")
        out = generate_sql_baseline_a(
            question="List the top 5 countries by event count.",
            json_sample={"events": [{"geo": {"country": "US"}}]},
            table_name="ga4",
            json_column="raw_data",
            llm=llm,
        )
        self.assertEqual(out["llm_calls"], 1)
        self.assertEqual(llm.calls, 1)
        self.assertIn("sql", out)
        self.assertIn("SELECT", out["sql"].upper())
        self.assertEqual(out["raw_response"], SQL_V1)

    def test_baseline_b_always_two_calls_when_looks_correct(self):
        critique = f"LOOKS CORRECT\n{SQL_V1}"
        llm = BaselineFakeLLM(critique_response=critique)
        out = generate_sql_baseline_b(
            question="List the top 5 countries by event count.",
            json_sample={"events": [{"geo": {"country": "US"}}]},
            table_name="ga4",
            json_column="raw_data",
            llm=llm,
        )
        self.assertEqual(out["llm_calls"], 2)
        self.assertEqual(llm.calls, 2)
        self.assertFalse(out["revised"])
        self.assertEqual(out["sql"], SQL_V1)
        self.assertEqual(out["self_critique_response"], critique)

    def test_baseline_b_revised_true_when_critique_changes_sql(self):
        critique = f"REVISED\n{SQL_V2}"
        llm = BaselineFakeLLM(critique_response=critique)
        out = generate_sql_baseline_b(
            question="List the top 5 countries by event count.",
            json_sample={"events": [{"geo": {"country": "US"}}]},
            table_name="ga4",
            json_column="raw_data",
            llm=llm,
        )
        self.assertEqual(out["llm_calls"], 2)
        self.assertEqual(llm.calls, 2)
        self.assertTrue(out["revised"])
        self.assertEqual(out["sql"], SQL_V2)
        self.assertEqual(out["first_sql"], SQL_V1)


if __name__ == "__main__":
    unittest.main()
