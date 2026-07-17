import unittest

from workflow.nodes.intent_agent import normalize_query_spec


class TestIntentAgentNormalization(unittest.TestCase):
    def test_normalizes_bare_string_select_entries(self):
        raw = {
            "select": [
                "events[*]:event_name",
                "events[*]:event_timestamp",
            ],
            "filters": [],
            "group_by": [],
            "aggregations": [],
            "order_by": [],
            "limit": 5,
            "grain_hint": "document",
            "notes": "",
        }
        spec = normalize_query_spec(raw)

        self.assertEqual(len(spec["select"]), 2)
        self.assertEqual(spec["select"][0]["path"], "events[*]:event_name")
        self.assertEqual(spec["select"][0]["alias"], "event_name")
        self.assertIsNone(spec["select"][0]["cast"])
        self.assertEqual(spec["select"][1]["alias"], "event_timestamp")
        self.assertTrue(spec.get("normalization_notes"))
        self.assertTrue(
            any("bare string path normalized" in n for n in spec["normalization_notes"])
        )

    def test_leaves_well_formed_spec_unchanged(self):
        raw = {
            "select": [
                {"path": "events[*]:id", "alias": "id", "cast": "string"},
            ],
            "filters": [{"path": "events[*]:type", "op": "eq", "value": "PushEvent"}],
            "group_by": ["events[*]:type"],
            "aggregations": [{"func": "count", "path": None, "alias": "cnt"}],
            "order_by": [{"expr_alias": "cnt", "direction": "desc"}],
            "limit": 10,
            "grain_hint": "document",
            "notes": "",
        }
        spec = normalize_query_spec(raw)
        self.assertNotIn("normalization_notes", spec)
        self.assertEqual(spec["select"][0]["alias"], "id")


if __name__ == "__main__":
    unittest.main()
