import unittest

from workflow.nodes.static_validate import rank_candidates


class TestStaticValidate(unittest.TestCase):
    def test_penalizes_unknown_paths(self):
        schema_index = {
            "fields": {
                "known:path": {"type": "string", "samples": ["x"]},
            }
        }

        compiled = [
            {
                "name": "A",
                "sql": "SELECT v0:known:path::string AS x FROM base",
                "assumptions": {"flatten_arrays": []},
                "paths_used": ["known:path"],
                "issues": [],
            },
            {
                "name": "B",
                "sql": "SELECT v0:unknown:path::string AS x FROM base",
                "assumptions": {"flatten_arrays": []},
                "paths_used": ["unknown:path"],
                "issues": [],
            },
        ]

        ranked = rank_candidates(schema_index, compiled)
        self.assertEqual(ranked[0]["name"], "A")
        self.assertLess(ranked[1]["score"], ranked[0]["score"])


if __name__ == "__main__":
    unittest.main()

