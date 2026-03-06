import json
import os
import unittest

from workflow.nodes.schema_index import build_schema_index_and_catalog


class TestSchemaIndex(unittest.TestCase):
    def test_build_schema_index_and_catalog(self):
        path = os.path.join("data", "sample_data.json")
        with open(path, "r", encoding="utf-8") as f:
            sample = json.load(f)

        payload = build_schema_index_and_catalog(sample, catalog_limit=50)

        self.assertIn("ecommerce_events", payload["root_array_keys"])
        self.assertIn("ecommerce_events[*]", payload["arrays"])

        fields = payload["fields"]
        self.assertIn("ecommerce_events[*]:event_id", fields)
        self.assertIn("ecommerce_events[*]:user:email", fields)

        catalog = payload["field_catalog"]
        self.assertTrue(len(catalog) > 0)
        self.assertIn("path", catalog[0])


if __name__ == "__main__":
    unittest.main()

