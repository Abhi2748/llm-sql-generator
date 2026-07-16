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

    def test_indexes_fields_beyond_old_array_sample_cap(self):
        """Regression for gh-06: fields past the old max_array_samples=3 were invisible."""
        items = [{"id": i, "name": f"item_{i}"} for i in range(50)]
        items[40]["rare_field"] = "present"
        sample = {"items": items}

        payload = build_schema_index_and_catalog(sample)

        self.assertIn("items[*]:rare_field", payload["fields"])
        catalog_paths = {entry["path"] for entry in payload["field_catalog"]}
        self.assertIn("items[*]:rare_field", catalog_paths)

        # Explicit old cap still hides it (parameter remains usable).
        capped = build_schema_index_and_catalog(sample, max_array_samples=3)
        self.assertNotIn("items[*]:rare_field", capped["fields"])


if __name__ == "__main__":
    unittest.main()

