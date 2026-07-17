"""Structurally-aware JSON sample truncation for baselines."""

from __future__ import annotations

import json
import unittest

from baseline._util import even_subsample, format_json_sample


class TestBaselineTruncation(unittest.TestCase):
    def test_large_array_root_truncates_to_valid_json_under_cap(self):
        # Synthetic array large enough that pretty-printed form exceeds a small cap.
        sample = [{"id": i, "payload": f"row-{i}-" + ("x" * 40)} for i in range(200)]
        cap = 2_000
        full = json.dumps(sample, ensure_ascii=False, indent=2)
        self.assertGreater(len(full), cap)

        out = format_json_sample(sample, max_chars=cap)
        self.assertLessEqual(len(out), cap)
        parsed = json.loads(out)  # must be valid JSON — no mid-object cut
        self.assertIsInstance(parsed, list)
        self.assertGreater(len(parsed), 0)
        self.assertLess(len(parsed), len(sample))
        # Even sub-sampling should not be a pure prefix of the original.
        prefix = sample[: len(parsed)]
        self.assertNotEqual(parsed, prefix)

    def test_small_sample_returned_unchanged(self):
        sample = {"events": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}
        expected = json.dumps(sample, ensure_ascii=False, indent=2)
        out = format_json_sample(sample, max_chars=32_000)
        self.assertEqual(out, expected)
        self.assertEqual(json.loads(out), sample)

    def test_even_subsample_spreads_across_range(self):
        items = list(range(10))
        self.assertEqual(even_subsample(items, 10), items)
        self.assertEqual(even_subsample(items, 1), [5])
        self.assertEqual(even_subsample(items, 3), [0, 4, 9])


if __name__ == "__main__":
    unittest.main()
