"""
Repair patch application: critic query_spec_patch/plan_patch merge onto the
existing query_spec/plan. Patch wins for keys it specifies; before wins for
keys it omits. No LLM — see ADR 0003.
"""

from __future__ import annotations

import copy
import json
import unittest

from workflow.nodes.repair_agent import apply_repair_patch, repair_agent_node


def _ga4_03_before():
    return {
        "select": [
            {
                "path": "events[*]:geo:country",
                "alias": "country",
                "cast": "string",
            },
            {
                "path": "events[*]:event_name",
                "alias": "event_name",
                "cast": "string",
            },
        ],
        "filters": [],
        "group_by": ["country"],
        "aggregations": [
            {
                "func": "count",
                "path": "events[*]:event_name",
                "alias": "event_count",
                "cast": "number",
            }
        ],
        "order_by": [{"expr_alias": "event_count", "direction": "desc"}],
        "limit": 5,
        "grain_hint": "document",
        "notes": "",
    }


def _ga4_03_live_patch():
    # Exact shape logged live in critic_notes for ga4-03 — no order_by, no limit.
    return {
        "select": [
            {
                "path": "events[*]:geo:country",
                "alias": "country",
                "cast": "string",
            }
        ],
        "aggregations": [
            {
                "func": "count",
                "path": "events[*]:event_name",
                "alias": "event_count",
                "cast": "number",
            }
        ],
        "group_by": ["country"],
    }


class TestRepairPatchApplication(unittest.TestCase):
    def test_omitted_order_by_and_limit_survive_patch(self):
        before = _ga4_03_before()
        query_spec_patch = _ga4_03_live_patch()
        plan_before = {"candidates": [{"name": "CandidateA_DocPerRow"}], "notes": ""}

        state = {
            "query_spec": copy.deepcopy(before),
            "plan": copy.deepcopy(plan_before),
            "critic_notes": {
                "should_retry": True,
                "top_issues": [
                    "SELECT item 'event_name' is not aggregated and not part of the "
                    "declared grouping - QuerySpec inconsistency"
                ],
                "repairs": {
                    "query_spec_patch": query_spec_patch,
                    "plan_patch": {"flatten_arrays": ["events[*]"]},
                },
                "notes": "Patching to ensure proper aggregation and flattening of arrays.",
            },
        }

        after_state = repair_agent_node(state, llm=None)  # type: ignore[arg-type]
        after = after_state.get("query_spec") or {}
        after_plan = after_state.get("plan") or {}

        print("\n" + "=" * 78)
        print("REPAIR PATCH APPLICATION (deterministic merge)")
        print("=" * 78)
        print(f"BEFORE order_by: {before.get('order_by')!r}")
        print(f"BEFORE limit:    {before.get('limit')!r}")
        print(f"patch keys:      {sorted(query_spec_patch.keys())}")
        print(f"AFTER order_by:  {after.get('order_by')!r}")
        print(f"AFTER limit:     {after.get('limit')!r}")
        print(f"AFTER select:    {json.dumps(after.get('select'), indent=2)}")
        print(f"repair_notes:    {after_state.get('repair_notes')!r}")
        print("=" * 78 + "\n")

        self.assertEqual(
            after.get("order_by"),
            [{"expr_alias": "event_count", "direction": "desc"}],
        )
        self.assertEqual(after.get("limit"), 5)
        self.assertEqual(after.get("select"), query_spec_patch["select"])
        self.assertEqual(after.get("group_by"), ["country"])
        self.assertEqual(after_plan.get("flatten_arrays"), ["events[*]"])
        self.assertEqual(after_plan.get("candidates"), plan_before["candidates"])

    def test_patch_order_by_and_limit_overwrite_when_present(self):
        before = _ga4_03_before()
        patch = {
            **_ga4_03_live_patch(),
            "order_by": [{"expr_alias": "event_count", "direction": "asc"}],
            "limit": 10,
        }

        new_qs, new_plan, _notes = apply_repair_patch(
            copy.deepcopy(before),
            {"candidates": []},
            {"query_spec_patch": patch, "plan_patch": None},
        )

        print("\n" + "=" * 78)
        print("REPAIR PATCH OVERWRITE (order_by/limit in patch)")
        print("=" * 78)
        print(f"BEFORE order_by: {before.get('order_by')!r}  limit={before.get('limit')!r}")
        print(f"PATCH  order_by: {patch.get('order_by')!r}  limit={patch.get('limit')!r}")
        print(f"AFTER  order_by: {new_qs.get('order_by')!r}  limit={new_qs.get('limit')!r}")
        print("=" * 78 + "\n")

        self.assertEqual(
            new_qs.get("order_by"),
            [{"expr_alias": "event_count", "direction": "asc"}],
        )
        self.assertEqual(new_qs.get("limit"), 10)
        # Unpatched keys still preserved.
        self.assertEqual(new_qs.get("grain_hint"), "document")
        self.assertEqual(new_qs.get("filters"), [])
        self.assertEqual(new_plan, {"candidates": []})


if __name__ == "__main__":
    unittest.main()
