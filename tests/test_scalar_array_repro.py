"""
Repro: live comparison crash on ecom-06 ("List the tags associated with each
event.") — AttributeError: 'str' object has no attribute 'get'.

tags[*] is an array of plain strings, unlike every other array in the golden
set (arrays of objects). Deterministic path first (no LLM); live full-pipeline
extension only if steps 1–5 succeed cleanly.
"""

from __future__ import annotations

import json
import os
import traceback
import unittest

import pytest

from workflow.nodes.plan_agent import derive_candidates
from workflow.nodes.repair_agent import apply_repair_patch
from workflow.nodes.schema_index import schema_index_node
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates

QUESTION = "List the tags associated with each event."
SAMPLE_PATH = os.path.join("data", "sample_data.json")
TAGS_LEAF_HINT = "metadata:tags"


def _load_sample():
    with open(SAMPLE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_tags_path(fields: dict) -> str:
    """Confirm the scalar-array leaf path from the real schema_index, don't assume."""
    candidates = [
        p
        for p in fields
        if TAGS_LEAF_HINT in p.replace("[*]", "") or p.endswith("tags[*]") or p.endswith(":tags")
    ]
    # Prefer the [*] leaf (array-of-scalars path) when present.
    starred = [p for p in candidates if p.endswith("tags[*]")]
    if starred:
        return sorted(starred, key=len)[0]
    if candidates:
        return sorted(candidates, key=len)[0]
    raise AssertionError(
        f"No tags path found in schema_index fields. Nearby sample: "
        f"{[k for k in fields if 'tag' in k.lower()][:10]!r}"
    )


class TestScalarArrayRepro(unittest.TestCase):
    def test_core_compile_validate_path_with_scalar_array(self):
        sample = _load_sample()

        print("\n" + "=" * 78)
        print("SCALAR ARRAY REPRO — steps 1–5 (no LLM)")
        print("=" * 78)

        try:
            # 1. Real schema_index from sample_data.json
            state = schema_index_node({"json_sample": sample})
            schema_index = state["schema_index"]
            fields = schema_index.get("fields") or {}
            print(f"\n[1] schema_index arrays ({len(schema_index.get('arrays') or [])}):")
            for a in schema_index.get("arrays") or []:
                print(f"    {a}")
            print(f"    fields count: {len(fields)}")

            # 2. Confirm scalar-array leaf exists, hand-construct query_spec
            tags_path = _find_tags_path(fields)
            event_id_path = "ecommerce_events[*]:event_id"
            self.assertIn(
                tags_path,
                fields,
                msg=f"expected tags path {tags_path!r} in fields",
            )
            self.assertIn(event_id_path, fields)
            print(f"\n[2] confirmed tags path in fields: {tags_path!r}")
            print(f"    field info: {fields.get(tags_path)!r}")
            print(f"    event_id path present: {event_id_path in fields}")

            query_spec = {
                "select": [
                    {"path": event_id_path, "alias": "event_id", "cast": "string"},
                    {"path": tags_path, "alias": "tags", "cast": "string"},
                ],
                "filters": [],
                "group_by": [],
                "aggregations": [],
                "order_by": [],
                "limit": 100,
                "grain_hint": "event",
                "notes": "scalar array repro — tags[*] is array of plain strings",
            }
            print(f"    query_spec.select: {json.dumps(query_spec['select'], indent=2)}")

            # 3. derive_candidates
            plan = derive_candidates(schema_index, query_spec)
            candidates = plan.get("candidates") or []
            print(f"\n[3] derive_candidates -> {len(candidates)} candidate(s)")
            for i, c in enumerate(candidates):
                print(
                    f"    [{i}] name={c.get('name')!r} "
                    f"flatten_arrays={c.get('flatten_arrays')!r} "
                    f"grain={c.get('grain')!r}"
                )
            self.assertTrue(candidates, "derive_candidates returned no candidates")

            # 4. compile each candidate
            compiled = []
            print("\n[4] compile_candidate_sql")
            for i, c in enumerate(candidates):
                out = compile_candidate_sql(
                    schema_fields=fields,
                    candidate=c,
                    query_spec=query_spec,
                    table_name="ecommerce",
                    json_column="raw_data",
                )
                compiled.append(out)
                print(f"    [{i}] name={out.get('name')!r}")
                print(f"        paths_used={out.get('paths_used')!r}")
                print(f"        issues={out.get('issues')!r}")
                sql = out.get("sql") or ""
                print(f"        sql:\n{sql}")

            # 5. rank_candidates
            ranked = rank_candidates(schema_index, compiled)
            print("\n[5] rank_candidates")
            for r in ranked:
                print(
                    f"    {r.get('name')}: score={r.get('score')} "
                    f"issues={r.get('issues')}"
                )
            print("=" * 78)
            print(
                "STEPS 1–5 SUCCEEDED CLEANLY — bug is NOT in core compile/validate "
                "when QuerySpec entries are well-formed dicts."
            )
            print("=" * 78 + "\n")
            self.assertTrue(ranked)

            # 6. Same crash shape as live: bare-string aggregations via repair patch.
            # After fix, apply_repair_patch normalizes before compile.
            print("=" * 78)
            print("STEP 6 - repair patch with bare-string aggregations -> recompile")
            print("=" * 78)
            patched_qs, patched_plan, _notes = apply_repair_patch(
                query_spec,
                plan,
                {
                    "query_spec_patch": {
                        "aggregations": [tags_path],
                    }
                },
            )
            print(f"patched aggregations: {patched_qs.get('aggregations')!r}")
            for a in patched_qs.get("aggregations") or []:
                self.assertIsInstance(a, dict)
                self.assertIn("func", a)
            out = compile_candidate_sql(
                schema_fields=fields,
                candidate=(patched_plan.get("candidates") or candidates)[0],
                query_spec=patched_qs,
                table_name="ecommerce",
                json_column="raw_data",
            )
            print(f"STEP 6 compiled ok, sql starts: {(out.get('sql') or '')[:120]!r}...")
            print("=" * 78 + "\n")
            self.assertTrue(out.get("sql"))

        except AssertionError:
            raise
        except Exception:
            print("\n*** CRASH in steps 1–5 ***")
            traceback.print_exc()
            print("=" * 78 + "\n")
            raise


@pytest.mark.live
class TestScalarArrayLiveWorkflow(unittest.TestCase):
    """Full run_workflow — needs real LLM; excluded from default pytest suite."""

    def test_live_run_workflow_tags_question(self):
        from dotenv import load_dotenv

        from workflow.graph import run_workflow

        load_dotenv()
        sample = _load_sample()

        print("\n" + "=" * 78)
        print("SCALAR ARRAY REPRO — live run_workflow()")
        print(f"question: {QUESTION!r}")
        print("=" * 78)
        print(
            "Note: intermittent — well-formed intent often skips critic "
            "(score 100). Malformed aggregations/select as bare strings "
            "crash compile_candidate_sql. Retry a few times if needed."
        )

        try:
            result = run_workflow(
                question=QUESTION,
                json_sample=sample,
                table_name="ecommerce",
                json_column="raw_data",
                max_retries=2,
            )
            ranked = result.get("ranked_candidates") or []
            state = result.get("state") or {}
            print(f"loop_exit_reason: {result.get('loop_exit_reason')}")
            print(f"retry_count: {state.get('retry_count')}")
            print(f"query_spec: {json.dumps(result.get('query_spec'), indent=2, default=str)}")
            print(f"critic_notes: {result.get('critic_notes')!r}")
            print(f"repair_notes: {state.get('repair_notes')!r}")
            if ranked:
                top = ranked[0]
                print(
                    f"top: name={top.get('name')} score={top.get('score')} "
                    f"issues={top.get('issues')}"
                )
                print(f"sql:\n{top.get('sql')}")
            else:
                print("NO ranked_candidates")
            print("=" * 78 + "\n")
            self.assertTrue(True)  # live probe — success is "didn't crash"

        except Exception:
            print("\n*** CRASH in live run_workflow ***")
            traceback.print_exc()
            print("=" * 78 + "\n")
            raise


if __name__ == "__main__":
    unittest.main()
