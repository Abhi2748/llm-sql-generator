"""
Chat correction turns: interpret → deterministic patch merge → recompile →
always re-validate → critic only when dirty. Fake LLM only.
"""

from __future__ import annotations

import json
import os
import unittest

from api.index import handle_chat
from workflow.conversation_store import (
    clear_conversation_store,
    get_conversation,
    set_conversation,
)
from workflow.graph import CRITIC_SKIP_SCORE_THRESHOLD
from workflow.nodes.chat_correction import apply_chat_correction, interpret_correction
from workflow.nodes.plan_agent import derive_candidates
from workflow.nodes.schema_index import build_schema_index_and_catalog
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates


class _Resp:
    def __init__(self, content: str):
        self.content = content


CARRIER_PATH = "ecommerce_events[*]:transaction:shipping:tracking:carrier"
METHOD_PATH = "ecommerce_events[*]:transaction:shipping:method"
EVENT_ID_PATH = "ecommerce_events[*]:event_id"
BAD_PATH = "ecommerce_events[*]:totally_unknown_field_xyz"


class ChatCorrectionFakeLLM:
    """
    Returns a critic-shaped repair that swaps shipping:method → tracking:carrier.
    Critic prompt branch increments critic_calls (should stay 0 when validation clean).
    """

    def __init__(self, *, patch_to_bad_path: bool = False):
        self.calls = []
        self.critic_calls = 0
        self.correction_calls = 0
        self.patch_to_bad_path = patch_to_bad_path

    def invoke(self, messages):
        system = messages[0].content if hasattr(messages[0], "content") else str(messages[0])
        self.calls.append(system)

        if "conversational correction" in system or "free-text correction" in system:
            self.correction_calls += 1
            new_path = BAD_PATH if self.patch_to_bad_path else CARRIER_PATH
            alias = "bad_field" if self.patch_to_bad_path else "carrier"
            return _Resp(
                json.dumps(
                    {
                        "should_retry": True,
                        "top_issues": ["Wrong shipping field selected"],
                        "repairs": {
                            "query_spec_patch": {
                                "select": [
                                    {
                                        "path": EVENT_ID_PATH,
                                        "alias": "event_id",
                                        "cast": "string",
                                    },
                                    {
                                        "path": new_path,
                                        "alias": alias,
                                        "cast": "string",
                                    },
                                ]
                            },
                            "plan_patch": None,
                        },
                        "notes": "Patched select to the corrected path.",
                    }
                )
            )

        if "strict reviewer" in system and "should_retry" in system:
            self.critic_calls += 1
            return _Resp(
                json.dumps(
                    {
                        "should_retry": False,
                        "top_issues": ["still dirty"] if self.patch_to_bad_path else [],
                        "repairs": None,
                        "notes": "critic ran",
                    }
                )
            )

        return _Resp(json.dumps({}))


def _seed_state_with_method_select(sample: dict) -> dict:
    """Build a workflow-like state whose top SQL selects shipping:method."""
    payload = build_schema_index_and_catalog(sample)
    schema_index = {
        "root_type": payload["root_type"],
        "root_array_keys": payload["root_array_keys"],
        "arrays": payload["arrays"],
        "fields": payload["fields"],
    }
    query_spec = {
        "select": [
            {"path": EVENT_ID_PATH, "alias": "event_id", "cast": "string"},
            {"path": METHOD_PATH, "alias": "shipping_carrier", "cast": "string"},
        ],
        "filters": [],
        "group_by": [],
        "aggregations": [],
        "order_by": [],
        "limit": 100,
        "grain_hint": "document",
        "notes": "",
    }
    plan = derive_candidates(schema_index, query_spec)
    compiled = [
        compile_candidate_sql(
            schema_fields=schema_index["fields"],
            candidate=c,
            query_spec=query_spec,
            table_name="ecommerce",
            json_column="raw_data",
        )
        for c in (plan.get("candidates") or [])[:3]
    ]
    ranked = rank_candidates(schema_index, compiled)
    return {
        "conversation_id": "test-seed",
        "question": "List the shipping carrier used for each event.",
        "json_sample": sample,
        "table_name": "ecommerce",
        "json_column": "raw_data",
        "schema_index": schema_index,
        "schema_summary": "ecommerce events with shipping tracking carrier",
        "field_catalog": payload["field_catalog"],
        "query_spec": query_spec,
        "plan": plan,
        "ranked_candidates": ranked,
        "validation": {
            "candidate_count": len(compiled),
            "ranked_count": len(ranked),
            "top_score": ranked[0]["score"] if ranked else None,
        },
    }


class TestConversationStore(unittest.TestCase):
    def setUp(self):
        clear_conversation_store()

    def tearDown(self):
        clear_conversation_store()

    def test_set_get_roundtrip_and_unknown(self):
        self.assertIsNone(get_conversation("missing"))
        set_conversation("c1", {"question": "q", "ranked_candidates": []})
        got = get_conversation("c1")
        self.assertEqual(got["question"], "q")
        self.assertEqual(got["ranked_candidates"], [])


class TestChatCorrection(unittest.TestCase):
    def setUp(self):
        clear_conversation_store()
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            self.sample = json.load(f)

    def tearDown(self):
        clear_conversation_store()

    def test_select_path_correction_changes_sql(self):
        state = _seed_state_with_method_select(self.sample)
        before_sql = state["ranked_candidates"][0]["sql"]
        self.assertIn("shipping:method", before_sql)

        llm = ChatCorrectionFakeLLM()
        updated = apply_chat_correction(
            state,
            "that used the wrong field, it should be the tracking carrier not the shipping method",
            llm=llm,
        )
        after_sql = (updated.get("ranked_candidates") or [{}])[0].get("sql") or ""
        self.assertNotEqual(before_sql, after_sql)
        self.assertIn("shipping:tracking:carrier", after_sql)
        self.assertNotIn("shipping:method", after_sql)
        self.assertEqual(llm.correction_calls, 1)

        # Clean re-validation should skip critic (method→carrier is a real path).
        top = updated["ranked_candidates"][0]
        self.assertGreaterEqual(top.get("score"), CRITIC_SKIP_SCORE_THRESHOLD)
        self.assertEqual(top.get("issues") or [], [])
        self.assertEqual(llm.critic_calls, 0)
        self.assertEqual(
            updated.get("critic_notes"),
            {"skipped": True, "reason": "validation_clean"},
        )

    def test_dirty_correction_triggers_critic(self):
        state = _seed_state_with_method_select(self.sample)
        llm = ChatCorrectionFakeLLM(patch_to_bad_path=True)
        updated = apply_chat_correction(
            state,
            "use a made-up field instead",
            llm=llm,
        )
        top = (updated.get("ranked_candidates") or [{}])[0]
        self.assertTrue(
            (top.get("score") or 0) < CRITIC_SKIP_SCORE_THRESHOLD
            or (top.get("issues") or []),
            msg=top,
        )
        self.assertEqual(llm.critic_calls, 1)
        self.assertNotEqual(
            updated.get("critic_notes"),
            {"skipped": True, "reason": "validation_clean"},
        )

    def test_interpret_correction_returns_critic_shaped_repairs(self):
        llm = ChatCorrectionFakeLLM()
        out = interpret_correction(
            question="List carriers",
            previous_query_spec={"select": [{"path": METHOD_PATH, "alias": "x"}]},
            previous_sql="SELECT method",
            user_message="use tracking carrier",
            llm=llm,
        )
        self.assertIn("repairs", out)
        self.assertIn("query_spec_patch", out["repairs"] or {})


class TestChatApiHandle(unittest.TestCase):
    def setUp(self):
        clear_conversation_store()
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            self.sample = json.load(f)

    def tearDown(self):
        clear_conversation_store()

    def test_follow_up_uses_store_and_returns_conversation_id(self):
        state = _seed_state_with_method_select(self.sample)
        cid = "conv-api-1"
        state["conversation_id"] = cid
        set_conversation(cid, state)

        llm = ChatCorrectionFakeLLM()
        body, status = handle_chat(
            {
                "conversation_id": cid,
                "user_message": "use tracking carrier not shipping method",
            },
            llm=llm,
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["conversation_id"], cid)
        self.assertIn("shipping:tracking:carrier", body.get("top_sql") or "")
        stored = get_conversation(cid)
        self.assertIsNotNone(stored)
        self.assertIn(
            "shipping:tracking:carrier",
            (stored.get("ranked_candidates") or [{}])[0].get("sql") or "",
        )

    def test_unknown_conversation_correction_returns_410_expired(self):
        """Cloud Run recycle / wiped store: correction must not look like a missing question."""
        body, status = handle_chat(
            {
                "conversation_id": "expired-or-never-stored",
                "question": None,
                "user_message": "use tracking carrier not shipping method",
            },
            llm=ChatCorrectionFakeLLM(),
        )
        self.assertEqual(status, 410)
        self.assertTrue(body.get("conversation_expired"))
        self.assertIn("expired", (body.get("error") or "").lower())
        self.assertNotIn("question is required", (body.get("error") or "").lower())


if __name__ == "__main__":
    unittest.main()
