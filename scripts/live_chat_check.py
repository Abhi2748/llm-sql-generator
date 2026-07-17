"""
Live end-to-end check for conversational chat correction (real LLM, no mocks).

Mirrors api/index.py's /api/chat path: fresh run_workflow → snapshot into
conversation_store → apply_chat_correction on follow-up. Not part of pytest.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from api.index import _snapshot_from_workflow  # noqa: E402  (same store shape as API)
from workflow.conversation_store import (  # noqa: E402
    clear_conversation_store,
    get_conversation,
    set_conversation,
)
from workflow.graph import run_workflow  # noqa: E402
from workflow.llm import build_chat_llm, default_llm_config  # noqa: E402
from workflow.nodes.chat_correction import apply_chat_correction  # noqa: E402

QUESTION = "List the shipping carrier used for each event."
USER_MESSAGE = (
    "That's the wrong field — I want the tracking carrier, not the shipping method."
)
SAMPLE_PATH = Path("data/sample_data.json")
TABLE_NAME = "ecommerce"
JSON_COLUMN = "raw_data"


def main() -> None:
    clear_conversation_store()
    with SAMPLE_PATH.open(encoding="utf-8") as f:
        sample = json.load(f)

    llm = build_chat_llm(default_llm_config())

    # --- Turn 1: fresh run_workflow (same call shape as api/index.py) ---
    result = run_workflow(
        question=QUESTION,
        json_sample=sample,
        table_name=TABLE_NAME,
        json_column=JSON_COLUMN,
        max_retries=2,
        llm=llm,
    )
    ranked1 = result.get("ranked_candidates") or []
    top1 = ranked1[0] if ranked1 else {}
    sql1 = top1.get("sql") or ""

    print("=== TURN 1 (fresh) ===")
    print(f"score: {top1.get('score')}")
    print(sql1)

    # Persist exactly as handle_chat's fresh-conversation branch does.
    conversation_id = str(uuid.uuid4())
    snapshot = _snapshot_from_workflow(
        conversation_id=conversation_id,
        question=QUESTION,
        json_sample=sample,
        table_name=TABLE_NAME,
        json_column=JSON_COLUMN,
        result=result,
    )
    set_conversation(conversation_id, snapshot)

    # --- Turn 2: same order/shape as handle_chat's correction branch ---
    known = get_conversation(str(conversation_id))
    assert known is not None, "conversation_store miss after set_conversation"
    updated = apply_chat_correction(known, str(USER_MESSAGE), llm=llm)
    updated["conversation_id"] = known.get("conversation_id") or conversation_id
    set_conversation(str(updated["conversation_id"]), updated)

    critique = (updated.get("chat_correction") or {}).get("critique") or {}
    repairs = critique.get("repairs")
    print("\n=== CORRECTION PATCH ===")
    print(json.dumps(repairs, indent=2, ensure_ascii=False))

    ranked2 = updated.get("ranked_candidates") or []
    top2 = ranked2[0] if ranked2 else {}
    sql2 = top2.get("sql") or ""

    print("\n=== TURN 2 (corrected) ===")
    print(f"score: {top2.get('score')}")
    print(f"issues: {top2.get('issues')}")
    print(sql2)

    has_carrier = "shipping:tracking:carrier" in sql2
    has_method = "shipping:method" in sql2
    print()
    if has_carrier and not has_method:
        print("PASS: turn 2 SQL uses shipping:tracking:carrier and not shipping:method")
    else:
        failures = []
        if not has_carrier:
            failures.append('missing "shipping:tracking:carrier"')
        if has_method:
            failures.append('still contains "shipping:method" as a selected field')
        print("FAIL: " + "; ".join(failures))


if __name__ == "__main__":
    main()
