from __future__ import annotations

import json
from typing import Any, Dict

from langchain_core.messages import HumanMessage, SystemMessage

from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from ..state import WorkflowState


def schema_summarizer_node(state: WorkflowState, *, llm: LLM) -> WorkflowState:
    prompt = load_prompt("schema_summarizer.md")
    schema_index = state.get("schema_index") or {}

    system = SystemMessage(content=prompt)
    user = HumanMessage(
        content="SCHEMA_INDEX_JSON:\n" + json.dumps(schema_index, ensure_ascii=False, indent=2)
    )

    resp = llm.invoke([system, user])
    text = resp.content if hasattr(resp, "content") else str(resp)
    obj = extract_json_object(text) or {}

    state["schema_summary"] = str(obj.get("schema_summary") or "")
    state["schema_summary_meta"] = obj  # type: ignore[typeddict-item]
    return state

