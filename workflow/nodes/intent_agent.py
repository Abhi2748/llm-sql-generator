from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage

from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from ..state import WorkflowState


def intent_agent_node(state: WorkflowState, *, llm: LLM) -> WorkflowState:
    prompt = load_prompt("intent_agent.md")
    question = state.get("question") or ""
    schema_summary = state.get("schema_summary") or ""
    field_catalog = state.get("field_catalog") or []

    system = SystemMessage(content=prompt)
    user = HumanMessage(
        content=json.dumps(
            {
                "question": question,
                "schema_summary": schema_summary,
                "field_catalog": field_catalog,
            },
            ensure_ascii=False,
        )
    )

    resp = llm.invoke([system, user])
    text = resp.content if hasattr(resp, "content") else str(resp)
    spec = extract_json_object(text) or {}
    spec["question"] = question
    state["query_spec"] = spec
    return state

