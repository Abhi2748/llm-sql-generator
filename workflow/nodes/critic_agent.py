from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage

from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from ..state import WorkflowState


def critic_agent_node(state: WorkflowState, *, llm: LLM) -> WorkflowState:
    prompt = load_prompt("critic_agent.md")
    schema_summary = state.get("schema_summary") or ""
    query_spec = state.get("query_spec") or {}
    ranked = state.get("ranked_candidates") or []
    validation = state.get("validation") or {}

    system = SystemMessage(content=prompt)
    user = HumanMessage(
        content=json.dumps(
            {
                "schema_summary": schema_summary,
                "query_spec": query_spec,
                "ranked_candidates": ranked[:3],
                "validation": validation,
            },
            ensure_ascii=False,
        )
    )

    resp = llm.invoke([system, user])
    text = resp.content if hasattr(resp, "content") else str(resp)
    critique = extract_json_object(text) or {}
    state["critic_notes"] = critique
    return state

