from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage

from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from ..state import WorkflowState


def plan_agent_node(state: WorkflowState, *, llm: LLM) -> WorkflowState:
    prompt = load_prompt("plan_agent.md")
    schema_summary = state.get("schema_summary") or ""
    query_spec = state.get("query_spec") or {}
    arrays = (state.get("schema_index") or {}).get("arrays") or []

    system = SystemMessage(content=prompt)
    user = HumanMessage(
        content=json.dumps(
            {
                "schema_summary": schema_summary,
                "arrays": arrays,
                "query_spec": query_spec,
            },
            ensure_ascii=False,
        )
    )

    resp = llm.invoke([system, user])
    text = resp.content if hasattr(resp, "content") else str(resp)
    plan = extract_json_object(text) or {}
    state["plan"] = plan
    return state

