from __future__ import annotations

import json

from langchain_core.messages import HumanMessage, SystemMessage

from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from ..state import WorkflowState


def repair_agent_node(state: WorkflowState, *, llm: LLM) -> WorkflowState:
    prompt = load_prompt("repair_agent.md")
    query_spec = state.get("query_spec") or {}
    plan = state.get("plan") or {}
    critic = state.get("critic_notes") or {}

    system = SystemMessage(content=prompt)
    user = HumanMessage(
        content=json.dumps(
            {
                "query_spec": query_spec,
                "plan": plan,
                "critic_repairs": critic.get("repairs"),
            },
            ensure_ascii=False,
        )
    )

    resp = llm.invoke([system, user])
    text = resp.content if hasattr(resp, "content") else str(resp)
    obj = extract_json_object(text) or {}

    if isinstance(obj.get("query_spec"), dict):
        state["query_spec"] = obj["query_spec"]
    if isinstance(obj.get("plan"), dict):
        state["plan"] = obj["plan"]
    state["repair_notes"] = obj.get("notes", "")  # type: ignore[typeddict-item]
    return state

