from __future__ import annotations

from typing import Any, Dict, Optional

from langgraph.graph import END, StateGraph

from .llm import LLM, LLMConfig, build_chat_llm, default_llm_config
from .state import WorkflowState
from .nodes.load_json import normalize_json_node
from .nodes.schema_index import schema_index_node
from .nodes.schema_summarizer_agent import schema_summarizer_node
from .nodes.intent_agent import intent_agent_node
from .nodes.plan_agent import plan_agent_node
from .nodes.sql_compiler import sql_compiler_node
from .nodes.static_validate import static_validate_node
from .nodes.critic_agent import critic_agent_node
from .nodes.repair_agent import repair_agent_node


def build_graph(*, llm: Optional[LLM] = None) -> Any:
    """
    Build the LangGraph StateGraph.
    Nodes will be added in subsequent steps (agents/validator/compiler).
    """
    if llm is None:
        raise ValueError("llm is required")

    workflow = StateGraph(WorkflowState)

    workflow.add_node("normalize_json", normalize_json_node)
    workflow.add_node("schema_index", schema_index_node)
    workflow.add_node("schema_summarizer", lambda s: schema_summarizer_node(s, llm=llm))
    workflow.add_node("intent_agent", lambda s: intent_agent_node(s, llm=llm))
    workflow.add_node("plan_agent", lambda s: plan_agent_node(s, llm=llm))
    workflow.add_node("compile_candidates", sql_compiler_node)
    workflow.add_node("static_validate", static_validate_node)
    workflow.add_node("critic_agent", lambda s: critic_agent_node(s, llm=llm))
    workflow.add_node("repair_agent", lambda s: repair_agent_node(s, llm=llm))

    workflow.set_entry_point("normalize_json")

    workflow.add_edge("normalize_json", "schema_index")
    workflow.add_edge("schema_index", "schema_summarizer")
    workflow.add_edge("schema_summarizer", "intent_agent")
    workflow.add_edge("intent_agent", "plan_agent")
    workflow.add_edge("plan_agent", "compile_candidates")
    workflow.add_edge("compile_candidates", "static_validate")
    workflow.add_edge("static_validate", "critic_agent")

    def decide_retry(state: WorkflowState) -> str:
        critic = state.get("critic_notes") or {}
        should = bool(critic.get("should_retry"))
        retry_count = int(state.get("retry_count") or 0)
        max_retries = int(state.get("max_retries") or 0)
        if should and retry_count < max_retries:
            return "retry"
        return "finalize"

    workflow.add_conditional_edges(
        "critic_agent",
        decide_retry,
        {
            "retry": "repair_agent",
            "finalize": END,
        },
    )

    # after repair, increment retry_count and go back to compile -> validate -> critic
    def increment_retry(state: WorkflowState) -> WorkflowState:
        state["retry_count"] = int(state.get("retry_count") or 0) + 1
        return state

    workflow.add_node("increment_retry", increment_retry)
    workflow.add_edge("repair_agent", "increment_retry")
    workflow.add_edge("increment_retry", "compile_candidates")

    return workflow.compile()


def run_workflow(
    *,
    question: str,
    json_sample: Any,
    table_name: str,
    json_column: str,
    max_retries: int = 2,
    llm_cfg: Optional[LLMConfig] = None,
    llm: Optional[LLM] = None,
) -> Dict[str, Any]:
    cfg = llm_cfg or default_llm_config()
    llm_instance = llm or build_chat_llm(cfg)
    graph = build_graph(llm=llm_instance)

    initial: WorkflowState = {
        "question": question,
        "json_sample": json_sample,
        "table_name": table_name,
        "json_column": json_column,
        "retry_count": 0,
        "max_retries": max_retries,
    }
    final_state = graph.invoke(initial)
    return {"state": final_state, "ranked_candidates": final_state.get("ranked_candidates"), "query_spec": final_state.get("query_spec"), "plan": final_state.get("plan"), "schema_summary": final_state.get("schema_summary"), "critic_notes": final_state.get("critic_notes")}

