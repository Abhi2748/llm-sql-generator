from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

from langgraph.graph import END, StateGraph

from .llm import LLM, LLMConfig, build_chat_llm, default_llm_config
from .state import WorkflowState
from .schema_cache import get_cached_schema, set_cached_schema, shape_hash
from .nodes.load_json import normalize_json_node
from .nodes.schema_index import schema_index_node
from .nodes.schema_summarizer_agent import schema_summarizer_node
from .nodes.intent_agent import intent_agent_node
from .nodes.plan_agent import plan_agent_node
from .nodes.sql_compiler import sql_compiler_node
from .nodes.static_validate import static_validate_node
from .nodes.critic_agent import critic_agent_node
from .nodes.repair_agent import repair_agent_node

# Skip the LLM critic when static validation already confirms a clean top candidate.
CRITIC_SKIP_SCORE_THRESHOLD = 90


def _decide_critic_branch(state: WorkflowState) -> str:
    validation = state.get("validation") or {}
    top_score = validation.get("top_score")
    ranked = state.get("ranked_candidates") or []
    top_issues = (ranked[0].get("issues") or []) if ranked else ["no candidates"]
    if (
        isinstance(top_score, (int, float))
        and top_score >= CRITIC_SKIP_SCORE_THRESHOLD
        and not top_issues
    ):
        return "finalize"
    return "critic"


def _decide_retry_branch(state: WorkflowState) -> str:
    critic = state.get("critic_notes") or {}
    should = bool(critic.get("should_retry"))
    retry_count = int(state.get("retry_count") or 0)
    max_retries = int(state.get("max_retries") or 0)
    if should and retry_count < max_retries:
        return "retry"
    if should and retry_count >= max_retries:
        return "finalize_max_retries"
    return "finalize_no_retry"


def _append_history(state: WorkflowState, entry: Dict[str, Any]) -> None:
    history: List[Dict[str, Any]] = list(state.get("iteration_history") or [])
    history.append(entry)
    state["iteration_history"] = history  # type: ignore[typeddict-item]


def build_graph(*, llm: Optional[LLM] = None) -> Any:
    """
    Build the LangGraph StateGraph.
    Nodes will be added in subsequent steps (agents/validator/compiler).
    """
    if llm is None:
        raise ValueError("llm is required")

    workflow = StateGraph(WorkflowState)

    def schema_summarizer_cached(state: WorkflowState) -> WorkflowState:
        schema_index = state.get("schema_index") or {}
        key = shape_hash(schema_index)
        cached = get_cached_schema(key)
        if cached is not None:
            state["schema_summary"] = cached["schema_summary"]
            state["schema_summary_meta"] = cached["schema_summary_meta"]
            return state
        state = schema_summarizer_node(state, llm=llm)
        set_cached_schema(
            key,
            state.get("schema_summary") or "",
            state.get("schema_summary_meta") or {},
        )
        return state

    def static_validate_with_history(state: WorkflowState) -> WorkflowState:
        state = static_validate_node(state)
        ranked = state.get("ranked_candidates") or []
        top = ranked[0] if ranked else {}
        branch = _decide_critic_branch(state)
        if branch == "finalize":
            state["loop_exit_reason"] = "validation_clean_skip_critic"
            # Drop stale critic output from an earlier dirty pass so final state
            # does not mix a skipped-critic ranked snapshot with old critic_notes.
            state["critic_notes"] = {
                "skipped": True,
                "reason": "validation_clean",
            }
        _append_history(
            state,
            {
                "phase": "static_validate",
                "retry_count": int(state.get("retry_count") or 0),
                "decide_critic_branch": branch,
                "query_spec": copy.deepcopy(state.get("query_spec") or {}),
                "top_name": top.get("name"),
                "top_score": top.get("score"),
                "top_issues": list(top.get("issues") or []),
                "top_sql": top.get("sql"),
                "top_select_aliases": list(top.get("select_aliases") or []),
                "validation_top_score": (state.get("validation") or {}).get("top_score"),
            },
        )
        return state

    def critic_with_history(state: WorkflowState) -> WorkflowState:
        state = critic_agent_node(state, llm=llm)
        retry_branch = _decide_retry_branch(state)
        if retry_branch == "retry":
            exit_reason = "continuing_to_repair"
        elif retry_branch == "finalize_max_retries":
            exit_reason = "hit_max_retries"
            state["loop_exit_reason"] = exit_reason
            state["retry_exhausted_unresolved"] = True
            # Surface distinctly from a first-pass-clean result even if the last
            # compile looks tidy (e.g. invalid filter silently dropped).
            marker = (
                "Retry exhausted without resolving critic issues "
                "(retry_exhausted_unresolved)"
            )
            ranked = list(state.get("ranked_candidates") or [])
            marked: List[Dict[str, Any]] = []
            for c in ranked:
                issues = list(c.get("issues") or [])
                if marker not in issues:
                    issues.append(marker)
                marked.append({**c, "issues": issues, "retry_exhausted_unresolved": True})
            state["ranked_candidates"] = marked  # type: ignore[typeddict-item]
        else:
            exit_reason = "critic_declined_retry"
            state["loop_exit_reason"] = exit_reason
        critic = state.get("critic_notes") or {}
        repairs = critic.get("repairs") if isinstance(critic.get("repairs"), dict) else {}
        ranked = state.get("ranked_candidates") or []
        ranked_top = ranked[0] if ranked else {}
        _append_history(
            state,
            {
                "phase": "critic_agent",
                "retry_count": int(state.get("retry_count") or 0),
                "decide_retry_branch": (
                    "retry" if retry_branch == "retry" else "finalize"
                ),
                "decide_retry_detail": retry_branch,
                "loop_exit_reason": exit_reason,
                "should_retry": bool(critic.get("should_retry")),
                "critic_top_issues": list(critic.get("top_issues") or []),
                "query_spec_patch": copy.deepcopy(repairs.get("query_spec_patch")),
                "plan_patch": copy.deepcopy(repairs.get("plan_patch")),
                # Snapshot ranked top at critic time for staleness checks.
                "ranked_top_score": ranked_top.get("score"),
                "ranked_top_issues": list(ranked_top.get("issues") or []),
                "ranked_top_sql": ranked_top.get("sql"),
            },
        )
        return state

    workflow.add_node("normalize_json", normalize_json_node)
    workflow.add_node("schema_index", schema_index_node)
    workflow.add_node("schema_summarizer", schema_summarizer_cached)
    workflow.add_node("intent_agent", lambda s: intent_agent_node(s, llm=llm))
    workflow.add_node("plan_agent", lambda s: plan_agent_node(s, llm=llm))
    workflow.add_node("compile_candidates", sql_compiler_node)
    workflow.add_node("static_validate", static_validate_with_history)
    workflow.add_node("critic_agent", critic_with_history)
    workflow.add_node("repair_agent", lambda s: repair_agent_node(s, llm=llm))

    workflow.set_entry_point("normalize_json")

    workflow.add_edge("normalize_json", "schema_index")
    workflow.add_edge("schema_index", "schema_summarizer")
    workflow.add_edge("schema_summarizer", "intent_agent")
    workflow.add_edge("intent_agent", "plan_agent")
    workflow.add_edge("plan_agent", "compile_candidates")
    workflow.add_edge("compile_candidates", "static_validate")

    def decide_critic(state: WorkflowState) -> str:
        return _decide_critic_branch(state)

    workflow.add_conditional_edges(
        "static_validate",
        decide_critic,
        {
            "critic": "critic_agent",
            "finalize": END,
        },
    )

    def decide_retry(state: WorkflowState) -> str:
        branch = _decide_retry_branch(state)
        return "retry" if branch == "retry" else "finalize"

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
        _append_history(
            state,
            {
                "phase": "repair_applied",
                "retry_count": int(state.get("retry_count") or 0),
                "repair_notes": state.get("repair_notes"),
                "query_spec": copy.deepcopy(state.get("query_spec") or {}),
                "plan": copy.deepcopy(state.get("plan") or {}),
            },
        )
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
        "iteration_history": [],
    }
    final_state = graph.invoke(initial)
    return {
        "state": final_state,
        "ranked_candidates": final_state.get("ranked_candidates"),
        "query_spec": final_state.get("query_spec"),
        "plan": final_state.get("plan"),
        "schema_summary": final_state.get("schema_summary"),
        "critic_notes": final_state.get("critic_notes"),
        "iteration_history": final_state.get("iteration_history"),
        "loop_exit_reason": final_state.get("loop_exit_reason"),
        "retry_exhausted_unresolved": bool(
            final_state.get("retry_exhausted_unresolved")
        ),
    }
