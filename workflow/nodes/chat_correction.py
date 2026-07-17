from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage

from ..graph import CRITIC_SKIP_SCORE_THRESHOLD
from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from .critic_agent import critic_agent_node
from .plan_agent import derive_candidates
from .repair_agent import apply_repair_patch
from .sql_compiler import compile_candidate_sql
from .static_validate import rank_candidates


def interpret_correction(
    question: str,
    previous_query_spec: Dict[str, Any],
    previous_sql: str,
    user_message: str,
    llm: LLM,
) -> Dict[str, Any]:
    """
    One LLM call: map a free-text user correction onto a critic-shaped repair
    payload (``repairs.query_spec_patch`` / ``plan_patch``).
    """
    prompt = load_prompt("chat_correction.md")
    system = SystemMessage(content=prompt)
    user = HumanMessage(
        content=json.dumps(
            {
                "question": question,
                "previous_query_spec": previous_query_spec,
                "previous_sql": previous_sql,
                "user_message": user_message,
            },
            ensure_ascii=False,
        )
    )
    resp = llm.invoke([system, user])
    text = resp.content if hasattr(resp, "content") else str(resp)
    return extract_json_object(text) or {}


def apply_chat_correction(
    state: Dict[str, Any],
    user_message: str,
    *,
    llm: LLM,
) -> Dict[str, Any]:
    """
    Apply a human correction turn: interpret → deterministic patch merge →
    recompile → always re-validate → critic only if dirty (ADR 0002 threshold).
    """
    question = state.get("question") or ""
    previous_query_spec = dict(state.get("query_spec") or {})
    previous_plan = dict(state.get("plan") or {})
    ranked_prev = state.get("ranked_candidates") or []
    previous_sql = (ranked_prev[0].get("sql") if ranked_prev else "") or ""

    critique = interpret_correction(
        question=question,
        previous_query_spec=previous_query_spec,
        previous_sql=previous_sql,
        user_message=user_message,
        llm=llm,
    )
    repairs = critique.get("repairs") if isinstance(critique.get("repairs"), dict) else {}

    new_query_spec, new_plan, sanitize_notes = apply_repair_patch(
        previous_query_spec, previous_plan, repairs
    )

    schema_index = state.get("schema_index") or {}
    # Refresh candidates from the patched QuerySpec (deterministic; free), then
    # re-apply any plan_patch keys so critic-shaped flatten hints still win.
    derived = derive_candidates(schema_index, new_query_spec)
    plan_patch = repairs.get("plan_patch") if isinstance(repairs, dict) else None
    if isinstance(plan_patch, dict) and plan_patch:
        new_plan = {**derived, **plan_patch}
        # Preserve derived candidates unless the patch explicitly replaces them.
        if "candidates" not in plan_patch:
            new_plan["candidates"] = derived.get("candidates") or []
    else:
        new_plan = derived

    schema_fields = (schema_index.get("fields") or {}) if isinstance(schema_index, dict) else {}
    table_name = state.get("table_name") or "your_table"
    json_column = state.get("json_column") or "your_variant"
    candidates_plan = list(new_plan.get("candidates") or [])[:3]

    compiled: List[Dict[str, Any]] = []
    for cand in candidates_plan:
        compiled.append(
            compile_candidate_sql(
                schema_fields=schema_fields,
                candidate=cand,
                query_spec=new_query_spec,
                table_name=table_name,
                json_column=json_column,
            )
        )

    # Always re-validate — free, and consistent with every other candidate path.
    ranked = rank_candidates(schema_index, compiled)
    top = ranked[0] if ranked else {}
    top_score = top.get("score")
    top_issues = list(top.get("issues") or []) if ranked else ["no candidates"]
    validation = {
        "candidate_count": len(compiled),
        "ranked_count": len(ranked),
        "top_score": top_score,
    }

    state = dict(state)
    state["query_spec"] = new_query_spec
    state["plan"] = new_plan
    state["candidates"] = compiled
    state["ranked_candidates"] = ranked
    state["validation"] = validation
    state["repair_notes"] = (
        f"Chat correction merge"
        + (f"; {'; '.join(sanitize_notes)}" if sanitize_notes else "")
    )
    state["chat_correction"] = {
        "user_message": user_message,
        "critique": critique,
        "sanitize_notes": sanitize_notes,
    }

    needs_critic = not (
        isinstance(top_score, (int, float))
        and top_score >= CRITIC_SKIP_SCORE_THRESHOLD
        and not top_issues
    )
    if needs_critic:
        state = critic_agent_node(state, llm=llm)  # type: ignore[arg-type]
    else:
        state["critic_notes"] = {"skipped": True, "reason": "validation_clean"}

    return state
