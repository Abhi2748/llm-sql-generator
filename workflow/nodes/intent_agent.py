from __future__ import annotations

import json
from typing import Any, Dict, List

from langchain_core.messages import HumanMessage, SystemMessage

from ..llm import LLM, extract_json_object
from ..prompt_loader import load_prompt
from ..state import WorkflowState


def _alias_from_path(path: str) -> str:
    leaf = path.split(":")[-1].replace("[*]", "")
    return leaf or "value"


def normalize_query_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and normalize QuerySpec shape.

    Used after the intent LLM and after every ``apply_repair_patch`` merge so
    bare-string select/aggregation entries never reach ``compile_candidate_sql``.
    Coerces unambiguous malformations and records changes in normalization_notes.
    """
    out = dict(spec)
    notes: List[str] = list(out.get("normalization_notes") or [])

    def _norm_list(name: str, items: Any, normalize_item) -> List[Dict[str, Any]]:
        if items is None:
            return []
        if not isinstance(items, list):
            notes.append(f"{name} was {type(items).__name__}; reset to []")
            return []
        result: List[Dict[str, Any]] = []
        for i, item in enumerate(items):
            normalized = normalize_item(name, i, item)
            if normalized is not None:
                result.append(normalized)
        return result

    def _norm_select(name: str, i: int, item: Any) -> Dict[str, Any] | None:
        if isinstance(item, str) and item:
            notes.append(f"{name}[{i}]: bare string path normalized to dict")
            return {"path": item, "alias": _alias_from_path(item), "cast": None}
        if isinstance(item, dict) and item.get("path"):
            return dict(item)
        notes.append(f"{name}[{i}]: dropped invalid entry ({type(item).__name__})")
        return None

    def _norm_filter(name: str, i: int, item: Any) -> Dict[str, Any] | None:
        if isinstance(item, dict) and item.get("path") and item.get("op"):
            return dict(item)
        notes.append(f"{name}[{i}]: dropped invalid entry ({type(item).__name__})")
        return None

    def _norm_aggregation(name: str, i: int, item: Any) -> Dict[str, Any] | None:
        if isinstance(item, str) and item:
            # Mirror select: bare string is a field path. Default to count(path),
            # the unambiguous aggregation shape that always compiles.
            notes.append(f"{name}[{i}]: bare string path normalized to dict")
            alias = _alias_from_path(item) or "count_value"
            return {"func": "count", "path": item, "alias": alias, "cast": None}
        if isinstance(item, dict) and item.get("func"):
            return dict(item)
        notes.append(f"{name}[{i}]: dropped invalid entry ({type(item).__name__})")
        return None

    def _norm_order_by(name: str, i: int, item: Any) -> Dict[str, Any] | None:
        if isinstance(item, dict) and item.get("expr_alias"):
            return dict(item)
        notes.append(f"{name}[{i}]: dropped invalid entry ({type(item).__name__})")
        return None

    out["select"] = _norm_list("select", out.get("select"), _norm_select)
    out["filters"] = _norm_list("filters", out.get("filters"), _norm_filter)
    out["aggregations"] = _norm_list("aggregations", out.get("aggregations"), _norm_aggregation)
    out["order_by"] = _norm_list("order_by", out.get("order_by"), _norm_order_by)

    group_by = out.get("group_by")
    if group_by is None:
        out["group_by"] = []
    elif not isinstance(group_by, list):
        notes.append(f"group_by was {type(group_by).__name__}; reset to []")
        out["group_by"] = []
    else:
        out["group_by"] = [g for g in group_by if isinstance(g, str) and g]

    if notes:
        out["normalization_notes"] = notes
    elif "normalization_notes" in out:
        del out["normalization_notes"]

    return out


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
    state["query_spec"] = normalize_query_spec(spec)
    return state
