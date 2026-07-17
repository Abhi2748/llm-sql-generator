from __future__ import annotations

import ast
import copy
from typing import Any, Dict, List, Optional, Tuple

from ..llm import LLM
from ..state import WorkflowState
from .intent_agent import normalize_query_spec


def _coerce_in_filter_value(value: Any) -> Tuple[Optional[list], str]:
    """
    Ensure an ``in`` filter value is a real list.

    Returns (list_or_None, status) where status is ``ok`` | ``coerced`` | ``reject``.
    """
    if isinstance(value, list):
        return value, "ok"
    if isinstance(value, str):
        text = value.strip()
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError, MemoryError):
            return None, "reject"
        if isinstance(parsed, list):
            return parsed, "coerced"
        return None, "reject"
    return None, "reject"


def _sanitize_filters_in_patch(
    patch_filters: Any,
    previous_filters: List[Any],
) -> Tuple[List[Any], List[str]]:
    """
    Validate/coerce each patched filter. Malformed ``in`` values are coerced from
    Python list-literal strings when safe; otherwise that filter entry falls back
    to the previous value (or is dropped).
    """
    notes: List[str] = []
    if not isinstance(patch_filters, list):
        notes.append(
            f"Rejected query_spec_patch.filters: expected list, got {type(patch_filters).__name__}"
        )
        return list(previous_filters), notes

    sanitized: List[Any] = []
    for i, item in enumerate(patch_filters):
        if not isinstance(item, dict):
            sanitized.append(item)
            continue
        filt = dict(item)
        op = (filt.get("op") or "").lower()
        if op != "in":
            sanitized.append(filt)
            continue

        coerced, status = _coerce_in_filter_value(filt.get("value"))
        if status == "ok":
            sanitized.append(filt)
        elif status == "coerced":
            filt["value"] = coerced
            notes.append(
                f"Coerced filters[{i}] op=in value from string list-literal to list"
            )
            sanitized.append(filt)
        else:
            notes.append(
                f"Rejected filters[{i}] op=in value: expected list, got "
                f"{type(filt.get('value')).__name__}"
            )
            if i < len(previous_filters) and isinstance(previous_filters[i], dict):
                sanitized.append(copy.deepcopy(previous_filters[i]))
            # else: drop the malformed filter entry
    return sanitized, notes


def apply_repair_patch(
    query_spec: Dict[str, Any],
    plan: Dict[str, Any],
    critic_repairs: Dict[str, Any] | None,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Deterministically merge critic patches onto the current query_spec/plan.

    Patch wins for keys it specifies; existing values win for keys it omits.
    Malformed ``in`` filter values in the patch are coerced or rejected (see
    ``_sanitize_filters_in_patch``). Always runs ``normalize_query_spec`` on the
    merged result so bare-string select/aggregation entries from a patch cannot
    reach ``compile_candidate_sql`` (shared by the repair loop and chat correction).

    Returns (new_query_spec, new_plan, sanitize_notes).
    """
    repairs = critic_repairs if isinstance(critic_repairs, dict) else {}
    qs_patch = repairs.get("query_spec_patch") or {}
    plan_patch = repairs.get("plan_patch") or {}
    if not isinstance(qs_patch, dict):
        qs_patch = {}
    if not isinstance(plan_patch, dict):
        plan_patch = {}

    sanitize_notes: List[str] = []
    qs_patch = dict(qs_patch)
    if "filters" in qs_patch:
        sanitized_filters, filter_notes = _sanitize_filters_in_patch(
            qs_patch.get("filters"),
            list(query_spec.get("filters") or []),
        )
        qs_patch["filters"] = sanitized_filters
        sanitize_notes.extend(filter_notes)

    new_query_spec = normalize_query_spec({**query_spec, **qs_patch})
    new_plan = {**plan, **plan_patch}
    return new_query_spec, new_plan, sanitize_notes


def repair_agent_node(state: WorkflowState, *, llm: LLM) -> WorkflowState:
    """
    Apply critic query_spec_patch / plan_patch via shallow merge.

    ``llm`` is unused (kept for graph.py call-site compatibility). Repair is
    deterministic — see ADR 0003.
    """
    del llm  # signature retained; no LLM call
    query_spec = dict(state.get("query_spec") or {})
    plan = dict(state.get("plan") or {})
    critic = state.get("critic_notes") or {}
    repairs = critic.get("repairs") if isinstance(critic, dict) else None

    new_query_spec, new_plan, sanitize_notes = apply_repair_patch(
        query_spec, plan, repairs
    )

    qs_patch = (repairs or {}).get("query_spec_patch") if isinstance(repairs, dict) else None
    plan_patch = (repairs or {}).get("plan_patch") if isinstance(repairs, dict) else None
    qs_keys = sorted((qs_patch or {}).keys()) if isinstance(qs_patch, dict) else []
    plan_keys = sorted((plan_patch or {}).keys()) if isinstance(plan_patch, dict) else []

    state["query_spec"] = new_query_spec
    state["plan"] = new_plan
    notes = (
        f"Deterministic merge of critic patches "
        f"(query_spec keys={qs_keys}, plan keys={plan_keys})"
    )
    if sanitize_notes:
        notes += "; " + "; ".join(sanitize_notes)
    state["repair_notes"] = notes
    return state
