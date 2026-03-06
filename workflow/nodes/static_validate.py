from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ..state import WorkflowState
from .sql_compiler import array_ancestors


def rank_candidates(schema_index: Dict[str, Any], compiled: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fields = (schema_index or {}).get("fields") or {}
    ranked: List[Dict[str, Any]] = []

    for c in compiled:
        score = 100
        issues = list(c.get("issues") or [])

        paths = c.get("paths_used") or []
        unknown = [p for p in paths if p not in fields]
        if unknown:
            score -= 40
            issues.append(f"Unknown paths (not in schema sample): {unknown[:6]}")

        flattened = set((c.get("assumptions") or {}).get("flatten_arrays") or [])
        for p in paths:
            missing = [a for a in array_ancestors(p) if a not in flattened]
            if missing:
                score -= 15
                issues.append(f"Missing FLATTEN for arrays: {missing}")
                break

        sql = c.get("sql") or ""
        sql_lower = sql.lower()
        if "select *" in sql_lower:
            score -= 10
            issues.append("Uses SELECT *.")
        if "::" not in sql:
            score -= 10
            issues.append("No :: type casts found.")
        if ":" not in sql:
            score -= 20
            issues.append("No : JSON traversal found.")

        score = max(0, min(100, score))
        ranked.append({**c, "score": score, "issues": issues})

    ranked.sort(key=lambda x: (-int(x.get("score") or 0), str(x.get("name") or "")))
    return ranked


def static_validate_node(state: WorkflowState) -> WorkflowState:
    compiled = state.get("candidates") or []
    schema_index = state.get("schema_index") or {}
    ranked = rank_candidates(schema_index, compiled)

    state["ranked_candidates"] = ranked
    state["validation"] = {
        "candidate_count": len(compiled),
        "ranked_count": len(ranked),
        "top_score": ranked[0]["score"] if ranked else None,
    }
    return state

