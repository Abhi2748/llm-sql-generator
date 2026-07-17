from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from ..state import WorkflowState
from .sql_compiler import array_ancestors


def _normalize_path_key(path: str) -> str:
    """Strip [*] markers so path comparisons ignore flatten-array notation."""
    return path.replace("[*]", "")


def _path_compare_keys(path: str) -> Set[str]:
    """
    Keys used for path equality, matching how this file already compares
    group_by entries to select paths/aliases (full path and leaf / colon tail).
    """
    if not path:
        return set()
    normalized = _normalize_path_key(path)
    keys = {path, normalized}
    leaf = normalized.split(":")[-1] if normalized else ""
    if leaf:
        keys.add(leaf)
    # Also accept trailing colon-segment suffixes (e.g. items:price vs full path).
    parts = [p for p in normalized.split(":") if p]
    for i in range(len(parts)):
        keys.add(":".join(parts[i:]))
    return keys


def _agg_path_matches_group_by(agg_path: str, group_by_declared: List[str]) -> bool:
    agg_keys = _path_compare_keys(agg_path)
    for g in group_by_declared:
        if not isinstance(g, str) or not g:
            continue
        if agg_keys & _path_compare_keys(g):
            return True
    return False


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

        select_aliases: List[str] = list(c.get("select_aliases") or [])
        select_exprs: List[str] = list(c.get("select_exprs") or [])
        select_paths: List[str] = list(c.get("select_paths") or [])
        agg_aliases: List[str] = list(c.get("agg_aliases") or [])
        agg_items: List[str] = list(c.get("agg_items") or [])
        group_exprs: List[str] = list(c.get("group_exprs") or [])
        group_by_declared: List[str] = list(c.get("group_by_declared") or [])
        aggregations_declared: List[Dict[str, Any]] = list(c.get("aggregations_declared") or [])

        # Duplicate aliases across SELECT + aggregations (e.g. raw max_price + MAX(...) AS max_price).
        seen_aliases: Dict[str, int] = {}
        for alias in select_aliases + agg_aliases:
            seen_aliases[alias] = seen_aliases.get(alias, 0) + 1
        for alias, count in seen_aliases.items():
            if count > 1:
                score -= 20
                issues.append(
                    f"Duplicate alias '{alias}' used for two different SELECT expressions"
                )

        # QuerySpec inconsistency: raw SELECT columns not declared in group_by when aggregating.
        if agg_items:
            group_by_set = set(group_by_declared)
            group_expr_set = set(group_exprs)
            agg_alias_set = set(agg_aliases)
            for i, alias in enumerate(select_aliases):
                if alias in agg_alias_set:
                    continue
                path = select_paths[i] if i < len(select_paths) else ""
                expr = select_exprs[i] if i < len(select_exprs) else ""
                declared = (
                    alias in group_by_set
                    or path in group_by_set
                    or alias in group_expr_set
                    or (expr and expr in group_expr_set)
                )
                if not declared:
                    score -= 35
                    issues.append(
                        f"SELECT item '{alias}' is not aggregated and not part of the "
                        f"declared grouping - QuerySpec inconsistency"
                    )

        # Self-referential aggregation: MIN/MAX/SUM/AVG on a field that is also GROUP BY'd
        # is a no-op (within each group the field is constant). COUNT(field) GROUP BY field
        # is a legitimate idiom and must not be flagged.
        for agg in aggregations_declared:
            func = (agg.get("func") or "").lower()
            agg_path: Optional[str] = agg.get("path")
            if func == "count" or not isinstance(agg_path, str) or not agg_path:
                continue
            if _agg_path_matches_group_by(agg_path, group_by_declared):
                score -= 35
                issues.append(
                    f"Aggregation '{func}' on '{agg_path}' is self-referential with "
                    f"GROUP BY on the same field - this aggregation is a no-op"
                )

        # Fan-out / double-counting: SUM/AVG/MIN/MAX of an ancestor-level field while
        # flattened to a deeper child grain multiplies the value once per child row.
        # Natural grain = array-ancestor depth of the aggregated path; candidate depth =
        # number of LATERAL FLATTEN arrays. COUNT is excluded (cardinality, not value).
        flatten_list = list(
            (c.get("assumptions") or {}).get("flatten_arrays")
            or c.get("flatten_arrays")
            or []
        )
        candidate_flatten_depth = len(flatten_list)
        for agg in aggregations_declared:
            func = (agg.get("func") or "").lower()
            agg_path = agg.get("path")
            if func not in {"sum", "avg", "min", "max"}:
                continue
            if not isinstance(agg_path, str) or not agg_path:
                continue
            natural_grain_depth = len(array_ancestors(agg_path))
            if natural_grain_depth < candidate_flatten_depth:
                score -= 35
                issues.append(
                    f"Aggregation '{func}' on '{agg_path}' is at a shallower grain "
                    f"than the candidate's flatten depth - this will multiply the "
                    f"value once per child row (fan-out)"
                )

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
