from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ..state import WorkflowState


def _strip_star(path: str) -> str:
    return path.replace("[*]", "")


def _join_path(prefix: str, segment: str) -> str:
    return f"{prefix}:{segment}" if prefix else segment


def array_ancestors(path: str) -> List[str]:
    ancestors: List[str] = []
    parts = path.split(":")
    acc = ""
    for p in parts:
        acc = _join_path(acc, p)
        if acc.endswith("[*]"):
            ancestors.append(acc)
    return ancestors


def strip_root_array_prefix(path: str, root_key: str) -> Optional[str]:
    prefix = f"{root_key}[*]"
    if path == prefix:
        return ""
    if path.startswith(prefix + ":"):
        return path[len(prefix) + 1 :]
    return None


def best_cast(field_type: str) -> str:
    if field_type in {"string", "number", "boolean", "date", "timestamp"}:
        return field_type
    return "variant"


def _path_cast(schema_fields: Dict[str, Any], schema_path: str, override: Optional[str]) -> str:
    if override:
        return override
    info = schema_fields.get(schema_path)
    if not info:
        return "variant"
    return best_cast(info.get("type") or "variant")


def _best_alias_for_array(array_path: str, is_first: bool) -> str:
    last = array_path.split(":")[-1]
    if is_first:
        return "event"
    if "items[*]" in last or last.endswith("items[*]"):
        return "item"
    if "page_views[*]" in last or last.endswith("page_views[*]"):
        return "page_view"
    if "recent_reviews[*]" in last or last.endswith("recent_reviews[*]"):
        return "review"
    return "v"


def compile_candidate_sql(
    *,
    schema_fields: Dict[str, Any],
    candidate: Dict[str, Any],
    query_spec: Dict[str, Any],
    table_name: str,
    json_column: str,
) -> Dict[str, Any]:
    """
    Deterministically compile Snowflake SQL using:
    - candidate.flatten_arrays (canonical [*] paths)
    - query_spec select/filters/group/aggregations
    """
    issues: List[str] = []
    paths_used: List[str] = []

    flatten_arrays: List[str] = candidate.get("flatten_arrays") or []
    strip_root_key = (candidate.get("path_rewrite") or {}).get("strip_root_array_key")

    # Build CTE chain
    ctes: List[str] = []
    ctes.append(f"base AS (\n  SELECT t.{json_column} AS v0\n  FROM {table_name} t\n)")

    current_cte = "base"
    base_var = "v0"
    array_alias_map: Dict[str, str] = {}

    for i, array_path in enumerate(flatten_arrays):
        parent_array: Optional[str] = None
        for prev in flatten_arrays[:i]:
            if array_path.startswith(prev + ":"):
                if parent_array is None or len(prev) > len(parent_array):
                    parent_array = prev

        parent_var = array_alias_map[parent_array] if parent_array else base_var
        rel = array_path
        if parent_array and array_path.startswith(parent_array + ":"):
            rel = array_path[len(parent_array) + 1 :]
        rel_no_star = _strip_star(rel)

        flatten_alias = f"f{i+1}"
        el_alias = _best_alias_for_array(array_path, is_first=(i == 0))
        if el_alias in array_alias_map.values():
            el_alias = f"{el_alias}{i+1}"

        cte_name = f"lvl{i+1}"
        ctes.append(
            f"""{cte_name} AS (
  SELECT
    {parent_var} AS {parent_var},
    {flatten_alias}.value AS {el_alias}
  FROM {current_cte},
  LATERAL FLATTEN(input => {parent_var}:{rel_no_star}) {flatten_alias}
)"""
        )
        array_alias_map[array_path] = el_alias
        current_cte = cte_name

    def normalize_path(path: str) -> Tuple[str, str]:
        schema_path = path
        expr_path = path
        if strip_root_key:
            stripped = strip_root_array_prefix(path, strip_root_key)
            if stripped is not None:
                expr_path = stripped
        return schema_path, expr_path

    def expr_for_path(path: str, cast: Optional[str]) -> str:
        schema_path, expr_path = normalize_path(path)
        paths_used.append(schema_path)

        base_alias = base_var
        rel_path = expr_path
        best_prefix = None
        for ap in flatten_arrays:
            _, ap_expr = normalize_path(ap)
            if rel_path == ap_expr or rel_path.startswith(ap_expr + ":"):
                if best_prefix is None or len(ap_expr) > len(best_prefix):
                    best_prefix = ap_expr
                    base_alias = array_alias_map.get(ap, base_alias)
                    rel_path = rel_path[len(ap_expr) + 1 :] if rel_path.startswith(ap_expr + ":") else ""

        if rel_path == "":
            expr = base_alias
        else:
            expr = f"{base_alias}:{_strip_star(rel_path)}"

        c = _path_cast(schema_fields, schema_path, cast)
        if c != "variant":
            expr = f"{expr}::{c}"
        return expr

    # SELECT items + aggregations
    select_items: List[str] = []
    for s in (query_spec.get("select") or []):
        p = s.get("path")
        if not p:
            continue
        alias = s.get("alias") or p.split(":")[-1].replace("[*]", "")
        select_items.append(f"{expr_for_path(p, s.get('cast'))} AS {alias}")

    agg_items: List[str] = []
    for a in (query_spec.get("aggregations") or []):
        func = (a.get("func") or "").lower()
        alias = a.get("alias") or f"{func}_value"
        if func == "count" and not a.get("path"):
            agg_items.append(f"COUNT(*) AS {alias}")
        else:
            p = a.get("path")
            if not p:
                issues.append(f"Aggregation {func} missing path")
                continue
            agg_items.append(f"{func.upper()}({expr_for_path(p, a.get('cast'))}) AS {alias}")

    all_select = select_items + agg_items
    if not all_select:
        issues.append("No select fields or aggregations inferred.")
        all_select = ["v0 AS raw_variant"]

    # WHERE (basic subset)
    where_clauses: List[str] = []
    for f in (query_spec.get("filters") or []):
        p = f.get("path")
        op = f.get("op")
        if not p or not op:
            continue
        lhs = expr_for_path(p, f.get("cast"))
        val = f.get("value")
        if isinstance(val, str):
            val_sql = "'" + val.replace("'", "''") + "'"
        elif val is None:
            val_sql = "NULL"
        else:
            val_sql = str(val)

        if op == "eq":
            where_clauses.append(f"{lhs} = {val_sql}")
        elif op == "neq":
            where_clauses.append(f"{lhs} <> {val_sql}")
        elif op == "contains":
            where_clauses.append(f"{lhs} ILIKE '%' || {val_sql} || '%'")

    group_exprs: List[str] = []
    for g in (query_spec.get("group_by") or []):
        if not isinstance(g, str):
            continue
        if ":" in g or "[*]" in g:
            group_exprs.append(expr_for_path(g, None))
        else:
            group_exprs.append(g)

    limit = query_spec.get("limit")
    if not isinstance(limit, int) or limit <= 0:
        limit = 100

    sql = "WITH\n  " + ",\n  ".join(ctes) + "\n"
    sql += "SELECT\n  " + ",\n  ".join(all_select) + f"\nFROM {current_cte}\n"
    if where_clauses:
        sql += "WHERE " + " AND ".join(where_clauses) + "\n"
    if group_exprs:
        sql += "GROUP BY " + ", ".join(group_exprs) + "\n"
    if not group_exprs:
        sql += f"LIMIT {limit}\n"

    uniq_paths: List[str] = []
    for p in paths_used:
        if p not in uniq_paths:
            uniq_paths.append(p)

    return {
        "name": candidate.get("name") or "Candidate",
        "sql": sql,
        "assumptions": {
            "row_model": candidate.get("row_model"),
            "grain": candidate.get("grain"),
            "flatten_arrays": flatten_arrays,
            "path_rewrite": candidate.get("path_rewrite") or {},
            "notes": candidate.get("notes") or "",
        },
        "paths_used": uniq_paths,
        "issues": issues,
    }


def sql_compiler_node(state: WorkflowState) -> WorkflowState:
    schema_fields = (state.get("schema_index") or {}).get("fields") or {}
    plan = state.get("plan") or {}
    candidates_plan = plan.get("candidates") or []
    query_spec = state.get("query_spec") or {}

    compiled: List[Dict[str, Any]] = []
    for cand in candidates_plan[:3]:
        compiled.append(
            compile_candidate_sql(
                schema_fields=schema_fields,
                candidate=cand,
                query_spec=query_spec,
                table_name=state.get("table_name") or "your_table",
                json_column=state.get("json_column") or "your_variant",
            )
        )
    state["candidates"] = compiled
    return state

