from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .planner import ExecutionPlan
from .query_spec import AggregationItem, FilterItem, QuerySpec, SelectItem
from .schema_index import SchemaIndex, best_cast, strip_root_array_prefix


@dataclass(frozen=True)
class CompiledCandidate:
    name: str
    sql: str
    assumptions: Dict[str, Any]
    paths_used: List[str]  # canonical (schema) paths
    issues: List[str]


def _strip_star(path: str) -> str:
    return path.replace("[*]", "")


def _best_alias_for_array(array_path: str, is_first: bool) -> str:
    last = array_path.split(":")[-1]
    if is_first:
        return "event"
    if "items[*]" in last or last.endswith("items[*]"):
        return "item"
    if "page_views[*]" in last or last.endswith("page_views[*]"):
        return "page_view"
    if "reviews[*]" in last or last.endswith("reviews[*]"):
        return "review"
    return "v"


def _normalize_path_for_plan(path: str, plan: ExecutionPlan) -> Tuple[str, str]:
    """
    Returns (schema_path, expr_path).
    - schema_path is the original canonical path (used for type lookup)
    - expr_path is rewritten for the plan (e.g., strip root array prefix in event-per-row model)
    """
    schema_path = path
    expr_path = path
    if plan.root_strip_key:
        stripped = strip_root_array_prefix(path, plan.root_strip_key)
        if stripped is not None:
            expr_path = stripped
    return schema_path, expr_path


def _path_cast(index: SchemaIndex, schema_path: str, override_cast: Optional[str]) -> str:
    if override_cast:
        return override_cast
    f = index.fields.get(schema_path)
    if not f:
        return "variant"
    return best_cast(f.scalar_type)


def _quote_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "(" + ", ".join(_quote_sql_literal(v) for v in value) + ")"
    # string-ish
    s = str(value).replace("'", "''")
    return f"'{s}'"


def compile_to_snowflake_sql(
    index: SchemaIndex,
    plan: ExecutionPlan,
    spec: QuerySpec,
    table_name: str,
    json_column: str,
) -> CompiledCandidate:
    issues: List[str] = []
    paths_used: List[str] = []

    # Build flatten chain CTEs
    ctes: List[str] = []
    ctes.append(f"base AS (\n  SELECT t.{json_column} AS v0\n  FROM {table_name} t\n)")

    current_cte = "base"
    current_alias = "v0"
    array_alias_map: Dict[str, str] = {}

    for i, array_path in enumerate(plan.flatten_arrays):
        # Determine parent array (deepest already-flattened prefix)
        parent_array: Optional[str] = None
        for prev in plan.flatten_arrays[:i]:
            if array_path.startswith(prev + ":"):
                if parent_array is None or len(prev) > len(parent_array):
                    parent_array = prev

        parent_var = array_alias_map[parent_array] if parent_array else current_alias
        # relative path from parent
        rel = array_path
        if parent_array and array_path.startswith(parent_array + ":"):
            rel = array_path[len(parent_array) + 1 :]
        rel_no_star = _strip_star(rel)

        flatten_alias = f"f{i+1}"
        el_alias = _best_alias_for_array(array_path, is_first=(i == 0))
        # Avoid alias collisions
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
        current_alias = el_alias

    # Determine how to reference a field path (which alias + relative path)
    def expr_for_path(path: str, cast: Optional[str]) -> str:
        schema_path, expr_path = _normalize_path_for_plan(path, plan)
        paths_used.append(schema_path)

        # pick deepest array prefix present in plan
        base_alias = "v0"
        rel_path = expr_path
        best_prefix = None
        for ap in plan.flatten_arrays:
            # compare using expr form of array path too
            _, ap_expr = _normalize_path_for_plan(ap, plan)
            if rel_path == ap_expr or rel_path.startswith(ap_expr + ":"):
                if best_prefix is None or len(ap_expr) > len(best_prefix):
                    best_prefix = ap_expr
                    # map schema array path to alias (map uses schema form)
                    base_alias = array_alias_map.get(ap, base_alias)
                    rel_path = rel_path[len(ap_expr) + 1 :] if rel_path.startswith(ap_expr + ":") else ""

        if rel_path == "":
            # selecting the whole element variant
            expr = base_alias
        else:
            expr = f"{base_alias}:{_strip_star(rel_path)}"

        c = _path_cast(index, schema_path, cast)
        if c != "variant":
            expr = f"{expr}::{c}"
        return expr

    # Compile SELECT list
    select_items: List[str] = []
    select_spec: List[SelectItem] = spec.get("select", []) or []
    aggs: List[AggregationItem] = spec.get("aggregations", []) or []

    # If user asked only for aggregations, allow empty select
    for s in select_spec:
        p = s.get("path")
        if not p:
            continue
        alias = s.get("alias") or p.split(":")[-1].replace("[*]", "")
        select_items.append(f"{expr_for_path(p, s.get('cast'))} AS {alias}")

    # Aggregations
    agg_items: List[str] = []
    for a in aggs:
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
        issues.append("No select fields or aggregations inferred from the question.")
        all_select = ["v0 AS raw_variant"]

    # WHERE
    where_clauses: List[str] = []
    for f in (spec.get("filters", []) or []):
        p = f.get("path")
        op = f.get("op")
        if not p or not op:
            continue
        lhs = expr_for_path(p, f.get("cast"))
        val = f.get("value")
        if op == "eq":
            where_clauses.append(f"{lhs} = {_quote_sql_literal(val)}")
        elif op == "neq":
            where_clauses.append(f"{lhs} <> {_quote_sql_literal(val)}")
        elif op in {"gt", "gte", "lt", "lte"}:
            op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
            where_clauses.append(f"{lhs} {op_map[op]} {_quote_sql_literal(val)}")
        elif op == "contains":
            where_clauses.append(f"{lhs} ILIKE '%' || {_quote_sql_literal(val)} || '%'")
        elif op == "in":
            if not isinstance(val, (list, tuple)):
                issues.append("Filter op 'in' requires a list value.")
                continue
            where_clauses.append(f"{lhs} IN {_quote_sql_literal(list(val))}")
        else:
            issues.append(f"Unsupported filter op: {op}")

    # GROUP BY
    group_by = spec.get("group_by", []) or []
    group_exprs: List[str] = []
    # If group_by contains paths, we re-add expressions (Snowflake doesn't allow grouping by alias reliably)
    for g in group_by:
        if not isinstance(g, str):
            continue
        if ":" in g or "[*]" in g:
            group_exprs.append(expr_for_path(g, None))
        else:
            # treat as alias; if alias not found, keep raw
            group_exprs.append(g)

    # ORDER BY (by alias only)
    order_items = []
    for o in (spec.get("order_by", []) or []):
        expr_alias = o.get("expr_alias")
        if not expr_alias:
            continue
        direction = o.get("direction") or "asc"
        order_items.append(f"{expr_alias} {direction.upper()}")

    limit = spec.get("limit")
    if not isinstance(limit, int) or limit <= 0:
        limit = 100

    sql = "WITH\n  " + ",\n  ".join(ctes) + "\n"
    sql += "SELECT\n  " + ",\n  ".join(all_select) + f"\nFROM {current_cte}\n"
    if where_clauses:
        sql += "WHERE " + " AND ".join(where_clauses) + "\n"
    if group_exprs:
        sql += "GROUP BY " + ", ".join(group_exprs) + "\n"
    if order_items:
        sql += "ORDER BY " + ", ".join(order_items) + "\n"
    if not group_exprs:  # typically avoid limiting grouped queries unless specified
        sql += f"LIMIT {limit}\n"

    assumptions = {
        "row_model": plan.assumptions.row_model,
        "root_array_key": plan.assumptions.root_array_key,
        "grain": plan.assumptions.grain,
        "flatten_arrays": list(plan.flatten_arrays),
        "notes": spec.get("notes", ""),
    }

    # Deduplicate paths_used
    uniq_paths = []
    for p in paths_used:
        if p not in uniq_paths:
            uniq_paths.append(p)

    return CompiledCandidate(
        name=plan.name,
        sql=sql,
        assumptions=assumptions,
        paths_used=uniq_paths,
        issues=issues,
    )

