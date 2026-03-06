from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict


GrainHint = Literal["unknown", "document", "event", "item"]


class SelectItem(TypedDict, total=False):
    path: str  # canonical path from field catalog
    alias: str
    cast: str  # string|number|boolean|date|timestamp|variant


class FilterItem(TypedDict, total=False):
    path: str
    op: Literal["eq", "neq", "gt", "gte", "lt", "lte", "contains", "in"]
    value: Any
    cast: str


class AggregationItem(TypedDict, total=False):
    func: Literal["count", "sum", "avg", "min", "max"]
    path: Optional[str]  # None allowed for count(*)
    alias: str
    cast: str


class OrderByItem(TypedDict, total=False):
    expr_alias: str
    direction: Literal["asc", "desc"]


class QuerySpec(TypedDict, total=False):
    question: str
    select: List[SelectItem]
    filters: List[FilterItem]
    group_by: List[str]  # list of paths OR select aliases (we will normalize to aliases)
    aggregations: List[AggregationItem]
    order_by: List[OrderByItem]
    limit: int
    grain_hint: GrainHint
    notes: str


def empty_query_spec(question: str) -> QuerySpec:
    return {
        "question": question,
        "select": [],
        "filters": [],
        "group_by": [],
        "aggregations": [],
        "order_by": [],
        "limit": 100,
        "grain_hint": "unknown",
        "notes": "",
    }

