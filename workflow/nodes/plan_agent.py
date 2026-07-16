from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from .sql_compiler import array_ancestors, strip_root_array_prefix
from ..state import WorkflowState


def _collect_query_paths(query_spec: Dict[str, Any]) -> List[str]:
    """Gather every field path referenced by the QuerySpec."""
    paths: List[str] = []

    for s in query_spec.get("select") or []:
        if isinstance(s, dict) and s.get("path"):
            paths.append(str(s["path"]))

    for f in query_spec.get("filters") or []:
        if isinstance(f, dict) and f.get("path"):
            paths.append(str(f["path"]))

    for g in query_spec.get("group_by") or []:
        if isinstance(g, str) and g:
            paths.append(g)

    for a in query_spec.get("aggregations") or []:
        if isinstance(a, dict) and a.get("path"):
            paths.append(str(a["path"]))

    for o in query_spec.get("order_by") or []:
        if not isinstance(o, dict):
            continue
        expr_alias = o.get("expr_alias")
        if isinstance(expr_alias, str) and (":" in expr_alias or "[*]" in expr_alias):
            paths.append(expr_alias)

    return paths


def _required_arrays(paths: List[str], known_arrays: Set[str]) -> List[str]:
    """
    Arrays that any query_spec path passes through, restricted to schema-known
    array paths, ordered shallowest → deepest.
    """
    required: Set[str] = set()
    for p in paths:
        for ancestor in array_ancestors(p):
            if ancestor in known_arrays:
                required.add(ancestor)
    return sorted(required, key=lambda a: (a.count(":"), len(a), a))


def _strip_root_from_arrays(arrays: List[str], root_key: str) -> List[str]:
    """Drop the root array itself; rewrite nested paths relative to the root element."""
    out: List[str] = []
    for ap in arrays:
        stripped = strip_root_array_prefix(ap, root_key)
        if stripped is None:
            out.append(ap)
        elif stripped == "":
            continue  # the root array itself — not flattened under event_per_row
        else:
            out.append(stripped)
    return out


def derive_candidates(schema_index: Dict[str, Any], query_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministically derive 2–3 row-grain SQL candidates from schema arrays
    and the paths already chosen in query_spec. Replaces the former LLM plan agent.
    """
    known_arrays: Set[str] = set(schema_index.get("arrays") or [])
    root_keys: List[str] = list(schema_index.get("root_array_keys") or [])
    root_key: Optional[str] = root_keys[0] if root_keys else None
    root_array_path = f"{root_key}[*]" if root_key else None

    paths = _collect_query_paths(query_spec)
    required = _required_arrays(paths, known_arrays)

    # Ensure root array is present when it exists and paths go through it (or we
    # have a root array to model at all for the standard doc/event pair).
    if root_array_path and root_array_path in known_arrays:
        if root_array_path not in required:
            # Still produce doc/event candidates that flatten the root when any
            # path is under it, or when we have no path-derived arrays yet.
            under_root = any(
                p == root_array_path or p.startswith(root_array_path + ":") for p in paths
            )
            if under_root or not required:
                required = [root_array_path] + [a for a in required if a != root_array_path]

    grain_hint = (query_spec.get("grain_hint") or "unknown").lower()
    deepest = required[-1] if required else None
    nested_deeper_than_root = bool(
        root_array_path and deepest and deepest != root_array_path and deepest.startswith(root_array_path + ":")
    )

    candidates: List[Dict[str, Any]] = []

    if root_key and root_array_path:
        doc_flatten = list(required) if required else [root_array_path]
        candidates.append(
            {
                "name": "CandidateA_DocPerRow",
                "row_model": "doc_per_row",
                "grain": "event",
                "flatten_arrays": doc_flatten,
                "path_rewrite": {"strip_root_array_key": None},
                "notes": "Document-per-row: FLATTEN from the root VARIANT through required arrays.",
            }
        )
        candidates.append(
            {
                "name": "CandidateB_EventPerRow",
                "row_model": "event_per_row",
                "grain": "event",
                "flatten_arrays": _strip_root_from_arrays(required, root_key),
                "path_rewrite": {"strip_root_array_key": root_key},
                "notes": "Event-per-row: treat each row as a root-array element; strip root key from paths.",
            }
        )
        if grain_hint == "item" and nested_deeper_than_root and deepest:
            # Flatten all the way to the deepest required nested array.
            item_flatten = [a for a in required if a == root_array_path or a.startswith(root_array_path + ":")]
            if deepest not in item_flatten:
                item_flatten = list(required)
            candidates.append(
                {
                    "name": "CandidateC_ItemPerRow",
                    "row_model": "doc_per_row",
                    "grain": "item",
                    "flatten_arrays": item_flatten,
                    "path_rewrite": {"strip_root_array_key": None},
                    "notes": "Item-per-row: FLATTEN through nested item array required by grain_hint.",
                }
            )
    else:
        # No root array — single candidate with whatever arrays paths require.
        candidates.append(
            {
                "name": "CandidateA_DocPerRow",
                "row_model": "doc_per_row",
                "grain": grain_hint if grain_hint in {"document", "event", "item"} else "document",
                "flatten_arrays": list(required),
                "path_rewrite": {"strip_root_array_key": None},
                "notes": "No root array; flatten only arrays required by query_spec paths.",
            }
        )

    return {
        "candidates": candidates,
        "notes": "derived deterministically from query_spec paths and schema_index arrays",
    }


def plan_agent_node(state: WorkflowState, *, llm: Any = None) -> WorkflowState:
    """
    Deterministic planning node (LLM arg retained for graph wiring compatibility;
    it is unused — see ADR 0002).
    """
    schema_index = state.get("schema_index") or {}
    query_spec = state.get("query_spec") or {}
    state["plan"] = derive_candidates(schema_index, query_spec)
    return state
