from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .query_spec import QuerySpec
from .schema_index import SchemaIndex, array_ancestors, is_under_root_array, strip_root_array_prefix


@dataclass(frozen=True)
class PlanAssumptions:
    row_model: str  # "doc_per_row" | "event_per_row"
    root_array_key: Optional[str]
    grain: str  # "event" | "item" | "document"


@dataclass(frozen=True)
class ExecutionPlan:
    name: str
    assumptions: PlanAssumptions
    # canonical array paths to flatten in order (relative to the current variant alias)
    flatten_arrays: Tuple[str, ...]
    # path rewriter: if set, we remove a leading root-array prefix for event-per-row mode
    root_strip_key: Optional[str]


def _collect_paths(spec: QuerySpec) -> List[str]:
    paths: List[str] = []
    for s in spec.get("select", []):
        if isinstance(s, dict) and s.get("path"):
            paths.append(str(s["path"]))
    for f in spec.get("filters", []):
        if isinstance(f, dict) and f.get("path"):
            paths.append(str(f["path"]))
    for a in spec.get("aggregations", []):
        if isinstance(a, dict) and a.get("path"):
            paths.append(str(a["path"]))
    # group_by may contain aliases or paths; we only add those that look like paths
    for g in spec.get("group_by", []):
        if isinstance(g, str) and (":" in g or "[*]" in g):
            paths.append(g)
    return paths


def _normalize_for_event_per_row(paths: Sequence[str], root_key: str) -> List[str]:
    out: List[str] = []
    for p in paths:
        stripped = strip_root_array_prefix(p, root_key)
        out.append(stripped if stripped is not None else p)
    return out


def build_candidate_plans(index: SchemaIndex, spec: QuerySpec) -> List[ExecutionPlan]:
    """
    Produce 2–3 plans:
    - CandidateA: doc-per-row, flatten the top-level root array key (if present)
    - CandidateB: event-per-row, no root flatten (strip root key from paths)
    - CandidateC: item-level (when items array is referenced), flatten items in addition to the root/event plan
    """
    raw_paths = _collect_paths(spec)
    root_key = index.root_array_keys[0] if index.root_array_keys else None

    # Determine if spec references item-level arrays
    mentions_item = spec.get("grain_hint") == "item"
    for p in raw_paths:
        if "items[*]" in p or p.endswith(":items[*]") or ":items[*]:" in p:
            mentions_item = True

    plans: List[ExecutionPlan] = []

    # CandidateA: doc-per-row with root array flatten (when the JSON sample is a dict containing a top-level array)
    if root_key:
        # if user didn't reference root paths, still include because it often is the right model
        paths_a = raw_paths
        flatten_a: List[str] = [f"{root_key}[*]"]

        # Determine additional array ancestors (excluding the root itself)
        for p in paths_a:
            if is_under_root_array(p, root_key):
                for anc in array_ancestors(p):
                    if anc != f"{root_key}[*]" and anc not in flatten_a:
                        flatten_a.append(anc)

        grain = "item" if mentions_item else "event"
        plans.append(
            ExecutionPlan(
                name="CandidateA_DocPerRow",
                assumptions=PlanAssumptions(row_model="doc_per_row", root_array_key=root_key, grain=grain),
                flatten_arrays=tuple(flatten_a if grain == "item" else flatten_a),
                root_strip_key=None,
            )
        )

    # CandidateB: event-per-row (strip root key when present)
    if root_key:
        paths_b = _normalize_for_event_per_row(raw_paths, root_key)
    else:
        paths_b = raw_paths

    flatten_b: List[str] = []
    for p in paths_b:
        for anc in array_ancestors(p):
            if anc not in flatten_b:
                flatten_b.append(anc)

    grain_b = "item" if mentions_item else "event"
    plans.append(
        ExecutionPlan(
            name="CandidateB_EventPerRow",
            assumptions=PlanAssumptions(row_model="event_per_row", root_array_key=root_key, grain=grain_b),
            flatten_arrays=tuple(flatten_b),
            root_strip_key=root_key,
        )
    )

    # CandidateC: force item-level flatten if we see items referenced (for both root models)
    if mentions_item:
        # Add an item-focused plan for doc-per-row if root exists, else for event-per-row only
        if root_key:
            plans.append(
                ExecutionPlan(
                    name="CandidateC_DocPerRow_ItemGrain",
                    assumptions=PlanAssumptions(row_model="doc_per_row", root_array_key=root_key, grain="item"),
                    flatten_arrays=tuple([f"{root_key}[*]", f"{root_key}[*]:transaction:items[*]"]),
                    root_strip_key=None,
                )
            )
        plans.append(
            ExecutionPlan(
                name="CandidateC_EventPerRow_ItemGrain",
                assumptions=PlanAssumptions(row_model="event_per_row", root_array_key=root_key, grain="item"),
                flatten_arrays=tuple(["transaction:items[*]"]),
                root_strip_key=root_key,
            )
        )

    # Deduplicate by (row_model, flatten_arrays)
    uniq: Dict[Tuple[str, Tuple[str, ...]], ExecutionPlan] = {}
    for p in plans:
        key = (p.assumptions.row_model, p.flatten_arrays)
        if key not in uniq:
            uniq[key] = p
    return list(uniq.values())

