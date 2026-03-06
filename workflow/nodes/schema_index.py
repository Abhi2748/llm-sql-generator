from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from ..state import WorkflowState


ScalarType = str


@dataclass(frozen=True)
class Field:
    path: str
    scalar_type: ScalarType
    sample_values: Tuple[str, ...]


def _looks_like_date(value: str) -> bool:
    import re

    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value) or re.match(r"^\d{2}/\d{2}/\d{4}$", value))


def _looks_like_timestamp(value: str) -> bool:
    import re

    return bool(re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", value))


def infer_scalar_type(value: Any) -> ScalarType:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) or isinstance(value, float):
        return "number"
    if isinstance(value, str):
        if _looks_like_date(value):
            return "date"
        if _looks_like_timestamp(value):
            return "timestamp"
        return "string"
    if value is None:
        return "string"
    return "variant"


def _merge_type(a: ScalarType, b: ScalarType) -> ScalarType:
    if a == b:
        return a
    return "variant"


def _join_path(prefix: str, segment: str) -> str:
    return f"{prefix}:{segment}" if prefix else segment


def _ensure_array_path(path: str) -> str:
    return path if path.endswith("[*]") else f"{path}[*]"


def _strip_star(path: str) -> str:
    return path.replace("[*]", "")


def _iter_nodes(obj: Any, prefix: str = "", max_array_samples: int = 3):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _iter_nodes(v, _join_path(prefix, str(k)), max_array_samples=max_array_samples)
        return
    if isinstance(obj, list):
        array_path = _ensure_array_path(prefix) if prefix else "[*]"
        yield (array_path, obj, "array")
        for el in obj[:max_array_samples]:
            yield from _iter_nodes(el, array_path, max_array_samples=max_array_samples)
        return
    yield (prefix, obj, "leaf")


IMPORTANT_KEYS = {
    "id",
    "event_id",
    "user_id",
    "email",
    "name",
    "timestamp",
    "event_type",
    "type",
    "category",
    "price",
    "amount",
    "total",
    "total_amount",
    "currency",
}


def _label_from_path(path: str) -> str:
    return path.split(":")[-1].replace("[*]", "")


def _importance_score(path: str) -> int:
    label = _label_from_path(path).lower()
    score = 0
    if label in IMPORTANT_KEYS:
        score += 10
    if "id" in label:
        score += 3
    if "timestamp" in label or "date" in label:
        score += 2
    if "amount" in label or "price" in label or "total" in label:
        score += 2
    score -= min(5, path.count(":"))
    return score


def build_schema_index_and_catalog(json_sample: Any, catalog_limit: int = 60) -> Dict[str, Any]:
    fields: Dict[str, Field] = {}
    arrays: List[str] = []

    root_type = "scalar"
    if isinstance(json_sample, dict):
        root_type = "object"
    elif isinstance(json_sample, list):
        root_type = "array"

    root_array_keys: List[str] = []
    if isinstance(json_sample, dict):
        for k, v in json_sample.items():
            if isinstance(v, list):
                root_array_keys.append(str(k))

    for path, value, kind in _iter_nodes(json_sample, prefix=""):
        if kind == "array":
            arrays.append(path)
            continue
        if kind == "leaf":
            if not path:
                continue
            stype = infer_scalar_type(value)
            sample = "" if value is None else str(value)
            if path in fields:
                merged = _merge_type(fields[path].scalar_type, stype)
                merged_samples = tuple(list(fields[path].sample_values) + ([sample] if sample else []))[:3]
                fields[path] = Field(path=path, scalar_type=merged, sample_values=merged_samples)
            else:
                fields[path] = Field(path=path, scalar_type=stype, sample_values=(sample,) if sample else tuple())

    # Field catalog = leaf fields only, prioritized
    ordered = sorted(fields.values(), key=lambda f: (-_importance_score(f.path), f.path))
    catalog = []
    for f in ordered[:catalog_limit]:
        sample = f.sample_values[0] if f.sample_values else ""
        catalog.append(
            {
                "path": f.path,
                "type": f.scalar_type,
                "label": _label_from_path(f.path),
                "sample": sample[:80],
            }
        )

    return {
        "root_type": root_type,
        "root_array_keys": root_array_keys,
        "arrays": sorted(set(arrays)),
        "fields": {p: {"type": fld.scalar_type, "samples": list(fld.sample_values)} for p, fld in fields.items()},
        "field_catalog": catalog,
    }


def schema_index_node(state: WorkflowState) -> WorkflowState:
    payload = build_schema_index_and_catalog(state.get("json_sample"))
    state["schema_index"] = {
        "root_type": payload["root_type"],
        "root_array_keys": payload["root_array_keys"],
        "arrays": payload["arrays"],
        "fields": payload["fields"],
    }
    state["field_catalog"] = payload["field_catalog"]
    return state

