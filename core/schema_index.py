from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


ScalarType = str  # "string" | "number" | "boolean" | "date" | "timestamp" | "variant"


@dataclass(frozen=True)
class Field:
    path: str  # canonical path with ":" and optional "[*]" segments
    scalar_type: ScalarType
    sample_values: Tuple[str, ...]


@dataclass(frozen=True)
class ArrayNode:
    path: str  # canonical array path ending in "[*]" or top-level array key + "[*]"


@dataclass
class SchemaIndex:
    root_type: str  # "object" | "array" | "scalar"
    fields: Dict[str, Field]  # leaf scalar fields
    arrays: Dict[str, ArrayNode]  # array nodes keyed by canonical path
    root_array_keys: Tuple[str, ...]  # top-level keys whose value is a list

    def has_field(self, path: str) -> bool:
        return path in self.fields

    def has_array(self, array_path: str) -> bool:
        return array_path in self.arrays


def _looks_like_date(value: str) -> bool:
    import re

    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value) or re.match(r"^\d{2}/\d{2}/\d{4}$", value))


def _looks_like_timestamp(value: str) -> bool:
    import re

    # ISO-ish timestamps: 2024-01-15T10:30:00Z, 2024-01-15 10:30:00
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
        # treat null as string unless we see a non-null sample later
        return "string"
    return "variant"


def _merge_type(a: ScalarType, b: ScalarType) -> ScalarType:
    if a == b:
        return a
    # If mixed numeric, keep number; else variant to avoid wrong casts
    if {a, b} <= {"number", "string"}:
        return "variant"
    return "variant"


def _join_path(prefix: str, segment: str) -> str:
    return f"{prefix}:{segment}" if prefix else segment


def _ensure_array_path(path: str) -> str:
    return path if path.endswith("[*]") else f"{path}[*]"


def _strip_array_marker(path: str) -> str:
    return path[:-3] if path.endswith("[*]") else path


def iter_canonical_nodes(obj: Any, prefix: str = "", max_array_samples: int = 3) -> Iterable[Tuple[str, Any, str]]:
    """
    Yield (canonical_path, value, node_kind) where node_kind is:
    - "array": canonical array path ending with [*], value is list
    - "leaf": canonical leaf scalar path, value is scalar
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            child = _join_path(prefix, str(k))
            yield from iter_canonical_nodes(v, child, max_array_samples=max_array_samples)
        return

    if isinstance(obj, list):
        array_path = _ensure_array_path(prefix) if prefix else "[*]"
        yield (array_path, obj, "array")
        # Sample a few elements for shape
        for i, el in enumerate(obj[:max_array_samples]):
            # In canonical representation, we analyze elements under [*]
            el_prefix = array_path
            yield from iter_canonical_nodes(el, el_prefix, max_array_samples=max_array_samples)
        return

    # Scalar leaf
    yield (prefix, obj, "leaf")


def build_schema_index(json_sample: Any) -> SchemaIndex:
    fields: Dict[str, Field] = {}
    arrays: Dict[str, ArrayNode] = {}

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

    for path, value, kind in iter_canonical_nodes(json_sample, prefix=""):
        if kind == "array":
            arrays[path] = ArrayNode(path=path)
            continue
        if kind == "leaf":
            if not path:
                continue
            scalar_type = infer_scalar_type(value)
            sample = "" if value is None else str(value)
            if path in fields:
                merged = _merge_type(fields[path].scalar_type, scalar_type)
                merged_samples = tuple(list(fields[path].sample_values) + ([sample] if sample else []))[:3]
                fields[path] = Field(path=path, scalar_type=merged, sample_values=merged_samples)
            else:
                fields[path] = Field(path=path, scalar_type=scalar_type, sample_values=(sample,) if sample else tuple())

    return SchemaIndex(
        root_type=root_type,
        fields=fields,
        arrays=arrays,
        root_array_keys=tuple(root_array_keys),
    )


def array_ancestors(path: str) -> List[str]:
    """
    Return canonical array paths that are ancestors of a canonical field path.
    Example: ecommerce_events[*]:transaction:items[*]:name -> [ecommerce_events[*], ecommerce_events[*]:transaction:items[*]]
    """
    ancestors: List[str] = []
    parts = path.split(":")
    acc = ""
    for p in parts:
        acc = _join_path(acc, p)
        if acc.endswith("[*]"):
            ancestors.append(acc)
    return ancestors


def strip_root_array_prefix(path: str, root_key: str) -> Optional[str]:
    """
    If path begins with root_key[*]:..., strip that prefix to make it relative to an event-per-row model.
    """
    prefix = f"{root_key}[*]"
    if path == prefix:
        return ""
    if path.startswith(prefix + ":"):
        return path[len(prefix) + 1 :]
    return None


def is_under_root_array(path: str, root_key: str) -> bool:
    return path == f"{root_key}[*]" or path.startswith(f"{root_key}[*]:")


def best_cast(scalar_type: ScalarType) -> str:
    if scalar_type in {"string", "number", "boolean", "date", "timestamp"}:
        return scalar_type
    return "variant"

