"""Schema-summary cache keyed by structural shape hash.

Backing store is a module-level dict: a single-process, in-memory cache.
This is intentional for the current portfolio/demo scale (see ADR 0002,
Alternatives Considered — Redis/Memcached deferred until a multi-instance
deployment needs a shared cache that survives Cloud Run cold starts).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

# Single-process in-memory cache (see module docstring / ADR 0002).
_SCHEMA_CACHE: Dict[str, Dict[str, Any]] = {}


def shape_hash(schema_index: Dict[str, Any]) -> str:
    """
    Deterministic hash of schema *shape*: sorted field paths + inferred types
    and sorted array paths. Excludes sample values so two JSON samples with
    the same structure but different data share a cache entry.
    """
    fields = schema_index.get("fields") or {}
    field_parts = []
    for path in sorted(fields.keys()):
        info = fields[path]
        ftype = info.get("type") if isinstance(info, dict) else None
        field_parts.append({"path": path, "type": ftype})

    arrays = sorted(schema_index.get("arrays") or [])
    payload = json.dumps(
        {"fields": field_parts, "arrays": arrays},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_cached_schema(shape_hash_key: str) -> Optional[Dict[str, Any]]:
    """Return {"schema_summary": str, "schema_summary_meta": dict} or None."""
    entry = _SCHEMA_CACHE.get(shape_hash_key)
    if entry is None:
        return None
    return {
        "schema_summary": entry["schema_summary"],
        "schema_summary_meta": entry["schema_summary_meta"],
    }


def set_cached_schema(
    shape_hash_key: str,
    schema_summary: str,
    schema_summary_meta: Dict[str, Any],
) -> None:
    _SCHEMA_CACHE[shape_hash_key] = {
        "schema_summary": schema_summary,
        "schema_summary_meta": schema_summary_meta,
    }


def clear_schema_cache() -> None:
    """Test helper: wipe the in-process cache."""
    _SCHEMA_CACHE.clear()
