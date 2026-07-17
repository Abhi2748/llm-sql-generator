from __future__ import annotations

import json
from typing import Any, Callable, List, Optional, Sequence

# Default model in workflow/llm.py:default_llm_config is gpt-4o-mini (128K context).
# Full GA4 pretty JSON is ~860KB ≈ ~215K tokens — exceeds the window alone — so
# baselines must subsample, not embed the whole file.
GPT_4O_MINI_CONTEXT_TOKENS = 128_000

# ~4 chars/token heuristic. Reserve ~48K tokens for system/user framing, SQL
# output, and margin → ~80K tokens for the sample ≈ 320_000 chars. Github fits
# fully; GA4 is evenly subsampled to stay under this ceiling.
MAX_JSON_SAMPLE_CHARS = 320_000


def even_subsample(items: Sequence[Any], n: int) -> List[Any]:
    """
    Keep ``n`` elements spaced evenly across ``items`` (not just a prefix).

    Same idea as schema_index's array-element cap (limit how many array rows we
    keep), but preserves diversity instead of ``items[:n]``.
    """
    total = len(items)
    if n <= 0:
        return []
    if n >= total:
        return list(items)
    if n == 1:
        return [items[total // 2]]
    indices: List[int] = []
    seen = set()
    for i in range(n):
        idx = round(i * (total - 1) / (n - 1))
        if idx not in seen:
            seen.add(idx)
            indices.append(idx)
    return [items[i] for i in indices]


def _serialize(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _fit_array_under_cap(
    arr: List[Any],
    wrap: Callable[[List[Any]], Any],
    max_chars: int,
) -> str:
    """
    Binary-search how many evenly-spaced elements fit under ``max_chars`` when
    re-serialized. Never cuts mid-element — output is always valid JSON.
    """
    if not arr:
        return _serialize(wrap([]))

    full = wrap(arr)
    full_text = _serialize(full)
    if len(full_text) <= max_chars:
        return full_text

    lo, hi = 1, len(arr)
    best_text: Optional[str] = None
    while lo <= hi:
        mid = (lo + hi) // 2
        text = _serialize(wrap(even_subsample(arr, mid)))
        if len(text) <= max_chars:
            best_text = text
            lo = mid + 1
        else:
            hi = mid - 1

    if best_text is not None:
        return best_text

    # Single element still too large with indent — try compact, then empty array.
    one = wrap(even_subsample(arr, 1))
    compact = json.dumps(one, ensure_ascii=False, separators=(",", ":"))
    if len(compact) <= max_chars:
        return compact
    return _serialize(wrap([]))


def _primary_array(obj: Any) -> tuple[Optional[List[Any]], Callable[[List[Any]], Any]]:
    """
    Locate the primary array to subsample.

    - root list → that list
    - root dict with array-valued keys → the sole array key, or the largest if
      several (GA4/github/ecommerce fixtures are single-key array roots)
    """
    if isinstance(obj, list):
        return obj, (lambda sub: sub)

    if isinstance(obj, dict):
        array_keys = [k for k, v in obj.items() if isinstance(v, list)]
        if not array_keys:
            return None, (lambda sub: obj)
        if len(array_keys) == 1:
            key = array_keys[0]
        else:
            key = max(array_keys, key=lambda k: len(obj[k]))

        def wrap(sub: List[Any], _key: str = key) -> Any:
            out = dict(obj)
            out[_key] = sub
            return out

        return list(obj[key]), wrap

    return None, (lambda sub: obj)


def format_json_sample(json_sample: Any, *, max_chars: int = MAX_JSON_SAMPLE_CHARS) -> str:
    """
    Pretty-print a JSON sample under ``max_chars``, always as valid JSON.

    When over budget, evenly subsample the primary root array (drop whole
    elements only — never mid-object character slicing).
    """
    text = _serialize(json_sample)
    if len(text) <= max_chars:
        return text

    arr, wrap = _primary_array(json_sample)
    if arr is None:
        # No array to subsample; compact form is the only structure-preserving option.
        compact = json.dumps(json_sample, ensure_ascii=False, separators=(",", ":"))
        if len(compact) <= max_chars:
            return compact
        # Still too large with no array handle — return empty object/array shape.
        if isinstance(json_sample, dict):
            return "{}"
        return "null"

    return _fit_array_under_cap(arr, wrap, max_chars)


def extract_sql(text: str) -> str:
    """
    Pull SQL out of model output. Prefers fenced ```sql blocks; otherwise
    returns the full stripped response (callers may still want raw_response).
    """
    if not text:
        return ""
    stripped = text.strip()
    lower = stripped.lower()
    fence_sql = "```sql"
    fence_generic = "```"
    if fence_sql in lower:
        start = lower.find(fence_sql) + len(fence_sql)
        end = stripped.find("```", start)
        if end == -1:
            return stripped[start:].strip()
        return stripped[start:end].strip()
    if stripped.startswith(fence_generic):
        # ``` ... ``` without language tag
        inner = stripped[3:]
        end = inner.find("```")
        if end != -1:
            body = inner[:end].strip()
            if body.lower().startswith("sql\n"):
                body = body[4:].strip()
            return body
    return stripped


def normalize_sql(sql: str) -> str:
    return " ".join((sql or "").split()).strip().rstrip(";").lower()
