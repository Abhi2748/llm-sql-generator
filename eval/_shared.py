"""
Shared helpers for live eval harnesses (comparison + degradation study).

Not imported by pytest unit tests by default — these scripts make real LLM calls.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Set

from baseline.single_shot import generate_sql_baseline_a
from baseline.single_shot_with_retry import generate_sql_baseline_b
from workflow.graph import run_workflow
from workflow.nodes.schema_index import build_schema_index_and_catalog

GOLDEN_DIR = os.path.join("eval", "golden")
RESULTS_DIR = os.path.join("eval", "results")
FIXTURES_DIR = os.path.join("eval", "fixtures")
JSON_COLUMN = "raw_data"

MULTI_FLATTEN_TARGET_HINTS = (
    "two_level_nested_flatten",
    "item_grain",
    "nested_array_aggregation",
    "nested_array",
)

SCORING_LIMITATION = (
    "Leaf-token substring score is approximate (case-insensitive presence of "
    "expected path leaves in SQL text). It enables cross-system comparison for "
    "baselines that have no paths_used tracking, but is NOT equivalent to the "
    "pipeline's deterministic FieldCatalog / static_validate path checks."
)

BASELINE_NO_CATCH_NOTE = (
    "Baselines have no static_validate/critic equivalent — absence of "
    "pipeline_caught_it is architectural asymmetry, not missing data."
)

# Hedge / insufficiency language (case-insensitive) for "no-caveat" checks.
CAVEAT_PATTERNS = (
    "insufficient",
    "not enough",
    "too few",
    "cannot determine",
    "can't determine",
    "unable to",
    "no matching",
    "not present",
    "missing field",
    "unknown field",
    "does not exist",
    "don't have enough",
    "do not have enough",
    "sample is too small",
    "incomplete sample",
    "cannot answer",
    "can't answer",
)


class CountingLLM:
    """Thin wrapper that counts invoke() calls against a real chat model."""

    def __init__(self, inner: Any):
        self._inner = inner
        self.calls = 0

    def invoke(self, messages: Any) -> Any:
        self.calls += 1
        return self._inner.invoke(messages)


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def leaf_token(path_or_alias: str) -> str:
    return path_or_alias.split(":")[-1].replace("[*]", "").strip()


def collect_required_tokens(expected: Dict[str, Any]) -> List[str]:
    """
    Leaf field names that should appear in SQL (deduped, ordered).

    Aggregation *aliases* (cnt, event_count, ...) are cosmetic output labels and
    are NOT required — only aggregation paths (the field being aggregated) count.
    ORDER BY aliases that merely point at an aggregation alias are skipped too.
    """
    tokens: List[str] = []
    seen: Set[str] = set()
    agg_aliases: Set[str] = set()

    def add(raw: Optional[str]) -> None:
        if not raw or not isinstance(raw, str):
            return
        tok = leaf_token(raw)
        if not tok:
            return
        key = tok.lower()
        if key in seen:
            return
        seen.add(key)
        tokens.append(tok)

    for s in expected.get("select") or []:
        if isinstance(s, str):
            add(s)
        elif isinstance(s, dict) and s.get("path"):
            add(str(s["path"]))

    for f in expected.get("filters") or []:
        if isinstance(f, dict) and f.get("path"):
            add(str(f["path"]))

    for g in expected.get("group_by") or []:
        if isinstance(g, str):
            add(g)

    for a in expected.get("aggregations") or []:
        if not isinstance(a, dict):
            continue
        if a.get("path"):
            add(str(a["path"]))
        alias = a.get("alias")
        if isinstance(alias, str) and alias:
            agg_aliases.add(alias.lower())

    for o in expected.get("order_by") or []:
        if not isinstance(o, dict):
            continue
        expr = o.get("expr_alias")
        if not isinstance(expr, str) or not expr:
            continue
        # Don't require cosmetic agg output names (cnt vs event_count).
        if expr.lower() in agg_aliases:
            continue
        add(expr)

    return tokens


def token_score(sql: str, required_tokens: List[str]) -> Dict[str, Any]:
    if not required_tokens:
        return {
            "score": None,
            "required_tokens": [],
            "matched_tokens": [],
            "missing_tokens": [],
            "note": "no required tokens in expected block",
        }
    sql_l = (sql or "").lower()
    matched: List[str] = []
    missing: List[str] = []
    for tok in required_tokens:
        if tok.lower() in sql_l:
            matched.append(tok)
        else:
            missing.append(tok)
    return {
        "score": len(matched) / len(required_tokens),
        "required_tokens": required_tokens,
        "matched_tokens": matched,
        "missing_tokens": missing,
    }


def needs_multi_flatten(targets: List[Any]) -> bool:
    joined = " ".join(str(t).lower() for t in targets)
    return any(hint in joined for hint in MULTI_FLATTEN_TARGET_HINTS)


def flatten_check(sql: str, targets: List[Any]) -> Dict[str, Any]:
    required = needs_multi_flatten(targets)
    count = len(re.findall(r"lateral\s+flatten", sql or "", flags=re.IGNORECASE))
    return {
        "required": required,
        "lateral_flatten_count": count,
        "ok": (not required) or count >= 2,
    }


def qualitative_reason(question: Dict[str, Any]) -> Optional[str]:
    if question.get("adversarial"):
        return "adversarial"
    if question.get("ambiguous"):
        return "ambiguous"
    if question.get("known_limitation"):
        return "known_limitation"
    expected = question.get("expected")
    if not isinstance(expected, dict):
        return "no_expected_block"
    return None


def table_name_for_golden(golden_path: str, golden: Dict[str, Any]) -> str:
    stem = os.path.splitext(os.path.basename(golden_path))[0]
    if stem:
        return stem
    schema_file = str(golden.get("schema_file") or "")
    base = os.path.basename(schema_file).replace("sample_data_", "").replace(
        "sample_data", "ecommerce"
    )
    return os.path.splitext(base)[0] or "unknown"


def score_sql(
    sql: str,
    expected: Optional[Dict[str, Any]],
    targets: List[Any],
    *,
    scoreable: bool,
) -> Dict[str, Any]:
    if not scoreable or not isinstance(expected, dict):
        return {
            "token_score": None,
            "flatten_check": flatten_check(sql, targets),
            "scored": False,
        }
    tokens = collect_required_tokens(expected)
    ts = token_score(sql, tokens)
    return {
        "token_score": ts["score"],
        "required_tokens": ts["required_tokens"],
        "matched_tokens": ts["matched_tokens"],
        "missing_tokens": ts["missing_tokens"],
        "flatten_check": flatten_check(sql, targets),
        "scored": True,
    }


def run_baseline_a(
    question: str,
    json_sample: Any,
    table_name: str,
    llm: CountingLLM,
) -> Dict[str, Any]:
    llm.calls = 0
    out = generate_sql_baseline_a(
        question, json_sample, table_name, JSON_COLUMN, llm=llm
    )
    return {
        "sql": out.get("sql") or "",
        "llm_calls": out.get("llm_calls") if out.get("llm_calls") is not None else llm.calls,
        "revised": None,
        "raw": out,
    }


def run_baseline_b(
    question: str,
    json_sample: Any,
    table_name: str,
    llm: CountingLLM,
) -> Dict[str, Any]:
    llm.calls = 0
    out = generate_sql_baseline_b(
        question, json_sample, table_name, JSON_COLUMN, llm=llm
    )
    return {
        "sql": out.get("sql") or "",
        "llm_calls": out.get("llm_calls") if out.get("llm_calls") is not None else llm.calls,
        "revised": bool(out.get("revised")),
        "raw": out,
    }


def run_pipeline(
    question: str,
    json_sample: Any,
    table_name: str,
    llm: CountingLLM,
) -> Dict[str, Any]:
    llm.calls = 0
    result = run_workflow(
        question=question,
        json_sample=json_sample,
        table_name=table_name,
        json_column=JSON_COLUMN,
        max_retries=2,
        llm=llm,
    )
    state = result.get("state") or {}
    ranked = result.get("ranked_candidates") or []
    top = ranked[0] if ranked else {}
    return {
        "sql": top.get("sql") or "",
        "llm_calls": llm.calls,
        "revised": None,
        "retry_count": int(state.get("retry_count") or 0),
        "static_validate_score": top.get("score"),
        "static_validate_issues": list(top.get("issues") or []),
        "paths_used": list(top.get("paths_used") or []),
        "loop_exit_reason": result.get("loop_exit_reason") or state.get("loop_exit_reason"),
        "critic_notes": result.get("critic_notes"),
        "query_spec": result.get("query_spec"),
        "raw": {
            "top_name": top.get("name"),
            "iteration_history_len": len(state.get("iteration_history") or []),
        },
    }


def progress(msg: str) -> None:
    print(msg, flush=True)


def mean(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / len(vals)


def schema_field_leaves(schema_index: Dict[str, Any]) -> Set[str]:
    leaves: Set[str] = set()
    for path in (schema_index.get("fields") or {}):
        leaf = leaf_token(str(path))
        if leaf:
            leaves.add(leaf.lower())
    return leaves


def schema_array_container_leaves(schema_index: Dict[str, Any]) -> Set[str]:
    """Bare names of array containers (events, items, ...) — not scalar fields."""
    leaves: Set[str] = set()
    for k in schema_index.get("root_array_keys") or []:
        leaf = leaf_token(str(k))
        if leaf:
            leaves.add(leaf.lower())
    for p in schema_index.get("arrays") or []:
        leaf = leaf_token(str(p))
        if leaf:
            leaves.add(leaf.lower())
    return leaves


def schema_non_leaf_container_keys(schema_index: Dict[str, Any]) -> Set[str]:
    """
    All non-leaf container/object key names from the tier schema_index.

    Includes root/nested array containers plus every intermediate path segment
    that has children (e.g. ``org`` from ``events[*]:org:login``). These are
    never entries in schema_index["fields"] and must not count as unsupported
    leaf references in overconfidence detection.
    """
    keys = set(schema_array_container_leaves(schema_index))
    for path in (schema_index.get("fields") or {}):
        parts = [p for p in str(path).replace("[*]", "").split(":") if p]
        for seg in parts[:-1]:
            keys.add(seg.lower())
    return keys


def _strip_flatten_input_clauses(sql: str) -> str:
    """
    Remove FLATTEN ``input => <expr>`` clauses so colon-path extraction does not
    treat the array being iterated (e.g. v0:events) as a selected data field.
    """
    return re.sub(
        r"input\s*=>\s*[^,)\n]+",
        "input => __FLATTEN_INPUT__",
        sql or "",
        flags=re.IGNORECASE,
    )


def extract_sql_colon_path_leaves(sql: str) -> Set[str]:
    """
    Best-effort leaf extraction from Snowflake VARIANT SQL (baseline outputs).
    Matches fragments like event:geo:country before any :: cast.

    FLATTEN input expressions are stripped first; remaining container-key
    false positives are filtered in overconfidence_flag via
    schema_non_leaf_container_keys (subsumes array-only exclusion).
    """
    scrubbed = _strip_flatten_input_clauses(sql)
    leaves: Set[str] = set()
    for m in re.finditer(r"([A-Za-z_][\w]*(?::[A-Za-z_][\w]*)+)", scrubbed):
        frag = m.group(1)
        leaf = leaf_token(frag)
        if leaf and leaf.lower() not in {
            "string",
            "number",
            "boolean",
            "variant",
            "date",
            "timestamp",
        }:
            leaves.add(leaf.lower())
    return leaves


def text_has_caveat(text: str) -> bool:
    blob = (text or "").lower()
    return any(p in blob for p in CAVEAT_PATTERNS)


def required_tokens_present_in_tier(
    required_tokens: List[str],
    tier_leaves: Set[str],
) -> Dict[str, Any]:
    """
    Whether golden-required leaf tokens exist as field leaves in THIS tier's
    schema_index. Aggregation aliases (cnt, etc.) are ignored for presence —
    only tokens that look like data fields matter; we treat aliases as optional
    by also accepting them if present, but absence of an alias alone does not
    mark the tier incomplete.
    """
    # Heuristic: skip pure aggregation-style aliases when judging tier support.
    skip = {"cnt", "count", "total", "avg", "sum", "max", "min", "event_count", "item_count"}
    data_tokens = [t for t in required_tokens if t.lower() not in skip]
    if not data_tokens:
        data_tokens = list(required_tokens)
    missing = [t for t in data_tokens if t.lower() not in tier_leaves]
    return {
        "tier_has_required_fields": len(missing) == 0,
        "missing_in_tier": missing,
        "checked_tokens": data_tokens,
    }


def overconfidence_flag(
    *,
    sql: str,
    system: str,
    run_out: Dict[str, Any],
    tier_leaves: Set[str],
    array_container_leaves: Optional[Set[str]] = None,
    non_leaf_container_keys: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    True when SQL references a leaf field absent from THIS tier's schema_index
    and the system gave a no-caveat answer.
    """
    referenced: Set[str] = set()
    paths_used = run_out.get("paths_used") or []
    if paths_used:
        for p in paths_used:
            leaf = leaf_token(str(p))
            if leaf:
                referenced.add(leaf.lower())
    referenced |= extract_sql_colon_path_leaves(sql)

    # Exclude ALL non-leaf container/object keys (arrays + intermediate objects
    # like org in org:login). These never appear in schema_index["fields"] as
    # leaves, so references like ``f.value:org IS NOT NULL`` or FLATTEN(input =>
    # v0:events) must not count as unsupported field hallucinations. This
    # subsumes the earlier array-/FLATTEN-only exclusions.
    exclude: Set[str] = set()
    if non_leaf_container_keys:
        exclude |= {x.lower() for x in non_leaf_container_keys}
    if array_container_leaves:
        exclude |= {x.lower() for x in array_container_leaves}
    if exclude:
        referenced -= exclude

    # Ignore SQL keywords / cast names that sometimes look like leaves.
    ignore = {
        "string",
        "number",
        "boolean",
        "variant",
        "date",
        "timestamp",
        "select",
        "from",
        "where",
        "group",
        "order",
        "limit",
        "as",
        "and",
        "or",
        "by",
        "desc",
        "asc",
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "null",
        "true",
        "false",
        "lateral",
        "flatten",
        "input",
        "value",
        "raw_data",
    }
    referenced = {r for r in referenced if r not in ignore}

    unsupported = sorted(r for r in referenced if r not in tier_leaves)

    caveat_bits = [sql, str(run_out.get("raw") or "")]
    qs = run_out.get("query_spec") or {}
    if isinstance(qs, dict):
        caveat_bits.append(str(qs.get("notes") or ""))
    critic = run_out.get("critic_notes") or {}
    if isinstance(critic, dict):
        caveat_bits.append(str(critic.get("notes") or ""))
        caveat_bits.extend(str(x) for x in (critic.get("top_issues") or []))
    caveat_bits.extend(str(x) for x in (run_out.get("static_validate_issues") or []))
    has_caveat = text_has_caveat("\n".join(caveat_bits))

    flagged = bool(unsupported) and (not has_caveat) and bool((sql or "").strip())
    return {
        "overconfidence_flag": flagged,
        "unsupported_leaves_in_sql": unsupported,
        "referenced_leaves": sorted(referenced),
        "has_caveat": has_caveat,
        "note": (
            None
            if system == "pipeline"
            else BASELINE_NO_CATCH_NOTE
        ),
    }


def pipeline_caught_insufficient(
    run_out: Dict[str, Any],
    *,
    overconfidence: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Did static_validate / critic / notes catch a problem for this tier?
    Only meaningful for the pipeline; baselines have no equivalent.
    """
    issues = list(run_out.get("static_validate_issues") or [])
    score = run_out.get("static_validate_score")
    score_dropped = isinstance(score, (int, float)) and score < 90
    notes_blob = "\n".join(
        [
            str((run_out.get("query_spec") or {}).get("notes") or ""),
            str((run_out.get("critic_notes") or {}).get("notes") or ""),
            "\n".join(str(x) for x in ((run_out.get("critic_notes") or {}).get("top_issues") or [])),
            "\n".join(str(x) for x in issues),
        ]
    )
    notes_flag = text_has_caveat(notes_blob)
    caught = bool(issues) or score_dropped or notes_flag or bool(
        overconfidence.get("has_caveat")
    )
    return {
        "pipeline_caught_it": caught,
        "reasons": {
            "issues_non_empty": bool(issues),
            "score_below_90": score_dropped,
            "notes_mention_insufficiency": notes_flag,
            "static_validate_score": score,
            "static_validate_issues": issues,
        },
    }
