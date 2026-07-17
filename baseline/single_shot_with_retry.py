from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from langchain_core.messages import HumanMessage, SystemMessage

from workflow.llm import LLM, LLMConfig, build_chat_llm, default_llm_config
from workflow.prompt_loader import load_prompt

from ._util import extract_sql, format_json_sample, normalize_sql
from .single_shot import generate_sql_baseline_a


def _strip_critique_markers(text: str) -> str:
    lines = (text or "").splitlines()
    while lines:
        head = lines[0].strip().upper()
        if (
            head in {"LOOKS CORRECT", "REVISED"}
            or head.startswith("LOOKS CORRECT")
            or head.startswith("REVISED")
        ):
            lines = lines[1:]
            continue
        break
    return "\n".join(lines).strip()


def _apply_self_critique(first_sql: str, critique_raw: str) -> Tuple[str, bool]:
    """
    Return (final_sql, revised).

    LOOKS CORRECT (and same / empty SQL body) keeps first_sql.
    Otherwise, a different extracted SQL body counts as a revision.
    """
    upper = (critique_raw or "").upper()
    looks_correct = "LOOKS CORRECT" in upper
    body = _strip_critique_markers(extract_sql(critique_raw))

    if looks_correct and (
        not body or normalize_sql(body) == normalize_sql(first_sql)
    ):
        return first_sql, False
    if body and normalize_sql(body) != normalize_sql(first_sql):
        return body, True
    return first_sql, False


def generate_sql_baseline_b(
    question: str,
    json_sample: Any,
    table_name: str,
    json_column: str,
    *,
    llm: Optional[LLM] = None,
    llm_cfg: Optional[LLMConfig] = None,
) -> Dict[str, Any]:
    """
    Baseline B: baseline A, then exactly one unconditional self-critique LLM call.
    No external validation, schema index, or execution feedback.
    """
    llm_instance = llm or build_chat_llm(llm_cfg or default_llm_config())
    first = generate_sql_baseline_a(
        question,
        json_sample,
        table_name,
        json_column,
        llm=llm_instance,
    )
    first_sql = first.get("sql") or ""

    system = SystemMessage(content=load_prompt("baseline_self_critique.md"))
    user = HumanMessage(
        content=(
            f"table_name: {table_name}\n"
            f"json_column: {json_column}\n\n"
            f"JSON_SAMPLE:\n{format_json_sample(json_sample)}\n\n"
            f"QUESTION:\n{question}\n\n"
            f"GENERATED_SQL:\n{first_sql}\n"
        )
    )
    resp = llm_instance.invoke([system, user])
    critique_raw = resp.content if hasattr(resp, "content") else str(resp)
    final_sql, revised = _apply_self_critique(first_sql, critique_raw)

    return {
        "sql": final_sql,
        "raw_response": critique_raw,
        "self_critique_response": critique_raw,
        "revised": revised,
        "llm_calls": 2,
        "first_sql": first_sql,
        "first_raw_response": first.get("raw_response"),
    }
