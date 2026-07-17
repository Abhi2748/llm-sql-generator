from __future__ import annotations

from typing import Any, Dict, Optional

from langchain_core.messages import HumanMessage, SystemMessage

from workflow.llm import LLM, LLMConfig, build_chat_llm, default_llm_config
from workflow.prompt_loader import load_prompt

from ._util import extract_sql, format_json_sample


def generate_sql_baseline_a(
    question: str,
    json_sample: Any,
    table_name: str,
    json_column: str,
    *,
    llm: Optional[LLM] = None,
    llm_cfg: Optional[LLMConfig] = None,
) -> Dict[str, Any]:
    """
    Baseline A: one LLM call that writes Snowflake SQL directly from the question
    + JSON sample. No schema index, QuerySpec, validation, or retry.
    """
    llm_instance = llm or build_chat_llm(llm_cfg or default_llm_config())
    system = SystemMessage(content=load_prompt("baseline_single_shot.md"))
    user = HumanMessage(
        content=(
            f"table_name: {table_name}\n"
            f"json_column: {json_column}\n\n"
            f"JSON_SAMPLE:\n{format_json_sample(json_sample)}\n\n"
            f"QUESTION:\n{question}\n"
        )
    )
    resp = llm_instance.invoke([system, user])
    raw = resp.content if hasattr(resp, "content") else str(resp)
    return {
        "sql": extract_sql(raw),
        "raw_response": raw,
        "llm_calls": 1,
    }
