from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

from .field_catalog import CatalogField
from .query_spec import QuerySpec, empty_query_spec


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    Robustly extract a JSON object from model output.
    Handles markdown fences and leading/trailing commentary.
    """
    # Strip markdown ```json ... ```
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    if fence:
        candidate = fence.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            pass

    # Find first {...} block
    brace = re.search(r"(\{[\s\S]*\})", text)
    if brace:
        candidate = brace.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            pass

    # Last resort: try whole string
    try:
        return json.loads(text)
    except Exception:
        return None


class IntentAgent:
    """
    LLM component that converts natural language -> QuerySpec (structured intent).
    The LLM does NOT generate SQL in this workflow.
    """

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model or os.getenv("OPENAI_MODEL") or "gpt-3.5-turbo"

        try:
            from langchain_openai import ChatOpenAI
            from langchain_core.messages import HumanMessage, SystemMessage
        except Exception as e:  # pragma: no cover
            raise RuntimeError(
                "Missing langchain_openai/langchain_core dependencies. "
                "Install requirements.txt before running."
            ) from e

        self._SystemMessage = SystemMessage
        self._HumanMessage = HumanMessage
        self.llm = ChatOpenAI(model=self.model, temperature=0.0, api_key=self.api_key)

    def to_query_spec(
        self,
        question: str,
        field_catalog: List[CatalogField],
        default_limit: int = 100,
    ) -> QuerySpec:
        spec = empty_query_spec(question)
        spec["limit"] = default_limit

        catalog_payload = [
            {"path": f.path, "type": f.scalar_type, "label": f.label, "sample": f.sample}
            for f in field_catalog
        ]

        system = f"""
You convert user questions into a STRICT JSON QuerySpec for querying JSON stored in a Snowflake VARIANT column.

Rules:
- You MUST choose field paths ONLY from the provided FieldCatalog.
- DO NOT invent new paths.
- Prefer minimal fields that answer the question.
- If the question mentions items/products/prices, set grain_hint to \"item\" and include item-level fields.
- If the question is about totals per event, use group_by + aggregations.
- Always include casts on selected/filter/aggregation paths when possible (string/number/boolean/date/timestamp).
- Output ONLY a JSON object (no markdown).

QuerySpec JSON shape (keys):
{{
  \"select\": [{{\"path\": \"...\", \"alias\": \"...\", \"cast\": \"string|number|boolean|date|timestamp|variant\"}}],
  \"filters\": [{{\"path\": \"...\", \"op\": \"eq|neq|gt|gte|lt|lte|contains|in\", \"value\": \"...\", \"cast\": \"...\"}}],
  \"group_by\": [\"<alias_or_path>\"],
  \"aggregations\": [{{\"func\": \"count|sum|avg|min|max\", \"path\": \"...\"|null, \"alias\": \"...\", \"cast\": \"...\"}}],
  \"order_by\": [{{\"expr_alias\": \"...\", \"direction\": \"asc|desc\"}}],
  \"limit\": {default_limit},
  \"grain_hint\": \"unknown|document|event|item\",
  \"notes\": \"short reasoning/assumptions\"
}}

FieldCatalog (choose from these paths):
{json.dumps(catalog_payload, ensure_ascii=False)}
""".strip()

        user = f"Question: {question}"
        messages = [self._SystemMessage(content=system), self._HumanMessage(content=user)]
        resp = self.llm.invoke(messages)
        content = resp.content if hasattr(resp, "content") else str(resp)

        obj = _extract_json_object(content)
        if not isinstance(obj, dict):
            spec["notes"] = "Could not parse LLM output as JSON QuerySpec; falling back to empty spec."
            return spec

        # Normalize fields
        for key in ("select", "filters", "group_by", "aggregations", "order_by"):
            if key not in obj or obj[key] is None:
                obj[key] = []
        if "limit" not in obj or not isinstance(obj["limit"], int):
            obj["limit"] = default_limit
        if "grain_hint" not in obj or obj["grain_hint"] not in {"unknown", "document", "event", "item"}:
            obj["grain_hint"] = "unknown"
        if "notes" not in obj or not isinstance(obj["notes"], str):
            obj["notes"] = ""

        obj["question"] = question
        return obj  # type: ignore[return-value]

