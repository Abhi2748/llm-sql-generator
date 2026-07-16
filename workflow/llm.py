from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


class LLM(Protocol):
    def invoke(self, messages: Any) -> Any: ...


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    Robustly extract a JSON object from model output.
    Handles markdown fences and leading/trailing commentary.
    """
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    if fence:
        candidate = fence.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            pass

    brace = re.search(r"(\{[\s\S]*\})", text)
    if brace:
        candidate = brace.group(1)
        try:
            return json.loads(candidate)
        except Exception:
            pass

    try:
        return json.loads(text)
    except Exception:
        return None


@dataclass(frozen=True)
class LLMConfig:
    api_key: Optional[str]
    model: str
    temperature: float = 0.0


def build_chat_llm(cfg: LLMConfig) -> LLM:
    try:
        from langchain_openai import ChatOpenAI
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Missing langchain-openai; install requirements.txt") from e

    return ChatOpenAI(model=cfg.model, temperature=cfg.temperature, api_key=cfg.api_key)


def default_llm_config() -> LLMConfig:
    return LLMConfig(
        api_key=os.getenv("OPENAI_API_KEY"),
        model=os.getenv("OPENAI_MODEL") or "gpt-4o-mini",
        temperature=float(os.getenv("OPENAI_TEMPERATURE") or "0"),
    )

