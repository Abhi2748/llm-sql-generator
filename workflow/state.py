from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class WorkflowState(TypedDict, total=False):
    # Inputs
    question: str
    json_sample: Any
    table_name: str
    json_column: str

    # Deterministic schema artifacts
    schema_index: Dict[str, Any]
    field_catalog: List[Dict[str, Any]]

    # LLM artifacts
    schema_summary: str
    schema_summary_meta: Dict[str, Any]
    query_spec: Dict[str, Any]
    plan: Dict[str, Any]

    # Candidate SQL + evaluation
    candidates: List[Dict[str, Any]]
    validation: Dict[str, Any]
    ranked_candidates: List[Dict[str, Any]]

    # Critique/repair loop
    critic_notes: Dict[str, Any]
    repair_notes: str
    retry_count: int
    max_retries: int

    # Final
    final: Dict[str, Any]

