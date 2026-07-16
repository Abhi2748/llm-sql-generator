from __future__ import annotations

from typing import Any

from ..state import WorkflowState


def normalize_json_node(state: WorkflowState) -> WorkflowState:
    """
    Normalize JSON input if needed.
    Currently passes through; kept as a node for traceability/extension.
    """
    json_sample: Any = state.get("json_sample")
    state["json_sample"] = json_sample
    return state

