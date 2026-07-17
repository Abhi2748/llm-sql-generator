"""
Eval baselines: LLM-writes-SQL architectures (no QuerySpec / graph / compiler).

Kept independent of workflow.graph and workflow.nodes so Phase 2/3 comparisons
measure architecture, not shared implementation details. May reuse workflow.llm
and prompts/ loading only.
"""

from .single_shot import generate_sql_baseline_a
from .single_shot_with_retry import generate_sql_baseline_b

__all__ = ["generate_sql_baseline_a", "generate_sql_baseline_b"]
