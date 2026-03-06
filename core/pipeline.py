from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .field_catalog import CatalogField, build_field_catalog
from .intent_agent import IntentAgent
from .planner import build_candidate_plans
from .query_spec import QuerySpec
from .schema_index import SchemaIndex, build_schema_index
from .sql_compiler import CompiledCandidate, compile_to_snowflake_sql
from .static_validate import RankedCandidate, rank_candidates


def generate_sql_candidates(
    *,
    question: str,
    json_sample: Any,
    table_name: str,
    json_column: str,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> Tuple[SchemaIndex, List[CatalogField], QuerySpec, List[RankedCandidate]]:
    """
    End-to-end offline workflow:
    JSON sample -> SchemaIndex -> FieldCatalog -> LLM(QuerySpec) -> deterministic plans -> compile -> rank.
    """
    index = build_schema_index(json_sample)
    catalog = build_field_catalog(index, limit=60)

    agent = IntentAgent(api_key=api_key, model=model)
    spec = agent.to_query_spec(question=question, field_catalog=catalog, default_limit=100)

    plans = build_candidate_plans(index, spec)
    compiled: List[CompiledCandidate] = [
        compile_to_snowflake_sql(index, p, spec, table_name=table_name, json_column=json_column)
        for p in plans
    ]
    ranked = rank_candidates(index, compiled)
    return index, catalog, spec, ranked

