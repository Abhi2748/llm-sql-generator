from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .schema_index import SchemaIndex, array_ancestors
from .sql_compiler import CompiledCandidate


@dataclass(frozen=True)
class RankedCandidate:
    name: str
    sql: str
    score: int
    assumptions: Dict[str, Any]
    paths_used: List[str]
    issues: List[str]


def rank_candidates(index: SchemaIndex, compiled: List[CompiledCandidate]) -> List[RankedCandidate]:
    ranked: List[RankedCandidate] = []

    for c in compiled:
        score = 100
        issues = list(c.issues)

        # Unknown fields
        unknown = [p for p in c.paths_used if p not in index.fields]
        if unknown:
            score -= 40
            issues.append(f"Unknown paths referenced (not found in schema sample): {unknown[:6]}")

        # Flatten sanity: for each used path containing array markers, ensure candidate flattened its array ancestors
        flattened = set(c.assumptions.get("flatten_arrays") or [])
        for p in c.paths_used:
            ancestors = array_ancestors(p)
            missing = [a for a in ancestors if a not in flattened]
            if missing:
                score -= 15
                issues.append(f"Missing FLATTEN for arrays: {missing}")
                break

        sql_lower = c.sql.lower()
        if "select *" in sql_lower:
            score -= 10
            issues.append("Uses SELECT * (prefer explicit fields).")
        if "::" not in c.sql:
            score -= 10
            issues.append("No :: type casts found (Snowflake JSON best practice).")
        if ":" not in c.sql:
            score -= 20
            issues.append("No : JSON path traversal found (may not be querying JSON fields).")

        # Prefer plans that match the assumed grain
        grain = (c.assumptions.get("grain") or "").lower()
        if grain == "item" and "flatten" not in sql_lower:
            score -= 10
            issues.append("Item-grain requested but query does not flatten arrays.")

        # Clamp
        score = max(0, min(100, score))

        ranked.append(
            RankedCandidate(
                name=c.name,
                sql=c.sql,
                score=score,
                assumptions=c.assumptions,
                paths_used=c.paths_used,
                issues=issues,
            )
        )

    ranked.sort(key=lambda r: (-r.score, r.name))
    return ranked

