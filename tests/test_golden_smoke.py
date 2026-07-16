"""
Golden-set smoke test for deterministic derive_candidates (ADR 0002).

Feeds hand-verified QuerySpecs from eval/golden/*.json through
derive_candidates → compile → rank, with no LLM involved.
"""

from __future__ import annotations

import json
import os
import unittest
from typing import Any, Dict, List, Optional, Tuple

from workflow.nodes.plan_agent import derive_candidates
from workflow.nodes.schema_index import schema_index_node
from workflow.nodes.sql_compiler import compile_candidate_sql
from workflow.nodes.static_validate import rank_candidates

GOLDEN_DIR = os.path.join("eval", "golden")
MIN_SCORE = 70


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _should_skip(question: Dict[str, Any]) -> bool:
    if question.get("adversarial") or question.get("ambiguous") or question.get("known_limitation"):
        return True
    expected = question.get("expected")
    return not isinstance(expected, dict)


def _alias_from_path(path: str) -> str:
    leaf = path.split(":")[-1].replace("[*]", "")
    return leaf or "value"


def expected_to_query_spec(expected: Dict[str, Any]) -> Dict[str, Any]:
    """Map golden 'expected' shape onto the compiler's query_spec shape."""
    select_items: List[Dict[str, Any]] = []
    for s in expected.get("select") or []:
        if isinstance(s, str):
            select_items.append({"path": s, "alias": _alias_from_path(s)})
        elif isinstance(s, dict) and s.get("path"):
            item = dict(s)
            item.setdefault("alias", _alias_from_path(str(item["path"])))
            select_items.append(item)

    aggregations: List[Dict[str, Any]] = []
    for a in expected.get("aggregations") or []:
        if not isinstance(a, dict):
            continue
        aggregations.append(
            {
                "func": a.get("func"),
                "path": a.get("path"),
                "alias": a.get("alias") or f"{(a.get('func') or 'agg')}_value",
                "cast": a.get("cast"),
            }
        )

    return {
        "select": select_items,
        "filters": list(expected.get("filters") or []),
        "group_by": list(expected.get("group_by") or []),
        "aggregations": aggregations,
        "order_by": list(expected.get("order_by") or []),
        "limit": expected.get("limit") if isinstance(expected.get("limit"), int) else 100,
        "grain_hint": expected.get("grain") or "unknown",
        "notes": "",
    }


def _collect_referenced_paths(expected: Dict[str, Any]) -> List[str]:
    paths: List[str] = []
    for s in expected.get("select") or []:
        if isinstance(s, str):
            paths.append(s)
        elif isinstance(s, dict) and s.get("path"):
            paths.append(str(s["path"]))
    for f in expected.get("filters") or []:
        if isinstance(f, dict) and f.get("path"):
            paths.append(str(f["path"]))
    for g in expected.get("group_by") or []:
        if isinstance(g, str):
            paths.append(g)
    for a in expected.get("aggregations") or []:
        if isinstance(a, dict) and a.get("path"):
            paths.append(str(a["path"]))
    for o in expected.get("order_by") or []:
        if not isinstance(o, dict):
            continue
        expr = o.get("expr_alias")
        if isinstance(expr, str) and (":" in expr or "[*]" in expr):
            paths.append(expr)
        elif isinstance(expr, str) and expr:
            # Alias-only order_by (e.g. "cnt", "price") — still must appear in SQL.
            paths.append(expr)
    return paths


def _path_appears_in_sql(path_or_alias: str, sql: str) -> bool:
    """
    True if a SQL-relevant fragment of the golden path/alias appears in compiled SQL.

    Mirrors the style of test_sql_compiler assertions: strip [*], then look for
    colon-path suffixes (e.g. user:email, item:price) or the bare leaf/alias.
    """
    if not path_or_alias:
        return True
    if ":" not in path_or_alias and "[*]" not in path_or_alias:
        return path_or_alias in sql

    stripped = path_or_alias.replace("[*]", "")
    parts = [p for p in stripped.split(":") if p]
    if not parts:
        return True
    # Prefer longer suffixes (user:email before email) so we don't pass on
    # coincidental leaf collisions alone when a deeper fragment is available.
    for i in range(len(parts)):
        frag = ":".join(parts[i:])
        if frag and frag in sql:
            return True
    return False


def _missing_path_fragments(expected: Dict[str, Any], sql: str) -> List[str]:
    missing: List[str] = []
    for p in _collect_referenced_paths(expected):
        if not _path_appears_in_sql(p, sql):
            missing.append(p)
    return missing


def _iter_golden_cases() -> List[Tuple[str, str, Dict[str, Any]]]:
    """Yield (schema_file, question_id, question_dict) for fully-specified cases."""
    cases: List[Tuple[str, str, Dict[str, Any]]] = []
    for name in sorted(os.listdir(GOLDEN_DIR)):
        if not name.endswith(".json"):
            continue
        golden = _load_json(os.path.join(GOLDEN_DIR, name))
        schema_file = golden["schema_file"]
        for q in golden.get("questions") or []:
            if _should_skip(q):
                continue
            cases.append((schema_file, q["id"], q))
    return cases


class TestGoldenSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._schema_cache: Dict[str, Dict[str, Any]] = {}
        cls._results: List[Dict[str, Any]] = []

    def _schema_index_for(self, schema_file: str) -> Dict[str, Any]:
        if schema_file not in self._schema_cache:
            sample = _load_json(schema_file)
            state = schema_index_node({"json_sample": sample})
            self._schema_cache[schema_file] = state["schema_index"]
        return self._schema_cache[schema_file]

    def test_derive_candidates_against_golden(self):
        cases = _iter_golden_cases()
        self.assertTrue(cases, "No fully-specified golden questions found")

        failures: List[str] = []

        for schema_file, qid, question in cases:
            expected = question["expected"]
            row: Dict[str, Any] = {
                "id": qid,
                "candidate": "",
                "score": "",
                "status": "PASS",
                "detail": "",
            }
            try:
                schema_index = self._schema_index_for(schema_file)
                query_spec = expected_to_query_spec(expected)
                plan = derive_candidates(schema_index, query_spec)
                candidates = plan.get("candidates") or []
                if not candidates:
                    raise AssertionError("derive_candidates returned no candidates")

                compiled = [
                    compile_candidate_sql(
                        schema_fields=schema_index.get("fields") or {},
                        candidate=c,
                        query_spec=query_spec,
                        table_name="customer_data",
                        json_column="raw_data",
                    )
                    for c in candidates
                ]
                ranked = rank_candidates(schema_index, compiled)
                top = ranked[0]
                sql = top.get("sql") or ""
                score = int(top.get("score") or 0)
                name = str(top.get("name") or "")

                row["candidate"] = name
                row["score"] = score

                missing = _missing_path_fragments(expected, sql)
                problems: List[str] = []
                if score < MIN_SCORE:
                    problems.append(f"score {score} < {MIN_SCORE}")
                if missing:
                    problems.append(f"missing path fragments: {missing}")
                if problems:
                    row["status"] = "FAIL"
                    row["detail"] = "; ".join(problems)
                    failures.append(f"{qid}: {row['detail']}")
            except Exception as exc:  # report every failure; do not abort the loop
                row["status"] = "FAIL"
                row["detail"] = f"{type(exc).__name__}: {exc}"
                failures.append(f"{qid}: {row['detail']}")

            self._results.append(row)

        self._print_summary(self._results)

        if failures:
            self.fail(
                f"{len(failures)} golden smoke failure(s):\n"
                + "\n".join(f"  - {f}" for f in failures)
            )

    @staticmethod
    def _print_summary(results: List[Dict[str, Any]]) -> None:
        print("\n" + "=" * 78)
        print("GOLDEN SMOKE SUMMARY (derive_candidates -> compile -> rank)")
        print("=" * 78)
        header = f"{'id':<12} {'candidate':<28} {'score':>5}  {'result':<6}  detail"
        print(header)
        print("-" * 78)
        for r in results:
            print(
                f"{r['id']:<12} {str(r['candidate']):<28} {str(r['score']):>5}  "
                f"{r['status']:<6}  {r['detail']}"
            )
        n_pass = sum(1 for r in results if r["status"] == "PASS")
        n_fail = sum(1 for r in results if r["status"] == "FAIL")
        print("-" * 78)
        print(f"Total: {len(results)}  PASS: {n_pass}  FAIL: {n_fail}")
        print("=" * 78 + "\n")


if __name__ == "__main__":
    unittest.main()
