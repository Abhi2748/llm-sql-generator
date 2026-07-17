"""
Phase 2/3 comparison harness: baseline A, baseline B, and the real multi-agent
pipeline against eval/golden/*.json.

Makes REAL LLM calls — not part of the default pytest suite. Run manually:

  python eval/run_comparison.py --limit 3
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Allow `python eval/run_comparison.py` (repo root not on sys.path by default).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv

from eval._shared import (
    GOLDEN_DIR,
    RESULTS_DIR,
    SCORING_LIMITATION,
    CountingLLM,
    load_json,
    mean,
    progress,
    qualitative_reason,
    run_baseline_a,
    run_baseline_b,
    run_pipeline,
    score_sql,
    table_name_for_golden,
)
from workflow.llm import build_chat_llm, default_llm_config


def _fmt_score(v: Any) -> str:
    if v is None:
        return "  n/a"
    return f"{float(v):6.2f}"


def _print_summary(scored_rows: List[Dict[str, Any]]) -> None:
    headers = [
        "id",
        "a_score",
        "a_calls",
        "b_score",
        "b_calls",
        "b_rev",
        "p_score",
        "p_calls",
        "p_retry",
    ]
    print("\n" + "=" * 100)
    print("COMPARISON SUMMARY (scored questions only)")
    print(SCORING_LIMITATION)
    print("=" * 100)
    print(
        f"{'id':12} {'a_score':>7} {'a_calls':>7} {'b_score':>7} {'b_calls':>7} "
        f"{'b_rev':>5} {'p_score':>7} {'p_calls':>7} {'p_retry':>7}"
    )
    print("-" * 100)

    a_scores: List[float] = []
    b_scores: List[float] = []
    p_scores: List[float] = []
    a_calls: List[float] = []
    b_calls: List[float] = []
    p_calls: List[float] = []

    for row in scored_rows:
        a = row["systems"]["baseline_a"]
        b = row["systems"]["baseline_b"]
        p = row["systems"]["pipeline"]
        if isinstance(a.get("token_score"), (int, float)):
            a_scores.append(float(a["token_score"]))
        if isinstance(b.get("token_score"), (int, float)):
            b_scores.append(float(b["token_score"]))
        if isinstance(p.get("token_score"), (int, float)):
            p_scores.append(float(p["token_score"]))
        a_calls.append(float(a.get("llm_calls") or 0))
        b_calls.append(float(b.get("llm_calls") or 0))
        p_calls.append(float(p.get("llm_calls") or 0))

        print(
            f"{row['id'][:12]:12} "
            f"{_fmt_score(a.get('token_score'))} "
            f"{int(a.get('llm_calls') or 0):7d} "
            f"{_fmt_score(b.get('token_score'))} "
            f"{int(b.get('llm_calls') or 0):7d} "
            f"{str(bool(b.get('revised'))):>5} "
            f"{_fmt_score(p.get('token_score'))} "
            f"{int(p.get('llm_calls') or 0):7d} "
            f"{int(p.get('retry_count') or 0):7d}"
        )

    print("-" * 100)
    print(
        f"{'MEAN':12} "
        f"{_fmt_score(mean(a_scores))} "
        f"{_fmt_score(mean(a_calls))} "
        f"{_fmt_score(mean(b_scores))} "
        f"{_fmt_score(mean(b_calls))} "
        f"{'':>5} "
        f"{_fmt_score(mean(p_scores))} "
        f"{_fmt_score(mean(p_calls))} "
        f"{'':>7}"
    )
    print("=" * 100)
    print(f"(columns: {', '.join(headers)})\n")


def run_comparison(*, limit: Optional[int] = None) -> Dict[str, Any]:
    load_dotenv()
    cfg = default_llm_config()
    base_llm = build_chat_llm(cfg)

    golden_files = sorted(f for f in os.listdir(GOLDEN_DIR) if f.endswith(".json"))
    scored: List[Dict[str, Any]] = []
    qualitative: List[Dict[str, Any]] = []

    for golden_name in golden_files:
        golden_path = os.path.join(GOLDEN_DIR, golden_name)
        golden = load_json(golden_path)
        schema_file = golden.get("schema_file")
        if not schema_file:
            progress(f"SKIP {golden_name}: no schema_file")
            continue
        json_sample = load_json(schema_file)
        table_name = table_name_for_golden(golden_path, golden)
        questions = list(golden.get("questions") or [])
        if limit is not None:
            questions = questions[:limit]

        progress(f"\n=== schema={table_name} file={golden_name} questions={len(questions)} ===")

        for q in questions:
            qid = q.get("id") or "?"
            question_text = q.get("question") or ""
            targets = list(q.get("targets") or [])
            reason = qualitative_reason(q)
            scoreable = reason is None
            expected = q.get("expected") if isinstance(q.get("expected"), dict) else None

            progress(f"\n[{qid}] {question_text[:80]}")

            systems: Dict[str, Any] = {}
            runners = (
                ("baseline_a", run_baseline_a),
                ("baseline_b", run_baseline_b),
                ("pipeline", run_pipeline),
            )
            for sys_name, run_fn in runners:
                progress(f"  -> {sys_name} ...")
                counter = CountingLLM(base_llm)
                try:
                    run_out = run_fn(question_text, json_sample, table_name, counter)
                except Exception as e:  # pragma: no cover - live harness
                    progress(f"  !! {sys_name} FAILED: {e}")
                    systems[sys_name] = {
                        "sql": "",
                        "llm_calls": counter.calls,
                        "error": str(e),
                        "token_score": None,
                        "scored": False,
                    }
                    continue

                scored_bits = score_sql(
                    run_out.get("sql") or "",
                    expected,
                    targets,
                    scoreable=scoreable,
                )
                entry = {**run_out, **scored_bits}
                systems[sys_name] = entry
                progress(
                    f"  <- {sys_name}: calls={entry.get('llm_calls')} "
                    f"token_score={entry.get('token_score')} "
                    f"flatten_ok={((entry.get('flatten_check') or {}).get('ok'))}"
                )

            row = {
                "id": qid,
                "schema": table_name,
                "schema_file": schema_file,
                "question": question_text,
                "targets": targets,
                "bucket": "scored" if scoreable else "qualitative",
                "qualitative_reason": reason,
                "systems": systems,
            }
            if scoreable:
                scored.append(row)
            else:
                qualitative.append(row)

    aggregates = {
        "baseline_a": {
            "mean_token_score": mean(
                [
                    float(r["systems"]["baseline_a"]["token_score"])
                    for r in scored
                    if isinstance(r["systems"]["baseline_a"].get("token_score"), (int, float))
                ]
            ),
            "mean_llm_calls": mean(
                [float(r["systems"]["baseline_a"].get("llm_calls") or 0) for r in scored]
            ),
        },
        "baseline_b": {
            "mean_token_score": mean(
                [
                    float(r["systems"]["baseline_b"]["token_score"])
                    for r in scored
                    if isinstance(r["systems"]["baseline_b"].get("token_score"), (int, float))
                ]
            ),
            "mean_llm_calls": mean(
                [float(r["systems"]["baseline_b"].get("llm_calls") or 0) for r in scored]
            ),
        },
        "pipeline": {
            "mean_token_score": mean(
                [
                    float(r["systems"]["pipeline"]["token_score"])
                    for r in scored
                    if isinstance(r["systems"]["pipeline"].get("token_score"), (int, float))
                ]
            ),
            "mean_llm_calls": mean(
                [float(r["systems"]["pipeline"].get("llm_calls") or 0) for r in scored]
            ),
            "mean_retry_count": mean(
                [float(r["systems"]["pipeline"].get("retry_count") or 0) for r in scored]
            ),
        },
    }

    return {
        "meta": {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "limit_per_schema": limit,
            "model": cfg.model,
            "scoring_limitation": SCORING_LIMITATION,
            "scored_count": len(scored),
            "qualitative_count": len(qualitative),
        },
        "aggregates": aggregates,
        "scored": scored,
        "qualitative": qualitative,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run golden-set system comparison (live LLM).")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only the first N questions per golden schema (smoke test).",
    )
    args = parser.parse_args(argv)

    payload = run_comparison(limit=args.limit)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(RESULTS_DIR, f"comparison_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    _print_summary(payload["scored"])
    print(f"Wrote full results: {out_path}")
    print(f"Qualitative (unscored) questions logged: {payload['meta']['qualitative_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
