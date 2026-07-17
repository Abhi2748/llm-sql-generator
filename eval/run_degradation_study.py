"""
Sample-completeness degradation study.

For selected golden questions, run baseline_a / baseline_b / pipeline against
prefix tiers [2, 5, 20, full] and measure overconfidence vs hedging — not just
token scores (which are often meaningless when the tier lacks required fields).

Makes REAL LLM calls — not part of the default pytest suite:

  python eval/run_degradation_study.py --limit 1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

# Allow `python eval/run_degradation_study.py` (repo root not on sys.path by default).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv

from eval._shared import (
    BASELINE_NO_CATCH_NOTE,
    FIXTURES_DIR,
    GOLDEN_DIR,
    RESULTS_DIR,
    SCORING_LIMITATION,
    CountingLLM,
    collect_required_tokens,
    load_json,
    overconfidence_flag,
    pipeline_caught_insufficient,
    progress,
    required_tokens_present_in_tier,
    run_baseline_a,
    run_baseline_b,
    run_pipeline,
    schema_field_leaves,
    schema_non_leaf_container_keys,
    score_sql,
    table_name_for_golden,
    token_score,
)
from eval.fixtures.tiered_samples import PREFIX_TIERS, SOURCES, generate_all, tier_path
from workflow.llm import build_chat_llm, default_llm_config
from workflow.nodes.schema_index import build_schema_index_and_catalog

TIERS: List[Union[int, str]] = [*PREFIX_TIERS, "full"]


def _basename_prefix_for_schema_file(schema_file: str) -> Optional[str]:
    for src, prefix in SOURCES:
        if os.path.normpath(src) == os.path.normpath(schema_file):
            return prefix
    # Also match absolute / cwd-relative variants
    for src, prefix in SOURCES:
        if os.path.basename(schema_file) == os.path.basename(src):
            return prefix
    return None


def _fmt_token_display(entry: Dict[str, Any]) -> str:
    if entry.get("token_score_meaningful") is False:
        return "N/A - tier lacks required fields"
    score = entry.get("token_score")
    if score is None:
        return "n/a"
    return f"{float(score):.2f}"


def _print_summary(rows: List[Dict[str, Any]]) -> None:
    print("\n" + "=" * 120)
    print("DEGRADATION STUDY SUMMARY")
    print(SCORING_LIMITATION)
    print(BASELINE_NO_CATCH_NOTE)
    print("=" * 120)
    print(
        f"{'question':10} {'tier':5} {'system':11} {'overconf':8} "
        f"{'token_score':>32} {'pipe_caught':>11}"
    )
    print("-" * 120)
    for row in rows:
        qid = row["question_id"]
        tier = str(row["tier"])
        for sys_name, entry in row["systems"].items():
            over = str(bool(entry.get("overconfidence_flag")))
            tok = _fmt_token_display(entry)
            if sys_name == "pipeline":
                caught = str(bool((entry.get("pipeline_caught") or {}).get("pipeline_caught_it")))
            else:
                caught = "n/a"
            print(
                f"{qid[:10]:10} {tier:5} {sys_name:11} {over:8} "
                f"{tok:>32} {caught:>11}"
            )
    print("=" * 120 + "\n")


def run_degradation_study(*, limit: Optional[int] = None) -> Dict[str, Any]:
    load_dotenv()
    # Ensure prefix-tier fixtures exist (idempotent).
    generate_all(write=True)

    cfg = default_llm_config()
    base_llm = build_chat_llm(cfg)

    summary_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []

    golden_files = ["github.json", "ga4.json"]
    for golden_name in golden_files:
        golden_path = os.path.join(GOLDEN_DIR, golden_name)
        if not os.path.isfile(golden_path):
            progress(f"SKIP missing {golden_path}")
            continue
        golden = load_json(golden_path)
        schema_file = golden.get("schema_file")
        study_ids = list(golden.get("degradation_study_ids") or [])
        if limit is not None:
            study_ids = study_ids[:limit]

        by_id = {q.get("id"): q for q in (golden.get("questions") or []) if q.get("id")}
        table_name = table_name_for_golden(golden_path, golden)
        prefix = _basename_prefix_for_schema_file(str(schema_file))
        if not prefix:
            progress(f"SKIP {golden_name}: no tier mapping for {schema_file}")
            continue

        progress(f"\n=== degradation schema={table_name} ids={study_ids} ===")

        for qid in study_ids:
            q = by_id.get(qid)
            if not q:
                progress(f"SKIP unknown id {qid}")
                continue
            question_text = q.get("question") or ""
            targets = list(q.get("targets") or [])
            expected = q.get("expected") if isinstance(q.get("expected"), dict) else None
            required = collect_required_tokens(expected) if expected else []

            for tier in TIERS:
                sample_path = tier_path(prefix, tier)
                json_sample = load_json(sample_path)
                schema_index = build_schema_index_and_catalog(
                    json_sample, max_array_samples=10_000
                )
                # Use full field map from schema_index (not truncated catalog).
                tier_leaves = schema_field_leaves(schema_index)
                non_leaf_keys = schema_non_leaf_container_keys(schema_index)
                presence = required_tokens_present_in_tier(required, tier_leaves)
                tier_ok = bool(presence["tier_has_required_fields"])

                progress(f"\n[{qid} tier={tier}] {question_text[:70]}")
                progress(
                    f"  tier_has_required_fields={tier_ok} "
                    f"missing_in_tier={presence['missing_in_tier']}"
                )

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
                    except Exception as e:  # pragma: no cover
                        progress(f"  !! {sys_name} FAILED: {e}")
                        systems[sys_name] = {
                            "error": str(e),
                            "llm_calls": counter.calls,
                            "overconfidence_flag": False,
                            "token_score": None,
                            "token_score_meaningful": False,
                            "token_score_display": "N/A - tier lacks required fields"
                            if not tier_ok
                            else "n/a",
                        }
                        continue

                    sql = run_out.get("sql") or ""
                    scored_bits = score_sql(sql, expected, targets, scoreable=True)
                    ts = token_score(sql, required) if required else {"score": None}

                    # Primary signal at incomplete tiers is NOT token score.
                    if tier_ok:
                        token_display = ts.get("score")
                        meaningful = True
                    else:
                        token_display = None
                        meaningful = False

                    over = overconfidence_flag(
                        sql=sql,
                        system=sys_name,
                        run_out=run_out,
                        tier_leaves=tier_leaves,
                        non_leaf_container_keys=non_leaf_keys,
                    )
                    entry: Dict[str, Any] = {
                        **run_out,
                        **scored_bits,
                        "token_score": ts.get("score"),
                        "token_score_meaningful": meaningful,
                        "token_score_display": (
                            f"{float(token_display):.2f}"
                            if isinstance(token_display, (int, float))
                            else "N/A - tier lacks required fields"
                        ),
                        "tier_field_presence": presence,
                        "overconfidence_flag": over["overconfidence_flag"],
                        "overconfidence_detail": over,
                    }
                    if sys_name == "pipeline":
                        entry["pipeline_caught"] = pipeline_caught_insufficient(
                            run_out, overconfidence=over
                        )
                    else:
                        entry["pipeline_caught"] = {
                            "pipeline_caught_it": None,
                            "note": BASELINE_NO_CATCH_NOTE,
                        }

                    systems[sys_name] = entry
                    progress(
                        f"  <- {sys_name}: overconf={entry['overconfidence_flag']} "
                        f"token={entry['token_score_display']} "
                        f"caught={entry['pipeline_caught'].get('pipeline_caught_it')}"
                    )

                row = {
                    "question_id": qid,
                    "schema": table_name,
                    "tier": tier,
                    "sample_path": sample_path,
                    "question": question_text,
                    "targets": targets,
                    "tier_has_required_fields": tier_ok,
                    "tier_field_presence": presence,
                    "systems": systems,
                }
                detail_rows.append(row)
                summary_rows.append(row)

    return {
        "meta": {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "limit_questions_per_schema": limit,
            "tiers": TIERS,
            "model": cfg.model,
            "scoring_limitation": SCORING_LIMITATION,
            "baseline_asymmetry_note": BASELINE_NO_CATCH_NOTE,
            "fixtures_dir": FIXTURES_DIR,
            "row_count": len(detail_rows),
        },
        "rows": detail_rows,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run sample-completeness degradation study (live LLM)."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only the first N degradation_study_ids per schema (smoke test).",
    )
    args = parser.parse_args(argv)

    payload = run_degradation_study(limit=args.limit)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(RESULTS_DIR, f"degradation_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    _print_summary(payload["rows"])
    print(f"Wrote full results: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
