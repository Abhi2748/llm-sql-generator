"""
Manual sanity check: run the REAL redesigned graph (real LLM calls, real
conditional critic routing, real schema cache) against a handful of golden
questions. Not a scored eval - just eyeball whether live output looks sane.
"""
import json
from workflow.graph import run_workflow

from dotenv import load_dotenv
load_dotenv()

# One from each schema, deliberately covering: nested flatten, aggregation,
# order_by/limit, an adversarial case, and an ambiguous case - the highest-risk
# categories from ADR 0002's rewrite.
CHECKS = [
    ("data/sample_data.json", "ecommerce", "ecom-03",
     "List all item names and prices in the transaction for evt_001."),
    ("data/sample_data.json", "ecommerce", "ecom-11",
     "What is the single most expensive item, and its price?"),
    ("data/sample_data.json", "ecommerce", "ecom-13",
     "List the shipping carrier used for each event."),  # adversarial
    ("data/sample_data_github.json", "github", "gh-03",
     "Which actor has the most events, and how many?"),
    ("data/sample_data_ga4.json", "ga4", "ga4-03",
     "List the top 5 countries by event count."),
    ("data/sample_data_ga4.json", "ga4", "ga4-11",
     "Order events by their timestamp, most recent first, and show the top 5."),
     ("data/sample_data.json", "ecommerce", "ecom-16",
     "Which product review has the highest rating, and what does it say?"),
]

for path, table, qid, question in CHECKS:
    with open(path) as f:
        sample = json.load(f)

    print(f"\n{'='*70}\n{qid}: {question}\n{'='*70}")
    result = run_workflow(
        question=question,
        json_sample=sample,
        table_name=table,
        json_column="raw_data",
        max_retries=2,
    )
    ranked = result.get("ranked_candidates") or []
    state = result.get("state") or {}
    history = result.get("iteration_history") or state.get("iteration_history") or []
    if not ranked:
        print("NO CANDIDATES GENERATED")
        continue
    top = ranked[0]
    print(f"top candidate: {top.get('name')} | score: {top.get('score')}")
    print(f"issues: {top.get('issues')}")
    print(f"loop_exit_reason: {result.get('loop_exit_reason') or state.get('loop_exit_reason')}")
    print(f"retry_count: {state.get('retry_count')}")
    print(f"repair_notes: {state.get('repair_notes')}")
    print(f"critic_notes (final): {result.get('critic_notes')}")
    print("--- iteration_history ---")
    for i, entry in enumerate(history):
        phase = entry.get("phase")
        rc = entry.get("retry_count")
        if phase == "static_validate":
            print(
                f"  [{i}] static_validate rc={rc} "
                f"branch={entry.get('decide_critic_branch')} "
                f"score={entry.get('top_score')} issues={entry.get('top_issues')}"
            )
        elif phase == "critic_agent":
            print(
                f"  [{i}] critic_agent rc={rc} "
                f"branch={entry.get('decide_retry_detail')} "
                f"should_retry={entry.get('should_retry')} "
                f"patch_keys={list((entry.get('query_spec_patch') or {}).keys())}"
            )
        elif phase == "repair_applied":
            print(f"  [{i}] repair_applied rc={rc} notes={entry.get('repair_notes')!r}")
        else:
            print(f"  [{i}] {phase} rc={rc}")
    print(f"--- SQL ---\n{top.get('sql')}")
