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
    critic = result.get("critic_notes") or {}
    if not ranked:
        print("NO CANDIDATES GENERATED")
        continue
    top = ranked[0]
    print(f"top candidate: {top.get('name')} | score: {top.get('score')}")
    print(f"issues: {top.get('issues')}")
    print(f"critic ran: {bool(critic)} | critic notes: {critic}")
    print(f"--- SQL ---\n{top.get('sql')}")