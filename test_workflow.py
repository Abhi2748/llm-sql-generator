"""
Test script to run the schema-first workflow and print ranked candidates.
Run from project root: python test_workflow.py
"""
import json
import os
import sys

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.pipeline import generate_sql_candidates


def load_sample_data():
    with open("data/sample_data.json", "r", encoding="utf-8") as f:
        return json.load(f)


def run_test(user_query: str, table_name: str = "customer_data", json_column: str = "raw_data"):
    print("=" * 60)
    print("QUERY:", user_query)
    print("=" * 60)
    
    json_data = load_sample_data()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your_openai_api_key_here":
        print("SKIP: Set OPENAI_API_KEY in .env to run LLM tests.")
        return None

    index, catalog, query_spec, ranked = generate_sql_candidates(
        question=user_query,
        json_sample=json_data,
        table_name=table_name,
        json_column=json_column,
        api_key=api_key,
        model=os.getenv("OPENAI_MODEL"),
    )

    print("QuerySpec:", json.dumps(query_spec, indent=2))
    print()

    for c in ranked[:3]:
        print("-" * 60)
        print("CANDIDATE:", c.name, "score=", c.score)
        print("ASSUMPTIONS:", c.assumptions)
        if c.issues:
            print("NOTES:", c.issues[:6])
        print("SQL:\n", c.sql)
    print()
    return ranked


if __name__ == "__main__":
    queries = [
        "List all event IDs and user emails",
        "What is the total transaction amount per event?",
        "Get product names and prices from purchase events",
    ]
    for q in queries:
        run_test(q)
