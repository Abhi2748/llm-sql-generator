"""
Schema-only test: no OpenAI needed. Shows what context is sent to the LLM.
Run: python test_schema_only.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.schema_agent import SnowflakeSchemaAgent


def main():
    with open("data/sample_data.json", "r", encoding="utf-8") as f:
        json_data = json.load(f)

    table_name = "customer_data"
    json_column = "raw_data"
    schema_agent = SnowflakeSchemaAgent()
    schema_agent.set_table_info(table_name, json_column)
    analysis = schema_agent.analyze_json_for_snowflake(json_data, table_name, json_column)
    context = schema_agent.get_snowflake_context()

    print("=== ANALYSIS (first 20 paths) ===")
    for i, p in enumerate(analysis["json_paths"][:20]):
        print(f"  {p}")
    print(f"  ... total paths: {len(analysis['json_paths'])}")

    print("\n=== CONTEXT STRING SENT TO LLM ===")
    print(context)

    print("\n=== QUERYABLE FIELDS (sample) ===")
    for f in analysis["queryable_fields"][:8]:
        print(f"  {f}")


if __name__ == "__main__":
    main()
