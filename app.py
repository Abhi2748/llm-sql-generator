# app.py
import json
import os
from typing import Any, Optional, Tuple

import streamlit as st
from dotenv import load_dotenv

from workflow.graph import run_workflow

load_dotenv()

st.set_page_config(
    page_title="Snowflake JSON SQL Generator",
    page_icon="🧾",
    layout="centered",
)

def check_api_key_status():
    """Check if OpenAI API key is properly configured"""
    api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        return False, "No API key found in environment variables"
    elif api_key in {"your_api_key_here", "your_openai_api_key_here"}:
        return False, "Please replace the placeholder value with your actual OpenAI API key in .env file"
    elif len(api_key) < 20:
        return False, "API key appears to be invalid (too short)"
    else:
        return True, f"API key loaded successfully"

def format_sql_nicely(sql: str) -> str:
    """Format SQL for better readability"""
    import re
    
    # Basic SQL formatting
    sql = sql.strip()
    
    # Add line breaks after SELECT, FROM, WHERE, GROUP BY, ORDER BY, JOIN
    sql = re.sub(r'\bSELECT\b', '\nSELECT\n  ', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bFROM\b', '\nFROM', sql, flags=re.IGNORECASE)  
    sql = re.sub(r'\bWHERE\b', '\nWHERE', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGROUP BY\b', '\nGROUP BY', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bORDER BY\b', '\nORDER BY', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bJOIN\b', '\nJOIN', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bLEFT JOIN\b', '\nLEFT JOIN', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bINNER JOIN\b', '\nINNER JOIN', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bLATERAL FLATTEN\b', '\nLATERAL FLATTEN', sql, flags=re.IGNORECASE)
    
    # Add proper spacing around commas in SELECT
    sql = re.sub(r',\s*(?=\w)', ',\n  ', sql)
    
    # Clean up extra whitespace
    sql = re.sub(r'\n\s*\n', '\n', sql)
    sql = sql.strip()
    
    return sql


def _load_repo_sample_json() -> Tuple[Optional[Any], Optional[str]]:
    try:
        with open(os.path.join("data", "sample_data.json"), "r", encoding="utf-8") as f:
            return json.load(f), None
    except Exception as e:
        return None, str(e)

def main():
    st.title("Snowflake JSON SQL Generator")
    st.caption("LangGraph multi-agent workflow. Give a JSON sample + a question. You will get 2–3 ranked Snowflake SQL candidates.")
    
    api_key_valid, api_key_message = check_api_key_status()
    
    if not api_key_valid:
        st.warning(f"OpenAI API key not configured: {api_key_message}")

    st.subheader("1) Table configuration")
    c1, c2 = st.columns(2)
    with c1:
        table_name = st.text_input("Table name", value="customer_data")
    with c2:
        json_column = st.text_input("VARIANT column", value="raw_data")

    st.subheader("2) JSON sample")
    input_method = st.radio("JSON input", ["Use repo sample", "Upload JSON file", "Paste JSON"], horizontal=True)

    json_data: Optional[Any] = None
    if input_method == "Use repo sample":
        json_data, err = _load_repo_sample_json()
        if err:
            st.error(f"Could not load `data/sample_data.json`: {err}")
        elif json_data is not None:
            st.info("Loaded `data/sample_data.json`.")
    elif input_method == "Upload JSON file":
        uploaded = st.file_uploader("Choose a JSON file", type="json")
        if uploaded is not None:
            try:
                json_data = json.load(uploaded)
            except Exception as e:
                st.error(f"Invalid JSON file: {e}")
    else:
        raw = st.text_area("Paste JSON", height=180, placeholder='{"example": [{"id": 1, "name": "Alice"}]}')
        if raw.strip():
            try:
                json_data = json.loads(raw)
            except Exception as e:
                st.error(f"Invalid JSON: {e}")

    if json_data is not None:
        with st.expander("Preview JSON (collapsed by default)", expanded=False):
            st.json(json_data)

    st.subheader("3) Question → SQL")
    question = st.text_area("Your question", height=100, placeholder="e.g. List event IDs and user emails")

    c3, c4 = st.columns(2)
    with c3:
        candidates_to_show = st.selectbox("Candidates to show", [2, 3], index=1)
    with c4:
        show_details = st.checkbox("Show details (QuerySpec, paths, assumptions)", value=False)

    can_run = api_key_valid and (json_data is not None) and bool(question.strip())
    generate = st.button("Generate SQL", type="primary", disabled=not can_run)

    if generate:
        with st.spinner("Generating ranked SQL candidates..."):
            try:
                result = run_workflow(
                    question=question.strip(),
                    json_sample=json_data,
                    table_name=table_name.strip() or "your_table",
                    json_column=json_column.strip() or "your_variant_column",
                    max_retries=2,
                )
            except Exception as e:
                st.error(f"Generation failed: {e}")
                return

        ranked = result.get("ranked_candidates") or []
        query_spec = result.get("query_spec") or {}
        plan = result.get("plan") or {}
        schema_summary = result.get("schema_summary") or ""
        critic_notes = result.get("critic_notes") or {}

        if not ranked:
            st.error("No SQL candidates generated.")
            return

        ranked = ranked[:candidates_to_show]
        st.success("Done. Choose the candidate that matches your table’s row grain.")
        st.caption(
            "Tip: If each row contains a full document with a root array (like ecommerce_events), the doc-per-row candidate is usually right. "
            "If each row is already one event/object, the event-per-row candidate is usually right."
        )

        labels = [f"{c.name} (score {c.score})" for c in ranked]
        pick = st.selectbox("Pick a candidate", list(range(len(ranked))), format_func=lambda i: labels[i])
        chosen = ranked[pick]

        st.markdown("### SQL")
        st.code(format_sql_nicely(chosen.get("sql") or ""), language="sql")

        if chosen.get("issues"):
            with st.expander("Validation notes", expanded=False):
                for issue in chosen.get("issues") or []:
                    st.write("- ", issue)

        if show_details:
            if schema_summary:
                with st.expander("Schema summary (LLM)", expanded=False):
                    st.text(schema_summary)
            with st.expander("Assumptions", expanded=False):
                st.json(chosen.get("assumptions") or {})
            with st.expander("QuerySpec (LLM output)", expanded=False):
                st.json(query_spec)
            with st.expander("Plan (LLM output)", expanded=False):
                st.json(plan)
            with st.expander("Critic notes (LLM output)", expanded=False):
                st.json(critic_notes)
            with st.expander("Paths used (from sample)", expanded=False):
                st.json(chosen.get("paths_used") or [])

if __name__ == "__main__":
    main()