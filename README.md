# Snowflake JSON SQL Generator

## Overview

An LLM-focused tool that converts natural language questions into **Snowflake SQL** for querying JSON data stored in **VARIANT** columns.

This project is built as a **LangGraph multi-agent workflow**:
- **SchemaIndex node (deterministic)**: extracts canonical JSON paths, types, and arrays from a sample
- **SchemaSummarizer agent (LLM)**: compresses schema into a short human-readable summary
- **Intent agent (LLM)**: question → structured `QuerySpec` JSON (select/filters/group/aggregations)
- **Plan agent (LLM)**: chooses 2–3 candidate strategies (doc-per-row vs event-per-row vs item grain)
- **SQLCompiler node (deterministic)**: compiles each plan into Snowflake SQL (`LATERAL FLATTEN`, `:` traversal, `::` casts)
- **StaticValidate node (deterministic)**: scores/ranks candidates and explains issues
- **Critic + Repair agents (LLM)**: critique and optionally patch QuerySpec/plan with a retry loop

## Project Structure

```
/
├── workflow/
│   ├── state.py           # LangGraph state
│   ├── graph.py           # LangGraph build + run
│   ├── llm.py             # LLM config + robust JSON extraction
│   ├── prompt_loader.py   # Loads prompts from prompts/
│   └── nodes/             # Deterministic + LLM agent nodes
├── prompts/               # Prompt templates for each LLM agent role
├── data/
│   └── sample_data.json   # Sample JSON data for testing
├── tests/                 # Offline tests (and a mocked-LLM graph test)
├── app.py                 # Streamlit web application
├── .env                   # Environment variables (API keys)
├── .env.example           # Template for environment setup
├── requirements.txt       # Python dependencies
└── README.md
```

## Features

- LangGraph multi-agent workflow (schema summarizer, intent, planner, critic, repair)
- Deterministic Snowflake SQL compilation (`LATERAL FLATTEN`, `:` traversal, `::` casts)
- Multiple ranked candidates to handle row-grain ambiguity
- No Snowflake connection required (static validation + LLM critique/repair)
- Simple Streamlit UI

## Setup

1. Clone the repository

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure OpenAI API key:
   - Copy `.env.example` to `.env`
   - Add your OpenAI API key: `OPENAI_API_KEY=your_actual_key_here`

4. Run the application:
```bash
streamlit run app.py
```

## How It Works

1. **Input JSON Data**: Provide JSON structure via file upload, paste, or use sample data
2. **Ask Questions**: Enter natural language queries about what you want to extract
3. **LangGraph Workflow Runs**: schema index → schema summary → intent → plan → compile → validate → critic/repair loop
4. **Review Results**: Pick the top-ranked candidate (or choose an alternate if your table’s row-grain differs)

## Notes

- The generated SQL is specifically formatted for Snowflake's VARIANT column type
- No Snowflake connection is required; validation is static + agent critique/repair
