# Snowflake JSON SQL Generator

## Overview

An LLM-powered tool that converts natural language questions into **Snowflake SQL** for querying JSON data stored in **VARIANT** columns.

This project uses a **schema-first workflow**:
- Build a JSON **schema index** from your sample data
- Use the LLM only to produce a structured **QuerySpec** (intent), constrained to known JSON paths
- Deterministically compile and rank **2–3 SQL candidates** to handle ambiguity (doc-per-row vs event-per-row)

## Project Structure

```
/
├── core/
│   ├── __init__.py
│   ├── schema_index.py    # Builds canonical JSON path/type index
│   ├── field_catalog.py   # Catalog of queryable leaf fields
│   ├── query_spec.py      # QuerySpec types
│   ├── intent_agent.py    # LLM: natural language -> QuerySpec (no SQL)
│   ├── planner.py         # Builds multiple execution plans (flatten strategy)
│   ├── sql_compiler.py    # Deterministic Snowflake SQL compiler
│   └── static_validate.py # Static validation + ranking
├── agents/
│   ├── __init__.py
│   ├── graph_flow.py      # Orchestrates the SQL generation workflow
│   ├── schema_agent.py    # Analyzes JSON structure and identifies paths
│   └── sql_agent.py       # Generates SQL using LLM
├── data/
│   └── sample_data.json   # Sample JSON data for testing
├── utils/
│   ├── __init__.py
│   └── Prompts.py         # LLM prompt templates
├── app.py                 # Streamlit web application
├── .env                   # Environment variables (API keys)
├── .env.example           # Template for environment setup
├── requirements.txt       # Python dependencies
└── README.md
```

## Features

- Natural language to Snowflake SQL conversion (schema-first)
- JSON schema indexing with canonical path discovery
- Multiple ranked SQL candidates to handle row-grain ambiguity
- Deterministic `LATERAL FLATTEN` generation for arrays
- Snowflake-specific syntax (`:` traversal and `::` casting)
- Interactive web interface built with Streamlit

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
3. **Generate Candidates**: The system builds a schema index, generates a QuerySpec (intent), then compiles 2–3 SQL candidates with different assumptions
4. **Review Results**: Pick the top-ranked candidate (or choose an alternate if your table’s row-grain differs)

## Components

### Agents

- **schema_agent.py**: Analyzes JSON structure, identifies paths, arrays needing FLATTEN, and queryable fields
- **sql_agent.py**: Uses OpenAI GPT to convert natural language to SQL with retry logic
- **graph_flow.py**: Coordinates the workflow between schema analysis and SQL generation

### Utils

- **Prompts.py**: Contains prompt templates for the LLM to generate accurate Snowflake SQL

### Data

- **sample_data.json**: Example e-commerce JSON data for testing and demonstration

## Notes

- The generated SQL is specifically formatted for Snowflake's VARIANT column type
- No Snowflake connection is required; validation is static (schema-aware)
- Web interface provides visual JSON structure exploration and multiple SQL candidates
