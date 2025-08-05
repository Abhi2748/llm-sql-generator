# Snowflake JSON SQL Generator

## Overview

An LLM-powered tool that converts natural language queries into Snowflake SQL for querying JSON data stored in VARIANT columns. The application uses OpenAI's GPT models to understand user intent and generate appropriate Snowflake-specific SQL syntax.

## Project Structure

```
/
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

- Natural language to Snowflake SQL conversion
- JSON structure analysis with path discovery
- Support for nested objects and arrays
- LATERAL FLATTEN generation for array processing
- Snowflake-specific syntax (`:` notation and `::` type casting)
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
3. **Generate SQL**: The system analyzes the JSON structure and generates Snowflake SQL
4. **Review Results**: Get formatted SQL with explanations and JSON paths used

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
- Maximum 3 retries for SQL generation if initial attempts fail
- Web interface provides visual JSON structure exploration
