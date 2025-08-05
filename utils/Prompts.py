# utils/prompts.py

SNOWFLAKE_SQL_GENERATION_PROMPT = """You are an expert Snowflake SQL generator specializing in parsing and querying JSON data stored in Snowflake tables.

JSON DATA STRUCTURE:
{json_structure}

TABLE INFORMATION:
- Table Name: {table_name}
- JSON Column: {json_column}
- The JSON data is stored as a VARIANT type in Snowflake

SNOWFLAKE JSON QUERY PATTERNS:

1. Extract simple values:
   SELECT {json_column}:field_name::string as field_name
   FROM {table_name}

2. Extract nested values:
   SELECT {json_column}:level1.level2::string as nested_value
   FROM {table_name}

3. Extract from arrays:
   SELECT {json_column}:array_field[0]::string as first_item
   FROM {table_name}

4. Flatten arrays:
   SELECT 
     f.value:field::string as field_value
   FROM {table_name},
   LATERAL FLATTEN(input => {json_column}:array_field) f

5. Filter by JSON values:
   SELECT *
   FROM {table_name}
   WHERE {json_column}:field::string = 'value'

6. Aggregate JSON data:
   SELECT 
     {json_column}:category::string as category,
     COUNT(*) as count,
     AVG({json_column}:amount::number) as avg_amount
   FROM {table_name}
   GROUP BY {json_column}:category::string

USER QUERY: {user_query}

SNOWFLAKE-SPECIFIC RULES:
1. Always use :: for type casting (::string, ::number, ::boolean, ::date)
2. Use LATERAL FLATTEN for array processing
3. Use : notation for JSON path traversal
4. Handle NULL values with proper casting
5. Use VARIANT data type functions when needed
6. Consider using TRY_PARSE_JSON for validation
7. Use array indexing with [0], [1], etc.

RESPONSE FORMAT:
Return ONLY a JSON object with these exact keys:
{{
  "sql": "Snowflake SQL query with JSON functions",
  "explanation": "what this query does and which JSON paths it accesses",
  "json_paths_used": ["list of JSON paths accessed"],
  "snowflake_functions": ["list of Snowflake functions used"]
}}
"""

SNOWFLAKE_SCHEMA_ANALYSIS_PROMPT = """Analyze this JSON structure for Snowflake querying.

JSON SAMPLE:
{json_sample}

Identify:
1. All possible JSON paths that can be queried
2. Data types for proper Snowflake casting
3. Arrays that might need FLATTEN operations
4. Nested objects and their structure
5. Common query patterns this data supports

Create a comprehensive analysis for Snowflake JSON querying.
"""

SNOWFLAKE_QUERY_REFINEMENT_PROMPT = """The previous Snowflake SQL query had issues. Please refine it.

ORIGINAL QUERY: {original_query}
ISSUES FOUND: {issues}
USER INTENT: {user_query}
JSON STRUCTURE: {json_structure}
TABLE INFO: Table: {table_name}, JSON Column: {json_column}

Generate an improved Snowflake SQL query that:
1. Fixes the identified issues
2. Uses proper Snowflake JSON syntax
3. Includes appropriate type casting
4. Handles edge cases (NULLs, missing fields)

Return the same JSON format:
{{
  "sql": "improved Snowflake SQL query",
  "explanation": "what was fixed and how it works",
  "json_paths_used": ["JSON paths accessed"],
  "snowflake_functions": ["Snowflake functions used"]
}}
"""

# Snowflake JSON function examples
SNOWFLAKE_JSON_EXAMPLES = {
    "simple_extraction": {
        "description": "Extract simple JSON field",
        "json_path": "data:name",
        "sql": "SELECT data:name::string as customer_name FROM customers"
    },
    "nested_extraction": {
        "description": "Extract nested JSON field",
        "json_path": "data:profile.age",
        "sql": "SELECT data:profile.age::number as customer_age FROM customers"
    },
    "array_indexing": {
        "description": "Access array element by index",
        "json_path": "data:orders[0].total",
        "sql": "SELECT data:orders[0].total::number as first_order_total FROM customers"
    },
    "array_flattening": {
        "description": "Flatten array to rows",
        "json_path": "data:orders",
        "sql": """
        SELECT 
          c.data:name::string as customer_name,
          f.value:order_id::string as order_id,
          f.value:total::number as order_total
        FROM customers c,
        LATERAL FLATTEN(input => c.data:orders) f
        """
    },
    "filtering": {
        "description": "Filter by JSON field values",
        "json_path": "data:status",
        "sql": "SELECT * FROM customers WHERE data:status::string = 'active'"
    },
    "aggregation": {
        "description": "Aggregate JSON data",
        "json_path": "data:region, data:revenue",
        "sql": """
        SELECT 
          data:region::string as region,
          SUM(data:revenue::number) as total_revenue,
          COUNT(*) as customer_count
        FROM customers
        GROUP BY data:region::string
        """
    }
}

def get_snowflake_examples():
    """Return Snowflake-specific JSON query examples"""
    examples = []
    for key, example in SNOWFLAKE_JSON_EXAMPLES.items():
        examples.append({
            "title": example["description"],
            "json_path": example["json_path"],
            "sql": example["sql"].strip()
        })
    return examples

def generate_json_paths(json_sample, prefix="data"):
    """Generate all possible JSON paths from a sample"""
    paths = []
    
    def extract_paths(obj, current_path):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}:{key}"
                paths.append(new_path)
                if isinstance(value, (dict, list)):
                    extract_paths(value, new_path)
        elif isinstance(obj, list) and len(obj) > 0:
            # Add array access pattern
            paths.append(f"{current_path}[0]")
            if isinstance(obj[0], (dict, list)):
                extract_paths(obj[0], f"{current_path}[0]")
    
    extract_paths(json_sample, prefix)
    return paths