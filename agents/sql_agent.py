# agents/sql_agent.py
from typing import Dict, Any, Optional, List
try:
    from langchain_openai import ChatOpenAI
    from langchain_core.messages import HumanMessage, SystemMessage
except ImportError:
    # Fallback to older imports
    from langchain.chat_models import ChatOpenAI
    from langchain.schema import HumanMessage, SystemMessage
import os

class SnowflakeSQLAgent:
    """Agent responsible for generating Snowflake SQL queries for JSON data parsing"""
    
    # Embed the prompt directly to avoid import issues
    SNOWFLAKE_SQL_PROMPT = """You are an expert Snowflake SQL generator specializing in parsing and querying JSON data stored in Snowflake tables.

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
    
    def __init__(self, api_key: Optional[str] = None):
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0.1,
            api_key=api_key or os.getenv("OPENAI_API_KEY")
        )
        self.snowflake_context = ""
        self.table_name = "json_table"
        self.json_column = "data"
    
    def set_snowflake_context(self, context: str, table_name: str = "json_table", 
                            json_column: str = "data"):
        """Set the Snowflake JSON context for SQL generation"""
        self.snowflake_context = context
        self.table_name = table_name
        self.json_column = json_column
    
    def generate_snowflake_sql(self, user_query: str) -> Dict[str, Any]:
        """
        Generate Snowflake SQL query for JSON data parsing
        """
        system_prompt = self.SNOWFLAKE_SQL_PROMPT.format(
            json_structure=self.snowflake_context,
            table_name=self.table_name,
            json_column=self.json_column,
            user_query=user_query
        )
        
        user_message = f"Generate Snowflake SQL for: {user_query}"
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message)
        ]
        
        try:
            # Use invoke instead of calling directly
            response = self.llm.invoke(messages)
            
            # Handle different response formats
            response_content = response.content if hasattr(response, 'content') else str(response)
            
            # Parse the response - expect JSON format
            import json
            result = json.loads(response_content)
            
            # Validate required fields
            required_fields = ["sql", "explanation", "json_paths_used", "snowflake_functions"]
            for field in required_fields:
                if field not in result:
                    result[field] = []
            
            result["status"] = "success"
            result["database_type"] = "snowflake"
            return result
            
        except json.JSONDecodeError:
            # Fallback if LLM doesn't return proper JSON
            return {
                "sql": response_content,
                "explanation": "Generated Snowflake SQL query for JSON parsing",
                "json_paths_used": ["Unknown"],
                "snowflake_functions": ["Unknown"],
                "status": "success_fallback",
                "database_type": "snowflake"
            }
        except Exception as e:
            return {
                "sql": "",
                "explanation": f"Error generating Snowflake SQL: {str(e)}",
                "json_paths_used": [],
                "snowflake_functions": [],
                "status": "error",
                "database_type": "snowflake",
                "error": str(e)
            }
    
    def validate_snowflake_sql(self, sql: str) -> Dict[str, Any]:
        """
        Validate Snowflake SQL specifically for JSON operations
        """
        try:
            import sqlparse
            
            # Parse the SQL
            parsed = sqlparse.parse(sql)[0]
            sql_lower = sql.lower().strip()
            
            validation_result = {
                "is_valid": True,
                "issues": [],
                "warnings": [],
                "snowflake_features": []
            }
            
            # Check for Snowflake JSON syntax
            if ":" not in sql and "json" not in sql_lower:
                validation_result["warnings"].append("No JSON path syntax (:) detected - make sure you're querying JSON fields")
            
            # Check for proper type casting
            if "::" not in sql:
                validation_result["warnings"].append("No type casting (::) detected - consider adding ::string, ::number, etc.")
            
            # Check for dangerous operations
            dangerous_keywords = ["drop", "delete", "truncate", "alter", "create", "insert", "update"]
            for keyword in dangerous_keywords:
                if keyword in sql_lower and not sql_lower.startswith("select"):
                    validation_result["warnings"].append(f"Contains potentially dangerous keyword: {keyword}")
            
            # Check for basic SQL structure
            if not any(keyword in sql_lower for keyword in ["select", "with"]):
                validation_result["is_valid"] = False
                validation_result["issues"].append("No SELECT statement found")
            
            # Check for Snowflake-specific features
            snowflake_features = []
            if "flatten" in sql_lower:
                snowflake_features.append("LATERAL FLATTEN for array processing")
            if "::" in sql:
                snowflake_features.append("Type casting with ::")
            if ":" in sql and not "::" in sql.replace("::", ""):
                snowflake_features.append("JSON path notation with :")
            if "variant" in sql_lower:
                snowflake_features.append("VARIANT data type usage")
            
            validation_result["snowflake_features"] = snowflake_features
            
            # Check for common Snowflake JSON patterns
            if self.json_column in sql and ":" in sql:
                validation_result["warnings"].append(f"Good: Using JSON column '{self.json_column}' with path notation")
            
            return validation_result
            
        except Exception as e:
            return {
                "is_valid": False,
                "issues": [f"SQL parsing error: {str(e)}"],
                "warnings": [],
                "snowflake_features": []
            }
    
    def suggest_snowflake_improvements(self, sql: str) -> List[str]:
        """
        Suggest Snowflake-specific improvements for JSON queries
        """
        suggestions = []
        sql_lower = sql.lower()
        
        # Snowflake JSON-specific suggestions
        if "select *" in sql_lower:
            suggestions.append("Consider selecting specific JSON fields instead of SELECT * for better performance")
        
        if ":" in sql and "::" not in sql:
            suggestions.append("Add type casting (::string, ::number, ::boolean) to JSON field extractions")
        
        if "flatten" not in sql_lower and "[" in sql:
            suggestions.append("Consider using LATERAL FLATTEN for array processing instead of array indexing")
        
        if "where" in sql_lower and ":" in sql and "::" not in sql_lower.split("where")[1]:
            suggestions.append("Add type casting in WHERE clauses for proper JSON field comparison")
        
        if "group by" in sql_lower and ":" in sql:
            suggestions.append("Ensure GROUP BY fields use the same JSON path and casting as SELECT fields")
        
        if "order by" not in sql_lower and ("group by" in sql_lower or "aggregate" in sql_lower):
            suggestions.append("Consider adding ORDER BY for consistent result ordering")
        
        # Performance suggestions
        if "limit" not in sql_lower:
            suggestions.append("Consider adding LIMIT clause for testing JSON queries on large datasets")
        
        if sql.count(":") > 5:
            suggestions.append("Complex JSON query - consider breaking into CTEs for better readability")
        
        return suggestions
    
    def explain_snowflake_query(self, sql: str) -> str:
        """
        Generate a detailed explanation of the Snowflake JSON query
        """
        try:
            system_prompt = f"""You are a Snowflake expert. Explain this Snowflake SQL query that works with JSON data.

Table Context:
- Table: {self.table_name}
- JSON Column: {self.json_column} (VARIANT type)

Focus on:
1. What JSON paths are being accessed
2. How Snowflake JSON functions work in this query  
3. What type casting is being performed
4. Any array processing with FLATTEN
5. The business purpose of the query
6. Performance considerations

Keep the explanation clear for both technical and business users.
"""
            
            user_message = f"Explain this Snowflake JSON query: {sql}"
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message)
            ]
            
            response = self.llm.invoke(messages)
            return response.content if hasattr(response, 'content') else str(response)
            
        except Exception as e:
            return f"Could not generate explanation: {str(e)}"
    
    def generate_sample_queries(self, json_paths: List[str]) -> List[Dict[str, str]]:
        """
        Generate sample Snowflake queries based on available JSON paths
        """
        samples = []
        
        # Basic extraction samples
        for path in json_paths[:5]:
            if ":" in path and "[" not in path:
                field_name = path.split(":")[-1]
                samples.append({
                    "title": f"Extract {field_name}",
                    "sql": f"SELECT {self.json_column}:{path}::string as {field_name} FROM {self.table_name}",
                    "description": f"Extract the {field_name} field from JSON data"
                })
        
        # Array processing samples
        array_paths = [path for path in json_paths if "[" in path and "::" not in path]
        if array_paths:
            path = array_paths[0].replace("[0]", "").replace("[*]", "")
            samples.append({
                "title": "Flatten Array",
                "sql": f"""SELECT 
  f.value 
FROM {self.table_name},
LATERAL FLATTEN(input => {self.json_column}:{path}) f""",
                "description": f"Flatten the {path} array into separate rows"
            })
        
        # Filtering sample
        if json_paths:
            path = json_paths[0]
            field_name = path.split(":")[-1]
            samples.append({
                "title": f"Filter by {field_name}",
                "sql": f"SELECT * FROM {self.table_name} WHERE {self.json_column}:{path}::string = 'value'",
                "description": f"Filter records where {field_name} equals a specific value"
            })
        
        return samples