# agents/schema_agent.py
import json
from typing import Dict, List, Any, Optional

class SnowflakeSchemaAgent:
    """Agent responsible for analyzing JSON structure for Snowflake querying"""
    
    def __init__(self):
        self.json_structure = {}
        self.json_paths = []
        self.table_name = "json_table"
        self.json_column = "data"
    
    def analyze_json_for_snowflake(self, json_sample: Dict, table_name: str = "json_table", 
                                 json_column: str = "data") -> Dict[str, Any]:
        """
        Analyze JSON structure specifically for Snowflake querying
        """
        self.table_name = table_name
        self.json_column = json_column
        self.json_structure = json_sample
        
        analysis = {
            "table_name": table_name,
            "json_column": json_column,
            "json_paths": self._extract_all_paths(json_sample),
            "data_types": self._analyze_data_types(json_sample),
            "arrays": self._find_arrays(json_sample),
            "nested_objects": self._find_nested_objects(json_sample),
            "queryable_fields": self._identify_queryable_fields(json_sample),
            "snowflake_patterns": self._suggest_query_patterns(json_sample)
        }
        
        return analysis
    
    def _extract_all_paths(self, obj: Any, current_path: str = "") -> List[str]:
        """Extract all possible JSON paths for Snowflake querying"""
        paths = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}:{key}" if current_path else key
                paths.append(new_path)
                
                # Recursively get nested paths
                nested_paths = self._extract_all_paths(value, new_path)
                paths.extend(nested_paths)
                
        elif isinstance(obj, list) and len(obj) > 0:
            # Add array indexing patterns
            for i in range(min(3, len(obj))):  # Show first 3 indices as examples
                indexed_path = f"{current_path}[{i}]"
                paths.append(indexed_path)
            
            # Add general array access pattern
            if current_path:
                paths.append(f"{current_path}[*]")
            
            # Analyze first element structure
            if obj and isinstance(obj[0], (dict, list)):
                nested_paths = self._extract_all_paths(obj[0], f"{current_path}[0]")
                paths.extend(nested_paths)
        
        return paths
    
    def _analyze_data_types(self, obj: Any, current_path: str = "") -> Dict[str, str]:
        """Analyze data types for proper Snowflake casting"""
        types = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}:{key}" if current_path else key
                types[new_path] = self._get_snowflake_type(value)
                
                # Recursively analyze nested objects
                if isinstance(value, (dict, list)):
                    nested_types = self._analyze_data_types(value, new_path)
                    types.update(nested_types)
                    
        elif isinstance(obj, list) and len(obj) > 0:
            # Analyze array element types
            if obj:
                element_type = self._get_snowflake_type(obj[0])
                types[f"{current_path}[*]"] = element_type
                
                if isinstance(obj[0], (dict, list)):
                    nested_types = self._analyze_data_types(obj[0], f"{current_path}[0]")
                    types.update(nested_types)
        
        return types
    
    def _get_snowflake_type(self, value: Any) -> str:
        """Determine appropriate Snowflake type casting"""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "number"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            # Check for date patterns
            if self._looks_like_date(value):
                return "date"
            elif self._looks_like_timestamp(value):
                return "timestamp"
            else:
                return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        elif value is None:
            return "string"  # Default for null values
        else:
            return "variant"
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if string looks like a date"""
        import re
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _looks_like_timestamp(self, value: str) -> bool:
        """Check if string looks like a timestamp"""
        import re
        timestamp_patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO timestamp
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # SQL timestamp
        ]
        return any(re.match(pattern, value) for pattern in timestamp_patterns)
    
    def _find_arrays(self, obj: Any, current_path: str = "") -> List[Dict[str, Any]]:
        """Find all arrays that might need FLATTEN operations"""
        arrays = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}:{key}" if current_path else key
                
                if isinstance(value, list):
                    arrays.append({
                        "path": new_path,
                        "length": len(value),
                        "element_type": type(value[0]).__name__ if value else "unknown",
                        "needs_flatten": isinstance(value[0], dict) if value else False
                    })
                
                # Recursively find nested arrays
                if isinstance(value, (dict, list)):
                    nested_arrays = self._find_arrays(value, new_path)
                    arrays.extend(nested_arrays)
                    
        elif isinstance(obj, list) and len(obj) > 0:
            if isinstance(obj[0], (dict, list)):
                nested_arrays = self._find_arrays(obj[0], f"{current_path}[0]")
                arrays.extend(nested_arrays)
        
        return arrays
    
    def _find_nested_objects(self, obj: Any, current_path: str = "") -> List[str]:
        """Find all nested objects"""
        nested_objects = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}:{key}" if current_path else key
                
                if isinstance(value, dict):
                    nested_objects.append(new_path)
                    # Recursively find deeper nesting
                    deeper_objects = self._find_nested_objects(value, new_path)
                    nested_objects.extend(deeper_objects)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    # Objects inside arrays
                    deeper_objects = self._find_nested_objects(value[0], f"{new_path}[0]")
                    nested_objects.extend(deeper_objects)
        
        return nested_objects
    
    def _identify_queryable_fields(self, obj: Any, current_path: str = "") -> List[Dict[str, Any]]:
        """Identify fields that are commonly queried"""
        queryable = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}:{key}" if current_path else key
                
                # Identify potentially important fields
                if key.lower() in ['id', 'name', 'email', 'status', 'type', 'category', 
                                 'date', 'created_at', 'updated_at', 'amount', 'total']:
                    queryable.append({
                        "path": new_path,
                        "field_name": key,
                        "data_type": self._get_snowflake_type(value),
                        "sample_value": str(value)[:50] if value is not None else "null",
                        "query_type": self._suggest_query_type(key, value)
                    })
                
                # Recursively analyze nested objects
                if isinstance(value, dict):
                    nested_fields = self._identify_queryable_fields(value, new_path)
                    queryable.extend(nested_fields)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    nested_fields = self._identify_queryable_fields(value[0], f"{new_path}[0]")
                    queryable.extend(nested_fields)
        
        return queryable
    
    def _suggest_query_type(self, field_name: str, value: Any) -> str:
        """Suggest what type of queries this field is good for"""
        field_lower = field_name.lower()
        
        if 'id' in field_lower:
            return "filtering, joining"
        elif field_lower in ['name', 'title', 'description']:
            return "filtering, searching"
        elif field_lower in ['status', 'type', 'category']:
            return "filtering, grouping"
        elif field_lower in ['date', 'created_at', 'updated_at']:
            return "filtering, time-based analysis"
        elif field_lower in ['amount', 'price', 'total', 'value']:
            return "aggregation, numerical analysis"
        elif isinstance(value, bool):
            return "filtering, conditional logic"
        else:
            return "general filtering"
    
    def _suggest_query_patterns(self, obj: Any) -> List[Dict[str, str]]:
        """Suggest common Snowflake query patterns for this JSON structure"""
        patterns = []
        
        # Basic extraction patterns
        simple_paths = [path for path in self._extract_all_paths(obj) 
                       if ':' in path and '[' not in path][:5]
        
        for path in simple_paths:
            patterns.append({
                "pattern": "Simple Extraction",
                "example": f"SELECT {self.json_column}:{path}::string FROM {self.table_name}",
                "use_case": f"Extract {path.split(':')[-1]} field"
            })
        
        # Array flattening patterns
        arrays = self._find_arrays(obj)
        for array in arrays[:3]:  # Limit to first 3 arrays
            if array["needs_flatten"]:
                patterns.append({
                    "pattern": "Array Flattening",
                    "example": f"""SELECT f.value FROM {self.table_name},
LATERAL FLATTEN(input => {self.json_column}:{array["path"]}) f""",
                    "use_case": f"Flatten {array['path']} array to rows"
                })
        
        return patterns
    
    def get_snowflake_context(self) -> str:
        """Generate context description for Snowflake SQL generation"""
        if not self.json_structure:
            return "No JSON structure analyzed yet."
        
        analysis = self.analyze_json_for_snowflake(self.json_structure, 
                                                  self.table_name, self.json_column)
        
        context = f"""SNOWFLAKE TABLE CONTEXT:
Table Name: {analysis['table_name']}
JSON Column: {analysis['json_column']} (VARIANT type)

AVAILABLE JSON PATHS:
{chr(10).join(f"- {path}" for path in analysis['json_paths'][:15])}
{'...(showing first 15 paths)' if len(analysis['json_paths']) > 15 else ''}

KEY QUERYABLE FIELDS:
{chr(10).join(f"- {field['path']} ({field['data_type']}) - {field['query_type']}" 
             for field in analysis['queryable_fields'][:10])}

ARRAYS REQUIRING FLATTEN:
{chr(10).join(f"- {array['path']} (length: {array['length']})" 
             for array in analysis['arrays'] if array['needs_flatten'])}

SAMPLE QUERY PATTERNS:
{chr(10).join(f"- {pattern['pattern']}: {pattern['use_case']}" 
             for pattern in analysis['snowflake_patterns'][:5])}
"""
        
        return context
    
    def set_table_info(self, table_name: str, json_column: str):
        """Set the Snowflake table and column information"""
        self.table_name = table_name
        self.json_column = json_column