# agents/graph_flow.py
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from typing_extensions import Annotated, TypedDict

class SnowflakeGraphState(TypedDict):
    """State object for the Snowflake SQL generation workflow"""
    user_query: str
    json_structure: Dict[str, Any]
    table_name: str
    json_column: str
    snowflake_context: str
    sql_result: Dict[str, Any]
    validation_result: Dict[str, Any]
    retry_count: int
    max_retries: int
    messages: Annotated[List, add_messages]
    final_result: Dict[str, Any]

class SnowflakeSQLGraph:
    """LangGraph workflow for Snowflake SQL generation"""
    
    def __init__(self, schema_agent, sql_agent):
        self.schema_agent = schema_agent
        self.sql_agent = sql_agent
        self.graph = None
        self.build_graph()
    
    def build_graph(self):
        """Build the LangGraph workflow for Snowflake SQL generation"""
        
        workflow = StateGraph(SnowflakeGraphState)
        
        # Add nodes
        workflow.add_node("analyze_json_structure", self.analyze_json_structure_node)
        workflow.add_node("generate_snowflake_sql", self.generate_snowflake_sql_node)
        workflow.add_node("validate_snowflake_sql", self.validate_snowflake_sql_node)
        workflow.add_node("retry_sql_generation", self.retry_sql_generation_node)
        workflow.add_node("finalize_snowflake_result", self.finalize_snowflake_result_node)
        
        # Define the flow
        workflow.set_entry_point("analyze_json_structure")
        
        # JSON analysis leads to SQL generation
        workflow.add_edge("analyze_json_structure", "generate_snowflake_sql")
        
        # SQL generation leads to validation
        workflow.add_edge("generate_snowflake_sql", "validate_snowflake_sql")
        
        # Conditional logic after validation
        workflow.add_conditional_edges(
            "validate_snowflake_sql",
            self.should_retry_snowflake,
            {
                "retry": "retry_sql_generation",
                "finalize": "finalize_snowflake_result"
            }
        )
        
        # Retry leads back to validation
        workflow.add_edge("retry_sql_generation", "validate_snowflake_sql")
        
        # Finalize ends the workflow
        workflow.add_edge("finalize_snowflake_result", END)
        
        # Compile the graph
        self.graph = workflow.compile()
    
    def analyze_json_structure_node(self, state: SnowflakeGraphState) -> SnowflakeGraphState:
        """Node to analyze JSON structure for Snowflake querying"""
        try:
            json_structure = state["json_structure"]
            table_name = state["table_name"]
            json_column = state["json_column"]
            
            # Set table info in schema agent
            self.schema_agent.set_table_info(table_name, json_column)
            
            # Analyze JSON structure for Snowflake
            analysis = self.schema_agent.analyze_json_for_snowflake(
                json_structure, table_name, json_column
            )
            
            # Generate Snowflake context
            snowflake_context = self.schema_agent.get_snowflake_context()
            
            # Set context in SQL agent
            self.sql_agent.set_snowflake_context(snowflake_context, table_name, json_column)
            
            state["snowflake_context"] = snowflake_context
            state["messages"].append({
                "role": "system",
                "content": f"JSON structure analyzed for Snowflake. Found {len(analysis.get('json_paths', []))} queryable paths."
            })
            
        except Exception as e:
            state["snowflake_context"] = ""
            state["messages"].append({
                "role": "assistant",
                "content": f"JSON analysis failed: {str(e)}"
            })
        
        return state
    
    def generate_snowflake_sql_node(self, state: SnowflakeGraphState) -> SnowflakeGraphState:
        """Node to generate Snowflake SQL for JSON querying"""
        try:
            user_query = state["user_query"]
            
            # Generate Snowflake SQL
            sql_result = self.sql_agent.generate_snowflake_sql(user_query)
            
            state["sql_result"] = sql_result
            state["messages"].append({
                "role": "system",
                "content": f"Snowflake SQL generated: {sql_result.get('status', 'unknown status')}"
            })
            
        except Exception as e:
            state["sql_result"] = {
                "sql": "",
                "explanation": f"Snowflake SQL generation failed: {str(e)}",
                "status": "error",
                "error": str(e),
                "database_type": "snowflake"
            }
            state["messages"].append({
                "role": "assistant",
                "content": f"Snowflake SQL generation failed: {str(e)}"
            })
        
        return state
    
    def validate_snowflake_sql_node(self, state: SnowflakeGraphState) -> SnowflakeGraphState:
        """Node to validate Snowflake SQL"""
        try:
            sql = state["sql_result"].get("sql", "")
            
            if not sql:
                state["validation_result"] = {
                    "is_valid": False,
                    "issues": ["No SQL generated"],
                    "warnings": [],
                    "snowflake_features": []
                }
            else:
                # Validate using Snowflake-specific validation
                validation_result = self.sql_agent.validate_snowflake_sql(sql)
                state["validation_result"] = validation_result
            
            state["messages"].append({
                "role": "system",
                "content": f"Snowflake SQL validation: {'passed' if state['validation_result']['is_valid'] else 'failed'}"
            })
            
        except Exception as e:
            state["validation_result"] = {
                "is_valid": False,
                "issues": [f"Validation error: {str(e)}"],
                "warnings": [],
                "snowflake_features": []
            }
            state["messages"].append({
                "role": "assistant",
                "content": f"Snowflake SQL validation failed: {str(e)}"
            })
        
        return state
    
    def retry_sql_generation_node(self, state: SnowflakeGraphState) -> SnowflakeGraphState:
        """Node to retry Snowflake SQL generation with improvements"""
        try:
            # Increment retry counter
            state["retry_count"] = state.get("retry_count", 0) + 1
            
            # Get issues from validation
            issues = state["validation_result"].get("issues", [])
            warnings = state["validation_result"].get("warnings", [])
            
            # Create refined prompt with Snowflake-specific guidance
            refined_query = f"""
            Original query: {state['user_query']}
            Previous Snowflake SQL had these issues: {', '.join(issues + warnings)}
            
            Please generate improved Snowflake SQL that:
            1. Uses proper JSON path syntax with : notation
            2. Includes appropriate type casting with ::
            3. Uses LATERAL FLATTEN for array processing if needed
            4. Follows Snowflake JSON best practices
            
            Table: {state['table_name']}, JSON Column: {state['json_column']}
            """
            
            # Generate improved Snowflake SQL
            sql_result = self.sql_agent.generate_snowflake_sql(refined_query)
            state["sql_result"] = sql_result
            
            state["messages"].append({
                "role": "system",
                "content": f"Snowflake SQL retry #{state['retry_count']}: Generated improved SQL"
            })
            
        except Exception as e:
            state["messages"].append({
                "role": "assistant",
                "content": f"Snowflake SQL retry failed: {str(e)}"
            })
        
        return state
    
    def finalize_snowflake_result_node(self, state: SnowflakeGraphState) -> SnowflakeGraphState:
        """Node to finalize Snowflake SQL generation result"""
        try:
            # Compile final result with Snowflake-specific information
            final_result = {
                "success": state["validation_result"]["is_valid"],
                "sql": state["sql_result"].get("sql", ""),
                "explanation": state["sql_result"].get("explanation", ""),
                "json_paths_used": state["sql_result"].get("json_paths_used", []),
                "snowflake_functions": state["sql_result"].get("snowflake_functions", []),
                "validation": state["validation_result"],
                "retry_count": state.get("retry_count", 0),
                "messages": state["messages"],
                "database_type": "snowflake",
                "table_name": state["table_name"],
                "json_column": state["json_column"]
            }
            
            state["final_result"] = final_result
            
            state["messages"].append({
                "role": "system",
                "content": "Snowflake SQL generation workflow completed"
            })
            
        except Exception as e:
            state["final_result"] = {
                "success": False,
                "error": f"Finalization failed: {str(e)}",
                "messages": state.get("messages", []),
                "database_type": "snowflake"
            }
        
        return state
    
    def should_retry_snowflake(self, state: SnowflakeGraphState) -> str:
        """Decide whether to retry Snowflake SQL generation"""
        # Don't retry if validation passed
        if state["validation_result"]["is_valid"]:
            return "finalize"
        
        # Don't retry if we've hit max retries
        retry_count = state.get("retry_count", 0)
        max_retries = state.get("max_retries", 2)
        
        if retry_count >= max_retries:
            return "finalize"
        
        # Retry if we have validation issues
        return "retry"
    
    def run(self, user_query: str, json_structure: Dict[str, Any], 
            table_name: str = "json_table", json_column: str = "data", 
            max_retries: int = 2) -> Dict[str, Any]:
        """
        Run the complete Snowflake SQL generation workflow
        """
        initial_state = SnowflakeGraphState(
            user_query=user_query,
            json_structure=json_structure,
            table_name=table_name,
            json_column=json_column,
            snowflake_context="",
            sql_result={},
            validation_result={},
            retry_count=0,
            max_retries=max_retries,
            messages=[],
            final_result={}
        )
        
        # Run the graph
        final_state = self.graph.invoke(initial_state)
        
        return final_state["final_result"]