# app.py
import streamlit as st
import json
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import our Snowflake-specific modules
from agents.schema_agent import SnowflakeSchemaAgent
from agents.sql_agent import SnowflakeSQLAgent
from agents.graph_flow import SnowflakeSQLGraph

# Page configuration
st.set_page_config(
    page_title="❄️ Snowflake JSON SQL Generator",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced CSS with SVGs and colorful design
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .main {
        font-family: 'Inter', sans-serif;
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    
    .main-header {
        text-align: center;
        padding: 3rem 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #29b5e8 100%);
        color: white;
        border-radius: 25px;
        margin-bottom: 3rem;
        box-shadow: 0 15px 40px rgba(0,0,0,0.15);
        position: relative;
        overflow: hidden;
    }
    
    .main-header h1 {
        font-size: 4rem;
        font-weight: 700;
        margin-bottom: 1rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        position: relative;
        z-index: 1;
    }
    
    .main-header p {
        font-size: 1.4rem;
        font-weight: 300;
        opacity: 0.95;
        position: relative;
        z-index: 1;
    }
    
    .section-container {
        background: white;
        border-radius: 20px;
        padding: 2rem;
        margin: 2rem 0;
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        border: 1px solid rgba(102, 126, 234, 0.1);
        position: relative;
        overflow: hidden;
    }
    
    .section-container::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 5px;
        background: linear-gradient(90deg, #ff6b6b, #4ecdc4, #45b7d1, #96ceb4, #feca57);
    }
    
    .sql-box {
        background: linear-gradient(145deg, #1a1a1a 0%, #2d2d2d 100%);
        color: #00ff88;
        border-radius: 15px;
        padding: 2rem;
        font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
        font-size: 1rem;
        line-height: 1.8;
        white-space: pre-wrap;
        border: 2px solid #29b5e8;
        box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        overflow-x: auto;
        margin: 1rem 0;
    }
    
    .json-path {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 25px;
        font-family: 'Monaco', monospace;
        font-size: 0.9rem;
        margin: 0.3rem;
        display: inline-block;
        box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
        transition: all 0.3s ease;
    }
    
    .json-path:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.6);
    }
    
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8f9ff 100%);
        border-radius: 20px;
        padding: 2rem;
        text-align: center;
        box-shadow: 0 8px 25px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .metric-card:hover {
        transform: translateY(-10px) scale(1.02);
        box-shadow: 0 15px 40px rgba(0,0,0,0.2);
    }
    
    .metric-value {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(45deg, #667eea, #764ba2, #ff6b6b);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
    }
    
    .metric-label {
        font-size: 1rem;
        color: #666;
        font-weight: 500;
    }
    
    .about-section {
        background: linear-gradient(135deg, #96ceb4 0%, #ffeaa7 100%);
        border-radius: 25px;
        padding: 3rem;
        margin: 3rem 0;
        color: #2d3436;
        position: relative;
        overflow: hidden;
    }
    
    .about-section h3 {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 1.5rem;
        color: #2d3436;
        position: relative;
        z-index: 1;
    }
    
    .feature-highlight {
        background: rgba(255, 255, 255, 0.3);
        border-radius: 15px;
        padding: 1rem;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.2);
        position: relative;
        z-index: 1;
    }
    
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Keep normal spacing for better readability */
    
    .stButton > button {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
        color: white;
        border: none;
        border-radius: 15px;
        padding: 1rem 2rem;
        font-weight: 600;
        font-size: 1.1rem;
        transition: all 0.3s ease;
        box-shadow: 0 8px 25px rgba(255, 107, 107, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-3px);
        box-shadow: 0 12px 35px rgba(255, 107, 107, 0.5);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def initialize_snowflake_components():
    """Initialize Snowflake-specific components"""
    schema_agent = SnowflakeSchemaAgent()
    
    # Check if API key is available
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        sql_agent = SnowflakeSQLAgent(api_key)
    else:
        sql_agent = SnowflakeSQLAgent()  # Will use environment variable
    
    sql_graph = SnowflakeSQLGraph(schema_agent, sql_agent)
    
    return schema_agent, sql_agent, sql_graph

def check_api_key_status():
    """Check if OpenAI API key is properly configured"""
    api_key = os.getenv("OPENAI_API_KEY")
    
    if not api_key:
        return False, "No API key found in environment variables"
    elif api_key == "your_api_key_here":
        return False, "Please replace 'your_api_key_here' with your actual OpenAI API key in .env file"
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

def display_json_structure(json_data, max_depth=3, current_depth=0):
    """Display JSON structure in a readable format"""
    if current_depth >= max_depth:
        st.text("...")
        return
    
    if isinstance(json_data, dict):
        for key, value in list(json_data.items())[:5]:  # Show first 5 keys
            if isinstance(value, (dict, list)):
                with st.expander(f"📁 {key}", expanded=current_depth < 2):
                    display_json_structure(value, max_depth, current_depth + 1)
            else:
                st.text(f"• {key}: {type(value).__name__} = {str(value)[:50]}...")
        if len(json_data) > 5:
            st.text(f"... and {len(json_data) - 5} more fields")
    elif isinstance(json_data, list):
        st.text(f"Array with {len(json_data)} items")
        if json_data and current_depth < max_depth:
            st.text("First item structure:")
            display_json_structure(json_data[0], max_depth, current_depth + 1)

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>❄️ Snowflake JSON SQL Generator</h1>
        <p>Transform natural language into powerful Snowflake SQL queries for JSON data</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Check API key status silently
    api_key_valid, api_key_message = check_api_key_status()
    
    # Only show error if API key is not configured
    if not api_key_valid:
        st.error("⚠️ OpenAI API key not configured. Please add OPENAI_API_KEY to your .env file.")
        st.markdown("""
        **To fix this:**
        1. Create/edit `.env` file in project root
        2. Add: `OPENAI_API_KEY=your_actual_key_here`
        3. Restart the Streamlit app
        """)
        return
    
    # Initialize components
    try:
        schema_agent, sql_agent, sql_graph = initialize_snowflake_components()
    except Exception as e:
        st.error(f"Failed to initialize components: {str(e)}")
        st.stop()
    
    # Sidebar
    with st.sidebar:
        # Snowflake Table Information
        st.header("🏗️ Table Configuration")
        table_name = st.text_input("Table Name", value="customer_data", 
                                  help="Your Snowflake table name")
        json_column = st.text_input("JSON Column", value="raw_data", 
                                   help="Column containing JSON data (VARIANT type)")
    
    # JSON Data Input Section
    st.markdown('<div class="section-container">', unsafe_allow_html=True)
    st.markdown("## 📋 How would you like to input JSON data?")
    st.markdown("Choose your preferred method to provide the JSON data structure:")
    
    # Input method selection
    input_method = st.radio(
        "Input Method",
        ["🎯 Use Sample Data", "📁 Upload JSON File", "✏️ Paste JSON Text"],
        horizontal=True,
        label_visibility="collapsed"
    )
    
    json_data = None
    
    if "📁" in input_method:
        st.markdown("### 📁 Upload Your JSON File")
        uploaded_file = st.file_uploader("Choose a JSON file", type="json", 
                                        help="Upload a JSON file to analyze its structure")
        if uploaded_file is not None:
            try:
                json_data = json.load(uploaded_file)
                st.success("✅ JSON file loaded successfully!")
            except Exception as e:
                st.error(f"❌ Error reading JSON file: {str(e)}")
    
    elif "✏️" in input_method:
        st.markdown("### ✏️ Paste Your JSON Data")
        json_text = st.text_area(
            "Paste your JSON data here:",
            height=200,
            placeholder='{"customers": [{"id": 1, "name": "John", "profile": {"age": 30}}]}',
            help="Paste your JSON data structure here"
        )
        if json_text:
            try:
                json_data = json.loads(json_text)
                st.success("✅ JSON parsed successfully!")
            except Exception as e:
                st.error(f"❌ Invalid JSON: {str(e)}")
    
    else:  # Sample data
        st.markdown("### 🎯 Using Sample E-commerce Data")
        json_data = {
            "customers": [
                {
                    "customer_id": 1,
                    "name": "Alice Johnson",
                    "email": "alice@email.com",
                    "profile": {
                        "age": 28,
                        "city": "New York",
                        "premium": True,
                        "preferences": {
                            "categories": ["electronics", "books"],
                            "notifications": {"email": True, "sms": False}
                        }
                    },
                    "orders": [
                        {
                            "order_id": "ORD001",
                            "date": "2024-01-10",
                            "total": 299.99,
                            "items": [
                                {"product": "Laptop", "price": 999.99, "qty": 1},
                                {"product": "Mouse", "price": 29.99, "qty": 2}
                            ]
                        }
                    ]
                }
            ]
        }
        st.success("✅ Sample e-commerce data loaded with nested customer profiles, orders, and items")
    
    # Display JSON structure
    if json_data:
        with st.expander("🔍 Explore JSON Structure", expanded=False):
            display_json_structure(json_data)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Query Input Section
    if json_data:
        st.markdown('<div class="section-container">', unsafe_allow_html=True)
        st.markdown("## 🤖 Generate Your Snowflake SQL")
        st.markdown("Describe what you want to extract from your JSON data in natural language:")
        
        # Query input
        user_query = st.text_area(
            "Enter your question:",
            height=120,
            placeholder="e.g., Extract customer names and their cities from the profile data\ne.g., Get all orders with items over $100\ne.g., Find premium customers who prefer email notifications",
            help="Enter your question in plain English - the AI will convert it to Snowflake SQL"
        )
        
        # Generate button and settings
        col1, col2 = st.columns([3, 1])
        with col1:
            generate_button = st.button("🚀 Generate Snowflake SQL", type="primary",
                                      disabled=not api_key_valid or not user_query.strip(),
                                      use_container_width=True)
        
        with col2:
            max_retries = st.selectbox("Max Retries", [0, 1, 2, 3], index=2,
                                     help="How many times to retry if SQL generation fails")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Results Section - Below the input
        if generate_button and user_query and json_data:
            st.markdown('<div class="section-container">', unsafe_allow_html=True)
            st.markdown("## ✨ Your Snowflake SQL Results")
            
            with st.spinner("❄️ Generating your Snowflake SQL query..."):
                try:
                    # Run the Snowflake SQL generation workflow
                    result = sql_graph.run(
                        user_query=user_query,
                        json_structure=json_data,
                        table_name=table_name,
                        json_column=json_column,
                        max_retries=max_retries
                    )
                    
                    if result.get("success"):
                        # Format the SQL nicely
                        formatted_sql = format_sql_nicely(result["sql"])
                        
                        # Display generated SQL
                        st.markdown("### ❄️ Generated SQL Query")
                        st.markdown(f'<div class="sql-box">{formatted_sql}</div>', 
                                  unsafe_allow_html=True)
                        
                        # Copy code block
                        st.code(formatted_sql, language="sql")
                        
                        # Display explanation and paths in columns
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if result.get("explanation"):
                                st.markdown("### 💭 Query Explanation")
                                st.info(result["explanation"])
                        
                        with col2:
                            if result.get("json_paths_used"):
                                st.markdown("### 🛤️ JSON Paths Used")
                                for path in result["json_paths_used"]:
                                    st.markdown(f'<span class="json-path">{json_column}:{path}</span>', 
                                              unsafe_allow_html=True)
                        
                        # Display retry info if any
                        if result.get("retry_count", 0) > 0:
                            st.info(f"🔄 Query was refined {result['retry_count']} time(s) for optimization")
                    
                    else:
                        st.error("❌ Failed to generate Snowflake SQL")
                        if result.get("error"):
                            st.error(f"Details: {result['error']}")
                
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    st.error("Try running the terminal test: `python terminal_test.py`")
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Analytics Dashboard
    if json_data:
        st.markdown('<div class="section-container">', unsafe_allow_html=True)
        st.markdown("## 📊 JSON Structure Analytics")
        
        # Analyze the JSON structure
        analysis = schema_agent.analyze_json_for_snowflake(json_data, table_name, json_column)
        
        # Metrics in beautiful cards
        col1, col2, col3, col4 = st.columns(4)
        
        metrics = [
            ("🛤️", len(analysis.get("json_paths", [])), "JSON Paths"),
            ("📊", sum(1 for arr in analysis.get("arrays", []) if arr.get("needs_flatten", False)), "Arrays for FLATTEN"),
            ("🗂️", len(analysis.get("nested_objects", [])), "Nested Objects"),
            ("🔍", len(analysis.get("queryable_fields", [])), "Queryable Fields")
        ]
        
        for i, (icon, value, label) in enumerate(metrics):
            with [col1, col2, col3, col4][i]:
                st.markdown(f'''
                <div class="metric-card">
                    <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">{icon}</div>
                    <div class="metric-value">{value}</div>
                    <div class="metric-label">{label}</div>
                </div>
                ''', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Snowflake Examples Section
    st.markdown('<div class="section-container">', unsafe_allow_html=True)
    st.markdown("## ❄️ Snowflake JSON Query Examples")
    st.markdown("**Learn common Snowflake JSON patterns:**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Basic Operations:**")
        st.code("""-- Extract simple field
SELECT data:name::string FROM table

-- Extract nested field  
SELECT data:profile.age::number FROM table

-- Array element access
SELECT data:orders[0].total::number FROM table""", language="sql")
    
    with col2:
        st.markdown("**Advanced Operations:**")
        st.code("""-- Flatten arrays to rows
SELECT f.value:item::string 
FROM table,
LATERAL FLATTEN(input => data:items) f

-- Filter by JSON field
SELECT * FROM table 
WHERE data:status::string = 'active'""", language="sql")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # About section with project information
    st.markdown('<div class="about-section">', unsafe_allow_html=True)
    st.markdown("### 🎯 About This Snowflake Tool")
    
    with st.container():
        st.markdown("#### 📋 What This Project Does")
        st.markdown("""
        This is an **LLM-powered SQL Generator** that converts natural language questions into Snowflake SQL queries for JSON data. 
        Simply describe what you want to extract from your JSON data in plain English, and the AI will generate the corresponding 
        Snowflake SQL with proper JSON functions.
        """)
        
        st.markdown("#### ⚡ Key Features")
        st.markdown("""
        - **❄️ Snowflake-specific syntax:** Uses `:` notation and `::` type casting
        - **🔄 Array processing:** Generates LATERAL FLATTEN for complex arrays  
        - **🔍 Path analysis:** Discovers all queryable JSON paths
        - **⚡ Performance optimized:** Suggests best practices and optimizations
        - **🏭 Production ready:** Generates SQL you can run directly in Snowflake
        """)
        
        st.markdown("#### ❄️ Snowflake JSON Functions Supported")
        st.markdown("""
        - **Path notation:** `data:field::string`
        - **Nested access:** `data:level1.level2::number`
        - **Array indexing:** `data:array[0]::string`
        - **Array flattening:** `LATERAL FLATTEN(input => data:array)`
        - **Type casting:** `::string, ::number, ::boolean, ::date`
        """)
        
    
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()