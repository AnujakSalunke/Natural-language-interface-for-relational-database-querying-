from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import os
import mysql.connector
import google.generativeai as genai
import pandas as pd
from typing import Dict, List, Any

# Configure GenAI Key
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

def create_db_connection(host: str, user: str, password: str, database: str) -> mysql.connector.connection.MySQLConnection:
    """Create MySQL database connection"""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return connection
    except Exception as e:
        raise Exception(f"Error connecting to MySQL Database: {str(e)}")

def get_table_schema(connection: mysql.connector.connection.MySQLConnection) -> Dict[str, Any]:
    """Extract schema information from all tables in the database"""
    cursor = connection.cursor(dictionary=True)
    schema_info = {}
    
    # Get all tables in the database
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    # Get the database name to use as the key for table names
    cursor.execute("SELECT DATABASE()")
    db_name = cursor.fetchone()['DATABASE()']
    
    for table_record in tables:
        # Get the table name using the database name as key
        table_name = table_record[f'Tables_in_{db_name}']
        
        # Get column information
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_data = cursor.fetchall()
        
        # Get foreign key information
        cursor.execute(f"""
            SELECT 
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                TABLE_NAME = '{table_name}'
                AND REFERENCED_TABLE_NAME IS NOT NULL
                AND TABLE_SCHEMA = '{db_name}'
        """)
        foreign_keys = cursor.fetchall()
        
        schema_info[table_name] = {
            'columns': columns,
            'sample_data': sample_data,
            'foreign_keys': foreign_keys
        }
    
    cursor.close()
    return schema_info

def format_schema_for_prompt(schema_info: Dict[str, Any]) -> str:
    """Format schema information into a string for the prompt"""
    schema_text = "Database Schema:\n"
    
    # First, describe all tables and their columns
    for table_name, info in schema_info.items():
        schema_text += f"\nTable: {table_name}\n"
        schema_text += "Columns:\n"
        for col in info['columns']:
            col_name = col['Field']
            col_type = col['Type']
            nullable = 'NULL' if col['Null'] == 'YES' else 'NOT NULL'
            key = col['Key']
            schema_text += f"- {col_name} ({col_type}) {nullable}"
            if key == 'PRI':
                schema_text += " PRIMARY KEY"
            schema_text += "\n"
    
    # Then, describe foreign key relationships
    schema_text += "\nRelationships:\n"
    for table_name, info in schema_info.items():
        for fk in info['foreign_keys']:
            schema_text += f"- {table_name}.{fk['COLUMN_NAME']} -> {fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}\n"
    
    return schema_text

def get_gemini_response(question: str, schema_info: Dict[str, Any]) -> str:
    """Get SQL query from Gemini API"""
    model = genai.GenerativeModel('gemini-pro')
    
    schema_text = format_schema_for_prompt(schema_info)
    prompt = f"""You are an expert in converting English questions to MySQL queries.
    {schema_text}
    
    Important rules:
    1. Generate only the SQL query without any explanations or markdown formatting
    2. Use appropriate JOIN operations when query involves multiple tables
    3. If the question only requires data from one table, don't include unnecessary JOINs
    4. Ensure proper table name qualification when using multiple tables
    5. Use appropriate aggregation functions (COUNT, SUM, AVG, etc.) when needed
    6. Include proper GROUP BY and HAVING clauses when necessary
    7. Handle NULL values appropriately
    8. Use appropriate WHERE conditions for filtering
    9. Use MySQL-specific syntax (such as LIMIT instead of TOP)
    
    Convert the following question to SQL: {question}
    """
    
    try:
        response = model.generate_content(prompt)
        # Check if response has parts
        if hasattr(response, 'parts'):
            # Get the text from the first part
            sql_query = response.parts[0].text.strip()
        elif hasattr(response, 'text'):
            # If response has direct text attribute
            sql_query = response.text.strip()
        else:
            raise Exception("Unexpected response format from Gemini")
            
        # Remove any markdown SQL formatting if present
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
        
        if not sql_query:
            raise Exception("No SQL query generated from Gemini")
            
        return sql_query
    except Exception as e:
        raise Exception(f"Error generating SQL query: {str(e)}")

def execute_sql_query(connection: mysql.connector.connection.MySQLConnection, query: str) -> pd.DataFrame:
    """Execute SQL query and return results as a pandas DataFrame"""
    try:
        return pd.read_sql_query(query, connection)
    except Exception as e:
        raise Exception(f"Error executing query: {str(e)}")

def main():
    st.set_page_config(page_title="MySQL Query Generator", layout="wide")
    st.title("MySQL Query Generator")
    
    # Database configuration
    with st.sidebar:
        st.header("Database Connection")
        host = st.text_input("Host", "localhost")
        user = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database Name")
        
        if st.button("Connect to Database"):
            try:
                st.session_state.db_connection = create_db_connection(host, user, password, database)
                st.session_state.schema_info = get_table_schema(st.session_state.db_connection)
                st.success("Successfully connected to database!")
            except Exception as e:
                st.error(f"Connection failed: {str(e)}")
                return
    
    # Main application
    if 'db_connection' in st.session_state and 'schema_info' in st.session_state:
        # Display schema information
        with st.expander("Database Schema", expanded=False):
            st.code(format_schema_for_prompt(st.session_state.schema_info))
        
        # Query input
        question = st.text_area(
            "Enter your question:",
            placeholder="e.g., Show all students with marks above 80 in Data Science class"
        )
        
        if st.button("Generate and Execute Query"):
            if question:
                try:
                    # Generate SQL query
                    with st.spinner("Generating SQL query..."):
                        sql_query = get_gemini_response(question, st.session_state.schema_info)
                        
                        # Display the generated query
                        st.subheader("Generated SQL Query")
                        st.code(sql_query, language="sql")
                    
                    # Execute query and display results
                    with st.spinner("Executing query..."):
                        results_df = execute_sql_query(st.session_state.db_connection, sql_query)
                        
                        st.subheader("Query Results")
                        if not results_df.empty:
                            # Display results
                            st.dataframe(
                                results_df,
                                use_container_width=True,
                                hide_index=True
                            )
                            
                            # Show result statistics
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Number of Rows", len(results_df))
                            with col2:
                                st.metric("Number of Columns", len(results_df.columns))
                            
                            # Export options
                            st.download_button(
                                label="Download Results as CSV",
                                data=results_df.to_csv(index=False).encode('utf-8'),
                                file_name="query_results.csv",
                                mime="text/csv"
                            )
                        else:
                            st.info("Query executed successfully but returned no results.")
                            
                except Exception as e:
                    st.error(str(e))
            else:
                st.warning("Please enter a question first.")
    else:
        st.info("Please connect to your database using the sidebar.")

if __name__ == "__main__":
    main()