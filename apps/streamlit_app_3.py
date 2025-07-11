"""
Streamlit dashboard for AI-Powered NYC 311 Analysis using Text-to-SQL.

This application allows users to ask natural language questions about the
NYC 311 dataset. An LLM converts the question into a Spark SQL query.
The user can then verify or edit the query before executing it against
the Bronze, Silver, and Gold Delta tables. The results are displayed
as a table and a chart.
"""
import os
import streamlit as st
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaLLM

from constant import constants
from utils.spark_utils import get_spark_session

# --- Constants and Configuration ---

# Define the schemas of ALL tables for the LLM's context.
# This helps the LLM generate accurate queries across the entire pipeline.
ALL_TABLE_SCHEMAS = """
You have access to the following tables. Please use the table that is most appropriate for the user's question.

1. bronze_complaints (
    unique_key STRING,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    complaint_type STRING,
    descriptor STRING,
    incident_address STRING,
    borough STRING,
    status STRING,
    latitude DOUBLE,
    longitude DOUBLE
)
Description: This bronze layer table of the ETL pipeline contains the raw, unprocessed 311 complaint data. It has the most detail but may contain duplicates or nulls.

2. silver_complaints (
    unique_key STRING,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    complaint_type STRING,
    descriptor STRING,
    borough STRING,
    status STRING,
    month INT,
    year INT
)
Description: This silver layer table of the ETL pipeline contains the cleaned and de-duplicated data from the bronze layer. It is the main source for aggregations.

3. gold_top_complaints (
    complaint_type STRING,
    year INT,
    month INT,
    count BIGINT
)
Description: This gold layer table of the ETL pipeline contains the total count of 311 complaints aggregated by complaint type, year, and month. Use this for questions about complaint volumes by complaint type.

4. gold_by_borough (
    borough STRING,
    year INT,
    month INT,
    count BIGINT
)
Description: This gold layer table of the ETL pipeline contains the total count of 311 complaints aggregated by borough, year, and month. Use this for questions about complaint volumes by location.
"""

# Create a prompt template for the Text-to-SQL conversion.
# This guides the LLM to generate valid Spark SQL across all tables.
TEXT_TO_SQL_TEMPLATE = """
You are an expert Spark SQL data analyst. Your task is to convert a user's question into a valid Spark SQL query.
You can only query the tables provided in the schema information below.
Do not use any tables that are not listed. The available tables are `bronze_complaints`, `silver_complaints`, `top_complaints`, and `by_borough`.
The user's question might be conversational. Your generated query should be directly executable.
Only output the SQL query and nothing else. Do not add explanations, introductory text, or markdown formatting.

Schema Information:
{schema}

User Question:
{question}

Spark SQL Query:
"""

@st.cache_resource
def get_cached_spark_session():
    """
    Initializes and caches the Spark session to avoid re-creating it on every script rerun.
    """
    return get_spark_session("streamlit_app_3_text_to_sql")

def main() -> None:
    """The main entry point for the Text-to-SQL Streamlit application."""
    st.set_page_config(page_title="NYC 311 Pipeline Query Executor", layout="wide")
    st.title("🗽 AI-Powered NYC 311 Pipeline Query Executor")
    st.write(
        "Ask a question about the NYC 311 dataset. "
        "The AI will generate a Spark SQL query for you to verify, edit, and execute."
    )

    # --- Check for all required Delta tables ---
    bronze_path = constants.BRONZE_OUTPUT_FILE_PATH
    silver_path = constants.SILVER_OUTPUT_FILE_PATH
    top_complaints_path = constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS
    by_borough_path = constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH

    paths_exist = all(os.path.exists(p) for p in [bronze_path, silver_path, top_complaints_path, by_borough_path])

    if not paths_exist:
        st.warning("One or more data layers (Bronze, Silver, Gold) not found. Please run the full ETL pipeline first.")
        st.stop()

    # Get the cached Spark session and initialize the LLM.
    spark = get_cached_spark_session()
    llm = OllamaLLM(model=constants.LOCAL_LLM_MODEL, base_url=constants.OLLAMA_BASE_URL)

    # Create the LangChain chain for Text-to-SQL generation.
    prompt_template = ChatPromptTemplate.from_template(TEXT_TO_SQL_TEMPLATE)
    sql_generation_chain = prompt_template | llm | StrOutputParser()

    # Initialize session state to hold the query
    if "sql_query" not in st.session_state:
        st.session_state.sql_query = ""

    # Handle user input from the chat interface.
    if user_question := st.chat_input("e.g., Show me 10 records from the silver_complaints table for Jan 2023"):
        with st.chat_message("user"):
            st.markdown(user_question)
        
        with st.chat_message("assistant"):
            with st.spinner("Generating SQL query..."):
                raw_generated_sql = sql_generation_chain.invoke({"schema": ALL_TABLE_SCHEMAS, "question": user_question})
                
                # Clean the generated query to remove markdown formatting
                cleaned_sql = raw_generated_sql.strip()
                if cleaned_sql.startswith("```sql"):
                    cleaned_sql = cleaned_sql[len("```sql"):].strip()
                if cleaned_sql.endswith("```"):
                    cleaned_sql = cleaned_sql[:-len("```")].strip()
                
                # Store the cleaned query in the session state
                st.session_state.sql_query = cleaned_sql

    # If a query has been generated, display the editor and execute button
    if st.session_state.sql_query:
        st.write("#### Generated SQL Query (Editable)")
        # Display the query in a text area for editing
        edited_query = st.text_area(
            "Edit the query below, then click 'Execute'", 
            st.session_state.sql_query, 
            height=150,
            label_visibility="collapsed"
        )

        if st.button("Execute Query"):
            with st.spinner("Executing query and generating chart..."):
                try:
                    # --- Register all tables as temporary views ---
                    spark.read.format("delta").load(bronze_path).createOrReplaceTempView("bronze_complaints")
                    spark.read.format("delta").load(silver_path).createOrReplaceTempView("silver_complaints")
                    spark.read.format("delta").load(top_complaints_path).createOrReplaceTempView("gold_top_complaints")
                    spark.read.format("delta").load(by_borough_path).createOrReplaceTempView("gold_by_borough")

                    # Execute the potentially edited query
                    result_df = spark.sql(edited_query)
                    pandas_df = result_df.toPandas()

                    st.write("### Query Results")
                    if not pandas_df.empty:
                        st.dataframe(pandas_df)
                        # Attempt to create a bar chart if the data is suitable
                        if len(pandas_df.columns) > 1:
                            try:
                                st.bar_chart(pandas_df.set_index(pandas_df.columns[0]))
                            except Exception as chart_error:
                                st.info(f"Could not generate a chart for this query. Error: {chart_error}")
                    else:
                        st.info("The query returned no data.")
                except Exception as e:
                    st.error(f"An error occurred while executing the query:\n\n{e}")

if __name__ == "__main__":
    main()
