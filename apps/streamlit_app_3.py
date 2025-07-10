"""
Streamlit dashboard for AI-Powered NYC 311 Analysis using Text-to-SQL.

This application allows users to ask natural language questions about the
NYC 311 dataset. An LLM converts the question into a Spark SQL query,
which is then executed against the Gold Delta tables. The results are
displayed as a table and a chart.
"""
import os
import streamlit as st
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import OllamaLLM

from constant import constants
from utils.spark_utils import get_spark_session

# --- Constants and Configuration ---

# Define the schemas of the Gold tables for the LLM's context.
# This helps the LLM generate accurate queries.
GOLD_TABLE_SCHEMAS = """
1. top_complaints (
    complaint_type STRING,
    year INT,
    month INT,
    count BIGINT
)
Description: This table contains the total count of 311 complaints aggregated by complaint type, year, and month.

2. by_borough (
    borough STRING,
    year INT,
    month INT,
    count BIGINT
)
Description: This table contains the total count of 311 complaints aggregated by borough, year, and month. The borough can be 'BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', or NULL for unspecified.
"""

# Create a prompt template for the Text-to-SQL conversion.
# This guides the LLM to generate valid Spark SQL.
TEXT_TO_SQL_TEMPLATE = """
You are an expert Spark SQL data analyst. Your task is to convert a user's question into a valid Spark SQL query.
You can only query the tables provided in the schema information below.
Do not use any tables that are not listed. The available tables are `top_complaints` and `by_borough`.
Always wrap table and column names in backticks (`) if they contain spaces or are keywords.
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
    The `st.cache_resource` decorator ensures the Spark session persists across user interactions.

    Returns:
        SparkSession: The active Spark session.
    """
    return get_spark_session("streamlit_app_3_text_to_sql")

def main() -> None:
    """The main entry point for the Text-to-SQL Streamlit application."""
    st.set_page_config(page_title="NYC 311 AI Analyst (Text-to-SQL)", layout="wide")
    st.title("🗽 AI-Powered NYC 311 Analyst (Text-to-SQL)")
    st.write(
        "Ask a question about NYC 311 service requests. "
        "The AI will generate a Spark SQL query, execute it against the gold data, and visualize the result."
    )

    # Check if the required Gold Delta tables exist before proceeding.
    top_complaints_path = constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS
    by_borough_path = constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH

    if not os.path.exists(top_complaints_path) or not os.path.exists(by_borough_path):
        st.warning(f"Gold data not found:  {top_complaints_path} . Please run the ETL pipeline (bronze, silver, gold layers) first.")
        st.stop()

    # Get the cached Spark session and initialize the LLM.
    spark = get_spark_session("streamlit_app_3_text_to_sql")
    llm = OllamaLLM(model=constants.LOCAL_LLM_MODEL, base_url=constants.OLLAMA_BASE_URL)

    # Create the LangChain chain for Text-to-SQL generation.
    prompt_template = ChatPromptTemplate.from_template(TEXT_TO_SQL_TEMPLATE)
    sql_generation_chain = prompt_template | llm | StrOutputParser()

    # Handle user input from the chat interface.
    if user_question := st.chat_input("e.g., What were the top 5 complaint types in 2023?"):
        st.chat_message("user").markdown(user_question)

        with st.chat_message("assistant"):
            with st.spinner("Generating SQL query..."):
                generated_sql = sql_generation_chain.invoke({"schema": GOLD_TABLE_SCHEMAS, "question": user_question})
                st.write("Generated SQL Query:")
                st.code(generated_sql, language="sql")

            with st.spinner("Executing query and generating chart..."):
                try:
                    spark.read.format("delta").load(top_complaints_path).createOrReplaceTempView("top_complaints")
                    spark.read.format("delta").load(by_borough_path).createOrReplaceTempView("by_borough")

                    result_df = spark.sql(generated_sql)
                    pandas_df = result_df.toPandas()

                    st.write("### Query Results")
                    if not pandas_df.empty:
                        st.dataframe(pandas_df)
                        st.bar_chart(pandas_df.set_index(pandas_df.columns[0]))
                    else:
                        st.info("The query returned no data.")
                except Exception as e:
                    st.error(f"An error occurred while executing the query:\n\n{e}")

if __name__ == "__main__":
    main()
