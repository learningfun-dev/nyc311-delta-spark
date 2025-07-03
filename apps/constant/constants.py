"""
Project Level Constants
"""

import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

APP_NAME = "spark-sql"

# Core Configuration - Override with environment variables
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")

SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "localhost")
SPARK_DRIVER_PORT = int(os.getenv("SPARK_DRIVER_PORT", "25333"))

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
HOME = os.getenv("HOME", "/opt/bitnami/spark") # Home directory inside the container
SHARED_DIRECTORY = os.getenv("SHARED_DIRECTORY", os.path.join(PROJECT_ROOT, "data"))
OUTPUT_FILE_NAME = "311_service_requests/"

# input file
INPUT_FILE_NAME = os.getenv("INPUT_FILE_NAME", "erm2-nwe9_version_176301.csv") # use for end to end final output

# Bronze  constants
BRONZE_APP_NAME = APP_NAME + ":Bronze"
BRONZE_INPUT_FILE_PATH = os.path.join(SHARED_DIRECTORY, "raw", INPUT_FILE_NAME)
BRONZE_OUTPUT_FILE_PATH = os.path.join(SHARED_DIRECTORY, "bronze",  OUTPUT_FILE_NAME)


# Silver constants
SILVER_APP_NAME = APP_NAME + ":Silver"
SILVER_INPUT_FILE_PATH = BRONZE_OUTPUT_FILE_PATH
SILVER_OUTPUT_FILE_PATH = os.path.join(SHARED_DIRECTORY, "silver",  OUTPUT_FILE_NAME)


# Gold constants
GOLD_INPUT_FILE_PATH = SILVER_OUTPUT_FILE_PATH
GOLD_APP_NAME = APP_NAME + ":Gold"
GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS = os.path.join(SHARED_DIRECTORY, "gold",  "top_complaints")
GOLD_OUTPUT_FILE_PATH_BY_BOROUGH = os.path.join(SHARED_DIRECTORY, "gold",  "by_borough")


# EMBEDDING constants
EMBEDDING_APP_NAME = APP_NAME + ":Embedding"
EMBEDDING_PATH_TOP_COMPLAINTS = GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS
EMBEDDING_PATH_BY_BOROUGH = GOLD_OUTPUT_FILE_PATH_BY_BOROUGH
EMBEDDING_COLLECTION_NAME = "nyc_311_gold"
EMBEDDING_CHROMA_HOST = os.getenv("EMBEDDING_CHROMA_HOST", "localhost")
EMBEDDING_CHROMA_PORT = int(os.getenv("EMBEDDING_CHROMA_PORT", "8000"))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "5000"))


# RAG API constants
API_TITLE = os.getenv("API_TITLE", "AI-Powered NYC 311 Analyst API (RAG 2.0)")
API_DESCRIPTION = os.getenv("API_DESCRIPTION", "API for querying NYC 311 data with query rephrasing and a local LLM.")
API_VERSION = os.getenv("API_VERSION", "2.0.0")
LOCAL_LLM_MODEL = os.getenv("LOCAL_LLM_MODEL", "llama3:instruct")
# Use host.docker.internal to allow the container to connect to the Ollama server running on the host machine.
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

# streamlit_app constants
STREAMLIT_APP_NAME = APP_NAME + ":Streamlit"
API_HOST = os.getenv("API_HOST", "localhost")
API_PORT = int(os.getenv("API_PORT", 8001))
API_URL = f"http://{API_HOST}:{API_PORT}/stream_chat"
