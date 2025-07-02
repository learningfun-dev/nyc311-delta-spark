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

# INPUT_FILE_NAME = "small_erm2-nwe9_version_176301.csv" # dev file path


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
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "5000"))


# streamlit_app constants
STREAMLIT_APP_NAME = APP_NAME + ":Streamlit"
