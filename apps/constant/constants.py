"""
Project Level Constants
"""

import os

APP_NAME = "spark-sql"



SPARK_MASTER = "spark://localhost:7077"

SHARED_DIRECTORY = "data"
OUTPUT_FILE_NAME = "311_service_requests/"

# input file
INPUT_FILE_NAME = "erm2-nwe9_version_176301.csv" # use for end to end final output

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

# streamlit_app constants
STREAMLIT_APP_NAME = APP_NAME + ":Streamlit"
