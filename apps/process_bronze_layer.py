'''
    Process bronze layer
'''
import os
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, TimestampType
import glob
from constant import constants
from utils.spark_utils import get_spark_session
from utils.logging_utils import get_logger

BRONZE_BANNER = """
▀█████████▄     ▄████████  ▄██████▄  ███▄▄▄▄    ▄███████▄     ▄████████       ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███   ███    ███ ███    ███ ███▀▀▀██▄ ██▀     ▄██   ███    ███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    ███   ███    ███ ███    ███ ███   ███       ▄███▀   ███    █▀       ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
 ▄███▄▄▄██▀   ▄███▄▄▄▄██▀ ███    ███ ███   ███  ▀█▀▄███▀▄▄  ▄███▄▄▄          ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀▀███▀▀▀██▄  ▀▀███▀▀▀▀▀   ███    ███ ███   ███   ▄███▀   ▀ ▀▀███▀▀▀          ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
  ███    ██▄ ▀███████████ ███    ███ ███   ███ ▄███▀         ███    █▄       ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
  ███    ███   ███    ███ ███    ███ ███   ███ ███▄     ▄█   ███    ███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
▄█████████▀    ███    ███  ▀██████▀   ▀█   █▀   ▀████████▀   ██████████      █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
               ███    ███                                                    ▀                                               ███    ███ 
"""

def main():
    '''
        the main entry point for the application
    '''
    # Initialize SparkSession
    spark = get_spark_session(constants.BRONZE_APP_NAME)
    logger = get_logger(spark, "Bronze Layer")

    logger.info(f"Spark Master in use: {spark.sparkContext.master}")
    logger.info(BRONZE_BANNER)
    logger.info("--- Starting Bronze Layer Processing ---")

    try:
        # Assuming BRONZE_INPUT_FILE_PATH points to the 'data/raw' directory
        input_path = constants.BRONZE_INPUT_FILE_PATH
        # Check if the directory exists and contains any CSV files
        csv_files = glob.glob(os.path.join(input_path, "*.csv"))
        if os.path.isdir(input_path) and csv_files:
            logger.info(f"Input directory found at {input_path} with {len(csv_files)} CSV files.")

            input_csv_file_schema = StructType([
                StructField("unique_key", StringType(), True),
                StructField("created_date", TimestampType(), True),
                StructField("closed_date", TimestampType(), True),
                StructField("agency", StringType(), True),
                StructField("agency_name", StringType(), True),
                StructField("complaint_type", StringType(), True),
                StructField("descriptor", StringType(), True),
                StructField("location_type", StringType(), True),
                StructField("incident_zip", StringType(), True),
                StructField("incident_address", StringType(), True),
                StructField("street_name", StringType(), True),
                StructField("cross_street_1", StringType(), True),
                StructField("cross_street_2", StringType(), True),
                StructField("intersection_street_1", StringType(), True),
                StructField("intersection_street_2", StringType(), True),
                StructField("address_type", StringType(), True),
                StructField("city", StringType(), True),
                StructField("landmark", StringType(), True),
                StructField("facility_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("due_date", TimestampType(), True),
                StructField("resolution_description", StringType(), True),
                StructField("resolution_action_updated_date", TimestampType(), True),
                StructField("community_board", StringType(), True),
                StructField("bbl", StringType(), True),
                StructField("borough", StringType(), True),
                StructField("x_coordinate_state_plane", StringType(), True),
                StructField("y_coordinate_state_plane", StringType(), True),
                StructField("open_data_channel_type", StringType(), True),
                StructField("park_facility_name", StringType(), True),
                StructField("park_borough", StringType(), True),
                StructField("vehicle_type", StringType(), True),
                StructField("taxi_company_borough", StringType(), True),
                StructField("taxi_pick_up_location", StringType(), True),
                StructField("bridge_highway_name", StringType(), True),
                StructField("bridge_highway_direction", StringType(), True),
                StructField("road_ramp", StringType(), True),
                StructField("bridge_highway_segment", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("location_city", StringType(), True),
                StructField("location", StringType(), True),
                StructField("location_address", StringType(), True),
                StructField("location_zip", StringType(), True),
                StructField("location_state", StringType(), True),
            ])

            logger.info(f"Reading all CSV files from {input_path}")
            # Read all CSV files from the directory
            bronze_df = (
                spark.read
                .format("csv")
                .option("header", True)
                .option("inferSchema", False)
                .schema(input_csv_file_schema)
                .load(input_path)
                )

            logger.info(f"Writing bronze delta table to {constants.BRONZE_OUTPUT_FILE_PATH}")
            # transform into Delta Lake
            (
                bronze_df.write
                .format("delta")
                .mode("overwrite")
                .save(constants.BRONZE_OUTPUT_FILE_PATH)
            )
            logger.info("Successfully converted CSV to Delta format.")

        else:
            logger.error(f"Input directory does not exist or contains no CSV files at {input_path}")
    except Exception as e:
        logger.error(f"An error occurred during Bronze Layer processing: {e}", exc_info=True)
        raise
    finally:
        # Stop the SparkSession
        logger.info("--- Bronze Layer Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
