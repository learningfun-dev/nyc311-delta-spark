'''
    Bronze Layer Streaming Processing
'''
import os
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, TimestampType
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
    The main entry point for the streaming application.
    '''
    # Initialize SparkSession
    spark = get_spark_session(constants.BRONZE_APP_NAME)
    logger = get_logger(spark, "Bronze Layer Streaming")

    logger.info(f"Spark Master in use: {spark.sparkContext.master}")
    logger.info(BRONZE_BANNER)
    logger.info("--- Starting Bronze Layer Streaming Processing ---")

    try:
        input_path = constants.BRONZE_INPUT_FILE_PATH
        output_path = constants.BRONZE_OUTPUT_FILE_PATH
        checkpoint_path = os.path.join(os.path.dirname(output_path), "checkpoints/bronze_streaming")

        logger.info(f"Monitoring input directory: {input_path}")
        logger.info(f"Output will be written to Delta table at: {output_path}")
        logger.info(f"Checkpoint directory: {checkpoint_path}")

        # Define the schema for the incoming CSV files
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

        # 1. Read data from the CSV source directory as a stream
        # Spark will automatically discover new files added to the directory.
        bronze_stream_df = (
            spark.readStream
            .format("csv")
            .option("header", True)
            .schema(input_csv_file_schema)
            .load(input_path)
        )

        # 2. Write the stream to a Delta Lake table
        # The 'append' mode adds new records to the table.
        # Checkpointing is essential for fault-tolerant, exactly-once processing.
        query = (
            bronze_stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True) # Process all available files and then stop, mimicking batch behavior.
            .start(output_path)
        )

        # 3. Wait for the streaming query to terminate
        query.awaitTermination()
        logger.info("Successfully processed available CSV files into the bronze Delta table.")

    except Exception as e:
        logger.error(f"An error occurred during Bronze Layer streaming processing: {e}", exc_info=True)
        raise
    finally:
        # Stop the SparkSession
        logger.info("--- Bronze Layer Streaming Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
