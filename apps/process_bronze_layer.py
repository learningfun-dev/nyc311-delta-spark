'''
    Process bronze layer
'''
import os
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, TimestampType
from constant import constants
from pystyle import Colors, Colorate
from utils.spark_utils import get_spark_session


def main():
    '''
        the main entry point for the application
    '''

    print(Colorate.Vertical(Colors.blue_to_green, """


▀█████████▄     ▄████████  ▄██████▄  ███▄▄▄▄    ▄███████▄     ▄████████       ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███   ███    ███ ███    ███ ███▀▀▀██▄ ██▀     ▄██   ███    ███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    ███   ███    ███ ███    ███ ███   ███       ▄███▀   ███    █▀       ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
 ▄███▄▄▄██▀   ▄███▄▄▄▄██▀ ███    ███ ███   ███  ▀█▀▄███▀▄▄  ▄███▄▄▄          ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀▀███▀▀▀██▄  ▀▀███▀▀▀▀▀   ███    ███ ███   ███   ▄███▀   ▀ ▀▀███▀▀▀          ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
  ███    ██▄ ▀███████████ ███    ███ ███   ███ ▄███▀         ███    █▄       ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
  ███    ███   ███    ███ ███    ███ ███   ███ ███▄     ▄█   ███    ███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
▄█████████▀    ███    ███  ▀██████▀   ▀█   █▀   ▀████████▀   ██████████      █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
               ███    ███                                                    ▀                                               ███    ███ 


    """, 1))


    # Initialize SparkSession
    spark = get_spark_session(constants.BRONZE_APP_NAME)

    print("{:*<120}".format("*"))
    print("{: ^120}".format("1. BRONZE LAYER PROCESSING: Convert CSV to Delta Format"))
    print("{:*<120}".format("*"))

    try:
        if os.path.exists(constants.BRONZE_INPUT_FILE_PATH):
            print("Bronze layer: input file exists")

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

            print("Bronze layer: reading CSV file")
            # Read CSV File
            bronze_df = (
                spark.read
                .format("csv")
                .option("header", True)
                .option("inferSchema", False)
                .schema(input_csv_file_schema)
                .load(constants.BRONZE_INPUT_FILE_PATH)
                )

            print("Bronze layer: writing delta table")
            # transform into Delta Lake
            (
                bronze_df.write
                .format("delta")
                .mode("overwrite")
                .save(constants.BRONZE_OUTPUT_FILE_PATH)
            )

        else:
            print("Bronze layer input file does not exists")
    finally:
        # Stop the SparkSession
        print("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
