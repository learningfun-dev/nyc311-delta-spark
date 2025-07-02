'''
    Gold Layer processing
'''
import os
from pyspark.sql.functions import col
from constant import constants
from pystyle import Colors, Colorate
from utils.spark_utils import get_spark_session


def main():
    '''
        the main entry point for the application
    '''

    print(Colorate.Vertical(Colors.blue_to_green, """


   ▄██████▄   ▄██████▄   ▄█       ████████▄        ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███ ███    ███ ███       ███   ▀███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    █▀  ███    ███ ███       ███    ███      ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
 ▄███        ███    ███ ███       ███    ███      ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀▀███ ████▄  ███    ███ ███       ███    ███      ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
  ███    ███ ███    ███ ███       ███    ███      ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
  ███    ███ ███    ███ ███▌    ▄ ███   ▄███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
  ████████▀   ▀██████▀  █████▄▄██ ████████▀       █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
                        ▀                         ▀                                               ███    ███ 


    """, 1))

    # Initialize SparkSession
    spark = get_spark_session(constants.GOLD_APP_NAME)

    print("{:*<120}".format("*"))
    print("{: ^120}".format("3. GOLD LAYER PROCESSING: Data Aggregation"))
    print("{:*<120}".format("*"))

    try:
        if os.path.exists(constants.GOLD_INPUT_FILE_PATH):
            print("Gold layer: input file exists")
            print("Gold layer: reading input delta table")
            gold_df = ( spark.read
                .format("delta")
                .load(constants.GOLD_INPUT_FILE_PATH)
            )

            # top_complaints
            top_complaints = gold_df.groupBy("complaint_type","year","month").count().orderBy(col("count").desc())

            print("Gold layer: writing output delta table top_complaints")
            (
                top_complaints.write
                .format("delta")
                .mode("overwrite")
                .save(constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS)
            )

            # by_borough
            by_borough = gold_df.groupBy("borough","year","month").count()

            print("Gold layer: writing output delta table by_borough")
            (
                by_borough.write
                .format("delta")
                .mode("overwrite")
                .save(constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH)
            )

        else:
            print("Gold layer input file does not exists")
    finally:
        # Stop the SparkSession
        print("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
