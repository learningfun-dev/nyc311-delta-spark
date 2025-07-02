'''
    Silver layer processing
'''
import os
from pyspark.sql.functions import col, to_timestamp, month, year
from constant import constants
from pystyle import Colors, Colorate
from utils.spark_utils import get_spark_session


def main():
    '''
        the main entry point for the application
    '''

    print(Colorate.Vertical(Colors.blue_to_green, """


   ▄████████  ▄█   ▄█        ▄█    █▄     ▄████████    ▄████████       ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███ ███  ███       ███    ███   ███    ███   ███    ███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    █▀  ███▌ ███       ███    ███   ███    █▀    ███    ███      ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
  ███        ███▌ ███       ███    ███  ▄███▄▄▄      ▄███▄▄▄▄██▀      ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀███████████ ███▌ ███       ███    ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀        ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
         ███ ███  ███       ███    ███   ███    █▄  ▀███████████      ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
   ▄█    ███ ███  ███▌    ▄ ███    ███   ███    ███   ███    ███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
 ▄████████▀  █▀   █████▄▄██  ▀██████▀    ██████████   ███    ███      █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
                  ▀                                   ███    ███      ▀                                               ███    ███ 

    """, 1))
    # Initialize SparkSession
    spark = get_spark_session(constants.SILVER_APP_NAME)

    print("{:*<120}".format("*"))
    print("{: ^120}".format("2. SILVER LAYER PROCESSING: Data Cleaning and adding columns"))
    print("{:*<120}".format("*"))

    try:
        if os.path.exists(constants.SILVER_INPUT_FILE_PATH):
            print("Silver layer: input file exists")

            print("Silver layer: reading input delta table")
            #  Read Delta Table
            silver_df = ( spark.read
                .format("delta")
                .load(constants.SILVER_INPUT_FILE_PATH)
                .filter(col("complaint_type").isNotNull())
                .withColumn("created_date"
                            ,to_timestamp('created_date', 'yyyy-MM-dd')) #Coaerce to datettime
                .withColumn("closed_date"
                            ,to_timestamp('closed_date', 'yyyy-MM-dd')) #Coaerce to datettime
                .withColumn("month",month('created_date'))#extract month
                .withColumn("year",year('created_date'))#extract year
                .dropDuplicates(["unique_key"])
            )

            print("Silver layer: writing output delta table")
            # Save to a delta table with partitions
            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("year","month")
                .save(constants.SILVER_OUTPUT_FILE_PATH)
            )

        else:
            print("Silver layer input file does not exists")
    finally:
        # Stop the SparkSession
        print("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    main()
