'''
    Silver layer processing
'''
import os
from pyspark.sql.functions import col, to_timestamp, month, year
from constant import constants
from utils.spark_utils import get_spark_session
from utils.logging_utils import get_logger

SILVER_BANNER = """
   ▄████████  ▄█   ▄█        ▄█    █▄     ▄████████    ▄████████       ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███ ███  ███       ███    ███   ███    ███   ███    ███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    █▀  ███▌ ███       ███    ███   ███    █▀    ███    ███      ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
  ███        ███▌ ███       ███    ███  ▄███▄▄▄      ▄███▄▄▄▄██▀      ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀███████████ ███▌ ███       ███    ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀        ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
         ███ ███  ███       ███    ███   ███    █▄  ▀███████████      ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
   ▄█    ███ ███  ███▌    ▄ ███    ███   ███    ███   ███    ███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
 ▄████████▀  █▀   █████▄▄██  ▀██████▀    ██████████   ███    ███      █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
                  ▀                                   ███    ███      ▀                                               ███    ███ 
"""

def main():
    '''
        the main entry point for the application
    '''
    # Initialize SparkSession
    spark = get_spark_session(constants.SILVER_APP_NAME)
    logger = get_logger(spark, "Silver Layer")

    logger.info(f"Spark Master in use: {spark.sparkContext.master}")
    logger.info(SILVER_BANNER)
    logger.info("--- Starting Silver Layer Processing ---")

    try:
        if os.path.exists(constants.SILVER_INPUT_FILE_PATH):
            logger.info(f"Input file found at {constants.SILVER_INPUT_FILE_PATH}")

            logger.info(f"Reading bronze delta table from {constants.SILVER_INPUT_FILE_PATH}")
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

            logger.info(f"Writing silver delta table to {constants.SILVER_OUTPUT_FILE_PATH}")
            # Save to a delta table with partitions
            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("year","month")
                .save(constants.SILVER_OUTPUT_FILE_PATH)
            )
            logger.info("Successfully cleaned data and created Silver layer.")

        else:
            logger.error(f"Input file does not exist at {constants.SILVER_INPUT_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred during Silver Layer processing: {e}", exc_info=True)
        raise
    finally:
        # Stop the SparkSession
        logger.info("--- Silver Layer Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
