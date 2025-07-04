'''
    Gold Layer processing
'''
import os
from pyspark.sql.functions import col
from constant import constants
from utils.spark_utils import get_spark_session
from utils.logging_utils import get_logger

GOLD_BANNER = """
   ▄██████▄   ▄██████▄   ▄█       ████████▄        ▄█          ▄████████ ▄██   ▄      ▄████████    ▄████████ 
  ███    ███ ███    ███ ███       ███   ▀███      ███         ███    ███ ███   ██▄   ███    ███   ███    ███ 
  ███    █▀  ███    ███ ███       ███    ███      ███         ███    ███ ███▄▄▄███   ███    █▀    ███    ███ 
 ▄███        ███    ███ ███       ███    ███      ███         ███    ███ ▀▀▀▀▀▀███  ▄███▄▄▄      ▄███▄▄▄▄██▀ 
▀▀███ ████▄  ███    ███ ███       ███    ███      ███       ▀███████████ ▄██   ███ ▀▀███▀▀▀     ▀▀███▀▀▀▀▀   
  ███    ███ ███    ███ ███       ███    ███      ███         ███    ███ ███   ███   ███    █▄  ▀███████████ 
  ███    ███ ███    ███ ███▌    ▄ ███   ▄███      ███▌    ▄   ███    ███ ███   ███   ███    ███   ███    ███ 
  ████████▀   ▀██████▀  █████▄▄██ ████████▀       █████▄▄██   ███    █▀   ▀█████▀    ██████████   ███    ███ 
                        ▀                         ▀                                               ███    ███ 
"""

def main():
    '''
        the main entry point for the application
    '''
    # Initialize SparkSession
    spark = get_spark_session(constants.GOLD_APP_NAME)
    logger = get_logger(spark, "Gold Layer")

    logger.info(f"Spark Master in use: {spark.sparkContext.master}")
    logger.info(GOLD_BANNER)
    logger.info("--- Starting Gold Layer Processing ---")

    try:
        if os.path.exists(constants.GOLD_INPUT_FILE_PATH):
            logger.info(f"Input file found at {constants.GOLD_INPUT_FILE_PATH}")
            logger.info(f"Reading silver delta table from {constants.GOLD_INPUT_FILE_PATH}")
            gold_df = ( spark.read
                .format("delta")
                .load(constants.GOLD_INPUT_FILE_PATH)
            )

            logger.info("Aggregating data for top complaints.")
            top_complaints = gold_df.groupBy("complaint_type","year","month").count().orderBy(col("count").desc())

            logger.info(f"Writing top_complaints delta table to {constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS}")
            (
                top_complaints.write
                .format("delta")
                .mode("overwrite")
                .save(constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS)
            )

            logger.info("Aggregating data by borough.")
            by_borough = gold_df.groupBy("borough","year","month").count()

            logger.info(f"Writing by_borough delta table to {constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH}")
            (
                by_borough.write
                .format("delta")
                .mode("overwrite")
                .save(constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH)
            )
            logger.info("Successfully created Gold tables.")

        else:
            logger.error(f"Input file does not exist at {constants.GOLD_INPUT_FILE_PATH}")
    except Exception as e:
        logger.error(f"An error occurred during Gold Layer processing: {e}", exc_info=True)
        raise
    finally:
        # Stop the SparkSession
        logger.info("--- Gold Layer Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
