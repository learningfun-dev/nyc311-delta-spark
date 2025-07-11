'''
    Gold Layer Streaming Processing
'''
import os
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from utils.spark_utils import get_spark_session
from utils.logging_utils import get_logger
from constant import constants

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
    The main entry point for the streaming application.
    '''
    # Initialize SparkSession
    spark = get_spark_session(constants.GOLD_APP_NAME)
    logger = get_logger(spark, "Gold Layer Streaming")

    logger.info(f"Spark Master in use: {spark.sparkContext.master}")
    logger.info(GOLD_BANNER)
    logger.info("--- Starting Gold Layer Streaming Processing ---")

    try:
        silver_path = constants.GOLD_INPUT_FILE_PATH
        top_complaints_path = constants.GOLD_OUTPUT_FILE_PATH_TOP_COMPLAINTS
        by_borough_path = constants.GOLD_OUTPUT_FILE_PATH_BY_BOROUGH
        # A single checkpoint for the input stream
        checkpoint_path = os.path.join(os.path.dirname(top_complaints_path), "checkpoints/gold_streaming")

        if not DeltaTable.isDeltaTable(spark, silver_path):
            logger.error(f"Silver table not found at {silver_path}. Please run the silver layer first.")
            return

        # 1. Read data from the silver Delta table as a stream
        silver_stream_df = (
            spark.readStream
            .format("delta")
            .option("ignoreDeletes", "true")
            .load(silver_path)
        )

        # Function to perform aggregations and upserts for each micro-batch
        def process_aggregates(micro_batch_df, batch_id):
            logger.info(f"--- Processing Micro-Batch ID: {batch_id} ---")

            if micro_batch_df.rdd.isEmpty():
                logger.info("Micro-batch is empty, skipping.")
                return

            # Cache the micro-batch to avoid re-reading from source for each aggregation
            micro_batch_df.cache()
            
            # --- Process top_complaints ---
            logger.info("Aggregating new data for top complaints.")
            new_top_complaints = micro_batch_df.groupBy("complaint_type", "year", "month").count()

            # Merge into the 'top_complaints' gold table
            if DeltaTable.isDeltaTable(spark, top_complaints_path):
                gold_top_complaints_table = DeltaTable.forPath(spark, top_complaints_path)
                (gold_top_complaints_table.alias("target")
                 .merge(new_top_complaints.alias("source"), 
                        "target.year = source.year AND target.month = source.month AND target.complaint_type = source.complaint_type")
                 .whenMatchedUpdate(set={"count": col("target.count") + col("source.count")})
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                (new_top_complaints.write.format("delta").mode("overwrite").save(top_complaints_path))
            
            logger.info(f"Successfully merged into {top_complaints_path}")

            # --- Process by_borough ---
            logger.info("Aggregating new data by borough.")
            new_by_borough = micro_batch_df.groupBy("borough", "year", "month").count()

            # Merge into the 'by_borough' gold table
            if DeltaTable.isDeltaTable(spark, by_borough_path):
                gold_by_borough_table = DeltaTable.forPath(spark, by_borough_path)
                (gold_by_borough_table.alias("target")
                 .merge(new_by_borough.alias("source"), 
                        "target.year = source.year AND target.month = source.month AND target.borough <=> source.borough")
                 .whenMatchedUpdate(set={"count": col("target.count") + col("source.count")})
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                (new_by_borough.write.format("delta").mode("overwrite").save(by_borough_path))

            logger.info(f"Successfully merged into {by_borough_path}")

            # Unpersist the cached DataFrame
            micro_batch_df.unpersist()
            logger.info(f"--- Finished processing Micro-Batch ID: {batch_id} ---")


        # 2. Define and start the streaming query
        logger.info("Starting stream to process gold layer aggregates.")
        query = (
            silver_stream_df.writeStream
            .foreachBatch(process_aggregates)
            .outputMode("update")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True) # Processes all available data in a single batch and then stops.
            .start()
        )

        # 3. Wait for the streaming query to terminate
        query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred during Gold Layer streaming processing: {e}", exc_info=True)
        raise
    finally:
        # Stop the SparkSession
        logger.info("--- Gold Layer Streaming Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
