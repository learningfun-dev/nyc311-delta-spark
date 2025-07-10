import os
from pyspark.sql.functions import col, to_timestamp, month, year
from delta.tables import DeltaTable
from utils.spark_utils import get_spark_session
from utils.logging_utils import get_logger
from constant import constants

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
    The main entry point for the streaming application.
    '''
    # Initialize SparkSession
    spark = get_spark_session(constants.SILVER_APP_NAME)
    logger = get_logger(spark, "Silver Layer Streaming")

    logger.info(f"Spark Master in use: {spark.sparkContext.master}")
    logger.info(SILVER_BANNER)
    logger.info("--- Starting Silver Layer Streaming Processing ---")

    try:
        bronze_path = constants.SILVER_INPUT_FILE_PATH
        silver_path = constants.SILVER_OUTPUT_FILE_PATH
        # Checkpointing is crucial for streaming to track progress and ensure fault-tolerance
        checkpoint_path = os.path.join(os.path.dirname(silver_path), "checkpoints/silver_streaming")

        if not DeltaTable.isDeltaTable(spark, bronze_path):
            logger.error(f"Bronze table not found at {bronze_path}. Please run the bronze layer first.")
            return

        # 1. Read data from the bronze Delta table as a stream
        bronze_stream_df = (
            spark.readStream
            .format("delta")
            .option("ignoreDeletes", "true") # Process appends and updates
            .load(bronze_path)
        )

        # Function to perform transformations and upsert for each micro-batch
        def upsert_to_silver(micro_batch_df, batch_id):
            logger.info(f"--- Processing Micro-Batch ID: {batch_id} ---")

            if micro_batch_df.rdd.isEmpty():
                logger.info("Micro-batch is empty, skipping.")
                return

            # 2. Apply transformations within the micro-batch
            # dropDuplicates is applied here to handle duplicates within the current micro-batch.
            # The MERGE operation will handle duplicates against the historical data in the target table.
            silver_df = (
                micro_batch_df
                .filter(col("complaint_type").isNotNull())
                .withColumn("created_date", to_timestamp('created_date', 'yyyy-MM-dd'))
                .withColumn("closed_date", to_timestamp('closed_date', 'yyyy-MM-dd'))
                .withColumn("month", month('created_date'))
                .withColumn("year", year('created_date'))
                .dropDuplicates(["unique_key"])
            )

            # 3. Use MERGE to perform an idempotent upsert into the silver Delta table
            if DeltaTable.isDeltaTable(spark, silver_path):
                logger.info(f"Merging new data into silver delta table at {silver_path}")
                silver_table = DeltaTable.forPath(spark, silver_path)
                (silver_table.alias("target")
                 .merge(silver_df.alias("source"), "target.unique_key = source.unique_key")
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                # This block will run only once, when the silver table doesn't exist yet
                logger.info(f"Creating new silver delta table at {silver_path}")
                (silver_df.write
                 .format("delta")
                 .partitionBy("year", "month")
                 .save(silver_path))

            logger.info(f"--- Finished processing Micro-Batch ID: {batch_id} ---")

        # 4. Define and start the streaming query
        logger.info(f"Starting stream write to {silver_path}")
        query = (
            bronze_stream_df.writeStream
            .foreachBatch(upsert_to_silver)
            .outputMode("update")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True) # Use availableNow for a batch-like execution or processingTime for continuous
            .start()
        )

        # 5. Wait for the streaming query to terminate
        query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred during Silver Layer streaming processing: {e}", exc_info=True)
        raise
    finally:
        # Stop the SparkSession
        logger.info("--- Silver Layer Streaming Processing Finished ---")
        spark.stop()


if __name__ == "__main__":
    main()
