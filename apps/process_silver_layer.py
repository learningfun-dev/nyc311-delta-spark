'''
    Silver layer processing
'''
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, year
from delta.pip_utils import configure_spark_with_delta_pip
from constant import constants


def main():
    '''
        the main entry point for the application
    '''

    # Initialize SparkSession
    spark = (
    SparkSession
    .builder.master(constants.SPARK_MASTER)
    .appName(constants.SILVER_APP_NAME)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(spark).getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    
    print("""
        # ********************************************************************************
        # 2. SILVER LAYER PROCESSING: Data Cleaning and adding columns
        # ********************************************************************************
        """)

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

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
