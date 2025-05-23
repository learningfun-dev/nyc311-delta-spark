'''
    Gold Layer processing
'''
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
    .appName(constants.GOLD_APP_NAME)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(spark).getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    print("""
        # ********************************************************************************
        # 3. GOLD LAYER PROCESSING: Data Aggregation
        # ********************************************************************************
        """)

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

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
