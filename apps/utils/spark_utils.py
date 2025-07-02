"""
Spark Utility Functions
"""
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from constant import constants


def get_spark_session(app_name: str) -> SparkSession:
    """
    Initializes and returns a SparkSession with a given app name and Delta Lake support.

    """
    spark = (
        SparkSession.builder.master(constants.SPARK_MASTER)
        .appName(app_name)
        .config("spark.driver.memory", constants.SPARK_DRIVER_MEMORY)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(spark).getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    return spark