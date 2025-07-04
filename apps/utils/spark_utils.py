"""
Spark Utility Functions
"""
import os
from pyspark.sql import SparkSession
from constant import constants


def get_spark_session(app_name: str) -> SparkSession:
    """
    Initializes and returns a SparkSession with a given app name and Delta Lake support.
    If running via spark-submit, the master is picked from the command line.
    Otherwise, it uses the configured constant.
    """

    # Define the paths to the pre-downloaded Delta Lake JARs inside the container.
    delta_jars = [
        "/opt/bitnami/spark/delta_jars/delta-spark_2.13-4.0.0.jar",
        "/opt/bitnami/spark/delta_jars/delta-storage-4.0.0.jar",
        "/opt/bitnami/spark/delta_jars/antlr4-runtime-4.13.1.jar",
        "/opt/bitnami/spark/jars/spark-ui_2.13-4.0.0.jar"
    ]

    spark_builder = SparkSession.builder.appName(app_name)

    # Only set master if not running under spark-submit
    if not os.environ.get("PYSPARK_SUBMIT_ARGS"):
        spark_builder = spark_builder.master(constants.SPARK_MASTER)

    spark_builder = (
        spark_builder
        .config("spark.driver.memory", constants.SPARK_DRIVER_MEMORY)
        .config("spark.driver.host", constants.SPARK_DRIVER_HOST)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", str(constants.SPARK_DRIVER_PORT))
        .config("spark.driver.extraJavaOptions", f"-Duser.home={constants.HOME} -Duser.name=spark")
        .config("spark.executor.extraJavaOptions", f"-Duser.home={constants.HOME} -Duser.name=spark")
        .config("spark.jars", ",".join(delta_jars))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
    )

    spark = spark_builder.getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    print("✅ Spark Master in use:", spark.sparkContext.master)

    return spark
