from pyspark.sql import SparkSession


def get_logger(spark: SparkSession, module_name: str):
    """
    Gets a logger instance that works with Spark's log4j configuration.

    This allows Python code to log through the same log4j setup used by Spark,
    enabling centralized log configuration.

    Args:
        spark (SparkSession): The active SparkSession.
        module_name (str): The name of the module for which the logger is created.
                           e.g., "Bronze Layer", "Gold Layer".

    Returns:
        A log4j logger instance with methods like .info(), .warn(), .error().
    """
    log4j = spark._jvm.org.apache.log4j
    app_logger_prefix = "nyc311"
    sanitized_module_name = module_name.replace(" ", "_").replace("-", "_")
    logger_name = f"{app_logger_prefix}.{sanitized_module_name}"
    return log4j.LogManager.getLogger(logger_name)
