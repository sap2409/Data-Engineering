import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "bronze-silver-quality") -> SparkSession:
    """Create a local Spark session suitable for learning workloads.

    Clears SPARK_HOME so pip-installed PySpark jars are used instead of a
    mismatched system Spark install (common on Windows learning setups).
    """
    # Avoid JVM/Python package skew when a global SPARK_HOME is set.
    os.environ.pop("SPARK_HOME", None)

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
