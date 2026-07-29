import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "gsk-pharma-lead-time") -> SparkSession:
    os.environ.pop("SPARK_HOME", None)
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
