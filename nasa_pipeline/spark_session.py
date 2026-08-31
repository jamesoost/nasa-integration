from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "nasa-insight-weather") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
