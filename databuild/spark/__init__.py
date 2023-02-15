from pyspark.sql import SparkSession

def get_or_create_local_session(app_name: str) -> SparkSession:
    return SparkSession.builder.master("local[1]").appName(app_name).getOrCreate()
