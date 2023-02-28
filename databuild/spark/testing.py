from spark import helpers
from pyspark.sql import SparkSession


def assert_dataframes(df1, df2):
    assert df1.collect() == df2.collect()


def assert_csv_with_table(spark_session: SparkSession, csv_path, table_name):
    helpers.load_table_from_csv(
        spark_session=spark_session, table_name="expected_table", file_path=csv_path
    )
    expected_df = spark_session.sql("select * from expected_table")
    actual_df = spark_session.sql(f"select * from {table_name}")
    assert_dataframes(expected_df, actual_df)
