from spark import helpers
from pyspark.sql import SparkSession


def assert_dataframes(df1, df2):
    df1_collect = df1.collect()
    df2_collect = df2.collect()

    if df1_collect != df2_collect:
        print("dataframes are not equal:")
        print("left:")
        df1.show()
        print("right:")
        df2.show()

    assert df1_collect == df2_collect


def assert_csv_with_table(spark_session: SparkSession, csv_path, table_name):
    spark_session.sql("drop table if exists expected_table")
    helpers.load_table_from_csv(
        spark_session=spark_session, table_name="expected_table", file_path=csv_path
    )
    expected_df = spark_session.sql("select * from expected_table")
    actual_df = spark_session.sql(f"select * from {table_name}")
    assert_dataframes(expected_df, actual_df)
