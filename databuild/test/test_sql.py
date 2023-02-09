from sql import Script, SparkRunner
from spark import get_or_create_local_session
from spark.testing import assert_dataframes


class TestSQL:
    def test_execute_select_statement(self):
        spark_session = get_or_create_local_session("app")
        runner = SparkRunner(spark_session=spark_session)

        script = Script("select 1")

        # pylint: disable=invalid-name
        df = runner.run(script)

        expected_df = spark_session.sql("select 1")
        assert_dataframes(expected_df, df)
