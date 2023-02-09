from sql import Script, SparkRunner, Context
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


    def test_execute_select_statement(self):
        spark_session = get_or_create_local_session("app")
        runner = SparkRunner(spark_session=spark_session)

        context = Context(variables={
            "logical_date": "2023-01-01"
        })

        statement = "SELECT '{{ var.logical_date }}' AS date"

        script = Script(statement)

        # pylint: disable=invalid-name
        df = runner.run(script, context=context)

        expected_df = spark_session.sql("select '2023-01-01' AS date")
        assert_dataframes(expected_df, df)
