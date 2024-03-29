import unittest
from pathlib import Path
from sql import SparkRunner, Context
from spark.helpers import get_or_create_local_session
from spark.testing import assert_dataframes, assert_csv_with_table
from tests import test_helper


class TestSQL(unittest.TestCase):
    def setUp(self) -> None:
        self.spark_session = get_or_create_local_session("app")
        self.spark_session.sql("DROP TABLE IF EXISTS items")
        self.spark_session.sql("DROP TABLE IF EXISTS events")
        test_helper.delete_spark_warehouse()
        return super().setUp()

    def test_execute_select_statement(self):
        runner = SparkRunner(spark_session=self.spark_session)

        statement = "select 1"

        # pylint: disable=invalid-name
        df = runner.execute_template(statement)

        expected_df = self.spark_session.sql("select 1")
        assert_dataframes(expected_df, df)

    def test_compile_params(self):
        spark_session = get_or_create_local_session("app")
        runner = SparkRunner(spark_session=spark_session)

        context = Context(params={"logical_date": "2023-01-01"}, template_directory=".")

        statement = "SELECT '{{ params.logical_date }}' AS date"

        # pylint: disable=invalid-name
        df = runner.execute_template(statement, context=context)

        expected_df = spark_session.sql("select '2023-01-01' AS date")
        assert_dataframes(expected_df, df)

    def test_compile_template_file(self):
        spark_session = get_or_create_local_session("app")
        runner = SparkRunner(spark_session=spark_session)

        path = Path(__file__)
        template_directory = path.joinpath(
            path.parent.absolute(), "fixtures", "templates"
        )

        context = Context(
            params={"logical_date": "2023-01-01"}, template_directory=template_directory
        )

        # pylint: disable=invalid-name
        df = runner.execute_template_path("select_date.j2.sql", context=context)

        expected_df = spark_session.sql("select '2023-01-01' AS date")
        assert_dataframes(expected_df, df)

    def test_compile_template_file_with_star(self):
        runner = SparkRunner(spark_session=self.spark_session)

        path = Path(__file__)

        template_directory = path.joinpath(
            path.parent.absolute(), "fixtures", "templates"
        )

        context = Context(
            params={"logical_date": "2023-01-01"}, template_directory=template_directory
        )

        # pylint: disable=invalid-name
        runner.execute_template_path("items/create_items_table.j2.sql", context=context)

        runner.execute_template_path(
            "items/insert_into_items_table.j2.sql", context=context
        )

        assert_csv_with_table(
            spark_session=self.spark_session,
            csv_path=test_helper.fixture_path("fixtures/data/items/expected_items.csv"),
            table_name="items",
        )

    def test_compile_template_file_with_star_and_except_columns(self):
        runner = SparkRunner(spark_session=self.spark_session)

        path = Path(__file__)

        template_directory = path.joinpath(
            path.parent.absolute(), "fixtures", "templates"
        )

        context = Context(
            template_directory=template_directory,
        )

        # pylint: disable=invalid-name
        runner.execute_template_path(
            "events/create_events_table.j2.sql", context=context
        )

        runner.execute_template_path("events/transform.j2.sql", context=context)

        assert_csv_with_table(
            spark_session=self.spark_session,
            csv_path=test_helper.fixture_path(
                "fixtures/data/events/expected_events.csv"
            ),
            table_name="events",
        )
