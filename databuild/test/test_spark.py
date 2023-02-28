from test import test_helper
from spark.helpers import get_or_create_local_session, load_table_from_csv
from spark.testing import assert_dataframes, assert_csv_with_table


class TestSpark:
    def test_load_csv_as_table(self):
        spark_session = get_or_create_local_session("app")
        load_table_from_csv(
            spark_session,
            "orders",
            test_helper.fixture_path("fixtures/data/orders.csv"),
        )
        df = spark_session.sql("select * from orders")
        expected_df = spark_session.sql(
            """
        SELECT data.*
            FROM VALUES
                    ('ODER12345', 10000, DATE('2023-01-01 18:00:00'))
                AS data(id, user_id, created_at)
        """
        )
        assert_dataframes(expected_df, df)

    def test_load_csv_with_multiple_rows_as_table(self):
        spark_session = get_or_create_local_session("app")
        load_table_from_csv(
            spark_session,
            "orders",
            test_helper.fixture_path("fixtures/data/orders_multiple.csv"),
        )
        df = spark_session.sql("select * from orders")
        expected_df = spark_session.sql(
            """
        SELECT data.*
            FROM VALUES
                    ('ODER12345', 10000, DATE('2023-01-01 18:00:00')),
                    ('ODER12346', 10000, DATE('2023-01-01 19:00:00'))
                AS data(id, user_id, created_at)
        """
        )
        assert_dataframes(expected_df, df)        

    def test_assert_csv_and_table(self):
        spark_session = get_or_create_local_session("app")
        spark_session.sql(
            """
        SELECT data.*
            FROM VALUES
                    ('ODER12345', 10000, DATE('2023-01-01 18:00:00'))
                AS data(id, user_id, created_at)
        """
        ).createOrReplaceTempView("orders")

        assert_csv_with_table(
            spark_session=spark_session,
            csv_path=test_helper.fixture_path("fixtures/data/orders.csv"),
            table_name="orders",
        )
