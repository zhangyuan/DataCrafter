from test import test_helper
from spark import get_or_create_local_session, load_table_from_csv
from spark.testing import assert_dataframes


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
