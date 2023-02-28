import csv
from pyspark.sql import SparkSession


def get_or_create_local_session(app_name: str) -> SparkSession:
    return SparkSession.builder.master("local[1]").appName(app_name).getOrCreate()


with_values_template = """
SELECT data.*
            FROM VALUES
                    {values}
                AS data({headers})
"""


def load_table_from_csv(spark_session: SparkSession, table_name, file_path):
    with open(file_path) as csv_file:
        reader = csv.reader(csv_file)
        headers = ",".join(next(reader))
        values = ",".join(["(" + ",".join(row) + ")" for row in reader])

    query = with_values_template.format(
        with_values_template, values=values, headers=headers
    )

    df = spark_session.sql(query)

    df.createOrReplaceTempView(table_name)
