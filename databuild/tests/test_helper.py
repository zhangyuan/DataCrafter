import os
import shutil

def fixture_path(path):
    parent_path = os.path.dirname(__file__)
    return os.path.join(parent_path, path)


def delete_spark_warehouse():
    tests_folder = os.path.dirname(__file__)
    module_folder = os.path.dirname(tests_folder)
    spark_warehouse_folder = os.path.join(module_folder, "spark-warehouse")
    if os.path.exists(spark_warehouse_folder) and os.path.isdir(spark_warehouse_folder):
        shutil.rmtree(spark_warehouse_folder)
