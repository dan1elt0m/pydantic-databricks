import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_warehouse_dir() -> str:
    return tempfile.mkdtemp()


@pytest.fixture(scope="session", autouse=True)
def spark(spark_warehouse_dir) -> SparkSession:
    builder = (
        SparkSession.builder.config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.2.20,com.microsoft.sqlserver:mssql-jdbc:9.2.1.jre8",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.warehouse.dir", spark_warehouse_dir)
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(autouse=True)
def _empty_spark_warehouse(spark: SparkSession):
    for db in spark.catalog.listDatabases():
        for table in spark.catalog.listTables(db.name):
            spark.sql(f"DROP TABLE {table.database}.{table.name}")
