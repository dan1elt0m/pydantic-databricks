from typing import Optional

import pytest
from pydantic_databricks.models import CreateMode, DatabricksModel, DataSource
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField


def test_create_table(spark):
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"

        col1: str
        col2: int
        col3: float
        col4: Optional[bool]

    result = Schema.create_table()
    assert "CREATE TABLE default.test" in result

    spark.sql(result)
    df = spark.sql("SELECT * FROM default.test")
    assert df.schema.fields == [
        StructField("col1", StringType(), False),
        StructField("col2", LongType(), False),
        StructField("col3", DoubleType(), False),
        StructField("col4", BooleanType(), True),
    ]

    table_description = spark.sql("describe table default.test")
    assert table_description.collect()


def test_create_table_partition_columns(spark):
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _partition_columns = ["col1", "col2"]

        col1: str
        col2: int
        col3: float

    result = Schema.create_table()
    assert "PARTITIONED BY (col1, col2)" in result

    spark.sql(result)
    # Check parition columns
    table_description = spark.sql("describe table default.test").collect()
    assert table_description[5]["col_name"] == "col1"
    assert table_description[6]["col_name"] == "col2"


def test_create_table_with_table_properties(spark):
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _table_properties = {"spark.sql.sources.partitionOverwriteMode": "dynamic"}

        col1: str
        col2: int

    spark.sql(Schema.create_table())

    table_properties = spark.sql("show tblproperties default.test").collect()
    assert table_properties[2]["key"] == "spark.sql.sources.partitionOverwriteMode"
    assert table_properties[2]["value"] == "dynamic"


def test_create_table_with_options(spark):
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _options = {"test": "abc"}

        col1: str
        col2: int

    query = Schema.create_table()
    assert "OPTIONS('test'='abc')" in query
    spark.sql(query)

    table_properties = spark.sql("show tblproperties default.test").collect()
    assert table_properties[2]["key"] == "option.test"
    assert table_properties[2]["value"] == "abc"


def test_create_table_if_not_exists():
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _table_create_mode = CreateMode.CREATE_IF_NOT_EXISTS

        col1: str
        col2: int

    result = Schema.create_table()
    assert "CREATE TABLE IF NOT EXISTS default.test" in result


def test_create_or_replace_table():
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _table_create_mode = CreateMode.CREATE_OR_REPLACE

        col1: str
        col2: int

    result = Schema.create_table()
    assert "CREATE OR REPLACE TABLE default.test" in result


def test_create_table_default():
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"

        col1: str
        col2: int

    result = Schema.create_table()
    assert "CREATE TABLE default.test" in result
    assert "USING DELTA" in result


@pytest.mark.parametrize("data_source", [DataSource.PARQUET, DataSource.DELTA, DataSource.JDBC, DataSource.JSON])
def test_create_table_data_sources(data_source):
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _table_data_source = data_source

        col1: str
        col2: int

    result = Schema.create_table()
    assert f"USING {data_source.value}" in result


def test_create_table_external():
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _location_prefix = "s3://bucket/prefix"

        col1: str
        col2: int

    result = Schema.create_table()
    assert "LOCATION 's3://bucket/prefix/default.test'" in result
    assert "CREATE EXTERNAL TABLE" in result


def test_create_table_with_comment():
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _comment = "test comment"

        col1: str
        col2: int

    result = Schema.create_table()
    assert "COMMENT 'test comment'" in result
