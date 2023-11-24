import pytest
from datetime import datetime

from pydantic_databricks.models import DatabricksModel, LocationPrefixNotSetError
from pydantic_databricks.sql import create_external_table_if_not_exists, create_or_replace_table, \
    create_table_if_not_exists, get_column_definition, create_or_replace_external_table


def test_add_table_properties():
    class Foo(DatabricksModel):
        _table_name = "table"
        _schema_name = "schema"
        _table_properties = {"property1": "value1", "property2": "value2"}

        c1: str
        c2: int

    assert create_table_if_not_exists(Foo) == "CREATE TABLE IF NOT EXISTS schema.table (c1 STRING, c2 LONG) TBLPROPERTIES(property1 = value1, property2 = value2)"

def test_create_or_replace_table():
    class Foo(DatabricksModel):
        _table_name = "table"
        _schema_name = "schema"

        c1: str
        c2: int

    assert create_or_replace_table(Foo) == "CREATE OR REPLACE TABLE schema.table (c1 STRING, c2 LONG)"

def test_create_table_if_not_exists():
    class Foo(DatabricksModel):
        _table_name = "table"
        _schema_name = "schema"

        c1: str
        c2: int

    assert create_table_if_not_exists(Foo) == "CREATE TABLE IF NOT EXISTS schema.table (c1 STRING, c2 LONG)"

def test_create_or_replace_external_table():
    class Foo(DatabricksModel):
        _table_name = "table"
        _schema_name = "schema"
        _location_prefix = "s3://some-bucket"

        c1: str
        c2: int

    assert create_or_replace_external_table(Foo) == "CREATE OR REPLACE EXTERNAL TABLE schema.table (c1 STRING, c2 LONG) USING DELTA LOCATION 's3://some-bucket/schema.table'"
def test_create_external_table_if_not_exists_with_location_prefix():
    class Foo(DatabricksModel):
        _table_name = "table"
        _schema_name = "schema"
        _location_prefix = "s3://some-bucket"

        c1: str
        c2: int

    assert create_external_table_if_not_exists(Foo) == "CREATE EXTERNAL TABLE IF NOT EXISTS schema.table (c1 STRING, c2 LONG) USING DELTA LOCATION 's3://some-bucket/schema.table'"


def test_create_external_table_without_location_prefix():
    class Foo(DatabricksModel):
        _table_name = "table"
        _schema_name = "schema"

        c1: str
        c2: int

    with pytest.raises(LocationPrefixNotSetError):
        create_external_table_if_not_exists(Foo) == "CREATE EXTERNAL TABLE IF NOT EXISTS schema.table (c1 STRING, c2 LONG) USING DELTA LOCATION 's3://some-bucket/schema.table'"

def test_get_column_definition():
    class Foo(DatabricksModel):
        c1: str
        c2: int
        c3: float
        c4: bool
        c5: datetime

    assert get_column_definition(Foo) == "c1 STRING, c2 LONG, c3 DOUBLE, c4 BOOLEAN, c5 TIMESTAMP"