from typing import Type

from pydantic_databricks.models import DatabricksModel


def get_column_definition(model: Type[DatabricksModel]) -> str:
    """Returns the column definition including the sql types for the model"""
    schema = model.spark_schema()
    return ", ".join([f"{field.get('name')} {field.get('type').upper()}" for field in schema.get("fields")])


def add_table_properties(sql: str, model: Type[DatabricksModel]):
    """Adds table properties to sql"""
    if not model.table_properties:
        return sql
    return f"{sql} TBLPROPERTIES({', '.join([f'{k} = {v}' for k, v in model.table_properties.items()])})"


def create_table_if_not_exists(model: Type[DatabricksModel]) -> str:
    """Returns the sql statement to create the table in databricks"""
    columns = get_column_definition(model)
    return add_table_properties(f"CREATE TABLE IF NOT EXISTS {model.full_table_name} ({columns})", model)


def create_or_replace_table(model: Type[DatabricksModel]) -> str:
    """Returns the sql statement to create or replace the table in databricks"""
    columns = get_column_definition(model)
    return add_table_properties(f"CREATE OR REPLACE TABLE {model.full_table_name} ({columns})", model)


def create_external_table_if_not_exists(model: Type[DatabricksModel]) -> str:
    """Returns the sql statement to create the table in databricks"""
    columns = get_column_definition(model)
    return  add_table_properties(f"CREATE EXTERNAL TABLE IF NOT EXISTS {model.full_table_name} ({columns}) USING DELTA LOCATION '{model.location_prefix}/{model.full_table_name}'", model)


def create_or_replace_external_table(model: Type[DatabricksModel]) -> str:
    """Returns the sql statement to create or replace the table in databricks"""
    columns = get_column_definition(model)
    return add_table_properties(f"CREATE OR REPLACE EXTERNAL TABLE {model.full_table_name} ({columns}) USING DELTA LOCATION '{model.location_prefix}/{model.full_table_name}'", model)

