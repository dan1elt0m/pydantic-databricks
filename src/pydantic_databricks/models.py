from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import Any, List

from jinja2 import Template
from pydantic import BaseModel
from pydantic_core._pydantic_core import PydanticUndefinedType
from pydantic_spark.base import SparkBase


class SchemaNameNotSetError(Exception):
    pass


class TableNameNotSetError(Exception):
    pass


class LocationPrefixNotSetError(Exception):
    pass


class GrantAction(Enum):
    select = "select"
    remove = "remove"
    insert = "insert"


@dataclass(frozen=True, repr=True)
class Grant:
    action: GrantAction
    principal: str

    def __eq__(self, other: Grant) -> bool:
        return self.action == other.action and self.principal == other.principal

    def __hash__(self) -> int:
        return hash((self.action, self.principal))


def is_databricks_env() -> bool:
    """True is it runs inside databricks"""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None


class CreateMode(str, Enum):
    CREATE = "CREATE"
    CREATE_OR_REPLACE = "CREATE OR REPLACE"
    CREATE_IF_NOT_EXISTS = "CREATE IF NOT EXISTS"


class DataSource(str, Enum):
    AVRO = "AVRO"
    BINARYFILE = "BINARYFILE"
    CSV = "CSV"
    DELTA = "DELTA"
    JSON = "JSON"
    ORC = "ORC"
    PARQUET = "PARQUET"
    TEXT = "TEXT"
    JDBC = "JDBC"
    LIBSVM = "LIBSVM"


def render_create_table_template(table_config: TableConfig) -> str:
    """Renders the jinja template with the table config"""
    from importlib import resources as impresources

    from pydantic_databricks import templates

    inp_file = impresources.files(templates) / "create_table.j2"
    with inp_file.open("r") as f:
        template_str = f.read()

    template = Template(template_str)
    return template.render(table_dict=table_config.model_dump())


class DatabricksModel(SparkBase):
    """This is the base pydantic class for all Databricks delta tables.
    Mandatory fields to be overwritten when inheriting:

    - _table_name: the name of the table
    - _schema_name: the name of the schema
    - _grants: the grants for the table

    The _catalog_name is optional, if not given it will be ignored
    """

    _catalog_name: str = None
    _schema_name: str
    _table_name: str
    _grants: set[Grant]
    _location_prefix: str = None
    _table_properties: dict[str, str] = {}
    _table_create_mode: CreateMode = CreateMode.CREATE
    _table_data_source: DataSource = DataSource.DELTA
    _partition_columns: List[str] = None
    _options: dict[str, str] = {}
    _comment: str = None

    @classmethod
    def _get_field(cls, field: str) -> Any | None:  # noqa: ANN401
        """Returns the value of a field on the class if it exists, otherwise None"""
        if hasattr(cls, field):
            return getattr(cls, field).default
        return None

    @classmethod
    @property
    def grants(cls) -> frozenset[Grant]:
        """Returns the grants for the table as a set"""
        base_grants = frozenset(
            chain.from_iterable([base.grants for base in cls.__bases__ if hasattr(base, "_grants")]),
        )
        return frozenset(cls._get_field("_grants")).union(base_grants)

    @classmethod
    @property
    def catalog_name(cls) -> str | None:
        """Returns the catalog name"""
        return cls._get_field("_catalog_name")

    @classmethod
    @property
    def table_name(cls) -> str | None:
        """Returns the table name"""
        return cls._get_field("_table_name")

    @classmethod
    @property
    def schema_name(cls) -> str | None:
        """Returns the schema name"""
        schema_name = cls._get_field("_schema_name")
        if isinstance(schema_name, PydanticUndefinedType):
            raise SchemaNameNotSetError
        return schema_name

    @classmethod
    @property
    def storage_location(cls) -> str | None:
        """Returns the storage location"""
        location_prefix = cls._get_field("_location_prefix")
        if not location_prefix:
            return None
        return f"{location_prefix}/{cls.full_table_name}"

    @classmethod
    @property
    def full_schema_name(cls) -> str:
        """
        Return full schema name

        :raises SchemaNameNotSetError: When schema name is not set, full_schema name cannot be returned
        :return: full schema name of delta table
        """
        if is_databricks_env():
            return f"{cls.catalog_name}.{cls.schema_name}"

        return cls.schema_name

    @classmethod
    @property
    def full_table_name(cls) -> str:
        """
        Returns full table name

        :raises TableNameNotSetError: When table name is not set, full_schema name cannot be returned
        :return: full table name of delta table
        """
        if isinstance(cls.table_name, PydanticUndefinedType):
            raise TableNameNotSetError

        return f"{cls.full_schema_name}.{cls.table_name}"

    @classmethod
    @property
    def table_properties(cls) -> str:
        return ", ".join([f"'{key}'='{value}'" for key, value in cls._get_field("_table_properties").items()])

    @classmethod
    @property
    def options(cls) -> str:
        return ", ".join([f"'{key}'='{value}'" for key, value in cls._get_field("_options").items()])

    @classmethod
    @property
    def table_data_source(cls) -> DataSource:
        return cls._get_field("_table_data_source")

    @classmethod
    @property
    def table_create_mode(cls) -> CreateMode:
        return cls._get_field("_table_create_mode")

    @classmethod
    @property
    def partition_columns(cls) -> str | None:
        partition_columns = cls._get_field("_partition_columns")
        if not partition_columns:
            return None
        return ", ".join(partition_columns)

    @classmethod
    def column_definition(cls) -> List[TableColumn]:
        """Returns the column definition including the sql types for the model"""
        schema = cls.spark_schema()
        columns = []
        for field in schema.get("fields"):
            # Not null only supported in root
            column_properties = ["NOT NULL"] if not field.get("nullable") else []
            column_type = get_column_type(field)
            column = TableColumn(
                column_identifier=field.get("name"),
                column_type=column_type,
                column_properties=column_properties,
            )
            columns.append(column)
        return columns

    @classmethod
    def create_table(cls) -> str:
        """Returns the sql statement to create the table in databricks"""
        table_config = TableConfig(
            replace_table=cls.table_create_mode.value == CreateMode.CREATE_OR_REPLACE.value,
            external=cls.storage_location is not None,
            if_not_exists=cls.table_create_mode.value == CreateMode.CREATE_IF_NOT_EXISTS.value,
            table_name=cls.full_table_name,
            table_specification=TableSpecification(columns=cls.column_definition()),
            table_properties=cls.table_properties,
            options=cls.options,
            using_data_source=str(cls.table_data_source.value),
            partition_columns=cls.partition_columns,
            storage_location=cls.storage_location,
            comment=cls._get_field("_comment"),
        )
        return render_create_table_template(table_config)


class ColumnProperty(BaseModel):
    property_name: str
    property_value: None | str


class TableColumn(BaseModel):
    column_identifier: str
    column_type: str
    column_properties: List[str] = []


class TableSpecification(BaseModel):
    columns: List[TableColumn]
    table_constraints: None | List[str] = []


class TableConfig(BaseModel):
    replace_table: bool = False
    external: bool
    if_not_exists: bool = True
    table_name: str
    table_specification: None | TableSpecification
    using_data_source: None | str
    partition_columns: None | str
    table_properties: None | str
    options: None | str
    storage_location: None | str
    comment: None | str


def handle_struct_type(fields: List[dict[str, Any]]) -> str:
    """recursive function that takes in a list of fields and returns a string of the struct type"""
    columns = []
    for field in fields:
        if isinstance(field.get("type"), dict) and field.get("type").get("type") == "struct":
            columns.append(f"{field.get('name')}:{handle_struct_type(field.get('type').get('fields'))}")
        else:
            columns.append(f"{field.get('name')}:{field.get('type').upper()}")
    return f"struct<{','.join(columns)}>"


def get_column_type(field: dict[str, Any]) -> str:
    """Returns the column type for a given field"""
    if isinstance(field.get("type"), dict) and field.get("type").get("type") == "array":
        element_type = field.get("type").get("elementType")
        if isinstance(element_type, dict):
            if element_type.get("type") == "struct":
                return handle_struct_type(element_type.get("fields"))
            return f"array<{get_column_type(element_type.get('fields'))}>"
        return f"array<{field.get('type').get('elementType')}>"
    if isinstance(field.get("type"), dict) and field.get("type").get("type") == "struct":
        return handle_struct_type(field.get("type").get("fields"))
    if isinstance(field.get("type"), dict) and field.get("type").get("type") == "map":
        return f"map<string,{get_column_type(field.get('type').get('valueType'))}>"
    if field.get("type") == "struct":
        return handle_struct_type(field.get("fields"))

    return field.get("type").upper()
