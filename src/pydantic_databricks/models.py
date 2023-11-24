from __future__ import annotations

import os
from enum import Enum
from itertools import chain
from typing import Any

from dataclasses import dataclass

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
        return cls._get_field("_schema_name")

    @classmethod
    @property
    def location_prefix(cls) -> str:
        """Returns the location prefix"""
        location_prefix = cls._get_field("_location_prefix")
        if not location_prefix:
            raise LocationPrefixNotSetError("set the _location_prefix field on the class")
        return location_prefix

    @classmethod
    @property
    def full_schema_name(cls) -> str:
        """
        Return full schema name

        :raises SchemaNameNotSetError: When schema name is not set, full_schema name cannot be returned
        :return: full schema name of delta table
        """
        if not cls.schema_name:
            raise SchemaNameNotSetError

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
        if not cls.table_name:
            raise TableNameNotSetError

        return f"{cls.full_schema_name}.{cls.table_name}"

    @classmethod
    @property
    def table_properties(cls) -> dict[str, str]:
        return cls._get_field("_table_properties")
