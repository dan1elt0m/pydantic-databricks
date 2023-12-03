from typing import List, Optional

from pydantic import BaseModel, conlist


class ConstraintOptions(BaseModel):
    not_enforced: Optional[bool]
    deferrable: Optional[bool]
    initially_deferred: Optional[bool]
    norely: Optional[bool]


class ForeignKeyOptions(BaseModel):
    match_full: Optional[bool]
    on_update_no_action: Optional[bool]
    on_delete_no_action: Optional[bool]


class CheckConstraint(BaseModel):
    name: Optional[str]
    condition: str
    enforced: Optional[bool]
    options: ConstraintOptions


class KeyConstraint(BaseModel):
    name: Optional[str]
    primary_key: Optional[bool]
    key_columns: conlist(str, min_items=1)
    timeseries: Optional[bool]
    constraint_options: List[str]
    foreign_key: Optional[bool]
    foreign_key_columns: conlist(str, min_items=1)
    parent_table: Optional[str]
    parent_columns: Optional[List[str]]
    foreign_key_options: ForeignKeyOptions


class TableConstraints(BaseModel):
    table_name: str
    constraints: List[CheckConstraint | KeyConstraint]


# Example usage:
check_constraint = CheckConstraint(
    name="my_check",
    condition="column1 > 0",
    enforced=True,
    options=ConstraintOptions(not_enforced=False, deferrable=True, initially_deferred=True, norely=False),
)

key_constraint = KeyConstraint(
    name="pk_constraint",
    primary_key=True,
    key_columns=["id"],
    timeseries=True,
    constraint_options=["NOT ENFORCED"],
    foreign_key=False,
    foreign_key_columns=[],
    parent_table="",
    parent_columns=[],
    foreign_key_options=ForeignKeyOptions(match_full=True, on_update_no_action=True, on_delete_no_action=True),
)
