from pydantic_databricks import DatabricksModel
from pydantic_databricks.models import Grant, GrantAction


def test_grant():
    class Schema(DatabricksModel):
        _table_name = "test"
        _schema_name = "default"
        _grants = {
            Grant(action=GrantAction.MODIFY, principal="user1"),
            Grant(action=GrantAction.SELECT, principal="user2"),
        }

        col1: str

    assert sorted(Schema.grant_statements()) == (
        ["GRANT MODIFY ON default.test TO `user1`", "GRANT SELECT ON default.test TO `user2`"]
    )
