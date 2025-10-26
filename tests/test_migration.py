import pytest
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlmorpher import Database, migrate
from typing import Dict, Any, Optional


@pytest.fixture
def old_db():
    db = Database(type="sqlite", connection_string="sqlite:///:memory:")
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("username", String),
    )
    Table(
        "profiles",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer),
        Column("phone", String),
    )
    metadata.create_all(db.engine)
    db.metadata = metadata

    db.execute_query("INSERT INTO users (id, username) VALUES (1, 'alice')")
    db.execute_query(
        "INSERT INTO profiles (id, user_id, phone) VALUES (1, 1, '123456')"
    )
    return db


@pytest.fixture
def new_db():
    db = Database(type="sqlite", connection_string="sqlite:///:memory:")
    metadata = MetaData()
    Table(
        "comptes",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("login", String),
        Column("telephone", String),
    )
    metadata.create_all(db.engine)
    db.metadata = metadata
    return db


def test_migrate_with_transform_function(old_db, new_db):
    def transform_user_data(row: Dict[str, Any]) -> Dict[str, Any]:
        transformed = row.copy()

        if "login" in transformed and transformed["login"]:
            transformed["login"] = transformed["login"].upper()

        if "telephone" in transformed and transformed["telephone"]:
            transformed["telephone"] = f"+33-{transformed['telephone']}"

        return transformed

    transform_registry = {
        "transform_user_data": transform_user_data,
    }

    mapping = [
        {
            "root_table": "users",
            "joins": [
                {
                    "table": "profiles",
                    "on": "users.id = profiles.user_id",
                    "type": "LEFT",
                }
            ],
            "columns": {
                "users.id": "id",
                "users.username": "login",
                "profiles.phone": "telephone",
            },
            "target_table": "comptes",
            "insert_function": "transform_user_data",
        }
    ]

    migrate(old_db, new_db, mapping, transform_registry=transform_registry)

    result = new_db.execute_query("SELECT * FROM comptes").fetchall()
    assert len(result) == 1

    row = dict(result[0]._mapping)

    assert row["login"] == "ALICE"
    assert row["telephone"] == "+33-123456"
    assert row["id"] == 1


def test_migrate_with_filtering_function(old_db, new_db):
    def filter_users(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not row.get("telephone"):
            return None

        row["telephone"] = row["telephone"].replace("-", "").replace(" ", "")

        return row

    transform_registry = {
        "filter_users": filter_users,
    }

    mapping = [
        {
            "root_table": "users",
            "joins": [
                {
                    "table": "profiles",
                    "on": "users.id = profiles.user_id",
                    "type": "LEFT",
                }
            ],
            "columns": {
                "users.id": "id",
                "users.username": "login",
                "profiles.phone": "telephone",
            },
            "target_table": "comptes",
            "insert_function": "filter_users",
        }
    ]

    migrate(old_db, new_db, mapping, transform_registry=transform_registry)

    result = new_db.execute_query("SELECT * FROM comptes").fetchall()

    assert len(result) == 1

    for row in result:
        row_dict = dict(row._mapping)
        assert row_dict["telephone"]
        assert "-" not in row_dict["telephone"]
        assert " " not in row_dict["telephone"]


def test_migrate_with_multiple_transforms(old_db, new_db):
    def uppercase_transform(row: Dict[str, Any]) -> Dict[str, Any]:
        transformed = row.copy()
        for key, value in transformed.items():
            if isinstance(value, str):
                transformed[key] = value.upper()
        return transformed

    transform_registry = {
        "uppercase_transform": uppercase_transform,
    }

    mapping = [
        {
            "root_table": "users",
            "joins": [
                {
                    "table": "profiles",
                    "on": "users.id = profiles.user_id",
                    "type": "LEFT",
                }
            ],
            "columns": {
                "users.id": "id",
                "users.username": "login",
                "profiles.phone": "telephone",
            },
            "target_table": "comptes",
            "insert_function": "uppercase_transform",
        }
    ]

    migrate(old_db, new_db, mapping, transform_registry=transform_registry)

    result = new_db.execute_query("SELECT * FROM comptes").fetchall()
    assert len(result) == 1

    row = dict(result[0]._mapping)
    assert row["login"] == "ALICE"
    assert row["telephone"] == "123456"


def test_migrate_with_unknown_function_raises_error(old_db, new_db):
    transform_registry = {
        "existing_function": lambda x: x,
    }

    mapping = [
        {
            "root_table": "users",
            "joins": [],
            "columns": {
                "users.id": "id",
                "users.username": "login",
            },
            "target_table": "comptes",
            "insert_function": "non_existent_function",
        }
    ]

    with pytest.raises(
        ValueError, match="Transform function 'non_existent_function' not found"
    ):
        migrate(old_db, new_db, mapping, transform_registry=transform_registry)


def test_migrate_without_transform_function(old_db, new_db):
    mapping = [
        {
            "root_table": "users",
            "joins": [
                {
                    "table": "profiles",
                    "on": "users.id = profiles.user_id",
                    "type": "LEFT",
                }
            ],
            "columns": {
                "users.id": "id",
                "users.username": "login",
                "profiles.phone": "telephone",
            },
            "target_table": "comptes",
        }
    ]

    migrate(old_db, new_db, mapping)

    result = new_db.execute_query("SELECT * FROM comptes").fetchall()
    assert len(result) == 1

    row = dict(result[0]._mapping)
    assert row["login"] == "alice"
    assert row["telephone"] == "123456"
