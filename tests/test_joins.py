import pytest
import tempfile
from sqlmorpher import (
    Database,
    create_connection_string,
    validate_joins,
    generate_join_query,
)


@pytest.fixture
def db_with_schema():
    with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp:
        conn_str = create_connection_string("sqlite", path=tmp.name)
        db = Database(type="sqlite", connection_string=conn_str)

        db.execute_query(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL
            );
        """
        )
        db.execute_query(
            """
            CREATE TABLE profiles (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                phone TEXT,
                country_id INTEGER
            );
        """
        )
        db.execute_query(
            """
            CREATE TABLE countries (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        """
        )
        db.reflect_tables()
        yield db


def test_validate_joins_valid(db_with_schema):
    joins = [
        {
            "table": "profiles",
            "on_clause": "users.id = profiles.user_id",
            "type": "left",
        },
        {
            "table": "countries",
            "on_clause": "profiles.country_id = countries.id",
            "type": "inner",
        },
    ]

    validate_joins(db_with_schema, "users", joins)


def test_validate_joins_invalid_table(db_with_schema):
    joins = [
        {
            "table": "nonexistent",
            "on_clause": "users.id = nonexistent.id",
            "type": "left",
        }
    ]
    with pytest.raises(ValueError, match="Table 'nonexistent' does not exist"):
        validate_joins(db_with_schema, "users", joins)


def test_validate_joins_invalid_column(db_with_schema):
    joins = [
        {
            "table": "profiles",
            "on_clause": "users.id = profiles.unknown_col",
            "type": "left",
        }
    ]
    with pytest.raises(ValueError, match="Column 'unknown_col' does not exist"):
        validate_joins(db_with_schema, "users", joins)


def test_validate_joins_type_mismatch(db_with_schema):
    db_with_schema.execute_query("ALTER TABLE profiles ADD COLUMN created DATE;")
    joins = [
        {
            "table": "profiles",
            "on_clause": "users.id = profiles.created",
            "type": "left",
        }
    ]
    with pytest.raises(
        ValueError,
        match=r"Type mismatch between columns 'users\.id' .* 'profiles\.created'",
    ):
        validate_joins(db_with_schema, "users", joins)


def test_validate_joins_invalid_on_clause(db_with_schema):
    joins = [{"table": "profiles", "on_clause": "invalid_on_clause", "type": "left"}]
    with pytest.raises(ValueError, match="Invalid ON clause"):
        validate_joins(db_with_schema, "users", joins)


def test_generate_join_query(db_with_schema):
    joins = [
        {
            "table": "profiles",
            "on_clause": "users.id = profiles.user_id",
            "type": "LEFT",
        },
        {
            "table": "countries",
            "on_clause": "profiles.country_id = countries.id",
            "type": "INNER",
        },
    ]

    select_columns = {
        "users.id": "id",
        "profiles.phone": "phone",
        "countries.name": "country_name",
    }

    sql = generate_join_query(db_with_schema, "users", joins, select_columns)

    assert "SELECT" in sql
    assert "users.id AS id" in sql
    assert "profiles.phone AS phone" in sql
    assert "countries.name AS country_name" in sql
    assert "LEFT OUTER JOIN profiles" in sql
    assert "JOIN countries" in sql
    assert "ON users.id = profiles.user_id" in sql
    assert "ON profiles.country_id = countries.id" in sql
