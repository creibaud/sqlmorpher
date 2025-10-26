import tempfile
from sqlmorpher import create_connection_string
from sqlmorpher import Database


def test_database_sqlite_creation() -> None:
    with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp:
        conn_str = create_connection_string("sqlite", path=tmp.name)
        db = Database(type="sqlite", connection_string=conn_str)
        is_connection = db.validate_connection()

        assert db.type == "sqlite"
        assert db.engine is not None
        assert db.metadata is not None
        assert db.session_factory is not None
        assert is_connection is True


def test_database_reflect_tables() -> None:
    with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp:
        conn_str = create_connection_string("sqlite", path=tmp.name)
        db = Database(type="sqlite", connection_string=conn_str)
        db.execute_query(
            """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        """
        )
        db.reflect_tables()
        table = db.metadata.tables.get("test_table")
        assert table is not None
        assert "id" in table.c
        assert "name" in table.c
