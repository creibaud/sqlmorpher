import tempfile
import pytest
from sqlmorpher import create_connection_string


def test_sqlite_connection_string_tempfile() -> None:
    with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp:
        dsn = create_connection_string("sqlite", path=tmp.name)
        assert dsn.startswith("sqlite:///")


def test_sqlite_memory_connection_string() -> None:
    dsn = create_connection_string("sqlite")
    assert dsn == "sqlite:///:memory:"


def test_duckdb_connection_string_tempfile() -> None:
    with tempfile.NamedTemporaryFile(suffix=".duckdb") as tmp:
        dsn = create_connection_string("duckdb", path=tmp.name)
        assert dsn.startswith("duckdb:///")


def test_access_connection_string_tempfile() -> None:
    with tempfile.NamedTemporaryFile(suffix=".accdb") as tmp:
        dsn = create_connection_string(
            "access",
            path=tmp.name,
            driver="Microsoft Access Driver (*.mdb, *.accdb)",
        )
        assert dsn.startswith("access+pyodbc:///")


def test_firebird_connection_string_tempfile() -> None:
    with tempfile.NamedTemporaryFile(suffix=".fdb") as tmp:
        dsn = create_connection_string(
            "firebird", path=tmp.name, user="sysdba", password="masterkey"
        )
        assert dsn.startswith("firebird+pyodbc:///")


def test_postgresql_connection_string() -> None:
    dsn = create_connection_string(
        "postgresql",
        host="localhost",
        port=5432,
        user="postgres",
        password="secret",
        database="mydb",
    )
    assert dsn.startswith("postgresql+psycopg2://")
    assert "secret" in dsn


def test_mysql_connection_string() -> None:
    dsn = create_connection_string(
        "mysql",
        host="localhost",
        port=3306,
        user="root",
        password="secret",
        database="mydb",
    )
    assert dsn.startswith("mysql+pymysql://")
    assert "secret" in dsn


def test_oracle_connection_string() -> None:
    dsn = create_connection_string(
        "oracle",
        host="localhost",
        port=1521,
        user="system",
        password="secret",
        database="xe",
    )
    assert dsn.startswith("oracle+cx_oracle://")
    assert "secret" in dsn


def test_sqlserver_connection_string() -> None:
    dsn = create_connection_string(
        "mssql",
        host="localhost",
        port=1433,
        user="sa",
        password="secret",
        database="mydb",
        driver="ODBC Driver 18 for SQL Server",
        options={"Encrypt": "yes"},
    )
    assert dsn.startswith("mssql+pyodbc://")
    assert "Encrypt%3Dyes" in dsn


def test_invalid_db_type() -> None:
    with pytest.raises(ValueError):
        create_connection_string("notadb")


def test_invalid_port() -> None:
    with pytest.raises(ValueError):
        create_connection_string(
            "postgresql",
            host="localhost",
            port=70000,
            user="u",
            password="p",
            database="db",
        )
