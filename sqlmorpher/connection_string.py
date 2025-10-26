import os
from urllib.parse import quote_plus
from pydantic import validate_call
from typing import Optional, Dict, Any, cast
from dotenv import load_dotenv


load_dotenv()


DEFAULTS: Dict[str, Dict[str, Any]] = {
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "",
        "database": "postgres",
    },
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "",
        "database": "",
    },
    "oracle": {
        "host": "localhost",
        "port": 1521,
        "user": "system",
        "password": "",
        "database": "xe",
    },
    "mssql": {
        "host": "localhost",
        "port": 1433,
        "user": "sa",
        "password": "",
        "database": "",
    },
}


@validate_call
def _create_path_based(
    type: str, path: str, driver: Optional[str] = None, check_path: bool = True
) -> str:
    if check_path and not os.path.exists(path):
        raise ValueError(f"Database file path does not exist: {path}")
    if driver:
        return f"{type}+{driver}:///{path}"
    return f"{type}:///{path}"


@validate_call
def _create_server_based(
    type: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    driver: Optional[str] = None,
) -> str:
    if port <= 0 or port > 65535:
        raise ValueError("Port must be between 1 and 65535.")
    if driver:
        return f"{type}+{driver}://{user}:{password}@{host}:{port}/{database}"
    return f"{type}://{user}:{password}@{host}:{port}/{database}"


@validate_call
def _create_odbc(
    type: str, conn_str: str, driver: Optional[str] = None
) -> str:
    return (
        f"{type}+{driver}:///?odbc_connect={quote_plus(conn_str)}"
        if driver
        else f"{type}:///?odbc_connect={quote_plus(conn_str)}"
    )


def sqlite(path: Optional[str] = None) -> str:
    return _create_path_based("sqlite", path) if path else "sqlite:///:memory:"


def duckdb(path: Optional[str] = None) -> str:
    return _create_path_based("duckdb", path) if path else "duckdb:///:memory:"


def postgresql(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
) -> str:
    host = host or DEFAULTS["postgresql"]["host"]
    port = port or DEFAULTS["postgresql"]["port"]
    user = user or os.getenv("POSTGRES_USER") or DEFAULTS["postgresql"]["user"]
    password = (
        password
        or os.getenv("POSTGRES_PASSWORD")
        or DEFAULTS["postgresql"]["password"]
    )
    database = database or DEFAULTS["postgresql"]["database"]
    return _create_server_based(
        "postgresql",
        cast(str, host),
        cast(int, port),
        cast(str, user),
        cast(str, password),
        cast(str, database),
        driver="psycopg2",
    )


def mysql(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
) -> str:
    host = host or DEFAULTS["mysql"]["host"]
    port = port or DEFAULTS["mysql"]["port"]
    user = user or os.getenv("MYSQL_USER") or DEFAULTS["mysql"]["user"]
    password = (
        password
        or os.getenv("MYSQL_PASSWORD")
        or DEFAULTS["mysql"]["password"]
    )
    database = database or DEFAULTS["mysql"]["database"]
    return _create_server_based(
        "mysql",
        cast(str, host),
        cast(int, port),
        cast(str, user),
        cast(str, password),
        cast(str, database),
        driver="pymysql",
    )


def oracle(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
) -> str:
    host = host or DEFAULTS["oracle"]["host"]
    port = port or DEFAULTS["oracle"]["port"]
    user = user or os.getenv("ORACLE_USER") or DEFAULTS["oracle"]["user"]
    password = (
        password
        or os.getenv("ORACLE_PASSWORD")
        or DEFAULTS["oracle"]["password"]
    )
    database = database or DEFAULTS["oracle"]["database"]
    return _create_server_based(
        "oracle",
        cast(str, host),
        cast(int, port),
        cast(str, user),
        cast(str, password),
        cast(str, database),
        driver="cx_oracle",
    )


def sqlserver(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    driver: str = "ODBC Driver 18 for SQL Server",
    options: Optional[Dict[str, str]] = None,
) -> str:
    conn_str = (
        f"Driver={{{driver}}};Server={host},{port};"
        f"Database={database};UID={user};PWD={password}"
    )
    if options:
        conn_str += ";" + ";".join(f"{k}={v}" for k, v in options.items())
    return _create_odbc("mssql", conn_str, driver="pyodbc")


def access(
    path: str, driver: str = "Microsoft Access Driver (*.mdb, *.accdb)"
) -> str:
    conn_str = f"Driver={{{driver}}};DBQ={path}"
    return _create_odbc("access", conn_str, driver="pyodbc")


def firebird(
    path: str,
    user: str = "sysdba",
    password: str = "masterkey",
    driver: str = "Firebird",
) -> str:
    conn_str = f"DRIVER={{{driver}}};DBNAME={path};UID={user};PWD={password}"
    return _create_odbc("firebird", conn_str, driver="pyodbc")


DB_BUILDERS: Dict[str, Any] = {
    "sqlite": sqlite,
    "duckdb": duckdb,
    "postgresql": postgresql,
    "mysql": mysql,
    "oracle": oracle,
    "mssql": sqlserver,
    "access": access,
    "firebird": firebird,
}


def create_connection_string(db_type: str, **kwargs: Any) -> str:
    """Create a database connection string (DSN) based on the database
    type and parameters.

    Args:
        db_type (str): The type of the database. One of:
            "sqlite", "duckdb", "postgresql", "mysql", "oracle",
            "mssql" (alias "sqlserver"),
            "access", "firebird".
        For the following builders the accepted keyword arguments are:

        sqlite / duckdb:
            path (Optional[str]): Path to the database file.
            If omitted, an in-memory DB is used.

        postgresql / mysql / oracle:
            host (Optional[str]): Database host (defaults provided in DEFAULTS)
            port (Optional[int]): Database port (defaults provided in DEFAULTS)
            user (Optional[str]): Username
            can also be read from env vars:
            - POSTGRES_USER,
            - MYSQL_USER,
            - ORACLE_USER
            password (Optional[str]): Password
            can also be read from env vars:
             - POSTGRES_PASSWORD,
             - MYSQL_PASSWORD,
             - ORACLE_PASSWORD
            database (Optional[str]): Database name.

        mssql / sqlserver:
            host (str): Server host.
            port (int): Server port.
            user (str): Username.
            password (str): Password.
            database (str): Database name.
            driver (str): ODBC driver name
            (default "ODBC Driver 18 for SQL Server").
            options (Optional[Dict[str, str]]): Additional key/value options
            appended to the ODBC connection string.

        access:
            path (str): Path to the .mdb/.accdb file.
            driver (str): ODBC driver name
            (default "Microsoft Access Driver (*.mdb, *.accdb)").

        firebird:
            path (str): Path to the database file.
            user (str): Username (default "sysdba").
            password (str): Password (default "masterkey").
            driver (str): ODBC driver name (default "Firebird").

        Note: Parameters are forwarded to the corresponding builder function
        for the chosen db_type.

    Raises:
        ValueError: If the database type is not supported.

    Returns:
        str: The database connection string (DSN).
    """
    db_type = db_type.lower()
    if db_type not in DB_BUILDERS:
        raise ValueError(f"Database type '{db_type}' not supported.")
    return DB_BUILDERS[db_type](**kwargs)
