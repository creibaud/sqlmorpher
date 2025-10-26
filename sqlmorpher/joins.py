import re
from typing import List, Dict, Optional, Tuple, Any, Set
from sqlalchemy import inspect, Table, MetaData, select, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.sqltypes import Integer, String, Date, Boolean
from pydantic import validate_call
from .db import Database


def _get_column_type(table: Table, column: str) -> Optional[str]:
    col = table.c.get(column)
    if col is None:
        return None
    if isinstance(col.type, Integer):
        return "int"
    if isinstance(col.type, String):
        return "str"
    if isinstance(col.type, Date):
        return "date"
    if isinstance(col.type, Boolean):
        return "bool"
    return str(col.type)


def _parse_on_clause(on: str) -> Optional[Tuple[str, str, str, str]]:
    m = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", on)
    if not m:
        return None
    return (m.group(1), m.group(2), m.group(3), m.group(4))


def _load_table(metadata: MetaData, engine: Engine, table_name: str) -> Table:
    return Table(table_name, metadata, autoload_with=engine)


def _ensure_columns_exist(table: Table, columns: List[str]) -> None:
    for col in columns:
        if col not in table.c:
            raise ValueError(
                f"Column '{col}' does not exist in table '{table.name}'."
            )


def _ensure_referenced_tables(
    added_tables: Set[str], referenced: Tuple[str, str], current_table: str
) -> None:
    for t in referenced:
        if t not in added_tables and t != current_table:
            raise ValueError(f"Table '{t}' referenced before being joined.")


def _ensure_table_and_load(
    name: str, metadata: MetaData, db: Database, tables: Set[str]
) -> Table:
    if name not in tables:
        raise ValueError(f"Table '{name}' does not exist.")
    return _load_table(metadata, db.engine, name)


def _parse_on_or_raise(on: str) -> Tuple[str, str, str, str]:
    parsed = _parse_on_clause(on)
    if not parsed:
        raise ValueError(f"Invalid ON clause: {on}")
    return parsed


@validate_call(config={"arbitrary_types_allowed": True})
def validate_joins(
    db: Database, root_table_name: str, join_tables: List[Dict[str, str]]
) -> Tuple[Any, Dict[str, Table]]:
    """Validate join configurations for SQL queries.

    Args:
        db (Database): The database instance.
        root_table_name (str): The name of the root table.
        join_tables (List[Dict[str, str]]): A list of join specifications.

    Returns:
        Tuple containing:
        - from_clause: The SQLAlchemy from clause with all joins
        - table_map: Dictionary mapping table names to Table objects
    """
    inspector = inspect(db.engine)
    tables = set(inspector.get_table_names())
    if root_table_name not in tables:
        raise ValueError(f"Root table '{root_table_name}' does not exist.")

    metadata = MetaData()
    added_tables = {root_table_name}
    table_map: Dict[str, Table] = {}

    root_table = _load_table(metadata, db.engine, root_table_name)
    table_map[root_table_name] = root_table
    from_clause: Any = root_table

    for join_info in join_tables:
        table_name = join_info.get("table")
        on_clause = join_info.get("on_clause")
        join_type = join_info.get("type", "INNER").upper()

        if not table_name or not on_clause:
            raise ValueError("Each join must have 'table' and 'on' keys.")

        _ensure_table_and_load(table_name, metadata, db, tables)

        left_table_name, left_col_name, right_table_name, right_col_name = (
            _parse_on_or_raise(on_clause)
        )

        _ensure_referenced_tables(
            added_tables, (left_table_name, right_table_name), table_name
        )

        left_table = _ensure_table_and_load(
            left_table_name, metadata, db, tables
        )
        right_table = _ensure_table_and_load(
            right_table_name, metadata, db, tables
        )

        table_map[left_table_name] = left_table
        table_map[right_table_name] = right_table

        _ensure_columns_exist(left_table, [left_col_name])
        _ensure_columns_exist(right_table, [right_col_name])

        left_type = _get_column_type(left_table, left_col_name)
        right_type = _get_column_type(right_table, right_col_name)
        if left_type != right_type:
            raise ValueError(
                "Type mismatch between columns"
                f" '{left_table_name}.{left_col_name}' ({left_type}) "
                f"and '{right_table_name}.{right_col_name}' ({right_type})"
            )

        left_col = left_table.c[left_col_name]
        right_col = right_table.c[right_col_name]
        is_outer = join_type.startswith("LEFT")

        from_clause = from_clause.join(
            right_table, left_col == right_col, isouter=is_outer
        )
        added_tables.add(table_name)

    first_col = list(root_table.c)[0]
    stmt = select(first_col).select_from(from_clause).limit(1)
    sql_str = str(
        stmt.compile(db.engine, compile_kwargs={"literal_binds": True})
    )

    try:
        db.execute_query(sql_str)
    except SQLAlchemyError as e:
        raise ValueError(f"Join validation failed: {e}")

    return from_clause, table_map


@validate_call(config={"arbitrary_types_allowed": True})
def generate_join_query(
    db: Database,
    root_table_name: str,
    join_tables: List[Dict[str, Any]],
    select_columns: Dict[str, str],
) -> str:
    """
    Generate a SQL query with joins based on the provided root table and
    join specifications.

    Args:
        db (Database): The database instance.
        root_table_name (str): The name of the root table.
        join_tables (List[Dict[str, str]]): A list of join specifications.
        select_columns (Dict[str, str]): A dictionary mapping source columns
        to aliases.

    Returns:
        str: The generated SQL query with joins.
    """
    from_clause, table_map = validate_joins(db, root_table_name, join_tables)

    columns = [
        table_map[table_col.split(".")[0]]
        .c[table_col.split(".")[1]]
        .label(alias)
        for table_col, alias in select_columns.items()
    ]

    stmt = select(*columns).select_from(from_clause)
    sql_str = str(
        stmt.compile(db.engine, compile_kwargs={"literal_binds": True})
    )
    return sql_str
