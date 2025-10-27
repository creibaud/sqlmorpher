from .db import Database
from .joins import generate_join_query
from sqlalchemy.engine import RowMapping
from pydantic import validate_call
from typing import Dict, List, Callable, Any, Optional, Sequence, Mapping
from tqdm.rich import tqdm
import warnings

TransformFn = Callable[[Database, Mapping[str, Any]], None]

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


def _lookup_insert_fn(
    transform_registry: Dict[str, TransformFn], insert_fn_name: str
) -> Optional[TransformFn]:
    if not insert_fn_name:
        return None
    insert_fn = transform_registry.get(insert_fn_name)
    if insert_fn is None:
        raise ValueError(
            f"Transform function '{insert_fn_name}' not found in registry. "
            f"Available: {list(transform_registry.keys())}"
        )
    return insert_fn


def row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    try:
        return dict(row)
    except Exception:
        return {k: getattr(row, k) for k in dir(row) if not k.startswith("_")}


def _process_rows(
    new_db: Database,
    target_table: str,
    rows: Sequence[RowMapping],
    insert_fn: Optional[TransformFn] = None,
) -> None:
    for row in rows:
        row_dict: Dict[str, Any] = row_to_dict(row)

        if insert_fn and callable(insert_fn):
            insert_fn(new_db, row_dict)
            continue

        new_db.insert_row(target_table, row_dict)


@validate_call(config={"arbitrary_types_allowed": True})
def migrate(
    old_db: Database,
    new_db: Database,
    mapping: List[Dict[str, Any]],
    transform_registry: Optional[Dict[str, TransformFn]] = None,
) -> str:
    """Migrate data from old_db to new_db based on the provided mapping.

    Args:
        old_db (Database): The source database to migrate data from.
        new_db (Database): The target database to migrate data to.
        mapping (List[Dict]): The mapping configuration for migration.
        transform_registry (Dict[str, Callable], optional): A registry of
        transformation functions. Defaults to None.

    Returns:
        str: A message indicating the result of the migration.
    """
    if transform_registry is None:
        transform_registry = {}

    for map_entry in tqdm(mapping, desc="Migrating tables", unit="table"):
        root_table = map_entry["root_table"]
        joins = map_entry.get("joins", [])
        columns = map_entry.get("columns", {})
        target_table = map_entry.get("target_table", root_table)
        insert_fn_name = map_entry.get("insert_function", "")

        insert_fn = _lookup_insert_fn(transform_registry, insert_fn_name)
        join_query = generate_join_query(old_db, root_table, joins, columns)
        rows = old_db.execute_query(join_query) or []

        _process_rows(new_db, target_table, rows, insert_fn)

    return "Migration completed successfully"
