import os
import yaml
from .db import Database
from pydantic import validate_call
from typing import Dict, List, Any, cast
from .connection_string import create_connection_string


@validate_call
def load_config(path: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file and return its contents as a dictionary.
    Args:
        path (str): The file path to the YAML configuration file.
    Returns:
        dict: The contents of the YAML file as a dictionary.
    """
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError(
            "Configuration file does not contain a mapping at top level: "
            + path
        )
    return cast(Dict[str, Any], data)


@validate_call
def load_migration_config(path: str) -> List[Dict[str, Any]]:
    """
    Load a migration configuration from a YAML file.
    Args:
        path (str): The file path to the migration configuration YAML file.
    Returns:
        List: A list of migration configurations.
    """
    cfg = load_config(path)
    migrations_raw = cfg.get("migrations", [])
    if migrations_raw is None:
        return []
    if not isinstance(migrations_raw, list):
        raise ValueError("'migrations' should be a list in the config file")

    migrations: List[Any] = migrations_raw
    for idx, item in enumerate(migrations):
        if not isinstance(item, dict):
            raise ValueError(
                "Migration entry at index " + str(idx) + " is not a mapping"
            )
    return cast(List[Dict[str, Any]], migrations)


@validate_call
def load_db_config(path: str) -> Dict[str, Database]:
    """
    Load database configurations and create Database instances.
    Args:
        path (str): The file path to the database configuration YAML file.
    Returns:
        Dict[str, Database]: A dictionary with 'source' and 'target'
        Database instances.
    """
    db_configs = load_config(path).get("databases", {})
    databases: Dict[str, Database] = {}
    for key in ["source", "target"]:
        conf = db_configs.get(key)
        if not conf:
            raise ValueError(f"Database configuration for '{key}' is missing.")
        password_var = conf.pop("password_env_var", None)
        if password_var:
            password = os.getenv(password_var, "")
            if password:
                conf["password"] = password
        db_type = conf.pop("type")
        conn_str = create_connection_string(db_type, **conf)
        databases[key] = Database(type=db_type, connection_string=conn_str)
    return databases
