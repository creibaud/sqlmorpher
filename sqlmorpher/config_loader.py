import os
import yaml
from .db import Database
from pydantic import validate_call
from typing import Dict, List
from .connection_string import create_connection_string


@validate_call
def load_config(path: str) -> dict:
    """
    Load a YAML configuration file and return its contents as a dictionary.
    Args:
        path (str): The file path to the YAML configuration file.
    Returns:
        dict: The contents of the YAML file as a dictionary.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


@validate_call
def load_migration_config(path: str) -> List:
    """
    Load a migration configuration from a YAML file.
    Args:
        path (str): The file path to the migration configuration YAML file.
    Returns:
        List: A list of migration configurations.
    """
    return load_config(path).get("migrations", [])


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
    databases = {}
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
