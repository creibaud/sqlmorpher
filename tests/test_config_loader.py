import pytest
from _pytest.monkeypatch import MonkeyPatch
import tempfile
import yaml
import os
from sqlmorpher import (
    load_db_config,
    load_migration_config,
    Database,
)
from typing import Dict, Any, List


@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("DB_SOURCE_PASSWORD", "secret1")
    monkeypatch.setenv("DB_TARGET_PASSWORD", "secret2")
    monkeypatch.setenv("DB_TARGET_PASSWORD", "secret2")


@pytest.fixture
def migration_yaml_file() -> Any:
    content: Dict[str, List[Dict[str, Any]]] = {
        "migrations": [
            {
                "name": "Test migration",
                "root_table": "users",
                "joins": [],
                "columns": {"users.id": "id"},
                "target_table": "new_users",
            }
        ]
    }
    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f:
        yaml.dump(content, f)
        f.flush()
        yield f.name
        os.remove(f.name)


@pytest.fixture
def db_yaml_file() -> Any:
    content = {
        "databases": {
            "source": {
                "type": "sqlite",
            },
            "target": {
                "type": "sqlite",
            },
        }
    }
    with tempfile.NamedTemporaryFile("w+", suffix=".yaml", delete=False) as f:
        yaml.dump(content, f)
        f.flush()
        yield f.name
        os.remove(f.name)


def test_load_config_and_migration(migration_yaml_file: str) -> None:
    migration_conf = load_migration_config(migration_yaml_file)
    assert len(migration_conf) == 1
    assert migration_conf[0]["root_table"] == "users"


def test_load_db_config(db_yaml_file: str) -> None:
    databases = load_db_config(db_yaml_file)
    assert isinstance(databases["source"], Database)
    assert isinstance(databases["target"], Database)
    assert databases["source"].type == "sqlite"
    assert databases["target"].type == "sqlite"

    # Test que la connexion fonctionne
    assert databases["source"].validate_connection() is True
    assert databases["target"].validate_connection() is True
