# SQL Morpher

SQL Morpher is a tool designed to facilitate database schema migrations and data transformations. It allows users to define transformation functions and apply them during the migration process, ensuring that data integrity is maintained while adapting to new schema requirements.

## Features

- **Schema Migration**: Easily migrate your database schema between different database systems.
- **Data Transformation**: Define custom transformation functions to modify data during the migration process.
- **Database Support**: Works with various database systems, including PostgreSQL, MySQL, SQLite, and more.
- **Configuration Management**: Load and manage migration configurations using YAML files.
- **Join Handling**: Support for complex joins during data migration.
- **Comprehensive Testing**: Includes a suite of tests to ensure reliability and correctness.

## Installation

To install SQL Morpher, you can use pip:

```bash
pip install sqlmorpher
```

## Usage

To use SQL Morpher, you need to create a configuration file (e.g., `config.yaml`) that defines your source and target databases, as well as the migration steps. You can then run the migration using the following code snippet:

```python
from sqlmorpher import load_db_config, load_migration_config, migrate
from dotenv import load_dotenv
from your_registry_module import transform_registry

load_dotenv()
config_path = "config.yaml"
databases = load_db_config(config_path)
migrations = load_migration_config(config_path)

migrate(databases.get("source"), databases.get("target"), migrations, transform_registry)
```

## Migration Configuration

The migration configuration file should define the source and target databases, as well as the specific migration steps to be performed. Here is an example of a migration configuration:

```yaml
databases:
    source:
        type: "postgresql"
        host: "localhost"
        port: 5432
        user: "test_user"
        password_env_var: "DB_SOURCE_PASSWORD"
        database: "source_db"

    target:
        type: "postgresql"
        host: "localhost"
        port: 5432
        user: "test_user"
        password_env_var: "DB_TARGET_PASSWORD"
        database: "target_db"

migrations:
    - name: "Migrate users with profiles and countries"
      root_table: "users"
      target_table: "new_users"
      joins:
          - table: "profiles"
            on_clause: "users.id = profiles.user_id"
            type: "LEFT"
          - table: "countries"
            on_clause: "profiles.country_id = countries.id"
            type: "LEFT"
      columns:
          "users.id": "id"
          "users.username": "username"
          "users.email": "email"
          "profiles.phone": "phone"
          "countries.name": "country"
        transform_function: "transform_user_data"
```
