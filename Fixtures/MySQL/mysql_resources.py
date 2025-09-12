import pytest
import json
import time
import os
import mysql.connector
from typing import Dict, List, Any, Optional
from typing_extensions import TypedDict
from Fixtures.base_fixture import DEBenchFixture


# Type definitions for MySQL resources
class MySQLColumnConfig(TypedDict):
    name: str
    type: str
    primary_key: Optional[bool]
    not_null: Optional[bool]
    unique: Optional[bool]
    default: Optional[str]


class MySQLTableConfig(TypedDict):
    name: str
    columns: List[MySQLColumnConfig]
    data: Optional[List[Dict[str, Any]]]


class MySQLDatabaseConfig(TypedDict):
    name: str
    tables: List[MySQLTableConfig]


class MySQLResourceConfig(TypedDict):
    resource_id: str
    databases: List[MySQLDatabaseConfig]


class MySQLResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    created_resources: List[Dict[str, Any]]


class MySQLFixture(DEBenchFixture[MySQLResourceConfig, MySQLResourceData]):
    """MySQL fixture implementation following the DEBenchFixture interface"""

    def get_connection(self, database: Optional[str] = None):
        """Get a MySQL database connection. Useful for validation and testing."""
        connection_params = {
            "host": os.getenv("MYSQL_HOST"),
            "port": os.getenv("MYSQL_PORT"),
            "user": os.getenv("MYSQL_USERNAME"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "connect_timeout": 10,
        }

        if database:
            connection_params["database"] = database

        return mysql.connector.connect(**connection_params)

    def setup_resource(
        self, resource_config: Optional[MySQLResourceConfig] = None
    ) -> MySQLResourceData:
        """
        Set up MySQL resource based on configuration.
        Creates databases, tables, and populates data according to the config.
        """
        # Determine which config to use (priority: resource_config > custom_config > default_config)
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        print(
            f"Setting up MySQL resource with config: {config.get('resource_id', 'default')}"
        )
        creation_start = time.time()

        created_resources = []

        # Connect to MySQL (single connection for everything)
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USERNAME"),
            password=os.getenv("MYSQL_PASSWORD"),
            connect_timeout=10,
        )
        cursor = connection.cursor()

        try:
            # Process databases from template
            if "databases" in config:
                for db_config in config["databases"]:
                    db_name = db_config["name"]

                    # Drop and create database
                    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    cursor.execute(f"CREATE DATABASE {db_name}")
                    print(f"Created MySQL database {db_name}")

                    created_resources.append(
                        {"type": "database", "name": db_name, "tables": []}
                    )
                    db_resource = created_resources[-1]

                    # Switch to the new database
                    cursor.execute(f"USE {db_name}")

                    # Process tables in this database
                    if "tables" in db_config:
                        for table_config in db_config["tables"]:
                            table_name = table_config["name"]

                            # Generate and execute CREATE TABLE from JSON columns
                            if "columns" in table_config:
                                column_definitions = []
                                for col in table_config["columns"]:
                                    col_def = f"{col['name']} {col['type']}"

                                    if col.get("primary_key"):
                                        col_def += " PRIMARY KEY"
                                    if col.get("not_null"):
                                        col_def += " NOT NULL"
                                    if col.get("unique"):
                                        col_def += " UNIQUE"
                                    if col.get("default"):
                                        col_def += f" DEFAULT {col['default']}"

                                    column_definitions.append(col_def)

                                create_table_sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
                                cursor.execute(create_table_sql)
                                print(f"Created MySQL table {table_name} in {db_name}")
                                db_resource["tables"].append(table_name)

                                # Insert data if provided
                                if "data" in table_config and table_config["data"]:
                                    for record in table_config["data"]:
                                        columns = list(record.keys())
                                        values = list(record.values())
                                        placeholders = ", ".join(["%s"] * len(values))
                                        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                                        cursor.execute(insert_sql, values)

                                    connection.commit()
                                    print(
                                        f"Inserted {len(table_config['data'])} records into {table_name}"
                                    )

        finally:
            cursor.close()
            connection.close()

        creation_end = time.time()
        print(f"MySQL resource creation took {creation_end - creation_start:.2f}s")

        resource_id = config.get("resource_id", f"mysql_resource_{int(time.time())}")

        # Create detailed resource data
        resource_data = {
            "resource_id": resource_id,
            "type": "mysql_resource",
            "creation_time": time.time(),
            "creation_duration": creation_end - creation_start,
            "description": f"A MySQL resource",
            "status": "active",
            "created_resources": created_resources,
        }

        print(f"MySQL resource {resource_id} created successfully")
        return resource_data

    def teardown_resource(self, resource_data: MySQLResourceData) -> None:
        """Clean up MySQL resource"""
        resource_id = resource_data.get("resource_id", "unknown")
        print(f"Cleaning up MySQL resource {resource_id}")

        try:
            # Connect for cleanup
            cleanup_connection = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST"),
                port=os.getenv("MYSQL_PORT"),
                user=os.getenv("MYSQL_USERNAME"),
                password=os.getenv("MYSQL_PASSWORD"),
                connect_timeout=10,
            )
            cleanup_cursor = cleanup_connection.cursor()

            # Clean up created databases in reverse order
            created_resources = resource_data.get("created_resources", [])
            for resource in reversed(created_resources):
                if resource["type"] == "database":
                    db_name = resource["name"]
                    cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    print(f"Dropped MySQL database {db_name}")

            cleanup_connection.commit()
            cleanup_cursor.close()
            cleanup_connection.close()
            print(f"MySQL resource {resource_id} cleaned up successfully")

        except Exception as e:
            print(f"Error cleaning up MySQL resource: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "mysql_resource"

    @classmethod
    def get_default_config(cls) -> MySQLResourceConfig:
        """Return a default MySQL configuration"""
        return {
            "resource_id": "test_mysql_resource",
            "databases": [
                {
                    "name": "test_database",
                    "tables": [
                        {
                            "name": "test_table",
                            "columns": [
                                {
                                    "name": "id",
                                    "type": "INT AUTO_INCREMENT",
                                    "primary_key": True,
                                },
                                {
                                    "name": "name",
                                    "type": "VARCHAR(100)",
                                    "not_null": True,
                                },
                            ],
                            "data": [],
                        }
                    ],
                }
            ],
        }


# Global fixture instance - this is the only way to access MySQL resources now
mysql_fixture = MySQLFixture()


@pytest.fixture(scope="function")
def mysql_resource(request):
    """
    A function-scoped fixture that creates MySQL resources based on template.
    Uses the DEBenchFixture implementation.
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting mysql_resource for {test_name}")

    build_template = request.param

    # Use the fixture class
    resource_data = mysql_fixture.setup_resource(build_template)

    # Add test-specific metadata
    resource_data.update(
        {
            "test_name": test_name,
            "worker_pid": os.getpid(),
        }
    )

    fixture_end_time = time.time()
    print(
        f"Worker {os.getpid()}: MySQL fixture setup took {fixture_end_time - start_time:.2f}s total"
    )

    yield resource_data

    # Use the fixture class for teardown
    mysql_fixture.teardown_resource(resource_data)
