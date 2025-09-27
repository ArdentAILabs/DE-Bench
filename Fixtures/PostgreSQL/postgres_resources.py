"""
PostgreSQL fixture using DEBenchFixture pattern.
Handles PostgreSQL database creation and management with SQL file loading.
"""

import os
import time
import psycopg2
import subprocess
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Dict, Any, Optional, List
from typing_extensions import TypedDict
from pathlib import Path

from Fixtures.base_fixture import DEBenchFixture


class PostgreSQLDatabaseConfig(TypedDict):
    name: str
    sql_file: Optional[str]


class PostgreSQLResourceConfig(TypedDict):
    resource_id: str
    databases: List[PostgreSQLDatabaseConfig]
    test_module_path: Optional[str]
    load_bulk: Optional[bool]


class PostgreSQLDatabaseData(TypedDict):
    name: str
    tables: List[str]
    type: str


class PostgreSQLResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    created_resources: List[PostgreSQLDatabaseData]
    connection_params: Dict[str, str]


class PostgreSQLFixture(
    DEBenchFixture[PostgreSQLResourceConfig, PostgreSQLResourceData, Dict[str, Any]]
):
    """
    PostgreSQL fixture implementation using DEBenchFixture pattern.

    Supports creating multiple databases with optional SQL schema loading.
    Supports loading bulk tables from bulk_tables.sql before loading individual SQL files.
    """

    @classmethod
    def requires_session_setup(cls) -> bool:
        """PostgreSQL doesn't require session-level setup"""
        return False

    def session_setup(
        self, session_config: Optional[PostgreSQLResourceConfig] = None
    ) -> Dict[str, Any]:
        """No session setup needed for PostgreSQL"""
        return {}

    def session_teardown(self, session_data: Optional[Dict[str, Any]] = None) -> None:
        """No session teardown needed for PostgreSQL"""
        pass

    def get_connection(
        self, database: str = "postgres"
    ) -> psycopg2.extensions.connection:
        """
        Get a PostgreSQL connection to the specified database.

        Args:
            database: Database name to connect to (defaults to 'postgres')

        Returns:
            psycopg2 connection object
        """
        return psycopg2.connect(
            host=self.postgres_hostname,
            port=self.postgres_port,
            user=self.postgres_username,
            password=self.postgres_password,
            database=database,
            sslmode="require",
        )

    def test_setup(
        self, resource_config: Optional[PostgreSQLResourceConfig] = None
    ) -> PostgreSQLResourceData:
        """Set up PostgreSQL databases with optional SQL schema loading"""
        self.postgres_hostname = os.getenv("POSTGRES_HOSTNAME")
        self.postgres_port = os.getenv("POSTGRES_PORT")
        self.postgres_username = os.getenv("POSTGRES_USERNAME")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD")

        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]
        print(f"🐘 Setting up PostgreSQL resource: {resource_id}")

        creation_start = time.time()
        created_resources = []

        # Get system connection for database operations
        print("🐘 Getting PostgreSQL connection...")
        system_connection = self.get_connection()
        print("✅ PostgreSQL connection obtained")
        system_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        system_cursor = system_connection.cursor()

        # Sanity check to make sure the database is connected
        print("🐘 Sanity checking PostgreSQL connection...")
        system_cursor.execute(
            """
            SELECT 1
            """
        )
        print("✅ PostgreSQL connection sanity check passed")

        try:
            # Create each database specified in the config
            for db_config in config["databases"]:
                db_name = db_config["name"]

                # Terminate existing connections to the database
                try:
                    system_cursor.execute(
                        """
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = %s AND pid <> pg_backend_pid()
                        """,
                        (db_name,),
                    )
                except Exception as e:
                    print(
                        f"⚠️ Warning - could not terminate connections to {db_name}: {e}"
                    )

                # Drop and recreate database
                system_cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                system_cursor.execute(f"CREATE DATABASE {db_name}")
                print(f"✅ Created PostgreSQL database: {db_name}")

                # Track created database
                db_resource = PostgreSQLDatabaseData(
                    name=db_name, tables=[], type="database"
                )
                created_resources.append(db_resource)

                # Load bulk tables if requested
                if config.get("load_bulk", False):
                    self._load_bulk_tables(db_name, db_resource)

                # Load SQL file if specified
                if sql_file := db_config.get("sql_file"):
                    self._load_sql_file(db_name, sql_file, db_resource)

        except Exception as e:
            # Clean up on failure
            print(f"❌ Failed to create PostgreSQL resource {resource_id}: {e}")
            self._cleanup_databases(system_cursor, created_resources)
            raise
        finally:
            system_cursor.close()
            system_connection.close()

        creation_end = time.time()
        creation_duration = creation_end - creation_start

        # Store resource data on the fixture instance for later access
        resource_data = PostgreSQLResourceData(
            resource_id=resource_id,
            type="postgres_resource",
            creation_time=creation_start,
            creation_duration=creation_duration,
            description=f"PostgreSQL resource for {resource_id}",
            status="active",
            created_resources=created_resources,
            connection_params={
                "host": self.postgres_hostname,
                "port": self.postgres_port,
                "user": self.postgres_username,
                "password": self.postgres_password,
                "sslmode": "require",
            },
        )

        # Store for later access during validation
        self._resource_data = resource_data

        print(f"✅ PostgreSQL resource {resource_id} ready! ({creation_duration:.2f}s)")
        return resource_data

    def _load_sql_file(
        self, db_name: str, sql_file: str, db_resource: PostgreSQLDatabaseData
    ) -> None:
        """Load SQL file into the specified database"""
        # Resolve SQL file path
        if not os.path.isabs(sql_file):
            # First, try to get the test module path from custom config
            if (
                hasattr(self, "custom_config")
                and self.custom_config
                and "test_module_path" in self.custom_config
            ):
                test_module_path = self.custom_config["test_module_path"]
                test_dir = os.path.dirname(test_module_path)
                sql_file_path = os.path.join(test_dir, sql_file)
                if os.path.exists(sql_file_path):
                    sql_file = sql_file_path
                else:
                    raise FileNotFoundError(
                        f"SQL file not found in test directory: {sql_file_path}"
                    )
            else:
                # Fallback: Look for SQL file relative to current working directory first
                sql_file_path = Path(sql_file)
                if not sql_file_path.exists():
                    # Try to find it in test directories using absolute path
                    # Get the absolute path to the project root
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.dirname(
                        os.path.dirname(current_dir)
                    )  # Go up two levels from Fixtures/PostgreSQL/
                    tests_dir = os.path.join(project_root, "Tests")

                    if os.path.exists(tests_dir):
                        for test_dir in Path(tests_dir).glob("**/"):
                            potential_path = test_dir / sql_file
                            if potential_path.exists():
                                sql_file = str(potential_path)
                                break
                        else:
                            raise FileNotFoundError(f"SQL file not found: {sql_file}")
                    else:
                        raise FileNotFoundError(
                            f"SQL file not found: {sql_file} (Tests directory not found at {tests_dir})"
                        )

        if not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        print(f"📄 Loading SQL file {sql_file} into database {db_name}")

        # Set up environment for psql command
        env = os.environ.copy()
        env["PGPASSWORD"] = self.postgres_password

        # Run psql to load the SQL file
        cmd = [
            "psql",
            "-h",
            self.postgres_hostname,
            "-p",
            self.postgres_port,
            "-U",
            self.postgres_username,
            "-d",
            db_name,
            "-f",
            sql_file,
            "--quiet",
            "--set=sslmode=require",
        ]

        try:
            subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            print(f"✅ Successfully loaded SQL file into {db_name}")

            # Get table list
            db_connection = self.get_connection(db_name)
            db_cursor = db_connection.cursor()

            try:
                db_cursor.execute(
                    """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                    """
                )
                tables = [row[0] for row in db_cursor.fetchall()]
                db_resource["tables"] = tables
                print(f"📊 Loaded {len(tables)} tables from SQL file into {db_name}")

            finally:
                db_cursor.close()
                db_connection.close()

        except subprocess.CalledProcessError as e:
            print(f"❌ Error loading SQL file: {e}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            raise

    def _load_bulk_tables(
        self, db_name: str, db_resource: PostgreSQLDatabaseData
    ) -> None:
        """Load bulk tables from bulk_tables.sql into the specified database"""
        # Get the path to bulk_tables.sql in the same directory as this file
        bulk_sql_path = os.path.join(os.path.dirname(__file__), "bulk_tables.sql")
        
        if not os.path.exists(bulk_sql_path):
            print(f"⚠️ Warning - bulk_tables.sql not found at {bulk_sql_path}")
            return

        print(f"📄 Loading bulk tables from {bulk_sql_path} into database {db_name}")

        # Set up environment for psql command
        env = os.environ.copy()
        env["PGPASSWORD"] = os.getenv("POSTGRES_PASSWORD")

        # Run psql to load the bulk tables file
        cmd = [
            "psql",
            "-h",
            os.getenv("POSTGRES_HOSTNAME"),
            "-p",
            os.getenv("POSTGRES_PORT"),
            "-U",
            os.getenv("POSTGRES_USERNAME"),
            "-d",
            db_name,
            "-f",
            bulk_sql_path,
            "--quiet",
            "--set=sslmode=require",
        ]

        try:
            subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            print(f"✅ Successfully loaded bulk tables into {db_name}")

            # Update table list to include bulk tables
            db_connection = self.get_connection(db_name)
            db_cursor = db_connection.cursor()

            try:
                db_cursor.execute(
                    """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                    """
                )
                tables = [row[0] for row in db_cursor.fetchall()]
                db_resource["tables"] = tables
                print(f"📊 Loaded {len(tables)} tables from bulk_tables.sql into {db_name}")

            finally:
                db_cursor.close()
                db_connection.close()

        except subprocess.CalledProcessError as e:
            print(f"❌ Error loading bulk tables: {e}")
            print(f"STDOUT: {e.stdout}")
            print(f"STDERR: {e.stderr}")
            raise

    def _cleanup_databases(
        self, cursor, created_resources: List[PostgreSQLDatabaseData]
    ) -> None:
        """Clean up created databases"""
        for resource in reversed(created_resources):
            if resource["type"] == "database":
                db_name = resource["name"]
                try:
                    # Terminate connections
                    cursor.execute(
                        """
                        SELECT pg_terminate_backend(pid) 
                        FROM pg_stat_activity 
                        WHERE datname = %s AND pid <> pg_backend_pid()
                        """,
                        (db_name,),
                    )
                except Exception as e:
                    print(
                        f"⚠️ Warning during cleanup - could not terminate connections to {db_name}: {e}"
                    )

                cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                print(f"🗑️ Dropped database {db_name}")

    def test_teardown(self, resource_data: PostgreSQLResourceData) -> None:
        """Clean up PostgreSQL databases"""
        resource_id = resource_data["resource_id"]
        created_resources = resource_data["created_resources"]

        print(f"🧹 Cleaning up PostgreSQL resource: {resource_id}")

        try:
            # Get cleanup connection
            cleanup_connection = self.get_connection("postgres")
            cleanup_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cleanup_cursor = cleanup_connection.cursor()

            try:
                self._cleanup_databases(cleanup_cursor, created_resources)
            finally:
                cleanup_cursor.close()
                cleanup_connection.close()

            print(f"✅ PostgreSQL resource {resource_id} cleaned up successfully")

        except Exception as e:
            print(f"❌ Error cleaning up PostgreSQL resource {resource_id}: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "postgres_resource"

    @classmethod
    def get_default_config(cls) -> PostgreSQLResourceConfig:
        """Return default configuration for PostgreSQL resources"""
        timestamp = int(time.time())

        return PostgreSQLResourceConfig(
            resource_id=f"postgres_test_{timestamp}",
            databases=[
                PostgreSQLDatabaseConfig(
                    name=f"test_database_{timestamp}", sql_file=None
                )
            ],
            load_bulk=False,
        )

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create PostgreSQL config section using the fixture's resource data.

        Returns:
            Dictionary containing the postgreSQL service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "PostgreSQL resource data not available - ensure test_setup was called"
            )

        # Extract connection details and created databases
        connection_params = resource_data["connection_params"]
        created_resources = resource_data["created_resources"]

        return {
            "postgreSQL": {
                "hostname": connection_params["host"],
                "port": connection_params["port"],
                "username": connection_params["user"],
                "password": connection_params["password"],
                "databases": [{"name": db["name"]} for db in created_resources],
            }
        }
