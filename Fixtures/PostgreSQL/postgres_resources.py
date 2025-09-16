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
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=database,
            sslmode="require",
        )

    def setup_resource(
        self, resource_config: Optional[PostgreSQLResourceConfig] = None
    ) -> PostgreSQLResourceData:
        """Set up PostgreSQL databases with optional SQL schema loading"""
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
        system_connection = self.get_connection("postgres")
        system_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        system_cursor = system_connection.cursor()

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
            type="postgresql_resource",
            creation_time=creation_start,
            creation_duration=creation_duration,
            description=f"PostgreSQL resource for {resource_id}",
            status="active",
            created_resources=created_resources,
            connection_params={
                "host": os.getenv("POSTGRES_HOSTNAME"),
                "port": os.getenv("POSTGRES_PORT"),
                "user": os.getenv("POSTGRES_USERNAME"),
                "password": os.getenv("POSTGRES_PASSWORD"),
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
            # Look for SQL file relative to the test directory
            # For now, use current working directory as fallback
            sql_file_path = Path(sql_file)
            if not sql_file_path.exists():
                # Try to find it in test directories
                for test_dir in Path("Tests").glob("**/"):
                    potential_path = test_dir / sql_file
                    if potential_path.exists():
                        sql_file = str(potential_path)
                        break
                else:
                    raise FileNotFoundError(f"SQL file not found: {sql_file}")

        if not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        print(f"📄 Loading SQL file {sql_file} into database {db_name}")

        # Set up environment for psql command
        env = os.environ.copy()
        env["PGPASSWORD"] = os.getenv("POSTGRES_PASSWORD")

        # Run psql to load the SQL file
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
            sql_file,
            "--quiet",
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

    def teardown_resource(self, resource_data: PostgreSQLResourceData) -> None:
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
        return "postgresql_resource"

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
        )
