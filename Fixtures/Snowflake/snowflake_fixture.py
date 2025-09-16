"""
Snowflake fixture using DEBenchFixture pattern.
Handles Snowflake database and schema creation with SQL file loading and S3 integration.
"""

import os
import re
import time
import uuid
from typing import Dict, Any, Optional, List
from typing_extensions import TypedDict
from pathlib import Path

import snowflake.connector

from Fixtures.base_fixture import DEBenchFixture


class SnowflakeS3Config(TypedDict, total=False):
    bucket_url: str
    s3_key: str
    aws_key_id: str
    aws_secret_key: str


class SnowflakeResourceConfig(TypedDict):
    resource_id: str
    database: Optional[str]
    schema: Optional[str]
    sql_file: Optional[str]
    s3_config: Optional[SnowflakeS3Config]


class SnowflakeResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    database: str
    schema: str
    connection: Any  # snowflake.connector.SnowflakeConnection
    created_resources: List[Dict[str, Any]]


class SnowflakeFixture(
    DEBenchFixture[SnowflakeResourceConfig, SnowflakeResourceData, Dict[str, Any]]
):
    """
    Snowflake fixture implementation using DEBenchFixture pattern.

    Features:
    - Database and schema creation with unique naming
    - SQL file support with variable substitution
    - S3 integration for data loading
    - Multi-statement SQL execution
    - Automatic cleanup of created resources
    """

    @classmethod
    def requires_session_setup(cls) -> bool:
        """Snowflake doesn't require session-level setup"""
        return False

    def session_setup(
        self, session_config: Optional[SnowflakeResourceConfig] = None
    ) -> Dict[str, Any]:
        """No session setup needed for Snowflake"""
        return {}

    def session_teardown(self, session_data: Optional[Dict[str, Any]] = None) -> None:
        """No session teardown needed for Snowflake"""
        pass

    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """
        Get a Snowflake connection using environment variables.

        Returns:
            Snowflake connection object
        """
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            autocommit=True,
        )

    def setup_resource(
        self, resource_config: Optional[SnowflakeResourceConfig] = None
    ) -> SnowflakeResourceData:
        """Set up Snowflake database and schema with optional SQL file loading"""
        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]
        print(f"❄️ Setting up Snowflake resource: {resource_id}")

        # Generate unique database and schema names
        timestamp = int(time.time())
        test_uuid = uuid.uuid4().hex[:8]

        database_name = config.get("database") or f"BENCH_DB_{timestamp}_{test_uuid}"
        schema_name = config.get("schema") or f"TEST_SCHEMA_{timestamp}_{test_uuid}"
        database_name = database_name.upper()
        schema_name = schema_name.upper()

        creation_start = time.time()
        created_resources = []
        connection = None

        try:
            # Establish connection to Snowflake
            connection = self.get_connection()
            print(f"✅ Connected to Snowflake successfully")

            # Create database and schema
            cursor = connection.cursor()
            try:
                # Create database if it doesn't exist
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                cursor.execute(f"USE DATABASE {database_name}")

                # Create transient schema for cost optimization
                cursor.execute(f"CREATE TRANSIENT SCHEMA IF NOT EXISTS {schema_name}")
                cursor.execute(f"USE SCHEMA {schema_name}")

                print(
                    f"✅ Created Snowflake database {database_name} and schema {schema_name}"
                )

                # Track created resources
                created_resources.append(
                    {
                        "type": "database",
                        "name": database_name,
                        "schema": schema_name,
                        "tables": [],
                    }
                )

            finally:
                cursor.close()

            # Process SQL file if provided
            if sql_file := config.get("sql_file"):
                self._load_sql_file(
                    connection,
                    sql_file,
                    database_name,
                    schema_name,
                    config,
                    created_resources,
                )

        except Exception as e:
            # Clean up on failure
            print(f"❌ Failed to create Snowflake resource {resource_id}: {e}")
            if connection:
                self._cleanup_resources(connection, created_resources)
                connection.close()
            raise

        creation_end = time.time()
        creation_duration = creation_end - creation_start

        # Create resource data
        resource_data = SnowflakeResourceData(
            resource_id=resource_id,
            type="snowflake_resource",
            creation_time=creation_start,
            creation_duration=creation_duration,
            description=f"Snowflake resource for {resource_id}",
            status="active",
            database=database_name,
            schema=schema_name,
            connection=connection,
            created_resources=created_resources,
        )

        # Store for later access during validation
        self._resource_data = resource_data

        print(f"✅ Snowflake resource {resource_id} ready! ({creation_duration:.2f}s)")
        return resource_data

    def _load_sql_file(
        self,
        connection,
        sql_file: str,
        database_name: str,
        schema_name: str,
        config: SnowflakeResourceConfig,
        created_resources: List[Dict[str, Any]],
    ) -> None:
        """Load SQL file with variable substitution and S3 configuration"""
        # Resolve SQL file path
        if not os.path.isabs(sql_file):
            # Look for SQL file relative to test directories
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

        print(f"📄 Loading SQL file {sql_file}")

        # Read SQL file content
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Prepare template variables for substitution
        variables = {
            "DB": database_name,
            "SCHEMA": schema_name,
            "DATABASE": database_name,
            "WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        }

        # Add S3 configuration variables if provided
        if s3_config := config.get("s3_config"):
            # Resolve environment variable references
            aws_key_id = s3_config.get("aws_key_id", "")
            if aws_key_id.startswith("env:"):
                aws_key_id = os.getenv(aws_key_id[4:])

            aws_secret_key = s3_config.get("aws_secret_key", "")
            if aws_secret_key.startswith("env:"):
                aws_secret_key = os.getenv(aws_secret_key[4:])

            variables.update(
                {
                    "BUCKET_URL": s3_config.get("bucket_url", ""),
                    "S3_KEY": s3_config.get("s3_key", ""),
                    "AWS_ACCESS_KEY": aws_key_id,
                    "AWS_SECRET_KEY": aws_secret_key,
                }
            )

        # Perform variable substitution
        processed_sql = sql_content
        for var_name, var_value in variables.items():
            processed_sql = processed_sql.replace(f"{{{{{var_name}}}}}", str(var_value))

        print(f"🔄 Executing SQL file with variables substituted")

        # Execute SQL using Snowflake's execute_string for multi-statement support
        cursors = connection.execute_string(processed_sql)

        # Process results from each statement
        for i, cursor in enumerate(cursors):
            print(f"✅ Processed statement {i+1}")
            try:
                cursor.fetchall()
            except Exception:
                pass  # DDL and DML statements may not return results

        print(f"✅ Successfully executed SQL file")

        # Query created tables
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT table_name 
                FROM {database_name}.information_schema.tables 
                WHERE table_schema = '{schema_name}'
                ORDER BY table_name
            """
            )
            tables = [row[0] for row in cursor.fetchall()]
            created_resources[0]["tables"] = tables
            print(f"📊 Created {len(tables)} tables: {tables}")

        except Exception as e:
            print(f"⚠️ Error querying created tables: {e}")
        finally:
            cursor.close()

    def _cleanup_resources(
        self, connection, created_resources: List[Dict[str, Any]]
    ) -> None:
        """Clean up created Snowflake resources"""
        try:
            cursor = connection.cursor()

            # Clean up in reverse order
            for resource in reversed(created_resources):
                if resource["type"] == "database":
                    db_name = resource["name"]
                    schema_name = resource["schema"]

                    # Drop schema with CASCADE
                    cursor.execute(
                        f"DROP SCHEMA IF EXISTS {db_name}.{schema_name} CASCADE"
                    )
                    print(f"🗑️ Dropped schema {db_name}.{schema_name}")

            cursor.close()
        except Exception as e:
            print(f"⚠️ Error during Snowflake cleanup: {e}")

    def teardown_resource(self, resource_data: SnowflakeResourceData) -> None:
        """Clean up Snowflake resources"""
        resource_id = resource_data["resource_id"]
        connection = resource_data["connection"]
        created_resources = resource_data["created_resources"]

        print(f"🧹 Cleaning up Snowflake resource: {resource_id}")

        try:
            if connection:
                self._cleanup_resources(connection, created_resources)
                connection.close()

            print(f"✅ Snowflake resource {resource_id} cleaned up successfully")

        except Exception as e:
            print(f"❌ Error cleaning up Snowflake resource {resource_id}: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "snowflake_resource"

    @classmethod
    def get_default_config(cls) -> SnowflakeResourceConfig:
        """Return default configuration for Snowflake resources"""
        timestamp = int(time.time())
        test_uuid = uuid.uuid4().hex[:8]

        return SnowflakeResourceConfig(
            resource_id=f"snowflake_test_{timestamp}_{test_uuid}",
            database=None,  # Will be auto-generated
            schema=None,  # Will be auto-generated
            sql_file=None,
            s3_config=None,
        )

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create Snowflake config section using the fixture's resource data.

        Returns:
            Dictionary containing the snowflake service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "Snowflake resource data not available - ensure setup_resource was called"
            )

        # Extract connection details from resource data
        connection_params = resource_data.get("connection_params", {})

        return {
            "snowflake": {
                "account": connection_params.get(
                    "account", os.getenv("SNOWFLAKE_ACCOUNT")
                ),
                "user": connection_params.get("user", os.getenv("SNOWFLAKE_USERNAME")),
                "password": connection_params.get(
                    "password", os.getenv("SNOWFLAKE_PASSWORD")
                ),
                "database": resource_data.get("database_name"),
                "schema": resource_data.get("schema_name"),
                "warehouse": connection_params.get(
                    "warehouse", os.getenv("SNOWFLAKE_WAREHOUSE")
                ),
                "role": connection_params.get("role", os.getenv("SNOWFLAKE_ROLE")),
            }
        }
