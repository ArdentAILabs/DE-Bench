"""
This module provides a pytest fixture for creating isolated Snowflake database environments using SQL files and S3 data loading.
"""

import os
import re
import time
import uuid

import pytest
import snowflake.connector


@pytest.fixture(scope="function")
def snowflake_resource(request):
    """
    A function-scoped fixture that creates Snowflake resources from SQL files.
    Each test gets its own isolated database and schema.

    This fixture creates Snowflake databases, schemas, and tables from SQL files with support for
    S3 data loading and variable substitution. It follows the DE-Bench fixture pattern with
    comprehensive resource management and automatic cleanup.

    Features:
    - Function-scoped isolation with unique database and schema names
    - SQL file support with variable substitution ({{DB}}, {{SCHEMA}}, etc.)
    - S3 integration for loading data from Parquet files
    - Multi-statement SQL execution using Snowflake's execute_string
    - Automatic cleanup of created resources
    - Parallel execution support with unique resource naming
    - Direct database connection for test use

    Template structure: {
        "resource_id": "test_12345_abc",  # Optional, auto-generated if not provided
        "database": "BENCH_DB_12345_ABC",  # Optional, auto-generated if not provided
        "schema": "TEST_SCHEMA_12345_ABC",  # Optional, auto-generated if not provided
        "sql_file": "schema.sql",  # Optional, relative to test directory
        "s3_config": {  # Optional - for data loading from S3
            "bucket_url": "s3://de-bench/",
            "s3_key": "v1/users/users_simple_20250902_161500.parquet",
            "aws_key_id": "env:AWS_KEY_ID",  # Can reference env vars with "env:" prefix
            "aws_secret_key": "env:AWS_SECRET_KEY"
        }
    }

    Required environment variables:
    - SNOWFLAKE_USER: Snowflake username
    - SNOWFLAKE_PASSWORD: Snowflake password
    - SNOWFLAKE_ACCOUNT: Snowflake account identifier
    - SNOWFLAKE_WAREHOUSE: Snowflake warehouse name
    - SNOWFLAKE_ROLE: Snowflake role (optional, defaults to SYSADMIN)
    - AWS_ACCESS_KEY: AWS access key (if using S3 config)
    - AWS_SECRET_KEY: AWS secret key (if using S3 config)

    Returns:
        dict: Resource data containing:
            - resource_id: Unique identifier for the resource
            - type: "snowflake_resource"
            - test_name: Sanitized test function name
            - creation_time: Timestamp when resource was created
            - worker_pid: Process ID of the worker
            - creation_duration: Time taken to create the resource
            - description: Human-readable description
            - status: "active"
            - database: Database name used
            - schema: Schema name used
            - connection: Snowflake connection object for direct use
            - created_resources: List of created database objects

    Raises:
        FileNotFoundError: If SQL file is specified but not found
        snowflake.connector.errors.Error: If Snowflake connection or SQL execution fails
        Exception: For other setup or configuration errors

    Example:
        @pytest.mark.snowflake
        @pytest.mark.parametrize("snowflake_resource", [{
            "sql_file": "users_schema.sql",
            "s3_config": {
                "bucket_url": "s3://de-bench/",
                "s3_key": "v1/users.parquet",
                "aws_key_id": "env:AWS_ACCESS_KEY",
                "aws_secret_key": "env:AWS_SECRET_KEY"
            }
        }], indirect=True)
        def test_snowflake_operations(snowflake_resource):
            connection = snowflake_resource["connection"]
            database = snowflake_resource["database"]
            schema = snowflake_resource["schema"]
            # Test logic here...
    """
    # Initialize timing and logging
    start_time = time.time()
    test_name = re.sub(r"[^\w\-]", "_", request.node.name)
    print(f"Worker {os.getpid()}: Starting snowflake_resource for {test_name}")

    # Extract template configuration from pytest parametrize
    build_template = request.param

    # Generate unique resource names to prevent conflicts in parallel execution
    timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]

    # Create unique database and schema names, using template values if provided
    database_name = build_template.get(
        "database", f"BENCH_DB_{timestamp}_{test_uuid}"
    ).upper()
    schema_name = build_template.get(
        "schema", f"TEST_SCHEMA_{timestamp}_{test_uuid}"
    ).upper()
    resource_id = build_template.get(
        "resource_id", f"snowflake_resource_{test_name}_{timestamp}"
    )

    print(f"Worker {os.getpid()}: Creating Snowflake resource for {test_name}")
    print(f"Worker {os.getpid()}: Database: {database_name}, Schema: {schema_name}")

    # Initialize tracking variables
    creation_start = time.time()
    created_resources = []
    connection = None

    try:
        # Establish connection to Snowflake using environment variables
        connection = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            # role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
            autocommit=True,
        )

        print(f"Worker {os.getpid()}: Connected to Snowflake successfully")

        # Create database and transient schema for test isolation
        cursor = connection.cursor()
        try:
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            cursor.execute(f"USE DATABASE {database_name}")

            # Create transient schema for cost optimization (auto-cleanup)
            cursor.execute(f"CREATE TRANSIENT SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")

            print(
                f"Worker {os.getpid()}: Created database {database_name} and schema {schema_name}"
            )

            # Track created resources for cleanup
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

        # Process SQL file if provided in template
        if "sql_file" in build_template:
            sql_file = build_template["sql_file"]

            # Resolve SQL file path relative to test directory
            if not os.path.isabs(sql_file):
                test_file = request.fspath.strpath
                test_dir = os.path.dirname(test_file)
                sql_file = os.path.join(test_dir, sql_file)

            if not os.path.exists(sql_file):
                raise FileNotFoundError(f"SQL file not found: {sql_file}")

            print(f"Worker {os.getpid()}: Loading SQL file {sql_file}")

            # Read SQL file content for processing
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Prepare template variables for substitution
            variables = {
                "DB": database_name,
                "SCHEMA": schema_name,
                "DATABASE": database_name,  # Alternative name for compatibility
                "WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
                # "ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            }

            # Add S3 configuration variables if provided
            if "s3_config" in build_template:
                s3_config = build_template["s3_config"]

                # Resolve environment variable references (env:VARIABLE_NAME format)
                aws_key_id = s3_config.get("aws_key_id", "")
                if aws_key_id.startswith("env:"):
                    aws_key_id = os.getenv(aws_key_id[4:])

                aws_secret_key = s3_config.get("aws_secret_key", "")
                if aws_secret_key.startswith("env:"):
                    aws_secret_key = os.getenv(aws_secret_key[4:])

                # Add S3 variables to substitution dictionary
                variables.update(
                    {
                        "BUCKET_URL": s3_config.get("bucket_url", ""),
                        "S3_KEY": s3_config.get("s3_key", ""),
                        "AWS_ACCESS_KEY": aws_key_id,
                        "AWS_SECRET_KEY": aws_secret_key,
                    }
                )

            # Perform variable substitution in SQL content
            processed_sql = sql_content
            for var_name, var_value in variables.items():
                processed_sql = processed_sql.replace(
                    f"{{{{{var_name}}}}}", str(var_value)
                )

            print(
                f"Worker {os.getpid()}: Executing SQL file with variables substituted"
            )

            # Execute SQL using Snowflake's native execute_string for multi-statement support
            print(f"Worker {os.getpid()}: Executing multi-statement SQL")
            cursors = connection.execute_string(processed_sql)

            # Process results from each statement in the SQL file
            for i, cursor in enumerate(cursors):
                print(f"Worker {os.getpid()}: Processed statement {i + 1}")
                # Consume results if any (some statements don't return results)
                try:
                    cursor.fetchall()
                except Exception:
                    pass  # DDL and DML statements may not return results

            print(f"Worker {os.getpid()}: Successfully executed SQL file")

            # Query information schema to track created tables
            cursor = connection.cursor()
            try:
                cursor.execute(f"""
                    SELECT table_name 
                    FROM {database_name}.information_schema.tables 
                    WHERE table_schema = '{schema_name}'
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]
                created_resources[0]["tables"] = tables
                print(f"Worker {os.getpid()}: Created {len(tables)} tables: {tables}")

            except Exception as e:
                print(f"Worker {os.getpid()}: Error querying created tables: {e}")
                raise
            finally:
                cursor.close()

        # Calculate creation duration and log completion
        creation_end = time.time()
        print(
            f"Worker {os.getpid()}: Snowflake resource creation took {creation_end - creation_start:.2f}s"
        )

        # Create comprehensive resource data dictionary for test use
        resource_data = {
            "resource_id": resource_id,
            "type": "snowflake_resource",
            "test_name": test_name,
            "creation_time": time.time(),
            "worker_pid": os.getpid(),
            "creation_duration": creation_end - creation_start,
            "description": f"A Snowflake resource for {test_name}",
            "status": "active",
            "database": database_name,
            "schema": schema_name,
            "connection": connection,  # Provide direct connection for test use
            "created_resources": created_resources,
        }

        print(f"Worker {os.getpid()}: Created Snowflake resource {resource_id}")

        # Log total fixture setup time
        fixture_end_time = time.time()
        print(
            f"Worker {os.getpid()}: Snowflake fixture setup took {fixture_end_time - start_time:.2f}s total"
        )

        # Yield resource data to test function
        yield resource_data

    except Exception as e:
        # Log and re-raise any errors during fixture setup
        print(f"Worker {os.getpid()}: Error in Snowflake fixture: {e}")
        raise

    finally:
        # Cleanup after test completes - always executed
        print(f"Worker {os.getpid()}: Cleaning up Snowflake resource {resource_id}")

        if connection:
            try:
                cleanup_cursor = connection.cursor()

                # Clean up created resources in reverse order (schema first, then database)
                for resource in reversed(created_resources):
                    if resource["type"] == "database":
                        db_name = resource["name"]
                        schema_name = resource["schema"]

                        # Drop schema with CASCADE to remove all objects (tables, views, etc.)
                        cleanup_cursor.execute(
                            f"DROP SCHEMA IF EXISTS {db_name}.{schema_name} CASCADE"
                        )
                        print(
                            f"Worker {os.getpid()}: Dropped schema {db_name}.{schema_name}"
                        )

                        # Note: We leave the database intact to avoid conflicts with other tests
                        # Transient schemas are automatically cleaned up by Snowflake after session

                cleanup_cursor.close()
                connection.close()
                print(
                    f"Worker {os.getpid()}: Snowflake resource {resource_id} cleaned up successfully"
                )

            except Exception as e:
                # Log cleanup errors but don't fail the test
                print(
                    f"Worker {os.getpid()}: Error cleaning up Snowflake resource: {e}"
                )
                try:
                    if connection:
                        connection.close()
                except Exception:
                    pass  # Ignore connection close errors during cleanup
