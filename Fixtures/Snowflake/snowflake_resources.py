import pytest
import time
import os
import re
import time
import uuid
import snowflake.connector
from snowflake.connector import DictCursor


@pytest.fixture(scope="function")
def snowflake_resource(request):
    """
    A function-scoped fixture that creates Snowflake resources from SQL files.
    Each test gets its own isolated database and schema.

    Snowflake Resources Fixture

    This fixture creates Snowflake databases, schemas, and tables from SQL files.
    Follows the DE-Bench fixture pattern with SQL file support like PostgreSQL.

    Template structure: {
        "resource_id": "test_12345_abc",
        "database": "BENCH_DB_12345_ABC", 
        "schema": "TEST_SCHEMA_12345_ABC",
        "sql_file": "schema.sql",  # Relative to test directory
        "s3_config": {  # Optional - for data loading from S3
            "bucket_url": "s3://de-bench/",
            "s3_key": "v1/users/users_simple_20250902_161500.parquet",
            "aws_key_id": "env:AWS_KEY_ID",
            "aws_secret_key": "env:AWS_SECRET_KEY"
        }
    }

    """
    start_time = time.time()
    test_name = re.sub(r'[^\w\-]', '_', request.node.name)
    print(f"Worker {os.getpid()}: Starting snowflake_resource for {test_name}")
    
    # Extract template
    build_template = request.param
    
    # Generate unique resource names
    timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]
    
    database_name = build_template.get("database", f"BENCH_DB_{timestamp}_{test_uuid}").upper()
    schema_name = build_template.get("schema", f"TEST_SCHEMA_{timestamp}_{test_uuid}").upper()
    resource_id = build_template.get("resource_id", f"snowflake_resource_{test_name}_{timestamp}")
    
    print(f"Worker {os.getpid()}: Creating Snowflake resource for {test_name}")
    print(f"Worker {os.getpid()}: Database: {database_name}, Schema: {schema_name}")
    
    creation_start = time.time()
    created_resources = []
    connection = None
    
    try:
        # Connect to Snowflake
        connection = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
            autocommit=True,
        )
        
        print(f"Worker {os.getpid()}: Connected to Snowflake successfully")
        
        # Create database and schema first
        cursor = connection.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            cursor.execute(f"USE DATABASE {database_name}")
            cursor.execute(f"CREATE TRANSIENT SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            
            print(f"Worker {os.getpid()}: Created database {database_name} and schema {schema_name}")
            created_resources.append({
                "type": "database", 
                "name": database_name, 
                "schema": schema_name,
                "tables": []
            })
            
        finally:
            cursor.close()
        
        # Process SQL file if provided
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
            
            # Read and process SQL file
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Prepare variables for substitution
            variables = {
                "DB": database_name,
                "SCHEMA": schema_name,
                "DATABASE": database_name,  # Alternative name
                "WAREHOUSE": os.getenv('SNOWFLAKE_WAREHOUSE'),
                "ROLE": os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
            }
            
            # Add S3 config variables if provided
            if "s3_config" in build_template:
                s3_config = build_template["s3_config"]
                # Resolve env: variables
                aws_key_id = s3_config.get("aws_key_id", "")
                if aws_key_id.startswith("env:"):
                    aws_key_id = os.getenv(aws_key_id[4:])
                
                aws_secret_key = s3_config.get("aws_secret_key", "")
                if aws_secret_key.startswith("env:"):
                    aws_secret_key = os.getenv(aws_secret_key[4:])
                
                variables.update({
                    "BUCKET_URL": s3_config.get("bucket_url", ""),
                    "S3_KEY": s3_config.get("s3_key", ""),
                    "AWS_ACCESS_KEY": aws_key_id,
                    "AWS_SECRET_KEY": aws_secret_key,
                })
            
            # Replace {{VAR}} placeholders
            processed_sql = sql_content
            for var_name, var_value in variables.items():
                processed_sql = processed_sql.replace(f"{{{{{var_name}}}}}", str(var_value))
            
            print(f"Worker {os.getpid()}: Executing SQL file with variables substituted")
            
            # Execute SQL using Snowflake's native execute_string for multi-statement support
            print(f"Worker {os.getpid()}: Executing multi-statement SQL")
            cursors = connection.execute_string(processed_sql)
            
            # Process results from each statement
            for i, cursor in enumerate(cursors):
                print(f"Worker {os.getpid()}: Processed statement {i+1}")
                # Consume results if any
                try:
                    cursor.fetchall()
                except Exception:
                    pass  # Some statements don't return results (DDL, DML)
            
            print(f"Worker {os.getpid()}: Successfully executed SQL file")
            
            # Query created tables for tracking using a new cursor
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
                print(f"Worker {os.getpid()}: Error executing SQL file: {e}")
                raise
            finally:
                cursor.close()
        
        creation_end = time.time()
        print(f"Worker {os.getpid()}: Snowflake resource creation took {creation_end - creation_start:.2f}s")
        
        # Create detailed resource data
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
            "connection": connection,  # Provide connection for tests to use
            "created_resources": created_resources
        }
        
        print(f"Worker {os.getpid()}: Created Snowflake resource {resource_id}")
        
        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: Snowflake fixture setup took {fixture_end_time - start_time:.2f}s total")
        
        yield resource_data
        
    except Exception as e:
        print(f"Worker {os.getpid()}: Error in Snowflake fixture: {e}")
        raise
    
    finally:
        # Cleanup after test completes
        print(f"Worker {os.getpid()}: Cleaning up Snowflake resource {resource_id}")
        
        if connection:
            try:
                cleanup_cursor = connection.cursor()
                
                # Clean up created resources in reverse order
                for resource in reversed(created_resources):
                    if resource["type"] == "database":
                        db_name = resource["name"]
                        schema_name = resource["schema"]
                        
                        # Drop schema with CASCADE to remove all objects
                        cleanup_cursor.execute(f"DROP SCHEMA IF EXISTS {db_name}.{schema_name} CASCADE")
                        print(f"Worker {os.getpid()}: Dropped schema {db_name}.{schema_name}")
                        
                        # Optionally drop database if it's empty (be careful with this)
                        # For now, we'll leave the database and just clean the schema
                
                cleanup_cursor.close()
                connection.close()
                print(f"Worker {os.getpid()}: Snowflake resource {resource_id} cleaned up successfully")
                
            except Exception as e:
                print(f"Worker {os.getpid()}: Error cleaning up Snowflake resource: {e}")
                try:
                    if connection:
                        connection.close()
                except:
                    pass
