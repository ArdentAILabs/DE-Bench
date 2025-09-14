# Snowflake Resource Fixture

The `snowflake_resource` fixture provides isolated Snowflake database environments for each test using SQL file-based schema creation and S3 data loading capabilities.

## Overview

The fixture creates and manages Snowflake databases, schemas, and tables for each test function, with support for SQL file-based schema creation and S3 data loading. It provides a fully managed Snowflake environment with automatic cleanup and variable substitution.

## Features

- **Function-scoped**: Each test gets its own isolated database and schema
- **SQL file support**: Creates tables and loads data from SQL files with variable substitution
- **S3 integration**: Supports loading data from S3 using Parquet files with AWS credentials
- **Variable substitution**: Template variables for database names, schemas, warehouses, and S3 configs
- **Automatic cleanup**: Resources are automatically cleaned up after each test
- **Multi-statement SQL**: Supports complex SQL files with multiple statements
- **Connection management**: Provides direct database connection for test use
- **Parallel execution support**: Unique resource naming prevents conflicts in parallel test execution
- **Transient schemas**: Uses transient schemas for cost optimization

## Prerequisites

### Snowflake Environment
#### Creating a Snowflake Environment
If you do not have not have a Snowflake environment, then sign up for a trial. When signing up for a trial; be sure select Enterprise when selecting the Snowflake edition and select any cloud service provider.

### Required Environment Variables

#### Retrieving the Values
The values for these environment variables can be found by logging into Snowflake, clicking your name in the bottom left, click Account, then View Account Details. In the menu that pops up, click Config File and select your warehouse. Copy and paste the values for the appropriate environment variable and enter your password for the `SNOWFLAKE_PASSWORD` value.

The fixture requires the following environment variables to be set:
```bash
# Snowflake connection credentials (REQUIRED)
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_account_identifier  # e.g., xy12345.us-east-1
SNOWFLAKE_WAREHOUSE=your_warehouse_name
SNOWFLAKE_ROLE=SYSADMIN  # Optional, defaults to SYSADMIN

# AWS credentials for S3 data loading (OPTIONAL, only if using S3 config)
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
```

### Required Tools

- **Snowflake Python Connector**: Must be installed (`pip install snowflake-connector-python`)
- **Snowflake Account**: Active Snowflake account with appropriate permissions
- **AWS Account**: For S3 data loading (optional)

## Usage

### Basic Usage

```python
import pytest

@pytest.mark.snowflake
def test_my_snowflake_test(snowflake_resource):
    # Access the database connection
    connection = snowflake_resource["connection"]
    cursor = connection.cursor()
    
    # Access database and schema names
    database = snowflake_resource["database"]
    schema = snowflake_resource["schema"]
    
    # Your test logic here
    cursor.execute(f"SELECT * FROM {database}.{schema}.MY_TABLE")
    results = cursor.fetchall()
    
    # The Snowflake database and schema are already created and ready to use
```

### Resource Data Structure

The `snowflake_resource` fixture returns a dictionary with the following structure:

```python
{
    "resource_id": "snowflake_resource_test_name_timestamp",
    "type": "snowflake_resource",
    "test_name": "test_function_name",
    "creation_time": timestamp,
    "worker_pid": process_id,
    "creation_duration": setup_time_in_seconds,
    "description": "A Snowflake resource for test_name",
    "status": "active",
    "database": "BENCH_DB_timestamp_uuid",
    "schema": "TEST_SCHEMA_timestamp_uuid",
    "connection": snowflake_connection_object,
    "created_resources": [
        {
            "type": "database",
            "name": "BENCH_DB_timestamp_uuid",
            "schema": "TEST_SCHEMA_timestamp_uuid",
            "tables": ["table1", "table2", ...]
        }
    ]
}
```

### Template Structure

The fixture accepts a template parameter with the following structure:

```python
{
    "resource_id": "test_12345_abc",  # Optional, auto-generated if not provided
    "database": "BENCH_DB_12345_ABC",  # Optional, auto-generated if not provided
    "schema": "TEST_SCHEMA_12345_ABC",  # Optional, auto-generated if not provided
    "sql_file": "schema.sql",  # Optional, relative to test directory
    "s3_config": {  # Optional, for data loading from S3
        "bucket_url": "s3://de-bench/",
        "s3_key": "v1/users/users_simple_20250902_161500.parquet",
        "aws_key_id": "env:AWS_ACCESS_KEY",  # Can reference env vars with "env:" prefix
        "aws_secret_key": "env:AWS_SECRET_KEY"
    }
}
```

### Complete Example Test

```python
import pytest
import time
import uuid

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

@pytest.mark.snowflake
@pytest.mark.database
@pytest.mark.parametrize("snowflake_resource", [{
    "resource_id": f"snowflake_test_{test_timestamp}_{test_uuid}",
    "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
    "schema": f"TEST_SCHEMA_{test_timestamp}_{test_uuid}",
    "sql_file": "users_schema.sql",
    "s3_config": {
        "bucket_url": "s3://de-bench/",
        "s3_key": "v1/users_simple_20250901_233609.parquet",
        "aws_key_id": "env:AWS_ACCESS_KEY",
        "aws_secret_key": "env:AWS_SECRET_KEY"
    }
}], indirect=True)
def test_snowflake_data_operations(snowflake_resource):
    """Example test showing complete Snowflake workflow."""
    
    # Get connection details from fixture
    connection = snowflake_resource["connection"]
    database = snowflake_resource["database"]
    schema = snowflake_resource["schema"]
    
    cursor = connection.cursor()
    
    try:
        # Example: Verify initial data was loaded
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.USERS")
        user_count = cursor.fetchone()[0]
        print(f"Initial user count: {user_count}")
        
        # Example: Insert new record
        cursor.execute(f"""
            INSERT INTO {database}.{schema}.USERS 
            (USER_ID, FIRST_NAME, LAST_NAME, EMAIL, AGE, CITY, STATE, IS_ACTIVE, TOTAL_PURCHASES)
            VALUES (999, 'Test', 'User', 'test@example.com', 25, 'Test City', 'TC', TRUE, 0.00)
        """)
        
        # Example: Verify insertion
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.USERS")
        new_count = cursor.fetchone()[0]
        assert new_count == user_count + 1, f"Expected {user_count + 1} users, got {new_count}"
        
        # Example: Query specific data
        cursor.execute(f"""
            SELECT FIRST_NAME, LAST_NAME, EMAIL 
            FROM {database}.{schema}.USERS 
            WHERE USER_ID = 999
        """)
        result = cursor.fetchone()
        assert result[0] == 'Test', f"Expected 'Test', got '{result[0]}'"
        
        print("✅ All Snowflake operations completed successfully!")
        
    finally:
        cursor.close()
```

## How It Works

### Per-Test Setup
1. **Resource Naming**: Generates unique database and schema names using timestamp and UUID
2. **Database Connection**: Establishes connection to Snowflake using environment variables
3. **Database Creation**: Creates database and transient schema for the test
4. **SQL File Processing**: 
   - Loads SQL file from test directory (if provided)
   - Performs variable substitution for database names, schemas, warehouses, and S3 configs
   - Executes multi-statement SQL using Snowflake's `execute_string` method
5. **S3 Data Loading**: 
   - Creates temporary stages and file formats for S3 access
   - Loads data from Parquet files using COPY INTO statements
   - Supports AWS credential resolution from environment variables
6. **Resource Tracking**: Queries information schema to track created tables

### Test Execution
- Provides the Snowflake connection and resource details to the test
- Each test gets its own isolated database and schema
- Direct database connection allows for complex SQL operations

### Per-Test Cleanup
1. **Schema Cleanup**: Drops the transient schema with CASCADE to remove all objects
2. **Connection Cleanup**: Closes database connections
3. **Resource Tracking**: Logs cleanup completion and any errors

## SQL File Support

### Variable Substitution

SQL files support the following template variables:

- `{{DB}}` or `{{DATABASE}}`: Database name
- `{{SCHEMA}}`: Schema name  
- `{{WAREHOUSE}}`: Snowflake warehouse name
- `{{ROLE}}`: Snowflake role name
- `{{BUCKET_URL}}`: S3 bucket URL (if S3 config provided)
- `{{S3_KEY}}`: S3 object key (if S3 config provided)
- `{{AWS_ACCESS_KEY}}`: AWS access key (if S3 config provided)
- `{{AWS_SECRET_KEY}}`: AWS secret key (if S3 config provided)

### Example SQL File

```sql
-- users_schema.sql
USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create file format for Parquet
CREATE OR REPLACE FILE FORMAT PARQUET_STD TYPE=PARQUET;

-- Create temporary stage for S3 access
CREATE OR REPLACE TEMP STAGE _temp_stage
  URL='{{BUCKET_URL}}'
  CREDENTIALS=(AWS_KEY_ID='{{AWS_ACCESS_KEY}}' AWS_SECRET_KEY='{{AWS_SECRET_KEY}}');

-- Create table
CREATE OR REPLACE TABLE USERS (
    USER_ID NUMBER,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    AGE NUMBER,
    CITY VARCHAR(100),
    STATE VARCHAR(2),
    SIGNUP_DATE TIMESTAMP,
    IS_ACTIVE BOOLEAN,
    TOTAL_PURCHASES DECIMAL(10,2)
);

-- Load data from S3
COPY INTO USERS
FROM @_temp_stage
FILES = ('{{S3_KEY}}')
FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
ON_ERROR=ABORT_STATEMENT;

-- Verify data loaded
SELECT COUNT(*) AS total_users FROM USERS;
```

## S3 Integration

### S3 Configuration

The fixture supports loading data from S3 using the `s3_config` parameter:

```python
"s3_config": {
    "bucket_url": "s3://your-bucket/",
    "s3_key": "path/to/your/file.parquet",
    "aws_key_id": "env:AWS_ACCESS_KEY",  # References environment variable
    "aws_secret_key": "env:AWS_SECRET_KEY"  # References environment variable
}
```

### Supported File Formats

- **Parquet**: Primary format with automatic schema inference
- **CSV**: Supported with appropriate file format configuration
- **JSON**: Supported with appropriate file format configuration

### AWS Credential Resolution

Credentials can be provided in multiple ways:

1. **Environment Variables**: Use `"env:VARIABLE_NAME"` format
2. **Direct Values**: Provide actual credential values (not recommended for security)
3. **AWS IAM Roles**: If running in AWS environment with appropriate IAM roles

## Benefits

- **Isolated**: Each test gets its own database and schema
- **Flexible**: Supports complex SQL schemas and S3 data loading
- **Cost-optimized**: Uses transient schemas to minimize storage costs
- **Scalable**: Unique naming allows parallel test execution
- **Template-driven**: Variable substitution makes SQL files reusable
- **S3 integration**: Built-in support for loading data from S3
- **Automatic cleanup**: Resources are cleaned up after tests complete
- **Direct access**: Provides database connection for complex operations

## Integration with Existing Tests

To update existing tests to use the fixture:

1. Add `snowflake_resource` as a parameter to your test function
2. Use the provided connection instead of creating your own
3. Use the provided database and schema names in your SQL
4. Move schema creation to SQL files for better organization

### Migration Example

**Before (manual setup):**
```python
def test_snowflake_manual():
    connection = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE test_db")
    # Test logic...
    cursor.execute("DROP DATABASE test_db")
    connection.close()
```

**After (fixture):**
```python
@pytest.mark.snowflake
@pytest.mark.parametrize("snowflake_resource", [{
    "sql_file": "my_schema.sql"
}], indirect=True)
def test_snowflake_fixture(snowflake_resource):
    connection = snowflake_resource["connection"]
    database = snowflake_resource["database"]
    schema = snowflake_resource["schema"]
    cursor = connection.cursor()
    # Test logic using provided database/schema...
    # Cleanup is automatic
```

## Troubleshooting

### Common Issues

- **Missing environment variables**: Ensure all required Snowflake environment variables are set
- **SQL file not found**: Verify SQL file path is correct relative to test directory
- **S3 access denied**: Check AWS credentials and S3 bucket permissions
- **Connection failures**: Verify Snowflake account identifier and network access
- **SQL execution errors**: Check SQL syntax and variable substitution
- **Permission errors**: Ensure Snowflake user has appropriate role permissions
- **File format issues**: Verify S3 file format matches expected schema

### Debug Information

The fixture provides detailed logging including:
- Worker process ID for parallel execution tracking
- Creation timestamps and durations
- Database and schema names
- SQL file processing steps
- S3 configuration and loading status
- Table creation and data loading results
- Cleanup operations and any errors

### Performance Considerations

- **Database creation**: ~1-2 seconds for new databases
- **Schema creation**: ~1 second for transient schemas
- **SQL file execution**: Varies by complexity and data volume
- **S3 data loading**: Depends on file size and network speed
- **Cleanup operations**: ~1-2 seconds for schema drops
- **Connection overhead**: Minimal with connection reuse

### Resource Management

- **Transient schemas**: Automatically cleaned up by Snowflake after session
- **Temporary stages**: Created and dropped within the same session
- **Connection pooling**: Single connection per test for efficiency
- **Memory usage**: Minimal overhead with proper cursor management

## Dependencies

- **snowflake-connector-python**: For Snowflake database connectivity
- **pytest**: For fixture functionality
- **uuid**: For unique identifier generation (built into Python)
- **os**: For environment variable access (built into Python)
- **time**: For timestamp generation (built into Python)
- **re**: For test name sanitization (built into Python)

## Security Notes

- **Credentials**: All sensitive information handled through environment variables
- **S3 access**: AWS credentials resolved securely from environment
- **Connection security**: Uses Snowflake's secure connection protocol
- **Resource isolation**: Each test gets completely isolated database resources
- **Cleanup**: All resources are properly cleaned up after tests complete
- **No persistent data**: Transient schemas ensure no data persistence between tests

## Best Practices

### SQL File Organization

- Place SQL files in the same directory as your test files
- Use descriptive names for SQL files (e.g., `users_schema.sql`, `orders_schema.sql`)
- Include comments explaining the schema structure
- Use consistent variable naming in templates

### Test Structure

- Generate unique identifiers for parallel execution
- Use descriptive test names and resource IDs
- Include proper error handling and cleanup
- Document expected data and test scenarios

### S3 Data Management

- Use Parquet format for better performance and schema preservation
- Organize S3 data in logical directory structures
- Use environment variables for AWS credentials
- Test S3 access independently before running tests

### Performance Optimization

- Use transient schemas to minimize storage costs
- Batch SQL operations when possible
- Close cursors properly to free resources
- Use appropriate warehouse sizes for your workload

## Advanced Usage

### Custom Resource Naming

```python
@pytest.mark.parametrize("snowflake_resource", [{
    "resource_id": "custom_test_resource",
    "database": "CUSTOM_DB_NAME",
    "schema": "CUSTOM_SCHEMA_NAME"
}], indirect=True)
def test_with_custom_names(snowflake_resource):
    # Uses custom database and schema names
    pass
```

### Multiple SQL Files

```python
# You can reference different SQL files for different test scenarios
@pytest.mark.parametrize("snowflake_resource", [
    {"sql_file": "users_schema.sql"},
    {"sql_file": "orders_schema.sql"},
    {"sql_file": "products_schema.sql"}
], indirect=True)
def test_multiple_schemas(snowflake_resource):
    # Test with different schema configurations
    pass
```

### S3 Data Variants

```python
@pytest.mark.parametrize("snowflake_resource", [
    {
        "sql_file": "users_schema.sql",
        "s3_config": {
            "bucket_url": "s3://de-bench/",
            "s3_key": "v1/users_small.parquet",
            "aws_key_id": "env:AWS_ACCESS_KEY",
            "aws_secret_key": "env:AWS_SECRET_KEY"
        }
    },
    {
        "sql_file": "users_schema.sql", 
        "s3_config": {
            "bucket_url": "s3://de-bench/",
            "s3_key": "v1/users_large.parquet",
            "aws_key_id": "env:AWS_ACCESS_KEY",
            "aws_secret_key": "env:AWS_SECRET_KEY"
        }
    }
], indirect=True)
def test_different_data_sizes(snowflake_resource):
    # Test with different data volumes
    pass
```
