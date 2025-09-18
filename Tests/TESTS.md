# DE-Bench Test Pattern Documentation

This document explains the standardized test pattern used throughout the DE-Bench evaluation framework.

## Overview

All tests in DE-Bench follow a unified pattern that provides:
- **Consistent resource management** through the `DEBenchFixture` Abstract Base Class
- **Dynamic configuration generation** using fixture instances
- **Self-contained test execution** with automatic setup and cleanup
- **Standardized validation** with detailed test steps
- **Session-level resource sharing** for expensive resources like Airflow deployments

## Test Structure

Every test following the new pattern has these components:

### 1. Test Directory Structure
```
Tests/Test_Name/
├── Test_Configs.py          # Contains User_Input (task description)
├── test_test_name.py        # Main test implementation
├── schema.sql               # Database schema (if needed)
└── README.md               # Test documentation (optional)
```

### 2. Required Functions in Test File

Each test file must implement these three functions:

#### `get_fixtures() -> List[DEBenchFixture]`
Returns a list of fixture instances that the test requires.

```python
def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture
    
    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]
    
    postgres_fixture = PostgreSQLFixture(
        custom_config={
            "resource_id": f"test_{test_timestamp}_{test_uuid}",
            "databases": [
                {"name": f"test_db_{test_timestamp}_{test_uuid}", "sql_file": "schema.sql"}
            ],
        }
    )
    
    github_fixture = GitHubFixture(
        custom_config={
            "resource_id": f"test_github_{test_timestamp}_{test_uuid}",
            "create_branch": True,
        }
    )
    
    return [postgres_fixture, github_fixture]
```

#### `create_model_inputs(base_model_inputs, fixtures) -> Dict[str, Any]`
Generates the configuration for the AI model using the set-up fixtures.

```python
def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    This function has access to all fixture data after setup.
    """
    from extract_test_configs import create_config_from_fixtures
    
    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": Test_Configs.User_Input,  # Can be modified dynamically
    }
```

#### `validate_test(model_result, fixtures) -> Dict[str, Any]`
Validates the AI model's execution and returns a score with detailed test steps.

```python
def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully completed the task.
    
    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes the assigned task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed successfully...",
        },
        # Add more validation steps as needed
    ]
    
    try:
        # Validation logic here
        if model_result and model_result.get("status") != "failed":
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "✅ AI Agent completed successfully"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed"
            
    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"
    
    # Calculate score as fraction of passed steps
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
```

## Fixture System

### DEBenchFixture Abstract Base Class

All fixtures inherit from `DEBenchFixture` and must implement:

```python
class DEBenchFixture(ABC, Generic[ConfigType, ResourceType, ValidationArgsType]):
    def setup_resource(self, resource_config: Optional[ConfigType] = None) -> ResourceType:
        """Set up the resource (databases, services, etc.)"""
        pass
    
    def teardown_resource(self, resource_data: ResourceType) -> None:
        """Clean up the resource"""
        pass
    
    @classmethod
    def get_resource_type(cls) -> str:
        """Return unique resource type identifier"""
        pass
    
    @classmethod
    def get_default_config(cls) -> ConfigType:
        """Return default configuration"""
        pass
    
    def create_config_section(self) -> Dict[str, Any]:
        """Create configuration section for AI model"""
        pass
```

### Available Fixtures

#### Database Fixtures
- **`MongoDBFixture`**: MongoDB databases and collections
- **`MySQLFixture`**: MySQL databases and tables  
- **`PostgreSQLFixture`**: PostgreSQL databases with SQL file loading
- **`SnowflakeFixture`**: Snowflake databases with S3 integration

#### Service Fixtures
- **`AirflowFixture`**: Airflow deployments (session-level)
- **`GitHubFixture`**: GitHub repositories and branches

#### Planned Fixtures
- **`DatabricksFixture`**: Databricks clusters and notebooks (not yet implemented)

### Configuration Generation

Each fixture implements `create_config_section()` to provide its service configuration:

```python
def create_config_section(self) -> Dict[str, Any]:
    """
    Create config section using the fixture's resource data.
    """
    resource_data = getattr(self, "_resource_data", None)
    if not resource_data:
        raise Exception("Resource data not available - ensure setup_resource was called")
    
    return {
        "postgresql": {
            "hostname": self._connection_params["host"],
            "port": self._connection_params["port"],
            "username": self._connection_params["user"],
            "password": self._connection_params["password"],
            "databases": [{"name": db["name"]} for db in resource_data["created_resources"]],
        }
    }
```

The helper function `create_config_from_fixtures(fixtures)` automatically calls each fixture's `create_config_section()` method to build the complete configuration.

## Session-Level Fixtures

For expensive resources like Airflow deployments, fixtures can implement session-level behavior:

```python
def requires_session_setup(self) -> bool:
    """Return True if this fixture needs session-level setup"""
    return True

def session_setup(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
    """Set up session-level resources (called once per test run)"""
    pass

def session_teardown(self, session_data: Dict[str, Any]) -> None:
    """Clean up session-level resources"""
    pass
```

## Best Practices

### 1. Unique Resource Naming
Always use timestamps and UUIDs to avoid conflicts in parallel execution:

```python
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]
resource_id = f"test_{test_timestamp}_{test_uuid}"
```

### 2. Proper Error Handling
Handle errors gracefully in validation:

```python
try:
    # Validation logic
    pass
except Exception as e:
    for step in test_steps:
        if step["status"] == "running":
            step["status"] = "failed"
            step["Result_Message"] = f"❌ Error: {str(e)}"
```

### 3. Connection Parameter Storage
Store connection parameters during fixture setup for consistency:

```python
def setup_resource(self, resource_config):
    # Store connection params for later use
    self._connection_params = {
        "host": os.getenv("DATABASE_HOST"),
        "port": os.getenv("DATABASE_PORT"),
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASSWORD"),
    }
    # ... rest of setup
```

### 4. SQL File Path Resolution
Use absolute paths for SQL files to work in parallel execution:

```python
if not os.path.isabs(sql_file):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    tests_dir = os.path.join(project_root, "Tests")
    # Search for SQL file in test directories
```

## Converting Existing Tests

To convert a pytest-based test to the new pattern:

1. **Remove pytest decorators** and imports
2. **Add required imports**:
   ```python
   from typing import List, Dict, Any
   from Fixtures.base_fixture import DEBenchFixture
   ```
3. **Implement the three required functions**
4. **Update `Test_Configs.py`** to only contain `User_Input`
5. **Test the conversion** by running the test individually

## Example: Complete Test Implementation

```python
# test_example.py
import time
import uuid
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
import os
import importlib
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

def get_fixtures() -> List[DEBenchFixture]:
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    
    test_timestamp = int(time.time())
    test_uuid = uuid.uuid4().hex[:8]
    
    postgres_fixture = PostgreSQLFixture(
        custom_config={
            "resource_id": f"example_test_{test_timestamp}_{test_uuid}",
            "databases": [
                {"name": f"example_db_{test_timestamp}_{test_uuid}", "sql_file": "schema.sql"}
            ],
        }
    )
    
    return [postgres_fixture]

def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    from extract_test_configs import create_config_from_fixtures
    
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": Test_Configs.User_Input,
    }

def validate_test(model_result, fixtures=None):
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes the database task",
            "status": "running",
            "Result_Message": "Checking agent execution...",
        },
        {
            "name": "Database Validation",
            "description": "Verify database changes were made correctly",
            "status": "running", 
            "Result_Message": "Validating database state...",
        },
    ]
    
    try:
        # Basic agent execution check
        if model_result and model_result.get("status") != "failed":
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "✅ Agent executed successfully"
            
            # Database validation using fixtures
            postgres_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "postgres_resource"), None
            )
            
            if postgres_fixture:
                # Use fixture to validate database state
                connection = postgres_fixture.get_connection("example_db")
                cursor = connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM example_table")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    test_steps[1]["status"] = "passed"
                    test_steps[1]["Result_Message"] = f"✅ Found {count} records in database"
                else:
                    test_steps[1]["status"] = "failed"
                    test_steps[1]["Result_Message"] = "❌ No records found in database"
                    
                cursor.close()
                connection.close()
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = "❌ PostgreSQL fixture not available"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ Agent execution failed"
            
    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"
    
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
```

## Running Tests

### Individual Test
```bash
python run_braintrust_eval.py --filter "Test_Name" Ardent
```

### Multiple Tests  
```bash
python run_braintrust_eval.py --filter "PostgreSQL_Agent.*" Ardent
python run_braintrust_eval.py --filter "Airflow_Agent.*" Ardent
```

### All Tests
```bash
python run_braintrust_eval.py Ardent
```

## Test Discovery

Tests are automatically discovered by scanning the `Tests/` directory for:
1. A `Test_Configs.py` file with `User_Input`
2. A `test_*.py` file with `get_fixtures()`, `create_model_inputs()`, and `validate_test()` functions

Invalid tests are automatically excluded from execution.

## Troubleshooting

### Common Issues

1. **Path resolution errors**: Use absolute paths in fixtures
2. **Missing connection parameters**: Store `_connection_params` during setup
3. **Configuration key mismatches**: Ensure fixture config keys match `Configure_Model.py` expectations
4. **Resource cleanup issues**: Implement proper `teardown_resource()` methods
5. **Airflow GitHub branch names**: Airflow test names for the github_resource fixture should start with `test_airflow_`. This is naming convention is used to trigger redeploying the Astronomer cloud deployment.

### Debugging

Use the `--verbose` flag for detailed error information:
```bash
python run_braintrust_eval.py --filter "Test_Name" --verbose Ardent
```

---

This pattern ensures consistency, maintainability, and scalability across all DE-Bench tests while providing a clear framework for adding new tests and fixtures.
