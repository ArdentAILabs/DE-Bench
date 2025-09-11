# DE-Bench Test Creation Guide

This document explains how to create tests in the DE-Bench framework. The framework follows a standardized structure designed for testing data engineering agents with real-world scenarios.

## 📁 Directory Structure

Every test must follow this directory structure:

```
Tests/
├── Your_Test_Name/
│   ├── test_your_test_name.py      # Main test file
│   ├── Test_Configs.py             # Configuration and user input
│   ├── README.md                   # Test documentation (recommended)
│   └── Dockerfile                  # Optional: if custom Docker setup needed
```

### Required Files

1. **`test_*.py`** - Main test implementation
2. **`Test_Configs.py`** - Configuration and user query definition

### Optional Files

1. **`README.md`** - Test documentation (highly recommended)
2. **`Dockerfile`** - Custom Docker configuration if needed
3. **`docker-compose.yml`** - Multi-service Docker setup

## 🏗️ Test Architecture

DE-Bench uses a **standardized isolated test structure** to prevent resource conflicts:

### 🔐 **Standard Test Pattern** (Per-Test User Isolation)
- **Isolation**: Each test gets a unique Supabase user + API keys to prevent config clashing
- **Resource Separation**: Backend configs are stored per-user, so parallel tests don't interfere
- **Backend Integration**: Full integration with Ardent backend for realistic scenarios
- **Automatic Cleanup**: Users, API keys, and backend configurations cleaned automatically
- **Parallel Safe**: Multiple tests can run simultaneously without resource conflicts

## 🏗️ Standard Test Structure

All DE-Bench tests follow this **per-user isolation pattern** to prevent config conflicts:

## 🚀 Airflow Test Structure

Airflow tests follow a specialized pattern that integrates GitHub repository management with Airflow DAG deployment and execution. This structure supports the full CI/CD pipeline for Airflow DAGs.

### **Airflow Test Pattern (GitHub + Airflow Integration)**

```python
import importlib
import os
import pytest
import time
import uuid

from model.Configure_Model import cleanup_model_artifacts, set_up_model_configs
from model.Run_Model import run_model

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

@pytest.mark.airflow
@pytest.mark.pipeline
@pytest.mark.difficulty        # e.g., @pytest.mark.two
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"test_name_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_test_name_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
def test_airflow_function(request, airflow_resource, github_resource, supabase_account_resource):
    """Airflow test with GitHub integration and backend authentication."""
    
    # SECTION 1: SETUP THE TEST
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    
    # Add merge step to user input for GitHub workflow
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    # Define test-specific variables
    dag_name = "your_dag_name"
    pr_title = f"Add Your DAG {test_timestamp}_{test_uuid}"
    branch_name = f"feature/your-dag-{test_timestamp}_{test_uuid}"
    
    # Replace placeholders in user input
    Test_Configs.User_Input = Test_Configs.User_Input.replace("BRANCH_NAME", branch_name)
    Test_Configs.User_Input = Test_Configs.User_Input.replace("PR_NAME", pr_title)
    
    # Update GitHub secrets for deployment
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Define test steps for validation tracking
    test_steps = [
        {
            "name": "Checking Git Branch Existence",
            "description": "Checking if the git branch exists with the right name",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking PR Creation",
            "description": "Checking if the PR was created with the right name",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Checking DAG Results",
            "description": "Checking if the DAG produces the expected results",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    config_results = None
    custom_info = {"mode": request.config.getoption("--mode")}
    
    try:
        # Configure Airflow connection details from fixture
        Test_Configs.Configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        Test_Configs.Configs["services"]["airflow"]["username"] = airflow_resource["username"]
        Test_Configs.Configs["services"]["airflow"]["password"] = airflow_resource["password"]
        Test_Configs.Configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        
        # Add backend authentication if using Ardent mode
        if request.config.getoption("--mode") == "Ardent":
            custom_info["publicKey"] = supabase_account_resource["publicKey"]
            custom_info["secretKey"] = supabase_account_resource["secretKey"]

        # Set up model configurations
        config_results = set_up_model_configs(
            Configs=Test_Configs.Configs,
            custom_info=custom_info
        )

        custom_info = {
            **custom_info,
            **config_results,
        }

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None, 
            task=Test_Configs.User_Input, 
            configs=Test_Configs.Configs,
            extra_information=custom_info
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # Register Braintrust tracking
        if model_result:
            request.node.user_properties.append(("run_trace_id", model_result["bt_root_span_id"]))

        # SECTION 3: VERIFY GITHUB WORKFLOW
        # Wait for model to create branch and PR
        time.sleep(10)
        
        # Verify branch exists
        branch_exists, test_steps[0] = github_manager.verify_branch_exists(branch_name, test_steps[0])
        if not branch_exists:
            raise Exception(test_steps[0]["Result_Message"])

        # Find and merge PR
        pr_exists, test_steps[1] = github_manager.find_and_merge_pr(
            pr_title=pr_title, 
            test_step=test_steps[1], 
            commit_title=pr_title, 
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource["deployment_id"],
                "deploymentName": airflow_resource["deployment_name"],
            }
        )
        if not pr_exists:
            raise Exception("Unable to find and merge PR")

        # Wait for GitHub Action to complete
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("GitHub Action is not complete")
        
        # Wait for Airflow to redeploy
        airflow_instance = airflow_resource["airflow_instance"]
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully")

        # SECTION 4: VERIFY DAG EXECUTION
        # Verify DAG exists
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        # Trigger and monitor DAG
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor DAG completion
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 5: VALIDATE RESULTS
        # Get task logs and validate output
        logs = airflow_instance.get_task_instance_logs(
            dag_id=dag_name, 
            dag_run_id=dag_run_id, 
            task_id="your_task_name"
        )
        
        # Your validation logic here
        assert "expected_output" in logs, "Expected output not found in logs"
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "DAG produced expected results"

    finally:
        # CLEANUP
        try:
            if request.config.getoption("--mode") == "Ardent":
                custom_info['job_id'] = model_result.get("id") if model_result else None
            cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)
            github_manager.delete_branch(branch_name)
        except Exception as e:
            print(f"Error during cleanup: {e}")
```

### **Key Airflow Test Components**

1. **Dual Fixtures**: Both `airflow_resource` and `github_resource` are required
2. **GitHub Integration**: Automatic branch creation, PR management, and deployment
3. **Astro Integration**: Uses `ASTRO_ACCESS_TOKEN` for cloud deployment
4. **Dynamic Naming**: Unique branch names and PR titles for parallel execution
5. **Full CI/CD**: Tests the complete pipeline from code to deployment to execution

### **Airflow Test with Database Integration**

For tests that require database connectivity:

```python
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"test_name_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"test_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_with_database(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    # Access database connection
    created_db_name = postgres_resource["created_resources"][0]["name"]
    
    # Update configs with actual database name
    Test_Configs.Configs["services"]["postgreSQL"]["databases"][0]["name"] = created_db_name
    
    # Rest of test implementation...
```

```python
# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import pytest
import time

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

@pytest.mark.your_technology    # e.g., @pytest.mark.mongodb
@pytest.mark.your_category     # e.g., @pytest.mark.database
@pytest.mark.difficulty        # e.g., @pytest.mark.three
def test_your_function_name(request, resource_fixture, supabase_account_resource):
    """Your test description with backend authentication."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Step 1 Name", 
            "description": "What this step validates",
            "status": "did not reach",
            "Result_Message": "",
        },
        # Add more steps as needed
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    model_result = None  # Initialize before try block
    custom_info = {"mode": request.config.getoption("--mode")}
    if request.config.getoption("--mode") == "Ardent":
        custom_info["publicKey"] = supabase_account_resource["publicKey"]
        custom_info["secretKey"] = supabase_account_resource["secretKey"]
    
    config_results = set_up_model_configs(Configs=Test_Configs.Configs, custom_info=custom_info)
    custom_info = {
        **custom_info,
        **config_results,
    }

    try:
        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(
            container=None, 
            task=Test_Configs.User_Input, 
            configs=Test_Configs.Configs,
            extra_information=custom_info
        )
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))
        
        # Register the Braintrust root span ID for tracking (Ardent mode only)
        if model_result and "bt_root_span_id" in model_result:
            request.node.user_properties.append(("run_trace_id", model_result.get("bt_root_span_id")))
            print(f"Registered Braintrust root span ID: {model_result.get('bt_root_span_id')}")

        # SECTION 3: VERIFY THE OUTCOMES
        # Your validation logic here
        # Update test_steps with results
        
        # Example validation:
        if validation_passes:
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "Success message"
            assert True, "Test passed"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "Failure reason"
            raise AssertionError("Test failed because...")

    finally:
        # CLEANUP - Include mode-specific cleanup information
        if request.config.getoption("--mode") == "Ardent":
            custom_info['job_id'] = model_result.get("id") if model_result else None
        cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)
```

## 🎯 Execution Modes

DE-Bench supports multiple AI execution modes via the `--mode` flag:

### Available Modes

1. **Ardent** (default): Uses the Ardent backend with full Braintrust tracking
2. **Claude_Code**: Uses Claude Code CLI in Kubernetes containers  
3. **OpenAI_Codex**: Uses OpenAI Codex CLI (requires Azure OpenAI setup)

### Mode-Specific Behavior

**Ardent Mode:**
- Requires Supabase authentication (`publicKey`, `secretKey`)
- Provides Braintrust tracking with `bt_root_span_id`
- Full backend integration with job management

**Claude_Code Mode:**
- Runs in isolated Kubernetes pods
- No Supabase authentication required
- No Braintrust tracking (no `bt_root_span_id`)
- Automatic Kubernetes resource cleanup

**OpenAI_Codex Mode:**
- Runs in isolated Kubernetes pods
- Uses Azure OpenAI deployment
- No Supabase authentication required
- No Braintrust tracking (no `bt_root_span_id`)

### Running Tests with Different Modes

```bash
# Run with Ardent (default)
pytest Tests/MongoDB_Agent_Add_Record/test_mongodb_agent_add_record.py --mode=Ardent

# Run with Claude Code
pytest Tests/MongoDB_Agent_Add_Record/test_mongodb_agent_add_record.py --mode=Claude_Code

# Run with OpenAI Codex
pytest Tests/MongoDB_Agent_Add_Record/test_mongodb_agent_add_record.py --mode=OpenAI_Codex

# Run multiple tests in parallel with specific mode
pytest Tests/ -n auto --mode=Claude_Code
```

## 🧹 Cleanup and Job Management

### Mode-Aware Cleanup

The `cleanup_model_artifacts` function handles cleanup for all execution modes. The new pattern automatically manages mode-specific resources:

#### Required Pattern

```python
finally:
    # CLEANUP - Mode-aware cleanup
    if request.config.getoption("--mode") == "Ardent":
        custom_info['job_id'] = model_result.get("id") if model_result else None
    cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)
```

#### Key Components

1. **`custom_info`**: Contains all mode-specific information and configuration results
2. **Mode detection**: Uses `request.config.getoption("--mode")` to determine cleanup needs
3. **Ardent job_id**: Only added for Ardent mode from `model_result.get("id")`
4. **Error handling**: Cleanup should be in `finally` block to ensure it runs even if test fails

#### What Gets Cleaned Up by Mode

**Ardent Mode:**
- **Backend Jobs**: Active jobs are terminated and deleted from Ardent backend
- **Service Configurations**: Database connections, API keys, and other service configs
- **Supabase Resources**: User accounts and API keys

**Claude_Code Mode:**
- **Kubernetes Jobs**: Pods and jobs are deleted from AKS cluster
- **Service Configurations**: Database connections and other service configs
- **File Shares**: Azure file shares created for the session

**OpenAI_Codex Mode:**
- **Kubernetes Jobs**: Pods and jobs are deleted from AKS cluster  
- **Service Configurations**: Database connections and other service configs
- **File Shares**: Azure file shares created for the session

#### Complete Example with New Pattern

```python
def test_example_with_mode_support(request, mongo_resource, supabase_account_resource):
    """Example test showing the new mode-aware pattern."""
    
    # Initialize variables
    model_result = None
    custom_info = {"mode": request.config.getoption("--mode")}
    if request.config.getoption("--mode") == "Ardent":
        custom_info["publicKey"] = supabase_account_resource["publicKey"]
        custom_info["secretKey"] = supabase_account_resource["secretKey"]
    
    # Set up configurations and merge results
    config_results = set_up_model_configs(Configs=Test_Configs.Configs, custom_info=custom_info)
    custom_info = {
        **custom_info,
        **config_results,
    }

    try:
        # Run the model
        model_result = run_model(
            container=None,
            task=Test_Configs.User_Input,
            configs=Test_Configs.Configs,
            extra_information=custom_info
        )
        
        # Register Braintrust tracking (Ardent mode only)
        if model_result and "bt_root_span_id" in model_result:
            request.node.user_properties.append(("run_trace_id", model_result.get("bt_root_span_id")))
            print(f"Registered Braintrust root span ID: {model_result.get('bt_root_span_id')}")
        
        # Your test assertions here...
        
    finally:
        # Mode-aware cleanup
        if request.config.getoption("--mode") == "Ardent":
            custom_info['job_id'] = model_result.get("id") if model_result else None
        cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)
```

## ⚙️ Test_Configs.py Structure

The `Test_Configs.py` file defines the user query and system configuration:

```python
import os

# The task/query for the AI agent to solve
User_Input = """
Clear, specific instructions for the AI agent.
Include:
1. What to create/modify
2. Specific requirements
3. Expected outputs
4. Any naming conventions
"""

# System configuration for the test environment
Configs = {
    "services": {
        "service_name": {
            "param1": os.getenv("ENV_VAR_NAME"),
            "param2": "static_value",
            "nested_config": {
                "sub_param": os.getenv("ANOTHER_ENV_VAR")
            }
        }
    }
}
```

### Examples

**MongoDB Test Config:**
```python
import os

User_Input = "Go to test_collection in MongoDB and add another record. Please add the record with the name 'John Doe' and the age 30."

Configs = {
    "services": {
        "mongodb": {
            "connection_string": os.getenv("MONGODB_URI"),
            "databases": [
                {"name": "test_database", "collections": [{"name": "test_collection"}]}
            ],
        }
    }
}
```

**Airflow Test Config (Simple Pipeline):**
```python
import os

User_Input = """
Create a simple Airflow DAG that:
1. Prints "Hello World" to the logs
2. Runs daily at midnight
3. Has a single task named 'print_hello'
4. Name the DAG 'hello_world_dag'
5. Create it in a branch called 'BRANCH_NAME'
6. Name the PR 'PR_NAME'
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
            "host": os.getenv("AIRFLOW_HOST", "http://localhost:8080"),
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
        }
    }
}
```

**Airflow Test Config (With Database Integration):**
```python
import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Deduplicate users into a single user table called 'deduplicated_users'
2. Runs daily at midnight
3. Has a single task named 'deduplicate_users'
4. Name the DAG 'user_deduplication_dag'
5. Create it in a branch called 'BRANCH_NAME'
6. Name the PR 'PR_NAME'
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
            "host": os.getenv("AIRFLOW_HOST", "http://localhost:8080"),
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "user_data"}],  # Will be updated with actual database name from fixture
        }
    }
}
```

**Snowflake Test Config:**
```python
import os

User_Input = """
I need to add a new user record to the Snowflake users table.

The new user details are:
- Name: David Wilson
- Email: david.wilson@newuser.com  
- Age: 35
- City: Austin
- State: TX
- Active: True
- Initial purchases: 0.00

Please add this user to the USERS table in Snowflake and verify it was added successfully.
"""

Configs = {
    "services": {
        "snowflake": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "database": "TEST_DB",  # Will be overridden by fixture
            "schema": "TEST_SCHEMA"  # Will be overridden by fixture
        }
    }
}
```

## 🏷️ Test Markers

Use appropriate pytest markers to categorize your tests:

### Difficulty Markers (Required)
```python
@pytest.mark.one     # Difficulty 1 (easiest)
@pytest.mark.two     # Difficulty 2
# ... up to ...
@pytest.mark.ten     # Difficulty 10 (hardest)
```

### Technology Markers
```python
@pytest.mark.mongodb
@pytest.mark.airflow
@pytest.mark.postgres
@pytest.mark.mysql
@pytest.mark.snowflake
@pytest.mark.databricks
@pytest.mark.aws
@pytest.mark.s3
```

### Category Markers
```python
@pytest.mark.code_writing
@pytest.mark.database
@pytest.mark.pipeline
@pytest.mark.api_integration
@pytest.mark.environment_management
```

## 🔧 Fixtures and Resource Management

### Understanding Fixture Scopes

DE-Bench uses **three types of fixtures** based on resource lifecycle needs:

#### **📍 Per-Test Fixtures (`scope="function"`)** 
- **Creates fresh resources for EACH test**
- **Automatic cleanup after EACH test**
- **Use when**: Tests need isolation, modify data, or can't share safely

#### **🌍 Global/Shared Fixtures (`scope="session"`)** 
- **Creates ONE resource shared across ALL tests with same ID**
- **Cleanup only at session end**
- **Use when**: Resources are expensive, tests only read data, or sharing is safe

#### **🔐 Authentication Fixtures (`scope="function"`)** 
- **Creates unique user + API keys for EACH test**
- **Complete isolation between parallel tests**
- **Use when**: Tests require backend authentication via Ardent

### Available Fixtures

#### **Per-Test Fixtures (Function-Scoped)**

**MongoDB (Fresh per test):**
```python
@pytest.mark.parametrize("mongo_resource", [{
    "resource_id": "your_test_mongo_resource",
    "databases": [
        {
            "name": "test_database",
            "collections": [
                {
                    "name": "test_collection",
                    "data": []  # Initial data
                }
            ]
        }
    ]
}], indirect=True)
def test_mongodb_function(request, mongo_resource):
    # Each test gets fresh MongoDB collections
```

**Airflow (Fresh per test with GitHub integration):**
```python
@pytest.mark.airflow
@pytest.mark.pipeline
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"test_name_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_test_name_{test_timestamp}_{test_uuid}",
}], indirect=True)
def test_airflow_function(request, airflow_resource, github_resource, supabase_account_resource):
    # Each test gets its own Airflow instance + GitHub repository
    github_manager = github_resource["github_manager"]
    airflow_instance = airflow_resource["airflow_instance"]
    base_url = airflow_resource["base_url"]
    api_token = airflow_resource["api_token"]
```

**Snowflake (Fresh per test with S3 parquet loading):**

The Snowflake fixture provides isolated databases and schemas per test with scalable S3 parquet data loading. This pattern is ideal for testing with large datasets (millions of rows) loaded efficiently from S3.

```python
@pytest.mark.parametrize("snowflake_resource", [{
    "resource_id": "your_test_snowflake_resource",
    "database": "TEST_DB_12345_ABC",
    "schema": "TEST_SCHEMA_12345_ABC", 
    "sql_file": "users_schema.sql",  # SQL file with S3 loading commands
    "s3_config": {  # S3 configuration for data loading
        "bucket_url": "s3://de-bench/",
        "s3_key": "v1/users/users_simple_20250901_233609.parquet",
        "aws_key_id": "env:AWS_ACCESS_KEY",
        "aws_secret_key": "env:AWS_SECRET_KEY"
    }
}], indirect=True)
def test_snowflake_function(request, snowflake_resource):
    # Each test gets fresh Snowflake database + schema with S3 data
    connection = snowflake_resource["connection"]
    database = snowflake_resource["database"]
    schema = snowflake_resource["schema"]
```

#### **Snowflake SQL File Patterns**

**Key Features:**
- **Variable substitution**: Use `{{VAR}}` placeholders for dynamic values
- **S3 parquet loading**: Load large datasets efficiently from S3
- **Multi-statement support**: Execute complex setup scripts
- **Automatic cleanup**: Database/schema dropped after test
- **Data type handling**: Proper mapping for timestamps, decimals, booleans

**Required Environment Variables:**
```bash
# Snowflake connection
SNOWFLAKE_ACCOUNT=xy12345.us-west-2
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN

# AWS S3 access
AWS_ACCESS_KEY=AKIA...
AWS_SECRET_KEY=...
```

#### **Writing Snowflake SQL Files**

**1. File Structure Pattern:**
```sql
-- filename: users_schema.sql
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}
-- S3 variables: {{BUCKET_URL}}, {{S3_KEY}}, {{AWS_ACCESS_KEY}}, {{AWS_SECRET_KEY}}

-- Step 1: Set context
USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Step 2: Create file format
CREATE OR REPLACE FILE FORMAT PARQUET_STD TYPE=PARQUET;

-- Step 3: Create S3 stage
CREATE OR REPLACE TEMP STAGE _temp_stage
  URL='{{BUCKET_URL}}'
  CREDENTIALS=(AWS_KEY_ID='{{AWS_ACCESS_KEY}}' AWS_SECRET_KEY='{{AWS_SECRET_KEY}}');

-- Step 4: Create table with explicit schema
CREATE OR REPLACE TABLE USERS (
    USER_ID NUMBER,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    AGE NUMBER,
    CITY VARCHAR(100),
    STATE VARCHAR(2),
    SIGNUP_DATE TIMESTAMP,  -- Use TIMESTAMP for parquet date fields
    IS_ACTIVE BOOLEAN,
    TOTAL_PURCHASES DECIMAL(10,2)
);

-- Step 5: Load data from S3
COPY INTO USERS
FROM @_temp_stage
FILES = ('{{S3_KEY}}')
FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE  -- Required for parquet
ON_ERROR=ABORT_STATEMENT;

-- Step 6: Verify data loaded
SELECT COUNT(*) AS total_users FROM USERS;
```

**2. Data Type Best Practices:**

| Parquet Type | Snowflake Type | Notes |
|--------------|----------------|-------|
| `int64` | `NUMBER` | Use for IDs, counts |
| `string` | `VARCHAR(N)` | Specify appropriate length |
| `timestamp[ns]` | `TIMESTAMP` | **Not DATE** - parquet stores nanoseconds |
| `bool` | `BOOLEAN` | Direct mapping |
| `double` | `DECIMAL(10,2)` | Use DECIMAL for currency |

**3. S3 Loading Requirements:**

```sql
-- ✅ CORRECT: Use MATCH_BY_COLUMN_NAME for parquet files
COPY INTO USERS
FROM @_temp_stage
FILES = ('{{S3_KEY}}')
FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE  -- Essential for multi-column parquet
ON_ERROR=ABORT_STATEMENT;

-- ❌ WRONG: Missing MATCH_BY_COLUMN_NAME will cause errors
COPY INTO USERS
FROM @_temp_stage
FILES = ('{{S3_KEY}}')
FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
ON_ERROR=ABORT_STATEMENT;
```

**4. Variable Substitution Rules:**

```sql
-- Available variables (automatically substituted):
{{DB}}              -- Database name (e.g., BENCH_DB_1756838228_0FD5CA2C)
{{SCHEMA}}          -- Schema name (e.g., TEST_SCHEMA_1756838228_0FD5CA2C)
{{WAREHOUSE}}       -- Warehouse name (from SNOWFLAKE_WAREHOUSE)
{{ROLE}}            -- Role name (from SNOWFLAKE_ROLE, default: SYSADMIN)
{{BUCKET_URL}}      -- S3 bucket URL (e.g., s3://de-bench/)
{{S3_KEY}}          -- S3 object key (e.g., v1/users/file.parquet)
{{AWS_ACCESS_KEY}}  -- AWS access key (from AWS_ACCESS_KEY env var)
{{AWS_SECRET_KEY}}  -- AWS secret key (from AWS_SECRET_KEY env var)
```

**5. Common Patterns:**

```sql
-- Pattern 1: Simple table with S3 data
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID NUMBER,
    NAME VARCHAR(200),
    PRICE DECIMAL(10,2),
    CREATED_AT TIMESTAMP
);

COPY INTO PRODUCTS
FROM @_temp_stage
FILES = ('{{S3_KEY}}')
FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
ON_ERROR=ABORT_STATEMENT;

-- Pattern 2: Multiple tables from different S3 files
-- Note: Use separate test configs for multiple files
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID NUMBER PRIMARY KEY,
    USER_ID NUMBER,
    TOTAL DECIMAL(10,2),
    ORDER_DATE TIMESTAMP
);

-- Pattern 3: Data transformation during load
CREATE OR REPLACE TABLE USERS_CLEAN AS
SELECT 
    USER_ID,
    UPPER(FIRST_NAME) AS FIRST_NAME,
    LOWER(EMAIL) AS EMAIL,
    SIGNUP_DATE::DATE AS SIGNUP_DATE  -- Convert timestamp to date if needed
FROM USERS;
```

**6. Error Handling:**

```sql
-- Use ON_ERROR=ABORT_STATEMENT for tests (fail fast)
ON_ERROR=ABORT_STATEMENT;

-- For production, consider:
-- ON_ERROR=CONTINUE;  -- Skip bad records
-- ON_ERROR=SKIP_FILE; -- Skip entire file on error
```

#### **S3 Data Preparation**

**Creating Test Data:**
```python
# Example: Generate parquet test data
import pandas as pd
from datetime import datetime

users_data = {
    "user_id": [1, 2, 3, 4, 5],
    "first_name": ["Alice", "Bob", "Carol", "David", "Eve"],
    "last_name": ["Smith", "Jones", "Brown", "Wilson", "Davis"],
    "email": ["alice@example.com", "bob@example.com", "carol@example.com", 
              "david@example.com", "eve@example.com"],
    "age": [25, 32, 28, 45, 31],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "state": ["NY", "CA", "IL", "TX", "AZ"],
    "signup_date": pd.to_datetime(["2024-01-15", "2024-02-20", "2024-03-10", 
                                   "2024-01-05", "2024-04-12"]),
    "is_active": [True, True, False, True, True],
    "total_purchases": [150.50, 89.99, 0.00, 245.75, 67.25]
}

df = pd.DataFrame(users_data)
df.to_parquet("users_simple_20250901_233609.parquet", index=False)
```

**Upload to S3:**
```bash
# Upload parquet file to S3
aws s3 cp users_simple_20250901_233609.parquet s3://de-bench/v1/users_simple_20250901_233609.parquet --sse AES256

# Verify upload
aws s3 ls s3://de-bench/v1/users_simple_20250901_233609.parquet
```

**S3 Path Structure:**
```
s3://de-bench/
├── v1/
│   ├── users_simple_20250901_233609.parquet
│   ├── products_sample_20250901_234500.parquet
│   └── orders_test_20250901_235000.parquet
└── v2/
    └── enhanced_datasets/
```

#### **Complete Test Example**

**Directory Structure:**
```
Tests/Snowflake_Agent_Add_Record/
├── test_snowflake_agent_add_record.py
├── Test_Configs.py
└── users_schema.sql
```

**Test Configuration:**
```python
@pytest.mark.snowflake
@pytest.mark.database
@pytest.mark.two
@pytest.mark.parametrize("snowflake_resource", [{
    "resource_id": f"snowflake_test_{test_timestamp}_{test_uuid}",
    "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
    "schema": f"TEST_SCHEMA_{test_timestamp}_{test_uuid}",
    "sql_file": "users_schema.sql",
    "s3_config": {
        "bucket_url": "s3://de-bench/",
        "s3_key": "v1/users/users_simple_20250901_233609.parquet",
        "aws_key_id": "env:AWS_ACCESS_KEY",
        "aws_secret_key": "env:AWS_SECRET_KEY"
    }
}], indirect=True)
def test_snowflake_agent_add_record(request, snowflake_resource, supabase_account_resource):
    # Test implementation here
    connection = snowflake_resource["connection"]
    database = snowflake_resource["database"]
    schema = snowflake_resource["schema"]
    
    # Verify initial data loaded from S3
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.USERS")
    initial_count = cursor.fetchone()[0]
    assert initial_count > 0, "S3 data should be loaded"
```

#### **Troubleshooting Common Issues**

**1. S3 File Not Found:**
```
Error: Remote file 's3://de-bench/v1/file.parquet' was not found
```
- Verify S3 path: `aws s3 ls s3://de-bench/v1/file.parquet`
- Check AWS credentials are set correctly
- Ensure `bucket_url` and `s3_key` combine correctly

**2. Data Type Mismatch:**
```
Error: Failed to cast variant value 1705276800000000000 to DATE
```
- Use `TIMESTAMP` instead of `DATE` for parquet timestamp fields
- Check parquet schema: `pd.read_parquet("file.parquet").dtypes`

**3. Parquet Column Mapping:**
```
Error: PARQUET file format can produce one and only one column
```
- Add `MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE` to COPY statement
- Ensure table schema matches parquet columns

**4. AWS Permissions:**
```
Error: Access Denied
```
- Verify AWS credentials have S3 read permissions
- Check bucket policy allows Snowflake access
- Test with: `aws s3 ls s3://de-bench/`

#### **Authentication Fixtures (Function-Scoped)**

**Supabase Account Resource (Mode-Aware):**
```python
def test_with_backend_auth(request, mongo_resource, supabase_account_resource):
    # Mode-aware authentication setup
    custom_info = {"mode": request.config.getoption("--mode")}
    if request.config.getoption("--mode") == "Ardent":
        # Each test gets unique Supabase user + API keys for Ardent mode
        custom_info["publicKey"] = supabase_account_resource["publicKey"]    # e.g., "ardent_pk_..."
        custom_info["secretKey"] = supabase_account_resource["secretKey"]    # e.g., "ardent_sk_..."  
        user_id = supabase_account_resource["userID"]                        # Unique user ID
    
    # For Claude_Code and OpenAI_Codex modes, no Supabase authentication needed
    # Automatic cleanup handles mode-specific resource deletion
```

**Key Benefits:**
- ✅ **Complete isolation**: Each test gets its own user account
- ✅ **Parallel-safe**: No conflicts between concurrent tests  
- ✅ **Automatic cleanup**: Users and API keys automatically deleted
- ✅ **Secure**: Fresh credentials per test, no shared secrets

#### **Shared Fixtures (Session-Scoped)**

**Shared Resource (Multiple tests share same instance):**
```python
@pytest.mark.parametrize("shared_resource", ["shared_setup_id"], indirect=True)
def test_read_operation_1(request, shared_resource):
    # Uses shared resource with ID "shared_setup_id"
    pass

@pytest.mark.parametrize("shared_resource", ["shared_setup_id"], indirect=True)  
def test_read_operation_2(request, shared_resource):
    # Reuses SAME resource as test_read_operation_1
    pass

@pytest.mark.parametrize("shared_resource", ["different_setup_id"], indirect=True)
def test_isolated_operation(request, shared_resource):
    # Gets separate resource with ID "different_setup_id"
    pass
```

### Choosing the Right Fixture Type

| Scenario | Use Fixture Type | Reason |
|----------|------------------|---------|
| Tests modify/delete data | **Per-Test** (`mongo_resource`) | Need clean slate each test |
| Tests only read data | **Shared** (`shared_resource`) | Safe to share, faster execution |
| Expensive resource setup | **Shared** (`shared_resource`) | Amortize cost across tests |
| Test isolation critical | **Per-Test** (`mongo_resource`) | Prevent test interference |
| Multiple similar tests | **Shared** (`shared_resource`) | Resource efficiency |
| **Backend authentication needed** | **Auth** (`supabase_account_resource`) | **Secure, isolated API access** |
| **Parallel test execution** | **Auth** (`supabase_account_resource`) | **Prevent credential conflicts** |



**Step 2: Will the test modify/delete data?**
- ✅ **YES** → Use **Per-Test Fixtures** (`mongo_resource`, `airflow_resource`)
- ❌ **NO** → Continue to Step 3

**Step 3: Is resource setup expensive (>10 seconds)?**
- ✅ **YES** → Use **Shared Fixtures** (`shared_resource` with descriptive ID)
- ❌ **NO** → Use **Per-Test Fixtures** for simplicity

## ✅ Validation Patterns

### Multi-Layer Validation
Implement multiple validation layers for robust testing:

```python
# SECTION 3: VERIFY THE OUTCOMES
validation_passed = False

try:
    # Layer 1: Basic existence checks
    if basic_resource_exists():
        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "Resource created successfully"
        
        # Layer 2: Content validation
        if content_is_correct():
            test_steps[1]["status"] = "passed" 
            test_steps[1]["Result_Message"] = "Content validation passed"
            
            # Layer 3: Functional validation
            if functionality_works():
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = "Functional test passed"
                validation_passed = True
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "Functionality test failed"
        else:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = "Content validation failed"
    else:
        test_steps[0]["status"] = "failed"
        test_steps[0]["Result_Message"] = "Resource creation failed"

    if validation_passed:
        assert True, "All validations passed"
    else:
        raise AssertionError("One or more validations failed")

except Exception as e:
    # Update any remaining test steps
    for step in test_steps:
        if step["status"] == "did not reach":
            step["status"] = "failed"
            step["Result_Message"] = f"Exception occurred: {str(e)}"
    raise
```

### Test Isolation
Ensure each test run is completely isolated:

```python
import uuid
from datetime import datetime

# Generate unique identifiers
test_id = f"{int(datetime.now().timestamp())}_{str(uuid.uuid4())[:8]}"
unique_name = f"test_resource_{test_id}"

# Use in your test
User_Input = f"Create a resource named '{unique_name}' with..."
```

## 🌍 Environment Variables

All DE-Bench tests require these environment variables in your test's README:

```markdown
## Environment Requirements

### Core Framework Variables (Required for all tests)
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY`: Supabase service role key (for user management)
- `SUPABASE_JWT_SECRET`: JWT secret for token generation
- `ARDENT_BASE_URL`: Your Ardent backend base URL (Ardent mode only)

### Execution Mode Variables

#### Claude_Code Mode (Kubernetes + AWS)
- `AWS_ACCESS_KEY_ID_CLAUDE`: AWS access key for Claude Code
- `AWS_SECRET_ACCESS_KEY_CLAUDE`: AWS secret key for Claude Code
- `AWS_REGION_CLAUDE`: AWS region for Claude Code
- `CLAUDE_CODE_USE_BEDROCK`: Set to "true" to use AWS Bedrock
- `IS_SANDBOX`: Set to "1" for non-interactive execution
- `AZURE_CLIENT_ID`: Azure service principal client ID
- `AZURE_CLIENT_SECRET`: Azure service principal client secret
- `AZURE_TENANT_ID`: Azure tenant ID
- `AZURE_SUBSCRIPTION_ID`: Azure subscription ID
- `ACI_RESOURCE_GROUP`: Azure resource group name
- `AKS_CLUSTER_NAME`: Azure Kubernetes Service cluster name

#### OpenAI_Codex Mode (Kubernetes + Azure OpenAI)
- `OPENAI_API_KEY`: OpenAI API key (if using OpenAI directly)
- `AZURE_OPENAI_ENDPOINT`: Azure OpenAI endpoint URL
- `AZURE_OPENAI_API_VERSION`: Azure OpenAI API version
- `AZURE_OPENAI_CHAT_DEPLOYMENT_NAME`: Azure OpenAI deployment name
- `AZURE_CLIENT_ID`: Azure service principal client ID (same as Claude_Code)
- `AZURE_CLIENT_SECRET`: Azure service principal client secret (same as Claude_Code)
- `AZURE_TENANT_ID`: Azure tenant ID (same as Claude_Code)
- `AZURE_SUBSCRIPTION_ID`: Azure subscription ID (same as Claude_Code)
- `ACI_RESOURCE_GROUP`: Azure resource group name (same as Claude_Code)
- `AKS_CLUSTER_NAME`: Azure Kubernetes Service cluster name (same as Claude_Code)

### Service-Specific Variables

#### Snowflake Tests
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., abc12345.us-west-2)
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse name
- `SNOWFLAKE_ROLE`: Snowflake role (optional, defaults to SYSADMIN)

#### MongoDB Tests  
- `MONGODB_URI`: MongoDB connection string

#### PostgreSQL Tests
- `POSTGRES_HOSTNAME`: PostgreSQL host
- `POSTGRES_PORT`: PostgreSQL port
- `POSTGRES_USERNAME`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password

#### Airflow Tests
- `AIRFLOW_GITHUB_TOKEN`: GitHub token for DAG management
- `AIRFLOW_REPO`: GitHub repository URL
- `AIRFLOW_DAG_PATH`: Path to DAGs in repository
- `AIRFLOW_HOST`: Airflow webserver URL
- `AIRFLOW_USERNAME`: Airflow username
- `AIRFLOW_PASSWORD`: Airflow password
- `AIRFLOW_API_TOKEN`: Airflow API token for programmatic access
```

Add them to the main project's `.env` template in the root README.

## 📝 Test Documentation (README.md)

Create a README for each test with this structure:

```markdown
# Your Test Name

Brief description of what this test validates.

## Test Overview

The test:
1. Step 1 description
2. Step 2 description
3. Step 3 description

## Validation

The test validates:
- ✅ Criterion 1
- ✅ Criterion 2  
- ✅ Criterion 3

## Running the Test

```bash
pytest Tests/Your_Test_Name/test_your_test_name.py -v
```

## Environment Requirements

- `ENV_VAR_1`: Description
- `ENV_VAR_2`: Description

## What This Test Validates

1. **Agent Capability**: Specific capability being tested
2. **Technical Integration**: What systems are integrated
3. **Real-world Scenario**: What real problem this represents
```

## 🚀 Running Tests

### Basic Test Execution

```bash
# Run specific test with default mode (Ardent)
pytest Tests/Your_Test_Name/test_your_test_name.py -v

# Run specific test with Claude Code
pytest Tests/Your_Test_Name/test_your_test_name.py -v --mode=Claude_Code

# Run specific test with OpenAI Codex
pytest Tests/Your_Test_Name/test_your_test_name.py -v --mode=OpenAI_Codex
```

### Parallel Execution with Modes

```bash
# Run all tests in parallel with Ardent mode
pytest -n auto -sv --mode=Ardent

# Run all tests in parallel with Claude Code
pytest -n auto -sv --mode=Claude_Code

# Run all tests in parallel with OpenAI Codex
pytest -n auto -sv --mode=OpenAI_Codex
```

### Filtering Tests

```bash
# Run by markers with specific mode
pytest -m "mongodb and database" -v --mode=Claude_Code

# Run by difficulty with specific mode
pytest -m "three" -v --mode=Ardent

# Run with keywords and mode
pytest -k "your_keyword" -v --mode=OpenAI_Codex

# Run multiple specific tests with mode
pytest Tests/MongoDB_Agent_Add_Record/ Tests/PostgreSQL_Agent_Add_Record/ -v --mode=Claude_Code
```

## ✨ Best Practices

### 1. **Clear User Instructions**
- Write specific, actionable instructions in `User_Input`
- Include expected outputs and naming conventions
- Break complex tasks into numbered steps

### 2. **Robust Validation**
- Implement multiple validation layers
- Check both existence and correctness
- Provide clear success/failure messages

### 3. **Proper Resource Management**
- Always use `try/finally` blocks
- Clean up resources even if test fails
- Use appropriate fixtures for complex resources

### 4. **Test Isolation**
- Use unique identifiers for each test run
- Don't depend on previous test state
- Clean up completely between runs

### 5. **Error Handling**
- Catch and log meaningful errors
- Update `test_steps` with failure reasons
- Provide debugging information

### 6. **Documentation**
- Include a README for each test
- Document environment requirements
- Explain validation criteria clearly

## 🚀 Airflow-Specific Best Practices

### 1. **GitHub Integration**
- Always use unique branch names with timestamps to prevent conflicts
- Include `BRANCH_NAME` and `PR_NAME` placeholders in `User_Input` for dynamic replacement
- Use descriptive PR titles that include the test timestamp for easy identification
- Ensure GitHub secrets are properly configured with `ASTRO_ACCESS_TOKEN`

### 2. **DAG Development**
- Use clear, descriptive DAG names that match the test purpose
- Include specific task names that can be easily validated in logs
- Set appropriate scheduling (daily at midnight is common for tests)
- Ensure DAGs are idempotent and can run multiple times safely

### 3. **Test Validation Strategy**
- **Layer 1**: Verify GitHub branch creation and PR management
- **Layer 2**: Verify DAG appears in Airflow after deployment
- **Layer 3**: Verify DAG execution and task completion
- **Layer 4**: Verify specific output in task logs or database changes

### 4. **Timing and Synchronization**
- Add appropriate wait times for GitHub Actions to complete (typically 10+ seconds)
- Wait for Airflow to redeploy after PR merge before triggering DAGs
- Use `wait_for_airflow_to_be_ready()` to ensure deployment completion
- Monitor DAG runs with proper timeout handling

### 5. **Database Integration**
- For database-connected DAGs, use PostgreSQL fixtures with unique database names
- Update configuration with actual database names from fixtures
- Include SQL schema files for database setup
- Validate both DAG execution and database state changes

### 6. **Error Recovery**
- Implement comprehensive cleanup in `finally` blocks
- Delete GitHub branches after test completion
- Handle partial failures gracefully with detailed error messages
- Log deployment and execution details for debugging

### 7. **Parallel Execution**
- Use unique resource IDs with timestamps and UUIDs
- Ensure no shared state between concurrent test runs
- Test branch names and PR titles must be globally unique
- Database names and schemas should be isolated per test

### 8. **Environment Configuration**
- Use `api_token` for programmatic Airflow access
- Configure both local and cloud deployment options
- Ensure all required environment variables are documented

## 🔧 Airflow Troubleshooting

### Common Issues and Solutions

**1. DAG Not Appearing in Airflow**
```
Error: DAG 'your_dag_name' did not appear in Airflow
```
- **Cause**: GitHub Action deployment failed or incomplete
- **Solution**: Check GitHub Action logs, verify `ASTRO_ACCESS_TOKEN`, ensure DAG syntax is correct

**2. GitHub Action Not Completing**
```
Error: GitHub Action is not complete
```
- **Cause**: Deployment taking longer than expected
- **Solution**: Increase wait time, check GitHub Action status, verify repository permissions

**3. DAG Execution Fails**
```
Error: Failed to trigger DAG
```
- **Cause**: DAG has syntax errors or missing dependencies
- **Solution**: Check DAG logs, verify requirements.txt, if possible: test DAG locally first

**4. Task Logs Not Found**
```
Error: Expected output not found in logs
```
- **Cause**: Task failed or output format changed
- **Solution**: Check task status, verify log content, adjust validation logic

**5. Database Connection Issues**
```
Error: Database connection failed
```
- **Cause**: Database not ready or connection parameters incorrect
- **Solution**: Verify database fixture setup, check connection parameters, ensure database is accessible

**6. Branch/PR Conflicts**
```
Error: Branch already exists
```
- **Cause**: Non-unique branch names in parallel execution
- **Solution**: Use timestamps and UUIDs in branch names, ensure proper cleanup

### Debugging Tips

1. **Enable Verbose Logging**: Add print statements to track execution flow
2. **Check GitHub Actions**: Monitor deployment progress in GitHub repository
3. **Verify Airflow UI**: Check DAG status and logs in Airflow web interface
4. **Test Incrementally**: Start with simple DAGs before complex integrations
5. **Use Local Testing**: Test DAGs locally before running full integration tests

## 📊 Common Patterns

### Database Tests
```python
# Validate data was created
record = collection.find_one({"name": "John Doe"})
assert record is not None, "Record not found"
assert record["age"] == 30, "Age doesn't match"
```

### API Tests  
```python
# Validate API response
response = requests.get(f"{api_url}/endpoint", headers=headers)
assert response.status_code == 200, f"API call failed: {response.status_code}"
data = response.json()
assert "expected_field" in data, "Response missing expected field"
```

### File/Resource Tests
```python
# Validate file/resource exists
assert os.path.exists(expected_file_path), "Expected file not created"
with open(expected_file_path) as f:
    content = f.read()
    assert "expected_content" in content, "File content incorrect"
```

## 🎯 LLM Auto-Generation Guidelines

When using this guide to auto-generate tests, follow these key principles:

### Required Components for Every Test:
1. **Directory Structure**: Create `Tests/TestName/` with required files
2. **Test Markers**: Always include difficulty + technology + category markers
3. **Standard Imports**: Use the exact import pattern shown
4. **Three Sections**: Setup → Run Model → Verify Outcomes
5. **Test Steps Tracking**: Define and update `test_steps` list
6. **Resource Cleanup**: Always use try/finally blocks

### Fixture Selection Decision Tree:

**Step 1: User Isolation (Required for all tests)**
- ✅ **Always use** `supabase_account_resource` with `{"useArdent": True}` to prevent config clashing

**Step 2: Will the test modify/delete data?**
- ✅ **YES** → Use **Per-Test Fixtures** (`mongo_resource`, `airflow_resource`)
- ❌ **NO** → Continue to Step 3

**Step 3: Is resource setup expensive (>10 seconds)?**
- ✅ **YES** → Use **Shared Fixtures** (`shared_resource` with descriptive ID)
- ❌ **NO** → Use **Per-Test Fixtures** for simplicity

**Step 4: Will multiple similar tests benefit from sharing?**
- ✅ **YES** → Use **Shared Fixtures** (`shared_resource` with same ID)
- ❌ **NO** → Use **Per-Test Fixtures**

### Additional Fixture Usage Patterns:

**For Per-Test (Isolated) Resources:**
```python
@pytest.mark.parametrize("mongo_resource", [{
    "resource_id": "unique_test_id",
    # ... configuration
}], indirect=True)
def test_function(request, mongo_resource):
    # Each test gets fresh, isolated resource
```

**For Airflow Tests (GitHub + Airflow Integration):**
```python
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"test_name_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_test_name_{test_timestamp}_{test_uuid}",
}], indirect=True)
def test_airflow_function(request, airflow_resource, github_resource, supabase_account_resource):
    # Each test gets fresh Airflow instance + GitHub repository
```

**For Shared Resources:**
```python
@pytest.mark.parametrize("shared_resource", ["meaningful_shared_id"], indirect=True)
def test_function(request, shared_resource):
    # Multiple tests with same ID share the resource
```

### Naming Conventions:

**Directory Structure:**
- Pattern: `Technology_Agent/Chat_Source_Destination_Task` or `Technology_Agent_Task`
- Examples: 
  - `PostgreSQL_Agent_Add_Record/` (simple task)
  - `MongoDB_Agent_Add_Record/` (simple task)
  - `Snowflake_Agent_Add_Record/` (simple task)
  - `Airflow_Agent_Simple_Pipeline/` (simple DAG creation)
  - `Airflow_Agent_Hello_Universe_Pipeline/` (simple DAG with specific output)
  - `Airflow_Agent_Pandas_Pipeline/` (DAG with pandas processing)
  - `Airflow_Agent_Data_Deduplication/` (DAG with database operations)
  - `Airflow_Agent_Sales_Fact_Table/` (DAG with fact table creation)
  - `Airflow_Agent_Amazon_SP_API_To_PostgreSQL/` (source → destination)
  - `Airflow_Agent_PostgreSQL_To_MySQL/` (source → destination)
  - `Airflow_Agent_PostgreSQL_To_Snowflake_Workflow_Analytics/` (source → destination with analytics)
  - `Airflow_Agent_Enterprise_Data_Platform/` (complex multi-step pipeline)
  - `PostgreSQL_Agent_Denormalized_Normalized_ManyToMany/` (schema transformation)
  - `Databricks_Hello_World/` (simple task)

**File Naming:**
- Main test file: `test_` + lowercase directory name with underscores
- Config file: `Test_Configs.py` (exact capitalization)
- Examples:
  - Directory: `PostgreSQL_Agent_Add_Record/` → File: `test_postgresql_agent_add_record.py`
  - Directory: `PostgreSQL_Agent_Denormalized_Normalized_ManyToMany/` → File: `test_postgresql_agent_denormalized_normalized_many_to_many.py`

**Function Naming:**
- Test function: `test_` + lowercase description with underscores
- Examples:
  - `test_postgresql_agent_add_record()`
  - `test_postgresql_agent_denormalized_normalized_many_to_many()`

**Resource and Variable Naming:**
- Resource fixture parameter: Choose based on lifecycle needs:
  - `mongo_resource` (per-test isolation)
  - `postgres_resource` (per-test isolation)
  - `airflow_resource` (per-test isolation)
  - `github_resource` (per-test isolation for tests)
  - `shared_resource` (cross-test sharing)
- Airflow-specific variables:
  - `dag_name`: The name of the DAG being created (e.g., "hello_world_dag")
  - `pr_title`: Unique PR title with timestamp (e.g., "Add Hello World DAG 1234567890_ABC12345")
  - `branch_name`: Unique branch name with timestamp (e.g., "feature/hello_world_dag-1234567890_ABC12345")
  - `test_timestamp`: Unix timestamp for uniqueness
  - `test_uuid`: Short UUID for additional uniqueness
- Shared resource IDs: Use descriptive names like `"read_only_mongo_setup"`, `"shared_test_database"`
- Test steps: Descriptive names that clearly indicate what's being validated

### Configuration Patterns:
- Always use `os.getenv()` for environment variables
- Structure configs under `"services"` key
- Include all necessary connection parameters
- Use meaningful default values where appropriate

### Validation Requirements:
- Implement multiple validation layers (existence, content, functionality)
- Update `test_steps` status and messages
- Use descriptive assertion messages
- Handle exceptions gracefully

### Resource Lifecycle Awareness:
- **Per-Test fixtures**: Can safely modify data, each test starts clean
- **Shared fixtures**: Should only read data or perform non-destructive operations  
- **Isolation fixtures**: Each test gets unique user + API keys to prevent config conflicts, automatic cleanup
- **Resource cleanup**: Per-test fixtures clean automatically; shared fixtures clean at session end; isolation fixtures clean users and backend configs



This guide provides the foundation for creating consistent, reliable tests in the DE-Bench framework. Follow these patterns to ensure your tests integrate seamlessly with the existing test suite and provide meaningful validation of agent capabilities. 