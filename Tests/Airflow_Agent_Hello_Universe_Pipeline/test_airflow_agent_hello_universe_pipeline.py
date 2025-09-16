# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import requests
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a Hello Universe DAG pipeline.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture

    # Initialize Airflow fixture with test-specific configuration
    custom_airflow_config = {
        "resource_id": f"hello_universe_pipeline_test_{test_timestamp}_{test_uuid}",
        "runtime_version": "13.1.0",
        "scheduler_size": "small",
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    return [airflow_fixture]


def create_config(fixtures: List[DEBenchFixture]) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    This function has access to all fixture data after setup.
    """
    from extract_test_configs import create_config_from_fixtures

    # Use the helper to automatically create config from all fixtures
    return create_config_from_fixtures(fixtures)


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created a Hello Universe Airflow DAG.

    Expected behavior:
    - DAG should be created with name "hello_universe_dag"
    - DAG should have basic tasks for saying hello to the universe
    - DAG should be accessible via the Airflow API

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to create Hello Universe DAG",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Airflow DAG creation task...",
        },
        {
            "name": "DAG Creation Validation",
            "description": "Verify that hello_universe_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that Hello Universe DAG exists in Airflow...",
        },
        {
            "name": "DAG Structure Validation",
            "description": "Verify that the DAG has the expected tasks and structure",
            "status": "running",
            "Result_Message": "Validating DAG structure and tasks...",
        },
    ]

    overall_success = False

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"success": False, "test_steps": test_steps}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # Use fixture to get Airflow connection for validation
        airflow_fixture = None
        if fixtures:
            airflow_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "airflow_resource"),
                None,
            )

        if not airflow_fixture:
            raise Exception("Airflow fixture not found")

        # Get Airflow instance from stored resource data
        resource_data = getattr(airflow_fixture, "_resource_data", None)
        if not resource_data:
            raise Exception("Airflow resource data not available")

        airflow_instance = resource_data["airflow_instance"]
        api_headers = resource_data["api_headers"]
        base_url = resource_data["base_url"]

        # Step 2: Verify that hello_universe_dag was created
        dag_name = "hello_universe_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}")

        try:
            # Use Airflow REST API to check if DAG exists
            dag_url = f"{base_url}/api/v1/dags/{dag_name}"
            response = requests.get(dag_url, headers=api_headers, timeout=30)

            if response.status_code == 200:
                dag_info = response.json()
                test_steps[1]["status"] = "passed"
                test_steps[1][
                    "Result_Message"
                ] = f"✅ DAG '{dag_name}' found in Airflow: {dag_info.get('dag_id', 'N/A')}"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ DAG '{dag_name}' not found in Airflow (HTTP {response.status_code})"
                return {"success": False, "test_steps": test_steps}

        except Exception as e:
            test_steps[1]["status"] = "failed"
            test_steps[1][
                "Result_Message"
            ] = f"❌ Error checking DAG existence: {str(e)}"
            return {"success": False, "test_steps": test_steps}

        # Step 3: Verify DAG structure and tasks
        try:
            # Get DAG tasks
            tasks_url = f"{base_url}/api/v1/dags/{dag_name}/tasks"
            tasks_response = requests.get(tasks_url, headers=api_headers, timeout=30)

            if tasks_response.status_code == 200:
                tasks_data = tasks_response.json()
                tasks = tasks_data.get("tasks", [])

                if len(tasks) > 0:
                    task_names = [task.get("task_id", "unknown") for task in tasks]
                    test_steps[2]["status"] = "passed"
                    test_steps[2][
                        "Result_Message"
                    ] = f"✅ DAG has {len(tasks)} tasks: {', '.join(task_names)}"
                    overall_success = True
                else:
                    test_steps[2]["status"] = "failed"
                    test_steps[2]["Result_Message"] = "❌ DAG exists but has no tasks"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Could not retrieve DAG tasks (HTTP {tasks_response.status_code})"

        except Exception as e:
            test_steps[2]["status"] = "failed"
            test_steps[2][
                "Result_Message"
            ] = f"❌ Error validating DAG structure: {str(e)}"

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Airflow validation error: {str(e)}"

    return {"success": overall_success, "test_steps": test_steps}
