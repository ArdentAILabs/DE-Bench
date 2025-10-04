# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import psycopg2
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates sensor and conditional branching implementation.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize fixtures
    custom_airflow_config = {
        "resource_id": f"airflow_sensor_branch_{test_timestamp}_{test_uuid}",
    }

    custom_postgres_config = {
        "resource_id": f"sensor_db_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"sensor_db_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    custom_github_config = {
        "resource_id": f"test_airflow_sensor_{test_timestamp}_{test_uuid}",
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [airflow_fixture, postgres_fixture, github_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    """
    from extract_test_configs import create_config_from_fixtures

    # Get GitHub fixture for dynamic branch/PR names
    github_fixture = next(
        (f for f in fixtures if f.get_resource_type() == "github_resource"), None
    )

    if not github_fixture:
        raise Exception("GitHub fixture not found")

    github_resource_data = getattr(github_fixture, "_resource_data", None)
    if not github_resource_data:
        raise Exception("GitHub resource data not available")

    github_manager = github_resource_data.get("github_manager")
    if not github_manager:
        raise Exception("GitHub manager not available")

    # Generate dynamic branch and PR names
    pr_title = f"Add Event-Driven Financial Pipeline {test_timestamp}_{test_uuid}"
    branch_name = f"feature/sensor-branching-{test_timestamp}_{test_uuid}"

    # Start with original user input
    task_description = Test_Configs.User_Input

    # Add merge step
    task_description = github_manager.add_merge_step_to_user_input(task_description)

    # Replace placeholders
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    # Set up GitHub secrets
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )

    print(f"🔧 Generated dynamic branch name: {branch_name}")
    print(f"🔧 Generated dynamic PR title: {pr_title}")

    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created sensor and branching DAG.
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes DAG creation",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "Git Branch Creation",
            "description": "Verify git branch created",
            "status": "running",
            "Result_Message": "Checking if git branch exists...",
        },
        {
            "name": "PR Creation and Merge",
            "description": "Verify PR created and merged",
            "status": "running",
            "Result_Message": "Checking if PR was created and merged...",
        },
        {
            "name": "GitHub Action Completion",
            "description": "Verify GitHub action completed",
            "status": "running",
            "Result_Message": "Waiting for GitHub action...",
        },
        {
            "name": "Airflow Redeployment",
            "description": "Verify Airflow redeployed",
            "status": "running",
            "Result_Message": "Checking if Airflow redeployed...",
        },
        {
            "name": "DAG Creation Validation",
            "description": "Verify DAG exists in Airflow",
            "status": "running",
            "Result_Message": "Validating DAG existence...",
        },
        {
            "name": "Sensor Implementation",
            "description": "Verify sensor tasks exist in DAG",
            "status": "running",
            "Result_Message": "Checking for sensor tasks...",
        },
        {
            "name": "Branching Logic",
            "description": "Verify BranchOperator implementation",
            "status": "running",
            "Result_Message": "Checking branching logic...",
        },
        {
            "name": "DAG Execution Flow",
            "description": "Verify DAG can execute successfully",
            "status": "running",
            "Result_Message": "Testing DAG execution...",
        },
    ]

    try:
        # Step 1: Check agent execution
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed successfully"

        # Get fixtures
        airflow_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "airflow_resource"), None
        ) if fixtures else None
        
        github_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "github_resource"), None
        ) if fixtures else None

        if not airflow_fixture or not github_fixture:
            raise Exception("Required fixtures not found")

        # Get resource data
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        github_resource_data = getattr(github_fixture, "_resource_data", None)

        if not airflow_resource_data or not github_resource_data:
            raise Exception("Resource data not available")

        airflow_instance = airflow_resource_data["airflow_instance"]
        base_url = airflow_resource_data["base_url"]
        github_manager = github_resource_data.get("github_manager")

        # Generate same names used in create_model_inputs
        pr_title = f"Add Event-Driven Financial Pipeline {test_timestamp}_{test_uuid}"
        branch_name = f"feature/sensor-branching-{test_timestamp}_{test_uuid}"

        # Steps 2-5: GitHub and Airflow workflow
        print(f"🔍 Checking for branch: {branch_name}")
        time.sleep(10)

        branch_exists, test_steps[1] = github_manager.verify_branch_exists(branch_name, test_steps[1])
        if not branch_exists:
            test_steps[1]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = f"✅ Git branch '{branch_name}' created successfully"

        # Capture agent code snapshot
        print(f"📸 Capturing agent code snapshot from branch: {branch_name}")
        try:
            agent_code_snapshot = github_manager.get_multiple_file_contents_from_branch(
                branch_name=branch_name,
                paths_to_capture=[
                    "dags/",
                    "requirements.txt",
                    "Requirements/requirements.txt"
                ]
            )
            print(f"✅ Agent code snapshot captured: {agent_code_snapshot['summary']['total_files']} files")
            
            test_steps.append({
                "name": "Agent Code Snapshot Capture",
                "description": "Capture exact code created by agent",
                "status": "passed",
                "Result_Message": f"✅ Captured {agent_code_snapshot['summary']['total_files']} files",
                "agent_code_snapshot": agent_code_snapshot,
            })
        except Exception as e:
            print(f"⚠️ Failed to capture agent code snapshot: {e}")

        # PR creation and merge
        pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
            pr_title=pr_title,
            test_step=test_steps[2],
            commit_title=pr_title,
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource_data["deployment_id"],
                "deploymentName": airflow_resource_data["deployment_name"],
            },
        )

        if not pr_exists:
            test_steps[2]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"✅ PR '{pr_title}' created and merged"

        # GitHub action completion
        action_status = github_manager.check_if_action_is_complete(pr_title=pr_title, return_details=True)
        
        if not action_status["completed"] or not action_status["success"]:
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = f"❌ GitHub action failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "✅ GitHub action completed successfully"

        # Airflow redeployment
        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = "❌ Airflow did not redeploy"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = "✅ Airflow redeployed successfully"

        # Step 6: Check DAG existence
        dag_name = "event_driven_financial_pipeline"
        print(f"🔍 Checking for DAG: {dag_name}")

        if airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "passed"
            test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' found in Airflow"
        else:
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = f"❌ DAG '{dag_name}' not found"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Step 7: Check for sensor tasks
        print("🔍 Checking DAG structure for sensors...")
        
        # This requires inspecting DAG structure - we'll mark as partial if DAG exists
        test_steps[6]["status"] = "partial"
        test_steps[6]["Result_Message"] = "⚠️ DAG exists, sensor validation requires DAG introspection"

        # Step 8: Check for branching logic
        test_steps[7]["status"] = "partial"
        test_steps[7]["Result_Message"] = "⚠️ DAG exists, branching validation requires DAG introspection"

        # Step 9: Try to execute DAG
        print(f"🔍 Triggering DAG: {dag_name}")
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)

        if dag_run_id:
            # Monitor execution
            try:
                airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
                test_steps[8]["status"] = "passed"
                test_steps[8]["Result_Message"] = f"✅ DAG executed successfully (run_id: {dag_run_id})"
            except Exception as e:
                test_steps[8]["status"] = "partial"
                test_steps[8]["Result_Message"] = f"⚠️ DAG triggered but execution incomplete: {str(e)}"
        else:
            test_steps[8]["status"] = "failed"
            test_steps[8]["Result_Message"] = "❌ Failed to trigger DAG"

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
