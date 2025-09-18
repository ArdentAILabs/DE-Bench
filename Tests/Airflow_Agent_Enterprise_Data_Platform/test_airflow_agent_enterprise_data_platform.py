# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import psycopg2
import json
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
    This Airflow test validates that AI can create an enterprise-level data platform DAG.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with test-specific configuration
    custom_airflow_config = {
        "resource_id": f"enterprise_data_platform_test_{test_timestamp}_{test_uuid}",
    }

    # Initialize PostgreSQL fixture for the enterprise platform data
    custom_postgres_config = {
        "resource_id": f"enterprise_platform_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"enterprise_platform_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": f"test_airflow_enterprise_data_platform_test_{test_timestamp}_{test_uuid}",
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
    This function has access to all fixture data after setup and dynamically
    updates the task description with GitHub branch and PR information.
    """
    import os
    from extract_test_configs import create_config_from_fixtures

    # Get GitHub fixture to access manager for dynamic branch/PR creation
    github_fixture = next(
        (f for f in fixtures if f.get_resource_type() == "github_resource"), None
    )

    if not github_fixture:
        raise Exception(
            "GitHub fixture not found - required for branch and PR management"
        )

    # Get the GitHub manager from the fixture
    github_resource_data = getattr(github_fixture, "_resource_data", None)
    if not github_resource_data:
        raise Exception("GitHub resource data not available")

    github_manager = github_resource_data.get("github_manager")
    if not github_manager:
        raise Exception("GitHub manager not available")

    # Generate dynamic branch and PR names
    pr_title = f"Add Enterprise Data Platform with Advanced Analytics {test_timestamp}_{test_uuid}"
    branch_name = f"feature/enterprise-data-platform-{test_timestamp}_{test_uuid}"

    # Start with the original user input from Test_Configs
    task_description = Test_Configs.User_Input

    # Add merge step to user input
    task_description = github_manager.add_merge_step_to_user_input(task_description)

    # Replace placeholders with dynamic values
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    # Set up GitHub secrets for Astro access
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )

    print(f"🔧 Generated dynamic branch name: {branch_name}")
    print(f"🔧 Generated dynamic PR title: {pr_title}")

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created an enterprise data platform DAG.

    Expected behavior:
    - DAG should be created with name "enterprise_data_platform_dag"
    - DAG should implement enterprise-level data integration and analytics
    - Customer 360 views, ML models, and governance should be in place
    - Advanced analytics and operational intelligence should be functional

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    # Create comprehensive test steps for validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to create Enterprise Data Platform DAG",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Airflow DAG creation task...",
        },
        {
            "name": "Git Branch Creation",
            "description": "Verify that git branch was created with the correct name",
            "status": "running",
            "Result_Message": "Checking if git branch exists...",
        },
        {
            "name": "PR Creation and Merge",
            "description": "Verify that PR was created and merged successfully",
            "status": "running",
            "Result_Message": "Checking if PR was created and merged...",
        },
        {
            "name": "GitHub Action Completion",
            "description": "Verify that GitHub action completed successfully",
            "status": "running",
            "Result_Message": "Waiting for GitHub action to complete...",
        },
        {
            "name": "Airflow Redeployment",
            "description": "Verify that Airflow redeployed after GitHub action",
            "status": "running",
            "Result_Message": "Checking if Airflow redeployed successfully...",
        },
        {
            "name": "DAG Creation Validation",
            "description": "Verify that enterprise_data_platform_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that Enterprise Data Platform DAG exists in Airflow...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "Customer 360 Integration",
            "description": "Verify unified customer profiles and 360-degree view",
            "status": "running",
            "Result_Message": "Checking customer 360 integration...",
        },
        {
            "name": "Multi-Source Transaction Processing",
            "description": "Verify unified transaction processing across systems",
            "status": "running",
            "Result_Message": "Validating multi-source transaction processing...",
        },
        {
            "name": "Inventory Optimization Platform",
            "description": "Verify inventory optimization and demand forecasting",
            "status": "running",
            "Result_Message": "Checking inventory optimization platform...",
        },
        {
            "name": "Marketing Attribution and ROI",
            "description": "Verify marketing attribution and ROI calculations",
            "status": "running",
            "Result_Message": "Validating marketing attribution and ROI...",
        },
        {
            "name": "Product Analytics and Performance",
            "description": "Verify product analytics and recommendation systems",
            "status": "running",
            "Result_Message": "Checking product analytics and performance...",
        },
        {
            "name": "Operational Intelligence",
            "description": "Verify operational dashboards and optimization",
            "status": "running",
            "Result_Message": "Validating operational intelligence...",
        },
        {
            "name": "Advanced Analytics and ML",
            "description": "Verify machine learning models and predictive analytics",
            "status": "running",
            "Result_Message": "Checking advanced analytics and ML...",
        },
        {
            "name": "Data Governance and Compliance",
            "description": "Verify data governance, lineage, and compliance",
            "status": "running",
            "Result_Message": "Validating data governance and compliance...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed task execution successfully"

        # Get fixtures for Airflow, PostgreSQL, and GitHub
        airflow_fixture = next((f for f in fixtures if f.get_resource_type() == "airflow_resource"), None) if fixtures else None
        postgres_fixture = next((f for f in fixtures if f.get_resource_type() == "postgres_resource"), None) if fixtures else None
        github_fixture = next((f for f in fixtures if f.get_resource_type() == "github_resource"), None) if fixtures else None

        if not airflow_fixture:
            raise Exception("Airflow fixture not found")
        if not postgres_fixture:
            raise Exception("PostgreSQL fixture not found")
        if not github_fixture:
            raise Exception("GitHub fixture not found")

        # Get resource data
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        if not airflow_resource_data:
            raise Exception("Airflow resource data not available")

        postgres_resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not postgres_resource_data:
            raise Exception("PostgreSQL resource data not available")

        github_resource_data = getattr(github_fixture, "_resource_data", None)
        if not github_resource_data:
            raise Exception("GitHub resource data not available")

        airflow_instance = airflow_resource_data["airflow_instance"]
        base_url = airflow_resource_data["base_url"]
        github_manager = github_resource_data.get("github_manager")

        if not github_manager:
            raise Exception("GitHub manager not available")

        # Generate the same branch and PR names used in create_model_inputs
        pr_title = f"Add Enterprise Data Platform with Advanced Analytics {test_timestamp}_{test_uuid}"
        branch_name = f"feature/enterprise-data-platform-{test_timestamp}_{test_uuid}"

        # Step 2-6: GitHub and Airflow workflow (similar to other tests)
        print(f"🔍 Checking for branch: {branch_name}")
        time.sleep(10)

        branch_exists, test_steps[1] = github_manager.verify_branch_exists(branch_name, test_steps[1])
        if not branch_exists:
            test_steps[1]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = f"✅ Git branch '{branch_name}' created successfully"

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
            test_steps[2]["Result_Message"] = "❌ Unable to find and merge PR"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"✅ PR '{pr_title}' created and merged successfully"

        # GitHub action completion
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = "❌ GitHub action did not complete successfully"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "✅ GitHub action completed successfully"

        # Airflow redeployment
        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = "❌ Airflow instance did not redeploy successfully"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = "✅ Airflow redeployed successfully after GitHub action"

        # DAG existence check
        dag_name = "enterprise_data_platform_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}")

        if airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "passed"
            test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' found in Airflow"
        else:
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = f"❌ DAG '{dag_name}' not found in Airflow"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # DAG execution
        print(f"🔍 Triggering DAG: {dag_name}")
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)

        if not dag_run_id:
            test_steps[6]["status"] = "failed"
            test_steps[6]["Result_Message"] = "❌ Failed to trigger DAG"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Monitor the DAG run until completion
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
        test_steps[6]["status"] = "passed"
        test_steps[6]["Result_Message"] = f"✅ DAG '{dag_name}' executed successfully (run_id: {dag_run_id})"

        # Step 8-15: PostgreSQL Enterprise Database Validation
        postgres_config = postgres_resource_data.get("databases", [{}])[0]
        database_name = postgres_config.get("name", "")

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=database_name,
            sslmode="require",
        )
        cur = conn.cursor()

        print(f"🔍 Connected to PostgreSQL database: {database_name}")

        # Enterprise validation steps
        enterprise_tables = [
            ("unified_customer_profiles", 7, "Customer 360 integration"),
            ("unified_transactions", 8, "Multi-source transaction processing"),
            ("inventory_optimization", 9, "Inventory optimization"),
            ("marketing_attribution", 10, "Marketing attribution and ROI"),
            ("product_analytics", 11, "Product analytics and performance"),
            ("operational_intelligence", 12, "Operational intelligence"),
            ("ml_models", 13, "Advanced analytics and ML"),
            ("data_governance", 14, "Data governance and compliance"),
        ]

        for table_name, step_idx, description in enterprise_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]

                if count > 0:
                    test_steps[step_idx]["status"] = "passed"
                    test_steps[step_idx]["Result_Message"] = f"✅ {description} validated: {count} records"
                else:
                    test_steps[step_idx]["status"] = "failed"
                    test_steps[step_idx]["Result_Message"] = f"❌ No {table_name} data found"

            except psycopg2.Error as e:
                test_steps[step_idx]["status"] = "failed"
                test_steps[step_idx]["Result_Message"] = f"❌ {description} validation error: {str(e)}"

        cur.close()
        conn.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    passed_steps = sum([step["status"] == "passed" for step in test_steps])
    total_steps = len(test_steps)
    score = passed_steps / total_steps

    print(f"🎯 Validation completed: {passed_steps}/{total_steps} steps passed (Score: {score:.2f})")

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
