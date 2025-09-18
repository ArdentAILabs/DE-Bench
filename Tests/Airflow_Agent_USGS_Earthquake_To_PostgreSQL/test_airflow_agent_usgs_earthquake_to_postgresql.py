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

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a USGS earthquake data pipeline DAG.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with test-specific configuration
    custom_airflow_config = {
        "resource_id": f"usgs_earthquake_test_{test_timestamp}_{test_uuid}",
    }

    # Initialize PostgreSQL fixture for earthquake data
    custom_postgres_config = {
        "resource_id": f"usgs_earthquake_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"earthquake_data_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": f"test_airflow_usgs_earthquake_test_{test_timestamp}_{test_uuid}",
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
    pr_title = f"Add USGS Earthquake Data Pipeline {test_timestamp}_{test_uuid}"
    branch_name = f"feature/usgs-earthquake-{test_timestamp}_{test_uuid}"

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
    Validates that the AI agent successfully created a USGS earthquake data pipeline DAG.

    Expected behavior:
    - DAG should be created with name "usgs_earthquake_dag"
    - DAG should pull data from USGS API
    - DAG should store earthquake data in PostgreSQL database
    - DAG should demonstrate proper API integration and data modeling

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
            "description": "AI Agent executes task to create USGS Earthquake Pipeline DAG",
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
            "description": "Verify that usgs_earthquake_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that USGS Earthquake DAG exists in Airflow...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "API Integration Validation",
            "description": "Verify that DAG successfully connects to USGS API",
            "status": "running",
            "Result_Message": "Checking USGS API integration...",
        },
        {
            "name": "Database Table Creation",
            "description": "Verify that earthquake data table was created in PostgreSQL",
            "status": "running",
            "Result_Message": "Checking if earthquake data table exists...",
        },
        {
            "name": "Data Storage Validation",
            "description": "Verify that earthquake data was successfully stored",
            "status": "running",
            "Result_Message": "Validating earthquake data storage...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # Get fixtures for Airflow, PostgreSQL, and GitHub
        airflow_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "airflow_resource"),
                None,
            )
            if fixtures
            else None
        )
        postgres_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "postgres_resource"),
                None,
            )
            if fixtures
            else None
        )
        github_fixture = (
            next(
                (f for f in fixtures if f.get_resource_type() == "github_resource"),
                None,
            )
            if fixtures
            else None
        )

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
        pr_title = f"Add USGS Earthquake Data Pipeline {test_timestamp}_{test_uuid}"
        branch_name = f"feature/usgs-earthquake-{test_timestamp}_{test_uuid}"

        # Step 2-6: GitHub and Airflow workflow
        print(f"🔍 Checking for branch: {branch_name}")
        time.sleep(10)

        branch_exists, test_steps[1] = github_manager.verify_branch_exists(
            branch_name, test_steps[1]
        )
        if not branch_exists:
            test_steps[1]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[1]["status"] = "passed"
        test_steps[1][
            "Result_Message"
        ] = f"✅ Git branch '{branch_name}' created successfully"

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
        test_steps[2][
            "Result_Message"
        ] = f"✅ PR '{pr_title}' created and merged successfully"

        # GitHub action completion
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            test_steps[3]["status"] = "failed"
            test_steps[3][
                "Result_Message"
            ] = "❌ GitHub action did not complete successfully"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "✅ GitHub action completed successfully"

        # Airflow redeployment
        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            test_steps[4][
                "Result_Message"
            ] = "❌ Airflow instance did not redeploy successfully"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[4]["status"] = "passed"
        test_steps[4][
            "Result_Message"
        ] = "✅ Airflow redeployed successfully after GitHub action"

        # DAG existence check
        dag_name = "usgs_earthquake_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}")

        if airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "passed"
            test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' found in Airflow"
        else:
            test_steps[5]["status"] = "failed"
            test_steps[5][
                "Result_Message"
            ] = f"❌ DAG '{dag_name}' not found in Airflow"
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
        test_steps[6][
            "Result_Message"
        ] = f"✅ DAG '{dag_name}' executed successfully (run_id: {dag_run_id})"

        # Step 8: API Integration Validation (through task logs)
        try:
            print("🔍 Retrieving task logs to verify USGS API integration...")
            logs = airflow_instance.get_task_instance_logs(
                dag_id=dag_name,
                dag_run_id=dag_run_id,
                task_id="extract_earthquake_data",
            )

            if "earthquake.usgs.gov" in logs or "geojson" in logs or "features" in logs:
                test_steps[7]["status"] = "passed"
                test_steps[7][
                    "Result_Message"
                ] = "✅ USGS API integration validated: API calls found in logs"
            else:
                test_steps[7]["status"] = "failed"
                test_steps[7][
                    "Result_Message"
                ] = "❌ No evidence of USGS API integration in task logs"

        except Exception as e:
            test_steps[7]["status"] = "failed"
            test_steps[7][
                "Result_Message"
            ] = f"❌ Error validating API integration: {str(e)}"

        # Step 9 & 10: PostgreSQL Database Validation
        try:
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

            # Step 9: Check if earthquake-related table was created
            # Look for common earthquake table names
            table_names = [
                "earthquakes",
                "earthquake_data",
                "usgs_earthquakes",
                "earthquake_events",
            ]
            earthquake_table = None

            for table_name in table_names:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cur.fetchone()[0]
                    earthquake_table = table_name
                    break
                except psycopg2.Error:
                    continue

            if earthquake_table:
                test_steps[8]["status"] = "passed"
                test_steps[8][
                    "Result_Message"
                ] = f"✅ Earthquake table '{earthquake_table}' created successfully"

                # Step 10: Check if data was stored
                cur.execute(f"SELECT COUNT(*) FROM {earthquake_table}")
                data_count = cur.fetchone()[0]

                if data_count > 0:
                    test_steps[9]["status"] = "passed"
                    test_steps[9][
                        "Result_Message"
                    ] = f"✅ Earthquake data stored successfully: {data_count} records"
                else:
                    test_steps[9]["status"] = "failed"
                    test_steps[9][
                        "Result_Message"
                    ] = f"❌ No earthquake data found in table '{earthquake_table}'"
            else:
                test_steps[8]["status"] = "failed"
                test_steps[8][
                    "Result_Message"
                ] = "❌ No earthquake data table found in database"
                test_steps[9]["status"] = "failed"
                test_steps[9][
                    "Result_Message"
                ] = "❌ Cannot validate data storage - table not found"

            cur.close()
            conn.close()

        except Exception as e:
            test_steps[8]["status"] = "failed"
            test_steps[8]["Result_Message"] = f"❌ Database validation error: {str(e)}"
            test_steps[9]["status"] = "failed"
            test_steps[9]["Result_Message"] = f"❌ Database validation error: {str(e)}"

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

    print(
        f"🎯 Validation completed: {passed_steps}/{total_steps} steps passed (Score: {score:.2f})"
    )

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
