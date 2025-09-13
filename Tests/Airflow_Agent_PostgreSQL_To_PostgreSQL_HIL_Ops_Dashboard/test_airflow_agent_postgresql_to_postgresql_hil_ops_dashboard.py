import importlib
import os
import pytest
import re
import time
import uuid
import psycopg2

from model.Configure_Model import cleanup_model_artifacts
from model.Configure_Model import set_up_model_configs
from model.Run_Model import run_model

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.airflow
@pytest.mark.postgres
@pytest.mark.pipeline
@pytest.mark.database
@pytest.mark.three  # Difficulty 3 - involves multi-database ETL, categorization, and external API integration
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"hil_ops_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"workflow_db_{test_timestamp}_{test_uuid}",
            "sql_file": "postgres_schema.sql"
        }
    ]
}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_hil_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"hil_ops_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
def test_airflow_agent_postgresql_to_postgresql_hil_ops_dashboard(request, airflow_resource, github_resource, supabase_account_resource, postgres_resource):
    model_result = None  # Initialize before try block
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    dag_name = "hil_ops_dashboard_etl"
    pr_title = f"Add_HIL_Ops_Dashboard_ETL {test_timestamp}_{test_uuid}"
    branch_name = f"feature/hil_ops_dashboard_etl-{test_timestamp}_{test_uuid}"
    Test_Configs.User_Input = Test_Configs.User_Input.replace("BRANCH_NAME", branch_name)
    Test_Configs.User_Input = Test_Configs.User_Input.replace("PR_NAME", pr_title)
    Test_Configs.User_Input = Test_Configs.User_Input.replace("SLACK_APP_URL", os.getenv("SLACK_APP_URL"))
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the fixtures - following the exact pattern from working tests
    print("=== Starting PostgreSQL to PostgreSQL HIL Ops Dashboard Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
    print(f"Using PostgreSQL instance from fixture: {postgres_resource['resource_id']}")
    print(f"Airflow base URL: {airflow_resource['base_url']}")
    print(f"Test directory: {input_dir}")

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

    # SECTION 1: SETUP THE TEST - following exact pattern from working tests
    config_results = None  # Initialize before try block
    try:
        # Get the actual database names from the fixtures
        postgres_db_name = postgres_resource["created_resources"][0]["name"]
        
        print(f"Using PostgreSQL database: {postgres_db_name}")

        # Update the configs to use the fixture-created databases
        Test_Configs.Configs["services"]["postgreSQL"]["databases"][0]["name"] = postgres_db_name
        
        # Update Airflow configs with values from airflow_resource fixture - following exact pattern
        Test_Configs.Configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        Test_Configs.Configs["services"]["airflow"]["username"] = airflow_resource["username"]
        Test_Configs.Configs["services"]["airflow"]["password"] = airflow_resource["password"]
        Test_Configs.Configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]

        custom_info = {"mode": request.config.getoption("--mode")}
        if request.config.getoption("--mode") == "Ardent":
            custom_info["publicKey"] = supabase_account_resource["publicKey"]
            custom_info["secretKey"] = supabase_account_resource["secretKey"]

        config_results = set_up_model_configs(Configs=Test_Configs.Configs, custom_info=custom_info)

        custom_info = {
            **custom_info,
            **config_results,
        }

        # SECTION 2: RUN THE MODEL - following exact pattern
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

        # SECTION 3: VERIFY THE OUTCOMES - following exact pattern from working tests
        # Check if the branch exists and verify PR creation/merge
        print("Waiting 10 seconds for model to create branch and PR...")
        time.sleep(10)  # Give the model time to create the branch and PR
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists(branch_name, test_steps[0])
        if not branch_exists:
            raise Exception(test_steps[0]["Result_Message"])

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
            raise Exception("Unable to find and merge PR. Please check the PR title and commit title.")

        # Use the airflow instance from the fixture to pull DAGs from GitHub
        # The fixture already has the Docker instance running
        airflow_instance = airflow_resource["airflow_instance"]
        
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            raise Exception("Action is not complete")
        
        # verify the airflow instance is ready after the github action redeployed
        if not airflow_instance.wait_for_airflow_to_be_ready():
            raise Exception("Airflow instance did not redeploy successfully.")

        # Use the connection details from the fixture
        airflow_base_url = airflow_resource["base_url"]
        airflow_api_token = airflow_resource["api_token"]
        
        print(f"Connecting to Airflow at: {airflow_base_url}")
        print(f"Using API Token: {airflow_api_token}")

        # Wait for DAG to appear and trigger it
        dag_name = "hil_ops_dashboard_etl"
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # Verify the data was processed correctly
        print("Verifying data was processed in PostgreSQL...")
        postgres_conn = postgres_resource["connection"]
        cursor = postgres_conn.cursor()
        
        try:
            # Check ops_queue table
            cursor.execute("SELECT COUNT(*) FROM ops_queue")
            queue_count = cursor.fetchone()[0]
            print(f"Found {queue_count} items in ops_queue")
            
            # Check for specific categorization metrics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_items,
                    COUNT(DISTINCT intervention_id) as unique_interventions,
                    COUNT(CASE WHEN category = 'validation_error' THEN 1 END) as validation_errors,
                    COUNT(CASE WHEN category = 'step_error' THEN 1 END) as step_errors,
                    COUNT(CASE WHEN category = 'external_api_fail' THEN 1 END) as api_failures,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_items,
                    COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_items
                FROM ops_queue
            """)
            metrics = cursor.fetchone()
            print(f"HIL Ops metrics: {metrics[0]} total items, {metrics[1]} unique interventions, {metrics[2]} validation errors, {metrics[3]} step errors, {metrics[4]} API failures, {metrics[5]} pending, {metrics[6]} resolved")
            
            if queue_count > 0 and metrics[1] > 0:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"HIL Ops dashboard data successfully processed: {queue_count} items with {metrics[5]} pending and {metrics[6]} resolved across {metrics[1]} unique interventions"
                print("✅ All validations passed! HIL Ops dashboard ETL pipeline created and executed successfully.")
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "No HIL Ops data found in ops_queue table"
                raise Exception("ETL pipeline did not process HIL Ops data correctly")
                
        finally:
            cursor.close()

    finally:
        try:
            # CLEANUP - following exact pattern from working tests
            if request.config.getoption("--mode") == "Ardent":
                custom_info['job_id'] = model_result.get("id") if model_result else None
            cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)
            # Delete the branch from github using the github manager
            github_manager.delete_branch(branch_name)

        except Exception as e:
            print(f"Error during cleanup: {e}")
