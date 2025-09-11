import importlib
import os
import pytest
import re
import time
import uuid

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


@pytest.mark.pipeline
@pytest.mark.two  # Difficulty 2 - involves DAG creation, PR management, and validation
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"simple_pipeline_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_simple_pipeline_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
def test_airflow_agent_simple_pipeline(request, airflow_resource, github_resource, supabase_account_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    dag_name = "hello_world_dag"
    pr_title = f"Add Hello World DAG {test_timestamp}_{test_uuid}"
    branch_name = f"feature/hello_world_dag-{test_timestamp}_{test_uuid}"
    Test_Configs.User_Input = Test_Configs.User_Input.replace("BRANCH_NAME", branch_name)
    Test_Configs.User_Input = Test_Configs.User_Input.replace("PR_NAME", pr_title)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting Simple Airflow Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
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
            "name": "Checking Dag Results",
            "description": "Checking if the DAG produces the expected results",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None  # Initialize before try block
    custom_info = {"mode": request.config.getoption("--mode")}
    try:
        # The dags folder is already set up by the fixture
        print("GitHub repository setup completed by fixture")

        # set the airflow folder with the correct configs
        # this function is for you to take the configs for the test and set them up however you want. They follow a set structure
        Test_Configs.Configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        Test_Configs.Configs["services"]["airflow"]["username"] = airflow_resource["username"]
        Test_Configs.Configs["services"]["airflow"]["password"] = airflow_resource["password"]
        Test_Configs.Configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        if request.config.getoption("--mode") == "Ardent":
            custom_info["publicKey"] = supabase_account_resource["publicKey"]
            custom_info["secretKey"] = supabase_account_resource["secretKey"]

        config_results = set_up_model_configs(Configs=Test_Configs.Configs,custom_info=custom_info)

        custom_info = {
            **custom_info,
            **config_results,
        }

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        print("Running model to create DAG and PR...")
        model_result = run_model(container=None, task=Test_Configs.User_Input, configs=Test_Configs.Configs,extra_information = custom_info)
        end_time = time.time()
        print(f"Model execution completed. Result: {model_result}")
        request.node.user_properties.append(("model_runtime", end_time - start_time))

        # Register the Braintrust root span ID for tracking (Ardent mode only)
        if model_result and "bt_root_span_id" in model_result:
            request.node.user_properties.append(("run_trace_id", model_result.get("bt_root_span_id")))
            print(f"Registered Braintrust root span ID: {model_result.get('bt_root_span_id')}")

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
        # Wait for DAG to appear and trigger it
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 3: VERIFY THE OUTCOMES
        # Get the task logs to verify "Hello World" was printed
        print("Retrieving task logs to verify 'Hello World' output...")

        # get the logs for the task
        logs = airflow_instance.get_task_instance_logs(dag_id=dag_name, dag_run_id=dag_run_id, task_id="print_hello")
        print(f"Task logs retrieved. Log content length: {len(logs)} characters")
        print(f"Log content preview: {logs[:200]}...")

        assert "Hello World" in logs, "Expected 'Hello World' in task logs"
        print("✓ 'Hello World' found in task logs!")
        test_steps[2]["status"] = "passed"
        test_steps[2][
            "Result_Message"
        ] = "DAG produced the expected results of Hello World printed to the logs"

    finally:
        try:
            # this function is for you to remove the configs for the test. They follow a set structure.
            if request.config.getoption("--mode") == "Ardent":
                custom_info['job_id'] = model_result.get("id") if model_result else None
            cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)
            # Delete the branch from github using the github manager
            github_manager.delete_branch(branch_name)

        except Exception as e:
            print(f"Error during cleanup: {e}")
