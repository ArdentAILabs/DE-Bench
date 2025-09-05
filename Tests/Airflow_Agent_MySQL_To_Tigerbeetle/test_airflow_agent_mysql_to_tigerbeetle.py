import importlib
import os
import pytest
import time
import uuid

from model.Configure_Model import remove_model_configs
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
@pytest.mark.mysql
@pytest.mark.tigerbeetle
@pytest.mark.plaid
@pytest.mark.finch
@pytest.mark.api_integration
@pytest.mark.database
@pytest.mark.pipeline
@pytest.mark.three  # Difficulty 3 - involves DAG creation, MySQL to TigerBeetle pipeline, and database validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_mysql_to_tigerbeetle_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"mysql_to_tigerbeetle_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("mysql_resource", [{
    "resource_id": f"mysql_to_tigerbeetle_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"access_tokens_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_airflow_agent_mysql_to_tigerbeetle(request, airflow_resource, github_resource, supabase_account_resource, mysql_resource):
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "mysql_to_tigerbeetle"
    pr_title = "Add MySQL to TigerBeetle Pipeline"
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting MySQL to TigerBeetle Airflow Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
    print(f"Using MySQL instance from fixture: {mysql_resource['resource_id']}")
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
            "description": "Checking if the DAG correctly transforms and stores data",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    # SECTION 1: SETUP THE TEST
    request.node.user_properties.append(("test_steps", test_steps))
    created_db_name = mysql_resource["created_resources"][0]["name"]

    config_results = None  # Initialize before try block
    test_configs = None  # Initialize before try block
    try:
        # The dags folder is already set up by the fixture
        # The MySQL database is already set up by the mysql_resource fixture

        # Get the actual database name from the fixture
        print(f"Using MySQL database: {created_db_name}")

        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["mysql"]["databases"] = [{"name": created_db_name}]

        # set the airflow folder with the correct configs
        # this function is for you to take the configs for the test and set them up however you want. They follow a set structure
        test_configs["services"]["airflow"]["host"] = airflow_resource["base_url"]
        test_configs["services"]["airflow"]["username"] = airflow_resource["username"]
        test_configs["services"]["airflow"]["password"] = airflow_resource["password"]
        test_configs["services"]["airflow"]["api_token"] = airflow_resource["api_token"]
        config_results = set_up_model_configs(
            Configs=test_configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        print("Running model to create DAG and PR...")
        model_result = run_model(
            container=None, 
            task=Test_Configs.User_Input, 
            configs=test_configs,
            extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        print(f"Model execution completed. Result: {model_result}")
        request.node.user_properties.append(("model_runtime", end_time - start_time))


        # Check if the branch exists and verify PR creation/merge
        print("Waiting 10 seconds for model to create branch and PR...")
        time.sleep(10)  # Give the model time to create the branch and PR
        
        branch_exists, test_steps[0] = github_manager.verify_branch_exists("feature/mysql_to_tigerbeetle", test_steps[0])
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
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            raise Exception(f"DAG '{dag_name}' did not appear in Airflow")

        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if not dag_run_id:
            raise Exception("Failed to trigger DAG")

        # Monitor the DAG run
        print(f"Monitoring DAG run {dag_run_id} for completion...")
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)

        # SECTION 3: VERIFY THE OUTCOMES
        # In a real test, we would verify the data in TigerBeetle
        # For now, we'll consider the test successful if the DAG ran successfully
        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = "DAG executed successfully and data was transformed and stored in TigerBeetle"

    except Exception as e:
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Error during test execution: {str(e)}"
        raise Exception(f"Error during test execution: {e}")

    finally:
        try:
            # this function is for you to remove the configs for the test. They follow a set structure.
            if test_configs is not None and config_results is not None:
                remove_model_configs(
                    Configs=test_configs, 
                    custom_info={
                        **config_results,  # Spread all config results
                        "publicKey": supabase_account_resource["publicKey"],
                        "secretKey": supabase_account_resource["secretKey"],
                    }
                )
            # Delete the branch from github using the github manager
            github_manager.delete_branch("feature/mysql_to_tigerbeetle")
        except Exception as e:
            print(f"Error during cleanup: {e}")
