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
    This test validates dynamic DAG generation from database configuration.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # PostgreSQL for tenant configurations
    custom_postgres_config = {
        "resource_id": f"tenant_configs_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"tenant_config_db_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    # Snowflake for tenant data warehouses
    custom_snowflake_config = {
        "resource_id": f"tenant_dw_{test_timestamp}_{test_uuid}",
        "database": f"TENANT_DW_{test_timestamp}_{test_uuid}",
        "schema": f"PUBLIC_{test_timestamp}_{test_uuid}",
        "sql_file": None,
    }

    # Airflow for dynamic DAGs
    custom_airflow_config = {
        "resource_id": f"dynamic_dags_{test_timestamp}_{test_uuid}",
    }

    # GitHub
    custom_github_config = {
        "resource_id": f"test_airflow_dynamic_dag_{test_timestamp}_{test_uuid}",
    }

    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    snowflake_fixture = SnowflakeFixture(custom_config=custom_snowflake_config)
    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [postgres_fixture, snowflake_fixture, airflow_fixture, github_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    """
    from extract_test_configs import create_config_from_fixtures

    github_fixture = next((f for f in fixtures if f.get_resource_type() == "github_resource"), None)
    github_resource_data = getattr(github_fixture, "_resource_data", None)
    github_manager = github_resource_data.get("github_manager")

    pr_title = f"Add Dynamic DAG Generation System {test_timestamp}_{test_uuid}"
    branch_name = f"feature/dynamic-dags-{test_timestamp}_{test_uuid}"

    task_description = Test_Configs.User_Input
    task_description = github_manager.add_merge_step_to_user_input(task_description)
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    github_manager.check_and_update_gh_secrets(
        secrets={"ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"]}
    )

    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates dynamic DAG generation implementation.
    """
    test_steps = [
        {"name": "Agent Task Execution", "description": "AI Agent executes task", "status": "running", "Result_Message": "Checking agent execution..."},
        {"name": "Git Branch Creation", "description": "Verify branch created", "status": "running", "Result_Message": "Checking branch..."},
        {"name": "PR Creation and Merge", "description": "Verify PR merged", "status": "running", "Result_Message": "Checking PR..."},
        {"name": "GitHub Action Completion", "description": "Verify action completed", "status": "running", "Result_Message": "Checking action..."},
        {"name": "Airflow Redeployment", "description": "Verify Airflow redeployed", "status": "running", "Result_Message": "Checking Airflow..."},
        {"name": "Tenant Config Table", "description": "Verify tenant_pipeline_configs table", "status": "running", "Result_Message": "Checking config table..."},
        {"name": "Dynamic DAG Factory", "description": "Verify dynamic DAG code exists", "status": "running", "Result_Message": "Checking DAG factory..."},
        {"name": "Tenant DAGs Generated", "description": "Verify 4 tenant DAGs created", "status": "running", "Result_Message": "Checking generated DAGs..."},
        {"name": "DAG Execution", "description": "Verify tenant DAG runs", "status": "running", "Result_Message": "Testing DAG execution..."},
    ]

    try:
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ Agent failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ Agent completed"

        # Get fixtures
        postgres_fixture = next((f for f in fixtures if f.get_resource_type() == "postgres_resource"), None) if fixtures else None
        airflow_fixture = next((f for f in fixtures if f.get_resource_type() == "airflow_resource"), None) if fixtures else None
        github_fixture = next((f for f in fixtures if f.get_resource_type() == "github_resource"), None) if fixtures else None

        if not all([postgres_fixture, airflow_fixture, github_fixture]):
            raise Exception("Required fixtures not found")

        postgres_resource_data = getattr(postgres_fixture, "_resource_data", None)
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        github_resource_data = getattr(github_fixture, "_resource_data", None)

        airflow_instance = airflow_resource_data["airflow_instance"]
        github_manager = github_resource_data.get("github_manager")

        pr_title = f"Add Dynamic DAG Generation System {test_timestamp}_{test_uuid}"
        branch_name = f"feature/dynamic-dags-{test_timestamp}_{test_uuid}"

        # GitHub workflow
        time.sleep(10)
        branch_exists, test_steps[1] = github_manager.verify_branch_exists(branch_name, test_steps[1])
        if not branch_exists:
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[1]["status"] = "passed"

        pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
            pr_title=pr_title, test_step=test_steps[2], commit_title=pr_title, merge_method="squash",
            build_info={"deploymentId": airflow_resource_data["deployment_id"], "deploymentName": airflow_resource_data["deployment_name"]}
        )
        if not pr_exists:
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[2]["status"] = "passed"

        action_status = github_manager.check_if_action_is_complete(pr_title=pr_title, return_details=True)
        if not action_status["completed"] or not action_status["success"]:
            test_steps[3]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[3]["status"] = "passed"

        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[4]["status"] = "passed"

        # Check tenant config table in PostgreSQL
        postgres_db_name = postgres_resource_data["created_resources"][0]["name"]
        db_connection = postgres_fixture.get_connection(postgres_db_name)
        db_cursor = db_connection.cursor()

        try:
            db_cursor.execute("SELECT COUNT(*) FROM tenant_pipeline_configs")
            config_count = db_cursor.fetchone()[0]
            
            db_cursor.execute("SELECT COUNT(*) FROM tenant_pipeline_configs WHERE enabled = TRUE")
            enabled_count = db_cursor.fetchone()[0]
            
            if config_count >= 5 and enabled_count == 4:
                test_steps[5]["status"] = "passed"
                test_steps[5]["Result_Message"] = f"✅ tenant_pipeline_configs has {config_count} configs ({enabled_count} enabled)"
            elif config_count >= 5:
                test_steps[5]["status"] = "partial"
                test_steps[5]["Result_Message"] = f"⚠️ Table has {config_count} configs but {enabled_count} enabled (expected 4)"
            else:
                test_steps[5]["status"] = "failed"
                test_steps[5]["Result_Message"] = f"❌ Only {config_count} tenant configs found"

        finally:
            db_cursor.close()
            db_connection.close()

        # Check for dynamic DAG factory code
        test_steps[6]["status"] = "partial"
        test_steps[6]["Result_Message"] = "⚠️ DAG factory code validation requires GitHub inspection"

        # Check for generated tenant DAGs
        # Try to find DAGs matching pattern tenant_*_pipeline
        dag_list = []
        try:
            # This requires Airflow API to list all DAGs
            # We'll check if at least some DAGs exist
            test_steps[7]["status"] = "partial"
            test_steps[7]["Result_Message"] = "⚠️ Tenant DAG count validation requires Airflow API inspection"
        except Exception as e:
            test_steps[7]["status"] = "partial"
            test_steps[7]["Result_Message"] = f"⚠️ Cannot enumerate DAGs: {str(e)}"

        # Try to execute a tenant DAG
        tenant_dag_names = [
            "tenant_acme_corp_pipeline",
            "tenant_beta_inc_pipeline",
            "tenant_gamma_solutions_pipeline",
            "tenant_delta_systems_pipeline"
        ]
        
        executed_any = False
        for dag_name in tenant_dag_names:
            if airflow_instance.verify_airflow_dag_exists(dag_name):
                dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
                if dag_run_id:
                    executed_any = True
                    test_steps[8]["status"] = "passed"
                    test_steps[8]["Result_Message"] = f"✅ Successfully executed tenant DAG: {dag_name}"
                    break

        if not executed_any:
            test_steps[8]["status"] = "partial"
            test_steps[8]["Result_Message"] = "⚠️ Could not execute tenant DAGs (may not exist or use different naming)"

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {"score": score, "metadata": {"test_steps": test_steps}}
