# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import snowflake.connector
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
    This test validates cross-database integration with Airflow.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.MySQL.mysql_resources import MySQLFixture
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # PostgreSQL CRM database
    custom_postgres_config = {
        "resource_id": f"crm_db_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"crm_db_{test_timestamp}_{test_uuid}",
                "sql_file": "postgres_schema.sql",
            }
        ],
    }

    # MySQL e-commerce database
    custom_mysql_config = {
        "resource_id": f"ecommerce_db_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"ecommerce_db_{test_timestamp}_{test_uuid}",
                "tables": [
                    {
                        "name": "orders",
                        "columns": [
                            {"name": "order_id", "type": "INT AUTO_INCREMENT", "primary_key": True},
                            {"name": "customer_email", "type": "VARCHAR(255)", "not_null": True},
                            {"name": "order_date", "type": "DATE"},
                            {"name": "total_amount", "type": "DECIMAL(10,2)"},
                            {"name": "status", "type": "VARCHAR(50)"},
                        ],
                        "data": [
                            {"customer_email": "alice@example.com", "order_date": "2024-10-01", "total_amount": 299.99, "status": "completed"},
                            {"customer_email": "alice@example.com", "order_date": "2024-10-15", "total_amount": 150.00, "status": "completed"},
                            {"customer_email": "bob@example.com", "order_date": "2024-10-10", "total_amount": 89.99, "status": "completed"},
                            {"customer_email": "charlie@example.com", "order_date": "2024-10-20", "total_amount": 500.00, "status": "completed"},
                            {"customer_email": "unknown@example.com", "order_date": "2024-10-25", "total_amount": 75.00, "status": "completed"},
                        ],
                    },
                    {
                        "name": "products",
                        "columns": [
                            {"name": "product_id", "type": "INT AUTO_INCREMENT", "primary_key": True},
                            {"name": "name", "type": "VARCHAR(255)"},
                            {"name": "category", "type": "VARCHAR(100)"},
                            {"name": "price", "type": "DECIMAL(10,2)"},
                        ],
                        "data": [
                            {"name": "Laptop", "category": "Electronics", "price": 999.99},
                            {"name": "Shoes", "category": "Clothing", "price": 79.99},
                            {"name": "Book", "category": "Books", "price": 19.99},
                        ],
                    },
                ],
            }
        ],
    }

    # Snowflake analytics warehouse
    custom_snowflake_config = {
        "resource_id": f"analytics_dw_{test_timestamp}_{test_uuid}",
        "database": f"ANALYTICS_DW_{test_timestamp}_{test_uuid}",
        "schema": f"UNIFIED_{test_timestamp}_{test_uuid}",
        "sql_file": None,
    }

    # Airflow orchestration
    custom_airflow_config = {
        "resource_id": f"cross_db_integration_{test_timestamp}_{test_uuid}",
    }

    # GitHub
    custom_github_config = {
        "resource_id": f"test_airflow_cross_db_{test_timestamp}_{test_uuid}",
    }

    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    mysql_fixture = MySQLFixture(custom_config=custom_mysql_config)
    snowflake_fixture = SnowflakeFixture(custom_config=custom_snowflake_config)
    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [postgres_fixture, mysql_fixture, snowflake_fixture, airflow_fixture, github_fixture]


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

    pr_title = f"Add Cross-Database Analytics Pipeline {test_timestamp}_{test_uuid}"
    branch_name = f"feature/cross-db-integration-{test_timestamp}_{test_uuid}"

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
    Validates cross-database integration.
    """
    test_steps = [
        {"name": "Agent Task Execution", "description": "AI Agent executes task", "status": "running", "Result_Message": "Checking agent execution..."},
        {"name": "Git Branch Creation", "description": "Verify branch created", "status": "running", "Result_Message": "Checking branch..."},
        {"name": "PR Creation and Merge", "description": "Verify PR merged", "status": "running", "Result_Message": "Checking PR..."},
        {"name": "GitHub Action Completion", "description": "Verify action completed", "status": "running", "Result_Message": "Checking action..."},
        {"name": "Airflow Redeployment", "description": "Verify Airflow redeployed", "status": "running", "Result_Message": "Checking Airflow..."},
        {"name": "DAG Creation", "description": "Verify DAG exists", "status": "running", "Result_Message": "Checking DAG..."},
        {"name": "DAG Execution", "description": "Verify DAG runs", "status": "running", "Result_Message": "Running DAG..."},
        {"name": "Snowflake Staging Tables", "description": "Verify data extracted to Snowflake", "status": "running", "Result_Message": "Checking staging..."},
        {"name": "Customer 360 View", "description": "Verify unified view created", "status": "running", "Result_Message": "Checking customer_360..."},
    ]

    try:
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ Agent failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ Agent completed"

        # Get fixtures
        airflow_fixture = next((f for f in fixtures if f.get_resource_type() == "airflow_resource"), None) if fixtures else None
        snowflake_fixture = next((f for f in fixtures if f.get_resource_type() == "snowflake_resource"), None) if fixtures else None
        github_fixture = next((f for f in fixtures if f.get_resource_type() == "github_resource"), None) if fixtures else None

        if not all([airflow_fixture, snowflake_fixture, github_fixture]):
            raise Exception("Required fixtures not found")

        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        snowflake_resource_data = getattr(snowflake_fixture, "_resource_data", None)
        github_resource_data = getattr(github_fixture, "_resource_data", None)

        airflow_instance = airflow_resource_data["airflow_instance"]
        github_manager = github_resource_data.get("github_manager")
        database_name = snowflake_resource_data.get("database_name")
        schema_name = snowflake_resource_data.get("schema_name")

        pr_title = f"Add Cross-Database Analytics Pipeline {test_timestamp}_{test_uuid}"
        branch_name = f"feature/cross-db-integration-{test_timestamp}_{test_uuid}"

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

        # Check DAG
        dag_name = "cross_database_analytics_pipeline"
        if not airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = f"❌ DAG '{dag_name}' not found"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}
        test_steps[5]["status"] = "passed"
        test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' exists"

        # Execute DAG
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)
        if dag_run_id:
            airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
            test_steps[6]["status"] = "passed"
            test_steps[6]["Result_Message"] = f"✅ DAG executed successfully"
        else:
            test_steps[6]["status"] = "failed"
            test_steps[6]["Result_Message"] = "❌ DAG execution failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Check Snowflake for results
        snowflake_conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USERNAME"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=database_name,
            schema=schema_name,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        )
        snowflake_cur = snowflake_conn.cursor()

        try:
            # Check for staging tables
            snowflake_cur.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
            all_tables = [row[1] for row in snowflake_cur.fetchall()]
            
            staging_tables = [t for t in all_tables if 'STAGING' in t.upper() or 'STG' in t.upper()]
            
            if len(staging_tables) >= 2:
                test_steps[7]["status"] = "passed"
                test_steps[7]["Result_Message"] = f"✅ Found {len(staging_tables)} staging tables"
            else:
                test_steps[7]["status"] = "partial"
                test_steps[7]["Result_Message"] = f"⚠️ Found {len(all_tables)} tables in Snowflake"

            # Check for customer_360 or unified view
            customer_360_tables = [t for t in all_tables if 'CUSTOMER' in t.upper() and ('360' in t.upper() or 'UNIFIED' in t.upper())]
            
            if len(customer_360_tables) >= 1:
                # Verify it has data
                table_name = customer_360_tables[0]
                snowflake_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = snowflake_cur.fetchone()[0]
                
                if row_count > 0:
                    test_steps[8]["status"] = "passed"
                    test_steps[8]["Result_Message"] = f"✅ customer_360 view created with {row_count} customers"
                else:
                    test_steps[8]["status"] = "partial"
                    test_steps[8]["Result_Message"] = "⚠️ customer_360 table exists but has no data"
            else:
                test_steps[8]["status"] = "partial"
                test_steps[8]["Result_Message"] = f"⚠️ customer_360 table not found (found: {all_tables[:3]})"

        finally:
            snowflake_cur.close()
            snowflake_conn.close()

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {"score": score, "metadata": {"test_steps": test_steps}}
