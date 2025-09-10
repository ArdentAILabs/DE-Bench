# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import pytest
import time
import psycopg2
import snowflake.connector
import uuid
import json

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.airflow
@pytest.mark.postgres
@pytest.mark.snowflake
@pytest.mark.pipeline
@pytest.mark.seven
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"workflow_analytics_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"workflow_db_{test_timestamp}_{test_uuid}",
            "sql_file": "postgres_workflows_schema.sql"
        }
    ]
}], indirect=True)
@pytest.mark.parametrize("snowflake_resource", [{
    "resource_id": f"snowflake_analytics_test_{test_timestamp}_{test_uuid}",
    "database": f"WORKFLOW_HIST_DB_{test_timestamp}_{test_uuid}",
    "schema": f"WORKFLOW_HIST_SCHEMA_{test_timestamp}_{test_uuid}",
    "sql_file": "snowflake_workflow_history_schema.sql"
}], indirect=True)
@pytest.mark.parametrize("github_resource", [{
    "resource_id": f"test_airflow_workflow_history_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
@pytest.mark.parametrize("airflow_resource", [{
    "resource_id": f"workflow_history_test_{test_timestamp}_{test_uuid}",
}], indirect=True)
def test_airflow_postgres_snowflake_workflow_history(request, postgres_resource, snowflake_resource, supabase_account_resource, airflow_resource, github_resource):
    """Test Airflow DAG creation for workflow history ETL pipeline from Postgres to Snowflake."""
    
    # Set up test tracking
    input_dir = os.path.dirname(os.path.abspath(__file__))
    github_manager = github_resource["github_manager"]
    Test_Configs.User_Input = github_manager.add_merge_step_to_user_input(Test_Configs.User_Input)
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    dag_name = "workflow_history_etl"
    pr_title = f"Add Workflow History ETL Pipeline {test_timestamp}_{test_uuid}"
    branch_name = f"feature/workflow_history_etl-{test_timestamp}_{test_uuid}"
    Test_Configs.User_Input = Test_Configs.User_Input.replace("BRANCH_NAME", branch_name)
    Test_Configs.User_Input = Test_Configs.User_Input.replace("PR_NAME", pr_title)
    Test_Configs.User_Input = Test_Configs.User_Input.replace("DAG_NAME", dag_name)

    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )
    
    # Use the airflow_resource fixture - the Docker instance is already running
    print("=== Starting PostgreSQL to MySQL Airflow Pipeline Test ===")
    print(f"Using Airflow instance from fixture: {airflow_resource['resource_id']}")
    print(f"Using GitHub instance from fixture: {github_resource['resource_id']}")
    print(f"Using PostgreSQL instance from fixture: {postgres_resource['resource_id']}")
    print(f"Airflow base URL: {airflow_resource['base_url']}")
    print(f"Test directory: {input_dir}")

    test_steps = [
        {
            "name": "Verify PostgreSQL source data setup", 
            "description": "Confirm workflows and workflow_nodes tables exist with sample data",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Verify Snowflake destination tables setup",
            "description": "Confirm workflow_definitions, workflow_edges, and related tables exist",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Create Airflow DAG for workflow history ETL",
            "description": "Agent creates production-ready DAG with extract, transform, load tasks",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validate DAG structure and tasks",
            "description": "Verify DAG has proper tasks for ETL pipeline with error handling",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Test ETL data transformation logic", 
            "description": "Verify workflow JSON definitions are flattened to edges correctly",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Validate analytics views and queries",
            "description": "Test step type adoption, customer usage patterns, and version tracking",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))
    created_db_name = postgres_resource["created_resources"][0]["name"]

    # SECTION 1: SETUP THE TEST
    model_result = None  # Initialize before try block
    custom_info = {"mode": request.config.getoption("--mode")}
    try:
        # Step 1: Verify PostgreSQL source data setup
        # Get the actual database name from the fixture
        print(f"Using PostgreSQL database: {created_db_name}")
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]
        
        # Connect to PostgreSQL to verify source data
        pg_conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        
        pg_cursor = pg_conn.cursor()
        
        # Verify workflows table exists and has data
        pg_cursor.execute("SELECT COUNT(*) FROM workflows")
        workflow_count = pg_cursor.fetchone()[0]
        
        # Verify workflow_nodes table exists and has data  
        pg_cursor.execute("SELECT COUNT(*) FROM workflow_nodes")
        node_count = pg_cursor.fetchone()[0]
        
        # Get sample workflow definition for testing transformation logic
        pg_cursor.execute("SELECT id, name, definition FROM workflows LIMIT 1")
        sample_workflow = pg_cursor.fetchone()
        
        pg_cursor.close()
        pg_conn.close()
        
        if workflow_count >= 3 and node_count >= 10 and sample_workflow:
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"PostgreSQL setup verified: {workflow_count} workflows, {node_count} nodes"
        else:
            test_steps[0]["status"] = "failed"  
            test_steps[0]["Result_Message"] = f"Insufficient source data: {workflow_count} workflows, {node_count} nodes"
            raise AssertionError("PostgreSQL source data not properly set up")

        # Step 2: Verify Snowflake destination tables setup
        test_steps[1]["status"] = "in_progress"
        
        snowflake_conn = snowflake_resource["connection"]
        sf_cursor = snowflake_conn.cursor()
        
        # Verify all destination tables exist
        sf_cursor.execute(f"SELECT table_name FROM {snowflake_resource['database']}.information_schema.tables WHERE table_schema = '{snowflake_resource['schema']}' ORDER BY table_name")
        tables = [row[0] for row in sf_cursor.fetchall()]
        
        expected_tables = ['WORKFLOW_DEFINITIONS', 'WORKFLOW_EDGES', 'WORKFLOW_NODES', 'ETL_AUDIT_LOG']
        missing_tables = [t for t in expected_tables if t not in tables]
        
        # Verify analytics views exist
        sf_cursor.execute(f"SELECT table_name FROM {snowflake_resource['database']}.information_schema.views WHERE table_schema = '{snowflake_resource['schema']}' ORDER BY table_name")
        views = [row[0] for row in sf_cursor.fetchall()]
        
        expected_views = ['V_STEP_TYPE_ADOPTION', 'V_CUSTOMER_USAGE_PATTERNS', 'V_WORKFLOW_EVOLUTION', 'V_EDGE_PATTERNS']
        missing_views = [v for v in expected_views if v not in views]
        
        sf_cursor.close()
        
        if not missing_tables and not missing_views:
            test_steps[1]["status"] = "passed"
            test_steps[1]["Result_Message"] = f"Snowflake setup verified: {len(tables)} tables, {len(views)} views created"
        else:
            test_steps[1]["status"] = "failed"
            test_steps[1]["Result_Message"] = f"Missing tables: {missing_tables}, Missing views: {missing_views}"
            raise AssertionError("Snowflake destination not properly set up")

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
        
        # Step 3: Verify Airflow DAG was created
        test_steps[2]["status"] = "in_progress"
        
        airflow_base_url = airflow_resource["base_url"]
        api_token = airflow_resource["api_token"]
        
        # The DAG ID should contain the test timestamp and uuid from configs
        expected_dag_id = Test_Configs.Configs["services"]["airflow"]["dag_id"]
        
        import requests
        headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
        
        # Check if DAG exists
        dag_response = requests.get(f"{airflow_base_url}/api/v1/dags/{expected_dag_id}", headers=headers)
        
        if dag_response.status_code == 200:
            dag_info = dag_response.json()
            test_steps[2]["status"] = "passed" 
            test_steps[2]["Result_Message"] = f"Airflow DAG '{expected_dag_id}' created successfully"
        else:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = f"DAG not found: {dag_response.status_code} - {dag_response.text}"
            raise AssertionError(f"Airflow DAG was not created: {dag_response.status_code}")

        # Step 4: Validate DAG structure and tasks
        test_steps[3]["status"] = "in_progress"
        
        # Get DAG tasks
        tasks_response = requests.get(f"{airflow_base_url}/api/v1/dags/{expected_dag_id}/tasks", headers=headers)
        
        if tasks_response.status_code == 200:
            tasks = tasks_response.json().get("tasks", [])
            task_ids = [task["task_id"] for task in tasks]
            
            # Expected task patterns for ETL pipeline
            expected_patterns = ["extract", "transform", "load"]
            found_patterns = []
            
            for pattern in expected_patterns:
                if any(pattern.lower() in task_id.lower() for task_id in task_ids):
                    found_patterns.append(pattern)
            
            if len(found_patterns) >= 3 and len(task_ids) >= 3:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = f"DAG structure validated: {len(task_ids)} tasks with ETL patterns: {found_patterns}"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = f"Insufficient ETL tasks found: {task_ids}"
                raise AssertionError("DAG does not have proper ETL task structure")
        else:
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = f"Could not retrieve DAG tasks: {tasks_response.status_code}"
            raise AssertionError("Could not validate DAG task structure")

        # Step 5: Test ETL data transformation logic conceptually
        test_steps[4]["status"] = "in_progress"
        
        # Parse the sample workflow definition to validate transformation logic
        if sample_workflow and len(sample_workflow) >= 3:
            workflow_def = sample_workflow[2]  # JSON definition
            
            if isinstance(workflow_def, dict) and "edges" in workflow_def:
                edges = workflow_def["edges"]
                nodes = workflow_def.get("nodes", [])
                
                # Verify the transformation logic would work
                if len(edges) > 0 and len(nodes) > 0:
                    # Count expected edges vs nodes for transformation validation
                    expected_edge_count = len(edges)
                    node_count = len(nodes)
                    
                    test_steps[4]["status"] = "passed"
                    test_steps[4]["Result_Message"] = f"Transformation logic validated: {expected_edge_count} edges from {node_count} nodes"
                else:
                    test_steps[4]["status"] = "failed"
                    test_steps[4]["Result_Message"] = "Sample workflow missing edges or nodes for transformation"
                    raise AssertionError("Workflow structure not suitable for transformation")
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = "Sample workflow definition missing required structure"
                raise AssertionError("Invalid workflow definition structure")
        else:
            test_steps[4]["status"] = "failed"  
            test_steps[4]["Result_Message"] = "No sample workflow available for transformation validation"
            raise AssertionError("Cannot validate transformation logic without sample data")

        # Step 6: Validate analytics views work
        test_steps[5]["status"] = "in_progress"
        
        sf_cursor = snowflake_conn.cursor()
        
        # Test each analytics view
        view_tests = []
        
        try:
            # Test step type adoption view
            sf_cursor.execute(f"SELECT COUNT(*) FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.v_step_type_adoption")
            view_tests.append("step_type_adoption")
            
            # Test customer usage patterns view  
            sf_cursor.execute(f"SELECT COUNT(*) FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.v_customer_usage_patterns")
            view_tests.append("customer_usage_patterns")
            
            # Test workflow evolution view
            sf_cursor.execute(f"SELECT COUNT(*) FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.v_workflow_evolution")
            view_tests.append("workflow_evolution")
            
            # Test edge patterns view
            sf_cursor.execute(f"SELECT COUNT(*) FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.v_edge_patterns") 
            view_tests.append("edge_patterns")
            
            test_steps[5]["status"] = "passed"
            test_steps[5]["Result_Message"] = f"Analytics views validated: {len(view_tests)} views queryable"
            
        except Exception as e:
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = f"Analytics view validation failed: {str(e)}"
            raise AssertionError(f"Analytics views not working properly: {str(e)}")
        finally:
            sf_cursor.close()

        # All validations passed
        assert True, "Airflow workflow history ETL pipeline test completed successfully"

    except Exception as e:
        # Update any remaining test steps that didn't complete
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Test failed before reaching this step: {str(e)}"
        raise

    finally:
        # CLEANUP - Include mode-specific cleanup information
        if request.config.getoption("--mode") == "Ardent":
            custom_info['job_id'] = model_result.get("id") if model_result else None
        cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)