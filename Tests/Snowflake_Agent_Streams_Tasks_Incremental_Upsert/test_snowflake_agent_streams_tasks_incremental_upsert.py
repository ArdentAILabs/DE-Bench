# Braintrust-only Snowflake test - Streams & Tasks for Incremental Upsert CDC
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

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Snowflake test validates that AI can implement Streams & Tasks for CDC.
    """
    from Fixtures.Snowflake.snowflake_fixture import SnowflakeFixture

    # Initialize Snowflake fixture with test-specific configuration
    custom_snowflake_config = {
        "resource_id": f"snowflake_streams_tasks_test_{test_timestamp}_{test_uuid}",
        "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
        "schema": f"CDC_SCHEMA_{test_timestamp}_{test_uuid}",
        "sql_file": f"{os.path.dirname(os.path.abspath(__file__))}/streams_tasks_setup.sql",
    }

    snowflake_fixture = SnowflakeFixture(custom_config=custom_snowflake_config)
    return [snowflake_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    This function has access to all fixture data after setup.
    """
    from extract_test_configs import create_config_from_fixtures

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully implemented a CDC pipeline
    using Snowflake Streams and Tasks for incremental upserts.

    Expected behavior:
    - A Stream should be created on the ORDERS table
    - A Task should be created to process the stream data
    - The Task should perform incremental upserts on ORDER_SUMMARY
    - The pipeline should handle INSERT, UPDATE, DELETE operations
    - Processing should be automatic and serverless

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes Streams & Tasks CDC pipeline task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the CDC pipeline task...",
        },
        {
            "name": "Stream Creation",
            "description": "Verify Stream is created on ORDERS table for change capture",
            "status": "running",
            "Result_Message": "Validating stream configuration...",
        },
        {
            "name": "Task Creation",
            "description": "Verify Task is created for automated stream processing",
            "status": "running",
            "Result_Message": "Checking task configuration...",
        },
        {
            "name": "CDC Processing Logic",
            "description": "Verify task implements proper incremental upsert logic",
            "status": "running",
            "Result_Message": "Testing CDC processing logic...",
        },
        {
            "name": "Data Pipeline Validation",
            "description": "Test pipeline with INSERT/UPDATE/DELETE operations",
            "status": "running",
            "Result_Message": "Validating end-to-end pipeline functionality...",
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

        # Use fixture to get Snowflake connection for validation
        snowflake_fixture = None
        if fixtures:
            snowflake_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "snowflake_resource"),
                None,
            )

        if not snowflake_fixture:
            raise Exception("Snowflake fixture not found")

        # Get connection using the fixture
        conn = snowflake_fixture.get_connection()
        cursor = conn.cursor()

        try:
            # Get the database and schema names from the fixture
            resource_data = getattr(snowflake_fixture, "_resource_data", None)
            if not resource_data:
                raise Exception("Snowflake resource data not available")

            database_name = resource_data.get("database")
            schema_name = resource_data.get("schema")

            # Step 2: Check for Stream creation
            cursor.execute(f"""
                SELECT stream_name, table_name, stream_type, mode
                FROM {database_name}.information_schema.streams
                WHERE stream_schema = '{schema_name}'
            """)
            streams = cursor.fetchall()

            orders_stream = None
            for stream in streams:
                if 'ORDERS' in stream[1].upper():  # table_name contains ORDERS
                    orders_stream = stream
                    break

            if orders_stream:
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"✅ Stream '{orders_stream[0]}' found on ORDERS table with mode '{orders_stream[3]}'"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = f"❌ No stream found on ORDERS table. Found {len(streams)} streams total"

            # Step 3: Check for Task creation
            cursor.execute(f"""
                SELECT task_name, schedule, warehouse, state
                FROM {database_name}.information_schema.tasks
                WHERE task_schema = '{schema_name}'
            """)
            tasks = cursor.fetchall()

            cdc_task = None
            for task in tasks:
                task_name = task[0].upper()
                if any(keyword in task_name for keyword in ['CDC', 'STREAM', 'UPSERT', 'ORDER']):
                    cdc_task = task
                    break

            if cdc_task:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ CDC Task '{cdc_task[0]}' found with state '{cdc_task[3]}'"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"❌ No CDC task found. Found {len(tasks)} tasks total"

            # Step 4: Check for proper CDC processing logic by examining task definition
            if cdc_task:
                cursor.execute(f"""
                    SELECT task_definition
                    FROM {database_name}.information_schema.tasks
                    WHERE task_schema = '{schema_name}' AND task_name = '{cdc_task[0]}'
                """)
                task_def_result = cursor.fetchone()
                
                if task_def_result:
                    task_definition = task_def_result[0].upper()
                    
                    # Check for key CDC patterns
                    has_stream_reference = orders_stream and orders_stream[0].upper() in task_definition
                    has_upsert_logic = any(keyword in task_definition for keyword in ['MERGE', 'UPSERT', 'INSERT', 'UPDATE'])
                    has_aggregation = any(keyword in task_definition for keyword in ['SUM', 'COUNT', 'AVG', 'GROUP BY'])
                    
                    if has_stream_reference and has_upsert_logic and has_aggregation:
                        test_steps[3]["status"] = "passed"
                        test_steps[3]["Result_Message"] = "✅ Task implements proper CDC logic with stream processing and aggregation"
                    else:
                        test_steps[3]["status"] = "failed"
                        test_steps[3]["Result_Message"] = f"❌ Task definition missing key CDC patterns (stream: {has_stream_reference}, upsert: {has_upsert_logic}, agg: {has_aggregation})"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3]["Result_Message"] = "❌ Could not retrieve task definition"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = "❌ No CDC task available to validate"

            # Step 5: Test the pipeline by inserting new data and checking results
            # First, get initial summary counts
            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDER_SUMMARY")
            initial_summary_count = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDERS")
            initial_orders_count = cursor.fetchone()[0]

            # Insert a new order to trigger the stream
            cursor.execute(f"""
                INSERT INTO {database_name}.{schema_name}.ORDERS 
                (CUSTOMER_ID, PRODUCT_ID, QUANTITY, PRICE, STATUS, ORDER_DATE, UPDATED_AT)
                VALUES (999, 999, 1, 99.99, 'TEST', CURRENT_DATE(), CURRENT_TIMESTAMP())
            """)

            # Check that data was inserted
            cursor.execute(f"SELECT COUNT(*) FROM {database_name}.{schema_name}.ORDERS")
            new_orders_count = cursor.fetchone()[0]

            # Check if summary table structure is appropriate for CDC
            cursor.execute(f"""
                SELECT column_name
                FROM {database_name}.information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = 'ORDER_SUMMARY'
                ORDER BY ordinal_position
            """)
            summary_columns = [row[0] for row in cursor.fetchall()]

            expected_cols = ['CUSTOMER_ID', 'TOTAL_ORDERS', 'TOTAL_AMOUNT', 'LAST_ORDER_DATE']
            has_required_cols = all(col in summary_columns for col in expected_cols)

            if new_orders_count > initial_orders_count and has_required_cols:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"✅ Pipeline structure validated: {new_orders_count} orders, summary table has required columns"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = f"❌ Pipeline validation failed: orders {initial_orders_count}->{new_orders_count}, required cols: {has_required_cols}"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum(1 for step in test_steps if step["status"] == "passed") / len(test_steps)
    
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
