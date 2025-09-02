import importlib
import os
import time
import uuid

import pytest

from model.Configure_Model import cleanup_model_artifacts, set_up_model_configs
from model.Run_Model import run_model

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]

@pytest.mark.snowflake
@pytest.mark.database
@pytest.mark.two  # Difficulty 2 - involves database operations and validation
@pytest.mark.parametrize("supabase_account_resource", [{"useArdent": True}], indirect=True)
@pytest.mark.parametrize("snowflake_resource", [{
    "resource_id": f"snowflake_test_{test_timestamp}_{test_uuid}",
    "database": f"BENCH_DB_{test_timestamp}_{test_uuid}",
    "schema": f"TEST_SCHEMA_{test_timestamp}_{test_uuid}",
    "sql_file": "users_schema.sql",
    "s3_config": {
        "bucket_url": "s3://de-bench/",
        "s3_key": "v1/users_simple_20250901_233609.parquet",
        "aws_key_id": "env:AWS_ACCESS_KEY",
        "aws_secret_key": "env:AWS_SECRET_KEY"
    }
}], indirect=True)
def test_snowflake_agent_add_record(request, snowflake_resource, supabase_account_resource):
    """
    Test that an AI agent can successfully add a new user record to a Snowflake table.
    
    This test:
    1. Sets up a Snowflake database with users table (via SQL file)
    2. Runs the AI model to add a new user record
    3. Verifies the record was added correctly
    4. Validates the data integrity
    """
    input_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=== Starting Snowflake Agent Add Record Test ===")
    print(f"Using Snowflake database: {snowflake_resource['database']}")
    print(f"Using Snowflake schema: {snowflake_resource['schema']}")
    print(f"Test directory: {input_dir}")

    test_steps = [
        {
            "name": "Initial Data Verification",
            "description": "Verify initial users table has expected data",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Model Execution",
            "description": "Run AI model to add new user record",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Record Addition Verification",
            "description": "Verify new user record was added successfully",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Data Integrity Check",
            "description": "Verify data integrity and constraints",
            "status": "did not reach",
            "Result_Message": "",
        },
    ]

    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    try:
        print("Snowflake database and schema setup completed by fixture")
        
        # Update configs with actual Snowflake connection details from fixture
        Test_Configs.Configs["services"]["snowflake"]["database"] = snowflake_resource["database"]
        Test_Configs.Configs["services"]["snowflake"]["schema"] = snowflake_resource["schema"]
        
        config_results = set_up_model_configs(
            Configs=Test_Configs.Configs,
            custom_info={
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )

        # SECTION 2: VERIFY INITIAL STATE
        print("Verifying initial database state...")
        connection = snowflake_resource["connection"]
        cursor = connection.cursor()
        
        try:
            # Check initial user count
            cursor.execute(f"SELECT COUNT(*) FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.USERS")
            initial_count = cursor.fetchone()[0]
            print(f"Initial user count: {initial_count}")
            
            # Verify we have the expected test users
            cursor.execute(f"""
                SELECT USER_ID, FIRST_NAME, LAST_NAME, EMAIL 
                FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.USERS 
                ORDER BY USER_ID
            """)
            initial_users = cursor.fetchall()
            print(f"Initial users: {initial_users}")
            
            if initial_count >= 3:  # We expect at least 3 users from schema.sql
                test_steps[0]["status"] = "passed"
                test_steps[0]["Result_Message"] = f"Initial verification passed. Found {initial_count} users."
            else:
                raise Exception(f"Expected at least 3 initial users, found {initial_count}")
                
        finally:
            cursor.close()

        # SECTION 3: RUN THE MODEL
        start_time = time.time()
        print("Running model to add new user record...")
        model_result = run_model(
            container=None,
            task=Test_Configs.User_Input,
            configs=Test_Configs.Configs,
            extra_information={
                "useArdent": True,
                "publicKey": supabase_account_resource["publicKey"],
                "secretKey": supabase_account_resource["secretKey"],
            }
        )
        end_time = time.time()
        print(f"Model execution completed. Result: {model_result}")
        request.node.user_properties.append(("model_runtime", end_time - start_time))
        
        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "Model executed successfully"

        # SECTION 4: VERIFY THE NEW RECORD
        print("Verifying new user record was added...")
        cursor = connection.cursor()
        
        try:
            # Check new user count
            cursor.execute(f"SELECT COUNT(*) FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.USERS")
            final_count = cursor.fetchone()[0]
            print(f"Final user count: {final_count}")
            
            # Look for the new user (Sarah Johnson)
            cursor.execute(f"""
                SELECT USER_ID, FIRST_NAME, LAST_NAME, EMAIL, AGE, CITY, STATE, IS_ACTIVE, TOTAL_PURCHASES
                FROM {snowflake_resource['database']}.{snowflake_resource['schema']}.USERS
                WHERE FIRST_NAME = 'Sarah' AND LAST_NAME = 'Johnson'
            """)
            new_user = cursor.fetchone()

            if new_user:
                print(f"Found new user: {new_user}")
                user_id, first_name, last_name, email, age, city, state, is_active, total_purchases = new_user

                # Verify the details match what we requested
                assert first_name == 'Sarah', f"Expected first name 'Sarah', got '{first_name}'"
                assert last_name == 'Johnson', f"Expected last name 'Johnson', got '{last_name}'"
                assert email == 'sarah.johnson@newuser.com', f"Expected email 'sarah.johnson@newuser.com', got '{email}'"
                assert age == 35, f"Expected age 35, got {age}"
                assert city == 'Austin', f"Expected city 'Austin', got '{city}'"
                assert state == 'TX', f"Expected state 'TX', got '{state}'"
                assert is_active == True, f"Expected is_active True, got {is_active}"
                assert total_purchases == 0.00, f"Expected total_purchases 0.00, got {total_purchases}"
                
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"New user record added successfully. User ID: {user_id}"
                
            else:
                raise Exception("New user 'David Wilson' not found in database")
            
            # Verify count increased by exactly 1
            if final_count == initial_count + 1:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = f"Data integrity verified. Count increased from {initial_count} to {final_count}"
            else:
                raise Exception(f"Expected count to increase by 1, went from {initial_count} to {final_count}")
                
        finally:
            cursor.close()

        print("✅ All verifications passed! New user record added successfully.")

    finally:
        try:
            # Clean up model configs
            if config_results:
                cleanup_model_artifacts(
                    Configs=Test_Configs.Configs, 
                    custom_info={
                        **config_results,
                        "publicKey": supabase_account_resource["publicKey"],
                        "secretKey": supabase_account_resource["secretKey"],
                        'job_id': model_result.get("id") if model_result else None,
                    }
                )
        except Exception as e:
            print(f"Error during cleanup: {e}")
