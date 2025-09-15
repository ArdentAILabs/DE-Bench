import importlib
import os
import pytest
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


@pytest.mark.postgres
@pytest.mark.database
@pytest.mark.four  # Difficulty 4 - complex schema analysis and intelligent table selection
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"postgres_stress_test_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"stress_test_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_postgresql_agent_sales_fact_table_stress(request, postgres_resource, supabase_account_resource):
    model_result = None  # Initialize before try block
    input_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Use the fixtures - following the exact pattern from working tests
    print("=== Starting PostgreSQL Sales Fact Table Stress Test ===")
    print(f"Using PostgreSQL instance from fixture: {postgres_resource['resource_id']}")
    print(f"Test directory: {input_dir}")

    test_steps = [
        {
            "name": "Schema Analysis",
            "description": "Agent analyzes 100+ tables to identify relevant ones for sales fact table",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Table Selection",
            "description": "Agent intelligently selects appropriate tables without explicit guidance",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Sales Fact Table Creation",
            "description": "Agent creates sales fact table with proper relationships and data",
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
        print("Verifying sales fact table was created correctly...")
        postgres_conn = postgres_resource["connection"]
        cursor = postgres_conn.cursor()
        
        try:
            # Check if sales_fact table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'sales_fact'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                test_steps[0]["status"] = "failed"
                test_steps[0]["Result_Message"] = "Sales fact table was not created"
                raise Exception("Sales fact table does not exist")
            
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = "Sales fact table exists"
            print("✅ Sales fact table exists")

            # Check table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'sales_fact' 
                ORDER BY ordinal_position;
            """)
            columns = cursor.fetchall()
            print(f"Sales fact table has {len(columns)} columns")
            
            # Check for key expected columns
            column_names = [col[0] for col in columns]
            expected_columns = ['sales_id', 'transaction_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'sale_date']
            missing_columns = [col for col in expected_columns if col not in column_names]
            
            if missing_columns:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = f"Missing expected columns: {missing_columns}"
                raise Exception(f"Missing expected columns: {missing_columns}")
            
            test_steps[1]["status"] = "passed"
            test_steps[1]["Result_Message"] = f"All expected columns present: {expected_columns}"
            print("✅ All expected columns present")

            # Check for data in the table
            cursor.execute("SELECT COUNT(*) FROM sales_fact")
            row_count = cursor.fetchone()[0]
            print(f"Sales fact table has {row_count} rows")
            
            if row_count == 0:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "Sales fact table is empty"
                raise Exception("Sales fact table contains no data")
            
            # Check for foreign key relationships
            cursor.execute("""
                SELECT 
                    tc.constraint_name, 
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_name='sales_fact';
            """)
            foreign_keys = cursor.fetchall()
            print(f"Sales fact table has {len(foreign_keys)} foreign key constraints")
            
            # Check data integrity - no orphaned records
            cursor.execute("""
                SELECT COUNT(*) FROM sales_fact sf
                LEFT JOIN transactions t ON sf.transaction_id = t.transaction_id
                WHERE t.transaction_id IS NULL;
            """)
            orphaned_transactions = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM sales_fact sf
                LEFT JOIN customers c ON sf.customer_id = c.customer_id
                WHERE c.customer_id IS NULL;
            """)
            orphaned_customers = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM sales_fact sf
                LEFT JOIN products p ON sf.product_id = p.product_id
                WHERE p.product_id IS NULL;
            """)
            orphaned_products = cursor.fetchone()[0]
            
            total_orphaned = orphaned_transactions + orphaned_customers + orphaned_products
            
            if total_orphaned > 0:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = f"Data integrity issues: {total_orphaned} orphaned records"
                raise Exception(f"Data integrity issues: {total_orphaned} orphaned records")
            
            test_steps[2]["status"] = "passed"
            test_steps[2]["Result_Message"] = f"Sales fact table created successfully with {row_count} rows and proper relationships"
            print("✅ All validations passed! Sales fact table created successfully with proper relationships and data integrity.")
            
        finally:
            cursor.close()

    finally:
        try:
            # CLEANUP - following exact pattern from working tests
            if request.config.getoption("--mode") == "Ardent":
                custom_info['job_id'] = model_result.get("id") if model_result else None
            cleanup_model_artifacts(Configs=Test_Configs.Configs, custom_info=custom_info)

        except Exception as e:
            print(f"Error during cleanup: {e}")
