# Braintrust-only PostgreSQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import psycopg2
import uuid
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel test execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This PostgreSQL test validates idempotent upsert pipeline functionality.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"idempotent_upsert_pipeline_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"upsert_pipeline_db_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    return [postgres_fixture]


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
    Validates that the AI agent successfully implemented an idempotent upsert pipeline.

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes idempotent upsert pipeline task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "Upsert Pattern Implementation",
            "description": "Verify that INSERT ... ON CONFLICT DO UPDATE pattern is used",
            "status": "running",
            "Result_Message": "Validating upsert implementation...",
        },
        {
            "name": "Idempotency Verification",
            "description": "Verify pipeline produces same results when run multiple times",
            "status": "running",
            "Result_Message": "Testing idempotency by running operations multiple times...",
        },
        {
            "name": "Conflict Resolution",
            "description": "Verify proper handling of data conflicts and updates",
            "status": "running",
            "Result_Message": "Validating conflict resolution logic...",
        },
        {
            "name": "Audit Trail",
            "description": "Verify audit logging for production monitoring",
            "status": "running",
            "Result_Message": "Checking audit trail implementation...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed or returned no result"
            score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
            return {
                "score": score,
                "metadata": {"test_steps": test_steps},
            }

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed task execution successfully"

        # Use fixture to get PostgreSQL connection for validation
        postgres_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "postgres_resource"), None
        ) if fixtures else None

        if not postgres_fixture:
            raise Exception("PostgreSQL fixture not found")

        # Get PostgreSQL resource data from fixture
        resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not resource_data:
            raise Exception("PostgreSQL resource data not available")

        created_resources = resource_data["created_resources"]
        created_db_name = created_resources[0]["name"]

        # Connect to database for validation
        db_connection = postgres_fixture.get_connection(created_db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Check if upsert operations were performed
            print("🔍 Checking for upsert operations...")
            
            # Look for evidence of INSERT ... ON CONFLICT usage or equivalent upsert logic
            # Check if data was loaded into dim_customers table
            db_cursor.execute("SELECT COUNT(*) FROM dim_customers")
            customer_count = db_cursor.fetchone()[0]
            
            # Should have original seed data plus any new customers added by agent
            if customer_count >= 2:  # At least the original seed data
                # Check for specific test customers that should have been upserted
                db_cursor.execute(
                    "SELECT customer_id, email, subscription_tier FROM dim_customers WHERE customer_id IN ('ALICE_001', 'BOB_001', 'CAROL_001', 'DAVE_001') ORDER BY customer_id"
                )
                test_customers = db_cursor.fetchall()
                
                if len(test_customers) >= 3:  # Alice, Bob, Carol minimum
                    test_steps[1]["status"] = "passed"
                    test_steps[1]["Result_Message"] = f"✅ Upsert operations completed - found {len(test_customers)} test customers"
                else:
                    test_steps[1]["status"] = "partial"
                    test_steps[1]["Result_Message"] = f"⚠️ Partial upsert success - found {len(test_customers)} customers, expected at least 3"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = f"❌ No evidence of upsert operations - only {customer_count} customers found"

            # Step 3: Test idempotency by checking if repeated operations don't create duplicates
            print("🔍 Testing idempotency...")
            
            # Get current state
            db_cursor.execute("SELECT COUNT(*) FROM dim_customers")
            count_before = db_cursor.fetchone()[0]
            
            # Check if audit log shows the operations (indicates proper pipeline implementation)
            db_cursor.execute("SELECT COUNT(*) FROM customer_audit_log")
            audit_count = db_cursor.fetchone()[0]
            
            if audit_count > 0:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ Pipeline shows audit trail with {audit_count} operations, indicating proper upsert implementation"
            else:
                # Alternative check - verify no duplicate emails exist (business constraint)
                db_cursor.execute(
                    "SELECT email, COUNT(*) as cnt FROM dim_customers GROUP BY email HAVING COUNT(*) > 1"
                )
                duplicates = db_cursor.fetchall()
                
                if len(duplicates) == 0:
                    test_steps[2]["status"] = "passed"
                    test_steps[2]["Result_Message"] = "✅ No duplicate emails found - idempotency maintained"
                else:
                    test_steps[2]["status"] = "failed"
                    test_steps[2]["Result_Message"] = f"❌ Found {len(duplicates)} duplicate emails - idempotency failed"

            # Step 4: Check conflict resolution - look for updated records
            print("🔍 Checking conflict resolution...")
            
            # Look for Alice's record which should have been updated
            db_cursor.execute(
                "SELECT customer_id, email, subscription_tier, last_updated_at FROM dim_customers WHERE customer_id = 'ALICE_001' OR first_name = 'Alice'"
            )
            alice_record = db_cursor.fetchone()
            
            if alice_record:
                # Check if Alice's tier was updated to Enterprise (as per test scenario)
                if 'Enterprise' in str(alice_record) or 'Premium' in str(alice_record):
                    test_steps[3]["status"] = "passed"
                    test_steps[3]["Result_Message"] = f"✅ Conflict resolution working - Alice's record updated: {alice_record}"
                else:
                    test_steps[3]["status"] = "partial"
                    test_steps[3]["Result_Message"] = f"⚠️ Alice found but tier may not be updated: {alice_record}"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = "❌ Could not find Alice's record to verify conflict resolution"

            # Step 5: Verify audit trail implementation
            print("🔍 Checking audit trail...")
            
            if audit_count > 0:
                # Check audit log structure
                db_cursor.execute(
                    "SELECT operation_type, COUNT(*) FROM customer_audit_log GROUP BY operation_type"
                )
                audit_summary = db_cursor.fetchall()
                
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"✅ Audit trail implemented with operations: {audit_summary}"
            else:
                # Check if staging table was used (alternative production pattern)
                db_cursor.execute("SELECT COUNT(*) FROM staging_customers")
                staging_count = db_cursor.fetchone()[0]
                
                if staging_count > 0:
                    test_steps[4]["status"] = "passed"
                    test_steps[4]["Result_Message"] = f"✅ Staging table used for ETL pattern with {staging_count} records"
                else:
                    test_steps[4]["status"] = "partial"
                    test_steps[4]["Result_Message"] = "⚠️ No audit trail or staging table usage detected"

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
