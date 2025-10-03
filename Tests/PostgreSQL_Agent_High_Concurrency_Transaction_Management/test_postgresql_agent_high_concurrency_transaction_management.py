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
    This PostgreSQL test validates high-concurrency transaction management.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture with test-specific configuration
    custom_postgres_config = {
        "resource_id": f"high_concurrency_txn_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,  # Pass current module path for SQL file resolution
        "databases": [
            {
                "name": f"concurrency_txn_db_{test_timestamp}_{test_uuid}",
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
    Validates that the AI agent successfully implemented high-concurrency transaction management.

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes transaction management task",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "Transaction Processing",
            "description": "Verify transaction processing with proper ACID guarantees",
            "status": "running",
            "Result_Message": "Validating transaction processing...",
        },
        {
            "name": "Concurrency Control",
            "description": "Verify proper handling of concurrent operations",
            "status": "running",
            "Result_Message": "Testing concurrency control mechanisms...",
        },
        {
            "name": "Data Consistency",
            "description": "Verify account balances and transaction integrity",
            "status": "running",
            "Result_Message": "Validating data consistency...",
        },
        {
            "name": "Audit Trail Compliance",
            "description": "Verify complete audit trail for financial compliance",
            "status": "running",
            "Result_Message": "Checking audit trail completeness...",
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
            # Step 2: Verify transaction processing
            print("🔍 Checking transaction processing...")
            
            # Check if transactions were created beyond the seed data
            db_cursor.execute("SELECT COUNT(*) FROM transactions")
            transaction_count = db_cursor.fetchone()[0]
            
            # Check transaction status distribution
            db_cursor.execute("""
                SELECT status, COUNT(*) 
                FROM transactions 
                GROUP BY status 
                ORDER BY status
            """)
            transaction_status = db_cursor.fetchall()
            
            if transaction_count > 5:  # More than just seed transactions
                # Look for different transaction types
                db_cursor.execute("""
                    SELECT transaction_type, COUNT(*) 
                    FROM transactions 
                    GROUP BY transaction_type
                """)
                transaction_types = db_cursor.fetchall()
                
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = f"✅ Transaction processing active - {transaction_count} total transactions with types: {transaction_types}"
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1]["Result_Message"] = f"❌ No evidence of transaction processing - only {transaction_count} transactions found"

            # Step 3: Verify concurrency control mechanisms
            print("🔍 Testing concurrency control...")
            
            # Check for evidence of concurrent transaction handling
            # Look for transactions with retry counts (indicates deadlock handling)
            db_cursor.execute("SELECT COUNT(*) FROM transactions WHERE retry_count > 0")
            retry_transactions = db_cursor.fetchone()[0]
            
            # Check for transaction locks (indicates proper locking strategy)
            db_cursor.execute("SELECT COUNT(*) FROM transaction_locks")
            active_locks = db_cursor.fetchone()[0]
            
            # Check for optimistic locking usage (version field increments)
            db_cursor.execute("SELECT COUNT(*) FROM accounts WHERE version > 0")
            versioned_accounts = db_cursor.fetchone()[0]
            
            concurrency_indicators = []
            if retry_transactions > 0:
                concurrency_indicators.append(f"{retry_transactions} retry transactions")
            if active_locks > 0:
                concurrency_indicators.append(f"{active_locks} transaction locks")
            if versioned_accounts > 0:
                concurrency_indicators.append(f"{versioned_accounts} versioned accounts")
            
            if len(concurrency_indicators) >= 1:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ Concurrency control mechanisms present: {', '.join(concurrency_indicators)}"
            else:
                test_steps[2]["status"] = "partial"
                test_steps[2]["Result_Message"] = "⚠️ Limited evidence of concurrency control mechanisms"

            # Step 4: Verify data consistency
            print("🔍 Checking data consistency...")
            
            # Check that all account balances are non-negative
            db_cursor.execute("SELECT COUNT(*) FROM accounts WHERE balance < 0")
            negative_balances = db_cursor.fetchone()[0]
            
            # Check balance consistency with transaction history
            db_cursor.execute("""
                SELECT a.account_id, a.balance,
                       COALESCE(SUM(CASE WHEN t.to_account_id = a.account_id THEN t.amount ELSE 0 END), 0) as credits,
                       COALESCE(SUM(CASE WHEN t.from_account_id = a.account_id THEN t.amount ELSE 0 END), 0) as debits
                FROM accounts a
                LEFT JOIN transactions t ON (t.to_account_id = a.account_id OR t.from_account_id = a.account_id)
                    AND t.status = 'COMPLETED'
                GROUP BY a.account_id, a.balance
                HAVING ABS(a.balance - (credits - debits)) > 0.01
            """)
            inconsistent_balances = db_cursor.fetchall()
            
            if negative_balances == 0 and len(inconsistent_balances) == 0:
                test_steps[3]["status"] = "passed"
                test_steps[3]["Result_Message"] = "✅ Data consistency maintained - no negative balances or inconsistencies"
            elif negative_balances == 0:
                test_steps[3]["status"] = "partial"
                test_steps[3]["Result_Message"] = f"⚠️ No negative balances but {len(inconsistent_balances)} balance inconsistencies found"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = f"❌ Data consistency violated - {negative_balances} negative balances, {len(inconsistent_balances)} inconsistencies"

            # Step 5: Verify audit trail compliance
            print("🔍 Checking audit trail compliance...")
            
            # Check if balance history is being maintained
            db_cursor.execute("SELECT COUNT(*) FROM balance_history")
            balance_history_count = db_cursor.fetchone()[0]
            
            # Check if completed transactions have corresponding balance history
            db_cursor.execute("""
                SELECT COUNT(*) FROM transactions t
                WHERE t.status = 'COMPLETED' 
                AND NOT EXISTS (
                    SELECT 1 FROM balance_history bh 
                    WHERE bh.transaction_id = t.transaction_id
                )
            """)
            missing_audit_records = db_cursor.fetchone()[0]
            
            # Check for idempotency key usage (prevents duplicate transactions)
            db_cursor.execute("SELECT COUNT(DISTINCT idempotency_key) FROM transactions WHERE idempotency_key IS NOT NULL")
            idempotent_transactions = db_cursor.fetchone()[0]
            
            audit_score = 0
            audit_details = []
            
            if balance_history_count >= transaction_count:
                audit_score += 1
                audit_details.append(f"{balance_history_count} balance history records")
            
            if missing_audit_records == 0:
                audit_score += 1
                audit_details.append("complete transaction audit trail")
                
            if idempotent_transactions > 0:
                audit_score += 1
                audit_details.append(f"{idempotent_transactions} idempotent transactions")
            
            if audit_score >= 2:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"✅ Audit trail compliance met: {', '.join(audit_details)}"
            elif audit_score >= 1:
                test_steps[4]["status"] = "partial"
                test_steps[4]["Result_Message"] = f"⚠️ Partial audit compliance: {', '.join(audit_details)}"
            else:
                test_steps[4]["status"] = "failed"
                test_steps[4]["Result_Message"] = "❌ Insufficient audit trail for compliance requirements"

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
