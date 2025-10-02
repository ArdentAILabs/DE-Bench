# Braintrust-only PostgreSQL test - no pytest dependencies
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
    This PostgreSQL test validates logical replication setup.
    
    Note: This test simulates replication setup within a single database
    using schemas to represent publisher and subscriber.
    """
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture

    # Initialize PostgreSQL fixture - will use schemas to simulate multi-instance
    custom_postgres_config = {
        "resource_id": f"logical_replication_{test_timestamp}_{test_uuid}",
        "test_module_path": __file__,
        "databases": [
            {
                "name": f"replication_db_{test_timestamp}_{test_uuid}",
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
    """
    from extract_test_configs import create_config_from_fixtures

    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": Test_Configs.User_Input,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully implemented logical replication.
    
    Note: Due to fixture limitations, full replication may not be testable.
    We validate the setup steps (publication, subscription configuration).
    """
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes replication setup",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the task...",
        },
        {
            "name": "WAL Level Configuration",
            "description": "Verify wal_level set to logical",
            "status": "running",
            "Result_Message": "Checking WAL level setting...",
        },
        {
            "name": "Publication Creation",
            "description": "Verify publication created for tables",
            "status": "running",
            "Result_Message": "Checking for publications...",
        },
        {
            "name": "Publication Tables",
            "description": "Verify correct tables in publication",
            "status": "running",
            "Result_Message": "Validating publication tables...",
        },
        {
            "name": "Replication Slot",
            "description": "Verify replication slot exists",
            "status": "running",
            "Result_Message": "Checking replication slots...",
        },
        {
            "name": "Subscription Configuration",
            "description": "Check for subscription setup or documentation",
            "status": "running",
            "Result_Message": "Looking for subscription setup...",
        },
        {
            "name": "Monitoring Setup",
            "description": "Verify replication monitoring views/functions",
            "status": "running",
            "Result_Message": "Checking monitoring infrastructure...",
        },
        {
            "name": "Documentation",
            "description": "Verify setup is documented for operations team",
            "status": "running",
            "Result_Message": "Checking for documentation...",
        },
    ]

    try:
        # Step 1: Check agent execution
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed successfully"

        # Get PostgreSQL fixture
        postgres_fixture = next(
            (f for f in fixtures if f.get_resource_type() == "postgres_resource"), None
        ) if fixtures else None

        if not postgres_fixture:
            raise Exception("PostgreSQL fixture not found")

        resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not resource_data:
            raise Exception("PostgreSQL resource data not available")

        created_resources = resource_data["created_resources"]
        created_db_name = created_resources[0]["name"]

        # Connect to database
        db_connection = postgres_fixture.get_connection(created_db_name)
        db_cursor = db_connection.cursor()

        try:
            # Step 2: Check WAL level
            print("🔍 Checking WAL level...")
            
            db_cursor.execute("SHOW wal_level")
            wal_level = db_cursor.fetchone()[0]
            
            if wal_level == 'logical':
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = "✅ wal_level set to 'logical'"
            else:
                test_steps[1]["status"] = "partial"
                test_steps[1]["Result_Message"] = f"⚠️ wal_level is '{wal_level}' (logical replication requires 'logical')"

            # Step 3: Check for publications
            print("🔍 Checking for publications...")
            
            db_cursor.execute("SELECT pubname FROM pg_publication")
            publications = db_cursor.fetchall()
            
            if len(publications) >= 1:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = f"✅ Found {len(publications)} publication(s): {[p[0] for p in publications]}"
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2]["Result_Message"] = "❌ No publications found"

            # Step 4: Check publication tables
            print("🔍 Checking publication tables...")
            
            if len(publications) > 0:
                pub_name = publications[0][0]
                db_cursor.execute(f"""
                    SELECT schemaname, tablename 
                    FROM pg_publication_tables 
                    WHERE pubname = %s
                """, (pub_name,))
                pub_tables = db_cursor.fetchall()
                
                # Check if users, orders, products are in publication
                table_names = [t[1] for t in pub_tables]
                expected_tables = ['users', 'orders', 'products']
                found_tables = [t for t in expected_tables if t in table_names]
                
                if len(found_tables) >= 2:
                    test_steps[3]["status"] = "passed"
                    test_steps[3]["Result_Message"] = f"✅ Publication includes {len(found_tables)}/{len(expected_tables)} expected tables: {found_tables}"
                elif len(pub_tables) > 0:
                    test_steps[3]["status"] = "partial"
                    test_steps[3]["Result_Message"] = f"⚠️ Publication has {len(pub_tables)} tables but may not include all expected ones"
                else:
                    test_steps[3]["status"] = "failed"
                    test_steps[3]["Result_Message"] = "❌ Publication has no tables"
            else:
                test_steps[3]["status"] = "failed"
                test_steps[3]["Result_Message"] = "❌ No publication to check"

            # Step 5: Check replication slots
            print("🔍 Checking replication slots...")
            
            db_cursor.execute("SELECT slot_name, slot_type, active FROM pg_replication_slots")
            repl_slots = db_cursor.fetchall()
            
            if len(repl_slots) >= 1:
                test_steps[4]["status"] = "passed"
                test_steps[4]["Result_Message"] = f"✅ Found {len(repl_slots)} replication slot(s)"
            else:
                test_steps[4]["status"] = "partial"
                test_steps[4]["Result_Message"] = "⚠️ No replication slots (created when subscriber connects)"

            # Step 6: Check for subscription (may not exist in single-DB setup)
            print("🔍 Checking for subscriptions...")
            
            db_cursor.execute("SELECT subname FROM pg_subscription")
            subscriptions = db_cursor.fetchall()
            
            if len(subscriptions) >= 1:
                test_steps[5]["status"] = "passed"
                test_steps[5]["Result_Message"] = f"✅ Found {len(subscriptions)} subscription(s): {[s[0] for s in subscriptions]}"
            else:
                # Check if there's documentation or comments about subscription
                test_steps[5]["status"] = "partial"
                test_steps[5]["Result_Message"] = "⚠️ No subscription found (may require separate database instance)"

            # Step 7: Check monitoring setup
            print("🔍 Checking monitoring views...")
            
            monitoring_elements = []
            
            # Check for custom monitoring views
            db_cursor.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public'
                AND (table_name LIKE '%replication%' OR table_name LIKE '%repl%' OR table_name LIKE '%lag%')
            """)
            monitoring_views = db_cursor.fetchall()
            
            if len(monitoring_views) > 0:
                monitoring_elements.append(f"{len(monitoring_views)} monitoring views")
            
            # Check for monitoring functions
            db_cursor.execute("""
                SELECT routine_name 
                FROM information_schema.routines 
                WHERE routine_schema = 'public'
                AND (routine_name LIKE '%replication%' OR routine_name LIKE '%repl%' OR routine_name LIKE '%lag%')
            """)
            monitoring_functions = db_cursor.fetchall()
            
            if len(monitoring_functions) > 0:
                monitoring_elements.append(f"{len(monitoring_functions)} monitoring functions")
            
            if len(monitoring_elements) > 0:
                test_steps[6]["status"] = "passed"
                test_steps[6]["Result_Message"] = f"✅ Monitoring infrastructure: {', '.join(monitoring_elements)}"
            else:
                test_steps[6]["status"] = "partial"
                test_steps[6]["Result_Message"] = "⚠️ No custom monitoring (can use pg_stat_replication view)"

            # Step 8: Check for documentation (comments, readme files mentioned)
            print("🔍 Checking for documentation...")
            
            # This is hard to validate programmatically, so we'll mark as partial
            test_steps[7]["status"] = "partial"
            test_steps[7]["Result_Message"] = "⚠️ Documentation validation not automated (assumed present)"

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    score = sum([step["status"] == "passed" for step in test_steps]) / len(test_steps)
    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }
