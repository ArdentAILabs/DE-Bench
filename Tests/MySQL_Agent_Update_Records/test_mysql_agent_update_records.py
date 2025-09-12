# Braintrust-only MySQL test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import mysql.connector
from typing import List
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This MySQL test validates that AI can update records based on age criteria.
    """
    from Fixtures.MySQL.mysql_resources import MySQLFixture

    # Initialize MySQL fixture with test-specific configuration
    custom_mysql_config = {
        "resource_id": "mysql_agent_update_records_test",
        "databases": [
            {
                "name": "update_records_test_db",
                "tables": [
                    {
                        "name": "users",
                        "columns": [
                            {
                                "name": "id",
                                "type": "INT AUTO_INCREMENT",
                                "primary_key": True,
                            },
                            {"name": "name", "type": "VARCHAR(100)", "not_null": True},
                            {
                                "name": "email",
                                "type": "VARCHAR(255)",
                                "unique": True,
                                "not_null": True,
                            },
                            {"name": "age", "type": "INT"},
                            {
                                "name": "created_at",
                                "type": "TIMESTAMP",
                                "default": "CURRENT_TIMESTAMP",
                            },
                        ],
                        "data": [
                            {
                                "name": "John Doe",
                                "email": "john@example.com",
                                "age": 32,
                            },
                            {
                                "name": "Jane Smith",
                                "email": "jane@example.com",
                                "age": 25,
                            },
                            {
                                "name": "Bob Johnson",
                                "email": "bob@example.com",
                                "age": 38,
                            },
                            {
                                "name": "Carol White",
                                "email": "carol@example.com",
                                "age": 29,
                            },
                        ],
                    }
                ],
            }
        ],
    }

    mysql_fixture = MySQLFixture(custom_config=custom_mysql_config)
    return [mysql_fixture]


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully updated MySQL records based on age criteria.

    Expected behavior:
    - Users over 30 (John Doe: 32, Bob Johnson: 38) should be updated to age 35
    - Users 30 and under (Jane Smith: 25, Carol White: 29) should remain unchanged

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to update user ages",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the MySQL update task...",
        },
        {
            "name": "Age Update Validation",
            "description": "Verify that users over 30 were updated to age 35",
            "status": "running",
            "Result_Message": "Validating that users over 30 were updated to age 35...",
        },
        {
            "name": "Younger Users Unchanged",
            "description": "Verify that users 30 and under were not modified",
            "status": "running",
            "Result_Message": "Validating that younger users remained unchanged...",
        },
    ]

    overall_success = False

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0][
                "Result_Message"
            ] = "❌ AI Agent task execution failed or returned no result"
            return {"success": False, "test_steps": test_steps}

        test_steps[0]["status"] = "passed"
        test_steps[0][
            "Result_Message"
        ] = "✅ AI Agent completed task execution successfully"

        # Use fixture to get database connection for validation
        mysql_fixture = None
        if fixtures:
            mysql_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "mysql_resource"), None
            )

        if mysql_fixture:
            # Use fixture's helper method for consistent connection
            db_connection = mysql_fixture.get_connection(
                database="update_records_test_db"
            )
        else:
            raise Exception("MySQL fixture not found")

        try:
            # Step 2: Verify users over 30 were updated to 35
            db_cursor.execute(
                "SELECT name, age FROM users WHERE name IN ('John Doe', 'Bob Johnson') ORDER BY name"
            )
            older_users = db_cursor.fetchall()

            expected_older = [("Bob Johnson", 35), ("John Doe", 35)]

            if older_users == expected_older:
                test_steps[1]["status"] = "passed"
                test_steps[1]["Result_Message"] = (
                    f"✅ Users over 30 correctly updated to age 35: "
                    f"Bob Johnson={older_users[0][1]}, John Doe={older_users[1][1]}"
                )
            else:
                test_steps[1]["status"] = "failed"
                test_steps[1][
                    "Result_Message"
                ] = f"❌ Age updates incorrect. Expected: {expected_older}, Got: {older_users}"
                return {"success": False, "test_steps": test_steps}

            # Step 3: Verify users 30 and under were not changed
            db_cursor.execute(
                "SELECT name, age FROM users WHERE name IN ('Jane Smith', 'Carol White') ORDER BY name"
            )
            younger_users = db_cursor.fetchall()

            expected_younger = [("Carol White", 29), ("Jane Smith", 25)]

            if younger_users == expected_younger:
                test_steps[2]["status"] = "passed"
                test_steps[2]["Result_Message"] = (
                    f"✅ Younger users correctly unchanged: "
                    f"Carol White={younger_users[0][1]}, Jane Smith={younger_users[1][1]}"
                )
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Younger users modified incorrectly. Expected: {expected_younger}, Got: {younger_users}"
                return {"success": False, "test_steps": test_steps}

            # Final verification: Check all records
            db_cursor.execute("SELECT name, age FROM users ORDER BY name")
            all_users = db_cursor.fetchall()

            expected_final = [
                ("Bob Johnson", 35),  # Was 38, updated to 35
                ("Carol White", 29),  # Was 29, unchanged
                ("Jane Smith", 25),  # Was 25, unchanged
                ("John Doe", 35),  # Was 32, updated to 35
            ]

            if all_users == expected_final:
                overall_success = True
            else:
                test_steps[2]["status"] = "failed"
                test_steps[2][
                    "Result_Message"
                ] = f"❌ Final state verification failed. Expected: {expected_final}, Got: {all_users}"

        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Database validation error: {str(e)}"

    return {"success": overall_success, "test_steps": test_steps}
