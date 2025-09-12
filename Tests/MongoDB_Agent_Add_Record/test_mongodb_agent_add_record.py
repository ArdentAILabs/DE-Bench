# Braintrust-only MongoDB test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
from typing import List
from Configs.MongoConfig import syncMongoClient
from Fixtures.base_fixture import DEBenchFixture


current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the module path dynamically
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This is the main entry point for the Braintrust system.
    """
    from Fixtures.MongoDB.mongo_resources import MongoDBFixture

    # Initialize MongoDB fixture with custom configuration
    custom_mongo_config = {
        "resource_id": "mongodb_agent_add_record_test",
        "databases": [
            {
                "name": "agent_test_database",
                "collections": [
                    {
                        "name": "agent_test_collection",
                        "data": [
                            {"name": "Alice", "age": 25, "role": "tester"},
                            {"name": "Bob", "age": 30, "role": "developer"},
                        ],
                    },
                    {"name": "backup_collection", "data": []},
                ],
            }
        ],
    }

    mongo_fixture = MongoDBFixture(custom_config=custom_mongo_config)
    return [mongo_fixture]


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully added a record to MongoDB.

    This function creates test steps, performs validation, and returns detailed results
    for the Braintrust evaluation system.

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'success' boolean and 'test_steps' list with validation details
    """
    # Create test steps for this validation
    test_steps = [
        {
            "name": "MongoDB Record Addition",
            "description": "Verify that AI agent added 'John Doe' record to MongoDB",
            "status": "running",
            "Result_Message": "Checking if 'John Doe' record was added to agent_test_collection...",
        }
    ]

    success = False

    try:
        # Use fixture to get database connection for validation
        mongo_fixture = None
        if fixtures:
            mongo_fixture = next(
                (f for f in fixtures if f.get_resource_type() == "mongo_resource"), None
            )

        if mongo_fixture:
            # Use fixture's helper method for consistent connection
            db = mongo_fixture.get_database("agent_test_database")
        else:
            # Fallback to direct connection if no fixtures provided
            db = syncMongoClient["agent_test_database"]

        collection = db["agent_test_collection"]
        record = collection.find_one({"name": "John Doe", "age": 30})

        if record is not None:
            # Verify the record contents match expectations
            if record["name"] == "John Doe" and record["age"] == 30:
                test_steps[0]["status"] = "passed"
                test_steps[0]["Result_Message"] = (
                    "✅ AI agent successfully added John Doe record with correct values: "
                    f"name='{record['name']}', age={record['age']}"
                )
                success = True
            else:
                test_steps[0]["status"] = "failed"
                test_steps[0]["Result_Message"] = (
                    f"❌ Record found but values incorrect. Expected: name='John Doe', age=30. "
                    f"Found: name='{record.get('name')}', age={record.get('age')}"
                )
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = (
                "❌ John Doe record was not found in agent_test_collection. "
                "AI agent may not have executed the MongoDB insertion correctly."
            )

    except Exception as e:
        test_steps[0]["status"] = "failed"
        test_steps[0]["Result_Message"] = f"❌ Database validation error: {str(e)}"

    return {"success": success, "test_steps": test_steps}
