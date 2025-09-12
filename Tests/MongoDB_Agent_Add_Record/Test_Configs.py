import os

User_Input = "Go to agent_test_collection in MongoDB and add another record. Please add the record with the name 'John Doe' and the age 30."

Configs = {
    "services": {
        "mongodb": {
            "connection_string": os.getenv("MONGODB_URI"),
            "databases": [
                {
                    "name": "agent_test_database",
                    "collections": [{"name": "agent_test_collection"}],
                }
            ],
        }
    }
}
