import pytest
import json
import time
import os
from typing import Dict, List, Any, Optional
from typing_extensions import TypedDict
from Configs.MongoConfig import syncMongoClient
from pymongo.errors import CollectionInvalid
from Fixtures.base_fixture import DEBenchFixture


# Type definitions for MongoDB resources
class MongoCollectionConfig(TypedDict):
    name: str
    data: List[Dict[str, Any]]


class MongoDatabaseConfig(TypedDict):
    name: str
    collections: List[MongoCollectionConfig]


class MongoResourceConfig(TypedDict):
    resource_id: str
    databases: List[MongoDatabaseConfig]


class MongoResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    created_resources: List[Dict[str, str]]


class MongoDBFixture(
    DEBenchFixture[MongoResourceConfig, MongoResourceData, Dict[str, Any]]
):
    """MongoDB fixture implementation following the DEBenchFixture interface"""

    def get_client(self):
        """Get the MongoDB client. Useful for validation and testing."""
        return syncMongoClient

    def get_database(self, database_name: str):
        """Get a specific MongoDB database. Useful for validation and testing."""
        return syncMongoClient[database_name]

    def setup_resource(
        self, resource_config: Optional[MongoResourceConfig] = None
    ) -> MongoResourceData:
        """
        Set up MongoDB resource based on configuration.
        Template structure: {"resource_id": "id", "databases": [{"name": "db", "collections": [{"name": "col", "data": []}]}]}
        """
        # Determine which config to use (priority: resource_config > custom_config > default_config)
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        print(
            f"Setting up MongoDB resource with config: {config.get('resource_id', 'default')}"
        )
        creation_start = time.time()

        created_resources = []

        # Process databases from template
        if "databases" in config:
            for db_config in config["databases"]:
                db_name = db_config["name"]
                db = syncMongoClient[db_name]

                # Process collections in this database
                if "collections" in db_config:
                    for collection_config in db_config["collections"]:
                        collection_name = collection_config["name"]

                        # Create collection with error handling
                        try:
                            db.create_collection(collection_name)
                        except CollectionInvalid:
                            db.drop_collection(collection_name)
                            db.create_collection(collection_name)

                        created_resources.append(
                            {"db": db_name, "collection": collection_name}
                        )

                        # Add data if specified
                        if "data" in collection_config:
                            collection = db[collection_name]
                            for record in collection_config["data"]:
                                collection.insert_one(record)

        creation_end = time.time()
        print(f"MongoDB resource creation took {creation_end - creation_start:.2f}s")

        resource_id = config.get("resource_id", f"mongo_resource_{int(time.time())}")

        # Create detailed resource data
        resource_data = {
            "resource_id": resource_id,
            "type": "mongodb_resource",
            "creation_time": time.time(),
            "creation_duration": creation_end - creation_start,
            "description": f"A MongoDB resource",
            "status": "active",
            "created_resources": created_resources,
        }

        print(f"MongoDB resource {resource_id} created successfully")
        return resource_data

    def teardown_resource(self, resource_data: MongoResourceData) -> None:
        """Clean up MongoDB resource"""
        resource_id = resource_data.get("resource_id", "unknown")
        print(f"Cleaning up MongoDB resource {resource_id}")

        try:
            # Clean up created resources in reverse order
            created_resources = resource_data.get("created_resources", [])
            for resource in reversed(created_resources):
                db = syncMongoClient[resource["db"]]
                db.drop_collection(resource["collection"])
            print(f"MongoDB resource {resource_id} cleaned up successfully")
        except Exception as e:
            print(f"Error cleaning up MongoDB resource: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "mongo_resource"

    @classmethod
    def get_default_config(cls) -> MongoResourceConfig:
        """Return a default MongoDB configuration"""
        return {
            "resource_id": "test_mongo_resource",
            "databases": [
                {
                    "name": "test_database",
                    "collections": [{"name": "test_collection", "data": []}],
                }
            ],
        }

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create MongoDB config section using the fixture's resource data.

        Returns:
            Dictionary containing the mongoDB service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "MongoDB resource data not available - ensure setup_resource was called"
            )

        # Extract database names from created resources
        database_names = [db["name"] for db in resource_data["created_resources"]]

        return {
            "mongoDB": {
                "connectionString": self._connection_string,
                "databases": [{"name": db_name} for db_name in database_names],
            }
        }


# Global fixture instance - this is the only way to access MongoDB resources now
mongo_fixture = MongoDBFixture()


@pytest.fixture(scope="function")
def mongo_resource(request):
    """
    A function-scoped fixture that creates MongoDB resources based on template.
    Uses the DEBenchFixture implementation.
    """
    start_time = time.time()
    test_name = request.node.name
    print(f"Worker {os.getpid()}: Starting mongo_resource for {test_name}")

    build_template = request.param

    # Use the fixture class
    resource_data = mongo_fixture.setup_resource(build_template)

    # Add test-specific metadata
    resource_data.update(
        {
            "test_name": test_name,
            "worker_pid": os.getpid(),
        }
    )

    fixture_end_time = time.time()
    print(
        f"Worker {os.getpid()}: MongoDB fixture setup took {fixture_end_time - start_time:.2f}s total"
    )

    yield resource_data

    # Use the fixture class for teardown
    mongo_fixture.teardown_resource(resource_data)
