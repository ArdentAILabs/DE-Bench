import os
import importlib
import importlib.util
import time
import uuid
from typing import Dict, List, Any, Optional, Union
from typing_extensions import TypedDict, NotRequired
from Fixtures.Supabase_Account.supabase_account_resource import supabase_client
import requests
import jwt

# Type definitions for better code clarity and IDE support


class TestStepConfig(TypedDict):
    name: str
    description: str
    status: str
    Result_Message: str


class TestCaseInput(TypedDict):
    task: str
    configs: Dict[str, Any]
    mode: NotRequired[str]
    test_resources: NotRequired[Dict[str, Any]]
    test_name: NotRequired[str]


class TestCaseMetadata(TypedDict):
    test_name: str
    test_function: str
    resource_configs: Dict[str, Any]
    mode: NotRequired[str]


class TestCase(TypedDict):
    input: TestCaseInput
    metadata: TestCaseMetadata


class TestConfiguration(TypedDict):
    test_cases: List[TestCase]
    resource_configs: Dict[str, Any]


class ResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str


class SupabaseAccountResource(TypedDict):
    userID: str
    publicKey: NotRequired[str]
    secretKey: NotRequired[str]
    jwt_token: NotRequired[str]


def extract_test_configuration(test_name: str) -> TestConfiguration:
    """Generic test configuration extraction for any test following the standard pattern"""
    try:
        # Dynamically import the test config
        config_module_path = f"Tests.{test_name}.Test_Configs"
        Test_Configs = importlib.import_module(config_module_path)

        # Check if the test provides its own fixtures
        resource_configs = {}
        custom_fixtures = None

        # Try to import the test module to check for get_fixtures function
        try:
            test_files = []
            test_dir = f"Tests/{test_name}"

            for file in os.listdir(test_dir):
                if file.startswith("test_") and file.endswith(".py"):
                    test_files.append(file[:-3])  # Remove .py extension

            if test_files:
                test_module_path = f"Tests.{test_name}.{test_files[0]}"
                test_module = importlib.import_module(test_module_path)

                # Check if test defines its own fixtures
                if hasattr(test_module, "get_fixtures"):
                    custom_fixtures = test_module.get_fixtures()
                    print(
                        f"📦 {test_name} provides custom fixtures: {[f.get_resource_type() for f in custom_fixtures]}"
                    )

                    # If custom fixtures are provided, we don't need to auto-detect resources
                    resource_configs = {"custom_fixtures": custom_fixtures}
                else:
                    # This test hasn't been converted to the new pattern yet
                    print(
                        f"⚠️  {test_name} uses legacy fixture pattern - consider converting to get_fixtures()"
                    )
                    resource_configs = {"needs_supabase_account": True}
            else:
                # No test files found, use basic requirements
                print(f"⚠️  No test files found for {test_name}")
                resource_configs = {"needs_supabase_account": True}

        except Exception as e:
            print(f"Warning: Could not check for custom fixtures in {test_name}: {e}")
            # Fallback to basic requirements
            resource_configs = {"needs_supabase_account": True}

        # Find the test function name by convention
        test_function_name = get_test_function_name(test_name)

        # Create the test case data
        test_case = {
            "input": {"task": Test_Configs.User_Input, "configs": Test_Configs.Configs},
            "metadata": {
                "test_name": test_name,
                "test_function": test_function_name,
                "resource_configs": resource_configs,
                "custom_fixtures": custom_fixtures,
            },
        }

        return {"test_cases": [test_case], "resource_configs": resource_configs}

    except ImportError as e:
        raise ValueError(f"Could not import config for test {test_name}: {e}")


def get_test_function_name(test_name: str) -> str:
    """Convert test name to expected function name by convention"""
    # Convert CamelCase to snake_case and add test_ prefix
    import re

    snake_case = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", test_name)
    snake_case = re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_case).lower()
    return f"test_{snake_case}"


def get_resource_fixture(resource_type: str) -> Any:
    """
    Get the fixture class instance for a resource type.
    This is the new preferred way to access fixtures that implement DEBenchFixture.
    """
    from Fixtures.base_fixture import DEBenchFixture

    # Map resource types to their fixture class instances
    fixture_map = {
        "mongo_resource": "Fixtures.MongoDB.mongo_resources.MongoDBFixture",
        "mysql_resource": "Fixtures.MySQL.mysql_resources.MySQLFixture",
        # Future fixture classes:
        # "postgres_resource": "Fixtures.PostgreSQL.postgres_resources.PostgreSQLFixture",
        # "airflow_resource": "Fixtures.Airflow.airflow_resources.AirflowFixture",
    }

    if resource_type not in fixture_map:
        raise ValueError(f"Unknown fixture type: {resource_type}")

    # Import and instantiate the fixture class
    module_path, class_name = fixture_map[resource_type].rsplit(".", 1)
    module = importlib.import_module(module_path)
    fixture_class = getattr(module, class_name)

    # Verify it implements DEBenchFixture
    if not issubclass(fixture_class, DEBenchFixture):
        raise ValueError(
            f"Fixture class {fixture_class} does not implement DEBenchFixture"
        )

    return fixture_class()


# ========== Session-Level Fixture Management ==========


def discover_session_fixtures(all_fixtures: List[Any]) -> List[Any]:
    """
    Discover which fixtures require session-level setup across all tests.

    Args:
        all_fixtures: List of all DEBenchFixture instances from all tests

    Returns:
        List of unique fixture classes that require session setup
    """
    from Fixtures.base_fixture import DEBenchFixture

    session_fixture_classes = set()

    for fixture in all_fixtures:
        if (
            isinstance(fixture, DEBenchFixture)
            and fixture.__class__.requires_session_setup()
        ):
            session_fixture_classes.add(fixture.__class__)

    # Return one instance of each unique session fixture class
    return [fixture_class() for fixture_class in session_fixture_classes]


def setup_session_fixtures(session_fixtures: List[Any]) -> Dict[str, Any]:
    """
    Set up session-level fixtures that will be shared across all tests.

    Args:
        session_fixtures: List of unique session fixtures to set up

    Returns:
        Dictionary mapping resource_type -> session_data
    """
    from Fixtures.base_fixture import DEBenchFixture

    session_data = {}

    for fixture in session_fixtures:
        if not isinstance(fixture, DEBenchFixture):
            continue

        resource_type = fixture.get_resource_type()

        try:
            print(f"🔧 Setting up session-level {resource_type}...")
            fixture_session_data = fixture.session_setup()
            session_data[resource_type] = fixture_session_data
            print(f"✅ Session-level {resource_type} set up successfully")
        except Exception as e:
            print(f"❌ Failed to set up session-level {resource_type}: {e}")

    return session_data


def cleanup_session_fixtures(
    session_fixtures: List[Any], session_data: Dict[str, Any]
) -> None:
    """
    Clean up session-level fixtures after all tests complete.

    Args:
        session_fixtures: List of session fixtures to clean up
        session_data: Session data returned from setup_session_fixtures
    """
    from Fixtures.base_fixture import DEBenchFixture

    for fixture in session_fixtures:
        if not isinstance(fixture, DEBenchFixture):
            continue

        resource_type = fixture.get_resource_type()

        try:
            print(f"🧹 Cleaning up session-level {resource_type}...")
            fixture.session_teardown(session_data.get(resource_type))
            print(f"✅ Session-level {resource_type} cleaned up successfully")
        except Exception as e:
            print(f"❌ Failed to clean up session-level {resource_type}: {e}")


def setup_test_resources_from_fixtures(
    fixtures: List[Any],
    fixture_configs: Dict[str, Any] = None,
    session_data: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Set up test resources from a provided list of DEBenchFixture instances.
    This allows tests to provide their own initialized fixture objects.

    Args:
        fixtures: List of DEBenchFixture instances
        fixture_configs: Optional mapping of resource_type -> config for each fixture
        session_data: Optional session data from session-level fixtures

    Returns:
        Dictionary mapping resource_type -> resource_data
    """
    from Fixtures.base_fixture import DEBenchFixture

    resources = {}
    fixture_configs = fixture_configs or {}
    session_data = session_data or {}

    for fixture in fixtures:
        if not isinstance(fixture, DEBenchFixture):
            raise ValueError(f"Fixture {fixture} does not implement DEBenchFixture")

        resource_type = fixture.get_resource_type()

        # Pass session data to fixture if available
        if resource_type in session_data:
            fixture.session_data = session_data[resource_type]

        # Use provided config or default config
        config = fixture_configs.get(resource_type, fixture.get_default_config())

        try:
            # Check if fixture was initialized with custom config
            if hasattr(fixture, "custom_config") and fixture.custom_config is not None:
                # Fixture has custom config, let it use that
                resource_data = fixture.setup_resource()
                print(f"✅ Set up {resource_type} using fixture's custom config")
            else:
                # Use provided config or default
                resource_data = fixture.setup_resource(config)
                print(f"✅ Set up {resource_type} using provided fixture")

            resources[resource_type] = resource_data
        except Exception as e:
            print(f"❌ Failed to set up {resource_type}: {e}")

    return resources


def cleanup_test_resources_from_fixtures(
    fixtures: List[Any], resources: Dict[str, Any]
) -> None:
    """
    Clean up test resources using the provided list of DEBenchFixture instances.

    Args:
        fixtures: List of DEBenchFixture instances that were used to set up resources
        resources: Dictionary mapping resource_type -> resource_data
    """
    from Fixtures.base_fixture import DEBenchFixture

    for fixture in fixtures:
        if not isinstance(fixture, DEBenchFixture):
            continue

        resource_type = fixture.get_resource_type()

        if resource_type in resources:
            try:
                fixture.teardown_resource(resources[resource_type])
                print(f"✅ Cleaned up {resource_type} using provided fixture")
            except Exception as e:
                print(f"❌ Failed to clean up {resource_type}: {e}")


def setup_supabase_account_resource(mode: str = "Ardent") -> SupabaseAccountResource:
    """Set up Supabase account resource"""
    print(f"Setting up Supabase account resource for mode: {mode}")

    # Create unique email for this test to avoid conflicts
    test_id = str(uuid.uuid4())[:8]
    unique_email = f"test-{test_id}@example.com"

    # Create user with shared admin client
    resp = supabase_client.auth.admin.create_user(
        {"email": unique_email, "password": "Str0ngP@ss!", "email_confirm": True}
    )

    response = {}
    user_id = resp.user.id
    response["userID"] = user_id

    # Generate JWT and API keys for Ardent mode
    if mode == "Ardent":
        jwt_payload = {
            "sub": user_id,
            "email": unique_email,
            "role": "authenticated",
            "aud": "authenticated",
            "iss": "supabase",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "session_id": str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(
            jwt_payload, os.environ["SUPABASE_JWT_SECRET"], algorithm="HS256"
        )

        response["jwt_token"] = jwt_token

        # Create API keys
        token_creation_response = requests.post(
            f"{os.getenv('ARDENT_BASE_URL')}/v1/api/createKeys",
            json={"userID": user_id},
            headers={
                "Authorization": f"Bearer {jwt_token}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )

        if not token_creation_response.ok:
            raise requests.exceptions.ConnectionError(
                f"Failed to create keys: HTTP {token_creation_response.status_code} - {token_creation_response.text}"
            )

        token_data = token_creation_response.json()
        response["publicKey"] = token_data["publicKey"]
        response["secretKey"] = token_data["secretKey"]

    print(f"Supabase account resource created successfully for user: {user_id}")
    return response


def setup_test_resources(
    resource_configs: Dict[str, Any], session_data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Generic setup for all test resources using DEBenchFixture instances"""
    resources = {}
    session_data = session_data or {}

    # Check if test provides custom fixtures
    if "custom_fixtures" in resource_configs and resource_configs["custom_fixtures"]:
        custom_fixtures = resource_configs["custom_fixtures"]

        # Set up Supabase account if needed (always needed for Ardent mode)
        resources["supabase_account_resource"] = setup_supabase_account_resource()

        # Set up custom fixtures with session data
        fixture_resources = setup_test_resources_from_fixtures(
            custom_fixtures, session_data=session_data
        )
        resources.update(fixture_resources)

        return resources

    # Fall back to auto-detected resource setup
    for resource_key, resource_config in resource_configs.items():
        if resource_key == "needs_supabase_account" and resource_config:
            resources["supabase_account_resource"] = setup_supabase_account_resource()
        elif resource_key.endswith("_resource"):
            try:
                # Get the fixture instance for this resource type
                fixture = get_resource_fixture(resource_key)

                # Pass session data to fixture if available
                if resource_key in session_data:
                    fixture.session_data = session_data[resource_key]

                # Use the fixture to set up the resource
                resource_data = fixture.setup_resource(resource_config)
                resources[resource_key] = resource_data

            except Exception as e:
                print(f"Warning: Could not set up {resource_key}: {e}")

    return resources


def cleanup_supabase_account_resource(
    supabase_resource_data: SupabaseAccountResource,
) -> None:
    """Clean up Supabase account resource"""
    try:
        user_id = supabase_resource_data["userID"]

        # Delete API keys if they exist
        if (
            "publicKey" in supabase_resource_data
            and "jwt_token" in supabase_resource_data
        ):
            delete_key_response = requests.delete(
                f"{os.getenv('ARDENT_BASE_URL')}/v1/api/deleteKey",
                json={
                    "userID": user_id,
                    "publicKey": supabase_resource_data["publicKey"],
                },
                headers={
                    "Authorization": f"Bearer {supabase_resource_data['jwt_token']}",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )

        # Always delete the user
        supabase_client.auth.admin.delete_user(user_id)
        print(f"Supabase account resource for user {user_id} cleaned up successfully")

    except Exception as e:
        print(f"Error cleaning up Supabase account resource: {e}")


def cleanup_test_resources(
    resources: Dict[str, Any], custom_fixtures: List[Any] = None
) -> None:
    """Generic cleanup for all test resources using DEBenchFixture instances"""

    # If custom fixtures were provided, use them for cleanup
    if custom_fixtures:
        cleanup_test_resources_from_fixtures(custom_fixtures, resources)

        # Still need to clean up Supabase account separately
        if "supabase_account_resource" in resources:
            cleanup_supabase_account_resource(resources["supabase_account_resource"])

        return

    # Fall back to auto-detected resource cleanup
    for resource_key, resource_data in resources.items():
        if resource_key == "supabase_account_resource":
            cleanup_supabase_account_resource(resource_data)
        elif resource_key.endswith("_resource"):
            try:
                # Get the fixture instance for this resource type
                fixture = get_resource_fixture(resource_key)

                # Use the fixture to tear down the resource
                fixture.teardown_resource(resource_data)

            except Exception as e:
                print(f"Warning: Could not clean up {resource_key}: {e}")


def get_test_validator(test_name: str) -> callable:
    """Get a generic validator function for any test following the standard pattern"""

    def generic_validator(
        output: Dict[str, Any],
        expected: Optional[Any] = None,
        fixtures: Optional[List] = None,
    ) -> bool:
        """Generic test validation using the validate_test function from the test file"""
        try:
            # Check if the model execution was successful first
            if not output or output.get("status") == "failed":
                return False

            # Dynamically import the validate_test function from the test file
            test_files = []
            test_dir = f"Tests/{test_name}"

            # Find test files in the directory
            for file in os.listdir(test_dir):
                if file.startswith("test_") and file.endswith(".py"):
                    test_files.append(file[:-3])  # Remove .py extension

            if not test_files:
                print(f"No test files found for {test_name}")
                return False

            # Import the first test file found
            test_module_path = f"Tests.{test_name}.{test_files[0]}"
            test_module = importlib.import_module(test_module_path)

            # Get the validate_test function
            if not hasattr(test_module, "validate_test"):
                print(f"No validate_test function found in {test_module_path}")
                return False

            validate_test = test_module.validate_test

            # Call the validation function - pass fixtures if available
            validation_result = validate_test(output, fixtures=fixtures)

            # validate_test should return either:
            # - A boolean (simple pass/fail)
            # - A dict with 'success' boolean and 'test_steps' list
            if isinstance(validation_result, bool):
                return validation_result
            elif isinstance(validation_result, dict):
                success = validation_result.get("success", False)
                test_steps = validation_result.get("test_steps", [])

                # Log the test steps for debugging
                print(f"📋 Test steps for {test_name}:")
                for step in test_steps:
                    status = step.get("status", "unknown")
                    name = step.get("name", "unnamed step")
                    print(f"   • {name}: {status}")

                return success
            else:
                print(f"Invalid validation result type: {type(validation_result)}")
                return False

        except Exception as e:
            print(f"Validation error for {test_name}: {e}")
            return False

    return generic_validator
