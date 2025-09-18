"""
AirflowFixture using DEBenchFixture pattern with session-level support.
Handles expensive Airflow deployment operations at the session level.
"""

import os
import time
import tempfile
import uuid
from typing import Dict, Any, Optional, List
from typing_extensions import TypedDict
from pathlib import Path

from Fixtures.base_fixture import DEBenchFixture
from Fixtures.Airflow.Airflow import Airflow_Local
from Fixtures.Databricks.cache_manager import CacheManager
from Fixtures.Airflow.airflow_resources import (
    _ensure_astro_login,
    _ensure_cache_manager_initialized,
    _create_dir_and_astro_project,
    _create_deployment_in_astronomer,
    _wake_up_deployment,
    _hibernate_deployment,
    _get_deployment_id_by_name,
    _create_astro_deployment_api_token,
    _create_variables_in_airflow_deployment,
    _check_and_update_gh_secrets,
    _run_and_validate_subprocess,
    cleanup_airflow_resource,
)

from braintrust import traced


class AirflowResourceConfig(TypedDict):
    resource_id: str
    deployment_name: Optional[str]
    runtime_version: Optional[str]
    scheduler_size: Optional[str]


class AirflowResourceData(TypedDict):
    resource_id: str
    type: str
    creation_time: float
    creation_duration: float
    description: str
    status: str
    deployment_id: str
    deployment_name: str
    base_url: str
    api_url: str
    api_token: str
    api_headers: Dict[str, str]
    username: str
    password: str
    airflow_instance: Any
    test_dir: Path


class AirflowSessionData(TypedDict):
    cache_manager: CacheManager
    astro_logged_in: bool
    available_deployments: List[Dict[str, str]]


class AirflowFixture(
    DEBenchFixture[AirflowResourceConfig, AirflowResourceData, AirflowSessionData]
):
    """
    Airflow fixture implementation with session-level deployment pool management.

    Session-level: Astro login, cache manager, deployment pool
    Per-test: Allocate deployment, configure for specific test, cleanup
    """

    @classmethod
    def requires_session_setup(cls) -> bool:
        """Airflow requires expensive session-level setup"""
        return True

    def session_setup(
        self, session_config: Optional[AirflowResourceConfig] = None
    ) -> AirflowSessionData:
        """
        Set up shared Airflow session resources:
        - Astro CLI login
        - Cache manager initialization
        - Pre-create hibernated deployment pool
        """
        print("🌐 Setting up Airflow session-level resources...")

        # Verify required environment variables
        required_envars = [
            "ASTRO_WORKSPACE_ID",
            "AIRFLOW_GITHUB_TOKEN",
            "AIRFLOW_REPO",
            "ASTRO_CLOUD_PROVIDER",
            "ASTRO_REGION",
        ]
        if missing_envars := [
            envar for envar in required_envars if not os.getenv(envar)
        ]:
            raise ValueError(
                f"Missing required environment variables: {missing_envars}"
            )

        # make sure either ASTRO_ACCESS_TOKEN or ASTRO_API_TOKEN is set
        if not os.getenv("ASTRO_ACCESS_TOKEN") and not os.getenv("ASTRO_API_TOKEN"):
            raise ValueError("Either ASTRO_ACCESS_TOKEN or ASTRO_API_TOKEN must be set")

        # 1. Ensure Astro login
        print("🔐 Ensuring Astro CLI login...")
        _ensure_astro_login()

        # 2. Initialize cache manager
        print("💾 Initializing deployment cache manager...")
        cache_manager = _ensure_cache_manager_initialized()

        # 3. Get available deployments
        available_deployments = cache_manager.get_all_astronomer_deployments()

        print(
            f"✅ Airflow session setup complete! Found {len(available_deployments)} deployments in cache"
        )

        return AirflowSessionData(
            cache_manager=cache_manager,
            astro_logged_in=True,
            available_deployments=available_deployments,
        )

    def session_teardown(
        self, session_data: Optional[AirflowSessionData] = None
    ) -> None:
        """Clean up session-level Airflow resources"""
        if not session_data:
            return

        print("🧹 Cleaning up Airflow session-level resources...")

        # The cache manager and deployments will be cleaned up naturally
        # since they're managed by the Astronomer platform
        print("✅ Airflow session cleanup complete")

    @traced(name="AirflowFixture.test_setup")
    def test_setup(
        self, resource_config: Optional[AirflowResourceConfig] = None
    ) -> AirflowResourceData:
        """
        Set up individual Airflow resource for a test.
        Allocates a deployment from the session pool and configures it.
        """
        # Determine which config to use
        if resource_config is not None:
            config = resource_config
        elif self.custom_config is not None:
            config = self.custom_config
        else:
            config = self.get_default_config()

        resource_id = config["resource_id"]
        print(f"🚀 Setting up Airflow resource: {resource_id}")

        creation_start = time.time()

        # Get session data
        session_data = self.session_data
        if not session_data:
            raise RuntimeError(
                "Session data not available - session setup may have failed"
            )

        cache_manager = session_data["cache_manager"]

        # Create test directory and Astro project
        test_dir = _create_dir_and_astro_project(resource_id)

        try:
            # Try to allocate a hibernating deployment from session cache
            print(f"🔄 Attempting to allocate deployment for {resource_id}...")

            if deployment_info := cache_manager.allocate_astronomer_deployment(
                resource_id, os.getpid()
            ):
                # Got an existing hibernating deployment
                astro_deployment_id = deployment_info["deployment_id"]
                astro_deployment_name = deployment_info["deployment_name"]
                print(f"♻️  Allocated hibernating deployment: {astro_deployment_name}")
                _wake_up_deployment(astro_deployment_name)
            else:
                # No hibernating deployment available, create a new one
                print(
                    f"🆕 No hibernating deployments available, creating new: {resource_id}"
                )
                astro_deployment_id = _create_deployment_in_astronomer(resource_id)
                astro_deployment_name = resource_id

            # Update GitHub secrets for the deployment
            _check_and_update_gh_secrets(
                deployment_id=astro_deployment_id,
                deployment_name=astro_deployment_name,
                astro_access_token=os.environ["ASTRO_ACCESS_TOKEN"],
                astro_workspace_id=os.environ["ASTRO_WORKSPACE_ID"],
            )

            # Get deployment API URL
            api_url = "https://" + _run_and_validate_subprocess(
                [
                    "astro",
                    "deployment",
                    "inspect",
                    "--deployment-name",
                    astro_deployment_name,
                    "--key",
                    "metadata.airflow_api_url",
                ],
                "getting Astro deployment API URL",
                return_output=True,
            )
            base_url = api_url[: api_url.find("/api/v1")]

            # Get fresh deployment ID and create API token
            fresh_deployment_id = _get_deployment_id_by_name(astro_deployment_name)
            if not fresh_deployment_id:
                raise EnvironmentError(
                    f"Could not find deployment ID for {astro_deployment_name}"
                )

            api_token = os.getenv("ASTRO_API_TOKEN")

            if not api_token:
                raise ValueError(
                    "ASTRO_API_TOKEN is not set. This is now a required environment variable."
                )

            # Create user in Airflow deployment
            _create_variables_in_airflow_deployment(astro_deployment_name)

            # Create and validate Airflow instance
            airflow_instance = Airflow_Local(
                airflow_dir=test_dir,
                host=base_url,
                api_token=api_token,
                api_url=api_url,
            )
            airflow_instance.wait_for_airflow_to_be_ready()

            creation_end = time.time()
            print(
                f"✅ Airflow resource creation took {creation_end - creation_start:.2f}s"
            )

            # Create resource data
            resource_data = AirflowResourceData(
                resource_id=resource_id,
                type="airflow_resource",
                creation_time=creation_start,
                creation_duration=creation_end - creation_start,
                description=f"Airflow resource for {resource_id}",
                status="active",
                deployment_id=astro_deployment_id,
                deployment_name=astro_deployment_name,
                base_url=base_url,
                api_url=api_url,
                api_token=api_token,
                api_headers={
                    "Authorization": f"Bearer {api_token}",
                    "Cache-Control": "no-cache",
                },
                username=os.getenv("AIRFLOW_USERNAME", "airflow"),
                password=os.getenv("AIRFLOW_PASSWORD", "airflow"),
                airflow_instance=airflow_instance,
                test_dir=test_dir,
            )

            print(f"✅ Airflow resource {resource_id} ready!")
            return resource_data

        except Exception as e:
            print(f"❌ Failed to setup Airflow resource {resource_id}: {e}")
            # Clean up on failure
            if "test_dir" in locals() and test_dir.exists():
                import shutil

                shutil.rmtree(test_dir)
            raise

    @traced(name="AirflowFixture.test_teardown")
    def test_teardown(self, resource_data: AirflowResourceData) -> None:
        """Clean up individual Airflow resource"""
        resource_id = resource_data["resource_id"]
        deployment_name = resource_data["deployment_name"]
        test_dir = resource_data["test_dir"]

        print(f"🧹 Cleaning up Airflow resource: {resource_id}")

        try:
            # Get session data for cache manager
            session_data = self.session_data
            cache_manager = session_data["cache_manager"] if session_data else None

            # Clean up using existing logic
            test_resources = [(deployment_name, cache_manager)] if cache_manager else []
            cleanup_airflow_resource(resource_id, test_resources, test_dir)

            print(f"✅ Airflow resource {resource_id} cleaned up")

        except Exception as e:
            print(f"❌ Error cleaning up Airflow resource {resource_id}: {e}")

    @classmethod
    def get_resource_type(cls) -> str:
        """Return the resource type identifier"""
        return "airflow_resource"

    @classmethod
    def get_default_config(cls) -> AirflowResourceConfig:
        """Return default configuration for Airflow resources"""
        timestamp = int(time.time())
        uuid_suffix = uuid.uuid4().hex[:8]

        return AirflowResourceConfig(
            resource_id=f"airflow_test_{timestamp}_{uuid_suffix}",
            deployment_name=None,  # Will be generated from resource_id
            runtime_version=os.getenv("ASTRO_RUNTIME_VERSION", "13.1.0"),
            scheduler_size="small",
        )

    def create_config_section(self) -> Dict[str, Any]:
        """
        Create Airflow config section using the fixture's resource data.

        Returns:
            Dictionary containing the airflow service configuration
        """
        # Get the actual resource data from the fixture
        resource_data = getattr(self, "_resource_data", None)
        if not resource_data:
            raise Exception(
                "Airflow resource data not available - ensure test_setup was called"
            )

        print(f"resource_data: {resource_data}")

        # Extract connection details from resource data
        github_token = resource_data.get(
            "github_token", os.getenv("AIRFLOW_GITHUB_TOKEN")
        )
        repo = resource_data.get("repo", os.getenv("AIRFLOW_REPO"))
        dag_path = resource_data.get("dag_path", os.getenv("AIRFLOW_DAG_PATH"))
        requirements_path = resource_data.get(
            "requirements_path", os.getenv("AIRFLOW_REQUIREMENTS_PATH")
        )
        api_token = resource_data.get("api_token", os.getenv("AIRFLOW_API_TOKEN"))
        # Use the base URL from resource data if available
        base_url = resource_data.get("base_url", "http://localhost:8080")

        return {
            "airflow": {
                "github_token": github_token,
                "repo": repo,
                "dag_path": dag_path,
                "api_token": api_token,
                "requirements_path": requirements_path,
                "host": base_url,
                "username": "airflow",  # Standard Airflow username
                "password": "airflow",  # Standard Airflow password
            }
        }


# Note: No global instance - each test creates its own AirflowFixture
# with custom resource_id in get_fixtures()
