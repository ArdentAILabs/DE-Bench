"""
This module provides a pytest fixture for creating isolated Airflow instances using Docker Compose.
"""

import fcntl
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Union

import github
import pytest
from dotenv import load_dotenv
from github import Github

from Fixtures import parse_test_name
from Fixtures.Airflow.Airflow import Airflow_Local
from Fixtures.Databricks.cache_manager import CacheManager

VALIDATE_ASTRO_INSTALL = "Please check if the Astro CLI is installed and in PATH."
load_dotenv()


def _ensure_astro_login() -> None:
    """
    Ensure Astro is logged in using file-based coordination to prevent multiple logins in parallel.

    :raises ValueError: If ASTRO_ACCESS_TOKEN is not found in .env file.
    :raises EnvironmentError: If login fails.
    :rtype: None
    """
    # Create a lock file for coordination
    lock_file_path = os.path.join(tempfile.gettempdir(), "astro_login.lock")
    
    with open(lock_file_path, 'w') as lock_file:
        try:
            # Try to acquire an exclusive lock
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Check if already logged in
            try:
                # Try a simple command that requires authentication
                result = subprocess.run(
                    ["astro", "deployment", "list"], 
                    capture_output=True, 
                    text=True, 
                    timeout=10
                )
                if result.returncode == 0:
                    print(f"Worker {os.getpid()}: Astro already logged in")
                    return None
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
            
            # Not logged in, perform login
            # Load environment variables from .env file
            astro_token = os.getenv("ASTRO_ACCESS_TOKEN")
            
            if not astro_token:
                raise ValueError("ASTRO_ACCESS_TOKEN not found in .env file")
            
            print(f"Worker {os.getpid()}: Logging into Astro for test session")
            _run_and_validate_subprocess(
                ["astro", "login", "--token-login", astro_token],
                "login to Astro (session-wide)",
            )
            print(f"Worker {os.getpid()}: Successfully logged into Astro")
        except (IOError, OSError):
            # Another process has the lock, wait for login to complete
            print(f"Worker {os.getpid()}: Waiting for another process to complete Astro login...")
            try:
                # Wait for the lock to be released
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                print(f"Worker {os.getpid()}: Astro login completed by another process")
                return None
            except Exception as e:
                print(f"Worker {os.getpid()}: Error waiting for Astro login: {e}")
                raise


@pytest.fixture(scope="session")
def astro_login():
    """
    A session-scoped fixture that ensures Astro is logged in once for the entire test session.
    Uses file-based coordination to prevent multiple logins in parallel execution.
    """
    # We don't need to login if the API token is set
    if os.getenv("ASTRO_API_TOKEN"):
        print(f"Worker {os.getpid()}: Astro API token found, skipping login")
        return None
    return _ensure_astro_login()


def _ensure_cache_manager_initialized() -> CacheManager:
    """
    Ensure CacheManager is initialized using file-based coordination to prevent multiple initializations in parallel.

    :return: The initialized CacheManager instance.
    :rtype: CacheManager
    """
    # Create a lock file for coordination
    lock_file_path = os.path.join(tempfile.gettempdir(), "cache_manager_init.lock")
    
    # First, check if already initialized without acquiring lock
    cache_manager = CacheManager()

    # Not initialized, need to coordinate
    with open(lock_file_path, 'w') as lock_file:
        try:
            # Try to acquire an exclusive lock
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Always fetch deployments first
            if astro_deployments := fetch_astro_deployments():
                cache_manager.populate_astronomer_deployments(astro_deployments)
                print(f"Worker {os.getpid()}: CacheManager initialized with {len(astro_deployments)} deployments")
                
                # Debug: Print deployment details
                for deployment in astro_deployments:
                    print(f"Worker {os.getpid()}: Deployment: {deployment['deployment_name']} ({deployment['deployment_id']}) - Status: {deployment['status']}")
            else:
                print(f"Worker {os.getpid()}: No existing deployments found, CacheManager initialized with empty deployment list")
            
            # Verify population worked
            verification_deployments = cache_manager.get_all_astronomer_deployments()
            print(f"Worker {os.getpid()}: CacheManager initialized and populated with {len(verification_deployments)} deployments")
            return cache_manager
            
        except (IOError, OSError):
            # Another process has the lock, wait for initialization to complete
            print(f"Worker {os.getpid()}: Waiting for another process to complete CacheManager initialization...")
            
            # Wait for the lock to be released and check for deployments
            max_wait_time = 300  # 5 minutes max wait
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                try:
                    # Try to acquire the lock
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    # Lock acquired, check if deployments are now available
                    if existing_deployments := fetch_astro_deployments():
                        print(f"Worker {os.getpid()}: CacheManager initialization completed by another process ({len(existing_deployments)} deployments)")
                        cache_manager.populate_astronomer_deployments(existing_deployments)
                        return cache_manager
                    # Still no deployments, release lock and wait
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    time.sleep(2)  # Wait 2 seconds before trying again
                except (IOError, OSError):
                    # Lock not available, wait and try again
                    time.sleep(2)
                    continue
            # fallback to existing data
            if existing_deployments := fetch_astro_deployments():
                print(f"Worker {os.getpid()}: CacheManager initialization fallback after wait ({len(existing_deployments)} deployments)")
                return cache_manager
            # If we get here, something went wrong
            raise Exception(f"Timeout waiting for CacheManager initialization after {max_wait_time} seconds")


@pytest.fixture(scope="session")
def shared_cache_manager():
    """
    A session-scoped fixture that creates a single CacheManager instance shared across all tests.
    Uses file-based coordination to prevent multiple initializations in parallel execution.
    """
    return _ensure_cache_manager_initialized()


@pytest.fixture(scope="function")
def airflow_resource(request, astro_login, shared_cache_manager):
    """
    A function-scoped fixture that creates unique Airflow instances for each test.
    Each test gets its own isolated Airflow environment using docker-compose.
    """
    # verify the required astro envars are set
    build_template = request.param
    resource_id = build_template["resource_id"]
    required_envars = [
        "ASTRO_WORKSPACE_ID",
        "ASTRO_ACCESS_TOKEN",
        "AIRFLOW_GITHUB_TOKEN",
        "AIRFLOW_REPO",
        "ASTRO_CLOUD_PROVIDER",
        "ASTRO_REGION",
    ]
    if missing_envars := [envar for envar in required_envars if not os.getenv(envar)]:
        raise ValueError(f"The following envars are not set: {missing_envars}")  # noqa

    # make sure the astro cli is installed
    _parse_astro_version()

    start_time = time.time()
    print(f"Worker {os.getpid()}: Starting airflow_resource for {resource_id}")

    # Create Airflow resource
    print(f"Worker {os.getpid()}: Creating Airflow resource for {resource_id}")
    creation_start = time.time()
    test_resources = []

    # Astro login is handled by the session-scoped astro_login fixture
    test_dir = _create_dir_and_astro_project(resource_id)

    # Use the shared cache manager from the session-scoped fixture

    try:
        # Try to allocate a hibernating deployment from shared cache
        print(f"Worker {os.getpid()}: Attempting to allocate hibernating deployment for {resource_id}")

        if deployment_info := shared_cache_manager.allocate_astronomer_deployment(resource_id, os.getpid()):
            # Got an existing hibernating deployment
            astro_deployment_id = deployment_info["deployment_id"]
            astro_deployment_name = deployment_info["deployment_name"]
            print(f"Worker {os.getpid()}: Allocated hibernating deployment: {astro_deployment_name}")
            _wake_up_deployment(astro_deployment_name)
        else:
            # No hibernating deployment available, create a new one
            print(f"Worker {os.getpid()}: No hibernating deployments available, creating new deployment: {resource_id}")
            astro_deployment_id = _create_deployment_in_astronomer(resource_id)
            astro_deployment_name = resource_id

        # check and update the github secrets
        _check_and_update_gh_secrets(
            deployment_id=astro_deployment_id,
            deployment_name=astro_deployment_name,
            astro_access_token=os.environ["ASTRO_ACCESS_TOKEN"],
        )

        test_resources.append((astro_deployment_name, shared_cache_manager))
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

        api_token = os.getenv("ASTRO_API_TOKEN")

        # Get the fresh deployment ID before creating API token (in case cache is stale)
        fresh_deployment_id = _get_deployment_id_by_name(astro_deployment_name)
        if not fresh_deployment_id:
            raise EnvironmentError(f"Could not find deployment ID for deployment {astro_deployment_name}")
        
        print(f"Worker {os.getpid()}: Using fresh deployment ID {fresh_deployment_id} for {astro_deployment_name}")
        
        # create a token for the airflow resource
        api_token = api_token or _create_astro_deployment_api_token(
            deployment_id=fresh_deployment_id,
            deployment_name=astro_deployment_name,
        )
        # check if the token has any prefix
        if "astro api" in api_token.lower():
            api_token = api_token[api_token.find('\n') + 1:-1].strip()

        # create a user in the airflow deployment (ardent needs username and password for the Airflowconfig)
        _create_variables_in_airflow_deployment(astro_deployment_name)

        # validate the api server is running
        airflow_instance = Airflow_Local(
             airflow_dir=test_dir, host=base_url, api_token=api_token, api_url=api_url,
        )
        airflow_instance.wait_for_airflow_to_be_ready()

        creation_end = time.time()
        print(
            f"Worker {os.getpid()}: Airflow resource creation took {creation_end - creation_start:.2f}s"
        )

        # Create detailed resource data
        resource_data = {
            "resource_id": resource_id,
            "type": "airflow_resource",
            "test_name": parse_test_name(request.node.name),
            "creation_time": time.time(),
            "worker_pid": os.getpid(),
            "creation_duration": creation_end - creation_start,
            "description": f"An Airflow resource for {resource_id}",
            "status": "active",
            "project_name": test_dir.stem,
            "base_url": base_url,
            "deployment_id": astro_deployment_id,
            "deployment_name": astro_deployment_name,
            "api_url": api_url,
            "api_token": api_token,
            "api_headers": {"Authorization": f"Bearer {api_token}", "Cache-Control": "no-cache"},
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "airflow_instance": airflow_instance,
            "created_resources": test_resources,
            "cache_manager": shared_cache_manager,
        }

        print(f"Worker {os.getpid()}: Created Airflow resource {resource_id}")

        fixture_end_time = time.time()
        print(
            f"Worker {os.getpid()}: Airflow fixture setup took {fixture_end_time - start_time:.2f}s total"
        )
        yield resource_data
    except Exception as e:
        print(f"Worker {os.getpid()}: Error in Airflow fixture: {e}")
        raise e from e
    finally:
        # clean up the airflow resource after the test completes
        print(f"Worker {os.getpid()}: Cleaning up Airflow resource {resource_id}")
        cleanup_airflow_resource(resource_id, test_resources, test_dir)


def _parse_astro_version() -> None:
    """
    Runs the `astro version` command to check if the Astro CLI is installed and returns the version number.

    :raises EnvironmentError: If the Astro CLI is not installed or not in PATH, or if the version cannot be parsed.
    :rtype: None
    """
    try:
        # run a simple astro version command to check if astro cli is installed
        version = subprocess.run(["astro", "version"], check=True, capture_output=True)
        # parse the version out after checking it ran successfully
        if version.returncode != 0:
            raise subprocess.CalledProcessError(version.returncode, "astro version")
        # use regex to extract the version number from the output
        version_pattern = re.compile(r"(\d+\.\d+\.\d+)")
        match = version_pattern.search(version.stdout.decode("utf-8"))
        if not match:
            raise EnvironmentError("Could not parse Astro CLI version from output")
        astro_version = match.group(1)
        # print the version number
        print(f"Worker {os.getpid()}: Astro CLI version: {astro_version}")
    except Exception as e:
        print(
            "The Astro CLI is not installed or not in PATH. Please install it "
            "from https://docs.astronomer.io/cli/installation"
        )
        raise e from e


def _run_and_validate_subprocess(
    command: list[str],
    process_description: str,
    check: bool = True,
    capture_output: bool = True,
    return_output: bool = False,
    input_text: str = None,
) -> Union[subprocess.CompletedProcess, str]:
    """
    Helper function to run a subprocess command and validate the return code.

    :param command: The command to run.
    :param process_description: The description of the process, used for error messages.
    :param check: Whether to check the return code.
    :param capture_output: Whether to capture the output.
    :param return_output: Whether to return the output.
    :param input_text: Text to send to stdin if the command expects input.
    :return: The completed process, or the command output if `return_output` is True.
    :rtype: Union[subprocess.CompletedProcess, str]
    """
    try:
        if input_text:
            # If input is needed, use Popen to handle interactive input
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = process.communicate(input=input_text)
            if process.returncode != 0:
                print(stderr)
                raise subprocess.CalledProcessError(
                    process.returncode, command, stdout, stderr
                )
            if return_output:
                return stdout
            else:
                return subprocess.CompletedProcess(
                    command, process.returncode, stdout, stderr
                )
        else:
            process = subprocess.run(
                command, check=check, capture_output=capture_output
            )
            if process.returncode != 0:
                print(process.stderr.decode("utf-8"))
                raise subprocess.CalledProcessError(process.returncode, command)
            if return_output:
                return process.stdout.decode("utf-8").rstrip("\n")
            else:
                return process
    except Exception as e:
        print(f"Worker {os.getpid()}: Error running {process_description}: {e}")
        raise e from e


def _check_and_update_gh_secrets(deployment_id: str, deployment_name: str, astro_access_token: str) -> None:
    """
    Checks if the GitHub secrets exists, deletes them if they do, and creates new ones with the given
        deployment ID and name.

    :param str deployment_id: The ID of the deployment.
    :param str deployment_name: The name of the deployment.
    :rtype: None
    """
    gh_secrets = {
        "ASTRO_DEPLOYMENT_ID": deployment_id,
        "ASTRO_DEPLOYMENT_NAME": deployment_name,
        "ASTRO_ACCESS_TOKEN": astro_access_token,
    }
    airflow_github_repo = os.getenv("AIRFLOW_REPO")
    g = Github(os.getenv("AIRFLOW_GITHUB_TOKEN"))
    if "github.com" in airflow_github_repo:
        # Extract owner/repo from URL
        parts = airflow_github_repo.split("/")
        airflow_github_repo = f"{parts[-2]}/{parts[-1]}"
    repo = g.get_repo(airflow_github_repo)
    try:
        for secret, value in gh_secrets.items():
            try:
                if repo.get_secret(secret):
                    print(f"Worker {os.getpid()}: {secret} already exists, deleting...")
                    repo.delete_secret(secret)
                print(f"Worker {os.getpid()}: Creating {secret}...")
            except github.GithubException as e:
                if e.status == 404:
                    print(f"Worker {os.getpid()}: {secret} does not exist, creating...")
                else:
                    print(f"Worker {os.getpid()}: Error checking secret {secret}: {e}")
                    raise e
            repo.create_secret(secret, value)
            print(f"Worker {os.getpid()}: {secret} created successfully.")
    except Exception as e:
        print(f"Worker {os.getpid()}: Error checking and updating GitHub secrets: {e}")
        raise e from e


def _create_deployment_in_astronomer(deployment_name: str, wait: Optional[bool] = True) -> Optional[str]:
    """
    Creates a deployment in Astronomer.

    :param deployment_name: The name of the deployment to create.
    :param wait: Whether to wait for the deployment to be created, defaults to True.
    :raises EnvironmentError: If the deployment ID cannot be parsed from the output.
    :return: The ID of the created deployment, or None if wait is False.
    :rtype: str
    """
    try:
        # Run the command to create a deployment in Astronomer
        response = _run_and_validate_subprocess(
            [
                "astro", "deployment", "create",
                "--workspace-id", os.getenv("ASTRO_WORKSPACE_ID"),
                "--name", deployment_name,
                "--runtime-version", os.getenv("ASTRO_RUNTIME_VERSION", "13.1.0"),
                "--development-mode", "enable",
                "--cloud-provider", os.getenv("ASTRO_CLOUD_PROVIDER"),
                "--region", os.getenv("ASTRO_REGION", "us-east-1"),
                "--scheduler-size", "small",
                "--wait" if wait else "",
            ],
            "creating Astronomer deployment",
            return_output=True,
        )
        if not wait:
            return None
        # Parse the output to get the newly created deployment ID
        # Look for the deployment ID in the deployment dashboard URL
        deployment_id_pattern = re.compile(r"deployments/([a-zA-Z0-9]+)")
        match = deployment_id_pattern.search(response)
        if not match:
            raise EnvironmentError("Could not parse deployment ID from output")
        deployment_id = match.group(1)
        print(f"Worker {os.getpid()}: Created Astronomer deployment: {deployment_id}")
        return deployment_id
    except Exception as e:
        print(f"Worker {os.getpid()}: Error creating Astronomer deployment: {e}")
        raise e from e


def _create_dir_and_astro_project(unique_id: str) -> Path:
    """
    Creates a directory and an Astro project in it.

    :param unique_id: The unique id for the test.
    :return: The path to the created directory.
    :rtype: Path
    """
    # Use a temp directory inside the project root for Docker compatibility
    project_root = Path(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ).parent
    tmp_root = os.path.join(project_root, "tmp_airflow_tests")
    os.makedirs(tmp_root, exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix=f"airflow_test_{unique_id}", dir=tmp_root)
    # create a dags directory
    dags_dir = os.path.join(temp_dir, "dags")
    os.makedirs(dags_dir, exist_ok=True)
    print(f"Worker {os.getpid()}: Created temp directory for Airflow: {temp_dir}")
    # cd into the temp directory and run astro project init
    os.chdir(temp_dir)
    temp_dir = Path(temp_dir)

    astro_project = _run_and_validate_subprocess(
        ["astro", "dev", "init", "-n", temp_dir.stem],
        "initialize Astro project",
        return_output=True,
        input_text="y",
    )
    print(f"Worker {os.getpid()}: Astro project initialized: {astro_project}")
    return temp_dir


def fetch_astro_deployments() -> list[dict[str, str]]:
    """
    Helper method to find the hibernating deployment in Astronomer.

    :return: List of astro deployments with their name, id, and status.
    :rtype: list[dict[str, str]]
    """
    astro_deployments: list[dict[str, str]] = []
    # get all deployments
    deployment_command_output = _run_and_validate_subprocess(
        ["astro", "deployment", "list"],
        "listing deployments in Astronomer",
        return_output=True,
    )
    # parse the output to get the name and id of the hibernating deployment
    deployments = deployment_command_output.split("\n")
    # remove the headers to get only the deployments by finding name in the list
    if index := next((i for i, line in enumerate(deployments) if "NAME" in line), None):
        deployments = deployments[index + 1:]
    deployments = [deployment.split() for deployment in deployments if deployment.strip()]
    # Filter out header row and empty rows, only keep actual deployment rows
    deployments = [deployment for deployment in deployments if len(deployment) > 5 and deployment[0] != 'NAME']
    deployments = {deployment[0]: deployment[5] for deployment in deployments}
    for deployment_name, deployment_id in deployments.items():
        astro_deployments.append(
            {
                "deployment_name": deployment_name,
                "deployment_id": deployment_id,
                "status": _check_deployment_status(deployment_name),
            }
        )
    # no hibernating deployment found, create a new one
    print(f"Worker {os.getpid()}: found {len(deployments)} deployments.")
    return astro_deployments


def _check_deployment_status(deployment_name: str) -> str:
    """
    Helper method to check the status of a deployment in Astronomer.

    :param str deployment_name: The name of the Airflow deployment in Astronomer.
    :return: The status of the deployment.
    :rtype: str
    """
    try:
        status = _run_and_validate_subprocess(
            [
                "astro",
                "deployment",
                "inspect",
                "--deployment-name",
                deployment_name,
                "--key",
                "metadata.status",
            ],
            "getting Astro deployment status",
            return_output=True,
        )
        print(f"Worker {os.getpid()}: Deployment {deployment_name} status: {status}")
        return status
    except Exception as e:
        print(f"Worker {os.getpid()}: Error getting Astro deployment status: {e}")
        return "UNKNOWN"


def _wake_up_deployment(deployment_name: str) -> None:
    """
    Helper method to wake up a deployment in Astronomer.

    :param str deployment_name: The name of the Airflow deployment in Astronomer.
    :raises TimeoutError: If the deployment does not become healthy in time.
    :raises EnvironmentError: If the deployment cannot be woken up.
    :rtype: None
    """
    if wake_up_deployment := _run_and_validate_subprocess(
        ["astro", "deployment", "wake-up", "--deployment-name", deployment_name, "-f"],
        "waking up deployment",
    ):
        _validate_deployment_status(deployment_name=deployment_name, expected_status="healthy")
    else:
        print(f"Unable to wake up deployment {deployment_name}: {wake_up_deployment}")
        raise EnvironmentError(f"Unable to wake up deployment {deployment_name}")


def _validate_deployment_status(deployment_name: str, expected_status: str) -> None:
    """
    Validates the status of a deployment in Astronomer.

    :param str deployment_name: The name of the Airflow deployment in Astronomer.
    :param str expected_status: The expected status of the deployment.
    :raises TimeoutError: If the deployment status does not match the expected status within the timeout period.
    :rtype: None
    """
    start_time = time.time()
    print(f"Worker {os.getpid()}: Waiting for deployment {deployment_name} to have a hibernation status...")
    for _ in range(30):
        status = _check_deployment_status(deployment_name)
        if status.lower() == expected_status.lower():
            end_time = time.time()
            print(
                f"Worker {os.getpid()}: Deployment {deployment_name} is {expected_status} "
                f"after {end_time - start_time:.2f}s"
            )
            print(f"Worker {os.getpid()}: Deployment {deployment_name} {expected_status} successfully.")
            return
        time.sleep(10)
    raise TimeoutError(f"Deployment {deployment_name} did not become {expected_status} in time.")


def _hibernate_deployment(deployment_name: str) -> None:
    """
    Helper method to hibernate a deployment in Astronomer.

    :raises EnvironmentError: If the deployment cannot be hibernated.
    :param str deployment_name: The name of the Airflow deployment in Astronomer.
    :rtype: None
    """
    print(f"Worker {os.getpid()}: Hibernating deployment {deployment_name}...")
    if hibernating_deployment := _run_and_validate_subprocess(
        ["astro", "deployment", "hibernate", "--deployment-name", deployment_name, "-f"],
        "hibernating deployment",
    ):
        print(f"Worker {os.getpid()}: Deployment {deployment_name} hibernated successfully.")
    else:
        print(f"Unable to hibernate deployment {deployment_name}: {hibernating_deployment}")
        raise EnvironmentError(f"Unable to hibernate deployment {deployment_name}")


def _create_variables_in_airflow_deployment(deployment_name: str) -> None:
    """
    Helper method to create a user in the Airflow deployment using Astronomer CLI and environment variables in Airflow.

    :param str deployment_name: The name of the Airflow deployment in Astronomer.
    :rtype: None
    """
    username = os.getenv("AIRFLOW_USERNAME", "airflow")
    password = os.getenv("AIRFLOW_PASSWORD", "airflow")
    user_creation_commands = [
        [
            "astro", "deployment", "variable", "create",
            "_AIRFLOW_WWW_USER_CREATE=true",
            "--deployment-name", deployment_name
        ],
        [
            "astro", "deployment", "variable", "create",
            f"_AIRFLOW_WWW_USER_USERNAME={username}",
            "--deployment-name", deployment_name
        ],
        [
            "astro", "deployment", "variable", "create",
            f"_AIRFLOW_WWW_USER_PASSWORD={password}",
            "--deployment-name", deployment_name, "-s"
        ],
        [
            "astro", "deployment", "variable", "create",
            "AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth",
            "--deployment-name", deployment_name,
        ],
    ]
    if slack_app_url := os.getenv("SLACK_APP_URL"):
        user_creation_commands.append([
            "astro", "deployment", "variable", "create",
            f"SLACK_APP_URL={slack_app_url}",
            "--deployment-name", deployment_name,
            "-s"
        ])
    for command in user_creation_commands:
        variable_name = command[4].split('=')[0]  # Extract variable name from command
        try:
            _ = _run_and_validate_subprocess(command, f"creating/updating variable {variable_name}")
            print(f"Worker {os.getpid()}: Successfully created/updated variable {variable_name}")
        except subprocess.CalledProcessError as e:
            # Check if the error is because the variable already exists
            error_output = e.stderr.decode('utf-8') if e.stderr else str(e)
            if "already exists" in error_output.lower() or "conflict" in error_output.lower():
                print(f"Worker {os.getpid()}: Variable {variable_name} already exists, skipping creation")
            else:
                print(f"Worker {os.getpid()}: Error creating variable {variable_name}: {error_output}")
                # For critical variables, try to update instead of create
                if variable_name in ['_AIRFLOW_WWW_USER_CREATE', 'AIRFLOW__API__AUTH_BACKENDS']:
                    try:
                        # Try to update the variable instead
                        update_command = command.copy()
                        update_command[3] = "update"  # Change "create" to "update"
                        _ = _run_and_validate_subprocess(update_command, f"updating variable {variable_name}")
                        print(f"Worker {os.getpid()}: Successfully updated variable {variable_name}")
                    except Exception as update_error:
                        print(f"Worker {os.getpid()}: Failed to update variable {variable_name}: {update_error}")
                        # Continue with other variables rather than failing completely
    

def cleanup_airflow_resource(
    resource_id: str,
    test_resources: list[tuple],
    test_dir: Optional[Path] = None,
):
    """
    Cleans up an Airflow resource, including the temp directory and properly managing deployments through cache.

    :param resource_id: The ID of the resource.
    :param test_resources: List of tuples containing (deployment_name, cache_manager).
    :param test_dir: The path to the test directory.
    :rtype: None
    """
    if test_dir and test_dir.exists():
        try:
            shutil.rmtree(test_dir)
            print(
                f"Worker {os.getpid()}: Removed {resource_id}'s temp directory: {test_dir}"
            )
        except Exception as e:
            print(f"Worker {os.getpid()}: Error removing temp directory: {e}")

    # Cleanup after test completes
    print(f"Worker {os.getpid()}: Cleaning up Airflow resource {resource_id}")
    try:
        # Clean up created resources in reverse order
        for resource_info in reversed(test_resources):
            deployment_name, cache_manager = resource_info

            print(f"Worker {os.getpid()}: Deleting deployment created by test: {deployment_name}")
            _ = _run_and_validate_subprocess(
                ["astro", "deployment", "delete", "-n", deployment_name, "-f"],
                "delete Astronomer deployment",
                check=True,
            )
            _create_deployment_in_astronomer(deployment_name, wait=False)
            _hibernate_deployment(deployment_name)
            
            # Get the actual deployment ID after creation and hibernation
            new_id = _get_deployment_id_by_name(deployment_name)
            
            cache_manager.release_astronomer_deployment(deployment_name, os.getpid(), new_id)
            print(f"Worker {os.getpid()}: Deployment {deployment_name} - hibernated successfully.")
                    
        print(
            f"Worker {os.getpid()}: Airflow resource {resource_id} cleaned up successfully"
        )
    except Exception as e:
        print(f"Worker {os.getpid()}: Error cleaning up Airflow resource: {e}")


def _get_deployment_id_by_name(deployment_name: str) -> Optional[str]:
    """
    Helper function to get the deployment ID by deployment name from Astronomer using inspect command.
    
    :param deployment_name: The name of the deployment.
    :return: The deployment ID if found, None otherwise.
    :rtype: Optional[str]
    """
    try:
        # Use the astro deployment inspect command to get deployment ID directly
        # This is more reliable than parsing the deployment list output
        deployment_id = _run_and_validate_subprocess(
            [
                "astro", 
                "deployment", 
                "inspect",
                "--deployment-name", deployment_name,
                "--key", "metadata.deployment_id"
            ],
            f"getting deployment ID for {deployment_name}",
            return_output=True,
        )
        
        if deployment_id and deployment_id.strip():
            deployment_id = deployment_id.strip()
            print(f"Worker {os.getpid()}: Found deployment ID {deployment_id} for deployment {deployment_name}")
            return deployment_id
        else:
            print(f"Worker {os.getpid()}: No deployment ID returned for deployment {deployment_name}")
            return None
            
    except Exception as e:
        print(f"Worker {os.getpid()}: Error fetching deployment ID for {deployment_name}: {e}")
        return None


def _create_astro_deployment_api_token(deployment_id: str, deployment_name: str, retries: int = 5) -> str:
    """
    Creates an API token for a deployment in Astronomer using file-based coordination 
    to prevent parallel processes from hitting API rate limits.

    :param deployment_id: The ID of the deployment.
    :param deployment_name: The name of the deployment.
    :param retries: The number of times to retry the operation.
    :raises EnvironmentError: If the API token cannot be created after the retries.
    :return: The API token.
    :rtype: str
    """
    # Create a lock file for coordinating API token creation across processes
    lock_file_path = os.path.join(tempfile.gettempdir(), "astro_token_creation.lock")
    
    with open(lock_file_path, 'w') as lock_file:
        try:
            # Acquire an exclusive lock to serialize token creation
            print(f"Worker {os.getpid()}: Acquiring lock for API token creation for deployment {deployment_name}")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            print(f"Worker {os.getpid()}: Lock acquired for API token creation for deployment {deployment_name}")
            
            # Now attempt to create the token with retry logic
            for retry in range(retries + 1):
                error_message = f"Failed to create Astro deployment API token for deployment {deployment_name} after {retry} retries"
                try:
                    print(f"Worker {os.getpid()}: Attempting to create API token for {deployment_name} (attempt {retry + 1}/{retries + 1})")
                    
                    # Try to run the command and capture both stdout and stderr for debugging
                    try:
                        api_token = _run_and_validate_subprocess(
                            [
                                "astro",
                                "deployment",
                                "token",
                                "create",
                                "--description",
                                f"{deployment_name} API access for deployment {deployment_name}",
                                "--name",
                                f"{deployment_name} API access",
                                "--role",
                                "DEPLOYMENT_ADMIN",
                                "--expiration",
                                "30",
                                "--deployment-id",
                                deployment_id,
                                "--clean-output",
                            ],
                            "creating Astro deployment API token",
                            return_output=True,
                        )
                        if api_token:
                            print(f"Worker {os.getpid()}: Successfully created API token for deployment {deployment_name}")
                            return api_token
                    except subprocess.CalledProcessError as cmd_error:
                        # Capture the actual error output for debugging
                        stderr_output = cmd_error.stderr.decode('utf-8') if cmd_error.stderr else "No stderr"
                        stdout_output = cmd_error.stdout.decode('utf-8') if cmd_error.stdout else "No stdout"
                        print(f"Worker {os.getpid()}: Command failed with return code {cmd_error.returncode}")
                        print(f"Worker {os.getpid()}: STDOUT: {stdout_output}")
                        print(f"Worker {os.getpid()}: STDERR: {stderr_output}")
                        
                        # Check if it's a known recoverable error
                        if "rate limit" in stderr_output.lower() or "too many requests" in stderr_output.lower():
                            print(f"Worker {os.getpid()}: Rate limit detected, will retry with longer backoff")
                            if retry < retries:
                                sleep_time = min(30 + (retry * 10), 60)  # Longer backoff for rate limits
                                print(f"Worker {os.getpid()}: Waiting {sleep_time} seconds for rate limit...")
                                time.sleep(sleep_time)
                            continue
                        elif "deployment with id" in stderr_output.lower() and "not found" in stderr_output.lower():
                            print(f"Worker {os.getpid()}: Deployment ID mismatch detected, attempting to refresh deployment ID")
                            # Try to get the correct deployment ID from Astronomer
                            try:
                                correct_deployment_id = _get_deployment_id_by_name(deployment_name)
                                if correct_deployment_id and correct_deployment_id != deployment_id:
                                    print(f"Worker {os.getpid()}: Found correct deployment ID: {correct_deployment_id}, updating for retry")
                                    deployment_id = correct_deployment_id
                                    if retry < retries:
                                        print(f"Worker {os.getpid()}: Retrying with correct deployment ID...")
                                        continue
                            except Exception as refresh_error:
                                print(f"Worker {os.getpid()}: Failed to refresh deployment ID: {refresh_error}")
                            # If we can't refresh or this is the last retry, continue with normal error handling
                            raise cmd_error
                        else:
                            # Other errors, use normal retry logic
                            raise cmd_error
                    
                    print(f"Worker {os.getpid()}: {error_message}")
                    if retry < retries:  # Don't sleep on the last attempt
                        sleep_time = min(5 + (retry * 2), 15)  # Progressive backoff: 5, 7, 9, 11, 13, 15
                        print(f"Worker {os.getpid()}: Waiting {sleep_time} seconds before retry...")
                        time.sleep(sleep_time)
                except Exception as e:
                    print(f"Worker {os.getpid()}: {error_message}: {e}")
                    if retry < retries:  # Don't sleep on the last attempt
                        sleep_time = min(5 + (retry * 2), 15)  # Progressive backoff
                        print(f"Worker {os.getpid()}: Waiting {sleep_time} seconds before retry...")
                        time.sleep(sleep_time)
            
            # If we get here, all retries failed
            final_error = f"Failed to create Astro deployment API token for deployment {deployment_name} after {retries + 1} attempts"
            print(f"Worker {os.getpid()}: {final_error}")
            raise EnvironmentError(final_error)
            
        finally:
            # Lock will be automatically released when the file closes
            print(f"Worker {os.getpid()}: Releasing lock for API token creation for deployment {deployment_name}")
