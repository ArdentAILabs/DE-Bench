"""
This module provides a pytest fixture for managing GitHub operations in Airflow tests.
"""

import os
import time

import pytest

from Fixtures import parse_test_name
from Fixtures.GitHub.github_manager import GitHubManager


@pytest.fixture(scope="function")
def github_resource(request):
    """
    A function-scoped fixture that provides a GitHub manager for test operations.
    Each test gets its own GitHub manager instance.
    """
    build_template = request.param
    if "resource_id" not in build_template:
        import uuid
        time_stamp = int(time.time())
        test_uuid = uuid.uuid4().hex[:8]
        build_template["resource_id"] = f"github_resource_{time_stamp}_{test_uuid}"
    resource_id = build_template["resource_id"]
    # Verify required environment variables
    required_envars = [
        "AIRFLOW_GITHUB_TOKEN",
        "AIRFLOW_REPO",
    ]

    if missing_envars := [envar for envar in required_envars if not os.getenv(envar)]:
        raise ValueError(f"The following envars are not set: {missing_envars}")

    start_time = time.time()
    print(f"Worker {os.getpid()}: Starting github_resource for {resource_id}")
    
    # Create GitHub manager
    access_token = os.getenv("AIRFLOW_GITHUB_TOKEN")
    repo_url = os.getenv("AIRFLOW_REPO")
    
    github_manager = GitHubManager(access_token, repo_url, resource_id)
    
    try:
        # Clear the main dags folder
        try:
            github_manager.clear_folder("dags", keep_file_names=[".gitkeep"])
        except Exception as e:
            print(f"Warning: Could not clear main dags folder: {e}")
        print(f"Worker {os.getpid()}: Released GitHub lock for {resource_id}")

        creation_end = time.time()
        print(f"Worker {os.getpid()}: GitHub resource creation took {creation_end - start_time:.2f}s")

        # Create resource data
        resource_data = {
            "resource_id": resource_id,
            "type": "github_resource",
            "test_name": parse_test_name(request.node.name),
            "creation_time": time.time(),
            "worker_pid": os.getpid(),
            "creation_duration": creation_end - start_time,
            "description": f"A GitHub resource for {resource_id}",
            "status": "active",
            "github_manager": github_manager,
            "repo_info": github_manager.get_repo_info(),
        }

        print(f"Worker {os.getpid()}: Created GitHub resource {resource_id}")

        fixture_end_time = time.time()
        print(f"Worker {os.getpid()}: GitHub fixture setup took {fixture_end_time - start_time:.2f}s total")

        yield resource_data
    except Exception as e:
        print(f"Worker {os.getpid()}: Error in GitHub fixture: {e}")
        raise e from e
    finally:
        print(f"Worker {os.getpid()}: Cleaning up GitHub resource {resource_id}")
        cleanup_github_resource(github_manager)


def cleanup_github_resource(
    github_manager: GitHubManager,
) -> None:
    """
    Cleans up a GitHub resource, including the temp directory and the created resources in GitHub.

    :param GitHubManager github_manager : The GitHubManager instance managing the GitHub operations.
    :rtype: None
    """
    github_manager.reset_repo_state("dags")
    github_manager.cleanup_requirements()
    github_manager.delete_branch(github_manager.branch_name)
