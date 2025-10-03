"""
This module provides pytest fixtures for Airflow deployment management.
Follows the standard pattern used by other *_resources.py files.
"""

import os
import time
import tempfile
import fcntl

import pytest
from Fixtures import parse_test_name
from Fixtures.Databricks.cache_manager import CacheManager
from Fixtures.Airflow.Airflow_class import AirflowManager


def _ensure_cache_manager_initialized() -> CacheManager:
    """
    Ensure CacheManager is initialized using file-based coordination.
    
    Returns:
        The initialized CacheManager instance
    """
    lock_file_path = os.path.join(tempfile.gettempdir(), "cache_manager_init.lock")
    cache_manager = CacheManager()
    
    with open(lock_file_path, "w") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Create temporary manager to fetch deployments
            temp_manager = AirflowManager()
            if astro_deployments := temp_manager.fetch_astro_deployments():
                cache_manager.populate_astronomer_deployments(astro_deployments)
                print(f"Worker {os.getpid()}: CacheManager initialized with {len(astro_deployments)} deployments")
                
                for deployment in astro_deployments:
                    print(f"Worker {os.getpid()}: Deployment: {deployment['deployment_name']} "
                          f"({deployment['deployment_id']}) - Status: {deployment['status']}")
            else:
                print(f"Worker {os.getpid()}: No existing deployments found, CacheManager initialized with empty deployment list")
            
            verification_deployments = cache_manager.get_all_astronomer_deployments()
            print(f"Worker {os.getpid()}: CacheManager initialized and populated with {len(verification_deployments)} deployments")
            return cache_manager
        
        except (IOError, OSError):
            print(f"Worker {os.getpid()}: Waiting for another process to complete CacheManager initialization...")
            
            max_wait_time = 300
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    
                    temp_manager = AirflowManager()
                    if existing_deployments := temp_manager.fetch_astro_deployments():
                        print(f"Worker {os.getpid()}: CacheManager initialization completed by another process "
                              f"({len(existing_deployments)} deployments)")
                        cache_manager.populate_astronomer_deployments(existing_deployments)
                        return cache_manager
                    
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    time.sleep(2)
                except (IOError, OSError):
                    time.sleep(2)
                    continue
            
            # Fallback
            temp_manager = AirflowManager()
            if existing_deployments := temp_manager.fetch_astro_deployments():
                print(f"Worker {os.getpid()}: CacheManager initialization fallback after wait "
                      f"({len(existing_deployments)} deployments)")
                return cache_manager
            
            raise Exception(f"Timeout waiting for CacheManager initialization after {max_wait_time} seconds")


@pytest.fixture(scope="session")
def astro_login():
    """
    A session-scoped fixture that ensures Astro is logged in once for the entire test session.
    Uses file-based coordination to prevent multiple logins in parallel execution.
    """
    if os.getenv("ASTRO_API_TOKEN"):
        print(f"Worker {os.getpid()}: Astro API token found, skipping login")
        return None
    
    # Create temporary manager for login
    temp_manager = AirflowManager()
    return temp_manager._ensure_astro_login()


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
    A function-scoped fixture that creates unique AirflowManager instances for each test.
    Each test gets its own isolated Airflow environment.
    """
    build_template = request.param
    start_time = time.time()
    
    # Create AirflowManager resource
    print(f"Worker {os.getpid()}: Creating Airflow resource")
    creation_start = time.time()
    
    airflow_manager = AirflowManager.create_resource(request, build_template, shared_cache_manager)
    
    creation_end = time.time()
    print(f"Worker {os.getpid()}: Airflow resource creation took {creation_end - creation_start:.2f}s")
    
    # Create detailed resource data for backward compatibility
    resource_data = {
        "resource_id": airflow_manager.resource_id,
        "type": "airflow_resource",
        "test_name": parse_test_name(request.node.name),
        "creation_time": time.time(),
        "worker_pid": os.getpid(),
        "creation_duration": creation_end - creation_start,
        "description": f"An Airflow resource for {airflow_manager.resource_id}",
        "status": "active",
        "project_name": airflow_manager.airflow_dir.stem,
        "base_url": airflow_manager.host,
        "deployment_id": airflow_manager.deployment_id,
        "deployment_name": airflow_manager.deployment_name,
        "api_url": airflow_manager.api_url,
        "api_token": airflow_manager.api_token,
        "api_headers": airflow_manager.api_headers,
        "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
        "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
        "airflow_instance": airflow_manager,  # Include the manager instance
        "created_resources": airflow_manager.test_resources,
        "cache_manager": shared_cache_manager,
    }
    
    print(f"Worker {os.getpid()}: Created Airflow resource {airflow_manager.resource_id}")
    
    fixture_end_time = time.time()
    print(f"Worker {os.getpid()}: Airflow fixture setup took {fixture_end_time - start_time:.2f}s total")
    
    try:
        yield resource_data
    except Exception as e:
        print(f"Worker {os.getpid()}: Error in Airflow fixture: {e}")
        raise e from e
    finally:
        print(f"Worker {os.getpid()}: Cleaning up Airflow resource {airflow_manager.resource_id}")
        airflow_manager.cleanup_resource(airflow_manager.airflow_dir)