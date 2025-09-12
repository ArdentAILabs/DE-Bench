import os
import signal
import sys
import time
import requests
from typing import Dict, List, Any, Optional
import braintrust
from dotenv import load_dotenv
from model.BraintrustEval import run_de_bench_task
from extract_test_configs import (
    extract_test_configuration,
    setup_test_resources,
    cleanup_test_resources,
    get_test_validator,
)
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts

# Load environment variables
load_dotenv()

# Global cleanup flag to prevent double cleanup
cleanup_already_run = False
active_resources = {}


def cleanup_handler() -> None:
    """Cleanup function that runs on exit or interrupt - preserves existing logic"""
    global cleanup_already_run, active_resources

    if cleanup_already_run:
        print("🔄 Cleanup already completed, skipping...")
        return

    cleanup_already_run = True

    try:
        # Clean up any active test resources
        # Note: We don't have access to custom_fixtures in cleanup_handler,
        # but resources should already be cleaned up in the main flow
        if active_resources:
            for test_name, resources in active_resources.items():
                try:
                    cleanup_test_resources(resources)  # Fall back to auto-detection
                    print(f"✅ Emergency cleanup completed for {test_name}")
                except Exception as e:
                    print(f"❌ Emergency cleanup failed for {test_name}: {e}")

        # Use existing session spindown logic
        from Fixtures.session_spindown import session_spindown

        session_spindown()
        print("✅ Session spindown completed")
    except Exception as e:
        print(f"❌ Error during session spindown: {e}")

    # Clean up temp directory (preserve existing logic)
    import shutil

    if os.path.exists(".tmp"):
        try:
            shutil.rmtree(".tmp/")
            print("✅ Temp directory cleaned up")
        except Exception as e:
            print(f"❌ Error cleaning temp directory: {e}")


def signal_handler(signum: int, frame: Any) -> None:
    """Handle Ctrl+C (SIGINT) gracefully - preserves existing behavior"""
    print("\n🛑 Evaluation interrupted by user -- Running cleanup...")
    cleanup_handler()
    print("🔄 Cleanup completed. Exiting...")
    sys.exit(0)


def discover_available_tests() -> List[str]:
    """Discover available tests for Braintrust evaluation"""
    # For now, hardcode the available tests - in the future this could scan directories
    available_tests = [
        "MongoDB_Agent_Add_Record",
        "MySQL_Agent_Update_Records",
        "Simple_Hello_World_Test",
    ]
    return available_tests


def fetch_git_info() -> Dict[str, Any]:
    """
    Fetch git information from the Ardent API to use in experiment naming.

    Returns:
        Dict with git info (branch, error)
    """
    try:
        base_url = os.getenv("ARDENT_BASE_URL", "http://localhost:8080")
        response = requests.get(f"{base_url}/v1/system/git-info", timeout=5)

        if response.status_code == 200:
            git_info = response.json()
            branch = git_info.get("branch")
            print(f"📋 Git info: branch={branch if branch else 'N/A'}")
            return git_info
        else:
            print(f"⚠️  Failed to fetch git info: HTTP {response.status_code}")
            return {
                "branch": None,
                "error": f"HTTP {response.status_code}",
            }

    except Exception as e:
        print(f"⚠️  Could not fetch git info: {e}")
        return {
            "branch": None,
            "error": str(e),
        }


def construct_experiment_name(mode: str, git_info: Dict[str, Any]) -> str:
    """
    Construct a meaningful experiment name using git information and mode.

    Format: multi-test-{mode}-{branch}
    Fallback: multi-test-{mode}-{timestamp} if no git info available

    Args:
        mode: The execution mode (e.g., "Ardent", "Claude_Code")
        git_info: Git information from the Ardent API

    Returns:
        Experiment name string
    """

    # Try to use git branch for more meaningful names
    branch = git_info.get("branch")
    branch = branch if branch else "unknown-branch"

    # Clean branch name (remove special characters, limit length)
    return f"{branch}__{mode.lower()}"


def run_multi_test_evaluation(
    modes: List[str] = ["Ardent"], test_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Run multiple tests as Braintrust evaluation for specified modes"""
    global active_resources

    # Set up signal handler for graceful cleanup
    signal.signal(signal.SIGINT, signal_handler)

    # Initialize model (preserve existing logic)
    from model.Initialize_Model import initialize_model

    initialize_model()

    # Discover available tests if not specified
    if test_names is None:
        test_names = discover_available_tests()

    print(f"🚀 Starting DE-Bench Braintrust evaluation for tests: {test_names}...")

    # Collect all test configurations
    all_test_configs = []
    all_resources = {}

    try:
        # Extract configurations from all tests
        for test_name in test_names:
            print(f"📋 Preparing {test_name}...")

            # Extract test configuration
            test_data = extract_test_configuration(test_name)

            # Set up resources for this test
            test_resources = setup_test_resources(test_data["resource_configs"])
            all_resources[test_name] = test_resources

            # Store test config for reuse across modes
            for case in test_data["test_cases"]:
                all_test_configs.append(
                    {
                        "test_name": test_name,
                        "case": case,
                        "test_resources": test_resources,
                    }
                )

            print(f"✅ {test_name} resources set up successfully")

        active_resources = all_resources
        results = {}

        # Run one experiment per mode
        for mode in modes:
            print(f"\n🧪 Running Braintrust experiment for {mode} mode...")

        # Fetch git info for experiment naming
        git_info = fetch_git_info()
        experiment_name = construct_experiment_name(mode, git_info)

        # Create samples for this mode from all tests
        mode_samples = []
        for config in all_test_configs:
            sample = {
                "input": {
                    **config["case"]["input"],
                    "mode": mode,
                    "test_resources": config["test_resources"],
                    "test_name": config[
                        "test_name"
                    ],  # Add test_name to input for easy access
                },
                "metadata": {**config["case"]["metadata"], "mode": mode},
            }
            mode_samples.append(sample)

            # Set up model configs for tests that need them
            for test_name in test_names:
                if (
                    mode == "Ardent"
                    and "supabase_account_resource" in all_resources[test_name]
                ):
                    test_case = next(
                        config["case"]
                        for config in all_test_configs
                        if config["test_name"] == test_name
                    )

                    print(
                        f"🔧 Setting up model configs for {test_name} in {mode} mode..."
                    )
                    print(f"   Config being sent: {test_case['input']['configs']}")

                    try:
                        config_results = set_up_model_configs(
                            Configs=test_case["input"]["configs"],
                            custom_info={
                                "mode": mode,
                                "publicKey": all_resources[test_name][
                                    "supabase_account_resource"
                                ]["publicKey"],
                                "secretKey": all_resources[test_name][
                                    "supabase_account_resource"
                                ]["secretKey"],
                            },
                        )
                        print(f"✅ Model configs set up for {test_name} in {mode} mode")
                    except Exception as e:
                        print(f"❌ Failed to set up model configs for {test_name}:")
                        print(f"   Error type: {type(e).__name__}")
                        print(f"   Error message: {str(e)}")
                        print(f"   Config data:")
                        print(f"     - Test: {test_name}")
                        print(f"     - Mode: {mode}")
                        print(f"     - Configs: {test_case['input']['configs']}")

                        # Don't continue - this is a critical error
                        raise Exception(
                            f"Model config setup failed for {test_name}: {str(e)}"
                        )

            # Create unified validator that can handle all test types
            def unified_validator(input, output, expected=None):
                # Braintrust passes the full sample as 'input', so get test_name from there
                test_name = input.get("test_name", "Unknown")
                if test_name == "Unknown":
                    # Fallback: check in metadata if it exists
                    test_name = input.get("metadata", {}).get("test_name", "Unknown")

                # Extract test resources/fixtures from input
                test_resources = input.get("test_resources", {})

                # Convert test_resources back to fixture instances for validation
                fixtures = []
                if "custom_fixtures" in test_resources:
                    # Use the custom fixtures directly
                    fixtures = test_resources["custom_fixtures"]

                print(f"🔍 Validating test: {test_name} with {len(fixtures)} fixtures")
                try:
                    validator = get_test_validator(test_name)
                    result = validator(output, expected, fixtures=fixtures)
                    print(f"✅ Validation result for {test_name}: {result}")
                    return result
                except Exception as e:
                    print(f"❌ Validation error for {test_name}: {e}")
                    return False

            # Run Braintrust.Eval for this mode with all tests
            result = braintrust.Eval(
                name="DE-Bench",
                experiment_name=experiment_name,
                data=mode_samples,
                task=run_de_bench_task,
                scores=[unified_validator],
                metadata={
                    "mode": mode,
                    "test_types": test_names,
                    "timestamp": str(time.time()),
                    "git_info": git_info,
                },
                # TODO: Make this configurable
                max_concurrency=10,
            )

            results[mode] = result
            print(f"✅ Completed {mode} experiment with {len(mode_samples)} samples")
            print(f"   Summary: {result.summary}")

            # Clean up model artifacts for each test
            for test_name in test_names:
                if (
                    mode == "Ardent"
                    and "supabase_account_resource" in all_resources[test_name]
                ):
                    try:
                        test_case = next(
                            config["case"]
                            for config in all_test_configs
                            if config["test_name"] == test_name
                        )
                        cleanup_model_artifacts(
                            Configs=test_case["input"]["configs"],
                            custom_info={
                                "mode": mode,
                                "publicKey": all_resources[test_name][
                                    "supabase_account_resource"
                                ]["publicKey"],
                                "secretKey": all_resources[test_name][
                                    "supabase_account_resource"
                                ]["secretKey"],
                            },
                        )
                        print(f"✅ Model artifacts cleaned up for {test_name}")
                    except Exception as e:
                        print(
                            f"⚠️ Error cleaning up model artifacts for {test_name}: {e}"
                        )

        return results

    finally:
        # Clean up all resources
        print("\n🧹 Cleaning up all test resources...")
        for test_name, resources in all_resources.items():
            try:
                # Get custom fixtures if they exist
                test_config = next(
                    (
                        config
                        for config in all_test_configs
                        if config["test_name"] == test_name
                    ),
                    None,
                )
                custom_fixtures = (
                    test_config["case"]["metadata"].get("custom_fixtures")
                    if test_config
                    else None
                )

                cleanup_test_resources(resources, custom_fixtures)
                print(f"✅ Cleaned up {test_name}")
            except Exception as e:
                print(f"❌ Error cleaning up {test_name}: {e}")

        active_resources = {}
        print("✅ All test resources cleaned up")


if __name__ == "__main__":
    # Parse command line arguments for modes
    modes = sys.argv[1:] if len(sys.argv) > 1 else ["Ardent"]

    try:
        # Run evaluation on all available tests
        results = run_multi_test_evaluation(modes)
        print(f"\n🎉 Completed {len(results)} multi-test experiments!")

        # Print summary for each mode
        for mode, result in results.items():
            print(f"\n📊 {mode} Mode Results:")
            print(f"   Summary: {result.summary}")

    except KeyboardInterrupt:
        print("\n🛑 Evaluation interrupted by user")
    except Exception as e:
        print(f"❌ Evaluation failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Ensure final cleanup
        cleanup_handler()
