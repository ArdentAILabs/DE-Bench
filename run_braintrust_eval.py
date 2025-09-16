import os
import signal
import sys
import time
import requests
import argparse
import re
from typing import Dict, List, Any, Optional
import braintrust
from dotenv import load_dotenv
from model.BraintrustEval import run_de_bench_task
from extract_test_configs import (
    extract_test_configuration,
    get_test_validator,
    discover_session_fixtures,
    setup_session_fixtures,
    cleanup_session_fixtures,
)

# Note: set_up_model_configs and cleanup_model_artifacts are now used inside run_de_bench_task

# Load environment variables
load_dotenv()

# Global cleanup flag to prevent double cleanup
cleanup_already_run = False
active_session_fixtures = []
active_session_data = {}


def cleanup_handler() -> None:
    """Cleanup function that runs on exit or interrupt - preserves existing logic"""
    global cleanup_already_run, active_session_fixtures, active_session_data

    if cleanup_already_run:
        print("🔄 Cleanup already completed, skipping...")
        return

    cleanup_already_run = True

    try:
        # Note: Per-test resources are now cleaned up inside run_de_bench_task
        # Only session-level cleanup is needed here

        # Clean up session-level fixtures
        if active_session_fixtures:
            try:
                print("🧹 Cleaning up session-level fixtures...")
                cleanup_session_fixtures(active_session_fixtures, active_session_data)
                print("✅ Session-level fixtures cleaned up")
            except Exception as e:
                print(f"❌ Error cleaning up session fixtures: {e}")

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


def discover_available_tests(filter_patterns: Optional[List[str]] = None) -> List[str]:
    """
    Dynamically discover available tests for Braintrust evaluation with optional filtering.

    Scans the Tests directory and finds all tests that follow the new pattern:
    - Have a test file with get_fixtures() and create_config() functions
    - Have a Test_Configs.py with User_Input

    Args:
        filter_patterns: List of regex patterns to filter test names

    Returns:
        List of test names that match the filter patterns (if any)
    """
    available_tests = []
    tests_dir = "Tests"

    if not os.path.exists(tests_dir):
        print(f"⚠️  Tests directory '{tests_dir}' not found")
        return []

    # Scan all directories in Tests/
    for item in os.listdir(tests_dir):
        test_dir_path = os.path.join(tests_dir, item)

        # Skip if not a directory or starts with . or __
        if (
            not os.path.isdir(test_dir_path)
            or item.startswith(".")
            or item.startswith("__")
        ):
            continue

        # Check if this test follows the new pattern
        if _is_valid_new_pattern_test(item):
            available_tests.append(item)
            print(f"✅ Discovered test: {item}")
        else:
            print(f"⚠️  Skipping {item} - doesn't follow new pattern or has errors")

    # Sort for consistent ordering
    available_tests.sort()

    # Apply filters if provided
    if filter_patterns:
        filtered_tests = []
        for test_name in available_tests:
            for pattern in filter_patterns:
                try:
                    if re.search(pattern, test_name, re.IGNORECASE):
                        filtered_tests.append(test_name)
                        break  # Stop checking other patterns for this test
                except re.error as e:
                    print(f"⚠️  Invalid regex pattern '{pattern}': {e}")
                    continue
        return filtered_tests

    return available_tests


def _is_valid_new_pattern_test(test_name: str) -> bool:
    """
    Check if a test follows the new pattern with get_fixtures() and create_config().

    Args:
        test_name: Name of the test directory

    Returns:
        True if test follows new pattern, False otherwise
    """
    import importlib

    try:
        # Check if Test_Configs.py exists and has User_Input
        config_module_path = f"Tests.{test_name}.Test_Configs"
        config_module = importlib.import_module(config_module_path)

        if not hasattr(config_module, "User_Input"):
            return False

        # Find test files in the directory
        test_dir = f"Tests/{test_name}"
        test_files = []

        for file in os.listdir(test_dir):
            if file.startswith("test_") and file.endswith(".py"):
                test_files.append(file[:-3])  # Remove .py extension

        if not test_files:
            return False

        # Check the first test file for required functions
        test_module_path = f"Tests.{test_name}.{test_files[0]}"
        try:
            test_module = importlib.import_module(test_module_path)
        except Exception as e:
            print(
                f"Test '{test_name}' does not match pattern: failed to import '{test_module_path}': {e}"
            )
            return False

        # Must have get_fixtures function
        if not hasattr(test_module, "get_fixtures"):
            print(f"Test '{test_name}' does not match pattern: missing 'get_fixtures'")
            return False

        # Must have create_config function
        if not hasattr(test_module, "create_config"):
            print(f"Test '{test_name}' does not match pattern: missing 'create_config'")
            return False

        # Must have validate_test function
        if not hasattr(test_module, "validate_test"):
            print(f"Test '{test_name}' does not match pattern: missing 'validate_test'")
            return False

        # Verify get_fixtures returns a list
        get_fixtures_func = getattr(test_module, "get_fixtures")
        if not callable(get_fixtures_func):
            print(
                f"Test '{test_name}' does not match pattern: 'get_fixtures' is not callable"
            )
            return False

        # All checks passed
        return True

    except Exception as e:
        # Any import errors or missing attributes mean it's not a valid test
        print(f"Test '{test_name}' does not match pattern: {e}")
        return False


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


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run DE-Bench Braintrust evaluation with session-level fixture support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_braintrust_eval.py Ardent                    # Run all tests in Ardent mode
  python run_braintrust_eval.py Ardent Claude_Code        # Run all tests in both modes
  python run_braintrust_eval.py --filter "MongoDB.*"     # Run only MongoDB tests
  python run_braintrust_eval.py --filter ".*Hello.*"     # Run only Hello World tests
  python run_braintrust_eval.py --filter "MongoDB.*" "MySQL.*" Ardent  # MongoDB & MySQL in Ardent mode
        """,
    )

    parser.add_argument(
        "modes",
        nargs="*",
        default=["Ardent"],
        help="Execution modes to run (e.g., Ardent, Claude_Code). Default: ['Ardent']",
    )

    parser.add_argument(
        "--filter",
        action="append",
        dest="filter_patterns",
        help="Filter test names using regex patterns. Can be used multiple times. Case-insensitive.",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output with additional debugging information",
    )

    return parser.parse_args()


def run_multi_test_evaluation(
    modes: List[str] = ["Ardent"],
    test_names: Optional[List[str]] = None,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Run multiple tests as Braintrust evaluation for specified modes"""
    global active_session_fixtures, active_session_data

    # Set up signal handler for graceful cleanup
    signal.signal(signal.SIGINT, signal_handler)

    # Initialize model (preserve existing logic)
    from model.Initialize_Model import initialize_model

    initialize_model()

    # Discover available tests if not specified
    if test_names is None:
        test_names = discover_available_tests()

    if verbose:
        print(f"🔍 Available tests discovered: {test_names}")

    if not test_names:
        print("❌ No tests found matching the filter criteria")
        return {}

    print(f"🚀 Starting DE-Bench Braintrust evaluation for tests: {test_names}...")

    # Collect all test configurations and discover session fixtures
    all_test_configs = []
    all_fixtures = []

    try:
        # First pass: Extract configurations from all tests and collect fixtures for session discovery
        for test_name in test_names:
            print(f"📋 Preparing {test_name}...")

            # Extract test configuration
            test_data = extract_test_configuration(test_name)

            # Collect fixtures for session discovery (if any exist)
            if "custom_fixtures" in test_data.get("resource_configs", {}):
                all_fixtures.extend(test_data["resource_configs"]["custom_fixtures"])

            # Store base test configs (resources will be managed per-task now)
            for case in test_data["test_cases"]:
                all_test_configs.append(
                    {
                        "test_name": test_name,
                        "case": case,
                    }
                )

        # Discover and set up session-level fixtures only
        session_fixtures = discover_session_fixtures(all_fixtures)
        if session_fixtures:
            print(f"🌐 Found {len(session_fixtures)} session-level fixture types...")
            active_session_fixtures = session_fixtures
            active_session_data = setup_session_fixtures(session_fixtures)
            print("✅ Session-level fixtures set up successfully")
        else:
            print("📝 No session-level fixtures required")
            active_session_data = {}

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
                        "test_name": config["test_name"],
                        "session_data": active_session_data,  # Pass session data for per-task resource setup
                    },
                    "metadata": {**config["case"]["metadata"], "mode": mode},
                }
                mode_samples.append(sample)

            # Note: Model config setup and per-test resource setup now happens inside run_de_bench_task

            # Create unified validator that can handle all test types
            def unified_validator(input, output, expected=None):
                # Braintrust passes the full sample as 'input', so get test_name from there
                test_name = input.get("test_name", "Unknown")
                if test_name == "Unknown":
                    # Fallback: check in metadata if it exists
                    test_name = input.get("metadata", {}).get("test_name", "Unknown")

                print(f"🔍 Validating test: {test_name}")

                # Extract model result and fixtures from the task output
                model_result = (
                    output.get("result") if isinstance(output, dict) else output
                )
                fixtures_data = (
                    output.get("fixtures", {}) if isinstance(output, dict) else {}
                )

                try:
                    # fixtures_data now contains the actual fixture instances, not data to convert
                    fixtures = fixtures_data if isinstance(fixtures_data, list) else []

                    validator = get_test_validator(test_name)
                    result = validator(model_result, expected, fixtures=fixtures)

                    # Show test steps if available
                    if isinstance(result, dict) and "test_steps" in result:
                        print(f"📋 Test steps for {test_name}:")
                        for step in result["test_steps"]:
                            status_icon = (
                                "✅"
                                if step["status"] == "passed"
                                else "❌" if step["status"] == "failed" else "⚠️"
                            )
                            print(f"   {status_icon} {step['name']}: {step['status']}")
                        actual_result = result.get("success", False)
                    else:
                        actual_result = result

                    print(f"✅ Validation result for {test_name}: {actual_result}")
                    return actual_result
                except Exception as e:
                    print(f"❌ Validation error for {test_name}: {e}")
                    return False
                finally:
                    # Clean up test resources after validation
                    if isinstance(output, dict) and "fixtures" in output:
                        from model.BraintrustEval import (
                            _cleanup_test_resources_for_task,
                        )

                        # Use fixture instances for cleanup if available, otherwise fall back to test_resources
                        if output.get("fixtures"):
                            _cleanup_test_resources_for_task(
                                test_name, output["fixtures"], use_fixtures=True
                            )
                        elif output.get("test_resources"):
                            _cleanup_test_resources_for_task(
                                test_name, output["test_resources"]
                            )

            print(
                f"🔍 Running Braintrust.Eval for {mode} mode with {len(mode_samples)} samples"
            )

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

            # Note: Model artifacts and test resources are now cleaned up inside run_de_bench_task

        return results

    finally:
        # Note: Per-test resource cleanup now happens inside run_de_bench_task
        # Only session-level cleanup is needed here

        # Clean up session-level fixtures
        if active_session_fixtures:
            print("\n🧹 Cleaning up session-level fixtures...")
            try:
                cleanup_session_fixtures(active_session_fixtures, active_session_data)
                print("✅ Session-level fixtures cleaned up")
            except Exception as e:
                print(f"❌ Error cleaning up session fixtures: {e}")
            finally:
                active_session_fixtures = []
                active_session_data = {}


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    if args.verbose:
        print(f"🔧 Parsed arguments:")
        print(f"   Modes: {args.modes}")
        print(f"   Filter patterns: {args.filter_patterns}")
        print(f"   Verbose: {args.verbose}")

    try:
        # Discover tests with filtering
        if args.filter_patterns:
            print(f"🔍 Filtering tests with patterns: {args.filter_patterns}")
            filtered_tests = discover_available_tests(args.filter_patterns)
        else:
            filtered_tests = None

        # Run evaluation on filtered tests
        results = run_multi_test_evaluation(
            modes=args.modes, test_names=filtered_tests, verbose=args.verbose
        )

        if results:
            print(f"\n🎉 Completed {len(results)} multi-test experiments!")

            # Print summary for each mode
            for mode, result in results.items():
                print(f"\n📊 {mode} Mode Results:")
                print(f"   Summary: {result.summary}")
        else:
            print("📝 No experiments were run")

    except KeyboardInterrupt:
        print("\n🛑 Evaluation interrupted by user")
    except Exception as e:
        print(f"❌ Evaluation failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Ensure final cleanup
        cleanup_handler()
