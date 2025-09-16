import os
import braintrust
from model.Run_Model import run_model
from extract_test_configs import (
    extract_test_configuration,
    setup_test_resources,
    cleanup_test_resources,
)
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts


def _cleanup_test_resources_for_task(
    test_name, resources_or_fixtures, use_fixtures=False
):
    """Helper function to clean up test resources for a specific task."""
    try:
        if resources_or_fixtures:
            print(f"🧹 Cleaning up test resources for {test_name}...")

            if use_fixtures:
                # Use the fixture instances directly for cleanup
                from extract_test_configs import cleanup_test_resources_from_fixtures

                # We need the resource data for cleanup, get it from the actual task output
                cleanup_test_resources_from_fixtures(resources_or_fixtures, {})
            else:
                # Get custom fixtures from the test configuration
                test_data = extract_test_configuration(test_name)
                custom_fixtures = test_data.get("resource_configs", {}).get(
                    "custom_fixtures"
                )

                cleanup_test_resources(resources_or_fixtures, custom_fixtures)
            print(f"✅ Test resources cleaned up for {test_name}")
    except Exception as e:
        print(f"⚠️ Error cleaning up test resources for {test_name}: {e}")


def run_de_bench_task(test_input):
    """
    Convert DE-Bench test to Braintrust task function with per-test resource management.
    Each task execution is now self-contained with its own setup/teardown.
    """
    # Extract test configuration from input
    task_description = test_input["task"]
    mode = test_input.get("mode", "Ardent")
    test_name = test_input.get("test_name", "Unknown")
    session_data = test_input.get("session_data", {})

    print(f"🚀 Starting self-contained test execution: {test_name}")

    test_resources = {}
    fixture_instances = []
    config_results = None

    try:
        # 1. Extract test configuration and set up per-test resources
        print(f"📋 Setting up resources for {test_name}...")
        test_data = extract_test_configuration(test_name)

        # Set up per-test resources (using shared session data if available)
        test_resources, fixture_instances = setup_test_resources(
            test_data["resource_configs"], session_data=session_data
        )
        print(f"✅ Resources set up for {test_name}")

        # 2. Create configs using fixtures (all tests must have create_config now)
        create_config_func = test_data["resource_configs"].get("create_config_func")
        if not create_config_func:
            raise ValueError(
                f"❌ Test {test_name} is missing create_config function - all tests must use the new pattern"
            )

        # Use test-specific config creation function
        print(f"⚙️  Using custom config creation for {test_name}")
        updated_configs = create_config_func(fixture_instances)

        # 3. Set up model configurations if needed
        custom_info = {"mode": mode}

        if mode == "Ardent" and "supabase_account_resource" in test_resources:
            print(f"🔧 Setting up model configs for {test_name}...")

            custom_info.update(
                {
                    "publicKey": test_resources["supabase_account_resource"][
                        "publicKey"
                    ],
                    "secretKey": test_resources["supabase_account_resource"][
                        "secretKey"
                    ],
                }
            )

            config_results = set_up_model_configs(
                Configs=updated_configs,
                custom_info=custom_info,
            )
            print(f"✅ Model configs set up for {test_name}")

        elif mode == "Claude_Code":
            # Add Claude_Code-specific setup
            if "kubernetes_object" in test_resources:
                custom_info.update(
                    {
                        "kubernetes_object": test_resources["kubernetes_object"],
                        "pod_name": test_resources["pod_name"],
                    }
                )

        # 4. Execute the model
        print(f"🤖 Running model for {test_name}...")
        model_result = run_model(
            container=None,
            task=task_description,
            configs=updated_configs,
            extra_information=custom_info,
        )
        print(f"✅ Model execution completed for {test_name}")

        # Clean up model artifacts first (but keep test resources for validation)
        if (
            config_results
            and mode == "Ardent"
            and "supabase_account_resource" in test_resources
        ):
            print(f"🧹 Cleaning up model artifacts for {test_name}...")
            cleanup_model_artifacts(
                Configs=updated_configs,
                custom_info=custom_info,
            )
            print(f"✅ Model artifacts cleaned up for {test_name}")

        # Return both the model result and the fixture instances for validation
        # Store test_name and other cleanup info needed later
        return {
            "result": model_result,
            "fixtures": fixture_instances,  # Pass actual fixture instances with _resource_data
            "test_name": test_name,
            "test_resources": test_resources,  # Keep resource data for cleanup
            "updated_configs": updated_configs,
            "custom_info": custom_info,
        }

    except Exception as e:
        print(f"❌ Error in test execution for {test_name}: {e}")
        # Clean up resources on error (use fixture instances if available)
        if fixture_instances:
            _cleanup_test_resources_for_task(
                test_name, fixture_instances, use_fixtures=True
            )
        else:
            _cleanup_test_resources_for_task(test_name, test_resources)
        raise
