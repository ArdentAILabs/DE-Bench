import os
import braintrust
from model.Run_Model import run_model
from extract_test_configs import (
    extract_test_configuration,
    setup_test_resources,
    cleanup_test_resources,
    update_configs_with_fixture_data,
)
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts


def run_de_bench_task(test_input):
    """
    Convert DE-Bench test to Braintrust task function with per-test resource management.
    Each task execution is now self-contained with its own setup/teardown.
    """
    # Extract test configuration from input
    task_description = test_input["task"]
    base_configs = test_input["configs"]
    mode = test_input.get("mode", "Ardent")
    test_name = test_input.get("test_name", "Unknown")
    session_data = test_input.get("session_data", {})

    print(f"🚀 Starting self-contained test execution: {test_name}")

    test_resources = {}
    config_results = None

    try:
        # 1. Extract test configuration and set up per-test resources
        print(f"📋 Setting up resources for {test_name}...")
        test_data = extract_test_configuration(test_name)

        # Set up per-test resources (using shared session data if available)
        test_resources = setup_test_resources(
            test_data["resource_configs"], session_data=session_data
        )
        print(f"✅ Resources set up for {test_name}")

        # 2. Update configs with fixture data
        updated_configs = update_configs_with_fixture_data(base_configs, test_resources)

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
        result = run_model(
            container=None,
            task=task_description,
            configs=updated_configs,
            extra_information=custom_info,
        )
        print(f"✅ Model execution completed for {test_name}")

        return result

    except Exception as e:
        print(f"❌ Error in test execution for {test_name}: {e}")
        raise

    finally:
        # 5. Clean up resources and model artifacts
        try:
            # Clean up model artifacts first
            if (
                config_results
                and mode == "Ardent"
                and "supabase_account_resource" in test_resources
            ):
                print(f"🧹 Cleaning up model artifacts for {test_name}...")
                cleanup_model_artifacts(
                    Configs=(
                        updated_configs
                        if "updated_configs" in locals()
                        else base_configs
                    ),
                    custom_info=custom_info,
                )
                print(f"✅ Model artifacts cleaned up for {test_name}")
        except Exception as e:
            print(f"⚠️ Error cleaning up model artifacts for {test_name}: {e}")

        try:
            # Clean up test resources
            if test_resources:
                print(f"🧹 Cleaning up test resources for {test_name}...")

                # Get custom fixtures from the test configuration
                test_data = extract_test_configuration(test_name)
                custom_fixtures = test_data.get("resource_configs", {}).get(
                    "custom_fixtures"
                )

                cleanup_test_resources(test_resources, custom_fixtures)
                print(f"✅ Test resources cleaned up for {test_name}")
        except Exception as e:
            print(f"⚠️ Error cleaning up test resources for {test_name}: {e}")


def create_braintrust_scorer(validation_function):
    """Convert DE-Bench validation to Braintrust scorer"""

    def scorer(input, output, expected):
        try:
            # Run existing validation logic
            score = validation_function(output, expected)

            # If it's a bool, return 1.0 if score else 0.0
            if isinstance(score, bool):
                return 1.0 if score else 0.0
            elif isinstance(score, float) or isinstance(score, int):
                return score
            else:
                raise Exception(f"Invalid score type: {type(score)}")

        except Exception as e:
            print(f"Error in Braintrust scorer: {e}")
            return None  # Failed validation

    return scorer
