import os
import braintrust
from model.Run_Model import run_model
import traceback
from extract_test_configs import (
    extract_test_configuration,
    setup_test_resources,
    cleanup_supabase_account_resource,
)
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts


def _teardown_test_fixtures(test_name, fixtures):
    """Helper function to clean up test resources for a specific task."""
    try:
        if fixtures:
            print(
                f"🧹 Tearing down {len(fixtures)} fixtures for {test_name} (fixtures: {', '.join([f.get_resource_type() for f in fixtures])})"
            )

            for fixture in reversed(fixtures):
                try:
                    fixture._test_teardown()
                except Exception as e:
                    print(
                        f"⚠️ Error tearing down fixture: {fixture.get_resource_type()}: {e}\n{traceback.format_exc()}"
                    )
                    continue

                print(f"...✅ Tore down fixture: {fixture.get_resource_type()}")

        # Always clean up Supabase account separately (legacy resource)
        if "supabase_account_resource" in resources:
            cleanup_supabase_account_resource(resources["supabase_account_resource"])

    except Exception as e:
        print(f"⚠️ Error tearing down fixtures for {test_name}: {e}")


def full_model_run(
    test_name,
    mode,
    test_resources,
    fixture_instances,
    model_configs,
    task_description,
):
    """
    Step 3 and 4: Set up model configurations if needed and execute the model.
    """
    config_results = None
    custom_info = {"mode": mode}

    if mode == "Ardent" and "supabase_account_resource" in test_resources:
        print(f"🔧 Setting up model configs for {test_name}...")

        custom_info.update(
            {
                "publicKey": test_resources["supabase_account_resource"]["publicKey"],
                "secretKey": test_resources["supabase_account_resource"]["secretKey"],
            }
        )

        config_results = set_up_model_configs(
            Configs=model_configs,
            custom_info=custom_info,
        )
        print(f"✅ Model configs set up for {test_name}")

    elif mode == "Claude_Code":
        print(f"🔧 Setting up Kubernetes for Claude Code for {test_name}...")

        # Set up Kubernetes infrastructure for Claude Code
        config_results = set_up_model_configs(
            Configs=model_configs,
            custom_info=custom_info,
        )

        # Add the Kubernetes objects to custom_info for the model
        if config_results:
            custom_info.update(config_results)

        print(f"✅ Kubernetes setup completed for {test_name}")

    elif mode == "OpenAI_Codex":
        print(f"🔧 Setting up Kubernetes for OpenAI Codex for {test_name}...")

        # Set up Kubernetes infrastructure for OpenAI Codex
        config_results = set_up_model_configs(
            Configs=model_configs,
            custom_info=custom_info,
        )

        # Add the Kubernetes objects to custom_info for the model
        if config_results:
            custom_info.update(config_results)

        print(f"✅ Kubernetes setup completed for {test_name}")

    # 4. Execute the model
    print(f"🤖 Running model for {test_name}...")
    model_result = run_model(
        container=None,
        task=task_description,
        configs=model_configs,
        extra_information=custom_info,
    )
    print(f"✅ Model execution completed for {test_name}")

    # Clean up model artifacts first (but keep test resources for validation)
    if config_results:
        if mode == "Ardent" and "supabase_account_resource" in test_resources:
            print(f"🧹 Cleaning up model artifacts for {test_name}...")
            cleanup_model_artifacts(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Model artifacts cleaned up for {test_name}")
        elif mode == "Claude_Code":
            print(f"🧹 Cleaning up Kubernetes resources for {test_name}...")
            cleanup_model_artifacts(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Kubernetes resources cleaned up for {test_name}")
        elif mode == "OpenAI_Codex":
            print(f"🧹 Cleaning up Kubernetes resources for {test_name}...")
            cleanup_model_artifacts(
                Configs=model_configs,
                custom_info=custom_info,
            )
            print(f"✅ Kubernetes resources cleaned up for {test_name}")

    return {
        "result": model_result,
        "fixtures": fixture_instances,
        "test_name": test_name,
        "test_resources": test_resources,
        "model_configs": model_configs,
        "custom_info": custom_info,
    }


def run_de_bench_task(test_input):
    """
    Convert DE-Bench test to Braintrust task function with per-test resource management.
    Each task execution is now self-contained with its own setup/teardown.
    """
    try:
        # Extract test configuration from input
        task_description = test_input["task"]
        mode = test_input.get("mode", "Ardent")
        test_name = test_input.get("test_name", "Unknown")
        session_data = test_input.get("session_data", {})

        print(f"🚀 Starting self-contained test execution: {test_name}")

        test_resources = {}
        fixture_instances = []

        # 1. Extract test configuration and set up per-test resources
        print(f"📋 Setting up resources for {test_name}...")
        test_data = extract_test_configuration(test_name)

        # Set up per-test resources (using shared session data if available)
        test_resources, fixture_instances = setup_test_resources(
            test_data["resource_configs"], session_data=session_data
        )
        print(f"✅ Resources set up for {test_name}")

        model_inputs_base = {
            "test_name": test_name,
            "mode": mode,
            "test_resources": test_resources,
            "fixture_instances": fixture_instances,
            "task_description": task_description,
        }

        # 3. Modify inputs if needed
        create_model_inputs_func = test_data["resource_configs"].get(
            "create_model_inputs_func"
        )
        if create_model_inputs_func:
            final_full_model_run_args = create_model_inputs_func(
                model_inputs_base, fixture_instances
            )
        else:
            raise ValueError(
                f"❌ Test {test_name} is missing create_model_inputs_func function"
            )

        # Validate that model_configs and task_description are in the final_full_model_run_args
        if "model_configs" not in final_full_model_run_args:
            raise ValueError(
                f"❌ Test {test_name} did not return model_configs from create_model_inputs_func"
            )
        if "task_description" not in final_full_model_run_args:
            raise ValueError(
                f"❌ Test {test_name} did not return task_description from create_model_inputs_func"
            )

        # 3 & 4. Set up model configs and run model
        result = full_model_run(**final_full_model_run_args)

        # Note: Tear down doesn't happen here, it happens in the validator because we need to access the fixture instances
        return result

    except Exception as e:
        print(f"❌ Error in test execution for {test_name}: {e}")
        # Tear down test fixtures on error
        if fixture_instances:
            _teardown_test_fixtures(test_name, fixture_instances)

        raise
