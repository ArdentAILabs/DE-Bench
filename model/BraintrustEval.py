import os
import braintrust
from model.Run_Model import run_model


def run_de_bench_task(test_input):
    """Convert DE-Bench test to Braintrust task function"""
    # Extract test configuration from input
    task_description = test_input["task"]
    configs = test_input["configs"]
    mode = test_input.get("mode", "Ardent")
    test_resources = test_input.get("test_resources", {})

    # Set up model configurations with test resources
    custom_info = {"mode": mode}

    # Add mode-specific configuration (preserved from existing logic)
    if mode == "Ardent":
        # Add Ardent-specific setup
        if "supabase_account_resource" in test_resources:
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
    elif mode == "Claude_Code":
        # Add Claude_Code-specific setup
        if "kubernetes_object" in test_resources:
            custom_info.update(
                {
                    "kubernetes_object": test_resources["kubernetes_object"],
                    "pod_name": test_resources["pod_name"],
                }
            )

    # Execute model with existing resource setup and configs
    result = run_model(
        container=None,
        task=task_description,
        configs=configs,
        extra_information=custom_info,
    )

    return result


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
