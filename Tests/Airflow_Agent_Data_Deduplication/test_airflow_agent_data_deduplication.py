# Braintrust-only Airflow test - no pytest dependencies
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import time
import uuid
import psycopg2
from typing import List, Dict, Any
from Fixtures.base_fixture import DEBenchFixture

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


def get_fixtures() -> List[DEBenchFixture]:
    """
    Provides custom DEBenchFixture instances for Braintrust evaluation.
    This Airflow test validates that AI can create a user data deduplication DAG.
    """
    from Fixtures.Airflow.airflow_fixture import AirflowFixture
    from Fixtures.PostgreSQL.postgres_resources import PostgreSQLFixture
    from Fixtures.GitHub.github_fixture import GitHubFixture

    # Initialize Airflow fixture with test-specific configuration
    custom_airflow_config = {
        "resource_id": f"data_deduplication_test_{test_timestamp}_{test_uuid}",
    }

    # Initialize PostgreSQL fixture for the user data
    custom_postgres_config = {
        "resource_id": f"deduplication_test_{test_timestamp}_{test_uuid}",
        "databases": [
            {
                "name": f"user_data_{test_timestamp}_{test_uuid}",
                "sql_file": "schema.sql",
            }
        ],
    }

    # Initialize GitHub fixture for PR and branch management
    custom_github_config = {
        "resource_id": f"test_airflow_data_deduplication_test_{test_timestamp}_{test_uuid}",
    }

    airflow_fixture = AirflowFixture(custom_config=custom_airflow_config)
    postgres_fixture = PostgreSQLFixture(custom_config=custom_postgres_config)
    github_fixture = GitHubFixture(custom_config=custom_github_config)

    return [airflow_fixture, postgres_fixture, github_fixture]


def create_model_inputs(
    base_model_inputs: Dict[str, Any], fixtures: List[DEBenchFixture]
) -> Dict[str, Any]:
    """
    Create test-specific config using the set-up fixtures.
    This function has access to all fixture data after setup and dynamically
    updates the task description with GitHub branch and PR information.
    """
    import os
    from extract_test_configs import create_config_from_fixtures

    # Get GitHub fixture to access manager for dynamic branch/PR creation
    github_fixture = next(
        (f for f in fixtures if f.get_resource_type() == "github_resource"), None
    )

    if not github_fixture:
        raise Exception(
            "GitHub fixture not found - required for branch and PR management"
        )

    # Get the GitHub manager from the fixture
    github_resource_data = getattr(github_fixture, "_resource_data", None)
    if not github_resource_data:
        raise Exception("GitHub resource data not available")

    github_manager = github_resource_data.get("github_manager")
    if not github_manager:
        raise Exception("GitHub manager not available")

    # Generate dynamic branch and PR names
    pr_title = f"Add User Data Deduplication Pipeline {test_timestamp}_{test_uuid}"
    branch_name = f"feature/user-deduplication-{test_timestamp}_{test_uuid}"

    # Start with the original user input from Test_Configs
    task_description = Test_Configs.User_Input

    # Add merge step to user input
    task_description = github_manager.add_merge_step_to_user_input(task_description)

    # Replace placeholders with dynamic values
    task_description = task_description.replace("BRANCH_NAME", branch_name)
    task_description = task_description.replace("PR_NAME", pr_title)

    # Set up GitHub secrets for Astro access
    github_manager.check_and_update_gh_secrets(
        secrets={
            "ASTRO_ACCESS_TOKEN": os.environ["ASTRO_ACCESS_TOKEN"],
        }
    )

    print(f"🔧 Generated dynamic branch name: {branch_name}")
    print(f"🔧 Generated dynamic PR title: {pr_title}")

    # Use the helper to automatically create config from all fixtures
    return {
        **base_model_inputs,
        "model_configs": create_config_from_fixtures(fixtures),
        "task_description": task_description,
    }


def validate_test(model_result, fixtures=None):
    """
    Validates that the AI agent successfully created a user data deduplication DAG.

    Expected behavior:
    - DAG should be created with name "user_deduplication_dag"
    - DAG should have a task named "deduplicate_users"
    - DAG should run successfully and create a "deduplicated_users" table
    - Data should be properly deduplicated

    Args:
        model_result: The result from the AI model execution
        fixtures: List of DEBenchFixture instances used in the test

    Returns:
        dict: Contains 'score' float and 'metadata' dict with validation details
    """
    # Create comprehensive test steps for validation
    test_steps = [
        {
            "name": "Agent Task Execution",
            "description": "AI Agent executes task to create User Deduplication DAG",
            "status": "running",
            "Result_Message": "Checking if AI agent executed the Airflow DAG creation task...",
        },
        {
            "name": "Git Branch Creation",
            "description": "Verify that git branch was created with the correct name",
            "status": "running",
            "Result_Message": "Checking if git branch exists...",
        },
        {
            "name": "PR Creation and Merge",
            "description": "Verify that PR was created and merged successfully",
            "status": "running",
            "Result_Message": "Checking if PR was created and merged...",
        },
        {
            "name": "GitHub Action Completion",
            "description": "Verify that GitHub action completed successfully",
            "status": "running",
            "Result_Message": "Waiting for GitHub action to complete...",
        },
        {
            "name": "Airflow Redeployment",
            "description": "Verify that Airflow redeployed after GitHub action",
            "status": "running",
            "Result_Message": "Checking if Airflow redeployed successfully...",
        },
        {
            "name": "DAG Creation Validation",
            "description": "Verify that user_deduplication_dag was created in Airflow",
            "status": "running",
            "Result_Message": "Validating that User Deduplication DAG exists in Airflow...",
        },
        {
            "name": "DAG Task Validation",
            "description": "Verify that DAG has the deduplicate_users task",
            "status": "running",
            "Result_Message": "Checking if DAG has the required deduplicate_users task...",
        },
        {
            "name": "DAG Execution and Monitoring",
            "description": "Trigger the DAG and verify it runs successfully",
            "status": "running",
            "Result_Message": "Triggering DAG and monitoring execution...",
        },
        {
            "name": "Deduplication Table Creation",
            "description": "Verify that deduplicated_users table was created",
            "status": "running",
            "Result_Message": "Checking if deduplicated_users table exists...",
        },
        {
            "name": "Data Deduplication Validation",
            "description": "Verify that data was properly deduplicated",
            "status": "running",
            "Result_Message": "Validating data deduplication results...",
        },
    ]

    try:
        # Step 1: Check that the agent task executed
        if not model_result or model_result.get("status") == "failed":
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = "❌ AI Agent task execution failed or returned no result"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[0]["status"] = "passed"
        test_steps[0]["Result_Message"] = "✅ AI Agent completed task execution successfully"

        # Get fixtures for Airflow, PostgreSQL, and GitHub
        airflow_fixture = next((f for f in fixtures if f.get_resource_type() == "airflow_resource"), None) if fixtures else None
        postgres_fixture = next((f for f in fixtures if f.get_resource_type() == "postgres_resource"), None) if fixtures else None
        github_fixture = next((f for f in fixtures if f.get_resource_type() == "github_resource"), None) if fixtures else None

        if not airflow_fixture:
            raise Exception("Airflow fixture not found")
        if not postgres_fixture:
            raise Exception("PostgreSQL fixture not found")
        if not github_fixture:
            raise Exception("GitHub fixture not found")

        # Get resource data
        airflow_resource_data = getattr(airflow_fixture, "_resource_data", None)
        if not airflow_resource_data:
            raise Exception("Airflow resource data not available")

        postgres_resource_data = getattr(postgres_fixture, "_resource_data", None)
        if not postgres_resource_data:
            raise Exception("PostgreSQL resource data not available")

        github_resource_data = getattr(github_fixture, "_resource_data", None)
        if not github_resource_data:
            raise Exception("GitHub resource data not available")

        airflow_instance = airflow_resource_data["airflow_instance"]
        base_url = airflow_resource_data["base_url"]
        github_manager = github_resource_data.get("github_manager")

        if not github_manager:
            raise Exception("GitHub manager not available")

        # Generate the same branch and PR names used in create_model_inputs
        pr_title = f"Add User Data Deduplication Pipeline {test_timestamp}_{test_uuid}"
        branch_name = f"feature/user-deduplication-{test_timestamp}_{test_uuid}"

        # Step 2-6: GitHub and Airflow workflow
        print(f"🔍 Checking for branch: {branch_name}")
        time.sleep(10)

        branch_exists, test_steps[1] = github_manager.verify_branch_exists(branch_name, test_steps[1])
        if not branch_exists:
            test_steps[1]["status"] = "failed"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = f"✅ Git branch '{branch_name}' created successfully"

        # PR creation and merge
        pr_exists, test_steps[2] = github_manager.find_and_merge_pr(
            pr_title=pr_title,
            test_step=test_steps[2],
            commit_title=pr_title,
            merge_method="squash",
            build_info={
                "deploymentId": airflow_resource_data["deployment_id"],
                "deploymentName": airflow_resource_data["deployment_name"],
            },
        )

        if not pr_exists:
            test_steps[2]["status"] = "failed"
            test_steps[2]["Result_Message"] = "❌ Unable to find and merge PR"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[2]["status"] = "passed"
        test_steps[2]["Result_Message"] = f"✅ PR '{pr_title}' created and merged successfully"

        # GitHub action completion
        if not github_manager.check_if_action_is_complete(pr_title=pr_title):
            test_steps[3]["status"] = "failed"
            test_steps[3]["Result_Message"] = "❌ GitHub action did not complete successfully"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[3]["status"] = "passed"
        test_steps[3]["Result_Message"] = "✅ GitHub action completed successfully"

        # Airflow redeployment
        if not airflow_instance.wait_for_airflow_to_be_ready():
            test_steps[4]["status"] = "failed"
            test_steps[4]["Result_Message"] = "❌ Airflow instance did not redeploy successfully"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        test_steps[4]["status"] = "passed"
        test_steps[4]["Result_Message"] = "✅ Airflow redeployed successfully after GitHub action"

        # DAG existence check
        dag_name = "user_deduplication_dag"
        print(f"🔍 Checking for DAG: {dag_name} in Airflow at {base_url}")

        if airflow_instance.verify_airflow_dag_exists(dag_name):
            test_steps[5]["status"] = "passed"
            test_steps[5]["Result_Message"] = f"✅ DAG '{dag_name}' found in Airflow"
        else:
            test_steps[5]["status"] = "failed"
            test_steps[5]["Result_Message"] = f"❌ DAG '{dag_name}' not found in Airflow"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # DAG task validation - check if it has the deduplicate_users task
        try:
            dag_tasks = airflow_instance.get_dag_tasks(dag_name)
            task_ids = [task.get("task_id", "") for task in dag_tasks]
            
            if "deduplicate_users" in task_ids:
                test_steps[6]["status"] = "passed"
                test_steps[6]["Result_Message"] = f"✅ Found 'deduplicate_users' task in DAG (tasks: {', '.join(task_ids)})"
            else:
                test_steps[6]["status"] = "failed"
                test_steps[6]["Result_Message"] = f"❌ Task 'deduplicate_users' not found. Available tasks: {', '.join(task_ids)}"
        except Exception as e:
            test_steps[6]["status"] = "failed"
            test_steps[6]["Result_Message"] = f"❌ Error checking DAG tasks: {str(e)}"

        # DAG execution
        print(f"🔍 Triggering DAG: {dag_name}")
        dag_run_id = airflow_instance.unpause_and_trigger_airflow_dag(dag_name)

        if not dag_run_id:
            test_steps[7]["status"] = "failed"
            test_steps[7]["Result_Message"] = "❌ Failed to trigger DAG"
            return {"score": 0.0, "metadata": {"test_steps": test_steps}}

        # Monitor the DAG run until completion
        airflow_instance.verify_dag_id_ran(dag_name, dag_run_id)
        test_steps[7]["status"] = "passed"
        test_steps[7]["Result_Message"] = f"✅ DAG '{dag_name}' executed successfully (run_id: {dag_run_id})"

        # Capture comprehensive DAG information for debugging (source, import errors, task logs)
        print("📊 Capturing comprehensive DAG information for debugging...")
        try:
            comprehensive_dag_info = airflow_instance.get_comprehensive_dag_info(
                dag_id=dag_name,
                dag_run_id=dag_run_id,
                github_manager=github_manager,
            )

            # Add requirements.txt snapshot to comprehensive DAG info
            print(f"📦 Adding requirements.txt snapshot from feature branch: {branch_name}")
            req_snapshot = None
            candidate_paths = ["Requirements/requirements.txt", "requirements.txt"]
            
            for path in candidate_paths:
                try:
                    content = github_manager.get_file_content(path, branch_name)
                    if content is not None:
                        req_snapshot = {"path": path, "content": content}
                        print(f"✅ Found requirements.txt at {path}")
                        break
                except Exception as e:
                    print(f"⚠️ Could not read {path} from {branch_name}: {e}")
                    continue
            
            if req_snapshot:
                comprehensive_dag_info["requirements_snapshot"] = req_snapshot
                print(f"📄 Requirements snapshot added to comprehensive DAG info ({len(req_snapshot['content'])} chars)")
            else:
                print("⚠️ requirements.txt not found in any expected location")

            dag_source = comprehensive_dag_info.get("dag_source", {})
            import_errors = comprehensive_dag_info.get("import_errors", [])

            if dag_source.get("source_code"):
                print(
                    f"📄 DAG source code captured ({len(dag_source['source_code'])} characters)"
                )
                print(
                    f"📄 Source code preview: {dag_source['source_code'][:200]}..."
                )
            else:
                print("⚠️ DAG source code not available - attempting manual retrieval...")
                try:
                    dag_files = github_manager.get_all_dag_files()
                    if dag_files:
                        comprehensive_dag_info.setdefault("dag_source", {}).setdefault("github_files", dag_files)
                        for filename, file_data in dag_files.items():
                            if filename.endswith('.py'):
                                comprehensive_dag_info["dag_source"]["source_code"] = file_data["content"]
                                print(
                                    f"✅ Retrieved DAG source from GitHub: {filename} ({len(file_data['content'])} chars)"
                                )
                                break
                except Exception as e:
                    print(f"❌ Failed to retrieve DAG files from GitHub: {e}")

            if import_errors:
                print(f"❌ Found {len(import_errors)} import errors")
                for error in import_errors:
                    print(
                        f"   - {error.get('filename', 'Unknown')}: {error.get('stack_trace', 'No details')}"
                    )
            else:
                print("✅ No DAG import errors found")

            # Attach to test metadata
            test_steps.append(
                {
                    "name": "DAG Information Capture",
                    "description": "Capture comprehensive DAG information for debugging",
                    "status": "passed",
                    "Result_Message": "✅ Comprehensive DAG information captured successfully",
                    "comprehensive_dag_info": comprehensive_dag_info,
                    "dag_source_code": dag_source.get("source_code"),
                    "dag_file_path": dag_source.get("file_path"),
                    "dag_import_errors": import_errors,
                    "task_logs_summary": {
                        task_id: {
                            "state": task_info.get("state"),
                            "duration": task_info.get("duration"),
                            "log_length": len(task_info.get("logs", "")),
                        }
                        for task_id, task_info in comprehensive_dag_info.get("task_logs", {}).items()
                    },
                }
            )

        except Exception as e:
            print(f"⚠️ Could not capture comprehensive DAG info: {e}")
            test_steps.append(
                {
                    "name": "DAG Information Capture",
                    "description": "Capture comprehensive DAG information for debugging",
                    "status": "failed",
                    "Result_Message": f"❌ Failed to capture DAG information: {str(e)}",
                }
            )

        # Step 9-10: PostgreSQL Database Validation
        postgres_config = postgres_resource_data.get("databases", [{}])[0]
        database_name = postgres_config.get("name", "")

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=database_name,
            sslmode="require",
        )
        cur = conn.cursor()

        print(f"🔍 Connected to PostgreSQL database: {database_name}")

        # Step 9: Check if deduplicated_users table was created
        try:
            cur.execute("SELECT COUNT(*) FROM deduplicated_users")
            deduplicated_count = cur.fetchone()[0]

            if deduplicated_count > 0:
                test_steps[8]["status"] = "passed"
                test_steps[8]["Result_Message"] = f"✅ Deduplication table created with {deduplicated_count} records"
            else:
                test_steps[8]["status"] = "failed"
                test_steps[8]["Result_Message"] = "❌ deduplicated_users table exists but has no data"

        except psycopg2.Error as e:
            test_steps[8]["status"] = "failed"
            test_steps[8]["Result_Message"] = f"❌ deduplicated_users table validation error: {str(e)}"

        # Step 10: Validate data deduplication logic
        try:
            # Compare original vs deduplicated counts to verify deduplication occurred
            cur.execute("SELECT COUNT(*) FROM users")  # Assuming original table is called 'users'
            original_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM deduplicated_users")
            deduplicated_count = cur.fetchone()[0]

            if deduplicated_count <= original_count and deduplicated_count > 0:
                test_steps[9]["status"] = "passed"
                test_steps[9]["Result_Message"] = f"✅ Data deduplication validated: {original_count} → {deduplicated_count} records"
            else:
                test_steps[9]["status"] = "failed"
                test_steps[9]["Result_Message"] = f"❌ Deduplication failed: {original_count} → {deduplicated_count} records"

        except psycopg2.Error as e:
            test_steps[9]["status"] = "failed"
            test_steps[9]["Result_Message"] = f"❌ Data deduplication validation error: {str(e)}"

        cur.close()
        conn.close()

    except Exception as e:
        # Mark any unfinished steps as failed
        for step in test_steps:
            if step["status"] == "running":
                step["status"] = "failed"
                step["Result_Message"] = f"❌ Validation error: {str(e)}"

    # Calculate score as the fraction of steps that passed
    passed_steps = sum([step["status"] == "passed" for step in test_steps])
    total_steps = len(test_steps)
    score = passed_steps / total_steps

    print(f"🎯 Validation completed: {passed_steps}/{total_steps} steps passed (Score: {score:.2f})")

    return {
        "score": score,
        "metadata": {"test_steps": test_steps},
    }