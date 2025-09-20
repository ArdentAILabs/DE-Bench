import os
import uuid
import json
from ardent import ArdentClient, ArdentError

from dotenv import load_dotenv
from Environment.Kubernetes.Kubernetes import Kubernetes
from kubernetes import client as k8s_client_sdk
from Environment.File_Share.File_Share import create_file_share
from braintrust import current_span
from braintrust import traced

load_dotenv()


@traced(name="set_up_model_configs")
def set_up_model_configs(Configs, custom_info=None):
    mode = (custom_info or {}).get("mode", "Ardent")

    results = {}

    # For non-Ardent modes, no remote config setup is required
    if mode == "Ardent":

        Ardent_Client = ArdentClient(
            public_key=custom_info["publicKey"],
            secret_key=custom_info["secretKey"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

        if "services" in Configs:

            for service in Configs["services"]:
                service_config = Configs["services"][service]


                print(f"🔍 SERVICE CONFIG: {service_config}")
                print(f"🔍 SERVICE: {service}")

                # Handle different service types
                if service == "airflow":
                    # ensure all required fields are present
                    service_result = Ardent_Client.set_config(
                        config_type="airflow",
                        github_token=service_config["github_token"],
                        repo=service_config["repo"],
                        dag_path=service_config["dag_path"],
                        host=service_config["host"],
                        username=service_config["username"],
                        password=service_config["password"],
                        api_token=service_config["api_token"],
                        requirements_path=service_config["requirements_path"],
                        header_overrides={
                            "X-Braintrust-Exported-Parent-Span": current_span().export(),
                        },
                    )

                elif service == "mongodb":
                    print(f"🔧 Setting up MongoDB config:")
                    print(
                        f"   Connection string: {service_config.get('connection_string', 'MISSING')}"
                    )
                    print(f"   Databases: {service_config.get('databases', 'MISSING')}")

                    try:
                        service_result = Ardent_Client.set_config(
                            config_type="mongodb",
                            connection_string=service_config["connection_string"],
                            databases=service_config["databases"],
                            header_overrides={
                                "X-Braintrust-Exported-Parent-Span": current_span().export(),
                            },
                        )
                        print(f"✅ MongoDB config set successfully")
                    except Exception as e:
                        print(f"❌ MongoDB config failed:")
                        print(f"   Error: {str(e)}")
                        print(f"   Config data being sent:")
                        print(f"     - config_type: 'mongodb'")
                        print(
                            f"     - connection_string: {service_config.get('connection_string')}"
                        )
                        print(f"     - databases: {service_config.get('databases')}")
                        raise

                elif service == "postgreSQL":

                    print(f"🔧 Setting up PostgreSQL config:")
                    print(f"   Hostname: {service_config['hostname']}")
                    print(f"   Port: {service_config['port']}")
                    print(f"   Username: {service_config['username']}")
                    print(f"   Password: {service_config['password']}")
                    print(f"   Databases: {service_config['databases']}")

                    try:
                        service_result = Ardent_Client.set_config(
                            config_type="postgreSQL",
                            Hostname=service_config["hostname"],
                            Port=service_config["port"],
                            username=service_config["username"],
                            password=service_config["password"],
                            databases=service_config["databases"],
                            header_overrides={
                                "X-Braintrust-Exported-Parent-Span": current_span().export(),
                            },
                        )
                    except Exception as e:
                        print("EXCEPTION", e.response.text, e.response.__dict__)
                        raise

                elif service == "mysql":
                    service_result = Ardent_Client.set_config(
                        config_type="mysql",
                        host=service_config["host"],
                        port=service_config["port"],
                        username=service_config["username"],
                        password=service_config["password"],
                        databases=service_config["databases"],
                        header_overrides={
                            "X-Braintrust-Exported-Parent-Span": current_span().export(),
                        },
                    )

                elif service == "tigerbeetle":
                    service_result = Ardent_Client.set_config(
                        config_type="tigerbeetle",
                        cluster_id=service_config["cluster_id"],
                        replica_addresses=service_config["replica_addresses"],
                        header_overrides={
                            "X-Braintrust-Exported-Parent-Span": current_span().export(),
                        },
                    )

                elif service == "databricks":
                    service_result = Ardent_Client.set_config(
                        config_type="databricks",
                        server_hostname=service_config["host"],
                        access_token=service_config["token"],
                        http_path=service_config["http_path"],
                        cluster_id=service_config.get("cluster_id"),
                        catalogs=[
                            {
                                "name": service_config["catalog"],
                                "databases": [
                                    {"name": service_config["schema"], "tables": []}
                                ],
                            }
                        ],
                        header_overrides={
                            "X-Braintrust-Exported-Parent-Span": current_span().export(),
                        },
                    )

                elif service == "snowflake":
                    service_result = Ardent_Client.set_config(
                        config_type="snowflake",
                        account=service_config["account"],
                        user=service_config["user"],
                        password=service_config["password"],
                        warehouse=service_config["warehouse"],
                        role=service_config.get("role", "SYSADMIN"),
                        databases=[{"name": service_config["database"]}],
                        header_overrides={
                            "X-Braintrust-Exported-Parent-Span": current_span().export(),
                        },
                    )

                # Add the result to our results dictionary
                if not results:
                    results = {service: service_result}
                else:
                    results[service] = service_result
    elif mode == "Claude_Code":
        # set up the kubernetes job

        print("Setting up Kubernetes job for Claude Code")
        test_id = str(uuid.uuid4())
        session_id = test_id.replace("-", "")
        file_share_name, deps_share_name = create_file_share(session_id)

        # K8s job and command
        job_name = f"job-{session_id[:20]}".lower()

        # Kubernetes client
        job_k8s = Kubernetes(test_id=test_id)

        if not job_k8s:
            raise Exception("Kubernetes client not found")

        azure_client = job_k8s.cloud_provider_client
        api_instance = job_k8s.get_k8s_client(azure_client)

        # Create job with mounted Azure File Share
        job_k8s.create_job_in_namespace_with_volume_mount(
            api_instance=api_instance, shareName=file_share_name, jobID=job_name, mode = mode
        )

        # Wait for pod and run commands
        pod_name = job_k8s.wait_for_pod_to_be_avialable_and_get_name(
            api_instance, job_name
        )

        # First command: Install Node.js and Claude Code
        install_command = "curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && apt-get install -y nodejs && npm install -g @anthropic-ai/claude-code && claude --version"
        install_output = job_k8s.run_terminal_command_in_pod(pod_name, install_command)

        # Second command: Check environment variables
        env_command = 'env | grep -E "(AWS_|CLAUDE_)" | sort'
        env_output = job_k8s.run_terminal_command_in_pod(pod_name, env_command)

        results = {
            "pod_name": pod_name,
            "kubernetes_object": job_k8s,
            "k8s_job_name": job_name,
            "test_id": test_id,
        }
    elif mode == "OpenAI_Codex":
        # set up the kubernetes job for OpenAI Codex

        print("Setting up Kubernetes job for OpenAI Codex")
        test_id = str(uuid.uuid4())
        session_id = test_id.replace("-", "")
        file_share_name, deps_share_name = create_file_share(session_id)

        # K8s job and command
        job_name = f"job-{session_id[:20]}".lower()

        # Kubernetes client
        job_k8s = Kubernetes(test_id=test_id)

        if not job_k8s:
            raise Exception("Kubernetes client not found")

        azure_client = job_k8s.cloud_provider_client
        api_instance = job_k8s.get_k8s_client(azure_client)

        # Create job with mounted Azure File Share
        job_k8s.create_job_in_namespace_with_volume_mount(
            api_instance=api_instance, shareName=file_share_name, jobID=job_name, mode="OpenAI_Codex"
        )

        # Wait for pod and run commands
        pod_name = job_k8s.wait_for_pod_to_be_avialable_and_get_name(
            api_instance, job_name
        )

        # Install Node.js and OpenAI Codex
        install_command = "curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && apt-get install -y nodejs && npm install -g @openai/codex@0.29.0"
        install_output = job_k8s.run_terminal_command_in_pod(pod_name, install_command)

        results = {
            "pod_name": pod_name,
            "kubernetes_object": job_k8s,
            "k8s_job_name": job_name,
            "test_id": test_id,
        }
    return results


def cleanup_model_artifacts(Configs, custom_info=None):
    # This is a place where we can remove the model configs

    mode = custom_info.get("mode", "Ardent")

    print("Cleaning up model artifacts")
    print(f"--mode: {mode}")
    # Note: Skipping custom_info JSON dump to avoid serialization issues with Kubernetes objects

    if mode == "Ardent":
        Ardent_Client = ArdentClient(
            public_key=custom_info["publicKey"],
            secret_key=custom_info["secretKey"],
            base_url=os.getenv("ARDENT_BASE_URL"),
        )

        if "services" in Configs:
            for service in Configs["services"]:
                if service in custom_info:
                    id = custom_info[service]["specific_config"]["id"]
                    Ardent_Client.delete_config(config_id=id)

        if "job_id" in custom_info:
            Ardent_Client.delete_job(job_id=custom_info["job_id"])

    elif mode == "Claude_Code":

        print("Cleaning up Kubernetes job for Claude Code")
        print(custom_info)
        # Cleanup Kubernetes job for Claude Code
        if "k8s_job_name" in custom_info and "test_id" in custom_info:
            try:
                job_k8s = Kubernetes(test_id=custom_info["test_id"])
                azure_client = job_k8s.cloud_provider_client
                api_instance = job_k8s.get_k8s_client(azure_client)

                api_instance.delete_namespaced_job(
                    name=custom_info["k8s_job_name"],
                    namespace="default",
                    body=k8s_client_sdk.V1DeleteOptions(
                        propagation_policy="Foreground"
                    ),
                )
                print(f"Deleted Kubernetes job: {custom_info['k8s_job_name']}")
            except k8s_client_sdk.ApiException as e:
                print(f"Exception when deleting Kubernetes job: {e}")
            except Exception as e:
                print(f"Error during Kubernetes cleanup: {e}")

    elif mode == "OpenAI_Codex":

        print("Cleaning up Kubernetes job for OpenAI Codex")
        print(custom_info)
        # Cleanup Kubernetes job for OpenAI Codex
        if "k8s_job_name" in custom_info and "test_id" in custom_info:
            try:
                job_k8s = Kubernetes(test_id=custom_info["test_id"])
                azure_client = job_k8s.cloud_provider_client
                api_instance = job_k8s.get_k8s_client(azure_client)

                api_instance.delete_namespaced_job(
                    name=custom_info["k8s_job_name"],
                    namespace="default",
                    body=k8s_client_sdk.V1DeleteOptions(
                        propagation_policy="Foreground"
                    ),
                )
                print(f"Deleted Kubernetes job: {custom_info['k8s_job_name']}")
            except k8s_client_sdk.ApiException as e:
                print(f"Exception when deleting Kubernetes job: {e}")
            except Exception as e:
                print(f"Error during Kubernetes cleanup: {e}")


# Create an alias for backwards compatibility
remove_model_configs = cleanup_model_artifacts
