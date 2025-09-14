import os

User_Input = """
Create an Airflow DAG that:
1. Extracts workflow execution data from PostgreSQL database 'workflow_db', tables 'workflow_runs' and 'workflow_step_runs'
2. Transforms the execution data to compute observability metrics:
   - Calculate run duration and step latency
   - Flag failed runs vs succeeded runs
   - Enrich with organization and workflow metadata
3. Loads the results into Snowflake database 'observability_db', table 'workflow_step_events'
4. Runs every hour at minute 0
5. Name it workflow_observability_etl
6. Name the branch BRANCH_NAME
7. Call the PR PR_NAME
8. Use these DAG settings:
    - retries: 3
    - retry_delay: 10 minutes

Transform the execution data to extract:
- workflow_run_id, step_id, workflow_name, organization_id
- start_time, end_time, step_duration_seconds, run_duration_seconds
- status (success/failed), error_message
- customer_id, department, workflow_version
- step_type, step_category
- resource_usage (cpu, memory if available)

The goal is to enable:
- Reliability measurement (p99 latency, fail rates)
- Billing analysis based on execution metrics
- Long-term retention of workflow execution events
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "host": os.getenv("AIRFLOW_HOST"),
            "username": os.getenv("AIRFLOW_USERNAME"),
            "password": os.getenv("AIRFLOW_PASSWORD"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "workflow_db"}],  # Will be overridden by fixture
        },
        "snowflake": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "database": "OBSERVABILITY_DB",  # Will be overridden by fixture
            "schema": "WORKFLOW_OBSERVABILITY"  # Will be overridden by fixture
        },
    }
}
